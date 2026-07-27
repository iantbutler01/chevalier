//! The one sequenced write-ahead log of a mount-local view.
//!
//! Namespace and content mutations share a single monotonic sequence. Records
//! are appended as JSON lines to a rotating log generation; checkpoints anchor
//! replay so restart cost is proportional to the unacknowledged backlog rather
//! than to the lifetime of the mount.
//!
//! Three hard rules the implementation must not soften:
//!
//! 1. **Reads never touch this module.** `lookup`, `getattr`, `readdir`,
//!    `readlink` and `read` are answered by the backing tree alone. Nothing here
//!    is on a read path, so nothing here may be allowed to grow read latency.
//! 2. **The append critical section is proportional to the new event.** Under
//!    the append lock: assign a sequence, frame an already-serialized record,
//!    `write_all` it, and update the in-memory index. Never under it: payload
//!    copying, hashing, `fsync`, checkpoint writing, compaction, or any network
//!    call.
//! 3. **`fsync` is a local group sync.** A guest `fsync` costs one device sync
//!    per dirty file plus one for the log -- never one sync per older pending
//!    write, and never a network round trip.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::Notify;
use uuid::Uuid;

use super::payload::{PayloadReader, PayloadStore};
use super::types::{
    CompactionOutcome, MountCheckpoint, MountEvent, MountMutation, MountPreImage, PayloadRef,
    PayloadSource, PreparedEvent, PublishBatch, RecoveryResolution, RecoveryState, StoragePressure,
    WalRecord, validate_mount_path,
};
use super::{
    BACKING_FREE_FRACTION_FLOOR, LOG_TARGET_BYTES, MAX_RECORD_BYTES, MountStateLayout,
    WAL_FORMAT_VERSION, WAL_HARD_LIMIT_BYTES, WAL_SOFT_LIMIT_BYTES, create_new, open_append,
    read_json, sync_directory, write_json_atomic,
};

/// Wakes the publisher the moment a commit makes new work publishable.
/// `notify_one` is callable from a native FUSE request thread, so the append
/// path never needs the tokio runtime.
pub(crate) type WalNotify = Arc<Notify>;

/// One prepare as the in-memory index holds it: the event plus where its
/// `Prepared` record physically lives, which is what lets a checkpoint anchor
/// replay at the oldest still-needed record instead of at byte zero.
#[derive(Clone, Debug)]
struct PreparedRecord {
    event: MountEvent,
    generation: u64,
    offset: u64,
}

/// Everything the append critical section touches. One short mutex: assign a
/// sequence, write one framed buffer, update the index. Payload capture,
/// hashing, `fsync`, checkpoint writing and compaction all happen with this
/// lock dropped.
struct AppendState {
    file: File,
    /// Duplicate descriptor for the active generation so a group-commit leader
    /// can `fdatasync` it with no lock held.
    sync_handle: Arc<File>,
    /// Descriptors of generations that were rotated away but whose tail has not
    /// been proven durable yet. A sync round covers them before the active one.
    pending_sync: Vec<Arc<File>>,
    generation: u64,
    generation_len: u64,
    /// Bytes appended since `open`. Compared against the durable byte watermark
    /// so a sync that carries no new sequence (a `ContentDirty` or a
    /// `Checkpointed` marker) is still forced through.
    appended_bytes: u64,
    /// Set when a partial record could not be rolled back. Every later append
    /// fails closed rather than leaving garbage a future `open` would reject
    /// forever.
    poisoned: Option<String>,
    next_sequence: u64,
    prepared: BTreeMap<u64, PreparedRecord>,
    committed: BTreeSet<u64>,
    aborted: BTreeSet<u64>,
    acknowledged_sequence: u64,
    remote_revision: u64,
    first_unpruned_sequence: u64,
    tree_generation: u64,
    checkpoint_id: u64,
    active_generations: BTreeSet<u64>,
    dirty_content: BTreeSet<String>,
    pending_payload_bytes: u64,
    events_since_checkpoint: u64,
    last_checkpoint: Option<MountCheckpoint>,
    /// Payload files a durable checkpoint stopped naming. Only these are ever
    /// eligible for reclamation, which is what keeps compaction from racing a
    /// capture that has not been referenced by a prepare yet.
    reclaim_candidates: BTreeSet<String>,
    compaction_pending: bool,
}

/// The group-commit watermark. Waiters coalesce on the in-flight round instead
/// of each issuing their own device syncs.
struct SyncState {
    durable_sequence: u64,
    durable_bytes: u64,
    round: u64,
    in_flight: bool,
}

struct WalInner {
    layout: MountStateLayout,
    payloads: PayloadStore,
    epoch: String,
    notify: WalNotify,
    append: Mutex<AppendState>,
    sync: Mutex<SyncState>,
    sync_done: Condvar,
    /// Serializes checkpoint / rotation / compaction so two maintenance passes
    /// never publish contradictory checkpoints.
    maintenance: Mutex<()>,
    /// Sampling clock and cache for [`MountWal::storage_pressure`], which every
    /// content mutation consults and which must therefore never walk the state
    /// directory inline.
    started: Instant,
    pressure_sampled_at: AtomicU64,
    pressure_level: AtomicU8,
}

/// How long a storage-pressure sample is served before it is measured again.
const STORAGE_PRESSURE_REFRESH_MS: u64 = 250;
/// Sentinel for "no sample has been taken yet", distinguishable from a sample
/// taken in the first millisecond of the mount's life.
const NEVER_SAMPLED: u64 = u64::MAX;

fn encode_pressure(level: StoragePressure) -> u8 {
    match level {
        StoragePressure::None => 0,
        StoragePressure::Soft => 1,
        StoragePressure::Hard => 2,
    }
}

fn decode_pressure(level: u8) -> StoragePressure {
    match level {
        1 => StoragePressure::Soft,
        2 => StoragePressure::Hard,
        _ => StoragePressure::None,
    }
}

/// The sequenced log plus its payload store, checkpoint, and remote cursor.
///
/// Cloning is cheap and shares all state; the publisher, the maintenance task
/// and every FUSE callback hold clones.
#[derive(Clone)]
pub(crate) struct MountWal {
    inner: Arc<WalInner>,
}

impl MountWal {
    /// Open or recover the log under `layout`.
    ///
    /// Ordering: ensure the directories, read the checkpoint (falling back to
    /// the previous one only when the current file is absent or truncated),
    /// validate it, open the log generations it declares, replay forward from
    /// its replay point, truncate a torn final append, fail closed on interior
    /// corruption or on a payload that a committed event references but that no
    /// longer verifies, then open the active generation for append.
    ///
    /// `expected_epoch` is the ownership epoch the caller believes it holds.
    /// `None` accepts whatever the durable state carries (and mints one for a
    /// fresh state directory); `Some` mismatching the durable epoch is a hard
    /// error -- a lagging or foreign state directory is never adopted silently.
    pub(crate) fn open(layout: &MountStateLayout, expected_epoch: Option<&str>) -> Result<Self> {
        layout.ensure()?;
        let payloads = PayloadStore::open(&layout.payload_dir())?;
        let checkpoint = load_checkpoint(layout)?;
        let on_disk = layout.log_generations()?;

        let mut scan = ScanState {
            first_unpruned_sequence: checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.first_unpruned_sequence.max(1))
                .unwrap_or(1),
            ..ScanState::default()
        };
        // Seeded before the scan so a replayed `ContentDirty` re-adds and a
        // replayed committed `ReplaceFile` clears in the order they happened.
        // The checkpoint's snapshot is never older than the replay point, so the
        // union can only over-approximate, and an extra re-seal is harmless.
        if let Some(checkpoint) = checkpoint.as_ref() {
            for path in &checkpoint.dirty_content_paths {
                validate_mount_path(path)?;
                scan.dirty_content.insert(path.clone());
            }
        }

        let plan = replay_plan(layout, checkpoint.as_ref(), &on_disk)?;
        let replay_generation = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.replay_log_generation)
            .unwrap_or_else(|| plan.first().copied().unwrap_or(0));
        let replay_offset = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.replay_log_offset)
            .unwrap_or(0);

        for (index, generation) in plan.iter().copied().enumerate() {
            let path = layout.log_path(generation);
            let start = if generation == replay_generation {
                replay_offset
            } else {
                0
            };
            let scanned = scan_generation(&mut scan, &path, generation, start)?;
            if scanned.last_complete_offset < scanned.file_len {
                if index + 1 != plan.len() {
                    bail!(
                        "mount WAL generation {generation} has a torn record at byte {} but is not \
                         the final generation",
                        scanned.last_complete_offset
                    );
                }
                tracing::warn!(
                    generation,
                    offset = scanned.last_complete_offset,
                    length = scanned.file_len,
                    "truncating a torn mount WAL tail"
                );
                truncate_torn_tail(&path, scanned.last_complete_offset, layout)?;
            }
        }

        let epoch = reconcile_epoch(scan.epoch.take(), checkpoint.as_ref(), expected_epoch)?;

        let mut acknowledged_sequence = scan.acknowledged_sequence;
        let mut remote_revision = scan.remote_revision;
        let mut tree_generation = 0;
        let mut checkpoint_id = 0;
        if let Some(checkpoint) = checkpoint.as_ref() {
            // Explicit comparison: the checkpoint and the log are written
            // independently, so "checkpoint is ahead" and "checkpoint agrees"
            // are separate cases and neither may silently win.
            if checkpoint.acknowledged_sequence > acknowledged_sequence {
                acknowledged_sequence = checkpoint.acknowledged_sequence;
                remote_revision = checkpoint.remote_revision;
            } else if checkpoint.acknowledged_sequence == acknowledged_sequence {
                remote_revision = remote_revision.max(checkpoint.remote_revision);
            }
            tree_generation = checkpoint.tree_generation;
            checkpoint_id = checkpoint.checkpoint_id;
        }
        scan.acknowledged_sequence = acknowledged_sequence;
        scan.remote_revision = remote_revision;
        validate_scan(&scan)?;

        // Anything the cursor already covers is resolved and published; the
        // in-memory index tracks the unacknowledged backlog only.
        let retain = scan.acknowledged_sequence.saturating_add(1);
        if scan.first_unpruned_sequence < retain {
            scan.prepared = scan.prepared.split_off(&retain);
            scan.committed = scan.committed.split_off(&retain);
            scan.aborted = scan.aborted.split_off(&retain);
            scan.first_unpruned_sequence = retain;
        }

        let mut pending_payload_bytes = 0_u64;
        for (sequence, record) in &scan.prepared {
            if !scan.committed.contains(sequence) {
                continue;
            }
            if let Some(payload) = record.event.payload.as_ref() {
                payloads.verify(payload).with_context(|| {
                    format!("verify the payload of committed mount sequence {sequence}")
                })?;
                pending_payload_bytes = pending_payload_bytes.saturating_add(payload.length);
            }
        }

        let next_sequence = scan
            .prepared
            .keys()
            .next_back()
            .copied()
            .unwrap_or(scan.acknowledged_sequence)
            .max(scan.acknowledged_sequence)
            .checked_add(1)
            .ok_or_else(|| anyhow!("mount WAL sequence exhausted"))?;

        let active_generation = plan.last().copied().unwrap_or(0);
        let log_path = layout.log_path(active_generation);
        let is_new_generation = !log_path.exists();
        let mut file = open_append(&log_path)?;
        if is_new_generation {
            append_record(
                &mut file,
                &WalRecord::LogHeader {
                    format_version: WAL_FORMAT_VERSION,
                    epoch: epoch.clone(),
                    generation: active_generation,
                    first_sequence: next_sequence,
                },
            )?;
            file.sync_all()
                .with_context(|| format!("sync mount WAL {}", log_path.display()))?;
            sync_directory(&layout.wal_dir())?;
        }
        let generation_len = file
            .metadata()
            .with_context(|| format!("stat mount WAL {}", log_path.display()))?
            .len();
        let sync_handle = Arc::new(
            file.try_clone()
                .with_context(|| format!("duplicate mount WAL handle {}", log_path.display()))?,
        );

        let mut active_generations: BTreeSet<u64> = plan.iter().copied().collect();
        active_generations.insert(active_generation);

        let wal = Self {
            inner: Arc::new(WalInner {
                layout: layout.clone(),
                payloads,
                epoch: epoch.clone(),
                notify: Arc::new(Notify::new()),
                append: Mutex::new(AppendState {
                    file,
                    sync_handle,
                    pending_sync: Vec::new(),
                    generation: active_generation,
                    generation_len,
                    appended_bytes: 0,
                    poisoned: None,
                    next_sequence,
                    prepared: scan.prepared,
                    committed: scan.committed,
                    aborted: scan.aborted,
                    acknowledged_sequence: scan.acknowledged_sequence,
                    remote_revision: scan.remote_revision,
                    first_unpruned_sequence: scan.first_unpruned_sequence.max(1),
                    tree_generation,
                    checkpoint_id,
                    active_generations,
                    dirty_content: scan.dirty_content,
                    pending_payload_bytes,
                    events_since_checkpoint: 0,
                    last_checkpoint: checkpoint,
                    reclaim_candidates: BTreeSet::new(),
                    compaction_pending: false,
                }),
                sync: Mutex::new(SyncState {
                    durable_sequence: next_sequence.saturating_sub(1),
                    durable_bytes: 0,
                    round: 0,
                    in_flight: false,
                }),
                sync_done: Condvar::new(),
                maintenance: Mutex::new(()),
                started: Instant::now(),
                pressure_sampled_at: AtomicU64::new(NEVER_SAMPLED),
                pressure_level: AtomicU8::new(encode_pressure(StoragePressure::None)),
            }),
        };
        // Re-anchor replay on the state we just proved, so the next open never
        // re-reads a prefix this one already validated.
        wal.checkpoint(tree_generation)?;
        Ok(wal)
    }

    pub(crate) fn epoch(&self) -> String {
        self.inner.epoch.clone()
    }

    pub(crate) fn layout(&self) -> &MountStateLayout {
        &self.inner.layout
    }

    pub(crate) fn payloads(&self) -> &PayloadStore {
        &self.inner.payloads
    }

    /// Handle the publisher waits on. One notify per commit is enough; the
    /// publisher coalesces.
    pub(crate) fn notify_handle(&self) -> WalNotify {
        Arc::clone(&self.inner.notify)
    }

    // -- mutation protocol ---------------------------------------------------

    /// Append the intent and capture its immutable payload.
    ///
    /// The payload is captured and the record is durable *before* the caller
    /// touches the backing tree. The returned token is what `commit` and `abort`
    /// require, so a mutation can never be terminated without its intent being
    /// on disk first.
    ///
    /// Payload capture happens with no append lock held; only sequence
    /// assignment and the framed `write_all` are inside it.
    pub(crate) fn prepare(
        &self,
        mutation: MountMutation,
        payload: PayloadSource<'_>,
        pre_image: MountPreImage,
    ) -> Result<PreparedEvent> {
        mutation.validate()?;
        if mutation.requires_payload() != payload.is_some() {
            bail!(
                "mount mutation payload mismatch: operation requires_payload={} supplied={}",
                mutation.requires_payload(),
                payload.is_some()
            );
        }
        // Capture, hash and copy with no append lock held.
        let payload = match payload {
            PayloadSource::None => None,
            PayloadSource::Bytes(bytes) => Some(self.inner.payloads.capture_bytes(bytes)?),
            PayloadSource::File(source) => Some(
                self.inner
                    .payloads
                    .capture_file(source, None)
                    .with_context(|| format!("capture mount payload from {}", source.display()))?,
            ),
        };

        let mut state = self.append_lock()?;
        let sequence = state.next_sequence;
        let event = MountEvent {
            format_version: WAL_FORMAT_VERSION,
            epoch: self.inner.epoch.clone(),
            sequence,
            idempotency_key: format!("{}:{sequence}", self.inner.epoch),
            mutation,
            payload,
            pre_image,
            local_identity: None,
        };
        let offset = append_framed(
            &mut state,
            &WalRecord::Prepared {
                event: event.clone(),
            },
        )?;
        let generation = state.generation;
        state.prepared.insert(
            sequence,
            PreparedRecord {
                event: event.clone(),
                generation,
                offset,
            },
        );
        state.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("mount WAL sequence exhausted"))?;
        Ok(PreparedEvent { event })
    }

    /// Make an applied mutation eligible for replay and publication.
    /// `local_identity` is the `dev:ino` the apply produced, when the mutation
    /// created or moved an entry.
    pub(crate) fn commit(
        &self,
        prepared: PreparedEvent,
        local_identity: Option<String>,
    ) -> Result<()> {
        {
            let mut state = self.append_lock()?;
            let sequence = prepared.event.sequence;
            let stored = state
                .prepared
                .get(&sequence)
                .ok_or_else(|| anyhow!("unknown prepared mount sequence {sequence}"))?;
            if stored.event != prepared.event {
                bail!("prepared mount event {sequence} does not match the durable record");
            }
            if state.aborted.contains(&sequence) {
                bail!("mount sequence {sequence} is already aborted");
            }
            if state.committed.contains(&sequence) {
                return Ok(());
            }
            append_framed(
                &mut state,
                &WalRecord::Committed {
                    epoch: self.inner.epoch.clone(),
                    sequence,
                    local_identity: local_identity.clone(),
                },
            )?;
            state.committed.insert(sequence);
            state.events_since_checkpoint = state.events_since_checkpoint.saturating_add(1);
            let payload_bytes = prepared.event.payload_length();
            state.pending_payload_bytes = state.pending_payload_bytes.saturating_add(payload_bytes);
            let mutation = prepared.event.mutation.clone();
            if let Some(record) = state.prepared.get_mut(&sequence) {
                record.event.local_identity = local_identity;
            }
            apply_committed_dirty_effect(&mut state.dirty_content, &mutation);
        }
        self.inner.notify.notify_one();
        Ok(())
    }

    /// Terminate a mutation whose local apply failed. An aborted event is never
    /// published and its sequence is folded into the next publishable prefix.
    pub(crate) fn abort(&self, prepared: PreparedEvent, reason: &str) -> Result<()> {
        {
            let mut state = self.append_lock()?;
            let sequence = prepared.event.sequence;
            let stored = state
                .prepared
                .get(&sequence)
                .ok_or_else(|| anyhow!("unknown prepared mount sequence {sequence}"))?;
            if stored.event != prepared.event {
                bail!("prepared mount event {sequence} does not match the durable record");
            }
            if state.committed.contains(&sequence) {
                bail!("mount sequence {sequence} is already committed");
            }
            if state.aborted.contains(&sequence) {
                return Ok(());
            }
            append_framed(
                &mut state,
                &WalRecord::Aborted {
                    epoch: self.inner.epoch.clone(),
                    sequence,
                    reason: reason.to_string(),
                },
            )?;
            state.aborted.insert(sequence);
            state.events_since_checkpoint = state.events_since_checkpoint.saturating_add(1);
        }
        self.inner.notify.notify_one();
        Ok(())
    }

    /// Record that a path's backing content diverged from its last published
    /// generation. Returns `true` when a durable record was appended, `false`
    /// when the path was already known dirty in this process.
    ///
    /// This is what makes a crash mid-generation converge rather than lose data:
    /// recovery re-seals every still-dirty path from the backing tree, which is
    /// exactly the state the guest already read back.
    pub(crate) fn record_content_dirty(&self, path: &str) -> Result<bool> {
        validate_mount_path(path)?;
        let mut state = self.append_lock()?;
        if state.dirty_content.contains(path) {
            return Ok(false);
        }
        append_framed(
            &mut state,
            &WalRecord::ContentDirty {
                epoch: self.inner.epoch.clone(),
                path: path.to_string(),
            },
        )?;
        state.dirty_content.insert(path.to_string());
        Ok(true)
    }

    /// Clear a path from the dirty set. Called by the seal path once the
    /// `ReplaceFile` for that generation has committed, and by remove/rename
    /// applies that make the old name meaningless.
    pub(crate) fn clear_content_dirty(&self, path: &str) -> Result<()> {
        let mut state = self.append_lock()?;
        state.dirty_content.remove(path);
        Ok(())
    }

    /// Retarget a dirty entry across a rename so the seal still finds it.
    pub(crate) fn rename_content_dirty(&self, old_path: &str, new_path: &str) -> Result<()> {
        validate_mount_path(old_path)?;
        validate_mount_path(new_path)?;
        let mut state = self.append_lock()?;
        retarget_subtree(&mut state.dirty_content, old_path, new_path);
        Ok(())
    }

    pub(crate) fn dirty_content_paths(&self) -> Result<Vec<String>> {
        Ok(self.append_lock()?.dirty_content.iter().cloned().collect())
    }

    // -- durability ----------------------------------------------------------

    /// Group sync through `sequence`. Returns the durable watermark, which may
    /// exceed the request because a concurrent leader's round covered more.
    ///
    /// Waiters coalesce on the in-flight round instead of each issuing their own
    /// device syncs; the leader performs the syncs with the append lock dropped.
    pub(crate) fn sync_through(&self, sequence: u64) -> Result<u64> {
        self.sync_watermark(sequence, 0)
    }

    /// Group sync through everything appended so far.
    pub(crate) fn sync_local(&self) -> Result<u64> {
        let (sequence, bytes) = {
            let state = self.append_lock()?;
            (state.next_sequence.saturating_sub(1), state.appended_bytes)
        };
        self.sync_watermark(sequence, bytes)
    }

    pub(crate) fn durable_sequence(&self) -> u64 {
        match self.inner.sync.lock() {
            Ok(state) => state.durable_sequence,
            Err(poisoned) => poisoned.into_inner().durable_sequence,
        }
    }

    // -- cursors -------------------------------------------------------------

    pub(crate) fn last_appended_sequence(&self) -> u64 {
        self.append_lock()
            .map(|state| state.next_sequence.saturating_sub(1))
            .unwrap_or(0)
    }

    pub(crate) fn last_committed_sequence(&self) -> u64 {
        self.append_lock()
            .map(|state| {
                state
                    .committed
                    .last()
                    .copied()
                    .unwrap_or(state.acknowledged_sequence)
            })
            .unwrap_or(0)
    }

    pub(crate) fn acknowledged_sequence(&self) -> u64 {
        self.append_lock()
            .map(|state| state.acknowledged_sequence)
            .unwrap_or(0)
    }

    pub(crate) fn remote_revision(&self) -> u64 {
        self.append_lock()
            .map(|state| state.remote_revision)
            .unwrap_or(0)
    }

    /// Events committed but not yet acknowledged.
    pub(crate) fn pending_publication_depth(&self) -> u64 {
        self.append_lock()
            .map(|state| state.committed.len() as u64)
            .unwrap_or(0)
    }

    pub(crate) fn pending_payload_bytes(&self) -> u64 {
        self.append_lock()
            .map(|state| state.pending_payload_bytes)
            .unwrap_or(0)
    }

    /// Terminal records appended since the last checkpoint. The maintenance task
    /// compares it against `CHECKPOINT_EVENT_INTERVAL`; the time trigger is its
    /// own concern.
    pub(crate) fn events_since_checkpoint(&self) -> u64 {
        self.append_lock()
            .map(|state| state.events_since_checkpoint)
            .unwrap_or(0)
    }

    // -- publication ---------------------------------------------------------

    /// The next contiguous committed prefix, bounded by event count and payload
    /// bytes. Stops at the first sequence that is missing or still unresolved,
    /// which is how one permanently failed event preserves and blocks its own
    /// WAL suffix. Aborted sequences inside the prefix advance
    /// `through_sequence` without producing an event.
    ///
    /// A single event larger than `max_payload_bytes` is still returned alone,
    /// so the cursor can always make progress.
    pub(crate) fn next_publish_batch(
        &self,
        max_events: usize,
        max_payload_bytes: u64,
    ) -> Result<Option<PublishBatch>> {
        if max_events == 0 {
            bail!("mount publication batch must allow at least one event");
        }
        let state = self.append_lock()?;
        let mut sequence = state.acknowledged_sequence.saturating_add(1);
        let mut through_sequence = state.acknowledged_sequence;
        let mut payload_bytes = 0_u64;
        let mut events: Vec<MountEvent> = Vec::new();
        loop {
            if state.aborted.contains(&sequence) {
                through_sequence = sequence;
                sequence = sequence.saturating_add(1);
                continue;
            }
            let Some(record) = state.prepared.get(&sequence) else {
                break;
            };
            if !state.committed.contains(&sequence) {
                break;
            }
            let event_bytes = record.event.payload_length();
            if !events.is_empty()
                && (events.len() >= max_events
                    || payload_bytes.saturating_add(event_bytes) > max_payload_bytes)
            {
                break;
            }
            payload_bytes = payload_bytes.saturating_add(event_bytes);
            events.push(record.event.clone());
            through_sequence = sequence;
            sequence = sequence.saturating_add(1);
        }
        if through_sequence == state.acknowledged_sequence {
            Ok(None)
        } else {
            Ok(Some(PublishBatch {
                events,
                through_sequence,
            }))
        }
    }

    /// Advance the remote cursor and write a checkpoint.
    ///
    /// Rejects a regression, requires every sequence in the range to be resolved,
    /// and never mutates anything the local view serves. Pruning the in-memory
    /// index and scheduling compaction happen here, not on a callback.
    pub(crate) fn acknowledge(
        &self,
        through_sequence: u64,
        remote_revision: u64,
        tree_generation: u64,
    ) -> Result<()> {
        {
            let mut state = self.append_lock()?;
            if through_sequence < state.acknowledged_sequence {
                bail!(
                    "mount acknowledgement regressed from {} to {through_sequence}",
                    state.acknowledged_sequence
                );
            }
            if remote_revision < state.remote_revision {
                bail!(
                    "mount remote revision regressed from {} to {remote_revision}",
                    state.remote_revision
                );
            }
            if through_sequence == state.acknowledged_sequence
                && remote_revision == state.remote_revision
            {
                return Ok(());
            }
            for sequence in state.acknowledged_sequence.saturating_add(1)..=through_sequence {
                if !(state.committed.contains(&sequence) || state.aborted.contains(&sequence)) {
                    bail!("cannot acknowledge unresolved mount sequence {sequence}");
                }
            }
            append_framed(
                &mut state,
                &WalRecord::RemoteAcknowledged {
                    epoch: self.inner.epoch.clone(),
                    through_sequence,
                    remote_revision,
                },
            )?;
            state.acknowledged_sequence = through_sequence;
            state.remote_revision = remote_revision;
            prune_resolved(&mut state, through_sequence);
            state.compaction_pending = true;
        }
        // The acknowledgement record must be durable before the checkpoint that
        // asserts it, and the checkpoint must be durable before compaction can
        // treat anything below it as unreachable.
        self.sync_local()?;
        self.checkpoint(tree_generation)
    }

    // -- recovery ------------------------------------------------------------

    /// What replay found. `mount.rs` resolves the unresolved prepares against the
    /// backing tree before the mount serves a callback.
    pub(crate) fn recovery_state(&self) -> Result<RecoveryState> {
        let state = self.append_lock()?;
        let mut committed_unacknowledged = Vec::new();
        let mut unresolved_prepares = Vec::new();
        for (sequence, record) in state
            .prepared
            .range(state.acknowledged_sequence.saturating_add(1)..)
        {
            if state.committed.contains(sequence) {
                committed_unacknowledged.push(record.event.clone());
            } else if !state.aborted.contains(sequence) {
                unresolved_prepares.push(record.event.clone());
            }
        }
        Ok(RecoveryState {
            epoch: self.inner.epoch.clone(),
            acknowledged_sequence: state.acknowledged_sequence,
            remote_revision: state.remote_revision,
            tree_generation: state.tree_generation,
            committed_unacknowledged,
            unresolved_prepares,
            dirty_content_paths: state.dirty_content.iter().cloned().collect(),
        })
    }

    /// Terminate one recovered prepare with the resolution `mount.rs` derived
    /// from the backing tree.
    pub(crate) fn resolve_recovered(
        &self,
        sequence: u64,
        resolution: RecoveryResolution,
    ) -> Result<()> {
        {
            let mut state = self.append_lock()?;
            let record = state
                .prepared
                .get(&sequence)
                .ok_or_else(|| anyhow!("unknown recovered mount sequence {sequence}"))?;
            if state.committed.contains(&sequence) || state.aborted.contains(&sequence) {
                bail!("recovered mount sequence {sequence} is already resolved");
            }
            let mutation = record.event.mutation.clone();
            let payload_bytes = record.event.payload_length();
            match resolution {
                RecoveryResolution::Commit => {
                    append_framed(
                        &mut state,
                        &WalRecord::Committed {
                            epoch: self.inner.epoch.clone(),
                            sequence,
                            local_identity: None,
                        },
                    )?;
                    state.committed.insert(sequence);
                    state.pending_payload_bytes =
                        state.pending_payload_bytes.saturating_add(payload_bytes);
                    apply_committed_dirty_effect(&mut state.dirty_content, &mutation);
                }
                RecoveryResolution::Abort => {
                    append_framed(
                        &mut state,
                        &WalRecord::Aborted {
                            epoch: self.inner.epoch.clone(),
                            sequence,
                            reason: "recovery proved the mutation was never applied".to_string(),
                        },
                    )?;
                    state.aborted.insert(sequence);
                }
            }
            state.events_since_checkpoint = state.events_since_checkpoint.saturating_add(1);
        }
        self.inner.notify.notify_one();
        Ok(())
    }

    // -- payloads ------------------------------------------------------------

    pub(crate) fn open_payload(&self, payload: &PayloadRef) -> Result<PayloadReader> {
        self.inner.payloads.open_reader(payload)
    }

    pub(crate) fn payload_bytes(&self, payload: &PayloadRef) -> Result<Vec<u8>> {
        self.inner.payloads.read_all(payload)
    }

    pub(crate) fn payload_path(&self, payload: &PayloadRef) -> Option<PathBuf> {
        self.inner.payloads.dedicated_path(payload)
    }

    // -- maintenance ---------------------------------------------------------

    /// Publish a checkpoint without an acknowledgement. Runs on the maintenance
    /// task on a time or event-count trigger, never on a FUSE callback.
    pub(crate) fn checkpoint(&self, tree_generation: u64) -> Result<()> {
        let _maintenance = self.maintenance_lock()?;
        self.checkpoint_locked(tree_generation)
    }

    /// Rotate the active log generation when it exceeds `LOG_TARGET_BYTES`.
    pub(crate) fn rotate_if_needed(&self) -> Result<bool> {
        let _maintenance = self.maintenance_lock()?;
        let (generation, first_sequence) = {
            let state = self.append_lock()?;
            if state.generation_len < LOG_TARGET_BYTES {
                return Ok(false);
            }
            (state.generation, state.next_sequence)
        };
        let next_generation = generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("mount WAL log generation exhausted"))?;
        let path = self.inner.layout.log_path(next_generation);

        // Create, header and sync the new generation with no append lock held.
        let mut file = create_new(&path)?;
        append_record(
            &mut file,
            &WalRecord::LogHeader {
                format_version: WAL_FORMAT_VERSION,
                epoch: self.inner.epoch.clone(),
                generation: next_generation,
                first_sequence,
            },
        )?;
        file.sync_all()
            .with_context(|| format!("sync mount WAL {}", path.display()))?;
        let generation_len = file
            .metadata()
            .with_context(|| format!("stat mount WAL {}", path.display()))?
            .len();
        sync_directory(&self.inner.layout.wal_dir())?;
        let sync_handle = Arc::new(
            file.try_clone()
                .with_context(|| format!("duplicate mount WAL handle {}", path.display()))?,
        );

        {
            let mut state = self.append_lock()?;
            if state.generation != generation {
                drop(file);
                let _ = std::fs::remove_file(&path);
                return Ok(false);
            }
            let previous = std::mem::replace(&mut state.sync_handle, sync_handle);
            // The rotated-away tail is not proven durable yet; the next sync
            // round covers it before the new active generation.
            state.pending_sync.push(previous);
            state.file = file;
            state.generation = next_generation;
            state.generation_len = generation_len;
            state.active_generations.insert(next_generation);
            state.poisoned = None;
            // A sealed generation below the new replay floor may now be
            // reclaimable.
            state.compaction_pending = true;
        }
        // Keep the declared generation set current, so recovery never has to
        // guess about a generation created after the last checkpoint.
        let tree_generation = self.append_lock()?.tree_generation;
        self.checkpoint_locked(tree_generation)?;
        Ok(true)
    }

    /// Delete log generations and payload files that neither the current
    /// checkpoint nor the remote cursor can still reach.
    pub(crate) fn compact(&self) -> Result<CompactionOutcome> {
        let _maintenance = self.maintenance_lock()?;
        let (floor_generation, candidates, referenced, protected) = {
            let mut state = self.append_lock()?;
            // Nothing has become unreachable since the last pass, so touch no
            // filesystem at all: compaction is background work, never a poll.
            if !state.compaction_pending && state.reclaim_candidates.is_empty() {
                return Ok(CompactionOutcome::default());
            }
            state.compaction_pending = false;
            let floor = state
                .last_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.replay_log_generation)
                .unwrap_or(state.generation);
            let candidates = std::mem::take(&mut state.reclaim_candidates);
            let referenced: BTreeSet<String> = state
                .prepared
                .values()
                .filter_map(|record| record.event.payload.as_ref())
                .map(|payload| payload.storage.file().to_string())
                .collect();
            let protected: BTreeSet<String> = state
                .last_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.active_segments.iter().cloned().collect())
                .unwrap_or_default();
            (floor, candidates, referenced, protected)
        };

        let mut outcome = CompactionOutcome::default();
        for generation in self.inner.layout.log_generations()? {
            if generation >= floor_generation {
                continue;
            }
            let path = self.inner.layout.log_path(generation);
            let bytes = std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
            std::fs::remove_file(&path)
                .with_context(|| format!("remove reclaimed mount WAL {}", path.display()))?;
            outcome.removed_log_generations = outcome.removed_log_generations.saturating_add(1);
            outcome.reclaimed_bytes = outcome.reclaimed_bytes.saturating_add(bytes);
        }
        if outcome.removed_log_generations > 0 {
            sync_directory(&self.inner.layout.wal_dir())?;
            let mut state = self.append_lock()?;
            state
                .active_generations
                .retain(|generation| *generation >= floor_generation);
        }

        if !candidates.is_empty() {
            let live = self.inner.payloads.live_files()?;
            let mut unreachable = BTreeSet::new();
            let mut deferred = BTreeSet::new();
            for name in candidates {
                if referenced.contains(&name) || protected.contains(&name) {
                    deferred.insert(name);
                } else if live.contains(&name) {
                    unreachable.insert(name);
                }
            }
            if !deferred.is_empty() {
                let mut state = self.append_lock()?;
                state.reclaim_candidates.extend(deferred);
            }
            if !unreachable.is_empty() {
                let retain: BTreeSet<String> = live.difference(&unreachable).cloned().collect();
                outcome.removed_payload_files = unreachable.len() as u64;
                outcome.reclaimed_bytes = outcome
                    .reclaimed_bytes
                    .saturating_add(self.inner.payloads.reclaim(&retain)?);
            }
        }
        Ok(outcome)
    }

    /// Local durable-storage pressure derived from unreclaimed WAL + payload
    /// bytes and from the backing filesystem's free space.
    ///
    /// Every content-bearing mutation consults this, and measuring it walks two
    /// directories plus a `statvfs`, so the measurement is sampled on an
    /// interval and served from a cached level in between. A racing double
    /// sample is harmless; a per-write `readdir` would not be.
    pub(crate) fn storage_pressure(&self) -> StoragePressure {
        let now = self.inner.started.elapsed().as_millis() as u64;
        let sampled_at = self.inner.pressure_sampled_at.load(Ordering::Relaxed);
        if sampled_at != NEVER_SAMPLED
            && now.saturating_sub(sampled_at) < STORAGE_PRESSURE_REFRESH_MS
        {
            return decode_pressure(self.inner.pressure_level.load(Ordering::Relaxed));
        }
        self.inner.pressure_sampled_at.store(now, Ordering::Relaxed);
        let level = self.measure_storage_pressure();
        self.inner
            .pressure_level
            .store(encode_pressure(level), Ordering::Relaxed);
        level
    }

    fn measure_storage_pressure(&self) -> StoragePressure {
        let payload_bytes = match self.inner.payloads.usage() {
            Ok(usage) => usage.total_bytes(),
            Err(error) => {
                tracing::warn!(%error, "mount payload usage is unavailable");
                0
            }
        };
        let log_bytes = self.log_bytes().unwrap_or(0);
        let total = log_bytes.saturating_add(payload_bytes);
        if total >= WAL_HARD_LIMIT_BYTES {
            return StoragePressure::Hard;
        }
        if let Some(free) = free_fraction(self.inner.layout.root()) {
            if free < BACKING_FREE_FRACTION_FLOOR {
                return StoragePressure::Hard;
            }
        }
        if total >= WAL_SOFT_LIMIT_BYTES {
            StoragePressure::Soft
        } else {
            StoragePressure::None
        }
    }

    /// Seal the log for shutdown: flush the active segment, sync everything, and
    /// publish a final checkpoint. Does not wait for the publisher.
    pub(crate) fn seal(&self, tree_generation: u64) -> Result<()> {
        self.sync_local()?;
        self.inner.payloads.seal_active_segment()?;
        self.checkpoint(tree_generation)
    }

    // -- internals -----------------------------------------------------------

    fn append_lock(&self) -> Result<MutexGuard<'_, AppendState>> {
        self.inner
            .append
            .lock()
            .map_err(|_| anyhow!("mount WAL append lock poisoned"))
    }

    fn maintenance_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.inner
            .maintenance
            .lock()
            .map_err(|_| anyhow!("mount WAL maintenance lock poisoned"))
    }

    /// The group-commit protocol. Returns once both watermarks cover the
    /// request, leading a round only when no other caller already is.
    fn sync_watermark(&self, sequence: u64, bytes: u64) -> Result<u64> {
        loop {
            let mut state = self
                .inner
                .sync
                .lock()
                .map_err(|_| anyhow!("mount WAL sync lock poisoned"))?;
            if state.durable_sequence >= sequence && state.durable_bytes >= bytes {
                return Ok(state.durable_sequence);
            }
            if state.in_flight {
                let round = state.round;
                while state.round == round {
                    state = self
                        .inner
                        .sync_done
                        .wait(state)
                        .map_err(|_| anyhow!("mount WAL sync lock poisoned"))?;
                }
                if state.durable_sequence >= sequence && state.durable_bytes >= bytes {
                    return Ok(state.durable_sequence);
                }
                // The round we joined either failed or started before our
                // record; lead one ourselves rather than reporting durability
                // we do not have.
                continue;
            }
            state.in_flight = true;
            drop(state);

            let outcome = self.run_sync_round();
            let mut state = self
                .inner
                .sync
                .lock()
                .map_err(|_| anyhow!("mount WAL sync lock poisoned"))?;
            state.in_flight = false;
            state.round = state.round.wrapping_add(1);
            let result = match outcome {
                Ok((durable_sequence, durable_bytes)) => {
                    state.durable_sequence = state.durable_sequence.max(durable_sequence);
                    state.durable_bytes = state.durable_bytes.max(durable_bytes);
                    Ok(state.durable_sequence)
                }
                Err(error) => Err(error),
            };
            drop(state);
            self.inner.sync_done.notify_all();
            return result;
        }
    }

    /// One leader round: payloads first, then every log generation with pending
    /// bytes. Nothing here holds the append lock across a device sync.
    fn run_sync_round(&self) -> Result<(u64, u64)> {
        let (sequence, bytes, handles) = {
            let mut state = self.append_lock()?;
            let mut handles: Vec<Arc<File>> = state.pending_sync.drain(..).collect();
            handles.push(Arc::clone(&state.sync_handle));
            (
                state.next_sequence.saturating_sub(1),
                state.appended_bytes,
                handles,
            )
        };

        // A payload must be durable before the record that references it, or a
        // crash between the two would fail the next open closed.
        let dirty = self.inner.payloads.take_dirty()?;
        if !dirty.is_empty() {
            if let Err(error) = self.inner.payloads.sync_dirty(&dirty) {
                self.inner.payloads.restore_dirty(dirty)?;
                self.restore_pending_sync(handles);
                return Err(error);
            }
        }
        for (index, handle) in handles.iter().enumerate() {
            if let Err(error) = handle.sync_data() {
                self.restore_pending_sync(handles[index..].to_vec());
                return Err(anyhow::Error::new(error).context("sync mount WAL generation"));
            }
        }
        Ok((sequence, bytes))
    }

    /// Put rotated-away generation handles back so a failed round retries them.
    fn restore_pending_sync(&self, mut handles: Vec<Arc<File>>) {
        if handles.is_empty() {
            return;
        }
        // The last handle is always the active generation, which the state
        // already owns.
        handles.pop();
        if handles.is_empty() {
            return;
        }
        if let Ok(mut state) = self.append_lock() {
            let mut restored = handles;
            restored.append(&mut state.pending_sync);
            state.pending_sync = restored;
        }
    }

    fn checkpoint_locked(&self, tree_generation: u64) -> Result<()> {
        self.sync_local()?;
        self.inner.payloads.seal_active_segment()?;

        let (checkpoint, previous) = {
            let mut state = self.append_lock()?;
            state.tree_generation = tree_generation;
            state.checkpoint_id = state.checkpoint_id.saturating_add(1);
            // The oldest retained prepare is also the oldest record replay can
            // still need: sequences are assigned and appended in one order, and
            // a terminal record never precedes its own prepare.
            let (replay_log_generation, replay_log_offset) = match state.prepared.values().next() {
                Some(record) => (record.generation, record.offset),
                None => (state.generation, state.generation_len),
            };
            let mut active_log_generations: BTreeSet<u64> = state
                .active_generations
                .iter()
                .copied()
                .filter(|generation| *generation >= replay_log_generation)
                .collect();
            active_log_generations.insert(state.generation);
            active_log_generations.insert(replay_log_generation);
            let active_segments: BTreeSet<String> = state
                .prepared
                .values()
                .filter_map(|record| record.event.payload.as_ref())
                .map(|payload| payload.storage.file().to_string())
                .collect();

            let checkpoint = MountCheckpoint {
                format_version: WAL_FORMAT_VERSION,
                epoch: self.inner.epoch.clone(),
                checkpoint_id: state.checkpoint_id,
                acknowledged_sequence: state.acknowledged_sequence,
                remote_revision: state.remote_revision,
                tree_generation,
                first_unpruned_sequence: state.first_unpruned_sequence.max(1),
                replay_log_generation,
                replay_log_offset,
                active_log_generations: active_log_generations.iter().copied().collect(),
                active_segments: active_segments.iter().cloned().collect(),
                dirty_content_paths: state.dirty_content.iter().cloned().collect(),
            };
            checkpoint.validate()?;

            // Only a segment a durable checkpoint has stopped naming can ever
            // become reclaimable; anything captured since is invisible here and
            // therefore safe from compaction.
            let stale: Vec<String> = state
                .last_checkpoint
                .as_ref()
                .map(|previous| {
                    previous
                        .active_segments
                        .iter()
                        .filter(|name| !active_segments.contains(*name))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            state.reclaim_candidates.extend(stale);
            state.active_generations = active_log_generations;
            state.events_since_checkpoint = 0;
            let previous = state.last_checkpoint.replace(checkpoint.clone());
            (checkpoint, previous)
        };

        if let Some(previous) = previous {
            write_json_atomic(&self.inner.layout.previous_checkpoint_path(), &previous)?;
        }
        write_json_atomic(&self.inner.layout.checkpoint_path(), &checkpoint)?;

        // Appended after the file is durable, so the recorded replay offset can
        // never point past the end of the log.
        let mut state = self.append_lock()?;
        append_framed(
            &mut state,
            &WalRecord::Checkpointed {
                epoch: self.inner.epoch.clone(),
                through_sequence: checkpoint.acknowledged_sequence,
                tree_generation,
            },
        )?;
        Ok(())
    }

    fn log_bytes(&self) -> Result<u64> {
        let mut total = 0_u64;
        for generation in self.inner.layout.log_generations()? {
            let path = self.inner.layout.log_path(generation);
            if let Ok(metadata) = std::fs::metadata(&path) {
                total = total.saturating_add(metadata.len());
            }
        }
        Ok(total)
    }
}

/// Serialize one record and append it with a single `write_all`.
///
/// Framing must be built into an owned buffer first: a partial write inside a
/// record is unrecoverable, so on failure the caller truncates back to the last
/// known-good offset and poisons the log for this generation rather than leaving
/// garbage the next `open` would fail closed on forever.
pub(crate) fn append_record(file: &mut File, record: &WalRecord) -> Result<()> {
    let mut buffer = serde_json::to_vec(record).context("encode mount WAL record")?;
    buffer.push(b'\n');
    if buffer.len() > MAX_RECORD_BYTES {
        bail!(
            "mount WAL record of {} bytes exceeds the {MAX_RECORD_BYTES} byte limit",
            buffer.len()
        );
    }
    file.write_all(&buffer).context("append mount WAL record")
}

/// Append inside the critical section, tracking the last known-good offset so a
/// failed write can be rolled back instead of bricking every future `open`.
fn append_framed(state: &mut AppendState, record: &WalRecord) -> Result<u64> {
    if let Some(reason) = state.poisoned.as_ref() {
        bail!(
            "mount WAL generation {} is poisoned: {reason}",
            state.generation
        );
    }
    let offset = state.generation_len;
    match append_record(&mut state.file, record) {
        Ok(()) => {
            let length = state
                .file
                .metadata()
                .context("stat mount WAL generation")?
                .len();
            state.appended_bytes = state
                .appended_bytes
                .saturating_add(length.saturating_sub(offset));
            state.generation_len = length;
            Ok(offset)
        }
        Err(error) => {
            let written = state
                .file
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or(offset);
            if written <= offset {
                // Nothing landed; the log is still exactly as long as the last
                // complete record left it and the caller may retry.
                return Err(error);
            }
            let rollback = state
                .file
                .set_len(offset)
                .and_then(|()| state.file.sync_all());
            match rollback {
                Ok(()) => {
                    state.generation_len = offset;
                    Err(error)
                        .context("rolled a partial mount WAL record back to its last good offset")
                }
                Err(rollback) => {
                    state.poisoned = Some(format!(
                        "partial record at byte {offset} could not be rolled back: {rollback}"
                    ));
                    Err(error).context("mount WAL retains a partial record and is poisoned")
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ScanState {
    epoch: Option<String>,
    prepared: BTreeMap<u64, PreparedRecord>,
    committed: BTreeSet<u64>,
    aborted: BTreeSet<u64>,
    acknowledged_sequence: u64,
    remote_revision: u64,
    dirty_content: BTreeSet<String>,
    first_unpruned_sequence: u64,
}

struct ScannedGeneration {
    last_complete_offset: u64,
    file_len: u64,
}

/// The generations replay must open: the ones the checkpoint declares from its
/// replay point forward, plus any generation created after that checkpoint was
/// written. Pruned generations below the replay point are never opened.
fn replay_plan(
    layout: &MountStateLayout,
    checkpoint: Option<&MountCheckpoint>,
    on_disk: &[u64],
) -> Result<Vec<u64>> {
    let mut plan: Vec<u64> = match checkpoint {
        Some(checkpoint) => {
            let mut declared: Vec<u64> = checkpoint
                .active_log_generations
                .iter()
                .copied()
                .filter(|generation| *generation >= checkpoint.replay_log_generation)
                .collect();
            declared.sort_unstable();
            declared.dedup();
            for generation in &declared {
                if !layout.log_path(*generation).exists() {
                    bail!(
                        "mount WAL generation {generation} named by the checkpoint is missing from {}",
                        layout.wal_dir().display()
                    );
                }
            }
            let highest = declared
                .last()
                .copied()
                .unwrap_or(checkpoint.replay_log_generation);
            for generation in on_disk {
                if *generation > highest {
                    declared.push(*generation);
                }
            }
            declared
        }
        None => on_disk.to_vec(),
    };
    plan.sort_unstable();
    plan.dedup();
    Ok(plan)
}

fn scan_generation(
    scan: &mut ScanState,
    path: &Path,
    generation: u64,
    start_offset: u64,
) -> Result<ScannedGeneration> {
    let file = File::open(path).with_context(|| format!("open mount WAL {}", path.display()))?;
    let file_len = file
        .metadata()
        .with_context(|| format!("stat mount WAL {}", path.display()))?
        .len();
    if start_offset > file_len {
        bail!(
            "mount checkpoint replay offset {start_offset} is past the end of {}",
            path.display()
        );
    }
    let mut reader = BufReader::new(file);
    if start_offset > 0 {
        reader
            .seek(SeekFrom::Start(start_offset))
            .with_context(|| format!("seek mount WAL {}", path.display()))?;
    }
    let mut offset = start_offset;
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        let read = reader
            .read_until(b'\n', &mut buffer)
            .with_context(|| format!("read mount WAL {}", path.display()))?;
        if read == 0 {
            break;
        }
        if buffer.last() != Some(&b'\n') {
            // Torn final append: the caller truncates back to `offset`.
            break;
        }
        if buffer.len() > MAX_RECORD_BYTES {
            bail!(
                "mount WAL record at byte {offset} of {} exceeds {MAX_RECORD_BYTES} bytes",
                path.display()
            );
        }
        let record: WalRecord = serde_json::from_slice(&buffer)
            .with_context(|| format!("decode mount WAL {} at byte {offset}", path.display()))?;
        apply_scanned_record(scan, record, generation, offset)?;
        offset = offset.saturating_add(read as u64);
    }
    Ok(ScannedGeneration {
        last_complete_offset: offset,
        file_len,
    })
}

fn apply_scanned_record(
    scan: &mut ScanState,
    record: WalRecord,
    generation: u64,
    offset: u64,
) -> Result<()> {
    match record {
        WalRecord::LogHeader {
            format_version,
            epoch,
            generation: recorded,
            first_sequence: _,
        } => {
            if format_version != WAL_FORMAT_VERSION {
                bail!("unsupported mount WAL header format {format_version}");
            }
            observe_epoch(&mut scan.epoch, &epoch)?;
            if recorded != generation {
                bail!(
                    "mount WAL generation {generation} carries a header for generation {recorded}"
                );
            }
        }
        WalRecord::Prepared { event } => {
            if event.format_version != WAL_FORMAT_VERSION {
                bail!(
                    "unsupported mount WAL event format {}",
                    event.format_version
                );
            }
            event.mutation.validate()?;
            if event.mutation.requires_payload() != event.payload.is_some() {
                bail!(
                    "mount WAL sequence {} has an invalid payload shape",
                    event.sequence
                );
            }
            observe_epoch(&mut scan.epoch, &event.epoch)?;
            if event.sequence >= scan.first_unpruned_sequence {
                let sequence = event.sequence;
                if scan
                    .prepared
                    .insert(
                        sequence,
                        PreparedRecord {
                            event,
                            generation,
                            offset,
                        },
                    )
                    .is_some()
                {
                    bail!("duplicate mount WAL prepare for sequence {sequence}");
                }
            }
        }
        WalRecord::Committed {
            epoch,
            sequence,
            local_identity,
        } => {
            observe_epoch(&mut scan.epoch, &epoch)?;
            if sequence >= scan.first_unpruned_sequence {
                if scan.aborted.contains(&sequence) {
                    bail!("mount WAL commits already aborted sequence {sequence}");
                }
                if !scan.committed.insert(sequence) {
                    bail!("duplicate mount WAL commit for sequence {sequence}");
                }
                if let Some(record) = scan.prepared.get_mut(&sequence) {
                    record.event.local_identity = local_identity;
                    let mutation = record.event.mutation.clone();
                    apply_committed_dirty_effect(&mut scan.dirty_content, &mutation);
                }
            }
        }
        WalRecord::Aborted {
            epoch, sequence, ..
        } => {
            observe_epoch(&mut scan.epoch, &epoch)?;
            if sequence >= scan.first_unpruned_sequence {
                if scan.committed.contains(&sequence) {
                    bail!("mount WAL aborts already committed sequence {sequence}");
                }
                if !scan.aborted.insert(sequence) {
                    bail!("duplicate mount WAL abort for sequence {sequence}");
                }
            }
        }
        WalRecord::ContentDirty { epoch, path } => {
            observe_epoch(&mut scan.epoch, &epoch)?;
            validate_mount_path(&path)?;
            scan.dirty_content.insert(path);
        }
        WalRecord::RemoteAcknowledged {
            epoch,
            through_sequence,
            remote_revision,
        } => {
            observe_epoch(&mut scan.epoch, &epoch)?;
            if through_sequence < scan.acknowledged_sequence
                || remote_revision < scan.remote_revision
            {
                bail!("mount WAL acknowledgement regressed");
            }
            scan.acknowledged_sequence = through_sequence;
            scan.remote_revision = remote_revision;
        }
        WalRecord::Checkpointed { epoch, .. } => {
            observe_epoch(&mut scan.epoch, &epoch)?;
        }
    }
    Ok(())
}

/// Every terminal record must have its prepare, and every acknowledged sequence
/// that was not pruned must be resolved. The scan deliberately starts at
/// `first_unpruned_sequence`: demanding a record for sequence 1 forever is what
/// made the previous single-file log impossible to compact.
fn validate_scan(scan: &ScanState) -> Result<()> {
    for sequence in scan.committed.iter().chain(scan.aborted.iter()) {
        if !scan.prepared.contains_key(sequence) {
            bail!("mount WAL terminal record has no prepare for sequence {sequence}");
        }
    }
    let first = scan.first_unpruned_sequence.max(1);
    for sequence in first..=scan.acknowledged_sequence {
        if !(scan.committed.contains(&sequence) || scan.aborted.contains(&sequence)) {
            bail!("mount WAL acknowledged unresolved sequence {sequence}");
        }
    }
    Ok(())
}

fn observe_epoch(current: &mut Option<String>, epoch: &str) -> Result<()> {
    match current {
        Some(current) if current != epoch => {
            bail!("mount WAL contains multiple ownership epochs")
        }
        Some(_) => Ok(()),
        None => {
            *current = Some(epoch.to_string());
            Ok(())
        }
    }
}

fn reconcile_epoch(
    scanned: Option<String>,
    checkpoint: Option<&MountCheckpoint>,
    expected: Option<&str>,
) -> Result<String> {
    let durable = match (
        scanned,
        checkpoint.map(|checkpoint| checkpoint.epoch.clone()),
    ) {
        (Some(scanned), Some(recorded)) if scanned != recorded => {
            bail!("mount WAL epoch {scanned} does not match checkpoint epoch {recorded}")
        }
        (Some(scanned), _) => Some(scanned),
        (None, Some(recorded)) => Some(recorded),
        (None, None) => None,
    };
    match (durable, expected) {
        (Some(durable), Some(expected)) if durable != expected => {
            bail!("mount state directory carries epoch {durable} but the caller holds {expected}")
        }
        (Some(durable), _) => Ok(durable),
        (None, Some(expected)) if !expected.is_empty() => Ok(expected.to_string()),
        (None, _) => Ok(Uuid::now_v7().to_string()),
    }
}

fn truncate_torn_tail(path: &Path, offset: u64, layout: &MountStateLayout) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .with_context(|| format!("open mount WAL {} for repair", path.display()))?;
    file.set_len(offset)
        .with_context(|| format!("truncate torn mount WAL {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync repaired mount WAL {}", path.display()))?;
    sync_directory(&layout.wal_dir())
}

// ---------------------------------------------------------------------------
// Index maintenance
// ---------------------------------------------------------------------------

/// Drop every resolved sequence at or below the cursor. Pruning is what makes
/// the in-memory index proportional to the unacknowledged backlog rather than to
/// the lifetime of the mount, and it is what makes log compaction legal.
fn prune_resolved(state: &mut AppendState, through_sequence: u64) {
    let retain = through_sequence.saturating_add(1);
    let kept_committed = state.committed.split_off(&retain);
    let removed_committed = std::mem::replace(&mut state.committed, kept_committed);
    let kept_aborted = state.aborted.split_off(&retain);
    let _removed_aborted = std::mem::replace(&mut state.aborted, kept_aborted);
    let kept_prepared = state.prepared.split_off(&retain);
    let removed_prepared = std::mem::replace(&mut state.prepared, kept_prepared);

    for sequence in &removed_committed {
        if let Some(record) = removed_prepared.get(sequence) {
            state.pending_payload_bytes = state
                .pending_payload_bytes
                .saturating_sub(record.event.payload_length());
        }
    }
    state.first_unpruned_sequence = retain;
}

/// A committed mutation's effect on the content dirty set. Applied both when a
/// callback commits and when replay reconstructs the set, so recovery re-seals
/// exactly the paths whose backing bytes the guest could still read back.
fn apply_committed_dirty_effect(dirty: &mut BTreeSet<String>, mutation: &MountMutation) {
    match mutation {
        MountMutation::ReplaceFile { path, .. } | MountMutation::RemoveFile { path, .. } => {
            dirty.remove(path);
        }
        MountMutation::RemoveDirectory { path } => {
            remove_subtree(dirty, path);
        }
        MountMutation::Rename {
            old_path, new_path, ..
        } => {
            retarget_subtree(dirty, old_path, new_path);
        }
        _ => {}
    }
}

fn remove_subtree(dirty: &mut BTreeSet<String>, prefix: &str) {
    let matched: Vec<String> = dirty
        .iter()
        .filter(|path| is_at_or_below(path, prefix))
        .cloned()
        .collect();
    for path in matched {
        dirty.remove(&path);
    }
}

fn retarget_subtree(dirty: &mut BTreeSet<String>, old_prefix: &str, new_prefix: &str) {
    let matched: Vec<String> = dirty
        .iter()
        .filter(|path| is_at_or_below(path, old_prefix))
        .cloned()
        .collect();
    for path in matched {
        dirty.remove(&path);
        let suffix = &path[old_prefix.len()..];
        dirty.insert(format!("{new_prefix}{suffix}"));
    }
}

fn is_at_or_below(path: &str, prefix: &str) -> bool {
    path == prefix
        || (path.len() > prefix.len()
            && path.starts_with(prefix)
            && path.as_bytes()[prefix.len()] == b'/')
}

// ---------------------------------------------------------------------------
// Checkpoint files
// ---------------------------------------------------------------------------

/// Read the current checkpoint, falling back to the previous one only when the
/// current file is absent or unreadable. A checkpoint that parses but fails
/// `validate()` fails the mount closed rather than being replaced by an older,
/// disagreeing view of the same state directory.
fn load_checkpoint(layout: &MountStateLayout) -> Result<Option<MountCheckpoint>> {
    let current: Option<MountCheckpoint> = match read_json(&layout.checkpoint_path()) {
        Ok(checkpoint) => checkpoint,
        Err(error) => {
            tracing::warn!(
                %error,
                "mount checkpoint is unreadable; falling back to the previous checkpoint"
            );
            None
        }
    };
    let checkpoint = match current {
        Some(checkpoint) => Some(checkpoint),
        None => read_json(&layout.previous_checkpoint_path())?,
    };
    if let Some(checkpoint) = checkpoint.as_ref() {
        checkpoint.validate()?;
    }
    Ok(checkpoint)
}

// ---------------------------------------------------------------------------
// Backing filesystem free space
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn free_fraction(root: &Path) -> Option<f64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(root.as_os_str().as_bytes()).ok()?;
    let mut buffer = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let status = unsafe { libc::statvfs(path.as_ptr(), buffer.as_mut_ptr()) };
    if status != 0 {
        return None;
    }
    let stats = unsafe { buffer.assume_init() };
    let blocks = stats.f_blocks as u64;
    if blocks == 0 {
        return None;
    }
    Some(stats.f_bavail as f64 / blocks as f64)
}

#[cfg(not(unix))]
fn free_fraction(_root: &Path) -> Option<f64> {
    None
}

// These are the seven invariants the previous single-file `local_view.rs` pinned.
// They are carried forward verbatim in intent, retargeted onto the split module's
// signatures. They encode the recovery, ordering and durability contract of this
// file and must not be weakened.
#[cfg(test)]
mod tests {
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};

    use super::append_record;
    use super::{MountStateLayout, MountWal};
    use crate::fuse::local_view::types::{
        MountMutation, MountPreImage, PayloadSource, PayloadStorage, WalRecord,
    };

    fn open(state_dir: &std::path::Path) -> MountWal {
        MountWal::open(&MountStateLayout::new(state_dir), None).expect("open WAL")
    }

    #[test]
    fn packs_small_payloads_and_recovers_only_committed_events() {
        let temp = tempfile::tempdir().expect("tempdir");
        let wal = open(temp.path());
        let directory = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "repo".to_string(),
                    mode: 0o755,
                },
                PayloadSource::None,
                MountPreImage::empty(),
            )
            .expect("prepare directory");
        wal.commit(directory, None).expect("commit directory");
        let file = wal
            .prepare(
                MountMutation::ReplaceFile {
                    path: "repo/file.txt".to_string(),
                    mode: 0o644,
                    expected_file_id: None,
                    base_content_hash: None,
                },
                PayloadSource::Bytes(b"hello"),
                MountPreImage::empty(),
            )
            .expect("prepare file");
        let payload = file.event.payload.clone().expect("payload");
        assert!(matches!(
            payload.storage,
            PayloadStorage::Segment { offset: 0, .. }
        ));
        wal.commit(file, None).expect("commit file");
        let dangling = wal
            .prepare(
                MountMutation::Rename {
                    old_path: "repo/file.txt".to_string(),
                    new_path: "repo/renamed.txt".to_string(),
                    flags: 0,
                },
                PayloadSource::None,
                MountPreImage::empty(),
            )
            .expect("prepare dangling rename");
        wal.sync_local().expect("sync WAL");
        drop(wal);

        let recovered = open(temp.path());
        let state = recovered.recovery_state().expect("recovery state");
        assert_eq!(state.committed_unacknowledged.len(), 2);
        assert_eq!(state.unresolved_prepares, vec![dangling.event]);
        let batch = recovered
            .next_publish_batch(64, 1024)
            .expect("batch")
            .expect("pending batch");
        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.through_sequence, 2);
        let mut bytes = Vec::new();
        recovered
            .open_payload(&payload)
            .expect("open payload")
            .read_to_end(&mut bytes)
            .expect("read payload");
        assert_eq!(bytes, b"hello");
    }

    #[test]
    fn aborted_sequence_advances_a_contiguous_publication_prefix() {
        let temp = tempfile::tempdir().expect("tempdir");
        let wal = open(temp.path());
        let first = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "one".to_string(),
                    mode: 0o755,
                },
                PayloadSource::None,
                MountPreImage::empty(),
            )
            .expect("prepare first");
        wal.abort(first, "local mkdir failed").expect("abort first");
        let second = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "two".to_string(),
                    mode: 0o755,
                },
                PayloadSource::None,
                MountPreImage::empty(),
            )
            .expect("prepare second");
        wal.commit(second, None).expect("commit second");
        let batch = wal
            .next_publish_batch(64, 1024)
            .expect("batch")
            .expect("pending batch");
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].sequence, 2);
        assert_eq!(batch.through_sequence, 2);
        wal.acknowledge(2, 9, 2).expect("acknowledge");
        assert!(
            wal.next_publish_batch(64, 1024)
                .expect("empty batch")
                .is_none()
        );
    }

    #[test]
    fn torn_tail_is_truncated_but_interior_corruption_fails_closed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = MountStateLayout::new(temp.path());
        let wal = open(temp.path());
        let event = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "repo".to_string(),
                    mode: 0o755,
                },
                PayloadSource::None,
                MountPreImage::empty(),
            )
            .expect("prepare");
        wal.commit(event, None).expect("commit");
        wal.sync_local().expect("sync");
        drop(wal);
        let event_path = layout.log_path(0);
        OpenOptions::new()
            .append(true)
            .open(&event_path)
            .expect("open WAL")
            .write_all(br#"{"phase":"prepared""#)
            .expect("write torn tail");
        open(temp.path());

        OpenOptions::new()
            .append(true)
            .open(&event_path)
            .expect("open WAL")
            .write_all(b"{not-json}\n")
            .expect("write corrupt record");
        let error = match MountWal::open(&MountStateLayout::new(temp.path()), None) {
            Ok(_) => panic!("interior corruption must fail"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("decode mount WAL"));
    }

    #[test]
    fn acknowledgement_requires_a_resolved_monotonic_prefix() {
        let temp = tempfile::tempdir().expect("tempdir");
        let wal = open(temp.path());
        let event = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "repo".to_string(),
                    mode: 0o755,
                },
                PayloadSource::None,
                MountPreImage::empty(),
            )
            .expect("prepare");
        assert!(wal.acknowledge(1, 1, 0).is_err());
        wal.commit(event, None).expect("commit");
        wal.acknowledge(1, 7, 1).expect("acknowledge");
        assert!(wal.acknowledge(0, 8, 1).is_err());
        assert!(wal.acknowledge(1, 6, 1).is_err());
        drop(wal);

        let recovered = open(temp.path());
        let state = recovered.recovery_state().expect("recovery state");
        assert_eq!(state.acknowledged_sequence, 1);
        assert_eq!(state.remote_revision, 7);
        assert!(state.committed_unacknowledged.is_empty());
    }

    #[test]
    fn large_payload_is_streamed_to_a_dedicated_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("source.bin");
        let mut source_file = File::create(&source).expect("create source");
        for _ in 0..4 {
            source_file
                .write_all(&vec![0x5a; 1024 * 1024])
                .expect("write source");
        }
        source_file.sync_all().expect("sync source");
        drop(source_file);

        let state_dir = temp.path().join("state");
        let wal = open(&state_dir);
        let event = wal
            .prepare(
                MountMutation::ReplaceFile {
                    path: "large.bin".to_string(),
                    mode: 0o644,
                    expected_file_id: None,
                    base_content_hash: None,
                },
                PayloadSource::File(&source),
                MountPreImage::empty(),
            )
            .expect("prepare streamed payload");
        let payload = event.event.payload.clone().expect("payload");
        assert!(matches!(
            payload.storage,
            PayloadStorage::DedicatedFile { .. }
        ));
        assert_eq!(payload.length, 4 * 1024 * 1024);
        wal.commit(event, None).expect("commit");
        wal.sync_local().expect("sync");
        drop(wal);
        open(&state_dir);
    }

    #[test]
    fn path_traversal_is_rejected_before_it_reaches_the_wal() {
        let temp = tempfile::tempdir().expect("tempdir");
        let wal = open(temp.path());
        assert!(
            wal.prepare(
                MountMutation::CreateDirectory {
                    path: "../escape".to_string(),
                    mode: 0o755,
                },
                PayloadSource::None,
                MountPreImage::empty(),
            )
            .is_err()
        );
        assert!(
            wal.prepare(
                MountMutation::Rename {
                    old_path: "safe".to_string(),
                    new_path: "/absolute".to_string(),
                    flags: 0,
                },
                PayloadSource::None,
                MountPreImage::empty(),
            )
            .is_err()
        );
    }

    #[test]
    fn duplicate_terminal_record_is_rejected_on_recovery() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout = MountStateLayout::new(temp.path());
        let wal = open(temp.path());
        let event = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "repo".to_string(),
                    mode: 0o755,
                },
                PayloadSource::None,
                MountPreImage::empty(),
            )
            .expect("prepare");
        wal.commit(event, None).expect("commit");
        wal.sync_local().expect("sync");
        let epoch = wal.epoch();
        drop(wal);

        let mut file = OpenOptions::new()
            .append(true)
            .open(layout.log_path(0))
            .expect("open WAL");
        append_record(
            &mut file,
            &WalRecord::Committed {
                epoch,
                sequence: 1,
                local_identity: None,
            },
        )
        .expect("append duplicate");
        file.sync_all().expect("sync duplicate");
        assert!(MountWal::open(&MountStateLayout::new(temp.path()), None).is_err());
    }
}
