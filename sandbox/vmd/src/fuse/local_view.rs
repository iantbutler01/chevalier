//! Durable state for one single-owner writable VFS mount.
//!
//! This module deliberately knows nothing about the gateway's local or GCS
//! backend. It records the mutation sequence accepted by the owning mount and
//! exposes immutable payloads to a publisher. The publisher may acknowledge a
//! contiguous prefix only after the selected backend has made that prefix
//! durable.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use chevalier_vfs_hash::{ContentHasher, algorithm, hash_bytes};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const WAL_FORMAT_VERSION: u32 = 1;
const MAX_RECORD_BYTES: usize = 1024 * 1024;
const SEGMENT_TARGET_BYTES: u64 = 16 * 1024 * 1024;
const MAX_SEGMENTED_PAYLOAD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub(super) enum MountMutation {
    CreateDirectory {
        path: String,
        mode: u32,
    },
    CreateFile {
        path: String,
        mode: u32,
    },
    ReplaceFile {
        path: String,
        mode: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_file_id: Option<String>,
    },
    CreateSymlink {
        path: String,
        target: String,
    },
    CreateHardLink {
        existing_path: String,
        new_path: String,
    },
    Rename {
        old_path: String,
        new_path: String,
        flags: u32,
    },
    RemoveFile {
        path: String,
    },
    RemoveDirectory {
        path: String,
    },
    SetMode {
        path: String,
        mode: u32,
    },
}

impl MountMutation {
    fn validate(&self) -> Result<()> {
        match self {
            Self::CreateDirectory { path, .. }
            | Self::CreateFile { path, .. }
            | Self::ReplaceFile { path, .. }
            | Self::CreateSymlink { path, .. }
            | Self::RemoveFile { path }
            | Self::RemoveDirectory { path }
            | Self::SetMode { path, .. } => validate_mount_path(path),
            Self::CreateHardLink {
                existing_path,
                new_path,
            } => {
                validate_mount_path(existing_path)?;
                validate_mount_path(new_path)
            }
            Self::Rename {
                old_path, new_path, ..
            } => {
                validate_mount_path(old_path)?;
                validate_mount_path(new_path)
            }
        }
    }

    fn requires_payload(&self) -> bool {
        matches!(self, Self::ReplaceFile { .. })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "storage", rename_all = "snake_case")]
pub(super) enum PayloadStorage {
    Segment { file: String, offset: u64 },
    DedicatedFile { file: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct PayloadRef {
    pub(super) storage: PayloadStorage,
    pub(super) length: u64,
    pub(super) hash_algorithm: String,
    pub(super) content_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct MountEvent {
    pub(super) format_version: u32,
    pub(super) epoch: String,
    pub(super) sequence: u64,
    pub(super) idempotency_key: String,
    pub(super) mutation: MountMutation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) payload: Option<PayloadRef>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
enum WalRecord {
    Prepared {
        event: MountEvent,
    },
    Committed {
        epoch: String,
        sequence: u64,
    },
    Aborted {
        epoch: String,
        sequence: u64,
        reason: String,
    },
    RemoteAcknowledged {
        epoch: String,
        through_sequence: u64,
        remote_revision: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct MountCheckpoint {
    format_version: u32,
    epoch: String,
    acknowledged_sequence: u64,
    remote_revision: u64,
    tree_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PreparedEvent {
    pub(super) event: MountEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PublishBatch {
    pub(super) events: Vec<MountEvent>,
    /// Includes aborted sequence numbers between the previous cursor and the
    /// last event. A backend may acknowledge this prefix after all `events`
    /// have committed there.
    pub(super) through_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RecoveryState {
    pub(super) epoch: String,
    pub(super) acknowledged_sequence: u64,
    pub(super) remote_revision: u64,
    pub(super) committed_unacknowledged: Vec<MountEvent>,
    pub(super) unresolved_prepares: Vec<MountEvent>,
}

#[derive(Default)]
struct ScanState {
    epoch: Option<String>,
    prepared: BTreeMap<u64, MountEvent>,
    committed: BTreeSet<u64>,
    aborted: BTreeSet<u64>,
    acknowledged_sequence: u64,
    remote_revision: u64,
    last_complete_offset: u64,
}

struct CurrentSegment {
    name: String,
    file: File,
    len: u64,
}

struct MountWalState {
    payload_dir: PathBuf,
    event_path: PathBuf,
    checkpoint_path: PathBuf,
    event_file: File,
    epoch: String,
    next_sequence: u64,
    prepared: BTreeMap<u64, MountEvent>,
    committed: BTreeSet<u64>,
    aborted: BTreeSet<u64>,
    acknowledged_sequence: u64,
    remote_revision: u64,
    current_segment: Option<CurrentSegment>,
    next_segment_id: u64,
    dirty_payload_files: BTreeSet<String>,
    payload_directory_dirty: bool,
}

#[derive(Clone)]
pub(super) struct MountWal {
    state: Arc<Mutex<MountWalState>>,
}

impl MountWal {
    pub(super) fn open(state_dir: &Path) -> Result<Self> {
        fs::create_dir_all(state_dir)
            .with_context(|| format!("create mount state directory {}", state_dir.display()))?;
        let payload_dir = state_dir.join("payloads");
        fs::create_dir_all(&payload_dir)
            .with_context(|| format!("create mount payload directory {}", payload_dir.display()))?;
        sync_directory(state_dir)?;

        let event_path = state_dir.join("events.jsonl");
        let checkpoint_path = state_dir.join("checkpoint.json");
        let checkpoint = read_checkpoint(&checkpoint_path)?;
        let mut scan = scan_wal(&event_path)?;
        if event_path.exists() {
            let file = OpenOptions::new()
                .write(true)
                .open(&event_path)
                .with_context(|| format!("open mount WAL {}", event_path.display()))?;
            let len = file.metadata()?.len();
            if scan.last_complete_offset < len {
                file.set_len(scan.last_complete_offset)
                    .with_context(|| format!("truncate torn mount WAL {}", event_path.display()))?;
                file.sync_all()
                    .with_context(|| format!("sync repaired mount WAL {}", event_path.display()))?;
                sync_directory(state_dir)?;
            }
        }

        let epoch = reconcile_epoch(scan.epoch.take(), checkpoint.as_ref())?;
        if let Some(checkpoint) = checkpoint.as_ref() {
            scan.acknowledged_sequence = scan
                .acknowledged_sequence
                .max(checkpoint.acknowledged_sequence);
            if checkpoint.acknowledged_sequence >= scan.acknowledged_sequence {
                scan.remote_revision = scan.remote_revision.max(checkpoint.remote_revision);
            }
        }
        validate_scan(&scan)?;
        for event in scan.prepared.values() {
            if scan.committed.contains(&event.sequence) {
                if let Some(payload) = event.payload.as_ref() {
                    verify_payload(&payload_dir, payload)?;
                }
            }
        }

        let next_sequence = scan
            .prepared
            .keys()
            .next_back()
            .copied()
            .unwrap_or(scan.acknowledged_sequence)
            .saturating_add(1);
        let next_segment_id = next_segment_id(&payload_dir)?;
        let event_file = open_append(&event_path)?;
        sync_directory(state_dir)?;
        Ok(Self {
            state: Arc::new(Mutex::new(MountWalState {
                payload_dir,
                event_path,
                checkpoint_path,
                event_file,
                epoch,
                next_sequence,
                prepared: scan.prepared,
                committed: scan.committed,
                aborted: scan.aborted,
                acknowledged_sequence: scan.acknowledged_sequence,
                remote_revision: scan.remote_revision,
                current_segment: None,
                next_segment_id,
                dirty_payload_files: BTreeSet::new(),
                payload_directory_dirty: false,
            })),
        })
    }

    pub(super) fn prepare(
        &self,
        mutation: MountMutation,
        payload: Option<&[u8]>,
    ) -> Result<PreparedEvent> {
        mutation.validate()?;
        if mutation.requires_payload() != payload.is_some() {
            bail!(
                "mount mutation payload mismatch: operation requires_payload={} supplied={}",
                mutation.requires_payload(),
                payload.is_some()
            );
        }
        let mut state = self.lock()?;
        let payload = payload
            .map(|bytes| append_payload_bytes(&mut state, bytes))
            .transpose()?;
        prepare_locked(&mut state, mutation, payload)
    }

    /// Stage a complete file generation without holding it all in memory.
    pub(super) fn prepare_file(
        &self,
        mutation: MountMutation,
        source: &Path,
    ) -> Result<PreparedEvent> {
        mutation.validate()?;
        if !mutation.requires_payload() {
            bail!("streamed payload is valid only for a ReplaceFile mutation");
        }
        let (payload, name) = {
            let state = self.lock()?;
            let name = format!("payload-{}.bin", Uuid::now_v7());
            let destination = state.payload_dir.join(&name);
            drop(state);
            (
                copy_payload_file(source, &destination)
                    .with_context(|| format!("stage mount payload from {}", source.display()))?,
                name,
            )
        };
        let mut state = self.lock()?;
        state.dirty_payload_files.insert(name);
        state.payload_directory_dirty = true;
        prepare_locked(&mut state, mutation, Some(payload))
    }

    pub(super) fn commit(&self, prepared: PreparedEvent) -> Result<()> {
        let mut state = self.lock()?;
        let sequence = prepared.event.sequence;
        let stored = state
            .prepared
            .get(&sequence)
            .ok_or_else(|| anyhow!("unknown prepared mount sequence {sequence}"))?;
        if stored != &prepared.event {
            bail!("prepared mount event {sequence} does not match the durable record");
        }
        if state.aborted.contains(&sequence) {
            bail!("mount sequence {sequence} is already aborted");
        }
        if state.committed.contains(&sequence) {
            return Ok(());
        }
        let record = WalRecord::Committed {
            epoch: state.epoch.clone(),
            sequence,
        };
        append_record(&mut state.event_file, &record)?;
        state.committed.insert(sequence);
        Ok(())
    }

    pub(super) fn abort(&self, prepared: PreparedEvent, reason: &str) -> Result<()> {
        let mut state = self.lock()?;
        let sequence = prepared.event.sequence;
        let stored = state
            .prepared
            .get(&sequence)
            .ok_or_else(|| anyhow!("unknown prepared mount sequence {sequence}"))?;
        if stored != &prepared.event {
            bail!("prepared mount event {sequence} does not match the durable record");
        }
        if state.committed.contains(&sequence) {
            bail!("mount sequence {sequence} is already committed");
        }
        if state.aborted.contains(&sequence) {
            return Ok(());
        }
        let record = WalRecord::Aborted {
            epoch: state.epoch.clone(),
            sequence,
            reason: reason.to_string(),
        };
        append_record(&mut state.event_file, &record)?;
        state.aborted.insert(sequence);
        Ok(())
    }

    pub(super) fn next_publish_batch(
        &self,
        max_events: usize,
        max_payload_bytes: u64,
    ) -> Result<Option<PublishBatch>> {
        if max_events == 0 {
            bail!("mount publication batch must allow at least one event");
        }
        let state = self.lock()?;
        let mut sequence = state.acknowledged_sequence.saturating_add(1);
        let mut through_sequence = state.acknowledged_sequence;
        let mut payload_bytes = 0_u64;
        let mut events = Vec::new();
        loop {
            if state.aborted.contains(&sequence) {
                through_sequence = sequence;
                sequence = sequence.saturating_add(1);
                continue;
            }
            let Some(event) = state.prepared.get(&sequence) else {
                break;
            };
            if !state.committed.contains(&sequence) {
                break;
            }
            let event_bytes = event
                .payload
                .as_ref()
                .map(|payload| payload.length)
                .unwrap_or(0);
            if !events.is_empty()
                && (events.len() >= max_events
                    || payload_bytes.saturating_add(event_bytes) > max_payload_bytes)
            {
                break;
            }
            payload_bytes = payload_bytes.saturating_add(event_bytes);
            events.push(event.clone());
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

    pub(super) fn acknowledge(
        &self,
        through_sequence: u64,
        remote_revision: u64,
        tree_generation: u64,
    ) -> Result<()> {
        let mut state = self.lock()?;
        if through_sequence < state.acknowledged_sequence {
            bail!(
                "mount acknowledgement regressed from {} to {through_sequence}",
                state.acknowledged_sequence
            );
        }
        if through_sequence == state.acknowledged_sequence {
            if remote_revision < state.remote_revision {
                bail!(
                    "mount remote revision regressed from {} to {remote_revision}",
                    state.remote_revision
                );
            }
            return Ok(());
        }
        for sequence in state.acknowledged_sequence.saturating_add(1)..=through_sequence {
            if !(state.committed.contains(&sequence) || state.aborted.contains(&sequence)) {
                bail!("cannot acknowledge unresolved mount sequence {sequence}");
            }
        }
        let record = WalRecord::RemoteAcknowledged {
            epoch: state.epoch.clone(),
            through_sequence,
            remote_revision,
        };
        append_record(&mut state.event_file, &record)?;
        state
            .event_file
            .sync_data()
            .with_context(|| format!("sync mount WAL {}", state.event_path.display()))?;
        let checkpoint = MountCheckpoint {
            format_version: WAL_FORMAT_VERSION,
            epoch: state.epoch.clone(),
            acknowledged_sequence: through_sequence,
            remote_revision,
            tree_generation,
        };
        write_checkpoint(&state.checkpoint_path, &checkpoint)?;
        state.acknowledged_sequence = through_sequence;
        state.remote_revision = remote_revision;
        Ok(())
    }

    pub(super) fn sync_local(&self) -> Result<()> {
        let mut state = self.lock()?;
        let dirty = std::mem::take(&mut state.dirty_payload_files);
        for name in dirty {
            let path = state.payload_dir.join(&name);
            File::open(&path)
                .with_context(|| format!("open mount payload {}", path.display()))?
                .sync_data()
                .with_context(|| format!("sync mount payload {}", path.display()))?;
        }
        if state.payload_directory_dirty {
            sync_directory(&state.payload_dir)?;
            state.payload_directory_dirty = false;
        }
        state
            .event_file
            .sync_data()
            .with_context(|| format!("sync mount WAL {}", state.event_path.display()))
    }

    pub(super) fn recovery_state(&self) -> Result<RecoveryState> {
        let state = self.lock()?;
        let mut committed_unacknowledged = Vec::new();
        let mut unresolved_prepares = Vec::new();
        for (sequence, event) in state.prepared.range(state.acknowledged_sequence + 1..) {
            if state.committed.contains(sequence) {
                committed_unacknowledged.push(event.clone());
            } else if !state.aborted.contains(sequence) {
                unresolved_prepares.push(event.clone());
            }
        }
        Ok(RecoveryState {
            epoch: state.epoch.clone(),
            acknowledged_sequence: state.acknowledged_sequence,
            remote_revision: state.remote_revision,
            committed_unacknowledged,
            unresolved_prepares,
        })
    }

    pub(super) fn open_payload(&self, payload: &PayloadRef) -> Result<PayloadReader> {
        let state = self.lock()?;
        open_payload(&state.payload_dir, payload)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, MountWalState>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("mount WAL lock poisoned"))
    }
}

pub(super) struct PayloadReader {
    file: File,
    remaining: u64,
}

impl Read for PayloadReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let limit =
            usize::try_from(self.remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        let read = self.file.read(&mut buffer[..limit])?;
        self.remaining = self.remaining.saturating_sub(read as u64);
        Ok(read)
    }
}

fn prepare_locked(
    state: &mut MountWalState,
    mutation: MountMutation,
    payload: Option<PayloadRef>,
) -> Result<PreparedEvent> {
    let sequence = state.next_sequence;
    let event = MountEvent {
        format_version: WAL_FORMAT_VERSION,
        epoch: state.epoch.clone(),
        sequence,
        idempotency_key: format!("{}:{sequence}", state.epoch),
        mutation,
        payload,
    };
    append_record(
        &mut state.event_file,
        &WalRecord::Prepared {
            event: event.clone(),
        },
    )?;
    state.prepared.insert(sequence, event.clone());
    state.next_sequence = sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("mount WAL sequence exhausted"))?;
    Ok(PreparedEvent { event })
}

fn append_payload_bytes(state: &mut MountWalState, bytes: &[u8]) -> Result<PayloadRef> {
    if bytes.len() > MAX_SEGMENTED_PAYLOAD_BYTES {
        let name = format!("payload-{}.bin", Uuid::now_v7());
        let path = state.payload_dir.join(&name);
        let mut file = create_new(&path)?;
        file.write_all(bytes)
            .with_context(|| format!("write mount payload {}", path.display()))?;
        state.dirty_payload_files.insert(name.clone());
        state.payload_directory_dirty = true;
        return Ok(PayloadRef {
            storage: PayloadStorage::DedicatedFile { file: name },
            length: bytes.len() as u64,
            hash_algorithm: algorithm().as_str().to_string(),
            content_hash: hash_bytes(bytes),
        });
    }

    let rotate = state.current_segment.as_ref().is_none_or(|segment| {
        segment.len > 0 && segment.len.saturating_add(bytes.len() as u64) > SEGMENT_TARGET_BYTES
    });
    if rotate {
        let segment_id = state.next_segment_id;
        state.next_segment_id = state.next_segment_id.saturating_add(1);
        let name = format!("segment-{segment_id:020}.bin");
        let path = state.payload_dir.join(&name);
        let file = create_new(&path)?;
        state.current_segment = Some(CurrentSegment { name, file, len: 0 });
        state.payload_directory_dirty = true;
    }
    let segment = state
        .current_segment
        .as_mut()
        .ok_or_else(|| anyhow!("mount payload segment was not created"))?;
    let offset = segment.len;
    segment
        .file
        .write_all(bytes)
        .with_context(|| format!("append mount payload segment {}", segment.name))?;
    segment.len = segment.len.saturating_add(bytes.len() as u64);
    state.dirty_payload_files.insert(segment.name.clone());
    Ok(PayloadRef {
        storage: PayloadStorage::Segment {
            file: segment.name.clone(),
            offset,
        },
        length: bytes.len() as u64,
        hash_algorithm: algorithm().as_str().to_string(),
        content_hash: hash_bytes(bytes),
    })
}

fn copy_payload_file(source: &Path, destination: &Path) -> Result<PayloadRef> {
    let mut input =
        File::open(source).with_context(|| format!("open payload source {}", source.display()))?;
    let mut output = create_new(destination)?;
    let mut hasher = ContentHasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = input
            .read(&mut buffer)
            .with_context(|| format!("read payload source {}", source.display()))?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .with_context(|| format!("write mount payload {}", destination.display()))?;
        hasher.update(&buffer[..read]);
        length = length.saturating_add(read as u64);
    }
    let file = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("mount payload path is not UTF-8: {}", destination.display()))?
        .to_string();
    Ok(PayloadRef {
        storage: PayloadStorage::DedicatedFile { file },
        length,
        hash_algorithm: algorithm().as_str().to_string(),
        content_hash: hasher.finalize(),
    })
}

fn scan_wal(path: &Path) -> Result<ScanState> {
    if !path.exists() {
        return Ok(ScanState::default());
    }
    let file = File::open(path).with_context(|| format!("open mount WAL {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut scan = ScanState::default();
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
            break;
        }
        if buffer.len() > MAX_RECORD_BYTES {
            bail!("mount WAL record exceeds {MAX_RECORD_BYTES} bytes");
        }
        let record: WalRecord = serde_json::from_slice(&buffer)
            .with_context(|| format!("decode mount WAL at byte {}", scan.last_complete_offset))?;
        apply_scanned_record(&mut scan, record)?;
        scan.last_complete_offset = scan.last_complete_offset.saturating_add(read as u64);
    }
    Ok(scan)
}

fn apply_scanned_record(scan: &mut ScanState, record: WalRecord) -> Result<()> {
    match record {
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
            if scan.prepared.insert(event.sequence, event).is_some() {
                bail!("duplicate mount WAL prepare sequence");
            }
        }
        WalRecord::Committed { epoch, sequence } => {
            observe_epoch(&mut scan.epoch, &epoch)?;
            if scan.aborted.contains(&sequence) || !scan.committed.insert(sequence) {
                bail!("invalid mount WAL commit sequence {sequence}");
            }
        }
        WalRecord::Aborted {
            epoch, sequence, ..
        } => {
            observe_epoch(&mut scan.epoch, &epoch)?;
            if scan.committed.contains(&sequence) || !scan.aborted.insert(sequence) {
                bail!("invalid mount WAL abort sequence {sequence}");
            }
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
    }
    Ok(())
}

fn validate_scan(scan: &ScanState) -> Result<()> {
    for sequence in scan.committed.iter().chain(scan.aborted.iter()) {
        if !scan.prepared.contains_key(sequence) {
            bail!("mount WAL terminal record has no prepare for sequence {sequence}");
        }
    }
    for sequence in 1..=scan.acknowledged_sequence {
        if !(scan.committed.contains(&sequence) || scan.aborted.contains(&sequence)) {
            bail!("mount WAL acknowledged unresolved sequence {sequence}");
        }
    }
    Ok(())
}

fn reconcile_epoch(
    scanned_epoch: Option<String>,
    checkpoint: Option<&MountCheckpoint>,
) -> Result<String> {
    match (scanned_epoch, checkpoint) {
        (Some(scanned), Some(checkpoint)) if scanned != checkpoint.epoch => bail!(
            "mount WAL epoch {} does not match checkpoint epoch {}",
            scanned,
            checkpoint.epoch
        ),
        (Some(scanned), _) => Ok(scanned),
        (None, Some(checkpoint)) => Ok(checkpoint.epoch.clone()),
        (None, None) => Ok(Uuid::now_v7().to_string()),
    }
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

fn verify_payload(payload_dir: &Path, payload: &PayloadRef) -> Result<()> {
    let mut reader = open_payload(payload_dir, payload)?;
    let mut hasher = ContentHasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length = length.saturating_add(read as u64);
    }
    if length != payload.length {
        bail!(
            "mount payload length mismatch: expected {}, read {length}",
            payload.length
        );
    }
    if payload.hash_algorithm != algorithm().as_str() {
        bail!(
            "mount payload hash algorithm {} does not match active {}",
            payload.hash_algorithm,
            algorithm().as_str()
        );
    }
    let actual = hasher.finalize();
    if actual != payload.content_hash {
        bail!(
            "mount payload hash mismatch: expected {}, read {actual}",
            payload.content_hash
        );
    }
    Ok(())
}

fn open_payload(payload_dir: &Path, payload: &PayloadRef) -> Result<PayloadReader> {
    let (name, offset) = match &payload.storage {
        PayloadStorage::Segment { file, offset } => (file, *offset),
        PayloadStorage::DedicatedFile { file } => (file, 0),
    };
    validate_payload_name(name)?;
    let path = payload_dir.join(name);
    let mut file =
        File::open(&path).with_context(|| format!("open mount payload {}", path.display()))?;
    let end = offset
        .checked_add(payload.length)
        .ok_or_else(|| anyhow!("mount payload range overflow"))?;
    if file.metadata()?.len() < end {
        bail!(
            "mount payload {} is shorter than referenced range {offset}..{end}",
            path.display()
        );
    }
    file.seek(SeekFrom::Start(offset))
        .with_context(|| format!("seek mount payload {}", path.display()))?;
    Ok(PayloadReader {
        file,
        remaining: payload.length,
    })
}

fn validate_mount_path(path: &str) -> Result<()> {
    if path.is_empty() {
        bail!("mount mutation path must not be empty");
    }
    let candidate = Path::new(path);
    if candidate.is_absolute()
        || candidate.components().any(|component| {
            matches!(
                component,
                Component::ParentDir
                    | Component::CurDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
    {
        bail!("mount mutation path must be normalized and relative: {path:?}");
    }
    Ok(())
}

fn validate_payload_name(name: &str) -> Result<()> {
    let candidate = Path::new(name);
    if name.is_empty()
        || candidate.components().count() != 1
        || !matches!(candidate.components().next(), Some(Component::Normal(_)))
    {
        bail!("invalid mount payload filename {name:?}");
    }
    Ok(())
}

fn next_segment_id(payload_dir: &Path) -> Result<u64> {
    let mut next = 0_u64;
    for entry in fs::read_dir(payload_dir)
        .with_context(|| format!("read mount payload directory {}", payload_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(raw) = name
            .strip_prefix("segment-")
            .and_then(|name| name.strip_suffix(".bin"))
        else {
            continue;
        };
        if let Ok(id) = raw.parse::<u64>() {
            next = next.max(id.saturating_add(1));
        }
    }
    Ok(next)
}

fn read_checkpoint(path: &Path) -> Result<Option<MountCheckpoint>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes =
        fs::read(path).with_context(|| format!("read mount checkpoint {}", path.display()))?;
    let checkpoint: MountCheckpoint = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode mount checkpoint {}", path.display()))?;
    if checkpoint.format_version != WAL_FORMAT_VERSION {
        bail!(
            "unsupported mount checkpoint format {}",
            checkpoint.format_version
        );
    }
    Ok(Some(checkpoint))
}

fn write_checkpoint(path: &Path, checkpoint: &MountCheckpoint) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("mount checkpoint has no parent: {}", path.display()))?;
    let temp = parent.join(format!(".checkpoint-{}.tmp", Uuid::now_v7()));
    let mut file = create_new(&temp)?;
    serde_json::to_writer(&mut file, checkpoint)
        .with_context(|| format!("encode mount checkpoint {}", temp.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("write mount checkpoint {}", temp.display()))?;
    file.sync_all()
        .with_context(|| format!("sync mount checkpoint {}", temp.display()))?;
    fs::rename(&temp, path).with_context(|| {
        format!(
            "publish mount checkpoint {} -> {}",
            temp.display(),
            path.display()
        )
    })?;
    sync_directory(parent)
}

fn append_record(file: &mut File, record: &WalRecord) -> Result<()> {
    serde_json::to_writer(&mut *file, record).context("encode mount WAL record")?;
    file.write_all(b"\n").context("append mount WAL record")
}

fn open_append(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .with_context(|| format!("open append-only mount WAL {}", path.display()))
}

fn create_new(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(path)
        .with_context(|| format!("create {}", path.display()))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};

    use super::{MountMutation, MountWal, PayloadStorage, WalRecord, append_record};

    #[test]
    fn packs_small_payloads_and_recovers_only_committed_events() {
        let temp = tempfile::tempdir().expect("tempdir");
        let wal = MountWal::open(temp.path()).expect("open WAL");
        let directory = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "repo".to_string(),
                    mode: 0o755,
                },
                None,
            )
            .expect("prepare directory");
        wal.commit(directory).expect("commit directory");
        let file = wal
            .prepare(
                MountMutation::ReplaceFile {
                    path: "repo/file.txt".to_string(),
                    mode: 0o644,
                    expected_file_id: None,
                },
                Some(b"hello"),
            )
            .expect("prepare file");
        let payload = file.event.payload.clone().expect("payload");
        assert!(matches!(
            payload.storage,
            PayloadStorage::Segment { offset: 0, .. }
        ));
        wal.commit(file).expect("commit file");
        let dangling = wal
            .prepare(
                MountMutation::Rename {
                    old_path: "repo/file.txt".to_string(),
                    new_path: "repo/renamed.txt".to_string(),
                    flags: 0,
                },
                None,
            )
            .expect("prepare dangling rename");
        wal.sync_local().expect("sync WAL");
        drop(wal);

        let recovered = MountWal::open(temp.path()).expect("reopen WAL");
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
        let wal = MountWal::open(temp.path()).expect("open WAL");
        let first = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "one".to_string(),
                    mode: 0o755,
                },
                None,
            )
            .expect("prepare first");
        wal.abort(first, "local mkdir failed").expect("abort first");
        let second = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "two".to_string(),
                    mode: 0o755,
                },
                None,
            )
            .expect("prepare second");
        wal.commit(second).expect("commit second");
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
        let wal = MountWal::open(temp.path()).expect("open WAL");
        let event = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "repo".to_string(),
                    mode: 0o755,
                },
                None,
            )
            .expect("prepare");
        wal.commit(event).expect("commit");
        wal.sync_local().expect("sync");
        drop(wal);
        let event_path = temp.path().join("events.jsonl");
        OpenOptions::new()
            .append(true)
            .open(&event_path)
            .expect("open WAL")
            .write_all(br#"{"phase":"prepared""#)
            .expect("write torn tail");
        MountWal::open(temp.path()).expect("torn final record is recoverable");

        OpenOptions::new()
            .append(true)
            .open(&event_path)
            .expect("open WAL")
            .write_all(b"{not-json}\n")
            .expect("write corrupt record");
        let error = match MountWal::open(temp.path()) {
            Ok(_) => panic!("interior corruption must fail"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("decode mount WAL"));
    }

    #[test]
    fn acknowledgement_requires_a_resolved_monotonic_prefix() {
        let temp = tempfile::tempdir().expect("tempdir");
        let wal = MountWal::open(temp.path()).expect("open WAL");
        let event = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "repo".to_string(),
                    mode: 0o755,
                },
                None,
            )
            .expect("prepare");
        assert!(wal.acknowledge(1, 1, 0).is_err());
        wal.commit(event).expect("commit");
        wal.acknowledge(1, 7, 1).expect("acknowledge");
        assert!(wal.acknowledge(0, 8, 1).is_err());
        assert!(wal.acknowledge(1, 6, 1).is_err());
        drop(wal);

        let recovered = MountWal::open(temp.path()).expect("reopen WAL");
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

        let wal = MountWal::open(&temp.path().join("state")).expect("open WAL");
        let event = wal
            .prepare_file(
                MountMutation::ReplaceFile {
                    path: "large.bin".to_string(),
                    mode: 0o644,
                    expected_file_id: None,
                },
                &source,
            )
            .expect("prepare streamed payload");
        let payload = event.event.payload.clone().expect("payload");
        assert!(matches!(
            payload.storage,
            PayloadStorage::DedicatedFile { .. }
        ));
        assert_eq!(payload.length, 4 * 1024 * 1024);
        wal.commit(event).expect("commit");
        wal.sync_local().expect("sync");
        drop(wal);
        MountWal::open(&temp.path().join("state")).expect("verify streamed payload on recovery");
    }

    #[test]
    fn path_traversal_is_rejected_before_it_reaches_the_wal() {
        let temp = tempfile::tempdir().expect("tempdir");
        let wal = MountWal::open(temp.path()).expect("open WAL");
        assert!(
            wal.prepare(
                MountMutation::CreateDirectory {
                    path: "../escape".to_string(),
                    mode: 0o755,
                },
                None,
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
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn duplicate_terminal_record_is_rejected_on_recovery() {
        let temp = tempfile::tempdir().expect("tempdir");
        let wal = MountWal::open(temp.path()).expect("open WAL");
        let event = wal
            .prepare(
                MountMutation::CreateDirectory {
                    path: "repo".to_string(),
                    mode: 0o755,
                },
                None,
            )
            .expect("prepare");
        wal.commit(event).expect("commit");
        wal.sync_local().expect("sync");
        let state = wal.lock().expect("lock");
        let epoch = state.epoch.clone();
        drop(state);
        drop(wal);

        let event_path = temp.path().join("events.jsonl");
        let mut file = OpenOptions::new()
            .append(true)
            .open(event_path)
            .expect("open WAL");
        append_record(&mut file, &WalRecord::Committed { epoch, sequence: 1 })
            .expect("append duplicate");
        file.sync_all().expect("sync duplicate");
        assert!(MountWal::open(temp.path()).is_err());
    }
}
