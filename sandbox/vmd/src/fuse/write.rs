#[cfg(test)]
use std::cell::Cell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chevalier_sandbox::vfs::{
    VFS_SURFACE_KIND_VM_SHARED, VFS_SURFACE_KIND_VM_WORKSPACE, VfsDirEntry,
    VfsPublicationSnapshotEntry,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use super::client::{RemoteVfsClient, RemoteWrite, rejected_request_status};

// This is an IDLE debounce, so it adds no wait to close(2) and explicit
// barriers bypass it.
// Eight milliseconds was shorter than the mounted create/write/close cadence:
// the worker published the first partial queue while the burst was still
// arriving, every publication advanced the global coherence fence, and the
// remaining creates fell back to metadata/stat traffic. A 100ms idle window
// keeps a package-install burst together while fsync still force-flushes.
const BATCH_DELAY: Duration = Duration::from_millis(100);
const RETRY_DELAY_MIN: Duration = Duration::from_millis(100);
const RETRY_DELAY_MAX: Duration = Duration::from_secs(5);
const FLUSH_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
/// Emit one WARN if a flush barrier parks past this before the hard timeout, so
/// an intermittent upstream stall is visible in production without per-poll spam.
const SLOW_FLUSH_WARN_AFTER: Duration = Duration::from_secs(10);
/// Cap on per-id terminal errors retained for a waiter that may never come.
/// Close does not wait (see `flush_handle_locked`), so these would otherwise grow
/// without bound on a mount that keeps dead-lettering.
const MAX_RETAINED_TERMINAL_ERRORS: usize = 1024;
/// Brief enqueue window for concurrent close(2) callbacks. Ordinary close
/// returns after the staging file and WAL record are installed in the host page
/// cache; the publication worker establishes stable-storage ordering for the
/// complete queued batch before sending any of it to the gateway.
const DURABLE_ENQUEUE_GROUP_DELAY: Duration = Duration::from_millis(1);
const MAX_DURABLE_ENQUEUE_GROUP: usize = 128;
/// Parallel fdatasync lanes used by the publication worker. Concurrent file
/// syncs let the filesystem coalesce device barriers across the queued set
/// before its WAL is synced and its gateway request is issued.
const MAX_PUBLICATION_SYNC_WORKERS: usize = 16;

// One package install commonly creates hundreds of small files in one
// directory. Keep the complete burst in one atomic write-many publication; the
// byte cap remains the guard for large payloads.
const MAX_BATCH_WRITES: usize = 1_024;
const MAX_BATCH_BYTES: u64 = 16 * 1024 * 1024;
/// A single staged write at or above this size uses the gateway's raw streamed
/// upload route. Below it, `/write-many` remains more efficient because it can
/// atomically publish a whole small-file burst in one request.
const STREAMED_WRITE_MIN_BYTES: u64 = 8 * 1024 * 1024;
const JOURNAL_READ_BUFFER_BYTES: usize = 64 * 1024;
/// Journal records contain metadata and bounded paths, never file payloads.
/// One MiB is far above the supported path envelope while keeping a corrupt
/// or unterminated JSONL record from forcing an unbounded allocation.
const MAX_JOURNAL_RECORD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct JournalWrite {
    id: u64,
    path: String,
    staged_file: String,
    size_bytes: u64,
    base_content_hash: Option<String>,
    /// Stable identity that must still own `path` when this write commits.
    /// Old journals predate identity-aware publication and intentionally
    /// decode this as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_file_id: Option<String>,
    /// POSIX mode to apply if this write CREATES the path.
    ///
    /// Set when a creation is folded into the write that follows it, so the
    /// pair costs one publication instead of two. Durable for the same reason
    /// `expected_file_id` is: the journal line is the only record that survives
    /// a restart between enqueue and publication, and losing the mode would
    /// strip the executable bit off a file the guest already saw as executable.
    /// Old journals decode this as `None`, which means "do not set mode".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    create_mode: Option<u32>,
}

type WriteTarget = (String, Option<String>);

pub(super) struct PendingCreatedFile {
    pub(super) size_bytes: u64,
    pub(super) file_id: Option<String>,
    pub(super) mode: u32,
}

impl JournalWrite {
    fn target(&self) -> WriteTarget {
        (self.path.clone(), self.expected_file_id.clone())
    }
}

struct JournalState {
    pending: VecDeque<JournalWrite>,
    journal: File,
    next_id: u64,
    force_flush: bool,
    flushing: bool,
    stop: bool,
    /// A rewrite crossed or may have crossed the atomic rename boundary but
    /// did not complete both parent-directory sync and append-handle reopen.
    /// No later append may proceed until the full pending state is rewritten.
    journal_needs_repair: bool,
    last_error: Option<String>,
    /// Latched when writes were dead-lettered; consumed by the next flush()
    /// so exactly one fsync/close waiter observes the failure (POSIX
    /// deferred-writeback semantics) without wedging later flushes.
    dead_letter_error: Option<String>,
    /// Terminal publication failures keyed by the exact enqueue id. A handle
    /// barrier consumes only its own entry, so concurrent fsync/close waiters
    /// cannot steal another file's deferred error.
    terminal_errors: HashMap<u64, String>,
    /// Descendant-or-equal path prefixes of an in-flight namespace delete /
    /// rename. A content-write enqueue whose path falls under any of these
    /// blocks until the namespace mutation completes, so the write cannot be
    /// published server-side after the RemoveDirectory and resurrect a subtree.
    /// Guarded by the same `state` lock as `pending`, so the barrier check and
    /// the journal append are atomic against `install_descendant_barrier`.
    descendant_barriers: Vec<String>,
}

type DeadLetterHook = Box<dyn Fn(&str) + Send + Sync>;
type CommitHook = Box<dyn Fn(u64, &[WriteTarget], &[VfsPublicationSnapshotEntry]) + Send + Sync>;

struct DurableEnqueue {
    path: String,
    bytes: Vec<u8>,
    base_content_hash: Option<String>,
    expected_file_id: Option<String>,
    create_mode: Option<u32>,
    result: Mutex<Option<std::result::Result<u64, String>>>,
    ready: Condvar,
}

#[derive(Default)]
struct DurableEnqueueState {
    pending: VecDeque<Arc<DurableEnqueue>>,
    /// Paths currently being staged and appended by the group-commit leader.
    /// Namespace barriers inspect both this and `pending` so a request that
    /// entered the durability queue first is drained ahead of the mutation.
    active_paths: Vec<String>,
    processing: bool,
}

struct Shared {
    state: Mutex<JournalState>,
    changed: Condvar,
    durable_enqueues: Mutex<DurableEnqueueState>,
    durable_enqueues_changed: Condvar,
    journal_path: PathBuf,
    staging_dir: PathBuf,
    /// Invalidates reader-visible caches for a path whose write was dropped.
    on_dead_letter: Option<DeadLetterHook>,
}

pub struct WriteJournal {
    shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl WriteJournal {
    pub fn open(
        client: RemoteVfsClient,
        scope_path: &str,
        journal_path: &Path,
        tokio: Handle,
        on_dead_letter: Option<DeadLetterHook>,
    ) -> Result<Self> {
        Self::open_with_commit_hook(
            client,
            scope_path,
            journal_path,
            tokio,
            on_dead_letter,
            None,
        )
    }

    pub fn open_with_commit_hook(
        client: RemoteVfsClient,
        scope_path: &str,
        journal_path: &Path,
        tokio: Handle,
        on_dead_letter: Option<DeadLetterHook>,
        on_commit: Option<CommitHook>,
    ) -> Result<Self> {
        let staging_dir = journal_path.with_extension("writes");
        fs::create_dir_all(&staging_dir).with_context(|| {
            format!(
                "create vfs write staging directory {}",
                staging_dir.display()
            )
        })?;
        sync_parent_directory(&staging_dir)?;
        let pending = read_journal(journal_path)?;
        validate_staged_writes(&staging_dir, &pending)?;
        remove_orphaned_staged_writes(&staging_dir, &pending)?;
        let next_id = pending
            .iter()
            .map(|write| write.id)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let journal = open_append(journal_path)?;
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending,
                journal,
                next_id,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path: journal_path.to_path_buf(),
            staging_dir,
            on_dead_letter,
        });
        let worker_shared = Arc::clone(&shared);
        let scope_path = scope_path.trim_matches('/').to_string();
        let worker = std::thread::Builder::new()
            .name("chevalier-vfs-writes".to_string())
            .spawn(move || run_worker(worker_shared, client, scope_path, tokio, on_commit))
            .context("spawn vfs write journal worker")?;
        shared.changed.notify_all();
        Ok(Self {
            shared,
            worker: Mutex::new(Some(worker)),
        })
    }

    pub fn enqueue(
        &self,
        path: &str,
        bytes: &[u8],
        base_content_hash: Option<String>,
        expected_file_id: Option<String>,
    ) -> Result<u64> {
        self.enqueue_with_mode(path, bytes, base_content_hash, expected_file_id, None)
    }

    /// Enqueue a write that may also CREATE the path, carrying the mode the
    /// creation would have published. Folding the two into one journal entry is
    /// what turns a create+write from two publications into one.
    pub fn enqueue_with_mode(
        &self,
        path: &str,
        bytes: &[u8],
        base_content_hash: Option<String>,
        expected_file_id: Option<String>,
        create_mode: Option<u32>,
    ) -> Result<u64> {
        // Park a barred path before it enters the shared durability queue. This
        // preserves path-scoped namespace ordering without making an unrelated
        // enqueue wait behind the barred request at the head of the group.
        //
        // Barrier installation holds `durable_enqueues` until it has installed
        // the journal-state barrier. Therefore the gap between this check and
        // queue admission is ordered: either this request queues first and the
        // barrier waits for it, or the barrier becomes visible first and the
        // group re-check below parks behind it.
        {
            let mut state = self
                .shared
                .state
                .lock()
                .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
            while path_within_any_barrier(&state.descendant_barriers, path) {
                state = self
                    .shared
                    .changed
                    .wait(state)
                    .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
            }
        }
        let request = Arc::new(DurableEnqueue {
            path: path.to_string(),
            bytes: bytes.to_vec(),
            base_content_hash,
            expected_file_id,
            create_mode,
            result: Mutex::new(None),
            ready: Condvar::new(),
        });
        let leader = {
            let mut enqueues = self
                .shared
                .durable_enqueues
                .lock()
                .map_err(|_| anyhow!("vfs durable enqueue lock poisoned"))?;
            enqueues.pending.push_back(Arc::clone(&request));
            if enqueues.processing {
                false
            } else {
                enqueues.processing = true;
                true
            }
        };
        if leader {
            drive_durable_enqueues(&self.shared);
        }
        let mut result = request
            .result
            .lock()
            .map_err(|_| anyhow!("vfs durable enqueue result lock poisoned"))?;
        while result.is_none() {
            result = request
                .ready
                .wait(result)
                .map_err(|_| anyhow!("vfs durable enqueue result lock poisoned"))?;
        }
        result
            .take()
            .ok_or_else(|| anyhow!("vfs durable enqueue result missing"))?
            .map_err(|error| anyhow!(error))
    }

    pub fn flush(&self) -> Result<()> {
        flush_shared(&self.shared)
    }

    /// A cloneable handle that can drain this journal without owning the
    /// `WriteJournal`. The namespace recovery worker holds one so it can flush
    /// stragglers before retrying a conflicted RemoveDirectory/DeleteFile.
    pub(crate) fn drain_handle(&self) -> WriteDrainHandle {
        WriteDrainHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Bar new content-write enqueues whose path is a descendant-or-equal of any
    /// `prefixes` entry until the returned guard drops. Installed by the
    /// namespace publication path around a delete/rename so a racing write is
    /// either drained ahead of the mutation or blocked behind it. Idempotent for
    /// disjoint prefixes; each guard removes exactly the prefixes it added.
    pub(crate) fn install_descendant_barrier(&self, prefixes: Vec<String>) -> WriteBarrierGuard {
        install_descendant_barrier(&self.shared, prefixes)
    }

    /// Wait for one exact enqueue to resolve and report only that operation's
    /// terminal error. The global journal barrier remains available for
    /// namespace ordering, but file fsync/close should use this method.
    /// Wait for one specific queued write to publish.
    ///
    /// Called by `fsync` and by the paths that report authoritative
    /// post-publication state. NOT by `close(2)` — see `flush_handle_locked`.
    ///
    /// `force_flush` here is deliberate and is why this must stay off the close
    /// path: it shuts the batch window immediately so THIS id goes out now. That
    /// is exactly right for an fsync, which is a caller asking to pay for
    /// durability now, and exactly wrong for a close, where it meant every file
    /// in a sequential loop published alone.
    pub fn flush_through(&self, id: u64) -> Result<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
        state.force_flush = true;
        self.shared.changed.notify_all();
        let start = Instant::now();
        let deadline = start + FLUSH_RETRY_TIMEOUT;
        let mut slow_warned = false;
        while state.pending.iter().any(|write| write.id == id) {
            let now = Instant::now();
            if now >= deadline {
                return Err(anyhow!(state.last_error.clone().unwrap_or_else(|| {
                    format!("timed out flushing vfs write journal operation {id}")
                })));
            }
            let elapsed = now.duration_since(start);
            if !slow_warned && elapsed >= SLOW_FLUSH_WARN_AFTER {
                slow_warned = true;
                tracing::warn!(
                    waited_secs = elapsed.as_secs(),
                    operation = id,
                    pending = state.pending.len(),
                    "vfs write journal flush_through still waiting on a pending operation"
                );
            }
            let mut wait_for = deadline.saturating_duration_since(now);
            if !slow_warned {
                wait_for = wait_for.min(SLOW_FLUSH_WARN_AFTER.saturating_sub(elapsed));
            }
            let waited = self
                .shared
                .changed
                .wait_timeout(state, wait_for)
                .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
            state = waited.0;
        }
        if let Some(error) = state.terminal_errors.remove(&id) {
            return Err(anyhow!(error));
        }
        // Bound the map. It is keyed per write id and drained only by a waiter,
        // and close no longer waits — so without this, every dead-lettered write
        // nobody fsynced would leak an entry for the life of the mount. The
        // failure itself is not lost: `dead_letter_error` is latched for the next
        // `flush()` (which every `flush_writes()` reaches) and the dead-letter
        // hook has already invalidated the path so no reader serves the dropped
        // bytes.
        if state.terminal_errors.len() > MAX_RETAINED_TERMINAL_ERRORS {
            let excess = state.terminal_errors.len() - MAX_RETAINED_TERMINAL_ERRORS;
            let stale = state
                .terminal_errors
                .keys()
                .copied()
                .take(excess)
                .collect::<Vec<_>>();
            for key in stale {
                state.terminal_errors.remove(&key);
            }
        }
        // `last_error` summarizes the journal worker globally and may belong
        // to another pathname. Once this exact id has left `pending`, only its
        // own terminal result is relevant to this handle's fsync/close.
        Ok(())
    }

    /// Whether a queued write lives UNDER `path` — i.e. whether this mount's own
    /// unpublished work implies `path` is an existing directory.
    ///
    /// Read-your-writes for ancestors. A queued write to `many/file-0` means
    /// `many` exists as far as this mount is concerned, but the gateway has not
    /// been told yet, so a wire stat of `many` answers 404. Draining to settle
    /// it would defeat the batching that made the write async in the first
    /// place, and the namespace projection cannot answer either — a folded
    /// creation withdrew its record precisely because the write now carries it.
    pub fn has_pending_under(&self, path: &str) -> bool {
        let prefix = path.trim_matches('/');
        if prefix.is_empty() {
            return self
                .shared
                .state
                .lock()
                .map(|state| !state.pending.is_empty())
                .unwrap_or(false);
        }
        self.shared
            .state
            .lock()
            .map(|state| {
                state.pending.iter().any(|write| {
                    write
                        .path
                        .trim_matches('/')
                        .strip_prefix(prefix)
                        .is_some_and(|rest| rest.starts_with('/'))
                })
            })
            .unwrap_or(false)
    }

    pub fn has_pending_path(&self, path: &str) -> bool {
        self.shared
            .state
            .lock()
            .map(|state| state.pending.iter().any(|write| write.path == path))
            .unwrap_or(true)
    }

    /// Return the newest queued contents for a pathname whose creation is still
    /// owned by this journal, together with the durable creation mode.
    ///
    /// Node's `copyFile` path performs `create/write/close/chmod` even when the
    /// chmod merely repeats the mode used by create. The mode is already in the
    /// write WAL in that case; forcing the whole write journal to publish and
    /// then issuing `stat + SetMode` turned one package import into thousands
    /// of synchronous namespace round trips. This view lets the filesystem
    /// recognize that exact no-op without weakening a real mode change.
    pub fn pending_created_file(&self, path: &str) -> Option<PendingCreatedFile> {
        let state = self.shared.state.lock().ok()?;
        pending_created_file(state.pending.iter(), path)
    }

    /// Merge this mount's unpublished direct-child writes into a directory
    /// listing without exposing them through the cache shared by sibling
    /// mounts.
    ///
    /// Folded creations have no namespace-journal record: their write WAL entry
    /// is the only mount-local proof that the name exists until `write-many`
    /// commits. Without this overlay an immediate `readdir` can observe the
    /// authoritative pre-publication listing (often the seeded empty listing),
    /// so `rm -rf` skips every child and sends only the final rmdir. The rmdir
    /// drains the write journal, creates those skipped children server-side,
    /// and then correctly fails as not-empty.
    pub fn project_directory(
        &self,
        path: &str,
        mut entries: Vec<VfsDirEntry>,
    ) -> Result<(Vec<VfsDirEntry>, bool)> {
        let directory = path.trim_matches('/');
        let state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
        let mut applied = false;
        for write in &state.pending {
            let write_path = write.path.trim_matches('/');
            let direct_name = if directory.is_empty() {
                (!write_path.contains('/')).then_some(write_path)
            } else {
                write_path
                    .strip_prefix(directory)
                    .and_then(|suffix| suffix.strip_prefix('/'))
                    .filter(|suffix| !suffix.is_empty() && !suffix.contains('/'))
            };
            let Some(name) = direct_name else {
                continue;
            };
            if let Some(entry) = entries.iter_mut().find(|entry| entry.name == name) {
                entry.size_bytes = write.size_bytes;
                entry.content_hash = None;
                if let Some(file_id) = write.expected_file_id.as_ref() {
                    entry.file_id = Some(file_id.clone());
                }
                if let Some(mode) = write.create_mode {
                    entry.mode = Some(mode);
                    entry.executable = mode & 0o111 != 0;
                }
                applied = true;
            } else if let Some(mode) = write.create_mode {
                entries.push(VfsDirEntry {
                    name: name.to_string(),
                    kind: "file".to_string(),
                    size_bytes: write.size_bytes,
                    file_id: write.expected_file_id.clone(),
                    link_count: 1,
                    link_target: None,
                    content_hash: None,
                    executable: mode & 0o111 != 0,
                    mode: Some(mode),
                    updated_at: None,
                });
                applied = true;
            }
        }
        Ok((entries, applied))
    }
}

fn drive_durable_enqueues(shared: &Arc<Shared>) {
    loop {
        std::thread::sleep(DURABLE_ENQUEUE_GROUP_DELAY);
        let requests = {
            let mut enqueues = shared
                .durable_enqueues
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if enqueues.pending.is_empty() {
                enqueues.processing = false;
                enqueues.active_paths.clear();
                shared.durable_enqueues_changed.notify_all();
                return;
            }
            let take = enqueues.pending.len().min(MAX_DURABLE_ENQUEUE_GROUP);
            let requests = enqueues.pending.drain(..take).collect::<Vec<_>>();
            enqueues.active_paths = requests
                .iter()
                .map(|request| request.path.clone())
                .collect();
            requests
        };
        let outcome = process_durable_enqueue_group(shared, &requests);
        match outcome {
            Ok(ids) => {
                for (request, id) in requests.iter().zip(ids) {
                    *request
                        .result
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Ok(id));
                    request.ready.notify_all();
                }
            }
            Err(error) => {
                let error = error.to_string();
                for request in &requests {
                    *request
                        .result
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some(Err(error.clone()));
                    request.ready.notify_all();
                }
            }
        }
        let mut enqueues = shared
            .durable_enqueues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        enqueues.active_paths.clear();
        shared.durable_enqueues_changed.notify_all();
    }
}

fn process_durable_enqueue_group(
    shared: &Arc<Shared>,
    requests: &[Arc<DurableEnqueue>],
) -> Result<Vec<u64>> {
    let mut state = shared
        .state
        .lock()
        .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
    // The state lock makes the whole group atomic against installation of a
    // delete/rename descendant barrier. If the barrier won the race, wait for
    // it exactly as the former per-file enqueue did; if this group won, the
    // namespace operation installs behind these durable WAL entries and its
    // normal write drain orders them before the mutation.
    while requests
        .iter()
        .any(|request| path_within_any_barrier(&state.descendant_barriers, request.path.as_str()))
    {
        state = shared
            .changed
            .wait(state)
            .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
    }
    repair_before_append(&shared.journal_path, &mut state)?;

    let mut prepared = Vec::with_capacity(requests.len());
    let mut journal_touched = false;
    let result = (|| -> Result<Vec<u64>> {
        for request in requests {
            let id = state.next_id;
            state.next_id = state.next_id.saturating_add(1);
            let staged_file = format!("{id}.bin");
            let staged_path = shared.staging_dir.join(staged_file.as_str());
            let temporary = staged_path.with_extension("tmp");
            let mut file = File::create(&temporary)
                .with_context(|| format!("create staged vfs write {}", temporary.display()))?;
            file.write_all(request.bytes.as_slice())
                .context("stage vfs write bytes")?;
            prepared.push((
                temporary,
                staged_path,
                JournalWrite {
                    id,
                    path: request.path.clone(),
                    staged_file,
                    size_bytes: request.bytes.len() as u64,
                    base_content_hash: request.base_content_hash.clone(),
                    expected_file_id: request.expected_file_id.clone(),
                    create_mode: request.create_mode,
                },
                file,
            ));
        }
        // close(2) is not fsync(2). Install complete staging files and WAL
        // records in the host page cache; the publication worker syncs the
        // complete queued set in one ordered group before it goes remote.
        for (temporary, staged_path, _, _) in &prepared {
            fs::rename(temporary, staged_path)
                .with_context(|| format!("install staged vfs write {}", staged_path.display()))?;
        }
        for (_, _, write, _) in &prepared {
            append_json_line_unsynced(&mut state.journal, write, "append vfs write journal")?;
            journal_touched = true;
        }
        let ids = prepared
            .iter()
            .map(|(_, _, write, _)| write.id)
            .collect::<Vec<_>>();
        state
            .pending
            .extend(prepared.iter().map(|(_, _, write, _)| write.clone()));
        state.last_error = None;
        if state.pending.len() >= MAX_BATCH_WRITES {
            state.force_flush = true;
        }
        shared.changed.notify_all();
        Ok(ids)
    })();

    if let Err(error) = result {
        if journal_touched {
            // A partial/unsynced append is repaired from the authoritative
            // in-memory pending queue before any later append is admitted.
            state.journal_needs_repair = true;
        }
        for (temporary, staged_path, _, _) in &prepared {
            let _ = fs::remove_file(temporary);
            if !journal_touched {
                let _ = fs::remove_file(staged_path);
            }
        }
        return Err(error);
    }
    result
}

fn pending_created_file<'a>(
    pending: impl DoubleEndedIterator<Item = &'a JournalWrite> + Clone,
    path: &str,
) -> Option<PendingCreatedFile> {
    let latest = pending.clone().rev().find(|write| write.path == path)?;
    let mode = pending
        .rev()
        .find_map(|write| (write.path == path).then_some(write.create_mode).flatten())?;
    Some(PendingCreatedFile {
        size_bytes: latest.size_bytes,
        file_id: latest.expected_file_id.clone(),
        mode,
    })
}

impl Drop for WriteJournal {
    fn drop(&mut self) {
        let _ = self.flush();
        if let Ok(mut state) = self.shared.state.lock() {
            state.stop = true;
            state.force_flush = true;
            self.shared.changed.notify_all();
        }
        if let Ok(mut worker) = self.worker.lock()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

/// A detached drain handle over a live journal's shared state. Cloning the
/// `Arc` keeps the journal's worker and staging directory alive for as long as
/// any handle exists, exactly like the owning `WriteJournal`.
#[derive(Clone)]
pub(crate) struct WriteDrainHandle {
    shared: Arc<Shared>,
}

impl WriteDrainHandle {
    /// Block until the journal has drained (or the flush deadline elapses),
    /// surfacing the same terminal/dead-letter error `WriteJournal::flush` does.
    pub(crate) fn flush(&self) -> Result<()> {
        flush_shared(&self.shared)
    }

    pub(crate) fn install_descendant_barrier(&self, prefixes: Vec<String>) -> WriteBarrierGuard {
        install_descendant_barrier(&self.shared, prefixes)
    }
}

/// Install a descendant barrier atomically against both durable queued writes
/// and writes already present in the publication journal.
///
/// The durable queue lock is held until the journal-state barrier is visible:
/// a write that queued first is allowed to finish its durable append, while a
/// later write cannot enter the queue until it will observe the new barrier.
fn install_descendant_barrier(shared: &Arc<Shared>, prefixes: Vec<String>) -> WriteBarrierGuard {
    let mut prior_write_ids = Vec::new();
    if !prefixes.is_empty() {
        let mut enqueues = shared
            .durable_enqueues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while durable_enqueues_intersect(&enqueues, &prefixes) {
            enqueues = shared
                .durable_enqueues_changed
                .wait(enqueues)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prior_write_ids.extend(
            state
                .pending
                .iter()
                .filter(|write| path_within_any_barrier(&prefixes, write.path.as_str()))
                .map(|write| write.id),
        );
        state.descendant_barriers.extend(prefixes.iter().cloned());
    }
    WriteBarrierGuard {
        shared: Arc::clone(shared),
        prefixes,
        prior_write_ids,
    }
}

fn durable_enqueues_intersect(enqueues: &DurableEnqueueState, prefixes: &[String]) -> bool {
    enqueues
        .pending
        .iter()
        .any(|request| path_within_any_barrier(prefixes, request.path.as_str()))
        || enqueues
            .active_paths
            .iter()
            .any(|path| path_within_any_barrier(prefixes, path))
}

/// RAII guard for an installed descendant write-barrier. Dropping it removes the
/// prefixes it added and wakes any content-write enqueue waiting behind them.
pub(crate) struct WriteBarrierGuard {
    shared: Arc<Shared>,
    prefixes: Vec<String>,
    /// Writes under this barrier that were admitted before it became visible.
    /// Later matching writes are parked by `prefixes`; unrelated later writes
    /// must not keep the namespace mutation waiting for a globally empty queue.
    prior_write_ids: Vec<u64>,
}

impl WriteBarrierGuard {
    pub(crate) fn drain_handle(&self) -> WriteBarrierDrain {
        WriteBarrierDrain {
            shared: Arc::clone(&self.shared),
            prior_write_ids: self.prior_write_ids.clone(),
        }
    }
}

impl Drop for WriteBarrierGuard {
    fn drop(&mut self) {
        if self.prefixes.is_empty() {
            return;
        }
        if let Ok(mut state) = self.shared.state.lock() {
            for prefix in &self.prefixes {
                if let Some(index) = state
                    .descendant_barriers
                    .iter()
                    .position(|active| active == prefix)
                {
                    state.descendant_barriers.remove(index);
                }
            }
        }
        // Wake enqueues parked on the barrier; they re-check under `state`.
        self.shared.changed.notify_all();
    }
}

pub(crate) struct WriteBarrierDrain {
    shared: Arc<Shared>,
    prior_write_ids: Vec<u64>,
}

impl WriteBarrierDrain {
    pub(crate) fn flush(&self) -> Result<()> {
        flush_selected_shared(&self.shared, &self.prior_write_ids)
    }
}

fn flush_selected_shared(shared: &Arc<Shared>, ids: &[u64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let ids = ids.iter().copied().collect::<HashSet<_>>();
    let mut state = shared
        .state
        .lock()
        .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
    state.force_flush = true;
    shared.changed.notify_all();
    let start = Instant::now();
    let deadline = start + FLUSH_RETRY_TIMEOUT;
    let mut slow_warned = false;
    while state.pending.iter().any(|write| ids.contains(&write.id)) {
        let now = Instant::now();
        if now >= deadline {
            return Err(anyhow!(state.last_error.clone().unwrap_or_else(|| {
                "timed out flushing writes ordered before a namespace mutation".to_string()
            })));
        }
        let elapsed = now.duration_since(start);
        if !slow_warned && elapsed >= SLOW_FLUSH_WARN_AFTER {
            slow_warned = true;
            tracing::warn!(
                waited_secs = elapsed.as_secs(),
                selected = ids.len(),
                pending = state.pending.len(),
                "vfs namespace write watermark still draining prior writes"
            );
        }
        let mut wait_for = deadline.saturating_duration_since(now);
        if !slow_warned {
            wait_for = wait_for.min(SLOW_FLUSH_WARN_AFTER.saturating_sub(elapsed));
        }
        let waited = shared
            .changed
            .wait_timeout(state, wait_for)
            .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
        state = waited.0;
    }
    let mut first_error = None;
    for id in ids {
        if let Some(error) = state.terminal_errors.remove(&id)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    match first_error {
        Some(error) => Err(anyhow!(error)),
        None => Ok(()),
    }
}

fn flush_shared(shared: &Arc<Shared>) -> Result<()> {
    let mut state = shared
        .state
        .lock()
        .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
    state.force_flush = true;
    shared.changed.notify_all();
    let start = Instant::now();
    let deadline = start + FLUSH_RETRY_TIMEOUT;
    let mut slow_warned = false;
    while !state.pending.is_empty() || state.flushing {
        let now = Instant::now();
        if now >= deadline {
            return Err(anyhow!(state.last_error.clone().unwrap_or_else(|| {
                "timed out flushing vfs write journal".to_string()
            })));
        }
        let elapsed = now.duration_since(start);
        if !slow_warned && elapsed >= SLOW_FLUSH_WARN_AFTER {
            slow_warned = true;
            tracing::warn!(
                waited_secs = elapsed.as_secs(),
                pending = state.pending.len(),
                flushing = state.flushing,
                "vfs write journal flush still draining pending writes"
            );
        }
        let mut wait_for = deadline.saturating_duration_since(now);
        if !slow_warned {
            wait_for = wait_for.min(SLOW_FLUSH_WARN_AFTER.saturating_sub(elapsed));
        }
        let waited = shared
            .changed
            .wait_timeout(state, wait_for)
            .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
        state = waited.0;
    }
    if let Some(error) = state.dead_letter_error.take() {
        return Err(anyhow!(error));
    }
    if let Some(error) = state.last_error.clone() {
        return Err(anyhow!(error));
    }
    Ok(())
}

/// Whether `path` is a descendant-or-equal of any active barrier prefix.
fn path_within_any_barrier(barriers: &[String], path: &str) -> bool {
    barriers
        .iter()
        .any(|prefix| path_within_barrier(prefix, path))
}

/// Segment-boundary descendant-or-equal match: `prefix` matches `path` itself or
/// any path under `prefix/`, but never an unrelated sibling like `foobar` for
/// `foo`.
fn path_within_barrier(prefix: &str, path: &str) -> bool {
    let prefix = prefix.trim_matches('/');
    let path = path.trim_matches('/');
    if prefix.is_empty() {
        // A whole-scope barrier (empty prefix) would bar every write; the
        // namespace layer never installs one, but treat it as scope-wide.
        return true;
    }
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

/// Whether `name` is a server-side write-staging temporary of the shape
/// `.{name}.{uuid}.tmp` produced by `install_writes` (vfs/src/local.rs). Such
/// entries are never guest-visible files, so they are safe residue to delete
/// when reconciling a not-empty directory. Ordinary dotfiles (`.gitignore`,
/// `.env.tmp`, `.a.b.tmp`) are rejected because their penultimate segment is
/// not a UUID.
pub(crate) fn is_write_staging_temp_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix('.') else {
        return false;
    };
    let Some(rest) = rest.strip_suffix(".tmp") else {
        return false;
    };
    let Some((inner, candidate_uuid)) = rest.rsplit_once('.') else {
        return false;
    };
    !inner.is_empty() && is_uuid_like(candidate_uuid)
}

/// Whether `value` has the canonical hyphenated UUID shape (8-4-4-4-12 hex).
/// Deliberately format-only: any v4 UUID minted by `Uuid::new_v4` matches, and
/// no ordinary filename segment does.
fn is_uuid_like(value: &str) -> bool {
    if value.len() != 36 {
        return false;
    }
    value.bytes().enumerate().all(|(index, byte)| {
        if matches!(index, 8 | 13 | 18 | 23) {
            byte == b'-'
        } else {
            byte.is_ascii_hexdigit()
        }
    })
}

/// Establish the write-ahead ordering for every record currently visible in
/// the journal before any selected batch is published remotely.
///
/// The caller holds `state`, so no later WAL append can be included in the
/// journal sync without its staged file also being included here.
fn sync_pending_before_publication(shared: &Arc<Shared>, state: &mut JournalState) -> Result<()> {
    let pending = state.pending.iter().cloned().collect::<Vec<_>>();
    if pending.is_empty() {
        return Ok(());
    }
    let worker_count = pending.len().min(MAX_PUBLICATION_SYNC_WORKERS).max(1);
    let chunk_size = pending.len().div_ceil(worker_count);
    std::thread::scope(|scope| -> Result<()> {
        let mut workers = Vec::with_capacity(worker_count);
        for chunk in pending.chunks(chunk_size) {
            workers.push(scope.spawn(move || -> Result<()> {
                for write in chunk {
                    let staged_path = shared.staging_dir.join(write.staged_file.as_str());
                    File::open(&staged_path)
                        .with_context(|| {
                            format!("open staged vfs write for sync {}", staged_path.display())
                        })?
                        .sync_data()
                        .with_context(|| {
                            format!("sync staged vfs write bytes {}", staged_path.display())
                        })?;
                }
                Ok(())
            }));
        }
        for worker in workers {
            worker
                .join()
                .map_err(|_| anyhow!("vfs staged write sync worker panicked"))??;
        }
        Ok(())
    })?;
    let first_staged = shared.staging_dir.join(pending[0].staged_file.as_str());
    sync_parent_directory(&first_staged)?;
    state
        .journal
        .sync_data()
        .context("sync vfs write journal before publication")
}

fn run_worker(
    shared: Arc<Shared>,
    client: RemoteVfsClient,
    scope_path: String,
    tokio: Handle,
    on_commit: Option<CommitHook>,
) {
    let mut retry_delay = RETRY_DELAY_MIN;
    loop {
        let (batch, surface, durability) = {
            let mut state = match shared.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            while state.pending.is_empty() && !state.stop {
                // Nothing queued, so a pending force-flush is satisfied by
                // definition — retire it rather than carrying it into work that
                // has not been asked for yet. `flush()` raises the flag
                // unconditionally, including when the journal is already empty,
                // and only a batch take (below) lowers it: a flush with nothing
                // to drain therefore latched it until the NEXT enqueue, which
                // the worker then published ALONE instead of batching. The
                // namespace journal had the identical bug.
                state.force_flush = false;
                state = match shared.changed.wait(state) {
                    Ok(state) => state,
                    Err(_) => return,
                };
            }
            if state.stop && state.pending.is_empty() {
                return;
            }
            if !state.force_flush && state.pending.len() < MAX_BATCH_WRITES {
                // Debounce on ENQUEUE activity, not on the first entry. A fixed
                // deadline from the first write split one steady sequential
                // create loop every 8ms even though entries kept arriving. The
                // worker now publishes after the queue has been idle for one
                // batch window (or reaches a hard size/byte boundary).
                let mut observed_len = state.pending.len();
                let mut deadline = Instant::now() + BATCH_DELAY;
                while !state.force_flush
                    && state.pending.len() < MAX_BATCH_WRITES
                    && Instant::now() < deadline
                {
                    let waited = match shared
                        .changed
                        .wait_timeout(state, deadline.saturating_duration_since(Instant::now()))
                    {
                        Ok(waited) => waited,
                        Err(_) => return,
                    };
                    state = waited.0;
                    if state.pending.len() > observed_len {
                        observed_len = state.pending.len();
                        deadline = Instant::now() + BATCH_DELAY;
                        continue;
                    }
                    if waited.1.timed_out() {
                        break;
                    }
                }
            }
            let surface = state
                .pending
                .front()
                .map(|write| path_surface(scope_path.as_str(), write.path.as_str()))
                .unwrap_or(VFS_SURFACE_KIND_VM_WORKSPACE);
            let mut bytes = 0_u64;
            let mut batch = Vec::new();
            for write in state.pending.iter().take(MAX_BATCH_WRITES) {
                if path_surface(scope_path.as_str(), write.path.as_str()) != surface {
                    break;
                }
                if !batch.is_empty() && bytes.saturating_add(write.size_bytes) > MAX_BATCH_BYTES {
                    break;
                }
                bytes = bytes.saturating_add(write.size_bytes);
                batch.push(write.clone());
            }
            let durability = sync_pending_before_publication(&shared, &mut state);
            state.force_flush = false;
            state.flushing = true;
            (batch, surface, durability)
        };

        let coalesced = coalesce_batch(&batch);
        let (committed_hashes, result) = if let Err(error) = durability {
            (HashMap::new(), Err(error))
        } else if coalesced.len() == 1 && coalesced[0].size_bytes >= STREAMED_WRITE_MIN_BYTES {
            let write = &coalesced[0];
            let staged_path = shared.staging_dir.join(write.staged_file.as_str());
            match stream_exact_file(&staged_path, None, write.size_bytes) {
                Ok(content_hash) => {
                    let committed_hashes = HashMap::from([(write.target(), content_hash.clone())]);
                    let result = tokio.block_on(client.write_staged_file(
                        write.path.as_str(),
                        &staged_path,
                        write.size_bytes,
                        content_hash.as_str(),
                        write.base_content_hash.as_deref(),
                        write.expected_file_id.as_deref(),
                        write.create_mode,
                        surface,
                    ));
                    (committed_hashes, result)
                }
                Err(error) => (HashMap::new(), Err(error)),
            }
        } else {
            let writes = coalesced
                .iter()
                .map(|write| {
                    fs::read(shared.staging_dir.join(write.staged_file.as_str()))
                        .map(|bytes| RemoteWrite {
                            path: write.path.clone(),
                            bytes,
                            base_content_hash: write.base_content_hash.clone(),
                            expected_file_id: write.expected_file_id.clone(),
                            mode: write.create_mode,
                        })
                        .with_context(|| format!("read staged vfs write {}", write.staged_file))
                })
                .collect::<Result<Vec<_>>>();
            let committed_hashes = writes
                .as_ref()
                .map(|writes| {
                    writes
                        .iter()
                        .map(|write| {
                            (
                                (write.path.clone(), write.expected_file_id.clone()),
                                content_hash_for_bytes(write.bytes.as_slice()),
                            )
                        })
                        .collect::<HashMap<_, _>>()
                })
                .unwrap_or_default();
            let result =
                writes.and_then(|writes| tokio.block_on(client.write_many(writes, surface)));
            (committed_hashes, result)
        };
        // A 4xx means the gateway rejected the batch outright; retrying the
        // same batch can never succeed. Resolve each write individually so one
        // poisoned entry cannot wedge the journal forever.
        let resolution = match &result {
            Err(error) if rejected_request_status(error).is_some() => {
                tracing::warn!(
                    writes = coalesced.len(),
                    error = %error,
                    "vfs write batch rejected; reconciling individual writes"
                );
                Some(resolve_rejected_batch(
                    &shared,
                    &client,
                    &tokio,
                    &coalesced,
                    surface,
                    on_commit.as_deref(),
                ))
            }
            _ => None,
        };
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        state.flushing = false;
        if let Some(resolution) = resolution {
            apply_batch_resolution(&shared, &mut state, &batch, &coalesced, resolution);
            shared.changed.notify_all();
            let failed = state.last_error.is_some();
            let stop = state.stop;
            drop(state);
            if stop && failed {
                return;
            }
            if failed {
                std::thread::sleep(retry_delay);
                retry_delay = retry_delay.saturating_mul(2).min(RETRY_DELAY_MAX);
            } else {
                retry_delay = RETRY_DELAY_MIN;
            }
            continue;
        }
        match result {
            Ok(publication) => {
                if let Some(on_commit) = on_commit.as_deref() {
                    let targets = coalesced
                        .iter()
                        .map(JournalWrite::target)
                        .collect::<Vec<_>>();
                    on_commit(
                        publication.revision,
                        targets.as_slice(),
                        publication.entries.as_slice(),
                    );
                }
                let recovered = state.last_error.take();
                let pending_before = state.pending.clone();
                for _ in 0..batch.len() {
                    state.pending.pop_front();
                }
                rebase_pending_after_commit(
                    &mut state.pending,
                    coalesced.as_slice(),
                    &committed_hashes,
                );
                if let Err(error) = rewrite_journal(&shared.journal_path, &mut state) {
                    // The old durable WAL still names this batch. Keep both
                    // the in-memory entries and staged bytes so reconnect can
                    // resolve an ambiguous remote completion safely.
                    state.pending = pending_before;
                    state.last_error = Some(error.to_string());
                } else {
                    remove_staged_after_wal(&shared, &batch);
                    state.last_error = None;
                    if let Some(error) = recovered {
                        tracing::info!(
                            journal = %shared.journal_path.display(),
                            write_count = batch.len(),
                            previous_error = %error,
                            "vfs write journal replay recovered"
                        );
                    }
                }
                retry_delay = RETRY_DELAY_MIN;
            }
            Err(error) => {
                let error = error.to_string();
                if state.last_error.as_deref() != Some(error.as_str()) {
                    tracing::warn!(
                        journal = %shared.journal_path.display(),
                        write_count = batch.len(),
                        first_path = batch.first().map(|write| write.path.as_str()),
                        error = %error,
                        "vfs write journal replay failed; retaining writes for retry"
                    );
                }
                state.last_error = Some(error);
            }
        }
        shared.changed.notify_all();
        let failed = state.last_error.is_some();
        let stop = state.stop;
        drop(state);
        if stop && failed {
            return;
        }
        if failed {
            std::thread::sleep(retry_delay);
            retry_delay = retry_delay.saturating_mul(2).min(RETRY_DELAY_MAX);
        }
    }
}

#[derive(Default)]
struct BatchResolution {
    /// (path, stable identity) -> committed content hash, for rebasing only
    /// the queued writes that still address the same inode incarnation.
    committed: HashMap<WriteTarget, String>,
    /// Open inode identities whose last namespace alias disappeared. Their
    /// local bytes remain valid until final close, but no WAL entry may
    /// recreate the retired pathname.
    retired: HashSet<WriteTarget>,
    /// Coalesced writes the gateway rejected with a 4xx, with the error text.
    dead_lettered: Vec<(JournalWrite, String)>,
    /// Targets that hit a transient failure and stay pending.
    retained: Vec<WriteTarget>,
}

#[derive(Debug, PartialEq, Eq)]
enum RejectedWriteOutcome {
    Committed(String),
    Retired,
    DeadLetter(String),
    Retained,
}

#[derive(Debug, PartialEq, Eq)]
struct VisibleWriteState {
    content_hash: Option<String>,
    file_id: Option<String>,
}

fn resolve_rejected_write(
    write: &JournalWrite,
    bytes: Vec<u8>,
    mut submit: impl FnMut(RemoteWrite) -> Result<()>,
    mut current_state: impl FnMut(&str) -> Result<Option<VisibleWriteState>>,
    mut surviving_alias: impl FnMut(&str, &str) -> Result<Option<String>>,
) -> RejectedWriteOutcome {
    let content_hash = content_hash_for_bytes(bytes.as_slice());
    let mut candidate_path = write.path.clone();
    let mut base_content_hash = write.base_content_hash.clone();
    let mut error = match submit(RemoteWrite {
        path: candidate_path.clone(),
        bytes: bytes.clone(),
        base_content_hash: base_content_hash.clone(),
        expected_file_id: write.expected_file_id.clone(),
        mode: write.create_mode,
    }) {
        Ok(()) => return RejectedWriteOutcome::Committed(content_hash),
        Err(error) => error,
    };

    // A path hint may go stale repeatedly under cross-mount rename churn.
    // Each retry revalidates stable identity and carries it atomically into
    // the write; the bounded loop prevents a hostile namespace from pinning
    // one journal worker forever.
    for _ in 0..4 {
        if !matches!(
            rejected_request_status(&error),
            Some(StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED)
        ) {
            return if rejected_request_status(&error).is_some() {
                RejectedWriteOutcome::DeadLetter(error.to_string())
            } else {
                RejectedWriteOutcome::Retained
            };
        }

        let current = match current_state(candidate_path.as_str()) {
            Ok(current) => current,
            Err(_) => return RejectedWriteOutcome::Retained,
        };
        let identity_matches = match write.expected_file_id.as_deref() {
            Some(expected) => {
                current.as_ref().and_then(|state| state.file_id.as_deref()) == Some(expected)
            }
            None => true,
        };
        if !identity_matches {
            let Some(expected_file_id) = write.expected_file_id.as_deref() else {
                return RejectedWriteOutcome::DeadLetter(error.to_string());
            };
            candidate_path = match surviving_alias(expected_file_id, candidate_path.as_str()) {
                Ok(Some(alias)) => alias,
                Ok(None) => return RejectedWriteOutcome::Retired,
                Err(_) => return RejectedWriteOutcome::Retained,
            };
            // An alias is another name for the same inode, so the original
            // content CAS base remains the correct write precondition.
            base_content_hash = write.base_content_hash.clone();
        } else if current
            .as_ref()
            .and_then(|state| state.content_hash.as_deref())
            == Some(content_hash.as_str())
        {
            // A precondition rejection can mean the write already landed (lost
            // response or a partially committed batch). Matching bytes are not
            // proof of durability: force an exact-content, exact-identity CAS
            // repair and retire the WAL only after that repair succeeds.
            base_content_hash = Some(content_hash.clone());
        } else {
            return RejectedWriteOutcome::DeadLetter(error.to_string());
        }

        error = match submit(RemoteWrite {
            path: candidate_path.clone(),
            bytes: bytes.clone(),
            base_content_hash: base_content_hash.clone(),
            expected_file_id: write.expected_file_id.clone(),
            mode: write.create_mode,
        }) {
            Ok(()) => return RejectedWriteOutcome::Committed(content_hash),
            Err(error) => error,
        };
    }
    RejectedWriteOutcome::Retained
}

fn resolve_rejected_batch(
    shared: &Shared,
    client: &RemoteVfsClient,
    tokio: &Handle,
    coalesced: &[JournalWrite],
    surface: &'static str,
    on_commit: Option<&(dyn Fn(u64, &[WriteTarget], &[VfsPublicationSnapshotEntry]) + Send + Sync)>,
) -> BatchResolution {
    let mut resolution = BatchResolution::default();
    for write in coalesced {
        let staged_path = shared.staging_dir.join(write.staged_file.as_str());
        let bytes = match fs::read(&staged_path) {
            Ok(bytes) => bytes,
            Err(error) => {
                resolution.dead_lettered.push((
                    write.clone(),
                    format!("read staged vfs write {}: {error}", staged_path.display()),
                ));
                continue;
            }
        };
        match resolve_rejected_write(
            write,
            bytes,
            |remote| {
                tokio
                    .block_on(client.write_many(vec![remote], surface))
                    .map(|_| ())
            },
            |path| {
                tokio.block_on(client.stat(path)).map(|metadata| {
                    metadata.map(|metadata| VisibleWriteState {
                        content_hash: metadata.content_hash,
                        file_id: metadata.file_id,
                    })
                })
            },
            |file_id, excluding_path| {
                tokio.block_on(client.find_hard_link_alias(file_id, excluding_path))
            },
        ) {
            RejectedWriteOutcome::Committed(content_hash) => {
                if let Some(on_commit) = on_commit {
                    on_commit(client.coherence_revision(), &[write.target()], &[]);
                }
                resolution.committed.insert(write.target(), content_hash);
            }
            RejectedWriteOutcome::Retired => {
                resolution.retired.insert(write.target());
            }
            RejectedWriteOutcome::DeadLetter(error) => {
                resolution.dead_lettered.push((write.clone(), error));
            }
            RejectedWriteOutcome::Retained => {
                resolution.retained.push(write.target());
            }
        }
    }
    resolution
}

fn apply_batch_resolution(
    shared: &Shared,
    state: &mut JournalState,
    batch: &[JournalWrite],
    coalesced: &[JournalWrite],
    resolution: BatchResolution,
) {
    let pending_before = state.pending.clone();
    let dead_letter_error_before = state.dead_letter_error.clone();
    let terminal_errors_before = state.terminal_errors.clone();
    let mut resolved_targets = resolution
        .committed
        .keys()
        .cloned()
        .collect::<HashSet<WriteTarget>>();
    resolved_targets.extend(resolution.retired.iter().cloned());
    let mut dead_lettered_paths = Vec::<String>::new();
    let mut dead_lettered_targets = HashMap::<WriteTarget, String>::new();
    let mut preservation_failures = Vec::<WriteTarget>::new();
    for (write, error) in &resolution.dead_lettered {
        // Only count the entry resolved once its bytes are safely preserved.
        // If preservation fails (disk full, permissions), the entry stays in
        // the journal and the worker retries the whole resolution later.
        match dead_letter_write(shared, write, error) {
            Ok(record_path) => {
                resolved_targets.insert(write.target());
                dead_lettered_paths.push(write.path.clone());
                dead_lettered_targets.insert(write.target(), error.clone());
                tracing::error!(
                    journal = %shared.journal_path.display(),
                    path = %write.path,
                    staged_bytes = write.size_bytes,
                    dead_letter = %record_path.display(),
                    error = %error,
                    "vfs write rejected by gateway; preserved in dead letter and dropped from journal"
                );
            }
            Err(record_error) => {
                preservation_failures.push(write.target());
                tracing::error!(
                    journal = %shared.journal_path.display(),
                    path = %write.path,
                    error = %error,
                    record_error = %record_error,
                    "vfs write rejected by gateway; dead-letter preservation failed, retaining entry"
                );
            }
        }
    }
    let resolved_ids = batch
        .iter()
        .filter(|write| resolved_targets.contains(&write.target()))
        .map(|write| write.id)
        .collect::<HashSet<_>>();
    state
        .pending
        .retain(|write| !resolved_ids.contains(&write.id));
    let committed_batch = coalesced
        .iter()
        .filter(|write| resolution.committed.contains_key(&write.target()))
        .cloned()
        .collect::<Vec<_>>();
    rebase_pending_after_commit(&mut state.pending, &committed_batch, &resolution.committed);
    if let Err(error) = rewrite_journal(&shared.journal_path, state) {
        state.pending = pending_before;
        state.dead_letter_error = dead_letter_error_before;
        state.terminal_errors = terminal_errors_before;
        state.last_error = Some(error.to_string());
        return;
    }
    let resolved = batch
        .iter()
        .filter(|write| resolved_ids.contains(&write.id))
        .cloned()
        .collect::<Vec<_>>();
    remove_staged_after_wal(shared, &resolved);
    for write in batch {
        if let Some(error) = dead_lettered_targets.get(&write.target()) {
            state.terminal_errors.insert(
                write.id,
                format!(
                    "vfs write {} rejected by the gateway and dead-lettered: {error}",
                    write.path
                ),
            );
        }
    }
    if !dead_lettered_paths.is_empty() {
        state.dead_letter_error = Some(format!(
            "vfs write(s) rejected by the gateway and dead-lettered under {}: {}",
            shared.journal_path.with_extension("dead-letter").display(),
            dead_lettered_paths.join(", "),
        ));
        if let Some(on_dead_letter) = shared.on_dead_letter.as_ref() {
            for path in &dead_lettered_paths {
                on_dead_letter(path.as_str());
            }
        }
    }
    let mut unresolved = resolution
        .retained
        .iter()
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    unresolved.extend(preservation_failures.into_iter().map(|(path, _)| path));
    state.last_error = if unresolved.is_empty() {
        None
    } else {
        Some(format!(
            "transient vfs write failure for {} path(s), retrying: {}",
            unresolved.len(),
            unresolved.join(", "),
        ))
    };
}

fn dead_letter_write(shared: &Shared, write: &JournalWrite, error: &str) -> Result<PathBuf> {
    let dead_letter_dir = shared.journal_path.with_extension("dead-letter");
    fs::create_dir_all(&dead_letter_dir).with_context(|| {
        format!(
            "create vfs dead letter directory {}",
            dead_letter_dir.display()
        )
    })?;
    sync_parent_directory(&dead_letter_dir)?;
    let unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    let staged = shared.staging_dir.join(write.staged_file.as_str());
    let preserved_name =
        preserve_staged_write(&staged, &dead_letter_dir, write.id, write.size_bytes)?;
    let record_path = dead_letter_dir.join("records.jsonl");
    let record = serde_json::json!({
        "id": write.id,
        "path": write.path,
        "preserved_file": preserved_name,
        "size_bytes": write.size_bytes,
        "base_content_hash": write.base_content_hash,
        "expected_file_id": write.expected_file_id,
        "error": error,
        "dead_lettered_at_unix": unix_seconds,
    });
    let mut file = open_append(&record_path)?;
    append_json_line(&mut file, &record, "append vfs dead letter record")?;
    Ok(record_path)
}

fn preserve_staged_write(
    staged: &Path,
    dead_letter_dir: &Path,
    id: u64,
    expected_size: u64,
) -> Result<String> {
    // Publish only a completely copied and synced inode. A crash can leave the
    // temporary link behind, so its deterministic per-journal-id name is
    // removed before and after every attempt.
    let temporary = dead_letter_dir.join(format!(".pending-{id}.tmp"));
    remove_dead_letter_temporary(&temporary)?;
    let result = (|| {
        let mut target = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .with_context(|| format!("create rejected vfs write {}", temporary.display()))?;
        let content_hash = stream_exact_file(staged, Some(&mut target), expected_size)?;
        target
            .sync_data()
            .context("sync preserved rejected vfs bytes")?;

        // Content-addressing makes the ambiguous window between publishing
        // the bytes and appending their metadata idempotent. A retry reuses
        // the exact complete file instead of accumulating timestamp suffixes.
        let preserved_name = format!("{content_hash}.bin");
        let preserved = dead_letter_dir.join(preserved_name.as_str());
        match fs::hard_link(&temporary, &preserved) {
            Ok(()) => sync_parent_directory(&preserved)?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing_hash = stream_exact_file(&preserved, None, expected_size)
                    .with_context(|| {
                        format!(
                            "validate existing rejected vfs write {}",
                            preserved.display()
                        )
                    })?;
                if existing_hash != content_hash {
                    return Err(anyhow!(
                        "existing rejected vfs write {} does not match its content hash",
                        preserved.display(),
                    ));
                }
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("publish rejected vfs write {}", preserved.display())
                });
            }
        }
        Ok(preserved_name)
    })();
    let cleanup = remove_dead_letter_temporary(&temporary);
    match (result, cleanup) {
        (Ok(name), Ok(())) => Ok(name),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(error.context(format!(
            "also failed to clean rejected vfs temporary: {cleanup_error}"
        ))),
    }
}

fn stream_exact_file(
    source: &Path,
    mut target: Option<&mut File>,
    expected_size: u64,
) -> Result<String> {
    let source_file = File::open(source)
        .with_context(|| format!("open rejected vfs write {}", source.display()))?;
    let mut source_reader = source_file.take(expected_size.saturating_add(1));
    let mut hasher = chevalier_vfs_hash::ContentHasher::new();
    let mut buffer = [0u8; JOURNAL_READ_BUFFER_BYTES];
    let mut copied = 0u64;
    loop {
        let read = source_reader
            .read(&mut buffer)
            .with_context(|| format!("read rejected vfs write {}", source.display()))?;
        if read == 0 {
            break;
        }
        if let Some(file) = target.as_mut() {
            file.write_all(&buffer[..read])
                .context("stream preserved rejected vfs bytes")?;
        }
        hasher.update(&buffer[..read]);
        copied = copied.saturating_add(read as u64);
    }
    if copied != expected_size {
        return Err(anyhow!(
            "rejected vfs write {} has {} bytes but journal requires {}",
            source.display(),
            copied,
            expected_size,
        ));
    }
    Ok(hasher.finalize())
}

fn remove_dead_letter_temporary(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => sync_parent_directory(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("remove rejected vfs temporary {}", path.display()))
        }
    }
}

fn coalesce_batch(batch: &[JournalWrite]) -> Vec<JournalWrite> {
    let mut writes = Vec::<JournalWrite>::new();
    let mut positions = HashMap::<WriteTarget, usize>::new();
    for write in batch {
        let target = write.target();
        if let Some(position) = positions.get(&target).copied() {
            let base_content_hash = writes[position].base_content_hash.clone();
            writes[position] = JournalWrite {
                base_content_hash,
                ..write.clone()
            };
        } else {
            positions.insert(target, writes.len());
            writes.push(write.clone());
        }
    }
    writes
}

fn rebase_pending_after_commit(
    pending: &mut VecDeque<JournalWrite>,
    committed_batch: &[JournalWrite],
    committed_hashes: &HashMap<WriteTarget, String>,
) {
    let mut committed_bases = HashMap::<WriteTarget, HashSet<Option<String>>>::new();
    for write in committed_batch {
        committed_bases
            .entry(write.target())
            .or_default()
            .insert(write.base_content_hash.clone());
    }
    for write in pending {
        let target = write.target();
        let Some(committed_hash) = committed_hashes.get(&target) else {
            continue;
        };
        if write.base_content_hash.as_ref() == Some(committed_hash) {
            continue;
        }
        if committed_bases
            .get(&target)
            .is_some_and(|bases| bases.contains(&write.base_content_hash))
        {
            write.base_content_hash = Some(committed_hash.clone());
        }
    }
}

/// VFS content identity, from the crate chevalier-vfs hashes with. vmd sends
/// these as CAS preconditions and the gateway compares them against its own
/// hashes, so sharing the implementation is what keeps a divergence from
/// failing every precondition-bearing write as EIO in the guest.
fn content_hash_for_bytes(bytes: &[u8]) -> String {
    chevalier_vfs_hash::hash_bytes(bytes)
}

fn path_surface(scope_path: &str, path: &str) -> &'static str {
    let scoped = if scope_path.is_empty() {
        path.to_string()
    } else {
        format!("{scope_path}/{path}")
    };
    if scoped.contains("/shared") {
        VFS_SURFACE_KIND_VM_SHARED
    } else {
        VFS_SURFACE_KIND_VM_WORKSPACE
    }
}

fn read_journal(path: &Path) -> Result<VecDeque<JournalWrite>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(VecDeque::new()),
        Err(error) => return Err(error).context("open vfs write journal"),
    };
    let reader = BufReader::with_capacity(JOURNAL_READ_BUFFER_BYTES, file);
    let (pending, repair_tail) = decode_journal(reader, path)?;
    if repair_tail {
        rewrite_pending(path, &pending)?;
    }
    Ok(pending)
}

fn decode_journal(mut reader: impl BufRead, path: &Path) -> Result<(VecDeque<JournalWrite>, bool)> {
    let mut pending = VecDeque::new();
    let mut repair_tail = false;
    let mut line = Vec::new();
    let mut line_number = 0usize;
    loop {
        let Some(record) =
            read_bounded_record(&mut reader, &mut line, path, "read vfs write journal")?
        else {
            break;
        };
        line_number += 1;
        if record.oversized {
            if record.terminated {
                return Err(anyhow!(
                    "vfs write journal record {} in {} exceeds the {} byte maximum",
                    line_number,
                    path.display(),
                    MAX_JOURNAL_RECORD_BYTES,
                ));
            }
            // An oversized unterminated record is necessarily the final
            // append. Treat it exactly like any other torn tail, but drain it
            // without retaining more than MAX_JOURNAL_RECORD_BYTES.
            repair_tail = true;
            tracing::warn!(
                journal = %path.display(),
                record = line_number,
                maximum_bytes = MAX_JOURNAL_RECORD_BYTES,
                "truncating oversized torn final vfs write journal record"
            );
            continue;
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            if !record.terminated {
                repair_tail = true;
            }
            continue;
        }
        match serde_json::from_slice::<JournalWrite>(&line) {
            Ok(write) => {
                pending.push_back(write);
                if !record.terminated {
                    repair_tail = true;
                }
            }
            Err(error) if !record.terminated => {
                // An append can be torn only at the unterminated tail. Drop
                // that incomplete record and canonicalize before reopening
                // for append; accepting any earlier corruption would silently
                // reorder or lose writes.
                repair_tail = true;
                tracing::warn!(
                    journal = %path.display(),
                    error = %error,
                    "truncating torn final vfs write journal record"
                );
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "decode vfs write journal record {} in {}",
                        line_number,
                        path.display()
                    )
                });
            }
        }
    }
    Ok((pending, repair_tail))
}

struct JournalRecordRead {
    terminated: bool,
    oversized: bool,
}

fn read_bounded_record(
    reader: &mut impl BufRead,
    record: &mut Vec<u8>,
    path: &Path,
    context: &'static str,
) -> Result<Option<JournalRecordRead>> {
    record.clear();
    let mut read_any = false;
    let mut oversized = false;
    loop {
        let buffer = reader
            .fill_buf()
            .with_context(|| format!("{context} {}", path.display()))?;
        if buffer.is_empty() {
            return Ok(read_any.then_some(JournalRecordRead {
                terminated: false,
                oversized,
            }));
        }
        read_any = true;
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(buffer.len(), |index| index + 1);
        if !oversized {
            let content_bytes = newline.unwrap_or(consumed);
            let remaining = MAX_JOURNAL_RECORD_BYTES.saturating_sub(record.len());
            let retained = content_bytes.min(remaining);
            record.extend_from_slice(&buffer[..retained]);
            oversized = content_bytes > remaining;
        }
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(JournalRecordRead {
                terminated: true,
                oversized,
            }));
        }
    }
}

fn rewrite_journal(path: &Path, state: &mut JournalState) -> Result<()> {
    state.journal_needs_repair = true;
    let rewrite_result = rewrite_pending(path, &state.pending);
    let reopen_result: Result<File> = (|| {
        #[cfg(test)]
        fail_rewrite_if_armed(RewriteFault::ReopenAfterRewrite)?;
        open_append(path)
    })();
    match reopen_result {
        Ok(journal) => {
            state.journal = journal;
            if rewrite_result.is_ok() {
                state.journal_needs_repair = false;
            }
            rewrite_result
        }
        Err(reopen_error) => match rewrite_result {
            Ok(()) => Err(reopen_error),
            Err(error) => Err(error.context(format!(
                "also failed to reopen vfs write journal after rewrite: {reopen_error}"
            ))),
        },
    }
}

fn repair_before_append(path: &Path, state: &mut JournalState) -> Result<()> {
    if !state.journal_needs_repair {
        return Ok(());
    }
    match rewrite_journal(path, state) {
        Ok(()) => {
            state.last_error = None;
            Ok(())
        }
        Err(error) => {
            state.last_error = Some(error.to_string());
            Err(error).context("repair vfs write journal before append")
        }
    }
}

fn rewrite_pending(path: &Path, pending: &VecDeque<JournalWrite>) -> Result<()> {
    let temporary = path.with_extension("jsonl.tmp");
    {
        let mut writer = BufWriter::new(File::create(&temporary)?);
        for write in pending {
            serde_json::to_writer(&mut writer, write).context("rewrite vfs write journal")?;
            writer
                .write_all(b"\n")
                .context("rewrite vfs write journal")?;
        }
        writer.flush().context("flush vfs write journal")?;
        writer
            .get_ref()
            .sync_data()
            .context("sync vfs write journal")?;
    }
    fs::rename(&temporary, path).with_context(|| format!("replace {}", path.display()))?;
    #[cfg(test)]
    fail_rewrite_if_armed(RewriteFault::ParentSyncAfterRename)?;
    sync_parent_directory(path)?;
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum RewriteFault {
    ParentSyncAfterRename,
    ReopenAfterRewrite,
}

#[cfg(test)]
thread_local! {
    static NEXT_REWRITE_FAULT: Cell<Option<RewriteFault>> = const { Cell::new(None) };
}

#[cfg(test)]
fn arm_rewrite_fault(fault: RewriteFault) {
    NEXT_REWRITE_FAULT.with(|slot| slot.set(Some(fault)));
}

#[cfg(test)]
fn fail_rewrite_if_armed(fault: RewriteFault) -> Result<()> {
    NEXT_REWRITE_FAULT.with(|slot| {
        if slot.get() == Some(fault) {
            slot.set(None);
            Err(anyhow!("injected vfs write journal rewrite fault"))
        } else {
            Ok(())
        }
    })
}

fn open_append(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let existed = path.exists();
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    if !existed {
        sync_parent_directory(path)?;
    }
    Ok(file)
}

fn append_json_line(file: &mut File, value: &impl Serialize, context: &'static str) -> Result<()> {
    append_json_line_unsynced(file, value, context)?;
    file.sync_data()
        .with_context(|| format!("sync {context}"))?;
    Ok(())
}

fn append_json_line_unsynced(
    file: &mut File,
    value: &impl Serialize,
    context: &'static str,
) -> Result<()> {
    serde_json::to_writer(&mut *file, value).with_context(|| context)?;
    file.write_all(b"\n").with_context(|| context)?;
    Ok(())
}

fn sync_parent_directory(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("{} has no containing directory", path.display()))?;
    File::open(parent)
        .with_context(|| format!("open containing directory {}", parent.display()))?
        .sync_all()
        .with_context(|| format!("sync containing directory {}", parent.display()))
}

fn validate_staged_writes(staging_dir: &Path, pending: &VecDeque<JournalWrite>) -> Result<()> {
    for write in pending {
        let staged = staging_dir.join(write.staged_file.as_str());
        let metadata = fs::metadata(&staged)
            .with_context(|| format!("validate staged vfs write {}", staged.display()))?;
        if metadata.len() != write.size_bytes {
            return Err(anyhow!(
                "staged vfs write {} has {} bytes but journal requires {}",
                staged.display(),
                metadata.len(),
                write.size_bytes
            ));
        }
    }
    Ok(())
}

fn remove_orphaned_staged_writes(
    staging_dir: &Path,
    pending: &VecDeque<JournalWrite>,
) -> Result<()> {
    let referenced = pending
        .iter()
        .map(|write| write.staged_file.as_str())
        .collect::<HashSet<_>>();
    let mut removed = false;
    for entry in fs::read_dir(staging_dir)
        .with_context(|| format!("list vfs write staging directory {}", staging_dir.display()))?
    {
        let entry = entry.context("read vfs write staging entry")?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if (name.ends_with(".bin") || name.ends_with(".tmp")) && !referenced.contains(name.as_ref())
        {
            fs::remove_file(entry.path()).with_context(|| {
                format!(
                    "remove orphaned staged vfs write {}",
                    entry.path().display()
                )
            })?;
            removed = true;
        }
    }
    if removed {
        File::open(staging_dir)
            .with_context(|| format!("open vfs write staging directory {}", staging_dir.display()))?
            .sync_all()
            .with_context(|| {
                format!("sync vfs write staging directory {}", staging_dir.display())
            })?;
    }
    Ok(())
}

fn remove_staged_after_wal(shared: &Shared, writes: &[JournalWrite]) {
    if writes.is_empty() {
        return;
    }
    let mut removed = false;
    for write in writes {
        let staged = shared.staging_dir.join(write.staged_file.as_str());
        match fs::remove_file(&staged) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(
                journal = %shared.journal_path.display(),
                staged = %staged.display(),
                error = %error,
                "committed vfs staged write cleanup failed"
            ),
        }
    }
    if removed
        && let Err(error) =
            File::open(&shared.staging_dir).and_then(|directory| directory.sync_all())
    {
        tracing::warn!(
            journal = %shared.journal_path.display(),
            staging = %shared.staging_dir.display(),
            error = %error,
            "sync committed vfs staged write cleanup failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::mpsc;

    #[test]
    fn coalescing_keeps_first_precondition_and_latest_bytes() {
        let batch = vec![
            JournalWrite {
                id: 1,
                path: "src/main.rs".to_string(),
                create_mode: None,
                staged_file: "1.bin".to_string(),
                size_bytes: 10,
                base_content_hash: Some("base".to_string()),
                expected_file_id: None,
            },
            JournalWrite {
                id: 2,
                path: "README.md".to_string(),
                create_mode: None,
                staged_file: "2.bin".to_string(),
                size_bytes: 20,
                base_content_hash: None,
                expected_file_id: None,
            },
            JournalWrite {
                id: 3,
                path: "src/main.rs".to_string(),
                create_mode: None,
                staged_file: "3.bin".to_string(),
                size_bytes: 30,
                base_content_hash: Some("intermediate".to_string()),
                expected_file_id: None,
            },
        ];

        let coalesced = coalesce_batch(&batch);
        assert_eq!(coalesced.len(), 2);
        assert_eq!(coalesced[0].staged_file, "3.bin");
        assert_eq!(coalesced[0].size_bytes, 30);
        assert_eq!(coalesced[0].base_content_hash.as_deref(), Some("base"));
        assert_eq!(coalesced[1].path, "README.md");
        assert_eq!(coalesced[1].base_content_hash, None);
    }

    #[test]
    fn pending_creation_reports_latest_size_and_durable_mode() {
        let pending = VecDeque::from([
            JournalWrite {
                id: 1,
                path: "node_modules/pkg/index.js".to_string(),
                staged_file: "1.bin".to_string(),
                size_bytes: 10,
                base_content_hash: Some("absent".to_string()),
                expected_file_id: None,
                create_mode: Some(0o644),
            },
            JournalWrite {
                id: 2,
                path: "node_modules/pkg/index.js".to_string(),
                staged_file: "2.bin".to_string(),
                size_bytes: 42,
                base_content_hash: Some("first".to_string()),
                expected_file_id: None,
                create_mode: None,
            },
        ]);

        let created = pending_created_file(pending.iter(), "node_modules/pkg/index.js").unwrap();
        assert_eq!(created.size_bytes, 42);
        assert_eq!(created.mode, 0o644);
        assert_eq!(created.file_id, None);
        assert!(pending_created_file(pending.iter(), "node_modules/pkg/missing.js").is_none());
    }

    #[test]
    fn descendant_barrier_waits_for_an_earlier_durable_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging directory");
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::new(),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 2,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState {
                pending: VecDeque::new(),
                active_paths: vec!["node_modules/pkg/index.js".to_string()],
                processing: true,
            }),
            durable_enqueues_changed: Condvar::new(),
            journal_path,
            staging_dir,
            on_dead_letter: None,
        });

        // The active path models a group that entered the durability queue
        // before this namespace operation. The barrier may not become visible
        // until that group has published its durable WAL records into `state`.
        let barrier_shared = Arc::clone(&shared);
        let (barrier_tx, barrier_rx) = mpsc::channel();
        let barrier_installer = std::thread::spawn(move || {
            let guard =
                install_descendant_barrier(&barrier_shared, vec!["node_modules/pkg".to_string()]);
            barrier_tx.send(guard).expect("barrier result receiver");
        });
        assert!(
            matches!(
                barrier_rx.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "a later namespace barrier must wait for the already-queued write"
        );

        {
            let mut state = shared.state.lock().expect("journal state");
            state.pending.push_back(JournalWrite {
                id: 1,
                path: "node_modules/pkg/index.js".to_string(),
                staged_file: "1.bin".to_string(),
                size_bytes: 6,
                base_content_hash: None,
                expected_file_id: None,
                create_mode: Some(0o644),
            });
        }
        {
            let mut enqueues = shared
                .durable_enqueues
                .lock()
                .expect("durable enqueue state");
            enqueues.active_paths.clear();
            enqueues.processing = false;
            shared.durable_enqueues_changed.notify_all();
        }
        let second_barrier = barrier_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("later barrier installs after the durable write");
        {
            let state = shared.state.lock().expect("journal state");
            assert_eq!(
                state
                    .pending
                    .iter()
                    .map(|write| write.path.as_str())
                    .collect::<Vec<_>>(),
                vec!["node_modules/pkg/index.js"],
                "the durable write is journaled before the later barrier becomes visible"
            );
        }
        drop(second_barrier);
        barrier_installer.join().expect("barrier installer");
    }

    #[test]
    fn descendant_barrier_drain_ignores_later_unrelated_writes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging directory");
        let write = |id, path: &str| JournalWrite {
            id,
            path: path.to_string(),
            staged_file: format!("{id}.bin"),
            size_bytes: 1,
            base_content_hash: None,
            expected_file_id: None,
            create_mode: Some(0o644),
        };
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([
                    write(1, "node_modules/pkg/index.js"),
                    write(2, "node_modules/other/index.js"),
                ]),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 3,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path,
            staging_dir,
            on_dead_letter: None,
        });

        let guard = install_descendant_barrier(&shared, vec!["node_modules/pkg".to_string()]);
        assert_eq!(guard.prior_write_ids, vec![1]);
        {
            let mut state = shared.state.lock().expect("state");
            state
                .pending
                .push_back(write(3, "node_modules/later/index.js"));
        }

        let drain = guard.drain_handle();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            drain.flush().expect("drain prior matching write");
            done_tx.send(()).expect("report drain");
        });
        assert!(
            matches!(
                done_rx.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "the barrier must still wait for the matching prior write"
        );
        {
            let mut state = shared.state.lock().expect("state");
            state.pending.retain(|write| write.id != 1);
            shared.changed.notify_all();
        }
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("unrelated pending writes do not hold the barrier");
        assert_eq!(
            shared
                .state
                .lock()
                .expect("state")
                .pending
                .iter()
                .map(|write| write.id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        waiter.join().expect("barrier drain waiter");
        drop(guard);
    }

    #[test]
    fn coalescing_never_crosses_stable_file_identity() {
        let entry = |id: u64, expected_file_id: &str, base: &str| JournalWrite {
            id,
            path: "config".to_string(),
            create_mode: None,
            staged_file: format!("{id}.bin"),
            size_bytes: id,
            base_content_hash: Some(base.to_string()),
            expected_file_id: Some(expected_file_id.to_string()),
        };
        let batch = vec![
            entry(1, "inode-old", "old-base"),
            entry(2, "inode-old", "old-intermediate"),
            entry(3, "inode-replacement", "replacement-base"),
        ];

        let coalesced = coalesce_batch(&batch);

        assert_eq!(coalesced.len(), 2);
        assert_eq!(coalesced[0].id, 2, "latest bytes win within one inode");
        assert_eq!(
            coalesced[0].base_content_hash.as_deref(),
            Some("old-base"),
            "the first CAS base remains authoritative within that inode"
        );
        assert_eq!(coalesced[0].expected_file_id.as_deref(), Some("inode-old"));
        assert_eq!(coalesced[1], batch[2]);
    }

    #[test]
    fn successful_batch_rebases_only_its_queued_write_chain() {
        let committed = vec![JournalWrite {
            id: 1,
            path: "src/main.rs".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: 10,
            base_content_hash: Some("base".to_string()),
            expected_file_id: None,
        }];
        let mut pending = VecDeque::from([
            JournalWrite {
                id: 2,
                path: "src/main.rs".to_string(),
                create_mode: None,
                staged_file: "2.bin".to_string(),
                size_bytes: 20,
                base_content_hash: Some("base".to_string()),
                expected_file_id: None,
            },
            JournalWrite {
                id: 3,
                path: "src/main.rs".to_string(),
                create_mode: None,
                staged_file: "3.bin".to_string(),
                size_bytes: 30,
                base_content_hash: Some("external".to_string()),
                expected_file_id: None,
            },
            JournalWrite {
                id: 4,
                path: "README.md".to_string(),
                create_mode: None,
                staged_file: "4.bin".to_string(),
                size_bytes: 40,
                base_content_hash: Some("readme-base".to_string()),
                expected_file_id: None,
            },
        ]);
        rebase_pending_after_commit(
            &mut pending,
            &committed,
            &HashMap::from([(("src/main.rs".to_string(), None), "committed".to_string())]),
        );

        assert_eq!(pending[0].base_content_hash.as_deref(), Some("committed"));
        assert_eq!(pending[1].base_content_hash.as_deref(), Some("external"));
        assert_eq!(pending[2].base_content_hash.as_deref(), Some("readme-base"));
    }

    #[test]
    fn successful_batch_never_rebases_a_reused_path_with_another_identity() {
        let entry = |id: u64, expected_file_id: &str| JournalWrite {
            id,
            path: "config".to_string(),
            create_mode: None,
            staged_file: format!("{id}.bin"),
            size_bytes: id,
            base_content_hash: Some("same-base".to_string()),
            expected_file_id: Some(expected_file_id.to_string()),
        };
        let committed = vec![entry(1, "inode-old")];
        let mut pending = VecDeque::from([entry(2, "inode-old"), entry(3, "inode-replacement")]);

        rebase_pending_after_commit(
            &mut pending,
            &committed,
            &HashMap::from([(
                ("config".to_string(), Some("inode-old".to_string())),
                "old-committed".to_string(),
            )]),
        );

        assert_eq!(
            pending[0].base_content_hash.as_deref(),
            Some("old-committed")
        );
        assert_eq!(
            pending[1].base_content_hash.as_deref(),
            Some("same-base"),
            "a path replacement is a separate WAL chain"
        );
    }

    /// Pins the same vector as `pack.rs` in chevalier-vfs. If the gateway ever
    /// changes content-hash algorithm without vmd following, this fails instead
    /// of every mounted delete/overwrite failing at runtime.
    #[test]
    fn content_hash_matches_the_configured_algorithms_vector() {
        assert_eq!(
            content_hash_for_bytes(b""),
            chevalier_vfs_hash::algorithm().empty_vector(),
            "the mount must hash with the algorithm the gateway is configured for"
        );
    }

    #[test]
    fn flush_waits_for_transient_failure_to_recover() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging dir");
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([JournalWrite {
                    id: 1,
                    path: "src/main.rs".to_string(),
                    create_mode: None,
                    staged_file: "1.bin".to_string(),
                    size_bytes: 4,
                    base_content_hash: None,
                    expected_file_id: None,
                }]),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 2,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path,
            staging_dir,
            on_dead_letter: None,
        });
        let worker_state = Arc::clone(&shared);
        let recovery = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            {
                let mut state = worker_state.state.lock().expect("state");
                state.last_error = Some("transient gateway failure".to_string());
                worker_state.changed.notify_all();
            }
            std::thread::sleep(Duration::from_millis(10));
            let mut state = worker_state.state.lock().expect("state");
            state.pending.clear();
            state.last_error = None;
            worker_state.changed.notify_all();
        });
        let journal = WriteJournal {
            shared,
            worker: Mutex::new(None),
        };

        journal
            .flush()
            .expect("flush should survive transient error");
        recovery.join().expect("recovery thread");
    }

    #[test]
    fn flush_reports_journal_rewrite_failure_after_remote_completion() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging dir");
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::new(),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 1,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: Some("rewrite failed".to_string()),
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path,
            staging_dir,
            on_dead_letter: None,
        });
        let journal = WriteJournal {
            shared,
            worker: Mutex::new(None),
        };

        assert_eq!(
            journal
                .flush()
                .expect_err("flush must surface failure")
                .to_string(),
            "rewrite failed",
        );
    }

    #[test]
    fn rejected_status_is_extracted_through_anyhow_chains() {
        let rejected = anyhow::Error::new(super::super::client::VfsRequestStatusError {
            status: reqwest::StatusCode::CONFLICT,
        })
        .context("vfs request failed: 409 Conflict precondition failed for write-many");
        assert_eq!(
            rejected_request_status(&rejected),
            Some(reqwest::StatusCode::CONFLICT)
        );

        let transport = anyhow!("send vfs request");
        assert_eq!(rejected_request_status(&transport), None);

        let server_error = anyhow::Error::new(super::super::client::VfsRequestStatusError {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        })
        .context("vfs request failed: 500");
        assert_eq!(rejected_request_status(&server_error), None);
    }

    #[test]
    fn write_staging_temp_name_matcher_accepts_only_uuid_stamped_temporaries() {
        // Exactly the `.{name}.{uuid}.tmp` shape install_writes stages under.
        let uuid = uuid::Uuid::new_v4();
        assert!(is_write_staging_temp_name(&format!(".main.rs.{uuid}.tmp")));
        assert!(is_write_staging_temp_name(&format!(".vfs.{uuid}.tmp")));
        // A dotted original filename keeps its dots ahead of the uuid segment.
        assert!(is_write_staging_temp_name(&format!(
            ".archive.tar.gz.{uuid}.tmp"
        )));
        // Uppercase hex is still a valid uuid shape.
        assert!(is_write_staging_temp_name(
            ".data.AB1279EF-0000-4000-8000-0123456789AB.tmp"
        ));

        // Ordinary dotfiles and near-misses are not residue.
        assert!(!is_write_staging_temp_name(".gitignore"));
        assert!(!is_write_staging_temp_name(".env.tmp"));
        assert!(!is_write_staging_temp_name(".a.b.tmp"));
        assert!(!is_write_staging_temp_name("main.rs"));
        assert!(!is_write_staging_temp_name(&format!("main.rs.{uuid}.tmp")));
        assert!(!is_write_staging_temp_name(&format!(".main.rs.{uuid}.bak")));
        // A uuid missing a hyphen boundary must not pass the shape check.
        assert!(!is_write_staging_temp_name(
            ".data.0123456789ab4000800001234567890abc.tmp"
        ));
    }

    #[test]
    fn matching_stat_requires_successful_exact_cas_repair_before_commit() {
        let write = JournalWrite {
            id: 1,
            path: "src/main.rs".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: b"desired".len() as u64,
            base_content_hash: Some("old".to_string()),
            expected_file_id: None,
        };
        let desired_hash = content_hash_for_bytes(b"desired");
        let mut submitted_bases = Vec::new();
        let outcome = resolve_rejected_write(
            &write,
            b"desired".to_vec(),
            |remote| {
                submitted_bases.push(remote.base_content_hash);
                if submitted_bases.len() == 1 {
                    Err(
                        anyhow::Error::new(super::super::client::VfsRequestStatusError {
                            status: StatusCode::CONFLICT,
                        })
                        .context("stale write precondition"),
                    )
                } else {
                    Ok(())
                }
            },
            |_| {
                Ok(Some(VisibleWriteState {
                    content_hash: Some(desired_hash.clone()),
                    file_id: None,
                }))
            },
            |_, _| unreachable!("identity-less CAS repair never resolves aliases"),
        );

        assert_eq!(
            outcome,
            RejectedWriteOutcome::Committed(desired_hash.clone())
        );
        assert_eq!(
            submitted_bases,
            vec![Some("old".to_string()), Some(desired_hash)],
            "matching visible bytes must trigger a desired-hash CAS repair"
        );
    }

    #[test]
    fn identity_mismatch_retargets_a_surviving_alias_without_touching_replacement_bytes() {
        let write = JournalWrite {
            id: 1,
            path: "config".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: b"identical".len() as u64,
            base_content_hash: Some("old".to_string()),
            expected_file_id: Some("inode-old".to_string()),
        };
        let desired_hash = content_hash_for_bytes(b"identical");
        let mut submissions = Vec::new();

        let outcome = resolve_rejected_write(
            &write,
            b"identical".to_vec(),
            |remote| {
                submissions.push((
                    remote.path,
                    remote.expected_file_id,
                    remote.base_content_hash,
                ));
                if submissions.len() == 1 {
                    Err(
                        anyhow::Error::new(super::super::client::VfsRequestStatusError {
                            status: StatusCode::PRECONDITION_FAILED,
                        })
                        .context("stable file identity changed"),
                    )
                } else {
                    Ok(())
                }
            },
            |path| {
                assert_eq!(path, "config");
                Ok(Some(VisibleWriteState {
                    content_hash: Some(desired_hash.clone()),
                    file_id: Some("inode-replacement".to_string()),
                }))
            },
            |file_id, excluding_path| {
                assert_eq!(file_id, "inode-old");
                assert_eq!(excluding_path, "config");
                Ok(Some("renamed-config".to_string()))
            },
        );

        assert_eq!(
            outcome,
            RejectedWriteOutcome::Committed(desired_hash.clone())
        );
        assert_eq!(
            submissions,
            [
                (
                    "config".to_string(),
                    Some("inode-old".to_string()),
                    Some("old".to_string()),
                ),
                (
                    "renamed-config".to_string(),
                    Some("inode-old".to_string()),
                    Some("old".to_string()),
                ),
            ],
            "retry must carry identity and the original inode CAS to its surviving alias"
        );
    }

    #[test]
    fn identity_mismatch_with_no_surviving_alias_retires_without_resurrection() {
        let write = JournalWrite {
            id: 1,
            path: "config".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: b"local open bytes".len() as u64,
            base_content_hash: Some("old".to_string()),
            expected_file_id: Some("inode-unlinked".to_string()),
        };
        let mut submissions = 0;

        let outcome = resolve_rejected_write(
            &write,
            b"local open bytes".to_vec(),
            |_| {
                submissions += 1;
                Err(
                    anyhow::Error::new(super::super::client::VfsRequestStatusError {
                        status: StatusCode::CONFLICT,
                    })
                    .context("pathname no longer owns the open inode"),
                )
            },
            |_| {
                Ok(Some(VisibleWriteState {
                    content_hash: Some("replacement-hash".to_string()),
                    file_id: Some("inode-replacement".to_string()),
                }))
            },
            |file_id, excluding_path| {
                assert_eq!(file_id, "inode-unlinked");
                assert_eq!(excluding_path, "config");
                Ok(None)
            },
        );

        assert_eq!(outcome, RejectedWriteOutcome::Retired);
        assert_eq!(
            submissions, 1,
            "last-unlink retirement must not recreate the stale pathname"
        );
    }

    #[test]
    fn failed_exact_cas_repair_retains_the_wal() {
        let write = JournalWrite {
            id: 1,
            path: "src/main.rs".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: b"desired".len() as u64,
            base_content_hash: Some("old".to_string()),
            expected_file_id: None,
        };
        let desired_hash = content_hash_for_bytes(b"desired");
        let mut submissions = 0;
        let outcome = resolve_rejected_write(
            &write,
            b"desired".to_vec(),
            |_| {
                submissions += 1;
                if submissions == 1 {
                    Err(
                        anyhow::Error::new(super::super::client::VfsRequestStatusError {
                            status: StatusCode::CONFLICT,
                        })
                        .context("stale write precondition"),
                    )
                } else {
                    Err(anyhow!("repair response lost"))
                }
            },
            |_| {
                Ok(Some(VisibleWriteState {
                    content_hash: Some(desired_hash.clone()),
                    file_id: None,
                }))
            },
            |_, _| unreachable!("identity-less CAS repair never resolves aliases"),
        );

        assert_eq!(outcome, RejectedWriteOutcome::Retained);
        assert_eq!(submissions, 2);
    }

    #[test]
    fn batch_resolution_dead_letters_rejected_write_and_rebases_committed_chain() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging dir");
        fs::write(staging_dir.join("1.bin"), b"committed bytes").expect("stage 1");
        fs::write(staging_dir.join("2.bin"), b"rejected bytes").expect("stage 2");
        fs::write(staging_dir.join("3.bin"), b"follow-on bytes").expect("stage 3");
        let entry = |id: u64, path: &str, base: Option<&str>| JournalWrite {
            id,
            path: path.to_string(),
            create_mode: None,
            staged_file: format!("{id}.bin"),
            size_bytes: match id {
                1 => b"committed bytes".len() as u64,
                2 => b"rejected bytes".len() as u64,
                3 => b"follow-on bytes".len() as u64,
                _ => unreachable!("test journal id"),
            },
            base_content_hash: base.map(str::to_string),
            expected_file_id: None,
        };
        let rejected = JournalWrite {
            expected_file_id: Some("file-probe".to_string()),
            ..entry(2, "probe.txt", Some("stale"))
        };
        let batch = vec![entry(1, "src/main.rs", Some("base")), rejected.clone()];
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([
                    entry(1, "src/main.rs", Some("base")),
                    rejected.clone(),
                    entry(3, "src/main.rs", Some("base")),
                ]),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 4,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: Some("vfs request failed: 409".to_string()),
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir: staging_dir.clone(),
            on_dead_letter: None,
        };
        let resolution = BatchResolution {
            committed: HashMap::from([(
                ("src/main.rs".to_string(), None),
                "committed-hash".to_string(),
            )]),
            retired: HashSet::new(),
            dead_lettered: vec![(rejected, "vfs request failed: 409 Conflict".to_string())],
            retained: Vec::new(),
        };

        {
            let mut state = shared.state.lock().expect("state");
            apply_batch_resolution(&shared, &mut state, &batch, &batch, resolution);

            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.pending[0].id, 3);
            assert_eq!(
                state.pending[0].base_content_hash.as_deref(),
                Some("committed-hash"),
                "follow-on write for the committed path must rebase onto the committed hash"
            );
            assert_eq!(
                state.last_error, None,
                "resolved batch must clear the error"
            );
            assert!(
                state
                    .dead_letter_error
                    .as_deref()
                    .is_some_and(|error| error.contains("probe.txt")),
                "a dead-letter must latch an error for the next flush waiter"
            );
            assert!(
                !state.terminal_errors.contains_key(&1),
                "the committed operation must not inherit another target's error"
            );
            assert!(
                state
                    .terminal_errors
                    .get(&2)
                    .is_some_and(|error| error.contains("probe.txt")),
                "the rejected operation must retain its own terminal error"
            );
        }

        let dead_letter_dir = journal_path.with_extension("dead-letter");
        let records = fs::read_to_string(dead_letter_dir.join("records.jsonl")).expect("records");
        assert!(records.contains("probe.txt"));
        assert!(records.contains("409"));
        let record: serde_json::Value =
            serde_json::from_str(records.lines().next().expect("one record")).expect("json");
        assert_eq!(record["expected_file_id"], "file-probe");
        let preserved_file = record["preserved_file"].as_str().expect("preserved_file");
        assert_eq!(
            fs::read(dead_letter_dir.join(preserved_file)).expect("preserved bytes"),
            b"rejected bytes",
            "rejected bytes must be preserved, not lost"
        );
        assert!(
            !staging_dir.join("1.bin").exists(),
            "committed staged file is cleaned up"
        );
        assert!(
            !staging_dir.join("2.bin").exists(),
            "dead-lettered staged file is moved out of staging"
        );
        assert!(
            staging_dir.join("3.bin").exists(),
            "still-pending staged file remains"
        );
        let journal_after = fs::read_to_string(&journal_path).expect("journal contents");
        assert!(journal_after.contains("\"id\":3"));
        assert!(!journal_after.contains("probe.txt"));
    }

    #[test]
    fn preservation_failure_retains_entry_instead_of_dropping_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging dir");
        fs::write(staging_dir.join("1.bin"), b"rejected bytes").expect("stage 1");
        // Occupy the dead-letter directory path with a FILE so preservation
        // (create_dir_all) deterministically fails.
        fs::write(journal_path.with_extension("dead-letter"), b"blocker").expect("blocker");
        let entry = JournalWrite {
            id: 1,
            path: "probe.txt".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: 14,
            base_content_hash: Some("stale".to_string()),
            expected_file_id: None,
        };
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([entry.clone()]),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 2,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: Some("vfs request failed: 409".to_string()),
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir: staging_dir.clone(),
            on_dead_letter: None,
        };
        let resolution = BatchResolution {
            committed: HashMap::new(),
            retired: HashSet::new(),
            dead_lettered: vec![(entry.clone(), "vfs request failed: 409".to_string())],
            retained: Vec::new(),
        };

        let mut state = shared.state.lock().expect("state");
        apply_batch_resolution(&shared, &mut state, &[entry], &[], resolution);

        assert_eq!(state.pending.len(), 1, "entry must stay pending");
        assert!(state.last_error.is_some(), "failure must stay loud");
        assert_eq!(state.dead_letter_error, None);
        drop(state);
        assert_eq!(
            fs::read(staging_dir.join("1.bin")).expect("staged bytes survive"),
            b"rejected bytes"
        );
    }

    #[test]
    fn dead_letter_invokes_cache_invalidation_hook() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging dir");
        fs::write(staging_dir.join("1.bin"), b"rejected bytes").expect("stage 1");
        let invalidated = Arc::new(Mutex::new(Vec::<String>::new()));
        let hook_log = Arc::clone(&invalidated);
        let entry = JournalWrite {
            id: 1,
            path: "probe.txt".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: 14,
            base_content_hash: Some("stale".to_string()),
            expected_file_id: None,
        };
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([entry.clone()]),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 2,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: Some("vfs request failed: 409".to_string()),
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir: staging_dir.clone(),
            on_dead_letter: Some(Box::new(move |path| {
                hook_log.lock().expect("hook log").push(path.to_string());
            })),
        };
        let resolution = BatchResolution {
            committed: HashMap::new(),
            retired: HashSet::new(),
            dead_lettered: vec![(entry.clone(), "vfs request failed: 409".to_string())],
            retained: Vec::new(),
        };

        let mut state = shared.state.lock().expect("state");
        apply_batch_resolution(&shared, &mut state, &[entry], &[], resolution);
        assert!(state.pending.is_empty());
        drop(state);

        assert_eq!(
            invalidated.lock().expect("hook log").as_slice(),
            ["probe.txt".to_string()]
        );
    }

    #[test]
    fn pending_path_check_is_path_scoped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging dir");
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([JournalWrite {
                    id: 1,
                    path: "logs/api.log".to_string(),
                    create_mode: None,
                    staged_file: "1.bin".to_string(),
                    size_bytes: 4,
                    base_content_hash: None,
                    expected_file_id: None,
                }]),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 2,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path,
            staging_dir,
            on_dead_letter: None,
        });
        let journal = WriteJournal {
            shared,
            worker: Mutex::new(None),
        };

        assert!(journal.has_pending_path("logs/api.log"));
        assert!(!journal.has_pending_path("src/main.rs"));

        journal
            .shared
            .state
            .lock()
            .expect("journal state")
            .pending
            .clear();
    }

    #[test]
    fn pending_folded_creates_are_mount_local_directory_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging dir");
        let pending = VecDeque::from([
            JournalWrite {
                id: 1,
                path: "tree/new".to_string(),
                create_mode: Some(0o755),
                staged_file: "1.bin".to_string(),
                size_bytes: 7,
                base_content_hash: Some("absent".to_string()),
                expected_file_id: None,
            },
            JournalWrite {
                id: 2,
                path: "tree/existing".to_string(),
                create_mode: None,
                staged_file: "2.bin".to_string(),
                size_bytes: 11,
                base_content_hash: Some("old".to_string()),
                expected_file_id: Some("existing-id".to_string()),
            },
            JournalWrite {
                id: 3,
                path: "tree/nested/child".to_string(),
                create_mode: Some(0o644),
                staged_file: "3.bin".to_string(),
                size_bytes: 13,
                base_content_hash: Some("absent".to_string()),
                expected_file_id: None,
            },
        ]);
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending,
                journal: open_append(&journal_path).expect("journal"),
                next_id: 4,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path,
            staging_dir,
            on_dead_letter: None,
        });
        let journal = WriteJournal {
            shared,
            worker: Mutex::new(None),
        };
        let existing = VfsDirEntry {
            name: "existing".to_string(),
            kind: "file".to_string(),
            size_bytes: 3,
            file_id: Some("existing-id".to_string()),
            link_count: 1,
            link_target: None,
            content_hash: Some("old".to_string()),
            executable: false,
            mode: Some(0o644),
            updated_at: None,
        };

        let (entries, applied) = journal
            .project_directory("tree", vec![existing])
            .expect("project pending writes");
        assert!(applied);
        assert_eq!(
            entries.len(),
            2,
            "nested descendants are not direct entries"
        );
        let new = entries
            .iter()
            .find(|entry| entry.name == "new")
            .expect("folded creation is visible");
        assert_eq!(new.size_bytes, 7);
        assert_eq!(new.mode, Some(0o755));
        assert!(new.executable);
        let existing = entries
            .iter()
            .find(|entry| entry.name == "existing")
            .expect("existing write remains visible");
        assert_eq!(existing.size_bytes, 11);
        assert_eq!(existing.file_id.as_deref(), Some("existing-id"));
        assert_eq!(existing.content_hash, None);

        let (unrelated, applied) = journal
            .project_directory("other", Vec::new())
            .expect("project unrelated directory");
        assert!(!applied);
        assert!(unrelated.is_empty());

        journal
            .shared
            .state
            .lock()
            .expect("journal state")
            .pending
            .clear();
    }

    #[test]
    fn exact_flush_barrier_ignores_unrelated_worker_error_and_consumes_only_its_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging dir");
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::new(),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 3,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: Some("transient failure for another pathname".to_string()),
                dead_letter_error: None,
                terminal_errors: HashMap::from([(
                    2,
                    "vfs write rejected for probe.txt".to_string(),
                )]),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path,
            staging_dir,
            on_dead_letter: None,
        });
        let journal = WriteJournal {
            shared,
            worker: Mutex::new(None),
        };

        journal
            .flush_through(1)
            .expect("another path's transient error must not fail this barrier");
        let error = journal
            .flush_through(2)
            .expect_err("the rejected operation observes its own terminal error");
        assert!(error.to_string().contains("probe.txt"));
        journal
            .flush_through(2)
            .expect("the exact deferred error is consumed once");
    }

    #[test]
    fn large_write_wal_streams_across_small_reader_buffers() {
        const RECORDS: u64 = 12_000;
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        {
            let mut writer = BufWriter::new(File::create(&journal_path).expect("journal"));
            for id in 1..=RECORDS {
                serde_json::to_writer(
                    &mut writer,
                    &JournalWrite {
                        id,
                        path: format!("src/generated/{id:05}/module.rs"),
                        create_mode: None,
                        staged_file: format!("{id}.bin"),
                        size_bytes: id,
                        base_content_hash: Some(format!("base-{id}")),
                        expected_file_id: None,
                    },
                )
                .expect("serialize");
                writer.write_all(b"\n").expect("delimiter");
            }
            writer.flush().expect("flush");
        }

        let reader = BufReader::with_capacity(31, File::open(&journal_path).expect("open"));
        let (pending, repair_tail) =
            decode_journal(reader, &journal_path).expect("decode large WAL");
        assert!(!repair_tail);
        assert_eq!(pending.len(), RECORDS as usize);
        assert_eq!(pending.front().expect("first").id, 1);
        assert_eq!(pending.back().expect("last").id, RECORDS);
    }

    #[test]
    fn restart_preserves_identity_preconditions_and_accepts_legacy_records() {
        let path = Path::new("memory-write-journal.jsonl");
        let legacy =
            br#"{"id":1,"path":"legacy","staged_file":"1.bin","size_bytes":1,"base_content_hash":null}"#;
        let identity_aware = JournalWrite {
            id: 2,
            path: "config".to_string(),
            create_mode: None,
            staged_file: "2.bin".to_string(),
            size_bytes: 2,
            base_content_hash: Some("base".to_string()),
            expected_file_id: Some("inode-stable".to_string()),
        };
        let mut bytes = legacy.to_vec();
        bytes.push(b'\n');
        bytes.extend(serde_json::to_vec(&identity_aware).expect("serialize identity-aware WAL"));
        bytes.push(b'\n');

        let (reopened, repair_tail) =
            decode_journal(BufReader::new(Cursor::new(bytes)), path).expect("restart WAL");

        assert!(!repair_tail);
        assert_eq!(reopened.len(), 2);
        assert_eq!(
            reopened[0].expected_file_id, None,
            "pre-upgrade WAL entries remain readable"
        );
        assert_eq!(reopened[1], identity_aware);
    }

    #[test]
    fn oversized_write_wal_record_is_bounded_and_classified_by_termination() {
        let path = Path::new("memory-write-journal.jsonl");
        let mut oversized_tail =
            BufReader::with_capacity(17, Cursor::new(vec![b'x'; MAX_JOURNAL_RECORD_BYTES + 4096]));
        let mut retained = Vec::new();
        let record = read_bounded_record(
            &mut oversized_tail,
            &mut retained,
            path,
            "read test write journal",
        )
        .expect("read")
        .expect("record");
        assert!(record.oversized);
        assert!(!record.terminated);
        assert_eq!(
            retained.len(),
            MAX_JOURNAL_RECORD_BYTES,
            "corrupt tail retention is capped even while the reader drains to EOF"
        );

        let first = JournalWrite {
            id: 1,
            path: "first.txt".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: 5,
            base_content_hash: None,
            expected_file_id: None,
        };
        let mut torn_bytes = serde_json::to_vec(&first).expect("serialize");
        torn_bytes.push(b'\n');
        torn_bytes.extend(std::iter::repeat(b'x').take(MAX_JOURNAL_RECORD_BYTES + 4096));
        let (pending, repair_tail) =
            decode_journal(BufReader::with_capacity(23, Cursor::new(torn_bytes)), path)
                .expect("oversized unterminated final append is a torn tail");
        assert_eq!(pending, VecDeque::from([first]));
        assert!(repair_tail);

        let mut complete_bytes = vec![b'x'; MAX_JOURNAL_RECORD_BYTES + 1];
        complete_bytes.push(b'\n');
        let error = decode_journal(
            BufReader::with_capacity(29, Cursor::new(complete_bytes)),
            path,
        )
        .expect_err("oversized terminated record is corrupt");
        assert!(error.to_string().contains("exceeds the"));
    }

    #[test]
    fn large_dead_letter_copy_is_exact_and_streamed() {
        const LARGE_BYTES: u64 = 8 * 1024 * 1024;
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        fs::create_dir_all(&staging_dir).expect("staging");
        let staged = staging_dir.join("1.bin");
        {
            let mut file = File::create(&staged).expect("create staged");
            let mut chunk = [0u8; JOURNAL_READ_BUFFER_BYTES];
            for (index, byte) in chunk.iter_mut().enumerate() {
                *byte = (index % 251) as u8;
            }
            for _ in 0..(LARGE_BYTES / chunk.len() as u64) {
                file.write_all(&chunk).expect("write staged");
            }
            file.sync_data().expect("sync staged");
        }
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::new(),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 2,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir: staging_dir.clone(),
            on_dead_letter: None,
        };
        let write = JournalWrite {
            id: 1,
            path: "large.bin".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: LARGE_BYTES,
            base_content_hash: None,
            expected_file_id: None,
        };

        dead_letter_write(&shared, &write, "rejected").expect("dead letter");
        let records = fs::read_to_string(
            journal_path
                .with_extension("dead-letter")
                .join("records.jsonl"),
        )
        .expect("records");
        let record: serde_json::Value =
            serde_json::from_str(records.lines().next().expect("record")).expect("json");
        let preserved = journal_path
            .with_extension("dead-letter")
            .join(record["preserved_file"].as_str().expect("preserved name"));
        assert_eq!(
            fs::metadata(&preserved).expect("metadata").len(),
            LARGE_BYTES
        );
        assert_eq!(
            stream_exact_file(&staged, None, LARGE_BYTES).expect("source hash"),
            stream_exact_file(&preserved, None, LARGE_BYTES).expect("preserved hash"),
        );
        assert!(staged.exists(), "WAL transition owns staged cleanup");
    }

    #[test]
    fn dead_letter_copy_cleans_partial_files_and_reuses_ambiguous_publish() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = dir.path().join("writes");
        let dead_letter_dir = journal_path.with_extension("dead-letter");
        fs::create_dir_all(&staging_dir).expect("staging");
        fs::create_dir_all(&dead_letter_dir).expect("dead letter");
        let staged = staging_dir.join("1.bin");
        fs::write(&staged, b"four").expect("stage");

        for expected in [3, 5] {
            preserve_staged_write(&staged, &dead_letter_dir, expected, expected)
                .expect_err("short and long staged files are rejected");
            assert!(
                fs::read_dir(&dead_letter_dir).expect("list").all(|entry| {
                    let name = entry.expect("entry").file_name();
                    let name = name.to_string_lossy();
                    !name.ends_with(".tmp") && !name.ends_with(".bin")
                }),
                "a failed bounded copy leaves no partial preserved file"
            );
        }

        let write = JournalWrite {
            id: 1,
            path: "four.txt".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: 4,
            base_content_hash: None,
            expected_file_id: None,
        };
        fs::create_dir(dead_letter_dir.join("records.jsonl")).expect("metadata blocker");
        dead_letter_write(
            &Shared {
                state: Mutex::new(JournalState {
                    pending: VecDeque::new(),
                    journal: open_append(&journal_path).expect("journal"),
                    next_id: 2,
                    force_flush: false,
                    flushing: false,
                    stop: false,
                    journal_needs_repair: false,
                    last_error: None,
                    dead_letter_error: None,
                    terminal_errors: HashMap::new(),
                    descendant_barriers: Vec::new(),
                }),
                changed: Condvar::new(),
                durable_enqueues: Mutex::new(DurableEnqueueState::default()),
                durable_enqueues_changed: Condvar::new(),
                journal_path: journal_path.clone(),
                staging_dir: staging_dir.clone(),
                on_dead_letter: None,
            },
            &write,
            "rejected",
        )
        .expect_err("metadata append fails after bytes publish");
        let preserved_before = fs::read_dir(&dead_letter_dir)
            .expect("list")
            .filter_map(|entry| {
                let entry = entry.ok()?;
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".bin")
                    .then_some(entry.path())
            })
            .collect::<Vec<_>>();
        assert_eq!(preserved_before.len(), 1);
        assert!(fs::read_dir(&dead_letter_dir).expect("list").all(|entry| {
            !entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));

        fs::remove_dir(dead_letter_dir.join("records.jsonl")).expect("remove blocker");
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::new(),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 2,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir,
            on_dead_letter: None,
        };
        dead_letter_write(&shared, &write, "rejected").expect("retry reuses exact bytes");
        let preserved_after = fs::read_dir(&dead_letter_dir)
            .expect("list")
            .filter_map(|entry| {
                let entry = entry.ok()?;
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".bin")
                    .then_some(entry.path())
            })
            .collect::<Vec<_>>();
        assert_eq!(preserved_after, preserved_before);
        assert!(dead_letter_dir.join("records.jsonl").is_file());
    }

    #[test]
    fn reopen_truncates_only_a_torn_final_write_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let first = JournalWrite {
            id: 1,
            path: "first.txt".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: 5,
            base_content_hash: None,
            expected_file_id: None,
        };
        let mut bytes = serde_json::to_vec(&first).expect("serialize");
        bytes.extend_from_slice(b"\n{\"id\":2,\"path\":\"torn");
        fs::write(&journal_path, bytes).expect("write torn journal");

        assert_eq!(
            read_journal(&journal_path)
                .expect("torn tail is recoverable")
                .iter()
                .map(|write| write.id)
                .collect::<Vec<_>>(),
            [1],
        );
        let repaired = fs::read(&journal_path).expect("repaired journal");
        assert!(repaired.ends_with(b"\n"));
        assert!(!String::from_utf8_lossy(&repaired).contains("\"id\":2"));
    }

    #[test]
    fn reopen_accepts_a_complete_unterminated_tail_and_canonicalizes_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let entry = JournalWrite {
            id: 7,
            path: "complete.txt".to_string(),
            create_mode: None,
            staged_file: "7.bin".to_string(),
            size_bytes: 8,
            base_content_hash: Some("base".to_string()),
            expected_file_id: None,
        };
        fs::write(
            &journal_path,
            serde_json::to_vec(&entry).expect("serialize"),
        )
        .expect("write unterminated journal");

        let reopened = read_journal(&journal_path).expect("complete tail is valid");
        assert_eq!(reopened.len(), 1);
        assert_eq!(reopened[0].id, 7);
        assert!(
            fs::read(&journal_path)
                .expect("canonical journal")
                .ends_with(b"\n")
        );
    }

    #[test]
    fn reopen_rejects_interior_write_journal_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        fs::write(
            &journal_path,
            b"{\"id\":1,\"path\":\"a\",\"staged_file\":\"1.bin\",\"size_bytes\":1,\"base_content_hash\":null}\n{broken}\n",
        )
        .expect("write corrupt journal");

        let error = read_journal(&journal_path).expect_err("interior corruption is fatal");
        assert!(error.to_string().contains("record 2"));
    }

    #[test]
    fn wal_rewrite_failure_retains_pending_entry_and_staged_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("writes.jsonl");
        let staging_dir = journal_path.with_extension("writes");
        fs::create_dir_all(&staging_dir).expect("staging");
        let entry = JournalWrite {
            id: 1,
            path: "src/main.rs".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: 15,
            base_content_hash: Some("base".to_string()),
            expected_file_id: None,
        };
        fs::write(staging_dir.join("1.bin"), b"committed bytes").expect("stage");
        fs::write(
            &journal_path,
            format!("{}\n", serde_json::to_string(&entry).expect("serialize")),
        )
        .expect("journal");
        // Occupying the atomic-rewrite temporary path with a directory is a
        // deterministic crash-point stand-in for an I/O failure after the
        // remote write completed but before the WAL transition committed.
        fs::create_dir(journal_path.with_extension("jsonl.tmp")).expect("rewrite blocker");
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([entry.clone()]),
                journal: open_append(&journal_path).expect("open journal"),
                next_id: 2,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
                terminal_errors: HashMap::new(),
                descendant_barriers: Vec::new(),
            }),
            changed: Condvar::new(),
            durable_enqueues: Mutex::new(DurableEnqueueState::default()),
            durable_enqueues_changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir: staging_dir.clone(),
            on_dead_letter: None,
        };
        let mut state = shared.state.lock().expect("state");
        apply_batch_resolution(
            &shared,
            &mut state,
            std::slice::from_ref(&entry),
            std::slice::from_ref(&entry),
            BatchResolution {
                committed: HashMap::from([(
                    entry.target(),
                    content_hash_for_bytes(b"committed bytes"),
                )]),
                retired: HashSet::new(),
                dead_lettered: Vec::new(),
                retained: Vec::new(),
            },
        );

        assert_eq!(
            state.pending,
            VecDeque::from([entry]),
            "in-memory replay state must match the still-old durable WAL"
        );
        assert!(state.last_error.is_some());
        drop(state);
        assert_eq!(
            fs::read(staging_dir.join("1.bin")).expect("staged bytes survive"),
            b"committed bytes"
        );
        assert!(
            fs::read_to_string(&journal_path)
                .expect("old WAL survives")
                .contains("src/main.rs")
        );
    }

    #[test]
    fn post_rename_rewrite_fault_must_repair_live_wal_before_later_enqueue() {
        for fault in [
            RewriteFault::ParentSyncAfterRename,
            RewriteFault::ReopenAfterRewrite,
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let journal_path = dir.path().join("writes.jsonl");
            let staging_dir = journal_path.with_extension("writes");
            fs::create_dir_all(&staging_dir).expect("staging");
            let first = JournalWrite {
                id: 1,
                path: "first.txt".to_string(),
                create_mode: None,
                staged_file: "1.bin".to_string(),
                size_bytes: 5,
                base_content_hash: None,
                expected_file_id: None,
            };
            fs::write(staging_dir.join("1.bin"), b"first").expect("stage first");
            fs::write(
                &journal_path,
                format!("{}\n", serde_json::to_string(&first).expect("serialize")),
            )
            .expect("journal");
            let shared = Arc::new(Shared {
                state: Mutex::new(JournalState {
                    pending: VecDeque::from([first]),
                    journal: open_append(&journal_path).expect("open journal"),
                    next_id: 2,
                    force_flush: false,
                    flushing: false,
                    stop: false,
                    journal_needs_repair: false,
                    last_error: None,
                    dead_letter_error: None,
                    terminal_errors: HashMap::new(),
                    descendant_barriers: Vec::new(),
                }),
                changed: Condvar::new(),
                durable_enqueues: Mutex::new(DurableEnqueueState::default()),
                durable_enqueues_changed: Condvar::new(),
                journal_path: journal_path.clone(),
                staging_dir,
                on_dead_letter: None,
            });
            {
                let mut state = shared.state.lock().expect("state");
                arm_rewrite_fault(fault);
                let error = rewrite_journal(&journal_path, &mut state)
                    .expect_err("post-rename rewrite fault");
                state.last_error = Some(error.to_string());
                assert!(state.journal_needs_repair);
            }
            let journal = WriteJournal {
                shared: Arc::clone(&shared),
                worker: Mutex::new(None),
            };

            arm_rewrite_fault(fault);
            journal
                .enqueue("later.txt", b"later", None, None)
                .expect_err("append cannot bypass a failed canonical repair");
            {
                let state = shared.state.lock().expect("state");
                assert!(state.journal_needs_repair);
                assert!(state.last_error.is_some(), "repair error stays latched");
                assert_eq!(state.next_id, 2, "failed repair allocates no write id");
            }

            journal
                .enqueue("later.txt", b"later", None, None)
                .expect("next append repairs and reanchors the live WAL");
            assert_eq!(
                read_journal(&journal_path)
                    .expect("reopen live WAL")
                    .iter()
                    .map(|write| (write.id, write.path.clone()))
                    .collect::<Vec<_>>(),
                [(1, "first.txt".to_string()), (2, "later.txt".to_string())],
            );
            {
                let mut state = shared.state.lock().expect("state");
                assert!(!state.journal_needs_repair);
                assert_eq!(state.last_error, None);
                state.pending.clear();
            }
            drop(journal);
        }
    }

    #[test]
    fn reopen_fails_loudly_when_wal_references_missing_staged_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let entry = JournalWrite {
            id: 1,
            path: "missing.txt".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: 7,
            base_content_hash: None,
            expected_file_id: None,
        };
        let error = validate_staged_writes(dir.path(), &VecDeque::from([entry]))
            .expect_err("missing staged content is corruption, not an empty write");
        assert!(error.to_string().contains("validate staged vfs write"));
    }

    #[test]
    fn reopen_removes_only_unreferenced_staging_artifacts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let entry = JournalWrite {
            id: 1,
            path: "kept.txt".to_string(),
            create_mode: None,
            staged_file: "1.bin".to_string(),
            size_bytes: 4,
            base_content_hash: None,
            expected_file_id: None,
        };
        fs::write(dir.path().join("1.bin"), b"kept").expect("referenced");
        fs::write(dir.path().join("2.bin"), b"orphan").expect("orphan");
        fs::write(dir.path().join("3.tmp"), b"torn").expect("temporary");
        fs::write(dir.path().join("notes.txt"), b"unrelated").expect("unrelated");

        remove_orphaned_staged_writes(dir.path(), &VecDeque::from([entry]))
            .expect("remove staging garbage");

        assert!(dir.path().join("1.bin").exists());
        assert!(!dir.path().join("2.bin").exists());
        assert!(!dir.path().join("3.tmp").exists());
        assert!(dir.path().join("notes.txt").exists());
    }
}
