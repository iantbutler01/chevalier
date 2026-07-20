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
use chevalier_sandbox::vfs::{VFS_SURFACE_KIND_VM_SHARED, VFS_SURFACE_KIND_VM_WORKSPACE};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::runtime::Handle;

use super::client::{RemoteVfsClient, RemoteWrite, rejected_request_status};

const BATCH_DELAY: Duration = Duration::from_millis(8);
const RETRY_DELAY_MIN: Duration = Duration::from_millis(100);
const RETRY_DELAY_MAX: Duration = Duration::from_secs(5);
const FLUSH_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BATCH_WRITES: usize = 256;
const MAX_BATCH_BYTES: u64 = 16 * 1024 * 1024;
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
}

type DeadLetterHook = Box<dyn Fn(&str) + Send + Sync>;

struct Shared {
    state: Mutex<JournalState>,
    changed: Condvar,
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
            }),
            changed: Condvar::new(),
            journal_path: journal_path.to_path_buf(),
            staging_dir,
            on_dead_letter,
        });
        let worker_shared = Arc::clone(&shared);
        let scope_path = scope_path.trim_matches('/').to_string();
        let worker = std::thread::Builder::new()
            .name("chevalier-vfs-writes".to_string())
            .spawn(move || run_worker(worker_shared, client, scope_path, tokio))
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
    ) -> Result<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
        repair_before_append(&self.shared.journal_path, &mut state)?;
        let id = state.next_id;
        state.next_id = state.next_id.saturating_add(1);
        let staged_file = format!("{id}.bin");
        let staged_path = self.shared.staging_dir.join(staged_file.as_str());
        let temporary = staged_path.with_extension("tmp");
        {
            let mut file = File::create(&temporary)
                .with_context(|| format!("create staged vfs write {}", temporary.display()))?;
            file.write_all(bytes).context("stage vfs write bytes")?;
            file.sync_data().context("sync staged vfs write bytes")?;
        }
        fs::rename(&temporary, &staged_path)
            .with_context(|| format!("install staged vfs write {}", staged_path.display()))?;
        sync_parent_directory(&staged_path)?;
        let write = JournalWrite {
            id,
            path: path.to_string(),
            staged_file,
            size_bytes: bytes.len() as u64,
            base_content_hash,
        };
        append_json_line(&mut state.journal, &write, "append vfs write journal")?;
        state.pending.push_back(write);
        state.last_error = None;
        if state.pending.len() >= MAX_BATCH_WRITES {
            state.force_flush = true;
        }
        self.shared.changed.notify_all();
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs write journal lock poisoned"))?;
        state.force_flush = true;
        self.shared.changed.notify_all();
        let deadline = Instant::now() + FLUSH_RETRY_TIMEOUT;
        while !state.pending.is_empty() || state.flushing {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(anyhow!(state.last_error.clone().unwrap_or_else(|| {
                    "timed out flushing vfs write journal".to_string()
                })));
            }
            let waited = self
                .shared
                .changed
                .wait_timeout(state, remaining)
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

    pub fn has_pending_path(&self, path: &str) -> bool {
        self.shared
            .state
            .lock()
            .map(|state| state.pending.iter().any(|write| write.path == path))
            .unwrap_or(true)
    }
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

fn run_worker(shared: Arc<Shared>, client: RemoteVfsClient, scope_path: String, tokio: Handle) {
    let mut retry_delay = RETRY_DELAY_MIN;
    loop {
        let (batch, surface) = {
            let mut state = match shared.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            while state.pending.is_empty() && !state.stop {
                state = match shared.changed.wait(state) {
                    Ok(state) => state,
                    Err(_) => return,
                };
            }
            if state.stop && state.pending.is_empty() {
                return;
            }
            if !state.force_flush && state.pending.len() < MAX_BATCH_WRITES {
                let deadline = Instant::now() + BATCH_DELAY;
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
            state.force_flush = false;
            state.flushing = true;
            (batch, surface)
        };

        let coalesced = coalesce_batch(&batch);
        let writes = coalesced
            .iter()
            .map(|write| {
                fs::read(shared.staging_dir.join(write.staged_file.as_str()))
                    .map(|bytes| RemoteWrite {
                        path: write.path.clone(),
                        bytes,
                        base_content_hash: write.base_content_hash.clone(),
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
                            write.path.clone(),
                            content_hash_for_bytes(write.bytes.as_slice()),
                        )
                    })
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        let result = writes.and_then(|writes| tokio.block_on(client.write_many(writes, surface)));
        // A 4xx means the gateway rejected the batch outright; retrying the
        // same batch can never succeed. Resolve each write individually so one
        // poisoned entry cannot wedge the journal forever.
        let resolution = match &result {
            Err(error) if rejected_request_status(error).is_some() => Some(resolve_rejected_batch(
                &shared, &client, &tokio, &coalesced, surface,
            )),
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
            Ok(()) => {
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
    /// path -> committed content hash, for rebasing queued follow-on writes.
    committed: HashMap<String, String>,
    /// Coalesced writes the gateway rejected with a 4xx, with the error text.
    dead_lettered: Vec<(JournalWrite, String)>,
    /// Paths that hit a transient failure and stay pending.
    retained: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum RejectedWriteOutcome {
    Committed(String),
    DeadLetter(String),
    Retained,
}

fn resolve_rejected_write(
    write: &JournalWrite,
    bytes: Vec<u8>,
    mut submit: impl FnMut(RemoteWrite) -> Result<()>,
    mut current_content_hash: impl FnMut() -> Result<Option<String>>,
) -> RejectedWriteOutcome {
    let content_hash = content_hash_for_bytes(bytes.as_slice());
    let initial = RemoteWrite {
        path: write.path.clone(),
        bytes: bytes.clone(),
        base_content_hash: write.base_content_hash.clone(),
    };
    match submit(initial) {
        Ok(()) => RejectedWriteOutcome::Committed(content_hash),
        Err(error)
            if matches!(
                rejected_request_status(&error),
                Some(StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED)
            ) =>
        {
            // A precondition rejection can mean the write already landed (lost
            // response or a partially committed batch). Matching bytes are not
            // proof of durability: the first request may have made page-cache
            // bytes visible and then failed its file/directory barrier. Force
            // an exact-content CAS repair and retire the WAL only after that
            // repair returns success.
            match current_content_hash() {
                Ok(Some(current)) if current == content_hash => {
                    let repair = RemoteWrite {
                        path: write.path.clone(),
                        bytes,
                        base_content_hash: Some(content_hash.clone()),
                    };
                    match submit(repair) {
                        Ok(()) => RejectedWriteOutcome::Committed(content_hash),
                        Err(_) => RejectedWriteOutcome::Retained,
                    }
                }
                Ok(_) => RejectedWriteOutcome::DeadLetter(error.to_string()),
                Err(_) => RejectedWriteOutcome::Retained,
            }
        }
        Err(error) if rejected_request_status(&error).is_some() => {
            RejectedWriteOutcome::DeadLetter(error.to_string())
        }
        Err(_) => RejectedWriteOutcome::Retained,
    }
}

fn resolve_rejected_batch(
    shared: &Shared,
    client: &RemoteVfsClient,
    tokio: &Handle,
    coalesced: &[JournalWrite],
    surface: &'static str,
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
            |remote| tokio.block_on(client.write_many(vec![remote], surface)),
            || {
                tokio
                    .block_on(client.stat(write.path.as_str()))
                    .map(|metadata| metadata.and_then(|metadata| metadata.content_hash))
            },
        ) {
            RejectedWriteOutcome::Committed(content_hash) => {
                resolution
                    .committed
                    .insert(write.path.clone(), content_hash);
            }
            RejectedWriteOutcome::DeadLetter(error) => {
                resolution.dead_lettered.push((write.clone(), error));
            }
            RejectedWriteOutcome::Retained => {
                resolution.retained.push(write.path.clone());
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
    let mut resolved_paths = HashSet::<&str>::new();
    resolved_paths.extend(resolution.committed.keys().map(String::as_str));
    let mut dead_lettered_paths = Vec::<String>::new();
    let mut preservation_failures = Vec::<String>::new();
    for (write, error) in &resolution.dead_lettered {
        // Only count the entry resolved once its bytes are safely preserved.
        // If preservation fails (disk full, permissions), the entry stays in
        // the journal and the worker retries the whole resolution later.
        match dead_letter_write(shared, write, error) {
            Ok(record_path) => {
                resolved_paths.insert(write.path.as_str());
                dead_lettered_paths.push(write.path.clone());
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
                preservation_failures.push(write.path.clone());
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
        .filter(|write| resolved_paths.contains(write.path.as_str()))
        .map(|write| write.id)
        .collect::<HashSet<_>>();
    state
        .pending
        .retain(|write| !resolved_ids.contains(&write.id));
    let committed_batch = coalesced
        .iter()
        .filter(|write| resolution.committed.contains_key(write.path.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    rebase_pending_after_commit(&mut state.pending, &committed_batch, &resolution.committed);
    if let Err(error) = rewrite_journal(&shared.journal_path, state) {
        state.pending = pending_before;
        state.dead_letter_error = dead_letter_error_before;
        state.last_error = Some(error.to_string());
        return;
    }
    let resolved = batch
        .iter()
        .filter(|write| resolved_ids.contains(&write.id))
        .cloned()
        .collect::<Vec<_>>();
    remove_staged_after_wal(shared, &resolved);
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
    let mut unresolved = resolution.retained.clone();
    unresolved.extend(preservation_failures);
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
    let mut hasher = Sha256::new();
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
    Ok(hex_encode(hasher.finalize().as_ref()))
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
    let mut positions = HashMap::<String, usize>::new();
    for write in batch {
        if let Some(position) = positions.get(write.path.as_str()).copied() {
            let base_content_hash = writes[position].base_content_hash.clone();
            writes[position] = JournalWrite {
                base_content_hash,
                ..write.clone()
            };
        } else {
            positions.insert(write.path.clone(), writes.len());
            writes.push(write.clone());
        }
    }
    writes
}

fn rebase_pending_after_commit(
    pending: &mut VecDeque<JournalWrite>,
    committed_batch: &[JournalWrite],
    committed_hashes: &HashMap<String, String>,
) {
    let mut committed_bases = HashMap::<String, HashSet<Option<String>>>::new();
    for write in committed_batch {
        committed_bases
            .entry(write.path.clone())
            .or_default()
            .insert(write.base_content_hash.clone());
    }
    for write in pending {
        let Some(committed_hash) = committed_hashes.get(write.path.as_str()) else {
            continue;
        };
        if write.base_content_hash.as_ref() == Some(committed_hash) {
            continue;
        }
        if committed_bases
            .get(write.path.as_str())
            .is_some_and(|bases| bases.contains(&write.base_content_hash))
        {
            write.base_content_hash = Some(committed_hash.clone());
        }
    }
}

fn content_hash_for_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(hasher.finalize().as_ref())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
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
    serde_json::to_writer(&mut *file, value).with_context(|| context)?;
    file.write_all(b"\n").with_context(|| context)?;
    file.sync_data()
        .with_context(|| format!("sync {context}"))?;
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

    #[test]
    fn coalescing_keeps_first_precondition_and_latest_bytes() {
        let batch = vec![
            JournalWrite {
                id: 1,
                path: "src/main.rs".to_string(),
                staged_file: "1.bin".to_string(),
                size_bytes: 10,
                base_content_hash: Some("base".to_string()),
            },
            JournalWrite {
                id: 2,
                path: "README.md".to_string(),
                staged_file: "2.bin".to_string(),
                size_bytes: 20,
                base_content_hash: None,
            },
            JournalWrite {
                id: 3,
                path: "src/main.rs".to_string(),
                staged_file: "3.bin".to_string(),
                size_bytes: 30,
                base_content_hash: Some("intermediate".to_string()),
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
    fn successful_batch_rebases_only_its_queued_write_chain() {
        let committed = vec![JournalWrite {
            id: 1,
            path: "src/main.rs".to_string(),
            staged_file: "1.bin".to_string(),
            size_bytes: 10,
            base_content_hash: Some("base".to_string()),
        }];
        let mut pending = VecDeque::from([
            JournalWrite {
                id: 2,
                path: "src/main.rs".to_string(),
                staged_file: "2.bin".to_string(),
                size_bytes: 20,
                base_content_hash: Some("base".to_string()),
            },
            JournalWrite {
                id: 3,
                path: "src/main.rs".to_string(),
                staged_file: "3.bin".to_string(),
                size_bytes: 30,
                base_content_hash: Some("external".to_string()),
            },
            JournalWrite {
                id: 4,
                path: "README.md".to_string(),
                staged_file: "4.bin".to_string(),
                size_bytes: 40,
                base_content_hash: Some("readme-base".to_string()),
            },
        ]);
        rebase_pending_after_commit(
            &mut pending,
            &committed,
            &HashMap::from([("src/main.rs".to_string(), "committed".to_string())]),
        );

        assert_eq!(pending[0].base_content_hash.as_deref(), Some("committed"));
        assert_eq!(pending[1].base_content_hash.as_deref(), Some("external"));
        assert_eq!(pending[2].base_content_hash.as_deref(), Some("readme-base"));
    }

    #[test]
    fn content_hash_matches_sha256_hex() {
        assert_eq!(
            content_hash_for_bytes(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
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
                    staged_file: "1.bin".to_string(),
                    size_bytes: 4,
                    base_content_hash: None,
                }]),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 2,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
            }),
            changed: Condvar::new(),
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
            }),
            changed: Condvar::new(),
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
    fn matching_stat_requires_successful_exact_cas_repair_before_commit() {
        let write = JournalWrite {
            id: 1,
            path: "src/main.rs".to_string(),
            staged_file: "1.bin".to_string(),
            size_bytes: b"desired".len() as u64,
            base_content_hash: Some("old".to_string()),
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
            || Ok(Some(desired_hash.clone())),
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
    fn failed_exact_cas_repair_retains_the_wal() {
        let write = JournalWrite {
            id: 1,
            path: "src/main.rs".to_string(),
            staged_file: "1.bin".to_string(),
            size_bytes: b"desired".len() as u64,
            base_content_hash: Some("old".to_string()),
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
            || Ok(Some(desired_hash.clone())),
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
            staged_file: format!("{id}.bin"),
            size_bytes: match id {
                1 => b"committed bytes".len() as u64,
                2 => b"rejected bytes".len() as u64,
                3 => b"follow-on bytes".len() as u64,
                _ => unreachable!("test journal id"),
            },
            base_content_hash: base.map(str::to_string),
        };
        let batch = vec![
            entry(1, "src/main.rs", Some("base")),
            entry(2, "probe.txt", Some("stale")),
        ];
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([
                    entry(1, "src/main.rs", Some("base")),
                    entry(2, "probe.txt", Some("stale")),
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
            }),
            changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir: staging_dir.clone(),
            on_dead_letter: None,
        };
        let resolution = BatchResolution {
            committed: HashMap::from([("src/main.rs".to_string(), "committed-hash".to_string())]),
            dead_lettered: vec![(
                entry(2, "probe.txt", Some("stale")),
                "vfs request failed: 409 Conflict".to_string(),
            )],
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
        }

        let dead_letter_dir = journal_path.with_extension("dead-letter");
        let records = fs::read_to_string(dead_letter_dir.join("records.jsonl")).expect("records");
        assert!(records.contains("probe.txt"));
        assert!(records.contains("409"));
        let record: serde_json::Value =
            serde_json::from_str(records.lines().next().expect("one record")).expect("json");
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
            staged_file: "1.bin".to_string(),
            size_bytes: 14,
            base_content_hash: Some("stale".to_string()),
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
            }),
            changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir: staging_dir.clone(),
            on_dead_letter: None,
        };
        let resolution = BatchResolution {
            committed: HashMap::new(),
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
            staged_file: "1.bin".to_string(),
            size_bytes: 14,
            base_content_hash: Some("stale".to_string()),
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
            }),
            changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir: staging_dir.clone(),
            on_dead_letter: Some(Box::new(move |path| {
                hook_log.lock().expect("hook log").push(path.to_string());
            })),
        };
        let resolution = BatchResolution {
            committed: HashMap::new(),
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
                    staged_file: "1.bin".to_string(),
                    size_bytes: 4,
                    base_content_hash: None,
                }]),
                journal: open_append(&journal_path).expect("journal"),
                next_id: 2,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
            }),
            changed: Condvar::new(),
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
                        staged_file: format!("{id}.bin"),
                        size_bytes: id,
                        base_content_hash: Some(format!("base-{id}")),
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
            staged_file: "1.bin".to_string(),
            size_bytes: 5,
            base_content_hash: None,
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
            }),
            changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir: staging_dir.clone(),
            on_dead_letter: None,
        };
        let write = JournalWrite {
            id: 1,
            path: "large.bin".to_string(),
            staged_file: "1.bin".to_string(),
            size_bytes: LARGE_BYTES,
            base_content_hash: None,
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
            staged_file: "1.bin".to_string(),
            size_bytes: 4,
            base_content_hash: None,
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
                }),
                changed: Condvar::new(),
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
            }),
            changed: Condvar::new(),
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
            staged_file: "1.bin".to_string(),
            size_bytes: 5,
            base_content_hash: None,
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
            staged_file: "7.bin".to_string(),
            size_bytes: 8,
            base_content_hash: Some("base".to_string()),
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
            staged_file: "1.bin".to_string(),
            size_bytes: 15,
            base_content_hash: Some("base".to_string()),
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
            }),
            changed: Condvar::new(),
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
                    entry.path.clone(),
                    content_hash_for_bytes(b"committed bytes"),
                )]),
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
                staged_file: "1.bin".to_string(),
                size_bytes: 5,
                base_content_hash: None,
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
                }),
                changed: Condvar::new(),
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
                .enqueue("later.txt", b"later", None)
                .expect_err("append cannot bypass a failed canonical repair");
            {
                let state = shared.state.lock().expect("state");
                assert!(state.journal_needs_repair);
                assert!(state.last_error.is_some(), "repair error stays latched");
                assert_eq!(state.next_id, 2, "failed repair allocates no write id");
            }

            journal
                .enqueue("later.txt", b"later", None)
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
            staged_file: "1.bin".to_string(),
            size_bytes: 7,
            base_content_hash: None,
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
            staged_file: "1.bin".to_string(),
            size_bytes: 4,
            base_content_hash: None,
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
