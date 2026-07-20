#[cfg(test)]
use std::cell::Cell;
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chevalier_sandbox::vfs::{
    VFS_SURFACE_KIND_VM_SHARED, VFS_SURFACE_KIND_VM_WORKSPACE, VfsNamespaceMutation,
};
use tokio::runtime::Handle;

use super::client::{RemoteVfsClient, rejected_request_status};

const BATCH_DELAY: Duration = Duration::from_millis(8);
const RETRY_DELAY: Duration = Duration::from_millis(100);
const FLUSH_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BATCH_MUTATIONS: usize = 4096;
const JOURNAL_READ_BUFFER_BYTES: usize = 64 * 1024;
/// Journal records contain metadata and bounded paths, never file payloads.
/// One MiB is far above the supported path envelope while keeping a corrupt
/// or unterminated JSONL record from forcing an unbounded allocation.
const MAX_JOURNAL_RECORD_BYTES: usize = 1024 * 1024;

struct JournalState {
    pending: VecDeque<VfsNamespaceMutation>,
    journal: File,
    force_flush: bool,
    flushing: bool,
    stop: bool,
    /// A rewrite crossed or may have crossed the atomic rename boundary but
    /// did not complete both parent-directory sync and append-handle reopen.
    /// No later append may proceed until the full pending state is rewritten.
    journal_needs_repair: bool,
    last_error: Option<String>,
    /// Latched when mutations were dead-lettered; consumed by the next
    /// flush() so one waiter observes the failure without wedging later ones.
    dead_letter_error: Option<String>,
}

struct Shared {
    state: Mutex<JournalState>,
    changed: Condvar,
    journal_path: PathBuf,
}

pub struct NamespaceJournal {
    shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl NamespaceJournal {
    pub fn open(
        client: RemoteVfsClient,
        scope_path: &str,
        journal_path: &Path,
        tokio: Handle,
    ) -> Result<Self> {
        if let Some(parent) = journal_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "create vfs namespace journal directory {}",
                    parent.display()
                )
            })?;
            sync_parent_directory(parent)?;
        }
        let pending = read_journal(journal_path)?;
        let journal = open_append(journal_path)?;
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending,
                journal,
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
            }),
            changed: Condvar::new(),
            journal_path: journal_path.to_path_buf(),
        });
        let worker_shared = Arc::clone(&shared);
        let scope_path = scope_path.trim_matches('/').to_string();
        let worker = std::thread::Builder::new()
            .name("chevalier-vfs-namespace".to_string())
            .spawn(move || run_worker(worker_shared, client, scope_path, tokio))
            .context("spawn vfs namespace journal worker")?;
        shared.changed.notify_all();
        Ok(Self {
            shared,
            worker: Mutex::new(Some(worker)),
        })
    }

    pub fn enqueue(&self, mutation: VfsNamespaceMutation) -> Result<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs namespace journal lock poisoned"))?;
        repair_before_append(&self.shared.journal_path, &mut state)?;
        append_json_line(
            &mut state.journal,
            &mutation,
            "append vfs namespace journal entry",
        )?;
        state.pending.push_back(mutation);
        state.last_error = None;
        if state.pending.len() >= MAX_BATCH_MUTATIONS {
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
            .map_err(|_| anyhow!("vfs namespace journal lock poisoned"))?;
        state.force_flush = true;
        self.shared.changed.notify_all();
        let deadline = Instant::now() + FLUSH_RETRY_TIMEOUT;
        while !state.pending.is_empty() || state.flushing {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(anyhow!(state.last_error.clone().unwrap_or_else(|| {
                    "timed out flushing vfs namespace journal".to_string()
                })));
            }
            let waited = self
                .shared
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| anyhow!("vfs namespace journal lock poisoned"))?;
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
}

impl Drop for NamespaceJournal {
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
    loop {
        let batch = {
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
            if !state.force_flush && state.pending.len() < MAX_BATCH_MUTATIONS {
                let deadline = Instant::now() + BATCH_DELAY;
                while !state.force_flush
                    && state.pending.len() < MAX_BATCH_MUTATIONS
                    && Instant::now() < deadline
                {
                    let timeout = deadline.saturating_duration_since(Instant::now());
                    let waited = match shared.changed.wait_timeout(state, timeout) {
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
                .map(|mutation| mutation_surface(&scope_path, mutation))
                .unwrap_or(VFS_SURFACE_KIND_VM_WORKSPACE);
            let batch = state
                .pending
                .iter()
                .take(MAX_BATCH_MUTATIONS)
                .take_while(|mutation| mutation_surface(&scope_path, mutation) == surface)
                .cloned()
                .collect::<Vec<_>>();
            state.force_flush = false;
            state.flushing = true;
            (batch, surface)
        };

        let result = tokio.block_on(client.apply_namespace_batch(batch.0.as_slice(), batch.1));
        // A 4xx rejection can never succeed by retrying the same batch. Replay
        // the batch one mutation at a time, in order, so a single rejected
        // mutation is recorded and dropped instead of wedging the journal.
        let resolution = match &result {
            Err(error) if rejected_request_status(error).is_some() => Some(
                resolve_rejected_namespace_batch(&client, &tokio, batch.0.as_slice(), batch.1),
            ),
            _ => None,
        };
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        state.flushing = false;
        if let Some(resolution) = resolution {
            apply_namespace_resolution(&shared, &mut state, resolution);
            shared.changed.notify_all();
            let failed = state.last_error.is_some();
            let stop = state.stop;
            drop(state);
            if stop && failed {
                return;
            }
            if failed {
                std::thread::sleep(RETRY_DELAY);
            }
            continue;
        }
        match result {
            Ok(()) => {
                let recovered = state.last_error.take();
                let pending_before = state.pending.clone();
                for _ in 0..batch.0.len() {
                    state.pending.pop_front();
                }
                if let Err(error) = rewrite_journal(&shared.journal_path, &mut state) {
                    // The durable journal still contains this batch. Preserve
                    // the in-memory copy too so an ambiguous remote completion
                    // is replayed rather than silently forgotten.
                    state.pending = pending_before;
                    state.last_error = Some(error.to_string());
                } else {
                    state.last_error = None;
                    if let Some(error) = recovered {
                        tracing::info!(
                            journal = %shared.journal_path.display(),
                            mutation_count = batch.0.len(),
                            previous_error = %error,
                            "vfs namespace journal replay recovered"
                        );
                    }
                }
            }
            Err(error) => {
                let error = error.to_string();
                if state.last_error.as_deref() != Some(error.as_str()) {
                    tracing::warn!(
                        journal = %shared.journal_path.display(),
                        mutation_count = batch.0.len(),
                        first_mutation = ?batch.0.first(),
                        error = %error,
                        "vfs namespace journal replay failed; retaining mutations for retry"
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
            std::thread::sleep(RETRY_DELAY);
        }
    }
}

struct NamespaceResolution {
    /// Leading mutations conclusively handled (applied or dead-lettered).
    resolved: usize,
    /// Mutations the gateway rejected with a 4xx, keyed by their batch index.
    dead_lettered: Vec<(usize, VfsNamespaceMutation, String)>,
    /// First transient failure; this and later mutations stay pending.
    transient_error: Option<String>,
}

fn resolve_rejected_namespace_batch(
    client: &RemoteVfsClient,
    tokio: &Handle,
    batch: &[VfsNamespaceMutation],
    surface: &'static str,
) -> NamespaceResolution {
    let mut resolution = NamespaceResolution {
        resolved: 0,
        dead_lettered: Vec::new(),
        transient_error: None,
    };
    for (index, mutation) in batch.iter().enumerate() {
        match tokio.block_on(client.apply_namespace_batch(std::slice::from_ref(mutation), surface))
        {
            Ok(()) => resolution.resolved += 1,
            Err(error) if rejected_request_status(&error).is_some() => {
                resolution
                    .dead_lettered
                    .push((index, mutation.clone(), error.to_string()));
                resolution.resolved += 1;
            }
            Err(error) => {
                resolution.transient_error = Some(error.to_string());
                break;
            }
        }
    }
    resolution
}

fn apply_namespace_resolution(
    shared: &Shared,
    state: &mut JournalState,
    resolution: NamespaceResolution,
) {
    let pending_before = state.pending.clone();
    let dead_letter_error_before = state.dead_letter_error.clone();
    let mut preserved_dead_letters = Vec::new();
    let mut preservation_failures = Vec::new();
    for (index, mutation, error) in &resolution.dead_lettered {
        match dead_letter_mutation(&shared.journal_path, mutation, error) {
            Ok(record_path) => {
                preserved_dead_letters.push(*index);
                tracing::error!(
                    journal = %shared.journal_path.display(),
                    mutation = ?mutation,
                    dead_letter = %record_path.display(),
                    error = %error,
                    "vfs namespace mutation rejected by gateway; recorded and dropped from journal"
                );
            }
            Err(record_error) => {
                preservation_failures.push((*index, record_error.to_string()));
                tracing::error!(
                    journal = %shared.journal_path.display(),
                    mutation = ?mutation,
                    error = %error,
                    record_error = %record_error,
                    "vfs namespace mutation rejected by gateway; failed to record dead letter"
                );
            }
        }
    }
    let failed_indices = preservation_failures
        .iter()
        .map(|(index, _)| *index)
        .collect::<std::collections::HashSet<_>>();
    state.pending = state
        .pending
        .drain(..)
        .enumerate()
        .filter_map(|(index, mutation)| {
            (index >= resolution.resolved || failed_indices.contains(&index)).then_some(mutation)
        })
        .collect();
    if let Err(error) = rewrite_journal(&shared.journal_path, state) {
        state.pending = pending_before;
        state.dead_letter_error = dead_letter_error_before;
        state.last_error = Some(error.to_string());
        return;
    }
    if !preserved_dead_letters.is_empty() {
        state.dead_letter_error = Some(format!(
            "vfs namespace mutation(s) rejected by the gateway and dead-lettered: {}",
            resolution
                .dead_lettered
                .iter()
                .filter(|(index, _, _)| preserved_dead_letters.contains(index))
                .map(|(_, mutation, _)| format!("{mutation:?}"))
                .collect::<Vec<_>>()
                .join(", "),
        ));
    }
    let mut failures = preservation_failures
        .into_iter()
        .map(|(index, error)| format!("mutation {index}: {error}"))
        .collect::<Vec<_>>();
    if let Some(error) = resolution.transient_error {
        failures.push(error);
    }
    state.last_error = (!failures.is_empty()).then(|| failures.join("; "));
}

fn dead_letter_mutation(
    journal_path: &Path,
    mutation: &VfsNamespaceMutation,
    error: &str,
) -> Result<PathBuf> {
    let record_path = journal_path.with_extension("dead-letter.jsonl");
    let unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    let record = serde_json::json!({
        "mutation": mutation,
        "error": error,
        "dead_lettered_at_unix": unix_seconds,
    });
    let mut file = open_append(&record_path)?;
    append_json_line(&mut file, &record, "append vfs dead letter record")?;
    Ok(record_path)
}

fn mutation_surface(scope_path: &str, mutation: &VfsNamespaceMutation) -> &'static str {
    let path = mutation
        .paths()
        .into_iter()
        .find(|path| !path.is_empty())
        .unwrap_or_default();
    let scoped = if scope_path.is_empty() {
        path.to_string()
    } else if path.is_empty() {
        scope_path.to_string()
    } else {
        format!("{scope_path}/{path}")
    };
    if scoped.contains("/shared") {
        VFS_SURFACE_KIND_VM_SHARED
    } else {
        VFS_SURFACE_KIND_VM_WORKSPACE
    }
}

fn read_journal(path: &Path) -> Result<VecDeque<VfsNamespaceMutation>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(VecDeque::new()),
        Err(error) => return Err(error).context("open vfs namespace journal"),
    };
    let reader = BufReader::with_capacity(JOURNAL_READ_BUFFER_BYTES, file);
    let (pending, repair_tail) = decode_journal(reader, path)?;
    if repair_tail {
        rewrite_pending(path, &pending)?;
    }
    Ok(pending)
}

fn decode_journal(
    mut reader: impl BufRead,
    path: &Path,
) -> Result<(VecDeque<VfsNamespaceMutation>, bool)> {
    let mut pending = VecDeque::new();
    let mut repair_tail = false;
    let mut line = Vec::new();
    let mut line_number = 0usize;
    loop {
        let Some(record) =
            read_bounded_record(&mut reader, &mut line, path, "read vfs namespace journal")?
        else {
            break;
        };
        line_number += 1;
        if record.oversized {
            if record.terminated {
                return Err(anyhow!(
                    "vfs namespace journal record {} in {} exceeds the {} byte maximum",
                    line_number,
                    path.display(),
                    MAX_JOURNAL_RECORD_BYTES,
                ));
            }
            repair_tail = true;
            tracing::warn!(
                journal = %path.display(),
                record = line_number,
                maximum_bytes = MAX_JOURNAL_RECORD_BYTES,
                "truncating oversized torn final vfs namespace journal record"
            );
            continue;
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            if !record.terminated {
                repair_tail = true;
            }
            continue;
        }
        match serde_json::from_slice::<VfsNamespaceMutation>(&line) {
            Ok(mutation) => {
                pending.push_back(mutation);
                if !record.terminated {
                    repair_tail = true;
                }
            }
            Err(error) if !record.terminated => {
                repair_tail = true;
                tracing::warn!(
                    journal = %path.display(),
                    error = %error,
                    "truncating torn final vfs namespace journal record"
                );
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "decode vfs namespace journal record {} in {}",
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
                "also failed to reopen vfs namespace journal after rewrite: {reopen_error}"
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
            Err(error).context("repair vfs namespace journal before append")
        }
    }
}

fn rewrite_pending(path: &Path, pending: &VecDeque<VfsNamespaceMutation>) -> Result<()> {
    let temporary = path.with_extension("jsonl.tmp");
    {
        let mut writer = BufWriter::new(
            File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?,
        );
        for mutation in pending {
            serde_json::to_writer(&mut writer, mutation).context("rewrite namespace journal")?;
            writer
                .write_all(b"\n")
                .context("rewrite namespace journal")?;
        }
        writer.flush().context("flush namespace journal")?;
        writer
            .get_ref()
            .sync_data()
            .context("sync namespace journal")?;
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
            Err(anyhow!("injected vfs namespace journal rewrite fault"))
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

fn append_json_line(
    file: &mut File,
    value: &impl serde::Serialize,
    context: &'static str,
) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn flush_waits_for_transient_failure_to_recover() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([VfsNamespaceMutation::CreateDirectory {
                    path: "src".to_string(),
                    mode: None,
                }]),
                journal: open_append(&journal_path).expect("journal"),
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
            }),
            changed: Condvar::new(),
            journal_path,
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
        let journal = NamespaceJournal {
            shared,
            worker: Mutex::new(None),
        };

        journal
            .flush()
            .expect("flush should survive transient error");
        recovery.join().expect("recovery thread");
    }

    #[test]
    fn namespace_resolution_drops_resolved_prefix_and_records_rejections() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let mkdir = VfsNamespaceMutation::CreateDirectory {
            path: "src".to_string(),
            mode: None,
        };
        let rejected = VfsNamespaceMutation::RemoveDirectory {
            path: "probe".to_string(),
        };
        let retained = VfsNamespaceMutation::CreateDirectory {
            path: "src/later".to_string(),
            mode: None,
        };
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([mkdir, rejected.clone(), retained.clone()]),
                journal: open_append(&journal_path).expect("journal"),
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: Some("vfs request failed: 409".to_string()),
                dead_letter_error: None,
            }),
            changed: Condvar::new(),
            journal_path: journal_path.clone(),
        };
        let resolution = NamespaceResolution {
            resolved: 2,
            dead_lettered: vec![(1, rejected, "vfs request failed: 409 Conflict".to_string())],
            transient_error: None,
        };

        let mut state = shared.state.lock().expect("state");
        apply_namespace_resolution(&shared, &mut state, resolution);

        assert_eq!(state.pending.len(), 1);
        assert_eq!(state.pending[0], retained);
        assert_eq!(state.last_error, None);
        assert!(
            state.dead_letter_error.is_some(),
            "a dead-letter must latch an error for the next flush waiter"
        );
        drop(state);

        let records = fs::read_to_string(journal_path.with_extension("dead-letter.jsonl"))
            .expect("dead letter records");
        assert!(records.contains("probe"));
        assert!(records.contains("409"));
        let journal_after = fs::read_to_string(&journal_path).expect("journal contents");
        assert!(journal_after.contains("src/later"));
        assert!(!journal_after.contains("probe"));
    }

    #[test]
    fn transient_namespace_failure_retains_failed_suffix_in_original_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let applied = VfsNamespaceMutation::CreateDirectory {
            path: "src".to_string(),
            mode: None,
        };
        let failed = VfsNamespaceMutation::Rename {
            from: "src/old.rs".to_string(),
            to: "src/new.rs".to_string(),
        };
        let later = VfsNamespaceMutation::DeleteFile {
            path: "src/stale.rs".to_string(),
            precondition: None,
        };
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([applied, failed.clone(), later.clone()]),
                journal: open_append(&journal_path).expect("journal"),
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
            }),
            changed: Condvar::new(),
            journal_path: journal_path.clone(),
        };

        let mut state = shared.state.lock().expect("state");
        apply_namespace_resolution(
            &shared,
            &mut state,
            NamespaceResolution {
                resolved: 1,
                dead_lettered: Vec::new(),
                transient_error: Some("gateway unavailable".to_string()),
            },
        );

        assert_eq!(
            state.pending,
            VecDeque::from([failed.clone(), later.clone()]),
            "the failed mutation and every later mutation remain ordered for reconnect"
        );
        assert_eq!(state.last_error.as_deref(), Some("gateway unavailable"));
        drop(state);
        assert_eq!(
            read_journal(&journal_path).expect("reopen journal"),
            VecDeque::from([failed, later]),
            "restart must replay the exact retained suffix"
        );
    }

    #[test]
    fn dead_letter_persistence_failure_keeps_rejected_mutation_pending() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let rejected = VfsNamespaceMutation::RemoveDirectory {
            path: "probe".to_string(),
        };
        let later = VfsNamespaceMutation::CreateDirectory {
            path: "src/later".to_string(),
            mode: None,
        };
        // A directory at the record-file path makes append fail
        // deterministically.
        fs::create_dir(journal_path.with_extension("dead-letter.jsonl"))
            .expect("dead-letter blocker");
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([rejected.clone(), later.clone()]),
                journal: open_append(&journal_path).expect("journal"),
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
            }),
            changed: Condvar::new(),
            journal_path: journal_path.clone(),
        };

        let mut state = shared.state.lock().expect("state");
        apply_namespace_resolution(
            &shared,
            &mut state,
            NamespaceResolution {
                resolved: 1,
                dead_lettered: vec![(
                    0,
                    rejected.clone(),
                    "vfs request failed: 409 Conflict".to_string(),
                )],
                transient_error: None,
            },
        );

        assert_eq!(
            state.pending,
            VecDeque::from([rejected.clone(), later.clone()]),
            "a terminal mutation cannot leave the journal until its dead letter is durable"
        );
        assert!(
            state.last_error.is_some(),
            "the namespace barrier stays loud"
        );
        assert_eq!(state.dead_letter_error, None);
        drop(state);
        assert_eq!(
            read_journal(&journal_path).expect("reopen journal"),
            VecDeque::from([rejected, later])
        );
    }

    #[test]
    fn flush_reports_journal_rewrite_failure_after_remote_completion() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::new(),
                journal: open_append(&journal_path).expect("journal"),
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: Some("rewrite failed".to_string()),
                dead_letter_error: None,
            }),
            changed: Condvar::new(),
            journal_path,
        });
        let journal = NamespaceJournal {
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
    fn large_namespace_wal_streams_across_small_reader_buffers() {
        const RECORDS: usize = 12_000;
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        {
            let mut writer = BufWriter::new(File::create(&journal_path).expect("journal"));
            for index in 0..RECORDS {
                serde_json::to_writer(
                    &mut writer,
                    &VfsNamespaceMutation::Rename {
                        from: format!("src/generated/{index:05}.old"),
                        to: format!("src/generated/{index:05}.new"),
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
        assert_eq!(pending.len(), RECORDS);
        assert_eq!(
            pending.front(),
            Some(&VfsNamespaceMutation::Rename {
                from: "src/generated/00000.old".to_string(),
                to: "src/generated/00000.new".to_string(),
            })
        );
        assert_eq!(
            pending.back(),
            Some(&VfsNamespaceMutation::Rename {
                from: "src/generated/11999.old".to_string(),
                to: "src/generated/11999.new".to_string(),
            })
        );
    }

    #[test]
    fn oversized_namespace_wal_record_is_bounded_and_classified_by_termination() {
        let path = Path::new("memory-namespace-journal.jsonl");
        let mut oversized_tail =
            BufReader::with_capacity(17, Cursor::new(vec![b'x'; MAX_JOURNAL_RECORD_BYTES + 4096]));
        let mut retained = Vec::new();
        let record = read_bounded_record(
            &mut oversized_tail,
            &mut retained,
            path,
            "read test namespace journal",
        )
        .expect("read")
        .expect("record");
        assert!(record.oversized);
        assert!(!record.terminated);
        assert_eq!(retained.len(), MAX_JOURNAL_RECORD_BYTES);

        let first = VfsNamespaceMutation::CreateDirectory {
            path: "first".to_string(),
            mode: None,
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
    fn reopen_truncates_only_a_torn_final_namespace_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let first = VfsNamespaceMutation::CreateDirectory {
            path: "first".to_string(),
            mode: None,
        };
        let mut bytes = serde_json::to_vec(&first).expect("serialize");
        bytes.extend_from_slice(b"\n{\"kind\":\"rename\",\"from\":\"torn");
        fs::write(&journal_path, bytes).expect("write torn journal");

        assert_eq!(
            read_journal(&journal_path).expect("torn tail is recoverable"),
            VecDeque::from([first]),
        );
        let repaired = fs::read(&journal_path).expect("repaired journal");
        assert!(repaired.ends_with(b"\n"));
        assert!(!String::from_utf8_lossy(&repaired).contains("torn"));
    }

    #[test]
    fn reopen_rejects_interior_namespace_journal_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let first = VfsNamespaceMutation::CreateDirectory {
            path: "first".to_string(),
            mode: None,
        };
        fs::write(
            &journal_path,
            format!(
                "{}\n{{broken}}\n",
                serde_json::to_string(&first).expect("serialize")
            ),
        )
        .expect("write corrupt journal");

        let error = read_journal(&journal_path).expect_err("interior corruption is fatal");
        assert!(error.to_string().contains("record 2"));
    }

    #[test]
    fn wal_rewrite_failure_restores_namespace_replay_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let mutation = VfsNamespaceMutation::Rename {
            from: "old".to_string(),
            to: "new".to_string(),
        };
        fs::write(
            &journal_path,
            format!("{}\n", serde_json::to_string(&mutation).expect("serialize")),
        )
        .expect("journal");
        fs::create_dir(journal_path.with_extension("jsonl.tmp")).expect("rewrite blocker");
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([mutation.clone()]),
                journal: open_append(&journal_path).expect("open journal"),
                force_flush: false,
                flushing: false,
                stop: false,
                journal_needs_repair: false,
                last_error: None,
                dead_letter_error: None,
            }),
            changed: Condvar::new(),
            journal_path: journal_path.clone(),
        };
        let mut state = shared.state.lock().expect("state");
        apply_namespace_resolution(
            &shared,
            &mut state,
            NamespaceResolution {
                resolved: 1,
                dead_lettered: Vec::new(),
                transient_error: None,
            },
        );

        assert_eq!(
            state.pending,
            VecDeque::from([mutation]),
            "in-memory replay state must match the still-old durable WAL"
        );
        assert!(state.last_error.is_some());
        drop(state);
        assert!(
            fs::read_to_string(&journal_path)
                .expect("old WAL survives")
                .contains("\"old\"")
        );
    }

    #[test]
    fn post_rename_rewrite_fault_must_repair_live_wal_before_later_enqueue() {
        for fault in [
            RewriteFault::ParentSyncAfterRename,
            RewriteFault::ReopenAfterRewrite,
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let journal_path = dir.path().join("namespace.jsonl");
            let first = VfsNamespaceMutation::CreateDirectory {
                path: "first".to_string(),
                mode: None,
            };
            fs::write(
                &journal_path,
                format!("{}\n", serde_json::to_string(&first).expect("serialize")),
            )
            .expect("journal");
            let shared = Arc::new(Shared {
                state: Mutex::new(JournalState {
                    pending: VecDeque::from([first.clone()]),
                    journal: open_append(&journal_path).expect("open journal"),
                    force_flush: false,
                    flushing: false,
                    stop: false,
                    journal_needs_repair: false,
                    last_error: None,
                    dead_letter_error: None,
                }),
                changed: Condvar::new(),
                journal_path: journal_path.clone(),
            });
            {
                let mut state = shared.state.lock().expect("state");
                arm_rewrite_fault(fault);
                let error = rewrite_journal(&journal_path, &mut state)
                    .expect_err("post-rename rewrite fault");
                state.last_error = Some(error.to_string());
                assert!(state.journal_needs_repair);
            }
            let journal = NamespaceJournal {
                shared: Arc::clone(&shared),
                worker: Mutex::new(None),
            };
            let later = VfsNamespaceMutation::CreateDirectory {
                path: "later".to_string(),
                mode: None,
            };

            arm_rewrite_fault(fault);
            journal
                .enqueue(later.clone())
                .expect_err("append cannot bypass a failed canonical repair");
            {
                let state = shared.state.lock().expect("state");
                assert!(state.journal_needs_repair);
                assert!(state.last_error.is_some(), "repair error stays latched");
                assert_eq!(state.pending, VecDeque::from([first.clone()]));
            }

            journal
                .enqueue(later.clone())
                .expect("next append repairs and reanchors the live WAL");
            assert_eq!(
                read_journal(&journal_path).expect("reopen live WAL"),
                VecDeque::from([first, later]),
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
}
