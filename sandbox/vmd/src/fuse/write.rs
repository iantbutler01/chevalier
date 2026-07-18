use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chevalier_sandbox::vfs::{VFS_SURFACE_KIND_VM_SHARED, VFS_SURFACE_KIND_VM_WORKSPACE};
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

#[derive(Clone, Debug, Deserialize, Serialize)]
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
    last_error: Option<String>,
}

struct Shared {
    state: Mutex<JournalState>,
    changed: Condvar,
    journal_path: PathBuf,
    staging_dir: PathBuf,
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
    ) -> Result<Self> {
        let staging_dir = journal_path.with_extension("writes");
        fs::create_dir_all(&staging_dir).with_context(|| {
            format!(
                "create vfs write staging directory {}",
                staging_dir.display()
            )
        })?;
        let pending = read_journal(journal_path)?;
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
                last_error: None,
            }),
            changed: Condvar::new(),
            journal_path: journal_path.to_path_buf(),
            staging_dir,
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
        let id = state.next_id;
        state.next_id = state.next_id.saturating_add(1);
        let staged_file = format!("{id}.bin");
        let staged_path = self.shared.staging_dir.join(staged_file.as_str());
        let temporary = staged_path.with_extension("tmp");
        {
            let mut file = File::create(&temporary)
                .with_context(|| format!("create staged vfs write {}", temporary.display()))?;
            file.write_all(bytes).context("stage vfs write bytes")?;
        }
        fs::rename(&temporary, &staged_path)
            .with_context(|| format!("install staged vfs write {}", staged_path.display()))?;
        let write = JournalWrite {
            id,
            path: path.to_string(),
            staged_file,
            size_bytes: bytes.len() as u64,
            base_content_hash,
        };
        serde_json::to_writer(&mut state.journal, &write).context("append vfs write journal")?;
        state
            .journal
            .write_all(b"\n")
            .context("append vfs write journal")?;
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
            Err(error) if rejected_request_status(error).is_some() => Some(
                resolve_rejected_batch(&shared, &client, &tokio, &coalesced, surface),
            ),
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
                for _ in 0..batch.len() {
                    if let Some(write) = state.pending.pop_front() {
                        let _ = fs::remove_file(shared.staging_dir.join(write.staged_file));
                    }
                }
                rebase_pending_after_commit(
                    &mut state.pending,
                    coalesced.as_slice(),
                    &committed_hashes,
                );
                if let Err(error) = rewrite_journal(&shared.journal_path, &mut state) {
                    state.last_error = Some(error.to_string());
                } else {
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
        let content_hash = content_hash_for_bytes(bytes.as_slice());
        let single = RemoteWrite {
            path: write.path.clone(),
            bytes,
            base_content_hash: write.base_content_hash.clone(),
        };
        match tokio.block_on(client.write_many(vec![single], surface)) {
            Ok(()) => {
                resolution.committed.insert(write.path.clone(), content_hash);
            }
            Err(error) if rejected_request_status(&error).is_some() => {
                resolution.dead_lettered.push((write.clone(), error.to_string()));
            }
            Err(_) => resolution.retained.push(write.path.clone()),
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
    let mut resolved_paths = HashSet::<&str>::new();
    resolved_paths.extend(resolution.committed.keys().map(String::as_str));
    for (write, error) in &resolution.dead_lettered {
        resolved_paths.insert(write.path.as_str());
        match dead_letter_write(shared, write, error) {
            Ok(record_path) => tracing::error!(
                journal = %shared.journal_path.display(),
                path = %write.path,
                staged_bytes = write.size_bytes,
                dead_letter = %record_path.display(),
                error = %error,
                "vfs write rejected by gateway; preserved in dead letter and dropped from journal"
            ),
            Err(record_error) => tracing::error!(
                journal = %shared.journal_path.display(),
                path = %write.path,
                error = %error,
                record_error = %record_error,
                "vfs write rejected by gateway; failed to record dead letter"
            ),
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
    for write in batch {
        if resolved_ids.contains(&write.id) {
            // Dead-lettered staged files were moved already; committed and
            // superseded ones are safe to drop.
            let _ = fs::remove_file(shared.staging_dir.join(write.staged_file.as_str()));
        }
    }
    let committed_batch = coalesced
        .iter()
        .filter(|write| resolution.committed.contains_key(write.path.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    rebase_pending_after_commit(&mut state.pending, &committed_batch, &resolution.committed);
    if let Err(error) = rewrite_journal(&shared.journal_path, state) {
        state.last_error = Some(error.to_string());
        return;
    }
    state.last_error = if resolution.retained.is_empty() {
        None
    } else {
        Some(format!(
            "transient vfs write failure for {} path(s), retrying: {}",
            resolution.retained.len(),
            resolution.retained.join(", "),
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
    let staged = shared.staging_dir.join(write.staged_file.as_str());
    let preserved = dead_letter_dir.join(write.staged_file.as_str());
    fs::rename(&staged, &preserved)
        .with_context(|| format!("preserve rejected vfs write {}", staged.display()))?;
    let record_path = dead_letter_dir.join("records.jsonl");
    let unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    let record = serde_json::json!({
        "id": write.id,
        "path": write.path,
        "preserved_file": write.staged_file,
        "size_bytes": write.size_bytes,
        "base_content_hash": write.base_content_hash,
        "error": error,
        "dead_lettered_at_unix": unix_seconds,
    });
    let mut file = open_append(&record_path)?;
    serde_json::to_writer(&mut file, &record).context("append vfs dead letter record")?;
    file.write_all(b"\n").context("append vfs dead letter record")?;
    Ok(record_path)
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
    let mut pending = VecDeque::new();
    for line in BufReader::new(file).lines() {
        let line = line.context("read vfs write journal")?;
        if !line.trim().is_empty() {
            pending.push_back(serde_json::from_str(&line).context("decode vfs write journal")?);
        }
    }
    Ok(pending)
}

fn rewrite_journal(path: &Path, state: &mut JournalState) -> Result<()> {
    let temporary = path.with_extension("jsonl.tmp");
    {
        let mut writer = BufWriter::new(File::create(&temporary)?);
        for write in &state.pending {
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
    state.journal = open_append(path)?;
    Ok(())
}

fn open_append(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
                last_error: None,
            }),
            changed: Condvar::new(),
            journal_path,
            staging_dir,
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
                last_error: Some("rewrite failed".to_string()),
            }),
            changed: Condvar::new(),
            journal_path,
            staging_dir,
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
            size_bytes: 8,
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
                last_error: Some("vfs request failed: 409".to_string()),
            }),
            changed: Condvar::new(),
            journal_path: journal_path.clone(),
            staging_dir: staging_dir.clone(),
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
            assert_eq!(state.last_error, None, "resolved batch must clear the error");
        }

        let dead_letter_dir = journal_path.with_extension("dead-letter");
        assert_eq!(
            fs::read(dead_letter_dir.join("2.bin")).expect("preserved bytes"),
            b"rejected bytes",
            "rejected bytes must be preserved, not lost"
        );
        let records = fs::read_to_string(dead_letter_dir.join("records.jsonl")).expect("records");
        assert!(records.contains("probe.txt"));
        assert!(records.contains("409"));
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
                last_error: None,
            }),
            changed: Condvar::new(),
            journal_path,
            staging_dir,
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
}
