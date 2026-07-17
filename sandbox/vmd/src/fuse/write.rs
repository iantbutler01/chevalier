use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chevalier_sandbox::vfs::{VFS_SURFACE_KIND_VM_SHARED, VFS_SURFACE_KIND_VM_WORKSPACE};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use super::client::{RemoteVfsClient, RemoteWrite};

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

        let writes = coalesce_batch(&batch)
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
        let result = writes.and_then(|writes| tokio.block_on(client.write_many(writes, surface)));
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        state.flushing = false;
        match result {
            Ok(()) => {
                for _ in 0..batch.len() {
                    if let Some(write) = state.pending.pop_front() {
                        let _ = fs::remove_file(shared.staging_dir.join(write.staged_file));
                    }
                }
                if let Err(error) = rewrite_journal(&shared.journal_path, &mut state) {
                    state.last_error = Some(error.to_string());
                } else {
                    state.last_error = None;
                }
                retry_delay = RETRY_DELAY_MIN;
            }
            Err(error) => state.last_error = Some(error.to_string()),
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

fn coalesce_batch(batch: &[JournalWrite]) -> Vec<JournalWrite> {
    let mut writes = Vec::<JournalWrite>::new();
    let mut positions = HashMap::<&str, usize>::new();
    for write in batch {
        if let Some(position) = positions.get(write.path.as_str()).copied() {
            let base_content_hash = writes[position].base_content_hash.clone();
            writes[position] = JournalWrite {
                base_content_hash,
                ..write.clone()
            };
        } else {
            positions.insert(write.path.as_str(), writes.len());
            writes.push(write.clone());
        }
    }
    writes
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
