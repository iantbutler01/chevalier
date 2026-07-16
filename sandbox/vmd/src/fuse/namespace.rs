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

use super::client::RemoteVfsClient;

const BATCH_DELAY: Duration = Duration::from_millis(8);
const RETRY_DELAY: Duration = Duration::from_millis(100);
const FLUSH_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BATCH_MUTATIONS: usize = 4096;

struct JournalState {
    pending: VecDeque<VfsNamespaceMutation>,
    journal: File,
    force_flush: bool,
    flushing: bool,
    stop: bool,
    last_error: Option<String>,
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
                last_error: None,
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
        serde_json::to_writer(&mut state.journal, &mutation)
            .context("append vfs namespace journal entry")?;
        state
            .journal
            .write_all(b"\n")
            .context("append vfs namespace journal delimiter")?;
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
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        state.flushing = false;
        match result {
            Ok(()) => {
                for _ in 0..batch.0.len() {
                    state.pending.pop_front();
                }
                if let Err(error) = rewrite_journal(&shared.journal_path, &mut state) {
                    state.last_error = Some(error.to_string());
                } else {
                    state.last_error = None;
                }
            }
            Err(error) => {
                state.last_error = Some(error.to_string());
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
    let mut pending = VecDeque::new();
    for line in BufReader::new(file).lines() {
        let line = line.context("read vfs namespace journal")?;
        if line.trim().is_empty() {
            continue;
        }
        pending.push_back(
            serde_json::from_str(line.as_str()).context("decode vfs namespace journal entry")?,
        );
    }
    Ok(pending)
}

fn rewrite_journal(path: &Path, state: &mut JournalState) -> Result<()> {
    let temporary = path.with_extension("jsonl.tmp");
    {
        let mut writer = BufWriter::new(
            File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?,
        );
        for mutation in &state.pending {
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
    state.journal = open_append(path)?;
    Ok(())
}

fn open_append(path: &Path) -> Result<File> {
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
    fn flush_waits_for_transient_failure_to_recover() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([VfsNamespaceMutation::CreateDirectory {
                    path: "src".to_string(),
                }]),
                journal: open_append(&journal_path).expect("journal"),
                force_flush: false,
                flushing: false,
                stop: false,
                last_error: None,
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
                last_error: Some("rewrite failed".to_string()),
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
}
