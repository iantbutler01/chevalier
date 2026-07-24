#[cfg(test)]
use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chevalier_sandbox::vfs::{
    VFS_SURFACE_KIND_VM_SHARED, VFS_SURFACE_KIND_VM_WORKSPACE, VfsDirEntry, VfsMetadata,
    VfsNamespaceMutation, VfsPublicationSnapshotEntry,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use uuid::Uuid;

use super::client::{RemoteVfsClient, rejected_request_status, request_status};
use super::write::{WriteDrainHandle, is_write_staging_temp_name};

/// Bounded self-healing budget for a RemoveDirectory/DeleteFile the gateway
/// rejected with a 409 conflict before it is dead-lettered: drain the write
/// journal and re-issue up to this many times, then (for a directory) reconcile
/// residue once.
const DELETION_RECOVERY_ATTEMPTS: u32 = 3;
/// Short pause between recovery re-issues so a straggler write has time to land
/// server-side before the retry re-reads directory state.
const DELETION_RECOVERY_BACKOFF: Duration = Duration::from_millis(50);
/// Upper bound on the "recently deleted by this mount" residue witness set.
/// Large enough to span a deep `rm -rf` in flight, small enough to stay cheap.
const RECENTLY_DELETED_CAPACITY: usize = 8192;

const BATCH_DELAY: Duration = Duration::from_millis(8);
const RETRY_DELAY: Duration = Duration::from_millis(100);
const FLUSH_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
/// Emit one WARN if the namespace flush barrier parks past this before the hard
/// timeout, surfacing an intermittent upstream stall without per-poll spam.
const SLOW_FLUSH_WARN_AFTER: Duration = Duration::from_secs(10);
const MAX_BATCH_MUTATIONS: usize = 4096;
const JOURNAL_READ_BUFFER_BYTES: usize = 64 * 1024;
/// Journal records contain metadata and bounded paths, never file payloads.
/// One MiB is far above the supported path envelope while keeping a corrupt
/// or unterminated JSONL record from forcing an unbounded allocation.
const MAX_JOURNAL_RECORD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct NamespaceJournalRecord {
    operation_id: String,
    mutation: VfsNamespaceMutation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    projected_metadata: Option<VfsMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    committed_revision: Option<u64>,
}

struct JournalState {
    pending: VecDeque<NamespaceJournalRecord>,
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

type CommitHook =
    Box<dyn Fn(u64, &[VfsNamespaceMutation], &[VfsPublicationSnapshotEntry]) + Send + Sync>;

struct Shared {
    state: Mutex<JournalState>,
    changed: Condvar,
    journal_path: PathBuf,
}

pub struct NamespaceJournal {
    shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

/// Result of merging one authoritative namespace read with this mount's
/// journal. `applied` is computed under the same journal lock as `value`; a
/// caller may publish the result to a cache shared by sibling mounts only when
/// it is false.
pub struct NamespaceProjection<T> {
    pub value: T,
    pub applied: bool,
}

impl NamespaceJournal {
    pub fn open(
        client: RemoteVfsClient,
        scope_path: &str,
        journal_path: &Path,
        tokio: Handle,
    ) -> Result<Self> {
        Self::open_with_commit_hook(client, scope_path, journal_path, tokio, None, None)
    }

    pub fn open_with_commit_hook(
        client: RemoteVfsClient,
        scope_path: &str,
        journal_path: &Path,
        tokio: Handle,
        on_commit: Option<CommitHook>,
        write_drain: Option<WriteDrainHandle>,
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
            .spawn(move || {
                run_worker(
                    worker_shared,
                    client,
                    scope_path,
                    tokio,
                    on_commit,
                    write_drain,
                )
            })
            .context("spawn vfs namespace journal worker")?;
        shared.changed.notify_all();
        Ok(Self {
            shared,
            worker: Mutex::new(Some(worker)),
        })
    }

    pub fn enqueue(&self, mutation: VfsNamespaceMutation) -> Result<()> {
        self.enqueue_with_metadata(mutation, None)
    }

    pub fn enqueue_with_metadata(
        &self,
        mutation: VfsNamespaceMutation,
        projected_metadata: Option<VfsMetadata>,
    ) -> Result<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs namespace journal lock poisoned"))?;
        repair_before_append(&self.shared.journal_path, &mut state)?;
        let record = NamespaceJournalRecord {
            operation_id: Uuid::new_v4().to_string(),
            mutation,
            projected_metadata,
            committed_revision: None,
        };
        append_json_line(
            &mut state.journal,
            &record,
            "append vfs namespace journal entry",
        )?;
        state.pending.push_back(record);
        state.last_error = None;
        if state
            .pending
            .iter()
            .filter(|record| record.committed_revision.is_none())
            .count()
            >= MAX_BATCH_MUTATIONS
        {
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
        let start = Instant::now();
        let deadline = start + FLUSH_RETRY_TIMEOUT;
        let mut slow_warned = false;
        while state
            .pending
            .iter()
            .any(|record| record.committed_revision.is_none())
            || state.flushing
        {
            let now = Instant::now();
            if now >= deadline {
                return Err(anyhow!(state.last_error.clone().unwrap_or_else(|| {
                    "timed out flushing vfs namespace journal".to_string()
                })));
            }
            let elapsed = now.duration_since(start);
            if !slow_warned && elapsed >= SLOW_FLUSH_WARN_AFTER {
                slow_warned = true;
                let uncommitted = state
                    .pending
                    .iter()
                    .filter(|record| record.committed_revision.is_none())
                    .count();
                tracing::warn!(
                    waited_secs = elapsed.as_secs(),
                    uncommitted,
                    flushing = state.flushing,
                    "vfs namespace journal flush still draining pending mutations"
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

    /// Take the latched terminal error without waiting for the journal to
    /// drain.
    ///
    /// Reads serve this mount's queued mutations from the projection instead of
    /// waiting on a publication, so they never call `flush`. They still must
    /// not report a projected entry whose publication has already been
    /// dead-lettered, which is what this surfaces: the same terminal error
    /// `flush` would return, consumed once so a single failure is reported to
    /// one caller rather than latched against every later read.
    pub fn take_terminal_error(&self) -> Result<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs namespace journal lock poisoned"))?;
        if let Some(error) = state.dead_letter_error.take() {
            return Err(anyhow!(error));
        }
        Ok(())
    }

    /// Whether an in-flight namespace mutation can change the lookup result for
    /// `path`. A mutation of an ancestor can retarget or remove the path. When
    /// `include_direct_children` is true, creation/removal/rename of an
    /// immediate child also affects the directory listing and link metadata.
    pub fn has_pending_for_path(&self, path: &str, include_direct_children: bool) -> Result<bool> {
        let state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs namespace journal lock poisoned"))?;
        Ok(state.pending.iter().any(|record| {
            record.committed_revision.is_none()
                && record.mutation.paths().into_iter().any(|mutation_path| {
                    !mutation_path.is_empty()
                        && namespace_mutation_affects_path(
                            mutation_path,
                            path,
                            include_direct_children,
                        )
                })
        }))
    }

    pub fn has_projection_for_path(
        &self,
        path: &str,
        include_direct_children: bool,
    ) -> Result<bool> {
        let state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs namespace journal lock poisoned"))?;
        Ok(state.pending.iter().any(|record| {
            record.mutation.paths().into_iter().any(|mutation_path| {
                !mutation_path.is_empty()
                    && namespace_mutation_affects_path(mutation_path, path, include_direct_children)
            })
        }))
    }

    pub fn project_metadata(
        &self,
        path: &str,
        metadata: Option<VfsMetadata>,
        observed_revision: u64,
    ) -> Result<NamespaceProjection<Option<VfsMetadata>>> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs namespace journal lock poisoned"))?;
        observe_revision(&self.shared, &mut state, observed_revision)?;
        let path = path.trim_matches('/');
        let applied = state.pending.iter().any(|record| {
            record.mutation.paths().into_iter().any(|mutation_path| {
                !mutation_path.is_empty()
                    && namespace_mutation_affects_path(mutation_path, path, false)
            })
        });
        let mut projected = metadata;
        for record in &state.pending {
            match &record.mutation {
                VfsNamespaceMutation::CreateFile {
                    path: created,
                    mode,
                } if created == path => {
                    projected = Some(
                        record
                            .projected_metadata
                            .clone()
                            .unwrap_or_else(|| projected_file(*mode)),
                    );
                }
                VfsNamespaceMutation::CreateDirectory {
                    path: created,
                    mode,
                } if created == path => {
                    projected = Some(
                        record
                            .projected_metadata
                            .clone()
                            .unwrap_or_else(|| projected_directory(*mode)),
                    );
                }
                VfsNamespaceMutation::CreateSymlink {
                    path: created,
                    target,
                } if created == path => {
                    projected = Some(
                        record
                            .projected_metadata
                            .clone()
                            .unwrap_or_else(|| projected_symlink(target)),
                    );
                }
                VfsNamespaceMutation::CreateHardLink {
                    source_path,
                    destination_path,
                } if source_path == path || destination_path == path => {
                    projected = record.projected_metadata.clone().or(projected);
                }
                VfsNamespaceMutation::DeleteFile { path: deleted, .. } if deleted == path => {
                    projected = None;
                }
                VfsNamespaceMutation::RemoveDirectory { path: deleted }
                    if path_suffix(path, deleted).is_some() =>
                {
                    projected = None;
                }
                VfsNamespaceMutation::Rename { from, to } => {
                    if path_suffix(path, from).is_some() {
                        projected = None;
                    } else if path_suffix(path, to).is_some() {
                        projected = record.projected_metadata.clone().or(projected);
                    }
                }
                VfsNamespaceMutation::SetMode {
                    path: changed,
                    mode,
                } if changed == path => {
                    if let Some(metadata) = projected.as_mut() {
                        metadata.mode = Some(*mode & 0o7777);
                        metadata.executable = *mode & 0o111 != 0;
                    }
                }
                _ => {}
            }
        }
        Ok(NamespaceProjection {
            value: projected,
            applied,
        })
    }

    pub fn project_directory(
        &self,
        path: &str,
        entries: Vec<VfsDirEntry>,
        observed_revision: u64,
    ) -> Result<NamespaceProjection<Vec<VfsDirEntry>>> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs namespace journal lock poisoned"))?;
        observe_revision(&self.shared, &mut state, observed_revision)?;
        let path = path.trim_matches('/');
        let applied = state.pending.iter().any(|record| {
            record.mutation.paths().into_iter().any(|mutation_path| {
                !mutation_path.is_empty()
                    && namespace_mutation_affects_path(mutation_path, path, true)
            })
        });
        let mut projected = entries
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect::<std::collections::BTreeMap<_, _>>();
        for record in &state.pending {
            match &record.mutation {
                VfsNamespaceMutation::CreateFile {
                    path: created,
                    mode,
                } => {
                    project_positive_child(
                        &mut projected,
                        path,
                        created,
                        record
                            .projected_metadata
                            .clone()
                            .unwrap_or_else(|| projected_file(*mode)),
                    );
                }
                VfsNamespaceMutation::CreateDirectory {
                    path: created,
                    mode,
                } => {
                    project_positive_child(
                        &mut projected,
                        path,
                        created,
                        record
                            .projected_metadata
                            .clone()
                            .unwrap_or_else(|| projected_directory(*mode)),
                    );
                }
                VfsNamespaceMutation::CreateSymlink {
                    path: created,
                    target,
                } => {
                    project_positive_child(
                        &mut projected,
                        path,
                        created,
                        record
                            .projected_metadata
                            .clone()
                            .unwrap_or_else(|| projected_symlink(target)),
                    );
                }
                VfsNamespaceMutation::CreateHardLink {
                    source_path,
                    destination_path,
                } => {
                    if let Some(metadata) = record.projected_metadata.clone() {
                        if let Some(name) = direct_child_name(path, source_path)
                            && let Some(entry) = projected.get_mut(name.as_str())
                        {
                            *entry = dir_entry_from_metadata(name, metadata.clone());
                        }
                        project_positive_child(&mut projected, path, destination_path, metadata);
                    }
                }
                VfsNamespaceMutation::DeleteFile { path: deleted, .. }
                | VfsNamespaceMutation::RemoveDirectory { path: deleted } => {
                    remove_direct_child(&mut projected, path, deleted);
                }
                VfsNamespaceMutation::Rename { from, to } => {
                    let source = direct_child_name(path, from)
                        .and_then(|name| projected.remove(name.as_str()));
                    if let Some(name) = direct_child_name(path, to) {
                        let metadata = record
                            .projected_metadata
                            .clone()
                            .or_else(|| source.as_ref().map(metadata_from_dir_entry));
                        if let Some(metadata) = metadata {
                            projected.insert(name.clone(), dir_entry_from_metadata(name, metadata));
                        }
                    }
                }
                VfsNamespaceMutation::SetMode {
                    path: changed,
                    mode,
                } => {
                    if let Some(name) = direct_child_name(path, changed)
                        && let Some(entry) = projected.get_mut(name.as_str())
                    {
                        entry.mode = Some(*mode & 0o7777);
                        entry.executable = *mode & 0o111 != 0;
                    }
                }
            }
        }
        Ok(NamespaceProjection {
            value: projected.into_values().collect(),
            applied,
        })
    }

    pub fn observe_server_revision(&self, revision: u64) -> Result<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("vfs namespace journal lock poisoned"))?;
        observe_revision(&self.shared, &mut state, revision)
    }
}

fn observe_revision(shared: &Shared, state: &mut JournalState, revision: u64) -> Result<()> {
    if revision == 0 {
        return Ok(());
    }
    let before = state.pending.clone();
    state.pending.retain(|record| {
        record
            .committed_revision
            .is_none_or(|committed| committed > revision)
    });
    if state.pending.len() != before.len()
        && let Err(error) = rewrite_journal(&shared.journal_path, state)
    {
        state.pending = before;
        return Err(error);
    }
    Ok(())
}

fn path_suffix<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    let path = path.trim_matches('/');
    let prefix = prefix.trim_matches('/');
    if path == prefix {
        Some("")
    } else {
        path.strip_prefix(prefix)
            .filter(|suffix| suffix.starts_with('/'))
    }
}

fn direct_child_name(parent: &str, child: &str) -> Option<String> {
    let parent = parent.trim_matches('/');
    let child = child.trim_matches('/');
    let relative = if parent.is_empty() {
        child
    } else {
        child.strip_prefix(&format!("{parent}/"))?
    };
    (!relative.is_empty() && !relative.contains('/')).then(|| relative.to_string())
}

fn projected_file(mode: Option<u32>) -> VfsMetadata {
    VfsMetadata {
        kind: "file".to_string(),
        size_bytes: 0,
        file_id: None,
        link_count: 1,
        link_target: None,
        content_hash: None,
        executable: mode.is_some_and(|mode| mode & 0o111 != 0),
        mode,
        updated_at: None,
    }
}

fn projected_directory(mode: Option<u32>) -> VfsMetadata {
    VfsMetadata {
        kind: "directory".to_string(),
        size_bytes: 0,
        file_id: None,
        link_count: 2,
        link_target: None,
        content_hash: None,
        executable: mode.unwrap_or(0o755) & 0o111 != 0,
        mode: Some(mode.unwrap_or(0o755)),
        updated_at: None,
    }
}

fn projected_symlink(target: &str) -> VfsMetadata {
    VfsMetadata {
        kind: "symlink".to_string(),
        size_bytes: target.len() as u64,
        file_id: None,
        link_count: 1,
        link_target: Some(target.to_string()),
        content_hash: None,
        executable: false,
        mode: Some(0o777),
        updated_at: None,
    }
}

fn project_positive_child(
    entries: &mut std::collections::BTreeMap<String, VfsDirEntry>,
    parent: &str,
    child: &str,
    metadata: VfsMetadata,
) {
    if let Some(name) = direct_child_name(parent, child) {
        entries.insert(name.clone(), dir_entry_from_metadata(name, metadata));
    }
}

fn remove_direct_child(
    entries: &mut std::collections::BTreeMap<String, VfsDirEntry>,
    parent: &str,
    child: &str,
) {
    if let Some(name) = direct_child_name(parent, child) {
        entries.remove(name.as_str());
    }
}

fn dir_entry_from_metadata(name: String, metadata: VfsMetadata) -> VfsDirEntry {
    VfsDirEntry {
        name,
        kind: metadata.kind,
        size_bytes: metadata.size_bytes,
        file_id: metadata.file_id,
        link_count: metadata.link_count,
        link_target: metadata.link_target,
        content_hash: metadata.content_hash,
        executable: metadata.executable,
        mode: metadata.mode,
        updated_at: metadata.updated_at,
    }
}

fn metadata_from_dir_entry(entry: &VfsDirEntry) -> VfsMetadata {
    VfsMetadata {
        kind: entry.kind.clone(),
        size_bytes: entry.size_bytes,
        file_id: entry.file_id.clone(),
        link_count: entry.link_count,
        link_target: entry.link_target.clone(),
        content_hash: entry.content_hash.clone(),
        executable: entry.executable,
        mode: entry.mode,
        updated_at: entry.updated_at,
    }
}

fn namespace_mutation_affects_path(
    mutation_path: &str,
    observed_path: &str,
    include_direct_children: bool,
) -> bool {
    let mutation_path = mutation_path.trim_matches('/');
    let observed_path = observed_path.trim_matches('/');
    if mutation_path == observed_path {
        return true;
    }
    if observed_path
        .strip_prefix(mutation_path)
        .is_some_and(|suffix| suffix.starts_with('/'))
    {
        return true;
    }
    include_direct_children
        && mutation_path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or_default()
            == observed_path
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

fn run_worker(
    shared: Arc<Shared>,
    client: RemoteVfsClient,
    scope_path: String,
    tokio: Handle,
    on_commit: Option<CommitHook>,
    write_drain: Option<WriteDrainHandle>,
) {
    // Witnesses which paths THIS mount has deleted, so a residue reconcile can
    // tell an entry the guest already removed (safe to re-delete) from a live
    // sibling-mount child (must never be deleted). Worker-local: single-threaded
    // and lock-free, spanning batches for the life of the mount.
    let mut recently_deleted = RecentlyDeleted::new(RECENTLY_DELETED_CAPACITY);
    loop {
        let batch = {
            let mut state = match shared.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            while !state
                .pending
                .iter()
                .any(|record| record.committed_revision.is_none())
                && !state.stop
            {
                state = match shared.changed.wait(state) {
                    Ok(state) => state,
                    Err(_) => return,
                };
            }
            if state.stop
                && !state
                    .pending
                    .iter()
                    .any(|record| record.committed_revision.is_none())
            {
                return;
            }
            let uncommitted_count = state
                .pending
                .iter()
                .filter(|record| record.committed_revision.is_none())
                .count();
            if !state.force_flush && uncommitted_count < MAX_BATCH_MUTATIONS {
                let deadline = Instant::now() + BATCH_DELAY;
                while !state.force_flush
                    && state
                        .pending
                        .iter()
                        .filter(|record| record.committed_revision.is_none())
                        .count()
                        < MAX_BATCH_MUTATIONS
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
                .iter()
                .find(|record| record.committed_revision.is_none())
                .map(|record| mutation_surface(&scope_path, &record.mutation))
                .unwrap_or(VFS_SURFACE_KIND_VM_WORKSPACE);
            let batch = state
                .pending
                .iter()
                .filter(|record| record.committed_revision.is_none())
                .take(MAX_BATCH_MUTATIONS)
                .take_while(|record| mutation_surface(&scope_path, &record.mutation) == surface)
                .cloned()
                .collect::<Vec<_>>();
            state.force_flush = false;
            state.flushing = true;
            (batch, surface)
        };

        let operation_ids = batch
            .0
            .iter()
            .map(|record| record.operation_id.clone())
            .collect::<Vec<_>>();
        let mutations = batch
            .0
            .iter()
            .map(|record| record.mutation.clone())
            .collect::<Vec<_>>();
        let result = tokio.block_on(client.apply_namespace_batch(
            operation_ids.as_slice(),
            mutations.as_slice(),
            batch.1,
        ));
        // A 4xx rejection can never succeed by retrying the same batch. Replay
        // the batch one mutation at a time, in order, so a single rejected
        // mutation is recorded and dropped instead of wedging the journal.
        let resolution = match &result {
            Err(error) if rejected_request_status(error).is_some() => {
                Some(resolve_rejected_namespace_batch(
                    &client,
                    &tokio,
                    batch.0.as_slice(),
                    batch.1,
                    on_commit.as_deref(),
                    &mut NamespaceRecovery {
                        drain_writes: write_drain.as_ref(),
                        recently_deleted: &mut recently_deleted,
                    },
                ))
            }
            _ => None,
        };
        if let Ok(publication) = result.as_ref() {
            // The whole batch applied cleanly; witness every mutation so a later
            // conflicted RemoveDirectory can recognize a resurrected child as
            // this mount's own residue — and so a delete-then-recreate clears
            // the witness and keeps the live recreation safe.
            for mutation in &mutations {
                witness_namespace_mutation(&mut recently_deleted, mutation);
            }
            if let Some(on_commit) = on_commit.as_deref() {
                on_commit(
                    publication.revision,
                    mutations.as_slice(),
                    publication.entries.as_slice(),
                );
            }
        }
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
            Ok(publication) => {
                let recovered = state.last_error.take();
                let pending_before = state.pending.clone();
                let committed_ids = batch
                    .0
                    .iter()
                    .map(|record| record.operation_id.as_str())
                    .collect::<std::collections::HashSet<_>>();
                if publication_snapshot_observes_mutations(
                    mutations.as_slice(),
                    publication.entries.as_slice(),
                ) {
                    // This is not retirement on a mutation acknowledgement:
                    // the response carries the authoritative post-mutation
                    // metadata snapshot under the same server revision. The
                    // commit hook installed that snapshot before we reached
                    // this point, so both the projection and cache have now
                    // observed the committed revision.
                    state
                        .pending
                        .retain(|record| !committed_ids.contains(record.operation_id.as_str()));
                } else {
                    // Rolling-upgrade and partial responses remain fail
                    // closed. Keep projecting until an ordinary read observes
                    // the committed revision.
                    for record in &mut state.pending {
                        if committed_ids.contains(record.operation_id.as_str()) {
                            record.committed_revision = Some(publication.revision);
                        }
                    }
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

fn publication_snapshot_observes_mutations(
    mutations: &[VfsNamespaceMutation],
    entries: &[VfsPublicationSnapshotEntry],
) -> bool {
    if mutations.is_empty() {
        return true;
    }
    let mut expected_presence = HashMap::<&str, bool>::new();
    for mutation in mutations {
        match mutation {
            VfsNamespaceMutation::CreateFile { path, .. }
            | VfsNamespaceMutation::CreateDirectory { path, .. }
            | VfsNamespaceMutation::CreateSymlink { path, .. }
            | VfsNamespaceMutation::SetMode { path, .. } => {
                expected_presence.insert(path.as_str(), true);
            }
            VfsNamespaceMutation::CreateHardLink {
                source_path,
                destination_path,
            } => {
                expected_presence.insert(source_path.as_str(), true);
                expected_presence.insert(destination_path.as_str(), true);
            }
            VfsNamespaceMutation::DeleteFile { path, .. }
            | VfsNamespaceMutation::RemoveDirectory { path } => {
                expected_presence.insert(path.as_str(), false);
            }
            VfsNamespaceMutation::Rename { from, to } => {
                if from == to {
                    expected_presence.insert(from.as_str(), true);
                } else {
                    expected_presence.insert(from.as_str(), false);
                    expected_presence.insert(to.as_str(), true);
                }
            }
        }
    }
    expected_presence.into_iter().all(|(path, expected)| {
        entries
            .iter()
            .find(|entry| entry.path.trim_matches('/') == path.trim_matches('/'))
            .is_some_and(|entry| entry.metadata.is_some() == expected)
    })
}

struct NamespaceResolution {
    committed: Vec<(String, u64)>,
    /// Mutations the gateway rejected with a 4xx.
    dead_lettered: Vec<(NamespaceJournalRecord, String)>,
    /// First transient failure; this and later mutations stay pending.
    transient_error: Option<String>,
}

/// Trait-object shape for the batch commit hook, shared by every single-op
/// apply path so their signatures stay legible.
type NamespaceCommitHook<'a> =
    &'a (dyn Fn(u64, &[VfsNamespaceMutation], &[VfsPublicationSnapshotEntry]) + Send + Sync);

/// Cross-journal context the rejected-batch resolver uses to self-heal a
/// conflicted deletion instead of dead-lettering it on the first replay.
struct NamespaceRecovery<'a> {
    /// Drains in-flight content writes so a straggler cannot resurrect a child
    /// after we re-issue the deletion. `None` on read-only / journal-less
    /// mounts, where no content-write channel exists to contend with.
    drain_writes: Option<&'a WriteDrainHandle>,
    /// Paths this mount has deleted (minus any it has since recreated): the
    /// residue witness that distinguishes a resurrected child from a live
    /// sibling-mount entry.
    recently_deleted: &'a mut RecentlyDeleted,
}

/// Outcome of self-healing one conflicted deletion.
enum RecoveryOutcome {
    /// Accepted at this server revision.
    Committed(u64),
    /// Recovery exhausted; record and drop as today.
    DeadLetter(String),
    /// A transient failure; retain and let the ordinary retry loop revisit it.
    Transient(String),
}

/// Outcome of one single-mutation apply, classified for recovery control flow.
enum SingleOutcome {
    Committed(u64),
    /// A 409 conflict — recoverable (retry after draining / reconciling).
    Conflict(String),
    /// Another terminal 4xx that can never succeed by retrying.
    Rejected(String),
    Transient(String),
}

fn is_recoverable_deletion(mutation: &VfsNamespaceMutation) -> bool {
    matches!(
        mutation,
        VfsNamespaceMutation::RemoveDirectory { .. } | VfsNamespaceMutation::DeleteFile { .. }
    )
}

fn resolve_rejected_namespace_batch(
    client: &RemoteVfsClient,
    tokio: &Handle,
    batch: &[NamespaceJournalRecord],
    surface: &'static str,
    on_commit: Option<NamespaceCommitHook<'_>>,
    recovery: &mut NamespaceRecovery<'_>,
) -> NamespaceResolution {
    let mut resolution = NamespaceResolution {
        committed: Vec::new(),
        dead_lettered: Vec::new(),
        transient_error: None,
    };
    for record in batch {
        match apply_single_mutation(
            client,
            tokio,
            &record.operation_id,
            &record.mutation,
            surface,
            on_commit,
        ) {
            SingleOutcome::Committed(revision) => {
                witness_namespace_mutation(recovery.recently_deleted, &record.mutation);
                resolution
                    .committed
                    .push((record.operation_id.clone(), revision));
            }
            // A directory/file deletion the gateway rejected as conflicting
            // (typically not-empty from a write that raced the delete). Do not
            // dead-letter on the first replay: drain, re-issue, and reconcile.
            SingleOutcome::Conflict(_) if is_recoverable_deletion(&record.mutation) => {
                match recover_conflicted_deletion(
                    client, tokio, record, surface, on_commit, recovery,
                ) {
                    RecoveryOutcome::Committed(revision) => {
                        resolution
                            .committed
                            .push((record.operation_id.clone(), revision));
                    }
                    RecoveryOutcome::DeadLetter(error) => {
                        resolution.dead_lettered.push((record.clone(), error));
                    }
                    RecoveryOutcome::Transient(error) => {
                        resolution.transient_error = Some(error);
                        break;
                    }
                }
            }
            SingleOutcome::Conflict(error) | SingleOutcome::Rejected(error) => {
                resolution.dead_lettered.push((record.clone(), error));
            }
            SingleOutcome::Transient(error) => {
                resolution.transient_error = Some(error);
                break;
            }
        }
    }
    resolution
}

/// Apply exactly one namespace mutation, firing the commit hook on success and
/// classifying any failure for recovery control flow.
fn apply_single_mutation(
    client: &RemoteVfsClient,
    tokio: &Handle,
    operation_id: &str,
    mutation: &VfsNamespaceMutation,
    surface: &'static str,
    on_commit: Option<NamespaceCommitHook<'_>>,
) -> SingleOutcome {
    match tokio.block_on(client.apply_namespace_batch(
        std::slice::from_ref(&operation_id.to_string()),
        std::slice::from_ref(mutation),
        surface,
    )) {
        Ok(publication) => {
            if let Some(on_commit) = on_commit {
                on_commit(
                    publication.revision,
                    std::slice::from_ref(mutation),
                    publication.entries.as_slice(),
                );
            }
            SingleOutcome::Committed(publication.revision)
        }
        Err(error) if request_status(&error) == Some(StatusCode::CONFLICT) => {
            SingleOutcome::Conflict(error.to_string())
        }
        Err(error) if rejected_request_status(&error).is_some() => {
            SingleOutcome::Rejected(error.to_string())
        }
        Err(error) => SingleOutcome::Transient(error.to_string()),
    }
}

/// Bounded self-healing for a deletion the gateway rejected with a 409: drain
/// pending writes and re-issue up to `DELETION_RECOVERY_ATTEMPTS` times, then
/// (for a directory) reconcile residue children once before dead-lettering.
/// Logs at warn while recovering; the terminal error is only logged when the
/// caller finally dead-letters.
fn recover_conflicted_deletion(
    client: &RemoteVfsClient,
    tokio: &Handle,
    record: &NamespaceJournalRecord,
    surface: &'static str,
    on_commit: Option<NamespaceCommitHook<'_>>,
    recovery: &mut NamespaceRecovery<'_>,
) -> RecoveryOutcome {
    for attempt in 1..=DELETION_RECOVERY_ATTEMPTS {
        // (a) Drain the content-write journal so any straggler write to the
        // target subtree lands before we re-read and re-issue the deletion.
        if let Some(drain) = recovery.drain_writes
            && let Err(error) = drain.flush()
        {
            tracing::warn!(
                mutation = ?record.mutation,
                error = %error,
                "vfs namespace deletion recovery could not drain the write journal; retaining"
            );
            return RecoveryOutcome::Transient(error.to_string());
        }
        if attempt > 1 {
            std::thread::sleep(DELETION_RECOVERY_BACKOFF);
        }
        // (b) Re-issue the deletion now that writes are drained.
        match apply_single_mutation(
            client,
            tokio,
            &record.operation_id,
            &record.mutation,
            surface,
            on_commit,
        ) {
            SingleOutcome::Committed(revision) => {
                witness_namespace_mutation(recovery.recently_deleted, &record.mutation);
                tracing::warn!(
                    mutation = ?record.mutation,
                    attempt,
                    "vfs namespace deletion recovered after draining pending writes"
                );
                return RecoveryOutcome::Committed(revision);
            }
            SingleOutcome::Conflict(error) => {
                tracing::warn!(
                    mutation = ?record.mutation,
                    attempt,
                    error = %error,
                    "vfs namespace deletion still conflicting; retrying after write drain"
                );
                continue;
            }
            SingleOutcome::Rejected(error) => return RecoveryOutcome::DeadLetter(error),
            SingleOutcome::Transient(error) => return RecoveryOutcome::Transient(error),
        }
    }
    // (c) Reconcile residue — directories only; a file has no children to sweep.
    match &record.mutation {
        VfsNamespaceMutation::RemoveDirectory { path } => {
            reconcile_remove_directory(client, tokio, record, path, surface, on_commit, recovery)
        }
        _ => RecoveryOutcome::DeadLetter(format!(
            "vfs deletion {:?} still conflicts after {DELETION_RECOVERY_ATTEMPTS} recovery attempts",
            record.mutation,
        )),
    }
}

/// Reconcile a still-not-empty directory: delete residue children ahead of a
/// final RemoveDirectory retry. Residue is a staging temporary or a child this
/// mount already deleted; anything else is treated as a live entry and the
/// directory is dead-lettered rather than risk deleting real sibling-mount data.
fn reconcile_remove_directory(
    client: &RemoteVfsClient,
    tokio: &Handle,
    record: &NamespaceJournalRecord,
    path: &str,
    surface: &'static str,
    on_commit: Option<NamespaceCommitHook<'_>>,
    recovery: &mut NamespaceRecovery<'_>,
) -> RecoveryOutcome {
    let listing = match tokio.block_on(client.list_dir(path)) {
        Ok(listing) => listing,
        Err(error) => {
            tracing::warn!(
                path,
                error = %error,
                "vfs RemoveDirectory reconcile could not list the target directory; retaining"
            );
            return RecoveryOutcome::Transient(error.to_string());
        }
    };
    let Some(entries) = listing else {
        // Already gone server-side; a final idempotent re-issue retires it.
        return finalize_remove_directory(client, tokio, record, surface, on_commit, recovery);
    };

    let mut live_child_remains = false;
    for entry in entries {
        let child_path = namespace_join(path, &entry.name);
        let is_residue = is_write_staging_temp_name(&entry.name)
            || recovery.recently_deleted.contains(&child_path);
        if !is_residue {
            // Never deleted by this mount and not a staging temporary: a live
            // entry (e.g. a sibling mount created it). The correct outcome is a
            // failed rmdir, exactly as before this recovery existed.
            tracing::warn!(
                child = %child_path,
                "vfs RemoveDirectory reconcile found a live child; leaving it and failing the rmdir"
            );
            live_child_remains = true;
            continue;
        }
        let delete = VfsNamespaceMutation::DeleteFile {
            path: child_path.clone(),
            precondition: None,
        };
        match apply_single_mutation(
            client,
            tokio,
            &Uuid::new_v4().to_string(),
            &delete,
            surface,
            on_commit,
        ) {
            SingleOutcome::Committed(_) => {
                recovery.recently_deleted.record(&child_path);
                tracing::warn!(child = %child_path, "vfs RemoveDirectory reconcile deleted residue child");
            }
            SingleOutcome::Conflict(_) | SingleOutcome::Rejected(_) => {
                // The child re-materialized or refused deletion; the rmdir
                // cannot succeed on this pass.
                live_child_remains = true;
            }
            SingleOutcome::Transient(error) => return RecoveryOutcome::Transient(error),
        }
    }

    if live_child_remains {
        return RecoveryOutcome::DeadLetter(format!(
            "vfs RemoveDirectory {path:?} retains a child that is not this mount's residue"
        ));
    }
    finalize_remove_directory(client, tokio, record, surface, on_commit, recovery)
}

/// Final RemoveDirectory re-issue after residue has been swept.
fn finalize_remove_directory(
    client: &RemoteVfsClient,
    tokio: &Handle,
    record: &NamespaceJournalRecord,
    surface: &'static str,
    on_commit: Option<NamespaceCommitHook<'_>>,
    recovery: &mut NamespaceRecovery<'_>,
) -> RecoveryOutcome {
    match apply_single_mutation(
        client,
        tokio,
        &record.operation_id,
        &record.mutation,
        surface,
        on_commit,
    ) {
        SingleOutcome::Committed(revision) => {
            witness_namespace_mutation(recovery.recently_deleted, &record.mutation);
            tracing::warn!(
                mutation = ?record.mutation,
                "vfs RemoveDirectory recovered after sweeping residue children"
            );
            RecoveryOutcome::Committed(revision)
        }
        SingleOutcome::Conflict(error) | SingleOutcome::Rejected(error) => {
            RecoveryOutcome::DeadLetter(error)
        }
        SingleOutcome::Transient(error) => RecoveryOutcome::Transient(error),
    }
}

/// Join a parent directory path with a single child name into a scope-relative
/// path, tolerating an empty (scope-root) parent.
fn namespace_join(parent: &str, child_name: &str) -> String {
    let parent = parent.trim_matches('/');
    let child_name = child_name.trim_matches('/');
    if parent.is_empty() {
        child_name.to_string()
    } else {
        format!("{parent}/{child_name}")
    }
}

/// Update the residue witness for one applied mutation: record explicit
/// deletions, and forget any path this mount (re)creates so a legitimate
/// recreation after a delete is never mistaken for residue.
fn witness_namespace_mutation(
    recently_deleted: &mut RecentlyDeleted,
    mutation: &VfsNamespaceMutation,
) {
    match mutation {
        VfsNamespaceMutation::DeleteFile { path, .. }
        | VfsNamespaceMutation::RemoveDirectory { path } => recently_deleted.record(path),
        VfsNamespaceMutation::CreateFile { path, .. }
        | VfsNamespaceMutation::CreateDirectory { path, .. }
        | VfsNamespaceMutation::CreateSymlink { path, .. }
        | VfsNamespaceMutation::SetMode { path, .. } => recently_deleted.forget(path),
        VfsNamespaceMutation::CreateHardLink {
            source_path,
            destination_path,
        } => {
            recently_deleted.forget(source_path);
            recently_deleted.forget(destination_path);
        }
        VfsNamespaceMutation::Rename { from, to } => {
            // `from` is vacated by this mount; `to` becomes live. A later
            // CreateFile at `from` would forget it again, keeping a recreated
            // path safe from the residue sweep.
            recently_deleted.record(from);
            recently_deleted.forget(to);
        }
    }
}

/// A bounded, FIFO-evicting witness of paths this mount has deleted. Used only
/// as a conservative "is this residue" signal during RemoveDirectory
/// reconciliation; a miss (evicted or never-seen) safely degrades to treating a
/// child as live and dead-lettering, never to deleting real data.
struct RecentlyDeleted {
    seen: std::collections::HashSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl RecentlyDeleted {
    fn new(capacity: usize) -> Self {
        Self {
            seen: std::collections::HashSet::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    fn record(&mut self, path: &str) {
        let path = path.trim_matches('/').to_string();
        if path.is_empty() {
            return;
        }
        if self.seen.insert(path.clone()) {
            self.order.push_back(path);
            while self.order.len() > self.capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.seen.remove(&evicted);
                }
            }
        }
    }

    fn forget(&mut self, path: &str) {
        let path = path.trim_matches('/');
        if self.seen.remove(path)
            && let Some(index) = self.order.iter().position(|entry| entry == path)
        {
            self.order.remove(index);
        }
    }

    fn contains(&self, path: &str) -> bool {
        self.seen.contains(path.trim_matches('/'))
    }
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
    for (record, error) in &resolution.dead_lettered {
        match dead_letter_mutation(&shared.journal_path, &record.mutation, error) {
            Ok(record_path) => {
                preserved_dead_letters.push(record.operation_id.clone());
                tracing::error!(
                    journal = %shared.journal_path.display(),
                    mutation = ?record.mutation,
                    dead_letter = %record_path.display(),
                    error = %error,
                    "vfs namespace mutation rejected by gateway; recorded and dropped from journal"
                );
            }
            Err(record_error) => {
                preservation_failures.push((record.operation_id.clone(), record_error.to_string()));
                tracing::error!(
                    journal = %shared.journal_path.display(),
                    mutation = ?record.mutation,
                    error = %error,
                    record_error = %record_error,
                    "vfs namespace mutation rejected by gateway; failed to record dead letter"
                );
            }
        }
    }
    let failed_ids = preservation_failures
        .iter()
        .map(|(operation_id, _)| operation_id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let committed = resolution
        .committed
        .iter()
        .map(|(operation_id, revision)| (operation_id.as_str(), *revision))
        .collect::<std::collections::HashMap<_, _>>();
    let dead_lettered = resolution
        .dead_lettered
        .iter()
        .map(|(record, _)| record.operation_id.as_str())
        .collect::<std::collections::HashSet<_>>();
    state.pending = state
        .pending
        .drain(..)
        .filter_map(|mut record| {
            if let Some(revision) = committed.get(record.operation_id.as_str()) {
                record.committed_revision = Some(*revision);
            }
            (!dead_lettered.contains(record.operation_id.as_str())
                || failed_ids.contains(record.operation_id.as_str()))
            .then_some(record)
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
                .filter(|(record, _)| preserved_dead_letters.contains(&record.operation_id))
                .map(|(record, _)| format!("{:?}", record.mutation))
                .collect::<Vec<_>>()
                .join(", "),
        ));
    }
    let mut failures = preservation_failures
        .into_iter()
        .map(|(operation_id, error)| format!("mutation {operation_id}: {error}"))
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

fn read_journal(path: &Path) -> Result<VecDeque<NamespaceJournalRecord>> {
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
) -> Result<(VecDeque<NamespaceJournalRecord>, bool)> {
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
        let decoded = serde_json::from_slice::<NamespaceJournalRecord>(&line).or_else(|_| {
            serde_json::from_slice::<VfsNamespaceMutation>(&line).map(|mutation| {
                NamespaceJournalRecord {
                    operation_id: Uuid::new_v4().to_string(),
                    mutation,
                    projected_metadata: None,
                    committed_revision: None,
                }
            })
        });
        match decoded {
            Ok(decoded_record) => {
                pending.push_back(decoded_record);
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

fn rewrite_pending(path: &Path, pending: &VecDeque<NamespaceJournalRecord>) -> Result<()> {
    let temporary = path.with_extension("jsonl.tmp");
    {
        let mut writer = BufWriter::new(
            File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?,
        );
        for record in pending {
            serde_json::to_writer(&mut writer, record).context("rewrite namespace journal")?;
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

    use axum::body::{Body, to_bytes};
    use axum::extract::State;
    use axum::http::{HeaderName, HeaderValue, Method, Request, StatusCode as AxumStatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::any;
    use axum::{Json, Router};
    use tokio::runtime::Builder;

    fn journal_record(
        operation_id: &str,
        mutation: VfsNamespaceMutation,
    ) -> NamespaceJournalRecord {
        NamespaceJournalRecord {
            operation_id: operation_id.to_string(),
            mutation,
            projected_metadata: None,
            committed_revision: None,
        }
    }

    fn journal_mutations(
        records: &VecDeque<NamespaceJournalRecord>,
    ) -> VecDeque<VfsNamespaceMutation> {
        records
            .iter()
            .map(|record| record.mutation.clone())
            .collect()
    }

    fn test_metadata(kind: &str, mode: u32, file_id: Option<&str>) -> VfsMetadata {
        VfsMetadata {
            kind: kind.to_string(),
            size_bytes: 0,
            file_id: file_id.map(str::to_string),
            link_count: if kind == "directory" { 2 } else { 1 },
            link_target: None,
            content_hash: None,
            executable: mode & 0o111 != 0,
            mode: Some(mode),
            updated_at: None,
        }
    }

    fn test_entry(name: &str, kind: &str, mode: u32, file_id: Option<&str>) -> VfsDirEntry {
        dir_entry_from_metadata(name.to_string(), test_metadata(kind, mode, file_id))
    }

    #[test]
    fn revision_fenced_snapshot_observes_the_final_generic_namespace_projection() {
        let mutations = vec![
            VfsNamespaceMutation::CreateFile {
                path: "created-then-deleted".to_string(),
                mode: Some(0o640),
            },
            VfsNamespaceMutation::DeleteFile {
                path: "created-then-deleted".to_string(),
                precondition: None,
            },
            VfsNamespaceMutation::CreateDirectory {
                path: "created-dir".to_string(),
                mode: Some(0o750),
            },
            VfsNamespaceMutation::CreateSymlink {
                path: "created-link".to_string(),
                target: "target".to_string(),
            },
            VfsNamespaceMutation::CreateHardLink {
                source_path: "source".to_string(),
                destination_path: "source-alias".to_string(),
            },
            VfsNamespaceMutation::Rename {
                from: "old-name".to_string(),
                to: "new-name".to_string(),
            },
            VfsNamespaceMutation::SetMode {
                path: "new-name".to_string(),
                mode: 0o700,
            },
            VfsNamespaceMutation::RemoveDirectory {
                path: "removed-dir".to_string(),
            },
        ];
        let entries = vec![
            VfsPublicationSnapshotEntry {
                path: "created-then-deleted".to_string(),
                metadata: None,
            },
            VfsPublicationSnapshotEntry {
                path: "created-dir".to_string(),
                metadata: Some(test_metadata("directory", 0o750, Some("dir"))),
            },
            VfsPublicationSnapshotEntry {
                path: "created-link".to_string(),
                metadata: Some(test_metadata("symlink", 0o777, Some("link"))),
            },
            VfsPublicationSnapshotEntry {
                path: "source".to_string(),
                metadata: Some(test_metadata("file", 0o644, Some("source"))),
            },
            VfsPublicationSnapshotEntry {
                path: "source-alias".to_string(),
                metadata: Some(test_metadata("file", 0o644, Some("source"))),
            },
            VfsPublicationSnapshotEntry {
                path: "old-name".to_string(),
                metadata: None,
            },
            VfsPublicationSnapshotEntry {
                path: "new-name".to_string(),
                metadata: Some(test_metadata("file", 0o700, Some("renamed"))),
            },
            VfsPublicationSnapshotEntry {
                path: "removed-dir".to_string(),
                metadata: None,
            },
        ];

        assert!(publication_snapshot_observes_mutations(
            mutations.as_slice(),
            entries.as_slice(),
        ));
        assert!(
            !publication_snapshot_observes_mutations(
                mutations.as_slice(),
                &entries[..entries.len() - 1],
            ),
            "a partial response must retain the projection for a later read fence",
        );
    }

    #[test]
    fn generic_projection_covers_every_namespace_mutation_until_revision_is_observed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let hard_link_metadata = VfsMetadata {
            link_count: 2,
            ..test_metadata("file", 0o644, Some("inode-source"))
        };
        let records = VecDeque::from([
            NamespaceJournalRecord {
                projected_metadata: Some(test_metadata("file", 0o640, None)),
                ..journal_record(
                    "create-file",
                    VfsNamespaceMutation::CreateFile {
                        path: "created".to_string(),
                        mode: Some(0o640),
                    },
                )
            },
            journal_record(
                "create-directory",
                VfsNamespaceMutation::CreateDirectory {
                    path: "created-dir".to_string(),
                    mode: Some(0o750),
                },
            ),
            journal_record(
                "create-symlink",
                VfsNamespaceMutation::CreateSymlink {
                    path: "created-link".to_string(),
                    target: "source".to_string(),
                },
            ),
            NamespaceJournalRecord {
                projected_metadata: Some(hard_link_metadata),
                ..journal_record(
                    "create-hard-link",
                    VfsNamespaceMutation::CreateHardLink {
                        source_path: "source".to_string(),
                        destination_path: "source-alias".to_string(),
                    },
                )
            },
            journal_record(
                "delete-file",
                VfsNamespaceMutation::DeleteFile {
                    path: "deleted".to_string(),
                    precondition: None,
                },
            ),
            journal_record(
                "remove-directory",
                VfsNamespaceMutation::RemoveDirectory {
                    path: "deleted-dir".to_string(),
                },
            ),
            NamespaceJournalRecord {
                projected_metadata: Some(test_metadata("file", 0o644, Some("inode-renamed"))),
                ..journal_record(
                    "rename",
                    VfsNamespaceMutation::Rename {
                        from: "old-name".to_string(),
                        to: "new-name".to_string(),
                    },
                )
            },
            journal_record(
                "set-mode",
                VfsNamespaceMutation::SetMode {
                    path: "new-name".to_string(),
                    mode: 0o700,
                },
            ),
            journal_record(
                "empty-probe-create",
                VfsNamespaceMutation::CreateFile {
                    path: "awaited-empty-probe".to_string(),
                    mode: Some(0o600),
                },
            ),
            journal_record(
                "empty-probe-unlink",
                VfsNamespaceMutation::DeleteFile {
                    path: "awaited-empty-probe".to_string(),
                    precondition: None,
                },
            ),
        ]);
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: records,
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
        let journal = NamespaceJournal {
            shared: Arc::clone(&shared),
            worker: Mutex::new(None),
        };
        let backing = vec![
            test_entry("source", "file", 0o644, Some("inode-source")),
            test_entry("created", "file", 0o600, Some("stale-created")),
            test_entry("deleted", "file", 0o644, Some("inode-deleted")),
            test_entry("deleted-dir", "directory", 0o755, Some("inode-deleted-dir")),
            test_entry("old-name", "file", 0o644, Some("inode-renamed")),
            test_entry("new-name", "file", 0o600, Some("stale-destination")),
        ];

        let projected = journal
            .project_directory("", backing, 0)
            .expect("project originating mount listing");
        assert!(projected.applied);
        let by_name = projected
            .value
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(by_name["created"].file_id, None);
        assert_eq!(by_name["created"].mode, Some(0o640));
        assert!(by_name.contains_key("created-dir"));
        assert!(by_name.contains_key("created-link"));
        assert_eq!(by_name["source"].link_count, 2);
        assert_eq!(
            by_name["source-alias"].file_id.as_deref(),
            Some("inode-source")
        );
        assert!(!by_name.contains_key("deleted"));
        assert!(!by_name.contains_key("deleted-dir"));
        assert!(!by_name.contains_key("old-name"));
        assert_eq!(
            by_name["new-name"].file_id.as_deref(),
            Some("inode-renamed")
        );
        assert_eq!(by_name["new-name"].mode, Some(0o700));
        assert!(!by_name.contains_key("awaited-empty-probe"));
        let projected = journal
            .project_metadata("awaited-empty-probe", None, 0)
            .expect("project awaited empty create/unlink");
        assert!(projected.applied);
        assert!(projected.value.is_none());

        {
            let mut state = shared.state.lock().expect("state");
            for record in &mut state.pending {
                record.committed_revision = Some(77);
            }
        }
        let projected = journal
            .project_metadata("deleted", Some(test_metadata("file", 0o644, None)), 76)
            .expect("projection before publication observation");
        assert!(projected.applied);
        assert!(projected.value.is_none());
        assert_eq!(shared.state.lock().expect("state").pending.len(), 10);
        journal
            .observe_server_revision(77)
            .expect("observe committed namespace revision");
        assert!(
            shared.state.lock().expect("state").pending.is_empty(),
            "projection retires only after a read observes its committed revision"
        );
        let projected = journal
            .project_metadata("deleted", Some(test_metadata("file", 0o644, None)), 77)
            .expect("projection after publication observation");
        assert!(!projected.applied);
        assert!(projected.value.is_some());
    }

    #[test]
    fn flush_waits_for_transient_failure_to_recover() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal_path = dir.path().join("namespace.jsonl");
        let shared = Arc::new(Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([journal_record(
                    "mkdir-src",
                    VfsNamespaceMutation::CreateDirectory {
                        path: "src".to_string(),
                        mode: None,
                    },
                )]),
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
        let mkdir_record = journal_record("mkdir", mkdir.clone());
        let rejected_record = journal_record("rejected", rejected.clone());
        let retained_record = journal_record("retained", retained.clone());
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([
                    mkdir_record.clone(),
                    rejected_record.clone(),
                    retained_record,
                ]),
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
            committed: vec![(mkdir_record.operation_id, 41)],
            dead_lettered: vec![(
                rejected_record,
                "vfs request failed: 409 Conflict".to_string(),
            )],
            transient_error: None,
        };

        let mut state = shared.state.lock().expect("state");
        apply_namespace_resolution(&shared, &mut state, resolution);

        assert_eq!(state.pending.len(), 2);
        assert_eq!(state.pending[0].mutation, mkdir);
        assert_eq!(state.pending[0].committed_revision, Some(41));
        assert_eq!(state.pending[1].mutation, retained);
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
        assert!(journal_after.contains("\"committed_revision\":41"));
        assert!(journal_after.contains("src/later"));
        assert!(!journal_after.contains("probe"));
    }

    /// Stub gateway driving the RemoveDirectory recovery path. `/namespace-many`
    /// rejects RemoveDirectory with 409 until either enough attempts pass
    /// (`rmdir_success_after`) or, when `require_residue_delete` is set, a
    /// DeleteFile has swept the residue child. `/tree` returns `tree_children`
    /// until the residue is deleted, then an empty listing.
    #[derive(Default)]
    struct RecoveryGatewayState {
        rmdir_calls: usize,
        rmdir_success_after: usize,
        require_residue_delete: bool,
        residue_deleted: bool,
        tree_children: Vec<VfsDirEntry>,
        deleted_paths: Vec<String>,
    }

    fn revision_response(mut response: Response) -> Response {
        response.headers_mut().insert(
            HeaderName::from_static("x-chevalier-vfs-namespace-revision"),
            HeaderValue::from_static("1"),
        );
        response
    }

    async fn recovery_gateway(
        State(state): State<Arc<Mutex<RecoveryGatewayState>>>,
        request: Request<Body>,
    ) -> Response {
        let method = request.method().clone();
        let path = request.uri().path().to_string();
        match (method, path.as_str()) {
            (Method::POST, "/lease") => Json(serde_json::json!({
                "resource_key": "namespace-recovery-test",
                "owner_token": "00000000-0000-0000-0000-000000000001",
                "task_id": null
            }))
            .into_response(),
            (Method::DELETE, "/lease") => AxumStatusCode::NO_CONTENT.into_response(),
            (Method::POST, "/namespace-many") => {
                let body = to_bytes(request.into_body(), 1024 * 1024)
                    .await
                    .expect("read namespace-many request");
                let payload: serde_json::Value =
                    serde_json::from_slice(&body).expect("decode namespace-many request");
                let mutation = &payload["mutations"][0];
                let kind = mutation["kind"].as_str().unwrap_or_default();
                let mut state = state.lock().unwrap();
                match kind {
                    "delete_file" => {
                        state.residue_deleted = true;
                        if let Some(path) = mutation["path"].as_str() {
                            state.deleted_paths.push(path.to_string());
                        }
                        revision_response(AxumStatusCode::OK.into_response())
                    }
                    "remove_directory" => {
                        state.rmdir_calls += 1;
                        let succeeds = if state.require_residue_delete {
                            state.residue_deleted
                        } else {
                            state.rmdir_calls > state.rmdir_success_after
                        };
                        if succeeds {
                            revision_response(AxumStatusCode::OK.into_response())
                        } else {
                            (AxumStatusCode::CONFLICT, "directory not empty").into_response()
                        }
                    }
                    _ => revision_response(AxumStatusCode::OK.into_response()),
                }
            }
            (Method::GET, "/tree") => {
                let entries = {
                    let state = state.lock().unwrap();
                    if state.residue_deleted {
                        Vec::new()
                    } else {
                        state.tree_children.clone()
                    }
                };
                revision_response(Json(entries).into_response())
            }
            _ => AxumStatusCode::NOT_FOUND.into_response(),
        }
    }

    fn recovery_test_harness(
        state: RecoveryGatewayState,
    ) -> (
        tokio::runtime::Runtime,
        tokio::task::JoinHandle<()>,
        RemoteVfsClient,
        Arc<Mutex<RecoveryGatewayState>>,
    ) {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let shared = Arc::new(Mutex::new(state));
        let server_state = Arc::clone(&shared);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(recovery_gateway))
                    .with_state(server_state),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        (runtime, server, client, shared)
    }

    #[test]
    fn conflicted_remove_directory_recovers_after_write_drain_retry() {
        let (runtime, server, client, _shared) = recovery_test_harness(RecoveryGatewayState {
            // Initial replay + first recovery re-issue conflict; the second
            // re-issue is accepted, so recovery succeeds without reconciling.
            rmdir_success_after: 2,
            ..Default::default()
        });
        let record = journal_record(
            "rmdir-op",
            VfsNamespaceMutation::RemoveDirectory {
                path: "probe".to_string(),
            },
        );
        let mut recently_deleted = RecentlyDeleted::new(RECENTLY_DELETED_CAPACITY);
        let resolution = resolve_rejected_namespace_batch(
            &client,
            runtime.handle(),
            std::slice::from_ref(&record),
            VFS_SURFACE_KIND_VM_WORKSPACE,
            None,
            &mut NamespaceRecovery {
                drain_writes: None,
                recently_deleted: &mut recently_deleted,
            },
        );

        assert_eq!(
            resolution.committed,
            vec![("rmdir-op".to_string(), 1)],
            "a 409-conflicted RemoveDirectory recovers instead of dead-lettering"
        );
        assert!(resolution.dead_lettered.is_empty());
        assert!(resolution.transient_error.is_none());
        assert!(recently_deleted.contains("probe"));
        server.abort();
    }

    #[test]
    fn conflicted_remove_directory_dead_letters_live_foreign_child() {
        let (runtime, server, client, _shared) = recovery_test_harness(RecoveryGatewayState {
            // Never accepts the rmdir; reconcile finds a live child it may not
            // delete, so the mutation must dead-letter exactly as before.
            rmdir_success_after: usize::MAX,
            tree_children: vec![test_entry("live.rs", "file", 0o644, Some("inode-live"))],
            ..Default::default()
        });
        let record = journal_record(
            "rmdir-op",
            VfsNamespaceMutation::RemoveDirectory {
                path: "probe".to_string(),
            },
        );
        let mut recently_deleted = RecentlyDeleted::new(RECENTLY_DELETED_CAPACITY);
        let resolution = resolve_rejected_namespace_batch(
            &client,
            runtime.handle(),
            std::slice::from_ref(&record),
            VFS_SURFACE_KIND_VM_WORKSPACE,
            None,
            &mut NamespaceRecovery {
                drain_writes: None,
                recently_deleted: &mut recently_deleted,
            },
        );

        assert!(resolution.committed.is_empty());
        assert_eq!(resolution.dead_lettered.len(), 1);
        assert_eq!(resolution.dead_lettered[0].0.operation_id, "rmdir-op");
        assert!(
            resolution.dead_lettered[0].1.contains("not this mount's residue"),
            "a live sibling-mount child must fail the rmdir, never be deleted"
        );
        assert!(resolution.transient_error.is_none());
        server.abort();
    }

    #[test]
    fn conflicted_remove_directory_sweeps_recently_deleted_residue_then_succeeds() {
        let (runtime, server, client, shared) = recovery_test_harness(RecoveryGatewayState {
            // Only accepts the rmdir once the resurrected child is swept.
            rmdir_success_after: usize::MAX,
            require_residue_delete: true,
            tree_children: vec![test_entry(
                "resurrected.rs",
                "file",
                0o644,
                Some("inode-resurrected"),
            )],
            ..Default::default()
        });
        let record = journal_record(
            "rmdir-op",
            VfsNamespaceMutation::RemoveDirectory {
                path: "probe".to_string(),
            },
        );
        // This mount previously deleted the child; a racing write resurrected
        // it. The witness lets the reconcile recognize it as sweepable residue.
        let mut recently_deleted = RecentlyDeleted::new(RECENTLY_DELETED_CAPACITY);
        recently_deleted.record("probe/resurrected.rs");
        let resolution = resolve_rejected_namespace_batch(
            &client,
            runtime.handle(),
            std::slice::from_ref(&record),
            VFS_SURFACE_KIND_VM_WORKSPACE,
            None,
            &mut NamespaceRecovery {
                drain_writes: None,
                recently_deleted: &mut recently_deleted,
            },
        );

        assert_eq!(
            resolution.committed,
            vec![("rmdir-op".to_string(), 1)],
            "residue sweep must let the RemoveDirectory finally commit"
        );
        assert!(resolution.dead_lettered.is_empty());
        assert!(resolution.transient_error.is_none());
        let deleted = shared.lock().unwrap().deleted_paths.clone();
        assert_eq!(
            deleted,
            vec!["test-scope/probe/resurrected.rs".to_string()],
            "exactly the scoped residue child is deleted ahead of the rmdir retry"
        );
        server.abort();
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
        let applied_record = journal_record("applied", applied.clone());
        let failed_record = journal_record("failed", failed.clone());
        let later_record = journal_record("later", later.clone());
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([applied_record.clone(), failed_record, later_record]),
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
                committed: vec![(applied_record.operation_id, 42)],
                dead_lettered: Vec::new(),
                transient_error: Some("gateway unavailable".to_string()),
            },
        );

        assert_eq!(
            journal_mutations(&state.pending),
            VecDeque::from([applied.clone(), failed.clone(), later.clone()]),
            "the committed projection remains ahead of the failed suffix until a read observes it"
        );
        assert_eq!(state.pending[0].committed_revision, Some(42));
        assert_eq!(state.last_error.as_deref(), Some("gateway unavailable"));
        drop(state);
        assert_eq!(
            journal_mutations(&read_journal(&journal_path).expect("reopen journal")),
            VecDeque::from([applied, failed, later]),
            "restart retains the committed projection but replays only the uncommitted suffix"
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
        let rejected_record = journal_record("rejected", rejected.clone());
        let later_record = journal_record("later", later.clone());
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([rejected_record.clone(), later_record]),
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
                dead_lettered: vec![(
                    rejected_record,
                    "vfs request failed: 409 Conflict".to_string(),
                )],
                committed: Vec::new(),
                transient_error: None,
            },
        );

        assert_eq!(
            journal_mutations(&state.pending),
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
            journal_mutations(&read_journal(&journal_path).expect("reopen journal")),
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
            pending.front().map(|record| &record.mutation),
            Some(&VfsNamespaceMutation::Rename {
                from: "src/generated/00000.old".to_string(),
                to: "src/generated/00000.new".to_string(),
            })
        );
        assert_eq!(
            pending.back().map(|record| &record.mutation),
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
        assert_eq!(journal_mutations(&pending), VecDeque::from([first]));
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
    fn namespace_read_barriers_are_scoped_to_affected_paths() {
        assert!(namespace_mutation_affects_path(
            "repo/src",
            "repo/src/lib.rs",
            false
        ));
        assert!(namespace_mutation_affects_path(
            "repo/src/new.rs",
            "repo/src",
            true
        ));
        assert!(!namespace_mutation_affects_path(
            "repo/target/tmp",
            "repo/src",
            true
        ));
        assert!(!namespace_mutation_affects_path(
            "other-repo/generated",
            "repo",
            true
        ));
        assert!(namespace_mutation_affects_path("repo", "repo", false));
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
            journal_mutations(&read_journal(&journal_path).expect("torn tail is recoverable")),
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
        let mutation_record = journal_record("rename", mutation.clone());
        let shared = Shared {
            state: Mutex::new(JournalState {
                pending: VecDeque::from([mutation_record.clone()]),
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
                committed: vec![(mutation_record.operation_id, 43)],
                dead_lettered: Vec::new(),
                transient_error: None,
            },
        );

        assert_eq!(
            journal_mutations(&state.pending),
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
            let first_record = journal_record("first", first.clone());
            let shared = Arc::new(Shared {
                state: Mutex::new(JournalState {
                    pending: VecDeque::from([first_record]),
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
                assert_eq!(
                    journal_mutations(&state.pending),
                    VecDeque::from([first.clone()])
                );
            }

            journal
                .enqueue(later.clone())
                .expect("next append repairs and reanchors the live WAL");
            assert_eq!(
                journal_mutations(&read_journal(&journal_path).expect("reopen live WAL")),
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
