//! Eager hydration of the remote scope into the mount-local backing tree, and
//! the read-only follower that keeps an observer mount current.
//!
//! Hydration runs once, at mount, **before** the FUSE session is spawned, so the
//! guest never observes a partially materialized tree. A complete eager hydrate
//! is deliberate: it gives one unambiguous view, and lazy remote population is a
//! later optimization only if startup evidence demands it.
//!
//! ## Route composition (no single route returns a whole tree)
//!
//! * `GET /tree` (`list_dir_versioned`) breadth-first from the scope root is the
//!   **only** source of directories, empty directories, and directory modes.
//!   `/subtree-metadata` walks directories but emits only files and symlinks, so
//!   it can accelerate file metadata but can never replace the tree walk.
//! * `POST /prefetch-subtree` warms small-file bytes in packs; every file the
//!   pack did not return is fetched with `GET /file/raw`, and anything above the
//!   in-memory bound is streamed with ranged reads straight into the backing
//!   file. A 1 GiB file is never buffered whole.
//! * Symlink targets come from the directory entry's `link_target`; their bytes
//!   are never read.
//! * Hard links are grouped by remote `file_id`: materialize the first occurrence,
//!   `link(2)` the rest locally. `find_hard_link_alias` is deliberately not used
//!   -- it has a 2 s retry budget tuned for the unlink path.
//!
//! ## Traps the implementation must handle
//!
//! * `/tree` is issued with a 1 MiB hash budget, so files above it come back with
//!   `content_hash: None`. A missing hash is not an empty file; hash locally after
//!   download instead.
//! * Every read carries its own namespace revision. A multi-call hydrate that
//!   straddles a remote publication is a hard error, not something to paper over:
//!   invariant 3 forbids a parallel server-side writer behind a mounted view.
//! * `subtree-metadata` silently truncates at its limit; a `node_modules` tree
//!   blows past it, so a complete hydrate can never be one call.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chevalier_sandbox::vfs::VfsDirEntry;
use futures::stream::{self, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::mount::MountLocalView;
use super::tree::{BackingTree, MountFile};
use super::types::{LocalKind, parent_of};
use super::{HYDRATE_CONCURRENCY, STREAM_PAYLOAD_THRESHOLD_BYTES};
use crate::fuse::client::{RangeRead, RemoteVfsClient};

/// Entry and byte bounds for one `prefetch-subtree` pack. The entry cap matches
/// the gateway's own `MAX_SUBTREE_METADATA_ENTRIES`, so a saturated answer is
/// exactly the signal to descend into the prefix's child directories.
const HYDRATE_PREFETCH_MAX_ENTRIES: i64 = 4_096;
const HYDRATE_PREFETCH_MAX_PACK_BYTES: u64 = 64 * 1024 * 1024;

/// A file at or below this size is fetched whole with `/file/raw`; anything
/// larger is streamed in ranged chunks straight into the backing file, so the
/// 1 GiB path never materializes a 1 GiB buffer.
const HYDRATE_INLINE_DOWNLOAD_LIMIT_BYTES: u64 = STREAM_PAYLOAD_THRESHOLD_BYTES;
const HYDRATE_STREAM_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

/// Modes applied when the gateway reports none. `executable` is the only
/// permission bit the older wire form carries.
const HYDRATE_DIRECTORY_MODE: u32 = 0o755;
const HYDRATE_EXECUTABLE_MODE: u32 = 0o755;
const HYDRATE_FILE_MODE: u32 = 0o644;
const POSIX_MODE_MASK: u32 = 0o7777;

/// How often a read-only observer probes the scope's namespace revision, and
/// how far it backs off while the gateway is unreachable.
const FOLLOWER_POLL_INTERVAL: Duration = Duration::from_secs(1);
const FOLLOWER_BACKOFF_MIN: Duration = Duration::from_millis(500);
const FOLLOWER_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// What one hydrate produced. `remote_revision` becomes the mount's checkpoint
/// base, and the publisher resumes from it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct HydrationOutcome {
    pub(crate) remote_revision: u64,
    pub(crate) directories: u64,
    pub(crate) files: u64,
    pub(crate) symlinks: u64,
    pub(crate) hard_links: u64,
    pub(crate) bytes: u64,
}

/// One remote file entry the walk discovered, reduced to what materializing it
/// needs.
#[derive(Clone, Debug)]
struct RemoteFile {
    path: String,
    size_bytes: u64,
    /// `None` means the file exceeded the `/tree` hash budget, **not** that it
    /// is empty. The download is hashed locally either way; only the comparison
    /// against the gateway is skipped.
    content_hash: Option<String>,
    mode: Option<u32>,
    executable: bool,
}

impl RemoteFile {
    fn local_mode(&self) -> u32 {
        match self.mode {
            Some(mode) => mode & POSIX_MODE_MASK,
            None if self.executable => HYDRATE_EXECUTABLE_MODE,
            None => HYDRATE_FILE_MODE,
        }
    }
}

/// Every path that shares one remote `file_id`. Exactly one member is
/// downloaded; the rest become local hard links onto it, which is what makes
/// hard-link identity survive hydration.
#[derive(Debug)]
struct LinkBucket {
    members: Vec<RemoteFile>,
    /// Index of the member whose bytes are already in the backing tree.
    materialized: Option<usize>,
}

/// Everything one hydrate accumulates while it walks, plus the single revision
/// the whole walk must agree on.
#[derive(Debug, Default)]
struct HydrationState {
    base_revision: u64,
    outcome: HydrationOutcome,
    buckets: Vec<LinkBucket>,
    bucket_of_path: HashMap<String, usize>,
    bucket_of_file_id: HashMap<String, usize>,
    /// Immediate child directories of every directory the walk listed, so the
    /// prefetch descent follows the same shape without a second round of `/tree`.
    child_directories: HashMap<String, Vec<String>>,
}

impl HydrationState {
    /// Fold one response's namespace revision into the hydrate's base.
    ///
    /// A `0` revision means the gateway sent no revision header at all, which is
    /// not evidence of anything and is ignored. Two different non-zero revisions
    /// mean a publication landed underneath the walk: with a single owner that is
    /// a parallel server-side writer, which invariant 3 forbids, so it is a hard
    /// error rather than something to re-walk around.
    fn observe_revision(&mut self, revision: u64, what: &str) -> Result<()> {
        if revision == 0 {
            return Ok(());
        }
        if self.base_revision == 0 {
            self.base_revision = revision;
            return Ok(());
        }
        if revision != self.base_revision {
            bail!(
                "vfs scope changed during hydration: {what} answered namespace revision {revision} \
                 but the hydrate is based on {}; a server-side writer behind a mounted view is not \
                 supported",
                self.base_revision
            );
        }
        Ok(())
    }

    /// Record a file entry, joining it to its hard-link bucket. A path already
    /// recorded by an earlier phase of the same hydrate is left alone.
    fn push_file(&mut self, file: RemoteFile, file_id: Option<String>) {
        if self.bucket_of_path.contains_key(&file.path) {
            return;
        }
        if let Some(file_id) = file_id {
            if let Some(&index) = self.bucket_of_file_id.get(&file_id) {
                self.bucket_of_path.insert(file.path.clone(), index);
                self.buckets[index].members.push(file);
                return;
            }
            self.bucket_of_file_id.insert(file_id, self.buckets.len());
        }
        let index = self.buckets.len();
        self.bucket_of_path.insert(file.path.clone(), index);
        self.buckets.push(LinkBucket {
            members: vec![file],
            materialized: None,
        });
    }

    /// Leader paths of every bucket that still needs bytes, sorted so the
    /// prefetch descent can test a prefix with a binary search.
    fn pending_paths_sorted(&self) -> Vec<String> {
        let mut paths: Vec<String> = self
            .buckets
            .iter()
            .filter(|bucket| bucket.materialized.is_none())
            .flat_map(|bucket| bucket.members.iter().map(|file| file.path.clone()))
            .collect();
        paths.sort();
        paths
    }
}

/// Materializes a remote scope into the backing tree through the gateway.
pub(crate) struct MountHydrator {
    client: RemoteVfsClient,
    view: Arc<MountLocalView>,
    /// Bound on concurrent gateway reads. Every phase issues its requests
    /// through one `buffer_unordered` of this width, so the hydrate never has
    /// more than this many round trips outstanding.
    concurrency: usize,
}

impl MountHydrator {
    pub(crate) fn new(client: RemoteVfsClient, view: Arc<MountLocalView>) -> Self {
        Self {
            client,
            view,
            concurrency: HYDRATE_CONCURRENCY,
        }
    }

    /// Materialize the whole scope.
    ///
    /// Order: record the base revision, BFS the directory tree creating each
    /// directory with its remote mode before descending, create symlinks, group
    /// files by `file_id` and download one member of each group, hard-link the
    /// rest, apply modes after content, verify every downloaded file against the
    /// remote content hash where one was supplied, prove the closing revision
    /// still equals the base, and seed the mount's checkpoint with it.
    ///
    /// On failure the backing tree is reset rather than left half-populated: an
    /// ambiguous view is never served.
    pub(crate) async fn hydrate(&self) -> Result<HydrationOutcome> {
        match self.hydrate_scope().await {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                if let Err(reset) = self.view.tree().reset() {
                    error!(
                        scope = %self.view.scope_path(),
                        error = %reset,
                        "failed to reset the backing tree after a failed hydrate"
                    );
                }
                Err(error)
            }
        }
    }

    async fn hydrate_scope(&self) -> Result<HydrationOutcome> {
        let tree = self.view.tree();
        // An interrupted earlier attempt leaves debris that no idempotent
        // re-materialization removes, and hydration is always this mount's first
        // act, so starting from an empty tree is both safe and necessary.
        if backing_directory_is_populated(tree.root())? {
            warn!(
                scope = %self.view.scope_path(),
                "backing tree was not empty at hydrate; discarding an interrupted earlier attempt"
            );
            tree.reset()
                .context("reset the backing tree before hydrating")?;
        }

        let mut state = HydrationState::default();
        let root = [String::new()];
        self.walk_subtree(&root[0], false, &mut state).await?;
        self.materialize_content(&root, &mut state).await?;

        // The closing listing is the other half of the trap-3 check: the walk
        // agreed with itself, and this proves it also agrees with the scope as it
        // stands now.
        let closing = self
            .client
            .list_dir_versioned(&root[0])
            .await
            .context("re-read the vfs scope root to close hydration")?;
        state.observe_revision(closing.revision, "the closing scope-root listing")?;

        // Hydrated bytes are only durable once the filesystem holding the backing
        // tree has flushed them; the checkpoint seeded below is what recovery
        // trusts afterwards, so the order matters.
        sync_backing_filesystem(tree.root())?;

        let mut outcome = state.outcome;
        outcome.remote_revision = state.base_revision;
        self.seed_checkpoint(outcome.remote_revision)?;
        info!(
            scope = %self.view.scope_path(),
            revision = outcome.remote_revision,
            directories = outcome.directories,
            files = outcome.files,
            symlinks = outcome.symlinks,
            hard_links = outcome.hard_links,
            bytes = outcome.bytes,
            "hydrated the vfs scope into the mount-local backing tree"
        );
        Ok(outcome)
    }

    /// Re-materialize exactly the paths and subtrees a remote publication
    /// touched. Used by [`RemoteFollower`]; never used by a writable mount,
    /// where a remote acknowledgement must not change the local view.
    pub(crate) async fn rehydrate_paths(
        &self,
        paths: &[String],
        subtrees: &[String],
    ) -> Result<HydrationOutcome> {
        let mut state = HydrationState::default();
        for path in paths {
            self.refresh_path(path, &mut state).await?;
        }
        let mut walked: Vec<String> = Vec::new();
        for subtree in subtrees {
            // The prefix's own entry is not in its own listing, so it is
            // refreshed as a point path first; only a directory is then walked.
            let kind = self.refresh_path(subtree, &mut state).await?;
            if subtree.is_empty() || matches!(kind, Some(LocalKind::Directory)) {
                self.walk_subtree(subtree, true, &mut state).await?;
                walked.push(subtree.clone());
            }
        }
        let mut prefixes = walked;
        prefixes.extend(paths.iter().map(|path| parent_of(path)));
        self.materialize_content(&prefixes, &mut state).await?;
        sync_backing_filesystem(self.view.tree().root())?;

        let mut outcome = state.outcome;
        outcome.remote_revision = state.base_revision;
        Ok(outcome)
    }

    // -- namespace walk ------------------------------------------------------

    /// Breadth-first `/tree` walk of one prefix. Each directory is created with
    /// its remote mode **before** the walk descends into it, so a child never
    /// races its parent, and symlinks are created from `link_target` without ever
    /// reading their bytes. Files are only recorded here; their content is
    /// materialized afterwards, once every hard-link bucket is complete.
    async fn walk_subtree(
        &self,
        prefix: &str,
        prune: bool,
        state: &mut HydrationState,
    ) -> Result<()> {
        let client = &self.client;
        let mut level = vec![prefix.to_string()];
        let mut is_first_level = true;
        while !level.is_empty() {
            let listings: Vec<(String, Result<_>)> =
                stream::iter(level.into_iter().map(move |dir| async move {
                    let answer = client.list_dir_versioned(&dir).await;
                    (dir, answer)
                }))
                .buffer_unordered(self.concurrency)
                .collect()
                .await;

            let mut next: Vec<String> = Vec::new();
            for (dir, answer) in listings {
                let answer = answer
                    .with_context(|| format!("list the vfs directory {dir:?} during hydration"))?;
                state.observe_revision(answer.revision, &format!("the listing of {dir:?}"))?;
                let Some(entries) = answer.value else {
                    if is_first_level {
                        // An absent scope root is an empty scope, not a fault.
                        debug!(
                            directory = %dir,
                            "vfs directory is absent at hydration; nothing to materialize"
                        );
                        continue;
                    }
                    bail!(
                        "vfs directory {dir:?} disappeared during hydration; a server-side writer \
                         behind a mounted view is not supported"
                    );
                };
                let children = self.apply_listing(&dir, entries, prune, state)?;
                next.extend(children);
            }
            is_first_level = false;
            level = next;
        }
        Ok(())
    }

    /// Materialize one directory's entries and report its child directories.
    fn apply_listing(
        &self,
        directory: &str,
        mut entries: Vec<VfsDirEntry>,
        prune: bool,
        state: &mut HydrationState,
    ) -> Result<Vec<String>> {
        let tree = self.view.tree();
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        let mut children: Vec<String> = Vec::new();
        let mut names: HashSet<String> = HashSet::with_capacity(entries.len());
        for entry in entries {
            if entry.name.is_empty() || entry.name.contains('/') {
                bail!(
                    "vfs directory {directory:?} returned an unusable entry name {:?}",
                    entry.name
                );
            }
            let path = join_mount_path(directory, &entry.name);
            names.insert(entry.name.clone());
            let Some(kind) = LocalKind::from_wire_kind(entry.kind.as_str()) else {
                bail!("vfs entry {path:?} has an unknown kind {:?}", entry.kind);
            };
            match kind {
                LocalKind::Directory => {
                    let mode = entry
                        .mode
                        .map(|mode| mode & POSIX_MODE_MASK)
                        .unwrap_or(HYDRATE_DIRECTORY_MODE);
                    tree.hydrate_directory(&path, mode)
                        .with_context(|| format!("hydrate the backing directory {path:?}"))?;
                    state.outcome.directories += 1;
                    children.push(path);
                }
                LocalKind::Symlink => {
                    let target = entry.link_target.as_deref().ok_or_else(|| {
                        anyhow!("vfs symlink {path:?} was reported without a link target")
                    })?;
                    tree.hydrate_symlink(&path, target)
                        .with_context(|| format!("hydrate the backing symlink {path:?}"))?;
                    state.outcome.symlinks += 1;
                }
                LocalKind::File => {
                    state.push_file(
                        RemoteFile {
                            path,
                            size_bytes: entry.size_bytes,
                            content_hash: entry.content_hash,
                            mode: entry.mode,
                            executable: entry.executable,
                        },
                        entry.file_id,
                    );
                }
            }
        }
        if prune {
            self.prune_removed(directory, &names)?;
        }
        state
            .child_directories
            .insert(directory.to_string(), children.clone());
        Ok(children)
    }

    /// Re-read one exact path and apply it to the backing tree, reporting the
    /// kind the gateway holds it as. `None` means the path is gone remotely and
    /// has been removed locally.
    async fn refresh_path(
        &self,
        path: &str,
        state: &mut HydrationState,
    ) -> Result<Option<LocalKind>> {
        let tree = self.view.tree();
        if path.is_empty() {
            // The scope root is the backing tree root; it exists by construction
            // and carries no entry of its own.
            return Ok(Some(LocalKind::Directory));
        }
        let answer = self
            .client
            .stat_versioned(path)
            .await
            .with_context(|| format!("stat the vfs path {path:?} during rehydration"))?;
        state.observe_revision(answer.revision, &format!("the stat of {path:?}"))?;
        let Some(metadata) = answer.value else {
            remove_backing_entry(&backing_path(tree, path)?)?;
            return Ok(None);
        };
        let Some(kind) = LocalKind::from_wire_kind(metadata.kind.as_str()) else {
            bail!("vfs path {path:?} has an unknown kind {:?}", metadata.kind);
        };
        match kind {
            LocalKind::Directory => {
                let mode = metadata
                    .mode
                    .map(|mode| mode & POSIX_MODE_MASK)
                    .unwrap_or(HYDRATE_DIRECTORY_MODE);
                tree.hydrate_directory(path, mode)
                    .with_context(|| format!("hydrate the backing directory {path:?}"))?;
                state.outcome.directories += 1;
            }
            LocalKind::Symlink => {
                let target = metadata.link_target.as_deref().ok_or_else(|| {
                    anyhow!("vfs symlink {path:?} was reported without a link target")
                })?;
                self.ensure_parent_directory(path)?;
                tree.hydrate_symlink(path, target)
                    .with_context(|| format!("hydrate the backing symlink {path:?}"))?;
                state.outcome.symlinks += 1;
            }
            LocalKind::File => {
                self.ensure_parent_directory(path)?;
                state.push_file(
                    RemoteFile {
                        path: path.to_string(),
                        size_bytes: metadata.size_bytes,
                        content_hash: metadata.content_hash,
                        mode: metadata.mode,
                        executable: metadata.executable,
                    },
                    metadata.file_id,
                );
            }
        }
        Ok(Some(kind))
    }

    /// A point refresh can name a path whose parent this observer has never
    /// listed. The walk always creates a parent before descending, so this only
    /// covers the point-path case.
    fn ensure_parent_directory(&self, path: &str) -> Result<()> {
        let parent = parent_of(path);
        if parent.is_empty() {
            return Ok(());
        }
        let tree = self.view.tree();
        if backing_path(tree, &parent)?.exists() {
            return Ok(());
        }
        tree.hydrate_directory(&parent, HYDRATE_DIRECTORY_MODE)
            .with_context(|| format!("hydrate the backing parent directory {parent:?}"))
    }

    /// Remove backing entries the gateway no longer lists. Only a rehydrate
    /// prunes: the eager hydrate starts from an empty tree.
    fn prune_removed(&self, directory: &str, remote_names: &HashSet<String>) -> Result<()> {
        let backing = backing_path(self.view.tree(), directory)?;
        let entries = match std::fs::read_dir(&backing) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read the backing directory {}", backing.display()));
            }
        };
        for entry in entries {
            let entry = entry.with_context(|| {
                format!(
                    "read an entry of the backing directory {}",
                    backing.display()
                )
            })?;
            let name = entry.file_name();
            // A name the gateway cannot spell can never be in the listing, so it
            // is by definition local-only debris.
            if name
                .to_str()
                .is_some_and(|name| remote_names.contains(name))
            {
                continue;
            }
            remove_backing_entry(&entry.path())?;
        }
        Ok(())
    }

    // -- content -------------------------------------------------------------

    /// Warm what the packs can carry, fetch the gaps, then link the aliases.
    async fn materialize_content(
        &self,
        prefixes: &[String],
        state: &mut HydrationState,
    ) -> Result<()> {
        if state.buckets.is_empty() {
            return Ok(());
        }
        self.warm_packs(prefixes, state).await?;
        self.fill_content_gaps(state).await?;
        self.link_aliases(state)
    }

    /// `prefetch-subtree` returns whatever the backend chose to pack, capped by
    /// entry count and pack bytes. A saturated answer means the prefix was
    /// truncated, so the descent re-issues against the prefix's child
    /// directories -- but only those that still cover a file needing bytes.
    ///
    /// A failing or absent prefetch route is not fatal: it is an accelerator, and
    /// `fill_content_gaps` fetches everything it did not deliver.
    async fn warm_packs(&self, prefixes: &[String], state: &mut HydrationState) -> Result<()> {
        let mut level: Vec<String> = prefixes.to_vec();
        level.sort();
        level.dedup();
        while !level.is_empty() {
            let pending = state.pending_paths_sorted();
            if pending.is_empty() {
                return Ok(());
            }
            level.retain(|prefix| subtree_has_pending(&pending, prefix));
            if level.is_empty() {
                return Ok(());
            }

            let client = &self.client;
            let answers: Vec<(String, Result<_>)> =
                stream::iter(level.into_iter().map(move |prefix| async move {
                    let answer = client
                        .prefetch_subtree_versioned(
                            &prefix,
                            HYDRATE_PREFETCH_MAX_ENTRIES,
                            HYDRATE_PREFETCH_MAX_PACK_BYTES,
                        )
                        .await;
                    (prefix, answer)
                }))
                .buffer_unordered(self.concurrency)
                .collect()
                .await;

            let mut next: Vec<String> = Vec::new();
            for (prefix, answer) in answers {
                let answer = match answer {
                    Ok(answer) => answer,
                    Err(error) => {
                        debug!(
                            prefix = %prefix,
                            error = %error,
                            "vfs prefetch-subtree is unavailable; hydrating this subtree with \
                             point reads"
                        );
                        continue;
                    }
                };
                state.observe_revision(
                    answer.revision,
                    &format!("the content prefetch of {prefix:?}"),
                )?;
                let saturated = answer.value.len() as i64 >= HYDRATE_PREFETCH_MAX_ENTRIES;
                for (path, bytes) in answer.value {
                    self.accept_packed_file(&path, &bytes, state)?;
                }
                if saturated {
                    if let Some(children) = state.child_directories.get(&prefix) {
                        next.extend(children.iter().cloned());
                    }
                }
            }
            level = next;
        }
        Ok(())
    }

    /// Take one packed file's bytes as its bucket's materialized member. The
    /// packed path becomes the bucket leader, so an alias that happened to be
    /// packed is used rather than re-downloaded under another name.
    fn accept_packed_file(
        &self,
        path: &str,
        bytes: &[u8],
        state: &mut HydrationState,
    ) -> Result<()> {
        let Some(&index) = state.bucket_of_path.get(path) else {
            return Ok(());
        };
        let bucket = &state.buckets[index];
        if bucket.materialized.is_some() {
            return Ok(());
        }
        let Some(member) = bucket.members.iter().position(|file| file.path == path) else {
            return Ok(());
        };
        let file = bucket.members[member].clone();
        verify_downloaded(
            &file,
            bytes.len() as u64,
            &chevalier_vfs_hash::hash_bytes(bytes),
        )?;
        let handle = self
            .view
            .tree()
            .hydrate_file(&file.path, file.local_mode())
            .with_context(|| format!("hydrate the backing file {:?}", file.path))?;
        write_all_at(&handle, bytes, 0)?;
        handle
            .set_mode(file.local_mode())
            .with_context(|| format!("apply the hydrated mode of {:?}", file.path))?;
        state.buckets[index].materialized = Some(member);
        state.outcome.files += 1;
        state.outcome.bytes += bytes.len() as u64;
        Ok(())
    }

    /// Fetch every bucket the packs did not deliver, bounded by the same
    /// concurrency as the walk.
    async fn fill_content_gaps(&self, state: &mut HydrationState) -> Result<()> {
        let pending: Vec<usize> = state
            .buckets
            .iter()
            .enumerate()
            .filter(|(_, bucket)| bucket.materialized.is_none())
            .map(|(index, _)| index)
            .collect();
        if pending.is_empty() {
            return Ok(());
        }
        let base_revision = state.base_revision;
        let results: Vec<(usize, Result<u64>)> = {
            let buckets = &state.buckets;
            stream::iter(pending.into_iter().map(move |index| {
                let file = &buckets[index].members[0];
                async move { (index, self.download_file(file, base_revision).await) }
            }))
            .buffer_unordered(self.concurrency)
            .collect()
            .await
        };

        for (index, result) in results {
            let bytes = result?;
            state.buckets[index].materialized = Some(0);
            state.outcome.files += 1;
            state.outcome.bytes += bytes;
        }
        Ok(())
    }

    /// Download one file straight into the backing tree.
    ///
    /// Small files come back whole from `/file/raw`; anything above the
    /// in-memory bound is streamed in ranged chunks written at their offset, so
    /// the peak footprint of a 1 GiB file is one chunk. The mode is applied after
    /// the content, and the local hash is always computed -- a missing gateway
    /// hash means the file exceeded the `/tree` hash budget, never that it is
    /// empty.
    async fn download_file(&self, file: &RemoteFile, base_revision: u64) -> Result<u64> {
        let mode = file.local_mode();
        let handle = self
            .view
            .tree()
            .hydrate_file(&file.path, mode)
            .with_context(|| format!("hydrate the backing file {:?}", file.path))?;

        let written = if file.size_bytes == 0 {
            verify_downloaded(file, 0, &chevalier_vfs_hash::hash_bytes(&[]))?;
            0
        } else if file.size_bytes <= HYDRATE_INLINE_DOWNLOAD_LIMIT_BYTES {
            let answer = self
                .client
                .read_file_raw_versioned(&file.path)
                .await
                .with_context(|| format!("read the vfs file {:?} during hydration", file.path))?;
            reconcile_revision(
                base_revision,
                answer.revision,
                &format!("the read of {:?}", file.path),
            )?;
            let bytes = answer.value.ok_or_else(|| {
                anyhow!(
                    "vfs file {:?} disappeared during hydration; a server-side writer behind a \
                     mounted view is not supported",
                    file.path
                )
            })?;
            verify_downloaded(
                file,
                bytes.len() as u64,
                &chevalier_vfs_hash::hash_bytes(&bytes),
            )?;
            write_all_at(&handle, &bytes, 0)?;
            bytes.len() as u64
        } else {
            self.stream_file(file, &handle, base_revision).await?
        };

        handle
            .set_mode(mode)
            .with_context(|| format!("apply the hydrated mode of {:?}", file.path))?;
        Ok(written)
    }

    /// Ranged streaming of one large file directly into its backing descriptor.
    async fn stream_file(
        &self,
        file: &RemoteFile,
        handle: &MountFile,
        base_revision: u64,
    ) -> Result<u64> {
        let mut hasher = chevalier_vfs_hash::ContentHasher::new();
        let mut offset = 0u64;
        while offset < file.size_bytes {
            let length = HYDRATE_STREAM_CHUNK_BYTES.min(file.size_bytes - offset);
            let answer = self
                .client
                .read_file_range_versioned(
                    &file.path,
                    offset,
                    length,
                    // A file above the `/tree` hash budget carries no hash to pin
                    // the range against; the revision check below is what proves
                    // the stream did not straddle a publication.
                    file.content_hash.as_deref(),
                )
                .await
                .with_context(|| {
                    format!(
                        "read bytes {offset}..{} of the vfs file {:?} during hydration",
                        offset + length,
                        file.path
                    )
                })?;
            reconcile_revision(
                base_revision,
                answer.revision,
                &format!("the ranged read of {:?}", file.path),
            )?;
            match answer.value {
                RangeRead::Bytes(bytes) => {
                    if bytes.is_empty() {
                        bail!(
                            "vfs file {:?} ended at {offset} bytes but was listed as {} bytes",
                            file.path,
                            file.size_bytes
                        );
                    }
                    hasher.update(&bytes);
                    write_all_at(handle, &bytes, offset)?;
                    offset += bytes.len() as u64;
                }
                RangeRead::NotFound => bail!(
                    "vfs file {:?} disappeared during hydration; a server-side writer behind a \
                     mounted view is not supported",
                    file.path
                ),
                RangeRead::Stale => bail!(
                    "vfs file {:?} changed during hydration; a server-side writer behind a \
                     mounted view is not supported",
                    file.path
                ),
            }
        }
        verify_downloaded(file, offset, &hasher.finalize())?;
        Ok(offset)
    }

    /// Give every remaining member of a bucket the leader's inode, so hard-link
    /// identity is a property of the backing tree from the first callback.
    fn link_aliases(&self, state: &mut HydrationState) -> Result<()> {
        let tree = self.view.tree();
        let mut linked = 0u64;
        for bucket in &state.buckets {
            let leader = bucket.materialized.ok_or_else(|| {
                anyhow!(
                    "hydration left {:?} without content",
                    bucket
                        .members
                        .first()
                        .map(|file| file.path.as_str())
                        .unwrap_or_default()
                )
            })?;
            let leader_path = bucket.members[leader].path.as_str();
            for (index, member) in bucket.members.iter().enumerate() {
                if index == leader {
                    continue;
                }
                // `link(2)` refuses an existing name, and a rehydrate can be
                // pointed at a path this observer already materialized. The
                // leader still holds the content, so detaching the alias first
                // cannot lose bytes.
                self.ensure_parent_directory(&member.path)?;
                remove_backing_entry(&backing_path(tree, &member.path)?)?;
                tree.hydrate_hard_link(leader_path, &member.path)
                    .with_context(|| {
                        format!(
                            "hard-link the hydrated alias {:?} onto {leader_path:?}",
                            member.path
                        )
                    })?;
                linked += 1;
            }
        }
        state.outcome.hard_links += linked;
        Ok(())
    }

    /// Seed the mount's durable cursor with the revision the hydrate is a
    /// faithful copy of, so the publisher resumes from a coherent base instead of
    /// from revision zero. A read-only observer has no WAL and nothing to seed.
    fn seed_checkpoint(&self, remote_revision: u64) -> Result<()> {
        let Some(wal) = self.view.wal() else {
            return Ok(());
        };
        if remote_revision == 0 || remote_revision <= wal.remote_revision() {
            return Ok(());
        }
        wal.acknowledge(
            wal.acknowledged_sequence(),
            remote_revision,
            self.view.tree().tree_generation(),
        )
        .context("seed the mount checkpoint with the hydrated revision")
    }
}

// ---------------------------------------------------------------------------
// Backing-tree helpers
// ---------------------------------------------------------------------------

/// The backing path of a mount-relative path. The mount root is the empty
/// string, which is not a path the tree's validator accepts, so it is answered
/// from the root directly.
fn backing_path(tree: &BackingTree, path: &str) -> Result<PathBuf> {
    if path.is_empty() {
        return Ok(tree.root().to_path_buf());
    }
    tree.resolve(path)
}

fn join_mount_path(directory: &str, name: &str) -> String {
    if directory.is_empty() {
        name.to_string()
    } else {
        format!("{directory}/{name}")
    }
}

fn backing_directory_is_populated(root: &Path) -> Result<bool> {
    let mut entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("read the backing tree root {}", root.display()));
        }
    };
    match entries.next() {
        None => Ok(false),
        Some(Ok(_)) => Ok(true),
        Some(Err(error)) => {
            Err(error).with_context(|| format!("read the backing tree root {}", root.display()))
        }
    }
}

fn remove_backing_entry(path: &Path) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat the backing entry {}", path.display()));
        }
    };
    let removed = if metadata.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    match removed {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("remove the backing entry {}", path.display()))
        }
    }
}

/// A short positional write can happen on any filesystem, so hydration writes in
/// a loop rather than assuming one call moves the whole chunk.
fn write_all_at(handle: &MountFile, mut bytes: &[u8], mut offset: u64) -> Result<()> {
    while !bytes.is_empty() {
        let written = handle
            .write_at(bytes, offset)
            .with_context(|| format!("write hydrated bytes to {:?}", handle.path()))?;
        if written == 0 {
            bail!(
                "hydrated write to {:?} made no progress at offset {offset}",
                handle.path()
            );
        }
        offset += written as u64;
        bytes = &bytes[written..];
    }
    Ok(())
}

/// Flush the filesystem holding the backing tree once, instead of one `fsync`
/// per hydrated file. The checkpoint seeded afterwards is what recovery trusts,
/// so the bytes must already be durable when it lands.
fn sync_backing_filesystem(root: &Path) -> Result<()> {
    let directory = std::fs::File::open(root)
        .with_context(|| format!("open the backing tree root {} for sync", root.display()))?;
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;

        // SAFETY: the descriptor is open for the duration of the call.
        if unsafe { libc::syncfs(directory.as_raw_fd()) } != 0 {
            return Err(std::io::Error::last_os_error()).with_context(|| {
                format!(
                    "sync the filesystem holding the backing tree {}",
                    root.display()
                )
            });
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        directory
            .sync_all()
            .with_context(|| format!("sync the backing tree root {}", root.display()))?;
        // SAFETY: `sync(2)` takes no arguments and cannot fail.
        unsafe { libc::sync() };
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Verification helpers
// ---------------------------------------------------------------------------

/// Fold one response's revision into a hydrate whose base is already fixed.
fn reconcile_revision(base_revision: u64, revision: u64, what: &str) -> Result<()> {
    if base_revision == 0 || revision == 0 || revision == base_revision {
        return Ok(());
    }
    bail!(
        "vfs scope changed during hydration: {what} answered namespace revision {revision} but \
         the hydrate is based on {base_revision}; a server-side writer behind a mounted view is \
         not supported"
    )
}

/// Check a downloaded file against what the gateway said about it.
///
/// A `content_hash` is authoritative when present. When it is absent the file
/// merely exceeded the `/tree` 1 MiB hash budget -- it is emphatically not an
/// empty file -- so the local digest is computed anyway and the length is what
/// gets compared.
fn verify_downloaded(file: &RemoteFile, length: u64, local_hash: &str) -> Result<()> {
    match file.content_hash.as_deref() {
        Some(expected) if expected != local_hash => bail!(
            "hydrated vfs file {:?} does not match the gateway content hash: expected {expected}, \
             local {local_hash}",
            file.path
        ),
        Some(_) => Ok(()),
        None => {
            if length != file.size_bytes {
                bail!(
                    "hydrated vfs file {:?} is {length} bytes but was listed as {} bytes",
                    file.path,
                    file.size_bytes
                );
            }
            debug!(
                path = %file.path,
                length,
                hash = %local_hash,
                "hydrated a file the gateway hashed past its budget; hashed locally"
            );
            Ok(())
        }
    }
}

/// Whether any pending leader path lies inside `prefix`. `pending` is sorted, so
/// the whole subtree is one contiguous range.
fn subtree_has_pending(pending: &[String], prefix: &str) -> bool {
    if pending.is_empty() {
        return false;
    }
    if prefix.is_empty() {
        return true;
    }
    let needle = format!("{prefix}/");
    let start = pending.partition_point(|path| path.as_str() < needle.as_str());
    pending
        .get(start)
        .is_some_and(|path| path.starts_with(&needle))
}

// ---------------------------------------------------------------------------
// Read-only observer
// ---------------------------------------------------------------------------

/// Keeps a **read-only** observer mount current with the remote scope.
///
/// This is the one legitimate remaining use of the gateway's revision watch: an
/// observer has no local authority, so an external mutation genuinely must reach
/// it. It re-hydrates the affected paths and then issues kernel attr/dentry
/// invalidations for exactly those paths, from a dedicated thread that no FUSE
/// operation waits on -- calling the notifier inline deadlocks against the guest
/// kernel's parent-inode lock.
///
/// A writable mount never runs this. There, the local view is the authority and a
/// remote acknowledgement advances a cursor and nothing else.
///
/// The affected-set precision the watch answer carries (`paths` vs `subtrees`) is
/// not reachable from outside `client.rs`: `run_revision_watch` and its response
/// types are private, and the watch loop's other half -- the shared metadata
/// cache -- is retired. The follower therefore probes the scope's namespace
/// revision with the cheapest hashless read the client exposes and re-reads the
/// scope subtree when it advances. That is conservative, never incorrect: an
/// observer's whole job is to converge on the remote scope.
pub(crate) struct RemoteFollower {
    client: RemoteVfsClient,
    hydrator: Arc<MountHydrator>,
    invalidator: Arc<dyn KernelPathInvalidator>,
    scope_path: String,
    cancel: Arc<Notify>,
    stopped: Arc<AtomicBool>,
}

/// Cancels the follower when the mount goes away.
pub(crate) struct RemoteFollowerHandle {
    cancel: Arc<Notify>,
    stopped: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
}

impl RemoteFollower {
    /// Spawn the watch loop. Returns `None` for a writable mount.
    pub(crate) fn spawn(
        client: RemoteVfsClient,
        hydrator: Arc<MountHydrator>,
        view: Arc<MountLocalView>,
        invalidator: Arc<dyn KernelPathInvalidator>,
        tokio: Handle,
    ) -> Option<RemoteFollowerHandle> {
        if !view.is_read_only() {
            // The owning mount is the authority for its scope. A remote
            // acknowledgement advances a cursor and never changes what the guest
            // already observed, so a writable mount must not follow anything.
            return None;
        }
        let cancel = Arc::new(Notify::new());
        let stopped = Arc::new(AtomicBool::new(false));
        let follower = Self {
            client,
            hydrator,
            invalidator,
            scope_path: view.scope_path().to_string(),
            cancel: Arc::clone(&cancel),
            stopped: Arc::clone(&stopped),
        };
        let task = tokio.spawn(async move { follower.run().await });
        Some(RemoteFollowerHandle {
            cancel,
            stopped,
            task: Some(task),
        })
    }

    async fn run(self) {
        let scope_subtree = [String::new()];
        let mut observed = self.client.observed_namespace_revision();
        let mut backoff = FOLLOWER_BACKOFF_MIN;
        loop {
            if self.sleep_or_cancel(FOLLOWER_POLL_INTERVAL).await {
                break;
            }
            let revision = match self.probe_revision().await {
                Ok(revision) => revision,
                Err(error) => {
                    warn!(
                        scope = %self.scope_path,
                        error = %error,
                        "read-only vfs observer could not probe the scope revision"
                    );
                    if self.sleep_or_cancel(backoff).await {
                        break;
                    }
                    backoff = (backoff * 2).min(FOLLOWER_BACKOFF_MAX);
                    continue;
                }
            };
            backoff = FOLLOWER_BACKOFF_MIN;
            if revision == 0 || revision <= observed {
                continue;
            }
            match self.refresh(&[], &scope_subtree).await {
                Ok(outcome) => {
                    observed = revision.max(outcome.remote_revision);
                    debug!(
                        scope = %self.scope_path,
                        revision = observed,
                        directories = outcome.directories,
                        files = outcome.files,
                        symlinks = outcome.symlinks,
                        bytes = outcome.bytes,
                        "read-only vfs observer converged on a new remote revision"
                    );
                }
                Err(error) => {
                    warn!(
                        scope = %self.scope_path,
                        revision,
                        error = %error,
                        "read-only vfs observer failed to re-read the remote scope"
                    );
                    // The tree may hold a partial update, so nothing the kernel
                    // cached about this mount can be trusted until the next pass
                    // succeeds.
                    self.invalidator.invalidate_all();
                    if self.sleep_or_cancel(backoff).await {
                        break;
                    }
                    backoff = (backoff * 2).min(FOLLOWER_BACKOFF_MAX);
                }
            }
        }
        debug!(scope = %self.scope_path, "read-only vfs observer stopped");
    }

    /// The cheapest read that carries the scope's namespace revision: a hashless
    /// stat of the scope root.
    async fn probe_revision(&self) -> Result<u64> {
        Ok(self.client.stat_attributes_versioned("").await?.revision)
    }

    /// Re-read the affected set, then revoke exactly what was re-read.
    ///
    /// The revocation is issued **after** the backing tree already holds the new
    /// state, so a guest lookup racing it re-reads the new entry rather than the
    /// old one, and it is issued from this task, which holds no FUSE lock:
    /// [`KernelPathInvalidator`] implementations enqueue and return, because
    /// calling the notifier on a thread a FUSE operation waits on deadlocks
    /// against the guest kernel's parent-inode lock.
    async fn refresh(&self, paths: &[String], subtrees: &[String]) -> Result<HydrationOutcome> {
        let outcome = self.hydrator.rehydrate_paths(paths, subtrees).await?;
        if !paths.is_empty() {
            self.invalidator.invalidate_paths(paths);
        }
        if !subtrees.is_empty() {
            self.invalidator.invalidate_subtrees(subtrees);
        }
        Ok(outcome)
    }

    /// Sleep, returning `true` when the follower has been asked to stop.
    async fn sleep_or_cancel(&self, duration: Duration) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return true;
        }
        tokio::select! {
            _ = self.cancel.notified() => true,
            _ = tokio::time::sleep(duration) => self.stopped.load(Ordering::Acquire),
        }
    }
}

impl RemoteFollowerHandle {
    pub(crate) async fn shutdown(mut self) {
        self.stopped.store(true, Ordering::Release);
        self.cancel.notify_one();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for RemoteFollowerHandle {
    fn drop(&mut self) {
        // The task holds its own `Arc`s, so dropping the handle without awaiting
        // still stops it at the next poll boundary.
        self.stopped.store(true, Ordering::Release);
        self.cancel.notify_one();
    }
}

/// Revokes a guest kernel's cached attributes and dentries for paths an external
/// writer changed. Implemented by `fs.rs` over the fuser notifier.
///
/// Contract: implementations must enqueue and return. They must never call the
/// notifier on a thread a FUSE operation is waiting on.
pub(crate) trait KernelPathInvalidator: Send + Sync {
    fn invalidate_paths(&self, paths: &[String]);
    fn invalidate_subtrees(&self, subtrees: &[String]);
    fn invalidate_all(&self);
}
