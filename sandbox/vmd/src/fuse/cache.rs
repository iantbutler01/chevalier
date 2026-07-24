use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::time::{Duration, Instant};

use chevalier_sandbox::vfs::{
    VfsDirEntry as RemoteDirEntry, VfsMetadata as RemoteMetadata, VfsNamespaceMutation,
    VfsPublicationSnapshotEntry,
};

const FILE_TTL: Duration = Duration::from_secs(60);
const MAX_FILE_BYTES: usize = 10 * 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 256 * 1024 * 1024;
const MAX_FILES: usize = 16_384;
const SUBTREE_LOAD_MISS_THRESHOLD: u32 = 8;
pub(super) const SUBTREE_LOAD_REVISION_QUIET_PERIOD: Duration = Duration::from_millis(250);

/// The exact set of entries one locally observed publication superseded,
/// returned by the `observe_*` publication hooks so the caller can mirror the
/// eviction into each sibling mount's *kernel* attribute/entry cache. The FUSE
/// layer hands the kernel positive attr/entry leases only while the revision
/// watch is live (see `fs::ATTR_ENTRY_LEASE_TTL`); those leases stay coherent
/// precisely because every publication revokes exactly this set in the kernel
/// before the publication is acked (the revocation-ack ordering invariant).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct PublicationInvalidation {
    /// Exact paths whose attrs/dentry the publication changed (each changed
    /// path plus its parent directory).
    pub(super) paths: Vec<String>,
    /// Directory prefixes whose entire subtree the publication changed
    /// (`RemoveDirectory` / `Rename`). Every descendant the kernel cached under
    /// the prefix must be invalidated, not just the prefix itself.
    pub(super) subtrees: Vec<String>,
    /// Stable identities (hard-link inodes) the publication changed. An alias
    /// the kernel cached under a name *other* than the written path is reached
    /// through the shared identity rather than the path.
    pub(super) identities: Vec<String>,
}

impl PublicationInvalidation {
    /// True when the publication superseded nothing this mount could have handed
    /// a kernel, so no invalidation is needed.
    pub(super) fn is_empty(&self) -> bool {
        self.paths.is_empty() && self.subtrees.is_empty() && self.identities.is_empty()
    }

    /// The set a remote publication reported over the revision watch.
    ///
    /// Path-only: the watch answer names the paths a publication touched, and
    /// each is also treated as a subtree prefix because a remote
    /// `RemoveDirectory`/`Rename` supersedes everything the kernel cached
    /// beneath it and the watch does not distinguish the kinds. Identities are
    /// empty — a hard-link alias this mount cached under another name is
    /// reached through its own path in the same answer.
    pub(super) fn for_paths(paths: &[String]) -> Self {
        Self {
            paths: paths.to_vec(),
            subtrees: paths.to_vec(),
            identities: Vec::new(),
        }
    }
}

/// One mount's hook into its kernel FUSE session's invalidation channel.
/// Implemented by the fs layer (which owns the fuser notifier and inode table);
/// the cache and revision watch drive it without depending on those types, so
/// the coherence stack stays decoupled from the FUSE wiring.
pub(super) trait KernelInvalidator: Send + Sync {
    /// Drop the kernel's cached attrs/dentries for exactly the superseded set.
    ///
    /// Returns whether every revocation landed. A `false` return means the
    /// kernel may still serve a leased attr the coherence stack has superseded,
    /// so the caller must not report itself coherent on the strength of this
    /// sweep.
    fn invalidate(&self, invalidation: &PublicationInvalidation) -> bool;
    /// Drop every attr/dentry this mount handed the kernel. Used for a remote
    /// (cross-process) publication whose exact path set this process never
    /// learned — the watch 200 carries only a revision. Same return contract as
    /// [`KernelInvalidator::invalidate`].
    fn invalidate_all(&self) -> bool;
}

/// Per-registry set of live mount kernel-invalidation hooks, keyed by the same
/// coherence key as the shared cache. A local publication's commit hook and the
/// revision watch both fan out over every mount of the registry in this process,
/// so a sibling observer's kernel is revoked in lockstep with the shared cache
/// (a same-process observer has no other notification channel: the single shared
/// watch loop only acks cross-process publications through the gateway).
#[derive(Default)]
pub(super) struct MountInvalidators {
    inner: Mutex<Vec<Weak<dyn KernelInvalidator>>>,
}

impl MountInvalidators {
    pub(super) fn shared(key: &str) -> Arc<Self> {
        static REGISTRIES: OnceLock<Mutex<HashMap<String, Weak<MountInvalidators>>>> =
            OnceLock::new();
        let mut registries = REGISTRIES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(registry) = registries.get(key).and_then(Weak::upgrade) {
            return registry;
        }
        let registry = Arc::new(Self::default());
        registries.insert(key.to_string(), Arc::downgrade(&registry));
        registry
    }

    /// Register one mount's kernel-invalidation hook. Dead (unmounted) hooks are
    /// pruned opportunistically so the registry never grows across remounts.
    pub(super) fn register(&self, invalidator: Weak<dyn KernelInvalidator>) {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.retain(|existing| existing.strong_count() > 0);
        guard.push(invalidator);
    }

    /// Returns whether every mount's revocation landed.
    pub(super) fn invalidate(&self, invalidation: &PublicationInvalidation) -> bool {
        if invalidation.is_empty() {
            return true;
        }
        let mut clean = true;
        for invalidator in self.live() {
            // Sweep every mount before reporting: a failure must not skip the
            // mounts behind it.
            clean &= invalidator.invalidate(invalidation);
        }
        clean
    }

    /// Returns whether every mount's revocation landed.
    pub(super) fn invalidate_all(&self) -> bool {
        let mut clean = true;
        for invalidator in self.live() {
            clean &= invalidator.invalidate_all();
        }
        clean
    }

    /// Snapshot the live hooks and release the registry lock before invoking any
    /// of them: an invalidator performs blocking `writev`s into `/dev/fuse`,
    /// which must never run under the registry mutex.
    fn live(&self) -> Vec<Arc<dyn KernelInvalidator>> {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.retain(|existing| existing.strong_count() > 0);
        guard.iter().filter_map(Weak::upgrade).collect()
    }
}

#[derive(Clone)]
struct CachedFile {
    /// Read-through copy only. Dirty/open authoritative buffers live in the
    /// FUSE handle table, while journal-enqueued writes own durability.
    bytes: Vec<u8>,
    metadata: Option<RemoteMetadata>,
    expires_at: Instant,
    last_access: Instant,
}

#[derive(Clone)]
struct CachedMetadata {
    metadata: RemoteMetadata,
    revision: u64,
}

#[derive(Clone)]
struct CachedDirectory {
    entries: Vec<RemoteDirEntry>,
    revision: u64,
}

#[derive(Default)]
struct CacheState {
    /// Latest authoritative gateway revision reflected by `metadata`.
    ///
    /// Revisions are opaque monotonic tokens, not contiguous counters: the
    /// gateway may advance from `R` to `max(R + 1, Date.now() * 1000)`.
    /// Known local publications may therefore retag entries from this exact
    /// prior revision without requiring `revision == R + 1`. An unclassified
    /// authoritative read at a newer revision remains fail-closed and drops
    /// metadata from the prior revision.
    metadata_revision: u64,
    file_bytes: usize,
    files: HashMap<String, CachedFile>,
    identity_paths: HashMap<String, std::collections::HashSet<String>>,
    metadata: HashMap<String, CachedMetadata>,
    missing_metadata: HashMap<String, u64>,
    directories: HashMap<String, CachedDirectory>,
    subtree_revisions: HashMap<String, u64>,
    subtree_loads: HashSet<String>,
    subtree_misses: HashMap<String, (u64, u32, Instant)>,
    subtree_disabled: bool,
    directory_generation: u64,
}

#[derive(Default)]
pub struct RemoteFuseCache {
    inner: Mutex<CacheState>,
    subtree_changed: Condvar,
}

impl RemoteFuseCache {
    pub fn shared(key: &str) -> Arc<Self> {
        static CACHES: OnceLock<Mutex<HashMap<String, Weak<RemoteFuseCache>>>> = OnceLock::new();
        let mut caches = CACHES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cache) = caches.get(key).and_then(Weak::upgrade) {
            return cache;
        }
        let cache = Arc::new(Self::default());
        caches.insert(key.to_string(), Arc::downgrade(&cache));
        cache
    }

    pub fn get_file_matching(&self, path: &str, metadata: &RemoteMetadata) -> Option<Vec<u8>> {
        let mut inner = self.lock_inner();
        let entry = inner.files.get_mut(path)?;
        if entry.expires_at <= Instant::now()
            || !cached_metadata_matches(entry.metadata.as_ref(), metadata)
        {
            remove_file_locked(&mut inner, path);
            return None;
        }
        entry.last_access = Instant::now();
        Some(entry.bytes.clone())
    }

    /// Return the metadata bundled with a still-live content-cache entry.
    /// Callers may use this only for same-mount isolation of a dirty existing
    /// handle; ordinary lookup/getattr/open paths must revalidate remotely.
    pub fn get_committed_file_metadata(&self, path: &str) -> Option<RemoteMetadata> {
        let mut inner = self.lock_inner();
        let entry = inner.files.get_mut(path)?;
        if entry.expires_at <= Instant::now() {
            remove_file_locked(&mut inner, path);
            return None;
        }
        entry.last_access = Instant::now();
        entry.metadata.clone()
    }

    pub fn put_file(&self, path: &str, bytes: Vec<u8>, metadata: Option<RemoteMetadata>) {
        let mut inner = self.lock_inner();
        let now = Instant::now();
        put_file_locked(&mut inner, path.to_string(), bytes, metadata, now);
        enforce_file_limits_locked(&mut inner, now, MAX_FILES, MAX_TOTAL_BYTES);
    }

    pub fn get_dir(&self, path: &str, revision: u64) -> Option<Vec<RemoteDirEntry>> {
        if revision == 0 {
            return None;
        }
        let mut inner = self.lock_inner();
        observe_authoritative_revision_locked(&mut inner, revision);
        let entry = inner.directories.get(path)?;
        if entry.revision != revision {
            inner.directories.remove(path);
            return None;
        }
        Some(entry.entries.clone())
    }

    pub fn put_dir(&self, path: &str, entries: Vec<RemoteDirEntry>, revision: u64) {
        let generation = self.directory_generation(path);
        let _ = self.put_dir_if_generation(path, generation, entries, revision);
    }

    pub fn directory_generation(&self, _path: &str) -> u64 {
        self.lock_inner().directory_generation
    }

    /// Accept a listing only if no concurrent local namespace mutation
    /// invalidated it while the authoritative request was in flight and the
    /// response still matches the shared coherence revision.
    pub fn put_dir_if_generation(
        &self,
        path: &str,
        generation: u64,
        entries: Vec<RemoteDirEntry>,
        revision: u64,
    ) -> bool {
        if revision == 0 {
            return false;
        }
        let mut inner = self.lock_inner();
        observe_authoritative_revision_locked(&mut inner, revision);
        if inner.directory_generation != generation || inner.metadata_revision != revision {
            return false;
        }
        inner
            .directories
            .insert(path.to_string(), CachedDirectory { entries, revision });
        true
    }

    pub fn get_metadata(&self, path: &str, revision: u64) -> Option<RemoteMetadata> {
        // A rolling-upgrade/legacy gateway response without a publication
        // revision cannot fence cached metadata against remote mutations.
        if revision == 0 {
            return None;
        }
        let mut inner = self.lock_inner();
        observe_authoritative_revision_locked(&mut inner, revision);
        let entry = inner.metadata.get(path)?;
        if entry.revision != revision {
            inner.metadata.remove(path);
            return None;
        }
        Some(entry.metadata.clone())
    }

    pub fn put_metadata(&self, path: &str, metadata: RemoteMetadata, revision: u64) {
        if revision == 0 {
            return;
        }
        let mut inner = self.lock_inner();
        observe_authoritative_revision_locked(&mut inner, revision);
        if inner.metadata_revision == revision {
            let metadata = inner
                .metadata
                .get(path)
                .filter(|entry| entry.revision == revision)
                .map(|entry| preserve_stronger_metadata(&entry.metadata, metadata.clone()))
                .unwrap_or(metadata);
            inner
                .metadata
                .insert(path.to_string(), CachedMetadata { metadata, revision });
            inner.missing_metadata.remove(path);
        }
    }

    pub fn is_known_missing(&self, path: &str, revision: u64) -> bool {
        if revision == 0 {
            return false;
        }
        let mut inner = self.lock_inner();
        observe_authoritative_revision_locked(&mut inner, revision);
        inner.missing_metadata.get(path).copied() == Some(revision)
    }

    pub fn put_missing_metadata(&self, path: &str, revision: u64) {
        if revision == 0 {
            return;
        }
        let mut inner = self.lock_inner();
        observe_authoritative_revision_locked(&mut inner, revision);
        if inner.metadata_revision == revision {
            inner.metadata.remove(path);
            remove_file_locked(&mut inner, path);
            inner.missing_metadata.insert(path.to_string(), revision);
        }
    }

    /// Fail-closed entry for an authoritative revision observed out of band
    /// (a revision-watch publication). An unclassified newer token may carry
    /// another process's publication, so metadata/dirs/missing from the prior
    /// token are dropped; an older or equal token is ignored. This adds no new
    /// invalidation semantics — it is the same audited path the read routes
    /// take when they observe a newer revision.
    pub fn observe_authoritative_revision(&self, revision: u64) {
        let mut inner = self.lock_inner();
        observe_authoritative_revision_locked(&mut inner, revision);
    }

    /// Advance cache entries across one locally observed publication. The
    /// publishing client knows the exact paths and stable identities changed
    /// by this revision, so unrelated metadata can remain coherent. A skipped
    /// revision means an unknown writer may have changed anything and remains
    /// fail-closed.
    pub(super) fn observe_namespace_publication(
        &self,
        revision: u64,
        mutations: &[VfsNamespaceMutation],
    ) -> PublicationInvalidation {
        let mut inner = self.lock_inner();
        let mut affected_paths = HashSet::new();
        let mut affected_subtrees = HashSet::new();
        let mut affected_identities = HashSet::new();
        for mutation in mutations {
            let affects_descendants = matches!(
                mutation,
                VfsNamespaceMutation::RemoveDirectory { .. } | VfsNamespaceMutation::Rename { .. }
            );
            for path in mutation.paths().into_iter().filter(|path| !path.is_empty()) {
                collect_affected_path(
                    &inner,
                    path,
                    affects_descendants,
                    true,
                    &mut affected_paths,
                    &mut affected_subtrees,
                    &mut affected_identities,
                );
            }
        }
        advance_known_revision_locked(
            &mut inner,
            revision,
            &affected_paths,
            &affected_subtrees,
            &affected_identities,
        );
        publication_invalidation(affected_paths, affected_subtrees, affected_identities)
    }

    pub(super) fn observe_namespace_publication_snapshot(
        &self,
        revision: u64,
        mutations: &[VfsNamespaceMutation],
        entries: &[VfsPublicationSnapshotEntry],
    ) -> PublicationInvalidation {
        let invalidation = self.observe_namespace_publication(revision, mutations);
        self.install_publication_snapshot(revision, entries);
        invalidation
    }

    /// Content publication changes the named inode and every cached hard-link
    /// alias of its stable identity, but not unrelated namespace entries.
    pub(super) fn observe_write_publication(
        &self,
        revision: u64,
        writes: &[(String, Option<String>)],
    ) -> PublicationInvalidation {
        let mut inner = self.lock_inner();
        let mut affected_paths = HashSet::new();
        let mut affected_subtrees = HashSet::new();
        let mut affected_identities = HashSet::new();
        for (path, expected_file_id) in writes {
            collect_affected_path(
                &inner,
                path,
                false,
                true,
                &mut affected_paths,
                &mut affected_subtrees,
                &mut affected_identities,
            );
            if let Some(file_id) = expected_file_id {
                affected_identities.insert(file_id.clone());
            }
        }
        advance_known_revision_locked(
            &mut inner,
            revision,
            &affected_paths,
            &affected_subtrees,
            &affected_identities,
        );
        publication_invalidation(affected_paths, affected_subtrees, affected_identities)
    }

    pub(super) fn observe_write_publication_snapshot(
        &self,
        revision: u64,
        writes: &[(String, Option<String>)],
        entries: &[VfsPublicationSnapshotEntry],
    ) -> PublicationInvalidation {
        let invalidation = self.observe_write_publication(revision, writes);
        self.install_publication_snapshot(revision, entries);
        invalidation
    }

    fn install_publication_snapshot(&self, revision: u64, entries: &[VfsPublicationSnapshotEntry]) {
        for entry in entries {
            match entry.metadata.clone() {
                Some(metadata) => self.put_metadata(entry.path.as_str(), metadata, revision),
                None => self.put_missing_metadata(entry.path.as_str(), revision),
            }
        }
    }

    /// Coalesce one server-side subtree snapshot per prefix and stable
    /// coherence revision across every mount in this vmd process. A stream of
    /// namespace mutations advances the revision after each point lookup; do
    /// not turn that into a growing full-tree scan after every create. Once
    /// several misses observe a genuinely quiet revision, the workload is
    /// read-heavy enough to amortize one metadata/content prefetch.
    pub fn begin_subtree_load(&self, prefix: &str, revision: u64) -> bool {
        self.begin_subtree_load_at(prefix, revision, Instant::now())
    }

    fn begin_subtree_load_at(&self, prefix: &str, revision: u64, now: Instant) -> bool {
        let mut inner = self.lock_inner();
        // Sync the cache's authoritative revision to the coherence fence before
        // accounting a miss. A later get/put that first advances
        // metadata_revision clears subtree_misses (fail-closed on a revision
        // change); doing it up front keeps a miss recorded here from being
        // retroactively wiped within the same dispatch, so a genuine revision
        // change still amortizes into one snapshot instead of stalling an extra
        // miss short of the threshold.
        observe_authoritative_revision_locked(&mut inner, revision);
        loop {
            if inner.subtree_disabled {
                return false;
            }
            if revision != 0 && inner.subtree_revisions.get(prefix).copied() == Some(revision) {
                return false;
            }
            if inner.subtree_loads.contains(prefix) {
                inner = self
                    .subtree_changed
                    .wait(inner)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                continue;
            }
            let misses = inner
                .subtree_misses
                .entry(prefix.to_string())
                .or_insert((revision, 0, now));
            if misses.0 != revision {
                *misses = (revision, 0, now);
            }
            misses.1 = misses.1.saturating_add(1);
            if misses.1 < SUBTREE_LOAD_MISS_THRESHOLD
                || now.saturating_duration_since(misses.2) < SUBTREE_LOAD_REVISION_QUIET_PERIOD
            {
                return false;
            }
            if inner.subtree_loads.insert(prefix.to_string()) {
                return true;
            }
        }
    }

    pub fn finish_subtree_load(
        &self,
        prefix: &str,
        revision: u64,
        entries: Vec<(String, RemoteMetadata)>,
    ) {
        let mut inner = self.lock_inner();
        if revision != 0 {
            observe_authoritative_revision_locked(&mut inner, revision);
            if inner.metadata_revision == revision {
                for (path, metadata) in entries {
                    inner
                        .metadata
                        .insert(path, CachedMetadata { metadata, revision });
                }
                inner.subtree_revisions.insert(prefix.to_string(), revision);
                inner.subtree_misses.remove(prefix);
            }
        }
        inner.subtree_loads.remove(prefix);
        self.subtree_changed.notify_all();
    }

    pub fn finish_subtree_load_with_files(
        &self,
        prefix: &str,
        revision: u64,
        entries: Vec<(String, RemoteMetadata)>,
        files: Vec<(String, Vec<u8>, RemoteMetadata)>,
    ) {
        let mut inner = self.lock_inner();
        if revision != 0 {
            observe_authoritative_revision_locked(&mut inner, revision);
            if inner.metadata_revision == revision {
                for (path, metadata) in entries {
                    inner
                        .metadata
                        .insert(path, CachedMetadata { metadata, revision });
                }
                let now = Instant::now();
                for (path, bytes, metadata) in files {
                    put_file_locked(&mut inner, path, bytes, Some(metadata), now);
                }
                enforce_file_limits_locked(&mut inner, now, MAX_FILES, MAX_TOTAL_BYTES);
                inner.subtree_revisions.insert(prefix.to_string(), revision);
                inner.subtree_misses.remove(prefix);
            }
        }
        inner.subtree_loads.remove(prefix);
        self.subtree_changed.notify_all();
    }

    pub fn abort_subtree_load(&self, prefix: &str) {
        let mut inner = self.lock_inner();
        inner.subtree_loads.remove(prefix);
        self.subtree_changed.notify_all();
    }

    pub fn disable_subtree_loads(&self, prefix: &str) {
        let mut inner = self.lock_inner();
        inner.subtree_disabled = true;
        inner.subtree_loads.remove(prefix);
        inner.subtree_misses.clear();
        self.subtree_changed.notify_all();
    }

    pub fn invalidate(&self, path: &str) {
        let mut inner = self.lock_inner();
        invalidate_path_locked(&mut inner, path);
    }

    pub fn invalidate_identity(&self, file_id: &str) {
        let mut inner = self.lock_inner();
        let paths = inner
            .identity_paths
            .get(file_id)
            .cloned()
            .unwrap_or_default();
        for path in paths {
            invalidate_path_locked(&mut inner, &path);
        }
    }

    pub fn aliases_for_identity(&self, file_id: &str) -> Vec<String> {
        self.lock_inner()
            .identity_paths
            .get(file_id)
            .map(|paths| paths.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn lock_inner(&self) -> MutexGuard<'_, CacheState> {
        self.inner.lock().unwrap_or_else(|err| err.into_inner())
    }
}

fn invalidate_path_locked(inner: &mut CacheState, path: &str) {
    remove_file_locked(inner, path);
    inner.metadata.remove(path);
    inner.missing_metadata.remove(path);
    inner.directories.remove(path);
    bump_directory_generation(inner, path);
    if let Some(parent) = parent_path(path) {
        inner.directories.remove(parent.as_str());
        bump_directory_generation(inner, parent.as_str());
    }
}

fn collect_affected_path(
    inner: &CacheState,
    path: &str,
    affects_descendants: bool,
    affects_parent: bool,
    affected_paths: &mut HashSet<String>,
    affected_subtrees: &mut HashSet<String>,
    affected_identities: &mut HashSet<String>,
) {
    let path = path.trim_matches('/').to_string();
    affected_paths.insert(path.clone());
    if affects_descendants {
        affected_subtrees.insert(path.clone());
    }
    if affects_parent {
        if let Some(parent) = parent_path(path.as_str()) {
            affected_paths.insert(parent);
        }
    }
    if let Some(file_id) = inner
        .metadata
        .get(path.as_str())
        .and_then(|entry| entry.metadata.file_id.clone())
    {
        affected_identities.insert(file_id);
    }
}

/// Collect the affected sets computed for one publication into the
/// invalidation record handed back to the kernel-invalidation fan-out. This is
/// exactly what the shared cache just evicted, so the kernel drops precisely the
/// same set — no more (unrelated leases stay warm) and no less (the superseded
/// set is revoked before the publication is acked).
fn publication_invalidation(
    affected_paths: HashSet<String>,
    affected_subtrees: HashSet<String>,
    affected_identities: HashSet<String>,
) -> PublicationInvalidation {
    PublicationInvalidation {
        paths: affected_paths.into_iter().collect(),
        subtrees: affected_subtrees.into_iter().collect(),
        identities: affected_identities.into_iter().collect(),
    }
}

fn advance_known_revision_locked(
    inner: &mut CacheState,
    revision: u64,
    affected_paths: &HashSet<String>,
    affected_subtrees: &HashSet<String>,
    affected_identities: &HashSet<String>,
) {
    if revision == 0 {
        inner.metadata_revision = 0;
        inner.metadata.clear();
        inner.missing_metadata.clear();
        inner.directories.clear();
        inner.subtree_revisions.clear();
        return;
    }
    let previous_revision = inner.metadata_revision;
    let stale_paths = inner
        .metadata
        .iter_mut()
        .filter_map(|(path, entry)| {
            let affected = path_or_identity_is_affected(
                path,
                entry.metadata.file_id.as_deref(),
                affected_paths,
                affected_subtrees,
                affected_identities,
            );
            if affected {
                Some(path.clone())
            } else if revision <= previous_revision {
                None
            } else if previous_revision == 0 || entry.revision != previous_revision {
                Some(path.clone())
            } else {
                entry.revision = revision;
                None
            }
        })
        .collect::<Vec<_>>();
    for path in stale_paths {
        invalidate_path_locked(inner, path.as_str());
    }
    let stale_missing = inner
        .missing_metadata
        .iter_mut()
        .filter_map(|(path, entry_revision)| {
            if path_or_identity_is_affected(
                path,
                None,
                affected_paths,
                affected_subtrees,
                affected_identities,
            ) || previous_revision == 0
                || *entry_revision != previous_revision
            {
                Some(path.clone())
            } else {
                *entry_revision = revision;
                None
            }
        })
        .collect::<Vec<_>>();
    for path in stale_missing {
        inner.missing_metadata.remove(path.as_str());
    }
    let stale_directories = inner
        .directories
        .iter_mut()
        .filter_map(|(path, entry)| {
            if path_or_identity_is_affected(
                path,
                None,
                affected_paths,
                affected_subtrees,
                affected_identities,
            ) || previous_revision == 0
                || entry.revision != previous_revision
            {
                Some(path.clone())
            } else {
                entry.revision = revision;
                None
            }
        })
        .collect::<Vec<_>>();
    for path in stale_directories {
        inner.directories.remove(path.as_str());
    }
    let affected_files = inner
        .files
        .iter()
        .filter_map(|(path, entry)| {
            path_or_identity_is_affected(
                path,
                entry
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.file_id.as_deref()),
                affected_paths,
                affected_subtrees,
                affected_identities,
            )
            .then(|| path.clone())
        })
        .collect::<Vec<_>>();
    for path in affected_files {
        remove_file_locked(inner, path.as_str());
    }
    inner.metadata_revision = inner.metadata_revision.max(revision);
    inner.subtree_revisions.clear();
}

/// Observe a revision whose exact mutation set is not available to this cache.
/// A newer token may include a publication from another process or mount, so
/// retaining metadata from the prior token would violate cross-mount
/// coherence. Older in-flight reads are ignored instead of rolling the cache
/// backwards.
fn observe_authoritative_revision_locked(inner: &mut CacheState, revision: u64) {
    if revision == 0 || revision <= inner.metadata_revision {
        return;
    }
    if inner.metadata_revision != 0 {
        inner.metadata.clear();
        inner.missing_metadata.clear();
        inner.directories.clear();
        inner.subtree_revisions.clear();
        inner.subtree_misses.clear();
    }
    inner.metadata_revision = revision;
}

fn path_or_identity_is_affected(
    path: &str,
    file_id: Option<&str>,
    affected_paths: &HashSet<String>,
    affected_subtrees: &HashSet<String>,
    affected_identities: &HashSet<String>,
) -> bool {
    affected_paths.contains(path)
        || affected_subtrees.iter().any(|affected_path| {
            !affected_path.is_empty()
                && path
                    .strip_prefix(affected_path)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
        || file_id.is_some_and(|file_id| affected_identities.contains(file_id))
}

fn remove_file_locked(inner: &mut CacheState, path: &str) {
    let Some(previous) = inner.files.remove(path) else {
        return;
    };
    inner.file_bytes = inner.file_bytes.saturating_sub(previous.bytes.len());
    if let Some(file_id) = previous.metadata.and_then(|metadata| metadata.file_id) {
        let remove_identity = inner.identity_paths.get_mut(&file_id).is_some_and(|paths| {
            paths.remove(path);
            paths.is_empty()
        });
        if remove_identity {
            inner.identity_paths.remove(&file_id);
        }
    }
}

fn put_file_locked(
    inner: &mut CacheState,
    path: String,
    bytes: Vec<u8>,
    metadata: Option<RemoteMetadata>,
    now: Instant,
) {
    if bytes.len() > MAX_FILE_BYTES {
        return;
    }
    remove_file_locked(inner, path.as_str());
    inner.file_bytes += bytes.len();
    let file_id = metadata
        .as_ref()
        .and_then(|metadata| metadata.file_id.clone());
    inner.files.insert(
        path.clone(),
        CachedFile {
            bytes,
            metadata,
            expires_at: now + FILE_TTL,
            last_access: now,
        },
    );
    if let Some(file_id) = file_id {
        inner
            .identity_paths
            .entry(file_id)
            .or_default()
            .insert(path);
    }
}

fn bump_directory_generation(inner: &mut CacheState, _path: &str) {
    inner.directory_generation = inner.directory_generation.wrapping_add(1);
}

fn cached_metadata_matches(cached: Option<&RemoteMetadata>, current: &RemoteMetadata) -> bool {
    let Some(cached) = cached else {
        return false;
    };
    if cached.file_id != current.file_id || cached.link_count != current.link_count {
        return false;
    }
    match (
        cached.content_hash.as_deref(),
        current.content_hash.as_deref(),
    ) {
        (Some(cached_hash), Some(current_hash)) => cached_hash == current_hash,
        (None, None) => {
            cached.kind == current.kind
                && cached.size_bytes == current.size_bytes
                && cached.link_target == current.link_target
                && cached.executable == current.executable
        }
        _ => false,
    }
}

fn preserve_stronger_metadata(
    cached: &RemoteMetadata,
    mut current: RemoteMetadata,
) -> RemoteMetadata {
    if current.content_hash.is_none()
        && cached.content_hash.is_some()
        && cached.kind == current.kind
        && cached.size_bytes == current.size_bytes
        && cached.file_id == current.file_id
        && cached.link_count == current.link_count
        && cached.link_target == current.link_target
        && cached.executable == current.executable
        && cached.mode == current.mode
    {
        current.content_hash.clone_from(&cached.content_hash);
    }
    current
}

fn enforce_file_limits_locked(
    inner: &mut CacheState,
    now: Instant,
    max_entries: usize,
    max_bytes: usize,
) {
    let entries_exceeded = inner.files.len() > max_entries;
    let bytes_exceeded = inner.file_bytes > max_bytes;
    if !entries_exceeded && !bytes_exceeded {
        return;
    }
    let target_entries = if entries_exceeded {
        max_entries.saturating_mul(3) / 4
    } else {
        max_entries
    };
    let target_bytes = if bytes_exceeded {
        max_bytes.saturating_mul(3) / 4
    } else {
        max_bytes
    };
    prune_files_locked(inner, now, target_entries, target_bytes);
}

fn prune_files_locked(
    inner: &mut CacheState,
    now: Instant,
    target_entries: usize,
    target_bytes: usize,
) {
    let expired = inner
        .files
        .iter()
        .filter(|(_, value)| value.expires_at <= now)
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    for path in expired {
        remove_file_locked(inner, path.as_str());
    }
    if inner.files.len() <= target_entries && inner.file_bytes <= target_bytes {
        return;
    }
    let mut oldest = inner
        .files
        .iter()
        .map(|(path, value)| (path.clone(), value.last_access))
        .collect::<Vec<_>>();
    oldest.sort_unstable_by_key(|(_, last_access)| *last_access);
    for (path, _) in oldest {
        if inner.files.len() <= target_entries && inner.file_bytes <= target_bytes {
            break;
        }
        remove_file_locked(inner, path.as_str());
    }
}

fn parent_path(path: &str) -> Option<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    trimmed
        .rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .or(Some(String::new()))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        CacheState, CachedFile, MAX_FILES, RemoteFuseCache, SUBTREE_LOAD_MISS_THRESHOLD,
        SUBTREE_LOAD_REVISION_QUIET_PERIOD, enforce_file_limits_locked,
    };
    use chevalier_sandbox::vfs::{
        VfsDirEntry as RemoteDirEntry, VfsMetadata as RemoteMetadata, VfsNamespaceMutation,
        VfsPublicationSnapshotEntry,
    };

    fn entry(name: &str) -> RemoteDirEntry {
        RemoteDirEntry {
            name: name.to_string(),
            kind: "file".to_string(),
            size_bytes: 0,
            file_id: None,
            link_count: 1,
            link_target: None,
            content_hash: None,
            executable: false,
            mode: None,
            updated_at: None,
        }
    }

    fn metadata(content_hash: &str, size_bytes: u64) -> RemoteMetadata {
        RemoteMetadata {
            kind: "file".to_string(),
            size_bytes,
            file_id: None,
            link_count: 1,
            link_target: None,
            content_hash: Some(content_hash.to_string()),
            executable: false,
            mode: None,
            updated_at: None,
        }
    }

    #[test]
    fn known_namespace_publication_advances_only_unaffected_metadata() {
        let cache = RemoteFuseCache::default();
        cache.put_metadata("tree/changed", metadata("old", 3), 17);
        cache.put_metadata("tree/stable", metadata("stable", 6), 17);

        cache.observe_namespace_publication(
            1_800_000,
            &[VfsNamespaceMutation::CreateFile {
                path: "tree/changed".to_string(),
                mode: Some(0o644),
            }],
        );

        assert!(cache.get_metadata("tree/changed", 1_800_000).is_none());
        assert_eq!(
            cache.get_metadata("tree/stable", 1_800_000),
            Some(metadata("stable", 6))
        );
        cache.observe_namespace_publication(
            1_800_000,
            &[VfsNamespaceMutation::CreateFile {
                path: "tree/changed".to_string(),
                mode: Some(0o644),
            }],
        );
        assert_eq!(
            cache.get_metadata("tree/stable", 1_800_000),
            Some(metadata("stable", 6)),
            "replayed publication callbacks must be idempotent"
        );

        cache.put_metadata("other/read", metadata("remote", 6), 2_000_000);
        assert!(
            cache.get_metadata("tree/stable", 2_000_000).is_none(),
            "a newer authoritative read without a known mutation set must remain fail-closed"
        );
    }

    #[test]
    fn publication_snapshot_seeds_exact_changed_and_missing_metadata() {
        let cache = RemoteFuseCache::default();
        cache.put_metadata("tree/new", metadata("stale", 9), 17);
        cache.put_metadata("tree/deleted", metadata("old", 3), 17);
        cache.put_metadata("tree/stable", metadata("stable", 6), 17);
        let current = metadata("new", 4);

        cache.observe_namespace_publication_snapshot(
            18,
            &[
                VfsNamespaceMutation::CreateFile {
                    path: "tree/new".to_string(),
                    mode: Some(0o644),
                },
                VfsNamespaceMutation::DeleteFile {
                    path: "tree/deleted".to_string(),
                    precondition: None,
                },
            ],
            &[
                VfsPublicationSnapshotEntry {
                    path: "tree/new".to_string(),
                    metadata: Some(current.clone()),
                },
                VfsPublicationSnapshotEntry {
                    path: "tree/deleted".to_string(),
                    metadata: None,
                },
            ],
        );

        assert_eq!(cache.get_metadata("tree/new", 18), Some(current));
        assert!(cache.is_known_missing("tree/deleted", 18));
        assert_eq!(
            cache.get_metadata("tree/stable", 18),
            Some(metadata("stable", 6))
        );
    }

    #[test]
    fn write_publication_snapshot_seeds_exact_written_metadata() {
        let cache = RemoteFuseCache::default();
        let mut old = metadata("old", 3);
        old.file_id = Some("inode-1".to_string());
        cache.put_metadata("tree/file", old, 17);
        let mut current = metadata("new", 4);
        current.file_id = Some("inode-1".to_string());

        cache.observe_write_publication_snapshot(
            18,
            &[("tree/file".to_string(), Some("inode-1".to_string()))],
            &[VfsPublicationSnapshotEntry {
                path: "tree/file".to_string(),
                metadata: Some(current.clone()),
            }],
        );

        assert_eq!(cache.get_metadata("tree/file", 18), Some(current));
    }

    #[test]
    fn known_write_publication_invalidates_every_cached_alias() {
        let cache = RemoteFuseCache::default();
        let mut alias = metadata("old", 3);
        alias.file_id = Some("inode-1".to_string());
        cache.put_metadata("tree/a", alias.clone(), 17);
        cache.put_metadata("tree/b", alias, 17);
        cache.put_file("tree/b", b"old".to_vec(), cache.get_metadata("tree/b", 17));
        cache.put_metadata("tree/stable", metadata("stable", 6), 17);

        cache.observe_write_publication(18, &[("tree/a".to_string(), Some("inode-1".to_string()))]);

        assert!(cache.get_metadata("tree/a", 18).is_none());
        assert!(cache.get_metadata("tree/b", 18).is_none());
        assert!(cache.get_committed_file_metadata("tree/b").is_none());
        assert_eq!(
            cache.get_metadata("tree/stable", 18),
            Some(metadata("stable", 6))
        );
    }

    #[test]
    fn metadata_is_retained_only_for_its_authoritative_revision() {
        let cache = RemoteFuseCache::default();
        cache.put_dir("tree", vec![entry("file")], 17);
        cache.put_metadata("tree/file", metadata("hash", 4), 17);

        assert_eq!(cache.get_dir("tree", 17), Some(vec![entry("file")]));
        assert_eq!(
            cache.get_metadata("tree/file", 17),
            Some(metadata("hash", 4))
        );
        assert!(cache.get_metadata("tree/file", 18).is_none());
        assert!(cache.get_dir("tree", 18).is_none());
    }

    #[test]
    fn weaker_same_revision_metadata_preserves_content_hash() {
        let cache = RemoteFuseCache::default();
        let strong = metadata("hash", 4);
        let mut attributes_only = strong.clone();
        attributes_only.content_hash = None;

        cache.put_metadata("tree/file", strong.clone(), 17);
        cache.put_metadata("tree/file", attributes_only, 17);

        assert_eq!(cache.get_metadata("tree/file", 17), Some(strong));
    }

    #[test]
    fn known_missing_metadata_is_revision_fenced_and_retagged_by_unrelated_publications() {
        let cache = RemoteFuseCache::default();
        cache.put_missing_metadata("tree/missing", 17);
        assert!(cache.is_known_missing("tree/missing", 17));

        cache.observe_namespace_publication(
            18,
            &[VfsNamespaceMutation::CreateFile {
                path: "other/new".to_string(),
                mode: Some(0o644),
            }],
        );
        assert!(cache.is_known_missing("tree/missing", 18));

        cache.observe_namespace_publication(
            19,
            &[VfsNamespaceMutation::CreateFile {
                path: "tree/missing".to_string(),
                mode: Some(0o644),
            }],
        );
        assert!(!cache.is_known_missing("tree/missing", 19));
    }

    #[test]
    fn sibling_mounts_share_one_revision_fenced_cache() {
        let key = format!("cache-test-{}", uuid::Uuid::new_v4());
        let first = RemoteFuseCache::shared(&key);
        let second = RemoteFuseCache::shared(&key);
        first.put_metadata("tree/file", metadata("hash", 4), 17);

        assert_eq!(
            second.get_metadata("tree/file", 17),
            Some(metadata("hash", 4))
        );
        assert!(second.get_metadata("tree/file", 18).is_none());
    }

    #[test]
    fn subtree_snapshot_is_shared_and_reloads_only_for_a_new_revision() {
        let key = format!("subtree-cache-test-{}", uuid::Uuid::new_v4());
        let first = RemoteFuseCache::shared(&key);
        let second = RemoteFuseCache::shared(&key);
        let now = Instant::now();

        for _ in 1..SUBTREE_LOAD_MISS_THRESHOLD {
            assert!(!first.begin_subtree_load_at("", 17, now));
        }
        assert!(!first.begin_subtree_load_at("", 17, now));
        assert!(first.begin_subtree_load_at("", 17, now + SUBTREE_LOAD_REVISION_QUIET_PERIOD));
        first.finish_subtree_load("", 17, vec![("tree/file".to_string(), metadata("hash", 4))]);

        assert!(!second.begin_subtree_load_at("", 17, now + SUBTREE_LOAD_REVISION_QUIET_PERIOD));
        assert_eq!(
            second.get_metadata("tree/file", 17),
            Some(metadata("hash", 4))
        );
        for _ in 1..SUBTREE_LOAD_MISS_THRESHOLD {
            assert!(!second.begin_subtree_load_at("", 18, now));
        }
        assert!(!second.begin_subtree_load_at("", 18, now));
        assert!(second.begin_subtree_load_at("", 18, now + SUBTREE_LOAD_REVISION_QUIET_PERIOD));
        second.abort_subtree_load("");
        assert!(first.get_metadata("tree/file", 18).is_none());
    }

    #[test]
    fn advancing_revisions_never_trigger_a_growing_subtree_reload() {
        let cache = RemoteFuseCache::default();
        let now = Instant::now();

        for revision in 1..=100 {
            assert!(!cache.begin_subtree_load_at(
                "",
                revision,
                now + SUBTREE_LOAD_REVISION_QUIET_PERIOD
            ));
        }
    }

    #[test]
    fn file_cache_requires_matching_authoritative_metadata() {
        let cache = RemoteFuseCache::default();
        let expected = metadata("complete-hash", 8);
        cache.put_file("Cargo.toml", b"complete".to_vec(), Some(expected.clone()));

        assert_eq!(
            cache.get_committed_file_metadata("Cargo.toml"),
            Some(expected.clone())
        );
        assert_eq!(
            cache.get_file_matching("Cargo.toml", &expected),
            Some(b"complete".to_vec())
        );
        assert!(
            cache
                .get_file_matching("Cargo.toml", &metadata("truncated-hash", 1))
                .is_none()
        );
        assert!(
            cache
                .get_file_matching("Cargo.toml", &metadata("complete-hash", 8))
                .is_none()
        );
    }

    #[test]
    fn file_cache_caps_twenty_thousand_zero_byte_entries() {
        let cache = RemoteFuseCache::default();
        for index in 0..20_000 {
            cache.put_file(&format!("zero-{index}"), Vec::new(), None);
        }

        let inner = cache.lock_inner();
        assert!(inner.files.len() <= MAX_FILES);
        assert_eq!(inner.file_bytes, 0);
        assert!(!inner.files.contains_key("zero-0"));
        assert!(inner.files.contains_key("zero-19999"));
    }

    #[test]
    fn file_cache_cardinality_eviction_preserves_hot_and_recent_entries() {
        let cache = RemoteFuseCache::default();
        let expected = metadata("empty", 0);
        for index in 0..MAX_FILES {
            cache.put_file(
                &format!("entry-{index}"),
                Vec::new(),
                Some(expected.clone()),
            );
        }
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(
            cache.get_file_matching("entry-0", &expected),
            Some(Vec::new())
        );
        cache.put_file("overflow", Vec::new(), Some(expected.clone()));

        assert_eq!(
            cache.get_file_matching("entry-0", &expected),
            Some(Vec::new())
        );
        assert!(cache.get_file_matching("entry-1", &expected).is_none());
        assert_eq!(
            cache.get_file_matching("overflow", &expected),
            Some(Vec::new())
        );
        assert!(cache.lock_inner().files.len() <= MAX_FILES);
    }

    #[test]
    fn file_cache_byte_eviction_uses_the_same_lru() {
        let now = Instant::now();
        let mut inner = CacheState::default();
        for (index, path) in ["old", "middle", "hot"].into_iter().enumerate() {
            let file_metadata = metadata(path, 40);
            inner.file_bytes += 40;
            inner.files.insert(
                path.to_string(),
                CachedFile {
                    bytes: vec![index as u8; 40],
                    metadata: Some(file_metadata.clone()),
                    expires_at: now + Duration::from_secs(1),
                    last_access: now + Duration::from_millis(index as u64),
                },
            );
        }

        enforce_file_limits_locked(&mut inner, now, 10, 100);

        assert_eq!(inner.file_bytes, 40);
        assert_eq!(inner.files.len(), 1);
        assert!(inner.files.contains_key("hot"));
    }

    #[test]
    fn identity_invalidation_drops_every_alias_and_parent_listing() {
        let cache = RemoteFuseCache::default();
        let mut shared = metadata("hash", 4);
        shared.file_id = Some("inode-1".to_string());
        shared.link_count = 2;
        cache.put_dir("left", vec![entry("a")], 17);
        cache.put_dir("right", vec![entry("b")], 17);
        cache.put_file("left/a", b"body".to_vec(), Some(shared.clone()));
        cache.put_file("right/b", b"body".to_vec(), Some(shared.clone()));

        cache.invalidate_identity("inode-1");

        assert!(cache.get_file_matching("left/a", &shared).is_none());
        assert!(cache.get_file_matching("right/b", &shared).is_none());
        assert!(cache.get_dir("left", 17).is_none());
        assert!(cache.get_dir("right", 17).is_none());
        assert!(cache.aliases_for_identity("inode-1").is_empty());
    }

    #[test]
    fn replacing_content_cache_entry_prunes_old_identity_reverse_index() {
        let cache = RemoteFuseCache::default();
        let mut first = metadata("first", 1);
        first.file_id = Some("inode-first".to_string());
        let mut second = metadata("second", 1);
        second.file_id = Some("inode-second".to_string());
        cache.put_file("path", b"a".to_vec(), Some(first));
        cache.put_file("path", b"b".to_vec(), Some(second));

        assert!(cache.aliases_for_identity("inode-first").is_empty());
        assert_eq!(
            cache.aliases_for_identity("inode-second"),
            vec!["path".to_string()]
        );
    }

    #[test]
    fn stale_directory_response_cannot_repopulate_after_invalidation() {
        let cache = RemoteFuseCache::default();
        let generation = cache.directory_generation("tree");
        cache.invalidate("tree/new");
        assert!(!cache.put_dir_if_generation("tree", generation, vec![entry("stale")], 17));
        assert!(cache.get_dir("tree", 17).is_none());

        let current = cache.directory_generation("tree");
        assert!(cache.put_dir_if_generation("tree", current, vec![entry("current")], 17));
        assert_eq!(cache.get_dir("tree", 17), Some(vec![entry("current")]));
    }
}
