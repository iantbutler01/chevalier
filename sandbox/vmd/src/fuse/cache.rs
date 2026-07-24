use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

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
/// Kernel revocations the invalidation worker may have outstanding before the
/// queue refuses new work.
///
/// The queue exists to keep the notifier's blocking `writev` off any thread a
/// FUSE op waits on, not to buffer unbounded history: a saturated queue means
/// the guest kernel is absorbing revocations slower than this process publishes,
/// and the honest answer at that point is a single full sweep (a strict superset
/// of everything dropped) rather than an ever-growing backlog of exact sets.
const KERNEL_REVOCATION_QUEUE_DEPTH: usize = 256;

/// The exact set of entries one locally observed publication superseded,
/// returned by the `observe_*` publication hooks so the caller can mirror the
/// eviction into each sibling mount's *kernel* attribute/entry cache. The FUSE
/// layer hands the kernel positive attr/entry leases only while the revision
/// watch is live (see `fs::ATTR_ENTRY_LEASE_TTL`); those leases stay coherent
/// because a *remote* publication revokes exactly this set in the kernel before
/// the watch acks it (the revocation-ack ordering invariant), and a *local*
/// publication's own projection answers reads of the paths it just published
/// while its revocation drains on the invalidation worker.
///
/// Every revocation is applied by that worker (see [`MountInvalidators`]), never
/// inline on a thread a FUSE op is waiting on: `notify_inval_entry` blocks in the
/// guest kernel on the parent inode's lock, which the in-flight op holds.
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

/// One queued kernel revocation's target set.
enum RevocationRequest {
    /// Revoke exactly the entries one publication superseded.
    Targeted(PublicationInvalidation),
    /// Revoke every entry each mount handed its kernel, for a cross-process
    /// publication whose exact path set this process never learned.
    Full,
}

/// One unit of work on the kernel-invalidation queue.
struct QueuedRevocation {
    request: RevocationRequest,
    /// Present when the enqueuer must learn whether this revocation landed
    /// before it proceeds (the revision watch, whose ack may not precede the
    /// revocation). Absent for fire-and-forget enqueues — this mount's own
    /// commit hooks, which must never wait on the worker.
    completion: Option<oneshot::Sender<bool>>,
}

/// Handle onto one enqueued revocation, resolving to whether it landed.
///
/// Awaiting it is how a caller keeps the revocation-ack ordering invariant
/// without running the notifier itself: the wait happens on the caller's own
/// task, while the blocking notifier calls happen on the invalidation worker.
pub(super) struct RevocationTicket {
    receiver: Option<oneshot::Receiver<bool>>,
    /// Outcome when the request never reached the worker: `true` when there was
    /// nothing to revoke, `false` when the queue refused it (saturated, or the
    /// worker is shutting down) — which the caller must treat exactly like a
    /// failed revocation.
    resolved: bool,
}

impl RevocationTicket {
    fn resolved(landed: bool) -> Self {
        Self {
            receiver: None,
            resolved: landed,
        }
    }

    fn pending(receiver: oneshot::Receiver<bool>) -> Self {
        Self {
            receiver: Some(receiver),
            resolved: false,
        }
    }

    /// Whether every mount's revocation for this request landed. Resolves only
    /// once the worker has actually applied (or refused) the queued item.
    pub(super) async fn landed(self) -> bool {
        match self.receiver {
            // A dropped sender means the worker shut down without applying this
            // revocation. Fail closed: the kernel may still hold a superseded
            // lease, so the caller must not report itself coherent.
            Some(receiver) => receiver.await.unwrap_or(false),
            None => self.resolved,
        }
    }
}

#[derive(Default)]
struct RevocationQueueState {
    queued: VecDeque<QueuedRevocation>,
    /// A fire-and-forget revocation was dropped because the queue was saturated.
    /// The worker escalates to one full sweep — a strict superset of whatever
    /// was dropped — rather than leaving a superseded lease in some kernel.
    overflowed: bool,
    closed: bool,
}

/// Bounded hand-off from the publication paths to the invalidation worker.
#[derive(Default)]
struct RevocationQueue {
    state: Mutex<RevocationQueueState>,
    changed: Condvar,
}

impl RevocationQueue {
    /// Hand a revocation to the worker. Returns whether the worker now owns it;
    /// `false` means the caller's request was refused (bounded queue saturated,
    /// or shutting down) and any ordering guarantee it needed is unmet.
    fn push(&self, request: RevocationRequest, completion: Option<oneshot::Sender<bool>>) -> bool {
        let mut state = self.lock();
        if state.closed {
            return false;
        }
        if state.queued.len() >= KERNEL_REVOCATION_QUEUE_DEPTH {
            if completion.is_some() {
                // A tracked enqueue's caller fails closed on `false`; never grow
                // the queue past its bound to accommodate it.
                return false;
            }
            state.overflowed = true;
        } else {
            state.queued.push_back(QueuedRevocation {
                request,
                completion,
            });
        }
        drop(state);
        self.changed.notify_one();
        true
    }

    fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        drop(state);
        self.changed.notify_all();
    }

    /// Block until there is work, returning the whole queued batch in FIFO order
    /// plus whether an overflow full sweep is owed. `None` once the queue is
    /// closed: still-queued completions are dropped there, so their waiters
    /// resolve to "did not land" instead of parking forever.
    fn take(&self) -> Option<(Vec<QueuedRevocation>, bool)> {
        let mut state = self.lock();
        loop {
            if state.closed {
                state.queued.clear();
                return None;
            }
            if !state.queued.is_empty() || state.overflowed {
                let overflowed = std::mem::take(&mut state.overflowed);
                let batch = state.queued.drain(..).collect::<Vec<_>>();
                return Some((batch, overflowed));
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn lock(&self) -> MutexGuard<'_, RevocationQueueState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The live mount hooks, split out of [`MountInvalidators`] so the worker thread
/// can hold them without keeping the registry — and therefore its own join
/// handle — alive.
#[derive(Default)]
struct InvalidatorRegistry {
    inner: Mutex<Vec<Weak<dyn KernelInvalidator>>>,
}

impl InvalidatorRegistry {
    fn register(&self, invalidator: Weak<dyn KernelInvalidator>) {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.retain(|existing| existing.strong_count() > 0);
        guard.push(invalidator);
    }

    /// Returns whether every mount's revocation landed.
    fn invalidate(&self, invalidation: &PublicationInvalidation) -> bool {
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
    fn invalidate_all(&self) -> bool {
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

/// Drains the revocation queue on its own OS thread.
///
/// Owning a thread (rather than a tokio task) is the point: every notifier call
/// blocks — `fuse_reverse_inval_entry` waits on the guest kernel's parent-inode
/// lock, which an in-flight FUSE op holds for the duration of that op. Running
/// that on the publishing thread deadlocks (the op waits for the publication,
/// the publication waits for the revocation, the revocation waits for the op's
/// lock); running it on a tokio worker steals a runtime thread from the very
/// requests that must complete to release the lock. This thread holds no journal
/// state, no FUSE lock, and nothing waits on it except the revision watch, which
/// holds neither.
fn run_revocation_worker(queue: Arc<RevocationQueue>, registry: Arc<InvalidatorRegistry>) {
    while let Some((batch, overflowed)) = queue.take() {
        if overflowed && !registry.invalidate_all() {
            // The escalated sweep is itself bounded and may decline
            // (`KERNEL_SWEEP_MAX_TARGETS`); replies stay strict until the
            // affected leases expire on their own.
            tracing::warn!(
                "vfs kernel-revocation queue overflowed and the catch-up sweep did not land"
            );
        }
        for item in batch {
            let landed = match &item.request {
                RevocationRequest::Targeted(invalidation) => registry.invalidate(invalidation),
                RevocationRequest::Full => registry.invalidate_all(),
            };
            if let Some(completion) = item.completion {
                let _ = completion.send(landed);
            }
        }
    }
}

/// Per-registry set of live mount kernel-invalidation hooks plus the worker that
/// applies their revocations, keyed by the same coherence key as the shared
/// cache. A local publication's commit hook and the revision watch both enqueue
/// over every mount of the registry in this process, so a sibling observer's
/// kernel is revoked alongside the shared cache (a same-process observer has no
/// other notification channel: the single shared watch loop only acks
/// cross-process publications through the gateway).
///
/// Nothing here ever calls a notifier on the caller's thread. Callers either
/// enqueue and return ([`MountInvalidators::enqueue_revocation`]) or enqueue and
/// await the drain ([`RevocationTicket::landed`]).
pub(super) struct MountInvalidators {
    registry: Arc<InvalidatorRegistry>,
    queue: Arc<RevocationQueue>,
    worker: Option<JoinHandle<()>>,
}

impl Default for MountInvalidators {
    fn default() -> Self {
        let registry = Arc::new(InvalidatorRegistry::default());
        let queue = Arc::new(RevocationQueue::default());
        let worker_registry = Arc::clone(&registry);
        let worker_queue = Arc::clone(&queue);
        let worker = std::thread::Builder::new()
            .name("chevalier-vfs-kernel-revocation".to_string())
            .spawn(move || run_revocation_worker(worker_queue, worker_registry))
            .map_err(|error| {
                // Without the worker there is no safe way to revoke, so close the
                // queue: every enqueue then refuses, the watch withholds its acks
                // and serves strict, and no path silently skips a revocation.
                tracing::warn!(%error, "failed to spawn vfs kernel-revocation worker");
                queue.close();
            })
            .ok();
        Self {
            registry,
            queue,
            worker,
        }
    }
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
        self.registry.register(invalidator);
    }

    /// Queue a revocation and return immediately, without waiting for any
    /// notifier call. This is the only form a FUSE-op thread (a commit hook) may
    /// use — see [`run_revocation_worker`] for why waiting there deadlocks.
    pub(super) fn enqueue_revocation(&self, invalidation: &PublicationInvalidation) {
        if invalidation.is_empty() {
            return;
        }
        self.queue
            .push(RevocationRequest::Targeted(invalidation.clone()), None);
    }

    /// Queue a revocation whose completion the caller will await before it
    /// reports itself coherent (the revision watch's ack ordering).
    pub(super) fn enqueue_revocation_tracked(
        &self,
        invalidation: &PublicationInvalidation,
    ) -> RevocationTicket {
        if invalidation.is_empty() {
            return RevocationTicket::resolved(true);
        }
        let (sender, receiver) = oneshot::channel();
        if self.queue.push(
            RevocationRequest::Targeted(invalidation.clone()),
            Some(sender),
        ) {
            RevocationTicket::pending(receiver)
        } else {
            RevocationTicket::resolved(false)
        }
    }

    /// Queue an untargeted sweep whose completion the caller will await. The
    /// sweep still declines itself past `fs::KERNEL_SWEEP_MAX_TARGETS`, which
    /// resolves the ticket to `false` exactly as a failed revocation does.
    pub(super) fn enqueue_full_sweep_tracked(&self) -> RevocationTicket {
        let (sender, receiver) = oneshot::channel();
        if self.queue.push(RevocationRequest::Full, Some(sender)) {
            RevocationTicket::pending(receiver)
        } else {
            RevocationTicket::resolved(false)
        }
    }
}

impl Drop for MountInvalidators {
    fn drop(&mut self) {
        // Closing first makes the worker abandon queued work (waiters resolve to
        // "did not land") so the join below cannot outlast the revocation
        // already in flight. Process exit never joins this thread: the registry
        // outlives every mount and is only dropped when the last one unmounts.
        self.queue.close();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
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
    /// A publication with no snapshot to seed from — every affected entry is
    /// dropped rather than replaced. Retained for the tests that pin that
    /// fail-closed half of the contract.
    #[cfg(test)]
    pub(super) fn observe_namespace_publication(
        &self,
        revision: u64,
        mutations: &[VfsNamespaceMutation],
    ) -> PublicationInvalidation {
        self.observe_namespace_publication_snapshot(revision, mutations, &[])
    }

    pub(super) fn observe_namespace_publication_snapshot(
        &self,
        revision: u64,
        mutations: &[VfsNamespaceMutation],
        entries: &[VfsPublicationSnapshotEntry],
    ) -> PublicationInvalidation {
        let mut inner = self.lock_inner();
        let mut affected = AffectedSet::default();
        for mutation in mutations {
            let affects_descendants = matches!(
                mutation,
                VfsNamespaceMutation::RemoveDirectory { .. } | VfsNamespaceMutation::Rename { .. }
            );
            for path in mutation.paths().into_iter().filter(|path| !path.is_empty()) {
                // A namespace mutation changes the parent's OWN metadata too
                // (an added or removed link), so the parent is superseded, not
                // merely relisted. The gateway's publication snapshot carries
                // the parent for exactly this reason (`namespace_snapshot_paths`
                // in crates/sandbox/src/vfs.rs), so the install below puts the
                // authoritative replacement back at this revision.
                collect_affected_path(
                    &inner,
                    path,
                    affects_descendants,
                    ParentEffect::Superseded,
                    &mut affected,
                );
            }
        }
        advance_known_revision_locked(&mut inner, revision, &affected);
        // Replace, do not merely drop: this mount produced the publication, so
        // the gateway answered it with the resulting state of everything it
        // touched. Seeding inside the same critical section means no reader ever
        // observes the transient hole between the eviction and the replacement
        // and turns it into a wire round trip.
        install_publication_snapshot_locked(&mut inner, revision, entries);
        publication_invalidation(affected)
    }

    /// Content publication changes the named inode and every cached hard-link
    /// alias of its stable identity, but not unrelated namespace entries.
    #[cfg(test)]
    pub(super) fn observe_write_publication(
        &self,
        revision: u64,
        writes: &[(String, Option<String>)],
    ) -> PublicationInvalidation {
        self.observe_write_publication_snapshot(revision, writes, &[])
    }

    pub(super) fn observe_write_publication_snapshot(
        &self,
        revision: u64,
        writes: &[(String, Option<String>)],
        entries: &[VfsPublicationSnapshotEntry],
    ) -> PublicationInvalidation {
        let mut inner = self.lock_inner();
        let mut affected = AffectedSet::default();
        for (path, expected_file_id) in writes {
            // A content write changes the file, not the directory holding it:
            // the parent's kind, identity, link count and mode are untouched.
            // What it does change is the parent's cached LISTING, whose entries
            // carry each child's size and content hash — and that listing is
            // already dropped by the child's own invalidation below.
            //
            // Treating the parent as superseded metadata instead is what made a
            // 1,000-file create storm: `write-many`'s publication snapshot names
            // only the written paths (`post_write_many` in
            // crates/sandbox/src/vfs.rs), so there is nothing to seed the parent
            // back with, and every create re-stat'd the directory it had just
            // written into. The parent is still revoked in the guest kernel (see
            // `PublicationInvalidation`), so the guest re-asks — and this mount
            // answers from the metadata it never had reason to drop.
            collect_affected_path(&inner, path, false, ParentEffect::Relisted, &mut affected);
            if let Some(file_id) = expected_file_id {
                affected.identities.insert(file_id.clone());
            }
        }
        advance_known_revision_locked(&mut inner, revision, &affected);
        install_publication_snapshot_locked(&mut inner, revision, entries);
        publication_invalidation(affected)
    }

    /// Advance the cache across one publication observed on the revision watch,
    /// whose exact affected set the gateway reported.
    ///
    /// `since` is the fence the watch polled from, so the reported set is the
    /// union of every publication in `(since, revision]`. That set can only be
    /// applied to entries this cache has already classified up to `since`;
    /// if the cache's own fence is older, publications between the two are
    /// unaccounted for and the blunt authoritative-revision clear is the only
    /// honest answer.
    ///
    /// Unlike a locally originated publication there is no snapshot to seed
    /// with, so every reported path is fully invalidated — including its parent,
    /// whose link metadata a remote create/delete may have changed, and its
    /// descendants, since the watch does not distinguish a subtree mutation from
    /// a point one.
    pub(super) fn observe_remote_publication(&self, since: u64, revision: u64, paths: &[String]) {
        if revision == 0 {
            self.observe_authoritative_revision(revision);
            return;
        }
        let mut inner = self.lock_inner();
        if since > inner.metadata_revision {
            observe_authoritative_revision_locked(&mut inner, revision);
            return;
        }
        let mut affected = AffectedSet::default();
        for path in paths.iter().filter(|path| !path.is_empty()) {
            collect_affected_path(&inner, path, true, ParentEffect::Superseded, &mut affected);
        }
        advance_known_revision_locked(&mut inner, revision, &affected);
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

/// What one publication did to the directory holding a changed entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParentEffect {
    /// The parent's own metadata changed — a link was added or removed. Its
    /// cached metadata is superseded and must not be served again until it is
    /// replaced by the publication's snapshot or re-read.
    Superseded,
    /// Only the parent's cached listing changed (its entries carry each child's
    /// size and content hash). The directory's own kind, identity, link count
    /// and mode are what they were, so its cached metadata stays serveable —
    /// the listing itself is dropped by the changed child's own invalidation.
    Relisted,
}

/// The exact sets one publication superseded, accumulated per changed path.
#[derive(Default)]
struct AffectedSet {
    /// Entries whose OWN metadata the publication superseded.
    paths: HashSet<String>,
    /// Directories the guest kernel must re-read but whose own metadata the
    /// publication did not change. Kernel-revocation targets only; never a
    /// cache-eviction reason.
    relisted: HashSet<String>,
    /// Directory prefixes whose entire subtree the publication superseded.
    subtrees: HashSet<String>,
    /// Stable identities (hard-link inodes) the publication changed.
    identities: HashSet<String>,
}

fn collect_affected_path(
    inner: &CacheState,
    path: &str,
    affects_descendants: bool,
    parent_effect: ParentEffect,
    affected: &mut AffectedSet,
) {
    let path = path.trim_matches('/').to_string();
    affected.paths.insert(path.clone());
    if affects_descendants {
        affected.subtrees.insert(path.clone());
    }
    if let Some(parent) = parent_path(path.as_str()) {
        match parent_effect {
            ParentEffect::Superseded => {
                affected.paths.insert(parent);
            }
            ParentEffect::Relisted => {
                affected.relisted.insert(parent);
            }
        }
    }
    if let Some(file_id) = inner
        .metadata
        .get(path.as_str())
        .and_then(|entry| entry.metadata.file_id.clone())
    {
        affected.identities.insert(file_id);
    }
}

/// Collect the affected sets computed for one publication into the
/// invalidation record handed back to the kernel-invalidation fan-out.
///
/// The kernel set is the union of superseded and relisted paths, which is
/// exactly what it has always been: a directory whose listing changed must
/// still drop its cached dentry and page cache in every guest, even when this
/// process can still answer a stat of it from metadata the publication did not
/// touch. Narrowing the CACHE eviction never narrows the kernel revocation.
fn publication_invalidation(affected: AffectedSet) -> PublicationInvalidation {
    let mut paths = affected.paths;
    paths.extend(affected.relisted);
    PublicationInvalidation {
        paths: paths.into_iter().collect(),
        subtrees: affected.subtrees.into_iter().collect(),
        identities: affected.identities.into_iter().collect(),
    }
}

fn advance_known_revision_locked(inner: &mut CacheState, revision: u64, affected: &AffectedSet) {
    if revision == 0 {
        inner.metadata_revision = 0;
        inner.metadata.clear();
        inner.missing_metadata.clear();
        inner.directories.clear();
        inner.subtree_revisions.clear();
        return;
    }
    let previous_revision = inner.metadata_revision;
    // An entry already tagged at or beyond this publication's revision has
    // already incorporated it: the gateway sequences a publication before it
    // serves any response carrying that revision, and nothing enters this cache
    // except authoritative data tagged with the exact revision of the response
    // that produced it. Re-evicting such an entry would throw away state that is
    // newer than the publication being applied — which is exactly what happened
    // when a mount's own publication was reported back to it on the revision
    // watch after its commit hook had already installed the fresh snapshot.
    let superseded = |path: &String, entry_revision: u64, file_id: Option<&str>| {
        entry_revision < revision && path_or_identity_is_affected(path, file_id, affected)
    };
    let stale_paths = inner
        .metadata
        .iter_mut()
        .filter_map(|(path, entry)| {
            if superseded(path, entry.revision, entry.metadata.file_id.as_deref()) {
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
            if superseded(path, *entry_revision, None)
                || previous_revision == 0
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
            if superseded(path, entry.revision, None)
                || previous_revision == 0
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
                affected,
            )
            .then(|| path.clone())
        })
        .collect::<Vec<_>>();
    for path in affected_files {
        remove_file_locked(inner, path.as_str());
    }
    inner.metadata_revision = inner.metadata_revision.max(revision);
    // A subtree snapshot is only usable while the fence it was taken at holds,
    // and this publication moved the fence. Retire the per-prefix bookkeeping —
    // the entries it installed remain individually fenced and were retagged
    // above, so the snapshot's value is preserved without claiming the prefix is
    // still wholly covered.
    inner.subtree_revisions.clear();
}

fn install_publication_snapshot_locked(
    inner: &mut CacheState,
    revision: u64,
    entries: &[VfsPublicationSnapshotEntry],
) {
    if revision == 0 || inner.metadata_revision != revision {
        return;
    }
    for entry in entries {
        let path = entry.path.trim_matches('/');
        match entry.metadata.clone() {
            Some(metadata) => {
                let metadata = inner
                    .metadata
                    .get(path)
                    .filter(|cached| cached.revision == revision)
                    .map(|cached| preserve_stronger_metadata(&cached.metadata, metadata.clone()))
                    .unwrap_or(metadata);
                inner
                    .metadata
                    .insert(path.to_string(), CachedMetadata { metadata, revision });
                inner.missing_metadata.remove(path);
            }
            None => {
                inner.metadata.remove(path);
                remove_file_locked(inner, path);
                inner.missing_metadata.insert(path.to_string(), revision);
            }
        }
    }
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

/// Whether one cached entry is among what a publication superseded.
///
/// `relisted` parents are deliberately not consulted: their listing is dropped
/// through the changed child's own `invalidate_path_locked`, and their metadata
/// was not superseded at all.
fn path_or_identity_is_affected(
    path: &str,
    file_id: Option<&str>,
    affected: &AffectedSet,
) -> bool {
    affected.paths.contains(path)
        || affected.subtrees.iter().any(|affected_path| {
            !affected_path.is_empty()
                && path
                    .strip_prefix(affected_path)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
        || file_id.is_some_and(|file_id| affected.identities.contains(file_id))
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use super::{
        CacheState, CachedFile, KernelInvalidator, MAX_FILES, MountInvalidators,
        PublicationInvalidation, RemoteFuseCache, SUBTREE_LOAD_MISS_THRESHOLD,
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

    fn directory_metadata() -> RemoteMetadata {
        RemoteMetadata {
            kind: "directory".to_string(),
            size_bytes: 0,
            file_id: None,
            link_count: 1,
            link_target: None,
            content_hash: None,
            executable: false,
            mode: Some(0o755),
            updated_at: None,
        }
    }

    /// A content publication this mount originated must leave the directory it
    /// wrote into serveable. `write-many`'s snapshot names only the written
    /// paths, so evicting the parent leaves nothing to restore it and every
    /// create in a loop re-stats a directory this mount never changed.
    #[test]
    fn local_write_publication_leaves_the_parent_directory_serveable() {
        let cache = RemoteFuseCache::default();
        cache.put_metadata("tree", directory_metadata(), 17);
        cache.put_metadata("tree/file", metadata("old", 3), 17);
        let written = metadata("new", 4);

        let invalidation = cache.observe_write_publication_snapshot(
            18,
            &[("tree/file".to_string(), None)],
            &[VfsPublicationSnapshotEntry {
                path: "tree/file".to_string(),
                metadata: Some(written.clone()),
            }],
        );

        assert_eq!(cache.get_metadata("tree/file", 18), Some(written));
        assert_eq!(
            cache.get_metadata("tree", 18),
            Some(directory_metadata()),
            "a content write does not change its parent directory's own metadata"
        );
        // Narrowing the CACHE eviction must not narrow the KERNEL revocation:
        // the guest still has to drop the directory's dentry and page cache,
        // because its listing carries the child's size and content hash.
        let mut paths = invalidation.paths.clone();
        paths.sort();
        assert_eq!(paths, vec!["tree".to_string(), "tree/file".to_string()]);
    }

    /// The other half of the same rule: a NAMESPACE publication does change the
    /// parent (a link appeared), so the parent is superseded — and stays
    /// invalidated whenever the publication's snapshot did not carry it.
    #[test]
    fn local_namespace_publication_replaces_a_carried_parent_and_drops_an_uncarried_one() {
        let carried = RemoteFuseCache::default();
        carried.put_metadata("tree", directory_metadata(), 17);
        let mut relinked = directory_metadata();
        relinked.link_count = 2;

        carried.observe_namespace_publication_snapshot(
            18,
            &[VfsNamespaceMutation::CreateFile {
                path: "tree/new".to_string(),
                mode: Some(0o644),
            }],
            &[
                VfsPublicationSnapshotEntry {
                    path: "tree/new".to_string(),
                    metadata: Some(metadata("new", 4)),
                },
                VfsPublicationSnapshotEntry {
                    path: "tree".to_string(),
                    metadata: Some(relinked.clone()),
                },
            ],
        );
        assert_eq!(
            carried.get_metadata("tree", 18),
            Some(relinked),
            "a carried parent is REPLACED with the publication's own state, not dropped"
        );

        let uncarried = RemoteFuseCache::default();
        uncarried.put_metadata("tree", directory_metadata(), 17);
        uncarried.observe_namespace_publication_snapshot(
            18,
            &[VfsNamespaceMutation::CreateFile {
                path: "tree/new".to_string(),
                mode: Some(0o644),
            }],
            &[VfsPublicationSnapshotEntry {
                path: "tree/new".to_string(),
                metadata: Some(metadata("new", 4)),
            }],
        );
        assert!(
            uncarried.get_metadata("tree", 18).is_none(),
            "freshness unknown means fail closed: never serve what the publication may have superseded"
        );
    }

    /// A publication observed on the revision watch carries the gateway's exact
    /// affected set, and the cache must apply that set rather than clearing
    /// itself. Clearing is what made a warm `git status` cost MORE than a cold
    /// one: git rewrites its index mid-scan, and every such publication —
    /// including this mount's own, which comes back on its own watch — wiped
    /// every unrelated path the scan had just read.
    #[test]
    fn remote_publication_applies_its_reported_set_instead_of_clearing() {
        let cache = RemoteFuseCache::default();
        cache.put_metadata("tree/scanned", metadata("scanned", 6), 17);
        cache.put_metadata("index", metadata("old-index", 3), 17);
        cache.put_dir("tree", vec![entry("scanned")], 17);

        cache.observe_remote_publication(17, 18, &["index".to_string()]);

        assert!(
            cache.get_metadata("index", 18).is_none(),
            "the reported path is superseded"
        );
        assert_eq!(
            cache.get_metadata("tree/scanned", 18),
            Some(metadata("scanned", 6)),
            "a path the publication did not touch stays serveable at the new fence"
        );
        assert_eq!(cache.get_dir("tree", 18), Some(vec![entry("scanned")]));
    }

    /// Fail-closed, unchanged, on both edges: a watcher whose own fence is older
    /// than the window the report covers, and a report the gateway could not
    /// complete (which reaches the cache as the blunt authoritative clear).
    #[test]
    fn remote_publication_falls_back_to_a_full_clear_when_the_report_cannot_be_trusted() {
        let behind = RemoteFuseCache::default();
        behind.put_metadata("tree/scanned", metadata("scanned", 6), 17);
        // `since` ahead of the cache's own fence means publications in between
        // were never classified here, and the report does not cover them.
        behind.observe_remote_publication(18, 19, &["index".to_string()]);
        assert!(
            behind.get_metadata("tree/scanned", 19).is_none(),
            "an unclassified gap must clear rather than narrow"
        );

        let truncated = RemoteFuseCache::default();
        truncated.put_metadata("tree/scanned", metadata("scanned", 6), 17);
        // The truncated answer routes through `observe_authoritative_revision`,
        // exactly as it did before the targeted path existed.
        truncated.observe_authoritative_revision(18);
        assert!(truncated.get_metadata("tree/scanned", 18).is_none());
    }

    /// An entry already tagged at the publication's own revision has already
    /// incorporated it, so re-applying that publication must not throw it away.
    /// This is what lets a mount's own publication arrive twice — once on its
    /// watch, once through its commit hook — without the second arrival undoing
    /// the fresh snapshot the first installed.
    #[test]
    fn a_publication_does_not_evict_entries_already_taken_at_its_own_revision() {
        let cache = RemoteFuseCache::default();
        cache.put_metadata("tree/file", metadata("old", 3), 17);
        let published = metadata("new", 4);
        cache.observe_write_publication_snapshot(
            18,
            &[("tree/file".to_string(), None)],
            &[VfsPublicationSnapshotEntry {
                path: "tree/file".to_string(),
                metadata: Some(published.clone()),
            }],
        );

        cache.observe_remote_publication(17, 18, &["tree/file".to_string()]);

        assert_eq!(
            cache.get_metadata("tree/file", 18),
            Some(published),
            "the same publication reported a second time must not evict its own result"
        );
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

    fn one_path(path: &str) -> PublicationInvalidation {
        PublicationInvalidation {
            paths: vec![path.to_string()],
            subtrees: Vec::new(),
            identities: Vec::new(),
        }
    }

    /// Records the order revocations are applied in, and parks inside the FIRST
    /// one it is handed (standing in for a notifier call blocked in the guest
    /// kernel) so everything enqueued after it piles up in the queue.
    struct OrderingInvalidator {
        applied: Arc<Mutex<Vec<String>>>,
        entered: mpsc::Sender<()>,
        gate: Arc<(Mutex<bool>, Condvar)>,
        parked: AtomicBool,
    }

    impl KernelInvalidator for OrderingInvalidator {
        fn invalidate(&self, invalidation: &PublicationInvalidation) -> bool {
            if !self.parked.swap(true, Ordering::AcqRel) {
                let _ = self.entered.send(());
                let (lock, condvar) = &*self.gate;
                let mut open = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                while !*open {
                    open = condvar
                        .wait(open)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
            self.applied
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(invalidation.paths.join(","));
            true
        }

        fn invalidate_all(&self) -> bool {
            self.applied
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("*".to_string());
            true
        }
    }

    /// The registry under test plus the handles its parked double exposes.
    /// Destructured by each test: the shutdown test has to move the registry
    /// Arc out (dropping it is what shuts the worker down) while still reading
    /// the recorded order.
    struct WorkerProbe {
        invalidators: Arc<MountInvalidators>,
        applied: Arc<Mutex<Vec<String>>>,
        entered: mpsc::Receiver<()>,
        gate: Arc<(Mutex<bool>, Condvar)>,
        registered: Arc<dyn KernelInvalidator>,
    }

    fn worker_probe() -> WorkerProbe {
        let invalidators = Arc::new(MountInvalidators::default());
        let applied = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (entered_tx, entered_rx) = mpsc::channel();
        let registered: Arc<dyn KernelInvalidator> = Arc::new(OrderingInvalidator {
            applied: Arc::clone(&applied),
            entered: entered_tx,
            gate: Arc::clone(&gate),
            parked: AtomicBool::new(false),
        });
        invalidators.register(Arc::downgrade(&registered));
        WorkerProbe {
            invalidators,
            applied,
            entered: entered_rx,
            gate,
            registered,
        }
    }

    fn recorded(applied: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
        applied
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn release(gate: &Arc<(Mutex<bool>, Condvar)>) {
        let (lock, condvar) = &**gate;
        *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        condvar.notify_all();
    }

    #[test]
    fn kernel_revocation_worker_drains_queued_batches_in_order() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let WorkerProbe {
            invalidators,
            applied,
            entered,
            gate,
            registered,
        } = worker_probe();

        // The first enqueue parks the worker inside the notifier double; the
        // rest queue up behind it and must be applied in enqueue order.
        invalidators.enqueue_revocation(&one_path("first"));
        entered
            .recv_timeout(Duration::from_secs(5))
            .expect("the worker must pick the first queued revocation up");
        invalidators.enqueue_revocation(&one_path("second"));
        invalidators.enqueue_revocation(&one_path("third"));
        let ticket = invalidators.enqueue_revocation_tracked(&one_path("fourth"));
        assert!(
            recorded(&applied).is_empty(),
            "nothing is applied while the worker is parked in the first revocation"
        );

        release(&gate);
        assert!(
            runtime.block_on(ticket.landed()),
            "a tracked revocation resolves once the worker has applied it"
        );
        assert_eq!(
            recorded(&applied),
            vec!["first", "second", "third", "fourth"],
            "queued revocations drain in FIFO order across batches"
        );
        drop(registered);
    }

    #[test]
    fn kernel_revocation_worker_shuts_down_cleanly_and_fails_closed_on_undrained_work() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let WorkerProbe {
            invalidators,
            applied,
            entered,
            gate,
            registered,
        } = worker_probe();
        invalidators.enqueue_revocation(&one_path("in-flight"));
        entered
            .recv_timeout(Duration::from_secs(5))
            .expect("the worker must pick the first queued revocation up");
        let ticket = invalidators.enqueue_revocation_tracked(&one_path("undrained"));

        // Dropping the last registry reference closes the queue and joins the
        // worker. Done on another thread so a shutdown that cannot complete
        // fails this test on a timeout instead of hanging the suite.
        let (shut_down_tx, shut_down_rx) = mpsc::channel();
        let dropper = std::thread::spawn(move || {
            drop(invalidators);
            shut_down_tx.send(()).unwrap();
        });
        assert!(
            shut_down_rx
                .recv_timeout(Duration::from_millis(200))
                .is_err(),
            "shutdown waits out the revocation already in flight rather than abandoning it"
        );

        release(&gate);
        shut_down_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the worker exits and the registry drop joins it");
        dropper.join().unwrap();
        assert_eq!(
            recorded(&applied),
            vec!["in-flight"],
            "work still queued at shutdown is abandoned, not applied"
        );
        assert!(
            !runtime.block_on(ticket.landed()),
            "an abandoned revocation resolves fail-closed so its caller never acks"
        );
        drop(registered);
    }
}
