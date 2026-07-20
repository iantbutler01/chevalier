use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use chevalier_sandbox::vfs::{
    VFS_OPERATION_RENAME, VFS_OPERATION_SETATTR_SIZE, VFS_OPERATION_WRITE_THROUGH,
    VFS_SURFACE_KIND_VM_SHARED, VFS_SURFACE_KIND_VM_WORKSPACE, VfsDirEntry as RemoteDirEntry,
    VfsLeaseGrant as LeaseGrant, VfsMetadata as RemoteMetadata, VfsNamespaceMutation,
    VfsWritePrecondition, scoped_vfs_path,
};
use fuser::{
    BsdFileFlags, Errno, FileAttr, FileHandle, FileType, FopenFlags, Generation, INodeNo,
    InitFlags, KernelConfig, LockNamespace, MountOption, OpenFlags, RenameFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen,
    ReplyWrite, TimeOrNow,
};
use sha2::{Digest, Sha256};
use tokio::runtime::Handle;
use uuid::Uuid;

use super::cache::RemoteFuseCache;
use super::client::{AdvisoryLockRenewalIdentity, RangeRead, RemoteVfsClient, request_status};
use super::namespace::NamespaceJournal;
use super::write::WriteJournal;

/// Kernel metadata and entry caching must stay disabled. Distinct VMs can
/// mount the same VFS scope through independent FUSE sessions, so there is no
/// kernel-to-kernel invalidation path that could make a positive TTL coherent.
const TTL: Duration = Duration::ZERO;
const ROOT_INO_RAW: u64 = 1;
const ROOT_INO: INodeNo = INodeNo(ROOT_INO_RAW);
const LARGE_FILE_BYTES: u64 = 10 * 1024 * 1024;
const MAX_OPEN_HANDLES: usize = 8_192;
const ADVISORY_LOCK_RETRY_DELAY: Duration = Duration::from_millis(50);
const ADVISORY_LOCK_BLOCK_TIMEOUT: Duration = Duration::from_secs(30);
const ADVISORY_LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const POSIX_MODE_MASK: u32 = 0o7777;

type FuseResult<T> = std::result::Result<T, Errno>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveAdvisoryLockFile {
    file_id: String,
    fh: u64,
}

type ActiveAdvisoryLocks = HashMap<(LockNamespace, String), HashMap<u64, ActiveAdvisoryLockFile>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LockWaitState {
    Pending,
    Cancelled,
    Completed,
}

pub(super) struct LockWaitCancellation {
    state: Mutex<LockWaitState>,
    wake: Condvar,
}

impl LockWaitCancellation {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(LockWaitState::Pending),
            wake: Condvar::new(),
        }
    }

    pub(super) fn cancel(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *state != LockWaitState::Pending {
            return false;
        }
        *state = LockWaitState::Cancelled;
        self.wake.notify_all();
        true
    }

    fn is_cancelled(&self) -> bool {
        *self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            == LockWaitState::Cancelled
    }

    fn wait_cancelled(&self, timeout: Duration) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *state == LockWaitState::Cancelled {
            return true;
        }
        let (state, _) = self
            .wake
            .wait_timeout_while(state, timeout, |state| *state == LockWaitState::Pending)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state == LockWaitState::Cancelled
    }

    fn finish(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *state != LockWaitState::Pending {
            return false;
        }
        *state = LockWaitState::Completed;
        true
    }
}

fn take_active_advisory_lock_file_id(
    active: &mut ActiveAdvisoryLocks,
    owner_key: &(LockNamespace, String),
    ino: u64,
) -> Option<String> {
    let (file_id, owner_is_empty) = match active.get_mut(owner_key) {
        Some(files) => {
            let file_id = files.remove(&ino).map(|file| file.file_id);
            (file_id, files.is_empty())
        }
        None => (None, false),
    };
    if owner_is_empty {
        active.remove(owner_key);
    }
    file_id
}

fn take_active_posix_handle_locks(
    active: &mut ActiveAdvisoryLocks,
    fh: u64,
    ino: u64,
) -> Vec<(String, String)> {
    let owners = active
        .iter()
        .filter_map(|((namespace, owner), files)| {
            (*namespace == LockNamespace::Posix
                && files.get(&ino).is_some_and(|file| file.fh == fh))
            .then(|| owner.clone())
        })
        .collect::<Vec<_>>();
    owners
        .into_iter()
        .filter_map(|owner| {
            let owner_key = (LockNamespace::Posix, owner.clone());
            take_active_advisory_lock_file_id(active, &owner_key, ino)
                .map(|file_id| (owner, file_id))
        })
        .collect()
}

fn active_advisory_lock_identities(
    active: &ActiveAdvisoryLocks,
) -> Vec<AdvisoryLockRenewalIdentity> {
    active
        .iter()
        .flat_map(|((namespace, lock_owner), files)| {
            files.values().map(move |file| {
                (
                    lock_owner.clone(),
                    match namespace {
                        LockNamespace::Posix => "posix".to_string(),
                        LockNamespace::Flock => "flock".to_string(),
                    },
                    file.file_id.clone(),
                )
            })
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(
            |(lock_owner, namespace, file_id)| AdvisoryLockRenewalIdentity {
                lock_owner,
                namespace,
                file_id,
            },
        )
        .collect()
}

fn combine_flush_and_lock_cleanup(
    operation: &'static str,
    flush_result: FuseResult<()>,
    cleanup_result: FuseResult<()>,
) -> FuseResult<()> {
    match (flush_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(primary), Err(cleanup)) => {
            tracing::warn!(
                operation,
                ?primary,
                ?cleanup,
                "VFS close writeback and advisory-lock cleanup both failed"
            );
            Err(primary)
        }
    }
}

#[derive(Default)]
struct InodeTable {
    next: u64,
    path_to_ino: BTreeMap<String, INodeNo>,
    identity_to_ino: HashMap<String, INodeNo>,
    ino_to_path: HashMap<INodeNo, InodeRecord>,
}

struct InodeRecord {
    path: String,
    paths: BTreeSet<String>,
    identity: Option<String>,
    last_access: Instant,
    lookup_count: u64,
}

impl InodeTable {
    fn new() -> Self {
        let mut table = Self {
            next: ROOT_INO_RAW + 1,
            path_to_ino: BTreeMap::new(),
            identity_to_ino: HashMap::new(),
            ino_to_path: HashMap::new(),
        };
        table.path_to_ino.insert(String::new(), ROOT_INO);
        table.ino_to_path.insert(
            ROOT_INO,
            InodeRecord {
                path: String::new(),
                paths: BTreeSet::from([String::new()]),
                identity: None,
                last_access: Instant::now(),
                lookup_count: u64::MAX,
            },
        );
        table
    }

    fn ensure(&mut self, path: &str) -> INodeNo {
        self.ensure_with_identity(path, None)
    }

    fn ensure_with_identity(&mut self, path: &str, identity: Option<&str>) -> INodeNo {
        if let Some(ino) = self.path_to_ino.get(path).copied() {
            if let Some(identity) = identity {
                let mapped = self.identity_to_ino.get(identity).copied();
                let record_identity = self
                    .ino_to_path
                    .get(&ino)
                    .and_then(|record| record.identity.as_deref());
                if record_identity.is_some_and(|current| current != identity)
                    || mapped.is_some_and(|mapped| mapped != ino)
                {
                    self.detach_exact(path);
                    return self.ensure_with_identity(path, Some(identity));
                }
            }
            if let Some(record) = self.ino_to_path.get_mut(&ino) {
                record.last_access = Instant::now();
                if record.identity.is_none() {
                    record.identity = identity.map(str::to_string);
                    if let Some(identity) = identity {
                        self.identity_to_ino.insert(identity.to_string(), ino);
                    }
                }
            }
            return ino;
        }
        if let Some(identity) = identity
            && let Some(ino) = self.identity_to_ino.get(identity).copied()
        {
            self.path_to_ino.insert(path.to_string(), ino);
            if let Some(record) = self.ino_to_path.get_mut(&ino) {
                record.paths.insert(path.to_string());
                if record.path.is_empty() {
                    record.path = path.to_string();
                }
                record.last_access = Instant::now();
            }
            return ino;
        }
        let ino = INodeNo(self.next);
        self.next += 1;
        self.path_to_ino.insert(path.to_string(), ino);
        if let Some(identity) = identity {
            self.identity_to_ino.insert(identity.to_string(), ino);
        }
        self.ino_to_path.insert(
            ino,
            InodeRecord {
                path: path.to_string(),
                paths: BTreeSet::from([path.to_string()]),
                identity: identity.map(str::to_string),
                last_access: Instant::now(),
                lookup_count: 0,
            },
        );
        ino
    }

    pub(super) fn lookup(&mut self, path: &str) -> INodeNo {
        self.lookup_with_identity(path, None)
    }

    fn lookup_with_identity(&mut self, path: &str, identity: Option<&str>) -> INodeNo {
        let ino = self.ensure_with_identity(path, identity);
        if let Some(record) = self.ino_to_path.get_mut(&ino) {
            record.lookup_count = record.lookup_count.saturating_add(1);
        }
        ino
    }

    fn path(&mut self, ino: INodeNo) -> Option<String> {
        let record = self.ino_to_path.get_mut(&ino)?;
        record.last_access = Instant::now();
        Some(record.path.clone())
    }

    fn route(&mut self, ino: INodeNo) -> Option<(String, Option<String>)> {
        let record = self.ino_to_path.get_mut(&ino)?;
        record.last_access = Instant::now();
        Some((record.path.clone(), record.identity.clone()))
    }

    fn retarget_identity(
        &mut self,
        ino: INodeNo,
        stale_path: &str,
        path: &str,
        identity: &str,
    ) -> bool {
        if self.identity_to_ino.get(identity).copied() != Some(ino)
            || self
                .ino_to_path
                .get(&ino)
                .and_then(|record| record.identity.as_deref())
                != Some(identity)
        {
            return false;
        }
        if let Some(other) = self.path_to_ino.get(path).copied()
            && other != ino
        {
            self.detach_exact(path);
        }
        if stale_path != path && self.path_to_ino.get(stale_path).copied() == Some(ino) {
            self.path_to_ino.remove(stale_path);
            if let Some(record) = self.ino_to_path.get_mut(&ino) {
                record.paths.remove(stale_path);
            }
        }
        self.path_to_ino.insert(path.to_string(), ino);
        let Some(record) = self.ino_to_path.get_mut(&ino) else {
            return false;
        };
        record.paths.insert(path.to_string());
        record.path = path.to_string();
        record.last_access = Instant::now();
        true
    }

    fn retarget_identity_path(&mut self, stale_path: &str, path: &str, identity: &str) {
        if let Some(ino) = self.identity_to_ino.get(identity).copied() {
            let _ = self.retarget_identity(ino, stale_path, path, identity);
        }
    }

    pub(super) fn forget(&mut self, ino: INodeNo, nlookup: u64) {
        if ino == ROOT_INO {
            return;
        }
        let Some(record) = self.ino_to_path.get_mut(&ino) else {
            return;
        };
        record.lookup_count = record.lookup_count.saturating_sub(nlookup);
        if record.lookup_count == 0 {
            let paths = record.paths.clone();
            let identity = record.identity.clone();
            self.ino_to_path.remove(&ino);
            for path in paths {
                if self.path_to_ino.get(path.as_str()) == Some(&ino) {
                    self.path_to_ino.remove(path.as_str());
                }
            }
            if let Some(identity) = identity
                && self.identity_to_ino.get(identity.as_str()) == Some(&ino)
            {
                self.identity_to_ino.remove(identity.as_str());
            }
        }
    }

    fn detach_exact(&mut self, path: &str) {
        let Some(ino) = self.path_to_ino.remove(path) else {
            return;
        };
        let mut remove_inode = false;
        if let Some(record) = self.ino_to_path.get_mut(&ino) {
            record.paths.remove(path);
            if record.path == path {
                let remaining = record.paths.iter().next().cloned();
                record.path = remaining.unwrap_or_else(|| {
                    if record.identity.is_some() && record.lookup_count > 0 {
                        // Keep a stale path only as a remote alias-search hint
                        // for a live stable inode. It is deliberately absent
                        // from path_to_ino, so path reuse binds a new inode.
                        path.to_string()
                    } else {
                        String::new()
                    }
                });
            }
            remove_inode =
                record.paths.is_empty() && !(record.identity.is_some() && record.lookup_count > 0);
        }
        if remove_inode {
            if let Some(record) = self.ino_to_path.remove(&ino)
                && let Some(identity) = record.identity
                && self.identity_to_ino.get(identity.as_str()) == Some(&ino)
            {
                self.identity_to_ino.remove(identity.as_str());
            }
        }
    }

    fn subtree_entries(&self, path: &str) -> Vec<(String, INodeNo)> {
        let prefix = format!("{path}/");
        let mut entries = self
            .path_to_ino
            .get(path)
            .map(|ino| vec![(path.to_string(), *ino)])
            .unwrap_or_default();
        entries.extend(
            self.path_to_ino
                .range(prefix.clone()..)
                .take_while(|(candidate, _)| candidate.starts_with(&prefix))
                .map(|(candidate, ino)| (candidate.clone(), *ino)),
        );
        entries
    }

    fn detach_subtree(&mut self, path: &str) {
        let mut emptied = Vec::new();
        for (candidate, ino) in self.subtree_entries(path) {
            self.path_to_ino.remove(candidate.as_str());
            if let Some(record) = self.ino_to_path.get_mut(&ino) {
                record.paths.remove(candidate.as_str());
                if record.path == candidate {
                    let remaining = record.paths.iter().next().cloned();
                    record.path = remaining.unwrap_or_else(|| {
                        if record.identity.is_some() && record.lookup_count > 0 {
                            candidate.clone()
                        } else {
                            String::new()
                        }
                    });
                }
                if record.paths.is_empty()
                    && !(record.identity.is_some() && record.lookup_count > 0)
                {
                    emptied.push(ino);
                }
            }
        }
        for ino in emptied {
            if let Some(record) = self.ino_to_path.remove(&ino)
                && let Some(identity) = record.identity
                && self.identity_to_ino.get(identity.as_str()) == Some(&ino)
            {
                self.identity_to_ino.remove(identity.as_str());
            }
        }
    }

    fn rename_path(&mut self, from: &str, to: &str) {
        self.detach_subtree(to);
        for (old_path, ino) in self.subtree_entries(from) {
            let suffix = old_path.strip_prefix(from).unwrap_or_default();
            let new_path = format!("{to}{suffix}");
            self.path_to_ino.remove(old_path.as_str());
            self.path_to_ino.insert(new_path.clone(), ino);
            if let Some(record) = self.ino_to_path.get_mut(&ino) {
                record.paths.remove(old_path.as_str());
                record.paths.insert(new_path.clone());
                if record.path == old_path {
                    record.path = new_path;
                }
                record.last_access = Instant::now();
            }
        }
    }

    fn aliases_for_path(&self, path: &str) -> Vec<String> {
        self.path_to_ino
            .get(path)
            .and_then(|ino| self.ino_to_path.get(ino))
            .map(|record| record.paths.iter().cloned().collect())
            .unwrap_or_else(|| vec![path.to_string()])
    }
}

struct HandleTable {
    next: u64,
    files: HashMap<u64, FileState>,
}

impl Default for HandleTable {
    fn default() -> Self {
        Self {
            next: 1,
            files: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct FileState {
    path: String,
    file_id: Option<String>,
    link_count: u64,
    /// The pathname has been removed and no remaining alias can publish this
    /// open inode. Its buffer remains usable until final release, but must
    /// never be flushed back into the deleted namespace.
    unlinked: bool,
    buffer: Vec<u8>,
    /// Permission and special bits, excluding the file-type bits carried
    /// separately by FUSE.
    mode: u32,
    /// Exact mode last known to exist at the gateway. New files have no
    /// baseline until their first mode-carrying write-through publishes them.
    base_mode: Option<u32>,
    /// The first if-absent write was acknowledged, but authoritative identity
    /// binding has not completed yet. A retry must stat, never create again.
    publication_acknowledged: bool,
    /// Exact ordered-write publication currently owned by this handle.
    /// Retained across a failed fsync/close so a retry waits for the original
    /// WAL entry instead of enqueueing a duplicate with ambiguous attribution.
    pending_publication: Option<HandlePublication>,
    created: bool,
    dirty: bool,
    loaded: bool,
    base_content_hash: Option<String>,
    /// Linearizes writeback, final release, and pathname transitions for this
    /// open handle. FUSE may issue duplicate FLUSH requests and concurrent
    /// RELEASE; clones must share the same gate.
    publication_gate: Arc<Mutex<()>>,
    revision: u64,
}

#[derive(Clone)]
struct HandlePublication {
    id: u64,
    revision: u64,
    path: String,
    file_id: Option<String>,
    size_bytes: u64,
    content_hash: String,
    mode: u32,
}

#[derive(Clone)]
struct LinkedFileRoute {
    path: String,
    metadata: RemoteMetadata,
}

enum StableFileRoute {
    Linked(LinkedFileRoute),
    Unlinked,
}

struct AdvisoryLockTarget {
    path: String,
    file_id: String,
}

pub struct RemoteFuseFs {
    client: RemoteVfsClient,
    cache: std::sync::Arc<RemoteFuseCache>,
    inodes: Mutex<InodeTable>,
    handles: Mutex<HandleTable>,
    namespace: Option<NamespaceJournal>,
    namespace_publication_gate: Mutex<()>,
    writes: Option<WriteJournal>,
    read_only: bool,
    scope_path: String,
    mount_id: String,
    active_lock_owners: std::sync::Arc<Mutex<ActiveAdvisoryLocks>>,
    tokio: Handle,
    uid: u32,
    gid: u32,
}

impl RemoteFuseFs {
    pub fn new(client: RemoteVfsClient, read_only: bool, scope_path: &str, tokio: Handle) -> Self {
        let (mount_id, active_lock_owners) = Self::start_lock_heartbeat(&client, &tokio);
        Self {
            client,
            cache: std::sync::Arc::new(RemoteFuseCache::default()),
            inodes: Mutex::new(InodeTable::new()),
            handles: Mutex::new(HandleTable::default()),
            namespace: None,
            namespace_publication_gate: Mutex::new(()),
            writes: None,
            read_only,
            scope_path: scope_path.trim_matches('/').to_string(),
            mount_id,
            active_lock_owners,
            tokio,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        }
    }

    pub fn new_with_namespace_journal(
        client: RemoteVfsClient,
        read_only: bool,
        scope_path: &str,
        journal_path: &Path,
        tokio: Handle,
    ) -> Result<Self> {
        let namespace = if read_only {
            None
        } else {
            Some(NamespaceJournal::open(
                client.clone(),
                scope_path,
                journal_path,
                tokio.clone(),
            )?)
        };
        let cache = std::sync::Arc::new(RemoteFuseCache::default());
        let writes = if read_only {
            None
        } else {
            let dead_letter_cache = std::sync::Arc::clone(&cache);
            Some(WriteJournal::open(
                client.clone(),
                scope_path,
                journal_path.with_extension("writes.jsonl").as_path(),
                tokio.clone(),
                // A dead-lettered write's content was installed into the read
                // cache at flush time; drop it so readers converge on the
                // gateway's authoritative content instead of the dropped bytes.
                Some(Box::new(move |path: &str| {
                    dead_letter_cache.invalidate(path)
                })),
            )?)
        };
        let (mount_id, active_lock_owners) = Self::start_lock_heartbeat(&client, &tokio);
        Ok(Self {
            client,
            cache,
            inodes: Mutex::new(InodeTable::new()),
            handles: Mutex::new(HandleTable::default()),
            namespace,
            namespace_publication_gate: Mutex::new(()),
            writes,
            read_only,
            scope_path: scope_path.trim_matches('/').to_string(),
            mount_id,
            active_lock_owners,
            tokio,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        })
    }

    fn start_lock_heartbeat(
        client: &RemoteVfsClient,
        tokio: &Handle,
    ) -> (String, std::sync::Arc<Mutex<ActiveAdvisoryLocks>>) {
        let mount_id = Uuid::new_v4().to_string();
        let active = std::sync::Arc::new(Mutex::new(HashMap::new()));
        let weak = std::sync::Arc::downgrade(&active);
        let heartbeat_client = client.clone();
        let heartbeat_mount_id = mount_id.clone();
        tokio.spawn(async move {
            let mut interval = tokio::time::interval(ADVISORY_LOCK_HEARTBEAT_INTERVAL);
            interval.tick().await;
            loop {
                interval.tick().await;
                let Some(active) = weak.upgrade() else {
                    break;
                };
                let identities = active
                    .lock()
                    .map(|owners| active_advisory_lock_identities(&owners))
                    .unwrap_or_default();
                if identities.is_empty() {
                    continue;
                }
                if let Err(error) = heartbeat_client
                    .renew_advisory_locks(&heartbeat_mount_id, &identities)
                    .await
                {
                    tracing::warn!(
                        mount_id = heartbeat_mount_id,
                        error = %error,
                        "failed to renew distributed VFS advisory locks"
                    );
                }
            }
        });
        (mount_id, active)
    }

    pub(super) fn tokio_handle(&self) -> Handle {
        self.tokio.clone()
    }

    pub fn mount_options(&self, tag: &str) -> fuser::Config {
        let mut mount_options = vec![
            MountOption::FSName(tag.to_string()),
            MountOption::Subtype("chevalier-vfs".to_string()),
            MountOption::DefaultPermissions,
        ];
        if self.read_only {
            mount_options.push(MountOption::RO);
        } else {
            mount_options.push(MountOption::RW);
        }
        let mut config = fuser::Config::default();
        config.mount_options = mount_options;
        config.n_threads = Some(4);
        config.clone_fd = true;
        config
    }

    fn requested_init_capabilities(&self) -> InitFlags {
        Self::requested_init_capabilities_for(self.read_only)
    }

    fn path_for_ino(&self, ino: INodeNo) -> FuseResult<String> {
        self.lock_inodes()?.path(ino).ok_or(Errno::ENOENT)
    }

    fn inode_route(&self, ino: INodeNo) -> FuseResult<(String, Option<String>)> {
        self.lock_inodes()?.route(ino).ok_or(Errno::ENOENT)
    }

    fn authoritative_file_route(&self, path: &str, file_id: &str) -> FuseResult<StableFileRoute> {
        let current = self
            .tokio
            .block_on(self.client.stat(path))
            .map_err(|error| {
                tracing::warn!(path, file_id, error = %error, "vfs identity stat failed");
                Errno::EIO
            })?;
        if current
            .as_ref()
            .and_then(|metadata| metadata.file_id.as_deref())
            == Some(file_id)
        {
            return Ok(StableFileRoute::Linked(LinkedFileRoute {
                path: path.to_string(),
                metadata: current.expect("matching metadata exists"),
            }));
        }
        let Some(alias) = self
            .tokio
            .block_on(self.client.find_hard_link_alias(file_id, path))
            .map_err(|error| {
                tracing::warn!(
                    path,
                    file_id,
                    error = %error,
                    "vfs hard-link alias lookup failed"
                );
                Errno::EIO
            })?
        else {
            return Ok(StableFileRoute::Unlinked);
        };
        let metadata = self
            .tokio
            .block_on(self.client.stat(&alias))
            .map_err(|error| {
                tracing::warn!(
                    path = alias,
                    file_id,
                    error = %error,
                    "vfs hard-link alias stat failed"
                );
                Errno::EIO
            })?
            .ok_or(Errno::EAGAIN)?;
        if metadata.file_id.as_deref() != Some(file_id) {
            return Err(Errno::EAGAIN);
        }
        Ok(StableFileRoute::Linked(LinkedFileRoute {
            path: alias,
            metadata,
        }))
    }

    fn retarget_identity_route(&self, stale_path: &str, route: &LinkedFileRoute) {
        let Some(file_id) = route.metadata.file_id.as_deref() else {
            return;
        };
        if stale_path != route.path {
            self.cache.invalidate(stale_path);
        }
        if let Ok(mut inodes) = self.lock_inodes() {
            inodes.retarget_identity_path(stale_path, &route.path, file_id);
        }
    }

    fn resolve_inode_file_route(&self, ino: INodeNo) -> FuseResult<LinkedFileRoute> {
        self.flush_namespace()?;
        self.flush_writes()?;
        let (path, identity) = self.inode_route(ino)?;
        let Some(identity) = identity else {
            let metadata = self
                .tokio
                .block_on(self.client.stat(&path))
                .map_err(|_| Errno::EIO)?
                .ok_or(Errno::ENOENT)?;
            let resolved_ino = self
                .lock_inodes()?
                .ensure_with_identity(&path, metadata.file_id.as_deref());
            if resolved_ino != ino {
                return Err(Errno::ENOENT);
            }
            return Ok(LinkedFileRoute { path, metadata });
        };
        match self.authoritative_file_route(&path, &identity)? {
            StableFileRoute::Linked(route) => {
                if !self
                    .lock_inodes()?
                    .retarget_identity(ino, &path, &route.path, &identity)
                {
                    return Err(Errno::ENOENT);
                }
                if path != route.path {
                    self.cache.invalidate(&path);
                }
                Ok(route)
            }
            StableFileRoute::Unlinked => Err(Errno::ENOENT),
        }
    }

    fn ensure_ino(&self, path: &str) -> INodeNo {
        self.lock_inodes()
            .map(|mut inodes| inodes.ensure(path))
            .unwrap_or(ROOT_INO)
    }

    fn lookup_ino(&self, path: &str) -> INodeNo {
        self.lock_inodes()
            .map(|mut inodes| inodes.lookup(path))
            .unwrap_or(ROOT_INO)
    }

    fn detach_inode_path(&self, path: &str) {
        if let Ok(mut inodes) = self.lock_inodes() {
            inodes.detach_exact(path);
        }
    }

    fn invalidate_inode_aliases(&self, path: &str) {
        if let Some(file_id) = self
            .cache
            .get_metadata(path)
            .and_then(|metadata| metadata.file_id)
        {
            self.cache.invalidate_identity(&file_id);
        }
        let aliases = self
            .lock_inodes()
            .map(|inodes| inodes.aliases_for_path(path))
            .unwrap_or_else(|_| vec![path.to_string()]);
        for alias in aliases {
            self.cache.invalidate(&alias);
        }
    }

    fn rename_inode_path(&self, from: &str, to: &str) {
        if let Ok(mut inodes) = self.lock_inodes() {
            inodes.rename_path(from, to);
        }
        if let Ok(mut handles) = self.lock_handles() {
            let prefix = format!("{from}/");
            for state in handles.files.values_mut() {
                if state.path == from || state.path.starts_with(&prefix) {
                    let suffix = state.path.strip_prefix(from).unwrap_or_default();
                    state.path = format!("{to}{suffix}");
                }
            }
        }
    }

    fn attr_for_metadata(
        &self,
        ino: INodeNo,
        metadata: &RemoteMetadata,
        preserve_zero_links: bool,
    ) -> FileAttr {
        let kind = file_type_for_kind(&metadata.kind);
        let mut mode = metadata_mode(metadata);
        if self.read_only && kind != FileType::Symlink {
            mode &= !0o222;
        }
        let mtime = metadata
            .updated_at
            .map(|value| value.into())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        FileAttr {
            ino,
            size: metadata.size_bytes,
            blocks: metadata.size_bytes.div_ceil(512),
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind,
            perm: mode as u16,
            nlink: (if preserve_zero_links {
                metadata.link_count
            } else {
                metadata.link_count.max(1)
            })
            .min(u32::MAX as u64) as u32,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn attr_for_path(&self, path: &str, metadata: &RemoteMetadata, lookup: bool) -> FileAttr {
        let ino = self
            .lock_inodes()
            .map(|mut inodes| {
                if lookup {
                    inodes.lookup_with_identity(path, metadata.file_id.as_deref())
                } else {
                    inodes.ensure_with_identity(path, metadata.file_id.as_deref())
                }
            })
            .unwrap_or(ROOT_INO);
        self.attr_for_metadata(ino, metadata, false)
    }

    fn root_attr(&self) -> FileAttr {
        FileAttr {
            ino: ROOT_INO,
            size: 0,
            blocks: 0,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            kind: FileType::Directory,
            perm: if self.read_only { 0o555 } else { 0o755 },
            nlink: 2,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn child_path(parent: &str, name: &OsStr) -> FuseResult<String> {
        let segment = name.to_str().ok_or(Errno::EINVAL)?;
        Ok(if parent.is_empty() {
            segment.to_string()
        } else {
            format!("{parent}/{segment}")
        })
    }

    fn parent_path(path: &str) -> String {
        path.rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
            .unwrap_or_default()
    }

    fn dir_entries(&self, path: &str) -> FuseResult<Vec<RemoteDirEntry>> {
        self.flush_namespace()?;
        if let Some(entries) = self.cache.get_dir(path) {
            return Ok(entries);
        }
        for _ in 0..2 {
            let generation = self.cache.directory_generation(path);
            let entries = self
                .tokio
                .block_on(self.client.list_dir(path))
                .map_err(|_| Errno::EIO)?
                .ok_or(Errno::ENOENT)?;
            if self
                .cache
                .put_dir_if_generation(path, generation, entries.clone())
            {
                return Ok(entries);
            }
        }
        Err(Errno::EAGAIN)
    }

    fn stat_path(&self, path: &str) -> FuseResult<Option<RemoteMetadata>> {
        self.flush_namespace()?;
        if let Some(metadata) = self.open_handle_metadata(path)? {
            return Ok(Some(metadata));
        }
        if let Some(metadata) = self.dirty_handle_committed_metadata(path)? {
            return Ok(Some(metadata));
        }
        if let Some(metadata) = self.cache.get_metadata(path) {
            if metadata.kind != "file" || metadata.content_hash.is_some() {
                return Ok(Some(metadata));
            }
        }
        if self
            .writes
            .as_ref()
            .is_some_and(|writes| writes.has_pending_path(path))
        {
            self.flush_writes()?;
        }
        let metadata = self
            .tokio
            .block_on(self.client.stat(path))
            .map_err(|_| Errno::EIO)?;
        if let Some(metadata) = metadata.as_ref() {
            self.cache.put_metadata(path, metadata.clone());
        }
        Ok(metadata)
    }

    fn stat_path_attributes(&self, path: &str) -> FuseResult<Option<RemoteMetadata>> {
        self.flush_namespace()?;
        if let Some(metadata) = self.open_handle_metadata(path)? {
            return Ok(Some(metadata));
        }
        if let Some(metadata) = self.dirty_handle_committed_metadata(path)? {
            return Ok(Some(metadata));
        }
        if let Some(metadata) = self.cache.get_metadata(path) {
            return Ok(Some(metadata));
        }
        if self
            .writes
            .as_ref()
            .is_some_and(|writes| writes.has_pending_path(path))
        {
            self.flush_writes()?;
        }
        let metadata = self
            .tokio
            .block_on(self.client.stat_attributes(path))
            .map_err(|_| Errno::EIO)?;
        if let Some(metadata) = metadata.as_ref() {
            self.cache.put_metadata(path, metadata.clone());
        }
        Ok(metadata)
    }

    /// Same-mount readers must continue seeing the last committed bytes while
    /// an existing file has a dirty private handle. This is the only path that
    /// may consume metadata embedded in the content cache without a new remote
    /// stat; other mounts have no such handle and always revalidate.
    fn dirty_handle_committed_metadata(&self, path: &str) -> FuseResult<Option<RemoteMetadata>> {
        let preserve_committed = self
            .lock_handles()?
            .files
            .values()
            .any(|state| state.path == path && !state.created && state.dirty);
        if preserve_committed {
            Ok(self.cache.get_committed_file_metadata(path))
        } else {
            Ok(None)
        }
    }

    fn open_handle_metadata(&self, path: &str) -> FuseResult<Option<RemoteMetadata>> {
        let handles = self.lock_handles()?;
        let Some(state) = handles
            .files
            .values()
            .find(|state| state.path == path && state.created && state.dirty)
        else {
            return Ok(None);
        };
        Ok(Some(RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: state.buffer.len() as u64,
            file_id: state.file_id.clone(),
            link_count: state.link_count.max(1),
            link_target: None,
            content_hash: Some(content_hash_for_bytes(&state.buffer)),
            executable: mode_is_executable(state.mode),
            mode: Some(state.mode),
            updated_at: None,
        }))
    }

    fn metadata_for_handle_state(
        &self,
        state: &FileState,
        authoritative: Option<&RemoteMetadata>,
    ) -> RemoteMetadata {
        // A clean loaded handle is only a read cache. Direct gateway and other
        // mount writers may have advanced the same stable inode since it was
        // loaded, so fstat must use the freshly resolved authoritative
        // metadata. Only unpublished/dirty or unlinked handle state is local.
        let use_buffer = state.dirty || state.created || state.unlinked;
        RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: if use_buffer {
                state.buffer.len() as u64
            } else {
                authoritative
                    .map(|metadata| metadata.size_bytes)
                    .unwrap_or(0)
            },
            file_id: state.file_id.clone(),
            link_count: if state.unlinked {
                0
            } else {
                authoritative
                    .map(|metadata| metadata.link_count.max(1))
                    .unwrap_or_else(|| state.link_count.max(1))
            },
            link_target: None,
            content_hash: if use_buffer {
                Some(content_hash_for_bytes(&state.buffer))
            } else {
                authoritative.and_then(|metadata| metadata.content_hash.clone())
            },
            executable: mode_is_executable(state.mode),
            mode: Some(state.mode),
            updated_at: authoritative.and_then(|metadata| metadata.updated_at),
        }
    }

    /// Content counterpart of `open_handle_metadata`: a newly created file
    /// exists only in its creator's handle buffer until flush, so readers
    /// that were shown its metadata must also be served its bytes.
    fn open_handle_content(&self, path: &str) -> FuseResult<Option<Vec<u8>>> {
        let handles = self.lock_handles()?;
        Ok(handles
            .files
            .values()
            .find(|state| state.path == path && state.created && state.dirty)
            .map(|state| state.buffer.clone()))
    }

    /// Apply a pathname-based metadata mutation to a file that exists only in
    /// this mount's created-handle state. Linux may omit `fh` on chmod/truncate
    /// immediately after create; routing that operation to the gateway would
    /// observe ENOENT because the first write has not been flushed yet.
    fn mutate_created_handle_for_path(
        &self,
        path: &str,
        size: Option<u64>,
        mode: Option<u32>,
    ) -> FuseResult<Option<RemoteMetadata>> {
        let mut handles = self.lock_handles()?;
        let Some(fh) = handles
            .files
            .iter()
            .find(|(_, state)| state.path == path && state.created)
            .map(|(fh, _)| *fh)
        else {
            return Ok(None);
        };
        let state = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
        if let Some(size) = size {
            state.buffer.resize(size as usize, 0);
        }
        if let Some(mode) = mode {
            state.mode = normalize_mode(mode);
        }
        if size.is_some() || mode.is_some() {
            state.dirty = true;
            state.loaded = true;
            state.revision = state.revision.saturating_add(1);
        }
        let metadata = RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: state.buffer.len() as u64,
            file_id: state.file_id.clone(),
            link_count: state.link_count.max(1),
            link_target: None,
            content_hash: Some(content_hash_for_bytes(&state.buffer)),
            executable: mode_is_executable(state.mode),
            mode: Some(state.mode),
            updated_at: None,
        };
        Self::mirror_handle_state_locked(&mut handles, fh)?;
        Ok(Some(metadata))
    }

    fn read_bytes(&self, path: &str, offset: u64, size: u32) -> FuseResult<Vec<u8>> {
        // A created-but-unflushed file has no gateway content yet; serve the
        // creator's buffer (checked first so an oversized created buffer never
        // routes into the ranged-read path, which would 404).
        if let Some(bytes) = self.open_handle_content(path)? {
            let start = (offset as usize).min(bytes.len());
            let end = start.saturating_add(size as usize).min(bytes.len());
            return Ok(bytes[start..end].to_vec());
        }
        // Large files never need a content hash (ranged reads are pinned by
        // fingerprint), so route them off the cheap attribute stat before
        // paying for a hashed stat.
        if let Some(attributes) = self.stat_path_attributes(path)? {
            if attributes.kind == "file" && attributes.size_bytes > LARGE_FILE_BYTES {
                return self.read_large_range(path, &attributes, offset, size);
            }
        }
        let metadata = self.stat_path(path)?.ok_or(Errno::ENOENT)?;
        if let Some(bytes) = self.cache.get_file_matching(path, &metadata) {
            let start = (offset as usize).min(bytes.len());
            let end = start.saturating_add(size as usize).min(bytes.len());
            return Ok(bytes[start..end].to_vec());
        }

        if metadata.size_bytes > LARGE_FILE_BYTES {
            return self.read_large_range(path, &metadata, offset, size);
        }
        let bytes = self
            .tokio
            .block_on(self.client.read_file_raw(path))
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;

        // The stat and the content fetch are separate requests; if the file
        // changed between them, cache metadata derived from the bytes we
        // actually hold so attrs and content can never disagree. Same-size
        // replacements are common (counters, locks, fixed-width status), so
        // the hash must be verified, not just the length; a corrected entry
        // drops updated_at because the pre-replacement mtime no longer
        // describes these bytes.
        let fetched_hash = content_hash_for_bytes(&bytes);
        let metadata = if bytes.len() as u64 == metadata.size_bytes
            && metadata.content_hash.as_deref() == Some(fetched_hash.as_str())
        {
            metadata
        } else {
            RemoteMetadata {
                size_bytes: bytes.len() as u64,
                content_hash: Some(fetched_hash),
                updated_at: None,
                ..metadata
            }
        };
        self.cache.put_metadata(path, metadata.clone());
        self.cache.put_file(path, bytes.clone(), Some(metadata));
        let start = (offset as usize).min(bytes.len());
        let end = start.saturating_add(size as usize).min(bytes.len());
        Ok(bytes[start..end].to_vec())
    }

    fn read_large_range(
        &self,
        path: &str,
        metadata: &RemoteMetadata,
        offset: u64,
        size: u32,
    ) -> FuseResult<Vec<u8>> {
        let mut fingerprint = range_fingerprint(metadata);
        let mut retry_delay = Duration::from_millis(10);
        for _ in 0..4 {
            let outcome = self
                .tokio
                .block_on(self.client.read_file_range(
                    path,
                    offset,
                    size as u64,
                    Some(fingerprint.as_str()),
                ))
                .map_err(|_| Errno::EIO)?;
            match outcome {
                RangeRead::Bytes(bytes) => return Ok(bytes),
                RangeRead::NotFound => return Err(Errno::ENOENT),
                RangeRead::Stale => {
                    // The file was replaced under the read; refresh identity
                    // and retry so the kernel sees one file, never a splice.
                    // The cached fingerprint can be up to ATTR_TTL stale, so
                    // budget enough attempts to converge under write churn.
                    std::thread::sleep(retry_delay);
                    retry_delay = retry_delay
                        .saturating_mul(2)
                        .min(Duration::from_millis(100));
                    self.cache.invalidate(path);
                    let refreshed = self.stat_path_attributes(path)?.ok_or(Errno::ENOENT)?;
                    fingerprint = range_fingerprint(&refreshed);
                }
            }
        }
        Err(Errno::EIO)
    }

    fn next_handle(
        &self,
        path: &str,
        initial: Vec<u8>,
        loaded: bool,
        base_content_hash: Option<String>,
        mode: u32,
        created: bool,
        file_id: Option<String>,
        link_count: u64,
    ) -> FuseResult<u64> {
        let mut handles = self.lock_handles()?;
        if handles.files.len() >= MAX_OPEN_HANDLES {
            return Err(Errno::EMFILE);
        }
        let sibling = handles
            .files
            .values()
            .find(|state| Self::same_open_file(state, path, file_id.as_deref()))
            .cloned();
        let coherent_sibling = sibling
            .as_ref()
            .filter(|state| state.loaded && (state.dirty || state.created));
        // An O_EXCL create can already have an empty gateway placeholder while
        // its creator still owns handle-local bytes and pathname mutations.
        // Presence of an authoritative content hash distinguishes that case
        // from the older fully-local create whose first publication is absent.
        let has_remote_baseline = !created || base_content_hash.is_some();
        let initial = coherent_sibling
            .map(|state| state.buffer.clone())
            .unwrap_or(initial);
        let mode = coherent_sibling
            .map(|state| state.mode)
            .unwrap_or_else(|| normalize_mode(mode));
        let base_mode = coherent_sibling
            .map(|state| state.base_mode)
            .unwrap_or_else(|| has_remote_baseline.then_some(mode));
        let created = coherent_sibling
            .map(|state| state.created)
            .unwrap_or(created);
        let dirty = coherent_sibling.is_some_and(|state| state.dirty);
        let loaded = coherent_sibling.is_some() || loaded;
        let base_content_hash = coherent_sibling
            .map(|state| state.base_content_hash.clone())
            .unwrap_or_else(|| {
                if created && base_content_hash.is_none() {
                    Some("absent".to_string())
                } else {
                    base_content_hash
                }
            });
        let publication_acknowledged =
            coherent_sibling.is_some_and(|state| state.publication_acknowledged);
        let pending_publication =
            coherent_sibling.and_then(|state| state.pending_publication.clone());
        let publication_gate = sibling
            .as_ref()
            .map(|state| Arc::clone(&state.publication_gate))
            .unwrap_or_else(|| Arc::new(Mutex::new(())));
        let revision = coherent_sibling.map(|state| state.revision).unwrap_or(0);
        let fh = handles.next;
        handles.next += 1;
        handles.files.insert(
            fh,
            FileState {
                path: path.to_string(),
                file_id,
                link_count,
                unlinked: false,
                buffer: initial,
                mode,
                base_mode,
                created,
                dirty,
                loaded,
                base_content_hash,
                publication_acknowledged,
                pending_publication,
                publication_gate,
                revision,
            },
        );
        Ok(fh)
    }

    fn same_open_file(state: &FileState, path: &str, file_id: Option<&str>) -> bool {
        match (state.file_id.as_deref(), file_id) {
            (Some(left), Some(right)) => left == right,
            _ => !state.unlinked && state.path == path,
        }
    }

    /// All open handles for one inode share a publication gate. Mirroring the
    /// mutable inode state after each ordered mutation gives pathname
    /// truncate/chmod and descriptor writes the same coherent view without
    /// weakening gateway CAS across independent mounts.
    fn mirror_handle_state_locked(handles: &mut HandleTable, source_fh: u64) -> FuseResult<()> {
        let source = handles
            .files
            .get(&source_fh)
            .cloned()
            .ok_or(Errno::ENOENT)?;
        for (fh, state) in &mut handles.files {
            if *fh == source_fh
                || !Self::same_open_file(state, &source.path, source.file_id.as_deref())
            {
                continue;
            }
            state.file_id = source.file_id.clone();
            state.link_count = source.link_count;
            state.buffer = source.buffer.clone();
            state.mode = source.mode;
            state.base_mode = source.base_mode;
            state.publication_acknowledged = source.publication_acknowledged;
            state.pending_publication = source.pending_publication.clone();
            state.created = source.created;
            state.dirty = source.dirty;
            state.loaded = source.loaded;
            state.base_content_hash = source.base_content_hash.clone();
            state.revision = source.revision;
        }
        Ok(())
    }

    fn stat_published_file(
        &self,
        path: &str,
        size_bytes: u64,
        content_hash: &str,
        mode: u32,
        expected_file_id: Option<&str>,
    ) -> FuseResult<LinkedFileRoute> {
        let mut retry_delay = Duration::from_millis(10);
        for _ in 0..4 {
            let route = if let Some(file_id) = expected_file_id {
                match self.authoritative_file_route(path, file_id) {
                    Ok(StableFileRoute::Linked(route)) => Some(route),
                    Ok(StableFileRoute::Unlinked) => None,
                    Err(error) if error.code() == Errno::EAGAIN.code() => None,
                    Err(error) => return Err(error),
                }
            } else {
                self.tokio
                    .block_on(self.client.stat(path))
                    .map_err(|_| Errno::EIO)?
                    .map(|metadata| LinkedFileRoute {
                        path: path.to_string(),
                        metadata,
                    })
            };
            if let Some(route) = route {
                let metadata = &route.metadata;
                if metadata.kind == "file"
                    && metadata.size_bytes == size_bytes
                    && metadata.content_hash.as_deref() == Some(content_hash)
                    && metadata_mode(&metadata) == mode
                    && metadata.file_id.is_some()
                    && expected_file_id
                        .is_none_or(|file_id| metadata.file_id.as_deref() == Some(file_id))
                {
                    return Ok(route);
                }
                return Err(Errno::EIO);
            }
            std::thread::sleep(retry_delay);
            retry_delay = retry_delay.saturating_mul(2);
        }
        Err(Errno::EIO)
    }

    fn acknowledge_pending_publication_locked(&self, fh: u64) -> FuseResult<()> {
        let pending = {
            let handles = self.lock_handles()?;
            handles
                .files
                .get(&fh)
                .ok_or(Errno::ENOENT)?
                .pending_publication
                .clone()
        };
        let Some(pending) = pending else {
            return Ok(());
        };
        self.writes
            .as_ref()
            .ok_or(Errno::EIO)?
            .flush_through(pending.id)
            .map_err(|_| Errno::EIO)?;
        let route = self.stat_published_file(
            &pending.path,
            pending.size_bytes,
            &pending.content_hash,
            pending.mode,
            pending.file_id.as_deref(),
        )?;
        let published_file_id = route
            .metadata
            .file_id
            .clone()
            .or_else(|| pending.file_id.clone());
        let published_link_count = route.metadata.link_count.max(1);
        let cache_bytes = {
            let mut handles = self.lock_handles()?;
            let handle = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
            if handle
                .pending_publication
                .as_ref()
                .is_none_or(|current| current.id != pending.id)
            {
                return Err(Errno::EAGAIN);
            }
            handle.path = route.path.clone();
            handle.file_id = published_file_id.clone();
            handle.link_count = published_link_count;
            handle.unlinked = false;
            handle.base_content_hash = Some(pending.content_hash.clone());
            handle.base_mode = Some(pending.mode);
            handle.created = false;
            handle.publication_acknowledged = false;
            handle.pending_publication = None;
            let cache_bytes = if handle.revision == pending.revision {
                handle.dirty = false;
                Some(handle.buffer.clone())
            } else {
                None
            };
            Self::mirror_handle_state_locked(&mut handles, fh)?;
            cache_bytes
        };
        self.invalidate_inode_aliases(&pending.path);
        if pending.path != route.path {
            self.cache.invalidate(&pending.path);
        }
        if let Some(bytes) = cache_bytes {
            self.cache
                .put_file(&route.path, bytes, Some(route.metadata.clone()));
        } else {
            self.cache.invalidate(&route.path);
        }
        if let Some(file_id) = published_file_id {
            self.lock_inodes()?
                .ensure_with_identity(&route.path, Some(&file_id));
            self.retarget_identity_route(&pending.path, &route);
        }
        Ok(())
    }

    fn publication_gate_for_handle(&self, fh: u64) -> FuseResult<Arc<Mutex<()>>> {
        self.lock_handles()?
            .files
            .get(&fh)
            .map(|state| Arc::clone(&state.publication_gate))
            .ok_or(Errno::ENOENT)
    }

    fn publication_gates_for_subtrees(
        &self,
        paths: &[&str],
    ) -> FuseResult<Vec<(u64, Arc<Mutex<()>>)>> {
        let handles = self.lock_handles()?;
        let mut gates = handles
            .files
            .iter()
            .filter(|(_, state)| {
                paths.iter().any(|path| {
                    state.path == *path
                        || state
                            .path
                            .strip_prefix(path)
                            .is_some_and(|suffix| suffix.starts_with('/'))
                })
            })
            .map(|(fh, state)| (*fh, Arc::clone(&state.publication_gate)))
            .collect::<Vec<_>>();
        // Overlapping renames can share more than one handle. One global
        // acquisition order prevents an AB/BA deadlock without serializing
        // unrelated files or mounts.
        gates.sort_unstable_by_key(|(fh, _)| *fh);
        Ok(gates)
    }

    fn lock_publication_gates<'a>(
        gates: &'a [(u64, Arc<Mutex<()>>)],
    ) -> FuseResult<Vec<MutexGuard<'a, ()>>> {
        gates
            .iter()
            .map(|(_, gate)| gate.lock().map_err(|_| Errno::EIO))
            .collect()
    }

    fn flush_handle_locked(&self, fh: u64) -> FuseResult<()> {
        if self.read_only {
            return Ok(());
        }
        let mut state = {
            let handles = self.lock_handles()?;
            // A duplicate FLUSH may have captured this handle's gate before a
            // concurrent RELEASE removed the table entry. That authorized
            // waiter is already satisfied by the releasing flush.
            let Some(state) = handles.files.get(&fh).cloned() else {
                return Ok(());
            };
            state
        };
        self.flush_namespace()?;
        self.acknowledge_pending_publication_locked(fh)?;
        state = self
            .lock_handles()?
            .files
            .get(&fh)
            .cloned()
            .ok_or(Errno::ENOENT)?;
        if !state.dirty {
            return Ok(());
        }
        if state.unlinked {
            if let Some(handle) = self.lock_handles()?.files.get_mut(&fh) {
                if handle.revision == state.revision {
                    handle.dirty = false;
                }
            }
            return Ok(());
        }

        if state.file_id.is_some() {
            match self.resolve_handle_route_locked(fh)? {
                StableFileRoute::Linked(_) => {
                    state = self
                        .lock_handles()?
                        .files
                        .get(&fh)
                        .cloned()
                        .ok_or(Errno::ENOENT)?;
                }
                StableFileRoute::Unlinked => {
                    if let Some(handle) = self.lock_handles()?.files.get_mut(&fh)
                        && handle.revision == state.revision
                    {
                        handle.dirty = false;
                    }
                    return Ok(());
                }
            }
        }
        let next_content_hash = content_hash_for_bytes(&state.buffer);
        let authoritative_route = if state.base_mode != Some(state.mode) {
            let lease = self
                .tokio
                .block_on(self.client.acquire_lease(
                    &state.path,
                    1,
                    "flush exact-mode vfs fuse write",
                ))
                .map_err(|_| Errno::EIO)?;
            let surface = self.surface_kind_for_path(&state.path);
            let result = (|| -> FuseResult<LinkedFileRoute> {
                if !state.publication_acknowledged {
                    let write = self.tokio.block_on(self.client.write_file(
                        &state.path,
                        &state.buffer,
                        mode_is_executable(state.mode),
                        Some(state.mode),
                        &lease,
                        surface,
                        VFS_OPERATION_WRITE_THROUGH,
                        state.base_content_hash.as_deref(),
                        state.file_id.as_deref(),
                    ));
                    if write.is_err() {
                        // A lost response is indistinguishable from a failed
                        // write until the exact identity/content/mode tuple is
                        // re-read. Never advance the handle on a mismatch.
                        return self.stat_published_file(
                            &state.path,
                            state.buffer.len() as u64,
                            &next_content_hash,
                            state.mode,
                            state.file_id.as_deref(),
                        );
                    }
                    if state.file_id.is_none()
                        && let Some(handle) = self.lock_handles()?.files.get_mut(&fh)
                    {
                        handle.publication_acknowledged = true;
                    }
                }
                self.stat_published_file(
                    &state.path,
                    state.buffer.len() as u64,
                    &next_content_hash,
                    state.mode,
                    state.file_id.as_deref(),
                )
            })();
            let _ = self.tokio.block_on(self.client.release_lease(&lease));
            result?
        } else {
            let publication_id = self
                .writes
                .as_ref()
                .ok_or(Errno::EIO)?
                .enqueue(
                    state.path.as_str(),
                    state.buffer.as_slice(),
                    state.base_content_hash.clone(),
                    state.file_id.clone(),
                )
                .map_err(|_| Errno::EIO)?;
            {
                let mut handles = self.lock_handles()?;
                let handle = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
                if handle.revision != state.revision
                    || handle.path != state.path
                    || handle.file_id != state.file_id
                {
                    return Err(Errno::EAGAIN);
                }
                handle.pending_publication = Some(HandlePublication {
                    id: publication_id,
                    revision: state.revision,
                    path: state.path.clone(),
                    file_id: state.file_id.clone(),
                    size_bytes: state.buffer.len() as u64,
                    content_hash: next_content_hash.clone(),
                    mode: state.mode,
                });
            }
            // The helper retains the exact id and all dirty/base state when
            // either the journal barrier or authoritative verification fails.
            self.acknowledge_pending_publication_locked(fh)?;
            return Ok(());
        };

        let authoritative_metadata = &authoritative_route.metadata;
        let published_file_id = authoritative_metadata
            .file_id
            .clone()
            .or_else(|| state.file_id.clone());
        let published_link_count = authoritative_metadata.link_count.max(1);
        self.invalidate_inode_aliases(&state.path);
        self.cache.put_file(
            &authoritative_route.path,
            state.buffer.clone(),
            Some(RemoteMetadata {
                kind: "file".to_string(),
                size_bytes: state.buffer.len() as u64,
                file_id: published_file_id.clone(),
                link_count: published_link_count,
                link_target: None,
                content_hash: Some(next_content_hash.clone()),
                executable: mode_is_executable(state.mode),
                mode: Some(state.mode),
                updated_at: authoritative_metadata.updated_at,
            }),
        );
        {
            let mut handles = self.lock_handles()?;
            let handle = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
            handle.path = authoritative_route.path.clone();
            handle.loaded = true;
            handle.base_content_hash = Some(next_content_hash);
            handle.base_mode = Some(state.mode);
            handle.file_id = published_file_id.clone();
            handle.link_count = published_link_count;
            handle.created = false;
            handle.publication_acknowledged = false;
            if handle.revision == state.revision {
                handle.dirty = false;
            }
            Self::mirror_handle_state_locked(&mut handles, fh)?;
        }
        if let Some(file_id) = published_file_id {
            self.lock_inodes()?
                .ensure_with_identity(&authoritative_route.path, Some(&file_id));
            self.retarget_identity_route(&state.path, &authoritative_route);
        }
        Ok(())
    }

    fn flush_handle(&self, fh: u64) -> FuseResult<()> {
        if self.read_only {
            return Ok(());
        }
        let gate = self.publication_gate_for_handle(fh)?;
        let _guard = gate.lock().map_err(|_| Errno::EIO)?;
        self.flush_handle_locked(fh)
    }

    fn flush_handle_immediate_locked(&self, fh: u64) -> FuseResult<()> {
        self.flush_handle_locked(fh)?;
        // A file can be renamed between its data flush and the final
        // close/fsync. Do not report a barrier until both ordered journals are
        // remotely acknowledged.
        self.flush_namespace()
    }

    fn flush_handle_immediate(&self, fh: u64) -> FuseResult<()> {
        if self.read_only {
            return Ok(());
        }
        let gate = self.publication_gate_for_handle(fh)?;
        let _guard = gate.lock().map_err(|_| Errno::EIO)?;
        self.flush_handle_immediate_locked(fh)
    }

    fn advisory_lock_target(&self, fh: u64) -> FuseResult<AdvisoryLockTarget> {
        let state = {
            let handles = self.lock_handles()?;
            handles.files.get(&fh).cloned().ok_or(Errno::ENOENT)?
        };
        if let Some(file_id) = state.file_id {
            return Ok(AdvisoryLockTarget {
                path: state.path,
                file_id,
            });
        }

        // create(2) is intentionally handle-local until a publication point.
        // POSIX/OFD/flock operations need the gateway's stable identity, so
        // publish the exact handle bytes and mode before asking for a lock.
        self.flush_handle_immediate(fh)?;
        for _ in 0..4 {
            let state = {
                let handles = self.lock_handles()?;
                handles.files.get(&fh).cloned().ok_or(Errno::ENOENT)?
            };
            if let Some(file_id) = state.file_id {
                return Ok(AdvisoryLockTarget {
                    path: state.path,
                    file_id,
                });
            }
            let metadata = self
                .tokio
                .block_on(self.client.stat_attributes(&state.path))
                .map_err(|_| Errno::EIO)?
                .ok_or(Errno::ENOENT)?;
            if metadata.kind != "file" {
                return Err(Errno::EINVAL);
            }
            let file_id = metadata.file_id.ok_or(Errno::EOPNOTSUPP)?;
            {
                let mut handles = self.lock_handles()?;
                let handle = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
                if handle.path != state.path {
                    continue;
                }
                handle.file_id = Some(file_id.clone());
                handle.link_count = metadata.link_count.max(1);
            }
            self.lock_inodes()?
                .ensure_with_identity(&state.path, Some(&file_id));
            return Ok(AdvisoryLockTarget {
                path: state.path,
                file_id,
            });
        }
        Err(Errno::EAGAIN)
    }

    fn flush_handles_for_path(&self, path: &str) -> FuseResult<()> {
        let handles = self
            .lock_handles()?
            .files
            .iter()
            .filter(|(_, state)| state.path == path)
            .map(|(fh, _)| *fh)
            .collect::<Vec<_>>();
        for fh in handles {
            let gate = self.publication_gate_for_handle(fh)?;
            let _guard = gate.lock().map_err(|_| Errno::EIO)?;
            // Preserve a clean open descriptor's content before deleting its
            // final remotely addressable pathname.
            self.ensure_handle_loaded_locked(fh)?;
            self.flush_handle_locked(fh)?;
        }
        Ok(())
    }

    fn resize_path_immediate(&self, path: &str, size: u64) -> FuseResult<FileAttr> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        // A path-based truncate must never race the ordered write journal
        // with a side-channel PUT: the PUT commits a new content hash at the
        // gateway while journaled writes still carry the pre-truncate base,
        // permanently 409ing them (the original stale-precondition wedge).
        //
        // If any open handle owns this path, resize its buffer; the normal
        // flush publishes it through the journal with correct base chaining.
        let open_handle = {
            let handles = self.lock_handles()?;
            handles
                .files
                .iter()
                .find(|(_, state)| state.path == path)
                .map(|(fh, _)| *fh)
        };
        if let Some(fh) = open_handle {
            self.ensure_handle_loaded(fh)?;
            let mut handles = self.lock_handles()?;
            if let Some(state) = handles.files.get_mut(&fh) {
                state.buffer.resize(size as usize, 0);
                state.dirty = true;
                state.loaded = true;
                state.revision = state.revision.saturating_add(1);
                let metadata = RemoteMetadata {
                    kind: "file".to_string(),
                    size_bytes: size,
                    file_id: state.file_id.clone(),
                    link_count: state.link_count.max(1),
                    link_target: None,
                    content_hash: Some(content_hash_for_bytes(&state.buffer)),
                    executable: mode_is_executable(state.mode),
                    mode: Some(state.mode),
                    updated_at: None,
                };
                Self::mirror_handle_state_locked(&mut handles, fh)?;
                return Ok(self.attr_for_path(path, &metadata, false));
            }
        }
        // No open handle: read the current content (draining any pending
        // journal writes first so the gateway is current), then publish the
        // resized content through the same journal as ordinary writes.
        self.flush_namespace()?;
        self.flush_writes()?;
        let prior = self.stat_path(path)?.ok_or(Errno::ENOENT)?;
        let mut bytes = self
            .tokio
            .block_on(self.client.read_file_raw(path))
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        bytes.resize(size as usize, 0);
        let next_hash = content_hash_for_bytes(&bytes);
        if let Some(writes) = self.writes.as_ref() {
            writes
                .enqueue(
                    path,
                    bytes.as_slice(),
                    prior.content_hash.clone(),
                    prior.file_id.clone(),
                )
                .map_err(|_| Errno::EIO)?;
        } else {
            let lease = self
                .tokio
                .block_on(self.client.acquire_lease(path, 1, "resize vfs fuse file"))
                .map_err(|_| Errno::EIO)?;
            let surface = self.surface_kind_for_path(path);
            let write_result = self.tokio.block_on(self.client.write_file(
                path,
                &bytes,
                prior.executable,
                None,
                &lease,
                surface,
                VFS_OPERATION_SETATTR_SIZE,
                prior.content_hash.as_deref(),
                prior.file_id.as_deref(),
            ));
            let _ = self.tokio.block_on(self.client.release_lease(&lease));
            write_result.map_err(|_| Errno::EIO)?;
        }
        self.invalidate_inode_aliases(path);
        let metadata = RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: size,
            file_id: prior.file_id,
            link_count: prior.link_count,
            link_target: None,
            content_hash: Some(next_hash.clone()),
            executable: prior.executable,
            mode: prior.mode,
            updated_at: None,
        };
        self.cache.put_file(path, bytes, Some(metadata.clone()));
        Ok(self.attr_for_path(path, &metadata, false))
    }

    fn set_mode_path_immediate(&self, path: &str, mode: u32) -> FuseResult<FileAttr> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let mode = normalize_mode(mode);
        self.flush_namespace()?;
        self.flush_writes()?;
        self.cache.invalidate(path);
        let metadata = self
            .tokio
            .block_on(self.client.stat(path))
            .map_err(|error| {
                tracing::warn!(path, error = %error, "vfs setattr stat failed");
                Errno::EIO
            })?
            .ok_or(Errno::ENOENT)?;
        if metadata.kind == "symlink" || metadata_mode(&metadata) == mode {
            self.cache.put_metadata(path, metadata.clone());
            return Ok(self.attr_for_path(path, &metadata, false));
        }

        self.commit_namespace(VfsNamespaceMutation::SetMode {
            path: path.to_string(),
            mode,
        })?;
        let mut updated = metadata;
        updated.mode = Some(mode);
        updated.executable = mode_is_executable(mode);
        self.cache.invalidate(path);
        self.cache.put_metadata(path, updated.clone());
        if let Ok(mut handles) = self.lock_handles() {
            for state in handles
                .files
                .values_mut()
                .filter(|state| state.path == path)
            {
                state.mode = mode;
                state.base_mode = Some(mode);
            }
        }
        Ok(self.attr_for_path(path, &updated, false))
    }

    fn reserve_file_if_absent(&self, path: &str, mode: u32) -> FuseResult<RemoteMetadata> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        self.flush_namespace()?;
        self.flush_writes()?;
        let lease = self
            .tokio
            .block_on(
                self.client
                    .acquire_lease(path, 1, "reserve exclusive vfs fuse file"),
            )
            .map_err(|_| Errno::EIO)?;
        let surface = self.surface_kind_for_path(path);
        let empty_hash = content_hash_for_bytes(&[]);
        let result = (|| -> FuseResult<RemoteMetadata> {
            if self
                .tokio
                .block_on(self.client.stat_attributes(path))
                .map_err(|_| Errno::EIO)?
                .is_some()
            {
                return Err(Errno::EEXIST);
            }
            let write = self.tokio.block_on(self.client.write_file(
                path,
                &[],
                mode_is_executable(mode),
                Some(mode),
                &lease,
                surface,
                VFS_OPERATION_WRITE_THROUGH,
                Some("absent"),
                None,
            ));
            match write {
                Ok(()) => self
                    .stat_published_file(path, 0, &empty_hash, mode, None)
                    .map(|route| route.metadata),
                Err(error)
                    if matches!(
                        request_status(&error),
                        Some(
                            reqwest::StatusCode::CONFLICT
                                | reqwest::StatusCode::PRECONDITION_FAILED
                        )
                    ) =>
                {
                    Err(Errno::EEXIST)
                }
                Err(_) => self
                    .stat_published_file(path, 0, &empty_hash, mode, None)
                    .map(|route| route.metadata)
                    .map_err(|_| Errno::EIO),
            }
        })();
        let _ = self.tokio.block_on(self.client.release_lease(&lease));
        result
    }

    fn mutate_namespace<F>(&self, path: &str, op: F) -> FuseResult<()>
    where
        F: FnOnce(&LeaseGrant, &'static str) -> Result<()>,
    {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let surface = self.surface_kind_for_path(path);
        let lease = self
            .tokio
            .block_on(
                self.client
                    .acquire_lease(path, 1, "apply vfs namespace mutation"),
            )
            .map_err(|_| Errno::EIO)?;
        let result = op(&lease, surface);
        let _ = self.tokio.block_on(self.client.release_lease(&lease));
        result.map_err(|_| Errno::EIO)?;
        self.cache.invalidate(path);
        Ok(())
    }

    fn enqueue_namespace(&self, mutation: VfsNamespaceMutation) -> FuseResult<()> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        if matches!(
            &mutation,
            VfsNamespaceMutation::DeleteFile { .. }
                | VfsNamespaceMutation::RemoveDirectory { .. }
                | VfsNamespaceMutation::SetMode { .. }
                | VfsNamespaceMutation::Rename { .. }
        ) {
            self.flush_writes()?;
        }
        let namespace = self.namespace.as_ref().ok_or(Errno::EIO)?;
        namespace.enqueue(mutation).map_err(|_| Errno::EIO)
    }

    fn commit_namespace(&self, mutation: VfsNamespaceMutation) -> FuseResult<()> {
        let _publication = self
            .namespace_publication_gate
            .lock()
            .map_err(|_| Errno::EIO)?;
        self.enqueue_namespace(mutation)?;
        // Namespace syscalls are publication points. Serialize enqueue+flush
        // so a terminal journal result is returned to the mutation that
        // caused it instead of being consumed by an unrelated waiter.
        self.flush_namespace_locked()
    }

    fn flush_namespace(&self) -> FuseResult<()> {
        let _publication = self
            .namespace_publication_gate
            .lock()
            .map_err(|_| Errno::EIO)?;
        self.flush_namespace_locked()
    }

    fn flush_namespace_locked(&self) -> FuseResult<()> {
        match self.namespace.as_ref() {
            Some(namespace) => namespace.flush().map_err(|error| {
                tracing::warn!(error = %error, "vfs namespace barrier failed");
                Errno::EIO
            }),
            None => Ok(()),
        }
    }

    fn flush_writes(&self) -> FuseResult<()> {
        match self.writes.as_ref() {
            Some(writes) => writes.flush().map_err(|error| {
                tracing::warn!(error = %error, "vfs write barrier failed");
                Errno::EIO
            }),
            None => Ok(()),
        }
    }

    fn resolve_handle_route_locked(&self, fh: u64) -> FuseResult<StableFileRoute> {
        let state = {
            let handles = self.lock_handles()?;
            handles.files.get(&fh).cloned().ok_or(Errno::ENOENT)?
        };
        if state.unlinked {
            return Ok(StableFileRoute::Unlinked);
        }
        let Some(file_id) = state.file_id.as_deref() else {
            return Ok(StableFileRoute::Linked(LinkedFileRoute {
                path: state.path,
                metadata: RemoteMetadata {
                    kind: "file".to_string(),
                    size_bytes: state.buffer.len() as u64,
                    file_id: None,
                    link_count: state.link_count.max(1),
                    link_target: None,
                    content_hash: state.base_content_hash,
                    executable: mode_is_executable(state.mode),
                    mode: Some(state.mode),
                    updated_at: None,
                },
            }));
        };
        let route = self.authoritative_file_route(&state.path, file_id)?;
        let mut handles = self.lock_handles()?;
        let handle = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
        if handle.path != state.path || handle.file_id != state.file_id {
            return Err(Errno::EAGAIN);
        }
        let retarget = match &route {
            StableFileRoute::Linked(route) => {
                handle.path = route.path.clone();
                handle.link_count = route.metadata.link_count.max(1);
                handle.unlinked = false;
                Some(route.clone())
            }
            StableFileRoute::Unlinked => {
                handle.unlinked = true;
                handle.link_count = 0;
                None
            }
        };
        drop(handles);
        if let Some(route) = retarget.as_ref() {
            self.retarget_identity_route(&state.path, route);
        }
        Ok(route)
    }

    fn read_verified_handle_bytes(
        &self,
        mut route: LinkedFileRoute,
        file_id: &str,
    ) -> FuseResult<(LinkedFileRoute, Vec<u8>)> {
        for _ in 0..4 {
            let bytes = self
                .tokio
                .block_on(self.client.read_file_raw(&route.path))
                .map_err(|_| Errno::EIO)?;
            if let Some(bytes) = bytes {
                let verified = self
                    .tokio
                    .block_on(self.client.stat(&route.path))
                    .map_err(|_| Errno::EIO)?;
                if let Some(metadata) = verified
                    && metadata.file_id.as_deref() == Some(file_id)
                    && metadata
                        .content_hash
                        .as_deref()
                        .is_none_or(|hash| hash == content_hash_for_bytes(&bytes))
                {
                    return Ok((
                        LinkedFileRoute {
                            path: route.path,
                            metadata,
                        },
                        bytes,
                    ));
                }
            }
            route = match self.authoritative_file_route(&route.path, file_id)? {
                StableFileRoute::Linked(route) => route,
                StableFileRoute::Unlinked => return Err(Errno::ENOENT),
            };
        }
        Err(Errno::EAGAIN)
    }

    fn ensure_handle_loaded_locked(&self, fh: u64) -> FuseResult<()> {
        let state = {
            let handles = self.lock_handles()?;
            handles.files.get(&fh).cloned().ok_or(Errno::ENOENT)?
        };
        if state.loaded && (state.dirty || state.unlinked || state.file_id.is_none()) {
            return Ok(());
        }
        self.flush_namespace()?;
        // A created-but-unflushed file exists only in its creator's buffer;
        // seed this handle from it instead of 404ing at the gateway.
        let (route, bytes) = if state.file_id.is_none() {
            let bytes = self
                .open_handle_content(&state.path)?
                .ok_or(Errno::ENOENT)?;
            (
                LinkedFileRoute {
                    path: state.path.clone(),
                    metadata: RemoteMetadata {
                        kind: "file".to_string(),
                        size_bytes: bytes.len() as u64,
                        file_id: None,
                        link_count: state.link_count.max(1),
                        link_target: None,
                        content_hash: Some(content_hash_for_bytes(&bytes)),
                        executable: mode_is_executable(state.mode),
                        mode: Some(state.mode),
                        updated_at: None,
                    },
                },
                bytes,
            )
        } else {
            let file_id = state.file_id.as_deref().expect("checked stable identity");
            let route = match self.resolve_handle_route_locked(fh)? {
                StableFileRoute::Linked(route) => route,
                StableFileRoute::Unlinked if state.loaded => return Ok(()),
                StableFileRoute::Unlinked => return Err(Errno::ENOENT),
            };
            if state.loaded
                && route.metadata.content_hash.is_some()
                && route.metadata.content_hash == state.base_content_hash
                && route.metadata.size_bytes == state.buffer.len() as u64
            {
                let mut handles = self.lock_handles()?;
                let handle = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
                if handle.revision != state.revision
                    || handle.file_id != state.file_id
                    || handle.dirty
                {
                    return Ok(());
                }
                handle.path = route.path.clone();
                handle.link_count = route.metadata.link_count.max(1);
                handle.mode = metadata_mode(&route.metadata);
                handle.base_mode = Some(handle.mode);
                return Ok(());
            }
            self.read_verified_handle_bytes(route, file_id)?
        };
        let content_hash = route
            .metadata
            .content_hash
            .clone()
            .unwrap_or_else(|| content_hash_for_bytes(&bytes));
        let mut handles = self.lock_handles()?;
        let handle = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
        if handle.loaded || handle.dirty || handle.revision != state.revision {
            return Ok(());
        }
        if handle.file_id != state.file_id {
            return Err(Errno::EAGAIN);
        }
        handle.path = route.path.clone();
        handle.buffer = bytes.clone();
        handle.loaded = true;
        handle.link_count = route.metadata.link_count.max(1);
        handle.mode = metadata_mode(&route.metadata);
        handle.base_mode = Some(handle.mode);
        handle.base_content_hash = Some(content_hash);
        drop(handles);
        self.cache
            .put_file(&route.path, bytes, Some(route.metadata.clone()));
        self.retarget_identity_route(&state.path, &route);
        Ok(())
    }

    fn ensure_handle_loaded(&self, fh: u64) -> FuseResult<()> {
        let gate = self.publication_gate_for_handle(fh)?;
        let _guard = gate.lock().map_err(|_| Errno::EIO)?;
        self.ensure_handle_loaded_locked(fh)
    }

    fn scoped_path(&self, path: &str) -> String {
        scoped_vfs_path(self.scope_path.as_str(), path)
    }

    fn surface_kind_for_path(&self, path: &str) -> &'static str {
        Self::surface_kind_for_scoped_path(self.scoped_path(path).as_str())
    }

    fn surface_kind_for_scoped_path(path: &str) -> &'static str {
        if path.contains("/shared") {
            VFS_SURFACE_KIND_VM_SHARED
        } else {
            VFS_SURFACE_KIND_VM_WORKSPACE
        }
    }

    fn lock_inodes(&self) -> FuseResult<MutexGuard<'_, InodeTable>> {
        self.inodes.lock().map_err(|_| Errno::EIO)
    }

    fn lock_handles(&self) -> FuseResult<MutexGuard<'_, HandleTable>> {
        self.handles.lock().map_err(|_| Errno::EIO)
    }

    fn advisory_lock_owner_key(&self, lock_owner: fuser::LockOwner) -> String {
        lock_owner.0.to_string()
    }

    fn advisory_lock_namespace(namespace: LockNamespace) -> &'static str {
        match namespace {
            LockNamespace::Posix => "posix",
            LockNamespace::Flock => "flock",
        }
    }

    fn advisory_lock_range(namespace: LockNamespace, start: u64, end: u64) -> (u64, u64) {
        match namespace {
            LockNamespace::Posix => (start, end),
            LockNamespace::Flock => (0, u64::MAX),
        }
    }

    fn advisory_lock_kind(typ: i32) -> FuseResult<&'static str> {
        match typ {
            value if value == i32::from(libc::F_RDLCK) => Ok("read"),
            value if value == i32::from(libc::F_WRLCK) => Ok("write"),
            value if value == i32::from(libc::F_UNLCK) => Ok("unlock"),
            _ => Err(Errno::EINVAL),
        }
    }

    fn advisory_lock_error(error: &anyhow::Error) -> Errno {
        match request_status(error) {
            Some(reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::NOT_IMPLEMENTED) => {
                Errno::EOPNOTSUPP
            }
            Some(reqwest::StatusCode::BAD_REQUEST) => Errno::EINVAL,
            _ => Errno::EIO,
        }
    }

    fn release_advisory_lock_owner(
        &self,
        ino: INodeNo,
        lock_owner: fuser::LockOwner,
        namespace: LockNamespace,
        posix_fh: Option<u64>,
    ) -> FuseResult<()> {
        let owner = self.advisory_lock_owner_key(lock_owner);
        let owner_key = (namespace, owner.clone());
        // Closing the local file description ends this process's ownership even
        // when the gateway release RPC is temporarily unavailable. Remove the
        // heartbeat source first so a failed close cannot renew an abandoned
        // remote lock forever; the persisted lease then expires naturally.
        let releases = {
            let mut active = self.active_lock_owners.lock().map_err(|_| Errno::EIO)?;
            let mut releases = if namespace == LockNamespace::Posix {
                posix_fh
                    .map(|fh| take_active_posix_handle_locks(&mut active, fh, ino.0))
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            if let Some(file_id) = take_active_advisory_lock_file_id(&mut active, &owner_key, ino.0)
                && !releases.iter().any(|(candidate_owner, candidate_file)| {
                    candidate_owner == &owner && candidate_file == &file_id
                })
            {
                releases.push((owner, file_id));
            }
            releases
        };
        let mut first_error = None;
        for (owner, file_id) in releases {
            if let Err(error) =
                self.release_remote_advisory_lock_identity(&owner, &file_id, namespace)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn release_remote_advisory_lock_identity(
        &self,
        owner: &str,
        file_id: &str,
        namespace: LockNamespace,
    ) -> FuseResult<()> {
        self.tokio
            .block_on(self.client.release_advisory_lock_owner(
                &self.mount_id,
                owner,
                file_id,
                Self::advisory_lock_namespace(namespace),
            ))
            .map_err(|error| Self::advisory_lock_error(&error))?;
        Ok(())
    }

    fn set_advisory_lock(
        &self,
        target: &AdvisoryLockTarget,
        lock_owner: fuser::LockOwner,
        namespace: LockNamespace,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        cancellation: Option<&LockWaitCancellation>,
    ) -> FuseResult<Option<String>> {
        let owner = self.advisory_lock_owner_key(lock_owner);
        let (start, end) = Self::advisory_lock_range(namespace, start, end);
        let kind = Self::advisory_lock_kind(typ)?;
        let deadline = Instant::now() + ADVISORY_LOCK_BLOCK_TIMEOUT;
        loop {
            if cancellation.is_some_and(LockWaitCancellation::is_cancelled) {
                return Err(Errno::EINTR);
            }
            let response = self
                .tokio
                .block_on(self.client.advisory_lock(
                    "set",
                    &target.path,
                    &self.mount_id,
                    &owner,
                    Self::advisory_lock_namespace(namespace),
                    start,
                    end,
                    kind,
                    pid,
                ))
                .map_err(|error| Self::advisory_lock_error(&error))?;
            if response.file_id.as_deref() != Some(target.file_id.as_str()) {
                return Err(Errno::EIO);
            }
            if response.acquired {
                return if kind == "unlock" {
                    Ok(None)
                } else {
                    response.file_id.map(Some).ok_or(Errno::EIO)
                };
            }
            if !sleep || Instant::now() >= deadline {
                return Err(Errno::EAGAIN);
            }
            if let Some(cancellation) = cancellation {
                if cancellation.wait_cancelled(ADVISORY_LOCK_RETRY_DELAY) {
                    return Err(Errno::EINTR);
                }
            } else {
                std::thread::sleep(ADVISORY_LOCK_RETRY_DELAY);
            }
        }
    }
}

impl Drop for RemoteFuseFs {
    fn drop(&mut self) {
        let client = self.client.clone();
        let mount_id = self.mount_id.clone();
        self.tokio.spawn(async move {
            if let Err(error) = client.release_advisory_lock_mount(&mount_id).await {
                tracing::debug!(
                    mount_id,
                    error = %error,
                    "best-effort distributed VFS lock release failed during unmount"
                );
            }
        });
    }
}

#[cfg(test)]
fn content_hash_conflicts(base: Option<&str>, current: Option<&str>) -> bool {
    matches!((base, current), (Some(base), Some(current)) if base != current)
}

/// Cheap file-identity fingerprint for pinning ranged reads; must mirror
/// `rangeFingerprint` in ts/vfs-gateway-server.ts exactly (epoch millis).
fn range_fingerprint(metadata: &RemoteMetadata) -> String {
    let millis = metadata
        .updated_at
        .map(|updated| updated.timestamp_millis())
        .unwrap_or(-1);
    format!("{}:{millis}", metadata.size_bytes)
}

fn content_hash_for_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(hasher.finalize().as_ref())
}

fn normalize_mode(mode: u32) -> u32 {
    mode & POSIX_MODE_MASK
}

fn mode_is_executable(mode: u32) -> bool {
    mode & 0o111 != 0
}

fn metadata_mode(metadata: &RemoteMetadata) -> u32 {
    if metadata.kind == "symlink" {
        return 0o777;
    }
    metadata.mode.map(normalize_mode).unwrap_or_else(|| {
        if metadata.kind == "directory" || metadata.executable {
            0o755
        } else {
            0o644
        }
    })
}

fn creation_mode(mode: u32, _umask: u32) -> u32 {
    // We intentionally do not request FUSE_DONT_MASK, so the kernel has
    // already applied umask before dispatching create/mkdir to userspace.
    normalize_mode(mode)
}

fn file_type_for_kind(kind: &str) -> FileType {
    match kind {
        "directory" => FileType::Directory,
        "symlink" => FileType::Symlink,
        _ => FileType::RegularFile,
    }
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

impl RemoteFuseFs {
    fn requested_init_capabilities_for(read_only: bool) -> InitFlags {
        let locks = InitFlags::FUSE_POSIX_LOCKS | InitFlags::FUSE_FLOCK_LOCKS;
        if read_only {
            InitFlags::FUSE_AUTO_INVAL_DATA | locks
        } else {
            InitFlags::FUSE_WRITEBACK_CACHE | InitFlags::FUSE_AUTO_INVAL_DATA | locks
        }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::extract::State;
    use axum::http::{Method, Request, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::any;
    use axum::{Json, Router};
    use chevalier_sandbox::vfs::VfsMetadata as RemoteMetadata;
    use fuser::{InitFlags, LockNamespace};
    use tokio::runtime::Builder;

    use super::super::client::RemoteVfsClient;
    use super::{
        ActiveAdvisoryLockFile, ActiveAdvisoryLocks, InodeTable, LockWaitCancellation, ROOT_INO,
        RemoteFuseFs, TTL, active_advisory_lock_identities, combine_flush_and_lock_cleanup,
        content_hash_conflicts, content_hash_for_bytes, creation_mode, range_fingerprint,
        take_active_advisory_lock_file_id, take_active_posix_handle_locks,
    };

    #[derive(Default)]
    struct LockPublicationGateway {
        stat_requests: usize,
        write_batches: Vec<Vec<u8>>,
        write_preconditions: Vec<Option<String>>,
    }

    async fn lock_publication_gateway(
        State(state): State<Arc<Mutex<LockPublicationGateway>>>,
        request: Request<Body>,
    ) -> Response {
        let method = request.method().clone();
        let path = request.uri().path().to_string();
        match (method, path.as_str()) {
            (Method::POST, "/lease") => Json(serde_json::json!({
                "resource_key": "lock-publication-test",
                "owner_token": "00000000-0000-0000-0000-000000000001",
                "task_id": null
            }))
            .into_response(),
            (Method::DELETE, "/lease") => StatusCode::NO_CONTENT.into_response(),
            (Method::PUT, "/file") => {
                let precondition = request
                    .headers()
                    .get("x-chevalier-vfs-precondition-fingerprint")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                let body = to_bytes(request.into_body(), 1024 * 1024)
                    .await
                    .expect("read write request");
                let mut state = state.lock().unwrap();
                state.write_preconditions.push(precondition);
                state.write_batches.push(body.to_vec());
                StatusCode::NO_CONTENT.into_response()
            }
            (Method::POST, "/write-many") => {
                let body = to_bytes(request.into_body(), 1024 * 1024)
                    .await
                    .expect("read write-many request");
                let payload: serde_json::Value =
                    serde_json::from_slice(&body).expect("decode write-many request");
                let bytes = payload["writes"][0]["body"]
                    .as_array()
                    .expect("write body array")
                    .iter()
                    .map(|byte| byte.as_u64().expect("write byte") as u8)
                    .collect::<Vec<_>>();
                state.lock().unwrap().write_batches.push(bytes);
                StatusCode::NO_CONTENT.into_response()
            }
            (Method::GET, "/stat") => {
                state.lock().unwrap().stat_requests += 1;
                Json(serde_json::json!({
                    "kind": "file",
                    "size_bytes": 0,
                    "file_id": "stable-new-file",
                    "link_count": 1,
                    "link_target": null,
                    "content_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    "executable": false,
                    "mode": 416,
                    "updated_at": null
                }))
                .into_response()
            }
            (Method::POST, "/posix-lock/v1") => StatusCode::NO_CONTENT.into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    #[test]
    fn kernel_metadata_cache_is_disabled_for_cross_mount_coherence() {
        assert_eq!(TTL, Duration::ZERO);
    }

    #[test]
    fn surface_kind_uses_scoped_path_not_mount_relative_path() {
        assert_eq!(
            RemoteFuseFs::surface_kind_for_scoped_path(
                "conversations/11111111-1111-1111-1111-111111111111/shared/note.txt"
            ),
            chevalier_sandbox::vfs::VFS_SURFACE_KIND_VM_SHARED
        );
        assert_eq!(
            RemoteFuseFs::surface_kind_for_scoped_path(
                "conversations/11111111-1111-1111-1111-111111111111/0001_assistant/mount/note.txt"
            ),
            chevalier_sandbox::vfs::VFS_SURFACE_KIND_VM_WORKSPACE
        );
    }

    #[test]
    fn mounts_request_automatic_data_invalidation() {
        assert_eq!(
            RemoteFuseFs::requested_init_capabilities_for(false),
            InitFlags::FUSE_WRITEBACK_CACHE
                | InitFlags::FUSE_AUTO_INVAL_DATA
                | InitFlags::FUSE_POSIX_LOCKS
                | InitFlags::FUSE_FLOCK_LOCKS
        );
        assert_eq!(
            RemoteFuseFs::requested_init_capabilities_for(true),
            InitFlags::FUSE_AUTO_INVAL_DATA
                | InitFlags::FUSE_POSIX_LOCKS
                | InitFlags::FUSE_FLOCK_LOCKS
        );
    }

    #[test]
    fn failed_remote_release_cannot_leave_an_abandoned_lock_heartbeat_active() {
        let mut active = ActiveAdvisoryLocks::new();
        let owner_key = (LockNamespace::Posix, "42".to_string());
        active.entry(owner_key.clone()).or_default().insert(
            77,
            ActiveAdvisoryLockFile {
                file_id: "stable-file-77".to_string(),
                fh: 7,
            },
        );
        active
            .entry((LockNamespace::Flock, "84".to_string()))
            .or_default()
            .insert(
                88,
                ActiveAdvisoryLockFile {
                    file_id: "stable-file-88".to_string(),
                    fh: 8,
                },
            );

        assert_eq!(
            take_active_advisory_lock_file_id(&mut active, &owner_key, 77).as_deref(),
            Some("stable-file-77")
        );
        let identities = active_advisory_lock_identities(&active);
        assert_eq!(identities.len(), 1);
        assert_eq!(identities[0].lock_owner, "84");
        assert_eq!(identities[0].namespace, "flock");
        assert_eq!(identities[0].file_id, "stable-file-88");
    }

    #[test]
    fn posix_close_releases_setlk_owners_by_open_file_description() {
        let mut active = ActiveAdvisoryLocks::new();
        for (owner, ino, fh) in [
            ("same-handle-a", 77, 700),
            ("same-handle-b", 77, 700),
            ("other-handle", 77, 701),
            ("other-inode", 78, 700),
        ] {
            active
                .entry((LockNamespace::Posix, owner.to_string()))
                .or_default()
                .insert(
                    ino,
                    ActiveAdvisoryLockFile {
                        file_id: format!("file-{ino}"),
                        fh,
                    },
                );
        }

        let mut releases = take_active_posix_handle_locks(&mut active, 700, 77);
        releases.sort();
        assert_eq!(
            releases,
            vec![
                ("same-handle-a".to_string(), "file-77".to_string()),
                ("same-handle-b".to_string(), "file-77".to_string()),
            ]
        );
        let identities = active_advisory_lock_identities(&active);
        assert_eq!(identities.len(), 2);
        assert!(
            identities
                .iter()
                .any(|identity| identity.lock_owner == "other-handle")
        );
        assert!(
            identities
                .iter()
                .any(|identity| identity.lock_owner == "other-inode")
        );
    }

    #[test]
    fn close_cleanup_runs_without_masking_the_primary_flush_error() {
        assert_eq!(
            combine_flush_and_lock_cleanup(
                "test",
                Err(fuser::Errno::ENOSPC),
                Err(fuser::Errno::EIO),
            )
            .unwrap_err()
            .code(),
            libc::ENOSPC
        );
        assert_eq!(
            combine_flush_and_lock_cleanup("test", Ok(()), Err(fuser::Errno::EIO))
                .unwrap_err()
                .code(),
            libc::EIO
        );
        assert_eq!(
            combine_flush_and_lock_cleanup("test", Err(fuser::Errno::ENOSPC), Ok(()))
                .unwrap_err()
                .code(),
            libc::ENOSPC
        );
    }

    #[test]
    fn lock_wait_cancellation_serializes_with_completion() {
        let cancelled = LockWaitCancellation::new();
        assert!(cancelled.cancel());
        assert!(!cancelled.finish());
        assert!(cancelled.wait_cancelled(Duration::ZERO));

        let completed = LockWaitCancellation::new();
        assert!(completed.finish());
        assert!(!completed.cancel());
        assert!(!completed.wait_cancelled(Duration::ZERO));
    }

    #[test]
    fn open_handles_track_exact_mode_and_created_publication_baselines() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new(client, false, "test-scope", runtime.handle().clone());
        let existing_handle = fs
            .next_handle("existing", Vec::new(), true, None, 0o751, false, None, 1)
            .unwrap();
        let created_handle = fs
            .next_handle("created", Vec::new(), true, None, 0o640, true, None, 1)
            .unwrap();
        let reserved_handle = fs
            .next_handle(
                "reserved",
                Vec::new(),
                true,
                Some("empty-hash".to_string()),
                0o644,
                true,
                Some("inode-reserved".to_string()),
                1,
            )
            .unwrap();

        let handles = fs.lock_handles().unwrap();
        let existing = handles.files.get(&existing_handle).unwrap();
        assert_eq!(existing.mode, 0o751);
        assert_eq!(existing.base_mode, Some(0o751));
        let authoritative = RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: 6,
            file_id: None,
            link_count: 1,
            link_target: None,
            content_hash: Some(content_hash_for_bytes(b"short\n")),
            executable: false,
            mode: Some(0o644),
            updated_at: None,
        };
        assert_eq!(
            fs.metadata_for_handle_state(existing, Some(&authoritative))
                .size_bytes,
            6,
            "a clean loaded handle must not hide a shorter gateway replacement"
        );

        let created = handles.files.get(&created_handle).unwrap();
        assert_eq!(created.mode, 0o640);
        assert_eq!(
            created.base_mode, None,
            "a newly created file has no exact gateway mode baseline"
        );
        assert_eq!(
            created.base_content_hash.as_deref(),
            Some("absent"),
            "first publication must use an if-absent CAS"
        );

        let reserved = handles.files.get(&reserved_handle).unwrap();
        assert!(
            reserved.created,
            "the creator still owns pathname mutations"
        );
        assert_eq!(reserved.base_mode, Some(0o644));
        assert_eq!(
            reserved.base_content_hash.as_deref(),
            Some("empty-hash"),
            "an O_EXCL placeholder is the creator's authoritative CAS baseline"
        );
    }

    #[test]
    fn same_inode_handles_share_dirty_state_and_publication_gate() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new(client, false, "test-scope", runtime.handle().clone());
        let first = fs
            .next_handle(
                "value",
                Vec::new(),
                true,
                Some("empty-hash".to_string()),
                0o644,
                true,
                Some("inode-value".to_string()),
                1,
            )
            .unwrap();
        {
            let mut handles = fs.lock_handles().unwrap();
            let state = handles.files.get_mut(&first).unwrap();
            state.buffer = b"dirty".to_vec();
            state.mode = 0o755;
            state.dirty = true;
            state.revision = 1;
        }
        let second = fs
            .next_handle(
                "value",
                Vec::new(),
                false,
                Some("empty-hash".to_string()),
                0o644,
                false,
                Some("inode-value".to_string()),
                1,
            )
            .unwrap();
        {
            let mut handles = fs.lock_handles().unwrap();
            let first_state = handles.files.get(&first).unwrap().clone();
            let second_state = handles.files.get(&second).unwrap();
            assert_eq!(second_state.buffer, b"dirty");
            assert_eq!(second_state.mode, 0o755);
            assert!(second_state.created);
            assert!(second_state.dirty);
            assert!(Arc::ptr_eq(
                &first_state.publication_gate,
                &second_state.publication_gate
            ));

            let second_state = handles.files.get_mut(&second).unwrap();
            second_state.buffer.truncate(3);
            second_state.dirty = false;
            second_state.base_content_hash = Some("published-hash".to_string());
            second_state.base_mode = Some(0o755);
            second_state.created = false;
            RemoteFuseFs::mirror_handle_state_locked(&mut handles, second).unwrap();
            let first_state = handles.files.get(&first).unwrap();
            assert_eq!(first_state.buffer, b"dir");
            assert!(!first_state.dirty);
            assert!(!first_state.created);
            assert_eq!(
                first_state.base_content_hash.as_deref(),
                Some("published-hash")
            );
        }
    }

    #[test]
    fn rename_serializes_with_inflight_and_duplicate_handle_publication() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let fs = Arc::new(RemoteFuseFs::new(
            client,
            false,
            "test-scope",
            runtime.handle().clone(),
        ));
        let handle = fs
            .next_handle(
                "repo/config.lock",
                Vec::new(),
                true,
                None,
                0o644,
                false,
                Some("stable-config".to_string()),
                1,
            )
            .unwrap();
        let inflight_gate = fs.publication_gate_for_handle(handle).unwrap();
        let inflight = inflight_gate.lock().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let rename_fs = Arc::clone(&fs);
        let rename = std::thread::spawn(move || {
            let gates = rename_fs
                .publication_gates_for_subtrees(&["repo/config.lock", "repo/config"])
                .unwrap();
            started_tx.send(()).unwrap();
            let _guards = RemoteFuseFs::lock_publication_gates(&gates).unwrap();
            rename_fs.rename_inode_path("repo/config.lock", "repo/config");
            completed_tx.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            completed_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "rename must wait for the in-flight handle publication"
        );
        drop(inflight);
        completed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        rename.join().unwrap();
        assert_eq!(
            fs.lock_handles().unwrap().files.get(&handle).unwrap().path,
            "repo/config"
        );

        // Model a duplicate FLUSH that captured the gate before RELEASE
        // removed the handle. Once admitted, missing table state is an
        // idempotent success rather than a spurious close error.
        let duplicate_gate = fs.publication_gate_for_handle(handle).unwrap();
        let duplicate = duplicate_gate.lock().unwrap();
        fs.lock_handles().unwrap().files.remove(&handle);
        drop(duplicate);
        assert!(fs.flush_handle_locked(handle).is_ok());
    }

    #[test]
    fn advisory_lock_publishes_a_brand_new_empty_file_once() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(LockPublicationGateway::default()));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(lock_publication_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let journal_dir = tempfile::tempdir().unwrap();
        let client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new_with_namespace_journal(
            client,
            false,
            "test-scope",
            &journal_dir.path().join("namespace.jsonl"),
            runtime.handle().clone(),
        )
        .unwrap();
        let handle = fs
            .next_handle("fresh.lock", Vec::new(), true, None, 0o640, true, None, 1)
            .unwrap();
        {
            let mut handles = fs.lock_handles().unwrap();
            handles.files.get_mut(&handle).unwrap().dirty = true;
        }

        let first = fs.advisory_lock_target(handle).unwrap();
        let second = fs.advisory_lock_target(handle).unwrap();

        assert_eq!(first.path, "fresh.lock");
        assert_eq!(first.file_id, "stable-new-file");
        assert_eq!(second.file_id, first.file_id);
        {
            let handles = fs.lock_handles().unwrap();
            let state = handles.files.get(&handle).unwrap();
            assert!(!state.dirty);
            assert_eq!(state.file_id.as_deref(), Some("stable-new-file"));
            assert_eq!(state.mode, 0o640);
        }
        let gateway = gateway.lock().unwrap();
        assert_eq!(gateway.write_batches, vec![Vec::<u8>::new()]);
        assert_eq!(
            gateway.write_preconditions,
            vec![Some("absent".to_string())],
            "first publication must be an atomic if-absent write"
        );
        assert_eq!(
            gateway.stat_requests, 1,
            "the recorded stable identity must skip later publication barriers"
        );
        drop(gateway);
        drop(fs);
        server.abort();
    }

    #[test]
    fn advisory_lock_reuses_an_existing_stable_identity_without_flushing() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new(client, false, "test-scope", runtime.handle().clone());
        let handle = fs
            .next_handle(
                "existing.lock",
                b"dirty local bytes".to_vec(),
                true,
                Some(content_hash_for_bytes(b"committed")),
                0o644,
                false,
                Some("stable-existing-file".to_string()),
                1,
            )
            .unwrap();
        {
            let mut handles = fs.lock_handles().unwrap();
            handles.files.get_mut(&handle).unwrap().dirty = true;
        }

        let target = fs.advisory_lock_target(handle).unwrap();

        assert_eq!(target.path, "existing.lock");
        assert_eq!(target.file_id, "stable-existing-file");
        assert!(
            fs.lock_handles().unwrap().files.get(&handle).unwrap().dirty,
            "an existing stable identity must not force unrelated dirty bytes to flush"
        );
    }

    #[test]
    fn inode_table_keeps_live_lookups_past_the_previous_capacity_boundary() {
        let mut table = InodeTable::new();
        let first = table.lookup("package-0");
        for index in 1..=70_000 {
            table.ensure(format!("package-{index}").as_str());
        }

        assert_eq!(table.path(first).as_deref(), Some("package-0"));
        assert_eq!(table.path(ROOT_INO).as_deref(), Some(""));

        table.forget(first, 1);
        assert!(table.path(first).is_none());
        assert_eq!(table.path(ROOT_INO).as_deref(), Some(""));
    }

    #[test]
    fn inode_table_tracks_directory_renames_and_detaches_deleted_subtrees() {
        let mut table = InodeTable::new();
        let directory = table.lookup("node_modules/pkg_tmp");
        let child = table.lookup("node_modules/pkg_tmp/index.js");

        table.rename_path("node_modules/pkg_tmp", "node_modules/pkg");

        assert_eq!(table.path(directory).as_deref(), Some("node_modules/pkg"));
        assert_eq!(
            table.path(child).as_deref(),
            Some("node_modules/pkg/index.js")
        );

        table.detach_subtree("node_modules/pkg");
        assert!(table.path(directory).is_none());
        assert!(table.path(child).is_none());
    }

    #[test]
    fn inode_table_exact_detach_does_not_scan_or_remove_neighboring_paths() {
        let mut table = InodeTable::new();
        let removed = table.lookup("node_modules/pkg/index.js");
        let neighbor = table.lookup("node_modules/pkg-extra/index.js");

        table.detach_exact("node_modules/pkg/index.js");

        assert!(table.path(removed).is_none());
        assert_eq!(
            table.path(neighbor).as_deref(),
            Some("node_modules/pkg-extra/index.js")
        );
    }

    #[test]
    fn inode_table_uses_one_inode_for_all_paths_of_stable_identity() {
        let mut table = InodeTable::new();
        let source = table.lookup_with_identity("source", Some("inode-1"));
        let alias = table.lookup_with_identity("nested/alias", Some("inode-1"));
        assert_eq!(source, alias);
        assert_eq!(
            table.aliases_for_path("source"),
            vec!["nested/alias".to_string(), "source".to_string()]
        );

        table.detach_exact("source");
        assert_eq!(table.path(alias).as_deref(), Some("nested/alias"));
        assert_eq!(table.ensure_with_identity("third", Some("inode-1")), alias);
    }

    #[test]
    fn inode_table_late_identity_binding_keeps_created_source_and_link_alias_together() {
        let mut table = InodeTable::new();
        let source = table.lookup("source");

        assert_eq!(
            table.ensure_with_identity("source", Some("inode-after-publish")),
            source
        );
        assert_eq!(
            table.lookup_with_identity("alias", Some("inode-after-publish")),
            source
        );
        assert_eq!(
            table.aliases_for_path("source"),
            vec!["alias".to_string(), "source".to_string()]
        );
    }

    #[test]
    fn inode_table_path_reuse_keeps_live_stable_identity_routable() {
        let mut table = InodeTable::new();
        let original = table.lookup_with_identity("config", Some("inode-original"));
        let replacement = table.lookup_with_identity("config", Some("inode-replacement"));

        assert_ne!(original, replacement);
        assert_eq!(
            table.route(original),
            Some(("config".to_string(), Some("inode-original".to_string()))),
            "the detached live inode retains only an alias-search hint"
        );
        assert_eq!(
            table.route(replacement),
            Some(("config".to_string(), Some("inode-replacement".to_string())))
        );

        assert_eq!(
            table.ensure_with_identity("surviving-alias", Some("inode-original")),
            original
        );
        assert_eq!(
            table.path(original).as_deref(),
            Some("surviving-alias"),
            "authoritative alias recovery retargets the original inode"
        );
        assert_eq!(
            table.path(replacement).as_deref(),
            Some("config"),
            "path reuse remains bound to the replacement identity"
        );
    }

    #[test]
    fn dirty_open_unlinked_handle_is_never_published_by_deleted_path() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new(client, false, "test-scope", runtime.handle().clone());
        let handle = fs
            .next_handle(
                "deleted",
                b"private dirty bytes".to_vec(),
                true,
                Some(content_hash_for_bytes(b"committed")),
                0o644,
                false,
                Some("inode-1".to_string()),
                0,
            )
            .unwrap();
        {
            let mut handles = fs.lock_handles().unwrap();
            let state = handles.files.get_mut(&handle).unwrap();
            state.dirty = true;
            state.unlinked = true;
        }

        fs.flush_handle(handle)
            .expect("unlinked flush is a local lifetime barrier");
        let handles = fs.lock_handles().unwrap();
        let state = handles.files.get(&handle).unwrap();
        assert!(!state.dirty);
        assert_eq!(state.path, "deleted");
        assert_eq!(state.buffer, b"private dirty bytes");
    }

    #[test]
    fn content_hash_conflict_requires_two_different_known_hashes() {
        assert!(content_hash_conflicts(Some("base"), Some("current")));
        assert!(!content_hash_conflicts(Some("same"), Some("same")));
        assert!(!content_hash_conflicts(None, Some("current")));
        assert!(!content_hash_conflicts(Some("base"), None));
        assert!(!content_hash_conflicts(None, None));
    }

    #[test]
    fn content_hash_for_bytes_matches_sha256_hex() {
        assert_eq!(
            content_hash_for_bytes(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn attrs_preserve_exact_mode_and_read_only_clears_only_write_bits() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let metadata = RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: 0,
            file_id: None,
            link_count: 1,
            link_target: None,
            content_hash: None,
            executable: true,
            mode: Some(0o3751),
            updated_at: None,
        };

        let writable = RemoteFuseFs::new(
            client.clone(),
            false,
            "test-scope",
            runtime.handle().clone(),
        );
        assert_eq!(
            writable.attr_for_path("exact", &metadata, false).perm,
            0o3751
        );

        let read_only = RemoteFuseFs::new(client, true, "test-scope", runtime.handle().clone());
        assert_eq!(
            read_only.attr_for_path("exact", &metadata, false).perm,
            0o3551
        );
    }

    #[test]
    fn fuse_creation_mode_is_already_kernel_umask_adjusted() {
        assert_eq!(
            creation_mode(0o750, 0o027),
            0o750,
            "FUSE_DONT_MASK is not requested, so userspace must not apply umask twice"
        );
    }

    #[test]
    fn path_readers_keep_committed_bytes_while_existing_file_is_replaced() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new(client, false, "test-scope", runtime.handle().clone());
        let path = "api/nym.toml";
        let committed = vec![b'a'; 64 * 1024];
        let committed_metadata = RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: committed.len() as u64,
            file_id: None,
            link_count: 1,
            link_target: None,
            content_hash: Some(content_hash_for_bytes(&committed)),
            executable: false,
            mode: Some(0o640),
            updated_at: None,
        };
        fs.cache
            .put_file(path, committed.clone(), Some(committed_metadata.clone()));

        let writer = fs
            .next_handle(
                path,
                Vec::new(),
                true,
                committed_metadata.content_hash.clone(),
                0o640,
                false,
                None,
                1,
            )
            .unwrap();
        {
            let mut handles = fs.lock_handles().unwrap();
            let state = handles.files.get_mut(&writer).unwrap();
            state.buffer = vec![b'b'; 2 * 1024];
            state.dirty = true;
            state.revision += 1;
        }

        assert_eq!(
            fs.stat_path(path).unwrap(),
            Some(committed_metadata.clone())
        );
        assert_eq!(
            fs.stat_path_attributes(path).unwrap(),
            Some(committed_metadata)
        );
        assert_eq!(
            fs.read_bytes(path, 0, committed.len() as u32).unwrap(),
            committed
        );

        let handles = fs.lock_handles().unwrap();
        assert_eq!(handles.files.get(&writer).unwrap().buffer.len(), 2 * 1024);
    }

    #[test]
    fn range_fingerprint_matches_gateway_epoch_millis_contract() {
        // Vector shared with ts/test/basic.test.cjs — both sides must produce
        // exactly this string for the same instant.
        let updated_at = chrono::DateTime::parse_from_rfc3339("2026-07-17T23:26:26.500Z")
            .expect("parse")
            .with_timezone(&chrono::Utc);
        let metadata = RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: 123,
            file_id: None,
            link_count: 1,
            link_target: None,
            content_hash: None,
            executable: false,
            mode: Some(0o640),
            updated_at: Some(updated_at),
        };
        assert_eq!(range_fingerprint(&metadata), "123:1784330786500");

        let unstamped = RemoteMetadata {
            updated_at: None,
            ..metadata
        };
        assert_eq!(range_fingerprint(&unstamped), "123:-1");
    }

    #[test]
    fn path_truncate_resizes_open_handle_buffer_instead_of_racing_the_journal() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new(client, false, "test-scope", runtime.handle().clone());
        let path = "api/nym.toml";
        let content = b"committed contents".to_vec();
        let writer = fs
            .next_handle(
                path,
                content.clone(),
                true,
                Some(content_hash_for_bytes(&content)),
                0o640,
                false,
                None,
                1,
            )
            .unwrap();

        // Must resolve entirely against the open handle — any network call
        // would error against the unroutable endpoint and fail the resize.
        let attr = fs.resize_path_immediate(path, 4).unwrap();
        assert_eq!(attr.size, 4);

        let handles = fs.lock_handles().unwrap();
        let state = handles.files.get(&writer).unwrap();
        assert_eq!(state.buffer, content[..4].to_vec());
        assert!(state.dirty, "resize must flow through the ordinary flush");
    }

    #[test]
    fn path_readers_can_see_an_uncommitted_new_file() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new(client, false, "test-scope", runtime.handle().clone());
        let path = "new-file.txt";
        let bytes = b"new file contents".to_vec();
        let writer = fs
            .next_handle(path, bytes.clone(), true, None, 0o640, true, None, 1)
            .unwrap();
        {
            let mut handles = fs.lock_handles().unwrap();
            let state = handles.files.get_mut(&writer).unwrap();
            state.dirty = true;
            state.revision += 1;
        }

        let metadata = fs.stat_path(path).unwrap().unwrap();
        assert_eq!(metadata.size_bytes, bytes.len() as u64);
        assert_eq!(metadata.content_hash, Some(content_hash_for_bytes(&bytes)));
        // Content must match the visible metadata: a reader that was shown the
        // created file's stat must get its bytes, not gateway ENOENT.
        assert_eq!(
            fs.read_bytes(path, 0, bytes.len() as u32).unwrap(),
            bytes,
            "path readers must be served a created file's uncommitted bytes"
        );
    }

    #[test]
    fn pathname_mutations_update_an_uncommitted_created_file_without_gateway_io() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new(client, false, "test-scope", runtime.handle().clone());
        let path = "git-meta/config.lock";
        let bytes = b"created contents".to_vec();
        let writer = fs
            .next_handle(path, bytes.clone(), true, None, 0o640, true, None, 1)
            .unwrap();
        {
            let mut handles = fs.lock_handles().unwrap();
            let state = handles.files.get_mut(&writer).unwrap();
            state.dirty = true;
            state.revision += 1;
        }

        // The endpoint is deliberately unroutable. A gateway stat/write would
        // fail this test; created-inode chmod/truncate must stay handle-local.
        let metadata = fs
            .mutate_created_handle_for_path(path, Some(7), Some(0o751))
            .unwrap()
            .unwrap();
        assert_eq!(metadata.size_bytes, 7);
        assert_eq!(metadata.mode, Some(0o751));
        assert!(metadata.executable);
        assert_eq!(
            metadata.content_hash,
            Some(content_hash_for_bytes(&bytes[..7]))
        );
        assert_eq!(fs.read_bytes(path, 0, 64).unwrap(), bytes[..7]);

        let handles = fs.lock_handles().unwrap();
        let state = handles.files.get(&writer).unwrap();
        assert_eq!(state.buffer, bytes[..7]);
        assert_eq!(state.mode, 0o751);
        assert!(state.dirty);
        assert!(state.loaded);
    }
}

/// FUSE operation bodies. Dispatched concurrently by `SpawnedFuseFs`
/// (fuse/dispatch.rs), which owns the actual `fuser::Filesystem` impl —
/// every method here may run on a worker thread and must stay `&self`-safe.
impl RemoteFuseFs {
    pub(super) fn init_op(&self, config: &mut KernelConfig) -> io::Result<()> {
        let requested = self.requested_init_capabilities();
        let available = config.capabilities();
        let supported = requested & available;
        let unsupported = requested & !available;
        if !supported.is_empty()
            && let Err(rejected) = config.add_capabilities(supported)
        {
            tracing::warn!(
                ?rejected,
                "vfs fuse kernel rejected capabilities it previously advertised"
            );
        }
        if !unsupported.is_empty() {
            tracing::warn!(
                ?unsupported,
                "vfs fuse kernel does not support requested capabilities"
            );
        }
        Ok(())
    }

    pub(super) fn forget(&self, ino: INodeNo, nlookup: u64) {
        if let Ok(mut inodes) = self.lock_inodes() {
            inodes.forget(ino, nlookup);
        }
    }

    pub(super) fn lookup(&self, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let result: FuseResult<FileAttr> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let child_path = Self::child_path(parent_path.as_str(), name)?;
            let Some(metadata) = self.stat_path_attributes(&child_path)? else {
                return Err(Errno::ENOENT);
            };
            Ok(self.attr_for_path(&child_path, &metadata, true))
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn getattr(&self, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        if ino == ROOT_INO {
            reply.attr(&TTL, &self.root_attr());
            return;
        }
        let result: FuseResult<FileAttr> = (|| {
            if let Some(fh) = fh
                && self.lock_handles()?.files.contains_key(&fh.0)
            {
                let gate = self.publication_gate_for_handle(fh.0)?;
                let _guard = gate.lock().map_err(|_| Errno::EIO)?;
                let route = self.resolve_handle_route_locked(fh.0)?;
                let state = self
                    .lock_handles()?
                    .files
                    .get(&fh.0)
                    .cloned()
                    .ok_or(Errno::ENOENT)?;
                let authoritative = match &route {
                    StableFileRoute::Linked(route) => Some(&route.metadata),
                    StableFileRoute::Unlinked => None,
                };
                let metadata = self.metadata_for_handle_state(&state, authoritative);
                return Ok(self.attr_for_metadata(ino, &metadata, true));
            }
            let route = self.resolve_inode_file_route(ino)?;
            Ok(self.attr_for_metadata(ino, &route.metadata, false))
        })();
        match result {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn readlink(&self, ino: INodeNo, reply: ReplyData) {
        let result: FuseResult<Vec<u8>> = (|| {
            let path = self.path_for_ino(ino)?;
            let metadata = self.stat_path_attributes(&path)?.ok_or(Errno::ENOENT)?;
            if metadata.kind != "symlink" {
                return Err(Errno::EINVAL);
            }
            let target = metadata.link_target.ok_or(Errno::EINVAL)?;
            Ok(target.into_bytes())
        })();
        match result {
            Ok(bytes) => reply.data(&bytes),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn setattr(
        &self,
        ino: INodeNo,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let result: FuseResult<FileAttr> = (|| {
            if ino == ROOT_INO {
                if size.is_some() {
                    return Err(Errno::EISDIR);
                }
                return Ok(self.root_attr());
            }

            if let Some(fh) = fh {
                if self.lock_handles()?.files.contains_key(&fh.0) {
                    let gate = self.publication_gate_for_handle(fh.0)?;
                    let _guard = gate.lock().map_err(|_| Errno::EIO)?;
                    let requested_mode = mode.map(normalize_mode);
                    if size.is_some() || requested_mode.is_some() {
                        self.ensure_handle_loaded_locked(fh.0)?;
                        let mut handles = self.lock_handles()?;
                        let state = handles.files.get_mut(&fh.0).ok_or(Errno::ENOENT)?;
                        if let Some(size) = size {
                            state.buffer.resize(size as usize, 0);
                        }
                        if let Some(mode) = requested_mode {
                            state.mode = mode;
                        }
                        state.dirty = true;
                        state.loaded = true;
                        state.revision = state.revision.saturating_add(1);
                        Self::mirror_handle_state_locked(&mut handles, fh.0)?;
                    }
                    // fchmod(2) is a publication point. Publish the handle
                    // through its stable identity instead of applying chmod
                    // to a pathname that another mount may have replaced.
                    if requested_mode.is_some() {
                        self.flush_handle_immediate_locked(fh.0)?;
                    }

                    let route = self.resolve_handle_route_locked(fh.0)?;
                    let state = {
                        let handles = self.lock_handles()?;
                        handles.files.get(&fh.0).cloned()
                    }
                    .ok_or(Errno::ENOENT)?;
                    let authoritative = match &route {
                        StableFileRoute::Linked(route) => Some(&route.metadata),
                        StableFileRoute::Unlinked => None,
                    };
                    let metadata = self.metadata_for_handle_state(&state, authoritative);
                    return Ok(self.attr_for_metadata(ino, &metadata, true));
                }
            }

            let path = self.path_for_ino(ino)?;
            if let Some(metadata) =
                self.mutate_created_handle_for_path(&path, size, mode.map(normalize_mode))?
            {
                return Ok(self.attr_for_path(&path, &metadata, false));
            }
            if let Some(size) = size {
                let mut attr = self.resize_path_immediate(&path, size)?;
                if let Some(mode) = mode {
                    attr.perm = self.set_mode_path_immediate(&path, mode)?.perm;
                }
                return Ok(attr);
            }
            if let Some(mode) = mode {
                return self.set_mode_path_immediate(&path, mode);
            }
            let metadata = self.stat_path(&path)?.ok_or(Errno::ENOENT)?;
            Ok(self.attr_for_path(&path, &metadata, false))
        })();
        match result {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn opendir(&self, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    pub(super) fn readdir(
        &self,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let result: FuseResult<()> = (|| {
            let path = self.path_for_ino(ino)?;
            let mut entries: Vec<(INodeNo, FileType, String)> = vec![
                (ino, FileType::Directory, ".".to_string()),
                (
                    self.ensure_ino(Self::parent_path(&path).as_str()),
                    FileType::Directory,
                    "..".to_string(),
                ),
            ];
            for entry in self.dir_entries(&path)? {
                let child_path = if path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{}/{}", path, entry.name)
                };
                let child_ino = self
                    .lock_inodes()
                    .map(|mut inodes| {
                        inodes.ensure_with_identity(&child_path, entry.file_id.as_deref())
                    })
                    .unwrap_or(ROOT_INO);
                let file_type = file_type_for_kind(&entry.kind);
                entries.push((child_ino, file_type, entry.name));
            }
            for (index, (entry_ino, kind, name)) in
                entries.into_iter().enumerate().skip(offset as usize)
            {
                if reply.add(entry_ino, (index + 1) as u64, kind, name) {
                    break;
                }
            }
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    /// One directory listing primes every child's inode and metadata cache
    /// entry, so a tree scan costs one wire call per directory instead of one
    /// per file (the kernel skips per-child lookup/getattr for entries
    /// returned here).
    pub(super) fn readdirplus(
        &self,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: fuser::ReplyDirectoryPlus,
    ) {
        let directory_metadata = || RemoteMetadata {
            kind: "directory".to_string(),
            size_bytes: 0,
            file_id: None,
            link_count: 2,
            link_target: None,
            content_hash: None,
            executable: false,
            mode: Some(0o755),
            updated_at: None,
        };
        let result: FuseResult<()> = (|| {
            let path = self.path_for_ino(ino)?;
            let parent_ino = self.ensure_ino(Self::parent_path(&path).as_str());
            let mut entries: Vec<(INodeNo, String, RemoteMetadata)> = vec![
                (ino, ".".to_string(), directory_metadata()),
                (parent_ino, "..".to_string(), directory_metadata()),
            ];
            for entry in self.dir_entries(&path)? {
                let child_path = if path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{}/{}", path, entry.name)
                };
                let metadata = RemoteMetadata {
                    kind: entry.kind,
                    size_bytes: entry.size_bytes,
                    file_id: entry.file_id,
                    link_count: entry.link_count,
                    link_target: entry.link_target,
                    content_hash: entry.content_hash,
                    executable: entry.executable,
                    mode: entry.mode,
                    updated_at: entry.updated_at,
                };
                let child_ino = self
                    .lock_inodes()
                    .map(|mut inodes| {
                        inodes.ensure_with_identity(&child_path, metadata.file_id.as_deref())
                    })
                    .unwrap_or(ROOT_INO);
                self.cache.put_metadata(&child_path, metadata.clone());
                entries.push((child_ino, entry.name, metadata));
            }
            for (index, (_entry_ino, name, metadata)) in
                entries.into_iter().enumerate().skip(offset as usize)
            {
                let dot_entry = name == "." || name == "..";
                let entry_path = if dot_entry {
                    path.clone()
                } else if path.is_empty() {
                    name.clone()
                } else {
                    format!("{}/{}", path, name)
                };
                // Children count as kernel lookups (a forget arrives for each
                // later); dot entries never do.
                let attr = self.attr_for_path(&entry_path, &metadata, !dot_entry);
                let entry_ino = attr.ino;
                if reply.add(
                    entry_ino,
                    (index + 1) as u64,
                    name,
                    &TTL,
                    &attr,
                    Generation(0),
                ) {
                    break;
                }
            }
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn open(&self, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let result: FuseResult<u64> = (|| {
            let route = self.resolve_inode_file_route(ino)?;
            let path = route.path;
            let metadata = route.metadata;
            if metadata.kind == "directory" {
                return Err(Errno::EISDIR);
            }
            let truncate = flags.0 & libc::O_TRUNC != 0;
            if truncate && self.read_only {
                return Err(Errno::EROFS);
            }
            let (initial, loaded, dirty) = if truncate {
                (Vec::new(), true, true)
            } else {
                self.cache
                    .get_file_matching(&path, &metadata)
                    .map(|bytes| (bytes, true, false))
                    .unwrap_or_else(|| (Vec::new(), false, false))
            };
            let base_content_hash = metadata.content_hash.clone();
            let fh = self.next_handle(
                &path,
                initial,
                loaded,
                base_content_hash,
                metadata_mode(&metadata),
                false,
                metadata.file_id.clone(),
                metadata.link_count,
            )?;
            if dirty {
                let mut handles = self.lock_handles()?;
                let state = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
                if truncate {
                    state.buffer.clear();
                    state.loaded = true;
                }
                state.dirty = true;
                state.revision = state.revision.saturating_add(1);
                Self::mirror_handle_state_locked(&mut handles, fh)?;
            }
            Ok(fh)
        })();
        match result {
            Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::empty()),
            Err(err) => {
                tracing::warn!(ino = ino.0, errno = ?err, "vfs open failed");
                reply.error(err);
            }
        }
    }

    pub(super) fn read(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let result: FuseResult<Vec<u8>> = (|| {
            if self.lock_handles()?.files.contains_key(&fh.0) {
                let gate = self.publication_gate_for_handle(fh.0)?;
                let _guard = gate.lock().map_err(|_| Errno::EIO)?;
                self.ensure_handle_loaded_locked(fh.0)?;
                let handles = self.lock_handles()?;
                let state = handles.files.get(&fh.0).ok_or(Errno::ENOENT)?;
                let start = (offset as usize).min(state.buffer.len());
                let end = start.saturating_add(size as usize).min(state.buffer.len());
                return Ok(state.buffer[start..end].to_vec());
            }
            let path = self.path_for_ino(ino)?;
            self.read_bytes(&path, offset, size)
        })();
        match result {
            Ok(bytes) => reply.data(&bytes),
            Err(err) => {
                tracing::warn!(
                    ino = ino.0,
                    fh = fh.0,
                    offset,
                    size,
                    errno = ?err,
                    "vfs read failed"
                );
                reply.error(err);
            }
        }
    }

    pub(super) fn write(
        &self,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }
        let result: FuseResult<u32> = (|| {
            let gate = self.publication_gate_for_handle(fh.0)?;
            let _guard = gate.lock().map_err(|_| Errno::EIO)?;
            self.ensure_handle_loaded_locked(fh.0)?;
            let mut handles = self.lock_handles()?;
            let state = handles.files.get_mut(&fh.0).ok_or(Errno::ENOENT)?;
            let start = offset as usize;
            if state.buffer.len() < start {
                state.buffer.resize(start, 0);
            }
            if state.buffer.len() < start + data.len() {
                state.buffer.resize(start + data.len(), 0);
            }
            state.buffer[start..start + data.len()].copy_from_slice(data);
            state.dirty = true;
            state.loaded = true;
            state.revision = state.revision.saturating_add(1);
            Self::mirror_handle_state_locked(&mut handles, fh.0)?;
            Ok(data.len() as u32)
        })();
        match result {
            Ok(written) => reply.written(written),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn flush(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        let flush_result = self.flush_handle_immediate(fh.0);
        let cleanup_result =
            self.release_advisory_lock_owner(ino, lock_owner, LockNamespace::Posix, Some(fh.0));
        match combine_flush_and_lock_cleanup("flush", flush_result, cleanup_result) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn fsync(&self, _ino: INodeNo, fh: FileHandle, _datasync: bool, reply: ReplyEmpty) {
        match self.flush_handle_immediate(fh.0) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn release(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let flush_result = match self.publication_gate_for_handle(fh.0) {
            Ok(gate) => match gate.lock() {
                Ok(_guard) => {
                    let result = self.flush_handle_immediate_locked(fh.0);
                    if result.is_ok() {
                        // Remove only after the exact publication and
                        // authoritative verification succeed. A failed close
                        // retains the handle, its WAL id, dirty bytes, and CAS
                        // base for a later flush/recovery attempt.
                        let _ = self
                            .lock_handles()
                            .map(|mut handles| handles.files.remove(&fh.0));
                    }
                    result
                }
                Err(_) => Err(Errno::EIO),
            },
            Err(error) => Err(error),
        };
        let cleanup_result = match lock_owner {
            Some(lock_owner) => {
                self.release_advisory_lock_owner(ino, lock_owner, LockNamespace::Flock, None)
            }
            None => Ok(()),
        };
        match combine_flush_and_lock_cleanup("release", flush_result, cleanup_result) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn getlk(
        &self,
        _ino: INodeNo,
        fh: FileHandle,
        lock_owner: fuser::LockOwner,
        namespace: LockNamespace,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: ReplyLock,
    ) {
        let result = (|| {
            let kind = Self::advisory_lock_kind(typ)?;
            if kind == "unlock" {
                return Err(Errno::EINVAL);
            }
            let target = self.advisory_lock_target(fh.0)?;
            let owner = self.advisory_lock_owner_key(lock_owner);
            let (start, end) = Self::advisory_lock_range(namespace, start, end);
            self.tokio
                .block_on(self.client.advisory_lock(
                    "get",
                    &target.path,
                    &self.mount_id,
                    &owner,
                    Self::advisory_lock_namespace(namespace),
                    start,
                    end,
                    kind,
                    pid,
                ))
                .map_err(|error| Self::advisory_lock_error(&error))
                .and_then(|response| {
                    if response.file_id.as_deref() == Some(target.file_id.as_str()) {
                        Ok(response)
                    } else {
                        Err(Errno::EIO)
                    }
                })
        })();
        match result {
            Ok(response) if response.acquired => {
                reply.locked(0, 0, i32::from(libc::F_UNLCK), 0);
            }
            Ok(response) => {
                let Some(conflict) = response.conflict else {
                    reply.error(Errno::EIO);
                    return;
                };
                let Ok(conflict_start) = conflict.start.parse::<u64>() else {
                    reply.error(Errno::EIO);
                    return;
                };
                let Ok(conflict_end) = conflict.end.parse::<u64>() else {
                    reply.error(Errno::EIO);
                    return;
                };
                let conflict_type = if conflict.kind == "read" {
                    i32::from(libc::F_RDLCK)
                } else {
                    i32::from(libc::F_WRLCK)
                };
                reply.locked(conflict_start, conflict_end, conflict_type, conflict.pid);
            }
            Err(error) => reply.error(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn setlk(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: fuser::LockOwner,
        namespace: LockNamespace,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        cancellation: Option<&LockWaitCancellation>,
        reply: ReplyEmpty,
    ) {
        let result = Self::advisory_lock_kind(typ).and_then(|_| {
            self.advisory_lock_target(fh.0).and_then(|target| {
                self.set_advisory_lock(
                    &target,
                    lock_owner,
                    namespace,
                    start,
                    end,
                    typ,
                    pid,
                    sleep,
                    cancellation,
                )
            })
        });
        if cancellation.is_some_and(|cancellation| !cancellation.finish()) {
            if let Ok(Some(file_id)) = result {
                let owner = self.advisory_lock_owner_key(lock_owner);
                if let Err(error) =
                    self.release_remote_advisory_lock_identity(&owner, &file_id, namespace)
                {
                    tracing::warn!(
                        ?error,
                        "failed to release advisory lock acquired by a cancelled waiter"
                    );
                }
            }
            reply.error(Errno::EINTR);
            return;
        }
        match result {
            Ok(file_id) => {
                if let Some(file_id) = file_id {
                    let owner_key = (namespace, self.advisory_lock_owner_key(lock_owner));
                    let inserted =
                        self.active_lock_owners
                            .lock()
                            .map_err(|_| Errno::EIO)
                            .map(|mut active| {
                                active
                                    .entry(owner_key)
                                    .or_default()
                                    .insert(ino.0, ActiveAdvisoryLockFile { file_id, fh: fh.0 });
                            });
                    if let Err(error) = inserted {
                        reply.error(error);
                        return;
                    }
                }
                reply.ok();
            }
            Err(error) => reply.error(error),
        }
    }

    pub(super) fn mkdir(
        &self,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let result: FuseResult<FileAttr> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), name)?;
            let mode = creation_mode(mode, umask);
            self.commit_namespace(VfsNamespaceMutation::CreateDirectory {
                path: path.clone(),
                mode: Some(mode),
            })?;
            self.cache.invalidate(&path);
            let metadata = RemoteMetadata {
                kind: "directory".to_string(),
                size_bytes: 0,
                file_id: None,
                link_count: 2,
                link_target: None,
                content_hash: None,
                executable: mode_is_executable(mode),
                mode: Some(mode),
                updated_at: None,
            };
            Ok(self.attr_for_path(&path, &metadata, true))
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn symlink(
        &self,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let result: FuseResult<FileAttr> = (|| {
            if self.read_only {
                return Err(Errno::EROFS);
            }
            let target = target.to_str().ok_or(Errno::EINVAL)?.to_string();
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), link_name)?;
            self.commit_namespace(VfsNamespaceMutation::CreateSymlink {
                path: path.clone(),
                target: target.clone(),
            })?;
            self.cache.invalidate(&path);
            let metadata = RemoteMetadata {
                kind: "symlink".to_string(),
                size_bytes: target.len() as u64,
                file_id: None,
                link_count: 1,
                link_target: Some(target),
                content_hash: None,
                executable: false,
                mode: Some(0o777),
                updated_at: None,
            };
            Ok(self.attr_for_path(&path, &metadata, true))
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn unlink(&self, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let result: FuseResult<()> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), name)?;
            // Publish pre-unlink dirty bytes while the pathname still exists.
            // Later writes on the open handle can then target an authoritative
            // surviving alias without resurrecting this name.
            self.flush_handles_for_path(&path)?;
            let metadata = self.stat_path_attributes(&path)?.ok_or(Errno::ENOENT)?;
            let file_id = metadata.file_id.clone();
            let mutation = VfsNamespaceMutation::DeleteFile {
                path: path.clone(),
                precondition: file_id.as_ref().map(|file_id| VfsWritePrecondition {
                    predicate: None,
                    fingerprint: None,
                    secondary_fingerprint: None,
                    expected_file_id: Some(file_id.clone()),
                }),
            };
            if let Err(error) = self.commit_namespace(mutation) {
                let completed = match file_id.as_deref() {
                    Some(file_id) => {
                        let current = match self.tokio.block_on(self.client.stat_attributes(&path))
                        {
                            Ok(current) => current,
                            Err(_) => return Err(error),
                        };
                        current.is_none_or(|current| current.file_id.as_deref() != Some(file_id))
                    }
                    None => false,
                };
                if !completed {
                    return Err(error);
                }
            }
            let surviving_route = match file_id.as_deref() {
                Some(file_id) => match self.authoritative_file_route(&path, file_id)? {
                    StableFileRoute::Linked(route) => Some(route),
                    StableFileRoute::Unlinked => None,
                },
                None => None,
            };
            self.cache.invalidate(&path);
            if let Ok(mut handles) = self.lock_handles() {
                for state in handles.files.values_mut().filter(|state| {
                    state.path == path
                        && file_id
                            .as_ref()
                            .is_none_or(|file_id| state.file_id.as_ref() == Some(file_id))
                }) {
                    if let Some(route) = surviving_route.as_ref() {
                        state.path = route.path.clone();
                        state.link_count = route.metadata.link_count.max(1);
                    } else {
                        state.unlinked = true;
                        state.link_count = 0;
                    }
                }
            }
            if let Some(route) = surviving_route.as_ref() {
                self.retarget_identity_route(&path, route);
            }
            self.detach_inode_path(&path);
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn rmdir(&self, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let result: FuseResult<()> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), name)?;
            self.commit_namespace(VfsNamespaceMutation::RemoveDirectory { path: path.clone() })?;
            self.cache.invalidate(&path);
            self.detach_inode_path(&path);
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn rename(
        &self,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let result: FuseResult<()> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let newparent_path = self.path_for_ino(newparent)?;
            let from = Self::child_path(parent_path.as_str(), name)?;
            let to = Self::child_path(newparent_path.as_str(), newname)?;
            if from == to {
                return Ok(());
            }
            let publication_gates = self.publication_gates_for_subtrees(&[&from, &to])?;
            let _publication_guards = Self::lock_publication_gates(&publication_gates)?;
            for (fh, _) in &publication_gates {
                self.flush_handle_locked(*fh)?;
            }
            self.flush_writes()?;
            self.flush_namespace()?;
            let source_metadata = self
                .tokio
                .block_on(self.client.stat(&from))
                .map_err(|_| Errno::EIO)?
                .ok_or(Errno::ENOENT)?;
            let replaced_metadata = self.stat_path_attributes(&to)?;
            if source_metadata.file_id.as_deref().is_some_and(|file_id| {
                replaced_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.file_id.as_deref())
                    == Some(file_id)
            }) {
                // POSIX rename between two hard-link aliases of the same
                // inode is a successful no-op; neither open alias is retired.
                return Ok(());
            }
            let lease = self
                .tokio
                .block_on(self.client.acquire_lease(&from, 1, "rename vfs fuse entry"))
                .map_err(|_| Errno::EIO)?;
            let surface = self.surface_kind_for_path(&to);
            let rename_result = self.tokio.block_on(self.client.rename(
                &from,
                &to,
                &lease,
                surface,
                VFS_OPERATION_RENAME,
            ));
            let completed = match rename_result {
                Ok(()) => true,
                Err(_) => {
                    let current_source = self
                        .tokio
                        .block_on(self.client.stat_attributes(&from))
                        .ok()
                        .flatten();
                    let current_destination = self
                        .tokio
                        .block_on(self.client.stat_attributes(&to))
                        .ok()
                        .flatten();
                    match source_metadata.file_id.as_deref() {
                        Some(file_id) => {
                            current_destination
                                .as_ref()
                                .and_then(|metadata| metadata.file_id.as_deref())
                                == Some(file_id)
                                && current_source
                                    .as_ref()
                                    .and_then(|metadata| metadata.file_id.as_deref())
                                    != Some(file_id)
                        }
                        None => {
                            current_source.is_none()
                                && current_destination
                                    .as_ref()
                                    .is_some_and(|metadata| metadata.kind == source_metadata.kind)
                        }
                    }
                }
            };
            let _ = self.tokio.block_on(self.client.release_lease(&lease));
            if !completed {
                return Err(Errno::EIO);
            }
            let replaced_route = match replaced_metadata
                .as_ref()
                .and_then(|metadata| metadata.file_id.as_deref())
            {
                Some(file_id) if source_metadata.file_id.as_deref() != Some(file_id) => {
                    match self.authoritative_file_route(&to, file_id)? {
                        StableFileRoute::Linked(route) => Some(route),
                        StableFileRoute::Unlinked => None,
                    }
                }
                _ => None,
            };
            if let Ok(mut handles) = self.lock_handles() {
                for state in handles.files.values_mut().filter(|state| state.path == to) {
                    if let Some(route) = replaced_route.as_ref() {
                        state.path = route.path.clone();
                        state.link_count = route.metadata.link_count.max(1);
                    } else {
                        state.unlinked = true;
                        state.link_count = 0;
                    }
                }
            }
            if let Some(route) = replaced_route.as_ref() {
                self.retarget_identity_route(&to, route);
            }
            self.rename_inode_path(&from, &to);
            self.cache.invalidate(&from);
            self.cache.invalidate(&to);
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn link(
        &self,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let result: FuseResult<FileAttr> = (|| {
            if self.read_only {
                return Err(Errno::EROFS);
            }
            let source_hint = self.path_for_ino(ino)?;
            let parent = self.path_for_ino(newparent)?;
            let destination = Self::child_path(parent.as_str(), newname)?;
            self.flush_handles_for_path(&source_hint)?;
            let source_route = self.resolve_inode_file_route(ino)?;
            let source = source_route.path;
            let source_metadata = source_route.metadata;
            if source_metadata.kind != "file" {
                return Err(Errno::EPERM);
            }
            let surface = self.surface_kind_for_path(&destination);
            let lease = self
                .tokio
                .block_on(
                    self.client
                        .acquire_lease(&destination, 1, "create vfs hard link"),
                )
                .map_err(|_| Errno::EIO)?;
            let response = (|| {
                if self
                    .tokio
                    .block_on(self.client.stat_attributes(&destination))
                    .map_err(|_| Errno::EIO)?
                    .is_some()
                {
                    return Err(Errno::EEXIST);
                }
                match self.tokio.block_on(self.client.create_hard_link(
                    &source,
                    &destination,
                    &lease,
                    surface,
                )) {
                    Ok(response) => Ok(response),
                    Err(_) => {
                        // Destination leases serialize contenders, so after
                        // the negative preflight an exact stable identity at
                        // the destination disambiguates our lost response.
                        let expected_file_id =
                            source_metadata.file_id.as_deref().ok_or(Errno::EIO)?;
                        let destination_metadata = self
                            .tokio
                            .block_on(self.client.stat(&destination))
                            .map_err(|_| Errno::EIO)?
                            .filter(|metadata| {
                                metadata.file_id.as_deref() == Some(expected_file_id)
                            })
                            .ok_or(Errno::EIO)?;
                        let source_metadata = self
                            .tokio
                            .block_on(self.client.stat(&source))
                            .map_err(|_| Errno::EIO)?
                            .filter(|metadata| {
                                metadata.file_id.as_deref() == Some(expected_file_id)
                            })
                            .unwrap_or_else(|| destination_metadata.clone());
                        Ok(chevalier_sandbox::vfs::VfsHardLinkMetadataResponse {
                            source: source_metadata,
                            destination: destination_metadata,
                        })
                    }
                }
            })();
            let _ = self.tokio.block_on(self.client.release_lease(&lease));
            let response = response?;
            if let Some(file_id) = response.source.file_id.as_deref() {
                self.cache.invalidate_identity(file_id);
                // A freshly created file first entered the inode table before
                // the gateway assigned its stable identity. Bind that existing
                // inode before allocating the destination attr so both names
                // are returned to the kernel as one hard-linked inode.
                self.lock_inodes()?
                    .ensure_with_identity(&source, Some(file_id));
            }
            self.cache.invalidate(&source);
            self.cache.invalidate(&destination);
            self.cache.put_metadata(&source, response.source);
            self.cache
                .put_metadata(&destination, response.destination.clone());
            if let Ok(mut handles) = self.lock_handles() {
                for state in handles.files.values_mut().filter(|state| {
                    state.path == source
                        || response
                            .destination
                            .file_id
                            .as_ref()
                            .is_some_and(|file_id| state.file_id.as_ref() == Some(file_id))
                }) {
                    state.file_id = response.destination.file_id.clone();
                    state.link_count = response.destination.link_count;
                }
            }
            Ok(self.attr_for_path(&destination, &response.destination, true))
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn create(
        &self,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let result: FuseResult<(FileAttr, u64)> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), name)?;
            let mode = creation_mode(mode, umask);
            let exclusive = flags & libc::O_EXCL != 0;
            let (metadata, reserved) = match self.reserve_file_if_absent(&path, mode) {
                Ok(metadata) => (metadata, true),
                Err(error) if !exclusive && error.code() == Errno::EEXIST.code() => {
                    let metadata = self
                        .tokio
                        .block_on(self.client.stat(&path))
                        .map_err(|_| Errno::EIO)?
                        .ok_or(Errno::EAGAIN)?;
                    if metadata.kind != "file" {
                        return Err(Errno::EEXIST);
                    }
                    (metadata, false)
                }
                Err(error) => return Err(error),
            };
            if reserved {
                self.cache
                    .put_file(&path, Vec::new(), Some(metadata.clone()));
            }
            let attr = self.attr_for_path(&path, &metadata, true);
            let truncate = !reserved && flags & libc::O_TRUNC != 0;
            let (initial, loaded) = if reserved || truncate {
                (Vec::new(), true)
            } else {
                self.cache
                    .get_file_matching(&path, &metadata)
                    .map(|bytes| (bytes, true))
                    .unwrap_or_else(|| (Vec::new(), false))
            };
            let fh = self.next_handle(
                &path,
                initial,
                loaded,
                metadata.content_hash.clone(),
                metadata_mode(&metadata),
                reserved,
                metadata.file_id.clone(),
                metadata.link_count,
            )?;
            if truncate
                && let Ok(mut handles) = self.lock_handles()
                && let Some(state) = handles.files.get_mut(&fh)
            {
                state.buffer.clear();
                state.loaded = true;
                state.dirty = true;
                state.revision = state.revision.saturating_add(1);
                let _ = Self::mirror_handle_state_locked(&mut handles, fh);
            }
            Ok((attr, fh))
        })();
        match result {
            Ok((attr, fh)) => reply.created(
                &TTL,
                &attr,
                Generation(0),
                FileHandle(fh),
                FopenFlags::empty(),
            ),
            Err(err) => reply.error(err),
        }
    }
}
