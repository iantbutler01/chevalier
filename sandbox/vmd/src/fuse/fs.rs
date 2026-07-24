use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use chevalier_sandbox::vfs::{
    VFS_OPERATION_SETATTR_SIZE, VFS_OPERATION_WRITE_THROUGH, VFS_SURFACE_KIND_VM_SHARED,
    VFS_SURFACE_KIND_VM_WORKSPACE, VfsDirEntry as RemoteDirEntry, VfsMetadata as RemoteMetadata,
    VfsNamespaceMutation, VfsWritePrecondition, scoped_vfs_path,
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

use super::cache::{
    KernelInvalidator, MountInvalidators, PublicationInvalidation, RemoteFuseCache,
};
use super::client::{
    AdvisoryLockRenewalIdentity, RangeRead, RemoteVfsClient, Versioned, request_status,
};
use super::namespace::{NamespaceJournal, NamespaceProjection};
use super::write::{WriteBarrierGuard, WriteJournal};

/// Positive attribute/entry lease handed to the kernel for a warm metadata hit.
///
/// This is a *liveness-bounded lease*, never a correctness boundary. While the
/// revision watch is live, every publication — local (a sibling mount's commit
/// hook) or remote (observed by the watch) — revokes the affected kernel
/// entries via the fuser notifier *before* it is acked (the revocation-ack
/// ordering invariant in `run_revision_watch` and the commit hooks). So the
/// kernel can never serve an attr the coherence stack has superseded: any lease
/// still alive is for a path no publication has touched since it was granted.
///
/// The lease therefore bounds only the window *after* the watch drops: replies
/// then carry `Duration::ZERO` (strict — every lstat/getattr crosses into
/// userspace and reconfirms), and any entry already leased in the kernel
/// naturally expires within this bound. `lease_ttl_for` selects between the two
/// at reply time based on watch liveness.
pub(super) const ATTR_ENTRY_LEASE_TTL: Duration = Duration::from_secs(1);
/// Largest untargeted kernel revocation this mount will issue in one sweep.
///
/// `notify_inval_entry` acquires the parent inode's write lock in the guest
/// kernel, so a sweep proportional to the whole working set blocks the guest's
/// own lookups for as long as it runs. Past this bound the mount declines the
/// sweep and falls back to strict serving until its leases expire — bounded
/// staleness-free degradation instead of a mount-wide stall.
const KERNEL_SWEEP_MAX_TARGETS: usize = 128;
const ROOT_INO_RAW: u64 = 1;
const ROOT_INO: INodeNo = INodeNo(ROOT_INO_RAW);
const LARGE_FILE_BYTES: u64 = 10 * 1024 * 1024;
const MAX_OPEN_HANDLES: usize = 8_192;
const MAX_METADATA_BATCH_PATHS: usize = 4_096;
const MAX_SUBTREE_METADATA_ENTRIES: i64 = 4_096;
const MAX_SUBTREE_PREFETCH_BYTES: u64 = 64 * 1024 * 1024;
const ADVISORY_LOCK_RETRY_DELAY: Duration = Duration::from_millis(50);
const ADVISORY_LOCK_BLOCK_TIMEOUT: Duration = Duration::from_secs(30);
/// Emit one WARN if a blocking advisory-lock acquisition parks past this before
/// the hard timeout, surfacing an intermittent upstream stall without per-poll
/// spam.
const SLOW_ADVISORY_LOCK_WARN_AFTER: Duration = Duration::from_secs(10);
const ADVISORY_LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const POSIX_MODE_MASK: u32 = 0o7777;

type FuseResult<T> = std::result::Result<T, Errno>;

/// Separate VMs have separate kernel page caches and there is no
/// cross-kernel invalidation channel. Keep file data caching in the
/// revision-aware FUSE layer so every handle read can revalidate the remote
/// stable identity and content hash.
fn remote_file_open_flags() -> FopenFlags {
    FopenFlags::FOPEN_DIRECT_IO
}

/// Select the attribute/entry TTL for a reply. A positive lease
/// (`ATTR_ENTRY_LEASE_TTL`) is handed to the kernel only while the registry's
/// revision watch is live at reply time; with the watch down the reply fails
/// closed to `Duration::ZERO`, so every subsequent lstat/getattr crosses into
/// userspace and reconfirms against the (now unconfirmed) coherence fence.
fn lease_ttl_for(watch_live: bool) -> Duration {
    if watch_live {
        ATTR_ENTRY_LEASE_TTL
    } else {
        Duration::ZERO
    }
}

fn metadata_from_dir_entry(entry: &RemoteDirEntry) -> RemoteMetadata {
    RemoteMetadata {
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

/// A namespace projection is mount-local read-your-writes state. Only an
/// unprojected server response may be published into the cache shared by
/// sibling mounts, and callers must tag that publication with the exact
/// revision carried by the response.
fn publish_authoritative_projection<T>(
    projection: &NamespaceProjection<T>,
    publish: impl FnOnce(&T),
) {
    if !projection.applied {
        publish(&projection.value);
    }
}

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
                let had_linked_path = !record.paths.is_empty();
                record.paths.insert(path.to_string());
                if record.path.is_empty() || !had_linked_path {
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

    fn detach_unlinked_identity(&mut self, identity: &str) -> bool {
        let Some(ino) = self.identity_to_ino.get(identity).copied() else {
            return false;
        };
        let Some(record) = self.ino_to_path.get_mut(&ino) else {
            return false;
        };
        if record.identity.as_deref() != Some(identity) {
            return false;
        }
        let paths = std::mem::take(&mut record.paths);
        for path in paths {
            if self.path_to_ino.get(path.as_str()) == Some(&ino) {
                self.path_to_ino.remove(path.as_str());
            }
        }
        self.identity_to_ino.remove(identity);
        true
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

    /// Kernel-cache invalidation target for one exact path. Read-only and
    /// allocation-free: it never mints an inode for a path the kernel never
    /// looked up here, so it returns `None` when this mount handed the kernel
    /// neither the path's inode nor its parent directory (there is nothing to
    /// drop). `ino` drives `notify_inval_inode` (attributes); `parent` + `name`
    /// drive `notify_inval_entry` (the dentry, positive or negative).
    fn invalidation_target(&self, path: &str) -> Option<KernelInvalTarget> {
        let path = path.trim_matches('/');
        let ino = self.path_to_ino.get(path).copied();
        let Some((parent, name)) = split_parent_and_leaf(path) else {
            // The scope root itself: attribute-only, it has no parent dentry.
            return ino.map(|ino| KernelInvalTarget {
                ino: Some(ino),
                parent: None,
                name: OsString::new(),
            });
        };
        let parent_ino = self.path_to_ino.get(parent).copied();
        if ino.is_none() && parent_ino.is_none() {
            return None;
        }
        Some(KernelInvalTarget {
            ino,
            parent: parent_ino,
            name: OsString::from(name),
        })
    }

    /// Invalidation targets for a directory and every descendant the kernel
    /// cached under it, for a subtree-wide change (rmdir / rename of a
    /// directory). The `path_to_ino` `BTreeMap` gives an ordered prefix scan.
    fn subtree_invalidation_targets(&self, prefix: &str) -> Vec<KernelInvalTarget> {
        let prefix = prefix.trim_matches('/');
        let mut targets = Vec::new();
        if let Some(target) = self.invalidation_target(prefix) {
            targets.push(target);
        }
        if prefix.is_empty() {
            // An empty prefix is the whole tree; a full sweep owns that case.
            return targets;
        }
        let child_prefix = format!("{prefix}/");
        for path in self.path_to_ino.range(child_prefix.clone()..) {
            if !path.0.starts_with(&child_prefix) {
                break;
            }
            if let Some(target) = self.invalidation_target(path.0) {
                targets.push(target);
            }
        }
        targets
    }

    /// Identity (hard-link) invalidation: drop the shared inode's attributes and
    /// every cached alias dentry for a stable identity the publication changed.
    /// Reaches aliases the kernel cached under a name other than the written
    /// path — they share one inode via `identity_to_ino`.
    fn identity_invalidation_targets(&self, file_id: &str) -> Vec<KernelInvalTarget> {
        let Some(ino) = self.identity_to_ino.get(file_id).copied() else {
            return Vec::new();
        };
        let Some(record) = self.ino_to_path.get(&ino) else {
            return Vec::new();
        };
        let mut targets = vec![KernelInvalTarget {
            ino: Some(ino),
            parent: None,
            name: OsString::new(),
        }];
        for alias in &record.paths {
            if let Some((parent, name)) = split_parent_and_leaf(alias.as_str())
                && let Some(parent_ino) = self.path_to_ino.get(parent).copied()
            {
                targets.push(KernelInvalTarget {
                    ino: None,
                    parent: Some(parent_ino),
                    name: OsString::from(name),
                });
            }
        }
        targets
    }

    /// Every attribute/dentry this mount handed the kernel, for a full sweep on
    /// a remote publication whose exact path set this process never learned.
    fn all_invalidation_targets(&self) -> Vec<KernelInvalTarget> {
        self.path_to_ino
            .keys()
            .filter_map(|path| self.invalidation_target(path))
            .collect()
    }
}

/// One path's kernel-cache invalidation target. `ino` (when present) drives an
/// attribute invalidation; `parent` + `name` (when present) drive a dentry
/// invalidation, which drops positive *and* negative cache entries for the name.
struct KernelInvalTarget {
    ino: Option<INodeNo>,
    parent: Option<INodeNo>,
    name: OsString,
}

/// Split a scope-relative VFS path into its parent directory and leaf name.
/// Returns `None` for the scope root (which has no parent dentry). A top-level
/// name yields `("", name)` — the empty parent is the scope root inode.
fn split_parent_and_leaf(path: &str) -> Option<(&str, &str)> {
    let path = path.trim_matches('/');
    if path.is_empty() {
        return None;
    }
    Some(match path.rsplit_once('/') {
        Some((parent, leaf)) => (parent, leaf),
        None => ("", path),
    })
}

/// One mount's implementation of the shared `KernelInvalidator` hook. Holds this
/// mount's fuser notifier (a cloneable `Send + Sync` handle onto its `/dev/fuse`
/// session) and a `Weak` into its inode table, so it can resolve a publication's
/// affected paths to the exact inodes/dentries this mount handed the kernel and
/// revoke them. Notifier calls originate from the watch task and journal-worker
/// threads; `Notifier` is `Send + Sync`, so no extra wrapping is required.
pub(super) struct MountKernelInvalidator {
    notifier: fuser::Notifier,
    inodes: Weak<Mutex<InodeTable>>,
    /// Edge-detects notify failures so an operator-facing WARN fires once per
    /// failing transition, not once per path.
    warned: AtomicBool,
}

impl MountKernelInvalidator {
    /// Fire the notifier for each target, resolving the inode-table lock first
    /// and dropping it before any `writev` into `/dev/fuse`.
    fn apply(&self, targets: &[KernelInvalTarget]) -> bool {
        let mut clean = true;
        for target in targets {
            if let Some(ino) = target.ino
                && let Err(error) = self.notifier.inval_inode(ino, 0, 0)
            {
                clean = false;
                self.warn_once(&error);
            }
            if let Some(parent) = target.parent
                && let Err(error) = self.notifier.inval_entry(parent, target.name.as_os_str())
            {
                clean = false;
                self.warn_once(&error);
            }
        }
        if clean {
            // A fully clean sweep re-arms the edge so a later failure warns again.
            self.warned.store(false, Ordering::Release);
        }
        clean
    }

    fn warn_sweep_declined(&self, targets: usize) {
        if !self.warned.swap(true, Ordering::AcqRel) {
            tracing::warn!(
                targets,
                limit = KERNEL_SWEEP_MAX_TARGETS,
                "vfs kernel sweep declined; serving strict until leases expire"
            );
        }
    }

    fn warn_once(&self, error: &io::Error) {
        // `notify_inval_*` already swallow ENOENT internally (the kernel had
        // already dropped the cached entry), so anything reaching here is a
        // genuine channel error (e.g. the session's fd closing on unmount).
        if !self.warned.swap(true, Ordering::AcqRel) {
            tracing::warn!(%error, "vfs fuse kernel cache invalidation failed");
        }
    }
}

impl KernelInvalidator for MountKernelInvalidator {
    fn invalidate(&self, invalidation: &PublicationInvalidation) -> bool {
        let Some(inodes) = self.inodes.upgrade() else {
            // The mount is gone; it holds no kernel state to revoke.
            return true;
        };
        let targets = {
            let table = inodes.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut targets = Vec::new();
            for path in &invalidation.paths {
                if let Some(target) = table.invalidation_target(path) {
                    targets.push(target);
                }
            }
            for prefix in &invalidation.subtrees {
                targets.extend(table.subtree_invalidation_targets(prefix));
            }
            for identity in &invalidation.identities {
                targets.extend(table.identity_invalidation_targets(identity));
            }
            targets
        };
        self.apply(&targets)
    }

    fn invalidate_all(&self) -> bool {
        let Some(inodes) = self.inodes.upgrade() else {
            return true;
        };
        let targets = {
            let table = inodes.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            table.all_invalidation_targets()
        };
        // A remote publication carries no path set, so the only fully accurate
        // revocation is every entry this mount handed the kernel. That is safe
        // when the working set is small and actively harmful when it is not:
        // `notify_inval_entry` takes the parent inode's write lock in the guest
        // kernel, so sweeping thousands of dentries stalls the guest's own
        // lookups behind the storm — a mount-wide freeze, not a fast revocation.
        //
        // Past the bound, decline the sweep instead. Returning `false` routes
        // the caller down the same fail-closed path a failed revocation takes:
        // watch liveness drops (replies revert to TTL=0, every serve becomes
        // wire-backed) and the ack is withheld until the leases granted before
        // this point have expired on their own. Correctness is preserved by
        // strictness rather than by a storm the guest cannot absorb.
        if targets.len() > KERNEL_SWEEP_MAX_TARGETS {
            self.warn_sweep_declined(targets.len());
            return false;
        }
        self.apply(&targets)
    }
}

/// Captured before the fs is moved into its FUSE session, this defers binding
/// the kernel notifier (which only exists after `spawn_mount2`) into the shared
/// invalidator registry. `install` returns the strong handle the mount owner
/// keeps for the mount's lifetime; dropping it deregisters the mount.
pub(super) struct KernelInvalidationRegistrar {
    invalidators: Arc<MountInvalidators>,
    inodes: Arc<Mutex<InodeTable>>,
}

impl KernelInvalidationRegistrar {
    pub(super) fn install(self, notifier: fuser::Notifier) -> Arc<MountKernelInvalidator> {
        let invalidator = Arc::new(MountKernelInvalidator {
            notifier,
            inodes: Arc::downgrade(&self.inodes),
            warned: AtomicBool::new(false),
        });
        let handle: Arc<dyn KernelInvalidator> = invalidator.clone();
        self.invalidators.register(Arc::downgrade(&handle));
        invalidator
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
    /// Exact gateway revision that verified `buffer` for a clean handle.
    /// Repeated direct-I/O reads may reuse the buffer only while the shared
    /// mount coherence revision remains equal to this token.
    loaded_coherence_revision: u64,
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
    revision: u64,
}

enum StableFileRoute {
    Linked(LinkedFileRoute),
    Unlinked,
}

struct AdvisoryLockTarget {
    path: String,
    file_id: String,
}

type MetadataBatchResult = std::result::Result<Versioned<Option<RemoteMetadata>>, String>;

struct MetadataBatchWaiter {
    path: String,
    result: Mutex<Option<MetadataBatchResult>>,
    ready: Condvar,
}

#[derive(Default)]
struct MetadataBatchState {
    pending: Vec<Arc<MetadataBatchWaiter>>,
    flushing: bool,
}

#[derive(Default)]
struct MetadataBatcher {
    state: Mutex<MetadataBatchState>,
}

impl MetadataBatcher {
    fn stat_attributes(
        &self,
        client: &RemoteVfsClient,
        tokio: &Handle,
        path: &str,
    ) -> MetadataBatchResult {
        let waiter = Arc::new(MetadataBatchWaiter {
            path: path.to_string(),
            result: Mutex::new(None),
            ready: Condvar::new(),
        });
        let leader = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "VFS metadata batch lock poisoned".to_string())?;
            state.pending.push(Arc::clone(&waiter));
            if state.flushing {
                false
            } else {
                state.flushing = true;
                true
            }
        };
        if leader {
            self.flush(client, tokio);
        }
        let mut result = waiter
            .result
            .lock()
            .map_err(|_| "VFS metadata batch result lock poisoned".to_string())?;
        while result.is_none() {
            result = waiter
                .ready
                .wait(result)
                .map_err(|_| "VFS metadata batch result lock poisoned".to_string())?;
        }
        result
            .take()
            .ok_or_else(|| "VFS metadata batch result missing".to_string())?
    }

    fn flush(&self, client: &RemoteVfsClient, tokio: &Handle) {
        loop {
            let requests = {
                let mut state = match self.state.lock() {
                    Ok(state) => state,
                    Err(poisoned) => poisoned.into_inner(),
                };
                let take = state.pending.len().min(MAX_METADATA_BATCH_PATHS);
                state.pending.drain(..take).collect::<Vec<_>>()
            };
            if requests.is_empty() {
                let mut state = match self.state.lock() {
                    Ok(state) => state,
                    Err(poisoned) => poisoned.into_inner(),
                };
                state.flushing = false;
                if state.pending.is_empty() {
                    return;
                }
                state.flushing = true;
                continue;
            }

            let mut unique_paths = Vec::new();
            let mut path_indexes = HashMap::<String, usize>::new();
            let mut request_indexes = Vec::with_capacity(requests.len());
            for request in &requests {
                let index = match path_indexes.get(&request.path) {
                    Some(index) => *index,
                    None => {
                        let index = unique_paths.len();
                        unique_paths.push(request.path.clone());
                        path_indexes.insert(request.path.clone(), index);
                        index
                    }
                };
                request_indexes.push(index);
            }
            let response = tokio
                .block_on(client.metadata_many_attributes_versioned(&unique_paths))
                .map_err(|error| error.to_string())
                .and_then(|response| {
                    if response.value.len() == unique_paths.len() {
                        Ok(response)
                    } else {
                        Err(format!(
                            "VFS metadata-many returned {} entries for {} paths",
                            response.value.len(),
                            unique_paths.len()
                        ))
                    }
                });
            for (request, index) in requests.into_iter().zip(request_indexes) {
                let result = match &response {
                    Ok(response) => Ok(Versioned {
                        value: response.value[index].clone(),
                        revision: response.revision,
                    }),
                    Err(error) => Err(error.clone()),
                };
                if let Ok(mut slot) = request.result.lock() {
                    *slot = Some(result);
                    request.ready.notify_one();
                }
            }
        }
    }
}

pub struct RemoteFuseFs {
    client: RemoteVfsClient,
    cache: std::sync::Arc<RemoteFuseCache>,
    metadata_batcher: MetadataBatcher,
    // `Arc` so the post-mount kernel invalidator (see `MountKernelInvalidator`)
    // can hold a `Weak` into this exact table and resolve affected paths to the
    // inodes this mount handed its kernel, without keeping the fs alive.
    inodes: Arc<Mutex<InodeTable>>,
    handles: Mutex<HandleTable>,
    namespace: Option<NamespaceJournal>,
    namespace_publication_gate: Mutex<()>,
    writes: Option<WriteJournal>,
    read_only: bool,
    scope_path: String,
    mount_id: String,
    active_lock_owners: std::sync::Arc<Mutex<ActiveAdvisoryLocks>>,
    // Shared per-registry set of every sibling mount's kernel-invalidation hook.
    // The commit hooks fan out over it on a local publication; the revision
    // watch fans out over it on a remote one. Held here so the watch's `Weak`
    // stays upgradeable for this mount's lifetime.
    invalidators: Arc<MountInvalidators>,
    tokio: Handle,
    uid: u32,
    gid: u32,
}

impl RemoteFuseFs {
    pub fn new(client: RemoteVfsClient, read_only: bool, scope_path: &str, tokio: Handle) -> Self {
        let (mount_id, active_lock_owners) = Self::start_lock_heartbeat(&client, &tokio);
        let cache = RemoteFuseCache::shared(&client.coherence_key());
        let invalidators = MountInvalidators::shared(&client.coherence_key());
        client.ensure_revision_watch(&tokio, &cache, &invalidators);
        Self {
            client,
            cache,
            metadata_batcher: MetadataBatcher::default(),
            inodes: Arc::new(Mutex::new(InodeTable::new())),
            handles: Mutex::new(HandleTable::default()),
            namespace: None,
            namespace_publication_gate: Mutex::new(()),
            writes: None,
            read_only,
            scope_path: scope_path.trim_matches('/').to_string(),
            mount_id,
            active_lock_owners,
            invalidators,
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
        let cache = RemoteFuseCache::shared(&client.coherence_key());
        let invalidators = MountInvalidators::shared(&client.coherence_key());
        client.ensure_revision_watch(&tokio, &cache, &invalidators);
        // The write journal is opened first so the namespace journal's recovery
        // worker can hold a drain handle onto it (see NamespaceJournal recovery:
        // it re-drains pending writes before re-issuing a conflicted deletion).
        let writes = if read_only {
            None
        } else {
            let dead_letter_cache = std::sync::Arc::clone(&cache);
            let commit_cache = std::sync::Arc::clone(&cache);
            let commit_invalidators = Arc::clone(&invalidators);
            Some(WriteJournal::open_with_commit_hook(
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
                Some(Box::new(move |revision, writes, entries| {
                    let invalidation =
                        commit_cache.observe_write_publication_snapshot(revision, writes, entries);
                    // Ordering invariant (local publication): the kernel drop
                    // completes before this hook returns — i.e. before the
                    // writer's publish RPC returns — so a same-process sibling
                    // observer's kernel never serves an attr the shared cache was
                    // just cleared of.
                    commit_invalidators.invalidate(&invalidation);
                })),
            )?)
        };
        let namespace = if read_only {
            None
        } else {
            let commit_cache = std::sync::Arc::clone(&cache);
            let commit_invalidators = Arc::clone(&invalidators);
            let write_drain = writes.as_ref().map(|writes| writes.drain_handle());
            Some(NamespaceJournal::open_with_commit_hook(
                client.clone(),
                scope_path,
                journal_path,
                tokio.clone(),
                Some(Box::new(move |revision, mutations, entries| {
                    let invalidation = commit_cache
                        .observe_namespace_publication_snapshot(revision, mutations, entries);
                    commit_invalidators.invalidate(&invalidation);
                })),
                write_drain,
            )?)
        };
        let (mount_id, active_lock_owners) = Self::start_lock_heartbeat(&client, &tokio);
        Ok(Self {
            client,
            cache,
            metadata_batcher: MetadataBatcher::default(),
            inodes: Arc::new(Mutex::new(InodeTable::new())),
            handles: Mutex::new(HandleTable::default()),
            namespace,
            namespace_publication_gate: Mutex::new(()),
            writes,
            read_only,
            scope_path: scope_path.trim_matches('/').to_string(),
            mount_id,
            active_lock_owners,
            invalidators,
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
        self.authoritative_file_route_with_metadata(path, file_id, false)
    }

    fn authoritative_file_route_attributes(
        &self,
        path: &str,
        file_id: &str,
    ) -> FuseResult<StableFileRoute> {
        self.authoritative_file_route_with_metadata(path, file_id, true)
    }

    fn authoritative_file_route_with_metadata(
        &self,
        path: &str,
        file_id: &str,
        attributes_only: bool,
    ) -> FuseResult<StableFileRoute> {
        let current = self
            .tokio
            .block_on(async {
                if attributes_only {
                    self.client.stat_attributes_versioned(path).await
                } else {
                    self.client.stat_versioned(path).await
                }
            })
            .map_err(|error| {
                tracing::warn!(path, file_id, error = %error, "vfs identity stat failed");
                Errno::EIO
            })?;
        if current
            .value
            .as_ref()
            .and_then(|metadata| metadata.file_id.as_deref())
            == Some(file_id)
        {
            return Ok(StableFileRoute::Linked(LinkedFileRoute {
                path: path.to_string(),
                metadata: current.value.expect("matching metadata exists"),
                revision: current.revision,
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
            .block_on(async {
                if attributes_only {
                    self.client.stat_attributes_versioned(&alias).await
                } else {
                    self.client.stat_versioned(&alias).await
                }
            })
            .map_err(|error| {
                tracing::warn!(
                    path = alias,
                    file_id,
                    error = %error,
                    "vfs hard-link alias stat failed"
                );
                Errno::EIO
            })?;
        let revision = metadata.revision;
        let metadata = metadata.value.ok_or(Errno::EAGAIN)?;
        if metadata.file_id.as_deref() != Some(file_id) {
            return Err(Errno::EAGAIN);
        }
        Ok(StableFileRoute::Linked(LinkedFileRoute {
            path: alias,
            metadata,
            revision,
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
        self.resolve_inode_file_route_with_metadata(ino, false)
    }

    fn resolve_inode_file_route_attributes(&self, ino: INodeNo) -> FuseResult<LinkedFileRoute> {
        self.resolve_inode_file_route_with_metadata(ino, true)
    }

    fn resolve_inode_file_route_with_metadata(
        &self,
        ino: INodeNo,
        attributes_only: bool,
    ) -> FuseResult<LinkedFileRoute> {
        let (path, identity) = self.inode_route(ino)?;
        let projected = if attributes_only {
            self.stat_path_attributes(&path)?
        } else {
            self.stat_path(&path)?
        };
        let Some(identity) = identity else {
            let metadata = projected.ok_or(Errno::ENOENT)?;
            let resolved_ino = self
                .lock_inodes()?
                .ensure_with_identity(&path, metadata.file_id.as_deref());
            if resolved_ino != ino {
                return Err(Errno::ENOENT);
            }
            return Ok(LinkedFileRoute {
                path,
                metadata,
                revision: 0,
            });
        };
        if projected
            .as_ref()
            .and_then(|metadata| metadata.file_id.as_deref())
            == Some(identity.as_str())
        {
            return Ok(LinkedFileRoute {
                path,
                metadata: projected.expect("matching projected metadata exists"),
                revision: 0,
            });
        }
        let route = if attributes_only {
            self.authoritative_file_route_attributes(&path, &identity)?
        } else {
            self.authoritative_file_route(&path, &identity)?
        };
        match route {
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

    /// The attribute/entry TTL to hand the kernel for a reply, chosen from live
    /// watch state at reply time (see `ATTR_ENTRY_LEASE_TTL`).
    fn reply_ttl(&self) -> Duration {
        lease_ttl_for(self.client.revision_watch_live())
    }

    /// Capture the handles the post-mount kernel-notifier install needs, before
    /// this fs is moved into its FUSE session. Called from mount setup while the
    /// fs is still reachable (see `handle::mount_remote_vfs_fuse`).
    pub(super) fn kernel_invalidation_registrar(&self) -> KernelInvalidationRegistrar {
        KernelInvalidationRegistrar {
            invalidators: Arc::clone(&self.invalidators),
            inodes: Arc::clone(&self.inodes),
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
            .get_metadata(path, self.client.coherence_revision())
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
        // No namespace barrier here. Read-your-writes is the journal
        // projection, not a publication wait: `project_directory` merges every
        // queued mutation kind over the authoritative listing below, so
        // draining the journal would only add a gateway round trip to reads
        // that already observe this mount's own pending mutations.
        self.assert_namespace_journal_healthy()?;
        // While a live revision watch keeps this mount's coherence fence
        // continuously confirmed, a fence-matched cached listing is valid to
        // serve without a wire round-trip. With the watch down we fail closed:
        // the fence only advances on a wire call, so an idle observer that
        // skipped the fetch could serve a listing a sibling already superseded.
        if self.client.revision_watch_live()
            && let Some(entries) = self.cache.get_dir(path, self.client.coherence_revision())
        {
            return Ok(entries);
        }
        let directory_generation = self.cache.directory_generation(path);
        // With the watch down a directory listing is always fetched over the
        // wire, never served from get_dir. This mount's coherence fence only
        // advances when it makes a wire call, so an idle observer that skipped
        // the fetch would serve a revision-fenced listing a sibling has already
        // superseded. The wire fetch here advances the fence at the start of
        // every tree walk. list_dir returns one coherent authoritative snapshot;
        // concurrent namespace changes may make that snapshot immediately old,
        // which is normal readdir behavior and must not turn a successful
        // listing into EAGAIN. The fresh listing is still published back into
        // the cache below (revision-fenced) for other consumers.
        let response = self
            .tokio
            .block_on(self.client.list_dir_versioned(path))
            .map_err(|_| Errno::EIO)?;
        let Some(entries) = response.value else {
            if let Some(namespace) = self.namespace.as_ref() {
                namespace
                    .observe_server_revision(response.revision)
                    .map_err(|_| Errno::EIO)?;
            }
            return Err(Errno::ENOENT);
        };
        let projection = match self.namespace.as_ref() {
            Some(namespace) => namespace
                .project_directory(path, entries, response.revision)
                .map_err(|_| Errno::EIO),
            None => Ok(NamespaceProjection {
                value: entries,
                applied: false,
            }),
        }?;
        publish_authoritative_projection(&projection, |entries| {
            let _ = self.cache.put_dir_if_generation(
                path,
                directory_generation,
                entries.clone(),
                response.revision,
            );
            for entry in entries {
                let child_path = if path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{path}/{}", entry.name)
                };
                self.cache.put_metadata(
                    &child_path,
                    metadata_from_dir_entry(entry),
                    response.revision,
                );
            }
        });
        Ok(projection.value)
    }

    fn stat_path(&self, path: &str) -> FuseResult<Option<RemoteMetadata>> {
        // See `dir_entries`: `project_metadata` supplies read-your-writes, so
        // reads never wait on a publication.
        self.assert_namespace_journal_healthy()?;
        if let Some(metadata) = self.open_handle_metadata(path)? {
            return Ok(Some(metadata));
        }
        if let Some(metadata) = self.dirty_handle_committed_metadata(path)? {
            return Ok(Some(metadata));
        }
        // A get_metadata or is_known_missing hit may not be returned on its
        // own: this mount's revision fence only advances on a wire call, so an
        // idle observer's fenced cache (or fenced negative entry) can be stale
        // relative to a sibling's write. The point stat below is the wire
        // round-trip that revalidates within this dispatch and reprimes the
        // cache. Read-your-writes was already served above from the open/dirty
        // handle; namespace projection is applied to the wire response.
        if self
            .writes
            .as_ref()
            .is_some_and(|writes| writes.has_pending_path(path))
        {
            self.flush_writes()?;
        }
        let response = self
            .tokio
            .block_on(self.client.stat_versioned(path))
            .map_err(|_| Errno::EIO)?;
        let metadata = response.value;
        let projection = if let Some(namespace) = self.namespace.as_ref() {
            namespace
                .project_metadata(path, metadata, response.revision)
                .map_err(|_| Errno::EIO)?
        } else {
            NamespaceProjection {
                value: metadata,
                applied: false,
            }
        };
        publish_authoritative_projection(&projection, |metadata| {
            if let Some(metadata) = metadata.as_ref() {
                self.cache
                    .put_metadata(path, metadata.clone(), response.revision);
            } else {
                self.cache.put_missing_metadata(path, response.revision);
            }
        });
        Ok(projection.value)
    }

    fn stat_path_attributes(&self, path: &str) -> FuseResult<Option<RemoteMetadata>> {
        // See `dir_entries`: `project_metadata` supplies read-your-writes, so
        // reads never wait on a publication.
        self.assert_namespace_journal_healthy()?;
        if let Some(metadata) = self.open_handle_metadata(path)? {
            return Ok(Some(metadata));
        }
        if let Some(metadata) = self.dirty_handle_committed_metadata(path)? {
            return Ok(Some(metadata));
        }
        let has_projection = self
            .namespace
            .as_ref()
            .map(|namespace| namespace.has_projection_for_path(path, false))
            .transpose()
            .map_err(|_| Errno::EIO)?
            .unwrap_or(false);
        // A get_metadata or is_known_missing hit may not be returned on its
        // own: an idle observer's revision fence lags a sibling's write, so a
        // fenced cache hit (or fenced negative entry) can be stale. The point
        // stat is routed through the subtree snapshot (which performs its own
        // wire fetch) or the metadata batcher below; either supplies the wire
        // round-trip that revalidates and reprimes the cache within this
        // dispatch, coalescing a concurrent burst into ~one gateway call. Only
        // the get_metadata serve that a same-dispatch subtree fetch has just
        // reprimed is returned here.
        if self
            .writes
            .as_ref()
            .is_some_and(|writes| writes.has_pending_path(path))
        {
            self.flush_writes()?;
        }
        if !has_projection {
            // A live revision watch keeps this mount's coherence fence
            // continuously confirmed, so a fence-matched cached attribute (or
            // fenced negative entry) is valid to serve without a wire call,
            // including entries a prior dispatch's subtree snapshot reprimed.
            // With the watch down we fail closed and reconfirm over the wire.
            if self.client.revision_watch_live() {
                let revision = self.client.coherence_revision();
                if let Some(metadata) = self.cache.get_metadata(path, revision) {
                    return Ok(Some(metadata));
                }
                if self.cache.is_known_missing(path, revision) {
                    return Ok(None);
                }
            }
            // Watch down: only a snapshot fetch that ran in THIS dispatch may
            // back a get_metadata serve. A short-circuited begin_subtree_load
            // (the shared snapshot already tagged for this fence) performs no
            // wire call, so its fenced entry can be stale relative to a sibling
            // process's newer publication that this idle mount never observed.
            // Fall through to the MetadataBatcher, which supplies the wire
            // round-trip, reprimes, and advances the fence.
            if self.ensure_subtree_metadata_snapshot()?
                && let Some(metadata) = self
                    .cache
                    .get_metadata(path, self.client.coherence_revision())
            {
                return Ok(Some(metadata));
            }
        }
        let response = if has_projection {
            self.tokio
                .block_on(self.client.stat_attributes_versioned(path))
                .map_err(|_| Errno::EIO)?
        } else {
            self.metadata_batcher
                .stat_attributes(&self.client, &self.tokio, path)
                .map_err(|error| {
                    tracing::warn!(path, error, "vfs batched attribute read failed");
                    Errno::EIO
                })?
        };
        let metadata = response.value;
        let projection = if let Some(namespace) = self.namespace.as_ref() {
            namespace
                .project_metadata(path, metadata, response.revision)
                .map_err(|_| Errno::EIO)?
        } else {
            NamespaceProjection {
                value: metadata,
                applied: false,
            }
        };
        publish_authoritative_projection(&projection, |metadata| {
            if let Some(metadata) = metadata.as_ref() {
                self.cache
                    .put_metadata(path, metadata.clone(), response.revision);
            } else {
                self.cache.put_missing_metadata(path, response.revision);
            }
        });
        Ok(projection.value)
    }

    /// Load one revision-fenced subtree metadata snapshot for the shared cache
    /// when the miss/quiet heuristic elects to. Returns `true` only when this
    /// dispatch actually performed the snapshot wire fetch and reprimed the
    /// cache at the response revision; a short-circuited or below-threshold call
    /// returns `false`. Callers must not serve a `get_metadata` hit off the
    /// shared snapshot unless this returned `true`: an idle mount whose fence
    /// has not advanced would otherwise serve a prior-dispatch snapshot entry
    /// that a sibling process may have superseded, with zero wire backing.
    fn ensure_subtree_metadata_snapshot(&self) -> FuseResult<bool> {
        let prefix = "";
        let revision = self.client.coherence_revision();
        if !self.cache.begin_subtree_load(prefix, revision) {
            return Ok(false);
        }
        match self.tokio.block_on(
            self.client
                .subtree_metadata_attributes_versioned(prefix, MAX_SUBTREE_METADATA_ENTRIES),
        ) {
            Ok(mut response) => {
                let prefetched = self.tokio.block_on(self.client.prefetch_subtree_versioned(
                    prefix,
                    MAX_SUBTREE_METADATA_ENTRIES,
                    MAX_SUBTREE_PREFETCH_BYTES,
                ));
                if let Ok(prefetched) = prefetched
                    && prefetched.revision == response.revision
                    && response.revision != 0
                {
                    let positions = response
                        .value
                        .iter()
                        .enumerate()
                        .map(|(index, (path, _))| (path.clone(), index))
                        .collect::<HashMap<_, _>>();
                    let mut files = Vec::new();
                    for (path, bytes) in prefetched.value {
                        let Some(index) = positions.get(path.as_str()).copied() else {
                            continue;
                        };
                        let metadata = &mut response.value[index].1;
                        if metadata.kind != "file" || metadata.size_bytes != bytes.len() as u64 {
                            continue;
                        }
                        metadata.content_hash = Some(content_hash_for_bytes(&bytes));
                        files.push((path, bytes, metadata.clone()));
                    }
                    self.cache.finish_subtree_load_with_files(
                        prefix,
                        response.revision,
                        response.value,
                        files,
                    );
                    return Ok(true);
                }
                self.cache
                    .finish_subtree_load(prefix, response.revision, response.value);
                Ok(true)
            }
            Err(error) => {
                if request_status(&error) == Some(reqwest::StatusCode::NOT_FOUND) {
                    self.cache.disable_subtree_loads(prefix);
                } else {
                    self.cache.abort_subtree_load(prefix);
                }
                tracing::debug!(
                    error = %error,
                    "vfs subtree metadata snapshot unavailable; falling back to path batches"
                );
                Ok(false)
            }
        }
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

    fn handle_metadata_needs_remote_route(state: &FileState) -> bool {
        // Dirty, newly created, pending-publication, and unlinked handles are
        // the authoritative open-file description until their publication
        // barrier completes. Re-statting the pathname cannot improve their
        // metadata and may observe an older remote version. Clean handles
        // still revalidate so writes, renames, and unlinks from other mounts
        // remain visible.
        !(state.dirty || state.created || state.pending_publication.is_some() || state.unlinked)
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
        let response = self
            .tokio
            .block_on(self.client.read_file_raw_versioned(path))
            .map_err(|_| Errno::EIO)?;
        let bytes = response.value.ok_or(Errno::ENOENT)?;

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
        self.cache
            .put_metadata(path, metadata.clone(), response.revision);
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
                loaded_coherence_revision: 0,
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

    /// Linux may omit `fh` from `getattr` even when servicing `fstat(2)`.
    /// Recover the open file description from the inode's stable identity so
    /// an unlinked-but-open file remains observable until its final release.
    fn open_handle_for_inode(&self, ino: INodeNo) -> FuseResult<Option<u64>> {
        let Some((path, identity)) = self.lock_inodes()?.route(ino) else {
            return Ok(None);
        };
        let handles = self.lock_handles()?;
        Ok(handles.files.iter().find_map(|(fh, state)| {
            let matches = match identity.as_deref() {
                Some(identity) => state.file_id.as_deref() == Some(identity),
                None => !state.unlinked && state.path == path,
            };
            matches.then_some(*fh)
        }))
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
            state.loaded_coherence_revision = source.loaded_coherence_revision;
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
                    .block_on(self.client.stat_versioned(path))
                    .map_err(|_| Errno::EIO)
                    .map(|response| {
                        response.value.map(|metadata| LinkedFileRoute {
                            path: path.to_string(),
                            metadata,
                            revision: response.revision,
                        })
                    })?
            };
            if let Some(route) = route {
                let metadata = &route.metadata;
                if Self::published_file_matches(
                    metadata,
                    size_bytes,
                    content_hash,
                    mode,
                    expected_file_id,
                ) {
                    return Ok(route);
                }
                return Err(Errno::EIO);
            }
            std::thread::sleep(retry_delay);
            retry_delay = retry_delay.saturating_mul(2);
        }
        Err(Errno::EIO)
    }

    fn published_file_matches(
        metadata: &RemoteMetadata,
        size_bytes: u64,
        content_hash: &str,
        mode: u32,
        expected_file_id: Option<&str>,
    ) -> bool {
        metadata.kind == "file"
            && metadata.size_bytes == size_bytes
            && metadata.content_hash.as_deref() == Some(content_hash)
            && metadata_mode(metadata) == mode
            && metadata.file_id.is_some()
            && expected_file_id.is_none_or(|file_id| metadata.file_id.as_deref() == Some(file_id))
    }

    fn cached_published_file(
        &self,
        path: &str,
        size_bytes: u64,
        content_hash: &str,
        mode: u32,
        expected_file_id: Option<&str>,
    ) -> FuseResult<Option<LinkedFileRoute>> {
        let revision = self.client.coherence_revision();
        let Some(metadata) = self.cache.get_metadata(path, revision) else {
            return Ok(None);
        };
        if !Self::published_file_matches(
            &metadata,
            size_bytes,
            content_hash,
            mode,
            expected_file_id,
        ) {
            return Err(Errno::EIO);
        }
        Ok(Some(LinkedFileRoute {
            path: path.to_string(),
            metadata,
            revision,
        }))
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
        let route = self
            .cached_published_file(
                &pending.path,
                pending.size_bytes,
                &pending.content_hash,
                pending.mode,
                pending.file_id.as_deref(),
            )?
            .map(Ok)
            .unwrap_or_else(|| {
                self.stat_published_file(
                    &pending.path,
                    pending.size_bytes,
                    &pending.content_hash,
                    pending.mode,
                    pending.file_id.as_deref(),
                )
            })?;
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
            handle.loaded_coherence_revision = 0;
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
        // unrelated files or mounts. The list keeps one entry per handle (fh) so
        // the caller can flush each descriptor, but several of those entries can
        // name the SAME gate: all handles for one inode share a single gate Arc
        // (see next_handle). `lock_publication_gates` collapses those repeats so
        // the non-reentrant gate is never locked twice on one thread.
        gates.sort_unstable_by_key(|(fh, _)| *fh);
        Ok(gates)
    }

    fn lock_publication_gates<'a>(
        gates: &'a [(u64, Arc<Mutex<()>>)],
    ) -> FuseResult<Vec<MutexGuard<'a, ()>>> {
        // Same-inode handles share one publication gate, so `gates` may list the
        // same Arc more than once (two descriptors on a file, hard-link aliases).
        // std::sync::Mutex is not reentrant: locking a repeat on this thread
        // would self-deadlock the rename. Acquire each DISTINCT gate exactly
        // once, in the caller's stable fh order — that still serializes every
        // listed handle against its inode's publications and keeps the global
        // order that prevents an AB/BA deadlock between overlapping renames.
        let mut seen: Vec<*const Mutex<()>> = Vec::with_capacity(gates.len());
        let mut guards = Vec::with_capacity(gates.len());
        for (_, gate) in gates {
            let identity = Arc::as_ptr(gate);
            if seen.contains(&identity) {
                continue;
            }
            guards.push(gate.lock().map_err(|_| Errno::EIO)?);
            seen.push(identity);
        }
        Ok(guards)
    }

    fn flush_handle_locked(&self, fh: u64) -> FuseResult<()> {
        if self.read_only {
            return Ok(());
        }
        {
            let handles = self.lock_handles()?;
            // A duplicate FLUSH may have captured this handle's gate before a
            // concurrent RELEASE removed the table entry. That authorized
            // waiter is already satisfied by the releasing flush.
            if !handles.files.contains_key(&fh) {
                return Ok(());
            }
        }
        self.flush_namespace()?;
        self.acknowledge_pending_publication_locked(fh)?;
        let state = self
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

        // Do not preflight the pathname before publishing an identified
        // inode. write-many carries the expected file identity and content
        // base in the same atomic mutation; a separate stat is both redundant
        // and a TOCTOU window. A real cross-mount rename/unlink is handled by
        // the journal's rejected-write resolver, which retargets a surviving
        // alias or retires the write without recreating the pathname.
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
        let had_dirty_data = self
            .lock_handles()?
            .files
            .get(&fh)
            .is_some_and(|state| state.dirty);
        self.flush_handle_locked(fh)?;
        if !had_dirty_data {
            return Ok(());
        }
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
        let response = self
            .tokio
            .block_on(self.client.stat_versioned(path))
            .map_err(|error| {
                tracing::warn!(path, error = %error, "vfs setattr stat failed");
                Errno::EIO
            })?;
        let metadata = response.value.ok_or(Errno::ENOENT)?;
        if metadata.kind == "symlink" || metadata_mode(&metadata) == mode {
            self.cache
                .put_metadata(path, metadata.clone(), response.revision);
            return Ok(self.attr_for_path(path, &metadata, false));
        }

        self.commit_namespace(VfsNamespaceMutation::SetMode {
            path: path.to_string(),
            mode,
        })?;
        let mut updated = metadata;
        updated.mode = Some(mode);
        updated.executable = mode_is_executable(mode);
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

    fn reserve_file_if_absent(
        &self,
        path: &str,
        mode: u32,
        exclusive: bool,
    ) -> FuseResult<RemoteMetadata> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        if exclusive {
            // O_EXCL is arbitrated by the gateway's expect-absent precondition,
            // so the mount must drain ahead of it and publish synchronously to
            // learn the verdict. Ordinary creates skip both drains: the
            // namespace journal is FIFO (a queued mutation for this path stays
            // ordered ahead of this one) and content writes are already fenced
            // by the delete/rename barriers, so neither drain adds ordering the
            // journal does not already provide.
            self.flush_namespace()?;
            self.flush_writes()?;
        }
        let empty_hash = content_hash_for_bytes(&[]);
        let projected = RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: 0,
            file_id: None,
            link_count: 1,
            link_target: None,
            content_hash: Some(empty_hash.clone()),
            executable: mode_is_executable(mode),
            mode: Some(mode),
            updated_at: None,
        };
        let mutation = VfsNamespaceMutation::CreateFile {
            path: path.to_string(),
            mode: Some(mode),
        };
        if !exclusive {
            // Journaled creation: durable append + projection, then answer from
            // the projection itself.
            //
            // Confirming through the gateway instead would undo the whole point
            // of deferring the publication. The projection is deliberately not
            // published to the shared cache (siblings must never observe an
            // unpublished mutation), so a cache-then-stat confirmation always
            // falls through to polling `/stat` until the worker's publication
            // lands — which makes the creating thread wait out its own
            // publication AND sleep through the journal's batch window, so
            // every creation publishes alone. The projected metadata is exactly
            // what a read of this path returns from this mount until the
            // publication lands, so returning it is both cheaper and the same
            // answer. The absent `file_id` is expected for an unpublished
            // creation and already handled downstream (`ensure_handle_loaded`
            // seeds such a handle from the creator's buffer rather than
            // 404ing).
            self.enqueue_namespace_creation(mutation, Some(projected.clone()))?;
            return Ok(projected);
        }
        let published = self.commit_namespace_with_metadata(mutation, Some(projected));
        if let Err(error) = published {
            if self
                .tokio
                .block_on(self.client.stat_attributes(path))
                .ok()
                .flatten()
                .is_some()
            {
                return Err(Errno::EEXIST);
            }
            return Err(error);
        }
        self.cached_published_file(path, 0, &empty_hash, mode, None)?
            .map(|route| route.metadata)
            .map(Ok)
            .unwrap_or_else(|| {
                self.stat_published_file(path, 0, &empty_hash, mode, None)
                    .map(|route| route.metadata)
            })
    }

    fn enqueue_namespace(
        &self,
        mutation: VfsNamespaceMutation,
        projected_metadata: Option<RemoteMetadata>,
    ) -> FuseResult<()> {
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
        namespace
            .enqueue_with_metadata(mutation, projected_metadata)
            .map_err(|_| Errno::EIO)
    }

    fn commit_namespace(&self, mutation: VfsNamespaceMutation) -> FuseResult<()> {
        self.commit_namespace_with_metadata(mutation, None)
    }

    fn commit_namespace_with_metadata(
        &self,
        mutation: VfsNamespaceMutation,
        projected_metadata: Option<RemoteMetadata>,
    ) -> FuseResult<()> {
        let _publication = self
            .namespace_publication_gate
            .lock()
            .map_err(|_| Errno::EIO)?;
        // Bar new content writes to the delete/rename target subtree for the
        // life of this mutation. Installed before enqueue_namespace's
        // flush_writes drain, so a racing descendant write is either drained
        // (ordered ahead of the mutation) or blocked (ordered strictly behind
        // it) and can never be applied server-side after the RemoveDirectory to
        // resurrect the subtree. The guard clears on drop once the mutation has
        // been applied or resolved below.
        //
        // No deadlock. A barred write parks without holding the write-journal
        // lock, so it never wedges the flush_writes drain below. A parked writer
        // may still hold its per-handle publication gate, but nothing between
        // installing this barrier and dropping it (the flush_writes drain, the
        // namespace enqueue, and flush_namespace_locked) ever acquires a handle
        // gate — and Rename, the only namespace mutation that touches handle
        // gates, takes them BEFORE it reaches this barrier and holds them
        // throughout, so it can never be the parked writer. The barrier's owner
        // therefore always makes progress and drops it, unparking the writer.
        let _write_barrier = self.install_descendant_write_barrier(&mutation);
        self.enqueue_namespace(mutation, projected_metadata)?;
        // Namespace syscalls are publication points. Serialize enqueue+flush
        // so a terminal journal result is returned to the mutation that
        // caused it instead of being consumed by an unrelated waiter.
        self.flush_namespace_locked()
    }

    /// Enqueue a creation without waiting for the gateway to publish it.
    ///
    /// The record is durably journaled (`enqueue_with_metadata` fsyncs the WAL
    /// line) and projected before this returns, so the guest observes the new
    /// entry immediately and the journal worker batches the publication behind
    /// it. This is the ordinary journaled-filesystem contract: creation latency
    /// is a local durable append, and a publication failure surfaces at the
    /// next barrier (fsync/close, or any delete/rename/set-mode) as EIO rather
    /// than at the creating syscall.
    ///
    /// Only creations may take this path. Deletes, renames and set-mode remain
    /// synchronous publication points, and an exclusive create still publishes
    /// synchronously because cross-mount O_EXCL arbitration is the gateway's
    /// `expect_absent` precondition, not a mount-local decision.
    fn enqueue_namespace_creation(
        &self,
        mutation: VfsNamespaceMutation,
        projected_metadata: Option<RemoteMetadata>,
    ) -> FuseResult<()> {
        debug_assert!(
            matches!(
                &mutation,
                VfsNamespaceMutation::CreateFile { .. }
                    | VfsNamespaceMutation::CreateDirectory { .. }
                    | VfsNamespaceMutation::CreateSymlink { .. }
                    | VfsNamespaceMutation::CreateHardLink { .. }
            ),
            "only creations may publish asynchronously",
        );
        let _publication = self
            .namespace_publication_gate
            .lock()
            .map_err(|_| Errno::EIO)?;
        self.enqueue_namespace(mutation, projected_metadata)
    }

    /// Install a write-barrier over the descendant subtree(s) a delete/rename
    /// mutation targets, or `None` for mutations that cannot be contaminated by
    /// a racing content write (creations, set-mode). Rename bars both endpoints.
    fn install_descendant_write_barrier(
        &self,
        mutation: &VfsNamespaceMutation,
    ) -> Option<WriteBarrierGuard> {
        let prefixes = descendant_write_barrier_prefixes(mutation);
        if prefixes.is_empty() {
            return None;
        }
        self.writes
            .as_ref()
            .map(|writes| writes.install_descendant_barrier(prefixes))
    }

    fn flush_namespace(&self) -> FuseResult<()> {
        let _publication = self
            .namespace_publication_gate
            .lock()
            .map_err(|_| Errno::EIO)?;
        self.flush_namespace_locked()
    }

    /// Read-side barrier for one namespace location. Namespace mutations remain
    /// durably journaled and mutation syscalls still synchronously publish, but
    /// a deep, unrelated rename/create must not globally stop every lookup on
    /// the mount while its HTTP publication is in flight.
    /// Surface a dead-lettered publication to a read without draining.
    ///
    /// Reads project this mount's queued mutations over authoritative state
    /// rather than waiting for them to publish, so the only journal state a
    /// read must consult is whether a publication has already failed
    /// terminally — otherwise a read could keep reporting an entry whose
    /// creation the gateway rejected.
    fn assert_namespace_journal_healthy(&self) -> FuseResult<()> {
        let Some(namespace) = self.namespace.as_ref() else {
            return Ok(());
        };
        namespace.take_terminal_error().map_err(|error| {
            tracing::warn!(error = %error, "vfs namespace publication dead-lettered");
            Errno::EIO
        })
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
                revision: 0,
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
            // The route's revision is the fence the route metadata was read at.
            // Under a live watch every publication advances that fence before
            // the publishing writer is allowed to return, so an unadvanced
            // fence is proof that nothing has been published against this path
            // since — which is exactly what the verifying stat below re-checks.
            // Skipping it halves the round trips of every whole-file read (the
            // dominant cost of scanning a working tree) without weakening the
            // check: any publication either advanced the fence already, or the
            // writer that made it has not yet been told it is coherent.
            let fence = self.client.coherence_revision();
            let fence_proves_unchanged = self.client.revision_watch_live()
                && route.revision != 0
                && route.revision == fence
                && route.metadata.file_id.as_deref() == Some(file_id);
            let bytes = self
                .tokio
                .block_on(self.client.read_file_raw(&route.path))
                .map_err(|_| Errno::EIO)?;
            if let Some(bytes) = bytes {
                if fence_proves_unchanged && self.client.coherence_revision() == fence {
                    return Ok((route, bytes));
                }
                let verified = self
                    .tokio
                    .block_on(self.client.stat_versioned(&route.path))
                    .map_err(|_| Errno::EIO)?;
                if let Some(metadata) = verified.value
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
                            revision: verified.revision,
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
        if state.loaded
            && state.loaded_coherence_revision != 0
            && state.loaded_coherence_revision == self.client.coherence_revision()
        {
            return Ok(());
        }
        self.assert_namespace_journal_healthy()?;
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
                    revision: 0,
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
                handle.loaded_coherence_revision = route.revision;
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
        // A clean loaded handle is a read cache, not a snapshot. If the
        // authoritative hash changed while another mount wrote the inode,
        // replace its stale buffer. Dirty/unlinked or concurrently revised
        // handles still retain their private open-file state.
        if handle.dirty || handle.unlinked || handle.revision != state.revision {
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
        handle.loaded_coherence_revision = route.revision;
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
        let blocking_started = Instant::now();
        let deadline = blocking_started + ADVISORY_LOCK_BLOCK_TIMEOUT;
        let mut slow_warned = false;
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
            let waited = blocking_started.elapsed();
            if !slow_warned && waited >= SLOW_ADVISORY_LOCK_WARN_AFTER {
                slow_warned = true;
                tracing::warn!(
                    waited_secs = waited.as_secs(),
                    path = %target.path,
                    kind,
                    "vfs advisory lock still blocked awaiting the gateway grant"
                );
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

/// Descendant write-barrier prefixes for a namespace mutation: the subtree(s) a
/// racing content write could resurrect. Only deletes and renames qualify;
/// creations and set-mode leave nothing for a write to contaminate.
fn descendant_write_barrier_prefixes(mutation: &VfsNamespaceMutation) -> Vec<String> {
    match mutation {
        VfsNamespaceMutation::RemoveDirectory { path }
        | VfsNamespaceMutation::DeleteFile { path, .. } => {
            vec![path.trim_matches('/').to_string()]
        }
        VfsNamespaceMutation::Rename { from, to } => vec![
            from.trim_matches('/').to_string(),
            to.trim_matches('/').to_string(),
        ],
        _ => Vec::new(),
    }
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
        let directory_prefetch = InitFlags::FUSE_DO_READDIRPLUS | InitFlags::FUSE_READDIRPLUS_AUTO;
        // FUSE_WRITEBACK_CACHE is never requested: under writeback the kernel
        // treats i_size as kernel-authoritative and ignores the daemon's fresh
        // size in getattr replies, so a sibling mount's extend stays invisible
        // to this mount's kernel regardless of attr TTL. Every open already
        // forces FOPEN_DIRECT_IO, so writeback buys nothing here.
        let _ = read_only;
        InitFlags::FUSE_AUTO_INVAL_DATA | locks | directory_prefetch
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Barrier;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::extract::State;
    use axum::http::{HeaderName, HeaderValue, Method, Request, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::any;
    use axum::{Json, Router};
    use chevalier_sandbox::vfs::{
        VfsMetadata as RemoteMetadata, VfsMetadataManyRequest, VfsMetadataManyResponse,
        VfsNamespaceMutation, VfsSubtreeMetadataEntry, VfsSubtreeMetadataResponse,
    };
    use fuser::{FopenFlags, InitFlags, LockNamespace};
    use tokio::runtime::Builder;
    use tokio::sync::Notify;

    use super::super::cache::{MountInvalidators, RemoteFuseCache, SUBTREE_LOAD_REVISION_QUIET_PERIOD};
    use super::super::client::RemoteVfsClient;
    use super::super::namespace::NamespaceProjection;
    use super::{
        ATTR_ENTRY_LEASE_TTL, ActiveAdvisoryLockFile, ActiveAdvisoryLocks, HandlePublication,
        InodeTable, LockWaitCancellation, ROOT_INO, RemoteFuseFs, active_advisory_lock_identities,
        combine_flush_and_lock_cleanup, content_hash_conflicts, content_hash_for_bytes,
        creation_mode, lease_ttl_for, publish_authoritative_projection, range_fingerprint,
        remote_file_open_flags, take_active_advisory_lock_file_id, take_active_posix_handle_locks,
    };

    #[derive(Default)]
    struct LockPublicationGateway {
        stat_requests: usize,
        write_batches: Vec<Vec<u8>>,
        write_preconditions: Vec<Option<String>>,
    }

    #[derive(Default)]
    struct MetadataBatchGateway {
        batches: Vec<Vec<String>>,
        queries: Vec<Option<String>>,
        sizes: HashMap<String, u64>,
        stat_requests: usize,
    }

    struct ContentRefreshGateway {
        bytes: Vec<u8>,
    }

    struct SubtreeSnapshotGateway {
        revision: u64,
        size_offset: u64,
        subtree_requests: usize,
        fallback_requests: usize,
        /// When false the /watch route errors so the revision watch stays down
        /// and serves stay strict (wire-backed). When true the first poll
        /// confirms the fence (flipping watch_live) and later polls hold open.
        watch_serves: bool,
        watch_requests: usize,
    }

    /// 200 body for the revision-watch route, stamped with the namespace
    /// revision header exactly as the real gateway does.
    fn watch_ok_response(revision: u64) -> Response {
        let mut response = Json(serde_json::json!({ "revision": revision })).into_response();
        response.headers_mut().insert(
            HeaderName::from_static("x-chevalier-vfs-namespace-revision"),
            HeaderValue::from_str(revision.to_string().as_str()).unwrap(),
        );
        response
    }

    /// Shared /watch behavior for the stub gateways. `watch_serves` false keeps
    /// the watch down (500 -> strict serves). Otherwise the first poll returns
    /// 200 with the current revision so `watch_live` flips deterministically,
    /// and subsequent polls hold open (mimicking a real long poll) so the fence
    /// is confirmed exactly once and the test controls all later advances.
    async fn serve_revision_watch(
        watch_serves: bool,
        watch_requests: usize,
        revision: u64,
    ) -> Response {
        if !watch_serves {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        if watch_requests <= 1 {
            return watch_ok_response(revision);
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
        StatusCode::NO_CONTENT.into_response()
    }

    /// Spin until the revision watch has confirmed the fence, or panic on
    /// timeout. Tests that assert amortized serves must wait for this so the
    /// gate is genuinely exercised under a live watch.
    fn await_watch_live(client: &RemoteVfsClient) {
        for _ in 0..200 {
            if client.revision_watch_live() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("revision watch never went live");
    }

    async fn subtree_snapshot_gateway(
        State(state): State<Arc<Mutex<SubtreeSnapshotGateway>>>,
        request: Request<Body>,
    ) -> Response {
        match (request.method(), request.uri().path()) {
            (&Method::POST, "/subtree-metadata") => {
                let (revision, size_offset) = {
                    let mut state = state.lock().unwrap();
                    state.subtree_requests += 1;
                    (state.revision, state.size_offset)
                };
                let entries = (0..1_000)
                    .map(|index| VfsSubtreeMetadataEntry {
                        path: format!("test-scope/file-{index}"),
                        kind: "file".to_string(),
                        size_bytes: size_offset + index,
                        file_id: Some(format!("identity-{index}")),
                        link_count: 1,
                        link_target: None,
                        content_hash: None,
                        executable: false,
                        mode: Some(0o644),
                        token_count: None,
                        version: None,
                        updated_at: None,
                        object_state: None,
                    })
                    .collect();
                let mut response = Json(VfsSubtreeMetadataResponse { entries }).into_response();
                response.headers_mut().insert(
                    HeaderName::from_static("x-chevalier-vfs-namespace-revision"),
                    HeaderValue::from_str(revision.to_string().as_str()).unwrap(),
                );
                response
            }
            (&Method::POST, "/metadata-many") => {
                let body = to_bytes(request.into_body(), 1024 * 1024)
                    .await
                    .expect("read metadata-many request");
                let payload: VfsMetadataManyRequest =
                    serde_json::from_slice(&body).expect("decode metadata-many request");
                let (revision, size_offset) = {
                    let mut state = state.lock().unwrap();
                    state.fallback_requests += 1;
                    (state.revision, state.size_offset)
                };
                let entries = payload
                    .paths
                    .iter()
                    .map(|path| {
                        path.strip_prefix("test-scope/file-")
                            .and_then(|index| index.parse::<u64>().ok())
                            .map(|index| RemoteMetadata {
                                kind: "file".to_string(),
                                size_bytes: size_offset + index,
                                file_id: Some(format!("identity-{index}")),
                                link_count: 1,
                                link_target: None,
                                content_hash: None,
                                executable: false,
                                mode: Some(0o644),
                                updated_at: None,
                            })
                    })
                    .collect();
                let mut response = Json(VfsMetadataManyResponse { entries }).into_response();
                response.headers_mut().insert(
                    HeaderName::from_static("x-chevalier-vfs-namespace-revision"),
                    HeaderValue::from_str(revision.to_string().as_str()).unwrap(),
                );
                response
            }
            (&Method::GET, "/stat") => {
                state.lock().unwrap().fallback_requests += 1;
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
            (&Method::GET, "/watch") => {
                let (serves, count, revision) = {
                    let mut state = state.lock().unwrap();
                    state.watch_requests += 1;
                    (state.watch_serves, state.watch_requests, state.revision)
                };
                serve_revision_watch(serves, count, revision).await
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn metadata_batch_gateway(
        State(state): State<Arc<Mutex<MetadataBatchGateway>>>,
        request: Request<Body>,
    ) -> Response {
        let method = request.method().clone();
        let path = request.uri().path().to_string();
        match (method, path.as_str()) {
            (Method::POST, "/metadata-many") => {
                let query = request.uri().query().map(str::to_string);
                let body = to_bytes(request.into_body(), 1024 * 1024)
                    .await
                    .expect("read metadata-many request");
                let payload: VfsMetadataManyRequest =
                    serde_json::from_slice(&body).expect("decode metadata-many request");
                let entries = {
                    let mut state = state.lock().unwrap();
                    state.batches.push(payload.paths.clone());
                    state.queries.push(query);
                    payload
                        .paths
                        .iter()
                        .map(|path| {
                            state
                                .sizes
                                .get(path)
                                .copied()
                                .map(|size_bytes| RemoteMetadata {
                                    kind: "file".to_string(),
                                    size_bytes,
                                    file_id: Some(format!("identity-{path}")),
                                    link_count: 1,
                                    link_target: None,
                                    content_hash: None,
                                    executable: false,
                                    mode: Some(0o644),
                                    updated_at: None,
                                })
                        })
                        .collect()
                };
                Json(VfsMetadataManyResponse { entries }).into_response()
            }
            (Method::GET, "/stat") => {
                state.lock().unwrap().stat_requests += 1;
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
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
                    .get("x-chevalier-vfs-precondition-kind")
                    .or_else(|| {
                        request
                            .headers()
                            .get("x-chevalier-vfs-precondition-fingerprint")
                    })
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

    async fn content_refresh_gateway(
        State(state): State<Arc<Mutex<ContentRefreshGateway>>>,
        request: Request<Body>,
    ) -> Response {
        let bytes = state.lock().unwrap().bytes.clone();
        match (request.method(), request.uri().path()) {
            (&Method::GET, "/stat") => Json(serde_json::json!({
                "kind": "file",
                "size_bytes": bytes.len(),
                "file_id": "stable-shared-file",
                "link_count": 1,
                "link_target": null,
                "content_hash": content_hash_for_bytes(&bytes),
                "executable": false,
                "mode": 420,
                "updated_at": null
            }))
            .into_response(),
            (&Method::GET, "/file/raw") => bytes.into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    #[test]
    fn attr_entry_lease_is_watch_bounded_not_a_correctness_boundary() {
        // Superseded design: the mount once replied TTL=0 everywhere to keep the
        // kernel metadata/entry cache disabled, because there was no
        // kernel-to-kernel invalidation path. There is one now (the fuser
        // notifier, driven by the commit hooks and the revision watch), so the
        // mount hands the kernel a positive attr/entry lease WHILE the watch is
        // live — a liveness-bounded lease, not a correctness boundary — and
        // fails closed to Duration::ZERO the moment the watch drops.
        assert_eq!(lease_ttl_for(true), ATTR_ENTRY_LEASE_TTL);
        assert!(ATTR_ENTRY_LEASE_TTL > Duration::ZERO);
        assert_eq!(lease_ttl_for(false), Duration::ZERO);
    }

    #[test]
    fn mount_local_projection_is_never_published_to_sibling_cache() {
        let key = format!("projection-publication-test-{}", uuid::Uuid::new_v4());
        let origin = RemoteFuseCache::shared(&key);
        let sibling = RemoteFuseCache::shared(&key);
        let metadata = RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: 4,
            file_id: Some("shared-file".to_string()),
            link_count: 1,
            link_target: None,
            content_hash: Some("hash".to_string()),
            executable: false,
            mode: Some(0o644),
            updated_at: None,
        };

        let projected = NamespaceProjection {
            value: Some(metadata.clone()),
            applied: true,
        };
        publish_authoritative_projection(&projected, |value| {
            origin.put_metadata("tree/file", value.clone().expect("projected metadata"), 17);
        });
        assert!(sibling.get_metadata("tree/file", 17).is_none());

        let authoritative = NamespaceProjection {
            value: Some(metadata.clone()),
            applied: false,
        };
        publish_authoritative_projection(&authoritative, |value| {
            origin.put_metadata(
                "tree/file",
                value.clone().expect("authoritative metadata"),
                17,
            );
        });
        assert_eq!(sibling.get_metadata("tree/file", 17), Some(metadata));
    }

    #[test]
    fn concurrent_attribute_reads_batch_without_retaining_stale_metadata() {
        const PATH_COUNT: usize = 32;
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(MetadataBatchGateway::default()));
        {
            let mut state = gateway.lock().unwrap();
            for index in 0..PATH_COUNT {
                state
                    .sizes
                    .insert(format!("test-scope/file-{index}"), index as u64);
            }
        }
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(metadata_batch_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        let fs = Arc::new(RemoteFuseFs::new(
            client,
            false,
            "test-scope",
            runtime.handle().clone(),
        ));
        let barrier = Arc::new(Barrier::new(PATH_COUNT + 1));
        let readers = (0..PATH_COUNT)
            .map(|index| {
                let fs = Arc::clone(&fs);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    fs.stat_path_attributes(&format!("file-{index}"))
                        .unwrap()
                        .unwrap()
                        .size_bytes
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for (index, reader) in readers.into_iter().enumerate() {
            assert_eq!(reader.join().unwrap(), index as u64);
        }

        {
            let state = gateway.lock().unwrap();
            assert!(
                state.batches.len() <= 4,
                "{} concurrent stats fragmented into {} batches",
                PATH_COUNT,
                state.batches.len()
            );
            assert_eq!(
                state.batches.iter().map(Vec::len).sum::<usize>(),
                PATH_COUNT
            );
            assert!(
                state
                    .queries
                    .iter()
                    .all(|query| query.as_deref() == Some("max_hash_bytes=0"))
            );
            assert_eq!(state.stat_requests, 0);
        }

        gateway
            .lock()
            .unwrap()
            .sizes
            .insert("test-scope/file-0".to_string(), 9_999);
        assert_eq!(
            fs.stat_path_attributes("file-0")
                .unwrap()
                .unwrap()
                .size_bytes,
            9_999,
            "a later authoritative batch must observe a cross-mount replacement"
        );
        server.abort();
    }

    #[test]
    fn sequential_thousand_file_stats_use_one_revision_fenced_subtree_snapshot() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(SubtreeSnapshotGateway {
            revision: 17,
            size_offset: 0,
            subtree_requests: 0,
            fallback_requests: 0,
            watch_serves: false,
            watch_requests: 0,
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(subtree_snapshot_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let first_client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        first_client.observe_published_revision(17);
        let first = RemoteFuseFs::new(
            first_client.clone(),
            false,
            "test-scope",
            runtime.handle().clone(),
        );

        for index in 0..7 {
            assert_eq!(
                first
                    .stat_path_attributes(&format!("file-{index}"))
                    .unwrap()
                    .unwrap()
                    .size_bytes,
                index
            );
        }
        std::thread::sleep(SUBTREE_LOAD_REVISION_QUIET_PERIOD);
        for index in 7..1_000 {
            assert_eq!(
                first
                    .stat_path_attributes(&format!("file-{index}"))
                    .unwrap()
                    .unwrap()
                    .size_bytes,
                index
            );
        }
        {
            // One shared subtree snapshot is fetched (the eighth miss past the
            // quiet period). Only that dispatch may serve get_metadata off the
            // snapshot; every other sequential stat reconfirms through the
            // coalescing metadata batcher rather than serving a fence that a
            // sibling process could have advanced past with zero wire backing.
            // These sequential stats do not overlap, so each takes its own
            // batched wire (real FUSE dispatch runs getattr concurrently, where
            // the batcher folds a scan burst back into a handful of wires). The
            // seven warm-up stats plus the 992 post-snapshot stats each wire.
            let state = gateway.lock().unwrap();
            assert_eq!(state.subtree_requests, 1);
            assert_eq!(state.fallback_requests, 999);
        }

        {
            let mut state = gateway.lock().unwrap();
            state.revision = 18;
            state.size_offset = 10_000;
        }
        let second_client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        second_client.observe_published_revision(18);
        let second =
            RemoteFuseFs::new(second_client, false, "test-scope", runtime.handle().clone());
        for index in 992..999 {
            assert_eq!(
                second
                    .stat_path_attributes(&format!("file-{index}"))
                    .unwrap()
                    .unwrap()
                    .size_bytes,
                10_000 + index
            );
        }
        std::thread::sleep(SUBTREE_LOAD_REVISION_QUIET_PERIOD);
        for index in 999..1_000 {
            assert_eq!(
                second
                    .stat_path_attributes(&format!("file-{index}"))
                    .unwrap()
                    .unwrap()
                    .size_bytes,
                10_000 + index
            );
        }
        {
            // The second mount clears the stale revision-17 snapshot on its
            // first fenced access, wires its seven warm-up stats, then fetches a
            // second snapshot at revision 18. No stat serves the earlier
            // snapshot without a wire, so the reconfirmation count is the first
            // mount's 999 plus these seven warm-up wires.
            let state = gateway.lock().unwrap();
            assert_eq!(state.subtree_requests, 2);
            assert_eq!(state.fallback_requests, 1_006);
        }
        server.abort();
    }

    // Watch-live counterpart of the strict thousand-file scan. A live revision
    // watch keeps each mount's coherence fence continuously confirmed, so after
    // one mount warms the shared subtree snapshot every remaining fence-matched
    // stat serves get_metadata with zero wire calls. The amortized fallbacks
    // return near the pre-strict counts (seven warm-up wires per mount) while
    // the strict variant above wires all 999.
    #[test]
    fn sequential_thousand_file_stats_amortize_under_a_live_watch() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(SubtreeSnapshotGateway {
            revision: 17,
            size_offset: 0,
            subtree_requests: 0,
            fallback_requests: 0,
            watch_serves: true,
            watch_requests: 0,
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(subtree_snapshot_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let first_client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        first_client.observe_published_revision(17);
        let first = RemoteFuseFs::new(
            first_client.clone(),
            false,
            "test-scope",
            runtime.handle().clone(),
        );
        await_watch_live(&first_client);

        for index in 0..7 {
            assert_eq!(
                first
                    .stat_path_attributes(&format!("file-{index}"))
                    .unwrap()
                    .unwrap()
                    .size_bytes,
                index
            );
        }
        std::thread::sleep(SUBTREE_LOAD_REVISION_QUIET_PERIOD);
        for index in 7..1_000 {
            assert_eq!(
                first
                    .stat_path_attributes(&format!("file-{index}"))
                    .unwrap()
                    .unwrap()
                    .size_bytes,
                index
            );
        }
        {
            // Seven warm-up stats wire before the eighth trips the shared
            // snapshot (one subtree fetch). Under the live watch every stat
            // after the snapshot serves get_metadata off the revision-17 fence
            // with zero wire calls, so the fallbacks stop at the seven warm-ups.
            let state = gateway.lock().unwrap();
            assert_eq!(state.subtree_requests, 1);
            assert_eq!(state.fallback_requests, 7);
        }

        {
            let mut state = gateway.lock().unwrap();
            state.revision = 18;
            state.size_offset = 10_000;
        }
        let second_client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        second_client.observe_published_revision(18);
        let second =
            RemoteFuseFs::new(second_client, false, "test-scope", runtime.handle().clone());
        for index in 992..999 {
            assert_eq!(
                second
                    .stat_path_attributes(&format!("file-{index}"))
                    .unwrap()
                    .unwrap()
                    .size_bytes,
                10_000 + index
            );
        }
        std::thread::sleep(SUBTREE_LOAD_REVISION_QUIET_PERIOD);
        for index in 999..1_000 {
            assert_eq!(
                second
                    .stat_path_attributes(&format!("file-{index}"))
                    .unwrap()
                    .unwrap()
                    .size_bytes,
                10_000 + index
            );
        }
        {
            // The second mount's first fenced stat clears the stale revision-17
            // snapshot (its fence is already 18 from the local publication),
            // wires its seven warm-ups, then trips a second snapshot at 18. The
            // amortized fallback total is the first mount's seven plus these
            // seven, never the strict variant's 1,006.
            let state = gateway.lock().unwrap();
            assert_eq!(state.subtree_requests, 2);
            assert_eq!(state.fallback_requests, 14);
        }
        server.abort();
    }

    #[test]
    fn clean_loaded_handle_refreshes_after_cross_mount_write() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(ContentRefreshGateway {
            bytes: b"A".to_vec(),
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(content_refresh_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new(client, false, "test-scope", runtime.handle().clone());
        let old_bytes = Vec::new();
        let handle = fs
            .next_handle(
                "shared",
                old_bytes.clone(),
                true,
                Some(content_hash_for_bytes(&old_bytes)),
                0o644,
                false,
                Some("stable-shared-file".to_string()),
                1,
            )
            .unwrap();

        fs.ensure_handle_loaded(handle).unwrap();

        let handles = fs.lock_handles().unwrap();
        let state = handles.files.get(&handle).unwrap();
        assert_eq!(state.buffer, b"A");
        assert_eq!(
            state.base_content_hash.as_deref(),
            Some(content_hash_for_bytes(b"A").as_str())
        );
        assert!(state.loaded);
        assert!(!state.dirty);
        drop(handles);
        server.abort();
    }

    // Two mounts of the same logical tree that live in different VMs each hold
    // a process-local coherence fence and cache (keyed by endpoint+scope), so a
    // sibling's write never advances the other's fence in-process. This gateway
    // is scope-agnostic: it serves one file's state to whichever mount asks, so
    // a distinct-scope client models a distinct-VM sibling against shared data.
    struct CrossVmGateway {
        content: Vec<u8>,
        revision: u64,
        present: bool,
        tree_requests: usize,
        metadata_many_requests: usize,
    }

    fn cross_vm_metadata(content: &[u8]) -> RemoteMetadata {
        RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: content.len() as u64,
            file_id: Some("stable-cross-vm-file".to_string()),
            link_count: 1,
            link_target: None,
            content_hash: Some(content_hash_for_bytes(content)),
            executable: false,
            mode: Some(0o644),
            updated_at: None,
        }
    }

    fn with_revision_header(mut response: Response, revision: u64) -> Response {
        response.headers_mut().insert(
            HeaderName::from_static("x-chevalier-vfs-namespace-revision"),
            HeaderValue::from_str(revision.to_string().as_str()).unwrap(),
        );
        response
    }

    async fn cross_vm_gateway(
        State(state): State<Arc<Mutex<CrossVmGateway>>>,
        request: Request<Body>,
    ) -> Response {
        let method = request.method().clone();
        let path = request.uri().path().to_string();
        match (method, path.as_str()) {
            (Method::GET, "/tree") => {
                let (entries, revision) = {
                    let mut state = state.lock().unwrap();
                    state.tree_requests += 1;
                    let entries: Vec<chevalier_sandbox::vfs::VfsDirEntry> = if state.present {
                        vec![chevalier_sandbox::vfs::VfsDirEntry {
                            name: "file".to_string(),
                            kind: "file".to_string(),
                            size_bytes: state.content.len() as u64,
                            file_id: Some("stable-cross-vm-file".to_string()),
                            link_count: 1,
                            link_target: None,
                            content_hash: Some(content_hash_for_bytes(&state.content)),
                            executable: false,
                            mode: Some(0o644),
                            updated_at: None,
                        }]
                    } else {
                        Vec::new()
                    };
                    (entries, state.revision)
                };
                with_revision_header(Json(entries).into_response(), revision)
            }
            (Method::GET, "/stat") => {
                let (present, content, revision) = {
                    let state = state.lock().unwrap();
                    (state.present, state.content.clone(), state.revision)
                };
                if present {
                    with_revision_header(
                        Json(cross_vm_metadata(&content)).into_response(),
                        revision,
                    )
                } else {
                    with_revision_header(StatusCode::NOT_FOUND.into_response(), revision)
                }
            }
            (Method::POST, "/metadata-many") => {
                let body = to_bytes(request.into_body(), 1024 * 1024)
                    .await
                    .expect("read metadata-many request");
                let payload: VfsMetadataManyRequest =
                    serde_json::from_slice(&body).expect("decode metadata-many request");
                let (present, content, revision) = {
                    let mut state = state.lock().unwrap();
                    state.metadata_many_requests += 1;
                    (state.present, state.content.clone(), state.revision)
                };
                let entries = payload
                    .paths
                    .iter()
                    .map(|_| present.then(|| cross_vm_metadata(&content)))
                    .collect();
                with_revision_header(
                    Json(VfsMetadataManyResponse { entries }).into_response(),
                    revision,
                )
            }
            (Method::GET, "/file/raw") => {
                let (present, content, revision) = {
                    let state = state.lock().unwrap();
                    (state.present, state.content.clone(), state.revision)
                };
                if present {
                    with_revision_header(content.into_response(), revision)
                } else {
                    with_revision_header(StatusCode::NOT_FOUND.into_response(), revision)
                }
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    #[test]
    fn idle_observer_metadata_refreshes_across_distinct_revision_registries() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(CrossVmGateway {
            content: b"01234".to_vec(),
            revision: 17,
            present: true,
            tree_requests: 0,
            metadata_many_requests: 0,
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(cross_vm_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });

        // Client B mounts one VM; its coherence fence and cache are keyed by
        // endpoint+scope, so they are distinct from any other-VM sibling.
        let observer_client = RemoteVfsClient::new(&endpoint, "token", "vm-b").unwrap();
        let observer =
            RemoteFuseFs::new(observer_client.clone(), false, "vm-b", runtime.handle().clone());

        // B warms its cache and fence at the pre-write revision.
        let warm_dir = observer.dir_entries("").unwrap();
        assert_eq!(warm_dir.len(), 1);
        assert_eq!(warm_dir[0].size_bytes, 5);
        assert_eq!(
            observer
                .stat_path_attributes("file")
                .unwrap()
                .unwrap()
                .size_bytes,
            5
        );
        assert_eq!(observer.read_bytes("file", 0, 64).unwrap(), b"01234".to_vec());
        assert_eq!(observer_client.coherence_revision(), 17);

        // A sibling on another VM extends the file and publishes R_write. Its
        // SharedRevisionState registry is distinct, so its read does not touch
        // B's fence.
        {
            let mut state = gateway.lock().unwrap();
            state.content = b"0123456789".to_vec();
            state.revision = 18;
        }
        let writer_client = RemoteVfsClient::new(&endpoint, "token", "vm-a").unwrap();
        assert_ne!(
            writer_client.coherence_key(),
            observer_client.coherence_key()
        );
        assert_eq!(
            runtime
                .block_on(writer_client.stat_attributes("file"))
                .unwrap()
                .unwrap()
                .size_bytes,
            10
        );
        assert_eq!(writer_client.coherence_revision(), 18);
        // B issued no wire call since R_write, so its fence is still stale.
        assert_eq!(observer_client.coherence_revision(), 17);

        // B repeats scandir + stat + read with no manual fence poke.
        let refreshed_dir = observer.dir_entries("").unwrap();
        assert_eq!(refreshed_dir[0].size_bytes, 10);
        assert_eq!(
            observer
                .stat_path_attributes("file")
                .unwrap()
                .unwrap()
                .size_bytes,
            10,
            "an idle observer's stat must reflect the sibling extension"
        );
        assert_eq!(
            observer.read_bytes("file", 0, 64).unwrap(),
            b"0123456789".to_vec(),
            "the fresh read must not be truncated by a stale cached size"
        );
        assert_eq!(observer_client.coherence_revision(), 18);
        server.abort();
    }

    #[test]
    fn dir_entries_refetches_when_local_coherence_lags_the_gateway() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(CrossVmGateway {
            content: b"01234".to_vec(),
            revision: 17,
            present: true,
            tree_requests: 0,
            metadata_many_requests: 0,
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(cross_vm_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "token", "vm-b").unwrap();
        let fs = RemoteFuseFs::new(client.clone(), false, "vm-b", runtime.handle().clone());

        let first = fs.dir_entries("").unwrap();
        assert_eq!(first[0].size_bytes, 5);
        assert_eq!(client.coherence_revision(), 17);

        // The gateway advances while this mount stays idle: its fence lags at 17
        // and it holds a revision-fenced directory entry for that revision.
        {
            let mut state = gateway.lock().unwrap();
            state.content = b"0123456789".to_vec();
            state.revision = 18;
        }
        assert_eq!(client.coherence_revision(), 17);
        let tree_requests_before = gateway.lock().unwrap().tree_requests;

        let second = fs.dir_entries("").unwrap();
        assert_eq!(
            second[0].size_bytes, 10,
            "a lagging mount must re-fetch the listing over the wire, not serve get_dir"
        );
        assert_eq!(
            gateway.lock().unwrap().tree_requests,
            tree_requests_before + 1
        );
        assert_eq!(client.coherence_revision(), 18);
        server.abort();
    }

    #[test]
    fn negative_metadata_entry_is_reconfirmed_over_the_wire_after_sibling_create() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(CrossVmGateway {
            content: b"hello".to_vec(),
            revision: 41,
            present: false,
            tree_requests: 0,
            metadata_many_requests: 0,
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(cross_vm_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "token", "vm-b").unwrap();
        let fs = RemoteFuseFs::new(client.clone(), false, "vm-b", runtime.handle().clone());

        // B observes the file missing and caches a revision-fenced ENOENT.
        assert!(fs.stat_path_attributes("file").unwrap().is_none());
        assert_eq!(client.coherence_revision(), 41);

        // A sibling creates the file and publishes R_write; B stays idle.
        {
            let mut state = gateway.lock().unwrap();
            state.present = true;
            state.revision = 42;
        }
        assert_eq!(client.coherence_revision(), 41);
        let metadata_requests_before = gateway.lock().unwrap().metadata_many_requests;

        // B's next lookup must reconfirm over the wire and find the file, not
        // serve the stale negative entry as ENOENT.
        let metadata = fs
            .stat_path_attributes("file")
            .unwrap()
            .expect("sibling-created file must be visible after a wire reconfirmation");
        assert_eq!(metadata.size_bytes, 5);
        assert_eq!(
            gateway.lock().unwrap().metadata_many_requests,
            metadata_requests_before + 1
        );
        assert_eq!(client.coherence_revision(), 42);
        server.abort();
    }

    // Hole #2 reproduction (cross-process subtree-snapshot fast path). An idle
    // mount warms a revision-fenced subtree snapshot in one dispatch, then a
    // sibling in ANOTHER process extends every file and publishes R_write. The
    // idle mount never observes that publication, so its fence stays at the old
    // revision and the shared subtree snapshot is still tagged for it. Its next
    // getattr must reconfirm over the wire (the MetadataBatcher) rather than
    // serving the stale prior-dispatch snapshot entry with zero wire calls.
    #[test]
    fn idle_observer_subtree_snapshot_stat_reconfirms_over_the_wire_after_sibling_write() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(SubtreeSnapshotGateway {
            revision: 17,
            size_offset: 0,
            subtree_requests: 0,
            fallback_requests: 0,
            watch_serves: false,
            watch_requests: 0,
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(subtree_snapshot_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        client.observe_published_revision(17);
        let fs = RemoteFuseFs::new(client.clone(), false, "test-scope", runtime.handle().clone());

        // Warm a revision-17 subtree snapshot: seven sub-threshold point stats,
        // then one past the miss threshold and quiet period triggers the shared
        // snapshot that primes every file at revision 17.
        for index in 0..7 {
            assert_eq!(
                fs.stat_path_attributes(&format!("file-{index}"))
                    .unwrap()
                    .unwrap()
                    .size_bytes,
                index
            );
        }
        std::thread::sleep(SUBTREE_LOAD_REVISION_QUIET_PERIOD);
        assert_eq!(
            fs.stat_path_attributes("file-7")
                .unwrap()
                .unwrap()
                .size_bytes,
            7
        );
        assert_eq!(gateway.lock().unwrap().subtree_requests, 1);
        assert_eq!(client.coherence_revision(), 17);

        // A sibling in another process extends every file and publishes
        // R_write = 18. This idle mount never observes that publication.
        {
            let mut state = gateway.lock().unwrap();
            state.revision = 18;
            state.size_offset = 10_000;
        }
        let fallback_before = gateway.lock().unwrap().fallback_requests;

        // The idle observer's next stat must reconfirm over the wire, not serve
        // the stale revision-17 snapshot entry with zero wire calls.
        assert_eq!(
            fs.stat_path_attributes("file-3")
                .unwrap()
                .unwrap()
                .size_bytes,
            10_003,
            "an idle mount must not serve a prior-dispatch subtree snapshot entry \
             without a wire reconfirmation"
        );
        assert_eq!(
            gateway.lock().unwrap().fallback_requests,
            fallback_before + 1,
            "the reconfirmation must be a single coalesced metadata wire call"
        );
        assert_eq!(client.coherence_revision(), 18);
        server.abort();
    }

    struct RevisionWatchGateway {
        revision: u64,
        present: bool,
        size_bytes: u64,
        tree_requests: usize,
        metadata_many_requests: usize,
        watch_serves: bool,
        watch_requests: usize,
    }

    async fn revision_watch_gateway(
        State(state): State<Arc<Mutex<RevisionWatchGateway>>>,
        request: Request<Body>,
    ) -> Response {
        let method = request.method().clone();
        let path = request.uri().path().to_string();
        match (method, path.as_str()) {
            (Method::GET, "/tree") => {
                let (present, size_bytes, revision) = {
                    let mut state = state.lock().unwrap();
                    state.tree_requests += 1;
                    (state.present, state.size_bytes, state.revision)
                };
                let entries: Vec<chevalier_sandbox::vfs::VfsDirEntry> = if present {
                    vec![chevalier_sandbox::vfs::VfsDirEntry {
                        name: "file".to_string(),
                        kind: "file".to_string(),
                        size_bytes,
                        file_id: Some("watch-file".to_string()),
                        link_count: 1,
                        link_target: None,
                        content_hash: None,
                        executable: false,
                        mode: Some(0o644),
                        updated_at: None,
                    }]
                } else {
                    Vec::new()
                };
                with_revision_header(Json(entries).into_response(), revision)
            }
            (Method::POST, "/metadata-many") => {
                let body = to_bytes(request.into_body(), 1024 * 1024)
                    .await
                    .expect("read metadata-many request");
                let payload: VfsMetadataManyRequest =
                    serde_json::from_slice(&body).expect("decode metadata-many request");
                let (present, size_bytes, revision) = {
                    let mut state = state.lock().unwrap();
                    state.metadata_many_requests += 1;
                    (state.present, state.size_bytes, state.revision)
                };
                let entries = payload
                    .paths
                    .iter()
                    .map(|_| {
                        present.then(|| RemoteMetadata {
                            kind: "file".to_string(),
                            size_bytes,
                            file_id: Some("watch-file".to_string()),
                            link_count: 1,
                            link_target: None,
                            content_hash: None,
                            executable: false,
                            mode: Some(0o644),
                            updated_at: None,
                        })
                    })
                    .collect();
                with_revision_header(
                    Json(VfsMetadataManyResponse { entries }).into_response(),
                    revision,
                )
            }
            (Method::GET, "/watch") => {
                let (serves, count, revision) = {
                    let mut state = state.lock().unwrap();
                    state.watch_requests += 1;
                    (state.watch_serves, state.watch_requests, state.revision)
                };
                serve_revision_watch(serves, count, revision).await
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    // Test 4a: the watch task drives the shared fence and fails the cache closed.
    // A 200 carrying a newer owner revision must advance coherence and clear a
    // stale prior-revision entry through the audited authoritative path, so it
    // can never be served again.
    #[test]
    fn watch_task_advances_fence_and_clears_stale_cache() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(RevisionWatchGateway {
            revision: 18,
            present: true,
            size_bytes: 10,
            tree_requests: 0,
            metadata_many_requests: 0,
            watch_serves: true,
            watch_requests: 0,
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(revision_watch_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "token", "test-scope").unwrap();
        client.observe_published_revision(17);
        let cache = RemoteFuseCache::shared(&client.coherence_key());
        cache.put_metadata(
            "file",
            RemoteMetadata {
                kind: "file".to_string(),
                size_bytes: 5,
                file_id: Some("watch-file".to_string()),
                link_count: 1,
                link_target: None,
                content_hash: None,
                executable: false,
                mode: Some(0o644),
                updated_at: None,
            },
            17,
        );
        assert_eq!(
            cache.get_metadata("file", 17).map(|metadata| metadata.size_bytes),
            Some(5)
        );

        // Start the watch directly against the shared cache; no mount involved.
        let invalidators = MountInvalidators::shared(&client.coherence_key());
        client.ensure_revision_watch(runtime.handle(), &cache, &invalidators);
        await_watch_live(&client);

        assert_eq!(client.coherence_revision(), 18);
        assert!(
            cache.get_metadata("file", 17).is_none(),
            "the stale revision-17 entry must be cleared once the watch observes revision 18"
        );
        server.abort();
    }

    // Test 4b: under a live watch, a fence-matched listing and attribute stat
    // serve from cache with zero additional wire calls.
    #[test]
    fn live_watch_serves_dir_and_metadata_without_wire() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(RevisionWatchGateway {
            revision: 17,
            present: true,
            size_bytes: 5,
            tree_requests: 0,
            metadata_many_requests: 0,
            watch_serves: true,
            watch_requests: 0,
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(revision_watch_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "token", "test-scope").unwrap();
        client.observe_published_revision(17);
        let fs = RemoteFuseFs::new(client.clone(), false, "test-scope", runtime.handle().clone());
        await_watch_live(&client);

        // Under a live watch, replies hand the kernel the positive attr/entry
        // lease (the kernel becomes a fast tier of the fence-gated cache).
        assert_eq!(fs.reply_ttl(), ATTR_ENTRY_LEASE_TTL);

        // One wire listing warms the cache (and primes the child's metadata).
        let dir = fs.dir_entries("").unwrap();
        assert_eq!(dir.len(), 1);
        assert_eq!(dir[0].size_bytes, 5);
        assert_eq!(gateway.lock().unwrap().tree_requests, 1);

        // The second listing serves get_dir off the confirmed fence: no /tree.
        let dir_again = fs.dir_entries("").unwrap();
        assert_eq!(dir_again[0].size_bytes, 5);
        assert_eq!(
            gateway.lock().unwrap().tree_requests,
            1,
            "a live watch must let dir_entries serve get_dir without a wire refetch"
        );

        // Attribute stats serve get_metadata off the same fence: no /metadata-many.
        assert_eq!(
            fs.stat_path_attributes("file").unwrap().unwrap().size_bytes,
            5
        );
        assert_eq!(
            fs.stat_path_attributes("file").unwrap().unwrap().size_bytes,
            5
        );
        assert_eq!(
            gateway.lock().unwrap().metadata_many_requests,
            0,
            "a live watch must let stat_path_attributes serve get_metadata without a wire"
        );
        assert_eq!(client.coherence_revision(), 17);
        server.abort();
    }

    // Test 4c: with the watch down (the endpoint errors), watch_live stays false
    // and every serve reconfirms over the wire (exactly today's strict behavior).
    #[test]
    fn watch_down_forces_strict_serves() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(RevisionWatchGateway {
            revision: 17,
            present: true,
            size_bytes: 5,
            tree_requests: 0,
            metadata_many_requests: 0,
            watch_serves: false,
            watch_requests: 0,
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(revision_watch_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "token", "test-scope").unwrap();
        client.observe_published_revision(17);
        let fs = RemoteFuseFs::new(client.clone(), false, "test-scope", runtime.handle().clone());
        assert!(!client.revision_watch_live());

        // Fail-closed: with the watch down every reply hands the kernel a zero
        // TTL, so no lease can outlive the confirmed fence and every op reaches
        // userspace to reconfirm.
        assert_eq!(fs.reply_ttl(), Duration::ZERO);

        // Warm the caches with one listing and one attribute stat.
        assert_eq!(fs.dir_entries("").unwrap()[0].size_bytes, 5);
        assert_eq!(
            fs.stat_path_attributes("file").unwrap().unwrap().size_bytes,
            5
        );
        assert_eq!(gateway.lock().unwrap().tree_requests, 1);
        assert_eq!(gateway.lock().unwrap().metadata_many_requests, 1);

        // With the watch down, every repeat reconfirms over the wire.
        assert_eq!(fs.dir_entries("").unwrap()[0].size_bytes, 5);
        assert_eq!(
            fs.stat_path_attributes("file").unwrap().unwrap().size_bytes,
            5
        );
        assert_eq!(
            gateway.lock().unwrap().tree_requests,
            2,
            "strict mode must refetch the listing over the wire"
        );
        assert_eq!(
            gateway.lock().unwrap().metadata_many_requests,
            2,
            "strict mode must reconfirm attributes over the wire"
        );
        assert!(!client.revision_watch_live());
        server.abort();
    }

    // Hole #1 coherence guarantee (in-process shared cache write publication).
    // Two mounts of the SAME scope in one vmd process share one RemoteFuseCache
    // and one SharedRevisionState. When the actor mount's write journal commits
    // a content extension, its commit hook publishes the write onto the shared
    // cache (observe_write_publication_snapshot) and write_many advances the
    // shared coherence fence. The observer mount must then reflect the new size.
    // This drives the exact commit-hook effect and inputs the write journal
    // produces for a content write, including the deployed gateway's empty
    // publication-snapshot entry set.
    #[test]
    fn in_process_observer_reflects_sibling_write_publication_on_the_shared_cache() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(Mutex::new(CrossVmGateway {
            content: b"01234".to_vec(),
            revision: 17,
            present: true,
            tree_requests: 0,
            metadata_many_requests: 0,
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(cross_vm_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });

        // Observer and actor share one scope, hence one coherence key, hence one
        // shared cache and revision fence.
        let observer_client = RemoteVfsClient::new(&endpoint, "token", "test-scope").unwrap();
        let observer = RemoteFuseFs::new(
            observer_client.clone(),
            false,
            "test-scope",
            runtime.handle().clone(),
        );
        let actor_client = RemoteVfsClient::new(&endpoint, "token", "test-scope").unwrap();
        assert_eq!(actor_client.coherence_key(), observer_client.coherence_key());
        let shared_cache = RemoteFuseCache::shared(&actor_client.coherence_key());

        // Observer warms its cache and the shared fence at revision 17.
        assert_eq!(observer.dir_entries("").unwrap().len(), 1);
        assert_eq!(
            observer
                .stat_path_attributes("file")
                .unwrap()
                .unwrap()
                .size_bytes,
            5
        );
        assert_eq!(observer_client.coherence_revision(), 17);

        // The actor extends the file to ten bytes and publishes R_write = 18.
        {
            let mut state = gateway.lock().unwrap();
            state.content = b"0123456789".to_vec();
            state.revision = 18;
        }
        // The write journal's commit hook publishes the content write onto the
        // shared cache. The deployed gateway returns no publication-snapshot
        // entries for a content write, so the affected entry must be dropped
        // (invalidate-or-replace) and the next read must wire.
        shared_cache.observe_write_publication_snapshot(
            18,
            &[("file".to_string(), Some("stable-cross-vm-file".to_string()))],
            &[],
        );
        // write_many advanced the shared coherence fence to R_write.
        actor_client.observe_published_revision(18);

        // The in-process observer must reflect the sibling extension.
        assert_eq!(
            observer
                .stat_path_attributes("file")
                .unwrap()
                .unwrap()
                .size_bytes,
            10,
            "an in-process observer must reflect a sibling's committed write \
             through the shared cache"
        );
        assert_eq!(observer_client.coherence_revision(), 18);
        server.abort();
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
            InitFlags::FUSE_AUTO_INVAL_DATA
                | InitFlags::FUSE_POSIX_LOCKS
                | InitFlags::FUSE_FLOCK_LOCKS
                | InitFlags::FUSE_DO_READDIRPLUS
                | InitFlags::FUSE_READDIRPLUS_AUTO
        );
        assert_eq!(
            RemoteFuseFs::requested_init_capabilities_for(true),
            InitFlags::FUSE_AUTO_INVAL_DATA
                | InitFlags::FUSE_POSIX_LOCKS
                | InitFlags::FUSE_FLOCK_LOCKS
                | InitFlags::FUSE_DO_READDIRPLUS
                | InitFlags::FUSE_READDIRPLUS_AUTO
        );
    }

    #[test]
    fn remote_file_handles_bypass_uncoordinated_kernel_page_caches() {
        assert_eq!(
            remote_file_open_flags(),
            FopenFlags::FOPEN_DIRECT_IO,
            "cross-mount coherence must stay in the revision-aware FUSE layer"
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
        assert!(
            RemoteFuseFs::handle_metadata_needs_remote_route(existing),
            "clean handles must revalidate cross-mount changes"
        );

        let created = handles.files.get(&created_handle).unwrap();
        assert_eq!(created.mode, 0o640);
        assert!(
            !RemoteFuseFs::handle_metadata_needs_remote_route(created),
            "a new open-file description is locally authoritative"
        );
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

        let mut dirty = existing.clone();
        dirty.dirty = true;
        assert!(
            !RemoteFuseFs::handle_metadata_needs_remote_route(&dirty),
            "dirty open-file metadata must not re-stat an older pathname"
        );
        let mut pending = existing.clone();
        pending.pending_publication = Some(HandlePublication {
            id: 7,
            revision: 3,
            path: "existing".to_string(),
            file_id: None,
            size_bytes: 0,
            content_hash: content_hash_for_bytes(&[]),
            mode: 0o751,
        });
        assert!(
            !RemoteFuseFs::handle_metadata_needs_remote_route(&pending),
            "pending publication remains authoritative until its exact barrier completes"
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
    fn last_unlink_retires_only_the_dead_identity_reverse_mapping() {
        let mut table = InodeTable::new();
        let original = table.lookup_with_identity("old", Some("unix:1:42"));

        assert!(table.detach_unlinked_identity("unix:1:42"));
        let replacement = table.lookup_with_identity("replacement", Some("unix:1:42"));

        assert_ne!(
            original, replacement,
            "a recycled Unix inode is a new object"
        );
        assert_eq!(
            table.route(original),
            Some(("old".to_string(), Some("unix:1:42".to_string()))),
            "the old inode record survives until its delayed kernel FORGET"
        );
        assert_eq!(
            table.route(replacement),
            Some(("replacement".to_string(), Some("unix:1:42".to_string())))
        );
    }

    #[test]
    fn authoritative_last_unlink_detaches_every_locally_known_alias() {
        let mut table = InodeTable::new();
        let original = table.lookup_with_identity("first", Some("unix:1:42"));
        assert_eq!(
            table.lookup_with_identity("surviving", Some("unix:1:42")),
            original
        );

        assert!(table.detach_unlinked_identity("unix:1:42"));
        assert!(
            table
                .aliases_for_path("first")
                .iter()
                .all(|path| path == "first")
        );
        assert!(
            table
                .aliases_for_path("surviving")
                .iter()
                .all(|path| path == "surviving")
        );
        assert_ne!(
            table.ensure_with_identity("third", Some("unix:1:42")),
            original
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

    struct BarrierGateway {
        namespace_hit: Arc<Notify>,
        release_namespace: Arc<Notify>,
    }

    async fn descendant_barrier_gateway(
        State(state): State<Arc<BarrierGateway>>,
        request: Request<Body>,
    ) -> Response {
        let method = request.method().clone();
        let path = request.uri().path().to_string();
        let revision = |mut response: Response| -> Response {
            response.headers_mut().insert(
                HeaderName::from_static("x-chevalier-vfs-namespace-revision"),
                HeaderValue::from_static("1"),
            );
            response
        };
        match (method, path.as_str()) {
            (Method::POST, "/lease") => Json(serde_json::json!({
                "resource_key": "descendant-barrier-test",
                "owner_token": "00000000-0000-0000-0000-000000000001",
                "task_id": null
            }))
            .into_response(),
            (Method::DELETE, "/lease") => StatusCode::NO_CONTENT.into_response(),
            (Method::POST, "/namespace-many") => {
                // Hold the RemoveDirectory publication open so the descendant
                // write-barrier it installed stays active while the test probes
                // concurrent write enqueues.
                state.namespace_hit.notify_one();
                state.release_namespace.notified().await;
                revision(StatusCode::OK.into_response())
            }
            (Method::POST, "/write-many") => {
                revision(Json(serde_json::json!({"results": [], "entries": []})).into_response())
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    #[test]
    fn descendant_write_barrier_blocks_only_the_deleted_subtree_until_rmdir_resolves() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gateway = Arc::new(BarrierGateway {
            namespace_hit: Arc::new(Notify::new()),
            release_namespace: Arc::new(Notify::new()),
        });
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(descendant_barrier_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let journal_dir = tempfile::tempdir().unwrap();
        let client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        let fs = Arc::new(
            RemoteFuseFs::new_with_namespace_journal(
                client,
                false,
                "test-scope",
                &journal_dir.path().join("namespace.jsonl"),
                runtime.handle().clone(),
            )
            .unwrap(),
        );

        // A RemoveDirectory{doomed} publication held in flight by the gateway,
        // so its descendant write-barrier stays installed for the probes below.
        let commit_fs = Arc::clone(&fs);
        let remover = std::thread::spawn(move || {
            commit_fs.commit_namespace(VfsNamespaceMutation::RemoveDirectory {
                path: "doomed".to_string(),
            })
        });
        runtime.block_on(async { gateway.namespace_hit.notified().await });

        // A write to a descendant of the deleted directory must block behind the
        // barrier; a write to an unrelated path must not.
        let (barred_tx, barred_rx) = mpsc::channel();
        let barred_fs = Arc::clone(&fs);
        let barred_writer = std::thread::spawn(move || {
            let result = barred_fs.writes.as_ref().unwrap().enqueue(
                "doomed/child.rs",
                b"resurrect",
                None,
                None,
            );
            barred_tx.send(result.is_ok()).unwrap();
        });

        fs.writes
            .as_ref()
            .unwrap()
            .enqueue("elsewhere/keep.rs", b"unrelated", None, None)
            .expect("a write outside the deleted subtree is never barred");
        assert!(
            barred_rx.recv_timeout(Duration::from_millis(500)).is_err(),
            "a write into the in-flight RemoveDirectory subtree must block on the barrier"
        );

        // Let the RemoveDirectory finish; the barrier clears and the parked
        // descendant write proceeds.
        gateway.release_namespace.notify_one();
        remover
            .join()
            .unwrap()
            .expect("RemoveDirectory publication completes once released");
        assert_eq!(
            barred_rx.recv_timeout(Duration::from_secs(5)),
            Ok(true),
            "the barred write unblocks and completes after the rmdir resolves"
        );
        barred_writer.join().unwrap();

        drop(fs);
        server.abort();
    }

    async fn always_ok_write_gateway(request: Request<Body>) -> Response {
        let revision = |mut response: Response| -> Response {
            response.headers_mut().insert(
                HeaderName::from_static("x-chevalier-vfs-namespace-revision"),
                HeaderValue::from_static("1"),
            );
            response
        };
        match (request.method().clone(), request.uri().path()) {
            (Method::POST, "/lease") => Json(serde_json::json!({
                "resource_key": "barrier-gate-holder-test",
                "owner_token": "00000000-0000-0000-0000-000000000001",
                "task_id": null
            }))
            .into_response(),
            (Method::DELETE, "/lease") => StatusCode::NO_CONTENT.into_response(),
            (Method::POST, "/write-many") => {
                revision(Json(serde_json::json!({"results": [], "entries": []})).into_response())
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    /// Regression for the descendant write-barrier's interaction with the
    /// per-handle publication gate. A content flush parks in `enqueue` on a
    /// barred subtree *while still holding that handle's publication gate*
    /// (exactly what `flush_handle_locked` does). A namespace rename's gate
    /// collection (`publication_gates_for_subtrees` + `lock_publication_gates`)
    /// then needs that same gate. The barred write must stay strictly ordered
    /// behind the barrier (never slip past), and the whole interleaving must
    /// resolve in bounded time once the barrier clears: the barrier owner
    /// (a delete/rmdir) never needs the gate, so dropping the barrier unparks
    /// the writer, which releases the gate to the rename. No cycle.
    #[test]
    fn descendant_write_barrier_gate_holder_orders_behind_namespace_without_deadlock() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/{*path}", any(always_ok_write_gateway)),
            )
            .await
            .unwrap();
        });
        let journal_dir = tempfile::tempdir().unwrap();
        let client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        let fs = Arc::new(
            RemoteFuseFs::new_with_namespace_journal(
                client,
                false,
                "test-scope",
                &journal_dir.path().join("namespace.jsonl"),
                runtime.handle().clone(),
            )
            .unwrap(),
        );
        // An open handle under the doomed subtree; its publication gate is the
        // one both the parked writer and the rename contend for.
        let handle = fs
            .next_handle(
                "doomed/child.rs",
                Vec::new(),
                true,
                None,
                0o644,
                false,
                Some("doomed-child".to_string()),
                1,
            )
            .unwrap();

        // Model an in-flight RemoveDirectory{doomed}: its descendant
        // write-barrier is installed and stays active for the probes below.
        // (A real rmdir/delete installs this from inside commit while holding
        // `namespace_publication_gate`; it never acquires a handle gate.)
        let barrier = fs
            .writes
            .as_ref()
            .unwrap()
            .install_descendant_barrier(vec!["doomed".to_string()]);

        // The writer takes the handle's publication gate (as flush_handle_locked
        // does) and then enqueues into the barred subtree, parking on the barrier
        // while still holding the gate.
        let (gate_held_tx, gate_held_rx) = mpsc::channel();
        let (enqueued_tx, enqueued_rx) = mpsc::channel();
        let writer_fs = Arc::clone(&fs);
        let writer = std::thread::spawn(move || {
            let gate = writer_fs.publication_gate_for_handle(handle).unwrap();
            let held = gate.lock().unwrap();
            // Announce ownership so the rename below deterministically contends
            // for a gate the writer already holds (no acquisition-order race).
            gate_held_tx.send(()).unwrap();
            let result = writer_fs.writes.as_ref().unwrap().enqueue(
                "doomed/child.rs",
                b"resurrect",
                None,
                None,
            );
            drop(held);
            enqueued_tx.send(result.is_ok()).unwrap();
        });
        // Only start the rename once the writer owns the gate.
        gate_held_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        // A concurrent rename over the doomed subtree collects and locks the
        // publication gates its handles share — exactly the multi-gate
        // acquisition rename performs before it commits.
        let (renamed_tx, renamed_rx) = mpsc::channel();
        let rename_fs = Arc::clone(&fs);
        let rename = std::thread::spawn(move || {
            let gates = rename_fs
                .publication_gates_for_subtrees(&["doomed"])
                .unwrap();
            let _guards = RemoteFuseFs::lock_publication_gates(&gates).unwrap();
            renamed_tx.send(gates.len()).unwrap();
        });

        // While the barrier is active the write is parked (strictly ordered
        // behind the namespace mutation) and the rename cannot acquire the gate
        // the parked writer holds.
        assert!(
            enqueued_rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "a write into a barred subtree must park behind the barrier"
        );
        assert!(
            renamed_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "rename's gate collection must wait for the gate-holding writer"
        );

        // Dropping the barrier (as the delete/rmdir commit does on return)
        // unparks the writer; it appends and releases the gate, and the rename
        // then acquires it. The whole graph drains in bounded time.
        drop(barrier);
        assert_eq!(
            enqueued_rx.recv_timeout(Duration::from_secs(5)),
            Ok(true),
            "the barred write completes once the barrier clears"
        );
        assert_eq!(
            renamed_rx.recv_timeout(Duration::from_secs(5)),
            Ok(1),
            "rename acquires the freed gate after the writer releases it"
        );
        writer.join().unwrap();
        rename.join().unwrap();

        drop(fs);
        server.abort();
    }

    /// A rename over a subtree collects the publication gate of every handle it
    /// contains (publication_gates_for_subtrees) and then locks them in fh order
    /// (lock_publication_gates). All handles for one inode SHARE a single gate
    /// (next_handle clones the sibling's Arc), so two such handles under the
    /// renamed subtree — two descriptors on one file, or two hard-link aliases —
    /// yield the same non-reentrant `Mutex` twice in that list. Locking it a
    /// second time on the same thread self-deadlocks the rename. The collection
    /// must therefore acquire each distinct gate exactly once.
    #[test]
    fn rename_gate_collection_locks_each_shared_gate_once_without_self_deadlock() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let fs = Arc::new(RemoteFuseFs::new(
            client,
            false,
            "test-scope",
            runtime.handle().clone(),
        ));
        // Two open descriptors on the same inode under the renamed subtree. They
        // share one publication gate, exactly as next_handle wires siblings.
        let first = fs
            .next_handle(
                "doomed/child.rs",
                Vec::new(),
                true,
                None,
                0o644,
                false,
                Some("doomed-child".to_string()),
                1,
            )
            .unwrap();
        let second = fs
            .next_handle(
                "doomed/child.rs",
                Vec::new(),
                true,
                None,
                0o644,
                false,
                Some("doomed-child".to_string()),
                1,
            )
            .unwrap();
        assert!(
            Arc::ptr_eq(
                &fs.publication_gate_for_handle(first).unwrap(),
                &fs.publication_gate_for_handle(second).unwrap(),
            ),
            "same-inode handles must share one publication gate"
        );

        // Acquire the subtree's gates on a worker thread so a self-deadlock
        // surfaces as a bounded timeout rather than hanging the test binary.
        let (done_tx, done_rx) = mpsc::channel();
        let probe_fs = Arc::clone(&fs);
        let probe = std::thread::spawn(move || {
            let gates = probe_fs.publication_gates_for_subtrees(&["doomed"]).unwrap();
            let guards = RemoteFuseFs::lock_publication_gates(&gates).unwrap();
            // Both descriptors are collected (so rename can flush each), but the
            // shared gate is locked once — the pair that would self-deadlock.
            done_tx.send((gates.len(), guards.len())).unwrap();
        });
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(3)),
            Ok((2, 1)),
            "rename's multi-gate acquisition must lock each shared gate once, not self-deadlock"
        );
        probe.join().unwrap();
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

    /// Gateway requests per route, the unit `op_class_round_trips` measures.
    ///
    /// Round trips x network RTT is a filesystem operation's user-visible
    /// latency, so the benchmark counts requests per route rather than wall
    /// clock against a loopback stub (which would report a link that does not
    /// exist).
    #[derive(Clone, Copy, Debug, Default)]
    struct RouteCounts {
        tree: usize,
        stat: usize,
        metadata_many: usize,
        subtree_metadata: usize,
        prefetch_subtree: usize,
        namespace_many: usize,
        write_many: usize,
        file: usize,
        lease: usize,
        /// The revision watch is a background long poll no operation waits on,
        /// so it is recorded but never charged to an operation.
        watch: usize,
    }

    impl RouteCounts {
        /// Round trips charged to the operations of a class.
        fn charged(&self) -> usize {
            self.tree
                + self.stat
                + self.metadata_many
                + self.subtree_metadata
                + self.prefetch_subtree
                + self.namespace_many
                + self.write_many
                + self.file
                + self.lease
        }

        fn since(&self, base: &Self) -> Self {
            Self {
                tree: self.tree - base.tree,
                stat: self.stat - base.stat,
                metadata_many: self.metadata_many - base.metadata_many,
                subtree_metadata: self.subtree_metadata - base.subtree_metadata,
                prefetch_subtree: self.prefetch_subtree - base.prefetch_subtree,
                namespace_many: self.namespace_many - base.namespace_many,
                write_many: self.write_many - base.write_many,
                file: self.file - base.file,
                lease: self.lease - base.lease,
                watch: self.watch - base.watch,
            }
        }

        fn breakdown(&self) -> String {
            let routes = [
                ("/tree", self.tree),
                ("/stat", self.stat),
                ("/metadata-many", self.metadata_many),
                ("/subtree-metadata", self.subtree_metadata),
                ("/prefetch-subtree", self.prefetch_subtree),
                ("/namespace-many", self.namespace_many),
                ("/write-many", self.write_many),
                ("/file", self.file),
                ("/lease", self.lease),
            ];
            let breakdown = routes
                .iter()
                .filter(|(_, count)| *count > 0)
                .map(|(route, count)| format!("{route}={count}"))
                .collect::<Vec<_>>();
            if breakdown.is_empty() {
                "-".to_string()
            } else {
                breakdown.join(" ")
            }
        }
    }

    /// Stub gateway for `op_class_round_trips`: an authoritative namespace plus
    /// a per-route request counter. Faithful to the production gateway
    /// (ts/vfs-gateway-server.ts) on the two behaviors that change the
    /// round-trip count — leases are advertised implicit, so the client stops
    /// issuing them after the first grant, and every response carries the
    /// namespace revision header.
    struct RoundTripGateway {
        scope: String,
        /// Scoped path -> file size. Directories are implied by their entries.
        files: std::collections::BTreeMap<String, u64>,
        revision: u64,
        counts: RouteCounts,
    }

    /// Every file the stub serves is empty, which is exactly the shape a
    /// freshly reserved file has: `published_file_matches` verifies size, hash,
    /// mode and identity against this.
    fn round_trip_file_metadata(path: &str) -> RemoteMetadata {
        RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: 0,
            file_id: Some(format!("identity:{path}")),
            link_count: 1,
            link_target: None,
            content_hash: Some(content_hash_for_bytes(&[])),
            executable: false,
            mode: Some(0o644),
            updated_at: None,
        }
    }

    /// Direct children of `dir`, or `None` when the directory does not exist.
    fn round_trip_children(
        state: &RoundTripGateway,
        dir: &str,
    ) -> Option<Vec<chevalier_sandbox::vfs::VfsDirEntry>> {
        let prefix = format!("{dir}/");
        let mut files = Vec::new();
        let mut directories = std::collections::BTreeSet::new();
        for path in state.files.keys() {
            let Some(rest) = path.strip_prefix(prefix.as_str()) else {
                continue;
            };
            match rest.split_once('/') {
                Some((name, _)) => {
                    directories.insert(name.to_string());
                }
                None => files.push(chevalier_sandbox::vfs::VfsDirEntry {
                    name: rest.to_string(),
                    kind: "file".to_string(),
                    size_bytes: 0,
                    file_id: Some(format!("identity:{path}")),
                    link_count: 1,
                    link_target: None,
                    content_hash: Some(content_hash_for_bytes(&[])),
                    executable: false,
                    mode: Some(0o644),
                    updated_at: None,
                }),
            }
        }
        if files.is_empty() && directories.is_empty() && dir != state.scope {
            return None;
        }
        for name in directories {
            files.push(chevalier_sandbox::vfs::VfsDirEntry {
                name,
                kind: "directory".to_string(),
                size_bytes: 0,
                file_id: None,
                link_count: 1,
                link_target: None,
                content_hash: None,
                executable: false,
                mode: Some(0o755),
                updated_at: None,
            });
        }
        Some(files)
    }

    /// Minimal percent-decoder for the stub's query parameters: the client
    /// encodes path separators and vmd carries no urlencoding dependency.
    fn decode_query_value(value: &str) -> String {
        let bytes = value.as_bytes();
        let mut decoded = String::with_capacity(value.len());
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'%' if index + 3 <= bytes.len() => {
                    match u8::from_str_radix(&value[index + 1..index + 3], 16) {
                        Ok(byte) => {
                            decoded.push(byte as char);
                            index += 3;
                        }
                        Err(_) => {
                            decoded.push('%');
                            index += 1;
                        }
                    }
                }
                byte => {
                    decoded.push(byte as char);
                    index += 1;
                }
            }
        }
        decoded
    }

    fn round_trip_query_path(request: &Request<Body>) -> String {
        request
            .uri()
            .query()
            .unwrap_or_default()
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .find(|(name, _)| *name == "path")
            .map(|(_, value)| decode_query_value(value))
            .unwrap_or_default()
    }

    async fn round_trip_gateway(
        State(state): State<Arc<Mutex<RoundTripGateway>>>,
        request: Request<Body>,
    ) -> Response {
        let method = request.method().clone();
        let route = request.uri().path().to_string();
        let path = round_trip_query_path(&request);
        match (method, route.as_str()) {
            (Method::GET, "/tree") => {
                let mut state = state.lock().unwrap();
                state.counts.tree += 1;
                let revision = state.revision;
                match round_trip_children(&state, path.as_str()) {
                    Some(entries) => with_revision_header(Json(entries).into_response(), revision),
                    None => with_revision_header(StatusCode::NOT_FOUND.into_response(), revision),
                }
            }
            (Method::GET, "/stat") => {
                let mut state = state.lock().unwrap();
                state.counts.stat += 1;
                let revision = state.revision;
                match state.files.get(path.as_str()) {
                    Some(_) => with_revision_header(
                        Json(round_trip_file_metadata(path.as_str())).into_response(),
                        revision,
                    ),
                    None => with_revision_header(StatusCode::NOT_FOUND.into_response(), revision),
                }
            }
            (Method::POST, "/metadata-many") => {
                let body = to_bytes(request.into_body(), 4 * 1024 * 1024)
                    .await
                    .expect("read metadata-many request");
                let payload: VfsMetadataManyRequest =
                    serde_json::from_slice(&body).expect("decode metadata-many request");
                let mut state = state.lock().unwrap();
                state.counts.metadata_many += 1;
                let revision = state.revision;
                let entries = payload
                    .paths
                    .iter()
                    .map(|path| {
                        state
                            .files
                            .contains_key(path.as_str())
                            .then(|| round_trip_file_metadata(path))
                    })
                    .collect();
                with_revision_header(
                    Json(VfsMetadataManyResponse { entries }).into_response(),
                    revision,
                )
            }
            (Method::POST, "/subtree-metadata") => {
                let body = to_bytes(request.into_body(), 4 * 1024 * 1024)
                    .await
                    .expect("read subtree-metadata request");
                let payload: chevalier_sandbox::vfs::VfsSubtreeMetadataRequest =
                    serde_json::from_slice(&body).expect("decode subtree-metadata request");
                let prefix = if payload.prefix.is_empty() {
                    String::new()
                } else {
                    format!("{}/", payload.prefix)
                };
                let mut state = state.lock().unwrap();
                state.counts.subtree_metadata += 1;
                let revision = state.revision;
                let entries = state
                    .files
                    .keys()
                    .filter(|path| path.starts_with(prefix.as_str()))
                    .map(|path| VfsSubtreeMetadataEntry {
                        path: path.clone(),
                        kind: "file".to_string(),
                        size_bytes: 0,
                        file_id: Some(format!("identity:{path}")),
                        link_count: 1,
                        link_target: None,
                        content_hash: Some(content_hash_for_bytes(&[])),
                        executable: false,
                        mode: Some(0o644),
                        token_count: None,
                        version: None,
                        updated_at: None,
                        object_state: None,
                    })
                    .collect();
                with_revision_header(
                    Json(VfsSubtreeMetadataResponse { entries }).into_response(),
                    revision,
                )
            }
            (Method::POST, "/prefetch-subtree") => {
                let mut state = state.lock().unwrap();
                state.counts.prefetch_subtree += 1;
                let revision = state.revision;
                // Every file the stub serves is empty, so a warmed-bytes pack
                // carries nothing; the route still round-trips, which is the
                // point of counting it.
                with_revision_header(
                    Json(chevalier_sandbox::vfs::VfsPrefetchSubtreeResponse {
                        warmed_file_bytes: Vec::new(),
                    })
                    .into_response(),
                    revision,
                )
            }
            (Method::POST, "/namespace-many") => {
                let body = to_bytes(request.into_body(), 4 * 1024 * 1024)
                    .await
                    .expect("read namespace-many request");
                let payload: chevalier_sandbox::vfs::VfsNamespaceMutationBatchBody =
                    serde_json::from_slice(&body).expect("decode namespace-many request");
                let mut state = state.lock().unwrap();
                state.counts.namespace_many += 1;
                let mut entries = Vec::new();
                for mutation in &payload.mutations {
                    match mutation {
                        VfsNamespaceMutation::CreateFile { path, .. } => {
                            state.files.insert(path.clone(), 0);
                            entries.push(chevalier_sandbox::vfs::VfsPublicationSnapshotEntry {
                                path: path.clone(),
                                metadata: Some(round_trip_file_metadata(path)),
                            });
                        }
                        VfsNamespaceMutation::DeleteFile { path, .. } => {
                            state.files.remove(path.as_str());
                            entries.push(chevalier_sandbox::vfs::VfsPublicationSnapshotEntry {
                                path: path.clone(),
                                metadata: None,
                            });
                        }
                        _ => {}
                    }
                }
                // Every publication advances the namespace revision, exactly as
                // the gateway's publication sequencer does.
                state.revision += 1;
                let revision = state.revision;
                with_revision_header(
                    Json(chevalier_sandbox::vfs::VfsNamespaceMutationBatchResponse { entries })
                        .into_response(),
                    revision,
                )
            }
            (Method::POST, "/write-many") => {
                let mut state = state.lock().unwrap();
                state.counts.write_many += 1;
                state.revision += 1;
                let revision = state.revision;
                with_revision_header(
                    Json(serde_json::json!({"results": [], "entries": []})).into_response(),
                    revision,
                )
            }
            (Method::GET, "/file/raw") => {
                let mut state = state.lock().unwrap();
                state.counts.file += 1;
                let revision = state.revision;
                match state.files.get(path.as_str()) {
                    Some(_) => with_revision_header(Vec::new().into_response(), revision),
                    None => with_revision_header(StatusCode::NOT_FOUND.into_response(), revision),
                }
            }
            (Method::PUT, "/file") => {
                let mut state = state.lock().unwrap();
                state.counts.file += 1;
                state.files.insert(path.clone(), 0);
                state.revision += 1;
                let revision = state.revision;
                with_revision_header(StatusCode::NO_CONTENT.into_response(), revision)
            }
            (Method::POST, "/lease") => {
                state.lock().unwrap().counts.lease += 1;
                // The production gateway answers with implicit lease mode, which
                // tells the client to stop issuing lease round trips entirely.
                let mut response = Json(serde_json::json!({
                    "resource_key": "round-trip-benchmark",
                    "owner_token": "00000000-0000-0000-0000-000000000001",
                    "task_id": null
                }))
                .into_response();
                response.headers_mut().insert(
                    HeaderName::from_static("x-chevalier-vfs-lease-mode"),
                    HeaderValue::from_static("implicit"),
                );
                response
            }
            (Method::DELETE, "/lease") => {
                state.lock().unwrap().counts.lease += 1;
                StatusCode::NO_CONTENT.into_response()
            }
            (Method::GET, "/watch") => {
                let (polls, revision) = {
                    let mut state = state.lock().unwrap();
                    state.counts.watch += 1;
                    (state.counts.watch, state.revision)
                };
                serve_revision_watch(true, polls, revision).await
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    /// Gateway round trips per filesystem operation class.
    ///
    /// Round trips x network RTT is what a user actually waits for, so this is
    /// the measurement that predicts whether an operation clears the 100-500ms
    /// bar over a real link — wall clock against a loopback stub would report a
    /// link that does not exist. The table is printed for
    /// `cargo test -p vmd op_class_round_trips -- --nocapture`; the assertions
    /// below pin the properties each amortization mechanism is supposed to
    /// guarantee so a regression fails the build instead of silently costing
    /// every operation another RTT.
    ///
    /// Not covered: handle-based reads (`next_handle` + `ensure_handle_loaded`)
    /// and content flushes, whose plumbing needs a handle table set up by
    /// `open`/`create` dispatch rather than an fs-level entry point.
    #[test]
    fn op_class_round_trips() {
        const CREATE_OPS: usize = 50;
        const STAT_OPS: usize = 50;
        const SEED_FILES: usize = 64;

        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let mut files = std::collections::BTreeMap::new();
        for index in 0..SEED_FILES {
            files.insert(format!("test-scope/seed/file-{index}"), 0);
        }
        let gateway = Arc::new(Mutex::new(RoundTripGateway {
            scope: "test-scope".to_string(),
            files,
            revision: 17,
            counts: RouteCounts::default(),
        }));
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/{*path}", any(round_trip_gateway))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let journal_dir = tempfile::tempdir().unwrap();
        let client = RemoteVfsClient::new(&endpoint, "test-token", "test-scope").unwrap();
        let fs = RemoteFuseFs::new_with_namespace_journal(
            client.clone(),
            false,
            "test-scope",
            &journal_dir.path().join("namespace.jsonl"),
            runtime.handle().clone(),
        )
        .unwrap();
        // Every amortized serve path (cached attrs, cached listings, positive
        // kernel leases) is gated on a live revision watch, so the whole table
        // must be measured under one.
        await_watch_live(&client);

        let sample = || gateway.lock().unwrap().counts;
        // Start each class from a fresh fence: advance the gateway's revision
        // and take one throwaway wire call so this mount observes it. That
        // clears the shared cache (so a "cold" class really is cold) and
        // restarts the subtree-snapshot miss/quiet window, so no class inherits
        // another's warmth or trips a bulk snapshot mid-class. The bulk snapshot
        // has its own coverage in
        // `sequential_thousand_file_stats_amortize_under_a_live_watch`.
        let settle = || {
            gateway.lock().unwrap().revision += 1;
            assert!(fs.stat_path_attributes("settle-probe").unwrap().is_none());
        };
        // (class, ops, round-trip budget, measured counts). The budget is the
        // regression gate: what the class is allowed to cost, set at the
        // measured number plus only the slack a documented scheduling detail
        // needs, so a change that adds a round trip to a class fails the test.
        let mut rows: Vec<(&'static str, usize, usize, RouteCounts)> = Vec::new();

        // create_journaled: what a create costs when it is only the journaled
        // append it is designed to be — `enqueue_namespace_creation` for each
        // path, then the drain that publishes them. Enqueue and drain are
        // measured as one class on purpose: the worker's 8ms batch window may
        // fire partway through the loop, so how many batches the 50 creations
        // split into (one to a handful, plus the one-time lease grant) is a
        // scheduling detail, while their sum staying near one publication —
        // rather than one per creation — is the property that matters.
        let base = sample();
        for index in 0..CREATE_OPS {
            fs.enqueue_namespace_creation(
                VfsNamespaceMutation::CreateFile {
                    path: format!("queued/new-{index}"),
                    mode: Some(0o644),
                },
                None,
            )
            .unwrap();
        }
        let journaled_enqueue = sample().since(&base);
        fs.flush_namespace().unwrap();
        let create_journaled = sample().since(&base);
        // Budget 11: at most ten batched publications (the gate against one
        // publication per creation) plus the one-time /lease grant.
        rows.push(("create_journaled", CREATE_OPS, 11, create_journaled));

        // create (non-exclusive), the real syscall path: `reserve_file_if_absent`
        // journals the creation and then confirms the reservation.
        let base = sample();
        for index in 0..CREATE_OPS {
            let metadata = fs
                .reserve_file_if_absent(&format!("created/new-{index}"), 0o644, false)
                .unwrap();
            assert_eq!(metadata.kind, "file");
        }
        let create = sample().since(&base);
        // MEASURED GAP, not a target: this settles at three round trips per
        // create (two confirmation stats and the publication each create ends
        // up waiting for). The budget is the structural ceiling of the poll
        // loop `stat_published_file` runs — four stats plus one publication —
        // so a slow machine that misses a poll window does not fail the test
        // while a new round-trip class does. See the assertions below.
        rows.push(("create", CREATE_OPS, CREATE_OPS / 2, create));

        // create_drain: what is left to publish once those creates return.
        // Nothing is, because each create already waited out its own
        // publication.
        let base = sample();
        fs.flush_namespace().unwrap();
        let create_drain = sample().since(&base);
        // A genuinely deferred create leaves a tail for the drain to publish.
        rows.push(("create_drain", CREATE_OPS, 2, create_drain));

        // stat_cold: first touch of a path this mount has never seen.
        settle();
        let base = sample();
        for index in 0..STAT_OPS {
            assert!(
                fs.stat_path_attributes(&format!("seed/file-{index}"))
                    .unwrap()
                    .is_some()
            );
        }
        let stat_cold = sample().since(&base);
        // Budget 1 per op: one batched attribute read per unseen path. The two
        // spare trips tolerate a bulk subtree snapshot being elected inside the
        // class (/subtree-metadata + /prefetch-subtree), which can only ever
        // lower the per-path cost.
        rows.push(("stat_cold", STAT_OPS, STAT_OPS + 2, stat_cold));

        // stat_warm: the same paths again on the same fence. No settle — the
        // point is that a live watch keeps the fence confirmed so the attrs the
        // cold pass installed stay servable.
        let base = sample();
        for index in 0..STAT_OPS {
            assert!(
                fs.stat_path_attributes(&format!("seed/file-{index}"))
                    .unwrap()
                    .is_some()
            );
        }
        let stat_warm = sample().since(&base);
        // Budget 0: the whole point of the fence-confirmed serve.
        rows.push(("stat_warm", STAT_OPS, 0, stat_warm));

        // readdir: one full directory listing, cold.
        settle();
        let base = sample();
        assert_eq!(fs.dir_entries("seed").unwrap().len(), SEED_FILES);
        let readdir = sample().since(&base);
        // Budget 1: one listing, regardless of how many entries it holds.
        rows.push(("readdir", 1, 1, readdir));

        // readdir_warm: the same listing on the same fence.
        let base = sample();
        assert_eq!(fs.dir_entries("seed").unwrap().len(), SEED_FILES);
        let readdir_warm = sample().since(&base);
        // Budget 0: a fence-matched listing is served without a wire call.
        rows.push(("readdir_warm", 1, 0, readdir_warm));

        // readdir_stat: the `ls -l` shape — stat every entry the listing just
        // returned. The listing installed each child's metadata fenced, so the
        // whole directory costs the one /tree above and nothing more.
        let base = sample();
        for index in 0..SEED_FILES {
            assert!(
                fs.stat_path_attributes(&format!("seed/file-{index}"))
                    .unwrap()
                    .is_some()
            );
        }
        let readdir_stat = sample().since(&base);
        rows.push(("readdir_stat", SEED_FILES, 0, readdir_stat));

        // lookup_miss: a negative lookup, the dominant cost of a build tool's
        // include/module search.
        settle();
        let base = sample();
        for index in 0..STAT_OPS {
            assert!(
                fs.stat_path_attributes(&format!("absent/missing-{index}"))
                    .unwrap()
                    .is_none()
            );
        }
        let lookup_miss = sample().since(&base);
        // Budget 1 per op: one batched attribute read, negative-cached fenced.
        // Same two-trip snapshot tolerance as `stat_cold`.
        rows.push(("lookup_miss", STAT_OPS, STAT_OPS + 2, lookup_miss));

        println!("\ngateway round trips per filesystem operation");
        println!(
            "{:<15} {:>5} {:>12} {:>8}  {}",
            "class", "ops", "round_trips", "per_op", "routes"
        );
        for (class, ops, _, counts) in &rows {
            println!(
                "{:<15} {:>5} {:>12} {:>8.2}  {}",
                class,
                ops,
                counts.charged(),
                counts.charged() as f64 / *ops as f64,
                counts.breakdown()
            );
        }
        println!(
            "(revision-watch long polls, never on an operation's path: {})",
            sample().watch
        );

        // Mechanism: `enqueue_namespace_creation` durably journals and projects
        // the creation and returns; publishing it is the journal worker's job,
        // behind the syscall. The enqueue path itself never stats — that is the
        // exact contrast with the create syscall measured below, and unlike the
        // publication count it cannot be perturbed by the worker's batch window
        // firing mid-loop.
        assert_eq!(
            journaled_enqueue.stat, 0,
            "a journaled creation must be a local append, not a confirmation stat"
        );
        // Mechanism: the journal worker coalesces every mutation queued inside
        // its batch window into one /namespace-many. 50 queued creations must
        // not become 50 publications.
        assert!(
            create_journaled.namespace_many <= 10,
            "50 queued creations published as {} batches",
            create_journaled.namespace_many
        );

        // Mechanism: an ordinary create is a durable journal append answered
        // from its own projection, so the syscall reaches the gateway not at
        // all. What remains on this row is the journal worker's batched
        // publications landing while the loop runs — off the syscall path, and
        // far fewer than one per create.
        //
        // This previously cost 3.00 round trips per create: the reservation was
        // confirmed through `cached_published_file`, whose cache the projection
        // never populates (siblings must not observe an unpublished mutation),
        // so it fell through to polling `/stat` until the worker published.
        // That made every create wait out its own publication and sleep through
        // the batch window, so each mutation published alone.
        assert_eq!(
            create.charged(),
            create.stat + create.namespace_many,
            "the create path must reach only /stat and /namespace-many: {}",
            create.breakdown()
        );
        // Publications must batch: one per create means the deferral is not
        // working, even when the syscall itself no longer blocks on it.
        assert!(
            create.namespace_many <= CREATE_OPS / 2,
            "creates published {} batches for {CREATE_OPS} creates",
            create.namespace_many
        );
        // A create must never confirm itself through the gateway: the moment it
        // does, it waits out its own publication and publishes alone.
        assert_eq!(
            create.stat, 0,
            "creates must not issue confirmation stats: {}",
            create.breakdown()
        );
        // Mechanism: a live revision watch keeps this mount's coherence fence
        // continuously confirmed, so `stat_path_attributes` serves a
        // fence-matched cached attribute with no wire call at all.
        assert_eq!(
            stat_warm.charged(),
            0,
            "warm stats under a live watch must not touch the gateway"
        );
        // Mechanism: `dir_entries` takes one authoritative listing and installs
        // both the listing and every child's metadata, so a directory costs one
        // round trip regardless of how many entries it holds.
        assert!(
            readdir.tree <= 1,
            "readdir issued {} /tree requests",
            readdir.tree
        );
        assert_eq!(
            readdir_warm.charged(),
            0,
            "a fence-matched listing must be served without a wire call"
        );
        assert_eq!(
            readdir_stat.charged(),
            0,
            "stat of every listed child must be served off the listing's metadata"
        );
        // Mechanism: `MetadataBatcher` routes a negative lookup through the same
        // batched attribute read as a positive one, and the negative result is
        // cached fenced, so a miss is one round trip and never a per-path stat.
        assert_eq!(
            lookup_miss.stat, 0,
            "negative lookups must not fall back to point stats"
        );
        // Per-class gate. Every read class is at or under two round trips per
        // op — the bar a 100-500ms budget needs to survive a realistic RTT —
        // and `create` is pinned at the gap documented above.
        for (class, ops, budget, counts) in &rows {
            assert!(
                counts.charged() <= *budget,
                "{class} cost {} round trips for {ops} ops (budget {budget}): {}",
                counts.charged(),
                counts.breakdown()
            );
        }

        drop(fs);
        server.abort();
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
            // A positive hit is leased to the kernel (watch-gated). ENOENT is
            // deliberately NOT negatively cached (no ino=0 + TTL reply): a
            // negative dentry carries no inode this mount tracks, so the remote
            // full-sweep path (which walks this mount's inodes) could not revoke
            // it, leaving a cross-process create invisible for up to the lease.
            // Leaving it uncached keeps negative lookups strict; the dominant
            // cost the lease targets is warm stats of paths that exist.
            Ok(attr) => reply.entry(&self.reply_ttl(), &attr, Generation(0)),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn getattr(&self, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        if ino == ROOT_INO {
            reply.attr(&self.reply_ttl(), &self.root_attr());
            return;
        }
        let result: FuseResult<FileAttr> = (|| {
            let handle = match fh {
                Some(fh) if self.lock_handles()?.files.contains_key(&fh.0) => Some(fh.0),
                _ => self.open_handle_for_inode(ino)?,
            };
            if let Some(fh) = handle {
                let gate = self.publication_gate_for_handle(fh)?;
                let _guard = gate.lock().map_err(|_| Errno::EIO)?;
                let state = self
                    .lock_handles()?
                    .files
                    .get(&fh)
                    .cloned()
                    .ok_or(Errno::ENOENT)?;
                let route = if Self::handle_metadata_needs_remote_route(&state) {
                    Some(self.resolve_handle_route_locked(fh)?)
                } else {
                    None
                };
                let authoritative = match route.as_ref() {
                    Some(StableFileRoute::Linked(route)) => Some(&route.metadata),
                    Some(StableFileRoute::Unlinked) | None => None,
                };
                let metadata = self.metadata_for_handle_state(&state, authoritative);
                return Ok(self.attr_for_metadata(ino, &metadata, true));
            }
            let route = self.resolve_inode_file_route_attributes(ino)?;
            Ok(self.attr_for_metadata(ino, &route.metadata, false))
        })();
        match result {
            Ok(attr) => reply.attr(&self.reply_ttl(), &attr),
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

                    let state = {
                        let handles = self.lock_handles()?;
                        handles.files.get(&fh.0).cloned()
                    }
                    .ok_or(Errno::ENOENT)?;
                    let route = if Self::handle_metadata_needs_remote_route(&state) {
                        Some(self.resolve_handle_route_locked(fh.0)?)
                    } else {
                        None
                    };
                    let authoritative = match route.as_ref() {
                        Some(StableFileRoute::Linked(route)) => Some(&route.metadata),
                        Some(StableFileRoute::Unlinked) | None => None,
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
            Ok(attr) => reply.attr(&self.reply_ttl(), &attr),
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
                entries.push((child_ino, entry.name, metadata));
            }
            // Readdirplus hands the kernel a dentry AND attrs per child; lease
            // them exactly as lookup/getattr do (watch-gated), chosen once for
            // this reply's assembly.
            let ttl = self.reply_ttl();
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
                    &ttl,
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
            Ok(fh) => reply.opened(FileHandle(fh), remote_file_open_flags()),
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
            Ok(attr) => reply.entry(&self.reply_ttl(), &attr, Generation(0)),
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
            Ok(attr) => reply.entry(&self.reply_ttl(), &attr, Generation(0)),
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
                self.detach_inode_path(&path);
            } else if let Some(file_id) = file_id.as_deref() {
                let mut inodes = self.lock_inodes()?;
                inodes.detach_unlinked_identity(file_id);
            } else {
                self.detach_inode_path(&path);
            }
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
            let rename_result = self.commit_namespace_with_metadata(
                VfsNamespaceMutation::Rename {
                    from: from.clone(),
                    to: to.clone(),
                },
                Some(source_metadata.clone()),
            );
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
            if self
                .tokio
                .block_on(self.client.stat_attributes(&destination))
                .map_err(|_| Errno::EIO)?
                .is_some()
            {
                return Err(Errno::EEXIST);
            }
            let mut projected = source_metadata.clone();
            projected.link_count = projected.link_count.saturating_add(1).max(2);
            if let Err(error) = self.commit_namespace_with_metadata(
                VfsNamespaceMutation::CreateHardLink {
                    source_path: source.clone(),
                    destination_path: destination.clone(),
                },
                Some(projected),
            ) {
                let expected_file_id = source_metadata.file_id.as_deref().ok_or(error)?;
                let completed = self
                    .tokio
                    .block_on(self.client.stat_attributes(&destination))
                    .ok()
                    .flatten()
                    .is_some_and(|metadata| metadata.file_id.as_deref() == Some(expected_file_id));
                if !completed {
                    return Err(error);
                }
            }
            let destination_metadata = self
                .tokio
                .block_on(self.client.stat(&destination))
                .map_err(|_| Errno::EIO)?
                .ok_or(Errno::EIO)?;
            let source_metadata = self
                .tokio
                .block_on(self.client.stat(&source))
                .map_err(|_| Errno::EIO)?
                .unwrap_or_else(|| destination_metadata.clone());
            let response = chevalier_sandbox::vfs::VfsHardLinkMetadataResponse {
                source: source_metadata,
                destination: destination_metadata,
            };
            if let Some(file_id) = response.source.file_id.as_deref() {
                // A freshly created file first entered the inode table before
                // the gateway assigned its stable identity. Bind that existing
                // inode before allocating the destination attr so both names
                // are returned to the kernel as one hard-linked inode.
                self.lock_inodes()?
                    .ensure_with_identity(&source, Some(file_id));
            }
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
            Ok(attr) => reply.entry(&self.reply_ttl(), &attr, Generation(0)),
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
            let (metadata, reserved) = match self.reserve_file_if_absent(&path, mode, exclusive) {
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
                &self.reply_ttl(),
                &attr,
                Generation(0),
                FileHandle(fh),
                remote_file_open_flags(),
            ),
            Err(err) => reply.error(err),
        }
    }
}
