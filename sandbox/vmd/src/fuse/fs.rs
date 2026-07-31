use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use chevalier_sandbox::vfs::{
    VFS_SURFACE_KIND_VM_SHARED, VFS_SURFACE_KIND_VM_WORKSPACE, scoped_vfs_path,
};
use fuser::{
    BsdFileFlags, Errno, FileAttr, FileHandle, FileType, FopenFlags, Generation, INodeNo,
    InitFlags, KernelConfig, LockNamespace, MountOption, OpenFlags, RenameFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen,
    ReplyStatfs, ReplyWrite, TimeOrNow,
};
use tokio::runtime::Handle;
use uuid::Uuid;

use super::client::{AdvisoryLockRenewalIdentity, RemoteVfsClient, request_status};
use super::local_view::hydrate::KernelPathInvalidator;
use super::local_view::mount::MountLocalView;
use super::local_view::tree::{MountFile, errno_of};
use super::local_view::types::{LocalDirEntry, LocalKind, LocalMetadata, LocalTimestamp};

/// Attribute/entry lease handed to the kernel for every positive reply.
///
/// A single owner materializes its scope into a local backing tree and is the
/// only writer of it, so a leased dentry or attribute cannot be invalidated by
/// anybody the mount does not already serialize against. The lease is therefore
/// a long fixed value rather than a function of remote watch liveness: a short,
/// watch-gated TTL existed only to bound the staleness of a projection
/// reconstructed from remote state, and there is no such projection any more.
///
/// A read-only observer keeps the same long lease. It has no local authority, so
/// its follower revokes exactly the entries it re-materializes through
/// `KernelPathInvalidator`; an explicit revocation is strictly stronger than
/// waiting out a TTL.
const REPLY_ATTR_TTL: Duration = Duration::from_secs(86_400);
/// Size past which a read-only observer's untargeted kernel sweep is worth a
/// diagnostic.
///
/// `notify_inval_entry` acquires the parent inode's write lock in the guest
/// kernel, so a sweep proportional to the whole working set blocks the guest's
/// own lookups for as long as it runs. An observer cannot decline the sweep —
/// converging on the remote scope is its whole contract — so this is a log
/// threshold, not a limit.
const KERNEL_SWEEP_MAX_TARGETS: usize = 128;
const ROOT_INO_RAW: u64 = 1;
const ROOT_INO: INodeNo = INodeNo(ROOT_INO_RAW);
/// Keep zero-lookup inode routes long enough for a kernel invalidation/FORGET
/// to cross concurrent operations that were already dispatched with that inode
/// as their parent. Explicit namespace deletion/rename detaches routes
/// immediately; this is only bounded retention for otherwise-live paths.
const MAX_RETAINED_INODE_RECORDS: usize = 262_144;
const MAX_OPEN_HANDLES: usize = 8_192;
const ADVISORY_LOCK_RETRY_DELAY: Duration = Duration::from_millis(50);
const ADVISORY_LOCK_BLOCK_TIMEOUT: Duration = Duration::from_secs(30);
/// Emit one WARN if a blocking advisory-lock acquisition parks past this before
/// the hard timeout, surfacing an intermittent upstream stall without per-poll
/// spam.
const SLOW_ADVISORY_LOCK_WARN_AFTER: Duration = Duration::from_secs(10);
const ADVISORY_LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// Bound retained for the userspace lock callbacks. Ordinary mounts no longer
/// negotiate those callbacks, but an unexpected direct invocation must still
/// fail boundedly rather than parking behind publication indefinitely.
const ADVISORY_LOCK_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const POSIX_MODE_MASK: u32 = 0o7777;

type FuseResult<T> = std::result::Result<T, Errno>;

/// Open flags returned to the kernel for a file handle.
///
/// `FOPEN_DIRECT_IO` used to be forced here because separate VMs had separate
/// page caches over the same remote object and there was no cross-kernel
/// invalidation channel, so a cached page could outlive the bytes it described.
/// A single owner reading and writing its own backing tree has no such sibling:
/// the guest's page cache is coherent with the only writer there is, and forcing
/// direct I/O only turned every read and write into an unbuffered round trip
/// through the FUSE transport.
///
/// `FUSE_WRITEBACK_CACHE` stays off (see `requested_init_capabilities_for`): it
/// changes write semantics and needs measurement, which is not this step's work.
fn remote_file_open_flags() -> FopenFlags {
    FopenFlags::empty()
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

// The identity-retargeting and alias helpers describe a gateway `file_id` that
// the backing entry's `dev:ino` has replaced as this mount's hard-link identity,
// so they have no caller on the callback path any more. They are kept because
// they are the inode table's complete contract -- `lookup`, `route`,
// `retarget_identity*`, `detach_unlinked_identity`, `knows_path` and
// `aliases_for_path` are the operations the structure is specified in terms of,
// and the alias/lookup-count bookkeeping they encode is what makes the rest of
// it correct.
#[allow(dead_code)]
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
                    // The name was reused, but the displaced object may still
                    // have a hard-link alias that a later lookup reveals.
                    self.detach_exact_preserving_identity(path);
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
                if identity.is_some() {
                    // This pathname was just proven by a backing-tree lstat.
                    // Prefer it over an older alias retained for the same
                    // dev:ino. Besides being valid for real hard links, this is
                    // essential when the host filesystem recycles an inode
                    // number while the kernel still holds the old FUSE inode:
                    // callbacks on the new dentry must not keep resolving
                    // through the vanished historical pathname.
                    record.path = path.to_string();
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
                // The caller obtained this identity from a positive local
                // lookup. It is therefore a stronger route than an older alias
                // or retained path hint.
                record.path = path.to_string();
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

    fn knows_path(&self, path: &str) -> bool {
        self.path_to_ino.contains_key(path)
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
            self.detach_exact_preserving_identity(path);
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
        if let Some(record) = self.ino_to_path.get_mut(&ino) {
            record.lookup_count = record.lookup_count.saturating_sub(nlookup);
            record.last_access = Instant::now();
        }
        self.prune_forgotten_records_to(MAX_RETAINED_INODE_RECORDS);
    }

    fn prune_forgotten_records_to(&mut self, limit: usize) {
        if self.ino_to_path.len() <= limit.max(1) {
            return;
        }
        let mut forgotten = self
            .ino_to_path
            .iter()
            .filter(|(ino, record)| **ino != ROOT_INO && record.lookup_count == 0)
            .map(|(ino, record)| (*ino, record.last_access))
            .collect::<Vec<_>>();
        forgotten.sort_by_key(|(_, last_access)| *last_access);
        let remove = self.ino_to_path.len().saturating_sub(limit.max(1));
        for (ino, _) in forgotten.into_iter().take(remove) {
            self.remove_inode_record(ino);
        }
    }

    fn remove_inode_record(&mut self, ino: INodeNo) {
        let Some(record) = self.ino_to_path.remove(&ino) else {
            return;
        };
        for path in record.paths {
            if self.path_to_ino.get(path.as_str()) == Some(&ino) {
                self.path_to_ino.remove(path.as_str());
            }
        }
        if let Some(identity) = record.identity
            && self.identity_to_ino.get(identity.as_str()) == Some(&ino)
        {
            self.identity_to_ino.remove(identity.as_str());
        }
    }

    fn detach_exact(&mut self, path: &str) {
        self.detach_exact_with_identity_retirement(path, true);
    }

    fn detach_exact_preserving_identity(&mut self, path: &str) {
        self.detach_exact_with_identity_retirement(path, false);
    }

    fn detach_exact_with_identity_retirement(&mut self, path: &str, retire: bool) {
        let Some(ino) = self.path_to_ino.remove(path) else {
            return;
        };
        let mut remove_inode = false;
        let mut retire_identity = None;
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
            if retire && record.paths.is_empty() {
                // Keep the FUSE inode record until the kernel's delayed FORGET,
                // but retire its reverse dev:ino binding immediately. The host
                // filesystem may recycle that inode number for the next package
                // directory while this record is still retained.
                retire_identity = record.identity.clone();
            }
            remove_inode =
                record.paths.is_empty() && !(record.identity.is_some() && record.lookup_count > 0);
        }
        if let Some(identity) = retire_identity
            && self.identity_to_ino.get(identity.as_str()) == Some(&ino)
        {
            self.identity_to_ino.remove(identity.as_str());
        }
        if remove_inode {
            self.ino_to_path.remove(&ino);
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
        let mut retired_identities = Vec::new();
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
                if record.paths.is_empty() {
                    if let Some(identity) = record.identity.clone() {
                        retired_identities.push((ino, identity));
                    }
                    if !(record.identity.is_some() && record.lookup_count > 0) {
                        emptied.push(ino);
                    }
                }
            }
        }
        for (ino, identity) in retired_identities {
            if self.identity_to_ino.get(identity.as_str()) == Some(&ino) {
                self.identity_to_ino.remove(identity.as_str());
            }
        }
        for ino in emptied {
            self.ino_to_path.remove(&ino);
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

    /// Every attribute/dentry this mount handed the kernel, for a read-only
    /// observer's full sweep when the affected path set is not known.
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

/// Captured before the fs is moved into its FUSE session, this defers binding
/// the kernel notifier — which only exists after `spawn_mount2` — to the inode
/// table it has to resolve paths through.
///
/// Only a read-only observer binds one. A writable mount owns its scope, serves
/// every read from its own backing tree and has nobody to be contradicted by, so
/// it revokes nothing and never builds an invalidator at all.
pub(super) struct KernelInvalidationBinding {
    inodes: Arc<Mutex<InodeTable>>,
}

impl KernelInvalidationBinding {
    /// Build the read-only observer's kernel-revocation sink.
    pub(super) fn path_invalidator(&self, notifier: fuser::Notifier) -> Arc<MountPathInvalidator> {
        MountPathInvalidator::spawn(notifier, Arc::downgrade(&self.inodes))
    }
}

/// Kernel revocation for a read-only observer mount, driven by
/// `local_view::hydrate::RemoteFollower`.
///
/// The follower re-materializes what an external writer changed into the backing
/// tree and then has to drop whatever this mount's guest kernel still caches for
/// those paths. `notify_inval_entry` takes the guest kernel's parent-inode lock,
/// which a parked FUSE operation may already hold, so the trait contract is
/// enqueue-and-return: every request lands in this queue and one dedicated
/// thread — which holds no FUSE lock and never runs a callback — issues the
/// notifier calls.
pub(super) struct MountPathInvalidator {
    inner: Arc<PathInvalidatorInner>,
}

struct PathInvalidatorInner {
    notifier: fuser::Notifier,
    /// `Weak`, so a queued revocation cannot keep the filesystem alive past
    /// unmount; an upgrade failure simply means there is nothing left to revoke.
    inodes: Weak<Mutex<InodeTable>>,
    queue: Mutex<PathInvalidatorQueue>,
    signal: Condvar,
    /// Edge-detects notifier failures so a broken channel warns once, not once
    /// per path.
    warned: AtomicBool,
}

/// Coalesced work. Repeated requests for the same path collapse into one
/// revocation, and a pending full sweep subsumes everything finer.
#[derive(Default)]
struct PathInvalidatorQueue {
    paths: BTreeSet<String>,
    subtrees: BTreeSet<String>,
    sweep: bool,
    stopped: bool,
}

impl PathInvalidatorQueue {
    fn is_idle(&self) -> bool {
        !self.sweep && self.paths.is_empty() && self.subtrees.is_empty()
    }
}

impl MountPathInvalidator {
    fn spawn(notifier: fuser::Notifier, inodes: Weak<Mutex<InodeTable>>) -> Arc<Self> {
        let inner = Arc::new(PathInvalidatorInner {
            notifier,
            inodes,
            queue: Mutex::new(PathInvalidatorQueue::default()),
            signal: Condvar::new(),
            warned: AtomicBool::new(false),
        });
        let worker = Arc::clone(&inner);
        if let Err(error) = std::thread::Builder::new()
            .name("vfs-observer-inval".to_string())
            .spawn(move || worker.run())
        {
            // Without the worker the observer would serve stale kernel entries
            // silently. Say so once, loudly; the mount stays usable and falls
            // back to lease expiry. Marking the queue stopped keeps every later
            // request from accumulating behind a thread that will never run it.
            tracing::error!(
                %error,
                "failed to start the read-only vfs observer's kernel revocation thread"
            );
            if let Ok(mut queue) = inner.queue.lock() {
                queue.stopped = true;
            }
        }
        Arc::new(Self { inner })
    }
}

impl Drop for MountPathInvalidator {
    fn drop(&mut self) {
        // The worker owns its own `Arc` on the inner state, so it outlives this
        // handle; ask it to finish the queue it holds and exit.
        if let Ok(mut queue) = self.inner.queue.lock() {
            queue.stopped = true;
        }
        self.inner.signal.notify_all();
    }
}

impl KernelPathInvalidator for MountPathInvalidator {
    fn invalidate_paths(&self, paths: &[String]) {
        self.inner.enqueue(|queue| {
            for path in paths {
                queue.paths.insert(path.trim_matches('/').to_string());
            }
        });
    }

    fn invalidate_subtrees(&self, subtrees: &[String]) {
        self.inner.enqueue(|queue| {
            for subtree in subtrees {
                let subtree = subtree.trim_matches('/');
                // The scope root as a subtree is the whole mount, which only a
                // full sweep expresses (`subtree_invalidation_targets` declines
                // an empty prefix precisely because it means everything).
                if subtree.is_empty() {
                    queue.sweep = true;
                } else {
                    queue.subtrees.insert(subtree.to_string());
                }
            }
        });
    }

    fn invalidate_all(&self) {
        self.inner.enqueue(|queue| queue.sweep = true);
    }
}

impl PathInvalidatorInner {
    fn enqueue(&self, fill: impl FnOnce(&mut PathInvalidatorQueue)) {
        let Ok(mut queue) = self.queue.lock() else {
            return;
        };
        if queue.stopped {
            return;
        }
        fill(&mut queue);
        drop(queue);
        self.signal.notify_one();
    }

    fn run(&self) {
        loop {
            let (paths, subtrees, sweep) = {
                let mut queue = match self.queue.lock() {
                    Ok(queue) => queue,
                    Err(_) => return,
                };
                while queue.is_idle() && !queue.stopped {
                    queue = match self.signal.wait(queue) {
                        Ok(queue) => queue,
                        Err(_) => return,
                    };
                }
                if queue.is_idle() {
                    // Idle and stopped: nothing left to revoke.
                    return;
                }
                (
                    std::mem::take(&mut queue.paths),
                    std::mem::take(&mut queue.subtrees),
                    std::mem::replace(&mut queue.sweep, false),
                )
            };
            // The inode-table lock is dropped before any `writev` into
            // `/dev/fuse`, so a revocation can never block a lookup behind it.
            let targets = self.resolve(&paths, &subtrees, sweep);
            self.apply(&targets);
        }
    }

    fn resolve(
        &self,
        paths: &BTreeSet<String>,
        subtrees: &BTreeSet<String>,
        sweep: bool,
    ) -> Vec<KernelInvalTarget> {
        let Some(inodes) = self.inodes.upgrade() else {
            return Vec::new();
        };
        let table = inodes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if sweep {
            // An observer has no ack to withhold and no projection of its own to
            // fall back on, so — unlike the owning mount's `invalidate_all` — it
            // cannot decline a large sweep. Converging on the remote scope is the
            // observer's entire contract.
            let targets = table.all_invalidation_targets();
            if targets.len() > KERNEL_SWEEP_MAX_TARGETS {
                tracing::debug!(
                    targets = targets.len(),
                    "read-only vfs observer sweeping a large kernel working set"
                );
            }
            return targets;
        }
        let mut targets = Vec::new();
        for path in paths {
            if let Some(target) = table.invalidation_target(path) {
                targets.push(target);
            }
        }
        for subtree in subtrees {
            targets.extend(table.subtree_invalidation_targets(subtree));
        }
        targets
    }

    fn apply(&self, targets: &[KernelInvalTarget]) {
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
            self.warned.store(false, Ordering::Release);
        }
    }

    fn warn_once(&self, error: &io::Error) {
        if !self.warned.swap(true, Ordering::AcqRel) {
            tracing::warn!(%error, "read-only vfs observer kernel revocation failed");
        }
    }
}

/// Open file descriptors on the backing tree.
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

/// One open file description.
///
/// There is no whole-file buffer, no CAS baseline, no publication bookkeeping
/// and no per-inode gate any more: the backing descriptor *is* the shared state
/// two descriptors on one inode used to have to mirror between them, and the
/// ordering a publication needed is the WAL's sequence.
#[derive(Clone)]
struct FileState {
    /// `Arc` so a callback can take the descriptor out from under the handle
    /// table lock and do its I/O without holding it -- the handle table is the
    /// outermost lock in the mount and must never span a syscall.
    file: Arc<MountFile>,
    /// The mount-relative pathname this descriptor was opened at, retargeted by
    /// `rename`. Kept alongside `MountFile`'s own name so the handle table can
    /// be scanned by pathname without touching a descriptor.
    path: String,
    /// The guest's open flags, as the kernel sent them.
    flags: i32,
}

impl FileState {
    /// Whether this description may mutate its file. The guest kernel normally
    /// enforces it, but every operation that changes bytes checks it too, so a
    /// read-only descriptor can never dirty a path and pull its content into a
    /// published generation.
    fn is_writable(&self) -> bool {
        self.flags & libc::O_ACCMODE != libc::O_RDONLY
    }
}

/// One open directory description.
///
/// `readdir`/`readdirplus` are served from a snapshot taken here rather than
/// re-listing the directory on every kernel round, which is what makes the
/// offset a real cursor: a continuation at offset N addresses the same child it
/// would have on the previous round. The snapshot is refreshed whenever the
/// kernel restarts the stream at offset 0, so `rewinddir(3)` still sees current
/// contents.
struct DirState {
    path: String,
    entries: Option<Arc<Vec<LocalDirEntry>>>,
}

struct DirHandleTable {
    next: u64,
    dirs: HashMap<u64, DirState>,
}

/// Directory handles are minted from the top of the handle space so they can
/// never collide with a file handle. `getattr` is dispatched with whichever
/// descriptor the caller `fstat`ed, including a directory's, and answering it
/// from a file table entry that happened to share the number would return a
/// completely unrelated inode's attributes.
const DIR_HANDLE_BASE: u64 = 1 << 63;

impl Default for DirHandleTable {
    fn default() -> Self {
        Self {
            next: DIR_HANDLE_BASE,
            dirs: HashMap::new(),
        }
    }
}

pub struct RemoteFuseFs {
    client: RemoteVfsClient,
    /// The mount-local materialized view: the backing tree, the WAL and the
    /// ownership lock for this mount's state directory. Opened, recovered and
    /// hydrated by `handle::mount_remote_vfs_fuse` before the session is
    /// spawned, so it is already the authoritative local state by the time the
    /// kernel can issue a callback against it.
    local: Arc<MountLocalView>,
    // `Arc` so a read-only observer's post-mount path invalidator (see
    // `MountPathInvalidator`) can hold a `Weak` into this exact table and
    // resolve re-hydrated paths to the inodes this mount handed its kernel,
    // without keeping the fs alive.
    inodes: Arc<Mutex<InodeTable>>,
    handles: Mutex<HandleTable>,
    dirs: Mutex<DirHandleTable>,
    read_only: bool,
    scope_path: String,
    mount_id: String,
    active_lock_owners: std::sync::Arc<Mutex<ActiveAdvisoryLocks>>,
    tokio: Handle,
    uid: u32,
    gid: u32,
}

impl RemoteFuseFs {
    /// Scratch-state-directory constructor for the in-crate unit tests, which
    /// drive callbacks directly without mounting. Production mounts go through
    /// [`RemoteFuseFs::new_for_mount`], which is handed the mount state
    /// directory the mount already owns.
    #[cfg(test)]
    pub fn new(client: RemoteVfsClient, read_only: bool, scope_path: &str, tokio: Handle) -> Self {
        let local = Self::scratch_local_view(scope_path, read_only, &tokio);
        let (mount_id, active_lock_owners) = Self::start_lock_heartbeat(&client, &tokio);
        Self {
            client,
            local,
            inodes: Arc::new(Mutex::new(InodeTable::new())),
            handles: Mutex::new(HandleTable::default()),
            dirs: Mutex::new(DirHandleTable::default()),
            read_only,
            scope_path: scope_path.trim_matches('/').to_string(),
            mount_id,
            active_lock_owners,
            tokio,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        }
    }

    /// Test-only scratch state directory, so a unit test that never mounts still
    /// gets a real backing tree and WAL. The directory is deliberately leaked:
    /// it must outlive the filesystem under test, and the OS reclaims it.
    #[cfg(test)]
    fn scratch_local_view(
        scope_path: &str,
        read_only: bool,
        tokio: &Handle,
    ) -> Arc<MountLocalView> {
        let root = tempfile::Builder::new()
            .prefix("chevalier-fuse-test-")
            .tempdir()
            .expect("create scratch mount state directory");
        let options = super::local_view::mount::MountLocalViewOptions {
            layout: super::local_view::MountStateLayout::new(root.path()),
            scope_path: scope_path.trim_matches('/').to_string(),
            endpoint: String::new(),
            mount_tag: "test".to_string(),
            read_only,
            tokio: tokio.clone(),
        };
        let opened = MountLocalView::open(options).expect("open scratch mount-local view");
        std::mem::forget(root);
        opened.view
    }

    /// The production constructor. `local` is the mount-local view
    /// `handle::mount_remote_vfs_fuse` has already opened, recovered and
    /// hydrated, so the filesystem never exists without its authoritative local
    /// state. Crate-visible because `MountLocalView` is: a mount can only be
    /// built by the mount path.
    ///
    /// There is nothing else to open. The two ordered gateway journals this used
    /// to construct — and the shared metadata cache, the kernel-invalidation
    /// registry and the revision watch they were coherent against — are retired:
    /// every mutation is a WAL event applied to the backing tree, and the
    /// asynchronous publisher is the only thing that talks to the gateway.
    pub(crate) fn new_for_mount(
        client: RemoteVfsClient,
        read_only: bool,
        scope_path: &str,
        local: Arc<MountLocalView>,
        tokio: Handle,
    ) -> Result<Self> {
        let (mount_id, active_lock_owners) = Self::start_lock_heartbeat(&client, &tokio);
        Ok(Self {
            client,
            local,
            inodes: Arc::new(Mutex::new(InodeTable::new())),
            handles: Mutex::new(HandleTable::default()),
            dirs: Mutex::new(DirHandleTable::default()),
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
        // Package extraction issues large concurrent LOOKUP/CREATE bursts, and
        // every callback now runs to completion against local storage. Thirty-two
        // request threads keep those bursts from queueing behind one another
        // without oversubscribing the device.
        config.n_threads = Some(32);
        config.clone_fd = true;
        config
    }

    fn requested_init_capabilities(&self) -> InitFlags {
        Self::requested_init_capabilities_for(self.read_only)
    }

    fn path_for_ino(&self, ino: INodeNo) -> FuseResult<String> {
        self.lock_inodes()?.path(ino).ok_or(Errno::ENOENT)
    }

    /// The attribute/entry TTL to hand the kernel for a reply. A fixed long
    /// lease: this mount owns its scope, so nothing can contradict what it just
    /// answered (see [`REPLY_ATTR_TTL`]).
    fn reply_ttl(&self) -> Duration {
        REPLY_ATTR_TTL
    }

    /// Capture the handles the post-mount kernel-notifier install needs, before
    /// this fs is moved into its FUSE session. Called from mount setup while the
    /// fs is still reachable (see `handle::mount_remote_vfs_fuse`).
    pub(super) fn kernel_invalidation_binding(&self) -> KernelInvalidationBinding {
        KernelInvalidationBinding {
            inodes: Arc::clone(&self.inodes),
        }
    }

    fn ensure_ino(&self, path: &str) -> INodeNo {
        self.lock_inodes()
            .map(|mut inodes| inodes.ensure(path))
            .unwrap_or(ROOT_INO)
    }

    fn detach_inode_path(&self, path: &str) {
        if let Ok(mut inodes) = self.lock_inodes() {
            inodes.detach_exact(path);
        }
    }

    /// Re-key the inode table and every open descriptor under a renamed prefix.
    ///
    /// The descriptors are retargeted as well as the `FileState` pathnames: the
    /// content dirty/seal lifecycle is keyed by pathname, so a descriptor still
    /// naming the source would dirty a name the rename has already removed and
    /// leave the bytes written after the rename unsealed at close.
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
                    state.file.retarget(&state.path);
                }
            }
        }
    }

    /// Project one backing-tree `lstat` into the attributes the kernel expects.
    ///
    /// `local_identity` (`dev:ino` of the backing entry) is what binds the inode,
    /// so hard links, rename and unlink-and-recreate are all answered by the
    /// backing filesystem instead of by a gateway `file_id` that only exists
    /// after a publication.
    fn attr_for_metadata(&self, ino: INodeNo, metadata: &LocalMetadata) -> FileAttr {
        let kind = file_type_for_kind(metadata.kind);
        let mut mode = metadata.mode;
        if self.read_only && kind != FileType::Symlink {
            mode &= !0o222;
        }
        FileAttr {
            ino,
            size: metadata.size_bytes,
            blocks: metadata.blocks,
            atime: system_time_of(metadata.atime),
            mtime: system_time_of(metadata.mtime),
            ctime: system_time_of(metadata.ctime),
            crtime: system_time_of(metadata.ctime),
            kind,
            perm: (mode & POSIX_MODE_MASK) as u16,
            nlink: metadata.link_count.min(u32::MAX as u64) as u32,
            uid: metadata.uid,
            gid: metadata.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn attr_for_path(&self, path: &str, metadata: &LocalMetadata, lookup: bool) -> FileAttr {
        let identity = Some(metadata.local_identity.as_str());
        let ino = self
            .lock_inodes()
            .map(|mut inodes| {
                if lookup {
                    inodes.lookup_with_identity(path, identity)
                } else {
                    inodes.ensure_with_identity(path, identity)
                }
            })
            .unwrap_or(ROOT_INO);
        self.attr_for_metadata(ino, metadata)
    }

    /// Synthetic root attributes, used only when the backing tree root cannot be
    /// stat'd at all (which means the state directory is gone underneath us).
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

    /// Register one open backing descriptor and return the FUSE handle for it.
    ///
    /// Two descriptors on one inode need no coordination any more: they share
    /// the backing file itself, so there is nothing to donate between them and
    /// nothing to mirror after a write.
    fn next_handle(&self, file: MountFile, path: &str, flags: i32) -> FuseResult<u64> {
        let mut handles = self.lock_handles()?;
        if handles.files.len() >= MAX_OPEN_HANDLES {
            return Err(Errno::EMFILE);
        }
        let fh = handles.next;
        handles.next += 1;
        handles.files.insert(
            fh,
            FileState {
                file: Arc::new(file),
                path: path.to_string(),
                flags,
            },
        );
        Ok(fh)
    }

    /// Clone one handle's state out from under the handle table lock, so the
    /// caller does its I/O with no mount lock held at all.
    fn handle_state(&self, fh: u64) -> FuseResult<FileState> {
        self.lock_handles()?
            .files
            .get(&fh)
            .cloned()
            .ok_or(Errno::EBADF)
    }

    /// Recover the open file description for an inode whose name no longer
    /// resolves.
    ///
    /// That is exactly the unlinked-but-open case: the backing entry survives
    /// its last link natively, but a pathname `lstat` cannot see it, and Linux
    /// omits `fh` from `getattr` even when servicing `fstat(2)`. The inode's
    /// bound identity picks the right descriptor when one name has held more
    /// than one inode over the mount's life.
    fn open_handle_for_inode(&self, ino: INodeNo) -> FuseResult<Option<FileState>> {
        let Some((path, identity)) = self.lock_inodes()?.route(ino) else {
            return Ok(None);
        };
        let candidates: Vec<FileState> = {
            let handles = self.lock_handles()?;
            handles
                .files
                .values()
                .filter(|state| state.path == path)
                .cloned()
                .collect()
        };
        let Some(identity) = identity else {
            return Ok(candidates.into_iter().next());
        };
        let mut fallback = None;
        for state in candidates {
            let matches = state
                .file
                .metadata()
                .map(|metadata| metadata.local_identity == identity)
                .unwrap_or(false);
            if matches {
                return Ok(Some(state));
            }
            fallback = fallback.or(Some(state));
        }
        Ok(fallback)
    }

    fn next_dir_handle(&self, path: &str) -> FuseResult<u64> {
        let mut dirs = self.lock_dirs()?;
        if dirs.dirs.len() >= MAX_OPEN_HANDLES {
            return Err(Errno::EMFILE);
        }
        let fh = dirs.next;
        dirs.next += 1;
        dirs.dirs.insert(
            fh,
            DirState {
                path: path.to_string(),
                entries: None,
            },
        );
        Ok(fh)
    }

    /// The listing one `readdir`/`readdirplus` round serves from.
    ///
    /// At offset 0 the directory is re-read and the snapshot replaced, so an
    /// `opendir`+`rewinddir` sequence sees current contents. At any other offset
    /// the existing snapshot is reused, which is what makes the offset a real
    /// cursor instead of a fresh O(n) listing per kernel round.
    ///
    /// A `readdir` the kernel issued without a matching `opendir` (or after the
    /// handle was released) falls back to a one-shot listing rather than failing.
    fn dir_listing(
        &self,
        fh: u64,
        path: &str,
        offset: u64,
    ) -> FuseResult<Option<Arc<Vec<LocalDirEntry>>>> {
        if offset != 0 {
            let dirs = self.lock_dirs()?;
            if let Some(state) = dirs.dirs.get(&fh)
                && state.path == path
                && let Some(entries) = state.entries.as_ref()
            {
                return Ok(Some(Arc::clone(entries)));
            }
        }
        let Some(entries) = self.local.tree().read_dir(path).map_err(errno_for)? else {
            return Ok(None);
        };
        let entries = Arc::new(entries);
        let mut dirs = self.lock_dirs()?;
        if let Some(state) = dirs.dirs.get_mut(&fh)
            && state.path == path
        {
            state.entries = Some(Arc::clone(&entries));
        }
        Ok(Some(entries))
    }

    /// The path an advisory lock is taken against, made visible to the gateway
    /// first.
    ///
    /// Advisory locks are cross-VM POSIX arbitration, not mount-local content,
    /// so they are the one deliberate synchronous gateway user on a callback
    /// (they are rare and off the throughput path). They no longer depend on a
    /// published `file_id`: a mount-local file exists the instant the guest
    /// creates it, long before the publisher has replicated it, so asking the
    /// gateway to identify it first would answer `ENOENT` for every freshly
    /// created lock file — which is every lock file a package manager makes.
    ///
    /// Instead the path's own publication is drained under a bound, and the
    /// gateway's answer to the lock request is taken as authoritative. A drain
    /// that times out or is blocked still issues the request: the path may
    /// already be remote from an earlier generation, and parking a guest syscall
    /// on a replication outage is exactly what this architecture forbids.
    fn advisory_lock_target(&self, fh: u64) -> FuseResult<String> {
        let state = self.handle_state(fh)?;
        let path = state.path;
        let metadata = state.file.metadata().map_err(errno_for)?;
        if !metadata.is_file() {
            return Err(Errno::EINVAL);
        }
        let drained = self
            .tokio
            .block_on(self.local.drain_path(&path, ADVISORY_LOCK_DRAIN_TIMEOUT));
        match drained {
            Ok(outcome) if outcome.is_drained() => {}
            Ok(outcome) => tracing::warn!(
                path,
                ?outcome,
                "vfs advisory lock is being taken on a path whose publication has not caught up"
            ),
            Err(error) => tracing::warn!(
                path,
                error = %format!("{error:#}"),
                "vfs advisory lock could not drain its path's publication"
            ),
        }
        Ok(path)
    }

    /// This mount's scope-qualified form of a mount-relative path.
    ///
    /// Nothing in `fs.rs` needs it any more: `RemoteVfsClient` scopes every
    /// request it issues, so the publisher, the hydrator and the advisory-lock
    /// callbacks all pass mount-relative paths. Retired with the rest of the
    /// gateway-on-a-callback surface.
    #[allow(dead_code)]
    fn scoped_path(&self, path: &str) -> String {
        scoped_vfs_path(self.scope_path.as_str(), path)
    }

    #[allow(dead_code)]
    fn surface_kind_for_path(&self, path: &str) -> &'static str {
        Self::surface_kind_for_scoped_path(self.scoped_path(path).as_str())
    }

    #[allow(dead_code)]
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

    fn lock_dirs(&self) -> FuseResult<MutexGuard<'_, DirHandleTable>> {
        self.dirs.lock().map_err(|_| Errno::EIO)
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
        path: &str,
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
                    path,
                    &self.mount_id,
                    &owner,
                    Self::advisory_lock_namespace(namespace),
                    start,
                    end,
                    kind,
                    pid,
                ))
                .map_err(|error| Self::advisory_lock_error(&error))?;
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
                    path,
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

fn normalize_mode(mode: u32) -> u32 {
    mode & POSIX_MODE_MASK
}

fn creation_mode(mode: u32, _umask: u32) -> u32 {
    // We intentionally do not request FUSE_DONT_MASK, so the kernel has
    // already applied umask before dispatching create/mkdir to userspace.
    normalize_mode(mode)
}

fn file_type_for_kind(kind: LocalKind) -> FileType {
    match kind {
        LocalKind::Directory => FileType::Directory,
        LocalKind::Symlink => FileType::Symlink,
        LocalKind::File => FileType::RegularFile,
    }
}

fn system_time_of(timestamp: LocalTimestamp) -> SystemTime {
    let nanos = u32::min(timestamp.nanos, 999_999_999);
    if timestamp.secs >= 0 {
        SystemTime::UNIX_EPOCH + Duration::new(timestamp.secs as u64, nanos)
    } else {
        SystemTime::UNIX_EPOCH
            .checked_sub(Duration::new(timestamp.secs.unsigned_abs(), 0))
            .map(|base| base + Duration::new(0, nanos))
            .unwrap_or(SystemTime::UNIX_EPOCH)
    }
}

/// The kernel's requested timestamp, in the WAL's wall-clock form.
fn local_timestamp_of(value: TimeOrNow) -> LocalTimestamp {
    let time = match value {
        TimeOrNow::SpecificTime(time) => time,
        TimeOrNow::Now => SystemTime::now(),
    };
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(since) => LocalTimestamp {
            secs: since.as_secs() as i64,
            nanos: since.subsec_nanos(),
        },
        Err(before) => {
            let before = before.duration();
            // A pre-epoch timestamp is `-secs` plus a positive nanosecond
            // remainder, exactly as `utimensat` expects it.
            match before.subsec_nanos() {
                0 => LocalTimestamp {
                    secs: -(before.as_secs() as i64),
                    nanos: 0,
                },
                nanos => LocalTimestamp {
                    secs: -(before.as_secs() as i64) - 1,
                    nanos: 1_000_000_000 - nanos,
                },
            }
        }
    }
}

/// Map a backing-tree failure onto the errno the local filesystem produced, so
/// the guest sees `ENOTEMPTY`, `EEXIST`, `ENOSPC` or `EROFS` rather than a
/// blanket `EIO`. Every syscall failure in `local_view` keeps its
/// `std::io::Error` in the context chain for exactly this reason.
fn errno_for(error: anyhow::Error) -> Errno {
    match errno_of(&error) {
        Some(code) => Errno::from_i32(code),
        None => {
            tracing::warn!(error = %format!("{error:#}"), "vfs mount-local operation failed");
            Errno::EIO
        }
    }
}

impl RemoteFuseFs {
    fn requested_init_capabilities_for(read_only: bool) -> InitFlags {
        let directory_prefetch = InitFlags::FUSE_DO_READDIRPLUS | InitFlags::FUSE_READDIRPLUS_AUTO;
        // One writable mount belongs to one VM. Leaving FUSE_POSIX_LOCKS and
        // FUSE_FLOCK_LOCKS unset makes the guest kernel arbitrate advisory locks
        // locally across every process on that mount. Advertising them forwards
        // getlk/setlk to userspace and turns a gateway or publisher stall into a
        // Cargo-visible lock failure, violating the local-first mount contract.
        // FUSE_WRITEBACK_CACHE is deliberately still not requested. It changes
        // write semantics (the kernel becomes authoritative for i_size and
        // batches writeback behind the daemon's back) and needs measurement
        // before it can be trusted; direct I/O is no longer forced, so the
        // ordinary page cache already covers the read path it would help.
        let _ = read_only;
        InitFlags::FUSE_AUTO_INVAL_DATA | directory_prefetch
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use std::time::Duration;

    use fuser::{InitFlags, LockNamespace};
    use tokio::runtime::Builder;

    use super::super::client::RemoteVfsClient;
    use super::super::local_view::types::{LocalKind, LocalMetadata, LocalTimestamp};
    use super::{
        ActiveAdvisoryLockFile, ActiveAdvisoryLocks, InodeTable, LockWaitCancellation, ROOT_INO,
        RemoteFuseFs, active_advisory_lock_identities, combine_flush_and_lock_cleanup,
        creation_mode, take_active_advisory_lock_file_id, take_active_posix_handle_locks,
    };

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
                | InitFlags::FUSE_DO_READDIRPLUS
                | InitFlags::FUSE_READDIRPLUS_AUTO
        );
        assert_eq!(
            RemoteFuseFs::requested_init_capabilities_for(true),
            InitFlags::FUSE_AUTO_INVAL_DATA
                | InitFlags::FUSE_DO_READDIRPLUS
                | InitFlags::FUSE_READDIRPLUS_AUTO
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
    fn inode_table_retains_forgotten_routes_for_crossed_inflight_operations() {
        let mut table = InodeTable::new();
        let first = table.lookup("package-0");
        for index in 1..=70_000 {
            table.ensure(format!("package-{index}").as_str());
        }

        assert_eq!(table.path(first).as_deref(), Some("package-0"));
        assert_eq!(table.path(ROOT_INO).as_deref(), Some(""));

        table.forget(first, 1);
        assert_eq!(
            table.path(first).as_deref(),
            Some("package-0"),
            "FORGET may cross a CREATE already dispatched with this parent inode"
        );
        assert_eq!(table.path(ROOT_INO).as_deref(), Some(""));
    }

    #[test]
    fn inode_table_bounds_forgotten_route_retention() {
        let mut table = InodeTable::new();
        for path in ["oldest", "middle", "newest"] {
            let ino = table.lookup(path);
            table.forget(ino, 1);
        }

        table.prune_forgotten_records_to(2);

        assert_eq!(table.ino_to_path.len(), 2);
        assert!(table.path(ROOT_INO).is_some());
        assert_eq!(
            table
                .ino_to_path
                .values()
                .filter(|record| record.lookup_count == 0)
                .count(),
            1
        );
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
    fn inode_table_replacing_rename_preserves_displaced_hard_link_alias() {
        let mut table = InodeTable::new();
        let source = table.lookup_with_identity("pkg_tmp", Some("source-inode"));
        let displaced = table.lookup_with_identity("pkg", Some("destination-inode"));
        let displaced_alias =
            table.lookup_with_identity("cache/pkg-copy", Some("destination-inode"));

        table.rename_path("pkg_tmp", "pkg");

        assert_eq!(table.path(source).as_deref(), Some("pkg"));
        assert_eq!(
            table.path(displaced).as_deref(),
            Some("cache/pkg-copy"),
            "replacing one name must not lose a surviving alias of the displaced inode"
        );
        assert_eq!(displaced, displaced_alias);
        assert_eq!(
            table.lookup_with_identity("pkg", Some("source-inode")),
            source,
            "the destination route now belongs to the renamed source inode"
        );
        assert_eq!(
            table.aliases_for_path("cache/pkg-copy"),
            vec!["cache/pkg-copy".to_string()]
        );
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
        assert_eq!(
            table.path(alias).as_deref(),
            Some("third"),
            "a newly proven alias becomes the callback route"
        );
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
    fn exact_detach_retires_identity_before_delayed_forget() {
        let mut table = InodeTable::new();
        let original = table.lookup_with_identity("package_tmp", Some("unix:1:42"));

        table.detach_exact("package_tmp");
        let replacement = table.lookup_with_identity("next-package", Some("unix:1:42"));

        assert_ne!(
            original, replacement,
            "a recycled backing inode must not reuse a retained FUSE inode"
        );
        assert_eq!(
            table.route(original),
            Some(("package_tmp".to_string(), Some("unix:1:42".to_string()))),
            "the old kernel route remains only until FORGET"
        );
        assert_eq!(
            table.route(replacement),
            Some(("next-package".to_string(), Some("unix:1:42".to_string())))
        );
    }

    #[test]
    fn subtree_detach_retires_descendant_identities_before_reuse() {
        let mut table = InodeTable::new();
        let original = table.lookup_with_identity("package_tmp/lib/node/utils", Some("unix:1:99"));

        table.detach_subtree("package_tmp");
        let replacement =
            table.lookup_with_identity("other-package/lib/node/utils", Some("unix:1:99"));

        assert_ne!(original, replacement);
        assert_eq!(
            table.path(replacement).as_deref(),
            Some("other-package/lib/node/utils")
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
    fn attrs_preserve_exact_mode_and_read_only_clears_only_write_bits() {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let client =
            RemoteVfsClient::new("http://127.0.0.1:1", "test-token", "test-scope").unwrap();
        let metadata = LocalMetadata {
            kind: LocalKind::File,
            size_bytes: 0,
            blocks: 0,
            mode: 0o100_3751,
            uid: 0,
            gid: 0,
            link_count: 1,
            local_identity: "unix:1:42".to_string(),
            backing_ino: 42,
            link_target: None,
            atime: LocalTimestamp::default(),
            mtime: LocalTimestamp::default(),
            ctime: LocalTimestamp::default(),
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
}

/// FUSE operation bodies. Dispatched concurrently by `SpawnedFuseFs`
/// (fuse/dispatch.rs), which owns the actual `fuser::Filesystem` impl —
/// every method here may run on a worker thread and must stay `&self`-safe.
///
/// Every one of them is served by the mount-local view: reads go straight to the
/// backing tree and take no journal, cache or ordering lock at all, and every
/// mutation is `validate -> WAL prepare -> one atomic backing syscall ->
/// commit`. No ordinary callback calls the gateway. The two exceptions are
/// deliberate and documented where they occur: `getlk`/`setlk`, which are
/// cross-VM POSIX arbitration rather than mount-local state.
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
            let parent_path = self.path_for_ino(parent).map_err(|error| {
                tracing::warn!(
                    parent_ino = parent.0,
                    name = ?name,
                    ?error,
                    "vfs lookup parent inode has no daemon route"
                );
                error
            })?;
            let child_path = Self::child_path(parent_path.as_str(), name)?;
            let Some(metadata) = self.local.tree().lstat(&child_path).map_err(errno_for)? else {
                return Err(Errno::ENOENT);
            };
            Ok(self.attr_for_path(&child_path, &metadata, true))
        })();
        match result {
            // A positive hit is leased for `REPLY_ATTR_TTL`. ENOENT is still not
            // negatively cached: a negative dentry carries no inode this mount
            // tracks, so a read-only observer's follower could not revoke it, and
            // for a writable mount a local `lstat` miss is already free.
            Ok(attr) => reply.entry(&self.reply_ttl(), &attr, Generation(0)),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn getattr(&self, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        let result: FuseResult<FileAttr> = (|| {
            // An explicit descriptor is authoritative: `fstat(2)` describes the
            // open file description, not whatever the name holds now.
            if let Some(fh) = fh
                && let Ok(state) = self.handle_state(fh.0)
            {
                let metadata = state.file.metadata().map_err(errno_for)?;
                return Ok(self.attr_for_metadata(ino, &metadata));
            }
            if ino == ROOT_INO {
                return Ok(self.root_attributes());
            }
            let path = self.path_for_ino(ino)?;
            if let Some(metadata) = self.local.tree().lstat(&path).map_err(errno_for)? {
                return Ok(self.attr_for_metadata(ino, &metadata));
            }
            // The name is gone. Recover the description from the inode rather
            // than answering ENOENT for a file the guest still holds open.
            let state = self.open_handle_for_inode(ino)?.ok_or(Errno::ENOENT)?;
            let metadata = state.file.metadata().map_err(errno_for)?;
            Ok(self.attr_for_metadata(ino, &metadata))
        })();
        match result {
            Ok(attr) => reply.attr(&self.reply_ttl(), &attr),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn readlink(&self, ino: INodeNo, reply: ReplyData) {
        let result: FuseResult<Vec<u8>> = (|| {
            let path = self.path_for_ino(ino)?;
            let target = self
                .local
                .tree()
                .read_link(&path)
                .map_err(errno_for)?
                .ok_or(Errno::ENOENT)?;
            Ok(target.into_bytes())
        })();
        match result {
            Ok(bytes) => reply.data(&bytes),
            Err(err) => reply.error(err),
        }
    }

    /// `setattr`. Every field the kernel sends is now applied to the backing
    /// tree and journalled, including the ones this filesystem used to drop on
    /// the floor: `uid`/`gid` become `SetOwner` and `atime`/`mtime` become
    /// `SetTimes`. Both are classified local-only by the publisher (the gateway
    /// contract has no field for either), so the divergence is recorded in the
    /// log instead of being invisible.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn setattr(
        &self,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
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
                // The mount root is the mountpoint, not an entry in the scope:
                // no mutation can name it, so a setattr against it reports the
                // root's attributes and changes nothing.
                if size.is_some() {
                    return Err(Errno::EISDIR);
                }
                return Ok(self.root_attributes());
            }
            let handle = fh.and_then(|fh| self.handle_state(fh.0).ok());
            let path = match handle.as_ref() {
                Some(state) => state.path.clone(),
                None => self.path_for_ino(ino)?,
            };
            // A descriptor whose name no longer resolves is an unlinked-but-open
            // file. Its metadata is observable only through that descriptor and
            // has no remote name to be published under, so it is applied to the
            // descriptor directly and deliberately not journalled.
            let detached = match handle.as_ref() {
                Some(_) => self.local.tree().lstat(&path).map_err(errno_for)?.is_none(),
                None => false,
            };

            // Size first: POSIX applies these independently, and ordering a
            // truncate behind a chmod in the log would describe a generation the
            // guest never observed.
            if let Some(size) = size {
                match handle.as_ref() {
                    Some(state) if detached => state.file.truncate(size).map_err(errno_for)?,
                    Some(state) => {
                        self.local.truncate(&state.file, size).map_err(errno_for)?;
                    }
                    None => {
                        self.local.truncate_path(&path, size).map_err(errno_for)?;
                    }
                }
            }
            if let Some(mode) = mode {
                let mode = normalize_mode(mode);
                match handle.as_ref() {
                    Some(state) if detached => state.file.set_mode(mode).map_err(errno_for)?,
                    _ => {
                        self.local.set_mode(&path, mode).map_err(errno_for)?;
                    }
                }
            }
            if uid.is_some() || gid.is_some() {
                match handle.as_ref() {
                    Some(state) if detached => state.file.set_owner(uid, gid).map_err(errno_for)?,
                    _ => {
                        self.local.set_owner(&path, uid, gid).map_err(errno_for)?;
                    }
                }
            }
            if atime.is_some() || mtime.is_some() {
                let atime = atime.map(local_timestamp_of);
                let mtime = mtime.map(local_timestamp_of);
                match handle.as_ref() {
                    Some(state) if detached => {
                        state.file.set_times(atime, mtime).map_err(errno_for)?
                    }
                    _ => {
                        self.local
                            .set_times(&path, atime, mtime)
                            .map_err(errno_for)?;
                    }
                }
            }

            if let Some(state) = handle.as_ref() {
                let metadata = state.file.metadata().map_err(errno_for)?;
                return Ok(self.attr_for_metadata(ino, &metadata));
            }
            let metadata = self
                .local
                .tree()
                .lstat(&path)
                .map_err(errno_for)?
                .ok_or(Errno::ENOENT)?;
            Ok(self.attr_for_metadata(ino, &metadata))
        })();
        match result {
            Ok(attr) => reply.attr(&self.reply_ttl(), &attr),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn opendir(&self, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let result: FuseResult<u64> = (|| {
            let path = self.path_for_ino(ino)?;
            self.next_dir_handle(&path)
        })();
        match result {
            Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::empty()),
            Err(err) => reply.error(err),
        }
    }

    pub(super) fn releasedir(&self, _ino: INodeNo, fh: FileHandle, reply: ReplyEmpty) {
        if let Ok(mut dirs) = self.lock_dirs() {
            dirs.dirs.remove(&fh.0);
        }
        reply.ok();
    }

    pub(super) fn readdir(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let result: FuseResult<()> = (|| {
            let path = self.path_for_ino(ino)?;
            let entries = self
                .dir_listing(fh.0, &path, offset)?
                .ok_or(Errno::ENOENT)?;
            let parent_ino = self.ensure_ino(Self::parent_path(&path).as_str());
            // Offsets index `[".", "..", <sorted children>]`, and the children
            // come from the cursor's snapshot, so a continuation round addresses
            // the same child it did on the previous one.
            let mut index = offset;
            loop {
                let full = match index {
                    0 => (ino, FileType::Directory, ".".to_string()),
                    1 => (parent_ino, FileType::Directory, "..".to_string()),
                    _ => {
                        let Some(entry) = entries.get((index - 2) as usize) else {
                            break;
                        };
                        let child_path = Self::join_child(&path, &entry.name);
                        let child_ino = self
                            .lock_inodes()
                            .map(|mut inodes| {
                                inodes.ensure_with_identity(
                                    &child_path,
                                    Some(entry.metadata.local_identity.as_str()),
                                )
                            })
                            .unwrap_or(ROOT_INO);
                        (
                            child_ino,
                            file_type_for_kind(entry.metadata.kind),
                            entry.name.clone(),
                        )
                    }
                };
                if reply.add(full.0, index + 1, full.1, full.2) {
                    break;
                }
                index += 1;
            }
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    /// One directory listing primes every child's inode and attributes, so a
    /// tree scan costs one `read_dir` per directory instead of a lookup plus a
    /// getattr per file (the kernel skips both for entries returned here).
    pub(super) fn readdirplus(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: fuser::ReplyDirectoryPlus,
    ) {
        let result: FuseResult<()> = (|| {
            let path = self.path_for_ino(ino)?;
            let entries = self
                .dir_listing(fh.0, &path, offset)?
                .ok_or(Errno::ENOENT)?;
            let parent_path = Self::parent_path(&path);
            let parent_ino = self.ensure_ino(parent_path.as_str());
            let ttl = self.reply_ttl();
            let mut index = offset;
            loop {
                // Dot entries never count as kernel lookups (no FORGET arrives
                // for them); every named child does.
                let (entry_ino, name, attr) = match index {
                    0 => {
                        let attr = self.directory_attributes(ino, &path);
                        (ino, ".".to_string(), attr)
                    }
                    1 => {
                        let attr = self.directory_attributes(parent_ino, &parent_path);
                        (parent_ino, "..".to_string(), attr)
                    }
                    _ => {
                        let Some(entry) = entries.get((index - 2) as usize) else {
                            break;
                        };
                        let child_path = Self::join_child(&path, &entry.name);
                        let attr = self.attr_for_path(&child_path, &entry.metadata, true);
                        (attr.ino, entry.name.clone(), attr)
                    }
                };
                if reply.add(entry_ino, index + 1, name, &ttl, &attr, Generation(0)) {
                    break;
                }
                index += 1;
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
            let path = self.path_for_ino(ino)?;
            let (file, metadata) = self.local.open_file(&path, flags.0).map_err(errno_for)?;
            if metadata.is_dir() {
                return Err(Errno::EISDIR);
            }
            self.next_handle(file, &path, flags.0)
        })();
        match result {
            Ok(fh) => reply.opened(FileHandle(fh), remote_file_open_flags()),
            Err(err) => {
                tracing::warn!(ino = ino.0, errno = ?err, "vfs open failed");
                reply.error(err);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
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
            let state = self.handle_state(fh.0)?;
            let mut buffer = vec![0u8; size as usize];
            let read = self
                .local
                .read(&state.file, &mut buffer, offset)
                .map_err(errno_for)?;
            buffer.truncate(read);
            Ok(buffer)
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

    #[allow(clippy::too_many_arguments)]
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
        let result: FuseResult<u32> = (|| {
            let state = self.handle_state(fh.0)?;
            if !state.is_writable() {
                return Err(Errno::EBADF);
            }
            // A through-write to the backing file. There is no read-modify-write
            // download: the bytes the guest is not overwriting are already there.
            // Durable-storage exhaustion surfaces as the local `ENOSPC` it is.
            let written = self
                .local
                .write(&state.file, data, offset)
                .map_err(errno_for)?;
            Ok(written as u32)
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
        // FLUSH is what close(2) sends. It seals this path's content generation
        // into one committed WAL record and returns; it is not a durability
        // point and it never waits for the publisher.
        let flush_result = self.seal_handle(fh.0);
        let cleanup_result =
            self.release_advisory_lock_owner(ino, lock_owner, LockNamespace::Posix, Some(fh.0));
        match combine_flush_and_lock_cleanup("flush", flush_result, cleanup_result) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    /// `fsync(2)`: the explicit durability point, and a purely local one. The
    /// generation is sealed, the backing file is synced, and the WAL is group
    /// synced through that record. The bytes are then durable on this host,
    /// which is what POSIX durability means for a mount that owns its scope; a
    /// network round trip would make `fsync` depend on a service POSIX never
    /// mentions.
    pub(super) fn fsync(&self, _ino: INodeNo, fh: FileHandle, datasync: bool, reply: ReplyEmpty) {
        let result: FuseResult<()> = (|| {
            let state = self.handle_state(fh.0)?;
            self.local
                .fsync_handle(&state.file, datasync)
                .map_err(errno_for)
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    /// The directory-level durability point: seal every dirty path under this
    /// directory, `fsync` the backing directory, group sync the WAL. Like
    /// `fsync`, it never reaches the network.
    pub(super) fn fsyncdir(
        &self,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let result: FuseResult<()> = (|| {
            let path = self.path_for_ino(ino)?;
            self.local.fsync_dir(&path).map_err(errno_for)
        })();
        match result {
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
        // RELEASE is the second half of close(2). The generation is sealed and
        // the descriptor retires; a file created and closed without a write
        // needs no promotion, because the create already appended its own
        // `CreateFile` record and the backing file already exists.
        let flush_result = self.seal_handle(fh.0);
        if flush_result.is_ok() {
            let _ = self
                .lock_handles()
                .map(|mut handles| handles.files.remove(&fh.0));
        }
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

    /// `fallocate(2)`. Nearly free now that a backing file exists, and it is what
    /// lets an archive extractor reserve a file's final length up front instead
    /// of growing it a write at a time.
    pub(super) fn fallocate(
        &self,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let result: FuseResult<()> = (|| {
            if mode != 0 {
                // Only plain allocation has a meaning the gateway's whole-file
                // content model can carry. Hole punching and collapse-range would
                // publish a generation whose bytes the local file never held.
                return Err(Errno::EOPNOTSUPP);
            }
            let state = self.handle_state(fh.0)?;
            if !state.is_writable() {
                return Err(Errno::EBADF);
            }
            self.local
                .allocate(&state.file, offset, length)
                .map_err(errno_for)
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    /// `copy_file_range(2)`. On a reflinking backing filesystem this copies no
    /// bytes at all, which is exactly the shape a `node_modules` install has.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn copy_file_range(
        &self,
        _ino_in: INodeNo,
        fh_in: FileHandle,
        offset_in: u64,
        _ino_out: INodeNo,
        fh_out: FileHandle,
        offset_out: u64,
        length: u64,
        reply: ReplyWrite,
    ) {
        let result: FuseResult<u32> = (|| {
            let source = self.handle_state(fh_in.0)?;
            let destination = self.handle_state(fh_out.0)?;
            if !destination.is_writable() {
                return Err(Errno::EBADF);
            }
            let copied = self
                .local
                .copy_range(
                    &destination.file,
                    &source.file,
                    offset_in,
                    offset_out,
                    length,
                )
                .map_err(errno_for)?;
            Ok(copied.min(u32::MAX as u64) as u32)
        })();
        match result {
            Ok(copied) => reply.written(copied),
            Err(err) => reply.error(err),
        }
    }

    /// `statfs(2)`, answered by the filesystem holding the backing tree.
    ///
    /// This is what makes the guest's free space honest: local exhaustion is a
    /// real filesystem answer the guest can act on, while a gateway outage never
    /// surfaces as one.
    pub(super) fn statfs(&self, _ino: INodeNo, reply: ReplyStatfs) {
        match self.local.statfs() {
            Ok(stat) => reply.statfs(
                stat.blocks,
                stat.blocks_free,
                stat.blocks_available,
                stat.files,
                stat.files_free,
                stat.block_size,
                stat.max_name_length,
                stat.fragment_size,
            ),
            Err(error) => reply.error(errno_for(error)),
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
            let path = self.advisory_lock_target(fh.0)?;
            let owner = self.advisory_lock_owner_key(lock_owner);
            let (start, end) = Self::advisory_lock_range(namespace, start, end);
            self.tokio
                .block_on(self.client.advisory_lock(
                    "get",
                    &path,
                    &self.mount_id,
                    &owner,
                    Self::advisory_lock_namespace(namespace),
                    start,
                    end,
                    kind,
                    pid,
                ))
                .map_err(|error| Self::advisory_lock_error(&error))
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
            self.advisory_lock_target(fh.0).and_then(|path| {
                self.set_advisory_lock(
                    &path,
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
            let metadata = self
                .local
                .create_directory(&path, creation_mode(mode, umask))
                .map_err(errno_for)?;
            // No cache seeding: an empty backing directory is its own proof of
            // emptiness, and the negative lookups an installer issues into it are
            // answered by a local `lstat` miss.
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
            let target = target.to_str().ok_or(Errno::EINVAL)?;
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), link_name)?;
            let metadata = self
                .local
                .create_symlink(&path, target)
                .map_err(errno_for)?;
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
            // `remove_file` seals any pending generation before the name goes,
            // so bytes the guest wrote are described while the name that names
            // them still exists. Open descriptors keep working afterwards: the
            // backing inode survives its last link natively.
            self.local.remove_file(&path).map_err(errno_for)?;
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
            // `unlinkat(AT_REMOVEDIR)` answers `ENOTEMPTY` locally, so emptiness
            // is no longer arbitrated remotely.
            self.local.remove_directory(&path).map_err(errno_for)?;
            // A successful rmdir proves the directory was empty, but this mount
            // may still hold inode records for names the guest has since removed
            // from it. Sweep the subtree rather than the exact path.
            if let Ok(mut inodes) = self.lock_inodes() {
                inodes.detach_subtree(&path);
            }
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    /// `rename(2)`, applied with one `renameat2`.
    ///
    /// A replacing rename needs no ambiguity verification and no alias
    /// resolution any more: the displaced entry survives as an open-unlinked
    /// backing inode exactly as it would on any local filesystem.
    /// `RENAME_NOREPLACE` is honoured by the syscall; `RENAME_EXCHANGE` and
    /// `RENAME_WHITEOUT` are rejected with `EINVAL`, because neither has an
    /// atomic form in the gateway contract and decomposing them would publish a
    /// namespace state this mount never had.
    pub(super) fn rename(
        &self,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let mut rename_paths = None;
        let result: FuseResult<()> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let newparent_path = self.path_for_ino(newparent)?;
            let from = Self::child_path(parent_path.as_str(), name)?;
            let to = Self::child_path(newparent_path.as_str(), newname)?;
            rename_paths = Some((from.clone(), to.clone()));
            if from == to {
                return Ok(());
            }
            self.local
                .rename(&from, &to, flags.bits())
                .map_err(errno_for)?;
            self.rename_inode_path(&from, &to);
            Ok(())
        })();
        if let Err(error) = result.as_ref() {
            let (from, to) = rename_paths
                .as_ref()
                .map(|(from, to)| (from.as_str(), to.as_str()))
                .unwrap_or(("<unresolved>", "<unresolved>"));
            tracing::warn!(from, to, ?error, ?flags, "vfs rename failed");
        }
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
            let source = self.path_for_ino(ino)?;
            let parent = self.path_for_ino(newparent)?;
            let destination = Self::child_path(parent.as_str(), newname)?;
            // `linkat` supplies `EEXIST`, `EPERM` for a directory source and the
            // resulting link count, all locally: none of the five gateway round
            // trips this used to cost describe anything the backing tree cannot.
            let metadata = self
                .local
                .create_hard_link(&source, &destination)
                .map_err(errno_for)?;
            // Bind the source name to the shared identity too, so both names are
            // returned to the kernel as one hard-linked inode.
            self.lock_inodes()?
                .ensure_with_identity(&source, Some(metadata.local_identity.as_str()));
            Ok(self.attr_for_path(&destination, &metadata, true))
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
            let parent_path = self.path_for_ino(parent).map_err(|error| {
                tracing::warn!(
                    parent_ino = parent.0,
                    name = ?name,
                    ?error,
                    "vfs create parent inode has no daemon route"
                );
                error
            })?;
            let path = Self::child_path(parent_path.as_str(), name)?;
            // FUSE presents a successful CREATE with `O_EXCL` set even for an
            // ordinary `echo x > new-file`, which used to force a synchronous
            // cross-mount arbitration per created file. The backing tree makes it
            // a real `openat(O_CREAT | O_EXCL)`: correct, local and free, with
            // `EEXIST` answered by the filesystem.
            let (file, metadata) = self
                .local
                .create_file(&path, creation_mode(mode, umask), flags)
                .map_err(errno_for)?;
            let attr = self.attr_for_path(&path, &metadata, true);
            let fh = self.next_handle(file, &path, flags)?;
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

    // -- shared callback helpers ---------------------------------------------

    /// Seal one handle's content generation. A handle the table no longer knows
    /// about is a duplicate FLUSH whose RELEASE already retired it, which is
    /// success, not `EBADF`.
    fn seal_handle(&self, fh: u64) -> FuseResult<()> {
        let Some(state) = self.lock_handles()?.files.get(&fh).cloned() else {
            return Ok(());
        };
        self.local.flush_handle(&state.file).map_err(errno_for)
    }

    fn join_child(parent: &str, name: &str) -> String {
        if parent.is_empty() {
            name.to_string()
        } else {
            format!("{parent}/{name}")
        }
    }

    /// Attributes for the mount root, from the backing tree root itself.
    fn root_attributes(&self) -> FileAttr {
        match self.local.tree().lstat("") {
            Ok(Some(metadata)) => self.attr_for_metadata(ROOT_INO, &metadata),
            Ok(None) | Err(_) => self.root_attr(),
        }
    }

    /// Attributes for a `.`/`..` entry in a `readdirplus` reply.
    fn directory_attributes(&self, ino: INodeNo, path: &str) -> FileAttr {
        match self.local.tree().lstat(path) {
            Ok(Some(metadata)) => self.attr_for_metadata(ino, &metadata),
            Ok(None) | Err(_) => {
                let mut attr = self.root_attr();
                attr.ino = ino;
                attr
            }
        }
    }
}
