//! `MountLocalView` -- the facade every FUSE callback talks to.
//!
//! It owns the backing tree, the WAL, the dependency-key lock table, the content
//! dirty/seal lifecycle, recovery, and the drain used by unmount, snapshot and
//! destructive lifecycle transitions.
//!
//! The mutation protocol, in the order the doc requires:
//!
//! ```text
//! validate  ->  take dependency keys  ->  observe pre-image
//!           ->  WAL prepare (intent + immutable payload durable)
//!           ->  apply to the backing tree (one atomic syscall)
//!           ->  WAL commit  (or abort, if the apply failed)
//!           ->  release dependency keys
//! ```
//!
//! Reads take no lock in this module at all: they go straight to the backing
//! tree. Nothing here ever calls a gateway.
//!
//! ## Lock ordering
//!
//! 1. dependency keys (`PathLocks`), always acquired in the sorted order
//!    [`MountMutation::dependency_keys`] returns, so two overlapping mutations
//!    can never deadlock;
//! 2. the WAL append lock, taken and released inside `prepare` / `commit` /
//!    `abort` -- never held across a backing-tree syscall, an `fsync`, a payload
//!    copy or an `await`;
//! 3. the payload store lock, taken inside a payload capture only.
//!
//! `fs.rs`'s handle and inode tables are strictly *above* this module: a
//! callback may hold them around a call in here, never the reverse.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::publisher::MountPublisher;
use super::tree::{BackingTree, MountFile};
use super::types::{
    AppliedMutation, ApplyState, DependencyKey, DrainOutcome, LocalMetadata, LocalStatfs,
    LocalTimestamp, MountMutation, MountOwnerRecord, PayloadSource, PublicationHealth,
    RENAME_EXCHANGE, RENAME_WHITEOUT, RecoveryResolution, RecoverySummary, StoragePressure,
    ancestors_of, merge_dependency_keys,
};
use super::wal::MountWal;
use super::{
    CHECKPOINT_EVENT_INTERVAL, CHECKPOINT_INTERVAL_MS, MountStateLayout, WAL_FORMAT_VERSION,
    sync_directory,
};

/// How the mount was constructed.
#[derive(Clone, Debug)]
pub(crate) struct MountLocalViewOptions {
    pub(crate) layout: MountStateLayout,
    pub(crate) scope_path: String,
    pub(crate) endpoint: String,
    pub(crate) mount_tag: String,
    pub(crate) read_only: bool,
    pub(crate) tokio: Handle,
}

/// What `open` produced, including whether the caller must run the eager
/// hydrate before the mount is allowed to serve a callback.
pub(crate) struct MountOpen {
    pub(crate) view: Arc<MountLocalView>,
    /// True for a fresh state directory. False when a durable WAL was recovered:
    /// a recovered mount already holds state the network replica may be behind
    /// on, and re-hydrating it would mount a lagging replica as though it were
    /// the latest state.
    pub(crate) needs_hydration: bool,
    pub(crate) recovery: RecoverySummary,
}

/// Exclusive ownership of one mount state directory.
///
/// Held for the mount's lifetime as an OS advisory lock over `owner.json`, so
/// exclusivity is enforced once at mount scope instead of being reconstructed
/// per lookup and close. A second process opening the same state directory fails
/// immediately and loudly.
pub(crate) struct MountOwnership {
    record: MountOwnerRecord,
    /// The locked descriptor. Closing it releases the `flock`, so it lives
    /// exactly as long as the mount does.
    _owner_file: File,
}

impl MountOwnership {
    /// Take the lock, minting the owner record for a fresh state directory and
    /// validating scope/endpoint/tag against an existing one.
    pub(crate) fn acquire(options: &MountLocalViewOptions) -> Result<Self> {
        options.layout.ensure()?;
        let path = options.layout.owner_path();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("open mount owner record {}", path.display()))?;
        lock_exclusive_nonblocking(&file, &path)?;

        let mut existing = String::new();
        file.read_to_string(&mut existing)
            .with_context(|| format!("read mount owner record {}", path.display()))?;
        let existing = existing.trim();

        let record = if existing.is_empty() {
            // A fresh state directory mints its epoch once. It persists across
            // restarts so `{epoch}:{sequence}` idempotency keys stay stable.
            let record = MountOwnerRecord {
                format_version: WAL_FORMAT_VERSION,
                epoch: Uuid::now_v7().to_string(),
                scope_path: options.scope_path.clone(),
                endpoint: options.endpoint.clone(),
                mount_tag: options.mount_tag.clone(),
                created_at: Utc::now().to_rfc3339(),
            };
            persist_owner_record(&mut file, &path, &record)?;
            sync_directory(options.layout.root())?;
            record
        } else {
            let record: MountOwnerRecord = serde_json::from_str(existing)
                .with_context(|| format!("decode mount owner record {}", path.display()))?;
            validate_owner_record(&record, options, &path)?;
            record
        };

        Ok(Self {
            record,
            _owner_file: file,
        })
    }

    pub(crate) fn epoch(&self) -> &str {
        &self.record.epoch
    }
}

fn validate_owner_record(
    record: &MountOwnerRecord,
    options: &MountLocalViewOptions,
    path: &Path,
) -> Result<()> {
    if record.format_version != WAL_FORMAT_VERSION {
        bail!(
            "mount owner record {} has format {} (expected {WAL_FORMAT_VERSION})",
            path.display(),
            record.format_version
        );
    }
    if record.epoch.is_empty() {
        bail!(
            "mount owner record {} carries no ownership epoch",
            path.display()
        );
    }
    // A state directory belongs to exactly one scope on one endpoint under one
    // tag. Adopting a foreign one would publish this mount's WAL into somebody
    // else's namespace, so every mismatch fails closed.
    if record.scope_path != options.scope_path {
        bail!(
            "mount state directory {} belongs to scope {:?}, not {:?}",
            path.display(),
            record.scope_path,
            options.scope_path
        );
    }
    if record.endpoint != options.endpoint {
        bail!(
            "mount state directory {} belongs to endpoint {:?}, not {:?}",
            path.display(),
            record.endpoint,
            options.endpoint
        );
    }
    if record.mount_tag != options.mount_tag {
        bail!(
            "mount state directory {} belongs to mount tag {:?}, not {:?}",
            path.display(),
            record.mount_tag,
            options.mount_tag
        );
    }
    Ok(())
}

/// Write the record over the descriptor the lock is held on. Deliberately not a
/// temp-file rename: a rename would swap the inode the `flock` is attached to,
/// and a second process could then lock the replacement and believe it owns the
/// directory. The record is written once, at creation, and fsynced.
fn persist_owner_record(file: &mut File, path: &Path, record: &MountOwnerRecord) -> Result<()> {
    let mut encoded = serde_json::to_vec(record)
        .with_context(|| format!("encode mount owner record {}", path.display()))?;
    encoded.push(b'\n');
    file.set_len(0)
        .with_context(|| format!("truncate mount owner record {}", path.display()))?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind mount owner record {}", path.display()))?;
    file.write_all(&encoded)
        .with_context(|| format!("write mount owner record {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync mount owner record {}", path.display()))
}

fn lock_exclusive_nonblocking(file: &File, path: &Path) -> Result<()> {
    // SAFETY: `file` owns the descriptor for the duration of the call.
    let locked = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if locked == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN
    ) {
        bail!(
            "mount state directory {} is already owned by another process",
            path.display()
        );
    }
    Err(anyhow::Error::new(error).context(format!("lock mount owner record {}", path.display())))
}

/// The mount-local materialized view.
pub(crate) struct MountLocalView {
    layout: MountStateLayout,
    scope_path: String,
    read_only: bool,
    tokio: Handle,
    /// Held for the mount's lifetime; dropping it releases the advisory lock.
    _ownership: MountOwnership,
    tree: BackingTree,
    /// `None` for a read-only observer mount, which has no local authority.
    wal: Option<MountWal>,
    locks: PathLocks,
    publisher: Mutex<Option<Arc<MountPublisher>>>,
    /// Hot-path index over the WAL's durable content-dirty set. The WAL remains
    /// the durable authority; this exists so `flush`/`fsync` on a clean handle
    /// costs one hash lookup instead of a snapshot of every dirty path.
    dirty: Mutex<BTreeSet<String>>,
    /// Last cached [`StoragePressure`], refreshed by the maintenance task. The
    /// write path must never pay for a `statvfs`.
    pressure: AtomicU8,
    checkpointed_sequence: AtomicU64,
    compacted_sequence: AtomicU64,
    stopping: AtomicBool,
    maintenance: Mutex<Option<JoinHandle<()>>>,
}

impl fmt::Debug for MountLocalView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountLocalView")
            .field("state_dir", &self.layout.root())
            .field("scope_path", &self.scope_path)
            .field("read_only", &self.read_only)
            .finish()
    }
}

impl MountLocalView {
    /// Open the state directory, take ownership, open the backing tree, recover
    /// the WAL, and resolve every unresolved prepare before returning.
    ///
    /// Recovery, in order:
    /// 1. take the ownership lock;
    /// 2. open the backing tree and the WAL (checkpoint-anchored replay);
    /// 3. for each unresolved prepare, oldest first, `BackingTree::classify`:
    ///    `NotApplied` -> `resolve_recovered(Abort)`;
    ///    `Applied` -> `resolve_recovered(Commit)`, because the guest already
    ///    observed it and the replica must converge onto it;
    ///    `Ambiguous` -> fail closed, deleting nothing local and nothing remote;
    /// 4. re-seal every path still marked content-dirty, capturing the backing
    ///    file as a fresh committed generation, so bytes the guest read back are
    ///    never lost to a crash mid-generation;
    /// 5. restore the tree generation counter and publish a checkpoint.
    ///
    /// A read-only mount takes no WAL at all: it gets a backing tree, a hydrate,
    /// and `EROFS` on every mutation.
    pub(crate) fn open(options: MountLocalViewOptions) -> Result<MountOpen> {
        options.layout.ensure()?;
        // Sampled before ownership mints `owner.json`, and always before the WAL
        // creates its first log generation. A recovered state directory must
        // never re-hydrate: that would mount a lagging replica over an
        // unpublished local WAL (invariant 7). A read-only observer has no local
        // authority, so it always re-materializes from the gateway.
        let needs_hydration = options.read_only || options.layout.is_empty()?;

        let ownership = MountOwnership::acquire(&options)?;
        let tree = BackingTree::open(&options.layout.tree_dir())?;
        let wal = if options.read_only {
            None
        } else {
            Some(MountWal::open(&options.layout, Some(ownership.epoch()))?)
        };

        let view = Arc::new(Self {
            layout: options.layout,
            scope_path: options.scope_path,
            read_only: options.read_only,
            tokio: options.tokio,
            _ownership: ownership,
            tree,
            wal,
            locks: PathLocks::new(),
            publisher: Mutex::new(None),
            dirty: Mutex::new(BTreeSet::new()),
            pressure: AtomicU8::new(encode_pressure(StoragePressure::None)),
            checkpointed_sequence: AtomicU64::new(0),
            compacted_sequence: AtomicU64::new(0),
            stopping: AtomicBool::new(false),
            maintenance: Mutex::new(None),
        });

        let recovery = view.recover()?;
        Ok(MountOpen {
            view,
            needs_hydration,
            recovery,
        })
    }

    fn recover(&self) -> Result<RecoverySummary> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(RecoverySummary::default());
        };
        let state = wal.recovery_state()?;
        let mut summary = RecoverySummary {
            replayed_events: (state.committed_unacknowledged.len()
                + state.unresolved_prepares.len()) as u64,
            committed_unacknowledged: state.committed_unacknowledged.len() as u64,
            acknowledged_sequence: state.acknowledged_sequence,
            remote_revision: state.remote_revision,
            ..RecoverySummary::default()
        };

        let mut unresolved = state.unresolved_prepares.clone();
        unresolved.sort_by_key(|event| event.sequence);
        for event in &unresolved {
            match self.tree.classify(event)? {
                ApplyState::NotApplied => {
                    wal.resolve_recovered(event.sequence, RecoveryResolution::Abort)?;
                    summary.resolved_aborted += 1;
                }
                ApplyState::Applied => {
                    // The guest already observed this. The replica converges onto
                    // it rather than away from it.
                    wal.resolve_recovered(event.sequence, RecoveryResolution::Commit)?;
                    summary.resolved_committed += 1;
                }
                ApplyState::Ambiguous => {
                    // Fail closed. Nothing local and nothing remote is deleted:
                    // the WAL, the payloads and the backing tree are left exactly
                    // as the crash left them, for an operator to inspect.
                    bail!(
                        "mount recovery cannot classify sequence {} on {:?} against the backing \
                         tree; refusing to mount",
                        event.sequence,
                        event.mutation.primary_path()
                    );
                }
            }
        }

        {
            let mut dirty = self.lock_dirty()?;
            for path in &state.dirty_content_paths {
                dirty.insert(path.clone());
            }
        }
        for path in &state.dirty_content_paths {
            let keys = content_keys(path, true);
            let _guard = self.locks.acquire(&keys)?;
            if self.seal_locked(wal, path)?.is_some() {
                summary.resealed_paths += 1;
            }
        }

        self.tree.set_tree_generation(state.tree_generation);
        wal.checkpoint(self.tree.tree_generation())?;
        self.checkpointed_sequence
            .store(wal.last_appended_sequence(), Ordering::Relaxed);
        self.compacted_sequence
            .store(wal.acknowledged_sequence(), Ordering::Relaxed);
        self.refresh_storage_pressure();
        Ok(summary)
    }

    pub(crate) fn tree(&self) -> &BackingTree {
        &self.tree
    }

    /// `None` for a read-only mount.
    pub(crate) fn wal(&self) -> Option<&MountWal> {
        self.wal.as_ref()
    }

    pub(crate) fn layout(&self) -> &MountStateLayout {
        &self.layout
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub(crate) fn scope_path(&self) -> &str {
        &self.scope_path
    }

    /// Bind the publisher once it has been spawned. Until then the mount is
    /// fully functional locally and simply accumulates durable events.
    pub(crate) fn attach_publisher(&self, publisher: Arc<MountPublisher>) -> Result<()> {
        let mut slot = self
            .publisher
            .lock()
            .map_err(|_| anyhow!("mount publisher slot poisoned"))?;
        if slot.is_some() {
            bail!("mount already has a publisher attached");
        }
        *slot = Some(publisher);
        Ok(())
    }

    // -- namespace mutations -------------------------------------------------

    pub(crate) fn create_directory(&self, path: &str, mode: u32) -> Result<LocalMetadata> {
        let wal = self.writable_wal()?;
        let mutation = MountMutation::CreateDirectory {
            path: path.to_string(),
            mode,
        };
        mutation.validate()?;
        let keys = mutation.dependency_keys();
        let _guard = self.locks.acquire(&keys)?;
        let applied = self.commit_mutation(wal, mutation, PayloadSource::None)?;
        self.metadata_after(&applied, path)
    }

    /// `create(2)`. The kernel-resolved negative dentry becomes a real
    /// `O_EXCL` on the backing tree, so `EEXIST` is local and free.
    pub(crate) fn create_file(
        &self,
        path: &str,
        mode: u32,
        flags: i32,
    ) -> Result<(MountFile, LocalMetadata)> {
        let wal = self.writable_wal()?;
        let mutation = MountMutation::CreateFile {
            path: path.to_string(),
            mode,
        };
        mutation.validate()?;
        let keys = mutation.dependency_keys();
        let _guard = self.locks.acquire(&keys)?;

        let pre_image = {
            let paths = mutation.affected_paths();
            self.tree.observe(&paths)?
        };
        let prepared = wal.prepare(mutation, PayloadSource::None, pre_image)?;
        let (file, applied) = match self.tree.apply_create_file(path, mode, flags) {
            Ok(created) => created,
            Err(error) => {
                wal.abort(prepared, &format!("{error:#}"))?;
                return Err(error);
            }
        };
        wal.commit(prepared, applied.local_identity.clone())?;
        // The created generation is content the gateway has never seen. Marking
        // it dirty is what lets the publisher fold the creation into the first
        // `ReplaceFile` instead of publishing an empty file and then its bytes.
        self.mark_dirty(wal, path)?;
        let metadata = match applied.metadata.clone() {
            Some(metadata) => metadata,
            None => file.metadata()?,
        };
        Ok((file, metadata))
    }

    pub(crate) fn create_symlink(&self, path: &str, target: &str) -> Result<LocalMetadata> {
        let wal = self.writable_wal()?;
        let mutation = MountMutation::CreateSymlink {
            path: path.to_string(),
            target: target.to_string(),
        };
        mutation.validate()?;
        let keys = mutation.dependency_keys();
        let _guard = self.locks.acquire(&keys)?;
        let applied = self.commit_mutation(wal, mutation, PayloadSource::None)?;
        self.metadata_after(&applied, path)
    }

    pub(crate) fn create_hard_link(
        &self,
        existing_path: &str,
        new_path: &str,
    ) -> Result<LocalMetadata> {
        let wal = self.writable_wal()?;
        let mutation = MountMutation::CreateHardLink {
            existing_path: existing_path.to_string(),
            new_path: new_path.to_string(),
        };
        mutation.validate()?;
        let keys = mutation.dependency_keys();
        let _guard = self.locks.acquire(&keys)?;
        let applied = self.commit_mutation(wal, mutation, PayloadSource::None)?;
        self.metadata_after(&applied, new_path)
    }

    /// `rename(2)`. Honours `RENAME_NOREPLACE` locally through `renameat2`;
    /// rejects `RENAME_EXCHANGE` and `RENAME_WHITEOUT` with `EINVAL`, because
    /// neither has an atomic form in the gateway contract and decomposing them
    /// would publish a namespace state this mount never had.
    ///
    /// A replacing rename needs no ambiguity verification any more: the
    /// displaced inode survives natively as an open-unlinked backing inode.
    pub(crate) fn rename(&self, old_path: &str, new_path: &str, flags: u32) -> Result<()> {
        let wal = self.writable_wal()?;
        if flags & (RENAME_EXCHANGE | RENAME_WHITEOUT) != 0 {
            return Err(errno_error(
                libc::EINVAL,
                "RENAME_EXCHANGE and RENAME_WHITEOUT have no atomic gateway form",
            ));
        }
        let mutation = MountMutation::Rename {
            old_path: old_path.to_string(),
            new_path: new_path.to_string(),
            flags,
        };
        mutation.validate()?;
        let keys = mutation.dependency_keys();
        let _guard = self.locks.acquire(&keys)?;
        self.commit_mutation(wal, mutation, PayloadSource::None)?;
        // The dirty marker follows the name, so a later seal still finds the
        // generation the guest wrote under the old path.
        self.retarget_dirty(wal, old_path, new_path)?;
        Ok(())
    }

    pub(crate) fn remove_file(&self, path: &str) -> Result<()> {
        let wal = self.writable_wal()?;
        // An unlinked name may still be reachable through a hard link, so the
        // pending generation is sealed before the name disappears. A clean path
        // costs nothing here.
        if self.is_dirty(path)? {
            self.seal_content(path)?;
        }
        let mutation = MountMutation::RemoveFile {
            path: path.to_string(),
            expected_file_id: None,
        };
        mutation.validate()?;
        let keys = mutation.dependency_keys();
        let _guard = self.locks.acquire(&keys)?;
        self.commit_mutation(wal, mutation, PayloadSource::None)?;
        if self.is_dirty(path)? {
            self.clear_dirty(wal, path)?;
        }
        Ok(())
    }

    pub(crate) fn remove_directory(&self, path: &str) -> Result<()> {
        let wal = self.writable_wal()?;
        let mutation = MountMutation::RemoveDirectory {
            path: path.to_string(),
        };
        mutation.validate()?;
        let keys = mutation.dependency_keys();
        let _guard = self.locks.acquire(&keys)?;
        self.commit_mutation(wal, mutation, PayloadSource::None)?;
        Ok(())
    }

    pub(crate) fn set_mode(&self, path: &str, mode: u32) -> Result<LocalMetadata> {
        let wal = self.writable_wal()?;
        let mutation = MountMutation::SetMode {
            path: path.to_string(),
            mode,
        };
        mutation.validate()?;
        let keys = mutation.dependency_keys();
        let _guard = self.locks.acquire(&keys)?;
        let applied = self.commit_mutation(wal, mutation, PayloadSource::None)?;
        self.metadata_after(&applied, path)
    }

    /// `utimens`. Journalled even though the gateway has no timestamp field, so
    /// the local log stays a complete description of the accepted tree; the
    /// publisher advances the cursor past it without a request.
    pub(crate) fn set_times(
        &self,
        path: &str,
        atime: Option<LocalTimestamp>,
        mtime: Option<LocalTimestamp>,
    ) -> Result<LocalMetadata> {
        let wal = self.writable_wal()?;
        let mutation = MountMutation::SetTimes {
            path: path.to_string(),
            atime,
            mtime,
        };
        mutation.validate()?;
        let keys = mutation.dependency_keys();
        let _guard = self.locks.acquire(&keys)?;
        let applied = self.commit_mutation(wal, mutation, PayloadSource::None)?;
        self.metadata_after(&applied, path)
    }

    /// `chown`. Same local-only treatment as `set_times`.
    pub(crate) fn set_owner(
        &self,
        path: &str,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<LocalMetadata> {
        let wal = self.writable_wal()?;
        let mutation = MountMutation::SetOwner {
            path: path.to_string(),
            uid,
            gid,
        };
        mutation.validate()?;
        let keys = mutation.dependency_keys();
        let _guard = self.locks.acquire(&keys)?;
        let applied = self.commit_mutation(wal, mutation, PayloadSource::None)?;
        self.metadata_after(&applied, path)
    }

    // -- content -------------------------------------------------------------

    /// `open(2)` on an existing path.
    pub(crate) fn open_file(&self, path: &str, flags: i32) -> Result<(MountFile, LocalMetadata)> {
        let wants_write = (flags & libc::O_ACCMODE) != libc::O_RDONLY;
        let wants_truncate = (flags & libc::O_TRUNC) != 0;
        if self.read_only && (wants_write || wants_truncate) {
            return Err(errno_error(
                libc::EROFS,
                "mount is a read-only replica of its scope",
            ));
        }
        // Creation and truncation are the mount's business, not the backing
        // open's: the truncate is a content mutation that has to dirty the path.
        let open_flags = flags & !(libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC);
        let file = self.tree.open_file(path, open_flags)?;
        if wants_truncate {
            let wal = self.writable_wal()?;
            self.reject_content_under_pressure("an O_TRUNC open")?;
            let keys = content_keys(path, false);
            let _guard = self.locks.acquire(&keys)?;
            self.mark_dirty(wal, path)?;
            file.truncate(0)?;
        }
        let metadata = file.metadata()?;
        Ok((file, metadata))
    }

    /// `write(2)`. Marks the path dirty on the clean -> dirty transition, then
    /// writes straight through to the backing file. No read-modify-write, no
    /// journal append per write, no gateway call.
    pub(crate) fn write(&self, file: &MountFile, bytes: &[u8], offset: u64) -> Result<usize> {
        let wal = self.writable_wal()?;
        self.reject_content_under_pressure("a write")?;
        let path = file.path();
        // Shared on the path (and on every ancestor) so a concurrent seal, which
        // takes the exclusive key, cannot interleave between payload capture and
        // the committed `ReplaceFile` that describes it.
        let keys = content_keys(&path, false);
        let _guard = self.locks.acquire(&keys)?;
        self.mark_dirty(wal, &path)?;
        file.write_at(bytes, offset)
    }

    /// `fallocate(2)`. Reserving space changes the file's length, so it is a
    /// content mutation and dirties the path exactly as a write does.
    pub(crate) fn allocate(&self, file: &MountFile, offset: u64, length: u64) -> Result<()> {
        let wal = self.writable_wal()?;
        self.reject_content_under_pressure("a fallocate")?;
        let path = file.path();
        let keys = content_keys(&path, false);
        let _guard = self.locks.acquire(&keys)?;
        self.mark_dirty(wal, &path)?;
        file.allocate(offset, length)
    }

    /// `copy_file_range(2)`. Only the destination is mutated, so only it takes a
    /// dirty marker; the source is read through its own descriptor and needs no
    /// key beyond the one its own writers already take.
    pub(crate) fn copy_range(
        &self,
        destination: &MountFile,
        source: &MountFile,
        source_offset: u64,
        offset: u64,
        length: u64,
    ) -> Result<u64> {
        let wal = self.writable_wal()?;
        self.reject_content_under_pressure("a copy_file_range")?;
        let path = destination.path();
        let keys = content_keys(&path, false);
        let _guard = self.locks.acquire(&keys)?;
        self.mark_dirty(wal, &path)?;
        destination.copy_range_from(source, source_offset, offset, length)
    }

    /// `read(2)`. Straight `pread` on the backing descriptor.
    pub(crate) fn read(&self, file: &MountFile, buffer: &mut [u8], offset: u64) -> Result<usize> {
        file.read_at(buffer, offset)
    }

    /// `setattr` with a size. A truncate is a content mutation, not a namespace
    /// one: it dirties the path and is sealed as a new generation.
    pub(crate) fn truncate(&self, file: &MountFile, size: u64) -> Result<LocalMetadata> {
        let wal = self.writable_wal()?;
        self.reject_content_under_pressure("a truncate")?;
        let path = file.path();
        let keys = content_keys(&path, false);
        let _guard = self.locks.acquire(&keys)?;
        self.mark_dirty(wal, &path)?;
        file.truncate(size)?;
        file.metadata()
    }

    /// Path-addressed truncate, for a `setattr` that arrived without a handle.
    /// The backing tree resolves a path without a handle natively, so none of the
    /// old handle-folding fallbacks are needed.
    pub(crate) fn truncate_path(&self, path: &str, size: u64) -> Result<LocalMetadata> {
        let wal = self.writable_wal()?;
        self.reject_content_under_pressure("a truncate")?;
        let file = self.tree.open_file(path, libc::O_WRONLY)?;
        let keys = content_keys(path, false);
        let _guard = self.locks.acquire(&keys)?;
        self.mark_dirty(wal, path)?;
        file.truncate(size)?;
        file.metadata()
    }

    /// Seal a path's current backing content into one committed `ReplaceFile`
    /// generation, returning its sequence.
    ///
    /// Holds the path's **exclusive** dependency key across payload capture and
    /// commit, so no guest write can interleave between the snapshot and the
    /// record. That is what lets recovery treat "a committed `ReplaceFile`
    /// clears the dirty marker" as unconditionally correct. With a reflink the
    /// window is microseconds; the streaming fallback is the only case where a
    /// concurrent writer waits, and it happens at close, when the writer is done.
    pub(crate) fn seal_content(&self, path: &str) -> Result<Option<u64>> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(None);
        };
        if !self.is_dirty(path)? {
            return Ok(None);
        }
        let keys = content_keys(path, true);
        let _guard = self.locks.acquire(&keys)?;
        if !self.is_dirty(path)? {
            // Another handle sealed this generation while we waited for the key.
            return Ok(None);
        }
        self.seal_locked(wal, path)
    }

    /// The seal itself. The caller must already hold `path`'s exclusive key.
    fn seal_locked(&self, wal: &MountWal, path: &str) -> Result<Option<u64>> {
        let Some(metadata) = self.tree.lstat(path)? else {
            // The name is gone; whatever content it held is either unlinked or
            // reachable under the name a rename moved it to.
            self.clear_dirty(wal, path)?;
            return Ok(None);
        };
        if !metadata.is_file() {
            self.clear_dirty(wal, path)?;
            return Ok(None);
        }

        let backing_path = self.tree.resolve(path)?;
        let mutation = MountMutation::ReplaceFile {
            path: path.to_string(),
            mode: metadata.mode,
            expected_file_id: None,
            base_content_hash: None,
        };
        mutation.validate()?;
        let pre_image = {
            let paths = mutation.affected_paths();
            self.tree.observe(&paths)?
        };
        let prepared = wal.prepare(
            mutation,
            PayloadSource::File(backing_path.as_path()),
            pre_image,
        )?;
        let sequence = prepared.sequence();
        match self.tree.apply(&prepared.event.mutation) {
            Ok(applied) => wal.commit(prepared, applied.local_identity.clone())?,
            Err(error) => {
                wal.abort(prepared, &format!("{error:#}"))?;
                return Err(error);
            }
        }
        self.clear_dirty(wal, path)?;
        Ok(Some(sequence))
    }

    /// Seal every dirty path. Used by `fsyncdir`, snapshot and drain.
    pub(crate) fn seal_all_dirty_content(&self) -> Result<u64> {
        let mut sealed = 0;
        for path in self.dirty_snapshot()? {
            if self.seal_content(&path)?.is_some() {
                sealed += 1;
            }
        }
        Ok(sealed)
    }

    /// `flush(2)` / `release(2)`: seal the generation locally. Never waits for
    /// the publisher and never calls a gateway.
    pub(crate) fn flush_handle(&self, file: &MountFile) -> Result<()> {
        self.seal_content(&file.path())?;
        Ok(())
    }

    /// `fsync(2)`: make the guest's bytes durable on the local device. Seals the
    /// generation, `fdatasync`s the backing file, and group-syncs the WAL. It is
    /// explicitly not a network round trip -- the bytes are already in the
    /// durable local log, which is what POSIX durability means for this mount.
    pub(crate) fn fsync_handle(&self, file: &MountFile, datasync: bool) -> Result<()> {
        let sealed = self.seal_content(&file.path())?;
        if datasync {
            file.sync_data()?;
        } else {
            file.sync_all()?;
        }
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        let through = sealed.unwrap_or_else(|| wal.last_appended_sequence());
        wal.sync_through(through)?;
        Ok(())
    }

    /// `fsyncdir(2)`: seal everything under the directory, fsync it, group-sync
    /// the WAL.
    pub(crate) fn fsync_dir(&self, path: &str) -> Result<()> {
        for dirty in self.dirty_snapshot()? {
            if is_at_or_under(&dirty, path) {
                self.seal_content(&dirty)?;
            }
        }
        self.tree.fsync_dir(path)?;
        if let Some(wal) = self.wal.as_ref() {
            wal.sync_local()?;
        }
        Ok(())
    }

    // -- reads ---------------------------------------------------------------

    pub(crate) fn statfs(&self) -> Result<LocalStatfs> {
        self.tree.statfs()
    }

    /// The last evaluated pressure. Evaluation itself is a `statvfs` plus a
    /// directory walk, so it belongs to the maintenance task, never to a
    /// callback that is about to be asked this question per write.
    pub(crate) fn storage_pressure(&self) -> StoragePressure {
        decode_pressure(self.pressure.load(Ordering::Relaxed))
    }

    // -- lifecycle -----------------------------------------------------------

    pub(crate) fn publication_health(&self) -> PublicationHealth {
        let mut health = match self.publisher_handle() {
            Some(publisher) => publisher.health(),
            None => PublicationHealth::default(),
        };
        if let Some(wal) = self.wal.as_ref() {
            health.acknowledged_sequence = wal.acknowledged_sequence();
            health.last_committed_sequence = wal.last_committed_sequence();
            health.pending_events = wal.pending_publication_depth();
            health.pending_payload_bytes = wal.pending_payload_bytes();
        }
        let pressure = self.storage_pressure();
        health.storage_pressure_soft =
            matches!(pressure, StoragePressure::Soft) || matches!(pressure, StoragePressure::Hard);
        health.storage_pressure_hard = matches!(pressure, StoragePressure::Hard);
        health
    }

    /// Wait until every committed event is remotely acknowledged, or the
    /// deadline expires, or a permanently rejected event blocks the suffix.
    ///
    /// Seals dirty content first, so a drain covers what the guest wrote rather
    /// than only what happened to be sealed already. Called by unmount, snapshot,
    /// fork, explicit remote-sync and delete.
    pub(crate) async fn drain(&self, deadline: Duration) -> Result<DrainOutcome> {
        self.seal_all_dirty_content()?;
        let Some(wal) = self.wal.as_ref() else {
            return Ok(DrainOutcome::Drained {
                through_sequence: 0,
            });
        };
        match self.live_publisher() {
            Some(publisher) => publisher.drain(deadline).await,
            None => Ok(local_drain_outcome(wal)),
        }
    }

    /// Drain only through the sequence that last touched `path`. Used by the
    /// advisory-lock callbacks, which are the one deliberate remaining gateway
    /// user on a callback and need the path to exist remotely first.
    pub(crate) async fn drain_path(&self, path: &str, deadline: Duration) -> Result<DrainOutcome> {
        let sealed = self.seal_content(path)?;
        let Some(wal) = self.wal.as_ref() else {
            return Ok(DrainOutcome::Drained {
                through_sequence: 0,
            });
        };
        // The WAL indexes sequences, not paths. When the path had no unsealed
        // generation of its own the committed watermark is the tightest bound
        // available, and draining a superset is always safe.
        let through = sealed.unwrap_or_else(|| wal.last_committed_sequence());
        match self.live_publisher() {
            Some(publisher) => publisher.drain_through(through, deadline).await,
            None => Ok(local_drain_outcome(wal)),
        }
    }

    /// Drain, stop the publisher, seal the log and publish a final checkpoint.
    /// After this the state directory is safe to leave behind for a restart and
    /// safe to delete only if the drain reported `Drained`.
    pub(crate) async fn shutdown(&self, deadline: Duration) -> Result<DrainOutcome> {
        // Idempotent: teardown retries, and a second pass must re-seal the log
        // without waiting on a coordinator the first pass already stopped.
        let already_stopped = self.stopping.swap(true, Ordering::AcqRel);
        self.seal_all_dirty_content()?;

        let publisher = match already_stopped {
            true => None,
            false => self.publisher_handle(),
        };
        let outcome = match (publisher, self.wal.as_ref()) {
            (Some(publisher), _) => publisher.shutdown(deadline).await?,
            (None, Some(wal)) => local_drain_outcome(wal),
            (None, None) => DrainOutcome::Drained {
                through_sequence: 0,
            },
        };

        let maintenance = {
            let mut slot = self
                .maintenance
                .lock()
                .map_err(|_| anyhow!("mount maintenance slot poisoned"))?;
            slot.take()
        };
        if let Some(task) = maintenance {
            task.abort();
            let _ = task.await;
        }

        if let Some(wal) = self.wal.as_ref() {
            wal.seal(self.tree.tree_generation())?;
        }
        Ok(outcome)
    }

    /// Publish a checkpoint now. Used by the maintenance task and by snapshot.
    pub(crate) fn checkpoint_now(&self) -> Result<()> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        wal.checkpoint(self.tree.tree_generation())?;
        self.checkpointed_sequence
            .store(wal.last_appended_sequence(), Ordering::Relaxed);
        Ok(())
    }

    /// Background maintenance: periodic checkpoint, log rotation, compaction and
    /// storage-pressure evaluation. Runs off every callback hot path.
    pub(crate) fn spawn_maintenance(self: &Arc<Self>) -> Result<()> {
        if self.wal.is_none() {
            return Ok(());
        }
        let mut slot = self
            .maintenance
            .lock()
            .map_err(|_| anyhow!("mount maintenance slot poisoned"))?;
        if slot.is_some() {
            return Ok(());
        }

        let weak = Arc::downgrade(self);
        let state_dir = self.layout.root().to_path_buf();
        let interval = Duration::from_millis(CHECKPOINT_INTERVAL_MS);
        let poll = Duration::from_millis((CHECKPOINT_INTERVAL_MS / 10).max(50));
        let task = self.tokio.spawn(async move {
            let mut last_checkpoint = Instant::now();
            loop {
                tokio::time::sleep(poll).await;
                let Some(view) = weak.upgrade() else {
                    return;
                };
                if view.stopping.load(Ordering::Acquire) {
                    return;
                }
                let checkpoint_due = last_checkpoint.elapsed() >= interval
                    || view.uncheckpointed_events() >= CHECKPOINT_EVENT_INTERVAL;
                // Every step here is blocking device work; none of it may run on
                // a runtime worker that a callback's publisher shares.
                match tokio::task::spawn_blocking(move || view.maintenance_cycle(checkpoint_due))
                    .await
                {
                    Ok(Ok(true)) => last_checkpoint = Instant::now(),
                    Ok(Ok(false)) => {}
                    Ok(Err(error)) => tracing::warn!(
                        state_dir = %state_dir.display(),
                        error = %format!("{error:#}"),
                        "mount-local maintenance cycle failed"
                    ),
                    Err(error) => {
                        if error.is_cancelled() {
                            return;
                        }
                        tracing::warn!(
                            state_dir = %state_dir.display(),
                            error = %error,
                            "mount-local maintenance task panicked"
                        );
                    }
                }
            }
        });
        *slot = Some(task);
        Ok(())
    }

    /// One maintenance pass. Returns whether a checkpoint was published.
    fn maintenance_cycle(&self, checkpoint_due: bool) -> Result<bool> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(false);
        };
        wal.rotate_if_needed()?;

        let appended = wal.last_appended_sequence();
        let checkpointed =
            checkpoint_due && appended != self.checkpointed_sequence.load(Ordering::Relaxed);
        if checkpointed {
            wal.checkpoint(self.tree.tree_generation())?;
            self.checkpointed_sequence
                .store(appended, Ordering::Relaxed);
        }

        let acknowledged = wal.acknowledged_sequence();
        if acknowledged > self.compacted_sequence.load(Ordering::Relaxed) {
            let outcome = wal.compact()?;
            self.compacted_sequence
                .store(acknowledged, Ordering::Relaxed);
            if outcome.reclaimed_bytes > 0 {
                tracing::debug!(
                    state_dir = %self.layout.root().display(),
                    removed_log_generations = outcome.removed_log_generations,
                    removed_payload_files = outcome.removed_payload_files,
                    reclaimed_bytes = outcome.reclaimed_bytes,
                    "mount-local WAL compaction reclaimed space"
                );
            }
        }

        let previous = self.storage_pressure();
        let pressure = self.refresh_storage_pressure();
        if pressure != previous && !matches!(pressure, StoragePressure::None) {
            tracing::warn!(
                state_dir = %self.layout.root().display(),
                pressure = ?pressure,
                pending_events = wal.pending_publication_depth(),
                pending_payload_bytes = wal.pending_payload_bytes(),
                "mount-local durable storage is under pressure"
            );
        }
        Ok(checkpointed)
    }

    fn uncheckpointed_events(&self) -> u64 {
        let Some(wal) = self.wal.as_ref() else {
            return 0;
        };
        wal.last_appended_sequence()
            .saturating_sub(self.checkpointed_sequence.load(Ordering::Relaxed))
    }

    fn refresh_storage_pressure(&self) -> StoragePressure {
        let pressure = match self.wal.as_ref() {
            Some(wal) => wal.storage_pressure(),
            None => StoragePressure::None,
        };
        self.pressure
            .store(encode_pressure(pressure), Ordering::Relaxed);
        pressure
    }

    // -- shared protocol -----------------------------------------------------

    /// prepare -> apply -> commit/abort, with the caller already holding every
    /// dependency key the mutation declared.
    fn commit_mutation(
        &self,
        wal: &MountWal,
        mutation: MountMutation,
        payload: PayloadSource<'_>,
    ) -> Result<AppliedMutation> {
        let prepared = {
            let paths = mutation.affected_paths();
            let pre_image = self.tree.observe(&paths)?;
            wal.prepare(mutation, payload, pre_image)?
        };
        match self.tree.apply(&prepared.event.mutation) {
            Ok(applied) => {
                wal.commit(prepared, applied.local_identity.clone())?;
                Ok(applied)
            }
            Err(error) => {
                wal.abort(prepared, &format!("{error:#}"))?;
                Err(error)
            }
        }
    }

    fn metadata_after(&self, applied: &AppliedMutation, path: &str) -> Result<LocalMetadata> {
        if let Some(metadata) = applied.metadata.clone() {
            return Ok(metadata);
        }
        self.tree
            .lstat(path)?
            .ok_or_else(|| anyhow!("mount path {path:?} disappeared immediately after its apply"))
    }

    fn writable_wal(&self) -> Result<&MountWal> {
        if self.read_only {
            return Err(errno_error(
                libc::EROFS,
                "mount is a read-only replica of its scope",
            ));
        }
        self.wal.as_ref().ok_or_else(|| {
            errno_error(
                libc::EROFS,
                "mount has no write-ahead log and cannot accept mutations",
            )
        })
    }

    fn reject_content_under_pressure(&self, what: &str) -> Result<()> {
        if self.storage_pressure().blocks_content() {
            return Err(errno_error(
                libc::ENOSPC,
                format!("mount-local durable storage is exhausted; {what} is refused"),
            ));
        }
        Ok(())
    }

    fn publisher_handle(&self) -> Option<Arc<MountPublisher>> {
        self.publisher
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().map(Arc::clone))
    }

    /// The publisher, but only while it can still advance the cursor.
    ///
    /// After `shutdown` the coordinator is stopped, so waiting on it would burn
    /// a whole lifecycle deadline to learn what the WAL already knows. Teardown
    /// retries an unmount more than once, so this is a real path, not a corner.
    fn live_publisher(&self) -> Option<Arc<MountPublisher>> {
        if self.stopping.load(Ordering::Acquire) {
            return None;
        }
        self.publisher_handle()
    }

    // -- content dirty set ---------------------------------------------------

    fn lock_dirty(&self) -> Result<MutexGuard<'_, BTreeSet<String>>> {
        self.dirty
            .lock()
            .map_err(|_| anyhow!("mount content-dirty index poisoned"))
    }

    fn is_dirty(&self, path: &str) -> Result<bool> {
        Ok(self.lock_dirty()?.contains(path))
    }

    /// Every dirty path, from the index **unioned with the WAL's durable set**.
    /// The union repairs the index, so a bulk seal can never miss a generation
    /// the durable set still holds. Only bulk callers pay for it; a per-handle
    /// flush stays on the O(1) index lookup.
    fn dirty_snapshot(&self) -> Result<Vec<String>> {
        let durable = match self.wal.as_ref() {
            Some(wal) => wal.dirty_content_paths()?,
            None => Vec::new(),
        };
        let mut dirty = self.lock_dirty()?;
        for path in durable {
            dirty.insert(path);
        }
        Ok(dirty.iter().cloned().collect())
    }

    /// Durable first, index second: a failed append leaves the path clean in the
    /// index and fails the write, never the other way round.
    fn mark_dirty(&self, wal: &MountWal, path: &str) -> Result<()> {
        if self.lock_dirty()?.contains(path) {
            return Ok(());
        }
        wal.record_content_dirty(path)?;
        self.lock_dirty()?.insert(path.to_string());
        Ok(())
    }

    fn clear_dirty(&self, wal: &MountWal, path: &str) -> Result<()> {
        wal.clear_content_dirty(path)?;
        self.lock_dirty()?.remove(path);
        Ok(())
    }

    fn retarget_dirty(&self, wal: &MountWal, old_path: &str, new_path: &str) -> Result<()> {
        let moved: Vec<String> = {
            let dirty = self.lock_dirty()?;
            dirty
                .iter()
                .filter(|path| is_at_or_under(path, old_path))
                .cloned()
                .collect()
        };
        if moved.is_empty() {
            return Ok(());
        }
        wal.rename_content_dirty(old_path, new_path)?;
        let mut dirty = self.lock_dirty()?;
        // The destination's own generation is displaced by the rename; only the
        // source subtree survives under the new name.
        let displaced: Vec<String> = dirty
            .iter()
            .filter(|path| is_at_or_under(path, new_path))
            .cloned()
            .collect();
        for path in displaced {
            dirty.remove(&path);
        }
        for path in moved {
            dirty.remove(&path);
            dirty.insert(retarget_path(&path, old_path, new_path));
        }
        Ok(())
    }
}

/// `path` is `prefix` itself or lives underneath it. An empty prefix is the
/// mount root, which contains everything.
fn is_at_or_under(path: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    if path == prefix {
        return true;
    }
    path.len() > prefix.len() && path.starts_with(prefix) && path.as_bytes()[prefix.len()] == b'/'
}

fn retarget_path(path: &str, old_prefix: &str, new_prefix: &str) -> String {
    if old_prefix.is_empty() {
        return match path.is_empty() {
            true => new_prefix.to_string(),
            false => format!("{new_prefix}/{path}"),
        };
    }
    match path.len() > old_prefix.len() {
        true => format!("{new_prefix}{}", &path[old_prefix.len()..]),
        false => new_prefix.to_string(),
    }
}

/// The keys a content mutation on `path` holds: shared on every proper ancestor
/// so an ancestor rename is mutually exclusive with it, and shared or exclusive
/// on the path itself. A seal takes the exclusive form; a write takes the shared
/// one, so concurrent writers to one file never serialize against each other.
fn content_keys(path: &str, exclusive: bool) -> Vec<DependencyKey> {
    let mut keys: Vec<DependencyKey> = ancestors_of(path)
        .into_iter()
        .map(|ancestor| DependencyKey {
            path: ancestor,
            exclusive: false,
        })
        .collect();
    keys.push(DependencyKey {
        path: path.to_string(),
        exclusive,
    });
    merge_dependency_keys(keys)
}

fn local_drain_outcome(wal: &MountWal) -> DrainOutcome {
    let pending = wal.pending_publication_depth();
    if pending == 0 {
        return DrainOutcome::Drained {
            through_sequence: wal.acknowledged_sequence(),
        };
    }
    // Nothing is publishing, so the backlog cannot move. Reporting a timeout
    // immediately is honest; the WAL is intact and the caller decides.
    DrainOutcome::TimedOut {
        acknowledged_sequence: wal.acknowledged_sequence(),
        pending_events: pending,
    }
}

fn encode_pressure(pressure: StoragePressure) -> u8 {
    match pressure {
        StoragePressure::None => 0,
        StoragePressure::Soft => 1,
        StoragePressure::Hard => 2,
    }
}

fn decode_pressure(value: u8) -> StoragePressure {
    match value {
        0 => StoragePressure::None,
        1 => StoragePressure::Soft,
        _ => StoragePressure::Hard,
    }
}

/// An error that carries a POSIX code, so `fs.rs` can answer the guest with the
/// errno the local filesystem would have produced instead of a blanket `EIO`.
fn errno_error(code: i32, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(std::io::Error::from_raw_os_error(code)).context(message.into())
}

/// A keyed shared/exclusive lock table over dependency keys.
///
/// Acquisition is always in the sorted order the key set is produced in, so
/// overlapping mutations cannot deadlock. Entries are reference-counted and
/// dropped when idle, so the table does not grow with the tree.
pub(crate) struct PathLocks {
    table: Arc<PathLockTable>,
}

#[derive(Default)]
struct PathLockEntry {
    shared: usize,
    exclusive: bool,
    waiters: usize,
}

impl PathLockEntry {
    fn idle(&self) -> bool {
        self.shared == 0 && !self.exclusive && self.waiters == 0
    }
}

struct PathLockTable {
    entries: Mutex<HashMap<String, PathLockEntry>>,
    released: Condvar,
}

impl PathLockTable {
    fn acquire_key(&self, key: &DependencyKey) -> Result<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| anyhow!("mount path lock table poisoned"))?;
        loop {
            let entry = entries.entry(key.path.clone()).or_default();
            let available = if key.exclusive {
                entry.shared == 0 && !entry.exclusive
            } else {
                !entry.exclusive
            };
            if available {
                if key.exclusive {
                    entry.exclusive = true;
                } else {
                    entry.shared += 1;
                }
                return Ok(());
            }
            entry.waiters += 1;
            entries = self
                .released
                .wait(entries)
                .map_err(|_| anyhow!("mount path lock table poisoned"))?;
            if let Some(entry) = entries.get_mut(key.path.as_str()) {
                entry.waiters = entry.waiters.saturating_sub(1);
                if entry.idle() {
                    entries.remove(key.path.as_str());
                }
            }
        }
    }

    fn release_key(&self, key: &DependencyKey) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        if let Some(entry) = entries.get_mut(key.path.as_str()) {
            if key.exclusive {
                entry.exclusive = false;
            } else {
                entry.shared = entry.shared.saturating_sub(1);
            }
            if entry.idle() {
                entries.remove(key.path.as_str());
            }
        }
        drop(entries);
        self.released.notify_all();
    }
}

impl PathLocks {
    pub(crate) fn new() -> Self {
        Self {
            table: Arc::new(PathLockTable {
                entries: Mutex::new(HashMap::new()),
                released: Condvar::new(),
            }),
        }
    }

    /// Acquire every key in `keys` in order, returning a guard that releases
    /// them in reverse.
    pub(crate) fn acquire(&self, keys: &[DependencyKey]) -> Result<PathLockGuard> {
        // The guard is built incrementally so a failure part-way through
        // releases exactly what was taken, in reverse, on unwind.
        let mut guard = PathLockGuard {
            table: Arc::clone(&self.table),
            held: Vec::with_capacity(keys.len()),
        };
        for key in keys {
            self.table.acquire_key(key)?;
            guard.held.push(key.clone());
        }
        Ok(guard)
    }
}

pub(crate) struct PathLockGuard {
    table: Arc<PathLockTable>,
    held: Vec<DependencyKey>,
}

impl Drop for PathLockGuard {
    fn drop(&mut self) {
        while let Some(key) = self.held.pop() {
            self.table.release_key(&key);
        }
    }
}

/// Derive the mount state directory for a mountpoint that has no VM directory
/// (the operator binary). Kept beside the mountpoint, never inside it.
pub(crate) fn default_state_dir_for_mountpoint(mountpoint: &Path) -> Result<MountStateLayout> {
    let parent = mountpoint.parent().ok_or_else(|| {
        anyhow!(
            "mountpoint {} has no parent directory to hold its mount state",
            mountpoint.display()
        )
    })?;
    let name = mountpoint
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            anyhow!(
                "mountpoint {} has no usable directory name",
                mountpoint.display()
            )
        })?;
    // The backing tree, the WAL and the payloads must share one filesystem with
    // each other, and must live outside the mountpoint itself.
    Ok(MountStateLayout::new(
        &parent.join(format!(".{name}-vfs-state")),
    ))
}
