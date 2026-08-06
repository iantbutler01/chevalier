//! Mount-local materialized view for one single-owner writable VFS mount.
//!
//! The guest reaches this mount through virtiofs over the host FUSE mount. Every
//! FUSE callback reads and mutates a real local backing tree that lives beside
//! the mountpoint, and every mutation is recorded in one sequenced write-ahead
//! log. No ordinary callback calls a gateway. A background publisher is the only
//! component that reconciles committed WAL records to the gateway, and a remote
//! acknowledgement only advances a cursor -- it never invalidates or rolls back
//! state the owning mount already accepted.
//!
//! Layering, outermost first:
//!
//! ```text
//! mount.rs      MountLocalView   the facade every FUSE callback uses
//!   tree.rs     BackingTree      the local materialized tree + single-syscall applies
//!   wal.rs      MountWal         the sequenced log, checkpoints, compaction, group fsync
//!     payload.rs PayloadStore    immutable payload segments and dedicated files
//! hydrate.rs    MountHydrator    eager gateway hydration into the backing tree
//! publisher.rs  MountPublisher   the ordered asynchronous gateway publisher
//! types.rs                       every shared type and on-disk record shape
//! ```
//!
//! Nothing below `mount.rs` knows about the gateway wire contract except
//! `hydrate.rs` and `publisher.rs`, which are the only gateway callers.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub(crate) mod hydrate;
pub(crate) mod mount;
pub(crate) mod payload;
pub(crate) mod publisher;
pub(crate) mod tree;
pub(crate) mod types;
pub(crate) mod wal;

/// Bumped whenever a record or checkpoint field changes meaning. A mount whose
/// durable state carries a different version fails closed rather than replaying
/// records it may misinterpret.
pub(crate) const WAL_FORMAT_VERSION: u32 = 2;

/// Largest single WAL line accepted during replay. Payloads are by reference, so
/// this bounds record framing only -- never file content.
pub(crate) const MAX_RECORD_BYTES: usize = 1024 * 1024;

/// Soft rotate threshold for a packed payload segment.
pub(crate) const SEGMENT_TARGET_BYTES: u64 = 16 * 1024 * 1024;

/// A payload at or below this size is packed into a shared segment; anything
/// larger gets its own immutable file so the publisher can stream it.
pub(crate) const MAX_SEGMENTED_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Soft rotate threshold for one WAL log generation. Sealed generations are
/// deletable once the checkpoint and the remote cursor both cover them.
pub(crate) const LOG_TARGET_BYTES: u64 = 64 * 1024 * 1024;

/// Payload bytes above which the publisher streams from a dedicated payload file
/// instead of reading it into a `write-many` batch.
pub(crate) const STREAM_PAYLOAD_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;

/// Largest event count and payload byte total the publisher pulls in one batch.
/// The gateway accepts 4096 items per namespace/write request. WAL events also
/// include local-only metadata and foldable create/mode records, so a 4096-event
/// prefix remains within that wire bound while avoiding hundreds of tiny
/// checkpoints for one materialized tree.
pub(crate) const PUBLISH_MAX_EVENTS: usize = 4096;
pub(crate) const PUBLISH_MAX_PAYLOAD_BYTES: u64 = 16 * 1024 * 1024;

/// Publication window: the coordinator waits for a short idle gap before
/// building a batch, but never delays a batch longer than the maximum.
pub(crate) const PUBLISH_IDLE_WINDOW_MS: u64 = 50;
pub(crate) const PUBLISH_MAX_WINDOW_MS: u64 = 100;

/// Bounded worker pool for independent payload uploads and independent
/// dependency sets.
pub(crate) const PUBLISH_CONCURRENCY: usize = 8;

/// Bounded concurrency for the eager hydrate's gateway reads.
pub(crate) const HYDRATE_CONCURRENCY: usize = 16;

/// Checkpoint cadence. Whichever bound trips first schedules a checkpoint on the
/// background maintenance task, never on a FUSE callback.
pub(crate) const CHECKPOINT_INTERVAL_MS: u64 = 5_000;
pub(crate) const CHECKPOINT_EVENT_INTERVAL: u64 = 4_096;

/// Unreclaimed WAL + payload bytes at which the mount starts pushing harder on
/// publication and surfaces a warning. A backlog alone must never manufacture
/// `ENOSPC`: the local view is authoritative and must remain writable while its
/// replica repairs. Hard pressure is derived from the backing filesystem's real
/// free space below.
pub(crate) const WAL_SOFT_LIMIT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Fraction of the backing filesystem that must remain free before the hard
/// limit applies. This is the only synthetic `ENOSPC` guard; actual filesystem
/// allocation errors continue to propagate directly from the backing tree.
pub(crate) const BACKING_FREE_FRACTION_FLOOR: f64 = 0.05;

/// Default bound for an explicit lifecycle drain (unmount, snapshot, delete).
pub(crate) const DEFAULT_DRAIN_TIMEOUT_MS: u64 = 30_000;

/// Bound for a drain taken while a per-VM lock is held (the qemu exit reaper).
pub(crate) const GUARDED_DRAIN_TIMEOUT_MS: u64 = 5_000;

const VFS_STATE_DIR: &str = "vfs-state";
const TREE_DIR: &str = "tree";
const WAL_DIR: &str = "wal";
const PAYLOAD_DIR: &str = "payloads";
const CHECKPOINT_FILE: &str = "checkpoint.json";
const PREVIOUS_CHECKPOINT_FILE: &str = "checkpoint.prev.json";
const OWNER_FILE: &str = "owner.json";
const LOG_PREFIX: &str = "events-";
const LOG_SUFFIX: &str = ".jsonl";

/// Every durable path this mount owns. The backing tree, the WAL and the
/// payloads live beside -- never inside -- the FUSE mountpoint, and all three
/// live on one filesystem so payload capture can reflink from the backing tree.
#[derive(Clone, Debug)]
pub(crate) struct MountStateLayout {
    root: PathBuf,
}

impl MountStateLayout {
    pub(crate) fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
        }
    }

    /// `<vm_dir>/vfs-state`: the directory holding one state directory per
    /// mount tag. Destructive VM lifecycle transitions walk it to find the
    /// durable state they are about to remove.
    pub(crate) fn vfs_state_root(vm_dir: &Path) -> PathBuf {
        vm_dir.join(VFS_STATE_DIR)
    }

    /// `<vm_dir>/vfs-state/<mount_tag>`. Deliberately outside `fuse-mounts/`:
    /// the launch path recursively chowns everything under the VM directory
    /// except `fuse-mounts`, and the stale-mount reaper lazily unmounts every
    /// directory inside it. Both would be catastrophic for a backing tree, so
    /// `configure_qemu_process_identity`'s skip list must name `vfs-state` too.
    pub(crate) fn for_mount(vm_dir: &Path, mount_tag: &str) -> Self {
        Self::new(&Self::vfs_state_root(vm_dir).join(mount_tag))
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn tree_dir(&self) -> PathBuf {
        self.root.join(TREE_DIR)
    }

    pub(crate) fn wal_dir(&self) -> PathBuf {
        self.root.join(WAL_DIR)
    }

    pub(crate) fn payload_dir(&self) -> PathBuf {
        self.root.join(PAYLOAD_DIR)
    }

    pub(crate) fn checkpoint_path(&self) -> PathBuf {
        self.wal_dir().join(CHECKPOINT_FILE)
    }

    pub(crate) fn previous_checkpoint_path(&self) -> PathBuf {
        self.wal_dir().join(PREVIOUS_CHECKPOINT_FILE)
    }

    pub(crate) fn owner_path(&self) -> PathBuf {
        self.root.join(OWNER_FILE)
    }

    pub(crate) fn log_path(&self, generation: u64) -> PathBuf {
        self.wal_dir()
            .join(format!("{LOG_PREFIX}{generation:020}{LOG_SUFFIX}"))
    }

    /// Create every directory and fsync the parents so a crash cannot leave a
    /// half-materialized state directory.
    pub(crate) fn ensure(&self) -> Result<()> {
        for directory in [
            self.root.clone(),
            self.tree_dir(),
            self.wal_dir(),
            self.payload_dir(),
        ] {
            std::fs::create_dir_all(&directory)
                .with_context(|| format!("create mount state directory {}", directory.display()))?;
        }
        sync_directory(&self.root)?;
        sync_directory(&self.wal_dir())?;
        sync_directory(&self.payload_dir())
    }

    /// True when this state directory has never been populated, i.e. the mount
    /// must eagerly hydrate rather than recover.
    pub(crate) fn is_empty(&self) -> Result<bool> {
        if !self.owner_path().exists() {
            return Ok(true);
        }
        Ok(self.log_generations()?.is_empty() && !self.checkpoint_path().exists())
    }

    /// Every WAL log generation present on disk, ascending.
    pub(crate) fn log_generations(&self) -> Result<Vec<u64>> {
        let directory = self.wal_dir();
        if !directory.exists() {
            return Ok(Vec::new());
        }
        let mut generations = Vec::new();
        for entry in std::fs::read_dir(&directory)
            .with_context(|| format!("read mount WAL directory {}", directory.display()))?
        {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(raw) = name
                .strip_prefix(LOG_PREFIX)
                .and_then(|name| name.strip_suffix(LOG_SUFFIX))
            else {
                continue;
            };
            if let Ok(generation) = raw.parse::<u64>() {
                generations.push(generation);
            }
        }
        generations.sort_unstable();
        Ok(generations)
    }
}

/// Atomically publish a JSON document: write a fresh temp file, sync it, rename
/// over the target, then sync the directory so the rename itself is durable.
pub(crate) fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("mount state file has no parent: {}", path.display()))?;
    let temp = parent.join(format!(".{}.tmp", Uuid::now_v7()));
    let mut file = create_new(&temp)?;
    serde_json::to_writer(&mut file, value)
        .with_context(|| format!("encode mount state file {}", temp.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("write mount state file {}", temp.display()))?;
    file.sync_all()
        .with_context(|| format!("sync mount state file {}", temp.display()))?;
    std::fs::rename(&temp, path).with_context(|| {
        format!(
            "publish mount state file {} -> {}",
            temp.display(),
            path.display()
        )
    })?;
    sync_directory(parent)
}

/// Read a JSON document written by [`write_json_atomic`]. A missing file is
/// `None`; a malformed one is an error, never a silent default.
pub(crate) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes =
        std::fs::read(path).with_context(|| format!("read mount state file {}", path.display()))?;
    if bytes.is_empty() {
        bail!("mount state file {} is empty", path.display());
    }
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode mount state file {}", path.display()))?;
    Ok(Some(value))
}

pub(crate) fn open_append(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .with_context(|| format!("open append-only mount WAL {}", path.display()))
}

pub(crate) fn create_new(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(path)
        .with_context(|| format!("create {}", path.display()))
}

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}
