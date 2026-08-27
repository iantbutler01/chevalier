//! Every shared type of the mount-local view: the mutation vocabulary, the
//! on-disk record and checkpoint shapes, the pre-image witness recovery needs,
//! and the projections `fs.rs` turns into FUSE attributes.
//!
//! This file has no owner in the implementation split: it is the contract the
//! other files are written against. It is complete -- if a change is needed
//! here, it is a contract change, not an implementation detail.

use std::path::{Component, Path};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use super::WAL_FORMAT_VERSION;

// ---------------------------------------------------------------------------
// Mutation vocabulary
// ---------------------------------------------------------------------------

/// A wall-clock timestamp carried by a `SetTimes` mutation and by
/// [`LocalMetadata`]. Seconds plus nanoseconds so the WAL never depends on
/// `i128` JSON encoding.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct LocalTimestamp {
    pub(crate) secs: i64,
    pub(crate) nanos: u32,
}

/// The three entry kinds the backing tree and the gateway both model.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LocalKind {
    File,
    Directory,
    Symlink,
}

impl LocalKind {
    pub(crate) fn as_wire_kind(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
            Self::Symlink => "symlink",
        }
    }

    pub(crate) fn from_wire_kind(kind: &str) -> Option<Self> {
        match kind {
            "file" => Some(Self::File),
            "directory" | "dir" => Some(Self::Directory),
            "symlink" | "link" => Some(Self::Symlink),
            _ => None,
        }
    }
}

/// One ordered mutation accepted by the owning mount.
///
/// Every variant applies to the backing tree with exactly **one** atomic
/// syscall. That is what makes recovery total: an unresolved prepare either did
/// not happen or happened completely, and the recorded [`MountPreImage`] is the
/// witness that tells the two apart. Any future variant that cannot be applied
/// with one syscall must be decomposed into several sequenced mutations.
///
/// `SetTimes` and `SetOwner` have no representation in the gateway contract.
/// They are still journalled so the local log is a complete description of the
/// accepted tree; the publisher classifies them as local-only and advances the
/// cursor past them without a gateway call. That divergence is recorded, not
/// silent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub(crate) enum MountMutation {
    CreateDirectory {
        path: String,
        mode: u32,
    },
    CreateFile {
        path: String,
        mode: u32,
    },
    /// A whole-file content generation. The payload is an immutable snapshot of
    /// the backing file taken while this path's exclusive dependency key is
    /// held, so no guest write can interleave between capture and commit.
    ReplaceFile {
        path: String,
        mode: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_file_id: Option<String>,
        /// CAS base for `write-many`. `None` means "no base known"; the
        /// publisher then relies on the `Absent` predicate of a folded creation
        /// or on identity alone.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_content_hash: Option<String>,
    },
    CreateSymlink {
        path: String,
        target: String,
    },
    CreateHardLink {
        existing_path: String,
        new_path: String,
    },
    /// `flags` carries only `RENAME_NOREPLACE`. It is enforced locally by
    /// `renameat2` and dropped by the publisher, because the gateway rename has
    /// no flag field and single ownership makes the local enforcement
    /// authoritative. `RENAME_EXCHANGE` and `RENAME_WHITEOUT` are rejected at
    /// the FUSE boundary with `EINVAL` -- they have no atomic remote form and
    /// decomposing them would publish a namespace state the mount never had.
    Rename {
        old_path: String,
        new_path: String,
        flags: u32,
    },
    RemoveFile {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_file_id: Option<String>,
    },
    RemoveDirectory {
        path: String,
    },
    SetMode {
        path: String,
        mode: u32,
    },
    SetTimes {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        atime: Option<LocalTimestamp>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mtime: Option<LocalTimestamp>,
    },
    SetOwner {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        uid: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gid: Option<u32>,
    },
}

impl MountMutation {
    pub(crate) fn validate(&self) -> Result<()> {
        for path in self.affected_paths() {
            validate_mount_path(path)?;
        }
        if let Self::Rename { flags, .. } = self {
            if *flags & !RENAME_NOREPLACE != 0 {
                bail!("unsupported rename flags {flags:#x} in a mount mutation");
            }
        }
        Ok(())
    }

    /// Only a content generation carries payload bytes.
    pub(crate) fn requires_payload(&self) -> bool {
        matches!(self, Self::ReplaceFile { .. })
    }

    /// True when the gateway contract cannot express this mutation. The
    /// publisher acknowledges these without a request.
    pub(crate) fn is_local_only(&self) -> bool {
        if matches!(self, Self::SetTimes { .. } | Self::SetOwner { .. }) {
            return true;
        }
        #[cfg(target_os = "macos")]
        {
            return self
                .affected_paths()
                .into_iter()
                .all(is_local_only_content_path);
        }
        #[cfg(not(target_os = "macos"))]
        false
    }

    /// True when the publisher routes this through `write-many` /
    /// `write_staged_file` rather than `namespace-many`.
    pub(crate) fn is_content(&self) -> bool {
        matches!(self, Self::ReplaceFile { .. })
    }

    /// The path the mutation is primarily about; for a rename it is the
    /// destination, which is the name that exists afterwards.
    pub(crate) fn primary_path(&self) -> &str {
        match self {
            Self::CreateDirectory { path, .. }
            | Self::CreateFile { path, .. }
            | Self::ReplaceFile { path, .. }
            | Self::CreateSymlink { path, .. }
            | Self::RemoveFile { path, .. }
            | Self::RemoveDirectory { path }
            | Self::SetMode { path, .. }
            | Self::SetTimes { path, .. }
            | Self::SetOwner { path, .. } => path,
            Self::CreateHardLink { new_path, .. } => new_path,
            Self::Rename { new_path, .. } => new_path,
        }
    }

    /// Every path the mutation touches, in the order the pre-image records them.
    pub(crate) fn affected_paths(&self) -> Vec<&str> {
        match self {
            Self::CreateDirectory { path, .. }
            | Self::CreateFile { path, .. }
            | Self::ReplaceFile { path, .. }
            | Self::CreateSymlink { path, .. }
            | Self::RemoveFile { path, .. }
            | Self::RemoveDirectory { path }
            | Self::SetMode { path, .. }
            | Self::SetTimes { path, .. }
            | Self::SetOwner { path, .. } => vec![path.as_str()],
            Self::CreateHardLink {
                existing_path,
                new_path,
            } => vec![existing_path.as_str(), new_path.as_str()],
            Self::Rename {
                old_path, new_path, ..
            } => vec![old_path.as_str(), new_path.as_str()],
        }
    }

    /// The dependency keys this mutation must hold, deduplicated and sorted so
    /// concurrent callbacks acquire them in one global order and cannot deadlock.
    ///
    /// Rules:
    /// * every affected path takes an exclusive key on itself, except a hard
    ///   link's source, which only needs to be prevented from disappearing;
    /// * every *proper ancestor* of an affected path takes a shared key. That is
    ///   what makes `rename("a")` mutually exclusive with `create("a/b/c")`:
    ///   the create holds shared `a` and shared `a/b`, the rename holds
    ///   exclusive `a`.
    ///
    /// The publisher derives independence from the same keys, so ordering on the
    /// wire matches ordering in the callbacks by construction.
    pub(crate) fn dependency_keys(&self) -> Vec<DependencyKey> {
        let mut keys: Vec<DependencyKey> = Vec::new();
        let mut push = |path: &str, exclusive: bool| {
            for ancestor in ancestors_of(path) {
                keys.push(DependencyKey {
                    path: ancestor,
                    exclusive: false,
                });
            }
            keys.push(DependencyKey {
                path: path.to_string(),
                exclusive,
            });
        };
        match self {
            Self::CreateHardLink {
                existing_path,
                new_path,
            } => {
                push(existing_path, false);
                push(new_path, true);
            }
            other => {
                for path in other.affected_paths() {
                    push(path, true);
                }
            }
        }
        merge_dependency_keys(keys)
    }
}

pub(crate) fn is_local_only_content_path(path: &str) -> bool {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = path;
        return false;
    }
    #[cfg(target_os = "macos")]
    {
        let root = path.split('/').next().unwrap_or(path);
        matches!(
            root,
            ".fseventsd"
                | ".Spotlight-V100"
                | ".Trashes"
                | ".DocumentRevisions-V100"
                | ".TemporaryItems"
        )
    }
}

/// `RENAME_NOREPLACE` as the kernel defines it. Declared here rather than taken
/// from `libc` so the WAL record shape is identical on every host.
pub(crate) const RENAME_NOREPLACE: u32 = 1;
pub(crate) const RENAME_EXCHANGE: u32 = 2;
pub(crate) const RENAME_WHITEOUT: u32 = 4;

/// One path-scoped ordering claim. `exclusive` claims conflict with everything
/// on the same path; shared claims conflict only with exclusive ones.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DependencyKey {
    pub(crate) path: String,
    pub(crate) exclusive: bool,
}

impl DependencyKey {
    pub(crate) fn conflicts_with(&self, other: &Self) -> bool {
        self.path == other.path && (self.exclusive || other.exclusive)
    }
}

/// True when two key sets may not be reordered relative to one another.
pub(crate) fn dependency_sets_conflict(left: &[DependencyKey], right: &[DependencyKey]) -> bool {
    left.iter()
        .any(|key| right.iter().any(|other| key.conflicts_with(other)))
}

/// Sort, deduplicate, and let an exclusive claim absorb a shared one.
pub(crate) fn merge_dependency_keys(mut keys: Vec<DependencyKey>) -> Vec<DependencyKey> {
    keys.sort();
    let mut merged: Vec<DependencyKey> = Vec::with_capacity(keys.len());
    for key in keys {
        match merged.last_mut() {
            Some(last) if last.path == key.path => {
                last.exclusive |= key.exclusive;
            }
            _ => merged.push(key),
        }
    }
    merged
}

/// Every proper ancestor of a mount-relative path, root first. The mount root is
/// the empty string, so `"a/b/c"` yields `["", "a", "a/b"]`.
pub(crate) fn ancestors_of(path: &str) -> Vec<String> {
    let mut ancestors = vec![String::new()];
    let mut current = String::new();
    let mut components = path.split('/').filter(|part| !part.is_empty()).peekable();
    while let Some(part) = components.next() {
        if components.peek().is_none() {
            break;
        }
        if !current.is_empty() {
            current.push('/');
        }
        current.push_str(part);
        ancestors.push(current.clone());
    }
    ancestors
}

/// The mount-relative parent of a path; the mount root is the empty string.
pub(crate) fn parent_of(path: &str) -> String {
    match path.rfind('/') {
        Some(index) => path[..index].to_string(),
        None => String::new(),
    }
}

/// Reject anything that could escape the backing tree or that the gateway would
/// interpret differently from the local filesystem.
pub(crate) fn validate_mount_path(path: &str) -> Result<()> {
    if path.is_empty() {
        bail!("mount mutation path must not be empty");
    }
    let candidate = Path::new(path);
    if candidate.is_absolute()
        || candidate.components().any(|component| {
            matches!(
                component,
                Component::ParentDir
                    | Component::CurDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
    {
        bail!("mount mutation path must be normalized and relative: {path:?}");
    }
    Ok(())
}

/// A payload filename must be a single component inside the payload directory.
pub(crate) fn validate_payload_name(name: &str) -> Result<()> {
    let candidate = Path::new(name);
    if name.is_empty()
        || candidate.components().count() != 1
        || !matches!(candidate.components().next(), Some(Component::Normal(_)))
    {
        bail!("invalid mount payload filename {name:?}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Immutable payloads
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "storage", rename_all = "snake_case")]
pub(crate) enum PayloadStorage {
    Segment { file: String, offset: u64 },
    DedicatedFile { file: String },
}

impl PayloadStorage {
    pub(crate) fn file(&self) -> &str {
        match self {
            Self::Segment { file, .. } | Self::DedicatedFile { file } => file,
        }
    }

    pub(crate) fn offset(&self) -> u64 {
        match self {
            Self::Segment { offset, .. } => *offset,
            Self::DedicatedFile { .. } => 0,
        }
    }

    pub(crate) fn is_dedicated(&self) -> bool {
        matches!(self, Self::DedicatedFile { .. })
    }
}

/// An immutable, content-addressed reference into the payload store. A mutable
/// backing pathname is never a valid payload reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PayloadRef {
    pub(crate) storage: PayloadStorage,
    pub(crate) length: u64,
    /// The active `chevalier_vfs_hash` algorithm name at capture time. Recovery
    /// re-checks it, so a host that flips `CHEVALIER_VFS_HASH_ALGORITHM` fails
    /// closed instead of publishing a hash the gateway will reject.
    pub(crate) hash_algorithm: String,
    pub(crate) content_hash: String,
}

/// Where a payload's bytes come from when a mutation is prepared.
pub(crate) enum PayloadSource<'a> {
    /// The mutation carries no content.
    None,
    /// Small generation already resident in the callback's buffer.
    Bytes(&'a [u8]),
    /// Capture an immutable snapshot of this backing file, preferring a reflink.
    File(&'a Path),
}

impl PayloadSource<'_> {
    pub(crate) fn is_some(&self) -> bool {
        !matches!(self, Self::None)
    }
}

// ---------------------------------------------------------------------------
// Pre-image witness
// ---------------------------------------------------------------------------

/// The observed state of one affected path immediately before a mutation is
/// applied.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum PreImageState {
    Absent,
    Present {
        kind: LocalKind,
        mode: u32,
        /// `dev:ino` of the backing entry -- this mount's stable file identity.
        local_identity: String,
        size_bytes: u64,
        link_count: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        link_target: Option<String>,
    },
}

impl PreImageState {
    pub(crate) fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }

    pub(crate) fn local_identity(&self) -> Option<&str> {
        match self {
            Self::Absent => None,
            Self::Present { local_identity, .. } => Some(local_identity.as_str()),
        }
    }

    pub(crate) fn kind(&self) -> Option<LocalKind> {
        match self {
            Self::Absent => None,
            Self::Present { kind, .. } => Some(*kind),
        }
    }

    pub(crate) fn mode(&self) -> Option<u32> {
        match self {
            Self::Absent => None,
            Self::Present { mode, .. } => Some(*mode),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PreImageEntry {
    pub(crate) path: String,
    pub(crate) state: PreImageState,
}

/// The witness recovery uses to decide whether an unresolved prepare was applied.
///
/// It is *not* a general undo log: most mutations are unwound by inspecting the
/// tree against this witness, and the single-atomic-syscall rule means "half
/// applied" is unreachable. Where an inverse is available and needed, it is
/// derived from these entries.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MountPreImage {
    #[serde(default)]
    pub(crate) entries: Vec<PreImageEntry>,
}

impl MountPreImage {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn state_of(&self, path: &str) -> Option<&PreImageState> {
        self.entries
            .iter()
            .find(|entry| entry.path == path)
            .map(|entry| &entry.state)
    }
}

/// Whether an unresolved prepare's effect is visible in the backing tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApplyState {
    /// The tree still matches the pre-image: the mutation never happened.
    NotApplied,
    /// The tree matches the mutation's postcondition: the mutation completed.
    Applied,
    /// Neither. The single-atomic-syscall rule makes this unreachable in
    /// practice, so it is treated as corruption and fails closed.
    Ambiguous,
}

/// How recovery resolved one unresolved prepare.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryResolution {
    /// Never applied -- append `Aborted`; the event is never published.
    Abort,
    /// Applied and therefore accepted local state -- append `Committed` so the
    /// publisher converges the replica onto what the guest already saw.
    Commit,
}

// ---------------------------------------------------------------------------
// Log records
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MountEvent {
    pub(crate) format_version: u32,
    pub(crate) epoch: String,
    pub(crate) sequence: u64,
    /// Stable across restart and retry: `{epoch}:{sequence}`. It is the
    /// `operation_id` the publisher sends and the key a post-rejection
    /// reconciliation is reasoned about with, because the gateway does not
    /// deduplicate operation ids across requests.
    pub(crate) idempotency_key: String,
    pub(crate) mutation: MountMutation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) payload: Option<PayloadRef>,
    #[serde(default)]
    pub(crate) pre_image: MountPreImage,
    /// `dev:ino` of the entry this mutation produced, filled in at commit. Used
    /// for hard-link and rename dependency tracking.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) local_identity: Option<String>,
}

impl MountEvent {
    pub(crate) fn payload_length(&self) -> u64 {
        self.payload
            .as_ref()
            .map(|payload| payload.length)
            .unwrap_or(0)
    }

    pub(crate) fn dependency_keys(&self) -> Vec<DependencyKey> {
        self.mutation.dependency_keys()
    }
}

/// The complete on-disk record vocabulary. One JSON object per line.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub(crate) enum WalRecord {
    /// First line of every log generation. Recovery uses it to bind a generation
    /// to its epoch and to the first sequence it may contain.
    LogHeader {
        format_version: u32,
        epoch: String,
        generation: u64,
        first_sequence: u64,
    },
    Prepared {
        event: MountEvent,
    },
    Committed {
        epoch: String,
        sequence: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        local_identity: Option<String>,
    },
    Aborted {
        epoch: String,
        sequence: u64,
        reason: String,
    },
    /// A path whose backing content diverged from its last published generation.
    /// Recovery re-seals every still-dirty path, so bytes the guest already read
    /// back are never lost even when the crash landed mid-generation.
    ContentDirty {
        epoch: String,
        path: String,
    },
    RemoteAcknowledged {
        epoch: String,
        through_sequence: u64,
        remote_revision: u64,
    },
    /// In-band marker that a checkpoint covering this prefix was made durable.
    /// Purely informational for forensics; the checkpoint file is authoritative.
    Checkpointed {
        epoch: String,
        through_sequence: u64,
        tree_generation: u64,
    },
}

/// The durable anchor recovery starts from. Everything needed to skip replaying
/// the whole lifetime of the mount and to know exactly which payload segments
/// and log generations are still reachable.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MountCheckpoint {
    pub(crate) format_version: u32,
    pub(crate) epoch: String,
    /// Monotonic id so a torn pair of checkpoint files resolves deterministically.
    pub(crate) checkpoint_id: u64,
    pub(crate) acknowledged_sequence: u64,
    pub(crate) remote_revision: u64,
    /// The backing tree's mutation counter at the replay point. Recovery
    /// compares it after replay as a consistency witness.
    pub(crate) tree_generation: u64,
    /// Sequences below this are pruned; replay must not demand records for them.
    pub(crate) first_unpruned_sequence: u64,
    /// Where replay starts. Both are inside `active_log_generations`.
    pub(crate) replay_log_generation: u64,
    pub(crate) replay_log_offset: u64,
    pub(crate) active_log_generations: Vec<u64>,
    /// Payload files still referenced by an unacknowledged event.
    pub(crate) active_segments: Vec<String>,
    /// Paths whose backing content had not been sealed when the checkpoint was
    /// taken. Recovery re-seals them.
    #[serde(default)]
    pub(crate) dirty_content_paths: Vec<String>,
}

impl MountCheckpoint {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.format_version != WAL_FORMAT_VERSION {
            bail!(
                "unsupported mount checkpoint format {} (expected {WAL_FORMAT_VERSION})",
                self.format_version
            );
        }
        if self.epoch.is_empty() {
            bail!("mount checkpoint has no ownership epoch");
        }
        if !self
            .active_log_generations
            .contains(&self.replay_log_generation)
        {
            bail!(
                "mount checkpoint replay generation {} is not in its active set",
                self.replay_log_generation
            );
        }
        if self.first_unpruned_sequence > self.acknowledged_sequence.saturating_add(1) {
            bail!(
                "mount checkpoint pruned sequence {} past its acknowledged cursor {}",
                self.first_unpruned_sequence,
                self.acknowledged_sequence
            );
        }
        Ok(())
    }
}

/// Durable proof that exactly one process owns this state directory. Held open
/// with an exclusive advisory lock for the mount's lifetime, so exclusivity is
/// enforced once at mount scope instead of being rebuilt per callback.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MountOwnerRecord {
    pub(crate) format_version: u32,
    pub(crate) epoch: String,
    pub(crate) scope_path: String,
    pub(crate) endpoint: String,
    pub(crate) mount_tag: String,
    pub(crate) created_at: String,
}

// ---------------------------------------------------------------------------
// Handles the WAL and the publisher exchange
// ---------------------------------------------------------------------------

/// The token a callback hands back to `commit` or `abort`. Holding it proves the
/// intent and its payload are already in the log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedEvent {
    pub(crate) event: MountEvent,
}

impl PreparedEvent {
    pub(crate) fn sequence(&self) -> u64 {
        self.event.sequence
    }
}

/// A contiguous committed prefix the publisher may reconcile.
///
/// `events` may omit an older whole-file generation when a later generation of
/// the same path supersedes it without an intervening conflicting mutation.
/// `through_sequence` still covers the omitted event: once the newer generation
/// lands, acknowledging both is safe because the gateway holds the same final
/// path state the ordered pair would have produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublishBatch {
    pub(crate) events: Vec<MountEvent>,
    /// May exceed the last event's sequence: aborted sequences inside the prefix
    /// are folded in because they will never be published.
    pub(crate) through_sequence: u64,
}

/// One issuable unit inside a batch. Runs are dependency-ordered rather than
/// necessarily WAL-ordered: unrelated paths may be grouped across route-class
/// boundaries. Namespace events retain WAL order within their request; content
/// events in one run are mutually independent so their uploads may proceed
/// concurrently. The WAL cursor advances only after every run succeeds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublishRun {
    /// Ordered `namespace-many` mutations.
    Namespace {
        events: Vec<MountEvent>,
        through_sequence: u64,
    },
    /// Independent content generations; large payloads stream individually.
    Content {
        events: Vec<MountEvent>,
        through_sequence: u64,
    },
    /// Local-only events (`SetTimes`, `SetOwner`) plus aborted gaps: the cursor
    /// advances with no gateway request at all.
    Cursor { through_sequence: u64 },
}

/// What the WAL knows immediately after `open`, before the backing tree has been
/// consulted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryState {
    pub(crate) epoch: String,
    pub(crate) acknowledged_sequence: u64,
    pub(crate) remote_revision: u64,
    pub(crate) tree_generation: u64,
    /// Committed but not yet acknowledged -- the publisher resumes from these.
    pub(crate) committed_unacknowledged: Vec<MountEvent>,
    /// Prepared with no terminal record. `mount.rs` resolves each one against
    /// the backing tree before the mount serves a single callback.
    pub(crate) unresolved_prepares: Vec<MountEvent>,
    /// Paths whose content must be re-sealed from the backing tree.
    pub(crate) dirty_content_paths: Vec<String>,
}

/// Summary of what recovery did, logged once at mount time.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RecoverySummary {
    pub(crate) replayed_events: u64,
    pub(crate) committed_unacknowledged: u64,
    pub(crate) resolved_committed: u64,
    pub(crate) resolved_aborted: u64,
    pub(crate) resealed_paths: u64,
    pub(crate) acknowledged_sequence: u64,
    pub(crate) remote_revision: u64,
}

/// Result of compaction, for logging and for the storage-pressure calculation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CompactionOutcome {
    pub(crate) removed_log_generations: u64,
    pub(crate) removed_payload_files: u64,
    pub(crate) reclaimed_bytes: u64,
}

/// Local durable-storage pressure. Derived from unreclaimed WAL + payload bytes
/// and from the backing filesystem's free space; never from gateway latency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StoragePressure {
    None,
    /// Widen publication batches, log, keep serving every mutation.
    Soft,
    /// New content-bearing mutations fail with `ENOSPC`. Namespace mutations are
    /// still admitted: they are tiny and they are what lets the backlog drain.
    Hard,
}

impl StoragePressure {
    pub(crate) fn blocks_content(self) -> bool {
        matches!(self, Self::Hard)
    }
}

/// Publisher liveness as the mount reports it. A currently unrepaired event is
/// surfaced here and remains durable; it is never dead-lettered, because the
/// local accepted view is authoritative. Replica repair retries in place.
#[derive(Clone, Debug, Default)]
pub(crate) struct PublicationHealth {
    pub(crate) acknowledged_sequence: u64,
    pub(crate) last_committed_sequence: u64,
    pub(crate) pending_events: u64,
    pub(crate) pending_payload_bytes: u64,
    pub(crate) blocked_sequence: Option<u64>,
    pub(crate) blocked_reason: Option<String>,
    pub(crate) last_error: Option<String>,
    pub(crate) storage_pressure_soft: bool,
    pub(crate) storage_pressure_hard: bool,
}

impl PublicationHealth {
    pub(crate) fn is_blocked(&self) -> bool {
        self.blocked_sequence.is_some()
    }
}

/// Outcome of an explicit lifecycle drain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DrainOutcome {
    /// Every committed event is remotely acknowledged.
    Drained { through_sequence: u64 },
    /// The deadline expired with work outstanding. The WAL is intact and durable;
    /// the caller decides whether that is fatal for its lifecycle transition.
    TimedOut {
        acknowledged_sequence: u64,
        pending_events: u64,
    },
    /// A rejected event has not yet been repaired before the lifecycle deadline.
    /// It remains durable and the background reconciler continues retrying.
    Blocked {
        blocked_sequence: u64,
        reason: String,
    },
}

impl DrainOutcome {
    pub(crate) fn is_drained(&self) -> bool {
        matches!(self, Self::Drained { .. })
    }
}

// ---------------------------------------------------------------------------
// Projections `fs.rs` turns into FUSE replies
// ---------------------------------------------------------------------------

/// A backing-tree `lstat`, projected once so no callback deals in raw `libc`
/// stat buffers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalMetadata {
    pub(crate) kind: LocalKind,
    pub(crate) size_bytes: u64,
    pub(crate) blocks: u64,
    pub(crate) mode: u32,
    pub(crate) uid: u32,
    pub(crate) gid: u32,
    pub(crate) link_count: u64,
    /// `dev:ino` of the backing entry. Replaces the gateway `file_id` as this
    /// mount's hard-link identity, and is available the moment the entry exists
    /// rather than after a publication.
    pub(crate) local_identity: String,
    pub(crate) backing_ino: u64,
    pub(crate) link_target: Option<String>,
    pub(crate) atime: LocalTimestamp,
    pub(crate) mtime: LocalTimestamp,
    pub(crate) ctime: LocalTimestamp,
}

impl LocalMetadata {
    pub(crate) fn is_dir(&self) -> bool {
        matches!(self.kind, LocalKind::Directory)
    }

    pub(crate) fn is_file(&self) -> bool {
        matches!(self.kind, LocalKind::File)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalDirEntry {
    pub(crate) name: String,
    pub(crate) metadata: LocalMetadata,
}

/// `statvfs` of the filesystem holding the backing tree. This is what makes the
/// guest's `ENOSPC` honest: local disk exhaustion is a real filesystem answer,
/// while a gateway outage never surfaces as one.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LocalStatfs {
    pub(crate) blocks: u64,
    pub(crate) blocks_free: u64,
    pub(crate) blocks_available: u64,
    pub(crate) files: u64,
    pub(crate) files_free: u64,
    pub(crate) block_size: u32,
    pub(crate) fragment_size: u32,
    pub(crate) max_name_length: u32,
}

/// What `BackingTree::apply` reports back so the commit record can bind identity
/// and the checkpoint can witness the tree generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AppliedMutation {
    pub(crate) tree_generation: u64,
    pub(crate) local_identity: Option<String>,
    pub(crate) metadata: Option<LocalMetadata>,
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::MountMutation;

    #[test]
    fn macos_volume_metadata_stays_in_the_guest_state_tree() {
        assert!(
            MountMutation::CreateFile {
                path: ".fseventsd/fseventsd-uuid".to_string(),
                mode: 0o600,
            }
            .is_local_only()
        );
        assert!(
            MountMutation::RemoveDirectory {
                path: ".Trashes/501".to_string(),
            }
            .is_local_only()
        );
        assert!(
            !MountMutation::Rename {
                old_path: ".TemporaryItems/work".to_string(),
                new_path: "project/work".to_string(),
                flags: 0,
            }
            .is_local_only()
        );
        assert!(
            !MountMutation::CreateFile {
                path: "project/.fseventsd".to_string(),
                mode: 0o600,
            }
            .is_local_only()
        );
    }
}
