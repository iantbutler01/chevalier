//! The mount-local materialized tree.
//!
//! This is the immediate read and write view. Lookups, directory membership,
//! rename atomicity, hard-link identity, modes and page-cache behaviour come
//! from a real local filesystem instead of being rebuilt by scanning journal
//! records. Nothing in this file consults the WAL, the publisher or the gateway.
//!
//! **The single-atomic-syscall rule.** Every [`MountMutation`] applies with
//! exactly one syscall on the backing tree. That is what makes recovery total:
//! an unresolved prepare either did not happen or happened completely, so
//! [`BackingTree::classify`] can always decide between the two by comparing the
//! recorded pre-image with what the tree holds now. A mutation that needed two
//! syscalls would create a third, undecidable state. If a future operation
//! cannot be applied atomically, it must be decomposed into several sequenced
//! mutations rather than applied as one.
//!
//! Platform note: `renameat2` and `FICLONE` are Linux-only. Non-Linux builds
//! exist so the crate type-checks on developer machines; they may fall back to
//! `std::fs::rename` and a streaming copy, and must reject `RENAME_NOREPLACE`
//! rather than silently ignoring it.

use std::ffi::CString;
use std::fs::File;
use std::io;
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow, bail};

use super::sync_directory;
use super::types::{
    AppliedMutation, ApplyState, LocalDirEntry, LocalKind, LocalMetadata, LocalStatfs,
    LocalTimestamp, MountEvent, MountMutation, MountPreImage, PreImageEntry, PreImageState,
    RENAME_NOREPLACE, validate_mount_path,
};

/// Buffer size for the portable `copy_range_from` fallback.
const COPY_CHUNK_BYTES: usize = 1024 * 1024;

/// Name of the data-only sync this platform provides, for error context.
#[cfg(target_os = "linux")]
const SYNC_DATA_CALL: &str = "fdatasync";
#[cfg(not(target_os = "linux"))]
const SYNC_DATA_CALL: &str = "fsync";

/// The backing tree root, plus the monotonic generation counter that witnesses
/// how many mutations have been applied to it.
#[derive(Debug)]
pub(crate) struct BackingTree {
    /// Canonicalized, so a symlinked state directory cannot make a validated
    /// relative path resolve outside the tree.
    root: PathBuf,
    /// There is deliberately no interior lock: ordering between concurrent
    /// mutations is the caller's dependency-key discipline, and the local
    /// filesystem provides atomicity for each individual apply.
    generation: AtomicU64,
}

impl BackingTree {
    /// Open (creating if needed) the backing tree root and fsync its parent.
    pub(crate) fn open(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root)
            .with_context(|| format!("create backing tree root {}", root.display()))?;
        if let Some(parent) = root.parent() {
            if parent.as_os_str().is_empty() {
                sync_directory(root)?;
            } else {
                sync_directory(parent)?;
            }
        } else {
            sync_directory(root)?;
        }
        let root = root
            .canonicalize()
            .with_context(|| format!("canonicalize backing tree root {}", root.display()))?;
        Ok(Self {
            root,
            generation: AtomicU64::new(0),
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Monotonic count of applied mutations. Recorded in the checkpoint as a
    /// consistency witness and compared after replay.
    pub(crate) fn tree_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Restore the generation counter after a checkpoint-anchored replay.
    pub(crate) fn set_tree_generation(&self, generation: u64) {
        self.generation.store(generation, Ordering::SeqCst);
    }

    /// Join a validated mount-relative path onto the root. Rejects absolute
    /// paths, `.`, `..` and any prefix component, so nothing can escape the tree.
    ///
    /// The empty string is the mount root itself. No mutation can ever name it
    /// -- [`MountMutation::validate`] rejects an empty path -- but every read
    /// (`statfs`, the root `getattr`, `readdir` of the root) needs it.
    pub(crate) fn resolve(&self, path: &str) -> Result<PathBuf> {
        if path.is_empty() {
            return Ok(self.root.clone());
        }
        validate_mount_path(path)?;
        Ok(self.root.join(path))
    }

    // -- reads (never touch the WAL) ----------------------------------------

    /// `lstat` one entry. `None` is a genuine local `ENOENT`, which is what makes
    /// a negative lookup free.
    pub(crate) fn lstat(&self, path: &str) -> Result<Option<LocalMetadata>> {
        let backing = self.resolve(path)?;
        lstat_backing(&backing)
    }

    /// `fstat` an open handle.
    pub(crate) fn fstat(&self, file: &MountFile) -> Result<LocalMetadata> {
        file.metadata()
    }

    /// One directory listing with per-entry metadata. Backs both `readdir` and
    /// `readdirplus`; the caller applies its own offset cursor.
    ///
    /// Entries are returned sorted by name so a kernel round at a non-zero
    /// offset addresses the same child it did on the previous round; raw
    /// `readdir(3)` order carries no such guarantee.
    pub(crate) fn read_dir(&self, path: &str) -> Result<Option<Vec<LocalDirEntry>>> {
        let backing = self.resolve(path)?;
        let reader = match std::fs::read_dir(&backing) {
            Ok(reader) => reader,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(anyhow::Error::new(error)
                    .context(format!("read backing directory {}", backing.display())));
            }
        };
        let mut entries: Vec<LocalDirEntry> = Vec::new();
        for entry in reader {
            let entry = entry.with_context(|| {
                format!("read backing directory entry under {}", backing.display())
            })?;
            let raw_name = entry.file_name();
            let Some(name) = raw_name.to_str().map(str::to_string) else {
                // Nothing in this mount can create such a name: every mutation
                // path is a validated `&str`. Skip it rather than fail a whole
                // listing, and say so loudly.
                tracing::warn!(
                    directory = %backing.display(),
                    name = %raw_name.to_string_lossy(),
                    "skipping non-UTF-8 backing tree entry"
                );
                continue;
            };
            let child = backing.join(&name);
            let Some(metadata) = lstat_backing(&child)? else {
                // Raced with a concurrent unlink; the entry is simply not there.
                continue;
            };
            entries.push(LocalDirEntry { name, metadata });
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(Some(entries))
    }

    pub(crate) fn read_link(&self, path: &str) -> Result<Option<String>> {
        let backing = self.resolve(path)?;
        read_link_backing(&backing)
    }

    /// `statvfs` of the filesystem holding the tree. This is the honest source
    /// for the guest's free space, and the reason local exhaustion surfaces as
    /// `ENOSPC` while a gateway outage never does.
    pub(crate) fn statfs(&self) -> Result<LocalStatfs> {
        let backing = c_path(&self.root)?;
        let mut buffer = MaybeUninit::<libc::statvfs>::uninit();
        let result = unsafe { libc::statvfs(backing.as_ptr(), buffer.as_mut_ptr()) };
        if result != 0 {
            return Err(syscall_failed("statvfs", &self.root));
        }
        let stat = unsafe { buffer.assume_init() };
        Ok(LocalStatfs {
            blocks: stat.f_blocks as u64,
            blocks_free: stat.f_bfree as u64,
            blocks_available: stat.f_bavail as u64,
            files: stat.f_files as u64,
            files_free: stat.f_ffree as u64,
            block_size: stat.f_bsize as u32,
            fragment_size: if stat.f_frsize == 0 {
                stat.f_bsize as u32
            } else {
                stat.f_frsize as u32
            },
            max_name_length: stat.f_namemax as u32,
        })
    }

    /// Observe the current state of the paths a mutation will touch. This is the
    /// pre-image witness recorded in the WAL before the tree is changed.
    pub(crate) fn observe(&self, paths: &[&str]) -> Result<MountPreImage> {
        let mut entries = Vec::with_capacity(paths.len());
        for path in paths {
            entries.push(PreImageEntry {
                path: (*path).to_string(),
                state: self.observe_one(path)?,
            });
        }
        Ok(MountPreImage { entries })
    }

    fn observe_one(&self, path: &str) -> Result<PreImageState> {
        Ok(match self.lstat(path)? {
            None => PreImageState::Absent,
            Some(metadata) => PreImageState::Present {
                kind: metadata.kind,
                mode: metadata.mode,
                local_identity: metadata.local_identity,
                size_bytes: metadata.size_bytes,
                link_count: metadata.link_count,
                link_target: metadata.link_target,
            },
        })
    }

    // -- open files ----------------------------------------------------------

    /// Open an existing backing file. The returned handle is what `read`,
    /// `write`, `fsync` and `setattr` operate on; there is no in-memory
    /// whole-file buffer anywhere in the design.
    pub(crate) fn open_file(&self, path: &str, flags: i32) -> Result<MountFile> {
        let backing = self.resolve(path)?;
        let descriptor = openat_backing(&backing, open_flags(flags), 0)?;
        Ok(MountFile::from_descriptor(path, backing, descriptor))
    }

    /// Open a backing directory so `fsyncdir` can sync it.
    pub(crate) fn open_dir(&self, path: &str) -> Result<MountFile> {
        let backing = self.resolve(path)?;
        let descriptor = openat_backing(
            &backing,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            0,
        )?;
        Ok(MountFile::from_descriptor(path, backing, descriptor))
    }

    // -- mutation ------------------------------------------------------------

    /// Apply exactly one mutation with exactly one syscall.
    ///
    /// * `CreateDirectory` -> `mkdirat(mode)`
    /// * `CreateFile` -> `openat(O_CREAT | O_EXCL, mode)` (the returned handle is
    ///   surfaced through [`Self::apply_create_file`] instead, so `create(2)` does
    ///   not open the path twice)
    /// * `CreateSymlink` -> `symlinkat`
    /// * `CreateHardLink` -> `linkat`
    /// * `Rename` -> `renameat2` with the validated flags
    /// * `RemoveFile` -> `unlinkat`
    /// * `RemoveDirectory` -> `unlinkat(AT_REMOVEDIR)` (local `ENOTEMPTY`)
    /// * `SetMode` -> `fchmodat`
    /// * `SetTimes` -> `utimensat`
    /// * `SetOwner` -> `fchownat(AT_SYMLINK_NOFOLLOW)`
    /// * `ReplaceFile` -> no syscall at all; the bytes are already in the backing
    ///   file and the mutation only records the sealed generation
    ///
    /// Bumps and returns the generation counter, and reports the resulting
    /// entry's `dev:ino` so the commit record can bind identity.
    pub(crate) fn apply(&self, mutation: &MountMutation) -> Result<AppliedMutation> {
        mutation.validate()?;
        match mutation {
            MountMutation::CreateDirectory { path, mode } => {
                let backing = self.resolve(path)?;
                let target = c_path(&backing)?;
                let result = unsafe {
                    libc::mkdirat(
                        libc::AT_FDCWD,
                        target.as_ptr(),
                        permission_bits(*mode) as libc::mode_t,
                    )
                };
                if result != 0 {
                    return Err(syscall_failed("mkdirat", &backing));
                }
                self.applied(path)
            }
            MountMutation::CreateFile { path, mode } => {
                // The handle is only interesting to `create(2)`; every other
                // caller just wants the name to exist.
                let (_file, applied) = self.apply_create_file(path, *mode, libc::O_WRONLY)?;
                Ok(applied)
            }
            MountMutation::ReplaceFile { path, .. } => {
                // No syscall: the backing file already holds the bytes, and the
                // payload was captured as an immutable snapshot before the
                // record was prepared. The mutation only names a generation.
                self.applied(path)
            }
            MountMutation::CreateSymlink { path, target } => {
                let backing = self.resolve(path)?;
                let link = c_path(&backing)?;
                let destination = CString::new(target.as_bytes()).with_context(|| {
                    format!("symlink target {target:?} contains an interior NUL")
                })?;
                let result =
                    unsafe { libc::symlinkat(destination.as_ptr(), libc::AT_FDCWD, link.as_ptr()) };
                if result != 0 {
                    return Err(syscall_failed("symlinkat", &backing));
                }
                self.applied(path)
            }
            MountMutation::CreateHardLink {
                existing_path,
                new_path,
            } => {
                let existing = self.resolve(existing_path)?;
                let created = self.resolve(new_path)?;
                let source = c_path(&existing)?;
                let destination = c_path(&created)?;
                let result = unsafe {
                    libc::linkat(
                        libc::AT_FDCWD,
                        source.as_ptr(),
                        libc::AT_FDCWD,
                        destination.as_ptr(),
                        0,
                    )
                };
                if result != 0 {
                    return Err(syscall_failed("linkat", &created));
                }
                self.applied(new_path)
            }
            MountMutation::Rename {
                old_path,
                new_path,
                flags,
            } => {
                let source = self.resolve(old_path)?;
                let destination = self.resolve(new_path)?;
                rename_backing(&source, &destination, *flags)?;
                self.applied(new_path)
            }
            MountMutation::RemoveFile { path, .. } => {
                let backing = self.resolve(path)?;
                let target = c_path(&backing)?;
                let result = unsafe { libc::unlinkat(libc::AT_FDCWD, target.as_ptr(), 0) };
                if result != 0 {
                    return Err(syscall_failed("unlinkat", &backing));
                }
                Ok(self.applied_removal())
            }
            MountMutation::RemoveDirectory { path } => {
                let backing = self.resolve(path)?;
                let target = c_path(&backing)?;
                // `AT_REMOVEDIR` is what makes `ENOTEMPTY` a local answer.
                let result =
                    unsafe { libc::unlinkat(libc::AT_FDCWD, target.as_ptr(), libc::AT_REMOVEDIR) };
                if result != 0 {
                    return Err(syscall_failed("unlinkat(AT_REMOVEDIR)", &backing));
                }
                Ok(self.applied_removal())
            }
            MountMutation::SetMode { path, mode } => {
                let backing = self.resolve(path)?;
                self.chmod_backing(&backing, *mode)?;
                self.applied(path)
            }
            MountMutation::SetTimes { path, atime, mtime } => {
                let backing = self.resolve(path)?;
                let target = c_path(&backing)?;
                let times = [timespec_for(*atime), timespec_for(*mtime)];
                let result = unsafe {
                    libc::utimensat(
                        libc::AT_FDCWD,
                        target.as_ptr(),
                        times.as_ptr(),
                        libc::AT_SYMLINK_NOFOLLOW,
                    )
                };
                if result != 0 {
                    return Err(syscall_failed("utimensat", &backing));
                }
                self.applied(path)
            }
            MountMutation::SetOwner { path, uid, gid } => {
                let backing = self.resolve(path)?;
                let target = c_path(&backing)?;
                let result = unsafe {
                    libc::fchownat(
                        libc::AT_FDCWD,
                        target.as_ptr(),
                        uid.unwrap_or(u32::MAX) as libc::uid_t,
                        gid.unwrap_or(u32::MAX) as libc::gid_t,
                        libc::AT_SYMLINK_NOFOLLOW,
                    )
                };
                if result != 0 {
                    return Err(syscall_failed("fchownat", &backing));
                }
                self.applied(path)
            }
        }
    }

    /// `CreateFile`'s apply, returning the handle the `create(2)` reply needs.
    /// `EEXIST` from `O_EXCL` is a local, free answer -- the transport `O_EXCL`
    /// bit no longer needs to become a synchronous cross-mount arbitration.
    pub(crate) fn apply_create_file(
        &self,
        path: &str,
        mode: u32,
        flags: i32,
    ) -> Result<(MountFile, AppliedMutation)> {
        validate_mount_path(path)?;
        let backing = self.resolve(path)?;
        let descriptor = openat_backing(
            &backing,
            open_flags(flags) | libc::O_CREAT | libc::O_EXCL,
            permission_bits(mode),
        )?;
        let file = MountFile::from_descriptor(path, backing, descriptor);
        let generation = self.bump_generation();
        let metadata = file.metadata()?;
        Ok((
            file,
            AppliedMutation {
                tree_generation: generation,
                local_identity: Some(metadata.local_identity.clone()),
                metadata: Some(metadata),
            },
        ))
    }

    /// Decide whether an unresolved prepare's effect is present in the tree.
    ///
    /// Compares the recorded pre-image against what the tree holds now:
    /// unchanged means `NotApplied`, matching the mutation's postcondition means
    /// `Applied`, anything else is `Ambiguous` and must fail the mount closed
    /// rather than serve a tree nobody can describe.
    ///
    /// The postcondition is tested first. It is the stronger statement, and for
    /// the metadata mutations (`SetMode`, `SetTimes`, `SetOwner`) it is the only
    /// one of the two that can distinguish an apply at all -- the pre-image does
    /// not witness times or ownership, and a chmod to the mode a path already had
    /// leaves both descriptions true at once.
    pub(crate) fn classify(&self, event: &MountEvent) -> Result<ApplyState> {
        let paths = event.mutation.affected_paths();
        let observed = self.observe(&paths)?;
        if matches!(event.mutation, MountMutation::ReplaceFile { .. }) {
            // A content generation applies with no syscall at all, so there is
            // nothing in the tree to inspect. Its payload is an immutable
            // snapshot of bytes the guest already observed, which is exactly the
            // state the replica must converge onto.
            return Ok(ApplyState::Applied);
        }
        if self.postcondition_holds(event, &observed)? {
            return Ok(ApplyState::Applied);
        }
        if pre_image_matches(&event.pre_image, &observed)? {
            return Ok(ApplyState::NotApplied);
        }
        Ok(ApplyState::Ambiguous)
    }

    /// Undo an unresolved prepare that recovery classified as `Applied` but that
    /// the caller decided to roll back. Only defined for mutations whose inverse
    /// is derivable from the pre-image; returns an error otherwise, which fails
    /// the mount closed.
    pub(crate) fn invert(&self, event: &MountEvent) -> Result<()> {
        match &event.mutation {
            MountMutation::ReplaceFile { .. } => {
                // Applied with no syscall, so there is nothing to undo: the
                // backing bytes were never touched by the mutation itself.
                Ok(())
            }
            MountMutation::CreateDirectory { path, .. } => {
                require_absent_pre_image(event, path)?;
                let backing = self.resolve(path)?;
                self.remove_backing(&backing, true)
            }
            MountMutation::CreateFile { path, .. } | MountMutation::CreateSymlink { path, .. } => {
                require_absent_pre_image(event, path)?;
                let backing = self.resolve(path)?;
                self.remove_backing(&backing, false)
            }
            MountMutation::CreateHardLink { new_path, .. } => {
                require_absent_pre_image(event, new_path)?;
                let backing = self.resolve(new_path)?;
                self.remove_backing(&backing, false)
            }
            MountMutation::Rename {
                old_path,
                new_path,
                flags,
            } => {
                // Only a non-replacing rename is invertible: a replacing one
                // destroyed the entry that used to sit at the destination, and
                // the pre-image describes it without being able to restore it.
                require_absent_pre_image(event, new_path)?;
                let source = self.resolve(new_path)?;
                let destination = self.resolve(old_path)?;
                rename_backing(&source, &destination, *flags | RENAME_NOREPLACE)?;
                self.bump_generation();
                Ok(())
            }
            MountMutation::SetMode { path, .. } => {
                let Some(mode) = pre_image_state(event, path)?.mode() else {
                    bail!(
                        "cannot invert SetMode on {path:?}: its pre-image records no prior entry"
                    );
                };
                let backing = self.resolve(path)?;
                self.chmod_backing(&backing, mode)?;
                self.bump_generation();
                Ok(())
            }
            MountMutation::RemoveFile { path, .. } | MountMutation::RemoveDirectory { path } => {
                bail!("cannot invert a removal of {path:?}: the entry is gone")
            }
            MountMutation::SetTimes { path, .. } => {
                bail!("cannot invert SetTimes on {path:?}: the pre-image records no timestamps")
            }
            MountMutation::SetOwner { path, .. } => {
                bail!("cannot invert SetOwner on {path:?}: the pre-image records no uid/gid")
            }
        }
    }

    // -- hydration primitives -----------------------------------------------

    /// Materialize a directory during hydration. Idempotent.
    pub(crate) fn hydrate_directory(&self, path: &str, mode: u32) -> Result<()> {
        let backing = self.resolve(path)?;
        match std::fs::create_dir_all(&backing) {
            Ok(()) => {}
            Err(error) => {
                return Err(anyhow::Error::new(error)
                    .context(format!("hydrate backing directory {}", backing.display())));
            }
        }
        self.chmod_backing(&backing, mode)
    }

    pub(crate) fn hydrate_symlink(&self, path: &str, target: &str) -> Result<()> {
        let backing = self.resolve(path)?;
        self.remove_backing(&backing, false)?;
        let link = c_path(&backing)?;
        let destination = CString::new(target.as_bytes())
            .with_context(|| format!("symlink target {target:?} contains an interior NUL"))?;
        let result =
            unsafe { libc::symlinkat(destination.as_ptr(), libc::AT_FDCWD, link.as_ptr()) };
        if result != 0 {
            return Err(syscall_failed("symlinkat", &backing));
        }
        Ok(())
    }

    /// Link a hydrated file to another name that shares its remote `file_id`.
    pub(crate) fn hydrate_hard_link(&self, existing: &str, new_path: &str) -> Result<()> {
        let source_path = self.resolve(existing)?;
        let destination_path = self.resolve(new_path)?;
        self.remove_backing(&destination_path, false)?;
        let source = c_path(&source_path)?;
        let destination = c_path(&destination_path)?;
        let result = unsafe {
            libc::linkat(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                0,
            )
        };
        if result != 0 {
            return Err(syscall_failed("linkat", &destination_path));
        }
        Ok(())
    }

    /// Open a backing file for hydration writes, truncating any partial content
    /// from an interrupted earlier attempt.
    pub(crate) fn hydrate_file(&self, path: &str, mode: u32) -> Result<MountFile> {
        let backing = self.resolve(path)?;
        let descriptor = openat_backing(
            &backing,
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | libc::O_CLOEXEC,
            permission_bits(mode),
        )?;
        Ok(MountFile::from_descriptor(path, backing, descriptor))
    }

    pub(crate) fn set_mode(&self, path: &str, mode: u32) -> Result<()> {
        let backing = self.resolve(path)?;
        self.chmod_backing(&backing, mode)
    }

    /// Empty the backing tree. Used only when a hydration attempt failed and the
    /// mount must start from a clean, unambiguous view -- never once the mount
    /// has accepted a guest mutation.
    pub(crate) fn reset(&self) -> Result<()> {
        let reader = std::fs::read_dir(&self.root)
            .with_context(|| format!("read backing tree root {}", self.root.display()))?;
        for entry in reader {
            let entry = entry.with_context(|| {
                format!("read backing tree root entry under {}", self.root.display())
            })?;
            let path = entry.path();
            let metadata = entry
                .file_type()
                .with_context(|| format!("stat backing tree entry {}", path.display()))?;
            let removal = if metadata.is_dir() {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            };
            match removal {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(anyhow::Error::new(error)
                        .context(format!("reset backing tree entry {}", path.display())));
                }
            }
        }
        self.generation.store(0, Ordering::SeqCst);
        sync_directory(&self.root)
    }

    pub(crate) fn fsync_dir(&self, path: &str) -> Result<()> {
        self.open_dir(path)?.sync_all()
    }

    // -- internals -----------------------------------------------------------

    fn bump_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Bump the generation and report the resulting entry at `path`.
    fn applied(&self, path: &str) -> Result<AppliedMutation> {
        let generation = self.bump_generation();
        let metadata = self.lstat(path)?;
        Ok(AppliedMutation {
            tree_generation: generation,
            local_identity: metadata
                .as_ref()
                .map(|metadata| metadata.local_identity.clone()),
            metadata,
        })
    }

    /// Bump the generation for a mutation whose result is the absence of a name.
    fn applied_removal(&self) -> AppliedMutation {
        AppliedMutation {
            tree_generation: self.bump_generation(),
            local_identity: None,
            metadata: None,
        }
    }

    fn chmod_backing(&self, backing: &Path, mode: u32) -> Result<()> {
        let target = c_path(backing)?;
        // `AT_SYMLINK_NOFOLLOW` is not supported by `fchmodat` on Linux, so the
        // flag set is deliberately empty: this matches `chmod(2)` exactly.
        let result = unsafe {
            libc::fchmodat(
                libc::AT_FDCWD,
                target.as_ptr(),
                permission_bits(mode) as libc::mode_t,
                0,
            )
        };
        if result != 0 {
            return Err(syscall_failed("fchmodat", backing));
        }
        Ok(())
    }

    /// Remove a name if it is there. A missing entry is success, which is what
    /// makes the inverses and the hydration primitives idempotent.
    fn remove_backing(&self, backing: &Path, directory: bool) -> Result<()> {
        let target = c_path(backing)?;
        let flags = if directory { libc::AT_REMOVEDIR } else { 0 };
        let result = unsafe { libc::unlinkat(libc::AT_FDCWD, target.as_ptr(), flags) };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(());
            }
            return Err(
                anyhow::Error::new(error).context(format!("unlinkat {}", backing.display()))
            );
        }
        Ok(())
    }

    /// Whether the tree now satisfies what the mutation set out to produce.
    fn postcondition_holds(&self, event: &MountEvent, observed: &MountPreImage) -> Result<bool> {
        let observed_state = |path: &str| -> Result<&PreImageState> {
            observed
                .state_of(path)
                .ok_or_else(|| anyhow!("no observation recorded for {path:?}"))
        };
        match &event.mutation {
            MountMutation::ReplaceFile { .. } => Ok(true),
            MountMutation::CreateDirectory { path, .. } => {
                Ok(observed_state(path)?.kind() == Some(LocalKind::Directory))
            }
            MountMutation::CreateFile { path, .. } => {
                Ok(observed_state(path)?.kind() == Some(LocalKind::File))
            }
            MountMutation::CreateSymlink { path, target } => match observed_state(path)? {
                PreImageState::Present {
                    kind: LocalKind::Symlink,
                    link_target,
                    ..
                } => Ok(link_target.as_deref() == Some(target.as_str())),
                _ => Ok(false),
            },
            MountMutation::CreateHardLink {
                existing_path,
                new_path,
            } => {
                let Some(identity) = pre_image_state(event, existing_path)?.local_identity() else {
                    // The source was absent when the pre-image was taken, so the
                    // link could not have succeeded and there is nothing to
                    // recognize.
                    return Ok(false);
                };
                Ok(observed_state(new_path)?.local_identity() == Some(identity))
            }
            MountMutation::Rename {
                old_path, new_path, ..
            } => {
                let Some(identity) = pre_image_state(event, old_path)?.local_identity() else {
                    return Ok(false);
                };
                Ok(observed_state(old_path)?.is_absent()
                    && observed_state(new_path)?.local_identity() == Some(identity))
            }
            MountMutation::RemoveFile { path, .. } | MountMutation::RemoveDirectory { path } => {
                Ok(observed_state(path)?.is_absent())
            }
            MountMutation::SetMode { path, mode } => {
                Ok(observed_state(path)?.mode() == Some(permission_bits(*mode)))
            }
            MountMutation::SetTimes { path, atime, mtime } => {
                if atime.is_none() && mtime.is_none() {
                    return Ok(false);
                }
                let Some(metadata) = self.lstat(path)? else {
                    return Ok(false);
                };
                let atime_matches = atime.is_none_or(|expected| metadata.atime == expected);
                let mtime_matches = mtime.is_none_or(|expected| metadata.mtime == expected);
                Ok(atime_matches && mtime_matches)
            }
            MountMutation::SetOwner { path, uid, gid } => {
                if uid.is_none() && gid.is_none() {
                    return Ok(false);
                }
                let Some(metadata) = self.lstat(path)? else {
                    return Ok(false);
                };
                let uid_matches = uid.is_none_or(|expected| metadata.uid == expected);
                let gid_matches = gid.is_none_or(|expected| metadata.gid == expected);
                Ok(uid_matches && gid_matches)
            }
        }
    }
}

/// An open backing-tree descriptor. This replaces the whole-file `Vec<u8>`
/// buffer the previous design kept per handle, so a 1 GiB write is a sequence of
/// positional writes and a first write at any offset never downloads the file.
#[derive(Debug)]
pub(crate) struct MountFile {
    /// The mount-relative name this descriptor is currently reachable by.
    ///
    /// Interior-mutable because `rename(2)` of an open file has to retarget it.
    /// Everything the content lifecycle does -- the dirty marker, the dependency
    /// key, the seal -- is keyed by pathname, so a descriptor still naming the
    /// pre-rename entry would dirty a name that no longer exists and leave the
    /// bytes the guest wrote after the rename unsealed.
    path: Mutex<String>,
    /// Where the descriptor was opened from. Deliberately *not* retargeted: it
    /// names the open, not the entry, and it is only ever used for error context.
    backing_path: PathBuf,
    file: File,
}

impl MountFile {
    fn from_descriptor(path: &str, backing_path: PathBuf, descriptor: RawFd) -> Self {
        Self {
            path: Mutex::new(path.to_string()),
            backing_path,
            // SAFETY: `descriptor` comes from a successful `openat` and is not
            // owned anywhere else.
            file: unsafe { File::from_raw_fd(descriptor) },
        }
    }

    pub(crate) fn path(&self) -> String {
        self.path
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Point this descriptor at the name a rename moved its entry to. Called by
    /// the FUSE `rename` callback for every open handle under the moved prefix,
    /// after the backing rename has been committed.
    pub(crate) fn retarget(&self, path: &str) {
        let mut current = self
            .path
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        current.clear();
        current.push_str(path);
    }

    pub(crate) fn backing_path(&self) -> &Path {
        &self.backing_path
    }

    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }

    pub(crate) fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            let read = unsafe {
                libc::pread(
                    self.as_raw_fd(),
                    buffer.as_mut_ptr().cast::<libc::c_void>(),
                    buffer.len(),
                    offset as libc::off_t,
                )
            };
            if read < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(anyhow::Error::new(error)
                    .context(format!("pread {}", self.backing_path.display())));
            }
            return Ok(read as usize);
        }
    }

    /// A full positional write. A short `pwrite` on a regular file only happens
    /// when the device fills up, and the caller needs "all of it or an error",
    /// so the loop is part of the contract rather than an optimization.
    pub(crate) fn write_at(&self, bytes: &[u8], offset: u64) -> Result<usize> {
        let mut written = 0usize;
        while written < bytes.len() {
            let chunk = &bytes[written..];
            let count = unsafe {
                libc::pwrite(
                    self.as_raw_fd(),
                    chunk.as_ptr().cast::<libc::c_void>(),
                    chunk.len(),
                    offset.saturating_add(written as u64) as libc::off_t,
                )
            };
            if count < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if written > 0 {
                    // Report the bytes the guest genuinely landed; the next
                    // write re-reports the failure.
                    return Ok(written);
                }
                return Err(anyhow::Error::new(error)
                    .context(format!("pwrite {}", self.backing_path.display())));
            }
            if count == 0 {
                break;
            }
            written += count as usize;
        }
        Ok(written)
    }

    pub(crate) fn truncate(&self, size: u64) -> Result<()> {
        loop {
            let result = unsafe { libc::ftruncate(self.as_raw_fd(), size as libc::off_t) };
            if result != 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(anyhow::Error::new(error)
                    .context(format!("ftruncate {}", self.backing_path.display())));
            }
            return Ok(());
        }
    }

    /// Reserve space. `fallocate` where the kernel and filesystem support it,
    /// an extending `ftruncate` everywhere else -- which is the same guarantee
    /// the guest gets from a filesystem without preallocation.
    pub(crate) fn allocate(&self, offset: u64, length: u64) -> Result<()> {
        if length == 0 {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        {
            let result = unsafe {
                libc::fallocate(
                    self.as_raw_fd(),
                    0,
                    offset as libc::off_t,
                    length as libc::off_t,
                )
            };
            if result == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            let unsupported = matches!(
                error.raw_os_error(),
                Some(libc::ENOSYS) | Some(libc::EOPNOTSUPP)
            );
            if !unsupported {
                return Err(anyhow::Error::new(error)
                    .context(format!("fallocate {}", self.backing_path.display())));
            }
        }
        let end = offset.saturating_add(length);
        let current = self.metadata()?.size_bytes;
        if end > current {
            self.truncate(end)?;
        }
        Ok(())
    }

    /// Server-side range copy. `copy_file_range` where it exists (it is what
    /// makes a reflinking filesystem copy no bytes at all), a bounded buffered
    /// copy otherwise.
    pub(crate) fn copy_range_from(
        &self,
        source: &MountFile,
        source_offset: u64,
        offset: u64,
        length: u64,
    ) -> Result<u64> {
        if length == 0 {
            return Ok(0);
        }
        #[cfg(target_os = "linux")]
        {
            let mut copied = 0u64;
            let mut fallback = false;
            while copied < length {
                let mut read_offset = (source_offset + copied) as libc::off64_t;
                let mut write_offset = (offset + copied) as libc::off64_t;
                let remaining = (length - copied) as usize;
                let count = unsafe {
                    libc::copy_file_range(
                        source.as_raw_fd(),
                        &mut read_offset,
                        self.as_raw_fd(),
                        &mut write_offset,
                        remaining,
                        0,
                    )
                };
                if count < 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    let unsupported = matches!(
                        error.raw_os_error(),
                        Some(libc::ENOSYS)
                            | Some(libc::EOPNOTSUPP)
                            | Some(libc::EXDEV)
                            | Some(libc::EINVAL)
                    );
                    if unsupported && copied == 0 {
                        fallback = true;
                        break;
                    }
                    return Err(anyhow::Error::new(error).context(format!(
                        "copy_file_range {} -> {}",
                        source.backing_path.display(),
                        self.backing_path.display()
                    )));
                }
                if count == 0 {
                    // Source EOF.
                    break;
                }
                copied += count as u64;
            }
            if !fallback {
                return Ok(copied);
            }
        }
        self.copy_range_buffered(source, source_offset, offset, length)
    }

    fn copy_range_buffered(
        &self,
        source: &MountFile,
        source_offset: u64,
        offset: u64,
        length: u64,
    ) -> Result<u64> {
        let mut buffer = vec![0u8; COPY_CHUNK_BYTES.min(length as usize).max(1)];
        let mut copied = 0u64;
        while copied < length {
            let want = ((length - copied) as usize).min(buffer.len());
            let read = source.read_at(&mut buffer[..want], source_offset + copied)?;
            if read == 0 {
                break;
            }
            let written = self.write_at(&buffer[..read], offset + copied)?;
            if written == 0 {
                break;
            }
            copied += written as u64;
        }
        Ok(copied)
    }

    pub(crate) fn set_mode(&self, mode: u32) -> Result<()> {
        let result =
            unsafe { libc::fchmod(self.as_raw_fd(), permission_bits(mode) as libc::mode_t) };
        if result != 0 {
            return Err(syscall_failed("fchmod", &self.backing_path));
        }
        Ok(())
    }

    pub(crate) fn set_owner(&self, uid: Option<u32>, gid: Option<u32>) -> Result<()> {
        let result = unsafe {
            libc::fchown(
                self.as_raw_fd(),
                uid.unwrap_or(u32::MAX) as libc::uid_t,
                gid.unwrap_or(u32::MAX) as libc::gid_t,
            )
        };
        if result != 0 {
            return Err(syscall_failed("fchown", &self.backing_path));
        }
        Ok(())
    }

    pub(crate) fn set_times(
        &self,
        atime: Option<LocalTimestamp>,
        mtime: Option<LocalTimestamp>,
    ) -> Result<()> {
        let times = [timespec_for(atime), timespec_for(mtime)];
        let result = unsafe { libc::futimens(self.as_raw_fd(), times.as_ptr()) };
        if result != 0 {
            return Err(syscall_failed("futimens", &self.backing_path));
        }
        Ok(())
    }

    pub(crate) fn metadata(&self) -> Result<LocalMetadata> {
        let mut buffer = MaybeUninit::<libc::stat>::uninit();
        let result = unsafe { libc::fstat(self.as_raw_fd(), buffer.as_mut_ptr()) };
        if result != 0 {
            return Err(syscall_failed("fstat", &self.backing_path));
        }
        let stat = unsafe { buffer.assume_init() };
        let link_target = if kind_of(&stat) == Some(LocalKind::Symlink) {
            read_link_backing(&self.backing_path)?
        } else {
            None
        };
        project_stat(&stat, link_target, &self.backing_path)
    }

    pub(crate) fn sync_data(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        let result = unsafe { libc::fdatasync(self.as_raw_fd()) };
        #[cfg(not(target_os = "linux"))]
        let result = unsafe { libc::fsync(self.as_raw_fd()) };
        if result != 0 {
            return Err(syscall_failed(SYNC_DATA_CALL, &self.backing_path));
        }
        Ok(())
    }

    pub(crate) fn sync_all(&self) -> Result<()> {
        let result = unsafe { libc::fsync(self.as_raw_fd()) };
        if result != 0 {
            return Err(syscall_failed("fsync", &self.backing_path));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// The errno behind a backing-tree failure, so a FUSE callback can answer with
/// the filesystem's own error instead of a blanket `EIO`. Every syscall failure
/// in this module keeps its [`std::io::Error`] in the context chain for exactly
/// this reason.
pub(crate) fn errno_of(error: &anyhow::Error) -> Option<i32> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .and_then(io::Error::raw_os_error)
    })
}

fn permission_bits(mode: u32) -> u32 {
    mode & 0o7777
}

/// The guest's open flags, minus everything that would make the backing handle
/// behave differently from the positional-I/O handle this design assumes.
///
/// * creation flags are decided by the caller (`apply_create_file` sets them),
/// * `O_TRUNC` is a content mutation and must go through the WAL,
/// * `O_APPEND` would make `pwrite` ignore its offset, and the kernel has
///   already resolved append offsets before the write reaches us,
/// * `O_DIRECT` existed only to defeat uncoordinated cross-VM page caches, which
///   single ownership plus a real backing tree removes.
fn open_flags(flags: i32) -> i32 {
    let stripped = libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC | libc::O_APPEND;
    #[cfg(target_os = "linux")]
    let stripped = stripped | libc::O_DIRECT;
    (flags & !stripped) | libc::O_CLOEXEC
}

fn c_path(path: &Path) -> Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("backing path {} contains an interior NUL", path.display()))
}

fn syscall_failed(call: &str, path: &Path) -> anyhow::Error {
    let error = io::Error::last_os_error();
    anyhow::Error::new(error).context(format!("{call} {}", path.display()))
}

fn openat_backing(backing: &Path, flags: i32, mode: u32) -> Result<RawFd> {
    let target = c_path(backing)?;
    loop {
        let descriptor =
            unsafe { libc::openat(libc::AT_FDCWD, target.as_ptr(), flags, mode as libc::c_uint) };
        if descriptor < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(anyhow::Error::new(error).context(format!("openat {}", backing.display())));
        }
        return Ok(descriptor);
    }
}

fn timespec_for(timestamp: Option<LocalTimestamp>) -> libc::timespec {
    match timestamp {
        Some(timestamp) => libc::timespec {
            tv_sec: timestamp.secs as libc::time_t,
            tv_nsec: timestamp.nanos as _,
        },
        None => libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT as _,
        },
    }
}

/// One atomic rename. `renameat2` carries `RENAME_NOREPLACE`; the plain
/// `renameat` is used when there are no flags, because it is supported
/// everywhere `renameat2` is not.
#[cfg(target_os = "linux")]
fn rename_backing(source: &Path, destination: &Path, flags: u32) -> Result<()> {
    let old = c_path(source)?;
    let new = c_path(destination)?;
    let (call, result) = if flags == 0 {
        ("renameat", unsafe {
            libc::renameat(libc::AT_FDCWD, old.as_ptr(), libc::AT_FDCWD, new.as_ptr())
        })
    } else {
        ("renameat2", unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                old.as_ptr(),
                libc::AT_FDCWD,
                new.as_ptr(),
                flags as libc::c_uint,
            ) as libc::c_int
        })
    };
    if result != 0 {
        return Err(syscall_failed(call, destination));
    }
    Ok(())
}

/// Developer-machine fallback. `RENAME_NOREPLACE` is rejected rather than
/// silently ignored: honouring it non-atomically would publish a namespace state
/// the mount never had.
#[cfg(not(target_os = "linux"))]
fn rename_backing(source: &Path, destination: &Path, flags: u32) -> Result<()> {
    if flags & RENAME_NOREPLACE != 0 {
        bail!(
            "RENAME_NOREPLACE has no atomic form on this platform: refusing to rename {} -> {}",
            source.display(),
            destination.display()
        );
    }
    if flags != 0 {
        bail!("unsupported rename flags {flags:#x} on this platform");
    }
    let old = c_path(source)?;
    let new = c_path(destination)?;
    let result =
        unsafe { libc::renameat(libc::AT_FDCWD, old.as_ptr(), libc::AT_FDCWD, new.as_ptr()) };
    if result != 0 {
        return Err(syscall_failed("renameat", destination));
    }
    Ok(())
}

fn kind_of(stat: &libc::stat) -> Option<LocalKind> {
    match (stat.st_mode as u32) & (libc::S_IFMT as u32) {
        value if value == libc::S_IFREG as u32 => Some(LocalKind::File),
        value if value == libc::S_IFDIR as u32 => Some(LocalKind::Directory),
        value if value == libc::S_IFLNK as u32 => Some(LocalKind::Symlink),
        _ => None,
    }
}

fn project_stat(
    stat: &libc::stat,
    link_target: Option<String>,
    backing: &Path,
) -> Result<LocalMetadata> {
    let Some(kind) = kind_of(stat) else {
        bail!(
            "backing entry {} is not a file, directory or symlink (mode {:#o})",
            backing.display(),
            stat.st_mode as u32
        );
    };
    Ok(LocalMetadata {
        kind,
        size_bytes: stat.st_size.max(0) as u64,
        blocks: stat.st_blocks.max(0) as u64,
        mode: permission_bits(stat.st_mode as u32),
        uid: stat.st_uid as u32,
        gid: stat.st_gid as u32,
        link_count: stat.st_nlink as u64,
        local_identity: format!("{}:{}", stat.st_dev as u64, stat.st_ino as u64),
        backing_ino: stat.st_ino as u64,
        link_target,
        atime: LocalTimestamp {
            secs: stat.st_atime as i64,
            nanos: stat.st_atime_nsec.max(0) as u32,
        },
        mtime: LocalTimestamp {
            secs: stat.st_mtime as i64,
            nanos: stat.st_mtime_nsec.max(0) as u32,
        },
        ctime: LocalTimestamp {
            secs: stat.st_ctime as i64,
            nanos: stat.st_ctime_nsec.max(0) as u32,
        },
    })
}

fn lstat_raw(backing: &Path) -> Result<Option<libc::stat>> {
    let target = c_path(backing)?;
    let mut buffer = MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            libc::AT_FDCWD,
            target.as_ptr(),
            buffer.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(anyhow::Error::new(error).context(format!("lstat {}", backing.display())));
    }
    Ok(Some(unsafe { buffer.assume_init() }))
}

fn lstat_backing(backing: &Path) -> Result<Option<LocalMetadata>> {
    let Some(stat) = lstat_raw(backing)? else {
        return Ok(None);
    };
    let link_target = if kind_of(&stat) == Some(LocalKind::Symlink) {
        match read_link_backing(backing)? {
            Some(target) => Some(target),
            // The symlink was unlinked between the two calls; report the entry
            // as gone rather than as a symlink with no target.
            None => return Ok(None),
        }
    } else {
        None
    };
    Ok(Some(project_stat(&stat, link_target, backing)?))
}

fn read_link_backing(backing: &Path) -> Result<Option<String>> {
    match std::fs::read_link(backing) {
        Ok(target) => match target.into_os_string().into_string() {
            Ok(target) => Ok(Some(target)),
            Err(raw) => bail!(
                "backing symlink {} has a non-UTF-8 target {:?}",
                backing.display(),
                raw
            ),
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(anyhow::Error::new(error).context(format!("readlink {}", backing.display())))
        }
    }
}

fn pre_image_state<'a>(event: &'a MountEvent, path: &str) -> Result<&'a PreImageState> {
    event.pre_image.state_of(path).ok_or_else(|| {
        anyhow!(
            "mount event {} has no pre-image entry for {path:?}",
            event.idempotency_key
        )
    })
}

fn require_absent_pre_image(event: &MountEvent, path: &str) -> Result<()> {
    if !pre_image_state(event, path)?.is_absent() {
        bail!(
            "cannot invert mount event {}: {path:?} was not absent before it applied",
            event.idempotency_key
        );
    }
    Ok(())
}

/// Whether the tree still holds exactly what the pre-image recorded.
///
/// `size_bytes` is deliberately excluded. Content writes are through-writes that
/// take no dependency key, so a file's length can legitimately change between
/// the observation and the crash while the namespace mutation itself has not run.
/// Every other field is protected by the exclusive key the prepare holds, so a
/// difference in one of them is real evidence.
fn pre_image_matches(recorded: &MountPreImage, observed: &MountPreImage) -> Result<bool> {
    for entry in &observed.entries {
        let Some(state) = recorded.state_of(&entry.path) else {
            bail!("no pre-image entry recorded for {:?}", entry.path);
        };
        if !states_match(state, &entry.state) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn states_match(recorded: &PreImageState, observed: &PreImageState) -> bool {
    match (recorded, observed) {
        (PreImageState::Absent, PreImageState::Absent) => true,
        (
            PreImageState::Present {
                kind: recorded_kind,
                mode: recorded_mode,
                local_identity: recorded_identity,
                link_count: recorded_links,
                link_target: recorded_target,
                ..
            },
            PreImageState::Present {
                kind: observed_kind,
                mode: observed_mode,
                local_identity: observed_identity,
                link_count: observed_links,
                link_target: observed_target,
                ..
            },
        ) => {
            recorded_kind == observed_kind
                && recorded_mode == observed_mode
                && recorded_identity == observed_identity
                && recorded_links == observed_links
                && recorded_target == observed_target
        }
        _ => false,
    }
}
