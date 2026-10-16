//! Lazy file content: a hydrated mount serves its namespace at once and fetches file bytes
//! on first open, or in the background, whichever comes first.
//!
//! ## Why this is sound
//!
//! Hydration still walks the whole namespace before the FUSE session starts, so every
//! directory, symlink, name, size and mode the guest can observe is final at mount time.
//! Only regular-file *bytes* are deferred. A file whose bytes have not arrived is a
//! **placeholder**: a backing file of the listed size (space reserved, reading as zeros)
//! carrying the [`PLACEHOLDER_XATTR`] extended attribute. Its content is always fetched
//! before anything can observe or change it:
//!
//! * every `open` (read or write), `truncate`, hard link, `utimens` and rename source on a
//!   placeholder blocks until its bytes are in place (`ensure`); the guest has no other way to
//!   reach a file's bytes, because each read and write goes through an open handle, so a
//!   placeholder's zeros are never read and never published;
//! * an unlink, or a rename over the placeholder, drops it without fetching anything.
//!
//! A placeholder's remote path still holds the bytes it was listed with: the mount is the only
//! writer of its scope, and the only way the guest can change that remote path is a mutation
//! that first fetches (or drops) the placeholder. Bytes are verified against the listed hash
//! (or size, past the `/tree` hash budget) before the placeholder is retired.
//!
//! ## Durability
//!
//! The attribute lives on the inode, so it travels with hard links, disappears with the last
//! unlink, and is absent from any file the guest creates at a placeholder's old name. Filling
//! one writes the bytes and restores the times the guest saw through one descriptor whose
//! attribute is checked first, makes the bytes durable, then removes the attribute and makes
//! that durable (one `fsync` for an open waiting on the file; two `syncfs` per batch in the
//! background fill); only then can an open see the file. A crash at any point therefore
//! leaves either a placeholder (fetched again after restart) or a complete file, never
//! zeros that look like content. A mount that still has placeholders marks its backing tree
//! root with [`PENDING_ROOT_XATTR`]; a restart that finds it rescans the tree and resumes.
//!
//! The one thing the guest can see is timing: the first open of a large file waits for its
//! download. Directory listings, `stat` and sizes never wait.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use super::hydrate::MountHydrator;

/// Marks a backing file whose bytes have not been fetched yet. Its value is the JSON
/// [`Placeholder`] the file was listed with.
pub(crate) const PLACEHOLDER_XATTR: &str = "user.chevalier.placeholder";
/// Marks a backing tree root that still holds placeholders, so a restart rescans it.
pub(crate) const PENDING_ROOT_XATTR: &str = "user.chevalier.placeholders-pending";
/// Opt out of lazy content (`1`): hydrate every byte before mounting, as before.
pub(crate) const EAGER_HYDRATE_ENV: &str = "CHEVALIER_VFS_EAGER_HYDRATE";

/// How long an open waits for a file's bytes before failing with `EIO`. The download of
/// one file is all that stands between the open and its answer, so this bounds a gateway
/// outage, not ordinary large-file transfer.
const ENSURE_TIMEOUT: Duration = Duration::from_secs(300);

/// What a placeholder was listed as, and what its bytes are verified against.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Placeholder {
    pub(crate) size_bytes: u64,
    /// `None` when the file exceeded the `/tree` hash budget: only its length is checked.
    pub(crate) content_hash: Option<String>,
}

/// One placeholder inode and every name it is reachable by.
#[derive(Clone, Debug)]
pub(crate) struct PendingEntry {
    pub(crate) members: Vec<String>,
    pub(crate) placeholder: Placeholder,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Status {
    Pending,
    Fetching,
    /// The last fetch failed; the next `ensure` tries again.
    Failed(String),
}

struct Entry {
    pending: PendingEntry,
    status: Status,
}

#[derive(Default)]
struct State {
    entries: HashMap<u64, Entry>,
    /// Ordered, so everything under a directory is one range (a rename of it asks).
    by_path: BTreeMap<String, u64>,
    next_id: u64,
}

impl State {
    /// The placeholder names at or below `prefix` (all of them for the scope root).
    fn within<'a>(&'a self, prefix: &str) -> Box<dyn Iterator<Item = (&'a String, &'a u64)> + 'a> {
        if prefix.is_empty() {
            return Box::new(self.by_path.iter());
        }
        // '0' is the character after '/', so this range is exactly the paths below prefix.
        let below = self
            .by_path
            .range(format!("{prefix}/")..format!("{prefix}0"));
        Box::new(self.by_path.get_key_value(prefix).into_iter().chain(below))
    }

    fn detach(&mut self, path: &str) -> Option<u64> {
        let id = self.by_path.remove(path)?;
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.pending.members.retain(|member| member != path);
        }
        Some(id)
    }

    /// Remove an entry nobody names any more, unless a fetch still owns it.
    fn drop_if_unnamed(&mut self, id: u64) -> bool {
        let unnamed = self.entries.get(&id).is_some_and(|entry| {
            entry.pending.members.is_empty() && entry.status != Status::Fetching
        });
        if unnamed {
            self.entries.remove(&id);
        }
        unnamed
    }
}

/// The mount's set of placeholders, shared by FUSE callbacks (which wait on it) and the
/// hydrator (which fills it).
pub(crate) struct LazyContent {
    state: Mutex<State>,
    changed: Condvar,
    /// Entries not yet filled, so a mount with none skips the lock on every open.
    outstanding: AtomicUsize,
    fetcher: OnceLock<Weak<MountHydrator>>,
    tokio: Handle,
}

impl LazyContent {
    pub(crate) fn new(tokio: Handle) -> Self {
        Self {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            outstanding: AtomicUsize::new(0),
            fetcher: OnceLock::new(),
            tokio,
        }
    }

    /// The hydrator fetches on demand. Weak, because the hydrator owns the view that owns
    /// this set.
    pub(crate) fn attach_fetcher(&self, hydrator: &Arc<MountHydrator>) {
        let _ = self.fetcher.set(Arc::downgrade(hydrator));
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn outstanding(&self) -> usize {
        self.outstanding.load(Ordering::Acquire)
    }

    /// Track a placeholder reachable by `members`. A member that named another entry is
    /// moved here: the backing name now holds this placeholder.
    pub(crate) fn register(&self, members: Vec<String>, placeholder: Placeholder) {
        let mut state = self.lock();
        for member in &members {
            if let Some(previous) = state.detach(member)
                && state.drop_if_unnamed(previous)
            {
                self.outstanding.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let id = state.next_id;
        state.next_id += 1;
        for member in &members {
            state.by_path.insert(member.clone(), id);
        }
        state.entries.insert(
            id,
            Entry {
                pending: PendingEntry {
                    members,
                    placeholder,
                },
                status: Status::Pending,
            },
        );
        self.outstanding.fetch_add(1, Ordering::AcqRel);
    }

    /// Forget every placeholder (a hydrate starting over from an empty tree).
    pub(crate) fn clear(&self) {
        let mut state = self.lock();
        *state = State {
            next_id: state.next_id,
            ..State::default()
        };
        self.outstanding.store(0, Ordering::Release);
        self.changed.notify_all();
    }

    /// `path` no longer names its placeholder (unlinked, renamed over, or replaced by the
    /// hydrator): nothing will ever need its bytes under that name.
    pub(crate) fn forget(&self, path: &str) {
        if self.outstanding() == 0 {
            return;
        }
        let mut state = self.lock();
        if let Some(id) = state.detach(path)
            && state.drop_if_unnamed(id)
        {
            self.outstanding.fetch_sub(1, Ordering::AcqRel);
            self.changed.notify_all();
        }
    }

    /// `forget` for `prefix` and everything below it.
    pub(crate) fn forget_subtree(&self, prefix: &str) {
        if self.outstanding() == 0 {
            return;
        }
        let paths: Vec<String> = {
            let state = self.lock();
            state.within(prefix).map(|(path, _)| path.clone()).collect()
        };
        for path in paths {
            self.forget(&path);
        }
    }

    /// `forget`, unless a fetch is writing `path`'s placeholder right now (false: try again
    /// once it has finished).
    pub(crate) fn forget_unless_fetching(&self, path: &str) -> bool {
        if self.outstanding() == 0 {
            return true;
        }
        let mut state = self.lock();
        let fetching = state
            .by_path
            .get(path)
            .and_then(|id| state.entries.get(id))
            .is_some_and(|entry| entry.status == Status::Fetching);
        if fetching {
            return false;
        }
        if let Some(id) = state.detach(path)
            && state.drop_if_unnamed(id)
        {
            self.outstanding.fetch_sub(1, Ordering::AcqRel);
            self.changed.notify_all();
        }
        true
    }

    /// Make every failed entry pending again, for another background round.
    pub(crate) fn retry_failed(&self) {
        let mut state = self.lock();
        for entry in state.entries.values_mut() {
            if matches!(entry.status, Status::Failed(_)) {
                entry.status = Status::Pending;
            }
        }
    }

    /// Block until `path`'s bytes are in place. Returns at once for anything that is not a
    /// placeholder. Fails with `EIO` when the fetch fails or outlasts [`ENSURE_TIMEOUT`].
    pub(crate) fn ensure(&self, path: &str) -> Result<()> {
        if self.outstanding() == 0 {
            return Ok(());
        }
        let id = {
            let state = self.lock();
            match state.by_path.get(path) {
                Some(id) => *id,
                None => return Ok(()),
            }
        };
        self.ensure_ids(&[id], path)
    }

    /// `ensure` for every placeholder at or below `prefix` (a directory about to be renamed).
    pub(crate) fn ensure_subtree(&self, prefix: &str) -> Result<()> {
        if self.outstanding() == 0 {
            return Ok(());
        }
        let ids: Vec<u64> = {
            let state = self.lock();
            let ids: HashSet<u64> = state.within(prefix).map(|(_, id)| *id).collect();
            ids.into_iter().collect()
        };
        if ids.is_empty() {
            return Ok(());
        }
        self.ensure_ids(&ids, prefix)
    }

    fn ensure_ids(&self, ids: &[u64], what: &str) -> Result<()> {
        let started = Instant::now();
        let deadline = started + ENSURE_TIMEOUT;
        let mut requested: HashSet<u64> = HashSet::new();
        let mut state = self.lock();
        loop {
            let mut waiting = false;
            for id in ids {
                let Some(entry) = state.entries.get_mut(id) else {
                    continue;
                };
                match &entry.status {
                    Status::Fetching => waiting = true,
                    Status::Failed(error) if requested.contains(id) => {
                        return Err(placeholder_error(what, error));
                    }
                    Status::Pending | Status::Failed(_) => {
                        requested.insert(*id);
                        if !self.spawn_fetch(*id) {
                            return Err(placeholder_error(what, "the mount is shutting down"));
                        }
                        entry.status = Status::Fetching;
                        waiting = true;
                    }
                }
            }
            if !waiting {
                if !requested.is_empty() {
                    tracing::debug!(
                        path = what,
                        files = requested.len(),
                        waited_ms = started.elapsed().as_millis() as u64,
                        "fetched placeholder content on first use"
                    );
                }
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(placeholder_error(
                    what,
                    &format!("its content did not arrive within {ENSURE_TIMEOUT:?}"),
                ));
            }
            state = self
                .changed
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
    }

    /// Start fetching an entry this caller just claimed; false when no hydrator is left to
    /// do it (the mount is going away).
    fn spawn_fetch(&self, id: u64) -> bool {
        let Some(hydrator) = self.fetcher.get().and_then(Weak::upgrade) else {
            return false;
        };
        self.tokio
            .spawn(async move { hydrator.fill_claimed(id).await });
        true
    }

    /// Take a pending entry for a background fetch. `None` when it is gone, already being
    /// fetched, or failed (a failed one is retried only when something needs it).
    pub(crate) fn claim(&self, id: u64) -> Option<PendingEntry> {
        let mut state = self.lock();
        let entry = state.entries.get_mut(&id)?;
        if entry.status != Status::Pending {
            return None;
        }
        entry.status = Status::Fetching;
        Some(entry.pending.clone())
    }

    /// `claim` by path.
    pub(crate) fn claim_path(&self, path: &str) -> Option<(u64, PendingEntry)> {
        let id = *self.lock().by_path.get(path)?;
        self.claim(id).map(|entry| (id, entry))
    }

    /// The entry a claimed fetch is filling, as it stands now (members may have gone).
    pub(crate) fn snapshot(&self, id: u64) -> Option<PendingEntry> {
        Some(self.lock().entries.get(&id)?.pending.clone())
    }

    /// Hand a claim back untouched (a pack carried bytes that did not verify).
    pub(crate) fn release(&self, id: u64) {
        let mut state = self.lock();
        if let Some(entry) = state.entries.get_mut(&id)
            && entry.status == Status::Fetching
        {
            entry.status = Status::Pending;
        }
        if state.drop_if_unnamed(id) {
            self.outstanding.fetch_sub(1, Ordering::AcqRel);
        }
        self.changed.notify_all();
    }

    /// The claimed entry's bytes are in place and durable.
    pub(crate) fn complete(&self, id: u64) {
        let mut state = self.lock();
        if let Some(entry) = state.entries.remove(&id) {
            for member in entry.pending.members {
                if state.by_path.get(&member) == Some(&id) {
                    state.by_path.remove(&member);
                }
            }
            self.outstanding.fetch_sub(1, Ordering::AcqRel);
        }
        self.changed.notify_all();
    }

    /// The claimed entry could not be fetched.
    pub(crate) fn fail(&self, id: u64, error: &anyhow::Error) {
        let mut state = self.lock();
        if let Some(entry) = state.entries.get_mut(&id) {
            entry.status = Status::Failed(format!("{error:#}"));
        }
        if state.drop_if_unnamed(id) {
            self.outstanding.fetch_sub(1, Ordering::AcqRel);
        }
        self.changed.notify_all();
    }

    /// Every entry still waiting for a background fetch, in path order.
    pub(crate) fn pending_ids(&self) -> Vec<u64> {
        let state = self.lock();
        let mut ids: Vec<(String, u64)> = state
            .entries
            .iter()
            .filter(|(_, entry)| entry.status == Status::Pending)
            .map(|(id, entry)| {
                (
                    entry.pending.members.first().cloned().unwrap_or_default(),
                    *id,
                )
            })
            .collect();
        ids.sort();
        ids.into_iter().map(|(_, id)| id).collect()
    }
}

fn placeholder_error(path: &str, error: &str) -> anyhow::Error {
    anyhow::Error::new(std::io::Error::from_raw_os_error(libc::EIO))
        .context(format!("fetch the content of {path:?}: {error}"))
}

// ---------------------------------------------------------------------------
// Extended attributes
// ---------------------------------------------------------------------------

/// Whether `error` says the filesystem keeps no user extended attributes.
pub(crate) fn xattrs_unsupported(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ENOTSUP) | Some(libc::ENOSYS)
    ) || error.kind() == std::io::ErrorKind::Unsupported
}

#[cfg(target_os = "linux")]
mod xattr {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::RawFd;
    use std::path::Path;

    fn name(name: &str) -> CString {
        CString::new(name).expect("xattr names carry no NUL")
    }

    fn c_path(path: &Path) -> io::Result<CString> {
        CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
    }

    fn check(result: libc::c_int) -> io::Result<()> {
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn fset(fd: RawFd, key: &str, value: &[u8]) -> io::Result<()> {
        let key = name(key);
        check(unsafe { libc::fsetxattr(fd, key.as_ptr(), value.as_ptr().cast(), value.len(), 0) })
    }

    pub(super) fn set(path: &Path, key: &str, value: &[u8]) -> io::Result<()> {
        let (path, key) = (c_path(path)?, name(key));
        check(unsafe {
            libc::lsetxattr(
                path.as_ptr(),
                key.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        })
    }

    fn read(
        mut call: impl FnMut(*mut libc::c_void, usize) -> isize,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut buffer = vec![0u8; 512];
        loop {
            let read = call(buffer.as_mut_ptr().cast(), buffer.len());
            if read >= 0 {
                buffer.truncate(read as usize);
                return Ok(Some(buffer));
            }
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::ENODATA) => return Ok(None),
                Some(libc::ERANGE) if buffer.len() < 64 * 1024 => {
                    buffer.resize(buffer.len() * 4, 0)
                }
                _ => return Err(error),
            }
        }
    }

    pub(super) fn fget(fd: RawFd, key: &str) -> io::Result<Option<Vec<u8>>> {
        let key = name(key);
        read(|buffer, len| unsafe { libc::fgetxattr(fd, key.as_ptr(), buffer, len) })
    }

    pub(super) fn get(path: &Path, key: &str) -> io::Result<Option<Vec<u8>>> {
        let (path, key) = (c_path(path)?, name(key));
        read(|buffer, len| unsafe { libc::lgetxattr(path.as_ptr(), key.as_ptr(), buffer, len) })
    }

    fn absent_is_ok(result: io::Result<()>) -> io::Result<()> {
        match result {
            Err(error) if error.raw_os_error() == Some(libc::ENODATA) => Ok(()),
            other => other,
        }
    }

    pub(super) fn fremove(fd: RawFd, key: &str) -> io::Result<()> {
        let key = name(key);
        absent_is_ok(check(unsafe { libc::fremovexattr(fd, key.as_ptr()) }))
    }

    pub(super) fn remove(path: &Path, key: &str) -> io::Result<()> {
        let (path, key) = (c_path(path)?, name(key));
        absent_is_ok(check(unsafe {
            libc::lremovexattr(path.as_ptr(), key.as_ptr())
        }))
    }
}

#[cfg(not(target_os = "linux"))]
mod xattr {
    use std::io;
    use std::os::unix::io::RawFd;
    use std::path::Path;

    fn unsupported<T>() -> io::Result<T> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
    pub(super) fn fset(_: RawFd, _: &str, _: &[u8]) -> io::Result<()> {
        unsupported()
    }
    pub(super) fn set(_: &Path, _: &str, _: &[u8]) -> io::Result<()> {
        unsupported()
    }
    pub(super) fn fget(_: RawFd, _: &str) -> io::Result<Option<Vec<u8>>> {
        Ok(None)
    }
    pub(super) fn get(_: &Path, _: &str) -> io::Result<Option<Vec<u8>>> {
        Ok(None)
    }
    pub(super) fn fremove(_: RawFd, _: &str) -> io::Result<()> {
        Ok(())
    }
    pub(super) fn remove(_: &Path, _: &str) -> io::Result<()> {
        Ok(())
    }
}

/// Mark the descriptor's inode as a placeholder for `placeholder`.
pub(crate) fn mark_placeholder(
    fd: std::os::unix::io::RawFd,
    placeholder: &Placeholder,
) -> std::io::Result<()> {
    let value = serde_json::to_vec(placeholder).map_err(std::io::Error::other)?;
    xattr::fset(fd, PLACEHOLDER_XATTR, &value)
}

fn decode(value: Option<Vec<u8>>) -> Option<Placeholder> {
    serde_json::from_slice(&value?).ok()
}

/// The placeholder the descriptor's inode is, if it is one.
pub(crate) fn placeholder_of_fd(
    fd: std::os::unix::io::RawFd,
) -> std::io::Result<Option<Placeholder>> {
    Ok(decode(xattr::fget(fd, PLACEHOLDER_XATTR)?))
}

/// The placeholder the backing entry at `path` is, if it is one. Symlinks are not followed.
pub(crate) fn placeholder_at(path: &Path) -> std::io::Result<Option<Placeholder>> {
    Ok(decode(xattr::get(path, PLACEHOLDER_XATTR)?))
}

pub(crate) fn clear_placeholder(fd: std::os::unix::io::RawFd) -> std::io::Result<()> {
    xattr::fremove(fd, PLACEHOLDER_XATTR)
}

/// Mark (or unmark) the backing tree root as holding placeholders. Marking is also how a
/// hydrate learns whether the backing filesystem keeps user extended attributes at all.
pub(crate) fn set_pending_root(root: &Path, pending: bool) -> std::io::Result<()> {
    if pending {
        xattr::set(root, PENDING_ROOT_XATTR, b"1")
    } else {
        xattr::remove(root, PENDING_ROOT_XATTR)
    }
}

pub(crate) fn root_has_pending(root: &Path) -> bool {
    matches!(xattr::get(root, PENDING_ROOT_XATTR), Ok(Some(_)))
}

/// Every placeholder under `root`, grouped by inode so hard links share one entry.
/// Paths are mount-relative.
pub(crate) fn scan_placeholders(root: &Path) -> Result<Vec<PendingEntry>> {
    use std::os::unix::fs::MetadataExt;

    let mut by_inode: HashMap<(u64, u64), PendingEntry> = HashMap::new();
    let mut directories = vec![(root.to_path_buf(), String::new())];
    while let Some((directory, relative)) = directories.pop() {
        let reader = std::fs::read_dir(&directory)
            .map_err(|error| anyhow!("scan {} for placeholders: {error}", directory.display()))?;
        for entry in reader {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let path = if relative.is_empty() {
                name.to_string()
            } else {
                format!("{relative}/{name}")
            };
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push((entry.path(), path));
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let Some(placeholder) = placeholder_at(&entry.path())? else {
                continue;
            };
            let metadata = entry.metadata()?;
            by_inode
                .entry((metadata.dev(), metadata.ino()))
                .or_insert_with(|| PendingEntry {
                    members: Vec::new(),
                    placeholder,
                })
                .members
                .push(path);
        }
    }
    Ok(by_inode.into_values().collect())
}
