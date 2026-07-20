// @dive-file: Local filesystem implementation of the optimized VFS storage trait.
// @dive-rel: Provides the direct/dev backend for chevalier-vfs without product policy or VM concerns.
// @dive-rel: Mirrors the old local nymfs adapter semantics while exposing batch-oriented calls.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
#[cfg(unix)]
use std::ffi::CString;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use sha2::Digest;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock as AsyncRwLock};
use uuid::Uuid;

use crate::{
    OptimizedVfsStorage, VfsStorageCasPredicate, VfsStorageDeleteResult, VfsStorageDirListFilter,
    VfsStorageDirListOrder, VfsStorageEntryKind, VfsStorageError, VfsStorageHardLinkResult,
    VfsStorageMetadata, VfsStorageMetadataFields, VfsStorageNamespaceMutation,
    VfsStorageObjectState, VfsStoragePrefetchOptions, VfsStoragePrefetchResult,
    VfsStorageReadIfChanged, VfsStorageReadIfChangedResult, VfsStorageReadRange,
    VfsStorageRenameResult, VfsStorageResult, VfsStorageSubtreeOptions, VfsStorageWrite,
    VfsStorageWriteOptions, VfsStorageWritePrecondition, VfsStorageWriteResult, normalize_vfs_mode,
    pack::{SlotCompression, hex_hash},
};

#[derive(Clone, Debug)]
pub struct LocalVfsStorage {
    root: PathBuf,
    hash_cache: Arc<Mutex<HashMap<PathBuf, CachedFileHash>>>,
    incomplete_absent_writes: Arc<Mutex<HashMap<PathBuf, IncompleteAbsentWrite>>>,
    path_locks: PathLockTable,
    #[cfg(test)]
    durability_sync_observer: Option<DurabilitySyncObserver>,
    #[cfg(test)]
    hash_read_count: Arc<AtomicUsize>,
}

#[cfg(test)]
#[derive(Clone)]
struct DurabilitySyncObserver(
    Arc<dyn Fn(&DurabilitySyncEvent) -> VfsStorageResult<()> + Send + Sync>,
);

#[cfg(test)]
impl std::fmt::Debug for DurabilitySyncObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("DurabilitySyncObserver")
            .field(&"<callback>")
            .finish()
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
enum DurabilitySyncEvent {
    File(PathBuf),
    Directory(PathBuf),
}

struct SymlinkTargetInfo {
    target_text: String,
}

#[derive(Clone, Debug)]
struct CachedFileHash {
    file_id: Option<String>,
    size_bytes: u64,
    mtime_ns: i128,
    change_ns: i128,
    cached_at: SystemTime,
    hash: String,
    trusted_write: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IncompleteAbsentWrite {
    content_hash: String,
    file_id: Option<String>,
    options: Option<VfsStorageWriteOptions>,
}

/// Per-path async mutual exclusion for the check+install critical section.
///
/// Waiting is done through `tokio::sync::Mutex`, which parks the *task* (yielding
/// the worker thread) rather than blocking the OS thread. A blocking wait here would
/// starve the napi tokio runtime: enough concurrent same-path mutations would leave
/// every worker parked in a `Condvar::wait`, with no thread left to run — and release
/// — the lock holder. The map's `std::sync::Mutex` is only ever held for the brief
/// fetch-or-create, never across an `.await`.
#[derive(Clone, Default)]
struct PathLockTable {
    inner: Arc<Mutex<HashMap<String, Arc<AsyncRwLock<()>>>>>,
}

struct PathLocks {
    guards: Vec<PathLockGuard>,
    keys: Vec<String>,
    table: PathLockTable,
}

enum PathLockGuard {
    Read { _guard: OwnedRwLockReadGuard<()> },
    Write { _guard: OwnedRwLockWriteGuard<()> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PathLockMode {
    Read,
    Write,
}

const HASH_CACHE_RECENCY_GUARD: Duration = Duration::from_secs(2);
const HASH_CACHE_MAX_AGE: Duration = Duration::from_secs(30);
const MAX_PARALLEL_FILE_SYNCS: usize = 8;
// A 10k-file Git working set must fit without a sequential status scan evicting
// the entries that the same scan is about to revisit. The cache remains
// hard-bounded; the torture test below reports its observed payload and a
// projected full-capacity footprint.
const MAX_HASH_CACHE_ENTRIES: usize = 16_384;

impl std::fmt::Debug for PathLockTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PathLockTable").finish_non_exhaustive()
    }
}

impl PathLockTable {
    /// Acquire intent-read locks for ancestors and exclusive locks for mutation
    /// targets. Sibling mutations remain concurrent, while an ancestor rename or
    /// removal excludes every descendant mutation. Keys are acquired in global
    /// lexical order so overlapping multi-path operations cannot deadlock.
    async fn lock(&self, paths: impl IntoIterator<Item = String>) -> PathLocks {
        self.lock_with_target_mode(paths, PathLockMode::Write).await
    }

    /// Point reads share their target lock. They still exclude an exact-path
    /// mutation, but a directory stat/list or subtree scan no longer takes an
    /// exclusive ancestor lock that stalls every descendant operation.
    async fn lock_read(&self, paths: impl IntoIterator<Item = String>) -> PathLocks {
        self.lock_with_target_mode(paths, PathLockMode::Read).await
    }

    async fn lock_with_target_mode(
        &self,
        paths: impl IntoIterator<Item = String>,
        target_mode: PathLockMode,
    ) -> PathLocks {
        let mut modes = HashMap::new();
        for path in paths {
            let path = path.trim_matches('/');
            let mut current = String::new();
            modes.entry(current.clone()).or_insert(PathLockMode::Read);
            let components = path
                .split('/')
                .filter(|component| !component.is_empty())
                .collect::<Vec<_>>();
            if components.is_empty() {
                modes.insert(current, target_mode);
                continue;
            }
            for (index, component) in components.iter().enumerate() {
                if !current.is_empty() {
                    current.push('/');
                }
                current.push_str(component);
                let mode = if index + 1 == components.len() {
                    target_mode
                } else {
                    PathLockMode::Read
                };
                modes
                    .entry(current.clone())
                    .and_modify(|existing| {
                        if mode == PathLockMode::Write {
                            *existing = PathLockMode::Write;
                        }
                    })
                    .or_insert(mode);
            }
        }
        let mut requested = modes.into_iter().collect::<Vec<_>>();
        requested.sort_by(|(left, _), (right, _)| left.cmp(right));
        let locks = {
            let mut map = self.inner.lock().unwrap_or_else(|err| err.into_inner());
            requested
                .iter()
                .map(|(key, mode)| {
                    let lock = map
                        .entry(key.clone())
                        .or_insert_with(|| Arc::new(AsyncRwLock::new(())))
                        .clone();
                    (lock, *mode)
                })
                .collect::<Vec<_>>()
        };
        let mut guards = Vec::with_capacity(locks.len());
        for (lock, mode) in locks {
            guards.push(match mode {
                PathLockMode::Read => PathLockGuard::Read {
                    _guard: lock.read_owned().await,
                },
                PathLockMode::Write => PathLockGuard::Write {
                    _guard: lock.write_owned().await,
                },
            });
        }
        PathLocks {
            guards,
            keys: requested.into_iter().map(|(key, _)| key).collect(),
            table: self.clone(),
        }
    }
}

impl Drop for PathLocks {
    fn drop(&mut self) {
        // Release the held guards first, then prune any per-path entry that no other
        // task is still holding or waiting on (strong_count == 1 => only the map).
        self.guards.clear();
        let mut map = self
            .table
            .inner
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        for key in &self.keys {
            if map
                .get(key)
                .is_some_and(|mutex| Arc::strong_count(mutex) == 1)
            {
                map.remove(key);
            }
        }
    }
}

impl LocalVfsStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            hash_cache: Arc::new(Mutex::new(HashMap::new())),
            incomplete_absent_writes: Arc::new(Mutex::new(HashMap::new())),
            path_locks: PathLockTable::default(),
            #[cfg(test)]
            durability_sync_observer: None,
            #[cfg(test)]
            hash_read_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn remember_incomplete_absent_write(
        &self,
        path: PathBuf,
        metadata: &fs::Metadata,
        content_hash: String,
        options: Option<VfsStorageWriteOptions>,
    ) {
        self.incomplete_absent_writes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                path,
                IncompleteAbsentWrite {
                    content_hash,
                    file_id: local_file_id(metadata),
                    options,
                },
            );
    }

    fn clear_incomplete_absent_write(&self, path: &Path) {
        self.incomplete_absent_writes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(path);
    }

    fn permits_incomplete_absent_replay(
        &self,
        path: &Path,
        content_hash: &str,
        options: Option<&VfsStorageWriteOptions>,
    ) -> VfsStorageResult<bool> {
        let Some(record) = self
            .incomplete_absent_writes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(path)
            .cloned()
        else {
            return Ok(false);
        };
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(VfsStorageError::Internal(error.to_string())),
        };
        Ok(record.content_hash == content_hash
            && record.file_id == local_file_id(&metadata)
            && record.options.as_ref() == options)
    }

    fn sync_file(&self, file: &fs::File, path: &Path) -> VfsStorageResult<()> {
        #[cfg(test)]
        if let Some(observer) = self.durability_sync_observer.as_ref() {
            (observer.0)(&DurabilitySyncEvent::File(path.to_path_buf()))?;
        }
        file.sync_all().map_err(|error| {
            VfsStorageError::Internal(format!("sync local VFS file {}: {error}", path.display()))
        })
    }

    fn sync_directory(&self, path: &Path) -> VfsStorageResult<()> {
        let directory = fs::File::open(path).map_err(|error| {
            VfsStorageError::Internal(format!(
                "open local VFS directory {} for sync: {error}",
                path.display()
            ))
        })?;
        self.sync_directory_handle(&directory, path)
    }

    fn sync_directory_handle(&self, directory: &fs::File, path: &Path) -> VfsStorageResult<()> {
        #[cfg(test)]
        if let Some(observer) = self.durability_sync_observer.as_ref() {
            (observer.0)(&DurabilitySyncEvent::Directory(path.to_path_buf()))?;
        }
        directory.sync_all().map_err(|error| {
            VfsStorageError::Internal(format!(
                "sync local VFS directory {}: {error}",
                path.display()
            ))
        })
    }

    async fn run_blocking<T>(
        &self,
        operation: impl FnOnce(Self) -> VfsStorageResult<T> + Send + 'static,
    ) -> VfsStorageResult<T>
    where
        T: Send + 'static,
    {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || operation(storage))
            .await
            .map_err(|error| {
                VfsStorageError::Internal(format!("local vfs blocking task failed: {error}"))
            })?
    }

    fn abs_path(&self, logical_path: &str) -> VfsStorageResult<PathBuf> {
        let logical_path = logical_path.trim_matches('/');
        let mut out = self.root.clone();
        if logical_path.is_empty() {
            return Ok(out);
        }
        for component in Path::new(logical_path).components() {
            match component {
                Component::Normal(part) => out.push(part),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(VfsStorageError::BadRequest(format!(
                        "invalid vfs path: {logical_path}"
                    )));
                }
            }
        }
        Ok(out)
    }

    fn logical_path_for(&self, abs_path: &Path) -> VfsStorageResult<String> {
        let rel = abs_path.strip_prefix(&self.root).map_err(|err| {
            VfsStorageError::Internal(format!("local path escaped vfs root: {err}"))
        })?;
        Ok(rel
            .components()
            .filter_map(|component| match component {
                Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("/"))
    }

    fn assert_no_symlink_ancestor(&self, abs_path: &Path) -> VfsStorageResult<()> {
        let rel = abs_path.strip_prefix(&self.root).map_err(|err| {
            VfsStorageError::Internal(format!("local path escaped vfs root: {err}"))
        })?;
        let mut current = self.root.clone();
        let mut components = rel.components().peekable();
        while let Some(component) = components.next() {
            let Component::Normal(part) = component else {
                continue;
            };
            current.push(part);
            if components.peek().is_none() {
                break;
            }
            let metadata = match fs::symlink_metadata(&current) {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(err) => return Err(VfsStorageError::Internal(err.to_string())),
            };
            if metadata.file_type().is_symlink() {
                return Err(VfsStorageError::BadRequest(
                    "unsupported file type: symlink".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn metadata_for_abs(&self, abs_path: &Path) -> VfsStorageResult<Option<VfsStorageMetadata>> {
        self.metadata_for_abs_with_hash_limit(abs_path, None)
    }

    fn metadata_for_abs_with_hash_limit(
        &self,
        abs_path: &Path,
        max_hash_bytes: Option<u64>,
    ) -> VfsStorageResult<Option<VfsStorageMetadata>> {
        self.assert_no_symlink_ancestor(abs_path)?;
        let metadata = match fs::symlink_metadata(abs_path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(VfsStorageError::Internal(err.to_string())),
        };
        let file_type = metadata.file_type();
        let kind = if file_type.is_symlink() {
            VfsStorageEntryKind::Symlink
        } else if metadata.is_dir() {
            VfsStorageEntryKind::Directory
        } else if metadata.is_file() {
            VfsStorageEntryKind::File
        } else {
            VfsStorageEntryKind::Special
        };
        let link_target = if kind == VfsStorageEntryKind::Symlink {
            Some(self.validate_symlink_target(abs_path)?.target_text)
        } else {
            None
        };
        let content_hash = if kind == VfsStorageEntryKind::File {
            self.hash_file_for_metadata(abs_path, &metadata, max_hash_bytes)?
        } else {
            None
        };
        let object_state = (kind == VfsStorageEntryKind::File).then(|| VfsStorageObjectState {
            size_bytes: metadata.len(),
            pack_key: format!("local://{}", abs_path.display()),
            pack_slot_offset: 0,
            pack_slot_length: metadata.len() as i64,
            pack_slot_compression: SlotCompression::Raw.as_db_smallint(),
        });
        Ok(Some(VfsStorageMetadata {
            path: self.logical_path_for(abs_path)?,
            kind,
            size_bytes: metadata.len(),
            file_id: local_file_id(&metadata),
            link_count: local_link_count(&metadata),
            link_target,
            mode: posix_mode_from_metadata(&metadata),
            executable: executable_from_metadata(&metadata, kind),
            content_hash,
            token_count: None,
            version: None,
            updated_at: modified_at(&metadata),
            object_state,
        }))
    }

    fn metadata_for_path(&self, path: &str) -> VfsStorageResult<Option<VfsStorageMetadata>> {
        let abs_path = self.abs_path(path)?;
        self.metadata_for_abs(&abs_path)
    }

    fn validate_symlink_target(&self, link_path: &Path) -> VfsStorageResult<SymlinkTargetInfo> {
        let target = fs::read_link(link_path).map_err(|_| unsupported_symlink_error())?;
        self.validate_symlink_target_text(link_path, &target)
    }

    fn validate_symlink_target_text(
        &self,
        link_path: &Path,
        target: &Path,
    ) -> VfsStorageResult<SymlinkTargetInfo> {
        let target_text = target.to_string_lossy().into_owned();
        if target_text.is_empty() {
            return Err(unsupported_symlink_error());
        }
        let lexical_root = self.lexical_root()?;
        let canonical_root =
            fs::canonicalize(&self.root).map_err(|_| unsupported_symlink_error())?;
        let link_rel = link_path
            .strip_prefix(&self.root)
            .map_err(|_| unsupported_symlink_error())?;
        let link_parent_rel = link_rel.parent().unwrap_or_else(|| Path::new(""));
        let link_parent = lexical_root.join(link_parent_rel);
        let resolved = if target.is_absolute() {
            lexical_normalize(target)
        } else {
            lexical_normalize(&link_parent.join(target))
        };
        if !resolved.starts_with(&lexical_root) {
            return Err(unsupported_symlink_error());
        }
        match fs::metadata(&resolved) {
            Ok(metadata) => {
                let canonical_target =
                    fs::canonicalize(&resolved).map_err(|_| unsupported_symlink_error())?;
                if !canonical_target.starts_with(&canonical_root) {
                    return Err(unsupported_symlink_error());
                }
                if !metadata.is_file() && !metadata.is_dir() {
                    return Err(unsupported_symlink_error());
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(unsupported_symlink_error()),
        }
        let resolved_rel = resolved
            .strip_prefix(&lexical_root)
            .map_err(|_| unsupported_symlink_error())?;
        let normalized_target = relative_path_between(link_parent_rel, resolved_rel);
        Ok(SymlinkTargetInfo {
            target_text: normalized_target,
        })
    }

    fn lexical_root(&self) -> VfsStorageResult<PathBuf> {
        let root = if self.root.is_absolute() {
            self.root.clone()
        } else {
            std::env::current_dir()
                .map_err(|err| VfsStorageError::Internal(err.to_string()))?
                .join(&self.root)
        };
        Ok(lexical_normalize(&root))
    }

    fn write_precondition(&self, path: &str) -> VfsStorageResult<VfsStorageWritePrecondition> {
        let abs_path = self.abs_path(path)?;
        self.assert_no_symlink_ancestor(&abs_path)?;
        let fingerprint = match fs::symlink_metadata(&abs_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let target = self
                    .metadata_for_path(path)?
                    .and_then(|metadata| metadata.link_target)
                    .ok_or_else(|| {
                        VfsStorageError::Internal(format!(
                            "symlink metadata did not include target for {path}"
                        ))
                    })?;
                format!("symlink:{}", hex_hash(target.as_bytes()))
            }
            Ok(metadata) if metadata.is_file() => hash_file_if_present_uncached(self, &abs_path)?
                .unwrap_or_else(|| "absent".to_string()),
            Ok(_) => "absent".to_string(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => "absent".to_string(),
            Err(err) => return Err(VfsStorageError::Internal(err.to_string())),
        };
        Ok(VfsStorageWritePrecondition {
            predicate: Some(if fingerprint == "absent" {
                VfsStorageCasPredicate::Absent
            } else {
                VfsStorageCasPredicate::ContentFingerprint {
                    fingerprint: fingerprint.clone(),
                }
            }),
            // Rolling-upgrade projection for callers that still inspect the
            // legacy field. Comparisons below use the typed predicate.
            fingerprint: Some(fingerprint),
            secondary_fingerprint: None,
            expected_file_id: None,
        })
    }

    fn assert_precondition(
        &self,
        path: &str,
        precondition: Option<&VfsStorageWritePrecondition>,
    ) -> VfsStorageResult<()> {
        let Some(precondition) = precondition else {
            return Ok(());
        };
        self.assert_expected_file_id(path, precondition)?;
        let Some(expected) = precondition.effective_predicate() else {
            return Ok(());
        };
        let actual = self.write_precondition(path)?.effective_predicate();
        if actual.as_ref() == Some(&expected) {
            Ok(())
        } else {
            Err(VfsStorageError::Conflict(format!(
                "local vfs write precondition failed for {path}"
            )))
        }
    }

    fn assert_expected_file_id(
        &self,
        path: &str,
        precondition: &VfsStorageWritePrecondition,
    ) -> VfsStorageResult<()> {
        if let Some(expected_file_id) = precondition.expected_file_id.as_deref() {
            let abs_path = self.abs_path(path)?;
            self.assert_no_symlink_ancestor(&abs_path)?;
            let actual_file_id = match fs::symlink_metadata(&abs_path) {
                Ok(metadata) => local_file_id(&metadata),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(VfsStorageError::Internal(error.to_string())),
            };
            if actual_file_id.as_deref() != Some(expected_file_id) {
                return Err(VfsStorageError::Conflict(format!(
                    "local vfs write identity precondition failed for {path}"
                )));
            }
        }
        Ok(())
    }

    async fn lock_write_paths(&self, paths: impl IntoIterator<Item = String>) -> PathLocks {
        self.lock_paths(paths, PathLockMode::Write).await
    }

    async fn lock_read_paths(&self, paths: impl IntoIterator<Item = String>) -> PathLocks {
        self.lock_paths(paths, PathLockMode::Read).await
    }

    async fn lock_paths(
        &self,
        paths: impl IntoIterator<Item = String>,
        target_mode: PathLockMode,
    ) -> PathLocks {
        let mut keys = Vec::new();
        for path in paths {
            if let Ok(abs_path) = self.abs_path(&path) {
                let cached_file_id = self
                    .hash_cache
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .get(&abs_path)
                    .and_then(|entry| entry.file_id.clone());
                let live_file_id = fs::symlink_metadata(abs_path)
                    .ok()
                    .filter(|metadata| metadata.is_file())
                    .and_then(|metadata| local_file_id(&metadata));
                if let Some(file_id) = cached_file_id.as_deref() {
                    keys.push(format!("\0inode:{file_id}"));
                }
                if let Some(file_id) = live_file_id.as_deref() {
                    if cached_file_id.as_deref() != Some(file_id) {
                        keys.push(format!("\0inode:{file_id}"));
                    }
                }
            }
            keys.push(path);
        }
        match target_mode {
            PathLockMode::Read => self.path_locks.lock_read(keys).await,
            PathLockMode::Write => self.path_locks.lock(keys).await,
        }
    }

    fn hash_file_for_metadata(
        &self,
        path: &Path,
        metadata: &fs::Metadata,
        max_hash_bytes: Option<u64>,
    ) -> VfsStorageResult<Option<String>> {
        if !metadata.is_file() {
            self.invalidate_hash(path);
            return Ok(None);
        }
        if max_hash_bytes.is_some_and(|max| metadata.len() > max) {
            return Ok(None);
        }
        let mtime_ns = metadata_mtime_ns(metadata);
        let change_ns = metadata_change_ns(metadata);
        if let Some(cached) = self
            .hash_cache
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .get(path)
            .cloned()
        {
            let cache_fresh = SystemTime::now()
                .duration_since(cached.cached_at)
                .is_ok_and(|age| age < HASH_CACHE_MAX_AGE);
            if (trusted_write_cache_reusable(&cached) || !metadata_is_recent(metadata))
                && cache_fresh
                && cached.size_bytes == metadata.len()
                && cached.mtime_ns == mtime_ns
                && cached.change_ns == change_ns
            {
                return Ok(Some(cached.hash));
            }
        }
        let hash = hash_file_if_present_uncached(self, path)?;
        if let Some(hash) = hash.as_ref() {
            self.remember_observed_hash(path, metadata, hash.clone());
        } else {
            self.invalidate_hash(path);
        }
        Ok(hash)
    }

    fn remember_observed_hash(&self, path: &Path, metadata: &fs::Metadata, hash: String) {
        self.remember_hash(path, metadata, hash, false);
    }

    fn remember_written_hash(&self, path: &Path, metadata: &fs::Metadata, hash: String) {
        self.remember_hash(path, metadata, hash, true);
    }

    fn remember_hash(
        &self,
        path: &Path,
        metadata: &fs::Metadata,
        hash: String,
        trusted_write: bool,
    ) {
        let mut cache = self
            .hash_cache
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        cache.insert(
            path.to_path_buf(),
            CachedFileHash {
                file_id: local_file_id(metadata),
                size_bytes: metadata.len(),
                mtime_ns: metadata_mtime_ns(metadata),
                change_ns: metadata_change_ns(metadata),
                cached_at: SystemTime::now(),
                hash,
                trusted_write,
            },
        );
        if cache.len() > MAX_HASH_CACHE_ENTRIES {
            let target = MAX_HASH_CACHE_ENTRIES.saturating_mul(3) / 4;
            let mut oldest = cache
                .iter()
                .map(|(path, entry)| (path.clone(), entry.cached_at))
                .collect::<Vec<_>>();
            oldest.sort_unstable_by_key(|(_, cached_at)| *cached_at);
            for (path, _) in oldest.into_iter().take(cache.len().saturating_sub(target)) {
                cache.remove(&path);
            }
        }
    }

    fn invalidate_hash(&self, path: &Path) {
        self.hash_cache
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .remove(path);
    }

    fn invalidate_hash_identity(&self, metadata: &fs::Metadata) {
        let Some(identity) = local_file_id(metadata) else {
            return;
        };
        if local_link_count(metadata) <= 1 {
            return;
        }
        let mut cache = self
            .hash_cache
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        cache.retain(|_, entry| entry.file_id.as_deref() != Some(identity.as_str()));
    }

    #[cfg(test)]
    fn hash_read_count(&self) -> usize {
        self.hash_read_count.load(AtomicOrdering::SeqCst)
    }

    #[cfg(test)]
    fn expire_cached_hash(&self, path: &Path) {
        if let Some(cached) = self
            .hash_cache
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .get_mut(path)
        {
            cached.cached_at = SystemTime::UNIX_EPOCH;
        }
    }
}

#[async_trait::async_trait]
impl OptimizedVfsStorage for LocalVfsStorage {
    fn backend_name(&self) -> &'static str {
        "local"
    }

    async fn stat(&self, path: &str) -> VfsStorageResult<Option<VfsStorageMetadata>> {
        let path = path.to_string();
        let _locks = self.lock_read_paths([path.clone()]).await;
        self.run_blocking(move |storage| storage.metadata_for_path(&path))
            .await
    }

    async fn stat_with_metadata_fields(
        &self,
        path: &str,
        fields: VfsStorageMetadataFields,
    ) -> VfsStorageResult<Option<VfsStorageMetadata>> {
        let path = path.to_string();
        let _locks = self.lock_read_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            storage.metadata_for_abs_with_hash_limit(&abs_path, fields.max_hash_bytes)
        })
        .await
    }

    async fn metadata_many(
        &self,
        paths: &[String],
        fields: VfsStorageMetadataFields,
    ) -> VfsStorageResult<Vec<Option<VfsStorageMetadata>>> {
        let paths = paths.to_vec();
        let _locks = self.lock_read_paths(paths.clone()).await;
        self.run_blocking(move |storage| {
            paths
                .iter()
                .map(|path| {
                    let abs_path = storage.abs_path(path)?;
                    storage.metadata_for_abs_with_hash_limit(&abs_path, fields.max_hash_bytes)
                })
                .collect()
        })
        .await
    }

    async fn list_dir_with_metadata(
        &self,
        path: &str,
        filter: VfsStorageDirListFilter,
    ) -> VfsStorageResult<Vec<VfsStorageMetadata>> {
        let path = path.to_string();
        let _locks = self.lock_read_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            let directory_metadata = match fs::symlink_metadata(&abs_path) {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Err(VfsStorageError::NotFound(path));
                }
                Err(err) => return Err(VfsStorageError::Internal(err.to_string())),
            };
            if directory_metadata.file_type().is_symlink()
                || (!directory_metadata.is_file() && !directory_metadata.is_dir())
            {
                return Ok(Vec::new());
            }
            if !directory_metadata.is_dir() {
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs path {path} is not a directory"
                )));
            }
            let read_dir = fs::read_dir(&abs_path).map_err(|err| match err.kind() {
                std::io::ErrorKind::NotFound => VfsStorageError::NotFound(path.clone()),
                _ => VfsStorageError::Internal(err.to_string()),
            })?;
            let mut entries = Vec::new();
            for entry in read_dir {
                let entry = entry.map_err(|err| VfsStorageError::Internal(err.to_string()))?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if !filter_name(&name, &filter) {
                    continue;
                }
                let metadata = match storage
                    .metadata_for_abs_with_hash_limit(&entry.path(), filter.max_hash_bytes)
                {
                    Ok(Some(metadata)) => metadata,
                    Ok(None) => continue,
                    Err(err) if is_excluded_listing_error(&err) => continue,
                    Err(err) => return Err(err),
                };
                if is_excluded_listing_kind(metadata.kind) {
                    continue;
                }
                if let Some(kind) = filter.entry_kind {
                    if metadata.kind != kind {
                        continue;
                    }
                }
                entries.push(metadata);
            }
            sort_entries(&mut entries, filter.order);
            if let Some(limit) = filter.limit {
                entries.truncate(limit.max(0) as usize);
            }
            Ok(entries)
        })
        .await
    }

    async fn list_subtree_file_metadata(
        &self,
        prefix: &str,
        options: VfsStorageSubtreeOptions,
    ) -> VfsStorageResult<Vec<VfsStorageMetadata>> {
        let prefix = prefix.to_string();
        let _locks = self.lock_read_paths([prefix.clone()]).await;
        self.run_blocking(move |storage| {
            let root = storage.abs_path(&prefix)?;
            storage.assert_no_symlink_ancestor(&root)?;
            let mut stack = vec![root];
            let mut out = Vec::new();
            while let Some(path) = stack.pop() {
                let metadata = match fs::symlink_metadata(&path) {
                    Ok(metadata) => metadata,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(VfsStorageError::Internal(err.to_string())),
                };
                let file_type = metadata.file_type();
                if file_type.is_symlink() {
                    match storage.metadata_for_abs(&path) {
                        Ok(Some(link_metadata)) => out.push(link_metadata),
                        Ok(None) => {}
                        Err(err) if is_excluded_listing_error(&err) => {}
                        Err(err) => return Err(err),
                    }
                    if let Some(limit) = options.limit {
                        if out.len() >= limit.max(0) as usize {
                            break;
                        }
                    }
                    continue;
                }
                if !metadata.is_file() && !metadata.is_dir() {
                    continue;
                }
                if metadata.is_dir() {
                    for entry in fs::read_dir(&path)
                        .map_err(|err| VfsStorageError::Internal(err.to_string()))?
                    {
                        stack.push(
                            entry
                                .map_err(|err| VfsStorageError::Internal(err.to_string()))?
                                .path(),
                        );
                    }
                    continue;
                }
                if metadata.is_file() {
                    if let Some(file_metadata) =
                        storage.metadata_for_abs_with_hash_limit(&path, options.max_hash_bytes)?
                    {
                        out.push(file_metadata);
                    }
                }
                if let Some(limit) = options.limit {
                    if out.len() >= limit.max(0) as usize {
                        break;
                    }
                }
            }
            out.sort_by(|a, b| a.path.cmp(&b.path));
            Ok(out)
        })
        .await
    }

    async fn read(&self, path: &str) -> VfsStorageResult<Bytes> {
        let path = path.to_string();
        let _locks = self.lock_read_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            read_file(&abs_path).map(Bytes::from)
        })
        .await
    }

    async fn read_range(&self, path: &str, range: VfsStorageReadRange) -> VfsStorageResult<Bytes> {
        let path = path.to_string();
        let _locks = self.lock_read_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            let mut file = open_regular_file(&abs_path)?;
            file.seek(SeekFrom::Start(range.offset))
                .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
            let mut bytes = Vec::with_capacity(range.length as usize);
            file.take(range.length)
                .read_to_end(&mut bytes)
                .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
            Ok(Bytes::from(bytes))
        })
        .await
    }

    async fn read_many(&self, paths: &[String]) -> VfsStorageResult<Vec<(String, Bytes)>> {
        let paths = paths.to_vec();
        let _locks = self.lock_read_paths(paths.clone()).await;
        self.run_blocking(move |storage| {
            let mut out = Vec::with_capacity(paths.len());
            for path in paths {
                let abs_path = storage.abs_path(&path)?;
                storage.assert_no_symlink_ancestor(&abs_path)?;
                match read_file(&abs_path).map(Bytes::from) {
                    Ok(bytes) => out.push((path, bytes)),
                    Err(VfsStorageError::NotFound(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            Ok(out)
        })
        .await
    }

    async fn read_many_if_etag_mismatch(
        &self,
        requests: &[VfsStorageReadIfChanged],
    ) -> VfsStorageResult<Vec<VfsStorageReadIfChangedResult>> {
        let requests = requests.to_vec();
        let _locks = self
            .lock_read_paths(requests.iter().map(|request| request.path.clone()))
            .await;
        self.run_blocking(move |storage| {
            let mut out = Vec::with_capacity(requests.len());
            for request in requests {
                let metadata = storage.metadata_for_path(&request.path)?;
                let Some(metadata) = metadata else {
                    out.push(VfsStorageReadIfChangedResult {
                        path: request.path,
                        content_hash: None,
                        bytes: None,
                    });
                    continue;
                };
                let hash = metadata.content_hash.clone();
                if hash == request.known_content_hash {
                    out.push(VfsStorageReadIfChangedResult {
                        path: request.path,
                        content_hash: hash,
                        bytes: None,
                    });
                    continue;
                }
                let abs_path = storage.abs_path(&request.path)?;
                storage.assert_no_symlink_ancestor(&abs_path)?;
                out.push(VfsStorageReadIfChangedResult {
                    path: request.path,
                    content_hash: hash,
                    bytes: Some(Bytes::from(read_file(&abs_path)?)),
                });
            }
            Ok(out)
        })
        .await
    }

    async fn write(
        &self,
        path: &str,
        bytes: Bytes,
        precondition: Option<VfsStorageWritePrecondition>,
    ) -> VfsStorageResult<VfsStorageWriteResult> {
        self.write_with_options(path, bytes, precondition, None)
            .await
    }

    async fn write_with_options(
        &self,
        path: &str,
        bytes: Bytes,
        precondition: Option<VfsStorageWritePrecondition>,
        options: Option<VfsStorageWriteOptions>,
    ) -> VfsStorageResult<VfsStorageWriteResult> {
        let write = VfsStorageWrite {
            path: path.to_string(),
            bytes,
            token_count: None,
            precondition,
        };
        let _locks = self.lock_write_paths([write.path.clone()]).await;
        self.run_blocking(move |storage| {
            let (pending, replays) =
                partition_conditional_write_replays(&storage, vec![(write, options)])?;
            let mut result = complete_exact_write_replays(&storage, &replays)?;
            result.extend(install_writes_with_options(&storage, pending)?);
            result
                .into_iter()
                .next()
                .ok_or_else(|| VfsStorageError::Internal("write returned no result".to_string()))
        })
        .await
    }

    async fn write_from_local_file(
        &self,
        path: &str,
        source_path: &Path,
        expected_content_hash: Option<&str>,
        precondition: Option<VfsStorageWritePrecondition>,
        options: Option<VfsStorageWriteOptions>,
    ) -> VfsStorageResult<VfsStorageWriteResult> {
        let path = path.to_string();
        let source_path = source_path.to_path_buf();
        let expected_content_hash = expected_content_hash.map(str::to_string);
        let _locks = self.lock_write_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            assert_supported_read_target(&source_path)?;
            if precondition.is_some() {
                let desired_hash = hash_regular_file(&source_path)?;
                if expected_content_hash
                    .as_deref()
                    .is_some_and(|expected| expected != desired_hash)
                {
                    return Err(VfsStorageError::Conflict(format!(
                        "staged VFS upload hash mismatch for {path}"
                    )));
                }
                let replay = ExactWriteReplay {
                    destination: storage.abs_path(&path)?,
                    path: path.clone(),
                    content_hash: desired_hash,
                    requested_options: options,
                };
                match storage.assert_precondition(&path, precondition.as_ref()) {
                    Ok(()) => {}
                    Err(conflict @ VfsStorageError::Conflict(_)) => {
                        if let Some(precondition) = precondition.as_ref() {
                            storage.assert_expected_file_id(&path, precondition)?;
                        }
                        if !precondition_allows_exact_replay(precondition.as_ref())
                            && !storage.permits_incomplete_absent_replay(
                                &replay.destination,
                                &replay.content_hash,
                                replay.requested_options.as_ref(),
                            )?
                        {
                            return Err(conflict);
                        }
                        if open_exact_write_replay_target(&storage, &replay)?.is_none() {
                            return Err(conflict);
                        }
                        return complete_exact_write_replays(&storage, &[replay])?
                            .into_iter()
                            .next()
                            .ok_or_else(|| {
                                VfsStorageError::Internal(
                                    "write replay returned no result".to_string(),
                                )
                            });
                    }
                    Err(error) => return Err(error),
                }
            }

            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            let previous_hash = storage
                .metadata_for_abs(&abs_path)?
                .and_then(|metadata| metadata.content_hash);
            let previous_mode = if options.is_some() {
                existing_regular_file_mode(&abs_path)?
            } else {
                None
            };
            if let Some(parent) = abs_path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
            }
            let tmp_path = abs_path.with_file_name(format!(
                ".{}.{}.tmp",
                abs_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("vfs"),
                Uuid::new_v4()
            ));

            let install = (|| -> VfsStorageResult<(String, bool)> {
                let mut source = open_regular_file(&source_path)?;
                let mut staged = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&tmp_path)
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                let mut hasher = sha2::Sha256::new();
                let mut buffer = vec![0_u8; 1024 * 1024];
                loop {
                    let read = source
                        .read(&mut buffer)
                        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..read]);
                    staged
                        .write_all(&buffer[..read])
                        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                }
                let content_hash = format!("{:x}", hasher.finalize());
                if expected_content_hash
                    .as_deref()
                    .is_some_and(|expected| expected != content_hash)
                {
                    return Err(VfsStorageError::Conflict(format!(
                        "staged VFS upload hash mismatch for {path}"
                    )));
                }
                apply_write_options(&tmp_path, options.as_ref(), previous_mode)?;
                storage.sync_file(&staged, &tmp_path)?;
                storage.assert_precondition(&path, precondition.as_ref())?;
                let sync_target = install_staged_file_preserving_identity(
                    &storage, &tmp_path, &staged, &abs_path,
                )?;
                let published_absent_write =
                    sync_target.is_none() && precondition_expects_absent(precondition.as_ref());
                if published_absent_write {
                    let metadata = fs::symlink_metadata(&abs_path)
                        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                    storage.remember_incomplete_absent_write(
                        abs_path.clone(),
                        &metadata,
                        content_hash.clone(),
                        options.clone(),
                    );
                }
                if let Some(sync_target) = sync_target {
                    apply_write_options(&abs_path, options.as_ref(), previous_mode)?;
                    storage.sync_file(&sync_target, &abs_path)?;
                }
                if let Some(parent) = abs_path.parent() {
                    let mut touched_directories = HashSet::new();
                    collect_directory_chain(&storage.root, parent, &mut touched_directories)?;
                    sync_directories_deepest_first(&storage, touched_directories)?;
                }
                Ok((content_hash, published_absent_write))
            })();

            let (content_hash, published_absent_write) = match install {
                Ok(result) => result,
                Err(error) => {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(error);
                }
            };
            let metadata = fs::symlink_metadata(&abs_path)
                .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
            storage.remember_written_hash(&abs_path, &metadata, content_hash.clone());
            if published_absent_write {
                storage.clear_incomplete_absent_write(&abs_path);
            }
            Ok(VfsStorageWriteResult {
                path,
                changed: previous_hash.as_deref() != Some(content_hash.as_str()),
                previous_hash,
                content_hash,
            })
        })
        .await
    }

    async fn write_many_atomic(
        &self,
        writes: Vec<VfsStorageWrite>,
    ) -> VfsStorageResult<Vec<VfsStorageWriteResult>> {
        let _locks = self
            .lock_write_paths(writes.iter().map(|write| write.path.clone()))
            .await;
        self.run_blocking(move |storage| {
            let order = writes
                .iter()
                .map(|write| write.path.clone())
                .collect::<Vec<_>>();
            let (pending, replays) = partition_conditional_write_replays(
                &storage,
                writes.into_iter().map(|write| (write, None)).collect(),
            )?;
            let mut results = complete_exact_write_replays(&storage, &replays)?;
            results.extend(install_writes_with_options(&storage, pending)?);
            write_results_in_order(&order, results)
        })
        .await
    }

    async fn write_many_if_changed_atomic(
        &self,
        writes: Vec<VfsStorageWrite>,
    ) -> VfsStorageResult<Vec<VfsStorageWriteResult>> {
        let _locks = self
            .lock_write_paths(writes.iter().map(|write| write.path.clone()))
            .await;
        self.run_blocking(move |storage| {
            let (pending, replays) = partition_conditional_write_replays(
                &storage,
                writes.into_iter().map(|write| (write, None)).collect(),
            )?;
            let mut replays = replays;
            let mut changed = Vec::new();
            for (write, _) in pending {
                let next_hash = hex_hash(&write.bytes);
                let replay = ExactWriteReplay {
                    destination: storage.abs_path(&write.path)?,
                    path: write.path.clone(),
                    content_hash: next_hash,
                    requested_options: None,
                };
                if open_exact_write_replay_target(&storage, &replay)?.is_some() {
                    replays.push(replay);
                } else {
                    changed.push(write);
                }
            }
            let mut out = complete_exact_write_replays(&storage, &replays)?;
            out.extend(install_writes(&storage, changed)?);
            out.sort_by(|a, b| a.path.cmp(&b.path));
            Ok(out)
        })
        .await
    }

    async fn mkdir(&self, path: &str) -> VfsStorageResult<()> {
        self.mkdir_with_mode(path, None).await
    }

    async fn mkdir_with_mode(&self, path: &str, mode: Option<u32>) -> VfsStorageResult<()> {
        let path = path.to_string();
        let _locks = self.lock_write_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            fs::create_dir_all(&abs_path)
                .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
            let sync_target = mode.and_then(|_| open_mode_sync_target(&abs_path).ok());
            apply_directory_mode(&abs_path, mode)?;
            if mode.is_some() {
                sync_mode_target(&storage, &abs_path, sync_target)?;
                sync_directory_chains(&storage, abs_path.parent().map(Path::to_path_buf))
            } else {
                sync_directory_chains(&storage, [abs_path])
            }
        })
        .await
    }

    async fn set_mode(&self, path: &str, mode: u32) -> VfsStorageResult<()> {
        let path = path.to_string();
        let _locks = self.lock_write_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            let sync_target = open_mode_sync_target(&abs_path).ok();
            apply_exact_mode(&abs_path, mode, false)?;
            sync_mode_target(&storage, &abs_path, sync_target)
        })
        .await
    }

    async fn create_symlink(&self, path: &str, target: &str) -> VfsStorageResult<()> {
        let path = path.to_string();
        let target = target.to_string();
        let _locks = self.lock_write_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            let canonical = storage.validate_symlink_target_text(&abs_path, Path::new(&target))?;
            match storage.metadata_for_path(&path)? {
                Some(metadata)
                    if metadata.kind == VfsStorageEntryKind::Symlink
                        && metadata.link_target.as_deref()
                            == Some(canonical.target_text.as_str()) => {}
                Some(_) => {
                    return Err(VfsStorageError::Conflict(format!(
                        "vfs path {path} already exists"
                    )));
                }
                None => create_symlink_impl(&storage, &path, &target)?,
            }
            sync_directory_chains(&storage, abs_path.parent().map(Path::to_path_buf))
        })
        .await
    }

    async fn create_hard_link(
        &self,
        source: &str,
        destination: &str,
    ) -> VfsStorageResult<VfsStorageHardLinkResult> {
        let source = source.to_string();
        let destination = destination.to_string();
        let _locks = self
            .lock_write_paths([source.clone(), destination.clone()])
            .await;
        self.run_blocking(move |storage| {
            let source_abs = storage.abs_path(&source)?;
            let destination_abs = storage.abs_path(&destination)?;
            storage.assert_no_symlink_ancestor(&source_abs)?;
            storage.assert_no_symlink_ancestor(&destination_abs)?;
            let source_before = fs::symlink_metadata(&source_abs).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    VfsStorageError::NotFound(source.clone())
                } else {
                    VfsStorageError::Internal(error.to_string())
                }
            })?;
            if !source_before.is_file() {
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs hard-link source {source} is not a regular file"
                )));
            }
            if let Ok(destination_before) = fs::symlink_metadata(&destination_abs) {
                if !destination_before.is_file()
                    || local_file_id(&source_before) != local_file_id(&destination_before)
                    || local_file_id(&source_before).is_none()
                {
                    return Err(VfsStorageError::Conflict(format!(
                        "vfs hard-link destination already exists: {destination}"
                    )));
                }
                sync_directory_chains(&storage, destination_abs.parent().map(Path::to_path_buf))?;
                let source = storage.metadata_for_abs(&source_abs)?.ok_or_else(|| {
                    VfsStorageError::Internal(
                        "hard-link source disappeared during replay".to_string(),
                    )
                })?;
                let destination = storage.metadata_for_abs(&destination_abs)?.ok_or_else(|| {
                    VfsStorageError::Internal(
                        "hard-link destination disappeared during replay".to_string(),
                    )
                })?;
                return Ok(VfsStorageHardLinkResult {
                    source,
                    destination,
                });
            }
            if let Some(parent) = destination_abs.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
            }
            fs::hard_link(&source_abs, &destination_abs).map_err(|error| match error.kind() {
                std::io::ErrorKind::AlreadyExists => VfsStorageError::Conflict(format!(
                    "vfs hard-link destination already exists: {destination}"
                )),
                std::io::ErrorKind::NotFound => VfsStorageError::NotFound(source.clone()),
                _ => VfsStorageError::Internal(error.to_string()),
            })?;
            sync_directory_chains(&storage, destination_abs.parent().map(Path::to_path_buf))?;
            storage.invalidate_hash_identity(&source_before);
            let source = storage.metadata_for_abs(&source_abs)?.ok_or_else(|| {
                VfsStorageError::Internal("hard-link source disappeared after link".to_string())
            })?;
            let destination = storage.metadata_for_abs(&destination_abs)?.ok_or_else(|| {
                VfsStorageError::Internal(
                    "hard-link destination disappeared after link".to_string(),
                )
            })?;
            Ok(VfsStorageHardLinkResult {
                source,
                destination,
            })
        })
        .await
    }

    async fn find_hard_link_alias(
        &self,
        file_id: &str,
        excluding_path: &str,
    ) -> VfsStorageResult<Option<String>> {
        let file_id = file_id.to_string();
        let excluding_path = excluding_path.trim_matches('/').to_string();
        self.run_blocking(move |storage| {
            let mut stack = vec![storage.root.clone()];
            while let Some(directory) = stack.pop() {
                for entry in fs::read_dir(&directory)
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?
                {
                    let entry =
                        entry.map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                    let metadata = fs::symlink_metadata(entry.path())
                        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                    if metadata.is_dir() {
                        stack.push(entry.path());
                    } else if metadata.is_file()
                        && local_file_id(&metadata).as_deref() == Some(file_id.as_str())
                    {
                        let logical = storage.logical_path_for(&entry.path())?;
                        if logical != excluding_path {
                            return Ok(Some(logical));
                        }
                    }
                }
            }
            Ok(None)
        })
        .await
    }

    async fn delete_file_with_metadata(
        &self,
        path: &str,
        precondition: Option<VfsStorageWritePrecondition>,
    ) -> VfsStorageResult<VfsStorageDeleteResult> {
        let path = path.to_string();
        let _locks = self.lock_write_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            let previous = storage.metadata_for_path(&path)?;
            if previous.is_none() {
                sync_directory_chains(&storage, abs_path.parent().map(Path::to_path_buf))?;
                return Ok(VfsStorageDeleteResult { previous });
            }
            storage.assert_precondition(&path, precondition.as_ref())?;
            if matches!(
                previous.as_ref().map(|metadata| metadata.kind),
                Some(VfsStorageEntryKind::Directory)
            ) {
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs path {path} is not a file"
                )));
            }
            storage.assert_no_symlink_ancestor(&abs_path)?;
            let removed_identity = fs::symlink_metadata(&abs_path).ok();
            match fs::remove_file(&abs_path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(VfsStorageError::Internal(err.to_string())),
            }
            sync_directory_chains(&storage, abs_path.parent().map(Path::to_path_buf))?;
            if let Some(metadata) = removed_identity.as_ref() {
                storage.invalidate_hash_identity(&metadata);
            } else {
                storage.invalidate_hash(&abs_path);
            }
            Ok(VfsStorageDeleteResult { previous })
        })
        .await
    }

    async fn rmdir(&self, path: &str) -> VfsStorageResult<()> {
        let path = path.to_string();
        let _locks = self.lock_write_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            let Some(metadata) = storage.metadata_for_path(&path)? else {
                sync_directory_chains(&storage, abs_path.parent().map(Path::to_path_buf))?;
                return Ok(());
            };
            if metadata.kind != VfsStorageEntryKind::Directory {
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs path {path} is not a directory"
                )));
            }
            storage.assert_no_symlink_ancestor(&abs_path)?;
            match fs::remove_dir(&abs_path) {
                Ok(()) => sync_directory_chains(&storage, abs_path.parent().map(Path::to_path_buf)),
                Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => Err(
                    VfsStorageError::Conflict(format!("vfs directory {path} is not empty")),
                ),
                Err(err) => Err(VfsStorageError::Internal(err.to_string())),
            }
        })
        .await
    }

    async fn rename_with_metadata(
        &self,
        from: &str,
        to: &str,
    ) -> VfsStorageResult<VfsStorageRenameResult> {
        let from = from.to_string();
        let to = to.to_string();
        let _locks = self.lock_write_paths([from.clone(), to.clone()]).await;
        self.run_blocking(move |storage| {
            let from_abs = storage.abs_path(&from)?;
            storage.assert_no_symlink_ancestor(&from_abs)?;
            let to_abs = storage.abs_path(&to)?;
            storage.assert_no_symlink_ancestor(&to_abs)?;
            let previous = storage.metadata_for_path(&from)?;
            if previous.is_none() {
                sync_directory_chains(
                    &storage,
                    [from_abs.parent(), to_abs.parent()]
                        .into_iter()
                        .flatten()
                        .map(Path::to_path_buf),
                )?;
                let current = storage.metadata_for_abs(&to_abs)?;
                return Ok(VfsStorageRenameResult { previous, current });
            }
            if let Some(parent) = to_abs.parent() {
                fs::create_dir_all(parent)
                    .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
            }
            fs::rename(&from_abs, &to_abs)
                .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
            sync_directory_chains(
                &storage,
                [from_abs.parent(), to_abs.parent()]
                    .into_iter()
                    .flatten()
                    .map(Path::to_path_buf),
            )?;
            storage.invalidate_hash(&from_abs);
            storage.invalidate_hash(&to_abs);
            let current = storage.metadata_for_abs(&to_abs)?;
            Ok(VfsStorageRenameResult { previous, current })
        })
        .await
    }

    async fn apply_namespace_batch(
        &self,
        mutations: Vec<VfsStorageNamespaceMutation>,
    ) -> VfsStorageResult<()> {
        let lock_paths = mutations
            .iter()
            .flat_map(VfsStorageNamespaceMutation::paths)
            .filter(|path| !path.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        let _locks = self.lock_write_paths(lock_paths).await;
        self.run_blocking(move |storage| {
            // Namespace batches are replayable, but delete preconditions must be
            // checked as one snapshot before the first path is removed. Holding all
            // path locks across this preflight and apply phase makes a conditional
            // delete-only batch all-or-none with respect to concurrent VFS writers.
            for mutation in &mutations {
                if let VfsStorageNamespaceMutation::DeleteFile { path, precondition } = mutation {
                    let current = storage.metadata_for_path(path.as_str())?;
                    if current.is_some() {
                        storage.assert_precondition(path.as_str(), precondition.as_ref())?;
                    }
                    if matches!(
                        current.as_ref().map(|metadata| metadata.kind),
                        Some(VfsStorageEntryKind::Directory)
                    ) {
                        return Err(VfsStorageError::BadRequest(format!(
                            "vfs path {path} is not a file"
                        )));
                    }
                }
            }

            let mut touched_directories = HashSet::new();
            for mutation in mutations {
                match mutation {
                    VfsStorageNamespaceMutation::CreateDirectory { path, mode } => {
                        let abs_path = storage.abs_path(path.as_str())?;
                        storage.assert_no_symlink_ancestor(&abs_path)?;
                        fs::create_dir_all(&abs_path).map_err(|error| {
                            // A file occupying the leaf or an ancestor can
                            // never resolve by retrying: report Conflict so
                            // replaying journals dead-letter instead of
                            // retrying a deterministic failure forever.
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::AlreadyExists
                                    | std::io::ErrorKind::NotADirectory
                            ) {
                                VfsStorageError::Conflict(format!(
                                    "vfs path {path} is blocked by a non-directory: {error}"
                                ))
                            } else {
                                VfsStorageError::Internal(error.to_string())
                            }
                        })?;
                        let sync_target =
                            mode.and_then(|_| open_mode_sync_target(&abs_path).ok());
                        apply_directory_mode(&abs_path, mode)?;
                        if mode.is_some() {
                            sync_mode_target(&storage, &abs_path, sync_target)?;
                            if let Some(parent) = abs_path.parent() {
                                collect_directory_chain(
                                    &storage.root,
                                    parent,
                                    &mut touched_directories,
                                )?;
                            }
                        } else {
                            collect_directory_chain(
                                &storage.root,
                                &abs_path,
                                &mut touched_directories,
                            )?;
                        }
                    }
                    VfsStorageNamespaceMutation::CreateSymlink { path, target } => {
                        let abs_path = storage.abs_path(path.as_str())?;
                        let canonical = storage
                            .validate_symlink_target_text(&abs_path, Path::new(target.as_str()))?;
                        match storage.metadata_for_path(path.as_str())? {
                            Some(metadata)
                                if metadata.kind == VfsStorageEntryKind::Symlink
                                    && metadata.link_target.as_deref()
                                        == Some(canonical.target_text.as_str()) => {}
                            Some(_) => {
                                return Err(VfsStorageError::Conflict(format!(
                                    "vfs path {path} already exists"
                                )));
                            }
                            None => {
                                create_symlink_impl(&storage, path.as_str(), target.as_str())?
                            }
                        }
                        if let Some(parent) = abs_path.parent() {
                            collect_directory_chain(
                                &storage.root,
                                parent,
                                &mut touched_directories,
                            )?;
                        }
                    }
                    VfsStorageNamespaceMutation::DeleteFile {
                        path,
                        precondition: _,
                    } => {
                        let abs_path = storage.abs_path(path.as_str())?;
                        storage.assert_no_symlink_ancestor(&abs_path)?;
                        if let Some(parent) = abs_path.parent() {
                            collect_directory_chain(
                                &storage.root,
                                parent,
                                &mut touched_directories,
                            )?;
                        }
                        match fs::remove_file(&abs_path) {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) if error.kind() == std::io::ErrorKind::IsADirectory => {
                                return Err(VfsStorageError::Conflict(format!(
                                    "vfs path {path} is a directory, not a file"
                                )));
                            }
                            Err(error) => {
                                return Err(VfsStorageError::Internal(error.to_string()));
                            }
                        }
                        storage.invalidate_hash(&abs_path);
                    }
                    VfsStorageNamespaceMutation::RemoveDirectory { path } => {
                        let abs_path = storage.abs_path(path.as_str())?;
                        storage.assert_no_symlink_ancestor(&abs_path)?;
                        if let Some(parent) = abs_path.parent() {
                            collect_directory_chain(
                                &storage.root,
                                parent,
                                &mut touched_directories,
                            )?;
                        }
                        match fs::remove_dir(&abs_path) {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                                return Err(VfsStorageError::Conflict(format!(
                                    "vfs directory {path} is not empty"
                                )));
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::NotADirectory => {
                                return Err(VfsStorageError::Conflict(format!(
                                    "vfs path {path} is not a directory"
                                )));
                            }
                            Err(error) => {
                                return Err(VfsStorageError::Internal(error.to_string()));
                            }
                        }
                    }
                    VfsStorageNamespaceMutation::SetMode { path, mode } => {
                        let abs_path = storage.abs_path(path.as_str())?;
                        storage.assert_no_symlink_ancestor(&abs_path)?;
                        let sync_target = open_mode_sync_target(&abs_path).ok();
                        if apply_exact_mode(&abs_path, mode, true)? {
                            sync_mode_target(&storage, &abs_path, sync_target)?;
                        }
                    }
                    VfsStorageNamespaceMutation::Rename { from, to } => {
                        let from_abs = storage.abs_path(from.as_str())?;
                        let to_abs = storage.abs_path(to.as_str())?;
                        storage.assert_no_symlink_ancestor(&from_abs)?;
                        storage.assert_no_symlink_ancestor(&to_abs)?;
                        for parent in [from_abs.parent(), to_abs.parent()]
                            .into_iter()
                            .flatten()
                        {
                            collect_directory_chain(
                                &storage.root,
                                parent,
                                &mut touched_directories,
                            )?;
                        }
                        remap_touched_directories_after_rename(
                            &mut touched_directories,
                            &from_abs,
                            &to_abs,
                        );
                        // Batch callers validate the source before journaling. A replay may
                        // observe neither path when a later mutation in the same completed
                        // batch already removed the destination, so a missing source is a
                        // successful no-op here.
                        match fs::symlink_metadata(&from_abs) {
                            Ok(_) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                            Err(error) => {
                                return Err(VfsStorageError::Internal(error.to_string()));
                            }
                        }
                        if to_abs.exists()
                            && paths_have_equivalent_contents(&from_abs, &to_abs)?
                        {
                            sync_path_permissions(&storage, &from_abs, &to_abs)?;
                            remove_path_for_replayed_rename(&from_abs)?;
                            storage.invalidate_hash(&from_abs);
                            storage.invalidate_hash(&to_abs);
                            continue;
                        }
                        if let Some(parent) = to_abs.parent() {
                            fs::create_dir_all(parent)
                                .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                        }
                        fs::rename(&from_abs, &to_abs).map_err(|error| {
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::DirectoryNotEmpty
                                    | std::io::ErrorKind::NotADirectory
                                    | std::io::ErrorKind::IsADirectory
                            ) {
                                VfsStorageError::Conflict(format!(
                                    "cannot replay rename {from} -> {to}: destination kind conflicts ({error})"
                                ))
                            } else {
                                VfsStorageError::Internal(error.to_string())
                            }
                        })?;
                        storage.invalidate_hash(&from_abs);
                        storage.invalidate_hash(&to_abs);
                    }
                }
            }
            sync_directories_deepest_first(&storage, touched_directories)
        })
        .await
    }

    async fn prefetch_subtree(
        &self,
        _prefix: &str,
        _options: VfsStoragePrefetchOptions,
    ) -> VfsStorageResult<VfsStoragePrefetchResult> {
        Ok(VfsStoragePrefetchResult::default())
    }
}

fn install_writes(
    storage: &LocalVfsStorage,
    writes: Vec<VfsStorageWrite>,
) -> VfsStorageResult<Vec<VfsStorageWriteResult>> {
    install_writes_with_options(
        storage,
        writes.into_iter().map(|write| (write, None)).collect(),
    )
}

struct ExactWriteReplay {
    path: String,
    destination: PathBuf,
    content_hash: String,
    requested_options: Option<VfsStorageWriteOptions>,
}

fn partition_conditional_write_replays(
    storage: &LocalVfsStorage,
    writes: Vec<(VfsStorageWrite, Option<VfsStorageWriteOptions>)>,
) -> VfsStorageResult<(
    Vec<(VfsStorageWrite, Option<VfsStorageWriteOptions>)>,
    Vec<ExactWriteReplay>,
)> {
    let mut seen = HashSet::new();
    let mut pending = Vec::new();
    let mut replays = Vec::new();
    for (write, options) in writes {
        if !seen.insert(write.path.clone()) {
            return Err(VfsStorageError::BadRequest(format!(
                "duplicate vfs write path: {}",
                write.path
            )));
        }
        let desired_hash = hex_hash(&write.bytes);
        match storage.assert_precondition(&write.path, write.precondition.as_ref()) {
            Ok(()) => pending.push((write, options)),
            Err(conflict @ VfsStorageError::Conflict(_)) => {
                if let Some(precondition) = write.precondition.as_ref() {
                    storage.assert_expected_file_id(&write.path, precondition)?;
                }
                let destination = storage.abs_path(&write.path)?;
                if !precondition_allows_exact_replay(write.precondition.as_ref())
                    && !storage.permits_incomplete_absent_replay(
                        &destination,
                        &desired_hash,
                        options.as_ref(),
                    )?
                {
                    return Err(conflict);
                }
                let replay = ExactWriteReplay {
                    destination,
                    path: write.path,
                    content_hash: desired_hash,
                    requested_options: options,
                };
                if open_exact_write_replay_target(storage, &replay)?.is_none() {
                    return Err(conflict);
                }
                replays.push(replay);
            }
            Err(error) => return Err(error),
        }
    }
    Ok((pending, replays))
}

fn precondition_allows_exact_replay(precondition: Option<&VfsStorageWritePrecondition>) -> bool {
    precondition.is_some_and(|precondition| precondition.effective_predicate().is_some())
}

fn precondition_expects_absent(precondition: Option<&VfsStorageWritePrecondition>) -> bool {
    precondition.is_some_and(|precondition| {
        matches!(
            precondition.effective_predicate(),
            Some(VfsStorageCasPredicate::Absent)
        ) && precondition.expected_file_id.is_none()
    })
}

fn open_exact_write_replay_target(
    storage: &LocalVfsStorage,
    replay: &ExactWriteReplay,
) -> VfsStorageResult<Option<fs::File>> {
    storage.assert_no_symlink_ancestor(&replay.destination)?;
    let path_metadata = match fs::symlink_metadata(&replay.destination) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(VfsStorageError::Internal(error.to_string())),
    };
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&replay.destination)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(VfsStorageError::Internal(error.to_string())),
    };
    let metadata = file
        .metadata()
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if !metadata.is_file() || !metadata_identity_matches(&path_metadata, &metadata) {
        return Ok(None);
    }
    #[cfg(test)]
    storage.hash_read_count.fetch_add(1, AtomicOrdering::SeqCst);
    let actual_hash = hash_open_file(&mut file)?;
    if actual_hash != replay.content_hash {
        return Ok(None);
    }
    Ok(Some(file))
}

fn complete_exact_write_replays(
    storage: &LocalVfsStorage,
    replays: &[ExactWriteReplay],
) -> VfsStorageResult<Vec<VfsStorageWriteResult>> {
    let mut synced_metadata = Vec::with_capacity(replays.len());
    let mut touched_directories = HashSet::new();
    for chunk in replays.chunks(MAX_PARALLEL_FILE_SYNCS) {
        let mut targets = Vec::with_capacity(chunk.len());
        for replay in chunk {
            let file = open_exact_write_replay_target(storage, replay)?.ok_or_else(|| {
                VfsStorageError::Conflict(format!(
                    "local vfs write precondition failed for {}",
                    replay.path
                ))
            })?;
            if let Some(options) = replay.requested_options.as_ref() {
                let previous_mode = existing_regular_file_mode(&replay.destination)?;
                apply_write_options(&replay.destination, Some(options), previous_mode)?;
                let metadata = file
                    .metadata()
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                if !write_options_match(&metadata, options) {
                    return Err(VfsStorageError::Internal(format!(
                        "local VFS mode did not converge for {}",
                        replay.path
                    )));
                }
            }
            targets.push(FileSyncTarget {
                path: replay.destination.clone(),
                file,
            });
        }
        sync_files_bounded(storage, &targets)?;
        for (replay, target) in chunk.iter().zip(&targets) {
            let metadata = target
                .file
                .metadata()
                .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
            synced_metadata.push(metadata);
            if let Some(parent) = replay.destination.parent() {
                collect_directory_chain(&storage.root, parent, &mut touched_directories)?;
            }
        }
    }
    sync_directories_deepest_first(storage, touched_directories)?;

    let mut results = Vec::with_capacity(replays.len());
    for (replay, synced_metadata) in replays.iter().zip(synced_metadata) {
        let current = open_exact_write_replay_target(storage, replay)?.ok_or_else(|| {
            VfsStorageError::Conflict(format!(
                "local vfs write replay changed before durability completed for {}",
                replay.path
            ))
        })?;
        let metadata = current
            .metadata()
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        if !metadata_identity_matches(&synced_metadata, &metadata)
            || replay
                .requested_options
                .as_ref()
                .is_some_and(|options| !write_options_match(&metadata, options))
        {
            return Err(VfsStorageError::Conflict(format!(
                "local vfs write replay changed before durability completed for {}",
                replay.path
            )));
        }
        storage.remember_written_hash(&replay.destination, &metadata, replay.content_hash.clone());
        storage.clear_incomplete_absent_write(&replay.destination);
        results.push(VfsStorageWriteResult {
            path: replay.path.clone(),
            content_hash: replay.content_hash.clone(),
            previous_hash: Some(replay.content_hash.clone()),
            changed: false,
        });
    }
    Ok(results)
}

fn write_results_in_order(
    order: &[String],
    results: impl IntoIterator<Item = VfsStorageWriteResult>,
) -> VfsStorageResult<Vec<VfsStorageWriteResult>> {
    let mut by_path = results
        .into_iter()
        .map(|result| (result.path.clone(), result))
        .collect::<HashMap<_, _>>();
    order
        .iter()
        .map(|path| {
            by_path.remove(path).ok_or_else(|| {
                VfsStorageError::Internal(format!("write returned no result for {path}"))
            })
        })
        .collect()
}

fn install_writes_with_options(
    storage: &LocalVfsStorage,
    writes: Vec<(VfsStorageWrite, Option<VfsStorageWriteOptions>)>,
) -> VfsStorageResult<Vec<VfsStorageWriteResult>> {
    let mut seen = HashSet::new();
    let mut staged = Vec::with_capacity(writes.len());
    for (write, options) in writes {
        if !seen.insert(write.path.clone()) {
            return Err(VfsStorageError::BadRequest(format!(
                "duplicate vfs write path: {}",
                write.path
            )));
        }
        let abs_path = storage.abs_path(&write.path)?;
        storage.assert_no_symlink_ancestor(&abs_path)?;
        let previous_hash = storage
            .metadata_for_abs(&abs_path)?
            .and_then(|metadata| metadata.content_hash);
        let previous_mode = if options.is_some() {
            existing_regular_file_mode(&abs_path)?
        } else {
            None
        };
        let content_hash = hex_hash(&write.bytes);
        if let Some(parent) = abs_path.parent() {
            fs::create_dir_all(parent).map_err(|err| VfsStorageError::Internal(err.to_string()))?;
        }
        let tmp_path = abs_path.with_file_name(format!(
            ".{}.{}.tmp",
            abs_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("vfs"),
            Uuid::new_v4()
        ));
        fs::write(&tmp_path, &write.bytes)
            .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
        let temporary_file = open_mode_sync_target(&tmp_path)?;
        apply_write_options(&tmp_path, options.as_ref(), previous_mode)?;
        staged.push(StagedWrite {
            path: write.path,
            destination: abs_path,
            temporary: tmp_path,
            temporary_file,
            content_hash,
            previous_hash,
            options,
            precondition: write.precondition,
            previous_mode,
        });
    }

    // Finish data and mode durability for the complete batch before the first
    // staged inode is made visible at its destination pathname.
    let staged_paths = staged
        .iter()
        .map(|write| write.temporary.clone())
        .collect::<Vec<_>>();
    let staged_sync_targets = staged
        .iter()
        .map(|write| {
            Ok(FileSyncTarget {
                path: write.temporary.clone(),
                file: write
                    .temporary_file
                    .try_clone()
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?,
            })
        })
        .collect::<VfsStorageResult<Vec<_>>>();
    let staged_sync_targets = match staged_sync_targets {
        Ok(targets) => targets,
        Err(error) => {
            for path in staged_paths {
                let _ = fs::remove_file(path);
            }
            return Err(error);
        }
    };
    if let Err(error) = sync_files_bounded(storage, &staged_sync_targets) {
        for path in staged_paths {
            let _ = fs::remove_file(path);
        }
        return Err(error);
    }
    if let Err(error) = staged
        .iter()
        .try_for_each(|write| storage.assert_precondition(&write.path, write.precondition.as_ref()))
    {
        for path in staged_paths {
            let _ = fs::remove_file(path);
        }
        return Err(error);
    }

    let mut installed = Vec::with_capacity(staged.len());
    let mut sync_targets = Vec::with_capacity(MAX_PARALLEL_FILE_SYNCS);
    let mut touched_directories = HashSet::new();
    for staged in staged {
        let sync_target = install_staged_file_preserving_identity(
            storage,
            &staged.temporary,
            &staged.temporary_file,
            &staged.destination,
        )?;
        let published_absent_write =
            sync_target.is_none() && precondition_expects_absent(staged.precondition.as_ref());
        if published_absent_write {
            let metadata = fs::symlink_metadata(&staged.destination)
                .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
            storage.remember_incomplete_absent_write(
                staged.destination.clone(),
                &metadata,
                staged.content_hash.clone(),
                staged.options.clone(),
            );
        }
        if let Some(target) = sync_target {
            apply_write_options(
                &staged.destination,
                staged.options.as_ref(),
                staged.previous_mode,
            )?;
            sync_targets.push(FileSyncTarget {
                path: staged.destination.clone(),
                file: target,
            });
            if sync_targets.len() == MAX_PARALLEL_FILE_SYNCS {
                sync_files_bounded(storage, &sync_targets)?;
                sync_targets.clear();
            }
        }
        if let Some(parent) = staged.destination.parent() {
            collect_directory_chain(&storage.root, parent, &mut touched_directories)?;
        }
        installed.push((
            staged.path,
            staged.destination,
            staged.content_hash,
            staged.previous_hash,
            published_absent_write,
        ));
    }
    sync_files_bounded(storage, &sync_targets)?;
    sync_directories_deepest_first(storage, touched_directories)?;

    let mut results = Vec::with_capacity(installed.len());
    for (path, abs_path, content_hash, previous_hash, published_absent_write) in installed {
        let metadata = fs::symlink_metadata(&abs_path)
            .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
        storage.invalidate_hash_identity(&metadata);
        storage.remember_written_hash(&abs_path, &metadata, content_hash.clone());
        if published_absent_write {
            storage.clear_incomplete_absent_write(&abs_path);
        }
        results.push(VfsStorageWriteResult {
            path,
            content_hash,
            previous_hash,
            changed: true,
        });
    }
    Ok(results)
}

struct StagedWrite {
    path: String,
    destination: PathBuf,
    temporary: PathBuf,
    temporary_file: fs::File,
    content_hash: String,
    previous_hash: Option<String>,
    options: Option<VfsStorageWriteOptions>,
    precondition: Option<VfsStorageWritePrecondition>,
    previous_mode: Option<u32>,
}

fn install_staged_file_preserving_identity(
    storage: &LocalVfsStorage,
    staged_path: &Path,
    staged_file: &fs::File,
    destination: &Path,
) -> VfsStorageResult<Option<fs::File>> {
    let existing = match fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.is_file() => Some(metadata),
        Ok(_) => None,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(VfsStorageError::Internal(error.to_string())),
    };
    if let Some(existing) = existing.as_ref() {
        // A write to an existing pathname mutates that inode; it does not
        // replace the directory entry with a newly allocated inode. This is
        // required even at nlink=1 so open handles and stable file identity
        // survive ordinary writes. Atomic replacement remains available to
        // callers through rename(2) of a separately created pathname.
        let mut source = staged_file
            .try_clone()
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        source
            .seek(SeekFrom::Start(0))
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        let mut target = open_existing_regular_file_for_rewrite(destination, existing)?;
        target
            .set_len(0)
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        target
            .seek(SeekFrom::Start(0))
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        std::io::copy(&mut source, &mut target)
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        fs::remove_file(staged_path)
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        storage.invalidate_hash_identity(existing);
        Ok(Some(target))
    } else {
        // The caller has already made the staged inode's data and final mode
        // durable. Publishing it therefore needs only the later directory
        // barriers; no second per-file sync is required.
        fs::rename(staged_path, destination)
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        storage.invalidate_hash(destination);
        Ok(None)
    }
}

fn open_regular_file_write_only(path: &Path) -> std::io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.write(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    options.open(path)
}

#[cfg(unix)]
fn open_existing_regular_file_for_rewrite(
    path: &Path,
    expected: &fs::Metadata,
) -> VfsStorageResult<fs::File> {
    match open_regular_file_write_only(path) {
        Ok(file) => {
            let opened = file
                .metadata()
                .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
            if !metadata_identity_matches(expected, &opened) {
                return Err(VfsStorageError::Conflict(format!(
                    "local VFS write target changed before open: {}",
                    path.display()
                )));
            }
            return Ok(file);
        }
        Err(error) if error.kind() != std::io::ErrorKind::PermissionDenied => {
            return Err(VfsStorageError::Internal(error.to_string()));
        }
        Err(_) => {}
    }

    // POSIX checks access at open(2), then the granted descriptor remains
    // writable after chmod. The gateway must model an already-open guest file
    // even when its exact logical mode is 0444/000 (Git loose objects use this
    // routinely). Briefly grant owner-write under the inode/path lock, open the
    // descriptor, and restore the exact mode on that descriptor before bytes
    // are changed. This preserves inode identity and hard-link aliasing.
    let original_mode = normalize_vfs_mode(expected.permissions().mode());
    let writable_mode = original_mode | 0o200;
    let writable = set_mode_nofollow(path, writable_mode)?;
    if !metadata_identity_matches(expected, &writable) {
        let _ = set_mode_nofollow(path, original_mode);
        return Err(VfsStorageError::Conflict(format!(
            "local VFS write target changed while granting write access: {}",
            path.display()
        )));
    }
    let opened = open_regular_file_write_only(path);
    let file = match opened {
        Ok(file) => file,
        Err(open_error) => {
            set_mode_nofollow(path, original_mode)?;
            return Err(VfsStorageError::Internal(open_error.to_string()));
        }
    };
    if let Err(restore_error) = file.set_permissions(fs::Permissions::from_mode(original_mode)) {
        let _ = set_mode_nofollow(path, original_mode);
        return Err(VfsStorageError::Internal(restore_error.to_string()));
    }
    let restored = file
        .metadata()
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if !metadata_identity_matches(expected, &restored)
        || posix_mode_from_metadata(&restored) != Some(original_mode)
    {
        return Err(VfsStorageError::Conflict(format!(
            "local VFS write target changed before mode restoration: {}",
            path.display()
        )));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_existing_regular_file_for_rewrite(
    path: &Path,
    expected: &fs::Metadata,
) -> VfsStorageResult<fs::File> {
    let file = open_regular_file_write_only(path)
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    let opened = file
        .metadata()
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if !metadata_identity_matches(expected, &opened) {
        return Err(VfsStorageError::Conflict(format!(
            "local VFS write target changed before open: {}",
            path.display()
        )));
    }
    Ok(file)
}

struct FileSyncTarget {
    path: PathBuf,
    file: fs::File,
}

fn sync_files_bounded(storage: &LocalVfsStorage, files: &[FileSyncTarget]) -> VfsStorageResult<()> {
    // Each handle names an independent existing inode. Overlap their durability
    // waits, but join every worker before publication returns: this preserves
    // the write barrier while avoiding one full filesystem round trip per file
    // in strict sequence.
    if files.is_empty() {
        return Ok(());
    }
    if files.len() == 1 {
        return storage.sync_file(&files[0].file, &files[0].path);
    }
    let worker_count = files.len().min(MAX_PARALLEL_FILE_SYNCS);
    let chunk_size = files.len().div_ceil(worker_count);
    std::thread::scope(|scope| {
        let workers = files
            .chunks(chunk_size)
            .map(|chunk| {
                scope.spawn(move || {
                    for file in chunk {
                        storage.sync_file(&file.file, &file.path)?;
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        join_sync_workers(workers)
    })
}

fn join_sync_workers<'scope>(
    workers: Vec<std::thread::ScopedJoinHandle<'scope, VfsStorageResult<()>>>,
) -> VfsStorageResult<()> {
    let mut first_error = None;
    for worker in workers {
        let result = worker.join().unwrap_or_else(|_| {
            Err(VfsStorageError::Internal(
                "local VFS file-sync worker panicked".to_string(),
            ))
        });
        if first_error.is_none() {
            if let Err(error) = result {
                first_error = Some(error);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn collect_directory_chain(
    root: &Path,
    directory: &Path,
    touched: &mut HashSet<PathBuf>,
) -> VfsStorageResult<()> {
    let relative = directory.strip_prefix(root).map_err(|error| {
        VfsStorageError::Internal(format!(
            "local VFS durability path {} escaped root {}: {error}",
            directory.display(),
            root.display()
        ))
    })?;
    let mut current = root.to_path_buf();
    touched.insert(current.clone());
    for component in relative.components() {
        let Component::Normal(part) = component else {
            continue;
        };
        current.push(part);
        touched.insert(current.clone());
    }
    Ok(())
}

fn remap_touched_directories_after_rename(
    touched: &mut HashSet<PathBuf>,
    source: &Path,
    destination: &Path,
) {
    let remapped = touched
        .iter()
        .filter_map(|path| {
            path.strip_prefix(source)
                .ok()
                .map(|suffix| (path.clone(), destination.join(suffix)))
        })
        .collect::<Vec<_>>();
    for (source_path, destination_path) in remapped {
        touched.remove(&source_path);
        touched.insert(destination_path);
    }
}

fn sync_directories_deepest_first(
    storage: &LocalVfsStorage,
    touched: HashSet<PathBuf>,
) -> VfsStorageResult<()> {
    let mut touched = touched.into_iter().collect::<Vec<_>>();
    touched.sort_unstable_by(|left, right| {
        right
            .components()
            .count()
            .cmp(&left.components().count())
            .then_with(|| left.cmp(right))
    });
    for directory in touched {
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(VfsStorageError::Internal(format!(
                    "local VFS durability path {} is not a directory",
                    directory.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(VfsStorageError::Internal(error.to_string())),
        }
        storage.sync_directory(&directory)?;
    }
    Ok(())
}

fn sync_directory_chains(
    storage: &LocalVfsStorage,
    directories: impl IntoIterator<Item = PathBuf>,
) -> VfsStorageResult<()> {
    let mut touched = HashSet::new();
    for directory in directories {
        collect_directory_chain(&storage.root, &directory, &mut touched)?;
    }
    sync_directories_deepest_first(storage, touched)
}

fn read_file(path: &Path) -> VfsStorageResult<Vec<u8>> {
    let mut file = open_regular_file(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
    Ok(bytes)
}

fn paths_have_equivalent_contents(left: &Path, right: &Path) -> VfsStorageResult<bool> {
    let left_metadata =
        fs::symlink_metadata(left).map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    let right_metadata = fs::symlink_metadata(right)
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    let left_type = left_metadata.file_type();
    let right_type = right_metadata.file_type();
    if left_type.is_file() != right_type.is_file()
        || left_type.is_dir() != right_type.is_dir()
        || left_type.is_symlink() != right_type.is_symlink()
    {
        return Ok(false);
    }
    if left_type.is_symlink() {
        return Ok(fs::read_link(left)
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?
            == fs::read_link(right)
                .map_err(|error| VfsStorageError::Internal(error.to_string()))?);
    }
    if left_type.is_file() {
        if left_metadata.len() != right_metadata.len() {
            return Ok(false);
        }
        return Ok(hash_regular_file(left)? == hash_regular_file(right)?);
    }
    if !left_type.is_dir() {
        return Ok(false);
    }

    let mut left_entries = fs::read_dir(left)
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name())
                .map_err(|error| VfsStorageError::Internal(error.to_string()))
        })
        .collect::<VfsStorageResult<Vec<_>>>()?;
    let mut right_entries = fs::read_dir(right)
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?
        .map(|entry| {
            entry
                .map(|entry| entry.file_name())
                .map_err(|error| VfsStorageError::Internal(error.to_string()))
        })
        .collect::<VfsStorageResult<Vec<_>>>()?;
    left_entries.sort();
    right_entries.sort();
    if left_entries != right_entries {
        return Ok(false);
    }
    for name in left_entries {
        if !paths_have_equivalent_contents(&left.join(&name), &right.join(&name))? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn sync_path_permissions(
    storage: &LocalVfsStorage,
    source: &Path,
    destination: &Path,
) -> VfsStorageResult<()> {
    let source_metadata = fs::symlink_metadata(source)
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if source_metadata.file_type().is_symlink() {
        return Ok(());
    }
    fs::set_permissions(destination, source_metadata.permissions())
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if !source_metadata.is_dir() {
        let target = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(destination)
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        return storage.sync_file(&target, destination);
    }
    for entry in
        fs::read_dir(source).map_err(|error| VfsStorageError::Internal(error.to_string()))?
    {
        let entry = entry.map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        sync_path_permissions(storage, &entry.path(), &destination.join(entry.file_name()))?;
    }
    storage.sync_directory(destination)
}

fn remove_path_for_replayed_rename(path: &Path) -> VfsStorageResult<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if metadata.is_dir() {
        fs::remove_dir_all(path).map_err(|error| VfsStorageError::Internal(error.to_string()))
    } else {
        fs::remove_file(path).map_err(|error| VfsStorageError::Internal(error.to_string()))
    }
}

fn hash_file_if_present_uncached(
    _storage: &LocalVfsStorage,
    path: &Path,
) -> VfsStorageResult<Option<String>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(VfsStorageError::Internal(err.to_string())),
    };
    if !metadata.is_file() {
        return Ok(None);
    }
    #[cfg(test)]
    _storage
        .hash_read_count
        .fetch_add(1, AtomicOrdering::SeqCst);
    match hash_regular_file(path) {
        Ok(hash) => Ok(Some(hash)),
        Err(VfsStorageError::NotFound(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

fn hash_regular_file(path: &Path) -> VfsStorageResult<String> {
    let mut file = open_regular_file(path)?;
    hash_open_file(&mut file)
}

fn hash_open_file(file: &mut fs::File) -> VfsStorageResult<String> {
    file.seek(SeekFrom::Start(0))
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    let mut hasher = sha2::Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn open_regular_file(path: &Path) -> VfsStorageResult<fs::File> {
    assert_supported_read_target(path)?;
    let file = fs::File::open(path).map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => VfsStorageError::NotFound(path.display().to_string()),
        _ => VfsStorageError::Internal(err.to_string()),
    })?;
    // The path is inside a locally trusted scope, but checking the opened fd
    // closes the cheap final-component kind-change window before reads.
    let metadata = file
        .metadata()
        .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
    if !metadata.is_file() {
        return Err(VfsStorageError::BadRequest(
            "unsupported file type: special".to_string(),
        ));
    }
    Ok(file)
}

fn assert_supported_read_target(path: &Path) -> VfsStorageResult<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(VfsStorageError::NotFound(path.display().to_string()));
        }
        Err(err) => return Err(VfsStorageError::Internal(err.to_string())),
    };
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(VfsStorageError::BadRequest(
            "unsupported file type: symlink".to_string(),
        ));
    }
    if !metadata.is_file() {
        if metadata.is_dir() {
            return Err(VfsStorageError::BadRequest(format!(
                "vfs path {} is not a file",
                path.display()
            )));
        }
        return Err(VfsStorageError::BadRequest(
            "unsupported file type: special".to_string(),
        ));
    }
    Ok(())
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(part) => out.push(part),
            Component::RootDir | Component::Prefix(_) => out.push(component.as_os_str()),
        }
    }
    out
}

fn relative_path_between(from_dir: &Path, target: &Path) -> String {
    let from = from_dir
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let to = target
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let common = from
        .iter()
        .zip(to.iter())
        .take_while(|(left, right)| left == right)
        .count();
    let mut parts = Vec::new();
    parts.extend(std::iter::repeat_n("..".to_string(), from.len() - common));
    parts.extend(to[common..].iter().cloned());
    if parts.is_empty() {
        ".".to_string()
    } else {
        parts.join("/")
    }
}

fn metadata_is_recent(metadata: &fs::Metadata) -> bool {
    let Ok(modified) = metadata.modified() else {
        return true;
    };
    match SystemTime::now().duration_since(modified) {
        Ok(elapsed) => elapsed < HASH_CACHE_RECENCY_GUARD,
        Err(_) => true,
    }
}

#[cfg(unix)]
fn trusted_write_cache_reusable(cached: &CachedFileHash) -> bool {
    cached.trusted_write
}

#[cfg(not(unix))]
fn trusted_write_cache_reusable(_cached: &CachedFileHash) -> bool {
    // Unix ctime lets us detect an out-of-band same-size write even when an
    // application preserves mtime. Other platforms keep the conservative
    // recency rehash until an equivalent change-generation signal is available.
    false
}

#[cfg(unix)]
fn metadata_mtime_ns(metadata: &fs::Metadata) -> i128 {
    metadata.mtime() as i128 * 1_000_000_000 + metadata.mtime_nsec() as i128
}

#[cfg(unix)]
fn metadata_change_ns(metadata: &fs::Metadata) -> i128 {
    metadata.ctime() as i128 * 1_000_000_000 + metadata.ctime_nsec() as i128
}

#[cfg(not(unix))]
fn metadata_change_ns(metadata: &fs::Metadata) -> i128 {
    metadata_mtime_ns(metadata)
}

#[cfg(not(unix))]
fn metadata_mtime_ns(metadata: &fs::Metadata) -> i128 {
    metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as i128)
        .unwrap_or_default()
}

#[cfg(unix)]
fn create_symlink_impl(
    storage: &LocalVfsStorage,
    path: &str,
    target: &str,
) -> VfsStorageResult<()> {
    let abs_path = storage.abs_path(path)?;
    storage.assert_no_symlink_ancestor(&abs_path)?;
    if let Some(parent) = abs_path.parent() {
        fs::create_dir_all(parent).map_err(|err| VfsStorageError::Internal(err.to_string()))?;
    }
    let target = storage.validate_symlink_target_text(&abs_path, Path::new(target))?;
    std::os::unix::fs::symlink(target.target_text, &abs_path)
        .map_err(|err| VfsStorageError::Internal(err.to_string()))
}

#[cfg(not(unix))]
fn create_symlink_impl(
    _storage: &LocalVfsStorage,
    _path: &str,
    _target: &str,
) -> VfsStorageResult<()> {
    Err(VfsStorageError::BadRequest(
        "local vfs backend does not support symlink creation on this platform".to_string(),
    ))
}

fn unsupported_symlink_error() -> VfsStorageError {
    VfsStorageError::BadRequest("unsupported file type: symlink".to_string())
}

fn is_excluded_listing_error(error: &VfsStorageError) -> bool {
    matches!(
        error,
        VfsStorageError::BadRequest(message)
            if message == "unsupported file type: symlink"
                || message == "unsupported file type: special"
    )
}

fn is_excluded_listing_kind(kind: VfsStorageEntryKind) -> bool {
    matches!(kind, VfsStorageEntryKind::Special)
}

#[cfg(unix)]
fn local_file_id(metadata: &fs::Metadata) -> Option<String> {
    Some(format!("unix:{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn local_file_id(_metadata: &fs::Metadata) -> Option<String> {
    None
}

fn metadata_identity_matches(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    match (local_file_id(left), local_file_id(right)) {
        (Some(left), Some(right)) => left == right,
        (None, None) => {
            left.is_file() == right.is_file()
                && left.len() == right.len()
                && left.modified().ok() == right.modified().ok()
        }
        _ => false,
    }
}

#[cfg(unix)]
fn local_link_count(metadata: &fs::Metadata) -> u64 {
    metadata.nlink()
}

#[cfg(not(unix))]
fn local_link_count(_metadata: &fs::Metadata) -> u64 {
    1
}

#[cfg(unix)]
fn executable_from_metadata(metadata: &fs::Metadata, kind: VfsStorageEntryKind) -> bool {
    kind == VfsStorageEntryKind::File && metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn executable_from_metadata(_metadata: &fs::Metadata, _kind: VfsStorageEntryKind) -> bool {
    false
}

#[cfg(unix)]
fn posix_mode_from_metadata(metadata: &fs::Metadata) -> Option<u32> {
    (metadata.is_file() || metadata.is_dir())
        .then(|| normalize_vfs_mode(metadata.permissions().mode()))
}

#[cfg(not(unix))]
fn posix_mode_from_metadata(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
fn existing_regular_file_mode(path: &Path) -> VfsStorageResult<Option<u32>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => {
            Ok(Some(normalize_vfs_mode(metadata.permissions().mode())))
        }
        Ok(_) => Ok(None),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(VfsStorageError::Internal(err.to_string())),
    }
}

#[cfg(not(unix))]
fn existing_regular_file_mode(_path: &Path) -> VfsStorageResult<Option<u32>> {
    Ok(None)
}

#[cfg(unix)]
fn apply_write_options(
    path: &Path,
    options: Option<&VfsStorageWriteOptions>,
    previous_mode: Option<u32>,
) -> VfsStorageResult<()> {
    let Some(options) = options else {
        return Ok(());
    };
    let metadata =
        fs::symlink_metadata(path).map_err(|err| VfsStorageError::Internal(err.to_string()))?;
    let current_mode = normalize_vfs_mode(metadata.permissions().mode());
    let target_mode = options.mode.map(normalize_vfs_mode).unwrap_or_else(|| {
        match (previous_mode, options.executable) {
            (Some(mode), true) => mode | 0o111,
            (Some(mode), false) => mode & !0o111,
            (None, true) => 0o755,
            (None, false) => current_mode & !0o111,
        }
    });
    apply_exact_mode(path, target_mode, false)?;
    Ok(())
}

#[cfg(not(unix))]
fn apply_write_options(
    _path: &Path,
    options: Option<&VfsStorageWriteOptions>,
    _previous_mode: Option<u32>,
) -> VfsStorageResult<()> {
    if options.and_then(|options| options.mode).is_some() {
        return Err(VfsStorageError::BadRequest(
            "exact POSIX mode is not supported on this platform".to_string(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn write_options_match(metadata: &fs::Metadata, options: &VfsStorageWriteOptions) -> bool {
    if let Some(mode) = options.mode {
        return posix_mode_from_metadata(metadata) == Some(normalize_vfs_mode(mode));
    }
    executable_from_metadata(metadata, VfsStorageEntryKind::File) == options.executable
}

#[cfg(not(unix))]
fn write_options_match(_metadata: &fs::Metadata, options: &VfsStorageWriteOptions) -> bool {
    options.mode.is_none()
}

#[cfg(unix)]
fn apply_directory_mode(path: &Path, mode: Option<u32>) -> VfsStorageResult<()> {
    let Some(mode) = mode else {
        return Ok(());
    };
    apply_exact_mode(path, mode, false)?;
    Ok(())
}

#[cfg(not(unix))]
fn apply_directory_mode(_path: &Path, mode: Option<u32>) -> VfsStorageResult<()> {
    if mode.is_some() {
        return Err(VfsStorageError::BadRequest(
            "exact POSIX mode is not supported on this platform".to_string(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn apply_exact_mode(path: &Path, mode: u32, allow_missing: bool) -> VfsStorageResult<bool> {
    let path_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if allow_missing && error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(false);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(VfsStorageError::NotFound(path.display().to_string()));
        }
        Err(error) => return Err(VfsStorageError::Internal(error.to_string())),
    };
    if !path_metadata.is_file() && !path_metadata.is_dir() {
        return Err(VfsStorageError::BadRequest(format!(
            "POSIX mode changes require a regular file or directory: {}",
            path.display()
        )));
    }
    let target_mode = normalize_vfs_mode(mode);
    let updated = set_mode_nofollow(path, target_mode)?;
    let path_after =
        fs::symlink_metadata(path).map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if !metadata_identity_matches(&path_metadata, &updated)
        || !metadata_identity_matches(&updated, &path_after)
        || posix_mode_from_metadata(&updated) != Some(target_mode)
    {
        return Err(VfsStorageError::Conflict(format!(
            "local VFS mode target changed before convergence for {}",
            path.display()
        )));
    }
    Ok(true)
}

#[cfg(target_os = "linux")]
fn set_mode_nofollow(path: &Path, mode: u32) -> VfsStorageResult<fs::Metadata> {
    let encoded = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        VfsStorageError::BadRequest(format!("local VFS path contains NUL: {}", path.display()))
    })?;
    let raw_fd = unsafe {
        libc::open(
            encoded.as_ptr(),
            libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if raw_fd < 0 {
        return Err(VfsStorageError::Internal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    let target = unsafe { fs::File::from_raw_fd(raw_fd) };
    let before = target
        .metadata()
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if !before.is_file() && !before.is_dir() {
        return Err(VfsStorageError::BadRequest(format!(
            "POSIX mode changes require a regular file or directory: {}",
            path.display()
        )));
    }
    let empty = [0_u8];
    let result = unsafe {
        libc::syscall(
            libc::SYS_fchmodat2,
            target.as_raw_fd(),
            empty.as_ptr().cast::<libc::c_char>(),
            mode as libc::mode_t,
            libc::AT_EMPTY_PATH,
        )
    };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        let fallback = error
            .raw_os_error()
            .is_some_and(|code| matches!(code, libc::ENOSYS | libc::EINVAL | libc::EOPNOTSUPP));
        if !fallback {
            return Err(VfsStorageError::Internal(error.to_string()));
        }
        let descriptor_path = CString::new(format!("/proc/self/fd/{}", target.as_raw_fd()))
            .expect("descriptor path contains no NUL");
        if unsafe { libc::chmod(descriptor_path.as_ptr(), mode as libc::mode_t) } != 0 {
            return Err(VfsStorageError::Internal(
                std::io::Error::last_os_error().to_string(),
            ));
        }
    }
    target
        .metadata()
        .map_err(|error| VfsStorageError::Internal(error.to_string()))
}

#[cfg(target_os = "macos")]
fn set_mode_nofollow(path: &Path, mode: u32) -> VfsStorageResult<fs::Metadata> {
    let encoded = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        VfsStorageError::BadRequest(format!("local VFS path contains NUL: {}", path.display()))
    })?;
    if unsafe {
        libc::fchmodat(
            libc::AT_FDCWD,
            encoded.as_ptr(),
            mode as libc::mode_t,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(VfsStorageError::Internal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    fs::symlink_metadata(path).map_err(|error| VfsStorageError::Internal(error.to_string()))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn set_mode_nofollow(path: &Path, mode: u32) -> VfsStorageResult<fs::Metadata> {
    let encoded = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        VfsStorageError::BadRequest(format!("local VFS path contains NUL: {}", path.display()))
    })?;
    if unsafe {
        libc::fchmodat(
            libc::AT_FDCWD,
            encoded.as_ptr(),
            mode as libc::mode_t,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(VfsStorageError::Internal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    fs::symlink_metadata(path).map_err(|error| VfsStorageError::Internal(error.to_string()))
}

#[cfg(not(unix))]
fn apply_exact_mode(_path: &Path, _mode: u32, _allow_missing: bool) -> VfsStorageResult<bool> {
    Err(VfsStorageError::BadRequest(
        "exact POSIX mode is not supported on this platform".to_string(),
    ))
}

fn open_mode_sync_target(path: &Path) -> VfsStorageResult<fs::File> {
    let open = |read: bool, write: bool| {
        let mut options = fs::OpenOptions::new();
        options.read(read).write(write);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        options.open(path)
    };
    open(true, false)
        .or_else(|_| open(false, true))
        .map_err(|error| VfsStorageError::Internal(error.to_string()))
}

fn sync_mode_target(
    storage: &LocalVfsStorage,
    path: &Path,
    opened_before_mode_change: Option<fs::File>,
) -> VfsStorageResult<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if !metadata.is_file() && !metadata.is_dir() {
        return Err(VfsStorageError::BadRequest(format!(
            "POSIX mode changes require a regular file or directory: {}",
            path.display()
        )));
    }
    let file = match opened_before_mode_change {
        Some(file) => file,
        None => open_mode_sync_target(path)?,
    };
    let opened = file
        .metadata()
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if !metadata_identity_matches(&metadata, &opened) {
        return Err(VfsStorageError::Conflict(format!(
            "local VFS mode target changed before durability for {}",
            path.display()
        )));
    }
    if metadata.is_dir() {
        storage.sync_directory_handle(&file, path)
    } else {
        storage.sync_file(&file, path)
    }
}

fn modified_at(metadata: &fs::Metadata) -> Option<DateTime<Utc>> {
    metadata.modified().ok().map(DateTime::<Utc>::from)
}

fn filter_name(name: &str, filter: &VfsStorageDirListFilter) -> bool {
    if let Some(pattern) = filter.name_like.as_deref() {
        if !sql_like_match(pattern, name) {
            return false;
        }
    }
    if let Some(pattern) = filter.name_not_like.as_deref() {
        if sql_like_match(pattern, name) {
            return false;
        }
    }
    true
}

fn sort_entries(entries: &mut [VfsStorageMetadata], order: Option<VfsStorageDirListOrder>) {
    match order.unwrap_or(VfsStorageDirListOrder::KindThenName) {
        VfsStorageDirListOrder::KindThenName => {
            entries.sort_by(|a, b| kind_order(a.kind, b.kind).then_with(|| a.path.cmp(&b.path)))
        }
        VfsStorageDirListOrder::NameAsc => entries.sort_by(|a, b| a.path.cmp(&b.path)),
        VfsStorageDirListOrder::NameDesc => entries.sort_by(|a, b| b.path.cmp(&a.path)),
        VfsStorageDirListOrder::UpdatedDesc => entries.sort_by(|a, b| {
            b.updated_at
                .cmp(&a.updated_at)
                .then_with(|| a.path.cmp(&b.path))
        }),
    }
}

fn kind_order(a: VfsStorageEntryKind, b: VfsStorageEntryKind) -> Ordering {
    match (a, b) {
        (VfsStorageEntryKind::Directory, VfsStorageEntryKind::File) => Ordering::Less,
        (VfsStorageEntryKind::File, VfsStorageEntryKind::Directory) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

fn sql_like_match(pattern: &str, name: &str) -> bool {
    fn match_inner(pat: &[u8], s: &[u8]) -> bool {
        let mut pi = 0;
        let mut si = 0;
        let mut star = None;
        let mut star_si = 0;
        while si < s.len() {
            if pi < pat.len() {
                match pat[pi] {
                    b'%' => {
                        star = Some(pi);
                        star_si = si;
                        pi += 1;
                        continue;
                    }
                    b'_' => {
                        pi += 1;
                        si += 1;
                        continue;
                    }
                    c if c == s[si] => {
                        pi += 1;
                        si += 1;
                        continue;
                    }
                    _ => {}
                }
            }
            if let Some(sp) = star {
                pi = sp + 1;
                star_si += 1;
                si = star_si;
            } else {
                return false;
            }
        }
        while pi < pat.len() && pat[pi] == b'%' {
            pi += 1;
        }
        pi == pat.len()
    }
    match_inner(pattern.as_bytes(), name.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    #[cfg(unix)]
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicBoolOrdering};
    use std::sync::{Arc, Barrier};

    fn set_old_mtime(path: &Path) {
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .expect("open for mtime");
        file.set_modified(SystemTime::now() - Duration::from_secs(5))
            .expect("set old mtime");
    }

    fn storage_with_durability_observer(
        root: &Path,
        observer: impl Fn(&DurabilitySyncEvent) -> VfsStorageResult<()> + Send + Sync + 'static,
    ) -> LocalVfsStorage {
        let mut storage = LocalVfsStorage::new(root);
        storage.durability_sync_observer = Some(DurabilitySyncObserver(Arc::new(observer)));
        storage
    }

    #[cfg(unix)]
    fn path_mode(path: &Path) -> u32 {
        normalize_vfs_mode(
            fs::symlink_metadata(path)
                .expect("mode metadata")
                .permissions()
                .mode(),
        )
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_new_file_syncs_data_and_mode_before_directory_chain() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            if let DurabilitySyncEvent::File(path) = event {
                let mode = fs::metadata(path)
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?
                    .permissions()
                    .mode();
                if mode & 0o111 == 0 {
                    return Err(VfsStorageError::Internal(
                        "staged executable mode was not applied before file sync".to_string(),
                    ));
                }
            }
            observed.lock().unwrap().push(event.clone());
            Ok(())
        });

        storage
            .write_with_options(
                "one/two/tool.sh",
                Bytes::from_static(b"#!/bin/sh\nexit 0\n"),
                None,
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: None,
                }),
            )
            .await
            .expect("durable executable write");

        let events = events.lock().unwrap().clone();
        let first_directory = events
            .iter()
            .position(|event| matches!(event, DurabilitySyncEvent::Directory(_)))
            .expect("directory syncs");
        assert_eq!(
            first_directory, 1,
            "new file must sync staged data and final mode before directories: {events:?}",
        );
        assert!(matches!(
            &events[0],
            DurabilitySyncEvent::File(path)
                if path.file_name().is_some_and(|name| name.to_string_lossy().ends_with(".tmp"))
        ));
        assert_eq!(
            &events[1..],
            &[
                DurabilitySyncEvent::Directory(dir.path().join("one/two")),
                DurabilitySyncEvent::Directory(dir.path().join("one")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );
        assert_ne!(
            fs::metadata(dir.path().join("one/two/tool.sh"))
                .expect("installed file")
                .permissions()
                .mode()
                & 0o111,
            0,
            "final executable mode must be installed before its file barrier",
        );
    }

    #[tokio::test]
    async fn local_batch_file_sync_failure_publishes_no_destinations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            if matches!(event, DurabilitySyncEvent::File(_)) {
                return Err(VfsStorageError::Internal(
                    "injected file sync failure".to_string(),
                ));
            }
            Ok(())
        });

        let result = storage
            .write_many_atomic(vec![
                VfsStorageWrite {
                    path: "one/a.txt".to_string(),
                    bytes: Bytes::from_static(b"a"),
                    token_count: None,
                    precondition: None,
                },
                VfsStorageWrite {
                    path: "two/b.txt".to_string(),
                    bytes: Bytes::from_static(b"b"),
                    token_count: None,
                    precondition: None,
                },
            ])
            .await;
        assert!(result.is_err(), "staged file barrier failure must abort");
        assert!(!dir.path().join("one/a.txt").exists());
        assert!(!dir.path().join("two/b.txt").exists());
        for parent in ["one", "two"] {
            let entries = fs::read_dir(dir.path().join(parent))
                .expect("staging parent")
                .collect::<Result<Vec<_>, _>>()
                .expect("staging entries");
            assert!(entries.is_empty(), "failed batch must clean staged files");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_existing_write_replay_repeats_file_and_parent_barriers() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("nested")).expect("nested");
        fs::write(dir.path().join("nested/value"), b"old").expect("seed");
        let destination = dir.path().join("nested/value");
        let fail_once = Arc::new(AtomicBool::new(true));
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed_fail = Arc::clone(&fail_once);
        let observed_events = Arc::clone(&events);
        let observed_destination = destination.clone();
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed_events.lock().unwrap().push(event.clone());
            if matches!(event, DurabilitySyncEvent::File(path) if path == &observed_destination)
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected destination sync failure".to_string(),
                ));
            }
            Ok(())
        });
        let precondition = VfsStorageWritePrecondition {
            predicate: None,
            fingerprint: Some(hex_hash(b"old")),
            secondary_fingerprint: None,
            expected_file_id: None,
        };

        let first = storage
            .write_with_options(
                "nested/value",
                Bytes::from_static(b"desired"),
                Some(precondition.clone()),
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: None,
                }),
            )
            .await;
        assert!(first.is_err(), "file barrier failure must prevent ack");
        assert_eq!(fs::read(&destination).expect("desired bytes"), b"desired");

        events.lock().unwrap().clear();
        let replay = storage
            .write_with_options(
                "nested/value",
                Bytes::from_static(b"desired"),
                Some(precondition.clone()),
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: None,
                }),
            )
            .await
            .expect("exact desired-state replay");
        assert!(!replay.changed);
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::File(destination.clone()),
                DurabilitySyncEvent::Directory(dir.path().join("nested")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );

        events.lock().unwrap().clear();
        let mismatch = storage
            .write_with_options(
                "nested/value",
                Bytes::from_static(b"different"),
                Some(precondition.clone()),
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: None,
                }),
            )
            .await;
        assert!(matches!(mismatch, Err(VfsStorageError::Conflict(_))));
        assert!(events.lock().unwrap().is_empty());

        let mut permissions = fs::metadata(&destination).expect("metadata").permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&destination, permissions).expect("remove executable mode");
        let mode_mismatch = storage
            .write_with_options(
                "nested/value",
                Bytes::from_static(b"desired"),
                Some(precondition),
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: None,
                }),
            )
            .await
            .expect("exact-byte replay must restore requested mode");
        assert!(!mode_mismatch.changed);
        assert_ne!(
            fs::metadata(&destination)
                .expect("restored metadata")
                .permissions()
                .mode()
                & 0o111,
            0,
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_new_write_directory_failure_replays_exact_file_and_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("nested/value");
        let fail_once = Arc::new(AtomicBool::new(true));
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed_fail = Arc::clone(&fail_once);
        let observed_events = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed_events.lock().unwrap().push(event.clone());
            if matches!(event, DurabilitySyncEvent::Directory(_))
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected directory sync failure".to_string(),
                ));
            }
            Ok(())
        });
        let precondition = VfsStorageWritePrecondition {
            predicate: None,
            fingerprint: None,
            secondary_fingerprint: None,
            expected_file_id: None,
        };

        let first = storage
            .write_with_options(
                "nested/value",
                Bytes::from_static(b"desired"),
                Some(precondition.clone()),
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: None,
                }),
            )
            .await;
        assert!(first.is_err(), "directory barrier failure must prevent ack");
        assert!(destination.exists());

        events.lock().unwrap().clear();
        storage
            .write_with_options(
                "nested/value",
                Bytes::from_static(b"desired"),
                Some(precondition.clone()),
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: None,
                }),
            )
            .await
            .expect("exact new-path replay");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::File(destination.clone()),
                DurabilitySyncEvent::Directory(dir.path().join("nested")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );

        let mut permissions = fs::metadata(&destination).expect("metadata").permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&destination, permissions).expect("mode mismatch");
        let completed_retry = storage
            .write_with_options(
                "nested/value",
                Bytes::from_static(b"desired"),
                Some(precondition),
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: None,
                }),
            )
            .await;
        assert!(
            matches!(completed_retry, Err(VfsStorageError::Conflict(_))),
            "a completed expect-absent operation must not authorize later identical creators",
        );
        assert_eq!(
            fs::metadata(&destination)
                .expect("preserved metadata")
                .permissions()
                .mode()
                & 0o111,
            0,
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_exact_replay_rejects_replaced_inode_and_final_symlink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("value");
        let replacement = dir.path().join("replacement");
        fs::write(&destination, b"desired").expect("destination");
        fs::write(&replacement, b"desired").expect("replacement");
        let replace_once = Arc::new(AtomicBool::new(true));
        let observed_replace = Arc::clone(&replace_once);
        let observed_destination = destination.clone();
        let observed_replacement = replacement.clone();
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            if matches!(event, DurabilitySyncEvent::File(path) if path == &observed_destination)
                && observed_replace.swap(false, AtomicBoolOrdering::SeqCst)
            {
                fs::rename(&observed_replacement, &observed_destination)
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
            }
            Ok(())
        });
        let stale = VfsStorageWritePrecondition {
            predicate: None,
            fingerprint: Some(hex_hash(b"old")),
            secondary_fingerprint: None,
            expected_file_id: None,
        };

        let replaced = storage
            .write("value", Bytes::from_static(b"desired"), Some(stale.clone()))
            .await;
        assert!(matches!(replaced, Err(VfsStorageError::Conflict(_))));

        fs::write(dir.path().join("target"), b"desired").expect("target");
        symlink("target", dir.path().join("link")).expect("link");
        let linked = storage
            .write("link", Bytes::from_static(b"desired"), Some(stale))
            .await;
        assert!(matches!(linked, Err(VfsStorageError::Conflict(_))));
        assert_eq!(
            fs::read_link(dir.path().join("link")).expect("preserved symlink"),
            PathBuf::from("target"),
        );
    }

    #[tokio::test]
    async fn local_mixed_batch_rebars_exact_replay_before_pending_publication() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("existing"), b"desired").expect("existing");
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed_events = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed_events.lock().unwrap().push(event.clone());
            Ok(())
        });

        storage
            .write_many_atomic(vec![
                VfsStorageWrite {
                    path: "existing".to_string(),
                    bytes: Bytes::from_static(b"desired"),
                    token_count: None,
                    precondition: Some(VfsStorageWritePrecondition {
                        predicate: None,
                        fingerprint: Some(hex_hash(b"old")),
                        secondary_fingerprint: None,
                        expected_file_id: None,
                    }),
                },
                VfsStorageWrite {
                    path: "new".to_string(),
                    bytes: Bytes::from_static(b"new"),
                    token_count: None,
                    precondition: Some(VfsStorageWritePrecondition {
                        predicate: None,
                        fingerprint: None,
                        secondary_fingerprint: None,
                        expected_file_id: None,
                    }),
                },
            ])
            .await
            .expect("mixed exact replay and pending write");

        let events = events.lock().unwrap();
        let replay_sync = events
            .iter()
            .position(|event| event == &DurabilitySyncEvent::File(dir.path().join("existing")))
            .expect("replay file barrier");
        let staged_sync = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    DurabilitySyncEvent::File(path)
                        if path.file_name().is_some_and(|name| {
                            name.to_string_lossy().starts_with(".new.")
                        })
                )
            })
            .expect("pending staged barrier");
        assert!(replay_sync < staged_sync);
    }

    #[tokio::test]
    async fn local_if_changed_equality_retry_repeats_barriers() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("value"), b"old").expect("seed");
        let destination = dir.path().join("value");
        let fail_once = Arc::new(AtomicBool::new(true));
        let observed_fail = Arc::clone(&fail_once);
        let observed_destination = destination.clone();
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            if matches!(event, DurabilitySyncEvent::File(path) if path == &observed_destination)
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected destination sync failure".to_string(),
                ));
            }
            Ok(())
        });
        let write = VfsStorageWrite {
            path: "value".to_string(),
            bytes: Bytes::from_static(b"desired"),
            token_count: None,
            precondition: None,
        };

        assert!(
            storage
                .write_many_if_changed_atomic(vec![write.clone()])
                .await
                .is_err()
        );
        let replay = storage
            .write_many_if_changed_atomic(vec![write])
            .await
            .expect("if-changed equality replay");
        assert_eq!(replay.len(), 1);
        assert!(!replay[0].changed);
    }

    #[tokio::test]
    async fn local_streamed_write_exact_replay_repeats_barriers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let staging = tempfile::tempdir().expect("staging");
        fs::write(dir.path().join("value"), b"old").expect("seed");
        let source = staging.path().join("source");
        fs::write(&source, b"desired").expect("source");
        let destination = dir.path().join("value");
        let fail_once = Arc::new(AtomicBool::new(true));
        let observed_fail = Arc::clone(&fail_once);
        let observed_destination = destination.clone();
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            if matches!(event, DurabilitySyncEvent::File(path) if path == &observed_destination)
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected destination sync failure".to_string(),
                ));
            }
            Ok(())
        });
        let precondition = VfsStorageWritePrecondition {
            predicate: None,
            fingerprint: Some(hex_hash(b"old")),
            secondary_fingerprint: None,
            expected_file_id: None,
        };

        assert!(
            storage
                .write_from_local_file(
                    "value",
                    &source,
                    Some(hex_hash(b"desired").as_str()),
                    Some(precondition.clone()),
                    None,
                )
                .await
                .is_err()
        );
        storage
            .write_from_local_file(
                "value",
                &source,
                Some(hex_hash(b"desired").as_str()),
                Some(precondition),
                None,
            )
            .await
            .expect("streamed exact replay");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_direct_namespace_routes_sync_affected_directory_chains() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("left")).expect("left");
        fs::create_dir_all(dir.path().join("right")).expect("right");
        fs::write(dir.path().join("left/source"), b"source").expect("source");
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed.lock().unwrap().push(event.clone());
            Ok(())
        });

        storage
            .rename_with_metadata("left/source", "right/destination")
            .await
            .expect("direct rename");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("left")),
                DurabilitySyncEvent::Directory(dir.path().join("right")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );

        events.lock().unwrap().clear();
        storage
            .create_hard_link("right/destination", "nested/alias")
            .await
            .expect("direct hard link");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("nested")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );

        events.lock().unwrap().clear();
        storage.mkdir("empty/leaf").await.expect("direct mkdir");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("empty/leaf")),
                DurabilitySyncEvent::Directory(dir.path().join("empty")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );

        events.lock().unwrap().clear();
        storage
            .create_symlink("right/link", "destination")
            .await
            .expect("direct symlink");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("right")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );

        events.lock().unwrap().clear();
        storage
            .delete_file_with_metadata("right/link", None)
            .await
            .expect("direct unlink");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("right")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );

        events.lock().unwrap().clear();
        storage.rmdir("empty/leaf").await.expect("direct rmdir");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("empty")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_hard_link_sync_failure_prevents_success_ack() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("source")).expect("source directory");
        fs::write(dir.path().join("source/value"), b"value").expect("source file");
        let fail_once = Arc::new(AtomicBool::new(true));
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed_fail = Arc::clone(&fail_once);
        let observed_events = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed_events.lock().unwrap().push(event.clone());
            if matches!(event, DurabilitySyncEvent::Directory(_))
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected directory sync failure".to_string(),
                ));
            }
            Ok(())
        });

        let first = storage
            .create_hard_link("source/value", "destination/value")
            .await;
        assert!(first.is_err(), "directory barrier failure must prevent ack");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[DurabilitySyncEvent::Directory(
                dir.path().join("destination")
            )],
        );
        assert!(
            fs::symlink_metadata(dir.path().join("destination/value")).is_ok(),
            "the uncertain outcome may have installed the directory entry",
        );

        let retry = storage
            .create_hard_link("source/value", "destination/value")
            .await
            .expect("same-inode hard-link replay");
        assert_eq!(retry.source.file_id, retry.destination.file_id);
        assert_eq!(
            &events.lock().unwrap()[1..],
            &[
                DurabilitySyncEvent::Directory(dir.path().join("destination")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );

        fs::write(dir.path().join("destination/different"), b"different").expect("different");
        let conflict = storage
            .create_hard_link("source/value", "destination/different")
            .await;
        assert!(matches!(conflict, Err(VfsStorageError::Conflict(_))));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_missing_unlink_retry_repeats_parent_barrier() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("nested")).expect("nested directory");
        fs::write(dir.path().join("nested/value"), b"value").expect("file");
        let fail_once = Arc::new(AtomicBool::new(true));
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed_fail = Arc::clone(&fail_once);
        let observed_events = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed_events.lock().unwrap().push(event.clone());
            if matches!(event, DurabilitySyncEvent::Directory(_))
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected directory sync failure".to_string(),
                ));
            }
            Ok(())
        });

        let precondition = VfsStorageWritePrecondition {
            predicate: None,
            fingerprint: Some(hex_hash(b"value")),
            secondary_fingerprint: None,
            expected_file_id: None,
        };
        let first = storage
            .delete_file_with_metadata("nested/value", Some(precondition.clone()))
            .await;
        assert!(first.is_err(), "directory barrier failure must prevent ack");
        assert!(!dir.path().join("nested/value").exists());

        events.lock().unwrap().clear();
        storage
            .delete_file_with_metadata("nested/value", Some(precondition))
            .await
            .expect("missing-path replay must complete the parent barrier");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("nested")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_missing_rmdir_retry_repeats_parent_barrier() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("nested/empty")).expect("empty directory");
        let fail_once = Arc::new(AtomicBool::new(true));
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed_fail = Arc::clone(&fail_once);
        let observed_events = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed_events.lock().unwrap().push(event.clone());
            if matches!(event, DurabilitySyncEvent::Directory(_))
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected directory sync failure".to_string(),
                ));
            }
            Ok(())
        });

        let first = storage.rmdir("nested/empty").await;
        assert!(first.is_err(), "directory barrier failure must prevent ack");
        assert!(!dir.path().join("nested/empty").exists());

        events.lock().unwrap().clear();
        storage
            .rmdir("nested/empty")
            .await
            .expect("missing-path replay must complete the parent barrier");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("nested")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_normalized_symlink_and_rename_retries_repeat_parent_barriers() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("from")).expect("from");
        fs::create_dir_all(dir.path().join("to")).expect("to");
        fs::write(dir.path().join("from/value"), b"value").expect("value");
        let fail_next = Arc::new(AtomicBool::new(true));
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed_fail = Arc::clone(&fail_next);
        let observed_events = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed_events.lock().unwrap().push(event.clone());
            if matches!(event, DurabilitySyncEvent::Directory(_))
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected directory sync failure".to_string(),
                ));
            }
            Ok(())
        });

        assert!(
            storage
                .create_symlink("to/link", "./missing")
                .await
                .is_err()
        );
        events.lock().unwrap().clear();
        storage
            .create_symlink("to/link", "./missing")
            .await
            .expect("normalized symlink replay");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("to")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );

        fail_next.store(true, AtomicBoolOrdering::SeqCst);
        events.lock().unwrap().clear();
        assert!(
            storage
                .rename_with_metadata("from/value", "to/value")
                .await
                .is_err()
        );
        events.lock().unwrap().clear();
        let replay = storage
            .rename_with_metadata("from/value", "to/value")
            .await
            .expect("missing-source rename replay");
        assert!(replay.previous.is_none());
        assert!(replay.current.is_some());
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("from")),
                DurabilitySyncEvent::Directory(dir.path().join("to")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );

        events.lock().unwrap().clear();
        let initial_missing = storage
            .rename_with_metadata("missing/source", "missing/destination")
            .await
            .expect("initial missing rename contract");
        assert!(initial_missing.previous.is_none());
        assert!(initial_missing.current.is_none());
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[DurabilitySyncEvent::Directory(dir.path().to_path_buf())],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_conditional_delete_batch_replay_completes_parent_barrier() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("nested")).expect("nested directory");
        fs::write(dir.path().join("nested/value"), b"value").expect("file");
        let fail_once = Arc::new(AtomicBool::new(true));
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed_fail = Arc::clone(&fail_once);
        let observed_events = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed_events.lock().unwrap().push(event.clone());
            if matches!(event, DurabilitySyncEvent::Directory(_))
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected directory sync failure".to_string(),
                ));
            }
            Ok(())
        });
        let mutation = VfsStorageNamespaceMutation::DeleteFile {
            path: "nested/value".to_string(),
            precondition: Some(VfsStorageWritePrecondition {
                predicate: None,
                fingerprint: Some(hex_hash(b"value")),
                secondary_fingerprint: None,
                expected_file_id: None,
            }),
        };

        let first = storage.apply_namespace_batch(vec![mutation.clone()]).await;
        assert!(first.is_err(), "directory barrier failure must prevent ack");
        assert!(!dir.path().join("nested/value").exists());

        events.lock().unwrap().clear();
        storage
            .apply_namespace_batch(vec![mutation])
            .await
            .expect("conditional replay must complete the parent barrier");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("nested")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_namespace_batch_skips_removed_directory_and_syncs_surviving_ancestors() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("parent/empty")).expect("empty directory");
        fs::write(dir.path().join("parent/empty/value"), b"value").expect("file");
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed.lock().unwrap().push(event.clone());
            Ok(())
        });

        storage
            .apply_namespace_batch(vec![
                VfsStorageNamespaceMutation::DeleteFile {
                    path: "parent/empty/value".to_string(),
                    precondition: None,
                },
                VfsStorageNamespaceMutation::RemoveDirectory {
                    path: "parent/empty".to_string(),
                },
            ])
            .await
            .expect("ordered delete and rmdir");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("parent")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_namespace_batch_remaps_touched_descendants_across_ancestor_rename() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("a/sub")).expect("subtree");
        fs::write(dir.path().join("a/sub/delete"), b"delete").expect("delete");
        fs::write(dir.path().join("a/sub/keep"), b"keep").expect("keep");
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed.lock().unwrap().push(event.clone());
            Ok(())
        });

        storage
            .apply_namespace_batch(vec![
                VfsStorageNamespaceMutation::DeleteFile {
                    path: "a/sub/delete".to_string(),
                    precondition: None,
                },
                VfsStorageNamespaceMutation::Rename {
                    from: "a".to_string(),
                    to: "b".to_string(),
                },
            ])
            .await
            .expect("descendant mutation and ancestor rename");
        assert!(dir.path().join("b/sub/keep").exists());
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("b/sub")),
                DurabilitySyncEvent::Directory(dir.path().join("b")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_namespace_batch_normalized_symlink_replay_converges() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fail_once = Arc::new(AtomicBool::new(true));
        let observed_fail = Arc::clone(&fail_once);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            if matches!(event, DurabilitySyncEvent::Directory(_))
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected directory sync failure".to_string(),
                ));
            }
            Ok(())
        });
        let mutation = VfsStorageNamespaceMutation::CreateSymlink {
            path: "links/value".to_string(),
            target: "./missing".to_string(),
        };

        assert!(
            storage
                .apply_namespace_batch(vec![mutation.clone()])
                .await
                .is_err()
        );
        storage
            .apply_namespace_batch(vec![mutation])
            .await
            .expect("canonical symlink replay");
        assert_eq!(
            fs::read_link(dir.path().join("links/value")).expect("link"),
            PathBuf::from("missing"),
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_namespace_batch_absent_parent_noop_syncs_existing_ancestor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed.lock().unwrap().push(event.clone());
            Ok(())
        });

        storage
            .apply_namespace_batch(vec![
                VfsStorageNamespaceMutation::DeleteFile {
                    path: "missing/deep/value".to_string(),
                    precondition: None,
                },
                VfsStorageNamespaceMutation::RemoveDirectory {
                    path: "missing/deep".to_string(),
                },
            ])
            .await
            .expect("absent subtree replay");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[DurabilitySyncEvent::Directory(dir.path().to_path_buf())],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_namespace_sync_failure_prevents_ack_and_replay_converges() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("from")).expect("from");
        fs::create_dir_all(dir.path().join("to")).expect("to");
        fs::write(dir.path().join("from/value"), b"value").expect("value");
        let fail_once = Arc::new(AtomicBool::new(true));
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed_fail = Arc::clone(&fail_once);
        let observed_events = Arc::clone(&events);
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            observed_events.lock().unwrap().push(event.clone());
            if matches!(event, DurabilitySyncEvent::Directory(_))
                && observed_fail.swap(false, AtomicBoolOrdering::SeqCst)
            {
                return Err(VfsStorageError::Internal(
                    "injected directory sync failure".to_string(),
                ));
            }
            Ok(())
        });
        let mutation = VfsStorageNamespaceMutation::Rename {
            from: "from/value".to_string(),
            to: "to/value".to_string(),
        };

        let first = storage.apply_namespace_batch(vec![mutation.clone()]).await;
        assert!(first.is_err(), "directory barrier failure must prevent ack");
        assert!(!dir.path().join("from/value").exists());
        assert!(dir.path().join("to/value").exists());

        events.lock().unwrap().clear();
        storage
            .apply_namespace_batch(vec![mutation])
            .await
            .expect("idempotent replay must complete the directory barrier");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[
                DurabilitySyncEvent::Directory(dir.path().join("from")),
                DurabilitySyncEvent::Directory(dir.path().join("to")),
                DurabilitySyncEvent::Directory(dir.path().to_path_buf()),
            ],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_namespace_batch_renames_dangling_symlink() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("from")).expect("from");
        fs::create_dir_all(dir.path().join("to")).expect("to");
        symlink("missing-target", dir.path().join("from/link")).expect("dangling symlink");
        let storage = LocalVfsStorage::new(dir.path());

        storage
            .apply_namespace_batch(vec![VfsStorageNamespaceMutation::Rename {
                from: "from/link".to_string(),
                to: "to/link".to_string(),
            }])
            .await
            .expect("rename dangling symlink");

        assert!(fs::symlink_metadata(dir.path().join("from/link")).is_err());
        assert_eq!(
            fs::read_link(dir.path().join("to/link")).expect("renamed symlink"),
            PathBuf::from("missing-target"),
        );
    }

    #[tokio::test]
    async fn path_locks_allow_siblings_and_exclude_ancestor_mutations() {
        let table = PathLockTable::default();
        let first_sibling = table.lock(["tree/a.txt".to_string()]).await;
        let tree_lock = table
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get("tree")
            .expect("tree intent lock")
            .clone();
        assert!(
            tree_lock.clone().try_read_owned().is_ok(),
            "sibling mutation should be able to share the ancestor intent lock",
        );
        assert!(
            tree_lock.clone().try_write_owned().is_err(),
            "ancestor mutation must wait for descendant mutations",
        );
        let second_sibling = table.lock(["tree/b.txt".to_string()]).await;

        drop(second_sibling);
        drop(first_sibling);
        assert!(
            tree_lock.try_write_owned().is_ok(),
            "ancestor mutation should proceed after descendants release",
        );
    }

    #[tokio::test]
    async fn path_locks_point_reads_exclude_exact_mutations_without_stalling_descendants() {
        let table = PathLockTable::default();
        let directory_read = table.lock_read(["tree".to_string()]).await;
        let tree_lock = table
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get("tree")
            .expect("tree point-read lock")
            .clone();
        assert!(
            tree_lock.clone().try_write_owned().is_err(),
            "an exact directory mutation must wait for its point read",
        );
        let descendant_intent = tree_lock
            .clone()
            .try_read_owned()
            .expect("a directory point read must allow descendant intent reads");
        drop(descendant_intent);
        drop(directory_read);
        assert!(
            tree_lock.try_write_owned().is_ok(),
            "the exact directory mutation should proceed after its read releases",
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_path_lock_covers_cached_and_live_file_identities() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write("path", Bytes::from_static(b"cached"), None)
            .await
            .expect("cached inode");
        let cached_id = storage
            .stat("path")
            .await
            .expect("cached stat")
            .expect("cached metadata")
            .file_id
            .expect("cached identity");
        fs::rename(dir.path().join("path"), dir.path().join("old-alias"))
            .expect("replace cached path");
        fs::write(dir.path().join("path"), b"live").expect("live inode");
        let live_id =
            local_file_id(&fs::symlink_metadata(dir.path().join("path")).expect("live metadata"))
                .expect("live identity");
        assert_ne!(cached_id, live_id);

        let locks = storage.lock_write_paths(["path".to_string()]).await;
        assert!(locks.keys.contains(&format!("\0inode:{cached_id}")));
        assert!(locks.keys.contains(&format!("\0inode:{live_id}")));
        assert!(locks.keys.contains(&"path".to_string()));
    }

    #[tokio::test]
    async fn local_storage_round_trips_batch_and_range_reads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write_many_atomic(vec![
                VfsStorageWrite {
                    path: "a/one.txt".to_string(),
                    bytes: Bytes::from_static(b"abcdef"),
                    token_count: None,
                    precondition: None,
                },
                VfsStorageWrite {
                    path: "a/two.txt".to_string(),
                    bytes: Bytes::from_static(b"ghijkl"),
                    token_count: None,
                    precondition: None,
                },
            ])
            .await
            .expect("write_many");

        let range = storage
            .read_range(
                "a/one.txt",
                VfsStorageReadRange {
                    offset: 2,
                    length: 3,
                },
            )
            .await
            .expect("range");
        assert_eq!(&range[..], b"cde");

        let many = storage
            .read_many(&[
                "a/one.txt".to_string(),
                "missing.txt".to_string(),
                "a/two.txt".to_string(),
            ])
            .await
            .expect("read_many");
        assert_eq!(many.len(), 2);
        assert_eq!(&many[0].1[..], b"abcdef");
        assert_eq!(&many[1].1[..], b"ghijkl");
    }

    #[tokio::test]
    async fn namespace_batch_is_ordered_and_replay_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write("incoming.txt", Bytes::from_static(b"payload"), None)
            .await
            .expect("seed file");
        let mutations = vec![
            VfsStorageNamespaceMutation::CreateDirectory {
                path: "tree".to_string(),
                mode: None,
            },
            VfsStorageNamespaceMutation::Rename {
                from: "incoming.txt".to_string(),
                to: "tree/result.txt".to_string(),
            },
            VfsStorageNamespaceMutation::DeleteFile {
                path: "tree/result.txt".to_string(),
                precondition: None,
            },
            VfsStorageNamespaceMutation::RemoveDirectory {
                path: "tree".to_string(),
            },
        ];

        storage
            .apply_namespace_batch(mutations.clone())
            .await
            .expect("apply namespace batch");
        storage
            .apply_namespace_batch(mutations)
            .await
            .expect("replay namespace batch");

        assert!(
            storage
                .stat("incoming.txt")
                .await
                .expect("stat source")
                .is_none()
        );
        assert!(storage.stat("tree").await.expect("stat tree").is_none());
    }

    #[tokio::test]
    async fn namespace_rename_replay_converges_identical_directory_trees() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        for root in ["source", "destination"] {
            storage
                .write(
                    format!("{root}/nested/package.json").as_str(),
                    Bytes::from_static(br#"{"name":"same"}"#),
                    None,
                )
                .await
                .expect("seed identical tree");
        }

        storage
            .apply_namespace_batch(vec![VfsStorageNamespaceMutation::Rename {
                from: "source".to_string(),
                to: "destination".to_string(),
            }])
            .await
            .expect("replay identical rename");

        assert!(storage.stat("source").await.expect("stat source").is_none());
        assert!(
            storage
                .stat("destination/nested/package.json")
                .await
                .expect("stat destination")
                .is_some()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn namespace_rename_replay_converges_content_with_different_permissions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        for root in ["source", "destination"] {
            storage
                .write(
                    format!("{root}/package.json").as_str(),
                    Bytes::from_static(br#"{"name":"same"}"#),
                    None,
                )
                .await
                .expect("seed identical content");
        }
        fs::set_permissions(dir.path().join("source"), fs::Permissions::from_mode(0o700))
            .expect("chmod source directory");
        fs::set_permissions(
            dir.path().join("source/package.json"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("chmod source file");
        fs::set_permissions(
            dir.path().join("destination"),
            fs::Permissions::from_mode(0o775),
        )
        .expect("chmod destination directory");
        fs::set_permissions(
            dir.path().join("destination/package.json"),
            fs::Permissions::from_mode(0o664),
        )
        .expect("chmod destination file");

        storage
            .apply_namespace_batch(vec![VfsStorageNamespaceMutation::Rename {
                from: "source".to_string(),
                to: "destination".to_string(),
            }])
            .await
            .expect("replay content-equivalent rename");

        assert!(storage.stat("source").await.expect("stat source").is_none());
        assert_eq!(
            fs::symlink_metadata(dir.path().join("destination"))
                .expect("destination directory")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::symlink_metadata(dir.path().join("destination/package.json"))
                .expect("destination file")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[tokio::test]
    async fn namespace_rename_replay_preserves_different_directory_trees() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write(
                "source/package.json",
                Bytes::from_static(br#"{"name":"source"}"#),
                None,
            )
            .await
            .expect("seed source");
        storage
            .write(
                "destination/package.json",
                Bytes::from_static(br#"{"name":"destination"}"#),
                None,
            )
            .await
            .expect("seed destination");

        let result = storage
            .apply_namespace_batch(vec![VfsStorageNamespaceMutation::Rename {
                from: "source".to_string(),
                to: "destination".to_string(),
            }])
            .await;

        assert!(matches!(result, Err(VfsStorageError::Conflict(_))));
        assert!(
            storage
                .stat("source/package.json")
                .await
                .expect("stat source")
                .is_some()
        );
        assert!(
            storage
                .stat("destination/package.json")
                .await
                .expect("stat destination")
                .is_some()
        );
    }

    #[tokio::test]
    async fn namespace_delete_batch_preflights_all_preconditions_before_mutating() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write("a.txt", Bytes::from_static(b"a"), None)
            .await
            .expect("seed a");
        storage
            .write("b.txt", Bytes::from_static(b"b"), None)
            .await
            .expect("seed b");
        let hash_a = hex_hash(b"a");

        let failed = storage
            .apply_namespace_batch(vec![
                VfsStorageNamespaceMutation::DeleteFile {
                    path: "a.txt".to_string(),
                    precondition: Some(VfsStorageWritePrecondition {
                        predicate: None,
                        fingerprint: Some(hash_a.clone()),
                        secondary_fingerprint: None,
                        expected_file_id: None,
                    }),
                },
                VfsStorageNamespaceMutation::DeleteFile {
                    path: "b.txt".to_string(),
                    precondition: Some(VfsStorageWritePrecondition {
                        predicate: None,
                        fingerprint: Some("stale".to_string()),
                        secondary_fingerprint: None,
                        expected_file_id: None,
                    }),
                },
            ])
            .await;
        assert!(matches!(failed, Err(VfsStorageError::Conflict(_))));
        assert!(storage.stat("a.txt").await.expect("stat a").is_some());
        assert!(storage.stat("b.txt").await.expect("stat b").is_some());

        storage
            .apply_namespace_batch(vec![
                VfsStorageNamespaceMutation::DeleteFile {
                    path: "a.txt".to_string(),
                    precondition: Some(VfsStorageWritePrecondition {
                        predicate: None,
                        fingerprint: Some(hash_a),
                        secondary_fingerprint: None,
                        expected_file_id: None,
                    }),
                },
                VfsStorageNamespaceMutation::DeleteFile {
                    path: "b.txt".to_string(),
                    precondition: Some(VfsStorageWritePrecondition {
                        predicate: None,
                        fingerprint: Some(hex_hash(b"b")),
                        secondary_fingerprint: None,
                        expected_file_id: None,
                    }),
                },
            ])
            .await
            .expect("conditional delete batch");
        assert!(storage.stat("a.txt").await.expect("stat a").is_none());
        assert!(storage.stat("b.txt").await.expect("stat b").is_none());
    }

    #[tokio::test]
    async fn changed_only_write_skips_identical_content() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write("note.txt", Bytes::from_static(b"same"), None)
            .await
            .expect("initial write");

        let results = storage
            .write_many_if_changed_atomic(vec![
                VfsStorageWrite {
                    path: "note.txt".to_string(),
                    bytes: Bytes::from_static(b"same"),
                    token_count: None,
                    precondition: None,
                },
                VfsStorageWrite {
                    path: "other.txt".to_string(),
                    bytes: Bytes::from_static(b"new"),
                    token_count: None,
                    precondition: None,
                },
            ])
            .await
            .expect("changed write");

        let by_path: HashMap<_, _> = results
            .into_iter()
            .map(|result| (result.path.clone(), result))
            .collect();
        assert!(!by_path["note.txt"].changed);
        assert!(by_path["other.txt"].changed);
    }

    #[tokio::test]
    async fn local_storage_lists_metadata_and_subtree_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage.mkdir("root/child").await.expect("mkdir");
        storage
            .write("root/a.txt", Bytes::from_static(b"a"), None)
            .await
            .expect("write a");
        storage
            .write("root/child/b.md", Bytes::from_static(b"b"), None)
            .await
            .expect("write b");

        let listed = storage
            .list_dir_with_metadata(
                "root",
                VfsStorageDirListFilter {
                    name_not_like: Some("%.digest-%".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("list dir");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].kind, VfsStorageEntryKind::Directory);

        let subtree = storage
            .list_subtree_file_metadata("root", VfsStorageSubtreeOptions::default())
            .await
            .expect("subtree");
        assert_eq!(
            subtree
                .into_iter()
                .map(|entry| entry.path)
                .collect::<Vec<_>>(),
            vec!["root/a.txt".to_string(), "root/child/b.md".to_string()]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn in_scope_file_symlink_is_listed_with_target_but_not_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("target.txt"), b"target").expect("target file");
        symlink("target.txt", dir.path().join("link.txt")).expect("symlink");

        let storage = LocalVfsStorage::new(dir.path());
        let metadata = storage
            .stat("link.txt")
            .await
            .expect("stat")
            .expect("symlink metadata");
        assert_eq!(metadata.kind, VfsStorageEntryKind::Symlink);
        assert_eq!(metadata.link_target.as_deref(), Some("target.txt"));
        assert_eq!(metadata.content_hash, None);

        let listed = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("list dir");
        let link = listed
            .iter()
            .find(|entry| entry.path == "link.txt")
            .expect("listed symlink");
        assert_eq!(link.kind, VfsStorageEntryKind::Symlink);
        assert_eq!(link.link_target.as_deref(), Some("target.txt"));

        let subtree = storage
            .list_subtree_file_metadata("", VfsStorageSubtreeOptions::default())
            .await
            .expect("subtree");
        assert!(subtree.iter().any(|entry| {
            entry.path == "link.txt"
                && entry.kind == VfsStorageEntryKind::Symlink
                && entry.link_target.as_deref() == Some("target.txt")
        }));

        let err = storage
            .read("link.txt")
            .await
            .expect_err("symlink read rejected");
        assert_eq!(
            err,
            VfsStorageError::BadRequest("unsupported file type: symlink".to_string())
        );
        let err = storage
            .read_range(
                "link.txt",
                VfsStorageReadRange {
                    offset: 0,
                    length: 1,
                },
            )
            .await
            .expect_err("symlink range read rejected");
        assert_eq!(
            err,
            VfsStorageError::BadRequest("unsupported file type: symlink".to_string())
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn escaping_symlink_is_absent_from_listings_and_read_stat_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let outside_file = outside.path().join("secret.txt");
        fs::write(&outside_file, b"secret").expect("outside file");
        symlink(&outside_file, dir.path().join("secret.txt")).expect("symlink");

        let storage = LocalVfsStorage::new(dir.path());
        let listed = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("list dir");
        assert!(listed.iter().all(|entry| entry.path != "secret.txt"));

        let subtree = storage
            .list_subtree_file_metadata("", VfsStorageSubtreeOptions::default())
            .await
            .expect("subtree");
        assert!(subtree.iter().all(|entry| entry.path != "secret.txt"));

        let err = storage
            .stat("secret.txt")
            .await
            .expect_err("escaping symlink stat rejected");
        assert_eq!(
            err,
            VfsStorageError::BadRequest("unsupported file type: symlink".to_string())
        );

        let err = storage
            .read("secret.txt")
            .await
            .expect_err("symlink read rejected");
        assert_eq!(
            err,
            VfsStorageError::BadRequest("unsupported file type: symlink".to_string())
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn in_scope_dir_symlink_is_listed_but_not_recursed_in_subtree_listing() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("a.txt"), b"a").expect("file");
        fs::create_dir(dir.path().join("real-dir")).expect("real dir");
        fs::write(dir.path().join("real-dir").join("nested.txt"), b"nested").expect("nested file");
        symlink("real-dir", dir.path().join("dir-link")).expect("dir symlink");
        let storage = LocalVfsStorage::new(dir.path());

        let listed = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("list dir");
        let link = listed
            .iter()
            .find(|entry| entry.path == "dir-link")
            .expect("listed dir symlink");
        assert_eq!(link.kind, VfsStorageEntryKind::Symlink);
        assert_eq!(link.link_target.as_deref(), Some("real-dir"));

        let subtree = storage
            .list_subtree_file_metadata("", VfsStorageSubtreeOptions::default())
            .await
            .expect("subtree");
        assert_eq!(
            subtree
                .into_iter()
                .map(|entry| entry.path)
                .collect::<Vec<_>>(),
            vec![
                "a.txt".to_string(),
                "dir-link".to_string(),
                "real-dir/nested.txt".to_string()
            ]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_symlink_accepts_in_scope_target_and_rejects_escape() {
        let base = tempfile::tempdir().expect("base tempdir");
        let root = base.path().join("root");
        let outside = base.path().join("outside");
        fs::create_dir_all(&root).expect("root");
        fs::create_dir_all(&outside).expect("outside");
        fs::write(root.join("target.txt"), b"target").expect("target");
        fs::write(outside.join("secret.txt"), b"secret").expect("secret");

        let storage = LocalVfsStorage::new(&root);
        storage
            .create_symlink("link.txt", "target.txt")
            .await
            .expect("create in-scope symlink");
        let metadata = storage
            .stat("link.txt")
            .await
            .expect("stat")
            .expect("symlink metadata");
        assert_eq!(metadata.kind, VfsStorageEntryKind::Symlink);
        assert_eq!(metadata.link_target.as_deref(), Some("target.txt"));

        let err = storage
            .create_symlink("bad-link.txt", "../outside/secret.txt")
            .await
            .expect_err("escaping symlink rejected");
        assert_eq!(
            err,
            VfsStorageError::BadRequest("unsupported file type: symlink".to_string())
        );
        assert!(!root.join("bad-link.txt").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_target_fingerprint_guards_delete() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        fs::write(dir.path().join("target.txt"), b"target").expect("target");
        storage
            .create_symlink("link.txt", "target.txt")
            .await
            .expect("create symlink");
        let expected = format!("symlink:{}", hex_hash(b"target.txt"));

        let stale = storage
            .delete_file_with_metadata(
                "link.txt",
                Some(VfsStorageWritePrecondition {
                    predicate: None,
                    fingerprint: Some("symlink:stale".to_string()),
                    secondary_fingerprint: None,
                    expected_file_id: None,
                }),
            )
            .await;
        assert!(matches!(stale, Err(VfsStorageError::Conflict(_))));
        assert!(dir.path().join("link.txt").exists());

        storage
            .delete_file_with_metadata(
                "link.txt",
                Some(VfsStorageWritePrecondition {
                    predicate: None,
                    fingerprint: Some(expected),
                    secondary_fingerprint: None,
                    expected_file_id: None,
                }),
            )
            .await
            .expect("matching symlink delete");
        assert!(!dir.path().join("link.txt").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_symlink_accepts_dangling_in_scope_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage.mkdir("proj").await.expect("mkdir");

        storage
            .create_symlink("proj/link.txt", "target.txt")
            .await
            .expect("dangling symlink is legal");
        let metadata = storage
            .stat("proj/link.txt")
            .await
            .expect("stat link")
            .expect("link metadata");
        assert_eq!(metadata.kind, VfsStorageEntryKind::Symlink);
        assert_eq!(metadata.link_target.as_deref(), Some("target.txt"));

        storage
            .write("proj/target.txt", Bytes::from_static(b"later"), None)
            .await
            .expect("create target later");
        let followed = fs::read(dir.path().join("proj/link.txt")).expect("host follows link");
        assert_eq!(followed, b"later");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_symlink_rejects_lexical_parent_escape() {
        let base = tempfile::tempdir().expect("base tempdir");
        let root = base.path().join("root");
        fs::create_dir_all(root.join("proj")).expect("root");
        let storage = LocalVfsStorage::new(&root);

        let err = storage
            .create_symlink("proj/bad-link.txt", "../../outside/secret.txt")
            .await
            .expect_err("lexical escape rejected");
        assert_eq!(
            err,
            VfsStorageError::BadRequest("unsupported file type: symlink".to_string())
        );
        assert!(!root.join("proj/bad-link.txt").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_symlink_rejects_existing_escaping_symlink_chain() {
        let base = tempfile::tempdir().expect("base tempdir");
        let root = base.path().join("root");
        let outside = base.path().join("outside");
        fs::create_dir_all(&root).expect("root");
        fs::create_dir_all(&outside).expect("outside");
        fs::write(outside.join("secret.txt"), b"secret").expect("secret");
        symlink(&outside, root.join("escape")).expect("escape symlink");
        let storage = LocalVfsStorage::new(&root);

        let err = storage
            .create_symlink("bad-link.txt", "escape/secret.txt")
            .await
            .expect_err("canonical symlink chain escape rejected");
        assert_eq!(
            err,
            VfsStorageError::BadRequest("unsupported file type: symlink".to_string())
        );
        assert!(!root.join("bad-link.txt").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn absolute_in_scope_symlink_target_is_reported_and_stored_relative() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir(dir.path().join("dir")).expect("dir");
        fs::write(dir.path().join("target.txt"), b"target").expect("target");
        let storage = LocalVfsStorage::new(dir.path());
        let absolute_target = dir.path().join("target.txt");

        storage
            .create_symlink(
                "dir/link.txt",
                absolute_target.to_str().expect("absolute target"),
            )
            .await
            .expect("create absolute in-scope symlink");
        assert_eq!(
            fs::read_link(dir.path().join("dir/link.txt")).expect("read link"),
            PathBuf::from("../target.txt")
        );
        let metadata = storage
            .stat("dir/link.txt")
            .await
            .expect("stat")
            .expect("link metadata");
        assert_eq!(metadata.link_target.as_deref(), Some("../target.txt"));

        symlink(&absolute_target, dir.path().join("raw-absolute")).expect("raw symlink");
        let raw = storage
            .stat("raw-absolute")
            .await
            .expect("stat raw")
            .expect("raw metadata");
        assert_eq!(raw.link_target.as_deref(), Some("target.txt"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fifo_is_excluded_from_listings_and_read_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("regular.txt"), b"regular").expect("regular file");
        let status = Command::new("mkfifo")
            .arg(dir.path().join("pipe"))
            .status()
            .expect("mkfifo command");
        assert!(status.success());
        let storage = LocalVfsStorage::new(dir.path());

        let listed = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("list dir");
        assert_eq!(
            listed
                .into_iter()
                .map(|entry| entry.path)
                .collect::<Vec<_>>(),
            vec!["regular.txt".to_string()]
        );

        let subtree = storage
            .list_subtree_file_metadata("", VfsStorageSubtreeOptions::default())
            .await
            .expect("subtree");
        assert_eq!(
            subtree
                .into_iter()
                .map(|entry| entry.path)
                .collect::<Vec<_>>(),
            vec!["regular.txt".to_string()]
        );

        let metadata = storage
            .stat("pipe")
            .await
            .expect("stat")
            .expect("fifo metadata");
        assert_eq!(metadata.kind, VfsStorageEntryKind::Special);

        let err = storage.read("pipe").await.expect_err("fifo read rejected");
        assert_eq!(
            err,
            VfsStorageError::BadRequest("unsupported file type: special".to_string())
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executable_metadata_reflects_regular_file_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write("script.sh", Bytes::from_static(b"#!/bin/sh\n"), None)
            .await
            .expect("write");
        let path = dir.path().join("script.sh");
        let mut permissions = fs::metadata(&path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).expect("chmod");

        let metadata = storage
            .stat("script.sh")
            .await
            .expect("stat")
            .expect("metadata");
        assert!(metadata.executable);
        assert_eq!(metadata.mode, Some(0o755));

        let listed = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("list dir");
        assert!(listed.iter().any(|entry| entry.executable));

        let subtree = storage
            .list_subtree_file_metadata("", VfsStorageSubtreeOptions::default())
            .await
            .expect("subtree");
        assert!(subtree.iter().any(|entry| entry.executable));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exact_write_mode_wins_and_replay_repairs_mode_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        let first = storage
            .write("tool", Bytes::from_static(b"old"), None)
            .await
            .expect("initial write");
        let file_id = storage
            .stat("tool")
            .await
            .expect("stat")
            .expect("metadata")
            .file_id
            .expect("stable identity");
        let precondition = VfsStorageWritePrecondition {
            predicate: None,
            fingerprint: Some(first.content_hash),
            secondary_fingerprint: None,
            expected_file_id: Some(file_id),
        };
        storage
            .write_with_options(
                "tool",
                Bytes::from_static(b"desired"),
                Some(precondition.clone()),
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: Some(0o1000640),
                }),
            )
            .await
            .expect("exact mode write");
        assert_eq!(path_mode(&dir.path().join("tool")), 0o640);
        let metadata = storage.stat("tool").await.expect("stat").expect("metadata");
        assert_eq!(metadata.mode, Some(0o640));
        assert!(!metadata.executable, "exact mode overrides executable");

        fs::set_permissions(dir.path().join("tool"), fs::Permissions::from_mode(0o600))
            .expect("force mode mismatch");
        let replay = storage
            .write_with_options(
                "tool",
                Bytes::from_static(b"desired"),
                Some(precondition),
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: Some(0o640),
                }),
            )
            .await
            .expect("same-byte exact-mode replay");
        assert!(!replay.changed);
        assert_eq!(path_mode(&dir.path().join("tool")), 0o640);

        storage
            .write_with_options(
                "zero",
                Bytes::from_static(b"zero"),
                Some(VfsStorageWritePrecondition {
                    predicate: None,
                    fingerprint: None,
                    secondary_fingerprint: None,
                    expected_file_id: None,
                }),
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: Some(0o000),
                }),
            )
            .await
            .expect("zero-mode write");
        assert_eq!(path_mode(&dir.path().join("zero")), 0o000);
        storage
            .set_mode("zero", 0o600)
            .await
            .expect("recover zero-mode file");
        assert_eq!(
            fs::read(dir.path().join("zero")).expect("zero-mode bytes"),
            b"zero",
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_rewrites_read_only_inode_without_changing_identity_or_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        let reserved = storage
            .write_with_options(
                "git-object",
                Bytes::new(),
                Some(VfsStorageWritePrecondition {
                    predicate: Some(VfsStorageCasPredicate::Absent),
                    fingerprint: None,
                    secondary_fingerprint: None,
                    expected_file_id: None,
                }),
                Some(VfsStorageWriteOptions {
                    executable: false,
                    mode: Some(0o444),
                }),
            )
            .await
            .expect("reserve read-only object");
        let before = storage
            .stat("git-object")
            .await
            .expect("stat")
            .expect("metadata");
        let file_id = before.file_id.clone().expect("stable identity");

        storage
            .write(
                "git-object",
                Bytes::from_static(b"compressed-git-object"),
                Some(VfsStorageWritePrecondition {
                    predicate: Some(VfsStorageCasPredicate::ContentFingerprint {
                        fingerprint: reserved.content_hash,
                    }),
                    fingerprint: None,
                    secondary_fingerprint: None,
                    expected_file_id: Some(file_id.clone()),
                }),
            )
            .await
            .expect("rewrite through the already-open guest semantics");

        assert_eq!(
            fs::read(dir.path().join("git-object")).expect("object bytes"),
            b"compressed-git-object"
        );
        assert_eq!(path_mode(&dir.path().join("git-object")), 0o444);
        let after = storage
            .stat("git-object")
            .await
            .expect("stat")
            .expect("metadata");
        assert_eq!(after.file_id.as_deref(), Some(file_id.as_str()));
        assert_eq!(after.mode, Some(0o444));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exact_directory_modes_are_idempotent_and_legacy_mkdir_preserves_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .mkdir_with_mode("parent/leaf", Some(0o750))
            .await
            .expect("mode-aware mkdir");
        let leaf = dir.path().join("parent/leaf");
        assert_eq!(path_mode(&leaf), 0o750);
        assert_eq!(
            storage
                .stat("parent/leaf")
                .await
                .expect("stat")
                .expect("metadata")
                .mode,
            Some(0o750),
        );

        fs::set_permissions(&leaf, fs::Permissions::from_mode(0o700))
            .expect("force directory mode mismatch");
        storage
            .apply_namespace_batch(vec![VfsStorageNamespaceMutation::CreateDirectory {
                path: "parent/leaf".to_string(),
                mode: Some(0o750),
            }])
            .await
            .expect("mkdir replay");
        assert_eq!(path_mode(&leaf), 0o750);

        fs::set_permissions(&leaf, fs::Permissions::from_mode(0o711))
            .expect("set legacy preserved mode");
        storage.mkdir("parent/leaf").await.expect("legacy mkdir");
        assert_eq!(path_mode(&leaf), 0o711);

        storage
            .mkdir_with_mode("zero", Some(0o000))
            .await
            .expect("zero-mode mkdir");
        assert_eq!(path_mode(&dir.path().join("zero")), 0o000);
        storage
            .set_mode("zero", 0o700)
            .await
            .expect("recover zero-mode directory");

        storage
            .mkdir_with_mode("umask-proof", Some(0o777))
            .await
            .expect("exact mode must not be masked twice");
        assert_eq!(path_mode(&dir.path().join("umask-proof")), 0o777);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn set_mode_is_replayable_for_files_directories_and_mode_zero_recovery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write("file", Bytes::from_static(b"value"), None)
            .await
            .expect("file");
        storage.mkdir("directory").await.expect("directory");
        let mutations = vec![
            VfsStorageNamespaceMutation::SetMode {
                path: "file".to_string(),
                mode: 0o1000640,
            },
            VfsStorageNamespaceMutation::SetMode {
                path: "directory".to_string(),
                mode: 0o751,
            },
        ];
        storage
            .apply_namespace_batch(mutations.clone())
            .await
            .expect("set modes");
        storage
            .apply_namespace_batch(mutations)
            .await
            .expect("replay set modes");
        assert_eq!(path_mode(&dir.path().join("file")), 0o640);
        assert_eq!(path_mode(&dir.path().join("directory")), 0o751);

        storage.set_mode("file", 0o000).await.expect("lock file");
        storage
            .set_mode("directory", 0o000)
            .await
            .expect("lock directory");
        assert_eq!(path_mode(&dir.path().join("file")), 0o000);
        assert_eq!(path_mode(&dir.path().join("directory")), 0o000);
        storage.set_mode("file", 0o640).await.expect("recover file");
        storage
            .set_mode("directory", 0o755)
            .await
            .expect("recover directory");
        assert_eq!(path_mode(&dir.path().join("file")), 0o640);
        assert_eq!(path_mode(&dir.path().join("directory")), 0o755);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn set_mode_rejects_a_final_symlink_without_touching_its_target() {
        let base = tempfile::tempdir().expect("tempdir");
        let root = base.path().join("root");
        fs::create_dir(&root).expect("root");
        let target = base.path().join("target");
        fs::write(&target, b"target").expect("target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o640)).expect("target mode");
        symlink(&target, root.join("link")).expect("symlink");
        let storage = LocalVfsStorage::new(&root);

        let result = storage.set_mode("link", 0o777).await;
        assert!(matches!(result, Err(VfsStorageError::BadRequest(_))));
        assert_eq!(path_mode(&target), 0o640);
    }

    #[tokio::test]
    async fn local_storage_reuses_hash_cache_for_unchanged_old_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cached.txt");
        fs::write(&path, b"cached").expect("write");
        set_old_mtime(&path);
        let storage = LocalVfsStorage::new(dir.path());

        let first = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("first list");
        assert_eq!(storage.hash_read_count(), 1);
        let second = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("second list");

        assert_eq!(storage.hash_read_count(), 1);
        assert_eq!(first[0].content_hash, second[0].content_hash);
    }

    #[tokio::test]
    async fn local_storage_reuses_trusted_write_hash_immediately() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write("cached.txt", Bytes::from_static(b"cached"), None)
            .await
            .expect("write");

        let first = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("first list");
        let second = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("second list");

        assert_eq!(storage.hash_read_count(), 0);
        assert_eq!(first[0].content_hash, second[0].content_hash);
    }

    #[tokio::test]
    async fn local_storage_rehashes_recent_observed_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cached.txt");
        fs::write(&path, b"cached").expect("write");
        let storage = LocalVfsStorage::new(dir.path());

        storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("first list");
        storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("second list");

        assert_eq!(storage.hash_read_count(), 2);
    }

    #[tokio::test]
    async fn local_storage_rehashes_when_file_stat_changes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cached.txt");
        fs::write(&path, b"cached").expect("write");
        set_old_mtime(&path);
        let storage = LocalVfsStorage::new(dir.path());

        let first = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("first list");
        fs::write(&path, b"changed").expect("rewrite");
        let second = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("second list");

        assert_eq!(storage.hash_read_count(), 2);
        assert_ne!(first[0].content_hash, second[0].content_hash);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_storage_eventually_rehashes_same_size_write_with_restored_mtime() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cached.txt");
        fs::write(&path, b"cached").expect("write");
        set_old_mtime(&path);
        let restored_mtime = fs::metadata(&path)
            .expect("metadata")
            .modified()
            .expect("mtime");
        let storage = LocalVfsStorage::new(dir.path());

        let first = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("first list");
        std::thread::sleep(Duration::from_millis(2));
        fs::write(&path, b"mutate").expect("same-size rewrite");
        fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("open for restored mtime")
            .set_modified(restored_mtime)
            .expect("restore mtime");
        // Some Linux backing filesystems expose ctime at coarse resolution. The
        // bounded cache age is the correctness fallback when size+mtime+ctime
        // all happen to collide.
        storage.expire_cached_hash(&path);
        let second = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("second list");

        assert_eq!(storage.hash_read_count(), 2);
        assert_ne!(first[0].content_hash, second[0].content_hash);
    }

    #[tokio::test]
    async fn local_storage_skips_hash_for_oversized_listing_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("large.bin"), b"large").expect("write");
        let storage = LocalVfsStorage::new(dir.path());

        let entries = storage
            .list_dir_with_metadata(
                "",
                VfsStorageDirListFilter {
                    max_hash_bytes: Some(4),
                    ..Default::default()
                },
            )
            .await
            .expect("list");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].size_bytes, 5);
        assert_eq!(entries[0].content_hash, None);
        assert_eq!(storage.hash_read_count(), 0);
    }

    #[tokio::test]
    async fn local_storage_skips_hash_for_lightweight_stat() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("package.json"), b"{}").expect("write");
        let storage = LocalVfsStorage::new(dir.path());

        let metadata = storage
            .stat_with_metadata_fields(
                "package.json",
                VfsStorageMetadataFields {
                    max_hash_bytes: Some(0),
                    ..Default::default()
                },
            )
            .await
            .expect("stat")
            .expect("metadata");

        assert_eq!(metadata.size_bytes, 2);
        assert_eq!(metadata.content_hash, None);
        assert_eq!(storage.hash_read_count(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_with_executable_option_sets_execute_bits() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write_with_options(
                "bin/tool",
                Bytes::from_static(b"tool"),
                None,
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: None,
                }),
            )
            .await
            .expect("write executable");
        let mode = fs::metadata(dir.path().join("bin/tool"))
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o755);
        let metadata = storage
            .stat("bin/tool")
            .await
            .expect("stat")
            .expect("metadata");
        assert!(metadata.executable);

        storage
            .write("bin/existing", Bytes::from_static(b"old"), None)
            .await
            .expect("write existing");
        let existing = dir.path().join("bin/existing");
        let mut permissions = fs::metadata(&existing).expect("metadata").permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(&existing, permissions).expect("chmod existing");
        storage
            .write_with_options(
                "bin/existing",
                Bytes::from_static(b"new"),
                None,
                Some(VfsStorageWriteOptions {
                    executable: true,
                    mode: None,
                }),
            )
            .await
            .expect("rewrite executable");
        let mode = fs::metadata(existing)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o711);
    }

    #[tokio::test]
    async fn local_storage_enforces_write_preconditions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        let first = storage
            .write("guarded.txt", Bytes::from_static(b"first"), None)
            .await
            .expect("initial write");
        let precondition = VfsStorageWritePrecondition {
            predicate: None,
            fingerprint: Some(first.content_hash),
            secondary_fingerprint: None,
            expected_file_id: None,
        };
        storage
            .write("guarded.txt", Bytes::from_static(b"second"), None)
            .await
            .expect("racing write");
        let err = storage
            .write(
                "guarded.txt",
                Bytes::from_static(b"third"),
                Some(precondition),
            )
            .await
            .expect_err("stale precondition");
        assert!(matches!(err, VfsStorageError::Conflict(_)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_storage_enforces_identity_only_and_combined_preconditions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write("guarded.txt", Bytes::from_static(b"first"), None)
            .await
            .expect("initial write");
        let metadata = storage
            .stat("guarded.txt")
            .await
            .expect("stat")
            .expect("metadata");
        let file_id = metadata.file_id.expect("stable identity");

        storage
            .write(
                "guarded.txt",
                Bytes::from_static(b"identity-only"),
                Some(VfsStorageWritePrecondition {
                    predicate: None,
                    fingerprint: None,
                    secondary_fingerprint: None,
                    expected_file_id: Some(file_id.clone()),
                }),
            )
            .await
            .expect("identity-only write");
        let mismatch = storage
            .write(
                "guarded.txt",
                Bytes::from_static(b"wrong"),
                Some(VfsStorageWritePrecondition {
                    predicate: None,
                    fingerprint: None,
                    secondary_fingerprint: None,
                    expected_file_id: Some("unix:0:0".to_string()),
                }),
            )
            .await;
        assert!(matches!(mismatch, Err(VfsStorageError::Conflict(_))));
        assert_eq!(
            fs::read(dir.path().join("guarded.txt")).expect("preserved bytes"),
            b"identity-only",
        );

        let current_hash = hex_hash(b"identity-only");
        storage
            .write(
                "guarded.txt",
                Bytes::from_static(b"combined"),
                Some(VfsStorageWritePrecondition {
                    predicate: None,
                    fingerprint: Some(current_hash),
                    secondary_fingerprint: None,
                    expected_file_id: Some(file_id),
                }),
            )
            .await
            .expect("combined identity and content precondition");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn identity_precondition_rejects_replacement_during_staging() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("guarded");
        let replacement = dir.path().join("replacement");
        let displaced = dir.path().join("displaced");
        fs::write(&destination, b"old").expect("destination");
        fs::write(&replacement, b"replacement").expect("replacement");
        let original = fs::symlink_metadata(&destination).expect("metadata");
        let expected_file_id = local_file_id(&original).expect("stable identity");
        let swap_once = Arc::new(AtomicBool::new(true));
        let observed_swap = Arc::clone(&swap_once);
        let observed_destination = destination.clone();
        let observed_replacement = replacement.clone();
        let observed_displaced = displaced.clone();
        let storage = storage_with_durability_observer(dir.path(), move |event| {
            if matches!(event, DurabilitySyncEvent::File(path) if path != &observed_destination)
                && observed_swap.swap(false, AtomicBoolOrdering::SeqCst)
            {
                fs::rename(&observed_destination, &observed_displaced)
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                fs::rename(&observed_replacement, &observed_destination)
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
            }
            Ok(())
        });

        let result = storage
            .write(
                "guarded",
                Bytes::from_static(b"desired"),
                Some(VfsStorageWritePrecondition {
                    predicate: None,
                    fingerprint: Some(hex_hash(b"old")),
                    secondary_fingerprint: None,
                    expected_file_id: Some(expected_file_id),
                }),
            )
            .await;
        assert!(matches!(result, Err(VfsStorageError::Conflict(_))));
        assert_eq!(
            fs::read(&destination).expect("replacement bytes"),
            b"replacement"
        );
        assert_eq!(fs::read(&displaced).expect("original bytes"), b"old");
        assert!(
            fs::read_dir(dir.path()).expect("root").all(|entry| !entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")),
            "identity-race failure must clean staged files",
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn identity_mismatch_never_converts_to_exact_byte_replay() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("guarded");
        fs::write(&destination, b"desired").expect("destination");
        let storage = LocalVfsStorage::new(dir.path());

        let result = storage
            .write(
                "guarded",
                Bytes::from_static(b"desired"),
                Some(VfsStorageWritePrecondition {
                    predicate: None,
                    fingerprint: Some(hex_hash(b"old")),
                    secondary_fingerprint: None,
                    expected_file_id: Some("unix:0:0".to_string()),
                }),
            )
            .await;
        assert!(matches!(result, Err(VfsStorageError::Conflict(_))));
        assert_eq!(fs::read(destination).expect("bytes"), b"desired");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn identical_expect_absent_creators_yield_one_conflict() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let create = |storage: LocalVfsStorage, barrier: Arc<tokio::sync::Barrier>| async move {
            barrier.wait().await;
            storage
                .write_with_options(
                    "created",
                    Bytes::from_static(b"identical"),
                    Some(VfsStorageWritePrecondition {
                        predicate: None,
                        fingerprint: None,
                        secondary_fingerprint: None,
                        expected_file_id: None,
                    }),
                    Some(VfsStorageWriteOptions {
                        executable: false,
                        mode: Some(0o640),
                    }),
                )
                .await
        };
        let (left, right) = tokio::join!(
            create(storage.clone(), Arc::clone(&barrier)),
            create(storage.clone(), barrier),
        );
        let outcomes = [left, right];
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(result, Err(VfsStorageError::Conflict(_))))
                .count(),
            1,
        );
        assert_eq!(
            fs::read(dir.path().join("created")).expect("created bytes"),
            b"identical",
        );
        assert_eq!(path_mode(&dir.path().join("created")), 0o640);
    }

    #[tokio::test]
    async fn local_storage_streams_staged_files_with_hash_and_cas_guards() {
        let dir = tempfile::tempdir().expect("tempdir");
        let staging = tempfile::tempdir().expect("staging tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        let original = storage
            .write("large.bin", Bytes::from_static(b"original"), None)
            .await
            .expect("initial write");
        let source = staging.path().join("payload");
        let payload = vec![0x5a_u8; 3 * 1024 * 1024 + 17];
        fs::write(&source, &payload).expect("stage payload");
        let expected = hex_hash(&payload);

        let result = storage
            .write_from_local_file(
                "large.bin",
                &source,
                Some(&expected),
                Some(VfsStorageWritePrecondition {
                    predicate: None,
                    fingerprint: Some(original.content_hash),
                    secondary_fingerprint: None,
                    expected_file_id: None,
                }),
                Some(VfsStorageWriteOptions {
                    executable: false,
                    mode: None,
                }),
            )
            .await
            .expect("streamed install");
        assert_eq!(result.content_hash, expected);
        assert_eq!(
            fs::read(dir.path().join("large.bin")).expect("read"),
            payload
        );

        fs::write(&source, b"different").expect("replace staged payload");
        let error = storage
            .write_from_local_file(
                "large.bin",
                &source,
                Some(&expected),
                Some(VfsStorageWritePrecondition {
                    predicate: None,
                    fingerprint: Some(result.content_hash),
                    secondary_fingerprint: None,
                    expected_file_id: None,
                }),
                None,
            )
            .await
            .expect_err("hash mismatch");
        assert!(matches!(error, VfsStorageError::Conflict(_)));
        assert_eq!(
            fs::read(dir.path().join("large.bin")).expect("read after rejection"),
            payload
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_storage_allows_only_one_concurrent_same_fingerprint_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        let first = storage
            .write("guarded.txt", Bytes::from_static(b"first"), None)
            .await
            .expect("initial write");
        let precondition = VfsStorageWritePrecondition {
            predicate: None,
            fingerprint: Some(first.content_hash),
            secondary_fingerprint: None,
            expected_file_id: None,
        };
        let barrier = Arc::new(Barrier::new(2));
        let left_storage = storage.clone();
        let right_storage = storage.clone();
        let left_precondition = precondition.clone();
        let right_precondition = precondition;
        let left_barrier = barrier.clone();
        let right_barrier = barrier;

        let left = tokio::spawn(async move {
            left_barrier.wait();
            left_storage
                .write(
                    "guarded.txt",
                    Bytes::from_static(b"left"),
                    Some(left_precondition),
                )
                .await
        });
        let right = tokio::spawn(async move {
            right_barrier.wait();
            right_storage
                .write(
                    "guarded.txt",
                    Bytes::from_static(b"right"),
                    Some(right_precondition),
                )
                .await
        });

        let results = vec![
            left.await.expect("left task"),
            right.await.expect("right task"),
        ];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        assert!(results.iter().any(|result| {
            matches!(result, Err(VfsStorageError::Conflict(message)) if message.contains("guarded.txt"))
        }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_hard_links_share_identity_content_and_link_counts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write("objects/source", Bytes::from_static(b"one"), None)
            .await
            .expect("write source");

        let linked = storage
            .create_hard_link("objects/source", "aliases/destination")
            .await
            .expect("create hard link");
        assert_eq!(linked.source.file_id, linked.destination.file_id);
        assert_eq!(linked.source.link_count, 2);
        assert_eq!(linked.destination.link_count, 2);

        storage
            .write("aliases/destination", Bytes::from_static(b"mutated"), None)
            .await
            .expect("write through alias");
        assert_eq!(
            storage.read("objects/source").await.expect("read source"),
            Bytes::from_static(b"mutated")
        );
        let source = storage
            .stat("objects/source")
            .await
            .expect("stat source")
            .expect("source");
        let destination = storage
            .stat("aliases/destination")
            .await
            .expect("stat destination")
            .expect("destination");
        assert_eq!(source.file_id, destination.file_id);
        assert_eq!(source.link_count, 2);
        assert_eq!(source.content_hash, destination.content_hash);

        storage
            .delete_file_with_metadata("objects/source", None)
            .await
            .expect("unlink source");
        assert!(
            storage
                .stat("objects/source")
                .await
                .expect("stat")
                .is_none()
        );
        let remaining = storage
            .stat("aliases/destination")
            .await
            .expect("stat alias")
            .expect("remaining alias");
        assert_eq!(remaining.link_count, 1);
        assert_eq!(
            storage
                .find_hard_link_alias(
                    remaining.file_id.as_deref().expect("stable identity"),
                    "aliases/destination",
                )
                .await
                .expect("resolve aliases"),
            None
        );
    }

    #[tokio::test]
    async fn local_git_metadata_batch_does_not_rehash_or_scan_cache_per_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        let writes = (0..1_000)
            .map(|index| VfsStorageWrite {
                path: format!(".git/objects/ab/{index:04x}"),
                bytes: Bytes::from(format!("object-{index:04}\n")),
                token_count: None,
                precondition: None,
            })
            .collect::<Vec<_>>();

        let initial_started = std::time::Instant::now();
        storage
            .write_many_atomic(writes.clone())
            .await
            .expect("initial Git metadata batch");
        let initial_elapsed = initial_started.elapsed();
        let rewrite_started = std::time::Instant::now();
        storage
            .write_many_atomic(writes)
            .await
            .expect("Git metadata rewrite batch");
        let rewrite_elapsed = rewrite_started.elapsed();

        assert_eq!(
            storage.hash_read_count(),
            0,
            "trusted write hashes should prevent disk rehashes during an unchanged metadata rewrite",
        );
        assert!(
            storage.hash_cache.lock().unwrap().len() <= MAX_HASH_CACHE_ENTRIES,
            "the local hash cache must remain bounded",
        );
        eprintln!(
            "git metadata local benchmark: initial={initial_elapsed:?} rewrite={rewrite_elapsed:?}"
        );
    }

    fn git_perf_writes(count: usize, generation: usize) -> Vec<VfsStorageWrite> {
        let mutation_count = (count / 100).max(1);
        let object_count = count.saturating_sub(mutation_count.saturating_mul(2));
        let mut writes = (0..mutation_count)
            .map(|index| VfsStorageWrite {
                path: format!(".git/refs/heads/perf-{index:05}.lock"),
                bytes: Bytes::from(format!("ref-{generation}-{index:05}\n")),
                token_count: None,
                precondition: None,
            })
            .chain((0..mutation_count).map(|index| VfsStorageWrite {
                path: format!("src/generated/perf-{index:05}.ts"),
                bytes: Bytes::from(format!("export const value = {generation}_{index};\n")),
                token_count: None,
                precondition: None,
            }))
            .chain((0..object_count).map(|index| VfsStorageWrite {
                path: format!(".git/objects/{:02x}/{:038x}", index % 256, index),
                bytes: Bytes::from(format!("blob {generation} {index:08}\n")),
                token_count: None,
                precondition: None,
            }))
            .collect::<Vec<_>>();
        writes.sort_unstable_by(|left, right| left.path.cmp(&right.path));
        writes
    }

    fn estimated_hash_cache_bytes(storage: &LocalVfsStorage) -> usize {
        let cache = storage
            .hash_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let bucket_bytes = cache
            .capacity()
            .saturating_mul(std::mem::size_of::<(PathBuf, CachedFileHash)>());
        let allocation_bytes = cache
            .iter()
            .map(|(path, entry)| {
                path.as_os_str().len()
                    + entry.hash.capacity()
                    + entry.file_id.as_ref().map_or(0, String::capacity)
            })
            .sum::<usize>();
        bucket_bytes.saturating_add(allocation_bytes)
    }

    /// Explicit performance torture: it is intentionally ignored so ordinary
    /// unit-test latency does not inherit two 10k-file filesystem lifecycles.
    /// Run with:
    /// `cargo test --features gateway local_git_small_file_perf_1k_10k -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "explicit 1k/10k Git small-file performance suite"]
    async fn local_git_small_file_perf_1k_10k() {
        let mut scale_samples = Vec::new();
        for count in [1_000_usize, 10_000] {
            let dir = tempfile::tempdir().expect("tempdir");
            let first = LocalVfsStorage::new(dir.path());
            let second = LocalVfsStorage::new(dir.path());
            let initial = git_perf_writes(count, 0);
            let paths = initial
                .iter()
                .map(|write| write.path.clone())
                .collect::<Vec<_>>();
            let mutation_count = (count / 100).max(1);

            let create_started = std::time::Instant::now();
            first
                .write_many_atomic(initial)
                .await
                .expect("create Git-shaped file set");
            let create_elapsed = create_started.elapsed();

            let cold_status_started = std::time::Instant::now();
            let cold_status = first
                .metadata_many(&paths, VfsStorageMetadataFields::default())
                .await
                .expect("cold status-like bulk metadata");
            let cold_status_elapsed = cold_status_started.elapsed();
            assert_eq!(
                cold_status.iter().filter(|entry| entry.is_some()).count(),
                count
            );

            let warm_status_started = std::time::Instant::now();
            let warm_status = first
                .metadata_many(&paths, VfsStorageMetadataFields::default())
                .await
                .expect("warm status-like bulk metadata");
            let warm_status_elapsed = warm_status_started.elapsed();
            assert_eq!(
                warm_status.iter().filter(|entry| entry.is_some()).count(),
                count
            );

            let targeted = git_perf_writes(count, 1)
                .into_iter()
                .filter(|write| {
                    write.path.starts_with(".git/refs/") || write.path.starts_with("src/generated/")
                })
                .collect::<Vec<_>>();
            assert_eq!(targeted.len(), mutation_count * 2);
            let rewrite_started = std::time::Instant::now();
            second
                .write_many_atomic(targeted)
                .await
                .expect("targeted ref/worktree rewrite");
            let rewrite_elapsed = rewrite_started.elapsed();

            let mutations = (0..mutation_count)
                .map(|index| VfsStorageNamespaceMutation::Rename {
                    from: format!(".git/refs/heads/perf-{index:05}.lock"),
                    to: format!(".git/refs/heads/perf-{index:05}"),
                })
                .chain(
                    (0..mutation_count).map(|index| VfsStorageNamespaceMutation::DeleteFile {
                        path: format!("src/generated/perf-{index:05}.ts"),
                        precondition: None,
                    }),
                )
                .collect::<Vec<_>>();
            let namespace_started = std::time::Instant::now();
            second
                .apply_namespace_batch(mutations)
                .await
                .expect("Git ref promotion and cleanup batch");
            let namespace_elapsed = namespace_started.elapsed();

            let survivor = format!(".git/objects/{:02x}/{:038x}", 257 % 256, 257);
            let _ = first
                .read(&survivor)
                .await
                .expect("prime first-client read");
            let replacement = Bytes::from_static(b"cross-client replacement with distinct size\n");
            let replacement_hash = hex_hash(&replacement);
            second
                .write(&survivor, replacement.clone(), None)
                .await
                .expect("second-client replacement");
            assert_eq!(
                first.read(&survivor).await.expect("first-client refresh"),
                replacement,
                "independent local clients must observe replacement bytes",
            );
            assert_eq!(
                first
                    .stat(&survivor)
                    .await
                    .expect("first-client stat")
                    .and_then(|metadata| metadata.content_hash)
                    .as_deref(),
                Some(replacement_hash.as_str()),
                "independent local clients must invalidate stale content hashes",
            );

            let hash_reads = first.hash_read_count() + second.hash_read_count();
            assert!(
                hash_reads <= count.saturating_mul(3),
                "hash reads must remain linear: count={count}, reads={hash_reads}",
            );
            for (name, storage) in [("first", &first), ("second", &second)] {
                assert!(
                    storage.hash_cache.lock().unwrap().len() <= MAX_HASH_CACHE_ENTRIES,
                    "{name} local hash cache exceeded its ceiling",
                );
                assert_eq!(
                    storage
                        .path_locks
                        .inner
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .len(),
                    0,
                    "{name} path locks leaked after the workload",
                );
            }

            let total = create_elapsed
                + cold_status_elapsed
                + warm_status_elapsed
                + rewrite_elapsed
                + namespace_elapsed;
            let cache_entries = first.hash_cache.lock().unwrap().len();
            let cache_estimated_bytes = estimated_hash_cache_bytes(&first);
            let projected_ceiling_bytes = if cache_entries == 0 {
                0
            } else {
                cache_estimated_bytes
                    .saturating_mul(MAX_HASH_CACHE_ENTRIES)
                    .div_ceil(cache_entries)
            };
            scale_samples.push((count, total));
            eprintln!(
                "git-small-file-perf backend=local files={count} create={create_elapsed:?} \
                 status_cold={cold_status_elapsed:?} status_warm={warm_status_elapsed:?} \
                 rewrite_2pct={rewrite_elapsed:?} namespace_2pct={namespace_elapsed:?} \
                 total={total:?} hash_reads={hash_reads} cache_entries={cache_entries} \
                 cache_estimated_bytes={cache_estimated_bytes} \
                 projected_ceiling_bytes={projected_ceiling_bytes}",
            );
        }
        let one_k = scale_samples[0].1.as_secs_f64();
        let ten_k = scale_samples[1].1.as_secs_f64();
        assert!(
            ten_k <= one_k * 35.0,
            "10x local workload regressed toward quadratic scaling: 1k={one_k:.3}s 10k={ten_k:.3}s",
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_independent_clients_observe_linked_inode_mutations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = LocalVfsStorage::new(dir.path());
        let second = LocalVfsStorage::new(dir.path());
        first
            .write("source", Bytes::from_static(b"one"), None)
            .await
            .expect("initial write");
        let linked = second
            .create_hard_link("source", "alias")
            .await
            .expect("second client hard link");
        let file_id = linked.source.file_id.expect("stable identity");

        second
            .write("alias", Bytes::from_static(b"two"), None)
            .await
            .expect("second client alias write");
        assert_eq!(first.read("source").await.unwrap().as_ref(), b"two");
        assert_eq!(first.read("alias").await.unwrap().as_ref(), b"two");
        let source = first.stat("source").await.unwrap().unwrap();
        let alias = first.stat("alias").await.unwrap().unwrap();
        assert_eq!(source.file_id.as_deref(), Some(file_id.as_str()));
        assert_eq!(alias.file_id.as_deref(), Some(file_id.as_str()));
        assert_eq!(source.link_count, 2);
        assert_eq!(source.content_hash, alias.content_hash);

        first
            .rename_with_metadata("alias", "renamed")
            .await
            .expect("first client rename");
        assert!(second.stat("alias").await.unwrap().is_none());
        let renamed = second.stat("renamed").await.unwrap().unwrap();
        assert_eq!(renamed.file_id.as_deref(), Some(file_id.as_str()));
        assert_eq!(renamed.link_count, 2);
        assert_eq!(second.read("source").await.unwrap().as_ref(), b"two");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_alias_writes_share_one_precondition_domain() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        let initial = storage
            .write("source", Bytes::from_static(b"base"), None)
            .await
            .expect("initial");
        storage
            .create_hard_link("source", "alias")
            .await
            .expect("link");
        let precondition = VfsStorageWritePrecondition {
            predicate: None,
            fingerprint: Some(initial.content_hash),
            secondary_fingerprint: None,
            expected_file_id: None,
        };
        let left = {
            let storage = storage.clone();
            let precondition = precondition.clone();
            tokio::spawn(async move {
                storage
                    .write("source", Bytes::from_static(b"left"), Some(precondition))
                    .await
            })
        };
        let right = {
            let storage = storage.clone();
            tokio::spawn(async move {
                storage
                    .write("alias", Bytes::from_static(b"right"), Some(precondition))
                    .await
            })
        };
        let results = [left.await.expect("left"), right.await.expect("right")];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        assert_eq!(
            storage.read("source").await.expect("source"),
            storage.read("alias").await.expect("alias")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_rename_replaces_destination_and_preserves_source_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        storage
            .write("source", Bytes::from_static(b"source body"), None)
            .await
            .expect("source");
        storage
            .write("destination", Bytes::from_static(b"old body"), None)
            .await
            .expect("destination");
        let source_id = storage.stat("source").await.unwrap().unwrap().file_id;

        storage
            .rename_with_metadata("source", "destination")
            .await
            .expect("replace rename");
        assert!(storage.stat("source").await.unwrap().is_none());
        let destination = storage.stat("destination").await.unwrap().unwrap();
        assert_eq!(destination.file_id, source_id);
        assert_eq!(
            storage.read("destination").await.unwrap(),
            Bytes::from_static(b"source body")
        );
    }

    #[tokio::test]
    async fn local_storage_rejects_parent_escape() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = LocalVfsStorage::new(dir.path());
        let err = storage
            .read("../outside.txt")
            .await
            .expect_err("escape rejected");
        assert!(matches!(err, VfsStorageError::BadRequest(_)));
    }
}
