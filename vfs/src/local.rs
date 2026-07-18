// @dive-file: Local filesystem implementation of the optimized VFS storage trait.
// @dive-rel: Provides the direct/dev backend for chevalier-vfs without product policy or VM concerns.
// @dive-rel: Mirrors the old local nymfs adapter semantics while exposing batch-oriented calls.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
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
    OptimizedVfsStorage, VfsStorageDeleteResult, VfsStorageDirListFilter, VfsStorageDirListOrder,
    VfsStorageEntryKind, VfsStorageError, VfsStorageMetadata, VfsStorageMetadataFields,
    VfsStorageNamespaceMutation, VfsStorageObjectState, VfsStoragePrefetchOptions,
    VfsStoragePrefetchResult, VfsStorageReadIfChanged, VfsStorageReadIfChangedResult,
    VfsStorageReadRange, VfsStorageRenameResult, VfsStorageResult, VfsStorageSubtreeOptions,
    VfsStorageWrite, VfsStorageWriteOptions, VfsStorageWritePrecondition, VfsStorageWriteResult,
    pack::{SlotCompression, hex_hash},
};

#[derive(Clone, Debug)]
pub struct LocalVfsStorage {
    root: PathBuf,
    hash_cache: Arc<Mutex<HashMap<PathBuf, CachedFileHash>>>,
    path_locks: PathLockTable,
    #[cfg(test)]
    hash_read_count: Arc<AtomicUsize>,
}

struct SymlinkTargetInfo {
    target_text: String,
}

#[derive(Clone, Debug)]
struct CachedFileHash {
    size_bytes: u64,
    mtime_ns: i128,
    change_ns: i128,
    cached_at: SystemTime,
    hash: String,
    trusted_write: bool,
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
                modes.insert(current, PathLockMode::Write);
                continue;
            }
            for (index, component) in components.iter().enumerate() {
                if !current.is_empty() {
                    current.push('/');
                }
                current.push_str(component);
                let mode = if index + 1 == components.len() {
                    PathLockMode::Write
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
            path_locks: PathLockTable::default(),
            #[cfg(test)]
            hash_read_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
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
            link_target,
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
            fingerprint: Some(fingerprint),
            secondary_fingerprint: None,
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
        let expected = precondition.fingerprint.as_deref().unwrap_or("absent");
        let actual = self
            .write_precondition(path)?
            .fingerprint
            .unwrap_or_else(|| "absent".to_string());
        if actual == expected {
            Ok(())
        } else {
            Err(VfsStorageError::Conflict(format!(
                "local vfs write precondition failed for {path}"
            )))
        }
    }

    async fn lock_write_paths(&self, paths: impl IntoIterator<Item = String>) -> PathLocks {
        self.path_locks.lock(paths).await
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
                size_bytes: metadata.len(),
                mtime_ns: metadata_mtime_ns(metadata),
                change_ns: metadata_change_ns(metadata),
                cached_at: SystemTime::now(),
                hash,
                trusted_write,
            },
        );
    }

    fn invalidate_hash(&self, path: &Path) {
        self.hash_cache
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .remove(path);
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
        self.run_blocking(move |storage| storage.metadata_for_path(&path))
            .await
    }

    async fn stat_with_metadata_fields(
        &self,
        path: &str,
        fields: VfsStorageMetadataFields,
    ) -> VfsStorageResult<Option<VfsStorageMetadata>> {
        let path = path.to_string();
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
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            read_file(&abs_path).map(Bytes::from)
        })
        .await
    }

    async fn read_range(&self, path: &str, range: VfsStorageReadRange) -> VfsStorageResult<Bytes> {
        let path = path.to_string();
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
            storage.assert_precondition(&write.path, write.precondition.as_ref())?;
            let result = install_writes_with_options(&storage, vec![(write, options)])?;
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
            storage.assert_precondition(&path, precondition.as_ref())?;

            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            let previous_hash = storage
                .metadata_for_abs(&abs_path)?
                .and_then(|metadata| metadata.content_hash);
            let previous_mode = existing_regular_file_mode(&abs_path)?;
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

            let install = (|| -> VfsStorageResult<String> {
                let mut source = open_regular_file(&source_path)?;
                let mut staged = fs::File::create(&tmp_path)
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
                staged
                    .sync_all()
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                let content_hash = format!("{:x}", hasher.finalize());
                if expected_content_hash
                    .as_deref()
                    .is_some_and(|expected| expected != content_hash)
                {
                    return Err(VfsStorageError::Conflict(format!(
                        "staged VFS upload hash mismatch for {path}"
                    )));
                }
                fs::rename(&tmp_path, &abs_path)
                    .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                let executable = options.as_ref().is_some_and(|value| value.executable);
                apply_executable_option(&abs_path, executable, previous_mode)?;
                Ok(content_hash)
            })();

            let content_hash = match install {
                Ok(content_hash) => content_hash,
                Err(error) => {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(error);
                }
            };
            let metadata = fs::symlink_metadata(&abs_path)
                .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
            storage.remember_written_hash(&abs_path, &metadata, content_hash.clone());
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
            for write in &writes {
                storage.assert_precondition(&write.path, write.precondition.as_ref())?;
            }
            install_writes(&storage, writes)
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
            for write in &writes {
                storage.assert_precondition(&write.path, write.precondition.as_ref())?;
            }
            let mut changed = Vec::new();
            let mut unchanged = Vec::new();
            for write in writes {
                let previous_hash = storage
                    .metadata_for_path(&write.path)?
                    .and_then(|metadata| metadata.content_hash);
                let next_hash = hex_hash(&write.bytes);
                if previous_hash.as_deref() == Some(next_hash.as_str()) {
                    unchanged.push(VfsStorageWriteResult {
                        path: write.path,
                        content_hash: next_hash,
                        previous_hash,
                        changed: false,
                    });
                } else {
                    changed.push(write);
                }
            }
            let mut out = install_writes(&storage, changed)?;
            out.extend(unchanged);
            out.sort_by(|a, b| a.path.cmp(&b.path));
            Ok(out)
        })
        .await
    }

    async fn mkdir(&self, path: &str) -> VfsStorageResult<()> {
        let path = path.to_string();
        let _locks = self.lock_write_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            fs::create_dir_all(abs_path).map_err(|err| VfsStorageError::Internal(err.to_string()))
        })
        .await
    }

    async fn create_symlink(&self, path: &str, target: &str) -> VfsStorageResult<()> {
        let path = path.to_string();
        let target = target.to_string();
        let _locks = self.lock_write_paths([path.clone()]).await;
        self.run_blocking(move |storage| create_symlink_impl(&storage, &path, &target))
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
            storage.assert_precondition(&path, precondition.as_ref())?;
            let previous = storage.metadata_for_path(&path)?;
            if matches!(
                previous.as_ref().map(|metadata| metadata.kind),
                Some(VfsStorageEntryKind::Directory)
            ) {
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs path {path} is not a file"
                )));
            }
            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            match fs::remove_file(&abs_path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(VfsStorageError::Internal(err.to_string())),
            }
            storage.invalidate_hash(&abs_path);
            Ok(VfsStorageDeleteResult { previous })
        })
        .await
    }

    async fn rmdir(&self, path: &str) -> VfsStorageResult<()> {
        let path = path.to_string();
        let _locks = self.lock_write_paths([path.clone()]).await;
        self.run_blocking(move |storage| {
            let Some(metadata) = storage.metadata_for_path(&path)? else {
                return Ok(());
            };
            if metadata.kind != VfsStorageEntryKind::Directory {
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs path {path} is not a directory"
                )));
            }
            let abs_path = storage.abs_path(&path)?;
            storage.assert_no_symlink_ancestor(&abs_path)?;
            match fs::remove_dir(abs_path) {
                Ok(()) => Ok(()),
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
            let previous = storage.metadata_for_path(&from)?;
            let Some(_) = previous else {
                return Err(VfsStorageError::NotFound(from));
            };
            let from_abs = storage.abs_path(&from)?;
            storage.assert_no_symlink_ancestor(&from_abs)?;
            let to_abs = storage.abs_path(&to)?;
            storage.assert_no_symlink_ancestor(&to_abs)?;
            if let Some(parent) = to_abs.parent() {
                fs::create_dir_all(parent)
                    .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
            }
            fs::rename(&from_abs, &to_abs)
                .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
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
                    storage.assert_precondition(path.as_str(), precondition.as_ref())?;
                    if matches!(
                        storage
                            .metadata_for_path(path.as_str())?
                            .as_ref()
                            .map(|metadata| metadata.kind),
                        Some(VfsStorageEntryKind::Directory)
                    ) {
                        return Err(VfsStorageError::BadRequest(format!(
                            "vfs path {path} is not a file"
                        )));
                    }
                }
            }

            for mutation in mutations {
                match mutation {
                    VfsStorageNamespaceMutation::CreateDirectory { path } => {
                        let abs_path = storage.abs_path(path.as_str())?;
                        storage.assert_no_symlink_ancestor(&abs_path)?;
                        fs::create_dir_all(abs_path)
                            .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
                    }
                    VfsStorageNamespaceMutation::CreateSymlink { path, target } => {
                        match storage.metadata_for_path(path.as_str())? {
                            Some(metadata)
                                if metadata.kind == VfsStorageEntryKind::Symlink
                                    && metadata.link_target.as_deref() == Some(target.as_str()) => {}
                            Some(_) => {
                                return Err(VfsStorageError::Conflict(format!(
                                    "vfs path {path} already exists"
                                )));
                            }
                            None => {
                                create_symlink_impl(&storage, path.as_str(), target.as_str())?
                            }
                        }
                    }
                    VfsStorageNamespaceMutation::DeleteFile {
                        path,
                        precondition: _,
                    } => {
                        let abs_path = storage.abs_path(path.as_str())?;
                        storage.assert_no_symlink_ancestor(&abs_path)?;
                        match fs::remove_file(&abs_path) {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => {
                                return Err(VfsStorageError::Internal(error.to_string()));
                            }
                        }
                        storage.invalidate_hash(&abs_path);
                    }
                    VfsStorageNamespaceMutation::RemoveDirectory { path } => {
                        let abs_path = storage.abs_path(path.as_str())?;
                        storage.assert_no_symlink_ancestor(&abs_path)?;
                        match fs::remove_dir(&abs_path) {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                                return Err(VfsStorageError::Conflict(format!(
                                    "vfs directory {path} is not empty"
                                )));
                            }
                            Err(error) => {
                                return Err(VfsStorageError::Internal(error.to_string()));
                            }
                        }
                    }
                    VfsStorageNamespaceMutation::Rename { from, to } => {
                        let from_abs = storage.abs_path(from.as_str())?;
                        let to_abs = storage.abs_path(to.as_str())?;
                        storage.assert_no_symlink_ancestor(&from_abs)?;
                        storage.assert_no_symlink_ancestor(&to_abs)?;
                        // Batch callers validate the source before journaling. A replay may
                        // observe neither path when a later mutation in the same completed
                        // batch already removed the destination, so a missing source is a
                        // successful no-op here.
                        if !from_abs.exists() {
                            continue;
                        }
                        if to_abs.exists()
                            && paths_have_equivalent_contents(&from_abs, &to_abs)?
                        {
                            sync_path_permissions(&from_abs, &to_abs)?;
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
                            if error.kind() == std::io::ErrorKind::DirectoryNotEmpty {
                                VfsStorageError::Conflict(format!(
                                    "cannot replay rename {from} -> {to}: destination differs and is not empty"
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
            Ok(())
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
        let executable = options.as_ref().is_some_and(|options| options.executable);
        let previous_mode = if executable {
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
        staged.push((
            write.path,
            abs_path,
            tmp_path,
            content_hash,
            previous_hash,
            executable,
            previous_mode,
        ));
    }

    let mut results = Vec::with_capacity(staged.len());
    for (path, abs_path, tmp_path, content_hash, previous_hash, executable, previous_mode) in staged
    {
        fs::rename(&tmp_path, &abs_path)
            .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
        apply_executable_option(&abs_path, executable, previous_mode)?;
        let metadata = fs::symlink_metadata(&abs_path)
            .map_err(|err| VfsStorageError::Internal(err.to_string()))?;
        storage.remember_written_hash(&abs_path, &metadata, content_hash.clone());
        results.push(VfsStorageWriteResult {
            path,
            content_hash,
            previous_hash,
            changed: true,
        });
    }
    Ok(results)
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

fn sync_path_permissions(source: &Path, destination: &Path) -> VfsStorageResult<()> {
    let source_metadata = fs::symlink_metadata(source)
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if source_metadata.file_type().is_symlink() {
        return Ok(());
    }
    fs::set_permissions(destination, source_metadata.permissions())
        .map_err(|error| VfsStorageError::Internal(error.to_string()))?;
    if !source_metadata.is_dir() {
        return Ok(());
    }
    for entry in
        fs::read_dir(source).map_err(|error| VfsStorageError::Internal(error.to_string()))?
    {
        let entry = entry.map_err(|error| VfsStorageError::Internal(error.to_string()))?;
        sync_path_permissions(&entry.path(), &destination.join(entry.file_name()))?;
    }
    Ok(())
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
fn executable_from_metadata(metadata: &fs::Metadata, kind: VfsStorageEntryKind) -> bool {
    kind == VfsStorageEntryKind::File && metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn executable_from_metadata(_metadata: &fs::Metadata, _kind: VfsStorageEntryKind) -> bool {
    false
}

#[cfg(unix)]
fn existing_regular_file_mode(path: &Path) -> VfsStorageResult<Option<u32>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata.permissions().mode() & 0o777)),
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
fn apply_executable_option(
    path: &Path,
    executable: bool,
    previous_mode: Option<u32>,
) -> VfsStorageResult<()> {
    if !executable {
        return Ok(());
    }
    let target_mode = previous_mode.map(|mode| mode | 0o111).unwrap_or(0o755);
    let metadata =
        fs::symlink_metadata(path).map_err(|err| VfsStorageError::Internal(err.to_string()))?;
    let mut permissions = metadata.permissions();
    permissions.set_mode(target_mode);
    fs::set_permissions(path, permissions).map_err(|err| VfsStorageError::Internal(err.to_string()))
}

#[cfg(not(unix))]
fn apply_executable_option(
    _path: &Path,
    _executable: bool,
    _previous_mode: Option<u32>,
) -> VfsStorageResult<()> {
    Ok(())
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
    use std::sync::{Arc, Barrier};

    fn set_old_mtime(path: &Path) {
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .expect("open for mtime");
        file.set_modified(SystemTime::now() - Duration::from_secs(5))
            .expect("set old mtime");
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
                        fingerprint: Some(hash_a.clone()),
                        secondary_fingerprint: None,
                    }),
                },
                VfsStorageNamespaceMutation::DeleteFile {
                    path: "b.txt".to_string(),
                    precondition: Some(VfsStorageWritePrecondition {
                        fingerprint: Some("stale".to_string()),
                        secondary_fingerprint: None,
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
                        fingerprint: Some(hash_a),
                        secondary_fingerprint: None,
                    }),
                },
                VfsStorageNamespaceMutation::DeleteFile {
                    path: "b.txt".to_string(),
                    precondition: Some(VfsStorageWritePrecondition {
                        fingerprint: Some(hex_hash(b"b")),
                        secondary_fingerprint: None,
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
                    fingerprint: Some("symlink:stale".to_string()),
                    secondary_fingerprint: None,
                }),
            )
            .await;
        assert!(matches!(stale, Err(VfsStorageError::Conflict(_))));
        assert!(dir.path().join("link.txt").exists());

        storage
            .delete_file_with_metadata(
                "link.txt",
                Some(VfsStorageWritePrecondition {
                    fingerprint: Some(expected),
                    secondary_fingerprint: None,
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
                Some(VfsStorageWriteOptions { executable: true }),
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
                Some(VfsStorageWriteOptions { executable: true }),
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
            fingerprint: Some(first.content_hash),
            secondary_fingerprint: None,
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
                    fingerprint: Some(original.content_hash),
                    secondary_fingerprint: None,
                }),
                Some(VfsStorageWriteOptions { executable: false }),
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
                    fingerprint: Some(result.content_hash),
                    secondary_fingerprint: None,
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
            fingerprint: Some(first.content_hash),
            secondary_fingerprint: None,
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
