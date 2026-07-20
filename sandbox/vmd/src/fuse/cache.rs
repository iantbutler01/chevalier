use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use chevalier_sandbox::vfs::{VfsDirEntry as RemoteDirEntry, VfsMetadata as RemoteMetadata};

const FILE_TTL: Duration = Duration::from_secs(60);
const DIR_TTL: Duration = Duration::from_secs(5);
const ATTR_TTL: Duration = Duration::from_secs(1);
const MAX_FILE_BYTES: usize = 10 * 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 256 * 1024 * 1024;
const MAX_DIRS: usize = 4_096;
const MAX_DIR_ENTRIES: usize = 10_000;
const MAX_ATTRS: usize = 131_072;

#[derive(Clone)]
struct CachedFile {
    bytes: Vec<u8>,
    metadata: Option<RemoteMetadata>,
    expires_at: Instant,
    last_access: Instant,
}

#[derive(Clone)]
struct CachedDir {
    entries: Vec<RemoteDirEntry>,
    expires_at: Instant,
    last_access: Instant,
}

#[derive(Clone)]
struct CachedMetadata {
    metadata: RemoteMetadata,
    expires_at: Instant,
    last_access: Instant,
}

#[derive(Default)]
struct CacheState {
    file_bytes: usize,
    files: HashMap<String, CachedFile>,
    dirs: HashMap<String, CachedDir>,
    attrs: HashMap<String, CachedMetadata>,
}

#[derive(Default)]
pub struct RemoteFuseCache {
    inner: Mutex<CacheState>,
}

impl RemoteFuseCache {
    pub fn get_file_matching(&self, path: &str, metadata: &RemoteMetadata) -> Option<Vec<u8>> {
        let mut inner = self.lock_inner();
        let entry = inner.files.get_mut(path)?;
        if entry.expires_at <= Instant::now()
            || !cached_metadata_matches(entry.metadata.as_ref(), metadata)
        {
            let removed = inner.files.remove(path)?;
            inner.file_bytes = inner.file_bytes.saturating_sub(removed.bytes.len());
            return None;
        }
        entry.last_access = Instant::now();
        Some(entry.bytes.clone())
    }

    pub fn put_file(&self, path: &str, bytes: Vec<u8>, metadata: Option<RemoteMetadata>) {
        if bytes.len() > MAX_FILE_BYTES {
            return;
        }
        let mut inner = self.lock_inner();
        if let Some(previous) = inner.files.remove(path) {
            inner.file_bytes = inner.file_bytes.saturating_sub(previous.bytes.len());
        }
        inner.file_bytes += bytes.len();
        inner.files.insert(
            path.to_string(),
            CachedFile {
                bytes,
                metadata,
                expires_at: Instant::now() + FILE_TTL,
                last_access: Instant::now(),
            },
        );
        if let Some(metadata) = inner
            .files
            .get(path)
            .and_then(|entry| entry.metadata.clone())
        {
            put_metadata_locked(&mut inner, path, metadata);
        }
        if inner.file_bytes > MAX_TOTAL_BYTES {
            prune_files_locked(&mut inner, MAX_TOTAL_BYTES.saturating_mul(3) / 4);
        }
    }

    pub fn get_dir(&self, path: &str) -> Option<Vec<RemoteDirEntry>> {
        let mut inner = self.lock_inner();
        let entry = inner.dirs.get_mut(path)?;
        if entry.expires_at <= Instant::now() {
            inner.dirs.remove(path);
            return None;
        }
        entry.last_access = Instant::now();
        Some(entry.entries.clone())
    }

    pub fn put_dir(&self, path: &str, entries: Vec<RemoteDirEntry>) {
        if entries.len() > MAX_DIR_ENTRIES {
            return;
        }
        let mut inner = self.lock_inner();
        let now = Instant::now();
        inner.dirs.insert(
            path.to_string(),
            CachedDir {
                entries,
                expires_at: now + DIR_TTL,
                last_access: now,
            },
        );
        let cached_entries = inner
            .dirs
            .get(path)
            .map(|entry| entry.entries.clone())
            .unwrap_or_default();
        for entry in cached_entries {
            put_metadata_locked(
                &mut inner,
                child_path(path, &entry.name).as_str(),
                metadata_from_dir_entry(entry),
            );
        }
        if inner.dirs.len() > MAX_DIRS {
            prune_dirs_locked(&mut inner, now, MAX_DIRS.saturating_mul(3) / 4);
        }
    }

    pub fn get_metadata(&self, path: &str) -> Option<RemoteMetadata> {
        let mut inner = self.lock_inner();
        let entry = inner.attrs.get_mut(path)?;
        if entry.expires_at <= Instant::now() {
            inner.attrs.remove(path);
            return None;
        }
        entry.last_access = Instant::now();
        Some(entry.metadata.clone())
    }

    pub fn put_metadata(&self, path: &str, metadata: RemoteMetadata) {
        let mut inner = self.lock_inner();
        put_metadata_locked(&mut inner, path, metadata);
    }

    pub fn invalidate(&self, path: &str) {
        let mut inner = self.lock_inner();
        if let Some(previous) = inner.files.remove(path) {
            inner.file_bytes = inner.file_bytes.saturating_sub(previous.bytes.len());
        }
        inner.attrs.remove(path);
        inner.dirs.remove(path);
        if let Some(parent) = parent_path(path) {
            inner.dirs.remove(parent.as_str());
        }
    }

    fn lock_inner(&self) -> MutexGuard<'_, CacheState> {
        self.inner.lock().unwrap_or_else(|err| err.into_inner())
    }
}

fn put_metadata_locked(inner: &mut CacheState, path: &str, metadata: RemoteMetadata) {
    let now = Instant::now();
    inner.attrs.insert(
        path.to_string(),
        CachedMetadata {
            metadata,
            expires_at: now + ATTR_TTL,
            last_access: now,
        },
    );
    if inner.attrs.len() > MAX_ATTRS {
        prune_attrs_locked(inner, now, MAX_ATTRS.saturating_mul(3) / 4);
    }
}

fn cached_metadata_matches(cached: Option<&RemoteMetadata>, current: &RemoteMetadata) -> bool {
    let Some(cached) = cached else {
        return false;
    };
    match (
        cached.content_hash.as_deref(),
        current.content_hash.as_deref(),
    ) {
        (Some(cached_hash), Some(current_hash)) => cached_hash == current_hash,
        (None, None) => {
            cached.kind == current.kind
                && cached.size_bytes == current.size_bytes
                && cached.link_target == current.link_target
                && cached.executable == current.executable
        }
        _ => false,
    }
}

fn prune_files_locked(inner: &mut CacheState, target_bytes: usize) {
    let mut oldest = inner
        .files
        .iter()
        .map(|(path, value)| (path.clone(), value.last_access))
        .collect::<Vec<_>>();
    oldest.sort_unstable_by_key(|(_, last_access)| *last_access);
    for (path, _) in oldest {
        if inner.file_bytes <= target_bytes {
            break;
        }
        if let Some(removed) = inner.files.remove(path.as_str()) {
            inner.file_bytes = inner.file_bytes.saturating_sub(removed.bytes.len());
        }
    }
}

fn prune_dirs_locked(inner: &mut CacheState, now: Instant, target_entries: usize) {
    inner.dirs.retain(|_, value| value.expires_at > now);
    if inner.dirs.len() <= target_entries {
        return;
    }
    let mut oldest = inner
        .dirs
        .iter()
        .map(|(path, value)| (path.clone(), value.last_access))
        .collect::<Vec<_>>();
    oldest.sort_unstable_by_key(|(_, last_access)| *last_access);
    let remove_count = inner.dirs.len().saturating_sub(target_entries);
    for (path, _) in oldest.into_iter().take(remove_count) {
        inner.dirs.remove(path.as_str());
    }
}

fn prune_attrs_locked(inner: &mut CacheState, now: Instant, target_entries: usize) {
    inner.attrs.retain(|_, value| value.expires_at > now);
    if inner.attrs.len() <= target_entries {
        return;
    }
    let mut oldest = inner
        .attrs
        .iter()
        .map(|(path, value)| (path.clone(), value.last_access))
        .collect::<Vec<_>>();
    oldest.sort_unstable_by_key(|(_, last_access)| *last_access);
    let remove_count = inner.attrs.len().saturating_sub(target_entries);
    for (path, _) in oldest.into_iter().take(remove_count) {
        inner.attrs.remove(path.as_str());
    }
}

fn metadata_from_dir_entry(entry: RemoteDirEntry) -> RemoteMetadata {
    RemoteMetadata {
        kind: entry.kind,
        size_bytes: entry.size_bytes,
        file_id: entry.file_id,
        link_count: entry.link_count,
        link_target: entry.link_target,
        content_hash: entry.content_hash,
        executable: entry.executable,
        updated_at: entry.updated_at,
    }
}

fn child_path(parent: &str, name: &str) -> String {
    let trimmed = parent.trim_matches('/');
    if trimmed.is_empty() {
        name.to_string()
    } else {
        format!("{trimmed}/{}", name.trim_matches('/'))
    }
}

fn parent_path(path: &str) -> Option<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    trimmed
        .rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .or(Some(String::new()))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        CacheState, CachedMetadata, MAX_DIR_ENTRIES, MAX_DIRS, RemoteFuseCache, prune_attrs_locked,
    };
    use chevalier_sandbox::vfs::{VfsDirEntry as RemoteDirEntry, VfsMetadata as RemoteMetadata};

    fn entry(name: &str) -> RemoteDirEntry {
        RemoteDirEntry {
            name: name.to_string(),
            kind: "file".to_string(),
            size_bytes: 0,
            file_id: None,
            link_count: 1,
            link_target: None,
            content_hash: None,
            executable: false,
            updated_at: None,
        }
    }

    fn symlink_entry(name: &str, target: &str) -> RemoteDirEntry {
        RemoteDirEntry {
            name: name.to_string(),
            kind: "symlink".to_string(),
            size_bytes: target.len() as u64,
            file_id: None,
            link_count: 1,
            link_target: Some(target.to_string()),
            content_hash: None,
            executable: false,
            updated_at: None,
        }
    }

    fn metadata(content_hash: &str, size_bytes: u64) -> RemoteMetadata {
        RemoteMetadata {
            kind: "file".to_string(),
            size_bytes,
            file_id: None,
            link_count: 1,
            link_target: None,
            content_hash: Some(content_hash.to_string()),
            executable: false,
            updated_at: None,
        }
    }

    #[test]
    fn directory_cache_evicts_by_capacity() {
        let cache = RemoteFuseCache::default();
        for index in 0..=MAX_DIRS {
            cache.put_dir(&format!("dir-{index}"), vec![entry("file")]);
        }
        assert!(cache.get_dir("dir-0").is_none());
        assert!(cache.get_dir(&format!("dir-{MAX_DIRS}")).is_some());
    }

    #[test]
    fn directory_cache_skips_oversized_directories() {
        let cache = RemoteFuseCache::default();
        let entries = (0..=MAX_DIR_ENTRIES)
            .map(|index| entry(&format!("file-{index}")))
            .collect();
        cache.put_dir("huge", entries);
        assert!(cache.get_dir("huge").is_none());
    }

    #[test]
    fn directory_cache_populates_child_metadata_for_readlink() {
        let cache = RemoteFuseCache::default();
        cache.put_dir("bin", vec![symlink_entry("tool", "../real-tool")]);

        let metadata = cache.get_metadata("bin/tool").expect("cached metadata");
        assert_eq!(metadata.kind, "symlink");
        assert_eq!(metadata.link_target.as_deref(), Some("../real-tool"));
    }

    #[test]
    fn file_cache_requires_matching_authoritative_metadata() {
        let cache = RemoteFuseCache::default();
        cache.put_file(
            "Cargo.toml",
            b"complete".to_vec(),
            Some(metadata("complete-hash", 8)),
        );

        assert_eq!(
            cache.get_file_matching("Cargo.toml", &metadata("complete-hash", 8)),
            Some(b"complete".to_vec())
        );
        assert!(
            cache
                .get_file_matching("Cargo.toml", &metadata("truncated-hash", 1))
                .is_none()
        );
        assert!(
            cache
                .get_file_matching("Cargo.toml", &metadata("complete-hash", 8))
                .is_none()
        );
    }

    #[test]
    fn metadata_cache_prunes_expired_and_old_entries_in_one_batch() {
        let now = Instant::now();
        let mut inner = CacheState::default();
        for index in 0..4 {
            inner.attrs.insert(
                format!("entry-{index}"),
                CachedMetadata {
                    metadata: super::metadata_from_dir_entry(entry("file")),
                    expires_at: if index == 0 {
                        now - Duration::from_secs(1)
                    } else {
                        now + Duration::from_secs(1)
                    },
                    last_access: now + Duration::from_millis(index),
                },
            );
        }

        prune_attrs_locked(&mut inner, now, 2);

        assert_eq!(inner.attrs.len(), 2);
        assert!(!inner.attrs.contains_key("entry-0"));
        assert!(inner.attrs.contains_key("entry-3"));
    }
}
