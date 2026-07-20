use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use chevalier_sandbox::vfs::{VfsDirEntry as RemoteDirEntry, VfsMetadata as RemoteMetadata};

const FILE_TTL: Duration = Duration::from_secs(60);
const MAX_FILE_BYTES: usize = 10 * 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 256 * 1024 * 1024;
const MAX_FILES: usize = 16_384;

#[derive(Clone)]
struct CachedFile {
    /// Read-through copy only. Dirty/open authoritative buffers live in the
    /// FUSE handle table, while journal-enqueued writes own durability.
    bytes: Vec<u8>,
    metadata: Option<RemoteMetadata>,
    expires_at: Instant,
    last_access: Instant,
}

#[derive(Default)]
struct CacheState {
    file_bytes: usize,
    files: HashMap<String, CachedFile>,
    identity_paths: HashMap<String, std::collections::HashSet<String>>,
    directory_generation: u64,
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
            remove_file_locked(&mut inner, path);
            return None;
        }
        entry.last_access = Instant::now();
        Some(entry.bytes.clone())
    }

    /// Return the metadata bundled with a still-live content-cache entry.
    /// Callers may use this only for same-mount isolation of a dirty existing
    /// handle; ordinary lookup/getattr/open paths must revalidate remotely.
    pub fn get_committed_file_metadata(&self, path: &str) -> Option<RemoteMetadata> {
        let mut inner = self.lock_inner();
        let entry = inner.files.get_mut(path)?;
        if entry.expires_at <= Instant::now() {
            remove_file_locked(&mut inner, path);
            return None;
        }
        entry.last_access = Instant::now();
        entry.metadata.clone()
    }

    pub fn put_file(&self, path: &str, bytes: Vec<u8>, metadata: Option<RemoteMetadata>) {
        if bytes.len() > MAX_FILE_BYTES {
            return;
        }
        let mut inner = self.lock_inner();
        remove_file_locked(&mut inner, path);
        let now = Instant::now();
        inner.file_bytes += bytes.len();
        inner.files.insert(
            path.to_string(),
            CachedFile {
                bytes,
                metadata,
                expires_at: now + FILE_TTL,
                last_access: now,
            },
        );
        if let Some(file_id) = inner
            .files
            .get(path)
            .and_then(|entry| entry.metadata.as_ref())
            .and_then(|metadata| metadata.file_id.as_ref())
            .cloned()
        {
            inner
                .identity_paths
                .entry(file_id)
                .or_default()
                .insert(path.to_string());
        }
        enforce_file_limits_locked(&mut inner, now, MAX_FILES, MAX_TOTAL_BYTES);
    }

    /// Directory and attribute responses are never retained. A different VM
    /// can publish through another virtiofsd/FUSE session, and this process has
    /// no remote invalidation channel that can make a positive TTL coherent.
    pub fn get_dir(&self, _path: &str) -> Option<Vec<RemoteDirEntry>> {
        None
    }

    pub fn put_dir(&self, path: &str, entries: Vec<RemoteDirEntry>) {
        let generation = self.directory_generation(path);
        let _ = self.put_dir_if_generation(path, generation, entries);
    }

    pub fn directory_generation(&self, _path: &str) -> u64 {
        self.lock_inner().directory_generation
    }

    /// Accept a listing only if no concurrent local namespace mutation
    /// invalidated it while the authoritative request was in flight. The
    /// listing itself is deliberately not retained.
    pub fn put_dir_if_generation(
        &self,
        _path: &str,
        generation: u64,
        _entries: Vec<RemoteDirEntry>,
    ) -> bool {
        self.lock_inner().directory_generation == generation
    }

    pub fn get_metadata(&self, _path: &str) -> Option<RemoteMetadata> {
        None
    }

    pub fn put_metadata(&self, _path: &str, _metadata: RemoteMetadata) {}

    pub fn invalidate(&self, path: &str) {
        let mut inner = self.lock_inner();
        invalidate_path_locked(&mut inner, path);
    }

    pub fn invalidate_identity(&self, file_id: &str) {
        let mut inner = self.lock_inner();
        let paths = inner
            .identity_paths
            .get(file_id)
            .cloned()
            .unwrap_or_default();
        for path in paths {
            invalidate_path_locked(&mut inner, &path);
        }
    }

    pub fn aliases_for_identity(&self, file_id: &str) -> Vec<String> {
        self.lock_inner()
            .identity_paths
            .get(file_id)
            .map(|paths| paths.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn lock_inner(&self) -> MutexGuard<'_, CacheState> {
        self.inner.lock().unwrap_or_else(|err| err.into_inner())
    }
}

fn invalidate_path_locked(inner: &mut CacheState, path: &str) {
    remove_file_locked(inner, path);
    bump_directory_generation(inner, path);
    if let Some(parent) = parent_path(path) {
        bump_directory_generation(inner, parent.as_str());
    }
}

fn remove_file_locked(inner: &mut CacheState, path: &str) {
    let Some(previous) = inner.files.remove(path) else {
        return;
    };
    inner.file_bytes = inner.file_bytes.saturating_sub(previous.bytes.len());
    if let Some(file_id) = previous.metadata.and_then(|metadata| metadata.file_id) {
        let remove_identity = inner.identity_paths.get_mut(&file_id).is_some_and(|paths| {
            paths.remove(path);
            paths.is_empty()
        });
        if remove_identity {
            inner.identity_paths.remove(&file_id);
        }
    }
}

fn bump_directory_generation(inner: &mut CacheState, _path: &str) {
    inner.directory_generation = inner.directory_generation.wrapping_add(1);
}

fn cached_metadata_matches(cached: Option<&RemoteMetadata>, current: &RemoteMetadata) -> bool {
    let Some(cached) = cached else {
        return false;
    };
    if cached.file_id != current.file_id || cached.link_count != current.link_count {
        return false;
    }
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

fn enforce_file_limits_locked(
    inner: &mut CacheState,
    now: Instant,
    max_entries: usize,
    max_bytes: usize,
) {
    let entries_exceeded = inner.files.len() > max_entries;
    let bytes_exceeded = inner.file_bytes > max_bytes;
    if !entries_exceeded && !bytes_exceeded {
        return;
    }
    let target_entries = if entries_exceeded {
        max_entries.saturating_mul(3) / 4
    } else {
        max_entries
    };
    let target_bytes = if bytes_exceeded {
        max_bytes.saturating_mul(3) / 4
    } else {
        max_bytes
    };
    prune_files_locked(inner, now, target_entries, target_bytes);
}

fn prune_files_locked(
    inner: &mut CacheState,
    now: Instant,
    target_entries: usize,
    target_bytes: usize,
) {
    let expired = inner
        .files
        .iter()
        .filter(|(_, value)| value.expires_at <= now)
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    for path in expired {
        remove_file_locked(inner, path.as_str());
    }
    if inner.files.len() <= target_entries && inner.file_bytes <= target_bytes {
        return;
    }
    let mut oldest = inner
        .files
        .iter()
        .map(|(path, value)| (path.clone(), value.last_access))
        .collect::<Vec<_>>();
    oldest.sort_unstable_by_key(|(_, last_access)| *last_access);
    for (path, _) in oldest {
        if inner.files.len() <= target_entries && inner.file_bytes <= target_bytes {
            break;
        }
        remove_file_locked(inner, path.as_str());
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

    use super::{CacheState, CachedFile, MAX_FILES, RemoteFuseCache, enforce_file_limits_locked};
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
    fn directory_and_metadata_responses_are_never_retained() {
        let cache = RemoteFuseCache::default();
        cache.put_dir("tree", vec![entry("file")]);
        cache.put_metadata("tree/file", metadata("hash", 4));

        assert!(cache.get_dir("tree").is_none());
        assert!(cache.get_metadata("tree/file").is_none());
    }

    #[test]
    fn file_cache_requires_matching_authoritative_metadata() {
        let cache = RemoteFuseCache::default();
        let expected = metadata("complete-hash", 8);
        cache.put_file("Cargo.toml", b"complete".to_vec(), Some(expected.clone()));

        assert_eq!(
            cache.get_committed_file_metadata("Cargo.toml"),
            Some(expected.clone())
        );
        assert_eq!(
            cache.get_file_matching("Cargo.toml", &expected),
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
    fn file_cache_caps_twenty_thousand_zero_byte_entries() {
        let cache = RemoteFuseCache::default();
        for index in 0..20_000 {
            cache.put_file(&format!("zero-{index}"), Vec::new(), None);
        }

        let inner = cache.lock_inner();
        assert!(inner.files.len() <= MAX_FILES);
        assert_eq!(inner.file_bytes, 0);
        assert!(!inner.files.contains_key("zero-0"));
        assert!(inner.files.contains_key("zero-19999"));
    }

    #[test]
    fn file_cache_cardinality_eviction_preserves_hot_and_recent_entries() {
        let cache = RemoteFuseCache::default();
        let expected = metadata("empty", 0);
        for index in 0..MAX_FILES {
            cache.put_file(
                &format!("entry-{index}"),
                Vec::new(),
                Some(expected.clone()),
            );
        }
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(
            cache.get_file_matching("entry-0", &expected),
            Some(Vec::new())
        );
        cache.put_file("overflow", Vec::new(), Some(expected.clone()));

        assert_eq!(
            cache.get_file_matching("entry-0", &expected),
            Some(Vec::new())
        );
        assert!(cache.get_file_matching("entry-1", &expected).is_none());
        assert_eq!(
            cache.get_file_matching("overflow", &expected),
            Some(Vec::new())
        );
        assert!(cache.lock_inner().files.len() <= MAX_FILES);
    }

    #[test]
    fn file_cache_byte_eviction_uses_the_same_lru() {
        let now = Instant::now();
        let mut inner = CacheState::default();
        for (index, path) in ["old", "middle", "hot"].into_iter().enumerate() {
            let file_metadata = metadata(path, 40);
            inner.file_bytes += 40;
            inner.files.insert(
                path.to_string(),
                CachedFile {
                    bytes: vec![index as u8; 40],
                    metadata: Some(file_metadata.clone()),
                    expires_at: now + Duration::from_secs(1),
                    last_access: now + Duration::from_millis(index as u64),
                },
            );
        }

        enforce_file_limits_locked(&mut inner, now, 10, 100);

        assert_eq!(inner.file_bytes, 40);
        assert_eq!(inner.files.len(), 1);
        assert!(inner.files.contains_key("hot"));
    }

    #[test]
    fn identity_invalidation_drops_every_alias_and_parent_listing() {
        let cache = RemoteFuseCache::default();
        let mut shared = metadata("hash", 4);
        shared.file_id = Some("inode-1".to_string());
        shared.link_count = 2;
        cache.put_dir("left", vec![entry("a")]);
        cache.put_dir("right", vec![entry("b")]);
        cache.put_file("left/a", b"body".to_vec(), Some(shared.clone()));
        cache.put_file("right/b", b"body".to_vec(), Some(shared.clone()));

        cache.invalidate_identity("inode-1");

        assert!(cache.get_file_matching("left/a", &shared).is_none());
        assert!(cache.get_file_matching("right/b", &shared).is_none());
        assert!(cache.get_dir("left").is_none());
        assert!(cache.get_dir("right").is_none());
        assert!(cache.aliases_for_identity("inode-1").is_empty());
    }

    #[test]
    fn replacing_content_cache_entry_prunes_old_identity_reverse_index() {
        let cache = RemoteFuseCache::default();
        let mut first = metadata("first", 1);
        first.file_id = Some("inode-first".to_string());
        let mut second = metadata("second", 1);
        second.file_id = Some("inode-second".to_string());
        cache.put_file("path", b"a".to_vec(), Some(first));
        cache.put_file("path", b"b".to_vec(), Some(second));

        assert!(cache.aliases_for_identity("inode-first").is_empty());
        assert_eq!(
            cache.aliases_for_identity("inode-second"),
            vec!["path".to_string()]
        );
    }

    #[test]
    fn stale_directory_response_cannot_repopulate_after_invalidation() {
        let cache = RemoteFuseCache::default();
        let generation = cache.directory_generation("tree");
        cache.invalidate("tree/new");
        assert!(!cache.put_dir_if_generation("tree", generation, vec![entry("stale")]));
        assert!(cache.get_dir("tree").is_none());

        let current = cache.directory_generation("tree");
        assert!(cache.put_dir_if_generation("tree", current, vec![entry("current")]));
        assert!(cache.get_dir("tree").is_none());
    }
}
