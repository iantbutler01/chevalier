// @dive-file: Object-store backed optimized VFS storage implementation.
// @dive-rel: Composes object-store I/O, pack slots, pack cache, and the manifest index so
// @dive-rel: product code can consume one Chevalier-owned storage path instead of owning a GCS
// @dive-rel: adapter with duplicated VFS mechanics.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use futures::{
    StreamExt as _, TryStreamExt as _,
    stream::{self},
};
use uuid::Uuid;

use crate::{
    OptimizedVfsStorage, VfsStorageDeleteResult, VfsStorageDirListFilter, VfsStorageEntryKind,
    VfsStorageError, VfsStorageMetadata, VfsStorageMetadataFields, VfsStoragePrefetchOptions,
    VfsStoragePrefetchResult, VfsStorageReadIfChanged, VfsStorageReadIfChangedResult,
    VfsStorageReadRange, VfsStorageRenameResult, VfsStorageResult, VfsStorageSubtreeOptions,
    VfsStorageWrite, VfsStorageWritePrecondition, VfsStorageWriteResult,
    index::{
        VfsIndexEntryWithManifest, VfsIndexScope, VfsManifestIndex, VfsPackedCommit,
        VfsPackedFileCommit,
    },
    manifest::{VfsPackInput, build_pack_manifest},
    object_store::{ObjectStoreClient, ObjectWriteCondition},
    pack::{SlotCompression, extract_slot, hex_hash},
    pack_cache::PackCache,
};

const DEFAULT_PACK_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_SMALL_FILE_CACHE_MAX_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug)]
pub struct ObjectBackedVfsStorageConfig {
    pub scope: VfsIndexScope,
    pub pack_key_prefix: String,
    pub pack_cache_max_bytes: usize,
    pub small_file_cache_max_bytes: usize,
}

impl ObjectBackedVfsStorageConfig {
    pub fn new(scope: VfsIndexScope) -> Self {
        Self {
            scope,
            pack_key_prefix: "packs".to_string(),
            pack_cache_max_bytes: DEFAULT_PACK_CACHE_MAX_BYTES,
            small_file_cache_max_bytes: DEFAULT_SMALL_FILE_CACHE_MAX_BYTES,
        }
    }
}

pub struct ObjectBackedVfsStorage {
    cfg: ObjectBackedVfsStorageConfig,
    store: Arc<dyn ObjectStoreClient>,
    index: Arc<dyn VfsManifestIndex>,
    cache: ObjectBackedVfsCache,
}

struct ObjectBackedVfsCache {
    pack_bytes: Arc<PackCache>,
    file_bytes: Mutex<HashMap<String, Option<Bytes>>>,
}

impl ObjectBackedVfsStorage {
    pub fn new(
        cfg: ObjectBackedVfsStorageConfig,
        store: Arc<dyn ObjectStoreClient>,
        index: Arc<dyn VfsManifestIndex>,
    ) -> Self {
        let pack_cache_max_bytes = cfg.pack_cache_max_bytes;
        Self::new_with_pack_cache(
            cfg,
            store,
            index,
            Arc::new(PackCache::new(pack_cache_max_bytes)),
        )
    }

    pub fn new_with_pack_cache(
        cfg: ObjectBackedVfsStorageConfig,
        store: Arc<dyn ObjectStoreClient>,
        index: Arc<dyn VfsManifestIndex>,
        pack_cache: Arc<PackCache>,
    ) -> Self {
        Self {
            cfg,
            store,
            index,
            cache: ObjectBackedVfsCache {
                pack_bytes: pack_cache,
                file_bytes: Mutex::new(HashMap::new()),
            },
        }
    }

    fn build_pack_key(&self) -> String {
        let prefix = self.cfg.pack_key_prefix.trim_matches('/');
        let scope = sanitize_scope_for_key(&self.cfg.scope.key);
        let pack_id = Uuid::new_v4().simple();
        if prefix.is_empty() {
            format!("{scope}/{pack_id}.pack")
        } else {
            format!("{prefix}/{scope}/{pack_id}.pack")
        }
    }

    fn cached_file_bytes(&self, path: &str) -> Option<Option<Bytes>> {
        self.cache
            .file_bytes
            .lock()
            .ok()
            .and_then(|guard| guard.get(path).cloned())
    }

    fn put_file_bytes_cache(&self, path: String, bytes: Option<Bytes>) {
        if let Ok(mut guard) = self.cache.file_bytes.lock() {
            guard.insert(path, bytes);
        }
    }

    fn invalidate_file_bytes(&self, path: &str) {
        if let Ok(mut guard) = self.cache.file_bytes.lock() {
            guard.remove(path);
        }
    }

    async fn read_manifest_bytes(
        &self,
        manifest: &crate::manifest::VfsFileManifest,
    ) -> VfsStorageResult<Bytes> {
        if manifest.pack_slot.pack_slot_length == 0 {
            return Ok(Bytes::new());
        }
        if let Some(pack_bytes) = self.cache.pack_bytes.get(&manifest.pack_slot.pack_key) {
            let extracted = extract_slot(
                pack_bytes.as_slice(),
                manifest.pack_slot.pack_slot_offset as u64,
                manifest.pack_slot.pack_slot_length as u64,
            )?;
            return Ok(Bytes::from(extracted.bytes));
        }
        let Some(slot_bytes) = self
            .store
            .get_object_range_async(
                &manifest.pack_slot.pack_key,
                manifest.pack_slot.pack_slot_offset as u64,
                manifest.pack_slot.pack_slot_length as u64,
            )
            .await?
        else {
            return Err(VfsStorageError::NotFound(format!(
                "vfs pack {} not found",
                manifest.pack_slot.pack_key
            )));
        };
        let extracted = extract_slot(
            slot_bytes.as_slice(),
            0,
            manifest.pack_slot.pack_slot_length as u64,
        )?;
        Ok(Bytes::from(extracted.bytes))
    }

    fn path_parts(path: &str) -> (String, String) {
        let trimmed = path.trim_matches('/').to_string();
        let parent = trimmed
            .rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
            .unwrap_or_default();
        let name = trimmed
            .rsplit_once('/')
            .map(|(_, name)| name.to_string())
            .unwrap_or_else(|| trimmed.clone());
        (parent, name)
    }

    async fn create_dir_all_metadata(&self, path: &str) -> VfsStorageResult<()> {
        let trimmed = path.trim_matches('/');
        if trimmed.is_empty() {
            return Ok(());
        }
        let mut current = String::new();
        for segment in trimmed.split('/') {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(segment);
            let parent = current
                .rsplit_once('/')
                .map(|(parent, _)| parent.to_string())
                .unwrap_or_default();
            self.index
                .create_directory(&self.cfg.scope, &current, &parent, segment)
                .await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl OptimizedVfsStorage for ObjectBackedVfsStorage {
    fn backend_name(&self) -> &'static str {
        "object_store"
    }

    async fn stat(&self, path: &str) -> VfsStorageResult<Option<VfsStorageMetadata>> {
        self.index
            .get_entry_with_manifest(&self.cfg.scope, path)
            .await
            .map(|entry| entry.map(|entry| entry.into_storage_metadata()))
    }

    async fn metadata_many(
        &self,
        paths: &[String],
        _fields: VfsStorageMetadataFields,
    ) -> VfsStorageResult<Vec<Option<VfsStorageMetadata>>> {
        let entries = self
            .index
            .list_entries_with_manifest_by_paths(&self.cfg.scope, paths)
            .await?
            .into_iter()
            .map(|entry| (entry.entry.logical_path.clone(), entry))
            .collect::<HashMap<_, _>>();
        Ok(paths
            .iter()
            .map(|path| {
                entries
                    .get(path)
                    .cloned()
                    .map(VfsIndexEntryWithManifest::into_storage_metadata)
            })
            .collect())
    }

    async fn list_dir_with_metadata(
        &self,
        path: &str,
        filter: VfsStorageDirListFilter,
    ) -> VfsStorageResult<Vec<VfsStorageMetadata>> {
        let entries = self
            .index
            .list_dir_with_manifest_attrs(&self.cfg.scope, path, filter)
            .await?
            .into_iter()
            .map(|entry| entry.into_storage_metadata())
            .collect::<Vec<_>>();
        Ok(entries)
    }

    async fn list_subtree_file_metadata(
        &self,
        prefix: &str,
        options: VfsStorageSubtreeOptions,
    ) -> VfsStorageResult<Vec<VfsStorageMetadata>> {
        self.index
            .list_current_file_manifests_in_subtree(&self.cfg.scope, prefix, options.limit)
            .await
            .map(|manifests| {
                manifests
                    .into_iter()
                    .map(|manifest| VfsStorageMetadata {
                        path: manifest.logical_path.clone(),
                        kind: VfsStorageEntryKind::File,
                        size_bytes: manifest.logical_size_bytes.max(0) as u64,
                        file_id: None,
                        link_count: 1,
                        link_target: None,
                        executable: false,
                        content_hash: Some(manifest.content_hash.clone()),
                        token_count: manifest.token_count,
                        version: None,
                        updated_at: None,
                        object_state: Some(manifest.object_state()),
                    })
                    .collect()
            })
    }

    async fn read(&self, path: &str) -> VfsStorageResult<Bytes> {
        if let Some(cached) = self.cached_file_bytes(path) {
            return cached.ok_or_else(|| VfsStorageError::NotFound(path.to_string()));
        }
        let Some(manifest) = self
            .index
            .get_current_file_manifest(&self.cfg.scope, path)
            .await?
        else {
            self.put_file_bytes_cache(path.to_string(), None);
            return Err(VfsStorageError::NotFound(path.to_string()));
        };
        let bytes = self.read_manifest_bytes(&manifest).await?;
        if bytes.len() <= self.cfg.small_file_cache_max_bytes {
            self.put_file_bytes_cache(path.to_string(), Some(bytes.clone()));
        }
        Ok(bytes)
    }

    async fn read_range(&self, path: &str, range: VfsStorageReadRange) -> VfsStorageResult<Bytes> {
        if range.length == 0 {
            return Ok(Bytes::new());
        }
        let bytes = self.read(path).await?;
        let start = (range.offset as usize).min(bytes.len());
        let end = start.saturating_add(range.length as usize).min(bytes.len());
        Ok(bytes.slice(start..end))
    }

    async fn read_many(&self, paths: &[String]) -> VfsStorageResult<Vec<(String, Bytes)>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        let mut unresolved = Vec::new();
        for path in paths {
            match self.cached_file_bytes(path) {
                Some(Some(bytes)) => results.push((path.clone(), bytes)),
                Some(None) => {}
                None => unresolved.push(path.clone()),
            }
        }
        if unresolved.is_empty() {
            return Ok(results);
        }

        let manifests = self
            .index
            .list_current_file_manifests_by_paths(&self.cfg.scope, &unresolved)
            .await?;
        let mut manifest_by_path = manifests
            .into_iter()
            .map(|manifest| (manifest.logical_path.clone(), manifest))
            .collect::<HashMap<_, _>>();
        for path in &unresolved {
            if !manifest_by_path.contains_key(path) {
                self.put_file_bytes_cache(path.clone(), None);
            }
        }
        for path in manifest_by_path
            .iter()
            .filter_map(|(path, manifest)| {
                (manifest.pack_slot.pack_slot_length == 0).then_some(path.clone())
            })
            .collect::<Vec<_>>()
        {
            self.put_file_bytes_cache(path.clone(), Some(Bytes::new()));
            results.push((path.clone(), Bytes::new()));
            manifest_by_path.remove(&path);
        }

        let mut packs: HashMap<String, Vec<(String, i64, i64)>> = HashMap::new();
        for (path, manifest) in manifest_by_path {
            packs.entry(manifest.pack_slot.pack_key).or_default().push((
                path,
                manifest.pack_slot.pack_slot_offset,
                manifest.pack_slot.pack_slot_length,
            ));
        }
        let pack_results = stream::iter(packs)
            .map(|(pack_key, slots)| async move {
                let range_start = slots
                    .iter()
                    .map(|(_, offset, _)| *offset as u64)
                    .min()
                    .unwrap_or(0);
                let range_end = slots
                    .iter()
                    .map(|(_, offset, length)| (*offset + *length) as u64)
                    .max()
                    .unwrap_or(0);
                let range_length = range_end.saturating_sub(range_start);
                let Some(bytes) = self
                    .store
                    .get_object_range_async(&pack_key, range_start, range_length)
                    .await?
                else {
                    return Err(VfsStorageError::NotFound(format!(
                        "vfs pack {pack_key} not found"
                    )));
                };
                Ok::<_, VfsStorageError>((pack_key, range_start, slots, bytes))
            })
            .buffer_unordered(256)
            .try_collect::<Vec<_>>()
            .await?;
        for (_pack_key, range_start, slots, bytes) in pack_results {
            for (path, slot_offset, slot_length) in slots {
                let offset_within_range = slot_offset as u64 - range_start;
                let extracted =
                    extract_slot(bytes.as_slice(), offset_within_range, slot_length as u64)?;
                let bytes = Bytes::from(extracted.bytes);
                if bytes.len() <= self.cfg.small_file_cache_max_bytes {
                    self.put_file_bytes_cache(path.clone(), Some(bytes.clone()));
                }
                results.push((path, bytes));
            }
        }
        Ok(results)
    }

    async fn read_many_if_etag_mismatch(
        &self,
        requests: &[VfsStorageReadIfChanged],
    ) -> VfsStorageResult<Vec<VfsStorageReadIfChangedResult>> {
        let paths = requests
            .iter()
            .map(|request| request.path.clone())
            .collect::<Vec<_>>();
        let manifests = self
            .index
            .list_current_file_manifests_by_paths(&self.cfg.scope, &paths)
            .await?
            .into_iter()
            .map(|manifest| (manifest.logical_path.clone(), manifest))
            .collect::<HashMap<_, _>>();
        let mut out = Vec::with_capacity(requests.len());
        for request in requests {
            let Some(manifest) = manifests.get(&request.path) else {
                out.push(VfsStorageReadIfChangedResult {
                    path: request.path.clone(),
                    content_hash: None,
                    bytes: None,
                });
                continue;
            };
            if request.known_content_hash.as_deref() == Some(manifest.content_hash.as_str()) {
                out.push(VfsStorageReadIfChangedResult {
                    path: request.path.clone(),
                    content_hash: Some(manifest.content_hash.clone()),
                    bytes: None,
                });
            } else {
                out.push(VfsStorageReadIfChangedResult {
                    path: request.path.clone(),
                    content_hash: Some(manifest.content_hash.clone()),
                    bytes: Some(self.read(&request.path).await?),
                });
            }
        }
        Ok(out)
    }

    async fn write(
        &self,
        path: &str,
        bytes: Bytes,
        precondition: Option<VfsStorageWritePrecondition>,
    ) -> VfsStorageResult<VfsStorageWriteResult> {
        self.write_many_atomic(vec![VfsStorageWrite {
            path: path.to_string(),
            bytes,
            token_count: None,
            precondition,
        }])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| VfsStorageError::Internal("write returned no result".to_string()))
    }

    async fn write_many_atomic(
        &self,
        writes: Vec<VfsStorageWrite>,
    ) -> VfsStorageResult<Vec<VfsStorageWriteResult>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        assert_unique_write_paths(&writes)?;
        for write in &writes {
            let (parent_logical_path, _) = Self::path_parts(&write.path);
            self.create_dir_all_metadata(&parent_logical_path).await?;
        }
        let previous = self
            .index
            .list_entries_with_manifest_by_paths(
                &self.cfg.scope,
                &writes
                    .iter()
                    .map(|write| write.path.clone())
                    .collect::<Vec<_>>(),
            )
            .await?
            .into_iter()
            .map(|entry| (entry.entry.logical_path.clone(), entry))
            .collect::<HashMap<_, _>>();
        let pack_key = self.build_pack_key();
        let inputs = writes
            .iter()
            .map(|write| VfsPackInput {
                logical_path: write.path.as_str(),
                bytes: &write.bytes,
                compression: SlotCompression::Zstd,
                token_count: write.token_count,
            })
            .collect::<Vec<_>>();
        let built = build_pack_manifest(pack_key, &inputs)?;
        self.store
            .put_object_async(
                &built.pack_record.pack_key,
                &built.pack.pack_bytes,
                ObjectWriteCondition {
                    if_absent: true,
                    ..Default::default()
                },
            )
            .await?;
        self.cache.pack_bytes.put(
            built.pack_record.pack_key.clone(),
            Arc::new(built.pack.pack_bytes.clone()),
        );
        let commit = VfsPackedCommit {
            pack: built.pack_record,
            files: writes
                .iter()
                .zip(built.file_manifests.iter())
                .map(|(write, manifest)| {
                    let (parent_logical_path, entry_name) = Self::path_parts(&write.path);
                    VfsPackedFileCommit {
                        logical_path: write.path.clone(),
                        parent_logical_path,
                        entry_name,
                        manifest: manifest.clone(),
                        expected_current_version: write
                            .precondition
                            .as_ref()
                            .and_then(|precondition| precondition.fingerprint.clone())
                            .or_else(|| {
                                previous
                                    .get(&write.path)
                                    .and_then(|entry| entry.entry.current_version.clone())
                            }),
                    }
                })
                .collect(),
        };
        self.index
            .commit_packed_files(&self.cfg.scope, commit)
            .await?;
        let results = writes
            .into_iter()
            .zip(built.file_manifests)
            .map(|(write, manifest)| {
                self.invalidate_file_bytes(&write.path);
                VfsStorageWriteResult {
                    previous_hash: previous
                        .get(&write.path)
                        .and_then(|value| value.entry.content_hash.clone()),
                    path: write.path,
                    content_hash: manifest.content_hash,
                    changed: true,
                }
            })
            .collect();
        Ok(results)
    }

    async fn write_many_if_changed_atomic(
        &self,
        writes: Vec<VfsStorageWrite>,
    ) -> VfsStorageResult<Vec<VfsStorageWriteResult>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        assert_unique_write_paths(&writes)?;
        let paths = writes
            .iter()
            .map(|write| write.path.clone())
            .collect::<Vec<_>>();
        let current = self
            .index
            .list_current_file_manifests_by_paths(&self.cfg.scope, &paths)
            .await?
            .into_iter()
            .map(|manifest| (manifest.logical_path.clone(), manifest))
            .collect::<HashMap<_, _>>();
        let mut changed = Vec::new();
        let mut unchanged = Vec::new();
        for write in writes {
            let next_hash = hex_hash(&write.bytes);
            let previous_hash = current
                .get(&write.path)
                .map(|manifest| manifest.content_hash.clone());
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
        let mut out = self.write_many_atomic(changed).await?;
        out.extend(unchanged);
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    async fn mkdir(&self, path: &str) -> VfsStorageResult<()> {
        self.create_dir_all_metadata(path).await
    }

    async fn delete_file_with_metadata(
        &self,
        path: &str,
        precondition: Option<VfsStorageWritePrecondition>,
    ) -> VfsStorageResult<VfsStorageDeleteResult> {
        let previous = self
            .index
            .delete_file_entry(
                &self.cfg.scope,
                path,
                precondition
                    .as_ref()
                    .and_then(|precondition| precondition.fingerprint.as_deref()),
            )
            .await?
            .map(VfsIndexEntryWithManifest::into_storage_metadata);
        self.invalidate_file_bytes(path);
        Ok(VfsStorageDeleteResult { previous })
    }

    async fn rmdir(&self, path: &str) -> VfsStorageResult<()> {
        self.index
            .remove_empty_directory(&self.cfg.scope, path)
            .await
    }

    async fn rename_with_metadata(
        &self,
        from: &str,
        to: &str,
    ) -> VfsStorageResult<VfsStorageRenameResult> {
        let Some(source) = self
            .index
            .get_entry_with_manifest(&self.cfg.scope, from)
            .await?
        else {
            return Err(VfsStorageError::NotFound(from.to_string()));
        };
        if source.entry.kind != VfsStorageEntryKind::File {
            return Err(VfsStorageError::BadRequest(format!(
                "vfs path {from} is not a file"
            )));
        }
        let (to_parent_logical_path, to_entry_name) = Self::path_parts(to);
        self.create_dir_all_metadata(&to_parent_logical_path)
            .await?;
        let (previous, current) = self
            .index
            .rename_file_entry(
                &self.cfg.scope,
                from,
                to,
                &to_parent_logical_path,
                &to_entry_name,
            )
            .await?;
        self.invalidate_file_bytes(from);
        self.invalidate_file_bytes(to);
        Ok(VfsStorageRenameResult {
            previous: Some(previous.into_storage_metadata()),
            current: Some(current.into_storage_metadata()),
        })
    }

    async fn prefetch_subtree(
        &self,
        prefix: &str,
        options: VfsStoragePrefetchOptions,
    ) -> VfsStorageResult<VfsStoragePrefetchResult> {
        let manifests = self
            .index
            .list_current_file_manifests_in_subtree(&self.cfg.scope, prefix, options.max_entries)
            .await?;
        let mut seen = HashSet::new();
        let unique_pack_keys = manifests
            .iter()
            .filter_map(|manifest| {
                seen.insert(manifest.pack_slot.pack_key.clone())
                    .then_some(manifest.pack_slot.pack_key.clone())
            })
            .collect::<Vec<_>>();
        let fetch_results = stream::iter(unique_pack_keys)
            .map(|pack_key| async move {
                self.store
                    .get_object_async(&pack_key)
                    .await
                    .map(|bytes| (pack_key, bytes))
            })
            .buffer_unordered(256)
            .try_collect::<Vec<_>>()
            .await?;
        for (pack_key, bytes) in fetch_results {
            let Some(bytes) = bytes else { continue };
            self.cache.pack_bytes.put(pack_key, Arc::new(bytes));
        }
        if !options.include_small_file_bytes {
            return Ok(VfsStoragePrefetchResult::default());
        }
        let mut warmed_file_bytes = Vec::new();
        for manifest in manifests {
            if manifest.logical_size_bytes as usize > self.cfg.small_file_cache_max_bytes {
                continue;
            }
            if let Some(pack_bytes) = self.cache.pack_bytes.get(&manifest.pack_slot.pack_key) {
                let extracted = extract_slot(
                    pack_bytes.as_slice(),
                    manifest.pack_slot.pack_slot_offset as u64,
                    manifest.pack_slot.pack_slot_length as u64,
                )?;
                let bytes = Bytes::from(extracted.bytes);
                self.put_file_bytes_cache(manifest.logical_path.clone(), Some(bytes.clone()));
                warmed_file_bytes.push((manifest.logical_path, bytes));
            }
        }
        Ok(VfsStoragePrefetchResult { warmed_file_bytes })
    }
}

fn assert_unique_write_paths(writes: &[VfsStorageWrite]) -> VfsStorageResult<()> {
    let mut seen = HashSet::new();
    for write in writes {
        if !seen.insert(write.path.as_str()) {
            return Err(VfsStorageError::BadRequest(format!(
                "duplicate vfs write path: {}",
                write.path
            )));
        }
    }
    Ok(())
}

fn sanitize_scope_for_key(scope: &str) -> String {
    scope
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use async_trait::async_trait;
    use chrono::Utc;

    use crate::{
        index::{VfsIndexEntry, VfsIndexEntryWithManifest, VfsPackedCommitResult},
        object_store::LocalObjectStoreClient,
    };

    #[derive(Default)]
    struct MemoryIndex {
        inner: Mutex<MemoryIndexInner>,
    }

    #[derive(Default)]
    struct MemoryIndexInner {
        entries: HashMap<String, VfsIndexEntryWithManifest>,
        version_counter: u64,
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

    #[async_trait]
    impl VfsManifestIndex for MemoryIndex {
        async fn get_current_file_manifest(
            &self,
            _scope: &VfsIndexScope,
            logical_path: &str,
        ) -> VfsStorageResult<Option<crate::manifest::VfsFileManifest>> {
            Ok(self
                .inner
                .lock()
                .unwrap()
                .entries
                .get(logical_path)
                .and_then(|entry| entry.manifest.clone()))
        }

        async fn list_current_file_manifests_by_paths(
            &self,
            _scope: &VfsIndexScope,
            logical_paths: &[String],
        ) -> VfsStorageResult<Vec<crate::manifest::VfsFileManifest>> {
            let guard = self.inner.lock().unwrap();
            Ok(logical_paths
                .iter()
                .filter_map(|path| {
                    guard
                        .entries
                        .get(path)
                        .and_then(|entry| entry.manifest.clone())
                })
                .collect())
        }

        async fn list_current_file_manifests_in_subtree(
            &self,
            _scope: &VfsIndexScope,
            logical_path_prefix: &str,
            limit: Option<i64>,
        ) -> VfsStorageResult<Vec<crate::manifest::VfsFileManifest>> {
            let mut manifests = self
                .inner
                .lock()
                .unwrap()
                .entries
                .values()
                .filter(|entry| {
                    logical_path_prefix.is_empty()
                        || entry.entry.logical_path == logical_path_prefix
                        || entry
                            .entry
                            .logical_path
                            .starts_with(&format!("{logical_path_prefix}/"))
                })
                .filter_map(|entry| entry.manifest.clone())
                .collect::<Vec<_>>();
            manifests.sort_by(|a, b| a.logical_path.cmp(&b.logical_path));
            if let Some(limit) = limit {
                manifests.truncate(limit.max(0) as usize);
            }
            Ok(manifests)
        }

        async fn get_entry_with_manifest(
            &self,
            _scope: &VfsIndexScope,
            logical_path: &str,
        ) -> VfsStorageResult<Option<VfsIndexEntryWithManifest>> {
            Ok(self
                .inner
                .lock()
                .unwrap()
                .entries
                .get(logical_path)
                .cloned())
        }

        async fn list_entries_with_manifest_by_paths(
            &self,
            _scope: &VfsIndexScope,
            logical_paths: &[String],
        ) -> VfsStorageResult<Vec<VfsIndexEntryWithManifest>> {
            let guard = self.inner.lock().unwrap();
            Ok(logical_paths
                .iter()
                .filter_map(|path| guard.entries.get(path).cloned())
                .collect())
        }

        async fn list_dir_with_manifest_attrs(
            &self,
            _scope: &VfsIndexScope,
            parent_logical_path: &str,
            filter: VfsStorageDirListFilter,
        ) -> VfsStorageResult<Vec<VfsIndexEntryWithManifest>> {
            let mut entries = self
                .inner
                .lock()
                .unwrap()
                .entries
                .values()
                .filter(|entry| entry.entry.parent_logical_path == parent_logical_path)
                .filter(|entry| {
                    filter
                        .name_like
                        .as_deref()
                        .is_none_or(|pattern| sql_like_match(pattern, &entry.entry.entry_name))
                })
                .filter(|entry| {
                    filter
                        .name_not_like
                        .as_deref()
                        .is_none_or(|pattern| !sql_like_match(pattern, &entry.entry.entry_name))
                })
                .filter(|entry| {
                    filter
                        .entry_kind
                        .is_none_or(|kind| entry.entry.kind == kind)
                })
                .cloned()
                .collect::<Vec<_>>();
            match filter
                .order
                .unwrap_or(crate::VfsStorageDirListOrder::KindThenName)
            {
                crate::VfsStorageDirListOrder::KindThenName => entries.sort_by(|a, b| {
                    b.entry
                        .kind
                        .as_str()
                        .cmp(a.entry.kind.as_str())
                        .then_with(|| a.entry.entry_name.cmp(&b.entry.entry_name))
                }),
                crate::VfsStorageDirListOrder::NameAsc => {
                    entries.sort_by(|a, b| a.entry.entry_name.cmp(&b.entry.entry_name));
                }
                crate::VfsStorageDirListOrder::NameDesc => {
                    entries.sort_by(|a, b| b.entry.entry_name.cmp(&a.entry.entry_name));
                }
                crate::VfsStorageDirListOrder::UpdatedDesc => {
                    entries.sort_by(|a, b| b.entry.updated_at.cmp(&a.entry.updated_at));
                }
            }
            if let Some(limit) = filter.limit {
                entries.truncate(limit.max(0) as usize);
            }
            Ok(entries)
        }

        async fn commit_packed_files(
            &self,
            _scope: &VfsIndexScope,
            commit: VfsPackedCommit,
        ) -> VfsStorageResult<VfsPackedCommitResult> {
            let mut guard = self.inner.lock().unwrap();
            for file in &commit.files {
                let actual = guard
                    .entries
                    .get(&file.logical_path)
                    .and_then(|entry| entry.entry.current_version.as_deref());
                if actual != file.expected_current_version.as_deref() {
                    return Err(VfsStorageError::Conflict(format!(
                        "conflict for {}",
                        file.logical_path
                    )));
                }
            }
            let mut committed_paths = Vec::with_capacity(commit.files.len());
            for file in commit.files {
                guard.version_counter += 1;
                let version = format!("v{}", guard.version_counter);
                guard.entries.insert(
                    file.logical_path.clone(),
                    VfsIndexEntryWithManifest {
                        entry: VfsIndexEntry {
                            logical_path: file.logical_path.clone(),
                            parent_logical_path: file.parent_logical_path,
                            entry_name: file.entry_name,
                            kind: VfsStorageEntryKind::File,
                            size_bytes: file.manifest.logical_size_bytes,
                            content_hash: Some(file.manifest.content_hash.clone()),
                            current_version: Some(version),
                            updated_at: Some(Utc::now()),
                        },
                        manifest: Some(file.manifest),
                    },
                );
                committed_paths.push(file.logical_path);
            }
            Ok(VfsPackedCommitResult { committed_paths })
        }

        async fn create_directory(
            &self,
            _scope: &VfsIndexScope,
            logical_path: &str,
            parent_logical_path: &str,
            entry_name: &str,
        ) -> VfsStorageResult<()> {
            let mut guard = self.inner.lock().unwrap();
            if matches!(
                guard
                    .entries
                    .get(logical_path)
                    .map(|entry| entry.entry.kind),
                Some(VfsStorageEntryKind::File)
            ) {
                return Err(VfsStorageError::Conflict(format!(
                    "vfs file already exists at directory path: {logical_path}"
                )));
            }
            guard.entries.insert(
                logical_path.to_string(),
                VfsIndexEntryWithManifest {
                    entry: VfsIndexEntry {
                        logical_path: logical_path.to_string(),
                        parent_logical_path: parent_logical_path.to_string(),
                        entry_name: entry_name.to_string(),
                        kind: VfsStorageEntryKind::Directory,
                        size_bytes: 0,
                        content_hash: None,
                        current_version: None,
                        updated_at: Some(Utc::now()),
                    },
                    manifest: None,
                },
            );
            Ok(())
        }

        async fn delete_file_entry(
            &self,
            _scope: &VfsIndexScope,
            logical_path: &str,
            expected_current_version: Option<&str>,
        ) -> VfsStorageResult<Option<VfsIndexEntryWithManifest>> {
            let mut guard = self.inner.lock().unwrap();
            match guard.entries.get(logical_path) {
                None => Ok(None),
                Some(entry) if entry.entry.kind == VfsStorageEntryKind::Directory => Err(
                    VfsStorageError::BadRequest(format!("vfs path {logical_path} is not a file")),
                ),
                Some(entry)
                    if expected_current_version.is_some()
                        && entry.entry.current_version.as_deref() != expected_current_version =>
                {
                    Err(VfsStorageError::Conflict(format!(
                        "vfs write precondition failed for {logical_path}"
                    )))
                }
                Some(_) => Ok(guard.entries.remove(logical_path)),
            }
        }

        async fn remove_empty_directory(
            &self,
            _scope: &VfsIndexScope,
            logical_path: &str,
        ) -> VfsStorageResult<()> {
            let mut guard = self.inner.lock().unwrap();
            let Some(entry) = guard.entries.get(logical_path) else {
                return Ok(());
            };
            if entry.entry.kind != VfsStorageEntryKind::Directory {
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs path {logical_path} is not a directory"
                )));
            }
            if guard
                .entries
                .values()
                .any(|entry| entry.entry.parent_logical_path == logical_path)
            {
                return Err(VfsStorageError::Conflict(format!(
                    "vfs directory {logical_path} is not empty"
                )));
            }
            guard.entries.remove(logical_path);
            Ok(())
        }

        async fn rename_file_entry(
            &self,
            _scope: &VfsIndexScope,
            from_logical_path: &str,
            to_logical_path: &str,
            to_parent_logical_path: &str,
            to_entry_name: &str,
        ) -> VfsStorageResult<(VfsIndexEntryWithManifest, VfsIndexEntryWithManifest)> {
            let mut guard = self.inner.lock().unwrap();
            if guard.entries.contains_key(to_logical_path) {
                return Err(VfsStorageError::Conflict(format!(
                    "vfs destination already exists: {to_logical_path}"
                )));
            }
            let Some(previous) = guard.entries.remove(from_logical_path) else {
                return Err(VfsStorageError::NotFound(from_logical_path.to_string()));
            };
            if previous.entry.kind != VfsStorageEntryKind::File {
                guard
                    .entries
                    .insert(from_logical_path.to_string(), previous.clone());
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs path {from_logical_path} is not a file"
                )));
            }
            let mut current = previous.clone();
            current.entry.logical_path = to_logical_path.to_string();
            current.entry.parent_logical_path = to_parent_logical_path.to_string();
            current.entry.entry_name = to_entry_name.to_string();
            current.entry.updated_at = Some(Utc::now());
            if let Some(manifest) = current.manifest.as_mut() {
                manifest.logical_path = to_logical_path.to_string();
            }
            guard
                .entries
                .insert(to_logical_path.to_string(), current.clone());
            Ok((previous, current))
        }
    }

    fn object_storage() -> (ObjectBackedVfsStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(LocalObjectStoreClient::new(dir.path().to_path_buf()).unwrap());
        let index = Arc::new(MemoryIndex::default());
        let storage = ObjectBackedVfsStorage::new(
            ObjectBackedVfsStorageConfig::new(VfsIndexScope::new("test-scope")),
            store,
            index,
        );
        (storage, dir)
    }

    #[tokio::test]
    async fn object_storage_round_trips_pack_backed_batch_reads() {
        let (storage, _dir) = object_storage();
        let results = storage
            .write_many_atomic(vec![
                VfsStorageWrite {
                    path: "notes/a.md".to_string(),
                    bytes: Bytes::from_static(b"alpha"),
                    token_count: Some(1),
                    precondition: None,
                },
                VfsStorageWrite {
                    path: "notes/b.md".to_string(),
                    bytes: Bytes::from_static(b"beta beta"),
                    token_count: Some(2),
                    precondition: None,
                },
            ])
            .await
            .expect("write_many");
        assert_eq!(results.len(), 2);

        let many = storage
            .read_many(&[
                "notes/a.md".to_string(),
                "missing.md".to_string(),
                "notes/b.md".to_string(),
            ])
            .await
            .expect("read_many");
        let by_path = many.into_iter().collect::<HashMap<_, _>>();
        assert_eq!(&by_path["notes/a.md"][..], b"alpha");
        assert_eq!(&by_path["notes/b.md"][..], b"beta beta");

        let range = storage
            .read_range(
                "notes/b.md",
                VfsStorageReadRange {
                    offset: 5,
                    length: 4,
                },
            )
            .await
            .expect("range");
        assert_eq!(&range[..], b"beta");
        assert_eq!(
            storage
                .stat("notes/b.md")
                .await
                .expect("stat")
                .and_then(|meta| meta.token_count),
            Some(2)
        );
    }

    #[tokio::test]
    async fn object_storage_changed_only_uses_manifest_hashes() {
        let (storage, _dir) = object_storage();
        storage
            .write("same.md", Bytes::from_static(b"same"), None)
            .await
            .expect("initial");
        let results = storage
            .write_many_if_changed_atomic(vec![
                VfsStorageWrite {
                    path: "same.md".to_string(),
                    bytes: Bytes::from_static(b"same"),
                    token_count: None,
                    precondition: None,
                },
                VfsStorageWrite {
                    path: "changed.md".to_string(),
                    bytes: Bytes::from_static(b"new"),
                    token_count: None,
                    precondition: None,
                },
            ])
            .await
            .expect("changed-only");
        let by_path = results
            .into_iter()
            .map(|result| (result.path.clone(), result))
            .collect::<HashMap<_, _>>();
        assert!(!by_path["same.md"].changed);
        assert!(by_path["changed.md"].changed);
    }

    #[tokio::test]
    async fn object_storage_prefetch_returns_warmed_small_file_bytes() {
        let (storage, _dir) = object_storage();
        storage
            .write_many_atomic(vec![
                VfsStorageWrite {
                    path: "notes/a.md".to_string(),
                    bytes: Bytes::from_static(b"alpha"),
                    token_count: None,
                    precondition: None,
                },
                VfsStorageWrite {
                    path: "notes/b.md".to_string(),
                    bytes: Bytes::from_static(b"beta"),
                    token_count: None,
                    precondition: None,
                },
            ])
            .await
            .expect("write files");

        let warmed = storage
            .prefetch_subtree(
                "notes",
                VfsStoragePrefetchOptions {
                    include_small_file_bytes: true,
                    max_entries: Some(10),
                    max_pack_bytes: None,
                },
            )
            .await
            .expect("prefetch subtree");
        let by_path = warmed
            .warmed_file_bytes
            .into_iter()
            .collect::<HashMap<_, _>>();

        assert_eq!(&by_path["notes/a.md"][..], b"alpha");
        assert_eq!(&by_path["notes/b.md"][..], b"beta");
    }

    #[tokio::test]
    async fn object_storage_rejects_stale_write_precondition() {
        let (storage, _dir) = object_storage();
        let first = storage
            .write("guarded.md", Bytes::from_static(b"first"), None)
            .await
            .expect("initial");
        let first_version = storage
            .stat("guarded.md")
            .await
            .unwrap()
            .unwrap()
            .version
            .unwrap();
        storage
            .write("guarded.md", Bytes::from_static(b"second"), None)
            .await
            .expect("racing write");
        let err = storage
            .write(
                "guarded.md",
                Bytes::from_static(b"third"),
                Some(VfsStorageWritePrecondition {
                    fingerprint: Some(first_version),
                    secondary_fingerprint: None,
                }),
            )
            .await
            .expect_err("stale precondition");
        assert_eq!(first.content_hash, hex_hash(b"first"));
        assert!(matches!(err, VfsStorageError::Conflict(_)));
    }

    #[tokio::test]
    async fn object_storage_keeps_directory_metadata_for_nested_writes_and_rename() {
        let (storage, _dir) = object_storage();
        storage
            .write("notes/a.md", Bytes::from_static(b"alpha"), None)
            .await
            .expect("write nested file");

        let root = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("list root");
        assert_eq!(root.len(), 1);
        assert_eq!(root[0].path, "notes");
        assert_eq!(root[0].kind, VfsStorageEntryKind::Directory);

        let notes = storage
            .list_dir_with_metadata("notes", VfsStorageDirListFilter::default())
            .await
            .expect("list notes");
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].path, "notes/a.md");
        assert_eq!(notes[0].kind, VfsStorageEntryKind::File);

        let rename = storage
            .rename_with_metadata("notes/a.md", "archive/a.md")
            .await
            .expect("rename file");
        assert_eq!(
            rename.previous.as_ref().map(|meta| meta.path.as_str()),
            Some("notes/a.md")
        );
        assert_eq!(
            rename.current.as_ref().map(|meta| meta.path.as_str()),
            Some("archive/a.md")
        );
        assert!(storage.read("notes/a.md").await.is_err());
        assert_eq!(&storage.read("archive/a.md").await.unwrap()[..], b"alpha");

        let delete = storage
            .delete_file_with_metadata("archive/a.md", None)
            .await
            .expect("delete file");
        assert_eq!(
            delete.previous.as_ref().map(|meta| meta.path.as_str()),
            Some("archive/a.md")
        );
        storage.rmdir("archive").await.expect("remove empty dir");
    }

    #[tokio::test]
    async fn object_storage_rejects_directory_as_file_delete_and_non_empty_rmdir() {
        let (storage, _dir) = object_storage();
        storage.mkdir("notes").await.expect("mkdir");
        let err = storage
            .delete_file_with_metadata("notes", None)
            .await
            .expect_err("directory is not a file");
        assert!(matches!(err, VfsStorageError::BadRequest(_)));

        storage
            .write("notes/a.md", Bytes::from_static(b"alpha"), None)
            .await
            .expect("write child");
        let err = storage.rmdir("notes").await.expect_err("not empty");
        assert!(matches!(err, VfsStorageError::Conflict(_)));
    }

    #[tokio::test]
    async fn object_storage_rejects_stale_delete_precondition() {
        let (storage, _dir) = object_storage();
        storage
            .write("notes/a.md", Bytes::from_static(b"alpha"), None)
            .await
            .expect("write");
        let err = storage
            .delete_file_with_metadata(
                "notes/a.md",
                Some(VfsStorageWritePrecondition {
                    fingerprint: Some("stale-version".to_string()),
                    secondary_fingerprint: None,
                }),
            )
            .await
            .expect_err("stale delete precondition");
        assert!(matches!(err, VfsStorageError::Conflict(_)));
        assert_eq!(&storage.read("notes/a.md").await.unwrap()[..], b"alpha");
    }
}
