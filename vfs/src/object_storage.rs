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
    OptimizedVfsStorage, VfsStorageCasPredicate, VfsStorageDeleteResult, VfsStorageDirListFilter,
    VfsStorageEntryKind, VfsStorageError, VfsStorageHardLinkResult, VfsStorageMetadata,
    VfsStorageMetadataFields, VfsStoragePrefetchOptions, VfsStoragePrefetchResult,
    VfsStorageReadIfChanged, VfsStorageReadIfChangedResult, VfsStorageReadRange,
    VfsStorageRenameResult, VfsStorageResult, VfsStorageSubtreeOptions, VfsStorageWrite,
    VfsStorageWritePrecondition, VfsStorageWriteResult,
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
const MAX_SMALL_FILE_CACHE_ENTRIES: usize = 4_096;
const MAX_SMALL_FILE_CACHE_TOTAL_BYTES: usize = 64 * 1024 * 1024;

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
    file_bytes: Mutex<SmallFileCache>,
}

#[derive(Default)]
struct SmallFileCache {
    entries: HashMap<String, CachedObjectFile>,
    sequence: u64,
    total_bytes: usize,
}

#[derive(Clone)]
struct CachedObjectFile {
    content_hash: String,
    bytes: Bytes,
    last_touched: u64,
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
                file_bytes: Mutex::new(SmallFileCache::default()),
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

    fn cached_file_bytes(&self, path: &str, content_hash: &str) -> Option<Bytes> {
        let mut cache = self.cache.file_bytes.lock().ok()?;
        cache.sequence = cache.sequence.saturating_add(1);
        let sequence = cache.sequence;
        let entry = cache.entries.get_mut(path)?;
        if entry.content_hash != content_hash {
            return None;
        }
        entry.last_touched = sequence;
        Some(entry.bytes.clone())
    }

    fn put_file_bytes_cache(&self, path: String, content_hash: String, bytes: Bytes) {
        if let Ok(mut cache) = self.cache.file_bytes.lock() {
            cache.sequence = cache.sequence.saturating_add(1);
            let sequence = cache.sequence;
            if let Some(previous) = cache.entries.remove(&path) {
                cache.total_bytes = cache.total_bytes.saturating_sub(previous.bytes.len());
            }
            cache.total_bytes = cache.total_bytes.saturating_add(bytes.len());
            cache.entries.insert(
                path,
                CachedObjectFile {
                    content_hash,
                    bytes,
                    last_touched: sequence,
                },
            );
            prune_small_file_cache(&mut cache);
        }
    }

    fn invalidate_file_bytes(&self, path: &str) {
        if let Ok(mut cache) = self.cache.file_bytes.lock() {
            if let Some(previous) = cache.entries.remove(path) {
                cache.total_bytes = cache.total_bytes.saturating_sub(previous.bytes.len());
            }
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

    async fn create_parent_directories_for_writes(
        &self,
        writes: &[VfsStorageWrite],
    ) -> VfsStorageResult<()> {
        let mut directories = HashMap::<String, (String, String)>::new();
        for write in writes {
            let (parent, _) = Self::path_parts(&write.path);
            let mut current = String::new();
            for segment in parent
                .trim_matches('/')
                .split('/')
                .filter(|part| !part.is_empty())
            {
                let ancestor = current.clone();
                if !current.is_empty() {
                    current.push('/');
                }
                current.push_str(segment);
                directories
                    .entry(current.clone())
                    .or_insert_with(|| (ancestor, segment.to_string()));
            }
        }
        let mut directories = directories.into_iter().collect::<Vec<_>>();
        directories.sort_unstable_by(|(left, _), (right, _)| {
            left.matches('/')
                .count()
                .cmp(&right.matches('/').count())
                .then_with(|| left.cmp(right))
        });
        for (path, (parent, name)) in directories {
            self.index
                .create_directory(&self.cfg.scope, &path, &parent, &name)
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
            .list_entries_with_manifest_in_subtree(&self.cfg.scope, prefix, options.limit)
            .await
            .map(|entries| {
                entries
                    .into_iter()
                    .map(VfsIndexEntryWithManifest::into_storage_metadata)
                    .collect()
            })
    }

    async fn read(&self, path: &str) -> VfsStorageResult<Bytes> {
        let Some(manifest) = self
            .index
            .get_current_file_manifest(&self.cfg.scope, path)
            .await?
        else {
            return Err(VfsStorageError::NotFound(path.to_string()));
        };
        if let Some(cached) = self.cached_file_bytes(path, &manifest.content_hash) {
            return Ok(cached);
        }
        let bytes = self.read_manifest_bytes(&manifest).await?;
        if bytes.len() <= self.cfg.small_file_cache_max_bytes {
            self.put_file_bytes_cache(path.to_string(), manifest.content_hash, bytes.clone());
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
        let manifests = self
            .index
            .list_current_file_manifests_by_paths(&self.cfg.scope, paths)
            .await?;
        let mut manifest_by_path = manifests
            .into_iter()
            .map(|manifest| (manifest.logical_path.clone(), manifest))
            .collect::<HashMap<_, _>>();
        for path in paths {
            if let Some(manifest) = manifest_by_path.get(path) {
                if let Some(bytes) = self.cached_file_bytes(path, &manifest.content_hash) {
                    results.push((path.clone(), bytes));
                    manifest_by_path.remove(path);
                }
            }
        }
        for path in manifest_by_path
            .iter()
            .filter_map(|(path, manifest)| {
                (manifest.pack_slot.pack_slot_length == 0).then_some(path.clone())
            })
            .collect::<Vec<_>>()
        {
            let content_hash = manifest_by_path
                .get(&path)
                .map(|manifest| manifest.content_hash.clone())
                .unwrap_or_default();
            self.put_file_bytes_cache(path.clone(), content_hash, Bytes::new());
            results.push((path.clone(), Bytes::new()));
            manifest_by_path.remove(&path);
        }

        let mut packs: HashMap<String, Vec<(String, String, i64, i64)>> = HashMap::new();
        for (path, manifest) in manifest_by_path {
            packs.entry(manifest.pack_slot.pack_key).or_default().push((
                path,
                manifest.content_hash,
                manifest.pack_slot.pack_slot_offset,
                manifest.pack_slot.pack_slot_length,
            ));
        }
        let pack_results = stream::iter(packs)
            .map(|(pack_key, slots)| async move {
                let range_start = slots
                    .iter()
                    .map(|(_, _, offset, _)| *offset as u64)
                    .min()
                    .unwrap_or(0);
                let range_end = slots
                    .iter()
                    .map(|(_, _, offset, length)| (*offset + *length) as u64)
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
            for (path, content_hash, slot_offset, slot_length) in slots {
                let offset_within_range = slot_offset as u64 - range_start;
                let extracted =
                    extract_slot(bytes.as_slice(), offset_within_range, slot_length as u64)?;
                let bytes = Bytes::from(extracted.bytes);
                if bytes.len() <= self.cfg.small_file_cache_max_bytes {
                    self.put_file_bytes_cache(path.clone(), content_hash, bytes.clone());
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
        let requested_paths = writes
            .iter()
            .map(|write| write.path.clone())
            .collect::<HashSet<_>>();
        self.create_parent_directories_for_writes(&writes).await?;
        let requested_previous = self
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
        let mut writes = writes;
        let explicit_by_identity = requested_previous
            .values()
            .filter(|entry| entry.entry.link_count > 1)
            .filter_map(|entry| {
                entry
                    .entry
                    .file_id
                    .as_ref()
                    .map(|file_id| (file_id.clone(), entry.entry.logical_path.clone()))
            })
            .collect::<HashMap<_, _>>();
        for (file_id, representative_path) in explicit_by_identity {
            let expected_file_ids = writes
                .iter()
                .filter(|write| {
                    requested_previous
                        .get(&write.path)
                        .and_then(|entry| entry.entry.file_id.as_deref())
                        == Some(file_id.as_str())
                })
                .filter_map(|write| {
                    write
                        .precondition
                        .as_ref()
                        .and_then(|precondition| precondition.expected_file_id.clone())
                })
                .collect::<HashSet<_>>();
            if expected_file_ids.len() > 1 {
                return Err(VfsStorageError::Conflict(format!(
                    "one atomic batch carries conflicting expected identities for hard-link identity {file_id}"
                )));
            }
            let expected_file_id = expected_file_ids.into_iter().next();
            let representative = writes
                .iter()
                .find(|write| write.path == representative_path)
                .cloned()
                .ok_or_else(|| {
                    VfsStorageError::Internal(format!(
                        "missing write for hard-link identity {file_id}"
                    ))
                })?;
            for alias in self
                .index
                .list_file_alias_paths(&self.cfg.scope, &file_id)
                .await?
            {
                if requested_paths.contains(&alias) {
                    let explicit = writes
                        .iter()
                        .find(|write| write.path == alias)
                        .expect("requested write exists");
                    if explicit.bytes != representative.bytes {
                        return Err(VfsStorageError::Conflict(format!(
                            "one atomic batch writes different content through hard-link aliases {representative_path} and {alias}"
                        )));
                    }
                } else {
                    let mut alias_write = representative.clone();
                    alias_write.path = alias;
                    alias_write.precondition = expected_file_id.as_ref().map(|expected_file_id| {
                        VfsStorageWritePrecondition {
                            predicate: None,
                            fingerprint: None,
                            secondary_fingerprint: None,
                            expected_file_id: Some(expected_file_id.clone()),
                        }
                    });
                    writes.push(alias_write);
                }
            }
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
        let identity_key = |write: &VfsStorageWrite| {
            previous
                .get(&write.path)
                .and_then(|entry| entry.entry.file_id.clone())
                .map(|file_id| format!("inode:{file_id}"))
                .unwrap_or_else(|| format!("path:{}", write.path))
        };
        let mut seen_content_identities = HashSet::new();
        let unique_writes = writes
            .iter()
            .filter(|write| seen_content_identities.insert(identity_key(write)))
            .collect::<Vec<_>>();
        let inputs = unique_writes
            .iter()
            .map(|write| VfsPackInput {
                logical_path: write.path.as_str(),
                bytes: &write.bytes,
                compression: SlotCompression::Zstd,
                token_count: write.token_count,
            })
            .collect::<Vec<_>>();
        let mut built = build_pack_manifest(pack_key, &inputs)?;
        let manifest_by_identity = unique_writes
            .iter()
            .zip(built.file_manifests.iter())
            .map(|(write, manifest)| (identity_key(write), manifest.clone()))
            .collect::<HashMap<_, _>>();
        let expanded_manifests = writes
            .iter()
            .map(|write| {
                let mut manifest = manifest_by_identity
                    .get(&identity_key(write))
                    .cloned()
                    .ok_or_else(|| {
                        VfsStorageError::Internal(format!(
                            "missing packed content for {}",
                            write.path
                        ))
                    })?;
                manifest.logical_path = write.path.clone();
                Ok(manifest)
            })
            .collect::<VfsStorageResult<Vec<_>>>()?;
        built.pack_record.reference_count = expanded_manifests.len() as i32;
        built.file_manifests = expanded_manifests;
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
                        file_id: previous
                            .get(&write.path)
                            .and_then(|entry| entry.entry.file_id.clone()),
                        expected_file_id: write
                            .precondition
                            .as_ref()
                            .and_then(|precondition| precondition.expected_file_id.clone()),
                        content_predicate: write
                            .precondition
                            .as_ref()
                            .and_then(VfsStorageWritePrecondition::effective_predicate)
                            .or_else(|| {
                                Some(
                                    match previous
                                        .get(&write.path)
                                        .and_then(|entry| entry.entry.content_hash.as_ref())
                                    {
                                        Some(fingerprint) => {
                                            VfsStorageCasPredicate::ContentFingerprint {
                                                fingerprint: fingerprint.clone(),
                                            }
                                        }
                                        None => VfsStorageCasPredicate::Absent,
                                    },
                                )
                            }),
                        expected_current_version: None,
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
            .filter(|result| requested_paths.contains(&result.path))
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
        let content_predicate = precondition
            .as_ref()
            .and_then(VfsStorageWritePrecondition::effective_predicate);
        let expected_file_id = precondition
            .as_ref()
            .and_then(|precondition| precondition.expected_file_id.as_deref());
        let previous = self
            .index
            .delete_file_entry_with_precondition(
                &self.cfg.scope,
                path,
                content_predicate.as_ref(),
                expected_file_id,
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

    async fn create_hard_link(
        &self,
        source: &str,
        destination: &str,
    ) -> VfsStorageResult<VfsStorageHardLinkResult> {
        let (parent, name) = Self::path_parts(destination);
        self.create_dir_all_metadata(&parent).await?;
        let result = self
            .index
            .create_hard_link_entry(&self.cfg.scope, source, destination, &parent, &name)
            .await?;
        self.invalidate_file_bytes(source);
        self.invalidate_file_bytes(destination);
        Ok(VfsStorageHardLinkResult {
            source: result.source.into_storage_metadata(),
            destination: result.destination.into_storage_metadata(),
        })
    }

    async fn find_hard_link_alias(
        &self,
        file_id: &str,
        excluding_path: &str,
    ) -> VfsStorageResult<Option<String>> {
        Ok(self
            .index
            .list_file_alias_paths(&self.cfg.scope, file_id)
            .await?
            .into_iter()
            .find(|path| path != excluding_path))
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
                self.put_file_bytes_cache(
                    manifest.logical_path.clone(),
                    manifest.content_hash,
                    bytes.clone(),
                );
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

fn prune_small_file_cache(cache: &mut SmallFileCache) {
    if cache.entries.len() <= MAX_SMALL_FILE_CACHE_ENTRIES
        && cache.total_bytes <= MAX_SMALL_FILE_CACHE_TOTAL_BYTES
    {
        return;
    }
    let target_entries = MAX_SMALL_FILE_CACHE_ENTRIES.saturating_mul(3) / 4;
    let target_bytes = MAX_SMALL_FILE_CACHE_TOTAL_BYTES.saturating_mul(3) / 4;
    let mut oldest = cache
        .entries
        .iter()
        .map(|(path, entry)| (path.clone(), entry.last_touched))
        .collect::<Vec<_>>();
    oldest.sort_unstable_by_key(|(_, last_touched)| *last_touched);
    for (path, _) in oldest {
        if cache.entries.len() <= target_entries && cache.total_bytes <= target_bytes {
            break;
        }
        if let Some(removed) = cache.entries.remove(&path) {
            cache.total_bytes = cache.total_bytes.saturating_sub(removed.bytes.len());
        }
    }
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use async_trait::async_trait;
    use chrono::Utc;

    use crate::{
        index::{VfsIndexEntry, VfsIndexEntryWithManifest, VfsPackedCommitResult},
        object_store::LocalObjectStoreClient,
    };

    #[derive(Default)]
    struct MemoryIndex {
        inner: Mutex<MemoryIndexInner>,
        alias_list_calls: AtomicUsize,
        create_directory_calls: AtomicUsize,
        entries_by_paths_calls: AtomicUsize,
        manifest_by_paths_calls: AtomicUsize,
        packed_commit_calls: AtomicUsize,
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
            self.manifest_by_paths_calls.fetch_add(1, Ordering::Relaxed);
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
            self.entries_by_paths_calls.fetch_add(1, Ordering::Relaxed);
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
            self.packed_commit_calls.fetch_add(1, Ordering::Relaxed);
            let mut guard = self.inner.lock().unwrap();
            for file in &commit.files {
                let actual_file_id = guard
                    .entries
                    .get(&file.logical_path)
                    .and_then(|entry| entry.entry.file_id.as_deref());
                if file.expected_file_id.as_deref().is_some()
                    && actual_file_id != file.expected_file_id.as_deref()
                {
                    return Err(VfsStorageError::Conflict(format!(
                        "identity conflict for {}",
                        file.logical_path
                    )));
                }
                let current = guard.entries.get(&file.logical_path);
                let content_matches = match file.content_predicate.as_ref() {
                    None => true,
                    Some(VfsStorageCasPredicate::Absent) => current.is_none(),
                    Some(VfsStorageCasPredicate::ContentFingerprint { fingerprint }) => {
                        current.and_then(|entry| entry.entry.content_hash.as_deref())
                            == Some(fingerprint.as_str())
                    }
                };
                let version_matches =
                    file.expected_current_version
                        .as_deref()
                        .is_none_or(|expected| {
                            current.and_then(|entry| entry.entry.current_version.as_deref())
                                == Some(expected)
                        });
                if !content_matches || !version_matches {
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
                // The in-memory test index already stores the link count on the
                // existing namespace entry. Recounting every identity by scanning
                // the complete map made this test double O(N²), masking the
                // production batch implementation's actual scaling at 10k files.
                let link_count = guard
                    .entries
                    .get(&file.logical_path)
                    .map(|entry| entry.entry.link_count)
                    .unwrap_or(1);
                let file_id = file.file_id.or_else(|| Some(Uuid::new_v4().to_string()));
                guard.entries.insert(
                    file.logical_path.clone(),
                    VfsIndexEntryWithManifest {
                        entry: VfsIndexEntry {
                            logical_path: file.logical_path.clone(),
                            parent_logical_path: file.parent_logical_path,
                            entry_name: file.entry_name,
                            kind: VfsStorageEntryKind::File,
                            file_id,
                            link_count,
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
            self.create_directory_calls.fetch_add(1, Ordering::Relaxed);
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
                        file_id: None,
                        link_count: 1,
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

        async fn create_hard_link_entry(
            &self,
            _scope: &VfsIndexScope,
            source_logical_path: &str,
            destination_logical_path: &str,
            destination_parent_logical_path: &str,
            destination_entry_name: &str,
        ) -> VfsStorageResult<crate::index::VfsIndexHardLinkResult> {
            let mut guard = self.inner.lock().unwrap();
            if guard.entries.contains_key(destination_logical_path) {
                return Err(VfsStorageError::Conflict(format!(
                    "vfs hard-link destination already exists: {destination_logical_path}"
                )));
            }
            let mut source = guard
                .entries
                .get(source_logical_path)
                .cloned()
                .ok_or_else(|| VfsStorageError::NotFound(source_logical_path.to_string()))?;
            if source.entry.kind != VfsStorageEntryKind::File {
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs hard-link source {source_logical_path} is not a file"
                )));
            }
            let file_id = source
                .entry
                .file_id
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            let link_count = guard
                .entries
                .values()
                .filter(|entry| entry.entry.file_id.as_deref() == Some(file_id.as_str()))
                .count() as u64
                + 1;
            for entry in guard.entries.values_mut() {
                if entry.entry.logical_path == source_logical_path
                    || entry.entry.file_id.as_deref() == Some(file_id.as_str())
                {
                    entry.entry.file_id = Some(file_id.clone());
                    entry.entry.link_count = link_count;
                }
            }
            source = guard.entries[source_logical_path].clone();
            let mut destination = source.clone();
            destination.entry.logical_path = destination_logical_path.to_string();
            destination.entry.parent_logical_path = destination_parent_logical_path.to_string();
            destination.entry.entry_name = destination_entry_name.to_string();
            destination.entry.link_count = link_count;
            if let Some(manifest) = destination.manifest.as_mut() {
                manifest.logical_path = destination_logical_path.to_string();
            }
            guard
                .entries
                .insert(destination_logical_path.to_string(), destination.clone());
            Ok(crate::index::VfsIndexHardLinkResult {
                source,
                destination,
            })
        }

        async fn list_file_alias_paths(
            &self,
            _scope: &VfsIndexScope,
            file_id: &str,
        ) -> VfsStorageResult<Vec<String>> {
            self.alias_list_calls.fetch_add(1, Ordering::Relaxed);
            let guard = self.inner.lock().unwrap();
            let mut paths = guard
                .entries
                .values()
                .filter(|entry| entry.entry.file_id.as_deref() == Some(file_id))
                .map(|entry| entry.entry.logical_path.clone())
                .collect::<Vec<_>>();
            paths.sort();
            Ok(paths)
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
                Some(_) => {
                    let removed = guard.entries.remove(logical_path);
                    if let Some(file_id) = removed.as_ref().and_then(|entry| {
                        (entry.entry.link_count > 1)
                            .then(|| entry.entry.file_id.clone())
                            .flatten()
                    }) {
                        let link_count = guard
                            .entries
                            .values()
                            .filter(|entry| {
                                entry.entry.file_id.as_deref() == Some(file_id.as_str())
                            })
                            .count() as u64;
                        for entry in guard.entries.values_mut() {
                            if entry.entry.file_id.as_deref() == Some(file_id.as_str()) {
                                entry.entry.link_count = link_count;
                            }
                        }
                    }
                    Ok(removed)
                }
            }
        }

        async fn delete_file_entry_with_precondition(
            &self,
            _scope: &VfsIndexScope,
            logical_path: &str,
            content_predicate: Option<&VfsStorageCasPredicate>,
            expected_file_id: Option<&str>,
        ) -> VfsStorageResult<Option<VfsIndexEntryWithManifest>> {
            let mut guard = self.inner.lock().unwrap();
            let current = guard.entries.get(logical_path);
            if current.is_some_and(|entry| entry.entry.kind == VfsStorageEntryKind::Directory) {
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs path {logical_path} is not a file"
                )));
            }
            if expected_file_id.is_some()
                && current.and_then(|entry| entry.entry.file_id.as_deref()) != expected_file_id
            {
                return Err(VfsStorageError::Conflict(format!(
                    "identity conflict for {logical_path}"
                )));
            }
            let content_matches = match content_predicate {
                None => true,
                Some(VfsStorageCasPredicate::Absent) => current.is_none(),
                Some(VfsStorageCasPredicate::ContentFingerprint { fingerprint }) => {
                    current.and_then(|entry| entry.entry.content_hash.as_deref())
                        == Some(fingerprint.as_str())
                }
            };
            if !content_matches {
                return Err(VfsStorageError::Conflict(format!(
                    "content conflict for {logical_path}"
                )));
            }
            let removed = guard.entries.remove(logical_path);
            if let Some(file_id) = removed.as_ref().and_then(|entry| {
                (entry.entry.link_count > 1)
                    .then(|| entry.entry.file_id.clone())
                    .flatten()
            }) {
                let link_count = guard
                    .entries
                    .values()
                    .filter(|entry| entry.entry.file_id.as_deref() == Some(file_id.as_str()))
                    .count() as u64;
                for entry in guard.entries.values_mut() {
                    if entry.entry.file_id.as_deref() == Some(file_id.as_str()) {
                        entry.entry.link_count = link_count;
                    }
                }
            }
            Ok(removed)
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
            if from_logical_path == to_logical_path {
                let entry = guard
                    .entries
                    .get(from_logical_path)
                    .cloned()
                    .ok_or_else(|| VfsStorageError::NotFound(from_logical_path.to_string()))?;
                return Ok((entry.clone(), entry));
            }
            let previous = guard
                .entries
                .get(from_logical_path)
                .cloned()
                .ok_or_else(|| VfsStorageError::NotFound(from_logical_path.to_string()))?;
            if previous.entry.kind != VfsStorageEntryKind::File {
                return Err(VfsStorageError::BadRequest(format!(
                    "vfs path {from_logical_path} is not a file"
                )));
            }
            if let Some(destination) = guard.entries.get(to_logical_path).cloned() {
                if destination.entry.kind != VfsStorageEntryKind::File {
                    return Err(VfsStorageError::Conflict(format!(
                        "vfs cannot replace non-file destination: {to_logical_path}"
                    )));
                }
                // POSIX rename(2) is a successful no-op when both pathnames
                // already name the same inode. Deleting the destination first
                // would incorrectly collapse two hard links into one.
                if previous.entry.file_id.is_some()
                    && previous.entry.file_id == destination.entry.file_id
                {
                    return Ok((previous, destination));
                }
            }
            let replaced_identity = guard.entries.remove(to_logical_path).and_then(|entry| {
                (entry.entry.link_count > 1)
                    .then_some(entry.entry.file_id)
                    .flatten()
            });
            if let Some(file_id) = replaced_identity {
                let count = guard
                    .entries
                    .values()
                    .filter(|entry| entry.entry.file_id.as_deref() == Some(file_id.as_str()))
                    .count() as u64;
                for entry in guard.entries.values_mut() {
                    if entry.entry.file_id.as_deref() == Some(file_id.as_str()) {
                        entry.entry.link_count = count;
                    }
                }
            }
            guard.entries.remove(from_logical_path);
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

    fn object_storage_clients() -> (
        ObjectBackedVfsStorage,
        ObjectBackedVfsStorage,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(LocalObjectStoreClient::new(dir.path().to_path_buf()).unwrap());
        let index = Arc::new(MemoryIndex::default());
        let cfg = ObjectBackedVfsStorageConfig::new(VfsIndexScope::new("shared-test-scope"));
        (
            ObjectBackedVfsStorage::new(cfg.clone(), store.clone(), index.clone()),
            ObjectBackedVfsStorage::new(cfg, store, index),
            dir,
        )
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
    async fn git_metadata_batch_avoids_per_file_directory_and_alias_queries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(LocalObjectStoreClient::new(dir.path().to_path_buf()).unwrap());
        let index = Arc::new(MemoryIndex::default());
        let storage = ObjectBackedVfsStorage::new(
            ObjectBackedVfsStorageConfig::new(VfsIndexScope::new("git-metadata-benchmark")),
            store,
            index.clone(),
        );
        let writes = (0..1_000)
            .map(|index| VfsStorageWrite {
                path: format!(".git/objects/ab/{index:04x}"),
                bytes: Bytes::from(format!("object-{index:04}\n")),
                token_count: None,
                precondition: None,
            })
            .collect::<Vec<_>>();

        let initial_started = Instant::now();
        storage
            .write_many_atomic(writes.clone())
            .await
            .expect("initial Git metadata batch");
        let initial_elapsed = initial_started.elapsed();
        assert_eq!(index.create_directory_calls.load(Ordering::Relaxed), 3);
        assert_eq!(index.alias_list_calls.load(Ordering::Relaxed), 0);

        let rewrite_started = Instant::now();
        storage
            .write_many_atomic(writes)
            .await
            .expect("Git metadata rewrite batch");
        let rewrite_elapsed = rewrite_started.elapsed();
        assert_eq!(index.create_directory_calls.load(Ordering::Relaxed), 6);
        assert_eq!(
            index.alias_list_calls.load(Ordering::Relaxed),
            0,
            "ordinary files must not issue one hard-link alias query per entry",
        );
        eprintln!(
            "git metadata object benchmark: initial={initial_elapsed:?} rewrite={rewrite_elapsed:?}"
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

    /// Explicit object/index/cache performance torture. It uses the in-memory
    /// index and filesystem object-store adapters so the exact operation counts,
    /// cache ceilings, and cross-client invalidation remain deterministic.
    #[tokio::test]
    #[ignore = "explicit 1k/10k Git small-file performance suite"]
    async fn object_git_small_file_perf_1k_10k() {
        let mut scale_samples = Vec::new();
        for count in [1_000_usize, 10_000] {
            let dir = tempfile::tempdir().expect("tempdir");
            let store = Arc::new(LocalObjectStoreClient::new(dir.path().to_path_buf()).unwrap());
            let index = Arc::new(MemoryIndex::default());
            let cfg =
                ObjectBackedVfsStorageConfig::new(VfsIndexScope::new(format!("git-perf-{count}")));
            let first = ObjectBackedVfsStorage::new(cfg.clone(), store.clone(), index.clone());
            let second = ObjectBackedVfsStorage::new(cfg, store, index.clone());
            let initial = git_perf_writes(count, 0);
            let paths = initial
                .iter()
                .map(|write| write.path.clone())
                .collect::<Vec<_>>();
            let mutation_count = (count / 100).max(1);

            let create_started = Instant::now();
            first
                .write_many_atomic(initial)
                .await
                .expect("create object-backed Git-shaped file set");
            let create_elapsed = create_started.elapsed();

            let status_started = Instant::now();
            let status = first
                .metadata_many(&paths, VfsStorageMetadataFields::default())
                .await
                .expect("object-backed status-like metadata batch");
            let status_elapsed = status_started.elapsed();
            assert_eq!(status.iter().filter(|entry| entry.is_some()).count(), count);

            let warm_started = Instant::now();
            let warmed = first
                .read_many(&paths)
                .await
                .expect("warm object-backed small files");
            let warm_elapsed = warm_started.elapsed();
            assert_eq!(warmed.len(), count);

            let targeted = git_perf_writes(count, 1)
                .into_iter()
                .filter(|write| {
                    write.path.starts_with(".git/refs/") || write.path.starts_with("src/generated/")
                })
                .collect::<Vec<_>>();
            let rewrite_started = Instant::now();
            second
                .write_many_atomic(targeted)
                .await
                .expect("targeted object-backed rewrite");
            let rewrite_elapsed = rewrite_started.elapsed();

            let namespace_started = Instant::now();
            for index in 0..mutation_count {
                second
                    .rename_with_metadata(
                        &format!(".git/refs/heads/perf-{index:05}.lock"),
                        &format!(".git/refs/heads/perf-{index:05}"),
                    )
                    .await
                    .expect("promote object-backed ref");
                second
                    .delete_file_with_metadata(&format!("src/generated/perf-{index:05}.ts"), None)
                    .await
                    .expect("delete object-backed worktree file");
            }
            let namespace_elapsed = namespace_started.elapsed();

            let survivor_index = count.saturating_sub(mutation_count * 2 + 1);
            let survivor = format!(
                ".git/objects/{:02x}/{:038x}",
                survivor_index % 256,
                survivor_index
            );
            let _ = first
                .read(&survivor)
                .await
                .expect("prime first-client cache");
            let replacement = Bytes::from_static(b"cross-client replacement with distinct size\n");
            second
                .write(&survivor, replacement.clone(), None)
                .await
                .expect("second-client object replacement");
            assert_eq!(
                first
                    .read(&survivor)
                    .await
                    .expect("first-client object refresh"),
                replacement,
                "manifest hash changes must invalidate a stale per-client byte entry",
            );

            for (name, storage) in [("first", &first), ("second", &second)] {
                let cache = storage.cache.file_bytes.lock().unwrap();
                assert!(
                    cache.entries.len() <= MAX_SMALL_FILE_CACHE_ENTRIES,
                    "{name} small-file cache exceeded its entry ceiling",
                );
                assert!(
                    cache.total_bytes <= MAX_SMALL_FILE_CACHE_TOTAL_BYTES,
                    "{name} small-file cache exceeded its byte ceiling",
                );
            }
            assert_eq!(
                index.alias_list_calls.load(Ordering::Relaxed),
                0,
                "ordinary Git files must not trigger hard-link alias scans",
            );
            assert!(
                index.create_directory_calls.load(Ordering::Relaxed)
                    <= 270 + mutation_count.saturating_mul(3),
                "parent index work must scale with unique directories, not file count",
            );
            assert_eq!(
                index.packed_commit_calls.load(Ordering::Relaxed),
                3,
                "initial, targeted, and cross-client writes should each be one packed commit",
            );
            assert_eq!(
                index.manifest_by_paths_calls.load(Ordering::Relaxed),
                1,
                "one read-many call should issue one bulk manifest query",
            );
            assert!(
                index.entries_by_paths_calls.load(Ordering::Relaxed) <= 7 + mutation_count,
                "bulk metadata/write paths unexpectedly degraded toward per-file index queries",
            );

            let first_cache = first.cache.file_bytes.lock().unwrap();
            let total = create_elapsed
                + status_elapsed
                + warm_elapsed
                + rewrite_elapsed
                + namespace_elapsed;
            scale_samples.push((count, total));
            eprintln!(
                "git-small-file-perf backend=object files={count} create={create_elapsed:?} \
                 status={status_elapsed:?} warm_reads={warm_elapsed:?} \
                 rewrite_2pct={rewrite_elapsed:?} namespace_2pct={namespace_elapsed:?} \
                 total={total:?} alias_queries={} parent_calls={} bulk_entry_queries={} \
                 cache_entries={} cache_bytes={}",
                index.alias_list_calls.load(Ordering::Relaxed),
                index.create_directory_calls.load(Ordering::Relaxed),
                index.entries_by_paths_calls.load(Ordering::Relaxed),
                first_cache.entries.len(),
                first_cache.total_bytes,
            );
        }
        let one_k = scale_samples[0].1.as_secs_f64();
        let ten_k = scale_samples[1].1.as_secs_f64();
        assert!(
            ten_k <= one_k * 35.0,
            "10x object workload regressed toward quadratic scaling: 1k={one_k:.3}s 10k={ten_k:.3}s",
        );
    }

    #[test]
    fn object_small_file_cache_is_bounded() {
        let (storage, _dir) = object_storage();
        for index in 0..(MAX_SMALL_FILE_CACHE_ENTRIES + 1_000) {
            storage.put_file_bytes_cache(
                format!(".git/cache/{index}"),
                format!("hash-{index}"),
                Bytes::from_static(b"x"),
            );
        }
        let cache = storage.cache.file_bytes.lock().unwrap();
        assert!(cache.entries.len() <= MAX_SMALL_FILE_CACHE_ENTRIES);
        assert!(cache.total_bytes <= MAX_SMALL_FILE_CACHE_TOTAL_BYTES);
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
    async fn object_storage_hard_links_share_identity_and_alias_writes() {
        let (storage, _dir) = object_storage();
        storage
            .write("source", Bytes::from_static(b"one"), None)
            .await
            .expect("source");
        let linked = storage
            .create_hard_link("source", "nested/alias")
            .await
            .expect("hard link");
        assert_eq!(linked.source.file_id, linked.destination.file_id);
        assert_eq!(linked.source.link_count, 2);
        assert_eq!(linked.destination.link_count, 2);
        assert_eq!(
            linked.source.object_state.as_ref().unwrap().pack_key,
            linked.destination.object_state.as_ref().unwrap().pack_key
        );
        assert_eq!(
            linked
                .source
                .object_state
                .as_ref()
                .unwrap()
                .pack_slot_offset,
            linked
                .destination
                .object_state
                .as_ref()
                .unwrap()
                .pack_slot_offset
        );

        storage
            .write("nested/alias", Bytes::from_static(b"two"), None)
            .await
            .expect("write alias");
        assert_eq!(storage.read("source").await.unwrap().as_ref(), b"two");
        assert_eq!(storage.read("nested/alias").await.unwrap().as_ref(), b"two");
        let source = storage.stat("source").await.unwrap().unwrap();
        let alias = storage.stat("nested/alias").await.unwrap().unwrap();
        assert_eq!(source.file_id, alias.file_id);
        assert_eq!(source.content_hash, alias.content_hash);
        assert_eq!(source.link_count, 2);

        storage
            .delete_file_with_metadata("source", None)
            .await
            .expect("unlink source");
        let remaining = storage.stat("nested/alias").await.unwrap().unwrap();
        assert_eq!(remaining.link_count, 1);
        assert_eq!(
            storage
                .find_hard_link_alias(remaining.file_id.as_deref().unwrap(), "nested/alias")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn object_storage_independent_clients_observe_linked_inode_mutations() {
        let (first, second, _dir) = object_storage_clients();
        first
            .write("source", Bytes::from_static(b"one"), None)
            .await
            .expect("initial write");
        assert_eq!(first.read("source").await.unwrap().as_ref(), b"one");

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
            .delete_file_with_metadata("source", None)
            .await
            .expect("first client unlinks one alias");
        assert!(second.stat("source").await.unwrap().is_none());
        let remaining = second.stat("alias").await.unwrap().unwrap();
        assert_eq!(remaining.file_id.as_deref(), Some(file_id.as_str()));
        assert_eq!(remaining.link_count, 1);
        assert_eq!(second.read("alias").await.unwrap().as_ref(), b"two");
    }

    #[tokio::test]
    async fn object_storage_replacement_rename_is_immediately_visible() {
        let (storage, _dir) = object_storage();
        storage
            .write("source", Bytes::from_static(b"new"), None)
            .await
            .unwrap();
        storage
            .write("destination", Bytes::from_static(b"old"), None)
            .await
            .unwrap();
        let source_id = storage.stat("source").await.unwrap().unwrap().file_id;

        storage
            .rename_with_metadata("source", "destination")
            .await
            .expect("replacement rename");
        assert!(storage.stat("source").await.unwrap().is_none());
        let destination = storage.stat("destination").await.unwrap().unwrap();
        assert_eq!(destination.file_id, source_id);
        assert_eq!(storage.read("destination").await.unwrap().as_ref(), b"new");
    }

    #[tokio::test]
    async fn object_storage_rename_between_aliases_of_one_inode_is_a_noop() {
        let (storage, _dir) = object_storage();
        storage
            .write("source", Bytes::from_static(b"body"), None)
            .await
            .unwrap();
        let linked = storage.create_hard_link("source", "alias").await.unwrap();
        let file_id = linked.source.file_id.expect("stable file identity");

        storage
            .rename_with_metadata("source", "alias")
            .await
            .expect("same-inode rename");

        for path in ["source", "alias"] {
            let metadata = storage.stat(path).await.unwrap().expect(path);
            assert_eq!(metadata.file_id.as_deref(), Some(file_id.as_str()));
            assert_eq!(metadata.link_count, 2);
            assert_eq!(storage.read(path).await.unwrap().as_ref(), b"body");
        }
    }

    #[tokio::test]
    async fn object_storage_rejects_conflicting_alias_writes_in_one_batch() {
        let (storage, _dir) = object_storage();
        storage
            .write("source", Bytes::from_static(b"base"), None)
            .await
            .unwrap();
        storage.create_hard_link("source", "alias").await.unwrap();
        let error = storage
            .write_many_atomic(vec![
                VfsStorageWrite {
                    path: "source".to_string(),
                    bytes: Bytes::from_static(b"left"),
                    token_count: None,
                    precondition: None,
                },
                VfsStorageWrite {
                    path: "alias".to_string(),
                    bytes: Bytes::from_static(b"right"),
                    token_count: None,
                    precondition: None,
                },
            ])
            .await
            .expect_err("conflicting aliases rejected");
        assert!(matches!(error, VfsStorageError::Conflict(_)));
        assert_eq!(storage.read("source").await.unwrap().as_ref(), b"base");
        assert_eq!(storage.read("alias").await.unwrap().as_ref(), b"base");
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
        storage
            .write("guarded.md", Bytes::from_static(b"second"), None)
            .await
            .expect("racing write");
        let err = storage
            .write(
                "guarded.md",
                Bytes::from_static(b"third"),
                Some(VfsStorageWritePrecondition::content_fingerprint(
                    first.content_hash.clone(),
                )),
            )
            .await
            .expect_err("stale precondition");
        assert_eq!(first.content_hash, hex_hash(b"first"));
        assert!(matches!(err, VfsStorageError::Conflict(_)));

        storage
            .write(
                "created.md",
                Bytes::from_static(b"created"),
                Some(VfsStorageWritePrecondition::absent()),
            )
            .await
            .expect("typed absence allows creation");
        let error = storage
            .write(
                "created.md",
                Bytes::from_static(b"must-not-replace"),
                Some(VfsStorageWritePrecondition::absent()),
            )
            .await
            .expect_err("typed absence rejects an existing path");
        assert!(matches!(error, VfsStorageError::Conflict(_)));
        assert_eq!(&storage.read("created.md").await.unwrap()[..], b"created");
    }

    #[tokio::test]
    async fn object_storage_preserves_and_enforces_expected_file_identity() {
        let (storage, _dir) = object_storage();
        storage
            .write("guarded.md", Bytes::from_static(b"first"), None)
            .await
            .expect("initial");
        let initial = storage
            .stat("guarded.md")
            .await
            .expect("stat")
            .expect("metadata");
        let file_id = initial.file_id.clone().expect("stable file identity");
        let mut precondition = VfsStorageWritePrecondition::content_fingerprint(
            initial.content_hash.clone().expect("content fingerprint"),
        );
        precondition.expected_file_id = Some(file_id.clone());
        storage
            .write(
                "guarded.md",
                Bytes::from_static(b"second"),
                Some(precondition),
            )
            .await
            .expect("matching content and identity");

        let current = storage
            .stat("guarded.md")
            .await
            .expect("stat")
            .expect("metadata");
        assert_eq!(current.file_id.as_deref(), Some(file_id.as_str()));
        let mut stale_identity = VfsStorageWritePrecondition::content_fingerprint(
            current.content_hash.clone().expect("content fingerprint"),
        );
        stale_identity.expected_file_id = Some("different-file-id".to_string());
        let error = storage
            .write(
                "guarded.md",
                Bytes::from_static(b"must-not-land"),
                Some(stale_identity),
            )
            .await
            .expect_err("identity mismatch");
        assert!(matches!(error, VfsStorageError::Conflict(_)));
        assert_eq!(&storage.read("guarded.md").await.unwrap()[..], b"second");

        let mut stale_delete = VfsStorageWritePrecondition::content_fingerprint(
            current.content_hash.expect("content fingerprint"),
        );
        stale_delete.expected_file_id = Some("different-file-id".to_string());
        let error = storage
            .delete_file_with_metadata("guarded.md", Some(stale_delete))
            .await
            .expect_err("delete identity mismatch");
        assert!(matches!(error, VfsStorageError::Conflict(_)));
        assert_eq!(&storage.read("guarded.md").await.unwrap()[..], b"second");
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
                Some(VfsStorageWritePrecondition::content_fingerprint(
                    "stale-content",
                )),
            )
            .await
            .expect_err("stale delete precondition");
        assert!(matches!(err, VfsStorageError::Conflict(_)));
        assert_eq!(&storage.read("notes/a.md").await.unwrap()[..], b"alpha");
    }
}
