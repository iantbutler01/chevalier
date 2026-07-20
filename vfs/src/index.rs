// @dive-file: Generic manifest/index boundary for packed VFS storage.
// @dive-rel: Lets product databases, local sidecars, or gateway services provide the logical
// @dive-rel: path -> current manifest/pack-slot index without making DB access a VFS primitive.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    VfsStorageCasPredicate, VfsStorageDirListFilter, VfsStorageEntryKind, VfsStorageError,
    VfsStorageMetadata, VfsStorageObjectState, VfsStorageResult,
    manifest::{VfsFileManifest, VfsPackRecord},
};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct VfsIndexScope {
    pub key: String,
}

impl VfsIndexScope {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsIndexEntry {
    pub logical_path: String,
    pub parent_logical_path: String,
    pub entry_name: String,
    pub kind: VfsStorageEntryKind,
    /// Stable regular-file identity shared by every hard-link alias.
    #[serde(default)]
    pub file_id: Option<String>,
    /// Current number of namespace entries for `file_id`.
    #[serde(default = "default_index_link_count")]
    pub link_count: u64,
    pub size_bytes: i64,
    pub content_hash: Option<String>,
    pub current_version: Option<String>,
    pub updated_at: Option<DateTime<Utc>>,
}

const fn default_index_link_count() -> u64 {
    1
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsIndexEntryWithManifest {
    pub entry: VfsIndexEntry,
    pub manifest: Option<VfsFileManifest>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsPackedFileCommit {
    pub logical_path: String,
    pub parent_logical_path: String,
    pub entry_name: String,
    pub manifest: VfsFileManifest,
    /// Preserve this identity when replacing content through any hard-link alias.
    #[serde(default)]
    pub file_id: Option<String>,
    /// Stable identity that must still own `logical_path` at commit time.
    /// This is checked independently from the content-version predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_file_id: Option<String>,
    /// Content predicate captured by the caller. Unlike
    /// `expected_current_version`, this is a content-domain CAS value and must
    /// be checked transactionally against the current entry content hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_predicate: Option<VfsStorageCasPredicate>,
    /// Deprecated manifest-version CAS retained only for rolling index
    /// implementations. New object-backed writes leave this unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_current_version: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsPackedCommit {
    pub pack: VfsPackRecord,
    pub files: Vec<VfsPackedFileCommit>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsPackedCommitResult {
    pub committed_paths: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsIndexHardLinkResult {
    pub source: VfsIndexEntryWithManifest,
    pub destination: VfsIndexEntryWithManifest,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsFileManifestRecord {
    pub id: String,
    pub scope: VfsIndexScope,
    pub manifest: VfsFileManifest,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsPackRecordWithScope {
    pub scope: VfsIndexScope,
    pub pack: VfsPackRecord,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsManifestRepoint {
    pub manifest_id: String,
    pub new_pack_key: String,
    pub new_pack_slot_offset: i64,
    pub new_pack_slot_length: i64,
    pub new_pack_slot_compression: i16,
}

#[async_trait]
pub trait VfsManifestIndex: Send + Sync {
    async fn get_current_file_manifest(
        &self,
        scope: &VfsIndexScope,
        logical_path: &str,
    ) -> VfsStorageResult<Option<VfsFileManifest>>;

    async fn list_current_file_manifests_by_paths(
        &self,
        scope: &VfsIndexScope,
        logical_paths: &[String],
    ) -> VfsStorageResult<Vec<VfsFileManifest>>;

    async fn list_current_file_manifests_in_subtree(
        &self,
        scope: &VfsIndexScope,
        logical_path_prefix: &str,
        limit: Option<i64>,
    ) -> VfsStorageResult<Vec<VfsFileManifest>>;

    async fn get_entry_with_manifest(
        &self,
        scope: &VfsIndexScope,
        logical_path: &str,
    ) -> VfsStorageResult<Option<VfsIndexEntryWithManifest>>;

    async fn list_entries_with_manifest_by_paths(
        &self,
        scope: &VfsIndexScope,
        logical_paths: &[String],
    ) -> VfsStorageResult<Vec<VfsIndexEntryWithManifest>>;

    async fn list_entries_with_manifest_in_subtree(
        &self,
        scope: &VfsIndexScope,
        logical_path_prefix: &str,
        limit: Option<i64>,
    ) -> VfsStorageResult<Vec<VfsIndexEntryWithManifest>> {
        let manifests = self
            .list_current_file_manifests_in_subtree(scope, logical_path_prefix, limit)
            .await?;
        Ok(manifests
            .into_iter()
            .map(|manifest| {
                let logical_path = manifest.logical_path.clone();
                let (parent_logical_path, entry_name) = logical_path
                    .rsplit_once('/')
                    .map(|(parent, name)| (parent.to_string(), name.to_string()))
                    .unwrap_or_else(|| (String::new(), logical_path.clone()));
                VfsIndexEntryWithManifest {
                    entry: VfsIndexEntry {
                        logical_path,
                        parent_logical_path,
                        entry_name,
                        kind: VfsStorageEntryKind::File,
                        file_id: None,
                        link_count: 1,
                        size_bytes: manifest.logical_size_bytes,
                        content_hash: Some(manifest.content_hash.clone()),
                        current_version: None,
                        updated_at: None,
                    },
                    manifest: Some(manifest),
                }
            })
            .collect())
    }

    async fn list_dir_with_manifest_attrs(
        &self,
        scope: &VfsIndexScope,
        parent_logical_path: &str,
        filter: VfsStorageDirListFilter,
    ) -> VfsStorageResult<Vec<VfsIndexEntryWithManifest>>;

    async fn commit_packed_files(
        &self,
        scope: &VfsIndexScope,
        commit: VfsPackedCommit,
    ) -> VfsStorageResult<VfsPackedCommitResult>;

    async fn create_directory(
        &self,
        scope: &VfsIndexScope,
        logical_path: &str,
        parent_logical_path: &str,
        entry_name: &str,
    ) -> VfsStorageResult<()>;

    /// Atomically add a pathname to the source's stable file identity.
    async fn create_hard_link_entry(
        &self,
        _scope: &VfsIndexScope,
        _source_logical_path: &str,
        _destination_logical_path: &str,
        _destination_parent_logical_path: &str,
        _destination_entry_name: &str,
    ) -> VfsStorageResult<VfsIndexHardLinkResult> {
        Err(crate::VfsStorageError::BadRequest(
            "hard links are not supported by this VFS manifest index".to_string(),
        ))
    }

    /// Return every live pathname for one stable file identity.
    async fn list_file_alias_paths(
        &self,
        _scope: &VfsIndexScope,
        _file_id: &str,
    ) -> VfsStorageResult<Vec<String>> {
        Err(crate::VfsStorageError::BadRequest(
            "hard-link alias lookup is not supported by this VFS manifest index".to_string(),
        ))
    }

    async fn delete_file_entry(
        &self,
        scope: &VfsIndexScope,
        logical_path: &str,
        expected_current_version: Option<&str>,
    ) -> VfsStorageResult<Option<VfsIndexEntryWithManifest>>;

    /// Delete with content-domain and stable-identity predicates. Indexes must
    /// validate both predicates in the same transaction as the delete.
    async fn delete_file_entry_with_precondition(
        &self,
        scope: &VfsIndexScope,
        logical_path: &str,
        content_predicate: Option<&VfsStorageCasPredicate>,
        expected_file_id: Option<&str>,
    ) -> VfsStorageResult<Option<VfsIndexEntryWithManifest>> {
        if content_predicate.is_some() || expected_file_id.is_some() {
            return Err(VfsStorageError::BadRequest(
                "manifest index does not support typed delete preconditions".to_string(),
            ));
        }
        self.delete_file_entry(scope, logical_path, None).await
    }

    async fn remove_empty_directory(
        &self,
        scope: &VfsIndexScope,
        logical_path: &str,
    ) -> VfsStorageResult<()>;

    async fn rename_file_entry(
        &self,
        scope: &VfsIndexScope,
        from_logical_path: &str,
        to_logical_path: &str,
        to_parent_logical_path: &str,
        to_entry_name: &str,
    ) -> VfsStorageResult<(VfsIndexEntryWithManifest, VfsIndexEntryWithManifest)>;
}

#[async_trait]
pub trait VfsPackLifecycleIndex: Send + Sync {
    async fn list_scopes_with_small_packs(
        &self,
        max_total_bytes: i64,
        max_total_slots: i32,
        min_small_packs: i64,
        limit: i64,
    ) -> VfsStorageResult<Vec<VfsIndexScope>>;

    async fn list_small_packs_for_scope(
        &self,
        scope: &VfsIndexScope,
        max_total_bytes: i64,
        max_total_slots: i32,
        limit: i64,
    ) -> VfsStorageResult<Vec<VfsPackRecord>>;

    async fn list_file_manifest_records_by_pack_keys(
        &self,
        scope: &VfsIndexScope,
        pack_keys: &[String],
    ) -> VfsStorageResult<Vec<VfsFileManifestRecord>>;

    async fn apply_pack_compaction(
        &self,
        scope: &VfsIndexScope,
        new_pack: VfsPackRecord,
        repoints: &[VfsManifestRepoint],
        old_pack_refcount_decrements: &[(String, i32)],
    ) -> VfsStorageResult<()>;

    async fn correct_pack_refcount_drift(&self) -> VfsStorageResult<u64>;

    async fn list_zero_reference_packs(
        &self,
        limit: i64,
    ) -> VfsStorageResult<Vec<VfsPackRecordWithScope>>;

    async fn recount_pack_reference_count(
        &self,
        scope: &VfsIndexScope,
        pack_key: &str,
    ) -> VfsStorageResult<i32>;

    async fn delete_pack_records(&self, packs: &[(VfsIndexScope, String)]) -> VfsStorageResult<()>;
}

impl VfsFileManifest {
    pub fn object_state(&self) -> VfsStorageObjectState {
        VfsStorageObjectState {
            size_bytes: self.logical_size_bytes.max(0) as u64,
            pack_key: self.pack_slot.pack_key.clone(),
            pack_slot_offset: self.pack_slot.pack_slot_offset,
            pack_slot_length: self.pack_slot.pack_slot_length,
            pack_slot_compression: self.pack_slot.pack_slot_compression,
        }
    }
}

impl VfsIndexEntryWithManifest {
    pub fn into_storage_metadata(self) -> VfsStorageMetadata {
        let object_state = self.manifest.as_ref().map(VfsFileManifest::object_state);
        let token_count = self
            .manifest
            .as_ref()
            .and_then(|manifest| manifest.token_count);
        VfsStorageMetadata {
            path: self.entry.logical_path,
            kind: self.entry.kind,
            size_bytes: self.entry.size_bytes.max(0) as u64,
            file_id: self.entry.file_id,
            link_count: self.entry.link_count.max(1),
            link_target: None,
            mode: None,
            executable: false,
            content_hash: self.entry.content_hash,
            token_count,
            version: self.entry.current_version,
            updated_at: self.entry.updated_at,
            object_state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::VfsPackSlotRef;

    #[test]
    fn manifest_object_state_preserves_slot_coordinates() {
        let manifest = VfsFileManifest {
            logical_path: "notes/a.md".to_string(),
            content_hash: "hash".to_string(),
            logical_size_bytes: 42,
            pack_slot: VfsPackSlotRef {
                pack_key: "packs/one.pack".to_string(),
                pack_slot_offset: 10,
                pack_slot_length: 20,
                pack_slot_compression: 1,
            },
            token_count: Some(7),
        };

        let state = manifest.object_state();

        assert_eq!(state.size_bytes, 42);
        assert_eq!(state.pack_key, "packs/one.pack");
        assert_eq!(state.pack_slot_offset, 10);
        assert_eq!(state.pack_slot_length, 20);
        assert_eq!(state.pack_slot_compression, 1);
    }

    #[test]
    fn entry_with_manifest_maps_to_storage_metadata() {
        let manifest = VfsFileManifest {
            logical_path: "notes/a.md".to_string(),
            content_hash: "hash".to_string(),
            logical_size_bytes: 42,
            pack_slot: VfsPackSlotRef {
                pack_key: "packs/one.pack".to_string(),
                pack_slot_offset: 10,
                pack_slot_length: 20,
                pack_slot_compression: 1,
            },
            token_count: Some(7),
        };
        let entry = VfsIndexEntry {
            logical_path: "notes/a.md".to_string(),
            parent_logical_path: "notes".to_string(),
            entry_name: "a.md".to_string(),
            kind: VfsStorageEntryKind::File,
            file_id: Some("file-1".to_string()),
            link_count: 1,
            size_bytes: 42,
            content_hash: Some("hash".to_string()),
            current_version: Some("v1".to_string()),
            updated_at: None,
        };

        let metadata = VfsIndexEntryWithManifest {
            entry,
            manifest: Some(manifest),
        }
        .into_storage_metadata();

        assert_eq!(metadata.path, "notes/a.md");
        assert_eq!(metadata.content_hash.as_deref(), Some("hash"));
        assert_eq!(metadata.token_count, Some(7));
        assert_eq!(
            metadata
                .object_state
                .as_ref()
                .map(|state| state.pack_key.as_str()),
            Some("packs/one.pack")
        );
    }
}
