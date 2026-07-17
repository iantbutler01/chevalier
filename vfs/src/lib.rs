// @dive-file: Optimized VFS storage primitives shared by local and gateway-backed consumers.
// @dive-rel: Owns generic storage semantics such as logical metadata, batch reads/writes,
// @dive-rel: preconditions, subtree prefetch, and pack-format helpers without product policy.
// @dive-rel: Complements vfs.rs, which remains the HTTP/FUSE gateway protocol boundary.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub mod compaction;
#[cfg(feature = "gateway")]
pub mod gateway;
#[cfg(feature = "gcs")]
pub mod gcs_object_store;
pub mod index;
pub mod local;
pub mod manifest;
pub mod object_storage;
pub mod object_store;
pub mod pack;
pub mod pack_cache;
#[cfg(feature = "postgres")]
pub mod postgres_index;

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum VfsStorageError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type VfsStorageResult<T> = std::result::Result<T, VfsStorageError>;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum VfsStorageEntryKind {
    File,
    Directory,
    Symlink,
    Special,
}

impl VfsStorageEntryKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
            Self::Symlink => "symlink",
            Self::Special => "special",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageObjectState {
    pub size_bytes: u64,
    pub pack_key: String,
    pub pack_slot_offset: i64,
    pub pack_slot_length: i64,
    pub pack_slot_compression: i16,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct VfsStorageMetadata {
    pub path: String,
    pub kind: VfsStorageEntryKind,
    pub size_bytes: u64,
    #[serde(default)]
    pub link_target: Option<String>,
    #[serde(default)]
    pub executable: bool,
    pub content_hash: Option<String>,
    pub token_count: Option<i32>,
    pub version: Option<String>,
    pub updated_at: Option<DateTime<Utc>>,
    pub object_state: Option<VfsStorageObjectState>,
}

impl Default for VfsStorageMetadata {
    fn default() -> Self {
        Self {
            path: String::new(),
            kind: VfsStorageEntryKind::File,
            size_bytes: 0,
            link_target: None,
            executable: false,
            content_hash: None,
            token_count: None,
            version: None,
            updated_at: None,
            object_state: None,
        }
    }
}

impl VfsStorageMetadata {
    pub fn new(path: impl Into<String>, kind: VfsStorageEntryKind, size_bytes: u64) -> Self {
        Self {
            path: path.into(),
            kind,
            size_bytes,
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageWritePrecondition {
    pub fingerprint: Option<String>,
    pub secondary_fingerprint: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageMetadataFields {
    pub include_object_state: bool,
    pub include_token_count: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageDirListFilter {
    pub name_like: Option<String>,
    pub name_not_like: Option<String>,
    pub entry_kind: Option<VfsStorageEntryKind>,
    pub limit: Option<i64>,
    pub order: Option<VfsStorageDirListOrder>,
    #[serde(default)]
    pub max_hash_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum VfsStorageDirListOrder {
    KindThenName,
    NameAsc,
    NameDesc,
    UpdatedDesc,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageSubtreeOptions {
    pub include_object_state: bool,
    pub include_token_count: bool,
    pub limit: Option<i64>,
    #[serde(default)]
    pub max_hash_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageReadRange {
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageWriteOptions {
    #[serde(default)]
    pub executable: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageWrite {
    pub path: String,
    pub bytes: Bytes,
    pub token_count: Option<i32>,
    pub precondition: Option<VfsStorageWritePrecondition>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageWriteResult {
    pub path: String,
    pub content_hash: String,
    pub previous_hash: Option<String>,
    pub changed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageReadIfChanged {
    pub path: String,
    pub known_content_hash: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageReadIfChangedResult {
    pub path: String,
    pub content_hash: Option<String>,
    pub bytes: Option<Bytes>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageDeleteResult {
    pub previous: Option<VfsStorageMetadata>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStorageRenameResult {
    pub previous: Option<VfsStorageMetadata>,
    pub current: Option<VfsStorageMetadata>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VfsStorageNamespaceMutation {
    CreateDirectory {
        path: String,
    },
    CreateSymlink {
        path: String,
        target: String,
    },
    DeleteFile {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        precondition: Option<VfsStorageWritePrecondition>,
    },
    RemoveDirectory {
        path: String,
    },
    Rename {
        from: String,
        to: String,
    },
}

impl VfsStorageNamespaceMutation {
    pub fn paths(&self) -> [&str; 2] {
        match self {
            Self::CreateDirectory { path }
            | Self::CreateSymlink { path, .. }
            | Self::DeleteFile { path, .. }
            | Self::RemoveDirectory { path } => [path.as_str(), ""],
            Self::Rename { from, to } => [from.as_str(), to.as_str()],
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStoragePrefetchOptions {
    pub include_small_file_bytes: bool,
    pub max_entries: Option<i64>,
    pub max_pack_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsStoragePrefetchResult {
    pub warmed_file_bytes: Vec<(String, Bytes)>,
}

#[async_trait::async_trait]
pub trait OptimizedVfsStorage: Send + Sync {
    fn backend_name(&self) -> &'static str;

    async fn stat(&self, path: &str) -> VfsStorageResult<Option<VfsStorageMetadata>>;

    async fn metadata_many(
        &self,
        paths: &[String],
        fields: VfsStorageMetadataFields,
    ) -> VfsStorageResult<Vec<Option<VfsStorageMetadata>>>;

    async fn list_dir_with_metadata(
        &self,
        path: &str,
        filter: VfsStorageDirListFilter,
    ) -> VfsStorageResult<Vec<VfsStorageMetadata>>;

    async fn list_subtree_file_metadata(
        &self,
        prefix: &str,
        options: VfsStorageSubtreeOptions,
    ) -> VfsStorageResult<Vec<VfsStorageMetadata>>;

    async fn read(&self, path: &str) -> VfsStorageResult<Bytes>;

    async fn read_range(&self, path: &str, range: VfsStorageReadRange) -> VfsStorageResult<Bytes>;

    async fn read_many(&self, paths: &[String]) -> VfsStorageResult<Vec<(String, Bytes)>>;

    async fn read_many_if_etag_mismatch(
        &self,
        requests: &[VfsStorageReadIfChanged],
    ) -> VfsStorageResult<Vec<VfsStorageReadIfChangedResult>>;

    async fn write(
        &self,
        path: &str,
        bytes: Bytes,
        precondition: Option<VfsStorageWritePrecondition>,
    ) -> VfsStorageResult<VfsStorageWriteResult>;

    async fn write_with_options(
        &self,
        path: &str,
        bytes: Bytes,
        precondition: Option<VfsStorageWritePrecondition>,
        options: Option<VfsStorageWriteOptions>,
    ) -> VfsStorageResult<VfsStorageWriteResult> {
        let _ = options;
        self.write(path, bytes, precondition).await
    }

    /// Atomically install a host-local regular file without requiring callers to
    /// materialize the entire payload in memory. Backends with a native streaming
    /// path override this; the default preserves compatibility for object stores.
    async fn write_from_local_file(
        &self,
        path: &str,
        source_path: &Path,
        expected_content_hash: Option<&str>,
        precondition: Option<VfsStorageWritePrecondition>,
        options: Option<VfsStorageWriteOptions>,
    ) -> VfsStorageResult<VfsStorageWriteResult> {
        let bytes = std::fs::read(source_path).map_err(|error| {
            VfsStorageError::Internal(format!("read staged VFS upload: {error}"))
        })?;
        let actual_hash = pack::hex_hash(&bytes);
        if expected_content_hash.is_some_and(|expected| expected != actual_hash) {
            return Err(VfsStorageError::Conflict(format!(
                "staged VFS upload hash mismatch for {path}"
            )));
        }
        self.write_with_options(path, Bytes::from(bytes), precondition, options)
            .await
    }

    async fn write_many_atomic(
        &self,
        writes: Vec<VfsStorageWrite>,
    ) -> VfsStorageResult<Vec<VfsStorageWriteResult>>;

    async fn write_many_if_changed_atomic(
        &self,
        writes: Vec<VfsStorageWrite>,
    ) -> VfsStorageResult<Vec<VfsStorageWriteResult>>;

    async fn mkdir(&self, path: &str) -> VfsStorageResult<()>;

    /// Create a symbolic link when the backend supports symlink metadata.
    ///
    /// Backends that cannot represent symlinks use this stable BadRequest. The
    /// FUSE bridge maps that BadRequest to EPERM for symlink(2).
    async fn create_symlink(&self, path: &str, target: &str) -> VfsStorageResult<()> {
        let _ = (path, target);
        Err(VfsStorageError::BadRequest(
            "symlink creation not supported by this VFS backend".to_string(),
        ))
    }

    async fn delete_file_with_metadata(
        &self,
        path: &str,
        precondition: Option<VfsStorageWritePrecondition>,
    ) -> VfsStorageResult<VfsStorageDeleteResult>;

    async fn rmdir(&self, path: &str) -> VfsStorageResult<()>;

    async fn rename_with_metadata(
        &self,
        from: &str,
        to: &str,
    ) -> VfsStorageResult<VfsStorageRenameResult>;

    /// Apply ordered namespace mutations through one backend call. Implementations
    /// must make successfully applied entries idempotent so an interrupted caller
    /// can replay the whole ordered batch without corrupting namespace state.
    async fn apply_namespace_batch(
        &self,
        mutations: Vec<VfsStorageNamespaceMutation>,
    ) -> VfsStorageResult<()> {
        for mutation in mutations {
            match mutation {
                VfsStorageNamespaceMutation::CreateDirectory { path } => {
                    self.mkdir(path.as_str()).await?;
                }
                VfsStorageNamespaceMutation::CreateSymlink { path, target } => {
                    self.create_symlink(path.as_str(), target.as_str()).await?;
                }
                VfsStorageNamespaceMutation::DeleteFile { path, precondition } => {
                    self.delete_file_with_metadata(path.as_str(), precondition)
                        .await?;
                }
                VfsStorageNamespaceMutation::RemoveDirectory { path } => {
                    self.rmdir(path.as_str()).await?;
                }
                VfsStorageNamespaceMutation::Rename { from, to } => {
                    self.rename_with_metadata(from.as_str(), to.as_str())
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn prefetch_subtree(
        &self,
        prefix: &str,
        options: VfsStoragePrefetchOptions,
    ) -> VfsStorageResult<VfsStoragePrefetchResult>;
}
