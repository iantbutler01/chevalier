// @dive-file: Generic VFS/FUSE protocol contract shared by vmd clients and product gateway adapters.
// @dive-rel: Used by vmd/src/fuse/client.rs and opt-in Axum gateway consumers to avoid product-owned route wiring.
// @dive-rel: Complements vm.rs mount planning by owning endpoint shape, protocol headers, DTOs, and gateway validation.

use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const CHEVALIER_VFS_ROUTE_PREFIX: &str = "/internal/chevalier/vfs";
pub const CHEVALIER_VFS_ENDPOINT_PATH_PREFIX: &str = "/v1/internal/chevalier/vfs";

pub const CHEVALIER_VFS_RUN_ID_HEADER: &str = "x-chevalier-vfs-run-id";
pub const CHEVALIER_VFS_COMPONENT_HEADER: &str = "x-chevalier-vfs-component";
pub const CHEVALIER_VFS_SURFACE_KIND_HEADER: &str = "x-chevalier-vfs-surface-kind";
pub const CHEVALIER_VFS_OPERATION_HEADER: &str = "x-chevalier-vfs-operation";
pub const CHEVALIER_VFS_REASON_HEADER: &str = "x-chevalier-vfs-reason";
pub const CHEVALIER_VFS_RESOURCE_KEY_HEADER: &str = "x-chevalier-vfs-resource-key";
pub const CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER: &str = "x-chevalier-vfs-lock-owner-token";
pub const CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER: &str =
    "x-chevalier-vfs-precondition-fingerprint";
pub const CHEVALIER_VFS_PRECONDITION_SECONDARY_FINGERPRINT_HEADER: &str =
    "x-chevalier-vfs-precondition-secondary-fingerprint";

pub const VFS_COMPONENT_VM_RUNTIME: &str = "vm_runtime";
pub const VFS_ENTRY_KIND_FILE: &str = "file";
pub const VFS_ENTRY_KIND_DIRECTORY: &str = "directory";
pub const VFS_ENTRY_KIND_SYMLINK: &str = "symlink";
pub const VFS_SURFACE_KIND_VM_SHARED: &str = "vm_shared_vfs";
pub const VFS_SURFACE_KIND_VM_WORKSPACE: &str = "vm_workspace_vfs";
pub const VFS_OPERATION_WRITE_THROUGH: &str = "vfs_write_through";
pub const VFS_OPERATION_SETATTR_SIZE: &str = "vfs_setattr_size";
pub const VFS_OPERATION_MKDIR: &str = "vfs_mkdir";
pub const VFS_OPERATION_UNLINK: &str = "vfs_unlink";
pub const VFS_OPERATION_RMDIR: &str = "vfs_rmdir";
pub const VFS_OPERATION_RENAME: &str = "vfs_rename";
pub const VFS_OPERATION_SYMLINK: &str = "vfs_symlink";

pub const DEFAULT_VFS_BODY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsDirEntry {
    pub name: String,
    pub kind: String,
    pub size_bytes: u64,
    #[serde(default)]
    pub link_target: Option<String>,
    pub content_hash: Option<String>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsMetadata {
    pub kind: String,
    pub size_bytes: u64,
    #[serde(default)]
    pub link_target: Option<String>,
    pub content_hash: Option<String>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsMetadataManyRequest {
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsMetadataManyResponse {
    pub entries: Vec<Option<VfsMetadata>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsDeleteMetadataResponse {
    pub previous: Option<VfsMetadata>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsRenameMetadataResponse {
    pub previous: Option<VfsMetadata>,
    pub current: Option<VfsMetadata>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsReadManyRequest {
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsReadManyResponse {
    pub entries: Vec<Option<Vec<u8>>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsListDirOptions {
    #[serde(default)]
    pub name_like: Option<String>,
    #[serde(default)]
    pub name_not_like: Option<String>,
    #[serde(default)]
    pub entry_kind: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub order: Option<String>,
    #[serde(default)]
    pub max_hash_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsObjectState {
    pub size_bytes: u64,
    pub pack_key: String,
    pub pack_slot_offset: i64,
    pub pack_slot_length: i64,
    pub pack_slot_compression: i16,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsSubtreeMetadataEntry {
    pub path: String,
    pub kind: String,
    pub size_bytes: u64,
    #[serde(default)]
    pub link_target: Option<String>,
    pub content_hash: Option<String>,
    pub token_count: Option<i32>,
    pub version: Option<String>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub object_state: Option<VfsObjectState>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsSubtreeMetadataRequest {
    pub prefix: String,
    #[serde(default)]
    pub include_object_state: bool,
    #[serde(default)]
    pub include_token_count: bool,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub max_hash_bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsSubtreeMetadataResponse {
    pub entries: Vec<VfsSubtreeMetadataEntry>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsPrefetchSubtreeRequest {
    pub prefix: String,
    #[serde(default)]
    pub include_small_file_bytes: bool,
    #[serde(default)]
    pub max_entries: Option<i64>,
    #[serde(default)]
    pub max_pack_bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsPrefetchFileBytes {
    pub path: String,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsPrefetchSubtreeResponse {
    pub warmed_file_bytes: Vec<VfsPrefetchFileBytes>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsWritePrecondition {
    #[serde(default)]
    pub fingerprint: Option<String>,
    #[serde(default)]
    pub secondary_fingerprint: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsWriteManyItem {
    pub path: String,
    pub body: Vec<u8>,
    #[serde(default)]
    pub precondition: Option<VfsWritePrecondition>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsWriteManyBody {
    pub writes: Vec<VfsWriteManyItem>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsWriteManyResult {
    pub path: String,
    pub content_hash: String,
    pub previous_hash: Option<String>,
    pub changed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsWriteManyResponse {
    pub results: Vec<VfsWriteManyResult>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsLeaseGrant {
    pub resource_key: String,
    pub owner_token: Uuid,
    pub task_id: Option<Uuid>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsLeaseAcquireRequest {
    pub path: String,
    #[serde(default)]
    pub mutation_count: Option<i32>,
    #[serde(default)]
    pub component: Option<String>,
    #[serde(default)]
    pub run_id: Option<Uuid>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsLeaseReleaseRequest {
    pub resource_key: String,
    pub owner_token: Uuid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VfsReadRange {
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsWriteScope {
    pub resource_key: String,
    pub default_surface_kind: String,
    pub task_id: Option<Uuid>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsWriteHeaders {
    pub run_id: Option<Uuid>,
    pub component: String,
    pub surface_kind: String,
    pub operation: String,
    pub reason: String,
    pub owner_token: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsWriteRequest {
    pub owner_id: String,
    pub path: String,
    pub body: Bytes,
    pub headers: VfsWriteHeaders,
    pub scope: VfsWriteScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsWriteManyRequest {
    pub owner_id: String,
    pub writes: Vec<VfsWriteManyItem>,
    pub headers: VfsWriteHeaders,
    pub scope: VfsWriteScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsNamespaceMutationRequest {
    pub owner_id: String,
    pub path: String,
    pub headers: VfsWriteHeaders,
    pub scope: VfsWriteScope,
    pub precondition: Option<VfsWritePrecondition>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsRenameRequest {
    pub owner_id: String,
    pub from: String,
    pub to: String,
    pub headers: VfsWriteHeaders,
    pub scope: VfsWriteScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsSymlinkRequest {
    pub owner_id: String,
    pub path: String,
    pub target: String,
    pub headers: VfsWriteHeaders,
    pub scope: VfsWriteScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsLeaseAcquire {
    pub owner_id: String,
    pub path: String,
    pub mutation_count: i32,
    pub component: String,
    pub run_id: Option<Uuid>,
    pub reason: Option<String>,
    pub owner_token: Uuid,
    pub scope: VfsWriteScope,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VfsHeaderAliases {
    pub run_id: Vec<&'static str>,
    pub component: Vec<&'static str>,
    pub surface_kind: Vec<&'static str>,
    pub operation: Vec<&'static str>,
    pub reason: Vec<&'static str>,
    pub resource_key: Vec<&'static str>,
    pub lock_owner_token: Vec<&'static str>,
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum VfsGatewayError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type VfsResult<T> = std::result::Result<T, VfsGatewayError>;

pub fn owner_vfs_endpoint(base_url: &str, owner_id: impl fmt::Display) -> String {
    format!(
        "{}{}/{}",
        base_url.trim().trim_end_matches('/'),
        CHEVALIER_VFS_ENDPOINT_PATH_PREFIX,
        owner_id
    )
}

pub fn scoped_vfs_path(scope_path: &str, relative: &str) -> String {
    let scope = scope_path.trim_matches('/');
    let rel = relative.trim_matches('/');
    if scope.is_empty() {
        rel.to_string()
    } else if rel.is_empty() {
        scope.to_string()
    } else {
        format!("{scope}/{rel}")
    }
}

pub fn parse_vfs_range_header(value: &str, total_size: u64) -> VfsResult<VfsReadRange> {
    let trimmed = value.trim();
    let Some(range) = trimmed.strip_prefix("bytes=") else {
        return Err(VfsGatewayError::BadRequest(format!(
            "unsupported range header: {trimmed}"
        )));
    };
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| VfsGatewayError::BadRequest(format!("invalid range header: {trimmed}")))?;
    let offset = start
        .parse::<u64>()
        .map_err(|err| VfsGatewayError::BadRequest(format!("invalid range start: {err}")))?;
    let length = if end.trim().is_empty() {
        total_size.checked_sub(offset).ok_or_else(|| {
            VfsGatewayError::BadRequest(format!("range start {offset} is beyond EOF {total_size}"))
        })?
    } else {
        let end = end
            .parse::<u64>()
            .map_err(|err| VfsGatewayError::BadRequest(format!("invalid range end: {err}")))?
            .min(total_size.saturating_sub(1));
        if end < offset {
            return Err(VfsGatewayError::BadRequest(format!(
                "invalid range header: {trimmed}"
            )));
        }
        end - offset + 1
    };
    if length == 0 {
        return Err(VfsGatewayError::BadRequest(format!(
            "range start {offset} is beyond EOF {total_size}"
        )));
    }
    Ok(VfsReadRange { offset, length })
}

#[cfg(feature = "vfs-server")]
mod server {
    use async_trait::async_trait;
    use axum::{
        Json, Router,
        body::{Body, Bytes},
        extract::{DefaultBodyLimit, FromRef, Path, Query, State},
        http::{HeaderMap, HeaderValue, StatusCode, header},
        response::{IntoResponse, Response},
        routing::{get, post, put},
    };
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::{
        CHEVALIER_VFS_COMPONENT_HEADER, CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
        CHEVALIER_VFS_OPERATION_HEADER, CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER,
        CHEVALIER_VFS_PRECONDITION_SECONDARY_FINGERPRINT_HEADER, CHEVALIER_VFS_REASON_HEADER,
        CHEVALIER_VFS_RESOURCE_KEY_HEADER, CHEVALIER_VFS_ROUTE_PREFIX, CHEVALIER_VFS_RUN_ID_HEADER,
        CHEVALIER_VFS_SURFACE_KIND_HEADER, DEFAULT_VFS_BODY_LIMIT_BYTES, VFS_COMPONENT_VM_RUNTIME,
        VFS_ENTRY_KIND_FILE, VFS_OPERATION_MKDIR, VFS_OPERATION_RENAME, VFS_OPERATION_RMDIR,
        VFS_OPERATION_SYMLINK, VFS_OPERATION_UNLINK, VFS_OPERATION_WRITE_THROUGH,
        VfsDeleteMetadataResponse, VfsDirEntry, VfsGatewayError, VfsHeaderAliases, VfsLeaseAcquire,
        VfsLeaseAcquireRequest, VfsLeaseGrant, VfsLeaseReleaseRequest, VfsListDirOptions,
        VfsMetadata, VfsMetadataManyRequest, VfsMetadataManyResponse, VfsNamespaceMutationRequest,
        VfsPrefetchSubtreeRequest, VfsPrefetchSubtreeResponse, VfsReadManyRequest,
        VfsReadManyResponse, VfsReadRange, VfsRenameMetadataResponse, VfsRenameRequest, VfsResult,
        VfsSubtreeMetadataEntry, VfsSubtreeMetadataRequest, VfsSubtreeMetadataResponse,
        VfsSymlinkRequest, VfsWriteHeaders, VfsWriteManyBody, VfsWriteManyRequest,
        VfsWriteManyResponse, VfsWriteManyResult, VfsWritePrecondition, VfsWriteRequest,
        VfsWriteScope, parse_vfs_range_header,
    };

    #[async_trait]
    pub trait VfsGatewayBackend: Clone + Send + Sync + 'static {
        fn header_aliases(&self) -> VfsHeaderAliases {
            VfsHeaderAliases::default()
        }

        fn cross_scope_rename_message(&self) -> String {
            "cross-scope rename is not supported for this vfs mount".to_string()
        }

        async fn list_dir(&self, owner_id: &str, path: &str) -> VfsResult<Vec<VfsDirEntry>>;
        async fn list_dir_with_options(
            &self,
            owner_id: &str,
            path: &str,
            options: VfsListDirOptions,
        ) -> VfsResult<Vec<VfsDirEntry>> {
            let entries = self.list_dir(owner_id, path).await?;
            Ok(filter_dir_entries(entries, &options))
        }
        async fn stat(&self, owner_id: &str, path: &str) -> VfsResult<VfsMetadata>;
        async fn metadata_many(
            &self,
            owner_id: &str,
            paths: &[String],
        ) -> VfsResult<Vec<Option<VfsMetadata>>> {
            let mut entries = Vec::with_capacity(paths.len());
            for path in paths {
                match self.stat(owner_id, path).await {
                    Ok(metadata) => entries.push(Some(metadata)),
                    Err(VfsGatewayError::NotFound(_)) => entries.push(None),
                    Err(error) => return Err(error),
                }
            }
            Ok(entries)
        }
        async fn list_subtree_file_metadata(
            &self,
            _owner_id: &str,
            _request: VfsSubtreeMetadataRequest,
        ) -> VfsResult<Vec<VfsSubtreeMetadataEntry>> {
            Err(VfsGatewayError::BadRequest(
                "gateway backend does not support subtree metadata".to_string(),
            ))
        }
        async fn prefetch_subtree(
            &self,
            _owner_id: &str,
            _request: VfsPrefetchSubtreeRequest,
        ) -> VfsResult<VfsPrefetchSubtreeResponse> {
            Ok(VfsPrefetchSubtreeResponse {
                warmed_file_bytes: Vec::new(),
            })
        }
        async fn stat_for_raw_read(&self, owner_id: &str, path: &str) -> VfsResult<VfsMetadata> {
            self.stat(owner_id, path).await
        }
        async fn read_file(
            &self,
            owner_id: &str,
            path: &str,
            range: Option<VfsReadRange>,
        ) -> VfsResult<Bytes>;
        async fn read_many(
            &self,
            owner_id: &str,
            paths: &[String],
        ) -> VfsResult<Vec<Option<Bytes>>> {
            let mut entries = Vec::with_capacity(paths.len());
            for path in paths {
                match self.read_file(owner_id, path, None).await {
                    Ok(bytes) => entries.push(Some(bytes)),
                    Err(VfsGatewayError::NotFound(_)) => entries.push(None),
                    Err(error) => return Err(error),
                }
            }
            Ok(entries)
        }
        async fn derive_write_scope(&self, owner_id: &str, path: &str) -> VfsResult<VfsWriteScope>;
        async fn write_file(&self, request: VfsWriteRequest) -> VfsResult<()>;
        async fn write_many_atomic(
            &self,
            _request: VfsWriteManyRequest,
        ) -> VfsResult<Vec<VfsWriteManyResult>> {
            Err(VfsGatewayError::BadRequest(
                "gateway backend does not support atomic write_many".to_string(),
            ))
        }
        async fn delete_file(&self, request: VfsNamespaceMutationRequest) -> VfsResult<()>;
        async fn delete_file_with_metadata(
            &self,
            request: VfsNamespaceMutationRequest,
        ) -> VfsResult<VfsDeleteMetadataResponse> {
            self.delete_file(request).await?;
            Ok(VfsDeleteMetadataResponse { previous: None })
        }
        async fn mkdir(&self, request: VfsNamespaceMutationRequest) -> VfsResult<()>;
        async fn create_symlink(&self, _request: VfsSymlinkRequest) -> VfsResult<()> {
            Err(VfsGatewayError::BadRequest(
                "gateway backend does not support symlink creation".to_string(),
            ))
        }
        async fn rmdir(&self, request: VfsNamespaceMutationRequest) -> VfsResult<()>;
        async fn rename(&self, request: VfsRenameRequest) -> VfsResult<()>;
        async fn rename_with_metadata(
            &self,
            request: VfsRenameRequest,
        ) -> VfsResult<VfsRenameMetadataResponse> {
            self.rename(request).await?;
            Ok(VfsRenameMetadataResponse {
                previous: None,
                current: None,
            })
        }
        async fn acquire_lease(&self, request: VfsLeaseAcquire) -> VfsResult<VfsLeaseGrant>;
        async fn release_lease(
            &self,
            owner_id: &str,
            request: VfsLeaseReleaseRequest,
        ) -> VfsResult<()>;
    }

    pub fn chevalier_vfs_routes<S, B>() -> Router<S>
    where
        S: Clone + Send + Sync + 'static,
        B: VfsGatewayBackend + FromRef<S>,
    {
        vfs_routes::<S, B>(CHEVALIER_VFS_ROUTE_PREFIX)
    }

    pub fn vfs_routes<S, B>(owner_route_prefix: &str) -> Router<S>
    where
        S: Clone + Send + Sync + 'static,
        B: VfsGatewayBackend + FromRef<S>,
    {
        let prefix = normalize_route_prefix(owner_route_prefix);
        Router::new()
            .route(
                &format!("{prefix}/{{owner_id}}/tree"),
                get(get_tree::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/file/raw"),
                get(get_file_raw::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/stat"),
                get(get_stat::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/metadata-many"),
                post(post_metadata_many::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/read-many"),
                post(post_read_many::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/subtree-metadata"),
                post(post_subtree_metadata::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/prefetch-subtree"),
                post(post_prefetch_subtree::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/write-many"),
                post(post_write_many::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/file"),
                put(put_file::<S, B>).delete(delete_file::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/dir"),
                put(put_dir::<S, B>).delete(delete_dir::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/symlink"),
                put(put_symlink::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/rename"),
                post(post_rename::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/lease"),
                post(post_lease::<S, B>).delete(delete_lease::<S, B>),
            )
            .layer(DefaultBodyLimit::max(DEFAULT_VFS_BODY_LIMIT_BYTES))
    }

    #[derive(Debug, Deserialize)]
    struct TreeQuery {
        path: Option<String>,
        name_like: Option<String>,
        name_not_like: Option<String>,
        entry_kind: Option<String>,
        limit: Option<i64>,
        order: Option<String>,
        max_hash_bytes: Option<u64>,
    }

    impl TreeQuery {
        fn options(&self) -> VfsListDirOptions {
            VfsListDirOptions {
                name_like: self.name_like.clone(),
                name_not_like: self.name_not_like.clone(),
                entry_kind: self.entry_kind.clone(),
                limit: self.limit,
                order: self.order.clone(),
                max_hash_bytes: self.max_hash_bytes,
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct PathQuery {
        path: Option<String>,
        return_metadata: Option<bool>,
    }

    #[derive(Debug, Deserialize)]
    struct SymlinkQuery {
        path: Option<String>,
        target: String,
    }

    #[derive(Debug, Deserialize)]
    struct RenameQuery {
        from: String,
        to: String,
        return_metadata: Option<bool>,
    }

    #[derive(Serialize)]
    struct ErrorBody {
        error: String,
    }

    impl IntoResponse for VfsGatewayError {
        fn into_response(self) -> Response {
            let status = match &self {
                VfsGatewayError::NotFound(_) => StatusCode::NOT_FOUND,
                VfsGatewayError::BadRequest(_) => StatusCode::BAD_REQUEST,
                VfsGatewayError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
                VfsGatewayError::Forbidden(_) => StatusCode::FORBIDDEN,
                VfsGatewayError::Conflict(_) => StatusCode::CONFLICT,
                VfsGatewayError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (
                status,
                Json(ErrorBody {
                    error: self.to_string(),
                }),
            )
                .into_response()
        }
    }

    async fn get_tree<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Query(params): Query<TreeQuery>,
    ) -> VfsResult<Json<Vec<VfsDirEntry>>>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let path = params.path.as_deref().unwrap_or_default();
        backend
            .list_dir_with_options(owner_id.as_str(), path, params.options())
            .await
            .map(Json)
    }

    async fn get_stat<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Query(params): Query<PathQuery>,
    ) -> VfsResult<Json<VfsMetadata>>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let path = params.path.as_deref().unwrap_or_default();
        backend.stat(owner_id.as_str(), path).await.map(Json)
    }

    async fn post_metadata_many<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsMetadataManyRequest>,
    ) -> VfsResult<Json<VfsMetadataManyResponse>>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let entries = backend
            .metadata_many(owner_id.as_str(), body.paths.as_slice())
            .await?;
        Ok(Json(VfsMetadataManyResponse { entries }))
    }

    async fn post_read_many<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsReadManyRequest>,
    ) -> VfsResult<Json<VfsReadManyResponse>>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let entries = backend
            .read_many(owner_id.as_str(), body.paths.as_slice())
            .await?
            .into_iter()
            .map(|entry| entry.map(|bytes| bytes.to_vec()))
            .collect();
        Ok(Json(VfsReadManyResponse { entries }))
    }

    async fn post_subtree_metadata<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsSubtreeMetadataRequest>,
    ) -> VfsResult<Json<VfsSubtreeMetadataResponse>>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let entries = backend
            .list_subtree_file_metadata(owner_id.as_str(), body)
            .await?;
        Ok(Json(VfsSubtreeMetadataResponse { entries }))
    }

    async fn post_prefetch_subtree<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsPrefetchSubtreeRequest>,
    ) -> VfsResult<Json<VfsPrefetchSubtreeResponse>>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        backend
            .prefetch_subtree(owner_id.as_str(), body)
            .await
            .map(Json)
    }

    async fn post_write_many<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        headers: HeaderMap,
        Json(body): Json<VfsWriteManyBody>,
    ) -> VfsResult<Json<VfsWriteManyResponse>>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        if body.writes.is_empty() {
            return Ok(Json(VfsWriteManyResponse {
                results: Vec::new(),
            }));
        }
        let first_path = required_path(Some(body.writes[0].path.as_str()))?;
        let first_scope = backend
            .derive_write_scope(owner_id.as_str(), first_path)
            .await?;
        for write in body.writes.iter().skip(1) {
            let path = required_path(Some(write.path.as_str()))?;
            let scope = backend.derive_write_scope(owner_id.as_str(), path).await?;
            if scope.resource_key != first_scope.resource_key {
                return Err(VfsGatewayError::Conflict(
                    backend.cross_scope_rename_message(),
                ));
            }
        }
        let aliases = backend.header_aliases();
        let write_headers = parse_write_headers(
            &headers,
            &aliases,
            first_scope.default_surface_kind.as_str(),
            VFS_OPERATION_WRITE_THROUGH,
        )?;
        validate_declared_resource_key(&headers, &aliases, first_scope.resource_key.as_str())?;
        let results = backend
            .write_many_atomic(VfsWriteManyRequest {
                owner_id,
                writes: body.writes,
                headers: write_headers,
                scope: first_scope,
            })
            .await?;
        Ok(Json(VfsWriteManyResponse { results }))
    }

    async fn get_file_raw<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Query(params): Query<PathQuery>,
        headers: HeaderMap,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let path = required_path(params.path.as_deref())?;
        let metadata = backend.stat_for_raw_read(owner_id.as_str(), path).await?;
        if metadata.kind != VFS_ENTRY_KIND_FILE {
            return Err(VfsGatewayError::BadRequest(format!(
                "vfs path {path} is not a file"
            )));
        }
        let range = headers
            .get(header::RANGE)
            .and_then(|value| value.to_str().ok())
            .map(|value| parse_vfs_range_header(value, metadata.size_bytes))
            .transpose()?;
        let bytes = backend.read_file(owner_id.as_str(), path, range).await?;
        let bytes_len = bytes.len() as u64;

        let mut response = Response::new(Body::from(bytes));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        response
            .headers_mut()
            .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        if let Some(range) = range {
            let end = range.offset.saturating_add(bytes_len).saturating_sub(1);
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!(
                    "bytes {}-{end}/{}",
                    range.offset, metadata.size_bytes
                ))
                .map_err(|err| VfsGatewayError::Internal(err.to_string()))?,
            );
        }
        Ok(response)
    }

    async fn put_file<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Query(params): Query<PathQuery>,
        headers: HeaderMap,
        body: Bytes,
    ) -> VfsResult<StatusCode>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let path = required_path(params.path.as_deref())?;
        let scope = backend.derive_write_scope(owner_id.as_str(), path).await?;
        let write_headers = parse_write_headers(
            &headers,
            &backend.header_aliases(),
            scope.default_surface_kind.as_str(),
            VFS_OPERATION_WRITE_THROUGH,
        )?;
        validate_declared_resource_key(
            &headers,
            &backend.header_aliases(),
            scope.resource_key.as_str(),
        )?;
        backend
            .write_file(VfsWriteRequest {
                owner_id,
                path: path.to_string(),
                body,
                headers: write_headers,
                scope,
            })
            .await?;
        Ok(StatusCode::NO_CONTENT)
    }

    async fn delete_file<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Query(params): Query<PathQuery>,
        headers: HeaderMap,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let request = namespace_mutation_request(
            &backend,
            owner_id,
            params.path.as_deref(),
            &headers,
            VFS_OPERATION_UNLINK,
        )
        .await?;
        if params.return_metadata.unwrap_or(false) {
            let response = backend.delete_file_with_metadata(request).await?;
            Ok((StatusCode::OK, Json(response)).into_response())
        } else {
            backend.delete_file(request).await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
    }

    async fn put_dir<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Query(params): Query<PathQuery>,
        headers: HeaderMap,
    ) -> VfsResult<StatusCode>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let request = namespace_mutation_request(
            &backend,
            owner_id,
            params.path.as_deref(),
            &headers,
            VFS_OPERATION_MKDIR,
        )
        .await?;
        backend.mkdir(request).await?;
        Ok(StatusCode::NO_CONTENT)
    }

    async fn put_symlink<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Query(params): Query<SymlinkQuery>,
        headers: HeaderMap,
    ) -> VfsResult<StatusCode>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let path = required_path(params.path.as_deref())?;
        if params.target.is_empty() {
            return Err(VfsGatewayError::BadRequest(
                "symlink target is required".to_string(),
            ));
        }
        let scope = backend.derive_write_scope(owner_id.as_str(), path).await?;
        let aliases = backend.header_aliases();
        let write_headers = parse_write_headers(
            &headers,
            &aliases,
            scope.default_surface_kind.as_str(),
            VFS_OPERATION_SYMLINK,
        )?;
        validate_declared_resource_key(&headers, &aliases, scope.resource_key.as_str())?;
        backend
            .create_symlink(VfsSymlinkRequest {
                owner_id,
                path: path.to_string(),
                target: params.target,
                headers: write_headers,
                scope,
            })
            .await?;
        Ok(StatusCode::NO_CONTENT)
    }

    async fn delete_dir<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Query(params): Query<PathQuery>,
        headers: HeaderMap,
    ) -> VfsResult<StatusCode>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let request = namespace_mutation_request(
            &backend,
            owner_id,
            params.path.as_deref(),
            &headers,
            VFS_OPERATION_RMDIR,
        )
        .await?;
        backend.rmdir(request).await?;
        Ok(StatusCode::NO_CONTENT)
    }

    async fn namespace_mutation_request<B>(
        backend: &B,
        owner_id: String,
        path: Option<&str>,
        headers: &HeaderMap,
        default_operation: &str,
    ) -> VfsResult<VfsNamespaceMutationRequest>
    where
        B: VfsGatewayBackend,
    {
        let path = required_path(path)?;
        let scope = backend.derive_write_scope(owner_id.as_str(), path).await?;
        let aliases = backend.header_aliases();
        let write_headers = parse_write_headers(
            headers,
            &aliases,
            scope.default_surface_kind.as_str(),
            default_operation,
        )?;
        validate_declared_resource_key(headers, &aliases, scope.resource_key.as_str())?;
        Ok(VfsNamespaceMutationRequest {
            owner_id,
            path: path.to_string(),
            headers: write_headers,
            scope,
            precondition: parse_write_precondition_headers(headers),
        })
    }

    fn parse_write_precondition_headers(headers: &HeaderMap) -> Option<VfsWritePrecondition> {
        let fingerprint = header_value(headers, CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER, &[]);
        let secondary_fingerprint = header_value(
            headers,
            CHEVALIER_VFS_PRECONDITION_SECONDARY_FINGERPRINT_HEADER,
            &[],
        );
        (fingerprint.is_some() || secondary_fingerprint.is_some()).then_some(VfsWritePrecondition {
            fingerprint,
            secondary_fingerprint,
        })
    }

    async fn post_rename<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Query(params): Query<RenameQuery>,
        headers: HeaderMap,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let from_scope = backend
            .derive_write_scope(owner_id.as_str(), params.from.as_str())
            .await?;
        let to_scope = backend
            .derive_write_scope(owner_id.as_str(), params.to.as_str())
            .await?;
        if from_scope.resource_key != to_scope.resource_key {
            return Err(VfsGatewayError::Conflict(
                backend.cross_scope_rename_message(),
            ));
        }
        let aliases = backend.header_aliases();
        let write_headers = parse_write_headers(
            &headers,
            &aliases,
            from_scope.default_surface_kind.as_str(),
            VFS_OPERATION_RENAME,
        )?;
        validate_declared_resource_key(&headers, &aliases, from_scope.resource_key.as_str())?;
        let return_metadata = params.return_metadata.unwrap_or(false);
        let request = VfsRenameRequest {
            owner_id,
            from: params.from,
            to: params.to,
            headers: write_headers,
            scope: from_scope,
        };
        if return_metadata {
            let response = backend.rename_with_metadata(request).await?;
            Ok((StatusCode::OK, Json(response)).into_response())
        } else {
            backend.rename(request).await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
    }

    async fn post_lease<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsLeaseAcquireRequest>,
    ) -> VfsResult<Json<VfsLeaseGrant>>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let scope = backend
            .derive_write_scope(owner_id.as_str(), body.path.as_str())
            .await?;
        let owner_token = Uuid::new_v4();
        backend
            .acquire_lease(VfsLeaseAcquire {
                owner_id,
                path: body.path,
                mutation_count: body.mutation_count.unwrap_or(1).max(1),
                component: body
                    .component
                    .unwrap_or_else(|| VFS_COMPONENT_VM_RUNTIME.to_string()),
                run_id: body.run_id,
                reason: body.reason,
                owner_token,
                scope,
            })
            .await
            .map(Json)
    }

    async fn delete_lease<S, B>(
        State(backend): State<B>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsLeaseReleaseRequest>,
    ) -> VfsResult<StatusCode>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        backend.release_lease(owner_id.as_str(), body).await?;
        Ok(StatusCode::NO_CONTENT)
    }

    fn filter_dir_entries(
        mut entries: Vec<VfsDirEntry>,
        options: &VfsListDirOptions,
    ) -> Vec<VfsDirEntry> {
        if let Some(kind) = options.entry_kind.as_deref() {
            entries.retain(|entry| entry.kind == kind);
        }
        if let Some(pattern) = options.name_like.as_deref() {
            entries.retain(|entry| sql_like_match(pattern, &entry.name));
        }
        if let Some(pattern) = options.name_not_like.as_deref() {
            entries.retain(|entry| !sql_like_match(pattern, &entry.name));
        }
        match options.order.as_deref().unwrap_or("kind_then_name") {
            "name_asc" => entries.sort_by(|a, b| a.name.cmp(&b.name)),
            "name_desc" => entries.sort_by(|a, b| b.name.cmp(&a.name)),
            "updated_desc" => entries.sort_by(|a, b| {
                b.updated_at
                    .cmp(&a.updated_at)
                    .then_with(|| a.name.cmp(&b.name))
            }),
            _ => entries.sort_by(|a, b| {
                let a_kind = if a.kind == VFS_ENTRY_KIND_FILE { 1 } else { 0 };
                let b_kind = if b.kind == VFS_ENTRY_KIND_FILE { 1 } else { 0 };
                a_kind.cmp(&b_kind).then_with(|| a.name.cmp(&b.name))
            }),
        }
        if let Some(limit) = options.limit {
            entries.truncate(limit.max(0) as usize);
        }
        entries
    }

    fn sql_like_match(pattern: &str, value: &str) -> bool {
        fn inner(pattern: &[char], value: &[char]) -> bool {
            match pattern.split_first() {
                None => value.is_empty(),
                Some(('%', rest)) => {
                    inner(rest, value) || (!value.is_empty() && inner(pattern, &value[1..]))
                }
                Some(('_', rest)) => !value.is_empty() && inner(rest, &value[1..]),
                Some((expected, rest)) => {
                    value.split_first().is_some_and(|(actual, value_rest)| {
                        actual == expected && inner(rest, value_rest)
                    })
                }
            }
        }

        inner(
            &pattern.chars().collect::<Vec<_>>(),
            &value.chars().collect::<Vec<_>>(),
        )
    }

    fn parse_write_headers(
        headers: &HeaderMap,
        aliases: &VfsHeaderAliases,
        default_surface_kind: &str,
        default_operation: &str,
    ) -> VfsResult<VfsWriteHeaders> {
        Ok(VfsWriteHeaders {
            run_id: parse_optional_uuid_header(
                headers,
                CHEVALIER_VFS_RUN_ID_HEADER,
                &aliases.run_id,
            )?,
            component: header_value(headers, CHEVALIER_VFS_COMPONENT_HEADER, &aliases.component)
                .unwrap_or_else(|| VFS_COMPONENT_VM_RUNTIME.to_string()),
            surface_kind: header_value(
                headers,
                CHEVALIER_VFS_SURFACE_KIND_HEADER,
                &aliases.surface_kind,
            )
            .unwrap_or_else(|| default_surface_kind.to_string()),
            operation: header_value(headers, CHEVALIER_VFS_OPERATION_HEADER, &aliases.operation)
                .unwrap_or_else(|| default_operation.to_string()),
            reason: header_value(headers, CHEVALIER_VFS_REASON_HEADER, &aliases.reason)
                .unwrap_or_else(|| default_operation.to_string()),
            owner_token: parse_required_uuid_header(
                headers,
                CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                &aliases.lock_owner_token,
            )?,
        })
    }

    fn validate_declared_resource_key(
        headers: &HeaderMap,
        aliases: &VfsHeaderAliases,
        derived: &str,
    ) -> VfsResult<()> {
        let value = header_value(
            headers,
            CHEVALIER_VFS_RESOURCE_KEY_HEADER,
            &aliases.resource_key,
        )
        .ok_or_else(|| {
            VfsGatewayError::BadRequest("missing x-chevalier-vfs-resource-key".to_string())
        })?;
        if value != derived {
            return Err(VfsGatewayError::Conflict(format!(
                "vfs resource key mismatch: declared {value}, derived {derived}"
            )));
        }
        Ok(())
    }

    fn header_value(
        headers: &HeaderMap,
        name: &'static str,
        aliases: &[&'static str],
    ) -> Option<String> {
        std::iter::once(name)
            .chain(aliases.iter().copied())
            .find_map(|candidate| {
                headers
                    .get(candidate)
                    .and_then(|value| value.to_str().ok())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            })
    }

    fn parse_optional_uuid_header(
        headers: &HeaderMap,
        name: &'static str,
        aliases: &[&'static str],
    ) -> VfsResult<Option<Uuid>> {
        let Some(value) = header_value(headers, name, aliases) else {
            return Ok(None);
        };
        Uuid::parse_str(&value)
            .map(Some)
            .map_err(|err| VfsGatewayError::BadRequest(format!("invalid {name}: {err}")))
    }

    fn parse_required_uuid_header(
        headers: &HeaderMap,
        name: &'static str,
        aliases: &[&'static str],
    ) -> VfsResult<Uuid> {
        parse_optional_uuid_header(headers, name, aliases)?
            .ok_or_else(|| VfsGatewayError::BadRequest(format!("missing {name}")))
    }

    fn required_path(path: Option<&str>) -> VfsResult<&str> {
        path.map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| VfsGatewayError::BadRequest("missing path".to_string()))
    }

    fn normalize_route_prefix(prefix: &str) -> String {
        let trimmed = prefix.trim();
        let trimmed = if trimmed.is_empty() { "/" } else { trimmed };
        format!("/{}", trimmed.trim_matches('/'))
    }
}

#[cfg(feature = "vfs-server")]
pub use server::{VfsGatewayBackend, chevalier_vfs_routes, vfs_routes};

#[cfg(test)]
mod tests {
    use super::{VfsGatewayError, owner_vfs_endpoint, parse_vfs_range_header, scoped_vfs_path};

    #[test]
    fn endpoint_helper_uses_generic_chevalier_vfs_route() {
        assert_eq!(
            owner_vfs_endpoint("http://internal-api/", "owner-1"),
            "http://internal-api/v1/internal/chevalier/vfs/owner-1"
        );
    }

    #[test]
    fn scoped_path_joins_without_double_slashes() {
        assert_eq!(
            scoped_vfs_path("workspace/", "/logs/out.txt"),
            "workspace/logs/out.txt"
        );
        assert_eq!(scoped_vfs_path("", "/logs/out.txt"), "logs/out.txt");
        assert_eq!(scoped_vfs_path("workspace", ""), "workspace");
    }

    #[test]
    fn parse_range_header_open_ended_consumes_to_eof() {
        assert_eq!(
            parse_vfs_range_header("bytes=100-", 1000).unwrap(),
            super::VfsReadRange {
                offset: 100,
                length: 900
            }
        );
    }

    #[test]
    fn parse_range_header_rejects_start_beyond_eof() {
        assert!(matches!(
            parse_vfs_range_header("bytes=100-", 100),
            Err(VfsGatewayError::BadRequest(_))
        ));
    }
}

#[cfg(all(test, feature = "vfs-server"))]
mod server_tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use axum::body::{Body, Bytes, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::{
        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER, CHEVALIER_VFS_OPERATION_HEADER,
        CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER,
        CHEVALIER_VFS_PRECONDITION_SECONDARY_FINGERPRINT_HEADER, CHEVALIER_VFS_RESOURCE_KEY_HEADER,
        VFS_ENTRY_KIND_DIRECTORY, VFS_ENTRY_KIND_FILE, VFS_OPERATION_SETATTR_SIZE,
        VFS_SURFACE_KIND_VM_WORKSPACE, VfsDirEntry, VfsGatewayBackend, VfsGatewayError,
        VfsLeaseAcquire, VfsLeaseGrant, VfsLeaseReleaseRequest, VfsMetadata,
        VfsMetadataManyResponse, VfsNamespaceMutationRequest, VfsReadManyResponse, VfsReadRange,
        VfsRenameRequest, VfsResult, VfsSymlinkRequest, VfsWriteManyRequest, VfsWriteManyResponse,
        VfsWriteManyResult, VfsWriteRequest, VfsWriteScope, chevalier_vfs_routes,
    };

    #[derive(Clone, Default)]
    struct MemoryBackend {
        inner: Arc<Mutex<MemoryState>>,
    }

    #[derive(Default)]
    struct MemoryState {
        files: HashMap<String, Bytes>,
        dirs: HashSet<String>,
        writes: Vec<VfsWriteRequest>,
        write_many: Vec<VfsWriteManyRequest>,
        deletes: Vec<VfsNamespaceMutationRequest>,
        mkdirs: Vec<VfsNamespaceMutationRequest>,
        rmdirs: Vec<VfsNamespaceMutationRequest>,
        renames: Vec<VfsRenameRequest>,
        symlinks: Vec<VfsSymlinkRequest>,
        leases: Vec<VfsLeaseAcquire>,
        releases: Vec<VfsLeaseReleaseRequest>,
        valid_tokens: HashSet<Uuid>,
        stat_calls: usize,
        raw_read_stat_calls: usize,
    }

    #[async_trait]
    impl VfsGatewayBackend for MemoryBackend {
        async fn list_dir(&self, owner_id: &str, path: &str) -> VfsResult<Vec<VfsDirEntry>> {
            Ok(vec![VfsDirEntry {
                name: format!("{owner_id}:{path}:file.txt"),
                kind: VFS_ENTRY_KIND_FILE.to_string(),
                size_bytes: 5,
                link_target: None,
                content_hash: None,
                updated_at: None,
            }])
        }

        async fn stat(&self, _owner_id: &str, path: &str) -> VfsResult<VfsMetadata> {
            let mut inner = self.inner.lock().unwrap();
            inner.stat_calls += 1;
            if path == "dir" || inner.dirs.contains(path) {
                return Ok(VfsMetadata {
                    kind: VFS_ENTRY_KIND_DIRECTORY.to_string(),
                    size_bytes: 0,
                    link_target: None,
                    content_hash: None,
                    updated_at: None,
                });
            }
            let Some(bytes) = inner.files.get(path) else {
                return Err(VfsGatewayError::NotFound(path.to_string()));
            };
            Ok(VfsMetadata {
                kind: VFS_ENTRY_KIND_FILE.to_string(),
                size_bytes: bytes.len() as u64,
                link_target: None,
                content_hash: None,
                updated_at: None,
            })
        }

        async fn stat_for_raw_read(&self, _owner_id: &str, path: &str) -> VfsResult<VfsMetadata> {
            let mut inner = self.inner.lock().unwrap();
            inner.raw_read_stat_calls += 1;
            if path == "dir" || inner.dirs.contains(path) {
                return Ok(VfsMetadata {
                    kind: VFS_ENTRY_KIND_DIRECTORY.to_string(),
                    size_bytes: 0,
                    link_target: None,
                    content_hash: None,
                    updated_at: None,
                });
            }
            let Some(bytes) = inner.files.get(path) else {
                return Err(VfsGatewayError::NotFound(path.to_string()));
            };
            Ok(VfsMetadata {
                kind: VFS_ENTRY_KIND_FILE.to_string(),
                size_bytes: bytes.len() as u64,
                link_target: None,
                content_hash: None,
                updated_at: None,
            })
        }

        async fn read_file(
            &self,
            _owner_id: &str,
            path: &str,
            range: Option<VfsReadRange>,
        ) -> VfsResult<Bytes> {
            let inner = self.inner.lock().unwrap();
            let bytes = inner
                .files
                .get(path)
                .ok_or_else(|| VfsGatewayError::NotFound(path.to_string()))?;
            let Some(range) = range else {
                return Ok(bytes.clone());
            };
            let start = range.offset as usize;
            let end = start.saturating_add(range.length as usize).min(bytes.len());
            Ok(bytes.slice(start..end))
        }

        async fn derive_write_scope(&self, owner_id: &str, path: &str) -> VfsResult<VfsWriteScope> {
            if path.starts_with("readonly/") {
                return Err(VfsGatewayError::Forbidden(
                    "read-only vfs mount rejected write".to_string(),
                ));
            }
            Ok(VfsWriteScope {
                resource_key: format!("owner:{owner_id}:workspace"),
                default_surface_kind: VFS_SURFACE_KIND_VM_WORKSPACE.to_string(),
                task_id: None,
            })
        }

        async fn write_file(&self, request: VfsWriteRequest) -> VfsResult<()> {
            let mut inner = self.inner.lock().unwrap();
            if request.path == "stale.txt"
                && !inner.valid_tokens.contains(&request.headers.owner_token)
            {
                return Err(VfsGatewayError::Conflict(
                    "stale lease token rejected".to_string(),
                ));
            }
            inner
                .files
                .insert(request.path.clone(), request.body.clone());
            inner.writes.push(request);
            Ok(())
        }

        async fn write_many_atomic(
            &self,
            request: VfsWriteManyRequest,
        ) -> VfsResult<Vec<VfsWriteManyResult>> {
            let mut inner = self.inner.lock().unwrap();
            let mut results = Vec::with_capacity(request.writes.len());
            for write in &request.writes {
                let previous = inner.files.get(write.path.as_str()).cloned();
                inner
                    .files
                    .insert(write.path.clone(), Bytes::from(write.body.clone()));
                let content_hash = format!("hash:{}", write.path);
                results.push(VfsWriteManyResult {
                    path: write.path.clone(),
                    previous_hash: previous.map(|_| format!("old:{}", write.path)),
                    changed: true,
                    content_hash,
                });
            }
            inner.write_many.push(request);
            Ok(results)
        }

        async fn delete_file(&self, request: VfsNamespaceMutationRequest) -> VfsResult<()> {
            let mut inner = self.inner.lock().unwrap();
            inner.files.remove(request.path.as_str());
            inner.deletes.push(request);
            Ok(())
        }

        async fn mkdir(&self, request: VfsNamespaceMutationRequest) -> VfsResult<()> {
            let mut inner = self.inner.lock().unwrap();
            inner.dirs.insert(request.path.clone());
            inner.mkdirs.push(request);
            Ok(())
        }

        async fn create_symlink(&self, request: VfsSymlinkRequest) -> VfsResult<()> {
            let mut inner = self.inner.lock().unwrap();
            inner.symlinks.push(request);
            Ok(())
        }

        async fn rmdir(&self, request: VfsNamespaceMutationRequest) -> VfsResult<()> {
            let mut inner = self.inner.lock().unwrap();
            inner.dirs.remove(request.path.as_str());
            inner.rmdirs.push(request);
            Ok(())
        }

        async fn rename(&self, request: VfsRenameRequest) -> VfsResult<()> {
            let mut inner = self.inner.lock().unwrap();
            if let Some(bytes) = inner.files.remove(request.from.as_str()) {
                inner.files.insert(request.to.clone(), bytes);
            }
            inner.renames.push(request);
            Ok(())
        }

        async fn acquire_lease(&self, request: VfsLeaseAcquire) -> VfsResult<VfsLeaseGrant> {
            let mut inner = self.inner.lock().unwrap();
            inner.valid_tokens.insert(request.owner_token);
            inner.leases.push(request.clone());
            Ok(VfsLeaseGrant {
                resource_key: request.scope.resource_key,
                owner_token: request.owner_token,
                task_id: request.scope.task_id,
            })
        }

        async fn release_lease(
            &self,
            _owner_id: &str,
            request: VfsLeaseReleaseRequest,
        ) -> VfsResult<()> {
            let mut inner = self.inner.lock().unwrap();
            inner.valid_tokens.remove(&request.owner_token);
            inner.releases.push(request);
            Ok(())
        }
    }

    #[tokio::test]
    async fn gateway_stat_list_and_full_read_use_backend() {
        let backend = MemoryBackend::default();
        backend
            .inner
            .lock()
            .unwrap()
            .files
            .insert("asset.bin".to_string(), Bytes::from_static(b"abcdef"));
        let app = chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend);

        let stat = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/stat?path=asset.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stat.status(), StatusCode::OK);
        let body = to_bytes(stat.into_body(), usize::MAX).await.unwrap();
        let metadata: VfsMetadata = serde_json::from_slice(&body).unwrap();
        assert_eq!(metadata.size_bytes, 6);

        let tree = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/tree?path=workspace")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tree.status(), StatusCode::OK);
        let body = to_bytes(tree.into_body(), usize::MAX).await.unwrap();
        let entries: Vec<VfsDirEntry> = serde_json::from_slice(&body).unwrap();
        assert_eq!(entries[0].name, "owner-1:workspace:file.txt");

        let raw = app
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/file/raw?path=asset.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(raw.status(), StatusCode::OK);
        let body = to_bytes(raw.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"abcdef");
    }

    #[tokio::test]
    async fn gateway_raw_read_uses_lightweight_stat_hook() {
        let backend = MemoryBackend::default();
        backend
            .inner
            .lock()
            .unwrap()
            .files
            .insert("asset.bin".to_string(), Bytes::from_static(b"abcdef"));
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/file/raw?path=asset.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let state = backend.inner.lock().unwrap();
        assert_eq!(state.stat_calls, 0);
        assert_eq!(state.raw_read_stat_calls, 1);
    }

    #[tokio::test]
    async fn gateway_metadata_many_preserves_order_and_missing_entries() {
        let backend = MemoryBackend::default();
        backend
            .inner
            .lock()
            .unwrap()
            .files
            .insert("asset.bin".to_string(), Bytes::from_static(b"abcdef"));
        let app = chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/chevalier/vfs/owner-1/metadata-many")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "paths": ["asset.bin", "missing.bin", "dir"],
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let response: VfsMetadataManyResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(response.entries.len(), 3);
        assert_eq!(response.entries[0].as_ref().unwrap().size_bytes, 6);
        assert!(response.entries[1].is_none());
        assert_eq!(
            response.entries[2].as_ref().unwrap().kind,
            VFS_ENTRY_KIND_DIRECTORY
        );
    }

    #[tokio::test]
    async fn gateway_read_many_preserves_order_and_missing_entries() {
        let backend = MemoryBackend::default();
        {
            let mut inner = backend.inner.lock().unwrap();
            inner
                .files
                .insert("first.txt".to_string(), Bytes::from_static(b"one"));
            inner
                .files
                .insert("second.txt".to_string(), Bytes::from_static(b"two"));
        }
        let app = chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/chevalier/vfs/owner-1/read-many")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "paths": ["first.txt", "missing.txt", "second.txt"],
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let response: VfsReadManyResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            response.entries,
            vec![Some(b"one".to_vec()), None, Some(b"two".to_vec())]
        );
    }

    #[tokio::test]
    async fn gateway_write_many_forwards_one_atomic_request() {
        let owner_token = Uuid::new_v4();
        let backend = MemoryBackend::default();
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/chevalier/vfs/owner-1/write-many")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                    .header(
                        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                        owner_token.to_string(),
                    )
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "writes": [
                                {
                                    "path": "first.txt",
                                    "body": [111, 110, 101],
                                    "precondition": {
                                        "fingerprint": "version-1",
                                        "secondary_fingerprint": "secondary-1"
                                    }
                                },
                                {"path": "second.txt", "body": [116, 119, 111]},
                            ],
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let response: VfsWriteManyResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].content_hash, "hash:first.txt");
        let inner = backend.inner.lock().unwrap();
        assert_eq!(inner.write_many.len(), 1);
        assert_eq!(
            inner.write_many[0].writes[0]
                .precondition
                .as_ref()
                .unwrap()
                .fingerprint
                .as_deref(),
            Some("version-1")
        );
        assert_eq!(
            inner.write_many[0].writes[0]
                .precondition
                .as_ref()
                .unwrap()
                .secondary_fingerprint
                .as_deref(),
            Some("secondary-1")
        );
        assert!(inner.write_many[0].writes[1].precondition.is_none());
        assert_eq!(inner.files.get("first.txt").unwrap().as_ref(), b"one");
        assert_eq!(inner.files.get("second.txt").unwrap().as_ref(), b"two");
    }

    #[tokio::test]
    async fn gateway_range_reads_emit_partial_content() {
        let backend = MemoryBackend::default();
        backend
            .inner
            .lock()
            .unwrap()
            .files
            .insert("asset.bin".to_string(), Bytes::from_static(b"abcdef"));
        let app = chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/file/raw?path=asset.bin")
                    .header(header::RANGE, "bytes=2-4")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 2-4/6"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"cde");
    }

    #[tokio::test]
    async fn gateway_write_and_namespace_mutations_forward_validated_requests() {
        let owner_token = Uuid::new_v4();
        let backend = MemoryBackend::default();
        backend
            .inner
            .lock()
            .unwrap()
            .files
            .insert("old.txt".to_string(), Bytes::from_static(b"rename-source"));
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        for (method, uri, body) in [
            (
                "PUT",
                "/internal/chevalier/vfs/owner-1/file?path=new.txt",
                "new",
            ),
            ("PUT", "/internal/chevalier/vfs/owner-1/dir?path=folder", ""),
            (
                "PUT",
                "/internal/chevalier/vfs/owner-1/symlink?path=link.txt&target=target.txt",
                "",
            ),
            (
                "DELETE",
                "/internal/chevalier/vfs/owner-1/file?path=new.txt",
                "",
            ),
            (
                "DELETE",
                "/internal/chevalier/vfs/owner-1/dir?path=folder",
                "",
            ),
            (
                "POST",
                "/internal/chevalier/vfs/owner-1/rename?from=old.txt&to=renamed.txt",
                "",
            ),
        ] {
            let mut builder = Request::builder()
                .method(method)
                .uri(uri)
                .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                .header(
                    CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                    owner_token.to_string(),
                );
            if method == "DELETE" && uri.contains("/file?") {
                builder = builder
                    .header(CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER, "version-new")
                    .header(
                        CHEVALIER_VFS_PRECONDITION_SECONDARY_FINGERPRINT_HEADER,
                        "secondary-new",
                    );
            }
            let response = app
                .clone()
                .oneshot(builder.body(Body::from(body)).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        }

        let inner = backend.inner.lock().unwrap();
        assert_eq!(inner.writes.len(), 1);
        assert_eq!(inner.deletes.len(), 1);
        assert_eq!(inner.mkdirs.len(), 1);
        assert_eq!(inner.symlinks.len(), 1);
        assert_eq!(inner.rmdirs.len(), 1);
        assert_eq!(inner.renames.len(), 1);
        assert_eq!(inner.symlinks[0].path, "link.txt");
        assert_eq!(inner.symlinks[0].target, "target.txt");
        let delete_precondition = inner.deletes[0]
            .precondition
            .as_ref()
            .expect("delete precondition");
        assert_eq!(
            delete_precondition.fingerprint.as_deref(),
            Some("version-new")
        );
        assert_eq!(
            delete_precondition.secondary_fingerprint.as_deref(),
            Some("secondary-new")
        );
        assert!(inner.files.contains_key("renamed.txt"));
    }

    #[tokio::test]
    async fn gateway_namespace_metadata_mutations_return_json_when_requested() {
        let owner_token = Uuid::new_v4();
        let backend = MemoryBackend::default();
        backend
            .inner
            .lock()
            .unwrap()
            .files
            .insert("old.txt".to_string(), Bytes::from_static(b"rename-source"));
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        let delete_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/internal/chevalier/vfs/owner-1/file?path=gone.txt&return_metadata=true")
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                    .header(
                        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                        owner_token.to_string(),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_response.status(), StatusCode::OK);
        let delete_body = to_bytes(delete_response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&delete_body[..], br#"{"previous":null}"#);

        let rename_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(
                        "/internal/chevalier/vfs/owner-1/rename?from=old.txt&to=renamed.txt&return_metadata=true",
                    )
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                    .header(CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER, owner_token.to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rename_response.status(), StatusCode::OK);
        let rename_body = to_bytes(rename_response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&rename_body[..], br#"{"previous":null,"current":null}"#);

        let inner = backend.inner.lock().unwrap();
        assert_eq!(inner.deletes.len(), 1);
        assert_eq!(inner.renames.len(), 1);
        assert!(inner.files.contains_key("renamed.txt"));
    }

    #[tokio::test]
    async fn gateway_rejects_resource_key_mismatch_before_write() {
        let owner_token = Uuid::new_v4();
        let backend = MemoryBackend::default();
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/internal/chevalier/vfs/owner-1/file?path=asset.bin")
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "wrong")
                    .header(
                        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                        owner_token.to_string(),
                    )
                    .body(Body::from("new"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(backend.inner.lock().unwrap().writes.is_empty());
    }

    #[tokio::test]
    async fn gateway_forwards_setattr_size_operation_header() {
        let owner_token = Uuid::new_v4();
        let backend = MemoryBackend::default();
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/internal/chevalier/vfs/owner-1/file?path=truncated.txt")
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                    .header(
                        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                        owner_token.to_string(),
                    )
                    .header(CHEVALIER_VFS_OPERATION_HEADER, VFS_OPERATION_SETATTR_SIZE)
                    .body(Body::from("shrunk"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let inner = backend.inner.lock().unwrap();
        assert_eq!(
            inner.writes[0].headers.operation,
            VFS_OPERATION_SETATTR_SIZE
        );
    }

    #[tokio::test]
    async fn gateway_rejects_missing_owner_token_before_write() {
        let backend = MemoryBackend::default();
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/internal/chevalier/vfs/owner-1/file?path=asset.bin")
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                    .body(Body::from("new"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(backend.inner.lock().unwrap().writes.is_empty());
    }

    #[tokio::test]
    async fn gateway_surfaces_read_only_and_stale_lease_rejections() {
        let owner_token = Uuid::new_v4();
        let backend = MemoryBackend::default();
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        let readonly = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/internal/chevalier/vfs/owner-1/file?path=readonly/file.txt")
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                    .header(
                        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                        owner_token.to_string(),
                    )
                    .body(Body::from("new"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(readonly.status(), StatusCode::FORBIDDEN);

        let stale = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/internal/chevalier/vfs/owner-1/file?path=stale.txt")
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                    .header(
                        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                        owner_token.to_string(),
                    )
                    .body(Body::from("new"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        assert!(backend.inner.lock().unwrap().writes.is_empty());
    }

    #[tokio::test]
    async fn gateway_lease_round_trip_uses_backend_scope() {
        let backend = MemoryBackend::default();
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/chevalier/vfs/owner-1/lease")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"path":"asset.bin","mutation_count":2}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let grant: VfsLeaseGrant = serde_json::from_slice(&body).unwrap();
        assert_eq!(grant.resource_key, "owner:owner-1:workspace");
        let inner = backend.inner.lock().unwrap();
        assert_eq!(inner.leases[0].mutation_count, 2);
    }

    #[tokio::test]
    async fn gateway_release_lease_forwards_body() {
        let owner_token = Uuid::new_v4();
        let backend = MemoryBackend::default();
        backend
            .inner
            .lock()
            .unwrap()
            .valid_tokens
            .insert(owner_token);
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/internal/chevalier/vfs/owner-1/lease")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{"resource_key":"owner:owner-1:workspace","owner_token":"{owner_token}"}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let inner = backend.inner.lock().unwrap();
        assert_eq!(inner.releases.len(), 1);
        assert!(!inner.valid_tokens.contains(&owner_token));
    }
}
