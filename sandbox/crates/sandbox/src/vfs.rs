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
/// Advertises how a gateway serializes mutations. `implicit` means the
/// mutation endpoint itself owns serialization and lease acquire/release are
/// compatibility no-ops; clients may elide those two HTTP round trips only
/// after observing this header from a successful lease acquisition.
pub const CHEVALIER_VFS_LEASE_MODE_HEADER: &str = "x-chevalier-vfs-lease-mode";
pub const CHEVALIER_VFS_LEASE_MODE_IMPLICIT: &str = "implicit";
pub const CHEVALIER_VFS_PRECONDITION_KIND_HEADER: &str = "x-chevalier-vfs-precondition-kind";
pub const CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER: &str =
    "x-chevalier-vfs-precondition-fingerprint";
pub const CHEVALIER_VFS_PRECONDITION_SECONDARY_FINGERPRINT_HEADER: &str =
    "x-chevalier-vfs-precondition-secondary-fingerprint";
pub const CHEVALIER_VFS_PRECONDITION_FILE_ID_HEADER: &str = "x-chevalier-vfs-precondition-file-id";
pub const CHEVALIER_VFS_EXECUTABLE_HEADER: &str = "x-chevalier-vfs-executable";
pub const CHEVALIER_VFS_MODE_HEADER: &str = "x-chevalier-vfs-mode";
pub const CHEVALIER_VFS_NAMESPACE_REVISION_HEADER: &str = "x-chevalier-vfs-namespace-revision";

pub const VFS_COMPONENT_VM_RUNTIME: &str = "vm_runtime";
pub const VFS_ENTRY_KIND_FILE: &str = "file";
pub const VFS_ENTRY_KIND_DIRECTORY: &str = "directory";
pub const VFS_ENTRY_KIND_SYMLINK: &str = "symlink";
pub const VFS_SURFACE_KIND_VM_SHARED: &str = "vm_shared_vfs";
pub const VFS_SURFACE_KIND_VM_WORKSPACE: &str = "vm_workspace_vfs";
pub const VFS_OPERATION_WRITE_THROUGH: &str = "vfs_write_through";
pub const VFS_OPERATION_SETATTR_SIZE: &str = "vfs_setattr_size";
pub const VFS_OPERATION_SETATTR_MODE: &str = "vfs_setattr_mode";
pub const VFS_OPERATION_MKDIR: &str = "vfs_mkdir";
pub const VFS_OPERATION_UNLINK: &str = "vfs_unlink";
pub const VFS_OPERATION_RMDIR: &str = "vfs_rmdir";
pub const VFS_OPERATION_RENAME: &str = "vfs_rename";
pub const VFS_OPERATION_LINK: &str = "vfs_hard_link";
pub const VFS_OPERATION_SYMLINK: &str = "vfs_symlink";
pub const VFS_OPERATION_NAMESPACE_BATCH: &str = "vfs_namespace_batch";

pub const DEFAULT_VFS_BODY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsDirEntry {
    pub name: String,
    pub kind: String,
    pub size_bytes: u64,
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default = "default_vfs_link_count")]
    pub link_count: u64,
    #[serde(default)]
    pub link_target: Option<String>,
    pub content_hash: Option<String>,
    #[serde(default)]
    pub executable: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_vfs_mode",
        serialize_with = "serialize_vfs_mode"
    )]
    pub mode: Option<u32>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsMetadata {
    pub kind: String,
    pub size_bytes: u64,
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default = "default_vfs_link_count")]
    pub link_count: u64,
    #[serde(default)]
    pub link_target: Option<String>,
    pub content_hash: Option<String>,
    #[serde(default)]
    pub executable: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_vfs_mode",
        serialize_with = "serialize_vfs_mode"
    )]
    pub mode: Option<u32>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

const fn default_vfs_link_count() -> u64 {
    1
}

fn deserialize_vfs_mode<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mode = Option::<u32>::deserialize(deserializer)?;
    if mode.is_some_and(|mode| mode > 0o7777) {
        return Err(serde::de::Error::custom(
            "vfs mode must be an integer between 0 and 0o7777",
        ));
    }
    Ok(mode)
}

fn serialize_vfs_mode<S>(mode: &Option<u32>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    mode.map(|mode| mode & 0o7777).serialize(serializer)
}

fn deserialize_required_vfs_mode<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mode = u32::deserialize(deserializer)?;
    if mode > 0o7777 {
        return Err(serde::de::Error::custom(
            "vfs mode must be an integer between 0 and 0o7777",
        ));
    }
    Ok(mode)
}

fn serialize_required_vfs_mode<S>(mode: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    (*mode & 0o7777).serialize(serializer)
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
pub struct VfsHardLinkBody {
    pub source_path: String,
    pub destination_path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsHardLinkMetadataResponse {
    pub source: VfsMetadata,
    pub destination: VfsMetadata,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsHardLinkAliasBody {
    pub file_id: String,
    pub excluding_path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsHardLinkAliasResponse {
    pub path: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsNamespaceMutationBatchBody {
    pub operation_ids: Vec<String>,
    pub mutations: Vec<VfsNamespaceMutation>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VfsNamespaceMutation {
    CreateFile {
        path: String,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_vfs_mode",
            serialize_with = "serialize_vfs_mode"
        )]
        mode: Option<u32>,
    },
    CreateDirectory {
        path: String,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_vfs_mode",
            serialize_with = "serialize_vfs_mode"
        )]
        mode: Option<u32>,
    },
    CreateSymlink {
        path: String,
        target: String,
    },
    CreateHardLink {
        source_path: String,
        destination_path: String,
    },
    DeleteFile {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        precondition: Option<VfsWritePrecondition>,
    },
    RemoveDirectory {
        path: String,
    },
    Rename {
        from: String,
        to: String,
    },
    SetMode {
        path: String,
        #[serde(
            deserialize_with = "deserialize_required_vfs_mode",
            serialize_with = "serialize_required_vfs_mode"
        )]
        mode: u32,
    },
}

impl VfsNamespaceMutation {
    pub fn paths(&self) -> [&str; 2] {
        match self {
            Self::CreateFile { path, .. }
            | Self::CreateDirectory { path, .. }
            | Self::CreateSymlink { path, .. }
            | Self::DeleteFile { path, .. }
            | Self::RemoveDirectory { path }
            | Self::SetMode { path, .. } => [path.as_str(), ""],
            Self::CreateHardLink {
                source_path,
                destination_path,
            } => [source_path.as_str(), destination_path.as_str()],
            Self::Rename { from, to } => [from.as_str(), to.as_str()],
        }
    }
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
    pub file_id: Option<String>,
    #[serde(default = "default_vfs_link_count")]
    pub link_count: u64,
    #[serde(default)]
    pub link_target: Option<String>,
    pub content_hash: Option<String>,
    #[serde(default)]
    pub executable: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_vfs_mode",
        serialize_with = "serialize_vfs_mode"
    )]
    pub mode: Option<u32>,
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
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VfsCasPredicate {
    Absent,
    ContentFingerprint { fingerprint: String },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsWritePrecondition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<VfsCasPredicate>,
    /// Rolling-upgrade inputs accepted from older gateway clients. New clients
    /// serialize only `predicate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_file_id: Option<String>,
}

impl VfsWritePrecondition {
    pub fn effective_predicate(&self) -> Option<VfsCasPredicate> {
        self.predicate
            .clone()
            .or_else(|| {
                self.fingerprint
                    .as_ref()
                    .or(self.secondary_fingerprint.as_ref())
                    .map(|fingerprint| {
                        if fingerprint == "absent" {
                            VfsCasPredicate::Absent
                        } else {
                            VfsCasPredicate::ContentFingerprint {
                                fingerprint: fingerprint.clone(),
                            }
                        }
                    })
            })
            .or_else(|| {
                self.expected_file_id
                    .is_none()
                    .then_some(VfsCasPredicate::Absent)
            })
    }
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
pub struct VfsPublicationSnapshotEntry {
    pub path: String,
    pub metadata: Option<VfsMetadata>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsNamespaceMutationBatchResponse {
    #[serde(default)]
    pub entries: Vec<VfsPublicationSnapshotEntry>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VfsWriteManyPublicationResponse {
    pub results: Vec<VfsWriteManyResult>,
    #[serde(default)]
    pub entries: Vec<VfsPublicationSnapshotEntry>,
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
    pub executable: Option<bool>,
    pub mode: Option<u32>,
    pub owner_token: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsWriteRequest {
    pub owner_id: String,
    pub path: String,
    pub body: Bytes,
    pub headers: VfsWriteHeaders,
    pub scope: VfsWriteScope,
    pub precondition: Option<VfsWritePrecondition>,
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
pub struct VfsNamespaceMutationBatchRequest {
    pub owner_id: String,
    pub mutations: Vec<VfsNamespaceMutation>,
    pub headers: VfsWriteHeaders,
    pub scope: VfsWriteScope,
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
pub struct VfsHardLinkRequest {
    pub owner_id: String,
    pub source_path: String,
    pub destination_path: String,
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
    use std::{
        collections::{HashMap, HashSet, VecDeque},
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use async_trait::async_trait;
    use axum::{
        Extension, Json, Router,
        body::{Body, Bytes},
        extract::{DefaultBodyLimit, FromRef, Path, Query, State},
        http::{HeaderMap, HeaderValue, StatusCode, header},
        response::{IntoResponse, Response},
        routing::{get, post, put},
    };
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::{
        CHEVALIER_VFS_COMPONENT_HEADER, CHEVALIER_VFS_EXECUTABLE_HEADER,
        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER, CHEVALIER_VFS_MODE_HEADER,
        CHEVALIER_VFS_NAMESPACE_REVISION_HEADER, CHEVALIER_VFS_OPERATION_HEADER,
        CHEVALIER_VFS_PRECONDITION_FILE_ID_HEADER, CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER,
        CHEVALIER_VFS_PRECONDITION_KIND_HEADER,
        CHEVALIER_VFS_PRECONDITION_SECONDARY_FINGERPRINT_HEADER, CHEVALIER_VFS_REASON_HEADER,
        CHEVALIER_VFS_RESOURCE_KEY_HEADER, CHEVALIER_VFS_ROUTE_PREFIX, CHEVALIER_VFS_RUN_ID_HEADER,
        CHEVALIER_VFS_SURFACE_KIND_HEADER, DEFAULT_VFS_BODY_LIMIT_BYTES, VFS_COMPONENT_VM_RUNTIME,
        VFS_ENTRY_KIND_FILE, VFS_OPERATION_LINK, VFS_OPERATION_MKDIR,
        VFS_OPERATION_NAMESPACE_BATCH, VFS_OPERATION_RENAME, VFS_OPERATION_RMDIR,
        VFS_OPERATION_SYMLINK, VFS_OPERATION_UNLINK, VFS_OPERATION_WRITE_THROUGH, VfsCasPredicate,
        VfsDeleteMetadataResponse, VfsDirEntry, VfsGatewayError, VfsHardLinkAliasBody,
        VfsHardLinkAliasResponse, VfsHardLinkBody, VfsHardLinkMetadataResponse, VfsHardLinkRequest,
        VfsHeaderAliases, VfsLeaseAcquire, VfsLeaseAcquireRequest, VfsLeaseGrant,
        VfsLeaseReleaseRequest, VfsListDirOptions, VfsMetadata, VfsMetadataManyRequest,
        VfsMetadataManyResponse, VfsNamespaceMutation, VfsNamespaceMutationBatchBody,
        VfsNamespaceMutationBatchRequest, VfsNamespaceMutationBatchResponse,
        VfsNamespaceMutationRequest, VfsPrefetchSubtreeRequest, VfsPrefetchSubtreeResponse,
        VfsPublicationSnapshotEntry, VfsReadManyRequest, VfsReadManyResponse, VfsReadRange,
        VfsRenameMetadataResponse, VfsRenameRequest, VfsResult, VfsSubtreeMetadataEntry,
        VfsSubtreeMetadataRequest, VfsSubtreeMetadataResponse, VfsSymlinkRequest, VfsWriteHeaders,
        VfsWriteManyBody, VfsWriteManyPublicationResponse, VfsWriteManyRequest,
        VfsWriteManyResult,
        VfsWritePrecondition, VfsWriteRequest, VfsWriteScope, parse_vfs_range_header,
    };

    pub(super) struct VfsPublicationCoordinator {
        owners: Mutex<HashMap<String, Arc<OwnerState>>>,
        /// Hard cap every publication waits for watcher acks before proceeding
        /// fail-open. Read from the environment at construction and threaded
        /// onto each lazily created `OwnerState`.
        ack_timeout: Duration,
    }

    impl Default for VfsPublicationCoordinator {
        fn default() -> Self {
            Self::with_ack_timeout(publication_ack_timeout_from_env())
        }
    }

    impl VfsPublicationCoordinator {
        /// Construct a coordinator with an explicit publication-ack cap. Tests
        /// inject a deterministic cap here; production goes through `default()`
        /// which reads `CHEVALIER_VFS_PUBLICATION_ACK_TIMEOUT_MS`.
        pub(super) fn with_ack_timeout(ack_timeout: Duration) -> Self {
            Self {
                owners: Mutex::new(HashMap::new()),
                ack_timeout,
            }
        }

        pub(super) fn owner(&self, owner_id: &str) -> Arc<OwnerState> {
            let mut owners = self
                .owners
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            Arc::clone(
                owners
                    .entry(owner_id.to_string())
                    .or_insert_with(|| Arc::new(OwnerState::new(self.ack_timeout))),
            )
        }
    }

    /// Hard cap on how long a publication waits for its active watchers to ack
    /// the new revision before proceeding fail-open. Overridable via
    /// `CHEVALIER_VFS_PUBLICATION_ACK_TIMEOUT_MS`. Read once per coordinator
    /// (one per router), never per publication.
    ///
    /// The default is deliberately close to a healthy watcher's round trip
    /// rather than generous. A watcher that acks at all acks within a few
    /// milliseconds on a LAN; a watcher that does not is going to miss the cap
    /// no matter how long it is, and every millisecond of that cap is charged
    /// to the writer's syscall. A generous cap therefore buys no additional
    /// coherence — it only converts one lagging watcher into mount-wide write
    /// latency.
    fn publication_ack_timeout_from_env() -> Duration {
        let millis = std::env::var("CHEVALIER_VFS_PUBLICATION_ACK_TIMEOUT_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(25);
        Duration::from_millis(millis)
    }

    /// A single active watcher's ack progress: the highest revision it has
    /// observed (its most recent poll's `since`) plus a liveness deadline after
    /// which, absent a fresh poll, it is pruned so it can never gate publications
    /// forever.
    struct WatcherAck {
        acked_revision: u64,
        expires_at: tokio::time::Instant,
    }

    /// Per-owner publication state: the serialized namespace revision, a `watch`
    /// side-channel that wakes long-poll `watch` requests, and the ack registry
    /// that makes publications revocation-acked.
    ///
    /// The revision `RwLock` preserves the existing reader/writer publication
    /// gate untouched, so every handler keeps calling `publication.read()` /
    /// `publication.write()` exactly as before. `revision_tx` is a pure
    /// side-channel wake for parked watchers. The ack registry gates a
    /// publication's *HTTP response* (never any lock): a mutation bumps and
    /// publishes the revision under the write guard, releases it, then delays its
    /// response until every registered watcher has re-polled with `since >=` the
    /// new revision (an ack that a fail-closed observer emits only after it has
    /// advanced its fence and cleared its cache).
    pub(super) struct OwnerState {
        revision: tokio::sync::RwLock<u64>,
        revision_tx: tokio::sync::watch::Sender<u64>,
        /// watcher_id -> ack progress. A watcher with no `watcher_id` is
        /// anonymous and never inserted here: it is notified but never gates a
        /// publication.
        acks: Mutex<HashMap<String, WatcherAck>>,
        /// Monotonic counter bumped whenever a watcher records an ack; parked
        /// publications subscribe and re-evaluate on every advance.
        ack_progress_tx: tokio::sync::watch::Sender<u64>,
        /// Micros-since-epoch of the last fail-open WARN, to rate-limit the log
        /// to at most once per second per owner.
        last_ack_warn_us: AtomicU64,
        /// Hard cap for the ack wait, inherited from the coordinator.
        ack_timeout: Duration,
        /// Recent publications as (revision, affected paths), newest last.
        ///
        /// A watcher learns only a revision number from its long poll, which
        /// leaves it no choice but to invalidate everything it has cached — a
        /// revocation proportional to the whole working set rather than to what
        /// actually changed. Retaining the affected paths lets the watch answer
        /// with the exact set instead, so a mount revokes what a publication
        /// touched and nothing else. Bounded: a watcher that has fallen further
        /// behind than this history is told the answer is truncated and falls
        /// back to its own conservative handling.
        publication_history: Mutex<VecDeque<PublishedPaths>>,
    }

    /// One publication's affected paths, retained so a lagging watcher can be
    /// told exactly what to revoke.
    struct PublishedPaths {
        revision: u64,
        paths: Arc<Vec<String>>,
    }

    /// How many publications of affected-path history an owner retains. A
    /// watcher polls continuously, so it is normally one publication behind;
    /// this covers a watcher that missed a burst without letting the history
    /// grow with the mount's lifetime.
    const PUBLICATION_HISTORY_LIMIT: usize = 256;
    /// Ceiling on paths returned for one watch answer. Past this the targeted
    /// answer stops being cheaper than the watcher's own fallback, so the watch
    /// reports truncation instead.
    const WATCH_PATHS_LIMIT: usize = 1024;

    impl OwnerState {
        fn new(ack_timeout: Duration) -> Self {
            let initial = namespace_revision_now();
            let (revision_tx, _) = tokio::sync::watch::channel(initial);
            let (ack_progress_tx, _) = tokio::sync::watch::channel(0u64);
            Self {
                revision: tokio::sync::RwLock::new(initial),
                revision_tx,
                acks: Mutex::new(HashMap::new()),
                ack_progress_tx,
                last_ack_warn_us: AtomicU64::new(0),
                ack_timeout,
                publication_history: Mutex::new(VecDeque::new()),
            }
        }

        /// Retain a publication's affected paths for lagging watchers, evicting
        /// the oldest once the bound is reached.
        fn record_publication(&self, revision: u64, paths: Vec<String>) {
            let mut history = self
                .publication_history
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            history.push_back(PublishedPaths {
                revision,
                paths: Arc::new(paths),
            });
            while history.len() > PUBLICATION_HISTORY_LIMIT {
                history.pop_front();
            }
        }

        /// The union of paths published in `(since, current]`.
        ///
        /// Returns `None` when the answer cannot be trusted to be complete —
        /// the watcher is further behind than the retained history, or the union
        /// exceeds [`WATCH_PATHS_LIMIT`] — in which case the caller reports
        /// truncation and the watcher falls back to its own handling rather than
        /// acting on a partial set.
        fn paths_published_since(&self, since: u64) -> Option<Vec<String>> {
            let history = self
                .publication_history
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let oldest = history.front()?.revision;
            // `since` must be covered: the watcher needs every publication after
            // it, and anything at or before the oldest retained entry may have
            // evicted publications the watcher never saw.
            if since < oldest {
                return None;
            }
            let mut seen = HashSet::new();
            let mut union = Vec::new();
            for entry in history.iter().filter(|entry| entry.revision > since) {
                for path in entry.paths.iter() {
                    if seen.insert(path.as_str()) {
                        union.push(path.clone());
                        if union.len() > WATCH_PATHS_LIMIT {
                            return None;
                        }
                    }
                }
            }
            Some(union)
        }

        /// Acquire a shared read snapshot of the current revision. The name and
        /// signature mirror the previous `RwLock<u64>` so existing call sites
        /// (`publication.read().await`) are unchanged.
        pub(super) async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, u64> {
            self.revision.read().await
        }

        /// Acquire the exclusive publication guard used by every mutation.
        pub(super) async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, u64> {
            self.revision.write().await
        }

        /// Announce a freshly bumped revision to any parked watchers. `send_replace`
        /// keeps the stored value authoritative even when no watcher is currently
        /// subscribed, and never fails or blocks the caller.
        pub(super) fn publish(&self, revision: u64) {
            self.revision_tx.send_replace(revision);
        }

        /// Subscribe to revision advances. The receiver observes the current
        /// version, so a publish that races between a watcher's fast-path check
        /// and its await is still delivered (no lost wakeups).
        pub(super) fn watch(&self) -> tokio::sync::watch::Receiver<u64> {
            self.revision_tx.subscribe()
        }

        /// Consume the write guard to publish a mutation with revocation-ack
        /// semantics: bump the revision, wake parked watchers, RELEASE the guard,
        /// then delay (bounded) until every registered watcher acks the new
        /// revision. The guard is dropped before the wait so the wait holds no
        /// lock — the revision and storage are already published, so this purely
        /// delays the HTTP response (design point 1c). Returns the published
        /// revision for the response header.
        pub(super) async fn commit_and_await_acks(
            &self,
            guard: tokio::sync::RwLockWriteGuard<'_, u64>,
        ) -> u64 {
            self.commit_and_await_acks_for(guard, Vec::new()).await
        }

        /// As [`OwnerState::commit_and_await_acks`], recording `paths` as this
        /// publication's affected set so watchers can revoke precisely.
        pub(super) async fn commit_and_await_acks_for(
            &self,
            mut guard: tokio::sync::RwLockWriteGuard<'_, u64>,
            paths: Vec<String>,
        ) -> u64 {
            *guard = (*guard + 1).max(namespace_revision_now());
            let published = *guard;
            // Record before announcing: a watcher woken by `publish` must never
            // find the history missing the revision it was woken for.
            self.record_publication(published, paths);
            self.publish(published);
            // Release the publication lock BEFORE parking on acks: same-owner
            // ordering stays correct because the revision + storage are already
            // committed, and other owners/readers are never touched by the wait.
            drop(guard);
            self.await_publication_acks(published).await;
            published
        }

        /// Record that `watcher_id` has observed (acked) revision `since` and
        /// refresh its liveness deadline (2x its poll timeout, capped at 60s).
        /// Anonymous watchers (empty id) are never registered. Wakes any
        /// publication parked on ack progress.
        pub(super) fn record_watcher_ack(&self, watcher_id: &str, since: u64, timeout: Duration) {
            if watcher_id.is_empty() {
                return;
            }
            let grace = timeout.saturating_mul(2).min(Duration::from_secs(60));
            let expires_at = tokio::time::Instant::now() + grace;
            {
                let mut acks = self.acks.lock().unwrap_or_else(|error| error.into_inner());
                let entry = acks.entry(watcher_id.to_string()).or_insert(WatcherAck {
                    acked_revision: since,
                    expires_at,
                });
                entry.acked_revision = entry.acked_revision.max(since);
                entry.expires_at = expires_at;
            }
            // Non-blocking wake: watch always marks changed on send, so a parked
            // publication's `changed()` fires and it re-counts laggards.
            self.ack_progress_tx.send_modify(|counter| {
                *counter = counter.wrapping_add(1);
            });
        }

        /// Count registered watchers that have not yet acked `revision`, pruning
        /// any past their liveness deadline first. Zero means every active
        /// watcher has observed the revision (or there are none).
        pub(super) fn unacked_watchers(&self, revision: u64) -> usize {
            let mut acks = self.acks.lock().unwrap_or_else(|error| error.into_inner());
            let now = tokio::time::Instant::now();
            acks.retain(|_, watcher| watcher.expires_at > now);
            acks.values()
                .filter(|watcher| watcher.acked_revision < revision)
                .count()
        }

        /// Block until every registered watcher acks `revision`, bounded by the
        /// coordinator's cap. Returns immediately when no watcher lags (the
        /// single-mount / all-acked fast path). On cap expiry with laggards,
        /// returns fail-open and logs one rate-limited WARN.
        async fn await_publication_acks(&self, revision: u64) {
            // Subscribe before the first count: an ack landing between the count
            // and the await still marks the channel changed, so no wake is lost.
            let mut progress = self.ack_progress_tx.subscribe();
            if self.unacked_watchers(revision) == 0 {
                return;
            }
            if self.ack_timeout.is_zero() {
                return;
            }
            let deadline = tokio::time::Instant::now() + self.ack_timeout;
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    self.warn_publication_lag(revision);
                    return;
                }
                match tokio::time::timeout(remaining, progress.changed()).await {
                    Ok(Ok(())) => {
                        if self.unacked_watchers(revision) == 0 {
                            return;
                        }
                    }
                    // Sender dropped with the owner state: nothing left to wait on.
                    Ok(Err(_)) => return,
                    // Cap elapsed with the revision still unacked: fail open.
                    Err(_) => {
                        self.warn_publication_lag(revision);
                        return;
                    }
                }
            }
        }

        /// Emit at most one WARN per second naming how many watchers failed to
        /// ack in time. Re-counts (and prunes) first so a laggard that acked or
        /// departed at the deadline does not produce a spurious warning.
        fn warn_publication_lag(&self, revision: u64) {
            let laggards = self.unacked_watchers(revision);
            if laggards == 0 {
                return;
            }
            let now_us = namespace_revision_now();
            let last = self.last_ack_warn_us.load(Ordering::Relaxed);
            if now_us.saturating_sub(last) < 1_000_000 {
                return;
            }
            if self
                .last_ack_warn_us
                .compare_exchange(last, now_us, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                tracing::warn!(
                    revision,
                    laggards,
                    "vfs publication ack cap elapsed; proceeding fail-open with unacked watchers"
                );
            }
        }
    }

    fn namespace_revision_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_micros().min(u64::MAX as u128) as u64)
            .unwrap_or_default()
    }

    fn with_namespace_revision(mut response: Response, revision: u64) -> Response {
        if let Ok(value) = HeaderValue::from_str(revision.to_string().as_str()) {
            response
                .headers_mut()
                .insert(CHEVALIER_VFS_NAMESPACE_REVISION_HEADER, value);
        }
        response
    }

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
        async fn set_mode(&self, _request: VfsNamespaceMutationRequest) -> VfsResult<()> {
            Err(VfsGatewayError::BadRequest(
                "gateway backend does not support exact mode changes".to_string(),
            ))
        }
        async fn create_symlink(&self, _request: VfsSymlinkRequest) -> VfsResult<()> {
            Err(VfsGatewayError::BadRequest(
                "gateway backend does not support symlink creation".to_string(),
            ))
        }
        async fn create_hard_link(
            &self,
            _request: VfsHardLinkRequest,
        ) -> VfsResult<VfsHardLinkMetadataResponse> {
            Err(VfsGatewayError::BadRequest(
                "gateway backend does not support hard-link creation".to_string(),
            ))
        }
        async fn find_hard_link_alias(
            &self,
            _owner_id: &str,
            _file_id: &str,
            _excluding_path: &str,
        ) -> VfsResult<Option<String>> {
            Err(VfsGatewayError::BadRequest(
                "gateway backend does not support hard-link alias resolution".to_string(),
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
        async fn apply_namespace_batch(
            &self,
            request: VfsNamespaceMutationBatchRequest,
        ) -> VfsResult<()> {
            for mutation in request.mutations {
                match mutation {
                    VfsNamespaceMutation::CreateFile { path, mode } => {
                        let mut headers = request.headers.clone();
                        headers.mode = mode.map(|mode| mode & 0o7777).or(headers.mode);
                        self.write_file(VfsWriteRequest {
                            owner_id: request.owner_id.clone(),
                            path,
                            body: Bytes::new(),
                            headers,
                            scope: request.scope.clone(),
                            precondition: Some(VfsWritePrecondition {
                                predicate: Some(VfsCasPredicate::Absent),
                                fingerprint: None,
                                secondary_fingerprint: None,
                                expected_file_id: None,
                            }),
                        })
                        .await?;
                    }
                    VfsNamespaceMutation::CreateDirectory { path, mode } => {
                        let mut headers = request.headers.clone();
                        headers.mode = mode.map(|mode| mode & 0o7777).or(headers.mode);
                        self.mkdir(VfsNamespaceMutationRequest {
                            owner_id: request.owner_id.clone(),
                            path,
                            headers,
                            scope: request.scope.clone(),
                            precondition: None,
                        })
                        .await?;
                    }
                    VfsNamespaceMutation::CreateSymlink { path, target } => {
                        self.create_symlink(VfsSymlinkRequest {
                            owner_id: request.owner_id.clone(),
                            path,
                            target,
                            headers: request.headers.clone(),
                            scope: request.scope.clone(),
                        })
                        .await?;
                    }
                    VfsNamespaceMutation::CreateHardLink {
                        source_path,
                        destination_path,
                    } => {
                        self.create_hard_link(VfsHardLinkRequest {
                            owner_id: request.owner_id.clone(),
                            source_path,
                            destination_path,
                            headers: request.headers.clone(),
                            scope: request.scope.clone(),
                        })
                        .await?;
                    }
                    VfsNamespaceMutation::DeleteFile { path, precondition } => {
                        self.delete_file(VfsNamespaceMutationRequest {
                            owner_id: request.owner_id.clone(),
                            path,
                            headers: request.headers.clone(),
                            scope: request.scope.clone(),
                            precondition,
                        })
                        .await?;
                    }
                    VfsNamespaceMutation::RemoveDirectory { path } => {
                        self.rmdir(VfsNamespaceMutationRequest {
                            owner_id: request.owner_id.clone(),
                            path,
                            headers: request.headers.clone(),
                            scope: request.scope.clone(),
                            precondition: None,
                        })
                        .await?;
                    }
                    VfsNamespaceMutation::Rename { from, to } => {
                        self.rename(VfsRenameRequest {
                            owner_id: request.owner_id.clone(),
                            from,
                            to,
                            headers: request.headers.clone(),
                            scope: request.scope.clone(),
                        })
                        .await?;
                    }
                    VfsNamespaceMutation::SetMode { path, mode } => {
                        let mut headers = request.headers.clone();
                        headers.mode = Some(mode & 0o7777);
                        self.set_mode(VfsNamespaceMutationRequest {
                            owner_id: request.owner_id.clone(),
                            path,
                            headers,
                            scope: request.scope.clone(),
                            precondition: None,
                        })
                        .await?;
                    }
                }
            }
            Ok(())
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
        vfs_routes_with_coordinator::<S, B>(
            owner_route_prefix,
            Arc::new(VfsPublicationCoordinator::default()),
        )
    }

    /// Build the owner-scoped VFS routes over an explicit publication
    /// coordinator. Production goes through `vfs_routes` (env-configured cap);
    /// tests use this to inject a deterministic ack cap.
    pub(super) fn vfs_routes_with_coordinator<S, B>(
        owner_route_prefix: &str,
        publications: Arc<VfsPublicationCoordinator>,
    ) -> Router<S>
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
                &format!("{prefix}/{{owner_id}}/watch"),
                get(get_watch::<S, B>),
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
                &format!("{prefix}/{{owner_id}}/namespace-many"),
                post(post_namespace_many::<S, B>),
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
                &format!("{prefix}/{{owner_id}}/hard-link/v1"),
                post(post_hard_link::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/hard-link-alias/v1"),
                post(post_hard_link_alias::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/rename"),
                post(post_rename::<S, B>),
            )
            .route(
                &format!("{prefix}/{{owner_id}}/lease"),
                post(post_lease::<S, B>).delete(delete_lease::<S, B>),
            )
            .layer(Extension(publications))
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

    const WATCH_TIMEOUT_MIN_MS: i64 = 1_000;
    const WATCH_TIMEOUT_MAX_MS: i64 = 30_000;
    const WATCH_TIMEOUT_DEFAULT_MS: u64 = 25_000;

    #[derive(Debug, Deserialize)]
    struct WatchQuery {
        since: Option<String>,
        timeout_ms: Option<String>,
        /// Opaque, stable per-observer identity. Present -> the poll's `since`
        /// acks that revision and the watcher is registered for ack-blocking.
        /// Absent -> the watcher is anonymous: notified, never ack-gating.
        watcher_id: Option<String>,
    }

    /// Trimmed, non-empty `watcher_id`, or "" for an anonymous (unregistered)
    /// watcher.
    fn watch_watcher_id(raw: Option<&str>) -> &str {
        raw.map(str::trim).filter(|text| !text.is_empty()).unwrap_or("")
    }

    #[derive(Serialize)]
    struct WatchResponse {
        revision: u64,
        /// Paths published in `(since, revision]`. A watcher revokes exactly
        /// these instead of everything it has cached.
        ///
        /// Always serialized when the set is known, including when it is empty:
        /// an omitted field means "this gateway does not report affected paths"
        /// and must send the watcher down its conservative fallback, which is a
        /// different statement from "this publication touched nothing".
        paths: Vec<String>,
        /// Set when the affected set could not be reported completely (the
        /// watcher is behind the retained history, or the set is too large).
        /// The watcher must not treat `paths` as exhaustive and falls back to
        /// its own conservative revocation.
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        truncated: bool,
    }

    /// `since` absent/invalid -> 0 (so the fast path answers immediately with the
    /// current revision). Parsed as a plain decimal `u64` to match the wire type.
    fn watch_since(raw: Option<&str>) -> u64 {
        raw.map(str::trim)
            .filter(|text| !text.is_empty())
            .and_then(|text| text.parse::<u64>().ok())
            .unwrap_or(0)
    }

    /// `timeout_ms` clamps to [1000, 30000]; absent or non-integer -> 25000.
    fn watch_timeout(raw: Option<&str>) -> std::time::Duration {
        let millis = raw
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .and_then(|text| text.parse::<i64>().ok())
            .map(|value| value.clamp(WATCH_TIMEOUT_MIN_MS, WATCH_TIMEOUT_MAX_MS) as u64)
            .unwrap_or(WATCH_TIMEOUT_DEFAULT_MS);
        std::time::Duration::from_millis(millis)
    }

    fn watch_hit(revision: u64, affected: Option<Vec<String>>) -> Response {
        let (paths, truncated) = match affected {
            Some(paths) => (paths, false),
            None => (Vec::new(), true),
        };
        with_namespace_revision(
            (
                StatusCode::OK,
                Json(WatchResponse {
                    revision,
                    paths,
                    truncated,
                }),
            )
                .into_response(),
            revision,
        )
    }

    fn watch_idle(revision: u64) -> Response {
        with_namespace_revision(StatusCode::NO_CONTENT.into_response(), revision)
    }

    /// Long-poll until the owner's namespace revision advances past `since`.
    ///
    /// Fast path: when the current revision already exceeds `since`, answer 200
    /// immediately with `{"revision": <current>}`. Otherwise park on the owner's
    /// watch channel and answer 200 the moment any mutation advances the revision,
    /// or 204 once `timeout_ms` elapses with the revision still <= `since`. Both
    /// responses stamp the standard namespace-revision header. The watcher only
    /// holds the read guard for the microsecond revision peek on each turn and is
    /// otherwise parked on the watch channel, so it never slows a concurrent
    /// mutation (whose sole added cost is the publisher's `publish` notify).
    async fn get_watch<S, B>(
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Query(params): Query<WatchQuery>,
    ) -> Response
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let since = watch_since(params.since.as_deref());
        let timeout = watch_timeout(params.timeout_ms.as_deref());
        let watcher_id = watch_watcher_id(params.watcher_id.as_deref());
        let publication = publications.owner(owner_id.as_str());
        // This poll's `since` is an ack that this watcher has observed (and, on
        // the client, already fence-advanced + cache-cleared through) `since`.
        // Register it at entry so a concurrent publication that is waiting for
        // this watcher unblocks the moment the re-poll lands.
        publication.record_watcher_ack(watcher_id, since, timeout);
        // Subscribe BEFORE the first revision read: a publish landing between the
        // read and the await still bumps the watch version, so `changed()` fires
        // immediately rather than being lost.
        let mut receiver = publication.watch();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let current = *publication.read().await;
            if current > since {
                // Read the affected set under the same read guard that produced
                // `current`, so the paths answered always cover exactly the
                // revisions the watcher is being advanced across.
                return watch_hit(current, publication.paths_published_since(since));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return watch_idle(current);
            }
            match tokio::time::timeout(remaining, receiver.changed()).await {
                Ok(Ok(())) => {
                    // A publisher advanced the revision; loop to re-read the
                    // authoritative value under the read guard.
                    receiver.borrow_and_update();
                }
                Ok(Err(_)) => {
                    // The owner state (hence the sender) was dropped; report the
                    // last known revision as an idle result.
                    return watch_idle(*publication.read().await);
                }
                Err(_) => {
                    // timeout_ms elapsed with the revision still <= since.
                    return watch_idle(*publication.read().await);
                }
            }
        }
    }

    async fn get_tree<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Query(params): Query<TreeQuery>,
    ) -> Response
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let path = params.path.as_deref().unwrap_or_default();
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.read().await;
        let response = backend
            .list_dir_with_options(owner_id.as_str(), path, params.options())
            .await
            .map(Json)
            .map(IntoResponse::into_response)
            .unwrap_or_else(IntoResponse::into_response);
        with_namespace_revision(response, *revision)
    }

    async fn get_stat<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Query(params): Query<PathQuery>,
    ) -> Response
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let path = params.path.as_deref().unwrap_or_default();
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.read().await;
        let response = backend
            .stat(owner_id.as_str(), path)
            .await
            .map(Json)
            .map(IntoResponse::into_response)
            .unwrap_or_else(IntoResponse::into_response);
        with_namespace_revision(response, *revision)
    }

    async fn post_metadata_many<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsMetadataManyRequest>,
    ) -> Response
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.read().await;
        let response = backend
            .metadata_many(owner_id.as_str(), body.paths.as_slice())
            .await
            .map(|entries| Json(VfsMetadataManyResponse { entries }))
            .map(IntoResponse::into_response)
            .unwrap_or_else(IntoResponse::into_response);
        with_namespace_revision(response, *revision)
    }

    async fn post_read_many<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsReadManyRequest>,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.read().await;
        let entries = backend
            .read_many(owner_id.as_str(), body.paths.as_slice())
            .await?
            .into_iter()
            .map(|entry| entry.map(|bytes| bytes.to_vec()))
            .collect();
        Ok(with_namespace_revision(
            Json(VfsReadManyResponse { entries }).into_response(),
            *revision,
        ))
    }

    async fn post_subtree_metadata<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsSubtreeMetadataRequest>,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.read().await;
        let entries = backend
            .list_subtree_file_metadata(owner_id.as_str(), body)
            .await?;
        Ok(with_namespace_revision(
            Json(VfsSubtreeMetadataResponse { entries }).into_response(),
            *revision,
        ))
    }

    async fn post_prefetch_subtree<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsPrefetchSubtreeRequest>,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.read().await;
        let response = backend.prefetch_subtree(owner_id.as_str(), body).await?;
        Ok(with_namespace_revision(
            Json(response).into_response(),
            *revision,
        ))
    }

    async fn post_write_many<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        headers: HeaderMap,
        Json(body): Json<VfsWriteManyBody>,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        if body.writes.is_empty() {
            let publication = publications.owner(owner_id.as_str());
            let revision = publication.read().await;
            return Ok(with_namespace_revision(
                Json(VfsWriteManyPublicationResponse {
                    results: Vec::new(),
                    entries: Vec::new(),
                })
                .into_response(),
                *revision,
            ));
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
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.write().await;
        let snapshot_paths = body
            .writes
            .iter()
            .map(|write| write.path.clone())
            .collect::<Vec<_>>();
        let results = backend
            .write_many_atomic(VfsWriteManyRequest {
                owner_id: owner_id.clone(),
                writes: body.writes,
                headers: write_headers,
                scope: first_scope,
            })
            .await?;
        let affected = snapshot_paths.clone();
        let entries = publication_snapshot(&backend, owner_id.as_str(), snapshot_paths).await?;
        let published = publication
            .commit_and_await_acks_for(revision, affected)
            .await;
        Ok(with_namespace_revision(
            Json(VfsWriteManyPublicationResponse { results, entries }).into_response(),
            published,
        ))
    }

    async fn post_namespace_many<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        headers: HeaderMap,
        Json(body): Json<VfsNamespaceMutationBatchBody>,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        if body.operation_ids.len() != body.mutations.len() {
            return Err(VfsGatewayError::BadRequest(
                "namespace-many requires one operation_id per mutation".to_string(),
            ));
        }
        let mut operation_ids = HashSet::with_capacity(body.operation_ids.len());
        for operation_id in &body.operation_ids {
            if operation_id.trim().is_empty() || !operation_ids.insert(operation_id) {
                return Err(VfsGatewayError::BadRequest(
                    "namespace-many operation_ids must be non-empty and unique".to_string(),
                ));
            }
        }
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.write().await;
        if body.mutations.is_empty() {
            return Ok(with_namespace_revision(
                StatusCode::NO_CONTENT.into_response(),
                *revision,
            ));
        }
        if body.mutations.len() > 4096 {
            return Err(VfsGatewayError::BadRequest(
                "namespace-many accepts at most 4096 mutations".to_string(),
            ));
        }
        let first_path = body.mutations[0]
            .paths()
            .into_iter()
            .find(|path| !path.is_empty())
            .ok_or_else(|| {
                VfsGatewayError::BadRequest("namespace mutation has no path".to_string())
            })?;
        let first_scope = backend
            .derive_write_scope(owner_id.as_str(), first_path)
            .await?;
        for path in body
            .mutations
            .iter()
            .flat_map(VfsNamespaceMutation::paths)
            .filter(|path| !path.is_empty())
        {
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
            VFS_OPERATION_NAMESPACE_BATCH,
        )?;
        validate_declared_resource_key(&headers, &aliases, first_scope.resource_key.as_str())?;
        let snapshot_paths = namespace_snapshot_paths(body.mutations.as_slice());
        backend
            .apply_namespace_batch(VfsNamespaceMutationBatchRequest {
                owner_id: owner_id.clone(),
                mutations: body.mutations,
                headers: write_headers,
                scope: first_scope,
            })
            .await?;
        let affected = snapshot_paths.clone();
        let entries = publication_snapshot(&backend, owner_id.as_str(), snapshot_paths).await?;
        let published = publication
            .commit_and_await_acks_for(revision, affected)
            .await;
        Ok(with_namespace_revision(
            Json(VfsNamespaceMutationBatchResponse { entries }).into_response(),
            published,
        ))
    }

    fn namespace_snapshot_paths(mutations: &[VfsNamespaceMutation]) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut paths = Vec::new();
        for mutation in mutations {
            for path in mutation.paths().into_iter().filter(|path| !path.is_empty()) {
                for candidate in [path.to_string(), immediate_parent(path)] {
                    if seen.insert(candidate.clone()) {
                        paths.push(candidate);
                    }
                }
            }
        }
        paths
    }

    fn immediate_parent(path: &str) -> String {
        path.trim_matches('/')
            .rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
            .unwrap_or_default()
    }

    async fn publication_snapshot<B>(
        backend: &B,
        owner_id: &str,
        paths: Vec<String>,
    ) -> VfsResult<Vec<VfsPublicationSnapshotEntry>>
    where
        B: VfsGatewayBackend,
    {
        let metadata = backend.metadata_many(owner_id, paths.as_slice()).await?;
        if metadata.len() != paths.len() {
            return Err(VfsGatewayError::Internal(format!(
                "publication snapshot returned {} entries for {} paths",
                metadata.len(),
                paths.len()
            )));
        }
        Ok(paths
            .into_iter()
            .zip(metadata)
            .map(|(path, metadata)| VfsPublicationSnapshotEntry { path, metadata })
            .collect())
    }

    async fn get_file_raw<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Query(params): Query<PathQuery>,
        headers: HeaderMap,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let path = required_path(params.path.as_deref())?;
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.read().await;
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
        Ok(with_namespace_revision(response, *revision))
    }

    async fn put_file<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Query(params): Query<PathQuery>,
        headers: HeaderMap,
        body: Bytes,
    ) -> VfsResult<Response>
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
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.write().await;
        backend
            .write_file(VfsWriteRequest {
                owner_id,
                path: path.to_string(),
                body,
                headers: write_headers,
                scope,
                precondition: parse_write_precondition_headers(&headers)?,
            })
            .await?;
        let published = publication.commit_and_await_acks(revision).await;
        Ok(with_namespace_revision(
            StatusCode::NO_CONTENT.into_response(),
            published,
        ))
    }

    async fn delete_file<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
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
        let publication = publications.owner(request.owner_id.as_str());
        let revision = publication.write().await;
        let response = if params.return_metadata.unwrap_or(false) {
            let response = backend.delete_file_with_metadata(request).await?;
            (StatusCode::OK, Json(response)).into_response()
        } else {
            backend.delete_file(request).await?;
            StatusCode::NO_CONTENT.into_response()
        };
        let published = publication.commit_and_await_acks(revision).await;
        Ok(with_namespace_revision(response, published))
    }

    async fn put_dir<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
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
            VFS_OPERATION_MKDIR,
        )
        .await?;
        let publication = publications.owner(request.owner_id.as_str());
        let revision = publication.write().await;
        backend.mkdir(request).await?;
        let published = publication.commit_and_await_acks(revision).await;
        Ok(with_namespace_revision(
            StatusCode::NO_CONTENT.into_response(),
            published,
        ))
    }

    async fn put_symlink<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Query(params): Query<SymlinkQuery>,
        headers: HeaderMap,
    ) -> VfsResult<Response>
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
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.write().await;
        backend
            .create_symlink(VfsSymlinkRequest {
                owner_id,
                path: path.to_string(),
                target: params.target,
                headers: write_headers,
                scope,
            })
            .await?;
        let published = publication.commit_and_await_acks(revision).await;
        Ok(with_namespace_revision(
            StatusCode::NO_CONTENT.into_response(),
            published,
        ))
    }

    async fn delete_dir<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
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
            VFS_OPERATION_RMDIR,
        )
        .await?;
        let publication = publications.owner(request.owner_id.as_str());
        let revision = publication.write().await;
        backend.rmdir(request).await?;
        let published = publication.commit_and_await_acks(revision).await;
        Ok(with_namespace_revision(
            StatusCode::NO_CONTENT.into_response(),
            published,
        ))
    }

    async fn post_hard_link<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        headers: HeaderMap,
        Json(body): Json<VfsHardLinkBody>,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        let source_path = required_path(Some(body.source_path.as_str()))?;
        let destination_path = required_path(Some(body.destination_path.as_str()))?;
        let source_scope = backend
            .derive_write_scope(owner_id.as_str(), source_path)
            .await?;
        let destination_scope = backend
            .derive_write_scope(owner_id.as_str(), destination_path)
            .await?;
        if source_scope.resource_key != destination_scope.resource_key {
            return Err(VfsGatewayError::Conflict(
                backend.cross_scope_rename_message(),
            ));
        }
        let aliases = backend.header_aliases();
        let write_headers = parse_write_headers(
            &headers,
            &aliases,
            source_scope.default_surface_kind.as_str(),
            VFS_OPERATION_LINK,
        )?;
        validate_declared_resource_key(&headers, &aliases, source_scope.resource_key.as_str())?;
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.write().await;
        let response = backend
            .create_hard_link(VfsHardLinkRequest {
                owner_id,
                source_path: source_path.to_string(),
                destination_path: destination_path.to_string(),
                headers: write_headers,
                scope: source_scope,
            })
            .await?;
        let published = publication.commit_and_await_acks(revision).await;
        Ok(with_namespace_revision(
            Json(response).into_response(),
            published,
        ))
    }

    async fn post_hard_link_alias<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
        Path(owner_id): Path<String>,
        Json(body): Json<VfsHardLinkAliasBody>,
    ) -> VfsResult<Response>
    where
        B: VfsGatewayBackend + FromRef<S>,
        S: Clone + Send + Sync + 'static,
    {
        if body.file_id.trim().is_empty() {
            return Err(VfsGatewayError::BadRequest(
                "hard-link alias resolution requires file_id".to_string(),
            ));
        }
        let publication = publications.owner(owner_id.as_str());
        let revision = publication.read().await;
        let path = backend
            .find_hard_link_alias(
                owner_id.as_str(),
                body.file_id.as_str(),
                body.excluding_path.as_str(),
            )
            .await?;
        Ok(with_namespace_revision(
            Json(VfsHardLinkAliasResponse { path }).into_response(),
            *revision,
        ))
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
            precondition: parse_write_precondition_headers(headers)?,
        })
    }

    fn parse_write_precondition_headers(
        headers: &HeaderMap,
    ) -> VfsResult<Option<VfsWritePrecondition>> {
        let kind = header_value(headers, CHEVALIER_VFS_PRECONDITION_KIND_HEADER, &[]);
        let fingerprint = header_value(headers, CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER, &[]);
        let secondary_fingerprint = header_value(
            headers,
            CHEVALIER_VFS_PRECONDITION_SECONDARY_FINGERPRINT_HEADER,
            &[],
        );
        let expected_file_id =
            header_value(headers, CHEVALIER_VFS_PRECONDITION_FILE_ID_HEADER, &[]);
        let predicate = match kind.as_deref() {
            Some("absent") => {
                if fingerprint.is_some() || secondary_fingerprint.is_some() {
                    return Err(VfsGatewayError::BadRequest(
                        "absent VFS precondition cannot include a fingerprint".to_string(),
                    ));
                }
                Some(VfsCasPredicate::Absent)
            }
            Some("content_fingerprint") => {
                let fingerprint = fingerprint.as_ref().ok_or_else(|| {
                    VfsGatewayError::BadRequest(
                        "content_fingerprint VFS precondition requires a fingerprint".to_string(),
                    )
                })?;
                Some(VfsCasPredicate::ContentFingerprint {
                    fingerprint: fingerprint.clone(),
                })
            }
            Some(other) => {
                return Err(VfsGatewayError::BadRequest(format!(
                    "unsupported VFS precondition kind: {other}"
                )));
            }
            None => fingerprint
                .as_ref()
                .or(secondary_fingerprint.as_ref())
                .map(|fingerprint| {
                    if fingerprint == "absent" {
                        VfsCasPredicate::Absent
                    } else {
                        VfsCasPredicate::ContentFingerprint {
                            fingerprint: fingerprint.clone(),
                        }
                    }
                }),
        };
        Ok(
            (predicate.is_some() || expected_file_id.is_some()).then_some(VfsWritePrecondition {
                predicate,
                fingerprint,
                secondary_fingerprint,
                expected_file_id,
            }),
        )
    }

    async fn post_rename<S, B>(
        State(backend): State<B>,
        Extension(publications): Extension<Arc<VfsPublicationCoordinator>>,
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
        let publication = publications.owner(request.owner_id.as_str());
        let revision = publication.write().await;
        let response = if return_metadata {
            let response = backend.rename_with_metadata(request).await?;
            (StatusCode::OK, Json(response)).into_response()
        } else {
            backend.rename(request).await?;
            StatusCode::NO_CONTENT.into_response()
        };
        let published = publication.commit_and_await_acks(revision).await;
        Ok(with_namespace_revision(response, published))
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
            executable: header_value(headers, CHEVALIER_VFS_EXECUTABLE_HEADER, &[])
                .map(|value| match value.as_str() {
                    "true" => Ok(true),
                    "false" => Ok(false),
                    _ => Err(VfsGatewayError::BadRequest(format!(
                        "invalid {CHEVALIER_VFS_EXECUTABLE_HEADER}: {value}"
                    ))),
                })
                .transpose()?,
            mode: header_value(headers, CHEVALIER_VFS_MODE_HEADER, &[])
                .map(|value| {
                    let mode = value.parse::<u32>().map_err(|_| {
                        VfsGatewayError::BadRequest(format!(
                            "invalid {CHEVALIER_VFS_MODE_HEADER}: expected an integer from 0 to 4095"
                        ))
                    })?;
                    if mode > 0o7777 {
                        return Err(VfsGatewayError::BadRequest(format!(
                            "invalid {CHEVALIER_VFS_MODE_HEADER}: expected an integer from 0 to 4095"
                        )));
                    }
                    Ok(mode)
                })
                .transpose()?,
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
    use super::{
        VfsGatewayError, VfsMetadata, VfsNamespaceMutation, VfsSubtreeMetadataEntry,
        owner_vfs_endpoint, parse_vfs_range_header, scoped_vfs_path,
    };

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
    fn metadata_without_exact_mode_retains_legacy_executable_fallback() {
        let metadata: VfsMetadata = serde_json::from_value(serde_json::json!({
            "kind": "file",
            "size_bytes": 1,
            "content_hash": null,
            "executable": true,
            "updated_at": null,
        }))
        .unwrap();

        assert_eq!(metadata.mode, None);
        assert!(metadata.executable);
    }

    #[test]
    fn metadata_exact_mode_is_accepted_on_decode_and_masked_on_trusted_encode() {
        let metadata: VfsMetadata = serde_json::from_value(serde_json::json!({
            "kind": "file",
            "size_bytes": 1,
            "content_hash": null,
            "executable": true,
            "mode": 0o755,
            "updated_at": null,
        }))
        .unwrap();
        assert_eq!(metadata.mode, Some(0o755));

        let mut metadata = metadata;
        metadata.mode = Some(0o106755);
        assert_eq!(serde_json::to_value(metadata).unwrap()["mode"], 0o6755);
    }

    #[test]
    fn subtree_metadata_supports_legacy_executable_and_optional_exact_mode() {
        let legacy: VfsSubtreeMetadataEntry = serde_json::from_value(serde_json::json!({
            "path": "script",
            "kind": "file",
            "size_bytes": 1,
            "content_hash": null,
            "token_count": null,
            "version": null,
            "updated_at": null,
            "object_state": null,
        }))
        .unwrap();
        assert!(!legacy.executable);
        assert_eq!(legacy.mode, None);

        let exact: VfsSubtreeMetadataEntry = serde_json::from_value(serde_json::json!({
            "path": "script",
            "kind": "file",
            "size_bytes": 1,
            "content_hash": null,
            "executable": true,
            "mode": 0o6755,
            "token_count": null,
            "version": null,
            "updated_at": null,
            "object_state": null,
        }))
        .unwrap();
        assert!(exact.executable);
        assert_eq!(exact.mode, Some(0o6755));
    }

    #[test]
    fn directory_mutation_mode_is_optional_and_exact() {
        let legacy: VfsNamespaceMutation = serde_json::from_value(serde_json::json!({
            "kind": "create_directory",
            "path": "tree",
        }))
        .unwrap();
        assert_eq!(
            legacy,
            VfsNamespaceMutation::CreateDirectory {
                path: "tree".to_string(),
                mode: None,
            }
        );

        let exact: VfsNamespaceMutation = serde_json::from_value(serde_json::json!({
            "kind": "create_directory",
            "path": "tree",
            "mode": 0o4775,
        }))
        .unwrap();
        assert_eq!(
            exact,
            VfsNamespaceMutation::CreateDirectory {
                path: "tree".to_string(),
                mode: Some(0o4775),
            }
        );
    }

    #[test]
    fn set_mode_mutation_accepts_exact_permission_bits() {
        let mutation: VfsNamespaceMutation = serde_json::from_value(serde_json::json!({
            "kind": "set_mode",
            "path": "script",
            "mode": 0o6755,
        }))
        .unwrap();
        assert_eq!(
            mutation,
            VfsNamespaceMutation::SetMode {
                path: "script".to_string(),
                mode: 0o6755,
            }
        );
        assert_eq!(serde_json::to_value(mutation).unwrap()["mode"], 0o6755);
    }

    #[test]
    fn external_exact_modes_reject_negative_or_out_of_range_values() {
        assert!(
            serde_json::from_value::<VfsMetadata>(serde_json::json!({
                "kind": "file",
                "size_bytes": 1,
                "content_hash": null,
                "mode": 0o100755,
                "updated_at": null,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<VfsNamespaceMutation>(serde_json::json!({
                "kind": "create_directory",
                "path": "tree",
                "mode": -1,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<VfsNamespaceMutation>(serde_json::json!({
                "kind": "set_mode",
                "path": "script",
                "mode": 0o100755,
            }))
            .is_err()
        );
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
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };

    use async_trait::async_trait;
    use axum::body::{Body, Bytes, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;
    use uuid::Uuid;

    use std::time::Duration;

    use super::server::{VfsPublicationCoordinator, vfs_routes_with_coordinator};
    use super::{
        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER, CHEVALIER_VFS_MODE_HEADER,
        CHEVALIER_VFS_NAMESPACE_REVISION_HEADER, CHEVALIER_VFS_OPERATION_HEADER,
        CHEVALIER_VFS_PRECONDITION_FILE_ID_HEADER, CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER,
        CHEVALIER_VFS_PRECONDITION_SECONDARY_FINGERPRINT_HEADER, CHEVALIER_VFS_RESOURCE_KEY_HEADER,
        CHEVALIER_VFS_ROUTE_PREFIX,
        VFS_ENTRY_KIND_DIRECTORY, VFS_ENTRY_KIND_FILE, VFS_OPERATION_SETATTR_SIZE,
        VFS_SURFACE_KIND_VM_WORKSPACE, VfsDirEntry, VfsGatewayBackend, VfsGatewayError,
        VfsLeaseAcquire, VfsLeaseGrant, VfsLeaseReleaseRequest, VfsMetadata,
        VfsMetadataManyResponse, VfsNamespaceMutation, VfsNamespaceMutationBatchRequest,
        VfsNamespaceMutationBatchResponse, VfsNamespaceMutationRequest, VfsReadManyResponse,
        VfsReadRange, VfsRenameRequest,
        VfsResult, VfsSymlinkRequest, VfsWriteManyRequest, VfsWriteManyResponse,
        VfsWriteManyResult, VfsWritePrecondition, VfsWriteRequest, VfsWriteScope,
        chevalier_vfs_routes,
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
        namespace_batches: Vec<VfsNamespaceMutationBatchRequest>,
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

    #[tokio::test]
    async fn publication_coordinator_allows_concurrent_read_snapshots() {
        let coordinator = VfsPublicationCoordinator::default();
        let publication = coordinator.owner("owner");
        let first = publication.read().await;
        let second =
            tokio::time::timeout(std::time::Duration::from_millis(100), publication.read())
                .await
                .expect("a second reader must not wait behind the first");
        assert_eq!(*first, *second);

        let writer_publication = Arc::clone(&publication);
        let (acquired_tx, mut acquired_rx) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn(async move {
            let _guard = writer_publication.write().await;
            let _ = acquired_tx.send(());
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut acquired_rx)
                .await
                .is_err(),
            "a writer must wait until every stable read snapshot completes"
        );
        drop(second);
        drop(first);
        tokio::time::timeout(std::time::Duration::from_millis(100), writer)
            .await
            .expect("the publication gate must release after readers complete")
            .expect("writer task must complete");
        acquired_rx
            .await
            .expect("writer must acquire the publication gate");
    }

    fn namespace_revision(response: &axum::http::Response<Body>) -> u64 {
        response
            .headers()
            .get(CHEVALIER_VFS_NAMESPACE_REVISION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .expect("namespace revision header must be present and numeric")
    }

    #[tokio::test]
    async fn watch_returns_immediately_when_revision_exceeds_since() {
        let backend = MemoryBackend::default();
        let app = chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend);

        // A fresh owner starts at a large microsecond revision, so since=0 must
        // resolve immediately with the current revision in both body and header.
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/watch?since=0&timeout_ms=1000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let revision = namespace_revision(&response);
        assert!(revision > 0, "fresh owner revision must exceed since=0");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["revision"].as_u64(), Some(revision));
    }

    #[tokio::test]
    async fn watch_resolves_promptly_when_a_mutation_publishes() {
        let backend = MemoryBackend::default();
        let app = chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend);

        // Read the baseline revision so the watcher can park exactly at it.
        let seed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/tree?path=")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let baseline = namespace_revision(&seed);

        // Park a watcher at the baseline with a generous timeout.
        let watch_app = app.clone();
        let watcher = tokio::spawn(async move {
            watch_app
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/internal/chevalier/vfs/owner-1/watch?since={baseline}&timeout_ms=30000"
                        ))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        });

        // Let the watcher reach its parked state before publishing.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let owner_token = Uuid::new_v4();
        let mutation = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/internal/chevalier/vfs/owner-1/dir?path=folder")
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                    .header(CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER, owner_token.to_string())
                    .header(CHEVALIER_VFS_MODE_HEADER, 0o750.to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mutation.status(), StatusCode::NO_CONTENT);
        let mutated = namespace_revision(&mutation);
        assert!(mutated > baseline, "mutation must advance the revision");

        // The parked watcher must resolve promptly after the publish.
        let response = tokio::time::timeout(std::time::Duration::from_millis(500), watcher)
            .await
            .expect("parked watcher must resolve within 500ms of the publish")
            .expect("watch task must not panic");
        assert_eq!(response.status(), StatusCode::OK);
        let woke_revision = namespace_revision(&response);
        assert!(woke_revision > baseline);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["revision"].as_u64(), Some(woke_revision));
    }

    #[tokio::test]
    async fn watch_times_out_with_204_when_revision_is_unchanged() {
        let backend = MemoryBackend::default();
        let app = chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend);

        // since=u64::MAX can never be exceeded, so the watcher parks and then
        // times out at the clamped 1s floor with the revision still unchanged.
        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/internal/chevalier/vfs/owner-1/watch\
                         ?since=18446744073709551615&timeout_ms=1000",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let revision = namespace_revision(&response);
        assert!(
            revision < u64::MAX,
            "204 must stamp the unchanged current revision"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty(), "a 204 response carries no body");
    }

    #[tokio::test]
    async fn watch_does_not_block_a_concurrent_mutation() {
        let backend = MemoryBackend::default();
        let app = chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend);

        let seed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/tree?path=")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let baseline = namespace_revision(&seed);

        // Park a watcher with a 30s timeout; it must never gate the mutation.
        let watch_app = app.clone();
        let watcher = tokio::spawn(async move {
            watch_app
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/internal/chevalier/vfs/owner-1/watch?since={baseline}&timeout_ms=30000"
                        ))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // A blocked mutation would stall until the 30s watch timeout; bounding it
        // at 500ms proves the parked watcher does not hold the publication gate.
        let owner_token = Uuid::new_v4();
        let mutation = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            app.clone().oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/internal/chevalier/vfs/owner-1/dir?path=folder")
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                    .header(CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER, owner_token.to_string())
                    .header(CHEVALIER_VFS_MODE_HEADER, 0o750.to_string())
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .expect("a parked watcher must not block a concurrent mutation")
        .unwrap();
        assert_eq!(mutation.status(), StatusCode::NO_CONTENT);

        // Drain the watcher (the publish wakes it) so the task does not leak.
        let woke = tokio::time::timeout(std::time::Duration::from_millis(500), watcher)
            .await
            .expect("watcher resolves after the mutation")
            .expect("watch task must not panic");
        assert_eq!(woke.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn watch_shares_the_owner_scoped_router_surface_as_siblings() {
        // NOTE: the `mod server` VFS router carries no bearer/token auth of its
        // own; every route (watch included) is authenticated by the deployment
        // that mounts `chevalier_vfs_routes`. This test pins the invariant that
        // makes "identical auth to siblings" hold: watch is reachable ONLY on the
        // owner-scoped surface, so it can never bypass that wrapper.
        let backend = MemoryBackend::default();
        let app = chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend);

        let unscoped = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/watch?since=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            unscoped.status(),
            StatusCode::NOT_FOUND,
            "watch outside the owner-scoped prefix must not be routed"
        );

        let scoped = app
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/watch?since=0&timeout_ms=1000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scoped.status(), StatusCode::OK);
    }

    // ---- revocation-acked publications ------------------------------------

    /// Build the owner-scoped router over a coordinator with an explicit
    /// publication-ack cap so ack-blocking outcomes are timing-unambiguous.
    fn ack_app(cap: Duration) -> axum::Router {
        vfs_routes_with_coordinator::<MemoryBackend, MemoryBackend>(
            CHEVALIER_VFS_ROUTE_PREFIX,
            Arc::new(VfsPublicationCoordinator::with_ack_timeout(cap)),
        )
        .with_state(MemoryBackend::default())
    }

    async fn watch_poll(
        app: &axum::Router,
        owner: &str,
        query: &str,
    ) -> axum::http::Response<Body> {
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/internal/chevalier/vfs/{owner}/watch?{query}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn seed_revision(app: &axum::Router, owner: &str) -> u64 {
        let seed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/internal/chevalier/vfs/{owner}/tree?path="))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        namespace_revision(&seed)
    }

    fn mkdir_request(owner: &str, path: &str) -> Request<Body> {
        Request::builder()
            .method("PUT")
            .uri(format!("/internal/chevalier/vfs/{owner}/dir?path={path}"))
            .header(
                CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                format!("owner:{owner}:workspace"),
            )
            .header(
                CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                Uuid::new_v4().to_string(),
            )
            .header(CHEVALIER_VFS_MODE_HEADER, format!("{}", 0o750))
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn publication_blocks_until_a_registered_watcher_acks_the_new_revision() {
        // A generous cap makes the outcome unambiguous: the publish can finish
        // quickly only via the ack, never via the (5s) fail-open cap.
        let app = ack_app(Duration::from_secs(5));
        let owner = "ack-blocks";
        let baseline = seed_revision(&app, owner).await;

        // Park an identified watcher at the baseline; entry registers ack(baseline).
        let parked = tokio::spawn({
            let app = app.clone();
            let owner = owner.to_string();
            let query = format!("since={baseline}&timeout_ms=30000&watcher_id=obs-1");
            async move { watch_poll(&app, &owner, &query).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Publish concurrently: bumps the revision, wakes the parked watcher,
        // then must WAIT for obs-1 to re-poll with since >= the new revision.
        let mut publish = tokio::spawn({
            let app = app.clone();
            let owner = owner.to_string();
            async move { app.oneshot(mkdir_request(&owner, "folder")).await.unwrap() }
        });

        // The parked poll wakes with the new revision, but waking is NOT an ack:
        // the ack is the NEXT poll's `since`.
        let woke = tokio::time::timeout(Duration::from_millis(500), parked)
            .await
            .expect("parked watcher wakes on the publish")
            .expect("watch task must not panic");
        assert_eq!(woke.status(), StatusCode::OK);
        let observed = namespace_revision(&woke);
        assert!(observed > baseline);

        // Still parked on the ack (cap is 5s away).
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut publish)
                .await
                .is_err(),
            "publication must not answer before the watcher acks the new revision"
        );

        // The observer re-polls with since = observed: THIS ack unblocks the
        // writer. It then parks; abort it after the publish answers.
        let ack = tokio::spawn({
            let app = app.clone();
            let owner = owner.to_string();
            let query = format!("since={observed}&timeout_ms=1000&watcher_id=obs-1");
            async move { watch_poll(&app, &owner, &query).await }
        });

        let published = tokio::time::timeout(Duration::from_millis(1000), publish)
            .await
            .expect("publication answers once the watcher acks")
            .expect("publish task must not panic");
        assert_eq!(published.status(), StatusCode::NO_CONTENT);
        assert!(namespace_revision(&published) >= observed);
        ack.abort();
    }

    #[tokio::test]
    async fn publication_fails_open_when_a_registered_watcher_goes_silent() {
        // A short cap keeps the fail-open fast.
        let cap = Duration::from_millis(200);
        let app = ack_app(cap);
        let owner = "ack-failopen";
        let baseline = seed_revision(&app, owner).await;

        // Register a watcher that wakes on the publish but never re-polls.
        let parked = tokio::spawn({
            let app = app.clone();
            let owner = owner.to_string();
            let query = format!("since={baseline}&timeout_ms=30000&watcher_id=ghost");
            async move { watch_poll(&app, &owner, &query).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The publish must proceed at the cap rather than hang on the laggard.
        let started = tokio::time::Instant::now();
        let publish = app.oneshot(mkdir_request(owner, "folder")).await.unwrap();
        let elapsed = started.elapsed();

        assert_eq!(publish.status(), StatusCode::NO_CONTENT);
        assert!(namespace_revision(&publish) > baseline);
        assert!(
            elapsed >= cap,
            "publication must wait the full cap before failing open (waited {elapsed:?})"
        );
        assert!(
            elapsed < cap + Duration::from_secs(2),
            "publication must fail open at the cap, not hang (waited {elapsed:?})"
        );

        // Drain the woken watcher so its task does not leak.
        let woke = tokio::time::timeout(Duration::from_millis(500), parked)
            .await
            .expect("silent watcher still woke on the publish")
            .expect("watch task must not panic");
        assert_eq!(woke.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn an_anonymous_watcher_never_gates_a_publication() {
        // Long cap: a registered watcher would stall the publish ~5s. An
        // anonymous one (no watcher_id) must not, so the publish returns at once.
        let app = ack_app(Duration::from_secs(5));
        let owner = "ack-anon";
        let baseline = seed_revision(&app, owner).await;

        let parked = tokio::spawn({
            let app = app.clone();
            let owner = owner.to_string();
            let query = format!("since={baseline}&timeout_ms=30000");
            async move { watch_poll(&app, &owner, &query).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let publish = tokio::time::timeout(
            Duration::from_millis(500),
            app.oneshot(mkdir_request(owner, "folder")),
        )
        .await
        .expect("an anonymous watcher must not gate a publication")
        .unwrap();
        assert_eq!(publish.status(), StatusCode::NO_CONTENT);

        let woke = tokio::time::timeout(Duration::from_millis(500), parked)
            .await
            .expect("anonymous watcher still wakes on the publish")
            .expect("watch task must not panic");
        assert_eq!(woke.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn a_publication_waits_for_every_registered_watcher() {
        let app = ack_app(Duration::from_secs(5));
        let owner = "ack-multi";
        let baseline = seed_revision(&app, owner).await;

        // Park two identified watchers.
        let mut parked = Vec::new();
        for id in ["obs-a", "obs-b"] {
            let app = app.clone();
            let owner = owner.to_string();
            let query = format!("since={baseline}&timeout_ms=30000&watcher_id={id}");
            parked.push(tokio::spawn(
                async move { watch_poll(&app, &owner, &query).await },
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut publish = tokio::spawn({
            let app = app.clone();
            let owner = owner.to_string();
            async move { app.oneshot(mkdir_request(&owner, "folder")).await.unwrap() }
        });

        // Drain both woken polls to learn the published revision.
        let mut observed = baseline;
        for handle in parked {
            let woke = tokio::time::timeout(Duration::from_millis(500), handle)
                .await
                .expect("watcher wakes on the publish")
                .expect("watch task must not panic");
            assert_eq!(woke.status(), StatusCode::OK);
            observed = observed.max(namespace_revision(&woke));
        }

        // Ack only the FIRST watcher; the publish must still block on the second.
        let ack_a = tokio::spawn({
            let app = app.clone();
            let owner = owner.to_string();
            let query = format!("since={observed}&timeout_ms=1000&watcher_id=obs-a");
            async move { watch_poll(&app, &owner, &query).await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut publish)
                .await
                .is_err(),
            "publication must keep blocking until every watcher acks"
        );

        // Ack the second watcher; now the publish may proceed.
        let ack_b = tokio::spawn({
            let app = app.clone();
            let owner = owner.to_string();
            let query = format!("since={observed}&timeout_ms=1000&watcher_id=obs-b");
            async move { watch_poll(&app, &owner, &query).await }
        });
        let published = tokio::time::timeout(Duration::from_millis(1000), publish)
            .await
            .expect("publication answers once every watcher acks")
            .expect("publish task must not panic");
        assert_eq!(published.status(), StatusCode::NO_CONTENT);
        ack_a.abort();
        ack_b.abort();
    }

    #[tokio::test]
    async fn gone_watchers_are_pruned_and_stop_gating_publications() {
        let coordinator = VfsPublicationCoordinator::with_ack_timeout(Duration::from_secs(5));
        let owner = coordinator.owner("owner");

        // A watcher with a tiny grace (2x its 20ms timeout) lags now...
        owner.record_watcher_ack("obs-gone", 10, Duration::from_millis(20));
        assert_eq!(
            owner.unacked_watchers(100),
            1,
            "a fresh watcher below the revision is a laggard"
        );

        // ...but past its grace it is pruned and no longer gates the revision.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            owner.unacked_watchers(100),
            0,
            "a watcher past its liveness grace is pruned"
        );

        // An anonymous ack (empty id) is never registered, so it gates nothing.
        owner.record_watcher_ack("", 10, Duration::from_secs(30));
        assert_eq!(
            owner.unacked_watchers(100),
            0,
            "anonymous watchers are never registered"
        );
    }

    #[tokio::test]
    async fn writer_publish_returns_only_after_the_observer_is_coherent() {
        // End-to-end shape against the real server: a writer's mutation must not
        // return until every observer has become coherent through (acked) the
        // published revision, so a read issued the instant the write returns is
        // fresh. The observer models the client's exact ack protocol: on a 200 it
        // advances its coherence fence BEFORE re-polling (the re-poll's `since`
        // is the ack) — the same fence->(cache)->re-poll order the real client
        // guarantees.
        let app = ack_app(Duration::from_secs(5));
        let owner = "ack-e2e";
        let baseline = seed_revision(&app, owner).await;

        let observer_fence = Arc::new(AtomicU64::new(baseline));
        let stop = Arc::new(AtomicBool::new(false));
        let observer = tokio::spawn({
            let app = app.clone();
            let owner = owner.to_string();
            let fence = Arc::clone(&observer_fence);
            let stop = Arc::clone(&stop);
            async move {
                while !stop.load(Ordering::Acquire) {
                    let since = fence.load(Ordering::Acquire);
                    let query = format!("since={since}&timeout_ms=1000&watcher_id=obs-e2e");
                    let response = watch_poll(&app, &owner, &query).await;
                    if response.status() == StatusCode::OK {
                        // Fence advance strictly BEFORE the next poll (the ack),
                        // mirroring the client's coherence-before-ack ordering.
                        fence.fetch_max(namespace_revision(&response), Ordering::AcqRel);
                    }
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let published = app.oneshot(mkdir_request(owner, "folder")).await.unwrap();
        assert_eq!(published.status(), StatusCode::NO_CONTENT);
        let revision = namespace_revision(&published);
        assert!(revision > baseline);

        // The observer set its fence to the published revision before issuing the
        // ack that unblocked the writer, so it is already coherent here.
        assert!(
            observer_fence.load(Ordering::Acquire) >= revision,
            "writer publish returned before the observer became coherent through it"
        );

        stop.store(true, Ordering::Release);
        observer.abort();
    }

    #[async_trait]
    impl VfsGatewayBackend for MemoryBackend {
        async fn list_dir(&self, owner_id: &str, path: &str) -> VfsResult<Vec<VfsDirEntry>> {
            Ok(vec![VfsDirEntry {
                name: format!("{owner_id}:{path}:file.txt"),
                kind: VFS_ENTRY_KIND_FILE.to_string(),
                size_bytes: 5,
                file_id: Some("memory:file.txt".to_string()),
                link_count: 1,
                link_target: None,
                content_hash: None,
                executable: false,
                mode: None,
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
                    file_id: Some("memory:dir".to_string()),
                    link_count: 1,
                    link_target: None,
                    content_hash: None,
                    executable: false,
                    mode: None,
                    updated_at: None,
                });
            }
            let Some(bytes) = inner.files.get(path) else {
                return Err(VfsGatewayError::NotFound(path.to_string()));
            };
            Ok(VfsMetadata {
                kind: VFS_ENTRY_KIND_FILE.to_string(),
                size_bytes: bytes.len() as u64,
                file_id: Some(format!("memory:{path}")),
                link_count: 1,
                link_target: None,
                content_hash: None,
                executable: false,
                mode: None,
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
                    file_id: Some("memory:dir".to_string()),
                    link_count: 1,
                    link_target: None,
                    content_hash: None,
                    executable: false,
                    mode: None,
                    updated_at: None,
                });
            }
            let Some(bytes) = inner.files.get(path) else {
                return Err(VfsGatewayError::NotFound(path.to_string()));
            };
            Ok(VfsMetadata {
                kind: VFS_ENTRY_KIND_FILE.to_string(),
                size_bytes: bytes.len() as u64,
                file_id: Some(format!("memory:{path}")),
                link_count: 1,
                link_target: None,
                content_hash: None,
                executable: false,
                mode: None,
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

        async fn apply_namespace_batch(
            &self,
            request: VfsNamespaceMutationBatchRequest,
        ) -> VfsResult<()> {
            let mut inner = self.inner.lock().unwrap();
            // Mirror the production/TS store: apply each mutation to the backing
            // namespace so the handler's post-apply publication snapshot observes
            // the resulting state (e.g. a freshly created directory stats `Some`,
            // while a path deleted at the end of the batch stats `None`).
            for mutation in &request.mutations {
                match mutation {
                    VfsNamespaceMutation::CreateFile { path, .. } => {
                        inner.files.entry(path.clone()).or_insert_with(Bytes::new);
                    }
                    VfsNamespaceMutation::CreateDirectory { path, .. } => {
                        inner.dirs.insert(path.clone());
                    }
                    VfsNamespaceMutation::DeleteFile { path, .. } => {
                        inner.files.remove(path.as_str());
                    }
                    VfsNamespaceMutation::RemoveDirectory { path } => {
                        inner.dirs.remove(path.as_str());
                    }
                    VfsNamespaceMutation::Rename { from, to } => {
                        if let Some(bytes) = inner.files.remove(from.as_str()) {
                            inner.files.insert(to.clone(), bytes);
                        }
                    }
                    VfsNamespaceMutation::SetMode { .. }
                    | VfsNamespaceMutation::CreateSymlink { .. }
                    | VfsNamespaceMutation::CreateHardLink { .. } => {}
                }
            }
            inner.namespace_batches.push(request);
            Ok(())
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
            .clone()
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
            .clone()
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
            .clone()
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
                                        "secondary_fingerprint": "secondary-1",
                                        "expected_file_id": "file-1"
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
        let committed_revision = response
            .headers()
            .get(CHEVALIER_VFS_NAMESPACE_REVISION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .expect("committed write revision");
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
        assert_eq!(
            inner.write_many[0].writes[0]
                .precondition
                .as_ref()
                .unwrap()
                .expected_file_id
                .as_deref(),
            Some("file-1")
        );
        assert!(inner.write_many[0].writes[1].precondition.is_none());
        assert_eq!(inner.files.get("first.txt").unwrap().as_ref(), b"one");
        assert_eq!(inner.files.get("second.txt").unwrap().as_ref(), b"two");
        drop(inner);

        let stat = app
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/stat?path=first.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stat.status(), StatusCode::OK);
        assert_eq!(
            stat.headers()
                .get(CHEVALIER_VFS_NAMESPACE_REVISION_HEADER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok()),
            Some(committed_revision),
        );
    }

    #[tokio::test]
    async fn gateway_namespace_many_forwards_one_ordered_batch() {
        let owner_token = Uuid::new_v4();
        let backend = MemoryBackend::default();
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/chevalier/vfs/owner-1/namespace-many")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                    .header(
                        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                        owner_token.to_string(),
                    )
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "operation_ids": [
                                "mkdir-tree",
                                "rename-result",
                                "chmod-result",
                                "delete-result"
                            ],
                            "mutations": [
                                {"kind": "create_directory", "path": "tree", "mode": 509},
                                {"kind": "rename", "from": "source.txt", "to": "tree/result.txt"},
                                {"kind": "set_mode", "path": "tree/result.txt", "mode": 3565},
                                {
                                    "kind": "delete_file",
                                    "path": "tree/result.txt",
                                    "precondition": {"fingerprint": "version-1"}
                                },
                            ],
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let committed_revision = response
            .headers()
            .get(CHEVALIER_VFS_NAMESPACE_REVISION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .expect("committed namespace revision");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let snapshot: VfsNamespaceMutationBatchResponse = serde_json::from_slice(&body).unwrap();
        assert!(
            snapshot
                .entries
                .iter()
                .any(|entry| entry.path == "tree" && entry.metadata.is_some())
        );
        assert!(
            snapshot
                .entries
                .iter()
                .any(|entry| entry.path == "tree/result.txt" && entry.metadata.is_none())
        );
        let inner = backend.inner.lock().unwrap();
        assert_eq!(inner.namespace_batches.len(), 1);
        assert_eq!(
            inner.namespace_batches[0].mutations,
            vec![
                VfsNamespaceMutation::CreateDirectory {
                    path: "tree".to_string(),
                    mode: Some(0o775),
                },
                VfsNamespaceMutation::Rename {
                    from: "source.txt".to_string(),
                    to: "tree/result.txt".to_string(),
                },
                VfsNamespaceMutation::SetMode {
                    path: "tree/result.txt".to_string(),
                    mode: 0o6755,
                },
                VfsNamespaceMutation::DeleteFile {
                    path: "tree/result.txt".to_string(),
                    precondition: Some(VfsWritePrecondition {
                        predicate: None,
                        fingerprint: Some("version-1".to_string()),
                        secondary_fingerprint: None,
                        expected_file_id: None,
                    }),
                },
            ]
        );
        drop(inner);

        let listing = app
            .oneshot(
                Request::builder()
                    .uri("/internal/chevalier/vfs/owner-1/tree?path=")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(listing.status(), StatusCode::OK);
        assert_eq!(
            listing
                .headers()
                .get(CHEVALIER_VFS_NAMESPACE_REVISION_HEADER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok()),
            Some(committed_revision),
        );
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
            if method == "PUT" {
                builder = builder.header(CHEVALIER_VFS_MODE_HEADER, 0o750.to_string());
            }
            if method == "PUT" && uri.contains("/file?") {
                builder = builder.header(CHEVALIER_VFS_PRECONDITION_FILE_ID_HEADER, "file-new");
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
        assert_eq!(inner.writes[0].headers.mode, Some(0o750));
        assert_eq!(
            inner.writes[0]
                .precondition
                .as_ref()
                .and_then(|precondition| precondition.expected_file_id.as_deref()),
            Some("file-new")
        );
        assert_eq!(inner.mkdirs[0].headers.mode, Some(0o750));
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
    async fn gateway_rejects_invalid_external_mode_headers() {
        let owner_token = Uuid::new_v4();
        let backend = MemoryBackend::default();
        let app =
            chevalier_vfs_routes::<MemoryBackend, MemoryBackend>().with_state(backend.clone());

        for mode in ["not-a-mode", "-1", "33256"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/internal/chevalier/vfs/owner-1/file?path=new.txt")
                        .header(CHEVALIER_VFS_RESOURCE_KEY_HEADER, "owner:owner-1:workspace")
                        .header(
                            CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                            owner_token.to_string(),
                        )
                        .header(CHEVALIER_VFS_MODE_HEADER, mode)
                        .body(Body::from("new"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        assert!(backend.inner.lock().unwrap().writes.is_empty());
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
