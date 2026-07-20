//! VFS storage bindings: `local` (filesystem) and `gateway` (HTTP) backends of
//! the engine's `OptimizedVfsStorage`. Returns metadata as JSON, bytes as
//! Buffer. (gcs / object-backed + manifest index are a follow-up.)

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use chevalier_vfs::gateway::{GatewayVfsStorage, GatewayVfsStorageConfig};
use chevalier_vfs::local::LocalVfsStorage;
use chevalier_vfs::{
    OptimizedVfsStorage, VFS_POSIX_MODE_MASK, VfsStorageCasPredicate, VfsStorageDirListFilter,
    VfsStorageEntryKind, VfsStorageError, VfsStorageMetadata, VfsStorageMetadataFields,
    VfsStorageNamespaceMutation, VfsStorageObjectState, VfsStorageWrite, VfsStorageWriteOptions,
    VfsStorageWritePrecondition,
};
use napi::bindgen_prelude::{BigInt, Buffer};
use napi_derive::napi;
use serde::Deserialize;
use serde_json::{Map, Value};

fn vfs_err(e: VfsStorageError) -> napi::Error {
    let (status, code) = match &e {
        VfsStorageError::NotFound(_) => (404, "VFS_NOT_FOUND"),
        VfsStorageError::BadRequest(_) => (400, "VFS_BAD_REQUEST"),
        VfsStorageError::Forbidden(_) => (403, "VFS_FORBIDDEN"),
        VfsStorageError::Conflict(_) => (409, "VFS_CONFLICT"),
        VfsStorageError::Internal(_) => (500, "VFS_INTERNAL"),
    };
    napi::Error::new(
        napi::Status::GenericFailure,
        format!("VFS: [{code} status={status}] {e}"),
    )
}

fn invalid_options_err(message: impl Into<String>) -> napi::Error {
    napi::Error::new(napi::Status::InvalidArg, message.into())
}

fn serialize_err(e: serde_json::Error) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, format!("serialize: {e}"))
}

fn to_json<T: serde::Serialize>(v: T) -> napi::Result<serde_json::Value> {
    serde_json::to_value(v).map_err(serialize_err)
}

fn normalize_if_match(value: String) -> Option<String> {
    let mut next = value.trim().to_string();
    if let Some(stripped) = next.strip_prefix("W/") {
        next = stripped.trim().to_string();
    }
    if next.len() >= 2
        && ((next.starts_with('"') && next.ends_with('"'))
            || (next.starts_with('\'') && next.ends_with('\'')))
    {
        next = next[1..next.len() - 1].trim().to_string();
    }
    if let Some(stripped) = next.strip_prefix("sha256:") {
        next = stripped.to_string();
    }
    if next.is_empty() || next.eq_ignore_ascii_case("null") {
        None
    } else {
        Some(next)
    }
}

fn options_object<'a>(options: Option<&'a Value>) -> napi::Result<Option<&'a Map<String, Value>>> {
    let Some(options) = options else {
        return Ok(None);
    };
    options
        .as_object()
        .ok_or_else(|| invalid_options_err("invalid VFS options: expected object"))
        .map(Some)
}

fn option_field<'a>(
    options: &'a Map<String, Value>,
    camel: &str,
    snake: &str,
) -> Option<&'a Value> {
    options.get(camel).or_else(|| options.get(snake))
}

fn precondition_from_options(
    options: Option<&Value>,
) -> napi::Result<Option<VfsStorageWritePrecondition>> {
    let Some(options) = options_object(options)? else {
        return Ok(None);
    };
    let if_match = option_field(options, "ifMatch", "if_match");
    let expected_file_id = match option_field(options, "expectedFileId", "expected_file_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
        Some(Value::String(_)) => {
            return Err(invalid_options_err(
                "invalid VFS options: expectedFileId must be a non-empty string",
            ));
        }
        Some(_) => {
            return Err(invalid_options_err(
                "invalid VFS options: expectedFileId must be a non-empty string",
            ));
        }
    };
    if if_match.is_none() && expected_file_id.is_none() {
        return Ok(None);
    }
    let predicate = match if_match {
        None => None,
        Some(Value::Null) => Some(VfsStorageCasPredicate::Absent),
        Some(Value::String(value)) => match normalize_if_match(value.clone()) {
            Some(fingerprint) => Some(VfsStorageCasPredicate::ContentFingerprint { fingerprint }),
            None => Some(VfsStorageCasPredicate::Absent),
        },
        Some(_) => {
            return Err(invalid_options_err(
                "invalid VFS options: ifMatch must be a string or null",
            ));
        }
    };
    Ok(Some(VfsStorageWritePrecondition {
        predicate,
        fingerprint: None,
        secondary_fingerprint: None,
        expected_file_id,
    }))
}

fn write_options_from_options(
    options: Option<&Value>,
) -> napi::Result<Option<VfsStorageWriteOptions>> {
    let Some(options) = options_object(options)? else {
        return Ok(None);
    };
    let mode = mode_from_object(options, "invalid VFS options: mode")?;
    let executable = match option_field(options, "executable", "executable") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(value)) => Some(*value),
        Some(_) => {
            return Err(invalid_options_err(
                "invalid VFS options: executable must be a boolean",
            ));
        }
    };
    if mode.is_none() && executable.is_none() {
        return Ok(None);
    }
    Ok(Some(VfsStorageWriteOptions {
        executable: mode
            .map(|value| value & 0o111 != 0)
            .or(executable)
            .unwrap_or(false),
        mode,
    }))
}

fn mode_from_object(options: &Map<String, Value>, context: &str) -> napi::Result<Option<u32>> {
    match option_field(options, "mode", "mode") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .filter(|value| *value <= u64::from(VFS_POSIX_MODE_MASK))
            .map(|value| Some(value as u32))
            .ok_or_else(|| {
                invalid_options_err(format!(
                    "{context} must be an integer between 0 and {VFS_POSIX_MODE_MASK}"
                ))
            }),
        Some(_) => Err(invalid_options_err(format!(
            "{context} must be an integer between 0 and {VFS_POSIX_MODE_MASK}"
        ))),
    }
}

fn mode_from_options(options: Option<&Value>, context: &str) -> napi::Result<Option<u32>> {
    let Some(options) = options_object(options)? else {
        return Ok(None);
    };
    mode_from_object(options, context)
}

fn validate_namespace_modes(mutations: &Value) -> napi::Result<()> {
    let Some(mutations) = mutations.as_array() else {
        return Ok(());
    };
    for mutation in mutations {
        let Some(mutation) = mutation.as_object() else {
            continue;
        };
        let kind = mutation.get("kind").and_then(Value::as_str);
        if matches!(kind, Some("create_directory" | "set_mode")) {
            let mode = mode_from_object(mutation, "invalid namespace mode")?;
            if kind == Some("set_mode") && mode.is_none() {
                return Err(invalid_options_err(
                    "invalid namespace mode: set_mode requires mode",
                ));
            }
        }
    }
    Ok(())
}

fn list_filter_from_options(options: Option<&Value>) -> napi::Result<VfsStorageDirListFilter> {
    let Some(options) = options_object(options)? else {
        return Ok(VfsStorageDirListFilter::default());
    };
    let max_hash_bytes = match option_field(options, "maxHashBytes", "max_hash_bytes") {
        None | Some(Value::Null) => None,
        Some(Value::Number(value)) => Some(value.as_u64().ok_or_else(|| {
            invalid_options_err("invalid VFS options: maxHashBytes must be a non-negative integer")
        })?),
        Some(_) => {
            return Err(invalid_options_err(
                "invalid VFS options: maxHashBytes must be a non-negative integer",
            ));
        }
    };
    Ok(VfsStorageDirListFilter {
        max_hash_bytes,
        ..Default::default()
    })
}

fn metadata_fields_from_options(options: Option<&Value>) -> napi::Result<VfsStorageMetadataFields> {
    let Some(options) = options_object(options)? else {
        return Ok(VfsStorageMetadataFields::default());
    };
    let max_hash_bytes = match option_field(options, "maxHashBytes", "max_hash_bytes") {
        None | Some(Value::Null) => None,
        Some(Value::Number(value)) => Some(value.as_u64().ok_or_else(|| {
            invalid_options_err("invalid VFS options: maxHashBytes must be a non-negative integer")
        })?),
        Some(_) => {
            return Err(invalid_options_err(
                "invalid VFS options: maxHashBytes must be a non-negative integer",
            ));
        }
    };
    Ok(VfsStorageMetadataFields {
        max_hash_bytes,
        ..Default::default()
    })
}

/// Pack-slot location for an object-backed file.
#[napi(object)]
pub struct VfsObjectState {
    /// `bigint` — exact size, lossless above 2^53.
    pub size_bytes: BigInt,
    pub pack_key: String,
    pub pack_slot_offset: BigInt,
    pub pack_slot_length: BigInt,
    pub pack_slot_compression: i32,
}

impl From<VfsStorageObjectState> for VfsObjectState {
    fn from(o: VfsStorageObjectState) -> Self {
        Self {
            size_bytes: BigInt::from(o.size_bytes),
            pack_key: o.pack_key,
            pack_slot_offset: BigInt::from(o.pack_slot_offset),
            pack_slot_length: BigInt::from(o.pack_slot_length),
            pack_slot_compression: o.pack_slot_compression as i32,
        }
    }
}

/// File/dir metadata. `sizeBytes` is a `bigint` so it never loses precision at
/// the FFI boundary (JS `number` only holds integers up to 2^53).
#[napi(object)]
pub struct VfsMetadata {
    pub path: String,
    /// `"File"`, `"Directory"`, `"Symlink"`, or `"Special"`.
    pub kind: String,
    pub size_bytes: BigInt,
    pub file_id: Option<String>,
    pub link_count: Option<BigInt>,
    pub link_target: Option<String>,
    pub mode: Option<u32>,
    pub executable: Option<bool>,
    pub content_hash: Option<String>,
    pub token_count: Option<i32>,
    pub version: Option<String>,
    /// RFC 3339 timestamp.
    pub updated_at: Option<String>,
    pub object_state: Option<VfsObjectState>,
}

#[napi(object)]
pub struct VfsHardLinkResult {
    pub source: VfsMetadata,
    pub destination: VfsMetadata,
}

impl From<VfsStorageMetadata> for VfsMetadata {
    fn from(m: VfsStorageMetadata) -> Self {
        Self {
            path: m.path,
            kind: match m.kind {
                VfsStorageEntryKind::File => "File",
                VfsStorageEntryKind::Directory => "Directory",
                VfsStorageEntryKind::Symlink => "Symlink",
                VfsStorageEntryKind::Special => "Special",
                _ => "Unknown",
            }
            .to_string(),
            size_bytes: BigInt::from(m.size_bytes),
            file_id: m.file_id,
            link_count: Some(BigInt::from(m.link_count)),
            link_target: m.link_target,
            mode: m.mode,
            executable: Some(m.executable),
            content_hash: m.content_hash,
            token_count: m.token_count,
            version: m.version,
            updated_at: m.updated_at.map(|d| d.to_rfc3339()),
            object_state: m.object_state.map(VfsObjectState::from),
        }
    }
}

/// Options for the HTTP gateway backend.
#[napi(object)]
pub struct GatewayOptions {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub scope_path: Option<String>,
    pub component: Option<String>,
    pub mutation_reason: Option<String>,
}

/// Write options for VFS storage.
#[napi(object)]
#[allow(dead_code)]
pub struct VfsWriteOptions {
    #[napi(ts_type = "string | null")]
    pub if_match: Option<String>,
    #[napi(ts_type = "string | null")]
    pub expected_file_id: Option<String>,
    pub executable: Option<bool>,
    pub mode: Option<u32>,
}

#[derive(Deserialize)]
struct VfsWriteManyInput {
    path: String,
    body: Vec<u8>,
    #[serde(default)]
    precondition: Option<VfsStorageWritePrecondition>,
}

/// A virtual filesystem. Construct via `VfsStorage.local(root)` or
/// `VfsStorage.gateway(opts)`.
#[napi]
pub struct VfsStorage {
    inner: Arc<dyn OptimizedVfsStorage>,
}

#[napi]
impl VfsStorage {
    /// Filesystem-backed storage rooted at `root`.
    #[napi(factory)]
    pub fn local(root: String) -> VfsStorage {
        VfsStorage {
            inner: Arc::new(LocalVfsStorage::new(PathBuf::from(root))),
        }
    }

    /// HTTP gateway-backed storage.
    #[napi(factory)]
    pub fn gateway(options: GatewayOptions) -> VfsStorage {
        let mut cfg = GatewayVfsStorageConfig::new(options.endpoint);
        if let Some(t) = options.auth_token {
            cfg = cfg.with_auth_token(t);
        }
        if let Some(s) = options.scope_path {
            cfg = cfg.with_scope_path(s);
        }
        if let Some(c) = options.component {
            cfg = cfg.with_component(c);
        }
        if let Some(r) = options.mutation_reason {
            cfg = cfg.with_mutation_reason(r);
        }
        VfsStorage {
            inner: Arc::new(GatewayVfsStorage::new(cfg)),
        }
    }

    /// Read a file's bytes.
    #[napi]
    pub async fn read(&self, path: String) -> napi::Result<Buffer> {
        let b = self.inner.read(&path).await.map_err(vfs_err)?;
        Ok(Buffer::from(b.to_vec()))
    }

    /// Read a bounded byte range without materializing the whole file.
    #[napi]
    pub async fn read_range(
        &self,
        path: String,
        offset: BigInt,
        length: u32,
    ) -> napi::Result<Buffer> {
        let (_, offset, lossless) = offset.get_u64();
        if !lossless {
            return Err(invalid_options_err(
                "invalid VFS range: offset must be a non-negative u64",
            ));
        }
        let bytes = self
            .inner
            .read_range(
                &path,
                chevalier_vfs::VfsStorageReadRange {
                    offset,
                    length: u64::from(length),
                },
            )
            .await
            .map_err(vfs_err)?;
        Ok(Buffer::from(bytes.to_vec()))
    }

    /// Write a file; returns the write result (JSON: content hash, changed, …).
    #[napi(ts_args_type = "path: string, data: Buffer, options?: VfsWriteOptions | null")]
    pub async fn write(
        &self,
        path: String,
        data: Buffer,
        options: Option<Value>,
    ) -> napi::Result<serde_json::Value> {
        let precondition = precondition_from_options(options.as_ref())?;
        let write_options = write_options_from_options(options.as_ref())?;
        let r = self
            .inner
            .write_with_options(
                &path,
                Bytes::from(data.to_vec()),
                precondition,
                write_options,
            )
            .await
            .map_err(vfs_err)?;
        to_json(r)
    }

    /// Atomically install a host-local staged file with bounded memory.
    #[napi(
        ts_args_type = "path: string, sourcePath: string, expectedContentHash: string, options?: VfsWriteOptions | null"
    )]
    pub async fn write_from_file(
        &self,
        path: String,
        source_path: String,
        expected_content_hash: String,
        options: Option<Value>,
    ) -> napi::Result<serde_json::Value> {
        let precondition = precondition_from_options(options.as_ref())?;
        let write_options = write_options_from_options(options.as_ref())?;
        let result = self
            .inner
            .write_from_local_file(
                &path,
                PathBuf::from(source_path).as_path(),
                Some(expected_content_hash.as_str()),
                precondition,
                write_options,
            )
            .await
            .map_err(vfs_err)?;
        to_json(result)
    }

    /// Stat a path; returns typed metadata (`sizeBytes` is a `bigint`) or null.
    #[napi(ts_args_type = "path: string, options?: { maxHashBytes?: number | null } | null")]
    pub async fn stat(
        &self,
        path: String,
        options: Option<Value>,
    ) -> napi::Result<Option<VfsMetadata>> {
        let fields = metadata_fields_from_options(options.as_ref())?;
        Ok(self
            .inner
            .stat_with_metadata_fields(&path, fields)
            .await
            .map_err(vfs_err)?
            .map(VfsMetadata::from))
    }

    /// List a directory's entries with typed metadata.
    #[napi(ts_args_type = "path: string, options?: { maxHashBytes?: number | null } | null")]
    pub async fn list_dir(
        &self,
        path: String,
        options: Option<Value>,
    ) -> napi::Result<Vec<VfsMetadata>> {
        let filter = list_filter_from_options(options.as_ref())?;
        let items = self
            .inner
            .list_dir_with_metadata(&path, filter)
            .await
            .map_err(vfs_err)?;
        Ok(items.into_iter().map(VfsMetadata::from).collect())
    }

    /// Read metadata for an indexed set of paths in one backend request.
    #[napi]
    pub async fn metadata_many(
        &self,
        paths: Vec<String>,
    ) -> napi::Result<Vec<Option<VfsMetadata>>> {
        let items = self
            .inner
            .metadata_many(&paths, VfsStorageMetadataFields::default())
            .await
            .map_err(vfs_err)?;
        Ok(items
            .into_iter()
            .map(|item| item.map(VfsMetadata::from))
            .collect())
    }

    /// Create a directory, optionally with an exact POSIX mode.
    #[napi(ts_args_type = "path: string, options?: { mode?: number | null } | null")]
    pub async fn mkdir(&self, path: String, options: Option<Value>) -> napi::Result<()> {
        let mode = mode_from_options(options.as_ref(), "invalid VFS mkdir mode")?;
        self.inner
            .mkdir_with_mode(&path, mode)
            .await
            .map_err(vfs_err)
    }

    /// Create a symbolic link.
    #[napi]
    pub async fn create_symlink(&self, path: String, target: String) -> napi::Result<()> {
        self.inner
            .create_symlink(&path, &target)
            .await
            .map_err(vfs_err)
    }

    /// Create a second pathname for one shared regular-file identity.
    #[napi]
    pub async fn create_hard_link(
        &self,
        source: String,
        destination: String,
    ) -> napi::Result<VfsHardLinkResult> {
        let result = self
            .inner
            .create_hard_link(&source, &destination)
            .await
            .map_err(vfs_err)?;
        Ok(VfsHardLinkResult {
            source: result.source.into(),
            destination: result.destination.into(),
        })
    }

    #[napi]
    pub async fn find_hard_link_alias(
        &self,
        file_id: String,
        excluding_path: String,
    ) -> napi::Result<Option<String>> {
        self.inner
            .find_hard_link_alias(&file_id, &excluding_path)
            .await
            .map_err(vfs_err)
    }

    /// Delete a file; returns the delete result (JSON).
    #[napi(ts_args_type = "path: string, options?: { ifMatch?: string | null } | null")]
    pub async fn remove(
        &self,
        path: String,
        options: Option<Value>,
    ) -> napi::Result<serde_json::Value> {
        let precondition = precondition_from_options(options.as_ref())?;
        let r = self
            .inner
            .delete_file_with_metadata(&path, precondition)
            .await
            .map_err(vfs_err)?;
        to_json(r)
    }

    /// Remove an (empty) directory.
    #[napi]
    pub async fn rmdir(&self, path: String) -> napi::Result<()> {
        self.inner.rmdir(&path).await.map_err(vfs_err)
    }

    /// Rename/move a file; returns the rename result (JSON).
    #[napi]
    pub async fn rename(&self, from: String, to: String) -> napi::Result<serde_json::Value> {
        let r = self
            .inner
            .rename_with_metadata(&from, &to)
            .await
            .map_err(vfs_err)?;
        to_json(r)
    }

    /// Apply an ordered namespace batch in one backend operation.
    #[napi]
    pub async fn apply_namespace_batch(&self, mutations: Value) -> napi::Result<()> {
        validate_namespace_modes(&mutations)?;
        let mutations = serde_json::from_value::<Vec<VfsStorageNamespaceMutation>>(mutations)
            .map_err(|error| invalid_options_err(format!("invalid namespace batch: {error}")))?;
        self.inner
            .apply_namespace_batch(mutations)
            .await
            .map_err(vfs_err)
    }

    /// Write an ordered set of files through one backend operation.
    #[napi]
    pub async fn write_many(&self, writes: Value) -> napi::Result<Value> {
        let writes = serde_json::from_value::<Vec<VfsWriteManyInput>>(writes)
            .map_err(|error| invalid_options_err(format!("invalid write batch: {error}")))?
            .into_iter()
            .map(|write| VfsStorageWrite {
                path: write.path,
                bytes: Bytes::from(write.body),
                token_count: None,
                precondition: write.precondition,
            })
            .collect();
        let result = self
            .inner
            .write_many_atomic(writes)
            .await
            .map_err(vfs_err)?;
        to_json(result)
    }
}
