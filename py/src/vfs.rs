use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use chevalier_vfs::SeededFileHash;
use chevalier_vfs::gateway::{GatewayVfsStorage, GatewayVfsStorageConfig};
use chevalier_vfs::local::LocalVfsStorage;
use chevalier_vfs::{
    OptimizedVfsStorage, VFS_POSIX_MODE_MASK, VfsStorageCasPredicate, VfsStorageDirListFilter,
    VfsStorageEntryKind, VfsStorageError, VfsStorageMetadata, VfsStorageMetadataFields,
    VfsStorageNamespaceMutation, VfsStorageObjectState, VfsStoragePrefetchOptions, VfsStorageWrite,
    VfsStorageWriteOptions, VfsStorageWritePrecondition,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::errors::invalid_argument;
use crate::json::{from_python, to_python, value_from_python};

fn vfs_error(error: VfsStorageError) -> PyErr {
    let (status, code) = match &error {
        VfsStorageError::NotFound(_) => (404, "VFS_NOT_FOUND"),
        VfsStorageError::BadRequest(_) => (400, "VFS_BAD_REQUEST"),
        VfsStorageError::Forbidden(_) => (403, "VFS_FORBIDDEN"),
        VfsStorageError::Conflict(_) => (409, "VFS_CONFLICT"),
        VfsStorageError::Internal(_) => (500, "VFS_INTERNAL"),
    };
    let exception = PyRuntimeError::new_err(format!("VFS: [{code} status={status}] {error}"));
    Python::with_gil(|py| {
        let value = exception.value(py);
        let _ = value.setattr("code", code);
        let _ = value.setattr("status", status);
        let _ = value.setattr("status_code", status);
    });
    exception
}

fn options_object(options: Option<&Value>) -> PyResult<Option<&Map<String, Value>>> {
    let Some(options) = options else {
        return Ok(None);
    };
    options
        .as_object()
        .ok_or_else(|| invalid_argument("invalid VFS options: expected object"))
        .map(Some)
}

fn option_field<'a>(
    options: &'a Map<String, Value>,
    camel: &str,
    snake: &str,
) -> Option<&'a Value> {
    options.get(camel).or_else(|| options.get(snake))
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

fn precondition_from_options(
    options: Option<&Value>,
) -> PyResult<Option<VfsStorageWritePrecondition>> {
    let Some(options) = options_object(options)? else {
        return Ok(None);
    };
    let if_match = option_field(options, "ifMatch", "if_match");
    let expected_file_id = match option_field(options, "expectedFileId", "expected_file_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
        Some(_) => {
            return Err(invalid_argument(
                "invalid VFS options: expected_file_id must be a non-empty string",
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
            return Err(invalid_argument(
                "invalid VFS options: if_match must be a string or None",
            ));
        }
    };
    Ok(Some(VfsStorageWritePrecondition {
        predicate,
        fingerprint: None,
        secondary_fingerprint: None,
        expected_file_id,
        expected_current_version: None,
    }))
}

fn mode_from_object(options: &Map<String, Value>, context: &str) -> PyResult<Option<u32>> {
    match option_field(options, "mode", "mode") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .filter(|value| *value <= u64::from(VFS_POSIX_MODE_MASK))
            .map(|value| Some(value as u32))
            .ok_or_else(|| {
                invalid_argument(format!(
                    "{context} must be an integer between 0 and {VFS_POSIX_MODE_MASK}"
                ))
            }),
        Some(_) => Err(invalid_argument(format!(
            "{context} must be an integer between 0 and {VFS_POSIX_MODE_MASK}"
        ))),
    }
}

fn mode_from_options(options: Option<&Value>, context: &str) -> PyResult<Option<u32>> {
    let Some(options) = options_object(options)? else {
        return Ok(None);
    };
    mode_from_object(options, context)
}

fn write_options_from_options(options: Option<&Value>) -> PyResult<Option<VfsStorageWriteOptions>> {
    let Some(options) = options_object(options)? else {
        return Ok(None);
    };
    let mode = mode_from_object(options, "invalid VFS options: mode")?;
    let executable = match option_field(options, "executable", "executable") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(value)) => Some(*value),
        Some(_) => {
            return Err(invalid_argument(
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

fn validate_namespace_modes(mutations: &Value) -> PyResult<()> {
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
                return Err(invalid_argument(
                    "invalid namespace mode: set_mode requires mode",
                ));
            }
        }
    }
    Ok(())
}

fn max_hash_bytes(options: &Map<String, Value>) -> PyResult<Option<u64>> {
    match option_field(options, "maxHashBytes", "max_hash_bytes") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value.as_u64().map(Some).ok_or_else(|| {
            invalid_argument("invalid VFS options: max_hash_bytes must be a non-negative integer")
        }),
        Some(_) => Err(invalid_argument(
            "invalid VFS options: max_hash_bytes must be a non-negative integer",
        )),
    }
}

fn list_filter_from_options(options: Option<&Value>) -> PyResult<VfsStorageDirListFilter> {
    let Some(options) = options_object(options)? else {
        return Ok(VfsStorageDirListFilter::default());
    };
    Ok(VfsStorageDirListFilter {
        max_hash_bytes: max_hash_bytes(options)?,
        ..Default::default()
    })
}

fn metadata_fields_from_options(options: Option<&Value>) -> PyResult<VfsStorageMetadataFields> {
    let Some(options) = options_object(options)? else {
        return Ok(VfsStorageMetadataFields::default());
    };
    Ok(VfsStorageMetadataFields {
        max_hash_bytes: max_hash_bytes(options)?,
        ..Default::default()
    })
}

fn optional_value(options: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Value>> {
    options.map(value_from_python).transpose()
}

#[derive(Serialize)]
struct VfsObjectState {
    size_bytes: u64,
    pack_key: String,
    pack_slot_offset: i64,
    pack_slot_length: i64,
    pack_slot_compression: i32,
}

impl From<VfsStorageObjectState> for VfsObjectState {
    fn from(state: VfsStorageObjectState) -> Self {
        Self {
            size_bytes: state.size_bytes,
            pack_key: state.pack_key,
            pack_slot_offset: state.pack_slot_offset,
            pack_slot_length: state.pack_slot_length,
            pack_slot_compression: state.pack_slot_compression as i32,
        }
    }
}

#[derive(Serialize)]
struct VfsMetadata {
    path: String,
    kind: String,
    size_bytes: u64,
    file_id: Option<String>,
    link_count: u64,
    link_target: Option<String>,
    mode: Option<u32>,
    executable: bool,
    content_hash: Option<String>,
    token_count: Option<i32>,
    version: Option<String>,
    updated_at: Option<String>,
    mtime_ns: Option<i64>,
    ctime_ns: Option<i64>,
    object_state: Option<VfsObjectState>,
}

impl From<VfsStorageMetadata> for VfsMetadata {
    fn from(metadata: VfsStorageMetadata) -> Self {
        Self {
            path: metadata.path,
            kind: match metadata.kind {
                VfsStorageEntryKind::File => "File",
                VfsStorageEntryKind::Directory => "Directory",
                VfsStorageEntryKind::Symlink => "Symlink",
                VfsStorageEntryKind::Special => "Special",
                _ => "Unknown",
            }
            .to_string(),
            size_bytes: metadata.size_bytes,
            file_id: metadata.file_id,
            link_count: metadata.link_count,
            link_target: metadata.link_target,
            mode: metadata.mode,
            executable: metadata.executable,
            content_hash: metadata.content_hash,
            token_count: metadata.token_count,
            version: metadata.version,
            updated_at: metadata.updated_at.map(|value| value.to_rfc3339()),
            mtime_ns: metadata.mtime_ns,
            ctime_ns: metadata.ctime_ns,
            object_state: metadata.object_state.map(Into::into),
        }
    }
}

#[derive(Deserialize)]
struct GatewayOptions {
    endpoint: String,
    #[serde(default)]
    auth_token: Option<String>,
    #[serde(default)]
    scope_path: Option<String>,
    #[serde(default)]
    component: Option<String>,
    #[serde(default)]
    mutation_reason: Option<String>,
}

#[derive(Default, Deserialize)]
struct VfsPrefetchOptions {
    #[serde(default)]
    include_small_file_bytes: Option<bool>,
    #[serde(default)]
    max_entries: Option<i32>,
    #[serde(default)]
    max_pack_bytes: Option<u32>,
}

#[derive(Deserialize)]
struct VfsSeededHash {
    path: String,
    size_bytes: String,
    mtime_ns: String,
    ctime_ns: String,
    content_hash: String,
}

#[derive(Deserialize)]
struct VfsWriteManyInput {
    path: String,
    body: Vec<u8>,
    #[serde(default)]
    mode: Option<u32>,
    #[serde(default)]
    precondition: Option<VfsStorageWritePrecondition>,
}

fn validate_write_modes(writes: impl IntoIterator<Item = Option<u32>>) -> PyResult<()> {
    if writes
        .into_iter()
        .any(|mode| mode.is_some_and(|mode| mode & !0o7777 != 0))
    {
        return Err(invalid_argument(
            "invalid write batch: mode must contain only POSIX permission and special bits",
        ));
    }
    Ok(())
}

fn storage_writes_from_value(writes: Value) -> PyResult<Vec<VfsStorageWrite>> {
    let writes = serde_json::from_value::<Vec<VfsWriteManyInput>>(writes)
        .map_err(|error| invalid_argument(format!("invalid write batch: {error}")))?;
    validate_write_modes(writes.iter().map(|write| write.mode))?;
    Ok(writes
        .into_iter()
        .map(|write| VfsStorageWrite {
            path: write.path,
            mode: write.mode,
            bytes: Bytes::from(write.body),
            token_count: None,
            precondition: write.precondition,
        })
        .collect())
}

#[pyclass(module = "chevalier.chevalier")]
pub struct VfsStorage {
    inner: Arc<dyn OptimizedVfsStorage>,
}

#[pymethods]
impl VfsStorage {
    #[staticmethod]
    fn local(root: String) -> Self {
        Self {
            inner: Arc::new(LocalVfsStorage::new(PathBuf::from(root))),
        }
    }

    #[staticmethod]
    fn gateway(options: &Bound<'_, PyAny>) -> PyResult<Self> {
        let options = from_python::<GatewayOptions>(options)?;
        let mut config = GatewayVfsStorageConfig::new(options.endpoint);
        if let Some(token) = options.auth_token {
            config = config.with_auth_token(token);
        }
        if let Some(path) = options.scope_path {
            config = config.with_scope_path(path);
        }
        if let Some(component) = options.component {
            config = config.with_component(component);
        }
        if let Some(reason) = options.mutation_reason {
            config = config.with_mutation_reason(reason);
        }
        Ok(Self {
            inner: Arc::new(GatewayVfsStorage::new(config)),
        })
    }

    fn read<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bytes = storage.read(&path).await.map_err(vfs_error)?;
            Python::with_gil(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
        })
    }

    fn read_range<'py>(
        &self,
        py: Python<'py>,
        path: String,
        offset: u64,
        length: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bytes = storage
                .read_range(
                    &path,
                    chevalier_vfs::VfsStorageReadRange {
                        offset,
                        length: u64::from(length),
                    },
                )
                .await
                .map_err(vfs_error)?;
            Python::with_gil(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
        })
    }

    #[pyo3(signature = (path, data, options=None))]
    fn write<'py>(
        &self,
        py: Python<'py>,
        path: String,
        data: Vec<u8>,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = optional_value(options)?;
        let precondition = precondition_from_options(options.as_ref())?;
        let write_options = write_options_from_options(options.as_ref())?;
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = storage
                .write_with_options(&path, Bytes::from(data), precondition, write_options)
                .await
                .map_err(vfs_error)?;
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    #[pyo3(signature = (path, source_path, expected_content_hash, options=None))]
    fn write_from_file<'py>(
        &self,
        py: Python<'py>,
        path: String,
        source_path: String,
        expected_content_hash: String,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = optional_value(options)?;
        let precondition = precondition_from_options(options.as_ref())?;
        let write_options = write_options_from_options(options.as_ref())?;
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = storage
                .write_from_local_file(
                    &path,
                    PathBuf::from(source_path).as_path(),
                    Some(&expected_content_hash),
                    precondition,
                    write_options,
                )
                .await
                .map_err(vfs_error)?;
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    fn seed_hash_cache(&self, entries: &Bound<'_, PyAny>) -> PyResult<u32> {
        let entries = from_python::<Vec<VfsSeededHash>>(entries)?;
        let seeded = entries
            .into_iter()
            .filter_map(|entry| {
                Some(SeededFileHash {
                    path: entry.path,
                    size_bytes: entry.size_bytes.parse().ok()?,
                    mtime_ns: entry.mtime_ns.parse().ok()?,
                    ctime_ns: entry.ctime_ns.parse().ok()?,
                    content_hash: entry.content_hash,
                })
            })
            .collect();
        self.inner
            .seed_hash_cache(seeded)
            .map(|accepted| accepted as u32)
            .map_err(vfs_error)
    }

    #[pyo3(signature = (path, options=None))]
    fn stat<'py>(
        &self,
        py: Python<'py>,
        path: String,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = optional_value(options)?;
        let fields = metadata_fields_from_options(options.as_ref())?;
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = storage
                .stat_with_metadata_fields(&path, fields)
                .await
                .map_err(vfs_error)?
                .map(VfsMetadata::from);
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    #[pyo3(signature = (path, options=None))]
    fn list_dir<'py>(
        &self,
        py: Python<'py>,
        path: String,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = optional_value(options)?;
        let filter = list_filter_from_options(options.as_ref())?;
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result: Vec<_> = storage
                .list_dir_with_metadata(&path, filter)
                .await
                .map_err(vfs_error)?
                .into_iter()
                .map(VfsMetadata::from)
                .collect();
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    fn metadata_many<'py>(
        &self,
        py: Python<'py>,
        paths: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result: Vec<_> = storage
                .metadata_many(&paths, VfsStorageMetadataFields::default())
                .await
                .map_err(vfs_error)?
                .into_iter()
                .map(|metadata| metadata.map(VfsMetadata::from))
                .collect();
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    #[pyo3(signature = (prefix, options=None))]
    fn prefetch_subtree<'py>(
        &self,
        py: Python<'py>,
        prefix: String,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options: VfsPrefetchOptions = options.map(from_python).transpose()?.unwrap_or_default();
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = storage
                .prefetch_subtree(
                    &prefix,
                    VfsStoragePrefetchOptions {
                        include_small_file_bytes: options.include_small_file_bytes.unwrap_or(false),
                        max_entries: options.max_entries.map(i64::from),
                        max_pack_bytes: options.max_pack_bytes.map(u64::from),
                    },
                )
                .await
                .map_err(vfs_error)?;
            Python::with_gil(|py| {
                let output = PyList::empty(py);
                for (path, body) in result.warmed_file_bytes {
                    let item = PyDict::new(py);
                    item.set_item("path", path)?;
                    item.set_item("body", PyBytes::new(py, &body))?;
                    output.append(item)?;
                }
                Ok(output.into_any().unbind())
            })
        })
    }

    #[pyo3(signature = (path, options=None))]
    fn mkdir<'py>(
        &self,
        py: Python<'py>,
        path: String,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = optional_value(options)?;
        let mode = mode_from_options(options.as_ref(), "invalid VFS mkdir mode")?;
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            storage
                .mkdir_with_mode(&path, mode)
                .await
                .map_err(vfs_error)
        })
    }

    fn create_symlink<'py>(
        &self,
        py: Python<'py>,
        path: String,
        target: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            storage
                .create_symlink(&path, &target)
                .await
                .map_err(vfs_error)
        })
    }

    fn create_hard_link<'py>(
        &self,
        py: Python<'py>,
        source: String,
        destination: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = storage
                .create_hard_link(&source, &destination)
                .await
                .map_err(vfs_error)?;
            let result = serde_json::json!({
                "source": VfsMetadata::from(result.source),
                "destination": VfsMetadata::from(result.destination),
            });
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    fn find_hard_link_alias<'py>(
        &self,
        py: Python<'py>,
        file_id: String,
        excluding_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            storage
                .find_hard_link_alias(&file_id, &excluding_path)
                .await
                .map_err(vfs_error)
        })
    }

    #[pyo3(signature = (path, options=None))]
    fn remove<'py>(
        &self,
        py: Python<'py>,
        path: String,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = optional_value(options)?;
        let precondition = precondition_from_options(options.as_ref())?;
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = storage
                .delete_file_with_metadata(&path, precondition)
                .await
                .map_err(vfs_error)?;
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    fn rmdir<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            storage.rmdir(&path).await.map_err(vfs_error)
        })
    }

    fn rename<'py>(
        &self,
        py: Python<'py>,
        from_: String,
        to: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = storage
                .rename_with_metadata(&from_, &to)
                .await
                .map_err(vfs_error)?;
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    fn apply_namespace_batch<'py>(
        &self,
        py: Python<'py>,
        mutations: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mutations = value_from_python(mutations)?;
        validate_namespace_modes(&mutations)?;
        let mutations = serde_json::from_value::<Vec<VfsStorageNamespaceMutation>>(mutations)
            .map_err(|error| invalid_argument(format!("invalid namespace batch: {error}")))?;
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            storage
                .apply_namespace_batch(mutations)
                .await
                .map_err(vfs_error)
        })
    }

    fn write_many<'py>(
        &self,
        py: Python<'py>,
        writes: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let writes = storage_writes_from_value(value_from_python(writes)?)?;
        let storage = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = storage.write_many_atomic(writes).await.map_err(vfs_error)?;
            Python::with_gil(|py| to_python(py, &result))
        })
    }
}

#[pyclass(module = "chevalier.chevalier")]
pub struct VfsContentHasher {
    inner: chevalier_vfs_hash::ContentHasher,
}

#[pymethods]
impl VfsContentHasher {
    #[new]
    fn new() -> Self {
        Self {
            inner: chevalier_vfs_hash::ContentHasher::new(),
        }
    }

    fn update(&mut self, chunk: Vec<u8>) {
        self.inner.update(&chunk);
    }

    fn digest(&self) -> String {
        self.inner.digest()
    }
}

#[pyfunction]
pub fn vfs_content_hash(bytes: Vec<u8>) -> String {
    chevalier_vfs_hash::hash_bytes(&bytes)
}

#[pyfunction]
pub fn vfs_content_hash_algorithm() -> String {
    chevalier_vfs_hash::algorithm().as_str().to_string()
}
