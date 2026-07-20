// @dive-file: Gateway-backed optimized VFS storage client.
// @dive-rel: Lets remote consumers use the same `OptimizedVfsStorage` read surface while
// @dive-rel: the HTTP/FUSE gateway protocol remains owned by chevalier-sandbox.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};
use tokio_util::io::ReaderStream;

use crate::{
    OptimizedVfsStorage, VfsStorageDeleteResult, VfsStorageDirListFilter, VfsStorageEntryKind,
    VfsStorageError, VfsStorageHardLinkResult, VfsStorageMetadata, VfsStorageMetadataFields,
    VfsStorageNamespaceMutation, VfsStorageObjectState, VfsStoragePrefetchOptions,
    VfsStoragePrefetchResult, VfsStorageReadIfChanged, VfsStorageReadIfChangedResult,
    VfsStorageReadRange, VfsStorageRenameResult, VfsStorageResult, VfsStorageSubtreeOptions,
    VfsStorageWrite, VfsStorageWriteOptions, VfsStorageWritePrecondition, VfsStorageWriteResult,
    pack::hex_hash,
};

const COMPONENT_HEADER: &str = "x-chevalier-vfs-component";
const OPERATION_HEADER: &str = "x-chevalier-vfs-operation";
const REASON_HEADER: &str = "x-chevalier-vfs-reason";
const RESOURCE_KEY_HEADER: &str = "x-chevalier-vfs-resource-key";
const SURFACE_KIND_HEADER: &str = "x-chevalier-vfs-surface-kind";
const LOCK_OWNER_TOKEN_HEADER: &str = "x-chevalier-vfs-lock-owner-token";
const PRECONDITION_FINGERPRINT_HEADER: &str = "x-chevalier-vfs-precondition-fingerprint";
const PRECONDITION_SECONDARY_FINGERPRINT_HEADER: &str =
    "x-chevalier-vfs-precondition-secondary-fingerprint";
const EXECUTABLE_HEADER: &str = "x-chevalier-vfs-executable";
const EXPECTED_CONTENT_HASH_HEADER: &str = "x-chevalier-vfs-expected-content-sha256";
const STREAM_UPLOAD_HEADER: &str = "x-chevalier-vfs-stream-upload";
const DEFAULT_COMPONENT: &str = "vfs_gateway_storage";
const DEFAULT_REASON: &str = "gateway vfs storage mutation";
const OP_WRITE: &str = "vfs_write_through";
const OP_MKDIR: &str = "vfs_mkdir";
const OP_UNLINK: &str = "vfs_unlink";
const OP_RMDIR: &str = "vfs_rmdir";
const OP_RENAME: &str = "vfs_rename";
const OP_SYMLINK: &str = "vfs_symlink";
const OP_HARD_LINK: &str = "vfs_hard_link";
const OP_NAMESPACE_BATCH: &str = "vfs_namespace_batch";
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct GatewayVfsStorageConfig {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub scope_path: String,
    pub component: String,
    pub surface_kind: Option<String>,
    pub mutation_reason: String,
}

impl GatewayVfsStorageConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            auth_token: None,
            scope_path: String::new(),
            component: DEFAULT_COMPONENT.to_string(),
            surface_kind: None,
            mutation_reason: DEFAULT_REASON.to_string(),
        }
    }

    pub fn with_auth_token(mut self, auth_token: impl Into<String>) -> Self {
        self.auth_token = Some(auth_token.into());
        self
    }

    pub fn with_scope_path(mut self, scope_path: impl Into<String>) -> Self {
        self.scope_path = scope_path.into();
        self
    }

    pub fn with_component(mut self, component: impl Into<String>) -> Self {
        self.component = component.into();
        self
    }

    pub fn with_surface_kind(mut self, surface_kind: impl Into<String>) -> Self {
        self.surface_kind = Some(surface_kind.into());
        self
    }

    pub fn with_mutation_reason(mut self, reason: impl Into<String>) -> Self {
        self.mutation_reason = reason.into();
        self
    }
}

#[derive(Clone)]
pub struct GatewayVfsStorage {
    cfg: GatewayVfsStorageConfig,
    client: Client,
}

impl GatewayVfsStorage {
    pub fn new(cfg: GatewayVfsStorageConfig) -> Self {
        Self {
            cfg,
            client: Client::builder()
                .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
                .timeout(DEFAULT_REQUEST_TIMEOUT)
                .build()
                .expect("default VFS gateway HTTP client must build"),
        }
    }

    pub fn with_client(cfg: GatewayVfsStorageConfig, client: Client) -> Self {
        Self { cfg, client }
    }

    fn url(&self, suffix: &str) -> String {
        format!("{}{}", self.cfg.endpoint.trim_end_matches('/'), suffix)
    }

    fn path_arg(&self, relative: &str) -> String {
        let scope = self.cfg.scope_path.trim_matches('/');
        let relative = relative.trim_matches('/');
        match (scope.is_empty(), relative.is_empty()) {
            (true, true) => String::new(),
            (true, false) => relative.to_string(),
            (false, true) => scope.to_string(),
            (false, false) => format!("{scope}/{relative}"),
        }
    }

    fn unscoped_path(&self, path: impl Into<String>) -> String {
        let path = path.into();
        let scope = self.cfg.scope_path.trim_matches('/');
        let path = path.trim_matches('/').to_string();
        if scope.is_empty() {
            return path;
        }
        if path == scope {
            return String::new();
        }
        path.strip_prefix(&format!("{scope}/"))
            .map(str::to_string)
            .unwrap_or(path)
    }

    fn authorize(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.cfg.auth_token.as_deref() {
            Some(token) if !token.is_empty() => builder.bearer_auth(token),
            _ => builder,
        }
    }

    async fn send(&self, builder: reqwest::RequestBuilder) -> VfsStorageResult<reqwest::Response> {
        let response = self.authorize(builder).send().await.map_err(|err| {
            VfsStorageError::Internal(format!("vfs gateway request failed: {err}"))
        })?;
        if response.status().is_success() || response.status() == StatusCode::PARTIAL_CONTENT {
            return Ok(response);
        }
        Err(storage_error_from_response(response).await)
    }

    async fn acquire_lease(
        &self,
        path: &str,
        mutation_count: i32,
        reason: &str,
    ) -> VfsStorageResult<GatewayLeaseGrant> {
        let response = self
            .send(
                self.client
                    .post(self.url("/lease"))
                    .json(&LeaseAcquireRequest {
                        path: self.path_arg(path),
                        mutation_count: Some(mutation_count.max(1)),
                        component: Some(self.cfg.component.clone()),
                        run_id: None,
                        reason: Some(reason.to_string()),
                    }),
            )
            .await?;
        response
            .json()
            .await
            .map_err(|err| VfsStorageError::Internal(format!("decode gateway lease: {err}")))
    }

    async fn release_lease(&self, lease: &GatewayLeaseGrant) -> VfsStorageResult<()> {
        self.send(
            self.client
                .delete(self.url("/lease"))
                .json(&LeaseReleaseRequest {
                    resource_key: lease.resource_key.clone(),
                    owner_token: lease.owner_token.clone(),
                }),
        )
        .await
        .map(|_| ())
    }

    fn mutation_headers(
        &self,
        builder: reqwest::RequestBuilder,
        lease: &GatewayLeaseGrant,
        operation: &str,
    ) -> reqwest::RequestBuilder {
        let builder = builder
            .header(COMPONENT_HEADER, self.cfg.component.as_str())
            .header(OPERATION_HEADER, operation)
            .header(REASON_HEADER, self.cfg.mutation_reason.as_str())
            .header(RESOURCE_KEY_HEADER, lease.resource_key.as_str())
            .header(LOCK_OWNER_TOKEN_HEADER, lease.owner_token.as_str());
        match self.cfg.surface_kind.as_deref() {
            Some(surface_kind) if !surface_kind.is_empty() => {
                builder.header(SURFACE_KIND_HEADER, surface_kind)
            }
            _ => builder,
        }
    }

    fn mutation_headers_with_precondition(
        &self,
        builder: reqwest::RequestBuilder,
        lease: &GatewayLeaseGrant,
        operation: &str,
        precondition: Option<&VfsStorageWritePrecondition>,
    ) -> reqwest::RequestBuilder {
        let mut builder = self.mutation_headers(builder, lease, operation);
        if let Some(fingerprint) = precondition.and_then(|value| value.fingerprint.as_deref()) {
            builder = builder.header(PRECONDITION_FINGERPRINT_HEADER, fingerprint);
        }
        if let Some(fingerprint) =
            precondition.and_then(|value| value.secondary_fingerprint.as_deref())
        {
            builder = builder.header(PRECONDITION_SECONDARY_FINGERPRINT_HEADER, fingerprint);
        }
        builder
    }

    async fn release_after<T>(
        &self,
        lease: &GatewayLeaseGrant,
        result: VfsStorageResult<T>,
    ) -> VfsStorageResult<T> {
        let release = self.release_lease(lease).await;
        match (result, release) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(_release_error)) => Err(error),
        }
    }
}

#[async_trait::async_trait]
impl OptimizedVfsStorage for GatewayVfsStorage {
    fn backend_name(&self) -> &'static str {
        "gateway"
    }

    async fn stat(&self, path: &str) -> VfsStorageResult<Option<VfsStorageMetadata>> {
        self.stat_with_metadata_fields(path, VfsStorageMetadataFields::default())
            .await
    }

    async fn stat_with_metadata_fields(
        &self,
        path: &str,
        fields: VfsStorageMetadataFields,
    ) -> VfsStorageResult<Option<VfsStorageMetadata>> {
        let mut query = vec![("path", self.path_arg(path))];
        if let Some(max_hash_bytes) = fields.max_hash_bytes {
            query.push(("max_hash_bytes", max_hash_bytes.to_string()));
        }
        let response = match self
            .send(self.client.get(self.url("/stat")).query(&query))
            .await
        {
            Ok(response) => response,
            Err(VfsStorageError::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let metadata = response
            .json::<RemoteMetadata>()
            .await
            .map_err(|err| VfsStorageError::Internal(format!("decode gateway stat: {err}")))?;
        Ok(Some(metadata.into_storage_metadata(path.to_string())?))
    }

    async fn metadata_many(
        &self,
        paths: &[String],
        _fields: VfsStorageMetadataFields,
    ) -> VfsStorageResult<Vec<Option<VfsStorageMetadata>>> {
        let scoped_paths = paths
            .iter()
            .map(|path| self.path_arg(path))
            .collect::<Vec<_>>();
        let response = self
            .send(
                self.client
                    .post(self.url("/metadata-many"))
                    .json(&PathBatchRequest {
                        paths: scoped_paths,
                    }),
            )
            .await?;
        let response = response
            .json::<MetadataManyResponse>()
            .await
            .map_err(|err| {
                VfsStorageError::Internal(format!("decode gateway metadata_many: {err}"))
            })?;
        Ok(response
            .entries
            .into_iter()
            .zip(paths.iter())
            .map(|(entry, path)| {
                entry
                    .map(|metadata| metadata.into_storage_metadata(path.clone()))
                    .transpose()
            })
            .collect::<VfsStorageResult<Vec<_>>>()?)
    }

    async fn list_dir_with_metadata(
        &self,
        path: &str,
        filter: VfsStorageDirListFilter,
    ) -> VfsStorageResult<Vec<VfsStorageMetadata>> {
        let mut query = vec![("path", self.path_arg(path))];
        if let Some(name_like) = filter.name_like {
            query.push(("name_like", name_like));
        }
        if let Some(name_not_like) = filter.name_not_like {
            query.push(("name_not_like", name_not_like));
        }
        if let Some(entry_kind) = filter.entry_kind {
            query.push(("entry_kind", entry_kind.as_str().to_string()));
        }
        if let Some(limit) = filter.limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(order) = filter.order {
            query.push(("order", dir_list_order_arg(order).to_string()));
        }
        if let Some(max_hash_bytes) = filter.max_hash_bytes {
            query.push(("max_hash_bytes", max_hash_bytes.to_string()));
        }
        let response = self
            .send(self.client.get(self.url("/tree")).query(&query))
            .await?;
        let entries = response
            .json::<Vec<RemoteDirEntry>>()
            .await
            .map_err(|err| VfsStorageError::Internal(format!("decode gateway tree: {err}")))?;
        entries
            .into_iter()
            .map(|entry| {
                let child_path = join_path(path, entry.name.as_str());
                entry.into_storage_metadata(child_path)
            })
            .collect()
    }

    async fn list_subtree_file_metadata(
        &self,
        prefix: &str,
        options: VfsStorageSubtreeOptions,
    ) -> VfsStorageResult<Vec<VfsStorageMetadata>> {
        let response =
            self.send(self.client.post(self.url("/subtree-metadata")).json(
                &SubtreeMetadataRequest {
                    prefix: self.path_arg(prefix),
                    include_object_state: options.include_object_state,
                    include_token_count: options.include_token_count,
                    limit: options.limit,
                    max_hash_bytes: options.max_hash_bytes,
                },
            ))
            .await?;
        let response = response
            .json::<SubtreeMetadataResponse>()
            .await
            .map_err(|err| {
                VfsStorageError::Internal(format!("decode gateway subtree metadata: {err}"))
            })?;
        response
            .entries
            .into_iter()
            .map(|entry| entry.into_storage_metadata(&self))
            .collect()
    }

    async fn read(&self, path: &str) -> VfsStorageResult<Bytes> {
        let response = self
            .send(
                self.client
                    .get(self.url("/file/raw"))
                    .query(&[("path", self.path_arg(path))]),
            )
            .await?;
        response
            .bytes()
            .await
            .map_err(|err| VfsStorageError::Internal(format!("read gateway bytes: {err}")))
    }

    async fn read_range(&self, path: &str, range: VfsStorageReadRange) -> VfsStorageResult<Bytes> {
        let end = range.offset.saturating_add(range.length.saturating_sub(1));
        let response = self
            .send(
                self.client
                    .get(self.url("/file/raw"))
                    .query(&[("path", self.path_arg(path))])
                    .header(header::RANGE, format!("bytes={}-{}", range.offset, end)),
            )
            .await?;
        response
            .bytes()
            .await
            .map_err(|err| VfsStorageError::Internal(format!("read gateway range: {err}")))
    }

    async fn read_many(&self, paths: &[String]) -> VfsStorageResult<Vec<(String, Bytes)>> {
        let scoped_paths = paths
            .iter()
            .map(|path| self.path_arg(path))
            .collect::<Vec<_>>();
        let response = self
            .send(
                self.client
                    .post(self.url("/read-many"))
                    .json(&PathBatchRequest {
                        paths: scoped_paths,
                    }),
            )
            .await?;
        let response = response
            .json::<ReadManyResponse>()
            .await
            .map_err(|err| VfsStorageError::Internal(format!("decode gateway read_many: {err}")))?;
        Ok(response
            .entries
            .into_iter()
            .zip(paths.iter())
            .filter_map(|(entry, path)| entry.map(|bytes| (path.clone(), Bytes::from(bytes))))
            .collect())
    }

    async fn read_many_if_etag_mismatch(
        &self,
        requests: &[VfsStorageReadIfChanged],
    ) -> VfsStorageResult<Vec<VfsStorageReadIfChangedResult>> {
        let paths = requests
            .iter()
            .map(|request| request.path.clone())
            .collect::<Vec<_>>();
        let metadata = self
            .metadata_many(&paths, VfsStorageMetadataFields::default())
            .await?;
        let mut changed_paths = Vec::new();
        let mut changed_indexes = Vec::new();
        let mut out = Vec::with_capacity(requests.len());
        for (index, (request, metadata)) in requests.iter().zip(metadata).enumerate() {
            match metadata {
                Some(metadata) if metadata.content_hash == request.known_content_hash => {
                    out.push(VfsStorageReadIfChangedResult {
                        path: request.path.clone(),
                        content_hash: metadata.content_hash,
                        bytes: None,
                    });
                }
                Some(metadata) => {
                    changed_indexes.push((index, metadata.content_hash));
                    changed_paths.push(request.path.clone());
                    out.push(VfsStorageReadIfChangedResult {
                        path: request.path.clone(),
                        content_hash: None,
                        bytes: None,
                    });
                }
                None => out.push(VfsStorageReadIfChangedResult {
                    path: request.path.clone(),
                    content_hash: None,
                    bytes: None,
                }),
            }
        }
        let changed_bytes = self
            .read_many(&changed_paths)
            .await?
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        for ((index, content_hash), path) in changed_indexes.into_iter().zip(changed_paths) {
            out[index].content_hash = content_hash;
            out[index].bytes = changed_bytes.get(&path).cloned();
        }
        Ok(out)
    }

    async fn write(
        &self,
        path: &str,
        bytes: Bytes,
        precondition: Option<VfsStorageWritePrecondition>,
    ) -> VfsStorageResult<VfsStorageWriteResult> {
        self.write_with_options(path, bytes, precondition, None)
            .await
    }

    async fn write_with_options(
        &self,
        path: &str,
        bytes: Bytes,
        precondition: Option<VfsStorageWritePrecondition>,
        options: Option<VfsStorageWriteOptions>,
    ) -> VfsStorageResult<VfsStorageWriteResult> {
        let previous_hash = self
            .stat(path)
            .await?
            .and_then(|metadata| metadata.content_hash);
        let next_hash = hex_hash(&bytes);
        let lease = self
            .acquire_lease(path, 1, self.cfg.mutation_reason.as_str())
            .await?;
        let mut request = self.mutation_headers_with_precondition(
            self.client
                .put(self.url("/file"))
                .query(&[("path", self.path_arg(path))])
                .body(bytes),
            &lease,
            OP_WRITE,
            precondition.as_ref(),
        );
        if let Some(options) = options {
            request = request.header(EXECUTABLE_HEADER, options.executable.to_string());
        }
        let result = self.send(request).await.map(|_| VfsStorageWriteResult {
            path: path.to_string(),
            content_hash: next_hash.clone(),
            previous_hash: previous_hash.clone(),
            changed: previous_hash.as_deref() != Some(next_hash.as_str()),
        });
        self.release_after(&lease, result).await
    }

    async fn write_from_local_file(
        &self,
        path: &str,
        source_path: &Path,
        expected_content_hash: Option<&str>,
        precondition: Option<VfsStorageWritePrecondition>,
        options: Option<VfsStorageWriteOptions>,
    ) -> VfsStorageResult<VfsStorageWriteResult> {
        let metadata = tokio::fs::metadata(source_path).await.map_err(|error| {
            VfsStorageError::Internal(format!("stat staged VFS upload: {error}"))
        })?;
        if !metadata.is_file() {
            return Err(VfsStorageError::BadRequest(
                "staged VFS upload source is not a regular file".to_string(),
            ));
        }
        let expected_content_hash = expected_content_hash.ok_or_else(|| {
            VfsStorageError::BadRequest(
                "streamed gateway writes require an expected content hash".to_string(),
            )
        })?;
        let previous_hash = self
            .stat(path)
            .await?
            .and_then(|metadata| metadata.content_hash);
        let lease = self
            .acquire_lease(path, 1, self.cfg.mutation_reason.as_str())
            .await?;
        let file = tokio::fs::File::open(source_path).await.map_err(|error| {
            VfsStorageError::Internal(format!("open staged VFS upload: {error}"))
        })?;
        let body =
            reqwest::Body::wrap(StreamBody::new(ReaderStream::new(file).map_ok(Frame::data)));
        let transfer_seconds = metadata.len().div_ceil(128 * 1024).max(300);
        let mut request = self.mutation_headers_with_precondition(
            self.client
                .put(self.url("/file"))
                .query(&[("path", self.path_arg(path))])
                .header(STREAM_UPLOAD_HEADER, "1")
                .header(EXPECTED_CONTENT_HASH_HEADER, expected_content_hash)
                .header(header::CONTENT_LENGTH, metadata.len())
                .timeout(Duration::from_secs(transfer_seconds))
                .body(body),
            &lease,
            OP_WRITE,
            precondition.as_ref(),
        );
        if let Some(options) = options {
            request = request.header(EXECUTABLE_HEADER, options.executable.to_string());
        }
        let content_hash = expected_content_hash.to_string();
        let result = self.send(request).await.map(|_| VfsStorageWriteResult {
            path: path.to_string(),
            changed: previous_hash.as_deref() != Some(content_hash.as_str()),
            previous_hash: previous_hash.clone(),
            content_hash,
        });
        self.release_after(&lease, result).await
    }

    async fn write_many_atomic(
        &self,
        writes: Vec<VfsStorageWrite>,
    ) -> VfsStorageResult<Vec<VfsStorageWriteResult>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        let original_paths = writes
            .iter()
            .map(|write| write.path.clone())
            .collect::<Vec<_>>();
        let lease = self
            .acquire_lease(
                writes[0].path.as_str(),
                writes.len() as i32,
                self.cfg.mutation_reason.as_str(),
            )
            .await?;
        let body = WriteManyBody {
            writes: writes
                .into_iter()
                .map(|write| WriteManyItem {
                    path: self.path_arg(&write.path),
                    body: write.bytes.to_vec(),
                    precondition: write.precondition.map(WritePrecondition::from),
                })
                .collect(),
        };
        let response = self
            .send(self.mutation_headers(
                self.client.post(self.url("/write-many")).json(&body),
                &lease,
                OP_WRITE,
            ))
            .await;
        let result = match response {
            Ok(response) => response
                .json::<WriteManyResponse>()
                .await
                .map_err(|err| {
                    VfsStorageError::Internal(format!("decode gateway write_many: {err}"))
                })
                .map(|response| {
                    response
                        .results
                        .into_iter()
                        .zip(original_paths)
                        .map(|(result, original_path)| {
                            let WriteManyResult {
                                path: _scoped_path,
                                content_hash,
                                previous_hash,
                                changed,
                            } = result;
                            VfsStorageWriteResult {
                                path: original_path,
                                content_hash,
                                previous_hash,
                                changed,
                            }
                        })
                        .collect()
                }),
            Err(error) => Err(error),
        };
        self.release_after(&lease, result).await
    }

    async fn write_many_if_changed_atomic(
        &self,
        writes: Vec<VfsStorageWrite>,
    ) -> VfsStorageResult<Vec<VfsStorageWriteResult>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        let paths = writes
            .iter()
            .map(|write| write.path.clone())
            .collect::<Vec<_>>();
        let candidate_hashes = writes
            .iter()
            .map(|write| hex_hash(&write.bytes))
            .collect::<Vec<_>>();
        let current = self
            .metadata_many(&paths, VfsStorageMetadataFields::default())
            .await?;
        let mut out = vec![None; writes.len()];
        let mut changed_indexes = Vec::new();
        let mut changed_writes = Vec::new();
        for (index, write) in writes.into_iter().enumerate() {
            let content_hash = candidate_hashes[index].clone();
            let previous_hash = current[index]
                .as_ref()
                .and_then(|metadata| metadata.content_hash.clone());
            if previous_hash.as_deref() == Some(content_hash.as_str()) {
                out[index] = Some(VfsStorageWriteResult {
                    path: write.path,
                    content_hash,
                    previous_hash,
                    changed: false,
                });
            } else {
                changed_indexes.push(index);
                changed_writes.push(write);
            }
        }
        if !changed_writes.is_empty() {
            let changed_results = self.write_many_atomic(changed_writes).await?;
            for (index, result) in changed_indexes.into_iter().zip(changed_results) {
                out[index] = Some(result);
            }
        }
        out.into_iter()
            .map(|entry| {
                entry.ok_or_else(|| {
                    VfsStorageError::Internal(
                        "gateway changed-only write planner lost an output slot".to_string(),
                    )
                })
            })
            .collect()
    }

    async fn mkdir(&self, path: &str) -> VfsStorageResult<()> {
        let lease = self
            .acquire_lease(path, 1, self.cfg.mutation_reason.as_str())
            .await?;
        let result = self
            .send(
                self.mutation_headers(
                    self.client
                        .put(self.url("/dir"))
                        .query(&[("path", self.path_arg(path))]),
                    &lease,
                    OP_MKDIR,
                ),
            )
            .await
            .map(|_| ());
        self.release_after(&lease, result).await
    }

    async fn create_symlink(&self, path: &str, target: &str) -> VfsStorageResult<()> {
        let lease = self
            .acquire_lease(path, 1, self.cfg.mutation_reason.as_str())
            .await?;
        let result = self
            .send(self.mutation_headers(
                self.client.put(self.url("/symlink")).query(&[
                    ("path", self.path_arg(path)),
                    ("target", target.to_string()),
                ]),
                &lease,
                OP_SYMLINK,
            ))
            .await
            .map(|_| ());
        self.release_after(&lease, result).await
    }

    async fn delete_file_with_metadata(
        &self,
        path: &str,
        precondition: Option<VfsStorageWritePrecondition>,
    ) -> VfsStorageResult<VfsStorageDeleteResult> {
        let lease = self
            .acquire_lease(path, 1, self.cfg.mutation_reason.as_str())
            .await?;
        let result = self
            .send(self.mutation_headers_with_precondition(
                self.client.delete(self.url("/file")).query(&[
                    ("path", self.path_arg(path)),
                    ("return_metadata", "true".to_string()),
                ]),
                &lease,
                OP_UNLINK,
                precondition.as_ref(),
            ))
            .await;
        let result = match result {
            Ok(response) => response
                .json::<DeleteMetadataResponse>()
                .await
                .map_err(|err| {
                    VfsStorageError::Internal(format!("decode gateway delete metadata: {err}"))
                })
                .and_then(|response| {
                    response
                        .previous
                        .map(|metadata| metadata.into_storage_metadata(path.to_string()))
                        .transpose()
                        .map(|previous| VfsStorageDeleteResult { previous })
                }),
            Err(error) => Err(error),
        };
        self.release_after(&lease, result).await
    }

    async fn rmdir(&self, path: &str) -> VfsStorageResult<()> {
        let lease = self
            .acquire_lease(path, 1, self.cfg.mutation_reason.as_str())
            .await?;
        let result = self
            .send(
                self.mutation_headers(
                    self.client
                        .delete(self.url("/dir"))
                        .query(&[("path", self.path_arg(path))]),
                    &lease,
                    OP_RMDIR,
                ),
            )
            .await
            .map(|_| ());
        self.release_after(&lease, result).await
    }

    async fn rename_with_metadata(
        &self,
        from: &str,
        to: &str,
    ) -> VfsStorageResult<VfsStorageRenameResult> {
        let lease = self
            .acquire_lease(from, 1, self.cfg.mutation_reason.as_str())
            .await?;
        let result = self
            .send(self.mutation_headers(
                self.client.post(self.url("/rename")).query(&[
                    ("from", self.path_arg(from)),
                    ("to", self.path_arg(to)),
                    ("return_metadata", "true".to_string()),
                ]),
                &lease,
                OP_RENAME,
            ))
            .await;
        let result = match result {
            Ok(response) => response
                .json::<RenameMetadataResponse>()
                .await
                .map_err(|err| {
                    VfsStorageError::Internal(format!("decode gateway rename metadata: {err}"))
                })
                .and_then(|response| {
                    let previous = response
                        .previous
                        .map(|metadata| metadata.into_storage_metadata(from.to_string()))
                        .transpose()?;
                    let current = response
                        .current
                        .map(|metadata| metadata.into_storage_metadata(to.to_string()))
                        .transpose()?;
                    Ok(VfsStorageRenameResult { previous, current })
                }),
            Err(error) => Err(error),
        };
        self.release_after(&lease, result).await
    }

    async fn create_hard_link(
        &self,
        source: &str,
        destination: &str,
    ) -> VfsStorageResult<VfsStorageHardLinkResult> {
        let lease_path = common_path_parent(source, destination);
        let lease = self
            .acquire_lease(&lease_path, 1, self.cfg.mutation_reason.as_str())
            .await?;
        let result = self
            .send(
                self.mutation_headers(
                    self.client
                        .post(self.url("/hard-link/v1"))
                        .json(&HardLinkRequest {
                            source_path: self.path_arg(source),
                            destination_path: self.path_arg(destination),
                        }),
                    &lease,
                    OP_HARD_LINK,
                ),
            )
            .await;
        let result = match result {
            Ok(response) => response
                .json::<HardLinkMetadataResponse>()
                .await
                .map_err(|error| {
                    VfsStorageError::Internal(format!("decode gateway hard-link metadata: {error}"))
                })
                .and_then(|response| {
                    Ok(VfsStorageHardLinkResult {
                        source: response.source.into_storage_metadata(source.to_string())?,
                        destination: response
                            .destination
                            .into_storage_metadata(destination.to_string())?,
                    })
                }),
            Err(error) => Err(error),
        };
        self.release_after(&lease, result).await
    }

    async fn find_hard_link_alias(
        &self,
        file_id: &str,
        excluding_path: &str,
    ) -> VfsStorageResult<Option<String>> {
        let response =
            self.send(self.client.post(self.url("/hard-link-alias/v1")).json(
                &HardLinkAliasRequest {
                    file_id,
                    excluding_path: self.path_arg(excluding_path),
                },
            ))
            .await?
            .json::<HardLinkAliasResponse>()
            .await
            .map_err(|error| {
                VfsStorageError::Internal(format!(
                    "decode gateway hard-link alias response: {error}"
                ))
            })?;
        Ok(response.path.map(|path| self.unscoped_path(path)))
    }

    async fn apply_namespace_batch(
        &self,
        mutations: Vec<VfsStorageNamespaceMutation>,
    ) -> VfsStorageResult<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        let lease_path = common_namespace_parent(mutations.as_slice());
        let lease = self
            .acquire_lease(
                lease_path.as_str(),
                mutations.len() as i32,
                self.cfg.mutation_reason.as_str(),
            )
            .await?;
        let body = NamespaceBatchBody {
            mutations: mutations
                .into_iter()
                .map(|mutation| NamespaceMutation::from_storage(mutation, self))
                .collect(),
        };
        let result = self
            .send(self.mutation_headers(
                self.client.post(self.url("/namespace-many")).json(&body),
                &lease,
                OP_NAMESPACE_BATCH,
            ))
            .await
            .map(|_| ());
        self.release_after(&lease, result).await
    }

    async fn prefetch_subtree(
        &self,
        prefix: &str,
        options: VfsStoragePrefetchOptions,
    ) -> VfsStorageResult<VfsStoragePrefetchResult> {
        let response =
            self.send(self.client.post(self.url("/prefetch-subtree")).json(
                &PrefetchSubtreeRequest {
                    prefix: self.path_arg(prefix),
                    include_small_file_bytes: options.include_small_file_bytes,
                    max_entries: options.max_entries,
                    max_pack_bytes: options.max_pack_bytes,
                },
            ))
            .await?;
        let response = response
            .json::<PrefetchSubtreeResponse>()
            .await
            .map_err(|err| VfsStorageError::Internal(format!("decode gateway prefetch: {err}")))?;
        Ok(VfsStoragePrefetchResult {
            warmed_file_bytes: response
                .warmed_file_bytes
                .into_iter()
                .map(|entry| (self.unscoped_path(entry.path), Bytes::from(entry.body)))
                .collect(),
        })
    }
}

#[derive(Serialize)]
struct PathBatchRequest {
    paths: Vec<String>,
}

#[derive(Serialize)]
struct NamespaceBatchBody {
    mutations: Vec<NamespaceMutation>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum NamespaceMutation {
    CreateDirectory {
        path: String,
    },
    CreateSymlink {
        path: String,
        target: String,
    },
    DeleteFile {
        path: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        precondition: Option<WritePrecondition>,
    },
    RemoveDirectory {
        path: String,
    },
    Rename {
        from: String,
        to: String,
    },
}

impl NamespaceMutation {
    fn from_storage(mutation: VfsStorageNamespaceMutation, storage: &GatewayVfsStorage) -> Self {
        match mutation {
            VfsStorageNamespaceMutation::CreateDirectory { path } => Self::CreateDirectory {
                path: storage.path_arg(path.as_str()),
            },
            VfsStorageNamespaceMutation::CreateSymlink { path, target } => Self::CreateSymlink {
                path: storage.path_arg(path.as_str()),
                target,
            },
            VfsStorageNamespaceMutation::DeleteFile { path, precondition } => Self::DeleteFile {
                path: storage.path_arg(path.as_str()),
                precondition: precondition.map(WritePrecondition::from),
            },
            VfsStorageNamespaceMutation::RemoveDirectory { path } => Self::RemoveDirectory {
                path: storage.path_arg(path.as_str()),
            },
            VfsStorageNamespaceMutation::Rename { from, to } => Self::Rename {
                from: storage.path_arg(from.as_str()),
                to: storage.path_arg(to.as_str()),
            },
        }
    }
}

fn common_namespace_parent(mutations: &[VfsStorageNamespaceMutation]) -> String {
    let mut parents = mutations.iter().flat_map(|mutation| {
        mutation
            .paths()
            .into_iter()
            .filter(|path| !path.is_empty())
            .map(parent_path)
    });
    let Some(first) = parents.next() else {
        return String::new();
    };
    let mut common = first
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    for parent in parents {
        let segments = parent
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let shared = common
            .iter()
            .zip(segments)
            .take_while(|(left, right)| left.as_str() == *right)
            .count();
        common.truncate(shared);
    }
    common.join("/")
}

fn common_path_parent(left: &str, right: &str) -> String {
    let left = left.trim_matches('/').split('/').collect::<Vec<_>>();
    let right = right.trim_matches('/').split('/').collect::<Vec<_>>();
    left.iter()
        .zip(right.iter())
        .take_while(|(left, right)| left == right)
        .map(|(component, _)| *component)
        .collect::<Vec<_>>()
        .join("/")
}

fn parent_path(path: &str) -> String {
    path.trim_matches('/')
        .rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

#[derive(Serialize)]
struct LeaseAcquireRequest {
    path: String,
    mutation_count: Option<i32>,
    component: Option<String>,
    run_id: Option<String>,
    reason: Option<String>,
}

#[derive(Deserialize)]
struct GatewayLeaseGrant {
    resource_key: String,
    owner_token: String,
}

#[derive(Serialize)]
struct LeaseReleaseRequest {
    resource_key: String,
    owner_token: String,
}

#[derive(Serialize)]
struct WriteManyBody {
    writes: Vec<WriteManyItem>,
}

#[derive(Serialize)]
struct WriteManyItem {
    path: String,
    body: Vec<u8>,
    precondition: Option<WritePrecondition>,
}

#[derive(Serialize)]
struct WritePrecondition {
    fingerprint: Option<String>,
    secondary_fingerprint: Option<String>,
}

impl From<VfsStorageWritePrecondition> for WritePrecondition {
    fn from(value: VfsStorageWritePrecondition) -> Self {
        Self {
            fingerprint: value.fingerprint,
            secondary_fingerprint: value.secondary_fingerprint,
        }
    }
}

#[derive(Deserialize)]
struct WriteManyResponse {
    results: Vec<WriteManyResult>,
}

#[derive(Deserialize)]
struct WriteManyResult {
    path: String,
    content_hash: String,
    previous_hash: Option<String>,
    changed: bool,
}

#[derive(Deserialize)]
struct MetadataManyResponse {
    entries: Vec<Option<RemoteMetadata>>,
}

#[derive(Deserialize)]
struct DeleteMetadataResponse {
    previous: Option<RemoteMetadata>,
}

#[derive(Deserialize)]
struct RenameMetadataResponse {
    previous: Option<RemoteMetadata>,
    current: Option<RemoteMetadata>,
}

#[derive(Serialize)]
struct HardLinkRequest {
    source_path: String,
    destination_path: String,
}

#[derive(Deserialize)]
struct HardLinkMetadataResponse {
    source: RemoteMetadata,
    destination: RemoteMetadata,
}

#[derive(Serialize)]
struct HardLinkAliasRequest<'a> {
    file_id: &'a str,
    excluding_path: String,
}

#[derive(Deserialize)]
struct HardLinkAliasResponse {
    path: Option<String>,
}

#[derive(Deserialize)]
struct ReadManyResponse {
    entries: Vec<Option<Vec<u8>>>,
}

#[derive(Serialize)]
struct SubtreeMetadataRequest {
    prefix: String,
    include_object_state: bool,
    include_token_count: bool,
    limit: Option<i64>,
    max_hash_bytes: Option<u64>,
}

#[derive(Deserialize)]
struct SubtreeMetadataResponse {
    entries: Vec<RemoteSubtreeMetadataEntry>,
}

#[derive(Deserialize)]
struct RemoteObjectState {
    size_bytes: u64,
    pack_key: String,
    pack_slot_offset: i64,
    pack_slot_length: i64,
    pack_slot_compression: i16,
}

#[derive(Deserialize)]
struct RemoteSubtreeMetadataEntry {
    path: String,
    kind: String,
    size_bytes: u64,
    #[serde(default)]
    file_id: Option<String>,
    #[serde(default = "default_remote_link_count")]
    link_count: u64,
    #[serde(default)]
    executable: bool,
    #[serde(default)]
    link_target: Option<String>,
    content_hash: Option<String>,
    token_count: Option<i32>,
    version: Option<String>,
    updated_at: Option<DateTime<Utc>>,
    object_state: Option<RemoteObjectState>,
}

impl RemoteSubtreeMetadataEntry {
    fn into_storage_metadata(
        self,
        storage: &GatewayVfsStorage,
    ) -> VfsStorageResult<VfsStorageMetadata> {
        let mut metadata = VfsStorageMetadata::new(
            storage.unscoped_path(self.path),
            parse_kind(&self.kind)?,
            self.size_bytes,
        );
        metadata.file_id = self.file_id;
        metadata.link_count = self.link_count;
        metadata.link_target = self.link_target;
        metadata.executable = self.executable;
        metadata.content_hash = self.content_hash;
        metadata.token_count = self.token_count;
        metadata.version = self.version;
        metadata.updated_at = self.updated_at;
        metadata.object_state = self.object_state.map(|state| VfsStorageObjectState {
            size_bytes: state.size_bytes,
            pack_key: state.pack_key,
            pack_slot_offset: state.pack_slot_offset,
            pack_slot_length: state.pack_slot_length,
            pack_slot_compression: state.pack_slot_compression,
        });
        Ok(metadata)
    }
}

#[derive(Serialize)]
struct PrefetchSubtreeRequest {
    prefix: String,
    include_small_file_bytes: bool,
    max_entries: Option<i64>,
    max_pack_bytes: Option<u64>,
}

#[derive(Deserialize)]
struct PrefetchSubtreeResponse {
    warmed_file_bytes: Vec<PrefetchFileBytes>,
}

#[derive(Deserialize)]
struct PrefetchFileBytes {
    path: String,
    body: Vec<u8>,
}

#[derive(Deserialize)]
struct RemoteMetadata {
    kind: String,
    size_bytes: u64,
    #[serde(default)]
    file_id: Option<String>,
    #[serde(default = "default_remote_link_count")]
    link_count: u64,
    #[serde(default)]
    executable: bool,
    #[serde(default)]
    link_target: Option<String>,
    content_hash: Option<String>,
    updated_at: Option<DateTime<Utc>>,
}

impl RemoteMetadata {
    fn into_storage_metadata(self, path: String) -> VfsStorageResult<VfsStorageMetadata> {
        let mut metadata = VfsStorageMetadata::new(path, parse_kind(&self.kind)?, self.size_bytes);
        metadata.file_id = self.file_id;
        metadata.link_count = self.link_count;
        metadata.link_target = self.link_target;
        metadata.executable = self.executable;
        metadata.content_hash = self.content_hash;
        metadata.updated_at = self.updated_at;
        Ok(metadata)
    }
}

#[derive(Deserialize)]
struct RemoteDirEntry {
    name: String,
    kind: String,
    size_bytes: u64,
    #[serde(default)]
    file_id: Option<String>,
    #[serde(default = "default_remote_link_count")]
    link_count: u64,
    #[serde(default)]
    executable: bool,
    #[serde(default)]
    link_target: Option<String>,
    content_hash: Option<String>,
    updated_at: Option<DateTime<Utc>>,
}

impl RemoteDirEntry {
    fn into_storage_metadata(self, path: String) -> VfsStorageResult<VfsStorageMetadata> {
        let mut metadata = VfsStorageMetadata::new(path, parse_kind(&self.kind)?, self.size_bytes);
        metadata.file_id = self.file_id;
        metadata.link_count = self.link_count;
        metadata.link_target = self.link_target;
        metadata.executable = self.executable;
        metadata.content_hash = self.content_hash;
        metadata.updated_at = self.updated_at;
        Ok(metadata)
    }
}

const fn default_remote_link_count() -> u64 {
    1
}

async fn storage_error_from_response(response: reqwest::Response) -> VfsStorageError {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let message = if body.is_empty() {
        status.to_string()
    } else {
        format!("{status} {body}")
    };
    match status {
        StatusCode::NOT_FOUND => VfsStorageError::NotFound(message),
        StatusCode::BAD_REQUEST => VfsStorageError::BadRequest(message),
        StatusCode::UNAUTHORIZED => VfsStorageError::Forbidden(message),
        StatusCode::FORBIDDEN => VfsStorageError::Forbidden(message),
        StatusCode::CONFLICT => VfsStorageError::Conflict(message),
        _ => VfsStorageError::Internal(message),
    }
}

fn parse_kind(kind: &str) -> VfsStorageResult<VfsStorageEntryKind> {
    match kind {
        "file" => Ok(VfsStorageEntryKind::File),
        "directory" => Ok(VfsStorageEntryKind::Directory),
        "symlink" => Ok(VfsStorageEntryKind::Symlink),
        "special" => Ok(VfsStorageEntryKind::Special),
        _ => Err(VfsStorageError::Internal(format!(
            "gateway returned unknown vfs entry kind {kind}"
        ))),
    }
}

fn join_path(parent: &str, name: &str) -> String {
    let parent = parent.trim_matches('/');
    let name = name.trim_matches('/');
    if parent.is_empty() {
        name.to_string()
    } else if name.is_empty() {
        parent.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

fn dir_list_order_arg(order: crate::VfsStorageDirListOrder) -> &'static str {
    match order {
        crate::VfsStorageDirListOrder::KindThenName => "kind_then_name",
        crate::VfsStorageDirListOrder::NameAsc => "name_asc",
        crate::VfsStorageDirListOrder::NameDesc => "name_desc",
        crate::VfsStorageDirListOrder::UpdatedDesc => "updated_desc",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Instant,
    };

    #[tokio::test]
    async fn gateway_list_dir_forwards_filters_and_maps_relative_paths() {
        let (endpoint, requests) = serve_one(
            r#"[{"name":"a.txt","kind":"file","size_bytes":3,"content_hash":"hash-a","updated_at":null}]"#,
        );
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));
        let entries = storage
            .list_dir_with_metadata(
                "jobs",
                VfsStorageDirListFilter {
                    name_like: Some("%.txt".to_string()),
                    name_not_like: Some("%.tmp".to_string()),
                    entry_kind: Some(VfsStorageEntryKind::File),
                    limit: Some(2),
                    order: Some(crate::VfsStorageDirListOrder::NameDesc),
                    max_hash_bytes: Some(64),
                },
            )
            .await
            .expect("list dir");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "jobs/a.txt");
        assert_eq!(entries[0].kind, VfsStorageEntryKind::File);
        assert_eq!(entries[0].content_hash.as_deref(), Some("hash-a"));

        let request = requests.recv().expect("captured request");
        assert_eq!(request.method, "GET");
        assert!(request.target.starts_with("/tree?"));
        assert_query_value(&request.target, "path", "scope/jobs");
        assert_query_value(&request.target, "name_like", "%.txt");
        assert_query_value(&request.target, "name_not_like", "%.tmp");
        assert_query_value(&request.target, "entry_kind", "file");
        assert_query_value(&request.target, "limit", "2");
        assert_query_value(&request.target, "order", "name_desc");
        assert_query_value(&request.target, "max_hash_bytes", "64");
    }

    #[tokio::test]
    async fn gateway_stat_forwards_hash_limit() {
        let (endpoint, requests) =
            serve_one(r#"{"kind":"file","size_bytes":3,"content_hash":null,"updated_at":null}"#);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        let metadata = storage
            .stat_with_metadata_fields(
                "jobs/a.txt",
                VfsStorageMetadataFields {
                    max_hash_bytes: Some(0),
                    ..Default::default()
                },
            )
            .await
            .expect("stat")
            .expect("metadata");

        assert_eq!(metadata.path, "jobs/a.txt");
        assert_eq!(metadata.content_hash, None);
        let request = requests.recv().expect("captured request");
        assert_eq!(request.method, "GET");
        assert!(request.target.starts_with("/stat?"));
        assert_query_value(&request.target, "path", "scope/jobs/a.txt");
        assert_query_value(&request.target, "max_hash_bytes", "0");
    }

    #[tokio::test]
    async fn gateway_preserves_symlink_link_target_metadata() {
        let (endpoint, requests) = serve_sequence(vec![
            r#"{"kind":"symlink","size_bytes":10,"link_target":"target.txt","content_hash":null,"updated_at":null}"#
                .to_string(),
            r#"[{"name":"link.txt","kind":"symlink","size_bytes":10,"link_target":"target.txt","content_hash":null,"updated_at":null}]"#
                .to_string(),
            r#"{"entries":[{"path":"scope/link.txt","kind":"symlink","size_bytes":10,"link_target":"target.txt","content_hash":null,"token_count":null,"version":null,"updated_at":null,"object_state":null}]}"#
                .to_string(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        let stat = storage
            .stat("link.txt")
            .await
            .expect("stat")
            .expect("metadata");
        assert_eq!(stat.kind, VfsStorageEntryKind::Symlink);
        assert_eq!(stat.link_target.as_deref(), Some("target.txt"));

        let listed = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .expect("list dir");
        assert_eq!(listed[0].kind, VfsStorageEntryKind::Symlink);
        assert_eq!(listed[0].link_target.as_deref(), Some("target.txt"));

        let subtree = storage
            .list_subtree_file_metadata("", VfsStorageSubtreeOptions::default())
            .await
            .expect("subtree metadata");
        assert_eq!(subtree[0].kind, VfsStorageEntryKind::Symlink);
        assert_eq!(subtree[0].path, "link.txt");
        assert_eq!(subtree[0].link_target.as_deref(), Some("target.txt"));

        let stat_request = requests.recv().expect("stat request");
        assert_eq!(stat_request.method, "GET");
        assert!(stat_request.target.starts_with("/stat?"));
        let tree_request = requests.recv().expect("tree request");
        assert_eq!(tree_request.method, "GET");
        assert!(tree_request.target.starts_with("/tree?"));
        let subtree_request = requests.recv().expect("subtree request");
        assert_eq!(subtree_request.method, "POST");
        assert_eq!(subtree_request.target, "/subtree-metadata");
    }

    #[tokio::test]
    async fn gateway_write_forwards_precondition_and_executable_metadata() {
        let body = Bytes::from_static(b"next");
        let (endpoint, requests) = serve_sequence(vec![
            r#"{"kind":"file","size_bytes":3,"content_hash":"old-hash","updated_at":null}"#
                .to_string(),
            r#"{"resource_key":"rk","owner_token":"ot"}"#.to_string(),
            String::new(),
            String::new(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        storage
            .write_with_options(
                "script.sh",
                body,
                Some(VfsStorageWritePrecondition {
                    fingerprint: Some("old-hash".to_string()),
                    secondary_fingerprint: None,
                }),
                Some(VfsStorageWriteOptions { executable: true }),
            )
            .await
            .expect("write with executable metadata");

        let stat_request = requests.recv().expect("stat request");
        assert_eq!(stat_request.method, "GET");
        let lease_request = requests.recv().expect("lease request");
        assert_eq!(lease_request.method, "POST");
        let write_request = requests.recv().expect("write request");
        assert_eq!(write_request.method, "PUT");
        assert!(
            write_request
                .headers
                .contains("x-chevalier-vfs-precondition-fingerprint: old-hash")
        );
        assert!(
            write_request
                .headers
                .contains("x-chevalier-vfs-executable: true")
        );
        let release_request = requests.recv().expect("release request");
        assert_eq!(release_request.method, "DELETE");
    }

    #[tokio::test]
    async fn gateway_git_metadata_batch_uses_constant_remote_round_trips() {
        let results = (0..1_000)
            .map(|index| {
                serde_json::json!({
                    "path": format!("scope/.git/objects/ab/{index:04x}"),
                    "content_hash": format!("hash-{index}"),
                    "previous_hash": null,
                    "changed": true,
                })
            })
            .collect::<Vec<_>>();
        let (endpoint, requests) = serve_sequence(vec![
            r#"{"resource_key":"rk","owner_token":"ot"}"#.to_string(),
            serde_json::json!({ "results": results }).to_string(),
            String::new(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));
        let writes = (0..1_000)
            .map(|index| VfsStorageWrite {
                path: format!(".git/objects/ab/{index:04x}"),
                bytes: Bytes::from(format!("object-{index:04}\n")),
                token_count: None,
                precondition: None,
            })
            .collect::<Vec<_>>();

        let started = Instant::now();
        let written = storage
            .write_many_atomic(writes)
            .await
            .expect("gateway Git metadata batch");
        let elapsed = started.elapsed();
        assert_eq!(written.len(), 1_000);
        let captured = (0..3)
            .map(|_| requests.recv().expect("captured gateway request"))
            .collect::<Vec<_>>();
        assert_eq!(
            captured
                .iter()
                .map(|request| request.target.as_str())
                .collect::<Vec<_>>(),
            ["/lease", "/write-many", "/lease"],
        );
        assert_eq!(
            captured[1]
                .body
                .matches(r#""path":"scope/.git/objects/ab/"#)
                .count(),
            1_000,
        );
        assert!(
            requests.try_recv().is_err(),
            "one batch must not degrade into per-entry gateway calls",
        );
        eprintln!("git metadata gateway benchmark: elapsed={elapsed:?}");
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

    fn gateway_write_many_response(writes: &[VfsStorageWrite]) -> String {
        serde_json::json!({
            "results": writes.iter().map(|write| {
                serde_json::json!({
                    "path": format!("scope/{}", write.path),
                    "content_hash": hex_hash(&write.bytes),
                    "previous_hash": null,
                    "changed": true,
                })
            }).collect::<Vec<_>>(),
        })
        .to_string()
    }

    fn gateway_metadata_many_response(writes: &[VfsStorageWrite]) -> String {
        serde_json::json!({
            "entries": writes.iter().map(|write| {
                Some(serde_json::json!({
                    "kind": "file",
                    "size_bytes": write.bytes.len(),
                    "file_id": format!("inode:{}", write.path),
                    "link_count": 1,
                    "content_hash": hex_hash(&write.bytes),
                    "updated_at": null,
                }))
            }).collect::<Vec<_>>(),
        })
        .to_string()
    }

    /// Explicit performance torture for request shape and serialization. The
    /// server is deliberately a wire-level fake: backend semantics are covered
    /// by the local/object tests while this test proves the gateway client keeps
    /// 1k and 10k operations in constant request batches.
    #[tokio::test]
    #[ignore = "explicit 1k/10k Git small-file performance suite"]
    async fn gateway_git_small_file_perf_1k_10k() {
        let mut scale_samples = Vec::new();
        for count in [1_000_usize, 10_000] {
            let initial = git_perf_writes(count, 0);
            let paths = initial
                .iter()
                .map(|write| write.path.clone())
                .collect::<Vec<_>>();
            let mutation_count = (count / 100).max(1);
            let targeted = git_perf_writes(count, 1)
                .into_iter()
                .filter(|write| {
                    write.path.starts_with(".git/refs/") || write.path.starts_with("src/generated/")
                })
                .collect::<Vec<_>>();
            let responses = vec![
                r#"{"resource_key":"create","owner_token":"owner"}"#.to_string(),
                gateway_write_many_response(&initial),
                String::new(),
                gateway_metadata_many_response(&initial),
                r#"{"resource_key":"rewrite","owner_token":"owner"}"#.to_string(),
                gateway_write_many_response(&targeted),
                String::new(),
                r#"{"resource_key":"namespace","owner_token":"owner"}"#.to_string(),
                String::new(),
                String::new(),
            ];
            let (endpoint, requests) = serve_sequence(responses);
            let storage = GatewayVfsStorage::new(
                GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"),
            );

            let create_started = Instant::now();
            storage
                .write_many_atomic(initial)
                .await
                .expect("gateway Git-shaped create batch");
            let create_elapsed = create_started.elapsed();

            let status_started = Instant::now();
            let status = storage
                .metadata_many(&paths, VfsStorageMetadataFields::default())
                .await
                .expect("gateway status-like metadata batch");
            let status_elapsed = status_started.elapsed();
            assert_eq!(status.len(), count);

            let rewrite_started = Instant::now();
            storage
                .write_many_atomic(targeted)
                .await
                .expect("gateway targeted rewrite batch");
            let rewrite_elapsed = rewrite_started.elapsed();

            let mutations = (0..mutation_count)
                .map(|index| VfsStorageNamespaceMutation::Rename {
                    from: format!(".git/refs/heads/perf-{index:05}.lock"),
                    to: format!(".git/refs/heads/perf-{index:05}"),
                })
                .chain(
                    (0..mutation_count).map(|index| VfsStorageNamespaceMutation::DeleteFile {
                        path: format!("src/generated/perf-{index:05}.ts"),
                        precondition: None,
                    }),
                )
                .collect::<Vec<_>>();
            let namespace_started = Instant::now();
            storage
                .apply_namespace_batch(mutations)
                .await
                .expect("gateway namespace batch");
            let namespace_elapsed = namespace_started.elapsed();

            let captured = (0..10)
                .map(|_| requests.recv().expect("captured gateway request"))
                .collect::<Vec<_>>();
            assert_eq!(
                captured
                    .iter()
                    .map(|request| request.target.as_str())
                    .collect::<Vec<_>>(),
                [
                    "/lease",
                    "/write-many",
                    "/lease",
                    "/metadata-many",
                    "/lease",
                    "/write-many",
                    "/lease",
                    "/lease",
                    "/namespace-many",
                    "/lease",
                ],
                "gateway request count must remain constant as file count grows",
            );
            let create_body: serde_json::Value =
                serde_json::from_str(&captured[1].body).expect("create request JSON");
            let status_body: serde_json::Value =
                serde_json::from_str(&captured[3].body).expect("status request JSON");
            let rewrite_body: serde_json::Value =
                serde_json::from_str(&captured[5].body).expect("rewrite request JSON");
            let namespace_body: serde_json::Value =
                serde_json::from_str(&captured[8].body).expect("namespace request JSON");
            assert_eq!(create_body["writes"].as_array().unwrap().len(), count);
            assert_eq!(status_body["paths"].as_array().unwrap().len(), count);
            assert_eq!(
                rewrite_body["writes"].as_array().unwrap().len(),
                mutation_count * 2,
            );
            assert_eq!(
                namespace_body["mutations"].as_array().unwrap().len(),
                mutation_count * 2,
            );
            assert!(
                requests.try_recv().is_err(),
                "gateway workload emitted unexpected per-file calls",
            );

            let total = create_elapsed + status_elapsed + rewrite_elapsed + namespace_elapsed;
            scale_samples.push((count, total));
            eprintln!(
                "git-small-file-perf backend=gateway files={count} create={create_elapsed:?} \
                 status={status_elapsed:?} rewrite_2pct={rewrite_elapsed:?} \
                 namespace_2pct={namespace_elapsed:?} total={total:?} requests={} \
                 create_body_bytes={}",
                captured.len(),
                captured[1].body.len(),
            );
        }
        let one_k = scale_samples[0].1.as_secs_f64();
        let ten_k = scale_samples[1].1.as_secs_f64();
        assert!(
            ten_k <= one_k * 35.0,
            "10x gateway serialization regressed toward quadratic scaling: 1k={one_k:.3}s 10k={ten_k:.3}s",
        );
    }

    #[tokio::test]
    async fn gateway_streams_staged_file_with_integrity_headers() {
        let staged = tempfile::NamedTempFile::new().expect("staged file");
        std::fs::write(staged.path(), b"stream me").expect("write staged file");
        let expected = hex_hash(b"stream me");
        let (endpoint, requests) = serve_sequence(vec![
            r#"{"kind":"file","size_bytes":3,"content_hash":"old-hash","updated_at":null}"#
                .to_string(),
            r#"{"resource_key":"rk","owner_token":"ot"}"#.to_string(),
            String::new(),
            String::new(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        storage
            .write_from_local_file(
                "large.bin",
                staged.path(),
                Some(&expected),
                Some(VfsStorageWritePrecondition {
                    fingerprint: None,
                    secondary_fingerprint: None,
                }),
                Some(VfsStorageWriteOptions { executable: false }),
            )
            .await
            .expect("stream upload");

        let stat_request = requests.recv().expect("stat request");
        assert_eq!(stat_request.method, "GET");
        assert!(stat_request.target.starts_with("/stat?"));
        let lease_request = requests.recv().expect("lease request");
        assert_eq!(lease_request.target, "/lease");
        let upload_request = requests.recv().expect("upload request");
        assert_eq!(upload_request.method, "PUT");
        assert_query_value(&upload_request.target, "path", "scope/large.bin");
        assert!(
            upload_request
                .headers
                .contains("x-chevalier-vfs-stream-upload: 1")
        );
        assert!(upload_request.headers.contains(&format!(
            "x-chevalier-vfs-expected-content-sha256: {expected}"
        )));
        assert!(
            upload_request
                .headers
                .contains("x-chevalier-vfs-executable: false")
        );
        assert_eq!(upload_request.body, "stream me");
        let release_request = requests.recv().expect("release request");
        assert_eq!(release_request.target, "/lease");
    }

    #[tokio::test]
    async fn gateway_subtree_metadata_strips_scope_and_preserves_object_state() {
        let (endpoint, requests) = serve_one(
            r#"{"entries":[{"path":"scope/jobs/a.txt","kind":"file","size_bytes":3,"content_hash":"hash-a","token_count":7,"version":"v1","updated_at":null,"object_state":{"size_bytes":3,"pack_key":"packs/1","pack_slot_offset":5,"pack_slot_length":7,"pack_slot_compression":1}}]}"#,
        );
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));
        let entries = storage
            .list_subtree_file_metadata(
                "jobs",
                VfsStorageSubtreeOptions {
                    include_object_state: true,
                    include_token_count: true,
                    limit: Some(5),
                    max_hash_bytes: Some(4096),
                },
            )
            .await
            .expect("subtree metadata");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "jobs/a.txt");
        assert_eq!(entries[0].token_count, Some(7));
        assert_eq!(entries[0].version.as_deref(), Some("v1"));
        let object_state = entries[0].object_state.as_ref().expect("object state");
        assert_eq!(object_state.pack_key, "packs/1");
        assert_eq!(object_state.pack_slot_offset, 5);
        assert_eq!(object_state.pack_slot_length, 7);
        assert_eq!(object_state.pack_slot_compression, 1);

        let request = requests.recv().expect("captured request");
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, "/subtree-metadata");
        assert!(request.body.contains(r#""prefix":"scope/jobs""#));
        assert!(request.body.contains(r#""include_object_state":true"#));
        assert!(request.body.contains(r#""include_token_count":true"#));
        assert!(request.body.contains(r#""limit":5"#));
        assert!(request.body.contains(r#""max_hash_bytes":4096"#));
    }

    #[tokio::test]
    async fn gateway_prefetch_subtree_maps_warmed_file_bytes() {
        let (endpoint, requests) =
            serve_one(r#"{"warmed_file_bytes":[{"path":"scope/jobs/a.txt","body":[1,2,3]}]}"#);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));
        let result = storage
            .prefetch_subtree(
                "jobs",
                VfsStoragePrefetchOptions {
                    include_small_file_bytes: true,
                    max_entries: Some(3),
                    max_pack_bytes: Some(4096),
                },
            )
            .await
            .expect("prefetch subtree");

        assert_eq!(result.warmed_file_bytes.len(), 1);
        assert_eq!(result.warmed_file_bytes[0].0, "jobs/a.txt");
        assert_eq!(result.warmed_file_bytes[0].1.as_ref(), &[1, 2, 3]);

        let request = requests.recv().expect("captured request");
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, "/prefetch-subtree");
        assert!(request.body.contains(r#""prefix":"scope/jobs""#));
        assert!(request.body.contains(r#""include_small_file_bytes":true"#));
        assert!(request.body.contains(r#""max_entries":3"#));
        assert!(request.body.contains(r#""max_pack_bytes":4096"#));
    }

    #[tokio::test]
    async fn gateway_changed_only_multi_write_skips_unchanged_files() {
        let unchanged_body = Bytes::from_static(b"same");
        let changed_body = Bytes::from_static(b"new");
        let unchanged_hash = hex_hash(&unchanged_body);
        let changed_hash = hex_hash(&changed_body);
        let (endpoint, requests) = serve_sequence(vec![
            format!(
                r#"{{"entries":[{{"kind":"file","size_bytes":4,"content_hash":"{unchanged_hash}","updated_at":null}},null]}}"#
            ),
            r#"{"resource_key":"rk","owner_token":"ot"}"#.to_string(),
            format!(
                r#"{{"results":[{{"path":"scope/b.txt","content_hash":"{changed_hash}","previous_hash":null,"changed":true}}]}}"#
            ),
            String::new(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        let results = storage
            .write_many_if_changed_atomic(vec![
                VfsStorageWrite {
                    path: "a.txt".to_string(),
                    bytes: unchanged_body,
                    token_count: None,
                    precondition: None,
                },
                VfsStorageWrite {
                    path: "b.txt".to_string(),
                    bytes: changed_body,
                    token_count: None,
                    precondition: Some(VfsStorageWritePrecondition {
                        fingerprint: Some("version-b".to_string()),
                        secondary_fingerprint: Some("secondary-b".to_string()),
                    }),
                },
            ])
            .await
            .expect("changed-only write");

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].path, "a.txt");
        assert!(!results[0].changed);
        assert_eq!(results[0].content_hash, unchanged_hash);
        assert_eq!(results[1].path, "b.txt");
        assert!(results[1].changed);
        assert_eq!(results[1].content_hash, changed_hash);

        let metadata_request = requests.recv().expect("metadata request");
        assert_eq!(metadata_request.method, "POST");
        assert_eq!(metadata_request.target, "/metadata-many");
        assert!(metadata_request.body.contains(r#""scope/a.txt""#));
        assert!(metadata_request.body.contains(r#""scope/b.txt""#));

        let lease_request = requests.recv().expect("lease request");
        assert_eq!(lease_request.method, "POST");
        assert_eq!(lease_request.target, "/lease");
        assert!(lease_request.body.contains(r#""path":"scope/b.txt""#));

        let write_request = requests.recv().expect("write-many request");
        assert_eq!(write_request.method, "POST");
        assert_eq!(write_request.target, "/write-many");
        assert!(!write_request.body.contains("a.txt"));
        assert!(write_request.body.contains(r#""path":"scope/b.txt""#));
        assert!(write_request.body.contains(r#""fingerprint":"version-b""#));
        assert!(
            write_request
                .body
                .contains(r#""secondary_fingerprint":"secondary-b""#)
        );

        let release_request = requests.recv().expect("release request");
        assert_eq!(release_request.method, "DELETE");
        assert_eq!(release_request.target, "/lease");
    }

    #[tokio::test]
    async fn gateway_delete_forwards_precondition_headers() {
        let (endpoint, requests) = serve_sequence(vec![
            r#"{"resource_key":"rk","owner_token":"ot"}"#.to_string(),
            r#"{"previous":{"kind":"file","size_bytes":3,"content_hash":"old-hash","updated_at":null}}"#
                .to_string(),
            String::new(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        let result = storage
            .delete_file_with_metadata(
                "a.txt",
                Some(VfsStorageWritePrecondition {
                    fingerprint: Some("version-a".to_string()),
                    secondary_fingerprint: Some("secondary-a".to_string()),
                }),
            )
            .await
            .expect("delete with precondition");

        assert_eq!(
            result
                .previous
                .as_ref()
                .and_then(|metadata| metadata.content_hash.as_deref()),
            Some("old-hash")
        );

        let lease_request = requests.recv().expect("lease request");
        assert_eq!(lease_request.method, "POST");
        assert_eq!(lease_request.target, "/lease");

        let delete_request = requests.recv().expect("delete request");
        assert_eq!(delete_request.method, "DELETE");
        assert!(delete_request.target.starts_with("/file?"));
        assert_query_value(&delete_request.target, "path", "scope/a.txt");
        assert_query_value(&delete_request.target, "return_metadata", "true");
        assert!(
            delete_request
                .headers
                .contains("x-chevalier-vfs-precondition-fingerprint: version-a")
        );
        assert!(
            delete_request
                .headers
                .contains("x-chevalier-vfs-precondition-secondary-fingerprint: secondary-a")
        );

        let release_request = requests.recv().expect("release request");
        assert_eq!(release_request.method, "DELETE");
        assert_eq!(release_request.target, "/lease");
    }

    #[tokio::test]
    async fn gateway_namespace_batch_serializes_delete_preconditions() {
        let (endpoint, requests) = serve_sequence(vec![
            r#"{"resource_key":"rk","owner_token":"ot"}"#.to_string(),
            String::new(),
            String::new(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        storage
            .apply_namespace_batch(vec![VfsStorageNamespaceMutation::DeleteFile {
                path: "a.txt".to_string(),
                precondition: Some(VfsStorageWritePrecondition {
                    fingerprint: Some("version-a".to_string()),
                    secondary_fingerprint: Some("secondary-a".to_string()),
                }),
            }])
            .await
            .expect("namespace batch");

        let lease_request = requests.recv().expect("lease request");
        assert_eq!(lease_request.method, "POST");
        assert_eq!(lease_request.target, "/lease");

        let batch_request = requests.recv().expect("namespace request");
        assert_eq!(batch_request.method, "POST");
        assert_eq!(batch_request.target, "/namespace-many");
        assert!(batch_request.body.contains(r#""path":"scope/a.txt""#));
        assert!(batch_request.body.contains(r#""fingerprint":"version-a""#));
        assert!(
            batch_request
                .body
                .contains(r#""secondary_fingerprint":"secondary-a""#)
        );

        let release_request = requests.recv().expect("release request");
        assert_eq!(release_request.method, "DELETE");
        assert_eq!(release_request.target, "/lease");
    }

    #[tokio::test]
    async fn gateway_create_symlink_uses_leased_symlink_route() {
        let (endpoint, requests) = serve_sequence(vec![
            r#"{"resource_key":"rk","owner_token":"ot"}"#.to_string(),
            String::new(),
            String::new(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        storage
            .create_symlink("link.txt", "target.txt")
            .await
            .expect("create symlink");

        let lease_request = requests.recv().expect("lease request");
        assert_eq!(lease_request.method, "POST");
        assert_eq!(lease_request.target, "/lease");
        assert!(lease_request.body.contains(r#""path":"scope/link.txt""#));

        let symlink_request = requests.recv().expect("symlink request");
        assert_eq!(symlink_request.method, "PUT");
        assert!(symlink_request.target.starts_with("/symlink?"));
        assert_query_value(&symlink_request.target, "path", "scope/link.txt");
        assert_query_value(&symlink_request.target, "target", "target.txt");
        assert!(
            symlink_request
                .headers
                .contains("x-chevalier-vfs-operation: vfs_symlink")
        );

        let release_request = requests.recv().expect("release request");
        assert_eq!(release_request.method, "DELETE");
        assert_eq!(release_request.target, "/lease");
    }

    #[tokio::test]
    async fn gateway_rename_uses_server_metadata_response() {
        let (endpoint, requests) = serve_sequence(vec![
            r#"{"resource_key":"rk","owner_token":"ot"}"#.to_string(),
            r#"{"previous":{"kind":"file","size_bytes":3,"content_hash":"old-hash","updated_at":null},"current":{"kind":"file","size_bytes":3,"content_hash":"new-hash","updated_at":null}}"#
                .to_string(),
            String::new(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        let result = storage
            .rename_with_metadata("old.txt", "new.txt")
            .await
            .expect("rename with metadata");

        assert_eq!(
            result
                .previous
                .as_ref()
                .and_then(|metadata| metadata.content_hash.as_deref()),
            Some("old-hash")
        );
        assert_eq!(
            result
                .current
                .as_ref()
                .and_then(|metadata| metadata.content_hash.as_deref()),
            Some("new-hash")
        );

        let lease_request = requests.recv().expect("lease request");
        assert_eq!(lease_request.method, "POST");
        assert_eq!(lease_request.target, "/lease");

        let rename_request = requests.recv().expect("rename request");
        assert_eq!(rename_request.method, "POST");
        assert!(rename_request.target.starts_with("/rename?"));
        assert_query_value(&rename_request.target, "from", "scope/old.txt");
        assert_query_value(&rename_request.target, "to", "scope/new.txt");
        assert_query_value(&rename_request.target, "return_metadata", "true");

        let release_request = requests.recv().expect("release request");
        assert_eq!(release_request.method, "DELETE");
        assert_eq!(release_request.target, "/lease");
    }

    #[derive(Debug)]
    struct RequestRecord {
        method: String,
        target: String,
        headers: String,
        body: String,
    }

    #[tokio::test]
    async fn gateway_hard_link_uses_versioned_route_and_preserves_identity() {
        let (endpoint, requests) = serve_sequence(vec![
            r#"{"resource_key":"rk","owner_token":"00000000-0000-0000-0000-000000000001"}"#
                .to_string(),
            r#"{"source":{"kind":"file","size_bytes":4,"file_id":"inode-1","link_count":2,"content_hash":"hash","updated_at":null},"destination":{"kind":"file","size_bytes":4,"file_id":"inode-1","link_count":2,"content_hash":"hash","updated_at":null}}"#
                .to_string(),
            String::new(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        let result = storage
            .create_hard_link("source", "nested/alias")
            .await
            .expect("hard link");
        assert_eq!(result.source.file_id.as_deref(), Some("inode-1"));
        assert_eq!(result.destination.file_id.as_deref(), Some("inode-1"));
        assert_eq!(result.source.link_count, 2);
        assert_eq!(result.destination.link_count, 2);

        let lease = requests.recv().expect("lease");
        assert_eq!(lease.target, "/lease");
        let link = requests.recv().expect("link");
        assert_eq!(link.method, "POST");
        assert_eq!(link.target, "/hard-link/v1");
        assert!(link.body.contains(r#""source_path":"scope/source""#));
        assert!(
            link.body
                .contains(r#""destination_path":"scope/nested/alias""#)
        );
        assert!(
            link.headers
                .contains("x-chevalier-vfs-operation: vfs_hard_link")
        );
        let release = requests.recv().expect("release");
        assert_eq!(release.method, "DELETE");
        assert_eq!(release.target, "/lease");
    }

    #[tokio::test]
    async fn gateway_hard_link_alias_resolution_scopes_request_and_response() {
        let (endpoint, requests) = serve_one(r#"{"path":"scope/nested/remaining"}"#);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        let alias = storage
            .find_hard_link_alias("inode-1", "removed")
            .await
            .expect("resolve alias");
        assert_eq!(alias.as_deref(), Some("nested/remaining"));
        let request = requests.recv().expect("request");
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, "/hard-link-alias/v1");
        assert!(request.body.contains(r#""file_id":"inode-1""#));
        assert!(request.body.contains(r#""excluding_path":"scope/removed""#));
    }

    #[tokio::test]
    async fn gateway_metadata_surfaces_preserve_defaulted_file_identity() {
        let (endpoint, _requests) = serve_sequence(vec![
            r#"{"kind":"file","size_bytes":1,"file_id":"inode-1","link_count":3,"content_hash":"h","updated_at":null}"#.to_string(),
            r#"[{"name":"a","kind":"file","size_bytes":1,"file_id":"inode-1","link_count":3,"content_hash":"h","updated_at":null}]"#.to_string(),
            r#"{"entries":[{"path":"scope/a","kind":"file","size_bytes":1,"file_id":"inode-1","link_count":3,"content_hash":"h","token_count":null,"version":null,"updated_at":null,"object_state":null}]}"#.to_string(),
        ]);
        let storage =
            GatewayVfsStorage::new(GatewayVfsStorageConfig::new(endpoint).with_scope_path("scope"));

        let stat = storage.stat("a").await.unwrap().unwrap();
        let listed = storage
            .list_dir_with_metadata("", VfsStorageDirListFilter::default())
            .await
            .unwrap();
        let subtree = storage
            .list_subtree_file_metadata("", VfsStorageSubtreeOptions::default())
            .await
            .unwrap();
        for metadata in [stat, listed[0].clone(), subtree[0].clone()] {
            assert_eq!(metadata.file_id.as_deref(), Some("inode-1"));
            assert_eq!(metadata.link_count, 3);
        }
    }

    fn serve_one(response_body: &'static str) -> (String, mpsc::Receiver<RequestRecord>) {
        serve_sequence(vec![response_body.to_string()])
    }

    fn serve_sequence(response_bodies: Vec<String>) -> (String, mpsc::Receiver<RequestRecord>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            for response_body in response_bodies {
                let (mut stream, _) = listener.accept().expect("accept request");
                let request = read_request(&mut stream);
                tx.send(request).expect("send request");
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write response");
            }
        });
        (endpoint, rx)
    }

    fn read_request(stream: &mut std::net::TcpStream) -> RequestRecord {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).expect("read request");
            assert!(read > 0, "connection closed before headers");
            bytes.extend_from_slice(&buffer[..read]);
            if request_is_complete(&bytes) {
                break;
            }
        }
        let request = String::from_utf8(bytes).expect("request utf8");
        let (head, body) = request.split_once("\r\n\r\n").expect("request head");
        let first = head.lines().next().expect("request line");
        let mut parts = first.split_whitespace();
        RequestRecord {
            method: parts.next().expect("method").to_string(),
            target: parts.next().expect("target").to_string(),
            headers: head.to_ascii_lowercase(),
            body: body.to_string(),
        }
    }

    fn request_is_complete(bytes: &[u8]) -> bool {
        let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let head = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find_map(|(name, value)| {
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        bytes.len() >= header_end + 4 + content_length
    }

    fn assert_query_value(target: &str, key: &str, value: &str) {
        let encoded_key = percent_encode(key);
        let encoded_value = percent_encode(value);
        let raw_pair = format!("{key}={value}");
        let encoded_pair = format!("{encoded_key}={encoded_value}");
        assert!(
            target.contains(&raw_pair) || target.contains(&encoded_pair),
            "missing query pair {key}={value} in {target}"
        );
    }

    fn percent_encode(value: &str) -> String {
        value
            .bytes()
            .map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    (byte as char).to_string()
                }
                byte => format!("%{byte:02X}"),
            })
            .collect()
    }
}
