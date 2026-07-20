use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chevalier_sandbox::vfs::{
    CHEVALIER_VFS_COMPONENT_HEADER, CHEVALIER_VFS_EXECUTABLE_HEADER,
    CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER, CHEVALIER_VFS_MODE_HEADER,
    CHEVALIER_VFS_OPERATION_HEADER, CHEVALIER_VFS_PRECONDITION_FILE_ID_HEADER,
    CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER, CHEVALIER_VFS_PRECONDITION_KIND_HEADER,
    CHEVALIER_VFS_RESOURCE_KEY_HEADER, CHEVALIER_VFS_SURFACE_KIND_HEADER, VFS_COMPONENT_VM_RUNTIME,
    VfsCasPredicate, VfsDirEntry as RemoteDirEntry, VfsHardLinkAliasBody, VfsHardLinkAliasResponse,
    VfsHardLinkBody, VfsHardLinkMetadataResponse, VfsLeaseAcquireRequest,
    VfsLeaseGrant as LeaseGrant, VfsLeaseReleaseRequest, VfsMetadata as RemoteMetadata,
    VfsNamespaceMutation, VfsNamespaceMutationBatchBody, VfsWriteManyBody, VfsWriteManyItem,
    VfsWritePrecondition, scoped_vfs_path,
};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};

pub const RANGE_FINGERPRINT_HEADER: &str = "x-chevalier-vfs-range-fingerprint";
/// Transient-failure budget sized to ride out a gateway restart, not mask a
/// broken one: only transport failures and 5xx/429/408 consume it — hard 4xx
/// rejections fail on the first attempt.
const METADATA_READ_RETRY_TIMEOUT: Duration = Duration::from_secs(8);
// File bodies have a longer per-attempt timeout than metadata. Their total
// budget must exceed one attempt or a congested first request can never retry.
const FILE_READ_RETRY_TIMEOUT: Duration = Duration::from_secs(45);
const METADATA_READ_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const FILE_READ_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(15);
const ADVISORY_LOCK_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const ADVISORY_LOCK_RENEWAL_BATCH_SIZE: usize = 4_096;
const READ_RETRY_DELAY_MIN: Duration = Duration::from_millis(50);
const READ_RETRY_DELAY_MAX: Duration = Duration::from_millis(500);

#[derive(Clone, Debug)]
pub struct RemoteVfsClient {
    client: Client,
    endpoint: String,
    auth_token: String,
    scope_path: String,
}

pub struct RemoteWrite {
    pub path: String,
    pub bytes: Vec<u8>,
    pub base_content_hash: Option<String>,
    pub expected_file_id: Option<String>,
}

/// Outcome of a fingerprint-pinned ranged read.
pub enum RangeRead {
    Bytes(Vec<u8>),
    NotFound,
    /// The file changed since the fingerprint was taken; re-stat and retry.
    Stale,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AdvisoryLockConflict {
    pub start: String,
    pub end: String,
    pub kind: String,
    pub pid: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AdvisoryLockResponse {
    pub acquired: bool,
    pub conflict: Option<AdvisoryLockConflict>,
    pub file_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AdvisoryLockRenewalIdentity {
    pub lock_owner: String,
    pub namespace: String,
    pub file_id: String,
}

#[derive(Serialize)]
struct AdvisoryLockRequest<'a> {
    action: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_id: Option<&'a str>,
    mount_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    lock_owner: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identities: Option<&'a [AdvisoryLockRenewalIdentity]>,
}

impl RemoteVfsClient {
    pub fn new(endpoint: &str, auth_token: &str, scope_path: &str) -> Result<Self> {
        let mut builder = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .http2_adaptive_window(true);
        if http2_prior_knowledge_enabled(endpoint) {
            builder = builder.http2_prior_knowledge();
        }
        let client = builder.build().context("build vfs reqwest client")?;
        Ok(Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            auth_token: auth_token.to_string(),
            scope_path: scope_path.trim_matches('/').to_string(),
        })
    }

    pub async fn list_dir(&self, path: &str) -> Result<Option<Vec<RemoteDirEntry>>> {
        self.read_decoded(
            self.client
                .get(self.url("/tree"))
                .query(&[
                    ("path", self.path_arg(path)),
                    ("max_hash_bytes", "0".to_string()),
                ])
                .timeout(METADATA_READ_ATTEMPT_TIMEOUT),
            METADATA_READ_RETRY_TIMEOUT,
            |status, body| {
                if status == StatusCode::NOT_FOUND {
                    return Ok(None);
                }
                serde_json::from_slice(body)
                    .context("decode vfs tree response")
                    .map(Some)
            },
        )
        .await
    }

    pub async fn stat(&self, path: &str) -> Result<Option<RemoteMetadata>> {
        self.stat_with_max_hash_bytes(path, None).await
    }

    pub async fn stat_attributes(&self, path: &str) -> Result<Option<RemoteMetadata>> {
        self.stat_with_max_hash_bytes(path, Some(0)).await
    }

    async fn stat_with_max_hash_bytes(
        &self,
        path: &str,
        max_hash_bytes: Option<u64>,
    ) -> Result<Option<RemoteMetadata>> {
        let mut query = vec![("path", self.path_arg(path))];
        if let Some(max_hash_bytes) = max_hash_bytes {
            query.push(("max_hash_bytes", max_hash_bytes.to_string()));
        }
        self.read_decoded(
            self.client
                .get(self.url("/stat"))
                .query(&query)
                .timeout(METADATA_READ_ATTEMPT_TIMEOUT),
            METADATA_READ_RETRY_TIMEOUT,
            |status, body| {
                if status == StatusCode::NOT_FOUND {
                    return Ok(None);
                }
                serde_json::from_slice(body)
                    .context("decode vfs stat response")
                    .map(Some)
            },
        )
        .await
    }

    pub async fn read_file_raw(&self, path: &str) -> Result<Option<Vec<u8>>> {
        self.read_decoded(
            self.client
                .get(self.url("/file/raw"))
                .query(&[("path", self.path_arg(path))])
                .timeout(FILE_READ_ATTEMPT_TIMEOUT),
            FILE_READ_RETRY_TIMEOUT,
            |status, body| {
                if status == StatusCode::NOT_FOUND {
                    return Ok(None);
                }
                if !status.is_success() {
                    return Err(anyhow!("vfs raw read failed: {status}"));
                }
                Ok(Some(body.to_vec()))
            },
        )
        .await
    }

    pub async fn read_file_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        fingerprint: Option<&str>,
    ) -> Result<RangeRead> {
        let mut request = self
            .client
            .get(self.url("/file/raw"))
            .query(&[("path", self.path_arg(path))])
            .header(
                header::RANGE,
                format!("bytes={offset}-{}", offset + length.saturating_sub(1)),
            )
            .timeout(FILE_READ_ATTEMPT_TIMEOUT);
        if let Some(fingerprint) = fingerprint {
            request = request.header(RANGE_FINGERPRINT_HEADER, fingerprint);
        }
        self.read_decoded(request, FILE_READ_RETRY_TIMEOUT, |status, body| {
            if status == StatusCode::NOT_FOUND {
                return Ok(RangeRead::NotFound);
            }
            if status == StatusCode::PRECONDITION_FAILED {
                return Ok(RangeRead::Stale);
            }
            Ok(RangeRead::Bytes(body.to_vec()))
        })
        .await
    }

    pub async fn write_file(
        &self,
        path: &str,
        bytes: &[u8],
        executable: bool,
        mode: Option<u32>,
        lease: &LeaseGrant,
        surface_kind: &str,
        operation: &str,
        base_content_hash: Option<&str>,
        expected_file_id: Option<&str>,
    ) -> Result<()> {
        let mut request = self
            .client
            .put(self.url("/file"))
            .query(&[("path", self.path_arg(path))])
            .header(CHEVALIER_VFS_COMPONENT_HEADER, VFS_COMPONENT_VM_RUNTIME)
            .header(CHEVALIER_VFS_SURFACE_KIND_HEADER, surface_kind)
            .header(CHEVALIER_VFS_OPERATION_HEADER, operation)
            .header(CHEVALIER_VFS_EXECUTABLE_HEADER, executable.to_string())
            .header(
                CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                lease.resource_key.as_str(),
            )
            .header(
                CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                lease.owner_token.to_string(),
            );
        request = with_mode_header(request, mode);
        request = with_precondition_headers(request, base_content_hash, expected_file_id);
        self.request(request.body(bytes.to_vec())).await?;
        Ok(())
    }

    pub async fn delete_file(
        &self,
        path: &str,
        lease: &LeaseGrant,
        surface_kind: &str,
        operation: &str,
    ) -> Result<()> {
        self.request(
            self.client
                .delete(self.url("/file"))
                .query(&[("path", self.path_arg(path))])
                .header(CHEVALIER_VFS_COMPONENT_HEADER, VFS_COMPONENT_VM_RUNTIME)
                .header(CHEVALIER_VFS_SURFACE_KIND_HEADER, surface_kind)
                .header(CHEVALIER_VFS_OPERATION_HEADER, operation)
                .header(
                    CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                    lease.resource_key.as_str(),
                )
                .header(
                    CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                    lease.owner_token.to_string(),
                ),
        )
        .await?;
        Ok(())
    }

    pub async fn mkdir(
        &self,
        path: &str,
        mode: Option<u32>,
        lease: &LeaseGrant,
        surface_kind: &str,
        operation: &str,
    ) -> Result<()> {
        let request = self
            .client
            .put(self.url("/dir"))
            .query(&[("path", self.path_arg(path))])
            .header(CHEVALIER_VFS_COMPONENT_HEADER, VFS_COMPONENT_VM_RUNTIME)
            .header(CHEVALIER_VFS_SURFACE_KIND_HEADER, surface_kind)
            .header(CHEVALIER_VFS_OPERATION_HEADER, operation)
            .header(
                CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                lease.resource_key.as_str(),
            )
            .header(
                CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                lease.owner_token.to_string(),
            );
        self.request(with_mode_header(request, mode)).await?;
        Ok(())
    }

    pub async fn create_symlink(
        &self,
        path: &str,
        target: &str,
        lease: &LeaseGrant,
        surface_kind: &str,
        operation: &str,
    ) -> Result<()> {
        self.request(
            self.client
                .put(self.url("/symlink"))
                .query(&[
                    ("path", self.path_arg(path)),
                    ("target", target.to_string()),
                ])
                .header(CHEVALIER_VFS_COMPONENT_HEADER, VFS_COMPONENT_VM_RUNTIME)
                .header(CHEVALIER_VFS_SURFACE_KIND_HEADER, surface_kind)
                .header(CHEVALIER_VFS_OPERATION_HEADER, operation)
                .header(
                    CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                    lease.resource_key.as_str(),
                )
                .header(
                    CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                    lease.owner_token.to_string(),
                ),
        )
        .await?;
        Ok(())
    }

    pub async fn create_hard_link(
        &self,
        source_path: &str,
        destination_path: &str,
        lease: &LeaseGrant,
        surface_kind: &str,
    ) -> Result<VfsHardLinkMetadataResponse> {
        self.request(
            self.client
                .post(self.url("/hard-link/v1"))
                .header(CHEVALIER_VFS_COMPONENT_HEADER, VFS_COMPONENT_VM_RUNTIME)
                .header(CHEVALIER_VFS_SURFACE_KIND_HEADER, surface_kind)
                .header(CHEVALIER_VFS_OPERATION_HEADER, "vfs_hard_link")
                .header(
                    CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                    lease.resource_key.as_str(),
                )
                .header(
                    CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                    lease.owner_token.to_string(),
                )
                .json(&VfsHardLinkBody {
                    source_path: self.path_arg(source_path),
                    destination_path: self.path_arg(destination_path),
                }),
        )
        .await?
        .json()
        .await
        .context("decode vfs hard-link response")
    }

    pub async fn find_hard_link_alias(
        &self,
        file_id: &str,
        excluding_path: &str,
    ) -> Result<Option<String>> {
        self.request(self.client.post(self.url("/hard-link-alias/v1")).json(
            &VfsHardLinkAliasBody {
                file_id: file_id.to_string(),
                excluding_path: self.path_arg(excluding_path),
            },
        ))
        .await?
        .json::<VfsHardLinkAliasResponse>()
        .await
        .map(|response| response.path.map(|path| self.unscoped_path(path.as_str())))
        .context("decode vfs hard-link alias response")
    }

    pub async fn rmdir(
        &self,
        path: &str,
        lease: &LeaseGrant,
        surface_kind: &str,
        operation: &str,
    ) -> Result<()> {
        self.request(
            self.client
                .delete(self.url("/dir"))
                .query(&[("path", self.path_arg(path))])
                .header(CHEVALIER_VFS_COMPONENT_HEADER, VFS_COMPONENT_VM_RUNTIME)
                .header(CHEVALIER_VFS_SURFACE_KIND_HEADER, surface_kind)
                .header(CHEVALIER_VFS_OPERATION_HEADER, operation)
                .header(
                    CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                    lease.resource_key.as_str(),
                )
                .header(
                    CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                    lease.owner_token.to_string(),
                ),
        )
        .await?;
        Ok(())
    }

    pub async fn rename(
        &self,
        from: &str,
        to: &str,
        lease: &LeaseGrant,
        surface_kind: &str,
        operation: &str,
    ) -> Result<()> {
        self.request(
            self.client
                .post(self.url("/rename"))
                .query(&[("from", self.path_arg(from)), ("to", self.path_arg(to))])
                .header(CHEVALIER_VFS_COMPONENT_HEADER, VFS_COMPONENT_VM_RUNTIME)
                .header(CHEVALIER_VFS_SURFACE_KIND_HEADER, surface_kind)
                .header(CHEVALIER_VFS_OPERATION_HEADER, operation)
                .header(
                    CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                    lease.resource_key.as_str(),
                )
                .header(
                    CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                    lease.owner_token.to_string(),
                ),
        )
        .await?;
        Ok(())
    }

    pub async fn acquire_lease(
        &self,
        path: &str,
        mutation_count: i32,
        reason: &str,
    ) -> Result<LeaseGrant> {
        let scoped_path = self.path_arg(path);
        self.request(
            self.client
                .post(self.url("/lease"))
                .json(&VfsLeaseAcquireRequest {
                    path: scoped_path,
                    mutation_count: Some(mutation_count),
                    component: Some(VFS_COMPONENT_VM_RUNTIME.to_string()),
                    run_id: None,
                    reason: Some(reason.to_string()),
                }),
        )
        .await?
        .json()
        .await
        .context("decode vfs lease response")
    }

    pub async fn release_lease(&self, lease: &LeaseGrant) -> Result<()> {
        self.request(
            self.client
                .delete(self.url("/lease"))
                .json(&VfsLeaseReleaseRequest {
                    resource_key: lease.resource_key.clone(),
                    owner_token: lease.owner_token,
                }),
        )
        .await?;
        Ok(())
    }

    pub async fn advisory_lock(
        &self,
        action: &str,
        path: &str,
        mount_id: &str,
        lock_owner: &str,
        namespace: &str,
        start: u64,
        end: u64,
        kind: &str,
        pid: u32,
    ) -> Result<AdvisoryLockResponse> {
        self.request(
            self.client
                .post(self.url("/posix-lock/v1"))
                .timeout(ADVISORY_LOCK_ATTEMPT_TIMEOUT)
                .json(&AdvisoryLockRequest {
                    action,
                    path: Some(self.path_arg(path)),
                    file_id: None,
                    mount_id,
                    lock_owner: Some(lock_owner),
                    namespace: Some(namespace),
                    start: Some(start.to_string()),
                    end: Some(end.to_string()),
                    kind: Some(kind),
                    pid: Some(pid),
                    identities: None,
                }),
        )
        .await?
        .json()
        .await
        .context("decode posix lock response")
    }

    pub async fn release_advisory_lock_owner(
        &self,
        mount_id: &str,
        lock_owner: &str,
        file_id: &str,
        namespace: &str,
    ) -> Result<()> {
        self.request(
            self.client
                .post(self.url("/posix-lock/v1"))
                .json(&AdvisoryLockRequest {
                    action: "release_owner",
                    path: None,
                    file_id: Some(file_id),
                    mount_id,
                    lock_owner: Some(lock_owner),
                    namespace: Some(namespace),
                    start: None,
                    end: None,
                    kind: None,
                    pid: None,
                    identities: None,
                }),
        )
        .await?;
        Ok(())
    }

    pub async fn renew_advisory_locks(
        &self,
        mount_id: &str,
        identities: &[AdvisoryLockRenewalIdentity],
    ) -> Result<()> {
        for identities in identities.chunks(ADVISORY_LOCK_RENEWAL_BATCH_SIZE) {
            self.request(
                self.client
                    .post(self.url("/posix-lock/v1"))
                    .json(&AdvisoryLockRequest {
                        action: "renew_owners",
                        path: None,
                        file_id: None,
                        mount_id,
                        lock_owner: None,
                        namespace: None,
                        start: None,
                        end: None,
                        kind: None,
                        pid: None,
                        identities: Some(identities),
                    }),
            )
            .await?;
        }
        Ok(())
    }

    pub async fn release_advisory_lock_mount(&self, mount_id: &str) -> Result<()> {
        self.request(
            self.client
                .post(self.url("/posix-lock/v1"))
                .json(&AdvisoryLockRequest {
                    action: "release_mount",
                    path: None,
                    file_id: None,
                    mount_id,
                    lock_owner: None,
                    namespace: None,
                    start: None,
                    end: None,
                    kind: None,
                    pid: None,
                    identities: None,
                }),
        )
        .await?;
        Ok(())
    }

    pub async fn apply_namespace_batch(
        &self,
        mutations: &[VfsNamespaceMutation],
        surface_kind: &str,
    ) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        let lease_path = common_namespace_parent(mutations);
        let lease = self
            .acquire_lease(
                lease_path.as_str(),
                mutations.len() as i32,
                "apply vfs namespace batch",
            )
            .await?;
        let scoped = mutations
            .iter()
            .map(|mutation| self.scope_namespace_mutation(mutation))
            .collect::<Vec<_>>();
        let result = self
            .request(
                self.client
                    .post(self.url("/namespace-many"))
                    .header(CHEVALIER_VFS_COMPONENT_HEADER, VFS_COMPONENT_VM_RUNTIME)
                    .header(CHEVALIER_VFS_SURFACE_KIND_HEADER, surface_kind)
                    .header(CHEVALIER_VFS_OPERATION_HEADER, "vfs_namespace_batch")
                    .header(
                        CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                        lease.resource_key.as_str(),
                    )
                    .header(
                        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                        lease.owner_token.to_string(),
                    )
                    .json(&VfsNamespaceMutationBatchBody { mutations: scoped }),
            )
            .await
            .map(|_| ());
        let release = self.release_lease(&lease).await;
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
        }
    }

    pub async fn write_many(&self, writes: Vec<RemoteWrite>, surface_kind: &str) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let lease_path = common_parent(writes.iter().map(|write| write.path.as_str()));
        let lease = self
            .acquire_lease(
                lease_path.as_str(),
                writes.len() as i32,
                "flush vfs fuse write batch",
            )
            .await?;
        let body = VfsWriteManyBody {
            writes: writes
                .into_iter()
                .map(|write| self.scope_remote_write(write))
                .collect(),
        };
        let result = self
            .request(
                self.client
                    .post(self.url("/write-many"))
                    .header(CHEVALIER_VFS_COMPONENT_HEADER, VFS_COMPONENT_VM_RUNTIME)
                    .header(CHEVALIER_VFS_SURFACE_KIND_HEADER, surface_kind)
                    .header(CHEVALIER_VFS_OPERATION_HEADER, "vfs_write_many")
                    .header(
                        CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                        lease.resource_key.as_str(),
                    )
                    .header(
                        CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                        lease.owner_token.to_string(),
                    )
                    .json(&body),
            )
            .await
            .map(|_| ());
        let release = self.release_lease(&lease).await;
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
        }
    }

    fn scope_namespace_mutation(&self, mutation: &VfsNamespaceMutation) -> VfsNamespaceMutation {
        match mutation {
            VfsNamespaceMutation::CreateDirectory { path, mode } => {
                VfsNamespaceMutation::CreateDirectory {
                    path: self.path_arg(path),
                    mode: mode.map(|mode| mode & 0o7777),
                }
            }
            VfsNamespaceMutation::CreateSymlink { path, target } => {
                VfsNamespaceMutation::CreateSymlink {
                    path: self.path_arg(path),
                    target: target.clone(),
                }
            }
            VfsNamespaceMutation::DeleteFile { path, precondition } => {
                VfsNamespaceMutation::DeleteFile {
                    path: self.path_arg(path),
                    precondition: precondition.clone(),
                }
            }
            VfsNamespaceMutation::RemoveDirectory { path } => {
                VfsNamespaceMutation::RemoveDirectory {
                    path: self.path_arg(path),
                }
            }
            VfsNamespaceMutation::Rename { from, to } => VfsNamespaceMutation::Rename {
                from: self.path_arg(from),
                to: self.path_arg(to),
            },
            VfsNamespaceMutation::SetMode { path, mode } => VfsNamespaceMutation::SetMode {
                path: self.path_arg(path),
                mode: mode & 0o7777,
            },
        }
    }

    fn scope_remote_write(&self, write: RemoteWrite) -> VfsWriteManyItem {
        let precondition = (write.base_content_hash.is_some() || write.expected_file_id.is_some())
            .then_some(VfsWritePrecondition {
                predicate: write.base_content_hash.as_ref().map(|fingerprint| {
                    if fingerprint == "absent" {
                        VfsCasPredicate::Absent
                    } else {
                        VfsCasPredicate::ContentFingerprint {
                            fingerprint: fingerprint.clone(),
                        }
                    }
                }),
                fingerprint: None,
                secondary_fingerprint: None,
                expected_file_id: write.expected_file_id,
            });
        VfsWriteManyItem {
            path: self.path_arg(write.path.as_str()),
            body: write.bytes,
            precondition,
        }
    }

    fn path_arg(&self, relative: &str) -> String {
        scoped_vfs_path(self.scope_path.as_str(), relative)
    }

    fn unscoped_path(&self, path: &str) -> String {
        let path = path.trim_matches('/');
        if self.scope_path.is_empty() {
            return path.to_string();
        }
        if path == self.scope_path {
            return String::new();
        }
        path.strip_prefix(&format!("{}/", self.scope_path))
            .unwrap_or(path)
            .to_string()
    }

    fn url(&self, suffix: &str) -> String {
        format!("{}{}", self.endpoint, suffix)
    }

    async fn request(&self, builder: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let response = builder
            .bearer_auth(&self.auth_token)
            .send()
            .await
            .context("send vfs request")?;
        if response.status().is_success() || response.status() == StatusCode::PARTIAL_CONTENT {
            return Ok(response);
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow::Error::new(VfsRequestStatusError { status })
            .context(format!("vfs request failed: {status} {body}")))
    }

    async fn read_decoded<T>(
        &self,
        builder: reqwest::RequestBuilder,
        retry_timeout: Duration,
        mut decode: impl FnMut(StatusCode, &[u8]) -> Result<T>,
    ) -> Result<T> {
        let request_url = builder
            .try_clone()
            .and_then(|request| request.build().ok())
            .map(|request| request.url().to_string())
            .unwrap_or_else(|| "<unavailable>".to_string());
        let deadline = Instant::now() + retry_timeout;
        let mut retry_delay = READ_RETRY_DELAY_MIN;
        loop {
            let request = builder
                .try_clone()
                .ok_or_else(|| anyhow!("cannot clone vfs read request for retry"))?;
            let outcome = async {
                let response = request
                    .bearer_auth(&self.auth_token)
                    .send()
                    .await
                    .context("send vfs request")
                    .map_err(ReadFailure::transient)?;
                let status = response.status();
                if status.is_success()
                    || status == StatusCode::PARTIAL_CONTENT
                    || status == StatusCode::NOT_FOUND
                    || status == StatusCode::PRECONDITION_FAILED
                {
                    let body = response
                        .bytes()
                        .await
                        .context("read vfs response body")
                        .map_err(ReadFailure::transient)?;
                    return Ok((status, body));
                }
                let body = response.text().await.unwrap_or_default();
                let error = anyhow!("vfs read failed: {status} {body}");
                if status.is_server_error()
                    || matches!(
                        status,
                        StatusCode::TOO_MANY_REQUESTS | StatusCode::REQUEST_TIMEOUT
                    )
                {
                    Err(ReadFailure::transient(error))
                } else {
                    Err(ReadFailure::terminal(error))
                }
            }
            .await;
            match outcome {
                Ok((status, body)) => return decode(status, body.as_ref()),
                Err(failure) if failure.transient && Instant::now() < deadline => {
                    tracing::debug!(error = %failure.error, "retrying transient vfs read failure");
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = retry_delay.saturating_mul(2).min(READ_RETRY_DELAY_MAX);
                }
                Err(failure) => {
                    tracing::warn!(
                        url = request_url,
                        transient = failure.transient,
                        retry_timeout_ms = retry_timeout.as_millis() as u64,
                        error = %failure.error,
                        "vfs read failed"
                    );
                    return Err(failure.error);
                }
            }
        }
    }
}

fn with_mode_header(
    request: reqwest::RequestBuilder,
    mode: Option<u32>,
) -> reqwest::RequestBuilder {
    match mode {
        Some(mode) => request.header(CHEVALIER_VFS_MODE_HEADER, (mode & 0o7777).to_string()),
        None => request,
    }
}

fn with_precondition_headers(
    mut request: reqwest::RequestBuilder,
    base_content_hash: Option<&str>,
    expected_file_id: Option<&str>,
) -> reqwest::RequestBuilder {
    if let Some(base_content_hash) = base_content_hash {
        if base_content_hash == "absent" {
            request = request.header(CHEVALIER_VFS_PRECONDITION_KIND_HEADER, "absent");
        } else {
            request = request
                .header(
                    CHEVALIER_VFS_PRECONDITION_KIND_HEADER,
                    "content_fingerprint",
                )
                .header(
                    CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER,
                    base_content_hash,
                );
        }
    }
    if let Some(expected_file_id) = expected_file_id {
        request = request.header(CHEVALIER_VFS_PRECONDITION_FILE_ID_HEADER, expected_file_id);
    }
    request
}

/// HTTP status carried through anyhow chains so journal replay can tell a
/// gateway rejection (4xx, will never succeed) from a transient failure.
#[derive(Debug)]
pub struct VfsRequestStatusError {
    pub status: StatusCode,
}

impl std::fmt::Display for VfsRequestStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "vfs request status {}", self.status)
    }
}

impl std::error::Error for VfsRequestStatusError {}

pub fn rejected_request_status(error: &anyhow::Error) -> Option<StatusCode> {
    request_status(error).filter(|status| {
        // Only statuses that mean "this exact payload can never succeed".
        // Auth failures (401/403), route skew during deploys (404), rate
        // limits (429), and timeouts (408) are transient conditions and
        // must retain-and-retry, never dead-letter.
        matches!(
            *status,
            StatusCode::BAD_REQUEST | StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED
        )
    })
}

pub fn request_status(error: &anyhow::Error) -> Option<StatusCode> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<VfsRequestStatusError>())
        .map(|status_error| status_error.status)
}

struct ReadFailure {
    transient: bool,
    error: anyhow::Error,
}

impl ReadFailure {
    fn transient(error: anyhow::Error) -> Self {
        Self {
            transient: true,
            error,
        }
    }

    fn terminal(error: anyhow::Error) -> Self {
        Self {
            transient: false,
            error,
        }
    }
}

fn http2_prior_knowledge_enabled(_endpoint: &str) -> bool {
    match std::env::var("CHEVALIER_VFS_HTTP2_PRIOR_KNOWLEDGE") {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

fn common_namespace_parent(mutations: &[VfsNamespaceMutation]) -> String {
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

fn parent_path(path: &str) -> String {
    path.trim_matches('/')
        .rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

fn common_parent<'a>(paths: impl Iterator<Item = &'a str>) -> String {
    let mut parents = paths.map(parent_path);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_retry_budgets_exceed_their_attempt_timeouts() {
        assert!(METADATA_READ_RETRY_TIMEOUT > METADATA_READ_ATTEMPT_TIMEOUT);
        assert!(FILE_READ_RETRY_TIMEOUT > FILE_READ_ATTEMPT_TIMEOUT);
    }

    #[test]
    fn exact_mode_header_is_optional_and_masked() {
        let request = with_mode_header(
            reqwest::Client::new().put("http://localhost"),
            Some(0o106755),
        )
        .build()
        .unwrap();
        assert_eq!(
            request
                .headers()
                .get(CHEVALIER_VFS_MODE_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            0o6755.to_string()
        );

        let request = with_mode_header(reqwest::Client::new().put("http://localhost"), None)
            .build()
            .unwrap();
        assert!(!request.headers().contains_key(CHEVALIER_VFS_MODE_HEADER));
    }

    #[test]
    fn direct_write_headers_preserve_identity_only_preconditions() {
        let request = with_precondition_headers(
            reqwest::Client::new().put("http://localhost"),
            None,
            Some("file-1"),
        )
        .build()
        .unwrap();
        assert!(
            !request
                .headers()
                .contains_key(CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER)
        );
        assert!(
            !request
                .headers()
                .contains_key(CHEVALIER_VFS_PRECONDITION_KIND_HEADER)
        );
        assert_eq!(
            request
                .headers()
                .get(CHEVALIER_VFS_PRECONDITION_FILE_ID_HEADER)
                .unwrap(),
            "file-1"
        );

        let absent = with_precondition_headers(
            reqwest::Client::new().put("http://localhost"),
            Some("absent"),
            None,
        )
        .build()
        .unwrap();
        assert_eq!(
            absent
                .headers()
                .get(CHEVALIER_VFS_PRECONDITION_KIND_HEADER)
                .unwrap(),
            "absent"
        );
        assert!(
            !absent
                .headers()
                .contains_key(CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER)
        );

        let content = with_precondition_headers(
            reqwest::Client::new().put("http://localhost"),
            Some("sha256-content"),
            None,
        )
        .build()
        .unwrap();
        assert_eq!(
            content
                .headers()
                .get(CHEVALIER_VFS_PRECONDITION_KIND_HEADER)
                .unwrap(),
            "content_fingerprint"
        );
        assert_eq!(
            content
                .headers()
                .get(CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER)
                .unwrap(),
            "sha256-content"
        );
    }

    #[test]
    fn namespace_scoping_preserves_optional_exact_modes() {
        let client = RemoteVfsClient::new("http://localhost", "token", "scope").unwrap();

        assert_eq!(
            client.scope_namespace_mutation(&VfsNamespaceMutation::CreateDirectory {
                path: "tree".to_string(),
                mode: Some(0o104775),
            }),
            VfsNamespaceMutation::CreateDirectory {
                path: "scope/tree".to_string(),
                mode: Some(0o4775),
            }
        );
        assert_eq!(
            client.scope_namespace_mutation(&VfsNamespaceMutation::CreateDirectory {
                path: "legacy".to_string(),
                mode: None,
            }),
            VfsNamespaceMutation::CreateDirectory {
                path: "scope/legacy".to_string(),
                mode: None,
            }
        );
        assert_eq!(
            client.scope_namespace_mutation(&VfsNamespaceMutation::SetMode {
                path: "script".to_string(),
                mode: 0o106755,
            }),
            VfsNamespaceMutation::SetMode {
                path: "scope/script".to_string(),
                mode: 0o6755,
            }
        );
    }

    #[test]
    fn write_many_preserves_identity_only_and_absent_preconditions() {
        let client = RemoteVfsClient::new("http://localhost", "token", "scope").unwrap();
        let identity_only = client.scope_remote_write(RemoteWrite {
            path: "tracked".to_string(),
            bytes: b"next".to_vec(),
            base_content_hash: None,
            expected_file_id: Some("file-1".to_string()),
        });
        assert_eq!(identity_only.path, "scope/tracked");
        assert_eq!(
            identity_only
                .precondition
                .as_ref()
                .and_then(|precondition| precondition.expected_file_id.as_deref()),
            Some("file-1")
        );
        assert_eq!(
            identity_only
                .precondition
                .as_ref()
                .and_then(|precondition| precondition.predicate.as_ref()),
            None
        );

        let absent = client.scope_remote_write(RemoteWrite {
            path: "new".to_string(),
            bytes: b"new".to_vec(),
            base_content_hash: Some("absent".to_string()),
            expected_file_id: None,
        });
        assert_eq!(
            absent
                .precondition
                .as_ref()
                .and_then(|precondition| precondition.predicate.as_ref()),
            Some(&VfsCasPredicate::Absent),
        );
        assert!(
            absent
                .precondition
                .as_ref()
                .is_some_and(|precondition| precondition.fingerprint.is_none()
                    && precondition.secondary_fingerprint.is_none())
        );
    }
}
