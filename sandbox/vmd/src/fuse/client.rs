use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chevalier_sandbox::vfs::{
    CHEVALIER_VFS_COMPONENT_HEADER, CHEVALIER_VFS_EXECUTABLE_HEADER,
    CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER, CHEVALIER_VFS_OPERATION_HEADER,
    CHEVALIER_VFS_RESOURCE_KEY_HEADER, CHEVALIER_VFS_SURFACE_KIND_HEADER, VFS_COMPONENT_VM_RUNTIME,
    VfsDirEntry as RemoteDirEntry, VfsLeaseAcquireRequest, VfsLeaseGrant as LeaseGrant,
    VfsLeaseReleaseRequest, VfsMetadata as RemoteMetadata, VfsNamespaceMutation,
    VfsNamespaceMutationBatchBody, VfsWriteManyBody, VfsWriteManyItem, VfsWritePrecondition,
    scoped_vfs_path,
};
use reqwest::{Client, StatusCode, header};

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
        let response = self
            .request(
                self.client
                    .get(self.url("/tree"))
                    .query(&[("path", self.path_arg(path))]),
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        response
            .json()
            .await
            .context("decode vfs tree response")
            .map(Some)
    }

    pub async fn stat(&self, path: &str) -> Result<Option<RemoteMetadata>> {
        let response = self
            .request(
                self.client
                    .get(self.url("/stat"))
                    .query(&[("path", self.path_arg(path))]),
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            bail!("vfs stat failed: {}", response.status());
        }
        response
            .json()
            .await
            .context("decode vfs stat response")
            .map(Some)
    }

    pub async fn read_file_raw(&self, path: &str) -> Result<Vec<u8>> {
        let response = self
            .request(
                self.client
                    .get(self.url("/file/raw"))
                    .query(&[("path", self.path_arg(path))]),
            )
            .await?;
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .context("read vfs raw bytes")
    }

    pub async fn read_file_range(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let response = self
            .request(
                self.client
                    .get(self.url("/file/raw"))
                    .query(&[("path", self.path_arg(path))])
                    .header(
                        header::RANGE,
                        format!("bytes={offset}-{}", offset + length.saturating_sub(1)),
                    ),
            )
            .await?;
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .context("read vfs ranged bytes")
    }

    pub async fn write_file(
        &self,
        path: &str,
        bytes: &[u8],
        executable: bool,
        lease: &LeaseGrant,
        surface_kind: &str,
        operation: &str,
    ) -> Result<()> {
        self.request(
            self.client
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
                )
                .body(bytes.to_vec()),
        )
        .await?;
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
        lease: &LeaseGrant,
        surface_kind: &str,
        operation: &str,
    ) -> Result<()> {
        self.request(
            self.client
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
                ),
        )
        .await?;
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
                .map(|write| VfsWriteManyItem {
                    path: self.path_arg(write.path.as_str()),
                    body: write.bytes,
                    precondition: write
                        .base_content_hash
                        .map(|fingerprint| VfsWritePrecondition {
                            fingerprint: Some(fingerprint),
                            secondary_fingerprint: None,
                        }),
                })
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
            VfsNamespaceMutation::CreateDirectory { path } => {
                VfsNamespaceMutation::CreateDirectory {
                    path: self.path_arg(path),
                }
            }
            VfsNamespaceMutation::CreateSymlink { path, target } => {
                VfsNamespaceMutation::CreateSymlink {
                    path: self.path_arg(path),
                    target: target.clone(),
                }
            }
            VfsNamespaceMutation::DeleteFile { path } => VfsNamespaceMutation::DeleteFile {
                path: self.path_arg(path),
            },
            VfsNamespaceMutation::RemoveDirectory { path } => {
                VfsNamespaceMutation::RemoveDirectory {
                    path: self.path_arg(path),
                }
            }
            VfsNamespaceMutation::Rename { from, to } => VfsNamespaceMutation::Rename {
                from: self.path_arg(from),
                to: self.path_arg(to),
            },
        }
    }

    fn path_arg(&self, relative: &str) -> String {
        scoped_vfs_path(self.scope_path.as_str(), relative)
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
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(response);
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!("vfs request failed: {status} {body}"))
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
