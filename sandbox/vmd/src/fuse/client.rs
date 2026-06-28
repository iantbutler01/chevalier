use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chevalier_sandbox::vfs::{
    CHEVALIER_VFS_COMPONENT_HEADER, CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
    CHEVALIER_VFS_OPERATION_HEADER, CHEVALIER_VFS_RESOURCE_KEY_HEADER,
    CHEVALIER_VFS_SURFACE_KIND_HEADER, VFS_COMPONENT_VM_RUNTIME, VfsDirEntry as RemoteDirEntry,
    VfsLeaseAcquireRequest, VfsLeaseGrant as LeaseGrant, VfsLeaseReleaseRequest,
    VfsMetadata as RemoteMetadata, scoped_vfs_path,
};
use reqwest::{Client, StatusCode, header};

#[derive(Clone, Debug)]
pub struct RemoteVfsClient {
    client: Client,
    endpoint: String,
    auth_token: String,
    scope_path: String,
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
