use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use base64::Engine as _;
use chevalier_sandbox::vfs::{
    CHEVALIER_VFS_COMPONENT_HEADER, CHEVALIER_VFS_LEASE_MODE_HEADER,
    CHEVALIER_VFS_LEASE_MODE_IMPLICIT, CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
    CHEVALIER_VFS_MODE_HEADER, CHEVALIER_VFS_NAMESPACE_REVISION_HEADER,
    CHEVALIER_VFS_OPERATION_HEADER, CHEVALIER_VFS_PRECONDITION_FILE_ID_HEADER,
    CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER, CHEVALIER_VFS_PRECONDITION_KIND_HEADER,
    CHEVALIER_VFS_RESOURCE_KEY_HEADER, CHEVALIER_VFS_SURFACE_KIND_HEADER, VFS_COMPONENT_VM_RUNTIME,
    VfsCasPredicate, VfsDirEntry as RemoteDirEntry, VfsLeaseAcquireRequest,
    VfsLeaseGrant as LeaseGrant, VfsLeaseReleaseRequest, VfsMetadata as RemoteMetadata,
    VfsNamespaceMutation, VfsNamespaceMutationBatchBody, VfsNamespaceMutationBatchResponse,
    VfsPrefetchSubtreeRequest, VfsPrefetchSubtreeResponse, VfsPublicationSnapshotEntry,
    VfsSubtreeMetadataRequest, VfsSubtreeMetadataResponse, VfsWriteManyItem,
    VfsWriteManyPublicationResponse, VfsWritePrecondition, scoped_vfs_path,
};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use tokio_util::io::ReaderStream;

pub const RANGE_FINGERPRINT_HEADER: &str = "x-chevalier-vfs-range-fingerprint";
/// Transient-failure budget sized to ride out a gateway restart, not mask a
/// broken one: only transport failures and 5xx/429/408 consume it — hard 4xx
/// rejections fail on the first attempt.
const METADATA_READ_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
// File bodies have a longer per-attempt timeout than metadata. Their total
// budget must exceed one attempt or a congested first request can never retry.
const FILE_READ_RETRY_TIMEOUT: Duration = Duration::from_secs(45);
const METADATA_READ_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const FILE_READ_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(15);
/// Hard wall for one streamed write publication. Large writes bypass the
/// client's normal 30s mutation timeout, but remain bounded so a wedged
/// gateway cannot turn a close/fsync into the old multi-minute stall.
const STREAM_WRITE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(300);
const STREAM_UPLOAD_HEADER: &str = "x-chevalier-vfs-stream-upload";
const EXPECTED_CONTENT_HASH_HEADER: &str = "x-chevalier-vfs-expected-content-sha256";
const STREAM_READ_BUFFER_BYTES: usize = 1024 * 1024;
const ADVISORY_LOCK_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const ADVISORY_LOCK_RENEWAL_BATCH_SIZE: usize = 4_096;
const READ_RETRY_DELAY_MIN: Duration = Duration::from_millis(50);
const READ_RETRY_DELAY_MAX: Duration = Duration::from_millis(500);
/// Content-hash budget the mount asks every bulk metadata route (`/tree`,
/// `/subtree-metadata`) to honour: hash a file at or under this size, skip it
/// above.
///
/// Hydration is the only caller now, and it uses the hash as the content
/// precondition it verifies each downloaded file against, so an entry that
/// arrives hashed saves a verification read the mount would otherwise do
/// itself.
///
/// The bound is the whole design. Hashing is the gateway reading the file, and
/// a bulk route can name thousands of paths, so an unbounded budget would turn
/// a metadata sweep into a full content read of the tree. At 1 MiB a hash costs
/// the gateway well under a millisecond against a local read (and is memoized
/// in its mtime/ctime-keyed hash cache, so a re-sweep is free). Above it the
/// entry simply arrives hashless and the hydrator verifies the bytes it
/// downloaded on its own terms. The bulk routes are entry capped
/// (`MAX_METADATA_BATCH_PATHS` / `MAX_SUBTREE_METADATA_ENTRIES`), so this bound
/// is also what caps the work one request can ask of the gateway.
///
/// This is a request, not a requirement: `max_hash_bytes` is an established
/// query/body field on both routes, and a gateway that ignores it (or answers
/// without hashes at all) simply leaves entries unhashed.
pub(super) const BULK_METADATA_MAX_HASH_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct RemoteVfsClient {
    client: Client,
    endpoint: String,
    auth_token: String,
    scope_path: String,
    revisions: Arc<SharedRevisionState>,
}

/// The little cross-clone state one mount's client keeps.
///
/// It used to be a per-(endpoint, scope) registry backing a coherence fence and
/// a long-poll revision watch. Both are gone: a writable mount owns its scope
/// and serves every read from its backing tree, so there is nothing for a
/// sibling to invalidate and nothing to keep confirmed. What survives is the
/// highest namespace revision this client has seen an authoritative read answer
/// with -- the seed a read-only observer's follower starts its poll from -- and
/// the gateway's implicit-lease advertisement.
#[derive(Debug, Default)]
struct SharedRevisionState {
    /// Highest revision proven visible by an authoritative read through this
    /// client. Read by `observed_namespace_revision`.
    observed_read: AtomicU64,
    /// Set only after the gateway explicitly advertises that mutation
    /// endpoints provide their own serialization. Real lease-backed gateways
    /// never set this and retain acquire/release behavior unchanged.
    implicit_leases: AtomicBool,
}

pub struct RemoteWrite {
    pub path: String,
    pub bytes: Vec<u8>,
    pub base_content_hash: Option<String>,
    pub expected_file_id: Option<String>,
    /// Applied only if this write creates the path; see `VfsWriteManyItem::mode`.
    pub mode: Option<u32>,
}

/// Compact wire form for the gateway's JSON `write-many` route.
///
/// Serializing `Vec<u8>` directly turns every byte into a decimal JSON token,
/// inflating a package tree several-fold before it crosses the network. The
/// gateway accepts this base64 field alongside the legacy `body: number[]`
/// shape and decodes it before calling the same backend-neutral `writeMany`.
#[derive(Serialize)]
struct CompactWriteManyItem {
    path: String,
    body_base64: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    precondition: Option<VfsWritePrecondition>,
}

#[derive(Serialize)]
struct CompactWriteManyBody {
    writes: Vec<CompactWriteManyItem>,
}

impl From<VfsWriteManyItem> for CompactWriteManyItem {
    fn from(write: VfsWriteManyItem) -> Self {
        Self {
            path: write.path,
            body_base64: base64::engine::general_purpose::STANDARD.encode(write.body),
            mode: write.mode,
            precondition: write.precondition,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Versioned<T> {
    pub value: T,
    /// Exact namespace revision attached to the response that produced
    /// `value`. Zero means the gateway did not provide a revision and the
    /// result must not seed a coherence-sensitive cache.
    pub revision: u64,
}

#[derive(Clone, Debug, Default)]
pub struct RemotePublication {
    pub revision: u64,
    pub entries: Vec<VfsPublicationSnapshotEntry>,
}

#[derive(Deserialize)]
struct StreamWritePublicationResponse {
    #[serde(default)]
    entries: Vec<VfsPublicationSnapshotEntry>,
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
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let scope_path = scope_path.trim_matches('/').to_string();
        let mut builder = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .http2_adaptive_window(true);
        if http2_prior_knowledge_enabled(&endpoint) {
            builder = builder.http2_prior_knowledge();
        }
        let client = builder.build().context("build vfs reqwest client")?;
        Ok(Self {
            client,
            endpoint,
            auth_token: auth_token.to_string(),
            scope_path,
            revisions: Arc::new(SharedRevisionState::default()),
        })
    }

    /// Highest namespace revision an authoritative read through this client has
    /// answered with. A read-only observer's follower seeds its poll from it so
    /// a hydrate that already saw the current revision does not immediately
    /// re-read the scope.
    pub fn observed_namespace_revision(&self) -> u64 {
        self.revisions.observed_read.load(Ordering::Acquire)
    }

    fn observe_read_revision(&self, revision: u64) {
        self.revisions
            .observed_read
            .fetch_max(revision, Ordering::AcqRel);
    }

    pub async fn list_dir_versioned(
        &self,
        path: &str,
    ) -> Result<Versioned<Option<Vec<RemoteDirEntry>>>> {
        self.read_decoded(
            self.client
                .get(self.url("/tree"))
                .query(&[
                    ("path", self.path_arg(path)),
                    ("max_hash_bytes", BULK_METADATA_MAX_HASH_BYTES.to_string()),
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

    /// A point stat carrying the gateway's full content hash. Hydration's
    /// per-file precondition and the publisher's reconciliation read both need
    /// it, so it is deliberately unbounded.
    pub async fn stat_versioned(&self, path: &str) -> Result<Versioned<Option<RemoteMetadata>>> {
        self.stat_with_max_hash_bytes_versioned(path, None).await
    }

    /// The cheapest point stat the gateway offers: attributes and the scope's
    /// namespace revision, no content hash and therefore no gateway-side read.
    /// This is the read a read-only observer's follower probes the scope
    /// revision with.
    pub async fn stat_attributes_versioned(
        &self,
        path: &str,
    ) -> Result<Versioned<Option<RemoteMetadata>>> {
        self.stat_with_max_hash_bytes_versioned(path, Some(0)).await
    }

    pub async fn subtree_metadata_attributes_versioned(
        &self,
        prefix: &str,
        limit: i64,
    ) -> Result<Versioned<Vec<(String, RemoteMetadata)>>> {
        self.read_decoded(
            self.client
                .post(self.url("/subtree-metadata"))
                .json(&VfsSubtreeMetadataRequest {
                    prefix: self.path_arg(prefix),
                    include_object_state: false,
                    include_token_count: false,
                    limit: Some(limit),
                    max_hash_bytes: Some(BULK_METADATA_MAX_HASH_BYTES),
                })
                .timeout(METADATA_READ_ATTEMPT_TIMEOUT),
            METADATA_READ_RETRY_TIMEOUT,
            |status, body| {
                if status == StatusCode::NOT_FOUND {
                    return Err(anyhow::Error::new(VfsRequestStatusError { status })
                        .context("vfs subtree-metadata route not found"));
                }
                if !status.is_success() {
                    return Err(anyhow!("vfs subtree-metadata failed: {status}"));
                }
                let response = serde_json::from_slice::<VfsSubtreeMetadataResponse>(body)
                    .context("decode vfs subtree-metadata response")?;
                Ok(response
                    .entries
                    .into_iter()
                    .map(|entry| {
                        (
                            self.unscoped_path(entry.path.as_str()),
                            RemoteMetadata {
                                kind: entry.kind,
                                size_bytes: entry.size_bytes,
                                file_id: entry.file_id,
                                link_count: entry.link_count,
                                link_target: entry.link_target,
                                content_hash: entry.content_hash,
                                executable: entry.executable,
                                mode: entry.mode,
                                updated_at: entry.updated_at,
                            },
                        )
                    })
                    .collect())
            },
        )
        .await
    }

    pub async fn prefetch_subtree_versioned(
        &self,
        prefix: &str,
        max_entries: i64,
        max_pack_bytes: u64,
    ) -> Result<Versioned<Vec<(String, Vec<u8>)>>> {
        self.read_decoded(
            self.client
                .post(self.url("/prefetch-subtree"))
                .json(&VfsPrefetchSubtreeRequest {
                    prefix: self.path_arg(prefix),
                    include_small_file_bytes: true,
                    max_entries: Some(max_entries),
                    max_pack_bytes: Some(max_pack_bytes),
                })
                .timeout(FILE_READ_ATTEMPT_TIMEOUT),
            FILE_READ_RETRY_TIMEOUT,
            |status, body| {
                if status == StatusCode::NOT_FOUND {
                    return Err(anyhow::Error::new(VfsRequestStatusError { status })
                        .context("vfs prefetch-subtree route not found"));
                }
                if !status.is_success() {
                    return Err(anyhow!("vfs prefetch-subtree failed: {status}"));
                }
                let response = serde_json::from_slice::<VfsPrefetchSubtreeResponse>(body)
                    .context("decode vfs prefetch-subtree response")?;
                Ok(response
                    .warmed_file_bytes
                    .into_iter()
                    .map(|entry| (self.unscoped_path(entry.path.as_str()), entry.body))
                    .collect())
            },
        )
        .await
    }

    async fn stat_with_max_hash_bytes_versioned(
        &self,
        path: &str,
        max_hash_bytes: Option<u64>,
    ) -> Result<Versioned<Option<RemoteMetadata>>> {
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

    pub async fn read_file_raw_versioned(&self, path: &str) -> Result<Versioned<Option<Vec<u8>>>> {
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

    pub async fn read_file_range_versioned(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        fingerprint: Option<&str>,
    ) -> Result<Versioned<RangeRead>> {
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

    pub async fn acquire_lease(
        &self,
        path: &str,
        mutation_count: i32,
        reason: &str,
    ) -> Result<LeaseGrant> {
        let scoped_path = self.path_arg(path);
        if self.revisions.implicit_leases.load(Ordering::Acquire) {
            return Ok(implicit_lease_grant(scoped_path.as_str()));
        }
        let response = self
            .request(
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
            .await?;
        if response
            .headers()
            .get(CHEVALIER_VFS_LEASE_MODE_HEADER)
            .and_then(|value| value.to_str().ok())
            == Some(CHEVALIER_VFS_LEASE_MODE_IMPLICIT)
        {
            self.revisions
                .implicit_leases
                .store(true, Ordering::Release);
        }
        response.json().await.context("decode vfs lease response")
    }

    pub async fn release_lease(&self, lease: &LeaseGrant) -> Result<()> {
        if self.revisions.implicit_leases.load(Ordering::Acquire) {
            return Ok(());
        }
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
        operation_ids: &[String],
        mutations: &[VfsNamespaceMutation],
        surface_kind: &str,
    ) -> Result<RemotePublication> {
        if mutations.is_empty() {
            return Ok(RemotePublication {
                revision: self.observed_namespace_revision(),
                entries: Vec::new(),
            });
        }
        if operation_ids.len() != mutations.len() {
            return Err(anyhow!(
                "namespace batch has {} operation ids for {} mutations",
                operation_ids.len(),
                mutations.len()
            ));
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
        let result = async {
            let response = self
                .request_mutation(
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
                        .json(&VfsNamespaceMutationBatchBody {
                            operation_ids: operation_ids.to_vec(),
                            mutations: scoped,
                        }),
                )
                .await?;
            let revision = parse_namespace_revision(response.headers())
                .expect("mutation response revision was validated");
            let body = response
                .bytes()
                .await
                .context("read namespace batch response")?;
            let decoded = if body.is_empty() {
                VfsNamespaceMutationBatchResponse::default()
            } else {
                serde_json::from_slice::<VfsNamespaceMutationBatchResponse>(body.as_ref())
                    .context("decode namespace batch response")?
            };
            Ok(RemotePublication {
                revision,
                entries: self.unscoped_publication_entries(decoded.entries),
            })
        }
        .await;
        let release = self.release_lease(&lease).await;
        match (result, release) {
            (Ok(publication), Ok(())) => Ok(publication),
            (Err(error), _) => Err(error),
            (Ok(publication), Err(error)) => {
                tracing::warn!(
                    revision = publication.revision,
                    error = %error,
                    "namespace batch committed but lease release failed"
                );
                Ok(publication)
            }
        }
    }

    pub async fn write_many(
        &self,
        writes: Vec<RemoteWrite>,
        surface_kind: &str,
    ) -> Result<RemotePublication> {
        if writes.is_empty() {
            return Ok(RemotePublication {
                revision: self.observed_namespace_revision(),
                entries: Vec::new(),
            });
        }
        let lease_path = common_parent(writes.iter().map(|write| write.path.as_str()));
        let lease = self
            .acquire_lease(
                lease_path.as_str(),
                writes.len() as i32,
                "flush vfs fuse write batch",
            )
            .await?;
        let body = CompactWriteManyBody {
            writes: writes
                .into_iter()
                .map(|write| self.scope_remote_write(write))
                .map(CompactWriteManyItem::from)
                .collect(),
        };
        let result = async {
            let response = self
                .request_mutation(
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
                .await?;
            let revision = parse_namespace_revision(response.headers())
                .expect("mutation response revision was validated");
            let body = response
                .bytes()
                .await
                .context("read write batch response")?;
            let decoded = serde_json::from_slice::<VfsWriteManyPublicationResponse>(body.as_ref())
                .context("decode write batch response")?;
            Ok(RemotePublication {
                revision,
                entries: self.unscoped_publication_entries(decoded.entries),
            })
        }
        .await;
        let release = self.release_lease(&lease).await;
        match (result, release) {
            (Ok(publication), Ok(())) => Ok(publication),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    /// Publish one already-staged large file without materializing it in the
    /// vmd heap or JSON/base64 expanding it through `/write-many`.
    ///
    /// The gateway verifies `content_hash` while streaming the request to its
    /// own temporary file, then hands that file to the storage backend. Small
    /// writes continue to use `write_many`; this is the bounded-memory path the
    /// publisher takes for a single oversized WAL payload.
    pub async fn write_staged_file(
        &self,
        path: &str,
        staged_path: &Path,
        size_bytes: u64,
        content_hash: &str,
        base_content_hash: Option<&str>,
        expected_file_id: Option<&str>,
        mode: Option<u32>,
        surface_kind: &str,
    ) -> Result<RemotePublication> {
        let staged = tokio::fs::File::open(staged_path)
            .await
            .with_context(|| format!("open staged vfs stream {}", staged_path.display()))?;
        let metadata = staged
            .metadata()
            .await
            .with_context(|| format!("stat staged vfs stream {}", staged_path.display()))?;
        if !metadata.is_file() || metadata.len() != size_bytes {
            return Err(anyhow!(
                "staged vfs stream {} has {} bytes but the publication requires {}",
                staged_path.display(),
                metadata.len(),
                size_bytes,
            ));
        }

        let lease = self
            .acquire_lease(path, 1, "flush streamed vfs fuse write")
            .await?;
        let result = async {
            let mut request = self
                .client
                .put(self.url("/file"))
                .query(&[("path", self.path_arg(path))])
                .header(CHEVALIER_VFS_COMPONENT_HEADER, VFS_COMPONENT_VM_RUNTIME)
                .header(CHEVALIER_VFS_SURFACE_KIND_HEADER, surface_kind)
                .header(CHEVALIER_VFS_OPERATION_HEADER, "vfs_stream_write")
                .header(
                    CHEVALIER_VFS_RESOURCE_KEY_HEADER,
                    lease.resource_key.as_str(),
                )
                .header(
                    CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER,
                    lease.owner_token.to_string(),
                )
                .header(STREAM_UPLOAD_HEADER, "1")
                .header(EXPECTED_CONTENT_HASH_HEADER, content_hash)
                .header(header::CONTENT_LENGTH, size_bytes)
                .timeout(STREAM_WRITE_ATTEMPT_TIMEOUT);
            request = with_mode_header(request, mode);
            request = with_precondition_headers(request, base_content_hash, expected_file_id);
            let body = reqwest::Body::wrap_stream(ReaderStream::with_capacity(
                staged,
                STREAM_READ_BUFFER_BYTES,
            ));
            let response = self.request_mutation(request.body(body)).await?;
            let revision = parse_namespace_revision(response.headers())
                .expect("mutation response revision was validated");
            let body = response
                .bytes()
                .await
                .context("read streamed write response")?;
            let decoded = serde_json::from_slice::<StreamWritePublicationResponse>(body.as_ref())
                .context("decode streamed write response")?;
            Ok(RemotePublication {
                revision,
                entries: self.unscoped_publication_entries(decoded.entries),
            })
        }
        .await;
        let release = self.release_lease(&lease).await;
        match (result, release) {
            (Ok(publication), Ok(())) => Ok(publication),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    fn scope_namespace_mutation(&self, mutation: &VfsNamespaceMutation) -> VfsNamespaceMutation {
        match mutation {
            VfsNamespaceMutation::CreateFile { path, mode } => VfsNamespaceMutation::CreateFile {
                path: self.path_arg(path),
                mode: mode.map(|mode| mode & 0o7777),
            },
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
            VfsNamespaceMutation::CreateHardLink {
                source_path,
                destination_path,
            } => VfsNamespaceMutation::CreateHardLink {
                source_path: self.path_arg(source_path),
                destination_path: self.path_arg(destination_path),
            },
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
            mode: write.mode,
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

    fn unscoped_publication_entries(
        &self,
        entries: Vec<VfsPublicationSnapshotEntry>,
    ) -> Vec<VfsPublicationSnapshotEntry> {
        entries
            .into_iter()
            .map(|entry| VfsPublicationSnapshotEntry {
                path: self.unscoped_path(entry.path.as_str()),
                metadata: entry.metadata,
            })
            .collect()
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

    async fn request_mutation(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response> {
        let response = self.request(builder).await?;
        // Validated, not recorded: a publication's revision belongs to the WAL's
        // acknowledgement cursor, which the caller advances. Nothing in this
        // client fences a read against it any more.
        parse_namespace_revision(response.headers())?;
        Ok(response)
    }

    async fn read_decoded<T>(
        &self,
        builder: reqwest::RequestBuilder,
        retry_timeout: Duration,
        mut decode: impl FnMut(StatusCode, &[u8]) -> Result<T>,
    ) -> Result<Versioned<T>> {
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
                    let revision = optional_namespace_revision(response.headers())
                        .map_err(ReadFailure::terminal)?;
                    let body = response
                        .bytes()
                        .await
                        .context("read vfs response body")
                        .map_err(ReadFailure::transient)?;
                    return Ok((status, body, revision));
                }
                let body = response.text().await.unwrap_or_default();
                let error = anyhow!("vfs read failed: {status} {body}");
                // CONFLICT on a READ is the gateway's optimistic-snapshot retry
                // signal, not a rejection: `optimisticRead` runs a recursive read
                // without excluding mutations and returns 409 when a writer
                // overlapped every attempt. The correct response is to retry, which
                // is precisely what this loop exists to do. Treating it as terminal
                // turned a "try again" into a hard failure for /subtree-metadata and
                // /prefetch-subtree under sustained write churn.
                if status.is_server_error()
                    || matches!(
                        status,
                        StatusCode::TOO_MANY_REQUESTS
                            | StatusCode::REQUEST_TIMEOUT
                            | StatusCode::CONFLICT
                    )
                {
                    Err(ReadFailure::transient(error))
                } else {
                    Err(ReadFailure::terminal(error))
                }
            }
            .await;
            match outcome {
                Ok((status, body, revision)) => {
                    let value = decode(status, body.as_ref())?;
                    if let Some(revision) = revision {
                        self.observe_read_revision(revision);
                    }
                    return Ok(Versioned {
                        value,
                        revision: revision.unwrap_or(0),
                    });
                }
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

fn implicit_lease_grant(scoped_path: &str) -> LeaseGrant {
    LeaseGrant {
        resource_key: format!("implicit:{scoped_path}"),
        owner_token: uuid::Uuid::nil(),
        task_id: None,
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

fn optional_namespace_revision(headers: &header::HeaderMap) -> Result<Option<u64>> {
    headers
        .get(CHEVALIER_VFS_NAMESPACE_REVISION_HEADER)
        .map(|value| {
            value
                .to_str()
                .context("namespace revision header is not ASCII")?
                .parse::<u64>()
                .context("namespace revision header is not a u64")
        })
        .transpose()
}

fn parse_namespace_revision(headers: &header::HeaderMap) -> Result<u64> {
    optional_namespace_revision(headers)?.ok_or_else(|| {
        anyhow!(
            "vfs namespace mutation response omitted {}",
            CHEVALIER_VFS_NAMESPACE_REVISION_HEADER
        )
    })
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

/// HTTP status carried through anyhow chains so the publisher can tell a gateway
/// rejection (4xx, will never succeed) from a transient failure.
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
    use std::sync::Mutex;

    use axum::response::IntoResponse;

    use super::*;

    #[test]
    fn read_retry_budgets_exceed_their_attempt_timeouts() {
        assert!(METADATA_READ_RETRY_TIMEOUT > METADATA_READ_ATTEMPT_TIMEOUT);
        assert!(FILE_READ_RETRY_TIMEOUT > FILE_READ_ATTEMPT_TIMEOUT);
    }

    #[test]
    fn bulk_metadata_routes_request_a_bounded_content_hash() {
        // Finite and non-zero: zero is "hash nothing", which costs hydration a
        // verification read it could have had for free; unbounded would hash
        // whatever a sweep over thousands of paths happened to name.
        const _: () = assert!(BULK_METADATA_MAX_HASH_BYTES > 0);
        const _: () = assert!(BULK_METADATA_MAX_HASH_BYTES <= 16 * 1024 * 1024);

        /// Route -> the `max_hash_bytes` that request carried, in arrival order.
        type RequestedBudgets = Arc<Mutex<Vec<(String, Option<String>)>>>;

        async fn record(
            axum::extract::State(seen): axum::extract::State<RequestedBudgets>,
            request: axum::http::Request<axum::body::Body>,
        ) -> axum::response::Response {
            let route = request.uri().path().to_string();
            let query = request.uri().query().unwrap_or_default().to_string();
            let query_budget = query.split('&').find_map(|pair| {
                pair.strip_prefix("max_hash_bytes=")
                    .map(std::string::ToString::to_string)
            });
            let body = axum::body::to_bytes(request.into_body(), 1 << 20)
                .await
                .unwrap_or_default();
            let body_budget = serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|body| {
                    body.get("max_hash_bytes")
                        .and_then(serde_json::Value::as_u64)
                        .map(|budget| budget.to_string())
                });
            seen.lock()
                .unwrap()
                .push((route.clone(), query_budget.or(body_budget)));
            match route.as_str() {
                "/tree" => axum::Json(serde_json::json!([])).into_response(),
                "/subtree-metadata" => {
                    axum::Json(serde_json::json!({ "entries": [] })).into_response()
                }
                // 404 is a legitimate stat answer (absent path), so the point
                // stat completes without needing a metadata body here.
                _ => StatusCode::NOT_FOUND.into_response(),
            }
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let seen: RequestedBudgets = Arc::new(Mutex::new(Vec::new()));
        let server_seen = Arc::clone(&seen);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                axum::Router::new()
                    .route("/{*path}", axum::routing::any(record))
                    .with_state(server_seen),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "token", "scope").unwrap();

        runtime.block_on(client.list_dir_versioned("dir")).unwrap();
        runtime
            .block_on(client.subtree_metadata_attributes_versioned("dir", 64))
            .unwrap();
        runtime.block_on(client.stat_versioned("dir/file")).unwrap();

        let expected = BULK_METADATA_MAX_HASH_BYTES.to_string();
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                ("/tree".to_string(), Some(expected.clone())),
                ("/subtree-metadata".to_string(), Some(expected)),
                ("/stat".to_string(), None),
            ]
        );
        server.abort();
    }

    #[test]
    fn explicitly_implicit_leases_elide_followup_acquire_and_release_round_trips() {
        use std::sync::atomic::AtomicUsize;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let posts = Arc::new(AtomicUsize::new(0));
        let deletes = Arc::new(AtomicUsize::new(0));
        let server_posts = Arc::clone(&posts);
        let server_deletes = Arc::clone(&deletes);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().route(
                    "/lease",
                    axum::routing::post(move || {
                        let posts = Arc::clone(&server_posts);
                        async move {
                            posts.fetch_add(1, Ordering::AcqRel);
                            let mut response = axum::Json(serde_json::json!({
                                "resource_key": "rk:first",
                                "owner_token": uuid::Uuid::new_v4()
                            }))
                            .into_response();
                            response.headers_mut().insert(
                                header::HeaderName::from_static(CHEVALIER_VFS_LEASE_MODE_HEADER),
                                header::HeaderValue::from_static(CHEVALIER_VFS_LEASE_MODE_IMPLICIT),
                            );
                            response
                        }
                    })
                    .delete(move || {
                        let deletes = Arc::clone(&server_deletes);
                        async move {
                            deletes.fetch_add(1, Ordering::AcqRel);
                            axum::http::StatusCode::NO_CONTENT
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "token", "scope").unwrap();

        let first = runtime
            .block_on(client.acquire_lease("a", 1, "first"))
            .unwrap();
        runtime.block_on(client.release_lease(&first)).unwrap();
        let second = runtime
            .block_on(client.acquire_lease("b", 1, "second"))
            .unwrap();
        runtime.block_on(client.release_lease(&second)).unwrap();

        assert_eq!(posts.load(Ordering::Acquire), 1);
        assert_eq!(deletes.load(Ordering::Acquire), 0);
        assert_eq!(second.owner_token, uuid::Uuid::nil());
        server.abort();
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
            mode: None,
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
            mode: None,
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

    #[test]
    fn write_many_uses_compact_base64_wire_bodies() {
        let encoded = CompactWriteManyItem::from(VfsWriteManyItem {
            path: "scope/file.bin".to_string(),
            body: vec![0, 1, 2, 255],
            mode: Some(0o755),
            precondition: None,
        });
        let wire = serde_json::to_value(encoded).unwrap();

        assert_eq!(wire["path"], "scope/file.bin");
        assert_eq!(wire["body_base64"], "AAEC/w==");
        assert_eq!(wire["mode"], 0o755);
        assert!(wire.get("body").is_none());
        assert!(wire.get("precondition").is_none());
    }
}
