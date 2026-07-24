use std::collections::HashMap;
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chevalier_sandbox::vfs::{
    CHEVALIER_VFS_COMPONENT_HEADER, CHEVALIER_VFS_EXECUTABLE_HEADER,
    CHEVALIER_VFS_LEASE_MODE_HEADER, CHEVALIER_VFS_LEASE_MODE_IMPLICIT,
    CHEVALIER_VFS_LOCK_OWNER_TOKEN_HEADER, CHEVALIER_VFS_MODE_HEADER,
    CHEVALIER_VFS_NAMESPACE_REVISION_HEADER, CHEVALIER_VFS_OPERATION_HEADER,
    CHEVALIER_VFS_PRECONDITION_FILE_ID_HEADER, CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER,
    CHEVALIER_VFS_PRECONDITION_KIND_HEADER, CHEVALIER_VFS_RESOURCE_KEY_HEADER,
    CHEVALIER_VFS_SURFACE_KIND_HEADER, VFS_COMPONENT_VM_RUNTIME, VfsCasPredicate,
    VfsDirEntry as RemoteDirEntry, VfsHardLinkAliasBody, VfsHardLinkAliasResponse, VfsHardLinkBody,
    VfsHardLinkMetadataResponse, VfsLeaseAcquireRequest, VfsLeaseGrant as LeaseGrant,
    VfsLeaseReleaseRequest, VfsMetadata as RemoteMetadata, VfsMetadataManyRequest,
    VfsMetadataManyResponse, VfsNamespaceMutation, VfsNamespaceMutationBatchBody,
    VfsNamespaceMutationBatchResponse, VfsPrefetchSubtreeRequest, VfsPrefetchSubtreeResponse,
    VfsPublicationSnapshotEntry, VfsSubtreeMetadataRequest, VfsSubtreeMetadataResponse,
    VfsWriteManyBody, VfsWriteManyItem, VfsWriteManyPublicationResponse, VfsWritePrecondition,
    scoped_vfs_path,
};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use super::cache::{MountInvalidators, PublicationInvalidation, RemoteFuseCache};
use super::fs::ATTR_ENTRY_LEASE_TTL;

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
const ADVISORY_LOCK_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const ADVISORY_LOCK_RENEWAL_BATCH_SIZE: usize = 4_096;
const READ_RETRY_DELAY_MIN: Duration = Duration::from_millis(50);
const READ_RETRY_DELAY_MAX: Duration = Duration::from_millis(500);
/// Long-poll window advertised to the gateway watch endpoint. The gateway
/// returns 200 as soon as the owner revision advances past `since`, or 204 at
/// this deadline. Kept inside the contract's 1000..30000 ms band.
const REVISION_WATCH_TIMEOUT_MS: u64 = 25_000;
/// Per-attempt HTTP budget for the watch poll: the long-poll window plus slack
/// for the response to land. Explicitly overrides the client's 30s mutation
/// timeout so a held-open watch is never mistaken for a stuck mutation.
const REVISION_WATCH_ATTEMPT_TIMEOUT: Duration =
    Duration::from_millis(REVISION_WATCH_TIMEOUT_MS + 5_000);
/// Reconnect backoff floor after a watch failure; rides out a gateway restart.
const REVISION_WATCH_BACKOFF_MIN: Duration = Duration::from_millis(500);
/// Reconnect backoff ceiling. The watch is down and serves fail closed to
/// strict while backing off, so a bounded retry cadence is enough.
const REVISION_WATCH_BACKOFF_MAX: Duration = Duration::from_secs(5);
/// Content-hash budget the mount asks every bulk metadata route (`/tree`,
/// `/metadata-many`, `/subtree-metadata`) to honour: hash a file at or under
/// this size, skip it above.
///
/// A full stat (`stat_path`, the route open(2) resolves through) promises a
/// content hash — `read_bytes` matches cached bytes against it and `open`
/// chains a write's CAS base from it — so an entry a bulk route installed
/// WITHOUT one cannot stand in for it, and every open of such a path fell
/// through to a point `/stat` (~30 per measured `git status` phase). Asking the
/// bulk routes to hash makes those entries complete, so the open is served from
/// the fence-matched cache instead of costing an RTT.
///
/// The bound is the whole design. Hashing is the gateway reading the file, and
/// a bulk route can name thousands of paths, so an unbounded budget would turn
/// a metadata sweep into a full content read of the tree. At 1 MiB a hash costs
/// the gateway well under a millisecond of hardware-accelerated SHA-256 against
/// a local read (and is memoized in its mtime/ctime-keyed hash cache, so a
/// re-sweep is free), against the ~4ms RTT each avoided point stat saves. Above
/// it, the point stat is the cheaper of the two and is what the mount keeps
/// paying — `full_stat_metadata_is_complete` still gates the serve, so an
/// unhashed entry wires exactly as it does today. The bulk routes are entry
/// capped (`MAX_METADATA_BATCH_PATHS` / `MAX_SUBTREE_METADATA_ENTRIES`), so this
/// bound is also what caps the work one request can ask of the gateway.
///
/// This is a request, not a requirement: `max_hash_bytes` is an established
/// query/body field on all three routes, and a gateway that ignores it (or
/// answers without hashes at all) simply leaves entries incomplete and the
/// mount falls through to the wire as before.
pub(super) const BULK_METADATA_MAX_HASH_BYTES: u64 = 1024 * 1024;

/// Hashing budget for the point `/stat` an `open(2)` falls through to when the
/// bulk-seeded entry is incomplete.
///
/// The point stat sends no budget by default — it owes its caller a hash at any
/// size — and hashing is the gateway READING the file. For a file past this
/// bound that trade is indefensible: opening a 5 GiB ML dataset makes the
/// gateway read 5 GiB, once per hash-cache expiry, to produce a hash the open
/// cannot use. It cannot, because a file this large is never held in the mount's
/// whole-file content cache (`MAX_FILE_BYTES` in fuse/cache.rs, the same 10 MiB
/// as `LARGE_FILE_BYTES`): there are no cached bytes to match it against, and
/// ranged reads are pinned by fingerprint, not by hash. The one remaining
/// consumer is the CAS base a later write chains from, and that base is
/// established authoritatively when the handle is first loaded for that write.
///
/// So the budget is set exactly at the content-cache ceiling: at or under it the
/// gateway still hashes (the open needs the hash to match cached bytes, and the
/// read is bounded), past it the answer comes back hashless and the open serves
/// it as-is. Callers that genuinely require a hash now (`O_TRUNC`, whose CAS
/// base is fixed at open because it publishes without ever loading) keep using
/// the unbounded `stat_versioned`.
pub(super) const OPEN_STAT_MAX_HASH_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct RemoteVfsClient {
    client: Client,
    endpoint: String,
    auth_token: String,
    scope_path: String,
    revisions: Arc<SharedRevisionState>,
}

#[derive(Debug, Default)]
struct SharedRevisionState {
    /// Highest revision proven visible by an authoritative read. Namespace
    /// projections may retire only against this value.
    observed_read: AtomicU64,
    /// Highest revision either read or published by a sibling mount. Cache
    /// entries are valid only while this generation is unchanged.
    coherence: AtomicU64,
    /// Set only after the gateway explicitly advertises that mutation
    /// endpoints provide their own serialization. Real lease-backed gateways
    /// never set this and retain acquire/release behavior unchanged.
    implicit_leases: AtomicBool,
    /// True only while a long-poll revision watch is actively confirming this
    /// registry's coherence fence. Amortized cache serves are valid only under
    /// a live watch; when it drops the fs layer fails closed to strict,
    /// wire-backed serves. This is a live-channel signal, never a wall-clock
    /// freshness window.
    watch_live: AtomicBool,
    /// Guards lazy, once-per-registry spawn of the watch task. The first mount
    /// to use this registry flips it and owns the detached watch loop.
    watch_started: AtomicBool,
    /// Stable, opaque identity for this registry's single watch loop, sent as
    /// `watcher_id` on every poll. The gateway keys revocation-ack progress by
    /// it: a poll's `since` acks that revision, and a sibling's publication
    /// blocks until this watcher's ack reaches the published revision. Generated
    /// once per registry — every sibling mount of the same (endpoint, scope)
    /// shares this one watch loop and therefore this one identity.
    watcher_id: String,
}

fn shared_revision_state(key: &str) -> Arc<SharedRevisionState> {
    static STATES: OnceLock<Mutex<HashMap<String, Weak<SharedRevisionState>>>> = OnceLock::new();
    let mut states = STATES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(state) = states.get(key).and_then(Weak::upgrade) {
        return state;
    }
    let state = Arc::new(SharedRevisionState {
        // A fresh identity per registry so every observer mount-host acks
        // independently; the default atomics fill the rest.
        watcher_id: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    });
    states.insert(key.to_string(), Arc::downgrade(&state));
    state
}

pub struct RemoteWrite {
    pub path: String,
    pub bytes: Vec<u8>,
    pub base_content_hash: Option<String>,
    pub expected_file_id: Option<String>,
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
        let revisions = shared_revision_state(&format!("{endpoint}\n{scope_path}"));
        Ok(Self {
            client,
            endpoint,
            auth_token: auth_token.to_string(),
            scope_path,
            revisions,
        })
    }

    pub fn observed_namespace_revision(&self) -> u64 {
        self.revisions.observed_read.load(Ordering::Acquire)
    }

    pub fn coherence_revision(&self) -> u64 {
        self.revisions.coherence.load(Ordering::Acquire)
    }

    pub fn coherence_key(&self) -> String {
        format!("{}\n{}", self.endpoint, self.scope_path)
    }

    fn observe_read_revision(&self, revision: u64) {
        self.revisions
            .observed_read
            .fetch_max(revision, Ordering::AcqRel);
        self.revisions
            .coherence
            .fetch_max(revision, Ordering::AcqRel);
    }

    pub(super) fn observe_published_revision(&self, revision: u64) {
        self.revisions
            .coherence
            .fetch_max(revision, Ordering::AcqRel);
    }

    /// Whether a long-poll revision watch is currently confirming this
    /// registry's coherence fence. The fs layer serves amortized cache hits
    /// only while this is true and fails closed to strict serves otherwise.
    pub(super) fn revision_watch_live(&self) -> bool {
        self.revisions.watch_live.load(Ordering::Acquire)
    }

    /// Lazily start the per-registry revision watch on first mount use. The
    /// watch keeps the coherence fence continuously confirmed so cache serves
    /// are valid; it is spawned exactly once per endpoint+scope registry and
    /// runs detached (it holds only weak references and never blocks process
    /// shutdown). `cache` is the shared cache for this registry, notified so a
    /// remote publication observed by the watch clears superseded entries;
    /// `invalidators` is the shared set of every mount's kernel-invalidation
    /// hook, swept in lockstep so a remote publication also revokes the affected
    /// kernel attr/entry leases before the watch acks it.
    pub(super) fn ensure_revision_watch(
        &self,
        tokio: &Handle,
        cache: &Arc<RemoteFuseCache>,
        invalidators: &Arc<MountInvalidators>,
    ) {
        if self.revisions.watch_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let http = self.client.clone();
        let endpoint = self.endpoint.clone();
        let auth_token = self.auth_token.clone();
        // The registry is keyed by endpoint + scope, so every mount sharing this
        // watch shares this scope — which is what makes it sound to translate
        // one answer's owner-absolute paths into mount-relative ones once, here.
        let scope_path = self.scope_path.clone();
        let revisions = Arc::downgrade(&self.revisions);
        let cache = Arc::downgrade(cache);
        let notifiers = Arc::downgrade(invalidators);
        tokio.spawn(run_revision_watch(
            http, endpoint, auth_token, scope_path, revisions, cache, notifiers,
        ));
    }

    pub async fn list_dir(&self, path: &str) -> Result<Option<Vec<RemoteDirEntry>>> {
        Ok(self.list_dir_versioned(path).await?.value)
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

    pub async fn stat(&self, path: &str) -> Result<Option<RemoteMetadata>> {
        Ok(self.stat_versioned(path).await?.value)
    }

    pub async fn stat_attributes(&self, path: &str) -> Result<Option<RemoteMetadata>> {
        Ok(self.stat_attributes_versioned(path).await?.value)
    }

    pub async fn stat_versioned(&self, path: &str) -> Result<Versioned<Option<RemoteMetadata>>> {
        self.stat_with_max_hash_bytes_versioned(path, None).await
    }

    pub async fn stat_attributes_versioned(
        &self,
        path: &str,
    ) -> Result<Versioned<Option<RemoteMetadata>>> {
        self.stat_with_max_hash_bytes_versioned(path, Some(0)).await
    }

    /// A point stat for a caller that can proceed without a hash it would have
    /// no use for. See [`OPEN_STAT_MAX_HASH_BYTES`]: the gateway hashes up to
    /// the mount's content-cache ceiling and answers hashless past it, so an
    /// open of a multi-GB file never asks the gateway to read multi-GB.
    pub async fn stat_bounded_hash_versioned(
        &self,
        path: &str,
    ) -> Result<Versioned<Option<RemoteMetadata>>> {
        self.stat_with_max_hash_bytes_versioned(path, Some(OPEN_STAT_MAX_HASH_BYTES))
            .await
    }

    pub async fn metadata_many_attributes(
        &self,
        paths: &[String],
    ) -> Result<Vec<Option<RemoteMetadata>>> {
        Ok(self.metadata_many_attributes_versioned(paths).await?.value)
    }

    pub async fn metadata_many_attributes_versioned(
        &self,
        paths: &[String],
    ) -> Result<Versioned<Vec<Option<RemoteMetadata>>>> {
        let body = VfsMetadataManyRequest {
            paths: paths.iter().map(|path| self.path_arg(path)).collect(),
        };
        self.read_decoded(
            self.client
                .post(self.url("/metadata-many"))
                .query(&[("max_hash_bytes", BULK_METADATA_MAX_HASH_BYTES)])
                .json(&body)
                .timeout(METADATA_READ_ATTEMPT_TIMEOUT),
            METADATA_READ_RETRY_TIMEOUT,
            |status, body| {
                if !status.is_success() {
                    return Err(anyhow!("vfs metadata-many failed: {status}"));
                }
                serde_json::from_slice::<VfsMetadataManyResponse>(body)
                    .context("decode vfs metadata-many response")
                    .map(|response| response.entries)
            },
        )
        .await
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

    pub async fn read_file_raw(&self, path: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.read_file_raw_versioned(path).await?.value)
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

    pub async fn read_file_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        fingerprint: Option<&str>,
    ) -> Result<RangeRead> {
        Ok(self
            .read_file_range_versioned(path, offset, length, fingerprint)
            .await?
            .value)
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
        self.request_mutation(request.body(bytes.to_vec())).await?;
        Ok(())
    }

    pub async fn delete_file(
        &self,
        path: &str,
        lease: &LeaseGrant,
        surface_kind: &str,
        operation: &str,
    ) -> Result<()> {
        self.request_mutation(
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
        self.request_mutation(with_mode_header(request, mode))
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
        self.request_mutation(
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
        self.request_mutation(
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
        Ok(self
            .read_decoded(
                self.client
                    .post(self.url("/hard-link-alias/v1"))
                    .json(&VfsHardLinkAliasBody {
                        file_id: file_id.to_string(),
                        excluding_path: self.path_arg(excluding_path),
                    })
                    .timeout(METADATA_READ_ATTEMPT_TIMEOUT),
                METADATA_READ_RETRY_TIMEOUT,
                |status, body| {
                    if !status.is_success() {
                        return Err(anyhow!("vfs hard-link alias failed: {status}"));
                    }
                    serde_json::from_slice::<VfsHardLinkAliasResponse>(body)
                        .context("decode vfs hard-link alias response")
                        .map(|response| response.path.map(|path| self.unscoped_path(path.as_str())))
                },
            )
            .await?
            .value)
    }

    pub async fn rmdir(
        &self,
        path: &str,
        lease: &LeaseGrant,
        surface_kind: &str,
        operation: &str,
    ) -> Result<()> {
        self.request_mutation(
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
        self.request_mutation(
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
                revision: self.coherence_revision(),
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
        let body = VfsWriteManyBody {
            writes: writes
                .into_iter()
                .map(|write| self.scope_remote_write(write))
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
        let revision = parse_namespace_revision(response.headers())?;
        self.observe_published_revision(revision);
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

#[derive(Deserialize)]
struct RevisionWatchResponse {
    revision: u64,
    /// Paths published between the poll's `since` and `revision`.
    ///
    /// `None` means the field was absent — a gateway that does not report
    /// affected paths at all — which is NOT the same as a present-but-empty
    /// set ("this publication touched nothing this mount must revoke"). The
    /// former must fall back to the conservative sweep; the latter is a
    /// complete answer.
    #[serde(default)]
    paths: Option<Vec<String>>,
    /// The subset of `paths` whose ENTIRE subtree the publications superseded —
    /// a `RemoveDirectory` or a `Rename`, the only two mutations that can move
    /// or remove a whole tree.
    ///
    /// `None` means the field was absent — a gateway too old to distinguish the
    /// kinds — which is NOT the same as a present-but-empty set ("these
    /// publications superseded no subtree"). The former must fall back to the
    /// conservative reading of `paths` as prefixes; the latter is a complete
    /// answer, and is what keeps a lock-file publication inside `.git/` from
    /// evicting `.git/config`.
    #[serde(default)]
    subtrees: Option<Vec<String>>,
    /// Set when the gateway could not report the affected set completely, so
    /// `paths` must not be treated as exhaustive.
    #[serde(default)]
    truncated: bool,
}

/// The affected set one watch answer reported, split by scope.
///
/// The split is the whole point: a publication's affected set names each
/// changed path AND its parent directory, so reading every entry as a subtree
/// prefix means one `.git/index.lock` create drops the cached metadata of
/// `.git/config`, `.git/HEAD`, `.git/info/exclude` and every ref — measured as
/// 30 point stats over 9 paths per warm `git status`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct WatchAffected {
    /// Exact paths the publications changed (each changed path plus its parent).
    paths: Vec<String>,
    /// Prefixes whose whole subtree they superseded. Equal to `paths` when the
    /// gateway did not report the set at all — the conservative reading an
    /// older gateway leaves no alternative to.
    subtrees: Vec<String>,
}

impl WatchAffected {
    /// Translate one answer out of the owner's namespace and into the one this
    /// registry's cache and inode tables are keyed on.
    ///
    /// The watch is per (endpoint, scope) registry, so one scope applies to
    /// every mount that observes this answer. A path outside that scope is
    /// dropped rather than passed through: this mount cannot name it, so it
    /// holds nothing to revoke for it, and passing it through would alias an
    /// unrelated owner path onto a mount-relative one of the same spelling.
    fn unscoped(self, scope_path: &str) -> Self {
        let unscope = |paths: Vec<String>| {
            paths
                .into_iter()
                .filter_map(|path| unscope_watch_path(scope_path, path.as_str()))
                .collect()
        };
        Self {
            paths: unscope(self.paths),
            subtrees: unscope(self.subtrees),
        }
    }
}

/// One owner-absolute watch path as this registry's mounts name it, or `None`
/// when it lies outside their scope.
fn unscope_watch_path(scope_path: &str, path: &str) -> Option<String> {
    let path = path.trim_matches('/');
    let scope_path = scope_path.trim_matches('/');
    if scope_path.is_empty() {
        return Some(path.to_string());
    }
    if path == scope_path {
        // The scope root itself: the mount's own root directory.
        return Some(String::new());
    }
    path.strip_prefix(&format!("{scope_path}/"))
        .map(str::to_string)
}

/// One completed watch poll. `Advanced` carries the new owner revision from a
/// 200; `Unchanged` is a 204 long-poll timeout. Both mean the channel is live.
enum RevisionWatchPoll {
    /// The owner revision advanced. `affected` carries the exact set the
    /// publications touched when the gateway could report it completely;
    /// `None` means the watcher must fall back to its own conservative
    /// revocation (older gateway, truncated set, or a watcher too far behind).
    Advanced {
        revision: u64,
        affected: Option<WatchAffected>,
    },
    Unchanged,
}

/// A watch poll that did not complete cleanly. `Transport` is a send-class
/// failure — the request never round-tripped (connect refused, or, by far most
/// often, the gateway reaped our idle keep-alive socket under its own
/// keepAliveTimeout, which is well under our 25s long-poll window). reqwest
/// evicts the dead socket on that error, so an immediate retry lands on a fresh
/// connection. `Protocol` is a completed round-trip the client rejects (a
/// non-200/204 status, or an undecodable 200 body); retrying it on a fresh
/// connection would not change the answer, so it fails through immediately.
enum RevisionWatchError {
    Transport(anyhow::Error),
    Protocol(anyhow::Error),
}

impl RevisionWatchError {
    /// The underlying cause, for the once-per-transition WARN. The classifier
    /// only gates the immediate retry; the operator-facing message is identical
    /// either way.
    fn into_inner(self) -> anyhow::Error {
        match self {
            RevisionWatchError::Transport(error) | RevisionWatchError::Protocol(error) => error,
        }
    }
}

/// Edge-detects watch health so a WARN is emitted once per transition (down,
/// then restored), never once per retry. Initial establishment is silent.
#[derive(Debug, Default, PartialEq, Eq)]
enum WatchHealth {
    #[default]
    Establishing,
    Live,
    Down,
}

impl WatchHealth {
    /// Record a successful poll. Returns true only on the down -> live edge,
    /// where a "watch restored" WARN should fire. The first establishment
    /// (Establishing -> Live) returns false and stays silent.
    fn on_success(&mut self) -> bool {
        let restored = *self == WatchHealth::Down;
        *self = WatchHealth::Live;
        restored
    }

    /// Record a failed poll. Returns true only on the first edge into Down,
    /// where a "watch unavailable" WARN should fire; further failures are
    /// silent until the channel is restored.
    fn on_failure(&mut self) -> bool {
        let newly_down = *self != WatchHealth::Down;
        *self = WatchHealth::Down;
        newly_down
    }
}

/// Read one watch 200 body into the affected set the watcher may act on.
///
/// An empty set from a gateway that reports completeness is a real answer
/// ("nothing this mount must revoke"); truncation, or a gateway that omits
/// `paths` entirely, forces the conservative fallback (`None`).
///
/// `subtrees` is read on the same terms one level down: present (empty
/// included) means the gateway distinguished the prefixes it wholly superseded
/// from the paths it merely touched, so a create/delete may not evict its
/// siblings. Absent means an older gateway that cannot say which affected paths
/// were directories it removed or renamed, and the only sound reading left is
/// the one this client used before the split existed — every path is a prefix.
fn watch_affected(body: RevisionWatchResponse) -> Option<WatchAffected> {
    if body.truncated {
        return None;
    }
    let paths = body.paths?;
    Some(WatchAffected {
        subtrees: body.subtrees.unwrap_or_else(|| paths.clone()),
        paths,
    })
}

/// Issue one long-poll against the gateway watch endpoint. Uses the same bearer
/// auth as every other route and an explicit read-class timeout above the
/// long-poll window so the client's default mutation timeout never applies.
async fn poll_revision_watch(
    http: &Client,
    endpoint: &str,
    auth_token: &str,
    watcher_id: &str,
    since: u64,
) -> Result<RevisionWatchPoll, RevisionWatchError> {
    let response = http
        .get(format!("{endpoint}/watch"))
        .query(&[
            ("since", since.to_string()),
            ("timeout_ms", REVISION_WATCH_TIMEOUT_MS.to_string()),
            // The stable identity that lets the gateway treat this poll's `since`
            // as an ack of that revision and gate sibling publications on it.
            ("watcher_id", watcher_id.to_string()),
        ])
        .bearer_auth(auth_token)
        .timeout(REVISION_WATCH_ATTEMPT_TIMEOUT)
        .send()
        .await
        // A failed `send` is the send-class case: the request never round-
        // tripped, so the pooled socket (if any) is now evicted.
        .context("send vfs revision watch")
        .map_err(RevisionWatchError::Transport)?;
    let status = response.status();
    if status == StatusCode::NO_CONTENT {
        return Ok(RevisionWatchPoll::Unchanged);
    }
    if status == StatusCode::OK {
        let body: RevisionWatchResponse = response
            .json()
            .await
            .context("decode vfs revision watch response")
            .map_err(RevisionWatchError::Protocol)?;
        return Ok(RevisionWatchPoll::Advanced {
            revision: body.revision,
            affected: watch_affected(body),
        });
    }
    let body = response.text().await.unwrap_or_default();
    Err(RevisionWatchError::Protocol(anyhow!(
        "vfs revision watch returned {status} {body}"
    )))
}

/// One watch poll, absorbing a single send-class blip. A gateway that reaps our
/// idle keep-alive socket surfaces the reap as a transport error on the NEXT
/// poll; reqwest evicts that dead socket as it fails, so the immediate retry
/// lands on a fresh connection and succeeds. Only when the retry ALSO fails
/// (a second consecutive failure) — or the first failure is a protocol reject,
/// which a retry cannot fix — does the error reach the caller, which then
/// declares the watch down. This keeps one reaped socket from flapping
/// `watch_live` (and resurfacing strict serves) every keepAlive interval.
async fn poll_revision_watch_resilient(
    http: &Client,
    endpoint: &str,
    auth_token: &str,
    watcher_id: &str,
    since: u64,
) -> Result<RevisionWatchPoll, RevisionWatchError> {
    match poll_revision_watch(http, endpoint, auth_token, watcher_id, since).await {
        Err(RevisionWatchError::Transport(first)) => {
            tracing::debug!(
                error = %first,
                "vfs revision watch transport blip; retrying once on a fresh connection"
            );
            poll_revision_watch(http, endpoint, auth_token, watcher_id, since).await
        }
        other => other,
    }
}

/// Per-registry watch loop. Keeps the coherence fence continuously confirmed so
/// the fs layer may serve fence-matched cache hits. Fails closed: any transport
/// error or non-200/204 clears `watch_live` (strict serves resume) and backs
/// off before retrying. Exits only when the registry itself is dropped so it
/// never blocks process shutdown.
async fn run_revision_watch(
    http: Client,
    endpoint: String,
    auth_token: String,
    scope_path: String,
    revisions: Weak<SharedRevisionState>,
    cache: Weak<RemoteFuseCache>,
    notifiers: Weak<MountInvalidators>,
) {
    let mut health = WatchHealth::default();
    let mut backoff = REVISION_WATCH_BACKOFF_MIN;
    // Capture the stable watcher identity once. It never changes for the life of
    // the registry, and every poll must present it so the gateway can key ack
    // progress to this watcher.
    let watcher_id = match revisions.upgrade() {
        Some(state) => state.watcher_id.clone(),
        None => return,
    };
    loop {
        // Read the fence, then drop the strong ref so a long-poll never pins
        // the registry alive across the await.
        let since = match revisions.upgrade() {
            Some(state) => state.coherence.load(Ordering::Acquire),
            None => return,
        };
        match poll_revision_watch_resilient(&http, &endpoint, &auth_token, &watcher_id, since).await
        {
            Ok(poll) => {
                let Some(state) = revisions.upgrade() else {
                    return;
                };
                if let RevisionWatchPoll::Advanced { revision, affected } = poll {
                    // The gateway answers in the owner's namespace; this cache
                    // and every mount's inode table are keyed on paths relative
                    // to the registry's scope. Translate once, here, before
                    // either is touched. Without it every targeted revocation
                    // silently matched nothing: the path a sibling actually
                    // changed was never evicted (it was retagged forward as
                    // "unaffected"), and no kernel lease was revoked, yet the
                    // watch still acked and unblocked the writer.
                    let affected = affected.map(|affected| affected.unscoped(scope_path.as_str()));
                    // ACK-ORDERING INVARIANT (revocation-acked publications): the
                    // gateway treats the NEXT poll's `since` as this watcher's ack
                    // of that revision, and a sibling's publication is blocked
                    // until this ack lands. We MUST advance the fence and clear the
                    // cache BEFORE that next poll is issued, so the ack can never
                    // precede this mount becoming coherent — otherwise the writer
                    // would unblock while this mount could still serve stale reads.
                    // The next poll reads `since` from `coherence` at the top of
                    // the loop, strictly after both stores below, so the order
                    // holds. Fence first:
                    state.coherence.fetch_max(revision, Ordering::AcqRel);
                    // Apply the publication to the shared cache with the SAME
                    // set the kernel revocation below uses. The gateway reports
                    // the exact union of paths published in `(since, revision]`
                    // (or reports the answer truncated, in which case `affected`
                    // is None), and that set is already trusted to be exhaustive
                    // enough to drive every guest kernel's revocation — so
                    // trusting anything less of it here was never a safety
                    // property, only a cost. Discarding it meant every
                    // publication, INCLUDING THIS PROCESS'S OWN, wiped the whole
                    // shared cache: a warm `git status` re-read the entire tree
                    // because git had rewritten its index mid-scan.
                    //
                    // Fail-closed is unchanged where it is load-bearing: without
                    // a trustworthy set the blunt authoritative-revision clear
                    // still runs, and the cache itself refuses the targeted path
                    // whenever its own fence is older than `since` (publications
                    // it never classified would otherwise go unaccounted for).
                    if let Some(cache) = cache.upgrade() {
                        match &affected {
                            // Point paths evict their own entry (and their
                            // parent's listing); only the prefixes the gateway
                            // named as superseded subtrees evict descendants.
                            // Reading every path as a prefix is what made one
                            // `.git/index.lock` publication drop the cached
                            // metadata of every `.git` internal and cost the
                            // next `git status` phase 30 point stats.
                            Some(affected) => cache.observe_remote_publication(
                                since,
                                revision,
                                &affected.paths,
                                &affected.subtrees,
                            ),
                            None => cache.observe_authoritative_revision(revision),
                        }
                    }
                    // Extend the ack-ordering invariant to the KERNEL: this
                    // remote publication carries only a revision (no path set),
                    // so sweep every attr/dentry each mount of this registry
                    // handed its kernel. This MUST complete before the ack — the
                    // next loop iteration reads `since` from `coherence`
                    // (advanced above) and re-polls, and the gateway treats that
                    // poll as this watcher's ack, unblocking the remote writer.
                    // Sweeping here guarantees the writer's fsync return implies
                    // this process's kernels hold no superseded attrs, exactly as
                    // the cache clear guarantees no stale userspace serve.
                    //
                    // The notifier calls themselves run on the registry's
                    // invalidation worker, never inline here: they block in the
                    // guest kernel (`fuse_reverse_inval_entry` waits on the
                    // parent inode's lock, held by whatever op is mutating that
                    // directory) and this is a tokio runtime thread — the same
                    // pool serving the FUSE dispatch work that must complete for
                    // that lock to drop. So enqueue, then await the drain: this
                    // task holds no FUSE lock and no FUSE op waits on it, so
                    // waiting here is safe and keeps the ordering exact.
                    let swept = match notifiers.upgrade() {
                        Some(notifiers) => {
                            let ticket = match &affected {
                                // The gateway reported exactly what changed, so
                                // revoke that and nothing else. This is the whole
                                // point of the targeted set: an untargeted sweep
                                // is proportional to the mount's working set and
                                // takes a guest-kernel parent-inode write lock
                                // per entry, which stalls the guest's own lookups
                                // behind it.
                                Some(affected) => notifiers.enqueue_revocation_tracked(
                                    &PublicationInvalidation::for_affected(
                                        &affected.paths,
                                        &affected.subtrees,
                                    ),
                                ),
                                // No trustworthy set: fall back to the untargeted
                                // sweep, which declines itself past its own bound
                                // and fails closed rather than storming.
                                None => notifiers.enqueue_full_sweep_tracked(),
                            };
                            // Resolves only once THIS enqueued revocation has
                            // been applied (or refused: a saturated queue and a
                            // shutting-down worker both resolve to `false`, which
                            // takes the same fail-closed path below as a notifier
                            // error).
                            ticket.landed().await
                        }
                        None => true,
                    };
                    if !swept {
                        // A revocation did not land, so the kernel may still
                        // serve an attr this revision supersedes. Acking now
                        // would unblock the remote writer against that stale
                        // lease, so fail closed instead: drop watch liveness
                        // (subsequent replies carry TTL=0 and every serve is
                        // wire-backed) and hold the ack until any lease granted
                        // before the failure has expired on its own.
                        tracing::warn!(
                            revision,
                            "vfs kernel revocation incomplete; serving strict until leases expire"
                        );
                        state.watch_live.store(false, Ordering::Release);
                        drop(state);
                        tokio::time::sleep(ATTR_ENTRY_LEASE_TTL).await;
                        continue;
                    }
                }
                // Set last: an observer that sees watch_live is guaranteed to
                // also see the fence advance, cache clear, and kernel sweep
                // above.
                state.watch_live.store(true, Ordering::Release);
                drop(state);
                if health.on_success() {
                    tracing::warn!("vfs revision watch restored");
                }
                backoff = REVISION_WATCH_BACKOFF_MIN;
                // Immediate re-poll (no sleep/backoff on success): this next poll
                // carries the ack for the revision just applied, so writer-visible
                // ack latency is ~RTT + apply cost, not RTT + a backoff.
            }
            Err(error) => {
                if let Some(state) = revisions.upgrade() {
                    state.watch_live.store(false, Ordering::Release);
                }
                if health.on_failure() {
                    tracing::warn!(
                        error = %error.into_inner(),
                        "vfs revision watch unavailable; serving strict metadata"
                    );
                }
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(REVISION_WATCH_BACKOFF_MAX);
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
    use axum::response::IntoResponse;

    #[test]
    fn read_retry_budgets_exceed_their_attempt_timeouts() {
        assert!(METADATA_READ_RETRY_TIMEOUT > METADATA_READ_ATTEMPT_TIMEOUT);
        assert!(FILE_READ_RETRY_TIMEOUT > FILE_READ_ATTEMPT_TIMEOUT);
    }

    /// The gateway answers in the OWNER's namespace; the shared cache and every
    /// mount's inode table are keyed relative to the registry's scope. Until the
    /// answer is translated, every targeted revocation matched nothing at all —
    /// the path a sibling mount actually changed was retagged forward as
    /// "unaffected" instead of evicted, and no kernel lease was revoked, yet the
    /// watch still acked and unblocked the writer.
    #[test]
    fn watch_paths_are_translated_out_of_the_owner_namespace() {
        let affected = WatchAffected {
            paths: vec![
                "test-scope/git/index.lock".to_string(),
                "test-scope/git".to_string(),
                "test-scope".to_string(),
                "other-scope/unrelated".to_string(),
            ],
            subtrees: vec![
                "test-scope/tree/doomed".to_string(),
                "other-scope/tree".to_string(),
            ],
        };

        assert_eq!(
            affected.clone().unscoped("test-scope"),
            WatchAffected {
                // The scope root maps to this mount's own root ("").
                paths: vec![
                    "git/index.lock".to_string(),
                    "git".to_string(),
                    String::new(),
                ],
                subtrees: vec!["tree/doomed".to_string()],
            },
            "a path outside the scope is dropped, never passed through: this \
             mount cannot name it, and passing it through would alias an \
             unrelated owner path onto a mount-relative one of the same spelling"
        );

        // An unscoped registry sees the owner namespace directly.
        assert_eq!(
            WatchAffected {
                paths: vec!["a/b".to_string()],
                subtrees: Vec::new(),
            }
            .unscoped(""),
            WatchAffected {
                paths: vec!["a/b".to_string()],
                subtrees: Vec::new(),
            }
        );
    }

    /// A watch answer's `subtrees` field decides whether a publication may cost
    /// a sibling entry its cached metadata, so all three of its states must stay
    /// distinguishable on the wire: populated, present-and-empty, and absent.
    #[test]
    fn watch_subtrees_distinguish_present_empty_from_absent() {
        let decode = |body: serde_json::Value| {
            watch_affected(serde_json::from_value::<RevisionWatchResponse>(body).unwrap())
        };

        // Present and EMPTY: a create/delete supersedes its own path and its
        // parent's listing, and nothing beneath either. No prefixes.
        assert_eq!(
            decode(serde_json::json!({
                "revision": 18,
                "paths": ["git/index.lock", "git"],
                "subtrees": [],
            })),
            Some(WatchAffected {
                paths: vec!["git/index.lock".to_string(), "git".to_string()],
                subtrees: Vec::new(),
            })
        );

        // Populated: only the rmdir/rename prefixes, never their parents.
        assert_eq!(
            decode(serde_json::json!({
                "revision": 19,
                "paths": ["tree/doomed", "tree"],
                "subtrees": ["tree/doomed"],
            })),
            Some(WatchAffected {
                paths: vec!["tree/doomed".to_string(), "tree".to_string()],
                subtrees: vec!["tree/doomed".to_string()],
            })
        );

        // ABSENT: an older gateway that cannot say which paths were directories.
        // The only sound reading left is the pre-split one — every path is a
        // prefix — which is conservative, not weaker.
        assert_eq!(
            decode(serde_json::json!({
                "revision": 20,
                "paths": ["git/index.lock", "git"],
            })),
            Some(WatchAffected {
                paths: vec!["git/index.lock".to_string(), "git".to_string()],
                subtrees: vec!["git/index.lock".to_string(), "git".to_string()],
            })
        );

        // Truncated and path-less answers keep forcing the full sweep.
        assert_eq!(
            decode(serde_json::json!({
                "revision": 21,
                "paths": [],
                "subtrees": [],
                "truncated": true,
            })),
            None
        );
        assert_eq!(decode(serde_json::json!({ "revision": 22 })), None);
    }

    /// Every bulk metadata route asks the gateway for a BOUNDED content hash,
    /// and the point stat asks for an unbounded one.
    ///
    /// A full stat is the route open(2) resolves through, and it can only be
    /// served from a cached entry carrying a content hash (`read_bytes` matches
    /// cached bytes against it, a write chains its CAS base from it). While the
    /// bulk routes sent `max_hash_bytes=0`, everything they installed was
    /// incomplete and every open of a path they had already described still fell
    /// through to a point `/stat` — ~30 per measured `git status` phase.
    ///
    /// Both halves are the fix. Sending a budget is what makes the bulk answer
    /// complete; keeping it finite is what stops a metadata sweep over thousands
    /// of paths from becoming a full content read of the tree. The point stat
    /// keeps no budget at all: it owes its caller a hash at any size, and it is
    /// where an oversized file's open still goes.
    #[test]
    fn bulk_metadata_routes_request_a_bounded_content_hash() {
        // Finite and non-zero: zero is "hash nothing", which is what left every
        // bulk-seeded entry incomplete; unbounded would hash whatever a sweep
        // over thousands of paths happened to name.
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
                "/metadata-many" => {
                    axum::Json(serde_json::json!({ "entries": [null] })).into_response()
                }
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
            .block_on(client.metadata_many_attributes_versioned(&["dir/file".to_string()]))
            .unwrap();
        runtime
            .block_on(client.subtree_metadata_attributes_versioned("dir", 64))
            .unwrap();
        runtime.block_on(client.stat_versioned("dir/file")).unwrap();

        let expected = BULK_METADATA_MAX_HASH_BYTES.to_string();
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                ("/tree".to_string(), Some(expected.clone())),
                ("/metadata-many".to_string(), Some(expected.clone())),
                ("/subtree-metadata".to_string(), Some(expected)),
                ("/stat".to_string(), None),
            ]
        );
        server.abort();
    }

    #[test]
    fn revision_watch_attempt_timeout_exceeds_the_long_poll_window() {
        // The read-class budget must outlast a full long-poll so a held-open
        // watch is never cut short by the client's default mutation timeout.
        assert!(
            REVISION_WATCH_ATTEMPT_TIMEOUT
                > Duration::from_millis(REVISION_WATCH_TIMEOUT_MS)
        );
        assert!(REVISION_WATCH_BACKOFF_MAX > REVISION_WATCH_BACKOFF_MIN);
    }

    #[test]
    fn watch_health_warns_once_per_transition() {
        let mut health = WatchHealth::default();
        // Initial establishment is silent (no "restored" on first success).
        assert!(!health.on_success());
        assert!(!health.on_success());
        // The first failure warns once; further failures stay silent.
        assert!(health.on_failure());
        assert!(!health.on_failure());
        assert!(!health.on_failure());
        // Recovery warns exactly once, then stays silent.
        assert!(health.on_success());
        assert!(!health.on_success());
        // A fresh drop warns again.
        assert!(health.on_failure());
        assert!(!health.on_failure());
    }

    #[test]
    fn watch_health_warns_on_a_first_poll_failure() {
        // A watch that never establishes still surfaces one WARN.
        let mut health = WatchHealth::default();
        assert!(health.on_failure());
        assert!(!health.on_failure());
    }

    #[test]
    fn sibling_mounts_share_cache_coherence_without_retiring_on_publish() {
        let endpoint = format!("http://revision-test-{}", uuid::Uuid::new_v4());
        let first = RemoteVfsClient::new(&endpoint, "token-a", "scope").unwrap();
        let second = RemoteVfsClient::new(&endpoint, "token-b", "scope").unwrap();

        first.observe_published_revision(41);
        assert_eq!(first.coherence_revision(), 41);
        assert_eq!(second.coherence_revision(), 41);
        assert_eq!(first.observed_namespace_revision(), 0);
        assert_eq!(second.observed_namespace_revision(), 0);

        second.observe_read_revision(41);
        assert_eq!(first.observed_namespace_revision(), 41);
        assert_eq!(second.observed_namespace_revision(), 41);
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
    fn versioned_read_keeps_its_response_revision_when_shared_coherence_is_newer() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().route(
                    "/stat",
                    axum::routing::get(|| async {
                        let mut response = axum::Json(serde_json::json!({
                            "kind": "file",
                            "size_bytes": 1,
                            "file_id": "file-1",
                            "link_count": 1,
                            "link_target": null,
                            "content_hash": null,
                            "executable": false,
                            "mode": 420,
                            "updated_at": null
                        }))
                        .into_response();
                        response.headers_mut().insert(
                            header::HeaderName::from_static(
                                CHEVALIER_VFS_NAMESPACE_REVISION_HEADER,
                            ),
                            header::HeaderValue::from_static("41"),
                        );
                        response
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let client = RemoteVfsClient::new(&endpoint, "token", "").unwrap();
        client.observe_read_revision(99);

        let response = runtime
            .block_on(client.stat_attributes_versioned("file"))
            .unwrap();

        assert_eq!(response.revision, 41);
        assert_eq!(client.coherence_revision(), 99);
        assert_eq!(response.value.unwrap().file_id.as_deref(), Some("file-1"));
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

    #[test]
    fn watch_acks_only_after_the_fence_advance_and_cache_clear() {
        use std::collections::HashMap;

        use super::super::cache::{KernelInvalidator, PublicationInvalidation};

        // In-process probe: the stub gateway, the client's cache, and a kernel-
        // invalidation double all share this process, so the ack-poll handler can
        // inspect them directly to prove BOTH the cache clear AND the kernel sweep
        // preceded the ack. (revocation-ack ordering invariant, cache + kernel)
        struct AckProbe {
            poll_count: usize,
            ack_since: Option<u64>,
            ack_watcher_id: Option<String>,
            cache_cleared_before_ack: bool,
            kernel_invalidated_before_ack: bool,
        }

        // Stands in for a mounted session's kernel-invalidation hook: the real
        // fuser notifier needs a live /dev/fuse fd, so the invalidation is
        // factored through the `KernelInvalidator` trait the watch drives, which
        // this double captures. A remote publication reaches `invalidate_all`.
        //
        // The watch no longer calls this inline — it enqueues onto the
        // registry's invalidation worker and waits for that item to drain — so
        // the double takes its time before recording. A watch that acked on
        // enqueue rather than on drain would send the ack poll during this
        // delay, and the ack-time assertions below would see `fired == false`.
        const REVOCATION_WORK: Duration = Duration::from_millis(250);
        struct RecordingInvalidator {
            fired: Arc<AtomicBool>,
        }
        impl RecordingInvalidator {
            fn apply(&self) -> bool {
                std::thread::sleep(REVOCATION_WORK);
                self.fired.store(true, Ordering::Release);
                true
            }
        }
        impl KernelInvalidator for RecordingInvalidator {
            fn invalidate(&self, _: &PublicationInvalidation) -> bool {
                self.apply()
            }
            fn invalidate_all(&self) -> bool {
                self.apply()
            }
        }

        const BASELINE: u64 = 1_000;
        const PUBLISHED: u64 = 2_000;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());

        let cache = Arc::new(RemoteFuseCache::default());
        // Seed a directory listing fenced at BASELINE. The client's fence advance
        // + cache clear on PUBLISHED must evict it BEFORE the ack poll is sent.
        cache.put_dir("probe", Vec::new(), BASELINE);

        let probe = Arc::new(Mutex::new(AckProbe {
            poll_count: 0,
            ack_since: None,
            ack_watcher_id: None,
            cache_cleared_before_ack: false,
            kernel_invalidated_before_ack: false,
        }));
        let ready = Arc::new(tokio::sync::Notify::new());

        // Register the kernel-invalidation double so the watch's remote-
        // publication sweep reaches it. Its flag mirrors the cache-clear probe.
        let kernel_invalidated = Arc::new(AtomicBool::new(false));
        let invalidators = Arc::new(MountInvalidators::default());
        let double: Arc<dyn KernelInvalidator> = Arc::new(RecordingInvalidator {
            fired: Arc::clone(&kernel_invalidated),
        });
        invalidators.register(Arc::downgrade(&double));

        let server_cache = Arc::clone(&cache);
        let server_probe = Arc::clone(&probe);
        let server_ready = Arc::clone(&ready);
        let server_kernel_invalidated = Arc::clone(&kernel_invalidated);
        let server = runtime.spawn(async move {
            let app = axum::Router::new().route(
                "/watch",
                axum::routing::get(
                    move |axum::extract::Query(params): axum::extract::Query<
                        HashMap<String, String>,
                    >| {
                        let cache = Arc::clone(&server_cache);
                        let probe = Arc::clone(&server_probe);
                        let ready = Arc::clone(&server_ready);
                        let kernel_invalidated = Arc::clone(&server_kernel_invalidated);
                        async move {
                            let since = params
                                .get("since")
                                .and_then(|value| value.parse::<u64>().ok())
                                .unwrap_or(0);
                            let watcher_id = params.get("watcher_id").cloned().unwrap_or_default();
                            let count = {
                                let mut guard =
                                    probe.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                                guard.poll_count += 1;
                                guard.poll_count
                            };
                            if count == 1 {
                                // A sibling's publication lands on the first poll.
                                return axum::Json(serde_json::json!({ "revision": PUBLISHED }))
                                    .into_response();
                            }
                            if count == 2 {
                                // This poll is the ACK. The BASELINE-fenced entry
                                // must already be evicted by the client's clear,
                                // and the kernel double must already have been
                                // swept — both strictly before this ack is sent.
                                let cleared = cache.get_dir("probe", BASELINE).is_none();
                                let swept = kernel_invalidated.load(Ordering::Acquire);
                                let mut guard =
                                    probe.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                                guard.ack_since = Some(since);
                                guard.ack_watcher_id = Some(watcher_id);
                                guard.cache_cleared_before_ack = cleared;
                                guard.kernel_invalidated_before_ack = swept;
                                drop(guard);
                                ready.notify_one();
                            }
                            axum::http::StatusCode::NO_CONTENT.into_response()
                        }
                    },
                ),
            );
            axum::serve(listener, app).await.unwrap();
        });

        let client = RemoteVfsClient::new(&endpoint, "token", "scope").unwrap();
        client.ensure_revision_watch(runtime.handle(), &cache, &invalidators);

        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), ready.notified())
                .await
                .expect("client must issue the ack poll after applying the publication");
        });

        let guard = probe.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(
            guard.ack_since,
            Some(PUBLISHED),
            "the ack poll's since must equal the fence-advanced (published) revision"
        );
        assert!(
            guard
                .ack_watcher_id
                .as_deref()
                .is_some_and(|id| !id.is_empty()),
            "the ack poll must carry the stable watcher_id"
        );
        assert!(
            guard.cache_cleared_before_ack,
            "the cache clear MUST precede the ack poll so a writer never unblocks \
             while this mount can still serve stale reads"
        );
        assert!(
            guard.kernel_invalidated_before_ack,
            "the queued kernel revocation MUST have DRAINED before the ack poll: \
             enqueueing is not enough, or a writer unblocks while this mount's \
             kernel can still serve a stale attr lease"
        );
        drop(guard);
        drop(double);
        server.abort();
    }

    #[test]
    fn watch_withholds_the_ack_and_drops_liveness_when_the_revocation_fails() {
        use std::collections::HashMap;

        use super::super::cache::{KernelInvalidator, PublicationInvalidation};

        // Moving the revocation onto a worker thread must not soften the
        // fail-closed path: a revocation that does not land still means the
        // kernel may serve a superseded attr, so the watch drops liveness
        // (replies revert to TTL=0) and holds the ack until the leases granted
        // before the failure have expired on their own.
        struct FailingInvalidator {
            fired: Mutex<Option<std::sync::mpsc::Sender<()>>>,
        }
        impl FailingInvalidator {
            fn refuse(&self) -> bool {
                if let Some(fired) = self
                    .fired
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take()
                {
                    let _ = fired.send(());
                }
                false
            }
        }
        impl KernelInvalidator for FailingInvalidator {
            fn invalidate(&self, _: &PublicationInvalidation) -> bool {
                self.refuse()
            }
            fn invalidate_all(&self) -> bool {
                self.refuse()
            }
        }

        const PUBLISHED: u64 = 3_000;

        struct FailProbe {
            polls: usize,
            withheld_ack_since: Option<u64>,
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

        let cache = Arc::new(RemoteFuseCache::default());
        let probe = Arc::new(Mutex::new(FailProbe {
            polls: 0,
            withheld_ack_since: None,
        }));
        let acked = Arc::new(tokio::sync::Notify::new());

        let (fired_tx, fired_rx) = std::sync::mpsc::channel();
        let invalidators = Arc::new(MountInvalidators::default());
        let double: Arc<dyn KernelInvalidator> = Arc::new(FailingInvalidator {
            fired: Mutex::new(Some(fired_tx)),
        });
        invalidators.register(Arc::downgrade(&double));

        let server_probe = Arc::clone(&probe);
        let server_acked = Arc::clone(&acked);
        let server = runtime.spawn(async move {
            let app = axum::Router::new().route(
                "/watch",
                axum::routing::get(
                    move |axum::extract::Query(params): axum::extract::Query<
                        HashMap<String, String>,
                    >| {
                        let probe = Arc::clone(&server_probe);
                        let acked = Arc::clone(&server_acked);
                        async move {
                            let since = params
                                .get("since")
                                .and_then(|value| value.parse::<u64>().ok())
                                .unwrap_or(0);
                            let count = {
                                let mut guard =
                                    probe.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                                guard.polls += 1;
                                guard.polls
                            };
                            match count {
                                // Establish the watch so `watch_live` is true and
                                // the drop below is a real transition.
                                1 => axum::http::StatusCode::NO_CONTENT.into_response(),
                                // A remote publication whose revocation fails.
                                2 => axum::Json(serde_json::json!({ "revision": PUBLISHED }))
                                    .into_response(),
                                // The withheld ack, issuable only after the lease
                                // TTL has elapsed.
                                3 => {
                                    let mut guard = probe
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                                    guard.withheld_ack_since = Some(since);
                                    drop(guard);
                                    acked.notify_one();
                                    axum::http::StatusCode::NO_CONTENT.into_response()
                                }
                                _ => {
                                    std::future::pending::<()>().await;
                                    unreachable!()
                                }
                            }
                        }
                    },
                ),
            );
            axum::serve(listener, app).await.unwrap();
        });

        let client = RemoteVfsClient::new(&endpoint, "token", "scope").unwrap();
        client.ensure_revision_watch(runtime.handle(), &cache, &invalidators);

        fired_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the publication's revocation must reach the invalidation worker");

        // The failure must land as dropped liveness, not as a silent ack.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while client.revision_watch_live() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !client.revision_watch_live(),
            "a revocation that did not land must drop watch liveness so replies serve strict"
        );
        assert_eq!(
            probe
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .polls,
            2,
            "the ack poll must be withheld while a superseded lease may still be live"
        );

        // It is withheld, not abandoned: once the lease TTL has elapsed the ack
        // goes out carrying the fence-advanced revision.
        runtime.block_on(async {
            tokio::time::timeout(ATTR_ENTRY_LEASE_TTL * 5, acked.notified())
                .await
                .expect("the ack must follow once the pre-failure leases have expired");
        });
        assert_eq!(
            probe
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .withheld_ack_since,
            Some(PUBLISHED),
            "the withheld ack still carries the fence-advanced revision"
        );

        drop(double);
        server.abort();
    }

    #[test]
    fn watch_survives_a_single_transport_blip_without_flapping_live() {
        // Mirrors the ack stub test: an in-process raw-TCP stub drives the real
        // watch loop. A gateway that reaps our idle keep-alive socket surfaces
        // the reap as a send-class error on the NEXT poll. The transport retry
        // must absorb that single blip WITHOUT ever clearing watch_live (which
        // would resurface strict serves and emit the down/restored WARN pair).
        //
        // Sequence: poll #1 establishes (204 -> watch_live true); poll #2 is the
        // reaped-socket blip (connection dropped, no response -> a `send`
        // failure); poll #3 is the immediate retry. The stub snapshots watch_live
        // exactly when poll #3 arrives — still MID-retry, before its own success
        // is recorded. With the retry, the blip never touched watch_live, so the
        // snapshot is `true`. Without it, poll #2's failure would have cleared it
        // and the snapshot would be `false`. Deterministic: no timing, no WARN
        // capture. Every response sets `connection: close` so each poll lands on
        // its own fresh connection and the poll count is unambiguous.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        const RESP_204: &[u8] =
            b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());

        let cache = Arc::new(RemoteFuseCache::default());
        let client = RemoteVfsClient::new(&endpoint, "token", "scope").unwrap();

        // Records watch_live as observed by the stub at the recovery poll (#3).
        let live_at_recovery = Arc::new(Mutex::new(None::<bool>));
        let ready = Arc::new(tokio::sync::Notify::new());

        let server_client = client.clone();
        let server_live = Arc::clone(&live_at_recovery);
        let server_ready = Arc::clone(&ready);
        let server = runtime.spawn(async move {
            let mut poll = 0usize;
            loop {
                let (mut socket, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                // Drain the request head so the peer's `send` fully lands before
                // we choose how (or whether) to answer.
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                loop {
                    match socket.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            head.extend_from_slice(&buf[..n]);
                            if head.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                poll += 1;
                match poll {
                    1 => {
                        // Establish the watch: a live long-poll timeout.
                        let _ = socket.write_all(RESP_204).await;
                        let _ = socket.flush().await;
                    }
                    2 => {
                        // The reaped-socket blip: drop with no response, exactly
                        // as a gateway keepAliveTimeout close would.
                        drop(socket);
                    }
                    3 => {
                        // The immediate retry. Snapshot watch_live as seen right
                        // now — the retry must have kept it live.
                        let live = server_client.revision_watch_live();
                        *server_live.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
                            Some(live);
                        let _ = socket.write_all(RESP_204).await;
                        let _ = socket.flush().await;
                        server_ready.notify_one();
                    }
                    _ => {
                        // Hold subsequent long-polls open so the client neither
                        // errors nor busy-loops while the test asserts.
                        std::future::pending::<()>().await;
                    }
                }
            }
        });

        let invalidators = Arc::new(MountInvalidators::default());
        client.ensure_revision_watch(runtime.handle(), &cache, &invalidators);

        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), ready.notified())
                .await
                .expect("the watch must issue a recovery poll after the transport blip");
        });

        let observed = *live_at_recovery
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(
            observed,
            Some(true),
            "a single send-class blip must be absorbed by the immediate retry: \
             watch_live must never be cleared, so it is still live at the recovery poll"
        );
        assert!(
            client.revision_watch_live(),
            "the watch must remain live after the retry recovers the blip"
        );
        server.abort();
    }
}
