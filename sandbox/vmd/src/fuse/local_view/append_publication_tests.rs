//! End-to-end append publication: a real `MountLocalView`, its WAL, the
//! `MountPublisher` and `RemoteVfsClient`, against an in-process gateway that
//! implements exactly the append contract of `PUT /file`:
//!
//! * `x-chevalier-vfs-append-offset: N` -- the body is bytes `[N, N+len)`;
//! * a `content_fingerprint` precondition naming the base's content hash;
//! * `x-chevalier-vfs-expected-content-sha256` naming the whole result's hash;
//! * 409 when the base hash or size differs or the result hash does not match.
//!
//! Its "legacy" switch models a gateway that predates the contract: it ignores
//! the append header, treats the body as the whole file and therefore fails the
//! expected-hash check with 409, which must send vmd to its full-content
//! fallback.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use chevalier_sandbox::vfs::{
    CHEVALIER_VFS_LEASE_MODE_HEADER, CHEVALIER_VFS_LEASE_MODE_IMPLICIT,
    CHEVALIER_VFS_NAMESPACE_REVISION_HEADER, CHEVALIER_VFS_OPERATION_HEADER,
    CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER, CHEVALIER_VFS_PRECONDITION_KIND_HEADER,
    VfsCasPredicate, VfsNamespaceMutation, VfsNamespaceMutationBatchBody, VfsWritePrecondition,
};
use chevalier_vfs_hash::{ContentHasher, hash_bytes};

use super::mount::{MountLocalView, MountLocalViewOptions};
use super::publisher::{GenerationSource, MountPublisher, PublisherOptions};
use super::tree::MountFile;
use super::types::DrainOutcome;
use super::{MAX_SEGMENTED_PAYLOAD_BYTES, MountStateLayout};
use crate::fuse::client::{APPEND_OFFSET_HEADER, RemoteVfsClient};

const EXPECTED_CONTENT_HASH_HEADER: &str = "x-chevalier-vfs-expected-content-sha256";
const SCOPE: &str = "scope";
const DRAIN: Duration = Duration::from_secs(60);

/// One `PUT /file` as the gateway saw it.
#[derive(Clone, Debug)]
struct Put {
    path: String,
    operation: Option<String>,
    append_offset: Option<u64>,
    precondition_kind: Option<String>,
    body_bytes: u64,
    status: StatusCode,
}

/// A stored file and the hash state after its bytes, so verifying an append
/// costs the appended bytes, as it does in an append-aware store.
struct Stored {
    bytes: Vec<u8>,
    hasher: ContentHasher,
}

impl Stored {
    fn new(bytes: Vec<u8>) -> Self {
        let mut hasher = ContentHasher::new();
        hasher.update(&bytes);
        Self { bytes, hasher }
    }

    fn hash(&self) -> String {
        self.hasher.digest()
    }

    fn extended(&self, tail: &[u8]) -> Self {
        let mut hasher = match &self.hasher {
            ContentHasher::Sha256(state) => ContentHasher::Sha256(state.clone()),
            ContentHasher::Blake3(state) => ContentHasher::Blake3(state.clone()),
        };
        hasher.update(tail);
        let mut bytes = self.bytes.clone();
        bytes.extend_from_slice(tail);
        Self { bytes, hasher }
    }
}

#[derive(Default)]
struct Gateway {
    files: HashMap<String, Stored>,
    revision: u64,
    /// Ignore the append header, as a gateway that predates it would.
    legacy: bool,
    puts: Vec<Put>,
}

type SharedGateway = Arc<Mutex<Gateway>>;

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn with_revision(mut response: Response, revision: u64) -> Response {
    response.headers_mut().insert(
        CHEVALIER_VFS_NAMESPACE_REVISION_HEADER,
        HeaderValue::from_str(&revision.to_string()).expect("revision header"),
    );
    response
}

async fn serve_gateway(
    State(gateway): State<SharedGateway>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Response {
    let path = query.get("path").cloned().unwrap_or_default();
    let mut state = gateway.lock().unwrap();
    match (method, uri.path()) {
        (Method::POST, "/lease") => {
            let mut response = Json(serde_json::json!({
                "resource_key": "implicit:test",
                "owner_token": uuid::Uuid::nil(),
                "task_id": null
            }))
            .into_response();
            response.headers_mut().insert(
                CHEVALIER_VFS_LEASE_MODE_HEADER,
                HeaderValue::from_static(CHEVALIER_VFS_LEASE_MODE_IMPLICIT),
            );
            response
        }
        (Method::GET, "/stat") => {
            let Some(stored) = state.files.get(&path) else {
                return with_revision(StatusCode::NOT_FOUND.into_response(), state.revision);
            };
            with_revision(
                Json(serde_json::json!({
                    "kind": "file",
                    "size_bytes": stored.bytes.len(),
                    "file_id": format!("remote:{path}"),
                    "link_count": 1,
                    "link_target": null,
                    "content_hash": stored.hash(),
                    "executable": false,
                    "mode": 0o644,
                    "updated_at": null
                }))
                .into_response(),
                state.revision,
            )
        }
        (Method::POST, "/namespace-many") => {
            let batch: VfsNamespaceMutationBatchBody =
                serde_json::from_slice(&body).expect("decode namespace batch");
            for mutation in batch.mutations {
                match mutation {
                    VfsNamespaceMutation::CreateFile { path, .. } => {
                        state
                            .files
                            .entry(path)
                            .or_insert_with(|| Stored::new(Vec::new()));
                    }
                    VfsNamespaceMutation::Rename { from, to } => {
                        let Some(stored) = state.files.remove(&from) else {
                            return with_revision(
                                StatusCode::CONFLICT.into_response(),
                                state.revision,
                            );
                        };
                        state.files.insert(to, stored);
                    }
                    VfsNamespaceMutation::DeleteFile { path, .. } => {
                        state.files.remove(&path);
                    }
                    VfsNamespaceMutation::SetMode { .. } => {}
                    other => panic!("fake gateway does not model {other:?}"),
                }
            }
            state.revision += 1;
            with_revision(
                Json(serde_json::json!({ "entries": [] })).into_response(),
                state.revision,
            )
        }
        (Method::POST, "/write-many") => {
            #[derive(serde::Deserialize)]
            struct Item {
                path: String,
                body_base64: String,
                precondition: Option<VfsWritePrecondition>,
            }
            #[derive(serde::Deserialize)]
            struct Batch {
                writes: Vec<Item>,
            }
            let batch: Batch = serde_json::from_slice(&body).expect("decode write batch");
            for item in &batch.writes {
                let current = state.files.get(&item.path);
                let holds = match item
                    .precondition
                    .as_ref()
                    .and_then(|precondition| precondition.predicate.as_ref())
                {
                    None => true,
                    Some(VfsCasPredicate::Absent) => current.is_none(),
                    Some(VfsCasPredicate::ContentFingerprint { fingerprint }) => {
                        current.map(Stored::hash).as_ref() == Some(fingerprint)
                    }
                };
                if !holds {
                    return with_revision(StatusCode::CONFLICT.into_response(), state.revision);
                }
            }
            for item in batch.writes {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(item.body_base64)
                    .expect("base64 body");
                state.files.insert(item.path, Stored::new(bytes));
            }
            state.revision += 1;
            with_revision(
                Json(serde_json::json!({ "results": [], "entries": [] })).into_response(),
                state.revision,
            )
        }
        (Method::PUT, "/file") => {
            let append_offset = header(&headers, APPEND_OFFSET_HEADER)
                .map(|raw| raw.parse::<u64>().expect("decimal append offset"));
            let precondition_kind = header(&headers, CHEVALIER_VFS_PRECONDITION_KIND_HEADER);
            let fingerprint = header(&headers, CHEVALIER_VFS_PRECONDITION_FINGERPRINT_HEADER);
            let expected = header(&headers, EXPECTED_CONTENT_HASH_HEADER)
                .expect("streamed writes name their resulting hash");
            let current = state.files.get(&path);
            let precondition_holds = match precondition_kind.as_deref() {
                None => true,
                Some("absent") => current.is_none(),
                Some("content_fingerprint") => current.map(Stored::hash) == fingerprint,
                Some(other) => panic!("unexpected precondition kind {other}"),
            };
            let result = match (append_offset, state.legacy) {
                (Some(offset), false) => current
                    .filter(|base| base.bytes.len() as u64 == offset && precondition_holds)
                    .map(|base| base.extended(&body)),
                // A legacy gateway reads the body as the whole file.
                _ => precondition_holds.then(|| Stored::new(body.to_vec())),
            };
            let status = match result {
                Some(stored) if stored.hash() == expected => {
                    state.files.insert(path.clone(), stored);
                    state.revision += 1;
                    StatusCode::OK
                }
                _ => StatusCode::CONFLICT,
            };
            state.puts.push(Put {
                path: path.clone(),
                operation: header(&headers, CHEVALIER_VFS_OPERATION_HEADER),
                append_offset,
                precondition_kind,
                body_bytes: body.len() as u64,
                status,
            });
            if status != StatusCode::OK {
                return with_revision(status.into_response(), state.revision);
            }
            with_revision(
                Json(serde_json::json!({
                    "path": path,
                    "content_hash": expected,
                    "previous_hash": null,
                    "changed": true,
                    "entries": []
                }))
                .into_response(),
                state.revision,
            )
        }
        (method, route) => panic!("fake gateway does not serve {method} {route}"),
    }
}

struct Harness {
    view: Arc<MountLocalView>,
    gateway: SharedGateway,
    server: tokio::task::JoinHandle<()>,
    _state: tempfile::TempDir,
}

impl Harness {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gateway");
        let endpoint = format!("http://{}", listener.local_addr().expect("gateway address"));
        let gateway: SharedGateway = Arc::default();
        let router = axum::Router::new()
            .route("/{*path}", axum::routing::any(serve_gateway))
            .layer(axum::extract::DefaultBodyLimit::max(256 << 20))
            .with_state(Arc::clone(&gateway));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve gateway");
        });

        let state = tempfile::tempdir().expect("tempdir");
        let view = MountLocalView::open(MountLocalViewOptions {
            layout: MountStateLayout::new(state.path()),
            scope_path: SCOPE.to_string(),
            endpoint: endpoint.clone(),
            mount_tag: "append-e2e".to_string(),
            read_only: false,
            tokio: tokio::runtime::Handle::current(),
        })
        .expect("open local view")
        .view;
        let weak = Arc::downgrade(&view);
        let options = PublisherOptions::defaults("test").with_generations(GenerationSource::new(
            move |path, size, content_hash| {
                weak.upgrade()
                    .expect("view alive")
                    .snapshot_generation(path, size, content_hash)
            },
        ));
        let publisher = MountPublisher::spawn(
            RemoteVfsClient::new(&endpoint, "token", SCOPE).expect("client"),
            view.wal().expect("writable WAL").clone(),
            tokio::runtime::Handle::current(),
            options,
        );
        view.attach_publisher(publisher).expect("attach publisher");
        Self {
            view,
            gateway,
            server,
            _state: state,
        }
    }

    async fn drain(&self) {
        let outcome = self.view.drain(DRAIN).await.expect("drain");
        assert!(
            matches!(outcome, DrainOutcome::Drained { .. }),
            "publication did not drain: {outcome:?}"
        );
    }

    fn puts(&self) -> Vec<Put> {
        self.gateway.lock().unwrap().puts.clone()
    }

    /// The remote bytes and the gateway's (incrementally built) hash of them.
    fn remote(&self, path: &str) -> Option<(Vec<u8>, String)> {
        self.gateway
            .lock()
            .unwrap()
            .files
            .get(&format!("{SCOPE}/{path}"))
            .map(|stored| (stored.bytes.clone(), stored.hash()))
    }

    fn local(&self, path: &str) -> Vec<u8> {
        std::fs::read(self.view.tree().resolve(path).expect("resolve")).expect("read backing")
    }

    /// The gateway holds exactly the guest's bytes. It accepted every append
    /// only because the result hash vmd claimed matched its own.
    fn assert_converged(&self, path: &str) {
        let local = self.local(path);
        let (remote, _) = self.remote(path).expect("remote file");
        assert_eq!(remote.len(), local.len(), "{path}: remote size");
        assert!(
            remote == local,
            "{path}: remote bytes differ from the mount"
        );
    }

    /// The gateway's incrementally extended hash equals a one-shot hash of the
    /// same bytes: incremental and whole-file hashing agree.
    fn assert_hash_agrees(&self, path: &str) {
        let (remote, hash) = self.remote(path).expect("remote file");
        assert_eq!(hash, hash_bytes(&remote));
    }

    fn diverge(&self, path: &str) {
        let (mut bytes, _) = self.remote(path).expect("remote file");
        bytes[17] ^= 0xff;
        self.gateway
            .lock()
            .unwrap()
            .files
            .insert(format!("{SCOPE}/{path}"), Stored::new(bytes));
    }

    async fn stop(self) {
        self.view.shutdown(DRAIN).await.expect("shutdown");
        self.server.abort();
    }
}

fn pattern(length: usize, seed: u8) -> Vec<u8> {
    (0..length)
        .map(|index| (index as u8).wrapping_mul(131).wrapping_add(seed) ^ (index >> 12) as u8)
        .collect()
}

fn append(view: &MountLocalView, file: &MountFile, bytes: &[u8]) {
    let end = file.metadata().expect("stat").size_bytes;
    view.write(file, bytes, end).expect("append");
    view.flush_handle(file).expect("close-time seal");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_to_a_large_file_upload_only_their_new_bytes_and_rejections_fall_back_to_full_content()
 {
    let harness = Harness::start().await;
    let view = &harness.view;

    // A 20 MiB log, sealed and published whole once.
    let initial = 20 * 1024 * 1024 + 1;
    let (file, _) = view
        .create_file("pilot.log", 0o644, libc::O_RDWR)
        .expect("create log");
    for (index, chunk) in pattern(initial, 1)
        .chunks(MAX_SEGMENTED_PAYLOAD_BYTES)
        .enumerate()
    {
        view.write(&file, chunk, (index * MAX_SEGMENTED_PAYLOAD_BYTES) as u64)
            .expect("write log");
    }
    view.flush_handle(&file).expect("seal log");
    harness.drain().await;
    let puts = harness.puts();
    assert_eq!(puts.len(), 1, "{puts:?}");
    assert_eq!(puts[0].path, format!("{SCOPE}/pilot.log"));
    assert_eq!(puts[0].append_offset, None);
    assert_eq!(puts[0].body_bytes, initial as u64);
    // `absent` when the creation folded into this write; nothing when the
    // publisher already sent the creation while the guest was still writing.
    assert!(matches!(
        puts[0].precondition_kind.as_deref(),
        None | Some("absent")
    ));
    harness.assert_converged("pilot.log");

    // Each close of an appended log uploads exactly the appended bytes.
    for round in 0..5_usize {
        let before = harness.local("pilot.log").len() as u64;
        let tail = pattern(1_000 * (round + 1) + 13, round as u8 + 2);
        append(view, &file, &tail);
        harness.drain().await;
        let put = harness.puts().last().cloned().expect("append upload");
        assert_eq!(put.append_offset, Some(before), "round {round}: {put:?}");
        assert_eq!(put.body_bytes, tail.len() as u64, "round {round}");
        assert_eq!(put.operation.as_deref(), Some("vfs_stream_append"));
        assert_eq!(
            put.precondition_kind.as_deref(),
            Some("content_fingerprint")
        );
        assert_eq!(put.status, StatusCode::OK);
        harness.assert_converged("pilot.log");
    }

    // A burst of closes before the publisher catches up: still only the new
    // bytes, and consecutive appends may share one request.
    let burst_start = harness.puts().len();
    let mut burst_bytes = 0_u64;
    for round in 0..10_usize {
        let tail = pattern(700 + round * 37, round as u8 + 40);
        burst_bytes += tail.len() as u64;
        append(view, &file, &tail);
    }
    harness.drain().await;
    let burst: Vec<Put> = harness.puts()[burst_start..].to_vec();
    assert!(!burst.is_empty() && burst.len() <= 10, "{burst:?}");
    assert!(
        burst
            .iter()
            .all(|put| put.append_offset.is_some() && put.status == StatusCode::OK),
        "{burst:?}"
    );
    assert_eq!(
        burst.iter().map(|put| put.body_bytes).sum::<u64>(),
        burst_bytes
    );
    harness.assert_converged("pilot.log");

    // A gateway that does not understand appends rejects the request; vmd
    // never retries it, it publishes the generation's full content instead.
    harness.gateway.lock().unwrap().legacy = true;
    let fallback_start = harness.puts().len();
    let before = harness.local("pilot.log").len() as u64;
    append(view, &file, &pattern(4_096, 77));
    harness.drain().await;
    let fallback: Vec<Put> = harness.puts()[fallback_start..].to_vec();
    let [rejected, full] = fallback.as_slice() else {
        panic!("expected one rejected append and one full upload: {fallback:?}");
    };
    assert_eq!(rejected.append_offset, Some(before));
    assert_eq!(rejected.body_bytes, 4_096);
    assert_eq!(rejected.status, StatusCode::CONFLICT);
    assert_eq!(full.append_offset, None);
    assert_eq!(full.precondition_kind, None);
    assert_eq!(full.operation.as_deref(), Some("vfs_stream_write"));
    assert_eq!(full.body_bytes, before + 4_096);
    assert_eq!(full.status, StatusCode::OK);
    harness.assert_converged("pilot.log");

    // Once the gateway understands appends again, the next append extends the
    // full content the fallback published.
    harness.gateway.lock().unwrap().legacy = false;
    let before = harness.local("pilot.log").len() as u64;
    append(view, &file, b"back to appends\n");
    harness.drain().await;
    let put = harness.puts().last().cloned().expect("append upload");
    assert_eq!(put.append_offset, Some(before));
    assert_eq!(put.body_bytes, b"back to appends\n".len() as u64);
    assert_eq!(put.status, StatusCode::OK);
    harness.assert_converged("pilot.log");

    // A replica that diverged underneath the mount fails the base
    // precondition; the full-content fallback repairs it.
    harness.diverge("pilot.log");
    let diverged_start = harness.puts().len();
    append(view, &file, b"after divergence\n");
    harness.drain().await;
    let repaired: Vec<Put> = harness.puts()[diverged_start..].to_vec();
    assert_eq!(repaired.len(), 2, "{repaired:?}");
    assert_eq!(repaired[0].status, StatusCode::CONFLICT);
    assert_eq!(repaired[1].append_offset, None);
    assert_eq!(repaired[1].status, StatusCode::OK);
    harness.assert_converged("pilot.log");
    harness.assert_hash_agrees("pilot.log");

    // Over the whole run, the appends cost their own bytes, not 20 MiB each:
    // one initial upload and two full-content fallbacks.
    let uploaded: u64 = harness.puts().iter().map(|put| put.body_bytes).sum();
    let file_size = harness.local("pilot.log").len() as u64;
    assert!(
        uploaded < file_size * 4,
        "uploaded {uploaded} bytes for a {file_size}-byte log"
    );

    harness.stop().await;
}

/// A legacy gateway rejects an append whose log was rotated away before the
/// publisher reached it: the fallback finds the generation's bytes under the
/// name the rename moved them to, publishes them at the append's own path, and
/// the replayed rename then carries them to their final name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_append_is_recovered_from_where_a_later_rename_moved_its_bytes() {
    let harness = Harness::start().await;
    let view = &harness.view;
    let (file, _) = view
        .create_file("app.log", 0o644, libc::O_RDWR)
        .expect("create log");
    view.write(&file, &pattern(3 * MAX_SEGMENTED_PAYLOAD_BYTES, 9), 0)
        .expect("write log");
    view.flush_handle(&file).expect("seal log");
    harness.drain().await;

    harness.gateway.lock().unwrap().legacy = true;
    append(view, &file, &pattern(10_000, 10));
    view.rename("app.log", "app.log.1", 0).expect("rotate");
    file.retarget("app.log.1");
    drop(file);
    let (fresh, _) = view
        .create_file("app.log", 0o644, libc::O_RDWR)
        .expect("create the new log");
    view.write(&fresh, b"new log\n", 0).expect("write new log");
    view.flush_handle(&fresh).expect("seal new log");
    harness.drain().await;

    let rejected = harness
        .puts()
        .iter()
        .filter(|put| put.status == StatusCode::CONFLICT)
        .count();
    assert!(
        rejected >= 1,
        "the legacy gateway must have rejected the append"
    );
    harness.assert_converged("app.log.1");
    harness.assert_converged("app.log");
    harness.stop().await;
}

/// A legacy gateway rejects an append that a later accepted whole-file
/// generation already replaced: nothing can or need be recovered, the later
/// generation determines the replica, and publication still converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_append_superseded_by_a_rewrite_converges_on_the_rewrite() {
    let harness = Harness::start().await;
    let view = &harness.view;
    let (file, _) = view
        .create_file("state.log", 0o644, libc::O_RDWR)
        .expect("create log");
    view.write(&file, &pattern(2 * MAX_SEGMENTED_PAYLOAD_BYTES, 20), 0)
        .expect("write log");
    view.flush_handle(&file).expect("seal log");
    harness.drain().await;

    harness.gateway.lock().unwrap().legacy = true;
    append(view, &file, &pattern(5_000, 21));
    // A chmod is an ordering boundary, so the rewrite cannot supersede the
    // append inside one publish batch: the append is sent and rejected first.
    view.set_mode("state.log", 0o600).expect("chmod");
    view.truncate(&file, 0).expect("truncate");
    view.write(&file, &pattern(2 * MAX_SEGMENTED_PAYLOAD_BYTES + 5, 22), 0)
        .expect("rewrite");
    view.flush_handle(&file).expect("seal rewrite");
    harness.drain().await;

    // The append went out once at most: a refusal is never retried.
    let appends = harness
        .puts()
        .iter()
        .filter(|put| put.append_offset.is_some())
        .count();
    assert!(appends <= 1, "{:?}", harness.puts());
    harness.assert_converged("state.log");
    harness.stop().await;
}
