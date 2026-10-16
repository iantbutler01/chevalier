//! Spec 86 D: a read-only mount following the change feed, against an in-process gateway that
//! serves `/tree`, `/stat`, `/file/raw` and a `/changes` stream the test drives, and counts
//! every request it gets.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use chevalier_vfs_hash::hash_bytes;
use tokio::sync::broadcast;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::BroadcastStream;

use super::change_stream::{
    ChangeCursor, ChangeStreamFollower, ChangeStreamFollowerHandle, FeedHead, FollowControl,
    load_cursor, read_feed_head, store_cursor,
};
use super::client::RemoteVfsClient;
use super::computer_mounts::{ComputerMounts, MountSpawner};
use super::handle::hydrate_or_reconcile;
use super::local_view::MountStateLayout;
use super::local_view::hydrate::{KernelPathInvalidator, MountHydrator};
use super::local_view::mount::{MountLocalView, MountLocalViewOptions};

const EPOCH: &str = "11111111-2222-3333-4444-555555555555";

struct Remote {
    files: Mutex<BTreeMap<String, Vec<u8>>>,
    requests: Mutex<Vec<String>>,
    raw_reads: AtomicUsize,
    fail_stat: AtomicBool,
    stat_failures: AtomicUsize,
    file_ids: Mutex<HashMap<String, String>>,
    frames: broadcast::Sender<String>,
    head: Mutex<i64>,
    applied: Mutex<Vec<i64>>,
    serves_changes: bool,
    /// `/changes` answers 502 this many more times (the gateway unreachable, as during an
    /// API restart).
    failing_opens: AtomicUsize,
    /// `/changes` answers 200 and closes at once this many more times (a gateway whose
    /// follow loop fails right after the open).
    closing_opens: AtomicUsize,
}

fn listing(
    files: &BTreeMap<String, Vec<u8>>,
    ids: &HashMap<String, String>,
    dir: &str,
) -> Option<Vec<serde_json::Value>> {
    let prefix = if dir.is_empty() {
        String::new()
    } else {
        format!("{dir}/")
    };
    let mut entries: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (path, bytes) in files {
        let Some(rest) = path.strip_prefix(&prefix) else {
            continue;
        };
        match rest.split_once('/') {
            Some((child, _)) => {
                entries.insert(child.to_string(), serde_json::json!({
                    "name": child, "kind": "directory", "size_bytes": 0, "content_hash": null, "mode": 0o755
                }));
            }
            None => {
                entries.insert(rest.to_string(), serde_json::json!({
                    "name": rest, "kind": "file", "size_bytes": bytes.len(), "file_id": ids.get(path).cloned().unwrap_or_else(|| format!("id:{path}")),
                    "content_hash": hash_bytes(bytes), "mode": 0o644
                }));
            }
        }
    }
    (dir.is_empty() || !entries.is_empty()).then(|| entries.into_values().collect())
}

async fn serve(
    State(remote): State<Arc<Remote>>,
    method: Method,
    uri: Uri,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    remote
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {}", uri.path()));
    let path = query.get("path").cloned().unwrap_or_default();
    match (method, uri.path()) {
        (Method::GET, "/changes")
            if remote
                .failing_opens
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok() =>
        {
            StatusCode::BAD_GATEWAY.into_response()
        }
        (Method::GET, "/changes")
            if remote
                .closing_opens
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok() =>
        {
            Response::new(Body::empty())
        }
        (Method::GET, "/changes") if remote.serves_changes => {
            let mut opening = Vec::new();
            if !query.contains_key("after") {
                let head = *remote.head.lock().unwrap();
                opening.push(format!(
                    "{{\"t\":\"caught_up\",\"epoch\":\"{EPOCH}\",\"head\":{head}}}\n"
                ));
            }
            let live = BroadcastStream::new(remote.frames.subscribe())
                .filter_map(|frame| frame.ok().map(|frame| format!("{frame}\n")));
            let stream = tokio_stream::iter(opening)
                .chain(live)
                .map(Ok::<_, std::convert::Infallible>);
            Response::new(Body::from_stream(stream))
        }
        (Method::POST, "/changes/applied") => StatusCode::NO_CONTENT.into_response(),
        (Method::GET, "/tree") => match listing(
            &remote.files.lock().unwrap(),
            &remote.file_ids.lock().unwrap(),
            &path,
        ) {
            Some(entries) => Json(entries).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        (Method::GET, "/file/raw") => {
            remote.raw_reads.fetch_add(1, Ordering::SeqCst);
            match remote.files.lock().unwrap().get(&path) {
                Some(bytes) => bytes.clone().into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }
        (Method::GET, "/stat") => {
            if remote.fail_stat.load(Ordering::SeqCst) {
                remote.stat_failures.fetch_add(1, Ordering::SeqCst);
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            let files = remote.files.lock().unwrap();
            if let Some(bytes) = files.get(&path) {
                return Json(serde_json::json!({
                    "kind": "file", "size_bytes": bytes.len(), "file_id": remote.file_ids.lock().unwrap().get(&path).cloned().unwrap_or_else(|| format!("id:{path}")),
                    "link_count": 1, "link_target": null, "content_hash": hash_bytes(bytes),
                    "executable": false, "mode": 0o644, "updated_at": null
                }))
                .into_response();
            }
            if listing(&files, &remote.file_ids.lock().unwrap(), &path).is_some() {
                return Json(serde_json::json!({
                    "kind": "directory", "size_bytes": 0, "file_id": null, "link_count": 1,
                    "link_target": null, "content_hash": null, "executable": true, "mode": 0o755,
                    "updated_at": null
                }))
                .into_response();
            }
            StatusCode::NOT_FOUND.into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

/// `applied` reports arrive as JSON bodies, which `serve` does not parse; this records them.
async fn record_applied(
    State(remote): State<Arc<Remote>>,
    Json(body): Json<serde_json::Value>,
) -> StatusCode {
    remote
        .requests
        .lock()
        .unwrap()
        .push("POST /changes/applied".into());
    if let Some(seq) = body.get("seq").and_then(|seq| seq.as_i64()) {
        remote.applied.lock().unwrap().push(seq);
    }
    StatusCode::NO_CONTENT
}

struct Gateway {
    remote: Arc<Remote>,
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
}

impl Gateway {
    async fn start(files: &[(&str, &str)], serves_changes: bool) -> Self {
        let remote = Arc::new(Remote {
            files: Mutex::new(
                files
                    .iter()
                    .map(|(path, bytes)| (path.to_string(), bytes.as_bytes().to_vec()))
                    .collect(),
            ),
            requests: Mutex::default(),
            raw_reads: AtomicUsize::new(0),
            fail_stat: AtomicBool::new(false),
            stat_failures: AtomicUsize::new(0),
            file_ids: Mutex::default(),
            frames: broadcast::channel(64).0,
            head: Mutex::new(0),
            applied: Mutex::default(),
            serves_changes,
            failing_opens: AtomicUsize::new(0),
            closing_opens: AtomicUsize::new(0),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gateway");
        let endpoint = format!("http://{}", listener.local_addr().expect("gateway address"));
        let router = axum::Router::new()
            .route("/changes/applied", axum::routing::post(record_applied))
            .route("/{*path}", axum::routing::any(serve))
            .with_state(Arc::clone(&remote));
        let server =
            tokio::spawn(
                async move { axum::serve(listener, router).await.expect("serve gateway") },
            );
        Self {
            remote,
            endpoint,
            server,
        }
    }

    /// Change a file on the gateway and push the event for it, as a commit would.
    fn commit_put(&self, seq: i64, path: &str, bytes: &str) {
        self.remote
            .files
            .lock()
            .unwrap()
            .insert(path.to_string(), bytes.as_bytes().to_vec());
        *self.remote.head.lock().unwrap() = seq;
        self.push(serde_json::json!({"t": "changes", "head": seq, "events": [{
            "seq": seq, "path": path, "op": "put", "kind": "file", "size": bytes.len(),
            "hash": hash_bytes(bytes.as_bytes()), "mode": 0o644, "origin": "api", "at_ms": 0
        }]}));
    }

    fn commit_delete(&self, seq: i64, path: &str) {
        self.remote.files.lock().unwrap().remove(path);
        *self.remote.head.lock().unwrap() = seq;
        self.push(serde_json::json!({"t": "changes", "head": seq, "events": [{
            "seq": seq, "path": path, "op": "delete", "at_ms": 0
        }]}));
    }

    fn push(&self, frame: serde_json::Value) {
        let _ = self.remote.frames.send(frame.to_string());
    }

    fn raw_reads(&self) -> usize {
        self.remote.raw_reads.load(Ordering::SeqCst)
    }

    fn requests(&self) -> Vec<String> {
        self.remote.requests.lock().unwrap().clone()
    }

    fn applied(&self) -> Vec<i64> {
        self.remote.applied.lock().unwrap().clone()
    }

    fn client(&self) -> RemoteVfsClient {
        RemoteVfsClient::new(&self.endpoint, "token", "").expect("client")
    }
}

#[derive(Default)]
struct RecordingInvalidator {
    paths: Mutex<Vec<String>>,
}

impl KernelPathInvalidator for RecordingInvalidator {
    fn invalidate_paths(&self, paths: &[String]) {
        self.paths.lock().unwrap().extend(paths.iter().cloned());
    }
    fn invalidate_subtrees(&self, subtrees: &[String]) {
        self.paths.lock().unwrap().extend(subtrees.iter().cloned());
    }
    fn invalidate_all(&self) {
        self.paths.lock().unwrap().push("*".into());
    }
}

/// A hydrated read-only view following the feed, as the daemon sets one up.
struct Mount {
    _view: Arc<MountLocalView>,
    hydrator: Arc<MountHydrator>,
    invalidator: Arc<RecordingInvalidator>,
    control: Arc<FollowControl>,
    follower: Option<ChangeStreamFollowerHandle>,
}

impl Mount {
    async fn open(gateway: &Gateway, state: &std::path::Path) -> Self {
        let opened = MountLocalView::open(MountLocalViewOptions {
            layout: MountStateLayout::new(state),
            scope_path: String::new(),
            endpoint: gateway.endpoint.clone(),
            mount_tag: "nymfs".to_string(),
            read_only: true,
            tokio: tokio::runtime::Handle::current(),
        })
        .expect("open read-only view");
        let hydrator = MountHydrator::shared(gateway.client(), Arc::clone(&opened.view));
        hydrator.follow_change_feed();
        let marker = MountStateLayout::new(state).hydrated_marker_path();
        hydrate_or_reconcile(&hydrator, &marker, true)
            .await
            .expect("hydrate");
        hydrator.fill_all().await;
        Self {
            _view: opened.view,
            hydrator,
            invalidator: Arc::default(),
            control: FollowControl::new(),
            follower: None,
        }
    }

    fn follow(
        &mut self,
        gateway: &Gateway,
        state: &std::path::Path,
        computer_mounts: Option<Arc<ComputerMounts>>,
    ) {
        let (mounted, signal) = tokio::sync::oneshot::channel();
        let invalidator: Arc<dyn KernelPathInvalidator> = self.invalidator.clone();
        self.follower = Some(ChangeStreamFollower::spawn(
            gateway.client(),
            Arc::clone(&self.hydrator),
            invalidator,
            state.to_path_buf(),
            Arc::clone(&self.control),
            computer_mounts,
            signal,
        ));
        mounted.send(()).expect("follower waiting");
    }
}

fn local(state: &std::path::Path, path: &str) -> Option<String> {
    std::fs::read_to_string(MountStateLayout::new(state).tree_dir().join(path)).ok()
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..200 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Waits until the follower has its stream open (the gateway saw the request).
async fn stream_open(gateway: &Gateway, opens: usize) {
    eventually("the change stream to open", || {
        gateway
            .requests()
            .iter()
            .filter(|request| *request == "GET /changes")
            .count()
            >= opens
    })
    .await;
    // The handler has subscribed by the time it answers; give the body a moment to start.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_followed_change_reaches_the_mount_and_advances_the_cursor() {
    let gateway = Gateway::start(&[("notes/a.md", "one"), ("notes/b.md", "two")], true).await;
    let state = tempfile::tempdir().expect("state");
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 0,
        },
    )
    .unwrap();
    mount.follow(&gateway, state.path(), None);
    stream_open(&gateway, 1).await;
    let reads_after_hydrate = gateway.raw_reads();

    gateway.commit_put(1, "notes/a.md", "one, rewritten");
    gateway.commit_put(2, "memory/new.md", "fresh");
    gateway.commit_delete(3, "notes/b.md");
    eventually("the changes to apply", || {
        local(state.path(), "notes/a.md").as_deref() == Some("one, rewritten")
            && local(state.path(), "memory/new.md").as_deref() == Some("fresh")
            && local(state.path(), "notes/b.md").is_none()
    })
    .await;
    eventually("the applied reports", || gateway.applied().ends_with(&[3])).await;

    assert_eq!(
        gateway.raw_reads() - reads_after_hydrate,
        2,
        "two files fetched, the delete is local"
    );
    assert_eq!(load_cursor(state.path()).map(|cursor| cursor.seq), Some(3));
    assert_eq!(mount.control.applied(), 3);
    assert!(
        mount
            .control
            .wait_applied(3, Duration::from_millis(10))
            .await
    );
    let invalidated = mount.invalidator.paths.lock().unwrap().clone();
    for path in ["notes/a.md", "memory/new.md", "notes/b.md"] {
        assert!(
            invalidated.iter().any(|seen| seen == path),
            "{path} revoked"
        );
    }
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_the_mount_already_reflects_fetches_nothing() {
    let gateway = Gateway::start(&[("work/out.txt", "same bytes")], true).await;
    let state = tempfile::tempdir().expect("state");
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 0,
        },
    )
    .unwrap();
    mount.follow(&gateway, state.path(), None);
    stream_open(&gateway, 1).await;
    let reads = gateway.raw_reads();
    let requests = gateway.requests().len();

    // The guest's own write coming back: same content.
    gateway.commit_put(1, "work/out.txt", "same bytes");
    // The report follows the local advance; wait for it so the request list is complete.
    eventually("the applied report", || gateway.applied() == vec![1]).await;

    assert_eq!(
        gateway.raw_reads(),
        reads,
        "no content re-fetch for an echo"
    );
    let since: Vec<String> = gateway.requests()[requests..].to_vec();
    assert_eq!(
        since,
        vec!["POST /changes/applied".to_string()],
        "no stat, no read"
    );
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_follower_makes_no_requests() {
    let gateway = Gateway::start(&[("a.md", "a")], true).await;
    let state = tempfile::tempdir().expect("state");
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 0,
        },
    )
    .unwrap();
    mount.follow(&gateway, state.path(), None);
    stream_open(&gateway, 1).await;
    let requests = gateway.requests().len();

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        gateway.requests().len(),
        requests,
        "an idle mount holds its one stream and sends nothing (the poll made one request a second)"
    );
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn resync_walks_the_scope_and_continues_from_the_sent_head() {
    let gateway = Gateway::start(&[("keep.md", "k"), ("gone.md", "g")], true).await;
    let state = tempfile::tempdir().expect("state");
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 0,
        },
    )
    .unwrap();
    mount.follow(&gateway, state.path(), None);
    stream_open(&gateway, 1).await;

    // Changes the feed no longer has, then the gateway's answer to the stale cursor.
    gateway.remote.files.lock().unwrap().remove("gone.md");
    gateway
        .remote
        .files
        .lock()
        .unwrap()
        .insert("added.md".into(), b"added".to_vec());
    gateway.push(
        serde_json::json!({"t": "resync", "epoch": EPOCH, "head": 40, "reason": "below_floor"}),
    );

    eventually("the resync walk", || {
        local(state.path(), "gone.md").is_none()
            && local(state.path(), "added.md").as_deref() == Some("added")
    })
    .await;
    eventually("the cursor at the resync head", || {
        load_cursor(state.path()).is_some_and(|cursor| cursor.seq == 40)
    })
    .await;
    // The follower reopens from the new cursor.
    stream_open(&gateway, 2).await;
    gateway.commit_put(41, "keep.md", "k2");
    eventually("streaming to resume", || {
        local(state.path(), "keep.md").as_deref() == Some("k2")
    })
    .await;
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_cursor_the_follower_walks_then_streams_from_the_head() {
    let gateway = Gateway::start(&[("a.md", "a")], true).await;
    *gateway.remote.head.lock().unwrap() = 7;
    let state = tempfile::tempdir().expect("state");
    let mut mount = Mount::open(&gateway, state.path()).await;
    mount.follow(&gateway, state.path(), None);
    eventually("a cursor from the head", || {
        load_cursor(state.path()).is_some_and(|cursor| cursor.seq == 7 && cursor.epoch == EPOCH)
    })
    .await;
    gateway.commit_put(8, "a.md", "a2");
    eventually("the next change", || {
        local(state.path(), "a.md").as_deref() == Some("a2")
    })
    .await;
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnect_request_reopens_the_stream_from_the_cursor() {
    let gateway = Gateway::start(&[("a.md", "a")], true).await;
    let state = tempfile::tempdir().expect("state");
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 5,
        },
    )
    .unwrap();
    mount.follow(&gateway, state.path(), None);
    stream_open(&gateway, 1).await;
    mount.control.request_reconnect();
    stream_open(&gateway, 2).await;
    gateway.commit_put(6, "a.md", "after reconnect");
    eventually("delivery on the new stream", || {
        local(state.path(), "a.md").as_deref() == Some("after reconnect")
    })
    .await;
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnect_request_cuts_a_failure_backoff_short() {
    // After a gateway outage the follower sleeps up to 30 s between attempts; the API's
    // pre-command poke must not wait that out.
    let gateway = Gateway::start(&[("a.md", "a")], true).await;
    gateway.remote.failing_opens.store(2, Ordering::SeqCst);
    let state = tempfile::tempdir().expect("state");
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 5,
        },
    )
    .unwrap();
    mount.follow(&gateway, state.path(), None);
    // Two failures: the follower now waits about a second before the third attempt.
    stream_open(&gateway, 2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let poked = std::time::Instant::now();
    mount.control.request_reconnect();
    stream_open(&gateway, 3).await;
    let reopened = poked.elapsed();
    assert!(
        reopened < Duration::from_millis(400),
        "the poke reopened the stream after {reopened:?}"
    );
    gateway.commit_put(6, "a.md", "after the outage");
    eventually("delivery on the reopened stream", || {
        local(state.path(), "a.md").as_deref() == Some("after the outage")
    })
    .await;
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_head_is_read_before_the_walk_and_an_old_gateway_means_polling() {
    let gateway = Gateway::start(&[], true).await;
    *gateway.remote.head.lock().unwrap() = 12;
    match read_feed_head(&gateway.client()).await {
        FeedHead::At(cursor) => assert_eq!(
            cursor,
            ChangeCursor {
                epoch: EPOCH.into(),
                seq: 12
            }
        ),
        _ => panic!("expected the head"),
    }
    let old = Gateway::start(&[], false).await;
    assert!(matches!(
        read_feed_head(&old.client()).await,
        FeedHead::Unsupported
    ));
    gateway.server.abort();
    old.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn computer_mount_links_follow_the_manifest_and_survive_a_walk() {
    let manifest = |links: serde_json::Value| {
        serde_json::json!({"version": 1, "mounts": [], "links": links}).to_string()
    };
    let gateway = Gateway::start(
        &[
            ("vm/mounts/task/.keep", ""),
            ("conversations/c/0001_assistant/mount/one.txt", "1"),
            ("conversations/c/0002_assistant/mount/two.txt", "2"),
            (".runtime/skills/pdf/v1/SKILL.md", "skill"),
        ],
        true,
    )
    .await;
    let state = tempfile::tempdir().expect("state");
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 0,
        },
    )
    .unwrap();
    let invalidator: Arc<dyn KernelPathInvalidator> = mount.invalidator.clone();
    let mounts = ComputerMounts::new(
        MountSpawner {
            endpoint: gateway.endpoint.clone(),
            credential: None,
            token: Some("token".into()),
            owner: None,
            state_root: state.path().join("bindings"),
            root_mountpoint: state.path().join("never-mounted"),
        },
        ".runtime/computer/mounts.json",
        Arc::clone(&mount.hydrator),
        invalidator,
    );
    mount.follow(&gateway, state.path(), Some(Arc::clone(&mounts)));
    stream_open(&gateway, 1).await;
    let link = |path: &str| {
        std::fs::read_link(MountStateLayout::new(state.path()).tree_dir().join(path))
            .ok()
            .map(|target| target.display().to_string())
    };

    gateway.commit_put(
        1,
        ".runtime/computer/mounts.json",
        &manifest(serde_json::json!({
            "vm/mounts/task": "conversations/c/0001_assistant/mount",
            "vm/mounts/skills": ".runtime/skills"
        })),
    );
    eventually("the task link", || {
        link("vm/mounts/task").as_deref() == Some("../../conversations/c/0001_assistant/mount")
    })
    .await;
    assert_eq!(
        link("vm/mounts/skills").as_deref(),
        Some("../../.runtime/skills")
    );

    // A full walk keeps the links (the gateway still lists vm/mounts/task as a directory).
    gateway.push(serde_json::json!({"t": "resync", "epoch": EPOCH, "head": 1, "reason": "epoch"}));
    stream_open(&gateway, 2).await;
    assert_eq!(
        link("vm/mounts/task").as_deref(),
        Some("../../conversations/c/0001_assistant/mount")
    );

    // A task switch repoints the link; no remount.
    gateway.commit_put(
        2,
        ".runtime/computer/mounts.json",
        &manifest(serde_json::json!({
            "vm/mounts/task": "conversations/c/0002_assistant/mount",
            "vm/mounts/skills": ".runtime/skills"
        })),
    );
    eventually("the switched task link", || {
        link("vm/mounts/task").as_deref() == Some("../../conversations/c/0002_assistant/mount")
    })
    .await;

    // No current task: the link goes and the gateway's directory comes back.
    gateway.commit_put(
        3,
        ".runtime/computer/mounts.json",
        &manifest(serde_json::json!({"vm/mounts/skills": ".runtime/skills"})),
    );
    eventually("the gateway's directory back in place of the link", || {
        link("vm/mounts/task").is_none()
            && MountStateLayout::new(state.path())
                .tree_dir()
                .join("vm/mounts/task/.keep")
                .exists()
    })
    .await;
    assert_eq!(
        link("vm/mounts/skills").as_deref(),
        Some("../../.runtime/skills")
    );
    gateway.server.abort();
}

fn change_opens(gateway: &Gateway) -> usize {
    gateway
        .requests()
        .iter()
        .filter(|request| request.as_str() == "GET /changes")
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_fails_right_after_every_open_backs_off_instead_of_storming() {
    // Spec 86 review 35bac974: opening alone must not reset the backoff, or a failure right
    // after each open (the gateway closing at once, a poison batch, a failing resync walk)
    // retries every 0.5 s forever.
    let gateway = Gateway::start(&[("a.md", "a")], true).await;
    gateway.remote.closing_opens.store(1_000, Ordering::SeqCst);
    let state = tempfile::tempdir().expect("state");
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 0,
        },
    )
    .unwrap();
    mount.follow(&gateway, state.path(), None);
    tokio::time::sleep(Duration::from_millis(3_200)).await;
    let opens = change_opens(&gateway);
    // Doubling from 0.5 s: about 0, 0.5, 1.5, 3.5 s, so 3 opens in 3.2 s (jitter allows 4).
    // A fixed 0.5 s backoff would be about 7.
    assert!(
        (2..=4).contains(&opens),
        "{opens} stream opens in 3.2 s; the backoff is not growing"
    );
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mode_change_with_the_same_content_reaches_the_mount() {
    // Spec 86 review ad95480c: a chmod row has the same size and hash; it must still apply.
    use std::os::unix::fs::PermissionsExt;
    let gateway = Gateway::start(&[("bin/run.sh", "#!/bin/sh\necho hi\n")], true).await;
    let state = tempfile::tempdir().expect("state");
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 0,
        },
    )
    .unwrap();
    mount.follow(&gateway, state.path(), None);
    stream_open(&gateway, 1).await;
    let reads = gateway.raw_reads();
    let bytes = "#!/bin/sh\necho hi\n";
    *gateway.remote.head.lock().unwrap() = 1;
    gateway.push(serde_json::json!({"t": "changes", "head": 1, "events": [{
        "seq": 1, "path": "bin/run.sh", "op": "put", "kind": "file", "size": bytes.len(),
        "hash": hash_bytes(bytes.as_bytes()), "mode": 0o755, "origin": "api", "at_ms": 0
    }]}));
    let backing = MountStateLayout::new(state.path())
        .tree_dir()
        .join("bin/run.sh");
    eventually("the mode change", || {
        std::fs::metadata(&backing)
            .map(|meta| meta.permissions().mode() & 0o777 == 0o755)
            .unwrap_or(false)
    })
    .await;
    assert_eq!(
        gateway.raw_reads(),
        reads,
        "a mode change fetches no content"
    );
    assert!(
        mount
            .invalidator
            .paths
            .lock()
            .unwrap()
            .iter()
            .any(|path| path == "bin/run.sh"),
        "the mode change revokes the kernel's cached attributes"
    );
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_change_never_advances_the_cursor_and_recovers_without_restart() {
    let gateway = Gateway::start(&[("a", "old")], true).await;
    let state = tempfile::tempdir().unwrap();
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 0,
        },
    )
    .unwrap();
    mount.follow(&gateway, state.path(), None);
    stream_open(&gateway, 1).await;
    gateway.remote.fail_stat.store(true, Ordering::SeqCst);
    for _ in 0..200 {
        mount.control.request_reconnect();
        tokio::time::sleep(Duration::from_millis(20)).await;
        gateway.commit_put(1, "a", "new");
        if gateway.remote.stat_failures.load(Ordering::SeqCst) >= 6 {
            break;
        }
    }
    assert!(gateway.remote.stat_failures.load(Ordering::SeqCst) >= 6);
    assert_eq!(load_cursor(state.path()).unwrap().seq, 0);
    assert_eq!(mount.control.applied(), 0);
    assert_eq!(local(state.path(), "a").as_deref(), Some("old"));
    assert!(!gateway.applied().contains(&1));
    gateway.remote.fail_stat.store(false, Ordering::SeqCst);
    for _ in 0..100 {
        mount.control.request_reconnect();
        tokio::time::sleep(Duration::from_millis(20)).await;
        gateway.commit_put(1, "a", "new");
        if mount.control.applied() == 1 {
            break;
        }
    }
    eventually("the retried change", || mount.control.applied() == 1).await;
    assert_eq!(local(state.path(), "a").as_deref(), Some("new"));
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_hard_links_join_existing_inodes() {
    use std::os::unix::fs::MetadataExt;
    let gateway = Gateway::start(&[("a", "shared")], true).await;
    let state = tempfile::tempdir().unwrap();
    let mount = Mount::open(&gateway, state.path()).await;
    let tree = MountStateLayout::new(state.path()).tree_dir();
    for alias in ["b", "0-first"] {
        gateway
            .remote
            .files
            .lock()
            .unwrap()
            .insert(alias.into(), b"shared".to_vec());
        gateway
            .remote
            .file_ids
            .lock()
            .unwrap()
            .insert(alias.into(), "id:a".into());
        mount
            .hydrator
            .rehydrate_paths(&[alias.into()], &[])
            .await
            .unwrap();
        let original = std::fs::metadata(tree.join("a")).unwrap();
        assert_eq!(
            original.ino(),
            std::fs::metadata(tree.join(alias)).unwrap().ino()
        );
    }
    let a = std::fs::metadata(tree.join("a")).unwrap();
    assert_eq!(a.nlink(), 3);
    assert_eq!(a.ino(), std::fs::metadata(tree.join("b")).unwrap().ino());
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_root_reposts_its_applied_cursor_after_reconnect() {
    let gateway = Gateway::start(&[("a", "shared")], true).await;
    let state = tempfile::tempdir().unwrap();
    let mut mount = Mount::open(&gateway, state.path()).await;
    store_cursor(
        state.path(),
        &ChangeCursor {
            epoch: EPOCH.into(),
            seq: 7,
        },
    )
    .unwrap();
    mount.control = FollowControl::local_only();
    mount.follow(&gateway, state.path(), None);
    stream_open(&gateway, 1).await;
    assert!(gateway.applied().is_empty());
    drop(mount.follower.take());
    mount.control = FollowControl::new();
    mount.follow(&gateway, state.path(), None);
    stream_open(&gateway, 2).await;
    eventually("root cursor acknowledgment", || {
        gateway.applied().contains(&7)
    })
    .await;
    gateway.server.abort();
}
