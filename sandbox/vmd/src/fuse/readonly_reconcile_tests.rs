//! A read-only mount restarted over a complete hydrate fetches only what changed, against a
//! fake gateway that serves `/tree`, `/file/raw` and `/stat` and counts raw reads.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use chevalier_vfs_hash::hash_bytes;

use super::client::RemoteVfsClient;
use super::handle::hydrate_or_reconcile;
use super::local_view::hydrate::MountHydrator;
use super::local_view::mount::{MountLocalView, MountLocalViewOptions};
use super::local_view::MountStateLayout;

#[derive(Default)]
struct Remote {
    files: Mutex<BTreeMap<String, Vec<u8>>>,
    raw_reads: AtomicUsize,
}

fn listing(files: &BTreeMap<String, Vec<u8>>, dir: &str) -> Option<Vec<serde_json::Value>> {
    let prefix = if dir.is_empty() { String::new() } else { format!("{dir}/") };
    let mut entries: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (path, bytes) in files {
        let Some(rest) = path.strip_prefix(&prefix) else { continue };
        match rest.split_once('/') {
            Some((child, _)) => {
                entries.insert(child.to_string(), serde_json::json!({
                    "name": child, "kind": "directory", "size_bytes": 0, "content_hash": null, "mode": 0o755
                }));
            }
            None => {
                entries.insert(rest.to_string(), serde_json::json!({
                    "name": rest, "kind": "file", "size_bytes": bytes.len(), "file_id": format!("id:{path}"),
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
    let path = query.get("path").cloned().unwrap_or_default();
    let files = remote.files.lock().unwrap();
    match (method, uri.path()) {
        (Method::GET, "/tree") => match listing(&files, &path) {
            Some(entries) => Json(entries).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        (Method::GET, "/file/raw") => {
            remote.raw_reads.fetch_add(1, Ordering::SeqCst);
            match files.get(&path) {
                Some(bytes) => bytes.clone().into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }
        (Method::GET, "/stat") => match files.get(&path) {
            Some(bytes) => Json(serde_json::json!({
                "kind": "file", "size_bytes": bytes.len(), "file_id": format!("id:{path}"), "link_count": 1,
                "link_target": null, "content_hash": hash_bytes(bytes), "executable": false, "mode": 0o644,
                "updated_at": null
            }))
            .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        // The pack accelerator is optional; hydration fetches whatever it does not deliver.
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

struct Gateway {
    remote: Arc<Remote>,
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
}

impl Gateway {
    async fn start(files: &[(&str, &str)]) -> Self {
        let remote = Arc::new(Remote::default());
        remote.files.lock().unwrap().extend(files.iter().map(|(p, b)| (p.to_string(), b.as_bytes().to_vec())));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind gateway");
        let endpoint = format!("http://{}", listener.local_addr().expect("gateway address"));
        let router = axum::Router::new()
            .route("/{*path}", axum::routing::any(serve))
            .with_state(Arc::clone(&remote));
        let server = tokio::spawn(async move { axum::serve(listener, router).await.expect("serve gateway") });
        Self { remote, endpoint, server }
    }

    fn set(&self, path: &str, bytes: &str) {
        self.remote.files.lock().unwrap().insert(path.to_string(), bytes.as_bytes().to_vec());
    }

    fn remove(&self, path: &str) {
        self.remote.files.lock().unwrap().remove(path);
    }

    fn raw_reads(&self) -> usize {
        self.remote.raw_reads.load(Ordering::SeqCst)
    }

    /// Open the read-only view over `state`, as a mount daemon start does, and hydrate or
    /// reconcile it. The view is dropped on return so the next start can take ownership.
    async fn start_mount(&self, state: &std::path::Path) {
        let opened = MountLocalView::open(MountLocalViewOptions {
            layout: MountStateLayout::new(state),
            scope_path: String::new(),
            endpoint: self.endpoint.clone(),
            mount_tag: "nymfs".to_string(),
            read_only: true,
            tokio: tokio::runtime::Handle::current(),
        })
        .expect("open read-only view");
        assert!(opened.needs_hydration, "a read-only view always asks to be brought current");
        let client = RemoteVfsClient::new(&self.endpoint, "token", "").expect("client");
        let hydrator = MountHydrator::new(client, Arc::clone(&opened.view));
        let marker = MountStateLayout::new(state).hydrated_marker_path();
        hydrate_or_reconcile(&hydrator, &marker, true).await.expect("hydrate or reconcile");
    }
}

fn local(state: &std::path::Path, path: &str) -> Option<String> {
    std::fs::read_to_string(MountStateLayout::new(state).tree_dir().join(path)).ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_over_a_complete_hydrate_fetches_only_what_changed() {
    let gateway = Gateway::start(&[
        ("conversations/a/0001_user.md", "hello"),
        ("conversations/a/0002_assistant.md", "hi there"),
        ("notes/todo.md", "buy milk"),
    ])
    .await;
    let state = tempfile::tempdir().expect("state dir");

    gateway.start_mount(state.path()).await;
    assert_eq!(gateway.raw_reads(), 3, "the first start hydrates every file");
    assert!(MountStateLayout::new(state.path()).hydrated_marker_path().exists());

    gateway.set("conversations/a/0002_assistant.md", "hi there, rewritten");
    gateway.remove("notes/todo.md");
    gateway.set("conversations/a/0003_user.md", "new turn");

    gateway.start_mount(state.path()).await;
    assert_eq!(gateway.raw_reads(), 5, "the restart fetches only the rewritten and the new file");
    assert_eq!(local(state.path(), "conversations/a/0001_user.md").as_deref(), Some("hello"));
    assert_eq!(local(state.path(), "conversations/a/0002_assistant.md").as_deref(), Some("hi there, rewritten"));
    assert_eq!(local(state.path(), "conversations/a/0003_user.md").as_deref(), Some("new turn"));
    assert_eq!(local(state.path(), "notes/todo.md"), None, "a file the gateway dropped is pruned");

    gateway.start_mount(state.path()).await;
    assert_eq!(gateway.raw_reads(), 5, "a restart with nothing changed downloads nothing");
    gateway.server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_populated_tree_without_the_marker_hydrates_from_empty() {
    let gateway = Gateway::start(&[("a.md", "one"), ("b.md", "two")]).await;
    let state = tempfile::tempdir().expect("state dir");

    gateway.start_mount(state.path()).await;
    std::fs::remove_file(MountStateLayout::new(state.path()).hydrated_marker_path()).expect("drop marker");
    gateway.start_mount(state.path()).await;

    assert_eq!(gateway.raw_reads(), 4, "an unmarked tree is not trusted and is downloaded again");
    assert_eq!(local(state.path(), "b.md").as_deref(), Some("two"));
    gateway.server.abort();
}
