//! Spec 86: a NymFS mount is ready once its namespace is, and file bytes follow. Runs the
//! real hydrator and mount-local view against a fake gateway that answers `/tree`,
//! `/file/raw` and `/stat` after a per-request delay (a guest's round trip to the API) and
//! counts content reads.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use chevalier_vfs_hash::hash_bytes;

use super::client::RemoteVfsClient;
use super::local_view::MountStateLayout;
use super::local_view::hydrate::{HydrationOutcome, MountHydrator};
use super::local_view::lazy::{placeholder_at, root_has_pending};
use super::local_view::mount::{MountLocalView, MountLocalViewOptions};

#[derive(Default)]
struct Remote {
    files: Mutex<BTreeMap<String, Vec<u8>>>,
    raw_reads: AtomicUsize,
    requests: AtomicUsize,
    latency_us: AtomicU64,
    /// Content hashes the gateway has on record, as the real one stores them, so a listing
    /// costs the fake no hashing.
    hashes: Mutex<HashMap<String, String>>,
    /// Once the first ranged read of this path has been answered, the file is rewritten to
    /// these bytes (a writer committing mid-fill).
    rewrite_after_first_range: Mutex<Option<(String, Vec<u8>)>>,
}

impl Remote {
    fn replace(&self, path: &str, bytes: Vec<u8>) {
        self.hashes
            .lock()
            .unwrap()
            .insert(path.to_string(), hash_bytes(&bytes));
        self.files.lock().unwrap().insert(path.to_string(), bytes);
    }
}

/// A ranged `/file/raw` answer, held to the fingerprint header as the real gateway does.
fn ranged(remote: &Remote, path: &str, range: &str, fingerprint: Option<&str>) -> Response {
    let Some(bytes) = remote.files.lock().unwrap().get(path).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if fingerprint.is_some_and(|fingerprint| fingerprint != remote.content_hash(path, &bytes)) {
        return StatusCode::PRECONDITION_FAILED.into_response();
    }
    let (start, end) = range
        .trim_start_matches("bytes=")
        .split_once('-')
        .map(|(start, end)| (start.parse::<usize>().unwrap(), end.parse::<usize>().unwrap()))
        .unwrap();
    let slice = bytes
        .get(start.min(bytes.len())..(end + 1).min(bytes.len()))
        .unwrap_or_default()
        .to_vec();
    let mut rewrite = remote.rewrite_after_first_range.lock().unwrap();
    if rewrite.as_ref().is_some_and(|(target, _)| target == path)
        && let Some((target, replacement)) = rewrite.take()
    {
        remote.replace(&target, replacement);
    }
    (StatusCode::PARTIAL_CONTENT, slice).into_response()
}

impl Remote {
    fn content_hash(&self, path: &str, bytes: &[u8]) -> String {
        self.hashes
            .lock()
            .unwrap()
            .entry(path.to_string())
            .or_insert_with(|| hash_bytes(bytes))
            .clone()
    }
}

fn listing(
    remote: &Remote,
    files: &BTreeMap<String, Vec<u8>>,
    dir: &str,
) -> Option<Vec<serde_json::Value>> {
    let prefix = if dir.is_empty() {
        String::new()
    } else {
        format!("{dir}/")
    };
    let mut entries: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (path, bytes) in files.range(prefix.clone()..) {
        let Some(rest) = path.strip_prefix(&prefix) else {
            break;
        };
        match rest.split_once('/') {
            Some((child, _)) => {
                entries.entry(child.to_string()).or_insert_with(|| {
                    serde_json::json!({
                        "name": child, "kind": "directory", "size_bytes": 0,
                        "content_hash": null, "mode": 0o755
                    })
                });
            }
            None => {
                let mode = if rest.ends_with(".sh") { 0o755 } else { 0o644 };
                entries.insert(rest.to_string(), serde_json::json!({
                    "name": rest, "kind": "file", "size_bytes": bytes.len(),
                    "file_id": format!("id:{path}"), "content_hash": remote.content_hash(path, bytes),
                    "mode": mode
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
    headers: axum::http::HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    remote.requests.fetch_add(1, Ordering::SeqCst);
    let latency = remote.latency_us.load(Ordering::SeqCst);
    if latency > 0 {
        tokio::time::sleep(Duration::from_micros(latency)).await;
    }
    let path = query.get("path").cloned().unwrap_or_default();
    if method == Method::GET
        && uri.path() == "/file/raw"
        && let Some(range) = headers.get("range").and_then(|value| value.to_str().ok())
    {
        remote.raw_reads.fetch_add(1, Ordering::SeqCst);
        let fingerprint = headers
            .get(super::client::RANGE_FINGERPRINT_HEADER)
            .and_then(|value| value.to_str().ok());
        return ranged(&remote, &path, range, fingerprint);
    }
    let files = remote.files.lock().unwrap();
    match (method, uri.path()) {
        (Method::GET, "/tree") => match listing(&remote, &files, &path) {
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
                "kind": "file", "size_bytes": bytes.len(), "file_id": format!("id:{path}"),
                "link_count": 1, "link_target": null, "content_hash": remote.content_hash(&path, bytes),
                "executable": false, "mode": 0o644, "updated_at": null
            }))
            .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        // No pack accelerator: every byte the mount gets is one counted raw read.
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

struct Gateway {
    remote: Arc<Remote>,
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
}

impl Gateway {
    async fn start(files: BTreeMap<String, Vec<u8>>) -> Self {
        let remote = Arc::new(Remote::default());
        for (path, bytes) in &files {
            remote.content_hash(path, bytes);
        }
        *remote.files.lock().unwrap() = files;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gateway");
        let endpoint = format!("http://{}", listener.local_addr().expect("gateway address"));
        let router = axum::Router::new()
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

    fn set_latency(&self, latency: Duration) {
        self.remote
            .latency_us
            .store(latency.as_micros() as u64, Ordering::SeqCst);
    }

    fn raw_reads(&self) -> usize {
        self.remote.raw_reads.load(Ordering::SeqCst)
    }

    fn bytes(&self, path: &str) -> Vec<u8> {
        self.remote.files.lock().unwrap()[path].clone()
    }

    fn client(&self) -> RemoteVfsClient {
        RemoteVfsClient::new(&self.endpoint, "token", "").expect("client")
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn open_view(state: &Path, endpoint: &str, read_only: bool) -> super::local_view::mount::MountOpen {
    MountLocalView::open(MountLocalViewOptions {
        layout: MountStateLayout::new(state),
        scope_path: String::new(),
        endpoint: endpoint.to_string(),
        mount_tag: "nymfs".to_string(),
        read_only,
        tokio: tokio::runtime::Handle::current(),
    })
    .expect("open the mount-local view")
}

/// A Nym root shaped like a real one: conversations, notes, an app with a dependency tree,
/// mostly small files and a few large ones. Deterministic.
fn nym_root(files: usize) -> BTreeMap<String, Vec<u8>> {
    let mut tree = BTreeMap::new();
    let mut seed: u64 = 0x5eed;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for index in 0..files {
        let roll = next();
        let path = match index % 4 {
            0 => format!("conversations/c{:03}/{:04}_turn.md", index % 97, index),
            1 => format!(
                "notes/{}/note-{index}.md",
                ["work", "home", "ideas"][index % 3]
            ),
            2 => format!(
                "apps/calories/node_modules/pkg{:03}/lib/m{index}.js",
                index % 211
            ),
            _ => format!("apps/calories/src/{:02}/f{index}.sh", index % 23),
        };
        let size = if index % 997 == 0 {
            2 * 1024 * 1024 + (roll % 4096) as usize
        } else {
            200 + (roll % 12_000) as usize
        };
        let fill = format!("{path}:{roll}\n").into_bytes();
        let bytes: Vec<u8> = fill.iter().copied().cycle().take(size).collect();
        tree.insert(path, bytes);
    }
    tree
}

async fn hydrate(hydrator: &MountHydrator) -> (HydrationOutcome, Duration) {
    let started = Instant::now();
    let outcome = hydrator.hydrate().await.expect("hydrate");
    (outcome, started.elapsed())
}

/// Open and read a file through the mount-local view, as a FUSE `open` + `read` does (on a
/// thread of its own: the open blocks until the bytes are there).
async fn read_through_view(view: &Arc<MountLocalView>, path: &str) -> std::io::Result<Vec<u8>> {
    let view = Arc::clone(view);
    let path = path.to_string();
    tokio::task::spawn_blocking(move || {
        let (file, metadata) = view
            .open_file(&path, libc::O_RDONLY)
            .map_err(|error| std::io::Error::other(format!("{error:#}")))?;
        let mut bytes = vec![0u8; metadata.size_bytes as usize];
        let mut read = 0;
        while read < bytes.len() {
            let count = view
                .read(&file, &mut bytes[read..], read as u64)
                .map_err(|error| std::io::Error::other(format!("{error:#}")))?;
            if count == 0 {
                break;
            }
            read += count;
        }
        bytes.truncate(read);
        Ok(bytes)
    })
    .await
    .expect("join the read")
}

fn backing(state: &Path, path: &str) -> std::path::PathBuf {
    MountStateLayout::new(state).tree_dir().join(path)
}

/// The mount is ready after the namespace walk alone: every name, size and mode is there and
/// not one byte was downloaded. The first open of a file fetches it ahead of the rest; the
/// background fill then brings every other file, verified, with the times the guest saw.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_is_ready_with_its_namespace_and_bytes_follow() {
    let files = nym_root(2_000);
    let gateway = Gateway::start(files.clone()).await;
    gateway.set_latency(Duration::from_millis(2));
    let state = tempfile::tempdir().expect("state dir");
    let opened = open_view(state.path(), &gateway.endpoint, false);
    let hydrator = MountHydrator::shared(gateway.client(), Arc::clone(&opened.view));

    let (outcome, ready) = hydrate(&hydrator).await;
    assert_eq!(
        gateway.raw_reads(),
        0,
        "no byte is fetched before the mount is ready"
    );
    assert_eq!(outcome.deferred_files, files.len() as u64);
    for (path, bytes) in &files {
        let local = opened
            .view
            .tree()
            .lstat(path)
            .unwrap()
            .expect("every name exists");
        assert_eq!(
            local.size_bytes,
            bytes.len() as u64,
            "{path} has its listed size"
        );
        let expected_mode = if path.ends_with(".sh") { 0o755 } else { 0o644 };
        assert_eq!(
            local.mode & 0o7777,
            expected_mode,
            "{path} has its listed mode"
        );
    }
    let sample = "apps/calories/src/03/f3.sh";
    let mtime_before = opened.view.tree().lstat(sample).unwrap().unwrap().mtime;

    let opened_at = Instant::now();
    let bytes = read_through_view(&opened.view, sample).await.expect("read");
    let first_open = opened_at.elapsed();
    assert_eq!(bytes, files[sample], "the first open reads the real bytes");
    assert_eq!(gateway.raw_reads(), 1, "and fetched only that file");
    assert!(
        placeholder_at(&backing(state.path(), sample))
            .unwrap()
            .is_none()
    );

    let filled_at = Instant::now();
    hydrator.fill_all().await;
    let filled = filled_at.elapsed();
    assert_eq!(opened.view.lazy().outstanding(), 0);
    for (path, bytes) in &files {
        let local = std::fs::read(backing(state.path(), path)).unwrap();
        assert!(local == *bytes, "{path} holds its remote bytes");
        assert!(
            placeholder_at(&backing(state.path(), path))
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(
        gateway.raw_reads(),
        files.len(),
        "each file was fetched once"
    );
    assert_eq!(
        opened.view.tree().lstat(sample).unwrap().unwrap().mtime,
        mtime_before,
        "filling keeps the times the guest saw"
    );
    assert!(
        !root_has_pending(&MountStateLayout::new(state.path()).tree_dir()),
        "the tree is no longer marked as holding placeholders"
    );
    eprintln!(
        "spec86 lazy mount: {} files ready in {ready:?}; first open {first_open:?}; background fill {filled:?}",
        files.len()
    );
}

/// Mount-ready time at Crow's scale (16.5k files) with a 5 ms round trip to the gateway:
/// staged against the eager hydrate it replaces. The fake has no `prefetch-subtree` packs, so
/// the eager side downloads file by file; its absolute time is pessimistic, the staged one is
/// a debug build's (the printed breakdown says where it goes).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mount_ready_time_at_crow_scale_staged_versus_eager() {
    let files = nym_root(16_500);
    let total_bytes: usize = files.values().map(Vec::len).sum();
    let gateway = Gateway::start(files.clone()).await;
    gateway.set_latency(Duration::from_millis(5));

    let lazy_state = tempfile::tempdir().expect("state dir");
    let lazy_view = open_view(lazy_state.path(), &gateway.endpoint, false);
    let lazy_hydrator = MountHydrator::shared(gateway.client(), Arc::clone(&lazy_view.view));
    let (lazy_outcome, lazy_ready) = hydrate(&lazy_hydrator).await;
    let requests_lazy = gateway.remote.requests.load(Ordering::SeqCst);
    assert_eq!(gateway.raw_reads(), 0, "ready before any file's bytes");
    let opened_at = Instant::now();
    let path = "notes/ideas/note-5.md";
    assert_eq!(
        read_through_view(&lazy_view.view, path).await.unwrap(),
        gateway.bytes(path)
    );
    let first_open = opened_at.elapsed();

    let eager_state = tempfile::tempdir().expect("state dir");
    let eager_view = open_view(eager_state.path(), &gateway.endpoint, false);
    let eager_hydrator =
        MountHydrator::shared_eager(gateway.client(), Arc::clone(&eager_view.view));
    let (eager_outcome, eager_ready) = hydrate(&eager_hydrator).await;

    eprintln!(
        "spec86 mount-ready at {} files / {} MiB, 5 ms gateway round trip: staged {lazy_ready:?} \
         (walk {} ms over {} directories / {} requests, placeholders {} ms, sync {} ms; first \
         open {first_open:?}) vs eager {eager_ready:?} (walk {} ms, download {} ms, sync {} ms)",
        files.len(),
        total_bytes / (1024 * 1024),
        lazy_outcome.namespace_ms,
        lazy_outcome.directories,
        requests_lazy,
        lazy_outcome.content_ms,
        lazy_outcome.sync_ms,
        eager_outcome.namespace_ms,
        eager_outcome.content_ms,
        eager_outcome.sync_ms,
    );
    assert_eq!(lazy_outcome.deferred_files, files.len() as u64);
    assert_eq!(eager_outcome.files, files.len() as u64);
    assert!(
        lazy_ready * 4 < eager_ready,
        "the staged mount is ready far sooner: {lazy_ready:?} vs {eager_ready:?}"
    );
}

/// What a writable mount's guest does to files whose bytes have not arrived: an unlink
/// fetches nothing; a rename, a hard link, a write-open and a `touch` fetch first, so the
/// bytes kept (and later published) are the real ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutations_of_placeholders_fetch_first_or_not_at_all() {
    let files: BTreeMap<String, Vec<u8>> = [
        ("a/unlinked.txt", "gone"),
        ("a/renamed.txt", "moved bytes"),
        ("a/linked.txt", "linked bytes"),
        ("a/appended.txt", "head;"),
        ("a/touched.txt", "touched bytes"),
        ("dir/one.txt", "one"),
        ("dir/sub/two.txt", "two"),
    ]
    .into_iter()
    .map(|(path, bytes)| (path.to_string(), bytes.as_bytes().to_vec()))
    .collect();
    let gateway = Gateway::start(files.clone()).await;
    let state = tempfile::tempdir().expect("state dir");
    let opened = open_view(state.path(), &gateway.endpoint, false);
    let view = Arc::clone(&opened.view);
    let hydrator = MountHydrator::shared(gateway.client(), Arc::clone(&view));
    hydrate(&hydrator).await;

    let mutate = {
        let view = Arc::clone(&view);
        tokio::task::spawn_blocking(move || {
            view.remove_file("a/unlinked.txt").expect("unlink");
            let after_unlink = view.lazy().outstanding();
            view.rename("a/renamed.txt", "b-renamed.txt", 0)
                .expect("rename");
            view.create_hard_link("a/linked.txt", "a/alias.txt")
                .expect("link");
            let (file, _) = view
                .open_file("a/appended.txt", libc::O_WRONLY)
                .expect("open for write");
            view.write(&file, b"tail", 5).expect("write");
            view.flush_handle(&file).expect("close");
            view.set_times("a/touched.txt", None, None).expect("touch");
            view.rename("dir", "moved-dir", 0)
                .expect("rename a directory");
            after_unlink
        })
    };
    let after_unlink = mutate.await.unwrap();
    assert_eq!(
        after_unlink,
        files.len() - 1,
        "the unlink dropped its placeholder"
    );
    assert_eq!(
        gateway.raw_reads(),
        6,
        "everything but the unlinked file was fetched first"
    );
    let local = |path: &str| std::fs::read(backing(state.path(), path)).unwrap();
    assert_eq!(local("b-renamed.txt"), b"moved bytes");
    assert_eq!(local("a/alias.txt"), b"linked bytes");
    assert_eq!(local("a/appended.txt"), b"head;tail");
    assert_eq!(local("a/touched.txt"), b"touched bytes");
    assert_eq!(local("moved-dir/sub/two.txt"), b"two");
    assert!(!backing(state.path(), "a/unlinked.txt").exists());
    assert_eq!(view.lazy().outstanding(), 0);
}

/// A writable mount restarted before its fill finished keeps its tree (it is the authority
/// for its scope) and picks its placeholders up again; a file it fetched before the restart
/// is never fetched over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_mount_resumes_its_placeholders() {
    let files = nym_root(300);
    let gateway = Gateway::start(files.clone()).await;
    let state = tempfile::tempdir().expect("state dir");
    let fetched = "notes/home/note-1.md";
    {
        let opened = open_view(state.path(), &gateway.endpoint, false);
        let hydrator = MountHydrator::shared(gateway.client(), Arc::clone(&opened.view));
        hydrate(&hydrator).await;
        read_through_view(&opened.view, fetched).await.unwrap();
        let view = Arc::clone(&opened.view);
        tokio::task::spawn_blocking(move || {
            let (file, _) = view
                .open_file(fetched, libc::O_WRONLY | libc::O_TRUNC)
                .unwrap();
            view.write(&file, b"local edit", 0).unwrap();
            view.fsync_handle(&file, false).unwrap();
        })
        .await
        .unwrap();
    }

    let reopened = open_view(state.path(), &gateway.endpoint, false);
    assert!(!reopened.needs_hydration, "a writable mount keeps its tree");
    let hydrator = MountHydrator::shared(gateway.client(), Arc::clone(&reopened.view));
    let resumed = hydrator.resume_placeholders().unwrap();
    assert_eq!(
        resumed,
        files.len() - 1,
        "every unfetched file is a placeholder again"
    );

    let other = "conversations/c004/0004_turn.md";
    assert_eq!(
        read_through_view(&reopened.view, other).await.unwrap(),
        files[other]
    );
    hydrator.fill_all().await;
    assert_eq!(
        std::fs::read(backing(state.path(), fetched)).unwrap(),
        b"local edit",
        "the guest's write survives the restart and the fill"
    );
    assert_eq!(
        gateway.raw_reads(),
        files.len(),
        "nothing was fetched twice"
    );
    assert_eq!(reopened.view.lazy().outstanding(), 0);
}

/// A fetch that fails answers the open with `EIO` rather than zeros, and the next open tries
/// again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_fetch_fails_the_open_and_a_later_open_retries() {
    let files: BTreeMap<String, Vec<u8>> = [("x.txt".to_string(), b"real bytes".to_vec())]
        .into_iter()
        .collect();
    let gateway = Gateway::start(files).await;
    let state = tempfile::tempdir().expect("state dir");
    let opened = open_view(state.path(), &gateway.endpoint, true);
    let hydrator = MountHydrator::shared(gateway.client(), Arc::clone(&opened.view));
    hydrate(&hydrator).await;

    let held = gateway
        .remote
        .files
        .lock()
        .unwrap()
        .remove("x.txt")
        .unwrap();
    let error = read_through_view(&opened.view, "x.txt")
        .await
        .expect_err("no bytes, no answer");
    assert!(format!("{error}").contains("x.txt"), "{error}");
    gateway
        .remote
        .files
        .lock()
        .unwrap()
        .insert("x.txt".to_string(), held);
    assert_eq!(
        read_through_view(&opened.view, "x.txt").await.unwrap(),
        b"real bytes"
    );
}

/// Spec 86 D review: a placeholder above the inline bound is streamed in ranges. A writer
/// that commits mid-fill must not leave a torn file, and a size change since the listing
/// must not leave a permanent `EIO`: the fill pins the current content, notices it moved,
/// and fetches the new bytes at their new size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_file_rewritten_mid_fill_is_fetched_again_whole() {
    let old: Vec<u8> = (0..9 * 1024 * 1024).map(|index| (index % 251) as u8).collect();
    let new: Vec<u8> = (0..10 * 1024 * 1024).map(|index| (index % 241) as u8).collect();
    let files: BTreeMap<String, Vec<u8>> = [("big.bin".to_string(), old.clone())]
        .into_iter()
        .collect();
    let gateway = Gateway::start(files).await;
    let state = tempfile::tempdir().expect("state dir");
    // Writable: the case where a stale size used to fail every open.
    let opened = open_view(state.path(), &gateway.endpoint, false);
    let hydrator = MountHydrator::shared(gateway.client(), Arc::clone(&opened.view));
    hydrate(&hydrator).await;
    assert!(placeholder_at(&backing(state.path(), "big.bin")).unwrap().is_some());

    *gateway.remote.rewrite_after_first_range.lock().unwrap() =
        Some(("big.bin".to_string(), new.clone()));
    let bytes = read_through_view(&opened.view, "big.bin")
        .await
        .expect("the fill recovers from the rewrite");
    assert_eq!(bytes.len(), new.len(), "filled at the current size");
    assert!(bytes == new, "the new bytes, not a mix of old and new");
    assert!(placeholder_at(&backing(state.path(), "big.bin")).unwrap().is_none());
    assert_eq!(
        std::fs::metadata(backing(state.path(), "big.bin")).unwrap().len(),
        new.len() as u64
    );
}
