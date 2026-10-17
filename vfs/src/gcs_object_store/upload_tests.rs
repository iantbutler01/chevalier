use super::*;
use std::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy)]
enum ResponseBehavior {
    FirstStalls {
        committed: bool,
        retry_delay: Duration,
    },
    Delay(Duration),
    Status(u16),
}

#[derive(Default)]
struct Stored {
    bytes: Option<Vec<u8>>,
    requests: Vec<(String, Vec<u8>)>,
    commits: usize,
}

struct Fixture {
    client: Option<GcsObjectStoreClient>,
    state: Arc<Mutex<Stored>>,
    server: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        // The synchronous client owns a runtime; drop it outside Tokio even
        // when an assertion unwinds the asynchronous test.
        if let Some(client) = self.client.take() {
            let _ = std::thread::spawn(move || drop(client)).join();
        }
    }
}

impl Fixture {
    async fn new(behavior: ResponseBehavior, timeout_seconds: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(Stored::default()));
        let shared = state.clone();
        let server = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let state = shared.clone();
                        handlers.spawn(async move { serve(socket, state, behavior).await });
                    }
                    _ = handlers.join_next(), if !handlers.is_empty() => {}
                }
            }
        });
        let request_timeout = Duration::from_secs(timeout_seconds);
        let client = GcsObjectStoreClient {
            client: build_blocking_gcs_client(timeout_seconds as i64).unwrap(),
            async_client: AsyncClient::builder()
                .timeout(request_timeout)
                .build()
                .unwrap(),
            cfg: Arc::new(GcsResolvedObjectStoreConfig {
                bucket: "fixture".into(),
                request_timeout,
                api_base_url: base.clone(),
                upload_base_url: base,
                token_uri: "unused".into(),
                service_account: GcsServiceAccountKey {
                    client_email: "fixture@example.test".into(),
                    private_key: "unused".into(),
                    token_uri: None,
                },
            }),
            token_cache: Arc::new(Mutex::new(Some(CachedAccessToken {
                access_token: "fixture".into(),
                expires_at: Utc::now() + chrono::Duration::hours(1),
            }))),
        };
        Self {
            client: Some(client),
            state,
            server,
        }
    }

    fn client(&self) -> &GcsObjectStoreClient {
        self.client.as_ref().unwrap()
    }

    async fn write(&self, bytes: &[u8], condition: ObjectWriteCondition) -> VfsStorageResult<()> {
        self.client()
            .put_object_async("immutable-pack", bytes, condition)
            .await
    }

    async fn assert_stored_once(&self, bytes: &[u8], attempts: usize) {
        assert_eq!(
            self.client()
                .get_object_async("immutable-pack")
                .await
                .unwrap()
                .as_deref(),
            Some(bytes)
        );
        let state = self.state.lock().unwrap();
        assert_eq!(state.commits, 1);
        assert_eq!(state.requests.len(), attempts);
        for (_, sent) in &state.requests {
            assert_eq!(
                sent, bytes,
                "retry must retain the complete original upload"
            );
        }
    }
}

async fn serve(
    mut socket: TcpStream,
    state: Arc<Mutex<Stored>>,
    behavior: ResponseBehavior,
) -> std::io::Result<()> {
    let mut request = Vec::new();
    let boundary = loop {
        let mut chunk = [0; 4096];
        let count = socket.read(&mut chunk).await?;
        if count == 0 {
            return Ok(());
        }
        request.extend_from_slice(&chunk[..count]);
        if let Some(at) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let headers = String::from_utf8_lossy(&request[..boundary]);
    let target = headers.lines().next().unwrap().to_owned();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap_or(0);
    while request.len() < boundary + length {
        let mut chunk = [0; 4096];
        let count = socket.read(&mut chunk).await?;
        if count == 0 {
            return Ok(());
        }
        request.extend_from_slice(&chunk[..count]);
    }
    let body = request[boundary..boundary + length].to_vec();
    if target.starts_with("GET ") {
        let bytes = state.lock().unwrap().bytes.clone();
        return respond(
            &mut socket,
            if bytes.is_some() { 200 } else { 404 },
            &bytes.unwrap_or_default(),
        )
        .await;
    }
    let index = {
        let mut state = state.lock().unwrap();
        state.requests.push((target.clone(), body.clone()));
        state.requests.len() - 1
    };
    match behavior {
        ResponseBehavior::FirstStalls { committed, .. } if index == 0 => {
            if committed {
                let mut state = state.lock().unwrap();
                state.bytes = Some(body);
                state.commits += 1;
            }
            // Other connections remain accepted while this response never arrives.
            return std::future::pending().await;
        }
        ResponseBehavior::FirstStalls { retry_delay, .. } => tokio::time::sleep(retry_delay).await,
        ResponseBehavior::Delay(delay) => tokio::time::sleep(delay).await,
        ResponseBehavior::Status(status) => return respond(&mut socket, status, b"failure").await,
    }
    let status = {
        let mut state = state.lock().unwrap();
        if target.contains("ifGenerationMatch=0") && state.bytes.is_some() {
            412
        } else {
            state.bytes = Some(body);
            state.commits += 1;
            200
        }
    };
    respond(&mut socket, status, b"").await
}

async fn respond(socket: &mut TcpStream, status: u16, body: &[u8]) -> std::io::Result<()> {
    socket
        .write_all(
            format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
    socket.write_all(body).await
}

fn create_only() -> ObjectWriteCondition {
    ObjectWriteCondition {
        if_absent: true,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fast_create_still_uses_one_upload() {
    let fixture = Fixture::new(ResponseBehavior::Delay(Duration::ZERO), 10).await;
    fixture
        .write(b"ordinary turn", create_only())
        .await
        .unwrap();
    fixture.assert_stored_once(b"ordinary turn", 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_upload_response_retries_without_overwriting_the_committed_object() {
    let fixture = Fixture::new(
        ResponseBehavior::FirstStalls {
            committed: true,
            retry_delay: Duration::ZERO,
        },
        10,
    )
    .await;
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture.write(b"turn and tools", create_only()),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.assert_stored_once(b"turn and tools", 2).await;
    assert!(
        fixture
            .state
            .lock()
            .unwrap()
            .requests
            .iter()
            .all(|(target, _)| target.contains("ifGenerationMatch=0"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_first_upload_keeps_the_normal_deadline_for_its_retry() {
    let fixture = Fixture::new(
        ResponseBehavior::FirstStalls {
            committed: false,
            retry_delay: Duration::from_secs(4),
        },
        10,
    )
    .await;
    tokio::time::timeout(
        Duration::from_secs(9),
        fixture.write(b"final turn", create_only()),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.assert_stored_once(b"final turn", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lower_configured_deadline_is_not_lengthened() {
    let fixture = Fixture::new(
        ResponseBehavior::FirstStalls {
            committed: true,
            retry_delay: Duration::ZERO,
        },
        1,
    )
    .await;
    tokio::time::timeout(
        Duration::from_millis(2500),
        fixture.write(b"short deadline", create_only()),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.assert_stored_once(b"short deadline", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ordinary_and_large_uploads_keep_their_original_deadline() {
    for (body, condition) in [
        (b"ordinary".to_vec(), ObjectWriteCondition::default()),
        (
            b"conditional replacement".to_vec(),
            ObjectWriteCondition {
                if_generation_match: Some("7".into()),
                ..Default::default()
            },
        ),
        (vec![b'x'; SMALL_CREATE_MAX_BYTES + 1], create_only()),
    ] {
        let fixture = Fixture::new(ResponseBehavior::Delay(Duration::from_millis(3200)), 10).await;
        fixture.write(&body, condition).await.unwrap();
        fixture.assert_stored_once(&body, 1).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transient_upload_failures_exhaust_the_existing_attempt_budget() {
    let fixture = Fixture::new(ResponseBehavior::Status(503), 10).await;
    assert!(
        fixture
            .write(b"never committed", create_only())
            .await
            .is_err()
    );
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.requests.len(), GCS_HTTP_MAX_ATTEMPTS);
    assert_eq!(state.commits, 0);
    assert!(state.bytes.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_permanent_upload_failure_is_not_retried() {
    let fixture = Fixture::new(ResponseBehavior::Status(403), 10).await;
    assert!(fixture.write(b"denied", create_only()).await.is_err());
    assert_eq!(fixture.state.lock().unwrap().requests.len(), 1);
}
