//! How a remote VFS client authenticates to its gateway.
//!
//! A mount is given either a static bearer token (the operator binary, a VMD host) or a
//! per-mount *mount credential*. The mount credential never goes on a file request: the
//! client exchanges it at `{endpoint}/access-token` for a short-lived access token, uses
//! that until it nears expiry or the gateway answers 401, then exchanges again. Nothing
//! runs on a timer, so an idle mount makes no calls, and a VM resumed after days simply
//! exchanges on its first request.
//!
//! An exchange that fails for a reason that may pass (no connection, a timeout, a 5xx,
//! 408 or 429) is retried with backoff for a few seconds before a fallback token answers
//! instead: a guest fresh from a snapshot can take seconds before its first connection
//! gets through, and a mount that gave up on that first attempt spent its next minute on
//! the deployment-wide token. A refusal (401, 403, 404, …) falls back at once.

use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tokio::sync::Mutex;

/// Path, below the owner endpoint, where a mount credential buys an access token.
pub const ACCESS_TOKEN_PATH: &str = "/access-token";
/// An access token is replaced this long before the gateway would stop taking it.
const MAX_REFRESH_MARGIN: Duration = Duration::from_secs(60);
/// After an exchange fails and a fallback token answered instead, how long the
/// fallback is used before the exchange is tried again.
const FALLBACK_RETRY_AFTER: Duration = Duration::from_secs(60);
/// How long, from the start of the first attempt, an exchange that keeps failing for a
/// reason that may pass is retried before the fallback token answers instead. The
/// client's connect timeout is 5 s, so a hung first connect still gets a second attempt.
const EXCHANGE_RETRY_WINDOW: Duration = Duration::from_secs(10);
/// First pause between exchange attempts; it doubles up to `EXCHANGE_BACKOFF_MAX`.
const EXCHANGE_BACKOFF_FIRST: Duration = Duration::from_millis(250);
const EXCHANGE_BACKOFF_MAX: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct VfsAuth {
    mount_credential: Option<String>,
    /// The deployment-wide token. With a mount credential it is only the rollout
    /// fallback for a gateway that cannot exchange yet.
    fallback_token: Option<String>,
    exchange_url: String,
    exchange_retry_window: Duration,
    cached: Mutex<Option<CachedBearer>>,
}

#[derive(Debug)]
struct CachedBearer {
    token: String,
    /// Both clocks: the monotonic one survives wall-clock jumps, the wall one keeps
    /// counting while a paused VM is frozen.
    refresh_at: Instant,
    refresh_at_wall: SystemTime,
    /// An access token the mount credential bought (not the rollout fallback).
    exchanged: bool,
}

impl CachedBearer {
    fn fresh(&self) -> bool {
        Instant::now() < self.refresh_at && SystemTime::now() < self.refresh_at_wall
    }
}

/// Why one exchange attempt failed, and whether trying again may succeed.
#[derive(Debug)]
struct ExchangeFailure {
    error: anyhow::Error,
    /// No answer arrived (connect, timeout) or the gateway was briefly unable (5xx, 408,
    /// 429). A refusal or an unusable answer is not transient.
    transient: bool,
}

impl ExchangeFailure {
    fn transient(error: anyhow::Error) -> Self {
        Self {
            error,
            transient: true,
        }
    }

    fn refused(error: anyhow::Error) -> Self {
        Self {
            error,
            transient: false,
        }
    }
}

#[derive(Deserialize)]
struct AccessTokenResponse {
    access_token: String,
    expires_in: u64,
}

impl VfsAuth {
    /// A fixed bearer token, sent as is on every request.
    pub fn static_token(token: &str) -> Self {
        Self {
            mount_credential: None,
            fallback_token: Some(token.to_string()),
            exchange_url: String::new(),
            exchange_retry_window: EXCHANGE_RETRY_WINDOW,
            cached: Mutex::new(None),
        }
    }

    /// Blank values count as absent; at least one of the two is required.
    pub fn new(
        endpoint: &str,
        mount_credential: Option<&str>,
        fallback_token: Option<&str>,
    ) -> Result<Self> {
        let nonblank = |value: Option<&str>| {
            value
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        let mount_credential = nonblank(mount_credential);
        let fallback_token = nonblank(fallback_token);
        if mount_credential.is_none() && fallback_token.is_none() {
            return Err(anyhow!("a VFS mount needs a mount credential or a token"));
        }
        Ok(Self {
            mount_credential,
            fallback_token,
            exchange_url: format!("{}{ACCESS_TOKEN_PATH}", endpoint.trim_end_matches('/')),
            exchange_retry_window: EXCHANGE_RETRY_WINDOW,
            cached: Mutex::new(None),
        })
    }

    #[cfg(test)]
    fn with_exchange_retry_window(mut self, window: Duration) -> Self {
        self.exchange_retry_window = window;
        self
    }

    /// Whether a rejected bearer can be replaced by exchanging again.
    pub fn refreshable(&self) -> bool {
        self.mount_credential.is_some()
    }

    /// The bearer to send now, exchanging the mount credential first when the cached
    /// access token is missing or near expiry. Concurrent callers share one exchange.
    pub async fn bearer(&self, client: &Client) -> Result<String> {
        let Some(credential) = self.mount_credential.as_deref() else {
            return self
                .fallback_token
                .clone()
                .ok_or_else(|| anyhow!("VFS auth has no token"));
        };
        let mut cached = self.cached.lock().await;
        if let Some(bearer) = cached.as_ref().filter(|bearer| bearer.fresh()) {
            return Ok(bearer.token.clone());
        }
        match self.exchange_with_retry(client, credential).await {
            Ok((token, expires_in)) => {
                let margin = MAX_REFRESH_MARGIN.min(expires_in / 2);
                let lifetime = expires_in.saturating_sub(margin);
                *cached = Some(CachedBearer {
                    token: token.clone(),
                    refresh_at: Instant::now() + lifetime,
                    refresh_at_wall: SystemTime::now() + lifetime,
                    exchanged: true,
                });
                Ok(token)
            }
            Err(error) => match self.fallback_token.as_deref() {
                Some(fallback) => {
                    tracing::warn!(%error, "VFS access token exchange failed; using the fallback token");
                    *cached = Some(CachedBearer {
                        token: fallback.to_string(),
                        refresh_at: Instant::now() + FALLBACK_RETRY_AFTER,
                        refresh_at_wall: SystemTime::now() + FALLBACK_RETRY_AFTER,
                        exchanged: false,
                    });
                    Ok(fallback.to_string())
                }
                None => Err(error),
            },
        }
    }

    /// Whether requests now go out with an access token exchanged for this mount's own
    /// per-computer credential. `false` for a deployment-token mount, and for a mount whose
    /// exchange failed and fell back to the deployment token.
    pub async fn holds_exchanged_token(&self) -> bool {
        self.mount_credential.is_some()
            && self
                .cached
                .lock()
                .await
                .as_ref()
                .is_some_and(|bearer| bearer.exchanged)
    }

    /// Forget `rejected` so the next `bearer` exchanges again. A token some other
    /// request already replaced is left alone.
    pub async fn invalidate(&self, rejected: &str) {
        let mut cached = self.cached.lock().await;
        if cached
            .as_ref()
            .is_some_and(|bearer| bearer.token == rejected)
        {
            *cached = None;
        }
    }

    /// Exchanges, retrying a failure that may pass with backoff until
    /// `exchange_retry_window` after the first attempt began. A refusal returns at once.
    async fn exchange_with_retry(
        &self,
        client: &Client,
        credential: &str,
    ) -> Result<(String, Duration)> {
        let deadline = Instant::now() + self.exchange_retry_window;
        let mut backoff = EXCHANGE_BACKOFF_FIRST;
        let mut attempt = 1u32;
        loop {
            let failure = match self.exchange(client, credential).await {
                Ok(exchanged) => return Ok(exchanged),
                Err(failure) => failure,
            };
            if !failure.transient || Instant::now() + backoff >= deadline {
                return Err(failure.error.context(format!(
                    "VFS access token exchange gave up after {attempt} attempt(s)"
                )));
            }
            tracing::info!(
                error = %failure.error,
                attempt,
                retry_in_ms = backoff.as_millis() as u64,
                "VFS access token exchange failed; retrying"
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(EXCHANGE_BACKOFF_MAX);
            attempt += 1;
        }
    }

    async fn exchange(
        &self,
        client: &Client,
        credential: &str,
    ) -> std::result::Result<(String, Duration), ExchangeFailure> {
        let response = client
            .post(&self.exchange_url)
            .bearer_auth(credential)
            .send()
            .await
            .context("send VFS access token exchange")
            .map_err(ExchangeFailure::transient)?;
        let status = response.status();
        if status != StatusCode::OK {
            let body = response.text().await.unwrap_or_default();
            let error = anyhow!("VFS access token exchange failed: {status} {body}");
            let passing = status.is_server_error()
                || status == StatusCode::REQUEST_TIMEOUT
                || status == StatusCode::TOO_MANY_REQUESTS;
            return Err(if passing {
                ExchangeFailure::transient(error)
            } else {
                ExchangeFailure::refused(error)
            });
        }
        let body: AccessTokenResponse = response.json().await.map_err(|error| {
            // A body cut off in transit may arrive whole next time; a malformed one won't.
            let transient = !error.is_decode();
            ExchangeFailure {
                error: anyhow::Error::new(error).context("decode VFS access token response"),
                transient,
            }
        })?;
        if body.access_token.trim().is_empty() || body.expires_in == 0 {
            return Err(ExchangeFailure::refused(anyhow!(
                "VFS access token response carried no usable token"
            )));
        }
        Ok((body.access_token, Duration::from_secs(body.expires_in)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex as StdMutex};

    use axum::{
        extract::State,
        http::{HeaderMap, StatusCode as HttpStatus},
        response::IntoResponse,
    };

    use super::*;
    use crate::fuse::client::RemoteVfsClient;

    #[derive(Default)]
    struct Gateway {
        issued: u32,
        /// The only access token file requests are answered with.
        valid: Option<String>,
        exchange_bearers: Vec<String>,
        file_bearers: Vec<String>,
    }

    type Shared = Arc<StdMutex<Gateway>>;

    fn bearer(headers: &HeaderMap) -> String {
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or_default()
            .to_string()
    }

    async fn exchange(State(gateway): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
        let mut gateway = gateway.lock().unwrap();
        gateway.exchange_bearers.push(bearer(&headers));
        if bearer(&headers) != "mount-cred" {
            return HttpStatus::UNAUTHORIZED.into_response();
        }
        gateway.issued += 1;
        let token = format!("access-{}", gateway.issued);
        gateway.valid = Some(token.clone());
        axum::Json(serde_json::json!({ "access_token": token, "expires_in": 900 })).into_response()
    }

    async fn file(State(gateway): State<Shared>, headers: HeaderMap) -> impl IntoResponse {
        let mut gateway = gateway.lock().unwrap();
        let token = bearer(&headers);
        gateway.file_bearers.push(token.clone());
        if gateway.valid.as_deref() == Some(token.as_str()) {
            // An absent path: a complete stat answer with no body to decode.
            HttpStatus::NOT_FOUND
        } else {
            HttpStatus::UNAUTHORIZED
        }
    }

    /// A mount exchanges once, reuses the access token, and when the gateway stops
    /// taking it (expired across a pause, rotated key) exchanges again and resends
    /// the same request. The mount credential never rides on a file request.
    #[test]
    fn a_rejected_access_token_is_exchanged_again_and_the_request_resent() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}/owner/nym-1", listener.local_addr().unwrap());
        let gateway: Shared = Arc::default();
        let server_gateway = Arc::clone(&gateway);
        let server = runtime.spawn(async move {
            axum::serve(
                listener,
                axum::Router::new()
                    .route("/owner/nym-1/access-token", axum::routing::post(exchange))
                    .route("/owner/nym-1/stat", axum::routing::get(file))
                    .with_state(server_gateway),
            )
            .await
            .unwrap();
        });
        let auth = VfsAuth::new(&endpoint, Some("mount-cred"), None).unwrap();
        let client = RemoteVfsClient::with_auth(&endpoint, auth, "scope").unwrap();

        runtime.block_on(client.stat_versioned("a")).unwrap();
        runtime.block_on(client.stat_versioned("b")).unwrap();
        // The gateway forgets the token, as it does once one expires.
        gateway.lock().unwrap().valid = None;
        runtime.block_on(client.stat_versioned("c")).unwrap();

        let gateway = gateway.lock().unwrap();
        assert_eq!(gateway.exchange_bearers, ["mount-cred", "mount-cred"]);
        assert_eq!(
            gateway.file_bearers,
            ["access-1", "access-1", "access-1", "access-2"]
        );
        drop(gateway);
        server.abort();
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    /// An exchange gateway that answers each attempt with the next status of `script`
    /// (200 issues a token), and with 200 once the script runs out. Returns its owner
    /// endpoint and the number of attempts it has seen.
    fn scripted_gateway(
        runtime: &tokio::runtime::Runtime,
        script: Vec<u16>,
    ) -> (String, Arc<StdMutex<u32>>) {
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}/owner/nym-1", listener.local_addr().unwrap());
        let attempts = Arc::new(StdMutex::new(0u32));
        let seen = Arc::clone(&attempts);
        let router = axum::Router::new().route(
            "/owner/nym-1/access-token",
            axum::routing::post(move || {
                let seen = Arc::clone(&seen);
                let script = script.clone();
                async move {
                    let attempt = {
                        let mut seen = seen.lock().unwrap();
                        *seen += 1;
                        *seen
                    };
                    match script.get(attempt as usize - 1).copied().unwrap_or(200) {
                        200 => axum::Json(serde_json::json!({
                            "access_token": format!("access-{attempt}"),
                            "expires_in": 900,
                        }))
                        .into_response(),
                        status => HttpStatus::from_u16(status).unwrap().into_response(),
                    }
                }
            }),
        );
        runtime.spawn(async move { axum::serve(listener, router).await.unwrap() });
        (endpoint, attempts)
    }

    /// The staging failure (spec 87 T1-4): a fresh guest's first exchange cannot connect
    /// for a few seconds. The mount keeps trying and ends up on its own access token, never
    /// on the deployment-wide one.
    #[test]
    fn a_first_exchange_that_cannot_connect_waits_for_the_gateway_instead_of_falling_back() {
        let runtime = runtime();
        // Reserve a port, close it, and only start serving on it after a while: until then
        // every connect is refused, as it is before a restored guest's network is up.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let issued = Arc::new(StdMutex::new(0u32));
        let server_issued = Arc::clone(&issued);
        runtime.spawn(async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
                .await
                .unwrap();
            let router = axum::Router::new().route(
                "/owner/nym-1/access-token",
                axum::routing::post(move || {
                    *server_issued.lock().unwrap() += 1;
                    async {
                        axum::Json(serde_json::json!({ "access_token": "own", "expires_in": 900 }))
                    }
                }),
            );
            axum::serve(listener, router).await.unwrap();
        });
        let endpoint = format!("http://127.0.0.1:{port}/owner/nym-1");
        let auth = VfsAuth::new(&endpoint, Some("mount-cred"), Some("global")).unwrap();

        let token = runtime.block_on(auth.bearer(&Client::new())).unwrap();

        assert_eq!(token, "own");
        assert!(runtime.block_on(auth.holds_exchanged_token()));
        assert_eq!(*issued.lock().unwrap(), 1);
    }

    #[test]
    fn a_gateway_that_fails_briefly_is_retried_until_it_issues_a_token() {
        let runtime = runtime();
        let (endpoint, attempts) = scripted_gateway(&runtime, vec![503, 502, 429]);
        let auth = VfsAuth::new(&endpoint, Some("mount-cred"), Some("global")).unwrap();

        let token = runtime.block_on(auth.bearer(&Client::new())).unwrap();

        assert_eq!(token, "access-4");
        assert_eq!(*attempts.lock().unwrap(), 4);
        assert!(runtime.block_on(auth.holds_exchanged_token()));
    }

    /// The fallback stays for an exchange that keeps failing; it answers once the retry
    /// window is spent, not before.
    #[test]
    fn an_exchange_that_keeps_failing_falls_back_once_the_window_is_spent() {
        let runtime = runtime();
        let window = Duration::from_millis(1200);
        // Nothing listens here: every attempt fails at connect.
        let auth = VfsAuth::new("http://127.0.0.1:1/owner", Some("cred"), Some("global"))
            .unwrap()
            .with_exchange_retry_window(window);
        let started = Instant::now();
        let token = runtime.block_on(auth.bearer(&Client::new())).unwrap();
        let elapsed = started.elapsed();
        assert_eq!(token, "global");
        assert!(!runtime.block_on(auth.holds_exchanged_token()));
        // 250 + 500 ms of backoff fit the window; the next 1 s pause does not. No new
        // attempt starts past the window, so only one attempt's own failure (bounded by
        // the 5 s connect timeout, milliseconds when refused) can run over it.
        assert!(elapsed >= Duration::from_millis(750), "{elapsed:?}");
        assert!(elapsed < window + Duration::from_secs(5), "{elapsed:?}");

        let without_fallback = VfsAuth::new("http://127.0.0.1:1/owner", Some("cred"), None)
            .unwrap()
            .with_exchange_retry_window(window);
        assert!(
            runtime
                .block_on(without_fallback.bearer(&Client::new()))
                .is_err()
        );
    }

    /// A gateway that refuses the credential (or has no exchange route yet) will not
    /// change its mind in a few seconds: the fallback answers on the first attempt.
    #[test]
    fn a_refused_exchange_falls_back_without_retrying() {
        let runtime = runtime();
        for refusal in [401, 403, 404] {
            let (endpoint, attempts) = scripted_gateway(&runtime, vec![refusal]);
            let auth = VfsAuth::new(&endpoint, Some("mount-cred"), Some("global")).unwrap();
            let token = runtime.block_on(auth.bearer(&Client::new())).unwrap();
            assert_eq!(token, "global", "{refusal}");
            assert_eq!(*attempts.lock().unwrap(), 1, "{refusal}");
        }
    }

    #[test]
    fn a_mount_needs_some_credential() {
        assert!(VfsAuth::new("http://gw", None, Some("  ")).is_err());
        assert!(
            !VfsAuth::new("http://gw", None, Some("tok"))
                .unwrap()
                .refreshable()
        );
        let auth = VfsAuth::new("http://gw/owner/", Some("cred"), None).unwrap();
        assert!(auth.refreshable());
        assert_eq!(auth.exchange_url, "http://gw/owner/access-token");
    }
}
