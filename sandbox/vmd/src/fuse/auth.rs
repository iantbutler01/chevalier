//! How a remote VFS client authenticates to its gateway.
//!
//! A mount is given either a static bearer token (the operator binary, a VMD host) or a
//! per-mount *mount credential*. The mount credential never goes on a file request: the
//! client exchanges it at `{endpoint}/access-token` for a short-lived access token, uses
//! that until it nears expiry or the gateway answers 401, then exchanges again. Nothing
//! runs on a timer, so an idle mount makes no calls, and a VM resumed after days simply
//! exchanges on its first request.

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

#[derive(Debug)]
pub struct VfsAuth {
    mount_credential: Option<String>,
    /// The deployment-wide token. With a mount credential it is only the rollout
    /// fallback for a gateway that cannot exchange yet.
    fallback_token: Option<String>,
    exchange_url: String,
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
            cached: Mutex::new(None),
        })
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
        match self.exchange(client, credential).await {
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

    async fn exchange(&self, client: &Client, credential: &str) -> Result<(String, Duration)> {
        let response = client
            .post(&self.exchange_url)
            .bearer_auth(credential)
            .send()
            .await
            .context("send VFS access token exchange")?;
        let status = response.status();
        if status != StatusCode::OK {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("VFS access token exchange failed: {status} {body}"));
        }
        let body: AccessTokenResponse = response
            .json()
            .await
            .context("decode VFS access token response")?;
        if body.access_token.trim().is_empty() || body.expires_in == 0 {
            return Err(anyhow!("VFS access token response carried no usable token"));
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

    #[test]
    fn a_failed_exchange_falls_back_to_the_rollout_token() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Nothing listens here: the exchange fails at connect.
        let auth = VfsAuth::new("http://127.0.0.1:1/owner", Some("cred"), Some("global")).unwrap();
        let token = runtime.block_on(auth.bearer(&Client::new())).unwrap();
        assert_eq!(token, "global");
        let without_fallback =
            VfsAuth::new("http://127.0.0.1:1/owner", Some("cred"), None).unwrap();
        assert!(
            runtime
                .block_on(without_fallback.bearer(&Client::new()))
                .is_err()
        );
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
