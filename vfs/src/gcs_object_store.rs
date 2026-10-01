// @dive-file: Google Cloud Storage implementation of the generic object-store boundary.
// @dive-rel: Owns token exchange, retry policy, conditional object writes/deletes, ranged
// @dive-rel: reads, and batch deletes independently of any product VFS projection.

use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use reqwest::{
    Client as AsyncClient, RequestBuilder as AsyncRequestBuilder, Response as AsyncResponse,
    StatusCode,
    blocking::{Client, RequestBuilder, Response},
    header::{CONTENT_LENGTH, CONTENT_TYPE},
};
use serde::{Deserialize, Serialize};
use tracing::{Instrument, info_span};
use uuid::Uuid;

use crate::{
    VfsStorageError, VfsStorageResult,
    object_store::{
        ObjectDeleteCondition, ObjectListPage, ObjectListRequest, ObjectMetadata,
        ObjectStoreClient, ObjectWriteCondition,
    },
};

const DEVSTORAGE_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
const GCS_HTTP_MAX_ATTEMPTS: usize = 5;
const GCS_HTTP_RETRY_BASE_DELAY_MS: u64 = 500;
const GCS_HTTP_RETRY_MAX_DELAY_MS: u64 = 8_000;
const GCS_HTTP_RETRY_JITTER_PCT: u64 = 25;
const GCS_HTTP_CONNECT_TIMEOUT_SECS: u64 = 10;
const GCS_HTTP_TCP_KEEPALIVE_SECS: u64 = 30;
const GCS_HTTP_POOL_IDLE_TIMEOUT_SECS: u64 = 30;
const GCS_BATCH_DELETE_MAX_CALLS: usize = 100;
const GCS_BATCH_ENDPOINT: &str = "https://storage.googleapis.com/batch/storage/v1";

#[derive(Clone, Debug)]
pub struct GcsObjectStoreConfig {
    pub bucket: String,
    pub service_account_key_path: Option<PathBuf>,
    pub api_base_url: String,
    pub upload_base_url: String,
    pub token_uri: String,
    pub timeout_seconds: i64,
}

#[derive(Clone)]
pub struct GcsObjectStoreClient {
    client: Client,
    async_client: AsyncClient,
    cfg: Arc<GcsResolvedObjectStoreConfig>,
    token_cache: Arc<Mutex<Option<CachedAccessToken>>>,
}

#[derive(Clone)]
struct GcsResolvedObjectStoreConfig {
    bucket: String,
    api_base_url: String,
    upload_base_url: String,
    token_uri: String,
    service_account: GcsServiceAccountKey,
}

impl GcsObjectStoreClient {
    pub fn new(cfg: GcsObjectStoreConfig) -> VfsStorageResult<Self> {
        let service_account_key_path = cfg.service_account_key_path.as_ref().ok_or_else(|| {
            VfsStorageError::Internal(
                "runtime.gcs.service_account_key_path is required when runtime.nymfs_backend=gcs"
                    .to_string(),
            )
        })?;
        let key_bytes = fs::read(service_account_key_path).map_err(internal)?;
        let service_account: GcsServiceAccountKey =
            serde_json::from_slice(&key_bytes).map_err(internal)?;

        let token_uri = service_account
            .token_uri
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| cfg.token_uri.trim().to_string());
        if token_uri.trim().is_empty() {
            return Err(VfsStorageError::Internal(
                "runtime.gcs.token_uri or service account token_uri is required when runtime.nymfs_backend=gcs"
                    .to_string(),
            ));
        }

        let client = build_blocking_gcs_client(cfg.timeout_seconds)?;
        let async_client = AsyncClient::builder()
            .connect_timeout(Duration::from_secs(GCS_HTTP_CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(cfg.timeout_seconds.max(1) as u64))
            .tcp_keepalive(Some(Duration::from_secs(GCS_HTTP_TCP_KEEPALIVE_SECS)))
            .pool_max_idle_per_host(256)
            .pool_idle_timeout(Some(Duration::from_secs(GCS_HTTP_POOL_IDLE_TIMEOUT_SECS)))
            .build()
            .map_err(internal)?;

        Ok(Self {
            client,
            async_client,
            cfg: Arc::new(GcsResolvedObjectStoreConfig {
                bucket: cfg.bucket.clone(),
                api_base_url: cfg.api_base_url.trim_end_matches('/').to_string(),
                upload_base_url: cfg.upload_base_url.trim_end_matches('/').to_string(),
                token_uri,
                service_account,
            }),
            token_cache: Arc::new(Mutex::new(None)),
        })
    }

    fn access_token(&self) -> VfsStorageResult<String> {
        if let Some(token) = self
            .token_cache
            .lock()
            .map_err(internal)?
            .as_ref()
            .filter(|token| token.expires_at > Utc::now() + chrono::Duration::seconds(30))
            .map(|token| token.access_token.clone())
        {
            return Ok(token);
        }

        let claims = GoogleServiceAccountClaims {
            iss: self.cfg.service_account.client_email.clone(),
            scope: DEVSTORAGE_SCOPE.to_string(),
            aud: self.cfg.token_uri.clone(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::minutes(55)).timestamp(),
        };
        let assertion = jsonwebtoken::encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(self.cfg.service_account.private_key.as_bytes())
                .map_err(internal)?,
        )
        .map_err(internal)?;

        let client = self.client.clone();
        let token_uri = self.cfg.token_uri.clone();
        let response = send_with_retries(
            move || {
                Ok(client.post(&token_uri).form(&[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                    ("assertion", assertion.as_str()),
                ]))
            },
            "oauth token exchange",
        )?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs oauth token exchange failed: {status} {body}"
            )));
        }
        let token: GoogleAccessTokenResponse = response.json().map_err(internal)?;
        let cached_token = CachedAccessToken {
            access_token: token.access_token.clone(),
            expires_at: Utc::now() + chrono::Duration::seconds(token.expires_in.max(1)),
        };
        *self.token_cache.lock().map_err(internal)? = Some(cached_token);
        Ok(token.access_token)
    }

    async fn access_token_async(&self) -> VfsStorageResult<String> {
        if let Some(token) = self
            .token_cache
            .lock()
            .map_err(internal)?
            .as_ref()
            .filter(|token| token.expires_at > Utc::now() + chrono::Duration::seconds(30))
            .map(|token| token.access_token.clone())
        {
            return Ok(token);
        }

        let claims = GoogleServiceAccountClaims {
            iss: self.cfg.service_account.client_email.clone(),
            scope: DEVSTORAGE_SCOPE.to_string(),
            aud: self.cfg.token_uri.clone(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::minutes(55)).timestamp(),
        };
        let assertion = jsonwebtoken::encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(self.cfg.service_account.private_key.as_bytes())
                .map_err(internal)?,
        )
        .map_err(internal)?;

        let client = self.async_client.clone();
        let token_uri = self.cfg.token_uri.clone();
        let response = send_with_retries_async(
            move || {
                Ok(client.post(&token_uri).form(&[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                    ("assertion", assertion.as_str()),
                ]))
            },
            "oauth token exchange",
        )
        .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs oauth token exchange failed: {status} {body}"
            )));
        }
        let token: GoogleAccessTokenResponse = response.json().await.map_err(internal)?;
        let cached_token = CachedAccessToken {
            access_token: token.access_token.clone(),
            expires_at: Utc::now() + chrono::Duration::seconds(token.expires_in.max(1)),
        };
        *self.token_cache.lock().map_err(internal)? = Some(cached_token);
        Ok(token.access_token)
    }

    fn object_metadata_url(&self, object_key: &str) -> String {
        format!(
            "{}/b/{}/o/{}",
            self.cfg.api_base_url,
            urlencoding::encode(&self.cfg.bucket),
            urlencoding::encode(object_key)
        )
    }

    fn object_media_url(&self, object_key: &str) -> String {
        format!("{}?alt=media", self.object_metadata_url(object_key))
    }

    fn batch_delete_path(&self, key: &str, condition: &ObjectDeleteCondition) -> String {
        let mut path = format!(
            "/storage/v1/b/{}/o/{}",
            urlencoding::encode(&self.cfg.bucket),
            urlencoding::encode(key)
        );
        let mut query = Vec::new();
        if let Some(generation) = condition.if_generation_match.as_deref() {
            query.push(format!(
                "ifGenerationMatch={}",
                urlencoding::encode(generation)
            ));
        }
        if let Some(metageneration) = condition.if_metageneration_match.as_deref() {
            query.push(format!(
                "ifMetagenerationMatch={}",
                urlencoding::encode(metageneration)
            ));
        }
        if !query.is_empty() {
            path.push('?');
            path.push_str(&query.join("&"));
        }
        path
    }

    async fn batch_delete_objects_async(
        &self,
        keys: &[String],
        condition: ObjectDeleteCondition,
    ) -> VfsStorageResult<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let client = self.async_client.clone();
        let auth = self.access_token_async().await?;

        for chunk in keys.chunks(GCS_BATCH_DELETE_MAX_CALLS) {
            let boundary = format!("===============nymfs-batch-{}==", Uuid::new_v4().simple());
            let mut body = String::new();
            for (idx, key) in chunk.iter().enumerate() {
                body.push_str(&format!("--{boundary}\r\n"));
                body.push_str("Content-Type: application/http\r\n");
                body.push_str("Content-Transfer-Encoding: binary\r\n");
                body.push_str(&format!("Content-ID: <nymfs-delete-{}>\r\n\r\n", idx + 1));
                body.push_str(&format!(
                    "DELETE {} HTTP/1.1\r\naccept: application/json\r\n\r\n",
                    self.batch_delete_path(key, &condition)
                ));
            }
            body.push_str(&format!("--{boundary}--\r\n"));
            let response = send_with_retries_async(
                || {
                    Ok(client
                        .post(GCS_BATCH_ENDPOINT)
                        .bearer_auth(&auth)
                        .header(
                            CONTENT_TYPE,
                            format!("multipart/mixed; boundary={boundary}"),
                        )
                        .body(body.clone()))
                },
                "batch delete",
            )
            .await?;
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(VfsStorageError::Internal(format!(
                    "gcs batch delete failed: {status} {body}"
                )));
            }
            self.validate_batch_delete_response(chunk, response).await?;
        }
        Ok(())
    }

    async fn validate_batch_delete_response(
        &self,
        keys: &[String],
        response: AsyncResponse,
    ) -> VfsStorageResult<()> {
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let boundary = content_type
            .split("boundary=")
            .nth(1)
            .map(|value| value.trim_matches('"').to_string())
            .ok_or_else(|| {
                VfsStorageError::Internal("gcs batch delete missing response boundary".to_string())
            })?;
        let body = response.text().await.map_err(internal)?;
        let mut statuses = Vec::new();
        for part in body.split(&format!("--{boundary}")) {
            let trimmed = part.trim();
            if trimmed.is_empty() || trimmed == "--" {
                continue;
            }
            if let Some(status_line) = trimmed.lines().find(|line| line.starts_with("HTTP/1.1 ")) {
                let code = status_line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|value| value.parse::<u16>().ok())
                    .ok_or_else(|| {
                        VfsStorageError::Internal(format!(
                            "gcs batch delete returned malformed status line: {status_line}"
                        ))
                    })?;
                statuses.push(code);
            }
        }
        if statuses.len() != keys.len() {
            return Err(VfsStorageError::Internal(format!(
                "gcs batch delete returned {} results for {} keys",
                statuses.len(),
                keys.len()
            )));
        }
        for (key, status) in keys.iter().zip(statuses) {
            if matches!(status, 200..=299 | 404) {
                continue;
            }
            if status == 412 {
                return Err(VfsStorageError::Conflict(format!(
                    "gcs delete precondition failed for {key}"
                )));
            }
            return Err(VfsStorageError::Internal(format!(
                "gcs batch delete failed for {key}: status {status}"
            )));
        }
        Ok(())
    }
}

fn build_blocking_gcs_client(timeout_seconds: i64) -> VfsStorageResult<Client> {
    let build = || {
        Client::builder()
            .connect_timeout(Duration::from_secs(GCS_HTTP_CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(timeout_seconds.max(1) as u64))
            .tcp_keepalive(Some(Duration::from_secs(GCS_HTTP_TCP_KEEPALIVE_SECS)))
            .pool_idle_timeout(Some(Duration::from_secs(GCS_HTTP_POOL_IDLE_TIMEOUT_SECS)))
            .http1_only()
            .build()
            .map_err(internal)
    };

    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(build)
    } else {
        build()
    }
}

#[async_trait]
impl ObjectStoreClient for GcsObjectStoreClient {
    fn stat_object(&self, key: &str) -> VfsStorageResult<Option<ObjectMetadata>> {
        let key = key.to_string();
        let client = self.client.clone();
        let object_url = self.object_metadata_url(&key);
        let auth = self.access_token()?;
        let response = send_with_retries(
            move || Ok(client.get(&object_url).bearer_auth(&auth)),
            "stat",
        )?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs stat failed for {key}: {status} {body}"
            )));
        }
        let payload: GcsObjectMetadataResponse = response.json().map_err(internal)?;
        Ok(Some(payload.try_into()?))
    }

    fn get_object(&self, key: &str) -> VfsStorageResult<Option<Vec<u8>>> {
        let key = key.to_string();
        let client = self.client.clone();
        let object_url = self.object_media_url(&key);
        let auth = self.access_token()?;
        let response = send_with_retries(
            move || Ok(client.get(&object_url).bearer_auth(&auth)),
            "read",
        )?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs read failed for {key}: {status} {body}"
            )));
        }
        Ok(Some(response.bytes().map_err(internal)?.to_vec()))
    }

    fn put_object(
        &self,
        key: &str,
        bytes: &[u8],
        condition: ObjectWriteCondition,
    ) -> VfsStorageResult<()> {
        let key = key.to_string();
        let body = bytes.to_vec();
        let request_key = key.clone();
        let client = self.client.clone();
        let bucket = self.cfg.bucket.clone();
        let upload_base_url = self.cfg.upload_base_url.clone();
        let auth = self.access_token()?;
        let response = send_with_retries(
            move || {
                let mut request = client
                    .post(format!(
                        "{}/b/{}/o",
                        upload_base_url,
                        urlencoding::encode(&bucket)
                    ))
                    .bearer_auth(&auth);
                request = request.query(&[("uploadType", "media"), ("name", request_key.as_str())]);
                if condition.if_absent {
                    request = request.query(&[("ifGenerationMatch", "0")]);
                } else if let Some(generation) = condition.if_generation_match.as_deref() {
                    request = request.query(&[("ifGenerationMatch", generation)]);
                }
                if let Some(metageneration) = condition.if_metageneration_match.as_deref() {
                    request = request.query(&[("ifMetagenerationMatch", metageneration)]);
                }
                Ok(request
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .header(CONTENT_LENGTH, body.len().to_string())
                    .body(body.clone()))
            },
            "write",
        )?;
        if condition.if_absent && response.status() == StatusCode::PRECONDITION_FAILED {
            tracing::info!(
                kind = "nymfs_perf",
                backend = "gcs",
                op = "write_if_missing",
                outcome = "precondition_failed",
                pack_key = %key,
                "optimistic write hit if_absent precondition (object already present)"
            );
            return Ok(());
        }
        if response.status() == StatusCode::PRECONDITION_FAILED {
            return Err(VfsStorageError::Conflict(format!(
                "gcs write precondition failed for {key}"
            )));
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs write failed for {key}: {status} {body}"
            )));
        }
        Ok(())
    }

    fn delete_object(&self, key: &str, condition: ObjectDeleteCondition) -> VfsStorageResult<()> {
        let key = key.to_string();
        let client = self.client.clone();
        let object_url = self.object_metadata_url(&key);
        let auth = self.access_token()?;
        let response = send_with_retries(
            move || {
                let mut request = client.delete(&object_url).bearer_auth(&auth);
                if let Some(generation) = condition.if_generation_match.as_deref() {
                    request = request.query(&[("ifGenerationMatch", generation)]);
                }
                if let Some(metageneration) = condition.if_metageneration_match.as_deref() {
                    request = request.query(&[("ifMetagenerationMatch", metageneration)]);
                }
                Ok(request)
            },
            "delete",
        )?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        if response.status() == StatusCode::PRECONDITION_FAILED {
            return Err(VfsStorageError::Conflict(format!(
                "gcs delete precondition failed for {key}"
            )));
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs delete failed for {key}: {status} {body}"
            )));
        }
        Ok(())
    }

    fn copy_object(&self, from: &str, to: &str) -> VfsStorageResult<()> {
        let from = from.to_string();
        let to = to.to_string();
        let request_from = from.clone();
        let request_to = to.clone();
        let client = self.client.clone();
        let api_base_url = self.cfg.api_base_url.clone();
        let bucket = self.cfg.bucket.clone();
        let auth = self.access_token()?;
        let response = send_with_retries(
            move || {
                Ok(client
                    .post(format!(
                        "{}/b/{}/o/{}/copyTo/b/{}/o/{}",
                        api_base_url,
                        urlencoding::encode(&bucket),
                        urlencoding::encode(&request_from),
                        urlencoding::encode(&bucket),
                        urlencoding::encode(&request_to)
                    ))
                    .bearer_auth(&auth))
            },
            "copy",
        )?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs copy failed from {from} to {to}: {status} {body}"
            )));
        }
        Ok(())
    }

    fn list_objects(&self, request: ObjectListRequest) -> VfsStorageResult<ObjectListPage> {
        let mut query: Vec<(&str, String)> = vec![("prefix", request.prefix)];
        if let Some(delimiter) = request.delimiter {
            query.push(("delimiter", delimiter));
        }
        if let Some(max_results) = request.max_results {
            query.push(("maxResults", max_results.to_string()));
        }
        if let Some(page_token) = request.page_token {
            query.push(("pageToken", page_token));
        }
        let client = self.client.clone();
        let api_base_url = self.cfg.api_base_url.clone();
        let bucket = self.cfg.bucket.clone();
        let auth = self.access_token()?;
        let response = send_with_retries(
            move || {
                Ok(client
                    .get(format!(
                        "{}/b/{}/o",
                        api_base_url,
                        urlencoding::encode(&bucket)
                    ))
                    .bearer_auth(&auth)
                    .query(&query))
            },
            "list",
        )?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs list failed: {status} {body}"
            )));
        }
        let payload: GcsListObjectsResponse = response.json().map_err(internal)?;
        Ok(ObjectListPage {
            objects: payload
                .items
                .into_iter()
                .map(TryInto::try_into)
                .collect::<VfsStorageResult<Vec<_>>>()?,
            prefixes: payload.prefixes,
            next_page_token: payload.next_page_token,
        })
    }

    async fn stat_object_async(&self, key: &str) -> VfsStorageResult<Option<ObjectMetadata>> {
        let key = key.to_string();
        let client = self.async_client.clone();
        let object_url = self.object_metadata_url(&key);
        let auth = self.access_token_async().await?;
        let response = send_with_retries_async(
            move || Ok(client.get(&object_url).bearer_auth(&auth)),
            "stat",
        )
        .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs stat failed for {key}: {status} {body}"
            )));
        }
        let payload: GcsObjectMetadataResponse = response.json().await.map_err(internal)?;
        Ok(Some(payload.try_into()?))
    }

    async fn get_object_async(&self, key: &str) -> VfsStorageResult<Option<Vec<u8>>> {
        let key = key.to_string();
        let client = self.async_client.clone();
        let object_url = self.object_media_url(&key);
        let auth = self.access_token_async().await?;
        read_object_with_retries_async(
            move || Ok(client.get(&object_url).bearer_auth(&auth)),
            "read",
            &key,
        )
        .await
    }

    async fn get_object_range_async(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> VfsStorageResult<Option<Vec<u8>>> {
        let key = key.to_string();
        let client = self.async_client.clone();
        let object_url = self.object_media_url(&key);
        let auth = self.access_token_async().await?;
        let range_header = format!(
            "bytes={offset}-{}",
            offset.saturating_add(length.saturating_sub(1))
        );
        read_object_with_retries_async(
            move || {
                Ok(client
                    .get(&object_url)
                    .bearer_auth(&auth)
                    .header(reqwest::header::RANGE, range_header.clone()))
            },
            "ranged read",
            &key,
        )
        .await
    }

    async fn put_object_async(
        &self,
        key: &str,
        bytes: &[u8],
        condition: ObjectWriteCondition,
    ) -> VfsStorageResult<()> {
        let payload_len = bytes.len();
        let key = key.to_string();
        let body = bytes.to_vec();
        let request_key = key.clone();
        let client = self.async_client.clone();
        let bucket = self.cfg.bucket.clone();
        let upload_base_url = self.cfg.upload_base_url.clone();
        let auth = self
            .access_token_async()
            .instrument(info_span!("gcs_put.access_token"))
            .await?;
        let response = send_with_retries_async(
            move || {
                let mut request = client
                    .post(format!(
                        "{}/b/{}/o",
                        upload_base_url,
                        urlencoding::encode(&bucket)
                    ))
                    .bearer_auth(&auth);
                request = request.query(&[("uploadType", "media"), ("name", request_key.as_str())]);
                if condition.if_absent {
                    request = request.query(&[("ifGenerationMatch", "0")]);
                } else if let Some(generation) = condition.if_generation_match.as_deref() {
                    request = request.query(&[("ifGenerationMatch", generation)]);
                }
                if let Some(metageneration) = condition.if_metageneration_match.as_deref() {
                    request = request.query(&[("ifMetagenerationMatch", metageneration)]);
                }
                Ok(request
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .header(CONTENT_LENGTH, body.len().to_string())
                    .body(body.clone()))
            },
            "write",
        )
        .instrument(info_span!("gcs_put.http_send", payload_bytes = payload_len, key = %key))
        .await?;
        if condition.if_absent && response.status() == StatusCode::PRECONDITION_FAILED {
            tracing::info!(
                kind = "nymfs_perf",
                backend = "gcs",
                op = "write_if_missing",
                outcome = "precondition_failed",
                pack_key = %key,
                "optimistic write hit if_absent precondition (object already present)"
            );
            return Ok(());
        }
        if response.status() == StatusCode::PRECONDITION_FAILED {
            return Err(VfsStorageError::Conflict(format!(
                "gcs write precondition failed for {key}"
            )));
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = async { response.text().await.unwrap_or_default() }
                .instrument(info_span!("gcs_put.read_error_body"))
                .await;
            return Err(VfsStorageError::Internal(format!(
                "gcs write failed for {key}: {status} {body}"
            )));
        }
        Ok(())
    }

    async fn delete_object_async(
        &self,
        key: &str,
        condition: ObjectDeleteCondition,
    ) -> VfsStorageResult<()> {
        let key = key.to_string();
        let client = self.async_client.clone();
        let object_url = self.object_metadata_url(&key);
        let auth = self.access_token_async().await?;
        let response = send_with_retries_async(
            move || {
                let mut request = client.delete(&object_url).bearer_auth(&auth);
                if let Some(generation) = condition.if_generation_match.as_deref() {
                    request = request.query(&[("ifGenerationMatch", generation)]);
                }
                if let Some(metageneration) = condition.if_metageneration_match.as_deref() {
                    request = request.query(&[("ifMetagenerationMatch", metageneration)]);
                }
                Ok(request)
            },
            "delete",
        )
        .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        if response.status() == StatusCode::PRECONDITION_FAILED {
            return Err(VfsStorageError::Conflict(format!(
                "gcs delete precondition failed for {key}"
            )));
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs delete failed for {key}: {status} {body}"
            )));
        }
        Ok(())
    }

    async fn delete_objects_async(
        &self,
        keys: &[String],
        condition: ObjectDeleteCondition,
    ) -> VfsStorageResult<()> {
        self.batch_delete_objects_async(keys, condition).await
    }

    async fn copy_object_async(&self, from: &str, to: &str) -> VfsStorageResult<()> {
        let from = from.to_string();
        let to = to.to_string();
        let request_from = from.clone();
        let request_to = to.clone();
        let client = self.async_client.clone();
        let api_base_url = self.cfg.api_base_url.clone();
        let bucket = self.cfg.bucket.clone();
        let auth = self.access_token_async().await?;
        let response = send_with_retries_async(
            move || {
                Ok(client
                    .post(format!(
                        "{}/b/{}/o/{}/copyTo/b/{}/o/{}",
                        api_base_url,
                        urlencoding::encode(&bucket),
                        urlencoding::encode(&request_from),
                        urlencoding::encode(&bucket),
                        urlencoding::encode(&request_to)
                    ))
                    .bearer_auth(&auth))
            },
            "copy",
        )
        .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs copy failed from {from} to {to}: {status} {body}"
            )));
        }
        Ok(())
    }

    async fn list_objects_async(
        &self,
        request: ObjectListRequest,
    ) -> VfsStorageResult<ObjectListPage> {
        let mut query: Vec<(&str, String)> = vec![("prefix", request.prefix)];
        if let Some(delimiter) = request.delimiter {
            query.push(("delimiter", delimiter));
        }
        if let Some(max_results) = request.max_results {
            query.push(("maxResults", max_results.to_string()));
        }
        if let Some(page_token) = request.page_token {
            query.push(("pageToken", page_token));
        }
        let client = self.async_client.clone();
        let api_base_url = self.cfg.api_base_url.clone();
        let bucket = self.cfg.bucket.clone();
        let auth = self.access_token_async().await?;
        let response = send_with_retries_async(
            move || {
                Ok(client
                    .get(format!(
                        "{}/b/{}/o",
                        api_base_url,
                        urlencoding::encode(&bucket)
                    ))
                    .bearer_auth(&auth)
                    .query(&query))
            },
            "list",
        )
        .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs list failed: {status} {body}"
            )));
        }
        let payload: GcsListObjectsResponse = response.json().await.map_err(internal)?;
        Ok(ObjectListPage {
            objects: payload
                .items
                .into_iter()
                .map(TryInto::try_into)
                .collect::<VfsStorageResult<Vec<_>>>()?,
            prefixes: payload.prefixes,
            next_page_token: payload.next_page_token,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct GcsServiceAccountKey {
    client_email: String,
    private_key: String,
    #[serde(default)]
    token_uri: Option<String>,
}

#[derive(Debug, Serialize)]
struct GoogleServiceAccountClaims {
    iss: String,
    scope: String,
    aud: String,
    iat: i64,
    exp: i64,
}

#[derive(Debug, Clone)]
struct CachedAccessToken {
    access_token: String,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct GoogleAccessTokenResponse {
    access_token: String,
    expires_in: i64,
}

#[derive(Debug, Deserialize)]
struct GcsObjectMetadataResponse {
    name: String,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    generation: Option<String>,
    #[serde(default)]
    metageneration: Option<String>,
    #[serde(default)]
    updated: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct GcsListObjectsResponse {
    #[serde(default)]
    items: Vec<GcsObjectMetadataResponse>,
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

impl TryFrom<GcsObjectMetadataResponse> for ObjectMetadata {
    type Error = VfsStorageError;

    fn try_from(value: GcsObjectMetadataResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            key: value.name,
            size_bytes: parse_size_bytes(value.size.as_deref())?,
            generation: value.generation,
            metageneration: value.metageneration,
            updated: value.updated,
        })
    }
}

fn parse_size_bytes(size: Option<&str>) -> VfsStorageResult<u64> {
    size.unwrap_or("0").parse::<u64>().map_err(internal)
}

fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn should_retry_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request() || error.is_body()
}

fn retry_delay(attempt: usize) -> Duration {
    let exp = GCS_HTTP_RETRY_BASE_DELAY_MS.saturating_mul(1u64 << attempt);
    let bounded = exp.min(GCS_HTTP_RETRY_MAX_DELAY_MS);
    let jitter_window = bounded.saturating_mul(GCS_HTTP_RETRY_JITTER_PCT) / 100;
    let jitter = if jitter_window == 0 {
        0
    } else {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
        nanos % jitter_window
    };
    Duration::from_millis(bounded.saturating_sub(jitter))
}

fn send_with_retries<F>(build: F, operation: &str) -> VfsStorageResult<Response>
where
    F: Fn() -> VfsStorageResult<RequestBuilder>,
{
    for attempt in 0..GCS_HTTP_MAX_ATTEMPTS {
        let request = build()?;
        match request.send() {
            Ok(response)
                if attempt + 1 < GCS_HTTP_MAX_ATTEMPTS
                    && should_retry_status(response.status()) =>
            {
                std::thread::sleep(retry_delay(attempt));
            }
            Ok(response) => return Ok(response),
            Err(error) if attempt + 1 < GCS_HTTP_MAX_ATTEMPTS && should_retry_error(&error) => {
                std::thread::sleep(retry_delay(attempt));
            }
            Err(error) => {
                return Err(VfsStorageError::Internal(format!(
                    "gcs {operation} request failed: {error}"
                )));
            }
        }
    }

    Err(VfsStorageError::Internal(format!(
        "gcs {operation} request exceeded retry budget"
    )))
}

async fn send_with_retries_async<F>(build: F, operation: &str) -> VfsStorageResult<AsyncResponse>
where
    F: Fn() -> VfsStorageResult<AsyncRequestBuilder>,
{
    for attempt in 0..GCS_HTTP_MAX_ATTEMPTS {
        let request = build()?;
        match request.send().await {
            Ok(response)
                if attempt + 1 < GCS_HTTP_MAX_ATTEMPTS
                    && should_retry_status(response.status()) =>
            {
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Ok(response) => return Ok(response),
            Err(error) if attempt + 1 < GCS_HTTP_MAX_ATTEMPTS && should_retry_error(&error) => {
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(error) => {
                return Err(VfsStorageError::Internal(format!(
                    "gcs {operation} request failed: {error}"
                )));
            }
        }
    }

    Err(VfsStorageError::Internal(format!(
        "gcs {operation} request exceeded retry budget"
    )))
}

/// Reads an object's bytes, `None` when it does not exist. The body is part of the attempt:
/// a response can arrive and then stall past the client timeout mid-body, which reqwest
/// reports as "error decoding response body", and one such pack read used to fail a whole
/// `read_many` and the turn waiting on it. Send, status and body share one retry budget.
async fn read_object_with_retries_async<F>(
    build: F,
    operation: &str,
    key: &str,
) -> VfsStorageResult<Option<Vec<u8>>>
where
    F: Fn() -> VfsStorageResult<AsyncRequestBuilder>,
{
    for attempt in 0..GCS_HTTP_MAX_ATTEMPTS {
        let last = attempt + 1 == GCS_HTTP_MAX_ATTEMPTS;
        let response = match build()?.send().await {
            Ok(response) if !last && should_retry_status(response.status()) => {
                tokio::time::sleep(retry_delay(attempt)).await;
                continue;
            }
            Ok(response) => response,
            Err(error) if !last && should_retry_error(&error) => {
                tokio::time::sleep(retry_delay(attempt)).await;
                continue;
            }
            Err(error) => {
                return Err(VfsStorageError::Internal(format!(
                    "gcs {operation} request failed for {key}: {error}"
                )));
            }
        };
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(VfsStorageError::Internal(format!(
                "gcs {operation} failed for {key}: {status} {body}"
            )));
        }
        match response.bytes().await {
            Ok(bytes) => return Ok(Some(bytes.to_vec())),
            Err(error) if !last && should_retry_error(&error) => {
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(error) => {
                return Err(VfsStorageError::Internal(format!(
                    "gcs {operation} body failed for {key}: {error}"
                )));
            }
        }
    }
    unreachable!("the last attempt always returns")
}

fn internal<E: std::fmt::Display>(error: E) -> VfsStorageError {
    VfsStorageError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    /// Serves `body`, but the first `stalls` connections send half of it and go quiet.
    async fn stalling_server(body: &'static [u8], stalls: usize) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/object", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let seen = connections.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let index = seen.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut request = [0u8; 1024];
                    let _ = socket.read(&mut request).await;
                    let head = format!(
                        "HTTP/1.1 206 Partial Content\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    socket.write_all(head.as_bytes()).await.unwrap();
                    if index < stalls {
                        socket.write_all(&body[..body.len() / 2]).await.unwrap();
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    } else {
                        socket.write_all(body).await.unwrap();
                    }
                });
            }
        });
        (url, connections)
    }

    fn client() -> AsyncClient {
        AsyncClient::builder()
            .timeout(Duration::from_millis(300))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn a_body_that_stalls_partway_is_read_again() {
        let (url, connections) = stalling_server(b"pack slot bytes", 1).await;
        let client = client();
        let bytes = read_object_with_retries_async(|| Ok(client.get(&url)), "ranged read", "pack")
            .await
            .unwrap();
        assert_eq!(bytes.as_deref(), Some(&b"pack slot bytes"[..]));
        assert_eq!(connections.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_body_that_never_arrives_fails_after_the_budget() {
        let (url, connections) = stalling_server(b"pack slot bytes", usize::MAX).await;
        let client = client();
        let error = read_object_with_retries_async(|| Ok(client.get(&url)), "read", "pack")
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("gcs read body failed for pack"),
            "{error}"
        );
        assert_eq!(connections.load(Ordering::SeqCst), GCS_HTTP_MAX_ATTEMPTS);
    }
}
