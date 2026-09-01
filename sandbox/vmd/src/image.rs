// @dive-file: Base image resolution and prebuilt image download pipeline for VM creation.
// @dive-rel: Used by vmd manager create-vm flows and warm-pool image provisioning paths.
// @dive-rel: Handles registry URL resolution, resumable downloads, and local qcow2 placement.

use std::{
    env,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result, anyhow};
use reqwest::{Client, StatusCode, header::RANGE};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use crate::config::{self, Config};

pub const BASE_IMAGE_EXT: &str = ".qcow2";
pub const BASE_IMAGE_SIZE_GB: i32 = 10;
pub const DEFAULT_VM_REGISTRY_URL: &str = "https://vm-images.openbracket.dev";
const DEFAULT_PREBUILT_IMAGE_NAMESPACE: &str = "ghcr.io/bracketdevelopers/";
const MAX_DOWNLOAD_ATTEMPTS: usize = 5;
const DOWNLOAD_LOCK_STALE_AFTER: Duration = Duration::from_secs(30 * 60);
const DOWNLOAD_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SHA256_HEX_LEN: usize = 64;

pub fn base_image_file_name(reference: &str, arch: &str) -> String {
    let sanitized = sanitize_image_reference(reference);
    format!("{sanitized}-{arch}{BASE_IMAGE_EXT}")
}

pub fn default_base_image_path(cfg: &Config, reference: &str, arch: &str) -> PathBuf {
    PathBuf::from(&cfg.data_dir)
        .join(config::BASE_IMAGES_DIR_NAME)
        .join(base_image_file_name(reference, arch))
}

pub fn sanitize_image_reference(reference: &str) -> String {
    reference
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub fn vm_registry_base_url() -> String {
    match env::var("BRACKET_VM_REGISTRY_URL") {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                DEFAULT_VM_REGISTRY_URL.to_string()
            } else {
                trimmed.trim_end_matches('/').to_string()
            }
        }
        Err(_) => DEFAULT_VM_REGISTRY_URL.to_string(),
    }
}

fn prebuilt_registry_base_url(reference: &str) -> Option<String> {
    let configured = env::var("BRACKET_VM_REGISTRY_URL").ok();
    prebuilt_registry_base_url_for(reference, configured.as_deref())
}

fn prebuilt_registry_base_url_for(reference: &str, configured: Option<&str>) -> Option<String> {
    match configured {
        Some(value) if !value.trim().is_empty() => {
            Some(value.trim().trim_end_matches('/').to_string())
        }
        _ if reference.starts_with(DEFAULT_PREBUILT_IMAGE_NAMESPACE) => {
            Some(DEFAULT_VM_REGISTRY_URL.to_string())
        }
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub struct DownloadProgress {
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub enum PrebuiltImageStatus {
    Downloaded { bytes: u64 },
    NotFound,
}

enum ExpectedImageDigest {
    Available(String),
    Missing,
    Invalid,
}

struct DownloadLock {
    path: PathBuf,
}

impl Drop for DownloadLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub async fn fetch_prebuilt_image(reference: &str, arch: &str, target: &Path) -> Result<bool> {
    match download_prebuilt_image(reference, arch, target).await {
        Ok(PrebuiltImageStatus::Downloaded { .. }) => Ok(true),
        Ok(PrebuiltImageStatus::NotFound) => Ok(false),
        Err(_) => Ok(false),
    }
}

pub async fn download_prebuilt_image(
    reference: &str,
    arch: &str,
    target: &Path,
) -> Result<PrebuiltImageStatus> {
    download_prebuilt_image_with_progress(reference, arch, target, |_| {}).await
}

pub async fn download_prebuilt_image_with_progress<F>(
    reference: &str,
    arch: &str,
    target: &Path,
    mut progress: F,
) -> Result<PrebuiltImageStatus>
where
    F: FnMut(DownloadProgress),
{
    let parent = target
        .parent()
        .context("determine directory for base image download")?;

    let Some(registry_base_url) = prebuilt_registry_base_url(reference) else {
        debug!(
            %reference,
            "image is outside the default prebuilt namespace; falling back to local conversion"
        );
        return Ok(PrebuiltImageStatus::NotFound);
    };
    let file_name = base_image_file_name(reference, arch);
    let url = format!("{registry_base_url}/{file_name}");
    let client = Client::new();
    debug!(%url, path = %target.display(), "attempting to download prebuilt VM image");
    let expected_digest = match fetch_expected_image_digest(&client, &url).await? {
        ExpectedImageDigest::Available(digest) => Some(digest),
        ExpectedImageDigest::Missing => None,
        ExpectedImageDigest::Invalid => {
            warn!(%url, "prebuilt VM image has an invalid digest sidecar; falling back to local conversion");
            return Ok(PrebuiltImageStatus::NotFound);
        }
    };

    let tmp_name = format!(
        "{}.part",
        target
            .file_name()
            .and_then(|segment| segment.to_str())
            .unwrap_or("image")
    );
    let tmp_path = parent.join(tmp_name);
    let _download_lock = acquire_download_lock(target).await?;
    if let Ok(metadata) = fs::metadata(target).await {
        return Ok(PrebuiltImageStatus::Downloaded {
            bytes: metadata.len(),
        });
    }

    let mut attempt = 0usize;
    let mut last_err = None;

    'attempt: loop {
        if attempt >= MAX_DOWNLOAD_ATTEMPTS {
            break;
        }
        attempt += 1;

        let resume_from = match fs::metadata(&tmp_path).await {
            Ok(metadata) => metadata.len(),
            Err(err) if err.kind() == ErrorKind::NotFound => 0,
            Err(err) => {
                warn!(path = %tmp_path.display(), error = %err, "failed to inspect partial download");
                return Err(err.into());
            }
        };

        let mut request = client.get(&url);
        if resume_from > 0 {
            // @dive: Resume via HTTP range keeps large prebuilt downloads incremental across retries.
            request = request.header(RANGE, format!("bytes={resume_from}-"));
        }

        let response = match request.send().await {
            Ok(resp) => resp,
            Err(err) => {
                warn!(
                    %url,
                    attempt,
                    error = %err,
                    "unable to contact prebuilt VM image registry"
                );
                last_err = Some(err.into());
                continue;
            }
        };

        match response.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {}
            StatusCode::NOT_FOUND => {
                debug!(%url, "no prebuilt VM image available");
                let _ = fs::remove_file(&tmp_path).await;
                return Ok(PrebuiltImageStatus::NotFound);
            }
            status => {
                warn!(
                    %url,
                    status = ?status,
                    "prebuilt VM image registry returned unexpected status"
                );
                return Err(anyhow!("unexpected status {status} from VM registry"));
            }
        }

        let resuming = response.status() == StatusCode::PARTIAL_CONTENT && resume_from > 0;
        if resume_from > 0 && !resuming {
            debug!(
                %url,
                attempt,
                "VM registry did not honor range request; restarting download"
            );
        }
        let mut hasher = Sha256::new();
        if resuming {
            if let Err(err) = hash_file_into(&tmp_path, &mut hasher).await {
                warn!(
                    attempt,
                    path = %tmp_path.display(),
                    error = %err,
                    "failed to hash partial VM image before resume"
                );
                last_err = Some(err);
                let _ = fs::remove_file(&tmp_path).await;
                continue 'attempt;
            }
        }
        // @dive: If server ignores range, we deliberately truncate local partial state to avoid mixed image fragments.
        let mut open_opts = OpenOptions::new();
        open_opts.write(true).create(true);
        if resuming {
            open_opts.append(true);
        } else {
            open_opts.truncate(true);
        }

        let mut file = match open_opts.open(&tmp_path).await {
            Ok(file) => file,
            Err(err) => {
                warn!(path = %tmp_path.display(), error = %err, "cannot open temp file for download");
                return Err(err.into());
            }
        };

        let mut written = if resuming { resume_from } else { 0 };
        let total = response.content_length().map(|length| length + written);
        progress(DownloadProgress {
            downloaded_bytes: written,
            total_bytes: total,
        });

        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(err) => {
                    warn!(
                        attempt,
                        error = %err,
                        "failed to read chunk from remote VM image"
                    );
                    // @dive: Chunk errors restart the full attempt so callers never consume partially-corrupted image tails.
                    last_err = Some(err.into());
                    continue 'attempt;
                }
            };

            if let Err(err) = file.write_all(&bytes).await {
                warn!(
                    attempt,
                    path = %tmp_path.display(),
                    error = %err,
                    "failed to write remote VM chunk"
                );
                last_err = Some(err.into());
                continue 'attempt;
            }
            hasher.update(&bytes);
            written += bytes.len() as u64;
            progress(DownloadProgress {
                downloaded_bytes: written,
                total_bytes: total,
            });
        }

        if let Err(err) = file.flush().await {
            warn!(
                attempt,
                path = %tmp_path.display(),
                error = %err,
                "failed to flush remote VM download"
            );
            last_err = Some(err.into());
            continue 'attempt;
        }

        let actual_digest = hex_encode(&hasher.finalize());
        if let Some(expected_digest) = expected_digest.as_deref() {
            if actual_digest != expected_digest {
                warn!(
                    attempt,
                    path = %tmp_path.display(),
                    expected_digest,
                    actual_digest,
                    "downloaded VM image digest mismatch"
                );
                let _ = fs::remove_file(&tmp_path).await;
                last_err = Some(anyhow!(
                    "downloaded VM image digest mismatch: expected {expected_digest}, got {actual_digest}"
                ));
                continue 'attempt;
            }
        }

        if let Err(err) = fs::rename(&tmp_path, target).await {
            warn!(
                path = %tmp_path.display(),
                error = %err,
                "failed to move downloaded VM image into place"
            );
            return Err(err.into());
        }

        if let Err(err) = write_digest_sidecar(target, &actual_digest).await {
            warn!(
                path = %target.display(),
                digest = %actual_digest,
                error = %err,
                "failed writing VM image digest sidecar"
            );
        }

        // @dive: Final rename publishes image atomically; readers only see fully-written qcow2 files.
        info!(path = %target.display(), url = %url, digest = %actual_digest, "downloaded prebuilt VM image");
        return Ok(PrebuiltImageStatus::Downloaded { bytes: written });
    }

    let err = last_err.unwrap_or_else(|| anyhow!("download attempts exhausted"));
    Err(err.context(format!(
        "failed to download prebuilt VM image after {MAX_DOWNLOAD_ATTEMPTS} attempts"
    )))
}

async fn fetch_expected_image_digest(
    client: &Client,
    image_url: &str,
) -> Result<ExpectedImageDigest> {
    let digest_url = format!("{image_url}.sha256");
    let response = match client.get(&digest_url).send().await {
        Ok(response) => response,
        Err(err) => {
            debug!(
                %digest_url,
                error = %err,
                "unable to fetch VM image digest sidecar"
            );
            return Ok(ExpectedImageDigest::Missing);
        }
    };

    match response.status() {
        StatusCode::OK => {
            let body = response
                .text()
                .await
                .with_context(|| format!("read VM image digest sidecar {digest_url}"))?;
            match parse_sha256_digest(&body) {
                Ok(digest) => Ok(ExpectedImageDigest::Available(digest)),
                Err(err) => {
                    warn!(
                        %digest_url,
                        error = %err,
                        "VM image registry returned an invalid digest sidecar"
                    );
                    Ok(ExpectedImageDigest::Invalid)
                }
            }
        }
        StatusCode::NOT_FOUND => Ok(ExpectedImageDigest::Missing),
        status => {
            debug!(
                %digest_url,
                status = ?status,
                "VM image registry returned unexpected digest sidecar status"
            );
            Ok(ExpectedImageDigest::Missing)
        }
    }
}

fn parse_sha256_digest(body: &str) -> Result<String> {
    body.split_whitespace()
        .find_map(normalize_sha256_hex)
        .ok_or_else(|| anyhow!("missing sha256 digest"))
}

fn normalize_sha256_hex(value: &str) -> Option<String> {
    if value.len() == SHA256_HEX_LEN && value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(value.to_ascii_lowercase())
    } else {
        None
    }
}

async fn hash_file_into(path: &Path, hasher: &mut Sha256) -> Result<()> {
    let mut file = fs::File::open(path)
        .await
        .with_context(|| format!("open partial VM image {}", path.display()))?;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .with_context(|| format!("read partial VM image {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(())
}

async fn write_digest_sidecar(target: &Path, digest: &str) -> Result<()> {
    let file_name = target
        .file_name()
        .and_then(|segment| segment.to_str())
        .unwrap_or("image");
    let sidecar_path = digest_sidecar_path(target);
    let contents = format!("{digest}  {file_name}\n");
    let mut file = fs::File::create(&sidecar_path)
        .await
        .with_context(|| format!("create VM image digest sidecar {}", sidecar_path.display()))?;
    file.write_all(contents.as_bytes())
        .await
        .with_context(|| format!("write VM image digest sidecar {}", sidecar_path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("flush VM image digest sidecar {}", sidecar_path.display()))?;
    Ok(())
}

fn digest_sidecar_path(target: &Path) -> PathBuf {
    let file_name = target
        .file_name()
        .and_then(|segment| segment.to_str())
        .unwrap_or("image");
    target.with_file_name(format!("{file_name}.sha256"))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

async fn acquire_download_lock(target: &Path) -> Result<DownloadLock> {
    let file_name = target
        .file_name()
        .and_then(|segment| segment.to_str())
        .unwrap_or("image");
    let lock_path = target.with_file_name(format!("{file_name}.lock"));
    loop {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .await
        {
            Ok(_) => return Ok(DownloadLock { path: lock_path }),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                remove_stale_download_lock(&lock_path).await;
                tokio::time::sleep(DOWNLOAD_LOCK_POLL_INTERVAL).await;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("acquire image download lock {}", lock_path.display())
                });
            }
        }
    }
}

async fn remove_stale_download_lock(lock_path: &Path) {
    let Ok(metadata) = fs::metadata(lock_path).await else {
        return;
    };
    let Ok(modified) = metadata.modified() else {
        return;
    };
    let Ok(age) = SystemTime::now().duration_since(modified) else {
        return;
    };
    if age > DOWNLOAD_LOCK_STALE_AFTER {
        let _ = fs::remove_file(lock_path).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn parse_sha256_digest_accepts_plain_or_sha256sum_format() {
        let digest = "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789";
        assert_eq!(
            parse_sha256_digest(digest).expect("plain digest"),
            digest.to_ascii_lowercase()
        );
        assert_eq!(
            parse_sha256_digest(&format!("{digest}  image.qcow2\n")).expect("sha256sum digest"),
            digest.to_ascii_lowercase()
        );
    }

    #[test]
    fn parse_sha256_digest_rejects_missing_or_malformed_digest() {
        assert!(parse_sha256_digest("image.qcow2").is_err());
        assert!(parse_sha256_digest("abc123 image.qcow2").is_err());
    }

    #[tokio::test]
    async fn malformed_digest_sidecar_rejects_prebuilt_image() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await.expect("read request");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\nnot-a-sha",
                )
                .await
                .expect("write response");
        });

        let client = Client::new();
        let image_url = format!("http://{address}/image.qcow2");
        let status = fetch_expected_image_digest(&client, &image_url)
            .await
            .expect("digest request");

        assert!(matches!(status, ExpectedImageDigest::Invalid));
        server.await.expect("test server task");
    }

    #[test]
    fn digest_sidecar_path_appends_sha256_to_full_file_name() {
        assert_eq!(
            digest_sidecar_path(Path::new("/tmp/base.qcow2")),
            PathBuf::from("/tmp/base.qcow2.sha256")
        );
    }

    #[test]
    fn default_registry_does_not_receive_private_image_references() {
        assert!(
            prebuilt_registry_base_url_for("127.0.0.1:5000/openbracket/sandbox:latest", None)
                .is_none()
        );
        assert_eq!(
            prebuilt_registry_base_url_for("ghcr.io/bracketdevelopers/uv-builder:main", None)
                .as_deref(),
            Some(DEFAULT_VM_REGISTRY_URL)
        );
    }
}
