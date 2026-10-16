//! Spec 86 D: the NymFS daemon's control socket.
//!
//! A one-line request, a one-line answer:
//!
//! - `reconnect`: drop the change stream and open a new one now. The API's pre-command barrier
//!   sends it after a pause, when a connection held across the pause may be dead without
//!   either end knowing.
//! - `sync <seq> <timeout_ms>`: wait until the feed is applied through `seq` (the
//!   read-your-writes barrier run from inside the guest).
//! - `applied`: the highest applied `seq`.
//!
//! Answers are `ok <applied>` or `timeout <applied>`. The socket takes requests from any guest
//! account (the API's barrier pokes it as the session user): reconnecting and waiting change
//! nothing a guest could not already observe. What a guest could abuse is the daemon's memory
//! and tasks, so a request line is capped at `MAX_REQUEST_BYTES` and at most
//! `MAX_CONCURRENT_REQUESTS` are answered at once; further connections are closed unanswered.
//! A connection that sends no request line within `REQUEST_READ_TIMEOUT` is closed, so idle
//! connections cannot hold every slot and shut out the API's `reconnect`.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;

use super::change_stream::FollowControl;

pub const DEFAULT_CONTROL_SOCKET: &str = "/run/chevalier-vfs-fuse.sock";
const MAX_SYNC_WAIT: Duration = Duration::from_secs(30);
/// Longest request line read; the longest real request (`sync <i64> <u64>`) is under 50 bytes.
const MAX_REQUEST_BYTES: u64 = 256;
/// Requests answered at once (a `sync` can wait up to `MAX_SYNC_WAIT`).
const MAX_CONCURRENT_REQUESTS: usize = 16;
/// How long a connection may take to send its request line (the real clients send it at once).
const REQUEST_READ_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(2)
};

/// The control socket of a mount scoped below the Nym's root: next to the root's, named by
/// the mount's tag, so starting it never removes the root daemon's socket.
pub fn scoped_socket_path(root_socket: &Path, tag: &str) -> PathBuf {
    let tag: String = tag
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    let mut name = root_socket
        .file_stem()
        .map(|stem| stem.to_string_lossy().to_string())
        .unwrap_or_else(|| "chevalier-vfs-fuse".to_string());
    name.push('-');
    name.push_str(&tag);
    name.push_str(".sock");
    root_socket.with_file_name(name)
}

/// Serve the control socket at `path` until the returned task is aborted.
pub fn serve(path: &Path, control: Arc<FollowControl>) -> Result<JoinHandle<()>> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("remove stale socket {}", path.display()));
        }
    }
    let listener =
        UnixListener::bind(path).with_context(|| format!("bind control socket {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))
        .with_context(|| format!("open control socket {} to guest accounts", path.display()))?;
    let path = path.to_path_buf();
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_REQUESTS));
    let limiter = Arc::new(ReconnectLimiter::default());
    Ok(tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                        tracing::debug!("vfs control socket busy; closing a connection unanswered");
                        drop(stream);
                        continue;
                    };
                    let control = Arc::clone(&control);
                    let limiter = Arc::clone(&limiter);
                    tokio::spawn(async move {
                        let _permit = permit;
                        if let Err(error) = answer(stream, &control, &limiter).await {
                            tracing::debug!(%error, "vfs control request failed");
                        }
                    });
                }
                Err(error) => {
                    tracing::warn!(socket = %path.display(), %error, "vfs control socket accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }))
}

/// At most one honoured `reconnect` per `MIN_RECONNECT_INTERVAL`: each one opens a new
/// gateway stream (auth and database reads on the API), and a guest process could loop it.
/// The API's barrier pokes at most once per command, far below this.
#[derive(Default)]
struct ReconnectLimiter {
    last: std::sync::Mutex<Option<std::time::Instant>>,
}

const MIN_RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

impl ReconnectLimiter {
    fn admit(&self, now: std::time::Instant) -> bool {
        let mut last = self.last.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if last.is_some_and(|at| now.duration_since(at) < MIN_RECONNECT_INTERVAL) {
            return false;
        }
        *last = Some(now);
        true
    }
}

async fn answer(
    stream: UnixStream,
    control: &FollowControl,
    limiter: &ReconnectLimiter,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    tokio::time::timeout(
        REQUEST_READ_TIMEOUT,
        BufReader::new(reader.take(MAX_REQUEST_BYTES)).read_line(&mut line),
    )
    .await
    .map_err(|_| anyhow!("no request line within {REQUEST_READ_TIMEOUT:?}"))??;
    let mut words = line.split_whitespace();
    let reply = match (words.next(), words.next(), words.next()) {
        (Some("reconnect"), _, _) => {
            // Within the interval the request is answered but folded into the last one,
            // whose fresh stream is opening anyway.
            if limiter.admit(std::time::Instant::now()) {
                control.request_reconnect();
            }
            format!("ok {}", control.applied())
        }
        (Some("applied"), _, _) => format!("ok {}", control.applied()),
        (Some("sync"), Some(seq), timeout) => {
            let seq: i64 = seq.parse().context("sync needs a seq")?;
            let timeout = timeout
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_millis(1_500))
                .min(MAX_SYNC_WAIT);
            if control.wait_applied(seq, timeout).await {
                format!("ok {}", control.applied())
            } else {
                format!("timeout {}", control.applied())
            }
        }
        _ => "error unknown request".to_string(),
    };
    writer.write_all(format!("{reply}\n").as_bytes()).await?;
    Ok(())
}

/// Send one request to the daemon at `path` and return its answer.
pub async fn request(path: &Path, request: &str) -> Result<String> {
    let stream = UnixStream::connect(path)
        .await
        .with_context(|| format!("connect to the vfs control socket {}", path.display()))?;
    let (reader, mut writer) = stream.into_split();
    writer.write_all(format!("{request}\n").as_bytes()).await?;
    let mut line = String::new();
    BufReader::new(reader).read_line(&mut line).await?;
    let line = line.trim().to_string();
    if line.is_empty() {
        return Err(anyhow!("the vfs daemon closed the control socket without answering"));
    }
    Ok(line)
}

pub fn socket_path_from_env() -> PathBuf {
    std::env::var("CHEVALIER_VFS_CONTROL_SOCKET")
        .ok()
        .filter(|path| !path.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONTROL_SOCKET))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scoped_mount_gets_its_own_socket_next_to_the_roots() {
        let root = Path::new("/run/chevalier-vfs-fuse.sock");
        assert_eq!(
            scoped_socket_path(root, "nymfs-skills"),
            PathBuf::from("/run/chevalier-vfs-fuse-nymfs-skills.sock")
        );
        assert_eq!(
            scoped_socket_path(root, "../x y"),
            PathBuf::from("/run/chevalier-vfs-fuse-___x_y.sock")
        );
        assert_ne!(scoped_socket_path(root, "nymfs-skills"), root);
    }

    #[test]
    fn reconnects_are_folded_to_one_per_interval() {
        let limiter = ReconnectLimiter::default();
        let start = std::time::Instant::now();
        assert!(limiter.admit(start));
        assert!(!limiter.admit(start + Duration::from_millis(10)));
        assert!(!limiter.admit(start + Duration::from_millis(900)));
        assert!(limiter.admit(start + Duration::from_millis(1_100)));
    }

    #[tokio::test]
    async fn an_overlong_request_line_is_cut_off_not_buffered() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.sock");
        let task = serve(&path, FollowControl::new()).unwrap();
        // A line with no newline far past the cap: the daemon reads at most the cap and answers.
        let stream = UnixStream::connect(&path).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let junk = vec![b'a'; 1 << 20];
        let _ = writer.write_all(&junk).await;
        let mut line = String::new();
        let read = tokio::time::timeout(
            Duration::from_secs(5),
            BufReader::new(reader).read_line(&mut line),
        )
        .await
        .expect("the daemon answers instead of reading forever");
        assert!(read.is_ok());
        assert_eq!(line.trim(), "error unknown request");
        // A well-formed request still works afterwards.
        assert!(request(&path, "applied").await.unwrap().starts_with("ok"));
        task.abort();
    }

    /// Connections that never send a request give their slots back, so the API's poke is
    /// still answered after a guest opened every slot and went quiet.
    #[tokio::test]
    async fn idle_connections_cannot_shut_out_a_request() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.sock");
        let task = serve(&path, FollowControl::new()).unwrap();
        let mut idle = Vec::new();
        for _ in 0..MAX_CONCURRENT_REQUESTS {
            idle.push(UnixStream::connect(&path).await.unwrap());
        }
        // Every slot is held: a request now is closed unanswered.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(request(&path, "applied").await.is_err());
        // Past the read deadline the idle connections are closed and their slots freed.
        tokio::time::sleep(REQUEST_READ_TIMEOUT + Duration::from_millis(200)).await;
        assert!(request(&path, "reconnect").await.unwrap().starts_with("ok"));
        drop(idle);
        task.abort();
    }
}
