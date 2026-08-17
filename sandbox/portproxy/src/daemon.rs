// @dive-file: Tracks named long-lived guest-side daemon processes used for attachable command execution streams.
// @dive-rel: Consumed by portproxy/src/services.rs DaemonManagerService for exec/attach semantics.
// @dive-rel: Enables distributed stream producer reattachment by name across control-plane rebind attempts.
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use portable_pty::CommandBuilder;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{Mutex, RwLock, broadcast, watch};
use tracing::{error, info, warn};

use crate::child_tracker::ChildTracker;
use crate::pb::bracket::portproxy::v1::{ExecDaemonRequest, ExecDaemonResponse};
use crate::process_group::{configure_child_process_group, kill_process_group_or_child};
use crate::system_env::{DEFAULT_EXEC_PATH, build_exec_env, default_exec_home};

const CHANNEL_CAPACITY: usize = 100;
const BACKLOG_MAX_FRAMES: usize = 512;
/// Grace window for keeping an exited daemon's entry around so a slightly-
/// late `attach_daemon` (or one delayed by guest-network jitter) can still
/// resolve it and replay backlog + final exit code. 5 minutes is more than
/// enough to cover the longest plausible api → vmd → portproxy roundtrip
/// while still bounding the registry size on a chatty session.
const DAEMON_POST_EXIT_RETENTION: std::time::Duration = std::time::Duration::from_secs(300);

#[derive(Error, Debug)]
pub enum DaemonError {
    #[error("daemon already running")]
    AlreadyRunning,
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("spawn error: {0}")]
    Spawn(#[from] std::io::Error),
}

#[derive(Clone)]
pub struct DaemonRegistry {
    daemons: Arc<RwLock<HashMap<String, Arc<DaemonEntry>>>>,
    tracker: ChildTracker,
}

impl DaemonRegistry {
    pub fn new(tracker: ChildTracker) -> Self {
        Self {
            daemons: Arc::new(RwLock::new(HashMap::new())),
            tracker,
        }
    }

    pub async fn exec_daemon(
        &self,
        req: ExecDaemonRequest,
    ) -> Result<ExecDaemonResponse, DaemonError> {
        if req.name.is_empty() {
            return Err(DaemonError::InvalidRequest("name is required".into()));
        }
        if req.args.is_empty() {
            return Err(DaemonError::InvalidRequest("args must not be empty".into()));
        }

        {
            let daemons = self.daemons.read().await;
            if daemons.contains_key(&req.name) {
                return Ok(ExecDaemonResponse { is_new: false });
            }
        }

        let mut command = Command::new(&req.args[0]);
        if req.args.len() > 1 {
            command.args(&req.args[1..]);
        }
        command.stdin(std::process::Stdio::piped());
        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());
        command.env_clear();
        for (key, value) in build_exec_env(DEFAULT_EXEC_PATH, default_exec_home(), &req.env) {
            command.env(key, value);
        }
        configure_child_process_group(&mut command);

        let mut child = command.spawn()?;

        let pid = match child.id() {
            Some(id) => id as i32,
            None => {
                let _ = child.start_kill();
                return Err(DaemonError::Spawn(std::io::Error::other(
                    "spawned daemon without pid",
                )));
            }
        };
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                kill_process_group_or_child(pid, &mut child);
                return Err(DaemonError::InvalidRequest(
                    "failed to capture stdin".into(),
                ));
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                kill_process_group_or_child(pid, &mut child);
                return Err(DaemonError::InvalidRequest(
                    "failed to capture stdout".into(),
                ));
            }
        };
        let stderr = match child.stderr.take() {
            Some(stderr) => stderr,
            None => {
                kill_process_group_or_child(pid, &mut child);
                return Err(DaemonError::InvalidRequest(
                    "failed to capture stderr".into(),
                ));
            }
        };

        let (stdout_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (stderr_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (exit_tx, _) = watch::channel(None::<i32>);
        let output_backlog = Arc::new(Mutex::new(OutputBacklog::default()));
        let (control, mut control_receiver) = crate::exec_control::channel();

        let stdout_tx_for_reader = stdout_tx.clone();
        let stderr_tx_for_reader = stderr_tx.clone();
        let stderr_tx_for_timeout = stderr_tx.clone();
        let entry = Arc::new(DaemonEntry {
            control,
            stdin_eof: control_receiver.eof.clone(),
            _name: req.name.clone(),
            stdin: Mutex::new(Some(stdin)),
            stdout_tx: std::sync::Mutex::new(Some(stdout_tx)),
            stderr_tx: std::sync::Mutex::new(Some(stderr_tx)),
            output_backlog: output_backlog.clone(),
            attach_lock: Arc::new(Mutex::new(())),
            exit_code: std::sync::Mutex::new(None),
            exit_tx: exit_tx.clone(),
        });

        {
            let mut daemons = self.daemons.write().await;
            if daemons.contains_key(&req.name) {
                kill_process_group_or_child(pid, &mut child);
                return Err(DaemonError::AlreadyRunning);
            }
            daemons.insert(req.name.clone(), entry.clone());
        }
        let mut child_exit = self.tracker.register(pid);
        let stdin_entry = entry.clone();
        tokio::spawn(async move {
            let mut eof = stdin_entry.stdin_eof.clone();
            let mut exited = stdin_entry.exit_tx.subscribe();
            tokio::select! {
                _ = crate::exec_control::stdin_closed(&mut eof) => {},
                _ = exited.wait_for(|code| code.is_some()) => {},
            }
            *stdin_entry.stdin.lock().await = None;
        });

        spawn_reader(
            stdout,
            stdout_tx_for_reader,
            output_backlog.clone(),
            OutputKind::Stdout,
            format!("stdout({})", req.name),
        );
        spawn_reader(
            stderr,
            stderr_tx_for_reader,
            output_backlog.clone(),
            OutputKind::Stderr,
            format!("stderr({})", req.name),
        );

        let daemons = self.daemons.clone();
        let timeout = req
            .timeout
            .filter(|secs| *secs > 0)
            .map(|secs| std::time::Duration::from_secs(secs as u64));
        let post_exit_entry = entry.clone();
        tokio::spawn(async move {
            let exit_code = match timeout {
                Some(timeout) => match tokio::time::timeout(
                    timeout,
                    control_receiver.wait(pid, child_exit.wait()),
                )
                .await
                {
                    Ok(Ok(exit)) => Some(exit.protocol_code()),
                    Ok(Err(err)) => Some(daemon_wait_error_code(&req.name, err)),
                    Err(_) => {
                        let timeout_message = b"\n(Command timed out)".to_vec();
                        {
                            let mut guard = output_backlog.lock().await;
                            guard.push(OutputKind::Stderr, timeout_message.clone());
                        }
                        let _ = stderr_tx_for_timeout.send(timeout_message);
                        kill_process_group_or_child(pid, &mut child);
                        match child_exit.wait().await {
                            Ok(_) => Some(124),
                            Err(error) => {
                                error!("daemon {} termination unconfirmed: {error}", req.name);
                                None
                            }
                        }
                    }
                },
                None => match control_receiver.wait(pid, child_exit.wait()).await {
                    Ok(exit) => Some(exit.protocol_code()),
                    Err(err) => Some(daemon_wait_error_code(&req.name, err)),
                },
            };
            post_exit_entry.set_exit_code(exit_code);
            let _ = exit_tx.send(exit_code);
            info!("daemon {} exited with {:?}", req.name, exit_code);

            // @dive: Drop the broadcast Senders held by the entry so that any
            //        in-flight attach_daemon's stdout/stderr forwarders observe
            //        Closed (once spawn_reader's own clone hits EOF and drops)
            //        and break. Without this, the entry's Sender keeps the
            //        broadcast alive for the entire DAEMON_POST_EXIT_RETENTION
            //        window, starving the attach response stream from closing
            //        and producing an api-side hang up to that grace duration.
            //        Stdin is dropped here too — child stdin is closed on exit
            //        anyway, but explicitly dropping the handle releases the
            //        async Mutex so the stdin-forwarder task's next write
            //        fails fast.
            if let Ok(mut guard) = post_exit_entry.stdout_tx.lock() {
                *guard = None;
            }
            if let Ok(mut guard) = post_exit_entry.stderr_tx.lock() {
                *guard = None;
            }
            *post_exit_entry.stdin.lock().await = None;

            // @dive: Keep the entry in the registry past child exit so a
            //        slightly-late attach_daemon (e.g. for a fast `echo` that
            //        finishes in microseconds before the api's RPC fan-out
            //        completes) can still resolve the name. The fast path in
            //        attach_daemon picks up the cached exit_code from
            //        exit_tx (watch channels keep the last value after the
            //        Sender is dropped) and the buffered output from the
            //        backlog, so this lookup remains useful.
            tokio::time::sleep(DAEMON_POST_EXIT_RETENTION).await;
            {
                let mut guard = daemons.write().await;
                guard.remove(&req.name);
            }
        });

        Ok(ExecDaemonResponse { is_new: true })
    }

    pub async fn get(&self, name: &str) -> Option<Arc<DaemonEntry>> {
        let guard = self.daemons.read().await;
        guard.get(name).cloned()
    }
}

pub struct DaemonEntry {
    pub control: crate::exec_control::ExecControl,
    pub stdin_eof: watch::Receiver<bool>,
    pub(crate) _name: String,
    /// Set to `None` after the daemon's child exits so the stdin-forwarder in
    /// attach_daemon can detect the post-exit state and stop trying to write
    /// into a broken pipe.
    pub stdin: Mutex<Option<tokio::process::ChildStdin>>,
    /// `Some` while the daemon's child is running, `None` after exit. Held in
    /// a `std::sync::Mutex` (not async) so the post-exit cleanup task can
    /// drop the Sender without yielding, ensuring downstream broadcast
    /// receivers observe `Closed` promptly.
    pub stdout_tx: std::sync::Mutex<Option<broadcast::Sender<Vec<u8>>>>,
    pub stderr_tx: std::sync::Mutex<Option<broadcast::Sender<Vec<u8>>>>,
    pub output_backlog: Arc<Mutex<OutputBacklog>>,
    pub attach_lock: Arc<Mutex<()>>,
    /// Retained terminal state for late attach. watch::Sender::send returns
    /// Err without storing the value when no receivers exist, which is common
    /// for fast commands that exit before vmd has attached.
    exit_code: std::sync::Mutex<Option<i32>>,
    pub exit_tx: watch::Sender<Option<i32>>,
}

#[derive(Clone, Copy, Debug)]
pub enum OutputKind {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug)]
pub struct OutputFrame {
    pub kind: OutputKind,
    pub data: Vec<u8>,
}

#[derive(Default)]
pub struct OutputBacklog {
    frames: VecDeque<OutputFrame>,
}

impl OutputBacklog {
    fn push(&mut self, kind: OutputKind, data: Vec<u8>) {
        if self.frames.len() >= BACKLOG_MAX_FRAMES {
            let _ = self.frames.pop_front();
        }
        self.frames.push_back(OutputFrame { kind, data });
    }

    fn drain(&mut self) -> Vec<OutputFrame> {
        self.frames.drain(..).collect()
    }
}

impl DaemonEntry {
    pub async fn drain_output_backlog(&self) -> Vec<OutputFrame> {
        let mut guard = self.output_backlog.lock().await;
        guard.drain()
    }

    pub fn cached_exit_code(&self) -> Option<i32> {
        self.exit_code.lock().ok().and_then(|guard| *guard)
    }

    fn set_exit_code(&self, code: Option<i32>) {
        if let Ok(mut guard) = self.exit_code.lock() {
            *guard = code;
        }
    }
}

fn daemon_wait_error_code(name: &str, err: impl std::fmt::Display) -> i32 {
    error!("daemon {name} wait failed: {err}");
    -1
}

fn spawn_reader<R>(
    mut reader: R,
    tx: broadcast::Sender<Vec<u8>>,
    backlog: Arc<Mutex<OutputBacklog>>,
    kind: OutputKind,
    label: String,
) where
    R: tokio::io::AsyncRead + Send + std::marker::Unpin + 'static,
{
    tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let payload = buf[..n].to_vec();
                    if tx.receiver_count() == 0 {
                        let mut guard = backlog.lock().await;
                        guard.push(kind, payload.clone());
                    }
                    if tx.send(payload).is_err() {
                        // No active listeners; keep reading so buffer does not back up.
                    }
                }
                Err(err) => {
                    warn!("failed reading {}: {}", label, err);
                    break;
                }
            }
        }
    });
}

pub fn build_command_builder(
    shell: &str,
    args: &[String],
    env: &std::collections::HashMap<String, String>,
    cwd: Option<String>,
) -> CommandBuilder {
    let mut builder = CommandBuilder::new(shell);
    if !args.is_empty() {
        builder.args(args);
    }
    builder.env_clear();
    for (k, v) in build_exec_env(DEFAULT_EXEC_PATH, default_exec_home(), env) {
        builder.env(k, v);
    }
    if let Some(dir) = cwd {
        builder.cwd(PathBuf::from(dir));
    }
    builder
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;

    #[tokio::test]
    async fn output_backlog_replays_stdout_emitted_before_attach() {
        let tracker = ChildTracker::new();
        let registry = DaemonRegistry::new(tracker);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("unix time should be monotonic")
            .as_nanos();
        let daemon_name = format!("backlog-{nonce}");
        let req = ExecDaemonRequest {
            name: daemon_name.clone(),
            args: vec![
                "sh".to_string(),
                "-lc".to_string(),
                "echo preattach-output; sleep 1".to_string(),
            ],
            env: HashMap::new(),
            timeout: None,
            detach: false,
        };

        registry
            .exec_daemon(req)
            .await
            .expect("exec_daemon should start process");
        tokio::time::sleep(Duration::from_millis(200)).await;

        let entry = registry
            .get(&daemon_name)
            .await
            .expect("daemon entry should exist before process exits");
        let frames = entry.drain_output_backlog().await;

        assert!(
            frames.iter().any(|frame| {
                matches!(frame.kind, OutputKind::Stdout)
                    && String::from_utf8_lossy(&frame.data).contains("preattach-output")
            }),
            "expected stdout emitted before attach to be retained in backlog"
        );
    }
}
