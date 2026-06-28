// @dive-file: Implements gRPC PortProxy, ShellExec, and DaemonManager services used inside guest VMs.
// @dive-rel: Uses portproxy/src/daemon.rs to provide named daemon exec streams that can be reattached.
// @dive-rel: Conforms to proto/bracket/portproxy/v1/portproxy.proto service contracts.
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::ExitStatus;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use portable_pty::PtySize;
use tokio::fs;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, warn};

use crate::child_tracker::ChildTracker;
use crate::daemon::{DaemonError, DaemonRegistry, OutputKind, build_command_builder};
use crate::pb::bracket::portproxy::v1::daemon_manager_server::DaemonManager;
use crate::pb::bracket::portproxy::v1::port_proxy_server::PortProxy;
use crate::pb::bracket::portproxy::v1::shell_exec_server::ShellExec;
use crate::pb::bracket::portproxy::v1::{
    AttachDaemonRequest, AttachDaemonResponse, DeletePathRequest, DirectoryEntry, ExecRequest,
    ExecResponse, ExecStart, InteractiveShellRequest, InteractiveShellResponse,
    ListDirectoryRequest, ListDirectoryResponse, ReadFileRequest, ReadFileResponse,
    WriteFileRequest,
};
use crate::pb::bracket::portproxy::v1::{ExecDaemonRequest, ExecDaemonResponse};
use crate::pb::google::protobuf::Empty;
use crate::process_group::{
    configure_child_process_group, kill_process_group_or_child, signal_process_group_or_pid,
};
use crate::system_env::build_exec_env;
use nix::sys::signal::Signal;

type ExecResponseStream = ReceiverStream<Result<ExecResponse, Status>>;
type InteractiveResponseStream = ReceiverStream<Result<InteractiveShellResponse, Status>>;
type AttachDaemonResponseStream =
    GuardedStream<ReceiverStream<Result<AttachDaemonResponse, Status>>>;

const DEFAULT_EXEC_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const DEFAULT_EXEC_HOME: &str = "/root";

pub struct GuardedStream<S> {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    inner: S,
}

impl<S> GuardedStream<S> {
    fn new(guard: tokio::sync::OwnedMutexGuard<()>, inner: S) -> Self {
        Self {
            _guard: guard,
            inner,
        }
    }
}

impl<S> Stream for GuardedStream<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll_next(cx)
    }
}

const READ_BUFFER: usize = 4096;
const MAX_FILE_RPC_BYTES: usize = 16 * 1024 * 1024;
const MAX_DIRECTORY_ENTRIES: usize = 10_000;

#[derive(Clone)]
pub struct ShellExecService {
    tracker: ChildTracker,
}

impl ShellExecService {
    pub fn new(tracker: ChildTracker) -> Self {
        Self { tracker }
    }
}

#[tonic::async_trait]
impl ShellExec for ShellExecService {
    type ExecStream = ExecResponseStream;
    type InteractiveShellStream = InteractiveResponseStream;

    async fn exec(
        &self,
        request: Request<tonic::Streaming<ExecRequest>>,
    ) -> Result<Response<Self::ExecStream>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("stream ended before ExecStart"))?;

        let ExecRequest {
            request: Some(crate::pb::bracket::portproxy::v1::exec_request::Request::Start(start)),
        } = first
        else {
            return Err(Status::invalid_argument("first message must be ExecStart"));
        };

        let args = validate_args(&start)?;

        let mut command = Command::new(&args[0]);
        if args.len() > 1 {
            command.args(&args[1..]);
        }
        command.stdin(std::process::Stdio::piped());
        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());
        command.env_clear();
        for (key, value) in build_exec_env(DEFAULT_EXEC_PATH, DEFAULT_EXEC_HOME, &start.env) {
            command.env(key, value);
        }
        configure_child_process_group(&mut command);

        let mut child = command
            .spawn()
            .map_err(|err| Status::internal(format!("failed to start command: {err}")))?;

        let pid = match child.id() {
            Some(pid) => pid as i32,
            None => {
                let _ = child.start_kill();
                return Err(Status::internal("spawned command without pid"));
            }
        };
        let mut stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                kill_process_group_or_child(pid, &mut child);
                return Err(Status::internal("missing stdin handle"));
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                kill_process_group_or_child(pid, &mut child);
                return Err(Status::internal("missing stdout handle"));
            }
        };
        let stderr = match child.stderr.take() {
            Some(stderr) => stderr,
            None => {
                kill_process_group_or_child(pid, &mut child);
                return Err(Status::internal("missing stderr handle"));
            }
        };
        let timeout = start.timeout.map(|secs| Duration::from_secs(secs as u64));

        let (tx, rx) = mpsc::channel(32);

        let stdout_reader = spawn_reader(stdout, tx.clone(), |data| ExecResponse {
            response: Some(
                crate::pb::bracket::portproxy::v1::exec_response::Response::StdoutData(data),
            ),
        });
        let stderr_reader = spawn_reader(stderr, tx.clone(), |data| ExecResponse {
            response: Some(
                crate::pb::bracket::portproxy::v1::exec_response::Response::StderrData(data),
            ),
        });

        tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(Some(ExecRequest {
                        request:
                            Some(crate::pb::bracket::portproxy::v1::exec_request::Request::StdinData(
                                data,
                            )),
                    })) => {
                        if let Err(err) = stdin.write_all(&data).await {
                            debug!("failed writing to stdin: {err}");
                            break;
                        }
                    }
                    Ok(Some(_)) => {
                        debug!("ignoring unexpected exec message");
                    }
                    Ok(None) => break,
                    Err(err) => {
                        debug!("stdin stream error: {err}");
                        break;
                    }
                }
            }
            let _ = stdin.shutdown().await;
        });

        tokio::spawn(async move {
            if let Some(duration) = timeout {
                match tokio::time::timeout(duration, child.wait()).await {
                    Ok(Ok(exit)) => {
                        drain_exec_readers(stdout_reader, stderr_reader).await;
                        finalize_exec_status(pid, tx, exit).await;
                    }
                    Ok(Err(err)) => finalize_exec_wait_error(pid, tx, err).await,
                    Err(_) => {
                        debug!("command timed out, killing process group for pid {pid}");
                        kill_process_group_or_child(pid, &mut child);
                        drain_exec_readers(stdout_reader, stderr_reader).await;
                        let _ = tx
                            .send(Ok(ExecResponse {
                                response: Some(
                                    crate::pb::bracket::portproxy::v1::exec_response::Response::StderrData(
                                        b"\n(Command timed out)".to_vec(),
                                    ),
                                ),
                            }))
                            .await;

                        let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
                        finalize_exec_with_code(pid, tx, 124).await;
                    }
                }
            } else {
                match child.wait().await {
                    Ok(exit) => {
                        drain_exec_readers(stdout_reader, stderr_reader).await;
                        finalize_exec_status(pid, tx, exit).await;
                    }
                    Err(err) => finalize_exec_wait_error(pid, tx, err).await,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn interactive_shell(
        &self,
        request: Request<tonic::Streaming<InteractiveShellRequest>>,
    ) -> Result<Response<Self::InteractiveShellStream>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("stream ended before start"))?;

        let InteractiveShellRequest {
            request:
                Some(crate::pb::bracket::portproxy::v1::interactive_shell_request::Request::Start(
                    start,
                )),
        } = first
        else {
            return Err(Status::invalid_argument(
                "first message must be InteractiveShellStart",
            ));
        };

        let shell = if start.shell.is_empty() {
            "/bin/sh".to_string()
        } else {
            start.shell.clone()
        };

        let pty_system = portable_pty::native_pty_system();
        let pair = pty_system
            .openpty(Default::default())
            .map_err(|err| Status::internal(format!("failed to open pty: {err}")))?;

        let builder = build_command_builder(&shell, &start.args, &start.env, {
            if start.cwd.is_empty() {
                None
            } else {
                Some(start.cwd.clone())
            }
        });

        let child = pair
            .slave
            .spawn_command(builder)
            .map_err(|err| Status::internal(format!("failed to spawn interactive shell: {err}")))?;

        let mut killer = child.clone_killer();
        let pid = child.process_id().unwrap_or(0) as i32;
        if pid <= 0 {
            let _ = killer.kill();
            return Err(Status::internal("spawned interactive shell without pid"));
        }

        let mut reader = match pair.master.try_clone_reader() {
            Ok(reader) => reader,
            Err(err) => {
                let _ = killer.kill();
                return Err(Status::internal(format!(
                    "failed to clone pty reader: {err}"
                )));
            }
        };
        let writer = match pair.master.take_writer() {
            Ok(writer) => writer,
            Err(err) => {
                let _ = killer.kill();
                return Err(Status::internal(format!(
                    "failed to take pty writer: {err}"
                )));
            }
        };

        let killer = Arc::new(StdMutex::new(Some(killer)));
        let mut child_exit = self.tracker.register(pid);

        let (tx, rx) = mpsc::channel(32);

        tokio::task::spawn_blocking({
            let tx = tx.clone();
            move || {
                let mut buf = vec![0u8; READ_BUFFER];
                loop {
                    match reader.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            if tx
                                .blocking_send(Ok(InteractiveShellResponse {
                                    response: Some(
                                        crate::pb::bracket::portproxy::v1::interactive_shell_response::Response::OutputData(
                                            buf[..n].to_vec(),
                                        ),
                                    ),
                                }))
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(err) => {
                            warn!("pty read error: {err}");
                            break;
                        }
                    }
                }
            }
        });

        let writer = Arc::new(StdMutex::new(writer));
        let master = Arc::new(StdMutex::new(pair.master));
        {
            let writer = writer.clone();
            let master = master.clone();
            let killer = killer.clone();
            tokio::spawn(async move {
                loop {
                    match stream.message().await {
                        Ok(Some(InteractiveShellRequest {
                            request:
                                Some(
                                    crate::pb::bracket::portproxy::v1::interactive_shell_request::Request::StdinData(
                                        data,
                                    ),
                                ),
                        })) => {
                            let writer = writer.clone();
                            let data = data.to_vec();
                            let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                                let mut guard = writer
                                    .lock()
                                    .map_err(|_| std::io::Error::other("writer poisoned"))?;
                                guard.write_all(&data)?;
                                guard.flush()
                            })
                            .await;
                            match result {
                                Ok(Ok(())) => {}
                                _ => break,
                            }
                        }
                        Ok(Some(InteractiveShellRequest {
                            request:
                                Some(
                                    crate::pb::bracket::portproxy::v1::interactive_shell_request::Request::Resize(
                                        size,
                                    ),
                                ),
                        })) => {
                            let master = master.clone();
                            let cols = u16::try_from(size.cols).unwrap_or(u16::MAX).max(1);
                            let rows = u16::try_from(size.rows).unwrap_or(u16::MAX).max(1);
                            let result = tokio::task::spawn_blocking(move || -> Result<(), String> {
                                let guard = master.lock().map_err(|_| "pty master poisoned".to_string())?;
                                guard.resize(PtySize {
                                    rows,
                                    cols,
                                    pixel_width: 0,
                                    pixel_height: 0,
                                })
                                .map_err(|err| err.to_string())
                            })
                            .await;
                            match result {
                                Ok(Ok(())) => {}
                                _ => break,
                            }
                        }
                        Ok(Some(_)) => {}
                        Ok(None) => break,
                        Err(err) => {
                            debug!("interactive shell input error: {err}");
                            break;
                        }
                    }
                }

                if let Ok(mut guard) = killer.lock() {
                    if let Some(mut killer) = guard.take() {
                        if let Err(err) = killer.kill() {
                            debug!("interactive shell kill failed: {err}");
                        }
                    }
                }
            });
        }

        let killer_for_wait = killer.clone();
        tokio::spawn(async move {
            let status = child_exit.wait().await;
            drop(child);

            if let Ok(mut guard) = killer_for_wait.lock() {
                guard.take();
            }

            let exit_code = match status {
                Ok(exit_status) => exit_status.interactive_code(),
                Err(err) => {
                    error!("interactive shell wait failed: {err}");
                    -1
                }
            };

            let _ = tx
                .send(Ok(InteractiveShellResponse {
                    response: Some(
                        crate::pb::bracket::portproxy::v1::interactive_shell_response::Response::ExitCode(exit_code),
                    ),
                }))
                .await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Clone)]
pub struct PortProxyService {
    tracker: ChildTracker,
}

impl PortProxyService {
    pub fn new(tracker: ChildTracker) -> Self {
        Self { tracker }
    }
}

#[tonic::async_trait]
impl PortProxy for PortProxyService {
    async fn prepare_shutdown(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        let pids = self.tracker.snapshot();
        for pid in &pids {
            if *pid <= 0 {
                continue;
            }
            if let Err(err) = signal_process_group_or_pid(*pid, Signal::SIGTERM) {
                warn!("failed to send SIGTERM to pid {}: {}", pid, err);
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        for pid in pids {
            if pid <= 0 {
                continue;
            }
            if let Err(err) = signal_process_group_or_pid(pid, Signal::SIGKILL) {
                warn!("failed to send SIGKILL to pid {}: {}", pid, err);
            }
        }
        Ok(Response::new(Empty {}))
    }

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<ReadFileResponse>, Status> {
        let path = validate_path(&request.get_ref().path)?;
        match read_file_bounded(&path).await {
            Ok(data) => Ok(Response::new(ReadFileResponse { data })),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(Status::not_found(format!("path {:?} not found", path)))
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidData => {
                Err(Status::resource_exhausted(format!("{err}")))
            }
            Err(err) => Err(Status::internal(format!(
                "failed to read file {:?}: {err}",
                path
            ))),
        }
    }

    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let path = validate_path(&req.path)?;
        if req.data.len() > MAX_FILE_RPC_BYTES {
            return Err(Status::resource_exhausted(format!(
                "write_file payload exceeds {} bytes",
                MAX_FILE_RPC_BYTES
            )));
        }
        if req.create_parents {
            if let Some(parent) = path.parent() {
                if let Err(err) = fs::create_dir_all(parent).await {
                    return Err(Status::internal(format!(
                        "failed to create parents for {:?}: {err}",
                        path
                    )));
                }
            }
        }
        if let Err(err) = fs::write(&path, req.data).await {
            return Err(Status::internal(format!(
                "failed to write {:?}: {err}",
                path
            )));
        }
        Ok(Response::new(Empty {}))
    }

    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        let path = validate_path(&request.get_ref().path)?;
        let mut entries = fs::read_dir(&path)
            .await
            .map_err(|err| map_fs_error(err, &path))?;

        let mut resp = ListDirectoryResponse { entries: vec![] };
        while let Some(entry) = entries.next_entry().await? {
            if resp.entries.len() >= MAX_DIRECTORY_ENTRIES {
                return Err(Status::resource_exhausted(format!(
                    "directory listing exceeds {} entries",
                    MAX_DIRECTORY_ENTRIES
                )));
            }
            let file_type = entry.file_type().await.map_err(|err| {
                Status::internal(format!(
                    "failed to read metadata for {:?}: {err}",
                    entry.path()
                ))
            })?;
            resp.entries.push(DirectoryEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                is_dir: file_type.is_dir(),
                is_symlink: file_type.is_symlink(),
            });
        }
        Ok(Response::new(resp))
    }

    async fn delete_path(
        &self,
        request: Request<DeletePathRequest>,
    ) -> Result<Response<Empty>, Status> {
        let path = validate_path(&request.get_ref().path)?;
        let path_clone = path.clone();
        tokio::task::spawn_blocking(move || {
            if path_clone.is_dir() {
                std::fs::remove_dir_all(&path_clone)
            } else {
                std::fs::remove_file(&path_clone)
            }
        })
        .await
        .map_err(|err| Status::internal(format!("delete task failed: {err}")))?
        .map_err(|err| Status::internal(format!("failed to delete {:?}: {err}", path)))?;

        Ok(Response::new(Empty {}))
    }
}

#[derive(Clone)]
pub struct DaemonManagerService {
    registry: DaemonRegistry,
}

impl DaemonManagerService {
    pub fn new(registry: DaemonRegistry) -> Self {
        Self { registry }
    }
}

#[tonic::async_trait]
impl DaemonManager for DaemonManagerService {
    type AttachDaemonStream = AttachDaemonResponseStream;

    async fn exec_daemon(
        &self,
        request: Request<ExecDaemonRequest>,
    ) -> Result<Response<ExecDaemonResponse>, Status> {
        match self.registry.exec_daemon(request.into_inner()).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(DaemonError::AlreadyRunning) => {
                Ok(Response::new(ExecDaemonResponse { is_new: false }))
            }
            Err(DaemonError::InvalidRequest(msg)) => Err(Status::invalid_argument(msg)),
            Err(DaemonError::Spawn(err)) => Err(Status::internal(format!("{err}"))),
        }
    }

    async fn attach_daemon(
        &self,
        request: Request<tonic::Streaming<AttachDaemonRequest>>,
    ) -> Result<Response<Self::AttachDaemonStream>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("stream ended before start"))?;

        let AttachDaemonRequest {
            request:
                Some(crate::pb::bracket::portproxy::v1::attach_daemon_request::Request::Start(start)),
        } = first
        else {
            return Err(Status::invalid_argument(
                "first message must be AttachDaemonStart",
            ));
        };

        let Some(entry) = self.registry.get(&start.name).await else {
            return Err(Status::not_found(format!(
                "daemon {} not found",
                start.name
            )));
        };

        let guard = entry.attach_lock.clone().lock_owned().await;

        let (tx, rx) = mpsc::channel(32);
        let mut exit_rx = entry.exit_tx.subscribe();
        let cached_exit = entry.cached_exit_code();
        let replay_frames = entry.drain_output_backlog().await;

        // @dive: Replay any buffered output the child wrote before this attach
        //        arrived. We send these synchronously so the post-exit fast
        //        path below can rely on the backlog being delivered first,
        //        ahead of the ExitCode frame.
        for frame in replay_frames {
            let response = match frame.kind {
                OutputKind::Stdout => {
                    crate::pb::bracket::portproxy::v1::attach_daemon_response::Response::StdoutData(
                        frame.data,
                    )
                }
                OutputKind::Stderr => {
                    crate::pb::bracket::portproxy::v1::attach_daemon_response::Response::StderrData(
                        frame.data,
                    )
                }
            };
            if tx
                .send(Ok(AttachDaemonResponse {
                    response: Some(response),
                }))
                .await
                .is_err()
            {
                break;
            }
        }

        // @dive: Post-exit fast path. When attach arrives after the daemon's
        //        child has already exited (common for fast `echo`-class
        //        commands plus the post-exit registry retention window), the
        //        broadcast::Senders held by `entry` outlive the spawn_reader
        //        tasks that produced into them, so subscribing here would
        //        block on `recv()` forever (Sender alive, no producers).
        //        Instead, deliver only the cached output backlog + final
        //        exit code, then drop tx so the response stream completes
        //        cleanly. No stdin task either — child stdin is closed.
        if let Some(exit_code) = cached_exit {
            let _ = tx
                .send(Ok(AttachDaemonResponse {
                    response: Some(
                        crate::pb::bracket::portproxy::v1::attach_daemon_response::Response::ExitCode(exit_code),
                    ),
                }))
                .await;
            drop(tx);
            let stream = GuardedStream::new(guard, ReceiverStream::new(rx));
            return Ok(Response::new(stream));
        }

        // Subscribe while child is still running. If the entry's broadcast
        // Sender has already been dropped (race with child exit between the
        // cached_exit check above and this point), fall through to the
        // post-exit ExitCode delivery below.
        let stdout_rx = entry
            .stdout_tx
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|tx| tx.subscribe()));
        let stderr_rx = entry
            .stderr_tx
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|tx| tx.subscribe()));

        if stdout_rx.is_none() || stderr_rx.is_none() {
            // Entry was post-exit cleaned between our cached_exit check and
            // here. Deliver whatever exit code is cached now and end.
            let final_exit = entry.cached_exit_code();
            if let Some(exit_code) = final_exit {
                let _ = tx
                    .send(Ok(AttachDaemonResponse {
                        response: Some(
                            crate::pb::bracket::portproxy::v1::attach_daemon_response::Response::ExitCode(exit_code),
                        ),
                    }))
                    .await;
            }
            drop(tx);
            let stream = GuardedStream::new(guard, ReceiverStream::new(rx));
            return Ok(Response::new(stream));
        }
        let mut stdout_rx = stdout_rx.unwrap();
        let mut stderr_rx = stderr_rx.unwrap();

        // @dive: One coordinator task instead of three concurrent forwarders.
        //        The previous design raced: stdout/stderr forwarders could drop
        //        their tx clones (broadcast Closed, observed when both
        //        spawn_reader and the entry's broadcast Sender had been
        //        dropped) before the exit forwarder ever scheduled, leaving
        //        the response stream to close without an ExitCode frame and
        //        forcing vmd's spawn loop into the terminal_emitted=false
        //        branch. Centralizing the lifecycle in one task lets us hold
        //        tx until we have either sent ExitCode or proven none will
        //        ever arrive.
        let entry_for_coord = entry.clone();
        tokio::spawn(async move {
            let mut stdout_done = false;
            let mut stderr_done = false;
            let mut exit_code: Option<i32> = None;
            // Initial check: post_exit may have fired exit_tx.send already
            // even though we got past the cached_exit fast-path check above.
            if let Some(code) = entry_for_coord.cached_exit_code() {
                exit_code = Some(code);
            }

            loop {
                if exit_code.is_some() {
                    break;
                }

                tokio::select! {
                    biased;
                    stdout_event = stdout_rx.recv(), if !stdout_done => match stdout_event {
                        Ok(data) => {
                            if tx
                                .send(Ok(AttachDaemonResponse {
                                    response: Some(
                                        crate::pb::bracket::portproxy::v1::attach_daemon_response::Response::StdoutData(data),
                                    ),
                                }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => stdout_done = true,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    },
                    stderr_event = stderr_rx.recv(), if !stderr_done => match stderr_event {
                        Ok(data) => {
                            if tx
                                .send(Ok(AttachDaemonResponse {
                                    response: Some(
                                        crate::pb::bracket::portproxy::v1::attach_daemon_response::Response::StderrData(data),
                                    ),
                                }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => stderr_done = true,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    },
                    changed = exit_rx.changed(), if exit_code.is_none() => match changed {
                        Ok(()) => {
                            if let Some(code) = entry_for_coord.cached_exit_code() {
                                exit_code = Some(code);
                            }
                        }
                        Err(_) => {
                            // All Senders dropped without setting Some(code).
                            // Break out — nothing more to do.
                            break;
                        }
                    },
                }
            }

            // Final guarantee: if either output stream is still open but we
            // already have an exit code, give the producer a brief window to
            // flush its last frames, then close. Without this, fast commands
            // that emit stdout right before exit could lose the trailing line
            // because select! might pick the changed() branch first.
            if exit_code.is_some() && (!stdout_done || !stderr_done) {
                let drain_deadline =
                    std::time::Instant::now() + std::time::Duration::from_millis(50);
                while !stdout_done || !stderr_done {
                    let remaining =
                        drain_deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    tokio::select! {
                        biased;
                        stdout_event = stdout_rx.recv(), if !stdout_done => match stdout_event {
                            Ok(data) => {
                                if tx
                                    .send(Ok(AttachDaemonResponse {
                                        response: Some(
                                            crate::pb::bracket::portproxy::v1::attach_daemon_response::Response::StdoutData(data),
                                        ),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => stdout_done = true,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        },
                        stderr_event = stderr_rx.recv(), if !stderr_done => match stderr_event {
                            Ok(data) => {
                                if tx
                                    .send(Ok(AttachDaemonResponse {
                                        response: Some(
                                            crate::pb::bracket::portproxy::v1::attach_daemon_response::Response::StderrData(data),
                                        ),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => stderr_done = true,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        },
                        _ = tokio::time::sleep(remaining) => break,
                    }
                }
            }

            // If the exit_rx changed() returned Err without ever setting a code
            // (all Senders dropped before child exit propagated), make one
            // last attempt to read the entry's retained terminal state.
            if exit_code.is_none() {
                if let Some(code) = entry_for_coord.cached_exit_code() {
                    exit_code = Some(code);
                }
            }

            if let Some(code) = exit_code {
                let _ = tx
                    .send(Ok(AttachDaemonResponse {
                        response: Some(
                            crate::pb::bracket::portproxy::v1::attach_daemon_response::Response::ExitCode(code),
                        ),
                    }))
                    .await;
            }
        });

        {
            let entry = entry.clone();
            tokio::spawn(async move {
                loop {
                    match stream.message().await {
                        Ok(Some(AttachDaemonRequest {
                            request:
                                Some(
                                    crate::pb::bracket::portproxy::v1::attach_daemon_request::Request::StdinData(
                                        data,
                                    ),
                                ),
                        })) => {
                            let mut stdin_guard = entry.stdin.lock().await;
                            let Some(stdin) = stdin_guard.as_mut() else {
                                // Child has exited and the post-exit cleanup
                                // task replaced stdin with None — nothing to
                                // write to.
                                break;
                            };
                            if let Err(err) = stdin.write_all(&data).await {
                                debug!("daemon stdin write failed: {err}");
                                break;
                            }
                        }
                        Ok(Some(_)) => {}
                        Ok(None) => break,
                        Err(err) => {
                            debug!("attach daemon stream error: {err}");
                            break;
                        }
                    }
                }
            });
        }

        let stream = GuardedStream::new(guard, ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }
}

fn validate_args(start: &ExecStart) -> Result<Vec<String>, Status> {
    if start.args.is_empty() {
        return Err(Status::invalid_argument("args must not be empty"));
    }
    Ok(start.args.clone())
}

fn validate_path(path: &str) -> Result<PathBuf, Status> {
    if path.is_empty() {
        return Err(Status::invalid_argument("path is required"));
    }
    Ok(PathBuf::from(path))
}

fn map_fs_error(err: std::io::Error, path: &PathBuf) -> Status {
    match err.kind() {
        io::ErrorKind::NotFound => Status::not_found(format!("path {:?} not found", path)),
        io::ErrorKind::PermissionDenied => {
            Status::permission_denied(format!("permission denied for {:?}", path))
        }
        _ => Status::internal(format!("failed to list {:?}: {err}", path)),
    }
}

async fn read_file_bounded(path: &PathBuf) -> io::Result<Vec<u8>> {
    let file = fs::File::open(path).await?;
    let mut data = Vec::new();
    file.take((MAX_FILE_RPC_BYTES + 1) as u64)
        .read_to_end(&mut data)
        .await?;
    if data.len() > MAX_FILE_RPC_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("file exceeds {MAX_FILE_RPC_BYTES} bytes"),
        ));
    }
    Ok(data)
}

fn spawn_reader<R, F>(
    mut reader: R,
    tx: mpsc::Sender<Result<ExecResponse, Status>>,
    build: F,
) -> JoinHandle<()>
where
    R: io::AsyncRead + Send + std::marker::Unpin + 'static,
    F: Fn(Vec<u8>) -> ExecResponse + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut buf = vec![0u8; READ_BUFFER];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if tx.send(Ok(build(buf[..n].to_vec()))).await.is_err() {
                        break;
                    }
                }
                Err(err) => {
                    warn!("io read error: {err}");
                    break;
                }
            }
        }
    })
}

async fn drain_exec_readers(stdout_reader: JoinHandle<()>, stderr_reader: JoinHandle<()>) {
    let _ = stdout_reader.await;
    let _ = stderr_reader.await;
}

async fn finalize_exec_status(
    pid: i32,
    tx: mpsc::Sender<Result<ExecResponse, Status>>,
    status: ExitStatus,
) {
    let code = status.code().or_else(|| status.signal()).unwrap_or(-1);
    finalize_exec_with_code(pid, tx, code).await;
}

async fn finalize_exec_wait_error(
    pid: i32,
    tx: mpsc::Sender<Result<ExecResponse, Status>>,
    err: impl std::fmt::Display,
) {
    error!("failed waiting for child pid {pid}: {err}");
    let _ = tx
        .send(Ok(ExecResponse {
            response: Some(
                crate::pb::bracket::portproxy::v1::exec_response::Response::StderrData(
                    format!("\n(Command exit status unavailable: {err})").into_bytes(),
                ),
            ),
        }))
        .await;
    finalize_exec_with_code(pid, tx, -1).await;
}

async fn finalize_exec_with_code(
    pid: i32,
    tx: mpsc::Sender<Result<ExecResponse, Status>>,
    code: i32,
) {
    if tx
        .send(Ok(ExecResponse {
            response: Some(
                crate::pb::bracket::portproxy::v1::exec_response::Response::ExitCode(code),
            ),
        }))
        .await
        .is_err()
    {
        debug!("failed to send exit code for pid {}", pid);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use futures::stream;
    use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
    use nix::unistd::Pid;
    use tokio::net::TcpListener;
    use tonic::Request;
    use tonic::transport::Server;

    use super::*;
    use crate::child_tracker::{ChildExit, ChildTracker};
    use crate::daemon::DaemonRegistry;
    use crate::pb::bracket::portproxy::v1::daemon_manager_client::DaemonManagerClient;
    use crate::pb::bracket::portproxy::v1::daemon_manager_server::DaemonManagerServer;
    use crate::pb::bracket::portproxy::v1::shell_exec_client::ShellExecClient;
    use crate::pb::bracket::portproxy::v1::shell_exec_server::ShellExecServer;
    use crate::pb::bracket::portproxy::v1::{
        AttachDaemonStart, attach_daemon_request, attach_daemon_response,
    };

    #[tokio::test]
    async fn exec_command_finishes_when_client_closes_stream_after_start() {
        let tracker = ChildTracker::new();
        spawn_test_child_reaper(tracker.clone());
        let service = ShellExecService::new(tracker);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let addr = listener.local_addr().expect("listener has local addr");
        let incoming = stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(ShellExecServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .expect("test shell exec server should run");
        });

        let mut client = ShellExecClient::connect(format!("http://{addr}"))
            .await
            .expect("client should connect");
        let (tx, rx) = mpsc::channel(2);
        tx.send(ExecRequest {
            request: Some(
                crate::pb::bracket::portproxy::v1::exec_request::Request::Start(ExecStart {
                    args: vec![
                        "sh".to_string(),
                        "-lc".to_string(),
                        "cat >/dev/null; printf stdin-closed-ok".to_string(),
                    ],
                    env: HashMap::new(),
                    timeout: Some(2),
                    detach: false,
                }),
            ),
        })
        .await
        .expect("start frame should enqueue");
        drop(tx);

        let mut stream = client
            .exec(Request::new(ReceiverStream::new(rx)))
            .await
            .expect("exec should start")
            .into_inner();

        let mut stdout = Vec::new();
        let mut exit_code = None;
        loop {
            let next = tokio::time::timeout(Duration::from_secs(3), stream.message())
                .await
                .expect("exec stream should not hang")
                .expect("exec stream should not error");
            let Some(frame) = next else { break };
            match frame.response {
                Some(crate::pb::bracket::portproxy::v1::exec_response::Response::StdoutData(
                    bytes,
                )) => {
                    stdout.extend(bytes);
                }
                Some(crate::pb::bracket::portproxy::v1::exec_response::Response::ExitCode(
                    code,
                )) => {
                    exit_code = Some(code);
                    break;
                }
                _ => {}
            }
        }
        server.abort();

        assert_eq!(String::from_utf8_lossy(&stdout), "stdin-closed-ok");
        assert_eq!(exit_code, Some(0));
    }

    #[tokio::test]
    async fn attach_daemon_fast_command_returns_exit_code_after_late_attach() {
        let tracker = ChildTracker::new();
        spawn_test_child_reaper(tracker.clone());
        let registry = DaemonRegistry::new(tracker);
        let service = DaemonManagerService::new(registry);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let addr = listener.local_addr().expect("listener has local addr");
        let incoming = stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(DaemonManagerServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .expect("test daemon manager server should run");
        });

        let mut client = DaemonManagerClient::connect(format!("http://{addr}"))
            .await
            .expect("client should connect");
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("unix time should be monotonic")
            .as_nanos();
        let daemon_name = format!("late-attach-{nonce}");
        client
            .exec_daemon(Request::new(ExecDaemonRequest {
                name: daemon_name.clone(),
                args: vec![
                    "sh".to_string(),
                    "-lc".to_string(),
                    "echo fast-attach-ok".to_string(),
                ],
                env: HashMap::new(),
                timeout: Some(5),
                detach: false,
            }))
            .await
            .expect("exec_daemon should start process");

        tokio::time::sleep(Duration::from_millis(150)).await;

        let (tx, rx) = mpsc::channel(2);
        tx.send(AttachDaemonRequest {
            request: Some(attach_daemon_request::Request::Start(AttachDaemonStart {
                name: daemon_name,
            })),
        })
        .await
        .expect("start frame should enqueue");
        drop(tx);
        let mut stream = client
            .attach_daemon(Request::new(ReceiverStream::new(rx)))
            .await
            .expect("attach_daemon should resolve retained daemon")
            .into_inner();

        let mut saw_stdout = false;
        let mut exit_code = None;
        loop {
            let next = tokio::time::timeout(Duration::from_secs(2), stream.message())
                .await
                .expect("attach stream should not hang")
                .expect("attach stream should not error");
            let Some(frame) = next else { break };
            match frame.response {
                Some(attach_daemon_response::Response::StdoutData(bytes)) => {
                    saw_stdout |= String::from_utf8_lossy(&bytes).contains("fast-attach-ok");
                }
                Some(attach_daemon_response::Response::ExitCode(code)) => {
                    exit_code = Some(code);
                }
                _ => {}
            }
        }
        server.abort();

        assert!(saw_stdout, "expected stdout backlog to replay before exit");
        assert_eq!(exit_code, Some(0), "late attach must include terminal exit");
    }

    #[tokio::test]
    async fn attach_daemon_live_command_returns_exit_code_after_client_eof() {
        let tracker = ChildTracker::new();
        spawn_test_child_reaper(tracker.clone());
        let registry = DaemonRegistry::new(tracker);
        let service = DaemonManagerService::new(registry);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let addr = listener.local_addr().expect("listener has local addr");
        let incoming = stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(DaemonManagerServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .expect("test daemon manager server should run");
        });

        let mut client = DaemonManagerClient::connect(format!("http://{addr}"))
            .await
            .expect("client should connect");
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("unix time should be monotonic")
            .as_nanos();
        let daemon_name = format!("live-attach-{nonce}");
        client
            .exec_daemon(Request::new(ExecDaemonRequest {
                name: daemon_name.clone(),
                args: vec![
                    "sh".to_string(),
                    "-lc".to_string(),
                    "echo live-attach-ok".to_string(),
                ],
                env: HashMap::new(),
                timeout: Some(5),
                detach: false,
            }))
            .await
            .expect("exec_daemon should start process");

        let (tx, rx) = mpsc::channel(2);
        tx.send(AttachDaemonRequest {
            request: Some(attach_daemon_request::Request::Start(AttachDaemonStart {
                name: daemon_name,
            })),
        })
        .await
        .expect("start frame should enqueue");
        drop(tx);
        let mut stream = client
            .attach_daemon(Request::new(ReceiverStream::new(rx)))
            .await
            .expect("attach_daemon should resolve running daemon")
            .into_inner();

        let mut saw_stdout = false;
        let mut exit_code = None;
        loop {
            let next = tokio::time::timeout(Duration::from_secs(2), stream.message())
                .await
                .expect("attach stream should not hang")
                .expect("attach stream should not error");
            let Some(frame) = next else { break };
            match frame.response {
                Some(attach_daemon_response::Response::StdoutData(bytes)) => {
                    saw_stdout |= String::from_utf8_lossy(&bytes).contains("live-attach-ok");
                }
                Some(attach_daemon_response::Response::ExitCode(code)) => {
                    exit_code = Some(code);
                }
                _ => {}
            }
        }
        server.abort();

        assert!(saw_stdout, "expected stdout from live attach");
        assert_eq!(exit_code, Some(0), "live attach must include terminal exit");
    }

    fn spawn_test_child_reaper(tracker: ChildTracker) {
        tokio::spawn(async move {
            loop {
                for pid in tracker.snapshot() {
                    match waitpid(Pid::from_raw(pid), Some(WaitPidFlag::WNOHANG)) {
                        Ok(WaitStatus::Exited(_, status)) => {
                            tracker.record_exit(pid, ChildExit::Exited(status));
                        }
                        Ok(WaitStatus::Signaled(_, signal, _)) => {
                            tracker.record_exit(pid, ChildExit::Signaled(signal as i32));
                        }
                        Ok(WaitStatus::StillAlive) => {}
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
    }
}
