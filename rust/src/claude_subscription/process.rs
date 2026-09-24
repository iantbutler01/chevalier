use super::{
    ClaudeSessionConfig, ClaudeSessionError, ClaudeSessionEvent, events,
    tools::{self, Pending},
    wire,
};
use crate::{
    error::{Error, Result},
    runtime::ToolExecutor,
};
use claude_codes::ClaudeOutput;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::{Mutex, mpsc},
    task::{JoinHandle, JoinSet},
};

const IDLE_POLL: Duration = Duration::from_millis(250);

pub(crate) struct Process {
    pub child: Arc<Mutex<Child>>,
    pub outbound: mpsc::UnboundedSender<Value>,
    pub incoming: Mutex<mpsc::UnboundedReceiver<Result<ClaudeSessionEvent>>>,
    pub pending: Pending,
    pub tasks: Vec<JoinHandle<()>>,
    pub stderr: Arc<Mutex<VecDeque<u8>>>,
}

fn validate_init(
    init: wire::InitProjection,
    server: &str,
    declared_tools: usize,
    cwd: &Path,
    stderr_tail: &str,
) -> std::result::Result<ClaudeSessionEvent, ClaudeSessionError> {
    if init.api_key_source != "none" {
        return Err(ClaudeSessionError::NotSubscription {
            api_key_source: init.api_key_source,
        });
    }
    if !init
        .mcp_servers
        .iter()
        .any(|entry| entry["name"] == server && entry["status"] == "connected")
        // A host that declared no tools has none to list; the server must still connect.
        || (declared_tools > 0
            && !init
                .tools
                .iter()
                .any(|tool| tool.starts_with(&format!("mcp__{server}__"))))
    {
        return Err(ClaudeSessionError::Protocol(format!(
            "server {server} not connected: {:?}; stderr: {stderr_tail}",
            init.mcp_servers
        )));
    }
    Ok(ClaudeSessionEvent::Init {
        session_id: init.session_id,
        cwd: cwd.to_path_buf(),
        model: init.model,
        cli_version: init.claude_code_version,
    })
}

pub(crate) async fn spawn(
    config: &ClaudeSessionConfig,
    cli: &PathBuf,
    schemas: HashMap<String, bool>,
    executor: ToolExecutor,
) -> Result<Process> {
    tokio::fs::create_dir_all(&config.cwd).await?;
    let mut child = Command::new(cli)
        .args(super::policy::argv(config))
        .current_dir(&config.cwd)
        .env_clear()
        .envs(super::policy::spawn_env(&config.client_app))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let child = Arc::new(Mutex::new(child));
    let (outbound, mut writes) = mpsc::unbounded_channel::<Value>();
    let (events_tx, incoming) = mpsc::unbounded_channel::<Result<ClaudeSessionEvent>>();
    let ring = Arc::new(Mutex::new(VecDeque::new()));
    let stderr_ring = ring.clone();
    let stderr_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        while let Ok(n) = stderr.read(&mut buf).await {
            if n == 0 {
                break;
            }
            let mut ring = stderr_ring.lock().await;
            ring.extend(&buf[..n]);
            while ring.len() > 65536 {
                ring.pop_front();
            }
        }
    });
    let writer = tokio::spawn(async move {
        while let Some(value) = writes.recv().await {
            let Ok(mut bytes) = serde_json::to_vec(&value) else {
                break;
            };
            bytes.push(b'\n');
            if stdin.write_all(&bytes).await.is_err() {
                break;
            }
        }
        let _ = stdin.shutdown().await;
    });
    let server = config.server_name.clone();
    let declared_tools = schemas.len();
    let resume_id = config
        .resume
        .as_ref()
        .map(|resume| resume.session_id.clone());
    let schemas = Arc::new(schemas);
    let cwd = config.cwd.clone();
    let idle = config.idle_timeout;
    let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
    let reader_pending = pending.clone();
    let idle_pending = pending.clone();
    let reader_outbound = outbound.clone();
    let reader_ring = ring.clone();
    let reader_child = child.clone();
    let dispatch = tools::DispatchContext {
        server: server.clone(),
        schemas,
        executor,
        pending: reader_pending,
        events: events_tx.clone(),
        outbound: reader_outbound.clone(),
    };
    let reader = tokio::spawn(async move {
        let mut bytes = Vec::new();
        let mut buf = [0u8; 4096];
        let mut bad_lines = 0;
        let mut initialized = false;
        let mut calls = JoinSet::new();
        let mut last_activity = tokio::time::Instant::now();
        loop {
            let read = tokio::time::timeout(IDLE_POLL, stdout.read(&mut buf)).await;
            // A tool call is the host's work, not the CLI's: the CLI is correctly silent while
            // one is outstanding (an approval can take minutes), so pending calls count as activity.
            if read.is_err() {
                if !idle_pending.lock().await.is_empty() {
                    last_activity = tokio::time::Instant::now();
                    continue;
                }
                if last_activity.elapsed() < idle {
                    continue;
                }
            }
            last_activity = tokio::time::Instant::now();
            let n = match read {
                Ok(Ok(n)) => n,
                Ok(Err(error)) => {
                    let _ = events_tx.send(Err(Error::ClaudeSession(
                        ClaudeSessionError::Protocol(error.to_string()),
                    )));
                    let _ = reader_child.lock().await.start_kill();
                    break;
                }
                Err(_) => {
                    let _ = reader_outbound.send(json!({"type":"control_request", "request_id": uuid::Uuid::new_v4().to_string(), "request":{"subtype":"interrupt"}}));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let _ = reader_child.lock().await.start_kill();
                    let tail = stderr_tail(&reader_ring).await;
                    let _ = events_tx.send(Err(Error::ClaudeSession(ClaudeSessionError::Idle {
                        stderr_tail: tail,
                    })));
                    break;
                }
            };
            if n == 0 {
                let status = match tokio::time::timeout(
                    Duration::from_secs(2),
                    reader_child.lock().await.wait(),
                )
                .await
                {
                    Ok(Ok(status)) => status.to_string(),
                    _ => "stdout closed".into(),
                };
                let tail = stderr_tail(&reader_ring).await;
                let _ = events_tx.send(Err(Error::ClaudeSession(ClaudeSessionError::Exited {
                    status,
                    stderr_tail: tail,
                })));
                break;
            }
            bytes.extend_from_slice(&buf[..n]);
            while let Some(pos) = bytes.iter().position(|byte| *byte == b'\n') {
                let line = bytes.drain(..=pos).collect::<Vec<_>>();
                let line = String::from_utf8_lossy(&line);
                let (typed, _) = match wire::decode(line.trim()) {
                    Ok(value) => {
                        bad_lines = 0;
                        value
                    }
                    Err(error) => {
                        if !wire::acted_shape(line.trim())
                            && serde_json::from_str::<Value>(line.trim()).is_ok()
                        {
                            tracing::debug!("ignoring unrecognized Claude output: {}", line.trim());
                            bad_lines = 0;
                            continue;
                        }
                        bad_lines += 1;
                        if bad_lines > 20 || line.trim_start().starts_with('{') {
                            let _ = events_tx.send(Err(Error::ClaudeSession(error)));
                            let _ = reader_child.lock().await.start_kill();
                            return;
                        }
                        continue;
                    }
                };
                if let ClaudeOutput::System(system) = &typed
                    && system.is_init()
                {
                    let init: wire::InitProjection =
                        match serde_json::from_value(system.data.clone()) {
                            Ok(value) => value,
                            Err(error) => {
                                let _ = events_tx.send(Err(Error::ClaudeSession(
                                    ClaudeSessionError::Protocol(format!(
                                        "invalid system/init: {error}"
                                    )),
                                )));
                                let _ = reader_child.lock().await.start_kill();
                                return;
                            }
                        };
                    match validate_init(
                        init,
                        &server,
                        declared_tools,
                        &cwd,
                        &stderr_tail(&reader_ring).await,
                    ) {
                        Ok(event) => {
                            initialized = true;
                            let _ = events_tx.send(Ok(event));
                        }
                        Err(error) => {
                            let _ = events_tx.send(Err(Error::ClaudeSession(error)));
                            let _ = reader_child.lock().await.start_kill();
                            return;
                        }
                    }
                }
                // A --resume whose transcript is gone ends with an error result before init.
                if !initialized
                    && let (Some(session_id), ClaudeOutput::Result(_)) = (&resume_id, &typed)
                {
                    let _ = events_tx.send(Err(Error::ClaudeSession(
                        ClaudeSessionError::ResumeNotFound {
                            session_id: session_id.clone(),
                        },
                    )));
                    let _ = reader_child.lock().await.start_kill();
                    return;
                }
                if !initialized
                    && !matches!(
                        typed,
                        ClaudeOutput::ControlResponse(_) | ClaudeOutput::System(_)
                    )
                {
                    let _ = events_tx.send(Err(Error::ClaudeSession(
                        ClaudeSessionError::Protocol("message before system/init".into()),
                    )));
                    let _ = reader_child.lock().await.start_kill();
                    return;
                }
                if let ClaudeOutput::ControlRequest(request) = &typed
                    && let claude_codes::ControlRequestPayload::McpMessage(mcp) = &request.request
                {
                    if mcp.server_name != server {
                        let _ = reader_outbound.send(wire::control_response(
                            &request.request_id,
                            mcp.message["id"].clone(),
                            super::ToolOutput {
                                content: vec![super::ToolContent::Text("Unknown server".into())],
                                is_error: true,
                            },
                        ));
                        continue;
                    }
                    calls.spawn(tools::dispatch(
                        request.request_id.clone(),
                        mcp.message.clone(),
                        dispatch.clone(),
                    ));
                }
                match events::map(&typed) {
                    Ok(mapped) => {
                        for event in mapped {
                            let _ = events_tx.send(Ok(event));
                        }
                    }
                    Err(error) => {
                        let _ = events_tx.send(Err(Error::ClaudeSession(error)));
                        let _ = reader_child.lock().await.start_kill();
                        return;
                    }
                }
            }
        }
        calls.abort_all();
    });
    Ok(Process {
        child,
        outbound,
        incoming: Mutex::new(incoming),
        pending,
        tasks: vec![reader, writer, stderr_task],
        stderr: ring,
    })
}

pub(crate) async fn stderr_tail(ring: &Arc<Mutex<VecDeque<u8>>>) -> String {
    String::from_utf8_lossy(&ring.lock().await.iter().copied().collect::<Vec<_>>()).into_owned()
}

pub(crate) async fn close(process: &Process) -> Result<super::ExitSummary> {
    for task in &process.tasks {
        task.abort();
    }
    let mut child = process.child.lock().await;
    if let Ok(Ok(status)) = tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
        return Ok(super::ExitSummary {
            status: status.to_string(),
            stderr_tail: stderr_tail(&process.stderr).await,
        });
    }
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        let _ = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .await;
    }
    if let Ok(Ok(status)) = tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        return Ok(super::ExitSummary {
            status: status.to_string(),
            stderr_tail: stderr_tail(&process.stderr).await,
        });
    }
    child.start_kill()?;
    let status = child.wait().await?;
    Ok(super::ExitSummary {
        status: status.to_string(),
        stderr_tail: stderr_tail(&process.stderr).await,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn init() -> wire::InitProjection {
        let line = include_str!("../../tests/fixtures/claude-subscription/happy.stdout.jsonl")
            .lines()
            .find(|line| line.contains("\"subtype\": \"init\""))
            .unwrap();
        let (_, raw) = wire::decode(line).unwrap();
        serde_json::from_value(raw).unwrap()
    }
    #[test]
    fn init_rejects_non_subscription() {
        let mut init = init();
        init.api_key_source = "ANTHROPIC_API_KEY".into();
        assert!(matches!(
            validate_init(init, "ob", 1, Path::new("/tmp"), ""),
            Err(ClaudeSessionError::NotSubscription { .. })
        ));
    }
    #[test]
    fn init_accepts_a_connected_server_with_no_declared_tools() {
        let mut init = init();
        init.tools.retain(|tool| !tool.starts_with("mcp__ob__"));
        assert!(validate_init(init, "ob", 0, Path::new("/tmp"), "").is_ok());
    }
    #[test]
    fn init_rejects_missing_server() {
        let mut init = init();
        init.mcp_servers.clear();
        assert!(
            matches!(validate_init(init, "ob", 1, Path::new("/tmp"), "stderr"), Err(ClaudeSessionError::Protocol(message)) if message.contains("stderr"))
        );
    }
}
