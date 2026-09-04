use crate::error::{Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinSet, time::Instant};
pub use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

#[cfg(feature = "sandbox")]
pub mod sandbox;

const MAX_BYTES: usize = 1024 * 1024;
const MAX_CALLS: usize = 1000;
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const RUNNER: &str = include_str!("programmatic_runner.cjs");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgrammaticTool {
    pub name: String,
    pub description: String,
    pub schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ProgrammaticEvent {
    Stdout { text: String },
    Stderr { text: String },
    Exit { code: i32 },
    Timeout,
}

#[async_trait]
pub trait ProgrammaticSession: Send + Sync {
    async fn write(&self, data: &str) -> Result<()>;
    async fn next(&self) -> Result<Option<ProgrammaticEvent>>;
    async fn signal(&self, signal: i32) -> Result<()>;
}

#[async_trait]
pub trait ProgrammaticSandbox: Send + Sync {
    async fn start_exec(
        &self,
        command: &str,
        timeout: Duration,
        cancel: CancellationToken,
    ) -> Result<Arc<dyn ProgrammaticSession>>;
}

#[async_trait]
pub trait ProgrammaticDispatcher: Send + Sync {
    async fn call(&self, name: &str, args: Value, cancel: CancellationToken) -> Result<Value>;
}

#[derive(Clone)]
pub struct ProgrammaticOptions {
    pub timeout: Duration,
    pub cancel: CancellationToken,
}

impl Default for ProgrammaticOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            cancel: CancellationToken::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProgrammaticResult {
    pub output: Vec<Value>,
}

pub fn programmatic_tool_description(tools: &[ProgrammaticTool]) -> String {
    let enabled: Vec<_> = tools
        .iter()
        .filter(|tool| tool.name != "execute_code")
        .collect();
    format!(
        "Execute JavaScript in the host-selected sandbox, with its existing filesystem, network and process access. \
         Requires a sandbox with Node.js installed; no local fallback or sandbox provisioning. \
         Use await tools[name](args) for enabled tools and text(value) for selected JSON output. \
         Await every call, using Promise.all for independent calls. Intermediate tool results stay in the program. \
         Calls retain the host's normal approval and permission policy. Use direct calls when each result needs model judgment. \
         Each execution is fresh. Enabled tools: {}",
        serde_json::to_string(&enabled).expect("tool descriptors contain JSON values")
    )
}

fn failure(message: impl Into<String>) -> Error {
    Error::NonRetryable(message.into())
}

pub async fn execute_programmatic(
    code: &str,
    tools: &[ProgrammaticTool],
    sandbox: Arc<dyn ProgrammaticSandbox>,
    dispatcher: Arc<dyn ProgrammaticDispatcher>,
    options: ProgrammaticOptions,
) -> Result<ProgrammaticResult> {
    if code.len() > MAX_BYTES {
        return Err(failure("Program exceeds size limit"));
    }
    if options.timeout.is_zero() || options.timeout.as_millis() > i32::MAX as u128 {
        return Err(failure("Invalid programmatic timeoutMs"));
    }
    let cancel = options.cancel.child_token();
    let _cancel_on_drop = cancel.clone().drop_guard();
    let options = ProgrammaticOptions { cancel, ..options };
    let code = code.to_owned();
    let enabled = tools
        .iter()
        .filter(|tool| tool.name != "execute_code")
        .map(|tool| tool.name.clone())
        .collect();
    tokio::spawn(run_program(code, enabled, sandbox, dispatcher, options))
        .await
        .map_err(|error| failure(format!("Programmatic executor failed: {error}")))?
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProgramFrame {
    Call {
        id: u64,
        name: String,
        #[serde(default)]
        args: Value,
    },
    Output {
        value: Value,
    },
    Error {
        message: String,
    },
    Done,
}

async fn run_program(
    code: String,
    names: Vec<String>,
    sandbox: Arc<dyn ProgrammaticSandbox>,
    dispatcher: Arc<dyn ProgrammaticDispatcher>,
    options: ProgrammaticOptions,
) -> Result<ProgrammaticResult> {
    let enabled: HashSet<_> = names.iter().cloned().collect();
    let deadline = Instant::now() + options.timeout;
    let command = format!("node -e '{}'", RUNNER.replace('\'', "'\\''"));
    let session = tokio::select! {
        biased;
        _ = options.cancel.cancelled() => return Err(failure("Programmatic execution cancelled")),
        _ = tokio::time::sleep_until(deadline) => {
            options.cancel.cancel();
            return Err(failure("Programmatic execution timed out"));
        }
        result = sandbox.start_exec(&command, options.timeout, options.cancel.clone()) => result?,
    };
    let (sender, mut events) = mpsc::channel(16);
    let reader_session = session.clone();
    let _reader = AbortOnDropHandle::new(tokio::spawn(async move {
        loop {
            let event = reader_session.next().await;
            let terminal = matches!(
                event,
                Ok(None | Some(ProgrammaticEvent::Exit { .. })) | Err(_)
            );
            if sender.send(event).await.is_err() || terminal {
                break;
            }
        }
    }));
    let marker = format!("__chevalier_{}__", uuid::Uuid::new_v4());
    let mut jobs: JoinSet<Result<(u64, Value)>> = JoinSet::new();
    let mut terminal = false;
    let operation = async {
        session
            .write(&format!(
                "{}\n",
                json!({"marker":marker,"names":names,"code":code})
            ))
            .await?;
        let mut buffer = String::new();
        let mut stderr = String::new();
        let mut seen = HashSet::new();
        let mut output = Vec::new();
        let mut output_bytes = 0;
        loop {
            tokio::select! {
                biased;
                _ = options.cancel.cancelled() => return Err(failure("Programmatic execution cancelled")),
                _ = tokio::time::sleep_until(deadline) => return Err(failure("Programmatic execution timed out")),
                completed = jobs.join_next(), if !jobs.is_empty() => {
                    let (id, mut reply) = completed.ok_or_else(|| failure("Missing tool result"))?
                        .map_err(|error| failure(format!("Tool dispatch failed: {error}")))??;
                    reply["id"] = json!(id);
                    session.write(&format!("{reply}\n")).await?;
                }
                event = events.recv() => match event.ok_or_else(|| failure("Sandbox stream closed unexpectedly"))?? {
                    None | Some(ProgrammaticEvent::Exit { .. }) => {
                        terminal = true;
                        return Err(failure(format!("Sandbox program exited before completion; Node.js is required. {stderr}")));
                    }
                    Some(ProgrammaticEvent::Timeout) => return Err(failure("Programmatic execution timed out")),
                    Some(ProgrammaticEvent::Stderr { text }) => {
                        stderr.extend(text.chars().take(8192usize.saturating_sub(stderr.len())));
                    }
                    Some(ProgrammaticEvent::Stdout { text }) => {
                        buffer.push_str(&text);
                        if buffer.len() > MAX_BYTES * 2 { return Err(failure("Program protocol frame exceeds size limit")); }
                        while let Some(end) = buffer.find('\n') {
                            let line: String = buffer.drain(..=end).collect();
                            let Some(frame) = line.strip_prefix(&marker) else { continue; };
                            let frame: ProgramFrame = serde_json::from_str(frame)
                                .map_err(|_| failure("Invalid programmatic tool call or protocol frame"))?;
                            match frame {
                                ProgramFrame::Error { message } => return Err(failure(message)),
                                ProgramFrame::Done => {
                                    if !jobs.is_empty() { return Err(failure("Program ended with unawaited tool calls")); }
                                    return Ok(ProgrammaticResult { output });
                                }
                                ProgramFrame::Output { value } => {
                                    output_bytes += line.len();
                                    if output_bytes > MAX_BYTES || output.len() >= MAX_CALLS {
                                        return Err(failure("Program output exceeds size limit"));
                                    }
                                    output.push(value);
                                }
                                ProgramFrame::Call { id, name, args } => {
                                    if id == 0 || !seen.insert(id) || seen.len() > MAX_CALLS {
                                        return Err(failure("Invalid programmatic tool call"));
                                    }
                                    let dispatcher = dispatcher.clone();
                                    let cancel = options.cancel.clone();
                                    let allowed = enabled.contains(&name);
                                    jobs.spawn(async move {
                                        let result = if allowed {
                                            dispatcher.call(&name, args, cancel).await
                                        } else { Err(Error::ToolNotFound(name)) };
                                        let reply = match result {
                                            Ok(value) if serde_json::to_vec(&value)?.len() <= MAX_BYTES => json!({"value":value}),
                                            Ok(_) => json!({"error":"Tool result exceeds JSON size limit"}),
                                            Err(error) => json!({"error":error.to_string()}),
                                        };
                                        Ok((id, reply))
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    let outcome = tokio::select! {
        biased;
        _ = options.cancel.cancelled() => Err(failure("Programmatic execution cancelled")),
        _ = tokio::time::sleep_until(deadline) => Err(failure("Programmatic execution timed out")),
        result = operation => result,
    };
    if outcome.is_err() {
        options.cancel.cancel();
    }
    let cleanup = async {
        if !terminal {
            let mut signalled = outcome.is_err();
            if signalled {
                let _ = session.signal(15).await;
            }
            let mut kill_at = Instant::now() + Duration::from_millis(200);
            let mut killed = false;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(kill_at), if !killed => {
                        if signalled {
                            killed = true;
                            session.signal(9).await?;
                        } else {
                            signalled = true;
                            session.signal(15).await?;
                            kill_at = Instant::now() + Duration::from_millis(200);
                        }
                    }
                    event = events.recv() => match event {
                        Some(Ok(None | Some(ProgrammaticEvent::Exit { .. }))) => break,
                        Some(Ok(_)) => {},
                        _ => return Err(failure("Sandbox did not confirm program termination")),
                    }
                }
            }
        }
        while jobs.join_next().await.is_some() {}
        Ok(())
    };
    let cleaned = tokio::time::timeout(CLEANUP_TIMEOUT, cleanup).await;
    options.cancel.cancel();
    match cleaned {
        Ok(Ok(())) => outcome,
        error => {
            jobs.abort_all();
            let detail = match error {
                Ok(Err(error)) => error.to_string(),
                Err(_) => "Sandbox did not confirm program termination or tool cancellation".into(),
                Ok(Ok(())) => unreachable!(),
            };
            Err(failure(match outcome {
                Ok(_) => detail,
                Err(error) => format!("{error}; {detail}"),
            }))
        }
    }
}
