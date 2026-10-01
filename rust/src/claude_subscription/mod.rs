mod events;
mod policy;
mod process;
mod tools;
mod wire;

use crate::{
    error::{Error as EngineError, Result},
    runtime::Runtime,
    types::{MediaPart, ProviderRateLimit, TokenUsage, ToolCall},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use thiserror::Error;

pub use policy::claude_subscription_status;
pub const CLAUDE_SUBSCRIPTION_LABEL: &str = "Claude (subscription)";

#[derive(Debug, Error)]
pub enum ClaudeSessionError {
    #[error("Claude CLI not found: {probed:?}")]
    CliNotFound { probed: Vec<PathBuf> },
    #[error("Claude CLI {found} is older than required {required}")]
    CliTooOld { found: String, required: String },
    #[error("Claude is not logged in")]
    NotLoggedIn,
    #[error("Claude is using {api_key_source} instead of a subscription")]
    NotSubscription { api_key_source: String },
    #[error("Claude protocol: {0}")]
    Protocol(String),
    #[error("Claude has no saved session {session_id} to resume")]
    ResumeNotFound { session_id: String },
    #[error("Claude session idle timeout; stderr: {stderr_tail}")]
    Idle { stderr_tail: String },
    #[error("Claude exited ({status}); stderr: {stderr_tail}")]
    Exited { status: String, stderr_tail: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
    Ultra,
}
impl Effort {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max | Self::Ultra => "max",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeTarget {
    pub session_id: String,
    pub cwd: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ClaudeSessionConfig {
    pub model: String,
    pub system_prompt: Option<String>,
    pub effort: Option<Effort>,
    pub resume: Option<ResumeTarget>,
    pub cwd: PathBuf,
    pub cli_path: Option<PathBuf>,
    pub client_app: String,
    pub server_name: String,
    pub idle_timeout: Duration,
    pub max_turns: Option<u32>,
    /// Surface every call as `ToolCall`, including handler-backed tools (e.g. MCP client tools),
    /// so a host that gates tools itself can run them through `Runtime::execute_tool_call`.
    pub host_dispatch_all: bool,
}
impl Default for ClaudeSessionConfig {
    fn default() -> Self {
        Self {
            model: "sonnet".into(),
            system_prompt: None,
            effort: None,
            resume: None,
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            cli_path: None,
            client_app: "chevalier".into(),
            server_name: "chevalier".into(),
            idle_timeout: Duration::from_secs(240),
            max_turns: None,
            host_dispatch_all: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserTurn {
    pub text: String,
    pub images: Vec<MediaPart>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolOutput {
    pub content: Vec<ToolContent>,
    pub is_error: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToolContent {
    Text(String),
    Image {
        data_base64: String,
        mime_type: String,
    },
}
impl ToolOutput {
    pub fn from_json(value: &Value) -> Result<Self> {
        let content = value["content"]
            .as_array()
            .ok_or_else(|| ClaudeSessionError::Protocol("tool output needs content array".into()))?
            .iter()
            .map(|item| match item["type"].as_str() {
                Some("text") => item["text"]
                    .as_str()
                    .map(|text| ToolContent::Text(text.into())),
                Some("image") => Some(ToolContent::Image {
                    data_base64: item["dataBase64"]
                        .as_str()
                        .or_else(|| item["data_base64"].as_str())
                        .or_else(|| item["data"].as_str())?
                        .into(),
                    mime_type: item["mimeType"]
                        .as_str()
                        .or_else(|| item["mime_type"].as_str())?
                        .into(),
                }),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| ClaudeSessionError::Protocol("invalid tool content".into()))?;
        Ok(Self {
            content,
            is_error: value["isError"].as_bool().unwrap_or(false),
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitSummary {
    pub status: String,
    pub stderr_tail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClaudeSessionEvent {
    Init {
        session_id: String,
        cwd: PathBuf,
        model: String,
        cli_version: String,
    },
    TextDelta(String),
    ThinkingDelta(String),
    AssistantMessage {
        text: String,
    },
    ToolCall {
        call_id: String,
        call: ToolCall,
    },
    ToolExecuted {
        call: ToolCall,
        output: ToolOutput,
    },
    ToolCancelled {
        call_id: String,
    },
    RateLimits(Vec<ProviderRateLimit>),
    ApiRetry {
        attempt: u32,
        delay_ms: u64,
        error: String,
    },
    TurnComplete {
        usage: TokenUsage,
        list_price_usd: Option<f64>,
        num_turns: u32,
        is_error: bool,
        subtype: String,
        result: Option<String>,
    },
}
impl ClaudeSessionEvent {
    pub fn to_json(&self) -> Value {
        use serde_json::json;
        match self {
            Self::Init {
                session_id,
                cwd,
                model,
                cli_version,
            } => {
                json!({"type":"init","sessionId":session_id,"cwd":cwd,"model":model,"cliVersion":cli_version})
            }
            Self::TextDelta(text) => json!({"type":"textDelta","text":text}),
            Self::ThinkingDelta(text) => json!({"type":"thinkingDelta","text":text}),
            Self::AssistantMessage { text } => json!({"type":"assistantMessage","text":text}),
            Self::ToolCall { call_id, call } => {
                json!({"type":"toolCall","callId":call_id,"call":{"toolUseId":call.tool_use_id,"toolName":call.tool_name,"args":call.args}})
            }
            Self::ToolExecuted { call, output } => {
                json!({"type":"toolExecuted","call":{"toolUseId":call.tool_use_id,"toolName":call.tool_name,"args":call.args},"output":output.to_json()})
            }
            Self::ToolCancelled { call_id } => json!({"type":"toolCancelled","callId":call_id}),
            Self::RateLimits(data) => json!({"type":"rateLimits","data":data}),
            Self::ApiRetry {
                attempt,
                delay_ms,
                error,
            } => json!({"type":"apiRetry","attempt":attempt,"delayMs":delay_ms,"error":error}),
            Self::TurnComplete {
                usage,
                list_price_usd,
                num_turns,
                is_error,
                subtype,
                result,
            } => {
                json!({"type":"turnComplete","usage":usage,"listPriceUsd":list_price_usd,"numTurns":num_turns,"isError":is_error,"subtype":subtype,"result":result})
            }
        }
    }
}
impl ToolOutput {
    pub fn to_json(&self) -> Value {
        let content: Vec<Value> = self.content.iter().map(|item| match item {
            ToolContent::Text(text) => serde_json::json!({"type":"text","text":text}),
            ToolContent::Image { data_base64, mime_type } => serde_json::json!({"type":"image","dataBase64":data_base64,"mimeType":mime_type}),
        }).collect();
        serde_json::json!({"content":content,"isError":self.is_error})
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum ClaudeSubscriptionStatus {
    Ready {
        email: Option<String>,
        subscription_type: Option<String>,
    },
    NotLoggedIn,
    CliNotFound {
        probed: Vec<PathBuf>,
    },
    Error {
        message: String,
    },
}

pub struct ClaudeSession {
    process: Option<process::Process>,
}

impl Runtime {
    pub async fn claude_session(&self, mut config: ClaudeSessionConfig) -> Result<ClaudeSession> {
        if let Some(resume) = &config.resume
            && config.cwd != resume.cwd
        {
            return Err(EngineError::ClaudeSession(ClaudeSessionError::Protocol(
                "resume cwd differs from session cwd".into(),
            )));
        }
        if let Some(model) = config.model.strip_prefix("claude-subscription:") {
            config.model = model.to_owned();
        }
        if let Some((model, effort)) = config.model.split_once("@effort=") {
            let parsed = match effort {
                "low" => Effort::Low,
                "medium" => Effort::Medium,
                "high" => Effort::High,
                "xhigh" => Effort::Xhigh,
                "max" => Effort::Max,
                _ => {
                    return Err(EngineError::NonRetryable(
                        "Invalid Claude subscription effort".into(),
                    ));
                }
            };
            config.model = model.to_owned();
            if config.effort.is_none() {
                config.effort = Some(parsed);
            }
        } else if config.model.contains('@') {
            return Err(EngineError::NonRetryable(
                "Unknown Claude subscription model parameter".into(),
            ));
        }
        let cli = policy::resolve_cli(config.cli_path.as_deref())?;
        policy::version(&cli).await?;
        match policy::claude_subscription_status(Some(&cli)).await {
            ClaudeSubscriptionStatus::Ready { .. } => {}
            ClaudeSubscriptionStatus::NotLoggedIn => {
                return Err(ClaudeSessionError::NotLoggedIn.into());
            }
            ClaudeSubscriptionStatus::Error { message } => {
                return Err(ClaudeSessionError::Protocol(format!("auth status: {message}")).into());
            }
            ClaudeSubscriptionStatus::CliNotFound { probed } => {
                return Err(ClaudeSessionError::CliNotFound { probed }.into());
            }
        }
        let schemas = self.get_tool_schemas().await;
        let names = self.claude_tool_names().await;
        let ordered: Vec<_> = names
            .into_iter()
            .filter_map(|name| schemas.get(&name).cloned())
            .collect();
        let modes = ordered
            .iter()
            .map(|schema| {
                (
                    schema.name.clone(),
                    schema.schema_only || config.host_dispatch_all,
                )
            })
            .collect();
        let process = process::spawn(&config, &cli, modes, self.tool_executor()).await?;
        process
            .outbound
            .send(wire::initialize(
                &uuid::Uuid::new_v4().to_string(),
                &config.server_name,
                &ordered,
                config.system_prompt.as_deref(),
            ))
            .map_err(|_| ClaudeSessionError::Exited {
                status: "stdin closed".into(),
                stderr_tail: String::new(),
            })?;
        Ok(ClaudeSession {
            process: Some(process),
        })
    }
}

impl ClaudeSession {
    pub async fn send(&self, turn: UserTurn) -> Result<()> {
        self.write(wire::user(turn)?)
    }
    pub async fn next_event(&self) -> Option<Result<ClaudeSessionEvent>> {
        self.process.as_ref()?.incoming.lock().await.recv().await
    }
    pub async fn respond_tool(&self, call_id: &str, result: ToolOutput) -> Result<()> {
        let process = self
            .process
            .as_ref()
            .ok_or_else(|| ClaudeSessionError::Protocol("session closed".into()))?;
        let Some(call) = process.pending.lock().await.remove(call_id) else {
            return Err(ClaudeSessionError::Protocol(format!(
                "unknown or cancelled call: {call_id}"
            ))
            .into());
        };
        if !call.host {
            return Err(
                ClaudeSessionError::Protocol(format!("handler-backed call: {call_id}")).into(),
            );
        }
        self.write(wire::control_response(call_id, call.rpc_id, result))
    }
    pub async fn interrupt(&self) -> Result<()> {
        self.write(serde_json::json!({"type":"control_request", "request_id":uuid::Uuid::new_v4().to_string(), "request":{"subtype":"interrupt"}}))
    }
    fn write(&self, value: Value) -> Result<()> {
        self.process
            .as_ref()
            .ok_or_else(|| ClaudeSessionError::Protocol("session closed".into()))?
            .outbound
            .send(value)
            .map_err(|_| {
                ClaudeSessionError::Exited {
                    status: "stdin closed".into(),
                    stderr_tail: String::new(),
                }
                .into()
            })
    }
    pub async fn shutdown(&self) -> Result<ExitSummary> {
        process::close(self.process.as_ref().expect("session open")).await
    }
    pub async fn close(self) -> Result<ExitSummary> {
        self.shutdown().await
    }
}
impl Drop for ClaudeSession {
    fn drop(&mut self) {
        if let Some(process) = &mut self.process {
            for task in &process.tasks {
                task.abort();
            }
            if let Ok(mut child) = process.child.try_lock() {
                let _ = child.start_kill();
            }
        }
    }
}

pub async fn status(cli_path: Option<&Path>) -> ClaudeSubscriptionStatus {
    claude_subscription_status(cli_path).await
}

#[cfg(test)]
mod tests {
    use crate::runtime::{RunParams, Runtime};

    #[tokio::test]
    async fn subscription_models_use_sessions() {
        let mut runtime = Runtime::with_config(Some("claude-subscription:sonnet".into()), None);
        let error = runtime.run(RunParams::default()).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("claude-subscription models run through Runtime::claude_session")
        );
        let error = match runtime.run_stream(RunParams::default()).await {
            Ok(_) => panic!("subscription model must not enter provider streaming"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("claude-subscription models run through Runtime::claude_session")
        );
    }
}
