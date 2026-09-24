use std::{path::PathBuf, sync::Arc, time::Duration};
use chevalier_core::{claude_subscription::{self, ClaudeSession as EngineSession, ClaudeSessionConfig, Effort, ResumeTarget, ToolOutput, UserTurn}, runtime::Runtime as EngineRuntime, types::{MediaPart, MediaSource}};
use napi_derive::napi;
use serde::Deserialize;
use serde_json::Value;
use crate::error::to_napi;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigInput {
    model: String,
    system_prompt: Option<String>,
    effort: Option<Effort>,
    resume: Option<ResumeInput>,
    cwd: Option<String>,
    cli_path: Option<String>,
    client_app: Option<String>,
    server_name: Option<String>,
    #[serde(default)]
    host_dispatch_all: bool,
    idle_timeout_ms: Option<u64>,
    max_turns: Option<u32>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResumeInput { session_id: String, cwd: String }

pub(crate) fn config(value: Value) -> napi::Result<ClaudeSessionConfig> {
    let input: ConfigInput = serde_json::from_value(value).map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let mut config = ClaudeSessionConfig::default();
    config.model = input.model;
    config.system_prompt = input.system_prompt;
    config.effort = input.effort;
    config.resume = input.resume.map(|r| ResumeTarget { session_id: r.session_id, cwd: PathBuf::from(r.cwd) });
    if let Some(cwd) = input.cwd { config.cwd = PathBuf::from(cwd); }
    config.cli_path = input.cli_path.map(PathBuf::from);
    if let Some(app) = input.client_app { config.client_app = app; }
    if let Some(server) = input.server_name { config.server_name = server; }
    config.host_dispatch_all = input.host_dispatch_all;
    if let Some(ms) = input.idle_timeout_ms { config.idle_timeout = Duration::from_millis(ms); }
    config.max_turns = input.max_turns;
    Ok(config)
}

#[napi]
pub struct ClaudeSession { inner: Arc<EngineSession> }

impl ClaudeSession { pub(crate) fn new(inner: EngineSession) -> Self { Self { inner: Arc::new(inner) } } }

#[napi]
impl ClaudeSession {
    #[napi]
    pub async fn next(&self) -> napi::Result<Option<Value>> {
        self.inner.next_event().await.transpose().map(|event| event.map(|event| event.to_json())).map_err(to_napi)
    }
    #[napi]
    pub async fn send(&self, turn: Value) -> napi::Result<()> {
        let text = turn["text"].as_str().ok_or_else(|| napi::Error::from_reason("turn.text is required"))?.to_string();
        let images = turn["images"].as_array().map(|images| images.iter().map(|image| {
            let data = image["imageBase64"].as_str().or_else(|| image["dataBase64"].as_str())
                .ok_or_else(|| napi::Error::from_reason("images require imageBase64"))?;
            let mime = image["mimeType"].as_str().ok_or_else(|| napi::Error::from_reason("images require mimeType"))?;
            Ok(MediaPart::image(MediaSource::base64(data, mime)))
        }).collect::<napi::Result<Vec<_>>>()).transpose()?.unwrap_or_default();
        self.inner.send(UserTurn { text, images }).await.map_err(to_napi)
    }
    #[napi]
    pub async fn respond_tool(&self, call_id: String, output: Value) -> napi::Result<()> {
        let output = ToolOutput::from_json(&output).map_err(to_napi)?;
        self.inner.respond_tool(&call_id, output).await.map_err(to_napi)
    }
    #[napi]
    pub async fn interrupt(&self) -> napi::Result<()> { self.inner.interrupt().await.map_err(to_napi) }
    #[napi]
    pub async fn close(&self) -> napi::Result<Value> {
        let summary = self.inner.shutdown().await.map_err(to_napi)?;
        serde_json::to_value(summary).map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

#[napi]
pub async fn claude_subscription_status(cli_path: Option<String>) -> napi::Result<Value> {
    serde_json::to_value(claude_subscription::claude_subscription_status(cli_path.as_deref().map(std::path::Path::new)).await)
        .map_err(|e| napi::Error::from_reason(e.to_string()))
}

pub(crate) async fn start(runtime: &EngineRuntime, value: Value) -> napi::Result<ClaudeSession> {
    runtime.claude_session(config(value)?).await.map(ClaudeSession::new).map_err(to_napi)
}
