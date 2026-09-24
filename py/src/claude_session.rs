use std::{path::PathBuf, sync::Arc, time::Duration};
use chevalier_core::{claude_subscription::{self, ClaudeSession as EngineSession, ClaudeSessionConfig, Effort, ResumeTarget, ToolOutput, UserTurn}, types::{MediaPart, MediaSource}};
use pyo3::{exceptions::PyStopAsyncIteration, prelude::*};
use serde::Deserialize;
use crate::{errors::{invalid_argument, to_py_err}, json::{from_python, to_python, value_from_python}};

fn event_to_python(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    let name = match value["type"].as_str() {
        Some("init") => "ClaudeInitEvent",
        Some("textDelta") => "ClaudeTextDeltaEvent",
        Some("thinkingDelta") => "ClaudeThinkingDeltaEvent",
        Some("assistantMessage") => "ClaudeAssistantMessageEvent",
        Some("toolCall") => "ClaudeToolCallEvent",
        Some("toolExecuted") => "ClaudeToolExecutedEvent",
        Some("toolCancelled") => "ClaudeToolCancelledEvent",
        Some("rateLimits") => "ClaudeRateLimitsEvent",
        Some("apiRetry") => "ClaudeApiRetryEvent",
        Some("turnComplete") => "ClaudeTurnCompleteEvent",
        _ => return Err(invalid_argument("unknown Claude session event")),
    };
    let data = to_python(py, value)?;
    py.import("chevalier")?.getattr(name)?.call1((data,)).map(Bound::unbind)
}

#[derive(Deserialize)]
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
struct ResumeInput { session_id: String, cwd: String }

pub(crate) fn config(value: &Bound<'_, PyAny>) -> PyResult<ClaudeSessionConfig> {
    let input: ConfigInput = from_python(value)?;
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

#[pyclass(module = "chevalier.chevalier")]
pub struct ClaudeSession { inner: Arc<EngineSession> }
impl ClaudeSession { pub(crate) fn new(inner: EngineSession) -> Self { Self { inner: Arc::new(inner) } } }

#[pymethods]
impl ClaudeSession {
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match session.next_event().await {
                Some(Ok(event)) => Python::with_gil(|py| event_to_python(py, &event.to_json()).map(Some)),
                Some(Err(error)) => Err(to_py_err(error)),
                None => Ok(None),
            }
        })
    }
    fn send<'py>(&self, py: Python<'py>, turn: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let turn = value_from_python(turn)?;
        let text = turn["text"].as_str().ok_or_else(|| invalid_argument("turn.text is required"))?.to_string();
        let images = turn["images"].as_array().map(|images| images.iter().map(|image| {
            let data = image["image_base64"].as_str().or_else(|| image["data_base64"].as_str())
                .ok_or_else(|| invalid_argument("images require image_base64"))?;
            let mime = image["mime_type"].as_str().ok_or_else(|| invalid_argument("images require mime_type"))?;
            Ok(MediaPart::image(MediaSource::base64(data, mime)))
        }).collect::<PyResult<Vec<_>>>()).transpose()?.unwrap_or_default();
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { session.send(UserTurn { text, images }).await.map_err(to_py_err) })
    }
    fn respond_tool<'py>(&self, py: Python<'py>, call_id: String, output: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let value = value_from_python(output)?;
        let normalized = serde_json::json!({"content":value["content"], "isError":value["is_error"]});
        let output = ToolOutput::from_json(&normalized).map_err(to_py_err)?;
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { session.respond_tool(&call_id, output).await.map_err(to_py_err) })
    }
    fn interrupt<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { session.interrupt().await.map_err(to_py_err) })
    }
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let summary = session.shutdown().await.map_err(to_py_err)?;
            Python::with_gil(|py| to_python(py, &summary))
        })
    }
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match session.next_event().await {
                Some(Ok(event)) => Python::with_gil(|py| event_to_python(py, &event.to_json())),
                Some(Err(error)) => Err(to_py_err(error)),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }
}

#[pyfunction]
#[pyo3(signature = (cli_path=None))]
pub fn claude_subscription_status<'py>(py: Python<'py>, cli_path: Option<String>) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let status = claude_subscription::claude_subscription_status(cli_path.as_deref().map(std::path::Path::new)).await;
        Python::with_gil(|py| to_python(py, &status))
    })
}
