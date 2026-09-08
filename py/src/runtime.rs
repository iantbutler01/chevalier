use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chevalier_core::error::{Error as EngineError, Result as EngineResult};
use chevalier_core::providers::{
    AnthropicProviderConfig, CodexSubscriptionProviderConfig, CodexSubscriptionTransport,
    KimiCodingAuthKind, KimiCodingProviderConfig, ProviderConfig,
};
use chevalier_core::runtime::{RunParams, Runtime as EngineRuntime, ToolFunction};
use chevalier_core::types::{CacheMarker, ToolCall};
use futures::StreamExt;
use futures::future::BoxFuture;
use pyo3::prelude::*;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::errors::{format_error, to_py_err};
use crate::json::{from_python, to_python, value_from_python};
use crate::messages::{Message, to_chat_message, to_conversation_message};
use crate::stream::StreamHandle;
use crate::types::{AssistantResponse, ToolSchemaOutput};

#[derive(Default, Deserialize)]
struct RuntimeOptions {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
}

#[derive(Default, Deserialize)]
struct RunOptions {
    responses: Option<ResponsesOptionsInput>,
    previous_response_id: Option<String>,
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    system: Option<String>,
    #[serde(default)]
    temperature: Option<f64>,
    #[serde(default)]
    top_p: Option<f64>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    output_schema: Option<serde_json::Value>,
    #[serde(default)]
    output_type: Option<String>,
    #[serde(default)]
    history: Option<Vec<Message>>,
    #[serde(default)]
    timeout_ms: Option<f64>,
}

#[derive(Deserialize)]
struct ResponsesOptionsInput {
    websocket: Option<bool>,
    compaction_threshold: Option<u32>,
}

impl RunOptions {
    fn into_params(self) -> RunParams {
        RunParams {
            prompt: self.prompt,
            system: self.system,
            history: self
                .history
                .map(|items| items.iter().map(to_conversation_message).collect()),
            output_type: self.output_type,
            output_schema: self.output_schema,
            temperature: self.temperature.map(|value| value as f32),
            top_p: self.top_p.map(|value| value as f32),
            max_tokens: self.max_tokens,
            reasoning_effort: None,
            model: self.model,
            api_key: self.api_key,
            timeout: self
                .timeout_ms
                .map(|milliseconds| Duration::from_millis(milliseconds as u64)),
            retry_config: None,
            previous_response_id: self.previous_response_id,
            responses: self.responses.map(|options| {
                chevalier_core::providers::responses_control::ResponsesOptions {
                    websocket: options.websocket.unwrap_or(false),
                    compaction_threshold: options.compaction_threshold,
                    control: None,
                }
            }),
        }
    }
}

#[derive(Deserialize)]
struct AnthropicCacheConfig {
    #[serde(default)]
    automatic_prompt_caching: Option<String>,
    #[serde(default)]
    tool_definitions_cache_breakpoint: Option<String>,
}

#[derive(Deserialize)]
struct KimiCodingConfigInput {
    token: String,
    auth_kind: String,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    user_agent: Option<String>,
}

#[derive(Deserialize)]
struct CodexSubscriptionConfigInput {
    prompt_cache_key: Option<String>,
    token: String,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    transport: Option<String>,
    #[serde(default)]
    sse_header_timeout_ms: Option<f64>,
    #[serde(default)]
    websocket_connect_timeout_ms: Option<f64>,
    #[serde(default)]
    reasoning_effort: Option<String>,
    #[serde(default)]
    reasoning_summary: Option<String>,
    #[serde(default)]
    text_verbosity: Option<String>,
    #[serde(default)]
    service_tier: Option<String>,
}

#[derive(Deserialize)]
struct ProviderConfigInput {
    #[serde(default)]
    anthropic: Option<AnthropicCacheConfig>,
    #[serde(default)]
    codex_subscription: Option<CodexSubscriptionConfigInput>,
    #[serde(default)]
    kimi_coding: Option<KimiCodingConfigInput>,
}

fn cache_marker(value: &str) -> Option<CacheMarker> {
    match value {
        "ephemeral" => Some(CacheMarker::Ephemeral),
        "ephemeral1h" => Some(CacheMarker::Ephemeral1h),
        _ => None,
    }
}

fn codex_subscription_transport(value: &str) -> Option<CodexSubscriptionTransport> {
    match value {
        "auto" => Some(CodexSubscriptionTransport::Auto),
        "websocket" | "ws" => Some(CodexSubscriptionTransport::WebSocket),
        "sse" => Some(CodexSubscriptionTransport::Sse),
        _ => None,
    }
}

fn provider_config(config: ProviderConfigInput) -> Option<ProviderConfig> {
    if let Some(kimi) = config.kimi_coding {
        Some(ProviderConfig::KimiCoding(Box::new(
            KimiCodingProviderConfig {
                token: kimi.token,
                auth_kind: if kimi.auth_kind == "oauth" {
                    KimiCodingAuthKind::OAuth
                } else {
                    KimiCodingAuthKind::ApiKey
                },
                base_url: kimi.base_url,
                user_agent: kimi.user_agent,
            },
        )))
    } else if let Some(codex) = config.codex_subscription {
        Some(ProviderConfig::CodexSubscription(Box::new(
            CodexSubscriptionProviderConfig {
                token: codex.token,
                account_id: codex.account_id,
                prompt_cache_key: codex.prompt_cache_key,
                base_url: codex.base_url,
                transport: codex
                    .transport
                    .as_deref()
                    .and_then(codex_subscription_transport),
                sse_header_timeout: codex
                    .sse_header_timeout_ms
                    .map(|value| Duration::from_millis(value.max(0.0).floor() as u64)),
                websocket_connect_timeout: codex
                    .websocket_connect_timeout_ms
                    .map(|value| Duration::from_millis(value.max(0.0).floor() as u64)),
                reasoning_effort: codex.reasoning_effort,
                reasoning_summary: codex.reasoning_summary,
                text_verbosity: codex.text_verbosity,
                service_tier: codex.service_tier,
            },
        )))
    } else {
        config.anthropic.map(|anthropic| {
            ProviderConfig::Anthropic(AnthropicProviderConfig {
                automatic_prompt_caching: anthropic
                    .automatic_prompt_caching
                    .as_deref()
                    .and_then(cache_marker),
                tool_definitions_cache_breakpoint: anthropic
                    .tool_definitions_cache_breakpoint
                    .as_deref()
                    .and_then(cache_marker),
            })
        })
    }
}

pub(crate) struct PythonToolHandler {
    callable: PyObject,
    locals: pyo3_async_runtimes::TaskLocals,
}

impl PythonToolHandler {
    pub(crate) fn new(py: Python<'_>, callable: PyObject) -> PyResult<Self> {
        Ok(Self {
            callable,
            locals: pyo3_async_runtimes::tokio::get_current_locals(py)?,
        })
    }
}

type PythonFuture = Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send>>;

pub(crate) async fn invoke_python_tool(
    handler: Arc<PythonToolHandler>,
    args: serde_json::Value,
) -> EngineResult<String> {
    let future: PythonFuture = Python::with_gil(|py| -> EngineResult<PythonFuture> {
        let args = to_python(py, &args)
            .map_err(|error| EngineError::NonRetryable(format!("tool args failed: {error}")))?;
        let coroutine = handler.callable.call1(py, (args,)).map_err(|error| {
            EngineError::NonRetryable(format!("tool handler call failed: {error}"))
        })?;
        let locals = handler.locals.clone_ref(py);
        let future =
            pyo3_async_runtimes::into_future_with_locals(&locals, coroutine.into_bound(py))
                .map_err(|error| {
                    EngineError::NonRetryable(format!(
                        "tool handler must return a coroutine yielding a string: {error}"
                    ))
                })?;
        Ok(Box::pin(future))
    })?;

    let value = future
        .await
        .map_err(|error| EngineError::NonRetryable(format!("tool handler rejected: {error}")))?;
    Python::with_gil(|py| {
        value.extract::<String>(py).map_err(|error| {
            EngineError::NonRetryable(format!("tool handler must return a string: {error}"))
        })
    })
}

#[pyclass(module = "chevalier.chevalier")]
pub struct Runtime {
    executor: chevalier_core::runtime::ToolExecutor,
    inner: Arc<Mutex<EngineRuntime>>,
}

#[pymethods]
impl Runtime {
    #[new]
    #[pyo3(signature = (options=None))]
    fn new(options: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let options: RuntimeOptions = options.map(from_python).transpose()?.unwrap_or_default();
        let engine = EngineRuntime::with_config(options.model, options.api_key);
        Ok(Self {
            executor: engine.tool_executor(),
            inner: Arc::new(Mutex::new(engine)),
        })
    }

    fn run<'py>(&self, py: Python<'py>, options: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let params = from_python::<RunOptions>(options)?.into_params();
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut runtime = inner.lock().await;
            let response = runtime.run(params).await.map_err(to_py_err)?;
            Ok(AssistantResponse::from(response))
        })
    }

    #[pyo3(signature = (handler, *, name=None, description=None))]
    fn tool<'py>(
        &self,
        py: Python<'py>,
        handler: PyObject,
        name: Option<String>,
        description: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (name, description, schema, handler): (String, String, PyObject, PyObject) = py
            .import("chevalier._tools")?
            .getattr("prepare_tool")?
            .call1((handler, name, description))?
            .extract()?;
        let schema = value_from_python(schema.bind(py))?;
        let handler = Arc::new(PythonToolHandler::new(py, handler)?);
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let function = ToolFunction::Async(Box::new(move |args| {
                let handler = handler.clone();
                Box::pin(async move { invoke_python_tool(handler, args).await })
                    as BoxFuture<'static, EngineResult<String>>
            }));
            inner
                .lock()
                .await
                .register_tool_with_schema(name, description, schema, function)
                .await
                .map_err(to_py_err)
        })
    }

    fn register_tool_schema<'py>(
        &self,
        py: Python<'py>,
        name: String,
        description: String,
        schema: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let schema = value_from_python(schema)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let function = ToolFunction::Async(Box::new(|_args| {
                Box::pin(async {
                    Err(EngineError::NonRetryable(
                        "tool was registered schema-only; dispatch it host-side from the tool call"
                            .to_string(),
                    ))
                }) as BoxFuture<'static, EngineResult<String>>
            }));
            inner
                .lock()
                .await
                .register_tool_with_schema(name, description, schema, function)
                .await
                .map_err(to_py_err)
        })
    }

    fn execute_tool_call<'py>(
        &self,
        py: Python<'py>,
        tool_name: String,
        args: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let args = value_from_python(args)?;
        let executor = self.executor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            executor
                .execute(&ToolCall::new(tool_name, args))
                .await
                .map_err(to_py_err)
        })
    }

    fn run_stream<'py>(
        &self,
        py: Python<'py>,
        options: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut params = from_python::<RunOptions>(options)?.into_params();
        let control = params
            .responses
            .as_mut()
            .filter(|options| options.websocket)
            .map(|options| {
                let control =
                    chevalier_core::providers::responses_control::ResponsesControl::default();
                options.control = Some(control.clone());
                control
            });
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            let task = tokio::spawn(async move {
                let mut runtime = inner.lock_owned().await;
                let result = runtime.run_stream(params).await;
                let mut stream = match result {
                    Ok(stream) => stream,
                    Err(error) => {
                        let _ = sender.send(Err(format_error(&error)));
                        return;
                    }
                };
                while let Some(item) = stream.next().await {
                    let item = item.map_err(|error| format_error(&error));
                    if sender.send(item).is_err() {
                        break;
                    }
                }
            });
            Ok(StreamHandle {
                control,
                receiver: Arc::new(Mutex::new(receiver)),
                abort: task.abort_handle(),
            })
        })
    }

    fn get_tool_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let schemas: Vec<_> = inner
                .lock()
                .await
                .get_tool_schemas()
                .await
                .into_values()
                .map(ToolSchemaOutput::from)
                .collect();
            Python::with_gil(|py| to_python(py, &schemas))
        })
    }

    #[pyo3(signature = (names=None))]
    fn set_model_tool_names<'py>(
        &self,
        py: Python<'py>,
        names: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .set_model_tool_names(names)
                .await
                .map_err(to_py_err)
        })
    }

    fn set_tool_async<'py>(
        &self,
        py: Python<'py>,
        name: String,
        asynchronous: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .set_tool_async(&name, asynchronous)
                .await
                .map_err(to_py_err)
        })
    }

    fn set_system_messages<'py>(
        &self,
        py: Python<'py>,
        messages: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let messages = from_python::<Vec<Message>>(messages)?;
        let messages = messages.iter().map(to_chat_message).collect();
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.set_system_messages(messages).await;
            Ok(())
        })
    }

    fn set_default_prompt<'py>(
        &self,
        py: Python<'py>,
        prompt: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.set_default_prompt(prompt).await;
            Ok(())
        })
    }

    fn set_provider_config<'py>(
        &self,
        py: Python<'py>,
        config: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let config = provider_config(from_python(config)?);
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.set_provider_config(config).await;
            Ok(())
        })
    }

    fn raw_response<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(inner.lock().await.raw_response().await)
        })
    }

    fn reasoning<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(inner.lock().await.reasoning().await)
        })
    }

    fn reasoning_segments<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let segments = inner.lock().await.reasoning_segments().await;
            Python::with_gil(|py| to_python(py, &segments))
        })
    }

    fn mcp<'py>(&self, py: Python<'py>, uri: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.mcp(uri).await.map_err(to_py_err)
        })
    }

    fn mcp_as<'py>(
        &self,
        py: Python<'py>,
        uri: String,
        label: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .mcp_as(uri, &label)
                .await
                .map_err(to_py_err)
        })
    }

    fn dispose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let runtime = inner.lock().await;
            let names: Vec<_> = runtime.get_tool_schemas().await.into_keys().collect();
            for name in names {
                runtime.unregister_tool(&name).await;
            }
            Ok(())
        })
    }
}
