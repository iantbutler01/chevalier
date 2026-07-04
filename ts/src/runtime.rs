//! The `Runtime` napi class — wraps `chevalier_core::runtime::Runtime`
//! directly (the high-level engine, which already does the tool loop, streaming,
//! structured output and MCP).

use std::sync::Arc;
use std::time::Duration;

use chevalier_core::error::{Error as EngineError, Result as EngineResult};
use chevalier_core::providers::{
    AnthropicProviderConfig, CodexSubscriptionProviderConfig, CodexSubscriptionTransport,
    ProviderConfig,
};
use chevalier_core::runtime::{RunParams, Runtime as EngineRuntime, ToolFunction};
use chevalier_core::types::{CacheMarker, ToolCall};
use futures::future::BoxFuture;
use napi::bindgen_prelude::Promise;
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;
use tokio::sync::Mutex;

use crate::error::{format_error, to_napi};
use crate::messages::{Message, to_chat_message, to_conversation_message};
use crate::stream::{StreamEvent, StreamHandle};
use crate::types::{RunResult, ToolSchemaJs};

/// JS tool handler: receives the parsed JSON args (CalleeHandled=false → JS sees
/// just the value), returns a `Promise<string>`. `Weak=true` so a stored handler
/// does not keep the Node event loop alive (the awaited run()/executeToolCall
/// promise keeps it alive while in use).
type ToolHandler = ThreadsafeFunction<
    serde_json::Value,
    Promise<String>,
    serde_json::Value,
    napi::Status,
    false,
    true,
>;

/// Options for constructing a `Runtime`.
#[napi(object)]
pub struct RuntimeOptions {
    /// Provider model string, e.g. `anthropic:claude-3-5-sonnet` or
    /// `openai:gpt-4o@server_url=http://host:port/v1`.
    pub model: Option<String>,
    /// API key. Falls back to the provider's env var when omitted.
    pub api_key: Option<String>,
}

/// Options for a single `run` / `runStream` call.
#[napi(object)]
pub struct RunOptions {
    pub prompt: Option<String>,
    pub system: Option<String>,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub max_tokens: Option<u32>,
    /// Override the runtime's model for this call.
    pub model: Option<String>,
    /// Override the runtime's API key for this call.
    pub api_key: Option<String>,
    /// JSON Schema for structured output (the TS layer derives this from Zod).
    pub output_schema: Option<serde_json::Value>,
    /// Name/title for the output type (optional hint for the provider).
    pub output_type: Option<String>,
    /// Prior conversation turns (chat / toolResult / reasoning).
    pub history: Option<Vec<Message>>,
    /// Per-call timeout in milliseconds.
    pub timeout_ms: Option<f64>,
}

impl RunOptions {
    fn into_params(self) -> RunParams {
        let history = self
            .history
            .map(|items| items.iter().map(to_conversation_message).collect());
        RunParams {
            prompt: self.prompt,
            system: self.system,
            history,
            output_type: self.output_type,
            output_schema: self.output_schema,
            temperature: self.temperature.map(|v| v as f32),
            top_p: self.top_p.map(|v| v as f32),
            max_tokens: self.max_tokens,
            model: self.model,
            api_key: self.api_key,
            timeout: self.timeout_ms.map(|ms| Duration::from_millis(ms as u64)),
            retry_config: None,
        }
    }
}

/// Anthropic prompt-caching config. Cache markers are `"ephemeral"` or `"ephemeral1h"`.
#[napi(object)]
pub struct AnthropicCacheConfig {
    pub automatic_prompt_caching: Option<String>,
    pub tool_definitions_cache_breakpoint: Option<String>,
}

/// Provider-specific request shaping.
#[napi(object)]
pub struct ProviderConfigInput {
    pub anthropic: Option<AnthropicCacheConfig>,
    pub codex_subscription: Option<CodexSubscriptionConfigInput>,
}

/// ChatGPT Codex subscription provider config.
#[napi(object)]
pub struct CodexSubscriptionConfigInput {
    pub token: String,
    pub account_id: Option<String>,
    pub base_url: Option<String>,
    pub transport: Option<String>,
    pub sse_header_timeout_ms: Option<f64>,
    pub websocket_connect_timeout_ms: Option<f64>,
    pub reasoning_effort: Option<String>,
    pub reasoning_summary: Option<String>,
    pub text_verbosity: Option<String>,
    pub service_tier: Option<String>,
}

fn cache_marker(s: &str) -> Option<CacheMarker> {
    match s {
        "ephemeral" => Some(CacheMarker::Ephemeral),
        "ephemeral1h" => Some(CacheMarker::Ephemeral1h),
        _ => None,
    }
}

fn codex_subscription_transport(s: &str) -> Option<CodexSubscriptionTransport> {
    match s {
        "auto" => Some(CodexSubscriptionTransport::Auto),
        "websocket" | "ws" => Some(CodexSubscriptionTransport::WebSocket),
        "sse" => Some(CodexSubscriptionTransport::Sse),
        _ => None,
    }
}

/// The Chevalier agent runtime.
#[napi]
pub struct Runtime {
    inner: Arc<Mutex<EngineRuntime>>,
}

#[napi]
impl Runtime {
    #[napi(constructor)]
    pub fn new(options: Option<RuntimeOptions>) -> Self {
        let (model, api_key) = match options {
            Some(o) => (o.model, o.api_key),
            None => (None, None),
        };
        let engine = EngineRuntime::with_config(model, api_key);
        Self {
            inner: Arc::new(Mutex::new(engine)),
        }
    }

    /// Non-streaming inference call.
    #[napi]
    pub async fn run(&self, options: RunOptions) -> napi::Result<RunResult> {
        let inner = self.inner.clone();
        let params = options.into_params();
        let mut guard = inner.lock().await;
        let resp = guard.run(params).await.map_err(to_napi)?;
        Ok(RunResult::from(resp))
    }

    /// Register a tool backed by an async JS handler. The handler receives the
    /// parsed JSON args and returns `Promise<string>`. The engine invokes it via
    /// `executeToolCall` (the agent loop is consumer-driven).
    #[napi(
        ts_args_type = "name: string, description: string, schema: any, handler: (args: any) => Promise<string>"
    )]
    pub async fn tool(
        &self,
        name: String,
        description: String,
        schema: serde_json::Value,
        handler: ToolHandler,
    ) -> napi::Result<()> {
        let handler = Arc::new(handler);
        let tool_fn = ToolFunction::Async(Box::new(move |args: serde_json::Value| {
            let handler = handler.clone();
            Box::pin(async move {
                let promise = handler.call_async(args).await.map_err(|e| {
                    EngineError::NonRetryable(format!("tool handler call failed: {e}"))
                })?;
                let result = promise.await.map_err(|e| {
                    EngineError::NonRetryable(format!("tool handler rejected: {e}"))
                })?;
                Ok(result)
            }) as BoxFuture<'static, EngineResult<String>>
        }));
        let guard = self.inner.lock().await;
        guard
            .register_tool_with_schema(name, description, schema, tool_fn)
            .await
            .map_err(to_napi)
    }

    /// Register a tool by schema only (no handler) — the model can call it, and
    /// the host dispatches it from the response/stream. Calling it via the engine
    /// errors (there is no handler).
    #[napi]
    pub async fn register_tool_schema(
        &self,
        name: String,
        description: String,
        schema: serde_json::Value,
    ) -> napi::Result<()> {
        let tool_fn = ToolFunction::Async(Box::new(move |_args| {
            Box::pin(async move {
                Err(EngineError::NonRetryable(
                    "tool was registered schema-only; dispatch it host-side from the tool call"
                        .to_string(),
                ))
            }) as BoxFuture<'static, EngineResult<String>>
        }));
        let guard = self.inner.lock().await;
        guard
            .register_tool_with_schema(name, description, schema, tool_fn)
            .await
            .map_err(to_napi)
    }

    /// Execute a registered (handler-backed) tool call and return its string result.
    #[napi]
    pub async fn execute_tool_call(
        &self,
        tool_name: String,
        args: serde_json::Value,
    ) -> napi::Result<String> {
        let tc = ToolCall::new(tool_name, args);
        let guard = self.inner.lock().await;
        guard.execute_tool_call(&tc).await.map_err(to_napi)
    }

    /// Streaming inference call. Returns a `StreamHandle`; pull events with
    /// `next()` until it yields `null`.
    #[napi]
    pub async fn run_stream(&self, options: RunOptions) -> napi::Result<StreamHandle> {
        use futures::StreamExt;
        let inner = self.inner.clone();
        let params = options.into_params();
        // Unbounded so the driver never parks on `send` waiting for the consumer.
        // It drains the engine stream to completion (or until aborted) and only
        // then releases the owned runtime lock — so a mid-stream `executeToolCall`
        // blocks at most until the stream ends, never permanently. `close()`
        // aborts the task to release the lock + provider request immediately.
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<StreamEvent, String>>();
        let task = tokio::spawn(async move {
            // `guard` must outlive `stream` (the engine's `impl Stream` return
            // captures the guard's lifetime under Rust 2024 rules).
            let mut guard = inner.lock_owned().await;
            let result = guard.run_stream(params).await;
            let mut stream = match result {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(Err(format_error(&e)));
                    return;
                }
            };
            while let Some(item) = stream.next().await {
                let msg = match item {
                    Ok(ev) => Ok(StreamEvent::from(ev)),
                    Err(e) => Err(format_error(&e)),
                };
                if tx.send(msg).is_err() {
                    break; // consumer dropped the handle
                }
            }
        });
        Ok(StreamHandle {
            rx: Arc::new(Mutex::new(rx)),
            abort: task.abort_handle(),
        })
    }

    /// All registered tool schemas (name, description, JSON-schema parameters).
    #[napi]
    pub async fn get_tool_schemas(&self) -> Vec<ToolSchemaJs> {
        let guard = self.inner.lock().await;
        guard
            .get_tool_schemas()
            .await
            .into_values()
            .map(ToolSchemaJs::from)
            .collect()
    }

    /// Set a structured system-message prefix applied to subsequent runs.
    #[napi]
    pub async fn set_system_messages(&self, messages: Vec<Message>) {
        let msgs = messages.iter().map(to_chat_message).collect();
        self.inner.lock().await.set_system_messages(msgs).await;
    }

    /// Set the default prompt used when a run omits `prompt`.
    #[napi]
    pub async fn set_default_prompt(&self, prompt: String) {
        self.inner.lock().await.set_default_prompt(prompt).await;
    }

    /// Set provider-specific request shaping (e.g. Anthropic prompt caching).
    #[napi]
    pub async fn set_provider_config(&self, config: ProviderConfigInput) {
        let pc = if let Some(codex) = config.codex_subscription {
            Some(ProviderConfig::CodexSubscription(Box::new(
                CodexSubscriptionProviderConfig {
                    token: codex.token,
                    account_id: codex.account_id,
                    base_url: codex.base_url,
                    transport: codex
                        .transport
                        .as_deref()
                        .and_then(codex_subscription_transport),
                    sse_header_timeout: codex
                        .sse_header_timeout_ms
                        .map(|ms| Duration::from_millis(ms.max(0.0).floor() as u64)),
                    websocket_connect_timeout: codex
                        .websocket_connect_timeout_ms
                        .map(|ms| Duration::from_millis(ms.max(0.0).floor() as u64)),
                    reasoning_effort: codex.reasoning_effort,
                    reasoning_summary: codex.reasoning_summary,
                    text_verbosity: codex.text_verbosity,
                    service_tier: codex.service_tier,
                },
            )))
        } else {
            config.anthropic.map(|a| {
                ProviderConfig::Anthropic(AnthropicProviderConfig {
                    automatic_prompt_caching: a
                        .automatic_prompt_caching
                        .as_deref()
                        .and_then(cache_marker),
                    tool_definitions_cache_breakpoint: a
                        .tool_definitions_cache_breakpoint
                        .as_deref()
                        .and_then(cache_marker),
                })
            })
        };
        self.inner.lock().await.set_provider_config(pc).await;
    }

    /// Accumulated assistant text from the last run/stream.
    #[napi]
    pub async fn raw_response(&self) -> String {
        self.inner.lock().await.raw_response().await
    }

    /// Accumulated reasoning text from the last run/stream.
    #[napi]
    pub async fn reasoning(&self) -> String {
        self.inner.lock().await.reasoning().await
    }

    /// Accumulated reasoning segments (JSON) from the last run/stream.
    #[napi]
    pub async fn reasoning_segments(&self) -> serde_json::Value {
        let segs = self.inner.lock().await.reasoning_segments().await;
        serde_json::to_value(segs).unwrap_or(serde_json::Value::Null)
    }

    /// Connect to an MCP server and register all its tools onto this runtime.
    /// Transport is auto-detected: `http(s)://` → HTTP, `ws(s)://` → WebSocket,
    /// otherwise stdio (a command to spawn).
    #[napi]
    pub async fn mcp(&self, uri: String) -> napi::Result<()> {
        self.inner.lock().await.mcp(uri).await.map_err(to_napi)
    }

    /// Like `mcp`, but namespaces the registered tools as `{label}_{tool}`.
    #[napi]
    pub async fn mcp_as(&self, uri: String, label: String) -> napi::Result<()> {
        self.inner
            .lock()
            .await
            .mcp_as(uri, &label)
            .await
            .map_err(to_napi)
    }

    /// Release all registered tools, dropping their JS handler references. Call
    /// this when done with a Runtime whose tool handlers capture the Runtime
    /// itself (otherwise the napi_ref ↔ JS closure cycle prevents GC). Idempotent.
    #[napi]
    pub async fn dispose(&self) {
        let guard = self.inner.lock().await;
        let names: Vec<String> = guard.get_tool_schemas().await.into_keys().collect();
        for name in names {
            guard.unregister_tool(&name).await;
        }
    }
}
