//! ChatGPT Codex subscription Responses client.
//!
//! This provider targets the ChatGPT Codex backend used by subscription
//! accounts. It intentionally reuses the OpenAI Responses message/tool format
//! and streaming parser, but owns the ChatGPT-specific URL and auth headers.

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::{SinkExt, StreamExt, channel::mpsc};
use http::{HeaderMap, HeaderName, HeaderValue, Uri};
use reqwest::{StatusCode, header};
use serde_json::Value;
use std::pin::Pin;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::providers::{
    CodexSubscriptionProviderConfig, CodexSubscriptionTransport, GenerationConfig,
    GenerationResponse, InferenceClient, StreamChunk, TraceCallback,
};
use crate::retry::{RetryConfig, retry_with_backoff};
use crate::schema::fix_tool_schema_for_provider;
use crate::types::{
    AssistantResponse, Provider, ProviderRateLimit, ProviderRateLimitScope, ResponsePart,
    TokenUsage, ToolCall,
};
use crate::utils::{
    ConversationMessage, convert_messages_to_responses_input, parse_json_value_strict_str,
    parse_sse_stream, validate_image_input_supported,
};

use super::openai_responses_streaming::{ResponsesToolAccumulator, parse_openai_responses_event};

const DEFAULT_CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api";
const JWT_CLAIM_PATH: &str = "https://api.openai.com/auth";
const OPENAI_BETA_RESPONSES: &str = "responses=experimental";
const OPENAI_BETA_RESPONSES_WEBSOCKETS: &str = "responses_websockets=2026-02-06";
const DEFAULT_SSE_HEADER_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_WEBSOCKET_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const CODEX_RATE_LIMIT_REACHED_TYPE_HEADER: &str = "x-codex-rate-limit-reached-type";
const CODEX_PRIMARY_USED_PERCENT_HEADER: &str = "x-codex-primary-used-percent";
const CODEX_PRIMARY_WINDOW_MINUTES_HEADER: &str = "x-codex-primary-window-minutes";
const CODEX_PRIMARY_RESET_AT_HEADER: &str = "x-codex-primary-reset-at";
const CODEX_SECONDARY_USED_PERCENT_HEADER: &str = "x-codex-secondary-used-percent";
const CODEX_SECONDARY_WINDOW_MINUTES_HEADER: &str = "x-codex-secondary-window-minutes";
const CODEX_SECONDARY_RESET_AT_HEADER: &str = "x-codex-secondary-reset-at";

/// ChatGPT Codex Responses client.
pub struct OpenAICodexResponsesClient {
    model: String,
    token: String,
    account_id: String,
    api_url: String,
    websocket_url: String,
    reasoning: Option<String>,
    reasoning_summary: Option<String>,
    text_verbosity: Option<String>,
    service_tier: Option<String>,
    transport: CodexSubscriptionTransport,
    sse_header_timeout: Duration,
    websocket_connect_timeout: Duration,
    trace_callback: Option<TraceCallback>,
}

impl Clone for OpenAICodexResponsesClient {
    fn clone(&self) -> Self {
        Self {
            model: self.model.clone(),
            token: self.token.clone(),
            account_id: self.account_id.clone(),
            api_url: self.api_url.clone(),
            websocket_url: self.websocket_url.clone(),
            reasoning: self.reasoning.clone(),
            reasoning_summary: self.reasoning_summary.clone(),
            text_verbosity: self.text_verbosity.clone(),
            service_tier: self.service_tier.clone(),
            transport: self.transport,
            sse_header_timeout: self.sse_header_timeout,
            websocket_connect_timeout: self.websocket_connect_timeout,
            trace_callback: self.trace_callback.clone(),
        }
    }
}

impl std::fmt::Debug for OpenAICodexResponsesClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAICodexResponsesClient")
            .field("model", &self.model)
            .field("api_url", &self.api_url)
            .field("websocket_url", &self.websocket_url)
            .field("reasoning", &self.reasoning)
            .field("reasoning_summary", &self.reasoning_summary)
            .field("text_verbosity", &self.text_verbosity)
            .field("service_tier", &self.service_tier)
            .field("transport", &self.transport)
            .field("sse_header_timeout", &self.sse_header_timeout)
            .field("websocket_connect_timeout", &self.websocket_connect_timeout)
            .finish_non_exhaustive()
    }
}

impl OpenAICodexResponsesClient {
    /// Create a Codex subscription client from typed provider config.
    pub fn new(config: CodexSubscriptionProviderConfig, model: impl Into<String>) -> Result<Self> {
        if config.token.trim().is_empty() {
            return Err(Error::NonRetryable(
                "Codex subscription token is empty".to_string(),
            ));
        }

        let account_id = match config.account_id.filter(|id| !id.trim().is_empty()) {
            Some(id) => id,
            None => extract_account_id(&config.token)?,
        };
        let api_url = resolve_codex_url(config.base_url.as_deref());
        let websocket_url = resolve_codex_websocket_url(&api_url)?;

        Ok(Self {
            model: model.into(),
            token: config.token,
            account_id,
            api_url,
            websocket_url,
            reasoning: config.reasoning_effort,
            reasoning_summary: config.reasoning_summary,
            text_verbosity: config.text_verbosity,
            service_tier: config.service_tier,
            transport: config.transport.unwrap_or(CodexSubscriptionTransport::Auto),
            sse_header_timeout: config
                .sse_header_timeout
                .unwrap_or(DEFAULT_SSE_HEADER_TIMEOUT),
            websocket_connect_timeout: config
                .websocket_connect_timeout
                .unwrap_or(DEFAULT_WEBSOCKET_CONNECT_TIMEOUT),
            trace_callback: None,
        })
    }

    /// Set reasoning effort.
    pub fn with_reasoning(mut self, reasoning: impl Into<String>) -> Self {
        self.reasoning = Some(reasoning.into());
        self
    }

    fn normalized_tools(&self, tools: &[Value]) -> Vec<Value> {
        tools
            .iter()
            .cloned()
            .map(|mut tool| {
                fix_tool_schema_for_provider(&mut tool, "openai-codex-responses");
                tool
            })
            .collect()
    }

    fn build_request_body(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
        stream: bool,
    ) -> Result<Value> {
        let model = config.effective_model(&self.model);
        validate_image_input_supported(messages, Provider::OpenAIResponses, model)?;

        let (instructions, input_items) =
            convert_messages_to_responses_input(messages, Provider::OpenAIResponses)?;

        let mut request = serde_json::json!({
            "model": model,
            "store": false,
            "stream": stream,
            "input": input_items,
            "text": { "verbosity": self.text_verbosity.as_deref().unwrap_or("medium") },
            "include": ["reasoning.encrypted_content"],
            "parallel_tool_calls": true,
        });

        if let Some(instructions) = instructions
            && !instructions.is_empty()
        {
            request["instructions"] = serde_json::json!(instructions);
        }

        if let Some(temperature) = config.temperature {
            request["temperature"] = serde_json::json!(temperature);
        }

        if let Some(ref tools) = config.tools
            && !tools.is_empty()
        {
            request["tools"] = serde_json::json!(self.normalized_tools(tools));
            request["tool_choice"] = serde_json::json!("auto");
        }

        if self.reasoning.is_some() || self.reasoning_summary.is_some() {
            let mut reasoning = serde_json::Map::new();
            if let Some(ref effort) = self.reasoning {
                reasoning.insert("effort".to_string(), serde_json::json!(effort));
            }
            reasoning.insert(
                "summary".to_string(),
                serde_json::json!(self.reasoning_summary.as_deref().unwrap_or("auto")),
            );
            request["reasoning"] = serde_json::Value::Object(reasoning);
        }

        if let Some(ref service_tier) = self.service_tier {
            request["service_tier"] = serde_json::json!(service_tier);
        }

        if let Some(ref schema) = config.output_schema {
            let type_name = config.output_type_name.as_deref().unwrap_or("response");
            request["text"] = serde_json::json!({
                "format": {
                    "type": "json_schema",
                    "name": type_name,
                    "schema": schema,
                    "strict": true
                },
                "verbosity": self.text_verbosity.as_deref().unwrap_or("medium")
            });
        }

        Ok(request)
    }

    fn sse_request_builder(
        &self,
        body: &Value,
        timeout: Option<Duration>,
    ) -> reqwest::RequestBuilder {
        reqwest::Client::new()
            .post(&self.api_url)
            .timeout(timeout.unwrap_or(Duration::from_secs(180)))
            .header(header::AUTHORIZATION, format!("Bearer {}", self.token))
            .header("chatgpt-account-id", &self.account_id)
            .header("originator", "pi")
            .header(header::USER_AGENT, "openbracket-chevalier (rust)")
            .header("OpenAI-Beta", OPENAI_BETA_RESPONSES)
            .header(header::ACCEPT, "text/event-stream")
            .header(header::CONTENT_TYPE, "application/json")
            .json(body)
    }

    async fn make_sse_request(
        &self,
        body: Value,
        timeout: Option<Duration>,
    ) -> Result<reqwest::Response> {
        let response = tokio::time::timeout(
            self.sse_header_timeout,
            self.sse_request_builder(&body, timeout).send(),
        )
        .await
        .map_err(|_| {
            Error::Inference(format!(
                "Codex SSE response headers timed out after {}ms",
                self.sse_header_timeout.as_millis()
            ))
        })??;

        Ok(response)
    }

    fn websocket_builder(&self) -> Result<tokio_websockets::ClientBuilder<'static>> {
        let uri: Uri = self.websocket_url.parse().map_err(|e| {
            Error::NonRetryable(format!(
                "Invalid Codex websocket URL '{}': {}",
                self.websocket_url, e
            ))
        })?;

        let mut builder = tokio_websockets::ClientBuilder::from_uri(uri);
        for (name, value) in [
            ("Authorization", format!("Bearer {}", self.token)),
            ("chatgpt-account-id", self.account_id.clone()),
            ("originator", "pi".to_string()),
            ("User-Agent", "openbracket-chevalier (rust)".to_string()),
            ("OpenAI-Beta", OPENAI_BETA_RESPONSES_WEBSOCKETS.to_string()),
        ] {
            builder = builder.add_header(
                HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                    Error::NonRetryable(format!("Invalid Codex websocket header '{}': {}", name, e))
                })?,
                HeaderValue::from_str(&value).map_err(|e| {
                    Error::NonRetryable(format!("Invalid Codex websocket header '{}': {}", name, e))
                })?,
            );
        }

        Ok(builder)
    }

    async fn connect_websocket_stream(
        &self,
        body: Value,
        has_tools: bool,
    ) -> Result<Pin<Box<dyn futures::stream::Stream<Item = Result<StreamChunk>> + Send>>> {
        let websocket_request = wrap_websocket_request_body(body)?;
        let request_text = serde_json::to_string(&websocket_request)?;
        let builder = self.websocket_builder()?;

        let connect_result =
            tokio::time::timeout(self.websocket_connect_timeout, builder.connect())
                .await
                .map_err(|_| {
                    Error::Inference(format!(
                        "Codex websocket connect timed out after {}ms",
                        self.websocket_connect_timeout.as_millis()
                    ))
                })?;
        let (mut client, handshake_response) = connect_result
            .map_err(|e| Error::Inference(format!("Codex websocket connect failed: {}", e)))?;
        let rate_limits = codex_rate_limits_from_headers(handshake_response.headers());

        client
            .send(tokio_websockets::Message::text(request_text))
            .await
            .map_err(|e| Error::Inference(format!("Codex websocket send failed: {}", e)))?;

        let debug = debug_codex_stream();
        let (tx, rx) = mpsc::unbounded::<Result<StreamChunk>>();
        if !rate_limits.is_empty()
            && tx
                .unbounded_send(Ok(StreamChunk::RateLimits(rate_limits)))
                .is_err()
        {
            return Ok(Box::pin(rx));
        }
        tokio::spawn(async move {
            let mut accumulator = ResponsesToolAccumulator::new();

            while let Some(item) = client.next().await {
                let message = match item {
                    Ok(message) => message,
                    Err(error) => {
                        let _ = tx.unbounded_send(Err(Error::Inference(format!(
                            "Codex websocket read failed: {}",
                            error
                        ))));
                        return;
                    }
                };

                if message.is_close() {
                    return;
                }

                let Some(text) = message.as_text() else {
                    continue;
                };

                let event_json = match parse_json_value_strict_str(text) {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = tx.unbounded_send(Err(Error::Inference(format!(
                            "Invalid Codex websocket JSON: {}",
                            error
                        ))));
                        return;
                    }
                };

                if debug {
                    eprintln!("codex websocket event: {}", event_json);
                }

                if let Err(error) = reject_codex_error_event(&event_json) {
                    let _ = tx.unbounded_send(Err(error));
                    return;
                }

                // Live per-turn usage: the `codex.rate_limits` stream event (the ONLY
                // source on the WS transport — headers are handshake-only).
                let rate_limit_updates = codex_rate_limits_from_event(&event_json);
                if !rate_limit_updates.is_empty() {
                    let _ = tx.unbounded_send(Ok(StreamChunk::RateLimits(rate_limit_updates)));
                    continue;
                }

                let done = is_completion_event(&event_json);
                let event_json = normalize_completion_event(event_json);
                for chunk in parse_openai_responses_event(&event_json, &mut accumulator, has_tools)
                {
                    if tx.unbounded_send(Ok(chunk)).is_err() {
                        return;
                    }
                }

                if done {
                    return;
                }
            }
        });

        Ok(Box::pin(rx))
    }

    fn handle_error_response(
        &self,
        status: StatusCode,
        headers: &HeaderMap,
        body: String,
    ) -> Error {
        if let Some(friendly) = codex_friendly_error(status, headers, &body) {
            return friendly;
        }

        match status {
            StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Error::NonRetryable(format!("{}: {}", status, body))
            }
            StatusCode::TOO_MANY_REQUESTS => Error::Inference(format!("Rate limited: {}", body)),
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => Error::Inference(format!("{}: {}", status, body)),
            _ => Error::Inference(format!("{}: {}", status, body)),
        }
    }

    async fn connect_sse_stream(
        &self,
        request_body: Value,
        timeout: Option<Duration>,
        retry_config: Option<RetryConfig>,
        has_tools: bool,
    ) -> Result<Pin<Box<dyn futures::stream::Stream<Item = Result<StreamChunk>> + Send>>> {
        let config = retry_config.unwrap_or_default();
        let response = retry_with_backoff(config, || async {
            let resp = self.make_sse_request(request_body.clone(), timeout).await?;
            let status = resp.status();
            if !status.is_success() {
                let headers = resp.headers().clone();
                let error_body = resp.text().await.unwrap_or_default();
                return Err(self.handle_error_response(status, &headers, error_body));
            }
            Ok(resp)
        })
        .await?;

        let debug = debug_codex_stream();
        let rate_limits = codex_rate_limits_from_headers(response.headers());
        let sse_stream = parse_sse_stream(response);
        let chunk_stream = sse_stream.scan(
            ResponsesToolAccumulator::new(),
            move |accumulator, sse_result| {
                let sse_json = match sse_result {
                    Ok(json) => json,
                    Err(e) => return futures::future::ready(Some(vec![Err(e)])),
                };

                if debug {
                    eprintln!("codex sse event: {}", sse_json);
                }

                if let Err(error) = reject_codex_error_event(&sse_json) {
                    return futures::future::ready(Some(vec![Err(error)]));
                }

                // Live per-turn usage on the SSE transport too (the `codex.rate_limits`
                // event, same as the WS path — not the response headers).
                let rate_limit_updates = codex_rate_limits_from_event(&sse_json);
                if !rate_limit_updates.is_empty() {
                    return futures::future::ready(Some(vec![Ok(StreamChunk::RateLimits(
                        rate_limit_updates,
                    ))]));
                }

                let event_json = normalize_completion_event(sse_json);
                let chunks = parse_openai_responses_event(&event_json, accumulator, has_tools);
                futures::future::ready(Some(chunks.into_iter().map(Ok).collect()))
            },
        );

        let header_chunks = if rate_limits.is_empty() {
            Vec::new()
        } else {
            vec![Ok(StreamChunk::RateLimits(rate_limits))]
        };

        Ok(Box::pin(
            futures::stream::iter(header_chunks)
                .chain(chunk_stream.flat_map(futures::stream::iter)),
        ))
    }

    async fn connect_auto_stream(
        &self,
        request_body: Value,
        timeout: Option<Duration>,
        retry_config: Option<RetryConfig>,
        has_tools: bool,
    ) -> Result<Pin<Box<dyn futures::stream::Stream<Item = Result<StreamChunk>> + Send>>> {
        let mut websocket_stream = match self
            .connect_websocket_stream(request_body.clone(), has_tools)
            .await
        {
            Ok(stream) => stream,
            Err(websocket_error) => {
                if debug_codex_stream() {
                    eprintln!(
                        "codex websocket failed before stream start; falling back to SSE: {}",
                        websocket_error
                    );
                }
                return self
                    .connect_sse_stream(request_body, timeout, retry_config, has_tools)
                    .await;
            }
        };

        match websocket_stream.next().await {
            Some(Ok(first_chunk)) => Ok(Box::pin(
                futures::stream::once(async move { Ok(first_chunk) }).chain(websocket_stream),
            )),
            Some(Err(websocket_error)) => {
                if debug_codex_stream() {
                    eprintln!(
                        "codex websocket failed before first chunk; falling back to SSE: {}",
                        websocket_error
                    );
                }
                self.connect_sse_stream(request_body, timeout, retry_config, has_tools)
                    .await
            }
            None => {
                if debug_codex_stream() {
                    eprintln!("codex websocket closed before first chunk; falling back to SSE");
                }
                self.connect_sse_stream(request_body, timeout, retry_config, has_tools)
                    .await
            }
        }
    }
}

#[async_trait]
impl InferenceClient for OpenAICodexResponsesClient {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        let mut stream = self.connect_and_listen(messages, config).await?;
        let mut response = AssistantResponse::default();
        let mut usage = TokenUsage::default();

        while let Some(chunk) = stream.next().await {
            match chunk? {
                StreamChunk::Content(text) => {
                    response.push_output(ResponsePart::Text { text });
                }
                StreamChunk::Reasoning(text) => {
                    response.push_output(ResponsePart::Reasoning { text });
                }
                StreamChunk::Signature(value) => {
                    response.push_output(ResponsePart::Signature { value });
                }
                StreamChunk::ToolCallComplete(tool) => {
                    response.push_output(ResponsePart::Tool {
                        call: ToolCall::from_provider_format(tool, Provider::OpenAIResponses)?,
                    });
                }
                StreamChunk::ToolCallPartial(_) => {}
                StreamChunk::RateLimits(_) => {}
                StreamChunk::Usage {
                    input_tokens,
                    output_tokens,
                    cached_tokens,
                    cache_write_input_tokens,
                } => {
                    usage = TokenUsage {
                        input_tokens,
                        output_tokens,
                        cached_tokens,
                        cache_write_input_tokens,
                    };
                }
            }
        }

        Ok(GenerationResponse::from_assistant_response(
            response, usage, None, None,
        ))
    }

    async fn connect_and_listen(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<Pin<Box<dyn futures::stream::Stream<Item = Result<StreamChunk>> + Send>>> {
        let request_body = self.build_request_body(messages, config, true)?;
        let has_tools = config.tools.as_ref().is_some_and(|tools| !tools.is_empty());

        match self.transport {
            CodexSubscriptionTransport::WebSocket => {
                self.connect_websocket_stream(request_body, has_tools).await
            }
            CodexSubscriptionTransport::Sse => {
                self.connect_sse_stream(
                    request_body,
                    config.timeout,
                    config.retry_config.clone(),
                    has_tools,
                )
                .await
            }
            CodexSubscriptionTransport::Auto => {
                self.connect_auto_stream(
                    request_body,
                    config.timeout,
                    config.retry_config.clone(),
                    has_tools,
                )
                .await
            }
        }
    }

    fn provider(&self) -> Provider {
        Provider::OpenAIResponses
    }

    fn set_trace_callback(&mut self, callback: TraceCallback) {
        self.trace_callback = Some(callback);
    }
}

fn resolve_codex_url(base_url: Option<&str>) -> String {
    let raw = base_url
        .filter(|url| !url.trim().is_empty())
        .unwrap_or(DEFAULT_CODEX_BASE_URL)
        .trim()
        .trim_end_matches('/')
        .to_string();

    if raw.ends_with("/codex/responses") {
        raw
    } else if raw.ends_with("/codex") {
        format!("{raw}/responses")
    } else {
        format!("{raw}/codex/responses")
    }
}

fn resolve_codex_websocket_url(api_url: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(api_url)
        .map_err(|e| Error::NonRetryable(format!("Invalid Codex URL '{}': {}", api_url, e)))?;
    match url.scheme() {
        "https" => {
            url.set_scheme("wss")
                .map_err(|_| Error::NonRetryable("Invalid Codex websocket scheme".to_string()))?;
        }
        "http" => {
            url.set_scheme("ws")
                .map_err(|_| Error::NonRetryable("Invalid Codex websocket scheme".to_string()))?;
        }
        "wss" | "ws" => {}
        scheme => {
            return Err(Error::NonRetryable(format!(
                "Unsupported Codex URL scheme '{}'",
                scheme
            )));
        }
    }
    Ok(url.to_string())
}

fn extract_account_id(token: &str) -> Result<String> {
    let payload = token.split('.').nth(1).ok_or_else(|| {
        Error::NonRetryable("Failed to extract accountId from Codex token".to_string())
    })?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).map_err(|_| {
        Error::NonRetryable("Failed to decode accountId from Codex token".to_string())
    })?;
    let body: Value = serde_json::from_slice(&decoded).map_err(|_| {
        Error::NonRetryable("Failed to parse accountId from Codex token".to_string())
    })?;
    body.get(JWT_CLAIM_PATH)
        .and_then(|claim| claim.get("chatgpt_account_id"))
        .and_then(|value| value.as_str())
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .ok_or_else(|| Error::NonRetryable("No chatgpt accountId in Codex token".to_string()))
}

fn wrap_websocket_request_body(body: Value) -> Result<Value> {
    let Value::Object(mut object) = body else {
        return Err(Error::NonRetryable(
            "Codex websocket request body must be an object".to_string(),
        ));
    };
    object.insert(
        "type".to_string(),
        Value::String("response.create".to_string()),
    );
    Ok(Value::Object(object))
}

fn is_completion_event(event_json: &Value) -> bool {
    matches!(
        event_json.get("type").and_then(|value| value.as_str()),
        Some("response.done" | "response.completed" | "response.incomplete")
    )
}

fn normalize_completion_event(mut event_json: Value) -> Value {
    if matches!(
        event_json.get("type").and_then(|value| value.as_str()),
        Some("response.completed" | "response.incomplete")
    ) && let Value::Object(ref mut object) = event_json
    {
        object.insert(
            "type".to_string(),
            Value::String("response.done".to_string()),
        );
    }
    event_json
}

fn reject_codex_error_event(event_json: &Value) -> Result<()> {
    match event_json.get("type").and_then(|value| value.as_str()) {
        Some("error") => {
            let nested = event_json.get("error");
            let code = codex_error_code(event_json).or_else(|| nested.and_then(codex_error_code));
            let reset_at =
                codex_error_reset_at(event_json).or_else(|| nested.and_then(codex_error_reset_at));
            if let Some(error) = codex_usage_limit_error_from_parts(None, code, reset_at) {
                return Err(error);
            }
            let detail = codex_error_detail(event_json);
            Err(Error::Inference(format!("Codex error: {detail}")))
        }
        Some("response.failed") => {
            let error = event_json
                .get("response")
                .and_then(|response| response.get("error"));
            let code = error.and_then(codex_error_code);
            let reset_at = error.and_then(codex_error_reset_at);
            if let Some(error) = codex_usage_limit_error_from_parts(None, code, reset_at) {
                return Err(error);
            }
            let message = error
                .and_then(|value| value.get("message"))
                .and_then(|value| value.as_str())
                .unwrap_or("Codex response failed");
            Err(Error::Inference(message.to_string()))
        }
        _ => Ok(()),
    }
}

fn codex_error_detail(event_json: &Value) -> String {
    let nested = event_json.get("error");
    let message = event_json
        .get("message")
        .or_else(|| nested.and_then(|error| error.get("message")))
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty());
    if let Some(message) = message {
        return message.to_string();
    }
    let code = event_json
        .get("code")
        .or_else(|| nested.and_then(|error| error.get("code")))
        .or_else(|| nested.and_then(|error| error.get("type")))
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty());
    if let Some(code) = code {
        return code.to_string();
    }
    event_json.to_string()
}

fn codex_rate_limits_from_headers(headers: &HeaderMap) -> Vec<ProviderRateLimit> {
    [
        (
            ProviderRateLimitScope::Session,
            CODEX_PRIMARY_USED_PERCENT_HEADER,
            CODEX_PRIMARY_WINDOW_MINUTES_HEADER,
            CODEX_PRIMARY_RESET_AT_HEADER,
        ),
        (
            ProviderRateLimitScope::Subscription,
            CODEX_SECONDARY_USED_PERCENT_HEADER,
            CODEX_SECONDARY_WINDOW_MINUTES_HEADER,
            CODEX_SECONDARY_RESET_AT_HEADER,
        ),
    ]
    .into_iter()
    .filter_map(|(scope, used_header, window_header, reset_header)| {
        Some(ProviderRateLimit {
            scope,
            used_percent: header_u32(headers, used_header)?,
            window_minutes: header_u64(headers, window_header)?,
            resets_at_epoch_sec: header_u64(headers, reset_header)?,
        })
    })
    .collect()
}

/// Live rate limits arrive as a `codex.rate_limits` STREAM event on the SSE/WebSocket
/// transports — the handshake/response headers only carry them on plain HTTP (which
/// Codex uses only for the non-streaming path). Mirrors the Codex CLI's own WS handler
/// (codex-rs/codex-api/src/endpoint/responses_websocket.rs: `event.kind() ==
/// "codex.rate_limits"` -> parse_rate_limit_event). Event shape:
///   { "type": "codex.rate_limits",
///     "rate_limits": { "primary":   { used_percent, window_minutes, reset_at },
///                      "secondary": { used_percent, window_minutes, reset_at } } }
fn codex_rate_limit_window_from_event(
    scope: ProviderRateLimitScope,
    window: Option<&Value>,
) -> Option<ProviderRateLimit> {
    let window = window?;
    let used_percent = window.get("used_percent").and_then(Value::as_f64)?;
    let window_minutes = window
        .get("window_minutes")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    // Codex's event window field is `reset_at`; some transports/logs use `resets_at`.
    let resets_at = window
        .get("reset_at")
        .or_else(|| window.get("resets_at"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Some(ProviderRateLimit {
        scope,
        used_percent: used_percent.round().clamp(0.0, u32::MAX as f64) as u32,
        window_minutes,
        resets_at_epoch_sec: resets_at,
    })
}

fn codex_rate_limits_from_event(event_json: &Value) -> Vec<ProviderRateLimit> {
    if event_json.get("type").and_then(Value::as_str) != Some("codex.rate_limits") {
        return Vec::new();
    }
    let details = event_json.get("rate_limits");
    [
        codex_rate_limit_window_from_event(
            ProviderRateLimitScope::Session,
            details.and_then(|d| d.get("primary")),
        ),
        codex_rate_limit_window_from_event(
            ProviderRateLimitScope::Subscription,
            details.and_then(|d| d.get("secondary")),
        ),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn header_u32(headers: &HeaderMap, name: &str) -> Option<u32> {
    header_string(headers, name)?.parse().ok()
}

fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    header_string(headers, name)?.parse().ok()
}

fn codex_error_code(error: &Value) -> Option<&str> {
    error
        .get("code")
        .or_else(|| error.get("type"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn codex_error_reset_at(error: &Value) -> Option<u64> {
    [
        "resets_at",
        "reset_at",
        "resetsAt",
        "resetAt",
        "resetAtEpochSec",
    ]
    .into_iter()
    .find_map(|key| match error.get(key)? {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    })
}

fn is_codex_usage_limit_reached_type(value: &str) -> bool {
    let value = value.trim().to_ascii_lowercase();
    value == "usage_limit_reached"
        || (value.starts_with("workspace_") && value.ends_with("_usage_limit_reached"))
        || value.ends_with("_credits_depleted")
}

fn codex_scope_from_reached_type(value: &str) -> Option<ProviderRateLimitScope> {
    let value = value.trim().to_ascii_lowercase();
    if value.contains("secondary") || value.contains("subscription") {
        Some(ProviderRateLimitScope::Subscription)
    } else if value.contains("primary") || value.contains("session") {
        Some(ProviderRateLimitScope::Session)
    } else if value.ends_with("_credits_depleted") || value == "credits_depleted" {
        Some(ProviderRateLimitScope::Subscription)
    } else if value == "usage_limit_reached" || value.ends_with("_usage_limit_reached") {
        Some(ProviderRateLimitScope::Session)
    } else {
        None
    }
}

fn codex_usage_limit_scope(
    reached_type: Option<&str>,
    body_code: Option<&str>,
    headers: Option<&HeaderMap>,
) -> ProviderRateLimitScope {
    reached_type
        .and_then(codex_scope_from_reached_type)
        .or_else(|| body_code.and_then(codex_scope_from_reached_type))
        .or_else(|| {
            headers.and_then(|headers| {
                let has_primary = header_u64(headers, CODEX_PRIMARY_RESET_AT_HEADER).is_some();
                let has_secondary = header_u64(headers, CODEX_SECONDARY_RESET_AT_HEADER).is_some();
                match (has_primary, has_secondary) {
                    (false, true) => Some(ProviderRateLimitScope::Subscription),
                    (true, _) => Some(ProviderRateLimitScope::Session),
                    _ => None,
                }
            })
        })
        .unwrap_or(ProviderRateLimitScope::Session)
}

fn codex_reset_for_scope(headers: &HeaderMap, scope: ProviderRateLimitScope) -> Option<u64> {
    match scope {
        ProviderRateLimitScope::Session => header_u64(headers, CODEX_PRIMARY_RESET_AT_HEADER),
        ProviderRateLimitScope::Subscription => {
            header_u64(headers, CODEX_SECONDARY_RESET_AT_HEADER)
        }
    }
}

fn codex_usage_limit_error_from_parts(
    headers: Option<&HeaderMap>,
    body_code: Option<&str>,
    body_reset_at: Option<u64>,
) -> Option<Error> {
    let header_reached_type =
        headers.and_then(|headers| header_string(headers, CODEX_RATE_LIMIT_REACHED_TYPE_HEADER));
    let header_is_usage = header_reached_type
        .as_deref()
        .is_some_and(is_codex_usage_limit_reached_type);
    let body_is_usage = body_code.is_some_and(is_codex_usage_limit_reached_type);
    if !header_is_usage && !body_is_usage {
        return None;
    }

    let reached_type = if header_is_usage {
        header_reached_type.clone()
    } else {
        body_code.map(str::to_string)
    };
    let scope = codex_usage_limit_scope(header_reached_type.as_deref(), body_code, headers);
    let resets_at_epoch_sec = headers
        .and_then(|headers| codex_reset_for_scope(headers, scope))
        .or(body_reset_at);
    let reset_text = resets_at_epoch_sec
        .map(codex_epoch_seconds_to_iso_utc)
        .unwrap_or_else(|| "unknown".to_string());

    Some(Error::CodexUsageLimit {
        message: format!(
            "Codex usage limit ({}) — resets {}",
            scope.as_str(),
            reset_text
        ),
        reached_type,
        resets_at_epoch_sec,
    })
}

fn codex_epoch_seconds_to_iso_utc(epoch_seconds: u64) -> String {
    let days = (epoch_seconds / 86_400) as i64;
    let seconds_of_day = epoch_seconds % 86_400;
    let (year, month, day) = civil_from_unix_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_unix_days(days: i64) -> (i64, u64, u64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year, month as u64, day as u64)
}

fn codex_friendly_error(status: StatusCode, headers: &HeaderMap, body: &str) -> Option<Error> {
    let parsed: Value = serde_json::from_str(body).ok()?;
    let err = parsed.get("error")?;
    let code = codex_error_code(err);
    let header_is_usage = header_string(headers, CODEX_RATE_LIMIT_REACHED_TYPE_HEADER)
        .as_deref()
        .is_some_and(is_codex_usage_limit_reached_type);
    if status != StatusCode::TOO_MANY_REQUESTS
        && !code.is_some_and(is_codex_usage_limit_reached_type)
        && !header_is_usage
    {
        return None;
    }

    codex_usage_limit_error_from_parts(Some(headers), code, codex_error_reset_at(err))
}

fn debug_codex_stream() -> bool {
    std::env::var("CHEVALIER_DEBUG_CODEX_STREAM")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CODEX_TOKEN: &str = "header.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjdF8xMjMifX0.signature";

    fn test_client() -> OpenAICodexResponsesClient {
        OpenAICodexResponsesClient::new(
            CodexSubscriptionProviderConfig {
                token: TEST_CODEX_TOKEN.to_string(),
                account_id: Some("acct_123".to_string()),
                base_url: None,
                transport: Some(CodexSubscriptionTransport::Sse),
                sse_header_timeout: None,
                websocket_connect_timeout: None,
                reasoning_effort: None,
                reasoning_summary: None,
                text_verbosity: None,
                service_tier: None,
            },
            "gpt-5.1-codex",
        )
        .unwrap()
    }

    fn codex_rate_limit_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            CODEX_PRIMARY_USED_PERCENT_HEADER,
            HeaderValue::from_static("75"),
        );
        headers.insert(
            CODEX_PRIMARY_WINDOW_MINUTES_HEADER,
            HeaderValue::from_static("300"),
        );
        headers.insert(CODEX_PRIMARY_RESET_AT_HEADER, HeaderValue::from_static("0"));
        headers.insert(
            CODEX_SECONDARY_USED_PERCENT_HEADER,
            HeaderValue::from_static("25"),
        );
        headers.insert(
            CODEX_SECONDARY_WINDOW_MINUTES_HEADER,
            HeaderValue::from_static("10080"),
        );
        headers.insert(
            CODEX_SECONDARY_RESET_AT_HEADER,
            HeaderValue::from_static("604800"),
        );
        headers
    }

    #[test]
    fn test_resolve_codex_url() {
        assert_eq!(
            resolve_codex_url(None),
            "https://chatgpt.com/backend-api/codex/responses"
        );
        assert_eq!(
            resolve_codex_url(Some("https://example.test/backend-api/codex")),
            "https://example.test/backend-api/codex/responses"
        );
        assert_eq!(
            resolve_codex_url(Some("https://example.test/backend-api/codex/responses")),
            "https://example.test/backend-api/codex/responses"
        );
    }

    #[test]
    fn test_wrap_websocket_request_body() {
        let body = serde_json::json!({"model": "gpt-5.1-codex", "stream": true});
        let wrapped = wrap_websocket_request_body(body).unwrap();
        assert_eq!(wrapped["type"], "response.create");
        assert_eq!(wrapped["model"], "gpt-5.1-codex");
    }

    #[test]
    fn test_extract_account_id_from_jwt() {
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::json!({
                JWT_CLAIM_PATH: {
                    "chatgpt_account_id": "acct_123"
                }
            })
            .to_string(),
        );
        let token = format!("header.{payload}.signature");
        assert_eq!(extract_account_id(&token).unwrap(), "acct_123");
    }

    #[test]
    fn test_build_codex_request_body() {
        let client = OpenAICodexResponsesClient::new(
            CodexSubscriptionProviderConfig {
                token: "header.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjdF8xMjMifX0.signature".to_string(),
                account_id: Some("acct_123".to_string()),
                base_url: None,
                transport: Some(CodexSubscriptionTransport::Sse),
                sse_header_timeout: None,
                websocket_connect_timeout: None,
                reasoning_effort: Some("high".to_string()),
                reasoning_summary: Some("concise".to_string()),
                text_verbosity: Some("medium".to_string()),
                service_tier: Some("priority".to_string()),
            },
            "gpt-5.1-codex",
        )
        .unwrap();
        let config = GenerationConfig::new("gpt-5.1-codex");
        let messages = vec![ConversationMessage::Chat(crate::types::ChatMessage::user(
            "Hello",
        ))];
        let body = client.build_request_body(&messages, &config, true).unwrap();

        assert_eq!(body["model"], "gpt-5.1-codex");
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        assert!(body["input"].is_array());
        assert_eq!(body["text"]["verbosity"], "medium");
        assert_eq!(body["reasoning"]["effort"], "high");
        assert_eq!(body["reasoning"]["summary"], "concise");
        assert_eq!(body["service_tier"], "priority");
    }

    #[test]
    fn test_build_codex_request_body_with_summary_only_reasoning() {
        let client = OpenAICodexResponsesClient::new(
            CodexSubscriptionProviderConfig {
                token: "header.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiYWNjdF8xMjMifX0.signature".to_string(),
                account_id: Some("acct_123".to_string()),
                base_url: None,
                transport: Some(CodexSubscriptionTransport::Sse),
                sse_header_timeout: None,
                websocket_connect_timeout: None,
                reasoning_effort: None,
                reasoning_summary: Some("detailed".to_string()),
                text_verbosity: None,
                service_tier: None,
            },
            "gpt-5.1-codex",
        )
        .unwrap();
        let config = GenerationConfig::new("gpt-5.1-codex");
        let messages = vec![ConversationMessage::Chat(crate::types::ChatMessage::user(
            "Hello",
        ))];
        let body = client.build_request_body(&messages, &config, true).unwrap();

        assert!(body["reasoning"].get("effort").is_none());
        assert_eq!(body["reasoning"]["summary"], "detailed");
        assert_eq!(body["text"]["verbosity"], "medium");
    }

    #[test]
    fn test_codex_rate_limit_headers_parse_sse_response() {
        let limits = codex_rate_limits_from_headers(&codex_rate_limit_headers());

        assert_eq!(
            limits,
            vec![
                ProviderRateLimit {
                    scope: ProviderRateLimitScope::Session,
                    used_percent: 75,
                    window_minutes: 300,
                    resets_at_epoch_sec: 0,
                },
                ProviderRateLimit {
                    scope: ProviderRateLimitScope::Subscription,
                    used_percent: 25,
                    window_minutes: 10080,
                    resets_at_epoch_sec: 604800,
                },
            ]
        );
    }

    #[test]
    fn test_codex_rate_limit_headers_parse_websocket_handshake() {
        let response = http::Response::builder()
            .status(101)
            .header(CODEX_PRIMARY_USED_PERCENT_HEADER, "99")
            .header(CODEX_PRIMARY_WINDOW_MINUTES_HEADER, "300")
            .header(CODEX_PRIMARY_RESET_AT_HEADER, "3600")
            .body(())
            .unwrap();

        let limits = codex_rate_limits_from_headers(response.headers());

        assert_eq!(
            limits,
            vec![ProviderRateLimit {
                scope: ProviderRateLimitScope::Session,
                used_percent: 99,
                window_minutes: 300,
                resets_at_epoch_sec: 3600,
            }]
        );
    }

    #[test]
    fn test_codex_usage_limit_family_is_non_retryable() {
        let headers = codex_rate_limit_headers();
        for (code, scope, reset) in [
            (
                "usage_limit_reached",
                ProviderRateLimitScope::Session,
                Some(0),
            ),
            (
                "workspace_primary_usage_limit_reached",
                ProviderRateLimitScope::Session,
                Some(0),
            ),
            (
                "workspace_secondary_usage_limit_reached",
                ProviderRateLimitScope::Subscription,
                Some(604800),
            ),
            (
                "secondary_credits_depleted",
                ProviderRateLimitScope::Subscription,
                Some(604800),
            ),
        ] {
            let body = serde_json::json!({ "error": { "code": code } }).to_string();
            let error =
                codex_friendly_error(StatusCode::TOO_MANY_REQUESTS, &headers, &body).unwrap();

            assert!(!error.is_retryable());
            assert!(matches!(
                &error,
                Error::CodexUsageLimit {
                    reached_type: Some(reached_type),
                    resets_at_epoch_sec,
                    ..
                } if reached_type == code && *resets_at_epoch_sec == reset
            ));
            assert_eq!(
                error.to_string(),
                format!(
                    "Codex usage limit ({}) — resets {}",
                    scope.as_str(),
                    codex_epoch_seconds_to_iso_utc(reset.unwrap())
                )
            );
        }
    }

    #[test]
    fn test_codex_usage_limit_reached_type_header_classifies_error() {
        let mut headers = codex_rate_limit_headers();
        headers.insert(
            CODEX_RATE_LIMIT_REACHED_TYPE_HEADER,
            HeaderValue::from_static("workspace_secondary_usage_limit_reached"),
        );
        let body = serde_json::json!({ "error": { "code": "rate_limit_reached" } }).to_string();

        let error = codex_friendly_error(StatusCode::TOO_MANY_REQUESTS, &headers, &body).unwrap();

        assert!(matches!(
            &error,
            Error::CodexUsageLimit {
                reached_type: Some(reached_type),
                resets_at_epoch_sec: Some(604800),
                ..
            } if reached_type == "workspace_secondary_usage_limit_reached"
        ));
        assert_eq!(
            error.to_string(),
            "Codex usage limit (subscription) — resets 1970-01-08T00:00:00Z"
        );
    }

    #[test]
    fn test_codex_bare_rate_limit_remains_retryable() {
        let headers = codex_rate_limit_headers();
        let body = serde_json::json!({ "error": { "code": "rate_limit_reached" } }).to_string();

        assert!(codex_friendly_error(StatusCode::TOO_MANY_REQUESTS, &headers, &body).is_none());
        let error =
            test_client().handle_error_response(StatusCode::TOO_MANY_REQUESTS, &headers, body);

        assert!(matches!(error, Error::Inference(_)));
        assert!(error.is_retryable());
    }

    #[test]
    fn test_codex_streamed_usage_limit_error_event_is_non_retryable() {
        let event = serde_json::json!({
            "type": "response.failed",
            "response": {
                "error": {
                    "code": "secondary_credits_depleted",
                    "reset_at": "604800"
                }
            }
        });

        let error = reject_codex_error_event(&event).unwrap_err();

        assert!(!error.is_retryable());
        assert!(matches!(
            &error,
            Error::CodexUsageLimit {
                reached_type: Some(reached_type),
                resets_at_epoch_sec: Some(604800),
                ..
            } if reached_type == "secondary_credits_depleted"
        ));
    }

    #[test]
    fn test_codex_rate_limits_stream_event_parses_session_and_weekly() {
        // The exact shape Codex streams on SSE/WS (from a real session rollout log).
        let event = serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {
                "primary":   { "used_percent": 8.0,  "window_minutes": 300,   "reset_at": 1783462580_i64 },
                "secondary": { "used_percent": 23.0, "window_minutes": 10080, "reset_at": 1783925974_i64 }
            }
        });
        let limits = codex_rate_limits_from_event(&event);
        assert_eq!(limits.len(), 2);
        let session = limits
            .iter()
            .find(|l| l.scope == ProviderRateLimitScope::Session)
            .unwrap();
        assert_eq!(session.used_percent, 8);
        assert_eq!(session.window_minutes, 300);
        assert_eq!(session.resets_at_epoch_sec, 1783462580);
        let weekly = limits
            .iter()
            .find(|l| l.scope == ProviderRateLimitScope::Subscription)
            .unwrap();
        assert_eq!(weekly.used_percent, 23);
        assert_eq!(weekly.window_minutes, 10080);
    }

    #[test]
    fn test_non_rate_limit_event_yields_no_limits() {
        let event = serde_json::json!({ "type": "response.output_text.delta", "delta": "hi" });
        assert!(codex_rate_limits_from_event(&event).is_empty());
    }
}
