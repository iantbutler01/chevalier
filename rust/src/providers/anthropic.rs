//! Anthropic Claude API client
//!
//! Implements the InferenceClient trait for Anthropic's Claude models.
//! Supports:
//! - Native tool calling with parallel execution
//! - Prompt caching with ephemeral markers
//! - Extended thinking mode
//! - Streaming with SSE

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use reqwest::StatusCode;
use std::collections::VecDeque;
use std::pin::Pin;

use crate::error::{Error, Result};
use crate::providers::{
    GenerationConfig, GenerationResponse, InferenceClient, ProviderConfig, StreamChunk,
    TraceCallback,
};
use crate::retry::{RetryConfig, retry_with_backoff};
use crate::schema::fix_tool_schema_for_provider;
use crate::types::{AssistantResponse, ChatRole, Provider, ResponsePart, TokenUsage, ToolCall};
use crate::utils::{
    ConversationMessage, convert_messages_to_provider_format, validate_image_input_supported,
};

#[derive(Clone, Copy)]
enum AnthropicAuthKind {
    ApiKey,
    Bearer,
}

/// Anthropic API client
#[derive(Clone)]
pub struct AnthropicClient {
    model: String,
    api_key: String,
    api_url: String,
    auth_kind: AnthropicAuthKind,
    send_beta_headers: bool,
    user_agent: Option<String>,
    thinking_budget: Option<u32>,
    /// Adaptive thinking (`thinking: {"type": "adaptive"}`) — the only
    /// supported thinking mode on Opus 4.7+ / Fable, where
    /// `enabled`+`budget_tokens` returns a 400. Mutually exclusive with
    /// `thinking_budget`; when both are set, `thinking_budget` wins (legacy).
    adaptive_thinking: bool,
    /// `output_config.effort` level (low|medium|high|xhigh|max) for models
    /// that support it (Opus 4.5+/Sonnet 4.6/Fable).
    effort: Option<String>,
    trace_callback: Option<TraceCallback>,
}

impl AnthropicClient {
    fn normalized_tools(&self, tools: &[serde_json::Value]) -> Vec<serde_json::Value> {
        tools
            .iter()
            .cloned()
            .map(|mut tool| {
                fix_tool_schema_for_provider(&mut tool, "anthropic");
                tool
            })
            .collect()
    }

    /// Create a new Anthropic client
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            api_key: api_key.into(),
            api_url: "https://api.anthropic.com/v1/messages".to_string(),
            auth_kind: AnthropicAuthKind::ApiKey,
            send_beta_headers: true,
            user_agent: None,
            thinking_budget: None,
            adaptive_thinking: false,
            effort: None,
            trace_callback: None,
        }
    }

    /// Set the thinking budget for extended thinking mode
    pub fn with_thinking_budget(mut self, budget: u32) -> Self {
        self.thinking_budget = Some(budget);
        self
    }

    /// Enable adaptive thinking (`thinking: {"type": "adaptive"}`).
    pub fn with_adaptive_thinking(mut self) -> Self {
        self.adaptive_thinking = true;
        self
    }

    /// Set the `output_config.effort` level (low|medium|high|xhigh|max).
    pub fn with_effort(mut self, effort: impl Into<String>) -> Self {
        self.effort = Some(effort.into());
        self
    }

    /// Set a custom API URL
    pub fn with_api_url(mut self, url: impl Into<String>) -> Self {
        self.api_url = url.into();
        self
    }

    /// Authenticate an Anthropic-compatible endpoint with a bearer token.
    pub fn with_bearer_auth(mut self) -> Self {
        self.auth_kind = AnthropicAuthKind::Bearer;
        self
    }

    /// Omit Anthropic beta headers for compatible providers that do not implement them.
    pub fn without_beta_headers(mut self) -> Self {
        self.send_beta_headers = false;
        self
    }

    /// Preserve the caller's honest client identity on compatible endpoints.
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Build the request body for Anthropic API
    fn build_request_body(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
        stream: bool,
    ) -> Result<serde_json::Value> {
        let model = config.effective_model(&self.model);
        validate_image_input_supported(messages, Provider::Anthropic, model)?;

        // Extract system message if present
        let (system, messages) = self.extract_system_message(messages)?;

        // Convert messages to provider format
        let formatted_messages =
            convert_messages_to_provider_format(messages, Provider::Anthropic)?;

        // Wrap string content in proper format
        let formatted_messages = self.wrap_string_content(formatted_messages);

        let mut formatted_messages = formatted_messages;
        // "Automatic" prompt caching: stamp an explicit cache_control marker on
        // the SECOND-TO-LAST message (falling back to the only message). Two
        // reasons, both empirical (verified via runtime_llm_usage_events):
        //
        // 1. The top-level `cache_control` request field never produced
        //    readable message-history entries — every agent-loop turn re-wrote
        //    its byte-identical history at the cache-write premium while
        //    cache_read stayed pinned to the system prefix.
        // 2. The final message is frequently ephemeral: `Runtime::run` appends
        //    `prompt`/`default_prompt` (e.g. a "[System continuation]" nudge)
        //    that is NOT part of durable history, so its bytes vanish from the
        //    next request. A marker on it keys a cache entry no later request
        //    can ever match. Stamping the message before it puts the breakpoint
        //    at the end of the SHARED prefix — the documented placement — and
        //    lets the volatile tail ride after the breakpoint uncached.
        if let Some(ProviderConfig::Anthropic(provider_config)) = config.provider_config.as_ref()
            && let Some(marker) = provider_config.automatic_prompt_caching.as_ref()
        {
            let target_index = formatted_messages.len().saturating_sub(2);
            if let Some(blocks) = formatted_messages
                .get_mut(target_index)
                .and_then(|msg| msg.get_mut("content"))
                .and_then(|content| content.as_array_mut())
                && let Some(last_block) = blocks.last_mut()
                && last_block.get("cache_control").is_none()
            {
                last_block["cache_control"] = marker.anthropic_cache_control();
            }
        }

        let mut request = serde_json::json!({
            "model": model,
            "max_tokens": config.max_tokens.unwrap_or(4096),
            "messages": formatted_messages,
            "stream": stream,
        });

        // Add temperature and top_p if not in thinking mode. (Sampling params
        // are incompatible with extended thinking, and removed entirely on the
        // models that require adaptive thinking.)
        if self.thinking_budget.is_none() && !self.adaptive_thinking {
            if let Some(temp) = config.temperature {
                request["temperature"] = serde_json::json!(temp);
            }
            if let Some(top_p) = config.top_p {
                request["top_p"] = serde_json::json!(top_p);
            }
        }

        // Add system if present
        if let Some(system) = system {
            request["system"] = system;
        }

        // Add tools if provided
        if let Some(ref tools) = config.tools
            && !tools.is_empty()
        {
            let mut normalized_tools = self.normalized_tools(tools);
            if let Some(ProviderConfig::Anthropic(provider_config)) =
                config.provider_config.as_ref()
                && let Some(marker) = provider_config.tool_definitions_cache_breakpoint.as_ref()
                && let Some(last_tool) = normalized_tools.last_mut()
                && last_tool.get("cache_control").is_none()
            {
                last_tool["cache_control"] = marker.anthropic_cache_control();
            }

            request["tools"] = serde_json::json!(normalized_tools);
            // Enable parallel tool calling via tool_choice
            request["tool_choice"] = serde_json::json!({
                "type": "auto",
                "disable_parallel_tool_use": false
            });
        }

        // Add thinking if configured. Legacy budgeted extended thinking takes
        // precedence (pre-4.7 models); adaptive thinking is the only on-mode
        // for Opus 4.7+/Fable and needs none of the legacy max_tokens or
        // temperature adjustments.
        if let Some(budget) = self.thinking_budget {
            request["thinking"] = serde_json::json!({
                "type": "enabled",
                "budget_tokens": budget
            });
            // Adjust max_tokens and temperature for thinking mode
            let current_max = request["max_tokens"].as_u64().unwrap_or(4096);
            request["max_tokens"] = serde_json::json!(current_max + budget as u64);
            request["temperature"] = serde_json::json!(1.0);
            if let Some(obj) = request.as_object_mut() {
                obj.remove("top_p");
            }
        } else if self.adaptive_thinking {
            request["thinking"] = serde_json::json!({ "type": "adaptive" });
        }

        // output_config carries structured outputs and effort. The top-level
        // `output_format` parameter is deprecated API-wide; `output_config`
        // with a nested `format` is the canonical replacement.
        let mut output_config = serde_json::Map::new();
        if let Some(effort) = self.effort.as_deref() {
            output_config.insert("effort".to_string(), serde_json::json!(effort));
        }
        if let Some(ref schema) = config.output_schema {
            output_config.insert(
                "format".to_string(),
                serde_json::json!({
                    "type": "json_schema",
                    "schema": schema
                }),
            );
        }
        if !output_config.is_empty() {
            request["output_config"] = serde_json::Value::Object(output_config);
        }

        Ok(request)
    }

    /// Extract system message and return (system, remaining_messages)
    fn extract_system_message<'a>(
        &self,
        messages: &'a [ConversationMessage],
    ) -> Result<(Option<serde_json::Value>, &'a [ConversationMessage])> {
        let mut system_blocks = Vec::new();
        let mut consumed = 0usize;

        for message in messages {
            let ConversationMessage::Chat(chat) = message else {
                break;
            };
            if chat.role != ChatRole::System {
                break;
            }

            let mut system_dict = serde_json::json!({
                "type": "text",
                "text": chat.content
            });

            if let Some(marker) = chat.cache_marker.as_ref() {
                system_dict["cache_control"] = marker.anthropic_cache_control();
            }

            system_blocks.push(system_dict);
            consumed += 1;
        }

        if !system_blocks.is_empty() {
            return Ok((
                Some(serde_json::json!(system_blocks)),
                &messages[consumed..],
            ));
        }

        Ok((None, messages))
    }

    /// Wrap string content in [{"type": "text", "text": "..."}] format
    /// Skips empty strings to avoid Anthropic's "text content blocks must be non-empty" error
    fn wrap_string_content(&self, mut messages: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
        for msg in messages.iter_mut() {
            if let Some(content) = msg.get("content")
                && let Some(text) = content.as_str()
            {
                if !text.is_empty() {
                    msg["content"] = serde_json::json!([{
                        "type": "text",
                        "text": text
                    }]);
                } else {
                    // Empty content - convert to empty array
                    // This can happen for assistant messages that only have tool calls
                    msg["content"] = serde_json::json!([]);
                }
            }
        }
        messages
    }

    /// Extract canonical assistant response from content blocks.
    fn extract_response(&self, content: &serde_json::Value) -> Result<AssistantResponse> {
        let mut response = AssistantResponse::default();
        if let Some(blocks) = content.as_array() {
            for block in blocks {
                match block["type"].as_str() {
                    Some("text") => {
                        if let Some(text) = block["text"].as_str()
                            && !text.is_empty()
                        {
                            response.push_output(ResponsePart::Text {
                                text: text.to_string(),
                            });
                        }
                    }
                    Some("thinking") => {
                        if let Some(text) = block["thinking"].as_str()
                            && !text.is_empty()
                        {
                            response.push_output(ResponsePart::Reasoning {
                                text: text.to_string(),
                            });
                        }
                        if let Some(signature) = block.get("signature").and_then(|v| v.as_str()) {
                            response.push_output(ResponsePart::Signature {
                                value: signature.to_string(),
                            });
                        }
                    }
                    Some("tool_use") => {
                        response.push_output(ResponsePart::Tool {
                            call: ToolCall::from_provider_format(
                                block.clone(),
                                Provider::Anthropic,
                            )?,
                        });
                        if let Some(signature) = block.get("signature").and_then(|v| v.as_str()) {
                            response.push_output(ResponsePart::Signature {
                                value: signature.to_string(),
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(response)
    }

    /// Parse token usage from response
    fn parse_usage(&self, usage: &serde_json::Value) -> TokenUsage {
        TokenUsage {
            input_tokens: usage["input_tokens"].as_u64().unwrap_or(0),
            output_tokens: usage["output_tokens"].as_u64().unwrap_or(0),
            cached_tokens: usage["cache_read_input_tokens"].as_u64().unwrap_or(0),
            cache_write_input_tokens: usage["cache_creation_input_tokens"].as_u64().unwrap_or(0),
            reasoning_tokens: None,
            provider_cost_dollars: None,
        }
    }

    /// Make HTTP request to Anthropic API
    async fn make_request(
        &self,
        body: serde_json::Value,
        use_structured_outputs: bool,
        timeout: Option<std::time::Duration>,
    ) -> Result<reqwest::Response> {
        let client = reqwest::Client::new();

        // Build beta header - add structured-outputs if needed
        let beta_header = if use_structured_outputs {
            "prompt-caching-2024-07-31,output-128k-2025-02-19,structured-outputs-2025-11-13"
        } else {
            "prompt-caching-2024-07-31,output-128k-2025-02-19"
        };

        let mut request = client
            .post(&self.api_url)
            .timeout(timeout.unwrap_or(std::time::Duration::from_secs(300)))
            .header("anthropic-version", "2023-06-01")
            .json(&body);
        request = match self.auth_kind {
            AnthropicAuthKind::ApiKey => request.header("x-api-key", &self.api_key),
            AnthropicAuthKind::Bearer => request.bearer_auth(&self.api_key),
        };
        if self.send_beta_headers {
            request = request.header("anthropic-beta", beta_header);
        }
        if let Some(user_agent) = self.user_agent.as_deref() {
            request = request.header(reqwest::header::USER_AGENT, user_agent);
        }
        let response = request.send().await?;

        Ok(response)
    }

    /// Handle error responses - categorize as retryable or non-retryable
    fn handle_error_response(&self, status: StatusCode, body: String) -> Error {
        match status {
            // Client errors (4xx) are generally not retryable
            StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Error::NonRetryable(format!("{}: {}", status, body))
            }
            // Rate limit - retryable
            StatusCode::TOO_MANY_REQUESTS => Error::Inference(format!("Rate limited: {}", body)),
            // Server errors (5xx) are retryable
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => Error::Inference(format!("{}: {}", status, body)),
            // Default: assume retryable for unknown errors
            _ => Error::Inference(format!("{}: {}", status, body)),
        }
    }

    /// Make request with retry and exponential backoff
    async fn make_request_with_retry(
        &self,
        body: serde_json::Value,
        use_structured_outputs: bool,
        timeout: Option<std::time::Duration>,
        retry_config: Option<RetryConfig>,
    ) -> Result<serde_json::Value> {
        let config = retry_config.unwrap_or_default();

        retry_with_backoff(config, || async {
            let response = self
                .make_request(body.clone(), use_structured_outputs, timeout)
                .await?;
            let status = response.status();

            if !status.is_success() {
                let error_body = response.text().await.unwrap_or_default();
                return Err(self.handle_error_response(status, error_body));
            }

            response.json().await.map_err(Error::from)
        })
        .await
    }
}

#[async_trait]
impl InferenceClient for AnthropicClient {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        let use_structured_outputs = config.output_schema.is_some();
        let request_body = self.build_request_body(messages, config, false)?;
        let body = self
            .make_request_with_retry(
                request_body,
                use_structured_outputs,
                config.timeout,
                config.retry_config.clone(),
            )
            .await?;

        // Parse usage statistics
        let usage = self.parse_usage(&body["usage"]);

        // Extract content
        let content_value = &body["content"];
        let response = self.extract_response(content_value)?;

        // If tools were provided, return full response for tool extraction
        let has_tools = config.tools.is_some() && !config.tools.as_ref().unwrap().is_empty();
        let has_tool_calls = response.has_tool_calls();

        Ok(GenerationResponse::from_assistant_response(
            response,
            usage,
            None,
            if has_tools || has_tool_calls {
                Some(body)
            } else {
                None
            },
        ))
    }

    async fn connect_and_listen(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
        use crate::providers::anthropic_streaming::{ToolCallAccumulator, parse_anthropic_chunk};
        use crate::utils::parse_sse_stream;

        let use_structured_outputs = config.output_schema.is_some();
        let request_body = self.build_request_body(messages, config, true)?;
        let timeout = config.timeout;

        // Retry the connection establishment with backoff
        let retry_config = config.retry_config.clone().unwrap_or_default();
        let response = retry_with_backoff(retry_config, || async {
            let resp = self
                .make_request(request_body.clone(), use_structured_outputs, timeout)
                .await?;
            let status = resp.status();

            if !status.is_success() {
                let error_body = resp.text().await.unwrap_or_default();
                return Err(self.handle_error_response(status, error_body));
            }

            Ok(resp)
        })
        .await?;

        let has_tools = config.tools.is_some() && !config.tools.as_ref().unwrap().is_empty();

        let sse_stream = parse_sse_stream(response);

        let chunk_stream = futures::stream::unfold(
            (
                sse_stream,
                ToolCallAccumulator::new(),
                VecDeque::<Result<StreamChunk>>::new(),
                false,
            ),
            move |(mut sse_stream, mut accumulator, mut pending, eof_flushed)| async move {
                loop {
                    if let Some(item) = pending.pop_front() {
                        return Some((item, (sse_stream, accumulator, pending, eof_flushed)));
                    }

                    if eof_flushed {
                        return None;
                    }

                    match sse_stream.next().await {
                        Some(Ok(sse_json)) => {
                            pending.extend(
                                parse_anthropic_chunk(&sse_json, &mut accumulator, has_tools)
                                    .into_iter()
                                    .map(Ok),
                            );
                        }
                        Some(Err(e)) => {
                            return Some((Err(e), (sse_stream, accumulator, pending, true)));
                        }
                        None => {
                            let flushed = accumulator.drain_all_tools();
                            if flushed.is_empty() {
                                return None;
                            }

                            tracing::warn!(
                                pending_tool_calls = flushed.len(),
                                "anthropic SSE stream ended with unfinished tool calls; flushing pending tools at EOF"
                            );
                            pending.extend(
                                flushed
                                    .into_iter()
                                    .map(StreamChunk::ToolCallComplete)
                                    .map(Ok),
                            );
                            return pending
                                .pop_front()
                                .map(|item| (item, (sse_stream, accumulator, pending, true)));
                        }
                    }
                }
            },
        );

        Ok(Box::pin(chunk_stream))
    }

    fn provider(&self) -> Provider {
        Provider::Anthropic
    }

    fn set_trace_callback(&mut self, callback: TraceCallback) {
        self.trace_callback = Some(callback);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CacheMarker, ChatMessage};

    #[test]
    fn test_client_creation() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        assert_eq!(client.model, "claude-3-opus-20240229");
        assert_eq!(client.api_key, "test-key");
        assert_eq!(client.api_url, "https://api.anthropic.com/v1/messages");
    }

    #[test]
    fn test_compatible_provider_transport_options() {
        let client = AnthropicClient::new("oauth-token", "k3")
            .with_api_url("https://api.kimi.com/coding/v1/messages")
            .with_bearer_auth()
            .without_beta_headers()
            .with_user_agent("OpenBracket");

        assert_eq!(client.api_url, "https://api.kimi.com/coding/v1/messages");
        assert!(matches!(client.auth_kind, AnthropicAuthKind::Bearer));
        assert!(!client.send_beta_headers);
        assert_eq!(client.user_agent.as_deref(), Some("OpenBracket"));
    }

    #[test]
    fn test_with_thinking_budget() {
        let client =
            AnthropicClient::new("test-key", "claude-3-opus-20240229").with_thinking_budget(1024);
        assert_eq!(client.thinking_budget, Some(1024));
    }

    #[test]
    fn test_extract_system_message() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::system("You are helpful")),
            ConversationMessage::Chat(ChatMessage::user("Hello")),
        ];

        let (system, remaining) = client.extract_system_message(&messages).unwrap();
        assert!(system.is_some());
        assert_eq!(remaining.len(), 1);

        let system_val = system.unwrap();
        assert_eq!(system_val[0]["type"], "text");
        assert_eq!(system_val[0]["text"], "You are helpful");
    }

    #[test]
    fn test_extract_system_with_cache_marker() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![ConversationMessage::Chat(
            ChatMessage::system("Long context").with_cache_marker(CacheMarker::Ephemeral),
        )];

        let (system, _) = client.extract_system_message(&messages).unwrap();
        let system_val = system.unwrap();
        assert!(system_val[0]["cache_control"].is_object());
        assert_eq!(system_val[0]["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn test_extract_system_with_one_hour_cache_marker() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![ConversationMessage::Chat(
            ChatMessage::system("Long-lived context").with_cache_marker(CacheMarker::Ephemeral1h),
        )];

        let (system, _) = client.extract_system_message(&messages).unwrap();
        let system_val = system.unwrap();
        assert_eq!(system_val[0]["cache_control"]["type"], "ephemeral");
        assert_eq!(system_val[0]["cache_control"]["ttl"], "1h");
    }

    #[test]
    fn test_extract_multiple_system_messages() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![
            ConversationMessage::Chat(
                ChatMessage::system("Stable runtime contract")
                    .with_cache_marker(CacheMarker::Ephemeral),
            ),
            ConversationMessage::Chat(
                ChatMessage::system("<soul>Durable identity</soul>")
                    .with_cache_marker(CacheMarker::Ephemeral),
            ),
            ConversationMessage::Chat(ChatMessage::user("Hello")),
        ];

        let (system, remaining) = client.extract_system_message(&messages).unwrap();
        let system_val = system.unwrap();
        assert_eq!(system_val.as_array().unwrap().len(), 2);
        assert_eq!(system_val[0]["text"], "Stable runtime contract");
        assert_eq!(system_val[1]["text"], "<soul>Durable identity</soul>");
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_wrap_string_content() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![serde_json::json!({
            "role": "user",
            "content": "Hello"
        })];

        let wrapped = client.wrap_string_content(messages);
        assert!(wrapped[0]["content"].is_array());
        assert_eq!(wrapped[0]["content"][0]["type"], "text");
        assert_eq!(wrapped[0]["content"][0]["text"], "Hello");
    }

    #[test]
    fn test_extract_text_content() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let content = serde_json::json!([
            {"type": "text", "text": "Hello, world!"}
        ]);

        let response = client.extract_response(&content).unwrap();
        assert_eq!(response.text(), "Hello, world!");
    }

    #[test]
    fn test_extract_tool_calls() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let content = serde_json::json!([
            {"type": "tool_use", "id": "toolu_123", "name": "get_weather", "input": {"city": "SF"}},
            {"type": "text", "text": "Using tool..."}
        ]);

        let response = client.extract_response(&content).unwrap();
        assert_eq!(response.tool_calls().len(), 1);
        assert_eq!(response.tool_calls()[0].tool_use_id, "toolu_123");
        assert_eq!(response.tool_calls()[0].tool_name, "get_weather");
    }

    #[test]
    fn test_parse_usage() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let usage = serde_json::json!({
            "input_tokens": 100,
            "output_tokens": 50,
            "cache_read_input_tokens": 25,
            "cache_creation_input_tokens": 10
        });

        let parsed = client.parse_usage(&usage);
        assert_eq!(parsed.input_tokens, 100);
        assert_eq!(parsed.output_tokens, 50);
        assert_eq!(parsed.cached_tokens, 25);
    }

    #[test]
    fn test_build_request_body_basic() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("claude-3-opus-20240229")
            .with_max_tokens(1024)
            .with_temperature(0.7);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["model"], "claude-3-opus-20240229");
        assert_eq!(body["max_tokens"], 1024);
        assert!((body["temperature"].as_f64().unwrap() - 0.7).abs() < 0.01);
        assert_eq!(body["stream"], false);
        assert!(body["messages"].is_array());
    }

    #[test]
    fn test_build_request_with_tools() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let tools = vec![serde_json::json!({"name": "get_weather"})];
        let config = GenerationConfig::new("claude-3-opus-20240229").with_tools(tools);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert!(body["tools"].is_array());
        assert!(body["tools"][0].get("strict").is_none());
        assert_eq!(body["tool_choice"]["type"], "auto");
        assert_eq!(body["tool_choice"]["disable_parallel_tool_use"], false);
    }

    #[test]
    fn test_build_request_with_automatic_prompt_caching() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("claude-3-opus-20240229").with_provider_config(
            ProviderConfig::Anthropic(crate::providers::AnthropicProviderConfig {
                automatic_prompt_caching: Some(CacheMarker::Ephemeral),
                tool_definitions_cache_breakpoint: None,
            }),
        );

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        // Automatic caching stamps an explicit marker on the second-to-last
        // message (sole message here), not the top-level request field. The
        // last message is often an ephemeral appended prompt whose bytes never
        // recur, so a marker there can never be read back.
        assert!(body.get("cache_control").is_none());
        let blocks = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(blocks.last().unwrap()["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn test_build_request_adaptive_thinking() {
        let client = AnthropicClient::new("test-key", "claude-opus-4-8").with_adaptive_thinking();
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("claude-opus-4-8")
            .with_temperature(0.7)
            .with_max_tokens(4096);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["thinking"]["type"], "adaptive");
        assert!(body["thinking"].get("budget_tokens").is_none());
        // Sampling params are removed on adaptive-thinking models; max_tokens
        // must not get the legacy budget adjustment.
        assert!(body.get("temperature").is_none());
        assert_eq!(body["max_tokens"], 4096);
    }

    #[test]
    fn test_build_request_effort_lands_in_output_config() {
        let client = AnthropicClient::new("test-key", "claude-opus-4-8")
            .with_adaptive_thinking()
            .with_effort("xhigh");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("claude-opus-4-8");

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["output_config"]["effort"], "xhigh");
    }

    #[test]
    fn test_build_request_output_schema_uses_output_config_format() {
        let client = AnthropicClient::new("test-key", "claude-opus-4-8");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let schema = serde_json::json!({"type": "object", "properties": {}});
        let config = GenerationConfig::new("claude-opus-4-8")
            .with_output_schema(schema.clone(), "TestOutput");

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        // Deprecated top-level param must be gone; canonical nested form used.
        assert!(body.get("output_format").is_none());
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        assert_eq!(body["output_config"]["format"]["schema"], schema);
    }

    #[test]
    fn test_automatic_prompt_caching_skips_trailing_ephemeral_prompt() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::user("Durable history tail")),
            ConversationMessage::Chat(ChatMessage::user("[System continuation] nudge")),
        ];
        let config = GenerationConfig::new("claude-3-opus-20240229").with_provider_config(
            ProviderConfig::Anthropic(crate::providers::AnthropicProviderConfig {
                automatic_prompt_caching: Some(CacheMarker::Ephemeral),
                tool_definitions_cache_breakpoint: None,
            }),
        );

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        let durable_blocks = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(
            durable_blocks.last().unwrap()["cache_control"]["type"],
            "ephemeral"
        );
        let trailing_blocks = body["messages"][1]["content"].as_array().unwrap();
        assert!(
            trailing_blocks
                .last()
                .unwrap()
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn test_build_request_with_tool_definitions_cache_breakpoint() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let tools = vec![
            serde_json::json!({"name": "get_weather"}),
            serde_json::json!({"name": "get_time"}),
        ];
        let config = GenerationConfig::new("claude-3-opus-20240229")
            .with_tools(tools)
            .with_provider_config(ProviderConfig::Anthropic(
                crate::providers::AnthropicProviderConfig {
                    automatic_prompt_caching: None,
                    tool_definitions_cache_breakpoint: Some(CacheMarker::Ephemeral),
                },
            ));

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert!(body["tools"][0].get("cache_control").is_none());
        assert_eq!(body["tools"][1]["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn test_build_request_with_tools_preserves_explicit_strict() {
        let client = AnthropicClient::new("test-key", "claude-3-opus-20240229");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let tools = vec![serde_json::json!({"name": "get_weather", "strict": false})];
        let config = GenerationConfig::new("claude-3-opus-20240229").with_tools(tools);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["tools"][0]["strict"], false);
    }

    #[test]
    fn test_build_request_with_thinking() {
        let client =
            AnthropicClient::new("test-key", "claude-3-opus-20240229").with_thinking_budget(1024);
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Think"))];
        let config = GenerationConfig::new("claude-3-opus-20240229").with_max_tokens(2048);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["thinking"]["type"], "enabled");
        assert_eq!(body["thinking"]["budget_tokens"], 1024);
        assert_eq!(body["max_tokens"], 3072); // 2048 + 1024
        assert_eq!(body["temperature"], 1.0);
        assert!(body.get("top_p").is_none());
    }
}
