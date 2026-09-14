//! OpenAI API client
//!
//! Implements the InferenceClient trait for OpenAI's GPT models and compatible APIs.
//! Supports:
//! - Native tool calling with parallel execution
//! - Reasoning mode (o-series models)
//! - Streaming with delta-based tool call accumulation
//! - Usage tracking with cache metrics

use super::openrouter::{PerformanceThreshold, ProviderSort};
use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use reqwest::StatusCode;
use std::pin::Pin;

use crate::error::{Error, Result};
use crate::providers::{
    GenerationConfig, GenerationResponse, InferenceClient, StreamChunk, TraceCallback,
};
use crate::retry::{RetryConfig, retry_with_backoff};
use crate::schema::fix_tool_schema_for_provider;
use crate::types::{AssistantResponse, Provider, ResponsePart, TokenUsage, ToolCall};
use crate::utils::{
    ConversationMessage, convert_messages_to_provider_format, parse_json_value_strict_str,
    validate_image_input_supported,
};

/// OpenAI API client (also serves as base for OpenRouter)
pub struct OAIClient {
    model: String,
    api_key: String,
    api_url: String,
    reasoning: Option<String>,
    ranking_referer: Option<String>,
    ranking_title: Option<String>,
    /// `@vision=` override for image-input support, or `None` to ask the
    /// provider's capability table.
    image_input: Option<bool>,
    trace_callback: Option<TraceCallback>,
    provider: Provider,
    openrouter_providers: Option<Vec<String>>,
    openrouter_provider_sort: Option<ProviderSort>,
    openrouter_min_throughput: Option<PerformanceThreshold>,
    openrouter_max_latency: Option<PerformanceThreshold>,
}

impl Clone for OAIClient {
    fn clone(&self) -> Self {
        Self {
            model: self.model.clone(),
            api_key: self.api_key.clone(),
            api_url: self.api_url.clone(),
            reasoning: self.reasoning.clone(),
            ranking_referer: self.ranking_referer.clone(),
            ranking_title: self.ranking_title.clone(),
            image_input: self.image_input,
            trace_callback: self.trace_callback.clone(),
            provider: self.provider,
            openrouter_providers: self.openrouter_providers.clone(),
            openrouter_provider_sort: self.openrouter_provider_sort,
            openrouter_min_throughput: self.openrouter_min_throughput.clone(),
            openrouter_max_latency: self.openrouter_max_latency.clone(),
        }
    }
}

impl std::fmt::Debug for OAIClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAIClient")
            .field("model", &self.model)
            .field("api_url", &self.api_url)
            .field("reasoning", &self.reasoning)
            .field("ranking_referer", &self.ranking_referer)
            .field("ranking_title", &self.ranking_title)
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

impl OAIClient {
    fn normalized_tools(&self, tools: &[serde_json::Value]) -> Vec<serde_json::Value> {
        let provider = match self.provider {
            Provider::OpenRouter => "openrouter",
            _ => "openai",
        };
        tools
            .iter()
            .cloned()
            .map(|mut tool| {
                fix_tool_schema_for_provider(&mut tool, provider);
                tool
            })
            .collect()
    }

    /// Create a new OpenAI client
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            api_key: api_key.into(),
            api_url: "https://api.openai.com/v1/chat/completions".to_string(),
            reasoning: None,
            ranking_referer: None,
            ranking_title: None,
            image_input: None,
            trace_callback: None,
            provider: Provider::OpenAI,
            openrouter_providers: None,
            openrouter_provider_sort: None,
            openrouter_min_throughput: None,
            openrouter_max_latency: None,
        }
    }

    fn extract_response(&self, message: &serde_json::Value) -> Result<AssistantResponse> {
        let mut response = AssistantResponse::default();

        if let Some(content) = message.get("content").and_then(|v| v.as_str())
            && !content.is_empty()
        {
            response.push_output(ResponsePart::Text {
                text: content.to_string(),
            });
        }

        if let Some(reasoning) = message.get("reasoning").and_then(|r| r.as_str())
            && !reasoning.is_empty()
        {
            response.push_output(ResponsePart::Reasoning {
                text: reasoning.to_string(),
            });
        }

        if let Some(tool_calls) = message.get("tool_calls").and_then(|tc| tc.as_array()) {
            for tool_call in tool_calls {
                response.push_output(ResponsePart::Tool {
                    call: ToolCall::from_provider_format(tool_call.clone(), self.provider)?,
                });
            }
        }

        Ok(response)
    }

    /// Set reasoning mode (for o-series models)
    /// - Numeric string (e.g., "1024") → max_tokens
    /// - Text string (e.g., "medium", "high") → effort level
    pub fn with_reasoning(mut self, reasoning: impl Into<String>) -> Self {
        self.reasoning = Some(reasoning.into());
        self
    }

    /// Set custom API URL (used by OpenRouter)
    pub fn with_api_url(mut self, url: impl Into<String>) -> Self {
        self.api_url = url.into();
        self
    }

    /// Set ranking headers (for OpenRouter)
    pub fn with_ranking_headers(mut self, referer: Option<String>, title: Option<String>) -> Self {
        self.ranking_referer = referer;
        self.ranking_title = title;
        self
    }

    /// Set provider type (used by OpenRouter subclass)
    pub(crate) fn with_provider(mut self, provider: Provider) -> Self {
        self.provider = provider;
        self
    }

    pub(crate) fn with_openrouter_provider_sort(mut self, sort: ProviderSort) -> Self {
        self.openrouter_provider_sort = Some(sort);
        self
    }

    pub(crate) fn with_openrouter_performance_preferences(
        mut self,
        min_throughput: Option<PerformanceThreshold>,
        max_latency: Option<PerformanceThreshold>,
    ) -> Self {
        self.openrouter_min_throughput = min_throughput;
        self.openrouter_max_latency = max_latency;
        self
    }

    pub(crate) fn with_openrouter_providers(mut self, providers: Vec<String>) -> Self {
        self.openrouter_providers = Some(providers);
        self
    }

    /// Build request body for OpenAI API
    fn build_request_body(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
        stream: bool,
    ) -> Result<serde_json::Value> {
        let model = config.effective_model(&self.model);
        validate_image_input_supported(messages, self.provider, model, self.image_input)?;

        // Convert messages to provider format
        let formatted_messages = convert_messages_to_provider_format(messages, self.provider)?;

        let mut request = serde_json::json!({
            "model": model,
            "messages": formatted_messages,
            "temperature": config.temperature.unwrap_or(0.7),
            "top_p": config.top_p.unwrap_or(1.0),
            "stream": stream,
        });

        // OpenRouter's parameter-aware routing recognizes max_tokens.
        let token_limit_field = if matches!(self.provider, Provider::OpenRouter) {
            "max_tokens"
        } else {
            "max_completion_tokens"
        };
        request[token_limit_field] = serde_json::json!(config.max_tokens.unwrap_or(4096));

        if matches!(self.provider, Provider::OpenAI)
            && let Some(retention) = config.prompt_cache_retention
        {
            request["prompt_cache_retention"] = serde_json::json!(retention.as_str());
        }

        if matches!(self.provider, Provider::OpenRouter)
            && let Some(ref providers) = self.openrouter_providers
        {
            request["provider"] = serde_json::json!({
                "order": providers,
                "only": providers,
                "allow_fallbacks": providers.len() > 1,
                "require_parameters": true,
            });
        }

        if matches!(self.provider, Provider::OpenRouter)
            && let Some(sort) = self.openrouter_provider_sort
        {
            request["provider"]["sort"] = serde_json::json!(sort);
        }

        if matches!(self.provider, Provider::OpenRouter) {
            if let Some(ref minimum) = self.openrouter_min_throughput {
                request["provider"]["preferred_min_throughput"] = serde_json::json!(minimum);
            }
            if let Some(ref maximum) = self.openrouter_max_latency {
                request["provider"]["preferred_max_latency"] = serde_json::json!(maximum);
            }
        }

        // Add stream_options for usage tracking when streaming
        if stream {
            request["stream_options"] = serde_json::json!({"include_usage": true});
        }

        // Add tools if provided
        if let Some(ref tools) = config.tools
            && !tools.is_empty()
        {
            request["tools"] = serde_json::json!(self.normalized_tools(tools));
            request["tool_choice"] = serde_json::json!("auto");
        }

        // Add reasoning if configured (client-level, then config-level fallback)
        let reasoning_value = self
            .reasoning
            .clone()
            .or_else(|| config.reasoning_effort.clone())
            .or_else(|| config.thinking_budget.map(|b| b.to_string()));

        if let Some(ref reasoning) = reasoning_value {
            if reasoning.chars().all(|c| c.is_ascii_digit()) {
                // Numeric: max_tokens. No top-level equivalent exists on the
                // OpenAI chat-completions schema, so this stays nested for
                // every provider.
                request["reasoning"] = serde_json::json!({
                    "max_tokens": reasoning.parse::<u32>().unwrap_or(1024)
                });
            } else if matches!(self.provider, Provider::OpenRouter) {
                // OpenRouter: nested effort level
                request["reasoning"] = serde_json::json!({
                    "effort": reasoning
                });
            } else {
                // OpenAI (and OpenAI-compatible servers such as vLLM) require
                // the top-level `reasoning_effort` string; nested `reasoning`
                // is ignored there.
                request["reasoning_effort"] = serde_json::json!(reasoning);
            }
        }

        // Add structured output schema if provided
        if let Some(ref schema) = config.output_schema {
            let type_name = config.output_type_name.as_deref().unwrap_or("response");
            request["response_format"] = serde_json::json!({
                "type": "json_schema",
                "json_schema": {
                    "name": type_name,
                    "schema": schema,
                    "strict": true
                }
            });
        }

        Ok(request)
    }

    /// Parse token usage from response
    fn parse_usage(&self, usage: &serde_json::Value) -> TokenUsage {
        TokenUsage {
            input_tokens: usage["prompt_tokens"].as_u64().unwrap_or(0),
            output_tokens: usage["completion_tokens"].as_u64().unwrap_or(0),
            cached_tokens: usage
                .get("prompt_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            cache_write_input_tokens: 0,
            reasoning_tokens: usage
                .get("completion_tokens_details")
                .and_then(|d| d.get("reasoning_tokens"))
                .and_then(|v| v.as_u64()),
            provider_cost_dollars: usage.get("cost").and_then(|v| v.as_f64()),
        }
    }

    /// Make HTTP request to OpenAI API
    async fn make_request(
        &self,
        body: serde_json::Value,
        timeout: Option<std::time::Duration>,
    ) -> Result<reqwest::Response> {
        let client = reqwest::Client::new();
        let mut req = client
            .post(&self.api_url)
            .timeout(timeout.unwrap_or(std::time::Duration::from_secs(180)))
            .header("Content-Type", "application/json")
            .json(&body);

        // Add Authorization header if API key provided
        if !self.api_key.is_empty() {
            req = req.header("Authorization", format!("Bearer {}", self.api_key));
        }

        // Add ranking headers (for OpenRouter)
        if let Some(ref referer) = self.ranking_referer {
            req = req.header("HTTP-Referer", referer);
        }
        if let Some(ref title) = self.ranking_title {
            req = req.header("X-Title", title);
        }

        let response = req.send().await?;
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
        timeout: Option<std::time::Duration>,
        retry_config: Option<RetryConfig>,
    ) -> Result<String> {
        let config = retry_config.unwrap_or_default();

        retry_with_backoff(config, || async {
            let response = self.make_request(body.clone(), timeout).await?;
            let status = response.status();
            let response_text = response.text().await.unwrap_or_default();

            if !status.is_success() {
                return Err(self.handle_error_response(status, response_text));
            }

            Ok(response_text)
        })
        .await
    }
}

impl OAIClient {
    /// Override whether this model accepts image input, from the model
    /// string's `@vision=` parameter.
    pub fn with_image_input(mut self, image_input: Option<bool>) -> Self {
        self.image_input = image_input;
        self
    }
}

#[async_trait]
impl InferenceClient for OAIClient {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        let request_body = self.build_request_body(messages, config, false)?;
        let response_text = self
            .make_request_with_retry(request_body, config.timeout, config.retry_config.clone())
            .await?;

        // Parse JSON - provide better error context if it fails
        let body: serde_json::Value = parse_json_value_strict_str(&response_text).map_err(|e| {
            Error::Inference(format!(
                "Failed to parse response as JSON: {}. Response: {}",
                e,
                if response_text.len() > 500 {
                    &response_text[..500]
                } else {
                    &response_text
                }
            ))
        })?;

        // Check for error in response body
        if let Some(error) = body.get("error") {
            let error_msg = error["message"]
                .as_str()
                .unwrap_or("Unknown error")
                .to_string();
            return Err(Error::Inference(format!("{} ({:?})", error_msg, error)));
        }

        // Parse usage statistics
        let usage_json = body.get("usage");
        let usage = usage_json.map(|u| self.parse_usage(u)).unwrap_or_default();

        // Extract provider cost if available (OpenRouter returns usage.cost in dollars)
        let provider_cost_dollars = if matches!(
            self.provider,
            Provider::OpenRouter | Provider::OpenRouterResponses
        ) {
            usage_json
                .and_then(|u| u.get("cost"))
                .and_then(|c| c.as_f64())
        } else {
            None
        };

        // Extract message and content
        let choice = &body["choices"][0];
        let message = &choice["message"];
        let response = self.extract_response(message)?;

        // If tools were provided, return full response for tool extraction
        let has_tools = config.tools.is_some() && !config.tools.as_ref().unwrap().is_empty();
        let has_tool_calls = response.has_tool_calls();

        Ok(GenerationResponse::from_assistant_response(
            response,
            usage,
            provider_cost_dollars,
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
        use crate::providers::openai_streaming::{OpenAIToolAccumulator, parse_openai_chunk};
        use crate::utils::parse_sse_stream;

        let request_body = self.build_request_body(messages, config, true)?;
        let timeout = config.timeout;

        // Retry the connection establishment with backoff
        let retry_config = config.retry_config.clone().unwrap_or_default();
        let response = retry_with_backoff(retry_config, || async {
            let resp = self.make_request(request_body.clone(), timeout).await?;
            let status = resp.status();

            if !status.is_success() {
                let error_body = resp.text().await.unwrap_or_default();
                return Err(self.handle_error_response(status, error_body));
            }

            Ok(resp)
        })
        .await?;

        let has_tools = config.tools.is_some() && !config.tools.as_ref().unwrap().is_empty();

        // Parse SSE stream
        let sse_stream = parse_sse_stream(response);

        // Process chunks with tool accumulator
        let chunk_stream = sse_stream.scan(
            OpenAIToolAccumulator::new(),
            move |accumulator, sse_result| {
                let sse_json = match sse_result {
                    Ok(json) => json,
                    Err(e) => return futures::future::ready(Some(vec![Err(e)])),
                };

                // Parse chunk and emit StreamChunks
                let chunks = parse_openai_chunk(&sse_json, accumulator, has_tools);
                futures::future::ready(Some(chunks.into_iter().map(Ok).collect()))
            },
        );

        // Flatten the Vec<Result<StreamChunk>> into individual items
        Ok(Box::pin(chunk_stream.flat_map(futures::stream::iter)))
    }

    fn provider(&self) -> Provider {
        self.provider
    }

    fn set_trace_callback(&mut self, callback: TraceCallback) {
        self.trace_callback = Some(callback);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ChatMessage;

    #[test]
    fn openrouter_pin_survives_clone_and_both_request_modes() {
        let client = OAIClient::new("test-key", "deepseek/deepseek-v4.1-flash")
            .with_provider(Provider::OpenRouter)
            .with_openrouter_providers(vec!["fireworks".into()])
            .clone();
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("deepseek/deepseek-v4.1-flash");
        for stream in [false, true] {
            let body = client
                .build_request_body(&messages, &config, stream)
                .unwrap();
            assert_eq!(
                body["provider"],
                serde_json::json!({
                    "order": ["fireworks"], "only": ["fireworks"], "allow_fallbacks": false, "require_parameters": true,
                })
            );
            assert_eq!(body["max_tokens"], 4096);
            assert!(body.get("max_completion_tokens").is_none());
            let unpinned = OAIClient::new("test-key", "test")
                .with_provider(Provider::OpenRouter)
                .build_request_body(&messages, &config, stream)
                .unwrap();
            assert!(unpinned.get("provider").is_none());
        }
    }

    #[test]
    fn openrouter_provider_list_bounds_fallbacks_and_preserves_order() {
        let client = OAIClient::new("test-key", "test")
            .with_provider(Provider::OpenRouter)
            .with_openrouter_providers(vec![
                "fireworks".into(),
                "deepseek".into(),
                "baseten".into(),
            ])
            .clone();
        for stream in [false, true] {
            let body = client
                .build_request_body(&[], &GenerationConfig::new("test"), stream)
                .unwrap();
            assert_eq!(
                body["provider"],
                serde_json::json!({
                    "order": ["fireworks", "deepseek", "baseten"],
                    "only": ["fireworks", "deepseek", "baseten"],
                    "allow_fallbacks": true,
                    "require_parameters": true,
                })
            );
        }
    }

    #[test]
    fn throughput_routing_keeps_all_providers_eligible_in_both_modes() {
        let client = OAIClient::new("test-key", "test")
            .with_provider(Provider::OpenRouter)
            .with_openrouter_provider_sort(ProviderSort::Throughput)
            .clone();
        for stream in [false, true] {
            let body = client
                .build_request_body(&[], &GenerationConfig::new("test"), stream)
                .unwrap();
            assert_eq!(body["provider"], serde_json::json!({"sort": "throughput"}));
        }
    }

    #[test]
    fn performance_preferences_reach_streaming_and_nonstreaming_requests() {
        let preferences = || {
            (
                serde_json::from_value(serde_json::json!({"p90":40})).unwrap(),
                serde_json::from_value(serde_json::json!({"p90":2.5})).unwrap(),
            )
        };
        for pinned in [false, true] {
            let (minimum, maximum) = preferences();
            let mut client = OAIClient::new("test-key", "test")
                .with_provider(Provider::OpenRouter)
                .with_openrouter_provider_sort(ProviderSort::Throughput)
                .with_openrouter_performance_preferences(Some(minimum), Some(maximum))
                .clone();
            if pinned {
                client =
                    client.with_openrouter_providers(vec!["fireworks".into(), "baseten".into()]);
            }
            for stream in [false, true] {
                let body = client
                    .build_request_body(&[], &GenerationConfig::new("test"), stream)
                    .unwrap();
                assert_eq!(body["provider"]["sort"], "throughput");
                assert_eq!(
                    body["provider"]["preferred_min_throughput"],
                    serde_json::json!({"p90":40})
                );
                assert_eq!(
                    body["provider"]["preferred_max_latency"],
                    serde_json::json!({"p90":2.5})
                );
                if pinned {
                    assert_eq!(
                        body["provider"]["only"],
                        serde_json::json!(["fireworks", "baseten"])
                    );
                    assert_eq!(body["provider"]["allow_fallbacks"], true);
                } else {
                    assert!(body["provider"].get("only").is_none());
                }
            }
        }
        let (minimum, maximum) = preferences();
        let body = OAIClient::new("test-key", "test")
            .with_openrouter_performance_preferences(Some(minimum), Some(maximum))
            .build_request_body(&[], &GenerationConfig::new("test"), false)
            .unwrap();
        assert!(body.get("provider").is_none());
    }

    const TINY_PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=";

    fn image_message() -> Vec<ConversationMessage> {
        use crate::types::{MediaPart, MediaSource, MultimodalMessage};
        vec![ConversationMessage::Multimodal(MultimodalMessage::user(
            vec![
                MediaPart::text("What color is this image?"),
                MediaPart::image(MediaSource::base64(TINY_PNG, "image/png")),
            ],
        ))]
    }

    /// The model string's `@vision=` reaches dispatch even though the wire
    /// model id no longer carries it.
    #[test]
    fn declared_image_input_reaches_request_building() {
        let config = GenerationConfig::default();

        let refused = OAIClient::new("test-key", "vendor/undocumented-vision-model")
            .with_provider(Provider::OpenRouter)
            .build_request_body(&image_message(), &config, false);
        assert!(
            refused.is_err(),
            "an unknown model should be refused without an override"
        );

        OAIClient::new("test-key", "vendor/undocumented-vision-model")
            .with_provider(Provider::OpenRouter)
            .with_image_input(Some(true))
            .build_request_body(&image_message(), &config, false)
            .expect("@vision=true must reach the dispatch check");

        let refused = OAIClient::new("test-key", "gpt-4o")
            .with_image_input(Some(false))
            .build_request_body(&image_message(), &config, false);
        assert!(
            refused.is_err(),
            "@vision=false must refuse a model the table would have allowed"
        );
    }

    #[test]
    fn test_client_creation() {
        let client = OAIClient::new("test-key", "gpt-4");
        assert_eq!(client.model, "gpt-4");
        assert_eq!(client.api_key, "test-key");
        assert_eq!(client.api_url, "https://api.openai.com/v1/chat/completions");
        assert_eq!(client.provider, Provider::OpenAI);
    }

    #[test]
    fn test_with_reasoning_numeric() {
        let client = OAIClient::new("test-key", "o3").with_reasoning("1024");
        assert_eq!(client.reasoning, Some("1024".to_string()));
    }

    #[test]
    fn test_with_reasoning_effort() {
        let client = OAIClient::new("test-key", "o3").with_reasoning("high");
        assert_eq!(client.reasoning, Some("high".to_string()));
    }

    #[test]
    fn test_with_ranking_headers() {
        let client = OAIClient::new("test-key", "gpt-4").with_ranking_headers(
            Some("https://example.com".to_string()),
            Some("My App".to_string()),
        );
        assert_eq!(
            client.ranking_referer,
            Some("https://example.com".to_string())
        );
        assert_eq!(client.ranking_title, Some("My App".to_string()));
    }

    #[test]
    fn test_build_request_basic() {
        let client = OAIClient::new("test-key", "gpt-4");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("gpt-4")
            .with_max_tokens(2048)
            .with_temperature(0.8);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["model"], "gpt-4");
        assert_eq!(body["max_completion_tokens"], 2048);
        assert!((body["temperature"].as_f64().unwrap() - 0.8).abs() < 0.01);
        assert_eq!(body["stream"], false);
        assert!(body["messages"].is_array());
    }

    #[test]
    fn test_build_request_with_streaming() {
        let client = OAIClient::new("test-key", "gpt-4");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("gpt-4");

        let body = client.build_request_body(&messages, &config, true).unwrap();

        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[test]
    fn test_build_request_with_prompt_cache_retention() {
        use crate::providers::PromptCacheRetention;

        let client = OAIClient::new("test-key", "gpt-4");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config =
            GenerationConfig::new("gpt-4").with_prompt_cache_retention(PromptCacheRetention::H24);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["prompt_cache_retention"], "24h");
    }

    #[test]
    fn test_build_request_ignores_prompt_cache_retention_for_openrouter() {
        use crate::providers::PromptCacheRetention;

        let client = OAIClient::new("test-key", "gpt-4").with_provider(Provider::OpenRouter);
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config =
            GenerationConfig::new("gpt-4").with_prompt_cache_retention(PromptCacheRetention::H24);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert!(body.get("prompt_cache_retention").is_none());
    }

    #[test]
    fn test_build_request_with_tools() {
        let client = OAIClient::new("test-key", "gpt-4");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let tools =
            vec![serde_json::json!({"type": "function", "function": {"name": "get_weather"}})];
        let config = GenerationConfig::new("gpt-4").with_tools(tools);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert!(body["tools"].is_array());
        assert_eq!(body["tool_choice"], "auto");
    }

    #[test]
    fn test_build_request_with_reasoning_numeric() {
        let client = OAIClient::new("test-key", "o3").with_reasoning("1024");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Think"))];
        let config = GenerationConfig::new("o3");

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["reasoning"]["max_tokens"], 1024);
    }

    #[test]
    fn test_build_request_with_reasoning_effort() {
        let client = OAIClient::new("test-key", "o3").with_reasoning("high");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Think"))];
        let config = GenerationConfig::new("o3");

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["reasoning_effort"], "high");
        assert!(body.get("reasoning").is_none());
    }

    #[test]
    fn test_build_request_with_reasoning_effort_from_config() {
        let client = OAIClient::new("test-key", "o3");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Think"))];
        let config = GenerationConfig::new("o3").with_reasoning_effort("medium");

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["reasoning_effort"], "medium");
        assert!(body.get("reasoning").is_none());
    }

    #[test]
    fn test_build_request_with_reasoning_effort_openrouter_stays_nested() {
        let client = OAIClient::new("test-key", "o3")
            .with_provider(Provider::OpenRouter)
            .with_reasoning("high");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Think"))];
        let config = GenerationConfig::new("o3");

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["reasoning"]["effort"], "high");
        assert!(body.get("reasoning_effort").is_none());
    }

    #[test]
    fn test_build_request_with_reasoning_numeric_openai_stays_nested() {
        let client = OAIClient::new("test-key", "o3").with_reasoning("2048");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Think"))];
        let config = GenerationConfig::new("o3");

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["reasoning"]["max_tokens"], 2048);
        assert!(body.get("reasoning_effort").is_none());
    }

    #[test]
    fn test_build_request_with_reasoning_numeric_openrouter_stays_nested() {
        let client = OAIClient::new("test-key", "o3")
            .with_provider(Provider::OpenRouter)
            .with_reasoning("2048");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Think"))];
        let config = GenerationConfig::new("o3");

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert_eq!(body["reasoning"]["max_tokens"], 2048);
        assert!(body.get("reasoning_effort").is_none());
    }

    #[test]
    fn test_parse_usage() {
        let client = OAIClient::new("test-key", "gpt-4");
        let usage = serde_json::json!({
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "prompt_tokens_details": {
                "cached_tokens": 25
            }
        });

        let parsed = client.parse_usage(&usage);
        assert_eq!(parsed.input_tokens, 100);
        assert_eq!(parsed.output_tokens, 50);
        assert_eq!(parsed.cached_tokens, 25);
    }

    #[test]
    fn test_parse_usage_without_cache() {
        let client = OAIClient::new("test-key", "gpt-4");
        let usage = serde_json::json!({
            "prompt_tokens": 100,
            "completion_tokens": 50
        });

        let parsed = client.parse_usage(&usage);
        assert_eq!(parsed.input_tokens, 100);
        assert_eq!(parsed.output_tokens, 50);
        assert_eq!(parsed.cached_tokens, 0);
    }
}
