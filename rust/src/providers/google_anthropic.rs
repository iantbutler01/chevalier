//! Google Anthropic (Vertex AI with Claude) client implementation
//!
//! This client allows running Anthropic Claude models through Google Cloud's
//! Vertex AI platform using Application Default Credentials (ADC).
//!
//! Uses the same request/response format as direct Anthropic API, but with:
//! - Vertex AI endpoint: `https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}/publishers/anthropic/models/{model}:streamRawPredict`
//! - ADC/OAuth2 authentication instead of API key
//! - `anthropic_version: "vertex-2023-10-16"` header in request body
//!
//! Requires the `google-adc` feature to be enabled.

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use reqwest::StatusCode;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::providers::{
    GenerationConfig, GenerationResponse, InferenceClient, StreamChunk, TraceCallback,
};
use crate::retry::{RetryConfig, retry_with_backoff};
use crate::schema::fix_tool_schema_for_provider;
use crate::types::{AssistantResponse, ChatRole, Provider, ResponsePart, TokenUsage, ToolCall};
use crate::utils::{
    ConversationMessage, convert_messages_to_provider_format, parse_json_value_strict_str,
    validate_image_input_supported,
};

/// Google Anthropic (Vertex AI with Claude) client
///
/// This client runs Claude models via Google Cloud's Vertex AI platform.
/// Authentication is done via Application Default Credentials (ADC).
#[cfg(feature = "google-adc")]
#[derive(Clone)]
pub struct GoogleAnthropicClient {
    model: String,
    project_id: String,
    region: String,
    token_provider: Arc<RwLock<Option<Arc<dyn gcp_auth::TokenProvider>>>>,
    thinking_budget: Option<u32>,
    trace_callback: Option<TraceCallback>,
}

#[cfg(feature = "google-adc")]
impl GoogleAnthropicClient {
    fn normalized_tools(&self, tools: &[serde_json::Value]) -> Vec<serde_json::Value> {
        tools
            .iter()
            .cloned()
            .map(|mut tool| {
                fix_tool_schema_for_provider(&mut tool, "google-anthropic");
                tool
            })
            .collect()
    }

    /// Create a new Google Anthropic client with Application Default Credentials (ADC)
    ///
    /// The project ID is automatically extracted from the service account JSON file
    /// referenced by `GOOGLE_APPLICATION_CREDENTIALS`.
    ///
    /// # Arguments
    /// * `model` - The Claude model name (e.g., "claude-3-5-sonnet-v2@20241022", "claude-sonnet-4@20250514")
    /// * `region` - The Google Cloud region (e.g., "us-east5", "europe-west1")
    ///
    /// # Panics
    /// Panics if `GOOGLE_APPLICATION_CREDENTIALS` is not set or if the credentials
    /// file cannot be read or doesn't contain a `project_id`.
    pub fn from_adc(model: impl Into<String>, region: impl Into<String>) -> Self {
        // Read project_id from the service account JSON file
        let creds_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .expect("GOOGLE_APPLICATION_CREDENTIALS environment variable must be set");

        let creds_content = std::fs::read_to_string(&creds_path)
            .unwrap_or_else(|_| panic!("Failed to read credentials file: {}", creds_path));

        let creds_json: serde_json::Value = parse_json_value_strict_str(&creds_content)
            .expect("Failed to parse credentials file as JSON");

        let project_id = creds_json["project_id"]
            .as_str()
            .expect("Credentials file must contain project_id")
            .to_string();

        Self {
            model: model.into(),
            project_id,
            region: region.into(),
            token_provider: Arc::new(RwLock::new(None)),
            thinking_budget: None,
            trace_callback: None,
        }
    }

    /// Create a new Google Anthropic client with explicit project ID
    ///
    /// Use this if you want to override the project_id from the credentials file.
    ///
    /// # Arguments
    /// * `model` - The Claude model name
    /// * `project_id` - The Google Cloud project ID
    /// * `region` - The Google Cloud region
    pub fn from_adc_with_project(
        model: impl Into<String>,
        project_id: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            model: model.into(),
            project_id: project_id.into(),
            region: region.into(),
            token_provider: Arc::new(RwLock::new(None)),
            thinking_budget: None,
            trace_callback: None,
        }
    }

    /// Set the thinking budget for extended thinking mode
    pub fn with_thinking_budget(mut self, budget: u32) -> Self {
        self.thinking_budget = Some(budget);
        self
    }

    /// Get the Vertex AI endpoint URL for this model
    fn get_endpoint_url(&self) -> String {
        format!(
            "https://{}-aiplatform.googleapis.com/v1/projects/{}/locations/{}/publishers/anthropic/models/{}:streamRawPredict",
            self.region, self.project_id, self.region, self.model
        )
    }

    /// Get a valid access token for ADC authentication
    async fn get_token(&self) -> Result<String> {
        let mut provider = self.token_provider.write().await;

        // Initialize if needed
        if provider.is_none() {
            let tp = gcp_auth::provider()
                .await
                .map_err(|e| Error::NonRetryable(format!("Failed to initialize ADC: {}", e)))?;
            *provider = Some(tp);
        }

        // Get token with cloud-platform scope
        let tp = provider.as_ref().unwrap();
        let scopes = &["https://www.googleapis.com/auth/cloud-platform"];
        let token = tp
            .token(scopes)
            .await
            .map_err(|e| Error::NonRetryable(format!("Failed to get ADC token: {}", e)))?;

        Ok(token.as_str().to_string())
    }

    /// Build the request body for Vertex AI Anthropic endpoint
    ///
    /// Uses Anthropic request format but with:
    /// - No "model" field (model is in the URL)
    /// - `anthropic_version: "vertex-2023-10-16"` instead of API version header
    fn build_request_body(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
        stream: bool,
    ) -> Result<serde_json::Value> {
        let model = config.effective_model(&self.model);
        validate_image_input_supported(messages, Provider::GoogleAnthropic, model)?;

        // Extract system message if present
        let (system, messages) = self.extract_system_message(messages)?;

        // Convert messages to Anthropic format
        let formatted_messages =
            convert_messages_to_provider_format(messages, Provider::Anthropic)?;

        // Wrap string content in proper format
        let formatted_messages = self.wrap_string_content(formatted_messages);

        let mut request = serde_json::json!({
            // Note: model is NOT included - it's in the URL for Vertex AI
            "anthropic_version": "vertex-2023-10-16",
            "max_tokens": config.max_tokens.unwrap_or(4096),
            "messages": formatted_messages,
            "stream": stream,
        });

        // Add temperature and top_p if not in thinking mode
        if self.thinking_budget.is_none() {
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
            request["tools"] = serde_json::json!(self.normalized_tools(tools));
            // Enable parallel tool calling via tool_choice
            request["tool_choice"] = serde_json::json!({
                "type": "auto",
                "disable_parallel_tool_use": false
            });
        }

        // Add extended thinking if configured
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
                                Provider::GoogleAnthropic,
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

    /// Make HTTP request to Vertex AI endpoint
    async fn make_request(
        &self,
        body: serde_json::Value,
        timeout: Option<std::time::Duration>,
    ) -> Result<reqwest::Response> {
        let token = self.get_token().await?;
        let client = reqwest::Client::new();

        let response = client
            .post(self.get_endpoint_url())
            .timeout(timeout.unwrap_or(std::time::Duration::from_secs(300)))
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

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
    #[cfg(feature = "google-adc")]
    async fn make_request_with_retry(
        &self,
        body: serde_json::Value,
        timeout: Option<std::time::Duration>,
        retry_config: Option<RetryConfig>,
    ) -> Result<serde_json::Value> {
        let config = retry_config.unwrap_or_default();

        retry_with_backoff(config, || async {
            let response = self.make_request(body.clone(), timeout).await?;
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

#[cfg(feature = "google-adc")]
#[async_trait]
impl InferenceClient for GoogleAnthropicClient {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        let request_body = self.build_request_body(messages, config, false)?;
        let body = self
            .make_request_with_retry(request_body, config.timeout, config.retry_config.clone())
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
                                "google-anthropic SSE stream ended with unfinished tool calls; flushing pending tools at EOF"
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
        Provider::GoogleAnthropic
    }

    fn set_trace_callback(&mut self, callback: TraceCallback) {
        self.trace_callback = Some(callback);
    }
}

#[cfg(all(test, feature = "google-adc"))]
mod tests {
    use super::*;
    use crate::types::{CacheMarker, ChatMessage};

    #[test]
    fn test_endpoint_url() {
        let client = GoogleAnthropicClient::from_adc_with_project(
            "claude-3-5-sonnet-v2@20241022",
            "my-project",
            "us-east5",
        );

        assert_eq!(
            client.get_endpoint_url(),
            "https://us-east5-aiplatform.googleapis.com/v1/projects/my-project/locations/us-east5/publishers/anthropic/models/claude-3-5-sonnet-v2@20241022:streamRawPredict"
        );
    }

    #[test]
    fn test_with_thinking_budget() {
        let client = GoogleAnthropicClient::from_adc_with_project(
            "claude-3-5-sonnet-v2@20241022",
            "my-project",
            "us-east5",
        )
        .with_thinking_budget(1024);

        assert_eq!(client.thinking_budget, Some(1024));
    }

    #[test]
    fn test_extract_multiple_system_messages() {
        let client = GoogleAnthropicClient::from_adc_with_project(
            "claude-sonnet-4@20250514",
            "test-project",
            "us-east5",
        );
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
        assert!(system_val[0]["cache_control"].is_object());
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_build_request_with_tools_defaults_to_strict() {
        let client = GoogleAnthropicClient::from_adc_with_project(
            "claude-3-5-sonnet-v2@20241022",
            "my-project",
            "us-east5",
        );
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let tools = vec![serde_json::json!({"name": "get_weather"})];
        let config = GenerationConfig::new("claude-3-5-sonnet-v2@20241022").with_tools(tools);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert!(body["tools"][0].get("strict").is_none());
        assert_eq!(body["tool_choice"]["type"], "auto");
        assert_eq!(body["tool_choice"]["disable_parallel_tool_use"], false);
    }
}
