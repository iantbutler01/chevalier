//! AWS Bedrock client implementation
//!
//! Bedrock wraps Anthropic's Claude models via AWS infrastructure.
//! Key differences from direct Anthropic API:
//! - Uses AWS SDK (boto3 → aws-sdk-bedrockruntime in Rust)
//! - Requires AWS credentials and region
//! - Same message format as Anthropic (coalescing, content wrapping)
//! - Streaming uses invoke_model_with_response_stream
//! - Same chunk types as Anthropic (content_block_*)
#[cfg(feature = "bedrock")]
use async_trait::async_trait;
#[cfg(feature = "bedrock")]
use futures::stream::Stream;
#[cfg(feature = "bedrock")]
use std::pin::Pin;

#[cfg(feature = "bedrock")]
use crate::error::{Error, Result};
#[cfg(feature = "bedrock")]
use crate::providers::{
    GenerationConfig, GenerationResponse, InferenceClient, StreamChunk, TraceCallback,
};
#[cfg(feature = "bedrock")]
use crate::schema::fix_tool_schema_for_provider;
#[cfg(feature = "bedrock")]
use crate::types::{AssistantResponse, Provider, ResponsePart, ToolCall};
#[cfg(feature = "bedrock")]
use crate::utils::{
    ConversationMessage, convert_messages_to_provider_format, validate_image_input_supported,
};

#[cfg(feature = "bedrock")]
use {
    crate::providers::TokenUsage,
    crate::providers::anthropic_streaming::{ToolCallAccumulator, parse_anthropic_chunk},
    crate::utils::{JsonStreamAccumulator, parse_json_value_strict_bytes},
    aws_sdk_bedrockruntime::{
        Client as BedrockRuntimeClient, primitives::Blob, types::ResponseStream,
    },
    futures::StreamExt,
    std::collections::VecDeque,
    std::sync::Arc,
    tokio::sync::OnceCell,
};

/// AWS Bedrock client for Claude models
#[derive(Clone)]
#[cfg(feature = "bedrock")]
pub struct BedrockClient {
    model: String,
    region_name: String,
    anthropic_version: String,
    trace_callback: Option<TraceCallback>,
    #[cfg(feature = "bedrock")]
    runtime_client: Arc<OnceCell<BedrockRuntimeClient>>,
}

#[cfg(feature = "bedrock")]
impl BedrockClient {
    fn normalized_tools(&self, tools: &[serde_json::Value]) -> Vec<serde_json::Value> {
        tools
            .iter()
            .cloned()
            .map(|mut tool| {
                fix_tool_schema_for_provider(&mut tool, "bedrock");
                tool
            })
            .collect()
    }

    /// Create a new Bedrock client
    ///
    /// # Arguments
    /// * `model` - Model ID (e.g., "anthropic.claude-3-5-sonnet-20240620-v1:0")
    /// * `region_name` - AWS region (default: "us-east-1")
    pub fn new(model: impl Into<String>, region_name: Option<String>) -> Self {
        Self {
            model: model.into(),
            region_name: region_name.unwrap_or_else(|| "us-east-1".to_string()),
            anthropic_version: "bedrock-2023-05-31".to_string(),
            trace_callback: None,
            #[cfg(feature = "bedrock")]
            runtime_client: Arc::new(OnceCell::new()),
        }
    }

    #[cfg(feature = "bedrock")]
    /// Initialize the AWS SDK client (lazy initialization)
    async fn get_client(&self) -> Result<&BedrockRuntimeClient> {
        self.runtime_client
            .get_or_try_init(|| async {
                #[allow(deprecated)]
                let config = aws_config::from_env()
                    .region(aws_config::Region::new(self.region_name.clone()))
                    .load()
                    .await;
                Ok::<_, Error>(BedrockRuntimeClient::new(&config))
            })
            .await
    }

    /// Build request body (same format as Anthropic)
    #[cfg(feature = "bedrock")]
    fn build_request_body(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<serde_json::Value> {
        let model = config.effective_model(&self.model);
        validate_image_input_supported(messages, Provider::Bedrock, model)?;

        // Extract system message if present
        let (system, remaining_messages) = self.extract_system_message(messages)?;

        // Convert messages to Anthropic/Bedrock format
        let mut formatted_messages =
            convert_messages_to_provider_format(remaining_messages, Provider::Bedrock)?;

        // Wrap string content in [{"type": "text"}] format
        // Skip empty strings to avoid "text content blocks must be non-empty" error
        for msg in &mut formatted_messages {
            if let Some(content) = msg.get_mut("content")
                && let Some(text) = content.as_str()
            {
                let text_owned = text.to_string();
                if !text_owned.is_empty() {
                    *content = serde_json::json!([{"type": "text", "text": text_owned}]);
                } else {
                    *content = serde_json::json!([]);
                }
            }
        }

        let mut request = serde_json::json!({
            "anthropic_version": self.anthropic_version,
            "max_tokens": config.max_tokens.unwrap_or(4096),
            "messages": formatted_messages,
        });

        // Add optional parameters
        if let Some(system) = system {
            request["system"] = serde_json::json!(system);
        }

        if let Some(temp) = config.temperature {
            request["temperature"] = serde_json::json!(temp);
        }

        if let Some(top_p) = config.top_p {
            request["top_p"] = serde_json::json!(top_p);
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

        // Add structured output schema if provided
        // Bedrock uses the same format as Anthropic API
        if let Some(ref schema) = config.output_schema {
            request["output_format"] = serde_json::json!({
                "type": "json_schema",
                "schema": schema
            });
        }

        Ok(request)
    }

    /// Extract system message from messages
    #[cfg(feature = "bedrock")]
    fn extract_system_message<'a>(
        &self,
        messages: &'a [ConversationMessage],
    ) -> Result<(Option<String>, &'a [ConversationMessage])> {
        if let Some(ConversationMessage::Chat(chat_msg)) = messages.first()
            && chat_msg.role == crate::types::ChatRole::System
        {
            return Ok((Some(chat_msg.content.clone()), &messages[1..]));
        }
        Ok((None, messages))
    }
}

#[async_trait]
#[cfg(feature = "bedrock")]
impl InferenceClient for BedrockClient {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        #[cfg(feature = "bedrock")]
        {
            let body = self.build_request_body(messages, config)?;
            let client = self.get_client().await?;

            let body_string = serde_json::to_string(&body)?;
            let response = client
                .invoke_model()
                .model_id(&self.model)
                .content_type("application/json")
                .accept("application/json")
                .body(Blob::new(body_string.as_bytes()))
                .send()
                .await
                .map_err(|e| Error::Inference(e.to_string()))?;

            let body_bytes = response.body().as_ref();
            let response_json: serde_json::Value = parse_json_value_strict_bytes(body_bytes)?;

            // Parse response (same format as Anthropic)
            let content_blocks = response_json["content"].as_array();
            let mut response = AssistantResponse::default();

            if let Some(blocks) = content_blocks {
                for block in blocks {
                    if block["type"] == "text" {
                        if let Some(text) = block["text"].as_str()
                            && !text.is_empty()
                        {
                            response.push_output(ResponsePart::Text {
                                text: text.to_string(),
                            });
                        }
                    } else if block["type"] == "thinking" {
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
                    } else if block["type"] == "tool_use" {
                        response.push_output(ResponsePart::Tool {
                            call: ToolCall::from_provider_format(block.clone(), Provider::Bedrock)?,
                        });
                    }
                }
            }

            let usage = TokenUsage {
                input_tokens: response_json["usage"]["input_tokens"].as_u64().unwrap_or(0),
                output_tokens: response_json["usage"]["output_tokens"]
                    .as_u64()
                    .unwrap_or(0),
                cached_tokens: response_json["usage"]["cache_read_input_tokens"]
                    .as_u64()
                    .unwrap_or(0),
                cache_write_input_tokens: response_json["usage"]["cache_creation_input_tokens"]
                    .as_u64()
                    .unwrap_or(0),
                reasoning_tokens: None,
                provider_cost_dollars: None,
            };

            // If tools were provided, return full response for tool extraction
            let has_tools = config.tools.is_some() && !config.tools.as_ref().unwrap().is_empty();
            let has_tool_calls = response.has_tool_calls();

            Ok(GenerationResponse::from_assistant_response(
                response,
                usage,
                None,
                if has_tools || has_tool_calls {
                    Some(response_json)
                } else {
                    None
                },
            ))
        }

        #[cfg(not(feature = "bedrock"))]
        {
            let _ = (messages, config);
            Err(Error::NonRetryable(
                "Bedrock requires 'bedrock' feature flag. Enable with: cargo build --features bedrock".to_string(),
            ))
        }
    }

    async fn connect_and_listen(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
        #[cfg(feature = "bedrock")]
        {
            let body = self.build_request_body(messages, config)?;
            let client = self.get_client().await?;

            let body_string = serde_json::to_string(&body)?;
            let response = client
                .invoke_model_with_response_stream()
                .model_id(&self.model)
                .content_type("application/json")
                .accept("application/json")
                .body(Blob::new(body_string.as_bytes()))
                .send()
                .await
                .map_err(|e| Error::Inference(e.to_string()))?;

            let receiver = response.body;
            let has_tools = config.tools.is_some() && !config.tools.as_ref().unwrap().is_empty();

            // AWS Bedrock EventReceiver uses async recv() instead of Stream trait
            // Convert it to a stream using unfold
            let stream = futures::stream::unfold(
                (
                    receiver,
                    ToolCallAccumulator::new(),
                    JsonStreamAccumulator::new(),
                    has_tools,
                    VecDeque::<Result<StreamChunk>>::new(),
                    false,
                ),
                |(
                    mut recv,
                    mut accumulator,
                    mut json_parser,
                    has_tools,
                    mut pending,
                    eof_flushed,
                )| async move {
                    loop {
                        if let Some(item) = pending.pop_front() {
                            return Some((
                                futures::stream::iter(vec![item]),
                                (
                                    recv,
                                    accumulator,
                                    json_parser,
                                    has_tools,
                                    pending,
                                    eof_flushed,
                                ),
                            ));
                        }

                        if eof_flushed {
                            return None;
                        }

                        match recv.recv().await {
                            Ok(Some(event)) => {
                                let chunk_bytes = match event {
                                    ResponseStream::Chunk(payload_part) => payload_part
                                        .bytes
                                        .map(|b| b.into_inner())
                                        .unwrap_or_default(),
                                    _ => Vec::new(),
                                };

                                match json_parser.push_bytes(&chunk_bytes) {
                                    Ok(values) => {
                                        for json in values {
                                            pending.extend(
                                                parse_anthropic_chunk(
                                                    &json,
                                                    &mut accumulator,
                                                    has_tools,
                                                )
                                                .into_iter()
                                                .map(Ok),
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        pending.push_back(Err(e));
                                    }
                                }
                            }
                            Ok(None) => {
                                let flushed = accumulator.drain_all_tools();
                                if flushed.is_empty() {
                                    return None;
                                }

                                tracing::warn!(
                                    pending_tool_calls = flushed.len(),
                                    "bedrock anthropic stream ended with unfinished tool calls; flushing pending tools at EOF"
                                );
                                pending.extend(
                                    flushed
                                        .into_iter()
                                        .map(StreamChunk::ToolCallComplete)
                                        .map(Ok),
                                );
                                return pending.pop_front().map(|item| {
                                    (
                                        futures::stream::iter(vec![item]),
                                        (
                                            recv,
                                            accumulator,
                                            json_parser,
                                            has_tools,
                                            pending,
                                            true,
                                        ),
                                    )
                                });
                            }
                            Err(e) => {
                                return Some((
                                    futures::stream::iter(vec![Err(Error::Inference(format!(
                                        "Bedrock stream error: {}",
                                        e
                                    )))]),
                                    (recv, accumulator, json_parser, has_tools, pending, true),
                                ));
                            }
                        }
                    }
                },
            )
            .flatten();

            Ok(Box::pin(stream))
        }

        #[cfg(not(feature = "bedrock"))]
        {
            let _ = (messages, config);
            Err(Error::NonRetryable(
                "Bedrock streaming requires 'bedrock' feature flag. Enable with: cargo build --features bedrock".to_string(),
            ))
        }
    }

    fn provider(&self) -> Provider {
        Provider::Bedrock
    }

    fn set_trace_callback(&mut self, callback: TraceCallback) {
        self.trace_callback = Some(callback);
    }
}

#[cfg(test)]
#[cfg(feature = "bedrock")]
mod tests {
    use super::*;
    use crate::types::ChatMessage;

    #[test]
    fn test_new_client() {
        let client = BedrockClient::new("anthropic.claude-3-5-sonnet-20240620-v1:0", None);
        assert_eq!(client.region_name, "us-east-1");
        assert_eq!(client.anthropic_version, "bedrock-2023-05-31");
        assert_eq!(client.provider(), Provider::Bedrock);
    }

    #[test]
    fn test_new_client_with_region() {
        let client = BedrockClient::new(
            "anthropic.claude-3-5-sonnet-20240620-v1:0",
            Some("us-west-2".to_string()),
        );
        assert_eq!(client.region_name, "us-west-2");
    }

    #[test]
    fn test_extract_system_message() {
        let client = BedrockClient::new("test-model", None);

        let messages = vec![
            ConversationMessage::Chat(ChatMessage::system("You are helpful")),
            ConversationMessage::Chat(ChatMessage::user("Hello")),
        ];

        let (system, remaining) = client.extract_system_message(&messages).unwrap();
        assert_eq!(system, Some("You are helpful".to_string()));
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_extract_system_message_none() {
        let client = BedrockClient::new("test-model", None);

        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];

        let (system, remaining) = client.extract_system_message(&messages).unwrap();
        assert_eq!(system, None);
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_build_request_body() {
        let client = BedrockClient::new("test-model", None);
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("test-model");

        let body = client.build_request_body(&messages, &config).unwrap();

        assert_eq!(body["anthropic_version"], "bedrock-2023-05-31");
        assert_eq!(body["max_tokens"], 4096);
        assert!(body["messages"].is_array());
    }

    #[test]
    fn test_build_request_body_with_system() {
        let client = BedrockClient::new("test-model", None);
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::system("You are helpful")),
            ConversationMessage::Chat(ChatMessage::user("Hello")),
        ];
        let config = GenerationConfig::new("test-model");

        let body = client.build_request_body(&messages, &config).unwrap();

        assert_eq!(body["system"], "You are helpful");
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_build_request_wraps_string_content() {
        let client = BedrockClient::new("test-model", None);
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("test-model");

        let body = client.build_request_body(&messages, &config).unwrap();
        let first_msg = &body["messages"][0];

        // Should wrap string in [{"type": "text", "text": "..."}]
        assert!(first_msg["content"].is_array());
        assert_eq!(first_msg["content"][0]["type"], "text");
        assert_eq!(first_msg["content"][0]["text"], "Hello");
    }

    #[test]
    fn test_build_request_body_with_tools_defaults_to_strict() {
        let client = BedrockClient::new("test-model", None);
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let tools = vec![serde_json::json!({"name": "get_weather"})];
        let config = GenerationConfig::new("test-model").with_tools(tools);

        let body = client.build_request_body(&messages, &config).unwrap();

        assert!(body["tools"][0].get("strict").is_none());
        assert_eq!(body["tool_choice"]["type"], "auto");
        assert_eq!(body["tool_choice"]["disable_parallel_tool_use"], false);
    }

    #[tokio::test]
    async fn test_get_generation_without_feature() {
        let client = BedrockClient::new("test-model", None);
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("test-model");

        let result = client.get_generation(&messages, &config).await;
        assert!(result.is_err());
        #[cfg(not(feature = "bedrock"))]
        assert!(result.unwrap_err().to_string().contains("bedrock"));
    }
}
