//! Inference orchestration utilities
//!
//! Handles LLM API calls with native tools or parser-based approaches.

use futures::stream::{Stream, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::providers::{
    AnthropicClient, GenerationConfig, GoogleGenAIClient, InferenceClient, OAIClient,
    OpenAICodexResponsesClient, OpenAIResponsesClient, OpenRouterClient, OpenRouterResponsesClient,
    PromptCacheRetention, ProviderConfig, StreamChunk,
};
use crate::schema::{
    apply_tool_strict_for_provider, fix_output_schema_for_provider,
    fix_tool_parameters_schema_for_provider,
};
use crate::types::{
    AssistantResponse, ChatMessage, CreateResult, Provider, ReasoningSegment, ResponsePart,
    ResponseStreamEvent, TokenUsage, ToolCall,
};
use crate::utils::ConversationMessage;

use super::{Accumulators, ToolFunction, ToolSchemaInfo};

fn supports_system_message_blocks(provider: Provider) -> bool {
    matches!(provider, Provider::Anthropic | Provider::GoogleAnthropic)
}

fn build_runtime_messages(
    prompt: Option<&str>,
    system: Option<&str>,
    system_messages: Option<Vec<ChatMessage>>,
    history: Option<Vec<ConversationMessage>>,
    provider: Option<Provider>,
) -> Vec<ConversationMessage> {
    let mut messages = Vec::new();

    if let Some(system_messages) = system_messages.filter(|messages| !messages.is_empty()) {
        if provider.is_some_and(supports_system_message_blocks) {
            messages.extend(system_messages.into_iter().map(ConversationMessage::Chat));
        } else {
            let joined = system_messages
                .into_iter()
                .map(|msg| msg.content)
                .filter(|content| !content.is_empty())
                .collect::<Vec<_>>()
                .join("\n\n");
            if !joined.is_empty() {
                messages.push(ConversationMessage::Chat(ChatMessage::system(joined)));
            }
        }
    } else if let Some(sys) = system {
        messages.push(ConversationMessage::Chat(ChatMessage::system(sys)));
    }

    if let Some(hist) = history {
        messages.extend(hist);
    }

    if let Some(p) = prompt {
        messages.push(ConversationMessage::Chat(ChatMessage::user(p)));
    }

    messages
}

fn resolve_provider_for_caching(model: &str) -> Option<Provider> {
    if resolve_provider_key(model) == "openai-codex-responses" {
        return Some(Provider::OpenAIResponses);
    }
    Provider::from_model_string(model)
        .ok()
        .map(|(provider, _)| provider)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedModelString {
    provider: String,
    model_name: String,
    reasoning: Option<String>,
    server_url: Option<String>,
    inline_api_key: Option<String>,
    prompt_cache_retention: Option<PromptCacheRetention>,
}

fn parse_prompt_cache_retention(value: &str) -> Result<PromptCacheRetention> {
    match value {
        "in_memory" => Ok(PromptCacheRetention::InMemory),
        "24h" => Ok(PromptCacheRetention::H24),
        other => Err(Error::NonRetryable(format!(
            "Unsupported @cache value '{}'. Expected 'in_memory' or '24h'",
            other
        ))),
    }
}

fn parse_model_string(model_str: &str) -> Result<ParsedModelString> {
    let parts: Vec<&str> = model_str.split(':').collect();
    if parts.len() < 2 {
        return Err(Error::NonRetryable(format!(
            "Invalid model string format: {}. Expected 'provider:model'",
            model_str
        )));
    }

    let (provider, model_part) = if parts.len() >= 3 && parts[1] == "resp" {
        (format!("{}-responses", parts[0]), parts[2..].join(":"))
    } else {
        (parts[0].to_string(), parts[1..].join(":"))
    };

    let mut reasoning = None;
    let mut server_url = None;
    let mut inline_api_key = None;
    let mut prompt_cache_retention = None;

    let model_name = if model_part.contains('@') {
        let model_parts: Vec<&str> = model_part.split('@').collect();

        for param in &model_parts[1..] {
            if param.starts_with("reasoning=") {
                reasoning = Some(param.strip_prefix("reasoning=").unwrap().to_string());
            } else if param.starts_with("server_url=") {
                server_url = Some(param.strip_prefix("server_url=").unwrap().to_string());
            } else if param.starts_with("api_key=") {
                inline_api_key = Some(param.strip_prefix("api_key=").unwrap().to_string());
            } else if param.starts_with("cache=") {
                prompt_cache_retention = Some(parse_prompt_cache_retention(
                    param.strip_prefix("cache=").unwrap(),
                )?);
            }
        }

        model_parts[0].to_string()
    } else {
        model_part.to_string()
    };

    Ok(ParsedModelString {
        provider,
        model_name,
        reasoning,
        server_url,
        inline_api_key,
        prompt_cache_retention,
    })
}

/// Result from non-streaming LLM call
pub struct CallResult {
    pub response: AssistantResponse,
}

async fn stream_chunk_to_runtime_events(
    chunk: StreamChunk,
    accumulators: Arc<RwLock<Accumulators>>,
    response: Arc<RwLock<AssistantResponse>>,
) -> Result<Vec<ResponseStreamEvent>> {
    match chunk {
        StreamChunk::Content(text) => {
            {
                let mut acc = accumulators.write().await;
                acc.raw_response.push(text.clone());
            }
            {
                let mut response = response.write().await;
                response.push_output(ResponsePart::Text { text: text.clone() });
            }
            Ok(vec![ResponseStreamEvent::Output(ResponsePart::Text {
                text,
            })])
        }
        StreamChunk::Reasoning(text) => {
            let mut acc = accumulators.write().await;
            acc.reasoning.push(text.clone());

            if let Some(current) = acc.current_reasoning_segment.as_mut() {
                current.content.push_str(&text);
                let updated_content = current.content.clone();
                if let Some(last) = acc.reasoning_segments.last_mut() {
                    last.content = updated_content;
                }
            } else {
                let segment =
                    ReasoningSegment::with_index(text.clone(), acc.reasoning_segments.len());
                acc.reasoning_segments.push(segment.clone());
                acc.current_reasoning_segment = Some(segment);
            }
            drop(acc);

            {
                let mut response = response.write().await;
                response.push_output(ResponsePart::Reasoning { text: text.clone() });
            }

            Ok(vec![ResponseStreamEvent::Output(ResponsePart::Reasoning {
                text,
            })])
        }
        StreamChunk::Signature(sig) => {
            let mut acc = accumulators.write().await;
            if let Some(current) = acc.current_reasoning_segment.as_mut() {
                current.signature = Some(sig.clone());
                if let Some(last) = acc.reasoning_segments.last_mut() {
                    last.signature = Some(sig.clone());
                }
            }
            drop(acc);

            {
                let mut response = response.write().await;
                response.push_output(ResponsePart::Signature { value: sig.clone() });
            }

            Ok(vec![ResponseStreamEvent::Output(ResponsePart::Signature {
                value: sig,
            })])
        }
        StreamChunk::ToolCallComplete(tool) => {
            let tool_calls = match ToolCall::create(tool)? {
                CreateResult::Single(call) => vec![call],
                CreateResult::Multiple(calls) => calls,
                CreateResult::Empty => Vec::new(),
            };

            let mut events = Vec::new();
            let mut response_guard = response.write().await;
            for call in tool_calls {
                response_guard.push_output(ResponsePart::Tool { call: call.clone() });
                events.push(ResponseStreamEvent::Output(ResponsePart::Tool { call }));
            }
            Ok(events)
        }
        StreamChunk::ToolCallPartial(tool) => Ok(vec![ResponseStreamEvent::ToolPartial(tool)]),
        StreamChunk::Usage {
            input_tokens,
            output_tokens,
            cached_tokens,
            cache_write_input_tokens,
        } => Ok(vec![ResponseStreamEvent::Usage(TokenUsage {
            input_tokens,
            output_tokens,
            cached_tokens,
            cache_write_input_tokens,
        })]),
        StreamChunk::RateLimits(rate_limits) => {
            Ok(vec![ResponseStreamEvent::RateLimits(rate_limits)])
        }
    }
}

/// Generate native tool schemas for the given model/provider
///
/// Creates provider-specific tool schemas from registered tools.
/// Uses the tool_schemas mapping for tools with explicit schema info.
///
/// Tools are serialized in REGISTRATION order, not sorted order. Tool
/// definitions render at the very front of the provider prompt, so their byte
/// order is the root of the prompt-cache prefix: appending a newly selected
/// tool extends the prefix (cheap — prior tool bytes still match), whereas
/// re-sorting would splice it into the middle and invalidate every cached
/// prefix from the insertion point onward. Names absent from `tool_order`
/// (defensive — should not happen) are appended in sorted order so output
/// stays deterministic.
fn generate_tool_schemas(
    tools: &HashMap<String, ToolFunction>,
    tool_schemas: &HashMap<String, ToolSchemaInfo>,
    tool_order: &[String],
    model: &str,
) -> Result<Vec<serde_json::Value>> {
    if tools.is_empty() {
        return Ok(Vec::new());
    }

    // Parse provider from model string
    let provider = resolve_provider_key(model);

    // Get appropriate schema generator
    let generator = crate::schema::get_schema_generator(&provider)?;

    // For each tool, generate schema
    let mut schemas = Vec::new();

    let mut tool_names: Vec<&String> = tool_order
        .iter()
        .filter(|name| tools.contains_key(*name))
        .collect();
    let mut unordered: Vec<&String> = tools
        .keys()
        .filter(|name| !tool_order.contains(name))
        .collect();
    unordered.sort();
    tool_names.extend(unordered);

    for tool_name in tool_names {
        // Check if we have schema info for this tool
        let tool_schema = if let Some(schema_info) = tool_schemas.get(tool_name) {
            let mut parameters = schema_info.parameters.to_json_schema();
            fix_tool_parameters_schema_for_provider(&mut parameters, &provider);
            let mut tool_schema =
                generator.generate_schema(tool_name, &schema_info.description, parameters);
            apply_tool_strict_for_provider(&mut tool_schema, &provider, schema_info.strict);
            tool_schema
        } else {
            // No schema info - generate minimal schema
            let mut parameters = serde_json::json!({
                "type": "object",
                "properties": {},
                "required": []
            });
            fix_output_schema_for_provider(&mut parameters, &provider);
            generator.generate_schema(tool_name, &format!("Tool: {}", tool_name), parameters)
        };

        schemas.push(tool_schema);
    }

    Ok(schemas)
}

/// Resolve provider key for model strings, including responses modifiers.
fn resolve_provider_key(model: &str) -> String {
    let parts: Vec<&str> = model.split(':').collect();
    if parts.len() >= 3 && parts[1] == "resp" {
        match parts[0] {
            "openai" => "openai-responses".to_string(),
            "openrouter" => "openrouter-responses".to_string(),
            "openai-codex-responses" => "openai-codex-responses".to_string(),
            other => other.to_string(),
        }
    } else {
        parts.first().unwrap_or(&model).to_string()
    }
}

/// Create an inference client from model string
///
/// Format: `provider:model@param=value` or `provider:resp:model@param=value`
///
/// Parameters:
/// - `@reasoning=<value>` - reasoning effort (numeric budget or level like `high`)
/// - `@cache=<value>` - OpenAI prompt cache retention (`in_memory` or `24h`)
/// - `@vision=<true|false>` - image-input capability override consumed by `Provider::supports_image_input`
/// - `@server_url=<url>` - custom API endpoint (required for `custom-openai`, optional for `openai`)
/// - `@api_key=<key>` - inline API key (overrides env var and `api_key` parameter)
///
/// Examples:
/// - `anthropic:claude-3-5-sonnet-20241022`
/// - `openrouter:openai/gpt-4o@reasoning=high`
/// - `openrouter:qwen/qwen2.5-vl-72b-instruct@vision=true`
/// - `openai:gpt-5.1@cache=24h`
/// - `openai:gpt-4o`
/// - `openai:resp:gpt-4o`
/// - `openrouter:resp:openai/o4-mini`
/// - `custom-openai:llama-3@server_url=http://localhost:8000/v1`
/// - `custom-openai:my-model@server_url=http://localhost:11434/v1@api_key=ollama`
pub fn create_inference_client(
    model_str: &str,
    api_key: Option<&str>,
) -> Result<Box<dyn InferenceClient>> {
    create_inference_client_with_config(model_str, api_key, None)
}

fn create_inference_client_with_config(
    model_str: &str,
    api_key: Option<&str>,
    provider_config: Option<&ProviderConfig>,
) -> Result<Box<dyn InferenceClient>> {
    let parsed = parse_model_string(model_str)?;
    let provider = parsed.provider;
    let reasoning = parsed.reasoning;
    let server_url = parsed.server_url;
    let inline_api_key = parsed.inline_api_key;
    let model_name = parsed.model_name;

    // Resolve API key: @api_key= > api_key parameter > env var
    let key = if let Some(k) = inline_api_key {
        k
    } else if let Some(k) = api_key {
        k.to_string()
    } else {
        match provider.as_str() {
            "anthropic" => std::env::var("ANTHROPIC_API_KEY")
                .map_err(|_| Error::NonRetryable("ANTHROPIC_API_KEY not set".to_string()))?,
            "openai" => std::env::var("OPENAI_API_KEY")
                .map_err(|_| Error::NonRetryable("OPENAI_API_KEY not set".to_string()))?,
            "openai-responses" => std::env::var("OPENAI_API_KEY")
                .map_err(|_| Error::NonRetryable("OPENAI_API_KEY not set".to_string()))?,
            "openai-codex-responses" => resolve_codex_subscription_token(api_key, provider_config)?,
            "openrouter" => std::env::var("OPENROUTER_API_KEY")
                .or_else(|_| std::env::var("OPENROUTER_KEY"))
                .map_err(|_| Error::NonRetryable("OPENROUTER_API_KEY not set".to_string()))?,
            "openrouter-responses" => std::env::var("OPENROUTER_API_KEY")
                .or_else(|_| std::env::var("OPENROUTER_KEY"))
                .map_err(|_| Error::NonRetryable("OPENROUTER_API_KEY not set".to_string()))?,
            "google" | "google-gemini" | "google-genai" | "gemini" => {
                std::env::var("GOOGLE_GEMINI_API_KEY")
                    .or_else(|_| std::env::var("GEMINI_API_KEY"))
                    .map_err(|_| {
                        Error::NonRetryable(
                            "GOOGLE_GEMINI_API_KEY or GEMINI_API_KEY not set".to_string(),
                        )
                    })?
            }
            "custom-openai" => {
                std::env::var("OPENAI_API_KEY").unwrap_or_else(|_| "not-needed".to_string())
            }
            _ => {
                return Err(Error::NonRetryable(format!(
                    "Unknown provider: {}",
                    provider
                )));
            }
        }
    };

    // Create client
    let client: Box<dyn InferenceClient> = match provider.as_str() {
        "anthropic" => {
            let mut client = AnthropicClient::new(key, model_name);
            // `@reasoning=` mapping for Anthropic models:
            //   <number>                      -> legacy extended thinking with a
            //                                    token budget (pre-4.6 models;
            //                                    400s on Opus 4.7+ / Fable)
            //   adaptive                      -> adaptive thinking
            //   low|medium|high|xhigh|max     -> adaptive thinking + that
            //                                    `output_config.effort` level
            // Anything else is rejected loudly instead of being silently
            // dropped (the previous behavior for non-numeric values).
            if let Some(r) = reasoning {
                if let Ok(budget) = r.parse::<u32>() {
                    client = client.with_thinking_budget(budget);
                } else if r == "adaptive" {
                    client = client.with_adaptive_thinking();
                } else if matches!(r.as_str(), "low" | "medium" | "high" | "xhigh" | "max") {
                    client = client.with_adaptive_thinking().with_effort(r);
                } else {
                    return Err(Error::NonRetryable(format!(
                        "Unsupported @reasoning value '{}' for anthropic. Expected a numeric \
                         thinking budget, 'adaptive', or an effort level \
                         (low|medium|high|xhigh|max).",
                        r
                    )));
                }
            }
            Box::new(client)
        }
        "openai" => {
            let mut client = OAIClient::new(key, model_name);
            if let Some(url) = server_url {
                client = client.with_api_url(url);
            } else if let Ok(base_url) = std::env::var("OPENAI_BASE_URL") {
                client = client.with_api_url(base_url);
            }
            if let Some(r) = reasoning {
                client = client.with_reasoning(r);
            }
            Box::new(client)
        }
        "openai-responses" => {
            let mut client = OpenAIResponsesClient::new(key, model_name);
            if let Some(r) = reasoning {
                client = client.with_reasoning(r);
            }
            Box::new(client)
        }
        "openai-codex-responses" => {
            let config = resolve_codex_subscription_config(key, server_url, provider_config)?;
            let mut client = OpenAICodexResponsesClient::new(config, model_name)?;
            if let Some(r) = reasoning {
                client = client.with_reasoning(r);
            }
            Box::new(client)
        }
        "openrouter" => {
            let mut client = OpenRouterClient::new(key, model_name, None, None);
            if let Some(r) = reasoning {
                client = client.with_reasoning(r);
            }
            Box::new(client)
        }
        "openrouter-responses" => {
            let mut client = OpenRouterResponsesClient::new(key, model_name, None, None);
            if let Some(r) = reasoning {
                client = client.with_reasoning(r);
            }
            Box::new(client)
        }
        "google" | "google-gemini" | "google-genai" | "gemini" => {
            let mut client = GoogleGenAIClient::new(key, model_name);
            if let Some(r) = reasoning
                && let Ok(budget) = r.parse::<u32>()
            {
                client = client.with_thinking_budget(budget);
            }
            Box::new(client)
        }
        "custom-openai" => {
            let url = server_url.ok_or_else(|| {
                Error::NonRetryable(
                    "custom-openai requires @server_url= parameter (e.g., custom-openai:model@server_url=http://localhost:8000/v1)".to_string(),
                )
            })?;
            let mut client = OAIClient::new(key, model_name).with_api_url(url);
            if let Some(r) = reasoning {
                client = client.with_reasoning(r);
            }
            Box::new(client)
        }
        _ => {
            return Err(Error::NonRetryable(format!(
                "Unsupported provider: {}",
                provider
            )));
        }
    };

    Ok(client)
}

fn resolve_codex_subscription_token(
    api_key: Option<&str>,
    provider_config: Option<&ProviderConfig>,
) -> Result<String> {
    if let Some(ProviderConfig::CodexSubscription(config)) = provider_config {
        return Ok(config.token.clone());
    }
    if let Some(api_key) = api_key.filter(|value| !value.trim().is_empty()) {
        return Ok(api_key.to_string());
    }
    std::env::var("OPENBRACKET_CODEX_SUBSCRIPTION_TOKEN")
        .or_else(|_| std::env::var("CODEX_SUBSCRIPTION_TOKEN"))
        .map_err(|_| {
            Error::NonRetryable(
                "OPENBRACKET_CODEX_SUBSCRIPTION_TOKEN not set for openai-codex-responses"
                    .to_string(),
            )
        })
}

fn resolve_codex_subscription_config(
    token: String,
    server_url: Option<String>,
    provider_config: Option<&ProviderConfig>,
) -> Result<crate::providers::CodexSubscriptionProviderConfig> {
    if let Some(ProviderConfig::CodexSubscription(config)) = provider_config {
        // The variant is boxed (clippy large_enum_variant); clone the inner config out.
        let mut config = (**config).clone();
        if config.base_url.is_none() {
            config.base_url = server_url;
        }
        return Ok(config);
    }

    let account_id = std::env::var("OPENBRACKET_CODEX_ACCOUNT_ID")
        .or_else(|_| std::env::var("CODEX_ACCOUNT_ID"))
        .ok();
    let base_url = server_url
        .or_else(|| std::env::var("OPENBRACKET_CODEX_BASE_URL").ok())
        .or_else(|| std::env::var("CODEX_BASE_URL").ok());
    let transport = std::env::var("OPENBRACKET_CODEX_TRANSPORT")
        .or_else(|_| std::env::var("CODEX_TRANSPORT"))
        .ok()
        .as_deref()
        .map(parse_codex_transport)
        .transpose()?;
    let sse_header_timeout = std::env::var("OPENBRACKET_CODEX_SSE_HEADER_TIMEOUT_MS")
        .ok()
        .as_deref()
        .map(parse_duration_ms)
        .transpose()?;
    let websocket_connect_timeout = std::env::var("OPENBRACKET_CODEX_WEBSOCKET_CONNECT_TIMEOUT_MS")
        .ok()
        .as_deref()
        .map(parse_duration_ms)
        .transpose()?;

    Ok(crate::providers::CodexSubscriptionProviderConfig {
        token,
        account_id,
        base_url,
        transport,
        sse_header_timeout,
        websocket_connect_timeout,
        reasoning_effort: None,
        reasoning_summary: None,
        text_verbosity: None,
        service_tier: None,
    })
}

fn parse_codex_transport(value: &str) -> Result<crate::providers::CodexSubscriptionTransport> {
    match value {
        "auto" => Ok(crate::providers::CodexSubscriptionTransport::Auto),
        "websocket" | "ws" => Ok(crate::providers::CodexSubscriptionTransport::WebSocket),
        "sse" => Ok(crate::providers::CodexSubscriptionTransport::Sse),
        other => Err(Error::NonRetryable(format!(
            "Unsupported Codex transport '{}'. Expected auto, websocket, or sse.",
            other
        ))),
    }
}

fn parse_duration_ms(value: &str) -> Result<std::time::Duration> {
    let millis = value.parse::<u64>().map_err(|_| {
        Error::NonRetryable(format!(
            "Invalid Codex timeout '{}'. Expected milliseconds.",
            value
        ))
    })?;
    Ok(std::time::Duration::from_millis(millis))
}

/// Execute a non-streaming LLM call
#[allow(clippy::too_many_arguments)]
pub async fn call_llm(
    prompt: Option<&str>,
    model: &str,
    tools: Arc<RwLock<HashMap<String, ToolFunction>>>,
    tool_order: Arc<RwLock<Vec<String>>>,
    tool_schema_info: Arc<RwLock<HashMap<String, ToolSchemaInfo>>>,
    output_type_name: Option<String>,
    output_schema: Option<serde_json::Value>,
    api_key: Option<&str>,
    system: Option<&str>,
    system_messages: Option<Vec<ChatMessage>>,
    history: Option<Vec<ConversationMessage>>,
    temperature: Option<f32>,
    top_p: Option<f32>,
    max_tokens: Option<u32>,
    timeout: Option<std::time::Duration>,
    retry_config: Option<crate::retry::RetryConfig>,
    provider_config: Option<ProviderConfig>,
    _call_context: Arc<RwLock<Option<HashMap<String, serde_json::Value>>>>,
) -> Result<CallResult> {
    // Create client
    let client = create_inference_client_with_config(model, api_key, provider_config.as_ref())?;
    let parsed_model = parse_model_string(model)?;
    let provider_key = resolve_provider_key(model);
    let message_provider = resolve_provider_for_caching(model);

    let messages =
        build_runtime_messages(prompt, system, system_messages, history, message_provider);

    if messages.is_empty() {
        return Err(Error::NonRetryable(
            "No messages to send to LLM".to_string(),
        ));
    }

    // Generate tool schemas (always enabled - native tools only)
    let tool_schemas = {
        let tools_guard = tools.read().await;
        let schemas_guard = tool_schema_info.read().await;
        let order_guard = tool_order.read().await;
        if !tools_guard.is_empty() {
            Some(generate_tool_schemas(
                &tools_guard,
                &schemas_guard,
                &order_guard,
                model,
            )?)
        } else {
            None
        }
    };

    // Fix output schema for provider-specific requirements
    let fixed_output_schema = output_schema.map(|mut schema| {
        fix_output_schema_for_provider(&mut schema, &provider_key);
        schema
    });

    // Build config
    let config = GenerationConfig {
        model: String::new(), // Use client's default
        max_tokens,
        temperature,
        top_p,
        tools: tool_schemas,
        native_tools: true, // Always true - we only support native tools
        reasoning_effort: None,
        thinking_budget: None,
        output_schema: fixed_output_schema,
        output_type_name,
        timeout,
        retry_config,
        prompt_cache_retention: match provider_key.as_str() {
            "openai" | "openai-responses" => parsed_model.prompt_cache_retention,
            _ => None,
        },
        provider_config,
    };

    // Make API call
    let response = client.get_generation(&messages, &config).await?;

    if response.response.output.is_empty() {
        return Err(Error::Inference(
            "Provider returned an empty assistant response".to_string(),
        ));
    }

    Ok(CallResult {
        response: response.response,
    })
}

/// Execute a streaming LLM call
#[allow(clippy::too_many_arguments)]
pub async fn call_llm_stream(
    prompt: Option<&str>,
    model: &str,
    tools: Arc<RwLock<HashMap<String, ToolFunction>>>,
    tool_order: Arc<RwLock<Vec<String>>>,
    tool_schema_info: Arc<RwLock<HashMap<String, ToolSchemaInfo>>>,
    output_type_name: Option<String>,
    output_schema: Option<serde_json::Value>,
    api_key: Option<&str>,
    system: Option<&str>,
    system_messages: Option<Vec<ChatMessage>>,
    history: Option<Vec<ConversationMessage>>,
    temperature: Option<f32>,
    top_p: Option<f32>,
    max_tokens: Option<u32>,
    timeout: Option<std::time::Duration>,
    retry_config: Option<crate::retry::RetryConfig>,
    provider_config: Option<ProviderConfig>,
    _call_context: Arc<RwLock<Option<HashMap<String, serde_json::Value>>>>,
    accumulators: Arc<RwLock<Accumulators>>,
) -> Result<Pin<Box<dyn Stream<Item = Result<ResponseStreamEvent>> + Send>>> {
    // Create client
    let client = create_inference_client_with_config(model, api_key, provider_config.as_ref())?;
    let parsed_model = parse_model_string(model)?;
    let provider_key = resolve_provider_key(model);
    let message_provider = resolve_provider_for_caching(model);

    let messages =
        build_runtime_messages(prompt, system, system_messages, history, message_provider);

    if messages.is_empty() {
        return Err(Error::NonRetryable(
            "No messages to send to LLM".to_string(),
        ));
    }

    // Generate tool schemas (always enabled - native tools only)
    let tool_schemas = {
        let tools_guard = tools.read().await;
        let schemas_guard = tool_schema_info.read().await;
        let order_guard = tool_order.read().await;
        if !tools_guard.is_empty() {
            Some(generate_tool_schemas(
                &tools_guard,
                &schemas_guard,
                &order_guard,
                model,
            )?)
        } else {
            None
        }
    };

    // Fix output schema for provider-specific requirements
    let fixed_output_schema = output_schema.map(|mut schema| {
        fix_output_schema_for_provider(&mut schema, &provider_key);
        schema
    });

    // Build config
    let config = GenerationConfig {
        model: String::new(),
        max_tokens,
        temperature,
        top_p,
        tools: tool_schemas,
        native_tools: true, // Always true - we only support native tools
        reasoning_effort: None,
        thinking_budget: None,
        output_schema: fixed_output_schema,
        output_type_name,
        timeout,
        retry_config,
        prompt_cache_retention: match provider_key.as_str() {
            "openai" | "openai-responses" => parsed_model.prompt_cache_retention,
            _ => None,
        },
        provider_config,
    };

    // Get streaming response
    let stream = client.connect_and_listen(&messages, &config).await?;

    let response = Arc::new(RwLock::new(AssistantResponse::default()));
    let stream_response = response.clone();

    let transformed = stream
        .then(move |chunk_result| {
            let accumulators = accumulators.clone();
            let response = stream_response.clone();
            async move {
                match chunk_result {
                    Ok(chunk) => {
                        match stream_chunk_to_runtime_events(chunk, accumulators, response).await {
                            Ok(events) => events.into_iter().map(Ok).collect(),
                            Err(e) => vec![Err(e)],
                        }
                    }
                    Err(e) => vec![Err(e)],
                }
            }
        })
        .flat_map(futures::stream::iter);

    let final_response = response.clone();
    let completed = futures::stream::once(async move {
        let response = final_response.read().await.clone();
        if response.output.is_empty() {
            Err(Error::Inference(
                "Provider returned an empty assistant response".to_string(),
            ))
        } else {
            Ok(ResponseStreamEvent::Complete(response))
        }
    });

    Ok(Box::pin(transformed.chain(completed)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ToolParametersSchema;
    use crate::types::{CacheMarker, ChatRole};

    /// Alphabetical order helper for tests that don't care about registration
    /// order — mirrors the old sorted serialization those tests were written
    /// against.
    fn sorted_order(tools: &HashMap<String, ToolFunction>) -> Vec<String> {
        let mut names: Vec<String> = tools.keys().cloned().collect();
        names.sort();
        names
    }

    fn set_test_env_var(key: &str, value: &str) {
        // SAFETY: These tests only set deterministic fake API keys used by
        // client-construction checks. They do not read or mutate process
        // environment concurrently inside these test bodies.
        unsafe {
            std::env::set_var(key, value);
        }
    }

    #[test]
    fn test_create_inference_client_anthropic() {
        set_test_env_var("ANTHROPIC_API_KEY", "test-key");
        let result = create_inference_client("anthropic:claude-3-5-sonnet-20241022", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_inference_client_with_reasoning() {
        set_test_env_var("ANTHROPIC_API_KEY", "test-key");
        let result =
            create_inference_client("anthropic:claude-3-5-sonnet-20241022@reasoning=1024", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_inference_client_invalid_format() {
        let result = create_inference_client("invalid-format", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_inference_client_openai() {
        set_test_env_var("OPENAI_API_KEY", "test-key");
        let result = create_inference_client("openai:gpt-4o", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_inference_client_openai_responses() {
        set_test_env_var("OPENAI_API_KEY", "test-key");
        let result = create_inference_client("openai:resp:gpt-4o", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_inference_client_accepts_google_catalog_alias() {
        let result = create_inference_client("google:gemini-2.5-flash", Some("test-key"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_model_string_with_openai_cache_retention() {
        let parsed = parse_model_string("openai:gpt-5.1@cache=24h").unwrap();
        assert_eq!(parsed.provider, "openai");
        assert_eq!(parsed.model_name, "gpt-5.1");
        assert_eq!(
            parsed.prompt_cache_retention,
            Some(PromptCacheRetention::H24)
        );
    }

    #[test]
    fn test_parse_model_string_strips_capability_overrides_from_provider_model() {
        let parsed =
            parse_model_string("openrouter:qwen/qwen2.5-vl-72b-instruct@vision=true").unwrap();
        assert_eq!(parsed.provider, "openrouter");
        assert_eq!(parsed.model_name, "qwen/qwen2.5-vl-72b-instruct");

        let parsed = parse_model_string("openai:resp:gpt-4o@vision=false").unwrap();
        assert_eq!(parsed.provider, "openai-responses");
        assert_eq!(parsed.model_name, "gpt-4o");
    }

    #[test]
    fn test_parse_model_string_rejects_invalid_cache_retention() {
        let result = parse_model_string("openai:gpt-5.1@cache=forever");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_stream_content_updates_raw_response_accumulator() {
        let accumulators = Arc::new(RwLock::new(Accumulators::default()));
        let response = Arc::new(RwLock::new(AssistantResponse::default()));
        let events = stream_chunk_to_runtime_events(
            StreamChunk::Content("hello".to_string()),
            accumulators.clone(),
            response.clone(),
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ResponseStreamEvent::Output(ResponsePart::Text { text }) if text == "hello"
        ));
        assert_eq!(
            accumulators.read().await.raw_response,
            vec!["hello".to_string()]
        );
        assert_eq!(response.read().await.text(), "hello");
    }

    #[test]
    fn test_create_inference_client_openrouter() {
        set_test_env_var("OPENROUTER_API_KEY", "test-key");
        let result = create_inference_client("openrouter:openai/gpt-4o", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_inference_client_openrouter_responses() {
        set_test_env_var("OPENROUTER_API_KEY", "test-key");
        let result = create_inference_client("openrouter:resp:openai/o4-mini", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_runtime_messages_preserves_system_array_for_anthropic_family() {
        let messages = build_runtime_messages(
            Some("Prompt"),
            None,
            Some(vec![
                ChatMessage::system("Runtime contract").with_cache_marker(CacheMarker::Ephemeral),
                ChatMessage::system("<soul>Identity</soul>")
                    .with_cache_marker(CacheMarker::Ephemeral),
            ]),
            None,
            Some(Provider::Anthropic),
        );

        assert_eq!(messages.len(), 3);
        assert!(matches!(
            &messages[0],
            ConversationMessage::Chat(ChatMessage {
                role: ChatRole::System,
                content,
                cache_marker: Some(CacheMarker::Ephemeral),
                ..
            }) if content == "Runtime contract"
        ));
        assert!(matches!(
            &messages[1],
            ConversationMessage::Chat(ChatMessage {
                role: ChatRole::System,
                content,
                cache_marker: Some(CacheMarker::Ephemeral),
                ..
            }) if content == "<soul>Identity</soul>"
        ));
    }

    #[test]
    fn test_build_runtime_messages_collapses_system_array_for_string_only_providers() {
        let messages = build_runtime_messages(
            Some("Prompt"),
            None,
            Some(vec![
                ChatMessage::system("Runtime contract").with_cache_marker(CacheMarker::Ephemeral),
                ChatMessage::system("<soul>Identity</soul>")
                    .with_cache_marker(CacheMarker::Ephemeral),
            ]),
            None,
            Some(Provider::Bedrock),
        );

        assert_eq!(messages.len(), 2);
        assert!(matches!(
            &messages[0],
            ConversationMessage::Chat(ChatMessage {
                role: ChatRole::System,
                content,
                cache_marker: None,
                ..
            }) if content == "Runtime contract\n\n<soul>Identity</soul>"
        ));
    }

    #[test]
    fn test_build_runtime_messages_preserves_plain_system_when_structured_system_is_empty() {
        let messages = build_runtime_messages(
            Some("Prompt"),
            Some("Plain system"),
            Some(Vec::new()),
            None,
            Some(Provider::Anthropic),
        );

        assert_eq!(messages.len(), 2);
        assert!(matches!(
            &messages[0],
            ConversationMessage::Chat(ChatMessage {
                role: ChatRole::System,
                content,
                cache_marker: None,
                ..
            }) if content == "Plain system"
        ));
        assert!(matches!(
            &messages[1],
            ConversationMessage::Chat(ChatMessage {
                role: ChatRole::User,
                content,
                cache_marker: None,
                ..
            }) if content == "Prompt"
        ));
    }

    #[test]
    fn test_build_runtime_messages_does_not_invent_prompt_cache_markers() {
        let messages = build_runtime_messages(
            Some("Prompt"),
            None,
            None,
            Some(vec![ConversationMessage::Chat(ChatMessage::user(
                "Prior user message",
            ))]),
            Some(Provider::Anthropic),
        );

        assert!(matches!(
            &messages[0],
            ConversationMessage::Chat(ChatMessage {
                role: ChatRole::User,
                content,
                cache_marker: None,
                ..
            }) if content == "Prior user message"
        ));
        assert!(matches!(
            &messages[1],
            ConversationMessage::Chat(ChatMessage {
                role: ChatRole::User,
                content,
                cache_marker: None,
                ..
            }) if content == "Prompt"
        ));
    }

    #[test]
    fn test_generate_tool_schemas_empty() {
        let tools = HashMap::new();
        let tool_schemas = HashMap::new();
        let result = generate_tool_schemas(
            &tools,
            &tool_schemas,
            &sorted_order(&tools),
            "anthropic:claude-3",
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_generate_tool_schemas_anthropic() {
        let mut tools = HashMap::new();
        tools.insert(
            "get_weather".to_string(),
            ToolFunction::Sync(Box::new(|_args| Ok("sunny".to_string()))),
        );

        let mut tool_schemas = HashMap::new();
        tool_schemas.insert(
            "get_weather".to_string(),
            ToolSchemaInfo {
                name: "get_weather".to_string(),
                description: "Get the weather for a location".to_string(),
                fields: vec![crate::parsers::FieldDescription {
                    name: "location".to_string(),
                    field_type: "string".to_string(),
                    description: "The city name".to_string(),
                    required: true,
                }],
                strict: None,
                parameters: ToolParametersSchema::from_json_schema(&serde_json::json!({
                    "type": "object",
                    "properties": {
                        "location": {
                            "type": "string",
                            "description": "The city name"
                        }
                    },
                    "required": ["location"]
                }))
                .unwrap(),
            },
        );

        let result = generate_tool_schemas(
            &tools,
            &tool_schemas,
            &sorted_order(&tools),
            "anthropic:claude-3",
        );
        assert!(result.is_ok());

        let schemas = result.unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0]["name"], "get_weather");
        assert!(schemas[0]["input_schema"].is_object());
        assert!(
            schemas[0]["input_schema"]
                .get("additionalProperties")
                .is_none()
        );
        assert_eq!(
            schemas[0]["input_schema"]["properties"]["location"]["type"],
            "string"
        );
    }

    #[test]
    fn test_generate_tool_schemas_openai() {
        let mut tools = HashMap::new();
        tools.insert(
            "calculate".to_string(),
            ToolFunction::Sync(Box::new(|_args| Ok("42".to_string()))),
        );

        let mut tool_schemas = HashMap::new();
        tool_schemas.insert(
            "calculate".to_string(),
            ToolSchemaInfo {
                name: "calculate".to_string(),
                description: "Calculate a math expression".to_string(),
                fields: vec![crate::parsers::FieldDescription {
                    name: "expression".to_string(),
                    field_type: "string".to_string(),
                    description: "Math expression to evaluate".to_string(),
                    required: true,
                }],
                strict: None,
                parameters: ToolParametersSchema::from_json_schema(&serde_json::json!({
                    "type": "object",
                    "properties": {
                        "expression": {
                            "type": "string",
                            "description": "Math expression to evaluate"
                        }
                    },
                    "required": ["expression"]
                }))
                .unwrap(),
            },
        );

        let result = generate_tool_schemas(
            &tools,
            &tool_schemas,
            &sorted_order(&tools),
            "openai:gpt-4o",
        );
        assert!(result.is_ok());

        let schemas = result.unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0]["type"], "function");
        assert_eq!(schemas[0]["function"]["name"], "calculate");
        assert!(
            schemas[0]["function"]["parameters"]
                .get("additionalProperties")
                .is_none()
        );
        assert_eq!(
            schemas[0]["function"]["parameters"]["properties"]["expression"]["type"],
            "string"
        );
    }

    #[test]
    fn test_generate_tool_schemas_uses_registration_order() {
        let mut tools = HashMap::new();
        for name in ["zeta", "alpha", "beta"] {
            tools.insert(
                name.to_string(),
                ToolFunction::Sync(Box::new(|_args| Ok("x".to_string()))),
            );
        }
        // Registration order intentionally non-alphabetical: serialization must
        // follow it so mid-session appends extend (not resort) the cached
        // prompt prefix.
        let order = vec!["zeta".to_string(), "alpha".to_string(), "beta".to_string()];

        let schemas = generate_tool_schemas(&tools, &HashMap::new(), &order, "anthropic:claude-3")
            .expect("schemas");
        let names: Vec<&str> = schemas
            .iter()
            .filter_map(|schema| schema["name"].as_str())
            .collect();

        assert_eq!(names, vec!["zeta", "alpha", "beta"]);
    }

    #[test]
    fn test_generate_tool_schemas_appends_unordered_names_deterministically() {
        let mut tools = HashMap::new();
        for name in ["zeta", "alpha", "mu"] {
            tools.insert(
                name.to_string(),
                ToolFunction::Sync(Box::new(|_args| Ok("x".to_string()))),
            );
        }
        // "mu" and "alpha" missing from the order record: they must still be
        // emitted, after the ordered names, in sorted order.
        let order = vec!["zeta".to_string()];

        let schemas = generate_tool_schemas(&tools, &HashMap::new(), &order, "anthropic:claude-3")
            .expect("schemas");
        let names: Vec<&str> = schemas
            .iter()
            .filter_map(|schema| schema["name"].as_str())
            .collect();

        assert_eq!(names, vec!["zeta", "alpha", "mu"]);
    }

    #[test]
    fn test_generate_tool_schemas_with_schema_info() {
        let mut tools = HashMap::new();
        tools.insert(
            "search".to_string(),
            ToolFunction::Sync(Box::new(|_args| Ok("results".to_string()))),
        );

        let mut tool_schemas = HashMap::new();
        tool_schemas.insert(
            "search".to_string(),
            ToolSchemaInfo {
                name: "search".to_string(),
                description: "Search for documents".to_string(),
                fields: vec![
                    crate::parsers::FieldDescription {
                        name: "query".to_string(),
                        field_type: "string".to_string(),
                        description: "Search query".to_string(),
                        required: true,
                    },
                    crate::parsers::FieldDescription {
                        name: "limit".to_string(),
                        field_type: "integer".to_string(),
                        description: "Max results".to_string(),
                        required: false,
                    },
                    crate::parsers::FieldDescription {
                        name: "ids".to_string(),
                        field_type: "Vec<i64>".to_string(),
                        description: "Optional document ids".to_string(),
                        required: false,
                    },
                ],
                strict: None,
                parameters: ToolParametersSchema::from_json_schema(&serde_json::json!({
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max results"
                        },
                        "ids": {
                            "type": "array",
                            "description": "Optional document ids",
                            "items": {
                                "type": "integer"
                            }
                        }
                    },
                    "required": ["query"]
                }))
                .unwrap(),
            },
        );

        let result = generate_tool_schemas(
            &tools,
            &tool_schemas,
            &sorted_order(&tools),
            "anthropic:claude-3",
        );
        assert!(result.is_ok());

        let schemas = result.unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0]["name"], "search");
        assert!(
            schemas[0]["input_schema"]
                .get("additionalProperties")
                .is_none()
        );
        assert_eq!(
            schemas[0]["input_schema"]["properties"]["ids"]["items"]["type"],
            "integer"
        );
    }

    #[test]
    fn test_generate_tool_schemas_unsupported_provider() {
        let mut tools = HashMap::new();
        tools.insert(
            "test".to_string(),
            ToolFunction::Sync(Box::new(|_args| Ok("ok".to_string()))),
        );
        let tool_schemas = HashMap::new();

        let result = generate_tool_schemas(
            &tools,
            &tool_schemas,
            &sorted_order(&tools),
            "unsupported:model",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_tool_schemas_preserves_nested_objects_and_google_constraints() {
        let mut tools = HashMap::new();
        tools.insert(
            "write_thread".to_string(),
            ToolFunction::Sync(Box::new(|_args| Ok("ok".to_string()))),
        );

        let mut tool_schemas = HashMap::new();
        tool_schemas.insert(
            "write_thread".to_string(),
            ToolSchemaInfo {
                name: "write_thread".to_string(),
                description: "Write a thread".to_string(),
                fields: vec![],
                strict: None,
                parameters: ToolParametersSchema::from_json_schema(&serde_json::json!({
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {
                                "$ref": "#/$defs/ThreadItem"
                            }
                        }
                    },
                    "required": ["items"],
                    "$defs": {
                        "ThreadItem": {
                            "type": "object",
                            "properties": {
                                "text": { "type": "string" },
                                "image_ids": {
                                    "type": "array",
                                    "items": { "type": "integer" }
                                }
                            },
                            "required": ["text"],
                            "additionalProperties": false
                        }
                    }
                }))
                .unwrap(),
            },
        );

        let google = generate_tool_schemas(
            &tools,
            &tool_schemas,
            &sorted_order(&tools),
            "gemini:gemini-3-flash-preview",
        )
        .unwrap();
        let nested = &google[0]["parameters"]["properties"]["items"]["items"]["properties"];
        assert_eq!(nested["text"]["type"], "string");
        assert_eq!(nested["image_ids"]["items"]["type"], "integer");
        assert!(
            google[0]["parameters"]["properties"]["items"]["items"]
                .get("additionalProperties")
                .is_none()
        );
    }
}
