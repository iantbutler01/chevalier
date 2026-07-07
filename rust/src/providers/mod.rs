//! LLM provider clients
//!
//! This module contains the InferenceClient trait and implementations
//! for various LLM providers (Anthropic, OpenAI, Google, etc.).

use async_trait::async_trait;
use futures::stream::Stream;
use std::pin::Pin;

use crate::error::Result;
use crate::utils::ConversationMessage;

// Re-export types for convenience
pub use crate::types::{
    AssistantResponse, CacheMarker, CostInfo, Provider, ProviderRateLimit, ProviderRateLimitScope,
    TokenUsage,
};

#[derive(Debug, Clone)]
pub struct AnthropicProviderConfig {
    pub automatic_prompt_caching: Option<CacheMarker>,
    pub tool_definitions_cache_breakpoint: Option<CacheMarker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexSubscriptionTransport {
    Auto,
    WebSocket,
    Sse,
}

#[derive(Debug, Clone)]
pub struct CodexSubscriptionProviderConfig {
    pub token: String,
    pub account_id: Option<String>,
    pub base_url: Option<String>,
    pub transport: Option<CodexSubscriptionTransport>,
    pub sse_header_timeout: Option<std::time::Duration>,
    pub websocket_connect_timeout: Option<std::time::Duration>,
    pub reasoning_effort: Option<String>,
    pub reasoning_summary: Option<String>,
    pub text_verbosity: Option<String>,
    pub service_tier: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ProviderConfig {
    Anthropic(AnthropicProviderConfig),
    // Boxed: this variant is ~208 bytes vs ~2 for Anthropic (clippy large_enum_variant);
    // boxing keeps every ProviderConfig move/clone small.
    CodexSubscription(Box<CodexSubscriptionProviderConfig>),
}

// Provider implementations
pub mod anthropic;
pub(crate) mod anthropic_streaming;
pub mod bedrock;
pub mod google;
#[cfg(feature = "google-adc")]
pub mod google_anthropic;
pub mod openai;
pub mod openai_codex_responses;
pub mod openai_responses;
pub(crate) mod openai_responses_streaming;
pub(crate) mod openai_streaming;
pub mod openrouter;
pub mod openrouter_responses;
pub mod tracing_client;

pub use anthropic::AnthropicClient;
#[cfg(feature = "bedrock")]
pub use bedrock::BedrockClient;
pub use google::{FileState, GoogleGenAIClient, UploadedFile};
#[cfg(feature = "google-adc")]
pub use google_anthropic::GoogleAnthropicClient;
pub use openai::OAIClient;
pub use openai_codex_responses::OpenAICodexResponsesClient;
pub use openai_responses::OpenAIResponsesClient;
pub use openrouter::OpenRouterClient;
pub use openrouter_responses::OpenRouterResponsesClient;
pub use tracing_client::TracingInferenceClient;

/// Configuration for generation requests
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptCacheRetention {
    InMemory,
    H24,
}

impl PromptCacheRetention {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InMemory => "in_memory",
            Self::H24 => "24h",
        }
    }
}

/// Configuration for generation requests
#[derive(Debug, Clone)]
pub struct GenerationConfig {
    /// Model name (provider-specific)
    pub model: String,

    /// Maximum tokens to generate
    pub max_tokens: Option<u32>,

    /// Temperature (0.0-1.0)
    pub temperature: Option<f32>,

    /// Top-p sampling
    pub top_p: Option<f32>,

    /// Tool schemas (provider-specific format)
    pub tools: Option<Vec<serde_json::Value>>,

    /// Whether to use native tool calling
    pub native_tools: bool,

    /// Reasoning effort
    pub reasoning_effort: Option<String>,

    /// Thinking budget tokens (for Claude extended thinking)
    pub thinking_budget: Option<u32>,

    /// Output schema for structured outputs (JSON schema)
    pub output_schema: Option<serde_json::Value>,

    /// Name for the output type (used in schema wrapper)
    pub output_type_name: Option<String>,

    /// Request timeout (overrides provider default)
    pub timeout: Option<std::time::Duration>,

    /// Retry policy for this request. When `None`, providers fall back to
    /// `RetryConfig::default()` (60s max_time, 3 retries) — the library-level
    /// baseline. LLM-heavy callers should inject their own policy here so
    /// long-running generations don't get killed by the conservative default.
    pub retry_config: Option<crate::retry::RetryConfig>,

    /// Prompt cache retention policy for providers that support request-level
    /// cache controls (currently OpenAI direct APIs).
    pub prompt_cache_retention: Option<PromptCacheRetention>,

    /// Provider-specific request-shaping options.
    pub provider_config: Option<ProviderConfig>,
}

impl Default for GenerationConfig {
    fn default() -> Self {
        Self {
            model: String::new(),
            max_tokens: Some(4096),
            temperature: None,
            top_p: None,
            tools: None,
            native_tools: false,
            reasoning_effort: None,
            thinking_budget: None,
            output_schema: None,
            output_type_name: None,
            timeout: None,
            retry_config: None,
            prompt_cache_retention: None,
            provider_config: None,
        }
    }
}

impl GenerationConfig {
    /// Create a new configuration with required model
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            ..Default::default()
        }
    }

    /// Return the explicit request model, or the client default when unset.
    pub fn effective_model<'a>(&'a self, client_model: &'a str) -> &'a str {
        if self.model.is_empty() {
            client_model
        } else {
            &self.model
        }
    }

    /// Set max tokens
    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }

    /// Set temperature
    pub fn with_temperature(mut self, temperature: f32) -> Self {
        self.temperature = Some(temperature);
        self
    }

    /// Set tools
    pub fn with_tools(mut self, tools: Vec<serde_json::Value>) -> Self {
        self.tools = Some(tools);
        self
    }

    /// Enable native tools
    pub fn with_native_tools(mut self, enabled: bool) -> Self {
        self.native_tools = enabled;
        self
    }

    /// Set reasoning effort (for o-series models)
    pub fn with_reasoning_effort(mut self, effort: impl Into<String>) -> Self {
        self.reasoning_effort = Some(effort.into());
        self
    }

    /// Set thinking budget (for Claude extended thinking)
    pub fn with_thinking_budget(mut self, budget: u32) -> Self {
        self.thinking_budget = Some(budget);
        self
    }

    /// Set output schema for structured outputs
    pub fn with_output_schema(
        mut self,
        schema: serde_json::Value,
        type_name: impl Into<String>,
    ) -> Self {
        self.output_schema = Some(schema);
        self.output_type_name = Some(type_name.into());
        self
    }

    /// Set request timeout
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set retry policy
    pub fn with_retry_config(mut self, retry_config: crate::retry::RetryConfig) -> Self {
        self.retry_config = Some(retry_config);
        self
    }

    /// Set prompt cache retention policy
    pub fn with_prompt_cache_retention(mut self, retention: PromptCacheRetention) -> Self {
        self.prompt_cache_retention = Some(retention);
        self
    }

    /// Set provider-specific request options
    pub fn with_provider_config(mut self, config: ProviderConfig) -> Self {
        self.provider_config = Some(config);
        self
    }
}

/// Response from a generation request
#[derive(Debug, Clone)]
pub struct GenerationResponse {
    /// Canonical assistant response.
    pub response: AssistantResponse,

    /// Concatenated text output for compatibility.
    pub content: String,

    /// Concatenated reasoning output for compatibility.
    pub reasoning: Option<String>,

    /// Tool calls in provider-preserving JSON form for compatibility.
    pub tool_calls: Vec<serde_json::Value>,

    /// Token usage statistics
    pub usage: TokenUsage,

    /// Provider-reported cost in dollars (e.g., from OpenRouter's usage.cost field)
    /// When present, this is the actual cost charged by the provider.
    pub provider_cost_dollars: Option<f64>,

    /// Raw response (for debugging/tool extraction)
    pub raw: Option<serde_json::Value>,
}

impl GenerationResponse {
    /// Create a simple text response
    pub fn text(content: impl Into<String>) -> Self {
        let response = AssistantResponse::from_text(content.into());
        Self {
            content: response.text(),
            reasoning: None,
            tool_calls: Vec::new(),
            response,
            usage: TokenUsage::default(),
            provider_cost_dollars: None,
            raw: None,
        }
    }

    /// Build a generation response from the canonical assistant response.
    pub fn from_assistant_response(
        response: AssistantResponse,
        usage: TokenUsage,
        provider_cost_dollars: Option<f64>,
        raw: Option<serde_json::Value>,
    ) -> Self {
        let content = response.text();
        let reasoning = {
            let reasoning = response.reasoning();
            if reasoning.is_empty() {
                None
            } else {
                Some(reasoning)
            }
        };
        let tool_calls = response
            .tool_calls()
            .into_iter()
            .map(normalized_tool_call_json)
            .collect();

        Self {
            response,
            content,
            reasoning,
            tool_calls,
            usage,
            provider_cost_dollars,
            raw,
        }
    }

    /// Check if response contains tool calls
    pub fn has_tool_calls(&self) -> bool {
        self.response.has_tool_calls() || !self.tool_calls.is_empty()
    }

    /// Set provider-reported cost
    pub fn with_provider_cost(mut self, cost_dollars: f64) -> Self {
        self.provider_cost_dollars = Some(cost_dollars);
        self
    }
}

fn normalized_tool_call_json(call: &crate::types::ToolCall) -> serde_json::Value {
    let args_for_function = call.raw_arguments.as_ref().map_or_else(
        || call.args.clone(),
        |raw| serde_json::Value::String(raw.clone()),
    );

    let mut value = serde_json::json!({
        "id": call.tool_use_id,
        "_tool_use_id": call.tool_use_id,
        "_tool_name": call.tool_name,
        "name": call.tool_name,
        "input": call.args,
        "function": {
            "name": call.tool_name,
            "arguments": args_for_function
        }
    });

    if let Some(signature) = &call.signature {
        value["signature"] = serde_json::json!(signature);
    }

    if let (Some(obj), Some(map)) = (call.args.as_object(), value.as_object_mut()) {
        for (key, entry) in obj {
            map.insert(key.clone(), entry.clone());
        }
    }

    value
}

/// Stream chunk types
#[derive(Debug, Clone)]
pub enum StreamChunk {
    /// Reasoning/thinking content
    Reasoning(String),

    /// Provider signature for reasoning preservation
    Signature(String),

    /// Main content chunk
    Content(String),

    /// Partial tool call (incomplete JSON)
    ToolCallPartial(serde_json::Value),

    /// Complete tool call
    ToolCallComplete(serde_json::Value),

    /// Token usage statistics (final message)
    Usage {
        input_tokens: u64,
        output_tokens: u64,
        cached_tokens: u64,
        cache_write_input_tokens: u64,
    },

    /// Provider rate-limit window usage.
    RateLimits(Vec<ProviderRateLimit>),
}

/// Trace callback type for monitoring (wrapped in Arc for Clone support)
///
/// Called after each inference request with:
/// - request_id: Unique identifier for this request
/// - messages: The input messages (as JSON values)
/// - response: The generation response
/// - cost: Cost information including token counts and calculated cost
pub type TraceCallback = std::sync::Arc<
    dyn Fn(
            u64,
            Vec<serde_json::Value>,
            &GenerationResponse,
            &CostInfo,
        ) -> Pin<Box<dyn futures::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// Trait for cost storage backends
///
/// Implementations can store costs in memory, databases, or external services.
#[async_trait]
pub trait CostStore: Send + Sync {
    /// Record cost from an inference request
    async fn record_cost(&self, model: &str, cost: &CostInfo) -> Result<()>;

    /// Get total credits used in microdollars
    async fn get_credits_used(&self) -> Result<u64>;
}

/// In-memory cost store using atomic operations
///
/// Stores costs in microdollars ($1 = 1,000,000) for billing-level precision.
pub struct MemoryCostStore {
    credits_used: std::sync::atomic::AtomicU64,
}

impl MemoryCostStore {
    /// Create a new memory cost store
    pub fn new() -> Self {
        Self {
            credits_used: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get current credits used (non-async for convenience)
    pub fn credits(&self) -> u64 {
        self.credits_used.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for MemoryCostStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CostStore for MemoryCostStore {
    async fn record_cost(&self, _model: &str, cost: &CostInfo) -> Result<()> {
        self.credits_used.fetch_add(
            cost.total_microdollars(),
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(())
    }

    async fn get_credits_used(&self) -> Result<u64> {
        Ok(self.credits())
    }
}

/// Core trait for LLM provider clients
#[async_trait]
pub trait InferenceClient: Send + Sync {
    /// Get a single generation (non-streaming)
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse>;

    /// Connect and listen for streaming responses
    async fn connect_and_listen(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>>;

    /// Get provider type
    fn provider(&self) -> Provider;

    /// Check if native tools are supported
    fn supports_native_tools(&self) -> bool {
        self.provider().supports_native_tools()
    }

    /// Set trace callback (optional)
    fn set_trace_callback(&mut self, _callback: TraceCallback) {
        // Default: no-op
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generation_config_builder() {
        let config = GenerationConfig::new("gpt-4")
            .with_max_tokens(2048)
            .with_temperature(0.5)
            .with_native_tools(true);

        assert_eq!(config.model, "gpt-4");
        assert_eq!(config.max_tokens, Some(2048));
        assert_eq!(config.temperature, Some(0.5));
        assert!(config.native_tools);
    }

    #[test]
    fn test_generation_config_effective_model() {
        assert_eq!(
            GenerationConfig::default().effective_model("client-model"),
            "client-model"
        );
        assert_eq!(
            GenerationConfig::new("request-model").effective_model("client-model"),
            "request-model"
        );
    }

    #[test]
    fn test_generation_response_text() {
        let response = GenerationResponse::text("Hello, world!");
        assert_eq!(response.response.text(), "Hello, world!");
        assert!(!response.has_tool_calls());
    }

    #[test]
    fn test_generation_response_with_tool_calls() {
        let mut response = GenerationResponse::text("Using tools...");
        response
            .response
            .push_output(crate::types::ResponsePart::Tool {
                call: crate::types::ToolCall::new("get_weather", serde_json::json!({"city": "SF"})),
            });
        assert!(response.has_tool_calls());
    }
}
