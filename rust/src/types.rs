//! Core types for Chevalier
//!
//! This module contains the fundamental data structures used throughout
//! the framework: messages, tool calls, tool results, and reasoning segments.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};

/// Format JSON to match Python's json.dumps default style (space after colon and comma)
fn format_json_python_style(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let pairs: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("\"{}\": {}", k, format_json_python_style(v)))
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_json_python_style).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::String(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
    }
}

/// LLM Provider enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Provider {
    /// Anthropic Claude (direct API)
    Anthropic,
    /// OpenAI GPT models (direct API)
    OpenAI,
    /// OpenAI Responses API
    OpenAIResponses,
    /// AWS Bedrock (Claude and others)
    Bedrock,
    /// Google Gemini via GenAI SDK
    GoogleGenAI,
    /// Anthropic Claude via Google Vertex AI
    GoogleAnthropic,
    /// OpenRouter proxy service
    OpenRouter,
    /// OpenRouter Responses API
    OpenRouterResponses,
}

impl Provider {
    /// Parse provider from model string (e.g., "anthropic:claude-3-opus")
    pub fn from_model_string(model: &str) -> Result<(Self, String)> {
        let parts: Vec<&str> = model.split(':').collect();
        if parts.len() < 2 {
            return Err(Error::InvalidProvider(format!(
                "Invalid model string format: {}. Expected 'provider:model'",
                model
            )));
        }

        let (provider_str, model_name) = if parts.len() >= 3 && parts[1] == "resp" {
            let provider = match parts[0] {
                "openai" => Provider::OpenAIResponses,
                "openrouter" => Provider::OpenRouterResponses,
                "openai-codex-responses" => Provider::OpenAIResponses,
                _ => return Err(Error::InvalidProvider(parts[0].to_string())),
            };
            return Ok((provider, parts[2..].join(":")));
        } else {
            (parts[0], parts[1..].join(":"))
        };

        let provider = match provider_str {
            "anthropic" => Provider::Anthropic,
            "openai" => Provider::OpenAI,
            "openai-codex-responses" => Provider::OpenAIResponses,
            "bedrock" => Provider::Bedrock,
            "google-genai" | "gemini" | "google-gemini" => Provider::GoogleGenAI,
            "google-anthropic" | "vertexai" => Provider::GoogleAnthropic,
            "openrouter" => Provider::OpenRouter,
            _ => return Err(Error::InvalidProvider(provider_str.to_string())),
        };
        Ok((provider, model_name))
    }

    /// Check if this provider supports native tool calling
    pub fn supports_native_tools(&self) -> bool {
        matches!(
            self,
            Provider::Anthropic
                | Provider::OpenAI
                | Provider::OpenAIResponses
                | Provider::Bedrock
                | Provider::GoogleGenAI
                | Provider::GoogleAnthropic
                | Provider::OpenRouter
                | Provider::OpenRouterResponses
        )
    }

    /// Return model-level provider capabilities using conservative allowlists.
    pub fn capabilities_for_model(&self, model: &str) -> ProviderCapabilities {
        let image_input = self.supports_image_input(model);
        ProviderCapabilities {
            image_input,
            image_input_sources: if image_input {
                match self {
                    Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic => {
                        vec![MediaSourceKind::Base64]
                    }
                    Provider::OpenAI | Provider::OpenRouter => {
                        vec![MediaSourceKind::Base64, MediaSourceKind::Url]
                    }
                    Provider::GoogleGenAI => {
                        vec![MediaSourceKind::Base64, MediaSourceKind::FileUri]
                    }
                    Provider::OpenAIResponses | Provider::OpenRouterResponses => {
                        vec![
                            MediaSourceKind::Base64,
                            MediaSourceKind::Url,
                            MediaSourceKind::FileId,
                        ]
                    }
                }
            } else {
                Vec::new()
            },
        }
    }

    /// Check whether this provider/model pair can inspect image inputs.
    pub fn supports_image_input(&self, model: &str) -> bool {
        if let Some(image_input) = model_image_input_override(model) {
            return image_input;
        }

        let normalized = normalize_capability_model_name(model);

        match self {
            Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic => {
                is_claude_vision_model(&normalized)
            }
            Provider::GoogleGenAI => is_gemini_vision_model(&normalized),
            Provider::OpenAI | Provider::OpenAIResponses => is_openai_vision_model(&normalized),
            Provider::OpenRouter | Provider::OpenRouterResponses => {
                is_openrouter_vision_model(&normalized)
            }
        }
    }
}

fn model_image_input_override(model: &str) -> Option<bool> {
    model
        .split('@')
        .skip(1)
        .filter_map(|param| param.split_once('='))
        .find_map(|(key, value)| {
            if key != "vision" {
                return None;
            }

            match value.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" => Some(true),
                "0" | "false" | "no" => Some(false),
                _ => None,
            }
        })
}

/// Provider-normalized capability flags for a concrete model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    /// Whether the provider/model can accept image inputs.
    pub image_input: bool,
    /// Source kinds accepted for image input by this provider path.
    pub image_input_sources: Vec<MediaSourceKind>,
}

/// Media source families exposed by provider capability checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaSourceKind {
    /// Base64 inline payload.
    Base64,
    /// Public or provider-resolvable URL.
    Url,
    /// Provider file ID.
    FileId,
    /// Provider file URI.
    FileUri,
}

fn normalize_capability_model_name(model: &str) -> String {
    model
        .split('@')
        .next()
        .unwrap_or(model)
        .trim()
        .to_ascii_lowercase()
}

fn is_claude_vision_model(model: &str) -> bool {
    model.contains("claude-3")
        || model.contains("claude-4")
        || model.contains("claude-sonnet-4")
        || model.contains("claude-opus-4")
        || model.contains("claude-haiku-4")
}

fn is_gemini_vision_model(model: &str) -> bool {
    model.contains("gemini")
}

fn is_openai_vision_model(model: &str) -> bool {
    model.starts_with("gpt-4o")
        || model.starts_with("gpt-4.1")
        || model.starts_with("gpt-4.5")
        || model.starts_with("gpt-5")
        || model.starts_with("o3")
        || model.starts_with("o4")
}

fn is_openrouter_vision_model(model: &str) -> bool {
    let model = model.strip_prefix("openai/").unwrap_or(model);
    if is_openai_vision_model(model) {
        return true;
    }

    let model = model.strip_prefix("anthropic/").unwrap_or(model);
    if is_claude_vision_model(model) {
        return true;
    }

    let model = model.strip_prefix("google/").unwrap_or(model);
    is_gemini_vision_model(model)
}

/// Chat message role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatRole {
    /// System message (instructions)
    System,
    /// User message
    User,
    /// Assistant message
    Assistant,
    /// Tool result message
    #[serde(rename = "tool")]
    Tool,
}

/// Cache marker for Anthropic prompt caching
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheMarker {
    /// Ephemeral cache point
    Ephemeral,
    /// Ephemeral cache point with a 1 hour TTL
    Ephemeral1h,
}

impl CacheMarker {
    pub fn anthropic_cache_control(&self) -> serde_json::Value {
        match self {
            Self::Ephemeral => serde_json::json!({ "type": "ephemeral" }),
            Self::Ephemeral1h => serde_json::json!({ "type": "ephemeral", "ttl": "1h" }),
        }
    }
}

/// A chat message in the conversation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Message role
    pub role: ChatRole,

    /// Message content (text)
    pub content: String,

    /// Optional cache marker (for Anthropic caching)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_marker: Option<CacheMarker>,

    /// Optional model family filtering
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_families: Option<Vec<String>>,

    /// Provider-specific signature for reasoning preservation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

impl ChatMessage {
    /// Create a new system message
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: ChatRole::System,
            content: content.into(),
            cache_marker: None,
            model_families: None,
            signature: None,
        }
    }

    /// Create a new user message
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: ChatRole::User,
            content: content.into(),
            cache_marker: None,
            model_families: None,
            signature: None,
        }
    }

    /// Create a new assistant message
    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Assistant,
            content: content.into(),
            cache_marker: None,
            model_families: None,
            signature: None,
        }
    }

    /// Add a cache marker to this message
    pub fn with_cache_marker(mut self, marker: CacheMarker) -> Self {
        self.cache_marker = Some(marker);
        self
    }
}

/// Canonical assistant response payload.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssistantResponse {
    /// Ordered output emitted by the assistant.
    pub output: Vec<ResponsePart>,
}

impl AssistantResponse {
    /// Create a new response from ordered output items.
    pub fn new(output: Vec<ResponsePart>) -> Self {
        Self { output }
    }

    /// Create a text-only assistant response.
    pub fn from_text(text: impl Into<String>) -> Self {
        Self {
            output: vec![ResponsePart::Text { text: text.into() }],
        }
    }

    /// Return all text output concatenated in-order.
    pub fn text(&self) -> String {
        self.output
            .iter()
            .filter_map(|part| match part {
                ResponsePart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Return the first text segment when available.
    pub fn as_str(&self) -> Option<&str> {
        self.output.iter().find_map(|part| match part {
            ResponsePart::Text { text } => Some(text.as_str()),
            _ => None,
        })
    }

    /// Return all reasoning output concatenated in-order.
    pub fn reasoning(&self) -> String {
        self.output
            .iter()
            .filter_map(|part| match part {
                ResponsePart::Reasoning { text } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Return all tool calls in-order.
    pub fn tool_calls(&self) -> Vec<&ToolCall> {
        self.output
            .iter()
            .filter_map(|part| match part {
                ResponsePart::Tool { call } => Some(call),
                _ => None,
            })
            .collect()
    }

    /// Return all signatures in-order.
    pub fn signatures(&self) -> Vec<&str> {
        self.output
            .iter()
            .filter_map(|part| match part {
                ResponsePart::Signature { value } => Some(value.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Check whether the response contains tool calls.
    pub fn has_tool_calls(&self) -> bool {
        self.output
            .iter()
            .any(|part| matches!(part, ResponsePart::Tool { .. }))
    }

    /// Append ordered output, coalescing adjacent text-like chunks.
    pub fn push_output(&mut self, part: ResponsePart) {
        match (self.output.last_mut(), &part) {
            (Some(ResponsePart::Text { text: existing }), ResponsePart::Text { text }) => {
                existing.push_str(text)
            }
            (
                Some(ResponsePart::Reasoning { text: existing }),
                ResponsePart::Reasoning { text },
            ) => existing.push_str(text),
            (
                Some(ResponsePart::Signature { value: existing }),
                ResponsePart::Signature { value },
            ) => existing.push_str(value),
            _ => self.output.push(part),
        }
    }
}

impl std::fmt::Display for AssistantResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.text())
    }
}

/// Ordered assistant output item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponsePart {
    /// User-visible text.
    Text { text: String },
    /// Reasoning/thinking text.
    Reasoning { text: String },
    /// Tool call emitted by the assistant.
    Tool { call: ToolCall },
    /// Provider signature needed for replay fidelity.
    Signature { value: String },
}

/// Typed streaming event for progressively building an assistant response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseStreamEvent {
    /// Ordered output emitted during streaming.
    Output(ResponsePart),
    /// Partial tool-call payload before a complete tool call is available.
    ToolPartial(serde_json::Value),
    /// Final usage statistics for the streamed response.
    Usage(TokenUsage),
    /// Provider rate-limit window usage for the streamed response.
    RateLimits(Vec<ProviderRateLimit>),
    /// Final canonical assistant response.
    Complete(AssistantResponse),
}

/// Provider rate-limit scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderRateLimitScope {
    Session,
    Subscription,
}

impl ProviderRateLimitScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Subscription => "subscription",
        }
    }
}

/// Provider rate-limit window usage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderRateLimit {
    pub scope: ProviderRateLimitScope,
    pub used_percent: u32,
    pub window_minutes: u64,
    pub resets_at_epoch_sec: u64,
}

/// Token usage statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    /// Input tokens consumed
    pub input_tokens: u64,

    /// Output tokens generated
    pub output_tokens: u64,

    /// Cached tokens (for Anthropic caching)
    pub cached_tokens: u64,

    /// Cache write / cache creation tokens (for Anthropic caching)
    #[serde(default)]
    pub cache_write_input_tokens: u64,
}

impl TokenUsage {
    /// Create new token usage stats
    pub fn new(input_tokens: u64, output_tokens: u64, cached_tokens: u64) -> Self {
        Self {
            input_tokens,
            output_tokens,
            cached_tokens,
            cache_write_input_tokens: 0,
        }
    }

    /// Total tokens (input + output)
    pub fn total_tokens(&self) -> u64 {
        self.input_tokens + self.output_tokens
    }
}

/// Cost information for inference requests
///
/// Stores costs in microdollars ($1 = 1,000,000 microdollars) for billing-level precision.
/// This avoids floating-point rounding errors when accumulating many small costs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CostInfo {
    /// Input tokens consumed
    pub input_tokens: u64,

    /// Output tokens generated
    pub output_tokens: u64,

    /// Cache read tokens (prompt caching hits)
    pub cache_read_input_tokens: u64,

    /// Cache write tokens (prompt caching misses that were cached)
    pub cache_write_input_tokens: u64,

    /// Cost in microdollars ($1 = 1,000,000) - calculated from pricing tables
    pub microdollar_cost: u64,

    /// Provider-reported cost in microdollars (e.g., from OpenRouter)
    /// When present, this takes precedence over calculated cost
    pub provider_cost_microdollars: Option<u64>,

    /// Manual adjustment in microdollars (can be negative via wrapping)
    pub microdollar_adjust: u64,
}

impl CostInfo {
    /// Create CostInfo from TokenUsage and model name
    ///
    /// Calculates cost based on per-model pricing. Uses microdollars for precision.
    pub fn from_usage(usage: &TokenUsage, model: &str) -> Self {
        let mut info = Self {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cache_read_input_tokens: usage.cached_tokens,
            cache_write_input_tokens: usage.cache_write_input_tokens,
            microdollar_cost: 0,
            provider_cost_microdollars: None,
            microdollar_adjust: 0,
        };

        info.microdollar_cost = info.calculate_microdollar_cost(model);
        info
    }

    /// Create CostInfo with provider-reported cost (e.g., from OpenRouter)
    ///
    /// When the provider returns cost directly, we use that instead of calculating.
    /// The provider cost takes precedence in `total_microdollars()`.
    pub fn from_usage_with_provider_cost(
        usage: &TokenUsage,
        model: &str,
        provider_cost_dollars: f64,
    ) -> Self {
        let mut info = Self::from_usage(usage, model);
        // Convert dollars to microdollars
        info.provider_cost_microdollars = Some((provider_cost_dollars * 1_000_000.0) as u64);
        info
    }

    /// Set provider-reported cost in dollars
    pub fn with_provider_cost(mut self, cost_dollars: f64) -> Self {
        self.provider_cost_microdollars = Some((cost_dollars * 1_000_000.0) as u64);
        self
    }

    /// Calculate cost in microdollars based on model pricing
    ///
    /// Pricing per million tokens, converted to microdollars per token:
    /// - $X per 1M tokens = X microdollars per token
    fn calculate_microdollar_cost(&self, model: &str) -> u64 {
        let model_lower = model.to_lowercase();

        // Anthropic models
        if model_lower.contains("claude") || model_lower.contains("anthropic") {
            if model_lower.contains("haiku") {
                // Haiku: $0.80/1M input, $4/1M output, $0.08/1M cache read
                // In microdollars: 0.80 input, 4.0 output, 0.08 cache read
                let input_cost = (self.input_tokens as f64) * 0.80;
                let output_cost = (self.output_tokens as f64) * 4.0;
                let cache_cost = (self.cache_read_input_tokens as f64) * 0.08;
                (input_cost + output_cost + cache_cost).ceil() as u64
            } else if model_lower.contains("opus") {
                // Opus: $15/1M input, $75/1M output, $1.50/1M cache read
                let input_cost = (self.input_tokens as f64) * 15.0;
                let output_cost = (self.output_tokens as f64) * 75.0;
                let cache_cost = (self.cache_read_input_tokens as f64) * 1.50;
                (input_cost + output_cost + cache_cost).ceil() as u64
            } else {
                // Sonnet (default): $3/1M input, $15/1M output, $0.30/1M cache read
                let input_cost = (self.input_tokens as f64) * 3.0;
                let output_cost = (self.output_tokens as f64) * 15.0;
                let cache_cost = (self.cache_read_input_tokens as f64) * 0.30;
                (input_cost + output_cost + cache_cost).ceil() as u64
            }
        }
        // OpenAI models
        else if model_lower.contains("gpt-4o") || model_lower.contains("o4-mini") {
            if model_lower.contains("mini") {
                // GPT-4o-mini / o4-mini: $1.10/1M input, $4.40/1M output
                let input_cost = (self.input_tokens as f64) * 1.10;
                let output_cost = (self.output_tokens as f64) * 4.40;
                let cache_cost = (self.cache_read_input_tokens as f64) * 0.275;
                (input_cost + output_cost + cache_cost).ceil() as u64
            } else {
                // GPT-4o: $5/1M input, $15/1M output
                let input_cost = (self.input_tokens as f64) * 5.0;
                let output_cost = (self.output_tokens as f64) * 15.0;
                let cache_cost = (self.cache_read_input_tokens as f64) * 1.25;
                (input_cost + output_cost + cache_cost).ceil() as u64
            }
        } else if model_lower.contains("o3") {
            // o3: $10/1M input, $40/1M output
            let input_cost = (self.input_tokens as f64) * 10.0;
            let output_cost = (self.output_tokens as f64) * 40.0;
            let cache_cost = (self.cache_read_input_tokens as f64) * 2.5;
            (input_cost + output_cost + cache_cost).ceil() as u64
        } else if model_lower.contains("o1") {
            // o1: $15/1M input, $60/1M output
            let input_cost = (self.input_tokens as f64) * 15.0;
            let output_cost = (self.output_tokens as f64) * 60.0;
            (input_cost + output_cost).ceil() as u64
        }
        // Google models
        else if model_lower.contains("gemini") {
            if model_lower.contains("flash") {
                // Gemini Flash: $0.075/1M input, $0.30/1M output
                let input_cost = (self.input_tokens as f64) * 0.075;
                let output_cost = (self.output_tokens as f64) * 0.30;
                (input_cost + output_cost).ceil() as u64
            } else if model_lower.contains("pro") {
                // Gemini Pro: $1.25/1M input, $5/1M output
                let input_cost = (self.input_tokens as f64) * 1.25;
                let output_cost = (self.output_tokens as f64) * 5.0;
                (input_cost + output_cost).ceil() as u64
            } else {
                0
            }
        } else {
            // Unknown model - return 0 cost
            0
        }
    }

    /// Total cost in microdollars (including adjustments)
    ///
    /// Prefers provider-reported cost when available (e.g., from OpenRouter),
    /// falls back to calculated cost from pricing tables.
    pub fn total_microdollars(&self) -> u64 {
        let base_cost = self
            .provider_cost_microdollars
            .unwrap_or(self.microdollar_cost);
        base_cost.wrapping_add(self.microdollar_adjust)
    }

    /// Get the calculated cost (from pricing tables), ignoring provider cost
    pub fn calculated_microdollars(&self) -> u64 {
        self.microdollar_cost.wrapping_add(self.microdollar_adjust)
    }

    /// Check if provider reported cost directly
    pub fn has_provider_cost(&self) -> bool {
        self.provider_cost_microdollars.is_some()
    }

    /// Convert to dollars (for display)
    pub fn total_dollars(&self) -> f64 {
        (self.total_microdollars() as f64) / 1_000_000.0
    }

    /// Convert to cents (for legacy compatibility)
    pub fn total_cents(&self) -> u64 {
        self.total_microdollars() / 10_000
    }
}

// ============================================================================
// Media Types - for multimodal content (images, audio, video)
// ============================================================================

/// Source of media content - where the data comes from
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MediaSource {
    /// Base64-encoded data with MIME type
    Base64 { data: String, mime_type: String },
    /// URL to the media (public URL)
    Url { url: String },
    /// Provider file ID (uploaded via provider's File API)
    FileId { file_id: String },
    /// Provider file URI (Google-style, includes YouTube URLs)
    FileUri {
        uri: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
    },
}

impl MediaSource {
    /// Create from base64 data
    pub fn base64(data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        Self::Base64 {
            data: data.into(),
            mime_type: mime_type.into(),
        }
    }

    /// Create from URL
    pub fn url(url: impl Into<String>) -> Self {
        Self::Url { url: url.into() }
    }

    /// Create from provider file ID
    pub fn file_id(file_id: impl Into<String>) -> Self {
        Self::FileId {
            file_id: file_id.into(),
        }
    }

    /// Create from provider file URI (Google)
    pub fn file_uri(uri: impl Into<String>) -> Self {
        Self::FileUri {
            uri: uri.into(),
            mime_type: None,
        }
    }

    /// Create from YouTube URL (Google)
    pub fn youtube(url: impl Into<String>) -> Self {
        Self::FileUri {
            uri: url.into(),
            mime_type: Some("video/*".to_string()),
        }
    }
}

/// Video-specific metadata for processing options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VideoMetadata {
    /// Start offset for clipping (e.g., "1250s" or "00:30")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_offset: Option<String>,

    /// End offset for clipping
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_offset: Option<String>,

    /// Frames per second for sampling (default 1 FPS)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fps: Option<f32>,
}

impl VideoMetadata {
    /// Create new video metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Set start offset
    pub fn with_start(mut self, offset: impl Into<String>) -> Self {
        self.start_offset = Some(offset.into());
        self
    }

    /// Set end offset
    pub fn with_end(mut self, offset: impl Into<String>) -> Self {
        self.end_offset = Some(offset.into());
        self
    }

    /// Set FPS for video sampling
    pub fn with_fps(mut self, fps: f32) -> Self {
        self.fps = Some(fps);
        self
    }

    /// Set clipping range
    pub fn with_clip(mut self, start: impl Into<String>, end: impl Into<String>) -> Self {
        self.start_offset = Some(start.into());
        self.end_offset = Some(end.into());
        self
    }
}

/// A media content part that can be attached to messages
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MediaPart {
    /// Text content
    Text { text: String },

    /// Image content
    Image {
        source: MediaSource,
        /// Detail level for processing (OpenAI: "high", "low", "auto")
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },

    /// Audio content
    Audio {
        source: MediaSource,
        /// Audio format hint (e.g., "wav", "mp3")
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<String>,
    },

    /// Video content
    Video {
        source: MediaSource,
        /// Video processing metadata (clipping, FPS)
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<VideoMetadata>,
    },

    /// PDF document (Anthropic, Google)
    Document { source: MediaSource },
}

impl MediaPart {
    /// Create a text part
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    /// Create an image part from a source
    pub fn image(source: MediaSource) -> Self {
        Self::Image {
            source,
            detail: None,
        }
    }

    /// Create an image part with detail level
    pub fn image_with_detail(source: MediaSource, detail: impl Into<String>) -> Self {
        Self::Image {
            source,
            detail: Some(detail.into()),
        }
    }

    /// Create an audio part
    pub fn audio(source: MediaSource) -> Self {
        Self::Audio {
            source,
            format: None,
        }
    }

    /// Create an audio part with format hint
    pub fn audio_with_format(source: MediaSource, format: impl Into<String>) -> Self {
        Self::Audio {
            source,
            format: Some(format.into()),
        }
    }

    /// Create a video part
    pub fn video(source: MediaSource) -> Self {
        Self::Video {
            source,
            metadata: None,
        }
    }

    /// Create a video part with metadata
    pub fn video_with_metadata(source: MediaSource, metadata: VideoMetadata) -> Self {
        Self::Video {
            source,
            metadata: Some(metadata),
        }
    }

    /// Create a video from YouTube URL
    pub fn youtube(url: impl Into<String>) -> Self {
        Self::Video {
            source: MediaSource::youtube(url),
            metadata: None,
        }
    }

    /// Create a video from YouTube URL with clipping
    pub fn youtube_clip(
        url: impl Into<String>,
        start: impl Into<String>,
        end: impl Into<String>,
    ) -> Self {
        Self::Video {
            source: MediaSource::youtube(url),
            metadata: Some(VideoMetadata::new().with_clip(start, end)),
        }
    }

    /// Create a document part
    pub fn document(source: MediaSource) -> Self {
        Self::Document { source }
    }
}

/// A multimodal message containing multiple parts (text, images, video, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultimodalMessage {
    /// Message role
    pub role: ChatRole,

    /// Content parts (text, images, video, audio, documents)
    pub parts: Vec<MediaPart>,

    /// Optional cache marker (for Anthropic caching)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_marker: Option<CacheMarker>,
}

impl MultimodalMessage {
    /// Create a new multimodal user message
    pub fn user(parts: Vec<MediaPart>) -> Self {
        Self {
            role: ChatRole::User,
            parts,
            cache_marker: None,
        }
    }

    /// Create a user message with text and media
    pub fn user_with_media(text: impl Into<String>, media: MediaPart) -> Self {
        Self {
            role: ChatRole::User,
            parts: vec![media, MediaPart::text(text)],
            cache_marker: None,
        }
    }

    /// Create a user message with text and video
    pub fn user_with_video(text: impl Into<String>, source: MediaSource) -> Self {
        Self::user_with_media(text, MediaPart::video(source))
    }

    /// Create a user message with text and image
    pub fn user_with_image(text: impl Into<String>, source: MediaSource) -> Self {
        Self::user_with_media(text, MediaPart::image(source))
    }

    /// Create a user message with text and YouTube video
    pub fn user_with_youtube(text: impl Into<String>, url: impl Into<String>) -> Self {
        Self::user_with_media(text, MediaPart::youtube(url))
    }

    /// Add a cache marker
    pub fn with_cache_marker(mut self, marker: CacheMarker) -> Self {
        self.cache_marker = Some(marker);
        self
    }
}

// ============================================================================
// Tool Types
// ============================================================================

/// Result of ToolCall::create() - handles single, multiple, or empty results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CreateResult {
    /// Single tool call
    Single(ToolCall),
    /// Multiple tool calls
    Multiple(Vec<ToolCall>),
    /// Empty input (no tool calls)
    Empty,
}

/// A tool call from the LLM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    /// Unique ID for this tool use
    pub tool_use_id: String,

    /// Name of the tool to call
    pub tool_name: String,

    /// Tool arguments (parsed JSON)
    pub args: serde_json::Value,

    /// Raw argument string (for debugging)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_arguments: Option<String>,

    /// Provider signature for reasoning preservation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,

    /// Original tool call object (for preserving provider format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_obj: Option<serde_json::Value>,
}

impl ToolCall {
    /// Create a new tool call
    pub fn new(tool_name: impl Into<String>, args: serde_json::Value) -> Self {
        if let Ok(CreateResult::Single(tool_call)) = Self::create(args.clone()) {
            return tool_call;
        }

        Self {
            tool_use_id: Uuid::new_v4().to_string(),
            tool_name: tool_name.into(),
            args,
            raw_arguments: None,
            signature: None,
            tool_obj: None,
        }
    }

    /// Create from provider-specific format with auto-detection
    ///
    /// Detects the provider format automatically based on the structure:
    /// - OpenAI: has "function" key with "name" and "arguments"
    /// - Anthropic: has "name" and "input" keys (no "function")
    /// - Google: has "functionCall" key
    /// - Deserializable object: has "_tool_name" attribute
    /// - List: returns Vec of ToolCalls
    pub fn create(data: serde_json::Value) -> Result<CreateResult> {
        // Handle list input
        if let Some(arr) = data.as_array() {
            if arr.is_empty() {
                return Ok(CreateResult::Empty);
            }
            let mut results = Vec::new();
            for item in arr {
                match Self::create(item.clone())? {
                    CreateResult::Single(tc) => results.push(tc),
                    CreateResult::Multiple(tcs) => results.extend(tcs),
                    CreateResult::Empty => {}
                }
            }
            return Ok(CreateResult::Multiple(results));
        }

        // Detect format and parse
        if data.get("function").is_some() {
            // OpenAI format
            Self::from_provider_format(data, Provider::OpenAI).map(CreateResult::Single)
        } else if data
            .get("type")
            .and_then(|v| v.as_str())
            .map(|t| t == "function_call")
            .unwrap_or(false)
            && data.get("name").is_some()
        {
            // OpenAI/OpenRouter Responses format
            Self::from_provider_format(data, Provider::OpenAIResponses).map(CreateResult::Single)
        } else if data.get("functionCall").is_some() {
            // Google format
            Self::from_provider_format(data, Provider::GoogleGenAI).map(CreateResult::Single)
        } else if data.get("name").is_some() && data.get("input").is_some() {
            // Anthropic format
            Self::from_provider_format(data, Provider::Anthropic).map(CreateResult::Single)
        } else if data.get("_tool_name").is_some() {
            // Deserializable object format
            let tool_name = data["_tool_name"]
                .as_str()
                .ok_or_else(|| Error::Parse("_tool_name must be a string".to_string()))?
                .to_string();
            let tool_use_id = data
                .get("_tool_use_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            let signature = data
                .get("signature")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| {
                    data.get("thought_signature")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                });

            // Extract args by filtering out internal fields
            let mut args = serde_json::Map::new();
            if let Some(obj) = data.as_object() {
                for (k, v) in obj {
                    if !k.starts_with('_') && k != "signature" && k != "thought_signature" {
                        args.insert(k.clone(), v.clone());
                    }
                }
            }

            Ok(CreateResult::Single(Self {
                tool_use_id,
                tool_name,
                args: serde_json::Value::Object(args),
                raw_arguments: None,
                signature,
                tool_obj: Some(data),
            }))
        } else {
            Err(Error::Parse("Unknown tool call format".to_string()))
        }
    }

    /// Create from provider-specific format
    pub fn from_provider_format(
        provider_format: serde_json::Value,
        provider: Provider,
    ) -> Result<Self> {
        match provider {
            Provider::Anthropic | Provider::Bedrock => {
                let signature = provider_format
                    .get("signature")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        provider_format
                            .get("thought_signature")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .or_else(|| {
                        provider_format
                            .get("thoughtSignature")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    });

                Ok(Self {
                    tool_use_id: provider_format["id"]
                        .as_str()
                        .ok_or_else(|| Error::Parse("Missing 'id' in tool call".to_string()))?
                        .to_string(),
                    tool_name: provider_format["name"]
                        .as_str()
                        .ok_or_else(|| Error::Parse("Missing 'name' in tool call".to_string()))?
                        .to_string(),
                    args: provider_format["input"].clone(),
                    raw_arguments: None,
                    signature,
                    tool_obj: Some(provider_format),
                })
            }
            Provider::OpenAI | Provider::OpenRouter => {
                let function = &provider_format["function"];
                let arguments = &function["arguments"];
                let explicit_raw_arguments = provider_format
                    .get("raw_arguments")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                // Handle both string (real API) and object (streaming accumulator) arguments
                let (args, raw_arguments) = if let Some(args_str) = arguments.as_str() {
                    let parsed: serde_json::Value = serde_json::from_str(args_str)?;
                    (
                        parsed,
                        Some(explicit_raw_arguments.unwrap_or_else(|| args_str.to_string())),
                    )
                } else if arguments.is_object() {
                    let raw = serde_json::to_string(arguments)?;
                    (
                        arguments.clone(),
                        Some(explicit_raw_arguments.unwrap_or(raw)),
                    )
                } else {
                    return Err(Error::Parse("Missing 'arguments' in tool call".to_string()));
                };

                Ok(Self {
                    tool_use_id: provider_format["id"]
                        .as_str()
                        .ok_or_else(|| Error::Parse("Missing 'id' in tool call".to_string()))?
                        .to_string(),
                    tool_name: function["name"]
                        .as_str()
                        .ok_or_else(|| Error::Parse("Missing 'name' in function".to_string()))?
                        .to_string(),
                    args,
                    raw_arguments,
                    signature: None,
                    tool_obj: Some(provider_format),
                })
            }
            Provider::OpenAIResponses | Provider::OpenRouterResponses => {
                let args_str = provider_format["arguments"].as_str().unwrap_or("");
                let raw_arguments = provider_format
                    .get("raw_arguments")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| args_str.to_string());
                let call_id = provider_format
                    .get("call_id")
                    .and_then(|v| v.as_str())
                    .or_else(|| provider_format.get("id").and_then(|v| v.as_str()))
                    .ok_or_else(|| Error::Parse("Missing 'call_id' in tool call".to_string()))?;
                Ok(Self {
                    tool_use_id: call_id.to_string(),
                    tool_name: provider_format["name"]
                        .as_str()
                        .ok_or_else(|| Error::Parse("Missing 'name' in tool call".to_string()))?
                        .to_string(),
                    args: serde_json::from_str(args_str).unwrap_or_else(|_| serde_json::json!({})),
                    raw_arguments: Some(raw_arguments),
                    signature: None,
                    tool_obj: Some(provider_format),
                })
            }
            Provider::GoogleGenAI | Provider::GoogleAnthropic => {
                // Google format generates tool_use_id based on name + args hash (matches Python)
                let func_call = &provider_format["functionCall"];
                let name = func_call["name"]
                    .as_str()
                    .ok_or_else(|| Error::Parse("Missing 'name' in functionCall".to_string()))?;
                let args = func_call
                    .get("args")
                    .cloned()
                    .unwrap_or(serde_json::json!({}));

                // Generate ID matching Python: google_{name}_{hash}
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                args.to_string().hash(&mut hasher);
                let hash = hasher.finish();
                let tool_use_id = format!("google_{}_{}", name, hash);

                let signature = provider_format
                    .get("thoughtSignature")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        provider_format
                            .get("thought_signature")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .or_else(|| {
                        provider_format
                            .get("signature")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    });

                Ok(Self {
                    tool_use_id,
                    tool_name: name.to_string(),
                    args,
                    raw_arguments: None,
                    signature,
                    tool_obj: Some(provider_format),
                })
            }
        }
    }

    /// Convert to provider-specific assistant message format
    pub fn to_provider_assistant_message(&self, provider: Provider) -> serde_json::Value {
        match provider {
            Provider::Anthropic | Provider::Bedrock => {
                serde_json::json!({
                    "role": "assistant",
                    "content": [{
                        "type": "tool_use",
                        "id": self.tool_use_id,
                        "name": self.tool_name,
                        "input": self.args
                    }]
                })
            }
            Provider::OpenAI | Provider::OpenRouter => {
                // Use custom formatting to match Python's json.dumps (space after colon/comma)
                let args_str = self.raw_arguments.clone().unwrap_or_else(|| {
                    // Format to match Python's json.dumps with separators=(", ", ": ")
                    format_json_python_style(&self.args)
                });
                serde_json::json!({
                    "role": "assistant",
                    "tool_calls": [{
                        "id": self.tool_use_id,
                        "type": "function",
                        "function": {
                            "name": self.tool_name,
                            "arguments": args_str
                        }
                    }]
                })
            }
            Provider::OpenAIResponses | Provider::OpenRouterResponses => {
                let args_str = self.raw_arguments.clone().unwrap_or_else(|| {
                    serde_json::to_string(&self.args).unwrap_or_else(|_| "{}".to_string())
                });
                serde_json::json!({
                    "type": "function_call",
                    "id": format!("fc_{}", self.tool_use_id),
                    "call_id": self.tool_use_id,
                    "name": self.tool_name,
                    "arguments": args_str
                })
            }
            Provider::GoogleGenAI | Provider::GoogleAnthropic => {
                serde_json::json!({
                    "role": "model",
                    "parts": [{
                        "functionCall": {
                            "name": self.tool_name,
                            "args": self.args
                        }
                    }]
                })
            }
        }
    }
}

/// Result of a tool execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    /// ID matching the original tool call
    pub tool_use_id: String,

    /// Name of the tool that was called (needed for Google API)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,

    /// Result content (stringified)
    pub content: String,

    /// Whether the tool execution resulted in an error
    pub is_error: bool,

    /// Provider signature for reasoning preservation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,

    /// Original tool call object for reference (as JSON)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_obj: Option<serde_json::Value>,
}

impl ToolResult {
    /// Create a successful tool result
    pub fn success(tool_use_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            tool_use_id: tool_use_id.into(),
            tool_name: None,
            content: content.into(),
            is_error: false,
            signature: None,
            tool_obj: None,
        }
    }

    /// Create a successful tool result with tool name (for Google API)
    pub fn success_with_name(
        tool_use_id: impl Into<String>,
        tool_name: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            tool_use_id: tool_use_id.into(),
            tool_name: Some(tool_name.into()),
            content: content.into(),
            is_error: false,
            signature: None,
            tool_obj: None,
        }
    }

    /// Create an error tool result
    pub fn error(tool_use_id: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            tool_use_id: tool_use_id.into(),
            tool_name: None,
            content: error.into(),
            is_error: true,
            signature: None,
            tool_obj: None,
        }
    }

    /// Set the tool name
    pub fn with_tool_name(mut self, name: impl Into<String>) -> Self {
        self.tool_name = Some(name.into());
        self
    }

    /// Set the tool object (JSON representation)
    pub fn with_tool_obj(mut self, obj: serde_json::Value) -> Self {
        self.tool_obj = Some(obj);
        self
    }

    /// Convert to provider-specific format
    pub fn to_provider_format(&self, provider: Provider) -> serde_json::Value {
        match provider {
            Provider::Anthropic | Provider::Bedrock => {
                serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": self.tool_use_id,
                    "content": self.content,
                    "is_error": self.is_error
                })
            }
            Provider::OpenAI | Provider::OpenRouter => {
                serde_json::json!({
                    "role": "tool",
                    "tool_call_id": self.tool_use_id,
                    "content": self.content
                })
            }
            Provider::OpenAIResponses | Provider::OpenRouterResponses => {
                serde_json::json!({
                    "type": "function_call_output",
                    "call_id": self.tool_use_id,
                    "output": self.content
                })
            }
            Provider::GoogleGenAI | Provider::GoogleAnthropic => {
                // Try to get tool_name from: 1) tool_name field, 2) tool_obj["_tool_name"], 3) "unknown"
                let name = self.tool_name.clone().unwrap_or_else(|| {
                    self.tool_obj
                        .as_ref()
                        .and_then(|obj| obj.get("_tool_name"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                });

                serde_json::json!({
                    "functionResponse": {
                        "name": name,
                        "response": {
                            "result": self.content
                        }
                    }
                })
            }
        }
    }
}

/// A reasoning segment (extended thinking)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReasoningSegment {
    /// Reasoning content
    pub content: String,

    /// Index in sequence of reasoning segments
    pub segment_index: usize,

    /// Provider signature for reasoning preservation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,

    /// Provider-specific metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_metadata: Option<serde_json::Value>,
}

impl ReasoningSegment {
    /// Create a new reasoning segment with just content (index defaults to 0)
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            segment_index: 0,
            signature: None,
            provider_metadata: None,
        }
    }

    /// Create a new reasoning segment with content and index
    pub fn with_index(content: impl Into<String>, segment_index: usize) -> Self {
        Self {
            content: content.into(),
            segment_index,
            signature: None,
            provider_metadata: None,
        }
    }

    /// Set the signature
    pub fn with_signature(mut self, signature: impl Into<String>) -> Self {
        self.signature = Some(signature.into());
        self
    }

    /// Set provider metadata
    pub fn with_provider_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.provider_metadata = Some(metadata);
        self
    }

    /// Set segment index
    pub fn with_segment_index(mut self, index: usize) -> Self {
        self.segment_index = index;
        self
    }

    /// Convert to provider-specific format
    pub fn to_provider_format(&self, provider: Provider) -> serde_json::Value {
        match provider {
            // Anthropic, Bedrock, and GoogleAnthropic (Vertex AI Claude) all use Anthropic format
            Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic => {
                let mut obj = serde_json::json!({
                    "type": "thinking",
                    "thinking": self.content
                });
                if let Some(ref sig) = self.signature {
                    obj["signature"] = serde_json::Value::String(sig.clone());
                }
                obj
            }
            Provider::OpenAI | Provider::OpenRouter => {
                let mut obj = serde_json::json!({
                    "type": "reasoning",
                    "content": self.content
                });
                if let Some(ref sig) = self.signature {
                    obj["signature"] = serde_json::Value::String(sig.clone());
                }
                obj
            }
            Provider::OpenAIResponses | Provider::OpenRouterResponses => {
                let mut obj = serde_json::json!({
                    "type": "reasoning",
                    "content": [{
                        "type": "output_text",
                        "text": self.content
                    }]
                });
                if let Some(ref sig) = self.signature {
                    obj["signature"] = serde_json::Value::String(sig.clone());
                }
                obj
            }
            Provider::GoogleGenAI => {
                let mut obj = serde_json::json!({
                    "thought": true,
                    "text": self.content
                });
                if let Some(ref sig) = self.signature {
                    obj["thoughtSignature"] = serde_json::Value::String(sig.clone());
                }
                obj
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_from_model_string() {
        let (provider, model) = Provider::from_model_string("anthropic:claude-3-opus").unwrap();
        assert_eq!(provider, Provider::Anthropic);
        assert_eq!(model, "claude-3-opus");

        let (provider, model) = Provider::from_model_string("openai:gpt-4").unwrap();
        assert_eq!(provider, Provider::OpenAI);
        assert_eq!(model, "gpt-4");

        let (provider, model) = Provider::from_model_string("openai:resp:gpt-4").unwrap();
        assert_eq!(provider, Provider::OpenAIResponses);
        assert_eq!(model, "gpt-4");

        let (provider, model) =
            Provider::from_model_string("openrouter:resp:openai/o4-mini").unwrap();
        assert_eq!(provider, Provider::OpenRouterResponses);
        assert_eq!(model, "openai/o4-mini");

        assert!(Provider::from_model_string("invalid").is_err());
    }

    #[test]
    fn test_provider_image_input_override() {
        assert!(
            Provider::OpenRouter.supports_image_input("qwen/qwen2.5-vl-72b-instruct@vision=true")
        );
        assert!(
            !Provider::OpenRouter.supports_image_input("qwen/qwen2.5-vl-72b-instruct@vision=maybe")
        );
        assert!(!Provider::OpenAI.supports_image_input("gpt-4o@vision=false"));
        assert!(
            !Provider::OpenRouter
                .supports_image_input("qwen/qwen2.5-vl-72b-instruct@image_input=true")
        );

        let capabilities = Provider::OpenRouter.capabilities_for_model("custom/model@vision=true");
        assert!(capabilities.image_input);
        assert_eq!(
            capabilities.image_input_sources,
            vec![MediaSourceKind::Base64, MediaSourceKind::Url]
        );
    }

    #[test]
    fn test_provider_supports_native_tools() {
        assert!(Provider::Anthropic.supports_native_tools());
        assert!(Provider::OpenAI.supports_native_tools());
        assert!(Provider::OpenAIResponses.supports_native_tools());
        assert!(Provider::GoogleAnthropic.supports_native_tools());
        assert!(Provider::GoogleGenAI.supports_native_tools());
        assert!(Provider::OpenRouter.supports_native_tools());
        assert!(Provider::OpenRouterResponses.supports_native_tools());
        assert!(Provider::Bedrock.supports_native_tools());
    }

    #[test]
    fn test_chat_message_constructors() {
        let msg = ChatMessage::system("You are helpful");
        assert_eq!(msg.role, ChatRole::System);
        assert_eq!(msg.content, "You are helpful");

        let msg = ChatMessage::user("Hello");
        assert_eq!(msg.role, ChatRole::User);

        let msg = ChatMessage::assistant("Hi there").with_cache_marker(CacheMarker::Ephemeral);
        assert_eq!(msg.role, ChatRole::Assistant);
        assert!(msg.cache_marker.is_some());
    }

    #[test]
    fn test_token_usage() {
        let usage = TokenUsage::new(100, 50, 25);
        assert_eq!(usage.input_tokens, 100);
        assert_eq!(usage.output_tokens, 50);
        assert_eq!(usage.cached_tokens, 25);
        assert_eq!(usage.total_tokens(), 150);
    }

    #[test]
    fn test_tool_call_creation() {
        let tool_call = ToolCall::new("get_weather", serde_json::json!({"city": "SF"}));
        assert_eq!(tool_call.tool_name, "get_weather");
        assert_eq!(tool_call.args["city"], "SF");
        assert!(!tool_call.tool_use_id.is_empty());
    }

    #[test]
    fn test_tool_call_from_anthropic_format() {
        let format = serde_json::json!({
            "id": "toolu_123",
            "name": "get_weather",
            "input": {"city": "SF"}
        });

        let tool_call = ToolCall::from_provider_format(format, Provider::Anthropic).unwrap();
        assert_eq!(tool_call.tool_use_id, "toolu_123");
        assert_eq!(tool_call.tool_name, "get_weather");
        assert_eq!(tool_call.args["city"], "SF");
    }

    #[test]
    fn test_tool_call_from_openai_format() {
        let format = serde_json::json!({
            "id": "call_123",
            "function": {
                "name": "get_weather",
                "arguments": "{\"city\":\"SF\"}"
            }
        });

        let tool_call = ToolCall::from_provider_format(format, Provider::OpenAI).unwrap();
        assert_eq!(tool_call.tool_use_id, "call_123");
        assert_eq!(tool_call.tool_name, "get_weather");
        assert_eq!(tool_call.args["city"], "SF");
    }

    #[test]
    fn test_tool_result_success() {
        let result = ToolResult::success("toolu_123", "Sunny, 72°F");
        assert_eq!(result.tool_use_id, "toolu_123");
        assert_eq!(result.content, "Sunny, 72°F");
        assert!(!result.is_error);
    }

    #[test]
    fn test_tool_result_error() {
        let result = ToolResult::error("toolu_123", "API error");
        assert!(result.is_error);
    }

    #[test]
    fn test_tool_result_to_anthropic_format() {
        let result = ToolResult::success("toolu_123", "Result");
        let format = result.to_provider_format(Provider::Anthropic);

        assert_eq!(format["type"], "tool_result");
        assert_eq!(format["tool_use_id"], "toolu_123");
        assert_eq!(format["content"], "Result");
    }

    #[test]
    fn test_tool_result_to_openai_format() {
        let result = ToolResult::success("call_123", "Result");
        let format = result.to_provider_format(Provider::OpenAI);

        assert_eq!(format["role"], "tool");
        assert_eq!(format["tool_call_id"], "call_123");
        assert_eq!(format["content"], "Result");
    }

    #[test]
    fn test_reasoning_segment() {
        let segment = ReasoningSegment::with_index("Thinking...", 0);
        assert_eq!(segment.content, "Thinking...");
        assert_eq!(segment.segment_index, 0);
    }

    #[test]
    fn test_reasoning_segment_to_anthropic_format() {
        let segment = ReasoningSegment::with_index("Thinking...", 0);
        let format = segment.to_provider_format(Provider::Anthropic);

        assert_eq!(format["type"], "thinking");
        assert_eq!(format["thinking"], "Thinking...");
    }
}
