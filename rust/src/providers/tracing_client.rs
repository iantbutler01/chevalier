//! TracingInferenceClient - Wraps providers with fallback, cost tracking, and OTEL spans
//!
//! Features:
//! - Automatic fallback switching on RetriesExceeded
//! - Cost tracking with per-model pricing (microdollar precision)
//! - OpenTelemetry span instrumentation (via `tracing` crate)
//! - Request/response tracing to CHEVALIER_TRACE directory
//! - Trace callback for custom monitoring
//! - Switches back to primary after 5 minutes
//!
//! OTEL Integration:
//! This client uses the `tracing` crate for instrumentation. To export spans to
//! OpenTelemetry, configure a `tracing-opentelemetry` layer in your application.
//! Spans include:
//! - `inference.usage.input_tokens`
//! - `inference.usage.output_tokens`
//! - `inference.usage.cached_tokens`
//! - `inference.cost.microdollars`

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::providers::{
    CostInfo, CostStore, GenerationConfig, GenerationResponse, InferenceClient, StreamChunk,
    TraceCallback,
};
use crate::types::Provider;
use crate::utils::ConversationMessage;

const FALLBACK_DURATION: Duration = Duration::from_secs(5 * 60); // 5 minutes

/// Tracks when fallback was activated
#[derive(Clone)]
struct FallbackState {
    activated_at: Option<Instant>,
}

impl FallbackState {
    fn new() -> Self {
        Self { activated_at: None }
    }

    fn activate(&mut self) {
        self.activated_at = Some(Instant::now());
    }

    fn should_use_fallback(&self) -> bool {
        if let Some(activated) = self.activated_at {
            activated.elapsed() < FALLBACK_DURATION
        } else {
            false
        }
    }

    fn try_reset(&mut self) -> bool {
        if let Some(activated) = self.activated_at
            && activated.elapsed() >= FALLBACK_DURATION
        {
            self.activated_at = None;
            return true;
        }
        false
    }
}

/// TracingInferenceClient wraps a primary and optional fallback client
///
/// Automatically switches to fallback on RetriesExceeded, tracks costs,
/// and traces requests/responses for debugging.
pub struct TracingInferenceClient {
    primary_client: Box<dyn InferenceClient>,
    fallback_client: Option<Box<dyn InferenceClient>>,
    fallback_state: Arc<RwLock<FallbackState>>,
    cost_store: Option<Arc<dyn CostStore>>,
    trace_callback: Option<TraceCallback>,
    trace_output_path: Option<String>,
    request_counter: AtomicU64,
}

impl TracingInferenceClient {
    /// Create a new tracing client with primary client only
    pub fn new(primary: Box<dyn InferenceClient>) -> Self {
        // Check for CHEVALIER_TRACE env var
        let trace_output_path = std::env::var("CHEVALIER_TRACE").ok();

        Self {
            primary_client: primary,
            fallback_client: None,
            fallback_state: Arc::new(RwLock::new(FallbackState::new())),
            cost_store: None,
            trace_callback: None,
            trace_output_path,
            request_counter: AtomicU64::new(0),
        }
    }

    /// Create with both primary and fallback clients
    pub fn with_fallback(
        primary: Box<dyn InferenceClient>,
        fallback: Box<dyn InferenceClient>,
    ) -> Self {
        let trace_output_path = std::env::var("CHEVALIER_TRACE").ok();

        Self {
            primary_client: primary,
            fallback_client: Some(fallback),
            fallback_state: Arc::new(RwLock::new(FallbackState::new())),
            cost_store: None,
            trace_callback: None,
            trace_output_path,
            request_counter: AtomicU64::new(0),
        }
    }

    /// Add a cost store for tracking credits used
    pub fn with_cost_store(mut self, store: Arc<dyn CostStore>) -> Self {
        self.cost_store = Some(store);
        self
    }

    /// Add a trace callback for custom monitoring
    pub fn with_trace_callback(mut self, callback: TraceCallback) -> Self {
        self.trace_callback = Some(callback);
        self
    }

    /// Set trace output path (overrides CHEVALIER_TRACE env var)
    pub fn with_trace_output(mut self, path: impl Into<String>) -> Self {
        self.trace_output_path = Some(path.into());
        self
    }

    /// Generate a unique request ID
    fn next_request_id(&self) -> u64 {
        self.request_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the active client (primary or fallback)
    async fn get_active_client(&self) -> &dyn InferenceClient {
        let mut state = self.fallback_state.write().await;

        // Try to switch back to primary
        if state.try_reset() {
            log::info!("Switching back to primary client after timeout");
            return self.primary_client.as_ref();
        }

        // Check if we should use fallback
        if state.should_use_fallback()
            && let Some(ref fallback) = self.fallback_client
        {
            return fallback.as_ref();
        }

        self.primary_client.as_ref()
    }

    /// Handle tracing after a successful request
    async fn handle_trace(
        &self,
        request_id: u64,
        messages: &[ConversationMessage],
        response: &GenerationResponse,
        model: &str,
    ) -> Result<()> {
        // Use provider cost if available (e.g., from OpenRouter), otherwise calculate
        let cost = if let Some(provider_cost) = response.provider_cost_dollars {
            CostInfo::from_usage_with_provider_cost(&response.usage, model, provider_cost)
        } else {
            CostInfo::from_usage(&response.usage, model)
        };

        // Record to cost store
        if let Some(ref store) = self.cost_store {
            store.record_cost(model, &cost).await?;
            log::debug!(
                "Cost tracking: {} microdollars for {} tokens (model: {})",
                cost.total_microdollars(),
                response.usage.input_tokens + response.usage.output_tokens,
                model
            );
        }

        // Call trace callback
        if let Some(ref cb) = self.trace_callback {
            let msgs_json: Vec<serde_json::Value> = messages
                .iter()
                .map(|m| serde_json::to_value(m).unwrap_or(serde_json::Value::Null))
                .collect();
            cb(request_id, msgs_json, response, &cost).await;
        }

        // Write trace output
        if let Some(ref path) = self.trace_output_path {
            self.write_trace(path, request_id, messages, response, &cost)
                .await?;
        }

        Ok(())
    }

    /// Write trace data to the configured output path
    async fn write_trace(
        &self,
        path: &str,
        request_id: u64,
        messages: &[ConversationMessage],
        response: &GenerationResponse,
        cost: &CostInfo,
    ) -> Result<()> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let msgs_json: Vec<serde_json::Value> = messages
            .iter()
            .map(|m| serde_json::to_value(m).unwrap_or(serde_json::Value::Null))
            .collect();

        let trace_data = serde_json::json!({
            "request_id": request_id,
            "timestamp": timestamp,
            "messages": msgs_json,
            "response": {
                "content": response.response.text(),
                "tool_calls_count": response.response.tool_calls().len(),
                "has_reasoning": !response.response.reasoning().is_empty(),
            },
            "usage": {
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
                "cached_tokens": response.usage.cached_tokens,
            },
            "cost": {
                "input_tokens": cost.input_tokens,
                "output_tokens": cost.output_tokens,
                "cache_read_input_tokens": cost.cache_read_input_tokens,
                "microdollar_cost": cost.microdollar_cost,
                "total_dollars": cost.total_dollars(),
            }
        });

        // Create directory if needed
        tokio::fs::create_dir_all(path).await.map_err(|e| {
            Error::NonRetryable(format!("Failed to create trace directory {}: {}", path, e))
        })?;

        // Write trace file
        let file_path = format!("{}/trace_{}.json", path, request_id);
        let json_str = serde_json::to_string_pretty(&trace_data)
            .map_err(|e| Error::NonRetryable(format!("Failed to serialize trace: {}", e)))?;

        tokio::fs::write(&file_path, json_str).await.map_err(|e| {
            Error::NonRetryable(format!("Failed to write trace file {}: {}", file_path, e))
        })?;

        log::debug!("Wrote trace to {}", file_path);
        Ok(())
    }

    /// Handle streaming cost tracking by wrapping the stream
    fn wrap_stream_for_cost_tracking(
        &self,
        stream: Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>,
        model: String,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>> {
        let cost_store = self.cost_store.clone();

        let wrapped = stream.then(move |chunk| {
            let model = model.clone();
            let cost_store = cost_store.clone();

            async move {
                if let Ok(StreamChunk::Usage {
                    input_tokens,
                    output_tokens,
                    cached_tokens,
                    cache_write_input_tokens,
                    ..
                }) = &chunk
                {
                    // Calculate and record cost
                    let mut usage = crate::types::TokenUsage::new(
                        *input_tokens,
                        *output_tokens,
                        *cached_tokens,
                    );
                    usage.cache_write_input_tokens = *cache_write_input_tokens;
                    let cost = CostInfo::from_usage(&usage, &model);

                    if let Some(ref store) = cost_store
                        && let Err(e) = store.record_cost(&model, &cost).await
                    {
                        log::warn!("Failed to record streaming cost: {}", e);
                    }

                    log::debug!(
                        "Streaming cost: {} microdollars for {} tokens",
                        cost.total_microdollars(),
                        input_tokens + output_tokens
                    );
                }
                chunk
            }
        });

        Box::pin(wrapped)
    }
}

#[async_trait]
impl InferenceClient for TracingInferenceClient {
    #[tracing::instrument(
        name = "TracingInferenceClient.get_generation",
        skip(self, messages, config),
        fields(
            provider,
            model = %config.model,
            inference.usage.input_tokens,
            inference.usage.output_tokens,
            inference.usage.cached_tokens,
            inference.cost.microdollars
        )
    )]
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        let request_id = self.next_request_id();
        let client = self.get_active_client().await;
        let provider = client.provider();

        // Record provider on span
        tracing::Span::current().record("provider", format!("{:?}", provider).as_str());

        // Try primary/current client
        let result = client.get_generation(messages, config).await;

        // Handle fallback on retries exceeded
        if let Err(Error::RetriesExceeded(_)) = result {
            if let Some(fallback) = self.fallback_client.as_ref() {
                log::info!("Switching to fallback client after retries exceeded");
                let mut state = self.fallback_state.write().await;
                state.activate();
                drop(state);

                // Retry with fallback
                let response = fallback.get_generation(messages, config).await?;

                // Record metrics on span
                let span = tracing::Span::current();
                span.record("inference.usage.input_tokens", response.usage.input_tokens);
                span.record(
                    "inference.usage.output_tokens",
                    response.usage.output_tokens,
                );
                span.record(
                    "inference.usage.cached_tokens",
                    response.usage.cached_tokens,
                );

                let cost = if let Some(provider_cost) = response.provider_cost_dollars {
                    CostInfo::from_usage_with_provider_cost(
                        &response.usage,
                        &config.model,
                        provider_cost,
                    )
                } else {
                    CostInfo::from_usage(&response.usage, &config.model)
                };
                span.record("inference.cost.microdollars", cost.total_microdollars());

                // Handle tracing
                self.handle_trace(request_id, messages, &response, &config.model)
                    .await?;

                return Ok(response);
            } else {
                return result;
            }
        }

        let response = result?;

        // Record metrics on span
        let span = tracing::Span::current();
        span.record("inference.usage.input_tokens", response.usage.input_tokens);
        span.record(
            "inference.usage.output_tokens",
            response.usage.output_tokens,
        );
        span.record(
            "inference.usage.cached_tokens",
            response.usage.cached_tokens,
        );

        let cost = if let Some(provider_cost) = response.provider_cost_dollars {
            CostInfo::from_usage_with_provider_cost(&response.usage, &config.model, provider_cost)
        } else {
            CostInfo::from_usage(&response.usage, &config.model)
        };
        span.record("inference.cost.microdollars", cost.total_microdollars());

        // Handle tracing
        self.handle_trace(request_id, messages, &response, &config.model)
            .await?;

        Ok(response)
    }

    #[tracing::instrument(
        name = "TracingInferenceClient.connect_and_listen",
        skip(self, messages, config),
        fields(
            provider,
            model = %config.model
        )
    )]
    async fn connect_and_listen(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
        let client = self.get_active_client().await;

        // Record provider on span
        tracing::Span::current().record("provider", format!("{:?}", client.provider()).as_str());

        // Try primary/current client
        let result = client.connect_and_listen(messages, config).await;

        // Handle fallback on retries exceeded
        if let Err(Error::RetriesExceeded(_)) = result {
            if let Some(fallback) = self.fallback_client.as_ref() {
                log::info!("Switching to fallback client for streaming after retries exceeded");
                let mut state = self.fallback_state.write().await;
                state.activate();
                drop(state);

                // Retry with fallback
                let stream = fallback.connect_and_listen(messages, config).await?;
                return Ok(self.wrap_stream_for_cost_tracking(stream, config.model.clone()));
            } else {
                return result;
            }
        }

        let stream = result?;
        Ok(self.wrap_stream_for_cost_tracking(stream, config.model.clone()))
    }

    fn provider(&self) -> Provider {
        // Return primary provider (fallback is transparent)
        self.primary_client.provider()
    }

    fn supports_native_tools(&self) -> bool {
        self.primary_client.supports_native_tools()
    }

    fn set_trace_callback(&mut self, callback: TraceCallback) {
        self.trace_callback = Some(callback);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::MemoryCostStore;
    use crate::types::TokenUsage;

    struct MockClient {
        should_fail: bool,
        provider: Provider,
    }

    #[async_trait]
    impl InferenceClient for MockClient {
        async fn get_generation(
            &self,
            _messages: &[ConversationMessage],
            _config: &GenerationConfig,
        ) -> Result<GenerationResponse> {
            if self.should_fail {
                Err(Error::RetriesExceeded("mock failure".to_string()))
            } else {
                Ok(GenerationResponse::from_assistant_response(
                    crate::types::AssistantResponse::from_text("test"),
                    TokenUsage {
                        input_tokens: 100,
                        output_tokens: 50,
                        cached_tokens: 0,
                        cache_write_input_tokens: 0,
                        reasoning_tokens: None,
                        provider_cost_dollars: None,
                    },
                    None,
                    None,
                ))
            }
        }

        async fn connect_and_listen(
            &self,
            _messages: &[ConversationMessage],
            _config: &GenerationConfig,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
            unimplemented!()
        }

        fn provider(&self) -> Provider {
            self.provider
        }

        fn supports_native_tools(&self) -> bool {
            true
        }

        fn set_trace_callback(&mut self, _callback: TraceCallback) {}
    }

    #[tokio::test]
    async fn test_primary_success() {
        let primary = Box::new(MockClient {
            should_fail: false,
            provider: Provider::Anthropic,
        });

        let client = TracingInferenceClient::new(primary);
        let config = GenerationConfig::new("claude-3-sonnet");
        let result = client.get_generation(&[], &config).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fallback_on_retries_exceeded() {
        let primary = Box::new(MockClient {
            should_fail: true,
            provider: Provider::Anthropic,
        });
        let fallback = Box::new(MockClient {
            should_fail: false,
            provider: Provider::OpenAI,
        });

        let client = TracingInferenceClient::with_fallback(primary, fallback);
        let config = GenerationConfig::new("claude-3-sonnet");
        let result = client.get_generation(&[], &config).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cost_tracking() {
        let primary = Box::new(MockClient {
            should_fail: false,
            provider: Provider::Anthropic,
        });

        let store = Arc::new(MemoryCostStore::new());
        let client = TracingInferenceClient::new(primary).with_cost_store(store.clone());

        // Use a model name that triggers Sonnet pricing
        let config = GenerationConfig::new("claude-3-sonnet");
        let _ = client.get_generation(&[], &config).await.unwrap();

        // Check cost was tracked
        let credits = store.credits();
        assert!(credits > 0, "Credits tracked but zero: {}", credits);

        // Verify the cost is in a reasonable range for Sonnet pricing
        // 100 input tokens * 3 microdollars + 50 output tokens * 15 microdollars = 1050 microdollars
        assert!(
            (1000..=1100).contains(&credits),
            "Unexpected credit amount: {} (expected ~1050)",
            credits
        );
    }

    #[tokio::test]
    async fn test_cost_info_calculation() {
        let usage = TokenUsage::new(1000, 500, 100);
        let cost = CostInfo::from_usage(&usage, "claude-3-sonnet");

        // Sonnet: 3 microdollars/input, 15 microdollars/output, 0.30 microdollars/cache
        // 1000 * 3 + 500 * 15 + 100 * 0.30 = 3000 + 7500 + 30 = 10530 (ceil to 10530)
        assert!(cost.microdollar_cost >= 10500);
        assert!(cost.microdollar_cost <= 10600);

        // Test Haiku pricing
        let cost_haiku = CostInfo::from_usage(&usage, "claude-3-haiku");
        // 1000 * 0.8 + 500 * 4 + 100 * 0.08 = 800 + 2000 + 8 = 2808
        assert!(cost_haiku.microdollar_cost >= 2800);
        assert!(cost_haiku.microdollar_cost <= 2900);

        // Test Opus pricing
        let cost_opus = CostInfo::from_usage(&usage, "claude-3-opus");
        // 1000 * 15 + 500 * 75 + 100 * 1.5 = 15000 + 37500 + 150 = 52650
        assert!(cost_opus.microdollar_cost >= 52600);
        assert!(cost_opus.microdollar_cost <= 52700);
    }

    #[test]
    fn test_memory_cost_store() {
        let store = MemoryCostStore::new();
        assert_eq!(store.credits(), 0);
    }
}
