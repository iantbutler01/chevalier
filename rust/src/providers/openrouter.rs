//! OpenRouter client implementation
//!
//! OpenRouter extends the OpenAI API with:
//! - Custom api_url (https://openrouter.ai/api/v1)
//! - Ranking headers (HTTP-Referer, X-Title)
//! - Cost tracking via _populate_cost()
//!
//! Internally delegates to OAIClient with Provider::OpenRouter type.

use async_trait::async_trait;
use futures::stream::Stream;
use std::pin::Pin;

use crate::error::Result;
use crate::providers::{GenerationConfig, GenerationResponse, InferenceClient, StreamChunk};
use crate::types::Provider;
use crate::utils::ConversationMessage;

use super::openai::OAIClient;

/// OpenRouter client (extends OpenAI API)
#[derive(Debug, Clone)]
pub struct OpenRouterClient {
    inner: OAIClient,
}

impl OpenRouterClient {
    /// Create a new OpenRouter client
    ///
    /// # Arguments
    /// * `api_key` - OpenRouter API key
    /// * `model` - Model name (e.g., "anthropic/claude-sonnet-4")
    /// * `referer` - Optional HTTP-Referer header for ranking
    /// * `title` - Optional X-Title header for ranking
    pub fn new(
        api_key: impl Into<String>,
        model: impl Into<String>,
        referer: Option<String>,
        title: Option<String>,
    ) -> Self {
        let inner = OAIClient::new(api_key, model)
            .with_api_url("https://openrouter.ai/api/v1/chat/completions")
            .with_ranking_headers(referer, title)
            .with_provider(Provider::OpenRouter);

        Self { inner }
    }

    /// Set reasoning mode
    pub fn with_reasoning(mut self, reasoning: impl Into<String>) -> Self {
        self.inner = self.inner.with_reasoning(reasoning);
        self
    }

    /// Pin an upstream provider with fallbacks disabled and parameter support required.
    pub fn with_upstream_provider(mut self, provider: impl Into<String>) -> Self {
        self.inner = self.inner.with_openrouter_provider(provider);
        self
    }

    /// Override whether this model accepts image input, from the model
    /// string's `@vision=` parameter.
    pub fn with_image_input(mut self, image_input: Option<bool>) -> Self {
        self.inner = self.inner.with_image_input(image_input);
        self
    }

    /// Populate cost information from OpenRouter response
    ///
    /// OpenRouter returns cost information in the response metadata:
    /// - `usage.prompt_tokens` * model's prompt cost
    /// - `usage.completion_tokens` * model's completion cost
    ///
    /// This is a placeholder for future cost tracking implementation.
    #[allow(dead_code)]
    async fn populate_cost(&self, _response: &mut GenerationResponse) -> Result<()> {
        // TODO: Implement cost calculation when we add cost tracking
        // Will need to:
        // 1. Fetch model pricing from OpenRouter API
        // 2. Calculate: prompt_tokens * prompt_price + completion_tokens * completion_price
        // 3. Store in response.cost field (when added)
        Ok(())
    }
}

#[async_trait]
impl InferenceClient for OpenRouterClient {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        let mut response = self.inner.get_generation(messages, config).await?;

        // Populate cost information (placeholder)
        self.populate_cost(&mut response).await?;

        Ok(response)
    }

    async fn connect_and_listen(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
        self.inner.connect_and_listen(messages, config).await
    }

    fn provider(&self) -> Provider {
        Provider::OpenRouter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_client() {
        let client = OpenRouterClient::new(
            "test-key",
            "anthropic/claude-sonnet-4",
            Some("https://example.com".to_string()),
            Some("Test App".to_string()),
        );

        assert_eq!(client.provider(), Provider::OpenRouter);
    }

    #[test]
    fn test_new_client_without_headers() {
        let client = OpenRouterClient::new("test-key", "anthropic/claude-sonnet-4", None, None);

        assert_eq!(client.provider(), Provider::OpenRouter);
    }

    #[test]
    fn test_with_reasoning() {
        let client = OpenRouterClient::new("test-key", "anthropic/claude-sonnet-4", None, None)
            .with_reasoning("high");

        assert_eq!(client.provider(), Provider::OpenRouter);
    }

    #[tokio::test]
    async fn test_populate_cost_placeholder() {
        let client = OpenRouterClient::new("test-key", "test-model", None, None);
        let mut response = GenerationResponse::text("test");

        // Should not error (placeholder implementation)
        let result = client.populate_cost(&mut response).await;
        assert!(result.is_ok());
    }
}
