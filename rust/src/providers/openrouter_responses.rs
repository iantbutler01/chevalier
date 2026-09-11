//! OpenRouter Responses API client implementation
//!
//! OpenRouter Responses extends the OpenAI Responses API with:
//! - Custom api_url (https://openrouter.ai/api/v1/responses)
//! - Ranking headers (HTTP-Referer, X-Title)
//! - Cost tracking placeholder
//!
//! Internally delegates to OpenAIResponsesClient with Provider::OpenRouterResponses type.

use async_trait::async_trait;
use futures::stream::Stream;
use std::pin::Pin;

use crate::error::Result;
use crate::providers::{GenerationConfig, GenerationResponse, InferenceClient, StreamChunk};
use crate::types::Provider;
use crate::utils::ConversationMessage;

use super::openai_responses::OpenAIResponsesClient;

/// OpenRouter Responses client (extends OpenAI Responses API)
#[derive(Debug, Clone)]
pub struct OpenRouterResponsesClient {
    inner: OpenAIResponsesClient,
}

impl OpenRouterResponsesClient {
    /// Create a new OpenRouter Responses client
    ///
    /// # Arguments
    /// * `api_key` - OpenRouter API key
    /// * `model` - Model name (e.g., "openai/o4-mini")
    /// * `referer` - Optional HTTP-Referer header for ranking
    /// * `title` - Optional X-Title header for ranking
    pub fn new(
        api_key: impl Into<String>,
        model: impl Into<String>,
        referer: Option<String>,
        title: Option<String>,
    ) -> Self {
        let inner = OpenAIResponsesClient::new(api_key, model)
            .with_api_url("https://openrouter.ai/api/v1/responses")
            .with_ranking_headers(referer, title)
            .with_provider(Provider::OpenRouterResponses);

        Self { inner }
    }

    /// Set reasoning mode
    pub fn with_reasoning(mut self, reasoning: impl Into<String>) -> Self {
        self.inner = self.inner.with_reasoning(reasoning);
        self
    }

    /// Override whether this model accepts image input, from the model
    /// string's `@vision=` parameter.
    pub fn with_image_input(mut self, image_input: Option<bool>) -> Self {
        self.inner = self.inner.with_image_input(image_input);
        self
    }

    #[allow(dead_code)]
    async fn populate_cost(&self, _response: &mut GenerationResponse) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl InferenceClient for OpenRouterResponsesClient {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        let mut response = self.inner.get_generation(messages, config).await?;
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
        Provider::OpenRouterResponses
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_client() {
        let client = OpenRouterResponsesClient::new(
            "test-key",
            "openai/o4-mini",
            Some("https://example.com".to_string()),
            Some("Test App".to_string()),
        );

        assert_eq!(client.provider(), Provider::OpenRouterResponses);
    }
}
