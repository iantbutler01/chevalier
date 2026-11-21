//! A second model for a first one that fails before it answers.
//!
//! The host publishes pairs of model strings, primary to fallback. A client built for a
//! primary with a pair asks the primary first, once, with a short timeout; when the primary
//! fails before producing any output (a refused connection, an error status, a timeout, a
//! stream whose first event is an error) the same request goes to the fallback with the
//! caller's own retry and timeout settings. A failed primary is skipped for a cooldown, so an
//! outage costs one failed attempt rather than one per call.
//!
//! Only Chat Completions providers pair, because the request (tool schemas, message format)
//! is built once from the primary's model string and sent to whichever client answers.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::{Stream, StreamExt, stream};

use crate::error::{Error, Result};
use crate::providers::{
    GenerationConfig, GenerationResponse, InferenceClient, StreamChunk, TraceCallback,
};
use crate::retry::RetryConfig;
use crate::types::Provider;
use crate::utils::ConversationMessage;

/// How long a failed primary is passed over before it is asked again.
const COOLDOWN: Duration = Duration::from_secs(60);
/// The longest the primary may go without sending a byte before the fallback is asked.
const PRIMARY_TIMEOUT: Duration = Duration::from_secs(30);
/// Providers whose requests share the Chat Completions wire format.
const CHAT_COMPLETIONS: &[&str] = &["opencode", "openrouter", "custom-openai", "openai"];

pub(super) struct Route {
    fallback: String,
    down_until: Mutex<Option<Instant>>,
}

impl Route {
    fn primary_up(&self) -> bool {
        let down_until = self.down_until.lock().unwrap_or_else(|e| e.into_inner());
        down_until.is_none_or(|until| Instant::now() >= until)
    }

    fn mark_down(&self) {
        *self.down_until.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(Instant::now() + COOLDOWN);
    }
}

static ROUTES: LazyLock<RwLock<HashMap<String, Arc<Route>>>> = LazyLock::new(Default::default);

/// Replace the published primary-to-fallback pairs. A pair that is unchanged keeps its
/// cooldown; a pair outside Chat Completions, or one naming itself, is dropped with a warning.
pub fn set_model_fallbacks(pairs: HashMap<String, String>) {
    let mut routes = ROUTES.write().unwrap_or_else(|e| e.into_inner());
    let previous = std::mem::take(&mut *routes);
    for (primary, fallback) in pairs {
        if primary == fallback || !chat_completions(&primary) || !chat_completions(&fallback) {
            tracing::warn!(%primary, %fallback, "model fallback ignored: both models must be distinct Chat Completions models");
            continue;
        }
        let route = match previous.get(&primary) {
            Some(route) if route.fallback == fallback => route.clone(),
            _ => Arc::new(Route {
                fallback,
                down_until: Mutex::new(None),
            }),
        };
        routes.insert(primary, route);
    }
}

fn chat_completions(model: &str) -> bool {
    super::inference::parse_model_provider(model)
        .is_some_and(|provider| CHAT_COMPLETIONS.contains(&provider.as_str()))
}

/// The fallback published for `model`, with the route that remembers its cooldown.
pub(super) fn route_for(model: &str) -> Option<(String, Arc<Route>)> {
    let routes = ROUTES.read().unwrap_or_else(|e| e.into_inner());
    routes
        .get(model)
        .map(|route| (route.fallback.clone(), route.clone()))
}

/// Whether the primary's failure says the provider is unwell, so the fallback should answer.
/// A request the provider rejected on its merits (too long, malformed) would be rejected by
/// the fallback too, and goes back to the caller as it is.
fn provider_failed(error: &Error) -> bool {
    match error {
        Error::ContextLengthExceeded(_) => false,
        Error::NonRetryable(message) => message.starts_with("401") || message.starts_with("403"),
        _ => true,
    }
}

pub(super) struct FailoverClient {
    primary_model: String,
    primary: Box<dyn InferenceClient>,
    fallback: Box<dyn InferenceClient>,
    route: Arc<Route>,
}

impl FailoverClient {
    pub(super) fn new(
        primary_model: &str,
        primary: Box<dyn InferenceClient>,
        fallback: Box<dyn InferenceClient>,
        route: Arc<Route>,
    ) -> Self {
        Self {
            primary_model: primary_model.to_owned(),
            primary,
            fallback,
            route,
        }
    }

    /// One attempt, bounded by `PRIMARY_TIMEOUT`, so a failing primary hands over quickly.
    fn primary_config(config: &GenerationConfig) -> GenerationConfig {
        let mut config = config.clone();
        config.retry_config = Some(RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        });
        config.timeout = Some(config.timeout.map_or(PRIMARY_TIMEOUT, |t| t.min(PRIMARY_TIMEOUT)));
        config
    }

    fn hand_over(&self, error: &Error) -> bool {
        if !provider_failed(error) {
            return false;
        }
        self.route.mark_down();
        tracing::warn!(
            model = %self.primary_model,
            fallback = %self.route.fallback,
            error = %error,
            "inference.primary_failed; asking the fallback"
        );
        true
    }
}

#[async_trait]
impl InferenceClient for FailoverClient {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        if self.route.primary_up() {
            match self
                .primary
                .get_generation(messages, &Self::primary_config(config))
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) if !self.hand_over(&error) => return Err(error),
                Err(_) => {}
            }
        }
        self.fallback.get_generation(messages, config).await
    }

    async fn connect_and_listen(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
        if self.route.primary_up() {
            match self
                .primary
                .connect_and_listen(messages, &Self::primary_config(config))
                .await
            {
                Ok(mut chunks) => match chunks.next().await {
                    // Output has begun; from here the primary owns the answer.
                    Some(Ok(first)) => {
                        return Ok(Box::pin(stream::once(async { Ok(first) }).chain(chunks)));
                    }
                    None => return Ok(Box::pin(stream::empty())),
                    Some(Err(error)) if !self.hand_over(&error) => {
                        return Ok(Box::pin(stream::once(async { Err(error) })));
                    }
                    Some(Err(_)) => {}
                },
                Err(error) if !self.hand_over(&error) => return Err(error),
                Err(_) => {}
            }
        }
        self.fallback.connect_and_listen(messages, config).await
    }

    fn provider(&self) -> Provider {
        self.primary.provider()
    }

    fn supports_native_tools(&self) -> bool {
        self.primary.supports_native_tools() && self.fallback.supports_native_tools()
    }

    fn set_trace_callback(&mut self, callback: TraceCallback) {
        self.primary.set_trace_callback(callback.clone());
        self.fallback.set_trace_callback(callback);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::StreamChunk;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fake {
        name: &'static str,
        fail: Option<fn() -> Error>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl InferenceClient for Fake {
        async fn get_generation(
            &self,
            _messages: &[ConversationMessage],
            _config: &GenerationConfig,
        ) -> Result<GenerationResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.fail {
                Some(error) => Err(error()),
                None => Ok(GenerationResponse::text(self.name)),
            }
        }

        async fn connect_and_listen(
            &self,
            _messages: &[ConversationMessage],
            _config: &GenerationConfig,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let fail = self.fail;
            let name = self.name;
            Ok(Box::pin(stream::once(async move {
                match fail {
                    Some(error) => Err(error()),
                    None => Ok(StreamChunk::Content(name.to_owned())),
                }
            })))
        }

        fn provider(&self) -> Provider {
            Provider::OpenAI
        }
    }

    fn pair(
        fail: Option<fn() -> Error>,
    ) -> (FailoverClient, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let (primary_calls, fallback_calls) = (Arc::default(), Arc::default());
        let client = FailoverClient::new(
            "opencode:m",
            Box::new(Fake { name: "primary", fail, calls: Arc::clone(&primary_calls) }),
            Box::new(Fake { name: "fallback", fail: None, calls: Arc::clone(&fallback_calls) }),
            Arc::new(Route { fallback: "openrouter:m".into(), down_until: Mutex::new(None) }),
        );
        (client, primary_calls, fallback_calls)
    }

    async fn first_content(client: &FailoverClient) -> Result<String> {
        let mut chunks = client
            .connect_and_listen(&[], &GenerationConfig::default())
            .await?;
        match chunks.next().await {
            Some(Ok(StreamChunk::Content(text))) => Ok(text),
            Some(Err(error)) => Err(error),
            other => panic!("unexpected first chunk: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_healthy_primary_answers_and_the_fallback_is_never_asked() {
        let (client, primary, fallback) = pair(None);
        assert_eq!(first_content(&client).await.unwrap(), "primary");
        let response = client.get_generation(&[], &GenerationConfig::default()).await.unwrap();
        assert_eq!(response.content, "primary");
        assert_eq!((primary.load(Ordering::SeqCst), fallback.load(Ordering::SeqCst)), (2, 0));
    }

    #[tokio::test]
    async fn an_outage_hands_the_request_over_and_skips_the_primary_while_it_cools_down() {
        let (client, primary, fallback) =
            pair(Some(|| Error::Inference("503 Service Unavailable".into())));
        assert_eq!(first_content(&client).await.unwrap(), "fallback");
        assert_eq!(first_content(&client).await.unwrap(), "fallback");
        let response = client.get_generation(&[], &GenerationConfig::default()).await.unwrap();
        assert_eq!(response.content, "fallback");
        assert_eq!((primary.load(Ordering::SeqCst), fallback.load(Ordering::SeqCst)), (1, 3));
    }

    #[tokio::test]
    async fn a_revoked_key_or_exhausted_balance_hands_over_too() {
        let (client, _, _) = pair(Some(|| Error::NonRetryable("401 Unauthorized: bad key".into())));
        assert_eq!(first_content(&client).await.unwrap(), "fallback");
        let (client, _, _) = pair(Some(|| Error::Inference("402 Payment Required: balance".into())));
        assert_eq!(first_content(&client).await.unwrap(), "fallback");
    }

    #[tokio::test]
    async fn a_request_rejected_on_its_merits_returns_to_the_caller() {
        let (client, primary, fallback) =
            pair(Some(|| Error::ContextLengthExceeded("too long".into())));
        assert!(matches!(first_content(&client).await, Err(Error::ContextLengthExceeded(_))));
        let (bad, _, bad_fallback) =
            pair(Some(|| Error::NonRetryable("400 Bad Request: schema".into())));
        assert!(bad.get_generation(&[], &GenerationConfig::default()).await.is_err());
        assert_eq!(primary.load(Ordering::SeqCst), 1);
        assert_eq!(fallback.load(Ordering::SeqCst) + bad_fallback.load(Ordering::SeqCst), 0);
        assert!(client.route.primary_up());
    }

    #[test]
    fn only_distinct_chat_completions_pairs_are_kept() {
        let up = "opencode:deepseek-v4.1-flash@reasoning=medium";
        let down = "openrouter:deepseek/deepseek-v4.1-flash-1017@server_url=https://us.openrouter.ai/api/v1";
        set_model_fallbacks(HashMap::from([
            (up.to_owned(), down.to_owned()),
            ("anthropic:claude-x".to_owned(), down.to_owned()),
            (down.to_owned(), down.to_owned()),
        ]));
        assert_eq!(route_for(up).map(|(fallback, _)| fallback).as_deref(), Some(down));
        assert!(route_for("anthropic:claude-x").is_none());
        assert!(route_for(down).is_none());
        set_model_fallbacks(HashMap::new());
        assert!(route_for(up).is_none());
    }
}
