//! Answering from a second model when the first fails before it answers.
//!
//! The caller names the fallback with each request (`RunParams::fallback_model`); which
//! model backs up which is the host's policy, not this crate's. What lives here is the
//! mechanism and the one piece of state it needs: when a primary last failed.
//!
//! A request with a fallback asks the primary first, once, and gives it `PRIMARY_START` to
//! begin answering. When the primary fails before producing any output (a refused
//! connection, an error status, no first chunk in time, a stream whose first event is an
//! error) the same request goes to the fallback with the caller's own retry and timeout
//! settings. Once output has begun the primary owns the answer: switching then would splice
//! two models' answers into one. A failed primary is passed over for `COOLDOWN`, so an
//! outage costs one failed attempt rather than one per call.
//!
//! Only Chat Completions providers pair, because the request (tool schemas, message format)
//! is built once from the primary's model string and sent to whichever client answers.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{LazyLock, Mutex};
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
/// How long the primary has to connect and send its first chunk. Shorter than a host's own
/// start deadline (Nym's is 30 s), so the fallback still has time to answer within it.
const PRIMARY_START: Duration = Duration::from_secs(20);
/// Providers whose requests share the Chat Completions wire format.
const CHAT_COMPLETIONS: &[&str] = &["opencode", "openrouter", "custom-openai", "openai"];

/// When each primary model string last failed, until its cooldown ends.
static DOWN_UNTIL: LazyLock<Mutex<HashMap<String, Instant>>> = LazyLock::new(Default::default);

fn primary_up(model: &str) -> bool {
    let down = DOWN_UNTIL.lock().unwrap_or_else(|e| e.into_inner());
    down.get(model).is_none_or(|until| Instant::now() >= *until)
}

fn mark_down(model: &str) {
    DOWN_UNTIL
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(model.to_owned(), Instant::now() + COOLDOWN);
}

/// Whether `primary` can be backed by `fallback`: two distinct Chat Completions models.
pub(super) fn pairable(primary: &str, fallback: &str) -> bool {
    let chat = |model: &str| {
        super::inference::parse_model_provider(model)
            .is_some_and(|provider| CHAT_COMPLETIONS.contains(&provider.as_str()))
    };
    primary != fallback && chat(primary) && chat(fallback)
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
    fallback_model: String,
    fallback: Box<dyn InferenceClient>,
}

type ChunkStream = Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>;

impl FailoverClient {
    pub(super) fn new(
        primary_model: &str,
        primary: Box<dyn InferenceClient>,
        fallback_model: &str,
        fallback: Box<dyn InferenceClient>,
    ) -> Self {
        Self {
            primary_model: primary_model.to_owned(),
            primary,
            fallback_model: fallback_model.to_owned(),
            fallback,
        }
    }

    /// The caller's request asked once: retries are the fallback's job.
    fn primary_config(config: &GenerationConfig) -> GenerationConfig {
        let mut config = config.clone();
        config.retry_config = Some(RetryConfig {
            max_retries: 0,
            ..config.retry_config.unwrap_or_default()
        });
        config
    }

    fn hand_over(&self, error: &Error) -> bool {
        if !provider_failed(error) {
            return false;
        }
        mark_down(&self.primary_model);
        tracing::warn!(
            model = %self.primary_model,
            fallback = %self.fallback_model,
            error = %error,
            "inference.primary_failed; asking the fallback"
        );
        true
    }

    /// The primary's stream once its first chunk is in hand, or why it never began.
    async fn primary_start(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<ChunkStream> {
        let config = Self::primary_config(config);
        let start = async {
            let mut chunks = self.primary.connect_and_listen(messages, &config).await?;
            match chunks.next().await {
                Some(Ok(first)) => {
                    Ok(Box::pin(stream::once(async { Ok(first) }).chain(chunks)) as ChunkStream)
                }
                Some(Err(error)) => Err(error),
                None => Ok(Box::pin(stream::empty()) as ChunkStream),
            }
        };
        tokio::time::timeout(PRIMARY_START, start)
            .await
            .unwrap_or_else(|_| {
                Err(Error::Inference(format!(
                    "no first chunk within {}s",
                    PRIMARY_START.as_secs()
                )))
            })
    }
}

#[async_trait]
impl InferenceClient for FailoverClient {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        if primary_up(&self.primary_model) {
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
    ) -> Result<ChunkStream> {
        if primary_up(&self.primary_model) {
            match self.primary_start(messages, config).await {
                Ok(chunks) => return Ok(chunks),
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    enum Behaviour {
        Answer,
        Fail(fn() -> Error),
        Hang,
    }

    struct Fake {
        name: &'static str,
        behaviour: Behaviour,
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
            match self.behaviour {
                Behaviour::Fail(error) => Err(error()),
                _ => Ok(GenerationResponse::text(self.name)),
            }
        }

        async fn connect_and_listen(
            &self,
            _messages: &[ConversationMessage],
            _config: &GenerationConfig,
        ) -> Result<ChunkStream> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let (behaviour, name) = (self.behaviour, self.name);
            Ok(match behaviour {
                Behaviour::Hang => Box::pin(stream::pending()),
                Behaviour::Fail(error) => Box::pin(stream::once(async move { Err(error()) })),
                Behaviour::Answer => Box::pin(stream::once(async move {
                    Ok(StreamChunk::Content(name.to_owned()))
                })),
            })
        }

        fn provider(&self) -> Provider {
            Provider::OpenAI
        }
    }

    /// A client whose primary behaves as given, under a model name no other test shares
    /// (the cooldown is process-wide, keyed by model string).
    fn pair(
        primary_model: &str,
        behaviour: Behaviour,
    ) -> (FailoverClient, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let (primary_calls, fallback_calls) = (Arc::default(), Arc::default());
        let client = FailoverClient::new(
            primary_model,
            Box::new(Fake {
                name: "primary",
                behaviour,
                calls: Arc::clone(&primary_calls),
            }),
            "openrouter:m",
            Box::new(Fake {
                name: "fallback",
                behaviour: Behaviour::Answer,
                calls: Arc::clone(&fallback_calls),
            }),
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

    fn calls(primary: &AtomicUsize, fallback: &AtomicUsize) -> (usize, usize) {
        (
            primary.load(Ordering::SeqCst),
            fallback.load(Ordering::SeqCst),
        )
    }

    #[tokio::test]
    async fn a_healthy_primary_answers_and_the_fallback_is_never_asked() {
        let (client, primary, fallback) = pair("opencode:healthy", Behaviour::Answer);
        assert_eq!(first_content(&client).await.unwrap(), "primary");
        let response = client
            .get_generation(&[], &GenerationConfig::default())
            .await
            .unwrap();
        assert_eq!(response.content, "primary");
        assert_eq!(calls(&primary, &fallback), (2, 0));
    }

    #[tokio::test]
    async fn an_outage_hands_over_and_the_primary_is_passed_over_while_it_cools_down() {
        let (client, primary, fallback) = pair(
            "opencode:outage",
            Behaviour::Fail(|| Error::Inference("503 Service Unavailable".into())),
        );
        assert_eq!(first_content(&client).await.unwrap(), "fallback");
        assert_eq!(first_content(&client).await.unwrap(), "fallback");
        // A new client for the same primary (the next call) remembers the outage too.
        let (next, next_primary, _) = pair("opencode:outage", Behaviour::Answer);
        assert_eq!(first_content(&next).await.unwrap(), "fallback");
        assert_eq!(calls(&primary, &fallback), (1, 2));
        assert_eq!(next_primary.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_primary_that_never_starts_answering_is_given_up_on_in_time() {
        let (client, _, fallback) = pair("opencode:hang", Behaviour::Hang);
        let started = tokio::time::Instant::now();
        assert_eq!(first_content(&client).await.unwrap(), "fallback");
        assert_eq!(started.elapsed(), PRIMARY_START);
        assert_eq!(fallback.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_revoked_key_or_spent_balance_hands_over_too() {
        let (client, _, _) = pair(
            "opencode:revoked",
            Behaviour::Fail(|| Error::NonRetryable("401 Unauthorized: bad key".into())),
        );
        assert_eq!(first_content(&client).await.unwrap(), "fallback");
        let (client, _, _) = pair(
            "opencode:spent",
            Behaviour::Fail(|| Error::Inference("402 Payment Required: balance".into())),
        );
        assert_eq!(first_content(&client).await.unwrap(), "fallback");
    }

    #[tokio::test]
    async fn a_request_rejected_on_its_merits_returns_to_the_caller() {
        let (client, _, fallback) = pair(
            "opencode:too-long",
            Behaviour::Fail(|| Error::ContextLengthExceeded("too long".into())),
        );
        assert!(matches!(
            first_content(&client).await,
            Err(Error::ContextLengthExceeded(_))
        ));
        let (bad, _, bad_fallback) = pair(
            "opencode:malformed",
            Behaviour::Fail(|| Error::NonRetryable("400 Bad Request: schema".into())),
        );
        assert!(
            bad.get_generation(&[], &GenerationConfig::default())
                .await
                .is_err()
        );
        assert_eq!(calls(&fallback, &bad_fallback), (0, 0));
        assert!(primary_up("opencode:too-long") && primary_up("opencode:malformed"));
    }

    #[test]
    fn only_distinct_chat_completions_models_pair() {
        let opencode = "opencode:deepseek-v4.1-flash@reasoning=medium";
        let us = "openrouter:deepseek/deepseek-v4.1-flash-1017@server_url=https://us.openrouter.ai/api/v1";
        assert!(pairable(opencode, us));
        assert!(!pairable("anthropic:claude-x", us));
        assert!(!pairable(us, us));
    }
}
