//! An answer with nothing in it is a failed attempt.
//!
//! A provider can take a request, answer 200 and still say nothing: no text, no reasoning, no
//! tool call. Handing that on fails the caller's task with nothing to show, though asking
//! again, or asking another model, usually answers. `RequireAnswer` wraps each model's client
//! so that an empty answer is an error, asked again under the caller's `retry_config`. When
//! those retries run out the error leaves the client before anything was answered, so it
//! takes the path of any other failure before answering: `FailoverClient` hands the request
//! to the fallback model, and a host with its own next model moves to it.
//!
//! A stream is judged by its first chunk that belongs to the answer. Until one arrives the
//! chunks that only describe the response (its id, its usage, rate-limit windows, the
//! provider's raw output items) are held back. An empty attempt therefore emits nothing:
//! a retry shows nothing twice and does not splice two responses' provider items into one.
//! Once a chunk of the answer arrives the attempt owns the answer, as in `FailoverClient`; if
//! it then completes nothing, the stream fails as it ends (`call_llm_stream`).

use std::pin::Pin;

use async_trait::async_trait;
use futures::{Stream, StreamExt, stream};

use crate::error::{Error, Result};
use crate::providers::{
    GenerationConfig, GenerationResponse, InferenceClient, StreamChunk, TraceCallback,
};
use crate::retry::retry_with_backoff_when;
use crate::types::Provider;
use crate::utils::ConversationMessage;

pub(super) const EMPTY_ANSWER: &str = "Provider returned an empty assistant response";

pub(super) fn empty_answer() -> Error {
    Error::Inference(EMPTY_ANSWER.to_owned())
}

fn is_empty_answer(error: &Error) -> bool {
    matches!(error, Error::Inference(message) if message == EMPTY_ANSWER)
}

/// Whether `chunk` is part of the answer, or something the attempt cannot take back (a
/// steer the provider applied), rather than a description of the response.
fn begins_answer(chunk: &StreamChunk) -> bool {
    !matches!(
        chunk,
        StreamChunk::ResponseId(_)
            | StreamChunk::Usage { .. }
            | StreamChunk::RateLimits(_)
            | StreamChunk::ResponseItems(_)
    )
}

type ChunkStream = Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>;

pub(super) struct RequireAnswer(Box<dyn InferenceClient>);

impl RequireAnswer {
    pub(super) fn wrap(client: Box<dyn InferenceClient>) -> Box<dyn InferenceClient> {
        Box::new(Self(client))
    }

    /// One attempt's stream, from its held-back chunks on, once its answer has begun.
    async fn start(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<ChunkStream> {
        let mut chunks = self.0.connect_and_listen(messages, config).await?;
        let mut held = Vec::new();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            let begun = begins_answer(&chunk);
            held.push(Ok(chunk));
            if begun {
                return Ok(Box::pin(stream::iter(held).chain(chunks)));
            }
        }
        Err(empty_answer())
    }
}

#[async_trait]
impl InferenceClient for RequireAnswer {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        let retries = config.retry_config.clone().unwrap_or_default();
        retry_with_backoff_when(retries, is_empty_answer, || async {
            let generation = self.0.get_generation(messages, config).await?;
            if generation.response.output.is_empty() {
                return Err(empty_answer());
            }
            Ok(generation)
        })
        .await
    }

    async fn connect_and_listen(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<ChunkStream> {
        let retries = config.retry_config.clone().unwrap_or_default();
        retry_with_backoff_when(retries, is_empty_answer, || self.start(messages, config)).await
    }

    fn provider(&self) -> Provider {
        self.0.provider()
    }

    fn supports_native_tools(&self) -> bool {
        self.0.supports_native_tools()
    }

    fn set_trace_callback(&mut self, callback: TraceCallback) {
        self.0.set_trace_callback(callback);
    }
}

#[cfg(test)]
mod tests {
    //! Through `call_llm` and `call_llm_stream`, against Chat Completions endpoints on
    //! loopback that answer empty when told to.

    use super::*;
    use crate::retry::RetryConfig;
    use crate::runtime::inference::{CallResult, call_llm, call_llm_stream};
    use crate::types::ResponseStreamEvent;
    use serde_json::{Value, json};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// An endpoint's replies to its requests in turn, the last repeating: `None` is a 200
    /// whose assistant message is empty.
    type Replies = Vec<Option<&'static str>>;

    async fn read_body(socket: &mut TcpStream) -> Value {
        let mut data = Vec::new();
        let mut buffer = [0; 8192];
        loop {
            let read = socket.read(&mut buffer).await.unwrap();
            data.extend_from_slice(&buffer[..read]);
            let text = String::from_utf8_lossy(&data);
            if let Some(end) = text.find("\r\n\r\n") {
                let length = text[..end]
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                if data.len() >= end + 4 + length || read == 0 {
                    return serde_json::from_slice(&data[end + 4..]).unwrap_or(Value::Null);
                }
            }
            assert!(read > 0, "request ended before its headers");
        }
    }

    fn streamed(id: &str, reply: Option<&str>) -> String {
        let opening = json!({"id": id, "choices": [{"index": 0,
            "delta": {"role": "assistant", "content": reply.unwrap_or("")}, "finish_reason": null}]});
        let closing = json!({"id": id, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1}});
        format!("data: {opening}\n\ndata: {closing}\n\ndata: [DONE]\n\n")
    }

    fn whole(id: &str, reply: Option<&str>) -> String {
        json!({"id": id, "choices": [{"index": 0,
            "message": {"role": "assistant", "content": reply}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1}})
        .to_string()
    }

    /// A model string for a new endpoint, and how many requests it has served.
    async fn endpoint(replies: Replies) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model = format!(
            "custom-openai:m@server_url=http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let served = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&served);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let turn = counter.fetch_add(1, SeqCst);
                let reply = replies[turn.min(replies.len() - 1)];
                let id = format!("gen-{}", turn + 1);
                let (kind, body) = if read_body(&mut socket).await["stream"] == true {
                    ("text/event-stream", streamed(&id, reply))
                } else {
                    ("application/json", whole(&id, reply))
                };
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(format!("{head}{body}").as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        (model, served)
    }

    fn retries() -> RetryConfig {
        RetryConfig {
            max_retries: 2,
            initial_backoff: Duration::from_millis(1),
            ..RetryConfig::default()
        }
    }

    async fn ask(model: &str, fallback: Option<&str>) -> Result<CallResult> {
        call_llm(
            Some("hi"),
            model,
            fallback,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(retries()),
            None,
            None,
            Default::default(),
            None,
        )
        .await
    }

    async fn ask_streaming(
        model: &str,
        fallback: Option<&str>,
    ) -> Result<Vec<ResponseStreamEvent>> {
        let events = call_llm_stream(
            Some("hi"),
            model,
            fallback,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(retries()),
            None,
            None,
            Default::default(),
            Default::default(),
            None,
        )
        .await?;
        events.collect::<Vec<_>>().await.into_iter().collect()
    }

    fn answered(events: &[ResponseStreamEvent]) -> String {
        events
            .iter()
            .find_map(|event| match event {
                ResponseStreamEvent::Complete(response) => Some(response.text()),
                _ => None,
            })
            .expect("the stream completes")
    }

    fn served(counters: &[&AtomicUsize]) -> Vec<usize> {
        counters.iter().map(|count| count.load(SeqCst)).collect()
    }

    #[tokio::test]
    async fn an_empty_answer_is_asked_again_and_only_the_answer_reaches_the_caller() {
        let (model, requests) = endpoint(vec![None, Some("hello")]).await;
        let events = ask_streaming(&model, None).await.unwrap();
        assert_eq!(answered(&events), "hello");
        // The empty attempt's id and usage were held back and dropped with it.
        let ids: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                ResponseStreamEvent::ResponseId(id) => Some(id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(ids, ["gen-2"]);
        let usages = events
            .iter()
            .filter(|event| matches!(event, ResponseStreamEvent::Usage(_)))
            .count();
        assert_eq!(usages, 1);
        assert_eq!(served(&[&requests]), [2]);

        let (model, requests) = endpoint(vec![None, Some("hello")]).await;
        assert_eq!(ask(&model, None).await.unwrap().response.text(), "hello");
        assert_eq!(served(&[&requests]), [2]);
    }

    #[tokio::test]
    async fn a_primary_that_answers_empty_hands_the_request_to_the_fallback() {
        let (primary, primary_requests) = endpoint(vec![None]).await;
        let (fallback, fallback_requests) = endpoint(vec![Some("from the fallback")]).await;
        let events = ask_streaming(&primary, Some(&fallback)).await.unwrap();
        assert_eq!(answered(&events), "from the fallback");
        // The primary is asked once under a fallback, as for any failure before answering.
        assert_eq!(served(&[&primary_requests, &fallback_requests]), [1, 1]);

        let (primary, primary_requests) = endpoint(vec![None]).await;
        let (fallback, fallback_requests) = endpoint(vec![Some("from the fallback")]).await;
        let answer = ask(&primary, Some(&fallback)).await.unwrap();
        assert_eq!(answer.response.text(), "from the fallback");
        assert_eq!(served(&[&primary_requests, &fallback_requests]), [1, 1]);
    }

    #[tokio::test]
    async fn the_call_fails_only_once_every_attempt_answered_empty() {
        let attempts = retries().max_retries as usize + 1;
        let (model, requests) = endpoint(vec![None]).await;
        let error = ask_streaming(&model, None).await.unwrap_err();
        assert!(error.to_string().contains(EMPTY_ANSWER), "{error}");
        let error = ask(&model, None)
            .await
            .err()
            .expect("an empty answer fails");
        assert!(error.to_string().contains(EMPTY_ANSWER), "{error}");
        assert_eq!(served(&[&requests]), [2 * attempts]);

        let (primary, primary_requests) = endpoint(vec![None]).await;
        let (fallback, fallback_requests) = endpoint(vec![None]).await;
        let error = ask_streaming(&primary, Some(&fallback)).await.unwrap_err();
        assert!(error.to_string().contains(EMPTY_ANSWER), "{error}");
        assert_eq!(
            served(&[&primary_requests, &fallback_requests]),
            [1, attempts]
        );
    }
}
