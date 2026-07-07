//! Streaming: a `StreamHandle` that yields engine `ResponseStreamEvent`s as a
//! JS-friendly discriminated union. The Rust stream is driven by a spawned task
//! that owns the runtime lock guard + the stream together (avoiding a
//! self-referential struct), forwarding events over an *unbounded* channel so
//! the driver never parks waiting for the consumer (which would pin the runtime
//! lock). `close()` aborts the driver to release the lock + provider request
//! promptly.

use std::sync::Arc;

use chevalier_core::types::{ResponsePart, ResponseStreamEvent};
use napi_derive::napi;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::AbortHandle;

use crate::types::ToolCallJs;

/// Serialize a value, or fall back to an `_serialization_error` marker so a
/// payload is never silently dropped to `null`.
pub(crate) fn to_value_or_marker<T: serde::Serialize>(v: T) -> serde_json::Value {
    serde_json::to_value(v)
        .unwrap_or_else(|e| serde_json::json!({ "_serialization_error": e.to_string() }))
}

/// A single streamed event. Discriminated by `type`:
/// `content` | `reasoning` | `signature` | `toolCall` | `toolPartial` | `usage` | `rateLimits` | `complete`.
#[napi(object)]
pub struct StreamEvent {
    #[napi(js_name = "type")]
    pub kind: String,
    /// Present for `content` / `reasoning` / `signature`.
    pub text: Option<String>,
    /// Present for `toolCall`.
    pub tool_call: Option<ToolCallJs>,
    /// Raw payload for `toolPartial` (partial args), `usage`, `rateLimits`, `complete` (full response).
    pub data: Option<serde_json::Value>,
}

impl From<ResponseStreamEvent> for StreamEvent {
    fn from(ev: ResponseStreamEvent) -> Self {
        let base = |kind: &str| StreamEvent {
            kind: kind.to_string(),
            text: None,
            tool_call: None,
            data: None,
        };
        match ev {
            ResponseStreamEvent::Output(part) => match part {
                ResponsePart::Text { text } => StreamEvent {
                    text: Some(text),
                    ..base("content")
                },
                ResponsePart::Reasoning { text } => StreamEvent {
                    text: Some(text),
                    ..base("reasoning")
                },
                ResponsePart::Signature { value } => StreamEvent {
                    text: Some(value),
                    ..base("signature")
                },
                ResponsePart::Tool { call } => StreamEvent {
                    tool_call: Some(ToolCallJs::from(&call)),
                    ..base("toolCall")
                },
            },
            ResponseStreamEvent::ToolPartial(v) => StreamEvent {
                data: Some(v),
                ..base("toolPartial")
            },
            ResponseStreamEvent::Usage(u) => StreamEvent {
                data: Some(to_value_or_marker(u)),
                ..base("usage")
            },
            ResponseStreamEvent::RateLimits(rate_limits) => StreamEvent {
                data: Some(to_value_or_marker(rate_limits)),
                ..base("rateLimits")
            },
            ResponseStreamEvent::Complete(r) => StreamEvent {
                data: Some(to_value_or_marker(r)),
                ..base("complete")
            },
        }
    }
}

/// Async cursor over a streaming run. Call `next()` until it returns `null`, or
/// `close()` to cancel early. The TS layer wraps this as an async iterator and
/// calls `close()` in a `finally`.
#[napi]
pub struct StreamHandle {
    pub(crate) rx: Arc<Mutex<UnboundedReceiver<Result<StreamEvent, String>>>>,
    pub(crate) abort: AbortHandle,
}

#[napi]
impl StreamHandle {
    /// The next event, or `null` when the stream is exhausted. Throws if the
    /// underlying run errored.
    #[napi]
    pub async fn next(&self) -> napi::Result<Option<StreamEvent>> {
        let rx = self.rx.clone();
        let mut guard = rx.lock().await;
        match guard.recv().await {
            Some(Ok(ev)) => Ok(Some(ev)),
            Some(Err(msg)) => Err(napi::Error::new(napi::Status::GenericFailure, msg)),
            None => Ok(None),
        }
    }

    /// Cancel the stream: aborts the driving task, releasing the runtime lock and
    /// the provider request. Idempotent.
    #[napi]
    pub fn close(&self) {
        self.abort.abort();
    }
}

impl Drop for StreamHandle {
    /// Safety net: if the handle is dropped/GC'd without `close()` (e.g. an
    /// abandoned iterator, or a stalled provider where the channel-close wouldn't
    /// be observed), abort the driver so the runtime lock + provider connection
    /// are released immediately rather than lingering.
    fn drop(&mut self) {
        self.abort.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chevalier_core::types::{ProviderRateLimit, ProviderRateLimitScope};

    #[test]
    fn test_rate_limits_event_is_js_visible() {
        let event = StreamEvent::from(ResponseStreamEvent::RateLimits(vec![ProviderRateLimit {
            scope: ProviderRateLimitScope::Session,
            used_percent: 42,
            window_minutes: 300,
            resets_at_epoch_sec: 1_783_504_800,
        }]));

        assert_eq!(event.kind, "rateLimits");
        assert_eq!(
            event.data,
            Some(serde_json::json!([{
                "scope": "session",
                "usedPercent": 42,
                "windowMinutes": 300,
                "resetsAtEpochSec": 1_783_504_800_u64
            }]))
        );
    }
}
