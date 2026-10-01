//! Server-Sent Events (SSE) parsing utilities
//!
//! Parses SSE streams from LLM providers (Anthropic, OpenAI).
//! Uses eventsource-stream for proper SSE parsing with buffering.

use eventsource_stream::Eventsource;
use futures::stream::{Stream, StreamExt};
use std::pin::Pin;

use crate::error::{Error, Result};
use crate::utils::parse_json_value_strict_str;

/// Parse SSE stream from HTTP response
///
/// SSE format:
/// ```text
/// data: {"type": "message_start", ...}
///
/// data: {"type": "content_block_delta", ...}
///
/// data: [DONE]
/// ```
///
/// Uses eventsource-stream crate for proper SSE parsing with buffering
/// to handle JSON events that span network chunk boundaries.
pub fn parse_sse_stream(
    response: reqwest::Response,
) -> Pin<Box<dyn Stream<Item = Result<serde_json::Value>> + Send>> {
    let event_stream = response.bytes_stream().eventsource().take_while(|event| {
        // [DONE] ends the protocol even if the upstream keeps the body open.
        futures::future::ready(!matches!(event, Ok(event) if event.data == "[DONE]"))
    });

    let json_stream = event_stream.filter_map(|event_result| async move {
        match event_result {
            Ok(event) => {
                let data = event.data;

                // Parse JSON
                match parse_json_value_strict_str(&data) {
                    Ok(json) => Some(Ok(json)),
                    Err(e) => Some(Err(Error::Json(e))),
                }
            }
            Err(e) => Some(Err(Error::Inference(format!("SSE error: {}", e)))),
        }
    });

    Box::pin(json_stream)
}

// Note: parse_sse_stream is tested via integration tests since it requires
// a real HTTP response. The eventsource-stream crate handles the low-level
// SSE parsing and buffering.
