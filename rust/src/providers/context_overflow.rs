//! Whether a provider refused a request because it is too large for the model.
//!
//! Adapted from opencode's `packages/llm/src/provider-error.ts`
//! (https://github.com/sst/opencode), MIT License, Copyright (c) 2025 opencode.
//! Providers word this refusal differently and most send it as a plain 400, so
//! the message decides. A refusal for size is `Error::ContextLengthExceeded`:
//! retrying the same request cannot succeed, and the caller can only recover by
//! sending less.

use std::sync::LazyLock;

use regex::RegexSet;
use reqwest::StatusCode;

use crate::error::Error;

static OVERFLOW: LazyLock<RegexSet> = LazyLock::new(|| {
    RegexSet::new([
        r"(?i)prompt is too long",
        r"(?i)request_too_large",
        r"(?i)input is too long for requested model",
        r"(?i)exceeds the context window",
        r"(?i)exceeds (?:the )?(?:model'?s )?maximum context length(?: of [\d,]+ tokens?|\s*\([\d,]+\))",
        r"(?i)input token count.*exceeds the maximum",
        r"(?i)tokens in request more than max tokens allowed",
        r"(?i)maximum prompt length is \d+",
        r"(?i)reduce the length of the messages",
        r"(?i)maximum context length is \d+ tokens",
        r"(?i)exceeds (?:the )?maximum allowed input length of [\d,]+ tokens?",
        r"(?i)input \(\d+ tokens\) is longer than the model'?s context length \(\d+ tokens\)",
        r"(?i)exceeds the limit of \d+",
        r"(?i)exceeds the available context size",
        r"(?i)greater than the context length",
        r"(?i)context window exceeds limit",
        r"(?i)exceeded model token limit",
        r"(?i)context[_ ]length[_ ]exceeded",
        r"(?i)request entity too large",
        r"(?i)context length is only \d+ tokens",
        r"(?i)input length.*exceeds.*context length",
        r"(?i)prompt too long; exceeded (?:max )?context length",
        r"(?i)too large for model with \d+ maximum context length",
        r"(?i)prompt has [\d,]+ tokens?, but the configured context size is [\d,]+ tokens?",
        r"(?i)model_context_window_exceeded",
        r"(?i)too many tokens",
        r"(?i)token limit exceeded",
    ])
    .expect("context overflow patterns compile")
});

static NOT_OVERFLOW: LazyLock<RegexSet> = LazyLock::new(|| {
    RegexSet::new([
        r"(?i)^(throttling error|service unavailable):",
        r"(?i)rate limit",
        r"(?i)too many requests",
    ])
    .expect("exclusion patterns compile")
});

/// Whether a provider's error message says the request was too large.
pub fn is_overflow_message(message: &str) -> bool {
    !NOT_OVERFLOW.is_match(message) && OVERFLOW.is_match(message)
}

/// `ContextLengthExceeded` when an error response is a refusal for size: a 413,
/// a 400 or 413 with no body (how some gateways refuse an oversized request),
/// or a message saying so.
pub fn classify(status: StatusCode, body: &str) -> Option<Error> {
    let bodyless = body.trim().is_empty()
        && matches!(
            status,
            StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE
        );
    let overflow = status == StatusCode::PAYLOAD_TOO_LARGE
        || bodyless
        // A 429 is a rate limit however it is worded ("too many tokens per minute").
        || (status.is_client_error()
            && status != StatusCode::TOO_MANY_REQUESTS
            && is_overflow_message(body));
    overflow.then(|| Error::ContextLengthExceeded(format!("{status}: {body}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_for_size_is_context_length_exceeded() {
        let openrouter = r#"{"error":{"message":"This endpoint's maximum context length is 1048576 tokens. However, you requested about 1200000 tokens.","code":400}}"#;
        assert!(matches!(
            classify(StatusCode::BAD_REQUEST, openrouter),
            Some(Error::ContextLengthExceeded(_))
        ));
        let anthropic = r#"{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 210000 tokens > 200000 maximum"}}"#;
        assert!(classify(StatusCode::BAD_REQUEST, anthropic).is_some());
        assert!(classify(StatusCode::PAYLOAD_TOO_LARGE, "anything").is_some());
        assert!(classify(StatusCode::BAD_REQUEST, "").is_some());
    }

    #[test]
    fn other_refusals_are_left_alone() {
        assert!(
            classify(
                StatusCode::BAD_REQUEST,
                r#"{"error":"invalid tool schema"}"#
            )
            .is_none()
        );
        assert!(classify(StatusCode::TOO_MANY_REQUESTS, "too many tokens per minute").is_none());
        assert!(classify(StatusCode::BAD_REQUEST, "rate limit: too many tokens").is_none());
        assert!(classify(StatusCode::INTERNAL_SERVER_ERROR, "prompt is too long").is_none());
    }

    #[test]
    fn a_too_large_error_is_not_retried() {
        let error = classify(StatusCode::PAYLOAD_TOO_LARGE, "").unwrap();
        assert!(!error.is_retryable());
    }
}
