//! Error types for Chevalier
//!
//! This module defines the error hierarchy for the Chevalier framework,
//! distinguishing between retryable and non-retryable errors.

use thiserror::Error;

/// Result type alias for Chevalier operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for Chevalier operations
#[derive(Error, Debug)]
pub enum Error {
    /// Retryable inference error (network issues, rate limits, 5xx errors)
    #[error("Inference error: {0}")]
    Inference(String),

    /// Context length exceeded (prompt + completion too long)
    #[error("Context length exceeded: {0}")]
    ContextLengthExceeded(String),

    /// Maximum retries exhausted (includes last error for context)
    #[error("Retries exceeded: {0}")]
    RetriesExceeded(String),

    /// Non-retryable error (validation, 4xx errors)
    #[error("Non-retryable error: {0}")]
    NonRetryable(String),

    /// Codex subscription usage limit reached.
    #[error("{message}")]
    CodexUsageLimit {
        message: String,
        reached_type: Option<String>,
        resets_at_epoch_sec: Option<u64>,
    },

    /// Parse error when parsing LLM output
    #[error("Parse error: {0}")]
    Parse(String),

    /// Validation error for structured outputs
    #[error("Validation error: {0}")]
    Validation(String),

    /// Tool not found in registry
    #[error("Tool not found: {0}")]
    ToolNotFound(String),

    /// Network error from reqwest
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// UTF-8 encoding error
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    /// UTF-8 conversion error from String
    #[error("UTF-8 conversion error: {0}")]
    FromUtf8(#[from] std::string::FromUtf8Error),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid provider specified
    #[error("Invalid provider: {0}")]
    InvalidProvider(String),

    /// Missing API key
    #[error("Missing API key: {0}")]
    MissingApiKey(String),

    /// Runtime not used (validation error)
    #[error("Runtime was not used in agentic function")]
    RuntimeNotUsed,
}

impl Error {
    /// Check if error is retryable (suitable for backoff retry)
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::Inference(_) | Error::Network(_) | Error::Io(_))
    }

    /// Check if error indicates retries were exceeded
    pub fn is_retries_exceeded(&self) -> bool {
        matches!(self, Error::RetriesExceeded(_))
    }

    /// Check if error indicates incomplete/partial data (for streaming)
    pub fn is_incomplete(&self) -> bool {
        matches!(self, Error::Parse(msg) if msg.contains("incomplete"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::Inference("rate limit".to_string());
        assert_eq!(err.to_string(), "Inference error: rate limit");
    }

    #[test]
    fn test_is_retryable() {
        assert!(Error::Inference("test".to_string()).is_retryable());
        assert!(Error::Io(std::io::Error::other("test")).is_retryable());
        assert!(!Error::NonRetryable("test".to_string()).is_retryable());
        assert!(!Error::Validation("test".to_string()).is_retryable());
    }

    #[test]
    fn test_is_retries_exceeded() {
        assert!(Error::RetriesExceeded("test".to_string()).is_retries_exceeded());
        assert!(!Error::Inference("test".to_string()).is_retries_exceeded());
    }

    #[test]
    fn test_is_incomplete() {
        assert!(Error::Parse("incomplete data".to_string()).is_incomplete());
        assert!(!Error::Parse("malformed json".to_string()).is_incomplete());
    }

    #[test]
    fn test_from_conversions() {
        let json_err: Error = serde_json::from_str::<i32>("invalid").unwrap_err().into();
        assert!(matches!(json_err, Error::Json(_)));

        let io_err: Error = std::io::Error::other("test").into();
        assert!(matches!(io_err, Error::Io(_)));
    }
}
