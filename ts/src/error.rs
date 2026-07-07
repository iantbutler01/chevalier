//! Map the Chevalier engine error into a `napi::Error` that carries a stable
//! `code` and a `retryable` hint (encoded in the message for now; a richer
//! error class can come later).

use chevalier_core::error::Error as EngineError;

/// Stable string code for each engine error variant — surfaced to JS.
pub fn error_code(e: &EngineError) -> &'static str {
    match e {
        EngineError::Inference(_) => "INFERENCE",
        EngineError::ContextLengthExceeded(_) => "CONTEXT_LENGTH_EXCEEDED",
        EngineError::RetriesExceeded(_) => "RETRIES_EXCEEDED",
        EngineError::NonRetryable(_) => "NON_RETRYABLE",
        EngineError::CodexUsageLimit { .. } => "CODEX_USAGE_LIMIT",
        EngineError::Parse(_) => "PARSE",
        EngineError::Validation(_) => "VALIDATION",
        EngineError::ToolNotFound(_) => "TOOL_NOT_FOUND",
        EngineError::Network(_) => "NETWORK",
        EngineError::Json(_) => "JSON",
        EngineError::Utf8(_) | EngineError::FromUtf8(_) => "UTF8",
        EngineError::Io(_) => "IO",
        EngineError::InvalidProvider(_) => "INVALID_PROVIDER",
        EngineError::MissingApiKey(_) => "MISSING_API_KEY",
        EngineError::RuntimeNotUsed => "RUNTIME_NOT_USED",
        // No catch-all: a new engine Error variant becomes a compile error here
        // (forcing an explicit code) rather than silently mapping to "ERROR".
    }
}

/// Format an engine error as `[CODE retryable=bool] message` (used for stream
/// errors carried over a channel).
pub fn format_error(e: &EngineError) -> String {
    format!("[{} retryable={}] {e}", error_code(e), e.is_retryable())
}

/// Convert an engine error into a `napi::Error`. The reason is prefixed with
/// `[CODE retryable=bool]` so JS callers can branch until a structured error
/// class is added.
pub fn to_napi(e: EngineError) -> napi::Error {
    let code = error_code(&e);
    let retryable = e.is_retryable();
    napi::Error::new(
        napi::Status::GenericFailure,
        format!("[{code} retryable={retryable}] {e}"),
    )
}
