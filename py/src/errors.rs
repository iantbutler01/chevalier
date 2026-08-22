use chevalier_core::error::Error as EngineError;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

pyo3::create_exception!(chevalier, ChevalierError, PyException);

pub fn error_code(error: &EngineError) -> &'static str {
    match error {
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
    }
}

pub fn format_error(error: &EngineError) -> String {
    format!(
        "[{} retryable={}] {error}",
        error_code(error),
        error.is_retryable()
    )
}

pub fn to_py_err(error: EngineError) -> PyErr {
    let code = error_code(&error);
    let retryable = error.is_retryable();
    let exception = ChevalierError::new_err(error.to_string());
    Python::with_gil(|py| {
        let value = exception.value(py);
        let _ = value.setattr("code", code);
        let _ = value.setattr("retryable", retryable);
        let _ = value.setattr("output", py.None());
    });
    exception
}

pub fn stream_error(message: String) -> PyErr {
    let (code, retryable, display_message) =
        parse_formatted_error(&message).unwrap_or(("ERROR", false, message.as_str()));
    let exception = ChevalierError::new_err(display_message.to_string());
    Python::with_gil(|py| {
        let value = exception.value(py);
        let _ = value.setattr("code", code);
        let _ = value.setattr("retryable", retryable);
        let _ = value.setattr("output", py.None());
    });
    exception
}

fn parse_formatted_error(message: &str) -> Option<(&str, bool, &str)> {
    let rest = message.strip_prefix('[')?;
    let (prefix, message) = rest.split_once("] ")?;
    let (code, retryable) = prefix.split_once(" retryable=")?;
    Some((code, retryable == "true", message))
}

pub fn invalid_argument(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

pub fn runtime_error(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

pub fn contextual_error(prefix: &str, error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("{prefix}: {error}"))
}
