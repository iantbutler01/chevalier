use chevalier_sandbox::SandboxError;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

pub fn sandbox_error(error: SandboxError) -> PyErr {
    PyRuntimeError::new_err(format!("Sandbox: {error}"))
}

pub fn invalid_argument(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}
