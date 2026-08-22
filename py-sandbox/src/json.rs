use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::invalid_argument;

pub fn from_python<T: DeserializeOwned>(value: &Bound<'_, PyAny>) -> PyResult<T> {
    pythonize::depythonize(value).map_err(|error| invalid_argument(error.to_string()))
}

pub fn to_python<T: Serialize>(py: Python<'_>, value: &T) -> PyResult<PyObject> {
    pythonize::pythonize(py, value)
        .map(Bound::unbind)
        .map_err(|error| PyRuntimeError::new_err(format!("serialize: {error}")))
}
