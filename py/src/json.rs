use pyo3::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::errors::{invalid_argument, runtime_error};

pub fn from_python<T: DeserializeOwned>(value: &Bound<'_, PyAny>) -> PyResult<T> {
    pythonize::depythonize(value).map_err(|error| invalid_argument(error.to_string()))
}

pub fn value_from_python(value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    from_python(value)
}

pub fn to_python<T: Serialize>(py: Python<'_>, value: &T) -> PyResult<PyObject> {
    pythonize::pythonize(py, value)
        .map(Bound::unbind)
        .map_err(|error| runtime_error(format!("serialize: {error}")))
}
