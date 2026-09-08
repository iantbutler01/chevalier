use std::sync::Arc;

use chevalier_core::types::ResponseStreamEvent;
use pyo3::prelude::*;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::AbortHandle;

use crate::errors::stream_error;
use crate::types::stream_event_to_python;

#[pyclass(module = "chevalier.chevalier")]
pub struct StreamHandle {
    pub(crate) control: Option<chevalier_core::providers::responses_control::ResponsesControl>,
    pub(crate) receiver: Arc<Mutex<UnboundedReceiver<Result<ResponseStreamEvent, String>>>>,
    pub(crate) abort: AbortHandle,
}

#[pymethods]
impl StreamHandle {
    fn steer<'py>(&self, py: Python<'py>, input: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let control = self.control.clone().ok_or_else(|| {
            stream_error("Stream control requires an explicitly enabled Responses WebSocket".into())
        })?;
        let input = crate::json::value_from_python(input)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            control
                .send("response.steer", input)
                .await
                .map_err(crate::errors::to_py_err)
        })
    }

    fn continue_response<'py>(
        &self,
        py: Python<'py>,
        input: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let control = self.control.clone().ok_or_else(|| {
            stream_error("Stream control requires an explicitly enabled Responses WebSocket".into())
        })?;
        let input = crate::json::value_from_python(input)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            control
                .send("response.create", input)
                .await
                .map_err(crate::errors::to_py_err)
        })
    }

    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let receiver = self.receiver.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut receiver = receiver.lock().await;
            match receiver.recv().await {
                Some(Ok(event)) => {
                    Python::with_gil(|py| stream_event_to_python(py, event).map(Some))
                }
                Some(Err(message)) => Err(stream_error(message)),
                None => Ok(None),
            }
        })
    }

    fn close(&self) {
        self.abort.abort();
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.abort.abort();
    }
}
