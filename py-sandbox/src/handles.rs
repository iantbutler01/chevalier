use std::sync::Arc;

use chevalier_sandbox::{
    EventStream, ExecEvent, ExecInput, ExecInputSender, ForwardHandle as EngineForwardHandle,
    ShellEvent, ShellInput,
};
use futures::StreamExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use tokio::sync::Mutex;

use crate::error::sandbox_error;

fn exec_event_to_python(py: Python<'_>, event: ExecEvent) -> PyResult<PyObject> {
    let output = PyDict::new(py);
    match event {
        ExecEvent::Stdout(data) => {
            output.set_item("type", "stdout")?;
            output.set_item("data", PyBytes::new(py, &data))?;
        }
        ExecEvent::Stderr(data) => {
            output.set_item("type", "stderr")?;
            output.set_item("data", PyBytes::new(py, &data))?;
        }
        ExecEvent::Exit(code) => {
            output.set_item("type", "exit")?;
            output.set_item("code", code)?;
        }
        ExecEvent::Timeout => output.set_item("type", "timeout")?,
    }
    Ok(output.into_any().unbind())
}

fn shell_event_to_python(py: Python<'_>, event: ShellEvent) -> PyResult<PyObject> {
    let output = PyDict::new(py);
    match event {
        ShellEvent::Output(data) => {
            output.set_item("type", "output")?;
            output.set_item("data", PyBytes::new(py, &data))?;
        }
        ShellEvent::Exit(code) => {
            output.set_item("type", "exit")?;
            output.set_item("code", code)?;
        }
    }
    Ok(output.into_any().unbind())
}

#[pyclass(module = "chevalier_sandbox.chevalier_sandbox")]
pub struct ExecHandle {
    pub(crate) input: ExecInputSender,
    pub(crate) events: Arc<Mutex<EventStream<ExecEvent>>>,
}

#[pymethods]
impl ExecHandle {
    fn write<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let input = self.input.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            input
                .send(ExecInput::Data(data))
                .await
                .map_err(|_| PyRuntimeError::new_err("exec stdin closed"))
        })
    }

    fn eof<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let input = self.input.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            input
                .send(ExecInput::Eof)
                .await
                .map_err(|_| PyRuntimeError::new_err("exec stdin closed"))
        })
    }

    fn signal<'py>(&self, py: Python<'py>, sig: i32) -> PyResult<Bound<'py, PyAny>> {
        let input = self.input.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            input
                .send(ExecInput::Signal(sig))
                .await
                .map_err(|_| PyRuntimeError::new_err("exec stdin closed"))
        })
    }

    fn resize<'py>(&self, py: Python<'py>, cols: u32, rows: u32) -> PyResult<Bound<'py, PyAny>> {
        let input = self.input.clone();
        let dimension = |value| u16::try_from(value).unwrap_or(u16::MAX);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            input
                .send(ExecInput::Resize {
                    cols: dimension(cols),
                    rows: dimension(rows),
                })
                .await
                .map_err(|_| PyRuntimeError::new_err("exec stdin closed"))
        })
    }

    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let events = self.events.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut events = events.lock().await;
            match events.next().await {
                Some(Ok(event)) => Python::with_gil(|py| exec_event_to_python(py, event).map(Some)),
                Some(Err(error)) => Err(sandbox_error(error)),
                None => Ok(None),
            }
        })
    }
}

#[pyclass(module = "chevalier_sandbox.chevalier_sandbox")]
pub struct ShellHandle {
    pub(crate) input: tokio::sync::mpsc::Sender<ShellInput>,
    pub(crate) events: Arc<Mutex<EventStream<ShellEvent>>>,
}

#[pymethods]
impl ShellHandle {
    fn write<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let input = self.input.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            input
                .send(ShellInput::Data(data))
                .await
                .map_err(|_| PyRuntimeError::new_err("shell stdin closed"))
        })
    }

    fn eof<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let input = self.input.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            input
                .send(ShellInput::Eof)
                .await
                .map_err(|_| PyRuntimeError::new_err("shell stdin closed"))
        })
    }

    fn resize<'py>(&self, py: Python<'py>, cols: u32, rows: u32) -> PyResult<Bound<'py, PyAny>> {
        let input = self.input.clone();
        let dimension = |value| u16::try_from(value).unwrap_or(u16::MAX).max(1);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            input
                .send(ShellInput::Resize {
                    cols: dimension(cols),
                    rows: dimension(rows),
                })
                .await
                .map_err(|_| PyRuntimeError::new_err("shell stdin closed"))
        })
    }

    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let events = self.events.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut events = events.lock().await;
            match events.next().await {
                Some(Ok(event)) => {
                    Python::with_gil(|py| shell_event_to_python(py, event).map(Some))
                }
                Some(Err(error)) => Err(sandbox_error(error)),
                None => Ok(None),
            }
        })
    }
}

#[pyclass(module = "chevalier_sandbox.chevalier_sandbox")]
pub struct ForwardHandle {
    inner: Arc<EngineForwardHandle>,
}

impl ForwardHandle {
    pub(crate) fn new(inner: EngineForwardHandle) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[pymethods]
impl ForwardHandle {
    #[getter]
    fn guest_port(&self) -> u32 {
        self.inner.guest_port.into()
    }

    #[getter]
    fn host_port(&self) -> u32 {
        self.inner.host_port.into()
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.close().await.map_err(sandbox_error)
        })
    }
}
