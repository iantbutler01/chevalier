use std::collections::HashMap;
use std::sync::Arc;

use chevalier_sandbox::{ForkOptions, Session as EngineSession};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::Mutex;

use crate::error::{invalid_argument, sandbox_error};
use crate::handles::{ExecHandle, ForwardHandle, ShellHandle};
use crate::json::{from_python, to_python};
use crate::options::{ExecOpts, ForkOpts, SessionSnapshotOpts, ShellOpts};
use crate::types::{
    HostPciInventory, PciDeviceAction, SessionCheckpoint, SessionDirectoryEntry, SessionSnapshot,
    vm_state_label,
};

#[pyclass(module = "chevalier_sandbox.chevalier_sandbox")]
pub struct Session {
    pub(crate) inner: EngineSession,
}

#[pymethods]
impl Session {
    #[getter]
    fn session_id(&self) -> String {
        self.inner.session_id().to_string()
    }

    #[getter]
    fn vm_id(&self) -> String {
        self.inner.vm_id().to_string()
    }

    #[pyo3(signature = (command, options=None))]
    fn exec<'py>(
        &self,
        py: Python<'py>,
        command: String,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = options
            .map(from_python::<ExecOpts>)
            .transpose()?
            .unwrap_or_default()
            .into();
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = session
                .exec(&command, options)
                .await
                .map_err(sandbox_error)?;
            Ok(ExecHandle {
                input: handle.input,
                events: Arc::new(Mutex::new(handle.events)),
            })
        })
    }

    #[pyo3(signature = (options=None))]
    fn shell<'py>(
        &self,
        py: Python<'py>,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = options
            .map(from_python::<ShellOpts>)
            .transpose()?
            .unwrap_or_default()
            .into();
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = session.shell(options).await.map_err(sandbox_error)?;
            Ok(ShellHandle {
                input: handle.input,
                events: Arc::new(Mutex::new(handle.events)),
            })
        })
    }

    fn read_file<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bytes = session.read_file(&path).await.map_err(sandbox_error)?;
            Python::with_gil(|py| Ok(PyBytes::new(py, &bytes).into_any().unbind()))
        })
    }

    fn list_dir<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let entries: Vec<_> = session
                .list_dir(&path)
                .await
                .map_err(sandbox_error)?
                .into_iter()
                .map(|entry| SessionDirectoryEntry {
                    name: entry.name,
                    is_dir: entry.is_dir,
                    is_symlink: entry.is_symlink,
                })
                .collect();
            Python::with_gil(|py| to_python(py, &entries))
        })
    }

    fn write_file<'py>(
        &self,
        py: Python<'py>,
        path: String,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session.write_file(&path, data).await.map_err(sandbox_error)
        })
    }

    #[pyo3(signature = (path, source_path, mode=None))]
    fn write_file_from_file<'py>(
        &self,
        py: Python<'py>,
        path: String,
        source_path: String,
        mode: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session
                .write_file_from_file(&path, &source_path, mode)
                .await
                .map_err(sandbox_error)
        })
    }

    #[pyo3(signature = (options=None))]
    fn fork<'py>(
        &self,
        py: Python<'py>,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = options
            .map(from_python::<ForkOpts>)
            .transpose()?
            .map(Into::into)
            .unwrap_or(ForkOptions {
                child_name: None,
                child_metadata: HashMap::new(),
                auto_start_child: true,
            });
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = session.fork(options).await.map_err(sandbox_error)?;
            Ok(Self {
                inner: result.child,
            })
        })
    }

    fn checkpoint<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let checkpoint = session.checkpoint(&name).await.map_err(sandbox_error)?;
            Python::with_gil(|py| to_python(py, &SessionCheckpoint { id: checkpoint.id }))
        })
    }

    fn restore_checkpoint<'py>(
        &self,
        py: Python<'py>,
        checkpoint_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let session = session
                .restore_checkpoint(&checkpoint_id)
                .await
                .map_err(sandbox_error)?;
            Ok(Self { inner: session })
        })
    }

    fn get_state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session
                .state()
                .await
                .map(vm_state_label)
                .map_err(sandbox_error)
        })
    }

    fn list_pci_devices<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inventory = session
                .list_pci_devices()
                .await
                .map(HostPciInventory::from)
                .map_err(sandbox_error)?;
            Python::with_gil(|py| to_python(py, &inventory))
        })
    }

    fn attach_pci_device<'py>(
        &self,
        py: Python<'py>,
        device_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let action = session
                .attach_pci_device(&device_id)
                .await
                .map(PciDeviceAction::from)
                .map_err(sandbox_error)?;
            Python::with_gil(|py| to_python(py, &action))
        })
    }

    fn detach_pci_device<'py>(
        &self,
        py: Python<'py>,
        device_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let action = session
                .detach_pci_device(&device_id)
                .await
                .map(PciDeviceAction::from)
                .map_err(sandbox_error)?;
            Python::with_gil(|py| to_python(py, &action))
        })
    }

    fn pause<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session
                .pause()
                .await
                .map(vm_state_label)
                .map_err(sandbox_error)
        })
    }

    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session
                .start()
                .await
                .map(vm_state_label)
                .map_err(sandbox_error)
        })
    }

    fn restart<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session
                .restart()
                .await
                .map(vm_state_label)
                .map_err(sandbox_error)
        })
    }

    fn resume<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session
                .resume()
                .await
                .map(vm_state_label)
                .map_err(sandbox_error)
        })
    }

    fn stop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session
                .stop()
                .await
                .map(vm_state_label)
                .map_err(sandbox_error)
        })
    }

    #[pyo3(signature = (options=None))]
    fn snapshot<'py>(
        &self,
        py: Python<'py>,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options: SessionSnapshotOpts =
            options.map(from_python).transpose()?.unwrap_or_default();
        let label = options.label.unwrap_or_else(|| "snapshot".to_string());
        let description = options.description.unwrap_or_default();
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let snapshot = session
                .snapshot(&label, &description)
                .await
                .map_err(sandbox_error)?;
            Python::with_gil(|py| {
                to_python(
                    py,
                    &SessionSnapshot {
                        id: snapshot.id,
                        name: snapshot.name,
                        label: snapshot.label,
                        description: snapshot.description,
                    },
                )
            })
        })
    }

    fn restore<'py>(&self, py: Python<'py>, snapshot_id: String) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session
                .restore(&snapshot_id)
                .await
                .map(vm_state_label)
                .map_err(sandbox_error)
        })
    }

    fn list_snapshots<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let snapshots: Vec<_> = session
                .list_snapshots()
                .await
                .map_err(sandbox_error)?
                .into_iter()
                .map(|snapshot| SessionSnapshot {
                    id: snapshot.id,
                    name: snapshot.name,
                    label: snapshot.label,
                    description: snapshot.description,
                })
                .collect();
            Python::with_gil(|py| to_python(py, &snapshots))
        })
    }

    fn delete_snapshot<'py>(
        &self,
        py: Python<'py>,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session
                .delete_snapshot(&snapshot_id)
                .await
                .map_err(sandbox_error)
        })
    }

    fn forward_port<'py>(&self, py: Python<'py>, guest_port: u32) -> PyResult<Bound<'py, PyAny>> {
        let guest_port = u16::try_from(guest_port)
            .map_err(|_| invalid_argument("guest_port must fit in uint16"))?;
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = session
                .forward_port(guest_port)
                .await
                .map_err(sandbox_error)?;
            Ok(ForwardHandle::new(handle))
        })
    }

    fn provider_preview_url<'py>(
        &self,
        py: Python<'py>,
        guest_port: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let guest_port = u16::try_from(guest_port)
            .map_err(|_| invalid_argument("guest_port must fit in uint16"))?;
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session
                .provider_preview_url(guest_port)
                .await
                .map_err(sandbox_error)
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session.close().await.map_err(sandbox_error)
        })
    }

    fn discard<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session.discard().await.map_err(sandbox_error)
        })
    }
}
