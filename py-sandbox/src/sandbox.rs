use std::time::Duration;

use chevalier_sandbox::{
    ResourceLimits, Sandbox as EngineSandbox, SandboxConfig, SandboxProviderConfig,
};
use pyo3::prelude::*;

use crate::error::sandbox_error;
use crate::json::{from_python, to_python};
use crate::options::{
    SandboxConnectOptions, SessionOpts, opencomputer_config_from_options, positive_resource,
};
use crate::session::Session;
use crate::types::{DurableVolumeInfo, HostPciInventory, SessionInfo};

#[pyclass(module = "chevalier_sandbox.chevalier_sandbox")]
pub struct Sandbox {
    inner: EngineSandbox,
}

#[pymethods]
impl Sandbox {
    #[staticmethod]
    #[pyo3(signature = (endpoint, options=None))]
    fn connect<'py>(
        py: Python<'py>,
        endpoint: String,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options: SandboxConnectOptions =
            options.map(from_python).transpose()?.unwrap_or_default();
        let mut config = SandboxConfig {
            endpoint: endpoint.clone(),
            ..Default::default()
        };
        config.distributed_control = options
            .distributed_control
            .map(TryInto::try_into)
            .transpose()
            .map_err(sandbox_error)?;
        if let Some(token) = options.auth_token {
            config.auth_token = Some(token);
        }
        if let Some(token) = options.pci_access_token {
            config.pci_access_token = Some(token);
        }
        if let Some(milliseconds) = options.connect_timeout_ms {
            config.connect_timeout = Duration::from_millis(milliseconds as u64);
        }
        if let Some(image) = options.default_image {
            config.default_image = image;
        }
        if let Some(architecture) = options.default_architecture {
            config.default_architecture = Some(architecture);
        }
        config.default_resources = ResourceLimits {
            vcpu: positive_resource(options.default_vcpu, "default vCPU count")
                .map_err(sandbox_error)?
                .unwrap_or(config.default_resources.vcpu),
            memory_mb: positive_resource(options.default_memory_mb, "default memory MB")
                .map_err(sandbox_error)?
                .unwrap_or(config.default_resources.memory_mb),
            disk_gb: positive_resource(options.default_disk_gb, "default disk GB")
                .map_err(sandbox_error)?
                .unwrap_or(config.default_resources.disk_gb),
        };
        match options.provider.as_deref().unwrap_or("chevalier") {
            "chevalier" | "local" | "vmd" => {}
            "opencomputer" | "open-computer" => {
                if config.distributed_control.is_some() {
                    return Err(sandbox_error(
                        chevalier_sandbox::SandboxError::InvalidConfig(
                            "distributed control requires the chevalier provider".into(),
                        ),
                    ));
                }
                config.provider = SandboxProviderConfig::OpenComputer(
                    opencomputer_config_from_options(options.open_computer)
                        .map_err(sandbox_error)?,
                );
            }
            other => {
                return Err(sandbox_error(
                    chevalier_sandbox::SandboxError::InvalidConfig(format!(
                        "unsupported sandbox provider `{other}`"
                    )),
                ));
            }
        }

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = EngineSandbox::connect(endpoint, config)
                .await
                .map_err(sandbox_error)?;
            Ok(Self { inner })
        })
    }

    #[pyo3(signature = (options=None))]
    fn session<'py>(
        &self,
        py: Python<'py>,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = options
            .map(from_python::<SessionOpts>)
            .transpose()?
            .unwrap_or_default()
            .into();
        let sandbox = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = sandbox.session(options).await.map_err(sandbox_error)?;
            Ok(Session { inner })
        })
    }

    fn attach_session<'py>(
        &self,
        py: Python<'py>,
        session_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sandbox = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = sandbox
                .attach_session(&session_id)
                .await
                .map_err(sandbox_error)?;
            Ok(Session { inner })
        })
    }

    fn attach_session_passive<'py>(
        &self,
        py: Python<'py>,
        session_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sandbox = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = sandbox
                .attach_session_passive(&session_id)
                .await
                .map_err(sandbox_error)?;
            Ok(Session { inner })
        })
    }

    fn list_sessions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sandbox = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sessions: Vec<_> = sandbox
                .list_sessions()
                .await
                .map_err(sandbox_error)?
                .into_iter()
                .map(SessionInfo::from)
                .collect();
            Python::with_gil(|py| to_python(py, &sessions))
        })
    }

    fn list_durable_volumes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sandbox = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let volumes: Vec<_> = sandbox
                .list_durable_volumes()
                .await
                .map_err(sandbox_error)?
                .into_iter()
                .map(DurableVolumeInfo::from)
                .collect();
            Python::with_gil(|py| to_python(py, &volumes))
        })
    }

    fn resize_durable_volume<'py>(
        &self,
        py: Python<'py>,
        owner_key: String,
        size_gb: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sandbox = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let volume = sandbox
                .resize_durable_volume(&owner_key, size_gb)
                .await
                .map_err(sandbox_error)?;
            Python::with_gil(|py| to_python(py, &DurableVolumeInfo::from(volume)))
        })
    }

    fn delete_durable_volume<'py>(
        &self,
        py: Python<'py>,
        owner_key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sandbox = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sandbox
                .delete_durable_volume(&owner_key)
                .await
                .map_err(sandbox_error)
        })
    }

    fn list_host_pci_devices<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sandbox = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inventory = sandbox
                .list_host_pci_devices()
                .await
                .map(HostPciInventory::from)
                .map_err(sandbox_error)?;
            Python::with_gil(|py| to_python(py, &inventory))
        })
    }

    fn discard_session_by_id<'py>(
        &self,
        py: Python<'py>,
        session_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sandbox = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            sandbox
                .discard_session_by_id(&session_id)
                .await
                .map_err(sandbox_error)
        })
    }
}
