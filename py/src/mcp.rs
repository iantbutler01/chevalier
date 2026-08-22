use std::sync::Arc;

use chevalier_mcp::client::{McpClient as EngineMcpClient, McpClientConfig};
use chevalier_mcp::server::{McpServerBuilder, ServerTransport};
use chevalier_mcp::{CallToolResult, Content, ErrorData};
use futures::future::BoxFuture;
use pyo3::prelude::*;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::errors::{contextual_error, invalid_argument};
use crate::json::{from_python, to_python, value_from_python};
use crate::runtime::{PythonToolHandler, invoke_python_tool};

fn mcp_error(error: chevalier_mcp::Error) -> PyErr {
    contextual_error("MCP", error)
}

#[pyclass(module = "chevalier.chevalier")]
pub struct McpClient {
    inner: Arc<EngineMcpClient>,
}

#[pymethods]
impl McpClient {
    #[staticmethod]
    fn connect<'py>(py: Python<'py>, config: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let config = from_python::<McpClientConfig>(config)
            .map_err(|error| invalid_argument(format!("invalid MCP client config: {error}")))?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = EngineMcpClient::connect(config).await.map_err(mcp_error)?;
            Ok(Self {
                inner: Arc::new(inner),
            })
        })
    }

    #[staticmethod]
    fn http<'py>(py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = EngineMcpClient::http(url).await.map_err(mcp_error)?;
            Ok(Self {
                inner: Arc::new(inner),
            })
        })
    }

    #[staticmethod]
    fn websocket<'py>(py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = EngineMcpClient::websocket(url).await.map_err(mcp_error)?;
            Ok(Self {
                inner: Arc::new(inner),
            })
        })
    }

    #[staticmethod]
    fn stdio<'py>(py: Python<'py>, command_line: String) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = EngineMcpClient::stdio(command_line)
                .await
                .map_err(mcp_error)?;
            Ok(Self {
                inner: Arc::new(inner),
            })
        })
    }

    fn list_tools<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = inner.list_tools().await.map_err(mcp_error)?;
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    fn call_tool<'py>(
        &self,
        py: Python<'py>,
        name: String,
        args: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let args = value_from_python(args)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = inner.call_tool(name, args).await.map_err(mcp_error)?;
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    fn list_resources<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = inner.list_resources().await.map_err(mcp_error)?;
            Python::with_gil(|py| to_python(py, &result))
        })
    }

    fn read_resource<'py>(&self, py: Python<'py>, uri: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = inner.read_resource(uri).await.map_err(mcp_error)?;
            Python::with_gil(|py| to_python(py, &result))
        })
    }
}

struct ToolRegistration {
    name: String,
    description: String,
    schema: serde_json::Value,
    handler: Arc<PythonToolHandler>,
}

#[derive(Default, Deserialize)]
struct McpServerOptions {
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

#[pyclass(module = "chevalier.chevalier")]
pub struct McpServer {
    name: String,
    version: Option<String>,
    description: Option<String>,
    tools: Arc<Mutex<Vec<ToolRegistration>>>,
}

#[pymethods]
impl McpServer {
    #[new]
    #[pyo3(signature = (name, options=None))]
    fn new(name: String, options: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let options: McpServerOptions = options.map(from_python).transpose()?.unwrap_or_default();
        Ok(Self {
            name,
            version: options.version,
            description: options.description,
            tools: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn tool<'py>(
        &self,
        py: Python<'py>,
        name: String,
        description: String,
        schema: &Bound<'_, PyAny>,
        handler: PyObject,
    ) -> PyResult<Bound<'py, PyAny>> {
        let schema = value_from_python(schema)?;
        let handler = Arc::new(PythonToolHandler::new(py, handler)?);
        let tools = self.tools.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tools.lock().await.push(ToolRegistration {
                name,
                description,
                schema,
                handler,
            });
            Ok(())
        })
    }

    #[pyo3(signature = (transport, addr=None))]
    fn serve<'py>(
        &self,
        py: Python<'py>,
        transport: String,
        addr: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let name = self.name.clone();
        let version = self.version.clone();
        let description = self.description.clone();
        let tools = self.tools.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut builder = McpServerBuilder::new(name);
            if let Some(version) = version {
                builder = builder.with_version(version);
            }
            if let Some(description) = description {
                builder = builder.with_description(description);
            }

            let tools = tools.lock().await;
            for tool in tools.iter() {
                let handler = tool.handler.clone();
                builder = builder.with_tool(
                    &tool.name,
                    &tool.description,
                    tool.schema.clone(),
                    move |_name, args| {
                        let handler = handler.clone();
                        Box::pin(async move {
                            let args = serde_json::Value::Object(args.unwrap_or_default());
                            invoke_python_tool(handler, args)
                                .await
                                .map(|value| CallToolResult::success(vec![Content::text(value)]))
                                .map_err(|error| ErrorData::internal_error(error.to_string(), None))
                        })
                            as BoxFuture<'static, Result<CallToolResult, ErrorData>>
                    },
                );
            }
            drop(tools);

            let transport = match transport.as_str() {
                "stdio" => ServerTransport::Stdio,
                "http" => ServerTransport::Http(
                    addr.ok_or_else(|| invalid_argument("http transport requires addr"))?,
                ),
                "websocket" | "ws" => ServerTransport::WebSocket(
                    addr.ok_or_else(|| invalid_argument("websocket transport requires addr"))?,
                ),
                other => {
                    return Err(invalid_argument(format!("unknown transport: {other}")));
                }
            };
            builder
                .build()
                .serve(transport)
                .await
                .map_err(|error| contextual_error("MCP serve", error))
        })
    }
}
