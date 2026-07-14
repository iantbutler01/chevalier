//! Standalone MCP client (connect to an MCP server, list/call tools, read
//! resources). MCP results are returned as JSON. (`Runtime.mcp`/`mcpAs` register
//! a server's tools directly onto a runtime — see runtime.rs.)

use std::sync::Arc;

use chevalier_mcp::client::{McpClient as Inner, McpClientConfig as InnerConfig};
use chevalier_mcp::server::{McpServerBuilder, ServerTransport};
use chevalier_mcp::{CallToolResult, Content, ErrorData};
use futures::future::BoxFuture;
use napi::bindgen_prelude::Promise;
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;
use tokio::sync::Mutex;

/// JS MCP tool handler: receives parsed JSON args, returns `Promise<string>`.
/// `Weak=true` so a registered handler doesn't keep the event loop alive.
type ServerToolHandler = ThreadsafeFunction<
    serde_json::Value,
    Promise<String>,
    serde_json::Value,
    napi::Status,
    false,
    true,
>;

fn mcp_err(e: chevalier_mcp::Error) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, format!("MCP: {e}"))
}

fn to_json<T: serde::Serialize>(v: T) -> napi::Result<serde_json::Value> {
    serde_json::to_value(v)
        .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("serialize: {e}")))
}

/// A connection to an MCP server.
#[napi]
pub struct McpClient {
    inner: Arc<Inner>,
}

#[napi]
impl McpClient {
    /// Connect with a structured HTTP, WebSocket, or stdio transport config.
    #[napi(
        factory,
        ts_args_type = "config: { transport: 'http' | 'websocket'; url: string; headers?: Record<string, string> } | { transport: 'stdio'; command: string; args?: string[]; env?: Record<string, string>; cwd?: string }"
    )]
    pub async fn connect(config: serde_json::Value) -> napi::Result<McpClient> {
        let config = serde_json::from_value::<InnerConfig>(config).map_err(|e| {
            napi::Error::new(
                napi::Status::InvalidArg,
                format!("Invalid MCP client config: {e}"),
            )
        })?;
        let c = Inner::connect(config).await.map_err(mcp_err)?;
        Ok(McpClient { inner: Arc::new(c) })
    }

    /// Connect over streamable HTTP, e.g. `http://localhost:8080/mcp`.
    #[napi(factory)]
    pub async fn http(url: String) -> napi::Result<McpClient> {
        let c = Inner::http(url).await.map_err(mcp_err)?;
        Ok(McpClient { inner: Arc::new(c) })
    }

    /// Connect over WebSocket, e.g. `ws://localhost:8080`.
    #[napi(factory)]
    pub async fn websocket(url: String) -> napi::Result<McpClient> {
        let c = Inner::websocket(url).await.map_err(mcp_err)?;
        Ok(McpClient { inner: Arc::new(c) })
    }

    /// Spawn a child process and connect over stdio, e.g.
    /// `npx @modelcontextprotocol/server-filesystem /tmp`.
    #[napi(factory)]
    pub async fn stdio(command_line: String) -> napi::Result<McpClient> {
        let c = Inner::stdio(command_line).await.map_err(mcp_err)?;
        Ok(McpClient { inner: Arc::new(c) })
    }

    /// List the server's tools (JSON).
    #[napi]
    pub async fn list_tools(&self) -> napi::Result<serde_json::Value> {
        let r = self.inner.list_tools().await.map_err(mcp_err)?;
        to_json(r)
    }

    /// Call a tool with JSON arguments; returns the tool result (JSON).
    #[napi]
    pub async fn call_tool(
        &self,
        name: String,
        args: serde_json::Value,
    ) -> napi::Result<serde_json::Value> {
        let r = self.inner.call_tool(name, args).await.map_err(mcp_err)?;
        to_json(r)
    }

    /// List the server's resources (JSON).
    #[napi]
    pub async fn list_resources(&self) -> napi::Result<serde_json::Value> {
        let r = self.inner.list_resources().await.map_err(mcp_err)?;
        to_json(r)
    }

    /// Read a resource by URI (JSON).
    #[napi]
    pub async fn read_resource(&self, uri: String) -> napi::Result<serde_json::Value> {
        let r = self.inner.read_resource(uri).await.map_err(mcp_err)?;
        to_json(r)
    }
}

struct ToolReg {
    name: String,
    description: String,
    schema: serde_json::Value,
    handler: Arc<ServerToolHandler>,
}

/// Options for constructing an `McpServer`.
#[napi(object)]
pub struct McpServerOptions {
    pub version: Option<String>,
    pub description: Option<String>,
}

/// Build and serve an MCP server. Register tools, then `serve(...)`.
#[napi]
pub struct McpServer {
    name: String,
    version: Option<String>,
    description: Option<String>,
    tools: Mutex<Vec<ToolReg>>,
}

#[napi]
impl McpServer {
    #[napi(constructor)]
    pub fn new(name: String, options: Option<McpServerOptions>) -> Self {
        let (version, description) = options
            .map(|o| (o.version, o.description))
            .unwrap_or((None, None));
        Self {
            name,
            version,
            description,
            tools: Mutex::new(Vec::new()),
        }
    }

    /// Register a tool. The handler receives the parsed JSON args and returns a
    /// `Promise<string>` (wrapped as the tool's text result).
    #[napi(
        ts_args_type = "name: string, description: string, schema: any, handler: (args: any) => Promise<string>"
    )]
    pub async fn tool(
        &self,
        name: String,
        description: String,
        schema: serde_json::Value,
        handler: ServerToolHandler,
    ) {
        self.tools.lock().await.push(ToolReg {
            name,
            description,
            schema,
            handler: Arc::new(handler),
        });
    }

    /// Serve the registered tools. `transport` is `"stdio"`, `"http"`, or
    /// `"websocket"`; `addr` (e.g. `127.0.0.1:8080`) is required for http/ws.
    /// Resolves when the server stops.
    #[napi]
    pub async fn serve(&self, transport: String, addr: Option<String>) -> napi::Result<()> {
        let mut builder = McpServerBuilder::new(self.name.clone());
        if let Some(v) = &self.version {
            builder = builder.with_version(v.clone());
        }
        if let Some(d) = &self.description {
            builder = builder.with_description(d.clone());
        }

        let tools = self.tools.lock().await;
        for t in tools.iter() {
            let handler = t.handler.clone();
            builder = builder.with_tool(
                &t.name,
                &t.description,
                t.schema.clone(),
                move |_name, args| {
                    let handler = handler.clone();
                    Box::pin(async move {
                        let args_value = serde_json::Value::Object(args.unwrap_or_default());
                        match handler.call_async(args_value).await {
                            Ok(promise) => match promise.await {
                                Ok(s) => Ok(CallToolResult::success(vec![Content::text(s)])),
                                Err(e) => Err(ErrorData::internal_error(
                                    format!("handler rejected: {e}"),
                                    None,
                                )),
                            },
                            Err(e) => Err(ErrorData::internal_error(
                                format!("handler call failed: {e}"),
                                None,
                            )),
                        }
                    })
                        as BoxFuture<'static, std::result::Result<CallToolResult, ErrorData>>
                },
            );
        }
        drop(tools);

        let server = builder.build();
        let transport = match transport.as_str() {
            "stdio" => ServerTransport::Stdio,
            "http" => ServerTransport::Http(
                addr.ok_or_else(|| napi::Error::from_reason("http transport requires addr"))?,
            ),
            "websocket" | "ws" => ServerTransport::WebSocket(
                addr.ok_or_else(|| napi::Error::from_reason("websocket transport requires addr"))?,
            ),
            other => {
                return Err(napi::Error::from_reason(format!(
                    "unknown transport: {other}"
                )));
            }
        };
        server
            .serve(transport)
            .await
            .map_err(|e| napi::Error::from_reason(format!("MCP serve: {e}")))
    }
}
