//! MCP Server handler implementation
//!
//! Provides a dynamic MCP server that supports runtime tool registration
//! and serves over stdio, HTTP, or WebSocket transports.

use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use rmcp::{
    ErrorData, RoleServer, ServerHandler, ServiceExt,
    handler::server::router::tool::{ToolRoute, ToolRouter},
    handler::server::tool::ToolCallContext,
    model::{
        CallToolRequestParams, CallToolResult, Implementation, ListResourcesResult,
        ListToolsResult, PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResult,
        ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::transport::WebSocketTransport;

#[cfg(feature = "apps")]
use crate::apps::{EXTENSION_ID, UiResource, UiResourceRegistry, UiResourceResolver, UiToolMeta};
#[cfg(feature = "apps")]
use rmcp::model::Meta;

/// Transport type for serving an MCP server
#[derive(Debug, Clone)]
pub enum ServerTransport {
    /// Serve over stdio (for Claude Desktop, etc.)
    Stdio,
    /// Serve over HTTP streaming (Streamable HTTP / SSE)
    Http(String),
    /// Serve over WebSocket
    WebSocket(String),
}

/// An MCP server that exposes tools to MCP clients.
///
/// Supports dynamic tool registration and multiple transport types.
///
/// # Example
/// ```rust,no_run
/// use chevalier_mcp::server::{McpServer, ServerTransport};
/// use rmcp::model::{CallToolResult, Content};
///
/// # async fn example() -> chevalier_mcp::Result<()> {
/// let server = McpServer::builder("my-server")
///     .with_description("A helpful tool server")
///     .with_tool("greet", "Greet someone by name", serde_json::json!({
///         "type": "object",
///         "properties": {
///             "name": { "type": "string", "description": "The name to greet" }
///         },
///         "required": ["name"]
///     }), |_name, args| {
///         Box::pin(async move {
///             let name = args
///                 .and_then(|a| a.get("name").and_then(|n| n.as_str().map(String::from)))
///                 .unwrap_or_else(|| "world".to_string());
///             Ok(CallToolResult::success(vec![Content::text(format!("Hello, {}!", name))]))
///         })
///     })
///     .build();
///
/// // Serve over stdio
/// server.serve(ServerTransport::Stdio).await?;
///
/// // Or over HTTP
/// // server.serve(ServerTransport::Http("127.0.0.1:8080".into())).await?;
///
/// // Or over WebSocket
/// // server.serve(ServerTransport::WebSocket("127.0.0.1:8080".into())).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct McpServer {
    name: String,
    version: String,
    description: Option<String>,
    title: Option<String>,
    icons: Option<Vec<rmcp::model::Icon>>,
    website_url: Option<String>,
    tool_router: ToolRouter<Self>,
    #[cfg(feature = "apps")]
    ui_registry: UiResourceRegistry,
}

impl McpServer {
    /// Create a new server builder
    pub fn builder(name: impl Into<String>) -> McpServerBuilder {
        McpServerBuilder::new(name)
    }

    /// Get the server name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the server description
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Serve over the given transport
    pub async fn serve(self, transport: ServerTransport) -> Result<()> {
        match transport {
            ServerTransport::Stdio => self.serve_stdio().await,
            ServerTransport::Http(addr) => self.serve_http(&addr).await,
            ServerTransport::WebSocket(addr) => self.serve_websocket(&addr).await,
        }
    }

    async fn serve_stdio(self) -> Result<()> {
        let service = ServiceExt::<RoleServer>::serve(self, rmcp::transport::stdio())
            .await
            .map_err(|e| Error::ServerInit(format!("Failed to start stdio server: {}", e)))?;
        service
            .waiting()
            .await
            .map_err(|e| Error::Transport(format!("Stdio server error: {}", e)))?;
        Ok(())
    }

    async fn serve_http(self, addr: &str) -> Result<()> {
        let ct = CancellationToken::new();
        let ct_clone = ct.clone();

        let http_service = StreamableHttpService::new(
            move || Ok(self.clone()),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig {
                cancellation_token: ct.child_token(),
                stateful_mode: true,
                ..Default::default()
            },
        );

        let router = axum::Router::new().nest_service("/mcp", http_service);
        let tcp_listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| Error::Transport(format!("Failed to bind to {}: {}", addr, e)))?;

        axum::serve(tcp_listener, router)
            .with_graceful_shutdown(async move {
                tokio::signal::ctrl_c().await.ok();
                ct_clone.cancel();
            })
            .await
            .map_err(|e| Error::Transport(format!("HTTP server error: {}", e)))?;

        Ok(())
    }

    async fn serve_websocket(self, addr: &str) -> Result<()> {
        let tcp_listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| Error::Transport(format!("Failed to bind to {}: {}", addr, e)))?;

        loop {
            let (stream, _) = tcp_listener
                .accept()
                .await
                .map_err(|e| Error::Transport(format!("Accept error: {}", e)))?;

            let server = self.clone();
            tokio::spawn(async move {
                let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, "WebSocket handshake failed");
                        return;
                    }
                };
                let transport: WebSocketTransport<RoleServer, _, _> =
                    WebSocketTransport::new(ws_stream);
                match ServiceExt::<RoleServer>::serve(server, transport).await {
                    Ok(service) => {
                        let _ = service.waiting().await;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to serve WebSocket client");
                    }
                }
            });
        }
    }
}

#[allow(clippy::manual_async_fn)]
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        #[cfg(feature = "apps")]
        let capabilities = if !self.ui_registry.is_empty() {
            let mut capabilities = ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build();
            capabilities.extensions = Some(
                [(EXTENSION_ID.to_string(), Default::default())]
                    .into_iter()
                    .collect(),
            );
            capabilities
        } else {
            ServerCapabilities::builder().enable_tools().build()
        };
        #[cfg(not(feature = "apps"))]
        let capabilities = ServerCapabilities::builder().enable_tools().build();

        ServerInfo {
            capabilities,
            server_info: Implementation {
                name: self.name.clone(),
                version: self.version.clone(),
                description: self.description.clone(),
                title: self.title.clone(),
                icons: self.icons.clone(),
                website_url: self.website_url.clone(),
            },
            instructions: self.description.clone(),
            ..Default::default()
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListToolsResult, ErrorData>> + Send + '_ {
        async move {
            Ok(ListToolsResult {
                tools: self.tool_router.list_all(),
                next_cursor: None,
                meta: None,
            })
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<CallToolResult, ErrorData>> + Send + '_ {
        async move {
            let tcc = ToolCallContext::new(self, request, context);
            self.tool_router.call(tcc).await
        }
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListResourcesResult, ErrorData>> + Send + '_ {
        async move {
            #[cfg(feature = "apps")]
            {
                Ok(ListResourcesResult {
                    resources: self.ui_registry.list_resources(),
                    next_cursor: None,
                    meta: None,
                })
            }
            #[cfg(not(feature = "apps"))]
            {
                Ok(ListResourcesResult {
                    resources: vec![],
                    next_cursor: None,
                    meta: None,
                })
            }
        }
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ReadResourceResult, ErrorData>> + Send + '_ {
        async move {
            #[cfg(feature = "apps")]
            {
                match self
                    .ui_registry
                    .read_resource_with_meta(
                        &request.uri,
                        (!_context.meta.0.is_empty()).then_some(_context.meta),
                    )
                    .await
                {
                    Some(result) => result,
                    None => Err(ErrorData::resource_not_found(
                        format!("Resource not found: {}", request.uri),
                        None,
                    )),
                }
            }
            #[cfg(not(feature = "apps"))]
            {
                Err(ErrorData::resource_not_found(
                    format!("Resource not found: {}", request.uri),
                    None,
                ))
            }
        }
    }
}

/// Builder for constructing an [McpServer]
pub struct McpServerBuilder {
    name: String,
    version: String,
    description: Option<String>,
    title: Option<String>,
    icons: Option<Vec<rmcp::model::Icon>>,
    website_url: Option<String>,
    tool_router: ToolRouter<McpServer>,
    #[cfg(feature = "apps")]
    ui_registry: UiResourceRegistry,
    #[cfg(feature = "apps")]
    last_tool_name: Option<String>,
}

impl McpServerBuilder {
    /// Create a new builder with the given server name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: None,
            title: None,
            icons: None,
            website_url: None,
            tool_router: ToolRouter::new(),
            #[cfg(feature = "apps")]
            ui_registry: UiResourceRegistry::new(),
            #[cfg(feature = "apps")]
            last_tool_name: None,
        }
    }

    /// Set the server version (defaults to crate version)
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Attach runtime-rendered UI whose resolver accepts opaque child paths.
    ///
    /// The base resource is advertised on the tool and by `resources/list`;
    /// reads below that URI are resolved independently and retain their exact URI.
    #[cfg(feature = "apps")]
    pub fn with_ui_prefix_resolver<R>(mut self, resource: UiResource, resolver: R) -> Self
    where
        R: UiResourceResolver,
    {
        self.attach_ui_to_last_tool(&resource);
        self.ui_registry.insert_runtime_prefix(resource, resolver);
        self
    }

    /// Set the server description / instructions
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the server display title
    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set the server icons
    pub fn with_icons(mut self, icons: Vec<rmcp::model::Icon>) -> Self {
        self.icons = Some(icons);
        self
    }

    /// Set the server website URL
    pub fn with_website_url(mut self, url: impl Into<String>) -> Self {
        self.website_url = Some(url.into());
        self
    }

    /// Register a tool with a JSON schema and handler function
    ///
    /// The handler receives the tool name and optional arguments map,
    /// and returns a CallToolResult.
    pub fn with_tool<F>(mut self, name: &str, description: &str, schema: Value, handler: F) -> Self
    where
        F: Fn(
                String,
                Option<serde_json::Map<String, Value>>,
            ) -> BoxFuture<'static, std::result::Result<CallToolResult, ErrorData>>
            + Send
            + Sync
            + 'static,
    {
        let input_schema: Arc<serde_json::Map<String, Value>> = match schema.as_object() {
            Some(obj) => Arc::new(obj.clone()),
            None => Arc::new(serde_json::Map::new()),
        };

        let tool = Tool::new(name.to_string(), description.to_string(), input_schema);

        let handler = Arc::new(handler);
        let route = ToolRoute::new_dyn(tool, move |tcc: ToolCallContext<'_, McpServer>| {
            let name = tcc.name().to_string();
            let args = tcc.arguments.clone();
            let handler = handler.clone();
            Box::pin(async move { handler(name, args).await })
        });

        self.tool_router.add_route(route);

        #[cfg(feature = "apps")]
        {
            self.last_tool_name = Some(name.to_string());
        }

        self
    }

    /// Attach a UI resource to the most recently registered tool.
    ///
    /// This sets `_meta.ui.resourceUri` on the tool (per SEP-1865) and stores
    /// the HTML resource so it can be served via `resources/read`.
    ///
    /// Must be called immediately after `with_tool`.
    ///
    /// # Panics
    ///
    /// Panics if called without a preceding `with_tool` call.
    #[cfg(feature = "apps")]
    pub fn with_ui(mut self, resource: UiResource) -> Self {
        self.attach_ui_to_last_tool(&resource);
        self.ui_registry.insert(resource);
        self
    }

    /// Attach runtime-rendered UI to the most recently registered tool.
    ///
    /// The resolver is invoked for each `resources/read`, so launch URLs, tokens,
    /// and other per-render configuration are not frozen at server construction.
    #[cfg(feature = "apps")]
    pub fn with_ui_resolver<R>(mut self, resource: UiResource, resolver: R) -> Self
    where
        R: UiResourceResolver,
    {
        self.attach_ui_to_last_tool(&resource);
        self.ui_registry.insert_runtime(resource, resolver);
        self
    }

    #[cfg(feature = "apps")]
    fn attach_ui_to_last_tool(&mut self, resource: &UiResource) {
        let tool_name = self
            .last_tool_name
            .as_ref()
            .expect("with_ui must be called after with_tool");

        // Set _meta.ui on the tool (visibility defaults to None = ["model", "app"] per spec)
        if let Some(route) = self.tool_router.map.get_mut(tool_name.as_str()) {
            let ui_meta = UiToolMeta {
                resource_uri: resource.uri.clone(),
                visibility: None,
            };
            let ui_value = serde_json::to_value(&ui_meta).unwrap_or_default();
            let meta = route
                .attr
                .meta
                .get_or_insert_with(|| Meta(serde_json::Map::new()));
            meta.0.insert("ui".to_string(), ui_value);
        }
    }

    /// Set the visibility of the most recently registered tool.
    ///
    /// Controls who can call the tool per SEP-1865:
    /// - `[Model]` — only the LLM agent can call this tool
    /// - `[App]` — only the embedded iframe can call this tool
    /// - `[Model, App]` — both can call it (this is the default when omitted)
    ///
    /// Must be chained after `with_ui`.
    #[cfg(feature = "apps")]
    pub fn visibility(mut self, visibility: Vec<crate::apps::Visibility>) -> Self {
        let tool_name = self
            .last_tool_name
            .as_ref()
            .expect("visibility must be called after with_tool");

        if let Some(route) = self.tool_router.map.get_mut(tool_name.as_str())
            && let Some(meta) = &mut route.attr.meta
            && let Some(ui_value) = meta.0.get_mut("ui")
            && let Some(obj) = ui_value.as_object_mut()
        {
            obj.insert(
                "visibility".to_string(),
                serde_json::to_value(&visibility).unwrap_or_default(),
            );
        }
        self
    }

    /// Build the server
    pub fn build(self) -> McpServer {
        McpServer {
            name: self.name,
            version: self.version,
            description: self.description,
            title: self.title,
            icons: self.icons,
            website_url: self.website_url,
            tool_router: self.tool_router,
            #[cfg(feature = "apps")]
            ui_registry: self.ui_registry,
        }
    }
}

impl std::fmt::Debug for McpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpServer")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("description", &self.description)
            .finish()
    }
}
