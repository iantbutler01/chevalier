//! MCP (Model Context Protocol) integration for chevalier
//!
//! Provides:
//! - **Client bridge**: `runtime.mcp("http://...")` to consume remote MCP tools transparently
//! - **McpServer**: Expose agentic functions and tools over MCP for external clients

use std::sync::Arc;

use chevalier_mcp::server::McpServerBuilder;
use chevalier_mcp::{CallToolResult, Content, ErrorData};
use futures::future::BoxFuture;
use serde_json::Value;

use crate::error::{Error, Result};

// Re-export ServerTransport directly from chevalier-mcp
pub use chevalier_mcp::server::ServerTransport;

// Re-export apps types when the mcp-apps feature is enabled
#[cfg(feature = "mcp-apps")]
pub use chevalier_mcp::apps::{
    DisplayMode, UiPermissions, UiResource, UiResourceCsp, UiResourceMeta, Visibility, ui_uri,
};

/// An MCP server that exposes agentic functions and tools to MCP clients.
///
/// Wraps `chevalier_mcp::server::McpServerBuilder` with a friendlier API
/// for registering agent handlers and plain tools.
///
/// # Example
/// ```rust,no_run
/// use chevalier::mcp::{McpServer, ServerTransport};
/// use chevalier_mcp::{CallToolResult, Content};
///
/// # async fn example() -> chevalier::error::Result<()> {
/// McpServer::new("my-server")
///     .tool("echo", "Echo a message", serde_json::json!({
///         "type": "object",
///         "properties": { "message": { "type": "string" } },
///         "required": ["message"]
///     }), |_name, args| {
///         Box::pin(async move {
///             let msg = args.unwrap_or_default();
///             let text = msg.get("message").and_then(|v| v.as_str()).unwrap_or("no message");
///             Ok(CallToolResult::success(vec![Content::text(text)]))
///         })
///     })
///     .serve(ServerTransport::Http("0.0.0.0:3000".into()))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct McpServer {
    builder: McpServerBuilder,
}

impl McpServer {
    /// Create a new MCP server with the given name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            builder: chevalier_mcp::server::McpServer::builder(name),
        }
    }

    /// Set the server description
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.builder = self.builder.with_description(description);
        self
    }

    /// Set the server version
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.builder = self.builder.with_version(version);
        self
    }

    /// Register a single agent as an MCP tool.
    ///
    /// The handler receives `serde_json::Value` (the tool arguments) and returns `Result<String>`.
    /// The string result is automatically wrapped in MCP `Content::text`.
    ///
    /// # Arguments
    /// * `name` - Tool name
    /// * `description` - Tool description
    /// * `schema` - JSON Schema for the tool's input parameters
    /// * `handler` - Async function that processes the arguments and returns a string result
    pub fn agent<F>(self, name: &str, description: &str, schema: Value, handler: F) -> Self
    where
        F: Fn(Value) -> BoxFuture<'static, Result<String>> + Send + Sync + 'static,
    {
        self.register_agent(name, description, schema, handler)
    }

    /// Register multiple agents as MCP tools in bulk.
    ///
    /// Each tuple is `(name, description, schema, handler)`.
    pub fn agents<F>(mut self, agents: Vec<(&str, &str, Value, F)>) -> Self
    where
        F: Fn(Value) -> BoxFuture<'static, Result<String>> + Send + Sync + 'static,
    {
        for (name, description, schema, handler) in agents {
            self = self.register_agent(name, description, schema, handler);
        }
        self
    }

    /// Register a single raw MCP tool (pass-through to chevalier-mcp).
    ///
    /// The handler signature matches chevalier-mcp's `McpServerBuilder::with_tool` directly.
    pub fn tool<F>(mut self, name: &str, description: &str, schema: Value, handler: F) -> Self
    where
        F: Fn(
                String,
                Option<serde_json::Map<String, Value>>,
            ) -> BoxFuture<'static, std::result::Result<CallToolResult, ErrorData>>
            + Send
            + Sync
            + 'static,
    {
        self.builder = self.builder.with_tool(name, description, schema, handler);
        self
    }

    /// Register multiple raw MCP tools in bulk.
    pub fn tools<F>(mut self, tools: Vec<(&str, &str, Value, F)>) -> Self
    where
        F: Fn(
                String,
                Option<serde_json::Map<String, Value>>,
            ) -> BoxFuture<'static, std::result::Result<CallToolResult, ErrorData>>
            + Send
            + Sync
            + 'static,
    {
        for (name, description, schema, handler) in tools {
            self.builder = self.builder.with_tool(name, description, schema, handler);
        }
        self
    }

    /// Attach a UI resource to the most recently registered tool.
    ///
    /// Must be called immediately after `.tool()` or `.agent()`.
    /// Sets `_meta.ui.resourceUri` on the tool (per SEP-1865) and stores
    /// the HTML resource so it can be served via `resources/read`.
    ///
    /// # Panics
    /// Panics if called without a preceding tool/agent registration.
    #[cfg(feature = "mcp-apps")]
    pub fn with_ui(mut self, resource: UiResource) -> Self {
        self.builder = self.builder.with_ui(resource);
        self
    }

    /// Attach runtime-rendered UI for opaque child resource paths.
    #[cfg(feature = "mcp-apps")]
    pub fn with_ui_prefix_resolver<R>(mut self, resource: UiResource, resolver: R) -> Self
    where
        R: chevalier_mcp::apps::UiResourceResolver,
    {
        self.builder = self.builder.with_ui_prefix_resolver(resource, resolver);
        self
    }

    /// Set the visibility of the most recently registered tool.
    ///
    /// Controls who can call the tool per SEP-1865:
    /// - `[Model]` — only the LLM agent can call this tool
    /// - `[App]` — only the embedded iframe can call this tool
    /// - `[Model, App]` — both (default when omitted)
    ///
    /// Must be chained after `with_ui`.
    #[cfg(feature = "mcp-apps")]
    pub fn visibility(mut self, visibility: Vec<chevalier_mcp::apps::Visibility>) -> Self {
        self.builder = self.builder.visibility(visibility);
        self
    }

    /// Serve over the given transport. This blocks until the server shuts down.
    pub async fn serve(self, transport: ServerTransport) -> Result<()> {
        self.builder
            .build()
            .serve(transport)
            .await
            .map_err(|e| Error::NonRetryable(format!("MCP server error: {}", e)))
    }

    /// Internal: wrap an agent handler into an MCP tool handler and register it
    fn register_agent<F>(mut self, name: &str, description: &str, schema: Value, handler: F) -> Self
    where
        F: Fn(Value) -> BoxFuture<'static, Result<String>> + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        self.builder = self
            .builder
            .with_tool(name, description, schema, move |_name, args| {
                let handler = handler.clone();
                Box::pin(async move {
                    // Convert Option<Map> to Value for the agent handler
                    let args_value = match args {
                        Some(map) => Value::Object(map),
                        None => Value::Object(serde_json::Map::new()),
                    };
                    match handler(args_value).await {
                        Ok(result) => Ok(CallToolResult::success(vec![Content::text(result)])),
                        Err(e) => Ok(CallToolResult::error(vec![Content::text(e.to_string())])),
                    }
                })
            });
        self
    }
}

/// Connect to an MCP server and register all its tools into the runtime.
///
/// Auto-detects transport from the URI:
/// - `http://` or `https://` → HTTP streaming
/// - `ws://` or `wss://` → WebSocket
/// - Anything else → stdio (treated as a command to spawn)
///
/// If `label` is provided, tool names are prefixed as `{label}_{tool_name}`.
/// The original tool name is still used when calling the remote server.
pub(crate) async fn connect_and_register(
    runtime: &crate::runtime::Runtime,
    uri: &str,
    label: Option<&str>,
) -> Result<Arc<chevalier_mcp::client::McpClient>> {
    // Auto-detect transport and connect
    let client = if uri.starts_with("http://") || uri.starts_with("https://") {
        chevalier_mcp::client::McpClient::http(uri).await
    } else if uri.starts_with("ws://") || uri.starts_with("wss://") {
        chevalier_mcp::client::McpClient::websocket(uri).await
    } else {
        chevalier_mcp::client::McpClient::stdio(uri).await
    }
    .map_err(|e| {
        Error::NonRetryable(format!("Failed to connect to MCP server '{}': {}", uri, e))
    })?;

    let client = Arc::new(client);

    // Discover tools
    let tools_result = client
        .list_tools()
        .await
        .map_err(|e| Error::NonRetryable(format!("Failed to list MCP tools: {}", e)))?;

    // Register each tool, skipping app-only tools per SEP-1865 visibility
    for tool in &tools_result.tools {
        if is_app_only_tool(tool) {
            continue;
        }

        let remote_name = tool.name.to_string();
        let registered_name = match label {
            Some(prefix) => format!("{}_{}", prefix, remote_name),
            None => remote_name.clone(),
        };
        let tool_description = tool.description.clone().unwrap_or_default();

        // Build schema from the tool's input_schema
        let schema = serde_json::to_value(&tool.input_schema)
            .unwrap_or(Value::Object(serde_json::Map::new()));

        // Create async handler that delegates to the MCP client
        // Always use the original remote_name when calling the server
        let client_ref = client.clone();
        let name_for_closure = remote_name;
        let handler = crate::runtime::ToolFunction::Async(Box::new(move |args: Value| {
            let client = client_ref.clone();
            let name = name_for_closure.clone();
            Box::pin(async move {
                let result = client.call_tool(&name, args).await.map_err(|e| {
                    Error::NonRetryable(format!("MCP tool '{}' failed: {}", name, e))
                })?;

                // Extract text content from the result
                let text: String = result
                    .content
                    .iter()
                    .filter_map(|c| c.as_text().map(|t| t.text.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n");

                Ok(text)
            }) as BoxFuture<'static, Result<String>>
        }));

        runtime
            .register_tool_with_schema(registered_name, tool_description, schema, handler)
            .await?;
    }

    Ok(client)
}

/// Check if a tool has visibility: ["app"] only (no "model").
/// Per SEP-1865, default visibility (None) means ["model", "app"].
fn is_app_only_tool(tool: &chevalier_mcp::McpTool) -> bool {
    let Some(meta) = &tool.meta else { return false };
    let Some(ui) = meta.0.get("ui") else {
        return false;
    };
    let Some(visibility) = ui.get("visibility") else {
        return false;
    };
    let Some(arr) = visibility.as_array() else {
        return false;
    };

    // If visibility is specified and doesn't include "model", it's app-only
    !arr.iter().any(|v| v.as_str() == Some("model"))
}
