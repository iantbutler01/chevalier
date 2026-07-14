//! MCP Client connection management

#[cfg(feature = "apps")]
use rmcp::model::ExtensionCapabilities;
use std::{collections::BTreeMap, path::PathBuf};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rmcp::{
    ClientHandler, RoleClient, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResult, ClientInfo, Implementation, ListResourcesResult,
        ListToolsResult, ReadResourceRequestParams, ReadResourceResult, ServerInfo,
    },
    service::RunningService,
    transport::{
        StreamableHttpClientTransport, TokioChildProcess,
        streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde::Deserialize;
use serde_json::Value;
use tokio::process::Command;

use crate::error::{Error, Result};

/// MCP client handler that advertises extension capabilities during initialize.
///
/// When the `apps` feature is enabled, advertises support for the
/// `io.modelcontextprotocol/ui` extension (SEP-1865) so servers can
/// expose UI-enabled tools.
struct ChevalierClientHandler;

impl ClientHandler for ChevalierClientHandler {
    fn get_info(&self) -> ClientInfo {
        #[cfg(feature = "apps")]
        let capabilities = {
            let mut ext = ExtensionCapabilities::new();
            let mut ui = serde_json::Map::new();
            ui.insert(
                "mimeTypes".to_string(),
                serde_json::json!(["text/html;profile=mcp-app"]),
            );
            ext.insert("io.modelcontextprotocol/ui".to_string(), ui);

            rmcp::model::ClientCapabilities::builder()
                .enable_extensions_with(ext)
                .build()
        };

        #[cfg(not(feature = "apps"))]
        let capabilities = rmcp::model::ClientCapabilities::default();

        ClientInfo {
            protocol_version: Default::default(),
            capabilities,
            client_info: Implementation {
                name: "chevalier-mcp".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
    }
}

/// Transport type for MCP connections
#[derive(Debug, Clone)]
pub enum Transport {
    /// HTTP/SSE streaming transport
    Http(String),
    /// WebSocket transport
    WebSocket(String),
    /// Stdio transport (spawns child process)
    Stdio { command: String, args: Vec<String> },
}

/// Structured configuration for an MCP client connection.
///
/// Header and environment values are supplied directly to the transport and
/// child process. Chevalier does not resolve credentials or interpret their
/// contents.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(tag = "transport", rename_all = "lowercase", deny_unknown_fields)]
pub enum McpClientConfig {
    /// Streamable HTTP transport.
    Http {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    /// WebSocket transport.
    WebSocket {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    /// Stdio transport (spawns a child process without invoking a shell).
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default)]
        cwd: Option<PathBuf>,
    },
}

/// MCP Server configuration for registering with a runtime
#[derive(Debug, Clone)]
pub struct McpServer {
    transport: Transport,
}

impl McpServer {
    /// Create an MCP server configuration for HTTP transport
    ///
    /// # Example
    /// ```rust,no_run
    /// use chevalier_mcp::client::McpServer;
    ///
    /// let server = McpServer::http("http://localhost:8080/mcp");
    /// ```
    pub fn http(url: impl Into<String>) -> Self {
        Self {
            transport: Transport::Http(url.into()),
        }
    }

    /// Create an MCP server configuration for WebSocket transport
    ///
    /// # Example
    /// ```rust,no_run
    /// use chevalier_mcp::client::McpServer;
    ///
    /// let server = McpServer::websocket("ws://localhost:8080/mcp");
    /// ```
    pub fn websocket(url: impl Into<String>) -> Self {
        Self {
            transport: Transport::WebSocket(url.into()),
        }
    }

    /// Create an MCP server configuration for stdio transport
    ///
    /// The command string is parsed to extract the command and arguments.
    ///
    /// # Example
    /// ```rust,no_run
    /// use chevalier_mcp::client::McpServer;
    ///
    /// let server = McpServer::stdio("npx @modelcontextprotocol/server-filesystem /tmp");
    /// ```
    pub fn stdio(command_line: impl Into<String>) -> Self {
        let command_line = command_line.into();
        let parts: Vec<&str> = command_line.split_whitespace().collect();
        let (command, args) = if parts.is_empty() {
            (command_line.clone(), vec![])
        } else {
            (
                parts[0].to_string(),
                parts[1..].iter().map(|s| s.to_string()).collect(),
            )
        };

        Self {
            transport: Transport::Stdio { command, args },
        }
    }

    /// Create an MCP server configuration for stdio with explicit command and args
    ///
    /// # Example
    /// ```rust,no_run
    /// use chevalier_mcp::client::McpServer;
    ///
    /// let server = McpServer::stdio_with_args("npx", &["@modelcontextprotocol/server-filesystem", "/tmp"]);
    /// ```
    pub fn stdio_with_args(command: impl Into<String>, args: &[impl AsRef<str>]) -> Self {
        Self {
            transport: Transport::Stdio {
                command: command.into(),
                args: args.iter().map(|s| s.as_ref().to_string()).collect(),
            },
        }
    }

    /// Get the transport configuration
    pub fn transport(&self) -> &Transport {
        &self.transport
    }

    /// Connect to the MCP server and return a client
    pub async fn connect(self) -> Result<McpClient> {
        McpClient::from_server(self).await
    }
}

/// An active MCP client connection
pub struct McpClient {
    inner: ClientInner,
}

enum ClientInner {
    Http(RunningService<RoleClient, ChevalierClientHandler>),
    WebSocket(RunningService<RoleClient, ChevalierClientHandler>),
    Stdio(RunningService<RoleClient, ChevalierClientHandler>),
}

impl McpClient {
    /// Connect using a structured transport configuration.
    pub async fn connect(config: McpClientConfig) -> Result<Self> {
        match config {
            McpClientConfig::Http { url, headers } => Self::http_configured(url, headers).await,
            McpClientConfig::WebSocket { url, headers } => {
                Self::websocket_configured(url, headers).await
            }
            McpClientConfig::Stdio {
                command,
                args,
                env,
                cwd,
            } => Self::stdio_configured(command, args, env, cwd).await,
        }
    }

    /// Connect to an MCP server via HTTP
    ///
    /// # Example
    /// ```rust,no_run
    /// use chevalier_mcp::client::McpClient;
    ///
    /// # async fn example() -> chevalier_mcp::Result<()> {
    /// let client = McpClient::http("http://localhost:8080/mcp").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn http(url: impl Into<String>) -> Result<Self> {
        Self::http_configured(url.into(), BTreeMap::new()).await
    }

    async fn http_configured(url: String, headers: BTreeMap<String, String>) -> Result<Self> {
        let headers = validate_headers(headers, "HTTP")?;
        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|e| Error::Transport(format!("Failed to configure HTTP client: {e}")))?;
        let transport = StreamableHttpClientTransport::with_client(
            http_client,
            StreamableHttpClientTransportConfig::with_uri(url.clone()),
        );
        let service = ChevalierClientHandler.serve(transport).await.map_err(|e| {
            Error::Transport(format!(
                "Failed to connect to HTTP MCP server at {}: {}",
                url, e
            ))
        })?;
        Ok(Self {
            inner: ClientInner::Http(service),
        })
    }

    /// Connect to an MCP server via WebSocket
    ///
    /// # Example
    /// ```rust,no_run
    /// use chevalier_mcp::client::McpClient;
    ///
    /// # async fn example() -> chevalier_mcp::Result<()> {
    /// let client = McpClient::websocket("ws://localhost:8080/mcp").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn websocket(url: impl Into<String>) -> Result<Self> {
        Self::websocket_configured(url.into(), BTreeMap::new()).await
    }

    async fn websocket_configured(url: String, headers: BTreeMap<String, String>) -> Result<Self> {
        let headers = validate_headers(headers, "WebSocket")?;
        let transport = crate::transport::websocket::connect_with_headers(&url, headers).await?;
        let service = ChevalierClientHandler.serve(transport).await.map_err(|e| {
            Error::Transport(format!(
                "Failed to connect to WebSocket MCP server at {}: {}",
                url, e
            ))
        })?;
        Ok(Self {
            inner: ClientInner::WebSocket(service),
        })
    }

    /// Connect to an MCP server via stdio (spawns child process)
    ///
    /// The command string is parsed to extract the command and arguments.
    ///
    /// # Example
    /// ```rust,no_run
    /// use chevalier_mcp::client::McpClient;
    ///
    /// # async fn example() -> chevalier_mcp::Result<()> {
    /// let client = McpClient::stdio("npx @modelcontextprotocol/server-filesystem /tmp").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stdio(command_line: impl Into<String>) -> Result<Self> {
        let command_line = command_line.into();
        let parts: Vec<&str> = command_line.split_whitespace().collect();
        if parts.is_empty() {
            return Err(Error::Transport("Empty command".to_string()));
        }

        let command = parts[0];
        let args: Vec<&str> = parts[1..].to_vec();

        Self::stdio_configured(
            command.to_string(),
            args.into_iter().map(str::to_string).collect(),
            BTreeMap::new(),
            None,
        )
        .await
    }

    /// Create a client from an McpServer configuration
    async fn from_server(server: McpServer) -> Result<Self> {
        match server.transport {
            Transport::Http(url) => Self::http(url).await,
            Transport::WebSocket(url) => Self::websocket(url).await,
            Transport::Stdio { command, args } => Self::stdio_with_args(&command, &args).await,
        }
    }

    /// Connect to an MCP server via stdio with explicit command and args
    pub async fn stdio_with_args(command: &str, args: &[String]) -> Result<Self> {
        Self::stdio_configured(command.to_string(), args.to_vec(), BTreeMap::new(), None).await
    }

    async fn stdio_configured(
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        cwd: Option<PathBuf>,
    ) -> Result<Self> {
        let child_command = build_stdio_command(&command, &args, &env, cwd.as_deref())?;
        let transport = TokioChildProcess::new(child_command).map_err(|e| {
            Error::Transport(format!("Failed to spawn process '{}': {}", command, e))
        })?;

        let service = ChevalierClientHandler.serve(transport).await.map_err(|e| {
            Error::Transport(format!(
                "Failed to connect to stdio MCP server '{}': {}",
                command, e
            ))
        })?;

        Ok(Self {
            inner: ClientInner::Stdio(service),
        })
    }

    /// Get information about the connected server
    ///
    /// Returns `None` if the server info is not yet available (shouldn't happen
    /// after successful connection).
    pub fn server_info(&self) -> Option<&ServerInfo> {
        self.service().peer_info()
    }

    /// List all available tools from the MCP server
    ///
    /// # Example
    /// ```rust,no_run
    /// use chevalier_mcp::client::McpClient;
    ///
    /// # async fn example() -> chevalier_mcp::Result<()> {
    /// let client = McpClient::http("http://localhost:8080/mcp").await?;
    /// let tools = client.list_tools().await?;
    /// for tool in &tools.tools {
    ///     println!("Tool: {} - {:?}", tool.name, tool.description);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_tools(&self) -> Result<ListToolsResult> {
        self.service()
            .list_tools(Default::default())
            .await
            .map_err(|e| Error::Protocol(format!("Failed to list tools: {}", e)))
    }

    /// Call a tool on the MCP server
    ///
    /// # Arguments
    /// * `name` - The name of the tool to call
    /// * `arguments` - The arguments to pass to the tool as a JSON value
    ///
    /// # Example
    /// ```rust,no_run
    /// use chevalier_mcp::client::McpClient;
    /// use serde_json::json;
    ///
    /// # async fn example() -> chevalier_mcp::Result<()> {
    /// let client = McpClient::http("http://localhost:8080/mcp").await?;
    /// let result = client.call_tool("read_file", json!({"path": "/tmp/test.txt"})).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call_tool(
        &self,
        name: impl Into<String>,
        arguments: Value,
    ) -> Result<CallToolResult> {
        let name = name.into();
        let arguments = arguments.as_object().cloned();

        self.service()
            .call_tool(CallToolRequestParams {
                meta: None,
                name: name.clone().into(),
                arguments,
                task: None,
            })
            .await
            .map_err(|e| Error::ToolExecution(format!("Failed to call tool '{}': {}", name, e)))
    }

    /// List all available resources from the MCP server
    pub async fn list_resources(&self) -> Result<ListResourcesResult> {
        self.service()
            .list_resources(Default::default())
            .await
            .map_err(|e| Error::Protocol(format!("Failed to list resources: {}", e)))
    }

    /// Read a resource by URI from the MCP server
    pub async fn read_resource(&self, uri: impl Into<String>) -> Result<ReadResourceResult> {
        let uri = uri.into();
        self.service()
            .read_resource(ReadResourceRequestParams {
                meta: None,
                uri: uri.clone(),
            })
            .await
            .map_err(|e| Error::Protocol(format!("Failed to read resource '{}': {}", uri, e)))
    }

    /// Gracefully close the connection
    pub async fn close(self) -> Result<()> {
        match self.inner {
            ClientInner::Http(service)
            | ClientInner::WebSocket(service)
            | ClientInner::Stdio(service) => {
                service
                    .cancel()
                    .await
                    .map_err(|e| Error::Transport(format!("Failed to close connection: {}", e)))?;
                Ok(())
            }
        }
    }

    fn service(&self) -> &RunningService<RoleClient, ChevalierClientHandler> {
        match &self.inner {
            ClientInner::Http(s) | ClientInner::WebSocket(s) | ClientInner::Stdio(s) => s,
        }
    }
}

fn validate_headers(headers: BTreeMap<String, String>, transport: &str) -> Result<HeaderMap> {
    let mut validated = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
            Error::Transport(format!("Invalid {transport} header name '{name}': {e}"))
        })?;
        let header_value = HeaderValue::from_str(&value).map_err(|_| {
            Error::Transport(format!("Invalid value for {transport} header '{name}'"))
        })?;
        validated.insert(header_name, header_value);
    }
    Ok(validated)
}

fn build_stdio_command(
    command: &str,
    args: &[String],
    env: &BTreeMap<String, String>,
    cwd: Option<&std::path::Path>,
) -> Result<Command> {
    if command.trim().is_empty() {
        return Err(Error::Transport("Empty command".to_string()));
    }

    let mut child = Command::new(command);
    child.args(args).envs(env);
    if let Some(cwd) = cwd {
        child.current_dir(cwd);
    }
    Ok(child)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdio_config_keeps_args_env_and_cwd_separate() {
        let args = vec!["--label".to_string(), "value with spaces".to_string()];
        let env = BTreeMap::from([(
            "CHEVALIER_TEST_SECRET".to_string(),
            "not-on-the-command-line".to_string(),
        )]);
        let cwd = std::env::temp_dir();

        let command = build_stdio_command("mcp-test-server", &args, &env, Some(&cwd)).unwrap();
        let command = command.as_std();

        assert_eq!(command.get_program(), "mcp-test-server");
        assert_eq!(
            command
                .get_args()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            args
        );
        assert_eq!(command.get_current_dir(), Some(cwd.as_path()));
        assert!(command.get_envs().any(|(name, value)| {
            name == "CHEVALIER_TEST_SECRET"
                && value == Some(std::ffi::OsStr::new("not-on-the-command-line"))
        }));
        assert!(
            command
                .get_args()
                .all(|arg| arg != "not-on-the-command-line")
        );
    }

    #[test]
    fn invalid_header_errors_do_not_echo_values() {
        let secret = "secret\nvalue";
        let error = validate_headers(
            BTreeMap::from([("authorization".to_string(), secret.to_string())]),
            "HTTP",
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("authorization"));
        assert!(!error.contains(secret));
    }
}
