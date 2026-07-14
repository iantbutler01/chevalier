# chevalier-mcp

MCP ([Model Context Protocol](https://modelcontextprotocol.io/)) client and server library for Rust. Part of the [chevalier](https://github.com/iantbutler01/chevalier) framework.

## Features

- **Server**: Build MCP servers with a simple builder API. Register tools, serve over stdio, HTTP, or WebSocket.
- **Client**: Connect to any MCP server. Discover tools, call them, read resources.
- **Apps** (optional): MCP Apps extension (SEP-1865) for attaching interactive HTML UIs to tools.

## Installation

```toml
[dependencies]
chevalier-mcp = "0.1"
```

Feature flags:

| Flag | Default | Description |
|------|---------|-------------|
| `client` | Yes | MCP client for connecting to servers |
| `server` | Yes | MCP server with tool registration |
| `apps` | No | MCP Apps extension (interactive UIs) |
| `full` | No | Enables all features |

## Server

Build an MCP server with tools using the builder pattern:

```rust
use rmcp::model::{CallToolResult, Content};
use chevalier_mcp::server::{McpServer, ServerTransport};
use serde_json::json;

#[tokio::main]
async fn main() -> chevalier_mcp::Result<()> {
    let server = McpServer::builder("my-server")
        .with_version("1.0.0")
        .with_description("A server with tools")
        .with_tool(
            "add",
            "Add two numbers",
            json!({
                "type": "object",
                "properties": {
                    "a": { "type": "number" },
                    "b": { "type": "number" }
                },
                "required": ["a", "b"]
            }),
            |_name, args| {
                Box::pin(async move {
                    let args = args.unwrap_or_default();
                    let a = args.get("a").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let b = args.get("b").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    Ok(CallToolResult::success(vec![
                        Content::text(format!("{}", a + b)),
                    ]))
                })
            },
        )
        .build();

    // Serve over stdio (for Claude Desktop, Claude Code, etc.)
    server.serve(ServerTransport::Stdio).await?;

    // Or over HTTP:
    // server.serve(ServerTransport::Http("127.0.0.1:8080".into())).await?;

    // Or WebSocket:
    // server.serve(ServerTransport::WebSocket("127.0.0.1:8080".into())).await?;

    Ok(())
}
```

### Transports

| Transport | Use case |
|-----------|----------|
| `ServerTransport::Stdio` | Claude Desktop, Claude Code, local MCP clients |
| `ServerTransport::Http(addr)` | Streamable HTTP (SSE), remote clients |
| `ServerTransport::WebSocket(addr)` | Full-duplex, real-time communication |

## Client

Connect to any MCP server and call tools:

```rust
use chevalier_mcp::client::McpClient;
use chevalier_mcp::client::McpClientConfig;
use serde_json::json;
use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> chevalier_mcp::Result<()> {
    // Connect via HTTP
    let client = McpClient::http("http://localhost:8080/mcp").await?;

    // Or via stdio (spawns a child process)
    // let client = McpClient::stdio("npx @modelcontextprotocol/server-filesystem /tmp").await?;

    // Or via WebSocket
    // let client = McpClient::websocket("ws://localhost:8080/mcp").await?;

    // Structured configs support generic HTTP/WebSocket headers and direct
    // stdio command args, environment, and working directory configuration.
    let client = McpClient::connect(McpClientConfig::Http {
        url: "http://localhost:8080/mcp".to_string(),
        headers: BTreeMap::from([(
            "authorization".to_string(),
            "Bearer token".to_string(),
        )]),
    }).await?;

    // Discover tools
    let tools = client.list_tools().await?;
    for tool in &tools.tools {
        println!("{}: {}", tool.name, tool.description.as_deref().unwrap_or(""));
    }

    // Call a tool
    let result = client.call_tool("add", json!({"a": 2, "b": 3})).await?;

    client.close().await?;
    Ok(())
}
```

## Apps Extension

Attach interactive HTML UIs to tools (requires `apps` feature):

```rust
use chevalier_mcp::apps::UiResource;

let server = McpServer::builder("my-server")
    .with_tool("chart", "Render a chart", schema, handler)
    .with_ui(UiResource::new("my-server", "chart", "<html>...</html>"))
    .build();
```

UIs are served as resources and rendered in iframes by supporting clients. See [SEP-1865](https://github.com/nicholasgasior/mcp-ui-extension) for the full specification.

## Examples

Run the examples with:

```bash
# Stdio server (connect from Claude Code with `claude mcp add`)
cargo run --example simple_server

# HTTP server
cargo run --example http_server

# Client that connects to the HTTP server
cargo run --example client_agent
```

### Connecting to Claude Code

```bash
# Add as a stdio MCP server
claude mcp add my-server -- cargo run --manifest-path /path/to/mcp/Cargo.toml --example simple_server

# Or as an HTTP server (start the server first)
cargo run --example http_server &
claude mcp add my-server --transport http http://127.0.0.1:8080/mcp
```

## License

Apache-2.0
