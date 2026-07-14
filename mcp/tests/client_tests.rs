//! Integration tests for MCP client transports

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use chevalier_mcp::WebSocketTransport;
use chevalier_mcp::client::{McpClient, McpClientConfig};
use rmcp::{
    RoleServer, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use serde_json::json;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

// Test Calculator server - same as rmcp example
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SumRequest {
    #[schemars(description = "the left hand side number")]
    pub a: i32,
    pub b: i32,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SubRequest {
    #[schemars(description = "the left hand side number")]
    pub a: i32,
    #[schemars(description = "the right hand side number")]
    pub b: i32,
}

#[derive(Debug, Clone)]
pub struct Calculator {
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl Calculator {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Calculate the sum of two numbers")]
    fn sum(&self, Parameters(SumRequest { a, b }): Parameters<SumRequest>) -> String {
        (a + b).to_string()
    }

    #[tool(description = "Calculate the difference of two numbers")]
    fn sub(&self, Parameters(SubRequest { a, b }): Parameters<SubRequest>) -> String {
        (a - b).to_string()
    }
}

#[tool_handler]
impl ServerHandler for Calculator {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some("A simple calculator".into()),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

/// Start a WebSocket MCP server on the given port using our real WebSocketTransport
async fn start_ws_server(port: u16) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .expect("Failed to bind TCP listener");

    tokio::spawn(async move {
        while let Ok((stream, _addr)) = listener.accept().await {
            tokio::spawn(async move {
                let ws_stream = tokio_tungstenite::accept_async(stream)
                    .await
                    .expect("Failed to accept WebSocket");
                // Use our real WebSocketTransport for the server side too
                let transport: WebSocketTransport<RoleServer, _, _> =
                    WebSocketTransport::new(ws_stream);
                let server = Calculator::new()
                    .serve(transport)
                    .await
                    .expect("Failed to serve");
                let _ = server.waiting().await;
            });
        }
    })
}

async fn start_http_header_server() -> (
    String,
    oneshot::Receiver<Option<String>>,
    tokio::task::JoinHandle<()>,
) {
    let cancellation = CancellationToken::new();
    let service = StreamableHttpService::new(
        || Ok(Calculator::new()),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig {
            cancellation_token: cancellation,
            stateful_mode: true,
            ..Default::default()
        },
    );
    let (header_tx, header_rx) = oneshot::channel();
    let header_tx = Arc::new(Mutex::new(Some(header_tx)));
    let router =
        axum::Router::new()
            .nest_service("/mcp", service)
            .layer(axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let header_tx = Arc::clone(&header_tx);
                    async move {
                        let value = request
                            .headers()
                            .get("x-chevalier-test")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string);
                        if let Some(tx) = header_tx.lock().expect("header lock poisoned").take() {
                            let _ = tx.send(value);
                        }
                        next.run(request).await
                    }
                },
            ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind HTTP listener");
    let address = listener
        .local_addr()
        .expect("Missing HTTP listener address");
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("HTTP server failed");
    });

    (format!("http://{address}/mcp"), header_rx, server)
}

async fn start_ws_header_server() -> (
    String,
    oneshot::Receiver<Option<String>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind WebSocket listener");
    let address = listener
        .local_addr()
        .expect("Missing WebSocket listener address");
    let (header_tx, header_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .expect("Failed to accept connection");
        let ws_stream = tokio_tungstenite::accept_hdr_async(
            stream,
            move |
                request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                response: tokio_tungstenite::tungstenite::handshake::server::Response,
            | {
                let value = request
                    .headers()
                    .get("x-chevalier-test")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                let _ = header_tx.send(value);
                Ok(response)
            },
        )
        .await
        .expect("WebSocket handshake failed");
        let transport: WebSocketTransport<RoleServer, _, _> = WebSocketTransport::new(ws_stream);
        let service = Calculator::new()
            .serve(transport)
            .await
            .expect("Failed to serve");
        let _ = service.waiting().await;
    });

    (format!("ws://{address}"), header_rx, server)
}

#[tokio::test]
async fn test_structured_http_config_passes_headers() {
    let (url, header_rx, server) = start_http_header_server().await;
    let client = McpClient::connect(McpClientConfig::Http {
        url,
        headers: BTreeMap::from([("x-chevalier-test".to_string(), "http-header".to_string())]),
    })
    .await
    .expect("Failed to connect over structured HTTP config");

    assert_eq!(
        header_rx.await.expect("Header capture dropped").as_deref(),
        Some("http-header")
    );
    client.close().await.expect("Failed to close");
    server.abort();
}

#[tokio::test]
async fn test_structured_websocket_config_passes_handshake_headers() {
    let (url, header_rx, server) = start_ws_header_server().await;
    let client = McpClient::connect(McpClientConfig::WebSocket {
        url,
        headers: BTreeMap::from([(
            "x-chevalier-test".to_string(),
            "websocket-header".to_string(),
        )]),
    })
    .await
    .expect("Failed to connect over structured WebSocket config");

    assert_eq!(
        header_rx.await.expect("Header capture dropped").as_deref(),
        Some("websocket-header")
    );
    client.close().await.expect("Failed to close");
    server.abort();
}

#[tokio::test]
async fn test_websocket_list_tools() {
    // Start test server
    let _server = start_ws_server(18081).await;
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect client
    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18081")
        .await
        .expect("Failed to connect");

    // List tools
    let tools = client.list_tools().await.expect("Failed to list tools");

    // Verify we got the calculator tools
    assert_eq!(tools.tools.len(), 2);

    let tool_names: Vec<_> = tools.tools.iter().map(|t| t.name.as_ref()).collect();
    assert!(tool_names.contains(&"sum"));
    assert!(tool_names.contains(&"sub"));

    // Clean up
    client.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_websocket_call_tool() {
    // Start test server
    let _server = start_ws_server(18082).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect client
    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18082")
        .await
        .expect("Failed to connect");

    // Call sum tool
    let result = client
        .call_tool("sum", json!({"a": 5, "b": 3}))
        .await
        .expect("Failed to call tool");

    // Check result
    assert!(!result.is_error.unwrap_or(false));
    assert!(!result.content.is_empty());

    // The result should contain "8"
    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .expect("Expected text content");
    assert_eq!(text.text, "8");

    client.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_websocket_call_sub_tool() {
    // Start test server
    let _server = start_ws_server(18083).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect client
    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18083")
        .await
        .expect("Failed to connect");

    // Call sub tool
    let result = client
        .call_tool("sub", json!({"a": 10, "b": 4}))
        .await
        .expect("Failed to call tool");

    // Check result
    assert!(!result.is_error.unwrap_or(false));

    let text = result
        .content
        .first()
        .and_then(|c| c.as_text())
        .expect("Expected text content");
    assert_eq!(text.text, "6");

    client.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_websocket_server_info() {
    // Start test server
    let _server = start_ws_server(18084).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect client
    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18084")
        .await
        .expect("Failed to connect");

    // Get server info
    let info = client
        .server_info()
        .expect("Server info should be available");

    assert_eq!(info.instructions.as_deref(), Some("A simple calculator"));

    client.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_websocket_connection_failure() {
    // Try to connect to a non-existent server
    let result = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:19999").await;

    assert!(result.is_err());
    match result {
        Err(chevalier_mcp::Error::Transport(_)) => {} // Expected
        Err(e) => panic!("Expected Transport error, got: {}", e),
        Ok(_) => panic!("Expected error, got success"),
    }
}

#[tokio::test]
async fn test_mcp_server_config_websocket() {
    // Start test server
    let _server = start_ws_server(18085).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Use McpServer config pattern
    let client = chevalier_mcp::client::McpServer::websocket("ws://127.0.0.1:18085")
        .connect()
        .await
        .expect("Failed to connect via McpServer config");

    let tools = client.list_tools().await.expect("Failed to list tools");
    assert_eq!(tools.tools.len(), 2);

    client.close().await.expect("Failed to close");
}
