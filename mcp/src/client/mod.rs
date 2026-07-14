//! MCP Client - connect to MCP servers and use their tools
//!
//! This module provides a high-level client API for connecting to MCP servers
//! via various transports (HTTP, WebSocket, stdio).

mod connection;
mod tools;

pub use connection::{McpClient, McpClientConfig, McpServer, Transport};
pub use tools::ToolInfo;
