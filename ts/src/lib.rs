//! Node-API (TypeScript) bindings for Chevalier — agents are just functions.
//!
//! Wraps the high-level `chevalier_core::runtime::Runtime` directly. The hard
//! parts (JS tool callbacks via ThreadsafeFunction, streaming via async
//! iterators) are layered on in later phases.

mod error;
mod mcp;
mod messages;
mod programmatic;
mod runtime;
mod stream;
mod types;
mod vfs;

pub use mcp::{McpClient, McpServer, McpServerOptions};
pub use messages::Message;
pub use programmatic::{ProgrammaticExecution, programmatic_description};
pub use runtime::{RunOptions, Runtime, RuntimeOptions};
pub use stream::{StreamEvent, StreamHandle};
pub use types::{RunResult, ToolCallJs, ToolSchemaJs};
pub use vfs::{GatewayOptions, VfsStorage};

use napi_derive::napi;

/// The chevalier-node binding version.
#[napi]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
