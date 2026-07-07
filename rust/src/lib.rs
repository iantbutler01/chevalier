//! # Chevalier - Agents are just functions
//!
//! Production-grade LLM agent framework with structured outputs,
//! multi-provider support, and native tool calling.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use chevalier::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Coming soon: agentic macro support
//!     Ok(())
//! }
//! ```

// @dive-file: Crate root for chevalier exports, feature gates, and prelude wiring.
// @dive-rel: Exposes optional protocol/runtime integrations such as MCP and sandbox behind features.
// @dive-rel: Re-exports macros and convenience types consumed by downstream agent services.

pub mod error;
pub mod retry;
pub mod runtime;
pub mod types;

// Module declarations (implementations coming in phases)
pub mod parsers;
pub mod providers;
pub mod schema;
pub mod templating;
pub mod tools;
pub mod utils;

#[cfg(feature = "mcp")]
pub mod mcp;
#[cfg(feature = "sandbox")]
pub mod sandbox;
#[cfg(feature = "vfs")]
pub use chevalier_vfs as vfs;

// Re-export proc macros from chevalier-macros crate
pub use chevalier_macros::{Deserializable, Tool, agentic, agentic_generator};

// Re-export McpServer for convenience
#[cfg(feature = "mcp")]
pub use crate::mcp::McpServer;

// Prelude for convenient imports
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use crate::runtime::{RunParams, Runtime};
    pub use crate::types::{
        AssistantResponse, CacheMarker, ChatMessage, ChatRole, CreateResult, MediaPart,
        MediaSource, MediaSourceKind, MultimodalMessage, Provider, ProviderCapabilities,
        ProviderRateLimit, ProviderRateLimitScope, ReasoningSegment, ResponsePart,
        ResponseStreamEvent, TokenUsage, ToolCall, ToolResult,
    };
}
