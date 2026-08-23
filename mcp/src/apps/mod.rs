//! MCP Apps Extension (SEP-1865) - Interactive HTML UIs in MCP
//!
//! This module implements the MCP Apps extension which allows tools to return
//! interactive HTML UIs that are rendered in iframes within the conversation.

mod bridge;
mod resource;
mod types;

pub use bridge::{
    AppInfo, HostContext, McpUiAppCapabilities, MessageParams, OpenLinkParams,
    RequestDisplayModeParams, RequestDisplayModeResult, ResourceTeardownParams,
    SandboxResourceReadyParams, SizeChangedParams, ToolCancelledParams, ToolInputParams,
    ToolResultParams, UiInitializeParams, UiInitializeResult, UpdateModelContextParams,
    methods as bridge_methods,
};
pub use resource::{
    UiResourceReadRequest, UiResourceRegistry, UiResourceRender, UiResourceResolveFuture,
    UiResourceResolver,
};
pub use types::{
    DisplayMode, EXTENSION_ID, MCP_APP_MIME_TYPE, UI_SCHEME, UiPermissions, UiResource,
    UiResourceCsp, UiResourceMeta, UiToolMeta, Visibility, ui_uri,
};
