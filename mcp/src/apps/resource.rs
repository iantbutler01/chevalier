//! UI resource registry
//!
//! Stores registered UI resources and provides lookups for serving
//! via MCP `resources/list` and `resources/read`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rmcp::{
    ErrorData,
    model::{Annotated, Meta, RawResource, ReadResourceResult, Resource, ResourceContents},
};
use serde_json::{Value, json};
use url::Url;

use super::types::{MCP_APP_MIME_TYPE, UiResource, UiResourceMeta};

/// The request passed to a runtime UI resource resolver.
#[derive(Debug, Clone)]
pub struct UiResourceReadRequest {
    /// URI requested by the MCP host.
    pub uri: Url,
    /// Protocol metadata supplied by the MCP host for this read.
    ///
    /// Runtime resolvers may use authenticated identity placed here by their
    /// host. Chevalier passes it through unchanged and does not interpret it.
    pub meta: Option<Meta>,
}

/// HTML and optional per-render metadata produced by a runtime resolver.
#[derive(Debug, Clone)]
pub struct UiResourceRender {
    /// Rendered MCP App HTML.
    pub html: String,
    /// Metadata for this render. When absent, the resource's registered metadata is used.
    pub meta: Option<UiResourceMeta>,
}

impl UiResourceRender {
    /// Create a render using the metadata registered on the resource.
    pub fn new(html: impl Into<String>) -> Self {
        Self {
            html: html.into(),
            meta: None,
        }
    }

    /// Override the registered metadata for this render.
    pub fn with_meta(mut self, meta: UiResourceMeta) -> Self {
        self.meta = Some(meta);
        self
    }
}

/// Future returned by a runtime UI resource resolver.
pub type UiResourceResolveFuture =
    Pin<Box<dyn Future<Output = Result<UiResourceRender, ErrorData>> + Send + 'static>>;

/// Async runtime-backed source for MCP App HTML.
///
/// Resolvers run for every `resources/read`, allowing production HTML to include
/// fresh launch configuration without changing the listed resource identity.
pub trait UiResourceResolver: Send + Sync + 'static {
    /// Render the requested resource.
    fn resolve(&self, request: UiResourceReadRequest) -> UiResourceResolveFuture;
}

impl<F> UiResourceResolver for F
where
    F: Fn(UiResourceReadRequest) -> UiResourceResolveFuture + Send + Sync + 'static,
{
    fn resolve(&self, request: UiResourceReadRequest) -> UiResourceResolveFuture {
        self(request)
    }
}

#[derive(Clone)]
struct UiResourceEntry {
    resource: Arc<UiResource>,
    resolver: Option<Arc<dyn UiResourceResolver>>,
    match_descendants: bool,
}

/// Registry of UI resources available to MCP hosts.
///
/// Stores `UiResource` instances keyed by their `ui://` URI and provides
/// conversion to rmcp protocol types for `resources/list` and `resources/read`.
#[derive(Clone, Default)]
pub struct UiResourceRegistry {
    resources: HashMap<String, UiResourceEntry>,
}

impl std::fmt::Debug for UiResourceRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UiResourceRegistry")
            .field("resources", &self.resources.keys())
            .finish()
    }
}

impl UiResourceRegistry {
    /// Create an empty registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a UI resource. Replaces any existing resource with the same URI.
    pub fn insert(&mut self, resource: UiResource) {
        self.resources.insert(
            resource.uri.to_string(),
            UiResourceEntry {
                resource: Arc::new(resource),
                resolver: None,
                match_descendants: false,
            },
        );
    }

    /// Register a UI resource whose HTML is resolved asynchronously for every read.
    pub fn insert_runtime<R>(&mut self, resource: UiResource, resolver: R)
    where
        R: UiResourceResolver,
    {
        self.resources.insert(
            resource.uri.to_string(),
            UiResourceEntry {
                resource: Arc::new(resource),
                resolver: Some(Arc::new(resolver)),
                match_descendants: false,
            },
        );
    }

    /// Register a runtime resource which also resolves opaque child paths.
    ///
    /// The registered URI remains the single listed resource identity. A read of
    /// `<registered-uri>/<opaque-id>` is dispatched to the resolver without
    /// requiring mutable per-instance registration in a shared MCP server.
    pub fn insert_runtime_prefix<R>(&mut self, resource: UiResource, resolver: R)
    where
        R: UiResourceResolver,
    {
        self.resources.insert(
            resource.uri.to_string(),
            UiResourceEntry {
                resource: Arc::new(resource),
                resolver: Some(Arc::new(resolver)),
                match_descendants: true,
            },
        );
    }

    /// Look up a resource by URI string
    pub fn get(&self, uri: &str) -> Option<&Arc<UiResource>> {
        self.resources.get(uri).map(|entry| &entry.resource)
    }

    /// Look up a resource by parsed URL
    pub fn get_by_url(&self, uri: &Url) -> Option<&Arc<UiResource>> {
        self.resources
            .get(uri.as_str())
            .map(|entry| &entry.resource)
    }

    /// Number of registered resources
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    /// Whether the registry is empty
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    /// Convert all registered resources to rmcp `Resource` values for `resources/list`.
    ///
    /// Each resource is returned with its `ui://` URI, name, description, and
    /// the MCP App MIME type. The `_meta.ui` field carries CSP, permissions,
    /// and display preferences per SEP-1865.
    pub fn list_resources(&self) -> Vec<Resource> {
        self.resources
            .values()
            .map(|entry| resource_to_mcp(&entry.resource))
            .collect()
    }

    /// Read a single resource by URI, returning rmcp's `ReadResourceResult`.
    ///
    /// Returns `None` if the URI is not registered.
    pub async fn read_resource(&self, uri: &str) -> Option<Result<ReadResourceResult, ErrorData>> {
        self.read_resource_with_meta(uri, None).await
    }

    /// Read a resource while preserving protocol request metadata for runtime resolvers.
    pub async fn read_resource_with_meta(
        &self,
        uri: &str,
        meta: Option<Meta>,
    ) -> Option<Result<ReadResourceResult, ErrorData>> {
        let entry = self.resources.get(uri).cloned().or_else(|| {
            self.resources
                .iter()
                .filter(|(prefix, entry)| {
                    entry.match_descendants
                        && uri
                            .strip_prefix(prefix.as_str())
                            .is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1)
                })
                .max_by_key(|(prefix, _)| prefix.len())
                .map(|(_, entry)| entry.clone())
        })?;
        let render = if let Some(resolver) = entry.resolver {
            let uri = match Url::parse(uri) {
                Ok(uri) => uri,
                Err(error) => {
                    return Some(Err(ErrorData::invalid_params(
                        format!("Invalid resource URI: {error}"),
                        None,
                    )));
                }
            };
            match resolver.resolve(UiResourceReadRequest { uri, meta }).await {
                Ok(render) => render,
                Err(error) => return Some(Err(error)),
            }
        } else {
            UiResourceRender::new(entry.resource.content.clone())
        };

        Some(Ok(read_resource_result(&entry.resource, uri, render)))
    }
}

/// Convert a `UiResource` to an rmcp `Resource` for listing.
fn resource_to_mcp(r: &UiResource) -> Resource {
    let mut meta = serde_json::Map::new();
    if let Some(ui_meta) = &r.meta {
        meta.insert("ui".to_string(), ui_meta_to_value(ui_meta));
    }

    Annotated {
        raw: RawResource {
            uri: r.uri.to_string(),
            name: r.name.clone(),
            title: None,
            description: r.description.clone(),
            mime_type: Some(MCP_APP_MIME_TYPE.to_string()),
            size: None,
            icons: None,
            meta: if meta.is_empty() {
                None
            } else {
                Some(Meta(meta))
            },
        },
        annotations: None,
    }
}

/// Build a `ReadResourceResult` for a `UiResource`.
fn read_resource_result(
    r: &UiResource,
    requested_uri: &str,
    render: UiResourceRender,
) -> ReadResourceResult {
    let mut content_meta = serde_json::Map::new();
    if let Some(ui_meta) = render.meta.as_ref().or(r.meta.as_ref()) {
        content_meta.insert("ui".to_string(), ui_meta_to_value(ui_meta));
    }

    ReadResourceResult {
        contents: vec![ResourceContents::TextResourceContents {
            uri: requested_uri.to_string(),
            mime_type: Some(MCP_APP_MIME_TYPE.to_string()),
            text: render.html,
            meta: if content_meta.is_empty() {
                None
            } else {
                Some(Meta(content_meta))
            },
        }],
    }
}

/// Serialize `UiResourceMeta` to a JSON `Value` for the `_meta.ui` field.
fn ui_meta_to_value(meta: &UiResourceMeta) -> Value {
    // Use serde to get the full representation, which already has
    // camelCase renaming and skip_serializing_if applied.
    serde_json::to_value(meta).unwrap_or(json!({}))
}
