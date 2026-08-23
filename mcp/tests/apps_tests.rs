//! Integration tests for MCP Apps extension (SEP-1865)
#![cfg(feature = "apps")]

// @dive-file: Integration tests validating MCP Apps extension behavior over real websocket transport.
// @dive-rel: Exercised only when the `apps` feature is enabled via Cargo required-features gating.
// @dive-rel: Verifies server-side UI metadata wiring implemented under src/apps and src/server/handler.rs.

use chevalier_mcp::apps::{
    EXTENSION_ID, MCP_APP_MIME_TYPE, UiPermissions, UiResource, UiResourceCsp, UiResourceMeta,
    UiResourceRegistry, UiResourceRender, UiToolMeta, Visibility,
};

#[tokio::test]
async fn runtime_prefix_resource_routes_and_preserves_opaque_child_uri() {
    let mut registry = UiResourceRegistry::new();
    registry.insert_runtime_prefix(
        UiResource::new("other-you", "app", "placeholder"),
        |request: chevalier_mcp::apps::UiResourceReadRequest| -> chevalier_mcp::apps::UiResourceResolveFuture {
            let uri = request.uri.to_string();
            Box::pin(async move { Ok(UiResourceRender::new(format!("rendered:{uri}"))) })
        },
    );

    assert_eq!(registry.list_resources()[0].raw.uri, "ui://other-you/app");
    let requested = "ui://other-you/app/1e62bd6d-74d3-4f9f-b60f-1bcfd76db6ca";
    let rendered = registry
        .read_resource(requested)
        .await
        .expect("prefix registered")
        .expect("rendered");
    let value = serde_json::to_value(rendered).expect("serializable");
    assert_eq!(value["contents"][0]["uri"], requested);
    assert_eq!(
        value["contents"][0]["text"],
        format!("rendered:{requested}")
    );
    assert!(
        registry
            .read_resource("ui://other-you/application/nope")
            .await
            .is_none()
    );
}
use chevalier_mcp::server::{McpServer, ServerTransport};
use rmcp::model::{CallToolResult, Content};
use serde_json::json;

const TEST_HTML: &str = "<html><body><h1>Chart</h1></body></html>";
const STATIC_LAUNCH_CREDENTIAL: &str = "static-launch-credential-must-never-be-served";

#[tokio::test]
async fn runtime_resource_lists_without_rendering_and_refreshes_read_metadata() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let renders = Arc::new(AtomicUsize::new(0));
    let observed_renders = Arc::clone(&renders);
    let mut registry = UiResourceRegistry::new();
    registry.insert_runtime(
        UiResource::new("other-you", "reference", STATIC_LAUNCH_CREDENTIAL)
            .with_description("Runtime reference UI")
            .with_csp(UiResourceCsp {
                connect_domains: Some(vec!["https://listed.invalid".into()]),
                ..Default::default()
            })
            .with_border(false),
        move |_request| -> chevalier_mcp::apps::UiResourceResolveFuture {
            let render_no = renders.fetch_add(1, Ordering::SeqCst) + 1;
            Box::pin(async move {
                Ok(UiResourceRender::new(format!(
                    "<script src=\"/assets/app.js\"></script><script>window.launch={{credential:'short-lived-{render_no}',backend:'/api/state'}}</script>"
                ))
                .with_meta(UiResourceMeta {
                    csp: Some(UiResourceCsp {
                        connect_domains: Some(vec!["https://embed.other-you.invalid".into()]),
                        resource_domains: Some(vec!["https://embed.other-you.invalid".into()]),
                        frame_domains: Some(vec![]),
                        base_uri_domains: Some(vec![]),
                    }),
                    permissions: Some(UiPermissions::default()),
                    prefers_border: Some(true),
                    domain: None,
                }))
            })
        },
    );

    let listed = registry.list_resources();
    assert_eq!(observed_renders.load(Ordering::SeqCst), 0);
    assert!(
        !serde_json::to_string(&listed)
            .expect("serializable")
            .contains(STATIC_LAUNCH_CREDENTIAL)
    );
    assert!(!format!("{registry:?}").contains(STATIC_LAUNCH_CREDENTIAL));
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].raw.uri, "ui://other-you/reference");
    assert_eq!(listed[0].raw.name, "reference");
    assert_eq!(
        listed[0].raw.description.as_deref(),
        Some("Runtime reference UI")
    );
    assert_eq!(
        listed[0]
            .raw
            .meta
            .as_ref()
            .and_then(|meta| meta.0.get("ui")),
        Some(&json!({
            "csp": {"connectDomains": ["https://listed.invalid"]},
            "prefersBorder": false
        }))
    );

    let first = registry
        .read_resource("ui://other-you/reference")
        .await
        .expect("registered")
        .expect("rendered");
    let second = registry
        .read_resource("ui://other-you/reference")
        .await
        .expect("registered")
        .expect("rendered");
    let first = serde_json::to_value(first).expect("serializable");
    let second = serde_json::to_value(second).expect("serializable");
    assert_eq!(
        first,
        json!({
            "contents": [{
                "uri": "ui://other-you/reference",
                "mimeType": MCP_APP_MIME_TYPE,
                "text": "<script src=\"/assets/app.js\"></script><script>window.launch={credential:'short-lived-1',backend:'/api/state'}</script>",
                "_meta": {"ui": {
                    "csp": {
                        "connectDomains": ["https://embed.other-you.invalid"],
                        "resourceDomains": ["https://embed.other-you.invalid"],
                        "frameDomains": [],
                        "baseUriDomains": []
                    },
                    "permissions": {},
                    "prefersBorder": true
                }}
            }]
        })
    );
    let second = second.to_string();
    assert!(second.contains("short-lived-2"));
    assert!(
        !second.contains("short-lived-1"),
        "credentials must not be reused"
    );
    assert!(!second.contains(STATIC_LAUNCH_CREDENTIAL));
}

#[tokio::test]
async fn runtime_resource_preserves_resolver_errors_and_skips_unknown_uris() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let expected = rmcp::ErrorData::internal_error("render failed", Some(json!({"retry": true})));
    let resolver_error = expected.clone();
    let mut registry = UiResourceRegistry::new();
    registry.insert_runtime(
        UiResource::new("other-you", "reference", "placeholder"),
        move |_request| -> chevalier_mcp::apps::UiResourceResolveFuture {
            calls.fetch_add(1, Ordering::SeqCst);
            let error = resolver_error.clone();
            Box::pin(async move { Err(error) })
        },
    );

    assert!(
        registry
            .read_resource("ui://other-you/not-registered")
            .await
            .is_none()
    );
    assert_eq!(observed_calls.load(Ordering::SeqCst), 0);

    let error = registry
        .read_resource("ui://other-you/reference")
        .await
        .expect("registered")
        .expect_err("resolver should fail");
    assert_eq!(error, expected);
    assert_eq!(observed_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn runtime_resource_reads_can_resolve_concurrently() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::Barrier;

    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let barrier = Arc::new(Barrier::new(2));
    let mut registry = UiResourceRegistry::new();
    registry.insert_runtime(
        UiResource::new("other-you", "reference", "placeholder"),
        move |_request| -> chevalier_mcp::apps::UiResourceResolveFuture {
            let render_no = calls.fetch_add(1, Ordering::SeqCst) + 1;
            let barrier = Arc::clone(&barrier);
            Box::pin(async move {
                barrier.wait().await;
                Ok(UiResourceRender::new(format!("render-{render_no}")))
            })
        },
    );

    let (first, second) = tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
        tokio::join!(
            registry.read_resource("ui://other-you/reference"),
            registry.read_resource("ui://other-you/reference")
        )
    })
    .await
    .expect("concurrent reads should both enter the resolver");

    let mut rendered = [first, second]
        .into_iter()
        .map(|result| {
            let result = result.expect("registered").expect("rendered");
            match result.contents.into_iter().next().expect("one content") {
                rmcp::model::ResourceContents::TextResourceContents { text, .. } => text,
                _ => panic!("expected text resource"),
            }
        })
        .collect::<Vec<_>>();
    rendered.sort();
    assert_eq!(rendered, ["render-1", "render-2"]);
    assert_eq!(observed_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn runtime_resource_resolver_is_wired_through_mcp_transport() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let observed_meta: Arc<std::sync::Mutex<Option<rmcp::model::Meta>>> =
        Arc::new(std::sync::Mutex::new(None));
    let resolver_meta = Arc::clone(&observed_meta);
    let server = McpServer::builder("runtime-apps-server")
        .with_tool(
            "runtime",
            "Runtime-rendered UI",
            json!({"type": "object"}),
            |_name, _args| {
                Box::pin(async move { Ok(CallToolResult::success(vec![Content::text("ok")])) })
            },
        )
        .with_ui_resolver(
            UiResource::new("runtime-apps-server", "runtime", "placeholder").with_csp(
                UiResourceCsp {
                    connect_domains: Some(vec!["https://listed.invalid".into()]),
                    ..Default::default()
                },
            ),
            move |request: chevalier_mcp::apps::UiResourceReadRequest| -> chevalier_mcp::apps::UiResourceResolveFuture {
                assert_eq!(request.uri.as_str(), "ui://runtime-apps-server/runtime");
                *resolver_meta.lock().expect("meta lock poisoned") = request.meta;
                let render_no = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Box::pin(async move {
                    Ok(
                        UiResourceRender::new(format!("render-{render_no}")).with_meta(
                            UiResourceMeta {
                                csp: Some(UiResourceCsp {
                                    connect_domains: Some(vec![format!(
                                        "https://render-{render_no}.invalid"
                                    )]),
                                    ..Default::default()
                                }),
                                permissions: Some(UiPermissions::default()),
                                ..Default::default()
                            },
                        ),
                    )
                })
            },
        )
        .build();

    let server_task = tokio::spawn(async move {
        server
            .serve(ServerTransport::WebSocket("127.0.0.1:18209".into()))
            .await
            .expect("server failed");
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18209")
        .await
        .expect("client failed to connect");

    let listed = client
        .list_resources()
        .await
        .expect("resource listing failed");
    assert_eq!(listed.resources.len(), 1);
    assert_eq!(
        listed.resources[0].raw.uri,
        "ui://runtime-apps-server/runtime"
    );
    assert_eq!(observed_calls.load(Ordering::SeqCst), 0);

    let request_meta = || {
        Some(rmcp::model::Meta(serde_json::Map::from_iter([(
            "requester".into(),
            json!("authenticated-host"),
        )])))
    };
    let rendered = serde_json::to_value(
        tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            client.read_resource_with_meta("ui://runtime-apps-server/runtime", request_meta()),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "resource read timed out after {} resolver calls with metadata {:?}",
                observed_calls.load(Ordering::SeqCst),
                observed_meta.lock().expect("meta lock poisoned")
            )
        })
        .expect("resource read failed"),
    )
    .unwrap();
    assert_eq!(rendered["contents"][0]["text"], "render-1");
    assert_eq!(
        rendered["contents"][0]["_meta"]["ui"],
        json!({
            "csp": {"connectDomains": ["https://render-1.invalid"]},
            "permissions": {}
        })
    );
    assert_eq!(observed_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        observed_meta
            .lock()
            .expect("meta lock poisoned")
            .as_ref()
            .and_then(|meta| meta.0.get("requester")),
        Some(&json!("authenticated-host"))
    );

    drop(client);
    server_task.abort();
}

fn build_apps_server() -> McpServer {
    McpServer::builder("apps-test-server")
        .with_version("1.0.0")
        .with_description("A server with UI tools")
        .with_tool(
            "chart",
            "Render a chart",
            json!({
                "type": "object",
                "properties": {
                    "data": { "type": "string" }
                },
                "required": ["data"]
            }),
            |_name, args| {
                Box::pin(async move {
                    let args = args.unwrap_or_default();
                    let data = args
                        .get("data")
                        .and_then(|v| v.as_str())
                        .unwrap_or("no data");
                    Ok(CallToolResult::success(vec![Content::text(format!(
                        "Rendered: {}",
                        data
                    ))]))
                })
            },
        )
        .with_ui(
            UiResource::new("apps-test-server", "chart", TEST_HTML)
                .with_description("Interactive chart UI"),
        )
        .with_tool(
            "plain",
            "A plain tool with no UI",
            json!({"type": "object"}),
            |_name, _args| {
                Box::pin(async move { Ok(CallToolResult::success(vec![Content::text("ok")])) })
            },
        )
        .build()
}

async fn start_apps_server(port: u16) -> tokio::task::JoinHandle<()> {
    let server = build_apps_server();
    let addr = format!("127.0.0.1:{}", port);
    tokio::spawn(async move {
        server
            .serve(ServerTransport::WebSocket(addr))
            .await
            .expect("Server failed");
    })
}

#[tokio::test]
async fn test_tool_has_ui_meta() {
    let _server = start_apps_server(18201).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18201")
        .await
        .expect("Failed to connect");

    let tools = client.list_tools().await.expect("Failed to list tools");
    assert_eq!(tools.tools.len(), 2);

    // The "chart" tool should have _meta.ui with resourceUri
    let chart_tool = tools.tools.iter().find(|t| t.name == "chart").unwrap();
    let meta = chart_tool
        .meta
        .as_ref()
        .expect("chart tool should have _meta");
    let ui_value = meta.0.get("ui").expect("_meta should have 'ui' key");
    let ui_meta: UiToolMeta =
        serde_json::from_value(ui_value.clone()).expect("should deserialize as UiToolMeta");
    assert_eq!(ui_meta.resource_uri.scheme(), "ui");
    assert_eq!(ui_meta.resource_uri.host_str(), Some("apps-test-server"));

    // The "plain" tool should have no _meta
    let plain_tool = tools.tools.iter().find(|t| t.name == "plain").unwrap();
    assert!(plain_tool.meta.is_none());

    client.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_list_resources() {
    let _server = start_apps_server(18202).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18202")
        .await
        .expect("Failed to connect");

    let resources = client
        .list_resources()
        .await
        .expect("Failed to list resources");

    assert_eq!(resources.resources.len(), 1);
    let resource = &resources.resources[0];
    assert_eq!(resource.raw.name, "chart");
    assert!(resource.raw.uri.starts_with("ui://apps-test-server/"));
    assert_eq!(resource.raw.mime_type.as_deref(), Some(MCP_APP_MIME_TYPE));
    assert_eq!(
        resource.raw.description.as_deref(),
        Some("Interactive chart UI")
    );

    client.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_read_resource() {
    let _server = start_apps_server(18203).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18203")
        .await
        .expect("Failed to connect");

    // Read the resource by its URI
    let result = client
        .read_resource("ui://apps-test-server/chart")
        .await
        .expect("Failed to read resource");

    assert_eq!(result.contents.len(), 1);
    match &result.contents[0] {
        rmcp::model::ResourceContents::TextResourceContents {
            text, mime_type, ..
        } => {
            assert_eq!(text, TEST_HTML);
            assert_eq!(mime_type.as_deref(), Some(MCP_APP_MIME_TYPE));
        }
        _ => panic!("Expected TextResourceContents"),
    }

    client.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_read_resource_not_found() {
    let _server = start_apps_server(18204).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18204")
        .await
        .expect("Failed to connect");

    let result = client
        .read_resource("ui://apps-test-server/nonexistent")
        .await;
    assert!(result.is_err(), "Reading nonexistent resource should fail");

    client.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_ui_resource_with_csp() {
    let resource = UiResource::new("my-server", "dashboard", "<html></html>")
        .with_description("Dashboard")
        .with_csp(UiResourceCsp {
            connect_domains: Some(vec!["api.example.com".to_string()]),
            resource_domains: None,
            frame_domains: None,
            base_uri_domains: None,
        });

    assert_eq!(resource.name, "dashboard");
    assert!(resource.meta.is_some());
    let meta = resource.meta.unwrap();
    let csp = meta.csp.unwrap();
    assert_eq!(
        csp.connect_domains,
        Some(vec!["api.example.com".to_string()])
    );
}

#[tokio::test]
async fn test_server_capabilities_include_resources() {
    let _server = start_apps_server(18205).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18205")
        .await
        .expect("Failed to connect");

    let info = client
        .server_info()
        .expect("Server info should be available");

    // Server with UI resources should advertise resources capability
    assert!(
        info.capabilities.resources.is_some(),
        "Server with UI tools should have resources capability"
    );
    assert_eq!(
        info.capabilities
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.get(EXTENSION_ID)),
        Some(&Default::default()),
        "Server with UI tools should advertise the MCP Apps extension"
    );

    client.close().await.expect("Failed to close");
}

// --- Visibility tests ---

#[tokio::test]
async fn test_tool_visibility_set_via_builder() {
    let server = McpServer::builder("vis-server")
        .with_tool(
            "model_only",
            "Only for the LLM",
            json!({"type": "object"}),
            |_name, _args| {
                Box::pin(async move { Ok(CallToolResult::success(vec![Content::text("ok")])) })
            },
        )
        .with_ui(UiResource::new("vis-server", "ui1", "<html></html>"))
        .visibility(vec![Visibility::Model])
        .with_tool(
            "app_only",
            "Only for the iframe",
            json!({"type": "object"}),
            |_name, _args| {
                Box::pin(async move { Ok(CallToolResult::success(vec![Content::text("ok")])) })
            },
        )
        .with_ui(UiResource::new("vis-server", "ui2", "<html></html>"))
        .visibility(vec![Visibility::App])
        .with_tool(
            "both",
            "For model and app",
            json!({"type": "object"}),
            |_name, _args| {
                Box::pin(async move { Ok(CallToolResult::success(vec![Content::text("ok")])) })
            },
        )
        .with_ui(UiResource::new("vis-server", "ui3", "<html></html>"))
        .visibility(vec![Visibility::Model, Visibility::App])
        .build();

    let addr = "127.0.0.1:18206";
    tokio::spawn(async move {
        server
            .serve(ServerTransport::WebSocket(addr.into()))
            .await
            .unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18206")
        .await
        .expect("Failed to connect");

    let tools = client.list_tools().await.unwrap();
    assert_eq!(tools.tools.len(), 3);

    let model_only = tools.tools.iter().find(|t| t.name == "model_only").unwrap();
    let ui = model_only.meta.as_ref().unwrap().0.get("ui").unwrap();
    let vis: Vec<String> = ui["visibility"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(vis, vec!["model"]);

    let app_only = tools.tools.iter().find(|t| t.name == "app_only").unwrap();
    let ui = app_only.meta.as_ref().unwrap().0.get("ui").unwrap();
    let vis: Vec<String> = ui["visibility"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(vis, vec!["app"]);

    let both = tools.tools.iter().find(|t| t.name == "both").unwrap();
    let ui = both.meta.as_ref().unwrap().0.get("ui").unwrap();
    let vis: Vec<String> = ui["visibility"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(vis, vec!["model", "app"]);

    client.close().await.unwrap();
}

#[tokio::test]
async fn test_default_visibility_omitted() {
    // When no .visibility() is chained, _meta.ui should not have a visibility field
    let server = McpServer::builder("default-vis-server")
        .with_tool(
            "default_tool",
            "Default visibility",
            json!({"type": "object"}),
            |_name, _args| {
                Box::pin(async move { Ok(CallToolResult::success(vec![Content::text("ok")])) })
            },
        )
        .with_ui(UiResource::new("default-vis-server", "ui", "<html></html>"))
        .build();

    let addr = "127.0.0.1:18207";
    tokio::spawn(async move {
        server
            .serve(ServerTransport::WebSocket(addr.into()))
            .await
            .unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18207")
        .await
        .expect("Failed to connect");

    let tools = client.list_tools().await.unwrap();
    let tool = &tools.tools[0];
    let ui = tool.meta.as_ref().unwrap().0.get("ui").unwrap();
    // No visibility field means default ["model", "app"] per spec
    assert!(ui.get("visibility").is_none());

    client.close().await.unwrap();
}

// --- E2E resource flow: tool resourceUri -> resources/read ---

#[tokio::test]
async fn test_e2e_tool_resource_uri_resolves() {
    let _server = start_apps_server(18208).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = chevalier_mcp::client::McpClient::websocket("ws://127.0.0.1:18208")
        .await
        .expect("Failed to connect");

    // 1. List tools and find one with a UI resource
    let tools = client.list_tools().await.unwrap();
    let chart_tool = tools.tools.iter().find(|t| t.name == "chart").unwrap();

    // 2. Extract the resourceUri from _meta.ui
    let ui_meta: UiToolMeta = serde_json::from_value(
        chart_tool
            .meta
            .as_ref()
            .unwrap()
            .0
            .get("ui")
            .unwrap()
            .clone(),
    )
    .unwrap();
    let resource_uri = ui_meta.resource_uri.to_string();

    // 3. Read that resource by its URI
    let read_result = client.read_resource(&resource_uri).await.unwrap();
    assert_eq!(read_result.contents.len(), 1);

    // 4. Verify we got the HTML
    match &read_result.contents[0] {
        rmcp::model::ResourceContents::TextResourceContents {
            text, mime_type, ..
        } => {
            assert_eq!(text, TEST_HTML);
            assert_eq!(mime_type.as_deref(), Some(MCP_APP_MIME_TYPE));
        }
        _ => panic!("Expected TextResourceContents"),
    }

    client.close().await.unwrap();
}
