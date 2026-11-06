use chevalier::runtime::{Runtime, ToolFunction};
use chevalier::types::ToolCall;
use serde_json::{Value, json};

async fn registry() -> Runtime {
    let runtime = Runtime::new();
    for (name, description) in [
        ("calendar_lookup", "Find an appointment"),
        ("invoice_lookup", "Find a receipt"),
        ("chat", "Speak to the user"),
    ] {
        runtime
            .register_tool_with_schema(
                name,
                description,
                json!({"type":"object","properties":{}}),
                ToolFunction::Sync(Box::new(move |_| Ok(format!("executed {name}")))),
            )
            .await
            .unwrap();
    }
    runtime
}

#[tokio::test]
async fn discovers_executes_and_restores_tools_without_losing_the_catalog() {
    let runtime = registry().await;
    runtime.enable_tool_search(&["chat".into()]).await.unwrap();
    assert_eq!(
        runtime.loaded_tool_names().await.unwrap(),
        ["chat", "tool_search"]
    );
    let result = runtime
        .execute_tool_call(&ToolCall::new(
            "tool_search",
            json!({"query":"appointment"}),
        ))
        .await
        .unwrap();
    let result: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(result["tools"][0]["name"], "calendar_lookup");
    assert_eq!(
        runtime
            .execute_tool_call(&ToolCall::new("calendar_lookup", json!({})))
            .await
            .unwrap(),
        "executed calendar_lookup"
    );
    let saved = runtime.loaded_tool_names().await.unwrap();
    let resumed = registry().await;
    resumed.enable_tool_search(&saved).await.unwrap();
    resumed
        .execute_tool_call(&ToolCall::new("tool_search", json!({"query":"receipt"})))
        .await
        .unwrap();
    assert_eq!(
        resumed.loaded_tool_names().await.unwrap(),
        ["chat", "tool_search", "calendar_lookup", "invoice_lookup"]
    );
    assert_eq!(
        resumed
            .execute_tool_call(&ToolCall::new("invoice_lookup", json!({})))
            .await
            .unwrap(),
        "executed invoice_lookup"
    );
}

#[tokio::test]
async fn invalid_search_preserves_discovery_and_can_be_corrected() {
    let runtime = registry().await;
    runtime.enable_tool_search(&[]).await.unwrap();
    assert!(
        runtime
            .execute_tool_call(&ToolCall::new("tool_search", json!({"query":"["})))
            .await
            .is_err()
    );
    runtime
        .execute_tool_call(&ToolCall::new(
            "tool_search",
            json!({"query":"lookup","limit":2}),
        ))
        .await
        .unwrap();
    assert_eq!(
        runtime.loaded_tool_names().await.unwrap(),
        ["tool_search", "calendar_lookup", "invoice_lookup"]
    );
}
