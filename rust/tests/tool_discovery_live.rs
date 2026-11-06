//! Explicitly invoked live inference test. Set CHEVALIER_DISCOVERY_MODEL to the
//! complete model route and provide that route's normal credential environment.
use chevalier::runtime::{RunParams, Runtime, ToolFunction};
use chevalier::types::{ChatMessage, ToolResult};
use chevalier::utils::ConversationMessage;
use serde_json::json;
use std::time::{Duration, Instant};

async fn registry() -> Runtime {
    let runtime = Runtime::new();
    for (name, description, file) in [
        (
            "read_appointment_record",
            "Read the saved appointment confirmation and arrival time",
            "appointment.txt",
        ),
        (
            "read_invoice_record",
            "Read the saved invoice confirmation and total paid",
            "invoice.txt",
        ),
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/tool-discovery")
            .join(file);
        runtime
            .register_tool_with_schema(
                name,
                description,
                json!({"type":"object","properties":{},"additionalProperties":false}),
                ToolFunction::Sync(Box::new(move |_| {
                    std::fs::read_to_string(&path)
                        .map_err(|error| chevalier::error::Error::NonRetryable(error.to_string()))
                })),
            )
            .await
            .unwrap();
    }
    runtime
}

async fn complete(
    runtime: &mut Runtime,
    model: &str,
    request: &str,
    expected: &str,
) -> Vec<String> {
    runtime.set_system_messages(vec![ChatMessage::system("Use the registered tools to read the requested record. Discover a missing capability with tool_search. Do not guess record contents. Return the confirmation and requested detail when you have read the record.")]).await;
    let mut history = vec![ConversationMessage::Chat(ChatMessage::user(request))];
    let mut called = Vec::new();
    let started = Instant::now();
    for _ in 0..10 {
        let response = runtime
            .run(RunParams {
                model: Some(model.into()),
                history: Some(history.clone()),
                max_tokens: Some(2048),
                timeout: Some(Duration::from_secs(120)),
                ..Default::default()
            })
            .await
            .expect("live provider generation");
        let calls = response
            .tool_calls()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        if calls.is_empty() {
            assert!(
                response.text().contains(expected),
                "actual response: {}",
                response.text()
            );
            eprintln!(
                "{}",
                json!({"model":model,"elapsed_ms":started.elapsed().as_millis(),"calls":called,"success":true})
            );
            return called;
        }
        history.push(ConversationMessage::AssistantResponse(response));
        for call in calls {
            let loaded = runtime.loaded_tool_names().await.unwrap();
            assert!(
                loaded.contains(&call.tool_name),
                "model called undiscovered {}",
                call.tool_name
            );
            let content = runtime
                .execute_tool_call(&call)
                .await
                .expect("execute real registered tool");
            called.push(call.tool_name.clone());
            history.push(ConversationMessage::ToolResult(
                ToolResult::success_with_name(call.tool_use_id, call.tool_name, content),
            ));
        }
    }
    panic!("live task did not finish within ten model turns");
}

#[tokio::test]
#[ignore = "Calls a real provider; requires CHEVALIER_DISCOVERY_MODEL and credentials"]
async fn discovers_executes_and_resumes_through_live_provider() {
    let model = std::env::var("CHEVALIER_DISCOVERY_MODEL").expect("explicit model route required");
    let mut runtime = registry().await;
    runtime.enable_tool_search(&[]).await.unwrap();
    let first = complete(
        &mut runtime,
        &model,
        "Read my appointment record and give me its confirmation code and arrival time.",
        "violet-cedar-7281",
    )
    .await;
    assert!(first.contains(&"tool_search".into()));
    assert!(first.contains(&"read_appointment_record".into()));
    let saved = serde_json::to_string(&runtime.loaded_tool_names().await.unwrap()).unwrap();
    drop(runtime);
    // No old conversation is retained: discovery must survive independently of it.
    let mut resumed = registry().await;
    let restored: Vec<String> = serde_json::from_str(&saved).unwrap();
    resumed.enable_tool_search(&restored).await.unwrap();
    assert_eq!(resumed.loaded_tool_names().await.unwrap(), restored);
    let second = complete(
        &mut resumed,
        &model,
        "Now read my invoice record and give me its confirmation code and total paid.",
        "amber-heron-4936",
    )
    .await;
    assert!(second.contains(&"tool_search".into()));
    assert!(second.contains(&"read_invoice_record".into()));
    assert!(
        resumed
            .loaded_tool_names()
            .await
            .unwrap()
            .contains(&"read_appointment_record".into())
    );
}
