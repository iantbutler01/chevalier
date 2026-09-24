#![cfg(feature = "claude-subscription")]
use chevalier::{
    claude_subscription::{
        ClaudeSessionConfig, ClaudeSessionError, ClaudeSessionEvent, ToolContent, ToolOutput,
        UserTurn,
    },
    error::Error,
    runtime::{Runtime, ToolFunction},
};
use std::{path::PathBuf, time::Duration};

fn config(mode: &str) -> ClaudeSessionConfig {
    ClaudeSessionConfig {
        cli_path: Some(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/claude-subscription/fake-claude.cjs"),
        ),
        cwd: std::env::temp_dir()
            .join("chevalier-claude-test")
            .join(mode),
        client_app: "chevalier-test".into(),
        server_name: "ob".into(),
        idle_timeout: Duration::from_millis(800),
        ..Default::default()
    }
}
fn schema() -> serde_json::Value {
    serde_json::json!({"type":"object", "properties":{}, "additionalProperties":true})
}
async fn runtime() -> Runtime {
    let runtime = Runtime::new();
    runtime
        .register_tool_with_schema(
            "read_file",
            "Read file",
            schema(),
            ToolFunction::Sync(Box::new(|_| Ok("alpha-7".into()))),
        )
        .await
        .unwrap();
    runtime
        .register_tool_schema("screenshot", "Screenshot", schema())
        .await
        .unwrap();
    runtime
}
#[tokio::test]
async fn happy() {
    let setup = config("happy");
    let session = runtime().await.claude_session(setup.clone()).await.unwrap();
    let mut init = false;
    let mut called = false;
    let mut executed = false;
    let mut rate = false;
    session
        .send(UserTurn {
            text: "go".into(),
            images: Vec::new(),
        })
        .await
        .unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), session.next_event())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        eprintln!("event: {event:?}");
        match event {
            ClaudeSessionEvent::Init { .. } => init = true,
            ClaudeSessionEvent::ToolCall { call_id, call } => {
                assert_eq!(call.tool_name, "screenshot");
                called = true;
                session.respond_tool(&call_id, ToolOutput { content: vec![ToolContent::Image { data_base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIAX8jx0gAAAABJRU5ErkJggg==".into(), mime_type: "image/png".into() }], is_error: false }).await.unwrap();
            }
            ClaudeSessionEvent::ToolExecuted { call, .. } => {
                assert_eq!(call.tool_name, "read_file");
                executed = true;
            }
            ClaudeSessionEvent::RateLimits(limits) => {
                assert!(!limits.is_empty());
                rate = true;
            }
            ClaudeSessionEvent::TurnComplete { usage, .. } => {
                assert!(usage.output_tokens > 0);
                break;
            }
            _ => {}
        }
    }
    assert!(init && called && executed && rate);
    session.close().await.unwrap();
    let record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(setup.cwd.join("fake-record.json")).unwrap())
            .unwrap();
    assert_eq!(
        record["argv"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|arg| *arg == "--strict-mcp-config")
            .count(),
        1
    );
    assert!(
        record["messages"].as_array().unwrap().iter().any(
            |line| line["response"]["response"]["mcp_response"]["result"]["content"][0]["mimeType"]
                == "image/png"
        )
    );
}
#[tokio::test]
async fn slow_host_tool_outlives_the_idle_watchdog() {
    // Same fake script as `happy`, own cwd so the two tests' records never collide.
    let mut setup = config("happy");
    setup.cwd = std::env::temp_dir()
        .join("chevalier-claude-test-slow")
        .join("happy");
    let session = runtime().await.claude_session(setup).await.unwrap();
    session
        .send(UserTurn {
            text: "go".into(),
            images: Vec::new(),
        })
        .await
        .unwrap();
    loop {
        match tokio::time::timeout(Duration::from_secs(10), session.next_event())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            ClaudeSessionEvent::ToolCall { call_id, .. } => {
                // Longer than the 800 ms idle timeout: the host is working (e.g. awaiting approval).
                tokio::time::sleep(Duration::from_millis(2_000)).await;
                session
                    .respond_tool(
                        &call_id,
                        ToolOutput {
                            content: vec![ToolContent::Text("done".into())],
                            is_error: false,
                        },
                    )
                    .await
                    .unwrap();
            }
            ClaudeSessionEvent::TurnComplete { is_error, .. } => {
                assert!(!is_error);
                break;
            }
            _ => {}
        }
    }
    session.close().await.unwrap();
}
#[tokio::test]
async fn host_dispatch_all_surfaces_handler_backed_tools() {
    let mut setup = config("happy");
    setup.cwd = std::env::temp_dir()
        .join("chevalier-claude-test-hostall")
        .join("happy");
    setup.host_dispatch_all = true;
    let session = runtime().await.claude_session(setup).await.unwrap();
    session
        .send(UserTurn {
            text: "go".into(),
            images: Vec::new(),
        })
        .await
        .unwrap();
    let mut surfaced = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(10), session.next_event())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            ClaudeSessionEvent::ToolCall { call_id, call } => {
                surfaced.push(call.tool_name.clone());
                session
                    .respond_tool(
                        &call_id,
                        ToolOutput {
                            content: vec![ToolContent::Text("host ran it".into())],
                            is_error: false,
                        },
                    )
                    .await
                    .unwrap();
            }
            ClaudeSessionEvent::ToolExecuted { call, .. } => {
                panic!(
                    "{} ran inside Chevalier despite host_dispatch_all",
                    call.tool_name
                )
            }
            ClaudeSessionEvent::TurnComplete { .. } => break,
            _ => {}
        }
    }
    assert!(surfaced.contains(&"read_file".to_string()), "{surfaced:?}");
    session.close().await.unwrap();
}
#[tokio::test]
async fn missing_resume_target_is_reported_as_such() {
    let mut setup = config("resume-missing");
    setup.resume = Some(chevalier::claude_subscription::ResumeTarget {
        session_id: "00000000-0000-4000-8000-000000000000".into(),
        cwd: setup.cwd.clone(),
    });
    let session = runtime().await.claude_session(setup).await.unwrap();
    session
        .send(UserTurn {
            text: "continue".into(),
            images: Vec::new(),
        })
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), session.next_event())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::ClaudeSession(ClaudeSessionError::ResumeNotFound { .. })
        ),
        "{error}"
    );
}
#[tokio::test]
async fn console_login_is_not_a_subscription() {
    // `auth status` runs outside the session cwd, so the fake learns its mode from a wrapper.
    let mut setup = config("console-login");
    let wrapper = std::env::temp_dir().join(format!("fake-claude-console-{}", std::process::id()));
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nCHEVALIER_FAKE_CLAUDE_MODE=console-login exec {:?} \"$@\"\n",
            setup.cli_path.as_ref().unwrap()
        ),
    )
    .unwrap();
    std::fs::set_permissions(
        &wrapper,
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .unwrap();
    setup.cli_path = Some(wrapper.clone());
    let result = runtime().await.claude_session(setup).await;
    std::fs::remove_file(wrapper).unwrap();
    let error = match result {
        Ok(_) => panic!("a Console login must not start a subscription session"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("not a Claude subscription"),
        "{error}"
    );
}
#[tokio::test]
async fn rejects_api_key() {
    let setup = config("api-key");
    let session = runtime().await.claude_session(setup.clone()).await.unwrap();
    let error = session.next_event().await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        Error::ClaudeSession(ClaudeSessionError::NotSubscription { .. })
    ));
    let record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(setup.cwd.join("fake-record.json")).unwrap())
            .unwrap();
    assert!(
        record["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|line| line["type"] != "user")
    );
}
#[tokio::test]
async fn garbage_is_protocol_error() {
    let session = runtime()
        .await
        .claude_session(config("garbage"))
        .await
        .unwrap();
    let error = session.next_event().await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        Error::ClaudeSession(ClaudeSessionError::Protocol(_))
    ));
}
#[tokio::test]
async fn idle_is_error() {
    let session = runtime()
        .await
        .claude_session(config("idle"))
        .await
        .unwrap();
    let error = session.next_event().await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        Error::ClaudeSession(ClaudeSessionError::Idle { .. })
    ));
}

#[tokio::test]
async fn crash_before_init_is_exited() {
    let session = runtime()
        .await
        .claude_session(config("crash-before-init"))
        .await
        .unwrap();
    assert!(matches!(
        session.next_event().await.unwrap(),
        Err(Error::ClaudeSession(ClaudeSessionError::Exited { .. }))
    ));
}

#[tokio::test]
async fn steering_reaches_pending_turn() {
    let session = runtime()
        .await
        .claude_session(config("steer"))
        .await
        .unwrap();
    session
        .send(UserTurn {
            text: "go".into(),
            images: vec![],
        })
        .await
        .unwrap();
    loop {
        match session.next_event().await.unwrap().unwrap() {
            ClaudeSessionEvent::ToolCall { call_id, .. } => {
                session
                    .send(UserTurn {
                        text: "steer".into(),
                        images: vec![],
                    })
                    .await
                    .unwrap();
                session
                    .respond_tool(
                        &call_id,
                        ToolOutput {
                            content: vec![ToolContent::Text("pink".into())],
                            is_error: false,
                        },
                    )
                    .await
                    .unwrap();
            }
            ClaudeSessionEvent::TurnComplete { .. } => break,
            _ => {}
        }
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn interrupt_cancels_host_call() {
    let session = runtime()
        .await
        .claude_session(config("interrupt"))
        .await
        .unwrap();
    session
        .send(UserTurn {
            text: "go".into(),
            images: vec![],
        })
        .await
        .unwrap();
    let mut interrupted = false;
    let mut cancelled = false;
    let mut cancelled_id = None;
    loop {
        match session.next_event().await.unwrap().unwrap() {
            ClaudeSessionEvent::ToolCall { call_id, .. } if !interrupted => {
                cancelled_id = Some(call_id);
                session.interrupt().await.unwrap();
                interrupted = true;
            }
            ClaudeSessionEvent::ToolCancelled { .. } => cancelled = true,
            ClaudeSessionEvent::TurnComplete {
                is_error, subtype, ..
            } => {
                assert!(is_error);
                assert_eq!(subtype, "error_during_execution");
                break;
            }
            _ => {}
        }
    }
    assert!(cancelled);
    assert!(
        session
            .respond_tool(
                &cancelled_id.unwrap(),
                ToolOutput {
                    content: vec![ToolContent::Text("late".into())],
                    is_error: false
                }
            )
            .await
            .is_err()
    );
    session.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a logged-in claude CLI on a Claude subscription"]
async fn live_subscription() {
    if std::env::var("CHEVALIER_LIVE_CLAUDE_SUBSCRIPTION").as_deref() != Ok("1") {
        return;
    }
    let runtime = runtime().await;
    let mut config = config("happy");
    config.cli_path = None;
    config.client_app = "chevalier-test".into();
    config.model = "sonnet".into();
    config.idle_timeout = Duration::from_secs(240);
    let session = runtime.claude_session(config.clone()).await.unwrap();
    session
        .send(UserTurn {
            text: "Call read_file and screenshot, describe the image, and remember alpha-7.".into(),
            images: Vec::new(),
        })
        .await
        .unwrap();
    let mut called = false;
    let mut rate = false;
    let mut tool_result = false;
    let mut session_id = None;
    let mut image_described = false;
    loop {
        let event = session.next_event().await.unwrap().unwrap();
        eprintln!("{event:?}");
        match event {
            ClaudeSessionEvent::Init { session_id: id, .. } => {
                session_id = Some(id);
                eprintln!("apiKeySource=none");
            }
            ClaudeSessionEvent::ToolCall { call_id, .. } => {
                called = true;
                session.respond_tool(&call_id, ToolOutput { content: vec![ToolContent::Image { data_base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIAX8jx0gAAAABJRU5ErkJggg==".into(), mime_type: "image/png".into() }], is_error: false }).await.unwrap();
            }
            ClaudeSessionEvent::ToolExecuted { .. } => tool_result = true,
            ClaudeSessionEvent::AssistantMessage { text } => {
                image_described |= text.to_lowercase().contains("pink");
            }
            ClaudeSessionEvent::RateLimits(_) => rate = true,
            ClaudeSessionEvent::TurnComplete { .. } => break,
            _ => {}
        }
    }
    assert!(called && tool_result && rate && image_described);
    session.close().await.unwrap();
    config.resume = Some(chevalier::claude_subscription::ResumeTarget {
        session_id: session_id.unwrap(),
        cwd: config.cwd.clone(),
    });
    let session = runtime.claude_session(config).await.unwrap();
    session
        .send(UserTurn {
            text: "What exact text did read_file return in the previous turn?".into(),
            images: vec![],
        })
        .await
        .unwrap();
    let mut recalled = false;
    loop {
        match session.next_event().await.unwrap().unwrap() {
            ClaudeSessionEvent::AssistantMessage { text } => recalled |= text.contains("alpha-7"),
            ClaudeSessionEvent::TurnComplete { result, .. } => {
                recalled |= result.unwrap_or_default().contains("alpha-7");
                break;
            }
            _ => {}
        }
    }
    eprintln!("resume recall: {recalled}");
    assert!(recalled);
    session.close().await.unwrap();
}
