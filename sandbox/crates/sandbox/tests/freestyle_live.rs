//! Live Freestyle smoke: gated so CI never hits the provider. Run with
//! `FREESTYLE_LIVE=1 FREESTYLE_API_KEY=... cargo test -p chevalier-sandbox --test freestyle_live`.
use std::time::Duration;

use chevalier_sandbox::{
    ExecEvent, ExecOptions, FreestyleBackendConfig, Sandbox, SandboxConfig, SandboxProviderConfig,
    SessionOptions, ShellEvent, ShellInput, ShellOptions,
};
use futures::StreamExt;
use tokio::time::timeout;

fn live_config() -> Option<SandboxConfig> {
    if std::env::var("FREESTYLE_LIVE").ok().as_deref() != Some("1") {
        return None;
    }
    let mut freestyle = FreestyleBackendConfig::from_env().ok()?;
    // Never leave a probe VM behind if the test dies. Not 0: ephemeral VMs cannot be
    // paused, and the test exercises pause/resume.
    freestyle.auto_delete_secs = Some(3600);
    Some(SandboxConfig {
        provider: SandboxProviderConfig::Freestyle(freestyle),
        prewarm_on_start: false,
        ..SandboxConfig::default()
    })
}

async fn exec_stdout(session: &chevalier_sandbox::Session, command: &str) -> String {
    let mut handle = session
        .exec(
            command,
            ExecOptions {
                timeout_secs: Some(60),
                close_stdin_on_start: true,
                ..ExecOptions::default()
            },
        )
        .await
        .expect("exec");
    let mut stdout = Vec::new();
    while let Some(event) = handle.events.next().await {
        match event.expect("exec event") {
            ExecEvent::Stdout(bytes) => stdout.extend(bytes),
            ExecEvent::Stderr(_) => {}
            ExecEvent::Exit(code) => {
                assert_eq!(code, 0, "command failed: {command}");
                break;
            }
            ExecEvent::Timeout => panic!("exec timed out: {command}"),
        }
    }
    String::from_utf8(stdout).expect("utf8")
}

#[tokio::test(flavor = "multi_thread")]
async fn freestyle_live_facade_smoke() {
    let Some(config) = live_config() else {
        eprintln!("skipping Freestyle live test; set FREESTYLE_LIVE=1 and FREESTYLE_API_KEY");
        return;
    };
    let sandbox = Sandbox::new(config).await.expect("connect");
    let session_id = format!("live-{}", uuid::Uuid::new_v4());
    let session = timeout(
        Duration::from_secs(120),
        sandbox.session(SessionOptions {
            session_id: Some(session_id.clone()),
            name: Some("chevalier-live-smoke".to_string()),
            ..SessionOptions::default()
        }),
    )
    .await
    .expect("session create timeout")
    .expect("session create");

    // exec, files, state
    assert_eq!(
        exec_stdout(&session, "echo exec-ok").await.trim(),
        "exec-ok"
    );
    session
        .write_file("/tmp/chevalier-live.txt", b"hello".to_vec())
        .await
        .expect("write file");
    assert_eq!(
        session
            .read_file("/tmp/chevalier-live.txt")
            .await
            .expect("read file"),
        b"hello"
    );
    assert!(
        session
            .list_dir("/tmp")
            .await
            .expect("list dir")
            .iter()
            .any(|entry| entry.name == "chevalier-live.txt")
    );
    assert_eq!(
        session.state().await.expect("state"),
        chevalier_sandbox::proto::vmd::v1::VmState::Running as i32
    );

    // attach by logical id (alias) and by metadata after the alias is gone
    let reattached = sandbox.attach_session(&session_id).await.expect("attach");
    assert_eq!(reattached.vm_id(), session.vm_id());
    let fresh = Sandbox::new(live_config().unwrap())
        .await
        .expect("fresh sandbox");
    let found = fresh
        .attach_session(&session_id)
        .await
        .expect("attach via metadata");
    assert_eq!(found.vm_id(), session.vm_id());

    // preview URL creates a TLS rule
    let preview = session
        .provider_preview_url(8080)
        .await
        .expect("preview url");
    assert!(preview.starts_with("https://"), "{preview}");

    // interactive shell over the PTY websocket
    let mut shell = session
        .shell(ShellOptions {
            cols: Some(100),
            rows: Some(30),
            ..ShellOptions::default()
        })
        .await
        .expect("shell");
    shell
        .input
        .send(ShellInput::Data(b"printf shell-ok; exit\n".to_vec()))
        .await
        .expect("send");
    let mut output = Vec::new();
    while let Some(event) = timeout(Duration::from_secs(30), shell.events.next())
        .await
        .expect("shell timeout")
    {
        match event.expect("shell event") {
            ShellEvent::Output(bytes) => output.extend(bytes),
            ShellEvent::Exit(_) => break,
        }
    }
    assert!(String::from_utf8_lossy(&output).contains("shell-ok"));

    // pause/resume + snapshot/fork keep running state
    session.pause().await.expect("pause");
    session.start().await.expect("resume");
    let snapshot = session.snapshot("live", "smoke").await.expect("snapshot");
    let restored = session
        .restore_checkpoint(&snapshot.id)
        .await
        .expect("restore checkpoint");
    assert_ne!(restored.vm_id(), session.vm_id());
    assert_eq!(
        exec_stdout(&restored, "cat /tmp/chevalier-live.txt").await,
        "hello"
    );

    restored.discard().await.expect("discard restored");
    session
        .delete_snapshot(&snapshot.id)
        .await
        .expect("delete snapshot");
    sandbox
        .discard_session_by_id(&session_id)
        .await
        .expect("discard session");
}
