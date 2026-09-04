#![cfg(unix)]

use async_trait::async_trait;
use chevalier::error::{Error, Result};
use chevalier::programmatic::{
    CancellationToken, ProgrammaticDispatcher, ProgrammaticEvent, ProgrammaticOptions,
    ProgrammaticSandbox, ProgrammaticSession, ProgrammaticTool, execute_programmatic,
};
use serde_json::{Value, json};
use std::process::Stdio;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::{Barrier, Mutex, Notify, mpsc};

struct ProcessSandbox {
    exited: Arc<AtomicBool>,
    identity: String,
    cwd: std::path::PathBuf,
}

impl Drop for ProcessSandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.cwd);
    }
}

struct ProcessSession {
    stdin: Mutex<ChildStdin>,
    events: Mutex<mpsc::UnboundedReceiver<ProgrammaticEvent>>,
    pid: u32,
    exited: Arc<AtomicBool>,
}

#[async_trait]
impl ProgrammaticSession for ProcessSession {
    async fn write(&self, data: &str) -> Result<()> {
        self.stdin.lock().await.write_all(data.as_bytes()).await?;
        Ok(())
    }

    async fn next(&self) -> Result<Option<ProgrammaticEvent>> {
        Ok(self.events.lock().await.recv().await)
    }

    async fn signal(&self, signal: i32) -> Result<()> {
        if !self.exited.load(Ordering::SeqCst) {
            let status = Command::new("/bin/kill")
                .args([format!("-{signal}"), self.pid.to_string()])
                .status()
                .await?;
            if !status.success() && !self.exited.load(Ordering::SeqCst) {
                return Err(Error::NonRetryable("Test process signal failed".into()));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ProgrammaticSandbox for ProcessSandbox {
    async fn start_exec(
        &self,
        command: &str,
        _timeout: Duration,
        _cancel: CancellationToken,
    ) -> Result<Arc<dyn ProgrammaticSession>> {
        let mut child = Command::new("/bin/sh")
            .args(["-c", &format!("exec {command}")])
            .env("CHEVALIER_TEST_SANDBOX", &self.identity)
            .current_dir(&self.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let pid = child.id().unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let (sender, events) = mpsc::unbounded_channel();
        let stdout_sender = sender.clone();
        let output_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let characters: Vec<_> = format!("{line}\n").chars().collect();
                for chunk in characters.chunks(17) {
                    let _ = stdout_sender.send(ProgrammaticEvent::Stdout {
                        text: chunk.iter().collect(),
                    });
                }
            }
        });
        let stderr_sender = sender.clone();
        let error_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(text)) = lines.next_line().await {
                let _ = stderr_sender.send(ProgrammaticEvent::Stderr { text });
            }
        });
        let exited = self.exited.clone();
        tokio::spawn(async move {
            let status = match tokio::time::timeout(Duration::from_secs(12), child.wait()).await {
                Ok(status) => status.unwrap(),
                Err(_) => {
                    child.start_kill().unwrap();
                    child.wait().await.unwrap()
                }
            };
            let _ = output_task.await;
            let _ = error_task.await;
            exited.store(true, Ordering::SeqCst);
            let _ = sender.send(ProgrammaticEvent::Exit {
                code: status.code().unwrap_or(-1),
            });
        });
        Ok(Arc::new(ProcessSession {
            stdin: Mutex::new(stdin),
            events: Mutex::new(events),
            pid,
            exited: self.exited.clone(),
        }))
    }
}

struct Dispatcher {
    overlap: Barrier,
    entered: Notify,
    cancelled: AtomicBool,
    calls: Mutex<Vec<String>>,
}

#[async_trait]
impl ProgrammaticDispatcher for Dispatcher {
    async fn call(&self, name: &str, args: Value, cancel: CancellationToken) -> Result<Value> {
        self.calls.lock().await.push(name.to_owned());
        match name {
            "lookup" => {
                tokio::select! {
                    _ = self.overlap.wait() => Ok(json!({"value":args["value"],"private":"not emitted"})),
                    _ = cancel.cancelled() => Err(Error::NonRetryable("Cancelled lookup".into())),
                }
            }
            "deny" => Err(Error::NonRetryable("Host approval denied".into())),
            "wait" => {
                self.entered.notify_one();
                cancel.cancelled().await;
                self.cancelled.store(true, Ordering::SeqCst);
                Err(Error::NonRetryable("Cancelled wait".into()))
            }
            _ => panic!("Undeclared tool reached host: {name}"),
        }
    }
}

fn fixture() -> (Arc<ProcessSandbox>, Arc<Dispatcher>, Vec<ProgrammaticTool>) {
    let identity = uuid::Uuid::new_v4().to_string();
    let cwd = std::env::temp_dir().join(format!("chevalier-ptc-{identity}"));
    std::fs::create_dir(&cwd).unwrap();
    let sandbox = Arc::new(ProcessSandbox {
        exited: Arc::new(AtomicBool::new(false)),
        identity,
        cwd: cwd.canonicalize().unwrap(),
    });
    let dispatcher = Arc::new(Dispatcher {
        overlap: Barrier::new(2),
        entered: Notify::new(),
        cancelled: AtomicBool::new(false),
        calls: Mutex::new(Vec::new()),
    });
    let tools = ["lookup", "deny", "wait", "execute_code"]
        .into_iter()
        .map(|name| ProgrammaticTool {
            name: name.into(),
            description: name.into(),
            schema: json!({"type":"object"}),
        })
        .collect();
    (sandbox, dispatcher, tools)
}

#[tokio::test]
async fn rust_consumer_uses_selected_process_and_concurrent_host_tools_with_selected_output() {
    let (sandbox, dispatcher, tools) = fixture();
    let result = execute_programmatic(
        "console.log('ordinary process output'); const results = await Promise.all([tools.lookup({value:3}), tools.lookup({value:7})]); text({sum:results.reduce((sum,result)=>sum+result.value,0), sandbox:process.env.CHEVALIER_TEST_SANDBOX, cwd:process.cwd(), unicode:'🦀'});",
        &tools, sandbox.clone(), dispatcher.clone(),
        ProgrammaticOptions { timeout: Duration::from_secs(5), ..Default::default() },
    ).await.unwrap();
    assert_eq!(
        result.output,
        vec![json!({"sum":10,"sandbox":sandbox.identity,"cwd":sandbox.cwd,"unicode":"🦀"})]
    );
    assert_eq!(*dispatcher.calls.lock().await, vec!["lookup", "lookup"]);
    assert!(sandbox.exited.load(Ordering::SeqCst));
}

#[tokio::test]
async fn unavailable_recursive_and_denied_tools_do_not_bypass_host_policy() {
    let (sandbox, dispatcher, tools) = fixture();
    let result = execute_programmatic(
        "text(typeof tools.missing); text(typeof tools.execute_code); try { await tools.deny({}); } catch(error) { text(error.message); } text('continued');",
        &tools, sandbox.clone(), dispatcher.clone(), ProgrammaticOptions::default(),
    ).await.unwrap();
    assert_eq!(result.output[0], "undefined");
    assert_eq!(result.output[1], "undefined");
    assert!(
        result.output[2]
            .as_str()
            .unwrap()
            .contains("Host approval denied")
    );
    assert_eq!(result.output[3], "continued");
    assert_eq!(*dispatcher.calls.lock().await, vec!["deny"]);
    assert!(sandbox.exited.load(Ordering::SeqCst));
}

#[tokio::test]
async fn caller_cancellation_settles_dispatch_and_reaps_process_before_return() {
    let (sandbox, dispatcher, tools) = fixture();
    let cancel = CancellationToken::new();
    let execution = execute_programmatic(
        "await tools.wait({}); text('must not complete');",
        &tools,
        sandbox.clone(),
        dispatcher.clone(),
        ProgrammaticOptions {
            cancel: cancel.clone(),
            ..Default::default()
        },
    );
    let cancellation = async {
        dispatcher.entered.notified().await;
        cancel.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(execution, cancellation)
    })
    .await
    .unwrap();
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert!(dispatcher.cancelled.load(Ordering::SeqCst));
    assert!(sandbox.exited.load(Ordering::SeqCst));
}

#[tokio::test]
async fn deadline_terminates_a_silent_process() {
    let (sandbox, dispatcher, tools) = fixture();
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        execute_programmatic(
            "await new Promise(()=>{});",
            &tools,
            sandbox.clone(),
            dispatcher,
            ProgrammaticOptions {
                timeout: Duration::from_millis(300),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    assert!(result.unwrap_err().to_string().contains("timed out"));
    assert!(sandbox.exited.load(Ordering::SeqCst));
}

#[tokio::test]
async fn dropping_rust_future_cancels_inflight_dispatch_and_reaps_process() {
    let (sandbox, dispatcher, tools) = fixture();
    let mut execution = Box::pin(execute_programmatic(
        "await tools.wait({});",
        &tools,
        sandbox.clone(),
        dispatcher.clone(),
        ProgrammaticOptions::default(),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut execution => panic!("Execution ended before cancellation: {result:?}"),
            _ = dispatcher.entered.notified() => {},
        }
    })
    .await
    .unwrap();
    drop(execution);
    tokio::time::timeout(Duration::from_secs(8), async {
        while !sandbox.exited.load(Ordering::SeqCst) || !dispatcher.cancelled.load(Ordering::SeqCst)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
