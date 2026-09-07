use std::{collections::HashMap, time::Duration};

use futures::stream;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request,
    transport::{Channel, Server},
};

use crate::pb::bracket::portproxy::v1::{
    self as wire, daemon_manager_client::DaemonManagerClient,
    daemon_manager_server::DaemonManagerServer, shell_exec_client::ShellExecClient,
    shell_exec_server::ShellExecServer,
};
use crate::services::{DaemonManagerService, ShellExecService};
use crate::{
    child_tracker::{ChildExit, ChildTracker},
    daemon::DaemonRegistry,
};

async fn services() -> (
    ShellExecClient<Channel>,
    DaemonManagerClient<Channel>,
    tokio::task::JoinHandle<()>,
    String,
) {
    let tracker = ChildTracker::new();
    let tracked = tracker.clone();
    tokio::spawn(async move {
        loop {
            for pid in tracked.snapshot() {
                use nix::{
                    sys::wait::{WaitPidFlag, WaitStatus, waitpid},
                    unistd::Pid,
                };
                match waitpid(Pid::from_raw(pid), Some(WaitPidFlag::WNOHANG)) {
                    Ok(WaitStatus::Exited(_, code)) => {
                        tracked.record_exit(pid, ChildExit::Exited(code));
                    }
                    Ok(WaitStatus::Signaled(_, signal, _)) => {
                        tracked.record_exit(pid, ChildExit::Signaled(signal as i32));
                    }
                    _ => {}
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ShellExecServer::new(ShellExecService::new(tracker.clone())))
            .add_service(DaemonManagerServer::new(DaemonManagerService::new(
                DaemonRegistry::new(tracker),
            )))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    (
        ShellExecClient::connect(endpoint.clone()).await.unwrap(),
        DaemonManagerClient::connect(endpoint.clone())
            .await
            .unwrap(),
        server,
        endpoint,
    )
}

fn signal(
    identity: &str,
    epoch: u64,
    sequence: u64,
    number: i32,
) -> Request<wire::ExecControlRequest> {
    let mut request = Request::new(wire::ExecControlRequest {
        execution_id: identity.into(),
        producer_epoch: epoch,
        control_seq: sequence,
        control: Some(wire::exec_control_request::Control::Signal(number)),
    });
    request.set_timeout(Duration::from_secs(2));
    request
}

async fn start(
    client: &mut ShellExecClient<Channel>,
    identity: &str,
    command: &str,
) -> (
    mpsc::Sender<wire::ExecRequest>,
    tonic::Streaming<wire::ExecResponse>,
) {
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(wire::ExecRequest {
            request: Some(wire::exec_request::Request::Start(wire::ExecStart {
                execution_id: identity.into(),
                args: vec!["/bin/sh".into(), "-c".into(), command.into()],
                env: HashMap::new(),
                detach: false,
                timeout: Some(30),
            })),
        })
        .await
        .unwrap();
    let response = client
        .exec(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    (sender, response)
}

async fn output_until(response: &mut tonic::Streaming<wire::ExecResponse>, marker: &str) -> String {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut output = String::new();
        loop {
            let frame = response
                .message()
                .await
                .unwrap()
                .expect("stream stays active");
            if let Some(wire::exec_response::Response::StdoutData(bytes)) = frame.response {
                output.push_str(&String::from_utf8_lossy(&bytes));
                if output.contains(marker) {
                    return output;
                }
            }
        }
    })
    .await
    .expect("expected process output")
}

async fn stopped(pid: i32) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let output = tokio::process::Command::new("ps")
                .args(["-o", "stat=", "-p", &pid.to_string()])
                .output()
                .await
                .unwrap();
            let status = String::from_utf8_lossy(&output.stdout);
            if status.trim().is_empty() || status.trim_start().starts_with('Z') {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("process must stop, not merely acknowledge the request");
}

#[tokio::test]
async fn eof_preserves_control_and_stop_reaches_descendants_before_terminal_result() {
    let (mut client, _, server, _) = services().await;
    let (sender, mut response) = start(
        &mut client,
        "eof-tree",
        "cat; sh -c 'trap \"\" TERM; while :; do sleep 1; done' & echo CHILD:$!; echo EOF; wait",
    )
    .await;
    sender
        .send(wire::ExecRequest {
            request: Some(wire::exec_request::Request::StdinData(
                b"ordered input\n".to_vec(),
            )),
        })
        .await
        .unwrap();
    sender
        .send(wire::ExecRequest {
            request: Some(wire::exec_request::Request::StdinEof(true)),
        })
        .await
        .unwrap();
    let output = output_until(&mut response, "EOF").await;
    assert!(output.contains("ordered input"));
    let pid = output
        .lines()
        .find_map(|line| line.strip_prefix("CHILD:"))
        .unwrap()
        .parse()
        .unwrap();
    client
        .control_exec(signal("eof-tree", 0, 1, 15))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(wire::exec_response::Response::ExitCode(_)) =
                response.message().await.unwrap().unwrap().response
            {
                break;
            }
        }
    })
    .await
    .expect("confirmed terminal result");
    stopped(pid).await;
    assert!(
        client
            .control_exec(signal("eof-tree", 0, 2, 9))
            .await
            .is_err()
    );
    server.abort();
}

#[tokio::test]
async fn stop_bypasses_blocked_stdin_and_unread_stdout() {
    let (mut client, _, server, endpoint) = services().await;
    let mut control = ShellExecClient::connect(endpoint).await.unwrap();
    let (sender, mut response) = start(&mut client, "backpressure", "echo PID:$$; trap '' TERM; while :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; done").await;
    let output = output_until(&mut response, "\n").await;
    let pid = output
        .lines()
        .next()
        .unwrap()
        .strip_prefix("PID:")
        .unwrap()
        .parse()
        .unwrap();
    let writer = tokio::spawn(async move {
        for _ in 0..1024 {
            if sender
                .send(wire::ExecRequest {
                    request: Some(wire::exec_request::Request::StdinData(vec![42; 65536])),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !writer.is_finished(),
        "stdin should actually be backpressured"
    );
    control
        .control_exec(signal("backpressure", 0, 1, 9))
        .await
        .unwrap();
    stopped(pid).await;
    writer.abort();
    drop(response);
    server.abort();
}

#[tokio::test]
async fn exec_stream_stop_bypasses_blocked_stdin_and_unread_stdout() {
    let (_, mut client, server, endpoint) = services().await;
    let mut control = DaemonManagerClient::connect(endpoint).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("pid");
    client
        .exec_daemon(wire::ExecDaemonRequest {
            name: "stream-backpressure".into(),
            args: vec![
                "sh".into(),
                "-c".into(),
                format!("echo $$ > {}; trap '' TERM; while :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; done", marker.display()),
            ],
            env: HashMap::new(),
            timeout: Some(30),
            detach: false,
        })
        .await
        .unwrap();
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(wire::AttachDaemonRequest {
            request: Some(wire::attach_daemon_request::Request::Start(
                wire::AttachDaemonStart {
                    name: "stream-backpressure".into(),
                },
            )),
        })
        .await
        .unwrap();
    let response = client
        .attach_daemon(ReceiverStream::new(receiver))
        .await
        .unwrap();
    let writer = tokio::spawn(async move {
        for _ in 0..1024 {
            if sender
                .send(wire::AttachDaemonRequest {
                    request: Some(wire::attach_daemon_request::Request::StdinData(vec![
                        42;
                        65536
                    ])),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !writer.is_finished(),
        "stdin should actually be backpressured"
    );
    let pid = tokio::fs::read_to_string(marker)
        .await
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    control
        .control_daemon(signal("stream-backpressure", 0, 1, 9))
        .await
        .unwrap();
    stopped(pid).await;
    writer.abort();
    drop(response);
    server.abort();
}

#[tokio::test]
async fn daemon_control_survives_reattach_and_deduplicates_generations() {
    let (_, mut client, server, _) = services().await;
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("signals");
    let command = format!(
        "trap 'echo received >> {}' USR1; echo READY; while :; do sleep 0.1; done",
        file.display()
    );
    client
        .exec_daemon(wire::ExecDaemonRequest {
            name: "stream-owner".into(),
            args: vec!["sh".into(), "-c".into(), command],
            env: HashMap::new(),
            timeout: Some(30),
            detach: true,
        })
        .await
        .unwrap();
    let mut attached = client
        .attach_daemon(stream::iter([wire::AttachDaemonRequest {
            request: Some(wire::attach_daemon_request::Request::Start(
                wire::AttachDaemonStart {
                    name: "stream-owner".into(),
                },
            )),
        }]))
        .await
        .unwrap()
        .into_inner();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(wire::attach_daemon_response::Response::StdoutData(data)) =
                attached.message().await.unwrap().unwrap().response
            {
                if String::from_utf8_lossy(&data).contains("READY") {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    drop(attached);
    client
        .control_daemon(signal("stream-owner", 1, 1, nix::libc::SIGUSR1))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !file.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("signal handler must run");
    client
        .control_daemon(signal("stream-owner", 1, 1, nix::libc::SIGUSR1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        tokio::fs::read_to_string(&file)
            .await
            .unwrap()
            .lines()
            .count(),
        1
    );
    client
        .control_daemon(wire::ExecControlRequest {
            execution_id: "stream-owner".into(),
            producer_epoch: 2,
            control_seq: 0,
            control: Some(wire::exec_control_request::Control::Claim(true)),
        })
        .await
        .unwrap();
    assert_eq!(
        client
            .control_daemon(signal("stream-owner", 1, 2, 9))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    client
        .control_daemon(signal("stream-owner", 2, 1, nix::libc::SIGUSR1))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while tokio::fs::read_to_string(&file)
            .await
            .unwrap()
            .lines()
            .count()
            < 2
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("new owner signal must run");
    client
        .control_daemon(signal("stream-owner", 2, 2, 9))
        .await
        .unwrap();
    let mut attached = client
        .attach_daemon(stream::iter([wire::AttachDaemonRequest {
            request: Some(wire::attach_daemon_request::Request::Start(
                wire::AttachDaemonStart {
                    name: "stream-owner".into(),
                },
            )),
        }]))
        .await
        .unwrap()
        .into_inner();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = attached.message().await.unwrap() {
            if matches!(
                frame.response,
                Some(wire::attach_daemon_response::Response::ExitCode(_))
            ) {
                return;
            }
        }
        panic!("reattached stream must confirm terminal state");
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test]
async fn exec_stream_reattaches_with_a_backlog_larger_than_the_transport_queue() {
    let (_, mut client, server, _) = services().await;
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("ready");
    let command = format!(
        "head -c 1048576 /dev/zero; touch {}; sleep 30",
        marker.display()
    );
    client
        .exec_daemon(wire::ExecDaemonRequest {
            name: "large-backlog".into(),
            args: vec!["sh".into(), "-c".into(), command],
            env: HashMap::new(),
            timeout: Some(30),
            detach: false,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut request = Request::new(stream::iter([wire::AttachDaemonRequest {
        request: Some(wire::attach_daemon_request::Request::Start(
            wire::AttachDaemonStart {
                name: "large-backlog".into(),
            },
        )),
    }]));
    request.set_timeout(Duration::from_secs(2));
    let mut response = client
        .attach_daemon(request)
        .await
        .expect("backlog must not block attachment")
        .into_inner();
    client
        .control_daemon(signal("large-backlog", 0, 1, 9))
        .await
        .unwrap();
    let bytes = tokio::time::timeout(Duration::from_secs(3), async {
        let mut bytes = 0;
        while let Some(frame) = response.message().await.unwrap() {
            match frame.response {
                Some(wire::attach_daemon_response::Response::StdoutData(data)) => {
                    bytes += data.len()
                }
                Some(wire::attach_daemon_response::Response::ExitCode(_)) => return bytes,
                _ => {}
            }
        }
        panic!("missing terminal result");
    })
    .await
    .unwrap();
    assert_eq!(bytes, 1048576);
    server.abort();
}
