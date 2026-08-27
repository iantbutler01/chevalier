// @dive-file: Portproxy daemon entrypoint wiring gRPC services, TCP forwarding, and process supervision.
// @dive-rel: Composes child_tracker, daemon, services, and port_forward modules into one runtime.
// @dive-rel: Runs in guest VMs and exposes shell/daemon execution channels used by vmd control paths.

#![allow(clippy::collapsible_if)]
#![allow(clippy::default_constructed_unit_structs)]

mod child_tracker;
mod cli;
mod daemon;
mod exec_control;
#[cfg(test)]
mod exec_control_tests;
mod execution_identity;
mod port_forward;
mod process_group;
mod services;
mod system_env;

pub mod pb {
    pub mod bracket {
        pub mod portproxy {
            pub mod v1 {
                tonic::include_proto!("bracket.portproxy.v1");
            }
        }
    }

    pub mod google {
        pub mod protobuf {
            tonic::include_proto!("google.protobuf");
        }
    }
}

use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;
use std::time::Duration;

use anyhow::Context;
use child_tracker::{ChildExit, ChildTracker};
use clap::Parser;
use cli::Args;
use daemon::DaemonRegistry;
use nix::sys::signal::Signal as UnixSignal;
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::Pid;
use process_group::signal_process_group_or_pid;
use services::{DaemonManagerService, PortProxyService, ShellExecService};
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::sleep;
use tonic::metadata::MetadataMap;
use tonic::service::Interceptor;
use tonic::transport::Server;
use tonic::{Request, Status};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::pb::bracket::portproxy::v1::daemon_manager_server::DaemonManagerServer;
use crate::pb::bracket::portproxy::v1::port_proxy_server::PortProxyServer;
use crate::pb::bracket::portproxy::v1::shell_exec_server::ShellExecServer;

const PORTPROXY_AUTH_TOKEN_ENV: &str = "CHEVALIER_PORTPROXY_AUTH_TOKEN";
const PORTPROXY_AUTH_TOKEN_FILE_ENV: &str = "CHEVALIER_PORTPROXY_AUTH_TOKEN_FILE";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let args = Args::parse();
    if let Err(err) = args.validate() {
        eprintln!("{err}");
        process::exit(1);
    }

    if args.server {
        run_server_mode(args).await
    } else {
        run_client_mode(args).await
    }
}

/// Default tracing directives for the portproxy guest agent.
///
/// portproxy runs inside microVMs whose stdout can land on the emulated serial
/// console (ttyS0). h2/hyper/tonic/tower emit multiple MB/min of TRACE-level
/// frame chatter, and synchronous serial-console writes at that volume are a
/// known guest soft-lockup trigger. So default our own spans to INFO and force
/// the transport stack down to WARN. A caller-supplied `RUST_LOG` still
/// overrides this completely.
const DEFAULT_LOG_DIRECTIVES: &str = "info,h2=warn,hyper=warn,tonic=warn,tower=warn";

/// Builds the daemon's [`EnvFilter`], honoring `RUST_LOG` when present and
/// otherwise falling back to [`DEFAULT_LOG_DIRECTIVES`].
fn build_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_DIRECTIVES))
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(build_env_filter())
        .init();
}

async fn run_server_mode(args: Args) -> anyhow::Result<()> {
    let tracker = ChildTracker::new();
    let daemon_registry = DaemonRegistry::new(tracker.clone());

    tokio::spawn(reap_zombies(tracker.clone()));
    tokio::spawn({
        let tracker = tracker.clone();
        async move {
            if let Err(err) = forward_signals(tracker).await {
                warn!("forward signal task failed: {}", err);
            }
        }
    });

    let rpc_addr = SocketAddr::new(args.rpc_bind_address, args.rpc_port);

    let grpc = serve_grpc(rpc_addr, tracker.clone(), daemon_registry.clone());
    let port_proxy = port_forward::run_server(&args.server_addr);

    tokio::try_join!(grpc, port_proxy)?;
    Ok(())
}

async fn run_client_mode(args: Args) -> anyhow::Result<()> {
    let listen = args.listen_port.expect("validated");
    let forward = args.forward_port.expect("validated");
    port_forward::run_client(listen, forward, &args.server_addr).await
}

async fn serve_grpc(
    addr: SocketAddr,
    tracker: ChildTracker,
    daemon_registry: DaemonRegistry,
) -> anyhow::Result<()> {
    let shell_exec = ShellExecService::new(tracker.clone());
    let port_proxy = PortProxyService::new(tracker.clone());
    let daemon_manager = DaemonManagerService::new(daemon_registry);
    let auth_interceptor = PortproxyAuthInterceptor::from_env()?;

    let (health_reporter, health_service) = tonic_health::server::health_reporter();

    health_reporter
        .set_serving::<ShellExecServer<ShellExecService>>()
        .await;
    health_reporter
        .set_serving::<PortProxyServer<PortProxyService>>()
        .await;
    health_reporter
        .set_serving::<DaemonManagerServer<DaemonManagerService>>()
        .await;

    info!("Starting gRPC server on {}", addr);

    Server::builder()
        .add_service(health_service)
        .add_service(ShellExecServer::with_interceptor(
            shell_exec,
            auth_interceptor.clone(),
        ))
        .add_service(PortProxyServer::with_interceptor(
            port_proxy,
            auth_interceptor.clone(),
        ))
        .add_service(DaemonManagerServer::with_interceptor(
            daemon_manager,
            auth_interceptor,
        ))
        .serve(addr)
        .await
        .context("gRPC server failed")
}

#[derive(Clone)]
struct PortproxyAuthInterceptor {
    token: Option<String>,
}

impl PortproxyAuthInterceptor {
    fn from_env() -> anyhow::Result<Self> {
        let token = load_auth_token(
            env::var(PORTPROXY_AUTH_TOKEN_ENV).ok(),
            env::var_os(PORTPROXY_AUTH_TOKEN_FILE_ENV).map(PathBuf::from),
        )?;
        if token.is_some() {
            info!("portproxy RPC auth is enabled");
        } else {
            warn!(
                "portproxy RPC auth is disabled because neither {PORTPROXY_AUTH_TOKEN_ENV} nor {PORTPROXY_AUTH_TOKEN_FILE_ENV} is set"
            );
        }
        Ok(Self { token })
    }
}

fn load_auth_token(
    direct: Option<String>,
    token_file: Option<PathBuf>,
) -> anyhow::Result<Option<String>> {
    if let Some(token) = direct
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        return Ok(Some(token.to_string()));
    }
    let Some(path) = token_file else {
        return Ok(None);
    };
    let contents = fs::read_to_string(&path)
        .with_context(|| format!("read portproxy auth token file {}", path.display()))?;
    let token = contents.trim();
    if token.is_empty() {
        anyhow::bail!("portproxy auth token file {} is empty", path.display());
    }
    Ok(Some(token.to_string()))
}

impl Interceptor for PortproxyAuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        match self.token.as_deref() {
            Some(expected) => authorize_portproxy_request(request, expected),
            None => Ok(request),
        }
    }
}

fn authorize_portproxy_request(
    request: Request<()>,
    expected_token: &str,
) -> Result<Request<()>, Status> {
    let provided = bearer_token_from_metadata(request.metadata())
        .ok_or_else(|| Status::unauthenticated("missing portproxy authorization"))?;
    if token_matches(&provided, expected_token) {
        Ok(request)
    } else {
        Err(Status::unauthenticated("invalid portproxy authorization"))
    }
}

fn bearer_token_from_metadata(metadata: &MetadataMap) -> Option<String> {
    let raw = metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())?
        .trim();
    raw.strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(ToOwned::to_owned)
}

fn token_matches(provided: &str, expected: &str) -> bool {
    let provided = provided.as_bytes();
    let expected = expected.as_bytes();
    if provided.len() != expected.len() {
        return false;
    }
    let diff = provided
        .iter()
        .zip(expected.iter())
        .fold(0u8, |diff, (a, b)| diff | (a ^ b));
    diff == 0
}

const CHILD_REAPER_FALLBACK_INTERVAL: Duration = Duration::from_millis(250);

async fn reap_zombies(tracker: ChildTracker) {
    let Ok(mut sigchld) = signal(SignalKind::child()) else {
        warn!("failed to install SIGCHLD handler; falling back to child reaper polling");
        loop {
            reap_available_children(&tracker);
            sleep(CHILD_REAPER_FALLBACK_INTERVAL).await;
        }
    };

    loop {
        reap_available_children(&tracker);
        tokio::select! {
            _ = sigchld.recv() => {}
            _ = sleep(CHILD_REAPER_FALLBACK_INTERVAL) => {}
        }
    }
}

fn reap_available_children(tracker: &ChildTracker) {
    use nix::errno::Errno;

    for tracked_pid in tracker.snapshot() {
        match waitpid(Pid::from_raw(tracked_pid), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => continue,
            Ok(WaitStatus::Exited(pid, status)) => {
                if tracker.record_exit(pid.as_raw(), ChildExit::Exited(status)) {
                    info!("Reaped tracked pid {} with status {}", pid, status);
                } else {
                    info!("Reaped untracked pid {} with status {}", pid, status);
                }
            }
            Ok(WaitStatus::Signaled(pid, signal, _)) => {
                if tracker.record_exit(pid.as_raw(), ChildExit::Signaled(signal as i32)) {
                    info!("Reaped tracked pid {} terminated by {:?}", pid, signal);
                } else {
                    info!("Reaped untracked pid {} terminated by {:?}", pid, signal);
                }
            }
            Ok(_) => continue,
            Err(Errno::ECHILD) => continue,
            Err(err) => {
                warn!("waitpid failed: {}", err);
                continue;
            }
        }
    }
}

async fn forward_signals(tracker: ChildTracker) -> anyhow::Result<()> {
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;

    let received = tokio::select! {
        _ = term.recv() => UnixSignal::SIGTERM,
        _ = int.recv() => UnixSignal::SIGINT,
    };

    info!(
        "Received signal {:?}, forwarding to child processes",
        received
    );

    let pids = tracker.snapshot();
    for pid in pids {
        if pid <= 0 {
            continue;
        }
        if let Err(err) = signal_process_group_or_pid(pid, received) {
            warn!("failed to send {:?} to pid {}: {}", received, pid, err);
        }
    }

    sleep(Duration::from_secs(2)).await;
    info!("Shutting down after signal");
    process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_token_file_is_loaded_without_exposing_the_token_in_service_arguments() {
        let directory = tempfile::tempdir().expect("tempdir");
        let token_file = directory.path().join("portproxy.token");
        fs::write(&token_file, "file-token\n").expect("write token");

        assert_eq!(
            load_auth_token(None, Some(token_file))
                .expect("load token")
                .as_deref(),
            Some("file-token")
        );
    }

    #[test]
    fn configured_auth_token_file_fails_closed() {
        let directory = tempfile::tempdir().expect("tempdir");

        assert!(load_auth_token(None, Some(directory.path().join("missing.token"))).is_err());
    }

    #[test]
    fn bearer_token_from_metadata_extracts_authorization_value() {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer secret-token".parse().unwrap());

        assert_eq!(
            bearer_token_from_metadata(request.metadata()).as_deref(),
            Some("secret-token")
        );
    }

    #[test]
    fn authorize_portproxy_request_rejects_missing_or_wrong_token() {
        assert!(
            authorize_portproxy_request(Request::new(()), "secret-token")
                .expect_err("missing token must be rejected")
                .code()
                == tonic::Code::Unauthenticated
        );

        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer wrong-token".parse().unwrap());
        assert!(
            authorize_portproxy_request(request, "secret-token")
                .expect_err("wrong token must be rejected")
                .code()
                == tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn authorize_portproxy_request_accepts_matching_token() {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer secret-token".parse().unwrap());

        assert!(authorize_portproxy_request(request, "secret-token").is_ok());
    }

    #[test]
    fn default_log_directives_suppress_transport_trace_flood() {
        // The console-flood mitigation lives in these directives: our own spans
        // stay at info while h2/hyper/tonic/tower are forced to warn. Assert each
        // suppression directive is present so a regression here is caught before
        // it can flood ttyS0 again.
        for directive in ["info", "h2=warn", "hyper=warn", "tonic=warn", "tower=warn"] {
            assert!(
                DEFAULT_LOG_DIRECTIVES.contains(directive),
                "default log directives must contain `{directive}`, got `{DEFAULT_LOG_DIRECTIVES}`"
            );
        }
    }

    #[test]
    fn default_log_directives_parse_into_a_valid_env_filter() {
        // build_env_filter falls back to DEFAULT_LOG_DIRECTIVES when RUST_LOG is
        // unset; that fallback must always construct without panicking.
        let _filter = EnvFilter::new(DEFAULT_LOG_DIRECTIVES);
    }
}
