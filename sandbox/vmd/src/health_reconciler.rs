// @dive-file: Background VM health reconciler that marks wedged guest-exec runtimes Error before user traffic waits on them.
// @dive-rel: Uses vmd-local process/portproxy visibility and Manager mark_vm_error recovery hooks; API recovery then restores or replaces on next attach.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use futures::FutureExt;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::guest_exec_probe::{
    GuestExecProbeFailure, GuestExecProbeFailureKind, portproxy_auth_header_from_token,
    probe_guest_exec_ready_for_platform,
};
use crate::state::manager::VmHealthProbeTarget;
use crate::state::{Manager, ManagerResult};

const HEALTH_RECONCILER_ENV: &str = "CHEVALIER_VMD_HEALTH_RECONCILER";
const HEALTH_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);
const HEALTH_RECONCILE_MIN_PROBE_INTERVAL: Duration = Duration::from_secs(25);
const HEALTH_RECONCILE_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const HEALTH_RECONCILE_COMMAND_TIMEOUT_SECS: i32 = 3;
const HEALTH_RECONCILE_FAILURE_THRESHOLD: u32 = 2;
// @dive: start_vm/restore_snapshot already wait for QMP Running; this grace is for
// guest-space portproxy/exec startup so cold restores do not get marked Error early.
const HEALTH_RECONCILE_START_GRACE: Duration = Duration::from_secs(180);

static MARKED_ERROR_AUTH_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MARKED_ERROR_GUEST_EXEC_UNREADY_TOTAL: AtomicU64 = AtomicU64::new(0);
static MARKED_ERROR_INVALID_AUTH_HEADER_TOTAL: AtomicU64 = AtomicU64::new(0);
static MARKED_ERROR_MISSING_RPC_PORT_TOTAL: AtomicU64 = AtomicU64::new(0);

pub struct VmHealthReconcilerHandle {
    stop_tx: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl VmHealthReconcilerHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

pub async fn start(manager: Arc<Manager>) -> Result<Option<VmHealthReconcilerHandle>> {
    if !health_reconciler_enabled() {
        info!(env = HEALTH_RECONCILER_ENV, "vm health reconciler disabled");
        return Ok(None);
    }

    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(HEALTH_RECONCILE_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = ticker.tick() => {
                    reconcile_once(&manager).await;
                }
            }
        }
    });

    info!("vm health reconciler started");
    Ok(Some(VmHealthReconcilerHandle {
        stop_tx: Some(stop_tx),
        join: Some(join),
    }))
}

async fn reconcile_once(manager: &Manager) {
    let targets = manager
        .running_vm_health_probe_targets(
            Utc::now(),
            Instant::now(),
            HEALTH_RECONCILE_START_GRACE,
            HEALTH_RECONCILE_MIN_PROBE_INTERVAL,
        )
        .await;

    for target in targets {
        let vm_id = target.vm_id.clone();
        if AssertUnwindSafe(reconcile_target(manager, target))
            .catch_unwind()
            .await
            .is_err()
        {
            warn!(
                vm_id = %vm_id,
                "vm health probe panicked; continuing reconciler loop"
            );
        }
    }
}

async fn reconcile_target(manager: &Manager, target: VmHealthProbeTarget) {
    let probe_result = probe_target(&target).await;
    match probe_result {
        Ok(()) => match manager
            .record_vm_health_probe_success(&target.vm_id, target.started_at)
            .await
        {
            Ok(true) => {
                info!(
                    vm_id = %target.vm_id,
                    "vm health probe recovered after transient failures"
                );
            }
            Ok(false) => {}
            Err(err) => {
                warn!(
                    vm_id = %target.vm_id,
                    error = %err,
                    "failed recording vm health probe success"
                );
            }
        },
        Err(failure) => {
            record_failure_and_maybe_mark(manager, &target, &failure).await;
        }
    }
}

async fn record_failure_and_maybe_mark(
    manager: &Manager,
    target: &VmHealthProbeTarget,
    failure: &ProbeFailure,
) {
    match record_probe_failure(manager, target, failure).await {
        Ok(Some(failures)) => mark_unhealthy_vm(manager, &target.vm_id, failure, failures).await,
        Ok(None) => {}
        Err(err) => {
            warn!(
                vm_id = %target.vm_id,
                error = %err,
                "failed recording vm health probe failure"
            );
        }
    }
}

async fn record_probe_failure(
    manager: &Manager,
    target: &VmHealthProbeTarget,
    failure: &ProbeFailure,
) -> ManagerResult<Option<u32>> {
    manager
        .record_vm_health_probe_failure(
            &target.vm_id,
            target.started_at,
            HEALTH_RECONCILE_FAILURE_THRESHOLD,
            failure.reason.is_permanent(),
        )
        .await
}

async fn probe_target(target: &VmHealthProbeTarget) -> Result<(), ProbeFailure> {
    if target.rpc_port <= 0 {
        return Err(ProbeFailure::permanent(
            ProbeFailureReason::MissingRpcPort,
            "running vm has no rpc_port",
        ));
    }
    let auth_header = portproxy_auth_header_from_token(target.portproxy_auth_token.as_deref())
        .map_err(|error| {
            ProbeFailure::permanent(ProbeFailureReason::InvalidAuthHeader, error.to_string())
        })?;
    let endpoint = format!("http://127.0.0.1:{}", target.rpc_port);
    match tokio::time::timeout(
        HEALTH_RECONCILE_PROBE_TIMEOUT,
        probe_guest_exec_ready_for_platform(
            endpoint.as_str(),
            auth_header.as_ref(),
            HEALTH_RECONCILE_COMMAND_TIMEOUT_SECS,
            target.platform,
        ),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ProbeFailure::from_guest_exec(error)),
        Err(_) => Err(ProbeFailure::transient(
            ProbeFailureReason::GuestExecUnready,
            "guest exec readiness probe timed out",
        )),
    }
}

async fn mark_unhealthy_vm(
    manager: &Manager,
    vm_id: &str,
    failure: &ProbeFailure,
    consecutive_failures: u32,
) {
    let reason = failure.reason.as_str();
    let diagnostic = format!("{}: {}", reason, failure.message);
    match manager.mark_vm_error(vm_id, diagnostic.as_str()).await {
        Ok(_) => {
            let count = increment_marked_error_counter(failure.reason);
            warn!(
                metric = "vmd_health_reconcile_marked_error_total",
                reason = %reason,
                count,
                vm_id = %vm_id,
                consecutive_failures,
                error = %failure.message,
                "vm health reconciler marked vm error"
            );
        }
        Err(err) => {
            warn!(
                vm_id = %vm_id,
                reason = %reason,
                error = %err,
                "vm health reconciler failed marking vm error"
            );
        }
    }
}

fn increment_marked_error_counter(reason: ProbeFailureReason) -> u64 {
    match reason {
        ProbeFailureReason::AuthRejected => {
            MARKED_ERROR_AUTH_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed) + 1
        }
        ProbeFailureReason::GuestExecUnready => {
            MARKED_ERROR_GUEST_EXEC_UNREADY_TOTAL.fetch_add(1, Ordering::Relaxed) + 1
        }
        ProbeFailureReason::InvalidAuthHeader => {
            MARKED_ERROR_INVALID_AUTH_HEADER_TOTAL.fetch_add(1, Ordering::Relaxed) + 1
        }
        ProbeFailureReason::MissingRpcPort => {
            MARKED_ERROR_MISSING_RPC_PORT_TOTAL.fetch_add(1, Ordering::Relaxed) + 1
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ProbeFailureReason {
    AuthRejected,
    GuestExecUnready,
    InvalidAuthHeader,
    MissingRpcPort,
}

impl ProbeFailureReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::AuthRejected => "auth_rejected",
            Self::GuestExecUnready => "guest_exec_unready",
            Self::InvalidAuthHeader => "invalid_auth_header",
            Self::MissingRpcPort => "missing_rpc_port",
        }
    }

    fn is_permanent(self) -> bool {
        matches!(
            self,
            Self::AuthRejected | Self::InvalidAuthHeader | Self::MissingRpcPort
        )
    }
}

#[derive(Debug)]
struct ProbeFailure {
    reason: ProbeFailureReason,
    message: String,
}

impl ProbeFailure {
    fn permanent(reason: ProbeFailureReason, message: impl Into<String>) -> Self {
        debug_assert!(reason.is_permanent());
        Self {
            reason,
            message: message.into(),
        }
    }

    fn transient(reason: ProbeFailureReason, message: impl Into<String>) -> Self {
        debug_assert!(!reason.is_permanent());
        Self {
            reason,
            message: message.into(),
        }
    }

    fn from_guest_exec(error: GuestExecProbeFailure) -> Self {
        match error.kind() {
            GuestExecProbeFailureKind::PermanentAuth => {
                Self::permanent(ProbeFailureReason::AuthRejected, error.to_string())
            }
            GuestExecProbeFailureKind::Transient => {
                Self::transient(ProbeFailureReason::GuestExecUnready, error.to_string())
            }
        }
    }
}

fn health_reconciler_enabled() -> bool {
    health_reconciler_enabled_value(std::env::var(HEALTH_RECONCILER_ENV).ok().as_deref())
}

fn health_reconciler_enabled_value(value: Option<&str>) -> bool {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    !matches!(
        value.to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "off" | "disabled"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_reconciler_is_enabled_by_default() {
        assert!(health_reconciler_enabled_value(None));
        assert!(health_reconciler_enabled_value(Some("")));
        assert!(health_reconciler_enabled_value(Some("1")));
        assert!(health_reconciler_enabled_value(Some("true")));
    }

    #[test]
    fn health_reconciler_disable_values_are_recognized() {
        for value in ["0", "false", "False", "NO", "off", "disabled"] {
            assert!(!health_reconciler_enabled_value(Some(value)));
        }
    }
}
