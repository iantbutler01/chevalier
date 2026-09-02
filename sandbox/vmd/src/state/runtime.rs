// @dive-file: In-memory runtime state for active VM processes and monitor handles.
// @dive-rel: Owned by vmd state manager lifecycle operations for running VM bookkeeping.
// @dive-rel: Carries process/socket paths and transient runtime flags not persisted to metadata.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Error;
use chrono::{DateTime, Utc};

use crate::fuse::FuseHandle;
use crate::network::tap::VmTapNetworkHandle;
use crate::state::types::VmState;
use crate::virt::{MonitorHandle, VirtiofsdHandle};

#[derive(Clone, Debug)]
pub struct VmRuntime {
    pub runtime_dir: PathBuf,
    pub qmp_path: PathBuf,
    pub pid_path: PathBuf,
    pub state: VmState,
    pub started_at: Option<DateTime<Utc>>,
    pub monitor: Option<MonitorHandle>,
    pub command_pid: Option<u32>,
    pub exit_status: Arc<Mutex<Option<Error>>>,
    pub suspending: bool,
    /// Per-shared-mount virtiofsd children spawned alongside the qemu process. These
    /// are reaped on stop_vm / force_stop_vm; they also self-exit when qemu closes
    /// its vhost-user socket during a normal shutdown.
    pub virtiofsd_handles: Vec<VirtiofsdHandle>,
    pub fuse_handles: Vec<FuseHandle>,
    pub tap_network: Option<VmTapNetworkHandle>,
    pub consecutive_health_failures: u32,
    pub last_health_probe_at: Option<Instant>,
    pub health_probe_suppressed_until: Option<Instant>,
}

impl VmRuntime {
    pub fn new(vm_dir: &PathBuf) -> Self {
        let runtime_dir = qemu_runtime_dir_for_vm(vm_dir);
        Self {
            qmp_path: runtime_dir.join("qmp.sock"),
            pid_path: runtime_dir.join("qemu.pid"),
            runtime_dir,
            state: VmState::Stopped,
            started_at: None,
            monitor: None,
            command_pid: None,
            exit_status: Arc::new(Mutex::new(None)),
            suspending: false,
            virtiofsd_handles: Vec::new(),
            fuse_handles: Vec::new(),
            tap_network: None,
            consecutive_health_failures: 0,
            last_health_probe_at: None,
            health_probe_suppressed_until: None,
        }
    }

    pub fn reset_health_tracking(&mut self) {
        self.consecutive_health_failures = 0;
        self.last_health_probe_at = None;
        self.health_probe_suppressed_until = None;
    }

    pub fn clear_health_failures(&mut self) {
        self.consecutive_health_failures = 0;
    }
}

fn qemu_runtime_dir_for_vm(vm_dir: &Path) -> PathBuf {
    configured_runtime_root().join(runtime_component_for_vm_dir(vm_dir))
}

fn configured_runtime_root() -> PathBuf {
    std::env::var("CHEVALIER_SANDBOX_RUNTIME_DIR")
        .or_else(|_| std::env::var("BRACKET_SANDBOX_RUNTIME_DIR"))
        .map(|raw| raw.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/chevalier-vmd"))
}

fn runtime_component_for_vm_dir(vm_dir: &Path) -> String {
    vm_dir
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_runtime_component)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown-vm".to_string())
}

fn sanitize_runtime_component(raw: &str) -> String {
    raw.chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_socket_path_stays_short_for_long_vm_data_roots() {
        let vm_dir = PathBuf::from(
            "/home/crow/chevalier-sandbox-migration-test/.run/ovh-vmd-data-node2/e023ca02-7f6d-4dfc-8667-e2bda4e483fa",
        );
        let runtime = VmRuntime::new(&vm_dir);

        assert_eq!(
            runtime.qmp_path,
            PathBuf::from("/tmp/chevalier-vmd/e023ca02-7f6d-4dfc-8667-e2bda4e483fa/qmp.sock")
        );
        assert_eq!(
            runtime.pid_path,
            PathBuf::from("/tmp/chevalier-vmd/e023ca02-7f6d-4dfc-8667-e2bda4e483fa/qemu.pid")
        );
        assert!(runtime.qmp_path.to_string_lossy().len() < 108);
    }
}
