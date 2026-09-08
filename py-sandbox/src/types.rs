use chevalier_sandbox::{
    DurableVolumeInfo as EngineDurableVolumeInfo, HostPciDevice as EngineHostPciDevice,
    HostPciDeviceState as EngineHostPciDeviceState, HostPciFunction as EngineHostPciFunction,
    HostPciInventory as EngineHostPciInventory, PciDeviceAction as EnginePciDeviceAction,
    SessionInfo as EngineSessionInfo,
};
use serde::Serialize;

#[derive(Serialize)]
pub struct SessionDirectoryEntry {
    pub name: String,
    pub is_dir: bool,
    pub is_symlink: bool,
}

#[derive(Serialize)]
pub struct SessionCheckpoint {
    pub id: String,
}

#[derive(Serialize)]
pub struct SessionSnapshot {
    pub id: String,
    pub name: String,
    pub label: String,
    pub description: String,
}

#[derive(Serialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub vm_id: String,
    pub name: String,
    pub state: i32,
    pub parent_session_id: Option<String>,
    pub fork_id: Option<String>,
}

impl From<EngineSessionInfo> for SessionInfo {
    fn from(info: EngineSessionInfo) -> Self {
        Self {
            session_id: info.session_id,
            vm_id: info.vm_id,
            name: info.name,
            state: info.state,
            parent_session_id: info.parent_session_id,
            fork_id: info.fork_id,
        }
    }
}

#[derive(Serialize)]
pub struct DurableVolumeInfo {
    pub owner_key: String,
    pub volume_id: String,
    pub size_gb: i32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub backing_volume_id: Option<String>,
    pub attached_vm_ids: Vec<String>,
}

impl From<EngineDurableVolumeInfo> for DurableVolumeInfo {
    fn from(info: EngineDurableVolumeInfo) -> Self {
        Self {
            owner_key: info.owner_key,
            volume_id: info.volume_id,
            size_gb: info.size_gb,
            created_at_ms: info.created_at_ms,
            updated_at_ms: info.updated_at_ms,
            backing_volume_id: info.backing_volume_id,
            attached_vm_ids: info.attached_vm_ids,
        }
    }
}

#[derive(Serialize)]
pub struct HostPciFunction {
    pub bdf: String,
    pub vendor_id: String,
    pub device_id: String,
    pub class_code: String,
    pub driver: String,
    pub iommu_group: String,
}

impl From<EngineHostPciFunction> for HostPciFunction {
    fn from(function: EngineHostPciFunction) -> Self {
        Self {
            bdf: function.bdf,
            vendor_id: function.vendor_id,
            device_id: function.device_id,
            class_code: function.class_code,
            driver: function.driver,
            iommu_group: function.iommu_group,
        }
    }
}

#[derive(Serialize)]
pub struct HostPciDevice {
    pub id: String,
    pub label: String,
    pub functions: Vec<HostPciFunction>,
    pub state: String,
    pub assigned_vm_id: String,
    pub managed: bool,
    pub hotplug_capable: bool,
    pub unavailable_reason: String,
}

impl From<EngineHostPciDevice> for HostPciDevice {
    fn from(device: EngineHostPciDevice) -> Self {
        let state = match device.state {
            EngineHostPciDeviceState::Disabled => "disabled",
            EngineHostPciDeviceState::Unavailable => "unavailable",
            EngineHostPciDeviceState::Host => "host",
            EngineHostPciDeviceState::Ready => "ready",
            EngineHostPciDeviceState::Assigned => "assigned",
            EngineHostPciDeviceState::Error => "error",
            EngineHostPciDeviceState::Unknown => "unknown",
        };
        Self {
            id: device.id,
            label: device.label,
            functions: device.functions.into_iter().map(Into::into).collect(),
            state: state.to_string(),
            assigned_vm_id: device.assigned_vm_id,
            managed: device.managed,
            hotplug_capable: device.hotplug_capable,
            unavailable_reason: device.unavailable_reason,
        }
    }
}

#[derive(Serialize)]
pub struct HostPciInventory {
    pub enabled: bool,
    pub devices: Vec<HostPciDevice>,
}

impl From<EngineHostPciInventory> for HostPciInventory {
    fn from(inventory: EngineHostPciInventory) -> Self {
        Self {
            enabled: inventory.enabled,
            devices: inventory.devices.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Serialize)]
pub struct PciDeviceAction {
    pub device: Option<HostPciDevice>,
    pub restart_required: bool,
    pub detail: String,
    pub vm_state: String,
}

impl From<EnginePciDeviceAction> for PciDeviceAction {
    fn from(action: EnginePciDeviceAction) -> Self {
        Self {
            device: action.device.map(Into::into),
            restart_required: action.restart_required,
            detail: action.detail,
            vm_state: vm_state_label(action.vm_state),
        }
    }
}

pub fn vm_state_label(state: i32) -> String {
    match state {
        1 => "creating",
        2 => "stopped",
        3 => "running",
        4 => "paused",
        5 => "error",
        _ => "unknown",
    }
    .to_string()
}

#[derive(Serialize)]
pub struct SessionDesktopTarget {
    pub kind: String,
    pub host: Option<String>,
    pub port: Option<u32>,
    pub password: Option<String>,
    pub authentication: String,
    pub view_only: bool,
}

impl From<chevalier_sandbox::SessionDesktopTarget> for SessionDesktopTarget {
    fn from(target: chevalier_sandbox::SessionDesktopTarget) -> Self {
        Self {
            kind: match target.kind {
                chevalier_sandbox::SessionDesktopKind::Vnc => "vnc".to_string(),
                chevalier_sandbox::SessionDesktopKind::NativeWindow => "native-window".to_string(),
            },
            host: target.host,
            port: target.port.map(u32::from),
            password: target.password,
            authentication: match target.authentication {
                chevalier_sandbox::SessionDesktopAuthentication::None => "none".to_string(),
                chevalier_sandbox::SessionDesktopAuthentication::Password => "password".to_string(),
                chevalier_sandbox::SessionDesktopAuthentication::Account => "account".to_string(),
            },
            view_only: target.view_only,
        }
    }
}
