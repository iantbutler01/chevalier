//! Disabled-by-default host PCI assignment support.
//!
//! vmd only exposes devices named in an operator-owned policy. Product concepts such
//! as threads, GPUs, or model servers intentionally live above this module.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use tokio::process::Command;

pub const PCI_CAPABILITY_HEADER: &str = "x-chevalier-pci-token";

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PciDevicePolicy {
    pub id: String,
    #[serde(default)]
    pub label: String,
    pub bdfs: Vec<String>,
    #[serde(default)]
    pub managed: bool,
    #[serde(default)]
    pub hotplug: bool,
    #[serde(default)]
    pub prepare_command: Vec<String>,
    #[serde(default)]
    pub release_command: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PciPolicyFile {
    capability_token: String,
    #[serde(default)]
    devices: Vec<PciDevicePolicy>,
}

#[derive(Clone)]
pub struct PciConfig {
    capability_token: Option<String>,
    devices: Vec<PciDevicePolicy>,
    sysfs_root: PathBuf,
    dev_root: PathBuf,
}

impl std::fmt::Debug for PciConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PciConfig")
            .field("enabled", &self.enabled())
            .field("device_count", &self.devices.len())
            .field("sysfs_root", &self.sysfs_root)
            .field("dev_root", &self.dev_root)
            .finish()
    }
}

impl Default for PciConfig {
    fn default() -> Self {
        Self {
            capability_token: None,
            devices: Vec::new(),
            sysfs_root: PathBuf::from("/sys"),
            dev_root: PathBuf::from("/dev"),
        }
    }
}

impl PciConfig {
    pub fn load(path: &Path) -> Result<Self> {
        validate_policy_permissions(path)?;
        let bytes =
            fs::read(path).with_context(|| format!("read PCI policy {}", path.display()))?;
        let policy: PciPolicyFile = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse PCI policy {}", path.display()))?;
        let mut config = Self {
            capability_token: Some(policy.capability_token),
            devices: policy.devices,
            ..Self::default()
        };
        config.normalize()?;
        Ok(config)
    }

    pub fn enabled(&self) -> bool {
        !self.devices.is_empty()
    }

    pub fn capability_token(&self) -> Option<&str> {
        self.capability_token.as_deref()
    }

    pub fn devices(&self) -> &[PciDevicePolicy] {
        &self.devices
    }

    pub fn device(&self, id: &str) -> Option<&PciDevicePolicy> {
        self.devices.iter().find(|device| device.id == id)
    }

    #[cfg(test)]
    pub fn for_test(
        capability_token: impl Into<String>,
        devices: Vec<PciDevicePolicy>,
        sysfs_root: PathBuf,
        dev_root: PathBuf,
    ) -> Result<Self> {
        let mut config = Self {
            capability_token: Some(capability_token.into()),
            devices,
            sysfs_root,
            dev_root,
        };
        config.normalize()?;
        Ok(config)
    }

    fn normalize(&mut self) -> Result<()> {
        if self.devices.is_empty() {
            self.capability_token = None;
            return Ok(());
        }
        if self
            .capability_token
            .as_deref()
            .is_none_or(|token| token.trim().is_empty())
        {
            bail!("PCI policy requires a non-empty capabilityToken");
        }

        let mut ids = HashSet::new();
        let mut bdfs = HashSet::new();
        for device in &mut self.devices {
            device.id = normalize_device_id(&device.id)?;
            if !ids.insert(device.id.clone()) {
                bail!("duplicate PCI policy id `{}`", device.id);
            }
            device.label = device.label.trim().to_string();
            if device.label.is_empty() {
                device.label = device.id.clone();
            }
            if device.bdfs.is_empty() {
                bail!("PCI policy `{}` must contain at least one BDF", device.id);
            }
            for bdf in &mut device.bdfs {
                *bdf = normalize_bdf(bdf)?;
                if !bdfs.insert(bdf.clone()) {
                    bail!("PCI BDF `{bdf}` appears in more than one policy device");
                }
            }
            device.bdfs.sort();
            device.bdfs.dedup();
            validate_hook(&device.prepare_command, &device.id, "prepareCommand")?;
            validate_hook(&device.release_command, &device.id, "releaseCommand")?;
        }
        self.devices.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(())
    }

    fn pci_device_path(&self, bdf: &str) -> PathBuf {
        self.sysfs_root.join("bus/pci/devices").join(bdf)
    }

    fn driver_probe_path(&self) -> PathBuf {
        self.sysfs_root.join("bus/pci/drivers_probe")
    }

    fn vfio_driver_path(&self) -> PathBuf {
        self.sysfs_root.join("bus/pci/drivers/vfio-pci")
    }

    fn driver_path(&self, driver: &str) -> PathBuf {
        self.sysfs_root.join("bus/pci/drivers").join(driver)
    }

    fn vfio_group_path(&self, group: &str) -> PathBuf {
        self.dev_root.join("vfio").join(group)
    }
}

fn validate_policy_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let metadata =
            fs::metadata(path).with_context(|| format!("stat PCI policy {}", path.display()))?;
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.uid() != 0 && metadata.uid() != effective_uid {
            bail!(
                "PCI policy {} must be owned by root or the vmd service user",
                path.display()
            );
        }
        if metadata.mode() & 0o077 != 0 {
            bail!("PCI policy {} must have mode 0600", path.display());
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PciDeviceAssignmentSpec {
    pub id: String,
    pub bdfs: Vec<String>,
    #[serde(default)]
    pub iommu_groups: Vec<String>,
    #[serde(default)]
    pub original_drivers: HashMap<String, String>,
}

#[derive(Debug)]
pub struct PciRollbackError {
    pub assignment: PciDeviceAssignmentSpec,
    source: anyhow::Error,
}

impl std::fmt::Display for PciRollbackError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.source)
    }
}

impl std::error::Error for PciRollbackError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PciInventoryState {
    Unavailable,
    Host,
    Ready,
    Assigned,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PciFunctionInfo {
    pub bdf: String,
    pub vendor_id: String,
    pub device_id: String,
    pub class_code: String,
    pub driver: String,
    pub iommu_group: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PciInventoryDevice {
    pub id: String,
    pub label: String,
    pub functions: Vec<PciFunctionInfo>,
    pub state: PciInventoryState,
    pub assigned_vm_id: String,
    pub managed: bool,
    pub hotplug_capable: bool,
    pub unavailable_reason: String,
}

pub fn qemu_device_id(device_id: &str, function_index: usize) -> String {
    let normalized: String = device_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    format!("pci_{normalized}_{function_index}")
}

pub fn assignment_ready(config: &PciConfig, assignment: &PciDeviceAssignmentSpec) -> bool {
    !assignment.iommu_groups.is_empty()
        && assignment.bdfs.iter().all(|bdf| {
            driver_name(&config.pci_device_path(bdf)) == "vfio-pci"
                && !iommu_group(&config.pci_device_path(bdf)).is_empty()
        })
        && assignment
            .iommu_groups
            .iter()
            .all(|group| config.vfio_group_path(group).exists())
}

pub fn normalize_bdf(raw: &str) -> Result<String> {
    let value = raw.trim().to_ascii_lowercase();
    let with_domain = if value.matches(':').count() == 1 {
        format!("0000:{value}")
    } else {
        value
    };
    let bytes = with_domain.as_bytes();
    let valid = bytes.len() == 12
        && bytes[4] == b':'
        && bytes[7] == b':'
        && bytes[10] == b'.'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7 | 10) || byte.is_ascii_hexdigit());
    if !valid {
        bail!("invalid PCI BDF `{raw}`; expected dddd:bb:ss.f");
    }
    Ok(with_domain)
}

fn normalize_device_id(raw: &str) -> Result<String> {
    let value = raw.trim().to_ascii_lowercase();
    if value.is_empty()
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        bail!("invalid PCI policy id `{raw}`");
    }
    Ok(value)
}

fn validate_hook(command: &[String], id: &str, field: &str) -> Result<()> {
    if command.is_empty() {
        return Ok(());
    }
    if command[0].trim().is_empty() || command.iter().any(|arg| arg.contains('\0')) {
        bail!("PCI policy `{id}` has an invalid {field}");
    }
    Ok(())
}

fn read_trimmed(path: &Path) -> Result<String> {
    Ok(fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?
        .trim()
        .to_string())
}

fn read_hex_attribute(path: &Path) -> String {
    read_trimmed(path)
        .map(|value| value.trim_start_matches("0x").to_ascii_lowercase())
        .unwrap_or_default()
}

fn driver_name(device_path: &Path) -> String {
    fs::read_link(device_path.join("driver"))
        .ok()
        .and_then(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_default()
}

fn iommu_group(device_path: &Path) -> String {
    fs::read_link(device_path.join("iommu_group"))
        .ok()
        .and_then(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_default()
}

fn group_members(device_path: &Path) -> Result<Vec<String>> {
    let group_path = fs::read_link(device_path.join("iommu_group"))
        .with_context(|| format!("device {} has no IOMMU group", device_path.display()))?;
    let absolute_group = if group_path.is_absolute() {
        group_path
    } else {
        device_path
            .join("iommu_group")
            .parent()
            .unwrap_or(device_path)
            .join(group_path)
    };
    let mut members = Vec::new();
    for entry in fs::read_dir(absolute_group.join("devices"))? {
        let entry = entry?;
        members.push(entry.file_name().to_string_lossy().into_owned());
    }
    members.sort();
    Ok(members)
}

fn is_iommu_bridge_infrastructure(device_path: &Path) -> bool {
    let class_code = read_hex_attribute(&device_path.join("class"));
    class_code.starts_with("0600") || class_code.starts_with("0604")
}

fn running_as_root() -> bool {
    #[cfg(unix)]
    {
        unsafe { libc::geteuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn inspect_function(config: &PciConfig, bdf: &str) -> Result<PciFunctionInfo> {
    let path = config.pci_device_path(bdf);
    if !path.exists() {
        bail!("PCI function {bdf} does not exist");
    }
    Ok(PciFunctionInfo {
        bdf: bdf.to_string(),
        vendor_id: read_hex_attribute(&path.join("vendor")),
        device_id: read_hex_attribute(&path.join("device")),
        class_code: read_hex_attribute(&path.join("class")),
        driver: driver_name(&path),
        iommu_group: iommu_group(&path),
    })
}

fn validate_iommu_group(config: &PciConfig, policy: &PciDevicePolicy) -> Result<()> {
    let configured: HashSet<&str> = policy.bdfs.iter().map(String::as_str).collect();
    for bdf in &policy.bdfs {
        let path = config.pci_device_path(bdf);
        let group = iommu_group(&path);
        if group.is_empty() {
            bail!("IOMMU is not enabled for PCI function {bdf}");
        }
        for member in group_members(&path)? {
            if configured.contains(member.as_str()) {
                continue;
            }
            let member_path = config.pci_device_path(&member);
            if is_iommu_bridge_infrastructure(&member_path) {
                continue;
            }
            bail!(
                "PCI policy `{}` does not include IOMMU group {} member {}",
                policy.id,
                group,
                member
            );
        }
    }
    Ok(())
}

pub fn inventory(config: &PciConfig, leases: &HashMap<String, String>) -> Vec<PciInventoryDevice> {
    if !config.enabled() {
        return Vec::new();
    }
    config
        .devices()
        .iter()
        .map(|policy| inventory_device(config, policy, leases.get(&policy.id)))
        .collect()
}

fn inventory_device(
    config: &PciConfig,
    policy: &PciDevicePolicy,
    assigned_vm_id: Option<&String>,
) -> PciInventoryDevice {
    let inspected: Result<Vec<_>> = policy
        .bdfs
        .iter()
        .map(|bdf| inspect_function(config, bdf))
        .collect();
    let functions = match inspected {
        Ok(functions) => functions,
        Err(error) => {
            return PciInventoryDevice {
                id: policy.id.clone(),
                label: policy.label.clone(),
                functions: Vec::new(),
                state: if assigned_vm_id.is_some() {
                    PciInventoryState::Assigned
                } else {
                    PciInventoryState::Unavailable
                },
                assigned_vm_id: assigned_vm_id.cloned().unwrap_or_default(),
                managed: policy.managed,
                hotplug_capable: policy.hotplug,
                unavailable_reason: error.to_string(),
            };
        }
    };
    let mut device = PciInventoryDevice {
        id: policy.id.clone(),
        label: policy.label.clone(),
        functions,
        state: PciInventoryState::Unavailable,
        assigned_vm_id: assigned_vm_id.cloned().unwrap_or_default(),
        managed: policy.managed,
        hotplug_capable: policy.hotplug,
        unavailable_reason: String::new(),
    };
    if let Some(vm_id) = assigned_vm_id {
        device.state = PciInventoryState::Assigned;
        device.assigned_vm_id = vm_id.clone();
        return device;
    }
    if cfg!(not(target_os = "linux")) {
        device.unavailable_reason =
            "PCI passthrough is supported only on Linux vmd hosts".to_string();
        return device;
    }
    if let Err(error) = validate_iommu_group(config, policy) {
        device.unavailable_reason = error.to_string();
        return device;
    }
    if policy.managed && !running_as_root() {
        device.unavailable_reason =
            "managed PCI assignment requires vmd to run as root".to_string();
        return device;
    }
    let all_vfio = device
        .functions
        .iter()
        .all(|function| function.driver == "vfio-pci");
    if !policy.managed && !all_vfio {
        device.unavailable_reason =
            "device must be pre-bound to vfio-pci or configured as managed".to_string();
        return device;
    }
    device.state = if all_vfio {
        PciInventoryState::Ready
    } else {
        PciInventoryState::Host
    };
    device
}

async fn run_hook(command: &[String], label: &str, device_id: &str) -> Result<()> {
    let Some(program) = command.first() else {
        return Ok(());
    };
    let status = Command::new(program)
        .args(&command[1..])
        .status()
        .await
        .with_context(|| format!("run PCI {label} hook for {device_id}"))?;
    if !status.success() {
        bail!("PCI {label} hook for {device_id} exited with {status}");
    }
    Ok(())
}

fn write_sysfs(path: &Path, value: &str) -> Result<()> {
    fs::write(path, value.as_bytes()).with_context(|| format!("write {}", path.display()))
}

async fn ensure_vfio_driver(config: &PciConfig) -> Result<()> {
    if config.vfio_driver_path().exists() {
        return Ok(());
    }
    let mut last_error = None;
    for program in ["modprobe", "/sbin/modprobe", "/usr/sbin/modprobe"] {
        match Command::new(program).arg("vfio-pci").status().await {
            Ok(status) if status.success() && config.vfio_driver_path().exists() => return Ok(()),
            Ok(status) => last_error = Some(anyhow!("{program} exited with {status}")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => last_error = Some(error.into()),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("vfio-pci is unavailable"))).context("load vfio-pci")
}

fn bind_driver(config: &PciConfig, bdf: &str, driver: &str) -> Result<()> {
    let device_path = config.pci_device_path(bdf);
    write_sysfs(&device_path.join("driver_override"), driver)?;
    let current = driver_name(&device_path);
    if !current.is_empty() && current != driver {
        write_sysfs(&device_path.join("driver/unbind"), bdf)?;
    }
    if driver_name(&device_path) != driver {
        write_sysfs(&config.driver_probe_path(), bdf)?;
    }
    let rebound = driver_name(&device_path);
    if rebound != driver {
        bail!("PCI function {bdf} bound to `{rebound}` instead of `{driver}`");
    }
    Ok(())
}

fn clear_driver_binding(config: &PciConfig, bdf: &str) -> Result<()> {
    let device_path = config.pci_device_path(bdf);
    let current = driver_name(&device_path);
    if !current.is_empty() {
        write_sysfs(&device_path.join("driver/unbind"), bdf)?;
    }
    write_sysfs(&device_path.join("driver_override"), "\n")
}

fn probe_driver(config: &PciConfig, bdf: &str, original_driver: &str) -> Result<()> {
    if original_driver.is_empty() {
        return Ok(());
    }
    let device_path = config.pci_device_path(bdf);
    if driver_name(&device_path).is_empty() {
        write_sysfs(&config.driver_probe_path(), bdf)?;
    }
    let restored = driver_name(&device_path);
    if restored != original_driver {
        bail!("PCI function {bdf} restored to `{restored}` instead of `{original_driver}`");
    }
    Ok(())
}

async fn ensure_kernel_driver(config: &PciConfig, driver: &str) -> Result<()> {
    if driver.is_empty() || config.driver_path(driver).exists() {
        return Ok(());
    }
    let mut last_error = None;
    for program in ["modprobe", "/sbin/modprobe", "/usr/sbin/modprobe"] {
        match Command::new(program).arg(driver).status().await {
            Ok(status) if status.success() && config.driver_path(driver).exists() => return Ok(()),
            Ok(status) => last_error = Some(anyhow!("{program} exited with {status}")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => last_error = Some(error.into()),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("kernel driver `{driver}` is unavailable")))
        .with_context(|| format!("load original PCI driver `{driver}`"))
}

#[cfg(unix)]
fn grant_vfio_group(path: &Path, uid: u32, gid: u32) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let encoded = CString::new(path.as_os_str().as_bytes())?;
    let result = unsafe { libc::chown(encoded.as_ptr(), uid, gid) };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("chown {}", path.display()));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o660))
        .with_context(|| format!("chmod {}", path.display()))
}

#[cfg(not(unix))]
fn grant_vfio_group(_path: &Path, _uid: u32, _gid: u32) -> Result<()> {
    bail!("VFIO permissions are supported only on Unix hosts")
}

pub async fn prepare_assignment(
    config: &PciConfig,
    device_id: &str,
    qemu_uid: u32,
    qemu_gid: u32,
) -> Result<PciDeviceAssignmentSpec> {
    let policy = config
        .device(device_id)
        .ok_or_else(|| anyhow!("PCI device `{device_id}` is not allowlisted"))?;
    validate_iommu_group(config, policy)?;

    let mut original_drivers = HashMap::new();
    let mut groups = HashSet::new();
    for bdf in &policy.bdfs {
        let path = config.pci_device_path(bdf);
        original_drivers.insert(bdf.clone(), driver_name(&path));
        let group = iommu_group(&path);
        if group.is_empty() {
            bail!("IOMMU is not enabled for PCI function {bdf}");
        }
        groups.insert(group);
    }
    let mut assignment = PciDeviceAssignmentSpec {
        id: policy.id.clone(),
        bdfs: policy.bdfs.clone(),
        iommu_groups: groups.into_iter().collect(),
        original_drivers,
    };
    assignment.iommu_groups.sort();

    if policy.managed && !running_as_root() {
        bail!("managed PCI assignment requires vmd to run as root");
    }
    if let Err(error) = run_hook(&policy.prepare_command, "prepare", &policy.id).await {
        let release_error = run_hook(&policy.release_command, "release", &policy.id)
            .await
            .err();
        return Err(match release_error {
            Some(release_error) => error.context(format!(
                "PCI prepare-hook cleanup also failed: {release_error}"
            )),
            None => error,
        });
    }

    let setup = async {
        if policy.managed {
            ensure_vfio_driver(config).await?;
            for bdf in &policy.bdfs {
                bind_driver(config, bdf, "vfio-pci")?;
            }
        } else {
            for bdf in &policy.bdfs {
                let driver = driver_name(&config.pci_device_path(bdf));
                if driver != "vfio-pci" {
                    bail!("PCI function {bdf} is bound to `{driver}`, not vfio-pci");
                }
            }
        }
        for group in &assignment.iommu_groups {
            let path = config.vfio_group_path(group);
            if !path.exists() {
                bail!("VFIO group device {} does not exist", path.display());
            }
            if policy.managed || running_as_root() {
                grant_vfio_group(&path, qemu_uid, qemu_gid)?;
            }
        }
        Result::<()>::Ok(())
    }
    .await;
    if let Err(error) = setup {
        let rollback = release_assignment(config, &assignment).await;
        return match rollback {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(anyhow::Error::new(PciRollbackError {
                assignment,
                source: error.context(format!("PCI bind rollback also failed: {rollback_error}")),
            })),
        };
    }
    Ok(assignment)
}

pub async fn release_assignment(
    config: &PciConfig,
    assignment: &PciDeviceAssignmentSpec,
) -> Result<()> {
    let policy = config.device(&assignment.id);
    let managed = policy.is_some_and(|policy| policy.managed)
        || assignment
            .original_drivers
            .values()
            .any(|driver| !driver.is_empty() && driver != "vfio-pci");
    if managed {
        if !running_as_root() {
            bail!("managed PCI release requires vmd to run as root");
        }
        let functions = assignment
            .bdfs
            .iter()
            .rev()
            .map(|bdf| {
                let original = assignment
                    .original_drivers
                    .get(bdf)
                    .cloned()
                    .unwrap_or_default();
                (bdf.clone(), original)
            })
            .collect::<Vec<_>>();
        for (bdf, _) in &functions {
            clear_driver_binding(config, bdf)?;
        }
        let mut drivers = functions
            .iter()
            .map(|(_, driver)| driver.clone())
            .filter(|driver| !driver.is_empty())
            .collect::<Vec<_>>();
        drivers.sort();
        drivers.dedup();
        for driver in &drivers {
            ensure_kernel_driver(config, driver).await?;
        }
        for (bdf, original) in &functions {
            probe_driver(config, bdf, original)?;
        }
    }
    if let Some(policy) = policy {
        run_hook(&policy.release_command, "release", &policy.id).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    fn policy(id: &str, bdfs: &[&str]) -> PciDevicePolicy {
        PciDevicePolicy {
            id: id.to_string(),
            label: String::new(),
            bdfs: bdfs.iter().map(|value| value.to_string()).collect(),
            managed: false,
            hotplug: false,
            prepare_command: Vec::new(),
            release_command: Vec::new(),
        }
    }

    #[test]
    fn disabled_config_has_no_devices_or_token() {
        let config = PciConfig::default();
        assert!(!config.enabled());
        assert!(config.capability_token().is_none());
        assert!(inventory(&config, &HashMap::new()).is_empty());
    }

    #[test]
    fn normalizes_bdfs_and_rejects_duplicates() {
        assert_eq!(normalize_bdf("09:00.0").unwrap(), "0000:09:00.0");
        assert!(normalize_bdf("09:0.0").is_err());
        let error = PciConfig::for_test(
            "token",
            vec![
                policy("gpu-a", &["09:00.0"]),
                policy("gpu-b", &["0000:09:00.0"]),
            ],
            PathBuf::from("/sys"),
            PathBuf::from("/dev"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("more than one policy device"));
    }

    #[test]
    fn enabled_policy_requires_a_capability_token() {
        let error = PciConfig::for_test(
            " ",
            vec![policy("gpu", &["09:00.0"])],
            PathBuf::from("/sys"),
            PathBuf::from("/dev"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("non-empty capabilityToken"));
    }

    #[cfg(unix)]
    fn add_group_member(sysfs_root: &Path, group_path: &Path, bdf: &str, class_code: &str) {
        let device_path = sysfs_root.join("bus/pci/devices").join(bdf);
        fs::create_dir_all(&device_path).expect("create fake PCI function");
        fs::write(device_path.join("class"), class_code).expect("write fake PCI class");
        symlink(group_path, device_path.join("iommu_group")).expect("link fake IOMMU group");
        fs::create_dir_all(group_path.join("devices").join(bdf))
            .expect("add fake IOMMU group member");
    }

    #[cfg(unix)]
    #[test]
    fn iommu_group_allows_host_and_pcie_bridges_but_rejects_unlisted_endpoints() {
        let temp = tempfile::tempdir().expect("create fake sysfs");
        let sysfs_root = temp.path().join("sys");
        let group_path = sysfs_root.join("kernel/iommu_groups/13");
        fs::create_dir_all(group_path.join("devices")).expect("create fake IOMMU group");

        add_group_member(&sysfs_root, &group_path, "0000:80:00.0", "0x060000");
        add_group_member(&sysfs_root, &group_path, "0000:80:01.1", "0x060400");
        add_group_member(&sysfs_root, &group_path, "0000:81:00.0", "0x030000");
        add_group_member(&sysfs_root, &group_path, "0000:81:00.1", "0x040300");

        let device = policy("gpu-a", &["81:00.0", "81:00.1"]);
        let config = PciConfig::for_test(
            "token",
            vec![device.clone()],
            sysfs_root.clone(),
            temp.path().join("dev"),
        )
        .expect("build PCI config");
        let normalized_device = &config.devices()[0];
        validate_iommu_group(&config, normalized_device)
            .expect("bridge-only group companions are valid");

        add_group_member(&sysfs_root, &group_path, "0000:80:02.0", "0x010802");
        let error =
            validate_iommu_group(&config, normalized_device).expect_err("extra endpoint must fail");
        assert!(error.to_string().contains("0000:80:02.0"));
    }

    #[cfg(unix)]
    #[test]
    fn policy_file_must_be_private_and_loads_when_mode_is_0600() {
        let temp = tempfile::tempdir().expect("create policy tempdir");
        let path = temp.path().join("pci-policy.json");
        fs::write(
            &path,
            r#"{
              "capabilityToken": "pci-secret",
              "devices": [{
                "id": "GPU_A",
                "label": "GPU A",
                "bdfs": ["09:00.0"],
                "managed": false,
                "hotplug": false
              }]
            }"#,
        )
        .expect("write policy");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("protect policy");

        let config = PciConfig::load(&path).expect("load protected policy");
        assert!(config.enabled());
        assert_eq!(config.capability_token(), Some("pci-secret"));
        assert_eq!(config.devices()[0].id, "gpu_a");
        assert_eq!(config.devices()[0].bdfs, ["0000:09:00.0"]);

        fs::set_permissions(&path, fs::Permissions::from_mode(0o640))
            .expect("make policy group-readable");
        let error = PciConfig::load(&path).unwrap_err();
        assert!(error.to_string().contains("mode 0600"));
    }
}
