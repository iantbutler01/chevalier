use std::env;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::warn;
use uuid::Uuid;

use crate::bootstrap;
use crate::state::types::VmMetadata;
use crate::virt;

pub const EFI_CODE_FILE_NAME: &str = "windows-efi-code.fd";
pub const EFI_VARS_FILE_NAME: &str = "windows-efi-vars.fd";
pub const STATE_DISK_FILE_NAME: &str = "windows-vfs-state.qcow2";
pub const RUNTIME_CONFIG_ISO_FILE_NAME: &str = "windows-runtime.iso";
pub const RUNTIME_CONFIG_DEVICE_ID: &str = "windows-runtime-config-device";
pub const RUNTIME_CONFIG_USB_ID: &str = "windows-runtime-config-usb";
pub const RUNTIME_CONFIG_BLOCK_NODE: &str = "windows-runtime-config";
pub const RUNTIME_CONFIG_FILE_NODE: &str = "windows-runtime-config-file";

const STATE_DISK_SIZE_GB: i32 = 16;

const AMD64: &str = "amd64";
const ARM64: &str = "arm64";

const AMD64_EFI_CODE_CANDIDATES: &[&str] = &[
    "/usr/share/OVMF/OVMF_CODE_4M.secboot.fd",
    "/usr/share/OVMF/OVMF_CODE.secboot.fd",
    "/usr/share/qemu/edk2-x86_64-secure-code.fd",
    "/opt/homebrew/opt/qemu/share/qemu/edk2-x86_64-secure-code.fd",
    "/usr/local/opt/qemu/share/qemu/edk2-x86_64-secure-code.fd",
    "/Applications/UTM.app/Contents/Resources/qemu/edk2-x86_64-secure-code.fd",
];

const ARM64_EFI_CODE_CANDIDATES: &[&str] = &[
    "/usr/share/AAVMF/AAVMF_CODE.secboot.fd",
    "/usr/share/qemu/edk2-aarch64-secure-code.fd",
    "/opt/homebrew/opt/qemu/share/qemu/edk2-aarch64-secure-code.fd",
    "/usr/local/opt/qemu/share/qemu/edk2-aarch64-secure-code.fd",
    "/Applications/UTM.app/Contents/Resources/qemu/edk2-aarch64-secure-code.fd",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateDescriptor {
    pub bundle_dir: PathBuf,
    pub disk_path: PathBuf,
    pub efi_code_path: PathBuf,
    pub efi_vars_path: PathBuf,
    pub architecture: String,
    pub profile: String,
    pub disk_sha256: String,
    pub status: String,
    pub production_ready: bool,
    pub virtual_size_bytes: u64,
}

pub fn load_template_descriptor(source: &Path) -> Result<TemplateDescriptor> {
    let (bundle_dir, manifest_path) = if source.is_dir() {
        (source.to_path_buf(), source.join("image-manifest.json"))
    } else {
        let parent = source
            .parent()
            .context("Windows template manifest has no parent directory")?;
        (parent.to_path_buf(), source.to_path_buf())
    };
    let bundle_dir = fs::canonicalize(&bundle_dir)
        .with_context(|| format!("resolve Windows template bundle {}", bundle_dir.display()))?;
    let manifest_bytes = fs::read(&manifest_path)
        .with_context(|| format!("read Windows template manifest {}", manifest_path.display()))?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes).with_context(|| {
        format!(
            "parse Windows template manifest {}",
            manifest_path.display()
        )
    })?;

    if manifest.get("schemaVersion").and_then(Value::as_u64) != Some(1) {
        bail!("Windows template manifest must use schemaVersion 1");
    }
    let platform = manifest
        .pointer("/guest/platform")
        .and_then(Value::as_str)
        .unwrap_or("windows");
    if platform != "windows" {
        bail!("Windows template manifest declares guest platform `{platform}`");
    }
    let architecture = normalize_architecture(
        manifest
            .pointer("/guest/architecture")
            .or_else(|| manifest.get("architecture"))
            .and_then(Value::as_str)
            .context("Windows template manifest is missing architecture")?,
    )?;
    let disk_file = manifest
        .pointer("/artifacts/disk/path")
        .or_else(|| manifest.pointer("/image/file"))
        .and_then(Value::as_str)
        .context("Windows template manifest is missing its qcow2 disk path")?;
    let disk_sha256 = manifest
        .pointer("/artifacts/disk/sha256")
        .or_else(|| manifest.pointer("/image/sha256"))
        .and_then(Value::as_str)
        .context("Windows template manifest is missing its disk SHA-256")?
        .to_ascii_lowercase();
    let virtual_size_bytes = manifest
        .pointer("/artifacts/disk/virtualSize")
        .or_else(|| manifest.pointer("/image/virtualSizeBytes"))
        .and_then(Value::as_u64)
        .context("Windows template manifest is missing its virtual disk size")?;
    let efi_vars_file = manifest
        .pointer("/artifacts/efiVars/path")
        .or_else(|| manifest.pointer("/efiVars/file"))
        .and_then(Value::as_str)
        .context("Windows template manifest is missing its EFI variables path")?;
    let profile = manifest
        .get("profile")
        .and_then(Value::as_str)
        .context("Windows template manifest is missing its profile")?
        .to_string();
    let status = manifest
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let production_ready = manifest
        .get("productionReady")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let disk_path = resolve_bundle_member(&bundle_dir, disk_file, "disk")?;
    let efi_vars_path = resolve_bundle_member(&bundle_dir, efi_vars_file, "EFI variables")?;
    let expected_code_sha256 = manifest
        .pointer("/firmwareCode/sha256")
        .or_else(|| manifest.pointer("/buildRuntime/secureBootCodeSha256"))
        .and_then(Value::as_str);
    let efi_code_path = resolve_efi_code(&bundle_dir, &architecture, expected_code_sha256)?;

    Ok(TemplateDescriptor {
        bundle_dir,
        disk_path,
        efi_code_path,
        efi_vars_path,
        architecture,
        profile,
        disk_sha256,
        status,
        production_ready,
        virtual_size_bytes,
    })
}

pub fn admit_native_architecture(
    template_architecture: &str,
    requested_architecture: &str,
    host_architecture: &str,
) -> Result<String> {
    let template = normalize_architecture(template_architecture)?;
    let host = normalize_architecture(host_architecture)?;
    let requested = if requested_architecture.trim().is_empty() {
        template.clone()
    } else {
        normalize_architecture(requested_architecture)?
    };
    if requested != template {
        bail!("Windows template architecture mismatch: requested {requested}, template {template}");
    }
    if template != host {
        bail!(
            "Windows template guests must match the host architecture: template {template}, host {host}"
        );
    }
    Ok(template)
}

pub async fn instantiate_template(
    descriptor: &TemplateDescriptor,
    vm_dir: &Path,
    disk_size_gb: i32,
    qemu_img_bin: &str,
) -> Result<()> {
    let disk_path = vm_dir.join("disk.qcow2");
    clone_or_copy(&descriptor.disk_path, &disk_path).await?;
    let template_size_gb = descriptor.virtual_size_bytes.div_ceil(1024_u64.pow(3)) as i32;
    if disk_size_gb > template_size_gb {
        virt::resize_qcow2(qemu_img_bin, &disk_path, disk_size_gb).await?;
    }
    clone_or_copy(&descriptor.efi_code_path, &vm_dir.join(EFI_CODE_FILE_NAME)).await?;
    clone_or_copy(&descriptor.efi_vars_path, &vm_dir.join(EFI_VARS_FILE_NAME)).await?;
    virt::create_qcow2(
        qemu_img_bin,
        &vm_dir.join(STATE_DISK_FILE_NAME),
        STATE_DISK_SIZE_GB,
    )
    .await?;
    Ok(())
}

pub fn create_runtime_config_iso(
    meta: &VmMetadata,
    vm_dir: &Path,
    generation: &str,
    vfs_service_token: &str,
) -> Result<()> {
    let mount = meta
        .shared_mounts
        .first()
        .context("Windows guest requires one VFS workspace mount")?;
    if meta.shared_mounts.len() != 1 || mount.vfs_endpoint.trim().is_empty() {
        bail!("Windows guest requires exactly one VFS-backed workspace mount");
    }
    let control_token = meta
        .metadata
        .get("chevalier.portproxy_auth_token")
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("Windows guest requires a control bearer token")?;
    if vfs_service_token.trim().is_empty() {
        bail!("Windows guest requires a VFS service token");
    }
    let endpoint = guest_visible_vfs_endpoint(&mount.vfs_endpoint)?;
    let bootstrap_password = format!("Aa1!{}", Uuid::new_v4().simple());
    let computer_name = windows_computer_name(&meta.id)?;
    let unattended = runtime_unattend_xml(
        &meta.architecture,
        &computer_name,
        "OpenBracket",
        &bootstrap_password,
    )?;
    let config = serde_json::to_vec_pretty(&serde_json::json!({
        "schemaVersion": 1,
        "vmId": meta.id,
        "generation": generation,
        "control": {
            "listenAddress": "0.0.0.0:13338",
            "tokenFile": "C:\\ProgramData\\Chevalier\\runtime\\control.token"
        },
        "vfs": {
            "endpoint": endpoint,
            "scope": mount.vfs_scope_path,
            "tokenFile": "C:\\ProgramData\\Chevalier\\runtime\\vfs.token",
            "stateDirectory": "C:\\ProgramData\\Chevalier\\state-volume\\workspace",
            "mountpoint": "W:",
            "statusFile": "C:\\ProgramData\\Chevalier\\state-volume\\workspace\\status.json",
            "drainTimeout": "90s"
        }
    }))?;
    bootstrap::create_data_iso(
        vm_dir.join(RUNTIME_CONFIG_ISO_FILE_NAME),
        "BRKCFG",
        vec![
            ("RUNTIME.JSN".to_string(), config),
            (
                "CTRL.TKN".to_string(),
                format!("{}\n", control_token.trim()).into_bytes(),
            ),
            (
                "VFS.TKN".to_string(),
                format!("{}\n", vfs_service_token.trim()).into_bytes(),
            ),
            (
                "BOOT.TKN".to_string(),
                format!("{bootstrap_password}\n").into_bytes(),
            ),
            ("AUTOUNATTEND.XML".to_string(), unattended.into_bytes()),
        ],
    )
}

fn windows_computer_name(vm_id: &str) -> Result<String> {
    let compact = vm_id
        .chars()
        .filter(|character| character.is_ascii_hexdigit())
        .collect::<String>();
    if compact.len() < 12 {
        bail!("Windows VM ID cannot form a computer name");
    }
    Ok(format!("OB-{}", compact[..12].to_ascii_uppercase()))
}

fn runtime_unattend_xml(
    architecture: &str,
    computer_name: &str,
    username: &str,
    password: &str,
) -> Result<String> {
    let architecture = normalize_architecture(architecture)?;
    let processor_architecture = match architecture.as_str() {
        AMD64 => "amd64",
        ARM64 => "arm64",
        _ => unreachable!("Windows architecture was normalized"),
    };
    let computer_name = xml_escape(computer_name);
    let username = xml_escape(username);
    let password = xml_escape(password);
    Ok(format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<unattend xmlns="urn:schemas-microsoft-com:unattend" xmlns:wcm="http://schemas.microsoft.com/WMIConfig/2002/State">
  <settings pass="specialize">
    <component name="Microsoft-Windows-Shell-Setup" processorArchitecture="{processor_architecture}" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS">
      <ComputerName>{computer_name}</ComputerName>
      <TimeZone>UTC</TimeZone>
    </component>
  </settings>
  <settings pass="oobeSystem">
    <component name="Microsoft-Windows-International-Core" processorArchitecture="{processor_architecture}" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS">
      <InputLocale>0409:00000409</InputLocale>
      <SystemLocale>en-US</SystemLocale>
      <UILanguage>en-US</UILanguage>
      <UserLocale>en-US</UserLocale>
    </component>
    <component name="Microsoft-Windows-Shell-Setup" processorArchitecture="{processor_architecture}" publicKeyToken="31bf3856ad364e35" language="neutral" versionScope="nonSxS">
      <OOBE>
        <HideEULAPage>true</HideEULAPage>
        <HideLocalAccountScreen>true</HideLocalAccountScreen>
        <HideOnlineAccountScreens>true</HideOnlineAccountScreens>
        <HideWirelessSetupInOOBE>true</HideWirelessSetupInOOBE>
        <ProtectYourPC>3</ProtectYourPC>
      </OOBE>
      <UserAccounts>
        <LocalAccounts>
          <LocalAccount wcm:action="add">
            <Password><Value>{password}</Value><PlainText>true</PlainText></Password>
            <Description>OpenBracket desktop user</Description>
            <DisplayName>{username}</DisplayName>
            <Group>Users</Group>
            <Name>{username}</Name>
          </LocalAccount>
        </LocalAccounts>
      </UserAccounts>
      <AutoLogon>
        <Password><Value>{password}</Value><PlainText>true</PlainText></Password>
        <Enabled>true</Enabled>
        <LogonCount>1</LogonCount>
        <Username>{username}</Username>
      </AutoLogon>
    </component>
  </settings>
</unattend>
"#
    ))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn guest_visible_vfs_endpoint(raw: &str) -> Result<String> {
    let mut endpoint = reqwest::Url::parse(raw).context("parse Windows VFS endpoint")?;
    if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
        bail!("Windows VFS endpoint must use http or https");
    }
    let host = endpoint
        .host_str()
        .context("Windows VFS endpoint has no host")?;
    if matches!(host, "127.0.0.1" | "localhost" | "0.0.0.0" | "::1" | "::") {
        endpoint
            .set_host(Some("10.0.2.2"))
            .map_err(|_| anyhow::anyhow!("rewrite Windows VFS endpoint host"))?;
    }
    Ok(endpoint.to_string())
}

pub fn build_qemu_args(
    meta: &VmMetadata,
    vm_dir: &Path,
    qmp_path: &Path,
    pid_path: &Path,
    host_architecture: &str,
    running_on_linux: bool,
    running_on_macos: bool,
    netdev: String,
) -> Result<Vec<String>> {
    let architecture =
        admit_native_architecture(&meta.architecture, &meta.architecture, host_architecture)?;
    let arm_machine = |accelerator: &str| {
        if meta.resources.memory_mb <= 3072 {
            format!("virt-10.2,highmem=off,accel={accelerator}")
        } else {
            format!("virt-10.2,highmem=on,accel={accelerator}")
        }
    };
    let (machine, cpu, root_device, state_device) = match architecture.as_str() {
        AMD64 if running_on_linux => (
            "q35,smm=on,accel=kvm".to_string(),
            "host,+invtsc,hv_relaxed,hv_vapic,hv_spinlocks=0x1fff,hv_time,migratable=off",
            "virtio-blk-pci,drive=windows-root",
            "virtio-blk-pci,drive=windows-state,serial=openbracket-vfs",
        ),
        AMD64 if running_on_macos => (
            "q35,smm=on,accel=hvf".to_string(),
            "host,hv_relaxed,hv_vapic,hv_spinlocks=0x1fff,hv_time",
            "virtio-blk-pci,drive=windows-root",
            "virtio-blk-pci,drive=windows-state,serial=openbracket-vfs",
        ),
        ARM64 if running_on_linux => (
            arm_machine("kvm"),
            "host",
            "nvme,drive=windows-root,serial=openbracket-root",
            "nvme,drive=windows-state,serial=openbracket-vfs",
        ),
        ARM64 if running_on_macos => (
            arm_machine("hvf"),
            "host",
            "nvme,drive=windows-root,serial=openbracket-root",
            "nvme,drive=windows-state,serial=openbracket-vfs",
        ),
        _ => bail!("native Windows QEMU guests require a Linux/KVM or macOS/HVF host"),
    };

    let disk_path = vm_dir.join("disk.qcow2");
    let efi_code_path = vm_dir.join(EFI_CODE_FILE_NAME);
    let efi_vars_path = vm_dir.join(EFI_VARS_FILE_NAME);
    let state_disk_path = vm_dir.join(STATE_DISK_FILE_NAME);
    let runtime_config_path = vm_dir.join(RUNTIME_CONFIG_ISO_FILE_NAME);
    for (label, path) in [
        ("Windows disk", &disk_path),
        ("Windows EFI code", &efi_code_path),
        ("Windows EFI variables", &efi_vars_path),
        ("Windows VFS state disk", &state_disk_path),
        ("Windows runtime config", &runtime_config_path),
    ] {
        if !path.is_file() {
            bail!("{label} is missing at {}", path.display());
        }
    }

    Ok(vec![
        "-machine".to_string(),
        machine,
        "-cpu".to_string(),
        cpu.to_string(),
        "-smp".to_string(),
        meta.resources.vcpu.to_string(),
        "-m".to_string(),
        meta.resources.memory_mb.to_string(),
        "-drive".to_string(),
        format!(
            "if=pflash,format=raw,readonly=on,file={}",
            efi_code_path.display()
        ),
        "-drive".to_string(),
        format!("if=pflash,format=raw,file={}", efi_vars_path.display()),
        "-drive".to_string(),
        format!(
            "file={},if=none,id=windows-root,cache=none,aio=threads,format=qcow2,discard=unmap,detect-zeroes=unmap",
            disk_path.display()
        ),
        "-device".to_string(),
        root_device.to_string(),
        "-drive".to_string(),
        format!(
            "file={},if=none,id=windows-state,cache=none,aio=threads,format=qcow2,discard=unmap,detect-zeroes=unmap",
            state_disk_path.display()
        ),
        "-device".to_string(),
        state_device.to_string(),
        "-netdev".to_string(),
        netdev,
        "-device".to_string(),
        format!("virtio-net-pci,netdev=net0,mac={}", meta.network.mac),
        "-device".to_string(),
        "qemu-xhci".to_string(),
        "-blockdev".to_string(),
        format!(
            "driver=file,filename={},node-name={RUNTIME_CONFIG_FILE_NODE},read-only=on",
            runtime_config_path.display()
        ),
        "-blockdev".to_string(),
        format!(
            "driver=raw,file={RUNTIME_CONFIG_FILE_NODE},node-name={RUNTIME_CONFIG_BLOCK_NODE},read-only=on"
        ),
        "-device".to_string(),
        format!("usb-bot,id={RUNTIME_CONFIG_USB_ID}"),
        "-device".to_string(),
        format!(
            "scsi-cd,bus={RUNTIME_CONFIG_USB_ID}.0,drive={RUNTIME_CONFIG_BLOCK_NODE},id={RUNTIME_CONFIG_DEVICE_ID}"
        ),
        "-device".to_string(),
        "usb-kbd".to_string(),
        "-device".to_string(),
        "usb-tablet".to_string(),
        "-device".to_string(),
        "ramfb".to_string(),
        "-device".to_string(),
        "virtio-gpu-pci".to_string(),
        "-vnc".to_string(),
        "127.0.0.1:0,to=99,share=ignore".to_string(),
        "-display".to_string(),
        "none".to_string(),
        "-serial".to_string(),
        "none".to_string(),
        "-qmp".to_string(),
        format!("unix:{},server=on,wait=off", qmp_path.display()),
        "-pidfile".to_string(),
        pid_path.display().to_string(),
        "-uuid".to_string(),
        meta.id.clone(),
    ])
}

fn normalize_architecture(value: &str) -> Result<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "amd64" | "x86_64" => Ok(AMD64.to_string()),
        "arm64" | "aarch64" => Ok(ARM64.to_string()),
        other => bail!("unsupported Windows template architecture `{other}`"),
    }
}

fn resolve_bundle_member(bundle_dir: &Path, relative: &str, label: &str) -> Result<PathBuf> {
    let relative_path = Path::new(relative);
    if relative_path.is_absolute()
        || relative_path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        bail!("Windows template {label} path must stay inside the bundle");
    }
    let candidate = fs::canonicalize(bundle_dir.join(relative_path)).with_context(|| {
        format!(
            "resolve Windows template {label} {}",
            bundle_dir.join(relative_path).display()
        )
    })?;
    if !candidate.starts_with(bundle_dir) || !candidate.is_file() {
        bail!("Windows template {label} path escapes or is not a file");
    }
    Ok(candidate)
}

fn resolve_efi_code(
    bundle_dir: &Path,
    architecture: &str,
    expected_sha256: Option<&str>,
) -> Result<PathBuf> {
    let env_name = match architecture {
        AMD64 => "CHEVALIER_SANDBOX_WINDOWS_AMD64_EFI_CODE",
        ARM64 => "CHEVALIER_SANDBOX_WINDOWS_ARM64_EFI_CODE",
        other => bail!("unsupported Windows EFI architecture `{other}`"),
    };
    let candidates = match architecture {
        AMD64 => AMD64_EFI_CODE_CANDIDATES,
        ARM64 => ARM64_EFI_CODE_CANDIDATES,
        _ => unreachable!(),
    };
    let mut paths = vec![bundle_dir.join("efi-code.fd")];
    if let Some(configured) = env::var_os(env_name).filter(|value| !value.is_empty()) {
        paths.push(PathBuf::from(configured));
    }
    if architecture == ARM64 {
        if let Some(builder_dir) = bundle_dir.parent().and_then(Path::parent) {
            paths.push(
                builder_dir
                    .join("artifacts")
                    .join("edk2-aarch64-secure-code.fd"),
            );
        }
    }
    paths.extend(candidates.iter().map(PathBuf::from));

    let expected = expected_sha256.map(|value| value.to_ascii_lowercase());
    for path in paths {
        if !path.is_file() {
            continue;
        }
        if let Some(expected) = expected.as_deref() {
            let actual = sha256_file(&path)?;
            if actual != expected {
                continue;
            }
        }
        return fs::canonicalize(&path)
            .with_context(|| format!("resolve Windows EFI code {}", path.display()));
    }
    let digest_detail = expected
        .map(|digest| format!(" with SHA-256 {digest}"))
        .unwrap_or_default();
    bail!(
        "no matching {architecture} Windows EFI code{digest_detail}; add efi-code.fd to the template or set {env_name}"
    )
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("open firmware for hashing {}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("hash firmware {}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

async fn clone_or_copy(source: &Path, destination: &Path) -> Result<()> {
    match virt::clone_file_cow(source, destination).await {
        Ok(()) => Ok(()),
        Err(clone_error) => {
            if let Err(error) = fs::remove_file(destination) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    return Err(error).with_context(|| {
                        format!("remove failed clone destination {}", destination.display())
                    });
                }
            }
            warn!(
                source = %source.display(),
                destination = %destination.display(),
                error = %clone_error,
                "CoW clone unavailable for Windows template artifact; copying bytes"
            );
            virt::copy_file(source, destination).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::types::{
        GuestProfile, GuestRuntime, NetworkSpec, ResourceSpec, SharedMountAvailability,
        SharedMountContinuity, SharedMountSpec, VmCapabilities, VmSource, VmSourceType, VmState,
    };
    use chrono::Utc;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn fixture_metadata(architecture: &str) -> VmMetadata {
        VmMetadata {
            id: "7a82a44f-a0a6-42f8-b41b-8f70e900151e".to_string(),
            name: "windows-test".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: VmState::Stopped,
            architecture: architecture.to_string(),
            guest_profile: GuestProfile::default(),
            guest_runtime: GuestRuntime::default(),
            capabilities: VmCapabilities::default(),
            source: VmSource {
                source_type: VmSourceType::WindowsTemplate,
                reference: "fixture".to_string(),
            },
            resources: ResourceSpec {
                vcpu: 4,
                memory_mb: 3072,
                disk_gb: 64,
            },
            network: NetworkSpec {
                mac: "52:54:00:12:34:56".to_string(),
                proxy_port: 30101,
                rpc_port: 30102,
            },
            metadata: HashMap::new(),
            snapshots: Vec::new(),
            shared_mounts: Vec::new(),
            pci_devices: Vec::new(),
            durable_volume: None,
            boot_incoming_ram_path: String::new(),
            started_at: None,
        }
    }

    fn runtime_files(root: &Path) {
        fs::write(root.join("disk.qcow2"), b"disk").unwrap();
        fs::write(root.join(EFI_CODE_FILE_NAME), b"code").unwrap();
        fs::write(root.join(EFI_VARS_FILE_NAME), b"vars").unwrap();
        fs::write(root.join(STATE_DISK_FILE_NAME), b"state").unwrap();
        fs::write(root.join(RUNTIME_CONFIG_ISO_FILE_NAME), b"config").unwrap();
    }

    #[test]
    fn loads_guest_winfsp_and_legacy_manifest_shapes() {
        let arm = TempDir::new().unwrap();
        fs::write(arm.path().join("image.qcow2"), b"disk").unwrap();
        fs::write(arm.path().join("efivars.fd"), b"vars").unwrap();
        fs::write(arm.path().join("efi-code.fd"), b"code").unwrap();
        fs::write(
            arm.path().join("image-manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "schemaVersion": 1,
                "profile": "windows-arm",
                "architecture": "arm64",
                "workspaceTransport": "guest-winfsp",
                "image": {
                    "file": "image.qcow2",
                    "sha256": "abc",
                    "virtualSizeBytes": 68_719_476_736_u64
                },
                "efiVars": { "file": "efivars.fd" },
                "firmwareCode": { "file": "efi-code.fd" }
            }))
            .unwrap(),
        )
        .unwrap();
        let descriptor = load_template_descriptor(arm.path()).unwrap();
        assert_eq!(descriptor.architecture, ARM64);
        assert_eq!(descriptor.profile, "windows-arm");

        let amd64 = TempDir::new().unwrap();
        fs::write(amd64.path().join("image.qcow2"), b"disk").unwrap();
        fs::write(amd64.path().join("efivars.fd"), b"vars").unwrap();
        fs::write(amd64.path().join("efi-code.fd"), b"code").unwrap();
        fs::write(
            amd64.path().join("image-manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "schemaVersion": 1,
                "profile": "windows-x64",
                "guest": { "platform": "windows", "architecture": "x86_64" },
                "artifacts": {
                    "disk": {
                        "path": "image.qcow2",
                        "sha256": "def",
                        "virtualSize": 68_719_476_736_u64
                    },
                    "efiVars": { "path": "efivars.fd" }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let descriptor = load_template_descriptor(amd64.path()).unwrap();
        assert_eq!(descriptor.architecture, AMD64);
        assert_eq!(descriptor.profile, "windows-x64");
    }

    #[test]
    fn architecture_admission_is_native_for_both_supported_architectures() {
        assert_eq!(
            admit_native_architecture("x86_64", "", "amd64").unwrap(),
            AMD64
        );
        assert_eq!(
            admit_native_architecture("arm64", "aarch64", "arm64").unwrap(),
            ARM64
        );
        assert!(admit_native_architecture("amd64", "", "arm64").is_err());
        assert!(admit_native_architecture("arm64", "amd64", "arm64").is_err());
    }

    #[test]
    fn runtime_gateway_endpoint_uses_the_slirp_host_alias() {
        assert_eq!(
            guest_visible_vfs_endpoint("http://127.0.0.1:63359/internal/chevalier/vfs/owner")
                .unwrap(),
            "http://10.0.2.2:63359/internal/chevalier/vfs/owner"
        );
        assert_eq!(
            guest_visible_vfs_endpoint("https://vfs.internal/owner").unwrap(),
            "https://vfs.internal/owner"
        );
        assert!(guest_visible_vfs_endpoint("file:///tmp/vfs").is_err());
    }

    #[test]
    fn runtime_iso_contains_first_boot_program_and_ephemeral_credentials() {
        let temp = TempDir::new().unwrap();
        let mut metadata = fixture_metadata(ARM64);
        metadata.metadata.insert(
            "chevalier.portproxy_auth_token".to_string(),
            "control-secret".to_string(),
        );
        metadata.shared_mounts.push(SharedMountSpec {
            host_path: String::new(),
            guest_path: "W:".to_string(),
            mount_tag: "workspace".to_string(),
            read_only: false,
            availability: SharedMountAvailability::NodeLocal,
            continuity: SharedMountContinuity::RestartSameNode,
            backend_profile: String::new(),
            vfs_endpoint: "http://127.0.0.1:63359/internal/chevalier/vfs/owner".to_string(),
            vfs_scope_path: "workspace/test".to_string(),
        });

        create_runtime_config_iso(&metadata, temp.path(), "generation-1", "vfs-secret").unwrap();
        let iso = fs::read(temp.path().join(RUNTIME_CONFIG_ISO_FILE_NAME)).unwrap();
        let text = String::from_utf8_lossy(&iso);
        assert!(text.contains("BOOT.TKN;1"));
        assert!(text.contains("AUTOUNATTEND.XML;1"));
        assert!(text.contains("OpenBracket"));
        assert!(text.contains("<Group>Users</Group>"));
        assert!(text.contains("<LogonCount>1</LogonCount>"));
        assert!(text.contains("control-secret"));
        assert!(text.contains("vfs-secret"));
    }

    #[test]
    fn arm64_launch_uses_hvf_nvme_and_private_efi_state() {
        let temp = TempDir::new().unwrap();
        runtime_files(temp.path());
        let args = build_qemu_args(
            &fixture_metadata(ARM64),
            temp.path(),
            &temp.path().join("qmp.sock"),
            &temp.path().join("qemu.pid"),
            ARM64,
            false,
            true,
            "user,id=net0".to_string(),
        )
        .unwrap();
        assert!(args.contains(&"virt-10.2,highmem=off,accel=hvf".to_string()));
        assert!(args.contains(&"nvme,drive=windows-root,serial=openbracket-root".to_string()));
        assert!(args.contains(&"nvme,drive=windows-state,serial=openbracket-vfs".to_string()));
        assert!(
            args.iter()
                .any(|argument| argument.contains(RUNTIME_CONFIG_ISO_FILE_NAME))
        );
        assert!(args.iter().any(|arg| arg.contains(EFI_VARS_FILE_NAME)));
        assert!(!args.iter().any(|arg| arg.contains("bootstrap.iso")));
        assert!(args.contains(&"virtio-gpu-pci".to_string()));
        assert!(args.contains(&"127.0.0.1:0,to=99,share=ignore".to_string()));

        let mut high_memory = fixture_metadata(ARM64);
        high_memory.resources.memory_mb = 4096;
        let args = build_qemu_args(
            &high_memory,
            temp.path(),
            &temp.path().join("qmp.sock"),
            &temp.path().join("qemu.pid"),
            ARM64,
            false,
            true,
            "user,id=net0".to_string(),
        )
        .unwrap();
        assert!(args.contains(&"virt-10.2,highmem=on,accel=hvf".to_string()));
    }

    #[test]
    fn amd64_launch_uses_kvm_and_virtio_root() {
        let temp = TempDir::new().unwrap();
        runtime_files(temp.path());
        let args = build_qemu_args(
            &fixture_metadata(AMD64),
            temp.path(),
            &temp.path().join("qmp.sock"),
            &temp.path().join("qemu.pid"),
            AMD64,
            true,
            false,
            "user,id=net0".to_string(),
        )
        .unwrap();
        assert!(args.contains(&"q35,smm=on,accel=kvm".to_string()));
        assert!(args.contains(&"virtio-blk-pci,drive=windows-root".to_string()));
        assert!(
            args.contains(&"virtio-blk-pci,drive=windows-state,serial=openbracket-vfs".to_string())
        );
        assert!(args.iter().any(|arg| arg.contains("hv_time")));
        assert!(args.contains(&"virtio-gpu-pci".to_string()));
        assert!(args.contains(&"127.0.0.1:0,to=99,share=ignore".to_string()));
    }
}
