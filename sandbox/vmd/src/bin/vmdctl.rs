// @dive-file: Operator CLI client for vmd lifecycle, snapshot, and fork operations.
// @dive-rel: Calls protobuf APIs exposed by vmd service and mirrors runtime config defaults.
// @dive-rel: Used by verification/integration harness scripts for black-box runtime assertions.

#![allow(clippy::collapsible_if)]
#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::convert::TryFrom;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use prost_types::Timestamp;
use serde_json::json;
use tonic::Request;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;
use vmd_rs::config::{self, Config};
use vmd_rs::image::{self, BASE_IMAGE_EXT, BASE_IMAGE_SIZE_GB};
use vmd_rs::proto::v1::vmd_service_client::VmdServiceClient;
use vmd_rs::proto::v1::{
    CreateSnapshotRequest, CreateVmPhase, CreateVmRequest, CreateVmStreamResponse,
    DeleteSnapshotRequest, DeleteVmRequest, ForkVmRequest, GetVmRequest, ListSnapshotsRequest,
    ListVMsRequest, Metadata, PreDownloadVmImagePhase, PreDownloadVmImageRequest, ResourceSpec,
    RestoreSnapshotRequest, Snapshot, Vm, VmActionRequest, VmSource,
    VmSourceType as ProtoVmSourceType, VmState as ProtoVmState, create_vm_stream_response,
};
use vmd_rs::virt;

#[derive(Parser, Debug)]
#[command(name = "vmdctl", about = "Bracket VM daemon CLI")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8052")]
    server: String,
    #[arg(long, default_value = "300")]
    timeout_secs: u64,
    #[arg(long)]
    auth_token: Option<String>,
    #[arg(long)]
    auth_token_file: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    ListVms {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        include_snapshots: bool,
    },
    GetVm {
        vm_id: String,
        #[arg(long)]
        json: bool,
    },
    CreateVm {
        #[arg(long, required = true)]
        source_ref: String,
        #[arg(long, value_enum, default_value = "docker")]
        source_type: SourceType,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value_t = 1)]
        vcpu: i32,
        #[arg(long, default_value_t = 1024)]
        memory_mb: i32,
        #[arg(long, default_value_t = 10)]
        disk_gb: i32,
        #[arg(long)]
        metadata: Vec<String>,
        #[arg(long)]
        auto_start: bool,
        #[arg(long)]
        arch: Option<String>,
        #[arg(long)]
        json: bool,
    },
    PreDownloadImage {
        #[arg(long, required = true)]
        source_ref: String,
        #[arg(long)]
        arch: Option<String>,
        #[arg(long)]
        force: bool,
    },
    DeleteVm {
        vm_id: String,
        #[arg(long)]
        purge_snapshots: bool,
    },
    ForkVm {
        parent_vm_id: String,
        #[arg(long)]
        child_name: Option<String>,
        #[arg(long)]
        child_metadata: Vec<String>,
        #[arg(long)]
        auto_start_child: bool,
        #[arg(long)]
        json: bool,
    },
    StartVm {
        vm_id: String,
        #[arg(long)]
        json: bool,
    },
    StopVm {
        vm_id: String,
        #[arg(long)]
        json: bool,
    },
    RestartVm {
        vm_id: String,
        #[arg(long)]
        json: bool,
    },
    PauseVm {
        vm_id: String,
        #[arg(long)]
        json: bool,
    },
    ResumeVm {
        vm_id: String,
        #[arg(long)]
        json: bool,
    },
    ForceStopVm {
        vm_id: String,
        #[arg(long)]
        json: bool,
    },
    ListSnapshots {
        vm_id: String,
        #[arg(long)]
        json: bool,
    },
    CreateSnapshot {
        vm_id: String,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        description: Option<String>,
        #[arg(long)]
        json: bool,
    },
    RestoreSnapshot {
        vm_id: String,
        snapshot_id: String,
        #[arg(long)]
        json: bool,
    },
    DeleteSnapshot {
        vm_id: String,
        snapshot_id: String,
    },
    ConvertImage {
        /// Docker image reference (e.g. repo/name:tag)
        image: String,
        /// Target architecture (amd64 or arm64)
        arch: String,
        /// Optional output path; defaults to the base-images dir layout
        #[arg(long)]
        output: Option<String>,
        /// Optional override for the local data directory
        #[arg(long)]
        data_dir: Option<String>,
        /// Optional override for the docker binary
        #[arg(long)]
        docker_bin: Option<String>,
        /// Skip checking S3 for prebuilt images and always build locally
        #[arg(long)]
        force_local_build: bool,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum SourceType {
    Docker,
    Snapshot,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let auth_header = compile_auth_header(resolve_secret(
        cli.auth_token.clone(),
        cli.auth_token_file.as_deref(),
    )?)?;
    match cli.command {
        Commands::ConvertImage {
            image,
            arch,
            output,
            data_dir,
            docker_bin,
            force_local_build,
        } => convert_image(image, arch, output, data_dir, docker_bin, force_local_build).await,
        other => {
            let mut client = connect(&cli.server, cli.timeout_secs).await?;
            match other {
                Commands::ListVms {
                    json,
                    include_snapshots,
                } => list_vms(&mut client, include_snapshots, json, auth_header.as_ref()).await?,
                Commands::GetVm { vm_id, json } => {
                    get_vm(&mut client, &vm_id, json, auth_header.as_ref()).await?
                }
                Commands::CreateVm {
                    source_ref,
                    source_type,
                    name,
                    vcpu,
                    memory_mb,
                    disk_gb,
                    metadata,
                    auto_start,
                    arch,
                    json,
                } => {
                    create_vm(
                        &mut client,
                        source_ref,
                        source_type,
                        name,
                        vcpu,
                        memory_mb,
                        disk_gb,
                        metadata,
                        auto_start,
                        arch,
                        json,
                        auth_header.as_ref(),
                    )
                    .await?
                }
                Commands::PreDownloadImage {
                    source_ref,
                    arch,
                    force,
                } => {
                    pre_download_image(&mut client, source_ref, arch, force, auth_header.as_ref())
                        .await?
                }
                Commands::DeleteVm {
                    vm_id,
                    purge_snapshots,
                } => delete_vm(&mut client, &vm_id, purge_snapshots, auth_header.as_ref()).await?,
                Commands::ForkVm {
                    parent_vm_id,
                    child_name,
                    child_metadata,
                    auto_start_child,
                    json,
                } => {
                    fork_vm(
                        &mut client,
                        &parent_vm_id,
                        child_name,
                        child_metadata,
                        auto_start_child,
                        json,
                        auth_header.as_ref(),
                    )
                    .await?
                }
                Commands::StartVm { vm_id, json } => {
                    vm_action(
                        &mut client,
                        &vm_id,
                        Action::Start,
                        json,
                        auth_header.as_ref(),
                    )
                    .await?
                }
                Commands::StopVm { vm_id, json } => {
                    vm_action(
                        &mut client,
                        &vm_id,
                        Action::Stop,
                        json,
                        auth_header.as_ref(),
                    )
                    .await?
                }
                Commands::RestartVm { vm_id, json } => {
                    vm_action(
                        &mut client,
                        &vm_id,
                        Action::Restart,
                        json,
                        auth_header.as_ref(),
                    )
                    .await?
                }
                Commands::PauseVm { vm_id, json } => {
                    vm_action(
                        &mut client,
                        &vm_id,
                        Action::Pause,
                        json,
                        auth_header.as_ref(),
                    )
                    .await?
                }
                Commands::ResumeVm { vm_id, json } => {
                    vm_action(
                        &mut client,
                        &vm_id,
                        Action::Resume,
                        json,
                        auth_header.as_ref(),
                    )
                    .await?
                }
                Commands::ForceStopVm { vm_id, json } => {
                    vm_action(
                        &mut client,
                        &vm_id,
                        Action::ForceStop,
                        json,
                        auth_header.as_ref(),
                    )
                    .await?
                }
                Commands::ListSnapshots { vm_id, json } => {
                    list_snapshots(&mut client, &vm_id, json, auth_header.as_ref()).await?
                }
                Commands::CreateSnapshot {
                    vm_id,
                    label,
                    description,
                    json,
                } => {
                    create_snapshot(
                        &mut client,
                        &vm_id,
                        label,
                        description,
                        json,
                        auth_header.as_ref(),
                    )
                    .await?
                }
                Commands::RestoreSnapshot {
                    vm_id,
                    snapshot_id,
                    json,
                } => {
                    restore_snapshot(
                        &mut client,
                        &vm_id,
                        &snapshot_id,
                        json,
                        auth_header.as_ref(),
                    )
                    .await?
                }
                Commands::DeleteSnapshot { vm_id, snapshot_id } => {
                    delete_snapshot(&mut client, &vm_id, &snapshot_id, auth_header.as_ref()).await?
                }
                Commands::ConvertImage { .. } => unreachable!(),
            }
            Ok(())
        }
    }
}

async fn connect(server: &str, timeout_secs: u64) -> Result<VmdServiceClient<Channel>> {
    let endpoint = Endpoint::from_shared(server.to_string())
        .with_context(|| format!("invalid server endpoint {server}"))?
        .timeout(Duration::from_secs(timeout_secs));
    Ok(VmdServiceClient::connect(endpoint).await?)
}

fn request_with_auth<T>(message: T, auth_header: Option<&MetadataValue<Ascii>>) -> Request<T> {
    let mut request = Request::new(message);
    if let Some(value) = auth_header {
        request
            .metadata_mut()
            .insert("authorization", value.clone());
    }
    request
}

fn resolve_secret(inline: Option<String>, file_path: Option<&str>) -> Result<Option<String>> {
    if let Some(value) = inline {
        let trimmed = value.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(Some(trimmed));
        }
    }
    let Some(file_path) = file_path else {
        return Ok(None);
    };
    let value = std::fs::read_to_string(file_path)?;
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(trimmed))
}

fn compile_auth_header(raw_token: Option<String>) -> Result<Option<MetadataValue<Ascii>>> {
    let token = raw_token
        .or_else(|| env::var("CHEVALIER_SANDBOX_AUTH_TOKEN").ok())
        .or_else(|| env::var("BRACKET_SANDBOX_AUTH_TOKEN").ok());
    let Some(token) = token else {
        return Ok(None);
    };
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let value = if trimmed.starts_with("Bearer ") {
        trimmed.to_string()
    } else {
        format!("Bearer {trimmed}")
    };
    let metadata = MetadataValue::try_from(value.as_str())
        .with_context(|| "authorization token is not valid ASCII metadata")?;
    Ok(Some(metadata))
}

async fn list_vms(
    client: &mut VmdServiceClient<Channel>,
    include_snapshots: bool,
    json: bool,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    let response = client
        .list_v_ms(request_with_auth(
            ListVMsRequest { include_snapshots },
            auth_header,
        ))
        .await?
        .into_inner();
    if json {
        let values: Vec<_> = response.vms.iter().map(vm_to_json).collect();
        println!("{}", serde_json::to_string_pretty(&values)?);
    } else {
        for vm in response.vms {
            let state = ProtoVmState::try_from(vm.state).unwrap_or(ProtoVmState::Unspecified);
            println!("{:<36} {:<20} {:?}", vm.id, vm.name, state);
        }
    }
    Ok(())
}

async fn get_vm(
    client: &mut VmdServiceClient<Channel>,
    vm_id: &str,
    json: bool,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    let response = client
        .get_vm(request_with_auth(
            GetVmRequest {
                vm_id: vm_id.to_string(),
            },
            auth_header,
        ))
        .await?
        .into_inner();
    if json {
        println!("{}", serde_json::to_string_pretty(&vm_to_json(&response))?);
    } else {
        print_vm(&response);
    }
    Ok(())
}

async fn create_vm(
    client: &mut VmdServiceClient<Channel>,
    source_ref: String,
    source_type: SourceType,
    name: Option<String>,
    vcpu: i32,
    memory_mb: i32,
    disk_gb: i32,
    metadata: Vec<String>,
    auto_start: bool,
    arch: Option<String>,
    json: bool,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    let mut meta_map = HashMap::new();
    for entry in metadata {
        if let Some((key, value)) = entry.split_once('=') {
            meta_map.insert(key.to_string(), value.to_string());
        } else {
            bail!("invalid metadata entry {entry}, expected key=value");
        }
    }
    let request = CreateVmRequest {
        name: name.unwrap_or_default(),
        source: Some(VmSource {
            r#type: match source_type {
                SourceType::Docker => ProtoVmSourceType::Docker as i32,
                SourceType::Snapshot => ProtoVmSourceType::Snapshot as i32,
            },
            reference: source_ref,
        }),
        resources: Some(ResourceSpec {
            vcpu,
            memory_mb,
            disk_gb,
        }),
        metadata: if meta_map.is_empty() {
            None
        } else {
            Some(Metadata { entries: meta_map })
        },
        auto_start,
        architecture: arch.unwrap_or_default(),
        shared_mounts: Vec::new(),
        pci_device_ids: Vec::new(),
    };

    let mut stream = client
        .create_vm(request_with_auth(request, auth_header))
        .await?
        .into_inner();
    let pb = ProgressBar::new(100);
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} {msg} {percent:>3}%")
            .unwrap()
            .tick_strings(&["-", "\\", "|", "/"]),
    );
    pb.enable_steady_tick(Duration::from_millis(120));
    let mut current_phase = CreateVmPhase::Unspecified;
    let mut final_vm: Option<Vm> = None;

    loop {
        match stream.message().await {
            Ok(Some(CreateVmStreamResponse { event })) => match event {
                Some(create_vm_stream_response::Event::Progress(progress)) => {
                    let phase = CreateVmPhase::try_from(progress.phase)
                        .unwrap_or(CreateVmPhase::Unspecified);
                    if phase != current_phase && phase != CreateVmPhase::Complete {
                        pb.set_position(0);
                        current_phase = phase;
                    }
                    let percent = progress.percent.min(100) as u64;
                    pb.set_position(percent);
                    let label = phase_label(phase);
                    let message = if progress.message.is_empty() {
                        label.to_string()
                    } else {
                        format!("{label}: {}", progress.message)
                    };
                    pb.set_message(message);
                    if phase == CreateVmPhase::Complete && percent >= 100 {
                        pb.finish_with_message("VM ready");
                    }
                }
                Some(create_vm_stream_response::Event::Vm(vm)) => {
                    final_vm = Some(vm);
                }
                None => {}
            },
            Ok(None) => break,
            Err(status) => {
                pb.finish_and_clear();
                return Err(status.into());
            }
        }
    }

    if !pb.is_finished() {
        pb.finish_and_clear();
    }
    let response = final_vm.ok_or_else(|| anyhow::anyhow!("create_vm stream ended without VM"))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&vm_to_json(&response))?);
    } else {
        print_vm(&response);
    }
    Ok(())
}

async fn pre_download_image(
    client: &mut VmdServiceClient<Channel>,
    source_ref: String,
    arch: Option<String>,
    force: bool,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    if source_ref.trim().is_empty() {
        bail!("--source-ref must be provided");
    }

    let normalized_arch = match arch {
        Some(value) => Some(normalize_arch(&value)?),
        None => None,
    };

    let request = PreDownloadVmImageRequest {
        reference: source_ref,
        architecture: normalized_arch.unwrap_or_default(),
        force,
    };

    let mut stream = client
        .pre_download_vm_image(request_with_auth(request, auth_header))
        .await?
        .into_inner();

    let spinner_style = ProgressStyle::with_template("{spinner:.green} {msg}")
        .unwrap()
        .tick_strings(&["-", "\\", "|", "/"]);
    let bar_style = ProgressStyle::with_template(
        "{spinner:.green} {msg} [{wide_bar:.cyan/blue}] {bytes:>10}/{total_bytes:>10} ({eta})",
    )
    .unwrap()
    .tick_strings(&["-", "\\", "|", "/"]);
    let pb = ProgressBar::new_spinner();
    pb.set_style(spinner_style.clone());
    pb.enable_steady_tick(Duration::from_millis(120));
    let mut last_message = "preparing download".to_string();
    pb.set_message(last_message.clone());
    let mut has_length = false;

    loop {
        match stream.message().await {
            Ok(Some(update)) => {
                if !update.message.is_empty() {
                    last_message = update.message.clone();
                    pb.set_message(last_message.clone());
                }
                if !has_length && update.total_bytes > 0 {
                    pb.set_length(update.total_bytes);
                    pb.set_style(bar_style.clone());
                    has_length = true;
                }
                if has_length {
                    let length = pb.length().unwrap_or(0);
                    let clamped = update.downloaded_bytes.min(length);
                    pb.set_position(clamped);
                }
                match PreDownloadVmImagePhase::try_from(update.phase)
                    .unwrap_or(PreDownloadVmImagePhase::Unspecified)
                {
                    PreDownloadVmImagePhase::CheckingCache => {
                        if last_message.is_empty() {
                            pb.set_message("checking cache");
                        }
                    }
                    PreDownloadVmImagePhase::AlreadyPresent => {
                        let msg = if last_message.is_empty() {
                            "base image already present".to_string()
                        } else {
                            last_message.clone()
                        };
                        pb.finish_with_message(msg);
                        return Ok(());
                    }
                    PreDownloadVmImagePhase::Downloading => {
                        if !has_length {
                            let msg = if last_message.is_empty() {
                                format!("downloading... {} bytes", update.downloaded_bytes)
                            } else {
                                format!("{last_message} ({} bytes)", update.downloaded_bytes)
                            };
                            pb.set_message(msg);
                        }
                    }
                    PreDownloadVmImagePhase::Complete => {
                        let msg = if last_message.is_empty() {
                            "base image ready".to_string()
                        } else {
                            last_message.clone()
                        };
                        pb.finish_with_message(msg);
                        return Ok(());
                    }
                    _ => {}
                }
            }
            Ok(None) => break,
            Err(status) => {
                pb.finish_and_clear();
                return Err(status.into());
            }
        }
    }

    pb.finish_with_message(last_message);
    Ok(())
}

async fn delete_vm(
    client: &mut VmdServiceClient<Channel>,
    vm_id: &str,
    purge_snapshots: bool,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    client
        .delete_vm(request_with_auth(
            DeleteVmRequest {
                vm_id: vm_id.to_string(),
                purge_snapshots,
            },
            auth_header,
        ))
        .await?;
    println!("deleted vm {vm_id}");
    Ok(())
}

async fn fork_vm(
    client: &mut VmdServiceClient<Channel>,
    parent_vm_id: &str,
    child_name: Option<String>,
    child_metadata: Vec<String>,
    auto_start_child: bool,
    json: bool,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    let mut child_metadata_map = HashMap::new();
    for entry in child_metadata {
        if let Some((key, value)) = entry.split_once('=') {
            child_metadata_map.insert(key.to_string(), value.to_string());
        } else {
            bail!("invalid child metadata entry {entry}, expected key=value");
        }
    }

    let response = client
        .fork_vm(request_with_auth(
            ForkVmRequest {
                parent_vm_id: parent_vm_id.to_string(),
                child_name: child_name.unwrap_or_default(),
                child_metadata: if child_metadata_map.is_empty() {
                    None
                } else {
                    Some(Metadata {
                        entries: child_metadata_map,
                    })
                },
                auto_start_child,
            },
            auth_header,
        ))
        .await?
        .into_inner();

    if json {
        let value = json!({
            "fork_id": response.fork_id,
            "parent_vm": response.parent_vm.as_ref().map(vm_to_json),
            "child_vm": response.child_vm.as_ref().map(vm_to_json),
        });
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    println!("fork_id: {}", response.fork_id);
    if let Some(parent) = response.parent_vm {
        println!("parent:");
        print_vm(&parent);
    }
    if let Some(child) = response.child_vm {
        println!("child:");
        print_vm(&child);
    }
    Ok(())
}

enum Action {
    Start,
    Stop,
    Restart,
    Pause,
    Resume,
    ForceStop,
}

async fn vm_action(
    client: &mut VmdServiceClient<Channel>,
    vm_id: &str,
    action: Action,
    json: bool,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    let payload = VmActionRequest {
        vm_id: vm_id.to_string(),
    };

    let response = match action {
        Action::Start => {
            client
                .start_vm(request_with_auth(payload.clone(), auth_header))
                .await?
        }
        Action::Stop => {
            client
                .stop_vm(request_with_auth(payload.clone(), auth_header))
                .await?
        }
        Action::Restart => {
            client
                .restart_vm(request_with_auth(payload.clone(), auth_header))
                .await?
        }
        Action::Pause => {
            client
                .pause_vm(request_with_auth(payload.clone(), auth_header))
                .await?
        }
        Action::Resume => {
            client
                .resume_vm(request_with_auth(payload.clone(), auth_header))
                .await?
        }
        Action::ForceStop => {
            client
                .force_stop_vm(request_with_auth(payload, auth_header))
                .await?
        }
    }
    .into_inner();

    if json {
        println!("{}", serde_json::to_string_pretty(&vm_to_json(&response))?);
    } else {
        print_vm(&response);
    }
    Ok(())
}

async fn list_snapshots(
    client: &mut VmdServiceClient<Channel>,
    vm_id: &str,
    json: bool,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    let response = client
        .list_snapshots(request_with_auth(
            ListSnapshotsRequest {
                vm_id: vm_id.to_string(),
            },
            auth_header,
        ))
        .await?
        .into_inner();
    if json {
        let snaps: Vec<_> = response.snapshots.iter().map(snapshot_to_json).collect();
        println!("{}", serde_json::to_string_pretty(&snaps)?);
    } else {
        for snap in response.snapshots {
            println!("{:<36} {:<20}", snap.id, snap.name);
        }
    }
    Ok(())
}

async fn create_snapshot(
    client: &mut VmdServiceClient<Channel>,
    vm_id: &str,
    label: Option<String>,
    description: Option<String>,
    json: bool,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    let response = client
        .create_snapshot(request_with_auth(
            CreateSnapshotRequest {
                vm_id: vm_id.to_string(),
                label: label.unwrap_or_default(),
                description: description.unwrap_or_default(),
            },
            auth_header,
        ))
        .await?
        .into_inner();
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&snapshot_to_json(&response))?
        );
    } else {
        println!("created snapshot {} ({})", response.id, response.name);
    }
    Ok(())
}

async fn restore_snapshot(
    client: &mut VmdServiceClient<Channel>,
    vm_id: &str,
    snapshot_id: &str,
    json: bool,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    let response = client
        .restore_snapshot(request_with_auth(
            RestoreSnapshotRequest {
                vm_id: vm_id.to_string(),
                snapshot_id: snapshot_id.to_string(),
            },
            auth_header,
        ))
        .await?
        .into_inner();
    if json {
        println!("{}", serde_json::to_string_pretty(&vm_to_json(&response))?);
    } else {
        print_vm(&response);
    }
    Ok(())
}

async fn delete_snapshot(
    client: &mut VmdServiceClient<Channel>,
    vm_id: &str,
    snapshot_id: &str,
    auth_header: Option<&MetadataValue<Ascii>>,
) -> Result<()> {
    client
        .delete_snapshot(request_with_auth(
            DeleteSnapshotRequest {
                vm_id: vm_id.to_string(),
                snapshot_id: snapshot_id.to_string(),
            },
            auth_header,
        ))
        .await?;
    println!("deleted snapshot {snapshot_id}");
    Ok(())
}

fn vm_to_json(vm: &Vm) -> serde_json::Value {
    json!({
        "id": vm.id,
        "name": vm.name,
        "state": ProtoVmState::try_from(vm.state).map(|s| format!("{:?}", s)).unwrap_or_default(),
        "architecture": vm.architecture,
        "created_at": ts_to_string(vm.created_at.as_ref()),
        "updated_at": ts_to_string(vm.updated_at.as_ref()),
        "started_at": ts_to_string(vm.started_at.as_ref()),
        "source": vm.source.as_ref().map(|s| json!({
            "type": format!("{:?}", ProtoVmSourceType::try_from(s.r#type).unwrap_or(ProtoVmSourceType::Unspecified)),
            "reference": s.reference,
        })),
        "resources": vm.resources.as_ref().map(|r| json!({
            "vcpu": r.vcpu,
            "memory_mb": r.memory_mb,
            "disk_gb": r.disk_gb,
        })),
        "network": vm.network.as_ref().map(|n| json!({
            "mac": n.mac,
            "portproxy": n.portproxy_ports.as_ref().map(|p| json!({
                "proxy_port": p.proxy_port,
                "rpc_port": p.rpc_port,
            })),
        })),
        "metadata": vm.metadata,
        "snapshots": vm.snapshots.iter().map(snapshot_to_json).collect::<Vec<_>>(),
    })
}

fn snapshot_to_json(snapshot: &Snapshot) -> serde_json::Value {
    json!({
        "id": snapshot.id,
        "name": snapshot.name,
        "label": snapshot.label,
        "description": snapshot.description,
        "created_at": ts_to_string(snapshot.created_at.as_ref()),
    })
}

fn ts_to_string(ts: Option<&Timestamp>) -> Option<String> {
    ts.and_then(|t| {
        chrono::DateTime::<Utc>::from_timestamp(t.seconds, t.nanos as u32).map(|dt| dt.to_rfc3339())
    })
}

fn print_vm(vm: &Vm) {
    println!("id: {}", vm.id);
    println!("name: {}", vm.name);
    println!(
        "state: {:?}",
        ProtoVmState::try_from(vm.state).unwrap_or(ProtoVmState::Unspecified)
    );
    println!("architecture: {}", vm.architecture);
    if let Some(ts) = ts_to_string(vm.created_at.as_ref()) {
        println!("created_at: {ts}");
    }
    if let Some(ts) = ts_to_string(vm.updated_at.as_ref()) {
        println!("updated_at: {ts}");
    }
    if let Some(ts) = ts_to_string(vm.started_at.as_ref()) {
        println!("started_at: {ts}");
    }
    if let Some(source) = &vm.source {
        println!(
            "source: {:?} ({})",
            ProtoVmSourceType::try_from(source.r#type).unwrap_or(ProtoVmSourceType::Unspecified),
            source.reference
        );
    }
    if let Some(resources) = &vm.resources {
        println!(
            "resources: vcpu={} memory_mb={} disk_gb={}",
            resources.vcpu, resources.memory_mb, resources.disk_gb
        );
    }
    if let Some(network) = &vm.network {
        if let Some(ports) = &network.portproxy_ports {
            println!(
                "network: mac={} proxy_port={} rpc_port={}",
                network.mac, ports.proxy_port, ports.rpc_port
            );
        } else {
            println!("network: mac={}", network.mac);
        }
    }
    if !vm.metadata.is_empty() {
        println!("metadata:");
        for (k, v) in &vm.metadata {
            println!("  {k}: {v}");
        }
    }
    if !vm.snapshots.is_empty() {
        println!("snapshots:");
        for snap in &vm.snapshots {
            println!(
                "  {} {} {}",
                snap.id,
                snap.name,
                ts_to_string(snap.created_at.as_ref()).unwrap_or_default()
            );
        }
    }
}

async fn convert_image(
    image: String,
    arch: String,
    output: Option<String>,
    data_dir: Option<String>,
    docker_bin: Option<String>,
    force_local_build: bool,
) -> Result<()> {
    if image.trim().is_empty() {
        bail!("image reference is required");
    }
    let defaults = Config::default();
    let data_dir = data_dir.unwrap_or_else(|| defaults.data_dir.clone());
    let docker = docker_bin.unwrap_or_else(|| defaults.docker_bin.clone());

    let normalized_arch = normalize_arch(&arch)?;

    let (dir, final_path) = resolve_output_paths(&data_dir, output, &image, &normalized_arch)?;
    if final_path.exists() {
        bail!(
            "output {} already exists; delete it or choose a different --output",
            final_path.display()
        );
    }

    if !force_local_build {
        if image::fetch_prebuilt_image(&image, &normalized_arch, final_path.as_path()).await? {
            println!("{}", final_path.display());
            return Ok(());
        }
    }

    ensure_platform_supported(&docker, &image, &normalized_arch).await?;

    let tmp_name = temp_file_name(&final_path);
    let tmp_path = dir.join(&tmp_name);
    let opts = virt::D2VmOptions {
        image: image.clone(),
        output: tmp_name.clone(),
        disk_gb: BASE_IMAGE_SIZE_GB,
        pull: true,
        platform: Some(format!("linux/{normalized_arch}")),
        include_bootstrap: true,
    };
    if let Err(err) = virt::run_d2vm(&docker, &dir, opts).await {
        if tmp_path.exists() {
            let _ = fs::remove_file(&tmp_path);
        }
        return Err(err);
    }

    fs::rename(&tmp_path, &final_path)
        .with_context(|| format!("move converted image into {}", final_path.display()))?;
    println!("{}", final_path.display());
    Ok(())
}

fn resolve_output_paths(
    data_dir: &str,
    output: Option<String>,
    image: &str,
    arch: &str,
) -> Result<(PathBuf, PathBuf)> {
    if let Some(path) = output {
        let candidate = expand_path(&path)?;
        let dir = candidate
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| anyhow::anyhow!("output path must include a file name"))?;
        fs::create_dir_all(&dir)
            .with_context(|| format!("create output directory {}", dir.display()))?;
        Ok((dir, candidate))
    } else {
        let resolved = expand_path(data_dir)?;
        fs::create_dir_all(&resolved)
            .with_context(|| format!("create data directory {}", resolved.display()))?;
        let base_dir = resolved.join(config::BASE_IMAGES_DIR_NAME);
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("create base image directory {}", base_dir.display()))?;
        let target = base_dir.join(image::base_image_file_name(image, arch));
        Ok((base_dir, target))
    }
}

fn expand_path(path: &str) -> Result<PathBuf> {
    if path.is_empty() {
        bail!("path must not be empty");
    }
    let expanded = if path.starts_with("~/") {
        let home = dirs::home_dir().context("resolve home directory")?;
        home.join(path.trim_start_matches("~/"))
    } else if path == "~" {
        dirs::home_dir().context("resolve home directory")?
    } else {
        let candidate = PathBuf::from(path);
        if candidate.is_absolute() {
            candidate
        } else {
            env::current_dir()?.join(candidate)
        }
    };
    Ok(expanded)
}

async fn ensure_platform_supported(docker: &str, image: &str, arch: &str) -> Result<()> {
    let platforms = virt::inspect_image_platforms(docker, image).await?;
    let supported = platforms.iter().any(|p| {
        p.os.to_lowercase() == "linux"
            && normalize_arch(&p.arch)
                .map(|candidate| candidate == arch)
                .unwrap_or(false)
    });
    if !supported {
        bail!("image {image} does not provide linux/{arch}");
    }
    Ok(())
}

fn normalize_arch(value: &str) -> Result<String> {
    match value.trim().to_lowercase().as_str() {
        "amd64" | "x86_64" => Ok("amd64".to_string()),
        "arm64" | "aarch64" => Ok("arm64".to_string()),
        other => bail!("unsupported architecture: {other}"),
    }
}

fn temp_file_name(target: &Path) -> String {
    let extension = target
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or_else(|| BASE_IMAGE_EXT.trim_start_matches('.'));
    let stem = target
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("disk");
    format!("{stem}-{}.{}", Uuid::new_v4(), extension)
}

fn phase_label(phase: CreateVmPhase) -> &'static str {
    match phase {
        CreateVmPhase::DownloadingImage => "Downloading",
        CreateVmPhase::ConvertingImage => "Converting",
        CreateVmPhase::StartingVm => "Starting VM",
        CreateVmPhase::Complete => "Complete",
        CreateVmPhase::Unspecified => "Working",
    }
}
