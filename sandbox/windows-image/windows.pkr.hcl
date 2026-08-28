packer {
  required_plugins {
    qemu = {
      source  = "github.com/hashicorp/qemu"
      version = "= 1.1.5"
    }
  }
}

variable "windows_iso_url" {
  type    = string
  default = "https://software-static.download.prss.microsoft.com/dbazure/998969d5-f34g-4e03-ac9d-1f9786c66749/26100.1742.240906-0331.ge_release_svc_refresh_CLIENT_IOT_LTSC_EVAL_A64FRE_en-us.iso"
}

variable "windows_iso_checksum" {
  type    = string
  default = "sha256:3dcdba9c9c0aa0430d4332b60c9afcb3cd613d648a49cbba2d4ef7b5978f32e8"
}

variable "build_password" {
  type      = string
  sensitive = true
}

variable "accelerator" {
  type    = string
  default = "hvf"
}

variable "cpu_model" {
  type    = string
  default = "host"
}

variable "qemu_binary" {
  type    = string
  default = "scripts/qemu-arm-hvf-wrapper.sh"
}

variable "machine_type" {
  type    = string
  default = "virt-10.2,highmem=off"
}

variable "boot_wait" {
  type    = string
  default = "2s"
}

variable "efi_firmware_code" {
  type    = string
  default = "artifacts/edk2-aarch64-secure-code.fd"
}

variable "efi_firmware_vars" {
  type    = string
  default = "artifacts/edk2-arm-secure-vars-64m.fd"
}

variable "output_directory" {
  type    = string
  default = "output/windows-11-iot-enterprise-ltsc-2024-arm64"
}

variable "skip_compaction" {
  type    = bool
  default = false
}

variable "artifact_directory" {
  type    = string
  default = "artifacts"
}

variable "qmp_socket_path" {
  type    = string
  default = "/tmp/openbracket-win11-arm64-qmp.sock"
}

variable "receipt_port" {
  type    = number
  default = 18080
}

locals {
  answer_file = templatefile("${path.root}/Autounattend.xml.pkrtpl", {
    build_password = var.build_password
  })
}

source "qemu" "windows_11_arm64" {
  iso_url      = var.windows_iso_url
  iso_checksum = var.windows_iso_checksum

  output_directory   = abspath(var.output_directory)
  vm_name            = "windows-11-iot-enterprise-ltsc-2024-arm64.qcow2"
  format             = "qcow2"
  disk_size          = "64G"
  disk_interface     = "virtio"
  disk_cache         = "writeback"
  disk_discard       = "unmap"
  disk_detect_zeroes = "unmap"
  skip_compaction    = var.skip_compaction

  qemu_binary  = abspath(var.qemu_binary)
  accelerator  = var.accelerator
  machine_type = var.machine_type
  cpu_model    = var.cpu_model
  cpus         = 4
  memory       = 3072

  efi_boot          = true
  efi_firmware_code = abspath(var.efi_firmware_code)
  efi_firmware_vars = abspath(var.efi_firmware_vars)
  efi_drop_efivars  = false

  net_device       = "virtio-net"
  cdrom_interface  = "ide"
  headless         = true
  vnc_bind_address = "127.0.0.1"
  qemuargs = [
    ["-netdev", "user,id=user.0"],
    ["-boot", "menu=on"],
  ]
  boot_wait = var.boot_wait
  boot_command = [
    "<spacebar><wait1><spacebar><wait1><spacebar><wait1><spacebar><wait1><spacebar><wait1><spacebar><wait1><spacebar><wait1><spacebar><wait1><spacebar><wait1><spacebar>",
  ]

  cd_label = "OPENBRACKET"
  cd_content = {
    "Autounattend.xml"        = local.answer_file
    "build-receipt-token.txt" = var.build_password
    "build-receipt-url.txt"   = "http://10.0.2.2:${var.receipt_port}/receipt"
  }
  cd_files = [
    "${path.root}/scripts/bootstrap-image.ps1",
    "${path.root}/scripts/complete-image.ps1",
    "${path.root}/scripts/provision.ps1",
    "${path.root}/scripts/initialize-state.ps1",
    "${path.root}/scripts/install-runtime-services.ps1",
    "${path.root}/scripts/verify.ps1",
    "${path.root}/scripts/install-seal-scripts.ps1",
    "${path.root}/scripts/finalize-image.ps1",
    "${path.root}/${var.artifact_directory}/winfsp-2.2.26215.msi",
    "${path.root}/${var.artifact_directory}/virtio-win-guest-tools-0.1.285.exe",
    "${path.root}/${var.artifact_directory}/PowerShell-7.6.5-win-arm64.msi",
    "${path.root}/${var.artifact_directory}/vc_redist.arm64-14.51.36247.exe",
    "${path.root}/${var.artifact_directory}/ripgrep-15.2.0-aarch64-pc-windows-msvc.zip",
    "${path.root}/${var.artifact_directory}/Git-2.55.0.4-64-bit.exe",
    "${path.root}/${var.artifact_directory}/chevalier-vfs-winfsp-arm64.exe",
    "${path.root}/${var.artifact_directory}/chevalier-guest-agent-arm64.exe",
    "${path.root}/${var.artifact_directory}/chevalier-guest-services.SHA256SUMS",
    "${path.root}/${var.artifact_directory}/viogpudo/*",
    "${path.root}/${var.artifact_directory}/answer-cd/*",
  ]

  communicator     = "none"
  shutdown_timeout = "30m"

  qmp_enable      = true
  qmp_socket_path = var.qmp_socket_path
}

build {
  sources = ["source.qemu.windows_11_arm64"]

  provisioner "shell-local" {
    environment_vars = [
      "OPENBRACKET_QEMU_IMAGE=${abspath(var.output_directory)}/windows-11-iot-enterprise-ltsc-2024-arm64.qcow2",
      "OPENBRACKET_RECEIPT_PORT=${var.receipt_port}",
      "OPENBRACKET_RECEIPT_TOKEN=${var.build_password}",
      "OPENBRACKET_WAIT_TIMEOUT_SECONDS=7200",
    ]
    script = "${path.root}/scripts/wait-for-guest-shutdown.sh"
  }

  post-processor "manifest" {
    output = "${path.root}/${var.output_directory}/packer-manifest.json"
    custom_data = {
      architecture        = "arm64"
      guest_platform      = "windows"
      profile             = "windows-11-iot-enterprise-ltsc-2024-arm64-guest-winfsp-v2"
      workspace_transport = "guest-winfsp"
      windows_iso_sha256  = trimprefix(var.windows_iso_checksum, "sha256:")
      virtio_win_version  = "0.1.285"
      winfsp_version      = "2.2.26215"
      powershell_version  = "7.6.5"
      visual_cpp_runtime  = "14.51.36247.0"
      ripgrep_version     = "15.2.0"
      git_version         = "2.55.0.windows.4"
    }
  }
}
