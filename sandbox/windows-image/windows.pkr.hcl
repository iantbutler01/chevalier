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
  default = "https://aka.ms/Win11E-ISO-25H2-en-us"
}

variable "windows_iso_checksum" {
  type    = string
  default = "sha256:a61adeab895ef5a4db436e0a7011c92a2ff17bb0357f58b13bbc4062e535e7b9"
}

variable "build_password" {
  type      = string
  sensitive = true
}

variable "accelerator" {
  type    = string
  default = "kvm"
}

variable "cpu_model" {
  type    = string
  default = "host"
}

variable "qemu_binary" {
  type    = string
  default = "qemu-system-x86_64"
}

variable "machine_type" {
  type    = string
  default = "q35,smm=on"
}

variable "boot_wait" {
  type    = string
  default = "5s"
}

variable "efi_firmware_code" {
  type    = string
  default = "/usr/share/OVMF/OVMF_CODE_4M.ms.fd"
}

variable "efi_firmware_vars" {
  type    = string
  default = "/usr/share/OVMF/OVMF_VARS_4M.ms.fd"
}

variable "output_directory" {
  type    = string
  default = "output/windows-11-enterprise-25h2-x64"
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
  default = "/tmp/openbracket-win11-qmp.sock"
}

locals {
  answer_file = templatefile("${path.root}/Autounattend.xml.pkrtpl", {
    build_password = var.build_password
  })
}

source "qemu" "windows_11_enterprise" {
  iso_url      = var.windows_iso_url
  iso_checksum = var.windows_iso_checksum

  output_directory   = abspath(var.output_directory)
  vm_name            = "windows-11-enterprise-25h2-x64.qcow2"
  format             = "qcow2"
  disk_size          = "64G"
  disk_interface     = "virtio"
  disk_cache         = "writeback"
  disk_discard       = "unmap"
  disk_detect_zeroes = "unmap"
  skip_compaction    = var.skip_compaction

  qemu_binary  = var.qemu_binary
  accelerator  = var.accelerator
  machine_type = var.machine_type
  cpu_model    = var.cpu_model
  cpus         = 4
  memory       = 8192

  efi_boot          = true
  efi_firmware_code = var.efi_firmware_code
  efi_firmware_vars = var.efi_firmware_vars
  efi_drop_efivars  = false
  vtpm              = true
  tpm_device_type   = "tpm-tis"

  net_device       = "e1000"
  cdrom_interface  = "ide"
  headless         = true
  vnc_bind_address = "127.0.0.1"
  qemuargs = [
    ["-netdev", "user,id=user.0,hostfwd=tcp:127.0.0.1:{{ .SSHHostPort }}-:5985"],
  ]
  boot_wait = var.boot_wait
  boot_command = [
    "<spacebar><wait2><spacebar><wait2><spacebar><wait2><spacebar><wait2><spacebar><wait2><spacebar><wait2><spacebar><wait2><spacebar><wait2><spacebar><wait2><spacebar><wait2><spacebar><wait2><spacebar>",
  ]

  cd_label = "OPENBRACKET"
  cd_content = {
    "Autounattend.xml" = local.answer_file
  }
  cd_files = [
    "${path.root}/scripts/enable-winrm.ps1",
    "${path.root}/${var.artifact_directory}/answer-cd/*",
  ]

  communicator   = "winrm"
  winrm_username = "Administrator"
  winrm_password = var.build_password
  winrm_use_ntlm = false
  winrm_timeout  = "8h"

  shutdown_command = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\\ProgramData\\Chevalier\\schedule-seal.ps1"
  shutdown_timeout = "30m"

  qmp_enable      = true
  qmp_socket_path = var.qmp_socket_path
}

build {
  sources = ["source.qemu.windows_11_enterprise"]

  provisioner "powershell" {
    inline = [
      "New-Item -ItemType Directory -Force -Path C:\\Windows\\Temp\\OpenBracketImage | Out-Null",
      "New-Item -ItemType Directory -Force -Path C:\\ProgramData\\Chevalier | Out-Null",
    ]
  }

  provisioner "file" {
    source      = "${path.root}/${var.artifact_directory}/winfsp-2.2.26194.msi"
    destination = "C:\\Windows\\Temp\\OpenBracketImage\\winfsp-2.2.26194.msi"
  }

  provisioner "file" {
    source      = "${path.root}/${var.artifact_directory}/virtio-win-guest-tools-0.1.285.exe"
    destination = "C:\\Windows\\Temp\\OpenBracketImage\\virtio-win-guest-tools-0.1.285.exe"
  }

  provisioner "file" {
    source      = "${path.root}/scripts/schedule-seal.ps1"
    destination = "C:\\Windows\\Temp\\OpenBracketImage\\schedule-seal.ps1"
  }

  provisioner "file" {
    source      = "${path.root}/scripts/finalize-image.ps1"
    destination = "C:\\Windows\\Temp\\OpenBracketImage\\finalize-image.ps1"
  }

  provisioner "powershell" {
    scripts = [
      "${path.root}/scripts/provision.ps1",
    ]
  }

  provisioner "windows-restart" {
    restart_timeout = "30m"
  }

  provisioner "powershell" {
    scripts = [
      "${path.root}/scripts/verify.ps1",
      "${path.root}/scripts/install-seal-scripts.ps1",
    ]
  }

  post-processor "manifest" {
    output = "${path.root}/${var.output_directory}/packer-manifest.json"
    custom_data = {
      architecture        = "x86_64"
      guest_platform      = "windows"
      profile             = "windows-11-enterprise-25h2-x64-v1"
      workspace_transport = "virtio-fs"
      windows_iso_sha256  = trimprefix(var.windows_iso_checksum, "sha256:")
      virtio_win_version  = "0.1.285"
      winfsp_version      = "2.2.26194"
    }
  }
}
