$ErrorActionPreference = "Stop"

if ((Get-CimInstance Win32_OperatingSystem).OSArchitecture -ne "64-bit") {
    throw "The image is not x86_64 Windows"
}
if (-not (Get-CimInstance Win32_ComputerSystem).HypervisorPresent) {
    throw "Windows does not report a hypervisor"
}
if ((Get-Tpm).TpmPresent -ne $true) {
    throw "TPM 2.0 is unavailable"
}
if (-not (Get-Service -Name VirtioFsSvc -ErrorAction SilentlyContinue)) {
    throw "VirtioFsSvc is not installed"
}
if (-not (Test-Path "C:\Program Files (x86)\WinFsp\bin\fsptool-x64.exe")) {
    throw "WinFsp x64 runtime is not installed"
}
if ((Get-ItemProperty -Path "HKLM:\SOFTWARE\virtiofs" -Name MountPoint).MountPoint -ne "W:") {
    throw "VirtioFsSvc is not pinned to W:"
}
if ((Get-BitLockerVolume -MountPoint "C:").ProtectionStatus -ne "Off") {
    throw "BitLocker must be disabled before the build TPM is discarded"
}
if (-not (Test-Path "C:\ProgramData\Chevalier\image-manifest.json")) {
    throw "The image manifest is missing"
}

$pendingRebootPaths = @(
    "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing\RebootPending",
    "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update\RebootRequired"
)
foreach ($path in $pendingRebootPaths) {
    if (Test-Path $path) {
        throw "Pending reboot marker remains at $path"
    }
}
