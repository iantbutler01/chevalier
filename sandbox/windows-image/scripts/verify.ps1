$ErrorActionPreference = "Stop"

if ($env:PROCESSOR_ARCHITECTURE -ne "ARM64") {
    throw "The image is not native ARM64 Windows"
}
if (-not (Get-CimInstance Win32_ComputerSystem).HypervisorPresent) {
    throw "Windows does not report a hypervisor"
}
$virtioFsService = Get-Service -Name VirtioFsSvc -ErrorAction SilentlyContinue
if ($virtioFsService -and $virtioFsService.StartType -ne "Disabled") {
    throw "VirtioFsSvc must remain disabled for the guest-native VFS profile"
}
if (-not (Test-Path "C:\Program Files (x86)\WinFsp\bin\fsptool-a64.exe")) {
    throw "WinFsp ARM64 runtime is not installed"
}
$winFspDriver = "C:\Program Files (x86)\WinFsp\bin\winfsp-a64.dll"
if ((Get-AuthenticodeSignature -FilePath $winFspDriver).Status -ne "Valid") {
    throw "WinFsp ARM64 runtime signature is invalid"
}
if ((Get-BitLockerVolume -MountPoint "C:").ProtectionStatus -ne "Off") {
    throw "BitLocker must remain disabled in the generalized image"
}
if (-not (Test-Path "C:\ProgramData\Chevalier\image-manifest.json")) {
    throw "The image manifest is missing"
}
foreach ($serviceName in "ChevalierVFS", "ChevalierGuest") {
    if (-not (Get-Service -Name $serviceName -ErrorAction SilentlyContinue)) {
        throw "$serviceName is not installed"
    }
}
if ((Get-Service -Name "ChevalierVFS").StartType -ne "Manual") {
    throw "ChevalierVFS must wait for per-VM runtime configuration"
}
if ((Get-Service -Name "ChevalierGuest").StartType -ne "Automatic") {
    throw "ChevalierGuest must start automatically"
}
foreach ($path in @(
    "C:\Program Files\Chevalier\chevalier-vfs-winfsp.exe",
    "C:\Program Files\Chevalier\chevalier-guest-agent.exe",
    "C:\Program Files\Chevalier\initialize-state.ps1"
)) {
    if (-not (Test-Path $path)) {
        throw "Guest runtime artifact is missing: $path"
    }
}
foreach ($secretPath in @(
    "C:\ProgramData\Chevalier\runtime\runtime.json",
    "C:\ProgramData\Chevalier\runtime\control.token",
    "C:\ProgramData\Chevalier\runtime\vfs.token"
)) {
    if (Test-Path $secretPath) {
        throw "Per-VM runtime material was baked into the image: $secretPath"
    }
}

& "C:\Program Files\Git\cmd\git.exe" --version | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Git for Windows is unavailable"
}
& "C:\Program Files\PowerShell\7\pwsh.exe" -NoLogo -NoProfile -Command '$PSVersionTable.PSVersion.ToString()' | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Native ARM64 PowerShell is unavailable"
}
& "C:\Program Files\OpenBracket\bin\rg.exe" --version | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Native ARM64 ripgrep is unavailable"
}

$memfs = "C:\Program Files (x86)\WinFsp\bin\memfs-a64.exe"
if (-not (Test-Path $memfs)) {
    throw "WinFsp ARM64 MEMFS probe is unavailable"
}
$memfsProcess = Start-Process -FilePath $memfs -ArgumentList "-i", "-F", "NTFS", "-m", "W:" -PassThru
try {
    $mounted = $false
    foreach ($attempt in 1..100) {
        if (Test-Path "W:\") {
            $mounted = $true
            break
        }
        Start-Sleep -Milliseconds 100
    }
    if (-not $mounted) {
        throw "WinFsp ARM64 MEMFS did not mount at W:"
    }
    $probe = "W:\openbracket-winfsp-arm64.txt"
    [System.IO.File]::WriteAllText($probe, "native-arm64-winfsp")
    if ([System.IO.File]::ReadAllText($probe) -ne "native-arm64-winfsp") {
        throw "WinFsp ARM64 MEMFS readback failed"
    }
    Remove-Item -Force $probe
} finally {
    Stop-Process -Id $memfsProcess.Id -Force -ErrorAction SilentlyContinue
    $memfsProcess.WaitForExit(10000) | Out-Null
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
