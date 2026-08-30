$ErrorActionPreference = "Stop"

$architecture = switch ($env:PROCESSOR_ARCHITECTURE) {
    "ARM64" { "arm64" }
    "AMD64" { "amd64" }
    default { throw "Unsupported Windows verification architecture: $env:PROCESSOR_ARCHITECTURE" }
}
$winFspSuffix = if ($architecture -eq "arm64") { "a64" } else { "x64" }
if (-not (Get-CimInstance Win32_ComputerSystem).HypervisorPresent) {
    throw "Windows does not report a hypervisor"
}
$virtioFsService = Get-Service -Name VirtioFsSvc -ErrorAction SilentlyContinue
if ($virtioFsService -and $virtioFsService.StartType -ne "Disabled") {
    throw "VirtioFsSvc must remain disabled for the guest-native VFS profile"
}
if (-not (Test-Path "C:\Program Files (x86)\WinFsp\bin\fsptool-$winFspSuffix.exe")) {
    throw "WinFsp $architecture runtime is not installed"
}
$winFspDriver = "C:\Program Files (x86)\WinFsp\bin\winfsp-$winFspSuffix.dll"
if ((Get-AuthenticodeSignature -FilePath $winFspDriver).Status -ne "Valid") {
    throw "WinFsp $architecture runtime signature is invalid"
}
if ((Get-BitLockerVolume -MountPoint "C:").ProtectionStatus -ne "Off") {
    throw "BitLocker must remain disabled in the generalized image"
}
if (-not (Test-Path "C:\ProgramData\Chevalier\image-manifest.json")) {
    throw "The image manifest is missing"
}
if ($architecture -eq "arm64") {
    $displayDriver = Get-WindowsDriver -Online | Where-Object {
        $_.ProviderName -eq "Red Hat, Inc." -and $_.ClassName -eq "Display"
    }
    if (-not $displayDriver) {
        throw "The ARM64 VirtIO GPU display driver is not staged"
    }
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
    throw "Native $architecture PowerShell is unavailable"
}
& "C:\Program Files\OpenBracket\bin\rg.exe" --version | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Native $architecture ripgrep is unavailable"
}

$memfs = "C:\Program Files (x86)\WinFsp\bin\memfs-$winFspSuffix.exe"
if (-not (Test-Path $memfs)) {
    throw "WinFsp $architecture MEMFS probe is unavailable"
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
        throw "WinFsp $architecture MEMFS did not mount at W:"
    }
    $probe = "W:\openbracket-winfsp-$architecture.txt"
    [System.IO.File]::WriteAllText($probe, "native-$architecture-winfsp")
    if ([System.IO.File]::ReadAllText($probe) -ne "native-$architecture-winfsp") {
        throw "WinFsp $architecture MEMFS readback failed"
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
