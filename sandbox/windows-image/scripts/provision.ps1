$ErrorActionPreference = "Stop"

$artifactRoot = "C:\Windows\Temp\OpenBracketImage"
$winFsp = Join-Path $artifactRoot "winfsp-2.2.26215.msi"
$virtioTools = Join-Path $artifactRoot "virtio-win-guest-tools-0.1.285.exe"
$powerShell = Join-Path $artifactRoot "PowerShell-7.6.5-win-arm64.msi"
$visualCppRuntime = Join-Path $artifactRoot "vc_redist.arm64-14.51.36247.exe"
$ripgrep = Join-Path $artifactRoot "ripgrep-15.2.0-aarch64-pc-windows-msvc.zip"
$git = Join-Path $artifactRoot "Git-2.55.0.4-64-bit.exe"
$installServices = Join-Path $artifactRoot "install-runtime-services.ps1"

function Assert-Sha256 {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Expected
    )

    $actual = (Get-FileHash -Algorithm SHA256 -Path $Path).Hash
    if ($actual -ne $Expected) {
        throw "SHA-256 mismatch for $Path; expected $Expected, got $actual"
    }
}

function Invoke-Installer {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string]$ArgumentList
    )

    $process = Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -Wait -PassThru
    if ($process.ExitCode -notin 0, 3010) {
        throw "$FilePath exited with $($process.ExitCode)"
    }
}

Assert-Sha256 -Path $winFsp -Expected "2ECB5C89405488A95BBD8A01875E02C48534FD37BBDFD84488F7590464D65944"
Assert-Sha256 -Path $virtioTools -Expected "C8B4A9FE87E1FC5D8E843495E082DEA53420587FE04740B1084D85089343F04D"
Assert-Sha256 -Path $powerShell -Expected "A1633B48B8E45C7767902EFC972BFF235B3594C192AD575AF4A4E8EB2EA3BD5A"
Assert-Sha256 -Path $visualCppRuntime -Expected "B70EF586669A620A0A30A1156969C05C6A3831DC8F8BC992DA75779D2A92F944"
Assert-Sha256 -Path $ripgrep -Expected "E4ABCA10C3A64EBEA742667DD7009449D49403DB5460DD6873E389FA2945360F"
Assert-Sha256 -Path $git -Expected "0CBC0B34A74B3AFF3ACE0910328549155A770E228331B19CB1498218A120E7FF"

Invoke-Installer -FilePath msiexec.exe -ArgumentList "/i `"$winFsp`" /qn /norestart INSTALLLEVEL=1000"
Invoke-Installer -FilePath $virtioTools -ArgumentList "/quiet /norestart"
Invoke-Installer -FilePath msiexec.exe -ArgumentList "/i `"$powerShell`" /qn /norestart ADD_PATH=1"
Invoke-Installer -FilePath $visualCppRuntime -ArgumentList "/install /quiet /norestart"
Invoke-Installer -FilePath $git -ArgumentList "/VERYSILENT /NORESTART /NOCANCEL /SP-"

$toolRoot = "C:\Program Files\OpenBracket\bin"
New-Item -ItemType Directory -Force -Path $toolRoot | Out-Null
$ripgrepRoot = Join-Path $artifactRoot "ripgrep"
Expand-Archive -Path $ripgrep -DestinationPath $ripgrepRoot -Force
$ripgrepExecutable = Get-ChildItem -Path $ripgrepRoot -Filter rg.exe -Recurse | Select-Object -First 1
if (-not $ripgrepExecutable) {
    throw "The ripgrep ARM64 archive did not contain rg.exe"
}
Copy-Item -Path $ripgrepExecutable.FullName -Destination (Join-Path $toolRoot "rg.exe") -Force

& $installServices -ArtifactRoot $artifactRoot

$toolPaths = @($toolRoot, "C:\Program Files\Git\cmd", "C:\Program Files\PowerShell\7")
$machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
foreach ($toolPath in $toolPaths) {
    if (($machinePath -split ';') -notcontains $toolPath) {
        $machinePath = "$machinePath;$toolPath"
    }
}
[Environment]::SetEnvironmentVariable("Path", $machinePath, "Machine")
$env:Path = "$machinePath;$env:Path"

& "C:\Program Files\Git\cmd\git.exe" config --system core.autocrlf false
& "C:\Program Files\Git\cmd\git.exe" config --system core.longpaths true
& "C:\Program Files\Git\cmd\git.exe" config --system core.ignorecase true

if (Get-Service -Name VirtioFsSvc -ErrorAction SilentlyContinue) {
    Stop-Service -Name VirtioFsSvc -Force -ErrorAction SilentlyContinue
    Set-Service -Name VirtioFsSvc -StartupType Disabled
}
if (Get-Service -Name QEMU-GA -ErrorAction SilentlyContinue) {
    Set-Service -Name QEMU-GA -StartupType Automatic
}

New-Item -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Force | Out-Null
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name LongPathsEnabled -PropertyType DWord -Value 1 -Force | Out-Null
New-Item -Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\AppModelUnlock" -Force | Out-Null
New-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\AppModelUnlock" -Name AllowDevelopmentWithoutDevLicense -PropertyType DWord -Value 1 -Force | Out-Null
$bitLockerKey = [Microsoft.Win32.Registry]::LocalMachine.CreateSubKey("SYSTEM\CurrentControlSet\Control\BitLocker")
$bitLockerKey.Dispose()
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\BitLocker" -Name PreventDeviceEncryption -PropertyType DWord -Value 1 -Force | Out-Null
New-Item -Path "HKLM:\SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU" -Force | Out-Null
New-ItemProperty -Path "HKLM:\SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU" -Name NoAutoRebootWithLoggedOnUsers -PropertyType DWord -Value 1 -Force | Out-Null
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Power" -Name HiberbootEnabled -PropertyType DWord -Value 0 -Force | Out-Null
powercfg.exe /hibernate off

$bitLocker = Get-BitLockerVolume -MountPoint "C:" -ErrorAction SilentlyContinue
if ($bitLocker -and $bitLocker.ProtectionStatus -ne "Off") {
    Disable-BitLocker -MountPoint "C:"
}

$manifest = [ordered]@{
    schemaVersion        = 1
    profile              = "windows-11-iot-enterprise-ltsc-2024-arm64-guest-winfsp-v2"
    platform             = "windows"
    architecture         = "arm64"
    workspaceTransport   = "guest-winfsp"
    workspaceMount       = "W:"
    virtioWinVersion     = "0.1.285"
    winFspVersion        = "2.2.26215"
    powerShellVersion    = "7.6.5"
    visualCppRuntime     = "14.51.36247.0"
    ripgrepVersion       = "15.2.0"
    gitVersion           = "2.55.0.windows.4"
    windowsIsoSha256     = "3DCDBA9C9C0AA0430D4332B60C9AFCB3CD613D648A49CBBA2D4EF7B5978F32E8"
}
$manifest | ConvertTo-Json | Set-Content -Encoding UTF8 -Path "C:\ProgramData\Chevalier\image-manifest.json"
