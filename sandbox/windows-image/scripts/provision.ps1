$ErrorActionPreference = "Stop"

$artifactRoot = "C:\Windows\Temp\OpenBracketImage"
$winFsp = Join-Path $artifactRoot "winfsp-2.2.26194.msi"
$virtioTools = Join-Path $artifactRoot "virtio-win-guest-tools-0.1.285.exe"

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

Assert-Sha256 -Path $winFsp -Expected "7B41020618CDCC33D699D0E15C1DF660F0762A09B57080049C565857AC00BD9D"
Assert-Sha256 -Path $virtioTools -Expected "C8B4A9FE87E1FC5D8E843495E082DEA53420587FE04740B1084D85089343F04D"

Invoke-Installer -FilePath msiexec.exe -ArgumentList "/i `"$winFsp`" /qn /norestart INSTALLLEVEL=1000"
Invoke-Installer -FilePath $virtioTools -ArgumentList "/quiet /norestart"

New-Item -Path "HKLM:\SOFTWARE\virtiofs" -Force | Out-Null
New-ItemProperty -Path "HKLM:\SOFTWARE\virtiofs" -Name MountPoint -PropertyType String -Value "W:" -Force | Out-Null
New-ItemProperty -Path "HKLM:\SOFTWARE\virtiofs" -Name CaseInsensitive -PropertyType DWord -Value 1 -Force | Out-Null
New-ItemProperty -Path "HKLM:\SOFTWARE\virtiofs" -Name FileSystemName -PropertyType String -Value "NTFS" -Force | Out-Null
New-ItemProperty -Path "HKLM:\SOFTWARE\virtiofs" -Name Owner -PropertyType String -Value "0:0" -Force | Out-Null

Set-Service -Name VirtioFsSvc -StartupType Automatic
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
    profile              = "windows-11-enterprise-25h2-x64-v1"
    platform             = "windows"
    architecture         = "x86_64"
    workspaceTransport   = "virtio-fs"
    workspaceMount       = "W:"
    virtioWinVersion     = "0.1.285"
    winFspVersion        = "2.2.26194"
    windowsIsoSha256     = "A61ADEAB895EF5A4DB436E0A7011C92A2FF17BB0357F58B13BBC4062E535E7B9"
}
$manifest | ConvertTo-Json | Set-Content -Encoding UTF8 -Path "C:\ProgramData\Chevalier\image-manifest.json"
