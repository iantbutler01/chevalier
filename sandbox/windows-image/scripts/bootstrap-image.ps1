$ErrorActionPreference = "Stop"

$sourceVolume = Get-Volume -FileSystemLabel "OPENBRACKET"
if (-not $sourceVolume -or -not $sourceVolume.DriveLetter) {
    throw "The pinned OpenBracket answer media is unavailable"
}
$source = "$($sourceVolume.DriveLetter):\"
$artifactRoot = "C:\Windows\Temp\OpenBracketImage"
$stateRoot = "C:\ProgramData\Chevalier"
New-Item -ItemType Directory -Force -Path $artifactRoot, $stateRoot | Out-Null

foreach ($receiptFile in "build-receipt-token.txt", "build-receipt-url.txt") {
    $receiptSource = Join-Path $source $receiptFile
    if (-not (Test-Path $receiptSource)) {
        throw "Pinned answer media is missing $receiptFile"
    }
    Copy-Item -Path $receiptSource -Destination (Join-Path $stateRoot $receiptFile) -Force
}

$files = @(
    "winfsp-2.2.26215.msi",
    "virtio-win-guest-tools-0.1.285.exe",
    "PowerShell-7.6.5-win-arm64.msi",
    "vc_redist.arm64-14.51.36247.exe",
    "ripgrep-15.2.0-aarch64-pc-windows-msvc.zip",
    "Git-2.55.0.4-64-bit.exe",
    "chevalier-vfs-winfsp-arm64.exe",
    "chevalier-guest-agent-arm64.exe",
    "chevalier-guest-services.SHA256SUMS",
    "initialize-state.ps1",
    "install-runtime-services.ps1",
    "provision.ps1",
    "verify.ps1",
    "install-seal-scripts.ps1",
    "finalize-image.ps1",
    "complete-image.ps1"
)
foreach ($file in $files) {
    $sourcePath = Join-Path $source $file
    if (-not (Test-Path $sourcePath)) {
        throw "Pinned answer media is missing $file"
    }
    Copy-Item -Path $sourcePath -Destination (Join-Path $artifactRoot $file) -Force
}

Start-Transcript -Path (Join-Path $stateRoot "image-build.log") -Append
try {
    & (Join-Path $artifactRoot "provision.ps1")
    Copy-Item -Path (Join-Path $artifactRoot "complete-image.ps1") -Destination $stateRoot -Force

    $buildPassword = [System.IO.File]::ReadAllText((Join-Path $stateRoot "build-receipt-token.txt")).Trim()
    if (-not $buildPassword) {
        throw "The temporary build password is unavailable"
    }

    $winlogon = "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon"
    New-ItemProperty -Path $winlogon -Name AutoAdminLogon -PropertyType String -Value "1" -Force | Out-Null
    New-ItemProperty -Path $winlogon -Name AutoLogonCount -PropertyType DWord -Value 1 -Force | Out-Null
    New-ItemProperty -Path $winlogon -Name DefaultUserName -PropertyType String -Value "Administrator" -Force | Out-Null
    New-ItemProperty -Path $winlogon -Name DefaultDomainName -PropertyType String -Value "OB-IMAGE-BUILD" -Force | Out-Null
    New-ItemProperty -Path $winlogon -Name DefaultPassword -PropertyType String -Value $buildPassword -Force | Out-Null

    $runOnce = "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\RunOnce"
    New-Item -Path $runOnce -Force | Out-Null
    New-ItemProperty -Path $runOnce -Name "!OpenBracketImageComplete" -PropertyType String -Value 'powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\ProgramData\Chevalier\complete-image.ps1"' -Force | Out-Null
} catch {
    $_ | Out-String | Set-Content -Encoding UTF8 -Path (Join-Path $stateRoot "image-build.error.txt")
    try {
        $failureToken = [System.IO.File]::ReadAllText((Join-Path $stateRoot "build-receipt-token.txt")).Trim()
        $failureUrl = [System.IO.File]::ReadAllText((Join-Path $stateRoot "build-receipt-url.txt")).Trim()
        if ($failureToken -and $failureUrl) {
            $failurePayload = @{ status = "failure"; stage = "bootstrap"; message = $_.Exception.Message } | ConvertTo-Json -Compress
            Invoke-WebRequest -UseBasicParsing -Uri $failureUrl -Method Post -Headers @{ Authorization = "Bearer $failureToken" } -ContentType "application/json" -Body $failurePayload -TimeoutSec 15 | Out-Null
        }
    } catch {
    }
    & shutdown.exe /s /t 0 /f
    throw
} finally {
    Stop-Transcript
}
