$ErrorActionPreference = "Stop"

$tokenPath = "C:\ProgramData\Chevalier\build-receipt-token.txt"
$urlPath = "C:\ProgramData\Chevalier\build-receipt-url.txt"
$stagePath = "C:\ProgramData\Chevalier\image-build-stage.txt"
$token = [System.IO.File]::ReadAllText($tokenPath).Trim()
$url = [System.IO.File]::ReadAllText($urlPath).Trim()
$artifactRoot = "C:\Windows\Temp\OpenBracketImage"
$architecture = switch ($env:PROCESSOR_ARCHITECTURE) {
    "ARM64" { "arm64" }
    "AMD64" { "amd64" }
    default { throw "Unsupported Windows seal architecture: $env:PROCESSOR_ARCHITECTURE" }
}

function Send-SealReceipt {
    param(
        [Parameter(Mandatory = $true)][ValidateSet("verified", "failure")][string]$Status,
        [string]$Message = ""
    )

    if (-not $token -or -not $url) {
        throw "The authenticated build receipt configuration is incomplete"
    }
    $payload = @{ status = $Status; stage = "seal"; message = $Message } | ConvertTo-Json -Compress
    Invoke-WebRequest -UseBasicParsing -Uri $url -Method Post -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body $payload -TimeoutSec 15 | Out-Null
}

$stagedServices = "C:\ProgramData\Chevalier\runtime-services-staged"
New-Item -ItemType Directory -Force -Path $stagedServices | Out-Null
foreach ($file in @(
    "chevalier-vfs-winfsp-$architecture.exe",
    "chevalier-guest-agent-$architecture.exe",
    "chevalier-guest-services.SHA256SUMS",
    "initialize-state.ps1",
    "install-runtime-services.ps1"
)) {
    Copy-Item -Force -Path (Join-Path $artifactRoot $file) -Destination (Join-Path $stagedServices $file)
}
$setupScripts = "C:\Windows\Setup\Scripts"
New-Item -ItemType Directory -Force -Path $setupScripts | Out-Null
Copy-Item -Force -Path (Join-Path $artifactRoot "SetupComplete-services.cmd") -Destination (Join-Path $setupScripts "SetupComplete.cmd")
$runtimeUnattend = "C:\ProgramData\Chevalier\unattend-runtime.xml"
Copy-Item -Force -Path (Join-Path $artifactRoot "unattend-runtime-$architecture.xml") -Destination $runtimeUnattend

Remove-Item -Recurse -Force $artifactRoot -ErrorAction SilentlyContinue
Remove-Item -Force "C:\ProgramData\Chevalier\complete-image.ps1" -ErrorAction SilentlyContinue
Remove-Item -Force "C:\ProgramData\Chevalier\image-build.log" -ErrorAction SilentlyContinue
Remove-Item -Force "C:\ProgramData\Chevalier\image-build.error.txt" -ErrorAction SilentlyContinue
$cachedUnattendPaths = @(
    "C:\Windows\Panther\unattend.xml",
    "C:\Windows\Panther\unattend-original.xml",
    "C:\Windows\Panther\Unattend\unattend.xml",
    "C:\Windows\Panther\Autounattend.xml"
)
Remove-Item -Force $cachedUnattendPaths -ErrorAction SilentlyContinue

Set-Content -Encoding ASCII -Path $stagePath -Value "cleanup-winrm"
$winrmPolicy = "HKLM:\SOFTWARE\Policies\Microsoft\Windows\WinRM\Service"
Remove-ItemProperty -Path $winrmPolicy -Name AllowBasic -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $winrmPolicy -Name AllowUnencryptedTraffic -ErrorAction SilentlyContinue

Get-NetFirewallRule -DisplayGroup "Windows Remote Management" -ErrorAction SilentlyContinue | Disable-NetFirewallRule
$winrmService = "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\WSMAN\Service"
New-ItemProperty -Path $winrmService -Name auth_basic -PropertyType DWord -Value 0 -Force | Out-Null
New-ItemProperty -Path $winrmService -Name allow_unencrypted -PropertyType DWord -Value 0 -Force | Out-Null
$winrmListeners = "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\WSMAN\Listener"
if (Test-Path $winrmListeners) {
    Get-ChildItem $winrmListeners | Remove-Item -Recurse -Force
}
Set-Service -Name WinRM -StartupType Disabled
Remove-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System" -Name LocalAccountTokenFilterPolicy -ErrorAction SilentlyContinue

if ((Test-Path $winrmListeners) -and @(Get-ChildItem $winrmListeners).Count -ne 0) {
    throw "WinRM listeners remain after image cleanup"
}
if ((Get-ItemProperty -Path $winrmService -Name auth_basic).auth_basic -ne 0) {
    throw "WinRM Basic authentication remains enabled"
}
if ((Get-ItemProperty -Path $winrmService -Name allow_unencrypted).allow_unencrypted -ne 0) {
    throw "WinRM unencrypted transport remains enabled"
}
if ((Get-Service -Name WinRM).StartType -ne "Disabled") {
    throw "WinRM is not disabled"
}
if (Get-NetFirewallRule -DisplayGroup "Windows Remote Management" -ErrorAction SilentlyContinue | Where-Object Enabled -eq "True") {
    throw "Windows Remote Management firewall rules remain enabled"
}
$remainingUnattendPaths = @($cachedUnattendPaths | Where-Object { Test-Path $_ })
if ($remainingUnattendPaths.Count -ne 0) {
    throw "Cached Windows answer files remain after image cleanup"
}

Set-Content -Encoding ASCII -Path $stagePath -Value "cleanup-autologon"
$winlogon = "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon"
Remove-ItemProperty -Path $winlogon -Name AutoAdminLogon -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $winlogon -Name AutoLogonCount -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $winlogon -Name DefaultDomainName -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $winlogon -Name DefaultPassword -ErrorAction SilentlyContinue
Remove-ItemProperty -Path $winlogon -Name DefaultUserName -ErrorAction SilentlyContinue

$runOnce = "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\RunOnce"
Remove-ItemProperty -Path $runOnce -Name "!OpenBracketImageComplete" -ErrorAction SilentlyContinue

$passwordBytes = New-Object byte[] 48
$passwordGenerator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
try {
    $passwordGenerator.GetBytes($passwordBytes)
} finally {
    $passwordGenerator.Dispose()
}
$scrubbedAdministratorPassword = ConvertTo-SecureString ([Convert]::ToBase64String($passwordBytes)) -AsPlainText -Force
Set-LocalUser -Name "Administrator" -Password $scrubbedAdministratorPassword

$currentScript = $PSCommandPath
$verifiedReceiptSent = $false
try {
    Set-Content -Encoding ASCII -Path $stagePath -Value "sysprep"
    Send-SealReceipt -Status "verified"
    $verifiedReceiptSent = $true

    Remove-Item -Force $tokenPath, $urlPath, $stagePath
    Remove-Item -Force $currentScript -ErrorAction SilentlyContinue

    $sysprep = Start-Process -FilePath "$env:WINDIR\System32\Sysprep\Sysprep.exe" -ArgumentList "/generalize", "/oobe", "/shutdown", "/quiet", "/unattend:$runtimeUnattend" -Wait -PassThru
    if ($sysprep.ExitCode -ne 0) {
        throw "Sysprep generalization failed with code $($sysprep.ExitCode)"
    }

    Start-Sleep -Seconds 300
    throw "Sysprep returned without shutting down Windows"
} catch {
    if ($verifiedReceiptSent) {
        try {
            Send-SealReceipt -Status "failure" -Message $_.Exception.Message
        } catch {
        }
    }
    throw
}
