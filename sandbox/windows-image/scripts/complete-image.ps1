$ErrorActionPreference = "Stop"

$artifactRoot = "C:\Windows\Temp\OpenBracketImage"
$stateRoot = "C:\ProgramData\Chevalier"

function Send-Receipt {
    param(
        [Parameter(Mandatory = $true)][ValidateSet("verified", "success", "failure")][string]$Status,
        [string]$Stage = "complete",
        [string]$Message = ""
    )

    $token = [System.IO.File]::ReadAllText((Join-Path $stateRoot "build-receipt-token.txt")).Trim()
    $url = [System.IO.File]::ReadAllText((Join-Path $stateRoot "build-receipt-url.txt")).Trim()
    if (-not $token -or -not $url) {
        throw "The authenticated build receipt configuration is incomplete"
    }
    $payload = @{ status = $Status; stage = $Stage; message = $Message } | ConvertTo-Json -Compress
    Invoke-WebRequest -UseBasicParsing -Uri $url -Method Post -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body $payload -TimeoutSec 15 | Out-Null
}

function Write-FailureReceipt {
    param([string]$Stage, [string]$Message)
    try {
        Send-Receipt -Status "failure" -Stage $Stage -Message $Message
    } catch {
    }
}

Start-Sleep -Seconds 20
Start-Transcript -Path (Join-Path $stateRoot "image-build.log") -Append
try {
    Set-Content -Encoding ASCII -Path (Join-Path $stateRoot "image-build-stage.txt") -Value "verifying"
    & (Join-Path $artifactRoot "verify.ps1")
    Set-Content -Encoding ASCII -Path (Join-Path $stateRoot "image-build-stage.txt") -Value "installing-seal"
    & (Join-Path $artifactRoot "install-seal-scripts.ps1")
} catch {
    $_ | Out-String | Set-Content -Encoding UTF8 -Path (Join-Path $stateRoot "image-build.error.txt")
    Write-FailureReceipt -Stage "verification" -Message $_.Exception.Message
    & shutdown.exe /s /t 0 /f
    throw
} finally {
    Stop-Transcript
}

Start-Sleep -Seconds 15
try {
    Set-Content -Encoding ASCII -Path (Join-Path $stateRoot "image-build-stage.txt") -Value "finalizing"
    & (Join-Path $stateRoot "finalize-image.ps1")
} catch {
    $_ | Out-String | Set-Content -Encoding UTF8 -Path (Join-Path $stateRoot "image-build.error.txt")
    Write-FailureReceipt -Stage "finalization" -Message $_.Exception.Message
    & shutdown.exe /s /t 0 /f
    throw
}
