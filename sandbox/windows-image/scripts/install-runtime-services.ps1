param(
    [Parameter(Mandatory = $true)][string]$ArtifactRoot,
    [switch]$StartGuestService
)

$ErrorActionPreference = "Stop"

$chevalierRoot = "C:\Program Files\Chevalier"
$runtimeConfig = "C:\ProgramData\Chevalier\runtime\runtime.json"
$artifacts = @{
    "chevalier-vfs-winfsp.exe" = "chevalier-vfs-winfsp-arm64.exe"
    "chevalier-guest-agent.exe" = "chevalier-guest-agent-arm64.exe"
    "initialize-state.ps1" = "initialize-state.ps1"
}

New-Item -ItemType Directory -Force -Path $chevalierRoot | Out-Null
$expectedHashes = @{}
foreach ($line in Get-Content -Path (Join-Path $ArtifactRoot "chevalier-guest-services.SHA256SUMS")) {
    if ($line -match '^([0-9a-fA-F]{64})\s+(.+)$') {
        $expectedHashes[$Matches[2]] = $Matches[1].ToUpperInvariant()
    }
}
foreach ($destinationName in $artifacts.Keys) {
    $source = Join-Path $ArtifactRoot $artifacts[$destinationName]
    if (-not (Test-Path $source)) {
        throw "Runtime service artifact is missing: $source"
    }
    if ($source.EndsWith(".exe")) {
        $sourceName = Split-Path -Leaf $source
        if (-not $expectedHashes.ContainsKey($sourceName)) {
            throw "Runtime service checksum is missing: $sourceName"
        }
        $actualHash = (Get-FileHash -Algorithm SHA256 -Path $source).Hash
        if ($actualHash -ne $expectedHashes[$sourceName]) {
            throw "Runtime service checksum mismatch: $sourceName"
        }
    }
    Copy-Item -Force -Path $source -Destination (Join-Path $chevalierRoot $destinationName)
}

$guestAgent = Join-Path $chevalierRoot "chevalier-guest-agent.exe"
& $guestAgent --install-services --config $runtimeConfig
if ($LASTEXITCODE -ne 0) {
    throw "Native Chevalier service installation failed with exit code $LASTEXITCODE"
}

if ($StartGuestService) {
    Start-Service -Name "ChevalierGuest"
}
