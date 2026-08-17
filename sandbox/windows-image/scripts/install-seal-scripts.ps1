$ErrorActionPreference = "Stop"

$destination = "C:\ProgramData\Chevalier"
New-Item -ItemType Directory -Force -Path $destination | Out-Null
Copy-Item "C:\Windows\Temp\OpenBracketImage\schedule-seal.ps1" -Destination $destination -Force -ErrorAction SilentlyContinue
Copy-Item "C:\Windows\Temp\OpenBracketImage\finalize-image.ps1" -Destination $destination -Force -ErrorAction SilentlyContinue

if (-not (Test-Path (Join-Path $destination "schedule-seal.ps1"))) {
    throw "schedule-seal.ps1 was not uploaded"
}
if (-not (Test-Path (Join-Path $destination "finalize-image.ps1"))) {
    throw "finalize-image.ps1 was not uploaded"
}
