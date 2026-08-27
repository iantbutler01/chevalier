$ErrorActionPreference = "Stop"

$root = "C:\ProgramData\Chevalier\winfsp-e2e"
$config = Get-Content -Raw -Path (Join-Path $root "e2e-config.json") | ConvertFrom-Json
$state = Join-Path $root "state"
$tokenFile = Join-Path $root "gateway.token"
$executable = Join-Path $root "chevalier-vfs-winfsp.exe"
$statusFile = Join-Path $state "status.json"
$stdoutLog = Join-Path $root "service.stdout.log"
$stderrLog = Join-Path $root "service.stderr.log"
New-Item -ItemType Directory -Force -Path $root, $state | Out-Null
[System.IO.File]::WriteAllText($tokenFile, [string]$config.gatewayToken)

function Send-Phase {
    param(
        [Parameter(Mandatory = $true)][string]$Phase,
        [hashtable]$Details = @{}
    )
    $body = @{ phase = $Phase; details = $Details } | ConvertTo-Json -Depth 8 -Compress
    Invoke-WebRequest -UseBasicParsing -Uri ([string]$config.resultUrl) -Method Post -Headers @{ Authorization = "Bearer $($config.resultToken)" } -ContentType "application/json" -Body $body -TimeoutSec 30 | Out-Null
}

function Wait-Mount {
    foreach ($attempt in 1..240) {
        if (Test-Path "W:\") {
            return
        }
        Start-Sleep -Milliseconds 250
    }
    throw "Chevalier WinFsp did not mount W:"
}

function Wait-HostRail {
    $uri = [Uri]$config.resultUrl
    foreach ($attempt in 1..240) {
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $connected = $client.ConnectAsync($uri.Host, $uri.Port).Wait(250)
            if ($connected -and $client.Connected) {
                return
            }
        } catch {
        } finally {
            $client.Dispose()
        }
        Start-Sleep -Milliseconds 250
    }
    throw "Chevalier host rail did not become reachable"
}

function Read-Status {
    if (-not (Test-Path $statusFile)) {
        return $null
    }
    try {
        return Get-Content -Raw -Path $statusFile | ConvertFrom-Json
    } catch {
        return $null
    }
}

function Wait-Drain {
    foreach ($attempt in 1..240) {
        $status = Read-Status
        if ($status -and $status.pending_events -eq 0 -and $status.acknowledged_sequence -eq $status.last_committed_sequence) {
            return $status
        }
        Start-Sleep -Milliseconds 250
    }
    $status = Read-Status
    throw "Chevalier WinFsp did not drain: $($status | ConvertTo-Json -Compress)"
}

function Start-Vfs {
    foreach ($logFile in @($stdoutLog, $stderrLog)) {
        if (Test-Path $logFile) {
            Remove-Item -Force $logFile
        }
    }
    $arguments = @(
        "--endpoint", [string]$config.gatewayEndpoint,
        "--scope", [string]$config.scope,
        "--token-file", $tokenFile,
        "--state-directory", $state,
        "--status-file", $statusFile,
        "--mount", "W:"
    )
    $process = Start-Process -FilePath $executable -ArgumentList $arguments -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog -PassThru
    Wait-Mount
    return $process
}

function Flush-Text {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Text
    )
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Create, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::Read)
    try {
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush($true)
    } finally {
        $stream.Dispose()
    }
}

function Run-Git-Gate {
    $git = "C:\Program Files\Git\cmd\git.exe"
    $repo = "W:\repo"

    function Invoke-GitChecked {
        param([Parameter(Mandatory = $true)][string[]]$GitArguments)
        $previousPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            $output = (& $git @GitArguments 2>&1 | Out-String).Trim()
            $exitCode = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $previousPreference
        }
        if ($exitCode -ne 0) {
            throw "git $($GitArguments -join ' ') failed with exit code ${exitCode}: $output"
        }
        if ($output) {
            Write-Host $output
        }
    }

    New-Item -ItemType Directory -Force -Path $repo | Out-Null
    Invoke-GitChecked -GitArguments @("-C", $repo, "init")
    Invoke-GitChecked -GitArguments @("-C", $repo, "config", "user.email", "winfsp-e2e@openbracket.invalid")
    Invoke-GitChecked -GitArguments @("-C", $repo, "config", "user.name", "WinFsp E2E")
    Invoke-GitChecked -GitArguments @("-C", $repo, "config", "core.autocrlf", "false")
    Set-Content -Encoding UTF8 -NoNewline -Path (Join-Path $repo "README.md") -Value "base`n"
    Invoke-GitChecked -GitArguments @("-C", $repo, "add", "README.md")
    Invoke-GitChecked -GitArguments @("-C", $repo, "commit", "-m", "base")
    Invoke-GitChecked -GitArguments @("-C", $repo, "switch", "-c", "feature")
    Add-Content -Encoding UTF8 -Path (Join-Path $repo "README.md") -Value "feature"
    Invoke-GitChecked -GitArguments @("-C", $repo, "commit", "-am", "feature")
    Invoke-GitChecked -GitArguments @("-C", $repo, "switch", "-")
    Set-Content -Encoding UTF8 -Path (Join-Path $repo "main.txt") -Value "main"
    Invoke-GitChecked -GitArguments @("-C", $repo, "add", "main.txt")
    Invoke-GitChecked -GitArguments @("-C", $repo, "commit", "-m", "main")
    Invoke-GitChecked -GitArguments @("-C", $repo, "merge", "--no-ff", "feature", "-m", "merge")
    Set-Content -Encoding UTF8 -Path (Join-Path $repo "stash.txt") -Value "stash"
    Invoke-GitChecked -GitArguments @("-C", $repo, "stash", "push", "--include-untracked", "-m", "e2e")
    Invoke-GitChecked -GitArguments @("-C", $repo, "stash", "pop")
    Invoke-GitChecked -GitArguments @("-C", $repo, "add", "stash.txt")
    Invoke-GitChecked -GitArguments @("-C", $repo, "commit", "-m", "stash")
    New-Item -ItemType File -Force -Path (Join-Path $repo ".git\index.lock") | Out-Null
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $lockOutput = (& $git -C $repo add README.md 2>&1 | Out-String).Trim()
        $lockExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($lockExitCode -eq 0) {
        throw "Git unexpectedly ignored an existing index.lock"
    }
    if ($lockOutput -notmatch "index\.lock") {
        throw "Git failed for the wrong reason with index.lock present: $lockOutput"
    }
    Remove-Item -Force (Join-Path $repo ".git\index.lock")
    Invoke-GitChecked -GitArguments @("-C", $repo, "gc")
    Invoke-GitChecked -GitArguments @("-C", $repo, "fsck", "--strict", "--full")
    $head = (& $git -C $repo rev-parse HEAD 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "git rev-parse HEAD failed: $head"
    }
    return $head
}

$vfs = $null
try {
    Wait-HostRail
    $vfs = Start-Vfs
    if ([System.IO.File]::ReadAllText("W:\seed.txt") -ne "seed-from-gateway") {
        throw "seed hydration through W: returned unexpected bytes"
    }
    Flush-Text -Path "W:\online.txt" -Text "online-published"
    Flush-Text -Path "W:\rename-source.txt" -Text "renamed-online"
    Move-Item -Path "W:\rename-source.txt" -Destination "W:\renamed.txt"
    Flush-Text -Path "W:\delete-me.txt" -Text "deleted-online"
    Remove-Item -Force "W:\delete-me.txt"
    if (Test-Path "W:\delete-me.txt") {
        throw "native WinFsp delete left W:\delete-me.txt visible"
    }
    $gitHead = Run-Git-Gate
    $onlineStatus = Wait-Drain
    Send-Phase -Phase "online-pass" -Details @{ gitHead = $gitHead; status = $onlineStatus }

    Flush-Text -Path "W:\offline-source.txt" -Text "offline-durable"
    Move-Item -Path "W:\offline-source.txt" -Destination "W:\offline-renamed.txt"
    $pending = $null
    foreach ($attempt in 1..120) {
        $pending = Read-Status
        if ($pending -and $pending.pending_events -gt 0 -and $pending.last_committed_sequence -gt $pending.acknowledged_sequence) {
            break
        }
        Start-Sleep -Milliseconds 250
    }
    if (-not $pending -or $pending.pending_events -eq 0) {
        throw "gateway outage did not leave a durable pending WAL"
    }
    Stop-Process -Id $vfs.Id -Force
    $vfs.WaitForExit(15000) | Out-Null
    foreach ($attempt in 1..120) {
        if (-not (Test-Path "W:\")) {
            break
        }
        Start-Sleep -Milliseconds 100
    }
    $vfs = Start-Vfs
    if ([System.IO.File]::ReadAllText("W:\offline-renamed.txt") -ne "offline-durable") {
        throw "service restart lost offline WAL-backed bytes"
    }
    Send-Phase -Phase "offline-recovered" -Details @{ status = (Read-Status) }
    $replayed = Wait-Drain
    & "C:\Program Files\Git\cmd\git.exe" -C "W:\repo" fsck --strict --full
    if ($LASTEXITCODE -ne 0) {
        throw "Git fsck failed after service recovery"
    }
    $finalHead = (& "C:\Program Files\Git\cmd\git.exe" -C "W:\repo" rev-parse HEAD).Trim()
    if ($finalHead -ne $gitHead) {
        throw "Git HEAD changed across service recovery"
    }
    Send-Phase -Phase "success" -Details @{ gitHead = $finalHead; status = $replayed }
} catch {
    try {
        $serviceLog = if (Test-Path $stderrLog) { (Get-Content -Tail 300 -Path $stderrLog | Out-String) } else { "" }
        Send-Phase -Phase "failure" -Details @{ error = ($_ | Out-String); status = (Read-Status); serviceLog = $serviceLog }
    } catch {
    }
    throw
} finally {
    if ($vfs -and -not $vfs.HasExited) {
        Stop-Process -Id $vfs.Id -Force -ErrorAction SilentlyContinue
    }
    Remove-Item -Force $tokenFile -ErrorAction SilentlyContinue
    shutdown.exe /s /t 5 /f
}
