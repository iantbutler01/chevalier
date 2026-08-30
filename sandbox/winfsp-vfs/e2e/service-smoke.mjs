#!/usr/bin/env node

import { access, readFile, rename, stat, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import { Sandbox, SessionSourceType } from "../../../ts-sandbox/index.js";

const required = (name) => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
};

const endpoint = required("OPENBRACKET_WINFSP_VMD_ENDPOINT");
const image = resolve(required("OPENBRACKET_WINFSP_IMAGE"));
const gatewayEndpoint = required("OPENBRACKET_WINFSP_GATEWAY_ENDPOINT");
const scope = required("OPENBRACKET_WINFSP_SCOPE");
const storagePath = resolve(required("OPENBRACKET_WINFSP_STORAGE_PATH"));
const vmdDataDirectory = resolve(required("OPENBRACKET_WINFSP_VMD_DATA_DIR"));
const acceptanceReceipt = resolve(required("OPENBRACKET_WINFSP_ACCEPTANCE_RECEIPT"));
const phaseEndpoint = required("OPENBRACKET_WINFSP_PHASE_ENDPOINT");
const phaseToken = required("OPENBRACKET_WINFSP_PHASE_TOKEN");
const architecture = required("OPENBRACKET_WINFSP_ARCHITECTURE");
if (architecture !== "arm64" && architecture !== "amd64") {
  throw new Error(`unsupported Windows acceptance architecture ${architecture}`);
}
const expectedIdentity = architecture === "arm64" ? "ARM64:True" : "AMD64:True";
const memoryMb = architecture === "arm64" ? 3072 : 8192;

const run = async (session, command, timeoutSecs = 120) => {
  const handle = await session.exec(command, {
    timeoutSecs,
    closeStdinOnStart: true,
  });
  const stdout = [];
  const stderr = [];
  let exitCode;
  for (;;) {
    const event = await handle.next();
    if (event === null) break;
    if (event.type === "stdout") stdout.push(event.data);
    if (event.type === "stderr") stderr.push(event.data);
    if (event.type === "exit") exitCode = event.code;
    if (event.type === "timeout") throw new Error(`command timed out: ${command}`);
  }
  const output = Buffer.concat(stdout).toString("utf8");
  const errorOutput = Buffer.concat(stderr).toString("utf8");
  if (exitCode !== 0) {
    throw new Error(`command exited ${exitCode}: ${command}\n${errorOutput}`);
  }
  return output;
};

const sendPhase = async (phase, details = {}) => {
  const response = await fetch(phaseEndpoint, {
    method: "POST",
    headers: {
      authorization: `Bearer ${phaseToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ phase, details }),
  });
  if (!response.ok) throw new Error(`phase ${phase} was rejected with HTTP ${response.status}`);
};

const sandbox = await Sandbox.connect(endpoint, {
  defaultImage: image,
  defaultArchitecture: architecture,
  defaultVcpu: 4,
  defaultMemoryMb: memoryMb,
  connectTimeoutMs: 10_000,
});

let session;
let identitySession;
try {
  session = await sandbox.session({
    name: "windows-winfsp-service-smoke",
    image,
    sourceType: SessionSourceType.WindowsTemplate,
    architecture,
    autoStart: true,
    metadata: {
      "chevalier.tier_b_eligible": "false",
      tenant_id: "winfsp-e2e",
      workspace_id: "winfsp-e2e",
    },
    sharedMounts: [
      {
        guestPath: "W:",
        mountTag: "workspace",
        vfsEndpoint: gatewayEndpoint,
        vfsScopePath: scope,
      },
    ],
  });
  const primaryVmId = session.vmId;

  const vmDirectory = join(vmdDataDirectory, primaryVmId);
  await stat(join(vmDirectory, "windows-vfs-state.qcow2"));
  try {
    await access(join(vmDirectory, "windows-runtime.img"));
    throw new Error("runtime secret disk remained attached after guest readiness");
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }

  const identity = (await run(session, "$env:PROCESSOR_ARCHITECTURE + ':' + [Environment]::Is64BitOperatingSystem")).trim();
  if (identity !== expectedIdentity) throw new Error(`unexpected guest identity ${identity}`);
  const firstMachineIdentity = JSON.parse(
    await run(
      session,
      "$administrator = Get-LocalUser -Name Administrator; [pscustomobject]@{ hostname = $env:COMPUTERNAME; machineGuid = (Get-ItemProperty 'HKLM:\\SOFTWARE\\Microsoft\\Cryptography').MachineGuid; machineSid = $administrator.SID.AccountDomainSid.Value } | ConvertTo-Json -Compress",
    ),
  );

  await run(
    session,
    [
      "foreach ($name in @('ChevalierGuest', 'ChevalierVFS')) { if ((Get-Service -Name $name).Status -ne 'Running') { throw \"$name is not running\" } }",
      "$status = Get-Content -Raw C:\\ProgramData\\Chevalier\\guest-status.json | ConvertFrom-Json",
      "if ($status.phase -ne 'ready' -or $status.error) { throw 'guest status is not ready' }",
    ].join("; "),
  );

  const hotpatchArtifact = process.env.OPENBRACKET_WINFSP_HOTPATCH_ARTIFACT?.trim();
  const hotpatchDebug = process.env.OPENBRACKET_WINFSP_HOTPATCH_DEBUG === "1";
  if (hotpatchArtifact) {
    const hotpatchUrl = required("OPENBRACKET_WINFSP_HOTPATCH_URL");
    const hotpatchToken = required("OPENBRACKET_WINFSP_HOTPATCH_TOKEN");
    await run(
      session,
      [
        "$temporary = 'C:\\ProgramData\\Chevalier\\runtime\\chevalier-vfs-winfsp.hotpatch.exe'",
        `$headers = @{ Authorization = 'Bearer ${hotpatchToken}' }`,
        `Invoke-WebRequest -UseBasicParsing -Uri '${hotpatchUrl}' -Headers $headers -OutFile $temporary`,
        "Stop-Service -Name ChevalierVFS",
        "Copy-Item -Force -LiteralPath $temporary -Destination 'C:\\Program Files\\Chevalier\\chevalier-vfs-winfsp.exe'",
        "Remove-Item -Force -LiteralPath $temporary",
        hotpatchDebug
          ? "$debugLog = 'C:\\ProgramData\\Chevalier\\runtime\\winfsp-debug.log'; Remove-Item -Force -ErrorAction SilentlyContinue $debugLog; Set-ItemProperty -LiteralPath 'HKLM:\\SYSTEM\\CurrentControlSet\\Services\\ChevalierVFS' -Name ImagePath -Value '\"C:\\Program Files\\Chevalier\\chevalier-vfs-winfsp.exe\" --config \"C:\\ProgramData\\Chevalier\\runtime\\runtime.json\" --debug --debug-log \"C:\\ProgramData\\Chevalier\\runtime\\winfsp-debug.log\"'; Start-Service -Name ChevalierVFS; (Get-Service -Name ChevalierVFS).WaitForStatus('Running', [TimeSpan]::FromSeconds(30))"
          : "Start-Service -Name ChevalierVFS; (Get-Service -Name ChevalierVFS).WaitForStatus('Running', [TimeSpan]::FromSeconds(30))",
        "$deadline = [DateTime]::UtcNow.AddSeconds(30)",
        "while (-not (Test-Path -LiteralPath W:\\seed.txt)) { if ([DateTime]::UtcNow -gt $deadline) { throw 'hotpatched W: did not become ready' }; Start-Sleep -Milliseconds 100 }",
      ].join("; "),
      180,
    );
  }

  const write = [
    "$path = 'W:\\service-smoke.txt'",
    "$bytes = [Text.Encoding]::UTF8.GetBytes('service-backed-winfsp')",
    "$file = [IO.File]::Open($path, [IO.FileMode]::Create, [IO.FileAccess]::ReadWrite, [IO.FileShare]::Read)",
    "try { $file.Write($bytes, 0, $bytes.Length); $file.Flush($true) } finally { $file.Dispose() }",
    "if ([IO.File]::ReadAllText($path) -ne 'service-backed-winfsp') { throw 'W: readback mismatch' }",
    "Write-Output command-and-winfsp-ok",
  ].join("; ");
  if (!(await run(session, write)).includes("command-and-winfsp-ok")) {
    throw new Error("authenticated command execution did not return its receipt");
  }
  await run(
    session,
    [
      "[IO.File]::WriteAllText('W:\\online.txt', 'online-published')",
      "[IO.File]::WriteAllText('W:\\rename-source.txt', 'renamed-online')",
      "Move-Item -LiteralPath 'W:\\rename-source.txt' -Destination 'W:\\renamed.txt'",
      "[IO.File]::WriteAllText('W:\\delete-me.txt', 'deleted-online')",
      "Remove-Item -LiteralPath 'W:\\delete-me.txt' -Force",
      "if (Test-Path -LiteralPath 'W:\\delete-me.txt') { throw 'WinFsp delete left delete-me.txt visible' }",
    ].join("; "),
  );

  const networkReceipt = (
    await run(
      session,
      "$response = Invoke-WebRequest -UseBasicParsing -Uri 'https://www.microsoft.com/robots.txt' -TimeoutSec 30; \"$($response.StatusCode):$($response.RawContentLength)\"",
      60,
    )
  ).trim();
  if (!/^200:[1-9][0-9]*$/.test(networkReceipt)) {
    throw new Error(`unexpected outbound network receipt ${JSON.stringify(networkReceipt)}`);
  }

  const firstBootDeadline = Date.now() + 10 * 60_000;
  for (;;) {
    const firstBoot = JSON.parse(
      await run(
        session,
        "Get-Content -Raw C:\\ProgramData\\Chevalier\\first-boot-status.json",
      ),
    );
    if (firstBoot.phase === "failed") {
      throw new Error(`Windows first boot failed: ${firstBoot.error ?? "unknown error"}`);
    }
    if (firstBoot.phase === "complete") break;
    if (Date.now() >= firstBootDeadline) {
      throw new Error(`Windows desktop did not complete first boot; last phase was ${firstBoot.phase}`);
    }
    await new Promise((resolveWait) => setTimeout(resolveWait, 1_000));
  }
  await run(
    session,
    [
      "$desktopSessions = (& query.exe user 2>&1 | Out-String)",
      "if ($desktopSessions -notmatch '(?im)^\\s*>?\\s*OpenBracket\\s+') { throw 'OpenBracket desktop session is not active' }",
      "if (Get-LocalUser -Name OpenBracketBootstrap -ErrorAction SilentlyContinue) { throw 'bootstrap account remains' }",
      "$answers = @('C:\\Windows\\Panther\\unattend.xml', 'C:\\Windows\\Panther\\Autounattend.xml', 'C:\\Windows\\Panther\\Unattend\\unattend.xml')",
      "if ($answers | Where-Object { Test-Path -LiteralPath $_ }) { throw 'cached first-boot answer remains' }",
      "$winlogon = 'HKLM:\\SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\Winlogon'",
      "foreach ($name in @('AutoAdminLogon', 'AutoLogonCount', 'DefaultDomainName', 'DefaultPassword', 'DefaultUserName')) { if ((Get-ItemProperty -LiteralPath $winlogon -Name $name -ErrorAction SilentlyContinue).$name) { throw \"Winlogon credential remains: $name\" } }",
    ].join("; "),
  );

  await run(
    session,
    [
      "$path = 'W:\\delete-smoke.txt'",
      "[IO.File]::WriteAllText($path, 'delete-me')",
      hotpatchDebug
        ? "try { Remove-Item -LiteralPath $path -Force -ErrorAction Stop } catch { Get-Content -ErrorAction SilentlyContinue -Tail 1000 'C:\\ProgramData\\Chevalier\\runtime\\winfsp-debug.log' | Write-Error; throw }"
        : "Remove-Item -LiteralPath $path -Force",
      "if (Test-Path -LiteralPath $path) { throw 'WinFsp delete did not remove the file' }",
      "Get-ChildItem -LiteralPath W:\\ -Filter '.chevalier-ready-*.tmp' | Remove-Item -Force",
    ].join("; "),
  );

  await session.writeFile("C:\\ProgramData\\Chevalier\\file-rpc-smoke.txt", Buffer.from("file-rpc-ok"));
  const rpcReadback = await session.readFile("C:\\ProgramData\\Chevalier\\file-rpc-smoke.txt");
  if (rpcReadback.toString("utf8") !== "file-rpc-ok") throw new Error("native file RPC readback mismatch");

  const git = [
    "$repo = 'W:\\repo'",
    "New-Item -ItemType Directory -Force -Path $repo | Out-Null",
    "& git -C $repo init | Out-Null",
    hotpatchDebug
      ? "if ($LASTEXITCODE) { Get-Content -ErrorAction SilentlyContinue -Tail 1000 'C:\\ProgramData\\Chevalier\\runtime\\winfsp-debug.log' | Write-Error; exit $LASTEXITCODE }"
      : "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo config user.email winfsp@openbracket.invalid",
    hotpatchDebug
      ? "if ($LASTEXITCODE) { Get-Content -ErrorAction SilentlyContinue -Tail 1000 'C:\\ProgramData\\Chevalier\\runtime\\winfsp-debug.log' | Write-Error; exit $LASTEXITCODE }"
      : "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo config user.name 'WinFsp Service'",
    hotpatchDebug
      ? "if ($LASTEXITCODE) { Get-Content -ErrorAction SilentlyContinue -Tail 1000 'C:\\ProgramData\\Chevalier\\runtime\\winfsp-debug.log' | Write-Error; exit $LASTEXITCODE }"
      : "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "for ($index = 0; $index -lt 20; $index++) { & git -C $repo config \"openbracket.lockstress$index\" \"value-$index\"; if ($LASTEXITCODE) { throw \"git config lock stress failed at iteration $index\" }; if (Test-Path -LiteralPath (Join-Path $repo '.git\\config.lock')) { throw \"git config left a lock at iteration $index\" } }",
    "[IO.File]::WriteAllText((Join-Path $repo 'README.md'), 'service git')",
    "& git -C $repo add README.md",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo commit -m initial | Out-Null",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "$mainBranch = (& git -C $repo branch --show-current).Trim()",
    "& git -C $repo switch -c feature | Out-Null",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "[IO.File]::AppendAllText((Join-Path $repo 'README.md'), \"`nfeature\")",
    "& git -C $repo commit -am feature | Out-Null",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo switch $mainBranch | Out-Null",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "[IO.File]::WriteAllText((Join-Path $repo 'main.txt'), 'main')",
    "& git -C $repo add main.txt",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo commit -m main | Out-Null",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo merge --no-ff feature -m merge | Out-Null",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "[IO.File]::WriteAllText((Join-Path $repo 'stash.txt'), 'stash')",
    "& git -C $repo stash push --include-untracked -m acceptance | Out-Null",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo stash pop | Out-Null",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo add stash.txt",
    "& git -C $repo commit -m stash | Out-Null",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "New-Item -ItemType File -Force -Path (Join-Path $repo '.git\\index.lock') | Out-Null",
    "& git -C $repo add README.md 2>$null",
    "if ($LASTEXITCODE -eq 0) { throw 'Git ignored an existing index.lock' }",
    "Remove-Item -LiteralPath (Join-Path $repo '.git\\index.lock') -Force",
    "& git -C $repo gc",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo fsck --strict --full",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo rev-parse HEAD",
  ].join("; ");
  const head = (await run(session, git, 300)).trim().split(/\r?\n/).at(-1);
  if (!/^[0-9a-f]{40}$/i.test(head)) throw new Error(`invalid Git HEAD ${head}`);

  await sendPhase("online-pass", { head });
  await run(
    session,
    [
      "$path = 'W:\\offline-source.txt'",
      "$bytes = [Text.Encoding]::UTF8.GetBytes('offline-durable')",
      "$file = [IO.File]::Open($path, [IO.FileMode]::Create, [IO.FileAccess]::ReadWrite, [IO.FileShare]::Read)",
      "try { $file.Write($bytes, 0, $bytes.Length); $file.Flush($true) } finally { $file.Dispose() }",
      "Move-Item -LiteralPath $path -Destination 'W:\\offline-renamed.txt'",
      "$deadline = [DateTime]::UtcNow.AddSeconds(30)",
      "do { $status = Get-Content -Raw 'C:\\ProgramData\\Chevalier\\state-volume\\workspace\\status.json' | ConvertFrom-Json; if ($status.pending_events -gt 0 -and $status.last_committed_sequence -gt $status.acknowledged_sequence) { break }; Start-Sleep -Milliseconds 100 } while ([DateTime]::UtcNow -lt $deadline)",
      "if ($status.pending_events -eq 0) { throw 'gateway outage did not leave a pending WAL event' }",
      "$service = Get-CimInstance Win32_Service -Filter \"Name='ChevalierVFS'\"",
      "Stop-Process -Id $service.ProcessId -Force",
      "$deadline = [DateTime]::UtcNow.AddSeconds(60)",
      "do { if ((Get-Service ChevalierVFS).Status -eq 'Running' -and (Test-Path -LiteralPath 'W:\\offline-renamed.txt')) { break }; Start-Sleep -Milliseconds 250 } while ([DateTime]::UtcNow -lt $deadline)",
      "if ([IO.File]::ReadAllText('W:\\offline-renamed.txt') -ne 'offline-durable') { throw 'VFS process recovery lost offline WAL-backed bytes' }",
    ].join("; "),
    180,
  );
  await sendPhase("offline-recovered", { head });
  await run(
    session,
    [
      "$deadline = [DateTime]::UtcNow.AddSeconds(90)",
      "do { $status = Get-Content -Raw 'C:\\ProgramData\\Chevalier\\state-volume\\workspace\\status.json' | ConvertFrom-Json; if ($status.pending_events -eq 0 -and $status.acknowledged_sequence -eq $status.last_committed_sequence) { break }; Start-Sleep -Milliseconds 100 } while ([DateTime]::UtcNow -lt $deadline)",
      "if ($status.pending_events -ne 0 -or $status.acknowledged_sequence -ne $status.last_committed_sequence) { throw 'recovered VFS WAL did not drain' }",
      "& git -C W:\\repo fsck --strict --full",
      "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    ].join("; "),
    180,
  );

  await session.stop();
  const published = await readFile(storagePath, "utf8");
  if (published !== "service-backed-winfsp") throw new Error(`gateway bytes after drain were ${JSON.stringify(published)}`);

  await session.start();
  const restartReadback = (await run(session, "[IO.File]::ReadAllText('W:\\service-smoke.txt')")).trim();
  if (restartReadback !== "service-backed-winfsp") throw new Error("state disk did not survive restart");
  const restartedHead = (await run(session, "git -C W:\\repo rev-parse HEAD")).trim();
  if (restartedHead !== head) throw new Error("Git HEAD changed across VM restart");

  await session.stop();
  await session.discard();
  session = undefined;
  identitySession = await sandbox.session({
    name: "windows-winfsp-identity-smoke",
    image,
    sourceType: SessionSourceType.WindowsTemplate,
    architecture,
    autoStart: true,
    metadata: {
      "chevalier.tier_b_eligible": "false",
      tenant_id: "winfsp-e2e",
      workspace_id: "winfsp-e2e-identity",
    },
    sharedMounts: [
      {
        guestPath: "W:",
        mountTag: "workspace",
        vfsEndpoint: gatewayEndpoint,
        vfsScopePath: scope,
      },
    ],
  });
  const secondMachineIdentity = JSON.parse(
    await run(
      identitySession,
      "$administrator = Get-LocalUser -Name Administrator; [pscustomobject]@{ hostname = $env:COMPUTERNAME; machineGuid = (Get-ItemProperty 'HKLM:\\SOFTWARE\\Microsoft\\Cryptography').MachineGuid; machineSid = $administrator.SID.AccountDomainSid.Value } | ConvertTo-Json -Compress",
    ),
  );
  for (const field of ["hostname", "machineGuid", "machineSid"]) {
    if (firstMachineIdentity[field] === secondMachineIdentity[field]) {
      throw new Error(`fresh Windows clones reused ${field} ${firstMachineIdentity[field]}`);
    }
  }
  await identitySession.discard();
  identitySession = undefined;
  await sendPhase("success", { head });

  if (!hotpatchArtifact) {
    const manifest = JSON.parse(await readFile(join(image, "image-manifest.json"), "utf8"));
    const temporaryReceipt = `${acceptanceReceipt}.${process.pid}`;
    await writeFile(
      temporaryReceipt,
      `${JSON.stringify(
        {
          schemaVersion: 1,
          acceptedAt: new Date().toISOString(),
          architecture,
          imageSha256: manifest.image.sha256,
          gitHead: head,
          cloneIdentities: [firstMachineIdentity, secondMachineIdentity],
          checks: [
            "runtime-secrets-absent-from-base",
            "runtime-disk-detached-before-ready",
            "authenticated-command-execution",
            "outbound-https",
            "native-file-rpc",
            "winfsp-create-write-flush-read-delete-basic-info",
            "git-init-add-commit-fsck",
            "git-lock-rename-stress",
            "guest-vfs-wal-recovery",
            "gateway-publication-drain",
            "fresh-identity-uniqueness-matrix",
            "same-node-state-disk-restart",
          ],
        },
        null,
        2,
      )}\n`,
      { mode: 0o600 },
    );
    await rename(temporaryReceipt, acceptanceReceipt);
  }

  console.log(JSON.stringify({ phase: "success", identity, head, vmId: primaryVmId }));
} catch (error) {
  await sendPhase("failure", { message: error instanceof Error ? error.message : String(error) }).catch(() => {});
  throw error;
} finally {
  if (identitySession) await identitySession.discard();
  if (session) await session.discard();
}
