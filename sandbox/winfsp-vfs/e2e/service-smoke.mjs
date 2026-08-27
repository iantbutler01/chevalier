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

const sandbox = await Sandbox.connect(endpoint, {
  defaultImage: image,
  defaultArchitecture: "arm64",
  defaultVcpu: 4,
  defaultMemoryMb: 3072,
  connectTimeoutMs: 10_000,
});

let session;
try {
  session = await sandbox.session({
    name: "windows-winfsp-service-smoke",
    image,
    sourceType: SessionSourceType.WindowsTemplate,
    architecture: "arm64",
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

  const vmDirectory = join(vmdDataDirectory, session.vmId);
  await stat(join(vmDirectory, "windows-vfs-state.qcow2"));
  try {
    await access(join(vmDirectory, "windows-runtime.iso"));
    throw new Error("runtime secret ISO remained attached after guest readiness");
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }

  const identity = (await run(session, "$env:PROCESSOR_ARCHITECTURE + ':' + [Environment]::Is64BitOperatingSystem")).trim();
  if (identity !== "ARM64:True") throw new Error(`unexpected guest identity ${identity}`);

  await run(
    session,
    [
      "if (Get-LocalUser -Name OpenBracketBootstrap -ErrorAction SilentlyContinue) { throw 'bootstrap account remains' }",
      "$answers = @('C:\\Windows\\Panther\\unattend.xml', 'C:\\Windows\\Panther\\Autounattend.xml', 'C:\\Windows\\Panther\\Unattend\\unattend.xml')",
      "if ($answers | Where-Object { Test-Path -LiteralPath $_ }) { throw 'cached first-boot answer remains' }",
      "foreach ($name in @('ChevalierGuest', 'ChevalierVFS')) { if ((Get-Service -Name $name).Status -ne 'Running') { throw \"$name is not running\" } }",
      "$status = Get-Content -Raw C:\\ProgramData\\Chevalier\\runtime\\guest-status.json | ConvertFrom-Json",
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
    "& git -C $repo config user.name 'WinFsp Service'",
    "[IO.File]::WriteAllText((Join-Path $repo 'README.md'), 'service git')",
    "& git -C $repo add README.md",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo commit -m initial | Out-Null",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo fsck --strict --full",
    "if ($LASTEXITCODE) { exit $LASTEXITCODE }",
    "& git -C $repo rev-parse HEAD",
  ].join("; ");
  const head = (await run(session, git, 300)).trim().split(/\r?\n/).at(-1);
  if (!/^[0-9a-f]{40}$/i.test(head)) throw new Error(`invalid Git HEAD ${head}`);

  await session.stop();
  const published = await readFile(storagePath, "utf8");
  if (published !== "service-backed-winfsp") throw new Error(`gateway bytes after drain were ${JSON.stringify(published)}`);

  await session.start();
  const restartReadback = (await run(session, "[IO.File]::ReadAllText('W:\\service-smoke.txt')")).trim();
  if (restartReadback !== "service-backed-winfsp") throw new Error("state disk did not survive restart");
  const restartedHead = (await run(session, "git -C W:\\repo rev-parse HEAD")).trim();
  if (restartedHead !== head) throw new Error("Git HEAD changed across VM restart");

  if (!hotpatchArtifact) {
    const manifest = JSON.parse(await readFile(join(image, "image-manifest.json"), "utf8"));
    const temporaryReceipt = `${acceptanceReceipt}.${process.pid}`;
    await writeFile(
      temporaryReceipt,
      `${JSON.stringify(
        {
          schemaVersion: 1,
          acceptedAt: new Date().toISOString(),
          architecture: "arm64",
          imageSha256: manifest.image.sha256,
          gitHead: head,
          checks: [
            "runtime-secrets-absent-from-base",
            "runtime-iso-detached-before-ready",
            "authenticated-command-execution",
            "native-file-rpc",
            "winfsp-create-write-flush-read-delete-basic-info",
            "git-init-add-commit-fsck",
            "gateway-publication-drain",
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

  console.log(JSON.stringify({ phase: "success", identity, head, vmId: session.vmId }));
} finally {
  if (session) await session.discard();
}
