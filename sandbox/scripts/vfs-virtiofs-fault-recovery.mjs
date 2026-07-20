#!/usr/bin/env node

import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");

if (process.argv.includes("--help")) {
  console.log(`Disposable mounted VFS barrier and fault-recovery harness.

Required:
  SANDBOX_ENDPOINT
  SANDBOX_IMAGE
  CHEVALIER_VFS_FAULT_GATEWAY_PUBLIC_URL
  CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN
    (SANDBOX_AUTH_TOKEN is accepted as a fallback)

Optional:
  SANDBOX_AUTH_TOKEN
  SANDBOX_ARCHITECTURE=amd64
  CHEVALIER_VFS_FAULT_GATEWAY_BIND=0.0.0.0
  CHEVALIER_VFS_FAULT_GATEWAY_PORT=19096
  CHEVALIER_VFS_FAULT_BACKEND_PROFILE=openbracket-vfs-fuse
  CHEVALIER_VFS_FAULT_COMMAND_TIMEOUT_MS=180000
  CHEVALIER_VFS_FAULT_CHECKS=1,2,3
  CHEVALIER_VFS_FAULT_VMD_RESTART_COMMAND
    Optional operator-supplied restart of an isolated vmd. The command runs
    only after the harness has committed a disposable marker. It must not
    restart a shared production daemon.
  CHEVALIER_MODULE_PATH=<repo>/ts/index.js
  CHEVALIER_SANDBOX_MODULE_PATH=<repo>/ts-sandbox/index.js

The harness never launches OpenBracket. It uses a fresh local store, fresh
owner/scope, two disposable VMs, a dedicated callback port, and exact cleanup.`);
  process.exit(0);
}

const required = (name) => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required; run with --help for the contract`);
  return value;
};

const sandboxEndpoint = required("SANDBOX_ENDPOINT");
const sandboxImage = required("SANDBOX_IMAGE");
const gatewayPublicUrl = required("CHEVALIER_VFS_FAULT_GATEWAY_PUBLIC_URL").replace(/\/+$/, "");
const sandboxAuthToken = process.env.SANDBOX_AUTH_TOKEN?.trim();
const vfsAuthToken =
  process.env.CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN?.trim() || sandboxAuthToken;
if (!vfsAuthToken) {
  throw new Error(
    "CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN (or SANDBOX_AUTH_TOKEN) is required",
  );
}

const architecture = process.env.SANDBOX_ARCHITECTURE?.trim() || "amd64";
const backendProfile =
  process.env.CHEVALIER_VFS_FAULT_BACKEND_PROFILE?.trim() || "openbracket-vfs-fuse";
const gatewayBind = process.env.CHEVALIER_VFS_FAULT_GATEWAY_BIND?.trim() || "0.0.0.0";
const gatewayPort = Number(process.env.CHEVALIER_VFS_FAULT_GATEWAY_PORT ?? "19096");
const commandTimeoutMs = Number(
  process.env.CHEVALIER_VFS_FAULT_COMMAND_TIMEOUT_MS ?? "180000",
);
if (!Number.isInteger(gatewayPort) || gatewayPort < 1 || gatewayPort > 65535) {
  throw new Error("CHEVALIER_VFS_FAULT_GATEWAY_PORT must be an integer in 1..65535");
}
if (!Number.isFinite(commandTimeoutMs) || commandTimeoutMs < 35_000) {
  throw new Error("CHEVALIER_VFS_FAULT_COMMAND_TIMEOUT_MS must be at least 35000");
}

const selectedChecks = (() => {
  const raw = process.env.CHEVALIER_VFS_FAULT_CHECKS?.trim();
  if (!raw) return null;
  return new Set(
    raw.split(",").map((part) => {
      const id = Number(part);
      if (!Number.isInteger(id) || id < 1 || id > 8) {
        throw new Error(`invalid CHEVALIER_VFS_FAULT_CHECKS entry: ${part}`);
      }
      return id;
    }),
  );
})();

const require = createRequire(import.meta.url);
const chevalierModulePath = resolve(
  process.env.CHEVALIER_MODULE_PATH?.trim() || join(repoRoot, "ts", "index.js"),
);
const sandboxModule = require(
  resolve(
    process.env.CHEVALIER_SANDBOX_MODULE_PATH?.trim() ||
      join(repoRoot, "ts-sandbox", "index.js"),
  ),
);
const { Sandbox } = sandboxModule;
if (typeof Sandbox?.connect !== "function") {
  throw new Error("native sandbox module does not expose Sandbox.connect");
}

const probeId = `fault-${Date.now()}-${randomUUID().slice(0, 8)}`;
const ownerId = `chevalier-vfs-${probeId}`;
const scopePath = `probes/${probeId}/repo`;
const mountPath = "/workspace";
const backingRoot = join(
  process.env.CHEVALIER_VFS_FAULT_TMPDIR?.trim() || "/tmp",
  `chevalier-${probeId}`,
);
const ownerRoot = join(backingRoot, ownerId);
const ownerEndpoint = `${gatewayPublicUrl}/internal/chevalier/vfs/${encodeURIComponent(ownerId)}`;

const withTimeout = async (promise, label, timeoutMs = commandTimeoutMs) => {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};

await mkdir(ownerRoot, { recursive: true });

let gatewayMode = "online";
let gatewayChild = null;
let gatewayChildStderr = "";
let gatewayRequests = 0;
let injectedFailures = 0;
let gatewayStarts = 0;
let pausedRequestCount = 0;
let gatewayProcessKills = 0;

const childScript = join(scriptDir, "vfs-fault-gateway-child.mjs");

const startGateway = async () => {
  if (gatewayChild !== null) return;
  gatewayChildStderr = "";
  const child = spawn(process.execPath, [childScript], {
    env: {
      ...process.env,
      CHEVALIER_VFS_FAULT_CHILD_OWNER_ID: ownerId,
      CHEVALIER_VFS_FAULT_CHILD_OWNER_ROOT: ownerRoot,
      CHEVALIER_VFS_FAULT_CHILD_AUTH_TOKEN: vfsAuthToken,
      CHEVALIER_VFS_FAULT_CHILD_BIND: gatewayBind,
      CHEVALIER_VFS_FAULT_CHILD_PORT: String(gatewayPort),
      CHEVALIER_VFS_FAULT_CHILD_MODULE_PATH: chevalierModulePath,
    },
    stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  gatewayChild = child;
  child.once("exit", () => {
    if (gatewayChild === child) gatewayChild = null;
  });
  child.stderr.on("data", (chunk) => {
    gatewayChildStderr = `${gatewayChildStderr}${Buffer.from(chunk).toString("utf8")}`.slice(
      -4_000,
    );
  });
  child.on("message", (message) => {
    if (!message || typeof message !== "object") return;
    if (message.type === "request") {
      gatewayRequests += 1;
      if (message.injected) injectedFailures += 1;
    } else if (message.type === "paused-request") {
      pausedRequestCount += 1;
    }
  });
  await withTimeout(
    new Promise((resolveReady, rejectReady) => {
      const onMessage = (message) => {
        if (message?.type === "ready") {
          cleanup();
          resolveReady();
        } else if (message?.type === "fatal") {
          cleanup();
          rejectReady(new Error(`gateway child failed: ${message.error}`));
        }
      };
      const onExit = (code, signal) => {
        cleanup();
        rejectReady(
          new Error(
            `gateway child exited before ready: code=${code} signal=${signal}; ${gatewayChildStderr}`,
          ),
        );
      };
      const cleanup = () => {
        child.off("message", onMessage);
        child.off("exit", onExit);
      };
      child.on("message", onMessage);
      child.once("exit", onExit);
    }),
    "start disposable gateway process",
    10_000,
  );
  gatewayMode = "online";
  gatewayStarts += 1;
};

const setGatewayMode = async (mode) => {
  const child = gatewayChild;
  if (child === null) throw new Error(`cannot set gateway mode ${mode}: process is stopped`);
  await withTimeout(
    new Promise((resolveMode, rejectMode) => {
      const onMessage = (message) => {
        if (message?.type === "mode" && message.mode === mode) {
          cleanup();
          resolveMode();
        }
      };
      const onExit = (code, signal) => {
        cleanup();
        rejectMode(
          new Error(`gateway exited while setting ${mode}: code=${code} signal=${signal}`),
        );
      };
      const cleanup = () => {
        child.off("message", onMessage);
        child.off("exit", onExit);
      };
      child.on("message", onMessage);
      child.once("exit", onExit);
      child.send({ type: "mode", mode }, (error) => {
        if (error) {
          cleanup();
          rejectMode(error);
        }
      });
    }),
    `set gateway mode ${mode}`,
    10_000,
  );
  gatewayMode = mode;
};

const stopGateway = async ({ injected = false } = {}) => {
  const child = gatewayChild;
  if (child === null) return;
  gatewayChild = null;
  const exit = new Promise((resolveExit) => child.once("exit", resolveExit));
  child.kill("SIGKILL");
  await withTimeout(exit, "kill disposable gateway process", 10_000);
  if (injected) gatewayProcessKills += 1;
};

await startGateway();

const commandResultText = (result) =>
  [`exit=${String(result.code)}`, result.stdout.trim(), result.stderr.trim()]
    .filter(Boolean)
    .join("\n");

const collectExecAtMarkers = async (handle, label, markerSteps = []) => {
  let code = null;
  let stdout = "";
  let stderr = "";
  let markerIndex = 0;
  for (;;) {
    const event = await withTimeout(handle.next(), `${label} output`);
    if (event === null) break;
    if (event.type === "stdout" && event.data) {
      stdout += Buffer.from(event.data).toString("utf8");
    }
    if (event.type === "stderr" && event.data) {
      stderr += Buffer.from(event.data).toString("utf8");
    }
    while (
      markerIndex < markerSteps.length &&
      stdout.includes(markerSteps[markerIndex].marker)
    ) {
      const { onMarker } = markerSteps[markerIndex];
      markerIndex += 1;
      await onMarker(handle);
    }
    if (event.type === "exit") {
      code = event.code ?? 0;
      break;
    }
    if (event.type === "timeout") {
      code = 124;
      break;
    }
  }
  if (markerIndex < markerSteps.length) {
    throw new Error(`${label} exited without marker ${markerSteps[markerIndex].marker}`);
  }
  return { code, stdout, stderr };
};

const collectExec = async (handle, label, marker, onMarker) =>
  collectExecAtMarkers(
    handle,
    label,
    marker === undefined ? [] : [{ marker, onMarker }],
  );

const startGuest = async (session, command, { timeoutSecs = 180, interactive = false } = {}) =>
  withTimeout(
    session.exec(`set -euo pipefail\n${command}`, {
      shell: "/bin/bash",
      closeStdinOnStart: !interactive,
      timeoutSecs,
    }),
    `start guest command: ${command.slice(0, 100)}`,
  );

const execGuest = async (session, command, timeoutSecs = 180) =>
  collectExec(
    await startGuest(session, command, { timeoutSecs }),
    command.slice(0, 100),
  );

const execGuestAtMarker = async (session, command, marker, onMarker, timeoutSecs = 180) =>
  collectExec(
    await startGuest(session, command, { timeoutSecs, interactive: true }),
    command.slice(0, 100),
    marker,
    onMarker,
  );

const execGuestAtMarkers = async (session, command, markerSteps, timeoutSecs = 180) =>
  collectExecAtMarkers(
    await startGuest(session, command, { timeoutSecs, interactive: true }),
    command.slice(0, 100),
    markerSteps,
  );

const results = [];
const check = async (id, name, body, { optional = false } = {}) => {
  if (selectedChecks && !selectedChecks.has(id)) {
    results.push({ id, name, status: "skip", durationMs: 0, detail: "not selected" });
    return;
  }
  const started = Date.now();
  process.stderr.write(`[vfs-fault] ${id}/8 ${name}...\n`);
  let result;
  try {
    const outcome = await body();
    result = {
      id,
      name,
      status: outcome.skip && optional ? "skip" : outcome.pass ? "pass" : "fail",
      durationMs: Date.now() - started,
      detail: outcome.detail,
    };
  } catch (error) {
    result = {
      id,
      name,
      status: "fail",
      durationMs: Date.now() - started,
      detail: error instanceof Error ? error.stack || error.message : String(error),
    };
  } finally {
    try {
      if (gatewayChild === null) await startGateway();
      if (gatewayMode !== "online") await setGatewayMode("online");
    } catch (error) {
      result = {
        id,
        name,
        status: "fail",
        durationMs: Date.now() - started,
        detail: `${result?.detail || ""}\ngateway restoration failed: ${
          error instanceof Error ? error.stack || error.message : String(error)
        }`.trim(),
      };
    }
  }
  results.push(result);
  process.stderr.write(
    `[vfs-fault] ${id}/8 ${results.at(-1).status} (${results.at(-1).durationMs} ms)\n`,
  );
};

const mount = {
  guestPath: mountPath,
  mountTag: `fault-${randomUUID().replaceAll("-", "").slice(0, 20)}`,
  readOnly: false,
  availability: "shared-storage",
  continuity: "restore-cross-node",
  backendProfile,
  vfsEndpoint: ownerEndpoint,
  vfsScopePath: scopePath,
};

let sandbox;
let first;
let second;
const sessions = [];
const cleanupErrors = [];

const createSession = async (suffix) => {
  const session = await withTimeout(
    sandbox.session({
      image: sandboxImage,
      architecture,
      name: `cv-vfs-fault-${suffix}-${Date.now()}`,
      metadata: { role: "chevalier-vfs-fault-recovery", probeId },
      autoStart: true,
      sharedMounts: [{ ...mount, mountTag: `${mount.mountTag}-${suffix}`.slice(0, 31) }],
    }),
    `create ${suffix} fault VM`,
    420_000,
  );
  sessions.push(session);
  for (let attempt = 0; attempt < 60; attempt += 1) {
    const ready = await execGuest(
      session,
      `test "$(findmnt -n -o FSTYPE ${mountPath})" = virtiofs &&
       printf ready >${mountPath}/.fault-ready-${suffix} &&
       rm ${mountPath}/.fault-ready-${suffix}`,
      30,
    ).catch(() => undefined);
    if (ready?.code === 0) return session;
    await delay(1_000);
  }
  throw new Error(`${suffix} VM did not expose a writable virtiofs mount`);
};

const waitForGuest = async (session, label) => {
  for (let attempt = 0; attempt < 90; attempt += 1) {
    const ready = await execGuest(
      session,
      `test "$(findmnt -n -o FSTYPE ${mountPath})" = virtiofs && echo READY`,
      20,
    ).catch(() => undefined);
    if (ready?.code === 0 && ready.stdout.includes("READY")) return;
    await delay(1_000);
  }
  throw new Error(`${label} did not recover a virtiofs mount`);
};

const runHook = async (command, label) =>
  withTimeout(
    new Promise((resolveHook, rejectHook) => {
      const child = spawn("/bin/bash", ["-lc", command], {
        env: process.env,
        stdio: ["ignore", "ignore", "pipe"],
      });
      let stderr = "";
      child.stderr.on("data", (chunk) => {
        stderr += Buffer.from(chunk).toString("utf8");
      });
      child.on("error", rejectHook);
      child.on("close", (code) => {
        if (code === 0) resolveHook();
        else rejectHook(new Error(`${label} failed with exit ${code}: ${stderr.slice(-1000)}`));
      });
    }),
    label,
    180_000,
  );

try {
  sandbox = await withTimeout(
    Sandbox.connect(sandboxEndpoint, {
      authToken: sandboxAuthToken,
      defaultImage: sandboxImage,
      defaultArchitecture: architecture,
      connectTimeoutMs: 300_000,
    }),
    "connect sandbox provider",
    300_000,
  );
  first = await createSession("a");
  second = await createSession("b");

  await check(1, "baseline fsync, close, rename, and unlink visibility barriers", async () => {
    const writer = await execGuest(
      first,
      `python3 - <<'PY'
import os
fd=os.open("/workspace/barrier-fsync", os.O_CREAT|os.O_EXCL|os.O_RDWR, 0o644)
os.write(fd,b"fsync-visible")
os.fsync(fd)
os.close(fd)
with open("/workspace/barrier-close","wb") as f:
    f.write(b"close-visible")
with open("/workspace/barrier-rename-source","wb") as f:
    f.write(b"rename-visible")
os.rename("/workspace/barrier-rename-source","/workspace/barrier-rename-target")
with open("/workspace/barrier-unlink","wb") as f:
    f.write(b"gone")
os.unlink("/workspace/barrier-unlink")
with open("/workspace/barrier-mode","wb") as f:
    f.write(b"mode-visible")
fd=os.open("/workspace/barrier-mode", os.O_RDWR)
os.fchmod(fd,0o755)
os.fsync(fd)
os.close(fd)
print("BASELINE_BARRIERS_OK")
PY`,
    );
    const executableReader = await execGuest(
      second,
      `test "$(cat /workspace/barrier-fsync)" = fsync-visible
test "$(cat /workspace/barrier-close)" = close-visible
test "$(cat /workspace/barrier-rename-target)" = rename-visible
test ! -e /workspace/barrier-rename-source
test ! -e /workspace/barrier-unlink
test -x /workspace/barrier-mode
echo BASELINE_CROSS_MOUNT_OK`,
    );
    const modeWriter = await execGuest(
      first,
      `python3 - <<'PY'
import os
fd=os.open("/workspace/barrier-mode", os.O_RDWR)
os.fchmod(fd,0o644)
os.fsync(fd)
os.close(fd)
print("MODE_CLEAR_OK")
PY`,
    );
    const nonExecutableReader = await execGuest(
      second,
      `test ! -x /workspace/barrier-mode
test "$(cat /workspace/barrier-mode)" = mode-visible
echo MODE_CLEAR_CROSS_MOUNT_OK`,
    );
    return {
      pass:
        writer.code === 0 &&
        executableReader.code === 0 &&
        modeWriter.code === 0 &&
        nonExecutableReader.code === 0,
      detail: [writer, executableReader, modeWriter, nonExecutableReader]
        .map(commandResultText)
        .join("\n"),
    };
  });

  await check(2, "transient 503 during fsync retries then commits exactly", async () => {
    await execGuest(first, "printf old >/workspace/fault-fsync");
    await setGatewayMode("reject");
    const writerPromise = execGuest(
      first,
      `python3 - <<'PY'
import os
fd=os.open("/workspace/fault-fsync", os.O_RDWR)
os.ftruncate(fd,0)
os.write(fd,b"fsync-after-recovery")
os.fsync(fd)
os.close(fd)
print("FSYNC_RECOVERED")
PY`,
      90,
    );
    await delay(1_500);
    await setGatewayMode("online");
    const writer = await writerPromise;
    const reader = await execGuest(
      second,
      `test "$(cat /workspace/fault-fsync)" = fsync-after-recovery && echo FSYNC_VISIBLE`,
    );
    return {
      pass: writer.code === 0 && reader.code === 0 && writer.stdout.includes("FSYNC_RECOVERED"),
      detail: [writer, reader].map(commandResultText).join("\n"),
    };
  });

  await check(3, "gateway process SIGKILL/restart during close barrier preserves bytes", async () => {
    await execGuest(first, "printf seed >/workspace/fault-close");
    const writer = await execGuestAtMarker(
      first,
      `python3 - <<'PY'
import os,sys
fd=os.open("/workspace/fault-close", os.O_RDWR)
print("CLOSE_FD_READY", flush=True)
sys.stdin.readline()
os.ftruncate(fd,0)
os.write(fd,b"close-after-restart")
os.close(fd)
print("CLOSE_RECOVERED")
PY`,
      "CLOSE_FD_READY",
      async (handle) => {
        await stopGateway({ injected: true });
        await handle.write(Buffer.from("go\n"));
        await delay(1_500);
        await startGateway();
        await handle.eof();
      },
      90,
    );
    const reader = await execGuest(
      second,
      `test "$(cat /workspace/fault-close)" = close-after-restart && echo CLOSE_VISIBLE`,
    );
    return {
      pass: writer.code === 0 && reader.code === 0 && writer.stdout.includes("CLOSE_RECOVERED"),
      detail: [writer, reader].map(commandResultText).join("\n"),
    };
  });

  await check(4, "paused gateway during rename and unlink preserves ordered namespace", async () => {
    await execGuest(
      first,
      `printf rename >/workspace/fault-rename-source
printf unlink >/workspace/fault-unlink`,
    );
    await setGatewayMode("paused");
    const mutationPromise = execGuest(
      first,
      `mv /workspace/fault-rename-source /workspace/fault-rename-target
rm /workspace/fault-unlink
echo NAMESPACE_RECOVERED`,
      90,
    );
    await delay(1_500);
    await setGatewayMode("online");
    const mutation = await mutationPromise;
    const reader = await execGuest(
      second,
      `test "$(cat /workspace/fault-rename-target)" = rename
test ! -e /workspace/fault-rename-source
test ! -e /workspace/fault-unlink
echo NAMESPACE_VISIBLE`,
    );
    return {
      pass:
        mutation.code === 0 &&
        reader.code === 0 &&
        mutation.stdout.includes("NAMESPACE_RECOVERED"),
      detail: [mutation, reader].map(commandResultText).join("\n"),
    };
  });

  await check(5, "terminal gateway outage fails fsync honestly then replays exact bytes", async () => {
    await execGuest(first, "printf stable >/workspace/fault-terminal");
    const writer = await execGuestAtMarkers(
      first,
      `python3 - <<'PY'
import os,sys
fd=os.open("/workspace/fault-terminal", os.O_RDWR)
os.ftruncate(fd,0)
os.write(fd,b"terminal-recovered")
print("DIRTY_FD_READY", flush=True)
sys.stdin.readline()
try:
    os.fsync(fd)
    print("FSYNC_WRONG_SUCCESS", flush=True)
    sys.exit(2)
except OSError as error:
    print("FSYNC_FAILED_HONESTLY:%s" % error.errno, flush=True)
sys.stdin.readline()
os.close(fd)
print("CLOSE_AFTER_RECOVERY")
PY`,
      [
        {
          marker: "DIRTY_FD_READY",
          onMarker: async (handle) => {
            await setGatewayMode("reject");
            await handle.write(Buffer.from("outage\n"));
          },
        },
        {
          marker: "FSYNC_FAILED_HONESTLY:",
          onMarker: async (handle) => {
            await setGatewayMode("online");
            await handle.write(Buffer.from("recover\n"));
            await handle.eof();
          },
        },
      ],
      120,
    );
    const reader = await execGuest(
      second,
      `test "$(cat /workspace/fault-terminal)" = terminal-recovered && echo TERMINAL_REPLAY_VISIBLE`,
      90,
    );
    return {
      pass:
        writer.code === 0 &&
        reader.code === 0 &&
        writer.stdout.includes("FSYNC_FAILED_HONESTLY:") &&
        !writer.stdout.includes("FSYNC_WRONG_SUCCESS"),
      detail: [writer, reader].map(commandResultText).join("\n"),
    };
  });

  await check(
    6,
    "session restart replaces virtiofsd during dirty data and namespace barriers",
    async () => {
    await execGuest(
      first,
      `printf virtiofsd-committed >/workspace/fault-virtiofsd
mkdir -p /workspace/fault-links/nested
printf symlink-target >/workspace/fault-links/target
ln -s ../target /workspace/fault-links/nested/relative
ln -s ../../missing-target /workspace/fault-links/nested/dangling`,
    );
    await execGuest(first, "printf dirty-old >/workspace/fault-dirty-restart");
    let restartState = "not-started";
    let interruptedExec = "";
    try {
      const dirty = await execGuestAtMarker(
        first,
        `python3 - <<'PY'
import os,sys
fd=os.open("/workspace/fault-dirty-restart", os.O_RDWR)
os.ftruncate(fd,0)
os.write(fd,b"dirty-new")
print("DIRTY_RESTART_READY", flush=True)
sys.stdin.readline()
os.fsync(fd)
os.close(fd)
PY`,
        "DIRTY_RESTART_READY",
        async () => {
          restartState = await withTimeout(
            first.restart(),
            "restart disposable VM with dirty descriptor",
            180_000,
          );
        },
        180,
      );
      interruptedExec = commandResultText(dirty);
    } catch (error) {
      interruptedExec = `expected interrupted exec: ${
        error instanceof Error ? error.message : String(error)
      }`;
    }
    await waitForGuest(first, "restarted VM");
    await execGuest(
      first,
      `printf namespace-restart >/workspace/fault-session-rename-source
rm -f /workspace/fault-session-rename-target`,
    );
    const pausedBefore = pausedRequestCount;
    await setGatewayMode("paused");
    const namespaceHandle = await startGuest(
      first,
      `mv /workspace/fault-session-rename-source /workspace/fault-session-rename-target`,
      { timeoutSecs: 180 },
    );
    const namespaceExecPromise = collectExec(namespaceHandle, "session restart rename").catch(
      (error) => ({
        code: null,
        stdout: "",
        stderr: `expected interrupted rename: ${
          error instanceof Error ? error.message : String(error)
        }`,
      }),
    );
    for (
      let attempt = 0;
      attempt < 100 && pausedRequestCount === pausedBefore;
      attempt += 1
    ) {
      await delay(50);
    }
    if (pausedRequestCount === pausedBefore) {
      await setGatewayMode("online");
      throw new Error("rename never reached the paused gateway before session restart");
    }
    const namespaceRestartPromise = first.restart();
    await delay(750);
    await setGatewayMode("online");
    const namespaceRestartState = await withTimeout(
      namespaceRestartPromise,
      "restart disposable VM during namespace barrier",
      180_000,
    );
    const namespaceExec = await namespaceExecPromise;
    await waitForGuest(first, "namespace-restarted VM");
    const reader = await execGuest(
      first,
      `python3 - <<'PY'
import os,stat
relative="/workspace/fault-links/nested/relative"
dangling="/workspace/fault-links/nested/dangling"
assert stat.S_ISLNK(os.lstat(relative).st_mode)
assert os.readlink(relative) == "../target"
assert open(relative,"rb").read() == b"symlink-target"
assert stat.S_ISLNK(os.lstat(dangling).st_mode)
assert os.readlink(dangling) == "../../missing-target"
assert os.path.lexists(dangling) and not os.path.exists(dangling)
dirty=open("/workspace/fault-dirty-restart","rb").read()
assert dirty in (b"dirty-old", b"dirty-new"), dirty
source=os.path.exists("/workspace/fault-session-rename-source")
target=os.path.exists("/workspace/fault-session-rename-target")
assert source != target, (source,target)
path="/workspace/fault-session-rename-source" if source else "/workspace/fault-session-rename-target"
assert open(path,"rb").read() == b"namespace-restart"
print("SYMLINKS_RECONNECTED")
PY
test "$(cat /workspace/fault-virtiofsd)" = virtiofsd-committed
printf dirty-normalized >/workspace/fault-dirty-restart
printf virtiofsd-after-restart >/workspace/fault-virtiofsd
sync
echo VIRTIOFSD_RECONNECTED`,
    );
    const cross = await execGuest(
      second,
      `test "$(cat /workspace/fault-virtiofsd)" = virtiofsd-after-restart
test "$(cat /workspace/fault-dirty-restart)" = dirty-normalized`,
    );
    return {
      pass:
        restartState !== "not-started" &&
        namespaceRestartState !== "" &&
        reader.code === 0 &&
        cross.code === 0,
      detail: `dirty_restart=${restartState}\n${interruptedExec}\nnamespace_restart=${namespaceRestartState}\n${commandResultText(namespaceExec)}\n${commandResultText(reader)}`,
    };
    },
  );

  await check(7, "replacement VM recovers exact committed state", async () => {
    await execGuest(first, "printf replacement-exact >/workspace/fault-replacement");
    const old = first;
    await old.discard();
    const oldIndex = sessions.indexOf(old);
    if (oldIndex >= 0) sessions.splice(oldIndex, 1);
    first = await createSession("replacement");
    const reader = await execGuest(
      first,
      `test "$(cat /workspace/fault-replacement)" = replacement-exact
python3 - <<'PY'
import os,stat
relative="/workspace/fault-links/nested/relative"
dangling="/workspace/fault-links/nested/dangling"
assert stat.S_ISLNK(os.lstat(relative).st_mode)
assert os.readlink(relative) == "../target"
assert open(relative,"rb").read() == b"symlink-target"
assert stat.S_ISLNK(os.lstat(dangling).st_mode)
assert os.readlink(dangling) == "../../missing-target"
assert os.path.lexists(dangling) and not os.path.exists(dangling)
PY
echo REPLACEMENT_EXACT`,
    );
    return {
      pass: reader.code === 0 && reader.stdout.includes("REPLACEMENT_EXACT"),
      detail: commandResultText(reader),
    };
  });

  await check(
    8,
    "isolated vmd process restart preserves committed scope and remounts",
    async () => {
      const restartCommand = process.env.CHEVALIER_VFS_FAULT_VMD_RESTART_COMMAND?.trim();
      if (!restartCommand) {
        return {
          skip: true,
          pass: false,
          detail: "CHEVALIER_VFS_FAULT_VMD_RESTART_COMMAND not set",
        };
      }
      await execGuest(
        first,
        `printf vmd-restart-exact >/workspace/fault-vmd
printf vmd-dirty-old >/workspace/fault-vmd-dirty
printf vmd-rename-exact >/workspace/fault-vmd-rename-source
rm -f /workspace/fault-vmd-rename-target
mkdir -p /workspace/fault-links/nested
printf symlink-target >/workspace/fault-links/target
ln -sfn ../target /workspace/fault-links/nested/relative
ln -sfn ../../missing-target /workspace/fault-links/nested/dangling
python3 - <<'PY'
import os
for path in ("/workspace/fault-vmd-mode-plus","/workspace/fault-vmd-mode-minus"):
    with open(path,"wb") as f:
        f.write(b"mode-restart")
    fd=os.open(path,os.O_RDWR)
    os.fchmod(fd,0o755)
    os.fsync(fd)
    os.close(fd)
fd=os.open("/workspace/fault-vmd-mode-minus",os.O_RDWR)
os.fchmod(fd,0o644)
os.fsync(fd)
os.close(fd)
PY`,
      );
      const sessionId = first.sessionId;
      let interruptedExec = "";
      try {
        const interrupted = await execGuestAtMarker(
          first,
          `python3 - <<'PY'
import os,sys
fd=os.open("/workspace/fault-vmd-dirty", os.O_RDWR)
os.ftruncate(fd,0)
os.write(fd,b"vmd-dirty-new")
print("VMD_DIRTY_READY", flush=True)
sys.stdin.readline()
os.rename("/workspace/fault-vmd-rename-source","/workspace/fault-vmd-rename-target")
os.fsync(fd)
os.close(fd)
PY`,
          "VMD_DIRTY_READY",
          async (handle) => {
            await setGatewayMode("paused");
            await handle.write(Buffer.from("mutate\n"));
            await delay(750);
            try {
              await runHook(restartCommand, "isolated vmd restart hook");
            } finally {
              await setGatewayMode("online");
            }
          },
          240,
        );
        interruptedExec = commandResultText(interrupted);
      } catch (error) {
        if (gatewayChild !== null) await setGatewayMode("online");
        interruptedExec = `expected interrupted exec: ${
          error instanceof Error ? error.message : String(error)
        }`;
      }
      await first.close().catch(() => undefined);
      const currentIndex = sessions.indexOf(first);
      if (currentIndex >= 0) sessions.splice(currentIndex, 1);
      sandbox = null;
      for (let attempt = 0; attempt < 90; attempt += 1) {
        try {
          sandbox = await Sandbox.connect(sandboxEndpoint, {
            authToken: sandboxAuthToken,
            defaultImage: sandboxImage,
            defaultArchitecture: architecture,
            connectTimeoutMs: 30_000,
          });
          break;
        } catch {
          await delay(1_000);
        }
      }
      if (!sandbox) throw new Error("sandbox provider did not reconnect after vmd restart");
      first = await withTimeout(
        sandbox.attachSession(sessionId),
        "reattach disposable session after vmd restart",
        180_000,
      );
      sessions.push(first);
      const state = await first.getState();
      if (state === "paused") await first.resume();
      else if (state !== "running") await first.start();
      await waitForGuest(first, "vmd-restarted VM");
      const reader = await execGuest(
        first,
        `test "$(cat /workspace/fault-vmd)" = vmd-restart-exact
python3 - <<'PY'
import os,stat
relative="/workspace/fault-links/nested/relative"
dangling="/workspace/fault-links/nested/dangling"
assert stat.S_ISLNK(os.lstat(relative).st_mode)
assert os.readlink(relative) == "../target"
assert open(relative,"rb").read() == b"symlink-target"
assert stat.S_ISLNK(os.lstat(dangling).st_mode)
assert os.readlink(dangling) == "../../missing-target"
assert os.path.lexists(dangling) and not os.path.exists(dangling)
assert os.stat("/workspace/fault-vmd-mode-plus").st_mode & 0o111
assert not (os.stat("/workspace/fault-vmd-mode-minus").st_mode & 0o111)
dirty=open("/workspace/fault-vmd-dirty","rb").read()
assert dirty in (b"vmd-dirty-old", b"vmd-dirty-new"), dirty
source=os.path.exists("/workspace/fault-vmd-rename-source")
target=os.path.exists("/workspace/fault-vmd-rename-target")
assert source != target, (source,target)
path="/workspace/fault-vmd-rename-source" if source else "/workspace/fault-vmd-rename-target"
assert open(path,"rb").read() == b"vmd-rename-exact"
PY
echo VMD_RESTART_EXACT`,
      );
      return {
        pass: reader.code === 0 && reader.stdout.includes("VMD_RESTART_EXACT"),
        detail: `reattached_state=${state}\n${interruptedExec}\n${commandResultText(reader)}`,
      };
    },
    { optional: true },
  );
} finally {
  if (gatewayChild !== null) {
    await setGatewayMode("online").catch(() => undefined);
  }
  for (const session of [...sessions].reverse()) {
    try {
      await session.discard();
    } catch (error) {
      cleanupErrors.push(
        `discard ${session.sessionId}: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  try {
    await stopGateway();
  } catch (error) {
    cleanupErrors.push(`gateway stop: ${error instanceof Error ? error.message : String(error)}`);
  }
  try {
    await rm(backingRoot, { recursive: true, force: true });
  } catch (error) {
    cleanupErrors.push(
      `backing root cleanup: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

if (cleanupErrors.length > 0) {
  results.push({
    id: "cleanup",
    name: "disposable resource cleanup",
    status: "fail",
    durationMs: 0,
    detail: cleanupErrors.join("\n"),
  });
}

const failed = results.filter((result) => result.status === "fail");
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      probeId,
      ownerId,
      scopePath,
      sandboxEndpoint,
      gatewayPublicUrl,
      ownerEndpoint,
      gatewayRequests,
      gatewayStarts,
      gatewayProcessKills,
      injectedFailures,
      summary: {
        passed: results.filter((result) => result.status === "pass").length,
        failed: failed.length,
        skipped: results.filter((result) => result.status === "skip").length,
      },
      cleanup: {
        ok: cleanupErrors.length === 0,
        errors: cleanupErrors,
        backingRoot,
      },
      results,
    },
    null,
    2,
  ),
);
if (failed.length > 0) process.exitCode = 1;
