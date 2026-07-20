#!/usr/bin/env node

import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdir, readdir, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");

if (process.argv.includes("--help")) {
  console.log(`Disposable VFS/virtiofsd lifecycle and resource-leak torture harness.

Required:
  SANDBOX_ENDPOINT
  SANDBOX_IMAGE
  CHEVALIER_VFS_LIFECYCLE_GATEWAY_PUBLIC_URL
  CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN
    (SANDBOX_AUTH_TOKEN is accepted as a fallback)

Optional:
  SANDBOX_AUTH_TOKEN
  SANDBOX_ARCHITECTURE=amd64
  CHEVALIER_VFS_LIFECYCLE_CYCLES=8
  CHEVALIER_VFS_LIFECYCLE_GATEWAY_BIND=0.0.0.0
  CHEVALIER_VFS_LIFECYCLE_GATEWAY_PORT=19094
  CHEVALIER_VFS_LIFECYCLE_BACKEND_PROFILE=openbracket-vfs-fuse
  CHEVALIER_VFS_LIFECYCLE_COMMAND_TIMEOUT_MS=300000
  CHEVALIER_VFS_LIFECYCLE_CLEANUP_TIMEOUT_MS=45000
  CHEVALIER_VFS_LIFECYCLE_TMPDIR=/tmp
  CHEVALIER_MODULE_PATH=<repo>/ts/index.js
  CHEVALIER_SANDBOX_MODULE_PATH=<repo>/ts-sandbox/index.js

Optional bismuth host observer:
  CHEVALIER_VFS_LIFECYCLE_OBSERVER_SSH_HOST=bismuth
  CHEVALIER_VFS_LIFECYCLE_OBSERVER_CONTAINER=openbracket-vmd
  CHEVALIER_VFS_LIFECYCLE_OBSERVER_DATA_ROOT=/home/crow/.openbracket-vmd-bismuth/vms
  CHEVALIER_VFS_LIFECYCLE_MAX_VMD_FD_GROWTH=4
  CHEVALIER_VFS_LIFECYCLE_MAX_VMD_THREAD_GROWTH=4
  CHEVALIER_VFS_LIFECYCLE_MAX_VMD_RSS_GROWTH_KIB=65536
  CHEVALIER_VFS_LIFECYCLE_MAX_VMD_RSS_SLOPE_KIB=8192
  CHEVALIER_VFS_LIFECYCLE_MAX_LOCAL_FD_GROWTH=8
  CHEVALIER_VFS_LIFECYCLE_MAX_LOCAL_RSS_GROWTH_KIB=65536
  CHEVALIER_VFS_LIFECYCLE_MAX_LOCAL_RSS_SLOPE_KIB=8192

Every cycle starts and stops the callback listener, creates one disposable
mounted VM, exercises acknowledged writes and namespace mutations, holds both
flock and POSIX locks, and discards the VM while the lock process is alive.
The result is one JSON document containing per-cycle session, lock, process,
mount, journal/temp, FD, RSS, and cleanup observations. The harness only
deletes sessions whose generated name includes its random probe id.`);
  process.exit(0);
}

const required = (name) => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required; run with --help for the complete contract`);
  return value;
};

const positiveInteger = (name, fallback, minimum = 1) => {
  const raw = process.env[name]?.trim() || String(fallback);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new Error(`${name} must be an integer >= ${minimum}`);
  }
  return value;
};

const sandboxEndpoint = required("SANDBOX_ENDPOINT");
const sandboxImage = required("SANDBOX_IMAGE");
const gatewayPublicUrl = required("CHEVALIER_VFS_LIFECYCLE_GATEWAY_PUBLIC_URL").replace(
  /\/+$/,
  "",
);
const sandboxAuthToken = process.env.SANDBOX_AUTH_TOKEN?.trim();
const vfsAuthToken =
  process.env.CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN?.trim() || sandboxAuthToken;
if (!vfsAuthToken) {
  throw new Error(
    "CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN (or SANDBOX_AUTH_TOKEN) is required",
  );
}

const architecture = process.env.SANDBOX_ARCHITECTURE?.trim() || "amd64";
const cycles = positiveInteger("CHEVALIER_VFS_LIFECYCLE_CYCLES", 8, 2);
const gatewayBind =
  process.env.CHEVALIER_VFS_LIFECYCLE_GATEWAY_BIND?.trim() || "0.0.0.0";
const gatewayPort = positiveInteger("CHEVALIER_VFS_LIFECYCLE_GATEWAY_PORT", 19094);
if (gatewayPort > 65_535) {
  throw new Error("CHEVALIER_VFS_LIFECYCLE_GATEWAY_PORT must be <= 65535");
}
const backendProfile =
  process.env.CHEVALIER_VFS_LIFECYCLE_BACKEND_PROFILE?.trim() ||
  "openbracket-vfs-fuse";
const commandTimeoutMs = positiveInteger(
  "CHEVALIER_VFS_LIFECYCLE_COMMAND_TIMEOUT_MS",
  300_000,
  1_000,
);
const cleanupTimeoutMs = positiveInteger(
  "CHEVALIER_VFS_LIFECYCLE_CLEANUP_TIMEOUT_MS",
  45_000,
  5_000,
);
const observerSshHost = process.env.CHEVALIER_VFS_LIFECYCLE_OBSERVER_SSH_HOST?.trim();
const observerContainer =
  process.env.CHEVALIER_VFS_LIFECYCLE_OBSERVER_CONTAINER?.trim() || "openbracket-vmd";
const observerDataRoot =
  process.env.CHEVALIER_VFS_LIFECYCLE_OBSERVER_DATA_ROOT?.trim() ||
  "/home/crow/.openbracket-vmd-bismuth/vms";
const maxVmdFdGrowth = positiveInteger(
  "CHEVALIER_VFS_LIFECYCLE_MAX_VMD_FD_GROWTH",
  4,
  0,
);
const maxVmdThreadGrowth = positiveInteger(
  "CHEVALIER_VFS_LIFECYCLE_MAX_VMD_THREAD_GROWTH",
  4,
  0,
);
const maxVmdRssGrowthKib = positiveInteger(
  "CHEVALIER_VFS_LIFECYCLE_MAX_VMD_RSS_GROWTH_KIB",
  65_536,
  0,
);
const maxVmdRssSlopeKib = positiveInteger(
  "CHEVALIER_VFS_LIFECYCLE_MAX_VMD_RSS_SLOPE_KIB",
  8_192,
  0,
);
const maxLocalFdGrowth = positiveInteger(
  "CHEVALIER_VFS_LIFECYCLE_MAX_LOCAL_FD_GROWTH",
  8,
  0,
);
const maxLocalRssGrowthKib = positiveInteger(
  "CHEVALIER_VFS_LIFECYCLE_MAX_LOCAL_RSS_GROWTH_KIB",
  65_536,
  0,
);
const maxLocalRssSlopeKib = positiveInteger(
  "CHEVALIER_VFS_LIFECYCLE_MAX_LOCAL_RSS_SLOPE_KIB",
  8_192,
  0,
);

const require = createRequire(import.meta.url);
const chevalierPath =
  process.env.CHEVALIER_MODULE_PATH?.trim() || join(repoRoot, "ts", "index.js");
const sandboxModulePath =
  process.env.CHEVALIER_SANDBOX_MODULE_PATH?.trim() ||
  join(repoRoot, "ts-sandbox", "index.js");
let chevalier;
let sandboxModule;
try {
  chevalier = require(resolve(chevalierPath));
  sandboxModule = require(resolve(sandboxModulePath));
} catch (error) {
  throw new Error(
    `load native Chevalier modules failed; build host-native ts and ts-sandbox bindings or set CHEVALIER_MODULE_PATH/CHEVALIER_SANDBOX_MODULE_PATH: ${error}`,
  );
}
const { createVfsGatewayServer, VfsStorage } = chevalier;
const { Sandbox } = sandboxModule;
if (
  typeof createVfsGatewayServer !== "function" ||
  typeof VfsStorage?.local !== "function" ||
  typeof Sandbox?.connect !== "function"
) {
  throw new Error(
    "loaded modules do not expose createVfsGatewayServer, VfsStorage.local, and Sandbox.connect",
  );
}

const probeId = `lifecycle-${Date.now()}-${randomUUID().slice(0, 8)}`;
const sessionNamePrefix = `cv-vfs-life-${probeId}`;
const backingRoot = join(
  process.env.CHEVALIER_VFS_LIFECYCLE_TMPDIR?.trim() || "/tmp",
  `chevalier-${probeId}`,
);
const mountPath = "/workspace";

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

const poll = async (label, body, timeoutMs = cleanupTimeoutMs, intervalMs = 100) => {
  const deadline = Date.now() + timeoutMs;
  let last;
  for (;;) {
    last = await body();
    if (last?.done) return last.value;
    if (Date.now() >= deadline) {
      throw new Error(`${label} did not converge within ${timeoutMs}ms: ${JSON.stringify(last)}`);
    }
    await delay(intervalMs);
  }
};

const readRequestBody = async (request) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
};

class InspectableAdvisoryLockState {
  byOwner = new Map();
  tails = new Map();

  async transact(ownerId, transaction) {
    const previous = this.tails.get(ownerId) || Promise.resolve();
    const current = previous
      .catch(() => undefined)
      .then(() => {
        const outcome = transaction([...(this.byOwner.get(ownerId) || [])]);
        this.byOwner.set(ownerId, [...outcome.locks]);
        return outcome.result;
      });
    this.tails.set(ownerId, current);
    try {
      return await current;
    } finally {
      if (this.tails.get(ownerId) === current) this.tails.delete(ownerId);
    }
  }

  rows(ownerId) {
    return [...(this.byOwner.get(ownerId) || [])].map((row) => ({
      ownerId: row.ownerId,
      mountId: row.mountId,
      lockOwner: row.lockOwner,
      namespace: row.namespace,
      fileId: row.fileId,
      start: String(row.start),
      end: String(row.end),
      kind: row.kind,
      pid: row.pid,
      expiresAt: row.expiresAt,
    }));
  }

  totalRows() {
    return [...this.byOwner.values()].reduce((sum, rows) => sum + rows.length, 0);
  }

  deleteOwner(ownerId) {
    this.byOwner.delete(ownerId);
    this.tails.delete(ownerId);
  }
}

const lockState = new InspectableAdvisoryLockState();
const stores = new Map();
const requestCounts = new Map();
const handler = createVfsGatewayServer({
  resolveStore: async (ownerId) => {
    const store = stores.get(ownerId);
    if (!store) throw new Error(`unknown disposable lifecycle owner ${ownerId}`);
    return store;
  },
  authToken: vfsAuthToken,
  allowGitMetadata: async (ownerId) => stores.has(ownerId),
  advisoryLockState: lockState,
});

const gatewayServer = createServer(async (incoming, outgoing) => {
  try {
    const method = incoming.method || "GET";
    const body =
      method === "GET" || method === "HEAD" ? undefined : await readRequestBody(incoming);
    const url = new URL(incoming.url || "/", `http://${incoming.headers.host || "localhost"}`);
    const ownerMatch = url.pathname.match(/^\/internal\/chevalier\/vfs\/([^/]+)/);
    const ownerId = ownerMatch ? decodeURIComponent(ownerMatch[1]) : "unknown";
    requestCounts.set(ownerId, (requestCounts.get(ownerId) || 0) + 1);
    const request = new Request(url, {
      method,
      headers: incoming.headers,
      body,
      ...(body === undefined ? {} : { duplex: "half" }),
    });
    const response = await handler(request);
    outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
    outgoing.end(Buffer.from(await response.arrayBuffer()));
  } catch (error) {
    outgoing.writeHead(500, { "content-type": "text/plain" });
    outgoing.end(error instanceof Error ? error.stack || error.message : String(error));
  }
});

let gatewayListening = false;
const startGateway = async () => {
  if (gatewayListening) return;
  await withTimeout(
    new Promise((resolveListen, rejectListen) => {
      const onError = (error) => {
        gatewayServer.off("listening", onListening);
        rejectListen(error);
      };
      const onListening = () => {
        gatewayServer.off("error", onError);
        resolveListen();
      };
      gatewayServer.once("error", onError);
      gatewayServer.once("listening", onListening);
      gatewayServer.listen(gatewayPort, gatewayBind);
    }),
    "start lifecycle callback gateway",
    10_000,
  );
  gatewayListening = true;
};

const stopGateway = async () => {
  if (!gatewayListening) return;
  await withTimeout(
    new Promise((resolveClose, rejectClose) => {
      gatewayServer.close((error) => (error ? rejectClose(error) : resolveClose()));
      gatewayServer.closeAllConnections?.();
    }),
    "stop lifecycle callback gateway",
    10_000,
  );
  gatewayListening = false;
};

const drainExec = async (handle, label, timeoutMs = commandTimeoutMs) => {
  let code = null;
  let stdout = "";
  let stderr = "";
  for (;;) {
    const event = await withTimeout(handle.next(), `${label} output`, timeoutMs);
    if (event === null) break;
    if (event.type === "stdout" && event.data) stdout += Buffer.from(event.data).toString("utf8");
    if (event.type === "stderr" && event.data) stderr += Buffer.from(event.data).toString("utf8");
    if (event.type === "exit") {
      code = event.code ?? 0;
      break;
    }
    if (event.type === "timeout") {
      code = 124;
      break;
    }
  }
  return { code, stdout, stderr };
};

const startGuest = async (session, command, timeoutSecs = 300) =>
  withTimeout(
    session.exec(`set -euo pipefail\n${command}`, {
      shell: "/bin/bash",
      closeStdinOnStart: true,
      timeoutSecs,
    }),
    `start guest command: ${command.slice(0, 100)}`,
  );

const execGuest = async (session, command, timeoutSecs = 300) =>
  drainExec(
    await startGuest(session, command, timeoutSecs),
    command.slice(0, 100),
  );

const waitForExecMarker = async (handle, marker, label) => {
  let stdout = "";
  let stderr = "";
  for (;;) {
    const event = await withTimeout(handle.next(), `${label} marker`, commandTimeoutMs);
    if (event === null) throw new Error(`${label} ended before ${marker}: ${stdout}\n${stderr}`);
    if (event.type === "stdout" && event.data) {
      stdout += Buffer.from(event.data).toString("utf8");
      if (stdout.includes(marker)) return { stdout, stderr };
    }
    if (event.type === "stderr" && event.data) stderr += Buffer.from(event.data).toString("utf8");
    if (event.type === "exit" || event.type === "timeout") {
      throw new Error(`${label} ended before ${marker}: ${stdout}\n${stderr}`);
    }
  }
};

const selfResourceSnapshot = async () => {
  const candidates = [`/proc/${process.pid}/fd`, "/dev/fd"];
  let fdCount = null;
  for (const candidate of candidates) {
    try {
      fdCount = (await readdir(candidate)).length;
      break;
    } catch {
      // Try the next platform-specific descriptor filesystem.
    }
  }
  const memory = process.memoryUsage();
  return {
    fdCount,
    rssBytes: memory.rss,
    heapUsedBytes: memory.heapUsed,
    externalBytes: memory.external,
  };
};

const remoteObserverPython = String.raw`
import json
import base64
import os
import pathlib
import subprocess
import sys

container, data_root, probe_id, encoded_ids = sys.argv[1:5]
encoded_ids += "=" * (-len(encoded_ids) % 4)
vm_ids = json.loads(base64.urlsafe_b64decode(encoded_ids).decode("utf-8"))

def run(args):
    completed = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if completed.returncode != 0:
        raise RuntimeError("command failed: %r\n%s" % (args, completed.stderr))
    return completed.stdout

def docker(*args):
    return run(["docker", "exec", container, *args])

status = docker("cat", "/proc/1/status")
rss_kib = 0
thread_count = 0
for line in status.splitlines():
    if line.startswith("VmRSS:"):
        rss_kib = int(line.split()[1])
    elif line.startswith("Threads:"):
        thread_count = int(line.split()[1])
fd_count = int(docker("sh", "-c", 'set -- /proc/1/fd/*; if [ ! -e "$1" ]; then echo 0; else echo "$#"; fi').strip())
process_text = docker("ps", "-eo", "pid=,ppid=,state=,comm=,args=")
process_lines = [line for line in process_text.splitlines() if line.strip()]
zombies = [line for line in process_lines if len(line.split()) >= 3 and line.split()[2].startswith("Z")]
virtiofsd = [line for line in process_lines if "virtiofsd" in line and "ps -eo" not in line]
qemu = [line for line in process_lines if "qemu-system" in line]
probe_processes = [line for line in process_lines if probe_id in line or any(vm_id in line for vm_id in vm_ids)]
probe_virtiofsd = [line for line in probe_processes if "virtiofsd" in line]
probe_qemu = [line for line in probe_processes if "qemu-system" in line]

mount_text = docker("findmnt", "-rn", "-o", "TARGET,FSTYPE")
mount_lines = [line for line in mount_text.splitlines() if line.strip()]
fuse_mounts = [line for line in mount_lines if len(line.split()) >= 2 and line.split()[-1].startswith("fuse")]
probe_mounts = [line for line in fuse_mounts if any(vm_id in line for vm_id in vm_ids)]

data_root_path = pathlib.Path(data_root)
existing_vm_dirs = []
artifact_paths = []
for vm_id in vm_ids:
    vm_dir = data_root_path / vm_id
    if vm_dir.exists():
        existing_vm_dirs.append(str(vm_dir))
        for root, dirs, files in os.walk(vm_dir):
            for name in files:
                if (name.endswith(".jsonl") or name.endswith(".tmp") or
                    name.endswith(".pid") or name.endswith(".sock")):
                    artifact_paths.append(str(pathlib.Path(root) / name))
            for name in dirs:
                if name.endswith(".writes") or name.endswith(".dead-letter"):
                    artifact_paths.append(str(pathlib.Path(root) / name))

runtime_existing = []
runtime_artifacts = []
for vm_id in vm_ids:
    runtime_dir = "/tmp/chevalier-vmd/" + vm_id
    test = subprocess.run(
        ["docker", "exec", container, "test", "-d", runtime_dir],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if test.returncode == 0:
        runtime_existing.append(runtime_dir)
        found = docker("find", runtime_dir, "-maxdepth", "2", "-type", "s", "-o", "-name", "*.pid")
        runtime_artifacts.extend([line for line in found.splitlines() if line.strip()])

print(json.dumps({
    "vmdFdCount": fd_count,
    "vmdThreadCount": thread_count,
    "vmdRssKib": rss_kib,
    "containerZombieCount": len(zombies),
    "containerZombies": zombies,
    "virtiofsdCount": len(virtiofsd),
    "qemuCount": len(qemu),
    "fuseMountCount": len(fuse_mounts),
    "probeProcessCount": len(probe_processes),
    "probeProcesses": probe_processes,
    "probeVirtiofsdCount": len(probe_virtiofsd),
    "probeQemuCount": len(probe_qemu),
    "probeFuseMountCount": len(probe_mounts),
    "probeFuseMounts": probe_mounts,
    "probeVmDirsExisting": existing_vm_dirs,
    "probeRuntimeDirsExisting": runtime_existing,
    "probeArtifactPaths": artifact_paths,
    "probeRuntimeArtifacts": runtime_artifacts,
}))
`;

const spawnCapture = async (command, args, input, label, timeoutMs = commandTimeoutMs) => {
  const child = spawn(command, args, { stdio: ["pipe", "pipe", "pipe"] });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += String(chunk);
  });
  child.stderr.on("data", (chunk) => {
    stderr += String(chunk);
  });
  child.stdin.end(input);
  const code = await withTimeout(
    new Promise((resolveExit, rejectExit) => {
      child.once("error", rejectExit);
      child.once("exit", resolveExit);
    }),
    label,
    timeoutMs,
  ).catch((error) => {
    child.kill("SIGKILL");
    throw error;
  });
  if (code !== 0) {
    throw new Error(`${label} exited ${String(code)}\n${stderr}\n${stdout}`);
  }
  return stdout;
};

const observeHost = async (vmIds = []) => {
  if (!observerSshHost) return null;
  const stdout = await spawnCapture(
    "ssh",
    [
      "-o",
      "BatchMode=yes",
      observerSshHost,
      "python3",
      "-",
      observerContainer,
      observerDataRoot,
      probeId,
      Buffer.from(JSON.stringify(vmIds), "utf8").toString("base64url"),
    ],
    remoteObserverPython,
    "remote lifecycle observer",
    30_000,
  );
  return JSON.parse(stdout);
};

const slope = (values) => {
  if (values.length < 2) return 0;
  const xMean = (values.length - 1) / 2;
  const yMean = values.reduce((sum, value) => sum + value, 0) / values.length;
  let numerator = 0;
  let denominator = 0;
  for (let index = 0; index < values.length; index += 1) {
    numerator += (index - xMean) * (values[index] - yMean);
    denominator += (index - xMean) ** 2;
  }
  return denominator === 0 ? 0 : numerator / denominator;
};

const results = [];
const failures = [];
const activeSessions = new Map();
const pendingCreations = new Map();
const lateCreatedSessions = new Map();
const providerCleanupDiagnostics = [];
const cycleEvidence = [];
let sandbox;
let observerBaseline = null;
let localBaseline = null;
let finalObserver = null;
let finalLocal = null;
let backingRootRemoved = false;
let cleanupSessions = [];

const fail = (name, detail) => {
  failures.push({ name, detail });
};

const listSessionsBounded = async (label) => {
  if (!sandbox) throw new Error(`${label}: sandbox provider is not connected`);
  return withTimeout(sandbox.listSessions(), label, cleanupTimeoutMs);
};

const discardSessionBounded = async (session, label) => {
  try {
    await withTimeout(session.discard(), `${label} through session handle`, cleanupTimeoutMs);
    return;
  } catch (handleError) {
    if (!sandbox || !session.sessionId) throw handleError;
    try {
      await withTimeout(
        sandbox.discardSessionById(session.sessionId),
        `${label} by session id`,
        cleanupTimeoutMs,
      );
      providerCleanupDiagnostics.push({
        label,
        sessionId: session.sessionId,
        fallback: "discardSessionById",
        handleError: handleError instanceof Error ? handleError.message : String(handleError),
        status: "pass",
      });
    } catch (fallbackError) {
      throw new Error(
        `${label} failed through handle (${handleError instanceof Error ? handleError.message : String(handleError)}) and id fallback (${fallbackError instanceof Error ? fallbackError.message : String(fallbackError)})`,
      );
    }
  }
};

const createSessionBounded = async (options, label) => {
  let cleanupIfLate = false;
  const creation = sandbox.session(options);
  const settlement = creation.then(async (created) => {
    if (!cleanupIfLate) return created;
    const diagnostic = {
      label,
      sessionId: created.sessionId,
      vmId: created.vmId,
      name: options.name,
      status: "cleanup-pending",
      error: null,
    };
    lateCreatedSessions.set(created.sessionId, diagnostic);
    try {
      await discardSessionBounded(created, `${label} late-settlement cleanup`);
      diagnostic.status = "cleaned";
    } catch (error) {
      diagnostic.status = "retained";
      diagnostic.error = error instanceof Error ? error.message : String(error);
      fail("late-created provider session cleanup", JSON.stringify(diagnostic));
    }
    return created;
  });
  pendingCreations.set(options.name, settlement);
  settlement
    .finally(() => {
      if (pendingCreations.get(options.name) === settlement) {
        pendingCreations.delete(options.name);
      }
    })
    .catch(() => undefined);
  try {
    return await withTimeout(creation, label, 420_000);
  } catch (error) {
    cleanupIfLate = true;
    throw error;
  }
};

try {
  await mkdir(backingRoot, { recursive: true });
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
  localBaseline = await selfResourceSnapshot();
  observerBaseline = await observeHost([]);

  const inheritedProbeSessions = (await listSessionsBounded(
    "list inherited lifecycle probe sessions",
  )).filter((entry) =>
    entry.name.includes(probeId),
  );
  if (inheritedProbeSessions.length !== 0) {
    throw new Error(`fresh probe id unexpectedly matched sessions: ${JSON.stringify(inheritedProbeSessions)}`);
  }

  for (let cycle = 1; cycle <= cycles; cycle += 1) {
    const startedAt = Date.now();
    const suffix = `${cycle}-${randomUUID().slice(0, 6)}`;
    const ownerId = `${sessionNamePrefix}-owner-${suffix}`;
    const scopePath = `probes/${probeId}/cycle-${cycle}`;
    const ownerRoot = join(backingRoot, ownerId);
    const ownerEndpoint = `${gatewayPublicUrl}/internal/chevalier/vfs/${encodeURIComponent(ownerId)}`;
    const mountTag = `life-${cycle}-${randomUUID().replaceAll("-", "").slice(0, 16)}`.slice(
      0,
      31,
    );
    const sessionName = `${sessionNamePrefix}-${suffix}`;
    const evidence = {
      cycle,
      ownerId,
      scopePath,
      sessionName,
      sessionId: null,
      vmId: null,
      requestCountBefore: requestCounts.get(ownerId) || 0,
      requestCountAfter: 0,
      lockRowsActive: [],
      lockRowsAfterDiscard: [],
      activeObserver: null,
      cleanupObserver: null,
      localAfterCleanup: null,
      providerSessionRemoved: false,
      ownerRootRemoved: false,
      status: "fail",
      durationMs: 0,
      error: null,
    };
    let session;
    let lockHandle;
    try {
      await mkdir(ownerRoot, { recursive: true });
      stores.set(ownerId, VfsStorage.local(ownerRoot));
      await startGateway();
      session = await createSessionBounded(
        {
          image: sandboxImage,
          architecture,
          name: sessionName,
          metadata: { role: "chevalier-vfs-lifecycle-leak-torture", probeId, cycle: String(cycle) },
          autoStart: true,
          sharedMounts: [
            {
              guestPath: mountPath,
              mountTag,
              readOnly: false,
              availability: "shared-storage",
              continuity: "restore-cross-node",
              backendProfile,
              vfsEndpoint: ownerEndpoint,
              vfsScopePath: scopePath,
            },
          ],
        },
        `create lifecycle VM cycle ${cycle}`,
      );
      evidence.sessionId = session.sessionId;
      evidence.vmId = session.vmId;
      activeSessions.set(session.sessionId, session);

      await poll(
        `cycle ${cycle} mounted callback readiness`,
        async () => {
          const challenge = `ready-${probeId}-${cycle}`;
          const command = await execGuest(
            session,
            `test "$(findmnt -n -o FSTYPE ${mountPath})" = virtiofs
printf '%s' '${challenge}' >${mountPath}/.lifecycle-ready
sync`,
            30,
          ).catch(() => null);
          if (command?.code !== 0) return { done: false, command };
          const bytes = await stores
            .get(ownerId)
            .read(`${scopePath}/.lifecycle-ready`)
            .catch(() => null);
          return { done: bytes?.toString("utf8") === challenge, value: command };
        },
        90_000,
        500,
      );

      const mutation = await execGuest(
        session,
        `python3 - <<'PY'
import os
root = "/workspace/lifecycle"
os.mkdir(root)
path = root + "/published"
fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
os.write(fd, b"cycle-data")
os.fsync(fd)
os.close(fd)
os.link(path, root + "/alias")
os.symlink("published", root + "/symlink")
os.rename(root + "/published", root + "/renamed")
fd = os.open(root + "/renamed", os.O_RDWR)
os.pwrite(fd, b"X", 2)
os.fsync(fd)
os.close(fd)
os.unlink(root + "/alias")
print("MUTATION_BARRIERS_OK")
PY`,
        60,
      );
      if (mutation.code !== 0 || !mutation.stdout.includes("MUTATION_BARRIERS_OK")) {
        throw new Error(`cycle ${cycle} mutation failed: ${JSON.stringify(mutation)}`);
      }

      lockHandle = await startGuest(
        session,
        `python3 -u - <<'PY'
import fcntl
import os
import time
root = "/workspace/lifecycle"
flock_fd = os.open(root + "/flock-held", os.O_CREAT | os.O_RDWR, 0o644)
posix_fd = os.open(root + "/posix-held", os.O_CREAT | os.O_RDWR, 0o644)
fcntl.flock(flock_fd, fcntl.LOCK_EX)
fcntl.lockf(posix_fd, fcntl.LOCK_EX, 8, 0)
print("LOCKS_HELD", flush=True)
time.sleep(600)
PY`,
        700,
      );
      await waitForExecMarker(lockHandle, "LOCKS_HELD", `cycle ${cycle} lock holder`);
      evidence.lockRowsActive = await poll(`cycle ${cycle} lock publication`, async () => {
        const rows = lockState.rows(ownerId);
        const namespaces = new Set(rows.map((row) => row.namespace));
        return {
          done: rows.length >= 2 && namespaces.has("flock") && namespaces.has("posix"),
          value: rows,
          rows,
        };
      });

      evidence.activeObserver = await observeHost([session.vmId]);
      if (evidence.activeObserver) {
        const active = evidence.activeObserver;
        if (active.probeVirtiofsdCount < 1) {
          throw new Error(`cycle ${cycle} observer saw no probe virtiofsd process`);
        }
        if (active.probeQemuCount < 1) {
          throw new Error(`cycle ${cycle} observer saw no probe qemu process`);
        }
        if (active.probeFuseMountCount < 1) {
          throw new Error(`cycle ${cycle} observer saw no probe FUSE mount`);
        }
        if (active.probeVmDirsExisting.length !== 1) {
          throw new Error(`cycle ${cycle} observer did not see the exact probe VM directory`);
        }
        if (active.probeRuntimeDirsExisting.length !== 1) {
          throw new Error(`cycle ${cycle} observer did not see the exact probe runtime directory`);
        }
        if (active.probeArtifactPaths.length === 0) {
          throw new Error(`cycle ${cycle} observer saw no journal or sidecar artifacts while active`);
        }
      }

      await stopGateway();
      await delay(150);
      await startGateway();
      const restart = await execGuest(
        session,
        `for attempt in $(seq 1 20); do
  if test "$(cat /workspace/lifecycle/renamed)" = "cyXle-data"; then
    echo GATEWAY_RESTART_OK
    exit 0
  fi
  sleep 0.1
done
exit 1`,
        30,
      );
      if (restart.code !== 0 || !restart.stdout.includes("GATEWAY_RESTART_OK")) {
        throw new Error(`cycle ${cycle} callback restart failed: ${JSON.stringify(restart)}`);
      }

      await discardSessionBounded(session, `discard lifecycle VM cycle ${cycle}`);
      activeSessions.delete(session.sessionId);
      await drainExec(lockHandle, `cycle ${cycle} discarded lock holder`, 15_000).catch(
        () => undefined,
      );
      lockHandle = undefined;

      await poll(`cycle ${cycle} provider session removal`, async () => {
        const matching = (await listSessionsBounded(
          `list sessions while verifying cycle ${cycle} removal`,
        )).filter(
          (entry) => entry.sessionId === evidence.sessionId || entry.name.includes(sessionName),
        );
        return { done: matching.length === 0, value: true, matching };
      });
      evidence.providerSessionRemoved = true;

      evidence.lockRowsAfterDiscard = await poll(
        `cycle ${cycle} advisory lock release`,
        async () => {
          const rows = lockState.rows(ownerId);
          return { done: rows.length === 0, value: rows, rows };
        },
        cleanupTimeoutMs,
        100,
      );

      evidence.cleanupObserver = await poll(
        `cycle ${cycle} host resource cleanup`,
        async () => {
          const observed = await observeHost([evidence.vmId]);
          if (!observed) return { done: true, value: null };
          const clean =
            observed.probeProcessCount === 0 &&
            observed.probeFuseMountCount === 0 &&
            observed.probeVmDirsExisting.length === 0 &&
            observed.probeRuntimeDirsExisting.length === 0 &&
            observed.probeArtifactPaths.length === 0 &&
            observed.probeRuntimeArtifacts.length === 0;
          return { done: clean, value: observed, observed };
        },
        cleanupTimeoutMs,
        250,
      );

      await stopGateway();
      stores.delete(ownerId);
      await rm(ownerRoot, { recursive: true, force: true });
      evidence.ownerRootRemoved = true;
      lockState.deleteOwner(ownerId);
      if (typeof global.gc === "function") global.gc();
      await delay(50);
      evidence.localAfterCleanup = await selfResourceSnapshot();
      evidence.requestCountAfter = requestCounts.get(ownerId) || 0;
      requestCounts.delete(ownerId);
      evidence.status = "pass";
    } catch (error) {
      evidence.error = error instanceof Error ? error.stack || error.message : String(error);
      fail(`cycle ${cycle}`, evidence.error);
      if (lockHandle) {
        await lockHandle.signal(9).catch(() => undefined);
      }
      if (session) {
        await discardSessionBounded(session, `failure cleanup for cycle ${cycle}`)
          .then(() => activeSessions.delete(session.sessionId))
          .catch((cleanupError) => {
            fail(
              "cycle failure provider cleanup",
              `${session.sessionId}: ${cleanupError instanceof Error ? cleanupError.message : String(cleanupError)}`,
            );
          });
      }
      await stopGateway().catch(() => undefined);
      stores.delete(ownerId);
      await rm(ownerRoot, { recursive: true, force: true }).catch(() => undefined);
      evidence.ownerRootRemoved = true;
      requestCounts.delete(ownerId);
    } finally {
      evidence.durationMs = Date.now() - startedAt;
      cycleEvidence.push(evidence);
      process.stderr.write(
        `[vfs-lifecycle] cycle ${cycle}/${cycles} ${evidence.status} (${evidence.durationMs} ms)\n`,
      );
    }
    if (evidence.status !== "pass") break;
  }
} finally {
  await stopGateway().catch((error) => {
    fail("final gateway cleanup", error instanceof Error ? error.message : String(error));
  });
  if (sandbox) {
    if (pendingCreations.size !== 0) {
      await withTimeout(
        Promise.allSettled([...pendingCreations.values()]),
        "wait for pending lifecycle session creations",
        cleanupTimeoutMs,
      ).catch((error) => {
        fail(
          "pending provider creation settlement",
          `${error instanceof Error ? error.message : String(error)}; pending=${JSON.stringify([...pendingCreations.keys()])}`,
        );
      });
    }
    const generated = (
      await listSessionsBounded("list generated sessions for final cleanup").catch((error) => {
        fail(
          "final provider session listing",
          error instanceof Error ? error.message : String(error),
        );
        return [];
      })
    ).filter((entry) => entry.name.includes(probeId));
    cleanupSessions = generated.map((entry) => ({
      sessionId: entry.sessionId,
      vmId: entry.vmId,
      name: entry.name,
    }));
    for (const entry of generated) {
      await withTimeout(
        sandbox.discardSessionById(entry.sessionId),
        `final provider cleanup ${entry.sessionId}`,
        cleanupTimeoutMs,
      ).catch((error) => {
        fail(
          "final provider cleanup",
          `${entry.sessionId}: ${error instanceof Error ? error.message : String(error)}`,
        );
      });
    }
    for (const [sessionId, session] of activeSessions) {
      await discardSessionBounded(session, `final handle cleanup ${sessionId}`)
        .then(() => activeSessions.delete(sessionId))
        .catch((error) => {
          fail(
            "final handle cleanup",
            `${sessionId}: ${error instanceof Error ? error.message : String(error)}`,
          );
        });
    }
    await poll(
      "final generated provider session cleanup",
      async () => {
        const remaining = (await listSessionsBounded(
          "list remaining generated sessions during final cleanup",
        )).filter((entry) =>
          entry.name.includes(probeId),
        );
        return { done: remaining.length === 0, value: remaining, remaining };
      },
      cleanupTimeoutMs,
      250,
    ).catch((error) => {
      fail("final provider session audit", error instanceof Error ? error.message : String(error));
    });
  }
  stores.clear();
  await rm(backingRoot, { recursive: true, force: true })
    .then(() => {
      backingRootRemoved = true;
    })
    .catch((error) => {
      fail("final backing root cleanup", error instanceof Error ? error.message : String(error));
    });
  if (typeof global.gc === "function") global.gc();
  await delay(100);
  finalLocal = await selfResourceSnapshot();
  finalObserver = await observeHost(
    cycleEvidence.map((entry) => entry.vmId).filter(Boolean),
  ).catch((error) => {
    fail("final remote observer", error instanceof Error ? error.message : String(error));
    return null;
  });
}

const completedCleanupObservers = cycleEvidence
  .filter((entry) => entry.status === "pass")
  .map((entry) => entry.cleanupObserver)
  .filter(Boolean);
const warmedObservers = completedCleanupObservers.slice(1);
const vmdFdValues = warmedObservers.map((entry) => entry.vmdFdCount);
const vmdThreadValues = warmedObservers.map((entry) => entry.vmdThreadCount);
const vmdRssValues = warmedObservers.map((entry) => entry.vmdRssKib);
const warmedLocal = cycleEvidence
  .filter((entry) => entry.status === "pass" && entry.localAfterCleanup)
  .slice(1)
  .map((entry) => entry.localAfterCleanup);
const localFdValues = warmedLocal
  .map((entry) => entry.fdCount)
  .filter((value) => value !== null);
const localRssValuesKib = warmedLocal.map((entry) => entry.rssBytes / 1024);
const vmdFdGrowth =
  vmdFdValues.length > 0 && observerBaseline
    ? Math.max(...vmdFdValues) - observerBaseline.vmdFdCount
    : null;
const vmdThreadGrowth =
  vmdThreadValues.length > 0 && observerBaseline
    ? Math.max(...vmdThreadValues) - observerBaseline.vmdThreadCount
    : null;
const vmdRssGrowthKib =
  vmdRssValues.length > 0 && observerBaseline
    ? Math.max(...vmdRssValues) - observerBaseline.vmdRssKib
    : null;
const vmdRssSlopeKib = vmdRssValues.length > 1 ? slope(vmdRssValues) : null;
const localFdGrowth =
  localFdValues.length > 0 && localBaseline?.fdCount !== null
    ? Math.max(...localFdValues) - localBaseline.fdCount
    : null;
const localRssGrowthKib =
  localRssValuesKib.length > 0 && localBaseline
    ? Math.max(...localRssValuesKib) - localBaseline.rssBytes / 1024
    : null;
const localRssSlopeKib =
  localRssValuesKib.length > 1 ? slope(localRssValuesKib) : null;

if (observerBaseline && finalObserver) {
  if (finalObserver.containerZombieCount > observerBaseline.containerZombieCount) {
    fail(
      "zombie process audit",
      `baseline=${observerBaseline.containerZombieCount} final=${finalObserver.containerZombieCount}`,
    );
  }
  if (finalObserver.virtiofsdCount > observerBaseline.virtiofsdCount) {
    fail(
      "orphan virtiofsd audit",
      `baseline=${observerBaseline.virtiofsdCount} final=${finalObserver.virtiofsdCount}`,
    );
  }
  if (finalObserver.qemuCount > observerBaseline.qemuCount) {
    fail(
      "orphan qemu audit",
      `baseline=${observerBaseline.qemuCount} final=${finalObserver.qemuCount}`,
    );
  }
  if (finalObserver.fuseMountCount > observerBaseline.fuseMountCount) {
    fail(
      "orphan FUSE mount audit",
      `baseline=${observerBaseline.fuseMountCount} final=${finalObserver.fuseMountCount}`,
    );
  }
  if (
    finalObserver.probeProcessCount !== 0 ||
    finalObserver.probeFuseMountCount !== 0 ||
    finalObserver.probeVmDirsExisting.length !== 0 ||
    finalObserver.probeRuntimeDirsExisting.length !== 0 ||
    finalObserver.probeArtifactPaths.length !== 0 ||
    finalObserver.probeRuntimeArtifacts.length !== 0
  ) {
    fail("final exact probe cleanup", JSON.stringify(finalObserver));
  }
}
if (vmdFdGrowth !== null && vmdFdGrowth > maxVmdFdGrowth) {
  fail("bounded vmd file descriptors", `growth=${vmdFdGrowth} allowed=${maxVmdFdGrowth}`);
}
if (vmdThreadGrowth !== null && vmdThreadGrowth > maxVmdThreadGrowth) {
  fail("bounded vmd threads", `growth=${vmdThreadGrowth} allowed=${maxVmdThreadGrowth}`);
}
if (vmdRssGrowthKib !== null && vmdRssGrowthKib > maxVmdRssGrowthKib) {
  fail(
    "bounded vmd RSS",
    `growthKib=${vmdRssGrowthKib} allowedKib=${maxVmdRssGrowthKib}`,
  );
}
if (vmdRssSlopeKib !== null && vmdRssSlopeKib > maxVmdRssSlopeKib) {
  fail(
    "bounded vmd RSS trend",
    `slopeKibPerCycle=${vmdRssSlopeKib} allowed=${maxVmdRssSlopeKib}`,
  );
}
if (localFdGrowth !== null && localFdGrowth > maxLocalFdGrowth) {
  fail(
    "bounded callback file descriptors",
    `growth=${localFdGrowth} allowed=${maxLocalFdGrowth}`,
  );
}
if (localRssGrowthKib !== null && localRssGrowthKib > maxLocalRssGrowthKib) {
  fail(
    "bounded callback RSS",
    `growthKib=${localRssGrowthKib} allowedKib=${maxLocalRssGrowthKib}`,
  );
}
if (localRssSlopeKib !== null && localRssSlopeKib > maxLocalRssSlopeKib) {
  fail(
    "bounded callback RSS trend",
    `slopeKibPerCycle=${localRssSlopeKib} allowed=${maxLocalRssSlopeKib}`,
  );
}
if (lockState.totalRows() !== 0) {
  fail("final advisory lock rows", `remaining=${lockState.totalRows()}`);
}
if (!backingRootRemoved) {
  fail("final backing root", `not removed: ${backingRoot}`);
}

results.push(
  {
    name: "repeated lifecycle cycles",
    status:
      cycleEvidence.length === cycles && cycleEvidence.every((entry) => entry.status === "pass")
        ? "pass"
        : "fail",
    detail: `${cycleEvidence.filter((entry) => entry.status === "pass").length}/${cycles} passed`,
  },
  {
    name: "exact disposable cleanup",
    status:
      backingRootRemoved &&
      lockState.totalRows() === 0 &&
      finalObserver?.probeProcessCount !== undefined
        ? failures.some((entry) =>
            [
              "zombie process audit",
              "orphan virtiofsd audit",
              "orphan qemu audit",
              "orphan FUSE mount audit",
              "final exact probe cleanup",
              "final advisory lock rows",
              "final backing root",
            ].includes(entry.name),
          )
          ? "fail"
          : "pass"
        : observerSshHost
          ? "fail"
          : "pass",
    detail: observerSshHost
      ? "provider sessions, host processes, FUSE mounts, runtime/data directories, journals, sidecars, and locks audited"
      : "provider sessions, backing roots, callback listener, and in-memory lock rows audited; host observer disabled",
  },
  {
    name: "bounded resources",
    status: failures.some((entry) =>
      [
        "bounded vmd file descriptors",
        "bounded vmd threads",
        "bounded vmd RSS",
        "bounded vmd RSS trend",
        "bounded callback file descriptors",
        "bounded callback RSS",
        "bounded callback RSS trend",
      ].includes(entry.name),
    )
      ? "fail"
      : "pass",
    detail: JSON.stringify({
      vmdFdGrowth,
      vmdThreadGrowth,
      vmdRssGrowthKib,
      vmdRssSlopeKib,
      localFdGrowth,
      localRssGrowthKib,
      localRssSlopeKib,
    }),
  },
);

const ok = failures.length === 0 && results.every((entry) => entry.status !== "fail");
console.log(
  JSON.stringify(
    {
      ok,
      probeId,
      sandboxEndpoint,
      image: sandboxImage,
      architecture,
      cycles,
      gatewayPublicUrl,
      observer: observerSshHost
        ? {
            sshHost: observerSshHost,
            container: observerContainer,
            dataRoot: observerDataRoot,
          }
        : null,
      thresholds: {
        maxVmdFdGrowth,
        maxVmdThreadGrowth,
        maxVmdRssGrowthKib,
        maxVmdRssSlopeKib,
        maxLocalFdGrowth,
        maxLocalRssGrowthKib,
        maxLocalRssSlopeKib,
      },
      baseline: {
        local: localBaseline,
        observer: observerBaseline,
      },
      final: {
        local: finalLocal,
        observer: finalObserver,
        backingRootRemoved: backingRootRemoved ? backingRoot : false,
        advisoryLockRows: lockState.totalRows(),
        generatedSessionsFoundDuringFinalCleanup: cleanupSessions,
        pendingCreationNames: [...pendingCreations.keys()],
        lateCreatedSessions: [...lateCreatedSessions.values()],
        activeSessionIds: [...activeSessions.keys()],
        providerCleanupDiagnostics,
      },
      resourceTrend: {
        vmdFdValues,
        vmdThreadValues,
        vmdRssValues,
        vmdFdGrowth,
        vmdThreadGrowth,
        vmdRssGrowthKib,
        vmdRssSlopeKib,
        localFdValues,
        localRssValuesKib,
        localFdGrowth,
        localRssGrowthKib,
        localRssSlopeKib,
      },
      summary: {
        passed: results.filter((entry) => entry.status === "pass").length,
        failed: results.filter((entry) => entry.status === "fail").length,
        skipped: results.filter((entry) => entry.status === "skip").length,
        failureCount: failures.length,
      },
      results,
      failures,
      cycleEvidence,
    },
    null,
    2,
  ),
);
if (!ok) process.exitCode = 1;
