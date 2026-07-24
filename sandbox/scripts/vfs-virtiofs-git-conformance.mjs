#!/usr/bin/env node

import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { mkdir, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { runPosixModelTorture } from "./posix-model-torture.mjs";
import { runVfsGatewayProtocolProbe } from "./vfs-gateway-protocol-probe.mjs";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");

if (process.argv.includes("--help")) {
  console.log(`Disposable real-path VFS + virtiofsd + VM + Git conformance harness.

Required:
  SANDBOX_ENDPOINT                         vmd/control-gateway URL
  SANDBOX_IMAGE                            bootable sandbox image
  CHEVALIER_VFS_HARNESS_GATEWAY_PUBLIC_URL URL by which vmd can reach this process
                                               (for example http://100.x.y.z:19091)
  CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN
                                             token configured in vmd; defaults to
                                             SANDBOX_AUTH_TOKEN

Optional:
  SANDBOX_AUTH_TOKEN
  SANDBOX_ARCHITECTURE=amd64
  CHEVALIER_VFS_HARNESS_GATEWAY_BIND=0.0.0.0
  CHEVALIER_VFS_HARNESS_GATEWAY_PORT=19091
  CHEVALIER_VFS_HARNESS_BACKEND_PROFILE=openbracket-vfs-fuse
  CHEVALIER_VFS_HARNESS_COMMAND_TIMEOUT_MS=300000
  CHEVALIER_VFS_HARNESS_CHECKS=1,2,3
  CHEVALIER_VFS_HARNESS_POSIX_SEED=<recorded-seed>
  CHEVALIER_VFS_HARNESS_POSIX_ONE_STEPS=64
  CHEVALIER_VFS_HARNESS_POSIX_TWO_STEPS=96
  CHEVALIER_VFS_HARNESS_GIT_FILE_COUNT=1000
  CHEVALIER_VFS_HARNESS_GIT_STATUS_COLD_MAX_MS=2000
  CHEVALIER_VFS_HARNESS_GIT_STATUS_WARM_MAX_MS=1500
  CHEVALIER_MODULE_PATH=<repo>/ts/index.js
  CHEVALIER_SANDBOX_MODULE_PATH=<repo>/ts-sandbox/index.js

The harness starts an authenticated in-process HTTP gateway backed by a fresh
local VFS root, proves the normal one-VM topology, adds a second disposable VM
on the same scope for stronger cross-mount acceptance, interrupts and restarts
the callback gateway, replaces one VM once, and removes every VM and backing
file on exit.`);
  process.exit(0);
}

const required = (name) => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required; run with --help for the complete contract`);
  return value;
};

const sandboxEndpoint = required("SANDBOX_ENDPOINT");
const sandboxImage = required("SANDBOX_IMAGE");
const gatewayPublicUrl = required("CHEVALIER_VFS_HARNESS_GATEWAY_PUBLIC_URL").replace(/\/+$/, "");
const sandboxAuthToken = process.env.SANDBOX_AUTH_TOKEN?.trim();
const vfsAuthToken =
  process.env.CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN?.trim() || sandboxAuthToken;
if (!vfsAuthToken) {
  throw new Error(
    "CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN (or SANDBOX_AUTH_TOKEN as fallback) is required",
  );
}

const architecture = process.env.SANDBOX_ARCHITECTURE?.trim() || "amd64";
const backendProfile =
  process.env.CHEVALIER_VFS_HARNESS_BACKEND_PROFILE?.trim() || "openbracket-vfs-fuse";
const gatewayBind = process.env.CHEVALIER_VFS_HARNESS_GATEWAY_BIND?.trim() || "0.0.0.0";
const gatewayPort = Number(process.env.CHEVALIER_VFS_HARNESS_GATEWAY_PORT ?? "19091");
const maxCommandTimeoutMs = 300_000;
const commandTimeoutMs = Number(
  process.env.CHEVALIER_VFS_HARNESS_COMMAND_TIMEOUT_MS ?? String(maxCommandTimeoutMs),
);
if (!Number.isInteger(gatewayPort) || gatewayPort < 1 || gatewayPort > 65535) {
  throw new Error("CHEVALIER_VFS_HARNESS_GATEWAY_PORT must be an integer in 1..65535");
}
if (
  !Number.isFinite(commandTimeoutMs) ||
  commandTimeoutMs < 1_000 ||
  commandTimeoutMs > maxCommandTimeoutMs
) {
  throw new Error(
    `CHEVALIER_VFS_HARNESS_COMMAND_TIMEOUT_MS must be between 1000 and ${maxCommandTimeoutMs}`,
  );
}

const selectedChecks = (() => {
  const raw = process.env.CHEVALIER_VFS_HARNESS_CHECKS?.trim();
  if (!raw) return null;
  return new Set(
    raw.split(",").map((part) => {
      const id = Number(part);
      if (!Number.isInteger(id) || id < 1 || id > 11) {
        throw new Error(`invalid CHEVALIER_VFS_HARNESS_CHECKS entry: ${part}`);
      }
      return id;
    }),
  );
})();

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
    `load native Chevalier modules failed; build the host-native ts and ts-sandbox bindings or set CHEVALIER_MODULE_PATH/CHEVALIER_SANDBOX_MODULE_PATH: ${error}`,
  );
}
const { createVfsGatewayServer, VfsStorage } = chevalier;
const { Sandbox } = sandboxModule;
if (
  typeof createVfsGatewayServer !== "function" ||
  typeof VfsStorage?.local !== "function" ||
  typeof Sandbox?.connect !== "function"
) {
  throw new Error("loaded modules do not expose createVfsGatewayServer, VfsStorage.local, and Sandbox.connect");
}

const probeId = `virtiofs-git-${Date.now()}-${randomUUID().slice(0, 8)}`;
const positiveIntegerEnv = (name, fallback) => {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
};
const posixModelSeed = process.env.CHEVALIER_VFS_HARNESS_POSIX_SEED?.trim() || probeId;
const posixModelOneSteps = positiveIntegerEnv(
  "CHEVALIER_VFS_HARNESS_POSIX_ONE_STEPS",
  64,
);
const posixModelTwoSteps = positiveIntegerEnv(
  "CHEVALIER_VFS_HARNESS_POSIX_TWO_STEPS",
  96,
);
const gitWorkloadFileCount = positiveIntegerEnv(
  "CHEVALIER_VFS_HARNESS_GIT_FILE_COUNT",
  1_000,
);
const gitStatusColdMaxMs = positiveIntegerEnv(
  "CHEVALIER_VFS_HARNESS_GIT_STATUS_COLD_MAX_MS",
  2_000,
);
const gitStatusWarmMaxMs = positiveIntegerEnv(
  "CHEVALIER_VFS_HARNESS_GIT_STATUS_WARM_MAX_MS",
  1_500,
);
if (selectedChecks?.has(11) && !selectedChecks.has(6)) {
  throw new Error(
    "CHEVALIER_VFS_HARNESS_CHECKS=11 requires correctness check 6 in the same run",
  );
}
const ownerId = `chevalier-vfs-harness-${probeId}`;
const gitDisabledOwnerId = `${ownerId}-git-disabled`;
const scopePath = `probes/${probeId}/repo`;
const mountPath = "/workspace";
const backingRoot = join(
  process.env.CHEVALIER_VFS_HARNESS_TMPDIR?.trim() || "/tmp",
  `chevalier-${probeId}`,
);
const ownerRoot = join(backingRoot, ownerId);
const gitDisabledOwnerRoot = join(backingRoot, gitDisabledOwnerId);
const ownerEndpoint = `${gatewayPublicUrl}/internal/chevalier/vfs/${encodeURIComponent(ownerId)}`;
const gitDisabledOwnerEndpoint =
  `${gatewayPublicUrl}/internal/chevalier/vfs/${encodeURIComponent(gitDisabledOwnerId)}`;
let gatewayRequestCount = 0;
const gatewayRequestCountsByRoute = new Map();
const gatewayPathCounts = new Map();
const snapshotGatewayRequestCounts = () => ({
  routes: new Map(gatewayRequestCountsByRoute),
  paths: new Map(gatewayPathCounts),
});
const gatewayRequestDelta = (before) => {
  const routes = {};
  let total = 0;
  for (const [route, count] of gatewayRequestCountsByRoute) {
    const delta = count - (before.routes.get(route) ?? 0);
    if (delta <= 0) continue;
    routes[route] = delta;
    total += delta;
  }
  const pathsByRoute = {};
  for (const [key, count] of gatewayPathCounts) {
    const delta = count - (before.paths.get(key) ?? 0);
    if (delta <= 0) continue;
    const separator = key.indexOf("\n");
    const route = key.slice(0, separator);
    const path = key.slice(separator + 1);
    const summary = pathsByRoute[route] ?? { total: 0, unique: 0, top: [] };
    summary.total += delta;
    summary.unique += 1;
    summary.top.push([path, delta]);
    pathsByRoute[route] = summary;
  }
  for (const summary of Object.values(pathsByRoute)) {
    summary.top.sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
    summary.top = summary.top.slice(0, 20);
  }
  return { total, routes, pathsByRoute };
};

const withTimeout = async (promise, label, timeoutMs = commandTimeoutMs) => {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};

const readRequestBody = async (request) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
};

const recordGatewayPaths = (routeKey, requestUrl, body) => {
  const paths = [];
  for (const name of ["path", "from", "to"]) {
    const value = requestUrl.searchParams.get(name);
    if (value !== null) paths.push(value);
  }
  if (body !== undefined && body.length !== 0) {
    try {
      const decoded = JSON.parse(body.toString("utf8"));
      for (const path of decoded.paths ?? []) {
        if (typeof path === "string") paths.push(path);
      }
      for (const write of decoded.writes ?? []) {
        if (typeof write?.path === "string") paths.push(write.path);
      }
      for (const mutation of decoded.mutations ?? []) {
        for (const name of ["path", "source_path", "destination_path", "from", "to"]) {
          if (typeof mutation?.[name] === "string") paths.push(mutation[name]);
        }
      }
      if (typeof decoded.path === "string") paths.push(decoded.path);
      if (typeof decoded.source_path === "string") paths.push(decoded.source_path);
      if (typeof decoded.destination_path === "string") paths.push(decoded.destination_path);
    } catch {
      // Binary write bodies and malformed requests are still counted by route.
    }
  }
  for (const path of paths) {
    const key = `${routeKey}\n${path}`;
    gatewayPathCounts.set(key, (gatewayPathCounts.get(key) ?? 0) + 1);
  }
};

await Promise.all([
  mkdir(ownerRoot, { recursive: true }),
  mkdir(gitDisabledOwnerRoot, { recursive: true }),
]);
const storage = VfsStorage.local(ownerRoot);
const gitDisabledStorage = VfsStorage.local(gitDisabledOwnerRoot);
const handleGatewayRequest = createVfsGatewayServer({
  resolveStore: async (requestedOwner) => {
    if (requestedOwner === ownerId) return storage;
    if (requestedOwner === gitDisabledOwnerId) return gitDisabledStorage;
    throw new Error(`unexpected owner: ${requestedOwner}`);
  },
  authToken: vfsAuthToken,
  allowGitMetadata: async (requestedOwner) => requestedOwner === ownerId,
});

const gatewayServer = createServer(async (incoming, outgoing) => {
  try {
    const method = incoming.method || "GET";
    const requestUrl = new URL(
      incoming.url || "/",
      `http://${incoming.headers.host || "localhost"}`,
    );
    const routeParts = requestUrl.pathname.split("/").filter(Boolean);
    const ownerIndex = routeParts.indexOf("vfs") + 1;
    const operation =
      ownerIndex > 0 ? routeParts.slice(ownerIndex + 1).join("/") : requestUrl.pathname;
    const routeKey = `${method} /${operation}`;
    gatewayRequestCountsByRoute.set(
      routeKey,
      (gatewayRequestCountsByRoute.get(routeKey) ?? 0) + 1,
    );
    const body = method === "GET" || method === "HEAD" ? undefined : await readRequestBody(incoming);
    recordGatewayPaths(routeKey, requestUrl, body);
    const request = new Request(
      requestUrl,
      {
        method,
        headers: incoming.headers,
        body,
        ...(body === undefined ? {} : { duplex: "half" }),
      },
    );
    const response = await handleGatewayRequest(request);
    gatewayRequestCount += 1;
    outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
    outgoing.end(Buffer.from(await response.arrayBuffer()));
  } catch (error) {
    outgoing.writeHead(500, { "content-type": "text/plain" });
    outgoing.end(error instanceof Error ? error.stack || error.message : String(error));
  }
});
// The vmd revision watch holds a 25s long-poll on an idle keep-alive socket.
// Node's default keepAliveTimeout (5s) would reap that idle connection and RST
// it, so the client's next pooled use fails with a send-class error (the watch
// flapping this harness was built to catch). Hold connections open well past
// the long-poll window; headersTimeout must exceed keepAliveTimeout so a slow
// header is never mistaken for an idle reap.
gatewayServer.keepAliveTimeout = 120_000;
gatewayServer.headersTimeout = 125_000;

let gatewayListening = false;
const startGateway = async () => {
  if (gatewayListening) return;
  await withTimeout(
    new Promise((resolveListen, reject) => {
      const onError = (error) => {
        gatewayServer.off("listening", onListening);
        reject(error);
      };
      const onListening = () => {
        gatewayServer.off("error", onError);
        resolveListen();
      };
      gatewayServer.once("error", onError);
      gatewayServer.once("listening", onListening);
      gatewayServer.listen(gatewayPort, gatewayBind);
    }),
    "start disposable VFS gateway",
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
    "stop disposable VFS gateway",
    10_000,
  );
  gatewayListening = false;
};

let gatewayProtocolEvidence;
let gatewayGitPolicyEvidence;
try {
  await startGateway();
  gatewayProtocolEvidence = await withTimeout(
    runVfsGatewayProtocolProbe({
      ownerEndpoint,
      authToken: vfsAuthToken,
      scopePath,
    }),
    "public callback gateway protocol preflight",
    30_000,
  );

  const policyPath = `${scopePath}/.gateway-policy/.git/HEAD`;
  const callPolicy = async (endpoint, suffix, init = {}) => {
    const response = await withTimeout(
      fetch(`${endpoint}${suffix}`, init),
      `gateway Git policy ${init.method || "GET"} ${suffix}`,
      30_000,
    );
    return {
      status: response.status,
      bytes: Buffer.from(await response.arrayBuffer()),
    };
  };
  await storage.write(policyPath, Buffer.from("ref: refs/heads/policy-proof\n"));
  const enabledRead = await callPolicy(
    ownerEndpoint,
    `/file/raw?path=${encodeURIComponent(policyPath)}`,
    { headers: { authorization: `Bearer ${vfsAuthToken}` } },
  );
  const disabledPut = await callPolicy(
    gitDisabledOwnerEndpoint,
    `/file?path=${encodeURIComponent(policyPath)}`,
    {
      method: "PUT",
      headers: { authorization: `Bearer ${vfsAuthToken}` },
      body: Buffer.from("ref: refs/heads/forbidden\n"),
      duplex: "half",
    },
  );
  const disabledStat = await callPolicy(
    gitDisabledOwnerEndpoint,
    `/stat?path=${encodeURIComponent(policyPath)}`,
    { headers: { authorization: `Bearer ${vfsAuthToken}` } },
  );
  const disabledRead = await callPolicy(
    gitDisabledOwnerEndpoint,
    `/file/raw?path=${encodeURIComponent(policyPath)}`,
    { headers: { authorization: `Bearer ${vfsAuthToken}` } },
  );
  if (
    enabledRead.status !== 200 ||
    enabledRead.bytes.toString("utf8") !== "ref: refs/heads/policy-proof\n" ||
    disabledPut.status !== 400 ||
    !/git|excluded|denied|refus/i.test(disabledPut.bytes.toString("utf8")) ||
    disabledStat.status !== 404 ||
    disabledRead.status !== 404
  ) {
    throw new Error(
      `gateway Git policy isolation failed: enabledRead=${enabledRead.status}, disabledPut=${disabledPut.status} ${disabledPut.bytes.toString("utf8")}, disabledStat=${disabledStat.status}, disabledRead=${disabledRead.status}`,
    );
  }
  gatewayGitPolicyEvidence = {
    enabledOwner: ownerId,
    disabledOwner: gitDisabledOwnerId,
    sameBearer: true,
    enabledRead: enabledRead.status,
    disabledPut: disabledPut.status,
    disabledStat: disabledStat.status,
    disabledRead: disabledRead.status,
  };
  await storage.remove(policyPath);
  await storage.rmdir(`${scopePath}/.gateway-policy/.git`);
  await storage.rmdir(`${scopePath}/.gateway-policy`);
} catch (error) {
  await stopGateway().catch(() => undefined);
  await rm(backingRoot, { recursive: true, force: true }).catch(() => undefined);
  throw error;
}

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
    if (event.type === "stdout" && event.data) stdout += Buffer.from(event.data).toString("utf8");
    if (event.type === "stderr" && event.data) stderr += Buffer.from(event.data).toString("utf8");
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
    throw new Error(
      `${label} exited without marker ${markerSteps[markerIndex].marker}\n${commandResultText({
        code,
        stdout,
        stderr,
      })}`,
    );
  }
  return { code, stdout, stderr };
};

const drainExec = async (handle, label) => collectExecAtMarkers(handle, label);

const startGuest = async (
  session,
  command,
  timeoutSecs = 300,
  { interactive = false } = {},
) =>
  withTimeout(
    session.exec(`set -euo pipefail\n${command}`, {
      shell: "/bin/bash",
      closeStdinOnStart: !interactive,
      timeoutSecs: Math.min(timeoutSecs, Math.floor(commandTimeoutMs / 1_000)),
      env: {
        GIT_AUTHOR_NAME: "Chevalier VFS Conformance",
        GIT_AUTHOR_EMAIL: "vfs-conformance@chevalier.test",
        GIT_COMMITTER_NAME: "Chevalier VFS Conformance",
        GIT_COMMITTER_EMAIL: "vfs-conformance@chevalier.test",
      },
    }),
    `start guest command: ${command.slice(0, 100)}`,
  );

let stallExecSeq = 0;
const execGuest = async (session, command, timeoutSecs = 300) => {
  // STALL DIAGNOSTIC (keep: low-noise): per-action wall-clock breadcrumbs so a
  // ~30s action-boundary stall is attributable to a hop. `t0` (issue) correlates
  // with the guest portproxy recv breadcrumb; `est` is harness->exec-established
  // (delivery hop), `exec` is established->exit (spawn + run + completion hop).
  const seq = ++stallExecSeq;
  const t0 = Date.now();
  process.stderr.write(`[stall] issue seq=${seq} t0=${t0}\n`);
  const handle = await startGuest(session, command, timeoutSecs);
  const t1 = Date.now();
  const result = await drainExec(handle, command.slice(0, 100));
  const t2 = Date.now();
  const total = t2 - t0;
  process.stderr.write(
    `[stall] end seq=${seq} est=${t1 - t0}ms exec=${t2 - t1}ms total=${total}ms code=${result.code}${total > 5000 ? " SLOW" : ""}\n`,
  );
  return result;
};

const waitForGuestBarrier = (promise, label, timeoutMs = 60_000) =>
  withTimeout(
    promise,
    label,
    Math.min(timeoutMs, commandTimeoutMs),
  );

const execGuestAtMarkers = async (
  session,
  command,
  markerSteps,
  timeoutSecs = 300,
) =>
  collectExecAtMarkers(
    await startGuest(session, command, timeoutSecs, { interactive: true }),
    command.slice(0, 100),
    markerSteps,
  );

const results = [];
const check = async (id, name, body) => {
  if (selectedChecks && !selectedChecks.has(id)) {
    results.push({ id, name, status: "skip", durationMs: 0, detail: "not selected" });
    return;
  }
  const started = Date.now();
  process.stderr.write(`[virtiofs-git] ${id}/11 ${name}...\n`);
  try {
    const outcome = await body();
    results.push({
      id,
      name,
      status: outcome.pass ? "pass" : "fail",
      durationMs: Date.now() - started,
      detail: outcome.detail,
      ...(outcome.evidence === undefined ? {} : { evidence: outcome.evidence }),
    });
  } catch (error) {
    results.push({
      id,
      name,
      status: "fail",
      durationMs: Date.now() - started,
      detail: error instanceof Error ? error.stack || error.message : String(error),
    });
  }
  process.stderr.write(
    `[virtiofs-git] ${id}/11 ${results.at(-1).status} (${results.at(-1).durationMs} ms)\n`,
  );
};

const mount = {
  guestPath: mountPath,
  mountTag: `cv-${randomUUID().replaceAll("-", "").slice(0, 24)}`,
  readOnly: false,
  availability: "shared-storage",
  continuity: "restore-cross-node",
  backendProfile,
  vfsEndpoint: ownerEndpoint,
  vfsScopePath: scopePath,
};

let sandbox;
const sessions = [];
const createSession = async (suffix) => {
  const callbackRequestsBefore = gatewayRequestCount;
  const session = await withTimeout(
    sandbox.session({
      image: sandboxImage,
      architecture,
      name: `cv-vfs-${suffix}-${Date.now()}`,
      metadata: { role: "chevalier-vfs-virtiofs-git-conformance", probeId },
      autoStart: true,
      sharedMounts: [{ ...mount, mountTag: `${mount.mountTag}-${suffix}`.slice(0, 31) }],
    }),
    `create ${suffix} VM`,
    420_000,
  );
  sessions.push(session);
  const readinessPath = `.ready-${suffix}`;
  const readinessBytes = `callback-${suffix}-${probeId}`;
  for (let attempt = 0; attempt < 60; attempt += 1) {
    const ready = await execGuest(
      session,
      `test "$(findmnt -n -o FSTYPE ${mountPath})" = virtiofs &&
       printf '%s' '${readinessBytes}' >${mountPath}/${readinessPath}`,
      30,
    ).catch(() => undefined);
    if (ready?.code === 0 && gatewayRequestCount > callbackRequestsBefore) {
      const callbackBytes = await storage
        .read(`${scopePath}/${readinessPath}`)
        .catch(() => undefined);
      if (callbackBytes?.toString("utf8") === readinessBytes) {
        const removed = await execGuest(session, `rm ${mountPath}/${readinessPath}`, 30);
        if (removed.code === 0) {
          process.stderr.write(
            `[virtiofs-git] mounted disposable ${suffix} VM through ${mountPath} (gateway requests=${gatewayRequestCount})\n`,
          );
          return session;
        }
      }
    }
    await delay(1_000);
  }
  throw new Error(`${suffix} VM did not expose a writable virtiofs mount at ${mountPath}`);
};

const git = (args) => `git -C ${mountPath}/worktree ${args}`;

let first;
let second;
let expectedHead = "";
let gatewayRestartProtocolEvidence;
let posixModelEvidence;
const cleanup = {
  replacementDiscarded: false,
  sessionDiscardErrors: [],
  errors: [],
  gatewayStopped: false,
  backingRootRemoved: false,
};
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

  await check(1, "one-VM virtiofs topology, POSIX barriers, and HTTP coherence", async () => {
    const guest = await execGuest(
      first,
      `python3 - <<'PY'
import os, stat
assert os.popen("findmnt -n -o FSTYPE /workspace").read().strip() == "virtiofs"
p = "/workspace/coherent"
fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
os.write(fd, b"abcdefghij")
os.pwrite(fd, b"XYZ", 4)
os.fchmod(fd, 0o755)
os.ftruncate(fd, 9)
os.fsync(fd)
os.close(fd)
os.rename(p, "/workspace/coherent-renamed")
fd = os.open("/workspace/coherent-renamed", os.O_RDONLY)
os.fsync(fd)
os.close(fd)
print("GUEST_BARRIER_OK")

root = "/workspace/ops"
os.mkdir(root)
assert "ops" in os.listdir("/workspace")
assert stat.S_ISDIR(os.stat(root).st_mode)

mode_file = root + "/mode-file"
with open(mode_file, "wb") as f:
    f.write(b"mode")
    f.flush()
    os.fsync(f.fileno())
os.chmod(mode_file, 0o751)
mode = stat.S_IMODE(os.stat(mode_file).st_mode)
print(f"MODE_FILE_LOCAL_ACTUAL:{mode:o}")
assert mode == 0o751
os.symlink("mode-file", root + "/mode-link")
assert stat.S_ISLNK(os.lstat(root + "/mode-link").st_mode)
assert os.readlink(root + "/mode-link") == "mode-file"

sparse = root + "/sparse"
fd = os.open(sparse, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
os.ftruncate(fd, 1024 * 1024)
os.pwrite(fd, b"OFFSET", 777777)
os.fsync(fd)
assert os.pread(fd, 6, 777777) == b"OFFSET"
assert os.pread(fd, 16, 400000) == b"\\0" * 16
os.close(fd)

with open(root + "/replace-target", "wb") as f:
    f.write(b"old")
with open(root + "/replace-source", "wb") as f:
    f.write(b"new")
    f.flush()
    os.fsync(f.fileno())
os.replace(root + "/replace-source", root + "/replace-target")
dirfd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
os.fsync(dirfd)
os.close(dirfd)
assert open(root + "/replace-target", "rb").read() == b"new"
assert not os.path.exists(root + "/replace-source")

open_unlink = root + "/open-unlink"
fd = os.open(open_unlink, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
os.write(fd, b"survives-unlink")
os.fsync(fd)
os.lseek(fd, 0, os.SEEK_SET)
os.unlink(open_unlink)
assert not os.path.exists(open_unlink)
assert os.read(fd, 64) == b"survives-unlink"
os.close(fd)
assert not os.path.exists(open_unlink)

os.mkdir(root + "/empty")
with open(root + "/remove-me", "wb") as f:
    f.write(b"remove")
os.unlink(root + "/remove-me")
os.rmdir(root + "/empty")
assert not os.path.exists(root + "/remove-me")
assert not os.path.exists(root + "/empty")

session_shrink = "/workspace/session-shrink"
with open(session_shrink, "wb") as f:
    f.write(b"x" * 47 + b"\\n")
assert os.stat(session_shrink).st_size == 48
assert open(session_shrink, "rb").read() == b"x" * 47 + b"\\n"
print("ONE_VM_TOPOLOGY_AND_OPS_OK")
PY`,
    );
    const remotePath = `${scopePath}/coherent-renamed`;
    const response = await withTimeout(
      fetch(`${ownerEndpoint}/file/raw?path=${encodeURIComponent(remotePath)}`, {
        headers: { authorization: `Bearer ${vfsAuthToken}` },
      }),
      "one-VM raw HTTP coherence read",
      30_000,
    );
    const bytes = Buffer.from(await response.arrayBuffer()).toString("utf8");
    await first.writeFile("/workspace/session-shrink", Buffer.from("short\n"));
    await storage.write(`${scopePath}/host-written`, Buffer.from("host-visible\n"));
    const hostVisible = await execGuest(
      first,
      `python3 - <<'PY'
import os, stat
root = "/workspace/ops"
assert "ops" in os.listdir("/workspace")
assert stat.S_ISDIR(os.stat(root).st_mode)
mode = stat.S_IMODE(os.stat(root + "/mode-file").st_mode)
print(f"MODE_FILE_CROSS_ACTUAL:{mode:o}")
assert mode == 0o751
assert stat.S_ISLNK(os.lstat(root + "/mode-link").st_mode)
assert os.readlink(root + "/mode-link") == "mode-file"
fd = os.open(root + "/sparse", os.O_RDONLY)
assert os.fstat(fd).st_size == 1024 * 1024
assert os.pread(fd, 6, 777777) == b"OFFSET"
assert os.pread(fd, 16, 400000) == b"\\0" * 16
os.close(fd)
assert open(root + "/replace-target", "rb").read() == b"new"
assert not os.path.exists(root + "/replace-source")
assert not os.path.exists(root + "/open-unlink")
assert not os.path.exists(root + "/remove-me")
assert not os.path.exists(root + "/empty")
assert os.stat("/workspace/session-shrink").st_size == 6
assert open("/workspace/session-shrink", "rb").read() == b"short\\n"
assert open("/workspace/host-written", "rb").read() == b"host-visible\\n"
print("HOST_WRITE_VISIBLE_IN_ONE_VM")
PY`,
    );
    return {
      pass:
        guest.code === 0 &&
        response.ok &&
        bytes === "abcdXYZhi" &&
        hostVisible.code === 0,
      detail: `${commandResultText(guest)}\nhttp=${response.status} bytes=${JSON.stringify(bytes)}\n${commandResultText(hostVisible)}`,
    };
  });

  second = await createSession("b");

  await check(2, "same/cross-mount exclusive and ordinary O_CREAT convergence", async () => {
    const namespaceOrigin = await execGuest(
      first,
      `python3 - <<'PY'
import os, shutil, stat
root = "/workspace/namespace-projection"
shutil.rmtree(root, ignore_errors=True)
os.mkdir(root, 0o750)
source = root + "/source"
fd = os.open(source, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o640)
os.close(fd)
os.symlink("source", root + "/source-link")
os.link(source, root + "/source-alias")
os.rename(root + "/source-alias", root + "/renamed-alias")
os.chmod(source, 0o700)
probe = root + "/awaited-empty-probe"
fd = os.open(probe, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
os.close(fd)
assert os.path.exists(probe)
os.unlink(probe)
empty_dir = root + "/empty-dir"
os.mkdir(empty_dir)
os.rmdir(empty_dir)
assert stat.S_IMODE(os.stat(source).st_mode) == 0o700
assert os.stat(source).st_ino == os.stat(root + "/renamed-alias").st_ino
assert os.readlink(root + "/source-link") == "source"
assert not os.path.lexists(probe)
assert not os.path.lexists(empty_dir)
print("NAMESPACE_ORIGIN_OK")
PY`,
    );
    const namespaceSecond = await execGuest(
      second,
      `python3 - <<'PY'
import os, stat
root = "/workspace/namespace-projection"
source = root + "/source"
renamed = root + "/renamed-alias"
assert stat.S_IMODE(os.stat(root).st_mode) == 0o750
assert stat.S_IMODE(os.stat(source).st_mode) == 0o700
assert os.stat(source).st_ino == os.stat(renamed).st_ino
assert os.stat(source).st_nlink == 2
assert os.readlink(root + "/source-link") == "source"
assert not os.path.lexists(root + "/source-alias")
assert not os.path.lexists(root + "/awaited-empty-probe")
assert not os.path.lexists(root + "/empty-dir")

# Exercise the same namespace classes from the second mount, then let the
# originating mount prove the reverse direction.
os.chmod(source, 0o644)
os.rename(renamed, root + "/renamed-by-second")
os.link(source, root + "/alias-by-second")
os.unlink(root + "/alias-by-second")
os.symlink("source", root + "/link-by-second")
probe = root + "/second-empty-probe"
fd = os.open(probe, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
os.close(fd)
os.unlink(probe)
empty_dir = root + "/second-empty-dir"
os.mkdir(empty_dir)
os.rmdir(empty_dir)
print("NAMESPACE_SECOND_OK")
PY`,
    );
    const namespaceReverse = await execGuest(
      first,
      `python3 - <<'PY'
import os, stat
root = "/workspace/namespace-projection"
source = root + "/source"
assert stat.S_IMODE(os.stat(source).st_mode) == 0o644
assert os.stat(source).st_ino == os.stat(root + "/renamed-by-second").st_ino
assert os.readlink(root + "/link-by-second") == "source"
assert not os.path.lexists(root + "/renamed-alias")
assert not os.path.lexists(root + "/alias-by-second")
assert not os.path.lexists(root + "/second-empty-probe")
assert not os.path.lexists(root + "/second-empty-dir")
print("NAMESPACE_REVERSE_OK")
PY`,
    );
    const same = await execGuest(
      first,
      `rm -f /workspace/exclusive-same /tmp/exclusive-same-*
for n in 1 2; do
  (python3 - "$n" <<'PY'
import os, stat, sys
try:
    fd = os.open("/workspace/exclusive-same", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    os.close(fd)
    print("ACQUIRED:" + sys.argv[1])
except FileExistsError:
    print("REJECTED:" + sys.argv[1])
PY
  ) >"/tmp/exclusive-same-$n" 2>&1 &
done
wait
cat /tmp/exclusive-same-*`,
    );
    await execGuest(first, "rm -f /workspace/exclusive-cross");
    let readyCount = 0;
    let resolveReady;
    const bothReady = new Promise((resolve) => {
      resolveReady = resolve;
    });
    let resultCount = 0;
    let resolveResults;
    const bothResults = new Promise((resolve) => {
      resolveResults = resolve;
    });
    const contender = (label) => `cat >/tmp/exclusive-contender-${label}.py <<'PY'
import os, stat, sys
path = "/workspace/exclusive-cross"
os.umask(0)
try:
    os.lstat(path)
    raise AssertionError("exclusive path existed before synchronized create")
except FileNotFoundError:
    pass
print("EXCLUSIVE_READY:${label}", flush=True)
assert sys.stdin.readline() == "go\\n"
fd = None
try:
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o642)
    outcome = "ACQUIRED"
except FileExistsError:
    outcome = "EEXIST"
print("EXCLUSIVE_RESULT:${label}:" + outcome, flush=True)
assert sys.stdin.readline() == "finish\\n"
if fd is not None:
    opened = os.fstat(fd)
    assert stat.S_ISREG(opened.st_mode)
    assert stat.S_IMODE(opened.st_mode) == 0o642
    payload = b"WINNER:${label}\\n"
    os.write(fd, payload)
    os.fsync(fd)
    published = os.fstat(fd)
    assert published.st_ino == opened.st_ino
    assert published.st_size == len(payload)
    print(f"EXCLUSIVE_PUBLISHED:${label}:ino={published.st_ino}:mode={stat.S_IMODE(published.st_mode):o}", flush=True)
    os.close(fd)
else:
    print("EXCLUSIVE_REJECTED:${label}:EEXIST", flush=True)
PY`;
    const runContender = (session, label) =>
      execGuestAtMarkers(
        session,
        `${contender(label)}
python3 /tmp/exclusive-contender-${label}.py`,
        [
          {
            marker: `EXCLUSIVE_READY:${label}`,
            onMarker: async (handle) => {
              readyCount += 1;
              if (readyCount === 2) resolveReady();
              await waitForGuestBarrier(bothReady, "both exclusive contenders ready");
              await handle.write(Buffer.from("go\n"));
            },
          },
          {
            marker: `EXCLUSIVE_RESULT:${label}:`,
            onMarker: async (handle) => {
              resultCount += 1;
              if (resultCount === 2) resolveResults();
              await waitForGuestBarrier(bothResults, "both exclusive contenders resolved");
              await handle.write(Buffer.from("finish\n"));
              await handle.eof();
            },
          },
        ],
        60,
      );
    const [a, b] = await Promise.all([
      runContender(first, "A"),
      runContender(second, "B"),
    ]);
    const sameAcquired = (same.stdout.match(/ACQUIRED:/g) || []).length;
    const crossOutput = `${a.stdout}\n${b.stdout}`;
    const crossAcquired = (crossOutput.match(/EXCLUSIVE_RESULT:[AB]:ACQUIRED/g) || []).length;
    const crossRejected = (crossOutput.match(/EXCLUSIVE_RESULT:[AB]:EEXIST/g) || []).length;
    const winner = crossOutput.match(/EXCLUSIVE_RESULT:([AB]):ACQUIRED/)?.[1] ?? null;
    const publishedInode = Number(
      crossOutput.match(/EXCLUSIVE_PUBLISHED:[AB]:ino=([0-9]+):mode=642/)?.[1] ?? 0,
    );
    const verifyWinner = (label, requireInode) => `python3 - <<'PY'
import os, stat
path = "/workspace/exclusive-cross"
entry = os.stat(path)
assert stat.S_ISREG(entry.st_mode)
assert stat.S_IMODE(entry.st_mode) == 0o642
assert open(path, "rb").read() == b"WINNER:${label}\\n"
${requireInode ? `assert entry.st_ino == ${publishedInode}` : "assert entry.st_ino > 1"}
print(f"EXCLUSIVE_VISIBLE:${label}:ino={entry.st_ino}:mode={stat.S_IMODE(entry.st_mode):o}")
PY`;
    const [visibleA, visibleB, authoritativeEntry, authoritativeBytes] = await Promise.all([
      execGuest(first, verifyWinner(winner, winner === "A")),
      execGuest(second, verifyWinner(winner, winner === "B")),
      storage.stat(`${scopePath}/exclusive-cross`),
      storage.read(`${scopePath}/exclusive-cross`),
    ]);
    await execGuest(first, "rm -f /workspace/create-shared");
    let ordinaryReadyCount = 0;
    let resolveOrdinaryReady;
    const ordinaryReady = new Promise((resolve) => {
      resolveOrdinaryReady = resolve;
    });
    let ordinaryOpenCount = 0;
    let resolveOrdinaryOpen;
    const ordinaryOpen = new Promise((resolve) => {
      resolveOrdinaryOpen = resolve;
    });
    let resolveAPublished;
    const aPublished = new Promise((resolve) => {
      resolveAPublished = resolve;
    });
    let resolveBPublished;
    const bPublished = new Promise((resolve) => {
      resolveBPublished = resolve;
    });
    const ordinaryCreator = (label) => `cat >/tmp/ordinary-creator-${label}.py <<'PY'
import os, stat, sys
path = "/workspace/create-shared"
os.umask(0)
try:
    os.lstat(path)
    raise AssertionError("ordinary-create path existed before synchronized create")
except FileNotFoundError:
    pass
print("ORDINARY_READY:${label}", flush=True)
assert sys.stdin.readline() == "go\\n"
fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o640)
opened = os.fstat(fd)
assert stat.S_ISREG(opened.st_mode)
assert stat.S_IMODE(opened.st_mode) == 0o640
print(f"ORDINARY_OPENED:${label}:ino={opened.st_ino}", flush=True)
if "${label}" == "A":
    assert sys.stdin.readline() == "publish\\n"
    assert os.pwrite(fd, b"A", 0) == 1
    os.fsync(fd)
    print("ORDINARY_A_PUBLISHED", flush=True)
    assert sys.stdin.readline() == "verify\\n"
    assert os.pread(fd, 2, 0) == b"AB"
else:
    assert sys.stdin.readline() == "observe\\n"
    assert os.pread(fd, 1, 0) == b"A"
    assert os.pwrite(fd, b"B", 1) == 1
    os.fsync(fd)
    print("ORDINARY_B_PUBLISHED", flush=True)
    assert sys.stdin.readline() == "verify\\n"
    assert os.pread(fd, 2, 0) == b"AB"
closed = os.fstat(fd)
assert closed.st_ino == opened.st_ino
assert stat.S_IMODE(closed.st_mode) == 0o640
os.close(fd)
print("ORDINARY_DONE:${label}", flush=True)
PY`;
    const runOrdinaryCreator = (session, label) =>
      execGuestAtMarkers(
        session,
        `${ordinaryCreator(label)}
python3 /tmp/ordinary-creator-${label}.py`,
        [
          {
            marker: `ORDINARY_READY:${label}`,
            onMarker: async (handle) => {
              ordinaryReadyCount += 1;
              if (ordinaryReadyCount === 2) resolveOrdinaryReady();
              await waitForGuestBarrier(ordinaryReady, "both ordinary creators ready");
              await handle.write(Buffer.from("go\n"));
            },
          },
          {
            marker: `ORDINARY_OPENED:${label}:`,
            onMarker: async (handle) => {
              ordinaryOpenCount += 1;
              if (ordinaryOpenCount === 2) resolveOrdinaryOpen();
              await waitForGuestBarrier(ordinaryOpen, "both ordinary creators opened");
              if (label === "A") {
                await handle.write(Buffer.from("publish\n"));
              } else {
                await waitForGuestBarrier(aPublished, "ordinary creator A published");
                await handle.write(Buffer.from("observe\n"));
              }
            },
          },
          ...(label === "A"
            ? [
                {
                  marker: "ORDINARY_A_PUBLISHED",
                  onMarker: async (handle) => {
                    resolveAPublished();
                    await waitForGuestBarrier(bPublished, "ordinary creator B published");
                    await handle.write(Buffer.from("verify\n"));
                    await handle.eof();
                  },
                },
              ]
            : [
                {
                  marker: "ORDINARY_B_PUBLISHED",
                  onMarker: async (handle) => {
                    resolveBPublished();
                    await handle.write(Buffer.from("verify\n"));
                    await handle.eof();
                  },
                },
              ]),
        ],
        60,
      );
    const [ordinaryA, ordinaryB] = await Promise.all([
      runOrdinaryCreator(first, "A"),
      runOrdinaryCreator(second, "B"),
    ]);
    const [
      ordinaryVisibleA,
      ordinaryVisibleB,
      ordinaryAuthoritativeEntry,
      ordinaryAuthoritativeBytes,
    ] = await Promise.all([
      execGuest(
        first,
        `python3 - <<'PY'
import os, stat
entry = os.stat("/workspace/create-shared")
assert stat.S_IMODE(entry.st_mode) == 0o640
assert open("/workspace/create-shared", "rb").read() == b"AB"
print(f"ORDINARY_VISIBLE:A:ino={entry.st_ino}")
PY`,
      ),
      execGuest(
        second,
        `python3 - <<'PY'
import os, stat
entry = os.stat("/workspace/create-shared")
assert stat.S_IMODE(entry.st_mode) == 0o640
assert open("/workspace/create-shared", "rb").read() == b"AB"
print(f"ORDINARY_VISIBLE:B:ino={entry.st_ino}")
PY`,
      ),
      storage.stat(`${scopePath}/create-shared`),
      storage.read(`${scopePath}/create-shared`),
    ]);
    return {
      pass:
        namespaceOrigin.code === 0 &&
        namespaceSecond.code === 0 &&
        namespaceReverse.code === 0 &&
        same.code === 0 &&
        a.code === 0 &&
        b.code === 0 &&
        sameAcquired === 1 &&
        crossAcquired === 1 &&
        crossRejected === 1 &&
        publishedInode > 1 &&
        visibleA.code === 0 &&
        visibleB.code === 0 &&
        authoritativeEntry !== null &&
        authoritativeEntry.fileId !== undefined &&
        authoritativeEntry.mode === 0o642 &&
        authoritativeBytes.toString("utf8") === `WINNER:${winner}\n` &&
        ordinaryA.code === 0 &&
        ordinaryB.code === 0 &&
        ordinaryVisibleA.code === 0 &&
        ordinaryVisibleB.code === 0 &&
        ordinaryAuthoritativeEntry !== null &&
        ordinaryAuthoritativeEntry.fileId !== undefined &&
        ordinaryAuthoritativeEntry.mode === 0o640 &&
        ordinaryAuthoritativeBytes.toString("utf8") === "AB",
      detail: [
        commandResultText(namespaceOrigin),
        commandResultText(namespaceSecond),
        commandResultText(namespaceReverse),
        `same acquired=${sameAcquired}`,
        `cross acquired=${crossAcquired} rejected=${crossRejected} winner=${winner}`,
        commandResultText(a),
        commandResultText(b),
        commandResultText(visibleA),
        commandResultText(visibleB),
        `authoritative fileId=${authoritativeEntry?.fileId ?? "missing"} mode=${authoritativeEntry?.mode?.toString(8) ?? "missing"} bytes=${JSON.stringify(authoritativeBytes.toString("utf8"))}`,
        commandResultText(ordinaryA),
        commandResultText(ordinaryB),
        commandResultText(ordinaryVisibleA),
        commandResultText(ordinaryVisibleB),
        `ordinary authoritative fileId=${ordinaryAuthoritativeEntry?.fileId ?? "missing"} mode=${ordinaryAuthoritativeEntry?.mode?.toString(8) ?? "missing"} bytes=${JSON.stringify(ordinaryAuthoritativeBytes.toString("utf8"))}`,
      ].join("\n"),
    };
  });

  await check(3, "same/cross-mount flock, POSIX ranges, release, and blocking", async () => {
    await execGuest(first, "rm -f /workspace/advisory.*");
    const same = await execGuest(
      first,
      `exec 8>/workspace/advisory.same
flock -n 8
(exec 9>/workspace/advisory.same; if flock -n 9; then echo SAME_WRONG; else echo SAME_REJECTED; fi)
flock -u 8
(exec 9>/workspace/advisory.same; flock -n 9; echo SAME_REACQUIRED)
python3 - <<'PY'
import fcntl, os
p="/workspace/advisory.same-range"
fd=os.open(p, os.O_CREAT|os.O_RDWR, 0o644)
fcntl.lockf(fd, fcntl.LOCK_EX, 8, 0)
pid=os.fork()
if pid == 0:
    contender=os.open(p, os.O_RDWR)
    try:
        fcntl.lockf(contender, fcntl.LOCK_EX|fcntl.LOCK_NB, 8, 0)
        print("SAME_RANGE_WRONG", flush=True)
        os._exit(2)
    except BlockingIOError:
        print("SAME_RANGE_REJECTED", flush=True)
        os._exit(0)
_,status=os.waitpid(pid, 0)
assert os.waitstatus_to_exitcode(status) == 0
os.close(fd)
reacquire=os.open(p, os.O_RDWR)
fcntl.lockf(reacquire, fcntl.LOCK_EX|fcntl.LOCK_NB, 8, 0)
print("SAME_RANGE_REACQUIRED")
os.close(reacquire)
PY`,
    );
    const holderHandle = await startGuest(
      first,
      `exec 9>/workspace/advisory.flock; flock -n 9; echo FLOCK_HELD:A; sleep 4`,
      30,
    );
    await delay(1_000);
    const rejected = await execGuest(
      second,
      `exec 9>/workspace/advisory.flock
if flock -n 9; then echo FLOCK_WRONG; else echo FLOCK_REJECTED:B; fi`,
      30,
    );
    const holder = await drainExec(holderHandle, "flock holder");
    const reacquired = await execGuest(
      second,
      `exec 9>/workspace/advisory.flock; flock -n 9; echo FLOCK_REACQUIRED:B`,
      30,
    );
    const rangeHolderHandle = await startGuest(
      first,
      `python3 - <<'PY'
import fcntl, os, time
fd=os.open("/workspace/advisory.range", os.O_CREAT|os.O_RDWR, 0o644)
fcntl.lockf(fd, fcntl.LOCK_EX, 8, 0)
print("RANGE_HELD:A", flush=True)
time.sleep(4)
os.close(fd)
PY`,
      30,
    );
    await delay(1_000);
    const range = await execGuest(
      second,
      `python3 - <<'PY'
import fcntl, os
fd=os.open("/workspace/advisory.range", os.O_RDWR)
try:
    fcntl.lockf(fd, fcntl.LOCK_EX|fcntl.LOCK_NB, 8, 0)
    print("RANGE_WRONG")
except BlockingIOError:
    print("RANGE_REJECTED:B")
fcntl.lockf(fd, fcntl.LOCK_EX|fcntl.LOCK_NB, 8, 16)
print("RANGE_DISJOINT:B")
os.close(fd)
PY`,
      30,
    );
    const rangeHolder = await drainExec(rangeHolderHandle, "range holder");
    const blockingHolderHandle = await startGuest(
      first,
      `exec 9>/workspace/advisory.blocking; flock 9; echo BLOCK_HELD:A; sleep 3`,
      30,
    );
    await delay(1_000);
    const blockingContenderHandle = await startGuest(
      second,
      `exec 9>/workspace/advisory.blocking; flock 9; echo BLOCK_ACQUIRED:B`,
      30,
    );
    const blockingHolder = await drainExec(blockingHolderHandle, "blocking holder");
    const blocking = await drainExec(blockingContenderHandle, "blocking contender");
    const output = [
      same,
      holder,
      rejected,
      reacquired,
      rangeHolder,
      range,
      blockingHolder,
      blocking,
    ]
      .map(commandResultText)
      .join("\n");
    return {
      pass:
        output.includes("SAME_REJECTED") &&
        output.includes("SAME_REACQUIRED") &&
        output.includes("SAME_RANGE_REJECTED") &&
        output.includes("SAME_RANGE_REACQUIRED") &&
        output.includes("FLOCK_REJECTED:B") &&
        output.includes("FLOCK_REACQUIRED:B") &&
        output.includes("RANGE_REJECTED:B") &&
        output.includes("RANGE_DISJOINT:B") &&
        output.includes("BLOCK_ACQUIRED:B"),
      detail: output,
    };
  });

  await check(4, "three-alias hard-link identity, mutation, rename, unlink, and open lifetime", async () => {
    const a = await execGuest(
      first,
      `rm -f /workspace/hard-a /workspace/hard-b /workspace/hard-c /workspace/hard-renamed
python3 - <<'PY'
import os, stat
paths = ["/workspace/hard-a", "/workspace/hard-b", "/workspace/hard-c"]
with open(paths[0], "wb") as f:
    f.write(b"before")
    f.flush()
    os.fsync(f.fileno())
os.link(paths[0], paths[1])
os.link(paths[1], paths[2])
dirfd = os.open("/workspace", os.O_RDONLY | os.O_DIRECTORY)
os.fsync(dirfd)
os.close(dirfd)
stats = [os.stat(path) for path in paths]
assert len({entry.st_ino for entry in stats}) == 1
assert {entry.st_nlink for entry in stats} == {3}
with open(paths[1], "r+b") as f:
    f.seek(0)
    f.write(b"local-alias")
    f.truncate()
    f.flush()
    os.fsync(f.fileno())
assert all(open(path, "rb").read() == b"local-alias" for path in paths)
print(f"HARD_LOCAL_THREE_OK:ino={stats[0].st_ino}:nlink={stats[0].st_nlink}")
PY`,
    );
    const b = await execGuest(
      second,
      `python3 - <<'PY'
import os
paths = ["/workspace/hard-a", "/workspace/hard-b", "/workspace/hard-c"]
stats = [os.stat(path) for path in paths]
assert len({entry.st_ino for entry in stats}) == 1
assert {entry.st_nlink for entry in stats} == {3}
assert all(open(path, "rb").read() == b"local-alias" for path in paths)
with open(paths[2], "r+b") as f:
    f.seek(0)
    f.write(b"CROSS-ALIAS")
    f.truncate()
    f.flush()
    os.fsync(f.fileno())
assert all(open(path, "rb").read() == b"CROSS-ALIAS" for path in paths)
inode = stats[0].st_ino
os.rename(paths[1], "/workspace/hard-renamed")
dirfd = os.open("/workspace", os.O_RDONLY | os.O_DIRECTORY)
os.fsync(dirfd)
os.close(dirfd)
renamed = ["/workspace/hard-a", "/workspace/hard-c", "/workspace/hard-renamed"]
stats = [os.stat(path) for path in renamed]
assert {entry.st_ino for entry in stats} == {inode}
assert {entry.st_nlink for entry in stats} == {3}
os.unlink("/workspace/hard-a")
dirfd = os.open("/workspace", os.O_RDONLY | os.O_DIRECTORY)
os.fsync(dirfd)
os.close(dirfd)
remaining = ["/workspace/hard-c", "/workspace/hard-renamed"]
stats = [os.stat(path) for path in remaining]
assert {entry.st_ino for entry in stats} == {inode}
assert {entry.st_nlink for entry in stats} == {2}
assert all(open(path, "rb").read() == b"CROSS-ALIAS" for path in remaining)
print(f"HARD_CROSS_RENAME_UNLINK_OK:ino={inode}:nlink=2")
PY`,
    );
    const [authoritativeC, authoritativeRenamed, authoritativeCBytes, authoritativeRenamedBytes] =
      await Promise.all([
        storage.stat(`${scopePath}/hard-c`),
        storage.stat(`${scopePath}/hard-renamed`),
        storage.read(`${scopePath}/hard-c`).catch(() => null),
        storage.read(`${scopePath}/hard-renamed`).catch(() => null),
      ]);
    const authoritative = {
      cExists: authoritativeC !== null,
      renamedExists: authoritativeRenamed !== null,
      sameIdentity:
        authoritativeC?.fileId !== undefined &&
        authoritativeC.fileId === authoritativeRenamed?.fileId,
      cLinkCount: authoritativeC?.linkCount?.toString() ?? null,
      renamedLinkCount: authoritativeRenamed?.linkCount?.toString() ?? null,
      bytesAliased:
        authoritativeCBytes?.toString("utf8") === "CROSS-ALIAS" &&
        authoritativeRenamedBytes?.toString("utf8") === "CROSS-ALIAS",
    };
    let remoteUnlink = null;
    const crossMountOpenLifetime = await execGuestAtMarkers(
      first,
      `cat >/tmp/hard-remote-open-lifetime.py <<'PY'
import os, stat, sys
c = "/workspace/hard-c"
renamed = "/workspace/hard-renamed"
sc, sr = os.stat(c), os.stat(renamed)
assert sc.st_ino == sr.st_ino and sc.st_nlink == sr.st_nlink == 2
fd = os.open(renamed, os.O_RDWR)
inode = os.fstat(fd).st_ino
print(f"HARD_REMOTE_UNLINK_READY:ino={inode}", flush=True)
assert sys.stdin.readline() == "continue\\n", "marker continuation"
assert os.pread(fd, 11, 0) == b"CROSS-ALIAS", "open descriptor bytes after remote unlink/reuse"
os.fchmod(fd, 0o640)
os.ftruncate(fd, 0)
os.write(fd, b"REMOTE-OPEN")
os.fsync(fd)
replacement = os.stat(renamed)
surviving = os.stat(c)
assert replacement.st_ino != inode, "replacement inode identity"
assert surviving.st_ino == inode, "surviving alias inode identity"
assert replacement.st_nlink == surviving.st_nlink == os.fstat(fd).st_nlink == 1, "post-unlink link counts"
assert stat.S_IMODE(os.fstat(fd).st_mode) == 0o640, "open descriptor mode"
assert stat.S_IMODE(surviving.st_mode) == 0o640, "surviving alias mode"
assert stat.S_IMODE(replacement.st_mode) == 0o644, "replacement mode"
assert os.pread(fd, 11, 0) == b"REMOTE-OPEN", "open descriptor bytes after descriptor write"
assert open(c, "rb").read() == b"REMOTE-OPEN", "surviving alias bytes after descriptor write"
assert open(renamed, "rb").read() == b"REPLACEMENT", "replacement bytes"
os.close(fd)
print(f"HARD_REMOTE_UNLINK_REUSE_FSYNC_OK:ino={inode}:replacement={replacement.st_ino}:nlink=1")
PY
python3 /tmp/hard-remote-open-lifetime.py`,
      [
        {
          marker: "HARD_REMOTE_UNLINK_READY:",
          onMarker: async (handle) => {
            remoteUnlink = await execGuest(
              second,
              `python3 - <<'PY'
import os, stat
c = "/workspace/hard-c"
renamed = "/workspace/hard-renamed"
sc, sr = os.stat(c), os.stat(renamed)
assert sc.st_ino == sr.st_ino and sc.st_nlink == sr.st_nlink == 2
os.unlink(renamed)
dirfd = os.open("/workspace", os.O_RDONLY | os.O_DIRECTORY)
os.fsync(dirfd)
os.close(dirfd)
assert not os.path.lexists(renamed)
assert os.stat(c).st_ino == sc.st_ino
assert os.stat(c).st_nlink == 1
with open(renamed, "xb") as replacement:
    replacement.write(b"CROSS-ALIAS")
    replacement.flush()
    os.fsync(replacement.fileno())
os.chmod(renamed, 0o644)
with open(renamed, "r+b") as replacement:
    replacement.seek(0)
    replacement.write(b"REPLACEMENT")
    replacement.truncate()
    replacement.flush()
    os.fsync(replacement.fileno())
dirfd = os.open("/workspace", os.O_RDONLY | os.O_DIRECTORY)
os.fsync(dirfd)
os.close(dirfd)
replacement = os.stat(renamed)
assert replacement.st_ino != sc.st_ino and replacement.st_nlink == 1
assert stat.S_IMODE(replacement.st_mode) == 0o644
assert open(c, "rb").read() == b"CROSS-ALIAS"
assert open(renamed, "rb").read() == b"REPLACEMENT"
print(f"HARD_REMOTE_ALIAS_REUSED:old={sc.st_ino}:replacement={replacement.st_ino}:nlink=1")
PY`,
            );
            await handle.write(Buffer.from("continue\n"));
          },
        },
      ],
    );
    const [
      authoritativeCAfter,
      authoritativeRenamedAfter,
      authoritativeCBytesAfter,
      authoritativeRenamedBytesAfter,
    ] =
      await Promise.all([
        storage.stat(`${scopePath}/hard-c`),
        storage.stat(`${scopePath}/hard-renamed`),
        storage.read(`${scopePath}/hard-c`).catch(() => null),
        storage.read(`${scopePath}/hard-renamed`).catch(() => null),
      ]);
    const authoritativeAfterRemoteReuse = {
      cExists: authoritativeCAfter !== null,
      renamedExists: authoritativeRenamedAfter !== null,
      sameIdentity:
        authoritativeCAfter?.fileId !== undefined &&
        authoritativeCAfter.fileId === authoritativeRenamedAfter?.fileId,
      cLinkCount: authoritativeCAfter?.linkCount?.toString() ?? null,
      renamedLinkCount: authoritativeRenamedAfter?.linkCount?.toString() ?? null,
      cMode: authoritativeCAfter?.mode?.toString(8) ?? null,
      renamedMode: authoritativeRenamedAfter?.mode?.toString(8) ?? null,
      cBytes: authoritativeCBytesAfter?.toString("utf8") ?? null,
      renamedBytes: authoritativeRenamedBytesAfter?.toString("utf8") ?? null,
    };
    const finalOpenLifetime = await execGuest(
      first,
      `python3 - <<'PY'
import os
c = "/workspace/hard-c"
assert open(c, "rb").read() == b"REMOTE-OPEN"
fd = os.open(c, os.O_RDWR)
inode = os.fstat(fd).st_ino
os.unlink(c)
dirfd = os.open("/workspace", os.O_RDONLY | os.O_DIRECTORY)
os.fsync(dirfd)
os.close(dirfd)
assert not os.path.lexists(c)
assert os.fstat(fd).st_ino == inode and os.fstat(fd).st_nlink == 0
os.pwrite(fd, b"!", 11)
os.fsync(fd)
assert os.pread(fd, 12, 0) == b"REMOTE-OPEN!"
os.close(fd)
assert not os.path.lexists(c)
assert open("/workspace/hard-renamed", "rb").read() == b"REPLACEMENT"
print(f"HARD_OPEN_FINAL_UNLINK_OK:ino={inode}:nlink=0")
PY`,
    );
    const final = await execGuest(
      second,
      `python3 - <<'PY'
import os
for path in (
    "/workspace/hard-a",
    "/workspace/hard-b",
    "/workspace/hard-c",
):
    assert not os.path.lexists(path), path
assert open("/workspace/hard-renamed", "rb").read() == b"REPLACEMENT"
os.unlink("/workspace/hard-renamed")
dirfd = os.open("/workspace", os.O_RDONLY | os.O_DIRECTORY)
os.fsync(dirfd)
os.close(dirfd)
assert not os.path.lexists("/workspace/hard-renamed")
print("HARD_PATHS_NEVER_RESURRECTED")
PY`,
    );
    return {
      pass:
        a.code === 0 &&
        b.code === 0 &&
        authoritative.cExists &&
        authoritative.renamedExists &&
        authoritative.sameIdentity &&
        authoritative.cLinkCount === "2" &&
        authoritative.renamedLinkCount === "2" &&
        authoritative.bytesAliased &&
        remoteUnlink?.code === 0 &&
        crossMountOpenLifetime.code === 0 &&
        authoritativeAfterRemoteReuse.cExists &&
        authoritativeAfterRemoteReuse.renamedExists &&
        !authoritativeAfterRemoteReuse.sameIdentity &&
        authoritativeAfterRemoteReuse.cLinkCount === "1" &&
        authoritativeAfterRemoteReuse.renamedLinkCount === "1" &&
        authoritativeAfterRemoteReuse.cMode === "640" &&
        authoritativeAfterRemoteReuse.renamedMode === "644" &&
        authoritativeAfterRemoteReuse.cBytes === "REMOTE-OPEN" &&
        authoritativeAfterRemoteReuse.renamedBytes === "REPLACEMENT" &&
        finalOpenLifetime.code === 0 &&
        final.code === 0,
      detail:
        `${[a, b, remoteUnlink, crossMountOpenLifetime, finalOpenLifetime, final]
          .filter(Boolean)
          .map(commandResultText)
          .join("\n")}` +
        `\nauthoritative=${JSON.stringify(authoritative)}` +
        `\nauthoritativeAfterRemoteReuse=${JSON.stringify(authoritativeAfterRemoteReuse)}`,
      evidence: { authoritative, authoritativeAfterRemoteReuse },
    };
  });

  await check(5, "Git init, commit, branch, merge, rebase, and stash", async () => {
    const command = await execGuest(
      first,
      `rm -rf /workspace/worktree
mkdir /workspace/worktree
${git("init")}
default_branch="$(${git("symbolic-ref --short HEAD")})"
test -n "$default_branch"
printf base\\n >/workspace/worktree/base.txt
${git("add base.txt")}
${git("commit -m base")}
${git("checkout -b feature")}
printf feature\\n >/workspace/worktree/feature.txt
${git("add feature.txt")}
${git("commit -m feature")}
${git('checkout "$default_branch"')}
${git("merge --no-edit feature")}
${git("checkout -b rebase-target")}
printf rebase\\n >/workspace/worktree/rebase.txt
${git("add rebase.txt")}
${git("commit -m rebase")}
${git('checkout "$default_branch"')}
printf upstream\\n >/workspace/worktree/upstream.txt
${git("add upstream.txt")}
${git("commit -m upstream")}
${git("checkout rebase-target")}
${git('rebase "$default_branch"')}
printf dirty\\n >>/workspace/worktree/base.txt
${git("stash push -m harness")}
${git("stash show --stat stash@{0}")}
${git("fsck --full")}
echo GIT_LIFECYCLE_OK`,
      900,
    );
    return {
      pass: command.code === 0 && command.stdout.includes("GIT_LIFECYCLE_OK"),
      detail: commandResultText(command),
    };
  });

  let gitWorkloadEvidence = null;
  await check(6, "Git small-file correctness with five-minute liveness ceiling", async () => {
    const phaseLabels = [
      "create_1000",
      "add_1000",
      "commit_1000",
      "status_cold",
      "status_warm",
      "gc",
      "fsck_full",
    ];
    let phaseRequestBaseline = snapshotGatewayRequestCounts();
    const gatewayRequestsByPhase = {};
    const command = await execGuestAtMarkers(
      first,
      `measure() {
  local label="$1"
  shift
  local started finished
  started=$(date +%s%N)
  "$@"
  finished=$(date +%s%N)
  echo "GIT_TIMING_MS:\${label}:$(((finished - started) / 1000000))" >&2
}
started=$(date +%s%N)
python3 - <<'PY'
import os
root="/workspace/worktree/many"
os.makedirs(root, exist_ok=True)
for i in range(${gitWorkloadFileCount}):
    with open(os.path.join(root, f"file-{i:04d}.txt"), "wb") as f:
        f.write((f"{i:04d}:" + "x"*4089 + "\\n").encode())
    if (i + 1) % 100 == 0:
        print(f"GIT_CREATE_PROGRESS:{i + 1}", flush=True)
PY
finished=$(date +%s%N)
echo "GIT_TIMING_MS:create_1000:$(((finished - started) / 1000000))" >&2
echo GIT_PHASE_DONE:create_1000
read -r phase_continue
measure add_1000 git -C /workspace/worktree add many
echo GIT_PHASE_DONE:add_1000
read -r phase_continue
measure commit_1000 git -C /workspace/worktree commit --quiet -m many
echo GIT_PHASE_DONE:commit_1000
read -r phase_continue
started=$(date +%s%N)
${git("status --porcelain")} >/tmp/status-first
finished=$(date +%s%N)
echo "GIT_TIMING_MS:status_cold:$(((finished - started) / 1000000))" >&2
echo GIT_PHASE_DONE:status_cold
read -r phase_continue
started=$(date +%s%N)
${git("status --porcelain")} >/tmp/status-warm
finished=$(date +%s%N)
echo "GIT_TIMING_MS:status_warm:$(((finished - started) / 1000000))" >&2
echo GIT_PHASE_DONE:status_warm
read -r phase_continue
test ! -s /tmp/status-first
test ! -s /tmp/status-warm
measure gc git -C /workspace/worktree gc
echo GIT_PHASE_DONE:gc
read -r phase_continue
measure fsck_full git -C /workspace/worktree fsck --full
echo GIT_PHASE_DONE:fsck_full
read -r phase_continue
echo GIT_WORKLOAD_OK`,
      phaseLabels.map((label, index) => ({
        marker: `GIT_PHASE_DONE:${label}`,
        onMarker: async (handle) => {
          gatewayRequestsByPhase[label] = gatewayRequestDelta(phaseRequestBaseline);
          phaseRequestBaseline = snapshotGatewayRequestCounts();
          await handle.write(Buffer.from("continue\n"));
          if (index === phaseLabels.length - 1) await handle.eof();
        },
      })),
      300,
    );
    const timingText = `${command.stdout}\n${command.stderr}`;
    const timingsMs = Object.fromEntries(
      [...timingText.matchAll(/GIT_TIMING_MS:([^:\s]+):(\d+)/g)].map((match) => [
        match[1],
        Number(match[2]),
      ]),
    );
    gitWorkloadEvidence = {
      fileCount: gitWorkloadFileCount,
      timingsMs,
      gatewayRequestsByPhase,
    };
    return {
      pass:
        command.code === 0 &&
        command.stdout.includes("GIT_WORKLOAD_OK") &&
        Object.keys(timingsMs).length === 7 &&
        !/input\/output error|\bEIO\b/i.test(`${command.stdout}\n${command.stderr}`),
      detail: commandResultText(command),
      evidence: gitWorkloadEvidence,
    };
  });

  await check(7, "cross-mount exact HEAD and close-barrier visibility", async () => {
    const writer = await execGuest(
      first,
      `printf 'barrier\\n' >/workspace/worktree/barrier.txt
python3 - <<'PY'
import os, stat
root = "/workspace/worktree"
package = root + "/.pnpm/pkg@1.0.0/node_modules/pkg"
os.makedirs(package, exist_ok=True)
with open(package + "/index.js", "wb") as file:
    file.write(b"module.exports = 'mounted-symlink-target'\\n")
    file.flush()
    os.fsync(file.fileno())
os.makedirs(root + "/node_modules", exist_ok=True)
os.makedirs(root + "/packages/app/node_modules", exist_ok=True)
os.symlink("../.pnpm/pkg@1.0.0/node_modules/pkg", root + "/node_modules/pkg")
os.symlink("../.pnpm/missing@1.0.0/node_modules/missing", root + "/node_modules/missing")
os.symlink("../../../node_modules/pkg", root + "/packages/app/node_modules/pkg")
for directory in (
    root,
    root + "/node_modules",
    root + "/packages/app/node_modules",
    package,
):
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    os.fsync(descriptor)
    os.close(descriptor)
assert stat.S_ISLNK(os.lstat(root + "/node_modules/pkg").st_mode)
assert stat.S_ISLNK(os.lstat(root + "/node_modules/missing").st_mode)
assert stat.S_ISLNK(os.lstat(root + "/packages/app/node_modules/pkg").st_mode)
assert os.readlink(root + "/node_modules/pkg") == "../.pnpm/pkg@1.0.0/node_modules/pkg"
assert os.readlink(root + "/node_modules/missing") == "../.pnpm/missing@1.0.0/node_modules/missing"
assert os.readlink(root + "/packages/app/node_modules/pkg") == "../../../node_modules/pkg"
assert open(root + "/packages/app/node_modules/pkg/index.js", "rb").read() == b"module.exports = 'mounted-symlink-target'\\n"
assert not os.path.exists(root + "/node_modules/missing")
print("SYMLINK_TREE_WRITER_OK")
PY
${git("add barrier.txt .pnpm node_modules packages")}
${git("commit -m barrier")}
${git("rev-parse HEAD")}`,
    );
    expectedHead = writer.stdout.trim().split(/\s+/).at(-1) || "";
    const reader = await execGuest(
      second,
      `${git("rev-parse HEAD")}
python3 - <<'PY'
import hashlib, os, stat
root = "/workspace/worktree"
barrier = open(root + "/barrier.txt", "rb").read()
print(f"BARRIER_CROSS_SIZE:{len(barrier)}:SHA256:{hashlib.sha256(barrier).hexdigest()}")
assert barrier == b"barrier\\n"
assert stat.S_IMODE(os.stat("/workspace/ops/mode-file").st_mode) == 0o751
assert open("/workspace/ops/mode-file", "rb").read() == b"mode"
sparse = os.open("/workspace/ops/sparse", os.O_RDONLY)
assert os.fstat(sparse).st_size == 1024 * 1024
assert os.pread(sparse, 6, 777777) == b"OFFSET"
os.close(sparse)
assert open("/workspace/ops/replace-target", "rb").read() == b"new"
assert open("/workspace/coherent-renamed", "rb").read() == b"abcdXYZhi"
links = {
    root + "/node_modules/pkg": "../.pnpm/pkg@1.0.0/node_modules/pkg",
    root + "/node_modules/missing": "../.pnpm/missing@1.0.0/node_modules/missing",
    root + "/packages/app/node_modules/pkg": "../../../node_modules/pkg",
}
for path, target in links.items():
    assert stat.S_ISLNK(os.lstat(path).st_mode)
    assert os.readlink(path) == target
assert open(root + "/node_modules/pkg/index.js", "rb").read() == b"module.exports = 'mounted-symlink-target'\\n"
assert open(root + "/packages/app/node_modules/pkg/index.js", "rb").read() == b"module.exports = 'mounted-symlink-target'\\n"
assert not os.path.exists(root + "/node_modules/missing")
print("SYMLINK_TREE_CROSS_MOUNT_OK")
PY
${git("fsck --full")}`,
      900,
    );
    const actualHead = reader.stdout.trim().split(/\s+/)[0] || "";
    return {
      pass: writer.code === 0 && reader.code === 0 && expectedHead !== "" && actualHead === expectedHead,
      detail: `expected=${expectedHead}; actual=${actualHead}\n${commandResultText(reader)}`,
    };
  });

  await check(8, "gateway interruption fails honestly, then exact replay recovers", async () => {
    const requestsBeforeOutage = gatewayRequestCount;
    let outage;
    await stopGateway();
    try {
      outage = await execGuest(
        first,
        `python3 - <<'PY'
import errno, os, sys
tmp = "/workspace/recovery-inflight.tmp"
final = "/workspace/recovery-inflight"
try:
    fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
    os.write(fd, b"must-not-report-success")
    os.fsync(fd)
    os.close(fd)
    os.rename(tmp, final)
    dirfd = os.open("/workspace", os.O_RDONLY | os.O_DIRECTORY)
    os.fsync(dirfd)
    os.close(dirfd)
except OSError as error:
    print(f"GATEWAY_OUTAGE_REJECTED:errno={error.errno}:name={errno.errorcode.get(error.errno, 'UNKNOWN')}")
    sys.exit(0)
print("GATEWAY_OUTAGE_FALSE_SUCCESS")
sys.exit(3)
PY`,
        45,
      );
    } finally {
      await startGateway();
    }

    gatewayRestartProtocolEvidence = await withTimeout(
      runVfsGatewayProtocolProbe({
        ownerEndpoint,
        authToken: vfsAuthToken,
        scopePath,
      }),
      "post-restart callback gateway protocol probe",
      30_000,
    );
    const replay = await execGuest(
      first,
      `rm -f /workspace/recovery-inflight.tmp /workspace/recovery-inflight
python3 - <<'PY'
import os
tmp = "/workspace/recovery-inflight.tmp"
final = "/workspace/recovery-inflight"
payload = b"replayed-after-gateway-restart\\n"
fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
os.write(fd, payload)
os.fsync(fd)
os.close(fd)
os.rename(tmp, final)
dirfd = os.open("/workspace", os.O_RDONLY | os.O_DIRECTORY)
os.fsync(dirfd)
os.close(dirfd)
assert open(final, "rb").read() == payload
print("GATEWAY_REPLAY_WRITER_OK")
PY`,
    );
    const crossMount = await execGuest(
      second,
      `test "$(cat /workspace/recovery-inflight)" = "replayed-after-gateway-restart" &&
       echo GATEWAY_REPLAY_CROSS_MOUNT_OK`,
    );
    const recoveredBytes = await storage.read(`${scopePath}/recovery-inflight`);
    return {
      pass:
        outage.code === 0 &&
        outage.stdout.includes("GATEWAY_OUTAGE_REJECTED:") &&
        !outage.stdout.includes("GATEWAY_OUTAGE_FALSE_SUCCESS") &&
        replay.code === 0 &&
        crossMount.code === 0 &&
        recoveredBytes.toString("utf8") === "replayed-after-gateway-restart\n" &&
        gatewayRequestCount > requestsBeforeOutage &&
        gatewayRestartProtocolEvidence.authentication === "401 enforced",
      detail: [
        `requestsBeforeOutage=${requestsBeforeOutage}; requestsAfterRecovery=${gatewayRequestCount}`,
        commandResultText(outage),
        commandResultText(replay),
        commandResultText(crossMount),
      ].join("\n"),
    };
  });

  await check(9, "seeded one-client and alternating two-client POSIX model torture", async () => {
    posixModelEvidence = await runPosixModelTorture({
      sessions: [first, second],
      execGuest,
      mountPath,
      seed: posixModelSeed,
      oneClientSteps: posixModelOneSteps,
      twoClientSteps: posixModelTwoSteps,
    });
    return {
      pass: posixModelEvidence.status === "pass",
      detail: posixModelEvidence.modes
        .map(
          (mode) =>
            `${mode.name}: status=${mode.status} seed=${mode.seed} completed=${mode.completedActions}/${mode.totalActions} durationMs=${mode.durationMs}${
              mode.failure ? ` failure=${mode.failure.message}` : ""
            }`,
        )
        .join("\n"),
    };
  });

  await check(10, "sequential replacement VM exact HEAD, symlinks, worktree, and fsck", async () => {
    await withTimeout(first.discard(), "discard first VM before replacement", 120_000);
    const discardedIndex = sessions.indexOf(first);
    if (discardedIndex >= 0) sessions.splice(discardedIndex, 1);
    await withTimeout(second.discard(), "discard second VM before replacement", 120_000);
    const secondDiscardedIndex = sessions.indexOf(second);
    if (secondDiscardedIndex >= 0) sessions.splice(secondDiscardedIndex, 1);
    cleanup.replacementDiscarded = true;
    first = await createSession("replacement");
    const command = await execGuest(
      first,
      `actual="$(${git("rev-parse HEAD")})"
test "$actual" = "${expectedHead}"
python3 - <<'PY'
import hashlib, os, stat
root = "/workspace/worktree"
barrier = open(root + "/barrier.txt", "rb").read()
print(f"BARRIER_REPLACEMENT_SIZE:{len(barrier)}:SHA256:{hashlib.sha256(barrier).hexdigest()}")
assert barrier == b"barrier\\n"
assert stat.S_IMODE(os.stat("/workspace/ops/mode-file").st_mode) == 0o751
assert open("/workspace/ops/mode-file", "rb").read() == b"mode"
sparse = os.open("/workspace/ops/sparse", os.O_RDONLY)
assert os.fstat(sparse).st_size == 1024 * 1024
assert os.pread(sparse, 6, 777777) == b"OFFSET"
os.close(sparse)
assert open("/workspace/ops/replace-target", "rb").read() == b"new"
assert open("/workspace/coherent-renamed", "rb").read() == b"abcdXYZhi"
links = {
    root + "/node_modules/pkg": "../.pnpm/pkg@1.0.0/node_modules/pkg",
    root + "/node_modules/missing": "../.pnpm/missing@1.0.0/node_modules/missing",
    root + "/packages/app/node_modules/pkg": "../../../node_modules/pkg",
}
for path, target in links.items():
    metadata = os.lstat(path)
    assert stat.S_ISLNK(metadata.st_mode)
    assert os.readlink(path) == target
assert open(root + "/node_modules/pkg/index.js", "rb").read() == b"module.exports = 'mounted-symlink-target'\\n"
assert open(root + "/packages/app/node_modules/pkg/index.js", "rb").read() == b"module.exports = 'mounted-symlink-target'\\n"
assert not os.path.exists(root + "/node_modules/missing")
print("REPLACEMENT_SYMLINK_TREE_OK")
PY
${git("fsck --full")}
echo REPLACEMENT_OK:$actual`,
      900,
    );
    return {
      pass: command.code === 0 && command.stdout.includes(`REPLACEMENT_OK:${expectedHead}`),
      detail: commandResultText(command),
    };
  });

  await check(11, "Git status usability latency", async () => {
    if (gitWorkloadEvidence === null) {
      return {
        pass: false,
        detail: "correctness check 6 did not produce Git timing evidence",
      };
    }
    const coldMs = gitWorkloadEvidence.timingsMs.status_cold;
    const warmMs = gitWorkloadEvidence.timingsMs.status_warm;
    const coldPass = Number.isFinite(coldMs) && coldMs <= gitStatusColdMaxMs;
    const warmPass = Number.isFinite(warmMs) && warmMs <= gitStatusWarmMaxMs;
    return {
      pass: coldPass && warmPass,
      detail:
        `status_cold=${String(coldMs)}ms budget=${gitStatusColdMaxMs}ms ` +
        `status_warm=${String(warmMs)}ms budget=${gitStatusWarmMaxMs}ms`,
      evidence: {
        fileCount: gitWorkloadEvidence.fileCount,
        observedMs: { cold: coldMs, warm: warmMs },
        budgetMs: { cold: gitStatusColdMaxMs, warm: gitStatusWarmMaxMs },
      },
    };
  });
} finally {
  for (const [index, session] of [...sessions].reverse().entries()) {
    try {
      await withTimeout(
        session.discard(),
        `discard disposable VM during cleanup (${index + 1}/${sessions.length})`,
        120_000,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      cleanup.sessionDiscardErrors.push(message);
      cleanup.errors.push(message);
    }
  }
  try {
    await stopGateway();
    cleanup.gatewayStopped = true;
  } catch (error) {
    cleanup.errors.push(
      `gateway close: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  try {
    await rm(backingRoot, { recursive: true, force: true });
    cleanup.backingRootRemoved = true;
  } catch (error) {
    cleanup.errors.push(
      `backing root removal: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

if (cleanup.errors.length > 0) {
  results.push({
    id: "cleanup",
    name: "disposable resource cleanup",
    status: "fail",
    durationMs: 0,
    detail: cleanup.errors.join("\n"),
  });
}

const failed = results.filter((result) => result.status === "fail");
const skipped = results.filter((result) => result.status === "skip");
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      probeId,
      ownerId,
      gitDisabledOwnerId,
      scopePath,
      sandboxEndpoint,
      gatewayPublicUrl,
      ownerEndpoint,
      image: sandboxImage,
      architecture,
      gatewayProtocolEvidence,
      gatewayGitPolicyEvidence,
      gatewayRestartProtocolEvidence,
      posixModelEvidence,
      gatewayRequestCount,
      gatewayRequestCountsByRoute: Object.fromEntries(
        [...gatewayRequestCountsByRoute].sort(([left], [right]) =>
          left.localeCompare(right),
        ),
      ),
      summary: {
        passed: results.filter((result) => result.status === "pass").length,
        failed: failed.length,
        skipped: skipped.length,
      },
      cleanup: {
        replacementDiscarded: cleanup.replacementDiscarded,
        sessionsDiscarded: cleanup.sessionDiscardErrors.length === 0,
        sessionDiscardErrors: cleanup.sessionDiscardErrors,
        errors: cleanup.errors,
        backingRootRemoved: cleanup.backingRootRemoved ? backingRoot : false,
        gatewayStopped: cleanup.gatewayStopped,
      },
      results,
    },
    null,
    2,
  ),
);
if (failed.length > 0) process.exitCode = 1;
