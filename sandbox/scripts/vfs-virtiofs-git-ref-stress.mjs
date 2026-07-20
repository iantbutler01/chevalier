#!/usr/bin/env node

import { createHash, randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { mkdir, readFile, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(scriptDirectory, "../..");

if (process.argv.includes("--help")) {
  console.log(`Disposable cross-mounted Git/ref concurrency stress.

Required:
  SANDBOX_ENDPOINT
  SANDBOX_IMAGE
  CHEVALIER_VFS_GIT_REF_STRESS_GATEWAY_PUBLIC_URL
  CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN
    (SANDBOX_AUTH_TOKEN is accepted as the VFS-token fallback)

Optional:
  SANDBOX_AUTH_TOKEN
  SANDBOX_ARCHITECTURE=amd64
  CHEVALIER_VFS_GIT_REF_STRESS_GATEWAY_BIND=0.0.0.0
  CHEVALIER_VFS_GIT_REF_STRESS_GATEWAY_PORT=19097
  CHEVALIER_VFS_GIT_REF_STRESS_BACKEND_PROFILE=openbracket-vfs-fuse
  CHEVALIER_VFS_GIT_REF_STRESS_TIMEOUT_MS=1800000
  CHEVALIER_VFS_GIT_REF_STRESS_CLEANUP_TIMEOUT_MS=120000
  CHEVALIER_VFS_GIT_REF_STRESS_LOCK_ROUNDS=6
  CHEVALIER_VFS_GIT_REF_STRESS_WRITER_COMMITS=20
  CHEVALIER_VFS_GIT_REF_STRESS_FEED_COMMITS=20
  CHEVALIER_VFS_GIT_REF_STRESS_REF_COUNT=200
  CHEVALIER_VFS_GIT_REF_STRESS_READER_ROUNDS=240
  CHEVALIER_VFS_GIT_REF_STRESS_MAINTENANCE_ROUNDS=6
  CHEVALIER_VFS_GIT_REF_STRESS_TMPDIR=/tmp
  CHEVALIER_MODULE_PATH=<repo>/ts/index.js
  CHEVALIER_SANDBOX_MODULE_PATH=<repo>/ts-sandbox/index.js

The harness starts a private authenticated gateway over a randomized backing
root and two disposable VMs mounting the same randomized scope. It never
accepts a user owner, scope, or repository path. Every session and the backing
root are removed in finally.`);
  process.exit(0);
}

const required = (name) => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required; use --help for the complete contract`);
  return value;
};

const positiveInteger = (name, fallback, minimum = 1) => {
  const raw = process.env[name]?.trim() || String(fallback);
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed < minimum) {
    throw new Error(`${name} must be an integer >= ${minimum}`);
  }
  return parsed;
};

const sandboxEndpoint = required("SANDBOX_ENDPOINT");
const sandboxImage = required("SANDBOX_IMAGE");
const gatewayPublicUrl = required(
  "CHEVALIER_VFS_GIT_REF_STRESS_GATEWAY_PUBLIC_URL",
).replace(/\/+$/, "");
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
  process.env.CHEVALIER_VFS_GIT_REF_STRESS_BACKEND_PROFILE?.trim() ||
  "openbracket-vfs-fuse";
const gatewayBind =
  process.env.CHEVALIER_VFS_GIT_REF_STRESS_GATEWAY_BIND?.trim() || "0.0.0.0";
const gatewayPort = positiveInteger(
  "CHEVALIER_VFS_GIT_REF_STRESS_GATEWAY_PORT",
  19097,
);
if (gatewayPort > 65535) {
  throw new Error("CHEVALIER_VFS_GIT_REF_STRESS_GATEWAY_PORT must be <= 65535");
}
const timeoutMs = positiveInteger(
  "CHEVALIER_VFS_GIT_REF_STRESS_TIMEOUT_MS",
  1_800_000,
  10_000,
);
const cleanupTimeoutMs = positiveInteger(
  "CHEVALIER_VFS_GIT_REF_STRESS_CLEANUP_TIMEOUT_MS",
  120_000,
  10_000,
);
const lockRounds = positiveInteger("CHEVALIER_VFS_GIT_REF_STRESS_LOCK_ROUNDS", 6);
const writerCommits = positiveInteger(
  "CHEVALIER_VFS_GIT_REF_STRESS_WRITER_COMMITS",
  20,
);
const feedCommits = positiveInteger(
  "CHEVALIER_VFS_GIT_REF_STRESS_FEED_COMMITS",
  20,
);
const refCount = positiveInteger("CHEVALIER_VFS_GIT_REF_STRESS_REF_COUNT", 200);
const readerRounds = positiveInteger(
  "CHEVALIER_VFS_GIT_REF_STRESS_READER_ROUNDS",
  240,
);
const maintenanceRounds = positiveInteger(
  "CHEVALIER_VFS_GIT_REF_STRESS_MAINTENANCE_ROUNDS",
  6,
);

const require = createRequire(import.meta.url);
const chevalierPath =
  process.env.CHEVALIER_MODULE_PATH?.trim() || join(repositoryRoot, "ts", "index.js");
const sandboxModulePath =
  process.env.CHEVALIER_SANDBOX_MODULE_PATH?.trim() ||
  join(repositoryRoot, "ts-sandbox", "index.js");
const chevalier = require(resolve(chevalierPath));
const sandboxModule = require(resolve(sandboxModulePath));
const { createVfsGatewayServer, VfsStorage } = chevalier;
const { Sandbox } = sandboxModule;
if (
  typeof createVfsGatewayServer !== "function" ||
  typeof VfsStorage?.local !== "function" ||
  typeof Sandbox?.connect !== "function"
) {
  throw new Error("native modules do not expose the required VFS and sandbox APIs");
}

const probeId = `git-ref-stress-${Date.now()}-${randomUUID().slice(0, 8)}`;
const ownerId = `chevalier-${probeId}`;
const scopePath = `probes/${probeId}/repo`;
const mountPath = "/workspace";
const backingRoot = join(
  process.env.CHEVALIER_VFS_GIT_REF_STRESS_TMPDIR?.trim() || "/tmp",
  `chevalier-${probeId}`,
);
const ownerRoot = join(backingRoot, ownerId);
const ownerEndpoint = `${gatewayPublicUrl}/internal/chevalier/vfs/${encodeURIComponent(ownerId)}`;
const guestScript = await readFile(
  join(scriptDirectory, "vfs-git-ref-stress-guest.sh"),
);
const guestScriptHash = createHash("sha256").update(guestScript).digest("hex");
const guestScriptBase64 = guestScript.toString("base64");

let requestedSignal;
const interruptController = new AbortController();
const signalHandlers = new Map();
for (const signalName of ["SIGINT", "SIGTERM"]) {
  const handler = () => {
    if (requestedSignal) return;
    requestedSignal = signalName;
    interruptController.abort(new Error(`received ${signalName}`));
  };
  signalHandlers.set(signalName, handler);
  process.once(signalName, handler);
}

const withTimeout = async (
  promise,
  label,
  limitMs = timeoutMs,
  { interruptible = true } = {},
) => {
  let timer;
  let abortHandler;
  const contenders = [
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`${label} timed out after ${limitMs}ms`)),
        limitMs,
      );
    }),
  ];
  if (interruptible) {
    contenders.push(
      new Promise((_, reject) => {
        abortHandler = () =>
          reject(
            interruptController.signal.reason instanceof Error
              ? interruptController.signal.reason
              : new Error(`interrupted during ${label}`),
          );
        if (interruptController.signal.aborted) {
          abortHandler();
        } else {
          interruptController.signal.addEventListener("abort", abortHandler, {
            once: true,
          });
        }
      }),
    );
  }
  try {
    return await Promise.race(contenders);
  } finally {
    if (timer) clearTimeout(timer);
    if (abortHandler) {
      interruptController.signal.removeEventListener("abort", abortHandler);
    }
  }
};

const withCleanupTimeout = (promise, label, limitMs = cleanupTimeoutMs) =>
  withTimeout(promise, label, limitMs, { interruptible: false });

const readRequestBody = async (request) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
};

await mkdir(ownerRoot, { recursive: true });
const storage = VfsStorage.local(ownerRoot);
const gatewayHandler = createVfsGatewayServer({
  resolveStore: async (requestedOwner) => {
    if (requestedOwner !== ownerId) throw new Error(`unexpected owner: ${requestedOwner}`);
    return storage;
  },
  authToken: vfsAuthToken,
  allowGitMetadata: async (requestedOwner) => requestedOwner === ownerId,
});
let gatewayRequestCount = 0;
const gateway = createServer(async (incoming, outgoing) => {
  try {
    const method = incoming.method || "GET";
    const body =
      method === "GET" || method === "HEAD" ? undefined : await readRequestBody(incoming);
    const request = new Request(
      new URL(incoming.url || "/", `http://${incoming.headers.host || "localhost"}`),
      {
        method,
        headers: incoming.headers,
        body,
        ...(body === undefined ? {} : { duplex: "half" }),
      },
    );
    const response = await gatewayHandler(request);
    gatewayRequestCount += 1;
    outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
    outgoing.end(Buffer.from(await response.arrayBuffer()));
  } catch (error) {
    outgoing.writeHead(500, { "content-type": "text/plain" });
    outgoing.end(error instanceof Error ? error.stack || error.message : String(error));
  }
});

const stopGatewayBounded = async (label) => {
  if (!gateway.listening) return { stopped: true, forced: false };
  const closing = new Promise((resolveClose, rejectClose) => {
    gateway.close((error) => (error ? rejectClose(error) : resolveClose()));
  });
  gateway.closeIdleConnections?.();
  try {
    await withCleanupTimeout(
      closing,
      `${label} graceful close`,
      Math.min(cleanupTimeoutMs, 10_000),
    );
    return { stopped: true, forced: false };
  } catch (gracefulError) {
    gateway.closeAllConnections?.();
    try {
      await withCleanupTimeout(
        closing,
        `${label} forced close`,
        Math.min(cleanupTimeoutMs, 10_000),
      );
      return {
        stopped: true,
        forced: true,
        gracefulError:
          gracefulError instanceof Error ? gracefulError.message : String(gracefulError),
      };
    } catch (forcedError) {
      throw new Error(
        `${label} failed graceful close (${gracefulError instanceof Error ? gracefulError.message : String(gracefulError)}) and forced close (${forcedError instanceof Error ? forcedError.message : String(forcedError)})`,
      );
    }
  }
};

try {
  await withTimeout(
    new Promise((resolveListen, rejectListen) => {
      gateway.once("error", rejectListen);
      gateway.listen(gatewayPort, gatewayBind, resolveListen);
    }),
    "start disposable Git/ref gateway",
    10_000,
  );
} catch (error) {
  if (gateway.listening) {
    await stopGatewayBounded("failed gateway startup").catch(() => undefined);
  }
  await rm(backingRoot, { recursive: true, force: true }).catch(() => undefined);
  throw error;
}

const shellArgument = (value) => `'${String(value).replaceAll("'", `'\"'\"'`)}'`;
const resultText = (result) =>
  [`exit=${String(result.code)}`, result.stdout.trim(), result.stderr.trim()]
    .filter(Boolean)
    .join("\n");

const drainExec = async (handle, label) => {
  let code = null;
  let stdout = "";
  let stderr = "";
  for (;;) {
    const event = await withTimeout(handle.next(), `${label} output`);
    if (event === null) break;
    if (event.type === "stdout" && event.data) {
      stdout += Buffer.from(event.data).toString("utf8");
    }
    if (event.type === "stderr" && event.data) {
      stderr += Buffer.from(event.data).toString("utf8");
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
  return { code, stdout, stderr };
};

const startGuestCommand = async (session, command, timeoutSeconds = 1800) =>
  withTimeout(
    session.exec(`set -euo pipefail\n${command}`, {
      shell: "/bin/bash",
      closeStdinOnStart: true,
      timeoutSecs: timeoutSeconds,
      env: {
        GIT_AUTHOR_NAME: "Chevalier Git Ref Stress",
        GIT_AUTHOR_EMAIL: "git-ref-stress@chevalier.test",
        GIT_COMMITTER_NAME: "Chevalier Git Ref Stress",
        GIT_COMMITTER_EMAIL: "git-ref-stress@chevalier.test",
        GIT_TERMINAL_PROMPT: "0",
      },
    }),
    `start guest command: ${command.slice(0, 120)}`,
  );

const execGuestCommand = async (session, command, timeoutSeconds = 1800) =>
  drainExec(
    await startGuestCommand(session, command, timeoutSeconds),
    command.slice(0, 120),
  );

const startMode = (session, mode, args = [], timeoutSeconds = 1800) =>
  startGuestCommand(
    session,
    `/tmp/vfs-git-ref-stress-guest.sh ${shellArgument(mode)} ${args
      .map(shellArgument)
      .join(" ")}`,
    timeoutSeconds,
  );

const runMode = async (session, mode, args = [], timeoutSeconds = 1800) =>
  drainExec(
    await startMode(session, mode, args, timeoutSeconds),
    `${mode} ${args.join(" ")}`,
  );

const requireSuccess = (result, label) => {
  if (result.code !== 0) throw new Error(`${label} failed\n${resultText(result)}`);
  return result;
};

const results = [];
const record = async (name, body) => {
  const startedAt = Date.now();
  process.stderr.write(`[git-ref-stress] ${name}...\n`);
  try {
    const detail = await body();
    results.push({
      name,
      status: "pass",
      durationMs: Date.now() - startedAt,
      detail,
    });
    process.stderr.write(
      `[git-ref-stress] PASS ${name} (${Date.now() - startedAt} ms)\n`,
    );
    return detail;
  } catch (error) {
    const detail = error instanceof Error ? error.stack || error.message : String(error);
    results.push({
      name,
      status: "fail",
      durationMs: Date.now() - startedAt,
      detail,
    });
    process.stderr.write(
      `[git-ref-stress] FAIL ${name} (${Date.now() - startedAt} ms)\n`,
    );
    throw error;
  }
};

const mount = {
  guestPath: mountPath,
  mountTag: `grs-${randomUUID().replaceAll("-", "").slice(0, 22)}`,
  readOnly: false,
  availability: "shared-storage",
  continuity: "restore-cross-node",
  backendProfile,
  vfsEndpoint: ownerEndpoint,
  vfsScopePath: scopePath,
};

let sandbox;
const sessions = [];
const pendingCreations = new Map();
const requestedSessionNames = new Set();
const sessionRecords = [];
const sessionCreationTimeoutMs = 420_000;
const cleanup = {
  sessionDiscardErrors: [],
  sessionDiscardReceipts: [],
  lateSessionReceipts: [],
  providerSessionsFound: [],
  providerSessionsRemaining: [],
  pendingCreationNames: [],
  gatewayStopped: false,
  gatewayForceClosed: false,
  backingRootRemoved: false,
  errors: [],
};

const listSessionsBounded = (label) => {
  if (!sandbox) throw new Error(`${label}: sandbox provider is not connected`);
  return withCleanupTimeout(sandbox.listSessions(), label);
};

const discardSessionBounded = async (session, label) => {
  try {
    await withCleanupTimeout(session.discard(), `${label} through session handle`);
    cleanup.sessionDiscardReceipts.push({
      label,
      sessionId: session.sessionId,
      method: "handle",
    });
    return;
  } catch (handleError) {
    if (!sandbox || !session.sessionId) throw handleError;
    try {
      await withCleanupTimeout(
        sandbox.discardSessionById(session.sessionId),
        `${label} through provider session id`,
      );
      cleanup.sessionDiscardReceipts.push({
        label,
        sessionId: session.sessionId,
        method: "provider-id-fallback",
        handleError:
          handleError instanceof Error ? handleError.message : String(handleError),
      });
      return;
    } catch (fallbackError) {
      const remaining = await listSessionsBounded(
        `${label} verify after discard failures`,
      ).catch(() => []);
      if (!remaining.some((entry) => entry.sessionId === session.sessionId)) {
        cleanup.sessionDiscardReceipts.push({
          label,
          sessionId: session.sessionId,
          method: "verified-absent-after-errors",
          handleError:
            handleError instanceof Error ? handleError.message : String(handleError),
          fallbackError:
            fallbackError instanceof Error ? fallbackError.message : String(fallbackError),
        });
        return;
      }
      throw new Error(
        `${label} failed through handle (${handleError instanceof Error ? handleError.message : String(handleError)}) and provider id (${fallbackError instanceof Error ? fallbackError.message : String(fallbackError)})`,
      );
    }
  }
};

const createSession = async (suffix) => {
  const requestCountBefore = gatewayRequestCount;
  const sessionName = `cv-${probeId}-${suffix}`;
  const options = {
    image: sandboxImage,
    architecture,
    name: sessionName,
    metadata: {
      role: "chevalier-vfs-git-ref-stress",
      probeId,
      suffix,
    },
    autoStart: true,
    sharedMounts: [
      {
        ...mount,
        mountTag: `${mount.mountTag}-${suffix}`.slice(0, 31),
      },
    ],
  };
  requestedSessionNames.add(sessionName);
  let cleanupIfLate = false;
  const creation = sandbox.session(options);
  const settlement = creation.then(async (created) => {
    if (!cleanupIfLate) return created;
    const receipt = {
      suffix,
      name: sessionName,
      sessionId: created.sessionId,
      vmId: created.vmId,
      status: "cleanup-pending",
    };
    cleanup.lateSessionReceipts.push(receipt);
    try {
      await discardSessionBounded(
        created,
        `late-settled disposable Git/ref VM ${suffix}`,
      );
      receipt.status = "cleaned";
    } catch (error) {
      receipt.status = "retained";
      receipt.error = error instanceof Error ? error.message : String(error);
      cleanup.errors.push(`late session cleanup: ${receipt.error}`);
    }
    return created;
  });
  pendingCreations.set(sessionName, settlement);
  settlement
    .finally(() => {
      if (pendingCreations.get(sessionName) === settlement) {
        pendingCreations.delete(sessionName);
      }
    })
    .catch(() => undefined);
  let session;
  try {
    session = await withTimeout(
      creation,
      `create disposable Git/ref VM ${suffix}`,
      sessionCreationTimeoutMs,
    );
  } catch (error) {
    cleanupIfLate = true;
    throw error;
  }
  sessions.push(session);
  sessionRecords.push({
    suffix,
    name: sessionName,
    sessionId: session.sessionId,
    vmId: session.vmId,
  });
  const challengePath = `.git-ref-stress-ready-${suffix}`;
  const challenge = `ready-${suffix}-${probeId}`;
  for (let attempt = 0; attempt < 120; attempt += 1) {
    const ready = await execGuestCommand(
      session,
      `test "$(findmnt -n -o FSTYPE ${mountPath})" = virtiofs
printf '%s' ${shellArgument(challenge)} >${mountPath}/${challengePath}`,
      30,
    ).catch(() => undefined);
    if (ready?.code === 0 && gatewayRequestCount > requestCountBefore) {
      const bytes = await storage
        .read(`${scopePath}/${challengePath}`)
        .catch(() => undefined);
      if (bytes?.toString("utf8") === challenge) {
        requireSuccess(
          await execGuestCommand(session, `rm ${mountPath}/${challengePath}`, 30),
          `remove ${suffix} readiness challenge`,
        );
        return session;
      }
    }
    await delay(1_000);
  }
  throw new Error(`${suffix} VM did not expose the disposable virtiofs scope`);
};

const installGuestScript = async (session, suffix) =>
  resultText(
    requireSuccess(
      await execGuestCommand(
        session,
        `printf '%s' ${shellArgument(guestScriptBase64)} | base64 -d >/tmp/vfs-git-ref-stress-guest.sh
chmod 700 /tmp/vfs-git-ref-stress-guest.sh
test "$(sha256sum /tmp/vfs-git-ref-stress-guest.sh | cut -d' ' -f1)" = ${shellArgument(guestScriptHash)}`,
        60,
      ),
      `install exact guest script on ${suffix}`,
    ),
  );

let fatalError;
let runtime = {};
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
  const inheritedProbeSessions = (
    await withTimeout(
      sandbox.listSessions(),
      "audit fresh Git/ref probe provider namespace",
      cleanupTimeoutMs,
    )
  ).filter((entry) => entry.name.includes(probeId));
  if (inheritedProbeSessions.length !== 0) {
    throw new Error(
      `fresh probe id unexpectedly matched provider sessions: ${JSON.stringify(inheritedProbeSessions)}`,
    );
  }
  const first = await createSession("a");
  const second = await createSession("b");

  await record("two distinct provider sessions and VMs", async () => {
    const identities = sessionRecords.map((entry) => ({
      suffix: entry.suffix,
      name: entry.name,
      sessionId: entry.sessionId,
      vmId: entry.vmId,
    }));
    if (
      identities.length !== 2 ||
      identities.some((entry) => !entry.sessionId || !entry.vmId) ||
      new Set(identities.map((entry) => entry.sessionId)).size !== 2 ||
      new Set(identities.map((entry) => entry.vmId)).size !== 2
    ) {
      throw new Error(
        `two unique sessionId/vmId pairs are required: ${JSON.stringify(identities)}`,
      );
    }
    return JSON.stringify(identities);
  });

  await record("install exact concurrent Git workload", async () => {
    const installed = await Promise.all([
      installGuestScript(first, "a"),
      installGuestScript(second, "b"),
    ]);
    return `sha256=${guestScriptHash}\n${installed.join("\n")}`;
  });

  await record("two-client mounted topology", async () => {
    const [a, b] = await Promise.all([
      runMode(first, "runtime"),
      runMode(second, "runtime"),
    ]);
    requireSuccess(a, "runtime A");
    requireSuccess(b, "runtime B");
    runtime = { a: a.stdout.trim(), b: b.stdout.trim() };
    if (!a.stdout.includes("virtiofs") || !b.stdout.includes("virtiofs")) {
      throw new Error(`both guests must report virtiofs\n${resultText(a)}\n${resultText(b)}`);
    }
    return `A:\n${resultText(a)}\nB:\n${resultText(b)}`;
  });

  await record("initialize shared repository, remote, and linked worktrees", async () =>
    resultText(requireSuccess(await runMode(first, "setup"), "repository setup")),
  );

  await record("repeated cross-client index, packed-refs, and ref lock honesty", async () => {
    const evidence = [];
    for (const kind of ["index", "packed-refs", "ref"]) {
      for (let round = 1; round <= lockRounds; round += 1) {
        const holder = await startMode(first, "lock-holder", [kind, round], 180);
        try {
          evidence.push(
            resultText(
              requireSuccess(
                await runMode(second, "wait-lock", [kind, round], 180),
                `wait for ${kind} round ${round}`,
              ),
            ),
          );
          evidence.push(
            resultText(
              requireSuccess(
                await runMode(second, "contend-lock", [kind, round], 180),
                `${kind} contender round ${round}`,
              ),
            ),
          );
        } finally {
          await runMode(second, "release-lock", [kind, round], 30).catch(
            () => undefined,
          );
        }
        evidence.push(
          resultText(
            requireSuccess(
              await drainExec(holder, `${kind} holder round ${round}`),
              `${kind} holder round ${round}`,
            ),
          ),
        );
        evidence.push(
          resultText(
            requireSuccess(
              await runMode(second, "recover-lock", [kind, round], 180),
              `${kind} recovery round ${round}`,
            ),
          ),
        );
      }
    }
    return evidence.join("\n");
  });

  await record(
    "simultaneous commits, fetch/merge, readers, ref churn, gc, repack, graph, and MIDX",
    async () => {
      const maintenanceBarrier = "live-storm";
      const workloads = [
        ["writer-a", first, "writer", ["a", writerCommits, maintenanceBarrier]],
        ["writer-b", second, "writer", ["b", writerCommits, maintenanceBarrier]],
        ["publisher", first, "publisher", [feedCommits, maintenanceBarrier]],
        ["integrator", second, "integrator", [feedCommits * 4, maintenanceBarrier]],
        ["reader-a", first, "reader", [readerRounds, maintenanceBarrier, "reader-a"]],
        ["reader-b", second, "reader", [readerRounds, maintenanceBarrier, "reader-b"]],
        ["refs-a", first, "ref-churn", ["a", refCount, maintenanceBarrier]],
        ["refs-b", second, "ref-churn", ["b", refCount, maintenanceBarrier]],
        [
          "gc",
          first,
          "maintenance",
          ["gc", maintenanceRounds, maintenanceBarrier],
        ],
        [
          "repack",
          second,
          "maintenance",
          ["repack", maintenanceRounds, maintenanceBarrier],
        ],
        [
          "commit-graph",
          first,
          "maintenance",
          ["commit-graph", maintenanceRounds * 2, maintenanceBarrier],
        ],
        [
          "midx",
          second,
          "maintenance",
          ["midx", maintenanceRounds * 2, maintenanceBarrier],
        ],
      ];
      const handles = [];
      for (const [label, session, mode, args] of workloads) {
        handles.push({
          label,
          handle: await startMode(session, mode, args),
        });
      }
      requireSuccess(
        await runMode(first, "wait-workload-barrier", [maintenanceBarrier, 12], 180),
        "wait for twelve workload participants",
      );
      requireSuccess(
        await runMode(second, "release-workload-barrier", [maintenanceBarrier], 30),
        "release twelve workload participants",
      );
      const completed = await Promise.all(
        handles.map(async ({ label, handle }) => ({
          label,
          result: await drainExec(handle, label),
        })),
      );
      for (const { label, result } of completed) requireSuccess(result, label);
      for (const { label, result } of completed) {
        const releaseReceipt =
          `WORKLOAD_BARRIER_RELEASED barrier=${maintenanceBarrier} participant=${label}`;
        if (!result.stdout.split("\n").includes(releaseReceipt)) {
          throw new Error(
            `${label} omitted exact workload barrier receipt\n${resultText(result)}`,
          );
        }
      }
      const gcResult = completed.find(({ label }) => label === "gc")?.result;
      const repackResult = completed.find(({ label }) => label === "repack")?.result;
      if (!gcResult || !repackResult) {
        throw new Error("gc and repack receipts are both required");
      }
      const exactNativeRejection =
        /^MAINTENANCE_REJECTED kind=gc round=[0-9]+ exit=128 reason=native-gc-repack-pack-race stdout=/m;
      if (!exactNativeRejection.test(gcResult.stdout)) {
        throw new Error(
          `maintenance storm produced no exact native gc/repack rejection\n` +
            `gc:\n${resultText(gcResult)}\nrepack:\n${resultText(repackResult)}`,
        );
      }
      return completed
        .map(({ label, result }) => `${label}:\n${resultText(result)}`)
        .join("\n");
    },
  );

  let expectedHead = "";
  await record("barrier, merge convergence, maintenance recovery, and strict fsck", async () => {
    const finalized = requireSuccess(
      await runMode(first, "finalize", [writerCommits, feedCommits, refCount]),
      "finalize shared repository",
    );
    expectedHead = requireSuccess(
      await execGuestCommand(first, "git -C /workspace/git-ref-stress/repo rev-parse HEAD"),
      "read final HEAD",
    ).stdout.trim();
    if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(expectedHead)) {
      throw new Error(`final HEAD is invalid: ${JSON.stringify(expectedHead)}`);
    }
    return `${resultText(finalized)}\nhead=${expectedHead}`;
  });

  await record("exact cross-client refs, objects, HEAD, and fsck convergence", async () => {
    const [snapshotA, snapshotB] = await Promise.all([
      runMode(first, "snapshot", [expectedHead]),
      runMode(second, "snapshot", [expectedHead]),
    ]);
    requireSuccess(snapshotA, "snapshot A");
    requireSuccess(snapshotB, "snapshot B");
    const normalizedA = snapshotA.stdout.trim();
    const normalizedB = snapshotB.stdout.trim();
    if (!normalizedA.includes("SNAPSHOT_OK") || normalizedA !== normalizedB) {
      throw new Error(
        `cross-client snapshots diverged\nA:\n${resultText(snapshotA)}\nB:\n${resultText(snapshotB)}`,
      );
    }
    return `A:\n${resultText(snapshotA)}\nB:\n${resultText(snapshotB)}`;
  });
} catch (error) {
  fatalError = error instanceof Error ? error.stack || error.message : String(error);
} finally {
  if (pendingCreations.size !== 0) {
    cleanup.pendingCreationNames = [...pendingCreations.keys()];
    await withCleanupTimeout(
      Promise.allSettled([...pendingCreations.values()]),
      "wait for pending Git/ref session creations",
      sessionCreationTimeoutMs,
    ).catch((error) => {
      cleanup.errors.push(
        `pending session creation settlement: ${error instanceof Error ? error.message : String(error)}`,
      );
    });
    cleanup.pendingCreationNames = [...pendingCreations.keys()];
  }
  for (const session of [...sessions].reverse()) {
    try {
      await discardSessionBounded(
        session,
        `discard Git/ref session ${session.sessionId}`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      cleanup.sessionDiscardErrors.push({
        sessionId: session.sessionId,
        message,
      });
      cleanup.errors.push(`session discard ${session.sessionId}: ${message}`);
    }
  }
  if (sandbox) {
    const matchesProbe = (entry) =>
      requestedSessionNames.has(entry.name) || entry.name?.includes(probeId);
    let generated = [];
    try {
      generated = (await listSessionsBounded(
        "list generated Git/ref sessions for cleanup",
      )).filter(matchesProbe);
      cleanup.providerSessionsFound = generated.map((entry) => ({
        sessionId: entry.sessionId,
        vmId: entry.vmId,
        name: entry.name,
      }));
    } catch (error) {
      cleanup.errors.push(
        `provider cleanup listing: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    for (const entry of generated) {
      await withCleanupTimeout(
        sandbox.discardSessionById(entry.sessionId),
        `discard generated provider session ${entry.sessionId}`,
      ).catch((error) => {
        cleanup.errors.push(
          `provider cleanup ${entry.sessionId}: ${error instanceof Error ? error.message : String(error)}`,
        );
      });
    }
    const providerAuditDeadline = Date.now() + cleanupTimeoutMs;
    for (;;) {
      try {
        const remainingMs = providerAuditDeadline - Date.now();
        if (remainingMs <= 0) {
          throw new Error("provider absence audit reached its deadline");
        }
        const remaining = (
          await withCleanupTimeout(
            sandbox.listSessions(),
            "audit generated Git/ref provider-session absence",
            remainingMs,
          )
        ).filter(matchesProbe);
        cleanup.providerSessionsRemaining = remaining.map((entry) => ({
          sessionId: entry.sessionId,
          vmId: entry.vmId,
          name: entry.name,
        }));
        if (remaining.length === 0) break;
        await delay(Math.min(250, Math.max(1, providerAuditDeadline - Date.now())));
      } catch (error) {
        cleanup.errors.push(
          `provider absence audit: ${error instanceof Error ? error.message : String(error)}`,
        );
        break;
      }
    }
  }
  try {
    const stopped = await stopGatewayBounded("final Git/ref gateway cleanup");
    cleanup.gatewayStopped = stopped.stopped;
    cleanup.gatewayForceClosed = stopped.forced;
  } catch (error) {
    cleanup.errors.push(
      `gateway close: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  try {
    await withCleanupTimeout(
      rm(backingRoot, { recursive: true, force: true }),
      "remove Git/ref backing root",
    );
    cleanup.backingRootRemoved = true;
  } catch (error) {
    cleanup.errors.push(
      `backing root removal: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

if (requestedSignal && fatalError === undefined) {
  fatalError = `received ${requestedSignal}`;
}
const ok =
  fatalError === undefined &&
  requestedSignal === undefined &&
  cleanup.errors.length === 0 &&
  results.every((result) => result.status === "pass");
console.log(
  JSON.stringify(
    {
      ok,
      probeId,
      ownerId,
      scopePath,
      sandboxEndpoint,
      gatewayPublicUrl,
      ownerEndpoint,
      image: sandboxImage,
      architecture,
      backendProfile,
      guestScriptHash,
      requestedSignal,
      sessions: sessionRecords,
      parameters: {
        lockRounds,
        writerCommits,
        feedCommits,
        refCount,
        readerRounds,
        maintenanceRounds,
        cleanupTimeoutMs,
      },
      runtime,
      gatewayRequestCount,
      fatalError,
      summary: {
        passed: results.filter((result) => result.status === "pass").length,
        failed: results.filter((result) => result.status === "fail").length,
      },
      cleanup: {
        sessionsDiscarded:
          cleanup.sessionDiscardErrors.length === 0 &&
          cleanup.providerSessionsRemaining.length === 0 &&
          cleanup.pendingCreationNames.length === 0,
        sessionDiscardErrors: cleanup.sessionDiscardErrors,
        sessionDiscardReceipts: cleanup.sessionDiscardReceipts,
        lateSessionReceipts: cleanup.lateSessionReceipts,
        pendingCreationNames: cleanup.pendingCreationNames,
        providerSessionsFound: cleanup.providerSessionsFound,
        providerSessionsRemaining: cleanup.providerSessionsRemaining,
        gatewayStopped: cleanup.gatewayStopped,
        gatewayForceClosed: cleanup.gatewayForceClosed,
        backingRootRemoved: cleanup.backingRootRemoved ? backingRoot : false,
        errors: cleanup.errors,
      },
      results,
    },
    null,
    2,
  ),
);
for (const [signalName, handler] of signalHandlers) {
  process.removeListener(signalName, handler);
}
if (!ok) process.exitCode = 1;
