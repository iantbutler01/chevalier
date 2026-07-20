#!/usr/bin/env node

import { createHash, randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { mkdir, readFile, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");

if (process.argv.includes("--help")) {
  console.log(`Disposable mounted VFS deep-Git matrix.

Required:
  SANDBOX_ENDPOINT
  SANDBOX_IMAGE
  CHEVALIER_VFS_GIT_MATRIX_GATEWAY_PUBLIC_URL
  CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN
    (SANDBOX_AUTH_TOKEN is accepted as the VFS-token fallback)

Optional:
  SANDBOX_AUTH_TOKEN
  SANDBOX_ARCHITECTURE=amd64
  CHEVALIER_VFS_GIT_MATRIX_GATEWAY_BIND=0.0.0.0
  CHEVALIER_VFS_GIT_MATRIX_GATEWAY_PORT=19092
  CHEVALIER_VFS_GIT_MATRIX_BACKEND_PROFILE=openbracket-vfs-fuse
  CHEVALIER_VFS_GIT_MATRIX_COMMAND_TIMEOUT_MS=1200000
  CHEVALIER_VFS_GIT_MATRIX_TMPDIR=/tmp
  CHEVALIER_MODULE_PATH=<repo>/ts/index.js
  CHEVALIER_SANDBOX_MODULE_PATH=<repo>/ts-sandbox/index.js

The harness creates one fresh local gateway scope and two disposable VMs. It
never points at a user owner or repository. It removes both VMs and the backing
root on every exit path.`);
  process.exit(0);
}

const required = (name) => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required; use --help for the complete contract`);
  return value;
};

const sandboxEndpoint = required("SANDBOX_ENDPOINT");
const sandboxImage = required("SANDBOX_IMAGE");
const gatewayPublicUrl = required("CHEVALIER_VFS_GIT_MATRIX_GATEWAY_PUBLIC_URL").replace(
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
const backendProfile =
  process.env.CHEVALIER_VFS_GIT_MATRIX_BACKEND_PROFILE?.trim() ||
  "openbracket-vfs-fuse";
const gatewayBind =
  process.env.CHEVALIER_VFS_GIT_MATRIX_GATEWAY_BIND?.trim() || "0.0.0.0";
const gatewayPort = Number(process.env.CHEVALIER_VFS_GIT_MATRIX_GATEWAY_PORT ?? "19092");
const commandTimeoutMs = Number(
  process.env.CHEVALIER_VFS_GIT_MATRIX_COMMAND_TIMEOUT_MS ?? "1200000",
);
if (!Number.isInteger(gatewayPort) || gatewayPort < 1 || gatewayPort > 65535) {
  throw new Error("CHEVALIER_VFS_GIT_MATRIX_GATEWAY_PORT must be an integer in 1..65535");
}
if (!Number.isFinite(commandTimeoutMs) || commandTimeoutMs < 10_000) {
  throw new Error("CHEVALIER_VFS_GIT_MATRIX_COMMAND_TIMEOUT_MS must be at least 10000");
}

const require = createRequire(import.meta.url);
const chevalierPath =
  process.env.CHEVALIER_MODULE_PATH?.trim() || join(repoRoot, "ts", "index.js");
const sandboxModulePath =
  process.env.CHEVALIER_SANDBOX_MODULE_PATH?.trim() ||
  join(repoRoot, "ts-sandbox", "index.js");
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

const probeId = `git-matrix-${Date.now()}-${randomUUID().slice(0, 8)}`;
const ownerId = `chevalier-${probeId}`;
const scopePath = `probes/${probeId}/repo`;
const mountPath = "/workspace";
const backingRoot = join(
  process.env.CHEVALIER_VFS_GIT_MATRIX_TMPDIR?.trim() || "/tmp",
  `chevalier-${probeId}`,
);
const ownerRoot = join(backingRoot, ownerId);
const ownerEndpoint = `${gatewayPublicUrl}/internal/chevalier/vfs/${encodeURIComponent(ownerId)}`;
const guestScript = await readFile(join(scriptDir, "vfs-git-matrix-guest.sh"));
const guestScriptSha256 = createHash("sha256").update(guestScript).digest("hex");
const guestScriptBase64 = guestScript.toString("base64");

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

const readRequestBody = async (request) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
};

await mkdir(ownerRoot, { recursive: true });
const storage = VfsStorage.local(ownerRoot);
const handleGatewayRequest = createVfsGatewayServer({
  resolveStore: async (requestedOwner) => {
    if (requestedOwner !== ownerId) throw new Error(`unexpected owner: ${requestedOwner}`);
    return storage;
  },
  authToken: vfsAuthToken,
  allowGitMetadata: async (requestedOwner) => requestedOwner === ownerId,
});
let gatewayRequestCount = 0;
const gatewayServer = createServer(async (incoming, outgoing) => {
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
    const response = await handleGatewayRequest(request);
    gatewayRequestCount += 1;
    outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
    outgoing.end(Buffer.from(await response.arrayBuffer()));
  } catch (error) {
    outgoing.writeHead(500, { "content-type": "text/plain" });
    outgoing.end(error instanceof Error ? error.stack || error.message : String(error));
  }
});

try {
  await withTimeout(
    new Promise((resolveListen, rejectListen) => {
      gatewayServer.once("error", rejectListen);
      gatewayServer.listen(gatewayPort, gatewayBind, resolveListen);
    }),
    "start disposable gateway",
    10_000,
  );
} catch (error) {
  if (gatewayServer.listening) {
    await new Promise((resolveClose) => gatewayServer.close(resolveClose));
  }
  await rm(backingRoot, { recursive: true, force: true }).catch(() => undefined);
  throw error;
}

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

const startGuestCommand = async (session, command, timeoutSecs = 1200) =>
  withTimeout(
    session.exec(`set -euo pipefail\n${command}`, {
      shell: "/bin/bash",
      closeStdinOnStart: true,
      timeoutSecs,
      env: {
        GIT_AUTHOR_NAME: "Chevalier Git Matrix",
        GIT_AUTHOR_EMAIL: "git-matrix@chevalier.test",
        GIT_COMMITTER_NAME: "Chevalier Git Matrix",
        GIT_COMMITTER_EMAIL: "git-matrix@chevalier.test",
        GIT_TERMINAL_PROMPT: "0",
      },
    }),
    `start guest command: ${command.slice(0, 120)}`,
  );

const execGuestCommand = async (session, command, timeoutSecs = 1200) =>
  drainExec(
    await startGuestCommand(session, command, timeoutSecs),
    command.slice(0, 120),
  );

const shellArg = (value) => `'${String(value).replaceAll("'", `'\"'\"'`)}'`;
const runMode = (session, mode, args = [], timeoutSecs = 1200) =>
  execGuestCommand(
    session,
    `/tmp/vfs-git-matrix-guest.sh ${shellArg(mode)} ${args.map(shellArg).join(" ")}`,
    timeoutSecs,
  );

const resultText = (result) =>
  [`exit=${String(result.code)}`, result.stdout.trim(), result.stderr.trim()]
    .filter(Boolean)
    .join("\n");

const results = [];
const record = async (name, body) => {
  const started = Date.now();
  process.stderr.write(`[git-matrix] ${name}...\n`);
  try {
    const detail = await body();
    results.push({
      name,
      status: "pass",
      durationMs: Date.now() - started,
      detail,
    });
    process.stderr.write(`[git-matrix] PASS ${name} (${Date.now() - started} ms)\n`);
    return detail;
  } catch (error) {
    const detail = error instanceof Error ? error.stack || error.message : String(error);
    results.push({
      name,
      status: "fail",
      durationMs: Date.now() - started,
      detail,
    });
    process.stderr.write(`[git-matrix] FAIL ${name} (${Date.now() - started} ms)\n`);
    throw error;
  }
};

const requireSuccess = (result, label) => {
  if (result.code !== 0) throw new Error(`${label} failed\n${resultText(result)}`);
  return result;
};

const mount = {
  guestPath: mountPath,
  mountTag: `gm-${randomUUID().replaceAll("-", "").slice(0, 24)}`,
  readOnly: false,
  availability: "shared-storage",
  continuity: "restore-cross-node",
  backendProfile,
  vfsEndpoint: ownerEndpoint,
  vfsScopePath: scopePath,
};

let sandbox;
const sessions = [];
const cleanup = {
  sessionDiscardErrors: [],
  gatewayStopped: false,
  backingRootRemoved: false,
  errors: [],
};

const createSession = async (suffix) => {
  const callbackRequestsBefore = gatewayRequestCount;
  const session = await withTimeout(
    sandbox.session({
      image: sandboxImage,
      architecture,
      name: `cv-git-matrix-${suffix}-${Date.now()}`,
      metadata: { role: "chevalier-vfs-git-matrix", probeId, suffix },
      autoStart: true,
      sharedMounts: [
        { ...mount, mountTag: `${mount.mountTag}-${suffix}`.slice(0, 31) },
      ],
    }),
    `create ${suffix} VM`,
    420_000,
  );
  sessions.push(session);
  const readinessPath = `.git-matrix-ready-${suffix}`;
  const readinessBytes = `ready-${suffix}-${probeId}`;
  for (let attempt = 0; attempt < 90; attempt += 1) {
    const ready = await execGuestCommand(
      session,
      `test "$(findmnt -n -o FSTYPE ${mountPath})" = virtiofs
printf '%s' ${shellArg(readinessBytes)} >${mountPath}/${readinessPath}`,
      30,
    ).catch(() => undefined);
    if (ready?.code === 0 && gatewayRequestCount > callbackRequestsBefore) {
      const bytes = await storage
        .read(`${scopePath}/${readinessPath}`)
        .catch(() => undefined);
      if (bytes?.toString("utf8") === readinessBytes) {
        requireSuccess(
          await execGuestCommand(session, `rm ${mountPath}/${readinessPath}`, 30),
          `remove ${suffix} readiness challenge`,
        );
        return session;
      }
    }
    await delay(1_000);
  }
  throw new Error(`${suffix} VM did not expose the disposable virtiofs scope`);
};

const installGuestScript = async (session, suffix) => {
  const result = requireSuccess(
    await execGuestCommand(
      session,
      `printf '%s' ${shellArg(guestScriptBase64)} | base64 -d >/tmp/vfs-git-matrix-guest.sh
chmod 700 /tmp/vfs-git-matrix-guest.sh
test "$(sha256sum /tmp/vfs-git-matrix-guest.sh | cut -d' ' -f1)" = ${shellArg(guestScriptSha256)}`,
      60,
    ),
    `install guest matrix script on ${suffix}`,
  );
  return resultText(result);
};

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
  const first = await createSession("a");
  const second = await createSession("b");
  await record("install exact guest workload", async () => {
    const installed = await Promise.all([
      installGuestScript(first, "a"),
      installGuestScript(second, "b"),
    ]);
    return `sha256=${guestScriptSha256}\n${installed.join("\n")}`;
  });

  await record("runtime and mounted topology", async () => {
    const [a, b] = await Promise.all([runMode(first, "runtime"), runMode(second, "runtime")]);
    requireSuccess(a, "runtime A");
    requireSuccess(b, "runtime B");
    runtime = { a: a.stdout.trim(), b: b.stdout.trim() };
    if (!a.stdout.includes("virtiofs") || !b.stdout.includes("virtiofs")) {
      throw new Error(`both guests must report virtiofs\n${resultText(a)}\n${resultText(b)}`);
    }
    return `A:\n${resultText(a)}\nB:\n${resultText(b)}`;
  });

  let expectedHead = "";
  await record("init clone status add commit switch merge rebase cherry-pick stash reset", async () => {
    const lifecycle = requireSuccess(await runMode(first, "lifecycle"), "Git lifecycle");
    const head = requireSuccess(
      await execGuestCommand(first, "git -C /workspace/git-matrix/repo rev-parse HEAD"),
      "read lifecycle HEAD",
    );
    expectedHead = head.stdout.trim();
    if (!lifecycle.stdout.includes("LIFECYCLE_OK") || !/^[0-9a-f]{40,64}$/.test(expectedHead)) {
      throw new Error(`lifecycle did not emit a valid terminal HEAD\n${resultText(lifecycle)}`);
    }
    return `${resultText(lifecycle)}\nhead=${expectedHead}`;
  });

  await record("cross-mounted lifecycle visibility", async () =>
    resultText(
      requireSuccess(
        await runMode(second, "cross-read", [expectedHead]),
        "cross-mounted lifecycle read",
      ),
    ),
  );

  await record("transactional refs and packed-refs", async () => {
    const refs = requireSuccess(await runMode(first, "refs"), "refs matrix");
    const cross = requireSuccess(
      await execGuestCommand(
        second,
        `test "$(git -C /workspace/git-matrix/repo rev-parse refs/heads/matrix-63)" = ${shellArg(expectedHead)}
test "$(git -C /workspace/git-matrix/repo rev-parse refs/tags/matrix-63)" = ${shellArg(expectedHead)}
grep -q refs/heads/matrix-63 /workspace/git-matrix/repo/.git/packed-refs`,
      ),
      "cross-mounted packed refs",
    );
    return `${resultText(refs)}\n${resultText(cross)}`;
  });

  await record("linked worktree lifecycle", async () => {
    const worktree = requireSuccess(await runMode(first, "worktree"), "worktree matrix");
    const head = requireSuccess(
      await execGuestCommand(first, "git -C /workspace/git-matrix/repo rev-parse HEAD"),
      "read worktree HEAD",
    );
    expectedHead = head.stdout.trim();
    const cross = requireSuccess(
      await runMode(second, "cross-read", [expectedHead]),
      "cross-mounted worktree result",
    );
    return `${resultText(worktree)}\n${resultText(cross)}`;
  });

  await record("cross-mounted Git index.lock exclusion and recovery", async () => {
    const before = expectedHead;
    const prepared = requireSuccess(
      await runMode(first, "index-lock-prepare"),
      "index lock preparation",
    );
    const rejected = requireSuccess(
      await runMode(second, "index-lock-contend", [before]),
      "cross-mounted index lock contender",
    );
    const recovered = requireSuccess(
      await runMode(first, "index-lock-release", [before]),
      "index lock recovery",
    );
    expectedHead = requireSuccess(
      await execGuestCommand(first, "git -C /workspace/git-matrix/repo rev-parse HEAD"),
      "post-index-lock HEAD",
    ).stdout.trim();
    requireSuccess(
      await runMode(second, "cross-read", [expectedHead]),
      "cross-mounted index lock result",
    );
    return `${resultText(prepared)}\n${resultText(rejected)}\n${resultText(recovered)}`;
  });

  await record("concurrent cross-mounted readers and writer", async () => {
    requireSuccess(await runMode(first, "seed-concurrency"), "concurrency seed");
    const writerHandle = await startGuestCommand(
      first,
      "/tmp/vfs-git-matrix-guest.sh writer 24",
    );
    const readerHandle = await startGuestCommand(
      second,
      "/tmp/vfs-git-matrix-guest.sh reader 120",
    );
    const [writer, reader] = await Promise.all([
      drainExec(writerHandle, "concurrent writer"),
      drainExec(readerHandle, "concurrent reader"),
    ]);
    requireSuccess(writer, "concurrent writer");
    requireSuccess(reader, "concurrent reader");
    expectedHead = requireSuccess(
      await execGuestCommand(first, "git -C /workspace/git-matrix/repo rev-parse HEAD"),
      "read concurrent HEAD",
    ).stdout.trim();
    requireSuccess(
      await runMode(second, "cross-read", [expectedHead]),
      "cross-mounted concurrent result",
    );
    return `${resultText(writer)}\n${resultText(reader)}\nhead=${expectedHead}`;
  });

  await record("concurrent ref writers and compare-and-swap exclusion", async () => {
    const [refsA, refsB] = await Promise.all([
      runMode(first, "refs-writer", ["a", "40"]),
      runMode(second, "refs-writer", ["b", "40"]),
    ]);
    requireSuccess(refsA, "ref writer A");
    requireSuccess(refsB, "ref writer B");
    const count = requireSuccess(
      await execGuestCommand(
        first,
        `test "$(git -C /workspace/git-matrix/repo for-each-ref --format='%(refname)' 'refs/heads/concurrent-a-*' | wc -l)" -eq 40
test "$(git -C /workspace/git-matrix/repo for-each-ref --format='%(refname)' 'refs/heads/concurrent-b-*' | wc -l)" -eq 40`,
      ),
      "concurrent ref counts",
    );

    const base = expectedHead;
    requireSuccess(
      await execGuestCommand(
        first,
        `git -C /workspace/git-matrix/repo update-ref refs/heads/concurrent-cas ${shellArg(base)}`,
      ),
      "initialize CAS ref",
    );
    const candidateA = requireSuccess(
      await runMode(first, "cas-prepare", ["a"]),
      "prepare CAS A",
    ).stdout.trim();
    const candidateB = requireSuccess(
      await runMode(second, "cas-prepare", ["b"]),
      "prepare CAS B",
    ).stdout.trim();
    const [casA, casB] = await Promise.all([
      runMode(first, "cas-contend", [candidateA, base, "1"]),
      runMode(second, "cas-contend", [candidateB, base, "1"]),
    ]);
    requireSuccess(casA, "CAS A");
    requireSuccess(casB, "CAS B");
    const combined = `${casA.stdout}\n${casB.stdout}`;
    if ((combined.match(/CAS_WON/g) || []).length !== 1 || (combined.match(/CAS_LOST/g) || []).length !== 1) {
      throw new Error(`CAS requires exactly one winner and loser\n${combined}`);
    }
    return [
      resultText(refsA),
      resultText(refsB),
      resultText(count),
      resultText(casA),
      resultText(casB),
    ].join("\n");
  });

  await record("killed commit and conflict abort recovery", async () => {
    const prepared = requireSuccess(
      await runMode(first, "interrupt-prepare"),
      "interrupt preparation",
    );
    const before = requireSuccess(
      await execGuestCommand(first, "git -C /workspace/git-matrix/repo rev-parse HEAD"),
      "pre-interrupt HEAD",
    ).stdout.trim();
    const interrupted = await startGuestCommand(
      first,
      "exec setsid /tmp/vfs-git-matrix-guest.sh interrupt-commit",
      180,
    );
    let hookReady = false;
    for (let attempt = 0; attempt < 60; attempt += 1) {
      const marker = await storage
        .read(`${scopePath}/git-matrix/hook-ready`)
        .catch(() => undefined);
      if (marker?.toString("utf8") === "ready") {
        hookReady = true;
        break;
      }
      await delay(250);
    }
    if (!hookReady) {
      await execGuestCommand(
        first,
        `pid=$(cat /workspace/git-matrix/interrupt-pid 2>/dev/null || true)
if test -n "$pid"; then kill -KILL -- "-$pid" 2>/dev/null || true; fi`,
        30,
      ).catch(() => undefined);
      throw new Error("pre-commit hook did not publish its interruption barrier");
    }
    requireSuccess(
      await execGuestCommand(
        first,
        `pid=$(cat /workspace/git-matrix/interrupt-pid)
test "$(ps -o pgid= -p "$pid" | tr -d ' ')" = "$pid"
kill -KILL -- "-$pid"`,
        30,
      ),
      "kill isolated Git process group",
    );
    const killed = await drainExec(interrupted, "interrupted commit");
    if (killed.code === 0) throw new Error(`commit unexpectedly completed\n${resultText(killed)}`);
    const recovered = requireSuccess(
      await runMode(first, "interrupt-recover", [before]),
      "interrupted operation recovery",
    );
    expectedHead = requireSuccess(
      await execGuestCommand(first, "git -C /workspace/git-matrix/repo rev-parse HEAD"),
      "post-recovery HEAD",
    ).stdout.trim();
    requireSuccess(
      await runMode(second, "cross-read", [expectedHead]),
      "cross-mounted recovery result",
    );
    return `${resultText(prepared)}\nkill=${resultText(killed)}\n${resultText(recovered)}`;
  });

  await record("repack gc commit-graph fsck and final cross-mount state", async () => {
    const maintenance = requireSuccess(
      await runMode(first, "maintenance"),
      "Git maintenance",
    );
    expectedHead = requireSuccess(
      await execGuestCommand(first, "git -C /workspace/git-matrix/repo rev-parse HEAD"),
      "maintenance HEAD",
    ).stdout.trim();
    const [finalA, finalB] = await Promise.all([
      runMode(first, "final", [expectedHead]),
      runMode(second, "final", [expectedHead]),
    ]);
    requireSuccess(finalA, "final A");
    requireSuccess(finalB, "final B");
    return `${resultText(maintenance)}\nA:\n${resultText(finalA)}\nB:\n${resultText(finalB)}`;
  });
} catch (error) {
  fatalError = error instanceof Error ? error.stack || error.message : String(error);
} finally {
  for (const session of [...sessions].reverse()) {
    try {
      await session.discard();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      cleanup.sessionDiscardErrors.push(message);
      cleanup.errors.push(`session discard: ${message}`);
    }
  }
  try {
    await new Promise((resolveClose, rejectClose) =>
      gatewayServer.close((error) => (error ? rejectClose(error) : resolveClose())),
    );
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

const ok =
  fatalError === undefined &&
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
      guestScriptSha256,
      runtime,
      gatewayRequestCount,
      fatalError,
      summary: {
        passed: results.filter((result) => result.status === "pass").length,
        failed: results.filter((result) => result.status === "fail").length,
      },
      cleanup: {
        sessionsDiscarded: cleanup.sessionDiscardErrors.length === 0,
        sessionDiscardErrors: cleanup.sessionDiscardErrors,
        gatewayStopped: cleanup.gatewayStopped,
        backingRootRemoved: cleanup.backingRootRemoved ? backingRoot : false,
        errors: cleanup.errors,
      },
      results,
    },
    null,
    2,
  ),
);
if (!ok) process.exitCode = 1;
