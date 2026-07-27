#!/usr/bin/env node
/**
 * Perf budgets measured on a REAL mounted VFS, inside a live sandbox VM.
 *
 * Why this exists: every other VFS test runs in-process against fake stores on a
 * quiet namespace. Not one of them can observe a 64ms file create or a 30s
 * unlink, because neither is a correctness fault — they are latency faults, and
 * latency only exists on the real mount. The suite stayed green while a package
 * install in the guest was unusable.
 *
 * The budgets below are derived from what the product has to do, not from what
 * the code currently achieves. A `node_modules` tree is 10k+ small files; at
 * 64ms per create that is 11 minutes, so per-op cost is the thing that decides
 * whether the mount is usable at all. Network is NOT the explanation: measured
 * RTT bismuth->corvidae is 0.87ms.
 *
 * These are ceilings that must come DOWN over time, never up. Raising one to
 * make a run pass defeats the entire point of the file.
 *
 * USAGE:
 *   SANDBOX_ENDPOINT=... SANDBOX_AUTH_TOKEN=... [SESSION_ID=...] \
 *     node sandbox/scripts/vfs-mounted-perf-budget.mjs
 */

import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");
const require = createRequire(import.meta.url);

const sandboxModulePath =
  process.env.CHEVALIER_SANDBOX_MODULE_PATH?.trim() || join(repoRoot, "ts-sandbox", "index.js");
const { Sandbox } = require(resolve(sandboxModulePath));

const endpoint = process.env.SANDBOX_ENDPOINT?.trim();
const authToken = process.env.SANDBOX_AUTH_TOKEN?.trim();
if (!endpoint || !authToken) {
  console.error("SANDBOX_ENDPOINT and SANDBOX_AUTH_TOKEN are required");
  process.exit(2);
}
const mountPath = process.env.VFS_PERF_MOUNT?.trim() || "/workspace";
const sampleCount = Number(process.env.VFS_PERF_SAMPLES ?? "25");

/**
 * Per-operation ceilings, in milliseconds.
 *
 * A mounted filesystem that costs tens of milliseconds per metadata mutation
 * cannot host an ordinary build. `stat` is included as the control: it is
 * already fast, so a regression there proves the harness itself is sound rather
 * than the mount merely being slow everywhere.
 */
/**
 * RATCHET POLICY: these only ever move DOWN.
 *
 * A budget far above current behaviour protects nothing. `hardlink-unlink` sat
 * at 2000ms while measuring 26ms — it could have regressed by 10x and still
 * passed, silently giving back the alias fix that took a day to find. Each value
 * is therefore pinned just above the measured result in an idle arena, with the
 * measurement recorded so a later reader can tell a real improvement from a
 * quiet loosening.
 *
 * Isolated-arena baseline, 2026-07-26 (cache=auto + parallel write path):
 *   stat 1.9 | mkdir ~22 | create 21.8 | rewrite 48.0 | unlink 24.5
 */
const BUDGETS_MS = {
  // 1.9ms measured. Held at 3 rather than 2: sub-millisecond timings carry real
  // jitter and a gate that flaps teaches people to ignore it.
  stat: 3,
  mkdir: 25,
  create: 25,
  // Still over: a rewrite costs ~2x a create, which looks like a second round
  // trip (read-modify-write or a CAS precondition fetch). Left as a target, not
  // relaxed to match reality.
  rewrite: 25,
  unlink: 25,
};

/** A package install is thousands of files; this is the shape that must work. */
const BULK_CREATE_COUNT = 200;
const BULK_CREATE_BUDGET_MS = 5_000; // measured 9160ms (45.8ms/file) — still a target

/** Hard links are how pnpm builds node_modules, and the unlink path resolves
 *  aliases for them. This is the exact shape that stalled 30s per file.
 *
 *  Ratcheted 2000 -> 100 -> 30 as the alias fix took it to 26ms. At 2000 a
 *  full 10x regression would still have passed; even 100 left ~4x of slack.
 *  30 is ~15% headroom over measured, which is enough for run-to-run noise and
 *  nothing like enough to hide a real regression. */
const HARDLINK_UNLINK_BUDGET_MS = 30;

const connect = async () => {
  const sandbox = await Sandbox.connect(endpoint, { authToken });
  const explicit = process.env.SESSION_ID?.trim();
  if (explicit) return { sandbox, session: await sandbox.attachSessionPassive(explicit) };

  const sessions = await sandbox.listSessions();
  // 3 === running (see vm_state_label in ts-sandbox); a stopped session cannot
  // be measured, and silently measuring the wrong VM is worse than failing.
  const running = sessions.filter((info) => info.state === 3);
  if (running.length === 0) {
    console.error(`no running sandbox session to measure (${sessions.length} known, none running)`);
    process.exit(2);
  }
  return { sandbox, session: await sandbox.attachSessionPassive(running[0].sessionId) };
};

const exec = async (session, command, timeoutSecs = 300) => {
  const handle = await session.exec(command, {
    shell: "/bin/bash",
    closeStdinOnStart: true,
    timeoutSecs,
  });
  let stdout = "";
  let code = null;
  for (;;) {
    const event = await handle.next();
    if (event === null) break;
    if (event.data && (event.type === "stdout" || event.type === "stderr")) {
      stdout += Buffer.from(event.data).toString("utf8");
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
  return { code, stdout };
};

const { sandbox, session } = await connect();
const results = [];
const record = (name, perOpMs, budgetMs, detail) => {
  const pass = perOpMs <= budgetMs;
  results.push({ name, perOpMs, budgetMs, pass, detail });
  const status = pass ? "[1;32mPASS[0m" : "[1;31mFAIL[0m";
  console.log(
    `  ${status}  ${name.padEnd(22)} ${perOpMs.toFixed(1).padStart(8)}ms  budget ${budgetMs}ms${detail ? `  (${detail})` : ""}`,
  );
};

console.log(`measuring mounted VFS at ${mountPath} (${sampleCount} samples/op)\n`);

// Per-op class timings. Each loop is timed as a whole and divided, so a single
// slow outlier cannot hide behind a fast median — bulk cost is what matters.
const opBench = await exec(
  session,
  `set -u
b=${mountPath}/.perfbudget-$$
rm -rf "$b"; mkdir -p "$b" || exit 3
cd "$b"
t() { s=$(date +%s%N); eval "$2"; e=$(date +%s%N); printf '%s %s\\n' "$1" $(( (e-s)/1000000 )); }
t mkdir   'for i in $(seq 1 ${sampleCount}); do mkdir -p d$i; done'
t create  'for i in $(seq 1 ${sampleCount}); do : > f$i; done'
t stat    'for i in $(seq 1 ${sampleCount}); do stat f$i >/dev/null; done'
t rewrite 'for i in $(seq 1 ${sampleCount}); do echo y > f$i; done'
t unlink  'for i in $(seq 1 ${sampleCount}); do rm -f f$i; done'
cd /; rm -rf "$b"`,
  600,
);
if (opBench.code !== 0) {
  console.error(`op benchmark failed (rc=${opBench.code}):\n${opBench.stdout}`);
  process.exit(2);
}
for (const line of opBench.stdout.trim().split("\n")) {
  const [name, totalMs] = line.trim().split(/\s+/);
  if (!(name in BUDGETS_MS)) continue;
  record(name, Number(totalMs) / sampleCount, BUDGETS_MS[name], `${totalMs}ms total`);
}

// The shape that actually broke: bulk small-file creation, as any installer does.
const bulk = await exec(
  session,
  `set -u
b=${mountPath}/.perfbulk-$$
rm -rf "$b"; mkdir -p "$b" || exit 3
s=$(date +%s%N)
for i in $(seq 1 ${BULK_CREATE_COUNT}); do echo x > "$b/f$i"; done
e=$(date +%s%N)
printf 'bulk %s\\n' $(( (e-s)/1000000 ))
rm -rf "$b"`,
  900,
);
if (bulk.code === 0) {
  const totalMs = Number(bulk.stdout.trim().split(/\s+/)[1]);
  const pass = totalMs <= BULK_CREATE_BUDGET_MS;
  results.push({ name: "bulk-create", perOpMs: totalMs, budgetMs: BULK_CREATE_BUDGET_MS, pass });
  console.log(
    `  ${pass ? "[1;32mPASS[0m" : "[1;31mFAIL[0m"}  ${"bulk-create".padEnd(22)} ${String(totalMs).padStart(8)}ms  budget ${BULK_CREATE_BUDGET_MS}ms  (${BULK_CREATE_COUNT} files, ${(totalMs / BULK_CREATE_COUNT).toFixed(1)}ms/file)`,
  );
}

// Hard-linked unlink: pnpm's shape, and the one that resolved aliases for 30s.
const hardlink = await exec(
  session,
  `set -u
b=${mountPath}/.perflink-$$
rm -rf "$b"; mkdir -p "$b" || exit 3
echo payload > "$b/original"
ln "$b/original" "$b/alias" 2>/dev/null || { echo "hardlink unsupported"; rm -rf "$b"; exit 4; }
s=$(date +%s%N)
rm -f "$b/alias"
e=$(date +%s%N)
printf 'hardlink-unlink %s\\n' $(( (e-s)/1000000 ))
rm -rf "$b"`,
  120,
);
if (hardlink.code === 0) {
  const totalMs = Number(hardlink.stdout.trim().split(/\s+/)[1]);
  const pass = totalMs <= HARDLINK_UNLINK_BUDGET_MS;
  results.push({ name: "hardlink-unlink", perOpMs: totalMs, budgetMs: HARDLINK_UNLINK_BUDGET_MS, pass });
  console.log(
    `  ${pass ? "[1;32mPASS[0m" : "[1;31mFAIL[0m"}  ${"hardlink-unlink".padEnd(22)} ${String(totalMs).padStart(8)}ms  budget ${HARDLINK_UNLINK_BUDGET_MS}ms`,
  );
} else if (hardlink.code === 4) {
  console.log("  [1;33mSKIP[0m  hardlink-unlink        (hard links unsupported on this mount)");
}

/**
 * LAYER DECOMPOSITION.
 *
 * A single "writes are slow" number tells you nothing about which layer to fix,
 * and re-deriving the split by hand every time is how a regression in one layer
 * hides behind an improvement in another. Each layer is gated separately, and
 * the deltas are what actually name the culprit:
 *
 *   framework      = /health            — Node + Hono + network floor
 *   storage-write  = PUT, no watcher    — hashing, local write, index update
 *   watcher-ack    = PUT with watcher − PUT without
 *   vmd-overhead   = guest create − PUT with watcher
 *
 * Measured 2026-07-26: framework 1.9ms, storage 12.6ms, watcher-ack 13.4ms,
 * vmd-overhead ~40ms. Network RTT was 0.87ms, so neither the link nor the HTTP
 * framework is the cost — a conclusion that only exists because of this split.
 */
const gatewayUrl = process.env.VFS_GATEWAY_URL?.trim();
const vfsToken = process.env.CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN?.trim();
const liveOwner = process.env.VFS_LIVE_OWNER?.trim();
const idleOwner = process.env.VFS_IDLE_OWNER?.trim();

/** Ratcheted to the isolated-arena baseline (2026-07-26): framework 6.9,
 *  storage-write 14.4, watcher-ack 0.0, vmd-overhead 9.6. `watcher-ack` in
 *  particular went 13.4 -> 0.0; a 5ms budget would let all of that come back. */
const LAYER_BUDGETS_MS = {
  // Measured from the harness host, so this carries that host's RTT. Run the
  // arena from the vmd host for a floor that reflects vmd's own position.
  framework: 8,
  "storage-write": 15,
  "watcher-ack": 2,
  // 9.6ms measured. This is a DERIVED value (guest create minus gateway PUT),
  // so it carries the noise of both terms — kept looser than the direct gates
  // on purpose.
  "vmd-overhead": 12,
};

const timeHttp = async (label, request, samples = 12) => {
  let total = 0;
  let status = 0;
  for (let index = 0; index < samples; index += 1) {
    const started = performance.now();
    try {
      const response = await request();
      status = response.status;
      await response.arrayBuffer();
    } catch (error) {
      console.log(`  [1;33mSKIP[0m  ${label.padEnd(22)} (${error.message})`);
      return null;
    }
    total += performance.now() - started;
  }
  return { mean: total / samples, status };
};

if (gatewayUrl && vfsToken && liveOwner && idleOwner) {
  console.log("\nlayer decomposition");
  const put = (owner) => () =>
    fetch(`${gatewayUrl}/internal/chevalier/vfs/${owner}/file?path=.perfprobe-layer.txt`, {
      method: "PUT",
      headers: { authorization: `Bearer ${vfsToken}` },
      body: "x",
    });

  const framework = await timeHttp("framework", () => fetch(`${gatewayUrl}/health`));
  const idleWrite = await timeHttp("storage-write", put(idleOwner));
  const liveWrite = await timeHttp("watcher-write", put(liveOwner));

  if (framework) record("framework", framework.mean, LAYER_BUDGETS_MS.framework, "/health floor");
  if (idleWrite) {
    record("storage-write", idleWrite.mean, LAYER_BUDGETS_MS["storage-write"], "PUT, no watcher");
  }
  if (idleWrite && liveWrite) {
    // A mount waiting on its OWN acknowledgement is self-inflicted cost, and it
    // scales with the number of attached mounts.
    record(
      "watcher-ack",
      Math.max(0, liveWrite.mean - idleWrite.mean),
      LAYER_BUDGETS_MS["watcher-ack"],
      "publication ack round trip",
    );
  }
  const guestCreate = results.find((entry) => entry.name === "create");
  if (liveWrite && guestCreate) {
    // Everything the FUSE/vmd side adds on top of the gateway call it makes.
    record(
      "vmd-overhead",
      Math.max(0, guestCreate.perOpMs - liveWrite.mean),
      LAYER_BUDGETS_MS["vmd-overhead"],
      "guest create minus gateway PUT",
    );
  }
} else {
  console.log(
    "\nlayer decomposition SKIPPED (set VFS_GATEWAY_URL, CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN, VFS_LIVE_OWNER, VFS_IDLE_OWNER)",
  );
}

const failed = results.filter((entry) => !entry.pass);
console.log(`\n${results.length - failed.length}/${results.length} budgets met`);
if (failed.length > 0) {
  console.log("over budget:");
  for (const entry of failed) {
    console.log(`  ${entry.name}: ${entry.perOpMs.toFixed(1)}ms vs ${entry.budgetMs}ms budget`);
  }
}
process.exit(failed.length > 0 ? 1 : 0);
