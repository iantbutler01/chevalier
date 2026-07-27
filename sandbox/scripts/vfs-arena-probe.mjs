#!/usr/bin/env node
/**
 * A disposable arena VM plus one shell script, for isolating a single question.
 *
 * The budget suites answer "how much does this op class cost". They cannot
 * answer "why", because every class is a loop and a loop conflates cold and warm
 * behaviour. This runs an explicit sequence in the guest and prints per-step
 * milliseconds, so a question like "is the second round trip of a rewrite
 * structural, or is it a cold cache" gets a direct answer instead of an
 * inference from an average.
 *
 * Arena only — it refuses the production ports like `vfs-perf-arena.mjs` does.
 *
 * USAGE:
 *   SANDBOX_ENDPOINT=... SANDBOX_AUTH_TOKEN=... VFS_GATEWAY_URL=... \
 *   CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN=... SANDBOX_IMAGE=... \
 *     node sandbox/scripts/vfs-arena-probe.mjs
 */

import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");
const require = createRequire(import.meta.url);
const { Sandbox } = require(
  resolve(process.env.CHEVALIER_SANDBOX_MODULE_PATH?.trim() || join(repoRoot, "ts-sandbox", "index.js")),
);

const need = (name) => {
  const value = process.env[name]?.trim();
  if (!value) {
    console.error(`${name} is required`);
    process.exit(2);
  }
  return value;
};

const endpoint = need("SANDBOX_ENDPOINT");
const authToken = need("SANDBOX_AUTH_TOKEN");
const image = need("SANDBOX_IMAGE");
const gatewayUrl = need("VFS_GATEWAY_URL").replace(/\/+$/, "");
const bulkCount = Number.parseInt(process.env.VFS_BULK_COUNT?.trim() || "500", 10);
const settleMs = Number.parseInt(process.env.VFS_PROBE_SETTLE_MS?.trim() || "0", 10);
if (!Number.isSafeInteger(bulkCount) || bulkCount < 1) {
  console.error("VFS_BULK_COUNT must be a positive integer");
  process.exit(2);
}
if (!Number.isSafeInteger(settleMs) || settleMs < 0) {
  console.error("VFS_PROBE_SETTLE_MS must be a non-negative integer");
  process.exit(2);
}
const settlePublisher = async () => {
  if (settleMs > 0) {
    await new Promise((resolve) => setTimeout(resolve, settleMs));
  }
};

for (const [name, value] of [
  ["SANDBOX_ENDPOINT", endpoint],
  ["VFS_GATEWAY_URL", gatewayUrl],
]) {
  const port = (() => {
    try {
      return new URL(value).port;
    } catch {
      return "";
    }
  })();
  if (port === "18062" || port === "8930") {
    console.error(`refusing to probe production (${name}=${value})`);
    process.exit(2);
  }
}

const stamp = `${Date.now()}`;
const owner = `arena-probe-${stamp}`;
const sandbox = await Sandbox.connect(endpoint, { authToken, defaultImage: image });
const session = await sandbox.session({
  image,
  architecture: process.env.SANDBOX_ARCHITECTURE?.trim() || "amd64",
  name: `arena-probe-${stamp}`,
  metadata: { role: "chevalier-arena-probe" },
  autoStart: true,
  sharedMounts: [
    {
      guestPath: "/workspace",
      mountTag: `probe-${stamp}`.slice(0, 31),
      readOnly: false,
      availability: "shared-storage",
      continuity: "restore-cross-node",
      backendProfile: "openbracket-vfs-fuse",
      vfsEndpoint: `${gatewayUrl}/internal/chevalier/vfs/${owner}`,
      vfsScopePath: `probe/${stamp}/repo`,
    },
  ],
});
console.log(`probe session=${session.sessionId} owner=${owner}`);

const exec = async (command, timeoutSecs = 180) => {
  const handle = await session.exec(command, { shell: "/bin/bash", closeStdinOnStart: true, timeoutSecs });
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

let mounted = false;
for (let attempt = 0; attempt < 90; attempt += 1) {
  const probe = await exec(
    `test "$(findmnt -n -o FSTYPE /workspace 2>/dev/null)" = virtiofs && touch /workspace/.probe-ready && rm -f /workspace/.probe-ready && echo ok`,
    30,
  );
  if (probe.code === 0 && probe.stdout.includes("ok")) {
    mounted = true;
    break;
  }
  await new Promise((r) => setTimeout(r, 2_000));
}

let exitCode = 0;
if (!mounted) {
  console.error("probe VM never mounted /workspace as virtiofs");
  exitCode = 2;
} else {
  // Each step is timed on its own so a warm repeat is distinguishable from a
  // cold first touch. `t` reports whole-milliseconds for one command.
  // Route counts, not milliseconds. Bracketing the loop with a counter reset is
  // the only way to say how many round trips the bulk creates ACTUALLY cost on
  // a real mount — the op-class inventory counts against an in-process stub
  // that never sees the kernel's own lookups, and a timing can only be divided
  // by a guess.
  const counters = async (reset) => {
    const url = `${gatewayUrl}/__vfs_route_counters${reset ? "?reset=1" : ""}`;
    try {
      const response = await fetch(url);
      return response.ok ? await response.json() : {};
    } catch {
      return {};
    }
  };
  await counters(true);
  const bulkPath = `/workspace/.bulk-${stamp}`;
  const bulk = await exec(
    `set -u
b=${bulkPath}
rm -rf "$b"; mkdir -p "$b"; cd "$b"
s=$(date +%s%N)
for i in $(seq 1 ${bulkCount}); do echo x > "f$i"; done
e=$(date +%s%N)
printf 'bulk-create ${bulkCount} files: %s ms total, %s ms/file\n' $(( (e-s)/1000000 )) $(( (e-s)/1000000/${bulkCount} ))
s=$(date +%s%N)
sync -d "$b"
e=$(date +%s%N)
printf 'bulk-publish barrier: %s ms\n' $(( (e-s)/1000000 ))`,
    600,
  );
  console.log(bulk.stdout.trim());
  await settlePublisher();
  console.log(`ROUTES for ${bulkCount} creates:`, JSON.stringify(await counters(true)));
  const cleanup = await exec(`rm -rf ${bulkPath}`, 300);
  if (cleanup.code !== 0) {
    console.error(`bulk cleanup failed: ${cleanup.stdout.trim()}`);
    exitCode = 1;
  }
  await settlePublisher();
  await counters(true);

  const script = `set -u
b=/workspace/.arena-probe-$$
rm -rf "$b"; mkdir -p "$b"; cd "$b"
t() { s=$(date +%s%N); eval "$2" >/dev/null 2>&1; e=$(date +%s%N); printf '%-28s %s ms\\n' "$1" $(( (e-s)/1000000 )); }

: > f
t "stat-cold                 " 'stat f'
t "stat-warm                 " 'stat f'
t "truncate-only (: > f)     " ': > f'
t "truncate-only-2 (: > f)   " ': > f'
t "write-no-trunc (dd)       " 'dd if=/dev/zero of=f bs=1 count=1 conv=notrunc'
t "rewrite-1 (echo y > f)    " 'echo y > f'
t "rewrite-2 (same file)     " 'echo y > f'
t "rewrite-3 (same file)     " 'echo y > f'
t "append (no O_TRUNC)       " 'printf z >> f'
t "append-2 (no O_TRUNC)     " 'printf z >> f'
: > g
t "stat g                    " 'stat g'
t "rewrite g (cold-ish)      " 'echo y > g'
cd /; rm -rf "$b"`;
  const run = await exec(script, 300);
  console.log(run.stdout.trim());
  if (run.code !== 0) exitCode = 1;
  await settlePublisher();
  console.log("ROUTES for scalar probe:", JSON.stringify(await counters(true)));
}

try {
  await sandbox.discardSessionById(session.sessionId);
  console.log(`probe discarded (${session.sessionId})`);
} catch (error) {
  console.error(`probe discard FAILED: ${error.message}`);
  exitCode = 1;
}
process.exit(exitCode);
