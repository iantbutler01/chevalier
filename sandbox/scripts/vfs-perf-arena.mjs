#!/usr/bin/env node
/**
 * Disposable performance/correctness arena.
 *
 * Creates its OWN VM and its OWN VFS scope, runs the budget suites inside it,
 * and discards everything. Nothing touches a real workspace.
 *
 * This exists because the budgets were previously measured against a live
 * thread's VM. That was wrong twice over: the numbers moved with whatever the
 * agent in that VM happened to be doing (68.6ms/file idle versus 134.6ms/file
 * busy — a 2x swing that has nothing to do with the code), and the fixtures
 * wrote into a synced workspace. A benchmark that competes with a user for the
 * machine it is measuring is not a benchmark.
 *
 * It must be pointed at the ARENA STACK, not the production one: a second vmd
 * and a second API, built from the working tree (OpenBracket
 * scripts/deploy-arena.sh). Its own VM and its own scope are not enough on
 * their own — run against the production endpoints and every number describes
 * the DEPLOYED build, so a change measures as having done nothing. The guard
 * below refuses the production ports outright rather than producing numbers
 * that look real.
 *
 * USAGE:
 *   scripts/deploy-arena.sh                     # in the OpenBracket repo
 *   SANDBOX_ENDPOINT=http://<bismuth>:18072 SANDBOX_AUTH_TOKEN=... \
 *   CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN=... \
 *   VFS_GATEWAY_URL=http://<corvidae>:8931 SANDBOX_IMAGE=... \
 *     node sandbox/scripts/vfs-perf-arena.mjs
 */

import { spawnSync } from "node:child_process";
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
const vfsToken = need("CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN");

// The production ports, by number. A benchmark aimed at them is answered by
// binaries nobody is testing: the change under measurement is not deployed
// there, so every gate passes or fails on the state of the last release and the
// run reads as "the fix did nothing". Refuse rather than report.
//
// Set VFS_ARENA_ALLOW_PRODUCTION=1 to deliberately profile the deployed stack.
const PRODUCTION_PORTS = new Map([
  ["18062", "vmd"],
  ["8930", "gateway"],
]);
if (process.env.VFS_ARENA_ALLOW_PRODUCTION?.trim() !== "1") {
  const aimedAtProduction = [
    ["SANDBOX_ENDPOINT", endpoint],
    ["VFS_GATEWAY_URL", gatewayUrl],
  ].flatMap(([name, value]) => {
    let port;
    try {
      port = new URL(value).port;
    } catch {
      return [];
    }
    const role = PRODUCTION_PORTS.get(port);
    return role === undefined ? [] : [`${name}=${value} is the production ${role}`];
  });
  if (aimedAtProduction.length > 0) {
    console.error(
      `refusing to benchmark production:\n  ${aimedAtProduction.join("\n  ")}\n` +
        "Stand up the arena stack (OpenBracket scripts/deploy-arena.sh) and point at\n" +
        "its ports, or set VFS_ARENA_ALLOW_PRODUCTION=1 to override deliberately.",
    );
    process.exit(2);
  }
}

// Distinct per run so a leaked arena can never collide with a later one, and so
// nothing is ever shared with a real thread's owner namespace.
const stamp = `${Date.now()}`;
const owner = `perf-arena-${stamp}`;
const scopePath = `arena/${stamp}/repo`;
const mountTag = `arena-${stamp}`.slice(0, 31);

const sandbox = await Sandbox.connect(endpoint, { authToken, defaultImage: image });

console.log(`arena owner=${owner} scope=${scopePath}`);
const session = await sandbox.session({
  image,
  architecture: process.env.SANDBOX_ARCHITECTURE?.trim() || "amd64",
  name: `perf-arena-${stamp}`,
  metadata: { role: "chevalier-perf-arena", arena: stamp },
  autoStart: true,
  sharedMounts: [
    {
      guestPath: "/workspace",
      mountTag,
      readOnly: false,
      // Must match what production mounts declare. Omitting these defaults the
      // mount to node-local/restart-same-node, and vmd then refuses the session
      // outright: a tier_b_eligible VM requires cross-node-restorable mounts.
      availability: "shared-storage",
      continuity: "restore-cross-node",
      backendProfile: "openbracket-vfs-fuse",
      vfsEndpoint: `${gatewayUrl}/internal/chevalier/vfs/${owner}`,
      vfsScopePath: scopePath,
    },
  ],
});
console.log(`arena session=${session.sessionId}`);

const exec = async (command, timeoutSecs = 120) => {
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

// Wait for the mount rather than assuming it: a VM that booted without its
// virtiofs share would otherwise "measure" the guest's own root disk and report
// spectacular, meaningless numbers.
let mounted = false;
for (let attempt = 0; attempt < 90; attempt += 1) {
  const probe = await exec(
    `test "$(findmnt -n -o FSTYPE /workspace 2>/dev/null)" = virtiofs && touch /workspace/.arena-ready && rm -f /workspace/.arena-ready && echo ok`,
    30,
  );
  if (probe.code === 0 && probe.stdout.includes("ok")) {
    mounted = true;
    break;
  }
  await new Promise((resolve) => setTimeout(resolve, 2_000));
}

let exitCode = 0;
if (!mounted) {
  const diag = await exec("findmnt -n -o TARGET,FSTYPE,SOURCE /workspace; dmesg | tail -5", 30);
  console.error(`arena VM never mounted /workspace as virtiofs:\n${diag.stdout}`);
  exitCode = 2;
} else {
  console.log("arena mount ready\n");
  const childEnv = {
    ...process.env,
    // The suites open their own provider connection to attach to this session,
    // and the Chevalier provider refuses to construct without a default image
    // even when it will only ever attach, never create.
    BRACKET_VM_IMAGE: process.env.BRACKET_VM_IMAGE?.trim() || image,
    SESSION_ID: session.sessionId,
    VFS_LIVE_OWNER: owner,
    // The layer decomposition compares a watched owner against an unwatched
    // one; the arena owner is watched by its own VM, so an idle sibling scope
    // provides the contrast.
    VFS_IDLE_OWNER: `${owner}-idle`,
  };
  for (const suite of ["vfs-mounted-perf-budget.mjs", "vfs-standard-budget.mjs"]) {
    console.log(`\n===== ${suite} =====`);
    const run = spawnSync(process.execPath, [join(scriptDir, suite)], {
      env: childEnv,
      stdio: "inherit",
      timeout: 45 * 60 * 1000,
    });
    if (run.status !== 0) exitCode = 1;
  }
}

// Always discard: an arena that outlives its run is a VM nobody owns, holding a
// scope nobody will clean up.
try {
  await sandbox.discardSessionById(session.sessionId);
  console.log(`\narena discarded (${session.sessionId})`);
} catch (error) {
  console.error(`arena discard FAILED for ${session.sessionId}: ${error.message}`);
  exitCode = 1;
}
process.exit(exitCode);
