#!/usr/bin/env node
/**
 * Install one real frontend manifest into node_modules on a disposable arena
 * VFS mount. The package files are read from the harness host and written into
 * /workspace through the guest session API; no host node_modules are copied.
 *
 * USAGE:
 *   VFS_INSTALL_PACKAGE_JSON=/path/to/package.json \
 *   VFS_INSTALL_LOCKFILE=/path/to/pnpm-lock.yaml \
 *   SANDBOX_ENDPOINT=http://<arena-vmd>:18072 SANDBOX_AUTH_TOKEN=... \
 *   VFS_GATEWAY_URL=http://<arena-api>:8931 SANDBOX_IMAGE=... \
 *     node sandbox/scripts/vfs-node-modules-arena.mjs
 *
 * The disposable VM defaults to OpenBracket's product sandbox posture:
 * 8 vCPU, 16 GiB RAM, and a 64 GiB boot disk. VFS_INSTALL_{VCPU,MEMORY_MB,
 * DISK_GB} can override those values for an explicit resource-shape test.
 */

import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { basename, dirname, join, resolve } from "node:path";
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
const positiveInteger = (name, fallback) => {
  const value = Number.parseInt(process.env[name]?.trim() || `${fallback}`, 10);
  if (!Number.isSafeInteger(value) || value < 1) {
    console.error(`${name} must be a positive integer`);
    process.exit(2);
  }
  return value;
};
const shellQuote = (value) => `'${value.replaceAll("'", `'\\''`)}'`;

const endpoint = need("SANDBOX_ENDPOINT");
const authToken = need("SANDBOX_AUTH_TOKEN");
const image = need("SANDBOX_IMAGE");
const gatewayUrl = need("VFS_GATEWAY_URL").replace(/\/+$/, "");
const packageJsonPath = need("VFS_INSTALL_PACKAGE_JSON");
const lockfilePath = process.env.VFS_INSTALL_LOCKFILE?.trim() || "";
const packageJson = readFileSync(packageJsonPath);
const lockfile = lockfilePath ? readFileSync(lockfilePath) : null;
const manifest = JSON.parse(packageJson.toString("utf8"));
const installTimeoutSecs = positiveInteger("VFS_INSTALL_TIMEOUT_SECS", 300);
const sandboxVcpu = positiveInteger("VFS_INSTALL_VCPU", 8);
const sandboxMemoryMb = positiveInteger("VFS_INSTALL_MEMORY_MB", 16 * 1024);
const sandboxDiskGb = positiveInteger("VFS_INSTALL_DISK_GB", 64);
const keepOnFailure = process.env.VFS_INSTALL_KEEP_ON_FAILURE?.trim() === "1";

for (const [name, value] of [
  ["SANDBOX_ENDPOINT", endpoint],
  ["VFS_GATEWAY_URL", gatewayUrl],
]) {
  let port = "";
  try {
    port = new URL(value).port;
  } catch {
    // The required-value checks above produce the useful error.
  }
  if (port === "18062" || port === "8930") {
    console.error(`refusing to install against production (${name}=${value})`);
    process.exit(2);
  }
}

const stamp = `${Date.now()}`;
const owner = `node-modules-arena-${stamp}`;
const sandbox = await Sandbox.connect(endpoint, {
  authToken,
  defaultImage: image,
  defaultVcpu: sandboxVcpu,
  defaultMemoryMb: sandboxMemoryMb,
  defaultDiskGb: sandboxDiskGb,
});
const session = await sandbox.session({
  image,
  architecture: process.env.SANDBOX_ARCHITECTURE?.trim() || "amd64",
  name: `node-modules-arena-${stamp}`,
  metadata: {
    role: "chevalier-node-modules-arena",
    package: manifest.name || basename(dirname(packageJsonPath)),
  },
  autoStart: true,
  sharedMounts: [
    {
      guestPath: "/workspace",
      mountTag: `node-modules-${stamp}`.slice(0, 31),
      readOnly: false,
      availability: "shared-storage",
      continuity: "restore-cross-node",
      backendProfile: "openbracket-vfs-fuse",
      vfsEndpoint: `${gatewayUrl}/internal/chevalier/vfs/${owner}`,
      vfsScopePath: `node-modules/${stamp}/frontend`,
    },
  ],
});
console.log(
  `node_modules arena session=${session.sessionId} owner=${owner} package=${manifest.name || "(unnamed)"} resources=${sandboxVcpu}vcpu/${sandboxMemoryMb}MiB/${sandboxDiskGb}GiB`,
);

const exec = async (command, timeoutSecs = 120) => {
  const handle = await session.exec(command, {
    shell: "/bin/bash",
    closeStdinOnStart: true,
    timeoutSecs,
  });
  let output = "";
  let code = null;
  for (;;) {
    const event = await handle.next();
    if (event === null) break;
    if (event.data && (event.type === "stdout" || event.type === "stderr")) {
      const text = Buffer.from(event.data).toString("utf8");
      output += text;
      process.stdout.write(text);
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
  return { code, output };
};

const counters = async (reset) => {
  try {
    const response = await fetch(
      `${gatewayUrl}/__vfs_route_counters${reset ? "?reset=1" : ""}`,
    );
    return response.ok ? await response.json() : {};
  } catch {
    return {};
  }
};

let exitCode = 0;
try {
  let mounted = false;
  for (let attempt = 0; attempt < 90; attempt += 1) {
    const probe = await exec(
      `test "$(findmnt -n -o FSTYPE /workspace 2>/dev/null)" = virtiofs && echo mounted`,
      30,
    );
    if (probe.code === 0 && probe.output.includes("mounted")) {
      mounted = true;
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 2_000));
  }
  if (!mounted) {
    throw new Error("arena VM never mounted /workspace as virtiofs");
  }

  console.log("package-manager preflight");
  const preflight = await exec(
    `set -euxo pipefail
node --version
npm --version
npm config get registry
timeout --signal=TERM --kill-after=5s 15s npm ping --loglevel=verbose
if command -v corepack >/dev/null 2>&1; then
  corepack pnpm@9.15.4 --version
elif command -v npx >/dev/null 2>&1; then
  timeout --signal=TERM --kill-after=10s 60s npx --yes --loglevel=verbose pnpm@9.15.4 --version
else
  echo 'neither corepack nor npx is available' >&2
  exit 127
fi`,
    90,
  );
  if (preflight.code !== 0) {
    throw new Error(`package-manager preflight failed with exit ${preflight.code}`);
  }

  await session.writeFile("/workspace/package.json", packageJson);
  if (lockfile) {
    await session.writeFile(`/workspace/${basename(lockfilePath)}`, lockfile);
  }
  await counters(true);

  const lockfileFlag = lockfile ? "--frozen-lockfile" : "--no-frozen-lockfile";
  const install = await exec(
    `set -euo pipefail
cd /workspace
start=$(date +%s)
store_dir=/var/cache/openbracket/pnpm-vfs-install-store
mkdir -p "$store_dir"
printf 'workspace-fstype=%s\\n' "$(findmnt -n -o FSTYPE /workspace)"
printf 'workspace-device=%s store-device=%s\\n' "$(stat -c %d /workspace)" "$(stat -c %d "$store_dir")"
if command -v corepack >/dev/null 2>&1; then
  package_manager=(corepack pnpm@9.15.4)
elif command -v npx >/dev/null 2>&1; then
  package_manager=(npx --yes pnpm@9.15.4)
else
  echo 'neither corepack nor npx is available' >&2
  exit 127
fi
CI=1 timeout --signal=TERM --kill-after=10s ${installTimeoutSecs}s "\${package_manager[@]}" install ${lockfileFlag} --store-dir "$store_dir" --package-import-method=copy --reporter=append-only
printf 'install-wall-seconds=%s\\n' $(( $(date +%s) - start ))
`,
    Math.min(installTimeoutSecs + 30, 600),
  );
  if (install.code !== 0) {
    console.error(`node_modules install failed with exit ${install.code}`);
    const failedCopy = /copyfile '([^']+)' -> '([^']+)'/.exec(install.output);
    const failedCopyProbe = failedCopy
      ? `printf '%s\\n' 'failed copy endpoints:'
stat -c 'source type=%F size=%s mode=%a path=%n' -- ${shellQuote(failedCopy[1])} || true
stat -c 'destination type=%F size=%s mode=%a path=%n' -- ${shellQuote(failedCopy[2])} || true
stat -c 'destination-parent type=%F mode=%a path=%n' -- ${shellQuote(dirname(failedCopy[2]))} || true`
      : "";
    await exec(
      `printf '%s\\n' 'partial workspace tree:'
${failedCopyProbe}
find /workspace -maxdepth 3 -printf '%y %s %p\\n' 2>/dev/null | head -200
esbuild_bin="$(find /workspace/node_modules/.pnpm -path '*/node_modules/esbuild/bin/esbuild' -type f -print -quit 2>/dev/null || true)"
if [[ -n "$esbuild_bin" ]]; then
  stat -c 'esbuild-before mode=%a inode=%i links=%h path=%n' "$esbuild_bin"
  chmod 0755 "$esbuild_bin"
  stat -c 'esbuild-after-chmod mode=%a inode=%i links=%h path=%n' "$esbuild_bin"
  "$esbuild_bin" --version || true
fi`,
      30,
    );
    exitCode = 1;
  } else {
    const verify = await exec(
      `set -euo pipefail
cd /workspace
test -d node_modules/.pnpm
test ! -e .pnpm-store
test -e node_modules/react
test -e node_modules/vite
test "$(findmnt -n -o FSTYPE -T node_modules)" = virtiofs
printf 'node_modules-files=%s\\n' "$(find node_modules -type f | wc -l)"
du -sm node_modules | awk '{print "node_modules-megabytes=" $1}'`,
      120,
    );
    if (verify.code !== 0) {
      console.error(`node_modules verification failed with exit ${verify.code}`);
      exitCode = 1;
    }
  }
  console.log(`ROUTES for node_modules install: ${JSON.stringify(await counters(true))}`);
} catch (error) {
  console.error(`node_modules arena failed: ${error.message}`);
  exitCode = 1;
} finally {
  if (exitCode !== 0 && keepOnFailure) {
    console.error(`node_modules arena retained for diagnosis (${session.sessionId})`);
    process.exit(exitCode);
  }
  try {
    await sandbox.discardSessionById(session.sessionId);
    console.log(`node_modules arena discarded (${session.sessionId})`);
  } catch (error) {
    console.error(`node_modules arena discard FAILED: ${error.message}`);
    exitCode = 1;
  }
}

process.exit(exitCode);
