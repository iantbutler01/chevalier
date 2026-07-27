#!/usr/bin/env node
/**
 * Reproduce the computer-use desktop bring-up in a disposable arena VM and
 * report which component actually fails.
 *
 * A black VNC panel that reads "Live" is not one failure, it is three
 * indistinguishable ones: Xvfb never started, the window manager never started
 * (so nothing is ever mapped and the server paints an empty root), or the app
 * was launched against a different display than the one x11vnc attached to.
 * Guessing between them from a screenshot is how the last hour got spent, so
 * this runs the documented sequence and prints each component's own log.
 *
 * Arena only — refuses the production ports.
 *
 * USAGE:
 *   SANDBOX_ENDPOINT=... SANDBOX_AUTH_TOKEN=... VFS_GATEWAY_URL=... \
 *   CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN=... SANDBOX_IMAGE=... \
 *     node sandbox/scripts/vfs-arena-desktop-probe.mjs
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
const sandbox = await Sandbox.connect(endpoint, { authToken, defaultImage: image });
const session = await sandbox.session({
  image,
  architecture: process.env.SANDBOX_ARCHITECTURE?.trim() || "amd64",
  name: `desktop-probe-${stamp}`,
  metadata: { role: "chevalier-desktop-probe" },
  autoStart: true,
});
console.log(`desktop probe session=${session.sessionId}`);

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

// Wait for the guest to answer at all before asking it anything.
for (let attempt = 0; attempt < 60; attempt += 1) {
  const ready = await exec("echo ok", 30);
  if (ready.code === 0 && ready.stdout.includes("ok")) break;
  await new Promise((r) => setTimeout(r, 2_000));
}

const script = `set -u
D=\${DISPLAY:-:99}
R=/tmp/openbracket-computer
mkdir -p "$R"
export DISPLAY="$D"

echo "--- inherited environment ---"
echo "DISPLAY=\${DISPLAY:-<unset>}"

echo "--- packages present ---"
for c in Xvfb openbox x11vnc xdpyinfo xdotool scrot dbus-daemon chromium chromium-browser firefox; do
  printf '%-18s %s\\n' "$c" "$(command -v $c 2>/dev/null || echo MISSING)"
done

echo "--- is a desktop already running? ---"
pgrep -ax Xvfb   || echo "Xvfb NOT running"
pgrep -ax openbox || echo "openbox NOT running"
pgrep -ax x11vnc || echo "x11vnc NOT running"

echo "--- bring up the documented sequence ---"
if ! xdpyinfo -display "$D" >/dev/null 2>&1; then
  nohup Xvfb "$D" -screen 0 1280x800x24 >"$R/xvfb.log" 2>&1 &
fi
for i in $(seq 1 40); do xdpyinfo -display "$D" >/dev/null 2>&1 && break; sleep 0.25; done
xdpyinfo -display "$D" >/dev/null 2>&1 && echo "Xvfb ready on $D" || echo "Xvfb FAILED on $D"

if command -v openbox >/dev/null 2>&1 && ! pgrep -x openbox >/dev/null 2>&1; then
  nohup openbox >"$R/openbox.log" 2>&1 &
  sleep 1
fi
pgrep -x openbox >/dev/null 2>&1 && echo "openbox running" || echo "openbox NOT running"

if command -v x11vnc >/dev/null 2>&1 && ! pgrep -x x11vnc >/dev/null 2>&1; then
  nohup x11vnc -display "$D" -localhost -forever -shared -viewonly -rfbport 5900 -nopw >"$R/x11vnc.log" 2>&1 &
  sleep 1
fi
pgrep -x x11vnc >/dev/null 2>&1 && echo "x11vnc running" || echo "x11vnc NOT running"

echo "--- does anything MAP a window? ---"
browser=""
for c in chromium chromium-browser firefox firefox-esr; do
  command -v "$c" >/dev/null 2>&1 && { browser="$c"; break; }
done
if [ -n "$browser" ]; then
  nohup "$browser" --no-sandbox --disable-gpu about:blank >"$R/browser.log" 2>&1 &
  sleep 6
  if command -v xdotool >/dev/null 2>&1; then
    echo "mapped windows: $(xdotool search --onlyvisible --name '.*' 2>/dev/null | wc -l)"
  fi
  xdpyinfo -display "$D" 2>/dev/null | grep -E "^  dimensions|^  depth" | head -2
else
  echo "no browser found"
fi

echo "--- logs ---"
for f in xvfb openbox x11vnc browser; do
  echo "== $f.log =="
  tail -12 "$R/$f.log" 2>/dev/null || echo "(none)"
done`;

const run = await exec(script, 300);
console.log(run.stdout.trim());

try {
  await sandbox.discardSessionById(session.sessionId);
  console.log(`desktop probe discarded (${session.sessionId})`);
} catch (error) {
  console.error(`discard FAILED: ${error.message}`);
}
process.exit(0);
