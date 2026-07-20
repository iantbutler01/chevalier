import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  buildPosixTrace,
  runPosixModelTorture,
} from "./posix-model-torture.mjs";

const runShell = (command, timeoutSecs) =>
  new Promise((resolveCommand) => {
    const child = spawn("/bin/bash", ["-lc", `set -euo pipefail\n${command}`], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8");
    });
    const timer = setTimeout(() => child.kill("SIGKILL"), timeoutSecs * 1000);
    child.once("close", (code) => {
      clearTimeout(timer);
      resolveCommand({ code, stdout, stderr });
    });
  });

test("seeded traces are deterministic and cover every required operation", () => {
  const first = buildPosixTrace({ seed: "deterministic-coverage", steps: 500 });
  const second = buildPosixTrace({
    seed: "deterministic-coverage",
    steps: 500,
  });
  assert.deepEqual(first, second);
  const operations = new Set(first.map((action) => action.op));
  assert.deepEqual([...operations].sort(), [
    "chmod",
    "create",
    "mkdir",
    "open_unlink",
    "pwrite",
    "read",
    "readlink",
    "rename_overwrite",
    "rmdir",
    "sparse",
    "stat",
    "symlink",
    "truncate",
    "unlink",
    "write",
  ]);
});

test("the controller localizes an injected cross-client snapshot divergence", async () => {
  const root = await mkdtemp(join(tmpdir(), "posix-model-negative-"));
  const mountPath = join(root, "mounted");
  let injected = false;
  try {
    const result = await runPosixModelTorture({
      sessions: [{ name: "local-a" }, { name: "local-b" }],
      execGuest: async (session, command, timeoutSecs) => {
        const commandResult = await runShell(command, timeoutSecs);
        if (
          !injected &&
          session.name === "local-b" &&
          commandResult.code === 0 &&
          command.includes("--snapshot") &&
          command.includes(mountPath)
        ) {
          const output = JSON.parse(commandResult.stdout.trim());
          output.snapshot.push({
            path: "injected-divergence",
            kind: "file",
            mode: 0o644,
            size: 0,
            content: {
              length: 0,
              sha256: "injected",
              prefixHex: "",
              suffixHex: "",
            },
          });
          commandResult.stdout = `${JSON.stringify(output)}\n`;
          injected = true;
        }
        return commandResult;
      },
      mountPath,
      seed: "negative-proof",
      oneClientSteps: 0,
      twoClientSteps: 0,
      operationTimeoutMs: 10_000,
      suiteTimeoutMs: 2 * 60_000,
    });
    assert.equal(injected, true);
    assert.equal(result.status, "fail");
    assert.equal(result.modes[0].status, "pass");
    assert.equal(result.modes[1].status, "fail");
    assert.equal(result.modes[1].failure.index, 0);
    assert.match(
      result.modes[1].failure.message,
      /cross-client snapshot diverged/,
    );
    assert.deepEqual(result.modes[1].cleanupErrors, []);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
