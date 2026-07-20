#!/usr/bin/env node

import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const enginePath = join(scriptDirectory, "posix-model-torture.py");
const engineSource = await readFile(enginePath);

const shellQuote = (value) => `'${String(value).replaceAll("'", `'\"'\"'`)}'`;

const stable = (value) => JSON.stringify(value);

const deepEqual = (left, right) =>
  JSON.stringify(left) === JSON.stringify(right);

const hashSeed = (seed) => {
  let value = 2166136261;
  for (const character of String(seed)) {
    value ^= character.codePointAt(0);
    value = Math.imul(value, 16777619);
  }
  return value >>> 0;
};

const randomGenerator = (seed) => {
  let state = hashSeed(seed) || 0x9e3779b9;
  return () => {
    state += 0x6d2b79f5;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
};

const choose = (random, values) => values[Math.floor(random() * values.length)];

const randomBytesHex = (random, length) => {
  const bytes = Buffer.alloc(length);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Math.floor(random() * 256);
  }
  return bytes.toString("hex");
};

const parentPath = (path) => {
  const slash = path.lastIndexOf("/");
  return slash < 0 ? "" : path.slice(0, slash);
};

const applyModel = (entries, action) => {
  const { op, path } = action;
  if (op === "mkdir") entries.set(path, { type: "directory" });
  if (op === "create" || op === "sparse") entries.set(path, { type: "file" });
  if (op === "symlink")
    entries.set(path, { type: "symlink", target: action.target });
  if (op === "unlink" || op === "open_unlink" || op === "rmdir")
    entries.delete(path);
  if (op === "rename_overwrite") {
    const source = entries.get(path);
    entries.delete(path);
    entries.delete(action.destination);
    entries.set(action.destination, source);
  }
};

const fixedTrace = () => [
  { op: "mkdir", path: "tree", mode: 0o755 },
  { op: "mkdir", path: "tree/sub", mode: 0o750 },
  {
    op: "create",
    path: "tree/a",
    mode: 0o644,
    dataHex: Buffer.from("alpha").toString("hex"),
  },
  { op: "read", path: "tree/a", offset: 0, length: 64 },
  { op: "stat", path: "tree/a" },
  { op: "chmod", path: "tree/a", mode: 0o751 },
  {
    op: "write",
    path: "tree/a",
    dataHex: Buffer.from("0123456789").toString("hex"),
  },
  {
    op: "pwrite",
    path: "tree/a",
    offset: 4,
    dataHex: Buffer.from("XYZ").toString("hex"),
  },
  { op: "truncate", path: "tree/a", size: 9 },
  {
    op: "sparse",
    path: "tree/sparse",
    mode: 0o640,
    size: 32768,
    offset: 16384,
    dataHex: Buffer.from("HOLE").toString("hex"),
  },
  { op: "symlink", path: "tree/a-link", target: "a" },
  { op: "readlink", path: "tree/a-link" },
  {
    op: "create",
    path: "tree/overwrite-source",
    mode: 0o600,
    dataHex: Buffer.from("new").toString("hex"),
  },
  {
    op: "create",
    path: "tree/overwrite-target",
    mode: 0o644,
    dataHex: Buffer.from("old").toString("hex"),
  },
  {
    op: "rename_overwrite",
    path: "tree/overwrite-source",
    destination: "tree/overwrite-target",
  },
  {
    op: "create",
    path: "tree/open-unlink",
    mode: 0o644,
    dataHex: Buffer.from("survives").toString("hex"),
  },
  {
    op: "open_unlink",
    path: "tree/open-unlink",
    suffixHex: Buffer.from("-fd").toString("hex"),
  },
  { op: "mkdir", path: "tree/empty", mode: 0o700 },
  { op: "rmdir", path: "tree/empty" },
  {
    op: "create",
    path: "tree/remove-me",
    mode: 0o600,
    dataHex: Buffer.from("remove").toString("hex"),
  },
  { op: "unlink", path: "tree/remove-me" },
];

export const buildPosixTrace = ({ seed, steps }) => {
  const random = randomGenerator(seed);
  const entries = new Map();
  const trace = fixedTrace();
  for (const action of trace) applyModel(entries, action);
  let nextName = 0;

  const pathsOfType = (type) =>
    [...entries.entries()]
      .filter(([, entry]) => entry.type === type)
      .map(([path]) => path);
  const directories = () => ["", ...pathsOfType("directory")];
  const newPath = (prefix = "entry") => {
    const directory = choose(random, directories());
    const name = `${prefix}-${String(nextName++).padStart(4, "0")}`;
    return directory ? `${directory}/${name}` : name;
  };
  const isEmptyDirectory = (path) =>
    ![...entries.keys()].some((candidate) => candidate.startsWith(`${path}/`));

  for (let index = 0; index < steps; index += 1) {
    const files = pathsOfType("file");
    const links = pathsOfType("symlink");
    const removable = [...files, ...links];
    const emptyDirectories = pathsOfType("directory").filter(isEmptyDirectory);
    const choices = ["create", "mkdir", "sparse"];
    if (files.length > 0) {
      choices.push(
        "read",
        "stat",
        "write",
        "pwrite",
        "truncate",
        "chmod",
        "open_unlink",
      );
    }
    if (removable.length > 0) choices.push("unlink", "rename_overwrite");
    if (directories().length > 0) choices.push("symlink");
    if (links.length > 0) choices.push("readlink");
    if (emptyDirectories.length > 0) choices.push("rmdir");

    let action;
    const operation = choose(random, choices);
    if (operation === "create") {
      action = {
        op: operation,
        path: newPath("file"),
        mode: choose(random, [0o600, 0o640, 0o644, 0o700, 0o751]),
        dataHex: randomBytesHex(random, Math.floor(random() * 257)),
      };
    } else if (operation === "mkdir") {
      action = {
        op: operation,
        path: newPath("dir"),
        mode: choose(random, [0o700, 0o750, 0o755]),
      };
    } else if (operation === "sparse") {
      const size = 4096 + Math.floor(random() * 61440);
      const dataLength = 1 + Math.floor(random() * 64);
      const offset = Math.floor(random() * Math.max(1, size - dataLength));
      action = {
        op: operation,
        path: newPath("sparse"),
        mode: choose(random, [0o600, 0o640, 0o644]),
        size,
        offset,
        dataHex: randomBytesHex(random, dataLength),
      };
    } else if (operation === "read") {
      action = {
        op: operation,
        path: choose(random, files),
        offset: 0,
        length: 131072,
      };
    } else if (operation === "stat") {
      action = { op: operation, path: choose(random, files) };
    } else if (operation === "write") {
      action = {
        op: operation,
        path: choose(random, files),
        dataHex: randomBytesHex(random, Math.floor(random() * 2049)),
      };
    } else if (operation === "pwrite") {
      action = {
        op: operation,
        path: choose(random, files),
        offset: Math.floor(random() * 32768),
        dataHex: randomBytesHex(random, 1 + Math.floor(random() * 128)),
      };
    } else if (operation === "truncate") {
      action = {
        op: operation,
        path: choose(random, files),
        size: Math.floor(random() * 65537),
      };
    } else if (operation === "chmod") {
      action = {
        op: operation,
        path: choose(random, files),
        mode: choose(random, [0o600, 0o640, 0o644, 0o700, 0o751]),
      };
    } else if (operation === "symlink") {
      const path = newPath("link");
      const sameDirectoryFiles = files.filter(
        (file) => parentPath(file) === parentPath(path),
      );
      const target =
        sameDirectoryFiles.length > 0
          ? choose(random, sameDirectoryFiles).split("/").at(-1)
          : `missing-${nextName}`;
      action = { op: operation, path, target };
    } else if (operation === "readlink") {
      action = { op: operation, path: choose(random, links) };
    } else if (operation === "unlink") {
      action = { op: operation, path: choose(random, removable) };
    } else if (operation === "open_unlink") {
      action = {
        op: operation,
        path: choose(random, files),
        suffixHex: randomBytesHex(random, 1 + Math.floor(random() * 32)),
      };
    } else if (operation === "rename_overwrite") {
      const source = choose(random, removable);
      const destinations = removable.filter((path) => path !== source);
      action = {
        op: operation,
        path: source,
        destination:
          destinations.length > 0 && random() < 0.55
            ? choose(random, destinations)
            : newPath("renamed"),
      };
    } else if (operation === "rmdir") {
      action = { op: operation, path: choose(random, emptyDirectories) };
    } else {
      throw new Error(`trace generator missed ${operation}`);
    }
    trace.push(action);
    applyModel(entries, action);
  }
  return trace;
};

const withDeadline = async (promise, label, timeoutMs) => {
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

const parseResult = (command, label) => {
  const combined = `${command.stdout ?? ""}\n${command.stderr ?? ""}`;
  if (command.code !== 0) {
    throw new Error(`${label} exited ${command.code}\n${combined}`);
  }
  if (/\bEIO\b|input\/output error/i.test(combined)) {
    throw new Error(`${label} hid an I/O failure\n${combined}`);
  }
  const line = String(command.stdout ?? "")
    .trim()
    .split("\n")
    .filter(Boolean)
    .at(-1);
  if (!line) throw new Error(`${label} returned no JSON`);
  const parsed = JSON.parse(line);
  if (parsed.ok !== true) throw new Error(`${label} failed: ${line}`);
  return parsed;
};

const commandFor = (engine, root, option, value) =>
  `python3 ${shellQuote(engine)} --root ${shellQuote(root)} --${option}${
    value === undefined ? "" : ` ${shellQuote(value)}`
  }`;

const normalizeExec = async (execGuest, session, command, timeoutSecs) => {
  const result = await execGuest(session, command, timeoutSecs);
  return {
    code: result.code === undefined ? 0 : (result.code ?? 125),
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
  };
};

const runMode = async ({
  name,
  sessions,
  execGuest,
  mountPath,
  seed,
  steps,
  operationTimeoutMs,
  suiteTimeoutMs,
}) => {
  const started = Date.now();
  const id = randomUUID().replaceAll("-", "").slice(0, 16);
  const engine = `/tmp/posix-model-torture-${id}.py`;
  const referenceRoot = `/tmp/posix-model-reference-${id}`;
  const mountedRoot = `${mountPath.replace(/\/+$/, "")}/.posix-model-${id}`;
  const trace = buildPosixTrace({ seed, steps });
  const assertions = [];
  const cleanupErrors = [];
  let failure;

  const execute = (session, command, label) =>
    withDeadline(
      normalizeExec(
        execGuest,
        session,
        command,
        Math.ceil(operationTimeoutMs / 1000),
      ),
      label,
      operationTimeoutMs,
    );
  const executeChecked = async (session, command, label) => {
    const result = await execute(session, command, label);
    const combined = `${result.stdout}\n${result.stderr}`;
    if (result.code !== 0) {
      throw new Error(`${label} exited ${result.code}\n${combined}`);
    }
    if (/\bEIO\b|input\/output error/i.test(combined)) {
      throw new Error(`${label} hid an I/O failure\n${combined}`);
    }
    return result;
  };
  const apply = async (session, root, action, label) =>
    parseResult(
      await execute(
        session,
        commandFor(engine, root, "action", JSON.stringify(action)),
        label,
      ),
      label,
    );
  const takeSnapshot = async (session, root, label) =>
    parseResult(
      await execute(session, commandFor(engine, root, "snapshot"), label),
      label,
    ).snapshot;

  try {
    const encoded = engineSource.toString("base64");
    await Promise.all(
      sessions.map((session, index) =>
        executeChecked(
          session,
          `printf %s ${shellQuote(encoded)} | base64 -d >${shellQuote(engine)} && chmod 700 ${shellQuote(engine)}`,
          `install action engine on client ${index}`,
        ),
      ),
    );
    await executeChecked(
      sessions[0],
      `rm -rf ${shellQuote(referenceRoot)} ${shellQuote(mountedRoot)}
mkdir -p ${shellQuote(referenceRoot)} ${shellQuote(mountedRoot)}
python3 - <<'PY'
import os
for path in (${JSON.stringify(referenceRoot)}, ${JSON.stringify(mountedRoot)}):
    descriptor = os.open(os.path.dirname(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
PY`,
      `${name} initialize roots`,
    );

    for (let index = 0; index < trace.length; index += 1) {
      if (Date.now() - started > suiteTimeoutMs) {
        throw new Error(`${name} suite timed out after ${suiteTimeoutMs}ms`);
      }
      const action = trace[index];
      const actorIndex = index % sessions.length;
      const actor = sessions[actorIndex];
      const observerIndex =
        sessions.length === 1 ? actorIndex : (actorIndex + 1) % sessions.length;
      const observer = sessions[observerIndex];
      const assertionStarted = Date.now();
      try {
        const referenceResult = await apply(
          sessions[0],
          referenceRoot,
          action,
          `${name} action ${index} reference`,
        );
        const mountedResult = await apply(
          actor,
          mountedRoot,
          action,
          `${name} action ${index} mounted client ${actorIndex}`,
        );
        if (!deepEqual(referenceResult.result, mountedResult.result)) {
          throw new Error(
            `operation result diverged: reference=${stable(referenceResult.result)} mounted=${stable(mountedResult.result)}`,
          );
        }
        const referenceSnapshot = await takeSnapshot(
          sessions[0],
          referenceRoot,
          `${name} action ${index} reference snapshot`,
        );
        const actorSnapshot = await takeSnapshot(
          actor,
          mountedRoot,
          `${name} action ${index} actor snapshot`,
        );
        if (!deepEqual(referenceSnapshot, actorSnapshot)) {
          throw new Error(
            `actor snapshot diverged\nreference=${JSON.stringify(referenceSnapshot)}\nmounted=${JSON.stringify(actorSnapshot)}`,
          );
        }
        if (observer !== actor) {
          const observerSnapshot = await takeSnapshot(
            observer,
            mountedRoot,
            `${name} action ${index} observer snapshot`,
          );
          if (!deepEqual(referenceSnapshot, observerSnapshot)) {
            throw new Error(
              `cross-client snapshot diverged\nreference=${JSON.stringify(referenceSnapshot)}\nobserver=${JSON.stringify(observerSnapshot)}`,
            );
          }
        }
        assertions.push({
          index,
          actor: actorIndex,
          observer: observerIndex,
          operation: action,
          status: "pass",
          durationMs: Date.now() - assertionStarted,
        });
      } catch (error) {
        failure = {
          index,
          actor: actorIndex,
          observer: observerIndex,
          operation: action,
          message:
            error instanceof Error
              ? error.stack || error.message
              : String(error),
        };
        assertions.push({
          ...failure,
          status: "fail",
          durationMs: Date.now() - assertionStarted,
        });
        break;
      }
    }
  } catch (error) {
    failure ??= {
      index: assertions.length,
      operation: null,
      message:
        error instanceof Error ? error.stack || error.message : String(error),
    };
  } finally {
    const cleanupResults = await Promise.allSettled(
      sessions.map((session, index) =>
        executeChecked(
          session,
          `rm -rf ${shellQuote(engine)}${
            index === 0
              ? ` ${shellQuote(referenceRoot)} ${shellQuote(mountedRoot)}`
              : ""
          }`,
          `${name} cleanup client ${index}`,
        ),
      ),
    );
    for (const [index, result] of cleanupResults.entries()) {
      if (result.status === "rejected") {
        cleanupErrors.push({
          client: index,
          message:
            result.reason instanceof Error
              ? result.reason.stack || result.reason.message
              : String(result.reason),
        });
      }
    }
    if (cleanupErrors.length > 0) {
      failure ??= {
        index: assertions.length,
        operation: null,
        phase: "cleanup",
        message: `cleanup failed on ${cleanupErrors.length} client(s)`,
      };
    }
  }

  return {
    name,
    status: failure ? "fail" : "pass",
    seed,
    randomSteps: steps,
    totalActions: trace.length,
    completedActions: assertions.filter(
      (assertion) => assertion.status === "pass",
    ).length,
    durationMs: Date.now() - started,
    failure,
    cleanupErrors,
    assertions,
  };
};

export const runPosixModelTorture = async ({
  sessions,
  execGuest,
  mountPath = "/workspace",
  seed = `posix-${Date.now()}`,
  oneClientSteps = 64,
  twoClientSteps = 96,
  operationTimeoutMs = 30_000,
  suiteTimeoutMs = 20 * 60_000,
}) => {
  if (!Array.isArray(sessions) || sessions.length < 2) {
    throw new Error(
      "runPosixModelTorture requires two simultaneously mounted sessions",
    );
  }
  if (typeof execGuest !== "function") {
    throw new Error(
      "runPosixModelTorture requires an execGuest(session, command, timeoutSecs) adapter",
    );
  }
  const oneClient = await runMode({
    name: "one-client-posix-model",
    sessions: [sessions[0]],
    execGuest,
    mountPath,
    seed: `${seed}:one`,
    steps: oneClientSteps,
    operationTimeoutMs,
    suiteTimeoutMs,
  });
  const twoClient = await runMode({
    name: "two-client-posix-model",
    sessions: sessions.slice(0, 2),
    execGuest,
    mountPath,
    seed: `${seed}:two`,
    steps: twoClientSteps,
    operationTimeoutMs,
    suiteTimeoutMs,
  });
  return {
    status:
      oneClient.status === "pass" && twoClient.status === "pass"
        ? "pass"
        : "fail",
    seed,
    generatedAt: new Date().toISOString(),
    modes: [oneClient, twoClient],
  };
};

const runLocalCommand = (command, timeoutSecs) =>
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

if (process.argv.includes("--self-test")) {
  const root = await mkdtemp(join(tmpdir(), "posix-model-self-test-"));
  const seedIndex = process.argv.indexOf("--seed");
  const seed = seedIndex >= 0 ? process.argv[seedIndex + 1] : "local-self-test";
  const stepsIndex = process.argv.indexOf("--steps");
  const steps = stepsIndex >= 0 ? Number(process.argv[stepsIndex + 1]) : 32;
  try {
    const result = await runPosixModelTorture({
      sessions: [{ name: "local-a" }, { name: "local-b" }],
      execGuest: (_session, command, timeoutSecs) =>
        runLocalCommand(command, timeoutSecs),
      mountPath: join(root, "mounted"),
      seed,
      oneClientSteps: steps,
      twoClientSteps: steps,
      operationTimeoutMs: 10_000,
      suiteTimeoutMs: 5 * 60_000,
    });
    process.stdout.write(`${JSON.stringify(result)}\n`);
    process.exitCode = result.status === "pass" ? 0 : 1;
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}
