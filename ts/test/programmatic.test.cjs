const { test } = require("node:test");
const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const { once } = require("node:events");
const { mkdtemp, readFile, realpath, rm } = require("node:fs/promises");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const {
  executeProgrammatic,
  programmaticToolDescription,
} = require("../programmatic.js");

const barrier = () => {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
};
const tools = [
  {
    name: "read",
    description: "Read allowed data",
    schema: { type: "object" },
  },
];

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "chevalier-program-"));
  const children = new Set();
  t.after(async () => {
    for (const child of children) {
      const closed = once(child, "close");
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch {}
      await closed;
    }
    await rm(root, { recursive: true, force: true });
  });
  const sandbox = {
    startExec: async ({ command }) => {
      const child = spawn("/bin/sh", ["-c", command], {
        cwd: root,
        detached: true,
        stdio: ["pipe", "pipe", "pipe"],
      });
      children.add(child);
      const queue = [];
      let waiter;
      let ended = false;
      const push = (event) => {
        if (waiter) {
          const resolve = waiter;
          waiter = undefined;
          resolve(event);
        } else queue.push(event);
      };
      child.stdout.setEncoding("utf8");
      child.stderr.setEncoding("utf8");
      child.stdout.on("data", (text) => push({ type: "stdout", text }));
      child.stderr.on("data", (text) => push({ type: "stderr", text }));
      child.on("close", (code) => {
        children.delete(child);
        ended = true;
        push({ type: "exit", code: code ?? 128 });
      });
      return {
        write: (data) =>
          new Promise((resolve, reject) =>
            child.stdin.write(data, (error) =>
              error ? reject(error) : resolve(),
            ),
          ),
        eof: async () => {
          child.stdin.end();
        },
        signal: async (signal) => {
          try {
            process.kill(-child.pid, signal);
          } catch (error) {
            if (error.code !== "ESRCH") throw error;
          }
        },
        next: () =>
          queue.length
            ? Promise.resolve(queue.shift())
            : ended
              ? Promise.resolve(null)
              : new Promise((resolve) => {
                  waiter = resolve;
                }),
      };
    },
  };
  return { root, children, sandbox };
}

test("requires explicit sandbox injection without provisioning or fallback", async () => {
  await assert.rejects(
    executeProgrammatic("text(1)", { tools, dispatch: async () => null }),
    /only be enabled with a sandbox/,
  );
});

test("uses selected execution environment and reaps the process after success", async (t) => {
  const { sandbox, root, children } = await fixture(t);
  const gracefulSandbox = {
    startExec: async (options) => {
      const session = await sandbox.startExec(options);
      return { ...session, signal: async () => { throw new Error("Successful execution must exit without signals"); } };
    },
  };
  const result = await executeProgrammatic(
    `
    const fs = await import('node:fs/promises');
    await fs.writeFile('proof.txt', 'selected sandbox');
    text(process.cwd());
    setInterval(() => {}, 1000);
  `,
    { sandbox: gracefulSandbox, tools, dispatch: async () => null },
  );
  assert.deepEqual(result.output, [await realpath(root)]);
  assert.equal(
    await readFile(join(root, "proof.txt"), "utf8"),
    "selected sandbox",
  );
  assert.equal(children.size, 0);
});

test("runs concurrent tool calls and emits only selected output", async (t) => {
  const { sandbox, children } = await fixture(t);
  const started = barrier();
  let active = 0;
  const result = await executeProgrammatic(
    `
    const results = await Promise.all([tools.read({id: 1}), tools.read({id: 2})]);
    text(results.map(result => result.id));
  `,
    {
      sandbox,
      tools,
      dispatch: async (_name, args, { signal }) => {
        assert.equal(signal.aborted, false);
        if (++active === 2) started.resolve();
        await started.promise;
        return { ...args, intermediate: "not emitted" };
      },
    },
  );
  assert.deepEqual(result, { output: [[1, 2]] });
  assert.equal(children.size, 0);
});

test("enabled registry only, no recursion, dispatcher permission denial preserved", async (t) => {
  const { sandbox } = await fixture(t);
  const enabled = [
    ...tools,
    { name: "execute_code", description: "RECURSION", schema: {} },
  ];
  assert.ok(!programmaticToolDescription(enabled).includes("RECURSION"));
  const result = await executeProgrammatic(
    `
    text(Object.keys(tools));
    text(typeof tools.disabled);
    try { await tools.read({}); } catch (error) { text(error.message); }
  `,
    {
      sandbox,
      tools: enabled,
      dispatch: async () => {
        throw new Error("Guardian denied");
      },
    },
  );
  assert.deepEqual(result.output.slice(0, 2), [["read"], "undefined"]);
  assert.match(result.output[2], /Guardian denied/);
});

for (const mode of ["cancel", "unawaited", "throw", "timeout"]) {
  test(`waits for child dispatch teardown on ${mode}`, async (t) => {
    const { sandbox, children } = await fixture(t);
    const started = barrier();
    const aborted = barrier();
    const cleanup = barrier();
    const controller = new AbortController();
    let active = false;
    let settled = false;
    const code =
      mode === "unawaited"
        ? "tools.read({});"
        : mode === "throw"
          ? "tools.read({}); throw new Error('broken');"
          : "await tools.read({});";
    const task = executeProgrammatic(code, {
      sandbox,
      tools,
      signal: controller.signal,
      timeoutMs: mode === "timeout" ? 500 : 3000,
      dispatch: async (_name, _args, { signal }) => {
        active = true;
        started.resolve();
        await new Promise((resolve) =>
          signal.addEventListener(
            "abort",
            () => {
              aborted.resolve();
              resolve();
            },
            { once: true },
          ),
        );
        await cleanup.promise;
        active = false;
        throw new Error("child cancelled");
      },
    });
    const outcome = task.then(
      () => {
        settled = true;
      },
      (error) => {
        settled = true;
        return error;
      },
    );
    await started.promise;
    if (mode === "cancel") controller.abort();
    await aborted.promise;
    assert.equal(active, true);
    assert.equal(settled, false);
    cleanup.resolve();
    assert.ok((await outcome) instanceof Error);
    assert.equal(active, false);
    assert.equal(children.size, 0);
  });
}

test("cancels CPU-bound code without blocking host and escalates ignored TERM", async (t) => {
  const { sandbox, children } = await fixture(t);
  const ready = barrier();
  const controller = new AbortController();
  const task = executeProgrammatic(
    `process.on('SIGTERM', () => {}); await tools.read({}); while (true) {}`,
    {
      sandbox,
      tools,
      signal: controller.signal,
      dispatch: async () => {
        ready.resolve();
        return null;
      },
    },
  );
  const failed = assert.rejects(task, /cancelled/);
  await ready.promise;
  controller.abort();
  await failed;
  assert.equal(children.size, 0);
});

test("timeout bounds unresolved JavaScript waits", async (t) => {
  const { sandbox, children } = await fixture(t);
  await assert.rejects(
    executeProgrammatic("await new Promise(() => {});", {
      sandbox,
      tools,
      timeoutMs: 200,
      dispatch: async () => null,
    }),
    /timed out/,
  );
  assert.equal(children.size, 0);
});

test("native binding retains and cleans a sandbox session created after cancellation", async (t) => {
  const { sandbox, children } = await fixture(t);
  const starting = barrier();
  const aborted = barrier();
  const release = barrier();
  const controller = new AbortController();
  let settled = false;
  const task = executeProgrammatic("text('must not run');", {
    tools,
    signal: controller.signal,
    dispatch: async () => null,
    sandbox: { startExec: async (options) => {
      options.signal.addEventListener("abort", aborted.resolve, { once: true });
      starting.resolve();
      await release.promise;
      return sandbox.startExec(options);
    } },
  });
  const outcome = task.then(() => { settled = true; }, (error) => { settled = true; return error; });
  await starting.promise;
  controller.abort();
  await aborted.promise;
  assert.equal(settled, false);
  release.resolve();
  assert.match((await outcome).message, /cancelled/);
  assert.equal(children.size, 0);
});

test("script failure and output bounds leave no process running", async (t) => {
  const { sandbox, children } = await fixture(t);
  for (const code of [
    'throw new Error("broken");',
    'text("x".repeat(1048577));',
    "this is invalid JS",
  ]) {
    await assert.rejects(
      executeProgrammatic(code, { sandbox, tools, dispatch: async () => null }),
    );
    assert.equal(children.size, 0);
  }
});
