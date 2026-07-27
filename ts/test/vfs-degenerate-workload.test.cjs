"use strict";

/**
 * Degenerate-workload gates for the mounted VFS gateway.
 *
 * These model the shape that broke a real sandbox rather than the shape that is
 * convenient to write: a namespace mutating CONTINUOUSLY while an unrelated
 * read runs, which is the steady state of any package install. Every existing
 * unit test resolved hard-link aliases on a QUIET namespace, and quiet is the
 * case that never happens in production — so the suite stayed green while
 * `pnpm install` in the guest took 30 seconds per unlink.
 *
 * Observed in production (vmd logs, 2026-07-26):
 *   vfs read failed: 409 Conflict namespace changed during recursive snapshot
 *     url=.../hard-link-alias/v1  transient=true  retry_timeout_ms=30000
 *   vfs fuse operation ... operation="unlink" operation_time_ms=30723
 *
 * Two candidate mechanisms were measured, and BOTH are gated below rather than
 * guessing between them:
 *   (A) an in-flight alias read blocks concurrent writes for the whole owner;
 *   (B) a write stalls on publication-ack accounting even with no watchers.
 * Whichever is real fails here; the other staying green identifies it.
 *
 * Every test carries a hard deadline. An earlier version of this file deadlocked
 * and hung the suite forever, which is strictly worse than no test: it looks
 * like coverage and asserts nothing.
 */

const test = require("node:test");
const assert = require("node:assert");

const { createVfsGatewayServer } = require("../index.js");

const OWNER = "degenerate-owner";

/** Reject rather than hang. A wedged gate must surface as a fast red. */
const withDeadline = (promise, ms, label) =>
  Promise.race([
    promise,
    new Promise((_, reject) => {
      const timer = setTimeout(() => reject(new Error(`DEADLINE ${label} (${ms}ms)`)), ms);
      if (typeof timer.unref === "function") timer.unref();
    }),
  ]);

const aliasRequest = (handler) =>
  handler(
    new Request(`http://local/internal/chevalier/vfs/${OWNER}/hard-link-alias/v1`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ file_id: "file-1", excluding_path: "pkg/node_modules/.bin/thing" }),
    }),
  );

const writeRequest = (handler, path) =>
  handler(
    new Request(
      `http://local/internal/chevalier/vfs/${OWNER}/file?path=${encodeURIComponent(path)}`,
      { method: "PUT", body: "noise" },
    ),
  );

/**
 * A store whose alias lookup can be parked on a gate, so a test can hold a read
 * in flight and observe what happens to concurrent writes — deterministically,
 * with no sleeps racing the assertion.
 */
const gatedStore = () => {
  const state = { aliasCalls: 0, parkOn: null, gate: null, releaseGate: null, onAliasEnter: null };
  state.gate = new Promise((resolve) => {
    state.releaseGate = resolve;
  });
  const store = {
    // Models the real store's contract: `stat` carries fileId and linkCount
    // (vfs/src/local.rs populates both). An earlier fake omitted them, which
    // would have let an answer-validating fix look broken when it was correct.
    async stat(path) {
      return {
        path,
        kind: "File",
        sizeBytes: 1n,
        contentHash: "a".repeat(64),
        fileId: "file-1",
        linkCount: 2n,
      };
    },
    async read() {
      return Buffer.from("x");
    },
    async write(path) {
      return { path, contentHash: "b".repeat(64), previousHash: null, changed: true };
    },
    async writeMany(writes) {
      return writes.map((write) => ({
        path: write.path,
        contentHash: "b".repeat(64),
        previousHash: null,
        changed: true,
      }));
    },
    async applyNamespaceBatch() {},
    async createSymlink() {},
    async createHardLink(source, destination) {
      const metadata = (path) => ({
        path,
        kind: "File",
        sizeBytes: 1n,
        contentHash: "a".repeat(64),
        fileId: "file-1",
        linkCount: 2n,
      });
      return { source: metadata(source), destination: metadata(destination) };
    },
    async findHardLinkAlias() {
      state.aliasCalls += 1;
      if (state.parkOn === state.aliasCalls) await state.gate;
      if (state.onAliasEnter) await state.onAliasEnter(state.aliasCalls);
      return "pkg/dist/index.js";
    },
    async list() {
      return [];
    },
    async remove() {},
    async createDirectory() {},
  };
  return { store, state };
};

/**
 * MECHANISM (A). A read that is merely *validated* by epoch checkpoints must not
 * exclude writers for its duration — that is the entire premise of using
 * `optimisticRead` on the unlink path instead of the blocking `read`. If a
 * parked alias lookup holds off unrelated writes, then every hard-link unlink
 * serializes the whole workspace's write traffic behind it.
 */
test("an in-flight alias read does not block concurrent writes", async () => {
  const { store, state } = gatedStore();
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  state.parkOn = 1;

  const aliasInFlight = aliasRequest(handler);
  // Let the request reach the parked read before probing the write path.
  await new Promise((resolve) => setTimeout(resolve, 50));

  let writeError = null;
  try {
    const response = await withDeadline(writeRequest(handler, "unrelated/churn.txt"), 1_000, "write");
    assert.strictEqual(response.status, 200);
  } catch (error) {
    writeError = error;
  } finally {
    state.releaseGate();
    await withDeadline(aliasInFlight, 5_000, "alias drain").catch(() => undefined);
  }

  assert.strictEqual(
    writeError,
    null,
    "a write was held off while an alias read was in flight; every hard-link unlink " +
      "then serializes the workspace's writes behind it",
  );
});

/**
 * MECHANISM (B). With no watchers registered there is nothing to wait for, so a
 * write must publish immediately. If this is red instead of (A), the stall is
 * ack accounting rather than reader/writer exclusion.
 */
test("a write with no watchers publishes without waiting on ack accounting", async () => {
  const { store } = gatedStore();
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const started = Date.now();
  const response = await withDeadline(writeRequest(handler, "solo/write.txt"), 1_000, "solo write");
  const elapsed = Date.now() - started;

  assert.strictEqual(response.status, 200);
  assert.ok(elapsed < 250, `an uncontended write took ${elapsed}ms with no watchers registered`);
});

/**
 * The product-level property, independent of which mechanism above is at fault:
 * alias resolution has to succeed on a workspace that is busy. Writes to
 * unrelated paths say nothing about whether THIS file's aliases moved, so they
 * must not invalidate the answer into a 409 the caller can only retry.
 */
test("alias resolution succeeds while unrelated paths mutate throughout", async () => {
  const { store, state } = gatedStore();
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  // Churn must land INSIDE the read window to be meaningful: `optimisticRead`
  // only rejects when the activity epoch moves between its two checkpoints, and
  // the read phase is the only window where that can happen. Driving the write
  // from the read callback puts it there deterministically, instead of hoping a
  // background loop interleaves inside a sub-millisecond read.
  state.onAliasEnter = async (attempt) => {
    await withDeadline(
      writeRequest(handler, `unrelated/churn-${attempt}.txt`),
      2_000,
      `churn write ${attempt}`,
    );
  };

  const response = await withDeadline(aliasRequest(handler), 5_000, "alias under churn");

  assert.strictEqual(
    response.status,
    200,
    `alias resolution returned ${response.status} on a busy namespace; vmd treats that as ` +
      "transient and retries for 30s, which is the observed per-unlink stall",
  );
  assert.strictEqual((await response.json()).path, "pkg/dist/index.js");
});

/**
 * Control. Kept so a fix for the gates above cannot "pass" by disabling snapshot
 * validation altogether — the quiet case must still resolve correctly.
 */
test("alias resolution still succeeds on a quiet namespace", async () => {
  const { store } = gatedStore();
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const response = await withDeadline(aliasRequest(handler), 2_000, "quiet alias");
  assert.strictEqual(response.status, 200);
  assert.strictEqual((await response.json()).path, "pkg/dist/index.js");
});
