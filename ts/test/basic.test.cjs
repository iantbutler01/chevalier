// Offline regression tests (no LLM needed). Live model tests are separate and
// gated on CHEVALIER_TEST_MODEL.
const { test } = require("node:test");
const assert = require("node:assert");
const os = require("node:os");
const path = require("node:path");
const fs = require("node:fs");
const { createHash } = require("node:crypto");
const {
  Runtime,
  agentic,
  createVfsGatewayServer,
  McpClient,
  McpServer,
  VfsStorage,
  version,
} = require("../index.js");

test("version()", () => {
  assert.match(version(), /^\d+\.\d+\.\d+/);
});

test("tool handler round-trip + schema introspection", async () => {
  const rt = new Runtime();
  let seen;
  await rt.tool({
    name: "add",
    description: "Add two numbers",
    schema: { type: "object", properties: { a: { type: "number" }, b: { type: "number" } }, required: ["a", "b"] },
    handler: async ({ a, b }) => {
      seen = { a, b };
      return String(a + b);
    },
  });
  assert.strictEqual(await rt.executeToolCall("add", { a: 2, b: 3 }), "5");
  assert.deepStrictEqual(seen, { a: 2, b: 3 });
  const schemas = await rt.getToolSchemas();
  assert.ok(schemas.some((s) => s.name === "add"));
});

test("vfs local round-trip", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "chev-test-"));
  const vfs = VfsStorage.local(root);
  await vfs.write("a.txt", Buffer.from("hi chevalier"));
  assert.strictEqual((await vfs.read("a.txt")).toString(), "hi chevalier");
  const st = await vfs.stat("a.txt");
  assert.strictEqual(st.kind, "File");
  assert.match(st.contentHash, /^[a-f0-9]{64}$/);
  const attrs = await vfs.stat("a.txt", { maxHashBytes: 0 });
  assert.strictEqual(attrs.kind, "File");
  assert.strictEqual(attrs.contentHash, undefined);
});

test("vfs gateway advertises its implicit mutation lease mode", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "chev-implicit-lease-test-"));
  try {
    const handler = createVfsGatewayServer({
      resolveStore: async () => VfsStorage.local(root),
    });
    const response = await handler(
      new Request("http://local/internal/chevalier/vfs/lease-owner/lease", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ path: "workspace" }),
      }),
    );

    assert.strictEqual(response.status, 200);
    assert.strictEqual(response.headers.get("x-chevalier-vfs-lease-mode"), "implicit");
    const grant = await response.json();
    assert.strictEqual(grant.resource_key, "rk:lease-owner:workspace");
    assert.match(grant.owner_token, /^[0-9a-f-]{36}$/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test(
  "vfs local preserves exact POSIX modes across writes, mkdir, and namespace updates",
  { skip: process.platform === "win32" },
  async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "chev-mode-test-"));
    const vfs = VfsStorage.local(root);

    await vfs.write("script.sh", Buffer.from("#!/bin/sh\n"), {
      // Exact mode wins over a contradictory rolling-upgrade compatibility bit.
      executable: false,
      mode: 0o751,
    });
    let metadata = await vfs.stat("script.sh");
    assert.strictEqual(metadata.mode, 0o751);
    assert.strictEqual(metadata.executable, true);
    assert.strictEqual(fs.statSync(path.join(root, "script.sh")).mode & 0o7777, 0o751);

    await vfs.write("script.sh", Buffer.from("#!/bin/sh\n"), { mode: 0o640 });
    metadata = await vfs.stat("script.sh");
    assert.strictEqual(metadata.mode, 0o640);
    assert.strictEqual(metadata.executable, false);

    await vfs.mkdir("private", { mode: 0o750 });
    assert.strictEqual((await vfs.stat("private")).mode, 0o750);
    await vfs.applyNamespaceBatch([
      { kind: "set_mode", path: "private", mode: 0o700 },
    ]);
    assert.strictEqual((await vfs.stat("private")).mode, 0o700);

    await assert.rejects(
      vfs.write("invalid", Buffer.from("x"), { mode: 0o10000 }),
      /mode must be an integer between 0 and 4095/,
    );
    await assert.rejects(
      vfs.mkdir("invalid-dir", { mode: -1 }),
      /mode must be an integer between 0 and 4095/,
    );
    await assert.rejects(
      vfs.applyNamespaceBatch([{ kind: "set_mode", path: "private", mode: 4096 }]),
      /mode must be an integer between 0 and 4095/,
    );
  },
);

test("vfs local forwards expected file identity preconditions through N-API writes", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "chev-file-id-precondition-test-"));
  const vfs = VfsStorage.local(root);
  await vfs.write("guarded.txt", Buffer.from("initial"));

  let expectedFileId = (await vfs.stat("guarded.txt")).fileId;
  assert.strictEqual(typeof expectedFileId, "string");
  await vfs.write("guarded.txt", Buffer.from("direct"), { expectedFileId });
  await assert.rejects(
    vfs.write("guarded.txt", Buffer.from("stale"), { expectedFileId: "inode:stale" }),
    /identity precondition failed/,
  );

  expectedFileId = (await vfs.stat("guarded.txt")).fileId;
  const stagedPath = path.join(root, "staged-payload");
  const stagedBody = Buffer.from("streamed");
  fs.writeFileSync(stagedPath, stagedBody);
  await vfs.writeFromFile(
    "guarded.txt",
    stagedPath,
    createHash("sha256").update(stagedBody).digest("hex"),
    { expectedFileId },
  );

  expectedFileId = (await vfs.stat("guarded.txt")).fileId;
  await vfs.writeMany([
    {
      path: "guarded.txt",
      body: [...Buffer.from("batch")],
      precondition: { expected_file_id: expectedFileId },
    },
  ]);
  await assert.rejects(
    vfs.writeMany([
      {
        path: "guarded.txt",
        body: [...Buffer.from("stale-batch")],
        precondition: { expected_file_id: "inode:stale" },
      },
    ]),
    /identity precondition failed/,
  );
  await assert.rejects(
    vfs.write("guarded.txt", Buffer.from("invalid"), { expectedFileId: "" }),
    /expectedFileId must be a non-empty string/,
  );
});

test("vfs local metadata exposes stable hard-link identity", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "chev-hardlink-metadata-test-"));
  fs.writeFileSync(path.join(root, "original"), "shared");
  fs.linkSync(path.join(root, "original"), path.join(root, "alias"));
  const vfs = VfsStorage.local(root);

  const original = await vfs.stat("original");
  const alias = await vfs.stat("alias");

  assert.ok(original.fileId);
  assert.strictEqual(alias.fileId, original.fileId);
  assert.strictEqual(original.linkCount, 2n);
  assert.strictEqual(alias.linkCount, 2n);
});

test("vfs bulk metadata preserves request order and missing entries", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "chev-metadata-many-test-"));
  const vfs = VfsStorage.local(root);
  await vfs.write("a.txt", Buffer.from("a"));
  await vfs.write("b.txt", Buffer.from("bb"));

  const rows = await vfs.metadataMany(["b.txt", "missing.txt", "a.txt"]);

  assert.strictEqual(rows[0].path, "b.txt");
  assert.strictEqual(rows[0].sizeBytes, 2n);
  assert.strictEqual(rows[1], null);
  assert.strictEqual(rows[2].path, "a.txt");
});

test("vfs gateway publishes one revision-fenced subtree metadata snapshot", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "chev-subtree-gateway-test-"));
  const store = VfsStorage.local(root);
  await store.mkdir("repo");
  await store.mkdir("repo/nested");
  await store.write("repo/a.txt", Buffer.from("a"));
  await store.write("repo/nested/b.txt", Buffer.from("bb"));
  await store.createSymlink("repo/link", "a.txt");
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const response = await handler(
    new Request(
      "http://local/internal/chevalier/vfs/owner/subtree-metadata",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          prefix: "repo",
          include_object_state: false,
          include_token_count: false,
          limit: 4096,
          max_hash_bytes: 0,
        }),
      },
    ),
  );

  assert.strictEqual(response.status, 200);
  assert.match(
    response.headers.get("x-chevalier-vfs-namespace-revision"),
    /^\d+$/,
  );
  const body = await response.json();
  assert.deepStrictEqual(
    body.entries.map((entry) => [entry.path, entry.kind, entry.content_hash]),
    [
      ["repo/a.txt", "file", null],
      ["repo/link", "symlink", null],
      ["repo/nested/b.txt", "file", null],
    ],
  );
});

test("vfs gateway publishes one revision-fenced bounded subtree prefetch", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "chev-prefetch-gateway-test-"));
  const store = VfsStorage.local(root);
  await store.mkdir("repo");
  await store.write("repo/a.txt", Buffer.from("a"));
  await store.write("repo/b.txt", Buffer.from("bb"));
  await store.write("repo/c.txt", Buffer.from("ccc"));
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const response = await handler(
    new Request(
      "http://local/internal/chevalier/vfs/owner/prefetch-subtree",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          prefix: "repo",
          include_small_file_bytes: true,
          max_entries: 2,
          max_pack_bytes: 3,
        }),
      },
    ),
  );

  assert.strictEqual(response.status, 200);
  assert.match(
    response.headers.get("x-chevalier-vfs-namespace-revision"),
    /^\d+$/,
  );
  assert.deepStrictEqual((await response.json()).warmed_file_bytes, [
    { path: "repo/a.txt", body: [...Buffer.from("a")] },
    { path: "repo/b.txt", body: [...Buffer.from("bb")] },
  ]);
});

test("slow subtree prefetch does not block a writer and retries an overlapping snapshot", async () => {
  const entries = new Map([
    ["repo/existing.txt", Buffer.from("existing")],
  ]);
  let firstPrefetchStarted;
  const prefetchStarted = new Promise((resolve) => {
    firstPrefetchStarted = resolve;
  });
  let releaseFirstPrefetch;
  const firstPrefetchMayFinish = new Promise((resolve) => {
    releaseFirstPrefetch = resolve;
  });
  let prefetchCalls = 0;
  const store = {
    async prefetchSubtree() {
      prefetchCalls += 1;
      const snapshot = [...entries].map(([path, body]) => ({ path, body }));
      if (prefetchCalls === 1) {
        firstPrefetchStarted();
        await firstPrefetchMayFinish;
      }
      return snapshot;
    },
    async writeMany(writes) {
      for (const write of writes) entries.set(write.path, Buffer.from(write.body));
      return writes.map((write) => ({
        path: write.path,
        content_hash: `hash:${write.path}`,
        previous_hash: null,
        changed: true,
      }));
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  const prefetch = handler(
    new Request(
      "http://local/internal/chevalier/vfs/owner/prefetch-subtree",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          prefix: "repo",
          include_small_file_bytes: true,
        }),
      },
    ),
  );
  await prefetchStarted;

  const write = await Promise.race([
    handler(
      new Request(
        "http://local/internal/chevalier/vfs/owner/write-many",
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            writes: [{ path: "repo/new.txt", body: [...Buffer.from("new")] }],
          }),
        },
      ),
    ),
    new Promise((_, reject) =>
      setTimeout(
        () => reject(new Error("subtree prefetch blocked an independent writer")),
        1_000,
      ),
    ),
  ]);
  assert.strictEqual(write.status, 200);
  const committedRevision = write.headers.get(
    "x-chevalier-vfs-namespace-revision",
  );
  releaseFirstPrefetch();

  const response = await prefetch;
  assert.strictEqual(response.status, 200);
  assert.strictEqual(prefetchCalls, 2);
  assert.strictEqual(
    response.headers.get("x-chevalier-vfs-namespace-revision"),
    committedRevision,
  );
  assert.deepStrictEqual((await response.json()).warmed_file_bytes, [
    { path: "repo/existing.txt", body: [...Buffer.from("existing")] },
    { path: "repo/new.txt", body: [...Buffer.from("new")] },
  ]);
});

test("vfs gateway coordinates leased advisory locks by stable file identity", async () => {
  const metadata = {
    path: "lock-a",
    kind: "File",
    sizeBytes: 0n,
    fileId: "stable-file-1",
    linkCount: 2n,
  };
  const store = {
    async stat(path) {
      if (path === "lock-a" || path === "lock-alias") return { ...metadata, path };
      if (path === "other") return { ...metadata, path, fileId: "stable-file-2", linkCount: 1n };
      return null;
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  const request = async (body) => {
    const response = await handler(
      new Request("http://local/internal/chevalier/vfs/owner/posix-lock/v1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
    );
    return { status: response.status, body: await response.json() };
  };
  const lock = (mount, owner, kind, start = "0", end = "9223372036854775807", path = "lock-a") =>
    request({
      action: "set",
      path,
      mount_id: mount,
      lock_owner: owner,
      start,
      end,
      kind,
      pid: 1,
    });

  assert.deepStrictEqual((await lock("mount-a", "owner-a", "write")).body.acquired, true);
  assert.deepStrictEqual((await lock("mount-a", "owner-a", "write", "0", "9", "other")).body.acquired, true);
  assert.deepStrictEqual(
    (await lock("mount-b", "owner-b", "write", "0", "9223372036854775807", "lock-alias")).body.acquired,
    false,
  );
  assert.deepStrictEqual((await lock("mount-b", "owner-b", "write", "0", "9")).body.acquired, false);
  assert.deepStrictEqual((await lock("mount-b", "owner-b", "write", "10", "19")).body.acquired, false);

  const released = await request({
    action: "release_owner",
    mount_id: "mount-a",
    lock_owner: "owner-a",
    file_id: "stable-file-1",
  });
  assert.strictEqual(released.status, 200);
  assert.deepStrictEqual((await lock("mount-b", "owner-b", "write", "0", "9", "other")).body.acquired, false);
  assert.deepStrictEqual((await lock("mount-b", "owner-b", "write", "0", "9")).body.acquired, true);
  assert.deepStrictEqual((await lock("mount-c", "owner-c", "write", "10", "19")).body.acquired, true);
  assert.deepStrictEqual((await lock("mount-d", "owner-d", "read", "0", "9")).body.acquired, false);

  assert.deepStrictEqual((await lock("mount-b", "owner-b", "unlock", "0", "9")).body.acquired, true);
  assert.deepStrictEqual((await lock("mount-d", "owner-d", "read", "0", "9")).body.acquired, true);
  assert.deepStrictEqual((await lock("mount-e", "owner-e", "read", "0", "9")).body.acquired, true);
});

test("vfs gateway advisory locks survive handler recreation with shared state", async () => {
  const byOwner = new Map();
  const sharedState = {
    async transact(ownerId, transaction) {
      const outcome = transaction([...(byOwner.get(ownerId) ?? [])]);
      byOwner.set(ownerId, outcome.locks);
      return outcome.result;
    },
  };
  const store = {
    async stat(path) {
      return path === "guard" ? {
        path,
        kind: "File",
        sizeBytes: 0n,
        fileId: "stable-guard",
        linkCount: 1n,
      } : null;
    },
  };
  const request = async (handler, mountId) => {
    const response = await handler(
      new Request("http://local/internal/chevalier/vfs/owner/posix-lock/v1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "set",
          path: "guard",
          mount_id: mountId,
          lock_owner: "process",
          start: "0",
          end: "9223372036854775807",
          kind: "write",
          pid: 1,
        }),
      }),
    );
    return response.json();
  };

  const beforeRestart = createVfsGatewayServer({
    resolveStore: async () => store,
    advisoryLockState: sharedState,
  });
  assert.strictEqual((await request(beforeRestart, "mount-a")).acquired, true);

  const afterRestart = createVfsGatewayServer({
    resolveStore: async () => store,
    advisoryLockState: sharedState,
  });
  assert.strictEqual((await request(afterRestart, "mount-b")).acquired, false);
});

test("vfs gateway streams verified uploads and serves bounded ranges", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "chev-stream-test-"));
  const backing = VfsStorage.local(root);
  const handler = createVfsGatewayServer({ resolveStore: async () => backing });
  const payload = Buffer.alloc(3 * 1024 * 1024 + 29, 0x5a);
  const expected = createHash("sha256").update(payload).digest("hex");
  const response = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=large.bin", {
      method: "PUT",
      headers: {
        "content-length": String(payload.length),
        "x-chevalier-vfs-expected-content-sha256": expected,
        "x-chevalier-vfs-stream-upload": "1",
      },
      body: payload,
    }),
  );
  assert.strictEqual(response.status, 200);
  const publication = await response.json();
  assert.deepStrictEqual(
    publication.entries.map((entry) => [entry.path, entry.metadata?.size_bytes]),
    [["large.bin", payload.length]],
  );
  assert.strictEqual((await backing.stat("large.bin")).contentHash, expected);

  const range = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file/raw?path=large.bin", {
      headers: { range: "bytes=1048576-2097151" },
    }),
  );
  assert.strictEqual(range.status, 206);
  assert.strictEqual((await range.arrayBuffer()).byteLength, 1024 * 1024);
  assert.strictEqual(range.headers.get("content-range"), `bytes 1048576-2097151/${payload.length}`);
});

test("vfs streamed upload rejects torn content and destination CAS races", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "chev-stream-race-test-"));
  const backing = VfsStorage.local(root);
  const original = Buffer.from("original");
  const raced = Buffer.from("raced destination");
  await backing.write("guarded.bin", original);
  const originalHash = createHash("sha256").update(original).digest("hex");
  const payload = Buffer.alloc(2 * 1024 * 1024 + 7, 0x44);
  const payloadHash = createHash("sha256").update(payload).digest("hex");
  let raceOnCommit = false;
  const store = {
    stat: (path) => backing.stat(path),
    writeFromFile: async (path, sourcePath, expectedHash, options) => {
      if (raceOnCommit) {
        raceOnCommit = false;
        await backing.write(path, raced);
      }
      return backing.writeFromFile(path, sourcePath, expectedHash, options);
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  const request = (expectedHash) =>
    handler(
      new Request("http://local/internal/chevalier/vfs/owner/file?path=guarded.bin", {
        method: "PUT",
        headers: {
          "content-length": String(payload.length),
          "x-chevalier-vfs-expected-content-sha256": expectedHash,
          "x-chevalier-vfs-precondition-fingerprint": originalHash,
          "x-chevalier-vfs-stream-upload": "1",
        },
        body: payload,
      }),
    );

  const torn = await request("0".repeat(64));
  assert.strictEqual(torn.status, 409);
  assert.strictEqual((await backing.read("guarded.bin")).toString(), "original");

  raceOnCommit = true;
  const racedResponse = await request(payloadHash);
  assert.strictEqual(racedResponse.status, 409);
  assert.strictEqual((await backing.read("guarded.bin")).toString(), "raced destination");
});

test("vfs gateway forwards conditional writes into the backing store", async () => {
  let currentHash = "old";
  let statCalls = 0;
  let releaseStats;
  const bothStatsReached = new Promise((resolve) => {
    releaseStats = resolve;
  });
  const store = {
    async stat(path) {
      const observedHash = currentHash;
      statCalls += 1;
      if (statCalls <= 2) {
        if (statCalls === 2) releaseStats();
        await bothStatsReached;
      }
      return {
        path,
        kind: "File",
        sizeBytes: BigInt(1),
        contentHash: observedHash,
        updatedAt: null,
      };
    },
    async write(_path, data, options) {
      if (Object.prototype.hasOwnProperty.call(options ?? {}, "ifMatch")) {
        if (options.ifMatch !== currentHash) {
          throw new Error("VFS: [VFS_CONFLICT status=409] conflict: stale write");
        }
      }
      const previousHash = currentHash;
      currentHash = data.toString("utf8");
      return { contentHash: currentHash, previousHash, changed: previousHash !== currentHash };
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  const request = (body) =>
    handler(
      new Request("http://local/internal/chevalier/vfs/owner/file?path=a.txt", {
        method: "PUT",
        headers: { "x-chevalier-vfs-precondition-fingerprint": "old" },
        body,
      }),
    );

  const responses = await Promise.all([request("first"), request("second")]);
  assert.deepStrictEqual(
    responses.map((response) => response.status).sort(),
    [200, 409],
  );
});

test("vfs gateway propagates expected file identity through every write path", async () => {
  const seen = [];
  const directStore = {
    async stat() {
      return null;
    },
    async write(path, data, options) {
      seen.push({ op: "write", path, body: data.toString("utf8"), options });
      return { contentHash: "direct-hash", previousHash: null, changed: true };
    },
    async writeFromFile(path, sourcePath, expectedHash, options) {
      seen.push({
        op: "writeFromFile",
        path,
        body: fs.readFileSync(sourcePath, "utf8"),
        expectedHash,
        options,
      });
      return { contentHash: expectedHash, previousHash: null, changed: true };
    },
    async writeMany(writes) {
      seen.push({ op: "writeMany", writes });
      return writes.map((write) => ({
        path: write.path,
        contentHash: "batch-hash",
        previousHash: null,
        changed: true,
      }));
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => directStore });

  const direct = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=direct.txt", {
      method: "PUT",
      headers: { "x-chevalier-vfs-precondition-file-id": "inode:direct" },
      body: "direct",
    }),
  );
  assert.strictEqual(direct.status, 200);

  const streamedBody = Buffer.from("streamed");
  const streamedHash = createHash("sha256").update(streamedBody).digest("hex");
  const streamed = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=streamed.txt", {
      method: "PUT",
      headers: {
        "content-length": String(streamedBody.length),
        "x-chevalier-vfs-expected-content-sha256": streamedHash,
        "x-chevalier-vfs-precondition-file-id": "inode:streamed",
        "x-chevalier-vfs-stream-upload": "1",
      },
      body: streamedBody,
    }),
  );
  assert.strictEqual(streamed.status, 200);

  const batch = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/write-many", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        writes: [
          {
            path: "batch.txt",
            body: [...Buffer.from("batch")],
            precondition: { expected_file_id: "inode:batch" },
          },
        ],
      }),
    }),
  );
  assert.strictEqual(batch.status, 200);
  assert.deepStrictEqual(seen, [
    {
      op: "write",
      path: "direct.txt",
      body: "direct",
      options: { expectedFileId: "inode:direct" },
    },
    {
      op: "writeFromFile",
      path: "streamed.txt",
      body: "streamed",
      expectedHash: streamedHash,
      options: { expectedFileId: "inode:streamed" },
    },
    {
      op: "writeMany",
      writes: [
        {
          path: "batch.txt",
          body: [...Buffer.from("batch")],
          precondition: { expected_file_id: "inode:batch" },
        },
      ],
    },
  ]);

  const fallbackSeen = [];
  const fallbackHandler = createVfsGatewayServer({
    resolveStore: async () => ({
      async stat() {
        return null;
      },
      async write(path, data, options) {
        fallbackSeen.push({ path, body: data.toString("utf8"), options });
        return { contentHash: "fallback-hash", previousHash: null, changed: true };
      },
    }),
  });
  const fallback = await fallbackHandler(
    new Request("http://local/internal/chevalier/vfs/owner/write-many", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        writes: [
          {
            path: "fallback.txt",
            body: [...Buffer.from("fallback")],
            precondition: { expected_file_id: "inode:fallback" },
          },
        ],
      }),
    }),
  );
  assert.strictEqual(fallback.status, 200);
  assert.deepStrictEqual(fallbackSeen, [
    {
      path: "fallback.txt",
      body: "fallback",
      options: { expectedFileId: "inode:fallback" },
    },
  ]);
});

test("vfs gateway rejects invalid expected file identity preconditions", async () => {
  let writes = 0;
  const handler = createVfsGatewayServer({
    resolveStore: async () => ({
      async write() {
        writes += 1;
      },
      async writeMany() {
        writes += 1;
      },
    }),
  });

  const emptyHeader = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=direct.txt", {
      method: "PUT",
      headers: { "x-chevalier-vfs-precondition-file-id": "" },
      body: "direct",
    }),
  );
  assert.strictEqual(emptyHeader.status, 400);

  const invalidBatch = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/write-many", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        writes: [
          {
            path: "batch.txt",
            body: [1],
            precondition: { expected_file_id: 42 },
          },
        ],
      }),
    }),
  );
  assert.strictEqual(invalidBatch.status, 400);
  assert.strictEqual(writes, 0);
});

test("vfs gateway forwards executable metadata on writes", async () => {
  const seen = [];
  const store = {
    async stat() {
      return null;
    },
    async write(path, data, options) {
      seen.push({ path, body: data.toString("utf8"), options });
      return { contentHash: "hash", previousHash: null, changed: true };
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const response = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=script.sh", {
      method: "PUT",
      headers: { "x-chevalier-vfs-executable": "true" },
      body: "#!/bin/sh\n",
    }),
  );

  assert.strictEqual(response.status, 200);
  assert.deepStrictEqual(seen, [
    { path: "script.sh", body: "#!/bin/sh\n", options: { executable: true } },
  ]);
});

test("vfs gateway forwards exact modes and derives the legacy executable bit", async () => {
  const seen = [];
  const store = {
    async stat() {
      return null;
    },
    async write(path, data, options) {
      seen.push({ op: "write", path, body: data.toString("utf8"), options });
      return { contentHash: "hash", previousHash: null, changed: true };
    },
    async mkdir(path, options) {
      seen.push({ op: "mkdir", path, options });
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const writeResponse = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=script.sh", {
      method: "PUT",
      headers: {
        "x-chevalier-vfs-mode": String(0o4751),
        // Exact mode is authoritative when a rolling-upgrade caller sends both.
        "x-chevalier-vfs-executable": "false",
      },
      body: "#!/bin/sh\n",
    }),
  );
  const mkdirResponse = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/dir?path=private", {
      method: "PUT",
      headers: { "x-chevalier-vfs-mode": String(0o750) },
    }),
  );

  assert.strictEqual(writeResponse.status, 200);
  assert.strictEqual(mkdirResponse.status, 204);
  assert.deepStrictEqual(seen, [
    {
      op: "write",
      path: "script.sh",
      body: "#!/bin/sh\n",
      options: { executable: true, mode: 0o4751 },
    },
    {
      op: "mkdir",
      path: "private",
      options: { executable: true, mode: 0o750 },
    },
  ]);
});

test("vfs gateway rejects malformed or out-of-range exact modes", async () => {
  let writes = 0;
  const store = {
    async stat() {
      return null;
    },
    async write() {
      writes += 1;
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  for (const mode of ["0o751", "-1", "4096", "1.5", "nope"]) {
    const response = await handler(
      new Request("http://local/internal/chevalier/vfs/owner/file?path=script.sh", {
        method: "PUT",
        headers: { "x-chevalier-vfs-mode": mode },
        body: "body",
      }),
    );
    assert.strictEqual(response.status, 400, `mode ${mode} must be rejected`);
  }
  assert.strictEqual(writes, 0);
});

test("vfs gateway emits exact mode metadata with legacy executable fallback", async () => {
  const store = {
    async stat(path) {
      return {
        path,
        kind: "File",
        sizeBytes: 1n,
        mode: 0o751,
        executable: false,
        contentHash: "hash",
      };
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const response = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/stat?path=script.sh"),
  );
  assert.strictEqual(response.status, 200);
  const metadata = await response.json();
  assert.strictEqual(metadata.mode, 0o751);
  assert.strictEqual(metadata.executable, true);
});

test("vfs gateway preserves exact directory modes in namespace batches", async () => {
  const seen = [];
  const store = {
    async applyNamespaceBatch(mutations) {
      seen.push(mutations);
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  const response = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/namespace-many", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        operation_ids: ["mkdir-private", "chmod-private"],
        mutations: [
          { kind: "create_directory", path: "private", mode: 0o2750 },
          { kind: "set_mode", path: "private", mode: 0o750 },
        ],
      }),
    }),
  );

  assert.strictEqual(response.status, 200);
  assert.deepStrictEqual(seen, [
    [
      { kind: "create_directory", path: "private", mode: 0o2750 },
      { kind: "set_mode", path: "private", mode: 0o750 },
    ],
  ]);

  for (const mutation of [
    { kind: "create_directory", path: "invalid", mode: 4096 },
    { kind: "set_mode", path: "private" },
  ]) {
    const invalid = await handler(
      new Request("http://local/internal/chevalier/vfs/owner/namespace-many", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ operation_ids: ["invalid"], mutations: [mutation] }),
      }),
    );
    assert.strictEqual(invalid.status, 400);
  }
  assert.strictEqual(seen.length, 1);
});

test("namespace publication is atomic and a second client observes every mutation at its committed revision", async () => {
  const entries = new Map([
    ["source", {
      path: "source",
      kind: "File",
      sizeBytes: 0n,
      fileId: "inode-source",
      linkCount: 1n,
      mode: 0o644,
    }],
    ["deleted", {
      path: "deleted",
      kind: "File",
      sizeBytes: 0n,
      fileId: "inode-deleted",
      linkCount: 1n,
      mode: 0o644,
    }],
    ["deleted-dir", {
      path: "deleted-dir",
      kind: "Directory",
      sizeBytes: 0n,
      fileId: "inode-deleted-dir",
      linkCount: 2n,
      mode: 0o755,
    }],
    ["old-name", {
      path: "old-name",
      kind: "File",
      sizeBytes: 0n,
      fileId: "inode-renamed",
      linkCount: 1n,
      mode: 0o644,
    }],
  ]);
  let mutationStarted;
  const started = new Promise((resolve) => {
    mutationStarted = resolve;
  });
  let publish;
  const publicationGate = new Promise((resolve) => {
    publish = resolve;
  });
  const store = {
    async stat(path) {
      return entries.get(path) ?? null;
    },
    async listDir(path) {
      assert.strictEqual(path, ".");
      return [...entries.values()];
    },
    async applyNamespaceBatch(mutations) {
      mutationStarted();
      await publicationGate;
      for (const mutation of mutations) {
        switch (mutation.kind) {
          case "create_file":
            entries.set(mutation.path, {
              path: mutation.path,
              kind: "File",
              sizeBytes: 0n,
              fileId: `inode:${mutation.path}`,
              linkCount: 1n,
              mode: mutation.mode ?? 0o644,
            });
            break;
          case "create_directory":
            entries.set(mutation.path, {
              path: mutation.path,
              kind: "Directory",
              sizeBytes: 0n,
              fileId: `inode:${mutation.path}`,
              linkCount: 2n,
              mode: mutation.mode ?? 0o755,
            });
            break;
          case "create_symlink":
            entries.set(mutation.path, {
              path: mutation.path,
              kind: "Symlink",
              sizeBytes: BigInt(mutation.target.length),
              fileId: `inode:${mutation.path}`,
              linkCount: 1n,
              mode: 0o777,
              linkTarget: mutation.target,
            });
            break;
          case "create_hard_link": {
            const source = entries.get(mutation.source_path);
            assert.ok(source);
            const linked = { ...source, linkCount: BigInt(Number(source.linkCount) + 1) };
            entries.set(mutation.source_path, linked);
            entries.set(mutation.destination_path, {
              ...linked,
              path: mutation.destination_path,
            });
            break;
          }
          case "delete_file":
          case "remove_directory":
            entries.delete(mutation.path);
            break;
          case "rename": {
            const source = entries.get(mutation.from);
            assert.ok(source);
            entries.delete(mutation.from);
            entries.set(mutation.to, { ...source, path: mutation.to });
            break;
          }
          case "set_mode": {
            const current = entries.get(mutation.path);
            assert.ok(current);
            entries.set(mutation.path, { ...current, mode: mutation.mode });
            break;
          }
          default:
            assert.fail(`unhandled mutation ${mutation.kind}`);
        }
      }
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  const mutations = [
    { kind: "create_file", path: "created", mode: 0o640 },
    { kind: "create_directory", path: "created-dir", mode: 0o750 },
    { kind: "create_symlink", path: "created-link", target: "source" },
    {
      kind: "create_hard_link",
      source_path: "source",
      destination_path: "source-alias",
    },
    { kind: "rename", from: "old-name", to: "new-name" },
    { kind: "set_mode", path: "new-name", mode: 0o700 },
    { kind: "delete_file", path: "deleted" },
    { kind: "remove_directory", path: "deleted-dir" },
    { kind: "create_file", path: "awaited-empty-probe", mode: 0o600 },
    { kind: "delete_file", path: "awaited-empty-probe" },
  ];
  const mutationRequest = handler(
    new Request("http://origin/internal/chevalier/vfs/owner/namespace-many", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        operation_ids: mutations.map((_, index) => `operation-${index}`),
        mutations,
      }),
    }),
  );
  await started;
  let secondClientReadCompleted = false;
  const secondClientRead = handler(
    new Request("http://second-mount/internal/chevalier/vfs/owner/tree?path="),
  ).then((response) => {
    secondClientReadCompleted = true;
    return response;
  });
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.strictEqual(
    secondClientReadCompleted,
    false,
    "listing cannot publish between the batch and its namespace revision",
  );
  publish();

  const mutationResponse = await mutationRequest;
  const committedRevision = mutationResponse.headers.get(
    "x-chevalier-vfs-namespace-revision",
  );
  assert.strictEqual(mutationResponse.status, 200);
  assert.match(committedRevision, /^\d+$/);
  const mutationSnapshot = await mutationResponse.json();
  const snapshotByPath = new Map(
    mutationSnapshot.entries.map((entry) => [entry.path, entry.metadata]),
  );
  assert.strictEqual(snapshotByPath.get("created")?.file_id, "inode:created");
  assert.strictEqual(snapshotByPath.get("source")?.link_count, 2);
  assert.strictEqual(snapshotByPath.get("new-name")?.mode, 0o700);
  assert.strictEqual(snapshotByPath.get("deleted"), null);
  assert.strictEqual(snapshotByPath.get("awaited-empty-probe"), null);
  const listingResponse = await secondClientRead;
  assert.strictEqual(listingResponse.status, 200);
  assert.strictEqual(
    listingResponse.headers.get("x-chevalier-vfs-namespace-revision"),
    committedRevision,
  );
  const byName = new Map((await listingResponse.json()).map((entry) => [entry.name, entry]));
  assert.strictEqual(byName.get("created")?.mode, 0o640);
  assert.strictEqual(byName.get("created-dir")?.kind, "directory");
  assert.strictEqual(byName.get("created-link")?.link_target, "source");
  assert.strictEqual(byName.get("source")?.link_count, 2);
  assert.strictEqual(byName.get("source-alias")?.file_id, "inode-source");
  assert.strictEqual(byName.get("new-name")?.mode, 0o700);
  assert.strictEqual(byName.has("old-name"), false);
  assert.strictEqual(byName.has("deleted"), false);
  assert.strictEqual(byName.has("deleted-dir"), false);
  assert.strictEqual(
    byName.has("awaited-empty-probe"),
    false,
    "an awaited empty-file create/unlink cannot leak through the second client",
  );
});

test("publication snapshots allow concurrent reads while preserving one stable revision", async () => {
  let activeReads = 0;
  let maxActiveReads = 0;
  let startedReads = 0;
  let releaseReads;
  const readsMayFinish = new Promise((resolve) => {
    releaseReads = resolve;
  });
  let bothReadsStarted;
  const bothReadsAreActive = new Promise((resolve) => {
    bothReadsStarted = resolve;
  });
  const store = {
    async stat(path) {
      activeReads += 1;
      startedReads += 1;
      maxActiveReads = Math.max(maxActiveReads, activeReads);
      if (startedReads === 2) bothReadsStarted();
      await readsMayFinish;
      activeReads -= 1;
      return {
        path,
        kind: "File",
        sizeBytes: 0n,
        fileId: `inode:${path}`,
        linkCount: 1n,
        mode: 0o644,
      };
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  const requests = ["first", "second"].map((path) =>
    handler(
      new Request(
        `http://local/internal/chevalier/vfs/owner/stat?path=${path}`,
      ),
    ),
  );

  await Promise.race([
    bothReadsAreActive,
    new Promise((_, reject) =>
      setTimeout(
        () => reject(new Error("publication coordinator serialized independent reads")),
        1_000,
      ),
    ),
  ]);
  releaseReads();

  const responses = await Promise.all(requests);
  assert.strictEqual(maxActiveReads, 2);
  assert.strictEqual(responses[0].status, 200);
  assert.strictEqual(responses[1].status, 200);
  assert.strictEqual(
    responses[0].headers.get("x-chevalier-vfs-namespace-revision"),
    responses[1].headers.get("x-chevalier-vfs-namespace-revision"),
  );
});

test("content publication advances the same revision observed by metadata reads", async () => {
  const entries = new Map();
  const store = {
    async stat(path) {
      return entries.get(path) ?? null;
    },
    async writeMany(writes) {
      return writes.map((write) => {
        entries.set(write.path, {
          path: write.path,
          kind: "File",
          sizeBytes: BigInt(write.body.length),
          fileId: `inode:${write.path}`,
          linkCount: 1n,
          contentHash: `hash:${write.path}`,
          mode: 0o644,
        });
        return {
          path: write.path,
          content_hash: `hash:${write.path}`,
          previous_hash: null,
          changed: true,
        };
      });
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  const write = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/write-many", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        writes: [{ path: "file.txt", body: [110, 101, 120, 116] }],
      }),
    }),
  );
  assert.strictEqual(write.status, 200);
  const committedRevision = Number(
    write.headers.get("x-chevalier-vfs-namespace-revision"),
  );
  assert.ok(Number.isSafeInteger(committedRevision) && committedRevision > 0);
  const writeSnapshot = await write.json();
  assert.strictEqual(writeSnapshot.entries[0]?.path, "file.txt");
  assert.strictEqual(writeSnapshot.entries[0]?.metadata?.file_id, "inode:file.txt");
  assert.strictEqual(
    writeSnapshot.entries[0]?.metadata?.content_hash,
    "hash:file.txt",
  );

  const stat = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/stat?path=file.txt"),
  );
  assert.strictEqual(stat.status, 200);
  assert.strictEqual(
    Number(stat.headers.get("x-chevalier-vfs-namespace-revision")),
    committedRevision,
  );
  assert.strictEqual((await stat.json()).content_hash, "hash:file.txt");
});

test("vfs gateway reports backing-store listing failures as 500, not an empty-looking 404", async () => {
  const store = {
    async listDir() {
      throw new Error("VFS: [VFS_INTERNAL status=500] disk read failed");
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const response = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/tree?path=.", { method: "GET" }),
  );

  assert.strictEqual(response.status, 500);
  assert.match(await response.text(), /disk read failed/);
});

test("vfs gateway forwards lightweight stat metadata options", async () => {
  const seen = [];
  const store = {
    async stat(path, options) {
      seen.push({ path, options });
      return {
        path,
        kind: "File",
        sizeBytes: 3n,
        contentHash: options?.maxHashBytes === 0 ? undefined : "hash",
      };
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const response = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/stat?path=a.txt&max_hash_bytes=0"),
  );

  assert.strictEqual(response.status, 200);
  assert.strictEqual((await response.json()).content_hash, null);
  assert.deepStrictEqual(seen, [{ path: "a.txt", options: { maxHashBytes: 0 } }]);
});

test("vfs gateway pins ranged reads with an epoch-millis fingerprint", async () => {
  // Vector shared with sandbox/vmd/src/fuse/fs.rs range_fingerprint tests.
  const updatedAt = "2026-07-17T23:26:26.500Z";
  const expectedFingerprint = "123:1784330786500";
  const store = {
    async stat() {
      return { path: "big.bin", kind: "File", sizeBytes: 123n, updatedAt };
    },
    async read() {
      return Buffer.alloc(123, 7);
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const pinned = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file/raw?path=big.bin", {
      headers: {
        range: "bytes=0-9",
        "x-chevalier-vfs-range-fingerprint": expectedFingerprint,
      },
    }),
  );
  assert.strictEqual(pinned.status, 206);
  assert.strictEqual(
    pinned.headers.get("x-chevalier-vfs-range-fingerprint"),
    expectedFingerprint,
  );
  assert.strictEqual((await pinned.arrayBuffer()).byteLength, 10);

  const stale = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file/raw?path=big.bin", {
      headers: {
        range: "bytes=0-9",
        "x-chevalier-vfs-range-fingerprint": "999:0",
      },
    }),
  );
  assert.strictEqual(stale.status, 412);
  assert.strictEqual(
    stale.headers.get("x-chevalier-vfs-range-fingerprint"),
    expectedFingerprint,
  );
});

test("vfs gateway rejects a ranged read when the file changes during the read", async () => {
  let statCalls = 0;
  const store = {
    async stat() {
      statCalls += 1;
      // First stat (pre-read) sees the old file; the post-read bracket stat
      // sees a replacement that landed mid-read.
      return statCalls === 1
        ? { path: "big.bin", kind: "File", sizeBytes: 100n, updatedAt: "2026-07-17T00:00:00Z" }
        : { path: "big.bin", kind: "File", sizeBytes: 80n, updatedAt: "2026-07-17T00:00:01Z" };
    },
    async read() {
      return Buffer.alloc(100, 7);
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const response = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file/raw?path=big.bin", {
      headers: { range: "bytes=0-9" },
    }),
  );
  assert.strictEqual(response.status, 412, "a splice-risk read must never return bytes");
});

test("vfs gateway CAS fingerprints symbolic-link targets", async () => {
  let target = "target.txt";
  const store = {
    async stat(path) {
      return target === null
        ? null
        : { path, kind: "Symlink", sizeBytes: BigInt(target.length), linkTarget: target };
    },
    async remove(_path, options) {
      const current =
        target === null ? null : `symlink:${createHash("sha256").update(target).digest("hex")}`;
      if (options?.ifMatch !== current) throw new Error("VFS: [VFS_CONFLICT status=409] stale link");
      target = null;
      return { removed: true };
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  const expected = `symlink:${createHash("sha256").update("target.txt").digest("hex")}`;

  const stale = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=link.txt", {
      method: "DELETE",
      headers: { "x-chevalier-vfs-precondition-fingerprint": "symlink:stale" },
    }),
  );
  assert.strictEqual(stale.status, 409);
  assert.strictEqual(target, "target.txt");

  const matched = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=link.txt", {
      method: "DELETE",
      headers: { "x-chevalier-vfs-precondition-fingerprint": expected },
    }),
  );
  assert.strictEqual(matched.status, 200);
  assert.strictEqual(target, null);
});

test("vfs gateway aliases If-Match and ifMatch into canonical 409 preconditions", async () => {
  const files = new Map([
    ["a.txt", Buffer.from("old")],
    ["batch.txt", Buffer.from("batch-old")],
  ]);
  const optionsSeen = [];
  const store = {
    async stat(path) {
      const bytes = files.get(path);
      if (bytes === undefined) return null;
      return {
        path,
        kind: "File",
        sizeBytes: BigInt(bytes.length),
        contentHash: bytes.toString("utf8"),
        updatedAt: null,
      };
    },
    async write(path, data, options) {
      optionsSeen.push({ op: "write", path, options });
      if (Object.prototype.hasOwnProperty.call(options ?? {}, "ifMatch")) {
        const currentHash = files.get(path)?.toString("utf8") ?? null;
        if (options.ifMatch !== currentHash) {
          throw new Error("VFS: [VFS_CONFLICT status=409] conflict: stale write");
        }
      }
      const previousHash = files.get(path)?.toString("utf8") ?? null;
      files.set(path, Buffer.from(data));
      return {
        contentHash: data.toString("utf8"),
        previousHash,
        changed: previousHash !== data.toString("utf8"),
      };
    },
    async remove(path, options) {
      optionsSeen.push({ op: "remove", path, options });
      if (Object.prototype.hasOwnProperty.call(options ?? {}, "ifMatch")) {
        const currentHash = files.get(path)?.toString("utf8") ?? null;
        if (options.ifMatch !== currentHash) {
          throw new Error("VFS: [VFS_CONFLICT status=409] conflict: stale delete");
        }
      }
      files.delete(path);
      return { removed: true };
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });

  const matchedHeader = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=a.txt", {
      method: "PUT",
      headers: { "If-Match": 'W/"sha256:old"' },
      body: "new",
    }),
  );
  assert.strictEqual(matchedHeader.status, 200);
  assert.deepStrictEqual(optionsSeen.at(-1), {
    op: "write",
    path: "a.txt",
    options: { ifMatch: "old" },
  });
  assert.strictEqual(files.get("a.txt")?.toString("utf8"), "new");

  const staleHeader = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=a.txt", {
      method: "PUT",
      headers: { "If-Match": "deadbeef" },
      body: "clobber",
    }),
  );
  assert.strictEqual(staleHeader.status, 409);
  assert.strictEqual(files.get("a.txt")?.toString("utf8"), "new");

  const matchedQuery = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/file?path=a.txt&ifMatch=sha256:new", {
      method: "DELETE",
    }),
  );
  assert.strictEqual(matchedQuery.status, 200);
  assert.deepStrictEqual(optionsSeen.at(-1), {
    op: "remove",
    path: "a.txt",
    options: { ifMatch: "new" },
  });
  assert.strictEqual(files.has("a.txt"), false);

  const batchBody = Buffer.from("batch-new");
  const matchedBody = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/write-many", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        writes: [{ path: "batch.txt", body: [...batchBody], ifMatch: '"sha256:batch-old"' }],
      }),
    }),
  );
  assert.strictEqual(matchedBody.status, 200);
  assert.deepStrictEqual(optionsSeen.at(-1), {
    op: "write",
    path: "batch.txt",
    options: { ifMatch: "batch-old" },
  });
  assert.strictEqual(files.get("batch.txt")?.toString("utf8"), "batch-new");

  const createdBody = Buffer.from("batch-created");
  const matchedNullBody = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/write-many", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        writes: [{ path: "created-by-batch.txt", body: [...createdBody], ifMatch: null }],
      }),
    }),
  );
  assert.strictEqual(matchedNullBody.status, 200);
  assert.deepStrictEqual(optionsSeen.at(-1), {
    op: "write",
    path: "created-by-batch.txt",
    options: { ifMatch: null },
  });
  assert.strictEqual(files.get("created-by-batch.txt")?.toString("utf8"), "batch-created");

  const rustWireBody = Buffer.from("rust-wire");
  const omittedPreconditionBody = await handler(
    new Request("http://local/internal/chevalier/vfs/owner/write-many", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        writes: [{ path: "rust-wire-batch.txt", body: [...rustWireBody], precondition: null }],
      }),
    }),
  );
  assert.strictEqual(omittedPreconditionBody.status, 200);
  assert.deepStrictEqual(optionsSeen.at(-1), {
    op: "write",
    path: "rust-wire-batch.txt",
    options: undefined,
  });
  assert.strictEqual(files.get("rust-wire-batch.txt")?.toString("utf8"), "rust-wire");
});

test("vfs gateway forwards conditional namespace batches and maps CAS races to 409", async () => {
  const seen = [];
  let fail = false;
  const store = {
    async applyNamespaceBatch(mutations) {
      seen.push(mutations);
      if (fail) throw new Error("VFS: [VFS_CONFLICT status=409] conflict: stale delete");
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: async () => store });
  const request = () =>
    handler(
      new Request("http://local/internal/chevalier/vfs/owner/namespace-many", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          operation_ids: ["delete-a", "delete-b"],
          mutations: [
            {
              kind: "delete_file",
              path: "a.txt",
              precondition: {
                predicate: { kind: "content_fingerprint", fingerprint: "old" },
                expected_file_id: "inode:a",
              },
            },
            {
              kind: "delete_file",
              path: "b.txt",
              precondition: { predicate: { kind: "absent" } },
            },
          ],
        }),
      }),
    );

  const matched = await request();
  assert.strictEqual(matched.status, 200);
  assert.deepStrictEqual(seen[0], [
    {
      kind: "delete_file",
      path: "a.txt",
      precondition: {
        predicate: { kind: "content_fingerprint", fingerprint: "old" },
        expected_file_id: "inode:a",
      },
    },
    {
      kind: "delete_file",
      path: "b.txt",
      precondition: { predicate: { kind: "absent" } },
    },
  ]);

  fail = true;
  const stale = await request();
  assert.strictEqual(stale.status, 409);
  assert.match(await stale.text(), /precondition failed/);
});

test("mcp server + client end-to-end", async () => {
  const server = new McpServer("test", { version: "0.0.1" });
  await server.tool(
    "echo",
    "echo back",
    { type: "object", properties: { m: { type: "string" } }, required: ["m"] },
    async ({ m }) => m,
  );
  server.serve("http", "127.0.0.1:38091").catch(() => {});
  await new Promise((r) => setTimeout(r, 1000));
  const client = await McpClient.http("http://127.0.0.1:38091/mcp");
  const tools = await client.listTools();
  assert.ok((tools.tools || []).some((t) => t.name === "echo"));
  const res = await client.callTool("echo", { m: "pong" });
  assert.ok(JSON.stringify(res).includes("pong"));

  const configuredClient = await McpClient.connect({
    transport: "http",
    url: "http://127.0.0.1:38091/mcp",
    headers: { "x-chevalier-test": "structured-config" },
  });
  const configuredTools = await configuredClient.listTools();
  assert.ok((configuredTools.tools || []).some((t) => t.name === "echo"));
});

test("mcp structured stdio config rejects an empty command", async () => {
  await assert.rejects(
    McpClient.connect({
      transport: "stdio",
      command: "",
      args: ["not flattened"],
      env: { CHEVALIER_MCP_TEST_SECRET: "not-on-the-command-line" },
      cwd: process.cwd(),
    }),
    /Empty command/,
  );
});

test("mcp structured stdio config preserves args, env, and cwd", async () => {
  const fixture = path.join(__dirname, "..", "test-fixtures", "mcp-stdio-server.cjs");
  const cwd = path.dirname(fixture);
  const client = await McpClient.connect({
    transport: "stdio",
    command: process.execPath,
    args: [fixture, "argument with spaces"],
    env: { CHEVALIER_MCP_TEST_SECRET: "environment-only-secret" },
    cwd,
  });

  const result = await client.callTool("config_report", {});
  const report = JSON.parse(result.content[0].text);
  assert.deepStrictEqual(report, {
    args: ["argument with spaces"],
    cwd,
    secret: "environment-only-secret",
  });
});

test("agentic() injects a Runtime as the last arg", () => {
  const fn = agentic({ model: "x" }, (a, rt) => (rt instanceof Runtime ? a * 2 : -1));
  assert.strictEqual(fn(21), 42);
});

test("schema-only tool registers (no handler)", async () => {
  const rt = new Runtime();
  await rt.tool({ name: "search", schema: { type: "object", properties: { q: { type: "string" } } } });
  const schemas = await rt.getToolSchemas();
  assert.ok(schemas.some((s) => s.name === "search"));
});

test("dispose() releases registered tools (breaks handler↔runtime cycle)", async () => {
  const rt = new Runtime();
  await rt.tool({ name: "x", schema: { type: "object", properties: {} }, handler: async () => "y" });
  assert.strictEqual((await rt.getToolSchemas()).length, 1);
  await rt.dispose();
  assert.strictEqual((await rt.getToolSchemas()).length, 0);
});
