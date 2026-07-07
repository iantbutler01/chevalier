// Offline regression tests (no LLM needed). Live model tests are separate and
// gated on CHEVALIER_TEST_MODEL.
const { test } = require("node:test");
const assert = require("node:assert");
const os = require("node:os");
const path = require("node:path");
const fs = require("node:fs");
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
