// Streamed append publication (`x-chevalier-vfs-append-offset`) over real HTTP:
// the TS gateway hosted on node:http, driven by raw requests with exact headers
// and by the Rust gateway client through the N-API binding.
const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const { once } = require("node:events");
const { Readable } = require("node:stream");
const { createVfsGatewayServer, VfsStorage, vfsContentHash } = require("../index.js");

async function serveGateway(context, resolveStore) {
  const handler = createVfsGatewayServer({ resolveStore });
  const server = http.createServer(async (req, res) => {
    try {
      const hasBody = req.method !== "GET" && req.method !== "HEAD";
      const response = await handler(
        new Request(`http://${req.headers.host}${req.url}`, {
          method: req.method,
          headers: req.headers,
          body: hasBody ? Readable.toWeb(req) : undefined,
          duplex: hasBody ? "half" : undefined,
        }),
      );
      res.writeHead(response.status, Object.fromEntries(response.headers));
      res.end(Buffer.from(await response.arrayBuffer()));
    } catch (error) {
      res.writeHead(500, { "content-type": "text/plain" });
      res.end(String(error?.stack ?? error));
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  context.after(() => {
    server.closeAllConnections();
    server.close();
  });
  return `http://127.0.0.1:${server.address().port}/internal/chevalier/vfs/owner`;
}

function tempDir(context, prefix) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  context.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  return dir;
}

function pattern(length, seed) {
  const bytes = Buffer.alloc(length);
  for (let index = 0; index < length; index += 1) bytes[index] = (index * 31 + seed) % 251;
  return bytes;
}

/** One `PUT /file` append with the contract's headers; `overrides` replaces or
 *  (with `undefined`) removes individual headers. */
function appendRequest(endpoint, filePath, { offset, baseHash, expectedHash, tail }, overrides = {}) {
  const headers = {
    "content-length": String(tail.length),
    "x-chevalier-vfs-stream-upload": "1",
    "x-chevalier-vfs-append-offset": String(offset),
    "x-chevalier-vfs-precondition-kind": "content_fingerprint",
    "x-chevalier-vfs-precondition-fingerprint": baseHash,
    "x-chevalier-vfs-expected-content-sha256": expectedHash,
    "x-chevalier-vfs-operation": "vfs_stream_append",
    ...overrides,
  };
  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined) delete headers[name];
  }
  return fetch(`${endpoint}/file?path=${encodeURIComponent(filePath)}`, {
    method: "PUT",
    headers,
    body: tail,
  });
}

async function readRaw(endpoint, filePath) {
  const response = await fetch(`${endpoint}/file/raw?path=${encodeURIComponent(filePath)}`);
  assert.equal(response.status, 200);
  return Buffer.from(await response.arrayBuffer());
}

test("a streamed append extends the stored file in place and publishes the full hash", async (context) => {
  const root = tempDir(context, "chev-append-root-");
  const staging = tempDir(context, "chev-append-staging-");
  const backing = VfsStorage.local(root);
  const endpoint = await serveGateway(context, async () => backing);
  const client = VfsStorage.gateway({ endpoint });

  // The base arrives the way vmd publishes a large file today: one streamed PUT.
  const base = pattern(3 * 1024 * 1024 + 7, 1);
  const basePath = path.join(staging, "base");
  fs.writeFileSync(basePath, base);
  await client.writeFromFile("logs/run.log", basePath, vfsContentHash(base), null);
  const before = await backing.stat("logs/run.log");
  assert.equal(before.contentHash, vfsContentHash(base));

  // Raw contract request: body is only the tail, hashes name base and full file.
  const tail = pattern(48 * 1024 + 3, 2);
  const full = Buffer.concat([base, tail]);
  const response = await appendRequest(endpoint, "logs/run.log", {
    offset: base.length,
    baseHash: vfsContentHash(base),
    expectedHash: vfsContentHash(full),
    tail,
  });
  assert.equal(response.status, 200, await response.clone().text());
  assert.match(response.headers.get("x-chevalier-vfs-namespace-revision") ?? "", /^\d+$/);
  const publication = await response.json();
  assert.equal(publication.path, "logs/run.log");
  assert.equal(publication.content_hash, vfsContentHash(full));
  assert.equal(publication.previous_hash, vfsContentHash(base));
  assert.equal(publication.changed, true);
  assert.deepEqual(
    publication.entries.map((entry) => [entry.path, entry.metadata?.size_bytes, entry.metadata?.content_hash]),
    [["logs/run.log", full.length, vfsContentHash(full)]],
  );

  // The Rust gateway client speaks the same contract for the next growth step.
  const tail2 = pattern(1500, 3);
  const full2 = Buffer.concat([full, tail2]);
  const tail2Path = path.join(staging, "tail2");
  fs.writeFileSync(tail2Path, tail2);
  const clientResult = await client.appendFromFile(
    "logs/run.log",
    tail2Path,
    BigInt(full.length),
    vfsContentHash(full2),
    { ifMatch: vfsContentHash(full) },
  );
  assert.equal(clientResult.content_hash, vfsContentHash(full2));
  assert.equal(clientResult.previous_hash, vfsContentHash(full));

  assert.ok((await readRaw(endpoint, "logs/run.log")).equals(full2), "byte-exact read-back");
  const stat = await (await fetch(`${endpoint}/stat?path=logs%2Frun.log`)).json();
  assert.equal(stat.size_bytes, full2.length);
  assert.equal(stat.content_hash, vfsContentHash(full2));
  const after = await backing.stat("logs/run.log");
  assert.equal(after.fileId, before.fileId, "the append extended the same inode");
});

test("append rejections are precise and leave the stored file unchanged", async (context) => {
  const root = tempDir(context, "chev-append-reject-");
  const staging = tempDir(context, "chev-append-reject-staging-");
  const backing = VfsStorage.local(root);
  const endpoint = await serveGateway(context, async () => backing);
  const base = Buffer.from("0123456789");
  await backing.write("app.log", base);
  const tail = Buffer.from("tail");
  const good = {
    offset: base.length,
    baseHash: vfsContentHash(base),
    expectedHash: vfsContentHash(Buffer.concat([base, tail])),
    tail,
  };
  const assertUnchanged = async (label) => {
    assert.ok((await readRaw(endpoint, "app.log")).equals(base), `${label} must not modify app.log`);
    assert.equal((await backing.stat("app.log")).contentHash, vfsContentHash(base), label);
  };

  const cases = [
    {
      label: "no precondition",
      request: appendRequest(endpoint, "app.log", good, {
        "x-chevalier-vfs-precondition-kind": undefined,
        "x-chevalier-vfs-precondition-fingerprint": undefined,
      }),
      status: 400,
      body: /content_fingerprint precondition/,
    },
    {
      label: "an absent precondition",
      request: appendRequest(endpoint, "app.log", good, {
        "x-chevalier-vfs-precondition-kind": "absent",
        "x-chevalier-vfs-precondition-fingerprint": undefined,
      }),
      status: 400,
      body: /content_fingerprint precondition/,
    },
    {
      label: "an unstreamed append",
      request: appendRequest(endpoint, "app.log", good, { "x-chevalier-vfs-stream-upload": undefined }),
      status: 400,
      body: /requires x-chevalier-vfs-stream-upload/,
    },
    {
      label: "a malformed offset",
      request: appendRequest(endpoint, "app.log", good, { "x-chevalier-vfs-append-offset": "-1" }),
      status: 400,
      body: /non-negative decimal integer/,
    },
    {
      label: "a stale base hash",
      request: appendRequest(endpoint, "app.log", { ...good, baseHash: vfsContentHash(Buffer.from("other")) }),
      status: 409,
      body: /^precondition failed for app\.log$/,
    },
    {
      label: "a wrong offset",
      request: appendRequest(endpoint, "app.log", { ...good, offset: base.length - 2 }),
      status: 409,
      body: /^append base mismatch for app\.log: stored size 10 does not equal append offset 8$/,
    },
    {
      label: "a wrong full-file hash",
      request: appendRequest(endpoint, "app.log", { ...good, expectedHash: vfsContentHash(Buffer.from("nope")) }),
      status: 409,
      body: /^append content hash mismatch for app\.log$/,
    },
    {
      label: "a short body",
      request: appendRequest(endpoint, "app.log", { ...good, tail: tail.subarray(0, 2) }, { "content-length": "2" }),
      status: 409,
      body: /append content hash mismatch/,
    },
  ];
  for (const { label, request, status, body } of cases) {
    const response = await request;
    assert.equal(response.status, status, `${label}: ${await response.clone().text()}`);
    assert.match(await response.text(), body, label);
    await assertUnchanged(label);
  }

  // The Rust client surfaces the same conflict as a VFS_CONFLICT error.
  const tailPath = path.join(staging, "tail");
  fs.writeFileSync(tailPath, tail);
  const client = VfsStorage.gateway({ endpoint });
  await assert.rejects(
    client.appendFromFile("app.log", tailPath, BigInt(base.length), good.expectedHash, {
      ifMatch: vfsContentHash(Buffer.from("stale")),
    }),
    /VFS_CONFLICT status=409/,
  );
  await assertUnchanged("a client append with a stale base");

  const ok = await appendRequest(endpoint, "app.log", good);
  assert.equal(ok.status, 200);
  assert.ok((await readRaw(endpoint, "app.log")).equals(Buffer.concat([base, tail])));
});

test("a store without native append gets a verified server-side construction", async (context) => {
  const root = tempDir(context, "chev-append-fallback-");
  const backing = VfsStorage.local(root);
  const installs = [];
  // Mirrors product wrappers that expose the structural VfsStorage surface but
  // not the runtime-only appendFromFile.
  const wrapper = {
    stat: (filePath, options) => backing.stat(filePath, options),
    read: (filePath) => backing.read(filePath),
    readRange: (filePath, offset, length) => backing.readRange(filePath, offset, length),
    writeFromFile: (filePath, sourcePath, expectedHash, options) => {
      installs.push({ filePath, size: fs.statSync(sourcePath).size, options });
      return backing.writeFromFile(filePath, sourcePath, expectedHash, options);
    },
  };
  const endpoint = await serveGateway(context, async () => wrapper);
  const base = pattern(9 * 1024 * 1024 + 5, 4); // spans more than one base-copy chunk
  await backing.write("big.log", base);
  const tail = Buffer.from("appended line\n");
  const full = Buffer.concat([base, tail]);
  const request = {
    offset: base.length,
    baseHash: vfsContentHash(base),
    expectedHash: vfsContentHash(full),
    tail,
  };

  const mismatch = await appendRequest(endpoint, "big.log", {
    ...request,
    expectedHash: vfsContentHash(Buffer.from("wrong")),
  });
  assert.equal(mismatch.status, 409);
  assert.match(await mismatch.text(), /^append content hash mismatch for big\.log$/);
  assert.equal(installs.length, 0, "a construction that fails verification installs nothing");
  assert.equal((await backing.stat("big.log")).contentHash, vfsContentHash(base));

  const response = await appendRequest(endpoint, "big.log", request);
  assert.equal(response.status, 200, await response.clone().text());
  const publication = await response.json();
  assert.equal(publication.content_hash, vfsContentHash(full));
  assert.equal(publication.previous_hash, vfsContentHash(base));
  assert.deepEqual(installs, [
    { filePath: "big.log", size: full.length, options: { ifMatch: vfsContentHash(base) } },
  ]);
  assert.ok((await readRaw(endpoint, "big.log")).equals(full));
});
