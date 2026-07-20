const { test } = require("node:test");
const assert = require("node:assert");
const { createVfsGatewayServer } = require("../index.js");

const BATCH_FUZZ_SEED = 0x62617463;

const xorshift32 = (seed) => {
  let state = seed >>> 0;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
};

const requestBody = async (handler, operation, body) =>
  handler(
    new Request(`http://local/internal/chevalier/vfs/security-owner/${operation}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: typeof body === "string" ? body : JSON.stringify(body),
    }),
  );

const countingStore = () => {
  const calls = {
    stat: 0,
    read: 0,
    write: 0,
    writeMany: 0,
    namespaceMany: 0,
    symlink: 0,
    hardLink: 0,
    hardLinkAlias: 0,
  };
  const store = {
    async stat(path) {
      calls.stat += 1;
      return {
        path,
        kind: "File",
        sizeBytes: 1n,
        contentHash: "a".repeat(64),
      };
    },
    async read() {
      calls.read += 1;
      return Buffer.from("x");
    },
    async write(path) {
      calls.write += 1;
      return {
        path,
        contentHash: "b".repeat(64),
        previousHash: null,
        changed: true,
      };
    },
    async writeMany(writes) {
      calls.writeMany += 1;
      return writes.map((write) => ({
        path: write.path,
        contentHash: "b".repeat(64),
        previousHash: null,
        changed: true,
      }));
    },
    async applyNamespaceBatch() {
      calls.namespaceMany += 1;
    },
    async createSymlink() {
      calls.symlink += 1;
    },
    async createHardLink(source, destination) {
      calls.hardLink += 1;
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
      calls.hardLinkAlias += 1;
      return ".GiT/HEAD";
    },
  };
  return { calls, store };
};

const noStorageCalls = {
  stat: 0,
  read: 0,
  write: 0,
  writeMany: 0,
  namespaceMany: 0,
  symlink: 0,
  hardLink: 0,
  hardLinkAlias: 0,
};

test("gateway rejects malformed and oversized path batches before storage", async () => {
  const { calls, store } = countingStore();
  const handler = createVfsGatewayServer({ resolveStore: () => store });
  const oversized = Array.from({ length: 4097 }, (_, index) => `file-${index}`);
  const malformedBodies = [
    "{bad-json",
    null,
    [],
    {},
    { paths: null },
    { paths: "file.txt" },
    { paths: [null] },
    { paths: [{}] },
    { paths: oversized },
  ];

  for (const operation of ["metadata-many", "read-many"]) {
    for (const body of malformedBodies) {
      const response = await requestBody(handler, operation, body);
      assert.strictEqual(response.status, 400, `${operation}: ${JSON.stringify(body)?.slice(0, 120)}`);
    }
  }

  assert.deepStrictEqual(calls, noStorageCalls);
});

test("gateway rejects malformed and oversized write batches atomically", async () => {
  const { calls, store } = countingStore();
  const handler = createVfsGatewayServer({ resolveStore: () => store });
  const oversized = Array.from({ length: 4097 }, (_, index) => ({
    path: `file-${index}`,
    body: [index % 256],
  }));
  const malformedBodies = [
    "{bad-json",
    null,
    [],
    {},
    { writes: null },
    { writes: "file.txt" },
    { writes: [null] },
    { writes: [{}] },
    { writes: [{ path: null, body: [1] }] },
    { writes: [{ path: "", body: [1] }] },
    { writes: [{ path: "file.txt", body: null }] },
    { writes: [{ path: "file.txt", body: [-1] }] },
    { writes: [{ path: "file.txt", body: [256] }] },
    { writes: [{ path: "file.txt", body: [1.5] }] },
    { writes: [{ path: "file.txt", body: ["1"] }] },
    { writes: [{ path: "file.txt", body: [1], ifMatch: 7 }] },
    { writes: [{ path: "file.txt", body: [1], precondition: "bad" }] },
    { writes: [{ path: "file.txt", body: [1], precondition: { fingerprint: 7 } }] },
    { writes: [{ path: "file.txt", body: [1] }, { path: ".git/HEAD", body: [2] }] },
    { writes: oversized },
  ];

  for (const body of malformedBodies) {
    const response = await requestBody(handler, "write-many", body);
    assert.strictEqual(response.status, 400, JSON.stringify(body)?.slice(0, 120));
  }

  assert.deepStrictEqual(calls, noStorageCalls);
});

test("gateway accepts bounded, well-formed batches after validation", async () => {
  const { calls, store } = countingStore();
  const handler = createVfsGatewayServer({ resolveStore: () => store });

  assert.strictEqual(
    (await requestBody(handler, "metadata-many", { paths: [" one.txt "] })).status,
    200,
  );
  assert.strictEqual(
    (await requestBody(handler, "read-many", { paths: [" two.txt "] })).status,
    200,
  );
  assert.strictEqual(
    (
      await requestBody(handler, "write-many", {
        writes: [{ path: " three.txt ", body: [0, 127, 255], precondition: { fingerprint: null } }],
      })
    ).status,
    200,
  );

  assert.deepStrictEqual(calls, {
    ...noStorageCalls,
    stat: 1,
    read: 1,
    writeMany: 1,
  });
});

test("gateway validates namespace batches and malformed preconditions before mutation", async () => {
  const { calls, store } = countingStore();
  const handler = createVfsGatewayServer({ resolveStore: () => store });
  const oversized = Array.from({ length: 4097 }, (_, index) => ({
    kind: "create_directory",
    path: `dir-${index}`,
  }));
  const malformedBodies = [
    "{bad-json",
    null,
    [],
    {},
    { mutations: null },
    { mutations: [{ kind: "delete_file", path: "file.txt", ifMatch: 7 }] },
    { mutations: oversized },
  ];

  for (const body of malformedBodies) {
    const response = await requestBody(handler, "namespace-many", body);
    assert.strictEqual(response.status, 400, JSON.stringify(body)?.slice(0, 120));
  }
  assert.strictEqual(calls.namespaceMany, 0);
});

test(`gateway rejects seeded malformed write items atomically (seed 0x${BATCH_FUZZ_SEED.toString(16)})`, async () => {
  const { calls, store } = countingStore();
  const handler = createVfsGatewayServer({ resolveStore: () => store });
  const random = xorshift32(BATCH_FUZZ_SEED);

  for (let caseIndex = 0; caseIndex < 256; caseIndex += 1) {
    const valid = { path: `valid-${caseIndex}.bin`, body: [0, random() % 256, 255] };
    const variant = random() % 6;
    const invalid =
      variant === 0
        ? { ...valid, path: null }
        : variant === 1
          ? { ...valid, body: [random() % 256, 256] }
          : variant === 2
            ? { ...valid, body: [random() / 2] }
            : variant === 3
              ? { ...valid, precondition: [] }
              : variant === 4
                ? { ...valid, ifMatch: { digest: "bad" } }
                : { ...valid, path: ".git/index" };
    const response = await requestBody(handler, "write-many", {
      writes: [valid, invalid],
    });
    assert.strictEqual(response.status, 400, `case ${caseIndex}, variant ${variant}`);
  }

  assert.strictEqual(calls.writeMany, 0);
});

test("disabled Git policy case-folds every decoded path before storage", async () => {
  const { calls, store } = countingStore();
  const handler = createVfsGatewayServer({
    resolveStore: () => store,
    allowGitMetadata: () => false,
  });
  const pathVariants = [".git/HEAD", ".GIT/HEAD", ".Git/HEAD", "nested/.gIt/index"];

  for (const path of pathVariants) {
    const encoded = encodeURIComponent(path);
    const stat = await handler(
      new Request(`http://local/internal/chevalier/vfs/security-owner/stat?path=${encoded}`),
    );
    const read = await handler(
      new Request(`http://local/internal/chevalier/vfs/security-owner/file/raw?path=${encoded}`),
    );
    assert.strictEqual(stat.status, 404, path);
    assert.strictEqual(read.status, 404, path);
  }

  const metadata = await requestBody(handler, "metadata-many", { paths: pathVariants });
  assert.strictEqual(metadata.status, 200);
  assert.deepStrictEqual((await metadata.json()).entries, pathVariants.map(() => null));
  const readMany = await requestBody(handler, "read-many", { paths: pathVariants });
  assert.strictEqual(readMany.status, 200);
  assert.deepStrictEqual((await readMany.json()).entries, pathVariants.map(() => null));

  assert.strictEqual(
    (
      await requestBody(handler, "write-many", {
        writes: [{ path: ".gIT/index", body: [1] }],
      })
    ).status,
    400,
  );
  assert.strictEqual(
    (
      await handler(
        new Request("http://local/internal/chevalier/vfs/security-owner/file?path=.%47IT%2fconfig", {
          method: "PUT",
          body: "blocked",
        }),
      )
    ).status,
    400,
  );
  assert.strictEqual(
    (
      await requestBody(handler, "hard-link/v1", {
        source_path: ".Git/HEAD",
        destination_path: "head-copy",
      })
    ).status,
    400,
  );
  assert.strictEqual(
    (
      await requestBody(handler, "hard-link-alias/v1", {
        file_id: "file-1",
        excluding_path: ".GIT/HEAD",
      })
    ).status,
    400,
  );
  assert.strictEqual(
    (
      await requestBody(handler, "namespace-many", {
        mutations: [{ kind: "create_directory", path: ".Git/objects" }],
      })
    ).status,
    400,
  );
  assert.strictEqual(
    (
      await requestBody(handler, "namespace-many", {
        mutations: [{ kind: "create_symlink", path: "head-link", target: ".GIT/HEAD" }],
      })
    ).status,
    400,
  );
  assert.strictEqual(
    (
      await handler(
        new Request(
          "http://local/internal/chevalier/vfs/security-owner/symlink?path=head-link&target=.%47iT%2fHEAD",
          { method: "PUT" },
        ),
      )
    ).status,
    400,
  );
  assert.strictEqual(
    (
      await handler(
        new Request("http://local/internal/chevalier/vfs/security-owner/lease", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ path: ".GIT/index.lock" }),
        }),
      )
    ).status,
    400,
  );

  assert.deepStrictEqual(calls, noStorageCalls);
});

test("enabled owner policy permits owner-local mixed-case Git metadata", async () => {
  const { calls, store } = countingStore();
  const handler = createVfsGatewayServer({
    resolveStore: () => store,
    allowGitMetadata: (ownerId) => ownerId === "enabled-owner",
  });

  const enabled = await handler(
    new Request("http://local/internal/chevalier/vfs/enabled-owner/stat?path=.GIT%2fHEAD"),
  );
  assert.strictEqual(enabled.status, 200);
  assert.strictEqual(calls.stat, 1);

  const disabled = await handler(
    new Request("http://local/internal/chevalier/vfs/disabled-owner/stat?path=.GIT%2fHEAD"),
  );
  assert.strictEqual(disabled.status, 404);
  assert.strictEqual(calls.stat, 1);
});

test("tree listings omit every mixed-case Git variant only for disabled owners", async () => {
  const metadata = (path) => ({
    path,
    kind: "File",
    sizeBytes: 1n,
    contentHash: "a".repeat(64),
  });
  const store = {
    async listDir() {
      return [metadata("src/app.ts"), metadata(".git/HEAD"), metadata(".GIT/index"), metadata("nested/.GiT/config")];
    },
  };
  const handler = createVfsGatewayServer({
    resolveStore: () => store,
    allowGitMetadata: (ownerId) => ownerId === "enabled-owner",
  });

  const disabled = await handler(
    new Request("http://local/internal/chevalier/vfs/disabled-owner/tree?path="),
  );
  assert.strictEqual(disabled.status, 200);
  assert.deepStrictEqual(
    (await disabled.json()).map((entry) => entry.name),
    ["app.ts"],
  );

  const enabled = await handler(
    new Request("http://local/internal/chevalier/vfs/enabled-owner/tree?path="),
  );
  assert.strictEqual(enabled.status, 200);
  assert.deepStrictEqual(
    (await enabled.json()).map((entry) => entry.name),
    ["app.ts", "HEAD", "index", "config"],
  );
});
