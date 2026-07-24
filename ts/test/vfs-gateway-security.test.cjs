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

test("attribute-only metadata batches preserve order and run hashless stats concurrently", async () => {
  let active = 0;
  let maxActive = 0;
  const options = [];
  const store = {
    async stat(path, requestedOptions) {
      options.push(requestedOptions);
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active -= 1;
      return {
        path,
        kind: "File",
        sizeBytes: BigInt(path.length),
        contentHash: null,
      };
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: () => store });
  const paths = Array.from({ length: 32 }, (_, index) => `file-${index}`);
  const response = await handler(
    new Request(
      "http://local/internal/chevalier/vfs/security-owner/metadata-many?max_hash_bytes=0",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ paths }),
      },
    ),
  );

  assert.strictEqual(response.status, 200);
  assert.deepStrictEqual(
    (await response.json()).entries.map((entry) => entry.size_bytes),
    paths.map((path) => path.length),
  );
  assert.deepStrictEqual(options, paths.map(() => ({ maxHashBytes: 0 })));
  assert.ok(maxActive > 1, `expected concurrent stats, saw max concurrency ${maxActive}`);
});

test("ordinary metadata batches retain serial full-metadata semantics", async () => {
  let active = 0;
  let maxActive = 0;
  const options = [];
  const store = {
    async stat(path, requestedOptions) {
      options.push(requestedOptions);
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 2));
      active -= 1;
      return { path, kind: "File", sizeBytes: 1n, contentHash: "a".repeat(64) };
    },
  };
  const handler = createVfsGatewayServer({ resolveStore: () => store });
  const response = await requestBody(handler, "metadata-many", {
    paths: ["one", "two", "three"],
  });

  assert.strictEqual(response.status, 200);
  assert.deepStrictEqual(options, [undefined, undefined, undefined]);
  assert.strictEqual(maxActive, 1);
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
    stat: 2,
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
    { operation_ids: [], mutations: null },
    {
      operation_ids: ["duplicate", "duplicate"],
      mutations: [
        { kind: "create_directory", path: "one" },
        { kind: "create_directory", path: "two" },
      ],
    },
    {
      operation_ids: ["only-one"],
      mutations: [
        { kind: "create_directory", path: "one" },
        { kind: "create_directory", path: "two" },
      ],
    },
    {
      operation_ids: ["invalid-delete"],
      mutations: [{ kind: "delete_file", path: "file.txt", ifMatch: 7 }],
    },
    {
      operation_ids: oversized.map((_, index) => `oversized-${index}`),
      mutations: oversized,
    },
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
        operation_ids: ["excluded-directory"],
        mutations: [{ kind: "create_directory", path: ".Git/objects" }],
      })
    ).status,
    400,
  );
  assert.strictEqual(
    (
      await requestBody(handler, "namespace-many", {
        operation_ids: ["excluded-symlink"],
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

// ---- long-poll revision watch ---------------------------------------------

const NAMESPACE_REVISION_HEADER = "x-chevalier-vfs-namespace-revision";

const watchRequest = (handler, owner, query, init) =>
  handler(
    new Request(
      `http://local/internal/chevalier/vfs/${owner}/watch${query === "" ? "" : `?${query}`}`,
      { method: "GET", ...(init ?? {}) },
    ),
  );

const putDir = (handler, owner, path) =>
  handler(
    new Request(`http://local/internal/chevalier/vfs/${owner}/dir?path=${path}`, {
      method: "PUT",
    }),
  );

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Race a promise against a deadline so a hung watcher/mutation fails loudly
// instead of stalling the test run.
const withDeadline = async (promise, ms, message) => {
  let timer;
  const guard = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  try {
    return await Promise.race([promise, guard]);
  } finally {
    clearTimeout(timer);
  }
};

test("watch answers 200 immediately when the revision already exceeds since", async () => {
  const handler = createVfsGatewayServer({ resolveStore: () => ({}) });

  const response = await watchRequest(handler, "owner-immediate", "since=0&timeout_ms=1000");
  assert.strictEqual(response.status, 200);
  const header = response.headers.get(NAMESPACE_REVISION_HEADER);
  assert.ok(header !== null && Number(header) > 0, "200 stamps the current revision header");
  const body = await response.json();
  assert.strictEqual(body.revision, Number(header), "body revision matches the header");

  // since absent/invalid is treated as 0, so this also resolves immediately.
  const absent = await watchRequest(handler, "owner-immediate", "");
  assert.strictEqual(absent.status, 200);
  const garbage = await watchRequest(handler, "owner-immediate", "since=not-a-number");
  assert.strictEqual(garbage.status, 200);
});

test("watch resolves promptly when a concurrent mutation publishes", async () => {
  const handler = createVfsGatewayServer({ resolveStore: () => ({ async mkdir() {} }) });
  const owner = "owner-notify";

  const seed = await watchRequest(handler, owner, "since=0");
  const baseline = (await seed.json()).revision;

  // Park a watcher exactly at the baseline with a long timeout.
  const parked = watchRequest(handler, owner, `since=${baseline}&timeout_ms=30000`);
  await delay(20);

  const mutation = await putDir(handler, owner, "folder");
  assert.strictEqual(mutation.status, 204);
  const mutated = Number(mutation.headers.get(NAMESPACE_REVISION_HEADER));
  assert.ok(mutated > baseline, "mutation advances the revision");

  const woke = await withDeadline(
    parked,
    500,
    "parked watcher did not resolve within 500ms of the publish",
  );
  assert.strictEqual(woke.status, 200);
  const body = await woke.json();
  assert.ok(body.revision > baseline);
  assert.strictEqual(body.revision, Number(woke.headers.get(NAMESPACE_REVISION_HEADER)));
});

test("watch answers 204 on timeout with the revision unchanged", async () => {
  const handler = createVfsGatewayServer({ resolveStore: () => ({}) });

  // since can never be exceeded, so the watcher parks and then times out at the
  // clamped 1s floor with the revision unchanged.
  const response = await watchRequest(
    handler,
    "owner-timeout",
    `since=${Number.MAX_SAFE_INTEGER}&timeout_ms=1000`,
  );
  assert.strictEqual(response.status, 204);
  const header = response.headers.get(NAMESPACE_REVISION_HEADER);
  assert.ok(header !== null, "204 stamps the namespace-revision header");
  assert.ok(
    Number(header) < Number.MAX_SAFE_INTEGER,
    "204 stamps the unchanged current revision",
  );
  assert.strictEqual(await response.text(), "", "204 carries no body");
});

test("a parked watch neither blocks nor slows a concurrent mutation", async () => {
  const handler = createVfsGatewayServer({ resolveStore: () => ({ async mkdir() {} }) });
  const owner = "owner-nonblocking";

  const seed = await watchRequest(handler, owner, "since=0");
  const baseline = (await seed.json()).revision;

  // Park a watcher with a 30s timeout; it must never gate the mutation.
  const parked = watchRequest(handler, owner, `since=${baseline}&timeout_ms=30000`);
  await delay(20);

  // A blocked mutation would stall until the 30s watch timeout; bounding it at
  // 500ms proves the parked watcher holds no lock on the mutation path.
  const mutation = await withDeadline(
    putDir(handler, owner, "folder"),
    500,
    "a parked watcher blocked a concurrent mutation",
  );
  assert.strictEqual(mutation.status, 204);

  // The mutation's publish drains the watcher; make sure it does not leak.
  const woke = await withDeadline(parked, 500, "watcher did not drain after the mutation");
  assert.strictEqual(woke.status, 200);
});

test("watch requires the same bearer auth as every other route", async () => {
  const handler = createVfsGatewayServer({
    resolveStore: () => ({}),
    authToken: "secret-token",
  });

  const unauthorized = await watchRequest(handler, "owner-auth", "since=0");
  assert.strictEqual(unauthorized.status, 401);

  const wrongToken = await watchRequest(handler, "owner-auth", "since=0", {
    headers: { authorization: "Bearer nope" },
  });
  assert.strictEqual(wrongToken.status, 401);

  const authorized = await watchRequest(handler, "owner-auth", "since=0", {
    headers: { authorization: "Bearer secret-token" },
  });
  assert.strictEqual(authorized.status, 200);
});

// ---- targeted revocation: affected paths on watch --------------------------

// A store that accepts every publication route the path tests drive, and whose
// snapshot stats are cheap (`null` == no metadata) so a 1100-path batch stays a
// unit test rather than a benchmark.
const watchPathStore = () => ({
  async mkdir() {},
  async applyNamespaceBatch() {},
  async stat() {
    return null;
  },
});

const namespaceMany = (handler, owner, mutations) =>
  handler(
    new Request(`http://local/internal/chevalier/vfs/${owner}/namespace-many`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        operation_ids: mutations.map((_, index) => `op-${index}`),
        mutations,
      }),
    }),
  );

const createFiles = (count, prefix) =>
  Array.from({ length: count }, (_, index) => ({
    kind: "create_file",
    path: `${prefix}/file-${index}.txt`,
  }));

test("watch answers with exactly the paths published since the watcher's revision", async () => {
  const handler = createVfsGatewayServer({ resolveStore: () => watchPathStore() });
  const owner = "owner-affected-paths";

  // Seed one publication so the watcher's `since` sits inside the retained
  // history; its own paths must NOT appear in the answer.
  const seeded = await namespaceMany(handler, owner, [
    { kind: "create_file", path: "seed/first.txt" },
  ]);
  assert.strictEqual(seeded.status, 200);
  const baseline = Number(seeded.headers.get(NAMESPACE_REVISION_HEADER));

  const second = await namespaceMany(handler, owner, [
    { kind: "create_file", path: "src/a.ts" },
    { kind: "create_directory", path: "src/nested" },
  ]);
  assert.strictEqual(second.status, 200);
  const third = await namespaceMany(handler, owner, [
    { kind: "rename", from: "src/a.ts", to: "src/b.ts" },
  ]);
  assert.strictEqual(third.status, 200);
  const latest = Number(third.headers.get(NAMESPACE_REVISION_HEADER));

  const response = await watchRequest(handler, owner, `since=${baseline}&timeout_ms=1000`);
  assert.strictEqual(response.status, 200);
  const body = await response.json();
  assert.strictEqual(body.revision, latest);
  assert.strictEqual(body.truncated, undefined, "a complete answer omits truncated");
  // Every path published after `since` — each mutation's own paths plus the
  // parent directories whose listings changed — and nothing from before it.
  assert.deepStrictEqual(
    [...body.paths].sort(),
    ["src", "src/a.ts", "src/b.ts", "src/nested"],
    "the union spans exactly the publications the watcher is advanced across",
  );
});

test("a watcher woken by a publication is answered with that publication's paths", async () => {
  const handler = createVfsGatewayServer({ resolveStore: () => watchPathStore() });
  const owner = "owner-woken-paths";

  const seeded = await namespaceMany(handler, owner, [
    { kind: "create_file", path: "seed/first.txt" },
  ]);
  const baseline = Number(seeded.headers.get(NAMESPACE_REVISION_HEADER));

  // Park at the baseline, then publish. The watcher is woken from inside the
  // writer release, so the publication must already be recorded when the wake
  // computes its answer — otherwise the very revision it was woken for would be
  // missing from the history.
  const parked = watchRequest(handler, owner, `since=${baseline}&timeout_ms=30000`);
  await delay(20);
  const published = await namespaceMany(handler, owner, [
    { kind: "create_file", path: "late/added.txt" },
  ]);
  assert.strictEqual(published.status, 200);

  const woke = await withDeadline(parked, 500, "parked watcher did not wake on the publish");
  assert.strictEqual(woke.status, 200);
  const body = await woke.json();
  assert.strictEqual(body.revision, Number(published.headers.get(NAMESPACE_REVISION_HEADER)));
  assert.strictEqual(body.truncated, undefined);
  assert.deepStrictEqual([...body.paths].sort(), ["late", "late/added.txt"]);
});

test("watch serializes an empty-but-complete path set distinguishably from a truncated one", async () => {
  const handler = createVfsGatewayServer({ resolveStore: () => watchPathStore() });
  const owner = "owner-empty-complete";

  const seeded = await namespaceMany(handler, owner, [
    { kind: "create_file", path: "seed/first.txt" },
  ]);
  const baseline = Number(seeded.headers.get(NAMESPACE_REVISION_HEADER));

  // A mkdir publishes without a known path set. It is recorded as an EMPTY
  // publication rather than skipped, so the watcher below is still answered
  // completely — a missing revision would have forced truncation instead.
  const mutation = await putDir(handler, owner, "folder");
  assert.strictEqual(mutation.status, 204);

  const response = await watchRequest(handler, owner, `since=${baseline}&timeout_ms=1000`);
  assert.strictEqual(response.status, 200);
  const body = await response.json();
  assert.ok(
    Object.prototype.hasOwnProperty.call(body, "paths"),
    "an empty answer still serializes `paths`; an omitted field would mean the gateway does not report affected paths at all",
  );
  assert.deepStrictEqual(body.paths, []);
  assert.strictEqual(body.truncated, undefined);

  // The same empty array, but flagged truncated for a watcher that cannot be
  // answered completely — the two are never confusable.
  const behind = await watchRequest(handler, owner, "since=1&timeout_ms=1000");
  assert.strictEqual(behind.status, 200);
  const behindBody = await behind.json();
  assert.deepStrictEqual(behindBody.paths, []);
  assert.strictEqual(behindBody.truncated, true);
});

test("a watcher behind the retained publication history is answered truncated", async () => {
  const handler = createVfsGatewayServer({ resolveStore: () => watchPathStore() });
  const owner = "owner-history-evicted";

  const first = await namespaceMany(handler, owner, [
    { kind: "create_file", path: "gone/first.txt" },
  ]);
  const evicted = Number(first.headers.get(NAMESPACE_REVISION_HEADER));
  const second = await namespaceMany(handler, owner, [
    { kind: "create_file", path: "kept/second.txt" },
  ]);
  const retained = Number(second.headers.get(NAMESPACE_REVISION_HEADER));

  // Inside the window both watchers are answered precisely.
  const complete = await watchRequest(handler, owner, `since=${evicted}&timeout_ms=1000`);
  const completeBody = await complete.json();
  assert.strictEqual(completeBody.truncated, undefined);
  assert.deepStrictEqual([...completeBody.paths].sort(), ["kept", "kept/second.txt"]);

  // 255 more publications puts the history at 257, one past its 256 bound, so
  // exactly the oldest entry is evicted: `evicted` falls out of the window and
  // `retained` becomes its oldest member.
  for (let index = 0; index < 255; index += 1) {
    assert.strictEqual((await putDir(handler, owner, `bulk-${index}`)).status, 204);
  }

  const response = await watchRequest(handler, owner, `since=${evicted}&timeout_ms=1000`);
  assert.strictEqual(response.status, 200);
  const body = await response.json();
  assert.strictEqual(
    body.truncated,
    true,
    "publications before the retained history may have been evicted unseen",
  );
  assert.deepStrictEqual(body.paths, []);

  // The watcher one revision ahead is still inside the window, and the empty
  // bulk publications give it a complete, empty answer.
  const inside = await watchRequest(handler, owner, `since=${retained}&timeout_ms=1000`);
  const insideBody = await inside.json();
  assert.strictEqual(insideBody.truncated, undefined);
  assert.deepStrictEqual(insideBody.paths, []);
});

test("a union past the watch path cap is answered truncated", async () => {
  const handler = createVfsGatewayServer({ resolveStore: () => watchPathStore() });
  const owner = "owner-path-cap";

  const seeded = await namespaceMany(handler, owner, [
    { kind: "create_file", path: "seed/first.txt" },
  ]);
  const baseline = Number(seeded.headers.get(NAMESPACE_REVISION_HEADER));

  // 900 files in one directory -> 901 paths (each file plus their shared
  // parent), comfortably under the 1024 cap: still answered precisely.
  const under = await namespaceMany(handler, owner, createFiles(900, "bulk"));
  assert.strictEqual(under.status, 200);
  const underRevision = Number(under.headers.get(NAMESPACE_REVISION_HEADER));
  const precise = await watchRequest(handler, owner, `since=${baseline}&timeout_ms=1000`);
  const preciseBody = await precise.json();
  assert.strictEqual(preciseBody.truncated, undefined);
  assert.strictEqual(preciseBody.paths.length, 901);

  // A second batch takes the union to 1102, past the cap, where the targeted
  // answer stops being cheaper than the watcher's own fallback.
  const over = await namespaceMany(handler, owner, createFiles(200, "more"));
  assert.strictEqual(over.status, 200);
  const response = await watchRequest(handler, owner, `since=${baseline}&timeout_ms=1000`);
  assert.strictEqual(response.status, 200);
  const body = await response.json();
  assert.strictEqual(body.truncated, true);
  assert.deepStrictEqual(body.paths, [], "a truncated answer carries no partial set");

  // A watcher that missed only the second batch stays under the cap and is
  // still told exactly what to revoke.
  const later = await watchRequest(handler, owner, `since=${underRevision}&timeout_ms=1000`);
  const laterBody = await later.json();
  assert.strictEqual(laterBody.truncated, undefined);
  assert.strictEqual(laterBody.paths.length, 201);
});

// ---- revocation-acked publications ----------------------------------------

test("a publication blocks until a registered watcher re-polls past the new revision", async () => {
  // A long cap makes the outcome unambiguous: the publish can finish quickly
  // only via the ack, never via the (5s) fail-open cap.
  const handler = createVfsGatewayServer({
    resolveStore: () => ({ async mkdir() {} }),
    publicationAckTimeoutMs: 5_000,
  });
  const owner = "ack-blocks";

  const seed = await watchRequest(handler, owner, "since=0&watcher_id=obs-1");
  const baseline = (await seed.json()).revision;

  // Park the identified watcher at the baseline (entry acks baseline).
  const parked = watchRequest(
    handler,
    owner,
    `since=${baseline}&timeout_ms=30000&watcher_id=obs-1`,
  );
  await delay(20);

  // Publish concurrently: it bumps the revision, wakes the watcher, then must
  // wait for obs-1 to re-poll with since >= the new revision.
  let published = null;
  const publish = putDir(handler, owner, "folder").then((response) => {
    published = response;
    return response;
  });

  // The parked poll wakes with the new revision, but waking is NOT an ack.
  const woke = await withDeadline(parked, 500, "watcher did not wake on the publish");
  assert.strictEqual(woke.status, 200);
  const observed = (await woke.json()).revision;
  assert.ok(observed > baseline);

  // Still blocked: the ack is the NEXT poll's since.
  await delay(100);
  assert.strictEqual(published, null, "publication must not answer before the ack");

  // Re-poll with since = observed: THIS ack unblocks the writer.
  const ack = watchRequest(
    handler,
    owner,
    `since=${observed}&timeout_ms=1000&watcher_id=obs-1`,
  );
  const result = await withDeadline(publish, 1_000, "publication did not answer after the ack");
  assert.strictEqual(result.status, 204);

  // Drain the ack poll (it parks then 204s) so it does not leak.
  await withDeadline(ack, 1_500, "ack poll did not drain");
});

test("a publication fails open at the cap when a registered watcher goes silent", async () => {
  const cap = 200;
  const handler = createVfsGatewayServer({
    resolveStore: () => ({ async mkdir() {} }),
    publicationAckTimeoutMs: cap,
  });
  const owner = "ack-failopen";

  const seed = await watchRequest(handler, owner, "since=0&watcher_id=ghost");
  const baseline = (await seed.json()).revision;

  // Register a watcher that wakes on the publish but never re-polls.
  const parked = watchRequest(
    handler,
    owner,
    `since=${baseline}&timeout_ms=30000&watcher_id=ghost`,
  );
  await delay(20);

  const started = Date.now();
  const mutation = await withDeadline(
    putDir(handler, owner, "folder"),
    2_000,
    "publication hung instead of failing open",
  );
  const elapsed = Date.now() - started;

  assert.strictEqual(mutation.status, 204);
  assert.ok(
    elapsed >= cap,
    `publication must wait the full cap before failing open (waited ${elapsed}ms)`,
  );
  assert.ok(
    elapsed < cap + 1_500,
    `publication must fail open near the cap, not hang (waited ${elapsed}ms)`,
  );

  const woke = await withDeadline(parked, 500, "silent watcher still woke on the publish");
  assert.strictEqual(woke.status, 200);
});

test("an anonymous watcher never gates a publication", async () => {
  // A registered watcher would stall the publish ~5s; an anonymous one must not.
  const handler = createVfsGatewayServer({
    resolveStore: () => ({ async mkdir() {} }),
    publicationAckTimeoutMs: 5_000,
  });
  const owner = "ack-anon";

  const seed = await watchRequest(handler, owner, "since=0");
  const baseline = (await seed.json()).revision;

  // Park an ANONYMOUS watcher (no watcher_id): notified but never ack-gating.
  const parked = watchRequest(handler, owner, `since=${baseline}&timeout_ms=30000`);
  await delay(20);

  const mutation = await withDeadline(
    putDir(handler, owner, "folder"),
    500,
    "an anonymous watcher gated a publication",
  );
  assert.strictEqual(mutation.status, 204);

  const woke = await withDeadline(parked, 500, "anonymous watcher still woke on the publish");
  assert.strictEqual(woke.status, 200);
});

test("a gone watcher is pruned and stops gating publications", async () => {
  // A tiny grace lets a silent watcher lapse fast; the long cap would otherwise
  // stall the publish ~5s if the gone watcher were still counted.
  const handler = createVfsGatewayServer({
    resolveStore: () => ({ async mkdir() {} }),
    publicationAckTimeoutMs: 5_000,
    publicationWatcherGraceMs: 100,
  });
  const owner = "ack-prune";

  // Register the watcher via a fast-path poll, then let it go silent.
  const seed = await watchRequest(handler, owner, "since=0&watcher_id=obs-gone");
  assert.strictEqual(seed.status, 200);

  // Wait past the 100ms grace so the silent watcher is prunable.
  await delay(200);

  // The publish must not wait the 5s cap for the pruned watcher.
  const mutation = await withDeadline(
    putDir(handler, owner, "folder"),
    500,
    "a gone watcher still gated a publication",
  );
  assert.strictEqual(mutation.status, 204);
});
