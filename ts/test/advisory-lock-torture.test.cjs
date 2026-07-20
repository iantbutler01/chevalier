const assert = require("node:assert/strict");
const test = require("node:test");

const { createVfsGatewayServer } = require("../vfs-gateway-server.js");

const FILE_ID = "stable-file";
const OTHER_FILE_ID = "other-file";

function makeHarness() {
  const files = new Map([
    ["guard", FILE_ID],
    ["guard-alias", FILE_ID],
    ["other", OTHER_FILE_ID],
  ]);
  const handler = createVfsGatewayServer({
    resolveStore: () => ({
      stat: async (path) => {
        const fileId = files.get(path);
        return fileId === undefined
          ? null
          : {
              path,
              kind: "File",
              sizeBytes: 0n,
              fileId,
              linkCount: fileId === FILE_ID ? 2n : 1n,
            };
      },
    }),
  });

  const request = async (body) => {
    const response = await handler(
      new Request("http://local/internal/chevalier/vfs/owner/posix-lock/v1", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
    );
    return {
      status: response.status,
      body: response.headers.get("content-type")?.includes("application/json")
        ? await response.json()
        : await response.text(),
    };
  };
  const set = (
    mount,
    owner,
    kind,
    {
      path = "guard",
      namespace = "posix",
      start = 0n,
      end = 99n,
      pid = 1,
    } = {},
  ) =>
    request({
      action: "set",
      path,
      mount_id: mount,
      lock_owner: owner,
      namespace,
      start: start.toString(),
      end: end.toString(),
      kind,
      pid,
    });
  const get = (
    mount,
    owner,
    kind,
    {
      path = "guard",
      namespace = "posix",
      start = 0n,
      end = 99n,
      pid = 1,
    } = {},
  ) =>
    request({
      action: "get",
      path,
      mount_id: mount,
      lock_owner: owner,
      namespace,
      start: start.toString(),
      end: end.toString(),
      kind,
      pid,
    });

  return { request, set, get };
}

test("failed POSIX conversion preserves the caller's existing lock", async () => {
  const { set } = makeHarness();

  assert.equal((await set("mount-a", "process-a", "read")).body.acquired, true);
  assert.equal((await set("mount-b", "process-b", "read")).body.acquired, true);

  // B prevents A from converting its shared lock to exclusive.
  assert.equal((await set("mount-a", "process-a", "write")).body.acquired, false);

  // A's original read lock must still exist after that rejected conversion.
  assert.equal((await set("mount-b", "process-b", "unlock")).body.acquired, true);
  const thirdWriter = await set("mount-c", "process-c", "write");
  assert.equal(thirdWriter.body.acquired, false);
  assert.deepEqual(thirdWriter.body.conflict, {
    start: "0",
    end: "99",
    kind: "read",
    pid: 1,
  });
});

test("range split, conversion, alias identity, and namespace separation compose", async () => {
  const { set, get } = makeHarness();

  assert.equal((await set("mount-a", "process-a", "write")).body.acquired, true);
  assert.equal(
    (await set("mount-a", "process-a", "unlock", { start: 25n, end: 74n })).body.acquired,
    true,
  );
  assert.equal(
    (await set("mount-b", "process-b", "write", { start: 25n, end: 74n })).body.acquired,
    true,
  );
  assert.equal(
    (await set("mount-c", "process-c", "write", { start: 0n, end: 24n })).body.acquired,
    false,
  );
  assert.equal(
    (await set("mount-c", "process-c", "write", { start: 75n, end: 99n })).body.acquired,
    false,
  );
  assert.equal(
    (await get("mount-c", "process-c", "write", { start: 25n, end: 74n })).body.acquired,
    false,
  );

  // An alias is the same lock identity, an unrelated file is not.
  assert.equal(
    (
      await set("mount-d", "process-d", "write", {
        path: "guard-alias",
        start: 0n,
        end: 24n,
      })
    ).body.acquired,
    false,
  );
  assert.equal(
    (await set("mount-d", "process-d", "write", { path: "other" })).body.acquired,
    true,
  );

  // flock and POSIX locks intentionally do not conflict.
  assert.equal(
    (
      await set("mount-e", "open-file-e", "write", {
        namespace: "flock",
        start: 0n,
        end: BigInt("18446744073709551615"),
      })
    ).body.acquired,
    true,
  );
});

test("read/read sharing, exact release scopes, renewal, expiry, and dead mounts", async () => {
  const originalNow = Date.now;
  let now = 1_000_000;
  Date.now = () => now;
  try {
    const { request, set } = makeHarness();
    assert.equal((await set("mount-a", "process-a", "read")).body.acquired, true);
    assert.equal((await set("mount-b", "process-b", "read")).body.acquired, true);
    assert.equal((await set("mount-c", "process-c", "write")).body.acquired, false);

    now += 30_000;
    assert.equal(
      (
        await request({
          action: "renew_owners",
          mount_id: "mount-a",
          identities: [
            {
              lock_owner: "process-a",
              namespace: "posix",
              file_id: FILE_ID,
            },
          ],
        })
      ).status,
      200,
    );
    now += 20_000;

    // B expired at t+45s; A was renewed through t+75s.
    assert.equal((await set("mount-c", "process-c", "write")).body.acquired, false);
    assert.equal(
      (
        await request({
          action: "release_owner",
          file_id: FILE_ID,
          mount_id: "mount-a",
          lock_owner: "process-a",
          namespace: "flock",
        })
      ).status,
      200,
    );
    // Wrong namespace release did not touch A's POSIX lock.
    assert.equal((await set("mount-c", "process-c", "write")).body.acquired, false);

    assert.equal(
      (
        await request({
          action: "release_mount",
          mount_id: "mount-a",
        })
      ).status,
      200,
    );
    assert.equal((await set("mount-c", "process-c", "write")).body.acquired, true);

    // An unrenewed dead mount becomes reclaimable after its lease.
    now += 46_000;
    assert.equal((await set("mount-d", "process-d", "write")).body.acquired, true);
  } finally {
    Date.now = originalNow;
  }
});

test("exact renewal keeps live identities without reviving abandoned owners", async () => {
  const originalNow = Date.now;
  let now = 2_000_000;
  Date.now = () => now;
  try {
    const { request, set, get } = makeHarness();
    const acquireRead = (owner, namespace, path, start, end) =>
      set("mount-a", owner, "read", {
        namespace,
        path,
        start,
        end,
      });

    assert.equal((await acquireRead("shared-owner", "posix", "guard", 0n, 9n)).body.acquired, true);
    assert.equal((await acquireRead("shared-owner", "posix", "other", 0n, 9n)).body.acquired, true);
    assert.equal((await acquireRead("shared-owner", "flock", "guard", 0n, 9n)).body.acquired, true);
    assert.equal((await acquireRead("live-flock", "flock", "guard", 20n, 29n)).body.acquired, true);
    assert.equal((await acquireRead("abandoned-owner", "posix", "guard", 10n, 19n)).body.acquired, true);
    assert.equal((await acquireRead("invalid-canary", "posix", "guard", 30n, 39n)).body.acquired, true);
    assert.equal(
      (
        await set("mount-b", "shared-owner", "read", {
          namespace: "posix",
          path: "guard",
          start: 40n,
          end: 49n,
        })
      ).body.acquired,
      true,
    );

    now += 30_000;
    const canaryIdentity = {
      lock_owner: "invalid-canary",
      namespace: "posix",
      file_id: FILE_ID,
    };
    const invalidBatch = await request({
      action: "renew_owners",
      mount_id: "mount-a",
      identities: [canaryIdentity, { ...canaryIdentity, file_id: " " }],
    });
    assert.equal(invalidBatch.status, 400);
    const oversizedBatch = await request({
      action: "renew_owners",
      mount_id: "mount-a",
      identities: Array.from({ length: 4097 }, () => canaryIdentity),
    });
    assert.equal(oversizedBatch.status, 400);
    assert.equal(
      (
        await request({
          action: "renew_owners",
          mount_id: "mount-a",
          identities: [],
        })
      ).status,
      400,
    );
    assert.equal(
      (
        await request({
          action: "renew_mount",
          mount_id: "mount-a",
        })
      ).status,
      400,
    );

    const renewed = await request({
      action: "renew_owners",
      mount_id: "mount-a",
      identities: [
        {
          lock_owner: "shared-owner",
          namespace: "posix",
          file_id: FILE_ID,
        },
        {
          lock_owner: "live-flock",
          namespace: "flock",
          file_id: FILE_ID,
        },
      ],
    });
    assert.equal(renewed.status, 200);
    assert.deepEqual(renewed.body, { ok: true, lease_ms: 45_000 });

    now += 20_000;
    const probeWrite = (path, namespace, start, end) =>
      get("probe-mount", "probe-owner", "write", {
        path,
        namespace,
        start,
        end,
      });

    assert.equal((await probeWrite("guard", "posix", 0n, 9n)).body.acquired, false);
    assert.equal((await probeWrite("guard", "posix", 10n, 19n)).body.acquired, true);
    assert.equal((await probeWrite("guard", "posix", 30n, 39n)).body.acquired, true);
    assert.equal((await probeWrite("guard", "posix", 40n, 49n)).body.acquired, true);
    assert.equal((await probeWrite("other", "posix", 0n, 9n)).body.acquired, true);
    assert.equal((await probeWrite("guard", "flock", 0n, 9n)).body.acquired, true);
    assert.equal((await probeWrite("guard", "flock", 20n, 29n)).body.acquired, false);
  } finally {
    Date.now = originalNow;
  }
});

test("high-contention nonblocking acquisition admits exactly one mount", async () => {
  const { set } = makeHarness();
  const contenders = await Promise.all(
    Array.from({ length: 256 }, (_, index) =>
      set(`mount-${index}`, `process-${index}`, "write", { pid: index + 1 }),
    ),
  );

  assert.equal(
    contenders.filter(({ body }) => body.acquired === true).length,
    1,
  );
  assert.equal(
    contenders.filter(({ body }) => body.acquired === false).length,
    255,
  );
});

test("default advisory lock state evicts fresh owners after their final release", async () => {
  const trackedOwners = new Set();
  const ownerPrefix = "bounded-owner-";
  const originalSet = Map.prototype.set;
  const originalDelete = Map.prototype.delete;
  Map.prototype.set = function set(key, value) {
    if (typeof key === "string" && key.startsWith(ownerPrefix)) trackedOwners.add(key);
    return Reflect.apply(originalSet, this, [key, value]);
  };
  Map.prototype.delete = function deleteKey(key) {
    if (typeof key === "string" && key.startsWith(ownerPrefix)) trackedOwners.delete(key);
    return Reflect.apply(originalDelete, this, [key]);
  };

  try {
    const handler = createVfsGatewayServer({
      resolveStore: () => ({
        stat: async (path) => ({
          path,
          kind: "File",
          sizeBytes: 0n,
          fileId: `file-${path}`,
          linkCount: 1n,
        }),
      }),
    });
    const request = async (ownerId, body) => {
      const response = await handler(
        new Request(
          `http://local/internal/chevalier/vfs/${ownerId}/posix-lock/v1`,
          {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
          },
        ),
      );
      assert.equal(response.status, 200);
      return response.json();
    };

    for (let index = 0; index < 4096; index += 1) {
      const ownerId = `${ownerPrefix}${index}`;
      const mountId = `mount-${index}`;
      assert.equal(
        (
          await request(ownerId, {
            action: "set",
            path: "guard",
            mount_id: mountId,
            lock_owner: `process-${index}`,
            namespace: "posix",
            start: "0",
            end: "99",
            kind: "write",
            pid: index + 1,
          })
        ).acquired,
        true,
      );
      await request(ownerId, {
        action: "release_mount",
        mount_id: mountId,
      });
    }

    assert.equal(
      trackedOwners.size,
      0,
      "the default store must not retain one empty map entry per released owner",
    );
  } finally {
    Map.prototype.set = originalSet;
    Map.prototype.delete = originalDelete;
  }
});

function seededRandom(seed) {
  let state = seed >>> 0;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
}

function makeReferenceLockModel(readNow) {
  const leaseMs = 45_000;
  let locks = [];

  const live = () => {
    const now = readNow();
    locks = locks.filter((lock) => lock.expiresAt > now);
    return now;
  };
  const overlaps = (left, right) => left.start <= right.end && right.start <= left.end;
  const subtract = (lock, range) => {
    const remaining = [];
    if (lock.start < range.start) remaining.push({ ...lock, end: range.start - 1n });
    if (lock.end > range.end) remaining.push({ ...lock, start: range.end + 1n });
    return remaining;
  };
  const conflictFor = (candidate, candidates = locks) =>
    candidates.find(
      (lock) =>
        lock.fileId === candidate.fileId &&
        lock.namespace === candidate.namespace &&
        !(lock.mountId === candidate.mountId && lock.lockOwner === candidate.lockOwner) &&
        overlaps(lock, candidate) &&
        (lock.kind === "write" || candidate.kind === "write"),
    );
  const conflictBody = (lock) =>
    lock === undefined
      ? null
      : {
          start: lock.start.toString(),
          end: lock.end.toString(),
          kind: lock.kind,
          pid: lock.pid,
        };

  return {
    apply(body, fileId) {
      const now = live();
      if (body.action === "renew_owners") {
        const identityKeys = new Set(
          body.identities.map((identity) =>
            JSON.stringify([
              identity.lock_owner,
              identity.namespace,
              identity.file_id,
            ]),
          ),
        );
        for (const lock of locks) {
          if (
            lock.mountId === body.mount_id &&
            identityKeys.has(
              JSON.stringify([lock.lockOwner, lock.namespace, lock.fileId]),
            )
          ) {
            lock.expiresAt = now + leaseMs;
          }
        }
        return { ok: true, lease_ms: leaseMs };
      }
      if (body.action === "release_mount") {
        locks = locks.filter((lock) => lock.mountId !== body.mount_id);
        return { ok: true };
      }

      const namespace = body.namespace ?? "posix";
      if (body.action === "release_owner") {
        locks = locks.filter(
          (lock) =>
            lock.mountId !== body.mount_id ||
            lock.lockOwner !== body.lock_owner ||
            lock.fileId !== body.file_id ||
            lock.namespace !== namespace,
        );
        return { ok: true };
      }

      const candidate = {
        mountId: body.mount_id,
        lockOwner: body.lock_owner,
        namespace,
        fileId,
        start: BigInt(body.start),
        end: BigInt(body.end),
        kind: body.kind,
        pid: body.pid,
      };
      if (body.action === "get") {
        const conflict = conflictFor(candidate);
        return {
          acquired: conflict === undefined,
          conflict: conflictBody(conflict),
          file_id: fileId,
          lease_ms: leaseMs,
        };
      }

      const owns = (lock) =>
        lock.mountId === candidate.mountId &&
        lock.lockOwner === candidate.lockOwner &&
        lock.namespace === candidate.namespace &&
        lock.fileId === candidate.fileId;
      const replacement = locks.flatMap((lock) =>
        owns(lock) && overlaps(lock, candidate) ? subtract(lock, candidate) : [lock],
      );
      if (candidate.kind === "unlock") {
        locks = replacement;
        return {
          acquired: true,
          conflict: null,
          file_id: fileId,
          lease_ms: leaseMs,
        };
      }

      const conflict = conflictFor(candidate, replacement);
      if (conflict !== undefined) {
        return {
          acquired: false,
          conflict: conflictBody(conflict),
          file_id: fileId,
          lease_ms: leaseMs,
        };
      }
      locks = [
        ...replacement,
        {
          ...candidate,
          expiresAt: now + leaseMs,
        },
      ];
      return {
        acquired: true,
        conflict: null,
        file_id: fileId,
        lease_ms: leaseMs,
      };
    },
  };
}

test("deterministic randomized advisory operations agree with a bounded reference model", async () => {
  const originalNow = Date.now;
  const seeds = [0x00c0ffee, 0x13579bdf, 0x2468ace0, 0x5eed1234];
  const paths = ["guard", "guard-alias", "other"];
  const fileIdForPath = (path) => (path === "other" ? OTHER_FILE_ID : FILE_ID);
  const ranges = [
    [0n, 15n],
    [16n, 31n],
    [0n, 31n],
    [8n, 23n],
  ];
  const coverage = {
    set: 0,
    get: 0,
    unlock: 0,
    releaseOwner: 0,
    releaseMount: 0,
    renewOwners: 0,
    advance: 0,
    posix: 0,
    flock: 0,
    rejected: 0,
  };

  try {
    for (const [seedIndex, seed] of seeds.entries()) {
      let now = 10_000_000 + seedIndex * 1_000_000;
      Date.now = () => now;
      const random = seededRandom(seed);
      const { request } = makeHarness();
      const model = makeReferenceLockModel(() => now);

      const perform = async (body) => {
        const fileId =
          body.action === "set" || body.action === "get"
            ? fileIdForPath(body.path)
            : null;
        const expected = model.apply(body, fileId);
        const actual = await request(body);
        assert.equal(actual.status, 200, JSON.stringify({ seed, body, actual }));
        assert.deepEqual(actual.body, expected, JSON.stringify({ seed, body }));
        if (actual.body.acquired === false) coverage.rejected += 1;
      };

      // Guarantee that every seed starts by exercising a rejected conversion,
      // namespace separation, and exact lease expiry before the random trace.
      await perform({
        action: "set",
        path: "guard",
        mount_id: "mount-a",
        lock_owner: "process-a",
        namespace: "posix",
        start: "0",
        end: "31",
        kind: "read",
        pid: 1,
      });
      await perform({
        action: "set",
        path: "guard-alias",
        mount_id: "mount-b",
        lock_owner: "process-b",
        namespace: "posix",
        start: "0",
        end: "31",
        kind: "read",
        pid: 2,
      });
      await perform({
        action: "set",
        path: "guard",
        mount_id: "mount-a",
        lock_owner: "process-a",
        namespace: "posix",
        start: "0",
        end: "31",
        kind: "write",
        pid: 1,
      });
      await perform({
        action: "set",
        path: "guard",
        mount_id: "mount-c",
        lock_owner: "open-c",
        namespace: "flock",
        start: "0",
        end: "31",
        kind: "write",
        pid: 3,
      });
      now += 45_000;
      coverage.advance += 1;

      for (let step = 0; step < 256; step += 1) {
        const choice = random() % 100;
        if (choice < 51) {
          const namespace = random() % 2 === 0 ? "posix" : "flock";
          const path = paths[random() % paths.length];
          const [start, end] = ranges[random() % ranges.length];
          const kindChoice = random() % 5;
          const kind = kindChoice === 0 ? "unlock" : kindChoice < 3 ? "read" : "write";
          coverage.set += 1;
          coverage[kind === "unlock" ? "unlock" : namespace] += 1;
          await perform({
            action: "set",
            path,
            mount_id: `mount-${random() % 6}`,
            lock_owner:
              namespace === "posix" ? `process-${random() % 5}` : `open-${random() % 5}`,
            namespace,
            start: start.toString(),
            end: end.toString(),
            kind,
            pid: (random() % 1000) + 1,
          });
        } else if (choice < 66) {
          const namespace = random() % 2 === 0 ? "posix" : "flock";
          const path = paths[random() % paths.length];
          const [start, end] = ranges[random() % ranges.length];
          coverage.get += 1;
          coverage[namespace] += 1;
          await perform({
            action: "get",
            path,
            mount_id: `mount-${random() % 6}`,
            lock_owner:
              namespace === "posix" ? `process-${random() % 5}` : `open-${random() % 5}`,
            namespace,
            start: start.toString(),
            end: end.toString(),
            kind: random() % 2 === 0 ? "read" : "write",
            pid: (random() % 1000) + 1,
          });
        } else if (choice < 77) {
          const namespace = random() % 2 === 0 ? "posix" : "flock";
          coverage.releaseOwner += 1;
          coverage[namespace] += 1;
          await perform({
            action: "release_owner",
            file_id: random() % 2 === 0 ? FILE_ID : OTHER_FILE_ID,
            mount_id: `mount-${random() % 6}`,
            lock_owner:
              namespace === "posix" ? `process-${random() % 5}` : `open-${random() % 5}`,
            namespace,
          });
        } else if (choice < 86) {
          coverage.releaseMount += 1;
          await perform({
            action: "release_mount",
            mount_id: `mount-${random() % 6}`,
          });
        } else if (choice < 94) {
          coverage.renewOwners += 1;
          const identities = Array.from(
            { length: (random() % 3) + 1 },
            () => {
              const namespace = random() % 2 === 0 ? "posix" : "flock";
              return {
                lock_owner:
                  namespace === "posix"
                    ? `process-${random() % 5}`
                    : `open-${random() % 5}`,
                namespace,
                file_id: random() % 2 === 0 ? FILE_ID : OTHER_FILE_ID,
              };
            },
          );
          await perform({
            action: "renew_owners",
            mount_id: `mount-${random() % 6}`,
            identities,
          });
        } else {
          const advances = [1, 14_999, 15_000, 30_000, 45_000, 46_000];
          now += advances[random() % advances.length];
          coverage.advance += 1;
        }

        // Sample a disinterested identity after every mutation. It exposes
        // accidental state loss or retention without inspecting implementation
        // internals, including after conversion, release, renewal, and expiry.
        const probeNamespace = random() % 2 === 0 ? "posix" : "flock";
        const probePath = paths[random() % paths.length];
        const [probeStart, probeEnd] = ranges[random() % ranges.length];
        await perform({
          action: "get",
          path: probePath,
          mount_id: `probe-mount-${seedIndex}`,
          lock_owner: `probe-owner-${seedIndex}`,
          namespace: probeNamespace,
          start: probeStart.toString(),
          end: probeEnd.toString(),
          kind: random() % 2 === 0 ? "read" : "write",
          pid: 10_000 + seedIndex,
        });
      }
    }
  } finally {
    Date.now = originalNow;
  }

  for (const [operation, count] of Object.entries(coverage)) {
    assert.ok(count > 0, `random trace did not cover ${operation}`);
  }
});
