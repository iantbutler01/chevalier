#!/usr/bin/env node

import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { mkdir, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath, pathToFileURL } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");

const assert = (condition, message) => {
  if (!condition) throw new Error(message);
};

const requestBody = async (incoming) => {
  const chunks = [];
  for await (const chunk of incoming) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
};

export const startNodeVfsGateway = async ({ handleRequest, bind = "127.0.0.1", port = 0 }) => {
  let requestCount = 0;
  const server = createServer(async (incoming, outgoing) => {
    try {
      const method = incoming.method || "GET";
      const body =
        method === "GET" || method === "HEAD" ? undefined : await requestBody(incoming);
      const request = new Request(
        new URL(incoming.url || "/", `http://${incoming.headers.host || "localhost"}`),
        {
          method,
          headers: incoming.headers,
          body,
          ...(body === undefined ? {} : { duplex: "half" }),
        },
      );
      const response = await handleRequest(request);
      requestCount += 1;
      outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
      outgoing.end(Buffer.from(await response.arrayBuffer()));
    } catch (error) {
      outgoing.writeHead(500, { "content-type": "text/plain" });
      outgoing.end(error instanceof Error ? error.stack || error.message : String(error));
    }
  });
  await new Promise((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(port, bind, resolveListen);
  });
  const address = server.address();
  assert(address && typeof address === "object", "gateway did not expose a TCP address");
  return {
    server,
    endpoint: `http://${bind}:${address.port}`,
    requestCount: () => requestCount,
    close: () =>
      new Promise((resolveClose, reject) =>
        server.close((error) => (error ? reject(error) : resolveClose())),
      ),
  };
};

export async function runVfsGatewayProtocolProbe({
  ownerEndpoint,
  authToken,
  scopePath,
}) {
  const scope = scopePath.replace(/^\/+|\/+$/g, "");
  assert(scope !== "", "protocol probe requires a non-empty scope path");
  const prefix = `${scope}/.chevalier-protocol-probe-${randomUUID()}`;
  const source = `${prefix}/source`;
  const alias = `${prefix}/alias`;
  const lockPath = `${prefix}/lock`;
  const auth = { authorization: `Bearer ${authToken}` };
  const mutationHeaders = (lease, operation) => ({
    ...auth,
    "x-chevalier-vfs-component": "vm_runtime",
    "x-chevalier-vfs-surface-kind": "project",
    "x-chevalier-vfs-operation": operation,
    "x-chevalier-vfs-resource-key": lease.resource_key,
    "x-chevalier-vfs-lock-owner-token": lease.owner_token,
  });
  const call = async (suffix, init = {}, expected = 200) => {
    const response = await fetch(`${ownerEndpoint}${suffix}`, init);
    const bytes = Buffer.from(await response.arrayBuffer());
    const accepted = Array.isArray(expected)
      ? expected.includes(response.status)
      : response.status === expected;
    if (!accepted) {
      throw new Error(
        `${init.method || "GET"} ${suffix}: expected ${expected}, got ${response.status}: ${bytes}`,
      );
    }
    return {
      status: response.status,
      bytes,
      json: () => JSON.parse(bytes.toString("utf8")),
    };
  };
  const lease = async (path, mutationCount = 1) =>
    (
      await call(
        "/lease",
        {
          method: "POST",
          headers: { ...auth, "content-type": "application/json" },
          body: JSON.stringify({
            path,
            mutation_count: mutationCount,
            component: "vm_runtime",
            reason: "gateway protocol conformance",
          }),
        },
        200,
      )
    ).json();
  const releaseLease = async (grant) =>
    call(
      "/lease",
      {
        method: "DELETE",
        headers: { ...auth, "content-type": "application/json" },
        body: JSON.stringify({
          resource_key: grant.resource_key,
          owner_token: grant.owner_token,
        }),
      },
      204,
    );
  const write = async (path, bytes) => {
    const grant = await lease(path);
    try {
      return await call(
        `/file?path=${encodeURIComponent(path)}`,
        {
          method: "PUT",
          headers: {
            ...mutationHeaders(grant, "vfs_write"),
            "x-chevalier-vfs-executable": "false",
          },
          body: bytes,
        },
        200,
      );
    } finally {
      await releaseLease(grant);
    }
  };
  const remove = async (path) => {
    const grant = await lease(path);
    try {
      await call(
        `/file?path=${encodeURIComponent(path)}`,
        {
          method: "DELETE",
          headers: mutationHeaders(grant, "vfs_unlink"),
        },
        200,
      );
    } finally {
      await releaseLease(grant);
    }
  };
  const lock = async (body) =>
    (
      await call(
        "/posix-lock/v1",
        {
          method: "POST",
          headers: { ...auth, "content-type": "application/json" },
          body: JSON.stringify(body),
        },
        200,
      )
    ).json();

  await call(`/stat?path=${encodeURIComponent(source)}`, {}, 401);
  await write(source, Buffer.from("source-v1"));
  await write(lockPath, Buffer.alloc(32));

  const sourceStat = (
    await call(`/stat?path=${encodeURIComponent(source)}`, { headers: auth }, 200)
  ).json();
  assert(sourceStat.kind === "file", "scoped source did not stat as a file");
  assert(
    typeof sourceStat.file_id === "string" && sourceStat.file_id !== "",
    "missing stable file_id",
  );

  const linkGrant = await lease(prefix, 1);
  let linked;
  try {
    linked = (
      await call(
        "/hard-link/v1",
        {
          method: "POST",
          headers: {
            ...mutationHeaders(linkGrant, "vfs_hard_link"),
            "content-type": "application/json",
          },
          body: JSON.stringify({
            source_path: source,
            destination_path: alias,
          }),
        },
        200,
      )
    ).json();
  } finally {
    await releaseLease(linkGrant);
  }
  assert(linked.source.file_id === linked.destination.file_id, "hard-link file identity diverged");
  assert(
    linked.source.link_count === 2 && linked.destination.link_count === 2,
    "hard-link count is not 2",
  );
  const resolvedAlias = (
    await call(
      "/hard-link-alias/v1",
      {
        method: "POST",
        headers: { ...auth, "content-type": "application/json" },
        body: JSON.stringify({
          file_id: linked.source.file_id,
          excluding_path: source,
        }),
      },
      200,
    )
  ).json();
  assert(resolvedAlias.path === alias, `hard-link alias escaped scope: ${resolvedAlias.path}`);
  await write(alias, Buffer.from("alias-mutated"));
  const sharedBytes = await call(
    `/file/raw?path=${encodeURIComponent(source)}`,
    { headers: auth },
    200,
  );
  assert(sharedBytes.bytes.toString("utf8") === "alias-mutated", "hard-link mutation did not alias");

  const mountA = `mount-a-${randomUUID()}`;
  const mountB = `mount-b-${randomUUID()}`;
  const sharedOwner = "guest-owner-42";
  const baseLock = {
    path: lockPath,
    lock_owner: sharedOwner,
    namespace: "posix",
    start: "0",
    end: "7",
    kind: "write",
    pid: 42,
  };
  const acquiredA = await lock({ ...baseLock, action: "set", mount_id: mountA });
  assert(acquiredA.acquired === true && acquiredA.file_id, "mount A did not acquire POSIX lock");
  const conflictB = await lock({ ...baseLock, action: "get", mount_id: mountB });
  assert(
    conflictB.acquired === false && conflictB.conflict?.pid === 42,
    "mount IDs were not independent",
  );
  const flockB = await lock({
    ...baseLock,
    action: "set",
    mount_id: mountB,
    namespace: "flock",
  });
  assert(flockB.acquired === true, "flock namespace incorrectly conflicted with POSIX");
  await lock({ action: "renew_mount", mount_id: mountA });
  const unlockedA = await lock({
    ...baseLock,
    action: "set",
    mount_id: mountA,
    kind: "unlock",
  });
  assert(unlockedA.acquired === true, "POSIX unlock failed");
  const acquiredB = await lock({ ...baseLock, action: "set", mount_id: mountB });
  assert(acquiredB.acquired === true, "mount B could not acquire after unlock");
  await lock({
    action: "release_owner",
    mount_id: mountB,
    lock_owner: sharedOwner,
    namespace: "posix",
    file_id: acquiredB.file_id,
  });
  const reacquiredA = await lock({ ...baseLock, action: "set", mount_id: mountA });
  assert(reacquiredA.acquired === true, "release_owner did not release POSIX lock");
  await lock({ action: "release_mount", mount_id: mountA });
  const reacquiredB = await lock({ ...baseLock, action: "set", mount_id: mountB });
  assert(reacquiredB.acquired === true, "release_mount did not release POSIX lock");
  await lock({ action: "release_mount", mount_id: mountB });

  await remove(source);
  const aliasAfterUnlink = (
    await call(`/stat?path=${encodeURIComponent(alias)}`, { headers: auth }, 200)
  ).json();
  assert(aliasAfterUnlink.link_count === 1, "unlink did not decrement surviving hard-link count");
  await remove(alias);
  await remove(lockPath);

  return {
    ownerEndpoint,
    scopePath: scope,
    scopedPrefix: prefix,
    authentication: "401 enforced",
    hardLinks: {
      fileId: linked.source.file_id,
      sharedIdentity: true,
      mutationAliased: true,
      aliasResolutionScoped: true,
      unlinkPreservedAlias: true,
    },
    advisoryLocks: {
      mountA,
      mountB,
      independentMountIdentities: true,
      posixConflict: true,
      flockNamespaceIndependent: true,
      renewMount: true,
      unlock: true,
      releaseOwner: true,
      releaseMount: true,
    },
  };
}

async function main() {
  const require = createRequire(import.meta.url);
  const chevalierPath =
    process.env.CHEVALIER_MODULE_PATH?.trim() || join(repoRoot, "ts", "index.js");
  const { createVfsGatewayServer, VfsStorage } = require(resolve(chevalierPath));
  const root = join(tmpdir(), `chevalier-gateway-protocol-${randomUUID()}`);
  const ownerId = `dry-${randomUUID()}`;
  const token = randomUUID();
  await mkdir(root, { recursive: true });
  const storage = VfsStorage.local(root);
  const handleRequest = createVfsGatewayServer({
    resolveStore: async (requestedOwner) => {
      if (requestedOwner !== ownerId) throw new Error(`unexpected owner: ${requestedOwner}`);
      return storage;
    },
    authToken: token,
    allowGitMetadata: async (requestedOwner) => requestedOwner === ownerId,
  });
  const gateway = await startNodeVfsGateway({ handleRequest });
  try {
    const ownerEndpoint = `${gateway.endpoint}/internal/chevalier/vfs/${encodeURIComponent(ownerId)}`;
    const evidence = await runVfsGatewayProtocolProbe({
      ownerEndpoint,
      authToken: token,
      scopePath: `scopes/${randomUUID()}/repo`,
    });
    console.log(JSON.stringify({ ok: true, requests: gateway.requestCount(), ...evidence }, null, 2));
  } finally {
    await gateway.close();
    await rm(root, { recursive: true, force: true });
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] || "").href) {
  await main();
}
