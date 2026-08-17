#!/usr/bin/env node

import { mkdir, readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { startNodeVfsGateway } from "./vfs-gateway-protocol-probe.mjs";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");
const require = createRequire(import.meta.url);
const chevalierPath =
  process.env.CHEVALIER_MODULE_PATH?.trim() || join(repoRoot, "ts", "index.js");
const { createVfsGatewayServer, VfsStorage } = require(resolve(chevalierPath));

const tokenFile = process.env.CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN_FILE?.trim();
const authToken =
  process.env.CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN?.trim() ||
  (tokenFile ? (await readFile(resolve(tokenFile), "utf8")).trim() : undefined);
if (!authToken) {
  throw new Error(
    "CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN or its _FILE variant is required",
  );
}

const port = Number(process.env.CHEVALIER_MACOS_VFS_GATEWAY_PORT ?? "63339");
if (!Number.isInteger(port) || port < 1 || port > 65_535) {
  throw new Error("CHEVALIER_MACOS_VFS_GATEWAY_PORT must be an integer in 1..65535");
}

const ownerId = process.env.CHEVALIER_MACOS_VFS_OWNER?.trim() || "macos-vz-dev";
const storageRoot = resolve(
  process.env.CHEVALIER_MACOS_VFS_STORAGE_ROOT?.trim() ||
    join(homedir(), "Library", "Application Support", "OpenBracket", "vz", "vfs-fixture"),
);
await mkdir(storageRoot, { recursive: true });

const storage = VfsStorage.local(storageRoot);
const gateway = await startNodeVfsGateway({
  bind: "127.0.0.1",
  port,
  handleRequest: createVfsGatewayServer({
    resolveStore: async (requestedOwner) => {
      if (requestedOwner !== ownerId) {
        throw new Error(`unexpected VFS owner: ${requestedOwner}`);
      }
      return storage;
    },
    authToken,
    allowGitMetadata: async (requestedOwner) => requestedOwner === ownerId,
  }),
});

console.log(
  JSON.stringify({
    endpoint: `${gateway.endpoint}/internal/chevalier/vfs/${encodeURIComponent(ownerId)}`,
    ownerId,
    storageRoot,
  }),
);

await new Promise((resolveSignal) => {
  process.once("SIGINT", resolveSignal);
  process.once("SIGTERM", resolveSignal);
});
await gateway.close();
