#!/usr/bin/env node

import { createServer } from "node:http";
import { mkdir } from "node:fs/promises";
import { createRequire } from "node:module";
import { resolve } from "node:path";

const required = (name) => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required for the disposable gateway child`);
  return value;
};

const ownerId = required("CHEVALIER_VFS_FAULT_CHILD_OWNER_ID");
const ownerRoot = required("CHEVALIER_VFS_FAULT_CHILD_OWNER_ROOT");
const authToken = required("CHEVALIER_VFS_FAULT_CHILD_AUTH_TOKEN");
const bind = required("CHEVALIER_VFS_FAULT_CHILD_BIND");
const port = Number(required("CHEVALIER_VFS_FAULT_CHILD_PORT"));
const modulePath = required("CHEVALIER_VFS_FAULT_CHILD_MODULE_PATH");

if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error("CHEVALIER_VFS_FAULT_CHILD_PORT must be an integer in 1..65535");
}

const require = createRequire(import.meta.url);
const { createVfsGatewayServer, VfsStorage } = require(resolve(modulePath));
if (
  typeof createVfsGatewayServer !== "function" ||
  typeof VfsStorage?.local !== "function"
) {
  throw new Error("Chevalier module does not expose the VFS gateway APIs");
}

await mkdir(ownerRoot, { recursive: true });
const storage = VfsStorage.local(ownerRoot);
const handleGatewayRequest = createVfsGatewayServer({
  resolveStore: async (requestedOwner) => {
    if (requestedOwner !== ownerId) throw new Error(`unexpected owner: ${requestedOwner}`);
    return storage;
  },
  authToken,
  allowGitMetadata: async (requestedOwner) => requestedOwner === ownerId,
});

let mode = "online";
let pausedResolvers = [];

const resumePausedRequests = () => {
  const resolvers = pausedResolvers;
  pausedResolvers = [];
  for (const resolvePaused of resolvers) resolvePaused();
};

process.on("message", (message) => {
  if (!message || message.type !== "mode") return;
  mode = message.mode;
  if (mode === "online") resumePausedRequests();
  process.send?.({ type: "mode", mode });
});

const readRequestBody = async (request) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
};

const server = createServer(async (incoming, outgoing) => {
  try {
    const injected = mode !== "online";
    process.send?.({ type: "request", injected });
    if (mode === "reject") {
      outgoing.writeHead(503, {
        "content-type": "text/plain",
        connection: "close",
      });
      outgoing.end("injected disposable gateway outage");
      return;
    }
    if (mode === "paused") {
      process.send?.({ type: "paused-request" });
      await new Promise((resolvePaused) => pausedResolvers.push(resolvePaused));
    }
    const method = incoming.method || "GET";
    const body =
      method === "GET" || method === "HEAD" ? undefined : await readRequestBody(incoming);
    const request = new Request(
      new URL(incoming.url || "/", `http://${incoming.headers.host || "localhost"}`),
      {
        method,
        headers: incoming.headers,
        body,
        ...(body === undefined ? {} : { duplex: "half" }),
      },
    );
    const response = await handleGatewayRequest(request);
    outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
    outgoing.end(Buffer.from(await response.arrayBuffer()));
  } catch (error) {
    if (!outgoing.headersSent) {
      outgoing.writeHead(500, { "content-type": "text/plain" });
    }
    outgoing.end(error instanceof Error ? error.message : String(error));
  }
});

const stopWithParent = () => {
  const forced = setTimeout(() => process.exit(0), 2_000);
  forced.unref();
  server.close(() => process.exit(0));
  server.closeAllConnections?.();
};
process.once("disconnect", stopWithParent);
process.once("SIGTERM", stopWithParent);
process.once("SIGINT", stopWithParent);

server.once("error", (error) => {
  process.send?.({ type: "fatal", error: error.message });
  process.exitCode = 1;
});
server.listen(port, bind, () => {
  process.send?.({ type: "ready" });
});
