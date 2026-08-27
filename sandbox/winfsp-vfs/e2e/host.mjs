#!/usr/bin/env node

import { timingSafeEqual } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { startNodeVfsGateway } from "../../scripts/vfs-gateway-protocol-probe.mjs";

const required = (name) => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
};

const gatewayToken = required("OPENBRACKET_WINFSP_GATEWAY_TOKEN");
const resultToken = required("OPENBRACKET_WINFSP_RESULT_TOKEN");
const storageRoot = resolve(required("OPENBRACKET_WINFSP_STORAGE_ROOT"));
const statusFile = resolve(required("OPENBRACKET_WINFSP_STATUS_FILE"));
const owner = required("OPENBRACKET_WINFSP_OWNER");
const scope = required("OPENBRACKET_WINFSP_SCOPE");
const gatewayPort = Number(required("OPENBRACKET_WINFSP_GATEWAY_PORT"));
const resultPort = Number(required("OPENBRACKET_WINFSP_RESULT_PORT"));
const hotpatchArtifact = process.env.OPENBRACKET_WINFSP_HOTPATCH_ARTIFACT?.trim();
const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../../..");
const require = createRequire(import.meta.url);
const { createVfsGatewayServer, VfsStorage } = require(join(repoRoot, "ts", "index.js"));

await mkdir(join(storageRoot, ...scope.split("/")), { recursive: true });
await writeFile(join(storageRoot, ...scope.split("/"), "seed.txt"), "seed-from-gateway", {
  flag: "wx",
});

let online = true;
const gatewayHandler = createVfsGatewayServer({
  resolveStore: async (requestedOwner) => {
    if (requestedOwner !== owner) throw new Error(`unexpected VFS owner ${requestedOwner}`);
    return VfsStorage.local(storageRoot);
  },
  authToken: gatewayToken,
  allowGitMetadata: async (requestedOwner) => requestedOwner === owner,
});
const gateway = await startNodeVfsGateway({
  bind: "127.0.0.1",
  port: gatewayPort,
  handleRequest: async (request) =>
    online
      ? gatewayHandler(request)
      : new Response(JSON.stringify({ error: "deliberate WinFsp E2E outage" }), {
          status: 503,
          headers: { "content-type": "application/json" },
        }),
});

const expectedAuthorization = Buffer.from(`Bearer ${resultToken}`);
const phases = [];
const resultServer = createServer((request, response) => {
  const authorization = Buffer.from(request.headers.authorization ?? "");
  const authorized =
    authorization.length === expectedAuthorization.length &&
    timingSafeEqual(authorization, expectedAuthorization);
  if (authorized && request.method === "GET" && request.url === "/hotpatch") {
    if (!hotpatchArtifact) {
      response.writeHead(404).end();
      return;
    }
    readFile(resolve(hotpatchArtifact)).then(
      (body) => {
        response.writeHead(200, {
          "content-length": body.length,
          "content-type": "application/octet-stream",
        });
        response.end(body);
      },
      (error) => response.writeHead(500).end(String(error)),
    );
    return;
  }
  if (!authorized || request.method !== "POST" || request.url !== "/phase") {
    response.writeHead(403).end();
    return;
  }
  let body = "";
  request.setEncoding("utf8");
  request.on("data", (chunk) => {
    body += chunk;
    if (body.length > 64 * 1024) request.destroy();
  });
  request.on("end", async () => {
    try {
      const receipt = JSON.parse(body);
      if (typeof receipt.phase !== "string") throw new Error("phase is required");
      phases.push(receipt);
      console.log(JSON.stringify({ receipt: receipt.phase }));
      if (receipt.phase === "online-pass") online = false;
      if (receipt.phase === "offline-recovered") online = true;
      if (receipt.phase === "success" || receipt.phase === "failure") {
        const temporary = `${statusFile}.${process.pid}`;
        await writeFile(temporary, JSON.stringify({ ...receipt, phases }, null, 2), {
          mode: 0o600,
        });
        await rename(temporary, statusFile);
      }
      response.writeHead(204).end();
    } catch (error) {
      response.writeHead(400).end(String(error));
    }
  });
});
await new Promise((resolveListen, rejectListen) => {
  resultServer.once("error", rejectListen);
  resultServer.listen(resultPort, "127.0.0.1", resolveListen);
});

console.log(JSON.stringify({ gateway: gateway.endpoint, resultPort, owner, scope }));

await new Promise((resolveSignal) => {
  process.once("SIGINT", resolveSignal);
  process.once("SIGTERM", resolveSignal);
});
await gateway.close();
await new Promise((resolveClose) => resultServer.close(resolveClose));

if (phases.at(-1)?.phase === "success") {
  const expected = new Map([
    ["online.txt", "online-published"],
    ["renamed.txt", "renamed-online"],
    ["offline-renamed.txt", "offline-durable"],
  ]);
  for (const [relative, bytes] of expected) {
    const actual = await readFile(join(storageRoot, ...scope.split("/"), relative), "utf8");
    if (actual !== bytes) throw new Error(`published ${relative} had unexpected bytes ${JSON.stringify(actual)}`);
  }
}
