import { timingSafeEqual } from "node:crypto";
import { closeSync, fsyncSync, openSync, renameSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";

const port = Number.parseInt(process.env.OPENBRACKET_RECEIPT_PORT ?? "", 10);
const statusFile = process.env.OPENBRACKET_RECEIPT_STATUS_FILE;
const token = process.env.OPENBRACKET_RECEIPT_TOKEN;

if (!Number.isInteger(port) || port < 1 || port > 65535 || !statusFile || !token) {
  process.exit(2);
}

const expectedAuthorization = Buffer.from(`Bearer ${token}`);
let acceptedStatus;

const server = createServer((request, response) => {
  const authorization = Buffer.from(request.headers.authorization ?? "");
  const authorized =
    authorization.length === expectedAuthorization.length &&
    timingSafeEqual(authorization, expectedAuthorization);
  if (request.method !== "POST" || request.url !== "/receipt" || !authorized) {
    response.writeHead(403).end();
    return;
  }

  let body = "";
  request.setEncoding("utf8");
  request.on("data", (chunk) => {
    body += chunk;
    if (body.length > 4096) request.destroy();
  });
  request.on("end", () => {
    let receipt;
    try {
      receipt = request.headers["content-type"]?.startsWith("application/json")
        ? JSON.parse(body)
        : { status: body };
    } catch {
      response.writeHead(400).end();
      return;
    }
    const status = receipt?.status;
    if (status !== "verified" && status !== "success" && status !== "failure") {
      response.writeHead(400).end();
      return;
    }

    const transitionAllowed =
      acceptedStatus === undefined ||
      (acceptedStatus === "verified" && (status === "success" || status === "failure"));
    if (!transitionAllowed) {
      response.writeHead(409).end();
      return;
    }

    const temporary = `${statusFile}.${process.pid}`;
    writeFileSync(temporary, status, { encoding: "utf8", mode: 0o600 });
    const descriptor = openSync(temporary, "r");
    try {
      fsyncSync(descriptor);
    } finally {
      closeSync(descriptor);
    }
    renameSync(temporary, statusFile);
    acceptedStatus = status;
    if (status === "failure") {
      const stage = String(receipt.stage ?? "unknown").replace(/[\r\n\t]/g, " ").slice(0, 128);
      const message = String(receipt.message ?? "unspecified error").replace(/[\r\n\t]/g, " ").slice(0, 2048);
      console.error(`Windows image build failed during ${stage}: ${message}`);
    }
    response.writeHead(204).end();
  });
});

server.listen(port, "127.0.0.1");
