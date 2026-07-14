const readline = require("node:readline");

const lines = readline.createInterface({ input: process.stdin });

function reply(id, result, exitAfterReply = false) {
  process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id, result })}\n`, () => {
    if (exitAfterReply) process.exit(0);
  });
}

lines.on("line", (line) => {
  const message = JSON.parse(line);

  if (message.method === "initialize") {
    reply(message.id, {
      protocolVersion: message.params.protocolVersion,
      capabilities: { tools: {} },
      serverInfo: { name: "chevalier-stdio-test", version: "0.0.0" },
    });
    return;
  }

  if (message.method === "tools/list") {
    reply(message.id, {
      tools: [
        {
          name: "config_report",
          description: "Report child process configuration",
          inputSchema: { type: "object", properties: {} },
        },
      ],
    });
    return;
  }

  if (message.method === "tools/call") {
    reply(
      message.id,
      {
        content: [
          {
            type: "text",
            text: JSON.stringify({
              args: process.argv.slice(2),
              cwd: process.cwd(),
              secret: process.env.CHEVALIER_MCP_TEST_SECRET,
            }),
          },
        ],
        isError: false,
      },
      true,
    );
  }
});
