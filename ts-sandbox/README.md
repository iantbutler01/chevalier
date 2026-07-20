# chevalier-sandbox

Node bindings for the Chevalier sandbox facade. This package is separate from [`chevalier`](../ts) so normal TypeScript users do not pull in the sandbox client unless they need VM/sandbox work.

```bash
npm install chevalier-sandbox
```

## Providers

The same `Sandbox` API can connect to:

- a Chevalier `vmd` endpoint
- OpenComputer, selected by config

```ts
import { Sandbox } from "chevalier-sandbox";

const sb = await Sandbox.connect("http://127.0.0.1:8052", {
  authToken: process.env.CHEVALIER_SANDBOX_AUTH_TOKEN,
});
```

OpenComputer:

```ts
const sb = await Sandbox.connect("opencomputer", {
  provider: "opencomputer",
  openComputer: {
    apiKey: process.env.OPENCOMPUTER_API_KEY,
    templateId: "base",
    egressAllowlist: ["api.anthropic.com", "*.openai.com"],
  },
});
```

## Sessions

```ts
const sess = await sb.session({
  name: "research",
  image: "ubuntu:22.04",
  autoStart: true,
});

const ex = await sess.exec("cat", { closeStdinOnStart: false });
await ex.write(Buffer.from("hello\n"));
await ex.eof();

for (;;) {
  const event = await ex.next();
  if (!event) break;
  if (event.type === "stdout") process.stdout.write(event.data);
  if (event.type === "exit") console.log("exit", event.code);
}

await sess.writeFile("/tmp/note.txt", Buffer.from("data"));
const bytes = await sess.readFile("/tmp/note.txt");
const entries = await sess.listDir("/tmp");
const child = await sess.fork({ childName: "branch" });
const again = await sb.attachSession(sess.sessionId);
```

## Current Surface

Exposed today:

- `Sandbox.connect`
- `session`
- `attachSession`
- bidirectional `exec`
- `readFile`
- `listDir`
- `writeFile`
- `fork`
- OpenComputer config: API URL/key, template, resources, burst, secret store, egress allowlist, mounts, shared mounts

Not exposed in this TS package yet:

- interactive shell handle
- port forwarding
- snapshot/restore helpers

Use the Rust crate directly if you need a lower-level surface before the binding catches up.

## Build

```bash
npm install
npm run build
```

## License

Apache-2.0
