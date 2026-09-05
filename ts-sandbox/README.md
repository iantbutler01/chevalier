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

- interactive shell handle
- port forwarding
- snapshot/restore helpers
- distributed discovery, placement, and command routing

## Distributed VMD

`Sandbox.connect` accepts `distributedControl`. The client discovers workers in
etcd and routes commands through NATS JetStream; there is no separate controller
binary to deploy. Workers need matching etcd prefixes, NATS cluster settings,
authentication, and unique stable advertised endpoints. The client must reach
each worker's advertised host, including dynamic guest RPC/forwarding ports.

```ts
const sb = await Sandbox.connect("http://worker-a:8052", {
  defaultImage: "registry.example/sandbox@sha256:...",
  authToken: process.env.CHEVALIER_SANDBOX_AUTH_TOKEN,
  distributedControl: {
    etcdEndpoints: ["http://etcd:2379"],
    natsUrl: "nats://nats:4222",
    natsAuthToken: process.env.NATS_AUTH_TOKEN,
    requiredContinuityTier: "tier-a",
    allowTierADegraded: true,
    allowCrossNodeRecovery: false,
  },
});
```

For node-bound disks, set `allowCrossNodeRecovery: false` and session metadata
`"chevalier.tier_b_eligible": "false"`. An unavailable recorded owner then stays
an error; a lookup miss on another worker cannot replace it. Existing recovery
behavior remains the default for callers that omit the policy. Tier-A workers
must explicitly advertise `CHEVALIER_SANDBOX_NODE_DEGRADED_MODE=true` to be
admitted by the distributed scheduler. This policy does not provide disk HA.

Build `sandbox/Dockerfile` from the repository root for Linux VMD. It requires
KVM, FUSE, privileged VM networking, persistent node storage, and a Docker daemon
for Docker-to-VM conversion. With a separate Docker daemon, mount the VMD data
directory at the same absolute path in both containers. Use `--force-local-build`
when private Docker images have no corresponding prebuilt VM registry artifacts.

The real owner-unavailable regression is `node test/distributed-node-bound.cjs`.
Set `SANDBOX_TEST_NODE_ENDPOINT`, `SANDBOX_TEST_ETCD_HTTP_URL`,
`SANDBOX_TEST_NATS_URL`, and the optional `SANDBOX_TEST_AUTH_TOKEN` and
`SANDBOX_TEST_NATS_AUTH_TOKEN`. It uses a unique etcd prefix and deletes it afterward.

## Build

```bash
npm install
npm run build
```

## License

Apache-2.0
