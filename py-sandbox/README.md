# chevalier-sandbox

Python bindings for the Chevalier sandbox client. This package connects to a sandbox provider; it does not start one.

```bash
pip install chevalier-sandbox
```

```python
from chevalier_sandbox import Sandbox

sandbox = await Sandbox.connect(
    "http://127.0.0.1:50051",
    {"default_image": "chevalier-base"},
)
session = await sandbox.session({"name": "example"})

process = await session.exec("printf hello")
while (event := await process.next()) is not None:
    if event["type"] == "stdout":
        print(event["data"].decode(), end="")

await session.close()
```

The package mirrors the separate TypeScript `chevalier-sandbox` binding: sessions, exec and PTY streams, files, forks, checkpoints, snapshots, lifecycle operations, port forwarding, durable volumes, and PCI operations. The full typed surface is in `chevalier_sandbox/__init__.pyi`.
