"""Synthetic, read-only VFS data for the explicitly gated scratch-VM test."""
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class Vfs(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        path = parse_qs(url.query).get("path", [""])[0].strip("/")
        if path == "slow":
            time.sleep(30)
        entry = {"kind": "directory", "size_bytes": 0, "content_hash": None,
                 "updated_at": None, "mode": 493}
        if url.path == "/tree":
            names = {"root": ["vm"], "root/vm": ["mounts"],
                     "root/vm/mounts": ["shared", "task", "skills", "slow"]}.get(path, [])
            self.reply([dict(entry, name=name) for name in names])
        elif url.path == "/stat":
            self.reply(entry)
        else:
            self.send_error(404)

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path == "/prefetch-subtree":
            self.reply({"warmed_file_bytes": []})
        else:
            self.send_error(404)

    def reply(self, body):
        body = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("x-chevalier-vfs-namespace-revision", "1")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


ThreadingHTTPServer(("127.0.0.1", 18991), Vfs).serve_forever()
