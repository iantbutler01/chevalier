#!/usr/bin/env python3

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat


def fail(message):
    raise ValueError(message)


def path_under(root, raw):
    candidate = PurePosixPath(raw)
    if candidate.is_absolute() or not candidate.parts:
        fail(f"path must be a non-empty relative path: {raw!r}")
    if any(part in ("", ".", "..") for part in candidate.parts):
        fail(f"path contains an unsafe component: {raw!r}")
    return root.joinpath(*candidate.parts)


def canonical_stat(value):
    mode = value.st_mode
    if stat.S_ISREG(mode):
        kind = "file"
    elif stat.S_ISDIR(mode):
        kind = "directory"
    elif stat.S_ISLNK(mode):
        kind = "symlink"
    else:
        kind = "other"
    result = {
        "kind": kind,
        "mode": stat.S_IMODE(mode),
    }
    if kind == "file":
        result["size"] = value.st_size
    return result


def fsync_parent(path):
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path.parent, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_all(descriptor, data):
    offset = 0
    while offset < len(data):
        written = os.write(descriptor, data[offset:])
        if written <= 0:
            raise OSError(errno.EIO, "write returned no progress")
        offset += written


def pwrite_all(descriptor, data, offset):
    written = 0
    while written < len(data):
        count = os.pwrite(descriptor, data[written:], offset + written)
        if count <= 0:
            raise OSError(errno.EIO, "pwrite returned no progress")
        written += count


def read_all(descriptor):
    size = os.fstat(descriptor).st_size
    chunks = []
    offset = 0
    while offset < size:
        chunk = os.pread(descriptor, min(65536, size - offset), offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    return b"".join(chunks)


def canonical_bytes(data):
    return {
        "length": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "prefixHex": data[:64].hex(),
        "suffixHex": data[-64:].hex() if len(data) > 64 else data.hex(),
    }


def apply_action(root, action):
    operation = action["op"]
    path = path_under(root, action["path"]) if "path" in action else None

    if operation == "mkdir":
        os.mkdir(path, int(action["mode"]))
        fsync_parent(path)
        return {"stat": canonical_stat(os.stat(path))}

    if operation == "rmdir":
        parent = path.parent
        os.rmdir(path)
        fsync_parent(parent / path.name)
        return {"removed": True}

    if operation == "create":
        descriptor = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_RDWR,
            int(action["mode"]),
        )
        try:
            data = bytes.fromhex(action.get("dataHex", ""))
            write_all(descriptor, data)
            os.fsync(descriptor)
            result = canonical_stat(os.fstat(descriptor))
        finally:
            os.close(descriptor)
        fsync_parent(path)
        return {"stat": result}

    if operation == "read":
        descriptor = os.open(path, os.O_RDONLY)
        try:
            offset = int(action.get("offset", 0))
            length = int(action.get("length", os.fstat(descriptor).st_size))
            data = os.pread(descriptor, length, offset)
            result = canonical_stat(os.fstat(descriptor))
        finally:
            os.close(descriptor)
        return {"data": canonical_bytes(data), "stat": result}

    if operation == "write":
        descriptor = os.open(path, os.O_WRONLY | os.O_TRUNC)
        try:
            write_all(descriptor, bytes.fromhex(action["dataHex"]))
            os.fsync(descriptor)
            result = canonical_stat(os.fstat(descriptor))
        finally:
            os.close(descriptor)
        fsync_parent(path)
        return {"stat": result}

    if operation == "pwrite":
        descriptor = os.open(path, os.O_RDWR)
        try:
            pwrite_all(
                descriptor,
                bytes.fromhex(action["dataHex"]),
                int(action["offset"]),
            )
            os.fsync(descriptor)
            result = canonical_stat(os.fstat(descriptor))
        finally:
            os.close(descriptor)
        return {"stat": result}

    if operation == "truncate":
        descriptor = os.open(path, os.O_RDWR)
        try:
            os.ftruncate(descriptor, int(action["size"]))
            os.fsync(descriptor)
            result = canonical_stat(os.fstat(descriptor))
        finally:
            os.close(descriptor)
        return {"stat": result}

    if operation == "sparse":
        descriptor = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_RDWR,
            int(action["mode"]),
        )
        try:
            os.ftruncate(descriptor, int(action["size"]))
            pwrite_all(
                descriptor,
                bytes.fromhex(action["dataHex"]),
                int(action["offset"]),
            )
            os.fsync(descriptor)
            result = canonical_stat(os.fstat(descriptor))
        finally:
            os.close(descriptor)
        fsync_parent(path)
        return {"stat": result}

    if operation == "stat":
        return {"stat": canonical_stat(os.stat(path))}

    if operation == "chmod":
        os.chmod(path, int(action["mode"]))
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
            result = canonical_stat(os.fstat(descriptor))
        finally:
            os.close(descriptor)
        return {"stat": result}

    if operation == "symlink":
        os.symlink(action["target"], path)
        fsync_parent(path)
        return {"target": os.readlink(path)}

    if operation == "readlink":
        return {"target": os.readlink(path)}

    if operation == "rename_overwrite":
        destination = path_under(root, action["destination"])
        source_parent = path.parent
        destination_parent = destination.parent
        os.replace(path, destination)
        fsync_parent(source_parent / path.name)
        if destination_parent != source_parent:
            fsync_parent(destination_parent / destination.name)
        return {"stat": canonical_stat(os.lstat(destination))}

    if operation == "unlink":
        parent = path.parent
        os.unlink(path)
        fsync_parent(parent / path.name)
        return {"removed": True}

    if operation == "open_unlink":
        descriptor = os.open(path, os.O_RDWR)
        try:
            before = read_all(descriptor)
            os.unlink(path)
            if os.path.lexists(path):
                fail("unlinked path remained visible")
            suffix = bytes.fromhex(action.get("suffixHex", ""))
            pwrite_all(descriptor, suffix, len(before))
            os.fsync(descriptor)
            after = read_all(descriptor)
        finally:
            os.close(descriptor)
        fsync_parent(path)
        if os.path.lexists(path):
            fail("unlinked path reappeared after close")
        return {
            "before": canonical_bytes(before),
            "after": canonical_bytes(after),
        }

    fail(f"unsupported operation: {operation!r}")


def snapshot(root):
    entries = []

    def walk(directory, prefix):
        with os.scandir(directory) as iterator:
            children = sorted(iterator, key=lambda entry: entry.name)
        for child in children:
            relative = f"{prefix}/{child.name}" if prefix else child.name
            value = child.stat(follow_symlinks=False)
            item = {
                "path": relative,
                **canonical_stat(value),
            }
            if stat.S_ISLNK(value.st_mode):
                item["target"] = os.readlink(child.path)
            elif stat.S_ISREG(value.st_mode):
                descriptor = os.open(child.path, os.O_RDONLY)
                try:
                    data = read_all(descriptor)
                finally:
                    os.close(descriptor)
                item["content"] = canonical_bytes(data)
            entries.append(item)
            if stat.S_ISDIR(value.st_mode):
                walk(Path(child.path), relative)

    walk(root, "")
    return entries


def main():
    parser = argparse.ArgumentParser(
        description="Apply one canonical POSIX action or snapshot a disposable tree."
    )
    parser.add_argument("--root", required=True)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--action")
    selection.add_argument("--snapshot", action="store_true")
    arguments = parser.parse_args()

    os.umask(0o022)
    root = Path(arguments.root)
    if arguments.snapshot:
        result = {"snapshot": snapshot(root)}
    else:
        action = json.loads(arguments.action)
        result = {"result": apply_action(root, action)}
    print(json.dumps({"ok": True, **result}, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        failure = {
            "ok": False,
            "error": type(error).__name__,
            "message": str(error),
        }
        print(json.dumps(failure, sort_keys=True, separators=(",", ":")))
        raise
