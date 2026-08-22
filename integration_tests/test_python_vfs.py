import asyncio

import chevalier


def test_local_vfs_real_filesystem_contract(tmp_path):
    async def scenario():
        storage = chevalier.VfsStorage.local(str(tmp_path))
        await storage.mkdir("dir", {"mode": 0o750})
        write = await storage.write("dir/a.txt", b"hello world", {"mode": 0o640})
        assert await storage.read("dir/a.txt") == b"hello world"
        assert await storage.read_range("dir/a.txt", 6, 5) == b"world"

        metadata = await storage.stat("dir/a.txt")
        assert metadata["kind"] == "File"
        assert metadata["size_bytes"] == 11
        assert metadata["mode"] & 0o777 == 0o640
        assert metadata["content_hash"] == write["content_hash"]
        assert [entry["path"] for entry in await storage.list_dir("dir")] == [
            "dir/a.txt"
        ]

        links = await storage.create_hard_link("dir/a.txt", "dir/b.txt")
        assert links["source"]["file_id"] == links["destination"]["file_id"]
        alias = await storage.find_hard_link_alias(metadata["file_id"], "dir/a.txt")
        assert alias == "dir/b.txt"

        await storage.rename("dir/b.txt", "dir/c.txt")
        batch = await storage.write_many(
            [{"path": "dir/d.txt", "body": [111, 107], "mode": 0o600}]
        )
        assert batch[0]["changed"] is True
        assert await storage.read("dir/d.txt") == b"ok"

        await storage.remove("dir/c.txt")
        try:
            await storage.read("dir/c.txt")
        except RuntimeError as error:
            assert error.code == "VFS_NOT_FOUND"
            assert error.status == 404
            assert error.status_code == 404
        else:
            raise AssertionError("missing path did not raise")

    asyncio.run(scenario())


def test_vfs_hash_functions_share_one_algorithm():
    hasher = chevalier.VfsContentHasher()
    hasher.update(b"abc")
    assert hasher.digest() == chevalier.vfs_content_hash(b"abc")
    assert chevalier.vfs_content_hash_algorithm() in {"blake3", "sha256"}
