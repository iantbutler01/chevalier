use std::collections::BTreeSet;

use bytes::Bytes;
#[cfg(feature = "postgres")]
use chevalier_vfs::VfsStorageWritePrecondition;
use chevalier_vfs::{
    OptimizedVfsStorage, VfsStorageMetadataFields, VfsStorageSubtreeOptions, local::LocalVfsStorage,
};

#[derive(Clone, Copy)]
struct XorShift64(u64);

impl XorShift64 {
    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn index(&mut self, len: usize) -> usize {
        (self.next() as usize) % len
    }
}

async fn assert_alias_state(
    clients: &[&dyn OptimizedVfsStorage],
    aliases: &[String],
    expected_file_id: &str,
    expected_bytes: &[u8],
) {
    let expected_paths = aliases.iter().cloned().collect::<BTreeSet<_>>();
    let subtree_prefix = aliases
        .first()
        .and_then(|path| path.rsplit_once('/').map(|(parent, _)| parent))
        .filter(|parent| {
            let prefix = format!("{parent}/");
            aliases.iter().all(|path| path.starts_with(&prefix))
        })
        .unwrap_or("tree");
    for client in clients {
        let mut expected_hash = None;
        for alias in aliases {
            let metadata = client
                .stat(alias)
                .await
                .unwrap_or_else(|error| panic!("stat {alias}: {error}"))
                .unwrap_or_else(|| panic!("{alias} disappeared"));
            assert_eq!(
                metadata.file_id.as_deref(),
                Some(expected_file_id),
                "{alias} changed identity",
            );
            assert_eq!(
                metadata.link_count,
                aliases.len() as u64,
                "{alias} reported the wrong link count",
            );
            assert_eq!(
                client.read(alias).await.unwrap().as_ref(),
                expected_bytes,
                "{alias} exposed stale or divergent bytes",
            );
            if let Some(hash) = expected_hash.as_ref() {
                assert_eq!(
                    metadata.content_hash.as_ref(),
                    Some(hash),
                    "{alias} exposed a divergent content hash",
                );
            } else {
                expected_hash = metadata.content_hash;
            }
        }

        let metadata = client
            .metadata_many(aliases, VfsStorageMetadataFields::default())
            .await
            .expect("metadata_many");
        assert_eq!(metadata.len(), aliases.len());
        assert!(metadata.into_iter().all(|entry| {
            entry.is_some_and(|entry| {
                entry.file_id.as_deref() == Some(expected_file_id)
                    && entry.link_count == aliases.len() as u64
            })
        }));

        let subtree_paths = client
            .list_subtree_file_metadata(subtree_prefix, VfsStorageSubtreeOptions::default())
            .await
            .expect("subtree metadata")
            .into_iter()
            .map(|entry| entry.path)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            subtree_paths, expected_paths,
            "subtree metadata published a missing or stale alias",
        );
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_randomized_alias_mutations_preserve_one_identity() {
    let directory = tempfile::tempdir().expect("tempdir");
    let first = LocalVfsStorage::new(directory.path());
    let second = LocalVfsStorage::new(directory.path());
    first
        .write("tree/alias-0", Bytes::from_static(b"seed"), None)
        .await
        .expect("seed");
    let initial = first.stat("tree/alias-0").await.unwrap().unwrap();
    let file_id = initial.file_id.expect("stable local identity");
    let mut aliases = vec!["tree/alias-0".to_string()];
    let mut expected_bytes = b"seed".to_vec();
    let mut next_path = 1_u64;
    let mut random = XorShift64(0x7e57_d15c_a11a_5eed);

    for step in 0..400_u64 {
        let client: &dyn OptimizedVfsStorage = if random.next() & 1 == 0 {
            &first
        } else {
            &second
        };
        match random.next() % 6 {
            0 | 1 => {
                let alias = aliases[random.index(aliases.len())].clone();
                expected_bytes = format!("payload-{step}-{}", random.next()).into_bytes();
                client
                    .write(&alias, Bytes::from(expected_bytes.clone()), None)
                    .await
                    .expect("write through alias");
            }
            2 if aliases.len() < 9 => {
                let source = aliases[random.index(aliases.len())].clone();
                let destination = format!("tree/alias-{next_path}");
                next_path += 1;
                client
                    .create_hard_link(&source, &destination)
                    .await
                    .expect("create randomized hard link");
                aliases.push(destination);
                aliases.sort();
            }
            3 => {
                let selected = random.index(aliases.len());
                let source = aliases[selected].clone();
                let destination = format!("tree/alias-{next_path}");
                next_path += 1;
                client
                    .rename_with_metadata(&source, &destination)
                    .await
                    .expect("rename randomized alias");
                aliases[selected] = destination;
                aliases.sort();
                assert!(client.stat(&source).await.unwrap().is_none());
            }
            4 if aliases.len() > 1 => {
                let selected = random.index(aliases.len());
                let removed = aliases.remove(selected);
                client
                    .delete_file_with_metadata(&removed, None)
                    .await
                    .expect("unlink randomized alias");
                assert!(client.stat(&removed).await.unwrap().is_none());
            }
            5 if aliases.len() > 1 => {
                let source_index = random.index(aliases.len());
                let mut destination_index = random.index(aliases.len() - 1);
                if destination_index >= source_index {
                    destination_index += 1;
                }
                client
                    .rename_with_metadata(
                        aliases[source_index].as_str(),
                        aliases[destination_index].as_str(),
                    )
                    .await
                    .expect("same-inode rename is a no-op");
            }
            _ => {}
        }
        assert_alias_state(&[&first, &second], &aliases, &file_id, &expected_bytes).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn local_final_unlink_does_not_invalidate_or_resurrect_an_open_file() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let directory = tempfile::tempdir().expect("tempdir");
    let storage = LocalVfsStorage::new(directory.path());
    storage
        .write("open-unlink", Bytes::from_static(b"before"), None)
        .await
        .expect("seed");
    let mut handle = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.path().join("open-unlink"))
        .expect("open handle");

    storage
        .delete_file_with_metadata("open-unlink", None)
        .await
        .expect("final unlink");
    assert!(storage.stat("open-unlink").await.unwrap().is_none());

    handle.set_len(0).expect("truncate open inode");
    handle.write_all(b"after").expect("write open inode");
    handle.seek(SeekFrom::Start(0)).expect("rewind");
    let mut bytes = Vec::new();
    handle.read_to_end(&mut bytes).expect("read open inode");
    assert_eq!(bytes, b"after");
    handle.sync_all().expect("sync open inode");
    drop(handle);
    assert!(
        storage.stat("open-unlink").await.unwrap().is_none(),
        "closing the unlinked inode must not resurrect its old pathname",
    );
}

#[cfg(all(unix, feature = "postgres"))]
mod postgres {
    use std::{env, sync::Arc, time::Instant};

    use chevalier_vfs::{
        VfsStorageError, VfsStorageWrite,
        index::VfsIndexScope,
        object_storage::{ObjectBackedVfsStorage, ObjectBackedVfsStorageConfig},
        object_store::LocalObjectStoreClient,
        postgres_index::PostgresVfsManifestIndex,
    };
    use sqlx::{PgPool, postgres::PgPoolOptions};
    use uuid::Uuid;

    use super::*;

    async fn test_pool() -> Option<PgPool> {
        let database_url = match env::var("CHEVALIER_VFS_TEST_DATABASE_URL") {
            Ok(value) => value,
            Err(_) => {
                eprintln!(
                    "CHEVALIER_VFS_TEST_DATABASE_URL is unset; skipping disposable Postgres identity torture"
                );
                return None;
            }
        };
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(&database_url)
            .await
            .expect("connect disposable postgres");
        let mut migration_connection = pool.acquire().await.expect("migration connection");
        sqlx::query("SELECT pg_advisory_lock(716_372_591)")
            .execute(&mut *migration_connection)
            .await
            .expect("serialize VFS test migrations");
        sqlx::raw_sql(include_str!(
            "../migrations/postgres/001_chevalier_vfs_index.sql"
        ))
        .execute(&mut *migration_connection)
        .await
        .expect("apply base VFS schema");
        sqlx::raw_sql(include_str!(
            "../migrations/postgres/002_chevalier_vfs_hard_link_identity.sql"
        ))
        .execute(&mut *migration_connection)
        .await
        .expect("apply hard-link identity schema");
        sqlx::query("SELECT pg_advisory_unlock(716_372_591)")
            .execute(&mut *migration_connection)
            .await
            .expect("release VFS test migration lock");
        Some(pool)
    }

    fn clients(
        pool: PgPool,
        object_root: &std::path::Path,
        scope: VfsIndexScope,
    ) -> (ObjectBackedVfsStorage, ObjectBackedVfsStorage) {
        let store = Arc::new(
            LocalObjectStoreClient::new(object_root.to_path_buf()).expect("local object store"),
        );
        let index = Arc::new(PostgresVfsManifestIndex::new(pool));
        let config = ObjectBackedVfsStorageConfig::new(scope);
        (
            ObjectBackedVfsStorage::new(config.clone(), store.clone(), index.clone()),
            ObjectBackedVfsStorage::new(config, store, index),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn postgres_randomized_alias_mutations_preserve_one_identity() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let directory = tempfile::tempdir().expect("tempdir");
        let scope = VfsIndexScope::new(format!("identity-random-{}", Uuid::new_v4()));
        let (first, second) = clients(pool, directory.path(), scope);
        first
            .write("tree/alias-0", Bytes::from_static(b"seed"), None)
            .await
            .expect("seed");
        let initial = first.stat("tree/alias-0").await.unwrap().unwrap();
        let file_id = initial.file_id.expect("stable postgres identity");
        let mut aliases = vec!["tree/alias-0".to_string()];
        let mut expected_bytes = b"seed".to_vec();
        let mut next_path = 1_u64;
        let mut random = XorShift64(0xa11a_51a5_c0de_5eed);

        for step in 0..250_u64 {
            let client: &dyn OptimizedVfsStorage = if random.next() & 1 == 0 {
                &first
            } else {
                &second
            };
            match random.next() % 6 {
                0 | 1 => {
                    let alias = aliases[random.index(aliases.len())].clone();
                    expected_bytes = format!("postgres-{step}-{}", random.next()).into_bytes();
                    client
                        .write(&alias, Bytes::from(expected_bytes.clone()), None)
                        .await
                        .expect("write through postgres alias");
                }
                2 if aliases.len() < 9 => {
                    let source = aliases[random.index(aliases.len())].clone();
                    let destination = format!("tree/alias-{next_path}");
                    next_path += 1;
                    client
                        .create_hard_link(&source, &destination)
                        .await
                        .expect("create postgres hard link");
                    aliases.push(destination);
                    aliases.sort();
                }
                3 => {
                    let selected = random.index(aliases.len());
                    let source = aliases[selected].clone();
                    let destination = format!("tree/alias-{next_path}");
                    next_path += 1;
                    client
                        .rename_with_metadata(&source, &destination)
                        .await
                        .expect("rename postgres alias");
                    aliases[selected] = destination;
                    aliases.sort();
                }
                4 if aliases.len() > 1 => {
                    let selected = random.index(aliases.len());
                    let removed = aliases.remove(selected);
                    client
                        .delete_file_with_metadata(&removed, None)
                        .await
                        .expect("unlink postgres alias");
                }
                5 if aliases.len() > 1 => {
                    let source_index = random.index(aliases.len());
                    let mut destination_index = random.index(aliases.len() - 1);
                    if destination_index >= source_index {
                        destination_index += 1;
                    }
                    client
                        .rename_with_metadata(
                            aliases[source_index].as_str(),
                            aliases[destination_index].as_str(),
                        )
                        .await
                        .expect("same-inode postgres rename is a no-op");
                }
                _ => {}
            }
            assert_alias_state(&[&first, &second], &aliases, &file_id, &expected_bytes).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn postgres_concurrent_alias_writes_keep_identity_and_content_coherent() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let directory = tempfile::tempdir().expect("tempdir");
        let scope = VfsIndexScope::new(format!("identity-race-{}", Uuid::new_v4()));
        let (first, second) = clients(pool, directory.path(), scope);
        first
            .write("tree/source", Bytes::from_static(b"seed"), None)
            .await
            .unwrap();
        let linked = first
            .create_hard_link("tree/source", "tree/alias")
            .await
            .unwrap();
        let file_id = linked.source.file_id.expect("stable postgres identity");

        for round in 0..100_u64 {
            let current = first.stat("tree/source").await.unwrap().unwrap();
            let precondition = VfsStorageWritePrecondition {
                predicate: None,
                fingerprint: current.version,
                secondary_fingerprint: None,
                expected_file_id: None,
            };
            let (left, right) = tokio::join!(
                first.write(
                    "tree/source",
                    Bytes::from(format!("left-{round}")),
                    Some(precondition.clone()),
                ),
                second.write(
                    "tree/alias",
                    Bytes::from(format!("right-{round}")),
                    Some(precondition),
                ),
            );
            assert_eq!(
                usize::from(left.is_ok()) + usize::from(right.is_ok()),
                1,
                "one identity-wide compare-and-swap must win",
            );
            let expected = first.read("tree/source").await.unwrap();
            assert_alias_state(
                &[&first, &second],
                &["tree/alias".to_string(), "tree/source".to_string()],
                &file_id,
                expected.as_ref(),
            )
            .await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn postgres_namespace_and_write_races_never_split_or_resurrect_an_inode() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let directory = tempfile::tempdir().expect("tempdir");
        let scope = VfsIndexScope::new(format!("identity-namespace-race-{}", Uuid::new_v4()));
        let (first, second) = clients(pool, directory.path(), scope);

        for round in 0..50_u64 {
            let source = format!("tree/link-write-{round}/source");
            let alias = format!("tree/link-write-{round}/alias");
            first
                .write(&source, Bytes::from_static(b"before"), None)
                .await
                .unwrap();
            let identity = first.stat(&source).await.unwrap().unwrap().file_id.unwrap();
            let (write, link) = tokio::join!(
                first.write(&source, Bytes::from_static(b"after"), None),
                second.create_hard_link(&source, &alias),
            );
            link.expect("concurrent hard link");
            let expected = match write {
                Ok(_) => b"after".as_slice(),
                Err(_) => b"before".as_slice(),
            };
            assert_alias_state(&[&first, &second], &[alias, source], &identity, expected).await;
        }

        for round in 0..50_u64 {
            let source = format!("tree/unlink-write-{round}/source");
            let alias = format!("tree/unlink-write-{round}/alias");
            first
                .write(&source, Bytes::from_static(b"before"), None)
                .await
                .unwrap();
            first.create_hard_link(&source, &alias).await.unwrap();
            let (write, unlink) = tokio::join!(
                first.write(&source, Bytes::from_static(b"after"), None),
                second.delete_file_with_metadata(&alias, None),
            );
            unlink.expect("concurrent unlink");
            assert!(
                second.stat(&alias).await.unwrap().is_none(),
                "a racing write resurrected an unlinked alias",
            );
            let source_metadata = second.stat(&source).await.unwrap().unwrap();
            assert_eq!(source_metadata.link_count, 1);
            match write {
                Ok(_) => assert_eq!(second.read(&source).await.unwrap().as_ref(), b"after"),
                Err(_) => assert_eq!(second.read(&source).await.unwrap().as_ref(), b"before"),
            }
        }

        for round in 0..50_u64 {
            let source = format!("tree/rename-write-{round}/source");
            let alias = format!("tree/rename-write-{round}/alias");
            let moved = format!("tree/rename-write-{round}/moved");
            first
                .write(&source, Bytes::from_static(b"before"), None)
                .await
                .unwrap();
            let linked = first.create_hard_link(&source, &alias).await.unwrap();
            let identity = linked.source.file_id.unwrap();
            let (write, rename) = tokio::join!(
                first.write(&source, Bytes::from_static(b"after"), None),
                second.rename_with_metadata(&alias, &moved),
            );
            rename.expect("concurrent alias rename");
            assert!(
                second.stat(&alias).await.unwrap().is_none(),
                "a racing write resurrected a renamed alias",
            );
            let expected = match write {
                Ok(_) => b"after".as_slice(),
                Err(_) => b"before".as_slice(),
            };
            assert_alias_state(&[&first, &second], &[moved, source], &identity, expected).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn postgres_identity_lock_timeout_rolls_back_and_releases_cleanly() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let directory = tempfile::tempdir().expect("tempdir");
        let scope = VfsIndexScope::new(format!("identity-timeout-{}", Uuid::new_v4()));
        let (first, second) = clients(pool.clone(), directory.path(), scope.clone());
        first
            .write("tree/source", Bytes::from_static(b"before"), None)
            .await
            .unwrap();
        let file_id = first
            .stat("tree/source")
            .await
            .unwrap()
            .unwrap()
            .file_id
            .unwrap();
        let before_counts = sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT
                (SELECT count(*) FROM chevalier_vfs_file_manifests WHERE scope_key = $1),
                (SELECT count(*) FROM chevalier_vfs_packs WHERE scope_key = $1)
            "#,
        )
        .bind(&scope.key)
        .fetch_one(&pool)
        .await
        .unwrap();

        let lock_key = format!(
            "chevalier-vfs-identity:{}:{}:{}:{}",
            scope.key.len(),
            scope.key,
            file_id.len(),
            file_id
        );
        let mut holder = pool.begin().await.expect("holder transaction");
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_key)
            .execute(&mut *holder)
            .await
            .expect("hold identity lock");

        let started = Instant::now();
        let error = second
            .write("tree/source", Bytes::from_static(b"blocked"), None)
            .await
            .expect_err("identity lock acquisition must time out");
        let elapsed = started.elapsed();
        assert!(
            matches!(error, VfsStorageError::Internal(ref message) if message.contains("timed out acquiring VFS file-identity lock")),
            "{error:?}",
        );
        assert!(elapsed >= std::time::Duration::from_millis(1_500));
        assert!(elapsed < std::time::Duration::from_secs(5));
        assert_eq!(first.read("tree/source").await.unwrap().as_ref(), b"before");
        let after_counts = sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT
                (SELECT count(*) FROM chevalier_vfs_file_manifests WHERE scope_key = $1),
                (SELECT count(*) FROM chevalier_vfs_packs WHERE scope_key = $1)
            "#,
        )
        .bind(&scope.key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            after_counts, before_counts,
            "a timed-out publication transaction must leave no index rows",
        );

        holder.rollback().await.expect("release held identity lock");
        second
            .write("tree/source", Bytes::from_static(b"after"), None)
            .await
            .expect("a clean transaction succeeds after timeout rollback");
        assert_eq!(first.read("tree/source").await.unwrap().as_ref(), b"after");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn postgres_large_batches_cross_parameter_chunk_boundary_atomically() {
        let Some(pool) = test_pool().await else {
            return;
        };
        let directory = tempfile::tempdir().expect("tempdir");

        for count in [3_999_usize, 4_001] {
            let scope = VfsIndexScope::new(format!("identity-chunk-{count}-{}", Uuid::new_v4()));
            let (storage, _) = clients(pool.clone(), directory.path(), scope);
            let writes = (0..count)
                .map(|index| VfsStorageWrite {
                    path: format!("batch-{count}/file-{index:05}"),
                    bytes: Bytes::from(format!("value-{index:05}")),
                    token_count: None,
                    precondition: None,
                })
                .collect::<Vec<_>>();
            let results = storage
                .write_many_atomic(writes)
                .await
                .unwrap_or_else(|error| panic!("{count}-file batch failed: {error}"));
            assert_eq!(results.len(), count);
            for index in [0, count / 2, count - 1] {
                assert_eq!(
                    storage
                        .read(&format!("batch-{count}/file-{index:05}"))
                        .await
                        .unwrap()
                        .as_ref(),
                    format!("value-{index:05}").as_bytes(),
                );
            }
        }

        let scope = VfsIndexScope::new(format!("identity-chunk-rollback-{}", Uuid::new_v4()));
        let (storage, _) = clients(pool.clone(), directory.path(), scope.clone());
        storage
            .write("rollback/guard", Bytes::from_static(b"first"), None)
            .await
            .unwrap();
        let stale_version = storage
            .stat("rollback/guard")
            .await
            .unwrap()
            .unwrap()
            .version
            .unwrap();
        storage
            .write("rollback/guard", Bytes::from_static(b"current"), None)
            .await
            .unwrap();
        let mut writes = (0..4_000)
            .map(|index| VfsStorageWrite {
                path: format!("rollback/file-{index:05}"),
                bytes: Bytes::from_static(b"new"),
                token_count: None,
                precondition: None,
            })
            .collect::<Vec<_>>();
        writes.push(VfsStorageWrite {
            path: "rollback/guard".to_string(),
            bytes: Bytes::from_static(b"stale"),
            token_count: None,
            precondition: Some(VfsStorageWritePrecondition {
                predicate: None,
                fingerprint: Some(stale_version),
                secondary_fingerprint: None,
                expected_file_id: None,
            }),
        });
        let before_counts = sqlx::query_as::<_, (i64, i64, i64)>(
            r#"
            SELECT
                (SELECT count(*) FROM chevalier_vfs_entries WHERE scope_key = $1),
                (SELECT count(*) FROM chevalier_vfs_file_manifests WHERE scope_key = $1),
                (SELECT count(*) FROM chevalier_vfs_packs WHERE scope_key = $1)
            "#,
        )
        .bind(&scope.key)
        .fetch_one(&pool)
        .await
        .unwrap();
        let error = storage
            .write_many_atomic(writes)
            .await
            .expect_err("stale item in second chunk must reject the whole batch");
        assert!(matches!(error, VfsStorageError::Conflict(_)));
        assert!(
            storage.stat("rollback/file-00000").await.unwrap().is_none(),
            "the successful first SQL chunk must roll back with the failed second chunk",
        );
        assert_eq!(
            storage.read("rollback/guard").await.unwrap().as_ref(),
            b"current",
        );
        let after_counts = sqlx::query_as::<_, (i64, i64, i64)>(
            r#"
            SELECT
                (SELECT count(*) FROM chevalier_vfs_entries WHERE scope_key = $1),
                (SELECT count(*) FROM chevalier_vfs_file_manifests WHERE scope_key = $1),
                (SELECT count(*) FROM chevalier_vfs_packs WHERE scope_key = $1)
            "#,
        )
        .bind(&scope.key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(after_counts, before_counts);
    }
}
