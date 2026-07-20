#![cfg(feature = "postgres")]

use std::{
    env,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;
use chevalier_vfs::{
    OptimizedVfsStorage, VfsStorageMetadataFields, VfsStorageWrite,
    index::VfsIndexScope,
    object_storage::{ObjectBackedVfsStorage, ObjectBackedVfsStorageConfig},
    object_store::LocalObjectStoreClient,
    postgres_index::PostgresVfsManifestIndex,
};
use futures::FutureExt;
use sqlx::{Connection, Executor, PgConnection, postgres::PgPoolOptions};
use uuid::Uuid;

const BASE_MIGRATION: &str = include_str!("../migrations/postgres/001_chevalier_vfs_index.sql");
const HARD_LINK_MIGRATION: &str =
    include_str!("../migrations/postgres/002_chevalier_vfs_hard_link_identity.sql");

fn git_perf_writes(count: usize, generation: usize) -> Vec<VfsStorageWrite> {
    let mutation_count = (count / 100).max(1);
    let object_count = count.saturating_sub(mutation_count.saturating_mul(2));
    let mut writes = (0..mutation_count)
        .map(|index| VfsStorageWrite {
            path: format!(".git/refs/heads/perf-{index:05}.lock"),
            bytes: Bytes::from(format!("ref-{generation}-{index:05}\n")),
            token_count: None,
            precondition: None,
        })
        .chain((0..mutation_count).map(|index| VfsStorageWrite {
            path: format!("src/generated/perf-{index:05}.ts"),
            bytes: Bytes::from(format!("export const value = {generation}_{index};\n")),
            token_count: None,
            precondition: None,
        }))
        .chain((0..object_count).map(|index| VfsStorageWrite {
            path: format!(".git/objects/{:02x}/{:038x}", index % 256, index),
            bytes: Bytes::from(format!("blob {generation} {index:08}\n")),
            token_count: None,
            precondition: None,
        }))
        .collect::<Vec<_>>();
    writes.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    writes
}

/// The in-module object benchmark uses a deterministic memory index to count
/// calls. This companion test exercises the actual Postgres batch builders and
/// their protocol parameter limits. It is env-gated and ignored because it
/// requires a disposable database:
///
/// `CHEVALIER_VFS_TEST_DATABASE_URL=postgres://... cargo test --features postgres \
///  --test git_small_file_postgres_perf -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit disposable-Postgres 1k/10k Git small-file performance suite"]
async fn postgres_object_git_small_file_perf_1k_10k() {
    let database_url = env::var("CHEVALIER_VFS_TEST_DATABASE_URL")
        .expect("CHEVALIER_VFS_TEST_DATABASE_URL must name a disposable Postgres database");
    let schema = format!("chevalier_git_perf_{}", Uuid::new_v4().simple());
    let mut admin = PgConnection::connect(&database_url)
        .await
        .expect("connect disposable Postgres");
    sqlx::raw_sql(&format!("CREATE SCHEMA {schema}"))
        .execute(&mut admin)
        .await
        .expect("create isolated perf schema");

    let schema_for_pool = schema.clone();
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .after_connect(move |connection, _metadata| {
            let statement = format!("SET search_path TO {schema_for_pool}");
            Box::pin(async move {
                connection.execute(statement.as_str()).await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await
        .expect("connect isolated perf pool");
    let result = AssertUnwindSafe(async {
        let mut migration = pool.acquire().await.expect("migration connection");
        sqlx::raw_sql(BASE_MIGRATION)
            .execute(&mut *migration)
            .await
            .expect("apply base VFS schema");
        sqlx::raw_sql(HARD_LINK_MIGRATION)
            .execute(&mut *migration)
            .await
            .expect("apply hard-link identity schema");
        drop(migration);

        let directory = tempfile::tempdir().expect("object-store tempdir");
        let store = Arc::new(
            LocalObjectStoreClient::new(directory.path().to_path_buf())
                .expect("local object store"),
        );
        let index = Arc::new(PostgresVfsManifestIndex::new(pool.clone()));
        let mut samples = Vec::new();
        for count in [1_000_usize, 10_000] {
            let config = ObjectBackedVfsStorageConfig::new(VfsIndexScope::new(format!(
                "postgres-git-perf-{}",
                Uuid::new_v4().simple()
            )));
            let first = ObjectBackedVfsStorage::new(config.clone(), store.clone(), index.clone());
            let second = ObjectBackedVfsStorage::new(config, store.clone(), index.clone());
            let initial = git_perf_writes(count, 0);
            let paths = initial
                .iter()
                .map(|write| write.path.clone())
                .collect::<Vec<_>>();
            let mutation_count = (count / 100).max(1);

            let create_started = Instant::now();
            first
                .write_many_atomic(initial)
                .await
                .expect("Postgres-backed Git-shaped create");
            let create_elapsed = create_started.elapsed();

            let status_started = Instant::now();
            let status = first
                .metadata_many(&paths, VfsStorageMetadataFields::default())
                .await
                .expect("Postgres-backed status-like bulk metadata");
            let status_elapsed = status_started.elapsed();
            assert_eq!(status.iter().filter(|entry| entry.is_some()).count(), count);

            let warm_started = Instant::now();
            let warmed = first
                .read_many(&paths)
                .await
                .expect("Postgres-backed warm small-file read");
            let warm_elapsed = warm_started.elapsed();
            assert_eq!(warmed.len(), count);

            let targeted = git_perf_writes(count, 1)
                .into_iter()
                .filter(|write| {
                    write.path.starts_with(".git/refs/") || write.path.starts_with("src/generated/")
                })
                .collect::<Vec<_>>();
            let rewrite_started = Instant::now();
            second
                .write_many_atomic(targeted)
                .await
                .expect("Postgres-backed targeted rewrite");
            let rewrite_elapsed = rewrite_started.elapsed();

            let namespace_started = Instant::now();
            for index in 0..mutation_count {
                second
                    .rename_with_metadata(
                        &format!(".git/refs/heads/perf-{index:05}.lock"),
                        &format!(".git/refs/heads/perf-{index:05}"),
                    )
                    .await
                    .expect("promote Postgres-backed ref");
                second
                    .delete_file_with_metadata(&format!("src/generated/perf-{index:05}.ts"), None)
                    .await
                    .expect("delete Postgres-backed worktree file");
            }
            let namespace_elapsed = namespace_started.elapsed();

            let survivor_index = count.saturating_sub(mutation_count * 2 + 1);
            let survivor = format!(
                ".git/objects/{:02x}/{:038x}",
                survivor_index % 256,
                survivor_index
            );
            let _ = first
                .read(&survivor)
                .await
                .expect("prime first Postgres-backed client cache");
            let replacement = Bytes::from_static(b"cross-client replacement with distinct size\n");
            second
                .write(&survivor, replacement.clone(), None)
                .await
                .expect("second Postgres-backed client replacement");
            assert_eq!(
                first
                    .read(&survivor)
                    .await
                    .expect("first Postgres-backed client refresh"),
                replacement,
                "Postgres manifest changes must invalidate stale object bytes",
            );

            let total = create_elapsed
                + status_elapsed
                + warm_elapsed
                + rewrite_elapsed
                + namespace_elapsed;
            samples.push((count, total));
            eprintln!(
                "git-small-file-perf backend=object-postgres files={count} \
                 create={create_elapsed:?} status={status_elapsed:?} warm_reads={warm_elapsed:?} \
                 rewrite_2pct={rewrite_elapsed:?} namespace_2pct={namespace_elapsed:?} \
                 total={total:?}",
            );
        }
        let one_k = samples[0].1.as_secs_f64();
        let ten_k = samples[1].1.as_secs_f64();
        assert!(
            ten_k <= one_k * 35.0,
            "10x Postgres object workload regressed toward quadratic scaling: \
             1k={one_k:.3}s 10k={ten_k:.3}s",
        );
    })
    .catch_unwind()
    .await;

    pool.close().await;
    sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&mut admin)
        .await
        .expect("drop isolated perf schema");
    if let Err(payload) = result {
        resume_unwind(payload);
    }
}
