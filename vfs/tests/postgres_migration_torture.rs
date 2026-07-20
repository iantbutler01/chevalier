#![cfg(feature = "postgres")]

use std::env;

use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

const BASE_MIGRATION: &str = include_str!("../migrations/postgres/001_chevalier_vfs_index.sql");
const HARD_LINK_MIGRATION: &str =
    include_str!("../migrations/postgres/002_chevalier_vfs_hard_link_identity.sql");

async fn connect(database_url: &str, schema: &str) -> PgConnection {
    let mut connection = PgConnection::connect(database_url)
        .await
        .expect("connect disposable Postgres");
    sqlx::raw_sql(&format!("SET search_path TO {schema}"))
        .execute(&mut connection)
        .await
        .expect("select isolated migration schema");
    connection
}

#[tokio::test]
async fn postgres_hard_link_migration_is_fresh_legacy_restart_and_failure_safe() {
    let database_url = match env::var("CHEVALIER_VFS_TEST_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => {
            eprintln!(
                "CHEVALIER_VFS_TEST_DATABASE_URL is unset; skipping disposable Postgres migration torture"
            );
            return;
        }
    };
    let suffix = Uuid::new_v4().simple().to_string();
    let fresh_schema = format!("chevalier_vfs_fresh_{suffix}");
    let legacy_schema = format!("chevalier_vfs_legacy_{suffix}");
    let rollback_schema = format!("chevalier_vfs_rollback_{suffix}");

    let mut admin = PgConnection::connect(&database_url)
        .await
        .expect("connect disposable Postgres");
    for schema in [&fresh_schema, &legacy_schema, &rollback_schema] {
        sqlx::raw_sql(&format!("CREATE SCHEMA {schema}"))
            .execute(&mut admin)
            .await
            .expect("create isolated migration schema");
    }

    let result = async {
        let mut fresh = connect(&database_url, &fresh_schema).await;
        sqlx::raw_sql(BASE_MIGRATION)
            .execute(&mut fresh)
            .await
            .expect("apply fresh base migration");
        sqlx::raw_sql(HARD_LINK_MIGRATION)
            .execute(&mut fresh)
            .await
            .expect("apply fresh hard-link migration");
        sqlx::query(
            r#"
            INSERT INTO chevalier_vfs_entries(
              id,scope_key,logical_path,parent_logical_path,entry_name,entry_kind,
              file_id,size_bytes,content_hash,storage_backend,materialization_generation
            ) VALUES
              ('fresh-source-id','fresh-scope','source','','source','file',
               'stable-shared-id',3,'hash-a','object_store',0),
              ('fresh-alias-id','fresh-scope','alias','','alias','file',
               'stable-shared-id',3,'hash-a','object_store',0)
            "#,
        )
        .execute(&mut fresh)
        .await
        .expect("seed an existing shared identity");
        sqlx::raw_sql(HARD_LINK_MIGRATION)
            .execute(&mut fresh)
            .await
            .expect("hard-link migration must be idempotent");
        let preserved_identity: (i64, i64) = sqlx::query_as(
            r#"
            SELECT COUNT(*),COUNT(DISTINCT file_id)
            FROM chevalier_vfs_entries
            WHERE scope_key='fresh-scope' AND file_id='stable-shared-id'
            "#,
        )
        .fetch_one(&mut fresh)
        .await
        .expect("inspect preserved shared identity");
        assert_eq!(preserved_identity, (2, 1));
        let fresh_column: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'chevalier_vfs_entries'
              AND column_name = 'file_id'
            "#,
        )
        .fetch_one(&mut fresh)
        .await
        .expect("inspect fresh file identity column");
        assert_eq!(fresh_column, 1);
        let fresh_index: String = sqlx::query_scalar(
            r#"
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND indexname = 'chevalier_vfs_entries_file_identity_idx'
            "#,
        )
        .fetch_one(&mut fresh)
        .await
        .expect("inspect fresh file identity index");
        assert!(fresh_index.contains("(scope_key, file_id)"));
        assert!(fresh_index.contains("entry_kind = 'file'"));
        fresh.close().await.expect("close fresh connection");

        let mut fresh_after_restart = connect(&database_url, &fresh_schema).await;
        sqlx::raw_sql(HARD_LINK_MIGRATION)
            .execute(&mut fresh_after_restart)
            .await
            .expect("restart replay must remain idempotent");
        let persisted_column: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'chevalier_vfs_entries'
              AND column_name = 'file_id'
            "#,
        )
        .fetch_one(&mut fresh_after_restart)
        .await
        .expect("inspect restarted schema");
        assert_eq!(persisted_column, 1);

        let mut legacy = connect(&database_url, &legacy_schema).await;
        sqlx::raw_sql(BASE_MIGRATION)
            .execute(&mut legacy)
            .await
            .expect("apply legacy base migration");
        sqlx::query(
            r#"
            INSERT INTO chevalier_vfs_entries(
              id,scope_key,logical_path,parent_logical_path,entry_name,entry_kind,
              size_bytes,content_hash,storage_backend,materialization_generation
            ) VALUES
              ('legacy-dir-id','legacy-scope','tree','','tree','directory',0,NULL,'object_store',0),
              ('legacy-file-id','legacy-scope','tree/a.txt','tree','a.txt','file',3,'hash-a','object_store',0)
            "#,
        )
        .execute(&mut legacy)
        .await
        .expect("seed legacy entries");
        sqlx::raw_sql(HARD_LINK_MIGRATION)
            .execute(&mut legacy)
            .await
            .expect("upgrade legacy entries");
        let legacy_rows = sqlx::query(
            r#"
            SELECT entry_kind,file_id
            FROM chevalier_vfs_entries
            WHERE scope_key='legacy-scope'
            ORDER BY entry_kind
            "#,
        )
        .fetch_all(&mut legacy)
        .await
        .expect("read upgraded legacy entries");
        assert_eq!(legacy_rows.len(), 2);
        assert_eq!(legacy_rows[0].get::<String, _>("entry_kind"), "directory");
        assert_eq!(legacy_rows[0].get::<Option<String>, _>("file_id"), None);
        assert_eq!(legacy_rows[1].get::<String, _>("entry_kind"), "file");
        assert_eq!(
            legacy_rows[1].get::<Option<String>, _>("file_id"),
            Some("legacy-file-id".to_string()),
        );

        let mut rollback = connect(&database_url, &rollback_schema).await;
        sqlx::raw_sql(BASE_MIGRATION)
            .execute(&mut rollback)
            .await
            .expect("apply rollback base migration");
        let mut transaction = rollback.begin().await.expect("begin migration transaction");
        sqlx::raw_sql(HARD_LINK_MIGRATION)
            .execute(&mut *transaction)
            .await
            .expect("apply migration before injected failure");
        let injected = sqlx::query("SELECT * FROM deliberately_missing_chevalier_vfs_table")
            .execute(&mut *transaction)
            .await;
        assert!(injected.is_err(), "injected migration failure must fail");
        transaction
            .rollback()
            .await
            .expect("roll back failed migration");
        let rollback_column: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'chevalier_vfs_entries'
              AND column_name = 'file_id'
            "#,
        )
        .fetch_one(&mut rollback)
        .await
        .expect("inspect rolled-back schema");
        assert_eq!(
            rollback_column, 0,
            "failed migration must not leave a partial identity column"
        );
    }
    .await;

    for schema in [&fresh_schema, &legacy_schema, &rollback_schema] {
        sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&mut admin)
            .await
            .expect("clean isolated migration schema");
    }
    result
}
