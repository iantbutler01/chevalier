// @dive-file: Generic object-store boundary and local filesystem-backed implementation.
// @dive-rel: Keeps object listing, conditional writes/deletes, range reads, and local-dev
// @dive-rel: storage mechanics in Chevalier instead of product adapters.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{VfsStorageError, VfsStorageResult};

#[derive(Debug, Clone)]
pub struct ObjectListRequest {
    pub prefix: String,
    pub delimiter: Option<String>,
    pub max_results: Option<usize>,
    pub page_token: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub key: String,
    pub size_bytes: u64,
    pub generation: Option<String>,
    pub metageneration: Option<String>,
    pub updated: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct ObjectListPage {
    pub objects: Vec<ObjectMetadata>,
    pub prefixes: Vec<String>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ObjectWriteCondition {
    pub if_absent: bool,
    pub if_generation_match: Option<String>,
    pub if_metageneration_match: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ObjectDeleteCondition {
    pub if_generation_match: Option<String>,
    pub if_metageneration_match: Option<String>,
}

#[async_trait]
pub trait ObjectStoreClient: Send + Sync {
    fn stat_object(&self, key: &str) -> VfsStorageResult<Option<ObjectMetadata>>;
    fn get_object(&self, key: &str) -> VfsStorageResult<Option<Vec<u8>>>;
    fn put_object(
        &self,
        key: &str,
        bytes: &[u8],
        condition: ObjectWriteCondition,
    ) -> VfsStorageResult<()>;
    fn delete_object(&self, key: &str, condition: ObjectDeleteCondition) -> VfsStorageResult<()>;
    fn copy_object(&self, from: &str, to: &str) -> VfsStorageResult<()>;
    fn list_objects(&self, request: ObjectListRequest) -> VfsStorageResult<ObjectListPage>;

    async fn stat_object_async(&self, key: &str) -> VfsStorageResult<Option<ObjectMetadata>> {
        self.stat_object(key)
    }

    async fn get_object_async(&self, key: &str) -> VfsStorageResult<Option<Vec<u8>>> {
        self.get_object(key)
    }

    async fn get_object_range_async(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> VfsStorageResult<Option<Vec<u8>>> {
        let Some(bytes) = self.get_object_async(key).await? else {
            return Ok(None);
        };
        let start = (offset as usize).min(bytes.len());
        let end = start.saturating_add(length as usize).min(bytes.len());
        Ok(Some(bytes[start..end].to_vec()))
    }

    async fn put_object_async(
        &self,
        key: &str,
        bytes: &[u8],
        condition: ObjectWriteCondition,
    ) -> VfsStorageResult<()> {
        self.put_object(key, bytes, condition)
    }

    async fn delete_object_async(
        &self,
        key: &str,
        condition: ObjectDeleteCondition,
    ) -> VfsStorageResult<()> {
        self.delete_object(key, condition)
    }

    async fn delete_objects_async(
        &self,
        keys: &[String],
        condition: ObjectDeleteCondition,
    ) -> VfsStorageResult<()> {
        for key in keys {
            self.delete_object_async(key, condition.clone()).await?;
        }
        Ok(())
    }

    async fn copy_object_async(&self, from: &str, to: &str) -> VfsStorageResult<()> {
        self.copy_object(from, to)
    }

    async fn list_objects_async(
        &self,
        request: ObjectListRequest,
    ) -> VfsStorageResult<ObjectListPage> {
        self.list_objects(request)
    }
}

#[derive(Clone)]
pub struct LocalObjectStoreClient {
    root: PathBuf,
}

impl LocalObjectStoreClient {
    pub fn new(root: PathBuf) -> VfsStorageResult<Self> {
        fs::create_dir_all(&root).map_err(|e| {
            VfsStorageError::Internal(format!(
                "failed to create local object store root {}: {e}",
                root.display()
            ))
        })?;
        Ok(Self { root })
    }

    fn key_to_path(&self, key: &str) -> PathBuf {
        let trimmed = key.trim_start_matches('/');
        self.root.join(trimmed)
    }
}

#[async_trait]
impl ObjectStoreClient for LocalObjectStoreClient {
    fn stat_object(&self, key: &str) -> VfsStorageResult<Option<ObjectMetadata>> {
        let path = self.key_to_path(key);
        match fs::metadata(&path) {
            Ok(meta) if meta.is_file() => Ok(Some(ObjectMetadata {
                key: key.to_string(),
                size_bytes: meta.len(),
                generation: None,
                metageneration: None,
                updated: None,
            })),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(VfsStorageError::Internal(format!(
                "stat {} failed: {e}",
                path.display()
            ))),
        }
    }

    fn get_object(&self, key: &str) -> VfsStorageResult<Option<Vec<u8>>> {
        let path = self.key_to_path(key);
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(VfsStorageError::Internal(format!(
                "read {} failed: {e}",
                path.display()
            ))),
        }
    }

    fn put_object(
        &self,
        key: &str,
        bytes: &[u8],
        condition: ObjectWriteCondition,
    ) -> VfsStorageResult<()> {
        let path = self.key_to_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                VfsStorageError::Internal(format!(
                    "create parent dir {} failed: {e}",
                    parent.display()
                ))
            })?;
        }

        if condition.if_absent && path.exists() {
            return Ok(());
        }

        let tmp_name = format!(".tmp-{}-{}", std::process::id(), Uuid::new_v4().simple());
        let tmp_path = path
            .parent()
            .map(|p| p.join(&tmp_name))
            .unwrap_or_else(|| PathBuf::from(&tmp_name));

        {
            let mut f = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp_path)
                .map_err(|e| {
                    VfsStorageError::Internal(format!(
                        "open temp {} failed: {e}",
                        tmp_path.display()
                    ))
                })?;
            f.write_all(bytes).map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                VfsStorageError::Internal(format!("write temp {} failed: {e}", tmp_path.display()))
            })?;
            f.sync_all().map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                VfsStorageError::Internal(format!("fsync temp {} failed: {e}", tmp_path.display()))
            })?;
        }

        fs::rename(&tmp_path, &path).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            VfsStorageError::Internal(format!(
                "rename {} -> {} failed: {e}",
                tmp_path.display(),
                path.display()
            ))
        })
    }

    fn delete_object(&self, key: &str, _condition: ObjectDeleteCondition) -> VfsStorageResult<()> {
        let path = self.key_to_path(key);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(VfsStorageError::Internal(format!(
                "remove {} failed: {e}",
                path.display()
            ))),
        }
    }

    fn copy_object(&self, from: &str, to: &str) -> VfsStorageResult<()> {
        let from_path = self.key_to_path(from);
        let to_path = self.key_to_path(to);
        if let Some(parent) = to_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                VfsStorageError::Internal(format!(
                    "create parent dir {} failed: {e}",
                    parent.display()
                ))
            })?;
        }
        fs::copy(&from_path, &to_path)
            .map(|_| ())
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => {
                    VfsStorageError::NotFound(format!("missing object {from}"))
                }
                _ => VfsStorageError::Internal(format!(
                    "copy {} -> {} failed: {e}",
                    from_path.display(),
                    to_path.display()
                )),
            })
    }

    fn list_objects(&self, request: ObjectListRequest) -> VfsStorageResult<ObjectListPage> {
        let mut all_keys: Vec<(String, u64)> = Vec::new();
        walk_collect(&self.root, &self.root, &mut all_keys)?;
        all_keys.sort_by(|a, b| a.0.cmp(&b.0));

        let mut page = ObjectListPage::default();
        let mut seen_prefixes = std::collections::BTreeSet::new();

        for (key, size) in all_keys {
            if !key.starts_with(&request.prefix) {
                continue;
            }
            let suffix = &key[request.prefix.len()..];
            if let Some(delimiter) = request.delimiter.as_deref() {
                if let Some(index) = suffix.find(delimiter) {
                    let prefix = format!("{}{}", request.prefix, &suffix[..=index]);
                    if seen_prefixes.insert(prefix.clone()) {
                        page.prefixes.push(prefix);
                    }
                    continue;
                }
            }
            page.objects.push(ObjectMetadata {
                key,
                size_bytes: size,
                generation: None,
                metageneration: None,
                updated: None,
            });
        }

        if let Some(max_results) = request.max_results {
            page.objects.truncate(max_results);
            page.prefixes.truncate(max_results);
        }
        page.next_page_token = None;
        Ok(page)
    }
}

fn walk_collect(root: &Path, dir: &Path, out: &mut Vec<(String, u64)>) -> VfsStorageResult<()> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(VfsStorageError::Internal(format!(
                "read_dir {} failed: {e}",
                dir.display()
            )));
        }
    };
    for entry in entries {
        let entry = entry.map_err(|e| {
            VfsStorageError::Internal(format!("dir entry under {} failed: {e}", dir.display()))
        })?;
        let path = entry.path();
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if file_name.starts_with(".tmp-") {
            continue;
        }
        let metadata = entry.metadata().map_err(|e| {
            VfsStorageError::Internal(format!("metadata {} failed: {e}", path.display()))
        })?;
        if metadata.is_dir() {
            walk_collect(root, &path, out)?;
        } else if metadata.is_file() {
            if let Ok(rel) = path.strip_prefix(root) {
                let key = rel.to_string_lossy().replace('\\', "/");
                out.push((key, metadata.len()));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fresh() -> (LocalObjectStoreClient, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let client = LocalObjectStoreClient::new(dir.path().to_path_buf()).expect("client");
        (client, dir)
    }

    #[test]
    fn put_then_get_roundtrip() {
        let (client, _dir) = fresh();
        client
            .put_object(
                "skills-canonical/foo/v1/SKILL.md",
                b"hello",
                ObjectWriteCondition::default(),
            )
            .expect("put");
        let got = client
            .get_object("skills-canonical/foo/v1/SKILL.md")
            .expect("get")
            .expect("present");
        assert_eq!(got, b"hello");
    }

    #[test]
    fn if_absent_existing_object_is_noop() {
        let (client, _dir) = fresh();
        client
            .put_object("k", b"first", ObjectWriteCondition::default())
            .expect("first put");
        client
            .put_object(
                "k",
                b"second",
                ObjectWriteCondition {
                    if_absent: true,
                    ..Default::default()
                },
            )
            .expect("if_absent skip");
        assert_eq!(
            client.get_object("k").expect("get").expect("present"),
            b"first"
        );
    }

    #[test]
    fn delete_missing_object_is_success() {
        let (client, _dir) = fresh();
        client
            .delete_object("nope", ObjectDeleteCondition::default())
            .expect("delete absent");
    }

    #[test]
    fn list_with_delimiter_collapses_synthetic_prefixes() {
        let (client, _dir) = fresh();
        for key in [
            "skills-canonical/foo/v1/SKILL.md",
            "skills-canonical/foo/v1/scripts/a.py",
            "skills-canonical/bar/v1/SKILL.md",
        ] {
            client
                .put_object(key, b"x", ObjectWriteCondition::default())
                .expect("put");
        }
        let page = client
            .list_objects(ObjectListRequest {
                prefix: "skills-canonical/".to_string(),
                delimiter: Some("/".to_string()),
                max_results: None,
                page_token: None,
            })
            .expect("list");
        let prefixes: Vec<_> = page.prefixes.to_vec();
        assert!(prefixes.contains(&"skills-canonical/foo/".to_string()));
        assert!(prefixes.contains(&"skills-canonical/bar/".to_string()));
        assert!(page.objects.is_empty());
    }

    #[test]
    fn copy_object_writes_destination() {
        let (client, _dir) = fresh();
        client
            .put_object("a", b"payload", ObjectWriteCondition::default())
            .expect("put");
        client.copy_object("a", "nested/b").expect("copy");
        assert_eq!(
            client
                .get_object("nested/b")
                .expect("get")
                .expect("present"),
            b"payload"
        );
    }
}
