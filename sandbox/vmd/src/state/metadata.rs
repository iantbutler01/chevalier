// @dive-file: Persistent vm.json metadata load/save helpers with normalization guarantees.
// @dive-rel: Used by vmd/src/state/manager.rs for durable VM state transitions.
// @dive-rel: Provides atomic metadata writes to preserve lineage and snapshot consistency.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;

use super::types::{DurableVolumeMetadata, VmMetadata};

const METADATA_FILENAME: &str = "vm.json";
const VOLUME_METADATA_SUFFIX: &str = ".volume.json";

fn metadata_path(dir: &Path) -> PathBuf {
    dir.join(METADATA_FILENAME)
}

/// Load VM metadata from `vm.json`.
pub fn load_metadata(dir: &Path) -> Result<VmMetadata> {
    let path = metadata_path(dir);
    let data = fs::read(&path).with_context(|| format!("read metadata {}", path.display()))?;
    let mut meta: VmMetadata = serde_json::from_slice(&data)
        .with_context(|| format!("decode metadata {}", path.display()))?;

    if meta.metadata.is_empty() {
        meta.metadata = Default::default();
    }
    if meta.snapshots.is_empty() {
        meta.snapshots = Default::default();
    }
    Ok(meta)
}

/// Persist VM metadata to `vm.json` atomically.
pub fn save_metadata(dir: &Path, meta: &mut VmMetadata) -> Result<()> {
    meta.updated_at = Utc::now();
    if meta.metadata.is_empty() {
        meta.metadata = Default::default();
    }

    // @dive: Write-through temp file + rename gives crash-safe metadata replacement without torn vm.json state.
    let mut tmp = tempfile::Builder::new()
        .prefix("vm-")
        .suffix(".json")
        .tempfile_in(dir)
        .context("create temp metadata file")?;

    let path = tmp.path().to_path_buf();

    {
        let file = tmp.as_file_mut();
        let mut value = serde_json::to_value(meta)?;
        // @dive: Timestamp normalization guarantees stable UTC/RFC3339 representation across update paths.
        normalize_timestamps(&mut value);
        let data = serde_json::to_vec_pretty(&value)?;
        file.write_all(&data)?;
        file.sync_all()?;
    }

    let dst = metadata_path(dir);
    fs::rename(path, &dst).with_context(|| format!("rename metadata to {}", dst.display()))?;
    Ok(())
}

pub fn load_volume_metadata(path: &Path) -> Result<DurableVolumeMetadata> {
    let data =
        fs::read(path).with_context(|| format!("read volume metadata {}", path.display()))?;
    serde_json::from_slice(&data)
        .with_context(|| format!("decode volume metadata {}", path.display()))
}

pub fn save_volume_metadata(dir: &Path, meta: &mut DurableVolumeMetadata) -> Result<()> {
    meta.updated_at = Utc::now();
    fs::create_dir_all(dir)
        .with_context(|| format!("ensure volume metadata dir {}", dir.display()))?;
    let mut tmp = tempfile::Builder::new()
        .prefix("volume-")
        .suffix(".json")
        .tempfile_in(dir)
        .context("create temp volume metadata file")?;
    {
        let file = tmp.as_file_mut();
        let mut value = serde_json::to_value(&*meta)?;
        normalize_timestamps(&mut value);
        let data = serde_json::to_vec_pretty(&value)?;
        file.write_all(&data)?;
        file.sync_all()?;
    }
    let dst = volume_metadata_path(dir, &meta.volume_id);
    fs::rename(tmp.path(), &dst)
        .with_context(|| format!("rename volume metadata to {}", dst.display()))?;
    Ok(())
}

pub fn volume_metadata_path(dir: &Path, volume_id: &str) -> PathBuf {
    dir.join(format!("{volume_id}{VOLUME_METADATA_SUFFIX}"))
}

fn normalize_timestamps(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                // @dive: Any *_at string field is coerced to UTC so diff/reconcile logic never depends on local timezone formatting.
                if k.ends_with("_at") {
                    if let Value::String(s) = v {
                        if let Ok(parsed) = DateTime::parse_from_rfc3339(s) {
                            *s = parsed.with_timezone(&Utc).to_rfc3339();
                        }
                    }
                }
                normalize_timestamps(v);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                normalize_timestamps(item);
            }
        }
        _ => {}
    }
}
