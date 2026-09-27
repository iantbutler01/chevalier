use anyhow::{Context, Result, bail};
use chevalier_sandbox::placement::{GIB, NodeCapacity, ResourceDemand};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use sysinfo::System;

pub(crate) fn sample(
    path: &Path,
    committed: ResourceDemand,
    creating: ResourceDemand,
) -> Result<NodeCapacity> {
    let mut system = System::new();
    system.refresh_memory();
    let mut memory_total = system.total_memory();
    let mut memory_free = system.available_memory();
    if let Some(limits) = system.cgroup_limits() {
        memory_total = memory_total.min(limits.total_memory);
        memory_free = memory_free.min(limits.free_memory);
    }
    let cpu_total = std::thread::available_parallelism()
        .context("read CPU capacity")?
        .get() as f64;
    let load = System::load_average().one;
    let (disk_total, disk_free) = disk_space(path)?;
    if memory_total == 0 || disk_total == 0 || !load.is_finite() {
        bail!("host capacity metrics are unavailable");
    }
    let memory_reserve = (memory_total / 20).max(GIB / 2);
    let disk_reserve = disk_reserve(disk_total);
    Ok(NodeCapacity {
        active_vms: 0,
        sampled_at_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
        cpu_available: (cpu_total - load.max(committed.cpu_cores)).max(0.0),
        memory_available_bytes: memory_free
            .min(memory_total.saturating_sub(committed.memory_bytes))
            .saturating_sub(memory_reserve),
        disk_available_bytes: disk_free
            .saturating_sub(disk_reserve)
            .saturating_sub(creating.disk_bytes),
        memory_reserve_bytes: memory_reserve,
        disk_reserve_bytes: disk_reserve,
    })
}

fn disk_reserve(total: u64) -> u64 {
    use crate::fuse::local_view::{
        DEFAULT_BACKING_FREE_BYTES_FLOOR, DEFAULT_BACKING_FREE_FRACTION_FLOOR,
    };
    let bytes = std::env::var("CHEVALIER_VMD_BACKING_FREE_BYTES_FLOOR")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_BACKING_FREE_BYTES_FLOOR);
    let fraction = std::env::var("CHEVALIER_VMD_BACKING_FREE_FRACTION_FLOOR")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
        .unwrap_or(DEFAULT_BACKING_FREE_FRACTION_FLOOR);
    bytes.max((total as f64 * fraction.max(0.1)).ceil() as u64)
}

#[cfg(unix)]
fn disk_space(root: &Path) -> Result<(u64, u64)> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let path = CString::new(root.as_os_str().as_bytes())?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("read backing filesystem capacity");
    }
    let stats = unsafe { stats.assume_init() };
    let block_size = if stats.f_frsize == 0 {
        stats.f_bsize
    } else {
        stats.f_frsize
    } as u64;
    Ok((
        (stats.f_blocks as u64).saturating_mul(block_size),
        (stats.f_bavail as u64).saturating_mul(block_size),
    ))
}

#[cfg(not(unix))]
fn disk_space(_root: &Path) -> Result<(u64, u64)> {
    bail!("backing filesystem capacity is unavailable on this platform")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_the_actual_backing_filesystem_and_accounts_for_pending_writes() {
        let directory = tempfile::tempdir().unwrap();
        let capacity = sample(
            directory.path(),
            ResourceDemand::default(),
            ResourceDemand::default(),
        )
        .unwrap();
        assert!(capacity.sampled_at_unix_ms > 0);
        assert!(capacity.memory_reserve_bytes >= GIB / 2);
        assert!(capacity.disk_reserve_bytes >= GIB);
        let full = ResourceDemand {
            cpu_cores: f64::MAX,
            memory_bytes: u64::MAX,
            disk_bytes: u64::MAX,
        };
        let blocked = sample(directory.path(), full, full).unwrap();
        assert_eq!(blocked.cpu_available, 0.0);
        assert_eq!(blocked.memory_available_bytes, 0);
        assert_eq!(blocked.disk_available_bytes, 0);
    }
}
