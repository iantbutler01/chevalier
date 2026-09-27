use serde::{Deserialize, Serialize};

pub const ADMISSION_REJECTED_HEADER: &str = "x-chevalier-admission-rejected";

pub const CAPACITY_MAX_AGE_MS: u64 = 45_000;
pub const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ResourceDemand {
    pub cpu_cores: f64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

impl ResourceDemand {
    pub fn for_vm(vcpu: i32, memory_mb: i32, disk_gb: i32, volume_gb: i32) -> Self {
        Self {
            cpu_cores: vcpu.max(1) as f64,
            memory_bytes: (if memory_mb > 0 { memory_mb } else { 1024 }) as u64 * 1024 * 1024,
            disk_bytes: ((if disk_gb > 0 { disk_gb } else { 10 }) as u64 + volume_gb.max(0) as u64)
                * GIB,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub active_vms: usize,
    pub sampled_at_unix_ms: u64,
    pub cpu_available: f64,
    pub memory_available_bytes: u64,
    pub disk_available_bytes: u64,
    pub memory_reserve_bytes: u64,
    pub disk_reserve_bytes: u64,
}

impl NodeCapacity {
    pub fn rejection(&self, demand: ResourceDemand, now_ms: u64) -> Option<String> {
        if self.sampled_at_unix_ms > now_ms.saturating_add(5_000)
            || now_ms.saturating_sub(self.sampled_at_unix_ms) > CAPACITY_MAX_AGE_MS
        {
            return Some("host capacity telemetry is stale".into());
        }
        if !self.cpu_available.is_finite() || self.cpu_available < demand.cpu_cores {
            return Some(format!(
                "CPU headroom: {:.2} cores available, {:.2} requested",
                self.cpu_available, demand.cpu_cores
            ));
        }
        if self.memory_available_bytes < demand.memory_bytes {
            return Some(format!(
                "RAM headroom: {} bytes available, {} requested ({} reserved for host)",
                self.memory_available_bytes, demand.memory_bytes, self.memory_reserve_bytes
            ));
        }
        if self.disk_available_bytes < demand.disk_bytes {
            return Some(format!(
                "disk headroom: {} bytes available, {} requested ({} reserved for host)",
                self.disk_available_bytes, demand.disk_bytes, self.disk_reserve_bytes
            ));
        }
        None
    }

    pub fn headroom(&self, demand: ResourceDemand) -> f64 {
        (self.cpu_available / demand.cpu_cores.max(1.0))
            .min(self.memory_available_bytes as f64 / demand.memory_bytes.max(1) as f64)
            .min(self.disk_available_bytes as f64 / demand.disk_bytes.max(1) as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capacity() -> NodeCapacity {
        NodeCapacity {
            active_vms: 0,
            sampled_at_unix_ms: 100_000,
            cpu_available: 12.0,
            memory_available_bytes: 32 * GIB,
            disk_available_bytes: 100 * GIB,
            memory_reserve_bytes: GIB,
            disk_reserve_bytes: 10 * GIB,
        }
    }

    #[test]
    fn admission_requires_every_resource_and_fresh_metrics() {
        let demand = ResourceDemand::for_vm(2, 2048, 16, 16);
        assert_eq!(demand.disk_bytes, 32 * GIB);
        let healthy = capacity();
        assert!(healthy.rejection(demand, 110_000).is_none());
        let mut low_disk = healthy.clone();
        low_disk.disk_available_bytes = 15 * GIB;
        assert!(
            low_disk
                .rejection(demand, 110_000)
                .unwrap()
                .contains("disk")
        );
        let mut low_ram = healthy.clone();
        low_ram.memory_available_bytes = GIB;
        assert!(low_ram.rejection(demand, 110_000).unwrap().contains("RAM"));
        let mut busy = healthy.clone();
        busy.cpu_available = 1.0;
        assert!(busy.rejection(demand, 110_000).unwrap().contains("CPU"));
        assert!(
            healthy
                .rejection(demand, 146_000)
                .unwrap()
                .contains("stale")
        );
        assert!(healthy.rejection(demand, 90_000).unwrap().contains("stale"));
    }

    #[test]
    fn bottleneck_ranking_does_not_hide_disk_pressure_behind_cpu_or_ram() {
        let demand = ResourceDemand::for_vm(2, 2048, 16, 16);
        let healthy = capacity();
        let mut imbalanced = healthy.clone();
        imbalanced.cpu_available = 120.0;
        imbalanced.memory_available_bytes = 512 * GIB;
        imbalanced.disk_available_bytes = 33 * GIB;
        assert!(healthy.headroom(demand) > imbalanced.headroom(demand));
    }
}
