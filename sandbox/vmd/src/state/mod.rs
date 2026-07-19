// @dive-file: State subsystem module root exporting manager, metadata, runtime, and VM types.
// @dive-rel: Consumed by app/control paths that orchestrate VM lifecycle operations.
// @dive-rel: Defines the cohesive state API boundary used throughout vmd runtime code.

pub mod manager;
pub mod metadata;
pub mod runtime;
pub mod types;

pub use manager::{Manager, ManagerError, ManagerResult, PendingSnapshot, SnapshotParams};
pub use metadata::{load_metadata, save_metadata};
pub use runtime::VmRuntime;
pub use types::{
    CreateVmParams, DurableVolumeAttachment, DurableVolumeMetadata, ForkVmParams, NetworkSpec,
    ResourceSpec, SharedMountAvailability, SharedMountContinuity, SharedMountSpec,
    SnapshotMetadata, SnapshotRecord, UpdateVmParams, Vm, VmMetadata, VmSource, VmSourceType,
    VmState,
};
