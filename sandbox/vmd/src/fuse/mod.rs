pub mod auth;
pub mod change_stream;
#[cfg(test)]
mod change_stream_tests;
pub mod client;
pub mod computer_mounts;
pub mod control;
mod dispatch;
pub mod fs;
pub mod handle;
#[cfg(test)]
mod lazy_content_tests;
pub(crate) mod local_view;
#[cfg(test)]
mod readonly_reconcile_tests;

pub use handle::{
    ComputerMountsOptions, DEFAULT_VFS_DRAIN_TIMEOUT, FollowOptions, FuseHandle,
    GUARDED_VFS_DRAIN_TIMEOUT, UnpublishedMountState, VfsPublicationStatus,
    active_mountpoints_under, default_vfs_state_dir, mount_remote_vfs_fuse,
    mount_remote_vfs_fuse_following, mount_vfs_fuse, report_unpublished_vfs_state_under,
    unmount_active_mountpoints_under, unmount_fuse, unmount_fuse_with_drain,
    warn_unpublished_vfs_state,
};
