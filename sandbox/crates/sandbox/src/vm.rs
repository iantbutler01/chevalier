// @dive-file: VM/VFS intent helpers for consumers that should not hand-build vmd mount wiring.
// @dive-rel: Used by adapters so consumers can describe scopes while chevalier owns SharedMount contracts.
// @dive-rel: Complements crate root sandbox facade; this module stays DTO/planning-only until runtime orchestration moves here.

use std::collections::HashSet;

use crate::{
    Result, SandboxError, SharedMount, SharedMountAvailability, SharedMountContinuity, proto,
};

pub const QEMU_MOUNT_TAG_MAX_LEN: usize = 31;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeOwnerRef {
    pub owner_type: String,
    pub owner_id: String,
}

impl RuntimeOwnerRef {
    pub fn new(owner_type: impl Into<String>, owner_id: impl Into<String>) -> Self {
        Self {
            owner_type: owner_type.into(),
            owner_id: owner_id.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsBackendContract {
    pub backend_profile: String,
    pub availability: SharedMountAvailability,
    pub continuity: SharedMountContinuity,
    pub fuse_backed: bool,
}

impl VfsBackendContract {
    pub fn local_path(backend_profile: impl Into<String>) -> Self {
        Self {
            backend_profile: backend_profile.into(),
            availability: SharedMountAvailability::NodeLocal,
            continuity: SharedMountContinuity::RestartSameNode,
            fuse_backed: false,
        }
    }

    pub fn shared_fuse(backend_profile: impl Into<String>) -> Self {
        Self {
            backend_profile: backend_profile.into(),
            availability: SharedMountAvailability::SharedStorage,
            continuity: SharedMountContinuity::RestoreCrossNode,
            fuse_backed: true,
        }
    }

    pub fn external_shared_path(backend_profile: impl Into<String>) -> Self {
        Self {
            backend_profile: backend_profile.into(),
            availability: SharedMountAvailability::SharedStorage,
            continuity: SharedMountContinuity::RestoreCrossNode,
            fuse_backed: false,
        }
    }

    pub fn from_classes(
        backend_profile: impl Into<String>,
        availability_class: &str,
        continuity_class: &str,
        fuse_backed: bool,
    ) -> Result<Self> {
        let availability = match availability_class.trim().to_ascii_lowercase().as_str() {
            "node-local" | "local" => SharedMountAvailability::NodeLocal,
            "shared-storage" | "shared" => SharedMountAvailability::SharedStorage,
            other => {
                return Err(SandboxError::InvalidConfig(format!(
                    "unknown shared mount availability class `{other}`"
                )));
            }
        };
        let continuity = match continuity_class.trim().to_ascii_lowercase().as_str() {
            "restart-same-node" | "same-node" => SharedMountContinuity::RestartSameNode,
            "restore-cross-node" | "cross-node" => SharedMountContinuity::RestoreCrossNode,
            other => {
                return Err(SandboxError::InvalidConfig(format!(
                    "unknown shared mount continuity class `{other}`"
                )));
            }
        };
        Ok(Self {
            backend_profile: backend_profile.into(),
            availability,
            continuity,
            fuse_backed,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsMountIntent {
    pub host_path: Option<String>,
    pub guest_path: String,
    pub mount_tag: String,
    pub read_only: bool,
    pub scope_path: String,
}

impl VfsMountIntent {
    pub fn new(
        guest_path: impl Into<String>,
        mount_tag: impl Into<String>,
        read_only: bool,
    ) -> Self {
        Self {
            host_path: None,
            guest_path: guest_path.into(),
            mount_tag: mount_tag.into(),
            read_only,
            scope_path: String::new(),
        }
    }

    pub fn root_read_only(guest_path: impl Into<String>, mount_tag: impl Into<String>) -> Self {
        Self::new(guest_path, mount_tag, true)
    }

    pub fn scoped(
        guest_path: impl Into<String>,
        mount_tag: impl Into<String>,
        scope_path: impl Into<String>,
        read_only: bool,
    ) -> Self {
        Self {
            scope_path: scope_path.into(),
            ..Self::new(guest_path, mount_tag, read_only)
        }
    }

    pub fn with_host_path(mut self, host_path: impl Into<String>) -> Self {
        self.host_path = Some(host_path.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmSurfaceSpec {
    pub owner: RuntimeOwnerRef,
    pub mounts: Vec<VfsMountIntent>,
}

pub fn build_shared_mounts(
    backend: &VfsBackendContract,
    vfs_endpoint: Option<&str>,
    intents: &[VfsMountIntent],
) -> Result<Vec<SharedMount>> {
    validate_backend_contract(backend)?;
    let endpoint = vfs_endpoint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.trim_end_matches('/').to_string());
    if backend.fuse_backed && endpoint.is_none() {
        return Err(SandboxError::InvalidConfig(
            "fuse-backed VFS mounts require a vfs_endpoint".to_string(),
        ));
    }

    let mut seen_guest_paths = HashSet::new();
    let mut seen_tags = HashSet::new();
    let mut mounts = Vec::with_capacity(intents.len());
    for intent in intents {
        validate_mount_intent(intent)?;
        if !seen_guest_paths.insert(intent.guest_path.trim().to_string()) {
            return Err(SandboxError::InvalidConfig(format!(
                "duplicate shared mount guest path `{}`",
                intent.guest_path
            )));
        }
        if !seen_tags.insert(intent.mount_tag.trim().to_string()) {
            return Err(SandboxError::InvalidConfig(format!(
                "duplicate shared mount tag `{}`",
                intent.mount_tag
            )));
        }
        let host_path = if backend.fuse_backed {
            String::new()
        } else {
            intent
                .host_path
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    SandboxError::InvalidConfig(format!(
                        "host-path VFS mount `{}` requires host_path",
                        intent.mount_tag
                    ))
                })?
                .to_string()
        };
        mounts.push(SharedMount {
            host_path,
            guest_path: intent.guest_path.trim().to_string(),
            mount_tag: intent.mount_tag.trim().to_string(),
            read_only: intent.read_only,
            availability: backend.availability.clone(),
            continuity: backend.continuity.clone(),
            backend_profile: backend.backend_profile.trim().to_string(),
            vfs_endpoint: if backend.fuse_backed {
                endpoint.clone().unwrap_or_default()
            } else {
                String::new()
            },
            vfs_scope_path: intent.scope_path.trim_matches('/').to_string(),
        });
    }
    Ok(mounts)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SharedMountSignature {
    pub guest_path: String,
    pub mount_tag: String,
    pub read_only: bool,
    pub backend_profile: String,
    pub vfs_endpoint: String,
    pub vfs_scope_path: String,
}

impl SharedMountSignature {
    pub fn from_shared_mount(mount: &SharedMount) -> Self {
        Self {
            guest_path: mount.guest_path.clone(),
            mount_tag: mount.mount_tag.clone(),
            read_only: mount.read_only,
            backend_profile: normalize_backend_profile(&mount.backend_profile),
            vfs_endpoint: mount.vfs_endpoint.trim_end_matches('/').to_string(),
            vfs_scope_path: mount.vfs_scope_path.trim_matches('/').to_string(),
        }
    }

    pub fn from_proto_mount(mount: &proto::vmd::v1::SharedMount) -> Self {
        Self {
            guest_path: mount.guest_path.clone(),
            mount_tag: mount.mount_tag.clone(),
            read_only: mount.read_only,
            backend_profile: normalize_backend_profile(&mount.backend_profile),
            vfs_endpoint: mount.vfs_endpoint.trim_end_matches('/').to_string(),
            vfs_scope_path: mount.vfs_scope_path.trim_matches('/').to_string(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManagedMountMatcher {
    exact_tags: HashSet<String>,
    tag_prefixes: Vec<String>,
}

impl ManagedMountMatcher {
    pub fn new<I, P>(exact_tags: I, tag_prefixes: P) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
        P: IntoIterator,
        P::Item: Into<String>,
    {
        Self {
            exact_tags: exact_tags.into_iter().map(Into::into).collect(),
            tag_prefixes: tag_prefixes.into_iter().map(Into::into).collect(),
        }
    }

    pub fn matches(&self, mount_tag: &str) -> bool {
        self.exact_tags.contains(mount_tag)
            || self
                .tag_prefixes
                .iter()
                .any(|prefix| mount_tag.starts_with(prefix))
    }
}

pub fn expected_mount_signatures(expected_mounts: &[SharedMount]) -> HashSet<SharedMountSignature> {
    expected_mounts
        .iter()
        .map(SharedMountSignature::from_shared_mount)
        .collect()
}

pub fn vm_mount_signatures(
    vm: &proto::vmd::v1::Vm,
    matcher: &ManagedMountMatcher,
) -> HashSet<SharedMountSignature> {
    vm.shared_mounts
        .iter()
        .filter(|mount| matcher.matches(&mount.mount_tag))
        .map(SharedMountSignature::from_proto_mount)
        .collect()
}

pub fn vm_has_mount_contract(
    vm: &proto::vmd::v1::Vm,
    expected_mounts: &[SharedMount],
    matcher: &ManagedMountMatcher,
) -> bool {
    vm_mount_signatures(vm, matcher) == expected_mount_signatures(expected_mounts)
}

pub fn vm_has_required_mounts(
    vm: &proto::vmd::v1::Vm,
    expected_mounts: &[SharedMount],
    matcher: &ManagedMountMatcher,
) -> bool {
    let actual_mounts = vm_mount_signatures(vm, matcher);
    expected_mount_signatures(expected_mounts)
        .into_iter()
        .all(|expected| actual_mounts.contains(&expected))
}

fn validate_backend_contract(backend: &VfsBackendContract) -> Result<()> {
    let backend_profile = backend.backend_profile.trim();
    if matches!(backend.availability, SharedMountAvailability::NodeLocal)
        && matches!(backend.continuity, SharedMountContinuity::RestoreCrossNode)
    {
        return Err(SandboxError::InvalidConfig(
            "node-local VFS backend cannot declare cross-node restore continuity".to_string(),
        ));
    }
    if matches!(backend.availability, SharedMountAvailability::SharedStorage)
        && backend_profile.is_empty()
    {
        return Err(SandboxError::InvalidConfig(
            "shared-storage VFS backend requires backend_profile".to_string(),
        ));
    }
    Ok(())
}

fn validate_mount_intent(intent: &VfsMountIntent) -> Result<()> {
    let guest_path = intent.guest_path.trim();
    if !guest_path.starts_with('/') {
        return Err(SandboxError::InvalidConfig(format!(
            "shared mount guest path `{guest_path}` must be absolute"
        )));
    }
    let mount_tag = intent.mount_tag.trim();
    if mount_tag.is_empty() {
        return Err(SandboxError::InvalidConfig(
            "shared mount tag must not be empty".to_string(),
        ));
    }
    if mount_tag.len() > QEMU_MOUNT_TAG_MAX_LEN {
        return Err(SandboxError::InvalidConfig(format!(
            "shared mount tag `{mount_tag}` exceeds {QEMU_MOUNT_TAG_MAX_LEN} bytes"
        )));
    }
    Ok(())
}

fn normalize_backend_profile(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proto_mount(mount: &SharedMount) -> proto::vmd::v1::SharedMount {
        proto::vmd::v1::SharedMount {
            host_path: mount.host_path.clone(),
            guest_path: mount.guest_path.clone(),
            mount_tag: mount.mount_tag.clone(),
            read_only: mount.read_only,
            availability: proto::vmd::v1::SharedMountAvailability::SharedStorage as i32,
            continuity: proto::vmd::v1::SharedMountContinuity::RestoreCrossNode as i32,
            backend_profile: mount.backend_profile.clone(),
            vfs_endpoint: mount.vfs_endpoint.clone(),
            vfs_scope_path: mount.vfs_scope_path.clone(),
        }
    }

    #[test]
    fn fuse_mounts_use_endpoint_and_hide_host_paths() {
        let mounts = build_shared_mounts(
            &VfsBackendContract::shared_fuse("gcs-vfs-fuse"),
            Some("http://api.internal/v1/vfs/runtime-1/"),
            &[
                VfsMountIntent::root_read_only("/workspace", "runtimefs")
                    .with_host_path("/ignored/when/fuse"),
                VfsMountIntent::scoped(
                    "/workspace/mounts/task",
                    "rfs-cur-task",
                    ".runtime/current-task-workspace",
                    false,
                ),
            ],
        )
        .expect("mounts build");

        assert_eq!(mounts[0].host_path, "");
        assert_eq!(
            mounts[0].vfs_endpoint,
            "http://api.internal/v1/vfs/runtime-1"
        );
        assert_eq!(mounts[0].vfs_scope_path, "");
        assert_eq!(mounts[1].vfs_scope_path, ".runtime/current-task-workspace");
        assert_eq!(
            mounts[1].availability,
            SharedMountAvailability::SharedStorage
        );
        assert_eq!(
            mounts[1].continuity,
            SharedMountContinuity::RestoreCrossNode
        );
    }

    #[test]
    fn host_path_mounts_require_host_paths_and_clear_endpoint() {
        let mounts = build_shared_mounts(
            &VfsBackendContract::local_path("local-path"),
            Some("http://unused"),
            &[VfsMountIntent::root_read_only("/workspace", "runtimefs")
                .with_host_path("/tmp/runtime")],
        )
        .expect("mounts build");

        assert_eq!(mounts[0].host_path, "/tmp/runtime");
        assert_eq!(mounts[0].vfs_endpoint, "");
        assert_eq!(mounts[0].availability, SharedMountAvailability::NodeLocal);
        assert_eq!(mounts[0].continuity, SharedMountContinuity::RestartSameNode);

        let err = build_shared_mounts(
            &VfsBackendContract::local_path("local-path"),
            None,
            &[VfsMountIntent::root_read_only("/workspace", "runtimefs")],
        )
        .expect_err("host path is required");
        assert!(err.to_string().contains("requires host_path"));
    }

    #[test]
    fn rejects_duplicate_tags_and_invalid_continuity() {
        let err = build_shared_mounts(
            &VfsBackendContract::shared_fuse("gcs-vfs-fuse"),
            Some("http://api"),
            &[
                VfsMountIntent::root_read_only("/workspace", "runtimefs"),
                VfsMountIntent::scoped("/other", "runtimefs", "other", false),
            ],
        )
        .expect_err("duplicate tag rejected");
        assert!(err.to_string().contains("duplicate shared mount tag"));

        let err = build_shared_mounts(
            &VfsBackendContract {
                backend_profile: "bad".to_string(),
                availability: SharedMountAvailability::NodeLocal,
                continuity: SharedMountContinuity::RestoreCrossNode,
                fuse_backed: false,
            },
            None,
            &[],
        )
        .expect_err("invalid continuity rejected");
        assert!(err.to_string().contains("node-local"));
    }

    #[test]
    fn mount_contract_comparison_detects_stale_vms() {
        let expected = build_shared_mounts(
            &VfsBackendContract::shared_fuse("gcs-vfs-fuse"),
            Some("http://api"),
            &[VfsMountIntent::scoped(
                "/workspace/projects/p/shared",
                "rfs-sh-p",
                "projects/p/shared",
                false,
            )],
        )
        .expect("mounts build");
        let matcher = ManagedMountMatcher::new(std::iter::empty::<&str>(), ["rfs-sh"]);
        let mut vm = proto::vmd::v1::Vm {
            shared_mounts: expected.iter().map(proto_mount).collect(),
            ..Default::default()
        };

        assert!(vm_has_mount_contract(&vm, &expected, &matcher));

        vm.shared_mounts[0].vfs_endpoint = "http://stale-api".to_string();
        assert!(!vm_has_mount_contract(&vm, &expected, &matcher));
        assert!(!vm_has_required_mounts(&vm, &expected, &matcher));

        vm.shared_mounts[0].vfs_endpoint = "http://api/".to_string();
        assert!(vm_has_mount_contract(&vm, &expected, &matcher));

        vm.shared_mounts[0].vfs_scope_path = "projects/stale/shared".to_string();
        assert!(!vm_has_mount_contract(&vm, &expected, &matcher));
        assert!(!vm_has_required_mounts(&vm, &expected, &matcher));

        vm.shared_mounts[0] = proto_mount(&expected[0]);
        vm.shared_mounts[0].vfs_endpoint = "http://stale-api".to_string();
        assert!(!vm_has_mount_contract(&vm, &expected, &matcher));
    }
}
