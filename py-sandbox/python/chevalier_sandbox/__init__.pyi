from typing import Awaitable, Dict, List, Optional
from typing_extensions import Literal, Required, TypedDict, final

class ExecEvent(TypedDict, total=False):
    type: Required[Literal["stdout", "stderr", "exit", "timeout"]]
    data: bytes
    code: int

class ShellEvent(TypedDict, total=False):
    type: Required[Literal["output", "exit"]]
    data: bytes
    code: int

class ExecOpts(TypedDict, total=False):
    env: Dict[str, str]
    timeout_secs: int
    detach: bool
    shell: str
    close_stdin_on_start: bool

class ShellOpts(TypedDict, total=False):
    shell: str
    args: List[str]
    env: Dict[str, str]
    cwd: str
    cols: int
    rows: int

class SharedMountOpts(TypedDict, total=False):
    host_path: str
    guest_path: Required[str]
    mount_tag: Required[str]
    read_only: bool
    availability: str
    continuity: str
    backend_profile: str
    vfs_endpoint: str
    vfs_scope_path: str

class SessionOpts(TypedDict, total=False):
    session_id: str
    name: str
    image: str
    architecture: str
    metadata: Dict[str, str]
    auto_start: bool
    shared_mounts: List[SharedMountOpts]
    egress_allowlist: List[str]
    pci_device_ids: List[str]
    storage_profile: str
    volume_owner_key: str
    volume_size_gb: int

class ForkOpts(TypedDict, total=False):
    child_name: str
    child_metadata: Dict[str, str]
    auto_start_child: bool

class SessionSnapshotOpts(TypedDict, total=False):
    label: str
    description: str

class OpenComputerMountOpts(TypedDict, total=False):
    path: str
    driver: str
    remote: str
    backend: str
    command: List[str]
    env: Dict[str, str]
    secrets: Dict[str, str]
    creds: Dict[str, str]
    rclone_config: str
    read_only: bool
    mount_options: List[str]

class OpenComputerProviderOpts(TypedDict, total=False):
    api_url: str
    api_key: str
    template_id: str
    timeout_secs: float
    default_cpu_count: int
    default_memory_mb: int
    default_disk_mb: int
    burst: bool
    secret_store: str
    egress_allowlist: List[str]
    mounts: List[OpenComputerMountOpts]
    shared_mounts: Dict[str, OpenComputerMountOpts]

class SandboxConnectOptions(TypedDict, total=False):
    auth_token: str
    pci_access_token: str
    connect_timeout_ms: float
    default_image: str
    default_architecture: str
    default_vcpu: int
    default_memory_mb: int
    default_disk_gb: int
    provider: str
    open_computer: OpenComputerProviderOpts

class SessionDirectoryEntry(TypedDict):
    name: str
    is_dir: bool
    is_symlink: bool

class SessionCheckpoint(TypedDict):
    id: str

class SessionSnapshot(TypedDict):
    id: str
    name: str
    label: str
    description: str

class SessionInfo(TypedDict, total=False):
    session_id: Required[str]
    vm_id: Required[str]
    name: Required[str]
    state: Required[int]
    parent_session_id: Optional[str]
    fork_id: Optional[str]

class DurableVolumeInfo(TypedDict, total=False):
    owner_key: Required[str]
    volume_id: Required[str]
    size_gb: Required[int]
    created_at_ms: Required[int]
    updated_at_ms: Required[int]
    backing_volume_id: Optional[str]
    attached_vm_ids: Required[List[str]]

class HostPciFunction(TypedDict):
    bdf: str
    vendor_id: str
    device_id: str
    class_code: str
    driver: str
    iommu_group: str

class HostPciDevice(TypedDict):
    id: str
    label: str
    functions: List[HostPciFunction]
    state: str
    assigned_vm_id: str
    managed: bool
    hotplug_capable: bool
    unavailable_reason: str

class HostPciInventory(TypedDict):
    enabled: bool
    devices: List[HostPciDevice]

class PciDeviceAction(TypedDict, total=False):
    device: Optional[HostPciDevice]
    restart_required: Required[bool]
    detail: Required[str]
    vm_state: Required[str]

@final
class ExecHandle:
    def write(self, data: bytes) -> Awaitable[None]: ...
    def eof(self) -> Awaitable[None]: ...
    def signal(self, sig: int) -> Awaitable[None]: ...
    def resize(self, cols: int, rows: int) -> Awaitable[None]: ...
    def next(self) -> Awaitable[Optional[ExecEvent]]: ...

@final
class ShellHandle:
    def write(self, data: bytes) -> Awaitable[None]: ...
    def eof(self) -> Awaitable[None]: ...
    def resize(self, cols: int, rows: int) -> Awaitable[None]: ...
    def next(self) -> Awaitable[Optional[ShellEvent]]: ...

@final
class ForwardHandle:
    @property
    def guest_port(self) -> int: ...
    @property
    def host_port(self) -> int: ...
    def close(self) -> Awaitable[None]: ...

@final
class Session:
    @property
    def session_id(self) -> str: ...
    @property
    def vm_id(self) -> str: ...
    def exec(self, command: str, options: Optional[ExecOpts] = ...) -> Awaitable[ExecHandle]: ...
    def shell(self, options: Optional[ShellOpts] = ...) -> Awaitable[ShellHandle]: ...
    def read_file(self, path: str) -> Awaitable[bytes]: ...
    def list_dir(self, path: str) -> Awaitable[List[SessionDirectoryEntry]]: ...
    def write_file(self, path: str, data: bytes) -> Awaitable[None]: ...
    def write_file_from_file(self, path: str, source_path: str, mode: Optional[int] = ...) -> Awaitable[None]: ...
    def fork(self, options: Optional[ForkOpts] = ...) -> Awaitable[Session]: ...
    def checkpoint(self, name: str) -> Awaitable[SessionCheckpoint]: ...
    def restore_checkpoint(self, checkpoint_id: str) -> Awaitable[Session]: ...
    def get_state(self) -> Awaitable[str]: ...
    def list_pci_devices(self) -> Awaitable[HostPciInventory]: ...
    def attach_pci_device(self, device_id: str) -> Awaitable[PciDeviceAction]: ...
    def detach_pci_device(self, device_id: str) -> Awaitable[PciDeviceAction]: ...
    def pause(self) -> Awaitable[str]: ...
    def start(self) -> Awaitable[str]: ...
    def restart(self) -> Awaitable[str]: ...
    def resume(self) -> Awaitable[str]: ...
    def stop(self) -> Awaitable[str]: ...
    def snapshot(self, options: Optional[SessionSnapshotOpts] = ...) -> Awaitable[SessionSnapshot]: ...
    def restore(self, snapshot_id: str) -> Awaitable[str]: ...
    def list_snapshots(self) -> Awaitable[List[SessionSnapshot]]: ...
    def delete_snapshot(self, snapshot_id: str) -> Awaitable[None]: ...
    def forward_port(self, guest_port: int) -> Awaitable[ForwardHandle]: ...
    def provider_preview_url(self, guest_port: int) -> Awaitable[str]: ...
    def close(self) -> Awaitable[None]: ...
    def discard(self) -> Awaitable[None]: ...

@final
class Sandbox:
    @staticmethod
    def connect(endpoint: str, options: Optional[SandboxConnectOptions] = ...) -> Awaitable[Sandbox]: ...
    def session(self, options: Optional[SessionOpts] = ...) -> Awaitable[Session]: ...
    def attach_session(self, session_id: str) -> Awaitable[Session]: ...
    def attach_session_passive(self, session_id: str) -> Awaitable[Session]: ...
    def list_sessions(self) -> Awaitable[List[SessionInfo]]: ...
    def list_durable_volumes(self) -> Awaitable[List[DurableVolumeInfo]]: ...
    def delete_durable_volume(self, owner_key: str) -> Awaitable[None]: ...
    def list_host_pci_devices(self) -> Awaitable[HostPciInventory]: ...
    def discard_session_by_id(self, session_id: str) -> Awaitable[None]: ...
