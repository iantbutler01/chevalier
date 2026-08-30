# Native-architecture Windows QEMU image builder

This directory builds native ARM64/HVF and AMD64/KVM Windows base images from
pinned Microsoft installation media. Packer drives unattended Windows Setup,
installs the matching guest substrate, runs an actual native WinFsp MEMFS
probe, removes build access, and seals the disk with Sysprep.

The completed services image contains the native `ChevalierVFS` and
`ChevalierGuest` binaries, their SCM registrations, and a static secretless
answer at `C:\Windows\Panther\Unattend\Unattend.xml`. Windows Setup consumes
that answer during normal `specialize` and `oobeSystem` passes, and
`SetupComplete.cmd` installs the baked services. At first boot,
`ChevalierGuest` imports fresh VM identity, scope, endpoint, credentials, and
bearers from a one-use FAT runtime disk, initializes the separate NTFS state
disk, starts the `W:` mount, removes one-use autologon data and cached answers,
and reports authenticated readiness. No local development key, runtime bearer,
scope, VM identity, or reusable desktop password is sealed into the base.

## Current evidence

The accepted services image is
`output/windows-11-iot-enterprise-ltsc-2024-arm64-services-v12`. Its qcow2 is
9,019,392,000 bytes with SHA-256
`7f474327146e87b1ea2e523999b156725385f609ff0949ea9777d30d7027a54c`.
The 2026-08-30 untouched-clone acceptance run started that image through vmd,
let Windows consume the embedded answer and install the baked services,
detached and deleted the one-use runtime disk before readiness, and proved:

- authenticated argv execution and bounded native file RPCs;
- an active desktop session and outbound public HTTPS;
- WinFsp create, write, `FlushFileBuffers`, read, forced delete, and basic-info
  changes on `W:`;
- Git init, add, commit, and strict/full fsck on the real mount;
- gateway publication drain and exact host bytes; and
- same-node stop/restart with the same state disk and Git HEAD.

`service-acceptance.json` binds this evidence to the image digest. The manifest
is `sealed-local-acceptance` and remains `productionReady:false` until the
power-cut WAL recovery, fresh identity matrix, and persistent runtime TPM gates
pass.

The accepted AMD64/KVM image is
`output/windows-11-enterprise-25h2-amd64-production`. Its qcow2 is
9,890,430,976 bytes with SHA-256
`f8f832521864d17ec87e9e2a62b4357804606450da64150488bc0574d650c234`.
The clean build completed in 20 minutes 4 seconds. An untouched two-clone run
passed command/file control, outbound HTTPS, WinFsp I/O, Git lock/rename and
branch/merge/stash/gc/fsck, gateway outage, offline WAL recovery after forced
VFS-service death, publication drain, cold restart, and distinct hostname,
MachineGuid, and machine SID. Its manifest has only
`persistent-runtime-tpm` left in `notProved`.

The 2026-08-17 clean build completed from ISO to Packer artifact in 8 minutes 21
seconds. The ignored output directory contains a 64 GiB-virtual qcow2 and build
EFI variables:

- qcow2 size 8,949,399,552 bytes, SHA-256
  `cf6e2db6aded851b09eef04660eb5c5cce5e9f2252a246641c59d2ed0c3467f5`;
- EFI variables size 67,108,864 bytes, SHA-256
  `6fdaadd529ae8adaf341c02d22c8c05a6d26eb9c517f2427008fa7ac7d3d7a02`;
- `qemu-img check` reports no errors; and
- `image-manifest.json` deliberately says `sealed-local-diagnostic` and
  `productionReady: false`.

A read-only disk audit found the Sysprep success tag and no receipt credential,
build script, stage file, or build-error residue. The embedded runtime answer
contains no secret and is removed after a clone consumes it. A disposable
qcow2 overlay using fresh EFI variables then booted Windows Boot Manager,
specialized successfully, generated a new CAPI machine GUID, advanced OOBE to
`IMAGE_STATE_COMPLETE`, and honored a graceful ACPI shutdown. The ARM ramfb was
black after firmware handoff, so this is log-backed first-boot evidence, not a
visual desktop/display acceptance result.

## Pinned inputs

- Windows 11 IoT Enterprise LTSC 2024 Evaluation ARM64, 5,042,194,432 bytes,
  observed SHA-256
  `3dcdba9c9c0aa0430d4332b60c9afcb3cd613d648a49cbba2d4ef7b5978f32e8`;
- virtio-win guest tools `0.1.285`, independently verified SHA-256
  `c8b4a9fe87e1fc5d8e843495e082dea53420587fe04740b1084d85089343f04d`;
- virtio-win driver ISO `0.1.285`, independently cross-verified SHA-512
  `4f13070cc9241fa342deab4ebfac360565030580ff77b6e5f1951a64627621e5da4abfd30e1e46ca8bae2bb7dd4ff98141aff424142c9629a5876a61283962e5`;
- WinFsp `2.2.26215` (`2.2B4`), upstream SHA-256
  `2ecb5c89405488a95bbd8a01875e02c48534fd37bbdfd84488f7590464d65944`;
- PowerShell `7.6.5` ARM64, Visual C++ runtime `14.51.36247.0` ARM64,
  ripgrep `15.2.0` ARM64, and Git for Windows `2.55.0.windows.4` x64, each
  pinned to the digest in `fetch-artifacts.sh`.
- Windows 11 Enterprise 25H2 Evaluation x64, SHA-256
  `a61adeab895ef5a4db436e0a7011c92a2ff17bb0357f58b13bbc4062e535e7b9`;
- PowerShell `7.6.5` x64, Visual C++ runtime `14.51.36247.0` x64, and ripgrep
  `15.2.0` x64, each pinned in `fetch-artifacts.sh`; and
- Microsoft-enrolled OVMF code/variables pinned to the exact hashes recorded
  in `fetch-artifacts.sh`.

Git runs through Windows on ARM's x64 emulation until Git for Windows ships a
native ARM64 distribution. Microsoft's published hash PDF currently prints a
malformed 65-character digest for the evaluation ARM64 object, so this builder
pins the exact Microsoft CDN object and measured hash and fails closed on any
byte change.

## Build

Prerequisites are QEMU, Packer, `7z`, and pinned ARM secure EDK2 firmware. The
default firmware source is UTM's signed bundle; UTM is not the builder or VM
runtime. Override `OPENBRACKET_ARM_EFI_CODE` and
`OPENBRACKET_ARM_EFI_VARS` with equivalent pinned inputs on another host.

```bash
cd sandbox/windows-image
./scripts/fetch-artifacts.sh
./scripts/build-guest-services.sh
packer init windows.pkr.hcl
packer fmt -check windows.pkr.hcl
PKR_VAR_build_password="$(openssl rand -base64 24)" packer validate windows.pkr.hcl
PKR_VAR_build_password="$(openssl rand -base64 24)" packer build -force windows.pkr.hcl
./scripts/seal-output.sh
```

On x86-64 Linux with KVM, QEMU, and Microsoft-enrolled OVMF:

```bash
cd sandbox/windows-image
OPENBRACKET_WINDOWS_ARCHITECTURE=amd64 ./scripts/fetch-artifacts.sh
./scripts/build-guest-services.sh
packer init windows-amd64.pkr.hcl
packer fmt -check windows-amd64.pkr.hcl
PKR_VAR_build_password="$(openssl rand -hex 24)" \
  packer validate windows-amd64.pkr.hcl
PKR_VAR_build_password="$(openssl rand -hex 24)" \
  packer build windows-amd64.pkr.hcl
OPENBRACKET_WINDOWS_ARCHITECTURE=amd64 \
  ./scripts/seal-output.sh output/windows-11-enterprise-25h2-amd64
```

Use a new password for validation and build, and never print or reuse it. The
password exists only on the answer media, as the temporary Audit Mode autologon
credential, and as the bearer for the one-way build receipt.

The build is guest-driven and has no Packer communicator. `Autounattend.xml`
starts `bootstrap-image.ps1` from the answer media; bootstrap installs the
artifacts and re-arms exactly one Audit Mode autologon plus `RunOnce` across the
required reboot. The completion script verifies the guest, removes the build
channel and secret, sends an authenticated `verified` receipt, and starts
Sysprep `/generalize /oobe /shutdown`. The host accepts the artifact only after
both the receipt and QEMU shutdown. A later `failure` receipt overrides an
earlier verification if Sysprep returns an error.

The finalizer also asserts that WinRM is disabled and has no listeners, Basic
authentication, unencrypted transport, or firewall exposure. The ARM builder
never enables or uses WinRM.

The wrapper converts Packer's PC-style CD-ROM drives to USB storage, adds xHCI,
USB input, and `ramfb`, and runs `qemu-system-aarch64` with
`virt-10.2,highmem=off`, HVF, `-cpu host`, 4 vCPUs, and 3,072 MiB RAM. The root
disk is NVMe using the Windows inbox driver. ARM64 `viostor` remains injected
for later data-disk testing. NetKVM is active for networking.

The build does not create a disposable TPM. Runtime VMs need unique persistent
TPM state tied to their own EFI variables and VM identity. BitLocker and
automatic device encryption remain disabled so this diagnostic base is not
bound to ephemeral or shared TPM state.

## What verification proves

The build gate verifies native-architecture Windows under a hypervisor, signed
WinFsp files, Git, PowerShell, and ripgrep execution, BitLocker disabled, no
selected pending reboot, and a real native MEMFS mount/read/write/delete cycle
at `W:`.

The service gate additionally verifies the product WinFsp and authenticated
control services using fresh per-VM credentials, a one-use runtime disk, and a
separate state disk. Both guest services cross-compile for ARM64 and AMD64;
vmd has native ARM64/HVF and AMD64/KVM-or-HVF launch shapes and rejects TCG.
Both sealed-image architectures have native-host live acceptance evidence.
Production promotion still requires:

- a gateway-enforced mount-generation authority fence;
- power-cut and prepare/apply/commit crash recovery;
- persistent runtime TPM and repeated fresh-identity proof;
- Windows share/delete/rename/mmap/lock/oplock/notification/path gates;
- repeated fresh-identity, lifecycle, security, fault, and leak tests.

Windows 11 IoT Enterprise LTSC Evaluation is a stable prototype source. A
durable product image should resolve Microsoft's current normal Windows 11
ARM64 ISO per build and record its official release hash.
