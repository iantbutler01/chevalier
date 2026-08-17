# Windows QEMU image builder

This directory builds the pinned `windows-11-enterprise-25h2-x64-v1` base image from Microsoft's
installation ISO. It does not consume or publish a Vagrant box.

The supported build host is x86_64 Linux with KVM, QEMU, OVMF, `swtpm`, `packer`, `7z`, and enough
free space for a 64 GiB sparse qcow2 image. Packer drives Windows Setup through Microsoft's
`Autounattend.xml` mechanism, boots the reference installation into Audit Mode, provisions over
build-only WinRM, verifies the guest prerequisites, removes the temporary listener,
and runs Sysprep to OOBE before capture. Audit Mode keeps image construction independent of the
network-triggered Windows Update ZDP phase in consumer OOBE.

Pinned inputs:

- Windows 11 Enterprise 25H2 Evaluation x64, Microsoft SHA-256
  `a61adeab895ef5a4db436e0a7011c92a2ff17bb0357f58b13bbc4062e535e7b9`;
- virtio-win guest tools `0.1.285`, independently verified SHA-256
  `c8b4a9fe87e1fc5d8e843495e082dea53420587fe04740b1084d85089343f04d`;
- virtio-win driver ISO `0.1.285`, independently cross-verified SHA-512
  `4f13070cc9241fa342deab4ebfac360565030580ff77b6e5f1951a64627621e5da4abfd30e1e46ca8bae2bb7dd4ff98141aff424142c9629a5876a61283962e5`;
- WinFsp `2.2.26194` (`2.2B3`), upstream SHA-256
  `7b41020618cdcc33d699d0e15c1df660f0762a09b57080049c565857ac00bd9d`.

The fetch step extracts the Windows 11 x64 `viostor` driver into the answer CD's conventional
`$WinPEDriver$` directory. Windows Setup therefore installs directly onto the same VirtIO block
device model used by the current Linux QEMU runtime instead of relying on a build-only IDE disk.

The WinFsp prerelease is intentional: the stable 2.1 build predates the current security and
cached-write/rename-deadlock fixes. Promotion still requires the Windows VirtioFS acceptance matrix
in OpenBracket Spec 59.

## Build on Linux/KVM

```bash
cd sandbox/windows-image
./scripts/fetch-artifacts.sh
packer init .
packer fmt -check .
PKR_VAR_build_password="$(openssl rand -base64 24)" packer validate .
PKR_VAR_build_password="$(openssl rand -base64 24)" packer build .
```

The password is build-only. The Audit Mode answer pass supplies the same value to the paired
`AutoLogon` and `AdministratorPassword` settings required by Windows Setup. The finalizer replaces
it with an unknown random value, removes WinRM listeners, cached answer files, and staging, then
Sysprep disables the built-in Administrator account. Do not restart the output image before cloning
it with fresh OVMF VARS, persistent TPM state, machine identity, and the per-clone bootstrap contract.

Packer uses temporary Basic WinRM because its Go NTLM client does not authenticate reliably against
this Audit Mode endpoint even though an independent NTLM client does. The QEMU host forward is
explicitly bound to `127.0.0.1`; the build host must remain single-user and trusted while the build is
running. The finalizer removes the Basic/unencrypted policy, disables both settings, removes every
listener, disables the firewall rules and WinRM service, and only then seals the image.

## Diagnostic build on Apple Silicon

An x64 build on Apple Silicon uses TCG and is expected to be slow:

```bash
PKR_VAR_build_password="$(openssl rand -base64 24)" packer build \
  -var accelerator=tcg \
  -var cpu_model=max \
  -var skip_compaction=true \
  -var qemu_binary="$PWD/scripts/qemu-tcg-wrapper.sh" \
  -var efi_firmware_code=/opt/homebrew/share/qemu/edk2-x86_64-secure-code.fd \
  -var efi_firmware_vars=/opt/homebrew/share/qemu/edk2-i386-vars.fd \
  .
```

Homebrew QEMU on macOS does not expose `vhost-user-fs-pci`, and Chevalier's patched `virtiofsd` is
Linux-only. A Mac build can prove unattended Windows installation and guest package provisioning,
but cannot prove the required host FUSE → patched virtiofsd → Windows VirtioFsSvc/WinFsp path.
The diagnostic wrapper waits one second before QEMU starts because Packer's QEMU plugin does not
wait for its newly launched `swtpm` control socket on macOS.

## Local diagnostic checkpoint

The ignored `output/windows-11-enterprise-25h2-x64` directory contains the 2026-08-17 local TCG
checkpoint: a generalized 64 GiB qcow2, its build EFI variables, `SHA256SUMS`, and an as-built
`image-manifest.json`. The qcow2 passed `qemu-img check` and has SHA-256
`43773d7f93863e1ccd1b2c1a6965d1edff38ffc3ba607269a9ad0f889c587681`.

That run proved Windows Setup on a VirtIO root disk, WinFsp/VirtIO installation, the guest
verification script, build-channel cleanup, credential rotation, and Sysprep generalize/shutdown.
It was recovered manually after exposing the Packer NTLM incompatibility, so it does not count as a
fresh end-to-end Packer pass. It also cannot be promoted until the Linux/KVM host-FUSE → patched
`virtiofsd` → QEMU VirtIOFS → Windows VirtioFsSvc/WinFsp conformance run passes.
