// @dive-file: Builds bootstrap ISO images that seed guest init scripts and bundled portproxy binaries.
// @dive-rel: Consumed by VM launch paths in vmd state manager to initialize guest runtime services.
// @dive-rel: Depends on assets::portproxy binary selection for architecture-specific guest payloads.
use std::fs;
use std::io::{Seek, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};

use crate::assets::portproxy;

const INIT_SCRIPT_NAME: &str = "init.sh";
const PORTPROXY_NAME: &str = "portproxy";
const SHARED_MOUNTS_NAME: &str = "shared-mounts.tsv";
const VOLUME_ID: &str = "brkboot";
const SECTOR_SIZE: u32 = 2048;

#[derive(Clone, Debug)]
pub struct Config {
    pub instance_id: String,
    pub hostname: String,
    pub arch: String,
    pub shared_mounts: Vec<SharedMount>,
    pub network: Option<NetworkConfig>,
    pub http_proxy_url: Option<String>,
    pub portproxy_auth_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NetworkConfig {
    pub mac_address: String,
    pub address_cidr: String,
    pub gateway: String,
    pub dns: String,
}

#[derive(Clone, Debug)]
pub struct SharedMount {
    pub guest_path: String,
    pub mount_tag: String,
    pub read_only: bool,
}

pub fn create_iso<P: AsRef<Path>>(path: P, cfg: Config) -> Result<()> {
    if cfg.instance_id.trim().is_empty() {
        bail!("bootstrap iso: instance ID required");
    }
    let hostname = if cfg.hostname.trim().is_empty() {
        cfg.instance_id.clone()
    } else {
        cfg.hostname.clone()
    };

    let bin = portproxy::binary(cfg.arch.as_str())
        .with_context(|| "bootstrap iso: locate portproxy binary")?;
    let init = build_init_script(
        &hostname,
        cfg.network.as_ref(),
        cfg.http_proxy_url.as_deref(),
        cfg.portproxy_auth_token.as_deref(),
    );
    let shared_mounts = build_shared_mounts_file(&cfg.shared_mounts);
    let entries = vec![
        IsoEntry::new(INIT_SCRIPT_NAME, init.into_bytes()),
        IsoEntry::new(PORTPROXY_NAME, bin),
        IsoEntry::new(SHARED_MOUNTS_NAME, shared_mounts.into_bytes()),
    ];

    if let Some(parent) = path.as_ref().parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("bootstrap iso: ensure directory {}", parent.display()))?;
        }
    }

    write_iso(path.as_ref(), VOLUME_ID, &entries)
}

fn build_init_script(
    hostname: &str,
    network: Option<&NetworkConfig>,
    http_proxy_url: Option<&str>,
    portproxy_auth_token: Option<&str>,
) -> String {
    let host_value = shell_escape(hostname);
    let network_setup = build_network_setup_script(network);
    let proxy_setup = build_proxy_setup_script(http_proxy_url);
    let portproxy_auth_setup = build_portproxy_auth_setup_script(portproxy_auth_token);
    format!(
        r#"#!/bin/bash
set -euxo pipefail

HOSTNAME={host_value}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TRACE_LOG=/var/log/bootstrap-init.trace.log

mkdir -p /var/log
touch "$TRACE_LOG"
chmod 0644 "$TRACE_LOG"
SERIAL_DEV=""
for dev in /dev/ttyAMA0 /dev/ttyS0 /dev/console; do
  if [ -c "$dev" ]; then
    SERIAL_DEV="$dev"
    break
  fi
done
if [ -n "$SERIAL_DEV" ]; then
  exec > >(tee -a "$TRACE_LOG" "$SERIAL_DEV") 2>&1
else
  exec >>"$TRACE_LOG" 2>&1
fi

log() {{
  printf 'bootstrap-init: %s %s\n' "$(date -Iseconds)" "$*"
}}

log "begin hostname=${{HOSTNAME}} script_dir=${{SCRIPT_DIR}}"

if [ -n "$HOSTNAME" ]; then
  if command -v hostnamectl >/dev/null 2>&1; then
    hostnamectl set-hostname --static "$HOSTNAME" || true
    hostnamectl set-hostname --pretty "$HOSTNAME" || true
  else
    echo "$HOSTNAME" >/etc/hostname
  fi
  if [ -w /etc/hosts ]; then
    if grep -q "127\.0\.1\.1" /etc/hosts; then
      sed -i "s/^127\.0\.1\.1.*/127.0.1.1 ${{HOSTNAME}}/g" /etc/hosts || true
    else
      printf '127.0.1.1 %s\n' "$HOSTNAME" >> /etc/hosts
    fi
  fi
fi

{network_setup}

log "searching bootstrap payload files"
SRC=""
for candidate in "$SCRIPT_DIR/portproxy" "$SCRIPT_DIR/PORTPROXY"; do
  if [ -f "$candidate" ]; then
    SRC="$candidate"
    break
  fi
done
DEST="/usr/sbin/portproxy"
if [ -z "$SRC" ]; then
  echo "bootstrap-init: portproxy binary missing on bootstrap volume (expected $SCRIPT_DIR/portproxy or $SCRIPT_DIR/PORTPROXY)" >&2
  ls -la "$SCRIPT_DIR" >&2 || true
  exit 52
fi
log "installing portproxy src=$SRC dest=$DEST"
install -m 0755 "$SRC" "$DEST"
ls -la "$DEST" || true

mkdir -p /etc/chevalier
if [ -f "$SCRIPT_DIR/shared-mounts.tsv" ]; then
  # @dive: Shared mount intent is persisted outside the ISO so a guest reboot can remount the same tags through systemd without reissuing the create request.
  install -m 0644 "$SCRIPT_DIR/shared-mounts.tsv" /etc/chevalier/shared-mounts.tsv
fi

{proxy_setup}

{portproxy_auth_setup}

cat <<'EOF' >/etc/systemd/system/portproxy.service
[Unit]
Description=Bracket PortProxy
After=network.target

[Service]
Type=simple
Environment=RUST_LOG=trace
EnvironmentFile=-/etc/chevalier/portproxy.env
ExecStartPre=/usr/local/sbin/chevalier-apply-tap-network.sh
ExecStartPre=/bin/sh -c 'echo "portproxy.service preflight: $(date -Iseconds) starting /usr/sbin/portproxy --server"'
ExecStart=/usr/sbin/portproxy --server
Restart=on-failure
RestartSec=2
StandardOutput=journal+console
StandardError=journal+console

[Install]
WantedBy=multi-user.target
EOF

cat <<'EOF' >/usr/local/sbin/portproxy-diagnostics.sh
#!/bin/bash
set -euo pipefail
echo "portproxy-diag: $(date -Iseconds) begin"
echo "portproxy-diag: uname=$(uname -a)"
echo "portproxy-diag: portproxy ls => $(ls -l /usr/sbin/portproxy 2>&1 || true)"
if command -v file >/dev/null 2>&1; then
  file /usr/sbin/portproxy || true
fi
systemctl is-enabled portproxy.service || true
systemctl is-active portproxy.service || true
systemctl --no-pager -l status portproxy.service || true
journalctl --no-pager -u portproxy.service -n 200 || true
ss -ltnp || true
echo "portproxy-diag: $(date -Iseconds) end"
EOF
chmod 0755 /usr/local/sbin/portproxy-diagnostics.sh

cat <<'EOF' >/usr/local/sbin/chevalier-mount-shares.sh
#!/bin/bash
set -euo pipefail

CONFIG=/etc/chevalier/shared-mounts.tsv
if [ ! -s "$CONFIG" ]; then
  exit 0
fi

log() {{
  printf 'chevalier-shared-mounts: %s %s\n' "$(date -Iseconds)" "$*" | systemd-cat -t chevalier-shared-mounts || true
}}

# @dive: virtio-fs is the preferred host↔guest shared FS transport because virtio-9p
# blocks qemu migration (see vmd plan notes). The init script tries virtio-fs first
# and falls back to virtio-9p for dev hosts (e.g., macOS) where the vmd daemon can't
# spawn virtiofsd and has to emit -virtfs devices instead.
if command -v modprobe >/dev/null 2>&1; then
  modprobe virtiofs || true
  modprobe 9pnet_virtio || true
  modprobe 9p || true
fi

while IFS=$'\t' read -r TAG GUEST MODE; do
  if [ -z "${{TAG:-}}" ] || [ -z "${{GUEST:-}}" ]; then
    continue
  fi

  # @dive: Precreate every mountpoint before mounting anything so nested writable mounts remain
  # mountable even when their parent path will become a read-only shared root.
  if [ -e "$GUEST" ]; then
    continue
  fi
  if ! mkdir -p "$GUEST"; then
    log "failed to create mountpoint tag=$TAG guest=$GUEST"
    exit 1
  fi
done < "$CONFIG"

while IFS=$'\t' read -r TAG GUEST MODE; do
  if [ -z "${{TAG:-}}" ] || [ -z "${{GUEST:-}}" ]; then
    continue
  fi

  if mountpoint -q "$GUEST"; then
    log "mount exists tag=$TAG guest=$GUEST"
    continue
  fi

  # virtiofs rejects the FUSE-only default_permissions/allow_other options on
  # current guest kernels. Preserve mount policy with `ro` only when requested;
  # otherwise omit -o entirely and let virtiofs mount read-write by default.
  VFS_OPTS=""
  NINEP_OPTS="trans=virtio,version=9p2000.L,msize=104857600"
  if [ "${{MODE:-rw}}" = "ro" ]; then
    VFS_OPTS="ro"
    NINEP_OPTS="$NINEP_OPTS,ro"
  fi

  MOUNTED=""
  for ATTEMPT in 1 2 3 4 5; do
    if {{
      if [ -n "$VFS_OPTS" ]; then
        mount -t virtiofs -o "$VFS_OPTS" "$TAG" "$GUEST" 2>/dev/null
      else
        mount -t virtiofs "$TAG" "$GUEST" 2>/dev/null
      fi
    }}; then
      log "mounted tag=$TAG guest=$GUEST mode=${{MODE:-rw}} fs=virtiofs attempt=$ATTEMPT"
      MOUNTED=1
      break
    elif mount -t 9p -o "$NINEP_OPTS" "$TAG" "$GUEST"; then
      log "mounted tag=$TAG guest=$GUEST mode=${{MODE:-rw}} fs=9p attempt=$ATTEMPT"
      MOUNTED=1
      break
    fi
    log "mount attempt failed tag=$TAG guest=$GUEST mode=${{MODE:-rw}} attempt=$ATTEMPT"
    sleep 1
  done

  if [ -z "$MOUNTED" ]; then
    log "failed tag=$TAG guest=$GUEST mode=${{MODE:-rw}}"
    exit 1
  fi
done < "$CONFIG"
EOF
chmod 0755 /usr/local/sbin/chevalier-mount-shares.sh

cat <<'EOF' >/etc/systemd/system/chevalier-shared-mounts.service
[Unit]
Description=Mount Chevalier shared directories
After=local-fs.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/chevalier-mount-shares.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

cat <<'EOF' >/etc/systemd/system/portproxy-diagnostics.service
[Unit]
Description=Dump portproxy diagnostics
After=portproxy.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/portproxy-diagnostics.sh
StandardOutput=journal+console
StandardError=journal+console
EOF

cat <<'EOF' >/etc/systemd/system/portproxy-diagnostics.timer
[Unit]
Description=Run portproxy diagnostics after boot

[Timer]
OnBootSec=12s
AccuracySec=1s
Unit=portproxy-diagnostics.service
Persistent=true

[Install]
WantedBy=timers.target
EOF

cat <<'EOF' >/usr/local/sbin/firstboot-resize-rootfs.sh
#!/bin/bash
set -eux

DONE_MARKER=/var/lib/bracket/firstboot-resize-rootfs.done
mkdir -p "$(dirname "$DONE_MARKER")"
if [ -f "$DONE_MARKER" ]; then
  exit 0
fi

ROOT_DEV=$(findmnt -n -o SOURCE / || true)
if [ -z "$ROOT_DEV" ]; then
  exit 0
fi

DISK=""
PART=""
PARENT=$(lsblk -no PKNAME "$ROOT_DEV" 2>/dev/null || true)
PARTNUM=$(lsblk -no PARTNUM "$ROOT_DEV" 2>/dev/null || true)
if [ -n "$PARENT" ] && [ -n "$PARTNUM" ]; then
  DISK="/dev/$PARENT"
  PART="$PARTNUM"
else
  case "$ROOT_DEV" in
    /dev/nvme*|/dev/mmcblk*)
      DISK="${{ROOT_DEV%p*}}"
      PART="${{ROOT_DEV##*p}}"
      ;;
    *)
      DISK="${{ROOT_DEV%[0-9]*}}"
      PART="${{ROOT_DEV#$DISK}}"
      ;;
  esac
fi

if [ -n "$DISK" ] && [ -n "$PART" ]; then
  if ! command -v growpart >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
      export DEBIAN_FRONTEND=noninteractive
      apt-get update || true
      apt-get install -y cloud-guest-utils || true
    fi
  fi
  if command -v growpart >/dev/null 2>&1; then
    growpart "$DISK" "$PART" || true
  fi
  if command -v resize2fs >/dev/null 2>&1; then
    resize2fs "$ROOT_DEV" || true
  fi
fi

touch "$DONE_MARKER"
EOF
chmod 0755 /usr/local/sbin/firstboot-resize-rootfs.sh

cat <<'EOF' >/etc/systemd/system/firstboot-resize-rootfs.service
[Unit]
Description=Resize root filesystem on first boot
ConditionPathExists=!/var/lib/bracket/firstboot-resize-rootfs.done

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/firstboot-resize-rootfs.sh

[Install]
WantedBy=multi-user.target
EOF

cat <<'EOF' >/etc/systemd/system/firstboot-resize-rootfs.timer
[Unit]
Description=Run first boot root filesystem resize

[Timer]
OnBootSec=30s
Unit=firstboot-resize-rootfs.service
Persistent=true

[Install]
WantedBy=multi-user.target
EOF

log "reloading systemd units"
systemctl daemon-reload
log "enabling and starting portproxy.service"
if ! systemctl enable portproxy.service; then
  log "failed enabling portproxy.service"
  exit 53
fi
if ! systemctl start portproxy.service; then
  log "failed starting portproxy.service"
  systemctl --no-pager -l status portproxy.service || true
  journalctl --no-pager -u portproxy.service -n 200 || true
  exit 54
fi

log "enabling resize timer"
systemctl enable firstboot-resize-rootfs.timer
systemctl start firstboot-resize-rootfs.timer

log "enabling shared mounts service"
systemctl enable chevalier-shared-mounts.service
systemctl start chevalier-shared-mounts.service || true

log "enabling diagnostics timer"
systemctl enable portproxy-diagnostics.timer
systemctl start portproxy-diagnostics.timer

log "portproxy startup status snapshot"
systemctl --no-pager -l status portproxy.service || true
journalctl --no-pager -u portproxy.service -n 100 || true
log "bootstrap init complete"
"#,
        host_value = host_value,
        network_setup = network_setup,
        proxy_setup = proxy_setup,
    )
}

fn build_network_setup_script(network: Option<&NetworkConfig>) -> String {
    let Some(network) = network else {
        return r#"log "managed tap network disabled"
mkdir -p /usr/local/sbin
cat <<'EOF' >/usr/local/sbin/chevalier-apply-tap-network.sh
#!/bin/bash
set -euo pipefail
exit 0
EOF
chmod 0755 /usr/local/sbin/chevalier-apply-tap-network.sh"#
            .to_string();
    };
    let mac = shell_escape(&network.mac_address.to_ascii_lowercase());
    let address_cidr = shell_escape(&network.address_cidr);
    let gateway = shell_escape(&network.gateway);
    let dns = shell_escape(&network.dns);
    format!(
        r#"log "configuring managed tap network"
TAP_MAC={mac}
TAP_ADDRESS_CIDR={address_cidr}
TAP_GATEWAY={gateway}
TAP_DNS={dns}

if command -v sysctl >/dev/null 2>&1; then
  sysctl -w net.ipv6.conf.all.disable_ipv6=1 || true
  sysctl -w net.ipv6.conf.default.disable_ipv6=1 || true
fi

TAP_IFACE=""
for path in /sys/class/net/*; do
  [ -e "$path/address" ] || continue
  if [ "$(tr '[:upper:]' '[:lower:]' < "$path/address")" = "$TAP_MAC" ]; then
    TAP_IFACE="$(basename "$path")"
    break
  fi
done
if [ -z "$TAP_IFACE" ]; then
  log "managed tap interface not found for mac=$TAP_MAC"
  ip link || true
  exit 55
fi

mkdir -p /etc/chevalier
cat <<EOF >/etc/chevalier/tap-network.env
TAP_IFACE=$TAP_IFACE
TAP_MAC=$TAP_MAC
TAP_ADDRESS_CIDR=$TAP_ADDRESS_CIDR
TAP_GATEWAY=$TAP_GATEWAY
TAP_DNS=$TAP_DNS
EOF
chmod 0644 /etc/chevalier/tap-network.env

cat <<'EOF' >/usr/local/sbin/chevalier-apply-tap-network.sh
#!/bin/bash
set -euo pipefail

CONFIG=/etc/chevalier/tap-network.env
if [ ! -s "$CONFIG" ]; then
  exit 0
fi

set -a
. "$CONFIG"
set +a

log() {{
  if command -v systemd-cat >/dev/null 2>&1; then
    printf 'chevalier-tap-network: %s %s\n' "$(date -Iseconds)" "$*" | systemd-cat -t chevalier-tap-network || true
  else
    printf 'chevalier-tap-network: %s %s\n' "$(date -Iseconds)" "$*" || true
  fi
}}

if command -v sysctl >/dev/null 2>&1; then
  sysctl -w net.ipv6.conf.all.disable_ipv6=1 || true
  sysctl -w net.ipv6.conf.default.disable_ipv6=1 || true
fi

TAP_IFACE_VALUE="${{TAP_IFACE:-}}"
if ip link show chevalier0 >/dev/null 2>&1; then
  TAP_IFACE_VALUE=chevalier0
fi
if [ -z "$TAP_IFACE_VALUE" ] || ! ip link show "$TAP_IFACE_VALUE" >/dev/null 2>&1; then
  TAP_IFACE_VALUE=""
  for path in /sys/class/net/*; do
    [ -e "$path/address" ] || continue
    if [ "$(tr '[:upper:]' '[:lower:]' < "$path/address")" = "$TAP_MAC" ]; then
      TAP_IFACE_VALUE="$(basename "$path")"
      break
    fi
  done
fi
if [ -z "$TAP_IFACE_VALUE" ]; then
  log "managed tap interface not found for mac=$TAP_MAC"
  ip link || true
  exit 55
fi

ip link set "$TAP_IFACE_VALUE" up || true
ip -4 addr flush dev "$TAP_IFACE_VALUE" || true
ip addr add "$TAP_ADDRESS_CIDR" dev "$TAP_IFACE_VALUE"
ip route replace default via "$TAP_GATEWAY" dev "$TAP_IFACE_VALUE"
printf 'nameserver %s\noptions edns0 trust-ad\n' "$TAP_DNS" >/etc/resolv.conf || true
cat <<ENV >/etc/chevalier/tap-network.env
TAP_IFACE=$TAP_IFACE_VALUE
TAP_MAC=$TAP_MAC
TAP_ADDRESS_CIDR=$TAP_ADDRESS_CIDR
TAP_GATEWAY=$TAP_GATEWAY
TAP_DNS=$TAP_DNS
ENV
chmod 0644 /etc/chevalier/tap-network.env
log "configured iface=$TAP_IFACE_VALUE address=$TAP_ADDRESS_CIDR gateway=$TAP_GATEWAY"
EOF
chmod 0755 /usr/local/sbin/chevalier-apply-tap-network.sh

if command -v netplan >/dev/null 2>&1; then
  mkdir -p /etc/netplan
  mkdir -p /etc/chevalier/disabled-netplan
  for existing in /etc/netplan/*.yaml /etc/netplan/*.yml; do
    [ -e "$existing" ] || continue
    case "$(basename "$existing")" in
      90-chevalier-tap.yaml)
        continue
        ;;
    esac
    disabled="/etc/chevalier/disabled-netplan/$(basename "$existing").disabled"
    log "disabling inherited netplan config src=$existing dest=$disabled"
    mv "$existing" "$disabled" || true
  done
  cat <<EOF >/etc/netplan/90-chevalier-tap.yaml
network:
  version: 2
  ethernets:
    chevalier0:
      match:
        macaddress: "$TAP_MAC"
      set-name: chevalier0
      dhcp4: false
      dhcp6: false
      accept-ra: false
      link-local: []
      addresses:
        - "$TAP_ADDRESS_CIDR"
      routes:
        - to: default
          via: "$TAP_GATEWAY"
      nameservers:
        addresses:
          - "$TAP_DNS"
EOF
  chmod 0600 /etc/netplan/90-chevalier-tap.yaml
  netplan apply || true
fi

/usr/local/sbin/chevalier-apply-tap-network.sh"#
    )
}

fn build_proxy_setup_script(http_proxy_url: Option<&str>) -> String {
    let managed_files = [
        "/etc/chevalier/proxy.env",
        "/etc/profile.d/chevalier-proxy.sh",
        "/etc/apt/apt.conf.d/90chevalier-proxy",
        "/etc/npmrc",
        "/root/.gitconfig",
        "/root/.config/pip/pip.conf",
    ];

    match http_proxy_url {
        Some(url) => {
            let proxy_url = shell_escape(url);
            let no_proxy = shell_escape("localhost,127.0.0.1");
            format!(
                r#"log "configuring managed guest proxy environment"
HTTP_PROXY_URL={proxy_url}
NO_PROXY_VALUE={no_proxy}

mkdir -p /etc/chevalier /etc/profile.d /etc/apt/apt.conf.d /root/.config/pip

cat <<EOF >/etc/chevalier/proxy.env
http_proxy=$HTTP_PROXY_URL
https_proxy=$HTTP_PROXY_URL
HTTP_PROXY=$HTTP_PROXY_URL
HTTPS_PROXY=$HTTP_PROXY_URL
all_proxy=$HTTP_PROXY_URL
ALL_PROXY=$HTTP_PROXY_URL
no_proxy=$NO_PROXY_VALUE
NO_PROXY=$NO_PROXY_VALUE
EOF
chmod 0644 /etc/chevalier/proxy.env

cat <<'EOF' >/etc/profile.d/chevalier-proxy.sh
if [ -f /etc/chevalier/proxy.env ]; then
  set -a
  . /etc/chevalier/proxy.env
  set +a
fi
EOF
chmod 0644 /etc/profile.d/chevalier-proxy.sh

cat <<EOF >/etc/apt/apt.conf.d/90chevalier-proxy
Acquire::http::Proxy "$HTTP_PROXY_URL";
Acquire::https::Proxy "$HTTP_PROXY_URL";
EOF
chmod 0644 /etc/apt/apt.conf.d/90chevalier-proxy

cat <<EOF >/etc/npmrc
proxy=$HTTP_PROXY_URL
https-proxy=$HTTP_PROXY_URL
EOF
chmod 0644 /etc/npmrc

cat <<EOF >/root/.gitconfig
[http]
	proxy = $HTTP_PROXY_URL
[https]
	proxy = $HTTP_PROXY_URL
EOF
chmod 0644 /root/.gitconfig

cat <<EOF >/root/.config/pip/pip.conf
[global]
proxy = $HTTP_PROXY_URL
EOF
chmod 0644 /root/.config/pip/pip.conf"#
            )
        }
        None => {
            let remove_lines = managed_files
                .iter()
                .map(|path| format!("rm -f {path}"))
                .collect::<Vec<_>>()
                .join("\n");
            format!(
                r#"log "managed guest proxy disabled; removing managed proxy files"
{remove_lines}"#
            )
        }
    }
}

fn build_portproxy_auth_setup_script(portproxy_auth_token: Option<&str>) -> String {
    match portproxy_auth_token
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        Some(token) => {
            let token = shell_escape(token);
            format!(
                r#"log "configuring managed portproxy auth"
mkdir -p /etc/chevalier
umask 077
set +x
printf 'CHEVALIER_PORTPROXY_AUTH_TOKEN=%s\n' {token} >/etc/chevalier/portproxy.env
set -x
chmod 0600 /etc/chevalier/portproxy.env"#
            )
        }
        None => r#"log "managed portproxy auth disabled; removing managed auth file"
rm -f /etc/chevalier/portproxy.env"#
            .to_string(),
    }
}

fn shell_escape(value: &str) -> String {
    if value.is_empty() {
        "''".to_string()
    } else if !value.contains('\'') {
        format!("'{value}'")
    } else {
        let mut out = String::with_capacity(value.len() + 2);
        out.push('\'');
        for ch in value.chars() {
            if ch == '\'' {
                out.push_str("'\\''");
            } else {
                out.push(ch);
            }
        }
        out.push('\'');
        out
    }
}

fn build_shared_mounts_file(shared_mounts: &[SharedMount]) -> String {
    // @dive: Nested guest mountpoints must appear after their parents so the guest mounts `/workspace`
    // before `/workspace/task-overlays`, while the bootstrap script precreates both paths ahead of time.
    let mut ordered_mounts = shared_mounts.to_vec();
    ordered_mounts.sort_by(|left, right| {
        let left_depth = Path::new(&left.guest_path).components().count();
        let right_depth = Path::new(&right.guest_path).components().count();
        left_depth
            .cmp(&right_depth)
            .then_with(|| left.guest_path.cmp(&right.guest_path))
            .then_with(|| left.mount_tag.cmp(&right.mount_tag))
    });

    let mut out = String::new();
    for mount in &ordered_mounts {
        let mode = if mount.read_only { "ro" } else { "rw" };
        out.push_str(&mount.mount_tag);
        out.push('\t');
        out.push_str(&mount.guest_path);
        out.push('\t');
        out.push_str(mode);
        out.push('\n');
    }
    out
}

#[derive(Clone)]
struct IsoEntry {
    name: String,
    data: Vec<u8>,
}

impl IsoEntry {
    fn new(name: &str, data: Vec<u8>) -> Self {
        Self {
            name: name.to_string(),
            data,
        }
    }
}

struct LayoutEntry {
    _name: String,
    data: Vec<u8>,
    ident: String,
    extent: u32,
}

struct IsoLayout {
    entries: Vec<LayoutEntry>,
    root_dir_sector: u32,
    root_dir_data: Vec<u8>,
    root_dir_size: usize,
    l_path_table: Vec<u8>,
    m_path_table: Vec<u8>,
    l_path_table_sector: u32,
    m_path_table_sector: u32,
    total_sectors: u32,
}

fn write_iso(path: &Path, volume_id: &str, files: &[IsoEntry]) -> Result<()> {
    if files.is_empty() {
        bail!("iso: at least one file required");
    }

    let now = current_time();
    let layout = prepare_layout(files, now)?;

    let mut file = fs::File::create(path)?;
    file.set_len((layout.total_sectors as u64) * (SECTOR_SIZE as u64))?;

    write_sector(
        &mut file,
        16,
        &build_primary_volume_descriptor(volume_id, &layout, now),
    )?;
    write_sector(&mut file, 17, &build_terminator_descriptor())?;
    write_data(&mut file, layout.l_path_table_sector, &layout.l_path_table)?;
    write_data(&mut file, layout.m_path_table_sector, &layout.m_path_table)?;
    write_data(&mut file, layout.root_dir_sector, &layout.root_dir_data)?;
    for entry in &layout.entries {
        write_data(&mut file, entry.extent, &entry.data)?;
    }

    Ok(())
}

fn prepare_layout(files: &[IsoEntry], ts: DateTime<Utc>) -> Result<IsoLayout> {
    let mut entries = Vec::with_capacity(files.len());
    for file in files {
        entries.push(LayoutEntry {
            _name: file.name.clone(),
            data: file.data.clone(),
            ident: make_identifier(&file.name),
            extent: 0,
        });
    }

    let dir_size = directory_data_length(&entries);
    let root_dir_sectors = sectors_for(dir_size);
    let l_path_sector = 18;
    let m_path_sector = 19;
    let mut current = 20 + root_dir_sectors as u32;
    for entry in &mut entries {
        entry.extent = current;
        current += sectors_for(entry.data.len()) as u32;
    }

    let total_sectors = current;
    let l_path = build_path_table(true, 20);
    let m_path = build_path_table(false, 20);
    let root_dir = build_root_directory(&entries, 20, dir_size, ts)?;

    Ok(IsoLayout {
        entries,
        root_dir_sector: 20,
        root_dir_data: root_dir.clone(),
        root_dir_size: root_dir.len(),
        l_path_table: l_path,
        m_path_table: m_path,
        l_path_table_sector: l_path_sector,
        m_path_table_sector: m_path_sector,
        total_sectors,
    })
}

fn write_sector(file: &mut fs::File, sector: u32, data: &[u8]) -> Result<()> {
    if data.len() != SECTOR_SIZE as usize {
        bail!("iso: sector {sector} invalid size {}", data.len());
    }
    file.seek(std::io::SeekFrom::Start(sector as u64 * SECTOR_SIZE as u64))?;
    file.write_all(data)?;
    Ok(())
}

fn write_data(file: &mut fs::File, sector: u32, data: &[u8]) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    file.seek(std::io::SeekFrom::Start(sector as u64 * SECTOR_SIZE as u64))?;
    file.write_all(&pad(data))?;
    Ok(())
}

fn build_primary_volume_descriptor(
    volume_id: &str,
    layout: &IsoLayout,
    ts: DateTime<Utc>,
) -> Vec<u8> {
    let mut buf = vec![0u8; SECTOR_SIZE as usize];
    buf[0] = 1;
    buf[1..6].copy_from_slice(b"CD001");
    buf[6] = 1;
    write_string(&mut buf[8..40], "GO-ISO");
    write_string(&mut buf[40..72], &volume_id.to_uppercase());
    put_both32(&mut buf[80..88], layout.total_sectors);
    put_both16(&mut buf[120..124], 1);
    put_both16(&mut buf[124..128], 1);
    put_both16(&mut buf[128..132], SECTOR_SIZE as u16);
    put_both32(&mut buf[132..140], layout.l_path_table.len() as u32);
    buf[140..144].copy_from_slice(&(layout.l_path_table_sector as u32).to_le_bytes());
    buf[144..148].copy_from_slice(&0u32.to_le_bytes());
    buf[148..152].copy_from_slice(&(layout.m_path_table_sector as u32).to_be_bytes());
    buf[152..156].copy_from_slice(&0u32.to_be_bytes());

    let root_record = build_directory_record(
        layout.root_dir_sector,
        layout.root_dir_size as u32,
        0x02,
        &[0],
        ts,
    );
    buf[156..156 + root_record.len()].copy_from_slice(&root_record);

    write_volume_timestamp(&mut buf[813..830], ts);
    write_volume_timestamp(&mut buf[830..847], ts);
    write_volume_timestamp(&mut buf[847..864], ts);
    write_volume_timestamp(&mut buf[864..881], ts);
    buf[881] = 0x01;
    buf
}

fn build_terminator_descriptor() -> Vec<u8> {
    let mut buf = vec![0u8; SECTOR_SIZE as usize];
    buf[0] = 255;
    buf[1..6].copy_from_slice(b"CD001");
    buf[6] = 1;
    buf
}

fn build_path_table(little_endian: bool, root_sector: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.push(1);
    buf.push(0);
    if little_endian {
        buf.extend_from_slice(&root_sector.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
    } else {
        buf.extend_from_slice(&root_sector.to_be_bytes());
        buf.extend_from_slice(&1u16.to_be_bytes());
    }
    buf.push(0);
    buf.push(0);
    pad(&buf)
}

fn build_root_directory(
    entries: &[LayoutEntry],
    sector: u32,
    dir_size: usize,
    ts: DateTime<Utc>,
) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(dir_size);
    buf.extend_from_slice(&build_directory_record(
        sector,
        dir_size as u32,
        0x02,
        &[0],
        ts,
    ));
    buf.extend_from_slice(&build_directory_record(
        sector,
        dir_size as u32,
        0x02,
        &[1],
        ts,
    ));
    for entry in entries {
        buf.extend_from_slice(&build_directory_record(
            entry.extent,
            entry.data.len() as u32,
            0x00,
            entry.ident.as_bytes(),
            ts,
        ));
    }
    Ok(pad(&buf))
}

fn build_directory_record(
    extent: u32,
    size: u32,
    flags: u8,
    ident: &[u8],
    ts: DateTime<Utc>,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(33 + ident.len());
    let ident_is_even = ident.len().is_multiple_of(2);
    buf.push(33 + ident.len() as u8 + ident_is_even as u8);
    buf.push(0);
    buf.extend_from_slice(&extent.to_le_bytes());
    buf.extend_from_slice(&extent.to_be_bytes());
    buf.extend_from_slice(&size.to_le_bytes());
    buf.extend_from_slice(&size.to_be_bytes());
    buf.extend_from_slice(&datetime_to_record(ts));
    buf.push(flags);
    buf.push(0); // file unit size
    buf.push(0); // interleave gap size
    buf.extend_from_slice(&1u16.to_le_bytes());
    buf.extend_from_slice(&1u16.to_be_bytes());
    buf.push(ident.len() as u8);
    buf.extend_from_slice(ident);
    if ident_is_even {
        buf.push(0);
    }
    buf
}

fn datetime_to_record(ts: DateTime<Utc>) -> [u8; 7] {
    [
        (ts.year() - 1900) as u8,
        ts.month() as u8,
        ts.day() as u8,
        ts.hour() as u8,
        ts.minute() as u8,
        ts.second() as u8,
        0,
    ]
}

fn put_both16(buf: &mut [u8], value: u16) {
    buf[..2].copy_from_slice(&value.to_le_bytes());
    buf[2..4].copy_from_slice(&value.to_be_bytes());
}

fn put_both32(buf: &mut [u8], value: u32) {
    buf[..4].copy_from_slice(&value.to_le_bytes());
    buf[4..8].copy_from_slice(&value.to_be_bytes());
}

fn write_string(target: &mut [u8], value: &str) {
    for (dst, src) in target.iter_mut().zip(value.bytes()) {
        *dst = src;
    }
}

fn write_volume_timestamp(buf: &mut [u8], ts: DateTime<Utc>) {
    let formatted = ts.format("%Y%m%d%H%M%S00").to_string();
    write_string(buf, &formatted);
}

fn directory_data_length(entries: &[LayoutEntry]) -> usize {
    let mut size = 0;
    size += build_directory_record(0, 0, 0x02, &[0], Utc::now()).len();
    size += build_directory_record(0, 0, 0x02, &[1], Utc::now()).len();
    for entry in entries {
        size += build_directory_record(
            entry.extent,
            entry.data.len() as u32,
            0,
            entry.ident.as_bytes(),
            Utc::now(),
        )
        .len();
    }
    size
}

fn sectors_for(size: usize) -> usize {
    (size + SECTOR_SIZE as usize - 1) / SECTOR_SIZE as usize
}

fn pad(data: &[u8]) -> Vec<u8> {
    let sectors = sectors_for(data.len());
    let mut out = vec![0u8; sectors * SECTOR_SIZE as usize];
    out[..data.len()].copy_from_slice(data);
    out
}

fn make_identifier(name: &str) -> String {
    format!("{};1", name.to_uppercase())
}

fn current_time() -> DateTime<Utc> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards");
    Utc.timestamp_opt(now.as_secs() as i64, now.subsec_nanos())
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::ensure;
    use std::fs;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn seed_iso_contains_required_files() -> Result<()> {
        let proxy_dir = tempdir()?;
        let binary_path = proxy_dir.path().join("portproxy-linux-amd64");
        fs::write(&binary_path, b"test-portproxy")?;
        unsafe {
            std::env::set_var("PROXY_BIN", proxy_dir.path());
        }

        let tmp_dir = tempdir()?;
        let iso_path = tmp_dir.path().join("bootstrap.iso");
        let cfg = Config {
            instance_id: "vm-test".to_string(),
            hostname: "vm-test".to_string(),
            arch: "amd64".to_string(),
            shared_mounts: vec![SharedMount {
                guest_path: "/workspace".to_string(),
                mount_tag: "runtimefs".to_string(),
                read_only: false,
            }],
            network: None,
            http_proxy_url: None,
            portproxy_auth_token: Some("test-token".to_string()),
        };
        create_iso(&iso_path, cfg)?;

        let entries = read_root_entries(&iso_path)?;
        assert!(
            entries.contains(&"INIT.SH;1".to_string()),
            "init script missing"
        );
        assert!(
            entries.contains(&"PORTPROXY;1".to_string()),
            "portproxy missing"
        );
        assert!(
            entries.contains(&"SHARED-MOUNTS.TSV;1".to_string()),
            "shared mounts manifest missing"
        );
        Ok(())
    }

    #[test]
    fn build_shared_mounts_file_orders_parent_before_nested_writable_children() {
        let mounts = vec![
            SharedMount {
                guest_path: "/workspace/conversations/conv-a/0007_assistant/mount".to_string(),
                mount_tag: "runtimefs-task-conv-a-0007".to_string(),
                read_only: false,
            },
            SharedMount {
                guest_path: "/workspace/conversations/conv-a/shared".to_string(),
                mount_tag: "runtimefs-shared-conv-a".to_string(),
                read_only: false,
            },
            SharedMount {
                guest_path: "/workspace".to_string(),
                mount_tag: "runtimefs".to_string(),
                read_only: true,
            },
        ];

        let file = build_shared_mounts_file(&mounts);
        let lines: Vec<&str> = file.lines().collect();
        assert_eq!(lines[0], "runtimefs\t/workspace\tro");
        assert_eq!(
            lines[1],
            "runtimefs-shared-conv-a\t/workspace/conversations/conv-a/shared\trw"
        );
        assert_eq!(
            lines[2],
            "runtimefs-task-conv-a-0007\t/workspace/conversations/conv-a/0007_assistant/mount\trw"
        );
    }

    #[test]
    fn init_script_precreates_guest_paths_before_mount_phase() {
        let script = build_init_script("vm-test", None, None, None);
        let precreate = script
            .find("mkdir -p \"$GUEST\"")
            .expect("precreate loop present");
        let mount_phase = script
            .rfind("if mountpoint -q \"$GUEST\"; then")
            .expect("mount loop present");

        assert!(
            precreate < mount_phase,
            "expected guest-path precreate pass before mount phase"
        );
    }

    #[test]
    fn init_script_uses_guest_safe_virtiofs_options() {
        let script = build_init_script("vm-test", None, None, None);
        assert!(script.contains("VFS_OPTS=\"\""));
        assert!(script.contains("VFS_OPTS=\"ro\""));
        assert!(script.contains("if [ -n \"$VFS_OPTS\" ]; then"));
        assert!(script.contains("mount -t virtiofs -o \"$VFS_OPTS\""));
        assert!(script.contains("mount -t virtiofs \"$TAG\" \"$GUEST\""));
        assert!(
            !script.contains("VFS_RC=$?"),
            "mount failures must stay inside an if condition so set -e does not skip 9p fallback"
        );
        assert!(
            !script.contains("VFS_OPTS=\"default_permissions"),
            "default_permissions is valid on the host FUSE mount but rejected by guest virtiofs"
        );
        assert!(
            !script.contains("VFS_OPTS=\"allow_other"),
            "allow_other is also rejected by the guest virtiofs mount"
        );
    }

    #[test]
    fn init_script_configures_managed_proxy_files_when_proxy_url_present() {
        let script = build_init_script("vm-test", None, Some("http://10.0.2.100:3128"), None);
        assert!(script.contains("log \"configuring managed guest proxy environment\""));
        assert!(script.contains("HTTP_PROXY_URL='http://10.0.2.100:3128'"));
        assert!(script.contains("cat <<EOF >/etc/chevalier/proxy.env"));
        assert!(script.contains("Acquire::http::Proxy \"$HTTP_PROXY_URL\";"));
        assert!(script.contains("cat <<EOF >/root/.config/pip/pip.conf"));
    }

    #[test]
    fn init_script_removes_managed_proxy_files_when_proxy_url_absent() {
        let script = build_init_script("vm-test", None, None, None);
        assert!(script.contains("managed guest proxy disabled; removing managed proxy files"));
        assert!(script.contains("rm -f /etc/chevalier/proxy.env"));
        assert!(script.contains("rm -f /etc/profile.d/chevalier-proxy.sh"));
        assert!(script.contains("rm -f /etc/apt/apt.conf.d/90chevalier-proxy"));
        assert!(script.contains("rm -f /root/.config/pip/pip.conf"));
    }

    #[test]
    fn init_script_configures_static_tap_network_when_present() {
        let network = NetworkConfig {
            mac_address: "02:00:00:00:00:10".to_string(),
            address_cidr: "198.18.0.2/30".to_string(),
            gateway: "198.18.0.1".to_string(),
            dns: "198.18.0.1".to_string(),
        };
        let script = build_init_script("vm-test", Some(&network), None, None);
        assert!(script.contains("log \"configuring managed tap network\""));
        assert!(script.contains("TAP_ADDRESS_CIDR='198.18.0.2/30'"));
        assert!(script.contains("ip route replace default via \"$TAP_GATEWAY\""));
        assert!(script.contains("net.ipv6.conf.all.disable_ipv6=1"));
        assert!(script.contains("disabling inherited netplan config"));
        assert!(script.contains("chmod 0600 /etc/netplan/90-chevalier-tap.yaml"));
        assert!(script.contains("accept-ra: false"));
        assert!(script.contains("link-local: []"));
        assert!(script.contains("ExecStartPre=/usr/local/sbin/chevalier-apply-tap-network.sh"));
        assert!(script.contains("cat <<'EOF' >/usr/local/sbin/chevalier-apply-tap-network.sh"));
        assert!(script.contains("ip -4 addr flush dev \"$TAP_IFACE_VALUE\""));
        assert!(script.contains("log \"configured iface=$TAP_IFACE_VALUE"));
    }

    #[test]
    fn init_script_installs_noop_tap_repair_when_network_absent() {
        let script = build_init_script("vm-test", None, None, None);
        assert!(script.contains("log \"managed tap network disabled\""));
        assert!(script.contains("ExecStartPre=/usr/local/sbin/chevalier-apply-tap-network.sh"));
        assert!(script.contains("cat <<'EOF' >/usr/local/sbin/chevalier-apply-tap-network.sh"));
        assert!(script.contains("exit 0"));
    }

    #[test]
    fn init_script_does_not_block_portproxy_on_network_online() {
        let script = build_init_script("vm-test", None, None, None);
        assert!(script.contains("After=network.target"));
        assert!(!script.contains("network-online.target"));
    }

    #[test]
    fn init_script_configures_portproxy_auth_env_file_when_token_present() {
        let script = build_init_script("vm-test", None, None, Some("guest-token"));
        assert!(script.contains("log \"configuring managed portproxy auth\""));
        assert!(script.contains("CHEVALIER_PORTPROXY_AUTH_TOKEN=%s"));
        assert!(script.contains("'guest-token' >/etc/chevalier/portproxy.env"));
        assert!(script.contains("chmod 0600 /etc/chevalier/portproxy.env"));
        assert!(script.contains("EnvironmentFile=-/etc/chevalier/portproxy.env"));
    }

    #[test]
    fn init_script_removes_portproxy_auth_env_file_when_token_absent() {
        let script = build_init_script("vm-test", None, None, None);
        assert!(script.contains("managed portproxy auth disabled; removing managed auth file"));
        assert!(script.contains("rm -f /etc/chevalier/portproxy.env"));
        assert!(script.contains("EnvironmentFile=-/etc/chevalier/portproxy.env"));
    }

    fn read_root_entries(path: &Path) -> Result<Vec<String>> {
        let mut file = fs::File::open(path)?;
        let root_record = read_root_directory_record(&mut file)?;
        let mut buf = vec![0u8; root_record.size as usize];
        file.seek(SeekFrom::Start(
            root_record.extent as u64 * SECTOR_SIZE as u64,
        ))?;
        file.read_exact(&mut buf)?;

        let mut entries = Vec::new();
        let mut offset = 0usize;
        while offset < buf.len() {
            if buf[offset] == 0 {
                let next_sector = ((offset / SECTOR_SIZE as usize) + 1) * SECTOR_SIZE as usize;
                if next_sector <= offset {
                    break;
                }
                offset = next_sector;
                continue;
            }

            let length = buf[offset] as usize;
            if offset + length > buf.len() || length < 34 {
                break;
            }
            let ident_len = buf[offset + 32] as usize;
            if 33 + ident_len > length {
                break;
            }
            let ident = &buf[offset + 33..offset + 33 + ident_len];
            if ident != [0] && ident != [1] {
                entries.push(String::from_utf8_lossy(ident).to_string());
            }
            offset += length;
        }
        Ok(entries)
    }

    struct DirectoryRecord {
        extent: u32,
        size: u32,
    }

    fn read_root_directory_record(file: &mut fs::File) -> Result<DirectoryRecord> {
        file.seek(SeekFrom::Start(SECTOR_SIZE as u64 * 16))?;
        let mut sector = vec![0u8; SECTOR_SIZE as usize];
        file.read_exact(&mut sector)?;
        ensure!(sector[0] == 1, "primary volume descriptor missing");
        let record_len = sector[156] as usize;
        ensure!(record_len >= 34, "invalid root directory record");
        let record = &sector[156..156 + record_len];
        let extent = u32::from_le_bytes([record[2], record[3], record[4], record[5]]);
        let size = u32::from_le_bytes([record[10], record[11], record[12], record[13]]);
        Ok(DirectoryRecord { extent, size })
    }
}
