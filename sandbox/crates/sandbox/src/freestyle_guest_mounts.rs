//! Guest-side Freestyle mount scripts: the boot script that launches each shared mount's
//! daemon, and the refresh an attach runs to bring a guest's script up to date.

use std::collections::HashMap;

use super::{shell_quote, shell_words_join};
use crate::{ManagedMountConfig, SharedMount, render_shared_mount_template};

/// Guest-side marker directory: one file per mount tag records that the mount
/// command was launched in this boot, so attach does not relaunch it.
pub(super) const MOUNT_STATE_DIR: &str = "/run/chevalier/mounts";
/// The mount script and unit are written into the guest so mounts come back on
/// every boot without the facade having to remember them.
pub(super) const MOUNT_SCRIPT_PATH: &str = "/etc/chevalier/mounts.sh";
pub(super) const MOUNT_UNIT_PATH: &str = "/etc/systemd/system/chevalier-mounts.service";
pub(super) const MOUNT_UNIT: &str = "[Unit]\nDescription=Chevalier shared mounts\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=oneshot\nRemainAfterExit=yes\nExecStart=/bin/sh /etc/chevalier/mounts.sh\n\n[Install]\nWantedBy=multi-user.target\n";
pub(super) const MOUNT_SCRIPT_NEXT_PATH: &str = "/etc/chevalier/mounts.sh.next";
/// Swaps in `mounts.sh.next` when it differs from the running script and nothing uses the
/// mounts, and reports `mounts=unchanged|restart|deferred` on its last line. A changed
/// script is applied by restarting the guest rather than remounting in place: mounts
/// nest (the workspaces sit inside the Nym's read-only root), and a guest whose mounts
/// came up in a different order cannot have them released cleanly while it runs.
/// Replay gets ten seconds; `--foreground` times out the shell without killing a
/// mount daemon that is still hydrating. A nonzero result reports mounts not ready.
pub(super) const MOUNT_REFRESH_SCRIPT: &str = r#"set -u
cur=/etc/chevalier/mounts.sh; next=/etc/chevalier/mounts.sh.next; state=/run/chevalier/mounts
if [ -f "$cur" ] && cmp -s "$next" "$cur"; then rm -f "$next"; timeout --foreground -k 1 10 /bin/sh "$cur" || { echo mounts=not-ready; exit 1; }; echo mounts=unchanged; exit 0; fi
pids=""
for marker in "$state"/*; do case "$marker" in *.log) continue;; esac; [ -f "$marker" ] && pids="$pids $(cat "$marker")"; done
points=$(awk '$3 ~ /^fuse/ && $3 != "fusectl" {print $2}' /proc/mounts)
busy=""
for proc in /proc/[0-9]*; do
  pid=${proc#/proc/}
  case " $pids $$ " in *" $pid "*) continue;; esac
  for link in "$proc/cwd" "$proc"/fd/*; do
    target=$(readlink "$link" 2>/dev/null) || continue
    for point in $points; do
      case "$target" in "$point"|"$point"/*) busy="$pid"; break 3;; esac
    done
  done
done
if [ -n "$busy" ] && [ -f "$cur" ]; then rm -f "$next"; timeout --foreground -k 1 10 /bin/sh "$cur" || { echo mounts=not-ready; exit 1; }; echo "mounts=deferred busy=$busy"; exit 0; fi
mv "$next" "$cur"; chmod 600 "$cur"
echo mounts=restart
"#;

/// What an attach did to a guest's mounts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MountRefresh {
    /// The caller passed no mounts; the guest's own script was replayed.
    Replayed,
    /// The guest already runs exactly these mounts.
    Unchanged,
    /// The new script was installed and the guest restarted to run it.
    Refreshed,
    /// The mounts differ but a process is using them; left for a later attach.
    Deferred,
}

impl MountRefresh {
    pub(super) fn from_report(stdout: &str) -> Option<Self> {
        let last = stdout
            .lines()
            .rev()
            .find(|line| line.starts_with("mounts="))?;
        match last
            .trim_start_matches("mounts=")
            .split_whitespace()
            .next()?
        {
            "unchanged" => Some(Self::Unchanged),
            "restart" => Some(Self::Refreshed),
            "deferred" => Some(Self::Deferred),
            _ => None,
        }
    }
}

/// The guest's boot-time mount script for `mounts`. Deterministic, so the same mounts
/// always render the same bytes and a refresh can tell "unchanged" by comparing files.
pub(super) fn mount_script(mounts: &[RenderedMount]) -> String {
    let mut script = String::from("set -e\n");
    // Boot and attach share markers. Daemons close fd 9 so they do not retain this lock.
    script.push_str(&format!(
        "mkdir -p {MOUNT_STATE_DIR}\nexec 9>{MOUNT_STATE_DIR}/.lock\nflock -w 8 9 || exit 1\n"
    ));
    for mount in mounts {
        script.push_str(&mount.launch_script());
    }
    script
}

/// How long the mount script waits for one mount to appear before starting the next.
/// A fresh guest hydrates each read-only scope eagerly before mounting it, and a Nym
/// root with tens of thousands of small files takes minutes; moving on early starts
/// the nested mounts on the bare mountpoint, where the root later hides them. It sits
/// just inside the exec cap the whole script runs under.
const MOUNT_READY_WAIT_SECS: u64 = 280;

/// A shared mount's launch command after `{placeholder}` rendering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RenderedMount {
    pub(super) mount_tag: String,
    pub(super) mountpoint: String,
    pub(super) command: Vec<String>,
    pub(super) env: HashMap<String, String>,
    pub(super) read_only: bool,
}

impl RenderedMount {
    pub(super) fn render(template: &ManagedMountConfig, shared: &SharedMount) -> Self {
        let mountpoint = if template.path.trim().is_empty() {
            shared.guest_path.clone()
        } else {
            render_shared_mount_template(&template.path, shared, Some(&shared.guest_path))
        };
        let render = |value: &str| render_shared_mount_template(value, shared, Some(&mountpoint));
        let command = template.command.iter().map(|value| render(value)).collect();
        let mut env: HashMap<String, String> = template
            .env
            .iter()
            .map(|(key, value)| (key.clone(), render(value)))
            .collect();
        for (key, value) in &template.secrets {
            env.insert(key.clone(), render(value));
        }
        Self {
            mount_tag: shared.mount_tag.clone(),
            mountpoint,
            command,
            env,
            read_only: template.read_only.unwrap_or(shared.read_only),
        }
    }

    /// Start a missing daemon or replace one whose mounted filesystem was detached.
    /// A live daemon with no prior mount log is still hydrating and keeps its wait.
    pub(super) fn launch_script(&self) -> String {
        let marker = format!("{MOUNT_STATE_DIR}/{}", sanitize_tag(&self.mount_tag));
        let mut exports = self
            .env
            .iter()
            .map(|(key, value)| format!("export {key}={}", shell_quote(value)))
            .collect::<Vec<_>>();
        exports.sort();
        let argv = shell_words_join(self.command.iter().map(String::as_str));
        format!(
            // Wait for this mount before the next starts, whether this run launched it or
            // a concurrent run (boot and an attach's replay) did: later mounts can sit
            // inside this one, and one started first would be hidden beneath it.
            r#"if ! timeout -k 1 1 mountpoint -q {mountpoint}; then
  pid=$(cat {marker} 2>/dev/null || true)
  case "$pid" in ''|*[!0-9]*) pid=0;; esac
  if [ "$pid" -gt 1 ]; then
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f {marker}
    elif sed 's/\x1b\[[0-9;]*m//g' {log} 2>/dev/null | grep -Fq -- {mounted_log}; then
      kill -TERM "$pid" 2>/dev/null || true
      kill -KILL "$pid" 2>/dev/null || true
      rm -f {marker}
    fi
  else
    rm -f {marker}
  fi
fi
if [ ! -e {marker} ]; then
  timeout -k 1 2 mkdir -p {mountpoint}
  {exports}
  export CHEVALIER_VFS_READ_ONLY={read_only}
  nohup {argv} >{log} 2>&1 </dev/null 9>&- &
  echo $! > {marker}
fi
pid=$(cat {marker} 2>/dev/null || echo 0)
for _ in $(seq 1 {polls}); do timeout -k 1 1 mountpoint -q {mountpoint} && break; kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
timeout -k 1 1 mountpoint -q {mountpoint} || {{ echo mount=not-ready >&2; exit 1; }}
"#,
            mounted_log = shell_quote(&format!("fuser::session: Mounting {}", self.mountpoint)),
            polls = MOUNT_READY_WAIT_SECS * 2,
            marker = shell_quote(&marker),
            mountpoint = shell_quote(&self.mountpoint),
            exports = if exports.is_empty() {
                ":".to_string()
            } else {
                exports.join("\n  ")
            },
            read_only = if self.read_only { "true" } else { "false" },
            argv = argv,
            log = shell_quote(&format!("{marker}.log")),
        )
    }
}

fn sanitize_tag(tag: &str) -> String {
    tag.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mount_refresh_reports_what_it_did() {
        assert_eq!(
            MountRefresh::from_report("mounted\nmounts=unchanged\n"),
            Some(MountRefresh::Unchanged)
        );
        assert_eq!(
            MountRefresh::from_report("mounts=deferred busy=4242\n"),
            Some(MountRefresh::Deferred)
        );
        assert_eq!(
            MountRefresh::from_report("mounts=restart"),
            Some(MountRefresh::Refreshed)
        );
        assert_eq!(MountRefresh::from_report("sh: cmp: not found\n"), None);
    }

    #[test]
    fn the_refresh_swaps_scripts_only_after_checking_nothing_uses_the_mounts() {
        let busy_check = MOUNT_REFRESH_SCRIPT.find("busy=\"$pid\"").unwrap();
        let deferred = MOUNT_REFRESH_SCRIPT.find("mounts=deferred").unwrap();
        let swap = MOUNT_REFRESH_SCRIPT.find("mv \"$next\" \"$cur\"").unwrap();
        let restart = MOUNT_REFRESH_SCRIPT.find("mounts=restart").unwrap();
        assert!(busy_check < deferred && deferred < swap && swap < restart);
        // Unchanged scripts are replayed before anything is inspected.
        assert!(MOUNT_REFRESH_SCRIPT.find("mounts=unchanged").unwrap() < busy_check);
        assert!(MOUNT_REFRESH_SCRIPT.contains(MOUNT_SCRIPT_NEXT_PATH));
        assert!(MOUNT_REFRESH_SCRIPT.contains(MOUNT_SCRIPT_PATH));
        assert!(MOUNT_REFRESH_SCRIPT.contains(MOUNT_STATE_DIR));
        // Nothing is stopped or unmounted in place.
        assert!(!MOUNT_REFRESH_SCRIPT.contains("kill -TERM"));
        assert!(!MOUNT_REFRESH_SCRIPT.contains("umount"));
    }
}
