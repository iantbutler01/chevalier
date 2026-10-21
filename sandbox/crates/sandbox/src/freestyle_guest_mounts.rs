//! Guest-side Freestyle mount scripts: the boot script that launches each shared mount's
//! daemon, and the refresh an attach runs to bring a guest's script up to date.

use std::collections::HashMap;

use super::{shell_quote, shell_words_join};
use crate::{
    MOUNT_CREDENTIAL_PLACEHOLDER, ManagedMountConfig, SharedMount, render_shared_mount_template,
};

/// Guest-side marker directory: one file per mount tag records that the mount
/// command was launched in this boot, so attach does not relaunch it.
pub(super) const MOUNT_STATE_DIR: &str = "/run/chevalier/mounts";
/// The mount script and unit are written into the guest so mounts come back on
/// every boot without the facade having to remember them.
pub(super) const MOUNT_SCRIPT_PATH: &str = "/etc/chevalier/mounts.sh";
pub(super) const MOUNT_UNIT_PATH: &str = "/etc/systemd/system/chevalier-mounts.service";
pub(super) const MOUNT_UNIT: &str = "[Unit]\nDescription=Chevalier shared mounts\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=oneshot\nRemainAfterExit=yes\nExecStart=/bin/sh /etc/chevalier/mounts.sh\n\n[Install]\nWantedBy=multi-user.target\n";
pub(super) const MOUNT_SCRIPT_NEXT_PATH: &str = "/etc/chevalier/mounts.sh.next";
/// Holds the mount scripts, which carry mount credentials: root's alone.
pub(super) const MOUNT_SCRIPT_DIR: &str = "/etc/chevalier";

/// Run as root before a mount script is written. The directory is made (or, on a guest
/// created before this, tightened to) 0700 root, so a script is never readable by the
/// session user even for the moment before its own `chmod 600`; and a refresh's script
/// left by an interrupted attach is removed rather than left holding credentials.
pub(super) fn mount_script_dir_prepare_command() -> String {
    format!(
        "install -d -o root -g root -m 0700 {MOUNT_SCRIPT_DIR} && chown root:root {MOUNT_SCRIPT_DIR} && chmod 0700 {MOUNT_SCRIPT_DIR} && rm -f {MOUNT_SCRIPT_NEXT_PATH}"
    )
}

/// Installs the mount script arriving on stdin at `path`: the directory made root-only
/// first, the file never readable by anyone but root.
pub(super) fn mount_script_install_command(path: &str) -> String {
    format!(
        "{} && (umask 077 && cat > {path}) && chmod 600 {path}",
        mount_script_dir_prepare_command()
    )
}

/// Seconds an attach gives the guest's mount script to confirm every mount. Boot runs
/// the same script under no such bound, so a hydrating root keeps its long wait there.
const MOUNT_REPLAY_SECS: u64 = 10;
/// How long the script waits for a concurrent run (boot, another attach) to release the
/// lock; inside the replay bound so a held lock reports "not ready" rather than a timeout.
const MOUNT_LOCK_WAIT_SECS: u64 = MOUNT_REPLAY_SECS - 2;
/// Seconds the refresh spends looking for processes that use the mounts. A scan that
/// does not finish counts as busy.
const MOUNT_BUSY_SCAN_SECS: u64 = 3;
/// `timeout -k 1` may run one second past its limit before its kill lands.
const KILL_GRACE_SECS: u64 = 1;
/// The refresh's own bound covers its longest path, a busy scan followed by a replay,
/// plus two seconds for the cheap steps, so it never cuts short a step whose own bound
/// has not expired.
const MOUNT_REFRESH_SECS: u64 =
    MOUNT_BUSY_SCAN_SECS + KILL_GRACE_SECS + MOUNT_REPLAY_SECS + KILL_GRACE_SECS + 2;
/// Provider deadlines for the attach-time commands, above the guest-side bounds.
pub(super) const MOUNT_REFRESH_EXEC_MS: u64 = (MOUNT_REFRESH_SECS + KILL_GRACE_SECS + 2) * 1000;
pub(super) const MOUNT_REPLAY_EXEC_MS: u64 = (MOUNT_REPLAY_SECS + KILL_GRACE_SECS + 2) * 1000;
/// Reading the guest's boot id, before a mount restart and while waiting for the new boot.
pub(super) const GUEST_BOOT_ID_EXEC_MS: u64 = 2_000;
/// How long a mount restart may take to come back as a new boot. Measured on staging
/// (2026-10-21): poweroff to the next boot's first log line took 6 s.
pub(super) const GUEST_RESTART_BOUND: std::time::Duration = std::time::Duration::from_secs(60);
// The bounds nest, outermost last; the API gives a whole attach 45 s.
const _: () = {
    assert!(MOUNT_LOCK_WAIT_SECS < MOUNT_REPLAY_SECS);
    assert!(MOUNT_REFRESH_SECS > MOUNT_BUSY_SCAN_SECS + MOUNT_REPLAY_SECS + 2 * KILL_GRACE_SECS);
    assert!(MOUNT_REFRESH_EXEC_MS > (MOUNT_REFRESH_SECS + KILL_GRACE_SECS) * 1000);
    assert!(MOUNT_REPLAY_EXEC_MS > (MOUNT_REPLAY_SECS + KILL_GRACE_SECS) * 1000);
    assert!(MOUNT_REFRESH_EXEC_MS <= 20_000);
};

/// The facts of every mount a script launches, in order: mountpoint, environment,
/// argv, log and marker. Both script generations print the same lines for the same
/// mounts once the self-healing script's bounded `mkdir` and its lock-closing redirect
/// are normalized, so two scripts that launch the same mounts compare equal here even
/// when their launch logic differs.
const LAUNCHES_SED: &str = r"sed -n -E -e 's/^  timeout -k 1 2 mkdir -p /  mkdir -p /' -e 's/ 9>&- &$/ \&/' -e '/^  (mkdir -p |export |nohup |echo \$! > )/p'";

/// Brings the guest's script to `mounts.sh.next` and reports `mounts=…` on its last line:
/// - `unchanged`: the same bytes; replayed.
/// - `updated`: different bytes that launch the same mounts (a new launch logic, or the
///   self-heal switch flipped). Installed in place and replayed; running daemons are kept,
///   so no guest restarts for a change in how mounts are launched.
/// - `restart`: different mounts and nothing uses the current ones. The new script is
///   installed and the caller restarts the guest: mounts nest (the workspaces sit inside
///   the Nym's read-only root), and a guest whose mounts came up in a different order
///   cannot have them released cleanly while it runs.
/// - `deferred`: different mounts but a process uses them, or the scan for one did not
///   finish; the current script is replayed and the change waits for a later attach.
///
/// Every replay is bounded; `--foreground` times out the shell without killing a mount
/// daemon that is still hydrating. `mounts=not-ready` with status 1 means a mount did not
/// come up within the bound.
fn mount_refresh_script() -> String {
    format!(
        r#"set -u
cur={MOUNT_SCRIPT_PATH}; next={MOUNT_SCRIPT_NEXT_PATH}; state={MOUNT_STATE_DIR}
chmod 600 "$next"
replay() {{ timeout --foreground -k {KILL_GRACE_SECS} {MOUNT_REPLAY_SECS} /bin/sh "$cur" || {{ echo mounts=not-ready; exit 1; }}; }}
launches() {{ {LAUNCHES_SED} "$1"; }}
if [ -f "$cur" ] && cmp -s "$next" "$cur"; then rm -f "$next"; replay; echo mounts=unchanged; exit 0; fi
if [ -f "$cur" ]; then
  wanted=$(launches "$next"); running=$(launches "$cur")
  if [ -n "$wanted" ] && [ "$wanted" = "$running" ]; then mv "$next" "$cur"; chmod 600 "$cur"; replay; echo mounts=updated; exit 0; fi
fi
pids=""
for marker in "$state"/*; do case "$marker" in *.log) continue;; esac; [ -f "$marker" ] && pids="$pids $(cat "$marker")"; done
points=$(awk '$3 ~ /^fuse/ && $3 != "fusectl" {{print $2}}' /proc/mounts)
scan=$(cat <<'SCAN'
for proc in /proc/[0-9]*; do
  pid=${{proc#/proc/}}
  case " $SKIP $$ " in *" $pid "*) continue;; esac
  for link in "$proc/cwd" "$proc"/fd/*; do
    target=$(readlink "$link" 2>/dev/null) || continue
    for point in $POINTS; do
      case "$target" in "$point"|"$point"/*) echo "$pid"; exit 0;; esac
    done
  done
done
exit 0
SCAN
)
busy=$(SKIP="$pids $$" POINTS="$points" timeout -k {KILL_GRACE_SECS} {MOUNT_BUSY_SCAN_SECS} sh -c "$scan") || busy=unknown
if [ -n "$busy" ] && [ -f "$cur" ]; then rm -f "$next"; replay; echo "mounts=deferred busy=$busy"; exit 0; fi
mv "$next" "$cur"; chmod 600 "$cur"
echo mounts=restart
"#
    )
}

/// The command an attach runs to refresh a guest's mounts (see `mount_refresh_script`).
pub(super) fn mount_refresh_command() -> String {
    format!(
        "timeout --foreground -k {KILL_GRACE_SECS} {MOUNT_REFRESH_SECS} sh -c {}",
        shell_quote(&mount_refresh_script())
    )
}

/// The command an attach with no mounts runs: replay whatever script the guest has.
pub(super) fn mount_replay_command() -> String {
    format!(
        "if [ -f {MOUNT_SCRIPT_PATH} ]; then timeout --foreground -k {KILL_GRACE_SECS} {MOUNT_REPLAY_SECS} /bin/sh {MOUNT_SCRIPT_PATH}; fi"
    )
}

/// What an attach did to a guest's mounts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MountRefresh {
    /// The caller passed no mounts; the guest's own script was replayed.
    Replayed,
    /// The guest already runs exactly these mounts.
    Unchanged,
    /// The guest's script launched the same mounts differently; the new script replaced
    /// it in place and was replayed, without a restart.
    Updated,
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
            "updated" => Some(Self::Updated),
            "restart" => Some(Self::Refreshed),
            "deferred" => Some(Self::Deferred),
            _ => None,
        }
    }
}

/// Which boot script a guest is given. Both launch the same mounts, so switching between
/// them is an in-place update (`MountRefresh::Updated`), never a restart.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum GuestMountScript {
    /// Replaces a daemon whose mount the kernel no longer lists, bounds every probe, and
    /// serializes boot and attach on a lock.
    #[default]
    SelfHealing,
    /// Byte for byte the script guests ran before self-heal (chevalier `0b265d1f`). Kept as
    /// the off position of the self-heal switch.
    Legacy,
}

impl GuestMountScript {
    /// The guest's boot-time mount script for `mounts`. Deterministic, so the same mounts
    /// always render the same bytes and a refresh can tell "unchanged" by comparing files.
    pub(super) fn render(self, mounts: &[RenderedMount]) -> String {
        match self {
            Self::SelfHealing => mount_script(mounts),
            Self::Legacy => {
                let mut script = format!("set -e\nmkdir -p {MOUNT_STATE_DIR}\n");
                for mount in mounts {
                    script.push_str(&mount.legacy_launch_script());
                }
                script
            }
        }
    }
}

/// The self-healing boot script for `mounts` (`GuestMountScript::SelfHealing`).
pub(super) fn mount_script(mounts: &[RenderedMount]) -> String {
    // Boot and attach share markers. Daemons close fd 9 so they do not retain this lock.
    // `mount_state` reads the kernel's mount table, which never waits on a FUSE daemon:
    // 0 mounted, 3 not mounted, anything else (unreadable, timed out) unknown.
    let mut script = format!(
        r#"set -e
mkdir -p {MOUNT_STATE_DIR}
exec 9>{MOUNT_STATE_DIR}/.lock
flock -w {MOUNT_LOCK_WAIT_SECS} 9 || exit 1
mount_state() {{ P=$1 timeout -k 1 2 awk '$5 == ENVIRON["P"] {{ found = 1 }} END {{ exit found ? 0 : 3 }}' /proc/self/mountinfo; }}
"#
    );
    // A mount waits only for the earlier mounts it sits inside (started on a bare mountpoint,
    // the parent would later hide it); siblings come up side by side, and the script ends
    // once every mount is up.
    for (index, mount) in mounts.iter().enumerate() {
        for parent in mounts[..index]
            .iter()
            .filter(|earlier| contains_path(&earlier.mountpoint, &mount.mountpoint))
        {
            script.push_str(&parent.wait_script());
        }
        script.push_str(&mount.start_script());
    }
    for mount in mounts {
        script.push_str(&mount.wait_script());
    }
    script
}

/// Whether `inner` is `outer` or lies beneath it.
fn contains_path(outer: &str, inner: &str) -> bool {
    let outer = outer.trim_end_matches('/');
    inner == outer
        || inner
            .strip_prefix(outer)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// How long the mount script waits for one mount to appear (before starting the mounts
/// inside it, or before it ends).
/// A fresh guest walks each scope's namespace before mounting it (file bytes follow in
/// the background, spec 86), which is seconds for a large Nym root, but a daemon with
/// eager hydration (`CHEVALIER_VFS_EAGER_HYDRATE=1`, or a backing filesystem without
/// user xattrs) still takes minutes; moving on early starts the nested mounts on the bare
/// mountpoint, where the root later hides them. It sits just inside the exec cap the
/// whole script runs under.
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
    /// `credential` fills `{vfs_credential}` in the template's secrets; absent, it is
    /// rendered empty so no literal placeholder reaches the guest.
    pub(super) fn render(
        template: &ManagedMountConfig,
        shared: &SharedMount,
        credential: Option<&str>,
    ) -> Self {
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
            env.insert(
                key.clone(),
                render(value).replace(MOUNT_CREDENTIAL_PLACEHOLDER, credential.unwrap_or_default()),
            );
        }
        Self {
            mount_tag: shared.mount_tag.clone(),
            mountpoint,
            command,
            env,
            read_only: template.read_only.unwrap_or(shared.read_only),
        }
    }

    /// The quoted pieces both script generations launch this mount from.
    fn launch_parts(&self) -> LaunchParts {
        let marker = format!("{MOUNT_STATE_DIR}/{}", sanitize_tag(&self.mount_tag));
        let mut exports = self
            .env
            .iter()
            .map(|(key, value)| format!("export {key}={}", shell_quote(value)))
            .collect::<Vec<_>>();
        exports.sort();
        LaunchParts {
            log: shell_quote(&format!("{marker}.log")),
            marker: shell_quote(&marker),
            mountpoint: shell_quote(&self.mountpoint),
            exports: if exports.is_empty() {
                ":".to_string()
            } else {
                exports.join("\n  ")
            },
            read_only: if self.read_only { "true" } else { "false" },
            argv: shell_words_join(self.command.iter().map(String::as_str)),
        }
    }

    /// Wait for this mount to appear, whoever launched it, while its daemon lives.
    pub(super) fn wait_script(&self) -> String {
        let marker = shell_quote(&format!(
            "{MOUNT_STATE_DIR}/{}",
            sanitize_tag(&self.mount_tag)
        ));
        format!(
            r#"pid=$(cat {marker} 2>/dev/null || echo 0)
for _ in $(seq 1 {polls}); do mount_state {table_path} && break; kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
mount_state {table_path} || {{ echo mount=not-ready >&2; exit 1; }}
"#,
            table_path = shell_quote(&mount_table_path(&self.mountpoint)),
            // Polled finely: every mount's wait sits on a new computer's first command.
            polls = MOUNT_READY_WAIT_SECS * 10,
        )
    }

    /// Start a missing daemon, or replace one the kernel no longer lists as mounted whose
    /// log shows it mounted before (an orphan). A live daemon that never logged its mount
    /// is still hydrating and keeps its wait. When the mount table cannot be read, nothing
    /// is replaced: a daemon is killed only on a definite "not mounted".
    pub(super) fn start_script(&self) -> String {
        let LaunchParts {
            marker,
            log,
            mountpoint,
            exports,
            read_only,
            argv,
        } = self.launch_parts();
        format!(
            r#"present=0; mount_state {table_path} || present=$?
if [ "$present" = 3 ]; then
  pid=$(cat {marker} 2>/dev/null || true)
  case "$pid" in ''|*[!0-9]*) pid=0;; esac
  if [ "$pid" -gt 1 ]; then
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f {marker}
    elif sed 's/\x1b\[[0-9;]*m//g' {log} 2>/dev/null | grep -Fq -- {mounted_log}; then
      kill -TERM "$pid" 2>/dev/null || true
      for _ in 1 2 3 4 5; do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
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
"#,
            table_path = shell_quote(&mount_table_path(&self.mountpoint)),
            mounted_log = shell_quote(&format!("fuser::session: Mounting {}", self.mountpoint)),
        )
    }

    /// This mount's lines in `GuestMountScript::Legacy`.
    fn legacy_launch_script(&self) -> String {
        let LaunchParts {
            marker,
            log,
            mountpoint,
            exports,
            read_only,
            argv,
        } = self.launch_parts();
        format!(
            "if [ ! -e {marker} ]; then\n  mkdir -p {mountpoint}\n  {exports}\n  export CHEVALIER_VFS_READ_ONLY={read_only}\n  nohup {argv} >{log} 2>&1 </dev/null &\n  echo $! > {marker}\nfi\npid=$(cat {marker} 2>/dev/null || echo 0)\nfor _ in $(seq 1 {polls}); do mountpoint -q {mountpoint} && break; kill -0 \"$pid\" 2>/dev/null || break; sleep 0.5; done\n",
            polls = MOUNT_READY_WAIT_SECS * 2,
        )
    }
}

struct LaunchParts {
    marker: String,
    log: String,
    mountpoint: String,
    exports: String,
    read_only: &'static str,
    argv: String,
}

/// `path` as `/proc/self/mountinfo` spells it: the kernel octal-escapes space, tab,
/// newline and backslash in mountpoints.
fn mount_table_path(path: &str) -> String {
    let mut escaped = String::with_capacity(path.len());
    for c in path.chars() {
        match c {
            ' ' => escaped.push_str("\\040"),
            '\t' => escaped.push_str("\\011"),
            '\n' => escaped.push_str("\\012"),
            '\\' => escaped.push_str("\\134"),
            c => escaped.push(c),
        }
    }
    escaped
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
        let script = mount_refresh_script();
        let at = |needle: &str| script.find(needle).unwrap_or_else(|| panic!("{needle}"));
        let busy_check = at("echo \"$pid\"; exit 0");
        let deferred = at("mounts=deferred");
        let restart = at("mounts=restart");
        assert!(busy_check < deferred && deferred < script.rfind("mv \"$next\" \"$cur\"").unwrap());
        assert!(deferred < restart);
        // Unchanged and same-mount scripts are settled before anything is inspected.
        assert!(at("mounts=unchanged") < busy_check && at("mounts=updated") < busy_check);
        for path in [MOUNT_SCRIPT_NEXT_PATH, MOUNT_SCRIPT_PATH, MOUNT_STATE_DIR] {
            assert!(script.contains(path));
        }
        // Nothing is stopped or unmounted in place.
        assert!(!script.contains("kill -TERM"));
        assert!(!script.contains("umount"));
    }

    /// The script directory ends 0700 whether it was missing or made world-readable by an
    /// older guest, and a leftover refresh script is gone before the next is written.
    #[test]
    fn the_script_directory_is_made_root_only_before_a_script_is_written() {
        let root = std::env::temp_dir().join(format!("mount-dir-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let dir = root.join("etc/chevalier");
        let command = mount_script_dir_prepare_command()
            .replace(MOUNT_SCRIPT_DIR, &dir.display().to_string())
            .replace("-o root -g root ", "")
            .replace(&format!("chown root:root {} && ", dir.display()), "");
        let run = || {
            assert!(
                std::process::Command::new("sh")
                    .arg("-c")
                    .arg(&command)
                    .status()
                    .unwrap()
                    .success()
            );
        };
        let mode = || {
            use std::os::unix::fs::PermissionsExt;
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777
        };
        run();
        assert_eq!(mode(), 0o700);
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        std::fs::write(dir.join("mounts.sh.next"), "credential").unwrap();
        run();
        assert_eq!(mode(), 0o700);
        assert!(!dir.join("mounts.sh.next").exists());
        std::fs::remove_dir_all(&root).unwrap();
        // The refresh tightens the script it was given before anything else.
        let script = mount_refresh_script();
        assert!(script.find("chmod 600 \"$next\"").unwrap() < script.find("cmp -s").unwrap());
    }

    #[test]
    fn mount_table_paths_use_the_kernels_escapes() {
        assert_eq!(
            mount_table_path("/nym/vm/mounts/task"),
            "/nym/vm/mounts/task"
        );
        assert_eq!(mount_table_path("/a b\\c"), "/a\\040b\\134c");
    }
}
