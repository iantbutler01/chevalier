//! Execute the rendered guest scripts with real processes against a fake mount table
//! (`/proc/self/mountinfo`, `/proc/mounts`) and a `mountpoint` whose answer the test
//! controls, so each test decides what a mount looks like without root or FUSE.
use super::*;
use std::{fs, os::unix::fs::PermissionsExt, process::Command, time::Instant};

/// What a deployed guest holds today: `mount_script` at chevalier origin/main `0b265d1f`,
/// rendered for `nym_mounts()`.
const DEPLOYED_SCRIPT: &str = include_str!("freestyle_mount_fixture_deployed.sh");

/// A Nym computer's four mounts, as the fixture was rendered from: the read-only root
/// with the workspaces nested inside it.
fn nym_mounts() -> Vec<RenderedMount> {
    let mount = |tag: &str, point: &str, read_only| RenderedMount {
        mount_tag: tag.into(),
        mountpoint: point.into(),
        read_only,
        env: HashMap::from([
            (
                "CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN".into(),
                "synthetic".into(),
            ),
            ("CHEVALIER_VFS_OWNER".into(), "nym".into()),
        ]),
        command: vec![
            "/usr/local/bin/chevalier-vfs-fuse".into(),
            "--endpoint".into(),
            "http://127.0.0.1:18991".into(),
            "--scope".into(),
            tag.into(),
            "--tag".into(),
            tag.into(),
            "--state-dir".into(),
            format!("/var/lib/lane-b/{tag}"),
            point.into(),
        ],
    };
    vec![
        mount("root", "/nym", true),
        mount("shared", "/nym/vm/mounts/shared", false),
        mount("task", "/nym/vm/mounts/task", false),
        mount("skills", "/nym/vm/mounts/skills", true),
    ]
}

/// A guest filesystem in a temporary directory. `localize` points a rendered script at
/// it; `run` executes bash with the fake `mountpoint` (and any other fakes) first on PATH.
struct Guest {
    _dir: tempfile::TempDir,
    root: String,
}

impl Guest {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap().to_string();
        for sub in ["bin", "etc", "run/mounts", "proc/1"] {
            fs::create_dir_all(dir.path().join(sub)).unwrap();
        }
        fs::write(dir.path().join("mountinfo"), "").unwrap();
        fs::write(dir.path().join("proc-mounts"), "").unwrap();
        // One process for the busy scan to inspect; it uses none of the mounts.
        std::os::unix::fs::symlink("/", dir.path().join("proc/1/cwd")).unwrap();
        let guest = Self { _dir: dir, root };
        // The probe the script before this fix relied on: it stats the mountpoint, so on a
        // FUSE root whose daemon does not answer it blocks (`hang`).
        guest.fake(
            "mountpoint",
            &format!(
                "[ -e {root}/hang ] && exec sleep 30\n[ -f {root}/mountinfo ] && grep -q \" $2 \" {root}/mountinfo\n",
                root = guest.root
            ),
        );
        guest
    }

    fn fake(&self, name: &str, body: &str) {
        let path = format!("{}/bin/{name}", self.root);
        fs::write(&path, format!("#!/bin/sh\n{body}")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn localize(&self, text: &str) -> String {
        text.replace("/nym", &format!("{}/nym", self.root))
            .replace(
                "/run/chevalier/mounts",
                &format!("{}/run/mounts", self.root),
            )
            .replace("/etc/chevalier", &format!("{}/etc", self.root))
            .replace("/proc/self/mountinfo", &format!("{}/mountinfo", self.root))
            .replace("/proc/mounts", &format!("{}/proc-mounts", self.root))
            .replace("/proc/[0-9]*", &format!("{}/proc/[0-9]*", self.root))
    }

    /// Runs `scenario` under bash. Helpers: `mi PATH` adds PATH to the mount table,
    /// `daemon TAG` starts a stand-in daemon recorded under TAG's marker,
    /// `mounted_log TAG PATH` gives TAG's daemon a log saying it mounted PATH (coloured, as
    /// real guest logs are). Every process recorded under a marker is killed on exit.
    fn run(&self, scenario: &str) -> std::process::Output {
        let root = &self.root;
        let script = format!(
            r#"set -eu
export PATH={root}/bin:$PATH
cd {root}
started=""
trap 'for p in $started $(cat {root}/run/mounts/* 2>/dev/null); do kill "$p" 2>/dev/null || true; done' EXIT
mi() {{ echo "36 25 0:99 / $1 rw,relatime shared:1 - fuse.test test rw" >> {root}/mountinfo; echo "test $1 fuse.test rw 0 0" >> {root}/proc-mounts; }}
daemon() {{ sleep 60 >/dev/null 2>&1 & started="$started $!"; echo $! > {root}/run/mounts/$1; }}
mounted_log() {{ printf '\033[2mfuser::session\033[0m\033[2m:\033[0m Mounting %s\n' "$2" > {root}/run/mounts/$1.log; }}
{scenario}"#
        );
        Command::new("bash").args(["-c", &script]).output().unwrap()
    }
}

fn assert_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "status={:?}\nstdout={}\nstderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// One mount at `{root}/point` whose stand-in daemon "mounts" by adding itself to the
/// mount table and then serves forever.
fn single_mount_script(guest: &Guest) -> String {
    let point = format!("{}/point", guest.root);
    let mount = RenderedMount {
        mount_tag: "test".into(),
        mountpoint: point.clone(),
        command: vec![
            "sh".into(),
            "-c".into(),
            format!(
                "echo '36 25 0:99 / {point} rw - fuse.test test rw' >> {}/mountinfo; exec sleep 60",
                guest.root
            ),
        ],
        env: HashMap::new(),
        read_only: false,
    };
    let script = guest.localize(&mount_script(&[mount]));
    fs::write(format!("{}/mounts.sh", guest.root), &script).unwrap();
    point
}

#[test]
fn a_detached_mount_with_a_live_mounted_daemon_is_replaced() {
    let guest = Guest::new();
    let point = single_mount_script(&guest);
    assert_success(&guest.run(&format!(
        r#"
daemon test; old=$(cat run/mounts/test)
mounted_log test {point}
timeout --foreground -k 1 3 sh mounts.sh
new=$(cat run/mounts/test)
[ "$new" != "$old" ]
grep -q " {point} " mountinfo
! kill -0 "$old" 2>/dev/null
# A daemon must not retain the script lock after its launching shell exits.
timeout --foreground -k 1 3 sh mounts.sh
[ "$(cat run/mounts/test)" = "$new" ]
"#
    )));
}

#[test]
fn attach_deadline_preserves_a_hydrating_daemon() {
    let guest = Guest::new();
    let _point = single_mount_script(&guest);
    assert_success(&guest.run(
        r#"
daemon test; old=$(cat run/mounts/test)
echo hydrating > run/mounts/test.log
set +e
timeout --foreground -k 1 0.5 sh mounts.sh
status=$?
set -e
[ "$status" = 124 ]
[ "$(cat run/mounts/test)" = "$old" ]
kill -0 "$old"
sh mounts.sh &
boot=$!
sleep 0.1
set +e
timeout --foreground -k 1 0.5 sh mounts.sh
status=$?
set -e
[ "$status" = 124 ]
kill -0 "$old"
kill -0 "$boot"
kill "$boot"
"#,
    ));
}

/// Review finding: a FUSE root whose daemon is slow to answer made a stat-based probe time
/// out, and the script then read "not mounted" and SIGKILLed a healthy daemon.
#[test]
fn a_mount_whose_probe_hangs_is_not_killed() {
    let guest = Guest::new();
    let point = single_mount_script(&guest);
    assert_success(&guest.run(&format!(
        r#"
daemon test; old=$(cat run/mounts/test)
mounted_log test {point}
mi {point}
touch hang
set +e
timeout --foreground -k 1 5 sh mounts.sh
status=$?
set -e
echo "mount script status=$status"
[ "$(cat run/mounts/test)" = "$old" ] || {{ echo "marker replaced: the healthy daemon was swapped out"; exit 1; }}
kill -0 "$old" || {{ echo "the healthy daemon was killed"; exit 1; }}
[ "$status" = 0 ]
"#
    )));
}

/// A mount table the script cannot read says nothing about the mount: nothing is killed.
#[test]
fn an_unreadable_mount_table_kills_nothing() {
    let guest = Guest::new();
    let point = single_mount_script(&guest);
    assert_success(&guest.run(&format!(
        r#"
daemon test; old=$(cat run/mounts/test)
mounted_log test {point}
rm mountinfo
set +e
timeout --foreground -k 1 2 sh mounts.sh
status=$?
set -e
echo "mount script status=$status"
[ "$(cat run/mounts/test)" = "$old" ] || {{ echo "marker replaced: the daemon was swapped out"; exit 1; }}
kill -0 "$old" || {{ echo "the daemon was killed"; exit 1; }}
[ "$status" != 0 ]
"#
    )));
}

/// Writes the guest's current script and the attach's next one, starts a live stand-in
/// daemon for every Nym mount, and lists each mount as mounted.
fn nym_guest(current: &str, next: &str) -> Guest {
    let guest = Guest::new();
    fs::write(
        format!("{}/etc/mounts.sh", guest.root),
        guest.localize(current),
    )
    .unwrap();
    fs::write(
        format!("{}/etc/mounts.sh.next", guest.root),
        guest.localize(next),
    )
    .unwrap();
    guest
}

const NYM_DAEMONS: &str = r#"
for mount in root:nym shared:nym/vm/mounts/shared task:nym/vm/mounts/task skills:nym/vm/mounts/skills; do
  daemon "${mount%%:*}"
  mi "$PWD/${mount#*:}"
  mounted_log "${mount%%:*}" "$PWD/${mount#*:}"
done
pids() { cat run/mounts/root run/mounts/shared run/mounts/task run/mounts/skills | tr '\n' ' '; }
before=$(pids)
refresh() { sh -c "$(cat refresh.sh)"; }
"#;

fn write_refresh(guest: &Guest) {
    fs::write(
        format!("{}/refresh.sh", guest.root),
        guest.localize(&mount_refresh_command()),
    )
    .unwrap();
}

/// Review finding: every deployed guest holds the script before this change, so a byte
/// comparison would restart every idle computer on its first attach. The same mounts are
/// updated in place: no restart, no daemon touched, and the next attach self-heals.
#[test]
fn a_guest_on_the_deployed_script_is_updated_in_place_and_then_self_heals() {
    let mounts = nym_mounts();
    let guest = nym_guest(DEPLOYED_SCRIPT, &mount_script(&mounts));
    write_refresh(&guest);
    let installed = guest.localize(&mount_script(&mounts));
    let root = &guest.root;
    let output = guest.run(&format!(
        r#"{NYM_DAEMONS}
out=$(refresh)
echo "$out"
[ "$(echo "$out" | tail -1)" = mounts=updated ]
[ "$(pids)" = "$before" ]
[ ! -e etc/mounts.sh.next ]
# The next attach: the task mount is detached while its daemon lives on.
old_task=$(cat run/mounts/task)
mounted_log task {root}/nym/vm/mounts/task
grep -v " {root}/nym/vm/mounts/task " mountinfo > mountinfo.new; mv mountinfo.new mountinfo
cp etc/mounts.sh etc/mounts.sh.next
set +e
out=$(refresh)
set -e
echo "$out"
! kill -0 "$old_task" 2>/dev/null
[ "$(cat run/mounts/task)" != "$old_task" ]
"#
    ));
    assert_success(&output);
    assert_eq!(
        fs::read_to_string(format!("{root}/etc/mounts.sh")).unwrap(),
        installed
    );
}

/// Mounts that genuinely differ still take the restart, as before.
#[test]
fn different_mounts_still_restart_the_guest() {
    let mut mounts = nym_mounts();
    mounts[2]
        .env
        .insert("CHEVALIER_VFS_CACHE".into(), "large".into());
    let guest = nym_guest(DEPLOYED_SCRIPT, &mount_script(&mounts));
    write_refresh(&guest);
    assert_success(&guest.run(&format!(
        r#"{NYM_DAEMONS}
out=$(refresh)
echo "$out"
[ "$(echo "$out" | tail -1)" = mounts=restart ]
"#
    )));
}

/// Review finding: the busy scan ran inside the refresh's outer bound with no bound of its
/// own, so a slow scan on a busy guest let the outer timeout kill a healthy attach before
/// it reported. A scan that cannot finish counts as busy: nothing restarts, and the
/// guest's current mounts are confirmed within the replay budget.
#[test]
fn a_slow_busy_scan_leaves_a_healthy_attach_ready() {
    let mut mounts = nym_mounts();
    mounts[2]
        .env
        .insert("CHEVALIER_VFS_CACHE".into(), "large".into());
    let guest = nym_guest(DEPLOYED_SCRIPT, &mount_script(&mounts));
    write_refresh(&guest);
    guest.fake("readlink", "exec sleep 100\n");
    let started = Instant::now();
    let output = guest.run(&format!(
        r#"{NYM_DAEMONS}
set +e
out=$(refresh)
status=$?
set -e
echo "refresh status=$status report=$out"
[ "$(echo "$out" | tail -1)" = "mounts=deferred busy=unknown" ]
[ "$(pids)" = "$before" ]
"#
    ));
    assert_success(&output);
    assert!(started.elapsed().as_secs() < 10, "{:?}", started.elapsed());
}

mod switch {
    use super::*;

    /// The kill switch renders exactly what guests run today, so turning self-heal off
    /// changes no guest's script.
    #[test]
    fn the_switched_off_script_is_what_guests_run_today() {
        assert_eq!(
            GuestMountScript::Legacy.render(&nym_mounts()),
            DEPLOYED_SCRIPT
        );
    }

    /// Turning the switch off on a self-healing guest is an in-place update too.
    #[test]
    fn switching_self_heal_off_updates_the_guest_in_place() {
        let mounts = nym_mounts();
        let guest = nym_guest(
            &GuestMountScript::SelfHealing.render(&mounts),
            &GuestMountScript::Legacy.render(&mounts),
        );
        write_refresh(&guest);
        assert_success(&guest.run(&format!(
            r#"{NYM_DAEMONS}
out=$(refresh)
echo "$out"
[ "$(echo "$out" | tail -1)" = mounts=updated ]
[ "$(pids)" = "$before" ]
"#
        )));
        assert_eq!(
            fs::read_to_string(format!("{}/etc/mounts.sh", guest.root)).unwrap(),
            guest.localize(DEPLOYED_SCRIPT)
        );
    }
}
