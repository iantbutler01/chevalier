//! Execute the rendered guest scripts with real processes and a controlled mount probe.
use super::*;
use std::{fs, os::unix::fs::PermissionsExt, process::Command};

fn run_case(mounted_before: bool) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let point = format!("{root}/point");
    fs::write(
        dir.path().join("mountpoint"),
        "#!/bin/sh\n[ -f \"$2.ready\" ]\n",
    )
    .unwrap();
    fs::set_permissions(
        dir.path().join("mountpoint"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let mount = RenderedMount {
        mount_tag: "test".into(),
        mountpoint: point.clone(),
        command: vec![
            "sh".into(),
            "-c".into(),
            format!("touch {point}.ready; exec sleep 60"),
        ],
        env: HashMap::new(),
        read_only: false,
    };
    let script = mount_script(&[mount]).replace(MOUNT_STATE_DIR, &format!("{root}/state"));
    fs::write(dir.path().join("mounts.sh"), script).unwrap();
    let setup = format!(
        r#"
set -eu
export PATH={root}:$PATH
mkdir -p {root}/state
sleep 60 &
old=$!
echo "$old" > {root}/state/test
trap 'kill "$old" $(cat {root}/state/test) 2>/dev/null || true' EXIT
"#
    );
    let scenario = if mounted_before {
        format!(
            r#"
printf '\033[2mfuser::session\033[0m\033[2m:\033[0m Mounting {point}\n' > {root}/state/test.log
timeout --foreground -k 1 3 sh {root}/mounts.sh
new=$(cat {root}/state/test)
[ "$new" != "$old" ]
[ -f {point}.ready ]
! kill -0 "$old" 2>/dev/null
# A daemon must not retain the script lock after its launching shell exits.
timeout --foreground -k 1 3 sh {root}/mounts.sh
[ "$(cat {root}/state/test)" = "$new" ]
"#
        )
    } else {
        format!(
            r#"
echo hydrating > {root}/state/test.log
set +e
timeout --foreground -k 1 0.2 sh {root}/mounts.sh
status=$?
set -e
[ "$status" = 124 ]
[ "$(cat {root}/state/test)" = "$old" ]
kill -0 "$old"
[ ! -f {point}.ready ]
sh {root}/mounts.sh &
boot=$!
sleep 0.1
set +e
timeout --foreground -k 1 0.2 sh {root}/mounts.sh
status=$?
set -e
[ "$status" = 124 ]
kill -0 "$old"
kill -0 "$boot"
kill "$boot"
"#
        )
    };
    let output = Command::new("bash")
        .args(["-c", &(setup + &scenario)])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn detached_mounted_daemon_is_replaced() {
    run_case(true);
}

#[test]
fn attach_deadline_preserves_a_hydrating_daemon() {
    run_case(false);
}

async fn admin(control: &FreestyleControl, vm: &str, command: &str) -> String {
    let result = control
        .exec_await(vm, command, None, Some(60_000), None, Some(ROOT_USER))
        .await
        .expect("scratch administration");
    assert_eq!(
        result.status_code,
        Some(0),
        "scratch command failed: {}",
        result.stderr.unwrap_or_default()
    );
    result.stdout.unwrap_or_default()
}

#[tokio::test]
#[ignore = "requires an explicitly provisioned disposable NYM_MOUNT_SCRATCH_VM"]
async fn scratch_vm_nested_mount_heals_and_hydration_survives_attach() {
    let vm = std::env::var("NYM_MOUNT_SCRATCH_VM").expect("scratch VM id");
    let mut cfg = FreestyleBackendConfig {
        api_key: std::env::var("FREESTYLE_API_KEY").expect("ops key"),
        linux_user: Some("nym".into()),
        ..Default::default()
    };
    let control = FreestyleControl::new(cfg.clone()).unwrap();
    let record = control.get_sandbox(&vm).await.unwrap();
    assert!(
        record
            .slug
            .as_deref()
            .unwrap_or_default()
            .starts_with("nym-lane-b-scratch-"),
        "only the lane scratch VM is allowed"
    );
    control
        .write_file(
            &vm,
            "/tmp/lane-b-vfs.py",
            include_bytes!("freestyle_mount_fixture.py").to_vec(),
        )
        .await
        .unwrap();
    admin(
        &control,
        &vm,
        "nohup python3 /tmp/lane-b-vfs.py >/tmp/lane-b-vfs.log 2>&1 </dev/null &",
    )
    .await;
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
    let mut mounts = vec![
        mount("root", "/nym", true),
        mount("shared", "/nym/vm/mounts/shared", false),
        mount("task", "/nym/vm/mounts/task", false),
        mount("skills", "/nym/vm/mounts/skills", true),
    ];
    control
        .write_file(&vm, MOUNT_SCRIPT_PATH, mount_script(&mounts).into_bytes())
        .await
        .unwrap();
    admin(&control, &vm, &format!("sh {MOUNT_SCRIPT_PATH}")).await;
    let old = admin(&control, &vm, "cat /run/chevalier/mounts/task; umount -l /nym/vm/mounts/task; kill -0 $(cat /run/chevalier/mounts/task)").await;
    let shared_mounts: Vec<SharedMount> = mounts
        .iter()
        .map(|mount| {
            let mut template =
                ManagedMountConfig::command(&mount.mountpoint, mount.command.clone());
            template.env = mount.env.clone();
            template.read_only = Some(mount.read_only);
            cfg.shared_mounts.insert(mount.mount_tag.clone(), template);
            SharedMount {
                host_path: String::new(),
                guest_path: mount.mountpoint.clone(),
                mount_tag: mount.mount_tag.clone(),
                read_only: mount.read_only,
                availability: Default::default(),
                continuity: Default::default(),
                backend_profile: "vfs".into(),
                vfs_endpoint: String::new(),
                vfs_scope_path: String::new(),
            }
        })
        .collect();
    let sandbox = Sandbox::new(crate::SandboxConfig {
        provider: crate::SandboxProviderConfig::Freestyle(cfg),
        prewarm_on_start: false,
        ..Default::default()
    })
    .await
    .unwrap();
    let start = std::time::Instant::now();
    let session = match sandbox
        .attach_session_with_mounts(&vm, &shared_mounts)
        .await
    {
        Ok(session) => session,
        Err(error) => {
            println!("{}", admin(&control, &vm, "for p in /run/chevalier/mounts/*; do echo FILE=$p; tail -8 $p; done; findmnt | grep /nym || true").await);
            panic!("heal scratch attach: {error:?}");
        }
    };
    let elapsed = start.elapsed();
    assert!(elapsed < std::time::Duration::from_secs(45));
    let new = admin(
        &control,
        &vm,
        "cat /run/chevalier/mounts/task; mountpoint -q /nym/vm/mounts/task",
    )
    .await;
    assert_ne!(old.trim(), new.trim());
    let mut exec = session
        .exec(
            "test -d /nym/vm/mounts/shared && test -d /nym/vm/mounts/task && printf lane-b-exec-ok",
            ExecOptions::default(),
        )
        .await
        .unwrap();
    let mut output = Vec::new();
    let mut exit = None;
    while let Some(event) = exec.events.next().await {
        match event.unwrap() {
            ExecEvent::Stdout(bytes) => output.extend(bytes),
            ExecEvent::Stderr(bytes) => eprintln!("{}", String::from_utf8_lossy(&bytes)),
            ExecEvent::Exit(code) => {
                exit = Some(code);
                break;
            }
            _ => {}
        }
    }
    assert_eq!(exit, Some(0));
    assert_eq!(String::from_utf8(output).unwrap(), "lane-b-exec-ok");
    println!(
        "scratch={vm} orphan_pid={} healed_pid={} attach_ms={} exec_exit=0",
        old.trim(),
        new.trim(),
        elapsed.as_millis()
    );
    mounts.push(mount("slow", "/nym/vm/mounts/slow", true));
    control
        .write_file(&vm, MOUNT_SCRIPT_PATH, mount_script(&mounts).into_bytes())
        .await
        .unwrap();
    admin(
        &control,
        &vm,
        &format!("nohup sh {MOUNT_SCRIPT_PATH} >/tmp/lane-b-boot.log 2>&1 </dev/null &"),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let start = std::time::Instant::now();
    assert!(sandbox.attach_session(&vm).await.is_err());
    let elapsed = start.elapsed();
    assert!(elapsed < std::time::Duration::from_secs(45));
    let pid = admin(&control, &vm, "cat /run/chevalier/mounts/slow; kill -0 $(cat /run/chevalier/mounts/slow); ! grep -q ' Mounting ' /run/chevalier/mounts/slow.log").await;
    println!(
        "scratch={vm} hydrating_pid={} attach_not_ready_ms={} daemon_alive=true",
        pid.trim(),
        elapsed.as_millis()
    );
}
