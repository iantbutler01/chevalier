//! The gated scratch-VM proof: a real Freestyle guest, the snapshot's own
//! `chevalier-vfs-fuse` daemons, synthetic VFS data. Runs only against an explicitly
//! provisioned disposable VM whose slug marks it as the lane's scratch VM.
use super::*;

async fn admin(control: &FreestyleControl, vm: &str, command: &str) -> String {
    let result = control
        .exec_await(vm, command, None, Some(60_000), None, Some(ROOT_USER))
        .await
        .expect("scratch administration");
    assert_eq!(
        result.status_code,
        Some(0),
        "scratch command failed: {command}\n{}",
        result.stderr.unwrap_or_default()
    );
    result.stdout.unwrap_or_default().trim().to_string()
}

/// Boot id, every marker pid, and which mounts are active: what a restart or a heal changes.
const GUEST_STATE: &str = "cat /proc/sys/kernel/random/boot_id; for t in root shared task skills; do printf '%s=%s ' $t $(cat /run/chevalier/mounts/$t); done; echo; for p in /nym /nym/vm/mounts/shared /nym/vm/mounts/task /nym/vm/mounts/skills; do grep -q \" $p \" /proc/self/mountinfo && printf '%s:mounted ' $p || printf '%s:absent ' $p; done";

async fn guest_state(control: &FreestyleControl, vm: &str) -> String {
    admin(control, vm, GUEST_STATE).await
}

async fn exec_as_session_user(session: &Session, command: &str) -> (Option<i32>, String) {
    let mut exec = session.exec(command, ExecOptions::default()).await.unwrap();
    let mut output = Vec::new();
    let mut exit = None;
    while let Some(event) = exec.events.next().await {
        match event.unwrap() {
            ExecEvent::Stdout(bytes) | ExecEvent::Stderr(bytes) => output.extend(bytes),
            ExecEvent::Exit(code) => {
                exit = Some(code);
                break;
            }
            _ => {}
        }
    }
    (exit, String::from_utf8_lossy(&output).into_owned())
}

#[tokio::test]
#[ignore = "requires an explicitly provisioned disposable NYM_MOUNT_SCRATCH_VM"]
async fn scratch_vm_deployed_guest_updates_in_place_and_self_heals() {
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
        "nohup python3 /tmp/lane-b-vfs.py >/tmp/lane-b-vfs.log 2>&1 </dev/null & sleep 1",
    )
    .await;
    println!(
        "vfs_binary_sha256={}",
        admin(&control, &vm, "sha256sum /usr/local/bin/chevalier-vfs-fuse").await
    );
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

    // 1. A guest as production has it: the deployed script, booted.
    let deployed = GuestMountScript::Legacy.render(&mounts);
    control
        .write_file(&vm, MOUNT_SCRIPT_PATH, deployed.clone().into_bytes())
        .await
        .unwrap();
    admin(&control, &vm, &format!("sh {MOUNT_SCRIPT_PATH}")).await;
    println!(
        "fuse_mounts_before={}",
        admin(&control, &vm, "grep fuse /proc/mounts | grep -v fusectl").await
    );
    // Chirchester's state: the task mount detached, its daemon alive.
    let orphan = admin(&control, &vm, "cat /run/chevalier/mounts/task; umount -l /nym/vm/mounts/task; kill -0 $(cat /run/chevalier/mounts/task)").await;
    let before = guest_state(&control, &vm).await;
    println!("deployed_guest_with_orphan:\n{before}");

    // 2. The first attach after deploy: same mounts, so the new script goes in in place.
    let start = std::time::Instant::now();
    let session = sandbox
        .attach_session_with_mounts(&vm, &shared_mounts)
        .await
        .expect("update-in-place attach");
    let update_ms = start.elapsed().as_millis();
    let after = guest_state(&control, &vm).await;
    println!("after_update_attach ({update_ms} ms):\n{after}");
    let installed = admin(&control, &vm, &format!("cat {MOUNT_SCRIPT_PATH}")).await;
    assert_eq!(
        installed,
        GuestMountScript::SelfHealing.render(&mounts).trim()
    );
    let boot = |state: &str| state.lines().next().unwrap().to_string();
    assert_eq!(boot(&before), boot(&after), "the guest restarted");
    assert!(!after.contains(&format!("task={} ", orphan)));
    assert!(after.contains("/nym/vm/mounts/task:mounted"));
    for tag in ["root", "shared", "skills"] {
        let pid = |state: &str| {
            state
                .split_whitespace()
                .find(|word| word.starts_with(&format!("{tag}=")))
                .unwrap()
                .to_string()
        };
        assert_eq!(pid(&before), pid(&after), "{tag} daemon was replaced");
    }
    println!(
        "fuse_mounts_after_heal={}",
        admin(&control, &vm, "grep fuse /proc/mounts | grep -v fusectl").await
    );
    let (exit, output) = exec_as_session_user(
        &session,
        "id -un; ls /nym/vm/mounts 2>&1; test -d /nym/vm/mounts/task && echo task-dir-ok",
    )
    .await;
    println!("exec_as_nym exit={exit:?} output={output:?}");
    assert!(exit.is_some(), "exec as the session user did not complete");

    // 3. The next attach: orphan the task mount again; the installed script heals it.
    let orphan = admin(&control, &vm, "cat /run/chevalier/mounts/task; umount -l /nym/vm/mounts/task; kill -0 $(cat /run/chevalier/mounts/task)").await;
    let start = std::time::Instant::now();
    sandbox
        .attach_session_with_mounts(&vm, &shared_mounts)
        .await
        .expect("self-heal attach");
    let heal_ms = start.elapsed().as_millis();
    let healed = guest_state(&control, &vm).await;
    println!("after_heal_attach ({heal_ms} ms) orphan={orphan}:\n{healed}");
    assert_eq!(boot(&before), boot(&healed));
    assert!(!healed.contains(&format!("task={} ", orphan)));
    assert!(healed.contains("/nym/vm/mounts/task:mounted"));
    assert!(heal_ms < 45_000);

    // 4. A FUSE root whose daemon does not answer (stopped): a read of /nym blocks, but
    // the attach reads only the kernel's mount table, so it neither waits nor kills.
    let root_pid = admin(&control, &vm, "cat /run/chevalier/mounts/root").await;
    admin(&control, &vm, &format!("kill -STOP {root_pid}")).await;
    let probe = admin(
        &control,
        &vm,
        "sleep 2; timeout 2 ls /nym >/dev/null 2>&1; echo ls_status=$?",
    )
    .await;
    assert_eq!(probe, "ls_status=124", "the stopped root still answered");
    let start = std::time::Instant::now();
    let hung_attach = sandbox
        .attach_session_with_mounts(&vm, &shared_mounts)
        .await;
    let hung_ms = start.elapsed().as_millis();
    admin(&control, &vm, &format!("kill -CONT {root_pid}")).await;
    let after_hung = guest_state(&control, &vm).await;
    println!(
        "hung_root {probe} attach_ok={} ({hung_ms} ms):\n{after_hung}",
        hung_attach.is_ok()
    );
    assert!(hung_attach.is_ok(), "{:?}", hung_attach.err());
    assert!(after_hung.contains(&format!("root={root_pid} ")));

    // 5. The switch off: back to the deployed script, in place.
    sandbox.set_mount_self_heal(false);
    sandbox
        .attach_session_with_mounts(&vm, &shared_mounts)
        .await
        .expect("switch-off attach");
    let switched = guest_state(&control, &vm).await;
    println!("after_switch_off_attach:\n{switched}");
    assert_eq!(
        admin(&control, &vm, &format!("cat {MOUNT_SCRIPT_PATH}")).await,
        deployed.trim()
    );
    assert_eq!(boot(&before), boot(&switched));
    sandbox.set_mount_self_heal(true);

    // 6. A mount still hydrating when an attach comes: not ready, daemon kept.
    mounts.push(mount("slow", "/nym/vm/mounts/slow", true));
    control
        .write_file(&vm, MOUNT_SCRIPT_PATH, mount_script(&mounts).into_bytes())
        .await
        .unwrap();
    admin(
        &control,
        &vm,
        &format!("nohup sh {MOUNT_SCRIPT_PATH} >/tmp/lane-b-boot.log 2>&1 </dev/null & sleep 1"),
    )
    .await;
    let start = std::time::Instant::now();
    assert!(sandbox.attach_session(&vm).await.is_err());
    let elapsed = start.elapsed();
    assert!(elapsed < std::time::Duration::from_secs(45));
    let pid = admin(&control, &vm, "cat /run/chevalier/mounts/slow; kill -0 $(cat /run/chevalier/mounts/slow); ! grep -q ' Mounting ' /run/chevalier/mounts/slow.log").await;
    println!(
        "scratch={vm} hydrating_pid={pid} attach_not_ready_ms={} daemon_alive=true",
        elapsed.as_millis()
    );
}
