// @dive-file: Pod-local iptables enforcement for guest egress hard barriers keyed to the non-root qemu UID.
// @dive-rel: Installed once at daemon startup so slirp-originated guest traffic is forced through proxy/DNS carveouts and denied elsewhere.
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::process::Command;
use std::{fs, path::Path, path::PathBuf};

use anyhow::{Context, Result, bail};
use tracing::warn;

use crate::config::Config;

pub(super) const LAN_EGRESS_MARK: u32 = 0x4000;

const FILTER_TABLE: &str = "filter";
const CHAIN_NAME: &str = "CHEVALIER_QEMU_EGRESS";
const ACCOUNT_CHAIN_NAME: &str = "CHEVALIER_QEMU_ACCOUNT";
const NAT_TABLE: &str = "nat";
const DNS_REDIRECT_CHAIN_NAME: &str = "CHEVALIER_QEMU_DNS_REDIRECT";
const PRIVATE_RANGES: [&str; 4] = [
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "100.64.0.0/10",
];
const RESOLVED_IP_BLOCK_RANGES: [&str; 10] = [
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "224.0.0.0/4",
    "240.0.0.0/4",
];
const WEBRTC_ICE_PORTS: &str = "19302,3478";
const WEBRTC_EPHEMERAL_PORT_RANGE: &str = "32768:65535";
const WEBRTC_NEW_FLOW_LIMIT: &str = "240/minute";
const WEBRTC_NEW_FLOW_BURST: &str = "48";

pub struct FirewallHandle {
    installed: bool,
    qemu_uid: u32,
}

pub struct ServiceProcessFirewallHandle {
    service: String,
    cgroup_path: String,
    cgroup_dir: PathBuf,
    dns_resolver_addr: Option<SocketAddr>,
}

pub struct PreparedServiceCgroup {
    service: String,
    cgroup_path: String,
    cgroup_dir: PathBuf,
    cgroup_procs_path: PathBuf,
}

impl FirewallHandle {
    pub fn is_installed(&self) -> bool {
        self.installed
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct VmIptablesCounterSnapshot {
    pub new_connections: u64,
    pub total_bytes: u64,
}

impl FirewallHandle {
    pub fn shutdown(self) {
        if !self.installed {
            return;
        }
        if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 {
            return;
        }
        if let Err(err) = remove_output_jump(self.qemu_uid) {
            warn!(error = %err, "failed removing qemu firewall OUTPUT jump");
        }
        if let Err(err) = remove_account_output_jump(self.qemu_uid) {
            warn!(error = %err, "failed removing qemu accounting OUTPUT jump");
        }
        if let Err(err) = remove_dns_redirect_output_jump(self.qemu_uid) {
            warn!(error = %err, "failed removing qemu DNS redirect OUTPUT jump");
        }
        if let Err(err) = flush_account_chain() {
            warn!(error = %err, "failed flushing qemu accounting chain");
        }
        if let Err(err) = flush_chain() {
            warn!(error = %err, "failed flushing qemu firewall chain");
        }
        if let Err(err) = flush_dns_redirect_chain() {
            warn!(error = %err, "failed flushing qemu DNS redirect chain");
        }
        if let Err(err) = delete_chain() {
            warn!(error = %err, "failed deleting qemu firewall chain");
        }
        if let Err(err) = delete_account_chain() {
            warn!(error = %err, "failed deleting qemu accounting chain");
        }
        if let Err(err) = delete_dns_redirect_chain() {
            warn!(error = %err, "failed deleting qemu DNS redirect chain");
        }
    }
}

pub fn install(config: &Config) -> Result<FirewallHandle> {
    if !cfg!(target_os = "linux") {
        return Ok(FirewallHandle {
            installed: false,
            qemu_uid: config.qemu_process.run_as_uid,
        });
    }
    if unsafe { libc::geteuid() } != 0 {
        warn!("skipping guest firewall install because vmd is not running as root");
        return Ok(FirewallHandle {
            installed: false,
            qemu_uid: config.qemu_process.run_as_uid,
        });
    }

    let dns_addr = guest_dns_redirect_addr(config)
        .map(|addr| addr.parse::<SocketAddr>())
        .transpose()
        .with_context(|| {
            format!(
                "parse firewall dns resolver addr {}",
                config.network_services.coredns_bind_addr
            )
        })?;
    let proxy_addr = config
        .guest_network
        .http_proxy_upstream_addr
        .as_deref()
        .unwrap_or("127.0.0.1:3128")
        .parse::<SocketAddr>()
        .with_context(|| "parse firewall proxy upstream addr".to_string())?;

    if dns_addr.is_some() {
        create_dns_redirect_chain_if_missing()?;
    }
    create_account_chain_if_missing()?;
    create_chain_if_missing()?;
    if dns_addr.is_some() {
        flush_dns_redirect_chain()?;
    }
    flush_account_chain()?;
    flush_chain()?;
    if let Some(dns_addr) = dns_addr {
        append_dns_redirect_rule(dns_addr, "udp")?;
        append_dns_redirect_rule(dns_addr, "tcp")?;
    }
    for port in ["25,465,587", "6667,6697"] {
        append_rule(&[
            "-p",
            "tcp",
            "-m",
            "multiport",
            "--dports",
            port,
            "-j",
            "DROP",
        ])?;
        append_rule(&[
            "-p",
            "udp",
            "-m",
            "multiport",
            "--dports",
            port,
            "-j",
            "DROP",
        ])?;
    }

    // Allow replies on already-established hostfwd/control-plane connections before the
    // private-range deny rules. Without this, the guest can accept an inbound probe via the
    // vmd pod's forwarded port, but the reply packets back to the calling pod IP are dropped
    // as RFC1918 egress.
    append_rule(&[
        "-m",
        "conntrack",
        "--ctstate",
        "ESTABLISHED,RELATED",
        "-j",
        "ACCEPT",
    ])?;

    for cidr in PRIVATE_RANGES {
        if allow_private_webrtc_udp(config) {
            // Local/browser watch sessions can advertise host ICE candidates on RFC1918 or
            // carrier-grade NAT space. In HA/prod mode this carveout is disabled so private
            // cluster ranges are never reachable through direct high-port UDP.
            append_rule(&[
                "-p",
                "udp",
                "-d",
                cidr,
                "--dport",
                WEBRTC_EPHEMERAL_PORT_RANGE,
                "-j",
                "ACCEPT",
            ])?;
        }
        append_rule(&["-d", cidr, "-j", "DROP"])?;
    }

    if let Some(dns_addr) = dns_addr {
        append_rule(&[
            "-p",
            "udp",
            "-d",
            &dns_addr.ip().to_string(),
            "--dport",
            &dns_addr.port().to_string(),
            "-j",
            "ACCEPT",
        ])?;
        append_rule(&[
            "-p",
            "tcp",
            "-d",
            &dns_addr.ip().to_string(),
            "--dport",
            &dns_addr.port().to_string(),
            "-j",
            "ACCEPT",
        ])?;
    } else {
        append_rule(&["-p", "udp", "--dport", "53", "-j", "ACCEPT"])?;
        append_rule(&["-p", "tcp", "--dport", "53", "-j", "ACCEPT"])?;
    }
    append_rule(&[
        "-p",
        "tcp",
        "-d",
        &proxy_addr.ip().to_string(),
        "--dport",
        &proxy_addr.port().to_string(),
        "-j",
        "ACCEPT",
    ])?;

    append_rule(&[
        "-p",
        "tcp",
        "-m",
        "multiport",
        "--dports",
        "80,443",
        "-j",
        "REJECT",
        "--reject-with",
        "tcp-reset",
    ])?;
    append_rule(&[
        "-p",
        "udp",
        "-m",
        "multiport",
        "--dports",
        "80,443",
        "-j",
        "REJECT",
        "--reject-with",
        "icmp-port-unreachable",
    ])?;
    if dns_addr.is_some() {
        append_rule(&["-p", "udp", "--dport", "53", "-j", "DROP"])?;
        append_rule(&["-p", "tcp", "--dport", "53", "-j", "DROP"])?;
    }
    // The current watch surface uses Google's public STUN service in GKE and coturn on
    // 3478 in local dev. Keep those explicit and avoid reopening arbitrary low-port UDP.
    append_rule(&[
        "-p",
        "udp",
        "-m",
        "multiport",
        "--dports",
        WEBRTC_ICE_PORTS,
        "-j",
        "ACCEPT",
    ])?;
    // WebRTC media rides public high UDP ports. Limit only *new* flow fanout here; once
    // a peer path is established, the earlier ESTABLISHED,RELATED rule carries the media
    // stream without tripping this abuse guardrail.
    append_rule(&[
        "-p",
        "udp",
        "--dport",
        WEBRTC_EPHEMERAL_PORT_RANGE,
        "-m",
        "conntrack",
        "--ctstate",
        "NEW",
        "-m",
        "hashlimit",
        "--hashlimit-above",
        WEBRTC_NEW_FLOW_LIMIT,
        "--hashlimit-burst",
        WEBRTC_NEW_FLOW_BURST,
        "--hashlimit-mode",
        "dstip,dstport",
        "--hashlimit-name",
        "chevalier-qemu-webrtc-new",
        "-j",
        "DROP",
    ])?;
    append_rule(&[
        "-p",
        "udp",
        "--dport",
        WEBRTC_EPHEMERAL_PORT_RANGE,
        "-j",
        "ACCEPT",
    ])?;
    append_rule(&["-j", "DROP"])?;

    ensure_account_output_jump(config.qemu_process.run_as_uid)?;
    if dns_addr.is_some() {
        ensure_dns_redirect_output_jump(config.qemu_process.run_as_uid)?;
    } else {
        remove_dns_redirect_output_jump(config.qemu_process.run_as_uid)?;
    }
    ensure_output_jump(config.qemu_process.run_as_uid)?;

    Ok(FirewallHandle {
        installed: true,
        qemu_uid: config.qemu_process.run_as_uid,
    })
}

pub fn qemu_firewall_ready(config: &Config) -> Result<bool> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 {
        return Ok(true);
    }
    if !chain_exists()? || !account_chain_exists()? {
        return Ok(false);
    }
    if guest_dns_redirect_addr(config).is_some() && !dns_redirect_chain_exists()? {
        return Ok(false);
    }
    if !output_jump_exists(config.qemu_process.run_as_uid)?
        || !account_output_jump_exists(config.qemu_process.run_as_uid)?
    {
        return Ok(false);
    }
    if guest_dns_redirect_addr(config).is_some()
        && !dns_redirect_output_jump_exists(config.qemu_process.run_as_uid)?
    {
        return Ok(false);
    }
    Ok(true)
}

impl ServiceProcessFirewallHandle {
    pub fn shutdown(self) {
        if let Err(err) = remove_service_private_egress_rules(
            &self.service,
            &self.cgroup_path,
            self.dns_resolver_addr,
        ) {
            warn!(
                service = %self.service,
                cgroup_path = %self.cgroup_path,
                error = %err,
                "failed removing service private-egress firewall rules"
            );
        }
        if let Err(err) = remove_service_cgroup_dir(&self.cgroup_dir) {
            warn!(
                service = %self.service,
                cgroup_path = %self.cgroup_path,
                error = %err,
                "failed removing service cgroup"
            );
        }
    }
}

impl PreparedServiceCgroup {
    pub fn cgroup_procs_path(&self) -> &Path {
        self.cgroup_procs_path.as_path()
    }

    pub fn cleanup(self) {
        if let Err(err) = remove_service_cgroup_dir(&self.cgroup_dir) {
            warn!(
                service = %self.service,
                cgroup_path = %self.cgroup_path,
                error = %err,
                "failed removing prepared service cgroup"
            );
        }
    }
}

pub fn prepare_service_process_cgroup(service: &str) -> Result<Option<PreparedServiceCgroup>> {
    prepare_process_cgroup(service, "chevalier-services", service)
}

pub fn prepare_vm_process_cgroup(vm_id: &str) -> Result<Option<PreparedServiceCgroup>> {
    prepare_process_cgroup(vm_id, "chevalier-vms", vm_id)
}

fn prepare_process_cgroup(
    label: &str,
    namespace: &str,
    component: &str,
) -> Result<Option<PreparedServiceCgroup>> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 {
        return Ok(None);
    }
    let parent = cgroup_path_for_pid_allow_root(std::process::id())
        .context("child process cgroup requires vmd cgroup path")?;
    let namespace = sanitize_cgroup_component(namespace);
    let process_component = sanitize_cgroup_component(component);
    let suffix = unique_cgroup_suffix();
    let cgroup_path = join_cgroup_paths(
        &parent,
        &format!("{namespace}/{process_component}-{suffix}"),
    );
    let cgroup_dir = cgroup_fs_path(&cgroup_path)?;
    fs::create_dir_all(&cgroup_dir)
        .with_context(|| format!("create child process cgroup {}", cgroup_dir.display()))?;
    let cgroup_procs_path = cgroup_dir.join("cgroup.procs");
    fs::metadata(&cgroup_procs_path)
        .with_context(|| format!("stat child cgroup.procs {}", cgroup_procs_path.display()))?;
    Ok(Some(PreparedServiceCgroup {
        service: label.to_string(),
        cgroup_path,
        cgroup_dir,
        cgroup_procs_path,
    }))
}

pub fn protect_service_cgroup_private_egress(
    cgroup: PreparedServiceCgroup,
    dns_resolver_addr: Option<SocketAddr>,
) -> Result<ServiceProcessFirewallHandle> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 {
        return Ok(ServiceProcessFirewallHandle {
            service: cgroup.service,
            cgroup_path: cgroup.cgroup_path,
            cgroup_dir: cgroup.cgroup_dir,
            dns_resolver_addr,
        });
    }
    if let Err(err) = install_service_private_egress_rules(
        &cgroup.service,
        &cgroup.cgroup_path,
        dns_resolver_addr,
    ) {
        cgroup.cleanup();
        return Err(err);
    }
    Ok(ServiceProcessFirewallHandle {
        service: cgroup.service,
        cgroup_path: cgroup.cgroup_path,
        cgroup_dir: cgroup.cgroup_dir,
        dns_resolver_addr,
    })
}

fn allow_private_webrtc_udp(config: &Config) -> bool {
    !config.ha_mode
}

fn guest_dns_redirect_addr(config: &Config) -> Option<String> {
    if config.guest_network.dns_server.is_some()
        || config.guest_network.http_proxy_upstream_addr.is_some()
    {
        match config
            .network_services
            .coredns_bind_addr
            .parse::<SocketAddr>()
        {
            Ok(addr) if addr.ip().is_unspecified() => Some(
                SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    addr.port(),
                )
                .to_string(),
            ),
            _ => Some(config.network_services.coredns_bind_addr.clone()),
        }
    } else {
        None
    }
}

pub fn allow_proxy_listener(listen_addr: SocketAddr) -> Result<()> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 {
        return Ok(());
    }
    let ip = listen_addr.ip().to_string();
    let port = listen_addr.port().to_string();
    let check_args = [
        "-t",
        FILTER_TABLE,
        "-C",
        CHAIN_NAME,
        "-p",
        "tcp",
        "-d",
        ip.as_str(),
        "--dport",
        port.as_str(),
        "-j",
        "ACCEPT",
    ];
    if Command::new("iptables")
        .args(check_args)
        .status()
        .context("check proxy listener allow rule")?
        .success()
    {
        return Ok(());
    }
    run_iptables([
        "-t",
        FILTER_TABLE,
        "-I",
        CHAIN_NAME,
        "1",
        "-p",
        "tcp",
        "-d",
        ip.as_str(),
        "--dport",
        port.as_str(),
        "-j",
        "ACCEPT",
    ])
    .context("install proxy listener allow rule")
}

pub fn revoke_proxy_listener(listen_addr: SocketAddr) -> Result<()> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 {
        return Ok(());
    }
    let ip = listen_addr.ip().to_string();
    let port = listen_addr.port().to_string();
    let delete_args = [
        "-t",
        FILTER_TABLE,
        "-D",
        CHAIN_NAME,
        "-p",
        "tcp",
        "-d",
        ip.as_str(),
        "--dport",
        port.as_str(),
        "-j",
        "ACCEPT",
    ];
    let _ = Command::new("iptables").args(delete_args).status();
    Ok(())
}

pub fn block_vm_process(vm_id: &str, pid: u32, cgroup_path: Option<&str>) -> Result<()> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 || pid == 0 {
        return Ok(());
    }
    let comment = vm_block_comment(vm_id);
    if let Some(cgroup_path) = cgroup_path.filter(|value| !value.trim().is_empty()) {
        let check_args = [
            "-t",
            FILTER_TABLE,
            "-C",
            CHAIN_NAME,
            "-m",
            "comment",
            "--comment",
            comment.as_str(),
            "-m",
            "cgroup",
            "--path",
            cgroup_path,
            "-j",
            "DROP",
        ];
        if Command::new("iptables")
            .args(check_args)
            .status()
            .context("check vm-specific qemu DROP cgroup rule")?
            .success()
        {
            return Ok(());
        }

        let cgroup_rule = [
            "-t",
            FILTER_TABLE,
            "-I",
            CHAIN_NAME,
            "1",
            "-m",
            "comment",
            "--comment",
            comment.as_str(),
            "-m",
            "cgroup",
            "--path",
            cgroup_path,
            "-j",
            "DROP",
        ];
        return run_iptables(cgroup_rule)
            .with_context(|| format!("install vm-specific qemu DROP rule for {vm_id}"));
    }
    warn!(
        vm_id = %vm_id,
        pid,
        "skipping vm-specific qemu DROP rule because cgroup path is unavailable"
    );
    Ok(())
}

pub fn unblock_vm_process(vm_id: &str, pid: u32, cgroup_path: Option<&str>) -> Result<()> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 || pid == 0 {
        return Ok(());
    }
    let comment = vm_block_comment(vm_id);
    if let Some(cgroup_path) = cgroup_path.filter(|value| !value.trim().is_empty()) {
        let cgroup_args = [
            "-t",
            FILTER_TABLE,
            "-D",
            CHAIN_NAME,
            "-m",
            "comment",
            "--comment",
            comment.as_str(),
            "-m",
            "cgroup",
            "--path",
            cgroup_path,
            "-j",
            "DROP",
        ];
        let _ = Command::new("iptables")
            .args(cgroup_args)
            .status()
            .context("remove vm-specific qemu DROP cgroup rule")?;
    }
    Ok(())
}

fn install_service_private_egress_rules(
    service: &str,
    cgroup_path: &str,
    dns_resolver_addr: Option<SocketAddr>,
) -> Result<()> {
    ensure_service_established_allow_rule(service, cgroup_path)?;
    if let Some(dns_addr) = dns_resolver_addr {
        for protocol in ["udp", "tcp"] {
            ensure_service_dns_allow_rule(service, cgroup_path, dns_addr, protocol)?;
        }
    }
    for cidr in PRIVATE_RANGES {
        if !Command::new("iptables")
            .args(service_lan_allow_rule_args(
                "-C",
                service,
                cgroup_path,
                cidr,
            ))
            .status()?
            .success()
        {
            run_iptables(service_lan_allow_rule_args(
                "-I",
                service,
                cgroup_path,
                cidr,
            ))?;
        }
    }
    for cidr in RESOLVED_IP_BLOCK_RANGES {
        ensure_service_private_drop_rule(service, cgroup_path, cidr)?;
    }
    Ok(())
}

fn remove_service_private_egress_rules(
    service: &str,
    cgroup_path: &str,
    dns_resolver_addr: Option<SocketAddr>,
) -> Result<()> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 {
        return Ok(());
    }
    for cidr in PRIVATE_RANGES {
        let _ = Command::new("iptables")
            .args(service_lan_allow_rule_args(
                "-D",
                service,
                cgroup_path,
                cidr,
            ))
            .status()?;
    }
    for cidr in RESOLVED_IP_BLOCK_RANGES {
        let _ = Command::new("iptables")
            .args(service_private_drop_rule_args(
                "-D",
                service,
                cgroup_path,
                cidr,
            ))
            .status()
            .with_context(|| format!("remove {service} private egress DROP rule"))?;
    }
    let _ = Command::new("iptables")
        .args(service_established_allow_rule_args(
            "-D",
            service,
            cgroup_path,
        ))
        .status()
        .with_context(|| format!("remove {service} established egress allow rule"))?;
    if let Some(dns_addr) = dns_resolver_addr {
        for protocol in ["udp", "tcp"] {
            let _ = Command::new("iptables")
                .args(service_dns_allow_rule_args(
                    "-D",
                    service,
                    cgroup_path,
                    dns_addr,
                    protocol,
                ))
                .status()
                .with_context(|| format!("remove {service} DNS allow rule"))?;
        }
    }
    Ok(())
}

fn ensure_service_established_allow_rule(service: &str, cgroup_path: &str) -> Result<()> {
    let check_args = service_established_allow_rule_args("-C", service, cgroup_path);
    if Command::new("iptables")
        .args(&check_args)
        .status()
        .with_context(|| format!("check {service} established egress allow rule"))?
        .success()
    {
        return Ok(());
    }

    // Envoy accepts proxy clients over loopback via QEMU guestfwd. Keep new
    // private connects blocked, but allow replies for already-accepted clients.
    run_iptables(service_established_allow_rule_args(
        "-I",
        service,
        cgroup_path,
    ))
    .with_context(|| format!("install {service} established egress allow rule"))
}

fn ensure_service_dns_allow_rule(
    service: &str,
    cgroup_path: &str,
    dns_addr: SocketAddr,
    protocol: &str,
) -> Result<()> {
    let check_args = service_dns_allow_rule_args("-C", service, cgroup_path, dns_addr, protocol);
    if Command::new("iptables")
        .args(&check_args)
        .status()
        .with_context(|| format!("check {service} DNS allow rule"))?
        .success()
    {
        return Ok(());
    }
    run_iptables(service_dns_allow_rule_args(
        "-A",
        service,
        cgroup_path,
        dns_addr,
        protocol,
    ))
    .with_context(|| format!("install {service} DNS allow rule"))
}

fn ensure_service_private_drop_rule(service: &str, cgroup_path: &str, cidr: &str) -> Result<()> {
    let check_args = service_private_drop_rule_args("-C", service, cgroup_path, cidr);
    if Command::new("iptables")
        .args(&check_args)
        .status()
        .with_context(|| format!("check {service} private egress DROP rule"))?
        .success()
    {
        return Ok(());
    }
    run_iptables(service_private_drop_rule_args(
        "-A",
        service,
        cgroup_path,
        cidr,
    ))
    .with_context(|| format!("install {service} private egress DROP rule"))
}

fn service_dns_allow_rule_args(
    operation: &'static str,
    service: &str,
    cgroup_path: &str,
    dns_addr: SocketAddr,
    protocol: &str,
) -> Vec<String> {
    vec![
        "-t".to_string(),
        FILTER_TABLE.to_string(),
        operation.to_string(),
        "OUTPUT".to_string(),
        "-m".to_string(),
        "comment".to_string(),
        "--comment".to_string(),
        service_private_egress_comment(service),
        "-m".to_string(),
        "cgroup".to_string(),
        "--path".to_string(),
        cgroup_path.to_string(),
        "-p".to_string(),
        protocol.to_string(),
        "-d".to_string(),
        dns_addr.ip().to_string(),
        "--dport".to_string(),
        dns_addr.port().to_string(),
        "-j".to_string(),
        "ACCEPT".to_string(),
    ]
}

fn service_established_allow_rule_args(
    operation: &'static str,
    service: &str,
    cgroup_path: &str,
) -> Vec<String> {
    vec![
        "-t".to_string(),
        FILTER_TABLE.to_string(),
        operation.to_string(),
        "OUTPUT".to_string(),
        "-m".to_string(),
        "comment".to_string(),
        "--comment".to_string(),
        service_private_egress_comment(service),
        "-m".to_string(),
        "cgroup".to_string(),
        "--path".to_string(),
        cgroup_path.to_string(),
        "-m".to_string(),
        "conntrack".to_string(),
        "--ctstate".to_string(),
        "ESTABLISHED,RELATED".to_string(),
        "-j".to_string(),
        "ACCEPT".to_string(),
    ]
}

fn service_lan_allow_rule_args(
    operation: &'static str,
    service: &str,
    cgroup_path: &str,
    cidr: &str,
) -> Vec<String> {
    let mut args = service_private_drop_rule_args(operation, service, cgroup_path, cidr);
    args.truncate(args.len() - 2);
    args.extend([
        "-m".into(),
        "mark".into(),
        "--mark".into(),
        LAN_EGRESS_MARK.to_string(),
        "-j".into(),
        "ACCEPT".into(),
    ]);
    args
}

fn service_private_drop_rule_args(
    operation: &'static str,
    service: &str,
    cgroup_path: &str,
    cidr: &str,
) -> Vec<String> {
    vec![
        "-t".to_string(),
        FILTER_TABLE.to_string(),
        operation.to_string(),
        "OUTPUT".to_string(),
        "-m".to_string(),
        "comment".to_string(),
        "--comment".to_string(),
        service_private_egress_comment(service),
        "-m".to_string(),
        "cgroup".to_string(),
        "--path".to_string(),
        cgroup_path.to_string(),
        "-d".to_string(),
        cidr.to_string(),
        "-j".to_string(),
        "DROP".to_string(),
    ]
}

pub fn install_vm_counter_rules(vm_id: &str, pid: u32, cgroup_path: Option<&str>) -> Result<()> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 || pid == 0 {
        return Ok(());
    }
    if cgroup_path
        .filter(|value| !value.trim().is_empty())
        .is_none()
    {
        warn!(
            vm_id = %vm_id,
            pid,
            "skipping vm accounting rules because cgroup path is unavailable"
        );
        return Ok(());
    }
    install_counter_rule(
        vm_connection_comment(vm_id).as_str(),
        pid,
        cgroup_path,
        true,
    )?;
    install_counter_rule(vm_bytes_comment(vm_id).as_str(), pid, cgroup_path, false)
}

pub fn remove_vm_counter_rules(vm_id: &str, pid: u32, cgroup_path: Option<&str>) -> Result<()> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 || pid == 0 {
        return Ok(());
    }
    remove_counter_rule(
        vm_connection_comment(vm_id).as_str(),
        pid,
        cgroup_path,
        true,
    )?;
    remove_counter_rule(vm_bytes_comment(vm_id).as_str(), pid, cgroup_path, false)
}

pub fn vm_counter_snapshot(vm_id: &str) -> Result<Option<VmIptablesCounterSnapshot>> {
    if !cfg!(target_os = "linux") || unsafe { libc::geteuid() } != 0 {
        return Ok(None);
    }
    let output = Command::new("iptables-save")
        .args(["-c", "-t", FILTER_TABLE])
        .output()
        .context("read iptables accounting counters")?;
    if !output.status.success() {
        bail!("iptables-save failed with status {}", output.status);
    }
    let contents = String::from_utf8_lossy(&output.stdout);
    let new_connections = parse_counter_for_comment(
        contents.as_ref(),
        ACCOUNT_CHAIN_NAME,
        &vm_connection_comment(vm_id),
    )
    .unwrap_or_default()
    .0;
    let total_bytes = parse_counter_for_comment(
        contents.as_ref(),
        ACCOUNT_CHAIN_NAME,
        &vm_bytes_comment(vm_id),
    )
    .unwrap_or_default()
    .1;
    if new_connections == 0 && total_bytes == 0 {
        return Ok(None);
    }
    Ok(Some(VmIptablesCounterSnapshot {
        new_connections,
        total_bytes,
    }))
}

pub fn cgroup_path_for_pid(pid: u32) -> Option<String> {
    if pid == 0 {
        return None;
    }
    let path = format!("/proc/{pid}/cgroup");
    parse_cgroup_path(&fs::read_to_string(path).ok()?)
}

fn cgroup_path_for_pid_allow_root(pid: u32) -> Option<String> {
    if pid == 0 {
        return None;
    }
    let path = format!("/proc/{pid}/cgroup");
    parse_cgroup_path_allow_root(&fs::read_to_string(path).ok()?)
}

fn parse_cgroup_path(contents: &str) -> Option<String> {
    parse_cgroup_path_inner(contents, false)
}

fn parse_cgroup_path_allow_root(contents: &str) -> Option<String> {
    parse_cgroup_path_inner(contents, true)
}

fn parse_cgroup_path_inner(contents: &str, allow_root: bool) -> Option<String> {
    contents
        .lines()
        .filter_map(|line| line.rsplit_once(':').map(|(_, path)| path.trim()))
        .find(|path| !path.is_empty() && (allow_root || *path != "/"))
        .map(|path| {
            if Path::new(path).is_absolute() {
                path.to_string()
            } else {
                format!("/{path}")
            }
        })
}

fn join_cgroup_paths(parent: &str, child: &str) -> String {
    let parent = parent.trim();
    let child = child.trim_matches('/');
    if parent.is_empty() || parent == "/" {
        format!("/{child}")
    } else {
        format!("{}/{}", parent.trim_end_matches('/'), child)
    }
}

fn cgroup_fs_path(cgroup_path: &str) -> Result<PathBuf> {
    let root = PathBuf::from("/sys/fs/cgroup");
    if !root.join("cgroup.controllers").exists() {
        bail!("cgroup v2 root is not available at {}", root.display());
    }
    let relative = cgroup_path.trim_start_matches('/');
    if relative.is_empty() {
        return Ok(root);
    }
    if relative
        .split('/')
        .any(|part| part == ".." || part.is_empty())
    {
        bail!("invalid cgroup path {cgroup_path}");
    }
    Ok(root.join(relative))
}

fn sanitize_cgroup_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "service".to_string()
    } else {
        sanitized.to_string()
    }
}

fn unique_cgroup_suffix() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{now}", std::process::id())
}

fn remove_service_cgroup_dir(path: &Path) -> Result<()> {
    match fs::remove_dir(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("remove service cgroup {}", path.display())),
    }
}

fn create_chain_if_missing() -> Result<()> {
    let status = Command::new("iptables")
        .args(["-t", FILTER_TABLE, "-N", CHAIN_NAME])
        .status()
        .context("create qemu firewall chain")?;
    if status.success() || chain_exists()? {
        return Ok(());
    }
    bail!("failed creating iptables chain {CHAIN_NAME}");
}

fn create_account_chain_if_missing() -> Result<()> {
    let status = Command::new("iptables")
        .args(["-t", FILTER_TABLE, "-N", ACCOUNT_CHAIN_NAME])
        .status()
        .context("create qemu accounting chain")?;
    if status.success() || account_chain_exists()? {
        return Ok(());
    }
    bail!("failed creating iptables chain {ACCOUNT_CHAIN_NAME}");
}

fn create_dns_redirect_chain_if_missing() -> Result<()> {
    let status = Command::new("iptables")
        .args(["-t", NAT_TABLE, "-N", DNS_REDIRECT_CHAIN_NAME])
        .status()
        .context("create qemu dns redirect chain")?;
    if status.success() || dns_redirect_chain_exists()? {
        return Ok(());
    }
    bail!("failed creating iptables chain {DNS_REDIRECT_CHAIN_NAME}");
}

fn flush_chain() -> Result<()> {
    run_iptables(["-t", FILTER_TABLE, "-F", CHAIN_NAME]).context("flush qemu firewall chain")
}

fn flush_account_chain() -> Result<()> {
    run_iptables(["-t", FILTER_TABLE, "-F", ACCOUNT_CHAIN_NAME])
        .context("flush qemu accounting chain")
}

fn flush_dns_redirect_chain() -> Result<()> {
    run_iptables(["-t", NAT_TABLE, "-F", DNS_REDIRECT_CHAIN_NAME])
        .context("flush qemu dns redirect chain")
}

fn delete_chain() -> Result<()> {
    run_iptables(["-t", FILTER_TABLE, "-X", CHAIN_NAME]).context("delete qemu firewall chain")
}

fn delete_account_chain() -> Result<()> {
    run_iptables(["-t", FILTER_TABLE, "-X", ACCOUNT_CHAIN_NAME])
        .context("delete qemu accounting chain")
}

fn delete_dns_redirect_chain() -> Result<()> {
    run_iptables(["-t", NAT_TABLE, "-X", DNS_REDIRECT_CHAIN_NAME])
        .context("delete qemu dns redirect chain")
}

fn chain_exists() -> Result<bool> {
    let status = Command::new("iptables")
        .args(["-t", FILTER_TABLE, "-S", CHAIN_NAME])
        .status()
        .context("inspect qemu firewall chain")?;
    Ok(status.success())
}

fn account_chain_exists() -> Result<bool> {
    let status = Command::new("iptables")
        .args(["-t", FILTER_TABLE, "-S", ACCOUNT_CHAIN_NAME])
        .status()
        .context("inspect qemu accounting chain")?;
    Ok(status.success())
}

fn dns_redirect_chain_exists() -> Result<bool> {
    let status = Command::new("iptables")
        .args(["-t", NAT_TABLE, "-S", DNS_REDIRECT_CHAIN_NAME])
        .status()
        .context("inspect qemu dns redirect chain")?;
    Ok(status.success())
}

fn append_rule(args: &[&str]) -> Result<()> {
    let mut command = vec!["-t", FILTER_TABLE, "-A", CHAIN_NAME];
    command.extend_from_slice(args);
    run_iptables(command).with_context(|| format!("append iptables rule to {CHAIN_NAME}"))
}

fn append_dns_redirect_rule(dns_addr: SocketAddr, protocol: &str) -> Result<()> {
    let dns_target = format!("{}:{}", dns_addr.ip(), dns_addr.port());
    run_iptables([
        "-t",
        NAT_TABLE,
        "-A",
        DNS_REDIRECT_CHAIN_NAME,
        "-p",
        protocol,
        "--dport",
        "53",
        "-j",
        "DNAT",
        "--to-destination",
        dns_target.as_str(),
    ])
    .with_context(|| format!("append {protocol} dns redirect rule to {DNS_REDIRECT_CHAIN_NAME}"))
}

fn ensure_output_jump(qemu_uid: u32) -> Result<()> {
    let uid = qemu_uid.to_string();
    let check_args = [
        "-t",
        FILTER_TABLE,
        "-C",
        "OUTPUT",
        "-m",
        "owner",
        "--uid-owner",
        uid.as_str(),
        "-j",
        CHAIN_NAME,
    ];
    if Command::new("iptables")
        .args(check_args)
        .status()
        .context("check qemu firewall OUTPUT jump")?
        .success()
    {
        return Ok(());
    }

    run_iptables([
        "-t",
        FILTER_TABLE,
        "-A",
        "OUTPUT",
        "-m",
        "owner",
        "--uid-owner",
        uid.as_str(),
        "-j",
        CHAIN_NAME,
    ])
    .context("install qemu firewall OUTPUT jump")
}

fn output_jump_exists(qemu_uid: u32) -> Result<bool> {
    let uid = qemu_uid.to_string();
    let status = Command::new("iptables")
        .args([
            "-t",
            FILTER_TABLE,
            "-C",
            "OUTPUT",
            "-m",
            "owner",
            "--uid-owner",
            uid.as_str(),
            "-j",
            CHAIN_NAME,
        ])
        .status()
        .context("check qemu firewall OUTPUT jump")?;
    Ok(status.success())
}

fn ensure_account_output_jump(qemu_uid: u32) -> Result<()> {
    let uid = qemu_uid.to_string();
    let check_args = [
        "-t",
        FILTER_TABLE,
        "-C",
        "OUTPUT",
        "-m",
        "owner",
        "--uid-owner",
        uid.as_str(),
        "-j",
        ACCOUNT_CHAIN_NAME,
    ];
    if Command::new("iptables")
        .args(check_args)
        .status()
        .context("check qemu accounting OUTPUT jump")?
        .success()
    {
        return Ok(());
    }

    run_iptables([
        "-t",
        FILTER_TABLE,
        "-I",
        "OUTPUT",
        "1",
        "-m",
        "owner",
        "--uid-owner",
        uid.as_str(),
        "-j",
        ACCOUNT_CHAIN_NAME,
    ])
    .context("install qemu accounting OUTPUT jump")
}

fn account_output_jump_exists(qemu_uid: u32) -> Result<bool> {
    let uid = qemu_uid.to_string();
    let status = Command::new("iptables")
        .args([
            "-t",
            FILTER_TABLE,
            "-C",
            "OUTPUT",
            "-m",
            "owner",
            "--uid-owner",
            uid.as_str(),
            "-j",
            ACCOUNT_CHAIN_NAME,
        ])
        .status()
        .context("check qemu accounting OUTPUT jump")?;
    Ok(status.success())
}

fn ensure_dns_redirect_output_jump(qemu_uid: u32) -> Result<()> {
    let uid = qemu_uid.to_string();
    let check_args = [
        "-t",
        NAT_TABLE,
        "-C",
        "OUTPUT",
        "-m",
        "owner",
        "--uid-owner",
        uid.as_str(),
        "-p",
        "udp",
        "--dport",
        "53",
        "-j",
        DNS_REDIRECT_CHAIN_NAME,
    ];
    if Command::new("iptables")
        .args(check_args)
        .status()
        .context("check qemu dns redirect OUTPUT jump")?
        .success()
    {
        return Ok(());
    }

    for protocol in ["udp", "tcp"] {
        run_iptables([
            "-t",
            NAT_TABLE,
            "-A",
            "OUTPUT",
            "-m",
            "owner",
            "--uid-owner",
            uid.as_str(),
            "-p",
            protocol,
            "--dport",
            "53",
            "-j",
            DNS_REDIRECT_CHAIN_NAME,
        ])
        .with_context(|| format!("install qemu dns redirect OUTPUT jump for {protocol}"))?;
    }
    Ok(())
}

fn dns_redirect_output_jump_exists(qemu_uid: u32) -> Result<bool> {
    let uid = qemu_uid.to_string();
    for protocol in ["udp", "tcp"] {
        let status = Command::new("iptables")
            .args([
                "-t",
                NAT_TABLE,
                "-C",
                "OUTPUT",
                "-m",
                "owner",
                "--uid-owner",
                uid.as_str(),
                "-p",
                protocol,
                "--dport",
                "53",
                "-j",
                DNS_REDIRECT_CHAIN_NAME,
            ])
            .status()
            .with_context(|| format!("check qemu dns redirect OUTPUT jump for {protocol}"))?;
        if !status.success() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn remove_output_jump(qemu_uid: u32) -> Result<()> {
    let uid = qemu_uid.to_string();
    let args = [
        "-t",
        FILTER_TABLE,
        "-D",
        "OUTPUT",
        "-m",
        "owner",
        "--uid-owner",
        uid.as_str(),
        "-j",
        CHAIN_NAME,
    ];
    if !Command::new("iptables")
        .args(args)
        .status()
        .context("remove qemu firewall OUTPUT jump")?
        .success()
    {
        warn!("qemu firewall OUTPUT jump was already absent");
    }
    Ok(())
}

fn remove_account_output_jump(qemu_uid: u32) -> Result<()> {
    let uid = qemu_uid.to_string();
    let args = [
        "-t",
        FILTER_TABLE,
        "-D",
        "OUTPUT",
        "-m",
        "owner",
        "--uid-owner",
        uid.as_str(),
        "-j",
        ACCOUNT_CHAIN_NAME,
    ];
    let _ = Command::new("iptables")
        .args(args)
        .status()
        .context("remove qemu accounting OUTPUT jump")?;
    Ok(())
}

fn remove_dns_redirect_output_jump(qemu_uid: u32) -> Result<()> {
    let uid = qemu_uid.to_string();
    for protocol in ["udp", "tcp"] {
        let args = [
            "-t",
            NAT_TABLE,
            "-D",
            "OUTPUT",
            "-m",
            "owner",
            "--uid-owner",
            uid.as_str(),
            "-p",
            protocol,
            "--dport",
            "53",
            "-j",
            DNS_REDIRECT_CHAIN_NAME,
        ];
        let _ = Command::new("iptables")
            .args(args)
            .status()
            .context("remove qemu dns redirect OUTPUT jump")?;
    }
    Ok(())
}

fn run_iptables<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let status = Command::new("iptables")
        .args(args)
        .status()
        .context("run iptables command")?;
    if !status.success() {
        bail!("iptables command failed with status {status}");
    }
    Ok(())
}

fn vm_block_comment(vm_id: &str) -> String {
    format!("chevalier-vm-block:{vm_id}")
}

fn vm_connection_comment(vm_id: &str) -> String {
    format!("chevalier-vm-conn:{vm_id}")
}

fn vm_bytes_comment(vm_id: &str) -> String {
    format!("chevalier-vm-bytes:{vm_id}")
}

fn service_private_egress_comment(service: &str) -> String {
    format!("chevalier-service-private-egress:{service}")
}

fn install_counter_rule(
    comment: &str,
    pid: u32,
    cgroup_path: Option<&str>,
    conntrack_new_only: bool,
) -> Result<()> {
    if let Some(cgroup_path) = cgroup_path.filter(|value| !value.trim().is_empty()) {
        let mut cgroup_check = vec![
            "-t",
            FILTER_TABLE,
            "-C",
            ACCOUNT_CHAIN_NAME,
            "-m",
            "comment",
            "--comment",
            comment,
            "-m",
            "cgroup",
            "--path",
            cgroup_path,
        ];
        if conntrack_new_only {
            cgroup_check.extend(["-m", "conntrack", "--ctstate", "NEW"]);
        }
        cgroup_check.extend(["-j", "RETURN"]);
        if Command::new("iptables")
            .args(&cgroup_check)
            .status()
            .context("check vm accounting cgroup rule")?
            .success()
        {
            return Ok(());
        }

        let mut cgroup_install = vec![
            "-t",
            FILTER_TABLE,
            "-I",
            ACCOUNT_CHAIN_NAME,
            "1",
            "-m",
            "comment",
            "--comment",
            comment,
            "-m",
            "cgroup",
            "--path",
            cgroup_path,
        ];
        if conntrack_new_only {
            cgroup_install.extend(["-m", "conntrack", "--ctstate", "NEW"]);
        }
        cgroup_install.extend(["-j", "RETURN"]);
        return run_iptables(cgroup_install);
    }

    warn!(
        comment,
        pid, "skipping vm accounting rule because cgroup path is unavailable"
    );
    Ok(())
}

fn remove_counter_rule(
    comment: &str,
    _pid: u32,
    cgroup_path: Option<&str>,
    conntrack_new_only: bool,
) -> Result<()> {
    if let Some(cgroup_path) = cgroup_path.filter(|value| !value.trim().is_empty()) {
        let mut cgroup_rule = vec![
            "-t",
            FILTER_TABLE,
            "-D",
            ACCOUNT_CHAIN_NAME,
            "-m",
            "comment",
            "--comment",
            comment,
            "-m",
            "cgroup",
            "--path",
            cgroup_path,
        ];
        if conntrack_new_only {
            cgroup_rule.extend(["-m", "conntrack", "--ctstate", "NEW"]);
        }
        cgroup_rule.extend(["-j", "RETURN"]);
        let _ = Command::new("iptables")
            .args(&cgroup_rule)
            .status()
            .context("remove vm accounting cgroup rule")?;
        return Ok(());
    }

    Ok(())
}

fn parse_counter_for_comment(
    contents: &str,
    chain_name: &str,
    comment: &str,
) -> Option<(u64, u64)> {
    for line in contents.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('[') || !trimmed.contains(chain_name) || !trimmed.contains(comment)
        {
            continue;
        }
        let end = trimmed.find(']')?;
        let counters = &trimmed[1..end];
        let (packets, bytes) = counters.split_once(':')?;
        return Some((packets.parse().ok()?, bytes.parse().ok()?));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, GuestNetworkConfig, QemuProcessConfig};
    use std::net::IpAddr;

    #[test]
    fn install_uses_qemu_uid_and_local_proxy_dns_endpoints() {
        let mut cfg = Config {
            qemu_process: QemuProcessConfig {
                run_as_uid: 1000,
                run_as_gid: 1000,
            },
            ..Config::default()
        };
        cfg.network_services.coredns_bind_addr = "127.0.0.53:53".to_string();
        cfg.guest_network.http_proxy_upstream_addr = Some("127.0.0.1:3128".to_string());

        let dns_addr = cfg
            .network_services
            .coredns_bind_addr
            .parse::<SocketAddr>()
            .expect("dns addr");
        let proxy_addr = cfg
            .guest_network
            .http_proxy_upstream_addr
            .as_deref()
            .expect("proxy addr")
            .parse::<SocketAddr>()
            .expect("proxy addr parse");

        assert_eq!(dns_addr.ip(), IpAddr::from([127, 0, 0, 53]));
        assert_eq!(dns_addr.port(), 53);
        assert_eq!(proxy_addr.ip(), IpAddr::from([127, 0, 0, 1]));
        assert_eq!(proxy_addr.port(), 3128);
        assert_eq!(cfg.qemu_process.run_as_uid, 1000);
    }

    #[test]
    fn dns_redirect_uses_nat_chain_and_local_coredns_target() {
        let dns_addr: SocketAddr = "127.0.0.53:53".parse().expect("dns addr");
        let dns_target = format!("{}:{}", dns_addr.ip(), dns_addr.port());
        assert_eq!(NAT_TABLE, "nat");
        assert_eq!(DNS_REDIRECT_CHAIN_NAME, "CHEVALIER_QEMU_DNS_REDIRECT");
        assert_eq!(dns_target, "127.0.0.53:53");
    }

    #[test]
    fn guest_dns_redirect_addr_requires_guest_dns_or_proxy_configuration() {
        let mut cfg = Config::default();
        cfg.network_services.coredns_bind_addr = "127.0.0.53:53".to_string();
        cfg.guest_network = GuestNetworkConfig::default();
        assert_eq!(guest_dns_redirect_addr(&cfg), None);

        cfg.guest_network.dns_server = Some("10.0.2.3".to_string());
        assert_eq!(
            guest_dns_redirect_addr(&cfg).as_deref(),
            Some("127.0.0.53:53")
        );

        cfg.guest_network = GuestNetworkConfig::default();
        cfg.guest_network.http_proxy_upstream_addr = Some("127.0.0.1:3128".to_string());
        assert_eq!(
            guest_dns_redirect_addr(&cfg).as_deref(),
            Some("127.0.0.53:53")
        );

        cfg.network_services.coredns_bind_addr = "0.0.0.0:15053".to_string();
        assert_eq!(
            guest_dns_redirect_addr(&cfg).as_deref(),
            Some("127.0.0.1:15053")
        );
    }

    #[test]
    fn private_webrtc_udp_carveout_is_disabled_in_ha_mode() {
        let mut cfg = Config::default();
        assert!(allow_private_webrtc_udp(&cfg));

        cfg.ha_mode = true;
        assert!(!allow_private_webrtc_udp(&cfg));
    }

    #[test]
    fn service_private_egress_blocks_loopback_and_link_local_ranges() {
        assert!(RESOLVED_IP_BLOCK_RANGES.contains(&"127.0.0.0/8"));
        assert!(RESOLVED_IP_BLOCK_RANGES.contains(&"169.254.0.0/16"));
        assert!(RESOLVED_IP_BLOCK_RANGES.contains(&"10.0.0.0/8"));
    }

    #[test]
    fn service_private_egress_rule_targets_service_cgroup() {
        let cgroup_path = "/kubepods.slice/pod123/chevalier-services/envoy-1234";
        let args = service_private_drop_rule_args("-A", "envoy", cgroup_path, "10.0.0.0/8");
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "--path" && pair[1] == cgroup_path)
        );
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "-m" && pair[1] == "cgroup")
        );
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "-d" && pair[1] == "10.0.0.0/8")
        );
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "-j" && pair[1] == "DROP")
        );
        assert!(
            args.iter()
                .any(|arg| arg == "chevalier-service-private-egress:envoy")
        );
    }

    #[test]
    fn service_established_allow_rule_preserves_loopback_replies_only() {
        let cgroup_path = "/kubepods.slice/pod123/chevalier-services/envoy-1234";
        let args = service_established_allow_rule_args("-I", "envoy", cgroup_path);

        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "--path" && pair[1] == cgroup_path)
        );
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "-m" && pair[1] == "conntrack")
        );
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "--ctstate" && pair[1] == "ESTABLISHED,RELATED")
        );
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "-j" && pair[1] == "ACCEPT")
        );
        assert!(!args.windows(2).any(|pair| pair[0] == "-d"));
    }

    #[test]
    fn parse_cgroup_path_prefers_non_root_entry() {
        let parsed = parse_cgroup_path("0::/\n1:name=systemd:/kubepods.slice/pod123/vm.scope\n");
        assert_eq!(parsed.as_deref(), Some("/kubepods.slice/pod123/vm.scope"));
    }

    #[test]
    fn parse_counter_for_comment_extracts_packets_and_bytes() {
        let contents = r#"
[12:3456] -A CHEVALIER_QEMU_ACCOUNT -m comment --comment "chevalier-vm-conn:vm-1" -m cgroup --path /kubepods.slice/pod123/vm.scope -m conntrack --ctstate NEW -j RETURN
[34:5678] -A CHEVALIER_QEMU_ACCOUNT -m comment --comment "chevalier-vm-bytes:vm-1" -m cgroup --path /kubepods.slice/pod123/vm.scope -j RETURN
"#;
        assert_eq!(
            parse_counter_for_comment(contents, ACCOUNT_CHAIN_NAME, "chevalier-vm-conn:vm-1"),
            Some((12, 3456))
        );
        assert_eq!(
            parse_counter_for_comment(contents, ACCOUNT_CHAIN_NAME, "chevalier-vm-bytes:vm-1"),
            Some((34, 5678))
        );
    }
    #[test]
    fn lan_exception_requires_both_service_identity_and_opt_in_mark() {
        let args = service_lan_allow_rule_args("-I", "envoy", "services/envoy", "192.168.0.0/16");
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--path", "services/envoy"])
        );
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--mark", LAN_EGRESS_MARK.to_string().as_str()])
        );
        assert!(args.windows(2).any(|pair| pair == ["-d", "192.168.0.0/16"]));
        assert!(args.ends_with(&["-j".into(), "ACCEPT".into()]));
        assert_eq!(LAN_EGRESS_MARK & 1, 0);
    }
}
