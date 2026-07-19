// @dive-file: Host-owned tap networking for VM egress capture.
// @dive-rel: Replaces QEMU user-mode guestfwd/hostfwd with a tap device, TPROXY capture, and vmd-owned control forwarders.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::config::Config;

pub const POLICY_ENVOY_PORT: u16 = 15001;
pub const POLICY_DNS_PORT: u16 = 15053;
const TPROXY_MARK_VALUE: &str = "0x1";
const TPROXY_MARK_MASKED: &str = "0x1/0x1";
const TPROXY_ROUTE_TABLE: &str = "100";
const GUEST_PROXY_PORT: u16 = 13337;
const GUEST_RPC_PORT: u16 = 13338;
const TAP_FORWARD_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct VmTapNetworkSpec {
    pub vm_id: String,
    pub tap_name: String,
    pub guest_ip: Ipv4Addr,
    pub gateway_ip: Ipv4Addr,
    pub prefix_len: u8,
    pub proxy_listen: SocketAddr,
    pub rpc_listen: SocketAddr,
    pub proxy_target: SocketAddr,
    pub rpc_target: SocketAddr,
    pub envoy_port: u16,
}

#[derive(Clone, Debug)]
pub struct VmTapAddressing {
    pub tap_name: String,
    pub guest_ip: Ipv4Addr,
    pub gateway_ip: Ipv4Addr,
    pub prefix_len: u8,
}

impl VmTapNetworkSpec {
    pub fn guest_cidr(&self) -> String {
        format!("{}/{}", self.guest_ip, self.prefix_len)
    }
}

#[derive(Clone, Debug)]
pub struct VmTapNetworkHandle {
    spec: VmTapNetworkSpec,
    stops: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
}

impl VmTapNetworkHandle {
    pub async fn shutdown(&self) {
        let stops = {
            let mut guard = self.stops.lock().unwrap_or_else(|error| error.into_inner());
            std::mem::take(&mut *guard)
        };
        for stop in stops {
            let _ = stop.send(());
        }
        let spec = self.spec.clone();
        let vm_id = spec.vm_id.clone();
        let tap = spec.tap_name.clone();
        match tokio::task::spawn_blocking(move || cleanup_vm_tap_network(&spec)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                warn!(
                    vm_id = %vm_id,
                    tap = %tap,
                    error = %err,
                    "failed cleaning up tap network"
                );
            }
            Err(err) => {
                warn!(
                    vm_id = %vm_id,
                    tap = %tap,
                    error = %err,
                    "tap network cleanup worker failed"
                );
            }
        }
    }
}

pub fn spec_for_vm(
    vm_id: &str,
    bind_addr: &str,
    proxy_port: i32,
    rpc_port: i32,
    envoy_port: u16,
) -> Result<VmTapNetworkSpec> {
    if proxy_port <= 0 {
        bail!("tap network requires a positive proxy port");
    }
    if rpc_port <= 0 {
        bail!("tap network requires a positive rpc port");
    }

    let addressing = addressing_for_vm(vm_id);
    let bind_ip = parse_bind_ip(bind_addr)?;

    Ok(VmTapNetworkSpec {
        vm_id: vm_id.to_string(),
        tap_name: addressing.tap_name.clone(),
        guest_ip: addressing.guest_ip,
        gateway_ip: addressing.gateway_ip,
        prefix_len: addressing.prefix_len,
        proxy_listen: SocketAddr::new(bind_ip, proxy_port as u16),
        rpc_listen: SocketAddr::new(bind_ip, rpc_port as u16),
        proxy_target: SocketAddr::new(IpAddr::V4(addressing.guest_ip), GUEST_PROXY_PORT),
        rpc_target: SocketAddr::new(IpAddr::V4(addressing.guest_ip), GUEST_RPC_PORT),
        envoy_port,
    })
}

pub fn addressing_for_vm(vm_id: &str) -> VmTapAddressing {
    let hash = Sha256::digest(vm_id.as_bytes());
    let suffix = hash[..4]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let tap_name = format!("tap{}", &suffix[..8]);

    // 198.18.0.0/15 is reserved for benchmarking and avoids colliding with common
    // RFC1918/Kubernetes service ranges. Each VM gets one deterministic /30.
    let raw = u32::from_be_bytes([hash[4], hash[5], hash[6], hash[7]]) % 32_768;
    let base = (198_u32 << 24) | (18_u32 << 16) | raw.saturating_mul(4);
    VmTapAddressing {
        tap_name,
        gateway_ip: Ipv4Addr::from(base + 1),
        guest_ip: Ipv4Addr::from(base + 2),
        prefix_len: 30,
    }
}

pub async fn start_vm_tap_network(
    cfg: &Config,
    spec: &VmTapNetworkSpec,
) -> Result<VmTapNetworkHandle> {
    if !cfg!(target_os = "linux") {
        bail!("tap policy networking requires a linux host");
    }
    if unsafe { libc::geteuid() } != 0 {
        bail!("tap policy networking requires root privileges");
    }

    ensure_global_tproxy_route()?;
    cleanup_vm_tap_network(spec)?;
    create_tap(cfg, spec)?;
    if let Err(err) = install_capture_rules(spec) {
        let _ = cleanup_vm_tap_network(spec);
        return Err(err);
    }

    let stops = Arc::new(Mutex::new(Vec::new()));
    if let Err(err) = start_forwarder(
        spec.vm_id.as_str(),
        "portproxy-tcp",
        spec.proxy_listen,
        spec.proxy_target,
        Arc::clone(&stops),
    )
    .await
    {
        let handle = VmTapNetworkHandle {
            spec: spec.clone(),
            stops,
        };
        handle.shutdown().await;
        return Err(err);
    }
    if let Err(err) = start_forwarder(
        spec.vm_id.as_str(),
        "portproxy-rpc",
        spec.rpc_listen,
        spec.rpc_target,
        Arc::clone(&stops),
    )
    .await
    {
        let handle = VmTapNetworkHandle {
            spec: spec.clone(),
            stops,
        };
        handle.shutdown().await;
        return Err(err);
    }

    Ok(VmTapNetworkHandle {
        spec: spec.clone(),
        stops,
    })
}

fn create_tap(cfg: &Config, spec: &VmTapNetworkSpec) -> Result<()> {
    let uid = cfg.qemu_process.run_as_uid.to_string();
    let gid = cfg.qemu_process.run_as_gid.to_string();
    run_cmd(
        "ip",
        &[
            "tuntap",
            "add",
            "dev",
            spec.tap_name.as_str(),
            "mode",
            "tap",
            "user",
            uid.as_str(),
            "group",
            gid.as_str(),
        ],
    )
    .with_context(|| format!("create tap {}", spec.tap_name))?;
    run_cmd(
        "ip",
        &[
            "addr",
            "replace",
            format!("{}/{}", spec.gateway_ip, spec.prefix_len).as_str(),
            "dev",
            spec.tap_name.as_str(),
        ],
    )
    .with_context(|| format!("assign tap address {}", spec.tap_name))?;
    run_cmd("ip", &["link", "set", "dev", spec.tap_name.as_str(), "up"])
        .with_context(|| format!("bring tap {} up", spec.tap_name))
}

fn ensure_global_tproxy_route() -> Result<()> {
    let _ = run_cmd(
        "ip",
        &[
            "rule",
            "add",
            "fwmark",
            TPROXY_MARK_VALUE,
            "lookup",
            TPROXY_ROUTE_TABLE,
        ],
    );
    run_cmd(
        "ip",
        &[
            "route",
            "replace",
            "local",
            "0.0.0.0/0",
            "dev",
            "lo",
            "table",
            TPROXY_ROUTE_TABLE,
        ],
    )
    .context("install tproxy local route")
}

fn install_capture_rules(spec: &VmTapNetworkSpec) -> Result<()> {
    let tap = spec.tap_name.as_str();
    let gateway_ip = spec.gateway_ip.to_string();
    let envoy_port = spec.envoy_port.to_string();
    let dns_port = POLICY_DNS_PORT.to_string();

    iptables(
        &[
            "-t",
            "mangle",
            "-A",
            "PREROUTING",
            "-i",
            tap,
            "-p",
            "tcp",
            "-d",
            gateway_ip.as_str(),
            "-j",
            "RETURN",
        ],
        "bypass tproxy for host-bound tap control traffic",
    )?;
    iptables(
        &[
            "-t",
            "mangle",
            "-A",
            "PREROUTING",
            "-i",
            tap,
            "-p",
            "tcp",
            "-m",
            "socket",
            "--transparent",
            "-j",
            "MARK",
            "--set-mark",
            TPROXY_MARK_VALUE,
        ],
        "mark packets owned by transparent sockets",
    )?;
    iptables(
        &[
            "-t",
            "mangle",
            "-A",
            "PREROUTING",
            "-i",
            tap,
            "-p",
            "tcp",
            "-m",
            "socket",
            "--transparent",
            "-j",
            "ACCEPT",
        ],
        "divert packets owned by transparent sockets",
    )?;
    iptables(
        &[
            "-t",
            "mangle",
            "-A",
            "PREROUTING",
            "-i",
            tap,
            "-p",
            "tcp",
            "--dport",
            "53",
            "-j",
            "RETURN",
        ],
        "allow tcp dns redirect before tproxy",
    )?;
    iptables(
        &[
            "-t",
            "mangle",
            "-A",
            "PREROUTING",
            "-i",
            tap,
            "-p",
            "tcp",
            "-j",
            "TPROXY",
            "--on-port",
            envoy_port.as_str(),
            "--tproxy-mark",
            TPROXY_MARK_MASKED,
        ],
        "capture tap tcp with tproxy",
    )?;
    for protocol in ["udp", "tcp"] {
        iptables(
            &[
                "-t",
                "nat",
                "-A",
                "PREROUTING",
                "-i",
                tap,
                "-p",
                protocol,
                "--dport",
                "53",
                "-j",
                "REDIRECT",
                "--to-ports",
                dns_port.as_str(),
            ],
            "redirect tap dns",
        )?;
    }
    iptables(
        &[
            "-A",
            "INPUT",
            "-i",
            tap,
            "-m",
            "conntrack",
            "--ctstate",
            "ESTABLISHED,RELATED",
            "-j",
            "ACCEPT",
        ],
        "allow established tap input",
    )?;
    iptables(
        &[
            "-A",
            "INPUT",
            "-i",
            tap,
            "-p",
            "tcp",
            "-m",
            "mark",
            "--mark",
            TPROXY_MARK_MASKED,
            "-j",
            "ACCEPT",
        ],
        "allow tproxy-marked tap tcp",
    )?;
    for protocol in ["udp", "tcp"] {
        iptables(
            &[
                "-A",
                "INPUT",
                "-i",
                tap,
                "-p",
                protocol,
                "--dport",
                dns_port.as_str(),
                "-j",
                "ACCEPT",
            ],
            "allow redirected tap dns",
        )?;
    }
    iptables(
        &["-A", "INPUT", "-i", tap, "-j", "DROP"],
        "drop uncaptured tap input",
    )?;
    iptables(
        &["-A", "FORWARD", "-i", tap, "-j", "DROP"],
        "drop tap forwarding",
    )?;

    let _ = run_cmd("ip6tables", &["-A", "INPUT", "-i", tap, "-j", "DROP"]);
    let _ = run_cmd("ip6tables", &["-A", "FORWARD", "-i", tap, "-j", "DROP"]);
    Ok(())
}

fn cleanup_vm_tap_network(spec: &VmTapNetworkSpec) -> Result<()> {
    let tap = spec.tap_name.as_str();
    let gateway_ip = spec.gateway_ip.to_string();
    let envoy_port = spec.envoy_port.to_string();
    let dns_port = POLICY_DNS_PORT.to_string();

    delete_iptables_repeated(&[
        "-t",
        "mangle",
        "-D",
        "PREROUTING",
        "-i",
        tap,
        "-p",
        "tcp",
        "-d",
        gateway_ip.as_str(),
        "-j",
        "RETURN",
    ]);
    delete_iptables_repeated(&[
        "-t",
        "mangle",
        "-D",
        "PREROUTING",
        "-i",
        tap,
        "-p",
        "tcp",
        "-m",
        "socket",
        "--transparent",
        "-j",
        "MARK",
        "--set-mark",
        TPROXY_MARK_VALUE,
    ]);
    delete_iptables_repeated(&[
        "-t",
        "mangle",
        "-D",
        "PREROUTING",
        "-i",
        tap,
        "-p",
        "tcp",
        "-m",
        "socket",
        "--transparent",
        "-j",
        "ACCEPT",
    ]);
    // Remove the pre-divert rule shipped in the first tap rollout. Returning
    // established packets here lets the SYN hit Envoy but strands payload data.
    delete_iptables_repeated(&[
        "-t",
        "mangle",
        "-D",
        "PREROUTING",
        "-i",
        tap,
        "-m",
        "conntrack",
        "--ctstate",
        "ESTABLISHED,RELATED",
        "-j",
        "RETURN",
    ]);
    delete_iptables_repeated(&[
        "-t",
        "mangle",
        "-D",
        "PREROUTING",
        "-i",
        tap,
        "-p",
        "tcp",
        "--dport",
        "53",
        "-j",
        "RETURN",
    ]);
    delete_iptables_repeated(&[
        "-t",
        "mangle",
        "-D",
        "PREROUTING",
        "-i",
        tap,
        "-p",
        "tcp",
        "-j",
        "TPROXY",
        "--on-port",
        envoy_port.as_str(),
        "--tproxy-mark",
        TPROXY_MARK_MASKED,
    ]);
    // Envoy binds a fresh ephemeral port on each VM start. The exact deletion
    // above cannot identify a TPROXY rule left by an earlier start, so remove
    // any remaining mangle rules owned by this deterministic tap before the
    // replacement policy is installed.
    delete_iptables_chain_rules_for_interface("mangle", "PREROUTING", tap)?;
    for protocol in ["udp", "tcp"] {
        delete_iptables_repeated(&[
            "-t",
            "nat",
            "-D",
            "PREROUTING",
            "-i",
            tap,
            "-p",
            protocol,
            "--dport",
            "53",
            "-j",
            "REDIRECT",
            "--to-ports",
            dns_port.as_str(),
        ]);
    }
    delete_iptables_repeated(&[
        "-D",
        "INPUT",
        "-i",
        tap,
        "-m",
        "conntrack",
        "--ctstate",
        "ESTABLISHED,RELATED",
        "-j",
        "ACCEPT",
    ]);
    delete_iptables_repeated(&[
        "-D",
        "INPUT",
        "-i",
        tap,
        "-p",
        "tcp",
        "-m",
        "mark",
        "--mark",
        TPROXY_MARK_MASKED,
        "-j",
        "ACCEPT",
    ]);
    for protocol in ["udp", "tcp"] {
        delete_iptables_repeated(&[
            "-D",
            "INPUT",
            "-i",
            tap,
            "-p",
            protocol,
            "--dport",
            dns_port.as_str(),
            "-j",
            "ACCEPT",
        ]);
    }
    delete_iptables_repeated(&["-D", "INPUT", "-i", tap, "-j", "DROP"]);
    delete_iptables_repeated(&["-D", "FORWARD", "-i", tap, "-j", "DROP"]);
    let _ = run_cmd("ip6tables", &["-D", "INPUT", "-i", tap, "-j", "DROP"]);
    let _ = run_cmd("ip6tables", &["-D", "FORWARD", "-i", tap, "-j", "DROP"]);
    let _ = run_cmd("ip", &["link", "del", "dev", tap]);
    Ok(())
}

async fn start_forwarder(
    vm_id: &str,
    label: &'static str,
    listen: SocketAddr,
    target: SocketAddr,
    stops: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
) -> Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind {label} forwarder on {listen}"))?;
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    {
        let mut guard = stops.lock().unwrap_or_else(|error| error.into_inner());
        guard.push(stop_tx);
    }
    let vm_id = vm_id.to_string();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((socket, peer)) => {
                            tokio::spawn(async move {
                                if let Err(err) = forward_connection(socket, target).await {
                                    debug!(%peer, %target, error = %err, "tap control forward failed");
                                }
                            });
                        }
                        Err(err) => {
                            warn!(vm_id = %vm_id, %listen, error = %err, "tap control forward accept failed");
                            break;
                        }
                    }
                }
            }
        }
    });
    Ok(())
}

async fn forward_connection(mut inbound: TcpStream, target: SocketAddr) -> Result<()> {
    let mut outbound =
        tokio::time::timeout(TAP_FORWARD_CONNECT_TIMEOUT, TcpStream::connect(target))
            .await
            .with_context(|| {
                format!(
                    "connect tap forward target {target} timed out after {}s",
                    TAP_FORWARD_CONNECT_TIMEOUT.as_secs()
                )
            })?
            .with_context(|| format!("connect tap forward target {target}"))?;
    io::copy_bidirectional(&mut inbound, &mut outbound)
        .await
        .with_context(|| format!("copy tap forward stream to {target}"))?;
    Ok(())
}

fn parse_bind_ip(value: &str) -> Result<IpAddr> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("empty bind address");
    }
    trimmed
        .parse::<IpAddr>()
        .with_context(|| format!("parse bind address {trimmed}"))
}

fn delete_iptables_repeated(args: &[&str]) {
    for _ in 0..16 {
        if !Command::new("iptables")
            .args(args)
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
        {
            break;
        }
    }
}

fn delete_iptables_chain_rules_for_interface(
    table: &str,
    chain: &str,
    interface: &str,
) -> Result<()> {
    let output = Command::new("iptables")
        .args(["-t", table, "-L", chain, "-n", "-v", "-x", "--line-numbers"])
        .output()
        .with_context(|| format!("list iptables {table}/{chain} rules"))?;
    if !output.status.success() {
        return Err(anyhow!(
            "iptables failed with status {} while listing {table}/{chain}",
            output.status
        ));
    }

    let mut rule_numbers = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let number = fields.next()?.parse::<u32>().ok()?;
            fields.any(|field| field == interface).then_some(number)
        })
        .collect::<Vec<_>>();
    rule_numbers.sort_unstable_by(|left, right| right.cmp(left));

    for rule_number in rule_numbers {
        let rule_number = rule_number.to_string();
        run_cmd(
            "iptables",
            &["-t", table, "-D", chain, rule_number.as_str()],
        )
        .with_context(|| format!("delete stale iptables {table}/{chain} rule for {interface}"))?;
    }
    Ok(())
}

fn iptables(args: &[&str], label: &str) -> Result<()> {
    run_cmd("iptables", args).with_context(|| format!("iptables: {label}"))
}

fn run_cmd(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .status()
        .with_context(|| format!("run {program}"))?;
    if !status.success() {
        return Err(anyhow!(
            "{program} failed with status {status}: {}",
            args.join(" ")
        ));
    }
    Ok(())
}
