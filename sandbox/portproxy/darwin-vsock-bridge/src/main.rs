#[cfg(not(target_os = "macos"))]
compile_error!("portproxy-darwin-vsock-bridge only supports macOS guests");

use std::env;
use std::fs::File;
use std::io;
use std::mem;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

const DEFAULT_VSOCK_PORT: u32 = 13_338;
const DEFAULT_TCP_ADDRESS: &str = "127.0.0.1:13338";
const DEFAULT_RETRY_MILLIS: u64 = 1_000;
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    vsock_port: u32,
    tcp_address: String,
    listen_address: Option<String>,
    retry_millis: u64,
    once: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            vsock_port: DEFAULT_VSOCK_PORT,
            tcp_address: DEFAULT_TCP_ADDRESS.to_owned(),
            listen_address: None,
            retry_millis: DEFAULT_RETRY_MILLIS,
            once: false,
        }
    }
}

enum Command {
    Run(Config),
    Check(Config),
    Version,
    Help,
}

fn main() -> ExitCode {
    let command = match parse_args(env::args().skip(1)) {
        Ok(command) => command,
        Err(message) => {
            eprintln!("portproxy-darwin-vsock-bridge: {message}");
            eprintln!("{}", usage());
            return ExitCode::from(64);
        }
    };

    match command {
        Command::Version => {
            println!("portproxy-darwin-vsock-bridge {VERSION}");
            ExitCode::SUCCESS
        }
        Command::Help => {
            print!("{}", usage());
            ExitCode::SUCCESS
        }
        Command::Check(config) => {
            let tcp_mode = config
                .listen_address
                .as_deref()
                .map(|address| format!("listen:{address}"))
                .unwrap_or_else(|| format!("connect:{}", config.tcp_address));
            println!(
                "vsock_cid={} vsock_port={} tcp_mode={} retry_millis={} once={}",
                libc::VMADDR_CID_HOST,
                config.vsock_port,
                tcp_mode,
                config.retry_millis,
                config.once
            );
            ExitCode::SUCCESS
        }
        Command::Run(config) => run(config),
    }
}

fn usage() -> &'static str {
    "Usage: portproxy-darwin-vsock-bridge [options]\n\
     \n\
     Connects from a macOS guest to a Virtualization.framework host listener and\n\
     relays the byte stream to or from a guest-loopback TCP endpoint.\n\
     \n\
     Options:\n\
       --vsock-port <port>       Host VZ listener port (default: 13338)\n\
       --tcp-address <host:port> Guest-local portproxy endpoint (default: 127.0.0.1:13338)\n\
       --listen-address <addr>   Listen for guest clients and relay each to host vsock\n\
       --retry-millis <ms>       Reconnect delay (default: 1000)\n\
       --once                    Exit after the first connection ends or fails\n\
       --check-config            Validate and print configuration without connecting\n\
       --version                 Print version\n\
       --help                    Print help\n"
}

fn parse_args(arguments: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut config = Config::default();
    let mut check = false;
    let mut tcp_address_set = false;
    let mut arguments = arguments.into_iter();

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--vsock-port" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "missing value for --vsock-port".to_owned())?;
                config.vsock_port = parse_port("--vsock-port", &value)?;
            }
            "--tcp-address" => {
                if config.listen_address.is_some() {
                    return Err(
                        "--tcp-address and --listen-address are mutually exclusive".to_owned()
                    );
                }
                let value = arguments
                    .next()
                    .ok_or_else(|| "missing value for --tcp-address".to_owned())?;
                validate_tcp_address(&value)?;
                config.tcp_address = value;
                tcp_address_set = true;
            }
            "--listen-address" => {
                if tcp_address_set {
                    return Err(
                        "--tcp-address and --listen-address are mutually exclusive".to_owned()
                    );
                }
                let value = arguments
                    .next()
                    .ok_or_else(|| "missing value for --listen-address".to_owned())?;
                validate_tcp_address(&value)?;
                config.listen_address = Some(value);
            }
            "--retry-millis" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "missing value for --retry-millis".to_owned())?;
                config.retry_millis = value
                    .parse()
                    .map_err(|_| format!("invalid --retry-millis value: {value}"))?;
                if config.retry_millis == 0 {
                    return Err("--retry-millis must be greater than zero".to_owned());
                }
            }
            "--once" => config.once = true,
            "--check-config" => check = true,
            "--version" => return Ok(Command::Version),
            "--help" | "-h" => return Ok(Command::Help),
            _ => return Err(format!("unexpected argument: {argument}")),
        }
    }

    if check {
        Ok(Command::Check(config))
    } else {
        Ok(Command::Run(config))
    }
}

fn parse_port(option: &str, value: &str) -> Result<u32, String> {
    let port = value
        .parse::<u32>()
        .map_err(|_| format!("invalid {option} value: {value}"))?;
    if port == 0 {
        return Err(format!("{option} must be greater than zero"));
    }
    Ok(port)
}

fn validate_tcp_address(value: &str) -> Result<(), String> {
    let address = value
        .parse::<SocketAddr>()
        .map_err(|_| format!("invalid TCP address: {value}"))?;
    if !address.ip().is_loopback() {
        return Err(format!("TCP address must be loopback: {value}"));
    }
    if address.port() == 0 {
        return Err("TCP address port must be greater than zero".to_owned());
    }
    Ok(())
}

fn run(config: Config) -> ExitCode {
    if config.listen_address.is_some() {
        return run_listener(config);
    }
    loop {
        match connect_and_relay(&config) {
            Ok(()) => eprintln!("portproxy-darwin-vsock-bridge: connection closed"),
            Err(error) => eprintln!("portproxy-darwin-vsock-bridge: {error}"),
        }

        if config.once {
            return ExitCode::from(1);
        }
        thread::sleep(Duration::from_millis(config.retry_millis));
    }
}

fn run_listener(config: Config) -> ExitCode {
    let address = config.listen_address.as_deref().expect("listener mode");
    let listener = match TcpListener::bind(address) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("portproxy-darwin-vsock-bridge: listen on {address}: {error}");
            return ExitCode::from(1);
        }
    };
    eprintln!(
        "portproxy-darwin-vsock-bridge: listening on {address} for host vsock:{}",
        config.vsock_port
    );

    for accepted in listener.incoming() {
        let tcp = match accepted {
            Ok(tcp) => tcp,
            Err(error) => {
                eprintln!("portproxy-darwin-vsock-bridge: accept on {address}: {error}");
                continue;
            }
        };
        let vsock_port = config.vsock_port;
        if config.once {
            return match relay_client_to_host(tcp, vsock_port) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("portproxy-darwin-vsock-bridge: {error}");
                    ExitCode::from(1)
                }
            };
        }
        thread::spawn(move || {
            if let Err(error) = relay_client_to_host(tcp, vsock_port) {
                eprintln!("portproxy-darwin-vsock-bridge: {error}");
            }
        });
    }
    ExitCode::from(1)
}

fn connect_and_relay(config: &Config) -> io::Result<()> {
    let tcp = TcpStream::connect(&config.tcp_address).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("connect guest-local TCP {}: {error}", config.tcp_address),
        )
    })?;
    tcp.set_nodelay(true)?;

    let vsock = connect_host_vsock(config.vsock_port).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("connect host vsock port {}: {error}", config.vsock_port),
        )
    })?;

    eprintln!(
        "portproxy-darwin-vsock-bridge: connected host vsock:{} to {}",
        config.vsock_port, config.tcp_address
    );
    relay(tcp, vsock)
}

fn relay_client_to_host(tcp: TcpStream, vsock_port: u32) -> io::Result<()> {
    tcp.set_nodelay(true)?;
    let vsock = connect_host_vsock(vsock_port).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("connect host vsock port {vsock_port}: {error}"),
        )
    })?;
    relay(tcp, vsock)
}

fn connect_host_vsock(port: u32) -> io::Result<File> {
    let descriptor = unsafe { libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM, 0) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    let descriptor = OwnedDescriptor::new(descriptor);
    let address = libc::sockaddr_vm {
        svm_len: mem::size_of::<libc::sockaddr_vm>() as u8,
        svm_family: libc::AF_VSOCK as libc::sa_family_t,
        svm_reserved1: 0,
        svm_port: port,
        svm_cid: libc::VMADDR_CID_HOST,
    };
    let result = unsafe {
        libc::connect(
            descriptor.raw,
            (&address as *const libc::sockaddr_vm).cast::<libc::sockaddr>(),
            mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    let raw = descriptor.into_raw();
    Ok(unsafe { File::from_raw_fd(raw) })
}

fn relay(tcp: TcpStream, vsock: File) -> io::Result<()> {
    let mut tcp_reader = tcp.try_clone()?;
    let mut tcp_writer = tcp;
    let mut vsock_reader = vsock.try_clone()?;
    let mut vsock_writer = vsock;

    let guest_to_host = thread::spawn(move || {
        let result = io::copy(&mut tcp_reader, &mut vsock_writer);
        let _ = tcp_reader.shutdown(Shutdown::Both);
        unsafe {
            libc::shutdown(vsock_writer.as_raw_fd(), libc::SHUT_RDWR);
        }
        result
    });
    let host_to_guest = thread::spawn(move || {
        let result = io::copy(&mut vsock_reader, &mut tcp_writer);
        let _ = tcp_writer.shutdown(Shutdown::Both);
        unsafe {
            libc::shutdown(vsock_reader.as_raw_fd(), libc::SHUT_RDWR);
        }
        result
    });

    let guest_result = guest_to_host
        .join()
        .map_err(|_| io::Error::other("guest-to-host relay thread panicked"))?;
    let host_result = host_to_guest
        .join()
        .map_err(|_| io::Error::other("host-to-guest relay thread panicked"))?;
    guest_result.and(host_result).map(|_| ())
}

struct OwnedDescriptor {
    raw: RawFd,
}

impl OwnedDescriptor {
    fn new(raw: RawFd) -> Self {
        Self { raw }
    }

    fn into_raw(mut self) -> RawFd {
        let raw = self.raw;
        self.raw = -1;
        raw
    }
}

impl Drop for OwnedDescriptor {
    fn drop(&mut self) {
        if self.raw >= 0 {
            unsafe {
                libc::close(self.raw);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    fn strings(values: &[&str]) -> impl Iterator<Item = String> {
        values.iter().map(|value| (*value).to_owned())
    }

    #[test]
    fn parses_defaults() {
        let Command::Run(config) = parse_args(strings(&[])).unwrap() else {
            panic!("expected run command");
        };
        assert_eq!(config, Config::default());
    }

    #[test]
    fn parses_overrides() {
        let Command::Run(config) = parse_args(strings(&[
            "--vsock-port",
            "4444",
            "--tcp-address",
            "127.0.0.1:5555",
            "--retry-millis",
            "50",
            "--once",
        ]))
        .unwrap() else {
            panic!("expected run command");
        };
        assert_eq!(config.vsock_port, 4444);
        assert_eq!(config.tcp_address, "127.0.0.1:5555");
        assert_eq!(config.listen_address, None);
        assert_eq!(config.retry_millis, 50);
        assert!(config.once);
    }

    #[test]
    fn rejects_invalid_values() {
        assert!(parse_args(strings(&["--vsock-port", "0"])).is_err());
        assert!(parse_args(strings(&["--tcp-address", "localhost"])).is_err());
        assert!(parse_args(strings(&["--tcp-address", "0.0.0.0:13338"])).is_err());
        assert!(
            parse_args(strings(&[
                "--tcp-address",
                "127.0.0.1:13338",
                "--listen-address",
                "127.0.0.1:18080",
            ]))
            .is_err()
        );
        assert!(parse_args(strings(&["--retry-millis", "0"])).is_err());
    }

    #[test]
    fn parses_guest_listener_mode() {
        let Command::Run(config) = parse_args(strings(&[
            "--vsock-port",
            "13339",
            "--listen-address",
            "127.0.0.1:18080",
        ]))
        .unwrap() else {
            panic!("expected run command");
        };

        assert_eq!(config.vsock_port, 13_339);
        assert_eq!(config.listen_address.as_deref(), Some("127.0.0.1:18080"));
    }

    #[test]
    fn relays_both_directions_and_exits_when_host_disconnects() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let mut guest = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (guest_agent, _) = listener.accept().unwrap();

        let mut descriptors = [-1; 2];
        let result = unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_STREAM,
                0,
                descriptors.as_mut_ptr(),
            )
        };
        assert_eq!(result, 0);
        let bridge_vsock = unsafe { File::from_raw_fd(descriptors[0]) };
        let mut host = unsafe { UnixStream::from_raw_fd(descriptors[1]) };
        guest
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        host.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

        let relay_task = thread::spawn(move || relay(guest_agent, bridge_vsock));

        guest.write_all(b"guest-to-host").unwrap();
        let mut from_guest = [0; 13];
        host.read_exact(&mut from_guest).unwrap();
        assert_eq!(&from_guest, b"guest-to-host");

        host.write_all(b"host-to-guest").unwrap();
        let mut from_host = [0; 13];
        guest.read_exact(&mut from_host).unwrap();
        assert_eq!(&from_host, b"host-to-guest");

        drop(host);
        relay_task.join().unwrap().unwrap();
    }
}
