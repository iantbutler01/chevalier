use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::mem;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

const SCHEMA_VERSION: u32 = 1;
const MAXIMUM_FRAME_BYTES: usize = 64 * 1024;
const PORTPROXY_TOKEN_PATH: &str = "/Library/Application Support/Chevalier/etc/portproxy.token";
const KICKSTART_PATH: &str =
    "/System/Library/CoreServices/RemoteManagement/ARDAgent.app/Contents/Resources/kickstart";

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GuestRuntimeConfiguration {
    schema_version: u32,
    portproxy_auth_token: Option<String>,
    vnc_legacy_enabled: Option<bool>,
    vnc_password: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GuestRuntimeConfigurationResponse {
    schema_version: u32,
    ok: bool,
    error: Option<String>,
}

pub fn run_agent(port: u32) -> ExitCode {
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("portproxy-darwin-vsock-bridge: runtime configuration agent must run as root");
        return ExitCode::from(77);
    }
    let listener = match bind_host_only_listener(port) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("portproxy-darwin-vsock-bridge: bind runtime agent: {error}");
            return ExitCode::from(1);
        }
    };
    eprintln!(
        "portproxy-darwin-vsock-bridge: runtime configuration agent listening on guest vsock:{port}"
    );

    loop {
        let descriptor = unsafe {
            libc::accept(
                listener.as_raw_fd(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if descriptor < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            eprintln!("portproxy-darwin-vsock-bridge: runtime agent accept: {error}");
            return ExitCode::from(1);
        }
        let mut connection = unsafe { File::from_raw_fd(descriptor) };
        let response = match configure_connection(&mut connection) {
            Ok(()) => GuestRuntimeConfigurationResponse {
                schema_version: SCHEMA_VERSION,
                ok: true,
                error: None,
            },
            Err(error) => GuestRuntimeConfigurationResponse {
                schema_version: SCHEMA_VERSION,
                ok: false,
                error: Some(error.to_string()),
            },
        };
        if let Err(error) = write_frame(&mut connection, &response) {
            eprintln!("portproxy-darwin-vsock-bridge: runtime agent response: {error}");
        }
    }
}

fn bind_host_only_listener(port: u32) -> io::Result<File> {
    let descriptor = unsafe { libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM, 0) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    let listener = unsafe { File::from_raw_fd(descriptor) };
    let address = libc::sockaddr_vm {
        svm_len: mem::size_of::<libc::sockaddr_vm>() as u8,
        svm_family: libc::AF_VSOCK as libc::sa_family_t,
        svm_reserved1: 0,
        svm_port: port,
        svm_cid: libc::VMADDR_CID_ANY,
    };
    let result = unsafe {
        libc::bind(
            listener.as_raw_fd(),
            (&address as *const libc::sockaddr_vm).cast::<libc::sockaddr>(),
            mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::listen(listener.as_raw_fd(), 4) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(listener)
}

fn configure_connection(connection: &mut File) -> io::Result<()> {
    require_host_peer(connection.as_raw_fd())?;
    configure_timeouts(connection.as_raw_fd())?;
    let config: GuestRuntimeConfiguration = read_frame(connection)?;
    validate_configuration(&config)?;
    if let Some(enabled) = config.vnc_legacy_enabled {
        configure_vnc(enabled, config.vnc_password.as_deref())?;
    }
    if let Some(token) = config.portproxy_auth_token.as_deref() {
        replace_portproxy_token(token)?;
    }
    Ok(())
}

fn require_host_peer(descriptor: RawFd) -> io::Result<()> {
    let mut address = libc::sockaddr_vm {
        svm_len: 0,
        svm_family: 0,
        svm_reserved1: 0,
        svm_port: 0,
        svm_cid: 0,
    };
    let mut length = mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t;
    let result = unsafe {
        libc::getpeername(
            descriptor,
            (&mut address as *mut libc::sockaddr_vm).cast::<libc::sockaddr>(),
            &mut length,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    if address.svm_family != libc::AF_VSOCK as libc::sa_family_t
        || address.svm_cid != libc::VMADDR_CID_HOST
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "runtime configuration connection did not originate from the VM host",
        ));
    }
    Ok(())
}

fn configure_timeouts(descriptor: RawFd) -> io::Result<()> {
    let timeout = libc::timeval {
        tv_sec: 10,
        tv_usec: 0,
    };
    for option in [libc::SO_RCVTIMEO, libc::SO_SNDTIMEO] {
        let result = unsafe {
            libc::setsockopt(
                descriptor,
                libc::SOL_SOCKET,
                option,
                (&timeout as *const libc::timeval).cast(),
                mem::size_of::<libc::timeval>() as libc::socklen_t,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn validate_configuration(config: &GuestRuntimeConfiguration) -> io::Result<()> {
    if config.schema_version != SCHEMA_VERSION {
        return Err(invalid_data(format!(
            "unsupported runtime configuration schema {}",
            config.schema_version
        )));
    }
    if let Some(token) = config.portproxy_auth_token.as_deref()
        && (!(32..=256).contains(&token.len()) || !token.bytes().all(is_safe_secret_byte))
    {
        return Err(invalid_data("invalid portproxy authentication token"));
    }
    match (config.vnc_legacy_enabled, config.vnc_password.as_deref()) {
        (Some(true), Some(password))
            if password.len() == 8 && password.bytes().all(|byte| byte.is_ascii_alphanumeric()) =>
        {
            Ok(())
        }
        (Some(false) | None, None) => Ok(()),
        _ => Err(invalid_data(
            "VNC legacy mode requires exactly eight ASCII alphanumeric password bytes",
        )),
    }
}

fn is_safe_secret_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'~' | b'-')
}

fn configure_vnc(enabled: bool, password: Option<&str>) -> io::Result<()> {
    let mut command = Command::new(KICKSTART_PATH);
    command.args([
        "-configure",
        "-clientopts",
        "-setvnclegacy",
        "-vnclegacy",
        if enabled { "yes" } else { "no" },
    ]);
    if let Some(password) = password {
        command.args(["-clientopts", "-setvncpw", "-vncpw", password]);
    }
    let status = command.args(["-restart", "-agent"]).status()?;
    if !status.success() {
        return Err(io::Error::other(format!(
            "macOS Screen Sharing configuration failed with {status}"
        )));
    }
    if enabled {
        wait_for_screen_sharing()?;
    }
    Ok(())
}

fn wait_for_screen_sharing() -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 5900);
    loop {
        match TcpStream::connect_timeout(&address.into(), Duration::from_secs(1)) {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(250)),
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    "macOS Screen Sharing is not enabled in the gold image",
                ));
            }
        }
    }
}

fn replace_portproxy_token(token: &str) -> io::Result<()> {
    let destination = Path::new(PORTPROXY_TOKEN_PATH);
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::other("portproxy token path has no parent"))?;
    fs::create_dir_all(parent)?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    let temporary = temporary_path(destination);
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o400)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temporary)?;
        file.write_all(token.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        if unsafe { libc::fchown(file.as_raw_fd(), 0, 0) } < 0 {
            return Err(io::Error::last_os_error());
        }
        drop(file);
        fs::rename(&temporary, destination)?;
        File::open(parent)?.sync_all()?;
        let status = Command::new("/bin/launchctl")
            .args(["kickstart", "-k", "system/com.bracket.portproxy"])
            .status()?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "restart portproxy after token rotation failed with {status}"
            )));
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn temporary_path(destination: &Path) -> PathBuf {
    let mut path = destination.as_os_str().to_owned();
    path.push(format!(".{}.tmp", std::process::id()));
    PathBuf::from(path)
}

fn read_frame<T: for<'de> Deserialize<'de>>(reader: &mut impl Read) -> io::Result<T> {
    let mut header = [0_u8; 4];
    reader.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 || length > MAXIMUM_FRAME_BYTES {
        return Err(invalid_data(format!("invalid frame length {length}")));
    }
    let mut payload = vec![0_u8; length];
    reader.read_exact(&mut payload)?;
    serde_json::from_slice(&payload).map_err(invalid_data)
}

fn write_frame(writer: &mut impl Write, value: &impl Serialize) -> io::Result<()> {
    let payload = serde_json::to_vec(value).map_err(invalid_data)?;
    if payload.is_empty() || payload.len() > MAXIMUM_FRAME_BYTES {
        return Err(invalid_data(format!(
            "invalid response frame length {}",
            payload.len()
        )));
    }
    writer.write_all(&(payload.len() as u32).to_be_bytes())?;
    writer.write_all(&payload)?;
    writer.flush()
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(enabled: Option<bool>, password: Option<&str>) -> GuestRuntimeConfiguration {
        GuestRuntimeConfiguration {
            schema_version: SCHEMA_VERSION,
            portproxy_auth_token: Some("a".repeat(32)),
            vnc_legacy_enabled: enabled,
            vnc_password: password.map(str::to_owned),
        }
    }

    #[test]
    fn validates_bounded_runtime_secrets() {
        assert!(validate_configuration(&config(Some(true), Some("A1b2C3d4"))).is_ok());
        assert!(validate_configuration(&config(Some(false), None)).is_ok());
        assert!(validate_configuration(&config(None, None)).is_ok());
        assert!(validate_configuration(&config(Some(true), Some("too-long9"))).is_err());
        assert!(validate_configuration(&config(Some(false), Some("A1b2C3d4"))).is_err());
        assert!(validate_configuration(&config(None, Some("A1b2C3d4"))).is_err());
        let mut invalid_token = config(None, None);
        invalid_token.portproxy_auth_token = Some("contains a space".to_owned());
        assert!(validate_configuration(&invalid_token).is_err());
    }

    #[test]
    fn frame_round_trip_is_length_bounded() {
        let response = GuestRuntimeConfigurationResponse {
            schema_version: SCHEMA_VERSION,
            ok: true,
            error: None,
        };
        let mut frame = Vec::new();
        write_frame(&mut frame, &response).unwrap();
        let decoded: serde_json::Value = read_frame(&mut frame.as_slice()).unwrap();
        assert_eq!(decoded["schemaVersion"], SCHEMA_VERSION);
        assert_eq!(decoded["ok"], true);
    }
}
