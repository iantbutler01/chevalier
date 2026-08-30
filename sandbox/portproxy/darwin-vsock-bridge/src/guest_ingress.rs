use std::fs::File;
use std::io::{self, Read};
use std::mem;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

pub const MAXIMUM_CONCURRENT_SESSIONS: usize = 16;

pub fn run_agent(port: u32) -> ExitCode {
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("portproxy-darwin-vsock-bridge: guest ingress agent must run as root");
        return ExitCode::from(77);
    }
    let listener = match bind_host_only_listener(port) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("portproxy-darwin-vsock-bridge: bind guest ingress: {error}");
            return ExitCode::from(1);
        }
    };
    eprintln!("portproxy-darwin-vsock-bridge: guest ingress listening on guest vsock:{port}");
    let active = Arc::new(AtomicUsize::new(0));

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
            eprintln!("portproxy-darwin-vsock-bridge: guest ingress accept: {error}");
            return ExitCode::from(1);
        }
        let mut connection = unsafe { File::from_raw_fd(descriptor) };
        if !reserve_session(&active) {
            eprintln!("portproxy-darwin-vsock-bridge: rejected guest ingress above session cap");
            continue;
        }
        let active = active.clone();
        thread::spawn(move || {
            if let Err(error) = handle_connection(&mut connection) {
                eprintln!("portproxy-darwin-vsock-bridge: guest ingress session: {error}");
            }
            active.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

fn reserve_session(active: &AtomicUsize) -> bool {
    active
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < MAXIMUM_CONCURRENT_SESSIONS).then_some(current + 1)
        })
        .is_ok()
}

fn handle_connection(connection: &mut File) -> io::Result<()> {
    require_host_peer(connection.as_raw_fd())?;
    configure_timeouts(connection.as_raw_fd())?;
    let port = read_destination_port(connection)?;
    clear_timeouts(connection.as_raw_fd())?;
    let target = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let tcp =
        TcpStream::connect_timeout(&target.into(), Duration::from_secs(5)).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("connect guest loopback {target}: {error}"),
            )
        })?;
    tcp.set_nodelay(true)?;
    crate::relay(tcp, connection.try_clone()?)
}

fn read_destination_port(reader: &mut impl Read) -> io::Result<u16> {
    let mut encoded = [0u8; 2];
    reader.read_exact(&mut encoded)?;
    let port = u16::from_be_bytes(encoded);
    if port == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "guest ingress destination port must be greater than zero",
        ));
    }
    Ok(port)
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
    if unsafe { libc::listen(listener.as_raw_fd(), MAXIMUM_CONCURRENT_SESSIONS as i32) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(listener)
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
            "guest ingress connection did not originate from the VM host",
        ));
    }
    Ok(())
}

fn configure_timeouts(descriptor: RawFd) -> io::Result<()> {
    set_timeouts(
        descriptor,
        libc::timeval {
            tv_sec: 10,
            tv_usec: 0,
        },
    )
}

fn clear_timeouts(descriptor: RawFd) -> io::Result<()> {
    set_timeouts(
        descriptor,
        libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
    )
}

fn set_timeouts(descriptor: RawFd, timeout: libc::timeval) -> io::Result<()> {
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

#[cfg(test)]
fn timeout_for(descriptor: RawFd, option: libc::c_int) -> io::Result<libc::timeval> {
    let mut timeout = libc::timeval {
        tv_sec: 10,
        tv_usec: 0,
    };
    let mut length = mem::size_of::<libc::timeval>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            descriptor,
            libc::SOL_SOCKET,
            option,
            (&mut timeout as *mut libc::timeval).cast(),
            &mut length,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(timeout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    #[test]
    fn parses_big_endian_destination_port() {
        assert_eq!(
            read_destination_port(&mut [0x17, 0x0c].as_slice()).unwrap(),
            5900
        );
    }

    #[test]
    fn rejects_zero_destination_port() {
        let error = read_destination_port(&mut [0, 0].as_slice()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn caps_concurrent_sessions() {
        let active = AtomicUsize::new(0);
        for _ in 0..MAXIMUM_CONCURRENT_SESSIONS {
            assert!(reserve_session(&active));
        }
        assert!(!reserve_session(&active));
    }

    #[test]
    fn clears_handshake_deadlines_before_long_lived_relay() {
        let (socket, _peer) = UnixStream::pair().unwrap();
        configure_timeouts(socket.as_raw_fd()).unwrap();
        assert_eq!(
            timeout_for(socket.as_raw_fd(), libc::SO_RCVTIMEO)
                .unwrap()
                .tv_sec,
            10
        );
        clear_timeouts(socket.as_raw_fd()).unwrap();
        for option in [libc::SO_RCVTIMEO, libc::SO_SNDTIMEO] {
            let timeout = timeout_for(socket.as_raw_fd(), option).unwrap();
            assert_eq!(timeout.tv_sec, 0);
            assert_eq!(timeout.tv_usec, 0);
        }
    }
}
