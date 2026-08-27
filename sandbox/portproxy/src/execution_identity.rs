use std::env;
use std::ffi::CString;
use std::io;
use std::os::unix::process::CommandExt;

use nix::unistd::{User, geteuid};
use portable_pty::CommandBuilder;
use tokio::process::Command;

const EXEC_USER_ENV: &str = "CHEVALIER_PORTPROXY_EXEC_USER";

#[derive(Clone)]
struct ExecutionIdentity {
    name: String,
    c_name: CString,
    uid: u32,
    gid: u32,
}

fn configured_identity() -> io::Result<Option<ExecutionIdentity>> {
    let Some(name) = env::var(EXEC_USER_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    validate_name(&name)?;
    let user = User::from_name(&name)
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("user {name} not found")))?;
    let c_name = CString::new(name.as_bytes()).map_err(io::Error::other)?;
    Ok(Some(ExecutionIdentity {
        name,
        c_name,
        uid: user.uid.as_raw(),
        gid: user.gid.as_raw(),
    }))
}

fn validate_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{EXEC_USER_ENV} contains unsupported characters"),
        ));
    }
    Ok(())
}

pub fn configure_command(command: &mut Command, run_as_root: bool) -> io::Result<()> {
    if run_as_root {
        return Ok(());
    }
    let Some(identity) = configured_identity()? else {
        return Ok(());
    };
    if geteuid().as_raw() == identity.uid {
        return Ok(());
    }
    if !geteuid().is_root() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("portproxy must run as root to execute as {}", identity.name),
        ));
    }
    let name = identity.c_name;
    let uid = identity.uid;
    let gid = identity.gid;
    unsafe {
        command.as_std_mut().pre_exec(move || {
            if nix::libc::initgroups(name.as_ptr(), gid as _) == -1 {
                return Err(io::Error::last_os_error());
            }
            if nix::libc::setgid(gid) == -1 {
                return Err(io::Error::last_os_error());
            }
            if nix::libc::setuid(uid) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

pub fn command_builder(program: &str) -> io::Result<CommandBuilder> {
    let Some(identity) = configured_identity()? else {
        return Ok(CommandBuilder::new(program));
    };
    if geteuid().as_raw() == identity.uid {
        return Ok(CommandBuilder::new(program));
    }
    if !geteuid().is_root() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("portproxy must run as root to execute as {}", identity.name),
        ));
    }
    let mut builder = CommandBuilder::new("/usr/bin/sudo");
    builder.args(["-H", "-E", "-u", &identity.name, "--", program]);
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_an_execution_user_that_could_be_parsed_as_arguments() {
        assert!(validate_name("openbracket --preserve-env").is_err());
        assert!(validate_name("openbracket").is_ok());
    }
}
