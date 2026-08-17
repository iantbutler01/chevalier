use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ffi::CStr;
use std::ffi::CString;
use std::io;
use std::io::IoSlice;
use std::mem::size_of;
use std::os::raw::c_void;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use log::trace;
use log::warn;
use nix::errno::Errno;
use parking_lot::Mutex;

use super::mount_options::MountOption;
use super::mount_options::option_to_string;
use crate::SessionACL;

type MFTypeRef = *mut c_void;
type MFMessageRef = MFTypeRef;
type MFChannelRef = MFTypeRef;
type FuseChannelRef = *mut c_void;

#[repr(C)]
struct FuseArgs {
    argc: libc::c_int,
    argv: *mut *mut libc::c_char,
    allocated: libc::c_int,
}

const MF_MOUNT_SUCCESS: i32 = 0;
const MF_MOUNT_UNSUPPORTED_OS_VERSION: i32 = 1;
const MF_MOUNT_HELPER_TOOLS_INSTALLATION_FAILED: i32 = 2;
const MF_MOUNT_FILE_SYSTEM_EXTENSION_NOT_FOUND: i32 = 3;
const MF_MOUNT_FILE_SYSTEM_EXTENSION_REQUIRES_APPROVAL: i32 = 4;
const MF_MOUNT_UNEXPECTED_FAILURE: i32 = -1;
const MACFUSE_VERSION: &str = "5.3.3";
const MACFUSE_VERSION_PLIST: &str = "/Library/Filesystems/macfuse.fs/Contents/version.plist";

unsafe extern "C" {
    fn getmntinfo_r_np(mount_buffer: *mut *mut libc::statfs, flags: libc::c_int) -> libc::c_int;
    fn MFRetain(reference: MFTypeRef) -> MFTypeRef;
    fn MFRelease(reference: MFTypeRef);
    fn MFMessageGetBodySize(message: MFMessageRef) -> libc::ssize_t;
    fn MFMessageGetBodyBuffers(
        message: MFMessageRef,
        buffers: *mut *const libc::iovec,
    ) -> libc::ssize_t;
    fn MFMessageGetReplyBuffer(message: MFMessageRef, buffer: *mut *mut c_void) -> libc::ssize_t;
    fn MFChannelInterrupt(channel: MFChannelRef) -> bool;
    fn MFChannelCopyNextMessage(channel: MFChannelRef) -> MFMessageRef;
    fn MFChannelSendMessage(
        channel: MFChannelRef,
        buffers: *const libc::iovec,
        count: usize,
    ) -> libc::ssize_t;
    fn fuse_mount(mount_point: *const libc::c_char, args: *mut FuseArgs) -> FuseChannelRef;
    fn fuse_opt_free_args(args: *mut FuseArgs);
    fn fuse_darwin_chan_mfch(channel: FuseChannelRef, mf_channel: *mut MFChannelRef)
    -> libc::c_int;
    fn fuse_darwin_chan_unmount(channel: FuseChannelRef);
    fn fuse_darwin_chan_not_mounted(channel: FuseChannelRef) -> bool;
    fn fuse_chan_destroy(channel: FuseChannelRef);
}

#[derive(Debug)]
struct PendingMessages<T> {
    messages: HashMap<u64, T>,
}

impl<T> PendingMessages<T> {
    fn new() -> Self {
        Self {
            messages: HashMap::new(),
        }
    }

    fn insert(&mut self, unique: u64, message: T) -> Result<(), T> {
        match self.messages.entry(unique) {
            Entry::Vacant(entry) => {
                entry.insert(message);
                Ok(())
            }
            Entry::Occupied(_) => Err(message),
        }
    }

    fn remove(&mut self, unique: u64) -> Option<T> {
        self.messages.remove(&unique)
    }

    fn clear(&mut self) {
        self.messages.clear();
    }
}

#[derive(Debug)]
struct FuseChannel {
    raw: NonNull<c_void>,
    channel: Option<Arc<MacFuseChannel>>,
}

impl FuseChannel {
    fn new(raw: NonNull<c_void>) -> Self {
        Self { raw, channel: None }
    }

    fn attach(&mut self, channel: Arc<MacFuseChannel>) {
        self.channel = Some(channel);
    }

    fn as_ptr(&self) -> FuseChannelRef {
        self.raw.as_ptr()
    }
}

unsafe impl Send for FuseChannel {}

impl Drop for FuseChannel {
    fn drop(&mut self) {
        if let Some(channel) = &self.channel {
            channel.mark_closed_by_fuse_channel();
        }
        unsafe { fuse_chan_destroy(self.raw.as_ptr()) };
    }
}

#[derive(Debug)]
pub(crate) struct MacFuseChannel {
    raw: NonNull<c_void>,
    closing: AtomicBool,
    closed: AtomicBool,
    mount_failure: Mutex<Option<MountFailure>>,
    pending_messages: Mutex<PendingMessages<Message>>,
}

// MFMount channels are designed for one blocked receiver and concurrent reply senders.
unsafe impl Send for MacFuseChannel {}
unsafe impl Sync for MacFuseChannel {}

impl MacFuseChannel {
    fn from_borrowed(raw: MFChannelRef) -> io::Result<Arc<Self>> {
        let raw = NonNull::new(unsafe { MFRetain(raw) })
            .ok_or_else(|| io::Error::other("MFRetain returned NULL"))?;
        Ok(Arc::new(Self {
            raw,
            closing: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            mount_failure: Mutex::new(None),
            pending_messages: Mutex::new(PendingMessages::new()),
        }))
    }

    pub(crate) fn receive(&self, buffer: &mut [u8]) -> nix::Result<usize> {
        if self.is_closing() {
            return Err(self.terminal_errno());
        }

        let message = NonNull::new(unsafe { MFChannelCopyNextMessage(self.raw.as_ptr()) })
            .ok_or_else(Errno::last)?;
        let message = Message(message);
        let body_size = unsafe { MFMessageGetBodySize(message.0.as_ptr()) };
        if body_size < 0 {
            return Err(Errno::last());
        }

        let body_size = usize::try_from(body_size).map_err(|_| Errno::EIO)?;
        let mut body_buffers = ptr::null();
        let body_count = unsafe { MFMessageGetBodyBuffers(message.0.as_ptr(), &mut body_buffers) };
        if body_count < 0 {
            return Err(Errno::last());
        }

        let body_count = usize::try_from(body_count).map_err(|_| Errno::EIO)?;
        let received = unsafe { copy_body_buffers(body_buffers, body_count, body_size, buffer) }?;
        let unique = fuse_message_unique(&buffer[..received])?;
        if self
            .pending_messages
            .lock()
            .insert(unique, message)
            .is_err()
        {
            return Err(Errno::EPROTO);
        }
        Ok(received)
    }

    pub(crate) fn complete_without_reply(&self, unique: u64) {
        if self.pending_messages.lock().remove(unique).is_none() {
            warn!("MFChannel no-reply completion had no pending message unique={unique}");
        }
    }

    pub(crate) fn fail_pending_request(&self, unique: u64, errno: libc::c_int) -> io::Result<()> {
        let pending_message = self.pending_messages.lock().remove(unique);
        if pending_message.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("MFChannel error completion had no pending message unique={unique}"),
            ));
        }
        if self.closing.load(Ordering::Acquire) {
            return Err(io::Error::from_raw_os_error(libc::ENODEV));
        }
        self.send_errno_reply(unique, errno)
    }

    fn mark_closed_by_fuse_channel(&self) {
        self.closing.store(true, Ordering::Release);
        self.closed.store(true, Ordering::Release);
        self.pending_messages.lock().clear();
    }

    pub(crate) fn send(&self, buffers: &[IoSlice<'_>]) -> io::Result<()> {
        if buffers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MFChannelSendMessage requires at least one buffer",
            ));
        }
        let unique = fuse_message_unique(buffers[0].as_ref()).map_err(io::Error::from)?;
        let pending_message = self.pending_messages.lock().remove(unique);
        if unique != 0 && pending_message.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("MFChannel reply had no pending message unique={unique}"),
            ));
        }
        if self.closing.load(Ordering::Acquire) {
            return Err(io::Error::from_raw_os_error(libc::ENODEV));
        }

        let send_result = self.prepare_and_send(buffers, pending_message.as_ref());
        let (sent, expected) = match send_result {
            Ok(result) => result,
            Err(error) if pending_message.is_some() => {
                warn!("MFChannel reply preparation failed unique={unique}: {error}");
                self.send_errno_reply(unique, libc::EIO)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        trace!("MFChannel send unique={unique} returned {sent}");
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }
        if usize::try_from(sent).ok() != Some(expected) {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!("MFChannelSendMessage sent {sent} of {expected} bytes"),
            ));
        }
        Ok(())
    }

    fn prepare_and_send(
        &self,
        buffers: &[IoSlice<'_>],
        pending_message: Option<&Message>,
    ) -> io::Result<(libc::ssize_t, usize)> {
        let ordinary_iovecs: Vec<_> = buffers
            .iter()
            .map(|buffer| libc::iovec {
                iov_base: buffer.as_ptr().cast_mut().cast(),
                iov_len: buffer.len(),
            })
            .collect();
        let ordinary_size = buffers.iter().try_fold(0usize, |size, buffer| {
            size.checked_add(buffer.len())
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "reply size overflow"))
        })?;
        let reply_buffer = pending_message
            .map(Message::reply_buffer)
            .transpose()?
            .flatten();
        let mut reply_header = [0u8; FUSE_OUT_HEADER_SIZE];
        let mut reply_buffer_out = FuseReplyBufferOut { size: 0, flags: 0 };
        let (sent, expected) = match reply_buffer {
            Some((reply_buffer, reply_buffer_size))
                if fuse_reply_succeeded(buffers[0].as_ref())? =>
            {
                prepare_buffered_reply(
                    reply_buffer,
                    reply_buffer_size,
                    buffers,
                    &mut reply_header,
                    &mut reply_buffer_out,
                )?;
                let reply_iovecs = [
                    libc::iovec {
                        iov_base: reply_header.as_mut_ptr().cast(),
                        iov_len: reply_header.len(),
                    },
                    libc::iovec {
                        iov_base: (&mut reply_buffer_out as *mut FuseReplyBufferOut).cast(),
                        iov_len: size_of::<FuseReplyBufferOut>(),
                    },
                ];
                (
                    unsafe {
                        MFChannelSendMessage(
                            self.raw.as_ptr(),
                            reply_iovecs.as_ptr(),
                            reply_iovecs.len(),
                        )
                    },
                    FUSE_OUT_HEADER_SIZE + size_of::<FuseReplyBufferOut>(),
                )
            }
            _ => (
                unsafe {
                    MFChannelSendMessage(
                        self.raw.as_ptr(),
                        ordinary_iovecs.as_ptr(),
                        ordinary_iovecs.len(),
                    )
                },
                ordinary_size,
            ),
        };
        Ok((sent, expected))
    }

    fn send_errno_reply(&self, unique: u64, errno: libc::c_int) -> io::Result<()> {
        let mut header = fuse_errno_reply_header(unique, errno);
        let iovec = libc::iovec {
            iov_base: header.as_mut_ptr().cast(),
            iov_len: header.len(),
        };
        let sent = unsafe { MFChannelSendMessage(self.raw.as_ptr(), &iovec, 1) };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }
        if usize::try_from(sent).ok() != Some(header.len()) {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "MFChannelSendMessage sent {sent} of {} error-reply bytes",
                    header.len()
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn interrupt(&self) -> io::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::from_raw_os_error(libc::ENODEV));
        }
        if unsafe { MFChannelInterrupt(self.raw.as_ptr()) } {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(crate) fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    pub(crate) fn terminal_errno(&self) -> Errno {
        self.mount_failure
            .lock()
            .as_ref()
            .map(MountFailure::terminal_errno)
            .unwrap_or(Errno::ENODEV)
    }

    pub(crate) fn mount_error(&self) -> Option<io::Error> {
        self.mount_failure
            .lock()
            .as_ref()
            .map(MountFailure::io_error)
    }
}

impl Drop for MacFuseChannel {
    fn drop(&mut self) {
        unsafe { MFRelease(self.raw.as_ptr()) };
    }
}

#[derive(Debug)]
struct Message(NonNull<c_void>);

impl Message {
    fn reply_buffer(&self) -> io::Result<Option<(*mut u8, usize)>> {
        let mut buffer = ptr::null_mut();
        let size = unsafe { MFMessageGetReplyBuffer(self.0.as_ptr(), &mut buffer) };
        if size < 0 {
            return Err(io::Error::last_os_error());
        }
        let size =
            usize::try_from(size).map_err(|_| io::Error::other("invalid reply buffer size"))?;
        if size == 0 {
            return Ok(None);
        }
        let buffer = NonNull::new(buffer.cast::<u8>())
            .ok_or_else(|| io::Error::other("MFMessageGetReplyBuffer returned NULL"))?;
        Ok(Some((buffer.as_ptr(), size)))
    }
}

impl Drop for Message {
    fn drop(&mut self) {
        unsafe { MFRelease(self.0.as_ptr()) };
    }
}

const FUSE_OUT_HEADER_SIZE: usize = 16;

#[repr(C)]
struct FuseReplyBufferOut {
    size: u32,
    flags: u32,
}

fn fuse_reply_succeeded(header: &[u8]) -> io::Result<bool> {
    let error = header
        .get(4..8)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "short FUSE reply header"))?;
    Ok(i32::from_ne_bytes(error.try_into().unwrap()) == 0)
}

fn fuse_errno_reply_header(unique: u64, errno: libc::c_int) -> [u8; FUSE_OUT_HEADER_SIZE] {
    let mut header = [0u8; FUSE_OUT_HEADER_SIZE];
    header[..4].copy_from_slice(&(FUSE_OUT_HEADER_SIZE as u32).to_ne_bytes());
    header[4..8].copy_from_slice(&(-errno.abs()).to_ne_bytes());
    header[8..16].copy_from_slice(&unique.to_ne_bytes());
    header
}

fn copy_reply_payload(
    destination: *mut u8,
    destination_size: usize,
    buffers: &[IoSlice<'_>],
) -> io::Result<usize> {
    if buffers.first().map(|buffer| buffer.len()) != Some(FUSE_OUT_HEADER_SIZE) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unexpected FUSE reply header size",
        ));
    }
    let payload_size = buffers.iter().skip(1).try_fold(0usize, |size, buffer| {
        size.checked_add(buffer.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "reply size overflow"))
    })?;
    if payload_size > destination_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("reply payload {payload_size} exceeds MFMessage buffer {destination_size}"),
        ));
    }
    let mut offset = 0usize;
    for buffer in buffers.iter().skip(1) {
        unsafe {
            ptr::copy_nonoverlapping(buffer.as_ptr(), destination.add(offset), buffer.len());
        }
        offset += buffer.len();
    }
    Ok(payload_size)
}

fn prepare_buffered_reply(
    destination: *mut u8,
    destination_size: usize,
    buffers: &[IoSlice<'_>],
    reply_header: &mut [u8; FUSE_OUT_HEADER_SIZE],
    reply_buffer_out: &mut FuseReplyBufferOut,
) -> io::Result<()> {
    let payload_size = copy_reply_payload(destination, destination_size, buffers)?;
    reply_header.copy_from_slice(buffers[0].as_ref());
    reply_header[..4].copy_from_slice(
        &u32::try_from(FUSE_OUT_HEADER_SIZE + size_of::<FuseReplyBufferOut>())
            .map_err(|_| io::Error::other("reply header size overflow"))?
            .to_ne_bytes(),
    );
    reply_buffer_out.size = u32::try_from(payload_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "reply payload exceeds u32"))?;
    reply_buffer_out.flags = 0;
    Ok(())
}

#[derive(Debug)]
pub(super) struct MountImpl {
    channel: Arc<MacFuseChannel>,
    fuse_channel: Option<FuseChannel>,
    mount_point: PathBuf,
}

impl MountImpl {
    pub(super) fn new(
        mount_point: &Path,
        options: &[MountOption],
        acl: SessionACL,
    ) -> io::Result<(Arc<MacFuseChannel>, Self)> {
        let mount_point_path = std::fs::canonicalize(mount_point)?;
        let mount_point = CString::new(mount_point_path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "mount point contains an interior NUL byte",
            )
        })?;
        preflight_macfuse_version(Path::new(MACFUSE_VERSION_PLIST))?;
        let (mut options, quiet) = mount_parameters(options, acl)?;
        if !options.is_empty() {
            options.push(',');
        }
        options.push_str("backend=fskit");
        if quiet {
            options.push_str(",quiet");
        }
        let options = CString::new(options).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "mount options contain an interior NUL byte",
            )
        })?;
        let program = CString::new("rust-fuse").unwrap();
        let option_flag = CString::new("-o").unwrap();
        let mut arguments = vec![
            program.as_ptr().cast_mut(),
            option_flag.as_ptr().cast_mut(),
            options.as_ptr().cast_mut(),
        ];
        let mut fuse_args = FuseArgs {
            argc: arguments.len() as libc::c_int,
            argv: arguments.as_mut_ptr(),
            allocated: 0,
        };
        let raw_fuse_channel = unsafe { fuse_mount(mount_point.as_ptr(), &mut fuse_args) };
        unsafe { fuse_opt_free_args(&mut fuse_args) };
        let mut fuse_channel = NonNull::new(raw_fuse_channel)
            .map(FuseChannel::new)
            .ok_or_else(|| io::Error::other("macFUSE fuse_mount returned NULL"))?;

        let mut raw_mf_channel = ptr::null_mut();
        let status = unsafe { fuse_darwin_chan_mfch(fuse_channel.as_ptr(), &mut raw_mf_channel) };
        if status != 0 || raw_mf_channel.is_null() {
            let errno = status
                .checked_neg()
                .filter(|value| *value > 0)
                .unwrap_or(libc::EIO);
            return Err(io::Error::from_raw_os_error(errno));
        }
        let channel = MacFuseChannel::from_borrowed(raw_mf_channel)?;
        fuse_channel.attach(channel.clone());

        Ok((
            channel.clone(),
            Self {
                channel,
                fuse_channel: Some(fuse_channel),
                mount_point: mount_point_path,
            },
        ))
    }

    pub(super) fn umount_impl(&mut self) -> io::Result<()> {
        let Some(fuse_channel) = self.fuse_channel.take() else {
            return Ok(());
        };
        let path_detached = match mountpoint_is_active(&self.mount_point) {
            Ok(active) => !active,
            Err(error) => {
                warn!(
                    "Could not determine whether {} is mounted before unmount: {error}",
                    self.mount_point.display()
                );
                false
            }
        };
        if !path_detached {
            let mount_point =
                CString::new(self.mount_point.as_os_str().as_bytes()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "mountpoint contains an interior NUL byte",
                    )
                })?;
            if let Err(error) = super::libc_umount(&mount_point)
                && error != nix::errno::Errno::EINVAL
                && error != nix::errno::Errno::ENOENT
            {
                warn!(
                    "POSIX unmount of {} failed before MFChannel teardown: {error}",
                    self.mount_point.display()
                );
            }
        }
        self.channel.closing.store(true, Ordering::Release);
        let _ = self.channel.interrupt();
        unsafe { fuse_darwin_chan_unmount(fuse_channel.as_ptr()) };
        if path_detached {
            return Ok(());
        }
        for _ in 0..100 {
            if unsafe { fuse_darwin_chan_not_mounted(fuse_channel.as_ptr()) } {
                return Ok(());
            }
            match mountpoint_is_active(&self.mount_point) {
                Ok(false) => return Ok(()),
                Ok(true) => {}
                Err(error) => warn!(
                    "Could not determine whether {} detached during unmount: {error}",
                    self.mount_point.display()
                ),
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "macFUSE did not finish unmounting within 10 seconds",
        ))
    }
}

fn mountpoint_is_active(mount_point: &Path) -> io::Result<bool> {
    let mut mounts = ptr::null_mut();
    let count = unsafe { getmntinfo_r_np(&mut mounts, libc::MNT_NOWAIT) };
    if count == 0 || mounts.is_null() {
        return Err(io::Error::last_os_error());
    }
    let entries = unsafe { std::slice::from_raw_parts(mounts, count as usize) };
    let active = entries.iter().any(|entry| {
        let mounted_on = unsafe { CStr::from_ptr(entry.f_mntonname.as_ptr()) };
        mounted_on.to_bytes() == mount_point.as_os_str().as_bytes()
    });
    unsafe { libc::free(mounts.cast()) };
    Ok(active)
}

fn mount_parameters(options: &[MountOption], acl: SessionACL) -> io::Result<(String, bool)> {
    let mut quiet = false;
    let mut kernel_options = Vec::new();
    for option in options {
        match option {
            MountOption::AutoUnmount => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "AutoUnmount is not supported by the direct MFMount transport",
                ));
            }
            MountOption::Subtype(_) => {}
            MountOption::CUSTOM(value) if value == "quiet" => quiet = true,
            MountOption::CUSTOM(value) if value == "backend=fskit" => {}
            MountOption::CUSTOM(value) if value.starts_with("backend=") => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{value} conflicts with the direct MFMount FSKit transport"),
                ));
            }
            option => {
                let value = option_to_string(option);
                if value.contains(',') {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("MFMount option contains an ambiguous comma: {value}"),
                    ));
                }
                kernel_options.push(value);
            }
        }
    }
    if let Some(acl) = acl.to_mount_option() {
        kernel_options.push(acl.to_owned());
    }
    Ok((kernel_options.join(","), quiet))
}

#[cfg(test)]
fn mount_result(result: i32) -> io::Result<()> {
    MountFailure::from_result(result).map_or(Ok(()), |failure| Err(failure.io_error()))
}

#[derive(Clone, Debug)]
struct MountFailure {
    result: i32,
    errno: Option<i32>,
    kind: io::ErrorKind,
    message: &'static str,
}

impl MountFailure {
    fn from_result(result: i32) -> Option<Self> {
        let (kind, message, errno) = match result {
            MF_MOUNT_SUCCESS => return None,
            MF_MOUNT_UNSUPPORTED_OS_VERSION => (
                io::ErrorKind::Unsupported,
                "MFMount does not support this macOS version",
                None,
            ),
            MF_MOUNT_HELPER_TOOLS_INSTALLATION_FAILED => (
                io::ErrorKind::Other,
                "MFMount could not install its required helper tools",
                None,
            ),
            MF_MOUNT_FILE_SYSTEM_EXTENSION_NOT_FOUND => (
                io::ErrorKind::NotFound,
                "MFMount could not find the macFUSE file system extension",
                None,
            ),
            MF_MOUNT_FILE_SYSTEM_EXTENSION_REQUIRES_APPROVAL => (
                io::ErrorKind::PermissionDenied,
                "the macFUSE file system extension requires approval in System Settings",
                None,
            ),
            MF_MOUNT_UNEXPECTED_FAILURE => {
                let error = io::Error::last_os_error();
                (
                    error.kind(),
                    "MFMount reported an unexpected failure",
                    error.raw_os_error(),
                )
            }
            _ => (
                io::ErrorKind::InvalidData,
                "MFMount returned an unknown result code",
                None,
            ),
        };
        Some(Self {
            result,
            errno,
            kind,
            message,
        })
    }

    fn io_error(&self) -> io::Error {
        let errno = self
            .errno
            .map(|errno| format!(", errno {errno}"))
            .unwrap_or_default();
        io::Error::new(
            self.kind,
            format!("{} (MFMount result {}{errno})", self.message, self.result),
        )
    }

    fn terminal_errno(&self) -> Errno {
        match self.result {
            MF_MOUNT_UNSUPPORTED_OS_VERSION => Errno::ENOTSUP,
            MF_MOUNT_FILE_SYSTEM_EXTENSION_REQUIRES_APPROVAL => Errno::EACCES,
            _ => Errno::EIO,
        }
    }
}

fn preflight_macfuse_version(version_plist: &Path) -> io::Result<()> {
    let plist = std::fs::read_to_string(version_plist).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("read {}: {error}", version_plist.display()),
        )
    })?;
    let installed_version =
        plist_string(&plist, "CFBundleShortVersionString").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} has no CFBundleShortVersionString",
                    version_plist.display()
                ),
            )
        })?;
    if installed_version != MACFUSE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "macFUSE {MACFUSE_VERSION} is required, found {installed_version} in {}",
                version_plist.display()
            ),
        ));
    }
    Ok(())
}

fn plist_string<'a>(plist: &'a str, key: &str) -> Option<&'a str> {
    let key = format!("<key>{key}</key>");
    let value = plist.split_once(&key)?.1;
    let value = value.split_once("<string>")?.1;
    value.split_once("</string>").map(|(value, _)| value.trim())
}

unsafe fn copy_body_buffers(
    body_buffers: *const libc::iovec,
    body_count: usize,
    body_size: usize,
    destination: &mut [u8],
) -> nix::Result<usize> {
    if body_size == 0 || body_size > destination.len() || body_count == 0 || body_buffers.is_null()
    {
        return Err(Errno::EIO);
    }

    let mut offset = 0usize;
    for index in 0..body_count {
        let body = unsafe { &*body_buffers.add(index) };
        let end = offset.checked_add(body.iov_len).ok_or(Errno::EIO)?;
        if end > body_size || (body.iov_base.is_null() && body.iov_len != 0) {
            return Err(Errno::EIO);
        }
        unsafe {
            ptr::copy_nonoverlapping(
                body.iov_base.cast::<u8>(),
                destination.as_mut_ptr().add(offset),
                body.iov_len,
            )
        };
        offset = end;
    }
    if offset != body_size {
        return Err(Errno::EIO);
    }
    Ok(offset)
}

fn fuse_message_unique(message: &[u8]) -> nix::Result<u64> {
    let unique = message.get(8..16).ok_or(Errno::EPROTO)?;
    Ok(u64::from_ne_bytes(
        unique.try_into().map_err(|_| Errno::EPROTO)?,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[derive(Debug)]
    struct DropRecord {
        id: usize,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DropRecord {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn no_reply_completion_releases_pending_message() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut pending = PendingMessages::new();
        pending
            .insert(
                42,
                DropRecord {
                    id: 1,
                    drops: drops.clone(),
                },
            )
            .unwrap();

        drop(pending.remove(42));

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(pending.remove(42).is_none());
    }

    #[test]
    fn duplicate_unique_rejection_preserves_original_message() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut pending = PendingMessages::new();
        pending
            .insert(
                42,
                DropRecord {
                    id: 1,
                    drops: drops.clone(),
                },
            )
            .unwrap();

        let rejected = pending
            .insert(
                42,
                DropRecord {
                    id: 2,
                    drops: drops.clone(),
                },
            )
            .unwrap_err();

        assert_eq!(rejected.id, 2);
        drop(rejected);
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let original = pending.remove(42).unwrap();
        assert_eq!(original.id, 1);
        drop(original);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn teardown_releases_all_pending_messages() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut pending = PendingMessages::new();
        for unique in 1..=3 {
            pending
                .insert(
                    unique,
                    DropRecord {
                        id: unique as usize,
                        drops: drops.clone(),
                    },
                )
                .unwrap();
        }

        pending.clear();

        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn mfmount_parameters_only_include_kernel_options() {
        let (options, quiet) = mount_parameters(
            &[
                MountOption::FSName("chevalier".to_owned()),
                MountOption::Subtype("ignored-by-mfmount".to_owned()),
                MountOption::RW,
                MountOption::CUSTOM("backend=fskit".to_owned()),
                MountOption::CUSTOM("quiet".to_owned()),
            ],
            SessionACL::RootAndOwner,
        )
        .unwrap();

        assert_eq!(options, "fsname=chevalier,rw,allow_other");
        assert!(quiet);
    }

    #[test]
    fn flattens_mfmessage_body_buffers() {
        let header = [1u8, 2, 3];
        let payload = [4u8, 5];
        let buffers = [
            libc::iovec {
                iov_base: header.as_ptr().cast_mut().cast(),
                iov_len: header.len(),
            },
            libc::iovec {
                iov_base: payload.as_ptr().cast_mut().cast(),
                iov_len: payload.len(),
            },
        ];
        let mut destination = [0u8; 5];

        let copied = unsafe {
            copy_body_buffers(
                buffers.as_ptr(),
                buffers.len(),
                destination.len(),
                &mut destination,
            )
        }
        .unwrap();

        assert_eq!(copied, destination.len());
        assert_eq!(destination, [1, 2, 3, 4, 5]);
    }

    #[test]
    fn flattens_mfmessage_with_more_than_two_body_buffers() {
        let first = [1u8, 2];
        let second = [3u8];
        let third = [4u8, 5];
        let buffers = [
            libc::iovec {
                iov_base: first.as_ptr().cast_mut().cast(),
                iov_len: first.len(),
            },
            libc::iovec {
                iov_base: second.as_ptr().cast_mut().cast(),
                iov_len: second.len(),
            },
            libc::iovec {
                iov_base: third.as_ptr().cast_mut().cast(),
                iov_len: third.len(),
            },
        ];
        let mut destination = [0u8; 5];

        let copied = unsafe {
            copy_body_buffers(
                buffers.as_ptr(),
                buffers.len(),
                destination.len(),
                &mut destination,
            )
        }
        .unwrap();

        assert_eq!(copied, destination.len());
        assert_eq!(destination, [1, 2, 3, 4, 5]);
    }

    #[test]
    fn rejects_inconsistent_mfmessage_body_size() {
        let body = [1u8, 2, 3];
        let buffers = [libc::iovec {
            iov_base: body.as_ptr().cast_mut().cast(),
            iov_len: body.len(),
        }];
        let mut destination = [0u8; 4];

        let error =
            unsafe { copy_body_buffers(buffers.as_ptr(), buffers.len(), 4, &mut destination) }
                .unwrap_err();

        assert_eq!(error, Errno::EIO);
    }

    #[test]
    fn extracts_fuse_message_unique() {
        let mut message = [0u8; 16];
        message[8..16].copy_from_slice(&42u64.to_ne_bytes());

        assert_eq!(fuse_message_unique(&message).unwrap(), 42);
        assert_eq!(
            fuse_message_unique(&message[..15]).unwrap_err(),
            Errno::EPROTO
        );
    }

    #[test]
    fn copies_data_reply_into_transport_buffer() {
        let header = [0u8; FUSE_OUT_HEADER_SIZE];
        let first = [1u8, 2];
        let second = [3u8, 4, 5];
        let buffers = [
            IoSlice::new(&header),
            IoSlice::new(&first),
            IoSlice::new(&second),
        ];
        let mut destination = [0u8; 8];

        let copied =
            copy_reply_payload(destination.as_mut_ptr(), destination.len(), &buffers).unwrap();

        assert_eq!(copied, 5);
        assert_eq!(&destination[..copied], &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn prepares_exact_macfuse_reply_buffer_message() {
        let mut header = [0u8; FUSE_OUT_HEADER_SIZE];
        header[..4].copy_from_slice(&21u32.to_ne_bytes());
        header[8..16].copy_from_slice(&42u64.to_ne_bytes());
        let payload = [1u8, 2, 3, 4, 5];
        let buffers = [IoSlice::new(&header), IoSlice::new(&payload)];
        let mut destination = [0u8; 8];
        let mut reply_header = [0u8; FUSE_OUT_HEADER_SIZE];
        let mut reply_buffer_out = FuseReplyBufferOut {
            size: u32::MAX,
            flags: u32::MAX,
        };

        prepare_buffered_reply(
            destination.as_mut_ptr(),
            destination.len(),
            &buffers,
            &mut reply_header,
            &mut reply_buffer_out,
        )
        .unwrap();

        assert_eq!(
            u32::from_ne_bytes(reply_header[..4].try_into().unwrap()),
            24
        );
        assert_eq!(
            i32::from_ne_bytes(reply_header[4..8].try_into().unwrap()),
            0
        );
        assert_eq!(
            u64::from_ne_bytes(reply_header[8..16].try_into().unwrap()),
            42
        );
        assert_eq!(reply_buffer_out.size, payload.len() as u32);
        assert_eq!(reply_buffer_out.flags, 0);
        assert_eq!(&destination[..payload.len()], &payload);
    }

    #[test]
    fn rejects_reply_payload_larger_than_transport_buffer() {
        let header = [0u8; FUSE_OUT_HEADER_SIZE];
        let payload = [1u8, 2, 3];
        let buffers = [IoSlice::new(&header), IoSlice::new(&payload)];
        let mut destination = [0u8; 2];

        let error =
            copy_reply_payload(destination.as_mut_ptr(), destination.len(), &buffers).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn recognizes_successful_and_error_replies() {
        let mut header = [0u8; FUSE_OUT_HEADER_SIZE];
        assert!(fuse_reply_succeeded(&header).unwrap());

        header[4..8].copy_from_slice(&(-libc::EIO).to_ne_bytes());
        assert!(!fuse_reply_succeeded(&header).unwrap());
    }

    #[test]
    fn builds_correlated_errno_reply() {
        let header = fuse_errno_reply_header(42, libc::EIO);

        assert_eq!(u32::from_ne_bytes(header[..4].try_into().unwrap()), 16);
        assert_eq!(
            i32::from_ne_bytes(header[4..8].try_into().unwrap()),
            -libc::EIO
        );
        assert_eq!(u64::from_ne_bytes(header[8..16].try_into().unwrap()), 42);
    }

    #[test]
    fn ordinary_directory_is_not_a_mountpoint() {
        let directory = tempfile::tempdir().unwrap();

        assert!(!mountpoint_is_active(directory.path()).unwrap());
    }

    #[test]
    fn mount_table_probe_finds_root_without_entering_the_mount() {
        assert!(mountpoint_is_active(Path::new("/")).unwrap());
    }

    #[test]
    fn maps_mfmount_setup_results() {
        assert_eq!(
            mount_result(MF_MOUNT_FILE_SYSTEM_EXTENSION_NOT_FOUND)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            mount_result(MF_MOUNT_FILE_SYSTEM_EXTENSION_REQUIRES_APPROVAL)
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            MountFailure::from_result(MF_MOUNT_FILE_SYSTEM_EXTENSION_NOT_FOUND)
                .unwrap()
                .terminal_errno(),
            Errno::EIO
        );
        assert_eq!(
            MountFailure::from_result(MF_MOUNT_FILE_SYSTEM_EXTENSION_REQUIRES_APPROVAL)
                .unwrap()
                .terminal_errno(),
            Errno::EACCES
        );
    }

    #[test]
    fn rejects_unsupported_or_ambiguous_mfmount_options() {
        assert_eq!(
            mount_parameters(&[MountOption::AutoUnmount], SessionACL::All)
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            mount_parameters(
                &[MountOption::CUSTOM("backend=kernel".to_owned())],
                SessionACL::Owner,
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            mount_parameters(
                &[MountOption::CUSTOM("rw,unexpected".to_owned())],
                SessionACL::Owner,
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn parses_exact_macfuse_package_version() {
        let plist = r#"<plist><dict>
            <key>CFBundleShortVersionString</key><string>5.3.3</string>
        </dict></plist>"#;

        assert_eq!(
            plist_string(plist, "CFBundleShortVersionString"),
            Some("5.3.3")
        );
    }

    #[test]
    fn rejects_a_different_installed_macfuse_version() {
        let version_plist = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            version_plist.path(),
            r#"<plist><dict>
                <key>CFBundleShortVersionString</key><string>5.3.2</string>
            </dict></plist>"#,
        )
        .unwrap();

        let error = preflight_macfuse_version(version_plist.path()).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("5.3.3 is required, found 5.3.2"));
    }
}
