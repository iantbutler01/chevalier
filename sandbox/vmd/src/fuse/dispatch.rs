//! Concurrent FUSE dispatch.
//!
//! `fuser::spawn_mount2` runs a single-threaded session loop: with the
//! filesystem handling operations inline, every op from the whole VM
//! serializes behind one network round trip at a time (~1/RTT ops/s for the
//! entire mount, with any slow op stalling all others). `SpawnedFuseFs` owns
//! the `fuser::Filesystem` impl and fans each operation out to the tokio
//! blocking pool, replying from the worker — the session loop only decodes
//! and dispatches. In-flight ops are bounded by a semaphore so a stat storm
//! cannot stampede the gateway.
//!
//! Kernel-side parallelism is raised to match in `init` (`max_background`
//! defaults to 12, which would throttle the whole exercise from above).

use std::ffi::{OsStr, OsString};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use fuser::{
    BsdFileFlags, FileHandle, Filesystem, INodeNo, KernelConfig, LockOwner, OpenFlags,
    RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow, WriteFlags,
};
use tokio::sync::Semaphore;

use super::fs::RemoteFuseFs;

/// Upper bound on concurrently executing FUSE ops per mount. The gateway
/// sustains far more, but each op holds a blocking-pool thread; 64 keeps a
/// dep-tree scan saturating the wire without monopolizing the pool.
const MAX_IN_FLIGHT_OPS: usize = 64;

pub struct SpawnedFuseFs {
    inner: Arc<RemoteFuseFs>,
    ops: Arc<Semaphore>,
}

impl SpawnedFuseFs {
    pub fn new(inner: RemoteFuseFs) -> Self {
        Self {
            inner: Arc::new(inner),
            ops: Arc::new(Semaphore::new(MAX_IN_FLIGHT_OPS)),
        }
    }

    pub fn inner(&self) -> &RemoteFuseFs {
        &self.inner
    }

    fn spawn(&self, op: impl FnOnce(&RemoteFuseFs) + Send + 'static) {
        let inner = Arc::clone(&self.inner);
        let ops = Arc::clone(&self.ops);
        let tokio = inner.tokio_handle();
        tokio.spawn_blocking(move || {
            // Blocking-pool threads may block on the runtime; a closed
            // semaphore can only happen at teardown, where dropping the op
            // (and its reply) makes the kernel fail the request cleanly.
            let Ok(_permit) = inner.tokio_handle().block_on(ops.acquire_owned()) else {
                return;
            };
            op(&inner);
        });
    }
}

impl Filesystem for SpawnedFuseFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
        // The kernel's default background-request window (12) would cap the
        // concurrency this whole module exists to provide.
        if let Err(rejected) = config.set_max_background(MAX_IN_FLIGHT_OPS as u16 * 2) {
            tracing::warn!(rejected, "vfs fuse kernel rejected max_background");
        }
        if let Err(rejected) = config.set_congestion_threshold(MAX_IN_FLIGHT_OPS as u16 + 32) {
            tracing::warn!(rejected, "vfs fuse kernel rejected congestion threshold");
        }
        self.inner.init_op(config)
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        // Purely in-memory; not worth a pool hop.
        self.inner.forget(ino, nlookup);
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name: OsString = name.to_owned();
        self.spawn(move |fs| fs.lookup(parent, &name, reply));
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        self.spawn(move |fs| fs.getattr(ino, fh, reply));
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        self.spawn(move |fs| fs.readlink(ino, reply));
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        self.spawn(move |fs| {
            fs.setattr(
                ino, mode, uid, gid, size, atime, mtime, ctime, fh, crtime, chgtime, bkuptime,
                flags, reply,
            );
        });
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        // Replies immediately without touching the network.
        self.inner.opendir(ino, flags, reply);
    }

    fn readdir(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, reply: ReplyDirectory) {
        self.spawn(move |fs| fs.readdir(ino, fh, offset, reply));
    }

    fn readdirplus(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, reply: ReplyDirectoryPlus) {
        self.spawn(move |fs| fs.readdirplus(ino, fh, offset, reply));
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        self.spawn(move |fs| fs.open(ino, flags, reply));
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        self.spawn(move |fs| fs.read(ino, fh, offset, size, flags, lock_owner, reply));
    }

    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        write_flags: WriteFlags,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let data = data.to_vec();
        self.spawn(move |fs| fs.write(ino, fh, offset, &data, write_flags, flags, lock_owner, reply));
    }

    fn flush(&self, _req: &Request, ino: INodeNo, fh: FileHandle, lock_owner: LockOwner, reply: ReplyEmpty) {
        self.spawn(move |fs| fs.flush(ino, fh, lock_owner, reply));
    }

    fn fsync(&self, _req: &Request, ino: INodeNo, fh: FileHandle, datasync: bool, reply: ReplyEmpty) {
        self.spawn(move |fs| fs.fsync(ino, fh, datasync, reply));
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        self.spawn(move |fs| fs.release(ino, fh, flags, lock_owner, flush, reply));
    }

    fn mkdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        let name: OsString = name.to_owned();
        self.spawn(move |fs| fs.mkdir(parent, &name, mode, umask, reply));
    }

    fn symlink(&self, _req: &Request, parent: INodeNo, link_name: &OsStr, target: &Path, reply: ReplyEntry) {
        let link_name: OsString = link_name.to_owned();
        let target = target.to_path_buf();
        self.spawn(move |fs| fs.symlink(parent, &link_name, &target, reply));
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name: OsString = name.to_owned();
        self.spawn(move |fs| fs.unlink(parent, &name, reply));
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name: OsString = name.to_owned();
        self.spawn(move |fs| fs.rmdir(parent, &name, reply));
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let name: OsString = name.to_owned();
        let newname: OsString = newname.to_owned();
        self.spawn(move |fs| fs.rename(parent, &name, newparent, &newname, flags, reply));
    }

    fn link(&self, _req: &Request, ino: INodeNo, newparent: INodeNo, newname: &OsStr, reply: ReplyEntry) {
        let newname: OsString = newname.to_owned();
        self.spawn(move |fs| fs.link(ino, newparent, &newname, reply));
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let name: OsString = name.to_owned();
        self.spawn(move |fs| fs.create(parent, &name, mode, umask, flags, reply));
    }
}
