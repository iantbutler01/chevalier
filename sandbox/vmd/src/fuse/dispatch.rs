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

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};

use fuser::{
    BsdFileFlags, FileHandle, Filesystem, INodeNo, InterruptResult, KernelConfig, LockNamespace,
    LockOwner, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen, ReplyWrite, Request,
    RequestId, TimeOrNow, WriteFlags,
};
use tokio::sync::Semaphore;

use super::fs::{LockWaitCancellation, RemoteFuseFs};

/// Upper bound on concurrently executing FUSE ops per mount. The gateway
/// sustains far more, but each op holds a blocking-pool thread; 64 keeps a
/// dep-tree scan saturating the wire without monopolizing the pool.
const MAX_IN_FLIGHT_OPS: usize = 64;
const MAX_IN_FLIGHT_BLOCKING_LOCKS: usize = 32;

#[derive(Default)]
struct LockWaitRegistry {
    waits: Mutex<HashMap<RequestId, LockWaitEntry>>,
}

struct LockWaitEntry {
    cancellation: Weak<LockWaitCancellation>,
    ino: INodeNo,
    lock_owner: LockOwner,
    namespace: LockNamespace,
}

impl LockWaitRegistry {
    fn register(
        self: &Arc<Self>,
        request_id: RequestId,
        ino: INodeNo,
        lock_owner: LockOwner,
        namespace: LockNamespace,
    ) -> LockWaitRegistration {
        let cancellation = Arc::new(LockWaitCancellation::new());
        self.waits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                request_id,
                LockWaitEntry {
                    cancellation: Arc::downgrade(&cancellation),
                    ino,
                    lock_owner,
                    namespace,
                },
            );
        LockWaitRegistration {
            request_id,
            cancellation,
            registry: Arc::downgrade(self),
        }
    }

    fn cancel(&self, request_id: RequestId) -> bool {
        let cancellation = {
            let mut waits = self
                .waits
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(cancellation) = waits
                .get(&request_id)
                .and_then(|entry| entry.cancellation.upgrade())
            else {
                waits.remove(&request_id);
                return false;
            };
            cancellation
        };
        cancellation.cancel()
    }

    fn cancel_owner(&self, ino: INodeNo, lock_owner: LockOwner, namespace: LockNamespace) {
        let cancellations = self
            .waits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|entry| {
                entry.ino == ino && entry.lock_owner == lock_owner && entry.namespace == namespace
            })
            .filter_map(|entry| entry.cancellation.upgrade())
            .collect::<Vec<_>>();
        for cancellation in cancellations {
            cancellation.cancel();
        }
    }
}

struct LockWaitRegistration {
    request_id: RequestId,
    cancellation: Arc<LockWaitCancellation>,
    registry: Weak<LockWaitRegistry>,
}

impl LockWaitRegistration {
    fn cancellation(&self) -> &LockWaitCancellation {
        &self.cancellation
    }
}

impl Drop for LockWaitRegistration {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry
                .waits
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.request_id);
        }
    }
}

pub struct SpawnedFuseFs {
    inner: Arc<RemoteFuseFs>,
    ops: Arc<Semaphore>,
    blocking_lock_ops: Arc<Semaphore>,
    lock_waits: Arc<LockWaitRegistry>,
}

impl SpawnedFuseFs {
    pub fn new(inner: RemoteFuseFs) -> Self {
        Self {
            inner: Arc::new(inner),
            ops: Arc::new(Semaphore::new(MAX_IN_FLIGHT_OPS)),
            blocking_lock_ops: Arc::new(Semaphore::new(MAX_IN_FLIGHT_BLOCKING_LOCKS)),
            lock_waits: Arc::new(LockWaitRegistry::default()),
        }
    }

    pub fn inner(&self) -> &RemoteFuseFs {
        &self.inner
    }

    fn spawn(&self, op: impl FnOnce(&RemoteFuseFs) + Send + 'static) {
        self.spawn_with_semaphore(Arc::clone(&self.ops), op);
    }

    fn spawn_blocking_lock(&self, op: impl FnOnce(&RemoteFuseFs) + Send + 'static) {
        self.spawn_with_semaphore(Arc::clone(&self.blocking_lock_ops), op);
    }

    fn spawn_with_semaphore(
        &self,
        ops: Arc<Semaphore>,
        op: impl FnOnce(&RemoteFuseFs) + Send + 'static,
    ) {
        let inner = Arc::clone(&self.inner);
        let tokio = inner.tokio_handle();
        spawn_bounded_blocking(&tokio, ops, move || op(&inner));
    }
}

fn spawn_bounded_blocking(
    tokio: &tokio::runtime::Handle,
    ops: Arc<Semaphore>,
    op: impl FnOnce() + Send + 'static,
) {
    let queued_at = Instant::now();
    tokio.spawn(async move {
        // Admission must happen before entering the blocking pool. Waiting for
        // this permit from inside spawn_blocking lets a FUSE request burst fill
        // every blocking thread with semaphore waiters, leaving no thread able
        // to run an admitted operation and release its permit.
        let Ok(permit) = ops.acquire_owned().await else {
            return;
        };
        let queue_wait = queued_at.elapsed();
        if queue_wait >= Duration::from_secs(1) {
            tracing::warn!(
                queue_wait_ms = queue_wait.as_millis(),
                "vfs fuse operation waited for dispatch admission"
            );
        }
        let _ = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let started_at = Instant::now();
            op();
            let operation_time = started_at.elapsed();
            if operation_time >= Duration::from_secs(1) {
                tracing::warn!(
                    operation_time_ms = operation_time.as_millis(),
                    "vfs fuse operation occupied a dispatch worker"
                );
            }
        })
        .await;
    });
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

    fn interrupt(&self, _req: &Request, request_id: RequestId) -> InterruptResult {
        if self.lock_waits.cancel(request_id) {
            InterruptResult::Handled
        } else {
            InterruptResult::Unhandled
        }
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

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        self.spawn(move |fs| fs.readdir(ino, fh, offset, reply));
    }

    fn readdirplus(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: ReplyDirectoryPlus,
    ) {
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
        self.spawn(move |fs| {
            fs.write(
                ino,
                fh,
                offset,
                &data,
                write_flags,
                flags,
                lock_owner,
                reply,
            )
        });
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        self.lock_waits
            .cancel_owner(ino, lock_owner, LockNamespace::Posix);
        self.spawn(move |fs| fs.flush(ino, fh, lock_owner, reply));
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
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
        if let Some(lock_owner) = lock_owner {
            self.lock_waits
                .cancel_owner(ino, lock_owner, LockNamespace::Flock);
        }
        self.spawn(move |fs| fs.release(ino, fh, flags, lock_owner, flush, reply));
    }

    fn getlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        lock_namespace: LockNamespace,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: ReplyLock,
    ) {
        self.spawn(move |fs| {
            fs.getlk(
                ino,
                fh,
                lock_owner,
                lock_namespace,
                start,
                end,
                typ,
                pid,
                reply,
            );
        });
    }

    fn setlk(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        lock_namespace: LockNamespace,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let wait = (sleep && typ != i32::from(libc::F_UNLCK)).then(|| {
            self.lock_waits
                .register(req.unique(), ino, lock_owner, lock_namespace)
        });
        let op = move |fs: &RemoteFuseFs| {
            fs.setlk(
                ino,
                fh,
                lock_owner,
                lock_namespace,
                start,
                end,
                typ,
                pid,
                sleep,
                wait.as_ref().map(LockWaitRegistration::cancellation),
                reply,
            );
        };
        if sleep {
            self.spawn_blocking_lock(op);
        } else {
            self.spawn(op);
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let name: OsString = name.to_owned();
        self.spawn(move |fs| fs.mkdir(parent, &name, mode, umask, reply));
    }

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
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

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use fuser::{INodeNo, LockNamespace, LockOwner, RequestId};
    use tokio::sync::Semaphore;

    use super::{LockWaitRegistry, spawn_bounded_blocking};

    #[test]
    fn semaphore_waiters_do_not_consume_blocking_pool_threads() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(2)
            .enable_all()
            .build()
            .expect("test runtime");
        let semaphore = Arc::new(Semaphore::new(1));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        spawn_bounded_blocking(&runtime.handle(), Arc::clone(&semaphore), move || {
            started_tx.send(()).expect("report admitted operation");
            release_rx.recv().expect("release admitted operation");
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first operation starts");

        for _ in 0..8 {
            spawn_bounded_blocking(&runtime.handle(), Arc::clone(&semaphore), || {});
        }

        let (probe_tx, probe_rx) = mpsc::channel();
        runtime.spawn_blocking(move || probe_tx.send(()).expect("report unrelated blocking work"));
        probe_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("semaphore waiters must not starve unrelated blocking work");

        release_tx.send(()).expect("release first operation");
    }

    #[test]
    fn lock_wait_registry_cancels_only_the_exact_live_request() {
        let registry = Arc::new(LockWaitRegistry::default());
        let request = registry.register(
            RequestId(41),
            INodeNo(7),
            LockOwner(8),
            LockNamespace::Posix,
        );
        let other = registry.register(
            RequestId(42),
            INodeNo(7),
            LockOwner(9),
            LockNamespace::Flock,
        );

        assert!(registry.cancel(RequestId(41)));
        assert!(!registry.cancel(RequestId(41)));
        assert!(!registry.cancel(RequestId(99)));
        registry.cancel_owner(INodeNo(7), LockOwner(9), LockNamespace::Flock);
        assert!(!registry.cancel(RequestId(42)));

        drop(request);
        drop(other);
        assert!(!registry.cancel(RequestId(41)));
        assert!(!registry.cancel(RequestId(42)));
    }
}
