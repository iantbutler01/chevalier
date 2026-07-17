use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use chevalier_sandbox::vfs::{
    VFS_OPERATION_LINK, VFS_OPERATION_SETATTR_MODE, VFS_OPERATION_SETATTR_SIZE,
    VFS_OPERATION_WRITE_THROUGH, VFS_SURFACE_KIND_VM_SHARED, VFS_SURFACE_KIND_VM_WORKSPACE,
    VfsDirEntry as RemoteDirEntry, VfsLeaseGrant as LeaseGrant, VfsMetadata as RemoteMetadata,
    VfsNamespaceMutation, scoped_vfs_path,
};
use fuser::{
    BsdFileFlags, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, InitFlags, KernelConfig, MountOption, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use sha2::{Digest, Sha256};
use tokio::runtime::Handle;

use super::cache::RemoteFuseCache;
use super::client::RemoteVfsClient;
use super::namespace::NamespaceJournal;
use super::write::WriteJournal;

const TTL: Duration = Duration::from_secs(1);
const ROOT_INO_RAW: u64 = 1;
const ROOT_INO: INodeNo = INodeNo(ROOT_INO_RAW);
const LARGE_FILE_BYTES: u64 = 10 * 1024 * 1024;
const MAX_OPEN_HANDLES: usize = 8_192;

type FuseResult<T> = std::result::Result<T, Errno>;

#[derive(Default)]
struct InodeTable {
    next: u64,
    path_to_ino: BTreeMap<String, INodeNo>,
    ino_to_path: HashMap<INodeNo, InodeRecord>,
}

struct InodeRecord {
    path: String,
    last_access: Instant,
    lookup_count: u64,
}

impl InodeTable {
    fn new() -> Self {
        let mut table = Self {
            next: ROOT_INO_RAW + 1,
            path_to_ino: BTreeMap::new(),
            ino_to_path: HashMap::new(),
        };
        table.path_to_ino.insert(String::new(), ROOT_INO);
        table.ino_to_path.insert(
            ROOT_INO,
            InodeRecord {
                path: String::new(),
                last_access: Instant::now(),
                lookup_count: u64::MAX,
            },
        );
        table
    }

    fn ensure(&mut self, path: &str) -> INodeNo {
        if let Some(ino) = self.path_to_ino.get(path) {
            if let Some(record) = self.ino_to_path.get_mut(ino) {
                record.last_access = Instant::now();
            }
            return *ino;
        }
        let ino = INodeNo(self.next);
        self.next += 1;
        self.path_to_ino.insert(path.to_string(), ino);
        self.ino_to_path.insert(
            ino,
            InodeRecord {
                path: path.to_string(),
                last_access: Instant::now(),
                lookup_count: 0,
            },
        );
        ino
    }

    fn lookup(&mut self, path: &str) -> INodeNo {
        let ino = self.ensure(path);
        if let Some(record) = self.ino_to_path.get_mut(&ino) {
            record.lookup_count = record.lookup_count.saturating_add(1);
        }
        ino
    }

    fn path(&mut self, ino: INodeNo) -> Option<String> {
        let record = self.ino_to_path.get_mut(&ino)?;
        record.last_access = Instant::now();
        Some(record.path.clone())
    }

    fn forget(&mut self, ino: INodeNo, nlookup: u64) {
        if ino == ROOT_INO {
            return;
        }
        let Some(record) = self.ino_to_path.get_mut(&ino) else {
            return;
        };
        record.lookup_count = record.lookup_count.saturating_sub(nlookup);
        if record.lookup_count == 0 {
            let path = record.path.clone();
            self.ino_to_path.remove(&ino);
            if self.path_to_ino.get(path.as_str()) == Some(&ino) {
                self.path_to_ino.remove(path.as_str());
            }
        }
    }

    fn detach_exact(&mut self, path: &str) {
        let Some(ino) = self.path_to_ino.remove(path) else {
            return;
        };
        self.ino_to_path.remove(&ino);
    }

    fn subtree_entries(&self, path: &str) -> Vec<(String, INodeNo)> {
        let prefix = format!("{path}/");
        let mut entries = self
            .path_to_ino
            .get(path)
            .map(|ino| vec![(path.to_string(), *ino)])
            .unwrap_or_default();
        entries.extend(
            self.path_to_ino
                .range(prefix.clone()..)
                .take_while(|(candidate, _)| candidate.starts_with(&prefix))
                .map(|(candidate, ino)| (candidate.clone(), *ino)),
        );
        entries
    }

    fn detach_subtree(&mut self, path: &str) {
        for (candidate, ino) in self.subtree_entries(path) {
            self.path_to_ino.remove(candidate.as_str());
            self.ino_to_path.remove(&ino);
        }
    }

    fn rename_path(&mut self, from: &str, to: &str) {
        self.detach_subtree(to);
        for (old_path, ino) in self.subtree_entries(from) {
            let suffix = old_path.strip_prefix(from).unwrap_or_default();
            let new_path = format!("{to}{suffix}");
            self.path_to_ino.remove(old_path.as_str());
            self.path_to_ino.insert(new_path.clone(), ino);
            if let Some(record) = self.ino_to_path.get_mut(&ino) {
                record.path = new_path;
                record.last_access = Instant::now();
            }
        }
    }
}

struct HandleTable {
    next: u64,
    files: HashMap<u64, FileState>,
}

impl Default for HandleTable {
    fn default() -> Self {
        Self {
            next: 1,
            files: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct FileState {
    path: String,
    buffer: Vec<u8>,
    executable: bool,
    dirty: bool,
    loaded: bool,
    base_content_hash: Option<String>,
    revision: u64,
}

pub struct RemoteFuseFs {
    client: RemoteVfsClient,
    cache: RemoteFuseCache,
    inodes: Mutex<InodeTable>,
    handles: Mutex<HandleTable>,
    namespace: Option<NamespaceJournal>,
    writes: Option<WriteJournal>,
    read_only: bool,
    scope_path: String,
    tokio: Handle,
    uid: u32,
    gid: u32,
}

impl RemoteFuseFs {
    pub fn new(client: RemoteVfsClient, read_only: bool, scope_path: &str, tokio: Handle) -> Self {
        Self {
            client,
            cache: RemoteFuseCache::default(),
            inodes: Mutex::new(InodeTable::new()),
            handles: Mutex::new(HandleTable::default()),
            namespace: None,
            writes: None,
            read_only,
            scope_path: scope_path.trim_matches('/').to_string(),
            tokio,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        }
    }

    pub fn new_with_namespace_journal(
        client: RemoteVfsClient,
        read_only: bool,
        scope_path: &str,
        journal_path: &Path,
        tokio: Handle,
    ) -> Result<Self> {
        let namespace = if read_only {
            None
        } else {
            Some(NamespaceJournal::open(
                client.clone(),
                scope_path,
                journal_path,
                tokio.clone(),
            )?)
        };
        let writes = if read_only {
            None
        } else {
            Some(WriteJournal::open(
                client.clone(),
                scope_path,
                journal_path.with_extension("writes.jsonl").as_path(),
                tokio.clone(),
            )?)
        };
        Ok(Self {
            client,
            cache: RemoteFuseCache::default(),
            inodes: Mutex::new(InodeTable::new()),
            handles: Mutex::new(HandleTable::default()),
            namespace,
            writes,
            read_only,
            scope_path: scope_path.trim_matches('/').to_string(),
            tokio,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        })
    }

    pub fn mount_options(&self, tag: &str) -> fuser::Config {
        let mut mount_options = vec![
            MountOption::FSName(tag.to_string()),
            MountOption::Subtype("chevalier-vfs".to_string()),
            MountOption::DefaultPermissions,
        ];
        if self.read_only {
            mount_options.push(MountOption::RO);
        } else {
            mount_options.push(MountOption::RW);
        }
        let mut config = fuser::Config::default();
        config.mount_options = mount_options;
        config.n_threads = Some(4);
        config.clone_fd = true;
        config
    }

    fn requested_init_capabilities(&self) -> InitFlags {
        Self::requested_init_capabilities_for(self.read_only)
    }

    fn path_for_ino(&self, ino: INodeNo) -> FuseResult<String> {
        self.lock_inodes()?.path(ino).ok_or(Errno::ENOENT)
    }

    fn ensure_ino(&self, path: &str) -> INodeNo {
        self.lock_inodes()
            .map(|mut inodes| inodes.ensure(path))
            .unwrap_or(ROOT_INO)
    }

    fn lookup_ino(&self, path: &str) -> INodeNo {
        self.lock_inodes()
            .map(|mut inodes| inodes.lookup(path))
            .unwrap_or(ROOT_INO)
    }

    fn detach_inode_path(&self, path: &str) {
        if let Ok(mut inodes) = self.lock_inodes() {
            inodes.detach_exact(path);
        }
    }

    fn rename_inode_path(&self, from: &str, to: &str) {
        if let Ok(mut inodes) = self.lock_inodes() {
            inodes.rename_path(from, to);
        }
        if let Ok(mut handles) = self.lock_handles() {
            let prefix = format!("{from}/");
            for state in handles.files.values_mut() {
                if state.path == from || state.path.starts_with(&prefix) {
                    let suffix = state.path.strip_prefix(from).unwrap_or_default();
                    state.path = format!("{to}{suffix}");
                }
            }
        }
    }

    fn attr_for_path(&self, path: &str, metadata: &RemoteMetadata, lookup: bool) -> FileAttr {
        let ino = if lookup {
            self.lookup_ino(path)
        } else {
            self.ensure_ino(path)
        };
        let kind = file_type_for_kind(&metadata.kind);
        let perm = match kind {
            FileType::Directory => {
                if self.read_only {
                    0o555
                } else {
                    0o755
                }
            }
            FileType::Symlink => 0o777,
            _ if self.read_only && metadata.executable => 0o555,
            _ if self.read_only => 0o444,
            _ if metadata.executable => 0o755,
            _ => 0o644,
        };
        let mtime = metadata
            .updated_at
            .map(|value| value.into())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        FileAttr {
            ino,
            size: metadata.size_bytes,
            blocks: metadata.size_bytes.div_ceil(512),
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind,
            perm,
            nlink: if matches!(kind, FileType::Directory) {
                2
            } else {
                1
            },
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn root_attr(&self) -> FileAttr {
        FileAttr {
            ino: ROOT_INO,
            size: 0,
            blocks: 0,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            kind: FileType::Directory,
            perm: if self.read_only { 0o555 } else { 0o755 },
            nlink: 2,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn child_path(parent: &str, name: &OsStr) -> FuseResult<String> {
        let segment = name.to_str().ok_or(Errno::EINVAL)?;
        Ok(if parent.is_empty() {
            segment.to_string()
        } else {
            format!("{parent}/{segment}")
        })
    }

    fn parent_path(path: &str) -> String {
        path.rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
            .unwrap_or_default()
    }

    fn dir_entries(&self, path: &str) -> FuseResult<Vec<RemoteDirEntry>> {
        self.flush_namespace()?;
        if let Some(entries) = self.cache.get_dir(path) {
            return Ok(entries);
        }
        let entries = self
            .tokio
            .block_on(self.client.list_dir(path))
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        self.cache.put_dir(path, entries.clone());
        Ok(entries)
    }

    fn stat_path(&self, path: &str) -> FuseResult<Option<RemoteMetadata>> {
        self.flush_namespace()?;
        if let Some(metadata) = self.open_handle_metadata(path)? {
            return Ok(Some(metadata));
        }
        if let Some(metadata) = self.cache.get_metadata(path) {
            return Ok(Some(metadata));
        }
        if self
            .writes
            .as_ref()
            .is_some_and(|writes| writes.has_pending_path(path))
        {
            self.flush_writes()?;
        }
        let metadata = self
            .tokio
            .block_on(self.client.stat(path))
            .map_err(|_| Errno::EIO)?;
        if let Some(metadata) = metadata.as_ref() {
            self.cache.put_metadata(path, metadata.clone());
        }
        Ok(metadata)
    }

    fn open_handle_metadata(&self, path: &str) -> FuseResult<Option<RemoteMetadata>> {
        let handles = self.lock_handles()?;
        let Some(state) = handles
            .files
            .values()
            .find(|state| state.path == path && state.dirty)
        else {
            return Ok(None);
        };
        Ok(Some(RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: state.buffer.len() as u64,
            link_target: None,
            content_hash: Some(content_hash_for_bytes(&state.buffer)),
            executable: state.executable,
            updated_at: None,
        }))
    }

    fn read_bytes(&self, path: &str, offset: u64, size: u32) -> FuseResult<Vec<u8>> {
        let metadata = self.stat_path(path)?.ok_or(Errno::ENOENT)?;
        if let Some(bytes) = self.cache.get_file_matching(path, &metadata) {
            let start = (offset as usize).min(bytes.len());
            let end = start.saturating_add(size as usize).min(bytes.len());
            return Ok(bytes[start..end].to_vec());
        }

        let bytes = if metadata.size_bytes > LARGE_FILE_BYTES {
            self.tokio
                .block_on(self.client.read_file_range(path, offset, size as u64))
                .map_err(|_| Errno::EIO)?
        } else {
            self.tokio
                .block_on(self.client.read_file_raw(path))
                .map_err(|_| Errno::EIO)?
        };

        if metadata.size_bytes <= LARGE_FILE_BYTES {
            self.cache.put_file(path, bytes.clone(), Some(metadata));
            let start = (offset as usize).min(bytes.len());
            let end = start.saturating_add(size as usize).min(bytes.len());
            return Ok(bytes[start..end].to_vec());
        }

        Ok(bytes)
    }

    fn next_handle(
        &self,
        path: &str,
        initial: Vec<u8>,
        loaded: bool,
        base_content_hash: Option<String>,
        executable: bool,
    ) -> FuseResult<u64> {
        let mut handles = self.lock_handles()?;
        if handles.files.len() >= MAX_OPEN_HANDLES {
            return Err(Errno::EMFILE);
        }
        let fh = handles.next;
        handles.next += 1;
        handles.files.insert(
            fh,
            FileState {
                path: path.to_string(),
                buffer: initial,
                executable,
                dirty: false,
                loaded,
                base_content_hash,
                revision: 0,
            },
        );
        Ok(fh)
    }

    fn flush_handle(&self, fh: u64) -> FuseResult<()> {
        if self.read_only {
            return Ok(());
        }
        let state = {
            let handles = self.lock_handles()?;
            handles.files.get(&fh).cloned().ok_or(Errno::ENOENT)?
        };
        if !state.dirty {
            return Ok(());
        }

        self.flush_namespace()?;
        let next_content_hash = content_hash_for_bytes(&state.buffer);
        if state.executable {
            self.flush_writes()?;
            let lease = self
                .tokio
                .block_on(self.client.acquire_lease(
                    &state.path,
                    1,
                    "flush executable vfs fuse write",
                ))
                .map_err(|_| Errno::EIO)?;
            let surface = self.surface_kind_for_path(&state.path);
            let result = self.tokio.block_on(self.client.write_file(
                &state.path,
                &state.buffer,
                true,
                &lease,
                surface,
                VFS_OPERATION_WRITE_THROUGH,
                state.base_content_hash.as_deref(),
            ));
            let _ = self.tokio.block_on(self.client.release_lease(&lease));
            result.map_err(|_| Errno::EIO)?;
        } else {
            self.writes
                .as_ref()
                .ok_or(Errno::EIO)?
                .enqueue(
                    state.path.as_str(),
                    state.buffer.as_slice(),
                    state.base_content_hash.clone(),
                )
                .map_err(|_| Errno::EIO)?;
        }

        self.cache.invalidate(&state.path);
        self.cache.put_file(
            &state.path,
            state.buffer.clone(),
            Some(RemoteMetadata {
                kind: "file".to_string(),
                size_bytes: state.buffer.len() as u64,
                link_target: None,
                content_hash: Some(next_content_hash.clone()),
                executable: state.executable,
                updated_at: None,
            }),
        );
        if let Some(handle) = self.lock_handles()?.files.get_mut(&fh) {
            handle.loaded = true;
            handle.base_content_hash = Some(next_content_hash);
            if handle.revision == state.revision {
                handle.dirty = false;
            }
        }
        Ok(())
    }

    fn flush_handle_immediate(&self, fh: u64) -> FuseResult<()> {
        self.flush_handle(fh)?;
        self.flush_writes()
    }

    fn flush_handles_for_path(&self, path: &str) -> FuseResult<()> {
        let handles = self
            .lock_handles()?
            .files
            .iter()
            .filter(|(_, state)| state.path == path && state.dirty)
            .map(|(fh, _)| *fh)
            .collect::<Vec<_>>();
        for fh in handles {
            self.flush_handle(fh)?;
        }
        self.flush_writes()?;
        Ok(())
    }

    fn flush_handles_for_subtree(&self, path: &str) -> FuseResult<()> {
        let prefix = format!("{path}/");
        let handles = self
            .lock_handles()?
            .files
            .iter()
            .filter(|(_, state)| {
                state.dirty && (state.path == path || state.path.starts_with(&prefix))
            })
            .map(|(fh, _)| *fh)
            .collect::<Vec<_>>();
        for fh in handles {
            self.flush_handle(fh)?;
        }
        self.flush_writes()?;
        Ok(())
    }

    fn resize_path_immediate(&self, path: &str, size: u64) -> FuseResult<FileAttr> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let prior = self.stat_path(path)?.ok_or(Errno::ENOENT)?;
        let mut bytes = self
            .tokio
            .block_on(self.client.read_file_raw(path))
            .map_err(|_| Errno::EIO)?;
        bytes.resize(size as usize, 0);
        let lease = self
            .tokio
            .block_on(self.client.acquire_lease(path, 1, "resize vfs fuse file"))
            .map_err(|_| Errno::EIO)?;
        let surface = self.surface_kind_for_path(path);
        let write_result = self.tokio.block_on(self.client.write_file(
            path,
            &bytes,
            prior.executable,
            &lease,
            surface,
            VFS_OPERATION_SETATTR_SIZE,
            prior.content_hash.as_deref(),
        ));
        let _ = self.tokio.block_on(self.client.release_lease(&lease));
        write_result.map_err(|_| Errno::EIO)?;
        self.cache.invalidate(path);
        let metadata = RemoteMetadata {
            kind: "file".to_string(),
            size_bytes: size,
            link_target: None,
            content_hash: Some(content_hash_for_bytes(&bytes)),
            executable: prior.executable,
            updated_at: None,
        };
        Ok(self.attr_for_path(path, &metadata, false))
    }

    fn set_executable_path_immediate(&self, path: &str, executable: bool) -> FuseResult<FileAttr> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let mut metadata = self.stat_path(path)?.ok_or(Errno::ENOENT)?;
        if metadata.kind != "file" || metadata.executable == executable {
            return Ok(self.attr_for_path(path, &metadata, false));
        }
        let bytes = self
            .tokio
            .block_on(self.client.read_file_raw(path))
            .map_err(|_| Errno::EIO)?;
        let lease = self
            .tokio
            .block_on(
                self.client
                    .acquire_lease(path, 1, "change vfs fuse executable mode"),
            )
            .map_err(|_| Errno::EIO)?;
        let surface = self.surface_kind_for_path(path);
        let write_result = self.tokio.block_on(self.client.write_file(
            path,
            &bytes,
            executable,
            &lease,
            surface,
            VFS_OPERATION_SETATTR_MODE,
            metadata.content_hash.as_deref(),
        ));
        let _ = self.tokio.block_on(self.client.release_lease(&lease));
        write_result.map_err(|_| Errno::EIO)?;
        self.cache.invalidate(path);
        metadata.executable = executable;
        Ok(self.attr_for_path(path, &metadata, false))
    }

    fn mutate_namespace<F>(&self, path: &str, op: F) -> FuseResult<()>
    where
        F: FnOnce(&LeaseGrant, &'static str) -> Result<()>,
    {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let surface = self.surface_kind_for_path(path);
        let lease = self
            .tokio
            .block_on(
                self.client
                    .acquire_lease(path, 1, "apply vfs namespace mutation"),
            )
            .map_err(|_| Errno::EIO)?;
        let result = op(&lease, surface);
        let _ = self.tokio.block_on(self.client.release_lease(&lease));
        result.map_err(|_| Errno::EIO)?;
        self.cache.invalidate(path);
        Ok(())
    }

    fn enqueue_namespace(&self, mutation: VfsNamespaceMutation) -> FuseResult<()> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        if matches!(
            &mutation,
            VfsNamespaceMutation::DeleteFile { .. }
                | VfsNamespaceMutation::RemoveDirectory { .. }
                | VfsNamespaceMutation::Rename { .. }
        ) {
            self.flush_writes()?;
        }
        let namespace = self.namespace.as_ref().ok_or(Errno::EIO)?;
        namespace.enqueue(mutation).map_err(|_| Errno::EIO)
    }

    fn flush_namespace(&self) -> FuseResult<()> {
        match self.namespace.as_ref() {
            Some(namespace) => namespace.flush().map_err(|_| Errno::EIO),
            None => Ok(()),
        }
    }

    fn flush_writes(&self) -> FuseResult<()> {
        match self.writes.as_ref() {
            Some(writes) => writes.flush().map_err(|_| Errno::EIO),
            None => Ok(()),
        }
    }

    fn ensure_handle_loaded(&self, fh: u64) -> FuseResult<()> {
        let state = {
            let handles = self.lock_handles()?;
            handles.files.get(&fh).cloned().ok_or(Errno::ENOENT)?
        };
        if state.loaded {
            return Ok(());
        }
        let bytes = self
            .tokio
            .block_on(self.client.read_file_raw(&state.path))
            .map_err(|_| Errno::EIO)?;
        let mut handles = self.lock_handles()?;
        let handle = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
        if handle.loaded || handle.dirty || handle.revision != state.revision {
            return Ok(());
        }
        handle.buffer = bytes;
        handle.loaded = true;
        Ok(())
    }

    fn scoped_path(&self, path: &str) -> String {
        scoped_vfs_path(self.scope_path.as_str(), path)
    }

    fn surface_kind_for_path(&self, path: &str) -> &'static str {
        Self::surface_kind_for_scoped_path(self.scoped_path(path).as_str())
    }

    fn surface_kind_for_scoped_path(path: &str) -> &'static str {
        if path.contains("/shared") {
            VFS_SURFACE_KIND_VM_SHARED
        } else {
            VFS_SURFACE_KIND_VM_WORKSPACE
        }
    }

    fn lock_inodes(&self) -> FuseResult<MutexGuard<'_, InodeTable>> {
        self.inodes.lock().map_err(|_| Errno::EIO)
    }

    fn lock_handles(&self) -> FuseResult<MutexGuard<'_, HandleTable>> {
        self.handles.lock().map_err(|_| Errno::EIO)
    }
}

#[cfg(test)]
fn content_hash_conflicts(base: Option<&str>, current: Option<&str>) -> bool {
    matches!((base, current), (Some(base), Some(current)) if base != current)
}

fn content_hash_for_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(hasher.finalize().as_ref())
}

fn file_type_for_kind(kind: &str) -> FileType {
    match kind {
        "directory" => FileType::Directory,
        "symlink" => FileType::Symlink,
        _ => FileType::RegularFile,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

impl RemoteFuseFs {
    fn requested_init_capabilities_for(read_only: bool) -> InitFlags {
        if read_only {
            InitFlags::FUSE_AUTO_INVAL_DATA
        } else {
            InitFlags::FUSE_WRITEBACK_CACHE | InitFlags::FUSE_AUTO_INVAL_DATA
        }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use fuser::InitFlags;

    use super::{
        InodeTable, ROOT_INO, RemoteFuseFs, content_hash_conflicts, content_hash_for_bytes,
    };

    #[test]
    fn surface_kind_uses_scoped_path_not_mount_relative_path() {
        assert_eq!(
            RemoteFuseFs::surface_kind_for_scoped_path(
                "conversations/11111111-1111-1111-1111-111111111111/shared/note.txt"
            ),
            chevalier_sandbox::vfs::VFS_SURFACE_KIND_VM_SHARED
        );
        assert_eq!(
            RemoteFuseFs::surface_kind_for_scoped_path(
                "conversations/11111111-1111-1111-1111-111111111111/0001_assistant/mount/note.txt"
            ),
            chevalier_sandbox::vfs::VFS_SURFACE_KIND_VM_WORKSPACE
        );
    }

    #[test]
    fn mounts_request_automatic_data_invalidation() {
        assert_eq!(
            RemoteFuseFs::requested_init_capabilities_for(false),
            InitFlags::FUSE_WRITEBACK_CACHE | InitFlags::FUSE_AUTO_INVAL_DATA
        );
        assert_eq!(
            RemoteFuseFs::requested_init_capabilities_for(true),
            InitFlags::FUSE_AUTO_INVAL_DATA
        );
    }

    #[test]
    fn inode_table_keeps_live_lookups_past_the_previous_capacity_boundary() {
        let mut table = InodeTable::new();
        let first = table.lookup("package-0");
        for index in 1..=70_000 {
            table.ensure(format!("package-{index}").as_str());
        }

        assert_eq!(table.path(first).as_deref(), Some("package-0"));
        assert_eq!(table.path(ROOT_INO).as_deref(), Some(""));

        table.forget(first, 1);
        assert!(table.path(first).is_none());
        assert_eq!(table.path(ROOT_INO).as_deref(), Some(""));
    }

    #[test]
    fn inode_table_tracks_directory_renames_and_detaches_deleted_subtrees() {
        let mut table = InodeTable::new();
        let directory = table.lookup("node_modules/pkg_tmp");
        let child = table.lookup("node_modules/pkg_tmp/index.js");

        table.rename_path("node_modules/pkg_tmp", "node_modules/pkg");

        assert_eq!(table.path(directory).as_deref(), Some("node_modules/pkg"));
        assert_eq!(
            table.path(child).as_deref(),
            Some("node_modules/pkg/index.js")
        );

        table.detach_subtree("node_modules/pkg");
        assert!(table.path(directory).is_none());
        assert!(table.path(child).is_none());
    }

    #[test]
    fn inode_table_exact_detach_does_not_scan_or_remove_neighboring_paths() {
        let mut table = InodeTable::new();
        let removed = table.lookup("node_modules/pkg/index.js");
        let neighbor = table.lookup("node_modules/pkg-extra/index.js");

        table.detach_exact("node_modules/pkg/index.js");

        assert!(table.path(removed).is_none());
        assert_eq!(
            table.path(neighbor).as_deref(),
            Some("node_modules/pkg-extra/index.js")
        );
    }

    #[test]
    fn content_hash_conflict_requires_two_different_known_hashes() {
        assert!(content_hash_conflicts(Some("base"), Some("current")));
        assert!(!content_hash_conflicts(Some("same"), Some("same")));
        assert!(!content_hash_conflicts(None, Some("current")));
        assert!(!content_hash_conflicts(Some("base"), None));
        assert!(!content_hash_conflicts(None, None));
    }

    #[test]
    fn content_hash_for_bytes_matches_sha256_hex() {
        assert_eq!(
            content_hash_for_bytes(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}

impl Filesystem for RemoteFuseFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
        let requested = self.requested_init_capabilities();
        if requested.is_empty() {
            return Ok(());
        }
        if let Err(unsupported) = config.add_capabilities(requested) {
            tracing::warn!(
                ?unsupported,
                "vfs fuse kernel did not accept requested cache capabilities"
            );
        }
        Ok(())
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        if let Ok(mut inodes) = self.lock_inodes() {
            inodes.forget(ino, nlookup);
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let result: FuseResult<FileAttr> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let child_path = Self::child_path(parent_path.as_str(), name)?;
            let Some(metadata) = self.stat_path(&child_path)? else {
                return Err(Errno::ENOENT);
            };
            Ok(self.attr_for_path(&child_path, &metadata, true))
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(err),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        if ino == ROOT_INO {
            reply.attr(&TTL, &self.root_attr());
            return;
        }
        let result: FuseResult<FileAttr> = (|| {
            let path = self.path_for_ino(ino)?;
            let metadata = self.stat_path(&path)?.ok_or(Errno::ENOENT)?;
            Ok(self.attr_for_path(&path, &metadata, false))
        })();
        match result {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(err) => reply.error(err),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let result: FuseResult<Vec<u8>> = (|| {
            let path = self.path_for_ino(ino)?;
            let metadata = self.stat_path(&path)?.ok_or(Errno::ENOENT)?;
            if metadata.kind != "symlink" {
                return Err(Errno::EINVAL);
            }
            let target = metadata.link_target.ok_or(Errno::EINVAL)?;
            Ok(target.into_bytes())
        })();
        match result {
            Ok(bytes) => reply.data(&bytes),
            Err(err) => reply.error(err),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let result: FuseResult<FileAttr> = (|| {
            if ino == ROOT_INO {
                if size.is_some() {
                    return Err(Errno::EISDIR);
                }
                return Ok(self.root_attr());
            }

            if let Some(fh) = fh {
                let executable = mode.map(|value| value & 0o111 != 0);
                if size.is_some() || executable.is_some() {
                    self.ensure_handle_loaded(fh.0)?;
                    let mut handles = self.lock_handles()?;
                    let state = handles.files.get_mut(&fh.0).ok_or(Errno::ENOENT)?;
                    if let Some(size) = size {
                        state.buffer.resize(size as usize, 0);
                    }
                    if let Some(executable) = executable {
                        state.executable = executable;
                    }
                    state.dirty = true;
                    state.loaded = true;
                    state.revision = state.revision.saturating_add(1);
                }
                let handle_state = {
                    let handles = self.lock_handles()?;
                    handles.files.get(&fh.0).cloned()
                };
                if let Some(state) = handle_state {
                    if state.dirty {
                        return Ok(self.attr_for_path(
                            &state.path,
                            &RemoteMetadata {
                                kind: "file".to_string(),
                                size_bytes: state.buffer.len() as u64,
                                link_target: None,
                                content_hash: state.base_content_hash,
                                executable: state.executable,
                                updated_at: None,
                            },
                            false,
                        ));
                    }
                    let metadata = self.stat_path(&state.path)?.ok_or(Errno::ENOENT)?;
                    return Ok(self.attr_for_path(&state.path, &metadata, false));
                }
            }

            let path = self.path_for_ino(ino)?;
            if let Some(size) = size {
                let attr = self.resize_path_immediate(&path, size)?;
                if mode.is_none() {
                    return Ok(attr);
                }
            }
            if let Some(mode) = mode {
                return self.set_executable_path_immediate(&path, mode & 0o111 != 0);
            }
            let metadata = self.stat_path(&path)?.ok_or(Errno::ENOENT)?;
            Ok(self.attr_for_path(&path, &metadata, false))
        })();
        match result {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(err) => reply.error(err),
        }
    }

    fn opendir(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let result: FuseResult<()> = (|| {
            let path = self.path_for_ino(ino)?;
            let mut entries: Vec<(INodeNo, FileType, String)> = vec![
                (ino, FileType::Directory, ".".to_string()),
                (
                    self.ensure_ino(Self::parent_path(&path).as_str()),
                    FileType::Directory,
                    "..".to_string(),
                ),
            ];
            for entry in self.dir_entries(&path)? {
                let child_path = if path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{}/{}", path, entry.name)
                };
                let child_ino = self.ensure_ino(&child_path);
                let file_type = file_type_for_kind(&entry.kind);
                entries.push((child_ino, file_type, entry.name));
            }
            for (index, (entry_ino, kind, name)) in
                entries.into_iter().enumerate().skip(offset as usize)
            {
                if reply.add(entry_ino, (index + 1) as u64, kind, name) {
                    break;
                }
            }
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let result: FuseResult<u64> = (|| {
            let path = self.path_for_ino(ino)?;
            let metadata = self.stat_path(&path)?.ok_or(Errno::ENOENT)?;
            if metadata.kind == "directory" {
                return Err(Errno::EISDIR);
            }
            let truncate = flags.0 & libc::O_TRUNC != 0;
            if truncate && self.read_only {
                return Err(Errno::EROFS);
            }
            let (initial, loaded, dirty) = if truncate {
                (Vec::new(), true, true)
            } else {
                self.cache
                    .get_file_matching(&path, &metadata)
                    .map(|bytes| (bytes, true, false))
                    .unwrap_or_else(|| (Vec::new(), false, false))
            };
            let base_content_hash = metadata.content_hash.clone();
            let fh = self.next_handle(
                &path,
                initial,
                loaded,
                base_content_hash,
                metadata.executable,
            )?;
            if dirty {
                let mut handles = self.lock_handles()?;
                let state = handles.files.get_mut(&fh).ok_or(Errno::ENOENT)?;
                state.dirty = true;
                state.revision = state.revision.saturating_add(1);
            }
            Ok(fh)
        })();
        match result {
            Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::empty()),
            Err(err) => reply.error(err),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let result: FuseResult<Vec<u8>> = (|| {
            let handle_state = {
                let handles = self.lock_handles()?;
                handles.files.get(&fh.0).cloned()
            };
            if let Some(state) = handle_state {
                if state.dirty {
                    let start = (offset as usize).min(state.buffer.len());
                    let end = start.saturating_add(size as usize).min(state.buffer.len());
                    return Ok(state.buffer[start..end].to_vec());
                }
                return self.read_bytes(&state.path, offset, size);
            }
            let path = self.path_for_ino(ino)?;
            self.read_bytes(&path, offset, size)
        })();
        match result {
            Ok(bytes) => reply.data(&bytes),
            Err(err) => reply.error(err),
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        if self.read_only {
            reply.error(Errno::EROFS);
            return;
        }
        let result: FuseResult<u32> = (|| {
            self.ensure_handle_loaded(fh.0)?;
            let mut handles = self.lock_handles()?;
            let state = handles.files.get_mut(&fh.0).ok_or(Errno::ENOENT)?;
            let start = offset as usize;
            if state.buffer.len() < start {
                state.buffer.resize(start, 0);
            }
            if state.buffer.len() < start + data.len() {
                state.buffer.resize(start + data.len(), 0);
            }
            state.buffer[start..start + data.len()].copy_from_slice(data);
            state.dirty = true;
            state.loaded = true;
            state.revision = state.revision.saturating_add(1);
            Ok(data.len() as u32)
        })();
        match result {
            Ok(written) => reply.written(written),
            Err(err) => reply.error(err),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        match self.flush_handle(fh.0) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.flush_handle_immediate(fh.0) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let flush_result = self.flush_handle(fh.0);
        let _ = self
            .lock_handles()
            .map(|mut handles| handles.files.remove(&fh.0));
        match flush_result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let result: FuseResult<FileAttr> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), name)?;
            self.enqueue_namespace(VfsNamespaceMutation::CreateDirectory { path: path.clone() })?;
            self.cache.invalidate(&path);
            let metadata = RemoteMetadata {
                kind: "directory".to_string(),
                size_bytes: 0,
                link_target: None,
                content_hash: None,
                executable: false,
                updated_at: None,
            };
            Ok(self.attr_for_path(&path, &metadata, true))
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(err),
        }
    }

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let result: FuseResult<FileAttr> = (|| {
            if self.read_only {
                return Err(Errno::EROFS);
            }
            let target = target.to_str().ok_or(Errno::EINVAL)?.to_string();
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), link_name)?;
            self.enqueue_namespace(VfsNamespaceMutation::CreateSymlink {
                path: path.clone(),
                target: target.clone(),
            })?;
            self.cache.invalidate(&path);
            let metadata = RemoteMetadata {
                kind: "symlink".to_string(),
                size_bytes: target.len() as u64,
                link_target: Some(target),
                content_hash: None,
                executable: false,
                updated_at: None,
            };
            Ok(self.attr_for_path(&path, &metadata, true))
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(err),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let result: FuseResult<()> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), name)?;
            self.enqueue_namespace(VfsNamespaceMutation::DeleteFile {
                path: path.clone(),
                precondition: None,
            })?;
            self.cache.invalidate(&path);
            self.detach_inode_path(&path);
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let result: FuseResult<()> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), name)?;
            self.enqueue_namespace(VfsNamespaceMutation::RemoveDirectory { path: path.clone() })?;
            self.cache.invalidate(&path);
            self.detach_inode_path(&path);
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let result: FuseResult<()> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let newparent_path = self.path_for_ino(newparent)?;
            let from = Self::child_path(parent_path.as_str(), name)?;
            let to = Self::child_path(newparent_path.as_str(), newname)?;
            self.flush_handles_for_subtree(&from)?;
            self.enqueue_namespace(VfsNamespaceMutation::Rename {
                from: from.clone(),
                to: to.clone(),
            })?;
            self.rename_inode_path(&from, &to);
            self.cache.invalidate(&from);
            self.cache.invalidate(&to);
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let result: FuseResult<FileAttr> = (|| {
            if self.read_only {
                return Err(Errno::EROFS);
            }
            let source = self.path_for_ino(ino)?;
            let parent = self.path_for_ino(newparent)?;
            let destination = Self::child_path(parent.as_str(), newname)?;
            if self.stat_path(&destination)?.is_some() {
                return Err(Errno::EEXIST);
            }
            self.flush_handles_for_path(&source)?;
            let source_metadata = self.stat_path(&source)?.ok_or(Errno::ENOENT)?;
            if source_metadata.kind != "file" {
                return Err(Errno::EPERM);
            }
            let bytes = self
                .tokio
                .block_on(self.client.read_file_raw(&source))
                .map_err(|_| Errno::EIO)?;
            self.mutate_namespace(&destination, |lease, surface| {
                self.tokio.block_on(self.client.write_file(
                    &destination,
                    &bytes,
                    source_metadata.executable,
                    lease,
                    surface,
                    VFS_OPERATION_LINK,
                    None,
                ))
            })?;
            let metadata = self.stat_path(&destination)?.unwrap_or(RemoteMetadata {
                kind: "file".to_string(),
                size_bytes: bytes.len() as u64,
                link_target: None,
                content_hash: source_metadata.content_hash,
                executable: source_metadata.executable,
                updated_at: source_metadata.updated_at,
            });
            Ok(self.attr_for_path(&destination, &metadata, true))
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(err),
        }
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let result: FuseResult<(FileAttr, u64)> = (|| {
            let parent_path = self.path_for_ino(parent)?;
            let path = Self::child_path(parent_path.as_str(), name)?;
            let metadata = RemoteMetadata {
                kind: "file".to_string(),
                size_bytes: 0,
                link_target: None,
                content_hash: None,
                executable: mode & 0o111 != 0,
                updated_at: None,
            };
            let attr = self.attr_for_path(&path, &metadata, true);
            let fh = self.next_handle(&path, Vec::new(), true, None, metadata.executable)?;
            if let Ok(mut handles) = self.lock_handles()
                && let Some(state) = handles.files.get_mut(&fh)
            {
                state.dirty = true;
            }
            Ok((attr, fh))
        })();
        match result {
            Ok((attr, fh)) => reply.created(
                &TTL,
                &attr,
                Generation(0),
                FileHandle(fh),
                FopenFlags::empty(),
            ),
            Err(err) => reply.error(err),
        }
    }
}
