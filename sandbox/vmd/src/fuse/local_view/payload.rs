//! Immutable payload capture for the mount-local WAL.
//!
//! A content generation is published from an immutable snapshot, never from the
//! mutable backing file, so a later guest write cannot change what a pending
//! publication means. Small generations are packed into shared segments so a
//! burst of small files costs one segment sync instead of one staging-file sync
//! per file; large generations get a dedicated file, captured with a reflink
//! when the filesystem supports it and by bounded streaming otherwise.
//!
//! Concurrency contract: the store's own mutex is held only to reserve a segment
//! range or to mint a filename. Byte copying, hashing and every `fsync` happen
//! outside it, so a 1 MiB memcpy or a slow device sync never serializes
//! concurrent FUSE writers. `MountWal` never holds its append lock across any
//! call into this module.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use chevalier_vfs_hash::{ContentHasher, algorithm, hash_bytes};
use uuid::Uuid;

use super::types::{PayloadRef, PayloadStorage, validate_payload_name};
use super::{MAX_SEGMENTED_PAYLOAD_BYTES, SEGMENT_TARGET_BYTES, create_new, sync_directory};

const SEGMENT_PREFIX: &str = "segment-";
const DEDICATED_PREFIX: &str = "payload-";
const PAYLOAD_SUFFIX: &str = ".bin";

/// Fixed staging buffer for every streamed copy and every hash pass. A 1 GiB
/// generation is captured through this buffer, never through one heap
/// allocation the size of the file.
const COPY_BUFFER_BYTES: usize = 1024 * 1024;

/// Upper bound on one `copy_file_range` request, so a huge generation still
/// makes bounded, interruptible progress.
#[cfg(target_os = "linux")]
const COPY_RANGE_CHUNK_BYTES: u64 = 1024 * 1024 * 1024;

/// Snapshot of the payload files that have been written but not yet
/// device-synced. Taken outside the store lock and handed to `sync_dirty`, which
/// performs the syncs with no lock held.
#[derive(Clone, Debug, Default)]
pub(crate) struct DirtyPayloads {
    pub(crate) files: BTreeSet<String>,
    pub(crate) directory_dirty: bool,
}

impl DirtyPayloads {
    pub(crate) fn is_empty(&self) -> bool {
        self.files.is_empty() && !self.directory_dirty
    }
}

/// Bytes currently occupied by the payload directory, split so the caller can
/// report where pressure is coming from.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PayloadUsage {
    pub(crate) segment_bytes: u64,
    pub(crate) dedicated_bytes: u64,
    pub(crate) file_count: u64,
}

impl PayloadUsage {
    pub(crate) fn total_bytes(&self) -> u64 {
        self.segment_bytes.saturating_add(self.dedicated_bytes)
    }
}

/// Which of the two payload filename shapes a directory entry has. Anything
/// else in the payload directory is foreign and is neither reported as live nor
/// reclaimed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PayloadKind {
    Segment,
    Dedicated,
}

fn payload_kind(name: &str) -> Option<PayloadKind> {
    let body = name.strip_suffix(PAYLOAD_SUFFIX)?;
    if let Some(raw) = body.strip_prefix(SEGMENT_PREFIX) {
        return raw.parse::<u64>().ok().map(|_| PayloadKind::Segment);
    }
    if body
        .strip_prefix(DEDICATED_PREFIX)
        .is_some_and(|raw| !raw.is_empty())
    {
        return Some(PayloadKind::Dedicated);
    }
    None
}

/// The active packed segment. The descriptor is shared so a reservation taken
/// under the store lock can be filled with a positional write after the lock is
/// released, and so sealing the segment cannot invalidate an in-flight write.
struct ActiveSegment {
    name: String,
    file: Arc<File>,
    len: u64,
}

/// A reserved, exclusively owned byte range of the active segment.
struct SegmentReservation {
    name: String,
    file: Arc<File>,
    offset: u64,
}

struct StoreState {
    next_segment_id: u64,
    current_segment: Option<ActiveSegment>,
    dirty: DirtyPayloads,
    /// Dedicated payload names minted but not yet fully written. They are kept
    /// out of the dirty set -- a sync round must never be handed a filename that
    /// does not exist yet -- while still being unreclaimable, so a compaction
    /// racing a long capture cannot unlink the file out from under it.
    reserved: BTreeSet<String>,
}

/// Result of capturing a dedicated file, before it is registered with the store.
struct CapturedFile {
    length: u64,
    content_hash: String,
}

/// Owns `<state_dir>/payloads` and every byte inside it.
pub(crate) struct PayloadStore {
    // The directory path is immutable, so it is read without the lock. The
    // active segment id/offset reservation lives behind one short-lived mutex;
    // the segment file descriptor is written with positional writes so two
    // captures can fill disjoint reserved ranges of the same segment
    // concurrently.
    directory: PathBuf,
    state: Mutex<StoreState>,
}

impl PayloadStore {
    /// Open (creating if needed) the payload directory and recover the segment
    /// id counter. The counter never regresses, so previously recorded segment
    /// offsets stay valid for the lifetime of the state directory.
    pub(crate) fn open(payload_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(payload_dir)
            .with_context(|| format!("create mount payload directory {}", payload_dir.display()))?;
        sync_directory(payload_dir)?;
        let next_segment_id = recover_next_segment_id(payload_dir)?;
        Ok(Self {
            directory: payload_dir.to_path_buf(),
            // `current_segment` deliberately starts `None`: a restart always
            // begins a fresh segment, so every offset recorded by a previous
            // process stays valid forever.
            state: Mutex::new(StoreState {
                next_segment_id,
                current_segment: None,
                dirty: DirtyPayloads::default(),
                reserved: BTreeSet::new(),
            }),
        })
    }

    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }

    /// Capture a small generation already resident in memory.
    ///
    /// Payloads at or below `MAX_SEGMENTED_PAYLOAD_BYTES` are packed into the
    /// active segment; larger ones get a dedicated file. The content hash is
    /// computed from the caller's buffer *before* the store lock is taken.
    pub(crate) fn capture_bytes(&self, bytes: &[u8]) -> Result<PayloadRef> {
        let content_hash = hash_bytes(bytes);
        let length = bytes.len() as u64;

        if bytes.len() > MAX_SEGMENTED_PAYLOAD_BYTES {
            let name = self.reserve_dedicated()?;
            let path = self.directory.join(&name);
            if let Err(error) = write_dedicated_bytes(&path, bytes) {
                self.discard_partial(&name, &path);
                return Err(error);
            }
            self.register_dedicated(&name)?;
            return Ok(PayloadRef {
                storage: PayloadStorage::DedicatedFile { file: name },
                length,
                hash_algorithm: algorithm().as_str().to_string(),
                content_hash,
            });
        }

        let reservation = self.reserve_segment(length)?;
        reservation
            .file
            .write_all_at(bytes, reservation.offset)
            .with_context(|| {
                format!(
                    "append mount payload segment {} at offset {}",
                    reservation.name, reservation.offset
                )
            })?;
        Ok(PayloadRef {
            storage: PayloadStorage::Segment {
                file: reservation.name,
                offset: reservation.offset,
            },
            length,
            hash_algorithm: algorithm().as_str().to_string(),
            content_hash,
        })
    }

    /// Capture an immutable snapshot of a backing file.
    ///
    /// Always produces a dedicated file. Tries, in order: `FICLONE` reflink,
    /// `copy_file_range`, then a bounded streaming copy through a fixed buffer.
    /// A 1 GiB generation must never be materialized in one heap allocation.
    /// `expected_len`, when supplied, is the length the caller believes the
    /// source has; a mismatch is an error rather than a silently short payload.
    pub(crate) fn capture_file(
        &self,
        source: &Path,
        expected_len: Option<u64>,
    ) -> Result<PayloadRef> {
        let name = self.reserve_dedicated()?;
        let path = self.directory.join(&name);
        let captured = match capture_dedicated_file(source, &path, expected_len) {
            Ok(captured) => captured,
            Err(error) => {
                // A failed capture must not leave an orphan behind: nothing
                // will ever reference this filename, so nothing would ever
                // reclaim it.
                self.discard_partial(&name, &path);
                return Err(error);
            }
        };
        self.register_dedicated(&name)?;
        Ok(PayloadRef {
            storage: PayloadStorage::DedicatedFile { file: name },
            length: captured.length,
            hash_algorithm: algorithm().as_str().to_string(),
            content_hash: captured.content_hash,
        })
    }

    /// Bounded reader over exactly the payload's byte range.
    pub(crate) fn open_reader(&self, payload: &PayloadRef) -> Result<PayloadReader> {
        let name = payload.storage.file();
        validate_payload_name(name)?;
        let offset = payload.storage.offset();
        let path = self.directory.join(name);
        let mut file =
            File::open(&path).with_context(|| format!("open mount payload {}", path.display()))?;
        let end = offset
            .checked_add(payload.length)
            .ok_or_else(|| anyhow!("mount payload range overflow"))?;
        let present = file
            .metadata()
            .with_context(|| format!("stat mount payload {}", path.display()))?
            .len();
        if present < end {
            bail!(
                "mount payload {} is shorter than referenced range {offset}..{end}",
                path.display()
            );
        }
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("seek mount payload {}", path.display()))?;
        Ok(PayloadReader {
            file,
            remaining: payload.length,
        })
    }

    /// Whole payload as bytes. Only valid below the publisher's streaming
    /// threshold; callers above it must use [`Self::dedicated_path`].
    pub(crate) fn read_all(&self, payload: &PayloadRef) -> Result<Vec<u8>> {
        let mut reader = self.open_reader(payload)?;
        // The reader is already clamped to `payload.length`, so the reserve is
        // an optimization only; it is capped so a corrupt length cannot make
        // this one allocation the failure.
        let reserve = usize::try_from(payload.length.min(COPY_BUFFER_BYTES as u64))
            .unwrap_or(COPY_BUFFER_BYTES);
        let mut bytes = Vec::with_capacity(reserve);
        reader
            .read_to_end(&mut bytes)
            .with_context(|| format!("read mount payload {}", payload.storage.file()))?;
        if bytes.len() as u64 != payload.length {
            bail!(
                "mount payload {} length mismatch: expected {}, read {}",
                payload.storage.file(),
                payload.length,
                bytes.len()
            );
        }
        Ok(bytes)
    }

    /// The on-disk path of a dedicated payload file, for a streamed upload.
    /// `None` for a segment-packed payload, whose bytes must be read instead.
    pub(crate) fn dedicated_path(&self, payload: &PayloadRef) -> Option<PathBuf> {
        match &payload.storage {
            PayloadStorage::DedicatedFile { file } => {
                if validate_payload_name(file).is_err() {
                    return None;
                }
                Some(self.directory.join(file))
            }
            PayloadStorage::Segment { .. } => None,
        }
    }

    /// Re-read a payload and prove its length, algorithm and hash still match.
    /// Fails closed: a missing or altered payload is never repaired silently.
    pub(crate) fn verify(&self, payload: &PayloadRef) -> Result<()> {
        if payload.hash_algorithm != algorithm().as_str() {
            bail!(
                "mount payload hash algorithm {} does not match active {}",
                payload.hash_algorithm,
                algorithm().as_str()
            );
        }
        let mut reader = self.open_reader(payload)?;
        let mut hasher = ContentHasher::new();
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        let mut length = 0_u64;
        loop {
            let read = reader
                .read(&mut buffer)
                .with_context(|| format!("read mount payload {}", payload.storage.file()))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            length = length.saturating_add(read as u64);
        }
        if length != payload.length {
            bail!(
                "mount payload length mismatch: expected {}, read {length}",
                payload.length
            );
        }
        let actual = hasher.finalize();
        if actual != payload.content_hash {
            bail!(
                "mount payload hash mismatch: expected {}, read {actual}",
                payload.content_hash
            );
        }
        Ok(())
    }

    /// Take the pending sync set. Called by the WAL's group-commit leader.
    pub(crate) fn take_dirty(&self) -> Result<DirtyPayloads> {
        let mut state = self.lock()?;
        Ok(std::mem::take(&mut state.dirty))
    }

    /// Re-register a dirty set that a failed sync round did not make durable.
    pub(crate) fn restore_dirty(&self, dirty: DirtyPayloads) -> Result<()> {
        if dirty.is_empty() {
            return Ok(());
        }
        let mut state = self.lock()?;
        state.dirty.files.extend(dirty.files);
        state.dirty.directory_dirty |= dirty.directory_dirty;
        Ok(())
    }

    /// `fdatasync` every dirty payload file, then the directory if new files
    /// appeared. Runs with no store lock and no WAL lock held.
    pub(crate) fn sync_dirty(&self, dirty: &DirtyPayloads) -> Result<()> {
        for name in &dirty.files {
            validate_payload_name(name)?;
            let path = self.directory.join(name);
            let file = File::open(&path)
                .with_context(|| format!("open mount payload {} for sync", path.display()))?;
            file.sync_data()
                .with_context(|| format!("sync mount payload {}", path.display()))?;
        }
        if dirty.directory_dirty {
            sync_directory(&self.directory)?;
        }
        Ok(())
    }

    /// Close the active segment so no later capture appends to it. Used before a
    /// checkpoint and at shutdown so the checkpoint's active-segment list is
    /// stable.
    pub(crate) fn seal_active_segment(&self) -> Result<()> {
        // Every segment write is a positional write straight to the descriptor,
        // so there is no user-space buffer to flush; taking the segment out of
        // the state is what seals it. The descriptor is dropped after the lock
        // is released, and reservations already handed out keep their own
        // reference, so an in-flight write is never disturbed.
        let sealed = {
            let mut state = self.lock()?;
            state.current_segment.take()
        };
        drop(sealed);
        Ok(())
    }

    /// Delete exactly the checkpoint-proven `candidates`, then fsync the
    /// directory. This must never be implemented as an inverse retain-set
    /// sweep: a payload created after the caller's snapshot would be absent
    /// from that snapshot and could be unlinked while still active.
    ///
    /// Only called off the callback hot path. The store mutex is held only
    /// while taking the protection snapshot, never across filesystem I/O.
    pub(crate) fn reclaim(&self, candidates: &BTreeSet<String>) -> Result<(u64, u64)> {
        // These protections are defensive revalidation. Candidate names come
        // only from durable checkpoints and names are never reused, but an
        // active, dirty, or reserved payload is unreclaimable regardless.
        let (active, pending) = {
            let state = self.lock()?;
            let mut pending = state.dirty.files.clone();
            pending.extend(state.reserved.iter().cloned());
            (
                state
                    .current_segment
                    .as_ref()
                    .map(|segment| segment.name.clone()),
                pending,
            )
        };
        let mut removed = 0_u64;
        let mut reclaimed = 0_u64;
        let mut removed_any = false;
        for name in candidates {
            validate_payload_name(name)?;
            if pending.contains(name) || active.as_deref() == Some(name.as_str()) {
                continue;
            }
            let path = self.directory.join(name);
            let length = match std::fs::metadata(&path) {
                Ok(metadata) if metadata.is_file() => metadata.len(),
                Ok(_) => continue,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("stat mount payload {}", path.display()));
                }
            };
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    removed = removed.saturating_add(1);
                    reclaimed = reclaimed.saturating_add(length);
                    removed_any = true;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("reclaim mount payload {}", path.display()));
                }
            }
        }
        if removed_any {
            sync_directory(&self.directory)?;
        }
        Ok((removed, reclaimed))
    }

    /// Current on-disk usage, for storage-pressure accounting.
    pub(crate) fn usage(&self) -> Result<PayloadUsage> {
        let mut usage = PayloadUsage::default();
        for (_, kind, length) in self.scan_directory()? {
            match kind {
                PayloadKind::Segment => {
                    usage.segment_bytes = usage.segment_bytes.saturating_add(length);
                }
                PayloadKind::Dedicated => {
                    usage.dedicated_bytes = usage.dedicated_bytes.saturating_add(length);
                }
            }
            usage.file_count = usage.file_count.saturating_add(1);
        }
        Ok(usage)
    }

    // -- internals -----------------------------------------------------------

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, StoreState>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("mount payload store lock poisoned"))
    }

    /// Reserve `length` bytes of the active segment, rotating first when the
    /// active one is full. Only the reservation happens under the lock; the
    /// bytes are written afterwards with a positional write, so two captures
    /// fill disjoint ranges of the same segment concurrently.
    fn reserve_segment(&self, length: u64) -> Result<SegmentReservation> {
        let mut state = self.lock()?;
        let rotate = state.current_segment.as_ref().is_none_or(|segment| {
            segment.len > 0 && segment.len.saturating_add(length) > SEGMENT_TARGET_BYTES
        });
        if rotate {
            let segment_id = state.next_segment_id;
            state.next_segment_id = state.next_segment_id.saturating_add(1);
            let name = format!("{SEGMENT_PREFIX}{segment_id:020}{PAYLOAD_SUFFIX}");
            let file = create_new(&self.directory.join(&name))?;
            state.current_segment = Some(ActiveSegment {
                name,
                file: Arc::new(file),
                len: 0,
            });
            state.dirty.directory_dirty = true;
        }
        let segment = state
            .current_segment
            .as_mut()
            .ok_or_else(|| anyhow!("mount payload segment was not created"))?;
        let offset = segment.len;
        segment.len = segment.len.saturating_add(length);
        let reservation = SegmentReservation {
            name: segment.name.clone(),
            file: Arc::clone(&segment.file),
            offset,
        };
        state.dirty.files.insert(reservation.name.clone());
        Ok(reservation)
    }

    /// Mint a dedicated payload filename and mark it in-flight, so a compaction
    /// running concurrently with a long capture cannot reclaim the file the
    /// capture is still filling in.
    fn reserve_dedicated(&self) -> Result<String> {
        let name = mint_dedicated_name();
        let mut state = self.lock()?;
        state.reserved.insert(name.clone());
        Ok(name)
    }

    /// Promote a fully written dedicated payload into the next sync round. The
    /// file exists by the time it enters the dirty set, so a sync round is never
    /// handed a name it cannot open.
    fn register_dedicated(&self, name: &str) -> Result<()> {
        let mut state = self.lock()?;
        state.reserved.remove(name);
        state.dirty.files.insert(name.to_string());
        state.dirty.directory_dirty = true;
        Ok(())
    }

    /// Remove a dedicated payload whose capture failed part-way through, and
    /// release its in-flight reservation. It never entered the dirty set, so no
    /// sync round can trip over the missing file.
    fn discard_partial(&self, name: &str, path: &Path) {
        match self.lock() {
            Ok(mut state) => {
                state.reserved.remove(name);
            }
            Err(error) => {
                tracing::warn!(
                    payload = name,
                    error = %error,
                    "failed to release partial mount payload reservation"
                );
            }
        }
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %error,
                    "failed to remove partial mount payload"
                );
            }
        }
    }

    /// Every recognized payload file with its kind and length. Foreign entries
    /// and subdirectories are ignored, so nothing this store did not create is
    /// ever reported live or reclaimed.
    fn scan_directory(&self) -> Result<Vec<(String, PayloadKind, u64)>> {
        let mut entries = Vec::new();
        let listing = std::fs::read_dir(&self.directory).with_context(|| {
            format!("read mount payload directory {}", self.directory.display())
        })?;
        for entry in listing {
            let entry = entry.with_context(|| {
                format!("read mount payload directory {}", self.directory.display())
            })?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(kind) = payload_kind(name) else {
                continue;
            };
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                // A concurrent reclaim may have unlinked it; that is not an
                // error for either accounting or enumeration.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("stat mount payload {}", self.directory.join(name).display())
                    });
                }
            };
            if !metadata.is_file() {
                continue;
            }
            entries.push((name.to_string(), kind, metadata.len()));
        }
        Ok(entries)
    }
}

/// A bounded, seekable-at-construction reader over one payload's range.
pub(crate) struct PayloadReader {
    file: File,
    remaining: u64,
}

impl Read for PayloadReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 || buffer.is_empty() {
            return Ok(0);
        }
        let limit =
            usize::try_from(self.remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        let read = self.file.read(&mut buffer[..limit])?;
        self.remaining = self.remaining.saturating_sub(read as u64);
        Ok(read)
    }
}

impl PayloadReader {
    pub(crate) fn remaining(&self) -> u64 {
        self.remaining
    }
}

// ---------------------------------------------------------------------------
// Capture helpers -- all run with no store lock held
// ---------------------------------------------------------------------------

fn mint_dedicated_name() -> String {
    format!("{DEDICATED_PREFIX}{}{PAYLOAD_SUFFIX}", Uuid::now_v7())
}

fn write_dedicated_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = create_new(path)?;
    file.write_all(bytes)
        .with_context(|| format!("write mount payload {}", path.display()))
}

/// Recover the segment id counter by scanning the payload directory. The
/// counter only ever moves forward, so a segment name is never reused and an
/// offset recorded by an earlier process stays meaningful forever.
fn recover_next_segment_id(payload_dir: &Path) -> Result<u64> {
    let mut next = 0_u64;
    for entry in std::fs::read_dir(payload_dir)
        .with_context(|| format!("read mount payload directory {}", payload_dir.display()))?
    {
        let entry = entry
            .with_context(|| format!("read mount payload directory {}", payload_dir.display()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(raw) = name
            .strip_prefix(SEGMENT_PREFIX)
            .and_then(|name| name.strip_suffix(PAYLOAD_SUFFIX))
        else {
            continue;
        };
        if let Ok(id) = raw.parse::<u64>() {
            next = next.max(id.saturating_add(1));
        }
    }
    Ok(next)
}

/// Materialize `source` as the immutable dedicated payload at `destination`,
/// preferring a reflink and falling back through `copy_file_range` to a bounded
/// streaming copy. The returned length is what actually landed in the payload,
/// never what the source claimed before the copy started.
fn capture_dedicated_file(
    source: &Path,
    destination: &Path,
    expected_len: Option<u64>,
) -> Result<CapturedFile> {
    let mut input =
        File::open(source).with_context(|| format!("open payload source {}", source.display()))?;
    let source_len = input
        .metadata()
        .with_context(|| format!("stat payload source {}", source.display()))?
        .len();
    if let Some(expected) = expected_len
        && expected != source_len
    {
        bail!(
            "payload source {} is {source_len} bytes, expected {expected}",
            source.display()
        );
    }

    let mut output = create_new(destination)?;
    let captured = if clone_or_copy_range(&input, &output, source_len)? {
        hash_captured_file(destination)?
    } else {
        stream_capture(&mut input, &mut output, source, destination)?
    };

    if let Some(expected) = expected_len
        && expected != captured.length
    {
        bail!(
            "captured payload {} is {} bytes, expected {expected}",
            destination.display(),
            captured.length
        );
    }
    Ok(captured)
}

/// Try the two zero-copy paths. `Ok(true)` means `destination` now holds the
/// whole source; `Ok(false)` means the caller must stream, and `destination` has
/// been reset to empty.
#[cfg(target_os = "linux")]
fn clone_or_copy_range(input: &File, output: &File, source_len: u64) -> Result<bool> {
    use std::os::fd::AsRawFd;

    const FICLONE: libc::c_ulong = 0x40049409;

    // A reflink is the whole point: capturing a 1 GiB generation must not cost
    // 1 GiB of writes on a filesystem that can share the extents.
    let cloned = unsafe { libc::ioctl(output.as_raw_fd(), FICLONE, input.as_raw_fd()) };
    if cloned == 0 {
        return Ok(true);
    }

    if source_len == 0 {
        return Ok(true);
    }

    let mut copied = 0_u64;
    while copied < source_len {
        let remaining = source_len - copied;
        let chunk = usize::try_from(remaining.min(COPY_RANGE_CHUNK_BYTES)).unwrap_or(usize::MAX);
        let mut offset_in = copied as libc::off64_t;
        let mut offset_out = copied as libc::off64_t;
        let written = unsafe {
            libc::copy_file_range(
                input.as_raw_fd(),
                &mut offset_in,
                output.as_raw_fd(),
                &mut offset_out,
                chunk,
                0,
            )
        };
        if written <= 0 {
            // Unsupported filesystem, a cross-device pair, or a source that
            // shrank underneath us: reset and let the streaming path produce
            // the authoritative bytes and length.
            reset_output(output)?;
            return Ok(false);
        }
        copied = copied.saturating_add(written as u64);
    }
    Ok(true)
}

#[cfg(not(target_os = "linux"))]
fn clone_or_copy_range(_input: &File, _output: &File, _source_len: u64) -> Result<bool> {
    // `FICLONE` and `copy_file_range` are Linux-only; production is Linux-only
    // and every other host streams.
    Ok(false)
}

#[cfg(target_os = "linux")]
fn reset_output(output: &File) -> Result<()> {
    output
        .set_len(0)
        .context("reset partially copied mount payload")
}

/// Bounded streaming copy: one fixed buffer, hashed as it goes, so a 1 GiB
/// generation is never one heap allocation and never a second read pass.
fn stream_capture(
    input: &mut File,
    output: &mut File,
    source: &Path,
    destination: &Path,
) -> Result<CapturedFile> {
    input
        .seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind payload source {}", source.display()))?;
    output
        .set_len(0)
        .with_context(|| format!("reset mount payload {}", destination.display()))?;
    output
        .seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind mount payload {}", destination.display()))?;

    let mut hasher = ContentHasher::new();
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    let mut length = 0_u64;
    loop {
        let read = input
            .read(&mut buffer)
            .with_context(|| format!("read payload source {}", source.display()))?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .with_context(|| format!("write mount payload {}", destination.display()))?;
        hasher.update(&buffer[..read]);
        length = length.saturating_add(read as u64);
    }
    Ok(CapturedFile {
        length,
        content_hash: hasher.finalize(),
    })
}

/// Hash a payload that a zero-copy path produced, through the same fixed buffer.
fn hash_captured_file(destination: &Path) -> Result<CapturedFile> {
    let mut file = File::open(destination)
        .with_context(|| format!("open mount payload {}", destination.display()))?;
    let mut hasher = ContentHasher::new();
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    let mut length = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("read mount payload {}", destination.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length = length.saturating_add(read as u64);
    }
    Ok(CapturedFile {
        length,
        content_hash: hasher.finalize(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::PayloadStore;

    #[test]
    fn reclaim_deletes_only_explicit_checkpoint_candidates() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = PayloadStore::open(temp.path()).expect("open payload store");

        let stale = store.capture_bytes(b"stale").expect("capture stale");
        let dirty = store.take_dirty().expect("take stale dirty set");
        store.sync_dirty(&dirty).expect("sync stale payload");
        store.seal_active_segment().expect("seal stale segment");

        let current = store.capture_bytes(b"current").expect("capture current");
        let stale_name = stale.storage.file().to_string();
        let current_name = current.storage.file().to_string();
        assert_ne!(stale_name, current_name);

        let candidates = BTreeSet::from([stale_name.clone()]);
        let (removed, bytes) = store.reclaim(&candidates).expect("reclaim candidate");

        assert_eq!(removed, 1);
        assert_eq!(bytes, b"stale".len() as u64);
        assert!(!temp.path().join(stale_name).exists());
        assert!(temp.path().join(current_name).exists());
        store.verify(&current).expect("current payload survives");
    }
}
