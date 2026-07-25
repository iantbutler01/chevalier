// @dive-file: Binary pack format primitives for optimized VFS storage.
// @dive-rel: Packs are multi-slot objects that can be range-read for single-file access while
// @dive-rel: still supporting bulk whole-pack fetch and cache warming.
// @dive-rel: Pure format code; no I/O, no DB, no GCS, and no product policy.
//
// Binary layout:
//     magic             8 bytes  "NYMPACK\0"
//     format_version    u16 LE   (1)
//     slot_count        u32 LE
//     (repeated slot_count times:)
//         slot_header_length   u16 LE   (total size of the header prefix including this field)
//         content_hash         32 bytes (raw sha256 of uncompressed payload)
//         uncompressed_length  u64 LE
//         compressed_length    u64 LE
//         compression_algo     u8       (0 = raw, 1 = zstd)
//         reserved             pad to slot_header_length bytes
//         payload              compressed_length bytes
//
// A slot's on-object extent is [slot_offset, slot_offset + slot_header_length + compressed_length).
// Readers record slot_offset + slot_length (header_len + compressed_len) in the DB manifest and
// can issue a GCS Range GET covering exactly that span.

use sha2::{Digest, Sha256};

use super::{VfsStorageError, VfsStorageResult};

const MAGIC: &[u8; 8] = b"NYMPACK\0";
const CURRENT_FORMAT_VERSION: u16 = 1;
const SLOT_HEADER_LEN_PREFIX_BYTES: usize = 2; // u16 LE `slot_header_length` field

// Minimum slot header size: the fields after `slot_header_length` that the current reader knows
// about. Future versions may extend this; readers tolerate a larger `slot_header_length` by
// skipping the trailing bytes they don't understand.
//
//     content_hash          32 bytes
//     uncompressed_length    8 bytes
//     compressed_length      8 bytes
//     compression_algo       1 byte
const MIN_SLOT_HEADER_PAYLOAD_BYTES: usize = 32 + 8 + 8 + 1;
const MIN_SLOT_HEADER_TOTAL_BYTES: usize =
    SLOT_HEADER_LEN_PREFIX_BYTES + MIN_SLOT_HEADER_PAYLOAD_BYTES;

const FILE_HEADER_BYTES: usize = 8 + 2 + 4; // magic + format_version + slot_count

const COMPRESSION_RAW: u8 = 0;
const COMPRESSION_ZSTD: u8 = 1;

/// Default zstd level. Balances CPU and ratio for the small markdown/json files that dominate
/// current VFS workloads. Tuned empirically — override via `PackBuilder::with_zstd_level` if needed.
const DEFAULT_ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotCompression {
    Raw,
    Zstd,
}

impl SlotCompression {
    pub fn algo_byte(self) -> u8 {
        match self {
            SlotCompression::Raw => COMPRESSION_RAW,
            SlotCompression::Zstd => COMPRESSION_ZSTD,
        }
    }

    pub fn from_algo_byte(byte: u8) -> VfsStorageResult<Self> {
        match byte {
            COMPRESSION_RAW => Ok(SlotCompression::Raw),
            COMPRESSION_ZSTD => Ok(SlotCompression::Zstd),
            other => Err(VfsStorageError::Internal(format!(
                "unknown pack slot compression algo byte: {other}"
            ))),
        }
    }

    pub fn as_db_smallint(self) -> i16 {
        self.algo_byte() as i16
    }

    pub fn from_db_smallint(value: i16) -> VfsStorageResult<Self> {
        if value < 0 || value > u8::MAX as i16 {
            return Err(VfsStorageError::Internal(format!(
                "pack slot compression db value out of range: {value}"
            )));
        }
        Self::from_algo_byte(value as u8)
    }
}

#[derive(Debug, Clone)]
pub struct BuiltSlot {
    pub logical_path: String,
    pub content_hash: [u8; 32],
    /// Byte offset into the pack where this slot begins (the slot_header_length prefix).
    pub pack_slot_offset: u64,
    /// Total byte length of this slot on the pack object, including header prefix,
    /// header payload, and compressed body. This is exactly what a GCS Range GET must cover.
    pub pack_slot_length: u64,
    pub uncompressed_length: u64,
    pub compression: SlotCompression,
}

#[derive(Debug, Clone)]
pub struct BuiltPack {
    pub pack_bytes: Vec<u8>,
    pub slots: Vec<BuiltSlot>,
}

impl BuiltPack {
    pub fn total_bytes(&self) -> u64 {
        self.pack_bytes.len() as u64
    }

    pub fn slot_count(&self) -> u32 {
        self.slots.len() as u32
    }
}

/// Accumulates slots into an in-memory buffer. Call `finish()` to emit the complete pack.
pub struct PackBuilder {
    buffer: Vec<u8>,
    slots: Vec<BuiltSlot>,
    zstd_level: i32,
    slot_count_finalized: bool,
}

impl PackBuilder {
    pub fn new() -> Self {
        let mut buffer = Vec::with_capacity(4096);
        buffer.extend_from_slice(MAGIC);
        buffer.extend_from_slice(&CURRENT_FORMAT_VERSION.to_le_bytes());
        // Placeholder slot_count; finalized by `finish`.
        buffer.extend_from_slice(&0u32.to_le_bytes());
        debug_assert_eq!(buffer.len(), FILE_HEADER_BYTES);
        Self {
            buffer,
            slots: Vec::new(),
            zstd_level: DEFAULT_ZSTD_LEVEL,
            slot_count_finalized: false,
        }
    }

    pub fn with_zstd_level(mut self, level: i32) -> Self {
        self.zstd_level = level;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// Append a file to the pack with the given compression algorithm.
    ///
    /// `logical_path` is retained on the returned `BuiltSlot` for the caller's convenience when
    /// inserting manifest rows — it is NOT written into the pack body, because path-to-slot
    /// mapping lives in the DB manifest, not inside the pack.
    pub fn add(
        &mut self,
        logical_path: &str,
        bytes: &[u8],
        compression: SlotCompression,
    ) -> VfsStorageResult<()> {
        if self.slot_count_finalized {
            return Err(VfsStorageError::Internal(
                "PackBuilder::add called after finish()".to_string(),
            ));
        }
        let content_hash = sha256_of(bytes);
        let uncompressed_length = bytes.len() as u64;
        let compressed_payload: Vec<u8> = match compression {
            SlotCompression::Raw => bytes.to_vec(),
            SlotCompression::Zstd => zstd::encode_all(bytes, self.zstd_level)
                .map_err(|e| VfsStorageError::Internal(format!("zstd encode failed: {e}")))?,
        };
        let compressed_length = compressed_payload.len() as u64;
        let slot_header_payload_bytes = MIN_SLOT_HEADER_PAYLOAD_BYTES;
        let slot_header_length = SLOT_HEADER_LEN_PREFIX_BYTES + slot_header_payload_bytes;
        if slot_header_length > u16::MAX as usize {
            return Err(VfsStorageError::Internal(
                "pack slot header exceeds u16::MAX".to_string(),
            ));
        }
        let pack_slot_offset = self.buffer.len() as u64;
        let slot_total_len = slot_header_length as u64 + compressed_length;

        // Serialize the slot header prefix + body.
        self.buffer
            .extend_from_slice(&(slot_header_length as u16).to_le_bytes());
        self.buffer.extend_from_slice(&content_hash);
        self.buffer
            .extend_from_slice(&uncompressed_length.to_le_bytes());
        self.buffer
            .extend_from_slice(&compressed_length.to_le_bytes());
        self.buffer.push(compression.algo_byte());
        // No reserved bytes yet; slot_header_length == header_payload + prefix.
        self.buffer.extend_from_slice(&compressed_payload);

        self.slots.push(BuiltSlot {
            logical_path: logical_path.to_string(),
            content_hash,
            pack_slot_offset,
            pack_slot_length: slot_total_len,
            uncompressed_length,
            compression,
        });
        Ok(())
    }

    pub fn finish(mut self) -> VfsStorageResult<BuiltPack> {
        if self.slot_count_finalized {
            return Err(VfsStorageError::Internal(
                "PackBuilder::finish called twice".to_string(),
            ));
        }
        if self.slots.is_empty() {
            return Err(VfsStorageError::Internal(
                "PackBuilder::finish called with no slots".to_string(),
            ));
        }
        let slot_count = self.slots.len() as u32;
        // Backfill slot_count in the file header.
        let slot_count_bytes = slot_count.to_le_bytes();
        let slot_count_offset = MAGIC.len() + 2; // after magic + format_version
        self.buffer[slot_count_offset..slot_count_offset + 4].copy_from_slice(&slot_count_bytes);
        self.slot_count_finalized = true;
        Ok(BuiltPack {
            pack_bytes: self.buffer,
            slots: self.slots,
        })
    }
}

impl Default for PackBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parsed file-header fields (magic, version, slot count). Useful for sanity-checking a fetched
/// pack before attempting slot extraction.
#[derive(Debug, Clone, Copy)]
pub struct PackHeader {
    pub format_version: u16,
    pub slot_count: u32,
}

pub fn parse_header(bytes: &[u8]) -> VfsStorageResult<PackHeader> {
    if bytes.len() < FILE_HEADER_BYTES {
        return Err(VfsStorageError::Internal(format!(
            "pack bytes too short for file header: {} < {}",
            bytes.len(),
            FILE_HEADER_BYTES
        )));
    }
    if &bytes[..MAGIC.len()] != MAGIC {
        return Err(VfsStorageError::Internal(
            "pack magic mismatch; not a nymfs pack".to_string(),
        ));
    }
    let format_version = u16::from_le_bytes([bytes[8], bytes[9]]);
    if format_version == 0 {
        return Err(VfsStorageError::Internal(
            "pack format_version=0 is invalid".to_string(),
        ));
    }
    let slot_count = u32::from_le_bytes([bytes[10], bytes[11], bytes[12], bytes[13]]);
    Ok(PackHeader {
        format_version,
        slot_count,
    })
}

#[derive(Debug, Clone)]
pub struct ExtractedSlot {
    pub content_hash: [u8; 32],
    pub bytes: Vec<u8>,
}

/// Extract a single slot from its pack-object slice. `pack_bytes` must contain AT LEAST the byte
/// range `[slot_offset, slot_offset + slot_length)`. For full-pack fetches this is the whole pack;
/// for ranged fetches it can be just the slot's own bytes, in which case the caller passes
/// `slot_offset = 0` and the slice starts at the slot's header.
///
/// On success the returned `bytes` are fully decompressed and their sha256 has been verified
/// against the slot header's content_hash. The caller MAY do an additional check against the
/// manifest row's content_hash field; they will match.
pub fn extract_slot(
    pack_bytes: &[u8],
    slot_offset: u64,
    slot_length: u64,
) -> VfsStorageResult<ExtractedSlot> {
    let slot_offset = slot_offset as usize;
    let slot_length = slot_length as usize;
    let slot_end = slot_offset
        .checked_add(slot_length)
        .ok_or_else(|| VfsStorageError::Internal("slot offset/length overflow".to_string()))?;
    if pack_bytes.len() < slot_end {
        return Err(VfsStorageError::Internal(format!(
            "pack slice too short for slot: need {slot_end} bytes, have {}",
            pack_bytes.len()
        )));
    }
    let slot = &pack_bytes[slot_offset..slot_end];
    if slot.len() < MIN_SLOT_HEADER_TOTAL_BYTES {
        return Err(VfsStorageError::Internal(format!(
            "slot too small: {} < {}",
            slot.len(),
            MIN_SLOT_HEADER_TOTAL_BYTES
        )));
    }
    let slot_header_length = u16::from_le_bytes([slot[0], slot[1]]) as usize;
    if slot_header_length < MIN_SLOT_HEADER_TOTAL_BYTES {
        return Err(VfsStorageError::Internal(format!(
            "slot_header_length {slot_header_length} < min {MIN_SLOT_HEADER_TOTAL_BYTES}"
        )));
    }
    if slot.len() < slot_header_length {
        return Err(VfsStorageError::Internal(
            "slot bytes shorter than declared header".to_string(),
        ));
    }
    let mut cursor = SLOT_HEADER_LEN_PREFIX_BYTES;
    let mut content_hash = [0u8; 32];
    content_hash.copy_from_slice(&slot[cursor..cursor + 32]);
    cursor += 32;
    let uncompressed_length = u64::from_le_bytes(
        slot[cursor..cursor + 8]
            .try_into()
            .map_err(|_| VfsStorageError::Internal("slice len mismatch for u64".to_string()))?,
    );
    cursor += 8;
    let compressed_length = u64::from_le_bytes(
        slot[cursor..cursor + 8]
            .try_into()
            .map_err(|_| VfsStorageError::Internal("slice len mismatch for u64".to_string()))?,
    );
    cursor += 8;
    let algo_byte = slot[cursor];
    // cursor += 1; // would advance past algo; unused further.
    let compression = SlotCompression::from_algo_byte(algo_byte)?;

    let body_start = slot_header_length;
    let body_end = body_start
        .checked_add(compressed_length as usize)
        .ok_or_else(|| VfsStorageError::Internal("slot body length overflow".to_string()))?;
    if slot.len() < body_end {
        return Err(VfsStorageError::Internal(format!(
            "slot truncated: need {body_end} bytes, have {}",
            slot.len()
        )));
    }
    let body = &slot[body_start..body_end];
    let decompressed: Vec<u8> = match compression {
        SlotCompression::Raw => body.to_vec(),
        SlotCompression::Zstd => zstd::decode_all(body)
            .map_err(|e| VfsStorageError::Internal(format!("zstd decode failed: {e}")))?,
    };
    if decompressed.len() as u64 != uncompressed_length {
        return Err(VfsStorageError::Internal(format!(
            "decompressed length mismatch: expected {uncompressed_length}, got {}",
            decompressed.len()
        )));
    }
    let actual_hash = sha256_of(&decompressed);
    if actual_hash != content_hash {
        return Err(VfsStorageError::Internal(
            "slot content_hash mismatch after decompression".to_string(),
        ));
    }
    Ok(ExtractedSlot {
        content_hash,
        bytes: decompressed,
    })
}

/// Compute the hex-encoded content hash of the given bytes. This is the
/// logical-file hash used by downstream manifest/content_hash columns; it is not
/// a whole-pack object hash.
///
/// BLAKE3, not SHA-256. Hashing is the floor on large-file cost and SHA-256 is
/// 2.6-10x slower depending on whether the host has SHA-NI: measured 2536 MB/s
/// on a Ryzen with the extension, 579 MB/s on Apple Silicon where the sha2 crate
/// falls back to software, against 6668 and 2450 MB/s for single-threaded
/// BLAKE3. Self-hosted fleets make that gap the common case -- Intel only gained
/// SHA-NI with Ice Lake (2019) / Rocket Lake (2021), while BLAKE3's baseline is
/// AVX2 (2013). See benches/hash_throughput.rs.
///
/// Both digests are 32 bytes and print as 64 hex characters, so a stored value
/// is NOT self-describing: anything persisted before this change is SHA-256 and
/// indistinguishable by shape. Callers comparing against stored hashes must
/// treat a mismatch as "re-derive", never as "content changed".
pub fn hex_hash(bytes: &[u8]) -> String {
    hex_encode(blake3::hash(bytes).as_bytes())
}

/// SHA-256 of the given bytes, retained for pack-format compatibility where the
/// on-disk layout pins the digest.
fn sha256_of(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let out = hasher.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_extract_single_slot_raw() {
        let mut b = PackBuilder::new();
        b.add("soul/soul.md", b"hello world", SlotCompression::Raw)
            .unwrap();
        let pack = b.finish().unwrap();
        assert_eq!(pack.slots.len(), 1);
        let slot = &pack.slots[0];
        let extracted = extract_slot(
            &pack.pack_bytes,
            slot.pack_slot_offset,
            slot.pack_slot_length,
        )
        .unwrap();
        assert_eq!(extracted.bytes, b"hello world");
        assert_eq!(extracted.content_hash, slot.content_hash);
    }

    #[test]
    fn build_and_extract_single_slot_zstd() {
        let input = b"the quick brown fox jumps over the lazy dog, repeatedly. ".repeat(20);
        let mut b = PackBuilder::new();
        b.add("memory/long_term/quote.md", &input, SlotCompression::Zstd)
            .unwrap();
        let pack = b.finish().unwrap();
        let slot = &pack.slots[0];
        let extracted = extract_slot(
            &pack.pack_bytes,
            slot.pack_slot_offset,
            slot.pack_slot_length,
        )
        .unwrap();
        assert_eq!(extracted.bytes, input);
        // Zstd should reduce the repetitive input.
        assert!(
            slot.pack_slot_length < input.len() as u64 + 64,
            "zstd slot should be noticeably smaller than input",
        );
    }

    #[test]
    fn build_and_extract_multi_slot_mixed() {
        let mut b = PackBuilder::new();
        let files: Vec<(&str, &[u8], SlotCompression)> = vec![
            ("soul/soul.md", b"soul", SlotCompression::Zstd),
            ("mind/conscious/now.md", b"", SlotCompression::Raw),
            (
                "conversations/.index.json",
                b"{\"conversations\":[]}\n",
                SlotCompression::Zstd,
            ),
            (
                "memory/long_term/preferences.md",
                b"# Preferences\n\n- dark mode\n",
                SlotCompression::Zstd,
            ),
        ];
        for (path, bytes, c) in &files {
            b.add(path, bytes, *c).unwrap();
        }
        let pack = b.finish().unwrap();
        assert_eq!(pack.slots.len(), files.len());
        let header = parse_header(&pack.pack_bytes).unwrap();
        assert_eq!(header.format_version, CURRENT_FORMAT_VERSION);
        assert_eq!(header.slot_count as usize, files.len());
        for (i, (_, bytes, _)) in files.iter().enumerate() {
            let slot = &pack.slots[i];
            let extracted = extract_slot(
                &pack.pack_bytes,
                slot.pack_slot_offset,
                slot.pack_slot_length,
            )
            .unwrap();
            assert_eq!(&extracted.bytes, *bytes);
        }
    }

    #[test]
    fn extract_from_ranged_slice() {
        // Simulate fetching JUST the slot bytes via a Range GET: slice the whole pack down to
        // the slot range, then call extract_slot with slot_offset=0.
        let mut b = PackBuilder::new();
        b.add("note.md", b"just the one slot", SlotCompression::Zstd)
            .unwrap();
        b.add("other.md", b"another slot entirely", SlotCompression::Zstd)
            .unwrap();
        let pack = b.finish().unwrap();

        let target = &pack.slots[1];
        let ranged = pack.pack_bytes[target.pack_slot_offset as usize
            ..(target.pack_slot_offset + target.pack_slot_length) as usize]
            .to_vec();
        let extracted = extract_slot(&ranged, 0, target.pack_slot_length).unwrap();
        assert_eq!(extracted.bytes, b"another slot entirely");
    }

    #[test]
    fn corrupted_slot_body_is_rejected() {
        let mut b = PackBuilder::new();
        b.add("x.md", b"payload", SlotCompression::Raw).unwrap();
        let mut pack = b.finish().unwrap();
        // Flip the last byte (inside the payload)
        let last = pack.pack_bytes.len() - 1;
        pack.pack_bytes[last] ^= 0xff;
        let slot = &pack.slots[0];
        let err = extract_slot(
            &pack.pack_bytes,
            slot.pack_slot_offset,
            slot.pack_slot_length,
        )
        .unwrap_err();
        match err {
            VfsStorageError::Internal(msg) => assert!(msg.contains("content_hash mismatch")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn empty_builder_rejects_finish() {
        let b = PackBuilder::new();
        let err = b.finish().unwrap_err();
        assert!(matches!(err, VfsStorageError::Internal(_)));
    }

    #[test]
    fn header_magic_mismatch_is_rejected() {
        let mut bytes = vec![0u8; FILE_HEADER_BYTES];
        bytes[..8].copy_from_slice(b"NOTAPACK");
        let err = parse_header(&bytes).unwrap_err();
        assert!(matches!(err, VfsStorageError::Internal(_)));
    }

    #[test]
    fn hex_hash_matches_known_value() {
        // BLAKE3("") per the reference vectors. Pinning this is what makes an
        // accidental digest change loud: both BLAKE3 and SHA-256 are 32 bytes and
        // print as 64 hex characters, so nothing else about a stored value
        // reveals which algorithm produced it.
        assert_eq!(
            hex_hash(b""),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
        // The previous SHA-256 value, kept as a guard against silently reverting.
        assert_ne!(
            hex_hash(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn slot_length_matches_on_object_extent() {
        let mut b = PackBuilder::new();
        b.add("a.md", b"aaaa", SlotCompression::Raw).unwrap();
        b.add("b.md", b"bbbbbb", SlotCompression::Raw).unwrap();
        let pack = b.finish().unwrap();
        // First slot starts after the file header.
        assert_eq!(pack.slots[0].pack_slot_offset, FILE_HEADER_BYTES as u64);
        // Second slot starts exactly after the first slot ends.
        assert_eq!(
            pack.slots[1].pack_slot_offset,
            pack.slots[0].pack_slot_offset + pack.slots[0].pack_slot_length
        );
        // Total pack size is file header + both slot lengths.
        assert_eq!(
            pack.pack_bytes.len() as u64,
            FILE_HEADER_BYTES as u64
                + pack.slots[0].pack_slot_length
                + pack.slots[1].pack_slot_length
        );
    }
}
