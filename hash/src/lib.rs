//! The VFS content-hash algorithm, selected once per process.
//!
//! Content hashes cross process and machine boundaries: vmd computes one and
//! sends it to the gateway as a CAS precondition, and the gateway compares it
//! against a hash it computed itself. So the algorithm is not a local choice —
//! every component in a deployment must agree, and a deployment whose stored
//! hashes were written under one algorithm must keep using it or explicitly
//! re-derive them.
//!
//! Both supported digests are 32 bytes and print as 64 lowercase hex
//! characters, so a stored value is **not self-describing**. That is why the
//! default is SHA-256 (what every existing deployment's stored hashes are) and
//! why moving to BLAKE3 is an explicit opt-in rather than a silent upgrade.

use std::str::FromStr;
use std::sync::OnceLock;

use sha2::Digest;

/// Environment variable selecting the algorithm. Read once, on first use.
/// Callers that keep configuration in files are expected to hydrate this before
/// the first hash is computed (OpenBracket's API does so at boot).
pub const HASH_ALGORITHM_ENV: &str = "CHEVALIER_VFS_HASH_ALGORITHM";

/// Empty-input vectors, pinned. These are the drift guard: if a component ever
/// computes a hash that disagrees with the rest of the deployment, a test that
/// pins these fails instead of every precondition-bearing write failing at
/// runtime as EIO in a guest.
pub const SHA256_EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
pub const BLAKE3_EMPTY: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VfsHashAlgorithm {
    /// The original algorithm, and the default. Every deployment that predates
    /// the configurable switch has SHA-256 in its stored hashes.
    Sha256,
    /// Faster, especially without SHA-NI (Intel gained it with Ice Lake 2019 /
    /// Rocket Lake 2021; BLAKE3's baseline is AVX2 2013). Opt-in: selecting it
    /// against SHA-256-era stored hashes means re-deriving them.
    Blake3,
}

impl VfsHashAlgorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Blake3 => "blake3",
        }
    }

    /// The hash of empty input under this algorithm.
    pub fn empty_vector(self) -> &'static str {
        match self {
            Self::Sha256 => SHA256_EMPTY,
            Self::Blake3 => BLAKE3_EMPTY,
        }
    }
}

impl Default for VfsHashAlgorithm {
    fn default() -> Self {
        Self::Sha256
    }
}

impl FromStr for VfsHashAlgorithm {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "sha256" | "sha-256" => Ok(Self::Sha256),
            "blake3" => Ok(Self::Blake3),
            other => Err(format!(
                "unknown VFS hash algorithm {other:?}; expected \"sha256\" or \"blake3\""
            )),
        }
    }
}

static ALGORITHM: OnceLock<VfsHashAlgorithm> = OnceLock::new();

/// The algorithm this process hashes with. Resolved once, from
/// `CHEVALIER_VFS_HASH_ALGORITHM`, defaulting to SHA-256.
///
/// An unparseable value falls back to the default rather than panicking: a
/// typo in an env var must not take a mount down, and the default is the one
/// that matches existing stored hashes.
pub fn algorithm() -> VfsHashAlgorithm {
    *ALGORITHM.get_or_init(|| {
        std::env::var(HASH_ALGORITHM_ENV)
            .ok()
            .filter(|raw| !raw.trim().is_empty())
            .and_then(|raw| VfsHashAlgorithm::from_str(&raw).ok())
            .unwrap_or_default()
    })
}

/// Force the algorithm, ignoring the environment. Returns whether this call is
/// the one that set it. Intended for tests and for hosts that resolve
/// configuration from files; must run before the first hash is computed.
pub fn set_algorithm(algorithm: VfsHashAlgorithm) -> bool {
    ALGORITHM.set(algorithm).is_ok()
}

/// Hex-encoded content hash of `bytes` under the configured algorithm.
pub fn hash_bytes(bytes: &[u8]) -> String {
    match algorithm() {
        VfsHashAlgorithm::Sha256 => hex_encode(sha2::Sha256::digest(bytes).as_slice()),
        VfsHashAlgorithm::Blake3 => hex_encode(blake3::hash(bytes).as_bytes()),
    }
}

/// Streaming hasher for content too large to hold in memory, dispatching on the
/// same configured algorithm as [`hash_bytes`].
pub enum ContentHasher {
    Sha256(Box<sha2::Sha256>),
    Blake3(Box<blake3::Hasher>),
}

impl ContentHasher {
    pub fn new() -> Self {
        match algorithm() {
            VfsHashAlgorithm::Sha256 => Self::Sha256(Box::new(sha2::Sha256::new())),
            VfsHashAlgorithm::Blake3 => Self::Blake3(Box::new(blake3::Hasher::new())),
        }
    }

    /// Absorb a chunk. BLAKE3 uses `update_rayon` so a large chunk spreads over
    /// the rayon pool; its tree structure makes that safe and it is the whole
    /// reason BLAKE3 is worth offering. SHA-256 is inherently sequential.
    pub fn update(&mut self, chunk: &[u8]) {
        match self {
            Self::Sha256(hasher) => {
                hasher.update(chunk);
            }
            Self::Blake3(hasher) => {
                hasher.update_rayon(chunk);
            }
        }
    }

    pub fn finalize(self) -> String {
        match self {
            Self::Sha256(hasher) => hex_encode(hasher.finalize().as_slice()),
            Self::Blake3(hasher) => hex_encode(hasher.finalize().as_bytes()),
        }
    }

    /// Digest without consuming the hasher, for bindings that expose a `digest()`
    /// callable more than once. BLAKE3 finalizes from `&self`; SHA-256's
    /// `finalize` consumes, so its state is cloned.
    pub fn digest(&self) -> String {
        match self {
            Self::Sha256(hasher) => {
                hex_encode(hasher.as_ref().clone().finalize().as_slice())
            }
            Self::Blake3(hasher) => hex_encode(hasher.finalize().as_bytes()),
        }
    }
}

impl Default for ContentHasher {
    fn default() -> Self {
        Self::new()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_algorithms_and_rejects_anything_else() {
        assert_eq!(
            VfsHashAlgorithm::from_str("sha256").unwrap(),
            VfsHashAlgorithm::Sha256
        );
        assert_eq!(
            VfsHashAlgorithm::from_str(" BLAKE3 ").unwrap(),
            VfsHashAlgorithm::Blake3
        );
        assert!(VfsHashAlgorithm::from_str("md5").is_err());
    }

    /// The default must stay SHA-256: it is what every deployment's stored
    /// hashes already are, and a stored 64-hex value cannot be told apart by
    /// shape, so defaulting to anything else silently reinterprets stored data.
    #[test]
    fn default_is_sha256() {
        assert_eq!(VfsHashAlgorithm::default(), VfsHashAlgorithm::Sha256);
    }

    /// Pins both vectors so a digest swap in either arm is caught here.
    #[test]
    fn empty_vectors_are_pinned_and_distinct() {
        assert_eq!(VfsHashAlgorithm::Sha256.empty_vector(), SHA256_EMPTY);
        assert_eq!(VfsHashAlgorithm::Blake3.empty_vector(), BLAKE3_EMPTY);
        assert_ne!(SHA256_EMPTY, BLAKE3_EMPTY);
        assert_eq!(hex_encode(sha2::Sha256::digest(b"").as_slice()), SHA256_EMPTY);
        assert_eq!(hex_encode(blake3::hash(b"").as_bytes()), BLAKE3_EMPTY);
    }

    /// Streaming and one-shot must agree, including across a chunk boundary.
    #[test]
    fn streaming_matches_one_shot_for_both_algorithms() {
        for (algorithm, expected_empty) in [
            (VfsHashAlgorithm::Sha256, SHA256_EMPTY),
            (VfsHashAlgorithm::Blake3, BLAKE3_EMPTY),
        ] {
            let one_shot = match algorithm {
                VfsHashAlgorithm::Sha256 => hex_encode(sha2::Sha256::digest(b"").as_slice()),
                VfsHashAlgorithm::Blake3 => hex_encode(blake3::hash(b"").as_bytes()),
            };
            assert_eq!(one_shot, expected_empty);

            let split = match algorithm {
                VfsHashAlgorithm::Sha256 => {
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(b"chevalier ");
                    hasher.update(b"vfs");
                    hex_encode(hasher.finalize().as_slice())
                }
                VfsHashAlgorithm::Blake3 => {
                    let mut hasher = blake3::Hasher::new();
                    hasher.update(b"chevalier ");
                    hasher.update(b"vfs");
                    hex_encode(hasher.finalize().as_bytes())
                }
            };
            let whole = match algorithm {
                VfsHashAlgorithm::Sha256 => {
                    hex_encode(sha2::Sha256::digest(b"chevalier vfs").as_slice())
                }
                VfsHashAlgorithm::Blake3 => {
                    hex_encode(blake3::hash(b"chevalier vfs").as_bytes())
                }
            };
            assert_eq!(split, whole, "{} split update", algorithm.as_str());
        }
    }
}
