//! Throughput comparison for the content-hash choice.
//!
//! Run with: cargo bench --bench hash_throughput  (or `cargo run --release --bench`)
//! Reports MB/s over an in-memory buffer so the disk is not the variable.
use sha2::Digest;
use std::time::Instant;

fn mb_per_s(bytes: usize, secs: f64) -> f64 {
    (bytes as f64 / (1024.0 * 1024.0)) / secs
}

fn main() {
    let size = 512 * 1024 * 1024; // 512 MiB
    let data = vec![0xA5_u8; size];
    println!(
        "buffer: {} MiB, threads available: {}",
        size / (1024 * 1024),
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    );

    let t = Instant::now();
    let mut hasher = sha2::Sha256::new();
    hasher.update(&data);
    let _ = hasher.finalize();
    let e = t.elapsed().as_secs_f64();
    println!(
        "sha256 (sha2 crate)      {:>8.0} MB/s   {:.2}s",
        mb_per_s(size, e),
        e
    );

    let t = Instant::now();
    let _ = blake3::hash(&data);
    let e = t.elapsed().as_secs_f64();
    println!(
        "blake3 (single-thread)   {:>8.0} MB/s   {:.2}s",
        mb_per_s(size, e),
        e
    );

    let t = Instant::now();
    let mut h = blake3::Hasher::new();
    h.update_rayon(&data);
    let _ = h.finalize();
    let e = t.elapsed().as_secs_f64();
    println!(
        "blake3 (multi-thread)    {:>8.0} MB/s   {:.2}s",
        mb_per_s(size, e),
        e
    );
}
