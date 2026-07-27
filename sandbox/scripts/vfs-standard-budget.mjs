#!/usr/bin/env node
/**
 * The standard VFS workload: fixed fixtures, measured for SPEED and verified for
 * CORRECTNESS on a real mounted filesystem.
 *
 * Two fixtures, because they stress different machinery and a mount can be fine
 * at one and useless at the other:
 *
 *   SMALL  500 files, 1-64KiB, deterministic content  -> per-op / metadata cost.
 *          This is the `node_modules` shape: thousands of tiny files where
 *          per-operation latency, not bandwidth, decides whether it completes.
 *   LARGE  one 1GiB file, incompressible               -> streaming, chunking,
 *          ranged reads, and the large-file path that skips the content cache.
 *
 * Correctness is not a separate suite. Every byte written is hashed, re-read,
 * and compared; a fast filesystem that loses or truncates data is worse than a
 * slow one, and the failure we actually shipped this week was a SHORT READ, not
 * a slow one. Speed budgets without integrity checks would have called that a
 * pass.
 *
 * Fixtures are deterministic (seeded AES-CTR keystream, incompressible) so runs
 * are comparable across machines and over time.
 *
 * USAGE:
 *   SANDBOX_ENDPOINT=... SANDBOX_AUTH_TOKEN=... SESSION_ID=... \
 *     node sandbox/scripts/vfs-standard-budget.mjs
 *
 * WRITES INTO THE MOUNT. Everything lands under a single scratch directory that
 * is removed on every exit path, but on a synced workspace the churn is real —
 * point VFS_STD_MOUNT at a disposable scope when one is available.
 */

import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");
const require = createRequire(import.meta.url);
const { Sandbox } = require(
  resolve(process.env.CHEVALIER_SANDBOX_MODULE_PATH?.trim() || join(repoRoot, "ts-sandbox", "index.js")),
);

const endpoint = process.env.SANDBOX_ENDPOINT?.trim();
const authToken = process.env.SANDBOX_AUTH_TOKEN?.trim();
if (!endpoint || !authToken) {
  console.error("SANDBOX_ENDPOINT and SANDBOX_AUTH_TOKEN are required");
  process.exit(2);
}

const mountPath = process.env.VFS_STD_MOUNT?.trim() || "/workspace";
const smallCount = Number(process.env.VFS_STD_SMALL_COUNT ?? "500");
const largeMiB = Number(process.env.VFS_STD_LARGE_MIB ?? "1024");

/**
 * Budgets are ceilings the product must meet, not records of current behaviour.
 * They only ever ratchet DOWN. Raising one to make a run green defeats the file.
 */
/** RATCHET POLICY: down only. Isolated-arena baseline 2026-07-26 —
 *  small-write 53.6ms, small-read 1.5ms, large-write 4.7MiB/s,
 *  large-read 3.8MiB/s, ranged-read 3119ms.
 *
 *  Values already met are pinned just past the measurement so the gain cannot
 *  be given back; values still missed keep the target they must reach rather
 *  than being relaxed to whatever the code currently does. */
const BUDGETS = {
  "small-write-per-file-ms": 25,
  "small-read-per-file-ms": 3,  // 1.5ms measured
  "large-write-mib-per-s": 20,
  "large-read-mib-per-s": 40,
  // REGRESSED: 1252ms before the change, 3119ms after. Held at the original
  // target so it stays visible instead of being normalised away.
  "large-ranged-read-ms": 500,
};

const results = [];
const record = (name, value, budget, { lowerIsBetter = true, unit = "ms", detail = "" } = {}) => {
  const pass = lowerIsBetter ? value <= budget : value >= budget;
  results.push({ name, value, budget, pass, kind: "perf" });
  console.log(
    `  ${pass ? "[1;32mPASS[0m" : "[1;31mFAIL[0m"}  ${name.padEnd(26)} ${value.toFixed(1).padStart(9)}${unit}  ${lowerIsBetter ? "max" : "min"} ${budget}${unit}${detail ? `  (${detail})` : ""}`,
  );
};
const check = (name, ok, detail) => {
  results.push({ name, pass: ok, kind: "correctness" });
  console.log(
    `  ${ok ? "[1;32mPASS[0m" : "[1;31mFAIL[0m"}  ${name.padEnd(26)} ${ok ? "verified" : "MISMATCH"}${detail ? `  (${detail})` : ""}`,
  );
};

const sandbox = await Sandbox.connect(endpoint, { authToken });
const explicitSession = process.env.SESSION_ID?.trim();
let session;
if (explicitSession) {
  session = await sandbox.attachSessionPassive(explicitSession);
} else {
  const running = (await sandbox.listSessions()).filter((info) => info.state === 3);
  if (running.length === 0) {
    console.error("no running sandbox session to measure");
    process.exit(2);
  }
  session = await sandbox.attachSessionPassive(running[0].sessionId);
}

const exec = async (command, timeoutSecs = 1_800) => {
  const handle = await session.exec(command, {
    shell: "/bin/bash",
    closeStdinOnStart: true,
    timeoutSecs,
  });
  let stdout = "";
  let code = null;
  for (;;) {
    const event = await handle.next();
    if (event === null) break;
    if (event.data && (event.type === "stdout" || event.type === "stderr")) {
      stdout += Buffer.from(event.data).toString("utf8");
    }
    if (event.type === "exit") {
      code = event.code ?? 0;
      break;
    }
    if (event.type === "timeout") {
      code = 124;
      break;
    }
  }
  return { code, stdout };
};

const field = (output, key) => {
  const match = new RegExp(`^${key}=(.*)$`, "m").exec(output);
  return match?.[1]?.trim();
};

const ROOT = `${mountPath}/.vfs-standard-budget`;
const STAGE = "/tmp/vfs-std-stage";

/**
 * Fixture bytes are generated OFF the mount, then copied ON with block-aligned
 * I/O.
 *
 * The first version streamed `openssl | head -c` straight onto the mount. That
 * emits small chunks, and on this filesystem every small write is a round trip:
 * the same 64MiB took 871s streamed (75KB/s) versus 5.9s block-aligned
 * (5.5MiB/s) — a 73x difference. It also made the file's metadata churn for the
 * whole write, which produced spurious "stat=0" and "hash unstable" failures
 * that did not reproduce. A fixture generator must never be the slowest part of
 * a benchmark, and must never be mistaken for the thing under test.
 *
 * Content stays deterministic and incompressible (seeded AES-CTR keystream) so
 * runs are comparable and storage cannot compress the workload away.
 */
const STAGE_FIXTURES = `
rm -rf ${STAGE}; mkdir -p ${STAGE} || exit 3
gen() { openssl enc -aes-256-ctr -pass pass:vfs-std-$2 -nosalt -pbkdf2 </dev/zero 2>/dev/null | head -c $1; }
for i in $(seq 1 ${smallCount}); do
  size=$(( (i * 1237 % 64512) + 1024 ))
  gen $size $i > ${STAGE}/f$i
done
gen $(( ${largeMiB} * 1024 * 1024 )) large > ${STAGE}/blob.bin
`;

console.log(`standard VFS budget on ${mountPath}`);
console.log(`  fixtures: ${smallCount} small files (1-64KiB), one ${largeMiB}MiB file\n`);

console.log("small-file fixture");
const small = await exec(`set -u
${STAGE_FIXTURES}
rm -rf ${ROOT}; mkdir -p ${ROOT}/small || exit 3
cd ${ROOT}/small
# Timed section copies pre-made bytes with block-aligned I/O: this measures the
# mount, not the generator.
s=$(date +%s%N)
for i in $(seq 1 ${smallCount}); do
  dd if=${STAGE}/f$i of=f$i bs=64K status=none
done
e=$(date +%s%N); echo "write_ms=$(( (e-s)/1000000 ))"
# Manifest of what we believe we wrote.
sha256sum f* | sort > /tmp/std-small.expected
echo "written=$(ls -1 f* | wc -l)"
s=$(date +%s%N)
sha256sum f* | sort > /tmp/std-small.actual
e=$(date +%s%N); echo "read_ms=$(( (e-s)/1000000 ))"
if diff -q /tmp/std-small.expected /tmp/std-small.actual >/dev/null; then echo "small_integrity=ok"; else echo "small_integrity=MISMATCH"; fi
# A short read shows up as stat size != bytes actually readable.
bad=0
for f in f*; do [ "$(stat -c '%s' "$f")" = "$(wc -c < "$f")" ] || bad=$((bad+1)); done
echo "small_short_reads=$bad"`);

if (small.code !== 0) {
  console.error(`small fixture failed rc=${small.code}\n${small.stdout}`);
  process.exit(2);
}
const written = Number(field(small.stdout, "written") ?? 0);
record("small-write-per-file", Number(field(small.stdout, "write_ms")) / smallCount, BUDGETS["small-write-per-file-ms"], {
  detail: `${written} files`,
});
record("small-read-per-file", Number(field(small.stdout, "read_ms")) / smallCount, BUDGETS["small-read-per-file-ms"]);
check("small-file-integrity", field(small.stdout, "small_integrity") === "ok", `${written} hashes`);
check("small-no-short-reads", field(small.stdout, "small_short_reads") === "0", `${field(small.stdout, "small_short_reads")} bad`);

console.log("\nlarge-file fixture");
const large = await exec(`set -u
mkdir -p ${ROOT}/large || exit 3
cd ${ROOT}/large
s=$(date +%s%N)
dd if=${STAGE}/blob.bin of=blob.bin bs=1M status=none
e=$(date +%s%N); echo "write_ms=$(( (e-s)/1000000 ))"
echo "size=$(stat -c '%s' blob.bin)"
# mtime must be sane. A large file mid-publication reported 1970 on this mount,
# and build tools decide what to rebuild from mtimes — a 1970 stamp is either
# permanently stale or permanently fresh depending on which way the tool reads it.
echo "mtime_year=$(date -d "@$(stat -c '%Y' blob.bin)" +%Y 2>/dev/null || echo unknown)"
# Stat stability: a file that exists must not blink out. A transient ENOENT was
# observed on a file whose neighbouring reads both succeeded.
missing=0
for i in $(seq 1 40); do stat blob.bin >/dev/null 2>&1 || missing=$((missing+1)); done
echo "stat_misses=$missing"
s=$(date +%s%N); H1=$(sha256sum blob.bin | cut -d' ' -f1); e=$(date +%s%N)
echo "read_ms=$(( (e-s)/1000000 ))"
H2=$(sha256sum blob.bin | cut -d' ' -f1)
echo "hash1=$H1"; echo "hash2=$H2"
# Ranged reads must be self-consistent and must agree with the whole-file view:
# the large-file path bypasses the content cache, so it is exercised here and
# nowhere else.
s=$(date +%s%N)
R1=$(dd if=blob.bin bs=1M skip=17 count=4 status=none | sha256sum | cut -d' ' -f1)
e=$(date +%s%N); echo "ranged_ms=$(( (e-s)/1000000 ))"
R2=$(dd if=blob.bin bs=1M skip=17 count=4 status=none | sha256sum | cut -d' ' -f1)
echo "range1=$R1"; echo "range2=$R2"
echo "read_bytes=$(wc -c < blob.bin)"`, 3_600);

if (large.code !== 0) {
  console.error(`large fixture failed rc=${large.code}\n${large.stdout}`);
} else {
  const sizeBytes = Number(field(large.stdout, "size") ?? 0);
  // Throughput is computed from the fixture size we KNOW we wrote, never from
  // stat: this mount intermittently reports 0 for a freshly written large file,
  // and dividing by that turned a 5.1MiB/s result into a meaningless "0.0MiB/s".
  // The stat discrepancy is still gated below as a correctness failure — it just
  // must not be allowed to corrupt the performance number as well.
  const actualMiB = largeMiB;
  const writeMs = Number(field(large.stdout, "write_ms"));
  const readMs = Number(field(large.stdout, "read_ms"));
  record("large-write-throughput", actualMiB / (writeMs / 1000), BUDGETS["large-write-mib-per-s"], {
    lowerIsBetter: false,
    unit: "MiB/s",
    detail: `${actualMiB.toFixed(0)}MiB in ${(writeMs / 1000).toFixed(1)}s`,
  });
  record("large-read-throughput", actualMiB / (readMs / 1000), BUDGETS["large-read-mib-per-s"], {
    lowerIsBetter: false,
    unit: "MiB/s",
  });
  record("large-ranged-read", Number(field(large.stdout, "ranged_ms")), BUDGETS["large-ranged-read-ms"], {
    detail: "4MiB at offset 17MiB",
  });
  check("large-hash-stable", field(large.stdout, "hash1") === field(large.stdout, "hash2"), "two whole-file reads");
  check("large-range-stable", field(large.stdout, "range1") === field(large.stdout, "range2"), "two identical ranges");
  check("large-no-short-read", String(sizeBytes) === field(large.stdout, "read_bytes"), `stat=${sizeBytes} read=${field(large.stdout, "read_bytes")}`);
  const mtimeYear = field(large.stdout, "mtime_year");
  check("large-mtime-sane", mtimeYear !== "1970" && mtimeYear !== "unknown", `mtime year ${mtimeYear}`);
  check("stat-stability", field(large.stdout, "stat_misses") === "0", `${field(large.stdout, "stat_misses")}/40 stat calls missed an existing file`);
}

await exec(`rm -rf ${ROOT} ${STAGE} /tmp/std-small.expected /tmp/std-small.actual`, 600);

const perf = results.filter((entry) => entry.kind === "perf");
const correctness = results.filter((entry) => entry.kind === "correctness");
const failedPerf = perf.filter((entry) => !entry.pass);
const failedCorrectness = correctness.filter((entry) => !entry.pass);
console.log(
  `\nperf ${perf.length - failedPerf.length}/${perf.length} budgets met | correctness ${correctness.length - failedCorrectness.length}/${correctness.length} verified`,
);
// Correctness failures are not "slow", they are data loss. Call them out apart
// from the budgets so a green-ish summary can never bury one.
if (failedCorrectness.length > 0) {
  console.log("[1;31mCORRECTNESS FAILURES:[0m " + failedCorrectness.map((entry) => entry.name).join(", "));
}
process.exit(failedPerf.length + failedCorrectness.length > 0 ? 1 : 0);
