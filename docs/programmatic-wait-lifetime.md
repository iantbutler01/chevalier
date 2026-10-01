# Programmatic wait lifetime

As built 2026-09-08. `rust/src/programmatic.rs` owns the code-mode deadline;
`rust/src/programmatic_runner.cjs` emits internal heartbeats while nested host
tool promises are pending. Heartbeats renew the host deadline only when its
dispatch set is nonempty. They also produce activity for sandbox transports whose
exec timeout is an inactivity deadline. They are not model-visible output.

Previously the executor used one absolute wall-clock deadline, so legitimate
workflow waits were cancelled after the default 30 seconds. Tool callers must not
shorten their own wait contracts to accommodate that outer timeout.

After tool completion, the runner has the configured execution window again.
Without pending host calls, heartbeats do not extend execution. A blocked runner
cannot emit heartbeats, and still times out even with a pending host call.
Cancellation and bounded process/tool cleanup are unchanged. Startup and writes
remain bounded. Sandboxes with an absolute wall-clock exec limit rather than an
inactivity limit still impose their own lifetime limit.

Verification:

- `cargo test --manifest-path rust/Cargo.toml --test programmatic`
- Build the native binding, then `node --test ts/test/programmatic.test.cjs`.

The tests cover delayed host results, cancellation, unawaited calls, silent
JavaScript, CPU-bound JavaScript with a pending host call, resumed deadlines after
host completion, permissions, concurrency, and process teardown.
