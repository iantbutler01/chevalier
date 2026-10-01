# Responses wire diagnostics

The controlled Responses WebSocket path now reports sustained whitespace at the raw-frame boundary, before `parse_openai_responses_event` or Runtime forwarding. Search server stderr for `Responses wire whitespace anomaly`. The single report per provider response records the response and item IDs, incoming event type, sequence number, count of non-increasing content-event sequence numbers, frame count, and whitespace count. It deliberately excludes request bodies, credentials, and text. Reporting begins at 1024 consecutive whitespace characters, before OpenBracket's 8192-character stop threshold.

An anomaly here proves whitespace was present in incoming provider frames, rather than invented by the downstream renderer or Runtime. Increasing sequence numbers distinguish separately sequenced upstream events from repeated sequence numbers. It does not prove why the provider generated those events or distinguish model sampling from a provider-side transport defect.

`wire_content_is_forwarded_once_without_synthesizing_whitespace` exercises a real loopback WebSocket connection. Each incoming content fragment is forwarded once; final output-item and response snapshots do not duplicate streamed content. `wire_diagnostics_identify_upstream_whitespace_without_logging_content` checks the diagnostic and its bounded emission.

Run `cargo test --lib providers::responses_websocket::tests::wire_ -- --nocapture` from `rust`. On corvidae, use `/home/crow/.rustup/toolchains/1.96.0-x86_64-unknown-linux-gnu/bin` in PATH. The interactive SSH shell does not necessarily expose Cargo.
