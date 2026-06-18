# Postmortem — tether-gossip

## Summary

- **Iterations**: 2 (iter-0: scaffold, iter-1: fixes)
- **Outcome**: All gates passed, 25 tests green, clippy clean, cargo deny clean
- **Reviewer verdict**: pass (Opus)

## Friction points

1. **MSRV 1.85 vs async-nats dep chain**: async-nats ≥0.35 pulls in icu_* and time crates that require 1.86+. Fixed by pinning async-nats to 0.33 and using `cargo update --precise` to pin idna_adapter and time.
2. **Missing `use PublishSink`**: The `PublishSink` trait import was missing from test files — `CaptureSink::drain()` requires the trait in scope. Fixed by adding imports.
3. **Stray root main.rs**: rsync from remote copied `main.rs` to project root instead of `src/main.rs`. Removed immediately.
4. **clippy::redundant_pub_crate in tokio::select!**: A known false positive with tokio 1.x select! macro. Fixed with `#![allow(clippy::redundant_pub_crate)]` in main.rs.
5. **tests/mocks module resolution**: `mod ac6` in `tests/mocks.rs` looks for `tests/ac6.rs` not `tests/mocks/ac6.rs`. Fixed with `#[path = "mocks/ac6.rs"] mod ac6`.

## Proposals

- Template: Add `#![allow(clippy::redundant_pub_crate)]` to main.rs scaffold for projects using tokio::select!.
- Template: Include `PublishSink`-style trait in pre-import list for acceptance test templates.
- Scaffold: Pin async-nats to a 1.85-compatible range in the template when tokio + NATS is needed.
