# tether-gossip

Bidirectional `gossip.md` mirror over the wintermute fleet bus — one gossip log across machines.

## TL;DR

`gossip.md` is the shared coordination channel where `/dream` and `/build` leave each other notes. Without this tool, the log silently forks the moment you move to the work node. `tether-gossip` mirrors gossip appends bidirectionally over NATS: an append on either machine appears on the other within seconds, loop-guarded and order-preserved, so there is **one** gossip log no matter which machine wrote the line.

## Acceptance criteria

| AC | Level | Description |
|----|-------|-------------|
| AC1 | MUST | New append → exactly one `wm.fleet.gossip.append` event, `body` matches, `seq` = previous + 1 |
| AC2 | MUST | Incoming event from remote node → appended under `(via tether)` provenance header; prior content byte-for-byte unchanged |
| AC3 | MUST | Same `(node, seq)` delivered twice → applied exactly once |
| AC4 | MUST | Self-echo (own node's event echoed back) → NOT re-applied |
| AC5 | MUST | `seq` persists across restarts; continues from saved value, not zero |
| AC6 | SHOULD | (deferred) Round-trip embedded NATS selftest — covered by mock |
| AC7 | MUST | `cargo test` green; `sigpipe::reset()` first in `main()`; `wm-tether-gossip status` does not panic |

## Install

```bash
# From source (requires Rust 1.85+):
cargo install --path .

# After install, the binary is at ~/.cargo/bin/wm-tether-gossip
```

## Usage

```bash
# Start the daemon (requires agorabus/NATS on default port):
wm-tether-gossip run

# With options:
wm-tether-gossip run \
  --gossip ~/wintermute/autobuilder/notes/gossip.md \
  --node laptop \
  --nats nats://127.0.0.1:4222 \
  --poll-ms 500

# Show status:
wm-tether-gossip status

# No-link mode (tail only, no NATS):
wm-tether-gossip run --no-link
```

## State

Persistent seq is stored at `~/.cache/wm-tether-gossip/state` (JSON). Safe to delete to reset seq to 0.

## Architecture

- **Tail half**: Polls `gossip.md` for new bytes, publishes `wm.fleet.gossip.append` events `{node, seq, ts, body}`.
- **Apply half**: Subscribes to `wm.fleet.gossip.append`, appends remote-node events under a provenance header. Never edits existing content.
- **Loop guard**: Own-node events are never re-applied (node-origin check).
- **Dedup**: `(node, seq)` seen-set persisted in state, bounded to 10,000 entries.

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.
