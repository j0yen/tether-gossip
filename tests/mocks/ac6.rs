//! AC6 mock: A round-trip selftest using an in-memory channel pair instead of
//! a real NATS server (deferred_acs: [6]).
//!
//! This mock exercises the same public API surface as the real round-trip test
//! would: tail_and_publish → PublishSink → apply_remote_event, with the same
//! call sequence and signatures. The invariant asserted: a tagged block
//! published locally arrives on the sink exactly once and is applied to the
//! second instance's gossip file exactly once.
//!
//! Hardware deferred because: a real end-to-end embedded NATS round-trip
//! requires a live NATS server; this mock replaces that dependency with a
//! channel-pair in-process fake.

use tether_gossip::{
    append_to_file, apply_remote_event, file_byte_len, tail_and_publish, CaptureSink, DaemonState,
};
use tempfile::TempDir;

/// Mock round-trip: publish from node A, apply to node B's gossip file.
#[test]
fn mock_round_trip_selftest() {
    // --- Node A: tail and publish. ---
    let dir_a = TempDir::new().expect("tempdir a");
    let gossip_a = dir_a.path().join("gossip.md");
    std::fs::write(&gossip_a, "").expect("write gossip_a");

    let mut state_a = DaemonState::default();
    let mut sink = CaptureSink::default();

    let offset0 = file_byte_len(&gossip_a).expect("len");
    let tagged_block = "## round-trip-tag-2026\nfoo bar\n";
    append_to_file(&gossip_a, tagged_block.as_bytes()).expect("append");

    let _new_off =
        tail_and_publish(&gossip_a, &mut state_a, "node-a", offset0, &mut sink).expect("tail");

    // Exactly one event on the sink.
    let events = sink.drain();
    assert_eq!(events.len(), 1, "exactly one event published");
    let published = events.into_iter().next().expect("event");
    assert!(
        published.body.contains("round-trip-tag-2026"),
        "tagged block in published body"
    );

    // --- Node B: apply the event. ---
    let dir_b = TempDir::new().expect("tempdir b");
    let gossip_b = dir_b.path().join("gossip.md");
    std::fs::write(&gossip_b, "").expect("write gossip_b");
    let mut state_b = DaemonState::default();

    let applied =
        apply_remote_event(&gossip_b, &mut state_b, "node-b", &published).expect("apply");
    assert!(applied, "event must be applied to node-b");

    let content_b = std::fs::read_to_string(&gossip_b).expect("read b");
    assert!(
        content_b.contains("round-trip-tag-2026"),
        "tagged block appears in node-b gossip"
    );
    assert!(
        content_b.contains("(via tether)"),
        "provenance header present"
    );

    // Deliver again — must be idempotent (applied exactly once total).
    let applied2 =
        apply_remote_event(&gossip_b, &mut state_b, "node-b", &published).expect("apply2");
    assert!(!applied2, "second delivery must be skipped");

    let content_b2 = std::fs::read_to_string(&gossip_b).expect("read b2");
    assert_eq!(
        content_b2.matches("round-trip-tag-2026").count(),
        1,
        "tagged block appears exactly once"
    );
}
