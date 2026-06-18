//! AC2: An incoming wm.fleet.gossip.append from node "worknode" is appended to
//! the local fixture file under a (via tether) provenance header; the file's
//! prior content is byte-for-byte unchanged above the new block (append-only invariant).

use tether_gossip::{apply_remote_event, DaemonState, GossipEvent};
use tempfile::TempDir;

#[test]
fn apply_appends_with_provenance_header_and_preserves_prior_content() {
    let dir = TempDir::new().expect("tempdir");
    let gossip = dir.path().join("gossip.md");
    let original_content = "## 2026-06-16\nprevious entry\n";
    std::fs::write(&gossip, original_content).expect("write initial");

    let mut state = DaemonState::default();
    let event = GossipEvent {
        node: "worknode".to_owned(),
        seq: 1,
        ts: 1_750_000_000,
        body: "remote gossip block".to_owned(),
    };

    let applied = apply_remote_event(&gossip, &mut state, "laptop", &event).expect("apply");
    assert!(applied, "event must be applied (not skipped)");

    let content = std::fs::read_to_string(&gossip).expect("read");

    // Prior content is byte-for-byte unchanged at the top.
    assert!(
        content.starts_with(original_content),
        "prior content intact: got {content:?}"
    );

    // Provenance header present.
    assert!(
        content.contains("(via tether)"),
        "provenance header present"
    );
    assert!(
        content.contains("worknode"),
        "node name in provenance header"
    );
    assert!(
        content.contains("1750000000"),
        "timestamp in provenance header"
    );

    // Body appended.
    assert!(
        content.contains("remote gossip block"),
        "body appended"
    );
}

#[test]
fn apply_returns_false_for_self_echo() {
    let dir = TempDir::new().expect("tempdir");
    let gossip = dir.path().join("gossip.md");
    std::fs::write(&gossip, "old\n").expect("write");

    let mut state = DaemonState::default();
    let event = GossipEvent {
        node: "laptop".to_owned(),
        seq: 1,
        ts: 0,
        body: "self-echo".to_owned(),
    };

    let applied = apply_remote_event(&gossip, &mut state, "laptop", &event).expect("apply");
    assert!(!applied, "self-echo must not be applied");

    let content = std::fs::read_to_string(&gossip).expect("read");
    assert_eq!(content, "old\n", "file unchanged");
}
