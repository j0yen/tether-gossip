//! AC4: Loop guard — a block this node published is NOT re-applied to its own
//! local file when the same event arrives back over the bus.

use tether_gossip::{apply_remote_event, DaemonState, GossipEvent};
use tempfile::TempDir;

#[test]
fn loop_guard_self_echo_not_applied() {
    let dir = TempDir::new().expect("tempdir");
    let gossip = dir.path().join("gossip.md");
    let original = "original content\n";
    std::fs::write(&gossip, original).expect("write initial");

    let mut state = DaemonState::default();
    // Simulate that this node published seq=3.
    state.last_published_seq = 3;

    // An echo of our own event arrives back on the bus.
    let self_event = GossipEvent {
        node: "laptop".to_owned(), // same as local_node
        seq: 3,
        ts: 0,
        body: "our own block".to_owned(),
    };

    let applied = apply_remote_event(&gossip, &mut state, "laptop", &self_event).expect("apply");
    assert!(!applied, "self-echo from our own node must be skipped");

    let content = std::fs::read_to_string(&gossip).expect("read");
    assert_eq!(content, original, "gossip.md unchanged after self-echo");
    assert!(
        !content.contains("our own block"),
        "self-echo body must not appear"
    );
}

#[test]
fn loop_guard_remote_event_is_applied() {
    // Sanity check: a genuinely remote event is NOT skipped by the loop guard.
    let dir = TempDir::new().expect("tempdir");
    let gossip = dir.path().join("gossip.md");
    std::fs::write(&gossip, "").expect("write empty");

    let mut state = DaemonState::default();
    let remote_event = GossipEvent {
        node: "worknode".to_owned(), // different from local_node
        seq: 1,
        ts: 0,
        body: "remote block".to_owned(),
    };

    let applied = apply_remote_event(&gossip, &mut state, "laptop", &remote_event).expect("apply");
    assert!(applied, "remote event must be applied");
}
