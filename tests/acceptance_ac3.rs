//! AC3: Dedup — the same (node, seq) event delivered twice is applied exactly once
//! (the local file gains one block, not two).

use tether_gossip::{apply_remote_event, DaemonState, GossipEvent};
use tempfile::TempDir;

#[test]
fn dedup_same_node_seq_applied_once() {
    let dir = TempDir::new().expect("tempdir");
    let gossip = dir.path().join("gossip.md");
    std::fs::write(&gossip, "").expect("write empty");

    let mut state = DaemonState::default();
    let event = GossipEvent {
        node: "worknode".to_owned(),
        seq: 7,
        ts: 0,
        body: "unique block".to_owned(),
    };

    let first = apply_remote_event(&gossip, &mut state, "laptop", &event).expect("first apply");
    assert!(first, "first delivery must be applied");

    let second = apply_remote_event(&gossip, &mut state, "laptop", &event).expect("second apply");
    assert!(!second, "second delivery must be skipped");

    let content = std::fs::read_to_string(&gossip).expect("read");
    assert_eq!(
        content.matches("unique block").count(),
        1,
        "body appears exactly once"
    );
}

#[test]
fn different_seqs_from_same_node_both_applied() {
    let dir = TempDir::new().expect("tempdir");
    let gossip = dir.path().join("gossip.md");
    std::fs::write(&gossip, "").expect("write empty");

    let mut state = DaemonState::default();
    for seq in 1_u64..=3 {
        let event = GossipEvent {
            node: "worknode".to_owned(),
            seq,
            ts: 0,
            body: format!("block-{seq}"),
        };
        let applied =
            apply_remote_event(&gossip, &mut state, "laptop", &event).expect("apply");
        assert!(applied, "seq {seq} must be applied");
    }

    let content = std::fs::read_to_string(&gossip).expect("read");
    assert_eq!(content.matches("block-").count(), 3, "three blocks applied");
}
