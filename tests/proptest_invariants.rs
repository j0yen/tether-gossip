//! Property-based invariants for tether-gossip.

use proptest::prelude::*;
use tether_gossip::{apply_remote_event, DaemonState, GossipEvent};
use tempfile::TempDir;

proptest! {
    /// Applying any event with a different node name never modifies prior content.
    #[test]
    fn append_only_invariant(
        prior in "[a-z ]{0,200}",
        body in "[a-z ]{1,200}",
        node in "[a-z]{1,20}",
        seq in 1_u64..=1000,
        ts in 0_u64..=9_999_999_999,
    ) {
        let dir = TempDir::new().expect("tempdir");
        let gossip = dir.path().join("gossip.md");
        std::fs::write(&gossip, prior.as_bytes()).expect("write prior");

        let mut state = DaemonState::default();
        let event = GossipEvent {
            node: node.clone(),
            seq,
            ts,
            body: body.clone(),
        };

        // Only apply if it's from a different node.
        let local = "laptop";
        if node == local {
            return Ok(());
        }

        apply_remote_event(&gossip, &mut state, local, &event).expect("apply");

        let content = std::fs::read_to_string(&gossip).expect("read");
        prop_assert!(
            content.starts_with(&prior),
            "prior content must be intact; got {:?}",
            &content[..content.len().min(100)]
        );
    }

    /// Dedup: applying the same (node, seq) twice never adds the body twice.
    #[test]
    fn dedup_idempotent(
        body in "[a-z]{5,50}",
        seq in 1_u64..=1000,
    ) {
        let dir = TempDir::new().expect("tempdir");
        let gossip = dir.path().join("gossip.md");
        std::fs::write(&gossip, "").expect("write empty");
        let mut state = DaemonState::default();
        let event = GossipEvent {
            node: "worknode".to_owned(),
            seq,
            ts: 0,
            body: body.clone(),
        };

        apply_remote_event(&gossip, &mut state, "laptop", &event).expect("apply1");
        apply_remote_event(&gossip, &mut state, "laptop", &event).expect("apply2");

        let content = std::fs::read_to_string(&gossip).expect("read");
        prop_assert_eq!(
            content.matches(&body as &str).count(),
            1,
            "body must appear exactly once"
        );
    }
}
