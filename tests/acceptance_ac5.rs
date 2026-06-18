//! AC5: seq persists across restarts — after a restart, the next published
//! event's seq continues from the persisted value, not from zero.

use tether_gossip::{append_to_file, file_byte_len, tail_and_publish, CaptureSink, DaemonState};
use tempfile::TempDir;

#[test]
fn seq_persists_across_restart() {
    let dir = TempDir::new().expect("tempdir");
    let gossip = dir.path().join("gossip.md");
    let state_path = dir.path().join("state");

    // --- First "run": publish a few events.
    {
        std::fs::write(&gossip, "").expect("write empty");
        let mut state = DaemonState::load(&state_path).expect("load initial state");
        let mut sink = CaptureSink::default();

        for i in 0..3_u32 {
            let block = format!("block {i}\n");
            append_to_file(&gossip, block.as_bytes()).expect("append");
            let offset = file_byte_len(&gossip).expect("len") - block.len() as u64;
            tail_and_publish(&gossip, &mut state, "laptop", offset, &mut sink).expect("tail");
        }

        assert_eq!(state.last_published_seq, 3, "3 events published in first run");
        state.save(&state_path).expect("save state");
    }

    // --- Second "run": simulate restart by loading state from disk.
    {
        let mut state = DaemonState::load(&state_path).expect("load persisted state");
        assert_eq!(
            state.last_published_seq, 3,
            "seq loaded from disk is 3 after restart"
        );

        let mut sink = CaptureSink::default();
        let offset = file_byte_len(&gossip).expect("len");
        append_to_file(&gossip, b"post-restart block\n").expect("append post-restart");
        tail_and_publish(&gossip, &mut state, "laptop", offset, &mut sink).expect("tail");

        let events = sink.drain();
        assert_eq!(events.len(), 1, "one new event");
        assert_eq!(events[0].seq, 4, "seq is 4 (continues from 3, not 0)");
    }
}
