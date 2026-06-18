//! AC1: Appending a new block to a fixture gossip.md causes wm-tether-gossip to
//! publish exactly one wm.fleet.gossip.append event whose body equals the appended
//! block and whose seq is previous_seq + 1 (asserted against a captured publish sink).

use tether_gossip::{append_to_file, file_byte_len, tail_and_publish, CaptureSink, DaemonState, PublishSink};
use tempfile::TempDir;

#[test]
fn tail_publishes_single_event_with_correct_body_and_seq() {
    let dir = TempDir::new().expect("tempdir");
    let gossip = dir.path().join("gossip.md");
    // Write some initial content.
    std::fs::write(&gossip, "initial content\n").expect("write initial");

    let mut state = DaemonState::default();
    let mut sink = CaptureSink::default();

    // Establish baseline offset (no new events yet).
    let offset0 = file_byte_len(&gossip).expect("len");
    let off1 =
        tail_and_publish(&gossip, &mut state, "laptop", offset0, &mut sink).expect("tail 1");
    assert_eq!(off1, offset0, "no new content, offset unchanged");
    let drained = sink.drain();
    assert!(drained.is_empty(), "no events published when nothing new");

    // Append a new block.
    let new_block = "## 2026-06-17\nsome gossip\n";
    append_to_file(&gossip, new_block.as_bytes()).expect("append");

    let previous_seq = state.last_published_seq;
    let off2 =
        tail_and_publish(&gossip, &mut state, "laptop", offset0, &mut sink).expect("tail 2");
    assert!(off2 > offset0, "offset advanced after append");

    let events = sink.drain();
    assert_eq!(events.len(), 1, "exactly one event published");
    let ev = &events[0];
    assert_eq!(ev.node, "laptop", "node field correct");
    assert_eq!(ev.seq, previous_seq + 1, "seq is previous + 1");
    assert!(
        ev.body.contains("some gossip"),
        "body contains appended content"
    );
}

#[test]
fn tail_publishes_seq_continues_from_existing_state() {
    let dir = TempDir::new().expect("tempdir");
    let gossip = dir.path().join("gossip.md");
    std::fs::write(&gossip, "old\n").expect("write");

    let mut state = DaemonState::default();
    state.last_published_seq = 5; // Simulate prior runs.
    let mut sink = CaptureSink::default();

    let offset0 = file_byte_len(&gossip).expect("len");
    append_to_file(&gossip, b"new block\n").expect("append");
    tail_and_publish(&gossip, &mut state, "laptop", offset0, &mut sink).expect("tail");

    let events = sink.drain();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].seq, 6, "seq continues from 5 → 6");
}
