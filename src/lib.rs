//! tether-gossip — bidirectional gossip.md mirror over fleet bus.
//!
//! Provides the core types and logic for:
//! - Tailing `gossip.md` for new appends and publishing them as `wm.fleet.gossip.append` events.
//! - Applying incoming events from remote nodes as provenance-headed appends.
//! - Deduplication by `(node, seq)`.
//! - Loop-guard against self-echo.
//! - Persistent monotonic `seq` across restarts.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// The NATS subject used for gossip events.
pub const GOSSIP_SUBJECT: &str = "wm.fleet.gossip.append";

/// A gossip event published or received over the fleet bus.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GossipEvent {
    /// The originating node name (e.g. "laptop", "worknode").
    pub node: String,
    /// Monotonic per-node counter, persisted across restarts.
    pub seq: u64,
    /// Unix timestamp (seconds since epoch) when the event was created.
    pub ts: u64,
    /// The appended text block from gossip.md.
    pub body: String,
}

/// Unique identity for a gossip event, used for deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventId {
    /// Node that produced the event.
    pub node: String,
    /// Sequence number at that node.
    pub seq: u64,
}

impl From<&GossipEvent> for EventId {
    fn from(ev: &GossipEvent) -> Self {
        Self {
            node: ev.node.clone(),
            seq: ev.seq,
        }
    }
}

/// Persistent state for the tether-gossip daemon on a given node.
///
/// Stored as JSON in `~/.cache/wm-tether-gossip/state`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DaemonState {
    /// Last seq this node published. Monotonically increasing.
    pub last_published_seq: u64,
    /// Last seq applied per remote node (`node_name` → `last_applied_seq`).
    pub last_applied: std::collections::HashMap<String, u64>,
    /// Seen event IDs for dedup (bounded; old entries pruned when > MAX_DEDUP).
    pub seen_ids: Vec<(String, u64)>,
}

/// Maximum number of seen IDs to retain for deduplication.
const MAX_DEDUP: usize = 10_000;

impl DaemonState {
    /// Load state from `path`, or return a fresh default if it doesn't exist.
    ///
    /// # Errors
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = std::fs::read_to_string(path)
            .with_context(|| format!("reading state from {}", path.display()))?;
        serde_json::from_str(&data)
            .with_context(|| format!("parsing state from {}", path.display()))
    }

    /// Persist state to `path` atomically (write to `.tmp`, then rename).
    ///
    /// # Errors
    /// Returns an error if the write or rename fails.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating state dir {}", parent.display()))?;
        }
        let tmp = path.with_extension("tmp");
        let data = serde_json::to_string_pretty(self).context("serializing state")?;
        std::fs::write(&tmp, data.as_bytes())
            .with_context(|| format!("writing state tmp {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("renaming state {} -> {}", tmp.display(), path.display()))
    }

    /// Check whether we have already seen `(node, seq)` and mark it seen.
    ///
    /// Returns `true` if it was already seen (duplicate), `false` if new.
    pub fn check_and_mark_seen(&mut self, node: &str, seq: u64) -> bool {
        let id = (node.to_owned(), seq);
        let already = self.seen_ids.contains(&id);
        if !already {
            self.seen_ids.push(id);
            if self.seen_ids.len() > MAX_DEDUP {
                self.seen_ids.drain(..self.seen_ids.len() - MAX_DEDUP);
            }
        }
        already
    }

    /// Return the next seq for this node and advance the counter.
    pub fn next_seq(&mut self) -> u64 {
        self.last_published_seq += 1;
        self.last_published_seq
    }
}

/// Default path for the daemon state file.
///
/// # Errors
/// Returns an error if the home/cache directory cannot be determined.
pub fn default_state_path() -> Result<PathBuf> {
    let cache = dirs::cache_dir().context("could not determine cache dir")?;
    Ok(cache.join("wm-tether-gossip").join("state"))
}

/// Default path for gossip.md, relative to the user's home dir.
///
/// # Errors
/// Returns an error if the home directory cannot be determined.
pub fn default_gossip_path() -> Result<PathBuf> {
    let home = dirs::home_dir().context("could not determine home dir")?;
    Ok(home.join("wintermute").join("autobuilder").join("notes").join("gossip.md"))
}

/// Build the provenance header for an applied remote block.
///
/// Format: `## <ts>  <node>  (via tether)\n`
#[must_use]
pub fn provenance_header(ts: u64, node: &str) -> String {
    format!("\n## {ts}  {node}  (via tether)\n")
}

/// Read the current byte length of a file (0 if it doesn't exist).
///
/// # Errors
/// Returns an error if the file cannot be stat'd.
pub fn file_byte_len(path: &Path) -> Result<u64> {
    match path.metadata() {
        Ok(m) => Ok(m.len()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e).with_context(|| format!("stat {}", path.display())),
    }
}

/// Read bytes from `path` starting at `offset`.
///
/// # Errors
/// Returns an error if the file cannot be opened or read.
pub fn read_from_offset(path: &Path, offset: u64) -> Result<Vec<u8>> {
    use std::io::Seek;
    let mut f = std::fs::File::open(path)
        .with_context(|| format!("open {}", path.display()))?;
    f.seek(std::io::SeekFrom::Start(offset))
        .with_context(|| format!("seek {} to {}", path.display(), offset))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)
        .with_context(|| format!("read {}", path.display()))?;
    Ok(buf)
}

/// Append `data` to `path`, creating the file and any parent dirs if needed.
///
/// # Errors
/// Returns an error if the file cannot be opened for appending or written to.
pub fn append_to_file(path: &Path, data: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating dir {}", parent.display()))?;
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open for append {}", path.display()))?;
    f.write_all(data)
        .with_context(|| format!("write to {}", path.display()))
}

/// Apply a remote gossip event to the local gossip file.
///
/// Appends a provenance header followed by the event body.
/// Never modifies existing content — only appends.
///
/// Returns `false` if the event was skipped (self-echo or duplicate).
///
/// # Errors
/// Returns an error if the file cannot be written to.
pub fn apply_remote_event(
    gossip_path: &Path,
    state: &mut DaemonState,
    local_node: &str,
    event: &GossipEvent,
) -> Result<bool> {
    // Loop guard: skip events from our own node.
    if event.node == local_node {
        return Ok(false);
    }

    // Dedup: skip if already seen.
    if state.check_and_mark_seen(&event.node, event.seq) {
        return Ok(false);
    }

    // Append provenance header + body.
    let header = provenance_header(event.ts, &event.node);
    let mut data = header.into_bytes();
    data.extend_from_slice(event.body.as_bytes());
    if !event.body.ends_with('\n') {
        data.push(b'\n');
    }

    append_to_file(gossip_path, &data)?;

    // Update last_applied for this node.
    state
        .last_applied
        .entry(event.node.clone())
        .and_modify(|v| *v = (*v).max(event.seq))
        .or_insert(event.seq);

    Ok(true)
}

/// A sink for published gossip events (testable abstraction).
pub trait PublishSink: Send {
    /// Publish a `GossipEvent`. Returns an error if the sink is broken.
    ///
    /// # Errors
    /// Returns an error if publishing fails.
    fn publish(&mut self, event: GossipEvent) -> Result<()>;

    /// Drain all events collected so far (for test assertions).
    fn drain(&mut self) -> Vec<GossipEvent>;
}

/// In-memory publish sink for testing.
#[derive(Debug, Default)]
pub struct CaptureSink {
    events: Vec<GossipEvent>,
}

impl PublishSink for CaptureSink {
    fn publish(&mut self, event: GossipEvent) -> Result<()> {
        self.events.push(event);
        Ok(())
    }

    fn drain(&mut self) -> Vec<GossipEvent> {
        std::mem::take(&mut self.events)
    }
}

/// Read new content from `gossip_path` starting at `known_offset`,
/// publish it as a gossip event, and return the new file offset.
///
/// If nothing new was appended, returns `known_offset` unchanged and
/// publishes nothing.
///
/// # Errors
/// Returns an error if the file cannot be read or publishing fails.
pub fn tail_and_publish(
    gossip_path: &Path,
    state: &mut DaemonState,
    local_node: &str,
    known_offset: u64,
    sink: &mut dyn PublishSink,
) -> Result<u64> {
    let current_len = file_byte_len(gossip_path)?;
    if current_len <= known_offset {
        return Ok(known_offset);
    }

    let new_bytes = read_from_offset(gossip_path, known_offset)?;
    if new_bytes.is_empty() {
        return Ok(known_offset);
    }

    let body = String::from_utf8_lossy(&new_bytes).into_owned();
    let seq = state.next_seq();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let event = GossipEvent {
        node: local_node.to_owned(),
        seq,
        ts,
        body,
    };

    sink.publish(event)?;

    Ok(current_len)
}

/// A set of seen `EventId`s built from a `DaemonState` for quick lookups.
#[must_use]
pub fn seen_set(state: &DaemonState) -> HashSet<EventId> {
    state
        .seen_ids
        .iter()
        .map(|(n, s)| EventId {
            node: n.clone(),
            seq: *s,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp_gossip(dir: &TempDir, initial: &str) -> PathBuf {
        let path = dir.path().join("gossip.md");
        std::fs::write(&path, initial).expect("write gossip");
        path
    }

    #[test]
    fn daemon_state_default_seq_starts_at_zero() {
        let s = DaemonState::default();
        assert_eq!(s.last_published_seq, 0);
    }

    #[test]
    fn daemon_state_next_seq_increments() {
        let mut s = DaemonState::default();
        assert_eq!(s.next_seq(), 1);
        assert_eq!(s.next_seq(), 2);
    }

    #[test]
    fn daemon_state_round_trips() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("state");
        let mut s = DaemonState::default();
        s.last_published_seq = 42;
        s.save(&path).expect("save");
        let loaded = DaemonState::load(&path).expect("load");
        assert_eq!(loaded.last_published_seq, 42);
    }

    #[test]
    fn dedup_marks_seen() {
        let mut s = DaemonState::default();
        assert!(!s.check_and_mark_seen("node1", 1));
        assert!(s.check_and_mark_seen("node1", 1));
        assert!(!s.check_and_mark_seen("node1", 2));
    }

    #[test]
    fn apply_remote_event_appends_with_provenance() {
        let dir = TempDir::new().expect("tempdir");
        let path = tmp_gossip(&dir, "existing line\n");
        let mut state = DaemonState::default();
        let event = GossipEvent {
            node: "worknode".to_owned(),
            seq: 1,
            ts: 1_000_000,
            body: "remote content".to_owned(),
        };
        let applied = apply_remote_event(&path, &mut state, "laptop", &event).expect("apply");
        assert!(applied);
        let content = std::fs::read_to_string(&path).expect("read");
        assert!(content.starts_with("existing line\n"), "prior content intact");
        assert!(content.contains("(via tether)"), "provenance header present");
        assert!(content.contains("remote content"), "body present");
    }

    #[test]
    fn apply_remote_event_self_echo_skipped() {
        let dir = TempDir::new().expect("tempdir");
        let path = tmp_gossip(&dir, "existing\n");
        let mut state = DaemonState::default();
        let event = GossipEvent {
            node: "laptop".to_owned(),
            seq: 1,
            ts: 0,
            body: "self".to_owned(),
        };
        let applied = apply_remote_event(&path, &mut state, "laptop", &event).expect("apply");
        assert!(!applied, "self-echo must be skipped");
        let content = std::fs::read_to_string(&path).expect("read");
        assert_eq!(content, "existing\n", "file unchanged after self-echo");
    }

    #[test]
    fn apply_remote_event_dedup() {
        let dir = TempDir::new().expect("tempdir");
        let path = tmp_gossip(&dir, "");
        let mut state = DaemonState::default();
        let event = GossipEvent {
            node: "worknode".to_owned(),
            seq: 1,
            ts: 0,
            body: "block".to_owned(),
        };
        apply_remote_event(&path, &mut state, "laptop", &event).expect("first apply");
        let applied2 =
            apply_remote_event(&path, &mut state, "laptop", &event).expect("second apply");
        assert!(!applied2, "second delivery must be skipped");
        let content = std::fs::read_to_string(&path).expect("read");
        assert_eq!(
            content.matches("block").count(),
            1,
            "block appears exactly once"
        );
    }

    #[test]
    fn tail_publishes_single_event() {
        let dir = TempDir::new().expect("tempdir");
        let path = tmp_gossip(&dir, "initial\n");
        let mut state = DaemonState::default();
        let mut sink = CaptureSink::default();

        // Nothing to publish yet (offset = file length).
        let offset0 = file_byte_len(&path).expect("len");
        let new_off =
            tail_and_publish(&path, &mut state, "laptop", offset0, &mut sink).expect("tail");
        assert_eq!(new_off, offset0);
        assert!(sink.drain().is_empty());

        // Append to file.
        append_to_file(&path, b"new block\n").expect("append");
        let new_off2 =
            tail_and_publish(&path, &mut state, "laptop", offset0, &mut sink).expect("tail2");
        assert!(new_off2 > offset0);
        let events = sink.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].node, "laptop");
        assert_eq!(events[0].seq, 1);
        assert!(events[0].body.contains("new block"));
    }

    #[test]
    fn provenance_header_format() {
        let h = provenance_header(1_000_000, "worknode");
        assert!(h.contains("1000000"));
        assert!(h.contains("worknode"));
        assert!(h.contains("(via tether)"));
    }
}
