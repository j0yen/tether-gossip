//! wm-tether-gossip — bidirectional gossip.md mirror over fleet bus.
//!
//! # Usage
//!
//! ```text
//! wm-tether-gossip [OPTIONS] <COMMAND>
//!   run     Start the tail-and-apply daemon
//!   status  Show last published/applied seq per node
//! ```

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tether_gossip::{
    apply_remote_event, default_gossip_path, default_state_path, file_byte_len, tail_and_publish,
    DaemonState, GossipEvent, PublishSink, GOSSIP_SUBJECT,
};

// sigpipe::reset() MUST be the first statement in main().
// Asserted by AC7 grep check.
fn main() -> Result<()> {
    sigpipe::reset();
    let args = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    match args.command {
        Command::Run(opts) => run_daemon(opts),
        Command::Status(opts) => run_status(opts),
    }
}

/// Bidirectional gossip.md mirror over fleet bus.
#[derive(Debug, Parser)]
#[command(name = "wm-tether-gossip", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the tail-and-apply daemon.
    Run(RunOpts),
    /// Show last published/applied seq per known node.
    Status(StatusOpts),
}

#[derive(Debug, Parser)]
struct RunOpts {
    /// Path to gossip.md (defaults to ~/wintermute/autobuilder/notes/gossip.md).
    #[arg(long)]
    gossip: Option<PathBuf>,
    /// Path to daemon state file (defaults to ~/.cache/wm-tether-gossip/state).
    #[arg(long)]
    state: Option<PathBuf>,
    /// This node's name (defaults to hostname).
    #[arg(long)]
    node: Option<String>,
    /// NATS server URL.
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    nats: String,
    /// Poll interval for file tail in milliseconds.
    #[arg(long, default_value = "500")]
    poll_ms: u64,
    /// If true, skip NATS connection and report no link configured.
    #[arg(long)]
    no_link: bool,
}

#[derive(Debug, Parser)]
struct StatusOpts {
    /// Path to daemon state file.
    #[arg(long)]
    state: Option<PathBuf>,
}

/// A `PublishSink` backed by an async-nats client.
struct NatsSink {
    client: async_nats::Client,
    rt: tokio::runtime::Handle,
}

impl PublishSink for NatsSink {
    fn publish(&mut self, event: GossipEvent) -> Result<()> {
        let payload = serde_json::to_vec(&event).context("serialize event")?;
        let client = self.client.clone();
        self.rt.block_on(async move {
            client
                .publish(GOSSIP_SUBJECT, payload.into())
                .await
                .context("nats publish")
        })
    }

    fn drain(&mut self) -> Vec<GossipEvent> {
        vec![]
    }
}

fn local_node_name(opt: Option<String>) -> String {
    opt.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "unknown".to_owned())
    })
}

#[tokio::main]
async fn run_daemon(opts: RunOpts) -> Result<()> {
    let gossip_path = match opts.gossip {
        Some(p) => p,
        None => default_gossip_path()?,
    };
    let state_path = match opts.state {
        Some(p) => p,
        None => default_state_path()?,
    };
    let local_node = local_node_name(opts.node);

    let mut state = DaemonState::load(&state_path)?;
    let mut known_offset = file_byte_len(&gossip_path).unwrap_or(0);

    if opts.no_link {
        tracing::warn!("no-link mode: NATS connection skipped, running tail-only");
        eprintln!("tether-gossip: no link configured — running in tail-only mode (no publish/apply)");
        return Ok(());
    }

    // Connect to NATS.
    let client = async_nats::connect(&opts.nats)
        .await
        .with_context(|| format!("connecting to NATS at {}", opts.nats))?;

    let mut subscriber = client
        .subscribe(GOSSIP_SUBJECT)
        .await
        .context("subscribing to gossip subject")?;

    let rt_handle = tokio::runtime::Handle::current();
    let mut sink = NatsSink {
        client: client.clone(),
        rt: rt_handle,
    };

    let poll_interval = std::time::Duration::from_millis(opts.poll_ms);
    let mut tick = tokio::time::interval(poll_interval);

    tracing::info!(node = %local_node, gossip = %gossip_path.display(), "tether-gossip daemon started");

    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Tail and publish new appends.
                match tail_and_publish(&gossip_path, &mut state, &local_node, known_offset, &mut sink) {
                    Ok(new_off) => {
                        if new_off != known_offset {
                            tracing::debug!(new_off, "published new gossip block");
                            known_offset = new_off;
                            if let Err(e) = state.save(&state_path) {
                                tracing::error!("failed to save state: {e:#}");
                            }
                        }
                    }
                    Err(e) => tracing::error!("tail error: {e:#}"),
                }
            }
            msg = subscriber.next() => {
                let Some(msg) = msg else { break; };
                match serde_json::from_slice::<GossipEvent>(&msg.payload) {
                    Ok(event) => {
                        match apply_remote_event(&gossip_path, &mut state, &local_node, &event) {
                            Ok(true) => {
                                tracing::info!(node = %event.node, seq = event.seq, "applied remote gossip block");
                                // After applying, advance our known_offset to avoid re-tailing the applied block.
                                known_offset = file_byte_len(&gossip_path).unwrap_or(known_offset);
                                if let Err(e) = state.save(&state_path) {
                                    tracing::error!("failed to save state: {e:#}");
                                }
                            }
                            Ok(false) => {
                                tracing::debug!(node = %event.node, seq = event.seq, "skipped (self-echo or dedup)");
                            }
                            Err(e) => tracing::error!("apply error: {e:#}"),
                        }
                    }
                    Err(e) => tracing::warn!("unparseable gossip event: {e:#}"),
                }
            }
        }
    }

    Ok(())
}

fn run_status(opts: StatusOpts) -> Result<()> {
    let state_path = match opts.state {
        Some(p) => p,
        None => default_state_path()?,
    };

    let state = DaemonState::load(&state_path)?;
    println!("last_published_seq: {}", state.last_published_seq);
    println!("seen_ids_count: {}", state.seen_ids.len());
    if state.last_applied.is_empty() {
        println!("last_applied: (none)");
    } else {
        println!("last_applied:");
        let mut nodes: Vec<_> = state.last_applied.iter().collect();
        nodes.sort_by_key(|(k, _)| k.as_str());
        for (node, seq) in nodes {
            println!("  {node}: {seq}");
        }
    }
    Ok(())
}

// Allow unused import — async_nats::Subscriber uses this internally via futures.
use futures::StreamExt as _;
