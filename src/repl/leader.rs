//! Leader side of one replica connection.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt, BufReader, BufWriter, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tracing::{info, warn};

use super::Node;
use super::backlog::Backlog;
use super::stream::{HEARTBEAT, SyncReply};
use crate::command::Command;
use crate::wal;

/// Bytes sent per write while streaming.
const CHUNK: usize = 64 * 1024;
pub const HEARTBEAT_EVERY: Duration = Duration::from_secs(1);

/// A client sent `SYNC`: this connection now belongs to a replica.
///
/// 1. Partial resync if the replica's (replid, offset) is still in our
///    backlog, otherwise a full sync (snapshot).
/// 2. Then stream every new record forever, with a heartbeat every second,
///    and read `ACK`s coming back.
pub async fn serve(
    node: Arc<Node>,
    mut lines: Lines<BufReader<OwnedReadHalf>>,
    writer: OwnedWriteHalf,
    peer: SocketAddr,
    replid: Option<String>,
    offset: Option<u64>,
) -> anyhow::Result<()> {
    let backlog = node.db.enable_backlog(node.backlog_capacity);
    let mut out = BufWriter::new(writer);

    let mut pos = match (replid, offset) {
        (Some(id), Some(offset)) if id == node.replid && backlog.contains(offset) => {
            node.partial_syncs.fetch_add(1, Ordering::Relaxed);
            info!(%peer, offset, "replica: partial resync");
            let reply = SyncReply::Continue {
                replid: node.replid.clone(),
            };
            out.write_all(reply.to_line().as_bytes()).await?;
            offset
        }
        (asked_id, asked_offset) => {
            node.full_syncs.fetch_add(1, Ordering::Relaxed);
            info!(%peer, ?asked_id, ?asked_offset, "replica: full sync");
            full_sync(&node, backlog, &mut out).await?
        }
    };
    out.flush().await?;

    let replica = node.register_replica(peer, pos);
    let mut end = backlog.subscribe();
    let mut heartbeat = tokio::time::interval(HEARTBEAT_EVERY);

    loop {
        // Send everything we have, then wait for more.
        let chunk = match backlog.read_from(pos, CHUNK) {
            Ok(chunk) => chunk,
            Err(e) => {
                // The replica fell so far behind that the backlog overwrote what
                // it still needs. Drop it; it will reconnect and full sync.
                warn!(%peer, pos, ?e, "replica too far behind, disconnecting");
                return Ok(());
            }
        };
        if !chunk.is_empty() {
            out.write_all(&chunk).await?;
            pos += chunk.len() as u64;
            continue;
        }
        out.flush().await?;

        tokio::select! {
            changed = end.changed() => changed?,
            _ = heartbeat.tick() => {
                out.write_all(&HEARTBEAT).await?;
                out.flush().await?;
            }
            line = lines.next_line() => match line? {
                Some(line) => match Command::parse(&line) {
                    Ok(Command::Ack { offset }) => {
                        replica.link.acked.store(offset, Ordering::Relaxed)
                    }
                    _ => warn!(%peer, line, "unexpected message from replica"),
                },
                None => {
                    info!(%peer, "replica disconnected");
                    return Ok(());
                }
            },
        }
    }
}

/// Send a snapshot and return the offset the stream continues from.
///
/// The snapshot is "fuzzy": shards are copied one after another while writes
/// keep going. That's fine because every record is a blind write (SET or DEL
/// of a whole key): starting from *any* state copied after offset `start`,
/// replaying the stream from `start` ends in the leader's exact state. Once the
/// replica has replayed up to `consistent_at` (the end of the stream when the
/// copy finished), its data is a real point-in-time copy.
async fn full_sync(
    node: &Node,
    backlog: &Backlog,
    out: &mut (impl AsyncWrite + Unpin),
) -> std::io::Result<u64> {
    let start = backlog.end();
    let entries: Vec<_> = (0..node.db.shard_count())
        .flat_map(|i| node.db.snapshot_shard(i))
        .collect();
    let consistent_at = backlog.end();

    let reply = SyncReply::FullSync {
        replid: node.replid.clone(),
        offset: start,
        consistent_at,
        keys: entries.len() as u64,
    };
    out.write_all(reply.to_line().as_bytes()).await?;
    for (key, value) in entries {
        out.write_all(&wal::encode_set(&key, &value)).await?;
    }
    Ok(start)
}
