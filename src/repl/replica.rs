//! Replica side: keep a connection to the leader, apply everything it sends.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{info, warn};

use super::leader::HEARTBEAT_EVERY;
use super::stream::{Frame, SyncReply, read_frame};
use crate::db::Db;

/// No data and no heartbeat for this long: the leader is considered gone.
pub const LEADER_TIMEOUT: Duration = Duration::from_secs(5);
const ACK_EVERY: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Not connected (yet, or again).
    Connecting,
    /// Loading a snapshot, or replaying the stream up to `consistent_at`: the
    /// data is not a consistent copy of the leader yet.
    Syncing,
    /// Consistent, applying the live stream.
    Streaming,
}

/// What a replica knows about its sync with the leader. Shared with `ROLE`.
pub struct Status {
    pub leader: String,
    progress: Mutex<Progress>,
}

#[derive(Debug, Clone)]
pub struct Progress {
    pub state: State,
    /// The leader stream we are a copy of. `None` until a full sync finishes
    /// loading its snapshot, so a half-loaded snapshot is never "resumed".
    pub leader_replid: Option<String>,
    /// How far into that stream we have applied.
    pub offset: u64,
    pub consistent_at: u64,
}

impl Status {
    pub fn new(leader: String) -> Arc<Self> {
        Arc::new(Self {
            leader,
            progress: Mutex::new(Progress {
                state: State::Connecting,
                leader_replid: None,
                offset: 0,
                consistent_at: 0,
            }),
        })
    }

    pub fn progress(&self) -> Progress {
        self.progress.lock().unwrap().clone()
    }

    fn set_state(&self, state: State) {
        self.progress.lock().unwrap().state = state;
    }

    /// One record applied.
    fn advance(&self, size: u64) {
        let mut p = self.progress.lock().unwrap();
        p.offset += size;
        if p.state == State::Syncing && p.offset >= p.consistent_at {
            p.state = State::Streaming;
        }
    }
}

/// Spawn the replication loop for `db`, following `status.leader`.
pub fn spawn(db: Db, status: Arc<Status>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(100);
        loop {
            match sync_once(&db, &status).await {
                Ok(()) => info!(leader = %status.leader, "leader closed the connection"),
                Err(e) => warn!(leader = %status.leader, error = %e, "replication link down"),
            }
            if status.progress().state != State::Connecting {
                backoff = Duration::from_millis(100); // we were connected: retry fast
            }
            status.set_state(State::Connecting);
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    })
}

/// One connection's life: handshake, maybe load a snapshot, then apply the
/// stream until the connection breaks.
async fn sync_once(db: &Db, status: &Arc<Status>) -> anyhow::Result<()> {
    let stream = timeout(LEADER_TIMEOUT, TcpStream::connect(&status.leader)).await??;
    stream.set_nodelay(true)?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let resume = status.progress();
    let request = match &resume.leader_replid {
        Some(id) => format!("SYNC {id} {}\n", resume.offset),
        None => "SYNC ? -1\n".to_string(),
    };
    writer.write_all(request.as_bytes()).await?;

    let mut line = String::new();
    if timeout(LEADER_TIMEOUT, reader.read_line(&mut line)).await?? == 0 {
        anyhow::bail!("leader closed the connection during the handshake");
    }
    match SyncReply::parse(line.trim_end())? {
        SyncReply::FullSync {
            replid,
            offset,
            consistent_at,
            keys,
        } => {
            info!(leader = %status.leader, %replid, keys, offset, "full sync: loading snapshot");
            *status.progress.lock().unwrap() = Progress {
                state: State::Syncing,
                leader_replid: None,
                offset,
                consistent_at,
            };
            db.clear()?;
            for _ in 0..keys {
                match timeout(LEADER_TIMEOUT, read_frame(&mut reader)).await?? {
                    Frame::Record(record, _) => db.apply(record)?,
                    Frame::Heartbeat => anyhow::bail!("heartbeat inside a snapshot"),
                }
            }
            let mut p = status.progress.lock().unwrap();
            p.leader_replid = Some(replid);
            if p.offset >= p.consistent_at {
                p.state = State::Streaming;
            }
        }
        SyncReply::Continue { replid } => {
            anyhow::ensure!(
                resume.leader_replid.as_ref() == Some(&replid),
                "leader continued a stream we didn't ask for"
            );
            info!(leader = %status.leader, offset = resume.offset, "partial resync");
            status.set_state(State::Streaming);
        }
    }

    // ACKs go out from their own task: `read_frame` below must not be
    // interrupted halfway through a record.
    let _acks = AbortOnDrop(tokio::spawn({
        let status = status.clone();
        async move {
            let mut tick = tokio::time::interval(ACK_EVERY);
            loop {
                tick.tick().await;
                let offset = status.progress().offset;
                if writer
                    .write_all(format!("ACK {offset}\n").as_bytes())
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }));

    loop {
        match timeout(LEADER_TIMEOUT, read_frame(&mut reader)).await?? {
            Frame::Heartbeat => {}
            Frame::Record(record, size) => {
                db.apply(record)?;
                status.advance(size);
            }
        }
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

const _: () = assert!(LEADER_TIMEOUT.as_secs() > HEARTBEAT_EVERY.as_secs() * 2);
