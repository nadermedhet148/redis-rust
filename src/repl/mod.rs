//! Leader → replica replication (asynchronous, like Redis' master/replica).
//!
//! - `backlog`: the ordered stream of encoded write records, addressed by byte offset.
//! - `stream`:  framing on the wire (handshake lines, WAL records, heartbeats).
//! - `leader`:  serves one replica connection: full or partial sync, then streaming.
//!
//! The full flow is described in docs/REPLICATION.md.

pub mod backlog;
pub mod leader;
pub mod stream;

use std::collections::BTreeMap;
use std::hash::{BuildHasher, RandomState};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::db::Db;

/// One server process: its data plus its replication identity and state.
pub struct Node {
    pub db: Db,
    /// Random ID of this node's write stream, new on every start. Offsets only
    /// mean something together with the replid they belong to.
    pub replid: String,
    backlog_capacity: usize,
    replicas: Mutex<BTreeMap<u64, Arc<ReplicaLink>>>,
    next_replica_id: AtomicU64,
    pub full_syncs: AtomicU64,
    pub partial_syncs: AtomicU64,
}

/// A replica connected to this node, as the leader sees it.
pub struct ReplicaLink {
    pub peer: SocketAddr,
    /// Last offset the replica reported with `ACK`.
    pub acked: AtomicU64,
}

impl Node {
    pub fn new(db: Db) -> Arc<Self> {
        Self::with_backlog(db, backlog::DEFAULT_CAPACITY)
    }

    pub fn with_backlog(db: Db, backlog_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            db,
            replid: new_replid(),
            backlog_capacity,
            replicas: Mutex::default(),
            next_replica_id: AtomicU64::new(0),
            full_syncs: AtomicU64::new(0),
            partial_syncs: AtomicU64::new(0),
        })
    }

    /// Offset of the end of this node's write stream (0 until a replica attaches).
    pub fn offset(&self) -> u64 {
        self.db.backlog().map_or(0, |b| b.end())
    }

    /// `ROLE` reply for a leader.
    pub fn describe_leader(&self) -> String {
        let offset = self.offset();
        let replicas = self.replicas.lock().unwrap();
        let mut out = format!(
            "leader replid={} offset={offset} full_syncs={} partial_syncs={} replicas={}",
            self.replid,
            self.full_syncs.load(Ordering::Relaxed),
            self.partial_syncs.load(Ordering::Relaxed),
            replicas.len()
        );
        for link in replicas.values() {
            let acked = link.acked.load(Ordering::Relaxed);
            out += &format!(
                " replica={},acked={acked},lag={}",
                link.peer,
                offset.saturating_sub(acked)
            );
        }
        out
    }

    /// Track a connected replica until the returned guard is dropped.
    fn register_replica(self: &Arc<Self>, peer: SocketAddr, offset: u64) -> ReplicaGuard {
        let id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        let link = Arc::new(ReplicaLink {
            peer,
            acked: AtomicU64::new(offset),
        });
        self.replicas.lock().unwrap().insert(id, link.clone());
        ReplicaGuard {
            node: self.clone(),
            id,
            link,
        }
    }
}

struct ReplicaGuard {
    node: Arc<Node>,
    id: u64,
    link: Arc<ReplicaLink>,
}

impl Drop for ReplicaGuard {
    fn drop(&mut self) {
        self.node.replicas.lock().unwrap().remove(&self.id);
    }
}

/// 32 hex chars. `RandomState` is seeded randomly per process; mixing in the
/// clock makes two nodes started in the same process differ too.
fn new_replid() -> String {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let a = RandomState::new().hash_one(now);
    let b = RandomState::new().hash_one((now, std::process::id()));
    format!("{a:016x}{b:016x}")
}
