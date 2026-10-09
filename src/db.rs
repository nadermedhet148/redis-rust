use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock, RwLockWriteGuard};

use bytes::Bytes;

use crate::repl::backlog::Backlog;
use crate::wal::{self, FsyncPolicy, Record, ReplayStats, Wal};

type Map = HashMap<String, Bytes>;

/// Shard count for `StoreKind::Sharded`. A power of two so picking a shard is a mask.
pub const DEFAULT_SHARDS: usize = 64;

/// Which locking strategy backs the store. ep02 benchmarks them against each other.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum StoreKind {
    /// One global Mutex: every op (read or write) is exclusive.
    #[default]
    Mutex,
    /// One global RwLock: GETs run in parallel, SET/DEL are exclusive.
    Rwlock,
    /// N independent RwLock<HashMap>s, picked by hash(key): ops on different
    /// shards never contend.
    Sharded,
}

/// Shared store. Cloning is cheap: every clone points at the same data.
///
/// With a WAL attached, every SET/DEL is appended to the log *while holding the
/// lock that protects that key*, then applied. So two writes to the same key
/// reach the log in the same order they reach memory, and replay rebuilds
/// exactly the state clients saw.
///
/// Once a replica attaches, the same encoded record is also pushed to the
/// replication backlog, under the same lock, so replicas see per-key writes in
/// the same order too.
#[derive(Clone, Default)]
pub struct Db {
    inner: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    store: Inner,
    wal: Option<Wal>,
    /// Created when the first replica attaches (like Redis), so a server with
    /// no replicas pays nothing for replication.
    backlog: OnceLock<Backlog>,
}

enum Inner {
    Mutex(Mutex<Map>),
    RwLock(RwLock<Map>),
    Sharded(Sharded),
}

struct Sharded {
    shards: Box<[RwLock<Map>]>,
    hasher: RandomState,
}

impl Sharded {
    fn new(n: usize) -> Self {
        assert!(n.is_power_of_two(), "shard count must be a power of two");
        Self {
            shards: (0..n).map(|_| RwLock::default()).collect(),
            hasher: RandomState::new(),
        }
    }

    fn shard(&self, key: &str) -> &RwLock<Map> {
        let h = self.hasher.hash_one(key) as usize;
        &self.shards[h & (self.shards.len() - 1)]
    }
}

impl Default for Inner {
    fn default() -> Self {
        Inner::Mutex(Mutex::default())
    }
}

impl Db {
    /// In-memory only: nothing survives a restart.
    pub fn new(kind: StoreKind) -> Self {
        Self {
            inner: Arc::new(Shared {
                store: Inner::new(kind),
                wal: None,
                backlog: OnceLock::new(),
            }),
        }
    }

    /// Durable: replay the WAL at `path` into memory, then log every write to it.
    pub fn open(
        kind: StoreKind,
        path: impl AsRef<Path>,
        policy: FsyncPolicy,
    ) -> io::Result<(Self, ReplayStats)> {
        let store = Inner::new(kind);
        let (wal, stats) = Wal::open(path, policy, |record| match record {
            Record::Set { key, value } => {
                store.write_map(&key).insert(key, value);
            }
            Record::Del { key } => {
                store.write_map(&key).remove(&key);
            }
        })?;
        let db = Self {
            inner: Arc::new(Shared {
                store,
                wal: Some(wal),
                backlog: OnceLock::new(),
            }),
        };
        Ok((db, stats))
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        // Bytes::clone is a refcount bump, not a copy of the value.
        match &self.inner.store {
            Inner::Mutex(m) => m.lock().unwrap().get(key).cloned(),
            Inner::RwLock(m) => m.read().unwrap().get(key).cloned(),
            Inner::Sharded(s) => s.shard(key).read().unwrap().get(key).cloned(),
        }
    }

    pub fn set(&self, key: String, value: Bytes) -> io::Result<()> {
        let mut map = self.inner.store.write_map(&key);
        self.log(|| wal::encode_set(&key, &value))?;
        map.insert(key, value);
        Ok(())
    }

    /// Returns true if the key existed.
    pub fn del(&self, key: &str) -> io::Result<bool> {
        let mut map = self.inner.store.write_map(key);
        if !map.contains_key(key) {
            return Ok(false); // nothing to change, nothing to log
        }
        self.log(|| wal::encode_del(key))?;
        map.remove(key);
        Ok(true)
    }

    /// Apply a record received from a leader (or read from a backup).
    pub fn apply(&self, record: Record) -> io::Result<()> {
        match record {
            Record::Set { key, value } => self.set(key, value),
            Record::Del { key } => self.del(&key).map(|_| ()),
        }
    }

    /// Called with the key's lock held: WAL first (a failure means the write is
    /// not applied), then the replication backlog.
    fn log(&self, encode: impl FnOnce() -> Vec<u8>) -> io::Result<()> {
        let backlog = self.inner.backlog.get();
        if self.inner.wal.is_none() && backlog.is_none() {
            return Ok(()); // pure in-memory, no replicas: don't even encode
        }
        let record = encode();
        if let Some(wal) = &self.inner.wal {
            wal.append(&record)?;
        }
        if let Some(backlog) = backlog {
            backlog.push(&record);
        }
        Ok(())
    }

    /// The replication backlog, if a replica ever attached.
    pub fn backlog(&self) -> Option<&Backlog> {
        self.inner.backlog.get()
    }

    /// Start recording writes for replicas (no-op if already started).
    pub fn enable_backlog(&self, capacity: usize) -> &Backlog {
        self.inner.backlog.get_or_init(|| Backlog::new(capacity))
    }

    /// Number of independently locked maps (1 unless the store is sharded).
    pub fn shard_count(&self) -> usize {
        match &self.inner.store {
            Inner::Sharded(s) => s.shards.len(),
            _ => 1,
        }
    }

    /// Copy of one shard's entries. Only that shard is locked, and only while
    /// copying (values are `Bytes`, so a copy is a refcount bump). Shards are
    /// copied at different moments: a full snapshot made this way is "fuzzy".
    pub fn snapshot_shard(&self, shard: usize) -> Vec<(String, Bytes)> {
        let copy = |m: &Map| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        match &self.inner.store {
            Inner::Mutex(m) => copy(&m.lock().unwrap()),
            Inner::RwLock(m) => copy(&m.read().unwrap()),
            Inner::Sharded(s) => copy(&s.shards[shard].read().unwrap()),
        }
    }

    /// Number of keys.
    pub fn len(&self) -> usize {
        match &self.inner.store {
            Inner::Mutex(m) => m.lock().unwrap().len(),
            Inner::RwLock(m) => m.read().unwrap().len(),
            Inner::Sharded(s) => s.shards.iter().map(|m| m.read().unwrap().len()).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// XOR of `entry_hash` over all entries: independent of insertion order and
    /// of the store kind, so two nodes with the same data have the same digest.
    pub fn digest(&self) -> u64 {
        (0..self.shard_count())
            .flat_map(|i| self.snapshot_shard(i))
            .fold(0, |acc, (k, v)| acc ^ entry_hash(&k, &v))
    }

    /// Delete every key. Each delete is logged like a normal DEL, so the WAL
    /// (and any replicas of this node) stay correct.
    pub fn clear(&self) -> io::Result<()> {
        for shard in 0..self.shard_count() {
            for (key, _) in self.snapshot_shard(shard) {
                self.del(&key)?;
            }
        }
        Ok(())
    }
}

/// FNV-1a 64 over `key 0xFF value`. Deterministic across processes (unlike
/// `RandomState`), so digests can be compared between nodes.
pub fn entry_hash(key: &str, value: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key.as_bytes().iter().chain(&[0xFF]).chain(value) {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

impl Inner {
    fn new(kind: StoreKind) -> Self {
        match kind {
            StoreKind::Mutex => Inner::Mutex(Mutex::default()),
            StoreKind::Rwlock => Inner::RwLock(RwLock::default()),
            StoreKind::Sharded => Inner::Sharded(Sharded::new(DEFAULT_SHARDS)),
        }
    }

    /// Exclusive access to the map that owns `key`.
    fn write_map(&self, key: &str) -> MapGuard<'_> {
        match self {
            Inner::Mutex(m) => MapGuard::Mutex(m.lock().unwrap()),
            Inner::RwLock(m) => MapGuard::RwLock(m.write().unwrap()),
            Inner::Sharded(s) => MapGuard::RwLock(s.shard(key).write().unwrap()),
        }
    }
}

enum MapGuard<'a> {
    Mutex(MutexGuard<'a, Map>),
    RwLock(RwLockWriteGuard<'a, Map>),
}

impl Deref for MapGuard<'_> {
    type Target = Map;
    fn deref(&self) -> &Map {
        match self {
            MapGuard::Mutex(g) => g,
            MapGuard::RwLock(g) => g,
        }
    }
}

impl DerefMut for MapGuard<'_> {
    fn deref_mut(&mut self) -> &mut Map {
        match self {
            MapGuard::Mutex(g) => g,
            MapGuard::RwLock(g) => g,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KINDS: [StoreKind; 3] = [StoreKind::Mutex, StoreKind::Rwlock, StoreKind::Sharded];

    fn fill(db: &Db, keys: impl Iterator<Item = usize>) {
        for i in keys {
            db.set(format!("k{i}"), Bytes::from(format!("v{i}")))
                .unwrap();
        }
    }

    #[test]
    fn digest_ignores_order_and_store_kind() {
        let digests: Vec<u64> = KINDS
            .iter()
            .enumerate()
            .map(|(n, &kind)| {
                let db = Db::new(kind);
                if n % 2 == 0 {
                    fill(&db, 0..500);
                } else {
                    fill(&db, (0..500).rev());
                }
                assert_eq!(db.len(), 500);
                db.digest()
            })
            .collect();
        assert!(digests.iter().all(|&d| d == digests[0]));

        let db = Db::new(StoreKind::Sharded);
        fill(&db, 0..500);
        db.set("k1".into(), Bytes::from("changed")).unwrap();
        assert_ne!(db.digest(), digests[0]);
    }

    #[test]
    fn backlog_records_writes_only_after_it_is_enabled() {
        let db = Db::new(StoreKind::Sharded);
        db.set("before".into(), Bytes::from("x")).unwrap();
        assert!(db.backlog().is_none());

        let backlog = db.enable_backlog(1024);
        db.set("a".into(), Bytes::from("1")).unwrap();
        db.del("a").unwrap();
        db.del("missing").unwrap(); // not a change: not logged

        let bytes = backlog.read_from(0, usize::MAX).unwrap();
        let mut r = bytes.as_slice();
        let mut records = Vec::new();
        while let Some((rec, _)) = wal::read_record(&mut r).unwrap() {
            records.push(rec);
        }
        assert_eq!(
            records,
            vec![
                Record::Set {
                    key: "a".into(),
                    value: Bytes::from("1")
                },
                Record::Del { key: "a".into() },
            ]
        );
    }

    #[test]
    fn snapshot_shards_cover_every_key_once() {
        for kind in KINDS {
            let db = Db::new(kind);
            fill(&db, 0..300);
            let mut keys: Vec<String> = (0..db.shard_count())
                .flat_map(|i| db.snapshot_shard(i))
                .map(|(k, _)| k)
                .collect();
            keys.sort();
            keys.dedup();
            assert_eq!(keys.len(), 300, "{kind:?}");
        }
    }

    #[test]
    fn clear_is_logged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        {
            let (db, _) = Db::open(StoreKind::Sharded, &path, FsyncPolicy::Never).unwrap();
            fill(&db, 0..50);
            db.clear().unwrap();
            assert!(db.is_empty());
            db.set("after".into(), Bytes::from("clear")).unwrap();
        }
        let (db, _) = Db::open(StoreKind::Sharded, &path, FsyncPolicy::Never).unwrap();
        assert_eq!(db.len(), 1);
        assert_eq!(db.get("after"), Some(Bytes::from("clear")));
    }
}
