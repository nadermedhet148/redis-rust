use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockWriteGuard};

use bytes::Bytes;

use crate::wal::{FsyncPolicy, Record, ReplayStats, Wal};

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
#[derive(Clone, Default)]
pub struct Db {
    inner: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    store: Inner,
    wal: Option<Wal>,
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
        if let Some(wal) = &self.inner.wal {
            wal.append_set(&key, &value)?;
        }
        map.insert(key, value);
        Ok(())
    }

    /// Returns true if the key existed.
    pub fn del(&self, key: &str) -> io::Result<bool> {
        let mut map = self.inner.store.write_map(key);
        if !map.contains_key(key) {
            return Ok(false); // nothing to change, nothing to log
        }
        if let Some(wal) = &self.inner.wal {
            wal.append_del(key)?;
        }
        map.remove(key);
        Ok(true)
    }
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
