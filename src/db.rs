use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;

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

/// Shared in-memory store. Cloning is cheap: every clone points at the same data.
#[derive(Clone, Default)]
pub struct Db {
    inner: Arc<Inner>,
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
    pub fn new(kind: StoreKind) -> Self {
        let inner = match kind {
            StoreKind::Mutex => Inner::Mutex(Mutex::default()),
            StoreKind::Rwlock => Inner::RwLock(RwLock::default()),
            StoreKind::Sharded => Inner::Sharded(Sharded::new(DEFAULT_SHARDS)),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        // Bytes::clone is a refcount bump, not a copy of the value.
        match &*self.inner {
            Inner::Mutex(m) => m.lock().unwrap().get(key).cloned(),
            Inner::RwLock(m) => m.read().unwrap().get(key).cloned(),
            Inner::Sharded(s) => s.shard(key).read().unwrap().get(key).cloned(),
        }
    }

    pub fn set(&self, key: String, value: Bytes) {
        match &*self.inner {
            Inner::Mutex(m) => m.lock().unwrap().insert(key, value),
            Inner::RwLock(m) => m.write().unwrap().insert(key, value),
            Inner::Sharded(s) => s.shard(&key).write().unwrap().insert(key, value),
        };
    }

    /// Returns true if the key existed.
    pub fn del(&self, key: &str) -> bool {
        match &*self.inner {
            Inner::Mutex(m) => m.lock().unwrap().remove(key).is_some(),
            Inner::RwLock(m) => m.write().unwrap().remove(key).is_some(),
            Inner::Sharded(s) => s.shard(key).write().unwrap().remove(key).is_some(),
        }
    }
}
