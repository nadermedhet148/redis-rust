use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;

type Map = HashMap<String, Bytes>;

/// Which locking strategy backs the store. ep02 benchmarks them against each other.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum StoreKind {
    /// One global Mutex: every op (read or write) is exclusive.
    #[default]
    Mutex,
    /// One global RwLock: GETs run in parallel, SET/DEL are exclusive.
    Rwlock,
}

/// Shared in-memory store. Cloning is cheap: every clone points at the same data.
#[derive(Clone, Default)]
pub struct Db {
    inner: Arc<Inner>,
}

enum Inner {
    Mutex(Mutex<Map>),
    RwLock(RwLock<Map>),
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
        }
    }

    pub fn set(&self, key: String, value: Bytes) {
        match &*self.inner {
            Inner::Mutex(m) => m.lock().unwrap().insert(key, value),
            Inner::RwLock(m) => m.write().unwrap().insert(key, value),
        };
    }

    /// Returns true if the key existed.
    pub fn del(&self, key: &str) -> bool {
        match &*self.inner {
            Inner::Mutex(m) => m.lock().unwrap().remove(key).is_some(),
            Inner::RwLock(m) => m.write().unwrap().remove(key).is_some(),
        }
    }
}
