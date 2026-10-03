use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

/// Shared in-memory store. Cloning is cheap: every clone points at the same map.
///
/// ep01: one global `Mutex<HashMap>`. Simple and correct, but every
/// operation serializes on a single lock — ep02 measures how much that costs.
#[derive(Clone, Default)]
pub struct Db {
    inner: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl Db {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        // Bytes::clone is a refcount bump, not a copy of the value.
        self.inner.lock().unwrap().get(key).cloned()
    }

    pub fn set(&self, key: String, value: Bytes) {
        self.inner.lock().unwrap().insert(key, value);
    }

    /// Returns true if the key existed.
    pub fn del(&self, key: &str) -> bool {
        self.inner.lock().unwrap().remove(key).is_some()
    }
}
