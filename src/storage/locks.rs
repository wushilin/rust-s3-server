use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectLockKey {
    pub bucket: String,
    pub object_key: String,
}

#[derive(Debug, Default)]
pub struct ObjectLockTable {
    inner: Arc<Mutex<HashMap<ObjectLockKey, Arc<Mutex<()>>>>>,
}

impl Clone for ObjectLockTable {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl ObjectLockTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn lock(&self, bucket: &str, object_key: &str) -> ObjectWriteGuard {
        let key = ObjectLockKey {
            bucket: bucket.to_string(),
            object_key: object_key.to_string(),
        };
        let lock = {
            let mut table = self.inner.lock().await;
            table
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let guard = lock.lock_owned().await;
        ObjectWriteGuard { _guard: guard, key }
    }
}

#[derive(Debug)]
pub struct ObjectWriteGuard {
    _guard: OwnedMutexGuard<()>,
    pub key: ObjectLockKey,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn same_key_lock_serializes() {
        let table = ObjectLockTable::new();
        let _first = table.lock("b", "k").await;
        let second =
            tokio::time::timeout(std::time::Duration::from_millis(10), table.lock("b", "k")).await;
        assert!(second.is_err());
    }

    #[tokio::test]
    async fn different_key_lock_does_not_block() {
        let table = ObjectLockTable::new();
        let _first = table.lock("b", "k1").await;
        let second =
            tokio::time::timeout(std::time::Duration::from_millis(10), table.lock("b", "k2")).await;
        assert!(second.is_ok());
    }
}
