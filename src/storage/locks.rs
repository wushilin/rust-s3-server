use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
};

use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectLockKey {
    pub bucket: String,
    pub object_key: String,
}

type LockMap = HashMap<ObjectLockKey, Arc<Mutex<()>>>;

#[derive(Debug, Default)]
pub struct ObjectLockTable {
    // The table is only held for HashMap operations, never across `.await`, so a
    // blocking std mutex is correct here and lets `ObjectWriteGuard::drop` evict
    // idle keys synchronously.
    inner: Arc<StdMutex<LockMap>>,
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
            let mut table = self.inner.lock().unwrap();
            table
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let guard = lock.lock_owned().await;
        ObjectWriteGuard {
            _guard: guard,
            table: Arc::clone(&self.inner),
            key,
        }
    }

    /// Number of per-key locks currently tracked. Idle keys are evicted on
    /// guard drop, so this reflects keys with a live holder or waiter.
    #[cfg(test)]
    pub fn tracked_len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

#[derive(Debug)]
pub struct ObjectWriteGuard {
    // Field order matters: this Drop body runs before fields are dropped, so
    // `_guard` is still alive (and still holds its Arc) while we inspect the
    // entry's strong count below.
    _guard: OwnedMutexGuard<()>,
    table: Arc<StdMutex<LockMap>>,
    pub key: ObjectLockKey,
}

impl Drop for ObjectWriteGuard {
    fn drop(&mut self) {
        let mut table = match self.table.lock() {
            Ok(table) => table,
            // A poisoned table only means some other writer panicked; skipping
            // eviction is harmless (the entry simply lingers).
            Err(_) => return,
        };
        if let Some(lock) = table.get(&self.key) {
            // While we hold the table lock no new `lock()` call can clone the
            // Arc. The only live references are the table's own (1) plus this
            // guard's still-alive `_guard` (1). A higher count means another
            // holder or a parked waiter needs the entry, so we leave it.
            if Arc::strong_count(lock) <= 2 {
                table.remove(&self.key);
            }
        }
    }
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

    #[tokio::test]
    async fn idle_keys_are_evicted_when_guard_drops() {
        let table = ObjectLockTable::new();
        {
            let _g1 = table.lock("b", "k1").await;
            let _g2 = table.lock("b", "k2").await;
            assert_eq!(table.tracked_len(), 2);
        }
        // Both guards dropped → no idle entries should remain.
        assert_eq!(table.tracked_len(), 0);
    }

    #[tokio::test]
    async fn waited_on_key_is_retained_until_last_holder_drops() {
        let table = ObjectLockTable::new();
        let first = table.lock("b", "k").await;

        // A second waiter is parked on the same key.
        let table2 = table.clone();
        let waiter =
            tokio::spawn(async move { table2.lock("b", "k").await });
        // Give the waiter time to register its Arc clone.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Dropping the first holder must NOT evict the entry: the waiter still
        // needs it. The waiter then acquires the lock.
        drop(first);
        let second = waiter.await.unwrap();
        assert_eq!(table.tracked_len(), 1);

        // Once the last holder drops, the entry is evicted.
        drop(second);
        assert_eq!(table.tracked_len(), 0);
    }
}
