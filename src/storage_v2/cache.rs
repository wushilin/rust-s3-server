use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

#[derive(Debug)]
pub struct BoundedLruCache<K, V> {
    shards: Vec<Mutex<CacheShard<K, V>>>,
    clock: AtomicU64,
}

#[derive(Debug)]
struct CacheShard<K, V> {
    entries: HashMap<K, CacheEntry<V>>,
    capacity: usize,
}

#[derive(Debug)]
struct CacheEntry<V> {
    value: V,
    last_access: u64,
}

impl<K, V> BoundedLruCache<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    pub fn new(capacity: usize, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1);
        let shard_capacity = (capacity / shard_count).max(1);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Mutex::new(CacheShard {
                entries: HashMap::new(),
                capacity: shard_capacity,
            }));
        }
        Self {
            shards,
            clock: AtomicU64::new(1),
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let mut shard = self.shard(key).lock().unwrap();
        let entry = shard.entries.get_mut(key)?;
        entry.last_access = self.clock.fetch_add(1, Ordering::Relaxed);
        Some(entry.value.clone())
    }

    pub fn insert(&self, key: K, value: V) {
        let access = self.next_access();
        let mut shard = self.shard(&key).lock().unwrap();
        shard.entries.insert(
            key,
            CacheEntry {
                value,
                last_access: access,
            },
        );
        prune_if_needed(&mut shard);
    }

    pub fn remove(&self, key: &K) {
        self.shard(key).lock().unwrap().entries.remove(key);
    }

    pub fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().unwrap().entries.len())
            .sum()
    }

    fn next_access(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    fn shard(&self, key: &K) -> &Mutex<CacheShard<K, V>> {
        &self.shards[shard_index(key, self.shards.len())]
    }
}

fn prune_if_needed<K, V>(shard: &mut CacheShard<K, V>)
where
    K: Clone + Eq + Hash,
{
    if shard.entries.len() <= shard.capacity {
        return;
    }

    let overshoot = shard.entries.len() - shard.capacity;
    let batch = if shard.capacity < 100 {
        0
    } else {
        (shard.capacity / 100).max(1)
    };
    let remove_count = overshoot + batch;
    let mut oldest = shard
        .entries
        .iter()
        .map(|(key, entry)| (entry.last_access, key.clone()))
        .collect::<Vec<_>>();
    oldest.sort_unstable_by_key(|(last_access, _)| *last_access);

    for (_, key) in oldest.into_iter().take(remove_count) {
        shard.entries.remove(&key);
    }
}

fn shard_index<K: Hash>(key: &K, shard_count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_least_recently_used_entries() {
        let cache = BoundedLruCache::new(2, 1);
        cache.insert("a", 1);
        cache.insert("b", 2);
        assert_eq!(cache.get(&"a"), Some(1));
        cache.insert("c", 3);

        assert_eq!(cache.get(&"a"), Some(1));
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(3));
        assert!(cache.len() <= 2);
    }
}
