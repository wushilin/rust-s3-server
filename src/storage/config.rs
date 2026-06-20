use serde::{Deserialize, Serialize};

/// Storage engine settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Maximum SQLite connections kept open per bucket index pool.
    #[serde(default = "default_sqlite_max_connections")]
    pub sqlite_max_connections: u32,
    /// Maximum number of object metadata entries held in the LRU cache.
    /// Each entry is roughly 400–700 bytes; 200 000 ≈ 80–140 MB.
    #[serde(default = "default_meta_cache_capacity")]
    pub meta_cache_capacity: usize,
    /// Maximum number of entries in the SQLite-repair suppression cache.
    /// Each entry is roughly 100 bytes; 200 000 ≈ 20 MB.
    #[serde(default = "default_sqlite_repair_cache_capacity")]
    pub sqlite_repair_cache_capacity: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            sqlite_max_connections: default_sqlite_max_connections(),
            meta_cache_capacity: default_meta_cache_capacity(),
            sqlite_repair_cache_capacity: default_sqlite_repair_cache_capacity(),
        }
    }
}

fn default_sqlite_max_connections() -> u32 {
    50
}
fn default_meta_cache_capacity() -> usize {
    200_000
}
fn default_sqlite_repair_cache_capacity() -> usize {
    200_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_pool_size_defaults() {
        assert_eq!(StorageConfig::default().sqlite_max_connections, 50);
        assert_eq!(StorageConfig::default().meta_cache_capacity, 200_000);
    }
}
