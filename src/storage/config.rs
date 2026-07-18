use serde::{Deserialize, Serialize};

/// Storage engine settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Maximum SQLite reader connections kept open per bucket index pool
    /// (mutations always use one dedicated writer connection).
    #[serde(default = "default_sqlite_max_connections")]
    pub sqlite_max_connections: u32,
    /// Maximum number of object metadata entries held in the LRU cache.
    /// Each entry is roughly 400–700 bytes; 200 000 ≈ 80–140 MB.
    #[serde(default = "default_meta_cache_capacity")]
    pub meta_cache_capacity: usize,
    /// `full`: blobs are fsynced before commit and SQLite runs
    /// `synchronous=FULL` — acked writes survive power loss.
    /// `relaxed`: no per-put blob fsync, `synchronous=NORMAL` — power loss
    /// may drop the last acked writes (never corrupts).
    #[serde(default = "default_durability")]
    pub durability: DurabilityMode,
    /// Worker tasks used by the index rebuild pipeline (each traverses
    /// directories AND parses meta.json). 0 = auto: one per CPU core.
    #[serde(default = "default_rebuild_reader_threads")]
    pub rebuild_reader_threads: usize,
    /// Bounded queue length between the rebuild walker and readers.
    #[serde(default = "default_rebuild_queue_bound")]
    pub rebuild_queue_bound: usize,
    /// Rows per transaction in the rebuild batch writer.
    #[serde(default = "default_rebuild_batch_size")]
    pub rebuild_batch_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityMode {
    Full,
    Relaxed,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            sqlite_max_connections: default_sqlite_max_connections(),
            meta_cache_capacity: default_meta_cache_capacity(),
            durability: default_durability(),
            rebuild_reader_threads: default_rebuild_reader_threads(),
            rebuild_queue_bound: default_rebuild_queue_bound(),
            rebuild_batch_size: default_rebuild_batch_size(),
        }
    }
}

fn default_sqlite_max_connections() -> u32 {
    50
}
fn default_meta_cache_capacity() -> usize {
    200_000
}
fn default_durability() -> DurabilityMode {
    DurabilityMode::Full
}
fn default_rebuild_reader_threads() -> usize {
    0 // auto: one worker per CPU core
}
fn default_rebuild_queue_bound() -> usize {
    1000
}
fn default_rebuild_batch_size() -> usize {
    1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let config = StorageConfig::default();
        assert_eq!(config.sqlite_max_connections, 50);
        assert_eq!(config.meta_cache_capacity, 200_000);
        assert_eq!(config.durability, DurabilityMode::Full);
        assert_eq!(config.rebuild_reader_threads, 0); // 0 = auto (core count)
    }

    #[test]
    fn old_config_files_still_parse() {
        // Pre-redesign config files carry `sqlite_repair_cache_capacity`;
        // unknown fields must not break deserialization.
        let yaml = "sqlite_max_connections: 10\nsqlite_repair_cache_capacity: 200000\n";
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.sqlite_max_connections, 10);
        assert_eq!(config.durability, DurabilityMode::Full);
    }
}
