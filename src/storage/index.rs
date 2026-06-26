use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool};

use super::errors::Result;
use super::time::now_ms;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectIndexEntry {
    pub object_key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage {
    pub entries: Vec<ObjectIndexEntry>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_after: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SqliteObjectIndex {
    pool: SqlitePool,
}

impl SqliteObjectIndex {
    pub async fn open(bucket_dir: &Path) -> Result<Self> {
        Self::open_with_max_connections(bucket_dir, 50).await
    }

    pub async fn open_with_max_connections(
        bucket_dir: &Path,
        max_connections: u32,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(bucket_dir).await?;
        let db_path = bucket_dir.join("index.sqlite");
        let url = format!("sqlite://{}", db_path.to_string_lossy());
        // WAL lets concurrent readers proceed alongside a single writer without
        // blocking each other; the built-in DELETE journal makes reads and
        // writes mutually exclusive on the whole file. NORMAL synchronous is
        // safe and standard under WAL and avoids an fsync per commit.
        let connect_options = SqliteConnectOptions::from_str(&url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections.max(1))
            .connect_with(connect_options)
            .await?;
        let index = Self { pool };
        index.migrate().await?;
        Ok(index)
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS objects (
                object_key TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                etag TEXT NOT NULL,
                last_modified_ms INTEGER NOT NULL,
                index_seen_at_ms INTEGER NOT NULL DEFAULT 0
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("ALTER TABLE objects ADD COLUMN index_seen_at_ms INTEGER NOT NULL DEFAULT 0")
            .execute(&self.pool)
            .await
            .ok();
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_key_order ON objects(object_key)")
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_objects_index_seen_at ON objects(index_seen_at_ms)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS visibility_repair_queue (
                object_key TEXT PRIMARY KEY,
                first_created_at_ms INTEGER NOT NULL,
                last_created_at_ms INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_visibility_repair_due ON visibility_repair_queue(first_created_at_ms)",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn put(&self, entry: &ObjectIndexEntry) -> Result<()> {
        self.put_seen_at(entry, now_ms()).await
    }

    pub async fn put_seen_at(&self, entry: &ObjectIndexEntry, seen_at_ms: i64) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO objects(object_key, size, etag, last_modified_ms, index_seen_at_ms)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(object_key) DO UPDATE SET
                size = excluded.size,
                etag = excluded.etag,
                last_modified_ms = excluded.last_modified_ms,
                index_seen_at_ms = excluded.index_seen_at_ms
            "#,
        )
        .bind(&entry.object_key)
        .bind(entry.size as i64)
        .bind(&entry.etag)
        .bind(entry.last_modified_ms)
        .bind(seen_at_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<ObjectIndexEntry>> {
        let row = sqlx::query(
            "SELECT object_key, size, etag, last_modified_ms FROM objects WHERE object_key = ?1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_entry))
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM objects WHERE object_key = ?1")
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Returns the active SQLite journal mode (e.g. "wal"). Useful for
    /// diagnostics and verifying the connection is configured as expected.
    pub async fn journal_mode(&self) -> Result<String> {
        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&self.pool)
            .await?;
        Ok(mode)
    }

    pub async fn is_empty(&self) -> Result<bool> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM objects")
            .fetch_one(&self.pool)
            .await?;
        Ok(count == 0)
    }

    pub async fn clear_all(&self) -> Result<()> {
        sqlx::query("DELETE FROM objects")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn replace_all(&self, entries: &[ObjectIndexEntry]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM objects").execute(&mut *tx).await?;
        for entry in entries {
            sqlx::query(
                r#"
                INSERT INTO objects(object_key, size, etag, last_modified_ms, index_seen_at_ms)
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
            )
            .bind(&entry.object_key)
            .bind(entry.size as i64)
            .bind(&entry.etag)
            .bind(entry.last_modified_ms)
            .bind(now_ms())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_entries_not_seen_since(&self, scan_started_at_ms: i64) -> Result<u64> {
        let result = sqlx::query("DELETE FROM objects WHERE index_seen_at_ms < ?1")
            .bind(scan_started_at_ms)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    pub async fn enqueue_visibility_repair(&self, key: &str, now_ms: i64) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO visibility_repair_queue(object_key, first_created_at_ms, last_created_at_ms)
            VALUES (?1, ?2, ?2)
            ON CONFLICT(object_key) DO UPDATE SET
                first_created_at_ms = MIN(first_created_at_ms, excluded.first_created_at_ms),
                last_created_at_ms = MAX(last_created_at_ms, excluded.last_created_at_ms)
            "#,
        )
        .bind(key)
        .bind(now_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_visibility_repair(&self, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM visibility_repair_queue WHERE object_key = ?1")
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn due_visibility_repairs(
        &self,
        now_ms: i64,
        min_age_ms: i64,
        limit: i64,
    ) -> Result<Vec<String>> {
        let cutoff = now_ms.saturating_sub(min_age_ms);
        let rows = sqlx::query(
            r#"
            SELECT object_key
            FROM visibility_repair_queue
            WHERE first_created_at_ms <= ?1
            ORDER BY first_created_at_ms, object_key
            LIMIT ?2
            "#,
        )
        .bind(cutoff)
        .bind(limit.max(1))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<String, _>("object_key"))
            .collect())
    }

    pub async fn mark_visibility_repair_attempt(&self, key: &str, now_ms: i64) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE visibility_repair_queue
            SET attempts = attempts + 1,
                last_created_at_ms = ?2
            WHERE object_key = ?1
            "#,
        )
        .bind(key)
        .bind(now_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Returns up to `limit` entries in ascending key order, starting after `after`.
    /// Pass `after = None` to start from the beginning.
    pub async fn all_entries_after(
        &self,
        after: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ObjectIndexEntry>> {
        let lower = after.unwrap_or("");
        let rows = sqlx::query(
            "SELECT object_key, size, etag, last_modified_ms FROM objects WHERE object_key > ?1 ORDER BY object_key LIMIT ?2",
        )
        .bind(lower)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_entry).collect())
    }

    pub async fn list(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max_keys: usize,
    ) -> Result<ListPage> {
        if max_keys == 0 {
            let has_any: Option<i64> = sqlx::query_scalar(
                r#"
                SELECT 1 FROM objects
                WHERE object_key >= ?1 AND object_key > ?2
                ORDER BY object_key
                LIMIT 1
                "#,
            )
            .bind(prefix)
            .bind(after.unwrap_or(""))
            .fetch_optional(&self.pool)
            .await?;
            return Ok(ListPage {
                entries: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: has_any.is_some(),
                next_after: after.map(str::to_string),
            });
        }

        let lower = after.unwrap_or("");
        let rows = sqlx::query(
            r#"
            SELECT object_key, size, etag, last_modified_ms
            FROM objects
            WHERE object_key >= ?1 AND object_key > ?2
            ORDER BY object_key
            LIMIT ?3
            "#,
        )
        .bind(prefix)
        .bind(lower)
        .bind((max_keys + 1).max(1) as i64 * 8)
        .fetch_all(&self.pool)
        .await?;

        let delimiter = delimiter.filter(|v| !v.is_empty());
        let mut entries = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut next_after = None;
        let mut skipped_common_prefixes = std::collections::HashSet::new();
        if let (Some(delimiter), Some(after)) = (delimiter, after) {
            if after.starts_with(prefix) {
                let rest = &after[prefix.len()..];
                if let Some(idx) = rest.find(delimiter) {
                    skipped_common_prefixes.insert(format!(
                        "{}{}",
                        prefix,
                        &rest[..idx + delimiter.len()]
                    ));
                }
            }
        }
        let mut more_matching = false;
        for row in rows {
            let entry = row_to_entry(row);
            if !entry.object_key.starts_with(prefix) {
                break;
            }
            if entries.len() + common_prefixes.len() >= max_keys {
                more_matching = true;
                break;
            }
            if let Some(delimiter) = delimiter {
                let rest = &entry.object_key[prefix.len()..];
                if let Some(idx) = rest.find(delimiter) {
                    let common = format!("{}{}", prefix, &rest[..idx + delimiter.len()]);
                    if !skipped_common_prefixes.insert(common.clone()) {
                        next_after = Some(entry.object_key);
                        continue;
                    }
                    if common_prefixes.last() != Some(&common) {
                        common_prefixes.push(common);
                    }
                    next_after = Some(entry.object_key);
                    continue;
                }
            }
            next_after = Some(entry.object_key.clone());
            entries.push(entry);
        }

        Ok(ListPage {
            entries,
            common_prefixes,
            is_truncated: more_matching,
            next_after,
        })
    }
}

fn row_to_entry(row: sqlx::sqlite::SqliteRow) -> ObjectIndexEntry {
    ObjectIndexEntry {
        object_key: row.get("object_key"),
        size: row.get::<i64, _>("size") as u64,
        etag: row.get("etag"),
        last_modified_ms: row.get("last_modified_ms"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn opens_in_wal_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let index = SqliteObjectIndex::open(tmp.path()).await.unwrap();
        let mode = index.journal_mode().await.unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
    }

    #[tokio::test]
    async fn list_dedupes_delimiter_common_prefixes() {
        let tmp = tempfile::tempdir().unwrap();
        let index = SqliteObjectIndex::open(tmp.path()).await.unwrap();
        for key in ["a/1", "a/2", "a/b/1", "a/b/2", "a/c/1"] {
            index
                .put(&ObjectIndexEntry {
                    object_key: key.to_string(),
                    size: 1,
                    etag: "e".to_string(),
                    last_modified_ms: 1,
                })
                .await
                .unwrap();
        }
        let page = index.list("a/", Some("/"), None, 10).await.unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|v| v.object_key.as_str())
                .collect::<Vec<_>>(),
            vec!["a/1", "a/2"]
        );
        assert_eq!(page.common_prefixes, vec!["a/b/", "a/c/"]);
    }

    #[tokio::test]
    async fn exact_size_page_is_not_truncated() {
        let tmp = tempfile::tempdir().unwrap();
        let index = SqliteObjectIndex::open(tmp.path()).await.unwrap();
        for key in ["a", "b"] {
            index
                .put(&ObjectIndexEntry {
                    object_key: key.to_string(),
                    size: 1,
                    etag: "e".to_string(),
                    last_modified_ms: 1,
                })
                .await
                .unwrap();
        }
        let page = index.list("", None, None, 2).await.unwrap();
        assert_eq!(page.entries.len(), 2);
        assert!(!page.is_truncated);
    }

    #[tokio::test]
    async fn zero_max_keys_returns_no_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let index = SqliteObjectIndex::open(tmp.path()).await.unwrap();
        index
            .put(&ObjectIndexEntry {
                object_key: "a".to_string(),
                size: 1,
                etag: "e".to_string(),
                last_modified_ms: 1,
            })
            .await
            .unwrap();
        let page = index.list("", None, None, 0).await.unwrap();
        assert!(page.entries.is_empty());
        assert!(page.common_prefixes.is_empty());
        assert!(page.is_truncated);
    }

    #[tokio::test]
    async fn delimiter_pagination_does_not_repeat_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let index = SqliteObjectIndex::open(tmp.path()).await.unwrap();
        for key in ["a/b/1", "a/b/2", "a/c/1"] {
            index
                .put(&ObjectIndexEntry {
                    object_key: key.to_string(),
                    size: 1,
                    etag: "e".to_string(),
                    last_modified_ms: 1,
                })
                .await
                .unwrap();
        }
        let first = index.list("a/", Some("/"), None, 1).await.unwrap();
        assert_eq!(first.common_prefixes, vec!["a/b/"]);
        assert!(first.is_truncated);

        let second = index
            .list("a/", Some("/"), first.next_after.as_deref(), 1)
            .await
            .unwrap();
        assert_eq!(second.common_prefixes, vec!["a/c/"]);
    }
}
