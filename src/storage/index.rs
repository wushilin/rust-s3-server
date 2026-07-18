//! Per-bucket SQLite catalog — the **source of truth** for object existence.
//!
//! A row in `objects` *is* the object: PUT commits the instant its row
//! upsert commits, DELETE the instant its row is deleted. The filesystem is
//! an immutable blob heap addressed by `blob_dir` (stored relative to the
//! bucket directory) and is never scanned on the request path.
//!
//! The `intents` table is a write-ahead record of planned live-tree
//! transitions: a `publish` intent is inserted (and committed) before a blob
//! is renamed into the live tree and deleted atomically with the row flip; a
//! `retire` intent records a pending move to trash and is deleted once the
//! move completes. Crash recovery and orphan cleanup read this table only —
//! never the tree.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool};

use super::errors::{Result, StorageError};

/// Schema version stamped into `PRAGMA user_version`. Any other value
/// (including 0, the pre-blob_dir schema) requires a rebuild.
pub const SCHEMA_VERSION: i32 = 2;

pub const INTENT_PUBLISH: &str = "publish";
pub const INTENT_RETIRE: &str = "retire";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRecord {
    pub object_key: String,
    /// Blob directory path relative to the bucket directory,
    /// e.g. `objects/aa/bb/cc/dd/V1AB3F7C_9F00`.
    pub blob_dir: String,
    pub size: u64,
    pub etag: String,
    pub last_modified_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentRecord {
    pub id: i64,
    pub op: String,
    pub object_key: String,
    pub blob_dir: String,
    pub created_at_ms: i64,
    pub attempts: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage {
    pub entries: Vec<ObjectRecord>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_after: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// fsync blobs before commit, `synchronous=FULL`: acked writes survive
    /// power loss.
    Full,
    /// `synchronous=NORMAL`, no per-put blob fsync: power loss may drop the
    /// last acked writes; consistency is preserved via startup
    /// reconciliation.
    Relaxed,
}

/// Outcome of a rebuild batch: blob dirs that lost newer-wins adjudication
/// and must be moved to trash by the caller.
#[derive(Debug, Default)]
pub struct RebuildBatchOutcome {
    pub inserted: usize,
    pub loser_blob_dirs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SqliteObjectIndex {
    /// Single-connection pool: all mutations flow through here so write
    /// transactions never contend with each other inside sqlx.
    writer: SqlitePool,
    /// Multi-connection pool for reads (WAL snapshots).
    reader: SqlitePool,
}

pub fn index_db_path(bucket_dir: &Path) -> std::path::PathBuf {
    bucket_dir.join("index.sqlite")
}

fn connect_options(db_path: &Path, durability: Durability, create: bool) -> Result<SqliteConnectOptions> {
    let url = format!("sqlite://{}", db_path.to_string_lossy());
    let synchronous = match durability {
        Durability::Full => SqliteSynchronous::Full,
        Durability::Relaxed => SqliteSynchronous::Normal,
    };
    Ok(SqliteConnectOptions::from_str(&url)?
        .create_if_missing(create)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(synchronous)
        .busy_timeout(Duration::from_secs(5)))
}

/// Returns true when the bucket needs a (blocking) rebuild before it can
/// serve: the database file is missing while blobs exist, or the schema
/// version is not current.
pub async fn needs_rebuild(bucket_dir: &Path) -> Result<bool> {
    let db_path = index_db_path(bucket_dir);
    if !db_path.exists() {
        // Fresh DB is only acceptable when there are no blobs to lose.
        let objects_dir = bucket_dir.join("objects");
        let mut has_blobs = false;
        if let Ok(mut entries) = tokio::fs::read_dir(&objects_dir).await {
            has_blobs = entries.next_entry().await?.is_some();
        }
        return Ok(has_blobs);
    }
    let options = connect_options(&db_path, Durability::Relaxed, false)?;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let version: i32 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await?;
    pool.close().await;
    Ok(version != SCHEMA_VERSION)
}

impl SqliteObjectIndex {
    /// Opens (creating if absent) a current-schema index. Fails with
    /// [`StorageError::IndexOutdated`] if the on-disk schema is not current —
    /// callers must run a rebuild first; this function never migrates.
    pub async fn open(bucket_dir: &Path, durability: Durability, reader_connections: u32) -> Result<Self> {
        tokio::fs::create_dir_all(bucket_dir).await?;
        let db_path = index_db_path(bucket_dir);
        let existed = db_path.exists();
        let index = Self::open_at(&db_path, durability, reader_connections).await?;
        let version: i32 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&index.writer)
            .await?;
        if existed && version != 0 && version != SCHEMA_VERSION {
            index.close().await;
            return Err(StorageError::IndexOutdated(format!(
                "schema version {version}, expected {SCHEMA_VERSION}"
            )));
        }
        if version == 0 {
            // Either a brand-new file or a legacy (pre-v2) database. Legacy
            // databases must be rebuilt, not migrated — the caller checks
            // `needs_rebuild` before opening; creating tables here is only
            // correct for the brand-new case, and harmless otherwise since a
            // rebuild replaces the file wholesale.
            if existed && Self::has_legacy_schema(&index.writer).await? {
                index.close().await;
                return Err(StorageError::IndexOutdated(
                    "legacy schema (no blob_dir); rebuild required".to_string(),
                ));
            }
            index.create_schema().await?;
        }
        Ok(index)
    }

    /// Opens a database file directly (used by the rebuild pipeline for the
    /// temporary database it constructs; `synchronous=OFF` is applied there
    /// via `durability_off`).
    pub async fn open_at(db_path: &Path, durability: Durability, reader_connections: u32) -> Result<Self> {
        let options = connect_options(db_path, durability, true)?;
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await?;
        let reader = SqlitePoolOptions::new()
            .max_connections(reader_connections.max(1))
            .connect_with(options.read_only(false))
            .await?;
        Ok(Self { writer, reader })
    }

    async fn has_legacy_schema(pool: &SqlitePool) -> Result<bool> {
        let objects_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'objects'",
        )
        .fetch_optional(pool)
        .await?;
        if objects_exists.is_none() {
            return Ok(false);
        }
        let has_blob_dir: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM pragma_table_info('objects') WHERE name = 'blob_dir'",
        )
        .fetch_optional(pool)
        .await?;
        Ok(has_blob_dir.is_none())
    }

    pub async fn create_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS objects (
                object_key       TEXT PRIMARY KEY,
                blob_dir         TEXT NOT NULL,
                size             INTEGER NOT NULL,
                etag             TEXT NOT NULL,
                last_modified_ms INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.writer)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS intents (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                op            TEXT NOT NULL,
                object_key    TEXT NOT NULL,
                blob_dir      TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                attempts      INTEGER NOT NULL DEFAULT 0
            )
            "#,
        )
        .execute(&self.writer)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_intents_created ON intents(created_at_ms)")
            .execute(&self.writer)
            .await?;
        sqlx::query(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    pub async fn close(&self) {
        self.writer.close().await;
        self.reader.close().await;
    }

    // ── objects ───────────────────────────────────────────────────────────────

    pub async fn get(&self, key: &str) -> Result<Option<ObjectRecord>> {
        let row = sqlx::query(
            "SELECT object_key, blob_dir, size, etag, last_modified_ms FROM objects WHERE object_key = ?1",
        )
        .bind(key)
        .fetch_optional(&self.reader)
        .await?;
        Ok(row.map(row_to_record))
    }

    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.object_count().await? == 0)
    }

    pub async fn object_count(&self) -> Result<u64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM objects")
            .fetch_one(&self.reader)
            .await?;
        Ok(count.max(0) as u64)
    }

    // ── intents ───────────────────────────────────────────────────────────────

    /// Records a `publish` intent. Must be committed before any rename into
    /// the live tree.
    pub async fn insert_publish_intent(
        &self,
        key: &str,
        blob_dir: &str,
        now_ms: i64,
    ) -> Result<i64> {
        let result = sqlx::query(
            "INSERT INTO intents(op, object_key, blob_dir, created_at_ms) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(INTENT_PUBLISH)
        .bind(key)
        .bind(blob_dir)
        .bind(now_ms)
        .execute(&self.writer)
        .await?;
        Ok(result.last_insert_rowid())
    }

    /// The atomic commit point of a PUT/copy/multipart-complete: upserts the
    /// row, removes the publish intent, and (for overwrites) inserts a
    /// `retire` intent for the displaced blob dir — all in one transaction.
    /// Returns the retire intent id when an old dir was displaced.
    pub async fn commit_publish(
        &self,
        record: &ObjectRecord,
        publish_intent_id: i64,
        displaced_blob_dir: Option<&str>,
        now_ms: i64,
    ) -> Result<Option<i64>> {
        let mut tx = self.writer.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO objects(object_key, blob_dir, size, etag, last_modified_ms)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(object_key) DO UPDATE SET
                blob_dir = excluded.blob_dir,
                size = excluded.size,
                etag = excluded.etag,
                last_modified_ms = excluded.last_modified_ms
            "#,
        )
        .bind(&record.object_key)
        .bind(&record.blob_dir)
        .bind(record.size as i64)
        .bind(&record.etag)
        .bind(record.last_modified_ms)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM intents WHERE id = ?1")
            .bind(publish_intent_id)
            .execute(&mut *tx)
            .await?;
        let retire_id = match displaced_blob_dir {
            Some(old_dir) => {
                let result = sqlx::query(
                    "INSERT INTO intents(op, object_key, blob_dir, created_at_ms) VALUES (?1, ?2, ?3, ?4)",
                )
                .bind(INTENT_RETIRE)
                .bind(&record.object_key)
                .bind(old_dir)
                .bind(now_ms)
                .execute(&mut *tx)
                .await?;
                Some(result.last_insert_rowid())
            }
            None => None,
        };
        tx.commit().await?;
        Ok(retire_id)
    }

    /// The atomic commit point of a DELETE: removes the row and records a
    /// `retire` intent for its blob dir in one transaction.
    pub async fn commit_delete(&self, key: &str, blob_dir: &str, now_ms: i64) -> Result<i64> {
        let mut tx = self.writer.begin().await?;
        sqlx::query("DELETE FROM objects WHERE object_key = ?1")
            .bind(key)
            .execute(&mut *tx)
            .await?;
        let result = sqlx::query(
            "INSERT INTO intents(op, object_key, blob_dir, created_at_ms) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(INTENT_RETIRE)
        .bind(key)
        .bind(blob_dir)
        .bind(now_ms)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(result.last_insert_rowid())
    }

    pub async fn delete_intent(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM intents WHERE id = ?1")
            .bind(id)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    pub async fn bump_intent_attempts(&self, id: i64) -> Result<()> {
        sqlx::query("UPDATE intents SET attempts = attempts + 1 WHERE id = ?1")
            .bind(id)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    /// Re-fetches an intent by id. Resolvers must confirm the intent still
    /// exists *after* taking the key lock — acting on a stale snapshot could
    /// trash a blob whose publish has since committed.
    pub async fn get_intent(&self, id: i64) -> Result<Option<IntentRecord>> {
        let row = sqlx::query(
            "SELECT id, op, object_key, blob_dir, created_at_ms, attempts FROM intents WHERE id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.reader)
        .await?;
        Ok(row.map(|row| IntentRecord {
            id: row.get("id"),
            op: row.get("op"),
            object_key: row.get("object_key"),
            blob_dir: row.get("blob_dir"),
            created_at_ms: row.get("created_at_ms"),
            attempts: row.get("attempts"),
        }))
    }

    /// Swaps a publish intent's blob dir (rename destination collided and a
    /// fresh name was chosen before anything touched the live tree).
    pub async fn update_intent_blob_dir(&self, id: i64, blob_dir: &str) -> Result<()> {
        sqlx::query("UPDATE intents SET blob_dir = ?2 WHERE id = ?1")
            .bind(id)
            .bind(blob_dir)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    /// Intents older than `min_age_ms` — candidates for background
    /// resolution. Pass `min_age_ms = 0` for the startup drain.
    pub async fn stale_intents(
        &self,
        now_ms: i64,
        min_age_ms: i64,
        limit: i64,
    ) -> Result<Vec<IntentRecord>> {
        let cutoff = now_ms.saturating_sub(min_age_ms);
        let rows = sqlx::query(
            r#"
            SELECT id, op, object_key, blob_dir, created_at_ms, attempts
            FROM intents
            WHERE created_at_ms <= ?1
            ORDER BY created_at_ms, id
            LIMIT ?2
            "#,
        )
        .bind(cutoff)
        .bind(limit.max(1))
        .fetch_all(&self.reader)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| IntentRecord {
                id: row.get("id"),
                op: row.get("op"),
                object_key: row.get("object_key"),
                blob_dir: row.get("blob_dir"),
                created_at_ms: row.get("created_at_ms"),
                attempts: row.get("attempts"),
            })
            .collect())
    }

    // ── rebuild ───────────────────────────────────────────────────────────────

    /// Bulk newer-wins insert used by the rebuild pipeline. Per key, the
    /// entry with the highest `last_modified_ms` wins (path as tie-break for
    /// determinism); every displaced blob dir is reported so the caller can
    /// trash it.
    pub async fn insert_rebuild_batch(
        &self,
        entries: &[ObjectRecord],
    ) -> Result<RebuildBatchOutcome> {
        let mut outcome = RebuildBatchOutcome::default();
        // Adjudicate duplicates within the batch first so the DB round-trip
        // sees at most one candidate per key.
        let mut best: HashMap<&str, &ObjectRecord> = HashMap::new();
        for entry in entries {
            match best.get(entry.object_key.as_str()) {
                Some(current) if !newer_wins(entry, current) => {
                    outcome.loser_blob_dirs.push(entry.blob_dir.clone());
                }
                Some(current) => {
                    outcome.loser_blob_dirs.push(current.blob_dir.clone());
                    best.insert(&entry.object_key, entry);
                }
                None => {
                    best.insert(&entry.object_key, entry);
                }
            }
        }
        let mut tx = self.writer.begin().await?;
        for entry in best.values() {
            let existing = sqlx::query(
                "SELECT object_key, blob_dir, size, etag, last_modified_ms FROM objects WHERE object_key = ?1",
            )
            .bind(&entry.object_key)
            .fetch_optional(&mut *tx)
            .await?
            .map(row_to_record);
            if let Some(existing) = existing {
                if !newer_wins(entry, &existing) {
                    outcome.loser_blob_dirs.push(entry.blob_dir.clone());
                    continue;
                }
                outcome.loser_blob_dirs.push(existing.blob_dir);
            }
            sqlx::query(
                r#"
                INSERT INTO objects(object_key, blob_dir, size, etag, last_modified_ms)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(object_key) DO UPDATE SET
                    blob_dir = excluded.blob_dir,
                    size = excluded.size,
                    etag = excluded.etag,
                    last_modified_ms = excluded.last_modified_ms
                "#,
            )
            .bind(&entry.object_key)
            .bind(&entry.blob_dir)
            .bind(entry.size as i64)
            .bind(&entry.etag)
            .bind(entry.last_modified_ms)
            .execute(&mut *tx)
            .await?;
            outcome.inserted += 1;
        }
        tx.commit().await?;
        Ok(outcome)
    }

    /// Flushes the WAL into the main database file so the file can be
    /// atomically renamed into place (rebuild pipeline).
    pub async fn checkpoint_truncate(&self) -> Result<()> {
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    // ── listing ───────────────────────────────────────────────────────────────

    /// Returns up to `limit` entries in ascending key order, starting after `after`.
    pub async fn all_entries_after(
        &self,
        after: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ObjectRecord>> {
        let lower = after.unwrap_or("");
        let rows = sqlx::query(
            "SELECT object_key, blob_dir, size, etag, last_modified_ms FROM objects WHERE object_key > ?1 ORDER BY object_key LIMIT ?2",
        )
        .bind(lower)
        .bind(limit)
        .fetch_all(&self.reader)
        .await?;
        Ok(rows.into_iter().map(row_to_record).collect())
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
            .fetch_optional(&self.reader)
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
            SELECT object_key, blob_dir, size, etag, last_modified_ms
            FROM objects
            WHERE object_key >= ?1 AND object_key > ?2
            ORDER BY object_key
            LIMIT ?3
            "#,
        )
        .bind(prefix)
        .bind(lower)
        .bind((max_keys + 1).max(1) as i64 * 8)
        .fetch_all(&self.reader)
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
            let entry = row_to_record(row);
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

/// Newer-wins adjudication: highest `last_modified_ms`, blob path as a
/// deterministic tie-break.
fn newer_wins(candidate: &ObjectRecord, incumbent: &ObjectRecord) -> bool {
    (candidate.last_modified_ms, candidate.blob_dir.as_str())
        > (incumbent.last_modified_ms, incumbent.blob_dir.as_str())
}

fn row_to_record(row: sqlx::sqlite::SqliteRow) -> ObjectRecord {
    ObjectRecord {
        object_key: row.get("object_key"),
        blob_dir: row.get("blob_dir"),
        size: row.get::<i64, _>("size") as u64,
        etag: row.get("etag"),
        last_modified_ms: row.get("last_modified_ms"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: &str, blob_dir: &str, lm: i64) -> ObjectRecord {
        ObjectRecord {
            object_key: key.to_string(),
            blob_dir: blob_dir.to_string(),
            size: 1,
            etag: "e".to_string(),
            last_modified_ms: lm,
        }
    }

    async fn open_tmp(tmp: &tempfile::TempDir) -> SqliteObjectIndex {
        SqliteObjectIndex::open(tmp.path(), Durability::Relaxed, 4)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn publish_commit_removes_intent_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        let intent = index.insert_publish_intent("k", "objects/x", 1).await.unwrap();
        assert_eq!(index.stale_intents(10, 0, 10).await.unwrap().len(), 1);
        let retire = index
            .commit_publish(&record("k", "objects/x", 5), intent, None, 5)
            .await
            .unwrap();
        assert!(retire.is_none());
        assert!(index.stale_intents(10, 0, 10).await.unwrap().is_empty());
        assert_eq!(index.get("k").await.unwrap().unwrap().blob_dir, "objects/x");
    }

    #[tokio::test]
    async fn object_count_tracks_committed_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        assert_eq!(index.object_count().await.unwrap(), 0);
        for key in ["a", "b", "c"] {
            let intent = index.insert_publish_intent(key, "objects/x", 1).await.unwrap();
            index
                .commit_publish(&record(key, "objects/x", 1), intent, None, 1)
                .await
                .unwrap();
        }
        assert_eq!(index.object_count().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn overwrite_commit_records_retire_intent() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        let i1 = index.insert_publish_intent("k", "objects/a", 1).await.unwrap();
        index
            .commit_publish(&record("k", "objects/a", 5), i1, None, 5)
            .await
            .unwrap();
        let i2 = index.insert_publish_intent("k", "objects/b", 6).await.unwrap();
        let retire = index
            .commit_publish(&record("k", "objects/b", 7), i2, Some("objects/a"), 7)
            .await
            .unwrap()
            .unwrap();
        let intents = index.stale_intents(100, 0, 10).await.unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].op, INTENT_RETIRE);
        assert_eq!(intents[0].blob_dir, "objects/a");
        index.delete_intent(retire).await.unwrap();
        assert!(index.stale_intents(100, 0, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_commit_removes_row_and_records_retire() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        let i = index.insert_publish_intent("k", "objects/a", 1).await.unwrap();
        index
            .commit_publish(&record("k", "objects/a", 5), i, None, 5)
            .await
            .unwrap();
        let retire = index.commit_delete("k", "objects/a", 6).await.unwrap();
        assert!(index.get("k").await.unwrap().is_none());
        let intents = index.stale_intents(100, 0, 10).await.unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].id, retire);
    }

    #[tokio::test]
    async fn rebuild_batch_newer_wins_reports_losers() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        let outcome = index
            .insert_rebuild_batch(&[record("k", "objects/old", 5), record("k", "objects/new", 9)])
            .await
            .unwrap();
        assert_eq!(outcome.inserted, 1);
        assert_eq!(outcome.loser_blob_dirs, vec!["objects/old".to_string()]);
        // A later, older duplicate arriving in a separate batch also loses.
        let outcome = index
            .insert_rebuild_batch(&[record("k", "objects/older", 1)])
            .await
            .unwrap();
        assert_eq!(outcome.inserted, 0);
        assert_eq!(outcome.loser_blob_dirs, vec!["objects/older".to_string()]);
        assert_eq!(index.get("k").await.unwrap().unwrap().blob_dir, "objects/new");
    }

    #[tokio::test]
    async fn legacy_schema_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = index_db_path(tmp.path());
        let index = SqliteObjectIndex::open_at(&db_path, Durability::Relaxed, 1)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE objects (object_key TEXT PRIMARY KEY, size INTEGER, etag TEXT, last_modified_ms INTEGER)")
            .execute(&index.writer)
            .await
            .unwrap();
        index.close().await;
        let err = SqliteObjectIndex::open(tmp.path(), Durability::Relaxed, 1).await;
        assert!(matches!(err, Err(StorageError::IndexOutdated(_))));
        assert!(needs_rebuild(tmp.path()).await.unwrap());
    }

    #[tokio::test]
    async fn list_dedupes_delimiter_common_prefixes() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        for key in ["a/1", "a/2", "a/b/1", "a/b/2", "a/c/1"] {
            let i = index.insert_publish_intent(key, "d", 1).await.unwrap();
            index
                .commit_publish(&record(key, "d", 1), i, None, 1)
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
        let index = open_tmp(&tmp).await;
        for key in ["a", "b"] {
            let i = index.insert_publish_intent(key, "d", 1).await.unwrap();
            index
                .commit_publish(&record(key, "d", 1), i, None, 1)
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
        let index = open_tmp(&tmp).await;
        let i = index.insert_publish_intent("a", "d", 1).await.unwrap();
        index
            .commit_publish(&record("a", "d", 1), i, None, 1)
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
        let index = open_tmp(&tmp).await;
        for key in ["a/b/1", "a/b/2", "a/c/1"] {
            let i = index.insert_publish_intent(key, "d", 1).await.unwrap();
            index
                .commit_publish(&record(key, "d", 1), i, None, 1)
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
