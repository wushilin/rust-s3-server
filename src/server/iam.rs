//! IAM users, access keys, and policies, backed by `admin.sqlite` at the
//! data root (outside any bucket — bucket index rebuilds never touch it).
//!
//! This database is *primary* data, not a derived cache: it cannot be
//! rebuilt from anything, so it is deliberately tiny and easy to back up.
//! The bootstrap admin account lives in the config file instead, so an
//! operator can always log in even against a bare data directory.
//!
//! All authorization-path reads are served from an in-memory snapshot
//! (`Arc<RwLock<…>>`) so SigV4 validation stays synchronous; mutations write
//! SQLite first, then refresh the snapshot.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use super::policy::PolicyDocument;
use crate::storage::errors::{Result, StorageError};
use crate::storage::time::now_ms;

type HmacSha256 = Hmac<Sha256>;

const PBKDF2_ITERATIONS: u32 = 100_000;
const SESSION_TTL_MS: i64 = 12 * 60 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamUser {
    pub username: String,
    pub policy: Option<PolicyDocument>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessKey {
    pub access_key: String,
    pub secret_key: String,
    pub username: String,
    pub created_at_ms: i64,
}

/// The resolved caller of an S3 API request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    /// A credential from the config file — unrestricted.
    Root,
    /// An IAM access key — bound by its user's policy.
    IamUser(String),
}

#[derive(Debug, Default)]
struct Snapshot {
    /// access_key → (secret_key, username)
    keys: HashMap<String, (String, String)>,
    /// username → policy (None = no policy attached = deny everything)
    policies: HashMap<String, Option<PolicyDocument>>,
}

#[derive(Debug, Clone)]
struct Session {
    username: String,
    is_root: bool,
    expires_at_ms: i64,
}

#[derive(Clone)]
pub struct IamStore {
    pool: SqlitePool,
    snapshot: Arc<RwLock<Snapshot>>,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
}

impl IamStore {
    /// Opens (creating if absent) `<data_root>/admin.sqlite` and loads the
    /// in-memory snapshot.
    pub async fn open(data_root: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(data_root).await?;
        let db_path = data_root.join("admin.sqlite");
        let url = format!("sqlite://{}", db_path.to_string_lossy());
        let options = SqliteConnectOptions::from_str(&url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS iam_users (
                username      TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                salt          TEXT NOT NULL,
                policy_json   TEXT,
                created_at_ms INTEGER NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS access_keys (
                access_key    TEXT PRIMARY KEY,
                secret_key    TEXT NOT NULL,
                username      TEXT NOT NULL REFERENCES iam_users(username) ON DELETE CASCADE,
                created_at_ms INTEGER NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await?;
        let store = Self {
            pool,
            snapshot: Arc::new(RwLock::new(Snapshot::default())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        };
        store.reload().await?;
        Ok(store)
    }

    /// Rebuilds the in-memory snapshot from SQLite. Called after every
    /// mutation; cheap because the tables are tiny.
    async fn reload(&self) -> Result<()> {
        let mut snapshot = Snapshot::default();
        let users = sqlx::query("SELECT username, policy_json FROM iam_users")
            .fetch_all(&self.pool)
            .await?;
        for row in users {
            let username: String = row.get("username");
            let policy = row
                .get::<Option<String>, _>("policy_json")
                .and_then(|json| serde_json::from_str(&json).ok());
            snapshot.policies.insert(username, policy);
        }
        let keys = sqlx::query("SELECT access_key, secret_key, username FROM access_keys")
            .fetch_all(&self.pool)
            .await?;
        for row in keys {
            snapshot.keys.insert(
                row.get("access_key"),
                (row.get("secret_key"), row.get("username")),
            );
        }
        *self.snapshot.write().unwrap() = snapshot;
        Ok(())
    }

    // ── synchronous auth-path lookups (in-memory) ─────────────────────────────

    /// Resolves an access key to its secret and owning user. Sync — used
    /// inside SigV4 validation.
    pub fn find_key(&self, access_key: &str) -> Option<(String, String)> {
        self.snapshot.read().unwrap().keys.get(access_key).cloned()
    }

    /// Returns the user's policy. `None` (no user / no policy attached)
    /// must be treated as deny-everything.
    pub fn policy_for(&self, username: &str) -> Option<PolicyDocument> {
        self.snapshot
            .read()
            .unwrap()
            .policies
            .get(username)
            .cloned()
            .flatten()
    }

    pub fn user_exists(&self, username: &str) -> bool {
        self.snapshot
            .read()
            .unwrap()
            .policies
            .contains_key(username)
    }

    // ── user management ───────────────────────────────────────────────────────

    pub async fn create_user(&self, username: &str, password: &str) -> Result<()> {
        validate_username(username)?;
        if password.len() < 8 {
            return Err(StorageError::Io("password must be at least 8 characters".into()));
        }
        let salt = random_hex(16);
        let hash = pbkdf2_hex(password, &salt);
        sqlx::query(
            "INSERT INTO iam_users(username, password_hash, salt, created_at_ms) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(username)
        .bind(hash)
        .bind(salt)
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .map_err(|_| StorageError::Io(format!("user {username} already exists")))?;
        self.reload().await
    }

    pub async fn set_password(&self, username: &str, password: &str) -> Result<()> {
        if password.len() < 8 {
            return Err(StorageError::Io("password must be at least 8 characters".into()));
        }
        let salt = random_hex(16);
        let hash = pbkdf2_hex(password, &salt);
        let result = sqlx::query(
            "UPDATE iam_users SET password_hash = ?2, salt = ?3 WHERE username = ?1",
        )
        .bind(username)
        .bind(hash)
        .bind(salt)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::Io(format!("no such user {username}")));
        }
        // A password reset is a security boundary: existing console
        // sessions for the user must authenticate again with the new value.
        self.sessions
            .lock()
            .unwrap()
            .retain(|_, s| s.is_root || s.username != username);
        Ok(())
    }

    pub async fn delete_user(&self, username: &str) -> Result<()> {
        sqlx::query("DELETE FROM access_keys WHERE username = ?1")
            .bind(username)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM iam_users WHERE username = ?1")
            .bind(username)
            .execute(&self.pool)
            .await?;
        // Invalidate any live sessions for the deleted user.
        self.sessions
            .lock()
            .unwrap()
            .retain(|_, s| s.is_root || s.username != username);
        self.reload().await
    }

    pub async fn set_policy(&self, username: &str, policy: Option<&PolicyDocument>) -> Result<()> {
        let json = match policy {
            Some(p) => Some(serde_json::to_string(p)?),
            None => None,
        };
        let result = sqlx::query("UPDATE iam_users SET policy_json = ?2 WHERE username = ?1")
            .bind(username)
            .bind(json)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::Io(format!("no such user {username}")));
        }
        self.reload().await
    }

    pub async fn list_users(&self) -> Result<Vec<IamUser>> {
        let rows = sqlx::query("SELECT username, policy_json, created_at_ms FROM iam_users ORDER BY username")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| IamUser {
                username: row.get("username"),
                policy: row
                    .get::<Option<String>, _>("policy_json")
                    .and_then(|json| serde_json::from_str(&json).ok()),
                created_at_ms: row.get("created_at_ms"),
            })
            .collect())
    }

    pub fn verify_password_sync(&self, _username: &str, _password: &str) -> bool {
        false // passwords are only verified via the async path (needs the DB row)
    }

    pub async fn verify_password(&self, username: &str, password: &str) -> Result<bool> {
        let row = sqlx::query("SELECT password_hash, salt FROM iam_users WHERE username = ?1")
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else { return Ok(false) };
        let hash: String = row.get("password_hash");
        let salt: String = row.get("salt");
        let candidate = pbkdf2_hex(password, &salt);
        Ok(constant_time_eq(&candidate, &hash))
    }

    // ── access keys ───────────────────────────────────────────────────────────

    pub async fn create_access_key(&self, username: &str) -> Result<AccessKey> {
        if !self.user_exists(username) {
            return Err(StorageError::Io(format!("no such user {username}")));
        }
        let key = AccessKey {
            access_key: format!("RSAK{}", random_hex(8).to_uppercase()),
            secret_key: random_hex(20),
            username: username.to_string(),
            created_at_ms: now_ms(),
        };
        sqlx::query(
            "INSERT INTO access_keys(access_key, secret_key, username, created_at_ms) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(&key.access_key)
        .bind(&key.secret_key)
        .bind(&key.username)
        .bind(key.created_at_ms)
        .execute(&self.pool)
        .await?;
        self.reload().await?;
        Ok(key)
    }

    pub async fn delete_access_key(&self, access_key: &str) -> Result<()> {
        sqlx::query("DELETE FROM access_keys WHERE access_key = ?1")
            .bind(access_key)
            .execute(&self.pool)
            .await?;
        self.reload().await
    }

    pub async fn list_access_keys(&self, username: &str) -> Result<Vec<AccessKey>> {
        let rows = sqlx::query(
            "SELECT access_key, secret_key, username, created_at_ms FROM access_keys WHERE username = ?1 ORDER BY created_at_ms",
        )
        .bind(username)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| AccessKey {
                access_key: row.get("access_key"),
                secret_key: row.get("secret_key"),
                username: row.get("username"),
                created_at_ms: row.get("created_at_ms"),
            })
            .collect())
    }

    // ── web sessions ──────────────────────────────────────────────────────────

    pub fn create_session(&self, username: &str, is_root: bool) -> String {
        let token = random_hex(32);
        self.sessions.lock().unwrap().insert(
            token.clone(),
            Session {
                username: username.to_string(),
                is_root,
                expires_at_ms: now_ms() + SESSION_TTL_MS,
            },
        );
        token
    }

    /// Returns `(username, is_root)` for a live session token.
    pub fn resolve_session(&self, token: &str) -> Option<(String, bool)> {
        let mut sessions = self.sessions.lock().unwrap();
        let now = now_ms();
        sessions.retain(|_, s| s.expires_at_ms > now);
        sessions
            .get(token)
            .map(|s| (s.username.clone(), s.is_root))
    }

    pub fn destroy_session(&self, token: &str) {
        self.sessions.lock().unwrap().remove(token);
    }
}

impl std::fmt::Debug for IamStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IamStore").finish_non_exhaustive()
    }
}

fn validate_username(username: &str) -> Result<()> {
    let ok = !username.is_empty()
        && username.len() <= 64
        && username
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.');
    if ok {
        Ok(())
    } else {
        Err(StorageError::Io(format!("invalid username {username:?}")))
    }
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// PBKDF2-HMAC-SHA256 built from the crates already in the tree.
fn pbkdf2_hex(password: &str, salt: &str) -> String {
    let mut derived = [0u8; 32];
    let mut block = {
        let mut mac = HmacSha256::new_from_slice(password.as_bytes()).unwrap();
        mac.update(salt.as_bytes());
        mac.update(&1u32.to_be_bytes());
        let out: [u8; 32] = mac.finalize().into_bytes().into();
        out
    };
    derived.copy_from_slice(&block);
    for _ in 1..PBKDF2_ITERATIONS {
        let mut mac = HmacSha256::new_from_slice(password.as_bytes()).unwrap();
        mac.update(&block);
        block = mac.finalize().into_bytes().into();
        for (d, b) in derived.iter_mut().zip(block.iter()) {
            *d ^= b;
        }
    }
    derived.iter().map(|b| format!("{b:02x}")).collect()
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn open_tmp() -> (tempfile::TempDir, IamStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = IamStore::open(tmp.path()).await.unwrap();
        (tmp, store)
    }

    #[tokio::test]
    async fn user_lifecycle_and_password_verification() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("alice", "correct horse").await.unwrap();
        assert!(iam.verify_password("alice", "correct horse").await.unwrap());
        assert!(!iam.verify_password("alice", "wrong").await.unwrap());
        assert!(!iam.verify_password("nobody", "x").await.unwrap());
        assert!(iam.create_user("alice", "duplicate1").await.is_err());
        iam.delete_user("alice").await.unwrap();
        assert!(!iam.verify_password("alice", "correct horse").await.unwrap());
    }

    #[tokio::test]
    async fn access_keys_resolve_to_owner_and_policy() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("bob", "password123").await.unwrap();
        let key = iam.create_access_key("bob").await.unwrap();
        let (secret, owner) = iam.find_key(&key.access_key).unwrap();
        assert_eq!(secret, key.secret_key);
        assert_eq!(owner, "bob");
        // No policy attached → policy_for is None (deny-everything).
        assert!(iam.policy_for("bob").is_none());
        let policy: PolicyDocument = serde_json::from_str(
            r#"{"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::*"}]}"#,
        )
        .unwrap();
        iam.set_policy("bob", Some(&policy)).await.unwrap();
        assert_eq!(iam.policy_for("bob"), Some(policy));
        iam.delete_access_key(&key.access_key).await.unwrap();
        assert!(iam.find_key(&key.access_key).is_none());
    }

    #[tokio::test]
    async fn state_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let key = {
            let iam = IamStore::open(tmp.path()).await.unwrap();
            iam.create_user("carol", "password123").await.unwrap();
            iam.create_access_key("carol").await.unwrap()
        };
        let iam = IamStore::open(tmp.path()).await.unwrap();
        assert!(iam.verify_password("carol", "password123").await.unwrap());
        assert_eq!(iam.find_key(&key.access_key).unwrap().1, "carol");
    }

    #[tokio::test]
    async fn sessions_expire_and_are_destroyed_with_user() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("dave", "password123").await.unwrap();
        let token = iam.create_session("dave", false);
        assert_eq!(iam.resolve_session(&token), Some(("dave".to_string(), false)));
        iam.delete_user("dave").await.unwrap();
        assert_eq!(iam.resolve_session(&token), None);
    }

    #[tokio::test]
    async fn password_reset_replaces_credentials_and_invalidates_sessions() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("erin", "old-password").await.unwrap();
        let token = iam.create_session("erin", false);

        iam.set_password("erin", "new-password").await.unwrap();

        assert!(!iam.verify_password("erin", "old-password").await.unwrap());
        assert!(iam.verify_password("erin", "new-password").await.unwrap());
        assert_eq!(iam.resolve_session(&token), None);
    }
}
