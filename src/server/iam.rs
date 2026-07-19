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

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use super::policy::{Effect, OneOrMany, PolicyDocument, Statement};
use crate::storage::errors::{Result, StorageError};
use crate::storage::time::now_ms;

type HmacSha256 = Hmac<Sha256>;

const PBKDF2_ITERATIONS: u32 = 100_000;
const SESSION_TTL_MS: i64 = 12 * 60 * 60 * 1000;

mod group_name {
    use super::{validate_group_name, Group};
    use crate::storage::errors::{Result, StorageError};

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct NamedGroup(String);

    impl NamedGroup {
        pub fn new(name: &str) -> Result<Self> {
            validate_group_name(name)?;
            match name.eq_ignore_ascii_case(Group::ADMIN_NAME) {
                true => Err(StorageError::Io("admin is a reserved system group".into())),
                false => Ok(Self(name.to_string())),
            }
        }

        pub fn as_str(&self) -> &str {
            &self.0
        }
    }
}

pub use group_name::NamedGroup;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Group {
    Admin,
    Named(NamedGroup),
}

impl Group {
    const ADMIN_NAME: &'static str = "admin";

    fn from_storage(name: &str, is_system: bool) -> Result<Self> {
        validate_group_name(name)?;
        match is_system {
            true => Ok(Self::Admin),
            false => NamedGroup::new(name).map(Self::Named),
        }
    }

    pub fn named(name: &str) -> Result<Self> {
        NamedGroup::new(name).map(Self::Named)
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Admin => Self::ADMIN_NAME,
            Self::Named(name) => name.as_str(),
        }
    }

    fn policy(&self, named: &HashMap<String, Option<PolicyDocument>>) -> Option<PolicyDocument> {
        match self {
            Self::Admin => Some(PolicyDocument {
                version: "2012-10-17".to_string(),
                statement: vec![Statement {
                    sid: Some("RustS3SystemAdmin".to_string()),
                    effect: Effect::Allow,
                    action: OneOrMany::One("s3:*".to_string()),
                    resource: OneOrMany::One("arn:aws:s3:::*".to_string()),
                    condition: None,
                }],
            }),
            Self::Named(name) => named
                .get(&name.as_str().to_ascii_lowercase())
                .cloned()
                .flatten(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamUser {
    pub username: String,
    pub policy: Option<PolicyDocument>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamGroup {
    pub group: Group,
    pub policy: Option<PolicyDocument>,
    pub created_at_ms: i64,
    pub members: u64,
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
    /// Hidden internal signing keys (`RSWEB_…`) used by the console to presign
    /// share links, keyed by access_key → (secret, username, is_builtin). Never
    /// listed or exposed; resolve to their owner's access on verification.
    web_keys: HashMap<String, (String, String, bool)>,
    /// username → effective merged policy (None = deny everything)
    policies: HashMap<String, Option<PolicyDocument>>,
    /// username → assigned group names.
    memberships: HashMap<String, Vec<Group>>,
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
            CREATE TABLE IF NOT EXISTS iam_groups (
                name          TEXT PRIMARY KEY COLLATE NOCASE,
                policy_json   TEXT,
                is_system     INTEGER NOT NULL DEFAULT 0,
                created_at_ms INTEGER NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS iam_user_groups (
                username   TEXT NOT NULL,
                group_name TEXT NOT NULL COLLATE NOCASE,
                PRIMARY KEY (username, group_name)
            )
            "#,
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO iam_groups(name, policy_json, is_system, created_at_ms) VALUES (?1, NULL, 1, ?2)",
        )
        .bind(Group::Admin.name())
        .bind(now_ms())
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
        // Hidden per-user signing keys for console-generated share links. Kept
        // in their own table (built-in/config admins are not `iam_users` rows,
        // so they can't use `access_keys`). Never surfaced in the UI.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS web_keys (
                username      TEXT PRIMARY KEY,
                access_key    TEXT NOT NULL UNIQUE,
                secret_key    TEXT NOT NULL,
                is_builtin    INTEGER NOT NULL,
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
        let mut direct_policies: HashMap<String, Option<PolicyDocument>> = HashMap::new();
        let users = sqlx::query("SELECT username, policy_json FROM iam_users")
            .fetch_all(&self.pool)
            .await?;
        for row in users {
            let username: String = row.get("username");
            let policy = row
                .get::<Option<String>, _>("policy_json")
                .and_then(|json| serde_json::from_str(&json).ok());
            direct_policies.insert(username.clone(), policy);
            snapshot.memberships.insert(username, Vec::new());
        }
        let groups = sqlx::query("SELECT name, policy_json, is_system FROM iam_groups")
            .fetch_all(&self.pool)
            .await?;
        let mut group_policies = HashMap::new();
        let mut group_types = HashMap::new();
        for row in groups {
            let name: String = row.get("name");
            let is_system = row.get::<i64, _>("is_system") != 0;
            let policy = row
                .get::<Option<String>, _>("policy_json")
                .and_then(|json| serde_json::from_str(&json).ok());
            group_policies.insert(name.to_ascii_lowercase(), policy);
            group_types.insert(
                name.to_ascii_lowercase(),
                Group::from_storage(&name, is_system)?,
            );
        }
        let memberships = sqlx::query("SELECT username, group_name FROM iam_user_groups")
            .fetch_all(&self.pool)
            .await?;
        for row in memberships {
            let username: String = row.get("username");
            let stored_name = row.get::<String, _>("group_name");
            let Some(group) = group_types.get(&stored_name.to_ascii_lowercase()).cloned() else {
                continue;
            };
            if !direct_policies.contains_key(&username) {
                continue;
            }
            snapshot
                .memberships
                .entry(username.clone())
                .or_default()
                .push(group);
        }
        for (username, direct) in direct_policies {
            let mut statements = direct
                .into_iter()
                .flat_map(|policy| policy.statement)
                .collect::<Vec<_>>();
            for group in snapshot.memberships.get(&username).into_iter().flatten() {
                if let Some(policy) = group.policy(&group_policies) {
                    statements.extend(policy.statement);
                }
            }
            snapshot.policies.insert(
                username,
                (!statements.is_empty()).then(|| PolicyDocument {
                    version: "2012-10-17".to_string(),
                    statement: statements,
                }),
            );
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
        let web_keys =
            sqlx::query("SELECT access_key, secret_key, username, is_builtin FROM web_keys")
                .fetch_all(&self.pool)
                .await?;
        for row in web_keys {
            snapshot.web_keys.insert(
                row.get("access_key"),
                (
                    row.get("secret_key"),
                    row.get("username"),
                    row.get::<i64, _>("is_builtin") != 0,
                ),
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

    /// Resolves a hidden `RSWEB_…` signing key to `(secret, username,
    /// is_builtin)`. Sync — used inside presigned-URL verification.
    pub fn find_web_key(&self, access_key: &str) -> Option<(String, String, bool)> {
        self.snapshot
            .read()
            .unwrap()
            .web_keys
            .get(access_key)
            .cloned()
    }

    /// Returns the caller's hidden web-signing key `(access_key, secret)`,
    /// creating it on first use. Used by the console to presign share links so
    /// a user never has to configure a real access key just to share. The key
    /// is owner-equivalent and never exposed.
    pub async fn web_key_for(
        &self,
        username: &str,
        is_builtin: bool,
    ) -> Result<(String, String)> {
        if let Some(row) =
            sqlx::query("SELECT access_key, secret_key FROM web_keys WHERE username = ?1")
                .bind(username)
                .fetch_optional(&self.pool)
                .await?
        {
            return Ok((row.get("access_key"), row.get("secret_key")));
        }
        let access_key = format!("RSWEB_{username}_{}", random_hex(12));
        let secret_key = random_hex(20);
        // `OR IGNORE` so a concurrent first-use race leaves exactly one row.
        sqlx::query(
            "INSERT OR IGNORE INTO web_keys(username, access_key, secret_key, is_builtin, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(username)
        .bind(&access_key)
        .bind(&secret_key)
        .bind(is_builtin as i64)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        self.reload().await?;
        let row = sqlx::query("SELECT access_key, secret_key FROM web_keys WHERE username = ?1")
            .bind(username)
            .fetch_one(&self.pool)
            .await?;
        Ok((row.get("access_key"), row.get("secret_key")))
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

    pub fn is_admin(&self, username: &str) -> bool {
        self.snapshot
            .read()
            .unwrap()
            .memberships
            .get(username)
            .map(|groups| groups.contains(&Group::Admin))
            .unwrap_or(false)
    }

    pub fn groups_for(&self, username: &str) -> Vec<String> {
        self.snapshot
            .read()
            .unwrap()
            .memberships
            .get(username)
            .map(|groups| groups.iter().map(|group| group.name().to_string()).collect())
            .unwrap_or_default()
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
        sqlx::query("DELETE FROM iam_user_groups WHERE username = ?1")
            .bind(username)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM access_keys WHERE username = ?1")
            .bind(username)
            .execute(&self.pool)
            .await?;
        // Their hidden signing key goes too, so any share links they made stop
        // working — deleting a user vanishes their shares.
        sqlx::query("DELETE FROM web_keys WHERE username = ?1")
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
        if let Some(policy) = policy {
            policy.validate().map_err(StorageError::Io)?;
        }
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

    // ── IAM groups ───────────────────────────────────────────────────────────

    pub async fn list_groups(&self) -> Result<Vec<IamGroup>> {
        let rows = sqlx::query(
            r#"
            SELECT g.name, g.policy_json, g.is_system, g.created_at_ms,
                   COUNT(m.username) AS members
            FROM iam_groups g
            LEFT JOIN iam_user_groups m ON m.group_name = g.name
            GROUP BY g.name, g.policy_json, g.is_system, g.created_at_ms
            ORDER BY g.is_system DESC, lower(g.name)
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let name: String = row.get("name");
                let group =
                    Group::from_storage(&name, row.get::<i64, _>("is_system") != 0)?;
                Ok(IamGroup {
                    group,
                    policy: row
                        .get::<Option<String>, _>("policy_json")
                        .and_then(|json| serde_json::from_str(&json).ok()),
                    created_at_ms: row.get("created_at_ms"),
                    members: row.get::<i64, _>("members").max(0) as u64,
                })
            })
            .collect::<Result<Vec<_>>>()?)
    }

    pub async fn create_group(
        &self,
        name: &str,
        policy: Option<&PolicyDocument>,
    ) -> Result<()> {
        let group = Group::named(name)?;
        if let Some(policy) = policy {
            policy.validate().map_err(StorageError::Io)?;
        }
        let json = policy.map(serde_json::to_string).transpose()?;
        sqlx::query(
            "INSERT INTO iam_groups(name, policy_json, is_system, created_at_ms) VALUES (?1, ?2, 0, ?3)",
        )
        .bind(group.name())
        .bind(json)
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .map_err(|_| StorageError::Io(format!("group {name} already exists")))?;
        self.reload().await
    }

    pub async fn set_group_policy(
        &self,
        name: &str,
        policy: Option<&PolicyDocument>,
    ) -> Result<()> {
        let group = self.resolve_groups(&[name.to_string()]).await?.remove(0);
        let Group::Named(group) = group else {
            return Err(StorageError::Io("admin group is not editable".into()));
        };
        if let Some(policy) = policy {
            policy.validate().map_err(StorageError::Io)?;
        }
        let json = policy.map(serde_json::to_string).transpose()?;
        let result = sqlx::query(
            "UPDATE iam_groups SET policy_json = ?2 WHERE name = ?1 AND is_system = 0",
        )
        .bind(group.as_str())
        .bind(json)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::Io(format!("no such editable group {name}")));
        }
        self.reload().await
    }

    pub async fn delete_group(&self, name: &str) -> Result<()> {
        let group = self.resolve_groups(&[name.to_string()]).await?.remove(0);
        let Group::Named(group) = group else {
            return Err(StorageError::Io("admin group cannot be deleted".into()));
        };
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM iam_user_groups WHERE group_name = ?1")
            .bind(group.as_str())
            .execute(&mut *tx)
            .await?;
        let result = sqlx::query("DELETE FROM iam_groups WHERE name = ?1 AND is_system = 0")
            .bind(group.as_str())
            .execute(&mut *tx)
            .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::Io(format!("no such editable group {name}")));
        }
        tx.commit().await?;
        self.reload().await
    }

    pub async fn resolve_groups(&self, names: &[String]) -> Result<Vec<Group>> {
        let mut resolved = Vec::new();
        let mut seen = HashSet::new();
        for name in names {
            let row = sqlx::query("SELECT name, is_system FROM iam_groups WHERE name = ?1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;
            let Some(row) = row else {
                return Err(StorageError::Io(format!("no such group {name}")));
            };
            let actual: String = row.get("name");
            let group = Group::from_storage(&actual, row.get::<i64, _>("is_system") != 0)?;
            if seen.insert(group.name().to_ascii_lowercase()) {
                resolved.push(group);
            }
        }
        Ok(resolved)
    }

    pub async fn set_user_groups(&self, username: &str, groups: &[Group]) -> Result<()> {
        if !self.user_exists(username) {
            return Err(StorageError::Io(format!("no such user {username}")));
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM iam_user_groups WHERE username = ?1")
            .bind(username)
            .execute(&mut *tx)
            .await?;
        for group in groups {
            sqlx::query(
                "INSERT INTO iam_user_groups(username, group_name) VALUES (?1, ?2)",
            )
            .bind(username)
            .bind(group.name())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.reload().await
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

fn validate_group_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.');
    if ok {
        Ok(())
    } else {
        Err(StorageError::Io(format!("invalid group name {name:?}")))
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
    use crate::server::policy::{is_authorized, Requirement};

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
    async fn web_key_is_stable_hidden_and_dies_with_the_user() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("wanda", "password123").await.unwrap();

        // Created on first use, marked, and stable across calls.
        let (ak, secret) = iam.web_key_for("wanda", false).await.unwrap();
        assert!(ak.starts_with("RSWEB_wanda_"));
        assert_eq!(iam.web_key_for("wanda", false).await.unwrap(), (ak.clone(), secret.clone()));

        // Resolves to its owner; not visible as a regular access key.
        assert_eq!(iam.find_web_key(&ak), Some((secret, "wanda".to_string(), false)));
        assert!(iam.find_key(&ak).is_none());
        assert!(iam.list_access_keys("wanda").await.unwrap().is_empty());

        // Deleting the user removes the web key (shares vanish).
        iam.delete_user("wanda").await.unwrap();
        assert!(iam.find_web_key(&ak).is_none());
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

    #[test]
    fn named_group_can_never_represent_admin() {
        for name in ["admin", "ADMIN", "Admin", "aDmIn"] {
            assert!(Group::named(name).is_err());
        }
        assert_eq!(Group::Admin.name(), "admin");
        assert!(matches!(
            Group::named("operators").unwrap(),
            Group::Named(_)
        ));
    }

    #[tokio::test]
    async fn user_and_group_policies_merge_with_explicit_deny_winning() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("frank", "password123").await.unwrap();
        let user_policy: PolicyDocument = serde_json::from_str(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}"#,
        )
        .unwrap();
        let group_policy: PolicyDocument = serde_json::from_str(
            r#"{"Statement":[{"Effect":"Deny","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::*"}]}"#,
        )
        .unwrap();
        iam.set_policy("frank", Some(&user_policy)).await.unwrap();
        iam.create_group("protected", Some(&group_policy)).await.unwrap();
        iam.set_user_groups("frank", &[Group::named("protected").unwrap()])
            .await
            .unwrap();

        let effective = iam.policy_for("frank").unwrap();
        assert!(is_authorized(
            &effective,
            &[Requirement::object("s3:GetObject", "b", "k")]
        ));
        assert!(!is_authorized(
            &effective,
            &[Requirement::object("s3:DeleteObject", "b", "k")]
        ));
    }

    #[tokio::test]
    async fn typed_admin_group_is_immutable_and_unrestricted() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("grace", "password123").await.unwrap();
        let resolved = iam.resolve_groups(&["AdMiN".to_string()]).await.unwrap();
        assert_eq!(resolved, vec![Group::Admin]);
        iam.set_user_groups("grace", &resolved)
            .await
            .unwrap();

        assert!(iam.is_admin("grace"));
        let effective = iam.policy_for("grace").unwrap();
        assert!(is_authorized(
            &effective,
            &[Requirement::object("s3:DeleteObject", "any", "key")]
        ));
        assert!(iam.set_group_policy(Group::Admin.name(), None).await.is_err());
        assert!(iam.delete_group(Group::Admin.name()).await.is_err());
    }
}
