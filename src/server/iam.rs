//! IAM users, access keys, and policies, backed by `admin.rocksdb` at the data
//! root (outside any bucket — bucket index rebuilds never touch it).
//!
//! This database is *primary* data, not a derived cache: it cannot be rebuilt
//! from anything, so it is deliberately tiny and easy to back up. The bootstrap
//! admin account lives in the config file instead, so an operator can always
//! log in even against a bare data directory.
//!
//! All authorization-path reads are served from an in-memory snapshot
//! (`Arc<RwLock<…>>`) so SigV4 validation stays synchronous; mutations write
//! RocksDB first, then refresh the snapshot. Because every auth lookup is
//! served from the snapshot, the on-disk layout needs no secondary indexes —
//! the handful of admin-path scans (list users, member counts, cascade delete)
//! run over tiny families and are cheap. Multi-family mutations
//! (`delete_user`, `delete_group`, `set_user_groups`) use a single atomic
//! `WriteBatch`.
//!
//! ## On-disk value format
//!
//! Every value is JSON tagged with a `"v"` version. Only **V1** exists today;
//! V1 parsing is lenient (missing fields default, unknown fields ignored) and a
//! value tagged with a newer version is rejected rather than misread.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use hmac::{Hmac, KeyInit, Mac};
use rand::Rng;
use rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options,
    WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// Multi-threaded RocksDB handle (see the same alias in `storage::index`).
type Db = DBWithThreadMode<MultiThreaded>;

use super::identity::Identity;
use super::policy::{Effect, OneOrMany, PolicyDocument, Statement};
use crate::storage::errors::{Result, StorageError};
use crate::storage::time::now_ms;

type HmacSha256 = Hmac<Sha256>;

const PBKDF2_ITERATIONS: u32 = 100_000;
const SESSION_TTL_MS: i64 = 12 * 60 * 60 * 1000;

/// Highest entity value version this build understands. See module docs.
const ENTITY_VERSION: u32 = 1;

/// The single tag of an access key that was stored before tags existed.
pub const DEFAULT_KEY_TAG: &str = "default";
const MAX_KEY_TAG_CHARS: usize = 256;
const MAX_KEY_TAGS: usize = 100;
/// Separates tags inside the stored `label` string. Tags are restricted to
/// letters, digits, and spaces, so it can never occur inside one.
const KEY_TAG_SEP: char = ',';

const CF_USERS: &str = "users";
const CF_GROUPS: &str = "groups";
const CF_ACCESS_KEYS: &str = "access_keys";
const CF_WEB_KEYS: &str = "web_keys";
const CF_USER_GROUPS: &str = "user_groups";

/// Separator between the two components of a `user_groups` key. Both usernames
/// and group names are validated to a restricted charset that excludes NUL, so
/// this can never collide with key data.
const SEP: u8 = 0;

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
    /// What the key is for — the apps, hosts, or owners using it. Never empty:
    /// a key stored before tags existed reads back as [`DEFAULT_KEY_TAG`].
    pub tags: Vec<String>,
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

// ── on-disk value encodings (V1) ────────────────────────────────────────────

fn default_version() -> u32 {
    ENTITY_VERSION
}

fn reject_newer(v: u32, what: &str) -> Result<()> {
    if v > ENTITY_VERSION {
        return Err(StorageError::Db(format!(
            "{what} value is v{v}, newer than this build understands (v{ENTITY_VERSION}); refusing to read"
        )));
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct UserV1 {
    #[serde(default = "default_version")]
    v: u32,
    #[serde(default)]
    password_hash: String,
    #[serde(default)]
    salt: String,
    #[serde(default)]
    policy_json: Option<String>,
    #[serde(default)]
    created_at_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct GroupV1 {
    #[serde(default = "default_version")]
    v: u32,
    /// Original display casing; the RocksDB key is the lowercased name.
    #[serde(default)]
    name: String,
    #[serde(default)]
    policy_json: Option<String>,
    #[serde(default)]
    is_system: bool,
    #[serde(default)]
    created_at_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct AccessKeyV1 {
    #[serde(default = "default_version")]
    v: u32,
    /// The access-key id. Also the RocksDB key; carried here so a snapshot scan
    /// need not thread key bytes through.
    #[serde(default)]
    access_key: String,
    #[serde(default)]
    secret_key: String,
    #[serde(default)]
    username: String,
    /// The key's tags joined with [`KEY_TAG_SEP`]; see [`AccessKeyV1::tags`].
    /// The field began life as a free-text label, hence the name — keeping it
    /// means a label written by that build is simply a one-tag key, and that
    /// build still reads what this one writes. Absent on older keys still.
    #[serde(default)]
    label: String,
    #[serde(default)]
    created_at_ms: i64,
}

impl AccessKeyV1 {
    /// The key's tags. Reading is lenient where writing is strict: whatever an
    /// older build stored is split and shown as it is, never rejected, and a
    /// key stored with nothing at all gets [`DEFAULT_KEY_TAG`].
    fn tags(&self) -> Vec<String> {
        let tags: Vec<String> = self
            .label
            .split(KEY_TAG_SEP)
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
            .map(str::to_string)
            .collect();
        match tags.is_empty() {
            true => vec![DEFAULT_KEY_TAG.to_string()],
            false => tags,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WebKeyV1 {
    #[serde(default = "default_version")]
    v: u32,
    #[serde(default)]
    access_key: String,
    #[serde(default)]
    secret_key: String,
    #[serde(default)]
    is_builtin: bool,
    #[serde(default)]
    created_at_ms: i64,
}

fn to_vec<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("IAM value serializes")
}

fn from_slice<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_slice(bytes)?)
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

/// Raw rows scanned out of RocksDB, before the (pure) snapshot assembly.
#[derive(Default)]
struct RawTables {
    users: Vec<(String, UserV1)>,
    groups: Vec<GroupV1>,
    /// (username, lowercased group name)
    memberships: Vec<(String, String)>,
    access_keys: Vec<AccessKeyV1>,
    /// (username, web key). Username is the RocksDB key of the family.
    web_keys: Vec<(String, WebKeyV1)>,
}

#[derive(Debug, Clone)]
struct Session {
    username: String,
    is_root: bool,
    expires_at_ms: i64,
}

#[derive(Clone)]
pub struct IamStore {
    db: Arc<Db>,
    snapshot: Arc<RwLock<Snapshot>>,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
}

fn cf<'a>(db: &'a Db, name: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'a>>> {
    db.cf_handle(name)
        .ok_or_else(|| StorageError::Db(format!("missing column family {name}")))
}

fn sync_write() -> WriteOptions {
    // IAM is primary data: always fsync the WAL on write.
    let mut opts = WriteOptions::default();
    opts.set_sync(true);
    opts
}

fn membership_key(username: &str, group_lower: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(username.len() + 1 + group_lower.len());
    key.extend_from_slice(username.as_bytes());
    key.push(SEP);
    key.extend_from_slice(group_lower.as_bytes());
    key
}

/// Splits a `user_groups` key back into `(username, group_lower)`.
fn split_membership_key(key: &[u8]) -> Option<(String, String)> {
    let idx = key.iter().position(|&b| b == SEP)?;
    let username = String::from_utf8_lossy(&key[..idx]).into_owned();
    let group = String::from_utf8_lossy(&key[idx + 1..]).into_owned();
    Some((username, group))
}

async fn blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(err) => Err(StorageError::Db(format!("iam task panicked: {err}"))),
    }
}

impl IamStore {
    /// Opens (creating if absent) `<data_root>/admin.rocksdb` and loads the
    /// in-memory snapshot.
    pub async fn open(data_root: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(data_root).await?;
        let db_path = data_root.join("admin.rocksdb");
        let db = blocking(move || {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            let cfs = [CF_USERS, CF_GROUPS, CF_ACCESS_KEYS, CF_WEB_KEYS, CF_USER_GROUPS]
                .into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
            let db = Db::open_cf_descriptors(&opts, &db_path, cfs)?;
            // Seed the built-in admin group if absent. Scoped so the CF handle
            // (which borrows `db`) is dropped before `db` is moved out.
            {
                let groups = cf(&db, CF_GROUPS)?;
                if db.get_cf(&groups, Group::ADMIN_NAME.as_bytes())?.is_none() {
                    let value = GroupV1 {
                        v: ENTITY_VERSION,
                        name: Group::ADMIN_NAME.to_string(),
                        policy_json: None,
                        is_system: true,
                        created_at_ms: now_ms(),
                    };
                    db.put_cf_opt(&groups, Group::ADMIN_NAME.as_bytes(), to_vec(&value), &sync_write())?;
                }
            }
            Ok(db)
        })
        .await?;
        let store = Self {
            db: Arc::new(db),
            snapshot: Arc::new(RwLock::new(Snapshot::default())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        };
        store.reload().await?;
        Ok(store)
    }

    /// Scans every family into owned rows. Runs on the blocking pool.
    async fn scan_all(&self) -> Result<RawTables> {
        let db = self.db.clone();
        blocking(move || {
            let mut raw = RawTables::default();
            let users = cf(&db, CF_USERS)?;
            for item in db.iterator_cf(&users, IteratorMode::Start) {
                let (key, value) = item?;
                let user: UserV1 = from_slice(&value)?;
                reject_newer(user.v, "iam user")?;
                raw.users.push((String::from_utf8_lossy(&key).into_owned(), user));
            }
            let groups = cf(&db, CF_GROUPS)?;
            for item in db.iterator_cf(&groups, IteratorMode::Start) {
                let (_key, value) = item?;
                let group: GroupV1 = from_slice(&value)?;
                reject_newer(group.v, "iam group")?;
                raw.groups.push(group);
            }
            let memberships = cf(&db, CF_USER_GROUPS)?;
            for item in db.iterator_cf(&memberships, IteratorMode::Start) {
                let (key, _value) = item?;
                if let Some((username, group_lower)) = split_membership_key(&key) {
                    raw.memberships.push((username, group_lower));
                }
            }
            let access = cf(&db, CF_ACCESS_KEYS)?;
            for item in db.iterator_cf(&access, IteratorMode::Start) {
                let (key, value) = item?;
                let mut ak: AccessKeyV1 = from_slice(&value)?;
                reject_newer(ak.v, "access key")?;
                // The access-key id is the RocksDB key; trust it over the value.
                ak.access_key = String::from_utf8_lossy(&key).into_owned();
                raw.access_keys.push(ak);
            }
            let web = cf(&db, CF_WEB_KEYS)?;
            for item in db.iterator_cf(&web, IteratorMode::Start) {
                let (key, value) = item?;
                let wk: WebKeyV1 = from_slice(&value)?;
                reject_newer(wk.v, "web key")?;
                raw.web_keys.push((String::from_utf8_lossy(&key).into_owned(), wk));
            }
            Ok(raw)
        })
        .await
    }

    /// Rebuilds the in-memory snapshot from RocksDB. Called after every
    /// mutation; cheap because the families are tiny.
    async fn reload(&self) -> Result<()> {
        let raw = self.scan_all().await?;
        let mut snapshot = Snapshot::default();

        let mut direct_policies: HashMap<String, Option<PolicyDocument>> = HashMap::new();
        for (username, user) in &raw.users {
            let policy = user
                .policy_json
                .as_ref()
                .and_then(|json| serde_json::from_str(json).ok());
            direct_policies.insert(username.clone(), policy);
            snapshot.memberships.insert(username.clone(), Vec::new());
        }

        let mut group_policies = HashMap::new();
        let mut group_types = HashMap::new();
        for group in &raw.groups {
            let policy = group
                .policy_json
                .as_ref()
                .and_then(|json| serde_json::from_str(json).ok());
            let lower = group.name.to_ascii_lowercase();
            group_policies.insert(lower.clone(), policy);
            group_types.insert(lower, Group::from_storage(&group.name, group.is_system)?);
        }

        for (username, group_lower) in &raw.memberships {
            let Some(group) = group_types.get(group_lower).cloned() else {
                continue;
            };
            if !direct_policies.contains_key(username) {
                continue;
            }
            snapshot
                .memberships
                .entry(username.clone())
                .or_default()
                .push(group);
        }

        for (username, direct) in direct_policies {
            // An administrator is root: their effective policy is the admin
            // grant and nothing else, so no Deny — from another group or from
            // a policy attached to them directly — can bind them. (Request
            // paths go further and skip evaluation altogether; see
            // `identity_for`. This keeps `policy_for` from saying otherwise.)
            let memberships = snapshot.memberships.get(&username);
            if memberships.is_some_and(|groups| groups.contains(&Group::Admin)) {
                snapshot.policies.insert(username, Group::Admin.policy(&group_policies));
                continue;
            }
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

        for ak in raw.access_keys {
            // The access-key id is the family key; carried in `access_key`.
            snapshot
                .keys
                .insert(ak.access_key.clone(), (ak.secret_key, ak.username));
        }
        for (username, wk) in raw.web_keys {
            snapshot
                .web_keys
                .insert(wk.access_key, (wk.secret_key, username, wk.is_builtin));
        }

        *self.snapshot.write().unwrap() = snapshot;
        Ok(())
    }

    // ── synchronous auth-path lookups (in-memory) ─────────────────────────────

    /// Resolves an access key to its secret and owning user. Sync — used inside
    /// SigV4 validation.
    pub fn find_key(&self, access_key: &str) -> Option<(String, String)> {
        self.snapshot.read().unwrap().keys.get(access_key).cloned()
    }

    /// Resolves a hidden `RSWEB_…` signing key to `(secret, username,
    /// is_builtin)`. Sync — used inside presigned-URL verification.
    pub fn find_web_key(&self, access_key: &str) -> Option<(String, String, bool)> {
        self.snapshot.read().unwrap().web_keys.get(access_key).cloned()
    }

    /// Returns the caller's hidden web-signing key `(access_key, secret)`,
    /// creating it on first use. Used by the console to presign share links so a
    /// user never has to configure a real access key just to share. The key is
    /// owner-equivalent and never exposed.
    pub async fn web_key_for(&self, username: &str, is_builtin: bool) -> Result<(String, String)> {
        let db = self.db.clone();
        let username_owned = username.to_string();
        let existing = blocking({
            let db = db.clone();
            let username = username_owned.clone();
            move || {
                let web = cf(&db, CF_WEB_KEYS)?;
                match db.get_cf(&web, username.as_bytes())? {
                    Some(value) => {
                        let wk: WebKeyV1 = from_slice(&value)?;
                        reject_newer(wk.v, "web key")?;
                        Ok(Some((wk.access_key, wk.secret_key)))
                    }
                    None => Ok(None),
                }
            }
        })
        .await?;
        if let Some(found) = existing {
            return Ok(found);
        }
        let access_key = format!("RSWEB_{username}_{}", random_hex(12));
        let secret_key = random_hex(20);
        let created = now_ms();
        let result = blocking({
            let db = db.clone();
            let username = username_owned.clone();
            let access_key = access_key.clone();
            let secret_key = secret_key.clone();
            move || {
                let web = cf(&db, CF_WEB_KEYS)?;
                // Only write if still absent, so a concurrent first-use race
                // leaves exactly one row (mirrors the old `INSERT OR IGNORE`).
                if let Some(value) = db.get_cf(&web, username.as_bytes())? {
                    let wk: WebKeyV1 = from_slice(&value)?;
                    return Ok((wk.access_key, wk.secret_key));
                }
                let value = WebKeyV1 {
                    v: ENTITY_VERSION,
                    access_key: access_key.clone(),
                    secret_key: secret_key.clone(),
                    is_builtin,
                    created_at_ms: created,
                };
                db.put_cf_opt(&web, username.as_bytes(), to_vec(&value), &sync_write())?;
                Ok((access_key, secret_key))
            }
        })
        .await?;
        self.reload().await?;
        Ok(result)
    }

    /// Returns the user's policy. `None` (no user / no policy attached) must be
    /// treated as deny-everything.
    pub fn policy_for(&self, username: &str) -> Option<PolicyDocument> {
        self.snapshot.read().unwrap().policies.get(username).cloned().flatten()
    }

    /// The identity an IAM user acts as — the one place that decides it, for
    /// the S3 API and the console alike. A member of the `admin` group is root
    /// in every sense: unrestricted, with no policy evaluated at all, so there
    /// is nothing a Deny could attach to. Everyone else is bound by their
    /// effective policy (none attached denies everything).
    pub fn identity_for(&self, username: &str) -> Identity {
        match self.is_admin(username) {
            true => Identity::root(Some(username.to_string()), None),
            false => Identity::iam(username.to_string(), self.policy_for(username)),
        }
    }

    pub fn user_exists(&self, username: &str) -> bool {
        self.snapshot.read().unwrap().policies.contains_key(username)
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
        let db = self.db.clone();
        let username_owned = username.to_string();
        blocking(move || {
            let users = cf(&db, CF_USERS)?;
            if db.get_cf(&users, username_owned.as_bytes())?.is_some() {
                return Err(StorageError::Io(format!("user {username_owned} already exists")));
            }
            let value = UserV1 {
                v: ENTITY_VERSION,
                password_hash: hash,
                salt,
                policy_json: None,
                created_at_ms: now_ms(),
            };
            db.put_cf_opt(&users, username_owned.as_bytes(), to_vec(&value), &sync_write())?;
            Ok(())
        })
        .await?;
        self.reload().await
    }

    pub async fn set_password(&self, username: &str, password: &str) -> Result<()> {
        if password.len() < 8 {
            return Err(StorageError::Io("password must be at least 8 characters".into()));
        }
        let salt = random_hex(16);
        let hash = pbkdf2_hex(password, &salt);
        let db = self.db.clone();
        let username_owned = username.to_string();
        blocking(move || {
            let users = cf(&db, CF_USERS)?;
            let Some(value) = db.get_cf(&users, username_owned.as_bytes())? else {
                return Err(StorageError::Io(format!("no such user {username_owned}")));
            };
            let mut user: UserV1 = from_slice(&value)?;
            user.v = ENTITY_VERSION;
            user.password_hash = hash;
            user.salt = salt;
            db.put_cf_opt(&users, username_owned.as_bytes(), to_vec(&user), &sync_write())?;
            Ok(())
        })
        .await?;
        // A password reset is a security boundary: existing console sessions for
        // the user must authenticate again with the new value.
        self.sessions
            .lock()
            .unwrap()
            .retain(|_, s| s.is_root || s.username != username);
        Ok(())
    }

    pub async fn delete_user(&self, username: &str) -> Result<()> {
        let db = self.db.clone();
        let username_owned = username.to_string();
        blocking(move || {
            let users = cf(&db, CF_USERS)?;
            let access = cf(&db, CF_ACCESS_KEYS)?;
            let web = cf(&db, CF_WEB_KEYS)?;
            let user_groups = cf(&db, CF_USER_GROUPS)?;
            let mut batch = WriteBatch::default();

            // Group memberships: all keys prefixed by `username\0`.
            let prefix = membership_key(&username_owned, "");
            for item in db.iterator_cf(&user_groups, IteratorMode::From(&prefix, Direction::Forward)) {
                let (key, _) = item?;
                if !key.starts_with(&prefix) {
                    break;
                }
                batch.delete_cf(&user_groups, &key);
            }
            // Access keys owned by the user (value carries username).
            for item in db.iterator_cf(&access, IteratorMode::Start) {
                let (key, value) = item?;
                let ak: AccessKeyV1 = from_slice(&value)?;
                if ak.username == username_owned {
                    batch.delete_cf(&access, &key);
                }
            }
            // Their hidden signing key goes too, so any share links they made
            // stop working — deleting a user vanishes their shares.
            batch.delete_cf(&web, username_owned.as_bytes());
            batch.delete_cf(&users, username_owned.as_bytes());
            db.write_opt(batch, &sync_write())?;
            Ok(())
        })
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
        let db = self.db.clone();
        let username_owned = username.to_string();
        blocking(move || {
            let users = cf(&db, CF_USERS)?;
            let Some(value) = db.get_cf(&users, username_owned.as_bytes())? else {
                return Err(StorageError::Io(format!("no such user {username_owned}")));
            };
            let mut user: UserV1 = from_slice(&value)?;
            user.v = ENTITY_VERSION;
            user.policy_json = json;
            db.put_cf_opt(&users, username_owned.as_bytes(), to_vec(&user), &sync_write())?;
            Ok(())
        })
        .await?;
        self.reload().await
    }

    pub async fn list_users(&self) -> Result<Vec<IamUser>> {
        let raw = self.scan_all().await?;
        let mut users = raw
            .users
            .into_iter()
            .map(|(username, user)| IamUser {
                username,
                policy: user
                    .policy_json
                    .as_ref()
                    .and_then(|json| serde_json::from_str(json).ok()),
                created_at_ms: user.created_at_ms,
            })
            .collect::<Vec<_>>();
        users.sort_by(|a, b| a.username.cmp(&b.username));
        Ok(users)
    }

    // ── IAM groups ───────────────────────────────────────────────────────────

    pub async fn list_groups(&self) -> Result<Vec<IamGroup>> {
        let raw = self.scan_all().await?;
        let mut counts: HashMap<String, u64> = HashMap::new();
        for (_username, group_lower) in &raw.memberships {
            *counts.entry(group_lower.clone()).or_default() += 1;
        }
        let mut groups = raw
            .groups
            .into_iter()
            .map(|group| {
                let members = counts.get(&group.name.to_ascii_lowercase()).copied().unwrap_or(0);
                let g = Group::from_storage(&group.name, group.is_system)?;
                Ok(IamGroup {
                    group: g,
                    policy: group
                        .policy_json
                        .as_ref()
                        .and_then(|json| serde_json::from_str(json).ok()),
                    created_at_ms: group.created_at_ms,
                    members,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // System groups first, then case-insensitive by name.
        groups.sort_by(|a, b| {
            let a_sys = matches!(a.group, Group::Admin);
            let b_sys = matches!(b.group, Group::Admin);
            b_sys
                .cmp(&a_sys)
                .then_with(|| a.group.name().to_ascii_lowercase().cmp(&b.group.name().to_ascii_lowercase()))
        });
        Ok(groups)
    }

    pub async fn create_group(&self, name: &str, policy: Option<&PolicyDocument>) -> Result<()> {
        let group = Group::named(name)?;
        if let Some(policy) = policy {
            policy.validate().map_err(StorageError::Io)?;
        }
        let json = policy.map(serde_json::to_string).transpose()?;
        let db = self.db.clone();
        let name_owned = name.to_string();
        let stored_name = group.name().to_string();
        blocking(move || {
            let groups = cf(&db, CF_GROUPS)?;
            let key = stored_name.to_ascii_lowercase();
            if db.get_cf(&groups, key.as_bytes())?.is_some() {
                return Err(StorageError::Io(format!("group {name_owned} already exists")));
            }
            let value = GroupV1 {
                v: ENTITY_VERSION,
                name: stored_name,
                policy_json: json,
                is_system: false,
                created_at_ms: now_ms(),
            };
            // A new group starts with no members. Memberships are keyed by
            // group *name*, so a row left under this name — by a delete that
            // raced an assignment, or by an import that carried memberships
            // without their group — would otherwise come alive here and hand
            // its user this group's policy without anyone having added them.
            let user_groups = cf(&db, CF_USER_GROUPS)?;
            let mut batch = WriteBatch::default();
            for item in db.iterator_cf(&user_groups, IteratorMode::Start) {
                let (membership, _) = item?;
                if split_membership_key(&membership).is_some_and(|(_, g)| g == key) {
                    batch.delete_cf(&user_groups, &membership);
                }
            }
            batch.put_cf(&groups, key.as_bytes(), to_vec(&value));
            db.write_opt(batch, &sync_write())?;
            Ok(())
        })
        .await?;
        self.reload().await
    }

    pub async fn set_group_policy(&self, name: &str, policy: Option<&PolicyDocument>) -> Result<()> {
        let group = self.resolve_groups(&[name.to_string()]).await?.remove(0);
        let Group::Named(group) = group else {
            return Err(StorageError::Io("admin group is not editable".into()));
        };
        if let Some(policy) = policy {
            policy.validate().map_err(StorageError::Io)?;
        }
        let json = policy.map(serde_json::to_string).transpose()?;
        let db = self.db.clone();
        let name_owned = name.to_string();
        let stored_name = group.as_str().to_string();
        blocking(move || {
            let groups = cf(&db, CF_GROUPS)?;
            let key = stored_name.to_ascii_lowercase();
            let Some(value) = db.get_cf(&groups, key.as_bytes())? else {
                return Err(StorageError::Io(format!("no such editable group {name_owned}")));
            };
            let mut record: GroupV1 = from_slice(&value)?;
            if record.is_system {
                return Err(StorageError::Io(format!("no such editable group {name_owned}")));
            }
            record.v = ENTITY_VERSION;
            record.policy_json = json;
            db.put_cf_opt(&groups, key.as_bytes(), to_vec(&record), &sync_write())?;
            Ok(())
        })
        .await?;
        self.reload().await
    }

    pub async fn delete_group(&self, name: &str) -> Result<()> {
        let group = self.resolve_groups(&[name.to_string()]).await?.remove(0);
        let Group::Named(group) = group else {
            return Err(StorageError::Io("admin group cannot be deleted".into()));
        };
        let db = self.db.clone();
        let name_owned = name.to_string();
        let group_lower = group.as_str().to_ascii_lowercase();
        blocking(move || {
            let groups = cf(&db, CF_GROUPS)?;
            let user_groups = cf(&db, CF_USER_GROUPS)?;
            let Some(value) = db.get_cf(&groups, group_lower.as_bytes())? else {
                return Err(StorageError::Io(format!("no such editable group {name_owned}")));
            };
            let record: GroupV1 = from_slice(&value)?;
            if record.is_system {
                return Err(StorageError::Io(format!("no such editable group {name_owned}")));
            }
            let mut batch = WriteBatch::default();
            // Remove every membership referencing this group (suffix match).
            for item in db.iterator_cf(&user_groups, IteratorMode::Start) {
                let (key, _) = item?;
                if let Some((_, g)) = split_membership_key(&key) {
                    if g == group_lower {
                        batch.delete_cf(&user_groups, &key);
                    }
                }
            }
            batch.delete_cf(&groups, group_lower.as_bytes());
            db.write_opt(batch, &sync_write())?;
            Ok(())
        })
        .await?;
        self.reload().await
    }

    pub async fn resolve_groups(&self, names: &[String]) -> Result<Vec<Group>> {
        let db = self.db.clone();
        let names = names.to_vec();
        blocking(move || {
            let groups = cf(&db, CF_GROUPS)?;
            let mut resolved = Vec::new();
            let mut seen = HashSet::new();
            for name in &names {
                let key = name.to_ascii_lowercase();
                let Some(value) = db.get_cf(&groups, key.as_bytes())? else {
                    return Err(StorageError::Io(format!("no such group {name}")));
                };
                let record: GroupV1 = from_slice(&value)?;
                let group = Group::from_storage(&record.name, record.is_system)?;
                if seen.insert(group.name().to_ascii_lowercase()) {
                    resolved.push(group);
                }
            }
            Ok(resolved)
        })
        .await
    }

    pub async fn set_user_groups(&self, username: &str, groups: &[Group]) -> Result<()> {
        if !self.user_exists(username) {
            return Err(StorageError::Io(format!("no such user {username}")));
        }
        let db = self.db.clone();
        let username_owned = username.to_string();
        let group_lowers: Vec<String> = groups.iter().map(|g| g.name().to_ascii_lowercase()).collect();
        blocking(move || {
            let user_groups = cf(&db, CF_USER_GROUPS)?;
            // Every group must exist *now*: a `Group` value only proves the name
            // was well-formed, or that the group existed when it was resolved.
            // See `create_group` for what a membership without a group would do.
            let groups = cf(&db, CF_GROUPS)?;
            for group_lower in &group_lowers {
                if db.get_cf(&groups, group_lower.as_bytes())?.is_none() {
                    return Err(StorageError::Io(format!("no such group {group_lower}")));
                }
            }
            let mut batch = WriteBatch::default();
            // Clear the user's existing memberships (prefix `username\0`).
            let prefix = membership_key(&username_owned, "");
            for item in db.iterator_cf(&user_groups, IteratorMode::From(&prefix, Direction::Forward)) {
                let (key, _) = item?;
                if !key.starts_with(&prefix) {
                    break;
                }
                batch.delete_cf(&user_groups, &key);
            }
            for group_lower in &group_lowers {
                batch.put_cf(&user_groups, membership_key(&username_owned, group_lower), []);
            }
            db.write_opt(batch, &sync_write())?;
            Ok(())
        })
        .await?;
        self.reload().await
    }

    pub async fn verify_password(&self, username: &str, password: &str) -> Result<bool> {
        let db = self.db.clone();
        let username_owned = username.to_string();
        let stored = blocking(move || {
            let users = cf(&db, CF_USERS)?;
            match db.get_cf(&users, username_owned.as_bytes())? {
                Some(value) => {
                    let user: UserV1 = from_slice(&value)?;
                    reject_newer(user.v, "iam user")?;
                    Ok(Some((user.password_hash, user.salt)))
                }
                None => Ok(None),
            }
        })
        .await?;
        let Some((hash, salt)) = stored else {
            return Ok(false);
        };
        let candidate = pbkdf2_hex(password, &salt);
        Ok(constant_time_eq(&candidate, &hash))
    }

    // ── access keys ───────────────────────────────────────────────────────────

    pub async fn create_access_key(&self, username: &str, tags: &[String]) -> Result<AccessKey> {
        if !self.user_exists(username) {
            return Err(StorageError::Io(format!("no such user {username}")));
        }
        let key = AccessKey {
            access_key: format!("RSAK{}", random_hex(8).to_uppercase()),
            secret_key: random_hex(20),
            username: username.to_string(),
            tags: validate_key_tags(tags)?,
            created_at_ms: now_ms(),
        };
        let db = self.db.clone();
        let stored = key.clone();
        blocking(move || {
            let access = cf(&db, CF_ACCESS_KEYS)?;
            let value = AccessKeyV1 {
                v: ENTITY_VERSION,
                access_key: stored.access_key.clone(),
                secret_key: stored.secret_key,
                username: stored.username,
                label: join_key_tags(&stored.tags),
                created_at_ms: stored.created_at_ms,
            };
            db.put_cf_opt(&access, stored.access_key.as_bytes(), to_vec(&value), &sync_write())?;
            Ok(())
        })
        .await?;
        self.reload().await?;
        Ok(key)
    }

    /// Replaces an existing access key's tags. The secret, owner, and creation
    /// time are untouched, so nothing the auth path reads changes.
    pub async fn set_access_key_tags(&self, access_key: &str, tags: &[String]) -> Result<()> {
        let label = join_key_tags(&validate_key_tags(tags)?);
        let db = self.db.clone();
        let access_key_owned = access_key.to_string();
        blocking(move || {
            let access = cf(&db, CF_ACCESS_KEYS)?;
            let Some(value) = db.get_cf(&access, access_key_owned.as_bytes())? else {
                return Err(StorageError::Io(format!("no such access key {access_key_owned}")));
            };
            let mut ak: AccessKeyV1 = from_slice(&value)?;
            reject_newer(ak.v, "access key")?;
            ak.label = label;
            db.put_cf_opt(&access, access_key_owned.as_bytes(), to_vec(&ak), &sync_write())?;
            Ok(())
        })
        .await
    }

    pub async fn delete_access_key(&self, access_key: &str) -> Result<()> {
        let db = self.db.clone();
        let access_key_owned = access_key.to_string();
        blocking(move || {
            let access = cf(&db, CF_ACCESS_KEYS)?;
            db.delete_cf_opt(&access, access_key_owned.as_bytes(), &sync_write())?;
            Ok(())
        })
        .await?;
        self.reload().await
    }

    pub async fn list_access_keys(&self, username: &str) -> Result<Vec<AccessKey>> {
        let db = self.db.clone();
        let username_owned = username.to_string();
        blocking(move || {
            let access = cf(&db, CF_ACCESS_KEYS)?;
            let mut keys = Vec::new();
            for item in db.iterator_cf(&access, IteratorMode::Start) {
                let (key, value) = item?;
                let ak: AccessKeyV1 = from_slice(&value)?;
                reject_newer(ak.v, "access key")?;
                if ak.username == username_owned {
                    keys.push(AccessKey {
                        access_key: String::from_utf8_lossy(&key).into_owned(),
                        tags: ak.tags(),
                        secret_key: ak.secret_key,
                        username: ak.username,
                        created_at_ms: ak.created_at_ms,
                    });
                }
            }
            keys.sort_by_key(|k| k.created_at_ms);
            Ok(keys)
        })
        .await
    }

    // ── raw export / import (backup, restore, migrate) ────────────────────────

    /// Dumps the entire global IAM database (all column families) to an
    /// in-memory buffer as a length-delimited protobuf stream of `(cf, key,
    /// value)` rows. See [`crate::storage::rawdb`]. Read-only and consistent
    /// (single snapshot); safe against the live store, so the console serves it
    /// straight into an HTTP download.
    pub async fn export_raw(&self) -> Result<Vec<u8>> {
        let db = self.db.clone();
        blocking(move || {
            let mut buf = Vec::new();
            crate::storage::rawdb::export(
                &db,
                &[CF_USERS, CF_GROUPS, CF_ACCESS_KEYS, CF_WEB_KEYS, CF_USER_GROUPS],
                &mut buf,
            )?;
            Ok(buf)
        })
        .await
    }

    /// Imports a dump produced by [`export_raw`](Self::export_raw), upserting
    /// every row (`put`) in one atomic batch. No family is cleared, so importing
    /// onto a fresh data directory reproduces the source exactly and onto a
    /// populated one merges. `Replace` erases the IAM families first for an exact
    /// restore. The apply is one atomic batch. Refreshes the in-memory snapshot
    /// afterward. Returns a per-family
    /// [`ImportReport`](crate::storage::rawdb::ImportReport) so the caller can
    /// show exactly what landed.
    pub async fn import_raw(
        &self,
        dump: Vec<u8>,
        mode: crate::storage::rawdb::ImportMode,
    ) -> Result<crate::storage::rawdb::ImportReport> {
        let db = self.db.clone();
        let report = blocking(move || {
            crate::storage::rawdb::import(
                &db,
                dump.as_slice(),
                mode,
                &[CF_USERS, CF_GROUPS, CF_ACCESS_KEYS, CF_WEB_KEYS, CF_USER_GROUPS],
            )
        })
        .await?;
        self.reload().await?;
        Ok(report)
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
        sessions.get(token).map(|s| (s.username.clone(), s.is_root))
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

/// Normalizes caller-supplied access key tags. Each is trimmed with inner
/// whitespace runs collapsed, and may hold only ASCII letters, digits, and
/// spaces — so a tag can never contain the separator it is stored with, nor
/// anything that needs escaping where it is rendered. Blank entries are
/// dropped and case-insensitive duplicates keep their first spelling. At least
/// one tag must remain.
fn validate_key_tags(tags: &[String]) -> Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for raw in tags {
        let tag = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        if tag.is_empty() {
            continue;
        }
        if !tag.chars().all(|c| c.is_ascii_alphanumeric() || c == ' ') {
            return Err(StorageError::Io(format!(
                "invalid tag {tag:?}: tags may contain only letters, digits, and spaces"
            )));
        }
        if tag.len() > MAX_KEY_TAG_CHARS {
            return Err(StorageError::Io(format!(
                "tag {tag:?} is longer than {MAX_KEY_TAG_CHARS} characters"
            )));
        }
        if !out.iter().any(|seen| seen.eq_ignore_ascii_case(&tag)) {
            out.push(tag);
        }
    }
    if out.is_empty() {
        return Err(StorageError::Io("an access key needs at least one tag".into()));
    }
    if out.len() > MAX_KEY_TAGS {
        return Err(StorageError::Io(format!(
            "an access key can have at most {MAX_KEY_TAGS} tags"
        )));
    }
    Ok(out)
}

fn join_key_tags(tags: &[String]) -> String {
    tags.join(&KEY_TAG_SEP.to_string())
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
    rand::rng().fill_bytes(&mut buf);
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

    fn tags(tags: &[&str]) -> Vec<String> {
        tags.iter().map(|tag| tag.to_string()).collect()
    }

    #[tokio::test]
    async fn access_key_tags_are_required_validated_and_editable() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("dave", "password123").await.unwrap();

        // At least one tag; letters, digits, and spaces only; bounded.
        assert!(iam.create_access_key("dave", &[]).await.is_err());
        assert!(iam.create_access_key("dave", &tags(&["  ", ""])).await.is_err());
        for bad in ["back-up", "a,b", "a;b", "caf\u{e9}", "under_score", "<b>", "nul\0"] {
            assert!(iam.create_access_key("dave", &tags(&[bad])).await.is_err(), "{bad:?}");
        }
        assert!(iam.create_access_key("dave", &tags(&[&"x".repeat(257)])).await.is_err());
        let too_many: Vec<String> = (0..101).map(|i| format!("t{i}")).collect();
        assert!(iam.create_access_key("dave", &too_many).await.is_err());
        // The limits themselves are fine, and survive the round trip to disk.
        let mut most: Vec<String> = (0..99).map(|i| format!("t{i}")).collect();
        most.push("x".repeat(256));
        let big = iam.create_access_key("dave", &most).await.unwrap();
        assert_eq!(big.tags, most);
        assert_eq!(iam.list_access_keys("dave").await.unwrap()[0].tags, most);
        iam.delete_access_key(&big.access_key).await.unwrap();

        // Normalized: trimmed, inner spaces collapsed, blanks dropped, and
        // duplicates folded case-insensitively onto their first spelling.
        let key = iam
            .create_access_key("dave", &tags(&["  Backup   Job ", "", "nas01", "NAS01", "backup job"]))
            .await
            .unwrap();
        assert_eq!(key.tags, ["Backup Job", "nas01"]);
        // Any whitespace counts as a space, so what is stored is still only
        // letters, digits, and single spaces.
        let odd = iam.create_access_key("dave", &tags(&["new\nline\there"])).await.unwrap();
        assert_eq!(odd.tags, ["new line here"]);
        iam.delete_access_key(&odd.access_key).await.unwrap();
        let listed = iam.list_access_keys("dave").await.unwrap();
        assert_eq!(listed[0].tags, ["Backup Job", "nas01"]);

        // Retagging keeps the credential intact, and is validated the same way.
        iam.set_access_key_tags(&key.access_key, &tags(&["prod", "etl 2"])).await.unwrap();
        assert!(iam.set_access_key_tags(&key.access_key, &[]).await.is_err());
        assert!(iam.set_access_key_tags(&key.access_key, &tags(&["no-hyphens"])).await.is_err());
        assert!(iam.set_access_key_tags("RSAKNOPE", &tags(&["x"])).await.is_err());
        let listed = iam.list_access_keys("dave").await.unwrap();
        assert_eq!(listed[0].tags, ["prod", "etl 2"]);
        assert_eq!(listed[0].secret_key, key.secret_key);
        assert_eq!(listed[0].created_at_ms, key.created_at_ms);
        assert_eq!(
            iam.find_key(&key.access_key).unwrap(),
            (key.secret_key.clone(), "dave".to_string())
        );
    }

    #[tokio::test]
    async fn rows_from_before_tags_read_back_as_tags_without_migration() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("erin", "password123").await.unwrap();
        let rows: [(&str, &[u8]); 4] = [
            // No `label` at all: the original format.
            ("RSAK1", br#"{"v":1,"secret_key":"s1","username":"erin","created_at_ms":1}"#),
            // A free-text label from the label build, characters tags now refuse included.
            ("RSAK2", br#"{"v":1,"secret_key":"s2","username":"erin","label":"nas01-backup (old)","created_at_ms":2}"#),
            // A label that happened to contain commas.
            ("RSAK3", br#"{"v":1,"secret_key":"s3","username":"erin","label":"etl, prod ,,","created_at_ms":3}"#),
            ("RSAK4", br#"{"v":1,"secret_key":"s4","username":"erin","label":" , ","created_at_ms":4}"#),
        ];
        {
            let access = cf(&iam.db, CF_ACCESS_KEYS).unwrap();
            for (ak, value) in rows {
                iam.db.put_cf(&access, ak.as_bytes(), value).unwrap();
            }
        }
        iam.reload().await.unwrap();
        assert_eq!(iam.find_key("RSAK2").unwrap().0, "s2");
        let listed = iam.list_access_keys("erin").await.unwrap();
        let got: Vec<_> = listed.iter().map(|k| (k.access_key.as_str(), k.tags.clone())).collect();
        assert_eq!(
            got,
            [
                ("RSAK1", tags(&[DEFAULT_KEY_TAG])),
                ("RSAK2", tags(&["nas01-backup (old)"])),
                ("RSAK3", tags(&["etl", "prod"])),
                ("RSAK4", tags(&[DEFAULT_KEY_TAG])),
            ]
        );

        // What this build writes, the label build reads back as its label.
        iam.set_access_key_tags("RSAK1", &tags(&["etl", "prod"])).await.unwrap();
        let access = cf(&iam.db, CF_ACCESS_KEYS).unwrap();
        let stored: serde_json::Value =
            serde_json::from_slice(&iam.db.get_cf(&access, b"RSAK1").unwrap().unwrap()).unwrap();
        assert_eq!(stored["label"], "etl,prod");
        assert_eq!(stored["secret_key"], "s1");
    }

    #[tokio::test]
    async fn access_keys_resolve_to_owner_and_policy() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("bob", "password123").await.unwrap();
        let key = iam.create_access_key("bob", &["ci".to_string()]).await.unwrap();
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
            iam.create_access_key("carol", &["ci".to_string()]).await.unwrap()
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
        assert!(matches!(Group::named("operators").unwrap(), Group::Named(_)));
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
        assert!(is_authorized(&effective, &[Requirement::object("s3:GetObject", "b", "k")]));
        assert!(!is_authorized(&effective, &[Requirement::object("s3:DeleteObject", "b", "k")]));
    }

    #[tokio::test]
    async fn typed_admin_group_is_immutable_and_unrestricted() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("grace", "password123").await.unwrap();
        let resolved = iam.resolve_groups(&["AdMiN".to_string()]).await.unwrap();
        assert_eq!(resolved, vec![Group::Admin]);
        iam.set_user_groups("grace", &resolved).await.unwrap();

        assert!(iam.is_admin("grace"));
        let effective = iam.policy_for("grace").unwrap();
        assert!(is_authorized(&effective, &[Requirement::object("s3:DeleteObject", "any", "key")]));
        assert!(iam.set_group_policy(Group::Admin.name(), None).await.is_err());
        assert!(iam.delete_group(Group::Admin.name()).await.is_err());
    }

    #[tokio::test]
    async fn an_orphan_membership_row_never_comes_alive_when_its_group_is_created() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("ivy", "password123").await.unwrap();
        iam.create_user("ivan", "password123").await.unwrap();
        iam.create_group("keepers", None).await.unwrap();
        iam.set_user_groups("ivan", &[Group::named("keepers").unwrap()]).await.unwrap();

        // The front door is shut: a membership needs a group that exists, and a
        // refused call changes nothing.
        let ghost = [Group::named("keepers").unwrap(), Group::named("ghost").unwrap()];
        assert!(iam.set_user_groups("ivy", &ghost).await.is_err());
        assert!(iam.groups_for("ivy").is_empty());

        // An orphan row that got in some other way (a delete racing an
        // assignment, an import carrying memberships without their group).
        {
            let user_groups = cf(&iam.db, CF_USER_GROUPS).unwrap();
            iam.db.put_cf(&user_groups, membership_key("ivy", "ghost"), []).unwrap();
        }
        iam.reload().await.unwrap();
        assert!(iam.groups_for("ivy").is_empty());
        assert!(iam.policy_for("ivy").is_none());

        let everything: PolicyDocument = serde_json::from_str(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}"#,
        )
        .unwrap();
        iam.create_group("Ghost", Some(&everything)).await.unwrap();
        assert!(iam.groups_for("ivy").is_empty());
        assert!(iam.policy_for("ivy").is_none());
        let ghost_group = iam.list_groups().await.unwrap().into_iter().find(|g| g.group.name() == "Ghost").unwrap();
        assert_eq!(ghost_group.members, 0);
        // Creating it swept only that name's rows.
        assert_eq!(iam.groups_for("ivan"), ["keepers"]);
        // And a real membership in the new group works as ever.
        iam.set_user_groups("ivy", &[Group::named("ghost").unwrap()]).await.unwrap();
        assert_eq!(iam.groups_for("ivy"), ["Ghost"]);
        assert!(iam.policy_for("ivy").is_some());
    }

    #[tokio::test]
    async fn group_lifecycle_and_member_counts() {
        let (_tmp, iam) = open_tmp().await;
        iam.create_user("hank", "password123").await.unwrap();
        iam.create_group("Ops", None).await.unwrap();
        // Case-insensitive: creating a differently-cased duplicate fails.
        assert!(iam.create_group("ops", None).await.is_err());
        iam.set_user_groups("hank", &[Group::named("Ops").unwrap()]).await.unwrap();

        let groups = iam.list_groups().await.unwrap();
        // admin (system) sorts first.
        assert_eq!(groups[0].group, Group::Admin);
        let ops = groups.iter().find(|g| g.group.name() == "Ops").unwrap();
        assert_eq!(ops.members, 1);

        iam.delete_group("ops").await.unwrap();
        assert!(iam.resolve_groups(&["Ops".to_string()]).await.is_err());
        // Membership was cleared with the group.
        assert!(iam.groups_for("hank").is_empty());
    }

    #[tokio::test]
    async fn export_import_round_trips_and_honors_modes() {
        use crate::storage::rawdb::ImportMode;

        let src = tempfile::tempdir().unwrap();
        let iam = IamStore::open(src.path()).await.unwrap();
        iam.create_user("alice", "password123").await.unwrap();
        let key = iam.create_access_key("alice", &["ci".to_string()]).await.unwrap();
        iam.create_group("ops", None).await.unwrap();
        iam.set_user_groups("alice", &[Group::named("ops").unwrap()]).await.unwrap();
        let (webak, _) = iam.web_key_for("alice", false).await.unwrap();

        let dump = iam.export_raw().await.unwrap();
        assert!(!dump.is_empty());

        // Merge onto a fresh store reproduces the source exactly.
        let dst = tempfile::tempdir().unwrap();
        let restored = IamStore::open(dst.path()).await.unwrap();
        let report = restored.import_raw(dump.clone(), ImportMode::Merge).await.unwrap();
        assert_eq!(report.mode, "merge");
        assert_eq!(report.erased, 0);
        assert!(report.total >= 4);
        assert!(report.per_cf.iter().any(|(cf, _)| cf == "users"));
        assert!(restored.verify_password("alice", "password123").await.unwrap());
        assert_eq!(restored.find_key(&key.access_key).unwrap().1, "alice");
        assert_eq!(restored.groups_for("alice"), vec!["ops".to_string()]);
        assert_eq!(restored.find_web_key(&webak).map(|w| w.1), Some("alice".to_string()));

        // Replace onto a store holding a different user wipes it, then restores.
        let repl = tempfile::tempdir().unwrap();
        let target = IamStore::open(repl.path()).await.unwrap();
        target.create_user("bob", "password123").await.unwrap();
        let report = target.import_raw(dump, ImportMode::Replace).await.unwrap();
        assert_eq!(report.mode, "replace");
        assert!(report.erased >= 1, "replace erases existing rows first");
        assert!(!target.user_exists("bob"), "replace drops rows not in the dump");
        assert!(target.verify_password("alice", "password123").await.unwrap());

        // A non-dump blob is rejected, not silently ignored.
        assert!(target.import_raw(b"not a dump".to_vec(), ImportMode::Merge).await.is_err());

        // A truncated dump (tail sentinel damaged) is rejected as incomplete.
        let mut truncated = iam.export_raw().await.unwrap();
        truncated.pop();
        assert!(target.import_raw(truncated, ImportMode::Merge).await.is_err());
    }

    #[tokio::test]
    async fn newer_value_version_is_rejected() {
        let json = br#"{"v":2,"password_hash":"h","salt":"s","created_at_ms":1}"#;
        let user: UserV1 = from_slice(json).unwrap();
        assert!(reject_newer(user.v, "iam user").is_err());
        // Missing "v" is lenient (defaults to v1).
        let user: UserV1 = from_slice(br#"{"password_hash":"h"}"#).unwrap();
        assert_eq!(user.v, ENTITY_VERSION);
        assert!(reject_newer(user.v, "iam user").is_ok());
    }
}
