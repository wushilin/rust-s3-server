//! Subscription persistence in `admin.rocksdb`.
//!
//! | family                  | key             | value                    |
//! |-------------------------|-----------------|--------------------------|
//! | `notify_subscriptions`  | subscription id | [`Subscription`] JSON    |
//! | `notify_state`          | subscription id | [`SubState`] JSON        |
//!
//! Configuration is written with a synced WAL; progress is not — losing the
//! last offset update to a power cut only means a batch is delivered again,
//! which consumers already de-duplicate by `seq`.

use std::sync::Arc;

use rocksdb::{IteratorMode, WriteBatch, WriteOptions};

use super::model::{SubState, Subscription, RECORD_VERSION};
use crate::server::iam::AdminDb;
use crate::storage::errors::{Result, StorageError};

pub const CF_SUBSCRIPTIONS: &str = "notify_subscriptions";
pub const CF_STATE: &str = "notify_state";

#[derive(Clone)]
pub struct NotifyDb {
    db: Arc<AdminDb>,
}

fn cf<'a>(db: &'a AdminDb, name: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'a>>> {
    db.cf_handle(name)
        .ok_or_else(|| StorageError::Db(format!("missing column family {name}")))
}

fn synced() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(true);
    opts
}

fn check_version(v: u32, what: &str) -> Result<()> {
    if v > RECORD_VERSION {
        return Err(StorageError::Db(format!(
            "{what} record is v{v}, newer than this build understands (v{RECORD_VERSION})"
        )));
    }
    Ok(())
}

async fn blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(err) => Err(StorageError::Db(format!("notify task panicked: {err}"))),
    }
}

impl NotifyDb {
    pub fn new(db: Arc<AdminDb>) -> Self {
        Self { db }
    }

    /// Every subscription with its state (a missing state row reads as the
    /// default).
    pub async fn load_all(&self) -> Result<Vec<(Subscription, SubState)>> {
        let db = self.db.clone();
        blocking(move || {
            let subs = cf(&db, CF_SUBSCRIPTIONS)?;
            let states = cf(&db, CF_STATE)?;
            let mut out = Vec::new();
            for item in db.iterator_cf(&subs, IteratorMode::Start) {
                let (key, value) = item?;
                let sub: Subscription = serde_json::from_slice(&value)?;
                check_version(sub.v, "subscription")?;
                let state = match db.get_cf(&states, &key)? {
                    Some(bytes) => {
                        let state: SubState = serde_json::from_slice(&bytes)?;
                        check_version(state.v, "subscription state")?;
                        state
                    }
                    None => SubState::default(),
                };
                out.push((sub, state));
            }
            Ok(out)
        })
        .await
    }

    /// Writes a subscription and its state together (create / reconfigure).
    pub async fn put(&self, sub: &Subscription, state: &SubState) -> Result<()> {
        let db = self.db.clone();
        let key = sub.id.clone();
        let sub = serde_json::to_vec(sub)?;
        let state = serde_json::to_vec(state)?;
        blocking(move || {
            let mut batch = WriteBatch::default();
            batch.put_cf(&cf(&db, CF_SUBSCRIPTIONS)?, key.as_bytes(), sub);
            batch.put_cf(&cf(&db, CF_STATE)?, key.as_bytes(), state);
            db.write_opt(batch, &synced())?;
            Ok(())
        })
        .await
    }

    /// Records delivery progress. `sync` for operator actions, which must
    /// not silently roll back.
    pub async fn put_state(&self, id: &str, state: &SubState, sync: bool) -> Result<()> {
        let db = self.db.clone();
        let key = id.to_string();
        let state = serde_json::to_vec(state)?;
        blocking(move || {
            let opts = if sync { synced() } else { WriteOptions::default() };
            db.put_cf_opt(&cf(&db, CF_STATE)?, key.as_bytes(), state, &opts)?;
            Ok(())
        })
        .await
    }

    pub async fn delete(&self, id: &str) -> Result<()> {
        let db = self.db.clone();
        let key = id.to_string();
        blocking(move || {
            let mut batch = WriteBatch::default();
            batch.delete_cf(&cf(&db, CF_SUBSCRIPTIONS)?, key.as_bytes());
            batch.delete_cf(&cf(&db, CF_STATE)?, key.as_bytes());
            db.write_opt(batch, &synced())?;
            Ok(())
        })
        .await
    }
}
