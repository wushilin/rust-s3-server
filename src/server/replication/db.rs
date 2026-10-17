//! Replication links in `admin.rocksdb`.
//!
//! | family                | key     | value              |
//! |-----------------------|---------|--------------------|
//! | `replication_links`   | link id | [`Link`] JSON      |
//!
//! Positions are not here: each link's checkpoint lives in its local
//! bucket's index (see `ObjectIndex::get_checkpoint`).

use std::sync::Arc;

use rocksdb::{IteratorMode, WriteOptions};

use super::model::{Link, RECORD_VERSION};
use crate::server::iam::AdminDb;
use crate::storage::errors::{Result, StorageError};

pub const CF_LINKS: &str = "replication_links";

#[derive(Clone)]
pub struct LinkDb {
    db: Arc<AdminDb>,
}

fn cf<'a>(db: &'a AdminDb) -> Result<Arc<rocksdb::BoundColumnFamily<'a>>> {
    db.cf_handle(CF_LINKS)
        .ok_or_else(|| StorageError::Db(format!("missing column family {CF_LINKS}")))
}

async fn blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(err) => Err(StorageError::Db(format!("replication task panicked: {err}"))),
    }
}

impl LinkDb {
    pub fn new(db: Arc<AdminDb>) -> Self {
        Self { db }
    }

    pub async fn load_all(&self) -> Result<Vec<Link>> {
        let db = self.db.clone();
        blocking(move || {
            let mut out = Vec::new();
            for item in db.iterator_cf(&cf(&db)?, IteratorMode::Start) {
                let (_, value) = item?;
                let link: Link = serde_json::from_slice(&value)?;
                if link.v > RECORD_VERSION {
                    return Err(StorageError::Db(format!(
                        "replication link {} is v{}, newer than this build understands",
                        link.id, link.v
                    )));
                }
                out.push(link);
            }
            Ok(out)
        })
        .await
    }

    pub async fn put(&self, link: &Link) -> Result<()> {
        let db = self.db.clone();
        let key = link.id.clone();
        let value = serde_json::to_vec(link)?;
        blocking(move || {
            let mut opts = WriteOptions::default();
            opts.set_sync(true);
            db.put_cf_opt(&cf(&db)?, key.as_bytes(), value, &opts)?;
            Ok(())
        })
        .await
    }

    pub async fn delete(&self, id: &str) -> Result<()> {
        let db = self.db.clone();
        let key = id.to_string();
        blocking(move || {
            let mut opts = WriteOptions::default();
            opts.set_sync(true);
            db.delete_cf_opt(&cf(&db)?, key.as_bytes(), &opts)?;
            Ok(())
        })
        .await
    }
}
