//! The embedded key/value store behind every metadata database: a thin layer
//! over [redb] (pure Rust) exposing exactly the subset of the RocksDB API this
//! codebase was written against — column families, write batches with an
//! associative merge operator, forward/reverse iterators, a seekable raw
//! iterator, range deletes, snapshots.
//!
//! **On-disk shape.** One redb file per database (`admin.redb`,
//! `buckets/<b>/index.redb`, …). Each column family is a redb table of the
//! same name with `&[u8]` keys and values, ordered bytewise — the same order
//! RocksDB's default comparator gives, so key encodings carry over unchanged
//! and the `rusts3-migrate` tool copies rows verbatim in both directions.
//!
//! **Durability: group commit.** Every write commits its own transaction
//! with `Durability::None` (no fsync) and gets the next *commit generation*.
//! One fsync — an empty `Durability::Immediate` commit, which in redb persists
//! every commit before it — makes a whole range of generations durable at
//! once, advancing the *durable generation*:
//!
//! - A `sync = true` write then waits until the durable generation covers it.
//!   If no fsync is in flight, that writer becomes the committer: it fsyncs on
//!   behalf of everyone committed so far, publishes the new durable
//!   generation, and wakes every waiter it covered (`Condvar::notify_all`).
//!   Writers arriving meanwhile are covered by the next fsync, as one group.
//!   Event driven, no polling; an acknowledged write is always durable.
//! - A `sync = false` write returns at once; the next fsync — at the latest
//!   [`MAX_UNSYNCED`] later (background flusher), at [`Db::flush`], or on
//!   drop — covers it. A crash can lose up to that long of such writes.
//!
//! A write is *visible* to readers before it is durable (by at most one
//! fsync). [`DurableClock`] lets a caller hold back what it publishes until
//! it is durable — the changelog watermark does, so a changelog consumer never
//! sees an entry a crash could take back — and [`Db::on_durable`] says when.
//!
//! **Format marker.** Every database carries a [`FORMAT_TABLE`] recording
//! the format of what it stores ([`FORMAT`]) and which build created and last
//! opened it. A database whose format is newer than this build knows is
//! refused instead of misread; a release that changes what any database
//! stores bumps [`FORMAT`]. `rusts3-migrate` stamps the same marker, and rolls
//! back only data still at format 1 (exactly what 0.8 stored).
//!
//! **Concurrency.** Readers never block. Writers are serialized per database
//! (redb has one write transaction at a time); every call here is a complete
//! transaction, so nothing holds the writer across calls.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// Longest an unsynced (`sync = false`) commit stays non-durable.
pub const MAX_UNSYNCED: Duration = Duration::from_secs(1);

/// File-name suffix of a database. RocksDB databases (directories) used
/// `.rocksdb`; see [`legacy_databases`].
pub const SUFFIX: &str = "redb";

/// The table holding a database's format marker. Not a column family: no
/// caller sees it, and it is not part of a `rawdb` export.
pub const FORMAT_TABLE: &str = "rusts3_format";
/// The format this build reads and writes. 1 = the rows of rusts3 0.8, moved
/// from RocksDB to redb unchanged. Bump it whenever a release changes what a
/// database stores (and teach `rusts3-migrate` the new bound).
pub const FORMAT: u32 = 1;
const KEY_FORMAT: &[u8] = b"format";
const KEY_CREATED_BY: &[u8] = b"created_by";
const KEY_OPENED_BY: &[u8] = b"opened_by";

fn this_build() -> String {
    format!("rusts3 {}", env!("CARGO_PKG_VERSION"))
}

/// Checks, and stamps, the format marker of a database being opened.
fn check_and_stamp_format(txn: &redb::WriteTransaction, path: &Path) -> Result<()> {
    let mut marker = txn.open_table(table(FORMAT_TABLE))?;
    let stored = marker.get(KEY_FORMAT)?.map(|v| v.value().to_vec());
    match stored {
        None => {
            // A new database (or one from a build before markers existed,
            // which only ever wrote format 1).
            marker.insert(KEY_FORMAT, FORMAT.to_le_bytes().as_slice())?;
            if marker.get(KEY_CREATED_BY)?.is_none() {
                marker.insert(KEY_CREATED_BY, this_build().as_bytes())?;
            }
        }
        Some(bytes) => {
            let format = <[u8; 4]>::try_from(bytes.as_slice()).map(u32::from_le_bytes).map_err(|_| {
                Error(format!("{}: unreadable format marker", path.display()))
            })?;
            if format > FORMAT {
                let created_by = marker
                    .get(KEY_CREATED_BY)?
                    .map(|v| String::from_utf8_lossy(v.value()).into_owned())
                    .unwrap_or_else(|| "an unknown build".into());
                return Err(Error(format!(
                    "{} has format {format} (created by {created_by}), newer than this build ({}) \
                     understands (format {FORMAT}); run a newer rusts3",
                    path.display(),
                    this_build()
                )));
            }
        }
    }
    marker.insert(KEY_OPENED_BY, this_build().as_bytes())?;
    Ok(())
}

/// Default page cache per database. redb's own default (1 GiB) is meant for
/// one database per process; this server opens one per bucket.
const DEFAULT_CACHE_BYTES: usize = 16 * 1024 * 1024;

// ─── errors ─────────────────────────────────────────────────────────────────

/// Any failure of the store, flattened to its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

macro_rules! from_redb {
    ($($t:ty),*) => {$(
        impl From<$t> for Error {
            fn from(value: $t) -> Self {
                Error(value.to_string())
            }
        }
    )*};
}
from_redb!(
    redb::Error,
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::SetDurabilityError
);

pub type Result<T> = std::result::Result<T, Error>;

// ─── options & descriptors (RocksDB-shaped) ─────────────────────────────────

/// Operands handed to a merge operator.
pub struct MergeOperands<'a>(&'a [Vec<u8>]);

impl<'a> MergeOperands<'a> {
    pub fn iter(&self) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.0.iter().map(Vec::as_slice)
    }
}

/// An associative full-merge function: `(key, existing, operands) -> new`.
/// `None` deletes the key.
pub type MergeFn = fn(&[u8], Option<&[u8]>, &MergeOperands) -> Option<Vec<u8>>;

/// Database and per-family options. Only what this codebase sets.
#[derive(Clone, Default)]
pub struct Options {
    merge: Option<MergeFn>,
    cache_bytes: Option<usize>,
}

impl Options {
    /// Accepted for source compatibility: databases are always created.
    pub fn create_if_missing(&mut self, _: bool) {}
    /// Accepted for source compatibility: every listed family is created.
    pub fn create_missing_column_families(&mut self, _: bool) {}
    /// Registers the family's merge operator (the name is informational).
    pub fn set_merge_operator_associative(&mut self, _name: &str, merge: MergeFn) {
        self.merge = Some(merge);
    }
    /// Page cache for the whole database (database-level options only).
    pub fn set_cache_size(&mut self, bytes: usize) {
        self.cache_bytes = Some(bytes);
    }
}

pub struct ColumnFamilyDescriptor {
    name: String,
    options: Options,
}

impl ColumnFamilyDescriptor {
    pub fn new(name: impl Into<String>, options: Options) -> Self {
        Self { name: name.into(), options }
    }
}

/// A handle to one column family (table). Cheap; the lifetime ties it to its
/// database the way RocksDB's did, so call sites read the same.
pub struct BoundColumnFamily<'a> {
    name: Arc<str>,
    _db: PhantomData<&'a Db>,
}

impl BoundColumnFamily<'_> {
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// What a column-family argument may be: a handle, or the `Arc` that
/// [`Db::cf_handle`] returns (as with RocksDB's `AsColumnFamilyRef`).
pub trait AsColumnFamilyRef {
    fn cf_name(&self) -> &Arc<str>;
}

impl AsColumnFamilyRef for BoundColumnFamily<'_> {
    fn cf_name(&self) -> &Arc<str> {
        &self.name
    }
}

impl<T: AsColumnFamilyRef> AsColumnFamilyRef for Arc<T> {
    fn cf_name(&self) -> &Arc<str> {
        (**self).cf_name()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WriteOptions {
    sync: bool,
}

impl WriteOptions {
    pub fn set_sync(&mut self, sync: bool) {
        self.sync = sync;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Reverse,
}

#[derive(Clone, Copy, Debug)]
pub enum IteratorMode<'a> {
    Start,
    End,
    From(&'a [u8], Direction),
}

fn table(name: &str) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
    TableDefinition::new(name)
}

// ─── write batch ────────────────────────────────────────────────────────────

enum Op {
    Put(Arc<str>, Vec<u8>, Vec<u8>),
    Delete(Arc<str>, Vec<u8>),
    Merge(Arc<str>, Vec<u8>, Vec<u8>),
    DeleteRange(Arc<str>, Vec<u8>, Vec<u8>),
}

/// Operations applied atomically, in order, by [`Db::write_opt`].
#[derive(Default)]
pub struct WriteBatch {
    ops: Vec<Op>,
}

impl WriteBatch {
    pub fn put_cf(&mut self, cf: &impl AsColumnFamilyRef, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.ops.push(Op::Put(cf.cf_name().clone(), key.as_ref().to_vec(), value.as_ref().to_vec()));
    }
    pub fn delete_cf(&mut self, cf: &impl AsColumnFamilyRef, key: impl AsRef<[u8]>) {
        self.ops.push(Op::Delete(cf.cf_name().clone(), key.as_ref().to_vec()));
    }
    pub fn merge_cf(&mut self, cf: &impl AsColumnFamilyRef, key: impl AsRef<[u8]>, operand: impl AsRef<[u8]>) {
        self.ops.push(Op::Merge(cf.cf_name().clone(), key.as_ref().to_vec(), operand.as_ref().to_vec()));
    }
    /// Deletes every key in `[from, to)`.
    pub fn delete_range_cf(&mut self, cf: &impl AsColumnFamilyRef, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) {
        self.ops.push(Op::DeleteRange(cf.cf_name().clone(), from.as_ref().to_vec(), to.as_ref().to_vec()));
    }
    pub fn len(&self) -> usize {
        self.ops.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

// ─── the database ───────────────────────────────────────────────────────────

struct Inner {
    /// `None` once the [`Db`] is dropped. The slot (rather than the handle
    /// living as long as the `Arc`) makes closing deterministic: the
    /// background flusher holds `Arc<Inner>` for a moment while it flushes,
    /// and must never keep a file open past its `Db` — a caller that drops a
    /// database and reopens it at once would find it still open.
    db: RwLock<Option<redb::Database>>,
    path: PathBuf,
    families: HashMap<Arc<str>, Option<MergeFn>>,
    /// Generation of the newest commit (each write's transaction gets one).
    committed: AtomicU64,
    /// Generation every commit up to which is durable, and whether an fsync
    /// is in flight; `synced` wakes the writers waiting on it.
    sync: Mutex<SyncState>,
    synced: Condvar,
    /// Mirror of `SyncState::durable` for lock-free reads.
    durable: AtomicU64,
    last_durable: Mutex<Instant>,
    /// Durable fsyncs performed (one per group).
    fsyncs: AtomicU64,
    /// Writes that have started committing / finished (committed or not).
    /// The committer waits for every write already started before it takes
    /// redb's writer — which it then holds through the fsync, so a write
    /// that missed this group could not commit until the fsync is over.
    applies_started: AtomicU64,
    applies_finished: Mutex<u64>,
    applied: Condvar,
    /// Called (outside any lock) whenever the durable generation advances.
    listeners: Mutex<Vec<Arc<dyn Fn() + Send + Sync>>>,
}

#[derive(Default)]
struct SyncState {
    durable: u64,
    syncing: bool,
}

impl Inner {
    /// Runs `f` on the open database (an error once it is closed).
    fn with<T>(&self, f: impl FnOnce(&redb::Database) -> Result<T>) -> Result<T> {
        match self.db.read().unwrap().as_ref() {
            Some(db) => f(db),
            None => Err(Error(format!("{} is closed", self.path.display()))),
        }
    }

    fn has_unsynced(&self) -> bool {
        self.committed.load(Ordering::Acquire) > self.durable.load(Ordering::Acquire)
    }

    /// Returns once every commit up to generation `target` is durable —
    /// performing the fsync itself when none is in flight (then for every
    /// commit so far, not just `target`), else waiting for the one that is.
    fn sync_to(&self, target: u64) -> Result<()> {
        let mut state = self.sync.lock().unwrap();
        loop {
            if state.durable >= target {
                return Ok(());
            }
            if state.syncing {
                state = self.synced.wait(state).unwrap();
                continue;
            }
            // This thread is the committer for the next group.
            state.syncing = true;
            drop(state);
            // Let every write already on its way commit first, so they ride
            // this fsync (event driven: each finishing write signals).
            // Bounded by the writes started *now*: a steady stream of new
            // ones cannot postpone the fsync.
            let started = self.applies_started.load(Ordering::Acquire);
            {
                let mut finished = self.applies_finished.lock().unwrap();
                while *finished < started {
                    finished = self.applied.wait(finished).unwrap();
                }
            }
            let outcome = self.with(|db| {
                let mut txn = db.begin_write()?;
                // Holding the writer: every generation assigned so far
                // belongs to a finished commit this fsync will cover. (A
                // commit that finished but has not yet taken its number is
                // covered too, and simply claimed by the next group.)
                let covered = self.committed.load(Ordering::Acquire);
                txn.set_durability(redb::Durability::Immediate)?;
                txn.commit()?;
                Ok(covered)
            });
            state = self.sync.lock().unwrap();
            state.syncing = false;
            let advanced = match &outcome {
                Ok(covered) if *covered > state.durable => {
                    state.durable = *covered;
                    self.durable.store(*covered, Ordering::Release);
                    self.fsyncs.fetch_add(1, Ordering::Relaxed);
                    *self.last_durable.lock().unwrap() = Instant::now();
                    true
                }
                _ => false,
            };
            // Wake everyone: those covered return; on a failed fsync the next
            // waiter becomes the committer and tries again.
            self.synced.notify_all();
            if advanced {
                drop(state);
                let listeners: Vec<_> = self.listeners.lock().unwrap().clone();
                for listener in listeners {
                    listener();
                }
                state = self.sync.lock().unwrap();
            }
            outcome?;
        }
    }

    /// Makes every commit so far durable.
    fn make_durable(&self) -> Result<()> {
        self.sync_to(self.committed.load(Ordering::Acquire))
    }
}

/// Read-only view of a database's commit and durable generations, held by
/// whatever must not publish a write before it is durable. Holds the database
/// weakly: once it is closed (which makes everything durable), every
/// generation counts as durable.
#[derive(Clone)]
pub struct DurableClock(Weak<Inner>);

impl DurableClock {
    /// The open database's state, or `None` once it is closed — judged by
    /// the file, not by whether `Inner` is still allocated (the background
    /// flusher may hold it a moment longer).
    fn open_inner(&self) -> Option<Arc<Inner>> {
        self.0.upgrade().filter(|inner| inner.db.read().unwrap().is_some())
    }
    /// The newest commit's generation: a write that has returned is covered.
    pub fn committed(&self) -> u64 {
        self.open_inner().map(|i| i.committed.load(Ordering::Acquire)).unwrap_or(u64::MAX)
    }
    /// Every commit up to this generation is durable.
    pub fn durable(&self) -> u64 {
        self.open_inner().map(|i| i.durable.load(Ordering::Acquire)).unwrap_or(u64::MAX)
    }
}

impl std::fmt::Debug for DurableClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableClock").field("durable", &self.durable()).finish()
    }
}

/// One open database. `Send + Sync`; share it behind an `Arc`. Dropping the
/// last handle makes everything durable and closes the file.
pub struct Db {
    inner: Arc<Inner>,
}

impl Drop for Db {
    fn drop(&mut self) {
        if let Err(err) = self.inner.make_durable() {
            log::error!("kv: final flush of {} failed: {err}", self.inner.path.display());
        }
        // Waits out a background flush in progress, then closes.
        drop(self.inner.db.write().unwrap().take());
    }
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").field("path", &self.inner.path).finish_non_exhaustive()
    }
}

/// Databases with unsynced commits, watched by the background flusher.
fn registry() -> &'static Mutex<Vec<Weak<Inner>>> {
    static REGISTRY: OnceLock<Mutex<Vec<Weak<Inner>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        std::thread::Builder::new()
            .name("kv-flusher".into())
            .spawn(|| loop {
                std::thread::sleep(MAX_UNSYNCED / 4);
                let live: Vec<Arc<Inner>> = {
                    let mut list = registry().lock().unwrap();
                    list.retain(|weak| weak.strong_count() > 0);
                    list.iter().filter_map(Weak::upgrade).collect()
                };
                for inner in live {
                    let due = inner.has_unsynced()
                        && inner.last_durable.lock().unwrap().elapsed() >= MAX_UNSYNCED;
                    if due {
                        if let Err(err) = inner.make_durable() {
                            log::error!("kv: background flush of {} failed: {err}", inner.path.display());
                        }
                    }
                }
            })
            .expect("spawn kv flusher");
        Mutex::new(Vec::new())
    })
}

impl Db {
    /// Opens (creating if absent) the database file at `path`, creating any
    /// listed family that does not exist yet.
    pub fn open_cf_descriptors(
        opts: &Options,
        path: impl AsRef<Path>,
        cfs: impl IntoIterator<Item = ColumnFamilyDescriptor>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.is_dir() {
            return Err(Error(format!(
                "{} is a directory (a RocksDB database?) — migrate it with rusts3-migrate",
                path.display()
            )));
        }
        let db = redb::Database::builder()
            .set_cache_size(opts.cache_bytes.unwrap_or(DEFAULT_CACHE_BYTES))
            .create(&path)?;
        let families: HashMap<Arc<str>, Option<MergeFn>> =
            cfs.into_iter().map(|cf| (Arc::from(cf.name.as_str()), cf.options.merge)).collect();
        if families.contains_key(FORMAT_TABLE) {
            return Err(Error(format!("{FORMAT_TABLE} is reserved")));
        }
        let mut txn = db.begin_write()?;
        txn.set_durability(redb::Durability::Immediate)?;
        check_and_stamp_format(&txn, &path)?;
        for name in families.keys() {
            txn.open_table(table(name))?;
        }
        txn.commit()?;
        let inner = Arc::new(Inner {
            db: RwLock::new(Some(db)),
            path,
            families,
            committed: AtomicU64::new(0),
            sync: Mutex::new(SyncState::default()),
            synced: Condvar::new(),
            durable: AtomicU64::new(0),
            last_durable: Mutex::new(Instant::now()),
            fsyncs: AtomicU64::new(0),
            applies_started: AtomicU64::new(0),
            applies_finished: Mutex::new(0),
            applied: Condvar::new(),
            listeners: Mutex::new(Vec::new()),
        });
        registry().lock().unwrap().push(Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// This database's commit/durable generations (see [`DurableClock`]).
    pub fn clock(&self) -> DurableClock {
        DurableClock(Arc::downgrade(&self.inner))
    }

    /// Registers `f` to run whenever the durable generation advances (from
    /// whichever thread performed the fsync; keep it short). It must not hold
    /// this `Db` (that would keep it open forever).
    pub fn on_durable(&self, f: Arc<dyn Fn() + Send + Sync>) {
        self.inner.listeners.lock().unwrap().push(f);
    }

    /// Durable fsyncs performed so far — one per group of commits.
    pub fn fsync_count(&self) -> u64 {
        self.inner.fsyncs.load(Ordering::Relaxed)
    }

    pub fn cf_handle(&self, name: &str) -> Option<Arc<BoundColumnFamily<'_>>> {
        self.inner
            .families
            .get_key_value(name)
            .map(|(name, _)| Arc::new(BoundColumnFamily { name: name.clone(), _db: PhantomData }))
    }

    // ── reads ──

    pub fn get_cf(&self, cf: &impl AsColumnFamilyRef, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let txn = self.inner.with(|db| Ok(db.begin_read()?))?;
        let table = match txn.open_table(table(cf.cf_name())) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        Ok(table.get(key.as_ref())?.map(|v| v.value().to_vec()))
    }

    pub fn iterator_cf<'a>(&'a self, cf: &impl AsColumnFamilyRef, mode: IteratorMode) -> DbIterator<'a> {
        Snapshot::of(self).and_then(|s| s.iter(cf.cf_name(), mode)).unwrap_or_else(DbIterator::failed)
    }

    pub fn raw_iterator_cf<'a>(&'a self, cf: &impl AsColumnFamilyRef) -> DBRawIterator<'a> {
        DBRawIterator::new(Snapshot::of(self), cf.cf_name())
    }

    /// A consistent point-in-time view for several reads.
    pub fn snapshot(&self) -> Snapshot<'_> {
        Snapshot::of(self).unwrap_or_else(|err| Snapshot { txn: Err(err), _db: PhantomData })
    }

    // ── writes ──

    pub fn put_cf(&self, cf: &impl AsColumnFamilyRef, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        self.put_cf_opt(cf, key, value, &WriteOptions::default())
    }

    pub fn put_cf_opt(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        opts: &WriteOptions,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, key, value);
        self.write_opt(batch, opts)
    }

    pub fn delete_cf(&self, cf: &impl AsColumnFamilyRef, key: impl AsRef<[u8]>) -> Result<()> {
        self.delete_cf_opt(cf, key, &WriteOptions::default())
    }

    pub fn delete_cf_opt(&self, cf: &impl AsColumnFamilyRef, key: impl AsRef<[u8]>, opts: &WriteOptions) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.delete_cf(cf, key);
        self.write_opt(batch, opts)
    }

    pub fn delete_range_cf(&self, cf: &impl AsColumnFamilyRef, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.delete_range_cf(cf, from, to);
        self.write(batch)
    }

    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        self.write_opt(batch, &WriteOptions::default())
    }

    /// Applies `batch` atomically, in order.
    pub fn write_opt(&self, batch: WriteBatch, opts: &WriteOptions) -> Result<()> {
        let inner = &self.inner;
        inner.applies_started.fetch_add(1, Ordering::AcqRel);
        let applied = inner.with(|db| self.apply(db, batch, opts));
        *inner.applies_finished.lock().unwrap() += 1;
        inner.applied.notify_all();
        let generation = applied?;
        if opts.sync {
            inner.sync_to(generation)?;
        }
        Ok(())
    }

    /// Commits `batch` (not yet durable) and returns its generation.
    fn apply(&self, db: &redb::Database, batch: WriteBatch, opts: &WriteOptions) -> Result<u64> {
        let inner = &self.inner;
        let mut txn = db.begin_write()?;
        // Never fsynced here: durability is the group commit's job.
        txn.set_durability(redb::Durability::None)?;
        for op in batch.ops {
            match op {
                Op::Put(cf, key, value) => {
                    txn.open_table(table(&cf))?.insert(key.as_slice(), value.as_slice())?;
                }
                Op::Delete(cf, key) => {
                    txn.open_table(table(&cf))?.remove(key.as_slice())?;
                }
                Op::Merge(cf, key, operand) => {
                    let merge = inner
                        .families
                        .get(&cf)
                        .copied()
                        .flatten()
                        .ok_or_else(|| Error(format!("column family {cf} has no merge operator")))?;
                    let mut t = txn.open_table(table(&cf))?;
                    let existing = t.get(key.as_slice())?.map(|v| v.value().to_vec());
                    let operands = [operand];
                    match merge(&key, existing.as_deref(), &MergeOperands(&operands)) {
                        Some(value) => {
                            t.insert(key.as_slice(), value.as_slice())?;
                        }
                        None => {
                            t.remove(key.as_slice())?;
                        }
                    }
                }
                Op::DeleteRange(cf, from, to) => {
                    if from < to {
                        txn.open_table(table(&cf))?
                            .retain_in::<&[u8], _>(from.as_slice()..to.as_slice(), |_, _| false)?;
                    }
                }
            }
        }
        txn.commit()?;
        let generation = inner.committed.fetch_add(1, Ordering::AcqRel) + 1;
        let _ = opts; // the caller waits (or not) after the writer is released
        Ok(generation)
    }

    /// Makes every commit so far durable.
    pub fn flush(&self) -> Result<()> {
        self.inner.make_durable()
    }

    /// Same as [`flush`](Self::flush): durability is per database.
    pub fn flush_cf(&self, _cf: &impl AsColumnFamilyRef) -> Result<()> {
        self.flush()
    }

    /// Same as [`flush`](Self::flush): there is no separate log.
    pub fn flush_wal(&self, _sync: bool) -> Result<()> {
        self.flush()
    }
}

// ─── reading: snapshots and iterators ───────────────────────────────────────

/// A read transaction: every read through it sees the same state.
pub struct Snapshot<'a> {
    txn: Result<redb::ReadTransaction>,
    _db: PhantomData<&'a Db>,
}

impl<'a> Snapshot<'a> {
    fn of(db: &'a Db) -> Result<Self> {
        Ok(Self { txn: Ok(db.inner.with(|db| Ok(db.begin_read()?))?), _db: PhantomData })
    }

    fn open(&self, name: &str) -> Result<Option<redb::ReadOnlyTable<&'static [u8], &'static [u8]>>> {
        let txn = self.txn.as_ref().map_err(Clone::clone)?;
        match txn.open_table(table(name)) {
            Ok(table) => Ok(Some(table)),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn iter(&self, name: &str, mode: IteratorMode) -> Result<DbIterator<'a>> {
        let Some(table) = self.open(name)? else {
            return Ok(DbIterator::empty());
        };
        let (range, reverse) = match mode {
            IteratorMode::Start => (table.range::<&[u8]>(..)?, false),
            IteratorMode::End => (table.range::<&[u8]>(..)?, true),
            IteratorMode::From(key, Direction::Forward) => (table.range::<&[u8]>(key..)?, false),
            IteratorMode::From(key, Direction::Reverse) => (table.range::<&[u8]>(..=key)?, true),
        };
        Ok(DbIterator { range: Some(range), reverse, error: None, _db: PhantomData })
    }

    pub fn iterator_cf(&self, cf: &impl AsColumnFamilyRef, mode: IteratorMode) -> DbIterator<'a> {
        self.iter(cf.cf_name(), mode).unwrap_or_else(DbIterator::failed)
    }

    pub fn get_cf(&self, cf: &impl AsColumnFamilyRef, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        match self.open(cf.cf_name())? {
            Some(table) => Ok(table.get(key.as_ref())?.map(|v| v.value().to_vec())),
            None => Ok(None),
        }
    }
}

type Range = redb::Range<'static, &'static [u8], &'static [u8]>;
type KvPair = (Box<[u8]>, Box<[u8]>);

fn owned(
    item: std::result::Result<
        (redb::AccessGuard<'static, &'static [u8]>, redb::AccessGuard<'static, &'static [u8]>),
        redb::StorageError,
    >,
) -> Result<KvPair> {
    let (k, v) = item?;
    Ok((Box::from(k.value()), Box::from(v.value())))
}

/// `(key, value)` pairs in key order (or reverse), like RocksDB's iterator.
pub struct DbIterator<'a> {
    range: Option<Range>,
    reverse: bool,
    error: Option<Error>,
    _db: PhantomData<&'a Db>,
}

impl DbIterator<'_> {
    fn empty() -> Self {
        Self { range: None, reverse: false, error: None, _db: PhantomData }
    }
    fn failed(err: Error) -> Self {
        Self { range: None, reverse: false, error: Some(err), _db: PhantomData }
    }
}

impl Iterator for DbIterator<'_> {
    type Item = Result<KvPair>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.error.take() {
            return Some(Err(err));
        }
        let range = self.range.as_mut()?;
        let item = if self.reverse { range.next_back() } else { range.next() };
        item.map(owned)
    }
}

/// A seekable cursor, like RocksDB's raw iterator: position with `seek*`,
/// step with `next`/`prev`, read with `key`/`value`, check `status`.
pub struct DBRawIterator<'a> {
    snapshot: Result<Snapshot<'a>>,
    table: Arc<str>,
    /// Forward range continuing after `current` (kept while stepping forward).
    forward: Option<Range>,
    current: Option<KvPair>,
    error: Option<Error>,
}

impl<'a> DBRawIterator<'a> {
    fn new(snapshot: Result<Snapshot<'a>>, table: &str) -> Self {
        Self { snapshot, table: Arc::from(table), forward: None, current: None, error: None }
    }

    fn with_table<T>(
        &mut self,
        f: impl FnOnce(&redb::ReadOnlyTable<&'static [u8], &'static [u8]>) -> Result<T>,
    ) -> Option<T> {
        let opened = match &self.snapshot {
            Ok(snapshot) => snapshot.open(&self.table),
            Err(err) => Err(err.clone()),
        };
        match opened.and_then(|table| match table {
            Some(table) => f(&table).map(Some),
            None => Ok(None),
        }) {
            Ok(value) => value,
            Err(err) => {
                self.error = Some(err);
                None
            }
        }
    }

    /// Starts a forward scan over `bounds` and positions on its first entry.
    fn start_forward(&mut self, lower: Bound<Vec<u8>>) {
        self.current = None;
        self.forward = None;
        let range = self.with_table(|table| {
            let range = match &lower {
                Bound::Unbounded => table.range::<&[u8]>(..)?,
                Bound::Included(k) => table.range::<&[u8]>(k.as_slice()..)?,
                Bound::Excluded(k) => {
                    table.range::<&[u8]>((Bound::Excluded(k.as_slice()), Bound::Unbounded))?
                }
            };
            Ok(range)
        });
        self.forward = range;
        self.step_forward();
    }

    fn step_forward(&mut self) {
        self.current = None;
        let Some(range) = self.forward.as_mut() else {
            return;
        };
        match range.next().map(owned) {
            Some(Ok(pair)) => self.current = Some(pair),
            Some(Err(err)) => {
                self.error = Some(err);
                self.forward = None;
            }
            None => self.forward = None,
        }
    }

    pub fn seek_to_first(&mut self) {
        self.start_forward(Bound::Unbounded);
    }

    pub fn seek(&mut self, key: impl AsRef<[u8]>) {
        self.start_forward(Bound::Included(key.as_ref().to_vec()));
    }

    pub fn seek_to_last(&mut self) {
        self.forward = None;
        self.current = self
            .with_table(|table| table.range::<&[u8]>(..)?.next_back().map(owned).transpose())
            .flatten();
    }

    pub fn next(&mut self) {
        match (&self.forward, &self.current) {
            (Some(_), _) => self.step_forward(),
            // Positioned by seek_to_last/prev: resume after the current key.
            (None, Some((key, _))) => {
                let key = key.to_vec();
                self.start_forward(Bound::Excluded(key));
            }
            (None, None) => {}
        }
    }

    pub fn prev(&mut self) {
        let Some((key, _)) = self.current.take() else {
            return;
        };
        self.forward = None;
        self.current = self
            .with_table(|table| table.range::<&[u8]>(..&*key)?.next_back().map(owned).transpose())
            .flatten();
    }

    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.current.as_ref().map(|(k, _)| &**k)
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.current.as_ref().map(|(_, v)| &**v)
    }

    pub fn status(&self) -> Result<()> {
        match &self.error {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}

// ─── files ──────────────────────────────────────────────────────────────────

/// Deletes a database file, if present.
pub fn remove(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// RocksDB databases (`*.rocksdb` directories) under a data root: the four at
/// the root and every bucket's index. This build cannot read them — the server
/// refuses to start until `rusts3-migrate` has converted them, rather than
/// silently starting empty (IAM) or rebuilding an index from the blobs (which
/// would discard its changelog).
pub fn legacy_databases(data_root: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = ["admin", "key_usage", "stats", "scans"]
        .iter()
        .map(|name| data_root.join(format!("{name}.rocksdb")))
        .filter(|path| path.is_dir())
        .collect();
    if let Ok(buckets) = std::fs::read_dir(data_root.join("buckets")) {
        for bucket in buckets.flatten() {
            let index = bucket.path().join("index.rocksdb");
            if index.is_dir() {
                found.push(index);
            }
        }
    }
    found.sort();
    found
}

/// The startup refusal for [`legacy_databases`], or `Ok` when there are none.
pub fn ensure_migrated(data_root: &Path) -> std::result::Result<(), String> {
    let legacy = legacy_databases(data_root);
    if legacy.is_empty() {
        return Ok(());
    }
    let shown: Vec<String> = legacy.iter().take(5).map(|p| format!("  {}", p.display())).collect();
    let more = if legacy.len() > 5 { format!("\n  … and {} more", legacy.len() - 5) } else { String::new() };
    Err(format!(
        "{} RocksDB database(s) under {} need converting before this version can start:\n{}{}\n\
         Stop the server and run:  rusts3-migrate migrate {}\n\
         (rusts3-migrate rollback converts back, for returning to an older version.)",
        legacy.len(),
        data_root.display(),
        shown.join("\n"),
        more,
        data_root.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sum(_k: &[u8], existing: Option<&[u8]>, ops: &MergeOperands) -> Option<Vec<u8>> {
        let dec = |b: &[u8]| b.try_into().map(i64::from_le_bytes).unwrap_or(0);
        let total = existing.map(dec).unwrap_or(0) + ops.iter().map(dec).sum::<i64>();
        Some(total.to_le_bytes().to_vec())
    }

    fn open(dir: &tempfile::TempDir) -> Db {
        let mut counters = Options::default();
        counters.set_merge_operator_associative("i64_sum", sum);
        Db::open_cf_descriptors(
            &Options::default(),
            dir.path().join("t.redb"),
            [ColumnFamilyDescriptor::new("a", Options::default()), ColumnFamilyDescriptor::new("c", counters)],
        )
        .unwrap()
    }

    fn keys(it: DbIterator) -> Vec<String> {
        it.map(|r| String::from_utf8(r.unwrap().0.into_vec()).unwrap()).collect()
    }

    #[test]
    fn put_get_delete_and_batches_apply_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        let a = db.cf_handle("a").unwrap();
        assert!(db.cf_handle("nope").is_none());
        db.put_cf(&a, b"k1", b"v1").unwrap();
        assert_eq!(db.get_cf(&a, b"k1").unwrap().as_deref(), Some(&b"v1"[..]));
        let mut batch = WriteBatch::default();
        batch.delete_cf(&a, b"k1");
        batch.put_cf(&a, b"k1", b"v2"); // later op wins
        batch.put_cf(&a, b"k2", b"x");
        db.write(batch).unwrap();
        assert_eq!(db.get_cf(&a, b"k1").unwrap().as_deref(), Some(&b"v2"[..]));
        db.delete_cf(&a, b"k2").unwrap();
        assert_eq!(db.get_cf(&a, b"k2").unwrap(), None);
    }

    #[test]
    fn merge_sums_counters_and_requires_an_operator() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        let (a, c) = (db.cf_handle("a").unwrap(), db.cf_handle("c").unwrap());
        for delta in [5i64, -2, 10] {
            let mut batch = WriteBatch::default();
            batch.merge_cf(&c, b"n", delta.to_le_bytes());
            db.write(batch).unwrap();
        }
        assert_eq!(db.get_cf(&c, b"n").unwrap().unwrap(), 13i64.to_le_bytes());
        let mut batch = WriteBatch::default();
        batch.put_cf(&a, b"untouched", b"1");
        batch.merge_cf(&a, b"x", b"1");
        assert!(db.write(batch).is_err());
        assert_eq!(db.get_cf(&a, b"untouched").unwrap(), None, "a failed batch applies nothing");
    }

    #[test]
    fn iterators_and_range_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        let a = db.cf_handle("a").unwrap();
        for k in ["b", "a", "d", "c", "e"] {
            db.put_cf(&a, k, k).unwrap();
        }
        assert_eq!(keys(db.iterator_cf(&a, IteratorMode::Start)), ["a", "b", "c", "d", "e"]);
        assert_eq!(keys(db.iterator_cf(&a, IteratorMode::End)), ["e", "d", "c", "b", "a"]);
        assert_eq!(keys(db.iterator_cf(&a, IteratorMode::From(b"bb", Direction::Forward))), ["c", "d", "e"]);
        assert_eq!(keys(db.iterator_cf(&a, IteratorMode::From(b"c", Direction::Reverse))), ["c", "b", "a"]);
        db.delete_range_cf(&a, b"b", b"d").unwrap();
        assert_eq!(keys(db.iterator_cf(&a, IteratorMode::Start)), ["a", "d", "e"]);
    }

    #[test]
    fn raw_iterator_seeks_and_steps() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        let a = db.cf_handle("a").unwrap();
        let mut it = db.raw_iterator_cf(&a);
        it.seek_to_first();
        assert!(!it.valid() && it.status().is_ok(), "empty table");
        for k in ["a", "c", "e", "g"] {
            db.put_cf(&a, k, k.to_uppercase()).unwrap();
        }
        let mut it = db.raw_iterator_cf(&a);
        it.seek(b"b");
        assert_eq!(it.key(), Some(&b"c"[..]));
        assert_eq!(it.value(), Some(&b"C"[..]));
        it.next();
        assert_eq!(it.key(), Some(&b"e"[..]));
        it.prev();
        assert_eq!(it.key(), Some(&b"c"[..]));
        it.next();
        assert_eq!(it.key(), Some(&b"e"[..]));
        it.seek_to_last();
        assert_eq!(it.key(), Some(&b"g"[..]));
        it.next();
        assert!(!it.valid());
        it.seek(b"zzz");
        assert!(!it.valid());
        it.seek_to_first();
        assert_eq!(it.key(), Some(&b"a"[..]));
        it.status().unwrap();
    }

    #[test]
    fn unsynced_writes_become_durable_on_flush_and_drop() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(&dir);
            let a = db.cf_handle("a").unwrap();
            db.put_cf(&a, b"soft", b"1").unwrap();
            assert!(db.inner.has_unsynced());
            db.flush().unwrap();
            assert!(!db.inner.has_unsynced());
            db.put_cf(&a, b"soft2", b"2").unwrap();
        } // dropped: final flush
        let db = open(&dir);
        let a = db.cf_handle("a").unwrap();
        assert!(db.get_cf(&a, b"soft").unwrap().is_some());
        assert!(db.get_cf(&a, b"soft2").unwrap().is_some());
    }

    #[test]
    fn dropping_closes_the_file_even_while_the_flusher_holds_it() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        db.put_cf(&db.cf_handle("a").unwrap(), b"k", b"v").unwrap();
        // What the background flusher holds for a moment while it works.
        let flusher_ref = db.inner.clone();
        drop(db);
        let reopened = open(&dir); // would fail "already open" if the file stayed open
        assert!(reopened.get_cf(&reopened.cf_handle("a").unwrap(), b"k").unwrap().is_some());
        assert!(flusher_ref.with(|_| Ok(())).is_err(), "a stale reference sees a closed database");
    }

    #[test]
    fn the_format_marker_is_stamped_and_a_newer_format_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        drop(open(&dir));
        let read = |key: &[u8]| {
            let db = redb::Database::open(dir.path().join("t.redb")).unwrap();
            let txn = db.begin_read().unwrap();
            let t = txn.open_table(table(FORMAT_TABLE)).unwrap();
            t.get(key).unwrap().map(|v| v.value().to_vec())
        };
        assert_eq!(read(KEY_FORMAT).unwrap(), FORMAT.to_le_bytes());
        assert_eq!(read(KEY_CREATED_BY).unwrap(), this_build().into_bytes());
        // A later release wrote format 2: refused, nothing opened.
        {
            let db = redb::Database::open(dir.path().join("t.redb")).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(table(FORMAT_TABLE)).unwrap().insert(KEY_FORMAT, (FORMAT + 1).to_le_bytes().as_slice()).unwrap();
            txn.open_table(table(FORMAT_TABLE)).unwrap().insert(KEY_CREATED_BY, &b"rusts3 9.9.9"[..]).unwrap();
            txn.commit().unwrap();
        }
        let err = Db::open_cf_descriptors(&Options::default(), dir.path().join("t.redb"), []).unwrap_err().to_string();
        assert!(err.contains("format 2") && err.contains("rusts3 9.9.9") && err.contains("newer"), "{err}");
        // The marker is not a family anyone can open or claim.
        let err = Db::open_cf_descriptors(
            &Options::default(),
            dir.path().join("u.redb"),
            [ColumnFamilyDescriptor::new(FORMAT_TABLE, Options::default())],
        )
        .unwrap_err();
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn concurrent_durable_writes_share_fsyncs_and_all_survive() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(open(&dir));
        let (threads, per_thread) = (16, 8);
        let mut sync = WriteOptions::default();
        sync.set_sync(true);
        std::thread::scope(|scope| {
            for t in 0..threads {
                let db = db.clone();
                scope.spawn(move || {
                    let a = db.cf_handle("a").unwrap();
                    for i in 0..per_thread {
                        db.put_cf_opt(&a, format!("t{t:02}-{i}"), b"v", &sync).unwrap();
                        // Acknowledged means durable.
                        assert!(db.inner.durable.load(Ordering::Acquire) > 0);
                    }
                });
            }
        });
        let writes = (threads * per_thread) as u64;
        let fsyncs = db.fsync_count();
        eprintln!("group commit: {fsyncs} fsyncs for {writes} durable writes");
        assert!(fsyncs < writes, "{fsyncs} fsyncs for {writes} durable writes: no grouping");
        assert!(!db.inner.has_unsynced());
        drop(db);
        let db = open(&dir);
        let a = db.cf_handle("a").unwrap();
        assert_eq!(db.iterator_cf(&a, IteratorMode::Start).count() as u64, writes);
    }

    #[test]
    fn the_clock_and_listeners_follow_the_durable_generation() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        let a = db.cf_handle("a").unwrap();
        let clock = db.clock();
        let calls = Arc::new(AtomicU64::new(0));
        let seen = calls.clone();
        db.on_durable(Arc::new(move || {
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        db.put_cf(&a, b"soft", b"1").unwrap(); // unsynced
        assert!(clock.committed() > clock.durable(), "visible, not yet durable");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        db.flush().unwrap();
        assert!(clock.durable() >= clock.committed());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "listener told of the advance");
        let mut sync = WriteOptions::default();
        sync.set_sync(true);
        db.put_cf_opt(&a, b"hard", b"2", &sync).unwrap();
        assert!(clock.durable() >= clock.committed(), "a synced write returns durable");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(db);
        assert_eq!(clock.durable(), u64::MAX, "a closed database is entirely durable");
    }

    #[test]
    fn snapshot_is_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir);
        let a = db.cf_handle("a").unwrap();
        db.put_cf(&a, b"x", b"1").unwrap();
        let snap = db.snapshot();
        db.put_cf(&a, b"y", b"2").unwrap();
        assert_eq!(keys(snap.iterator_cf(&a, IteratorMode::Start)), ["x"]);
        assert_eq!(snap.get_cf(&a, b"y").unwrap(), None);
    }

    #[test]
    fn legacy_databases_are_found_and_refused() {
        let dir = tempfile::tempdir().unwrap();
        assert!(ensure_migrated(dir.path()).is_ok());
        std::fs::create_dir_all(dir.path().join("admin.rocksdb")).unwrap();
        std::fs::create_dir_all(dir.path().join("buckets/photos/index.rocksdb")).unwrap();
        std::fs::create_dir_all(dir.path().join("buckets/docs")).unwrap();
        let found = legacy_databases(dir.path());
        assert_eq!(found.len(), 2);
        let message = ensure_migrated(dir.path()).unwrap_err();
        assert!(message.contains("rusts3-migrate migrate"), "{message}");
        // The engine itself refuses a RocksDB directory at a database path.
        assert!(Db::open_cf_descriptors(&Options::default(), dir.path().join("admin.rocksdb"), []).is_err());
    }
}
