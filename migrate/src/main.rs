//! `rusts3-migrate` — converts a rusts3 data directory's metadata databases
//! between RocksDB (rusts3 <= 0.8) and redb (0.9 and later).
//!
//! ```text
//! rusts3-migrate status   <base_dir>   # what is in which format
//! rusts3-migrate migrate  <base_dir>   # RocksDB -> redb   (upgrading to 0.9+)
//! rusts3-migrate rollback <base_dir>   # redb -> RocksDB   (returning to <= 0.8)
//! ```
//!
//! `<base_dir>` is the server's `server.base_dir`. The server must be stopped:
//! the tool takes the same `.rusts3.lock` the server holds, so neither can run
//! while the other does.
//!
//! **What is converted.** `admin`, `key_usage`, `stats` and `scans` at the
//! root, and every `buckets/<bucket>/index`. Each RocksDB column family
//! becomes the redb table of the same name and back, every key and value
//! copied byte for byte (the server's `storage::kv` maps families to tables
//! one to one). The index's `counters` family holds merge operands in RocksDB;
//! they are read merged, so redb gets plain totals, and rollback writes those
//! totals back as base values the older server reads unchanged.
//!
//! **Crash safety, per database.** The copy is built under a temporary name,
//! compared row by row against the source, and only then renamed into place;
//! after that the source is renamed aside (`*.rocksdb.migrated`,
//! `*.redb.rolledback`), never deleted. Interrupted at any point, a re-run
//! either redoes the copy or — when both formats are present and identical —
//! just finishes the rename. Both present and *different* is never resolved
//! automatically.
//!
//! **Rollback** copies the *current* redb data, including everything written
//! since the migration; the `.migrated` copy from before is left untouched.
//!
//! **Version bounds** (checked for every database before any is converted,
//! so a refusal never leaves a data directory half done):
//!
//! - `migrate` reads data written by any RocksDB release, 0.3.0 – 0.8.x. They
//!   share bucket-index schema 2 and differ only in which families exist
//!   (0.8 added `tombstones`, `versions`, `version_tombstones`); rows are
//!   copied verbatim, so the new server sees exactly what 0.8 would have in an
//!   in-place upgrade. A family no release had, or another index schema,
//!   means a build this tool does not know: refused.
//! - `rollback` produces data for **0.8.x** only (older releases cannot open
//!   the families 0.8 added). Every redb database carries a format marker
//!   (table `rusts3_format`, stamped by the server and by `migrate`);
//!   rollback requires format 1 — the rows of 0.8, unchanged — and also
//!   refuses a table 0.8 does not know or an index schema other than 2. Once a
//!   later version has changed the format, rolling back is no longer safe.

use std::collections::BTreeSet;
use std::fmt;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition, TableHandle};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, MergeOperands, Options, WriteBatch, WriteOptions, DB};

/// The databases at the root of a data directory.
const ROOT_DATABASES: [&str; 4] = ["admin", "key_usage", "stats", "scans"];
/// The family whose values are merge operands in RocksDB (the bucket index's
/// object counter).
const MERGE_FAMILY: &str = "counters";
/// The bucket-index schema of every RocksDB release (0.3.0 – 0.8.x), and of
/// the redb data this tool can roll back. Stored as an i64 (LE) under
/// `schema_version` in the index's `meta` family.
const INDEX_SCHEMA: i64 = 2;
const META_FAMILY: &str = "meta";
const SCHEMA_KEY: &[u8] = b"schema_version";

/// The families each database has in rusts3 0.8 — a superset of every
/// earlier release's, and exactly what rollback may produce.
fn known_families(kind: &str) -> Option<&'static [&'static str]> {
    Some(match kind {
        "admin" => &[
            "users", "groups", "access_keys", "web_keys", "user_groups",
            "notify_subscriptions", "notify_state", "replication_links",
        ],
        "key_usage" => &["usage"],
        "stats" => &["samples"],
        "scans" => &["reports", "findings"],
        "index" => &[
            "objects", "intents", "counters", "meta", "changelog",
            "tombstones", "versions", "version_tombstones",
        ],
        _ => return None,
    })
}

/// The server's format marker (`storage::kv`): a table of its own, never a
/// RocksDB family. Format 1 = the rows of rusts3 0.8 in redb.
const FORMAT_TABLE: &str = "rusts3_format";
const FORMAT: u32 = 1;

fn stamp_format(db: &redb::Database) -> Result<()> {
    let mut txn = db.begin_write()?;
    txn.set_durability(redb::Durability::Immediate)?;
    {
        let mut t = txn.open_table(table(FORMAT_TABLE))?;
        t.insert(&b"format"[..], FORMAT.to_le_bytes().as_slice())?;
        let by = format!("rusts3-migrate {} (from RocksDB)", env!("CARGO_PKG_VERSION"));
        t.insert(&b"created_by"[..], by.as_bytes())?;
    }
    txn.commit()?;
    Ok(())
}

/// The format a redb database is marked with; `None` when unmarked (only
/// builds before markers existed, which wrote format 1).
fn read_format(db: &redb::Database) -> Result<Option<u32>> {
    let txn = db.begin_read()?;
    let t = match txn.open_table(table(FORMAT_TABLE)) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let Some(v) = t.get(&b"format"[..])? else { return Ok(None) };
    match <[u8; 4]>::try_from(v.value()) {
        Ok(bytes) => Ok(Some(u32::from_le_bytes(bytes))),
        Err(_) => fail("unreadable format marker"),
    }
}

/// Rows per redb write transaction / RocksDB write batch.
const BATCH_ROWS: usize = 10_000;
const REDB_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

macro_rules! from_error {
    ($($t:ty),*) => {$(
        impl From<$t> for Error {
            fn from(value: $t) -> Self {
                Error(value.to_string())
            }
        }
    )*};
}
from_error!(
    std::io::Error,
    rocksdb::Error,
    redb::Error,
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::SetDurabilityError
);

type Result<T> = std::result::Result<T, Error>;

fn fail<T>(message: impl Into<String>) -> Result<T> {
    Err(Error(message.into()))
}

fn table(name: &str) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
    TableDefinition::new(name)
}

// ─── the databases of a data directory ──────────────────────────────────────

/// One logical database: `<base>.rocksdb` (a directory) and/or `<base>.redb`.
#[derive(Debug, Clone)]
struct Database {
    label: String,
    base: PathBuf,
}

impl Database {
    fn rocks(&self) -> PathBuf {
        self.with_suffix("rocksdb")
    }
    fn redb(&self) -> PathBuf {
        self.with_suffix("redb")
    }
    fn with_suffix(&self, suffix: &str) -> PathBuf {
        let mut name = self.base.file_name().expect("database base has a name").to_os_string();
        name.push(".");
        name.push(suffix);
        self.base.with_file_name(name)
    }
    /// `admin`, `key_usage`, `stats`, `scans` or `index`.
    fn kind(&self) -> String {
        self.base.file_name().unwrap().to_string_lossy().into_owned()
    }
    fn dir(&self) -> &Path {
        self.base.parent().expect("database base has a parent")
    }
    fn state(&self) -> State {
        match (self.rocks().is_dir(), self.redb().is_file()) {
            (false, false) => State::Absent,
            (true, false) => State::RocksDb,
            (false, true) => State::Redb,
            (true, true) => State::Both,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Absent,
    RocksDb,
    Redb,
    Both,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            State::Absent => "absent",
            State::RocksDb => "RocksDB (<= 0.8)",
            State::Redb => "redb (0.9+)",
            State::Both => "BOTH (interrupted conversion — re-run migrate or rollback)",
        })
    }
}

fn discover(root: &Path) -> Result<Vec<Database>> {
    let mut found: Vec<Database> = ROOT_DATABASES
        .iter()
        .map(|name| Database { label: name.to_string(), base: root.join(name) })
        .collect();
    let buckets = root.join("buckets");
    if buckets.is_dir() {
        let mut names: Vec<String> = std::fs::read_dir(&buckets)?
            .flatten()
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        for name in names {
            found.push(Database { label: format!("buckets/{name}"), base: buckets.join(&name).join("index") });
        }
    }
    found.retain(|db| db.state() != State::Absent);
    Ok(found)
}

/// Holds `<root>/.rusts3.lock` exclusively — the lock the server takes.
fn lock(root: &Path) -> Result<std::fs::File> {
    if !root.is_dir() {
        return fail(format!("{} is not a directory", root.display()));
    }
    let path = root.join(".rusts3.lock");
    let file = OpenOptions::new().create(true).write(true).open(&path)?;
    if let Err(err) = file.try_lock() {
        return fail(format!(
            "{} is locked ({err}): stop the rusts3 server using this data directory first",
            path.display()
        ));
    }
    Ok(file)
}

// ─── engines ────────────────────────────────────────────────────────────────

/// The server's (<= 0.8) object-counter merge: a sum of little-endian i64s,
/// anything malformed counting as 0.
fn counter_merge(_key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let decode = |b: &[u8]| -> i64 { b.try_into().map(i64::from_le_bytes).unwrap_or(0) };
    let mut sum = existing.map(decode).unwrap_or(0);
    for op in operands.iter() {
        sum += decode(op);
    }
    Some(sum.to_le_bytes().to_vec())
}

fn family_options(name: &str) -> Options {
    let mut opts = Options::default();
    if name == MERGE_FAMILY {
        opts.set_merge_operator_associative("i64_sum", counter_merge);
    }
    opts
}

fn open_rocks(path: &Path, families: &[String]) -> Result<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = families.iter().map(|name| ColumnFamilyDescriptor::new(name, family_options(name)));
    Ok(DB::open_cf_descriptors(&opts, path, cfs)?)
}

fn rocks_families(path: &Path) -> Result<Vec<String>> {
    Ok(DB::list_cf(&Options::default(), path)?)
}

fn redb_tables(db: &redb::Database) -> Result<Vec<String>> {
    let txn = db.begin_read()?;
    let names = txn.list_tables()?.map(|t| t.name().to_string()).collect();
    Ok(names)
}

/// RocksDB's `default` family exists in every database; it is carried only
/// when it holds rows (the server never uses it).
fn rocks_family_is_empty(rocks: &DB, name: &str) -> Result<bool> {
    let cf = rocks.cf_handle(name).ok_or_else(|| Error(format!("missing family {name}")))?;
    Ok(rocks.iterator_cf(cf, IteratorMode::Start).next().transpose()?.is_none())
}

/// Every row of `rocks` equals the row of `redb` at the same position, table
/// by table, and neither has a non-empty family/table the other lacks.
/// Returns the row count.
fn verify(rocks: &DB, families: &[String], redb: &redb::Database) -> Result<u64> {
    let txn = redb.begin_read()?;
    let mut rows = 0u64;
    let mut compared = BTreeSet::new();
    for name in families {
        let cf = rocks.cf_handle(name).ok_or_else(|| Error(format!("missing family {name}")))?;
        let mut a = rocks.iterator_cf(cf, IteratorMode::Start);
        let t = match txn.open_table(table(name)) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                if a.next().is_some() {
                    return fail(format!("verification failed: table {name} is missing from the redb copy"));
                }
                continue;
            }
            Err(err) => return Err(err.into()),
        };
        compared.insert(name.clone());
        let mut b = t.range::<&[u8]>(..)?;
        loop {
            match (a.next(), b.next()) {
                (None, None) => break,
                (Some(x), Some(y)) => {
                    let (ka, va) = x?;
                    let (kb, vb) = y?;
                    if *ka != *kb.value() || *va != *vb.value() {
                        return fail(format!(
                            "verification failed: {name} differs at key {:?}",
                            String::from_utf8_lossy(&ka)
                        ));
                    }
                    rows += 1;
                }
                _ => return fail(format!("verification failed: {name} has a different number of rows")),
            }
        }
    }
    for t in txn.list_tables()? {
        let name = t.name().to_string();
        if name == FORMAT_TABLE {
            continue;
        }
        if !compared.contains(&name) && !txn.open_table(table(&name))?.is_empty()? {
            return fail(format!("verification failed: table {name} has no RocksDB counterpart"));
        }
    }
    Ok(rows)
}

/// Copies the RocksDB database at `src` into a new redb file at `dst`.
fn rocks_to_redb(src: &Path, dst: &Path) -> Result<u64> {
    let families = rocks_families(src)?;
    let rocks = open_rocks(src, &families)?;
    let db = redb::Database::builder().set_cache_size(REDB_CACHE_BYTES).create(dst)?;
    for name in &families {
        if name == "default" && rocks_family_is_empty(&rocks, name)? {
            continue;
        }
        let cf = rocks.cf_handle(name).ok_or_else(|| Error(format!("missing family {name}")))?;
        let mut rows = rocks.iterator_cf(cf, IteratorMode::Start);
        loop {
            let mut txn = db.begin_write()?;
            txn.set_durability(redb::Durability::None)?;
            let mut done = false;
            {
                let mut t = txn.open_table(table(name))?;
                for _ in 0..BATCH_ROWS {
                    match rows.next() {
                        Some(row) => {
                            let (k, v) = row?;
                            t.insert(&*k, &*v)?;
                        }
                        None => {
                            done = true;
                            break;
                        }
                    }
                }
            }
            txn.commit()?;
            if done {
                break;
            }
        }
    }
    stamp_format(&db)?; // durable commit: everything above is durable too
    verify(&rocks, &families, &db)
}

/// Copies the redb database at `src` into a new RocksDB database at `dst`.
fn redb_to_rocks(src: &Path, dst: &Path) -> Result<u64> {
    let db = redb::Database::builder().set_cache_size(REDB_CACHE_BYTES).open(src)?;
    let mut families: Vec<String> = redb_tables(&db)?.into_iter().filter(|t| t != FORMAT_TABLE).collect();
    if !families.iter().any(|f| f == "default") {
        families.push("default".to_string());
    }
    let rocks = open_rocks(dst, &families)?;
    {
        let txn = db.begin_read()?;
        let mut unsynced = WriteOptions::default();
        unsynced.set_sync(false);
        for name in &families {
            let t = match txn.open_table(table(name)) {
                Ok(t) => t,
                Err(redb::TableError::TableDoesNotExist(_)) => continue,
                Err(err) => return Err(err.into()),
            };
            let cf = rocks.cf_handle(name).ok_or_else(|| Error(format!("missing family {name}")))?;
            let mut batch = WriteBatch::default();
            for row in t.range::<&[u8]>(..)? {
                let (k, v) = row?;
                batch.put_cf(cf, k.value(), v.value());
                if batch.len() >= BATCH_ROWS {
                    rocks.write_opt(std::mem::take(&mut batch), &unsynced)?;
                }
            }
            rocks.write_opt(batch, &unsynced)?;
        }
    }
    rocks.flush_wal(true)?;
    for name in &families {
        if let Some(cf) = rocks.cf_handle(name) {
            rocks.flush_cf(cf)?;
        }
    }
    verify(&rocks, &families, &db)
}

/// Verifies that the RocksDB and redb copies hold the same rows (used when an
/// interrupted run left both).
fn same_contents(db: &Database) -> Result<u64> {
    let families = rocks_families(&db.rocks())?;
    let rocks = open_rocks(&db.rocks(), &families)?;
    let redb = redb::Database::builder().set_cache_size(REDB_CACHE_BYTES).open(db.redb())?;
    verify(&rocks, &families, &redb)
}

// ─── version bounds ─────────────────────────────────────────────────────────

/// Checks one database's families and (for an index) schema against what
/// the target version understands. `families` lists what the source holds;
/// `schema` reads the index schema from it.
fn check_shape(db: &Database, families: &[String], schema: Option<i64>, direction: Direction) -> Result<()> {
    let kind = db.kind();
    let Some(known) = known_families(&kind) else {
        return fail(format!("{}: unknown database kind {kind:?}", db.label));
    };
    let unknown: Vec<&String> =
        families
            .iter()
            .filter(|f| f.as_str() != "default" && f.as_str() != FORMAT_TABLE && !known.contains(&f.as_str()))
            .collect();
    let (who, why) = match direction {
        Direction::Migrate => ("rusts3 0.3.0 – 0.8.x", "it was written by a version this tool does not know"),
        Direction::Rollback => ("rusts3 0.8.x", "it uses a format 0.8 cannot read, so rolling back is not safe"),
    };
    if !unknown.is_empty() {
        return fail(format!(
            "{}: has {unknown:?}, which {who} never had — {why}",
            db.label
        ));
    }
    if kind == "index" {
        if let Some(v) = schema.filter(|v| *v != INDEX_SCHEMA) {
            return fail(format!(
                "{}: index schema {v}, but {who} use schema {INDEX_SCHEMA} — {why}",
                db.label
            ));
        }
    }
    Ok(())
}

fn decode_schema(bytes: &[u8]) -> i64 {
    bytes.try_into().map(i64::from_le_bytes).unwrap_or(-1)
}

/// The source side of `db` for `direction`, checked against the bounds.
fn preflight(db: &Database, direction: Direction) -> Result<()> {
    let source_present = match direction {
        Direction::Migrate => db.rocks().is_dir(),
        Direction::Rollback => db.redb().is_file(),
    };
    if !source_present {
        return Ok(()); // nothing to convert (already done, or absent)
    }
    match direction {
        Direction::Migrate => {
            let families = rocks_families(&db.rocks())?;
            let rocks = open_rocks(&db.rocks(), &families)?;
            let schema = match rocks.cf_handle(META_FAMILY) {
                Some(meta) => rocks.get_cf(meta, SCHEMA_KEY)?.map(|b| decode_schema(&b)),
                None => None,
            };
            check_shape(db, &families, schema, direction)
        }
        Direction::Rollback => {
            let redb = redb::Database::builder().set_cache_size(REDB_CACHE_BYTES).open(db.redb())?;
            if let Some(format) = read_format(&redb)?.filter(|f| *f != FORMAT) {
                return fail(format!(
                    "{}: format {format}, but rusts3 0.8.x reads format {FORMAT} only — \
                     a later version changed what it stores, so rolling back is not safe",
                    db.label
                ));
            }
            let families = redb_tables(&redb)?;
            let txn = redb.begin_read()?;
            let schema = match txn.open_table(table(META_FAMILY)) {
                Ok(meta) => meta.get(SCHEMA_KEY)?.map(|v| decode_schema(v.value())),
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(err) => return Err(err.into()),
            };
            check_shape(db, &families, schema, direction)
        }
    }
}

// ─── filesystem steps ───────────────────────────────────────────────────────

fn fsync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

fn remove_any(path: &Path) -> Result<()> {
    if path.is_dir() {
        std::fs::remove_dir_all(path)?;
    } else if path.exists() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

/// Renames `path` to `<path>.<suffix>` (or `<path>.<suffix>-<unix secs>` when
/// that is taken) and returns the new name.
fn set_aside(path: &Path, suffix: &str) -> Result<PathBuf> {
    let mut name = path.file_name().unwrap().to_os_string();
    name.push(format!(".{suffix}"));
    let mut target = path.with_file_name(&name);
    if target.exists() {
        let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        name.push(format!("-{secs}"));
        target = path.with_file_name(name);
    }
    std::fs::rename(path, &target)?;
    Ok(target)
}

// ─── the two directions ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Migrate,
    Rollback,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Converted(u64),
    /// An interrupted run had already built the copy; it was verified and the
    /// source set aside.
    Finished(u64),
    AlreadyDone,
}

fn convert(db: &Database, direction: Direction) -> Result<Outcome> {
    let (source, target, from_state, to_state) = match direction {
        Direction::Migrate => (db.rocks(), db.redb(), State::RocksDb, State::Redb),
        Direction::Rollback => (db.redb(), db.rocks(), State::Redb, State::RocksDb),
    };
    let aside = match direction {
        Direction::Migrate => "migrated",
        Direction::Rollback => "rolledback",
    };
    match db.state() {
        State::Absent => Ok(Outcome::AlreadyDone),
        s if s == to_state => Ok(Outcome::AlreadyDone),
        State::Both => {
            // Only a run of this tool creates the second format, and only
            // after verifying it, and the server refuses to start while a
            // RocksDB copy exists — so identical copies mean an interrupted
            // commit. Anything else needs a human.
            let rows = same_contents(db).map_err(|err| {
                Error(format!(
                    "{}: both {} and {} exist and they differ ({err}); leaving both untouched",
                    db.label,
                    db.rocks().display(),
                    db.redb().display()
                ))
            })?;
            set_aside(&source, aside)?;
            fsync_dir(db.dir())?;
            Ok(Outcome::Finished(rows))
        }
        s => {
            debug_assert_eq!(s, from_state);
            let mut tmp_name = target.file_name().unwrap().to_os_string();
            tmp_name.push(".tmp");
            let tmp = target.with_file_name(tmp_name);
            remove_any(&tmp)?;
            let rows = match direction {
                Direction::Migrate => rocks_to_redb(&source, &tmp),
                Direction::Rollback => redb_to_rocks(&source, &tmp),
            };
            let rows = match rows {
                Ok(rows) => rows,
                Err(err) => {
                    let _ = remove_any(&tmp);
                    return Err(Error(format!("{}: {err}", db.label)));
                }
            };
            // Commit point: the verified copy takes the live name…
            std::fs::rename(&tmp, &target)?;
            fsync_dir(db.dir())?;
            // …then the source steps aside (kept, never deleted).
            set_aside(&source, aside)?;
            fsync_dir(db.dir())?;
            Ok(Outcome::Converted(rows))
        }
    }
}

fn run(command: &str, root: &Path) -> Result<()> {
    let direction = match command {
        "status" => None,
        "migrate" => Some(Direction::Migrate),
        "rollback" => Some(Direction::Rollback),
        other => return fail(format!("unknown command {other:?}\n\n{USAGE}")),
    };
    let _lock = lock(root)?;
    let databases = discover(root)?;
    if databases.is_empty() {
        println!("no rusts3 databases under {}", root.display());
        return Ok(());
    }
    let width = databases.iter().map(|d| d.label.len()).max().unwrap_or(0);
    let Some(direction) = direction else {
        for db in &databases {
            println!("{:width$}  {}", db.label, db.state());
        }
        return Ok(());
    };
    let (verb, arrow) = match direction {
        Direction::Migrate => ("migrate", "RocksDB → redb"),
        Direction::Rollback => ("rollback", "redb → RocksDB"),
    };
    // Every database is checked before any is converted.
    let refused: Vec<String> =
        databases.iter().filter_map(|db| preflight(db, direction).err().map(|e| e.to_string())).collect();
    if !refused.is_empty() {
        return fail(format!("nothing was converted:\n  {}", refused.join("\n  ")));
    }
    println!("{verb} {} ({arrow})", root.display());
    let mut converted = 0;
    for db in &databases {
        let started = Instant::now();
        match convert(db, direction)? {
            Outcome::Converted(rows) => {
                converted += 1;
                println!("  {:width$}  {rows} rows, verified ({:.1}s)", db.label, started.elapsed().as_secs_f64());
            }
            Outcome::Finished(rows) => {
                converted += 1;
                println!("  {:width$}  {rows} rows, verified — finished an interrupted run", db.label);
            }
            Outcome::AlreadyDone => println!("  {:width$}  already done", db.label),
        }
    }
    let kept = match direction {
        Direction::Migrate => "*.rocksdb.migrated",
        Direction::Rollback => "*.redb.rolledback",
    };
    println!("done: {converted} converted. The originals are kept as {kept}; delete them once you are satisfied.");
    Ok(())
}

const USAGE: &str = "\
usage: rusts3-migrate <command> <base_dir>

commands:
  status    show which databases are in which format
  migrate   convert RocksDB databases to redb, for rusts3 0.9+
            (reads data from any RocksDB release, 0.3.0 – 0.8.x)
  rollback  convert redb databases back to RocksDB, for rusts3 0.8.x
            (refused if the data uses anything 0.8 cannot read)

<base_dir> is the server's server.base_dir. Stop the server first.";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (command, root) = match args.as_slice() {
        [command, root] => (command.as_str(), PathBuf::from(root)),
        [flag] if flag == "-h" || flag == "--help" => {
            println!("{USAGE}");
            return;
        }
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    if let Err(err) = run(command, &root) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(rocks: &DB, cf: &str, k: &[u8], v: &[u8]) {
        rocks.put_cf(rocks.cf_handle(cf).unwrap(), k, v).unwrap();
    }

    /// A 0.8-shaped data directory: admin with two families, a bucket index
    /// whose counter is stored as unmerged merge operands, an empty stats db.
    fn legacy_root() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        {
            let admin = open_rocks(&root.path().join("admin.rocksdb"), &["users".into(), "groups".into()]).unwrap();
            put(&admin, "users", b"alice", br#"{"v":1}"#);
            put(&admin, "users", b"bob", br#"{"v":1}"#);
            put(&admin, "groups", b"admins", b"{}");
        }
        std::fs::create_dir_all(root.path().join("buckets/photos")).unwrap();
        {
            let index = open_rocks(
                &root.path().join("buckets/photos/index.rocksdb"),
                &["objects".into(), MERGE_FAMILY.into(), "changelog".into(), "meta".into()],
            )
            .unwrap();
            for i in 0..25_000u32 {
                put(&index, "objects", format!("img/{i:06}.jpg").as_bytes(), &i.to_le_bytes());
            }
            let counters = index.cf_handle(MERGE_FAMILY).unwrap();
            for delta in [10i64, 15, -3] {
                index.merge_cf(counters, b"object_count", delta.to_le_bytes()).unwrap();
            }
            put(&index, "changelog", &7u64.to_be_bytes(), b"put img/1");
            put(&index, "meta", SCHEMA_KEY, &INDEX_SCHEMA.to_le_bytes());
        }
        drop(open_rocks(&root.path().join("stats.rocksdb"), &["samples".into()]).unwrap());
        root
    }

    fn redb_get(path: &Path, t: &str, k: &[u8]) -> Option<Vec<u8>> {
        let db = redb::Database::open(path).unwrap();
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(table(t)).unwrap();
        table.get(k).unwrap().map(|v| v.value().to_vec())
    }

    fn redb_len(path: &Path, t: &str) -> u64 {
        let db = redb::Database::open(path).unwrap();
        let txn = db.begin_read().unwrap();
        txn.open_table(table(t)).unwrap().len().unwrap()
    }

    #[test]
    fn migrate_then_rollback_round_trips_every_row() {
        let root = legacy_root();
        let r = root.path();
        run("migrate", r).unwrap();

        let index = r.join("buckets/photos/index.redb");
        assert!(r.join("admin.redb").is_file() && index.is_file() && r.join("stats.redb").is_file());
        assert!(!r.join("admin.rocksdb").exists(), "source set aside");
        assert!(r.join("admin.rocksdb.migrated").is_dir(), "source kept");
        assert_eq!(redb_get(&r.join("admin.redb"), "users", b"alice").unwrap(), br#"{"v":1}"#);
        assert_eq!(redb_len(&index, "objects"), 25_000);
        assert_eq!(redb_get(&index, MERGE_FAMILY, b"object_count").unwrap(), 22i64.to_le_bytes(), "merged total");
        assert_eq!(redb_get(&index, FORMAT_TABLE, b"format").unwrap(), FORMAT.to_le_bytes(), "format marker stamped");

        // Writes made after migrating (as the 0.9 server would) must survive
        // a rollback.
        {
            let db = redb::Database::open(r.join("admin.redb")).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(table("users")).unwrap().insert(&b"carol"[..], &b"{}"[..]).unwrap();
            txn.commit().unwrap();
        }
        run("rollback", r).unwrap();
        assert!(r.join("admin.redb.rolledback").is_file() && !r.join("admin.redb").exists());
        let families = rocks_families(&r.join("admin.rocksdb")).unwrap();
        let admin = open_rocks(&r.join("admin.rocksdb"), &families).unwrap();
        let users = admin.cf_handle("users").unwrap();
        assert!(admin.get_cf(users, b"carol").unwrap().is_some(), "post-migration write carried back");
        assert!(admin.get_cf(users, b"alice").unwrap().is_some());
        drop(admin);
        let idx = open_rocks(&r.join("buckets/photos/index.rocksdb"), &rocks_families(&r.join("buckets/photos/index.rocksdb")).unwrap()).unwrap();
        // An older server adds operands to the rolled-back base value.
        idx.merge_cf(idx.cf_handle(MERGE_FAMILY).unwrap(), b"object_count", 1i64.to_le_bytes()).unwrap();
        assert_eq!(idx.get_cf(idx.cf_handle(MERGE_FAMILY).unwrap(), b"object_count").unwrap().unwrap(), 23i64.to_le_bytes());
    }

    #[test]
    fn a_second_run_is_a_no_op() {
        let root = legacy_root();
        run("migrate", root.path()).unwrap();
        let db = Database { label: "admin".into(), base: root.path().join("admin") };
        assert_eq!(convert(&db, Direction::Migrate).unwrap(), Outcome::AlreadyDone);
        run("migrate", root.path()).unwrap();
    }

    #[test]
    fn an_interrupted_commit_is_finished_and_a_conflict_is_left_alone() {
        let root = legacy_root();
        let r = root.path();
        run("migrate", r).unwrap();
        let db = Database { label: "admin".into(), base: r.join("admin") };
        // Crash between "copy renamed into place" and "source set aside".
        std::fs::rename(r.join("admin.rocksdb.migrated"), r.join("admin.rocksdb")).unwrap();
        assert_eq!(db.state(), State::Both);
        assert_eq!(convert(&db, Direction::Migrate).unwrap(), Outcome::Finished(3));
        assert_eq!(db.state(), State::Redb);

        // Both present but different: refused, nothing touched.
        std::fs::rename(r.join("admin.rocksdb.migrated"), r.join("admin.rocksdb")).unwrap();
        {
            let families = rocks_families(&r.join("admin.rocksdb")).unwrap();
            let admin = open_rocks(&r.join("admin.rocksdb"), &families).unwrap();
            put(&admin, "users", b"mallory", b"{}");
        }
        let err = convert(&db, Direction::Migrate).unwrap_err().to_string();
        assert!(err.contains("differ"), "{err}");
        assert_eq!(db.state(), State::Both);
    }

    #[test]
    fn data_from_an_unknown_version_is_refused_before_anything_converts() {
        let root = legacy_root();
        let r = root.path();
        // An index from a build with another schema.
        {
            let path = r.join("buckets/photos/index.rocksdb");
            let index = open_rocks(&path, &rocks_families(&path).unwrap()).unwrap();
            put(&index, "meta", SCHEMA_KEY, &3i64.to_le_bytes());
        }
        let err = run("migrate", r).unwrap_err().to_string();
        assert!(err.contains("nothing was converted") && err.contains("index schema 3"), "{err}");
        assert!(r.join("admin.rocksdb").is_dir() && !r.join("admin.redb").exists(), "admin untouched too");

        // A family no release had.
        let root = legacy_root();
        drop(open_rocks(&root.path().join("scans.rocksdb"), &["reports".into(), "from_the_future".into()]).unwrap());
        let err = run("migrate", root.path()).unwrap_err().to_string();
        assert!(err.contains("from_the_future"), "{err}");
    }

    #[test]
    fn rollback_is_refused_once_the_data_has_moved_past_0_8() {
        let root = legacy_root();
        let r = root.path();
        run("migrate", r).unwrap();
        // A later version adds a table 0.8 does not know.
        {
            let db = redb::Database::open(r.join("admin.redb")).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(table("new_feature")).unwrap().insert(&b"k"[..], &b"v"[..]).unwrap();
            txn.commit().unwrap();
        }
        let err = run("rollback", r).unwrap_err().to_string();
        assert!(err.contains("new_feature") && err.contains("0.8 cannot read"), "{err}");
        assert!(r.join("admin.redb").is_file() && !r.join("admin.rocksdb").exists(), "nothing rolled back");
    }

    #[test]
    fn rollback_requires_format_1() {
        let root = legacy_root();
        let r = root.path();
        run("migrate", r).unwrap();
        // A later release bumped the format of one database (same tables).
        {
            let db = redb::Database::open(r.join("stats.redb")).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(table(FORMAT_TABLE)).unwrap().insert(&b"format"[..], 2u32.to_le_bytes().as_slice()).unwrap();
            txn.commit().unwrap();
        }
        let err = run("rollback", r).unwrap_err().to_string();
        assert!(err.contains("stats: format 2") && err.contains("not safe"), "{err}");
        assert!(r.join("admin.redb").is_file() && !r.join("admin.rocksdb").exists(), "nothing rolled back");
        // The marker never becomes a RocksDB family.
        let root = legacy_root();
        run("migrate", root.path()).unwrap();
        run("rollback", root.path()).unwrap();
        assert!(!rocks_families(&root.path().join("admin.rocksdb")).unwrap().contains(&FORMAT_TABLE.to_string()));
    }

    #[test]
    fn a_running_server_blocks_the_tool() {
        let root = legacy_root();
        let held = OpenOptions::new().create(true).write(true).open(root.path().join(".rusts3.lock")).unwrap();
        held.lock().unwrap();
        let err = run("migrate", root.path()).unwrap_err().to_string();
        assert!(err.contains("stop the rusts3 server"), "{err}");
        assert!(root.path().join("admin.rocksdb").is_dir(), "nothing converted");
    }

    #[test]
    fn a_failed_copy_leaves_the_source_and_no_temp() {
        let root = legacy_root();
        let r = root.path();
        // A corrupt RocksDB directory cannot be opened.
        std::fs::create_dir_all(r.join("scans.rocksdb")).unwrap();
        std::fs::write(r.join("scans.rocksdb/CURRENT"), b"garbage").unwrap();
        let db = Database { label: "scans".into(), base: r.join("scans") };
        assert!(convert(&db, Direction::Migrate).is_err());
        assert!(r.join("scans.rocksdb").is_dir());
        assert!(!r.join("scans.redb").exists() && !r.join("scans.redb.tmp").exists());
    }
}
