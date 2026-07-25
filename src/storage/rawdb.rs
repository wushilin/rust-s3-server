//! Raw dump / restore for a RocksDB database.
//!
//! The dump is a fixed 6-byte magic header followed by a **length-delimited
//! protobuf stream** of [`Row`] messages — one per key/value pair, each
//! carrying its own column-family name:
//!
//! ```text
//! [ "RS3PB\x01" ]                          (6-byte magic header)
//! [ varint len ][ Row{ cf, key, value } ]
//! [ varint len ][ Row{ cf, key, value } ]
//! ...
//! [ 32-byte TRAILER ]                       (end-of-stream sentinel)
//! ```
//!
//! The trailer is a fixed 32-byte marker written only after the final row.
//! Import requires it to be present and exact, so a dump truncated mid-write
//! (missing or partial tail) is rejected rather than imported partially — a
//! cheap completeness guarantee.
//!
//! There is no SQL and no transformation: values are copied verbatim as opaque
//! bytes (they are already self-versioned JSON), so a dump is just the raw
//! `(cf, key, value)` triples. Import **upserts** every row (`put`) into the
//! named family — nothing is cleared — so restoring onto a fresh data directory
//! reproduces the source exactly, and restoring onto a populated one merges.
//!
//! Protobuf (rather than a hand-rolled binary framing) gives a standard,
//! tool-readable stream and a maintained parser for a schema that is fixed and
//! never evolves. Avro would be the equivalent alternative; it is heavier for a
//! flat two-field record and is not used here.

use std::io::{Read, Write};

use prost::Message;
use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded, ReadOptions, WriteBatch, WriteOptions};

use super::errors::{Result, StorageError};

type Db = DBWithThreadMode<MultiThreaded>;

/// File-type marker: `RS3PB` + format version byte.
const MAGIC: &[u8] = b"RS3PB\x01";

/// End-of-stream sentinel written after the last row: a descending
/// `0x1F, 0x1E, … 0x01, 0x00` run (32 bytes, ending in `0x00`). Its exact
/// presence at the tail proves the dump was written to completion; import
/// rejects any file whose tail does not match, catching truncation.
const TRAILER: [u8; 32] = [
    0x1F, 0x1E, 0x1D, 0x1C, 0x1B, 0x1A, 0x19, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x10,
    0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00,
];

/// How an import treats rows already in the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportMode {
    /// Upsert the dump's rows; leave any existing rows not in the dump.
    Merge,
    /// Erase the managed families first, then load — an exact restore.
    Replace,
}

impl ImportMode {
    pub fn label(self) -> &'static str {
        match self {
            ImportMode::Merge => "merge",
            ImportMode::Replace => "replace",
        }
    }
}

/// A legible summary of what an import applied: the mode, how many existing rows
/// were erased first (Replace only), the total imported, and a per-family
/// breakdown in the order the families first appeared.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ImportReport {
    pub mode: &'static str,
    pub erased: u64,
    pub total: u64,
    pub per_cf: Vec<(String, u64)>,
}

impl ImportReport {
    fn record(&mut self, cf: &str) {
        self.total += 1;
        match self.per_cf.iter_mut().find(|(name, _)| name == cf) {
            Some((_, count)) => *count += 1,
            None => self.per_cf.push((cf.to_string(), 1)),
        }
    }
}

/// One dumped key/value pair. `cf` travels with every row so import never
/// depends on ordering or grouping.
#[derive(Clone, PartialEq, Message)]
struct Row {
    #[prost(string, tag = "1")]
    cf: String,
    #[prost(bytes = "vec", tag = "2")]
    key: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    value: Vec<u8>,
}

/// Dumps every listed column family to `w` under a single consistent snapshot.
/// Returns the number of rows written.
pub fn export<W: Write>(db: &Db, cf_names: &[&str], w: &mut W) -> Result<u64> {
    w.write_all(MAGIC)?;
    let snapshot = db.snapshot();
    let mut rows = 0u64;
    let mut buf: Vec<u8> = Vec::new();
    for name in cf_names {
        let cf = db
            .cf_handle(name)
            .ok_or_else(|| StorageError::Db(format!("missing column family {name}")))?;
        let mut opts = ReadOptions::default();
        opts.set_snapshot(&snapshot);
        for item in db.iterator_cf_opt(&cf, opts, IteratorMode::Start) {
            let (key, value) = item?;
            let row = Row {
                cf: name.to_string(),
                key: key.into_vec(),
                value: value.into_vec(),
            };
            buf.clear();
            row.encode_length_delimited(&mut buf)
                .map_err(|e| StorageError::Db(format!("protobuf encode: {e}")))?;
            w.write_all(&buf)?;
            rows += 1;
        }
    }
    w.write_all(&TRAILER)?;
    w.flush()?;
    Ok(rows)
}

/// Validates the fixed framing of a dump (magic header + completeness
/// trailer) and returns the row-stream body between them.
fn dump_body(data: &[u8]) -> Result<&[u8]> {
    if data.len() < MAGIC.len() + TRAILER.len() {
        return Err(StorageError::Db("not a rusts3 dump (too short)".into()));
    }
    if &data[..MAGIC.len()] != MAGIC {
        return Err(StorageError::Db("not a rusts3 dump (bad magic header)".into()));
    }
    let body_end = data.len() - TRAILER.len();
    if data[body_end..] != TRAILER {
        return Err(StorageError::Db(
            "incomplete dump: end-of-stream marker missing or corrupt (truncated?)".into(),
        ));
    }
    Ok(&data[MAGIC.len()..body_end])
}

/// Per-family shape of a dump for a staged import preview: row count plus up
/// to [`SAMPLE_ROWS`] display names. Names are derived from row **keys** only
/// — values, which can carry secrets, are never decoded.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct PreviewFamily {
    pub cf: String,
    pub rows: u64,
    pub samples: Vec<String>,
}

pub const SAMPLE_ROWS: usize = 10;

/// Reads a dump and reports what an import would load, without touching any
/// database: families in first-appearance order, each with its row count and
/// key-derived sample names. Runs the same framing validation as
/// [`import`], so a truncated or foreign file is rejected here, before the
/// user is asked to confirm anything.
pub fn preview<R: Read>(mut r: R) -> Result<Vec<PreviewFamily>> {
    let mut data = Vec::new();
    r.read_to_end(&mut data)?;
    let mut cursor = dump_body(&data)?;
    let mut families: Vec<PreviewFamily> = Vec::new();
    let mut total = 0u64;
    while !cursor.is_empty() {
        let row = Row::decode_length_delimited(&mut cursor)
            .map_err(|e| StorageError::Db(format!("protobuf decode (near row {total}): {e}")))?;
        total += 1;
        let family = match families.iter_mut().position(|f| f.cf == row.cf) {
            Some(i) => &mut families[i],
            None => {
                families.push(PreviewFamily {
                    cf: row.cf.clone(),
                    ..Default::default()
                });
                families.last_mut().unwrap()
            }
        };
        family.rows += 1;
        if family.samples.len() < SAMPLE_ROWS {
            // Composite keys (`user_groups` is `user\0group`) render with an
            // arrow; simple keys pass through unchanged.
            let display = String::from_utf8_lossy(&row.key)
                .split('\0')
                .collect::<Vec<_>>()
                .join(" → ");
            family.samples.push(display);
        }
    }
    Ok(families)
}

/// Restores a dump into `db`. In [`ImportMode::Merge`] every row is upserted
/// (`put`) and nothing else changes. In [`ImportMode::Replace`] every family in
/// `managed_cfs` is erased first, so the result is exactly the dump (rows in the
/// DB but not in the dump are dropped). The whole restore — erase plus load — is
/// one atomic write batch. Row order in the dump does not matter: each row
/// carries its own family and lands independently. Returns a per-family
/// [`ImportReport`].
pub fn import<R: Read>(db: &Db, mut r: R, mode: ImportMode, managed_cfs: &[&str]) -> Result<ImportReport> {
    let mut data = Vec::new();
    r.read_to_end(&mut data)?;
    let body = dump_body(&data)?;
    let mut batch = WriteBatch::default();
    let mut report = ImportReport {
        mode: mode.label(),
        ..Default::default()
    };

    // Replace: queue a delete for every existing row in the managed families,
    // ahead of the puts. Within one batch, deletes then puts apply in order, so
    // a key present in both is left with its imported value and everything else
    // in those families is dropped.
    if mode == ImportMode::Replace {
        for name in managed_cfs {
            let cf = db
                .cf_handle(name)
                .ok_or_else(|| StorageError::Db(format!("missing column family {name}")))?;
            for item in db.iterator_cf(&cf, IteratorMode::Start) {
                let (key, _) = item?;
                batch.delete_cf(&cf, key);
                report.erased += 1;
            }
        }
    }

    let mut cursor: &[u8] = body;
    while !cursor.is_empty() {
        let row = Row::decode_length_delimited(&mut cursor).map_err(|e| {
            StorageError::Db(format!("protobuf decode (near row {}): {e}", report.total))
        })?;
        let cf = db
            .cf_handle(&row.cf)
            .ok_or_else(|| StorageError::Db(format!("dump references unknown column family {}", row.cf)))?;
        batch.put_cf(&cf, row.key, row.value);
        report.record(&row.cf);
    }

    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    db.write_opt(batch, &wo)?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-assembles a dump: magic + length-delimited rows + trailer.
    fn make_dump(rows: &[(&str, &[u8], &[u8])]) -> Vec<u8> {
        let mut out = Vec::from(MAGIC);
        for (cf, key, value) in rows {
            let row = Row {
                cf: cf.to_string(),
                key: key.to_vec(),
                value: value.to_vec(),
            };
            let mut buf = Vec::new();
            row.encode_length_delimited(&mut buf).unwrap();
            out.extend_from_slice(&buf);
        }
        out.extend_from_slice(&TRAILER);
        out
    }

    #[test]
    fn preview_counts_families_and_samples_keys_only() {
        let mut rows: Vec<(String, Vec<u8>, Vec<u8>)> = Vec::new();
        for i in 0..14 {
            rows.push((
                "users".to_string(),
                format!("user{i}").into_bytes(),
                br#"{"password_hash":"SUPERSECRET"}"#.to_vec(),
            ));
        }
        rows.push((
            "user_groups".to_string(),
            b"jack\0admins".to_vec(),
            b"{}".to_vec(),
        ));
        let borrowed: Vec<(&str, &[u8], &[u8])> = rows
            .iter()
            .map(|(cf, k, v)| (cf.as_str(), k.as_slice(), v.as_slice()))
            .collect();
        let dump = make_dump(&borrowed);

        let families = preview(dump.as_slice()).unwrap();
        assert_eq!(families.len(), 2);
        assert_eq!(families[0].cf, "users");
        assert_eq!(families[0].rows, 14);
        assert_eq!(families[0].samples.len(), SAMPLE_ROWS, "samples capped at {SAMPLE_ROWS}");
        assert_eq!(families[0].samples[0], "user0");
        assert_eq!(families[1].cf, "user_groups");
        assert_eq!(families[1].samples, vec!["jack → admins"]);
        // Secrets live in values; no sample may ever contain one.
        let all = families
            .iter()
            .flat_map(|f| f.samples.iter())
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!all.contains("SUPERSECRET"), "preview must never surface row values");
    }

    #[test]
    fn preview_rejects_truncated_dump() {
        let dump = make_dump(&[("users", b"jack".as_slice(), b"{}".as_slice())]);
        assert!(preview(&dump[..dump.len() - 1]).is_err(), "missing trailer byte must fail");
        assert!(preview(&dump[1..]).is_err(), "bad magic must fail");
    }
}
