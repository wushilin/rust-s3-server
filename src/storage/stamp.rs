//! Object version stamps and the per-site hybrid logical clock.
//!
//! Every object state carries a [`Stamp`] `(ms, c, site)`: when it was
//! written, a logical counter, and the site that wrote it. Stamps are totally
//! ordered (by `ms`, then `c`, then `site`) and identical on every site, so two
//! sites holding different states of a key independently agree which one is
//! newer — the basis of last-writer-wins replication. A replicated object keeps
//! the stamp of the site that wrote it.
//!
//! The clock is a hybrid logical clock: `ms` follows the wall clock but never
//! goes backwards, `c` counts writes within one millisecond (and resets when
//! `ms` moves on, so it is almost always 0), and [`SiteClock::observe`] moves
//! the clock past any stamp the site has seen, so a later local write always
//! wins over it. A high-water mark persisted in `site.json` (renewed a couple
//! of seconds ahead) keeps a restart from reissuing older stamps after a
//! clock step.

use std::cmp::Ordering;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::errors::{Result, StorageError};
use super::time::now_ms;

const SITE_FILE: &str = "site.json";
/// How far ahead of the clock the persisted high-water mark is renewed.
const RESERVE_MS: i64 = 2_000;
/// Replicated stamps further ahead than this are refused, so a bad peer
/// clock cannot drag this site's clock into the future.
pub const MAX_FUTURE_MS: i64 = 10 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Stamp {
    pub ms: i64,
    pub c: u32,
    /// The writing site's id. A string on the wire: 64-bit ids would lose
    /// precision in JSON numbers.
    pub site: String,
}

impl Ord for Stamp {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.ms, self.c, &self.site).cmp(&(other.ms, other.c, &other.site))
    }
}

impl PartialOrd for Stamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Stamp {
    /// `<ms>.<c>.<site>` — the `x-rs3-stamp` header form.
    pub fn to_header(&self) -> String {
        format!("{}.{}.{}", self.ms, self.c, self.site)
    }

    pub fn parse_header(value: &str) -> std::result::Result<Self, String> {
        let mut parts = value.trim().splitn(3, '.');
        let (Some(ms), Some(c), Some(site)) = (parts.next(), parts.next(), parts.next()) else {
            return Err("x-rs3-stamp must be <ms>.<counter>.<site>".to_string());
        };
        let ms = ms.parse::<i64>().map_err(|_| "x-rs3-stamp: bad milliseconds".to_string())?;
        let c = c.parse::<u32>().map_err(|_| "x-rs3-stamp: bad counter".to_string())?;
        validate_site_id(site)?;
        if ms < 0 {
            return Err("x-rs3-stamp: negative milliseconds".to_string());
        }
        Ok(Self { ms, c, site: site.to_string() })
    }

    /// The stamp an object written before stamps existed is treated as
    /// having: its Last-Modified, written by this site.
    pub fn fallback(last_modified_ms: i64, site: &str) -> Self {
        Self {
            ms: last_modified_ms,
            c: 0,
            site: site.to_string(),
        }
    }
}

/// Where a state of a key ranks in last-writer-wins. Every site computes it
/// the same way, so every site keeps the same winner.
///
/// A live object ranks by its stamp. A delete ranks by the stamp of the
/// version it **removed** — not by when it happened — just above that
/// version. So a delete only defeats what its author saw: a write made
/// concurrently on another site (a higher stamp than the removed version)
/// survives it ("observed-remove" / add-wins), while the removed version
/// itself, and anything older, stays deleted.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Rank {
    pub stamp: Stamp,
    /// A tombstone; ranks above a live state with the same stamp.
    pub deleted: bool,
}

impl Rank {
    pub fn live(stamp: Stamp) -> Self {
        Self { stamp, deleted: false }
    }

    pub fn deleted(removed: Stamp) -> Self {
        Self { stamp: removed, deleted: true }
    }
}

/// A site name: trimmed, 1–64 characters, no control characters. Returns
/// the trimmed name.
pub fn validate_site_name(name: &str) -> std::result::Result<String, String> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 64 || name.chars().any(char::is_control) {
        return Err("a site name is 1-64 characters, without control characters".to_string());
    }
    Ok(name.to_string())
}

/// A friendly default site name: two random words, like `quiet-falcon`.
pub fn generate_site_name() -> String {
    const ADJECTIVES: &[&str] = &[
        "amber", "bold", "brave", "bright", "calm", "clever", "crisp", "daring", "eager", "fancy", "gentle",
        "glad", "golden", "grand", "happy", "hidden", "humble", "jolly", "keen", "kind", "lively", "lucky",
        "mellow", "merry", "mighty", "misty", "noble", "patient", "polar", "proud", "quick", "quiet", "rapid",
        "rosy", "rustic", "silent", "silver", "sleek", "snowy", "solar", "steady", "stellar", "sunny", "swift",
        "tidy", "vivid", "warm", "wise",
    ];
    const NOUNS: &[&str] = &[
        "badger", "beacon", "birch", "canyon", "cedar", "comet", "coral", "crane", "delta", "dolphin", "eagle",
        "falcon", "fern", "fjord", "forest", "fox", "glacier", "harbor", "hawk", "heron", "island", "lagoon",
        "lantern", "maple", "meadow", "mesa", "otter", "owl", "panda", "pebble", "pine", "prairie", "raven",
        "reef", "river", "robin", "sparrow", "spruce", "summit", "tiger", "tundra", "valley", "willow", "wolf",
        "wren", "yak", "zephyr", "orchid",
    ];
    use rand::seq::IndexedRandom;
    let mut rng = rand::rng();
    format!("{}-{}", ADJECTIVES.choose(&mut rng).unwrap(), NOUNS.choose(&mut rng).unwrap())
}

/// Site ids are 1–64 characters of `[A-Za-z0-9_-]` (no dots: they end the
/// header form).
pub fn validate_site_id(site: &str) -> std::result::Result<(), String> {
    if site.is_empty()
        || site.len() > 64
        || !site.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(format!(
            "invalid site id {site:?}: use 1-64 letters, digits, '-' or '_'"
        ));
    }
    Ok(())
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SiteFile {
    /// Generated once, on first start.
    #[serde(default)]
    generated_site_id: String,
    /// The site id this data directory is: fixed at first start (the
    /// configured one, else the generated one) and never changed after —
    /// it is in every stamp the site wrote and is how peers know it.
    /// (Empty in files written before it was recorded: pinned on the next
    /// start.)
    #[serde(default)]
    site_id: String,
    /// The site's readable name: a label, free to change (console, or
    /// `server.site_name`). Generated as two words on first start.
    #[serde(default)]
    site_name: String,
    /// No stamp at or below this was ever issued... or above it: every
    /// issued `ms` is `<=` this.
    #[serde(default)]
    high_water_ms: i64,
}

#[derive(Debug)]
struct ClockState {
    last_ms: i64,
    last_c: u32,
    reserved_until: i64,
    /// The site name (kept here so every rewrite of site.json carries it).
    name: String,
}

#[derive(Debug)]
pub struct SiteClock {
    site: String,
    path: Option<PathBuf>,
    generated: String,
    state: Mutex<ClockState>,
}

impl SiteClock {
    /// Opens (creating on first start) `<root>/site.json`. The site id is
    /// generated at first start and recorded there; it is never configured
    /// and never changes — it is in every version stamp the site writes and
    /// is how replication peers know it (a readable `server.site_name` is
    /// what can change). A copy of the data directory is the same site.
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let path = root.join(SITE_FILE);
        let mut file: SiteFile = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => SiteFile::default(),
            Err(err) => return Err(err.into()),
        };
        if file.generated_site_id.is_empty() {
            use rand::Rng;
            let mut buf = [0u8; 8];
            rand::rng().fill_bytes(&mut buf);
            file.generated_site_id = buf.iter().map(|b| format!("{b:02x}")).collect();
        }
        if file.site_name.trim().is_empty() {
            file.site_name = generate_site_name();
        }
        // The recorded id (a file from before it was recorded: the
        // generated one).
        let site = match file.site_id.as_str() {
            "" => file.generated_site_id.clone(),
            recorded => {
                validate_site_id(recorded).map_err(StorageError::Io)?;
                recorded.to_string()
            }
        };
        let clock = Self {
            site,
            path: Some(path),
            generated: file.generated_site_id.clone(),
            state: Mutex::new(ClockState {
                // Resume at the high-water mark: nothing issued before the
                // restart can be reissued, whatever the wall clock says now.
                last_ms: file.high_water_ms,
                last_c: 0,
                reserved_until: file.high_water_ms,
                name: file.site_name.clone(),
            }),
        };
        // Persist the id (and an initial reservation) right away.
        {
            let mut state = clock.state.lock().unwrap();
            let until = now_ms().max(state.last_ms);
            clock.reserve(&mut state, until)?;
        }
        Ok(clock)
    }

    /// An unpersisted clock (tests, tools): a fixed site id.
    pub fn ephemeral(site: &str) -> Self {
        Self {
            site: site.to_string(),
            path: None,
            generated: site.to_string(),
            state: Mutex::new(ClockState {
                last_ms: 0,
                last_c: 0,
                reserved_until: i64::MAX,
                name: site.to_string(),
            }),
        }
    }

    pub fn site_id(&self) -> &str {
        &self.site
    }

    /// The site's readable name.
    pub fn site_name(&self) -> String {
        self.state.lock().unwrap().name.clone()
    }

    /// Renames the site (a label: nothing else changes), persisted.
    pub fn set_site_name(&self, name: &str) -> Result<()> {
        let name = validate_site_name(name).map_err(StorageError::Io)?;
        let mut state = self.state.lock().unwrap();
        let previous = std::mem::replace(&mut state.name, name);
        let until = state.reserved_until - RESERVE_MS;
        if let Err(err) = self.reserve(&mut state, until) {
            state.name = previous;
            return Err(err);
        }
        Ok(())
    }

    /// Persists `high_water_ms = until + RESERVE_MS`.
    fn reserve(&self, state: &mut ClockState, until: i64) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let high_water = until + RESERVE_MS;
        let body = serde_json::to_vec_pretty(&SiteFile {
            generated_site_id: self.generated.clone(),
            site_id: self.site.clone(),
            site_name: state.name.clone(),
            high_water_ms: high_water,
        })?;
        let tmp = path.with_extension("json.tmp");
        {
            let mut out = fs::File::create(&tmp)?;
            out.write_all(&body)?;
            out.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        state.reserved_until = high_water;
        Ok(())
    }

    /// The next local stamp: strictly greater than every stamp this site
    /// issued or observed.
    pub fn next(&self) -> Stamp {
        let mut state = self.state.lock().unwrap();
        let wall = now_ms();
        let (ms, c) = if wall > state.last_ms {
            (wall, 0)
        } else {
            (state.last_ms, state.last_c + 1)
        };
        if ms > state.reserved_until {
            if let Err(err) = self.reserve(&mut state, ms) {
                // Stamps stay correct while running; only restart safety
                // after a backwards clock step is weakened.
                log::warn!("could not persist the site clock high-water mark: {err}");
            }
        }
        state.last_ms = ms;
        state.last_c = c;
        Stamp {
            ms,
            c,
            site: self.site.clone(),
        }
    }

    /// Moves the clock past `stamp`, so later local stamps are greater.
    pub fn observe(&self, stamp: &Stamp) {
        let mut state = self.state.lock().unwrap();
        if (stamp.ms, stamp.c) > (state.last_ms, state.last_c) {
            state.last_ms = stamp.ms;
            state.last_c = stamp.c;
            if stamp.ms > state.reserved_until {
                if let Err(err) = self.reserve(&mut state, stamp.ms) {
                    log::warn!("could not persist the site clock high-water mark: {err}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_order_by_ms_then_counter_then_site() {
        let s = |ms, c, site: &str| Stamp { ms, c, site: site.into() };
        assert!(s(1, 9, "z") < s(2, 0, "a"));
        assert!(s(2, 0, "z") < s(2, 1, "a"));
        assert!(s(2, 1, "a") < s(2, 1, "b"));
        assert_eq!(Stamp::parse_header(&s(5, 2, "site-1").to_header()).unwrap(), s(5, 2, "site-1"));
        assert!(Stamp::parse_header("5.2").is_err());
        assert!(Stamp::parse_header("5.x.a").is_err());
        assert!(Stamp::parse_header("5.1.bad.site").is_err(), "no dots in site ids");
    }

    #[test]
    fn a_delete_ranks_just_above_the_version_it_removed() {
        let s = |ms| Stamp { ms, c: 0, site: "a".into() };
        let v1 = Rank::live(s(1));
        let delete_v1 = Rank::deleted(s(1));
        let v2 = Rank::live(s(2));
        assert!(delete_v1 > v1, "the delete beats the version it removed");
        assert!(v2 > delete_v1, "a newer write, seen or not, beats the delete of v1");
        assert!(Rank::deleted(s(2)) > v2, "deleting v2 needs to have seen v2");
    }

    #[test]
    fn local_stamps_strictly_increase_and_pass_observed_ones() {
        let clock = SiteClock::ephemeral("a");
        let mut last = clock.next();
        for _ in 0..1000 {
            let next = clock.next();
            assert!(next > last);
            last = next;
        }
        // A stamp from a site whose clock is ahead: the next local write
        // must still win over it.
        let ahead = Stamp { ms: now_ms() + 5_000, c: 7, site: "b".into() };
        clock.observe(&ahead);
        let after = clock.next();
        assert!(after > ahead);
        assert_eq!((after.ms, after.c), (ahead.ms, 8));
    }

    #[test]
    fn the_site_id_is_generated_once_and_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let first = SiteClock::open(tmp.path()).unwrap();
        let id = first.site_id().to_string();
        assert_eq!(id.len(), 16);
        let stamp = first.next();
        drop(first);
        let again = SiteClock::open(tmp.path()).unwrap();
        assert_eq!(again.site_id(), id, "generated once");
        assert!(again.next() > stamp, "a restart never reissues older stamps");
        let recorded: serde_json::Value = serde_json::from_slice(&std::fs::read(tmp.path().join(SITE_FILE)).unwrap()).unwrap();
        assert_eq!(recorded["site_id"], id.as_str(), "recorded");
        // Another directory, another site.
        let other = tempfile::tempdir().unwrap();
        assert_ne!(SiteClock::open(other.path()).unwrap().site_id(), id);
        // A file from before the id was recorded keeps the id it had.
        let legacy = tempfile::tempdir().unwrap();
        std::fs::write(legacy.path().join(SITE_FILE), r#"{"generated_site_id":"0011223344556677","high_water_ms":5}"#).unwrap();
        assert_eq!(SiteClock::open(legacy.path()).unwrap().site_id(), "0011223344556677");
        // A two-word name is generated once, kept, and can be changed —
        // without touching the id.
        let name = SiteClock::open(tmp.path()).unwrap().site_name();
        assert!(name.split('-').count() == 2 && name.chars().all(|c| c.is_ascii_lowercase() || c == '-'), "{name}");
        let clock = SiteClock::open(tmp.path()).unwrap();
        assert_eq!(clock.site_name(), name, "kept");
        clock.set_site_name("  Singapore DC 1  ").unwrap();
        assert_eq!(clock.site_name(), "Singapore DC 1", "trimmed");
        assert!(clock.set_site_name("").is_err() && clock.set_site_name(&"x".repeat(65)).is_err());
        assert_eq!(clock.site_name(), "Singapore DC 1", "a refused name changes nothing");
        drop(clock);
        let reopened = SiteClock::open(tmp.path()).unwrap();
        assert_eq!((reopened.site_id(), reopened.site_name().as_str()), (id.as_str(), "Singapore DC 1"));
        // The clock's rewrites of site.json keep the name.
        for _ in 0..3 {
            reopened.next();
        }
        assert_eq!(SiteClock::open(tmp.path()).unwrap().site_name(), "Singapore DC 1");

        // A recorded id that is not valid is refused, not replaced.
        std::fs::write(legacy.path().join(SITE_FILE), r#"{"generated_site_id":"0011223344556677","site_id":"bad.id"}"#).unwrap();
        assert!(SiteClock::open(legacy.path()).is_err());
    }

    #[test]
    fn restart_resumes_at_the_high_water_mark() {
        let tmp = tempfile::tempdir().unwrap();
        // Simulate a previous run that issued stamps far in the future
        // (relative to the wall clock now — as after a backwards step).
        fs::write(
            tmp.path().join(SITE_FILE),
            serde_json::to_vec(&SiteFile { generated_site_id: "abc".into(), site_id: String::new(), site_name: String::new(), high_water_ms: now_ms() + 60_000 }).unwrap(),
        )
        .unwrap();
        let clock = SiteClock::open(tmp.path()).unwrap();
        assert!(clock.next().ms >= now_ms() + 59_000);
    }
}
