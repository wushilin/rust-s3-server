//! Runtime system/process sampling for the "Runtime Stats" dashboard.
//!
//! Every field is derived from Linux `/proc` plus the in-process
//! [`TrafficMetrics`](super::TrafficMetrics), so there is no external
//! dependency and nothing to install in the image. Reads are cheap (a handful
//! of tiny virtual files) and happen once per `sample_secs`.
//!
//! A [`Sample`] is one point in time with all series bundled together — cheaper
//! than a row per metric, and the UI always wants them for the same instant.
//! Rate-style series (CPU %, disk B/s, network B/s, QPS) are computed here at
//! sample time by diffing against the previous raw counters, so stored values
//! are ready to plot. Absolute series (memory) are stored as-is.
//!
//! **Platform:** the `/proc` sources are Linux-only. On any other OS the reads
//! fail, the corresponding [`Sample`] fields are `None`, and the charts render
//! gaps — the sampler never panics. **Container caveat:** `/proc/stat` and
//! `/proc/diskstats` are host-wide, so `cpu_sys*` and `disk_sys*` reflect the
//! whole host; the `*_proc*` series (from `/proc/self/*`) are exact.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use super::config::StatsConfig;
use super::stats_store::StatsStore;
use super::TrafficMetrics;
use crate::storage::time::now_ms;

/// One sampled instant. Every metric is `Option` so an unavailable source
/// (non-Linux, or a `/proc` read that failed) becomes a null the chart draws
/// as a gap rather than a fabricated zero. Bytes and byte-rates are stored as
/// `f64` for uniform charting; the range (< 2^53) is never a concern here.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    /// Host CPU busy %, 0..=100 across all cores.
    pub cpu_sys: Option<f64>,
    /// This process's share of total CPU, %, comparable to `cpu_sys`.
    pub cpu_proc: Option<f64>,
    /// Host memory in use (bytes) = MemTotal − MemAvailable.
    pub mem_used: Option<f64>,
    /// Host memory total (bytes).
    pub mem_total: Option<f64>,
    /// This process's resident set (bytes).
    pub mem_proc_rss: Option<f64>,
    /// Process disk read / write, bytes per second.
    pub disk_proc_r: Option<f64>,
    pub disk_proc_w: Option<f64>,
    /// Host disk read / write, bytes per second.
    pub disk_sys_r: Option<f64>,
    pub disk_sys_w: Option<f64>,
    /// S3 network in / out, bytes per second.
    pub net_in: Option<f64>,
    pub net_out: Option<f64>,
    /// S3 requests per second across all methods.
    pub qps: Option<f64>,
}

impl Sample {
    /// Field count, and the column order shared by [`to_cols`](Self::to_cols),
    /// [`from_cols`](Self::from_cols), and the API's columnar response.
    pub(crate) const COLS: usize = 12;

    /// The metrics as a positional array, so the store can aggregate buckets
    /// generically instead of naming all twelve fields.
    pub(crate) fn to_cols(&self) -> [Option<f64>; Self::COLS] {
        [
            self.cpu_sys, self.cpu_proc, self.mem_used, self.mem_total, self.mem_proc_rss,
            self.disk_proc_r, self.disk_proc_w, self.disk_sys_r, self.disk_sys_w,
            self.net_in, self.net_out, self.qps,
        ]
    }

    pub(crate) fn from_cols(c: [Option<f64>; Self::COLS]) -> Self {
        Self {
            cpu_sys: c[0], cpu_proc: c[1], mem_used: c[2], mem_total: c[3], mem_proc_rss: c[4],
            disk_proc_r: c[5], disk_proc_w: c[6], disk_sys_r: c[7], disk_sys_w: c[8],
            net_in: c[9], net_out: c[10], qps: c[11],
        }
    }
}

/// Raw monotonic counters read each tick; a [`Sample`] is the diff of two.
#[derive(Debug, Clone)]
struct Raw {
    at_ms: i64,
    cpu_total: Option<u64>,
    cpu_idle: Option<u64>,
    proc_cpu: Option<u64>,
    mem_used: Option<u64>,
    mem_total: Option<u64>,
    proc_rss: Option<u64>,
    proc_io_r: Option<u64>,
    proc_io_w: Option<u64>,
    disk_r: Option<u64>,
    disk_w: Option<u64>,
    net_in: u64,
    net_out: u64,
    reqs: u64,
}

// ── /proc parsers (pure, unit-tested) ────────────────────────────────────────

/// `cpu  user nice system idle iowait irq softirq steal ...` → (total, idle).
/// idle counts both idle and iowait; total is the sum of every field.
fn parse_proc_stat(text: &str) -> Option<(u64, u64)> {
    let line = text.lines().find(|l| l.starts_with("cpu "))?;
    let vals: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();
    if vals.len() < 5 {
        return None;
    }
    let total: u64 = vals.iter().sum();
    let idle = vals[3] + vals[4]; // idle + iowait
    Some((total, idle))
}

/// `/proc/self/stat`: utime (field 14) + stime (field 15) in clock ticks. The
/// comm field (2) can contain spaces and parentheses, so we split after the
/// last `)` and index from there.
fn parse_self_stat(text: &str) -> Option<u64> {
    let after = &text[text.rfind(')')? + 1..];
    let f: Vec<&str> = after.split_whitespace().collect();
    // After ')', index 0 is field 3 (state); utime=14 → 11, stime=15 → 12.
    let utime: u64 = f.get(11)?.parse().ok()?;
    let stime: u64 = f.get(12)?.parse().ok()?;
    Some(utime + stime)
}

fn parse_meminfo_kb(text: &str, key: &str) -> Option<u64> {
    let line = text.lines().find(|l| l.starts_with(key))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

/// (MemTotal, MemAvailable) in bytes.
fn parse_meminfo(text: &str) -> Option<(u64, u64)> {
    let total = parse_meminfo_kb(text, "MemTotal:")? * 1024;
    let avail = parse_meminfo_kb(text, "MemAvailable:")? * 1024;
    Some((total, avail))
}

/// `/proc/self/status` VmRSS in bytes.
fn parse_vmrss(text: &str) -> Option<u64> {
    Some(parse_meminfo_kb(text, "VmRSS:")? * 1024)
}

/// `/proc/self/io` read_bytes / write_bytes.
fn parse_self_io(text: &str) -> Option<(u64, u64)> {
    let field = |k: &str| -> Option<u64> {
        text.lines()
            .find(|l| l.starts_with(k))?
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()
    };
    Some((field("read_bytes:")?, field("write_bytes:")?))
}

/// Whole physical disks only (no partitions, loop, or ram devices), so summing
/// sectors doesn't double-count a disk and its partitions.
fn whole_disk() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^(sd[a-z]+|vd[a-z]+|xvd[a-z]+|hd[a-z]+|nvme\d+n\d+|mmcblk\d+)$")
            .expect("static regex")
    })
}

/// Sum of sectors read/written across whole disks, converted to bytes
/// (512 B/sector). Returns `(read_bytes, write_bytes)`.
fn parse_diskstats(text: &str) -> Option<(u64, u64)> {
    let mut read_sectors = 0u64;
    let mut write_sectors = 0u64;
    let mut matched = false;
    for line in text.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        // major minor name reads rd_merged rd_sectors ... writes wr_merged wr_sectors
        if f.len() < 10 {
            continue;
        }
        if !whole_disk().is_match(f[2]) {
            continue;
        }
        matched = true;
        read_sectors += f[5].parse::<u64>().unwrap_or(0);
        write_sectors += f[9].parse::<u64>().unwrap_or(0);
    }
    // A readable diskstats with no recognized whole disk is a real zero, not an
    // error — only a missing/unreadable file yields None (handled by the caller).
    let _ = matched;
    Some((read_sectors * 512, write_sectors * 512))
}

// ── sampling ─────────────────────────────────────────────────────────────────

fn slurp(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

fn read_raw(metrics: &TrafficMetrics) -> Raw {
    // Warn once if /proc is absent (non-Linux dev host) so the empty charts are
    // explained rather than mysterious.
    static WARNED: AtomicBool = AtomicBool::new(false);
    let proc_stat = slurp("/proc/stat");
    if proc_stat.is_none() && !WARNED.swap(true, Ordering::Relaxed) {
        log::warn!("runtime stats: /proc is unavailable; system/process series will be empty");
    }

    let (cpu_total, cpu_idle) = proc_stat
        .as_deref()
        .and_then(parse_proc_stat)
        .map_or((None, None), |(t, i)| (Some(t), Some(i)));
    let (mem_total, mem_used) = slurp("/proc/meminfo")
        .as_deref()
        .and_then(parse_meminfo)
        .map_or((None, None), |(t, a)| (Some(t), Some(t.saturating_sub(a))));
    let (proc_io_r, proc_io_w) = slurp("/proc/self/io")
        .as_deref()
        .and_then(parse_self_io)
        .map_or((None, None), |(r, w)| (Some(r), Some(w)));
    let (disk_r, disk_w) = slurp("/proc/diskstats")
        .as_deref()
        .and_then(parse_diskstats)
        .map_or((None, None), |(r, w)| (Some(r), Some(w)));

    let (net_in, net_out) = metrics.byte_totals();
    Raw {
        at_ms: now_ms(),
        cpu_total,
        cpu_idle,
        proc_cpu: slurp("/proc/self/stat").as_deref().and_then(parse_self_stat),
        mem_used,
        mem_total,
        proc_rss: slurp("/proc/self/status").as_deref().and_then(parse_vmrss),
        proc_io_r,
        proc_io_w,
        disk_r,
        disk_w,
        net_in,
        net_out,
        reqs: metrics.request_total_count(),
    }
}

/// Both CPU %s share the aggregate jiffie delta as their denominator, so the
/// process figure is directly comparable to the system figure (share of the
/// whole machine, 0..=100).
fn diff(prev: &Raw, cur: &Raw) -> Sample {
    let dt_s = ((cur.at_ms - prev.at_ms).max(1) as f64) / 1000.0;

    let cpu_delta = match (prev.cpu_total, prev.cpu_idle, cur.cpu_total, cur.cpu_idle) {
        (Some(pt), Some(pi), Some(ct), Some(ci)) if ct > pt => {
            Some((ct - pt, (ct - pt).saturating_sub(ci.saturating_sub(pi))))
        }
        _ => None,
    };
    let cpu_sys = cpu_delta.map(|(total, busy)| busy as f64 / total as f64 * 100.0);
    let cpu_proc = match (cpu_delta, prev.proc_cpu, cur.proc_cpu) {
        (Some((total, _)), Some(pp), Some(cp)) => {
            Some(cp.saturating_sub(pp) as f64 / total as f64 * 100.0)
        }
        _ => None,
    };

    // bytes-per-second from a counter delta over the real elapsed time.
    let rate = |prev: Option<u64>, cur: Option<u64>| -> Option<f64> {
        match (prev, cur) {
            (Some(p), Some(c)) => Some(c.saturating_sub(p) as f64 / dt_s),
            _ => None,
        }
    };

    Sample {
        cpu_sys,
        cpu_proc,
        mem_used: cur.mem_used.map(|v| v as f64),
        mem_total: cur.mem_total.map(|v| v as f64),
        mem_proc_rss: cur.proc_rss.map(|v| v as f64),
        disk_proc_r: rate(prev.proc_io_r, cur.proc_io_r),
        disk_proc_w: rate(prev.proc_io_w, cur.proc_io_w),
        disk_sys_r: rate(prev.disk_r, cur.disk_r),
        disk_sys_w: rate(prev.disk_w, cur.disk_w),
        net_in: Some(cur.net_in.saturating_sub(prev.net_in) as f64 / dt_s),
        net_out: Some(cur.net_out.saturating_sub(prev.net_out) as f64 / dt_s),
        qps: Some(cur.reqs.saturating_sub(prev.reqs) as f64 / dt_s),
    }
}

/// Spawns the sampler task: every `sample_secs` it snapshots the raw counters,
/// stores the diff against the previous snapshot, and on a slow (~hourly)
/// cadence prunes samples older than the retention window. Stops on `shutdown`.
pub(crate) fn spawn_sampler(
    store: StatsStore,
    metrics: Arc<TrafficMetrics>,
    cfg: StatsConfig,
    shutdown: CancellationToken,
) {
    let period = cfg.sample_secs.max(1);
    let retention_ms = (cfg.retention_days.max(1)).saturating_mul(86_400).saturating_mul(1000) as i64;
    // Prune roughly hourly, but at least once every tick if the period is huge.
    let prune_every = (3600 / period).max(1);

    tokio::spawn(async move {
        log::info!(
            "runtime stats sampler started sample_secs={period} retention_days={}",
            cfg.retention_days
        );
        // Align the first tick to the next wall-clock multiple of the period, so
        // samples land on stable boundaries (:00/:05/:10…) across restarts.
        let period_ms = (period * 1000) as i64;
        let delay_ms = (period_ms - now_ms().rem_euclid(period_ms)) as u64;
        let start = tokio::time::Instant::now() + Duration::from_millis(delay_ms);
        let mut interval = tokio::time::interval_at(start, Duration::from_secs(period));
        // Fixed-rate: keep ticks on the original aligned grid, skipping any the
        // task was too busy to service — never bunching (Burst) or drifting the
        // phase (Delay).
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // The first tick establishes the baseline counters; every tick after it
        // stores the diff against the previous one.
        let mut prev: Option<Raw> = None;
        let mut samples: u64 = 0;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    log::info!("runtime stats sampler stopping");
                    break;
                }
                _ = interval.tick() => {
                    let cur = read_raw(&metrics);
                    if let Some(prev) = &prev {
                        let sample = diff(prev, &cur);
                        if let Err(err) = store.put(cur.at_ms, &sample).await {
                            log::warn!("runtime stats: failed to store sample: {err}");
                        }
                        samples += 1;
                        if samples % prune_every == 0 {
                            let before = now_ms() - retention_ms;
                            if let Err(err) = store.prune(before).await {
                                log::warn!("runtime stats: retention prune failed: {err}");
                            }
                        }
                    }
                    prev = Some(cur);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proc_stat_totals_and_idle() {
        // user nice system idle iowait irq softirq steal
        let (total, idle) = parse_proc_stat("cpu  100 0 50 800 40 0 10 0\ncpu0 1 2 3 4\n").unwrap();
        assert_eq!(total, 100 + 50 + 800 + 40 + 10);
        assert_eq!(idle, 800 + 40);
    }

    #[test]
    fn self_stat_handles_parens_in_comm() {
        // comm is "(rust s3)" with a space and parens; utime=14th, stime=15th.
        let line = "123 (rust s3) S 1 1 1 0 -1 0 0 0 0 0 700 300 0 0 20 0 1 0";
        assert_eq!(parse_self_stat(line), Some(1000));
    }

    #[test]
    fn meminfo_used_is_total_minus_available() {
        let text = "MemTotal:       1000 kB\nMemFree: 100 kB\nMemAvailable:    400 kB\n";
        let (total, avail) = parse_meminfo(text).unwrap();
        assert_eq!(total, 1000 * 1024);
        assert_eq!(avail, 400 * 1024);
    }

    #[test]
    fn vmrss_in_bytes() {
        assert_eq!(parse_vmrss("VmPeak: 999 kB\nVmRSS:\t  2048 kB\n"), Some(2048 * 1024));
    }

    #[test]
    fn self_io_read_write() {
        let text = "rchar: 1\nwchar: 2\nread_bytes: 4096\nwrite_bytes: 8192\n";
        assert_eq!(parse_self_io(text), Some((4096, 8192)));
    }

    #[test]
    fn diskstats_sums_whole_disks_only() {
        // sda (whole) counts; sda1 (partition) and loop0 are ignored.
        let text = "\
   8       0 sda 10 0 100 5 20 0 200 8 0 12 13
   8       1 sda1 5 0 50 2 10 0 100 4 0 6 7
   7       0 loop0 1 0 999 0 0 0 999 0 0 0 0
 259       0 nvme0n1 1 0 8 0 1 0 16 0 0 0 0";
        let (r, w) = parse_diskstats(text).unwrap();
        // sectors_read: sda 100 + nvme0n1 8 = 108; sectors_written: 200 + 16 = 216.
        assert_eq!(r, 108 * 512);
        assert_eq!(w, 216 * 512);
    }

    #[test]
    fn diff_computes_comparable_cpu_and_rates() {
        let prev = Raw {
            at_ms: 0,
            cpu_total: Some(1000),
            cpu_idle: Some(1000),
            proc_cpu: Some(0),
            mem_used: Some(500),
            mem_total: Some(2000),
            proc_rss: Some(100),
            proc_io_r: Some(0),
            proc_io_w: Some(0),
            disk_r: Some(0),
            disk_w: Some(0),
            net_in: 0,
            net_out: 0,
            reqs: 0,
        };
        let cur = Raw {
            at_ms: 1000, // 1 second later
            cpu_total: Some(1100), // +100 total
            cpu_idle: Some(1080),  // +80 idle → 20 busy
            proc_cpu: Some(10),    // +10 of the 100 total → 10%
            mem_used: Some(600),
            mem_total: Some(2000),
            proc_rss: Some(120),
            proc_io_r: Some(4096),
            proc_io_w: Some(2048),
            disk_r: Some(10 * 512),
            disk_w: Some(20 * 512),
            net_in: 1000,
            net_out: 500,
            reqs: 30,
        };
        let s = diff(&prev, &cur);
        assert_eq!(s.cpu_sys, Some(20.0));
        assert_eq!(s.cpu_proc, Some(10.0));
        assert_eq!(s.mem_used, Some(600.0));
        assert_eq!(s.disk_proc_r, Some(4096.0));
        assert_eq!(s.disk_proc_w, Some(2048.0));
        assert_eq!(s.disk_sys_r, Some((10 * 512) as f64));
        assert_eq!(s.net_in, Some(1000.0));
        assert_eq!(s.net_out, Some(500.0));
        assert_eq!(s.qps, Some(30.0));
    }

    #[test]
    fn diff_yields_gaps_when_sources_absent() {
        let base = Raw {
            at_ms: 0,
            cpu_total: None,
            cpu_idle: None,
            proc_cpu: None,
            mem_used: None,
            mem_total: None,
            proc_rss: None,
            proc_io_r: None,
            proc_io_w: None,
            disk_r: None,
            disk_w: None,
            net_in: 0,
            net_out: 0,
            reqs: 0,
        };
        let mut cur = base.clone();
        cur.at_ms = 1000;
        let s = diff(&base, &cur);
        assert_eq!(s.cpu_sys, None);
        assert_eq!(s.cpu_proc, None);
        assert_eq!(s.mem_used, None);
        assert_eq!(s.disk_proc_r, None);
        // Network/QPS come from in-process counters, always present.
        assert_eq!(s.net_in, Some(0.0));
        assert_eq!(s.qps, Some(0.0));
    }
}
