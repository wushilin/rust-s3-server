# Runtime Stats — Design

**Date:** 2026-07-26
**Status:** Approved (design), pending implementation plan

## Goal

Add a new admin-only **"Runtime Stats"** menu to the management console (port 8003)
showing overall system/process health as time-series line charts: CPU, memory,
disk IO, and S3 network throughput/QPS. Samples are taken every 5 seconds,
persisted in RocksDB, retained for 7 days, and rendered with period selection
and automatic downsampling so the UI never loads more than ~120 datapoints per
chart.

Non-goals: alerting, multi-node aggregation, exporting to Prometheus/StatsD,
per-bucket or per-key breakdowns. These are out of scope for this iteration.

## Metrics

One `Sample` is captured per 5-second tick and stores all series together
(one RocksDB row per timestamp — cheaper and simpler than a row per
`metric#timestamp`). Rates are computed at sample time by holding the previous
raw counters in memory, so stored values are ready to plot.

| Field | Source | Notes |
|---|---|---|
| `cpu_sys_pct` | `/proc/stat` (aggregate jiffie delta) | host-wide busy % |
| `cpu_proc_pct` | `/proc/self/stat` (utime+stime delta ÷ total ÷ ncpu) | rusts3 process |
| `mem_sys_used`, `mem_sys_total` | `/proc/meminfo` (`MemTotal`, `MemAvailable`) | used = total − available |
| `mem_proc_rss` | `/proc/self/status` (`VmRSS`) | rusts3 resident set |
| `disk_proc_read_bps`, `disk_proc_write_bps` | `/proc/self/io` (`read_bytes`, `write_bytes`) deltas | process disk IO |
| `disk_sys_read_bps`, `disk_sys_write_bps` | `/proc/diskstats` (sectors × 512) deltas, summed over real devices | host-wide disk IO |
| `net_in_bps`, `net_out_bps`, `qps` | `TrafficMetrics::snapshot()` deltas | reused free from the existing bandwidth loop (`mod.rs:816`) |

**Platform:** sampling reads Linux `/proc`. On non-Linux hosts the unavailable
fields are recorded as null and the charts render gaps — the feature never
panics or blocks startup. A single warning is logged the first time a source is
unavailable.

**Container caveat:** `/proc/stat` and `/proc/diskstats` are host-wide (not
cgroup-scoped), so `cpu_sys_*` and `disk_sys_*` reflect the whole host. The
process-scoped series (`/proc/self/*`) are exact. This is documented in the UI
subtitle/tooltip so operators aren't misled.

## Storage

New `StatsStore` module mirroring the existing `scan_store.rs`:

- Its own small RocksDB opened at `<base_dir>/stats`, kept separate from the
  object index and IAM DBs (same "one DB per concern" precedent as scan_store).
- Column family `samples`.
- Key: `format!("{:013}", now_ms())` — zero-padded Unix-millis, so keys sort
  chronologically (same pattern as `scan_store.rs:236`).
- Value: `serde_json` of the `Sample` struct (consistent with scan_store; at
  ~121k rows/week × ~150 bytes ≈ ~18 MB/week this is comfortably small).
- Writes use the existing `write_opts()` + `spawn_blocking` wrapper pattern.

**Retention:** a prune step deletes keys older than the retention window using
`delete_range_cf(0 .. now_ms()-retention)` (same technique as
`scan_store.rs:319`). It runs on a slow tick (hourly), not on every 5s sample.

## Sampler

A dedicated `tokio` task modeled on the existing bandwidth-report loop
(`mod.rs:816-858`): a `tokio::time::interval(sample_secs)` with a
`tokio::select!` on the shutdown token. Each tick:

1. Read `/proc` sources + `TrafficMetrics::snapshot()`.
2. Compute per-interval rates against the previous raw counters (held in the
   task's local state).
3. `StatsStore.put(now_ms(), sample)`.

The task is spawned during startup alongside the other maintenance jobs and
stops cleanly on the cancellation token.

## Configuration

New `stats:` section in the config (with env placeholders for the container
image, consistent with the rest of `config.docker.yaml`):

```yaml
stats:
  enabled: {{RUSTS3_STATS_ENABLED:true}}
  sample_secs: {{RUSTS3_STATS_SAMPLE_SECS:5}}
  retention_days: {{RUSTS3_STATS_RETENTION_DAYS:7}}
```

When `enabled: false` the sampler is not spawned, the store is not opened, and
the API endpoints return an empty/disabled response (the tab shows a
"stats disabled" note rather than erroring).

## API

All endpoints are **read-only GETs** on the UI router (`ui.rs router()`), served
on port 8003.

### Authentication & authorization (consistent + safe)

- Every `/api/stats/*` handler calls **`require_root(&state, &headers)`** as its
  first action — the exact same admin gate the Storage Scan tab uses
  (`ui.rs:288`). This returns **401** for no/expired session and **403** for a
  logged-in non-admin, via the shared session-cookie path (`session_of`).
- No S3-resource `Requirement`/`authorize()`/`begin_verb` is used: system stats
  are not an S3 resource and mutate nothing, so the resource-policy pipeline
  does not apply. The admin session check is the correct, minimal, consistent
  guard for operational data.
- Endpoints are **strictly read-only** — no state mutation, no task broadcast.
- The payload contains only aggregate host/process counters; **no secrets, keys,
  paths, or per-object data** are exposed.
- **Input validation:** `range` is whitelisted to the known enum
  (`15m|1h|6h|24h|7d`); anything else → 400. `points` is clamped to `1..=1000`
  (default 120) so a crafted request cannot force an unbounded scan or payload.
- **Defense in depth:** the nav item carries `data-admin-only` so non-admins
  never see the tab, but the server-side `require_root` is the actual
  enforcement — the client attribute is cosmetic only.
- The vendored uPlot JS/CSS are static assets served by `ui_asset` with no auth,
  consistent with every other embedded asset (they contain no data).

### Endpoint

```
GET /api/stats/series?range=<15m|1h|6h|24h|7d>&points=120
```

- **Downsampling (server-side, "avoid loading too many datapoints"):** the
  requested range is divided into exactly `points` equal time buckets (the
  validated value: default 120, clamped `1..=1000`). For each bucket the handler
  **seeks** to the first sample `>= bucket_start` (so ≈`points` RocksDB seeks
  total, not a 121k-row scan); if that sample's timestamp
  falls within the bucket it is used, otherwise the bucket is emitted as null
  (a gap). This keeps both the DB work and the response bounded regardless of
  range.
- **Response shape (µPlot-columnar):**
  ```json
  {
    "labels": ["time","cpu_sys","cpu_proc","mem_used","mem_proc_rss",
               "disk_proc_r","disk_proc_w","disk_sys_r","disk_sys_w",
               "net_in","net_out","qps"],
    "data": [[t0,t1,...],[..cpu_sys..],[..cpu_proc..], ...]
  }
  ```
  `data[0]` is the timestamp axis; each subsequent array aligns index-for-index.
  Empty buckets are `null` in every series so µPlot draws gaps.

## Frontend

Copies the existing "Storage Scan" (`perf`) tab pattern end-to-end:

- **Menu:** new `<button class="nav-item" data-tab="stats" data-admin-only>` in
  `ui.html`, plus a `<section id="tab_stats" class="page hidden">` body.
- **Wiring:** add `stats:['Runtime Stats','System & process health']` to
  `pageMeta` (`core.js:108`), extend the two hardcoded arrays in `showTab`
  (`core.js:110,112`), and dispatch `if(tab==='stats')initStats();`.
- **Assets:** vendor `uPlot.iife.min.js` (~50 KB) + `uPlot.min.css` into
  `src/server/assets/`, embed via `include_str!`, add `ui_asset` match arms and
  `<script>`/`<link>` tags in `ui.html`. Add `stats.js` the same way.
- **Page layout:** a period dropdown (`15m / 1h / 6h / 24h / 7d`) and four
  stacked line charts:
  1. **CPU %** — `cpu_sys`, `cpu_proc`
  2. **Memory** — `mem_used` (vs `mem_total`), `mem_proc_rss`
  3. **Disk IO (MB/s)** — process r/w + system r/w
  4. **Network** — `net_in`, `net_out` (MB/s) + `qps`
- **Refresh:** `initStats()` fetches `/api/stats/series` on load, on period
  change, and on a 5s timer while the tab is visible (the timer is cleared when
  another tab is shown, mirroring the existing `pingTimer` pattern in
  `core.js:101`).

## Testing

- **Unit — `/proc` parsers:** feed fixture strings for `/proc/stat`,
  `/proc/self/stat`, `/proc/meminfo`, `/proc/self/status`, `/proc/self/io`,
  `/proc/diskstats`; assert parsed counters and rate math (delta ÷ interval).
- **Unit — `StatsStore`:** put/range/get round-trip; retention prune removes
  only keys older than the window and leaves newer ones intact.
- **Unit — downsampling:** given N synthetic samples over a range, the bucketer
  returns ≤ `points` entries, correctly aligned, with gaps as null.
- **Integration:** with a running UI, `GET /api/stats/series` returns the
  documented columnar shape; an anonymous request gets 401 and a non-admin
  session gets 403 (auth regression guard).

## Files

**New**
- `src/server/stats_store.rs` — `StatsStore` (CF `samples`, put/range/prune).
- `src/server/sysstat.rs` — `/proc` readers + `Sample` + the 5s sampler task.
- `src/server/assets/stats.js` — tab logic + µPlot charts.
- `src/server/assets/uPlot.iife.min.js`, `src/server/assets/uPlot.min.css` — vendored.

**Touched**
- `src/server/config.rs` — `StatsConfig` + defaults.
- `src/server/mod.rs` — open `StatsStore`, spawn sampler, add to `UiState`.
- `src/server/ui.rs` — `/api/stats/series` handler + route, `ui_asset` arms.
- `src/server/ui.html` — nav item, `tab_stats` section, script/link tags.
- `src/server/assets/core.js` — `pageMeta` + `showTab` wiring.
- `config.docker.yaml` — `stats:` section with env placeholders.

## Follow-up (separate spec)

The `object_store` compatibility test folder requested alongside this feature is
an independent piece of work and will be specified separately — a self-contained
harness driving the Rust `object_store` crate's S3 client against a running
rusts3 instance. It is intentionally **not** part of this design.
