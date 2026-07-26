//! Job: the storage health scan, and the repairs an operator drives from its
//! report.
//!
//! A scan is a verb like any other — it registers in the task registry, so it
//! shows up in the live task panel with its progress line and can be cancelled
//! from there exactly like an upload. Unlike the sweeper jobs it is never
//! scheduled: it is expensive (a full walk of every object dir) and only an
//! admin starts one.
//!
//! It also carries its own broadcast channel. The task registry's status line
//! is one string; the Performance page wants structured per-phase counters
//! (which bucket, how many objects visited of how many, findings so far), so
//! this module owns a [`ScanService`] that live runs publish into and the
//! console WebSocket subscribes to. Late joiners get the current state on
//! connect, so opening the page mid-scan shows progress immediately.
//!
//! Findings are never accumulated: the scan engine streams them down a bounded
//! channel, and a persister task here writes them to RocksDB in batches. Memory
//! is flat regardless of how broken the store turns out to be.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::broadcast;

use crate::server::registry::{TaskKind, TaskRegistry};
use crate::server::scan_store::{new_report_id, ScanReport, ScanStatus, ScanStore};
use crate::storage::scan::{BucketReport, Finding, FindingState, RepairAction, ScanProgress};
use crate::storage::store::LocalObjectStore;
use crate::storage::time::now_ms;

pub(crate) const JOB: &str = "storage_scan";
pub(crate) const REPAIR_JOB: &str = "storage_repair";

/// How often live progress is broadcast. Fast enough to feel immediate,
/// slow enough that a scan visiting thousands of objects a second doesn't
/// spend its time serializing JSON.
const PUBLISH_EVERY: Duration = Duration::from_millis(250);

/// Findings buffered before a write. Bounds the persister's memory and keeps
/// RocksDB writes batched.
const FINDING_BATCH: usize = 256;

/// Backpressure bound on the finding channel: if the persister falls behind,
/// the walk waits rather than queueing without limit.
const FINDING_QUEUE: usize = 1024;

/// One thing running right now — a scan, or a repair pass over its findings.
struct LiveRun {
    /// `scan` or `repair`.
    kind: &'static str,
    report_id: String,
    /// Correlation id, so the UI can cancel through `/api/tasks/:id/cancel`.
    task_id: String,
    started_at_ms: i64,
    progress: Arc<ScanProgress>,
    /// Repair runs only: items done / total.
    repaired: Arc<std::sync::atomic::AtomicU64>,
    total: u64,
}

/// Owns scan history and the live-progress channel. One scan or repair runs at
/// a time — both walk the same trees, and both are expensive enough that
/// serialising them is a feature, not a limitation.
pub struct ScanService {
    store: ScanStore,
    hub: broadcast::Sender<Value>,
    live: Mutex<Option<LiveRun>>,
}

impl std::fmt::Debug for ScanService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanService").finish_non_exhaustive()
    }
}

impl ScanService {
    pub fn new(store: ScanStore) -> Arc<Self> {
        let (hub, _) = broadcast::channel(64);
        Arc::new(Self {
            store,
            hub,
            live: Mutex::new(None),
        })
    }

    pub fn store(&self) -> &ScanStore {
        &self.store
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.hub.subscribe()
    }

    /// The current state, as sent to a client the moment it connects.
    pub fn snapshot(&self) -> Value {
        let live = self.live.lock().unwrap();
        let Some(run) = live.as_ref() else {
            return json!({ "type": "idle" });
        };
        let progress = run.progress.snapshot();
        json!({
            "type": run.kind,
            "report_id": run.report_id,
            "task_id": run.task_id,
            "started_at_ms": run.started_at_ms,
            "now_ms": now_ms(),
            "progress": progress,
            "repaired": run.repaired.load(Ordering::Relaxed),
            "total": run.total,
        })
    }

    /// Whether something is already running, and what.
    pub fn busy(&self) -> Option<String> {
        self.live.lock().unwrap().as_ref().map(|r| r.kind.to_string())
    }

    /// The report a run is currently writing into, if any — so a bulk history
    /// delete can leave it alone.
    pub fn running_report_id(&self) -> Option<String> {
        self.live.lock().unwrap().as_ref().map(|r| r.report_id.clone())
    }

    /// Claims the single run slot. Returns false if a scan or repair already
    /// holds it — scans must never overlap: two walks of the same trees compete
    /// for the same disks, and two runs writing findings into two reports of
    /// the same bucket is a muddle nobody can read.
    fn begin(&self, run: LiveRun) -> bool {
        let mut live = self.live.lock().unwrap();
        if live.is_some() {
            return false;
        }
        *live = Some(run);
        true
    }

    fn end(&self) {
        *self.live.lock().unwrap() = None;
    }

    fn publish(&self, value: Value) {
        // A send with no subscribers is not an error — nobody is watching.
        let _ = self.hub.send(value);
    }
}

/// Releases the single run slot when dropped — on success, on error, and on
/// panic. Without this, a panicking run would leave the service permanently
/// "busy" and every later scan would be refused until a restart.
struct LiveGuard(Arc<ScanService>);

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.0.end();
    }
}

/// Starts a scan of `buckets`. Returns the new report id, or `None` if a scan
/// or repair is already running.
pub(crate) fn spawn(
    store: LocalObjectStore,
    service: Arc<ScanService>,
    tasks: Arc<TaskRegistry>,
    buckets: Vec<String>,
    actor: String,
) -> Option<String> {
    let report_id = new_report_id();
    let task_id = crate::server::new_request_id();
    let progress = Arc::new(ScanProgress::default());
    progress
        .buckets_total
        .store(buckets.len() as u64, Ordering::Relaxed);
    *progress.phase.lock().unwrap() = "starting".to_string();

    if !service.begin(LiveRun {
        kind: "scan",
        report_id: report_id.clone(),
        task_id: task_id.clone(),
        started_at_ms: now_ms(),
        progress: Arc::clone(&progress),
        repaired: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        total: buckets.len() as u64,
    }) {
        return None;
    }

    let returned = report_id.clone();
    tokio::spawn(async move {
        // Frees the run slot however this task ends — the guarantee that scans
        // never overlap is only as good as the release path.
        let _live = LiveGuard(Arc::clone(&service));
        // The task guard makes the scan visible and cancellable in the console's
        // task list; dropping it deregisters even on panic.
        let guard = tasks.register(
            &task_id,
            TaskKind::Job,
            JOB,
            format!("{} bucket{}", buckets.len(), if buckets.len() == 1 { "" } else { "s" }),
        );
        let cancel = guard.cancel_token();
        log::info!("[{task_id}] {JOB} started report={report_id} buckets={}", buckets.len());

        let mut report = ScanReport::new(report_id.clone(), actor, buckets.clone());
        if let Err(err) = service.store().put_report(&report).await {
            log::error!("[{task_id}] {JOB} could not write report={report_id} error={err}");
        }

        // Broadcast + task-status poller. Aborted by its guard if the body
        // unwinds, so a panic can never leak a task spinning on the timer.
        struct PollerGuard(tokio::task::JoinHandle<()>);
        impl Drop for PollerGuard {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let poller = {
            let service = Arc::clone(&service);
            let progress = Arc::clone(&progress);
            let task_progress = guard.progress();
            tokio::spawn(async move {
                loop {
                    let snapshot = progress.snapshot();
                    task_progress.set_note(render_status(&snapshot));
                    // The bar tracks the bucket being scanned, not the run, so
                    // it moves even on a single huge bucket.
                    task_progress.set_total(snapshot.objects_total);
                    service.publish(service.snapshot());
                    tokio::time::sleep(PUBLISH_EVERY).await;
                }
            })
        };
        let poller = PollerGuard(poller);

        // Persister: batches findings from the engine into RocksDB. Owns the
        // receiving end, so the engine's sends apply backpressure to the walk.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Finding>(FINDING_QUEUE);
        let persister = {
            let scan_store = service.store().clone();
            let report_id = report_id.clone();
            tokio::spawn(async move {
                let mut batch: Vec<Finding> = Vec::with_capacity(FINDING_BATCH);
                let mut written = 0u64;
                loop {
                    let received = rx.recv().await;
                    let closed = received.is_none();
                    if let Some(finding) = received {
                        batch.push(finding);
                    }
                    if batch.len() >= FINDING_BATCH || (closed && !batch.is_empty()) {
                        written += batch.len() as u64;
                        if let Err(err) = scan_store
                            .append_findings(&report_id, std::mem::take(&mut batch))
                            .await
                        {
                            log::error!("{JOB} could not persist findings report={report_id} error={err}");
                        }
                        batch = Vec::with_capacity(FINDING_BATCH);
                    }
                    if closed {
                        break;
                    }
                }
                written
            })
        };

        let mut status = ScanStatus::Completed;
        let mut error = None;
        for bucket in &buckets {
            if cancel.is_cancelled() || store.shutdown_token().is_cancelled() {
                status = ScanStatus::Cancelled;
                error = Some("cancelled by operator".to_string());
                break;
            }
            match store.scan_bucket(bucket, &cancel, &progress, &tx).await {
                Ok(bucket_report) => report.absorb(bucket_report),
                Err(err) => {
                    if cancel.is_cancelled() || store.shutdown_token().is_cancelled() {
                        status = ScanStatus::Cancelled;
                        error = Some("cancelled by operator".to_string());
                        break;
                    }
                    // One unscannable bucket (e.g. an index awaiting rebuild)
                    // must not sink the whole run — record it and carry on.
                    log::warn!("[{task_id}] {JOB} bucket failed bucket={bucket} error={err}");
                    report.absorb(BucketReport {
                        bucket: bucket.clone(),
                        error: Some(err.to_string()),
                        ..BucketReport::default()
                    });
                    progress.buckets_done.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Close the sink so the persister drains and finishes.
        drop(tx);
        match persister.await {
            Ok(written) => log::info!("[{task_id}] {JOB} persisted findings={written} report={report_id}"),
            Err(err) => log::error!("[{task_id}] {JOB} persister panicked report={report_id} error={err}"),
        }
        drop(poller);

        report.status = status;
        report.error = error;
        report.finished_at_ms = now_ms();
        if let Err(err) = service.store().put_report(&report).await {
            log::error!("[{task_id}] {JOB} could not finalize report={report_id} error={err}");
        }
        log::info!(
            "[{task_id}] {JOB} finished report={report_id} status={:?} findings={} buckets={}",
            report.status,
            report.findings_total,
            report.buckets.len()
        );

        service.publish(json!({
            "type": "finished",
            "report_id": report_id,
            "status": report.status,
            "findings_total": report.findings_total,
        }));
    });
    Some(returned)
}

/// Runs a batch of repairs against findings of one report. Each is re-verified
/// by the storage layer before it acts, so a report of any age is safe to drive
/// from; the outcome is written back onto the finding.
pub(crate) fn spawn_repair(
    store: LocalObjectStore,
    service: Arc<ScanService>,
    tasks: Arc<TaskRegistry>,
    report_id: String,
    items: Vec<(String, RepairAction)>,
) -> Option<String> {
    let task_id = crate::server::new_request_id();
    let repaired = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let total = items.len() as u64;
    if !service.begin(LiveRun {
        kind: "repair",
        report_id: report_id.clone(),
        task_id: task_id.clone(),
        started_at_ms: now_ms(),
        progress: Arc::new(ScanProgress::default()),
        repaired: Arc::clone(&repaired),
        total,
    }) {
        return None;
    }

    let returned = task_id.clone();
    tokio::spawn(async move {
        let _live = LiveGuard(Arc::clone(&service));
        let guard = tasks.register(
            &task_id,
            TaskKind::Job,
            REPAIR_JOB,
            format!("{total} finding{}", if total == 1 { "" } else { "s" }),
        );
        let cancel = guard.cancel_token();
        guard.progress().set_total(total);
        log::info!("[{task_id}] {REPAIR_JOB} started report={report_id} items={total}");

        let mut counts = (0u64, 0u64, 0u64); // repaired, stale, failed
        for (key, action) in items {
            if cancel.is_cancelled() || store.shutdown_token().is_cancelled() {
                log::info!("[{task_id}] {REPAIR_JOB} cancelled report={report_id}");
                break;
            }
            let finding = match service.store().get_finding(&key).await {
                Ok(Some(finding)) => finding,
                Ok(None) => continue, // the report row was deleted under us
                Err(err) => {
                    log::warn!("[{task_id}] {REPAIR_JOB} could not read finding key={key} error={err}");
                    continue;
                }
            };
            if finding.state == FindingState::Repaired {
                continue; // already done, don't act twice
            }
            let (state, message) = store.repair_finding(&finding, action).await;
            match state {
                FindingState::Repaired => counts.0 += 1,
                FindingState::Stale => counts.1 += 1,
                _ => counts.2 += 1,
            }
            if let Err(err) = service.store().record_outcome(&key, state, message.clone()).await {
                log::warn!("[{task_id}] {REPAIR_JOB} could not record outcome key={key} error={err}");
            }
            log::info!(
                "[{task_id}] {REPAIR_JOB} {} action={} bucket={} key={:?} dir={:?} outcome={message}",
                finding.kind.as_str(),
                action.as_str(),
                finding.bucket,
                finding.object_key,
                finding.blob_dir,
            );
            let done = repaired.fetch_add(1, Ordering::Relaxed) + 1;
            guard.progress().add_done(1);
            guard
                .progress()
                .set_note(format!("repairing {done}/{total} findings"));
            service.publish(service.snapshot());
        }

        log::info!(
            "[{task_id}] {REPAIR_JOB} finished report={report_id} repaired={} stale={} failed={}",
            counts.0, counts.1, counts.2
        );
        service.publish(json!({
            "type": "repair_finished",
            "report_id": report_id,
            "repaired": counts.0,
            "stale": counts.1,
            "failed": counts.2,
        }));
    });
    Some(returned)
}

/// The one-line status the task registry shows — the same shape the operator
/// asked for: what we're doing, and how far through.
fn render_status(p: &crate::storage::scan::ScanProgressSnapshot) -> String {
    let bucket = if p.bucket.is_empty() {
        String::new()
    } else {
        format!("{} · ", p.bucket)
    };
    let phase = match p.phase.as_str() {
        "index" => format!("reading index ({} rows)", thousands(p.objects_visited)),
        "disk" => {
            if p.objects_total > 0 {
                format!(
                    "scanning disk usage ({}/{} objects visited)",
                    thousands(p.objects_visited),
                    thousands(p.objects_total)
                )
            } else {
                format!("scanning disk usage ({} objects visited)", thousands(p.objects_visited))
            }
        }
        "verify" => "verifying candidates".to_string(),
        "reconcile" => "reconciling index against disk".to_string(),
        "usage" => "measuring trash and staging".to_string(),
        other => other.to_string(),
    };
    let findings = if p.findings > 0 {
        format!(" · {} finding{}", thousands(p.findings), if p.findings == 1 { "" } else { "s" })
    } else {
        String::new()
    };
    format!("{bucket}{phase}{findings}")
}

fn thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::scan::ScanProgressSnapshot;

    #[test]
    fn status_line_reads_like_the_operator_asked_for() {
        let snapshot = ScanProgressSnapshot {
            phase: "disk".into(),
            bucket: "photos".into(),
            objects_total: 1_204_900,
            objects_visited: 128_433,
            findings: 12,
            ..ScanProgressSnapshot::default()
        };
        assert_eq!(
            render_status(&snapshot),
            "photos · scanning disk usage (128,433/1,204,900 objects visited) · 12 findings"
        );
    }

    #[test]
    fn status_line_without_findings_or_totals() {
        let snapshot = ScanProgressSnapshot {
            phase: "reconcile".into(),
            bucket: "logs".into(),
            ..ScanProgressSnapshot::default()
        };
        assert_eq!(render_status(&snapshot), "logs · reconciling index against disk");
    }

    #[test]
    fn thousands_separates_groups() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1_000), "1,000");
        assert_eq!(thousands(1_204_900), "1,204,900");
    }

    #[tokio::test]
    async fn only_one_run_at_a_time() {
        let tmp = tempfile::tempdir().unwrap();
        let service = ScanService::new(ScanStore::open(tmp.path()).await.unwrap());
        let run = |kind| LiveRun {
            kind,
            report_id: "r".into(),
            task_id: "t".into(),
            started_at_ms: 0,
            progress: Arc::new(ScanProgress::default()),
            repaired: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            total: 0,
        };
        assert!(service.begin(run("scan")));
        assert!(!service.begin(run("repair")), "a second run must be refused");
        assert_eq!(service.busy().as_deref(), Some("scan"));
        service.end();
        assert!(service.busy().is_none());
        assert!(service.begin(run("repair")));
    }

    /// The slot must be released however a run ends. A panicking scan that
    /// left it claimed would refuse every later scan until a restart.
    #[tokio::test]
    async fn the_run_slot_is_released_even_when_a_run_panics() {
        let tmp = tempfile::tempdir().unwrap();
        let service = ScanService::new(ScanStore::open(tmp.path()).await.unwrap());
        assert!(service.begin(LiveRun {
            kind: "scan",
            report_id: "r".into(),
            task_id: "t".into(),
            started_at_ms: 0,
            progress: Arc::new(ScanProgress::default()),
            repaired: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            total: 0,
        }));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _live = LiveGuard(Arc::clone(&service));
            panic!("the scan blew up");
        }));
        assert!(result.is_err());
        assert!(service.busy().is_none(), "the slot must be free again");
    }

    #[tokio::test]
    async fn snapshot_is_idle_when_nothing_runs() {
        let tmp = tempfile::tempdir().unwrap();
        let service = ScanService::new(ScanStore::open(tmp.path()).await.unwrap());
        assert_eq!(service.snapshot()["type"], "idle");
    }
}
