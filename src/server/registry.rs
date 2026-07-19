//! Server-wide registry of in-flight work.
//!
//! Every verb — an HTTP request handler or a scheduled job — registers itself
//! here while it runs and is removed when it finishes. Registration returns a
//! [`TaskGuard`] whose `Drop` does the removal, so a task is deregistered on
//! success, on error, and even on panic (unwinding still runs the guard). This
//! gives one uniform, always-accurate view of what the server is doing right
//! now — for observability, the admin UI's "active tasks" panel, and graceful
//! shutdown draining.
//!
//! A running task can update its own [`TaskProgress`] (bytes done / total) so
//! the UI renders live status like `54MiB/89MiB (75%)`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use crate::server::event_hub::{Event, EventHub};
use crate::storage::time::now_ms;

/// How long a finished task is still reported in snapshots, so a client that
/// polls or reconnects still catches it. The client controls the actual
/// on-screen linger + fade-out (shorter); this is just the delivery window.
const COMPLETED_LINGER_MS: i64 = 5000;

/// Where a task came from: a client request or the server's own scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    /// An S3 API request handler.
    S3,
    /// A scheduler-triggered background job.
    Job,
}

impl TaskKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskKind::S3 => "s3",
            TaskKind::Job => "job",
        }
    }
}

/// The live, internally-mutable state of a running task. A task updates this
/// as it works; [`TaskProgress::render`] turns it into the one human string the
/// UI shows. Byte counters drive transfers (uploads/downloads); a free-form
/// note drives everything else (e.g. a job's `"deleted 6 dirs in trash"`).
/// `total == 0` means unknown.
#[derive(Debug, Default)]
pub struct TaskProgress {
    done: AtomicU64,
    total: AtomicU64,
    note: Mutex<Option<String>>,
}

impl TaskProgress {
    pub fn set_total(&self, total: u64) {
        self.total.store(total, Ordering::Relaxed);
    }

    pub fn add_done(&self, delta: u64) {
        self.done.fetch_add(delta, Ordering::Relaxed);
    }

    /// Sets a free-form status line (overrides the byte rendering).
    pub fn set_note(&self, note: impl Into<String>) {
        *self.note.lock().unwrap() = Some(note.into());
    }

    /// `(done, total)` — `total == 0` means the total is unknown/not
    /// applicable (drive an indeterminate bar, or none).
    pub fn read(&self) -> (u64, u64) {
        (
            self.done.load(Ordering::Relaxed),
            self.total.load(Ordering::Relaxed),
        )
    }

    /// Renders the current state as a single human status string.
    pub fn render(&self) -> String {
        if let Some(note) = self.note.lock().unwrap().clone() {
            return note;
        }
        let done = self.done.load(Ordering::Relaxed);
        let total = self.total.load(Ordering::Relaxed);
        if total > 0 {
            let pct = ((done as f64 / total as f64) * 100.0).round() as u64;
            format!("{}/{} ({}%)", human_bytes(done), human_bytes(total), pct.min(100))
        } else if done > 0 {
            human_bytes(done)
        } else {
            String::new()
        }
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}B")
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

struct TaskEntry {
    id: String,
    kind: TaskKind,
    op: String,
    target: String,
    started_at_ms: i64,
    progress: Arc<TaskProgress>,
    cancel: CancellationToken,
    cancellable: bool,
}

/// The outcome of a cancel request, so the caller can answer the client
/// precisely (found and cancelled / found but not cancellable / not found).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelResult {
    Cancelled(usize),
    Refused,
    NotFound,
}

/// A point-in-time view of one active task, safe to serialize for the UI. The
/// `status` is the task's own single-line rendering (the UI truncates it with
/// an ellipsis if it's too wide).
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub id: String,
    pub kind: TaskKind,
    pub op: String,
    pub target: String,
    pub started_at_ms: i64,
    pub status: String,
    pub cancellable: bool,
    /// Progress counters for a bar; `total == 0` = indeterminate.
    pub done: u64,
    pub total: u64,
    /// True for a just-finished task that is lingering before it vanishes.
    pub completed: bool,
}

struct RecentTask {
    snapshot: TaskSnapshot,
    ended_at_ms: i64,
}

/// The set of currently-running tasks, plus recently-finished ones that linger
/// briefly so the UI can show them complete before they vanish.
pub struct TaskRegistry {
    inner: Mutex<HashMap<u64, TaskEntry>>,
    recent: Mutex<Vec<RecentTask>>,
    seq: AtomicU64,
    hub: Arc<EventHub>,
}

impl TaskRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            recent: Mutex::new(Vec::new()),
            seq: AtomicU64::new(0),
            hub: EventHub::new(),
        })
    }

    /// Subscribe to task entry/exit notifications (drives the console
    /// WebSocket). Every open session gets every event.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Event> {
        self.hub.subscribe()
    }

    /// Publishes an arbitrary event onto the registry's hub. Lets callers that
    /// already hold the registry (e.g. the console pipeline) emit audit events
    /// through the same bus that carries task changes, without threading the
    /// [`EventHub`] separately.
    pub fn publish(&self, event: Event) {
        self.hub.publish(event);
    }

    /// Registers a cancellable task (the default) and returns a guard that
    /// deregisters it on drop.
    pub fn register(
        self: &Arc<Self>,
        id: impl Into<String>,
        kind: TaskKind,
        op: impl Into<String>,
        target: impl Into<String>,
    ) -> TaskGuard {
        self.register_inner(id, kind, op, target, true)
    }

    /// Registers a task that the operator may not cancel (e.g. an index
    /// rebuild, where a mid-flight abort just re-triggers on next access).
    pub fn register_uncancellable(
        self: &Arc<Self>,
        id: impl Into<String>,
        kind: TaskKind,
        op: impl Into<String>,
        target: impl Into<String>,
    ) -> TaskGuard {
        self.register_inner(id, kind, op, target, false)
    }

    fn register_inner(
        self: &Arc<Self>,
        id: impl Into<String>,
        kind: TaskKind,
        op: impl Into<String>,
        target: impl Into<String>,
        cancellable: bool,
    ) -> TaskGuard {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let id = id.into();
        let op = op.into();
        let progress = Arc::new(TaskProgress::default());
        let cancel = CancellationToken::new();
        let active = {
            let mut map = self.inner.lock().unwrap();
            map.insert(
                seq,
                TaskEntry {
                    id: id.clone(),
                    kind,
                    op: op.clone(),
                    target: target.into(),
                    started_at_ms: now_ms(),
                    progress: Arc::clone(&progress),
                    cancel: cancel.clone(),
                    cancellable,
                },
            );
            map.len()
        };
        log::debug!("[{id}] task+ {}:{op} active={active}", kind.as_str());
        // Entry event: consumers (the console WS) learn of it immediately.
        self.hub.publish(Event::TasksChanged);
        TaskGuard {
            registry: Arc::clone(self),
            seq,
            id,
            kind,
            op,
            progress,
            cancel,
        }
    }

    /// Cooperatively requests cancellation of every active task with this
    /// correlation id. Non-cancellable tasks are refused, never signalled. The
    /// task itself decides when to observe the signal; this never force-aborts.
    pub fn cancel(&self, id: &str) -> CancelResult {
        let map = self.inner.lock().unwrap();
        let mut matched = 0;
        let mut cancelled = 0;
        for entry in map.values() {
            if entry.id == id {
                matched += 1;
                if entry.cancellable {
                    entry.cancel.cancel();
                    cancelled += 1;
                }
            }
        }
        if matched == 0 {
            CancelResult::NotFound
        } else if cancelled == 0 {
            CancelResult::Refused
        } else {
            log::info!("task cancel requested id={id} tasks={cancelled}");
            CancelResult::Cancelled(cancelled)
        }
    }

    /// Active tasks plus recently-finished ones still within the linger window,
    /// oldest first. Finished tasks carry `completed = true`.
    pub fn snapshot(&self) -> Vec<TaskSnapshot> {
        let mut tasks: Vec<TaskSnapshot> = self
            .inner
            .lock()
            .unwrap()
            .values()
            .map(|entry| {
                let (done, total) = entry.progress.read();
                TaskSnapshot {
                    id: entry.id.clone(),
                    kind: entry.kind,
                    op: entry.op.clone(),
                    target: entry.target.clone(),
                    started_at_ms: entry.started_at_ms,
                    status: entry.progress.render(),
                    cancellable: entry.cancellable,
                    done,
                    total,
                    completed: false,
                }
            })
            .collect();
        let now = now_ms();
        for recent in self.recent.lock().unwrap().iter() {
            if now - recent.ended_at_ms < COMPLETED_LINGER_MS {
                tasks.push(recent.snapshot.clone());
            }
        }
        tasks.sort_by_key(|task| task.started_at_ms);
        tasks
    }

    pub fn active_count(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

/// Deregisters its task when dropped — success, error, or panic.
pub struct TaskGuard {
    registry: Arc<TaskRegistry>,
    seq: u64,
    id: String,
    kind: TaskKind,
    op: String,
    progress: Arc<TaskProgress>,
    cancel: CancellationToken,
}

impl TaskGuard {
    /// Shared progress handle — update it as work proceeds to drive live UI.
    pub fn progress(&self) -> Arc<TaskProgress> {
        Arc::clone(&self.progress)
    }

    /// True once this task has been asked to cancel (via the UI/`cancel`).
    /// Long-running work should poll this at safe checkpoints and stop.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// The task's cancellation token, to hand to work that already takes one
    /// (e.g. the rebuild engine).
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        let (entry, active) = {
            let mut map = self.registry.inner.lock().unwrap();
            let entry = map.remove(&self.seq);
            (entry, map.len())
        };
        // Move it to the linger list so the UI can show it finished for a
        // moment before it disappears, then prune expired ones.
        if let Some(entry) = entry {
            let (done, total) = entry.progress.read();
            let snapshot = TaskSnapshot {
                id: entry.id,
                kind: entry.kind,
                op: entry.op,
                target: entry.target,
                started_at_ms: entry.started_at_ms,
                status: entry.progress.render(),
                cancellable: false,
                done,
                total,
                completed: true,
            };
            let now = now_ms();
            let mut recent = self.registry.recent.lock().unwrap();
            recent.retain(|r| now - r.ended_at_ms < COMPLETED_LINGER_MS);
            recent.push(RecentTask { snapshot, ended_at_ms: now });
        }
        log::debug!("[{}] task- {}:{} active={active}", self.id, self.kind.as_str(), self.op);
        // Exit event: consumers learn immediately (and see it as completed).
        self.registry.hub.publish(Event::TasksChanged);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_registers_and_deregisters_with_progress() {
        let registry = TaskRegistry::new();
        assert_eq!(registry.active_count(), 0);
        {
            let guard = registry.register("rid", TaskKind::S3, "UPLOAD", "/b/k");
            guard.progress().set_total(100);
            guard.progress().add_done(75);
            assert_eq!(registry.active_count(), 1);
            let snap = registry.snapshot();
            assert_eq!(snap[0].op, "UPLOAD");
            assert_eq!(snap[0].status, "75B/100B (75%)");
        }
        assert_eq!(registry.active_count(), 0);
    }

    #[test]
    fn cancel_respects_cancellability() {
        let registry = TaskRegistry::new();
        let guard = registry.register("run-1", TaskKind::S3, "UPLOAD", "/b/k");
        assert!(!guard.is_cancelled());
        assert_eq!(registry.cancel("run-1"), CancelResult::Cancelled(1));
        assert!(guard.is_cancelled());
        // Unknown id.
        assert_eq!(registry.cancel("nope"), CancelResult::NotFound);
        // An uncancellable task is refused, never signalled.
        let rebuild =
            registry.register_uncancellable("run-2", TaskKind::Job, "rebuild_index", "/b");
        assert_eq!(registry.cancel("run-2"), CancelResult::Refused);
        assert!(!rebuild.is_cancelled());
    }

    #[test]
    fn deregisters_even_on_panic() {
        let registry = TaskRegistry::new();
        let reg = Arc::clone(&registry);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = reg.register("rid", TaskKind::Job, "sweep", "all-buckets");
            assert_eq!(reg.active_count(), 1);
            panic!("boom");
        }));
        assert!(result.is_err());
        assert_eq!(registry.active_count(), 0);
    }
}
