//! A tiny in-process publish/subscribe hub.
//!
//! Producers (e.g. the task registry) publish [`Event`]s; consumers (e.g. the
//! console WebSocket) subscribe and react. This keeps the producers unaware of
//! any transport — the registry just says "tasks changed," and whoever cares
//! listens. Backed by a `tokio::sync::broadcast` channel, so every subscriber
//! (every open console session) receives every event.

use std::sync::Arc;

use tokio::sync::broadcast;

/// Something worth notifying live consumers about. The console WebSocket only
/// reacts to [`Event::TasksChanged`]; [`Event::Audit`] rides the same bus so a
/// future external subscriber (webhooks/Kafka) can consume the full event
/// stream from one place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The set of in-flight tasks changed (one started or finished).
    TasksChanged,
    /// A security-relevant verb ran (or was denied). Emitted by the console
    /// pipeline and IAM management actions so every meaningful action is
    /// observable off one bus.
    Audit {
        /// Who performed the action (username).
        actor: String,
        /// The action or IAM action string (e.g. `create_bucket`, `s3:PutObject`).
        action: String,
        /// The resource the action targeted (e.g. `/bucket/key`).
        target: String,
        /// Whether policy allowed it (`false` = denied).
        allowed: bool,
        /// Correlation id shared with the task and access-log line.
        request_id: String,
    },
}

/// Fan-out event bus. Cheap to clone (it's just the sender handle).
#[derive(Clone)]
pub struct EventHub {
    tx: broadcast::Sender<Event>,
}

impl EventHub {
    pub fn new() -> Arc<Self> {
        // A small buffer; a lagging subscriber just gets `Lagged` and refreshes
        // from a full snapshot, so no event is ever truly "missed" in effect.
        let (tx, _) = broadcast::channel(64);
        Arc::new(Self { tx })
    }

    /// Publishes an event to all current subscribers. A no-op if nobody is
    /// listening.
    pub fn publish(&self, event: Event) {
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }
}
