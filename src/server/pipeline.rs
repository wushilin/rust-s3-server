//! The one logical verb pipeline, shared by every entry point.
//!
//! A "verb" (an S3 request or a console action) runs through three
//! cross-cutting steps: **authorize → register (broadcast) → execute**. On the
//! S3 API these are expressed by the middleware stack (`auth_middleware` +
//! `task_registry_middleware`). The console can't reuse that stack without
//! reconstructing an HTTP request, so it dispatches *logically* through the same
//! steps via [`begin`] — a direct, in-process call, no HTTP round-trip.
//!
//! The result: every console data verb authorizes with the *same* IAM identity
//! the S3 API uses, registers in the *same* task registry (so it flows through
//! the *same* event hub), and shares one correlation id with its access-log
//! line and audit event.

use std::sync::Arc;

use super::event_hub::Event;
use super::identity::Identity;
use super::logging::TARGET_AUTHZ;
use super::policy::Requirement;
use super::registry::{TaskGuard, TaskKind, TaskRegistry};

/// Returned when the caller's policy does not permit the verb. The HTTP entry
/// point maps this to `403 Forbidden`.
pub(crate) struct Denied;

/// Authorizes `identity` against `requirements`, and — if allowed — registers a
/// task so the verb is broadcast for its whole duration.
///
/// On **allow**: returns a [`TaskGuard`]; hold it across the work. Registration
/// broadcasts a "tasks changed" event immediately, and the guard's `Drop`
/// broadcasts completion — the caller does nothing extra.
///
/// On **deny**: logs to the authz channel, publishes an `Event::Audit`
/// (`allowed: false`) onto the hub, and returns [`Denied`] — no task is
/// registered.
pub(crate) fn begin(
    tasks: &Arc<TaskRegistry>,
    identity: &Identity,
    actor: &str,
    request_id: &str,
    kind: TaskKind,
    op: &'static str,
    target: String,
    requirements: &[Requirement],
) -> Result<TaskGuard, Denied> {
    if !identity.authorize(requirements) {
        for r in requirements {
            log::warn!(
                target: TARGET_AUTHZ,
                "[{request_id}] authz DENY actor={actor} action={} resource={}",
                r.action, r.resource,
            );
        }
        let action = requirements
            .first()
            .map(|r| r.action.to_string())
            .unwrap_or_else(|| op.to_string());
        tasks.publish(Event::Audit {
            actor: actor.to_string(),
            action,
            target,
            allowed: false,
            request_id: request_id.to_string(),
        });
        return Err(Denied);
    }
    // Log the allow decision too (Info, so it lands in authz.log at the default
    // level — unlike the S3 side's debug "authz ok"). Console traffic is
    // human-paced, so a per-verb authz line is a useful trail, not noise.
    let scope = if requirements.is_empty() {
        "unrestricted".to_string()
    } else {
        requirements.iter().map(|r| r.action).collect::<Vec<_>>().join(",")
    };
    log::info!(
        target: TARGET_AUTHZ,
        "[{request_id}] authz ok actor={actor} op={op} target={target} action={scope}",
    );
    Ok(tasks.register(request_id.to_string(), kind, op, target))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::event_hub::Event;
    use crate::server::identity::Identity;
    use crate::server::registry::{TaskKind, TaskRegistry};

    #[test]
    fn allow_registers_a_task_and_broadcasts() {
        let tasks = TaskRegistry::new();
        let mut rx = tasks.subscribe();
        let id = Identity::root(Some("root".into()), None);
        let reqs = [Requirement::bucket("s3:CreateBucket", "bucket")];
        let guard = begin(
            &tasks, &id, "root", "rid-1", TaskKind::S3, "CREATE_BUCKET", "/bucket".into(), &reqs,
        )
        .ok()
        .expect("root is unrestricted, must be allowed");
        assert_eq!(tasks.active_count(), 1, "allowed verb registers a task");
        assert!(matches!(rx.try_recv(), Ok(Event::TasksChanged)));
        drop(guard);
        assert_eq!(tasks.active_count(), 0, "guard drop deregisters");
    }

    #[test]
    fn deny_emits_an_audit_event_and_registers_nothing() {
        let tasks = TaskRegistry::new();
        let mut rx = tasks.subscribe();
        // An IAM identity with no policy denies everything.
        let id = Identity::iam("bob".into(), None);
        let reqs = [Requirement::object("s3:PutObject", "bucket", "k")];
        let res = begin(
            &tasks, &id, "bob", "rid-2", TaskKind::S3, "UPLOAD", "/bucket/k".into(), &reqs,
        );
        assert!(res.is_err(), "no policy must deny");
        assert_eq!(tasks.active_count(), 0, "a denied verb registers no task");
        match rx.try_recv() {
            Ok(Event::Audit { actor, allowed, request_id, action, .. }) => {
                assert_eq!(actor, "bob");
                assert!(!allowed, "deny must record allowed=false");
                assert_eq!(request_id, "rid-2");
                assert_eq!(action, "s3:PutObject");
            }
            other => panic!("expected a deny audit event, got {other:?}"),
        }
    }
}
