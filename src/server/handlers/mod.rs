//! S3 request handlers — one self-contained module per verb.
//!
//! Design rules:
//! - The router resolves a request into an [`ObjectCtx`] (the clean handover
//!   type) and dispatches to exactly one verb module.
//! - Verbs **never** call each other. Anything shared lives in the parent
//!   `server` module (the "library", reached via `crate::server::…`). Deleting
//!   one verb module can never break another.

use std::collections::HashMap;

use axum::http::{HeaderMap, Method};

use std::sync::Arc;

use crate::server::auth::AuthState;
use crate::server::identity::Identity;
use crate::server::registry::TaskRegistry;

// Each verb lives in its own directory (`<verb>/lib.rs`) so it can be read,
// tested, and even deleted in isolation.

// ── object-scoped verbs ──
#[path = "abort_multipart/lib.rs"]
pub(crate) mod abort_multipart;
#[path = "complete_multipart/lib.rs"]
pub(crate) mod complete_multipart;
#[path = "copy_object/lib.rs"]
pub(crate) mod copy_object;
#[path = "copy_part/lib.rs"]
pub(crate) mod copy_part;
#[path = "create_multipart/lib.rs"]
pub(crate) mod create_multipart;
#[path = "delete_object/lib.rs"]
pub(crate) mod delete_object;
#[path = "get_object/lib.rs"]
pub(crate) mod get_object;
#[path = "list_parts/lib.rs"]
pub(crate) mod list_parts;
#[path = "put_object/lib.rs"]
pub(crate) mod put_object;
#[path = "put_part/lib.rs"]
pub(crate) mod put_part;

// ── bucket-scoped verbs ──
#[path = "browser_post/lib.rs"]
pub(crate) mod browser_post;
#[path = "create_bucket/lib.rs"]
pub(crate) mod create_bucket;
#[path = "delete_bucket/lib.rs"]
pub(crate) mod delete_bucket;
#[path = "delete_objects/lib.rs"]
pub(crate) mod delete_objects;
#[path = "get_bucket_location/lib.rs"]
pub(crate) mod get_bucket_location;
#[path = "head_bucket/lib.rs"]
pub(crate) mod head_bucket;
#[path = "list_objects/lib.rs"]
pub(crate) mod list_objects;
#[path = "list_uploads/lib.rs"]
pub(crate) mod list_uploads;
#[path = "list_versions/lib.rs"]
pub(crate) mod list_versions;
#[path = "rebuild_index/lib.rs"]
pub(crate) mod rebuild_index;

/// Everything a verb needs about one object-scoped S3 request, resolved once
/// by the router before hand-off. Carrying the request id and identity here
/// keeps every handler traceable and able to authorize without re-parsing.
pub(crate) struct ObjectCtx {
    pub request_id: String,
    #[allow(dead_code)]
    pub identity: Option<Identity>,
    pub bucket: String,
    pub key: String,
    pub query: HashMap<String, String>,
    pub headers: HeaderMap,
    pub method: Method,
}

impl ObjectCtx {
    /// The `/bucket/key` resource string used in S3 error and log payloads.
    pub fn resource(&self) -> String {
        format!("/{}/{}", self.bucket, self.key)
    }
}

/// Everything a bucket-scoped verb needs, resolved once by the router. Carries
/// the identity (for body-aware authorization like multi-object delete) and the
/// auth state (for browser-POST form-signature verification).
pub(crate) struct BucketCtx {
    pub request_id: String,
    pub identity: Option<Identity>,
    pub auth_state: Option<AuthState>,
    /// The in-flight task registry, for verbs that launch their own jobs (e.g.
    /// rebuild-index). `None` only on code paths without a running server.
    pub tasks: Option<Arc<TaskRegistry>>,
    pub bucket: String,
    pub query: HashMap<String, String>,
    pub headers: HeaderMap,
    pub method: Method,
}

impl BucketCtx {
    /// The `/bucket` resource string used in S3 error and log payloads.
    pub fn resource(&self) -> String {
        format!("/{}", self.bucket)
    }
}
