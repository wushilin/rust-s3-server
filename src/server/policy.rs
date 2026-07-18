//! S3-compatible IAM policy documents and evaluation.
//!
//! Policies are attached to IAM users and govern every access key belonging
//! to that user. Evaluation follows AWS semantics: explicit `Deny` wins,
//! then explicit `Allow`, default is deny. `Action` and `Resource` support
//! `*` and `?` wildcards; action names match case-insensitively.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyDocument {
    #[serde(rename = "Version", default = "default_version")]
    pub version: String,
    #[serde(rename = "Statement")]
    pub statement: Vec<Statement>,
}

fn default_version() -> String {
    "2012-10-17".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Statement {
    #[serde(rename = "Sid", default, skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Action")]
    pub action: OneOrMany,
    #[serde(rename = "Resource")]
    pub resource: OneOrMany,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

/// AWS policies allow both `"s3:GetObject"` and `["s3:GetObject", …]`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

impl OneOrMany {
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        match self {
            OneOrMany::One(v) => std::slice::from_ref(v).iter().map(String::as_str),
            OneOrMany::Many(v) => v[..].iter().map(String::as_str),
        }
    }
}

/// One authorization requirement derived from an incoming request:
/// the S3 action plus the ARN it targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requirement {
    pub action: &'static str,
    pub resource: String,
}

impl Requirement {
    pub fn bucket(action: &'static str, bucket: &str) -> Self {
        Self {
            action,
            resource: format!("arn:aws:s3:::{bucket}"),
        }
    }

    pub fn object(action: &'static str, bucket: &str, key: &str) -> Self {
        Self {
            action,
            resource: format!("arn:aws:s3:::{bucket}/{key}"),
        }
    }

    pub fn all_buckets(action: &'static str) -> Self {
        Self {
            action,
            resource: "arn:aws:s3:::*".to_string(),
        }
    }
}

/// Returns true when `policy` allows every requirement.
pub fn is_authorized(policy: &PolicyDocument, requirements: &[Requirement]) -> bool {
    requirements.iter().all(|r| evaluate(policy, r))
}

/// AWS evaluation order: any matching Deny → denied; otherwise any matching
/// Allow → allowed; otherwise denied.
pub fn evaluate(policy: &PolicyDocument, requirement: &Requirement) -> bool {
    let mut allowed = false;
    for statement in &policy.statement {
        let action_matches = statement
            .action
            .iter()
            .any(|pattern| wildcard_match_ci(pattern, requirement.action));
        if !action_matches {
            continue;
        }
        let resource_matches = statement
            .resource
            .iter()
            .any(|pattern| wildcard_match(pattern, &requirement.resource));
        if !resource_matches {
            continue;
        }
        match statement.effect {
            Effect::Deny => return false,
            Effect::Allow => allowed = true,
        }
    }
    allowed
}

/// Glob match supporting `*` (any run) and `?` (any single char).
pub fn wildcard_match(pattern: &str, value: &str) -> bool {
    wildcard_match_bytes(pattern.as_bytes(), value.as_bytes())
}

fn wildcard_match_ci(pattern: &str, value: &str) -> bool {
    wildcard_match(&pattern.to_ascii_lowercase(), &value.to_ascii_lowercase())
}

fn wildcard_match_bytes(pattern: &[u8], value: &[u8]) -> bool {
    // Iterative glob with backtracking over the last `*`.
    let (mut p, mut v) = (0usize, 0usize);
    let (mut star, mut star_v) = (None::<usize>, 0usize);
    while v < value.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == value[v]) {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            star_v = v;
            p += 1;
        } else if let Some(sp) = star {
            p = sp + 1;
            star_v += 1;
            v = star_v;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

// ── Request → requirements mapping ───────────────────────────────────────────

/// Derives the authorization requirements for an S3 API request. Returns
/// `None` for admin-only operations that IAM users may never perform
/// (e.g. `?rebuildIndex`).
pub fn requirements_for_request(
    method: &str,
    path: &str,
    query: &str,
    copy_source: Option<&str>,
) -> Option<Vec<Requirement>> {
    let has = |name: &str| {
        query
            .split('&')
            .any(|p| p == name || p.starts_with(&format!("{name}=")))
    };
    let path = path.trim_start_matches('/');
    let (bucket, key) = match path.split_once('/') {
        Some((b, k)) if !k.is_empty() => (b, Some(k)),
        Some((b, _)) => (b, None),
        None => (path, None),
    };

    if bucket.is_empty() {
        return Some(vec![Requirement::all_buckets("s3:ListAllMyBuckets")]);
    }
    if has("rebuildIndex") {
        return None; // admin-only
    }

    let decoded_key = key.map(|k| {
        urlencoding::decode(k)
            .map(|v| v.into_owned())
            .unwrap_or_else(|_| k.to_string())
    });

    let reqs = match (method, &decoded_key) {
        // ── bucket level ──
        ("PUT", None) => vec![Requirement::bucket("s3:CreateBucket", bucket)],
        ("DELETE", None) => vec![Requirement::bucket("s3:DeleteBucket", bucket)],
        ("HEAD", None) => vec![Requirement::bucket("s3:ListBucket", bucket)],
        ("GET", None) => {
            if has("uploads") {
                vec![Requirement::bucket("s3:ListBucketMultipartUploads", bucket)]
            } else if has("versions") {
                vec![Requirement::bucket("s3:ListBucketVersions", bucket)]
            } else {
                vec![Requirement::bucket("s3:ListBucket", bucket)]
            }
        }
        ("POST", None) if has("delete") => {
            // Batch delete: the request body lists the keys; require the
            // whole-bucket object delete permission.
            vec![Requirement::object("s3:DeleteObject", bucket, "*")]
        }
        ("POST", None) => vec![Requirement::object("s3:PutObject", bucket, "*")],
        // ── object level ──
        ("GET" | "HEAD", Some(k)) => {
            if has("uploadId") {
                vec![Requirement::object("s3:ListMultipartUploadParts", bucket, k)]
            } else {
                vec![Requirement::object("s3:GetObject", bucket, k)]
            }
        }
        ("PUT", Some(k)) => {
            let mut reqs = vec![Requirement::object("s3:PutObject", bucket, k)];
            if let Some(source) = copy_source {
                let source = source.trim_start_matches('/');
                let decoded = urlencoding::decode(source)
                    .map(|v| v.into_owned())
                    .unwrap_or_else(|_| source.to_string());
                if let Some((sb, sk)) = decoded.split_once('/') {
                    reqs.push(Requirement::object("s3:GetObject", sb, sk));
                }
            }
            reqs
        }
        ("POST", Some(k)) => {
            // CreateMultipartUpload (?uploads) and CompleteMultipartUpload
            // (?uploadId) both require PutObject, per AWS.
            vec![Requirement::object("s3:PutObject", bucket, k)]
        }
        ("DELETE", Some(k)) => {
            if has("uploadId") {
                vec![Requirement::object("s3:AbortMultipartUpload", bucket, k)]
            } else {
                vec![Requirement::object("s3:DeleteObject", bucket, k)]
            }
        }
        _ => vec![Requirement::bucket("s3:ListBucket", bucket)],
    };
    Some(reqs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(json: &str) -> PolicyDocument {
        serde_json::from_str(json).unwrap()
    }

    const READ_ONLY_DOCS: &str = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow",
             "Action": ["s3:GetObject", "s3:ListBucket"],
             "Resource": ["arn:aws:s3:::docs", "arn:aws:s3:::docs/*"]}
        ]
    }"#;

    #[test]
    fn allow_matches_action_and_resource() {
        let p = policy(READ_ONLY_DOCS);
        assert!(evaluate(&p, &Requirement::object("s3:GetObject", "docs", "a/b.txt")));
        assert!(evaluate(&p, &Requirement::bucket("s3:ListBucket", "docs")));
        assert!(!evaluate(&p, &Requirement::object("s3:PutObject", "docs", "a")));
        assert!(!evaluate(&p, &Requirement::object("s3:GetObject", "other", "a")));
    }

    #[test]
    fn explicit_deny_wins_over_allow() {
        let p = policy(
            r#"{"Statement": [
                {"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::*"},
                {"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "arn:aws:s3:::*"}
            ]}"#,
        );
        assert!(evaluate(&p, &Requirement::object("s3:GetObject", "b", "k")));
        assert!(!evaluate(&p, &Requirement::object("s3:DeleteObject", "b", "k")));
    }

    #[test]
    fn action_match_is_case_insensitive_resource_is_not() {
        let p = policy(
            r#"{"Statement": [
                {"Effect": "Allow", "Action": "s3:getobject", "Resource": "arn:aws:s3:::b/*"}
            ]}"#,
        );
        assert!(evaluate(&p, &Requirement::object("s3:GetObject", "b", "k")));
        assert!(!evaluate(&p, &Requirement::object("s3:GetObject", "B", "k")));
    }

    #[test]
    fn single_string_action_and_resource_forms_parse() {
        let p = policy(
            r#"{"Statement": [
                {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::b/prefix/*"}
            ]}"#,
        );
        assert!(evaluate(&p, &Requirement::object("s3:GetObject", "b", "prefix/x")));
        assert!(!evaluate(&p, &Requirement::object("s3:GetObject", "b", "other/x")));
    }

    #[test]
    fn wildcard_matching() {
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("arn:aws:s3:::b/*", "arn:aws:s3:::b/a/b/c"));
        assert!(!wildcard_match("arn:aws:s3:::b/*", "arn:aws:s3:::b"));
        assert!(wildcard_match("a?c", "abc"));
        assert!(!wildcard_match("a?c", "abbc"));
        assert!(wildcard_match("*.dat", "1721893456123.dat"));
    }

    #[test]
    fn requirements_mapping_covers_core_operations() {
        let r = |m: &str, p: &str, q: &str| requirements_for_request(m, p, q, None).unwrap();
        assert_eq!(r("GET", "/", "")[0].action, "s3:ListAllMyBuckets");
        assert_eq!(r("GET", "/b", "list-type=2")[0].action, "s3:ListBucket");
        assert_eq!(r("GET", "/b", "uploads")[0].action, "s3:ListBucketMultipartUploads");
        assert_eq!(r("PUT", "/b", "")[0].action, "s3:CreateBucket");
        assert_eq!(r("DELETE", "/b", "")[0].action, "s3:DeleteBucket");
        assert_eq!(r("GET", "/b/k", "")[0], Requirement::object("s3:GetObject", "b", "k"));
        assert_eq!(r("PUT", "/b/k", "")[0], Requirement::object("s3:PutObject", "b", "k"));
        assert_eq!(r("DELETE", "/b/k", "")[0].action, "s3:DeleteObject");
        assert_eq!(r("DELETE", "/b/k", "uploadId=x")[0].action, "s3:AbortMultipartUpload");
        assert_eq!(r("POST", "/b/k", "uploads")[0].action, "s3:PutObject");
        assert_eq!(r("POST", "/b", "delete")[0], Requirement::object("s3:DeleteObject", "b", "*"));
        assert!(requirements_for_request("POST", "/b", "rebuildIndex", None).is_none());
    }

    #[test]
    fn copy_requires_read_on_source_and_write_on_dest() {
        let reqs = requirements_for_request("PUT", "/dst/key", "", Some("/src/orig")).unwrap();
        assert!(reqs.contains(&Requirement::object("s3:PutObject", "dst", "key")));
        assert!(reqs.contains(&Requirement::object("s3:GetObject", "src", "orig")));
    }
}
