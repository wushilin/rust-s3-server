//! S3-compatible IAM policy documents and evaluation.
//!
//! Policies are attached to IAM users and govern every access key belonging
//! to that user. Evaluation follows AWS semantics: explicit `Deny` wins,
//! then explicit `Allow`, default is deny. `Action` and `Resource` support
//! `*` and `?` wildcards; action names match case-insensitively.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct Statement {
    #[serde(rename = "Sid", default, skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Action")]
    pub action: OneOrMany,
    #[serde(rename = "Resource")]
    pub resource: OneOrMany,
    #[serde(rename = "Condition", default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<Condition>,
}

/// Supported AWS string conditions for S3 request parameters. The condition
/// key namespace is deliberately strict: unsupported keys fail validation
/// instead of being silently ignored.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Condition {
    #[serde(rename = "StringEquals", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub string_equals: BTreeMap<String, OneOrMany>,
    #[serde(rename = "StringLike", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub string_like: BTreeMap<String, OneOrMany>,
    #[serde(rename = "StringNotEquals", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub string_not_equals: BTreeMap<String, OneOrMany>,
    #[serde(rename = "StringNotLike", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub string_not_like: BTreeMap<String, OneOrMany>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RuleAccess {
    Read,
    Write,
    ReadWrite,
}

/// The deliberately small, safe rule model exposed by the management UI.
/// It compiles to regular S3 policy statements; it is not a second policy
/// evaluator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PolicyRule {
    pub effect: Effect,
    pub access: RuleAccess,
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
}

const READ_BUCKET_ACTIONS: &[&str] = &["s3:ListBucket", "s3:ListBucketVersions"];
const READ_OBJECT_ACTIONS: &[&str] = &["s3:GetObject"];
const WRITE_BUCKET_ACTIONS: &[&str] = &["s3:ListBucketMultipartUploads"];
const WRITE_OBJECT_ACTIONS: &[&str] = &[
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:AbortMultipartUpload",
    "s3:ListMultipartUploadParts",
];

pub fn compile_rules(rules: &[PolicyRule]) -> Result<PolicyDocument, String> {
    let mut statement = Vec::with_capacity(rules.len() * 2);
    for (index, rule) in rules.iter().enumerate() {
        let bucket = rule.bucket.trim();
        let prefix = rule.prefix.trim().trim_start_matches('/');
        if bucket.is_empty() {
            return Err(format!("Rule {}: bucket must not be empty", index + 1));
        }
        if bucket.contains('/') {
            return Err(format!("Rule {}: bucket must not contain '/'", index + 1));
        }
        if bucket == "*" && !prefix.is_empty() {
            return Err(format!(
                "Rule {}: a prefix requires a specific bucket",
                index + 1
            ));
        }

        let (_, object_actions) = rule_actions(rule.access);
        let bucket_resource = format!("arn:aws:s3:::{bucket}");
        let object_resource = if bucket == "*" {
            "arn:aws:s3:::*".to_string()
        } else {
            format!("arn:aws:s3:::{bucket}/{prefix}*")
        };
        let condition = (!prefix.is_empty()).then(|| Condition {
            string_like: BTreeMap::from([(
                "s3:prefix".to_string(),
                OneOrMany::One(format!("{prefix}*")),
            )]),
            ..Condition::default()
        });
        if matches!(rule.access, RuleAccess::Read | RuleAccess::ReadWrite) {
            statement.push(Statement {
                sid: Some(format!("RustS3Rule{}ReadBucket", index + 1)),
                effect: rule.effect,
                action: strings(READ_BUCKET_ACTIONS.to_vec()),
                resource: OneOrMany::One(bucket_resource.clone()),
                condition,
            });
        }
        if matches!(rule.access, RuleAccess::Write | RuleAccess::ReadWrite) {
            statement.push(Statement {
                sid: Some(format!("RustS3Rule{}WriteBucket", index + 1)),
                effect: rule.effect,
                action: strings(WRITE_BUCKET_ACTIONS.to_vec()),
                resource: OneOrMany::One(bucket_resource),
                condition: None,
            });
        }
        statement.push(Statement {
            sid: Some(format!("RustS3Rule{}Object", index + 1)),
            effect: rule.effect,
            action: strings(object_actions),
            resource: OneOrMany::One(object_resource),
            condition: None,
        });
    }
    let policy = PolicyDocument {
        version: default_version(),
        statement,
    };
    policy.validate()?;
    Ok(policy)
}

/// Converts only canonical rule-generated policies. Refusing anything else
/// guarantees that switching to the visual editor cannot discard information.
pub fn decompile_rules(policy: &PolicyDocument) -> Result<Vec<PolicyRule>, String> {
    let mut rules = Vec::new();
    let mut cursor = 0usize;
    while cursor < policy.statement.len() {
        let rule_number = rules.len() + 1;
        let first = &policy.statement[cursor];
        let read_sid = format!("RustS3Rule{rule_number}ReadBucket");
        let write_sid = format!("RustS3Rule{rule_number}WriteBucket");
        let (access, bucket_statement, write_statement) =
            if first.sid.as_deref() == Some(read_sid.as_str()) {
                cursor += 1;
                if policy
                    .statement
                    .get(cursor)
                    .and_then(|statement| statement.sid.as_deref())
                    == Some(write_sid.as_str())
                {
                    let write = &policy.statement[cursor];
                    cursor += 1;
                    (RuleAccess::ReadWrite, first, Some(write))
                } else {
                    (RuleAccess::Read, first, None)
                }
            } else if first.sid.as_deref() == Some(write_sid.as_str()) {
                cursor += 1;
                (RuleAccess::Write, first, None)
            } else {
                return Err("policy contains statements not representable by the rule builder".into());
            };
        let object_sid = format!("RustS3Rule{rule_number}Object");
        let object_statement = policy
            .statement
            .get(cursor)
            .filter(|statement| statement.sid.as_deref() == Some(object_sid.as_str()))
            .ok_or_else(|| {
                "policy contains statements not representable by the rule builder".to_string()
            })?;
        cursor += 1;
        if bucket_statement.effect != object_statement.effect
            || write_statement
                .map(|statement| statement.effect != object_statement.effect)
                .unwrap_or(false)
        {
            return Err("policy contains statements not representable by the rule builder".into());
        }
        let bucket_resource = one_value(&bucket_statement.resource)
            .ok_or_else(|| "rule bucket resource is not canonical".to_string())?;
        let bucket = bucket_resource
            .strip_prefix("arn:aws:s3:::")
            .filter(|value| !value.is_empty() && !value.contains('/'))
            .ok_or_else(|| "rule bucket resource is not canonical".to_string())?
            .to_string();
        let object_resource = one_value(&object_statement.resource)
            .ok_or_else(|| "rule object resource is not canonical".to_string())?;
        let prefix = if bucket == "*" {
            if object_resource != "arn:aws:s3:::*" {
                return Err("rule object resource is not canonical".into());
            }
            String::new()
        } else {
            object_resource
                .strip_prefix(&format!("arn:aws:s3:::{bucket}/"))
                .and_then(|value| value.strip_suffix('*'))
                .ok_or_else(|| "rule object resource is not canonical".to_string())?
                .to_string()
        };
        rules.push(PolicyRule {
            effect: bucket_statement.effect,
            access,
            bucket,
            prefix,
        });
    }
    if compile_rules(&rules)? != *policy {
        return Err("policy contains advanced data that the rule builder would lose".into());
    }
    Ok(rules)
}

fn rule_actions(access: RuleAccess) -> (Vec<&'static str>, Vec<&'static str>) {
    match access {
        RuleAccess::Read => (READ_BUCKET_ACTIONS.to_vec(), READ_OBJECT_ACTIONS.to_vec()),
        RuleAccess::Write => (WRITE_BUCKET_ACTIONS.to_vec(), WRITE_OBJECT_ACTIONS.to_vec()),
        RuleAccess::ReadWrite => (
            READ_BUCKET_ACTIONS
                .iter()
                .chain(WRITE_BUCKET_ACTIONS)
                .copied()
                .collect(),
            READ_OBJECT_ACTIONS
                .iter()
                .chain(WRITE_OBJECT_ACTIONS)
                .copied()
                .collect(),
        ),
    }
}

fn strings(values: Vec<&str>) -> OneOrMany {
    OneOrMany::Many(values.into_iter().map(str::to_string).collect())
}

fn one_value(value: &OneOrMany) -> Option<&str> {
    match value {
        OneOrMany::One(value) => Some(value),
        OneOrMany::Many(values) if values.len() == 1 => Some(&values[0]),
        OneOrMany::Many(_) => None,
    }
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

    fn is_empty(&self) -> bool {
        match self {
            Self::One(value) => value.is_empty(),
            Self::Many(values) => values.is_empty() || values.iter().any(String::is_empty),
        }
    }
}

impl PolicyDocument {
    /// Validates the subset of AWS policy syntax that this server enforces.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != "2012-10-17" && self.version != "2008-10-17" {
            return Err(format!("unsupported policy Version {:?}", self.version));
        }
        for (index, statement) in self.statement.iter().enumerate() {
            if statement.action.is_empty() {
                return Err(format!("Statement[{index}].Action must not be empty"));
            }
            if statement.resource.is_empty() {
                return Err(format!("Statement[{index}].Resource must not be empty"));
            }
            if let Some(condition) = &statement.condition {
                validate_condition(condition, statement, index)?;
            }
        }
        Ok(())
    }
}

fn validate_condition(
    condition: &Condition,
    statement: &Statement,
    statement_index: usize,
) -> Result<(), String> {
    if condition.string_equals.is_empty()
        && condition.string_like.is_empty()
        && condition.string_not_equals.is_empty()
        && condition.string_not_like.is_empty()
    {
        return Err(format!(
            "Statement[{statement_index}].Condition must not be empty"
        ));
    }
    const LIST_ACTIONS: &[&str] = &[
        "s3:ListBucket",
        "s3:ListBucketVersions",
    ];
    if statement
        .action
        .iter()
        .any(|action| !LIST_ACTIONS.iter().any(|allowed| action.eq_ignore_ascii_case(allowed)))
    {
        return Err(format!(
            "Statement[{statement_index}].Condition uses s3 list keys with a non-list Action"
        ));
    }
    if statement.resource.iter().any(|resource| {
        resource
            .strip_prefix("arn:aws:s3:::")
            .map(|suffix| suffix.is_empty() || suffix.contains('/'))
            .unwrap_or(true)
    }) {
        return Err(format!(
            "Statement[{statement_index}].Condition uses s3 list keys with a non-bucket Resource"
        ));
    }
    let operators = [
        ("StringEquals", &condition.string_equals),
        ("StringLike", &condition.string_like),
        ("StringNotEquals", &condition.string_not_equals),
        ("StringNotLike", &condition.string_not_like),
    ];
    for (operator, entries) in operators {
        for (key, values) in entries {
            if !matches!(key.to_ascii_lowercase().as_str(), "s3:prefix" | "s3:delimiter") {
                return Err(format!(
                    "Statement[{statement_index}].Condition.{operator} uses unsupported key {key:?}"
                ));
            }
            if values.is_empty() {
                return Err(format!(
                    "Statement[{statement_index}].Condition.{operator}.{key} must not be empty"
                ));
            }
        }
    }
    Ok(())
}

/// One authorization requirement derived from an incoming request:
/// the S3 action plus the ARN it targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requirement {
    pub action: &'static str,
    pub resource: String,
    pub context: BTreeMap<String, String>,
}

impl Requirement {
    pub fn bucket(action: &'static str, bucket: &str) -> Self {
        Self {
            action,
            resource: format!("arn:aws:s3:::{bucket}"),
            context: BTreeMap::new(),
        }
    }

    pub fn bucket_with_context(
        action: &'static str,
        bucket: &str,
        context: BTreeMap<String, String>,
    ) -> Self {
        Self {
            action,
            resource: format!("arn:aws:s3:::{bucket}"),
            context,
        }
    }

    pub fn object(action: &'static str, bucket: &str, key: &str) -> Self {
        Self {
            action,
            resource: format!("arn:aws:s3:::{bucket}/{key}"),
            context: BTreeMap::new(),
        }
    }

    pub fn all_buckets(action: &'static str) -> Self {
        Self {
            action,
            resource: "arn:aws:s3:::*".to_string(),
            context: BTreeMap::new(),
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
        if !conditions_match(statement.condition.as_ref(), &requirement.context) {
            continue;
        }
        match statement.effect {
            Effect::Deny => return false,
            Effect::Allow => allowed = true,
        }
    }
    allowed
}

fn conditions_match(
    condition: Option<&Condition>,
    context: &BTreeMap<String, String>,
) -> bool {
    let Some(condition) = condition else {
        return true;
    };
    condition_operator_matches(&condition.string_equals, context, |pattern, value| {
        pattern == value
    }) && condition_operator_matches(&condition.string_like, context, wildcard_match)
        && condition_negated_operator_matches(
            &condition.string_not_equals,
            context,
            |pattern, value| pattern == value,
        )
        && condition_negated_operator_matches(
            &condition.string_not_like,
            context,
            wildcard_match,
        )
}

fn condition_operator_matches(
    entries: &BTreeMap<String, OneOrMany>,
    context: &BTreeMap<String, String>,
    predicate: impl Fn(&str, &str) -> bool,
) -> bool {
    entries.iter().all(|(key, patterns)| {
        context_value(context, key)
            .map(|value| patterns.iter().any(|pattern| predicate(pattern, value)))
            .unwrap_or(false)
    })
}

fn condition_negated_operator_matches(
    entries: &BTreeMap<String, OneOrMany>,
    context: &BTreeMap<String, String>,
    predicate: impl Fn(&str, &str) -> bool,
) -> bool {
    entries.iter().all(|(key, patterns)| {
        context_value(context, key)
            .map(|value| patterns.iter().all(|pattern| !predicate(pattern, value)))
            .unwrap_or(true)
    })
}

fn context_value<'a>(context: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    context
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

/// Glob match supporting `*` (any run) and `?` (any single char).
pub fn wildcard_match(pattern: &str, value: &str) -> bool {
    wildcard_match_bytes(pattern.as_bytes(), value.as_bytes())
}

fn wildcard_match_ci(pattern: &str, value: &str) -> bool {
    wildcard_match_bytes_ci(pattern.as_bytes(), value.as_bytes())
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

fn wildcard_match_bytes_ci(pattern: &[u8], value: &[u8]) -> bool {
    let (mut p, mut v) = (0usize, 0usize);
    let (mut star, mut star_v) = (None::<usize>, 0usize);
    while v < value.len() {
        if p < pattern.len()
            && (pattern[p] == b'?' || pattern[p].eq_ignore_ascii_case(&value[v]))
        {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            p += 1;
            star_v = v;
        } else if let Some(star_pos) = star {
            p = star_pos + 1;
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
        // A triggerable admin action, authorized as a custom IAM action rather
        // than excluded outright: the admin group (`s3:*`) — or anyone
        // explicitly granted `s3:RebuildIndex` — can run it, and root config
        // credentials still work as break-glass. Future triggerable admin
        // actions follow the same pattern (map to an `s3:<Action>`, never a
        // blanket root-only exclusion).
        return Some(vec![Requirement::bucket("s3:RebuildIndex", bucket)]);
    }

    let decoded_key = key.map(|k| {
        urlencoding::decode(k)
            .map(|v| v.into_owned())
            .unwrap_or_else(|_| k.to_string())
    });
    let list_context = list_condition_context(query);

    let reqs = match (method, &decoded_key) {
        // ── bucket level ──
        ("PUT", None) => vec![Requirement::bucket("s3:CreateBucket", bucket)],
        ("DELETE", None) => vec![Requirement::bucket("s3:DeleteBucket", bucket)],
        ("HEAD", None) => vec![Requirement::bucket("s3:ListBucket", bucket)],
        ("GET", None) => {
            if has("uploads") {
                vec![Requirement::bucket_with_context(
                    "s3:ListBucketMultipartUploads",
                    bucket,
                    list_context,
                )]
            } else if has("versions") {
                vec![Requirement::bucket_with_context(
                    "s3:ListBucketVersions",
                    bucket,
                    list_context,
                )]
            } else {
                vec![Requirement::bucket_with_context(
                    "s3:ListBucket",
                    bucket,
                    list_context,
                )]
            }
        }
        ("POST", None) if has("delete") => {
            // The handler authorizes every key after parsing the XML body.
            Vec::new()
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
        _ => vec![Requirement::bucket_with_context(
            "s3:ListBucket",
            bucket,
            list_context,
        )],
    };
    Some(reqs)
}

fn list_condition_context(query: &str) -> BTreeMap<String, String> {
    let mut context = BTreeMap::new();
    // AWS evaluates an omitted prefix as the empty prefix.
    context.insert(
        "s3:prefix".to_string(),
        query_value(query, "prefix").unwrap_or_default(),
    );
    if let Some(delimiter) = query_value(query, "delimiter") {
        context.insert("s3:delimiter".to_string(), delimiter);
    }
    context
}

fn query_value(query: &str, wanted: &str) -> Option<String> {
    query.split('&').find_map(|part| {
        let (raw_key, raw_value) = part.split_once('=').unwrap_or((part, ""));
        let key = urlencoding::decode(raw_key).ok()?;
        if key != wanted {
            return None;
        }
        urlencoding::decode(raw_value)
            .ok()
            .map(|value| value.into_owned())
    })
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
    fn prefix_condition_scopes_bucket_listing() {
        let p = policy(
            r#"{"Statement":[{
                "Effect":"Allow",
                "Action":"s3:ListBucket",
                "Resource":"arn:aws:s3:::b",
                "Condition":{"StringLike":{"s3:prefix":"allowed/*"}}
            }]}"#,
        );
        p.validate().unwrap();
        let allowed = requirements_for_request("GET", "/b", "list-type=2&prefix=allowed%2F", None)
            .unwrap();
        let denied = requirements_for_request("GET", "/b", "list-type=2&prefix=other%2F", None)
            .unwrap();
        assert!(is_authorized(&p, &allowed));
        assert!(!is_authorized(&p, &denied));
        assert_eq!(allowed[0].context.get("s3:prefix").unwrap(), "allowed/");
    }

    #[test]
    fn delimiter_and_negated_string_conditions_are_evaluated() {
        let p = policy(
            r#"{"Statement":[{
                "Effect":"Allow",
                "Action":"s3:ListBucket",
                "Resource":"arn:aws:s3:::b",
                "Condition":{
                    "StringEquals":{"s3:delimiter":"/"},
                    "StringNotLike":{"s3:prefix":"private/*"}
                }
            }]}"#,
        );
        let allowed = requirements_for_request("GET", "/b", "prefix=public%2F&delimiter=%2F", None)
            .unwrap();
        let denied = requirements_for_request("GET", "/b", "prefix=private%2Fx&delimiter=%2F", None)
            .unwrap();
        assert!(is_authorized(&p, &allowed));
        assert!(!is_authorized(&p, &denied));
    }

    #[test]
    fn unsupported_condition_syntax_is_rejected_strictly() {
        let unsupported_operator = serde_json::from_str::<PolicyDocument>(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::b","Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.1"}}}]}"#,
        );
        assert!(unsupported_operator.is_err());

        let unsupported_key = policy(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::b","Condition":{"StringEquals":{"aws:username":"alice"}}}]}"#,
        );
        assert!(unsupported_key.validate().is_err());
    }

    #[test]
    fn visual_rules_compile_to_prefix_safe_s3_policy_and_round_trip() {
        let rules = vec![PolicyRule {
            effect: Effect::Allow,
            access: RuleAccess::ReadWrite,
            bucket: "docs".to_string(),
            prefix: "teams/red/".to_string(),
        }];
        let compiled = compile_rules(&rules).unwrap();
        assert_eq!(decompile_rules(&compiled).unwrap(), rules);

        let list_allowed = requirements_for_request(
            "GET",
            "/docs",
            "list-type=2&prefix=teams%2Fred%2F",
            None,
        )
        .unwrap();
        let list_denied = requirements_for_request(
            "GET",
            "/docs",
            "list-type=2&prefix=teams%2Fblue%2F",
            None,
        )
        .unwrap();
        assert!(is_authorized(&compiled, &list_allowed));
        assert!(!is_authorized(&compiled, &list_denied));
        assert!(evaluate(
            &compiled,
            &Requirement::object("s3:DeleteObject", "docs", "teams/red/a.txt")
        ));
        assert!(!evaluate(
            &compiled,
            &Requirement::object("s3:DeleteObject", "docs", "teams/blue/a.txt")
        ));
    }

    #[test]
    fn advanced_json_is_never_lossily_decompiled() {
        let advanced = policy(
            r#"{"Statement":[{"Sid":"Custom","Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#,
        );
        assert!(decompile_rules(&advanced).is_err());
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
        assert!(r("POST", "/b", "delete").is_empty());
        assert_eq!(
            requirements_for_request("POST", "/b", "rebuildIndex", None).unwrap()[0].action,
            "s3:RebuildIndex"
        );
    }

    #[test]
    fn rebuild_index_is_an_iam_action_admins_have_and_readers_do_not() {
        let reqs = requirements_for_request("POST", "/docs", "rebuildIndex", None).unwrap();
        let admin = policy(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}"#,
        );
        let read_only = policy(READ_ONLY_DOCS);
        assert!(is_authorized(&admin, &reqs), "admin (s3:*) can rebuild");
        assert!(
            !is_authorized(&read_only, &reqs),
            "a read-only policy cannot rebuild"
        );
    }

    #[test]
    fn copy_requires_read_on_source_and_write_on_dest() {
        let reqs = requirements_for_request("PUT", "/dst/key", "", Some("/src/orig")).unwrap();
        assert!(reqs.contains(&Requirement::object("s3:PutObject", "dst", "key")));
        assert!(reqs.contains(&Requirement::object("s3:GetObject", "src", "orig")));
    }
}
