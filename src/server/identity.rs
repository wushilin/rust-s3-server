//! The unified caller identity shared by both front doors.
//!
//! The S3 API (access-key SigV4) and the management console (username /
//! password session) authenticate differently, but they authorize
//! identically. Both resolve their credential to an [`Identity`], and every
//! authorization decision on either path goes through [`Identity::authorize`].
//! Keeping this the single choke point means a policy-enforcement fix lands in
//! exactly one place.

use super::policy::{is_authorized, PolicyDocument, Requirement};

/// A resolved caller, independent of how it authenticated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Identity {
    /// A root config credential or a built-in console admin: unrestricted, no
    /// policy applies.
    Unrestricted {
        username: Option<String>,
        access_key: Option<String>,
    },
    /// A runtime IAM user, bound by its effective policy. `policy: None` means
    /// no policy is attached, which denies everything.
    Iam {
        username: String,
        policy: Option<PolicyDocument>,
    },
}

impl Identity {
    /// Unrestricted root identity carrying an optional console username and/or
    /// the access key it authenticated with.
    pub fn root(username: Option<String>, access_key: Option<String>) -> Self {
        Identity::Unrestricted {
            username,
            access_key,
        }
    }

    /// A policy-bound IAM identity.
    pub fn iam(username: String, policy: Option<PolicyDocument>) -> Self {
        Identity::Iam { username, policy }
    }

    /// The single authorization decision point. Unrestricted identities always
    /// pass; IAM identities are evaluated against their policy (a missing
    /// policy denies everything).
    pub fn authorize(&self, requirements: &[Requirement]) -> bool {
        match self {
            Identity::Unrestricted { .. } => true,
            Identity::Iam {
                policy: Some(policy),
                ..
            } => is_authorized(policy, requirements),
            Identity::Iam { policy: None, .. } => false,
        }
    }

    /// True when no policy check applies to this identity.
    pub fn is_unrestricted(&self) -> bool {
        matches!(self, Identity::Unrestricted { .. })
    }

    pub fn username(&self) -> Option<&str> {
        match self {
            Identity::Unrestricted { username, .. } => username.as_deref(),
            Identity::Iam { username, .. } => Some(username),
        }
    }

    pub fn access_key(&self) -> Option<&str> {
        match self {
            Identity::Unrestricted { access_key, .. } => access_key.as_deref(),
            Identity::Iam { .. } => None,
        }
    }

    /// The IAM policy for a policy-bound identity, if any. Used by handlers
    /// that must authorize per-item after parsing a request body (e.g. S3
    /// multi-object delete).
    pub fn policy(&self) -> Option<&PolicyDocument> {
        match self {
            Identity::Iam { policy, .. } => policy.as_ref(),
            Identity::Unrestricted { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow_all() -> PolicyDocument {
        serde_json::from_str(
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}"#,
        )
        .unwrap()
    }

    #[test]
    fn unrestricted_allows_everything() {
        let id = Identity::root(Some("admin".into()), Some("AKID".into()));
        assert!(id.authorize(&[Requirement::object("s3:DeleteObject", "b", "k")]));
        assert!(id.is_unrestricted());
    }

    #[test]
    fn iam_without_policy_denies_everything() {
        let id = Identity::iam("bob".into(), None);
        assert!(!id.authorize(&[Requirement::object("s3:GetObject", "b", "k")]));
        assert_eq!(id.username(), Some("bob"));
    }

    #[test]
    fn iam_with_policy_is_evaluated() {
        let id = Identity::iam("alice".into(), Some(allow_all()));
        assert!(id.authorize(&[Requirement::object("s3:PutObject", "b", "k")]));
    }
}
