//! Environment substitution for the config file.
//!
//! A container gets its configuration from the environment, but this server is
//! configured by a YAML file. Rather than inventing an env var for every field
//! and keeping two sources of truth in sync, the file itself carries the
//! placeholders:
//!
//! ```yaml
//! server:
//!   bind_port: {{RUSTS3_PORT:8002}}
//!   base_dir: "{{RUSTS3_DATA_DIR:/data}}"
//! ```
//!
//! `{{NAME:default}}` expands to `$NAME` when it is set and non-empty, and to
//! `default` otherwise. `{{NAME}}` has no default: if it is unset the load
//! fails, loudly, naming the variable. That matters more than it might seem —
//! silently expanding a missing `{{RUSTS3_ADMIN_PASSWORD}}` to an empty string
//! would hand out an account with a blank password.
//!
//! The default may itself contain colons (`{{RUSTS3_ENDPOINT:http://host:80}}`)
//! because only the *first* colon separates name from default.
//!
//! Substitution is textual and happens before the YAML parser sees the
//! document, exactly like `envsubst`. So a value is inserted literally: if it
//! can contain spaces, `#`, or a leading `*`, quote the placeholder in the
//! template (`"{{NAME:default}}"`) as the shipped `config.docker.yaml` does.
//! A single quote inside a value that lands in a single-quoted scalar is the
//! one case that will still confuse YAML; use double quotes there.

use std::collections::BTreeSet;

/// A placeholder that was required but had nothing to expand to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingVars(pub Vec<String>);

impl std::fmt::Display for MissingVars {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "config references environment variable{} that {} not set and {} no default: {}",
            if self.0.len() == 1 { "" } else { "s" },
            if self.0.len() == 1 { "is" } else { "are" },
            if self.0.len() == 1 { "has" } else { "have" },
            self.0.join(", ")
        )
    }
}

impl std::error::Error for MissingVars {}

/// Expands every `{{NAME}}` / `{{NAME:default}}` in `text`, reading variables
/// through `lookup`. Collects *all* missing required variables before failing,
/// so one run tells the operator everything they still have to set.
pub fn expand_with<F>(text: &str, lookup: F) -> Result<String, MissingVars>
where
    F: Fn(&str) -> Option<String>,
{
    let mut out = String::with_capacity(text.len());
    let mut missing: BTreeSet<String> = BTreeSet::new();
    let mut rest = text;

    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            // An unterminated `{{` is just text; leave the rest untouched.
            out.push_str(&rest[start..]);
            return finish(out, missing);
        };
        let token = &after[..end];
        rest = &after[end + 2..];

        let (name, default) = match token.find(':') {
            Some(idx) => (token[..idx].trim(), Some(&token[idx + 1..])),
            None => (token.trim(), None),
        };

        // A name that isn't a plausible variable is left verbatim, so YAML that
        // happens to contain braces is not mangled.
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            out.push_str("{{");
            out.push_str(token);
            out.push_str("}}");
            continue;
        }

        // An empty value counts as unset: `-e RUSTS3_PORT=` in a compose file
        // means "I didn't set this", not "bind to no port".
        match lookup(name).filter(|value| !value.is_empty()) {
            Some(value) => out.push_str(&value),
            None => match default {
                Some(default) => out.push_str(default),
                None => {
                    missing.insert(name.to_string());
                }
            },
        }
    }
    out.push_str(rest);
    finish(out, missing)
}

fn finish(out: String, missing: BTreeSet<String>) -> Result<String, MissingVars> {
    if missing.is_empty() {
        Ok(out)
    } else {
        Err(MissingVars(missing.into_iter().collect()))
    }
}

/// Expands against the process environment.
pub fn expand(text: &str) -> Result<String, MissingVars> {
    expand_with(text, |name| std::env::var(name).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    #[test]
    fn set_variables_win_and_defaults_fill_the_rest() {
        let text = "port: {{RUSTS3_PORT:8002}}\ndir: {{RUSTS3_DATA_DIR:/data}}\n";
        let out = expand_with(text, env(&[("RUSTS3_PORT", "9000")])).unwrap();
        assert_eq!(out, "port: 9000\ndir: /data\n");
    }

    #[test]
    fn only_the_first_colon_separates_name_from_default() {
        let text = "endpoint: {{RUSTS3_ENDPOINT:http://host:80/path}}";
        let out = expand_with(text, env(&[])).unwrap();
        assert_eq!(out, "endpoint: http://host:80/path");
    }

    #[test]
    fn an_empty_default_is_allowed() {
        assert_eq!(expand_with("a: {{X:}}", env(&[])).unwrap(), "a: ");
    }

    #[test]
    fn an_empty_environment_value_falls_back_to_the_default() {
        // `-e RUSTS3_PORT=` in a compose file means "unset", not "empty port".
        let out = expand_with("port: {{RUSTS3_PORT:8002}}", env(&[("RUSTS3_PORT", "")])).unwrap();
        assert_eq!(out, "port: 8002");
    }

    #[test]
    fn a_required_variable_that_is_unset_fails_loudly() {
        // Expanding this to "" would create an account with a blank password.
        let err = expand_with("password: {{RUSTS3_ADMIN_PASSWORD}}", env(&[])).unwrap_err();
        assert_eq!(err.0, vec!["RUSTS3_ADMIN_PASSWORD".to_string()]);
        assert!(err.to_string().contains("RUSTS3_ADMIN_PASSWORD"));
    }

    #[test]
    fn every_missing_variable_is_reported_at_once() {
        let text = "a: {{A}}\nb: {{B}}\nc: {{C:ok}}\nd: {{A}}\n";
        let err = expand_with(text, env(&[])).unwrap_err();
        assert_eq!(err.0, vec!["A".to_string(), "B".to_string()]);
    }

    #[test]
    fn a_required_variable_that_is_set_is_fine() {
        let out = expand_with("password: {{P}}", env(&[("P", "hunter2")])).unwrap();
        assert_eq!(out, "password: hunter2");
    }

    #[test]
    fn text_without_placeholders_is_untouched() {
        let text = "server:\n  bind_port: 8002\n  note: 'braces {} are fine'\n";
        assert_eq!(expand_with(text, env(&[])).unwrap(), text);
    }

    #[test]
    fn things_that_are_not_variable_names_are_left_alone() {
        // Go/Helm-style templates and stray braces must survive untouched.
        for text in [
            "a: {{ .Values.thing }}",
            "b: {{}}",
            "c: {{not-a-name:x}}",
            "d: {{ has space }}",
        ] {
            assert_eq!(expand_with(text, env(&[])).unwrap(), text, "{text}");
        }
    }

    #[test]
    fn an_unterminated_placeholder_is_literal_text() {
        assert_eq!(expand_with("a: {{X", env(&[])).unwrap(), "a: {{X");
    }

    #[test]
    fn whitespace_around_the_name_is_ignored() {
        let out = expand_with("a: {{ NAME }}", env(&[("NAME", "v")])).unwrap();
        assert_eq!(out, "a: v");
    }

    #[test]
    fn repeated_placeholders_all_expand() {
        let out = expand_with("{{A}}-{{A}}-{{B:z}}", env(&[("A", "x")])).unwrap();
        assert_eq!(out, "x-x-z");
    }

    #[test]
    fn a_realistic_document_expands_end_to_end() {
        let text = concat!(
            "server:\n",
            "  bind_address: \"{{RUSTS3_BIND_ADDRESS:0.0.0.0}}\"\n",
            "  bind_port: {{RUSTS3_PORT:8002}}\n",
            "  base_dir: \"{{RUSTS3_DATA_DIR:/data}}\"\n",
            "auth:\n",
            "  enabled: {{RUSTS3_AUTH_ENABLED:true}}\n",
            "  users:\n",
            "    - user: \"{{RUSTS3_ADMIN_USER:admin}}\"\n",
            "      password: \"{{RUSTS3_ADMIN_PASSWORD:changeme}}\"\n",
        );
        let out = expand_with(
            text,
            env(&[("RUSTS3_PORT", "9000"), ("RUSTS3_ADMIN_PASSWORD", "s3cret")]),
        )
        .unwrap();
        assert!(out.contains("bind_port: 9000"));
        assert!(out.contains("bind_address: \"0.0.0.0\""));
        assert!(out.contains("base_dir: \"/data\""));
        assert!(out.contains("password: \"s3cret\""));
        // …and it is still valid YAML afterwards.
        let parsed: serde_yaml::Value = serde_yaml::from_str(&out).unwrap();
        assert_eq!(parsed["server"]["bind_port"].as_u64(), Some(9000));
    }
}
