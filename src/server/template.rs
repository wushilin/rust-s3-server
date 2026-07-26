//! Placeholder substitution for the config file.
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
//! Substitution happens on the *text*, before the YAML parser sees it. That is
//! not a stylistic choice: an unquoted `{{RUSTS3_PORT:8002}}` is not valid YAML
//! (it reads as a flow mapping whose key is another flow mapping), so numeric
//! and boolean fields could not be templated at all if the document were parsed
//! first.
//!
//! ## Providers
//!
//! A placeholder is `{{provider<sep>param<sep>param…}}`. The character
//! immediately after the provider name *is* the separator, so a value
//! containing colons can pick something else:
//!
//! ```text
//! {{env:RUSTS3_PORT:8002}}      env, params ["RUSTS3_PORT", "8002"]
//! {{env|DSN|postgres://x:5432}} env, separator '|', so the colons are data
//! {{upper$env$RUSTS3_REGION}}   upper applied to an env lookup
//! ```
//!
//! The first segment counts as a provider only if it *both* matches
//! `[A-Za-z_]+` — letters and underscores, no digits — and names a registered
//! provider. Failing either test, the placeholder falls back to the `env`
//! shorthand. So `{{a:b}}` is an `env` lookup of `a` defaulting to `b` unless
//! `a` is a registered provider; and a head containing a digit is disqualified
//! before the registry is consulted, which is why `RUSTS3_PORT` can never be
//! read as a call.
//!
//! Whichever form is used, everything after the first separator is the last
//! parameter: in `{{a:b:c:d}}` the default is `b:c:d`, colons and all.
//!
//! If you genuinely have a variable named after a provider, say the provider
//! explicitly — `{{env:ENV}}` reads `$ENV` — and the ambiguity is gone.
//!
//! Resolution rules are the provider's own. For `env`: the variable when it is
//! set and non-empty, else the default, and if there is no default the whole
//! load fails naming the variable. That last part matters more than it looks —
//! quietly expanding a missing `{{RUSTS3_ADMIN_PASSWORD}}` to an empty string
//! would hand out an account with a blank password.
//!
//! Extra parameters beyond what a provider uses are accepted and ignored, so
//! the calling convention can grow without a flag day.
//!
//! ## Adding a provider
//!
//! Implement [`Resolver`] and register it. A resolver receives the whole
//! [`Call`], including the registry, so it can resolve nested calls — which is
//! how `upper` composes with `env` above.

use std::collections::BTreeSet;
use std::collections::HashMap;

/// How deep nested provider calls may go. Bounds `{{upper:upper:upper:…}}`.
const MAX_DEPTH: usize = 8;

/// A parsed placeholder, handed to a [`Resolver`].
pub struct Call<'a> {
    /// Provider name as written (already matched case-insensitively).
    pub provider: &'a str,
    /// The character that separates the parameters.
    pub separator: char,
    /// Parameters, split on [`Call::separator`].
    pub params: Vec<&'a str>,
    /// Everything after the provider's separator, unsplit — for providers whose
    /// last parameter may itself contain the separator (an `env` default of
    /// `http://host:80`, say).
    pub rest: &'a str,
    /// The registry this call came from, so a provider can resolve nested
    /// calls.
    pub registry: &'a Registry,
    /// Nesting depth, checked against [`MAX_DEPTH`].
    pub depth: usize,
}

impl<'a> Call<'a> {
    /// The parameter at `index`, or `None` if it was not supplied.
    pub fn param(&self, index: usize) -> Option<&'a str> {
        self.params.get(index).copied()
    }

    /// Everything after parameter `index`, unsplit. `env` uses this so a
    /// default may contain the separator.
    pub fn rest_after(&self, index: usize) -> Option<&'a str> {
        let mut remaining = self.rest;
        for _ in 0..=index {
            let (_, tail) = remaining.split_once(self.separator)?;
            remaining = tail;
        }
        Some(remaining)
    }
}

/// Something that can turn a placeholder into a value.
///
/// `Ok(None)` means "no value, and that is not an error" — the caller reports
/// it as a missing required placeholder. `Err` is a malformed call.
pub trait Resolver: Send + Sync {
    /// Name as it appears in a placeholder. Matched case-insensitively.
    fn name(&self) -> &'static str;
    fn resolve(&self, call: &Call<'_>) -> Result<Option<String>, String>;
}

/// The providers available to a document.
pub struct Registry {
    resolvers: HashMap<String, Box<dyn Resolver>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl Registry {
    pub fn empty() -> Self {
        Self {
            resolvers: HashMap::new(),
        }
    }

    /// `env`, plus the `upper`/`lower` transforms.
    pub fn with_defaults() -> Self {
        let mut registry = Self::empty();
        registry.register(Box::new(EnvResolver::from_process()));
        registry.register(Box::new(CaseResolver::upper()));
        registry.register(Box::new(CaseResolver::lower()));
        registry
    }

    pub fn register(&mut self, resolver: Box<dyn Resolver>) {
        debug_assert!(
            is_provider_name(resolver.name()),
            "provider names are letters and underscores only, got {:?}",
            resolver.name()
        );
        self.resolvers
            .insert(resolver.name().to_ascii_lowercase(), resolver);
    }

    pub fn get(&self, name: &str) -> Option<&dyn Resolver> {
        // A name containing anything but letters and underscores is not a
        // provider, whatever the registry holds. This is what keeps the
        // shorthand unambiguous: `RUSTS3_PORT` has a digit in it, so it can
        // only ever be an environment variable, never a provider call.
        if !is_provider_name(name) {
            return None;
        }
        self.resolvers
            .get(&name.to_ascii_lowercase())
            .map(|boxed| boxed.as_ref())
    }

    /// Resolves one already-parsed placeholder body (`env:FOO:bar`), for
    /// providers that nest.
    pub fn resolve_token(&self, token: &str, depth: usize) -> Result<Option<String>, String> {
        if depth > MAX_DEPTH {
            return Err(format!("placeholder nested deeper than {MAX_DEPTH} levels"));
        }
        let Some(parsed) = parse_token(token) else {
            return Err(format!("{token:?} is not a placeholder"));
        };
        self.dispatch(parsed, depth)
    }

    fn dispatch(&self, parsed: Parsed<'_>, depth: usize) -> Result<Option<String>, String> {
        // A first segment naming a provider is a provider call; anything else
        // is the `{{NAME:default}}` shorthand for an env lookup.
        let (provider, params, rest) = match self.get(parsed.head) {
            Some(_) => (parsed.head, parsed.params.clone(), parsed.rest),
            None => (
                "env",
                std::iter::once(parsed.head).chain(parsed.params.clone()).collect(),
                parsed.token,
            ),
        };
        let resolver = self
            .get(provider)
            .ok_or_else(|| format!("no resolver named {provider:?}"))?;
        resolver.resolve(&Call {
            provider,
            separator: parsed.separator,
            params,
            rest,
            registry: self,
            depth,
        })
    }
}

/// Reads environment variables. Parameters: `NAME`, and optionally a default.
/// An empty value counts as unset — `-e RUSTS3_PORT=` in a compose file means
/// "I did not set this", not "bind to no port".
pub struct EnvResolver {
    lookup: Box<dyn Fn(&str) -> Option<String> + Send + Sync>,
}

impl EnvResolver {
    pub fn from_process() -> Self {
        Self {
            lookup: Box::new(|name| std::env::var(name).ok()),
        }
    }

    /// For tests and for callers that supply their own environment.
    pub fn with_lookup(lookup: impl Fn(&str) -> Option<String> + Send + Sync + 'static) -> Self {
        Self {
            lookup: Box::new(lookup),
        }
    }
}

impl Resolver for EnvResolver {
    fn name(&self) -> &'static str {
        "env"
    }

    fn resolve(&self, call: &Call<'_>) -> Result<Option<String>, String> {
        let Some(name) = call.param(0).map(str::trim).filter(|v| !v.is_empty()) else {
            return Err("env needs a variable name".to_string());
        };
        if let Some(value) = (self.lookup)(name).filter(|value| !value.is_empty()) {
            return Ok(Some(value));
        }
        // The default is the *unsplit* remainder, so it may contain the
        // separator: {{env:DSN:postgres://host:5432}} defaults to the whole URL.
        Ok(call.rest_after(0).map(str::to_string))
    }
}

/// Upper/lower-cases its argument. The argument may itself be a nested call,
/// which is what makes this useful: `{{upper:env:RUSTS3_REGION}}`.
pub struct CaseResolver {
    name: &'static str,
    upper: bool,
}

impl CaseResolver {
    pub fn upper() -> Self {
        Self {
            name: "upper",
            upper: true,
        }
    }

    pub fn lower() -> Self {
        Self {
            name: "lower",
            upper: false,
        }
    }
}

impl Resolver for CaseResolver {
    fn name(&self) -> &'static str {
        self.name
    }

    fn resolve(&self, call: &Call<'_>) -> Result<Option<String>, String> {
        // The whole remainder is the argument, unsplit — a transform takes one
        // thing, and that thing may contain the separator.
        let argument = call.rest;
        if argument.is_empty() {
            return Err(format!("{} needs a value", self.name));
        }
        // If the argument names a provider, resolve it first; otherwise take it
        // literally.
        let head_is_provider = parse_token(argument)
            .map(|parsed| call.registry.get(parsed.head).is_some())
            .unwrap_or(false);
        let value = if head_is_provider {
            match call.registry.resolve_token(argument, call.depth + 1)? {
                Some(value) => value,
                None => return Ok(None),
            }
        } else {
            argument.to_string()
        };
        Ok(Some(if self.upper {
            value.to_uppercase()
        } else {
            value.to_lowercase()
        }))
    }
}

/// Provider names are `[A-Za-z_]+`. Deliberately narrower than the identifiers
/// a placeholder can start with: env var names routinely contain digits
/// (`RUSTS3_PORT`), and a head containing a digit must never be mistaken for a
/// provider call.
fn is_provider_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphabetic() || c == '_')
}

/// The raw shape of a placeholder body, before a provider claims it.
#[derive(Clone)]
struct Parsed<'a> {
    /// The whole body, e.g. `RUSTS3_PORT:8002`.
    token: &'a str,
    /// Leading identifier: a provider name, or an env var name.
    head: &'a str,
    separator: char,
    /// Everything after the separator, unsplit.
    rest: &'a str,
    /// `rest`, split on the separator.
    params: Vec<&'a str>,
}

/// Splits `NAME<sep>a<sep>b` into its parts. `None` when the body does not
/// start with an identifier — so YAML that merely contains braces, and
/// templates belonging to something else (`{{ .Values.x }}`), are left alone.
fn parse_token(token: &str) -> Option<Parsed<'_>> {
    let trimmed = token.trim();
    let head_len = trimmed
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(trimmed.len());
    if head_len == 0 {
        return None;
    }
    let head = &trimmed[..head_len];
    let tail = &trimmed[head_len..];
    let Some(separator) = tail.chars().next() else {
        // Bare `{{NAME}}`: no separator, no parameters.
        return Some(Parsed {
            token: trimmed,
            head,
            separator: ':',
            rest: "",
            params: Vec::new(),
        });
    };
    let rest = &tail[separator.len_utf8()..];
    Some(Parsed {
        token: trimmed,
        head,
        separator,
        rest,
        params: rest.split(separator).collect(),
    })
}

/// Placeholders that resolved to nothing and had no default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingVars(pub Vec<String>);

impl std::fmt::Display for MissingVars {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "config references {} that {} not set and {} no default: {}",
            if self.0.len() == 1 { "a value" } else { "values" },
            if self.0.len() == 1 { "is" } else { "are" },
            if self.0.len() == 1 { "has" } else { "have" },
            self.0.join(", ")
        )
    }
}

impl std::error::Error for MissingVars {}

/// Expands every placeholder in `text` using `registry`.
pub fn expand_with_registry(text: &str, registry: &Registry) -> Result<String, MissingVars> {
    let mut out = String::with_capacity(text.len());
    let mut missing: BTreeSet<String> = BTreeSet::new();
    let mut rest = text;

    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            // An unterminated `{{` is just text.
            out.push_str(&rest[start..]);
            return finish(out, missing);
        };
        let token = &after[..end];
        rest = &after[end + 2..];

        let Some(parsed) = parse_token(token) else {
            // Not a placeholder: leave it exactly as written.
            out.push_str("{{");
            out.push_str(token);
            out.push_str("}}");
            continue;
        };

        match registry.dispatch(parsed, 0) {
            Ok(Some(value)) => out.push_str(&value),
            // Unresolved with no default: report the placeholder as written, so
            // the operator can find it in the file.
            Ok(None) => {
                missing.insert(token.trim().to_string());
            }
            // A malformed call is a config error too; name it the same way.
            Err(message) => {
                missing.insert(format!("{} ({message})", token.trim()));
            }
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

/// Expands against the default registry (the process environment).
pub fn expand(text: &str) -> Result<String, MissingVars> {
    expand_with_registry(text, &Registry::with_defaults())
}

/// Expands using a caller-supplied environment. Kept for callers (and tests)
/// that must not touch the process environment.
pub fn expand_with<F>(text: &str, lookup: F) -> Result<String, MissingVars>
where
    F: Fn(&str) -> Option<String> + Send + Sync + 'static,
{
    let mut registry = Registry::with_defaults();
    registry.register(Box::new(EnvResolver::with_lookup(lookup)));
    expand_with_registry(text, &registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + Send + Sync + 'static {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    fn registry(pairs: &[(&str, &str)]) -> Registry {
        let mut registry = Registry::with_defaults();
        registry.register(Box::new(EnvResolver::with_lookup(env(pairs))));
        registry
    }

    fn expand_env(text: &str, pairs: &[(&str, &str)]) -> Result<String, MissingVars> {
        expand_with_registry(text, &registry(pairs))
    }

    // ── the shorthand every existing config uses ─────────────────────────────

    #[test]
    fn set_variables_win_and_defaults_fill_the_rest() {
        let text = "port: {{RUSTS3_PORT:8002}}\ndir: {{RUSTS3_DATA_DIR:/data}}\n";
        let out = expand_env(text, &[("RUSTS3_PORT", "9000")]).unwrap();
        assert_eq!(out, "port: 9000\ndir: /data\n");
    }

    #[test]
    fn only_the_first_separator_splits_name_from_default() {
        let text = "endpoint: {{RUSTS3_ENDPOINT:http://host:80/path}}";
        assert_eq!(
            expand_env(text, &[]).unwrap(),
            "endpoint: http://host:80/path"
        );
    }

    #[test]
    fn an_empty_default_is_allowed() {
        assert_eq!(expand_env("a: {{X:}}", &[]).unwrap(), "a: ");
    }

    #[test]
    fn an_empty_environment_value_falls_back_to_the_default() {
        let out = expand_env("port: {{RUSTS3_PORT:8002}}", &[("RUSTS3_PORT", "")]).unwrap();
        assert_eq!(out, "port: 8002");
    }

    #[test]
    fn a_required_variable_that_is_unset_fails_loudly() {
        let err = expand_env("password: {{RUSTS3_ADMIN_PASSWORD}}", &[]).unwrap_err();
        assert_eq!(err.0, vec!["RUSTS3_ADMIN_PASSWORD".to_string()]);
        assert!(err.to_string().contains("RUSTS3_ADMIN_PASSWORD"));
    }

    #[test]
    fn every_missing_value_is_reported_at_once() {
        let err = expand_env("a: {{A}}\nb: {{B}}\nc: {{C:ok}}\nd: {{A}}\n", &[]).unwrap_err();
        assert_eq!(err.0, vec!["A".to_string(), "B".to_string()]);
    }

    #[test]
    fn text_without_placeholders_is_untouched() {
        let text = "server:\n  bind_port: 8002\n  note: 'braces {} are fine'\n";
        assert_eq!(expand_env(text, &[]).unwrap(), text);
    }

    #[test]
    fn things_that_are_not_placeholders_are_left_alone() {
        for text in ["a: {{ .Values.thing }}", "b: {{}}", "c: {{-nope:x}}"] {
            assert_eq!(expand_env(text, &[]).unwrap(), text, "{text}");
        }
    }

    #[test]
    fn an_unterminated_placeholder_is_literal_text() {
        assert_eq!(expand_env("a: {{X", &[]).unwrap(), "a: {{X");
    }

    // ── explicit providers ───────────────────────────────────────────────────

    #[test]
    fn the_env_provider_can_be_named_explicitly() {
        let out = expand_env("a: {{env:HOME_DIR:/tmp}}", &[("HOME_DIR", "/srv")]).unwrap();
        assert_eq!(out, "a: /srv");
        assert_eq!(expand_env("a: {{ENV:HOME_DIR:/tmp}}", &[]).unwrap(), "a: /tmp");
    }

    #[test]
    fn naming_the_provider_disambiguates_a_variable_called_env() {
        // The one case the shorthand cannot express.
        let out = expand_env("a: {{env:ENV:dev}}", &[("ENV", "production")]).unwrap();
        assert_eq!(out, "a: production");
    }

    #[test]
    fn any_character_after_the_provider_is_the_separator() {
        for text in [
            "a: {{env$PORT$8002}}",
            "a: {{env|PORT|8002}}",
            "a: {{env#PORT#8002}}",
            "a: {{env,PORT,8002}}",
        ] {
            assert_eq!(expand_env(text, &[]).unwrap(), "a: 8002", "{text}");
            assert_eq!(
                expand_env(text, &[("PORT", "9000")]).unwrap(),
                "a: 9000",
                "{text}"
            );
        }
    }

    #[test]
    fn a_separator_lets_a_default_contain_colons() {
        // The reason a choosable separator earns its keep.
        let out = expand_env("dsn: {{env|DSN|postgres://host:5432/db}}", &[]).unwrap();
        assert_eq!(out, "dsn: postgres://host:5432/db");
    }

    #[test]
    fn extra_parameters_are_accepted_and_ignored() {
        // Room for the calling convention to grow without a flag day.
        let out = expand_env("a: {{env:PORT:8002:future:args}}", &[]).unwrap();
        assert_eq!(out, "a: 8002:future:args");
        let out = expand_env("a: {{env|PORT|8002|future}}", &[]).unwrap();
        assert_eq!(out, "a: 8002|future");
    }

    #[test]
    fn transforms_apply_to_a_literal() {
        assert_eq!(expand_env("a: {{upper:hello}}", &[]).unwrap(), "a: HELLO");
        assert_eq!(expand_env("a: {{lower:HELLO}}", &[]).unwrap(), "a: hello");
    }

    #[test]
    fn transforms_compose_with_a_nested_lookup() {
        let out = expand_env("a: {{upper:env:REGION}}", &[("REGION", "us-east-1")]).unwrap();
        assert_eq!(out, "a: US-EAST-1");
        let out = expand_env("a: {{lower$env$REGION}}", &[("REGION", "US-WEST-2")]).unwrap();
        assert_eq!(out, "a: us-west-2");
    }

    #[test]
    fn a_nested_lookup_that_is_missing_is_still_reported() {
        let err = expand_env("a: {{upper:env:NOPE}}", &[]).unwrap_err();
        assert_eq!(err.0, vec!["upper:env:NOPE".to_string()]);
    }

    #[test]
    fn a_malformed_call_is_reported_rather_than_silently_expanded() {
        let err = expand_env("a: {{env:}}", &[]).unwrap_err();
        assert!(err.0[0].contains("env needs a variable name"), "{:?}", err.0);
    }

    #[test]
    fn runaway_nesting_is_bounded() {
        let text = format!("a: {{{{{}hello{}}}}}", "upper:".repeat(20), "");
        let err = expand_with_registry(&text, &registry(&[])).unwrap_err();
        assert!(err.0[0].contains("nested deeper"), "{:?}", err.0);
    }

    // ── extension ────────────────────────────────────────────────────────────

    #[test]
    fn a_head_containing_digits_is_never_a_provider() {
        // The rule that keeps the shorthand unambiguous: RUSTS3_PORT has a
        // digit, so it is an environment variable, full stop.
        assert!(!is_provider_name("RUSTS3_PORT"));
        assert!(!is_provider_name("env2"));
        assert!(!is_provider_name(""));
        assert!(is_provider_name("env"));
        assert!(is_provider_name("to_upper"));

        // `env2` therefore reads as a variable name with a default, not a call.
        let out = expand_env("a: {{env2:fallback}}", &[("env2", "value")]).unwrap();
        assert_eq!(out, "a: value");
        assert_eq!(expand_env("a: {{env2:fallback}}", &[]).unwrap(), "a: fallback");
    }

    #[test]
    fn everything_after_the_first_separator_is_the_default() {
        // Stated plainly: in {{a:b}}, b runs to the end, colons and all.
        assert_eq!(
            expand_env("a: {{VAR:b:c:d}}", &[]).unwrap(),
            "a: b:c:d"
        );
        // …and the same when the provider is explicit.
        assert_eq!(
            expand_env("a: {{env:VAR:b:c:d}}", &[]).unwrap(),
            "a: b:c:d"
        );
    }

    #[test]
    fn a_custom_provider_can_be_registered() {
        struct Reverse;
        impl Resolver for Reverse {
            fn name(&self) -> &'static str {
                "reverse"
            }
            fn resolve(&self, call: &Call<'_>) -> Result<Option<String>, String> {
                Ok(call.param(0).map(|v| v.chars().rev().collect()))
            }
        }
        let mut registry = Registry::with_defaults();
        registry.register(Box::new(Reverse));
        let out = expand_with_registry("a: {{reverse:abc}}", &registry).unwrap();
        assert_eq!(out, "a: cba");
    }

    #[test]
    fn a_realistic_document_expands_end_to_end() {
        let text = concat!(
            "server:\n",
            "  bind_address: \"{{RUSTS3_BIND_ADDRESS:0.0.0.0}}\"\n",
            "  bind_port: {{RUSTS3_PORT:8002}}\n",
            "  base_dir: \"{{env:RUSTS3_DATA_DIR:/data}}\"\n",
            "auth:\n",
            "  enabled: {{RUSTS3_AUTH_ENABLED:true}}\n",
            "  users:\n",
            "    - user: \"{{lower:env:RUSTS3_ADMIN_USER}}\"\n",
            "      password: \"{{RUSTS3_ADMIN_PASSWORD:changeme}}\"\n",
        );
        let out = expand_env(
            text,
            &[
                ("RUSTS3_PORT", "9000"),
                ("RUSTS3_ADMIN_PASSWORD", "s3cret"),
                ("RUSTS3_ADMIN_USER", "Admin"),
            ],
        )
        .unwrap();
        assert!(out.contains("bind_port: 9000"));
        assert!(out.contains("bind_address: \"0.0.0.0\""));
        assert!(out.contains("base_dir: \"/data\""));
        assert!(out.contains("user: \"admin\""), "{out}");
        assert!(out.contains("password: \"s3cret\""));
        let parsed: serde_yaml::Value = serde_yaml::from_str(&out).unwrap();
        assert_eq!(parsed["server"]["bind_port"].as_u64(), Some(9000));
    }
}
