//! Outbound webhook HTTP: request signing, custom headers, and the SSRF
//! guard (URL validation plus a resolver that refuses internal addresses —
//! the connection goes to an address that was checked, so DNS rebinding
//! cannot slip past it).

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, KeyInit, Mac};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use sha2::Sha256;

use crate::server::config::WebhookConfig;

pub const HEADER_SIGNATURE: &str = "x-rs3-signature";
pub const HEADER_DELIVERY: &str = "x-rs3-delivery";
pub const HEADER_SUBSCRIPTION: &str = "x-rs3-subscription";

/// Headers a webhook may not set: framing / routing headers the client owns,
/// and our own signature headers.
pub fn header_is_reserved(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(
        name.as_str(),
        "host"
            | "content-length"
            | "content-type"
            | "transfer-encoding"
            | "connection"
            | "upgrade"
            | "te"
            | "trailer"
            | "keep-alive"
            | "expect"
    ) || name.starts_with("proxy-")
        || name.starts_with("x-rs3-")
}

pub fn validate_headers(headers: &BTreeMap<String, String>) -> Result<(), String> {
    for (name, value) in headers {
        if reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_err() {
            return Err(format!("invalid header name {name:?}"));
        }
        if header_is_reserved(name) {
            return Err(format!("header {name:?} is reserved and cannot be set"));
        }
        if reqwest::header::HeaderValue::from_str(value).is_err() {
            return Err(format!("invalid value for header {name:?}"));
        }
    }
    Ok(())
}

/// Addresses a webhook may never reach unless explicitly allowed.
pub fn ip_is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_multicast()
                || o[0] == 0
                || (o[0] == 100 && (64..128).contains(&o[1])) // CGNAT 100.64/10
                || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0/24
                || (o[0] == 198 && (o[1] == 18 || o[1] == 19)) // benchmarking
                || o[0] >= 240
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return ip_is_internal(IpAddr::V4(v4));
            }
            let seg = v6.segments();
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (seg[0] & 0xfe00) == 0xfc00 // unique local fc00::/7
                || (seg[0] & 0xffc0) == 0xfe80 // link local fe80::/10
                || (seg[0] == 0x64 && seg[1] == 0xff9b) // NAT64 64:ff9b::/96
        }
    }
}

/// Checks a webhook URL against the config: scheme, and (for an IP-literal
/// host) the address guard. Hostnames are checked at connect time.
pub fn validate_url(url: &str, cfg: &WebhookConfig) -> Result<reqwest::Url, String> {
    let parsed = reqwest::Url::parse(url).map_err(|err| format!("invalid URL: {err}"))?;
    match parsed.scheme() {
        "https" => {}
        "http" if cfg.allow_http => {}
        "http" => {
            return Err("plain http:// webhooks are disabled (webhooks.allow_http)".to_string())
        }
        other => return Err(format!("unsupported URL scheme {other:?}")),
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("put credentials in a custom header, not in the URL".to_string());
    }
    let Some(host) = parsed.host_str() else {
        return Err("URL has no host".to_string());
    };
    if !cfg.allow_private_addresses {
        // IP-literal hosts never reach the resolver, so check them here.
        let literal = host.trim_start_matches('[').trim_end_matches(']').parse::<IpAddr>().ok();
        if let Some(ip) = literal.filter(|ip| ip_is_internal(*ip)) {
            return Err(format!(
                "{ip} is an internal address (webhooks.allow_private_addresses is off)"
            ));
        }
    }
    Ok(parsed)
}

/// DNS resolver that drops internal addresses, failing when none is left.
struct GuardedResolver;

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_string();
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .filter(|addr| !ip_is_internal(addr.ip()))
                .collect();
            if addrs.is_empty() {
                return Err(format!("{host} resolves only to internal addresses").into());
            }
            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

pub fn build_client(cfg: &WebhookConfig) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(cfg.request_timeout_secs.max(1)))
        .connect_timeout(Duration::from_secs(cfg.request_timeout_secs.clamp(1, 10)))
        .user_agent(concat!("rusts3-webhook/", env!("CARGO_PKG_VERSION")));
    if !cfg.allow_private_addresses {
        builder = builder.dns_resolver(Arc::new(GuardedResolver));
    }
    builder.build().expect("webhook HTTP client builds")
}

/// `t=<unix secs>,v1=<hex HMAC-SHA256(secret, "<t>.<body>")>`.
pub fn signature(secret: &str, unix_secs: i64, body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(secret.as_bytes()).expect("any key length");
    mac.update(unix_secs.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("t={unix_secs},v1={hex}")
}

/// Why a delivery did not succeed.
#[derive(Debug, Clone)]
pub struct Failure {
    pub status: Option<u16>,
    pub message: String,
    /// The receiver's `Retry-After`, in seconds.
    pub retry_after_secs: Option<u64>,
}

pub struct Request<'a> {
    pub url: &'a str,
    pub subscription: &'a str,
    pub delivery_id: &'a str,
    pub secret: &'a str,
    pub headers: &'a BTreeMap<String, String>,
    pub body: Vec<u8>,
}

/// POSTs one batch. Any non-2xx response or transport error is a failure.
pub async fn send(client: &reqwest::Client, cfg: &WebhookConfig, request: Request<'_>) -> Result<u16, Failure> {
    let url = validate_url(request.url, cfg).map_err(|message| Failure {
        status: None,
        message,
        retry_after_secs: None,
    })?;
    let now = chrono::Utc::now().timestamp();
    let mut builder = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(HEADER_SUBSCRIPTION, request.subscription)
        .header(HEADER_DELIVERY, request.delivery_id)
        .header(HEADER_SIGNATURE, signature(request.secret, now, &request.body));
    for (name, value) in request.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    let response = builder.body(request.body).send().await.map_err(|err| Failure {
        status: None,
        message: describe_transport_error(&err),
        retry_after_secs: None,
    })?;
    let status = response.status();
    if status.is_success() {
        return Ok(status.as_u16());
    }
    let retry_after_secs = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_retry_after);
    Err(Failure {
        status: Some(status.as_u16()),
        message: format!("receiver answered {status}"),
        retry_after_secs,
    })
}

fn describe_transport_error(err: &reqwest::Error) -> String {
    // The full chain names the actual cause (refused, DNS, TLS, guard).
    let mut message = if err.is_timeout() {
        "request timed out".to_string()
    } else if err.is_connect() {
        "connection failed".to_string()
    } else {
        "request failed".to_string()
    };
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

/// `Retry-After` as delta-seconds or an HTTP date.
pub fn parse_retry_after(value: &str) -> Option<u64> {
    let value = value.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(secs);
    }
    let at = crate::storage::time::parse_http_date_ms(value)?;
    let delta = at - crate::storage::time::now_ms();
    Some((delta.max(0) / 1000) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_addresses_are_refused() {
        for ip in ["127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "169.254.169.254", "100.64.0.1", "0.0.0.0", "::1", "fd00::1", "fe80::1", "::ffff:127.0.0.1"] {
            assert!(ip_is_internal(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["1.1.1.1", "8.8.8.8", "2606:4700::1111"] {
            assert!(!ip_is_internal(ip.parse().unwrap()), "{ip}");
        }
    }

    #[test]
    fn url_rules() {
        let strict = WebhookConfig::default();
        assert!(validate_url("https://example.com/hook", &strict).is_ok());
        assert!(validate_url("http://example.com/hook", &strict).is_err());
        assert!(validate_url("https://127.0.0.1/hook", &strict).is_err());
        assert!(validate_url("https://[::1]/hook", &strict).is_err());
        assert!(validate_url("https://u:p@example.com/", &strict).is_err());
        assert!(validate_url("ftp://example.com/", &strict).is_err());
        let open = WebhookConfig {
            allow_http: true,
            allow_private_addresses: true,
            ..WebhookConfig::default()
        };
        assert!(validate_url("http://127.0.0.1:9/hook", &open).is_ok());
    }

    #[test]
    fn reserved_headers_are_rejected() {
        let mut headers = BTreeMap::new();
        headers.insert("Authorization".to_string(), "Bearer x".to_string());
        assert!(validate_headers(&headers).is_ok());
        for name in ["Host", "content-type", "X-RS3-Signature", "Proxy-Authorization"] {
            let mut bad = BTreeMap::new();
            bad.insert(name.to_string(), "v".to_string());
            assert!(validate_headers(&bad).is_err(), "{name}");
        }
    }

    #[test]
    fn signature_is_stable() {
        let a = signature("secret", 1700000000, b"{}");
        assert_eq!(a, signature("secret", 1700000000, b"{}"));
        assert!(a.starts_with("t=1700000000,v1="));
        assert_ne!(a, signature("other", 1700000000, b"{}"));
    }

    #[test]
    fn retry_after_forms() {
        assert_eq!(parse_retry_after("120"), Some(120));
        assert_eq!(parse_retry_after("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
        assert_eq!(parse_retry_after("soon"), None);
    }
}
