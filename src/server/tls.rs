//! TLS for the server's own outbound connections: webhook deliveries and
//! replication pushes/pulls to a peer site.
//!
//! The crypto is [graviola] (via `rustls-graviola`): Rust, plus formally
//! verified assembly from s2n-bignum for the big-number and curve arithmetic.
//! No C is compiled into the server for TLS — `ring` and `aws-lc` are both
//! kept out of the release build on purpose.
//!
//! graviola needs a reasonably modern CPU (x86_64: AES-NI, PCLMULQDQ, AVX2,
//! BMI1, ADX — roughly 2013 onward; aarch64: AES, PMULL, SHA2, NEON — absent
//! on e.g. a Raspberry Pi 4). On a CPU without them it would panic mid-
//! handshake, so [`https_unavailable`] is checked before any `https://` URL is
//! accepted or contacted; plain `http://` keeps working either way.
//!
//! [graviola]: https://github.com/ctz/graviola

use std::sync::{Arc, OnceLock};

/// Why outbound HTTPS cannot be used on this machine, if it cannot.
pub(crate) fn https_unavailable() -> Option<&'static str> {
    (!graviola::util::is_cpu_supported()).then_some(
        "this CPU lacks the instructions the TLS library needs \
         (x86_64: AES-NI, PCLMULQDQ, AVX2, BMI1, ADX; aarch64: AES, PMULL, SHA2), \
         so https:// endpoints cannot be used — use http:// or run on a newer CPU",
    )
}

/// The client TLS configuration every outbound `reqwest::Client` is built
/// with: graviola crypto, TLS 1.2 and 1.3, Mozilla's root certificates
/// (bundled, so the host's CA store is not consulted), HTTP/1.1 only.
pub(crate) fn client_config() -> rustls::ClientConfig {
    static CONFIG: OnceLock<rustls::ClientConfig> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
            let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls_graviola::default_provider()))
                .with_safe_default_protocol_versions()
                .expect("graviola supports the default TLS versions")
                .with_root_certificates(roots)
                .with_no_client_auth();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            config
        })
        .clone()
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_config_uses_graviola_and_bundled_roots() {
        let config = super::client_config();
        assert!(!config.crypto_provider().cipher_suites.is_empty());
        // This machine runs the test suite, so it must be able to do TLS.
        assert_eq!(super::https_unavailable(), None);
    }

    /// reqwest only accepts a preconfigured config of *its own* rustls
    /// version; any other is an "unknown TLS backend" and `build()` fails.
    /// This catches the two drifting apart on a dependency upgrade.
    #[test]
    fn reqwest_accepts_the_config() {
        reqwest::Client::builder().tls_backend_preconfigured(super::client_config()).build().unwrap();
    }

    /// A real handshake against a public HTTPS endpoint, through the same
    /// reqwest setup the webhook and replication clients use. Needs network,
    /// so it is opt-in: `cargo test -- --ignored tls_handshake`.
    #[tokio::test]
    #[ignore]
    async fn tls_handshake_against_a_public_site() {
        let client = reqwest::Client::builder().tls_backend_preconfigured(super::client_config()).build().unwrap();
        let response = client.get("https://www.rust-lang.org/").send().await.unwrap();
        assert!(response.status().is_success(), "{}", response.status());
    }
}
