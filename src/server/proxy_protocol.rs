//! PROXY protocol (v1 and v2) on the listening sockets, auto-detected.
//!
//! A TCP-level load balancer (HAProxy, nginx `stream`, an AWS NLB, …) hides the
//! client's address: every connection appears to come from the balancer. The
//! PROXY protocol fixes that by having the balancer send one small header ahead
//! of the client's bytes naming the real source. This module reads that header
//! and serves the connection as if it had come from there — the address lands
//! in the same `ConnectInfo` the rest of the server already reads, so access
//! keys' "last used from" and the auth log are right without further changes.
//!
//! **Auto-detected, per connection.** The first bytes are peeked at: a PROXY
//! signature is consumed and honored, anything else is left untouched and
//! served as plain HTTP. One port therefore takes both a balancer's traffic and
//! direct clients (health checks, `curl` on the box), with nothing to switch.
//! Neither signature can begin an HTTP request — v2 opens with `\r\n\r\n\0`,
//! and no HTTP method is spelled `PROXY`.
//!
//! **Only a balancer may speak for a client.** A header is honored only from a
//! peer on a loopback, private, or link-local address — where a reverse proxy
//! lives, and the same rule `auth::client_ip` applies to `X-Forwarded-For`.
//! From a public peer it is a forgery attempt (or a misconfiguration worth
//! noticing), and the connection is dropped rather than served under a claimed
//! address.
//!
//! This is also the server's accept loop: axum 0.7's `serve` takes nothing but
//! a bare `TcpListener`, leaving no place to look at a connection before HTTP
//! does. [`serve`] is the same hyper machinery underneath — HTTP/1 (all this
//! server has ever spoken), upgrades (the console's WebSocket), graceful
//! shutdown.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use axum::extract::ConnectInfo;
use axum::http::Request;
use axum::Router;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;

use super::config::ProxyProtocolMode;

/// The 12 bytes every v2 header starts with.
const V2_SIGNATURE: [u8; 12] = *b"\r\n\r\n\0\r\nQUIT\n";
/// What every v1 header starts with.
const V1_SIGNATURE: &[u8] = b"PROXY ";
/// Longest legal v1 line, CRLF included (the spec's worst case, `UNKNOWN` with
/// two full IPv6 addresses).
const V1_MAX_LINE: usize = 107;
/// v2's fixed part: signature, version/command, family/protocol, length.
const V2_FIXED: usize = 16;
/// Upper bound on the variable part of a v2 header. The addresses need 36
/// bytes at most; the rest is TLVs (an NLB's endpoint id, TLS details), which
/// are small. The wire format allows 65535 — far more than a sender has any
/// business making us buffer before a request even starts.
const V2_MAX_PAYLOAD: usize = 4096;
/// How long a connection may take to deliver a header it has begun.
const HEADER_TIMEOUT: Duration = Duration::from_secs(5);

/// What a PROXY header says about where the connection came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// The client the balancer is relaying.
    Client(SocketAddr),
    /// No client to name: the balancer's own connection (v2 `LOCAL`, used for
    /// health checks) or an address family it could not express (`UNKNOWN`,
    /// `UNSPEC`, Unix sockets). The TCP peer stands.
    Peer,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Parses a v1 line, `PROXY TCP4 <src> <dst> <sport> <dport>\r\n`.
fn parse_v1(line: &[u8]) -> io::Result<Source> {
    let line = std::str::from_utf8(line).map_err(|_| invalid("PROXY v1 header is not ASCII"))?;
    let line = line
        .strip_suffix("\r\n")
        .ok_or_else(|| invalid("PROXY v1 header does not end in CRLF"))?;
    let mut fields = line.split(' ');
    if fields.next() != Some("PROXY") {
        return Err(invalid("not a PROXY v1 header"));
    }
    let family = fields.next().ok_or_else(|| invalid("PROXY v1 header names no protocol"))?;
    if family == "UNKNOWN" {
        // Everything after it is to be ignored, per the spec.
        return Ok(Source::Peer);
    }
    let (Some(src), Some(_dst), Some(sport), Some(_dport), None) =
        (fields.next(), fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return Err(invalid("PROXY v1 header has the wrong number of fields"));
    };
    if !matches!(family, "TCP4" | "TCP6") {
        return Err(invalid(format!("PROXY v1 protocol {family:?} is not supported")));
    }
    // The address speaks for itself; the TCP4/TCP6 label is not held against
    // it. Senders get the label wrong — curl's `--haproxy-clientip` labels by
    // its own connection, so an IPv6 client over an IPv4 hop arrives as `TCP4`
    // — and nothing is gained by refusing a well-formed address over it.
    let ip: IpAddr = src.parse().map_err(|_| invalid("PROXY v1 source is not an IP address"))?;
    let port: u16 = sport.parse().map_err(|_| invalid("PROXY v1 source port is not a port"))?;
    Ok(Source::Client(SocketAddr::new(ip, port)))
}

/// Parses a v2 header. `fixed` is its first 16 bytes; `payload` is the
/// `length`-byte remainder they announce (addresses, then TLVs we skip).
fn parse_v2(fixed: &[u8; V2_FIXED], payload: &[u8]) -> io::Result<Source> {
    if fixed[..12] != V2_SIGNATURE {
        return Err(invalid("not a PROXY v2 header"));
    }
    let (version, command) = (fixed[12] >> 4, fixed[12] & 0x0f);
    if version != 2 {
        return Err(invalid(format!("PROXY protocol version {version} is not supported")));
    }
    match command {
        0 => return Ok(Source::Peer), // LOCAL
        1 => {}                       // PROXY
        other => return Err(invalid(format!("PROXY v2 command {other} is not supported"))),
    }
    // High nibble: address family. The transport (low nibble) does not matter
    // to where the client is.
    match fixed[13] >> 4 {
        1 => {
            let block = payload.get(..12).ok_or_else(|| invalid("PROXY v2 IPv4 block is truncated"))?;
            let ip = Ipv4Addr::new(block[0], block[1], block[2], block[3]);
            let port = u16::from_be_bytes([block[8], block[9]]);
            Ok(Source::Client(SocketAddr::new(IpAddr::V4(ip), port)))
        }
        2 => {
            let block = payload.get(..36).ok_or_else(|| invalid("PROXY v2 IPv6 block is truncated"))?;
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&block[..16]);
            let port = u16::from_be_bytes([block[32], block[33]]);
            Ok(Source::Client(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port)))
        }
        // UNSPEC and Unix sockets: nothing we can call a remote address.
        _ => Ok(Source::Peer),
    }
}

/// The address to serve a connection as, given who connected and what (if
/// anything) their PROXY header claimed. See the module docs for why only a
/// private peer is believed.
fn effective_addr(peer: SocketAddr, header: Option<Source>) -> io::Result<SocketAddr> {
    let Some(source) = header else {
        return Ok(peer);
    };
    if !super::auth::may_be_proxy(&peer.ip().to_canonical()) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "PROXY header from a peer that is not on a loopback or private network",
        ));
    }
    Ok(match source {
        Source::Client(addr) => SocketAddr::new(addr.ip().to_canonical(), addr.port()),
        Source::Peer => peer,
    })
}

/// Which header, if any, a connection opens with.
enum Detected {
    V1,
    V2,
    Plain,
}

/// Looks at a connection's first bytes without consuming them. Bytes can
/// arrive a few at a time, so an undecided prefix is looked at again until it
/// resolves one way or the other; one that never does is simply not a PROXY
/// header, and is left for HTTP to make sense of.
async fn detect(stream: &TcpStream) -> io::Result<Detected> {
    let mut buf = [0u8; V2_SIGNATURE.len()];
    let deadline = tokio::time::Instant::now() + HEADER_TIMEOUT;
    loop {
        // Waits for the first byte for as long as it takes — an idle
        // connection is HTTP's to time out, exactly as before.
        let n = stream.peek(&mut buf).await?;
        let seen = &buf[..n];
        if n == 0 {
            return Ok(Detected::Plain);
        }
        if seen == V2_SIGNATURE {
            return Ok(Detected::V2);
        }
        if seen.starts_with(V1_SIGNATURE) {
            return Ok(Detected::V1);
        }
        let undecided = V2_SIGNATURE.starts_with(seen) || V1_SIGNATURE.starts_with(seen);
        if !undecided || tokio::time::Instant::now() >= deadline {
            return Ok(Detected::Plain);
        }
        // `peek` returns at once while the same bytes sit in the buffer, so
        // pace the re-check instead of spinning on it.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// Consumes a connection's PROXY header, if it has one. Reads exactly the
/// header and not a byte more, so the stream is left at the start of the HTTP
/// request and can be handed to hyper as it is.
async fn read_header(stream: &mut TcpStream) -> io::Result<Option<Source>> {
    match detect(stream).await? {
        Detected::Plain => Ok(None),
        Detected::V1 => {
            // The line's length is only known at its CRLF. Byte-wise reads are
            // what keep us from swallowing request bytes; it is ≤107 of them,
            // once per connection.
            let mut line = Vec::with_capacity(V1_MAX_LINE);
            while !line.ends_with(b"\r\n") {
                if line.len() == V1_MAX_LINE {
                    return Err(invalid("PROXY v1 header is longer than 107 bytes"));
                }
                line.push(stream.read_u8().await?);
            }
            parse_v1(&line).map(Some)
        }
        Detected::V2 => {
            let mut fixed = [0u8; V2_FIXED];
            stream.read_exact(&mut fixed).await?;
            let length = u16::from_be_bytes([fixed[14], fixed[15]]) as usize;
            if length > V2_MAX_PAYLOAD {
                return Err(invalid(format!("PROXY v2 header announces {length} bytes; refusing more than {V2_MAX_PAYLOAD}")));
            }
            let mut payload = vec![0u8; length];
            stream.read_exact(&mut payload).await?;
            parse_v2(&fixed, &payload).map(Some)
        }
    }
}

/// Resolves the address to serve `stream` as, consuming its PROXY header.
async fn client_addr(stream: &mut TcpStream, peer: SocketAddr, mode: ProxyProtocolMode) -> io::Result<SocketAddr> {
    if mode == ProxyProtocolMode::Off {
        return Ok(peer);
    }
    let header = tokio::time::timeout(HEADER_TIMEOUT * 2, read_header(stream))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "PROXY header was not completed in time"))??;
    effective_addr(peer, header)
}

/// Accept errors that concern one connection only and need no back-off.
fn is_connection_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset
    )
}

/// Serves `app` on `listener` until `shutdown` fires, then stops accepting,
/// lets in-flight requests finish, and returns once every connection has
/// closed. Each request carries `ConnectInfo<SocketAddr>` — the client named by
/// the connection's PROXY header when it has one, the TCP peer otherwise.
pub(crate) async fn serve(
    listener: TcpListener,
    app: Router,
    shutdown: CancellationToken,
    mode: ProxyProtocolMode,
    label: &'static str,
) -> io::Result<()> {
    // Every connection task holds a sender; `recv` yields `None` when the last
    // one is gone, which is how we know the listener has drained.
    let (alive, mut drained) = tokio::sync::mpsc::channel::<()>(1);
    loop {
        let (stream, peer) = tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok(connection) => connection,
                Err(err) if is_connection_error(&err) => continue,
                Err(err) => {
                    // Out of file descriptors, most likely. Back off rather
                    // than spin on an error that will not clear by itself.
                    log::error!("{label}: accept failed: {err}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            },
        };
        let (app, shutdown, alive) = (app.clone(), shutdown.clone(), alive.clone());
        tokio::spawn(async move {
            serve_connection(stream, peer, app, shutdown, mode, label).await;
            drop(alive);
        });
    }
    drop(listener);
    drop(alive);
    drained.recv().await;
    Ok(())
}

async fn serve_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    app: Router,
    shutdown: CancellationToken,
    mode: ProxyProtocolMode,
    label: &'static str,
) {
    let client = tokio::select! {
        _ = shutdown.cancelled() => return,
        resolved = client_addr(&mut stream, peer, mode) => match resolved {
            Ok(client) => client,
            Err(err) => {
                log::warn!("{label}: dropping connection from {peer}: {err}");
                return;
            }
        },
    };
    if client != peer {
        log::debug!("{label}: connection from {peer} is relaying {client} (PROXY protocol)");
    }

    let service = ServiceBuilder::new()
        .map_request(move |mut request: Request<Incoming>| {
            request.extensions_mut().insert(ConnectInfo(client));
            request
        })
        .service(app);
    let connection = http1::Builder::new()
        .serve_connection(TokioIo::new(stream), TowerToHyperService::new(service))
        .with_upgrades();
    tokio::pin!(connection);
    let mut draining = false;
    loop {
        tokio::select! {
            result = connection.as_mut() => {
                if let Err(err) = result {
                    log::trace!("{label}: connection from {client} ended: {err}");
                }
                return;
            }
            _ = shutdown.cancelled(), if !draining => {
                // Finish the request in flight, refuse the next one.
                connection.as_mut().graceful_shutdown();
                draining = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use tokio::io::AsyncWriteExt;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn v2(command: u8, family: u8, payload: &[u8]) -> ([u8; V2_FIXED], Vec<u8>) {
        let mut fixed = [0u8; V2_FIXED];
        fixed[..12].copy_from_slice(&V2_SIGNATURE);
        fixed[12] = 0x20 | command;
        fixed[13] = family;
        fixed[14..].copy_from_slice(&(payload.len() as u16).to_be_bytes());
        (fixed, payload.to_vec())
    }

    fn v2_tcp4(src: [u8; 4], sport: u16, tlvs: &[u8]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&src);
        payload.extend_from_slice(&[10, 0, 0, 1]);
        payload.extend_from_slice(&sport.to_be_bytes());
        payload.extend_from_slice(&443u16.to_be_bytes());
        payload.extend_from_slice(tlvs);
        let (fixed, payload) = v2(1, 0x11, &payload);
        [fixed.to_vec(), payload].concat()
    }

    #[test]
    fn v1_headers_parse() {
        assert_eq!(
            parse_v1(b"PROXY TCP4 203.0.113.7 10.0.0.1 56324 443\r\n").unwrap(),
            Source::Client(addr("203.0.113.7:56324"))
        );
        assert_eq!(
            parse_v1(b"PROXY TCP6 2001:db8::7 2001:db8::1 56324 443\r\n").unwrap(),
            Source::Client(addr("[2001:db8::7]:56324"))
        );
        // A mislabelled family is tolerated (this is what curl sends for an
        // IPv6 `--haproxy-clientip` over an IPv4 connection).
        assert_eq!(
            parse_v1(b"PROXY TCP4 2001:db8::7 127.0.0.1 56324 443\r\n").unwrap(),
            Source::Client(addr("[2001:db8::7]:56324"))
        );
        assert_eq!(
            parse_v1(b"PROXY TCP6 203.0.113.7 10.0.0.1 56324 443\r\n").unwrap(),
            Source::Client(addr("203.0.113.7:56324"))
        );
        assert_eq!(parse_v1(b"PROXY UNKNOWN\r\n").unwrap(), Source::Peer);
        assert_eq!(parse_v1(b"PROXY UNKNOWN ffff::1 ffff::2 1 2\r\n").unwrap(), Source::Peer);
        for bad in [
            &b"PROXY TCP4 203.0.113.7 10.0.0.1 56324 443\n"[..], // bare LF
            b"PROXY TCP4 203.0.113.7 10.0.0.1 56324\r\n",        // a field short
            b"PROXY TCP4 203.0.113.7 10.0.0.1 56324 443 x\r\n",  // a field over
            b"PROXY TCP4 203.0.113.700 10.0.0.1 56324 443\r\n",
            b"PROXY TCP4 203.0.113.7 10.0.0.1 99999 443\r\n",
            b"PROXY UDP4 203.0.113.7 10.0.0.1 56324 443\r\n",
            b"PROXY\r\n",
            b"PROXY TCP4 203.0.113.7 10.0.0.1 56324 443\r\n\xff",
        ] {
            assert!(parse_v1(bad).is_err(), "{:?}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn v2_headers_parse() {
        let tcp4 = v2_tcp4([203, 0, 113, 7], 56324, &[]);
        let (fixed, payload) = tcp4.split_at(V2_FIXED);
        let fixed: [u8; V2_FIXED] = fixed.try_into().unwrap();
        assert_eq!(parse_v2(&fixed, payload).unwrap(), Source::Client(addr("203.0.113.7:56324")));
        // Trailing TLVs are skipped, not mistaken for anything.
        let with_tlv = v2_tcp4([203, 0, 113, 7], 56324, &[0xEA, 0x00, 0x03, 1, 2, 3]);
        let (f, p) = with_tlv.split_at(V2_FIXED);
        assert_eq!(parse_v2(f.try_into().unwrap(), p).unwrap(), Source::Client(addr("203.0.113.7:56324")));

        let mut v6 = Vec::new();
        v6.extend_from_slice(&"2001:db8::7".parse::<Ipv6Addr>().unwrap().octets());
        v6.extend_from_slice(&"2001:db8::1".parse::<Ipv6Addr>().unwrap().octets());
        v6.extend_from_slice(&56324u16.to_be_bytes());
        v6.extend_from_slice(&443u16.to_be_bytes());
        let (fixed, payload) = v2(1, 0x21, &v6);
        assert_eq!(parse_v2(&fixed, &payload).unwrap(), Source::Client(addr("[2001:db8::7]:56324")));

        // LOCAL (a balancer's health check), UNSPEC, and Unix sockets name no client.
        let (fixed, payload) = v2(0, 0x00, &[]);
        assert_eq!(parse_v2(&fixed, &payload).unwrap(), Source::Peer);
        let (fixed, payload) = v2(1, 0x00, &[]);
        assert_eq!(parse_v2(&fixed, &payload).unwrap(), Source::Peer);
        let (fixed, payload) = v2(1, 0x31, &[0u8; 216]);
        assert_eq!(parse_v2(&fixed, &payload).unwrap(), Source::Peer);

        // Truncated address blocks, an unknown command, another version.
        let (fixed, payload) = v2(1, 0x11, &[1, 2, 3]);
        assert!(parse_v2(&fixed, &payload).is_err());
        let (fixed, payload) = v2(1, 0x21, &[0u8; 12]);
        assert!(parse_v2(&fixed, &payload).is_err());
        let (fixed, payload) = v2(2, 0x11, &[0u8; 12]);
        assert!(parse_v2(&fixed, &payload).is_err());
        let (mut fixed, payload) = v2(1, 0x11, &[0u8; 12]);
        fixed[12] = 0x31;
        assert!(parse_v2(&fixed, &payload).is_err());
    }

    #[test]
    fn only_a_private_peer_may_speak_for_a_client() {
        let client = Source::Client(addr("203.0.113.7:56324"));
        // No header: the peer, whoever it is.
        assert_eq!(effective_addr(addr("198.51.100.1:1000"), None).unwrap(), addr("198.51.100.1:1000"));
        // Where a balancer lives: believed.
        for peer in ["127.0.0.1:1000", "10.1.2.3:1000", "192.168.44.99:1000", "172.16.0.9:1000", "[::1]:1000", "[fd00::1]:1000", "[::ffff:10.1.2.3]:1000"] {
            assert_eq!(effective_addr(addr(peer), Some(client)).unwrap(), addr("203.0.113.7:56324"), "{peer}");
        }
        // Anyone on the internet: refused outright, not served under the claim
        // and not quietly served under their own address either.
        for peer in ["198.51.100.1:1000", "8.8.8.8:1000", "[2001:db8::99]:1000"] {
            let err = effective_addr(addr(peer), Some(client)).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied, "{peer}");
        }
        // A balancer's own connection keeps the balancer's address.
        assert_eq!(effective_addr(addr("10.1.2.3:1000"), Some(Source::Peer)).unwrap(), addr("10.1.2.3:1000"));
        // An IPv4 client relayed over an IPv6 hop reads as plain IPv4.
        let mapped = Source::Client(addr("[::ffff:203.0.113.7]:56324"));
        assert_eq!(effective_addr(addr("127.0.0.1:1"), Some(mapped)).unwrap(), addr("203.0.113.7:56324"));
    }

    /// A server whose one route answers with the address the request was
    /// served as.
    async fn start(mode: ProxyProtocolMode) -> (SocketAddr, CancellationToken, tokio::task::JoinHandle<io::Result<()>>) {
        let app = Router::new().route(
            "/whoami",
            get(|ConnectInfo(from): ConnectInfo<SocketAddr>| async move { from.to_string() }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let server = tokio::spawn(serve(listener, app, shutdown.clone(), mode, "test"));
        (bound, shutdown, server)
    }

    const REQUEST: &[u8] = b"GET /whoami HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n";

    /// Writes `chunks` one at a time (so a header can straddle packets), then
    /// returns the whole response — empty if the server closed without one.
    async fn exchange(server: SocketAddr, chunks: &[&[u8]]) -> (SocketAddr, String) {
        let mut stream = TcpStream::connect(server).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let local = stream.local_addr().unwrap();
        for chunk in chunks {
            if stream.write_all(chunk).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        let mut response = Vec::new();
        let _ = stream.read_to_end(&mut response).await;
        (local, String::from_utf8_lossy(&response).into_owned())
    }

    fn body(response: &str) -> &str {
        response.split("\r\n\r\n").nth(1).unwrap_or("")
    }

    #[tokio::test]
    async fn one_port_serves_plain_http_and_both_proxy_versions() {
        let (server, shutdown, task) = start(ProxyProtocolMode::Auto).await;

        // Plain HTTP, exactly as before: the TCP peer.
        let (local, response) = exchange(server, &[REQUEST]).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(body(&response), local.to_string());

        // v1, header and request in one write — nothing of the request is eaten.
        let v1 = b"PROXY TCP4 203.0.113.7 10.0.0.1 56324 443\r\n";
        let (_, response) = exchange(server, &[&[&v1[..], REQUEST].concat()]).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(body(&response), "203.0.113.7:56324");

        // v2 with TLVs, likewise.
        let header = v2_tcp4([198, 51, 100, 9], 40000, &[0xEA, 0x00, 0x02, 9, 9]);
        let (_, response) = exchange(server, &[&[&header[..], REQUEST].concat()]).await;
        assert_eq!(body(&response), "198.51.100.9:40000");

        // IPv6 client.
        let (_, response) = exchange(server, &[b"PROXY TCP6 2001:db8::7 2001:db8::1 56324 443\r\n", REQUEST]).await;
        assert_eq!(body(&response), "[2001:db8::7]:56324");

        // Headers that arrive in pieces — split inside the signature itself,
        // where detection has to wait rather than guess.
        let (_, response) = exchange(server, &[b"PR", b"OXY TCP4 203.0.1", b"13.8 10.0.0.1 1234 443\r", b"\n", REQUEST]).await;
        assert_eq!(body(&response), "203.0.113.8:1234");
        let (_, response) = exchange(server, &[&header[..5], &header[5..14], &header[14..], REQUEST]).await;
        assert_eq!(body(&response), "198.51.100.9:40000");
        // …and a plain request that arrives in pieces is still a plain request.
        let (local, response) = exchange(server, &[b"GE", b"T /whoami HTTP/1.1\r\nhost: t\r\nconn", b"ection: close\r\n\r\n"]).await;
        assert_eq!(body(&response), local.to_string());

        // A balancer's health check (v2 LOCAL) and v1 UNKNOWN keep the peer.
        let (fixed, _) = v2(0, 0x00, &[]);
        let (local, response) = exchange(server, &[&fixed, REQUEST]).await;
        assert_eq!(body(&response), local.to_string());
        let (local, response) = exchange(server, &[b"PROXY UNKNOWN\r\n", REQUEST]).await;
        assert_eq!(body(&response), local.to_string());

        // Garbage that starts like a header is dropped, never served.
        let (_, response) = exchange(server, &[b"PROXY TCP4 not-an-ip 10.0.0.1 1 2\r\n", REQUEST]).await;
        assert_eq!(response, "", "served after a bad header");
        let overlong = [b"PROXY TCP4 ".to_vec(), vec![b'1'; 200], b"\r\n".to_vec()].concat();
        let (_, response) = exchange(server, &[&overlong, REQUEST]).await;
        assert_eq!(response, "");
        let (mut fixed, _) = v2(1, 0x11, &[]);
        fixed[14..].copy_from_slice(&60000u16.to_be_bytes());
        let (_, response) = exchange(server, &[&fixed]).await;
        assert_eq!(response, "");

        // The server is none the worse for any of that.
        let (local, response) = exchange(server, &[REQUEST]).await;
        assert_eq!(body(&response), local.to_string());

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task).await.expect("server drains").unwrap().unwrap();
    }

    #[tokio::test]
    async fn the_address_holds_for_every_request_on_a_kept_alive_connection() {
        let (server, shutdown, task) = start(ProxyProtocolMode::Auto).await;
        let mut stream = TcpStream::connect(server).await.unwrap();
        stream.write_all(b"PROXY TCP4 203.0.113.7 10.0.0.1 56324 443\r\n").await.unwrap();
        for _ in 0..3 {
            stream.write_all(b"GET /whoami HTTP/1.1\r\nhost: t\r\n\r\n").await.unwrap();
            let mut seen = Vec::new();
            while !String::from_utf8_lossy(&seen).ends_with("203.0.113.7:56324") {
                let mut chunk = [0u8; 512];
                let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk)).await.unwrap().unwrap();
                assert!(n > 0, "closed early: {}", String::from_utf8_lossy(&seen));
                seen.extend_from_slice(&chunk[..n]);
            }
        }
        // An idle keep-alive connection does not hold shutdown up.
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task).await.expect("server drains").unwrap().unwrap();
    }

    #[tokio::test]
    async fn switched_off_a_proxy_header_is_just_bad_http() {
        let (server, shutdown, task) = start(ProxyProtocolMode::Off).await;
        let (local, response) = exchange(server, &[REQUEST]).await;
        assert_eq!(body(&response), local.to_string());
        let (_, response) = exchange(server, &[b"PROXY TCP4 203.0.113.7 10.0.0.1 56324 443\r\n", REQUEST]).await;
        assert!(!response.contains("203.0.113.7"), "{response}");
        assert!(!response.starts_with("HTTP/1.1 200"), "{response}");
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task).await.expect("server drains").unwrap().unwrap();
    }
}
