//! `aws-chunked` body framing (`Content-Encoding: aws-chunked`).
//!
//! The body is a sequence of `<hex-size>[;chunk-signature=<sig>]\r\n<data>\r\n`
//! chunks ended by a zero-size chunk, optional trailer lines and a blank line.
//! [`AwsChunkedDecoder`] parses it incrementally: chunk data is handed back as
//! ranges of the caller's input (never buffered, however large the declared
//! chunk), and only header/trailer *lines* are buffered — each capped at
//! [`MAX_LINE_BYTES`] so a header that never ends cannot grow without bound.
//!
//! With a [`ChunkSigner`] the decoder also verifies the SigV4 streaming
//! signature chain (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`): each
//! chunk's signature covers the previous signature and the SHA-256 of its data,
//! starting from the seed signature of the request's `Authorization` header and
//! ending with the final empty chunk.

use std::ops::Range;

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

use super::errors::{Result, StorageError};

type HmacSha256 = Hmac<Sha256>;

/// Longest accepted chunk-header or trailer line (excluding its CRLF).
pub const MAX_LINE_BYTES: usize = 4 * 1024;
/// Most trailer lines accepted after the final chunk.
pub const MAX_TRAILER_LINES: usize = 64;
/// Marker carried by a chunk-signature failure, so a storage writer that only
/// sees the stream error's text can still classify it.
pub const CHUNK_SIGNATURE_MISMATCH: &str = "aws-chunked chunk signature does not match";

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// The SigV4 context needed to verify a signed `aws-chunked` stream.
#[derive(Debug, Clone)]
pub struct ChunkSigner {
    /// The derived SigV4 signing key of the request's credential scope.
    pub signing_key: Vec<u8>,
    /// The request's `x-amz-date` (`YYYYMMDDTHHMMSSZ`).
    pub amz_date: String,
    /// `date/region/s3/aws4_request`.
    pub scope: String,
    /// The seed signature (from the `Authorization` header), then each
    /// verified chunk's signature in turn.
    pub prev_signature: String,
}

impl ChunkSigner {
    fn verify(&mut self, data_sha256: &str, provided: Option<&str>) -> Result<()> {
        let provided = provided.ok_or_else(|| {
            StorageError::InvalidAwsChunkedBody("missing chunk-signature".to_string())
        })?;
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{EMPTY_SHA256}\n{data_sha256}",
            self.amz_date, self.scope, self.prev_signature
        );
        let mut mac =
            HmacSha256::new_from_slice(&self.signing_key).expect("HMAC accepts any key length");
        mac.update(string_to_sign.as_bytes());
        let expected = hex(&mac.finalize().into_bytes());
        if !constant_time_eq(expected.as_bytes(), provided.as_bytes()) {
            return Err(StorageError::InvalidAwsChunkedBody(
                CHUNK_SIGNATURE_MISMATCH.to_string(),
            ));
        }
        self.prev_signature = expected;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Header,
    Data { remaining: u64 },
    DataCr,
    DataLf,
    Trailer,
    Done,
}

/// Incremental `aws-chunked` decoder (optionally signature-verifying).
#[derive(Debug)]
pub struct AwsChunkedDecoder {
    state: State,
    line: Vec<u8>,
    signer: Option<ChunkSigner>,
    hasher: Sha256,
    chunk_signature: Option<String>,
    trailer_lines: usize,
}

impl Default for AwsChunkedDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl AwsChunkedDecoder {
    /// A decoder that ignores chunk signatures (unsigned / already-verified
    /// streams).
    pub fn new() -> Self {
        Self {
            state: State::Header,
            line: Vec::new(),
            signer: None,
            hasher: Sha256::new(),
            chunk_signature: None,
            trailer_lines: 0,
        }
    }

    /// A decoder that verifies every chunk's signature against `signer`.
    pub fn with_signer(signer: ChunkSigner) -> Self {
        Self {
            signer: Some(signer),
            ..Self::new()
        }
    }

    /// True once the final chunk and its trailers have been consumed.
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    /// Errors unless the whole body (final chunk included) has been seen.
    pub fn finish(&self) -> Result<()> {
        if self.is_done() {
            Ok(())
        } else {
            Err(StorageError::InvalidAwsChunkedBody(
                "missing final chunk".to_string(),
            ))
        }
    }

    /// Consumes `input`, returning the ranges of it that are chunk data (in
    /// order). Errors on malformed framing or a bad chunk signature.
    pub fn feed(&mut self, input: &[u8]) -> Result<Vec<Range<usize>>> {
        let mut out = Vec::new();
        let mut i = 0usize;
        while i < input.len() {
            match self.state {
                State::Header | State::Trailer => {
                    let rest = &input[i..];
                    let (take, complete) = match rest.iter().position(|&b| b == b'\n') {
                        Some(pos) => (pos, true),
                        None => (rest.len(), false),
                    };
                    if self.line.len() + take > MAX_LINE_BYTES + 1 {
                        return Err(StorageError::InvalidAwsChunkedBody(
                            "chunk header or trailer line too long".to_string(),
                        ));
                    }
                    self.line.extend_from_slice(&rest[..take]);
                    i += take;
                    if complete {
                        i += 1; // the '\n'
                        let mut line = std::mem::take(&mut self.line);
                        if line.pop() != Some(b'\r') {
                            return Err(StorageError::InvalidAwsChunkedBody(
                                "chunk line is not CRLF-terminated".to_string(),
                            ));
                        }
                        if self.state == State::Header {
                            self.header_line(&line)?;
                        } else {
                            self.trailer_line(&line)?;
                        }
                    }
                }
                State::Data { remaining } => {
                    let available = (input.len() - i) as u64;
                    let n = remaining.min(available) as usize;
                    if self.signer.is_some() {
                        self.hasher.update(&input[i..i + n]);
                    }
                    out.push(i..i + n);
                    i += n;
                    let remaining = remaining - n as u64;
                    if remaining == 0 {
                        if let Some(signer) = self.signer.as_mut() {
                            let digest = std::mem::replace(&mut self.hasher, Sha256::new());
                            signer.verify(&hex(&digest.finalize()), self.chunk_signature.as_deref())?;
                        }
                        self.state = State::DataCr;
                    } else {
                        self.state = State::Data { remaining };
                    }
                }
                State::DataCr | State::DataLf => {
                    let expected = if self.state == State::DataCr { b'\r' } else { b'\n' };
                    if input[i] != expected {
                        return Err(StorageError::InvalidAwsChunkedBody(
                            "chunk data missing trailing CRLF".to_string(),
                        ));
                    }
                    i += 1;
                    self.state = if self.state == State::DataCr {
                        State::DataLf
                    } else {
                        State::Header
                    };
                }
                State::Done => {
                    return Err(StorageError::InvalidAwsChunkedBody(
                        "data after the final chunk".to_string(),
                    ));
                }
            }
        }
        Ok(out)
    }

    fn header_line(&mut self, line: &[u8]) -> Result<()> {
        let header = std::str::from_utf8(line).map_err(|_| {
            StorageError::InvalidAwsChunkedBody("chunk header is not utf-8".to_string())
        })?;
        let mut parts = header.split(';');
        let size_token = parts.next().unwrap_or("");
        if size_token.is_empty() || !size_token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(StorageError::InvalidAwsChunkedBody(format!(
                "invalid chunk size {size_token}"
            )));
        }
        let size = u64::from_str_radix(size_token, 16)
            .map_err(|_| StorageError::InvalidAwsChunkedBody("chunk size overflow".to_string()))?;
        self.chunk_signature = parts
            .find_map(|ext| ext.trim().strip_prefix("chunk-signature="))
            .map(str::to_string);
        if size == 0 {
            if let Some(signer) = self.signer.as_mut() {
                signer.verify(EMPTY_SHA256, self.chunk_signature.as_deref())?;
            }
            self.state = State::Trailer;
        } else {
            self.hasher = Sha256::new();
            self.state = State::Data { remaining: size };
        }
        Ok(())
    }

    fn trailer_line(&mut self, line: &[u8]) -> Result<()> {
        if line.is_empty() {
            self.state = State::Done;
            return Ok(());
        }
        self.trailer_lines += 1;
        if self.trailer_lines > MAX_TRAILER_LINES {
            return Err(StorageError::InvalidAwsChunkedBody(
                "too many trailer lines".to_string(),
            ));
        }
        Ok(())
    }
}

/// Decodes a complete in-memory `aws-chunked` body (chunk signatures are not
/// checked).
pub fn decode_aws_chunked(input: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = AwsChunkedDecoder::new();
    let mut output = Vec::new();
    for range in decoder.feed(input)? {
        output.extend_from_slice(&input[range]);
    }
    decoder.finish()?;
    Ok(output)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_chunks_and_consumes_trailers() {
        let body = b"5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=end\r\nx-amz-checksum-crc32: abcd\r\n\r\n";
        assert_eq!(decode_aws_chunked(body).unwrap(), b"hello world");
    }

    #[test]
    fn rejects_truncated_body() {
        assert!(decode_aws_chunked(b"5;chunk-signature=abc\r\nhel").is_err());
    }

    #[test]
    fn decodes_byte_by_byte_without_buffering_chunk_data() {
        let body = b"5\r\nhello\r\n0\r\n\r\n";
        let mut decoder = AwsChunkedDecoder::new();
        let mut out = Vec::new();
        for b in body.iter() {
            for range in decoder.feed(std::slice::from_ref(b)).unwrap() {
                out.extend_from_slice(&std::slice::from_ref(b)[range]);
            }
        }
        decoder.finish().unwrap();
        assert_eq!(out, b"hello");
    }

    #[test]
    fn a_header_line_without_crlf_is_capped() {
        let mut decoder = AwsChunkedDecoder::new();
        let junk = vec![b'a'; MAX_LINE_BYTES];
        decoder.feed(&junk).unwrap();
        assert!(decoder.feed(&junk).is_err());
    }

    #[test]
    fn a_huge_declared_chunk_is_streamed_not_buffered() {
        // 1 GiB declared; only 3 bytes arrive and they come straight back.
        let mut decoder = AwsChunkedDecoder::new();
        let ranges = decoder.feed(b"40000000\r\nabc").unwrap();
        assert_eq!(ranges, vec![10..13]);
        assert!(decoder.finish().is_err());
    }

    #[test]
    fn malformed_framing_is_rejected() {
        assert!(decode_aws_chunked(b"zz\r\n").is_err());
        assert!(decode_aws_chunked(b"3\r\nabcXY0\r\n\r\n").is_err());
        assert!(decode_aws_chunked(b"3\nabc\r\n0\r\n\r\n").is_err());
        assert!(decode_aws_chunked(b"0\r\n\r\nextra").is_err());
    }

    fn sign_chunk(key: &[u8], date: &str, scope: &str, prev: &str, data: &[u8]) -> String {
        let data_hash = hex(&Sha256::digest(data));
        let sts = format!("AWS4-HMAC-SHA256-PAYLOAD\n{date}\n{scope}\n{prev}\n{EMPTY_SHA256}\n{data_hash}");
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(sts.as_bytes());
        hex(&mac.finalize().into_bytes())
    }

    pub(crate) fn signed_body(key: &[u8], date: &str, scope: &str, seed: &str, chunks: &[&[u8]]) -> Vec<u8> {
        let mut prev = seed.to_string();
        let mut body = Vec::new();
        for chunk in chunks.iter().chain(std::iter::once(&&b""[..])) {
            let sig = sign_chunk(key, date, scope, &prev, chunk);
            body.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", chunk.len()).as_bytes());
            body.extend_from_slice(chunk);
            body.extend_from_slice(b"\r\n");
            prev = sig;
        }
        body
    }

    fn signer(seed: &str) -> ChunkSigner {
        ChunkSigner {
            signing_key: b"key".to_vec(),
            amz_date: "20260101T000000Z".to_string(),
            scope: "20260101/us-east-1/s3/aws4_request".to_string(),
            prev_signature: seed.to_string(),
        }
    }

    #[test]
    fn a_valid_signature_chain_verifies() {
        let body = signed_body(b"key", "20260101T000000Z", "20260101/us-east-1/s3/aws4_request", "seed", &[b"hello", b" world"]);
        let mut decoder = AwsChunkedDecoder::with_signer(signer("seed"));
        let data: Vec<u8> = decoder.feed(&body).unwrap().into_iter().flat_map(|r| body[r].to_vec()).collect();
        decoder.finish().unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn a_tampered_chunk_or_broken_chain_is_rejected() {
        let body = signed_body(b"key", "20260101T000000Z", "20260101/us-east-1/s3/aws4_request", "seed", &[b"hello"]);
        // Tampered data.
        let mut tampered = body.clone();
        let pos = tampered.windows(5).position(|w| w == b"hello").unwrap();
        tampered[pos] = b'j';
        assert!(AwsChunkedDecoder::with_signer(signer("seed")).feed(&tampered).is_err());
        // Wrong seed (a chain spliced onto another request).
        assert!(AwsChunkedDecoder::with_signer(signer("other")).feed(&body).is_err());
        // Missing signatures.
        assert!(AwsChunkedDecoder::with_signer(signer("seed")).feed(b"5\r\nhello\r\n0\r\n\r\n").is_err());
        // Forged final chunk: valid data chunk, bogus terminator signature.
        let cut = body.windows(3).position(|w| w == b"\n0;").unwrap() + 1;
        let mut forged = body[..cut].to_vec();
        forged.extend_from_slice(format!("0;chunk-signature={}\r\n\r\n", "0".repeat(64)).as_bytes());
        assert!(AwsChunkedDecoder::with_signer(signer("seed")).feed(&forged).is_err());
    }
}
