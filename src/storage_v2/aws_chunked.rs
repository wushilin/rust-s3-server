use super::errors::{Result, StorageError};

pub fn decode_aws_chunked(input: &[u8]) -> Result<Vec<u8>> {
    let mut pos = 0usize;
    let mut output = Vec::new();
    loop {
        let header_end = find_crlf(input, pos).ok_or_else(|| {
            StorageError::InvalidAwsChunkedBody("missing chunk header CRLF".to_string())
        })?;
        let header = std::str::from_utf8(&input[pos..header_end]).map_err(|_| {
            StorageError::InvalidAwsChunkedBody("chunk header is not utf-8".to_string())
        })?;
        let size_token = header.split(';').next().unwrap_or("");
        let size = usize::from_str_radix(size_token, 16)
            .map_err(|_| StorageError::InvalidAwsChunkedBody("invalid chunk size".to_string()))?;
        pos = header_end + 2;
        if size == 0 {
            consume_trailers(input, pos)?;
            return Ok(output);
        }
        let data_end = pos.checked_add(size).ok_or_else(|| {
            StorageError::InvalidAwsChunkedBody("chunk size overflow".to_string())
        })?;
        if data_end + 2 > input.len() {
            return Err(StorageError::InvalidAwsChunkedBody(
                "chunk data truncated".to_string(),
            ));
        }
        output.extend_from_slice(&input[pos..data_end]);
        if &input[data_end..data_end + 2] != b"\r\n" {
            return Err(StorageError::InvalidAwsChunkedBody(
                "missing chunk data CRLF".to_string(),
            ));
        }
        pos = data_end + 2;
    }
}

fn consume_trailers(input: &[u8], mut pos: usize) -> Result<()> {
    loop {
        let end = find_crlf(input, pos).ok_or_else(|| {
            StorageError::InvalidAwsChunkedBody("missing trailer terminator".to_string())
        })?;
        if end == pos {
            return Ok(());
        }
        pos = end + 2;
    }
}

fn find_crlf(input: &[u8], start: usize) -> Option<usize> {
    input
        .get(start..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|idx| start + idx)
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
}
