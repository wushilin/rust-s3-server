//! HTTP `Range` header parsing.

/// Result of parsing a `Range: bytes=…` header against a known object size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeSelection {
    /// No `Range` header — return the full object.
    Full,
    /// Satisfiable single-range request.
    Single { start: u64, end_inclusive: u64 },
    /// Range cannot be satisfied; respond with 416 and `Content-Range: bytes */{total_size}`.
    Unsatisfiable { total_size: u64 },
}

/// Parses a `Range` header value against the total object size.
///
/// Multi-range requests and malformed values are treated as `Full`.
pub fn parse_range_header(header: Option<&str>, total_size: u64) -> RangeSelection {
    let Some(raw) = header else {
        return RangeSelection::Full;
    };
    let Some(value) = raw.strip_prefix("bytes=") else {
        return RangeSelection::Full;
    };
    if value.contains(',') {
        return RangeSelection::Full;
    }
    let Some((start_raw, end_raw)) = value.split_once('-') else {
        return RangeSelection::Full;
    };

    if start_raw.is_empty() {
        let Ok(suffix_len) = end_raw.parse::<u64>() else {
            return RangeSelection::Full;
        };
        if suffix_len == 0 {
            return RangeSelection::Unsatisfiable { total_size };
        }
        if total_size == 0 {
            return RangeSelection::Unsatisfiable { total_size };
        }
        let start = total_size.saturating_sub(suffix_len);
        return RangeSelection::Single {
            start,
            end_inclusive: total_size - 1,
        };
    }

    let Ok(start) = start_raw.parse::<u64>() else {
        return RangeSelection::Full;
    };
    if total_size == 0 || start >= total_size {
        return RangeSelection::Unsatisfiable { total_size };
    }
    let end_inclusive = if end_raw.is_empty() {
        total_size - 1
    } else {
        let Ok(end) = end_raw.parse::<u64>() else {
            return RangeSelection::Full;
        };
        if end < start {
            return RangeSelection::Unsatisfiable { total_size };
        }
        end.min(total_size - 1)
    };
    RangeSelection::Single {
        start,
        end_inclusive,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_and_multirange_are_full_body() {
        assert_eq!(
            parse_range_header(Some("bytes=abc-def"), 100),
            RangeSelection::Full
        );
        assert_eq!(
            parse_range_header(Some("bytes=0-1,3-4"), 100),
            RangeSelection::Full
        );
    }

    #[test]
    fn valid_ranges_are_selected() {
        assert_eq!(
            parse_range_header(Some("bytes=10-19"), 100),
            RangeSelection::Single {
                start: 10,
                end_inclusive: 19
            }
        );
        assert_eq!(
            parse_range_header(Some("bytes=-10"), 100),
            RangeSelection::Single {
                start: 90,
                end_inclusive: 99
            }
        );
    }

    #[test]
    fn unsatisfiable_range_reports_total_size() {
        assert_eq!(
            parse_range_header(Some("bytes=100-"), 100),
            RangeSelection::Unsatisfiable { total_size: 100 }
        );
    }
}
