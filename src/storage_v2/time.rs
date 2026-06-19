use chrono::{TimeZone, Utc};

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

pub fn iso_utc_ms(epoch_ms: i64) -> String {
    Utc.timestamp_millis_opt(epoch_ms)
        .single()
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

pub fn http_date_ms(epoch_ms: i64) -> String {
    let dt = Utc
        .timestamp_millis_opt(epoch_ms)
        .single()
        .unwrap_or_else(Utc::now);
    httpdate::fmt_http_date(dt.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_s3_times() {
        assert_eq!(iso_utc_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(http_date_ms(0), "Thu, 01 Jan 1970 00:00:00 GMT");
    }
}
