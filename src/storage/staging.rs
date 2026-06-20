use rand::{rngs::OsRng, RngCore};

use super::errors::{Result, StorageError};

pub fn new_staging_id(epoch_ms: i64) -> String {
    let mut bytes = [0u8; 8];
    OsRng.fill_bytes(&mut bytes);
    let mut random = String::with_capacity(16);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(random, "{byte:02x}");
    }
    format!("{epoch_ms}_{random}")
}

pub fn validate_staging_id(id: &str) -> Result<()> {
    let Some((epoch, random)) = id.split_once('_') else {
        return Err(StorageError::InvalidStagingId(id.to_string()));
    };
    if epoch.is_empty() || !epoch.bytes().all(|b| b.is_ascii_digit()) {
        return Err(StorageError::InvalidStagingId(id.to_string()));
    }
    if random.len() < 16
        || !random
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(StorageError::InvalidStagingId(id.to_string()));
    }
    Ok(())
}

pub fn epoch_ms_from_staging_id(id: &str) -> Result<i64> {
    validate_staging_id(id)?;
    let (epoch, _) = id.split_once('_').unwrap();
    epoch
        .parse()
        .map_err(|_| StorageError::InvalidStagingId(id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_id_has_epoch_and_hex_random() {
        let id = new_staging_id(123);
        validate_staging_id(&id).unwrap();
        assert!(id.starts_with("123_"));
        assert_eq!(epoch_ms_from_staging_id(&id).unwrap(), 123);
    }

    #[test]
    fn staging_id_rejects_path_traversal() {
        for id in [
            "../../x",
            "123_abcdef",
            "123_ABCDEF1234567890",
            "abc_0123456789abcdef",
        ] {
            assert!(validate_staging_id(id).is_err(), "{id}");
        }
    }
}
