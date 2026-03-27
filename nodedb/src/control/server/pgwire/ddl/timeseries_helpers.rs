//! Internal helpers for timeseries DDL handlers.

/// Parse a `WITH (key = 'value', ...)` clause from a split DDL statement.
///
/// Returns `None` if no WITH clause is present or if the clause is empty.
pub(super) fn parse_with_clause(parts: &[&str]) -> Option<String> {
    let sql = parts.join(" ");
    let upper = sql.to_uppercase();
    let with_pos = upper.find("WITH")?;
    let after_with = &sql[with_pos + 4..].trim();

    let open = after_with.find('(')?;
    let close = after_with.rfind(')')?;
    if close <= open {
        return None;
    }
    let inner = &after_with[open + 1..close];

    let mut config = serde_json::Map::new();
    for pair in inner.split(',') {
        let pair = pair.trim();
        if let Some(eq) = pair.find('=') {
            let key = pair[..eq].trim().to_lowercase();
            let val = pair[eq + 1..].trim().trim_matches('\'').trim_matches('"');
            config.insert(key, serde_json::Value::String(val.to_string()));
        }
    }

    if config.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(config).to_string())
    }
}

/// Format a byte count into a human-readable string.
pub(super) fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_with_clause_basic() {
        let parts: Vec<&str> =
            "CREATE TIMESERIES metrics WITH (partition_by = '3d', retention_period = '30d')"
                .split_whitespace()
                .collect();
        let config = parse_with_clause(&parts).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&config).unwrap();
        assert_eq!(parsed["partition_by"], "3d");
        assert_eq!(parsed["retention_period"], "30d");
    }

    #[test]
    fn parse_with_clause_none() {
        let parts: Vec<&str> = "CREATE TIMESERIES metrics".split_whitespace().collect();
        assert!(parse_with_clause(&parts).is_none());
    }

    #[test]
    fn format_bytes_test() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1500), "1.5 KB");
        assert_eq!(format_bytes(1_500_000), "1.4 MB");
        assert_eq!(format_bytes(2_000_000_000), "1.9 GB");
    }
}
