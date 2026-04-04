//! SQL parsing for CREATE ALERT DDL.
//!
//! Syntax:
//! ```sql
//! CREATE ALERT <name> ON <collection>
//!     [WHERE <filter>]
//!     CONDITION <agg_func>(<column>) <op> <threshold>
//!     [GROUP BY <col>, ...]
//!     WINDOW '<duration>'
//!     [FOR '<N> consecutive windows']
//!     [RECOVER AFTER '<N> consecutive windows']
//!     SEVERITY '<level>'
//!     NOTIFY
//!         TOPIC '<name>',
//!         WEBHOOK '<url>',
//!         INSERT INTO <table> (<columns...>);
//! ```

use pgwire::error::PgWireResult;

use crate::event::alert::types::{AlertCondition, CompareOp, NotifyTarget};

use super::super::super::types::sqlstate_error;

pub(super) struct ParsedAlert {
    pub name: String,
    pub collection: String,
    pub where_filter: Option<String>,
    pub condition: AlertCondition,
    pub group_by: Vec<String>,
    pub window_ms: u64,
    pub fire_after: u32,
    pub recover_after: u32,
    pub severity: String,
    pub notify_targets: Vec<NotifyTarget>,
}

pub(super) fn parse_create_alert(sql: &str) -> PgWireResult<ParsedAlert> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    // Extract name: "CREATE ALERT <name> ON ..."
    let prefix = "CREATE ALERT ";
    if !upper.starts_with(prefix) {
        return Err(sqlstate_error("42601", "expected CREATE ALERT"));
    }
    let after_prefix = &trimmed[prefix.len()..];
    let name = after_prefix
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing alert name"))?
        .to_lowercase();

    // Extract collection: "... ON <collection> ..."
    let upper_rest = upper[prefix.len()..].to_string();
    let on_pos = upper_rest
        .find(" ON ")
        .ok_or_else(|| sqlstate_error("42601", "expected ON <collection>"))?;
    let after_on = &after_prefix[on_pos + 4..].trim_start();
    let collection = after_on
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing collection name"))?
        .to_lowercase();

    // Extract WHERE filter (optional): between "WHERE" and "CONDITION".
    let where_filter = extract_between(&upper, trimmed, "WHERE ", "CONDITION ");

    // Extract CONDITION: agg_func(column) op threshold
    let condition = extract_condition(&upper, trimmed)?;

    // Extract GROUP BY columns (optional).
    let group_by = extract_group_by_cols(&upper, trimmed);

    // Extract WINDOW duration.
    let window_ms = extract_window(&upper, trimmed)?;

    // Extract FOR 'N consecutive windows' (optional, default 1).
    let fire_after = extract_consecutive(&upper, "FOR ").unwrap_or(1).max(1);

    // Extract RECOVER AFTER 'N consecutive windows' (optional, default 1).
    let recover_after = extract_consecutive(&upper, "RECOVER AFTER ")
        .unwrap_or(1)
        .max(1);

    // Extract SEVERITY.
    let severity =
        extract_quoted_after(&upper, trimmed, "SEVERITY ").unwrap_or_else(|| "warning".to_string());

    // Extract NOTIFY targets.
    let notify_targets = extract_notify_targets(&upper, trimmed)?;

    Ok(ParsedAlert {
        name,
        collection,
        where_filter,
        condition,
        group_by,
        window_ms,
        fire_after,
        recover_after,
        severity,
        notify_targets,
    })
}

/// Extract text between two keywords (case-insensitive search, original-case result).
fn extract_between(upper: &str, sql: &str, start_kw: &str, end_kw: &str) -> Option<String> {
    let start = upper.find(start_kw)?;
    let after_start = start + start_kw.len();
    let end = upper[after_start..].find(end_kw)?;
    let text = sql[after_start..after_start + end].trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

/// Alert clause boundary keywords. Used to find where one clause ends
/// and the next begins when parsing positional SQL.
const CLAUSE_KEYWORDS: &[&str] = &[
    "CONDITION",
    "GROUP BY",
    "WINDOW",
    "FOR ",
    "RECOVER",
    "SEVERITY",
    "NOTIFY",
];

/// Find the byte offset of the nearest clause keyword after `start_pos`,
/// excluding any keywords in `exclude`.
fn find_clause_boundary(upper: &str, start_pos: usize, exclude: &[&str]) -> usize {
    CLAUSE_KEYWORDS
        .iter()
        .filter(|kw| !exclude.contains(kw))
        .filter_map(|kw| upper[start_pos..].find(kw))
        .min()
        .unwrap_or(upper.len() - start_pos)
}

/// Extract CONDITION: agg_func(column) op threshold.
fn extract_condition(upper: &str, sql: &str) -> PgWireResult<AlertCondition> {
    let pos = upper
        .find("CONDITION ")
        .ok_or_else(|| sqlstate_error("42601", "missing CONDITION clause"))?;
    let after = &sql[pos + 10..].trim_start();

    let end_pos = find_clause_boundary(upper, pos + 10, &["CONDITION"]);

    let cond_str = after[..end_pos].trim();

    // Parse: agg_func(column) op threshold
    let open = cond_str
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "expected agg_func(column) in CONDITION"))?;
    let close = cond_str
        .find(')')
        .ok_or_else(|| sqlstate_error("42601", "missing ')' in CONDITION"))?;

    let agg_func = cond_str[..open].trim().to_lowercase();
    let column = cond_str[open + 1..close].trim().to_lowercase();

    // After ')': operator and threshold.
    let remainder = cond_str[close + 1..].trim();

    // Find the operator (1 or 2 chars).
    let (op_str, rest) = if remainder.starts_with(">=")
        || remainder.starts_with("<=")
        || remainder.starts_with("!=")
        || remainder.starts_with("<>")
    {
        (&remainder[..2], remainder[2..].trim())
    } else if remainder.starts_with('>') || remainder.starts_with('<') || remainder.starts_with('=')
    {
        (&remainder[..1], remainder[1..].trim())
    } else {
        return Err(sqlstate_error(
            "42601",
            &format!("expected comparison operator after ')' in CONDITION: {remainder}"),
        ));
    };

    let op = CompareOp::parse(op_str)
        .ok_or_else(|| sqlstate_error("42601", &format!("unknown operator: {op_str}")))?;

    let threshold: f64 = rest.parse().map_err(|_| {
        sqlstate_error("42601", &format!("expected numeric threshold, got: {rest}"))
    })?;

    Ok(AlertCondition {
        agg_func,
        column,
        op,
        threshold,
    })
}

/// Extract GROUP BY columns.
fn extract_group_by_cols(upper: &str, sql: &str) -> Vec<String> {
    let gb_kw = "GROUP BY ";
    let pos = match upper.find(gb_kw) {
        Some(p) => p,
        None => return Vec::new(),
    };
    let after = &sql[pos + gb_kw.len()..];

    let end_pos = find_clause_boundary(upper, pos + gb_kw.len(), &["GROUP BY"]);

    after[..end_pos]
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Extract WINDOW '<duration>'.
fn extract_window(upper: &str, sql: &str) -> PgWireResult<u64> {
    let val = extract_quoted_after(upper, sql, "WINDOW ")
        .ok_or_else(|| sqlstate_error("42601", "missing WINDOW '<duration>'"))?;
    nodedb_types::kv_parsing::parse_interval_to_ms(&val)
        .map_err(|e| sqlstate_error("42601", &format!("invalid window duration: {e}")))
}

/// Extract "N consecutive windows" after a keyword like "FOR " or "RECOVER AFTER ".
fn extract_consecutive(upper: &str, keyword: &str) -> Option<u32> {
    let pos = upper.find(keyword)?;
    let after = &upper[pos + keyword.len()..];
    // Extract quoted: 'N consecutive windows'
    let start = after.find('\'')?;
    let end = after[start + 1..].find('\'')?;
    let inner = &after[start + 1..start + 1 + end];
    // Parse the number at the beginning.
    inner
        .split_whitespace()
        .next()
        .and_then(|n| n.parse::<u32>().ok())
}

/// Extract a single-quoted value after a keyword.
fn extract_quoted_after(upper: &str, sql: &str, keyword: &str) -> Option<String> {
    let pos = upper.find(keyword)?;
    let after = &sql[pos + keyword.len()..];
    let start = after.find('\'')?;
    let end = after[start + 1..].find('\'')?;
    Some(after[start + 1..start + 1 + end].to_string())
}

/// Extract NOTIFY targets.
fn extract_notify_targets(upper: &str, sql: &str) -> PgWireResult<Vec<NotifyTarget>> {
    let pos = match upper.find("NOTIFY") {
        Some(p) => p,
        None => return Ok(Vec::new()),
    };
    let after = &sql[pos + 6..].trim_start();

    let mut targets = Vec::new();
    // Split on top-level commas (respecting parentheses in INSERT INTO columns).
    for part in split_top_level_commas(after) {
        let part = part.trim().trim_end_matches(';').trim();
        if part.is_empty() {
            continue;
        }
        let upper_part = part.to_uppercase();

        if upper_part.starts_with("TOPIC ") {
            let name = extract_inner_quoted(part, 6)?;
            targets.push(NotifyTarget::Topic { name });
        } else if upper_part.starts_with("WEBHOOK ") {
            let url = extract_inner_quoted(part, 8)?;
            targets.push(NotifyTarget::Webhook { url });
        } else if upper_part.starts_with("INSERT INTO ") {
            let (table, columns) = parse_insert_target(&part[12..])?;
            targets.push(NotifyTarget::InsertInto { table, columns });
        }
    }

    Ok(targets)
}

/// Split on commas that are NOT inside parentheses.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                results.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        results.push(&s[start..]);
    }
    results
}

/// Extract a quoted string starting at a byte offset.
fn extract_inner_quoted(s: &str, offset: usize) -> PgWireResult<String> {
    let after = s[offset..].trim_start();
    let start = after
        .find('\'')
        .ok_or_else(|| sqlstate_error("42601", "expected quoted value"))?;
    let end = after[start + 1..]
        .find('\'')
        .ok_or_else(|| sqlstate_error("42601", "missing closing quote"))?;
    Ok(after[start + 1..start + 1 + end].to_string())
}

/// Parse "table (col1, col2, ...)" from INSERT INTO target.
fn parse_insert_target(s: &str) -> PgWireResult<(String, Vec<String>)> {
    let s = s.trim();
    if let Some(paren_start) = s.find('(') {
        let table = s[..paren_start].trim().to_lowercase();
        let paren_end = s
            .rfind(')')
            .ok_or_else(|| sqlstate_error("42601", "missing ')' in INSERT INTO target"))?;
        let cols: Vec<String> = s[paren_start + 1..paren_end]
            .split(',')
            .map(|c| c.trim().to_lowercase())
            .filter(|c| !c.is_empty())
            .collect();
        Ok((table, cols))
    } else {
        let table = s.split_whitespace().next().unwrap_or(s).to_lowercase();
        Ok((table, Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_alert() {
        let sql = "CREATE ALERT high_temperature ON sensor_data \
                    WHERE device_type = 'compressor' \
                    CONDITION AVG(temperature) > 90.0 \
                    GROUP BY device_id \
                    WINDOW '5 minutes' \
                    FOR '3 consecutive windows' \
                    RECOVER AFTER '2 consecutive windows' \
                    SEVERITY 'critical' \
                    NOTIFY \
                        TOPIC 'alerts', \
                        WEBHOOK 'https://ops.example.com/alerts', \
                        INSERT INTO alert_history (rule, device_id, severity, fired_at, value)";
        let parsed = parse_create_alert(sql).unwrap();

        assert_eq!(parsed.name, "high_temperature");
        assert_eq!(parsed.collection, "sensor_data");
        assert_eq!(
            parsed.where_filter.as_deref(),
            Some("device_type = 'compressor'")
        );
        assert_eq!(parsed.condition.agg_func, "avg");
        assert_eq!(parsed.condition.column, "temperature");
        assert_eq!(parsed.condition.op, CompareOp::Gt);
        assert!((parsed.condition.threshold - 90.0).abs() < 1e-12);
        assert_eq!(parsed.group_by, vec!["device_id"]);
        assert_eq!(parsed.window_ms, 300_000);
        assert_eq!(parsed.fire_after, 3);
        assert_eq!(parsed.recover_after, 2);
        assert_eq!(parsed.severity, "critical");
        assert_eq!(parsed.notify_targets.len(), 3);

        assert!(
            matches!(&parsed.notify_targets[0], NotifyTarget::Topic { name } if name == "alerts")
        );
        assert!(
            matches!(&parsed.notify_targets[1], NotifyTarget::Webhook { url } if url == "https://ops.example.com/alerts")
        );
        assert!(
            matches!(&parsed.notify_targets[2], NotifyTarget::InsertInto { table, columns } if table == "alert_history" && columns.len() == 5)
        );
    }

    #[test]
    fn parse_minimal_alert() {
        let sql = "CREATE ALERT simple ON metrics \
                    CONDITION MAX(cpu) > 95.0 \
                    WINDOW '1 minute' \
                    SEVERITY 'warning'";
        let parsed = parse_create_alert(sql).unwrap();

        assert_eq!(parsed.name, "simple");
        assert_eq!(parsed.collection, "metrics");
        assert!(parsed.where_filter.is_none());
        assert_eq!(parsed.condition.agg_func, "max");
        assert_eq!(parsed.condition.column, "cpu");
        assert!(parsed.group_by.is_empty());
        assert_eq!(parsed.window_ms, 60_000);
        assert_eq!(parsed.fire_after, 1); // default
        assert_eq!(parsed.recover_after, 1); // default
        assert!(parsed.notify_targets.is_empty());
    }

    #[test]
    fn parse_condition_operators() {
        for (op_str, expected_op) in [
            (">", CompareOp::Gt),
            (">=", CompareOp::Gte),
            ("<", CompareOp::Lt),
            ("<=", CompareOp::Lte),
        ] {
            let sql = format!(
                "CREATE ALERT test ON m CONDITION AVG(v) {op_str} 50.0 WINDOW '1m' SEVERITY 'info'"
            );
            let parsed = parse_create_alert(&sql).unwrap();
            assert_eq!(parsed.condition.op, expected_op);
        }
    }

    #[test]
    fn parse_missing_condition_errors() {
        let sql = "CREATE ALERT test ON m WINDOW '1m' SEVERITY 'info'";
        assert!(parse_create_alert(sql).is_err());
    }

    #[test]
    fn parse_missing_window_errors() {
        let sql = "CREATE ALERT test ON m CONDITION AVG(v) > 50 SEVERITY 'info'";
        assert!(parse_create_alert(sql).is_err());
    }
}
