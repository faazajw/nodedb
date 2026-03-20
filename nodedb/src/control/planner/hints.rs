//! SQL query hint parsing for CBO strategy overrides.
//!
//! Supports PostgreSQL-style optimizer hints in comments:
//! ```sql
//! SELECT /*+ BROADCAST(small_table) */ * FROM large JOIN small ON ...
//! SELECT /*+ SHUFFLE(orders) */ * FROM orders JOIN products ON ...
//! ```
//!
//! Hints override the automatic join strategy selection in the converter.

use std::collections::HashSet;

/// Parsed query hints extracted from SQL comments.
#[derive(Debug, Default)]
pub struct QueryHints {
    /// Tables that should be broadcast (small side sent to all cores).
    pub broadcast_tables: HashSet<String>,
    /// Tables that should be shuffled (repartitioned by join key).
    pub shuffle_tables: HashSet<String>,
}

impl QueryHints {
    /// Parse hints from a SQL string.
    ///
    /// Extracts `/*+ BROADCAST(table) */` and `/*+ SHUFFLE(table) */` patterns.
    /// Multiple hints can appear in a single comment: `/*+ BROADCAST(a) SHUFFLE(b) */`.
    pub fn parse(sql: &str) -> Self {
        let mut hints = Self::default();

        // Find all hint comments: /*+ ... */
        let mut pos = 0;
        while let Some(start) = sql[pos..].find("/*+") {
            let abs_start = pos + start + 3; // skip "/*+"
            if let Some(end) = sql[abs_start..].find("*/") {
                let hint_body = &sql[abs_start..abs_start + end];
                Self::parse_body(hint_body, &mut hints);
                pos = abs_start + end + 2;
            } else {
                break;
            }
        }

        hints
    }

    fn parse_body(body: &str, hints: &mut QueryHints) {
        // Tokenize by whitespace.
        let upper = body.to_uppercase();
        let mut chars = upper.chars().peekable();
        let mut token = String::new();

        while let Some(ch) = chars.next() {
            if ch == '(' {
                let directive = token.trim().to_string();
                token.clear();
                // Read table name until ')'.
                let mut table = String::new();
                for c in chars.by_ref() {
                    if c == ')' {
                        break;
                    }
                    table.push(c);
                }
                let table = table.trim().to_lowercase();
                if !table.is_empty() {
                    match directive.as_str() {
                        "BROADCAST" => {
                            hints.broadcast_tables.insert(table);
                        }
                        "SHUFFLE" => {
                            hints.shuffle_tables.insert(table);
                        }
                        _ => {} // Unknown hint — ignore.
                    }
                }
            } else {
                token.push(ch);
            }
        }
    }

    /// Whether any hints are present.
    pub fn is_empty(&self) -> bool {
        self.broadcast_tables.is_empty() && self.shuffle_tables.is_empty()
    }

    /// Check if a table should be broadcast.
    pub fn should_broadcast(&self, table: &str) -> bool {
        self.broadcast_tables.contains(&table.to_lowercase())
    }

    /// Check if a table should be shuffled.
    pub fn should_shuffle(&self, table: &str) -> bool {
        self.shuffle_tables.contains(&table.to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_broadcast_hint() {
        let sql = "SELECT /*+ BROADCAST(small_table) */ * FROM large JOIN small_table ON id = id";
        let hints = QueryHints::parse(sql);
        assert!(hints.should_broadcast("small_table"));
        assert!(!hints.should_shuffle("small_table"));
    }

    #[test]
    fn parse_shuffle_hint() {
        let sql = "SELECT /*+ SHUFFLE(orders) */ * FROM orders JOIN products ON product_id = id";
        let hints = QueryHints::parse(sql);
        assert!(hints.should_shuffle("orders"));
    }

    #[test]
    fn parse_multiple_hints() {
        let sql = "SELECT /*+ BROADCAST(a) SHUFFLE(b) */ * FROM a JOIN b ON x = y";
        let hints = QueryHints::parse(sql);
        assert!(hints.should_broadcast("a"));
        assert!(hints.should_shuffle("b"));
    }

    #[test]
    fn no_hints() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let hints = QueryHints::parse(sql);
        assert!(hints.is_empty());
    }

    #[test]
    fn case_insensitive() {
        let sql = "SELECT /*+ broadcast(MyTable) */ * FROM mytable";
        let hints = QueryHints::parse(sql);
        assert!(hints.should_broadcast("mytable"));
    }
}
