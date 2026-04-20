//! Structured detail body for `UNDROP COLLECTION` audit records.
//!
//! Serialized as JSON into the `AuditEntry::detail` field. SIEM /
//! compliance consumers filter on `owner_user_missing` without
//! string-scraping a human-readable message.

/// Stage of the UNDROP workflow this record describes.
///
/// A well-formed UNDROP always produces two records: one at `Requested`
/// (before the raft propose) and one at `Completed` (after the log
/// index is known). If the process crashes between them, the
/// `Requested` record alone is enough to reconstruct that an UNDROP
/// was attempted with the owner-missing flag visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UndropStage {
    Requested,
    Completed,
}

/// Structured detail for an `UNDROP COLLECTION` audit record under
/// `AuditEvent::AdminAction`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UndropAuditDetail {
    /// Always "undrop_collection"; kept as owned `String` for
    /// round-trip deserialization (no borrowed fields).
    pub action: String,
    pub collection: String,
    pub stage: UndropStage,
    /// Raft log index assigned at propose time. `None` on the
    /// `Requested` record (not yet proposed) or on the single-node
    /// direct-write fallback (log_index = 0).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_index: Option<u64>,
    /// The preserved owner user was absent from `credentials` at UNDROP
    /// time. Admin-only path took effect and SIEM consumers must flag
    /// this for post-hoc review.
    pub owner_user_missing: bool,
}

impl UndropAuditDetail {
    pub fn new(
        collection: impl Into<String>,
        stage: UndropStage,
        owner_user_missing: bool,
    ) -> Self {
        Self {
            action: "undrop_collection".to_string(),
            collection: collection.into(),
            stage,
            log_index: None,
            owner_user_missing,
        }
    }

    pub fn with_log_index(mut self, log_index: u64) -> Self {
        self.log_index = Some(log_index);
        self
    }

    /// Serialize to the JSON string that lands in `AuditEntry::detail`.
    /// sonic_rs per the project-wide runtime-JSON rule.
    pub fn to_json(&self) -> String {
        sonic_rs::to_string(self).unwrap_or_else(|_| {
            // Infallible for a flat struct of primitives; the fallback
            // exists so this helper never panics at an audit site.
            format!(
                "{{\"action\":\"undrop_collection\",\"collection\":\"{}\",\"owner_user_missing\":{}}}",
                self.collection.replace('"', "\\\""),
                self.owner_user_missing
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requested_stage_json_shape() {
        let d = UndropAuditDetail::new("orders", UndropStage::Requested, true);
        let json = d.to_json();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["action"].as_str(), Some("undrop_collection"));
        assert_eq!(v["collection"].as_str(), Some("orders"));
        assert_eq!(v["stage"].as_str(), Some("requested"));
        assert_eq!(v["owner_user_missing"].as_bool(), Some(true));
        assert!(v.get("log_index").is_none());
    }

    #[test]
    fn completed_stage_includes_log_index() {
        let d = UndropAuditDetail::new("orders", UndropStage::Completed, false).with_log_index(42);
        let json = d.to_json();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["stage"].as_str(), Some("completed"));
        assert_eq!(v["log_index"].as_u64(), Some(42));
        assert_eq!(v["owner_user_missing"].as_bool(), Some(false));
    }

    #[test]
    fn roundtrip_preserves_all_fields() {
        let d = UndropAuditDetail::new("orders", UndropStage::Completed, true).with_log_index(7);
        let json = d.to_json();
        let back: UndropAuditDetail = serde_json::from_str(&json).unwrap();
        assert_eq!(back.collection, "orders");
        assert_eq!(back.stage, UndropStage::Completed);
        assert_eq!(back.log_index, Some(7));
        assert!(back.owner_user_missing);
    }
}
