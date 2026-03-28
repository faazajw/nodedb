//! Column-level redaction: mask or pseudonymize fields based on role.
//!
//! Evaluated after RLS (row filtering), before result delivery.
//! Supports static masks (`'***@***.com'`) and hash pseudonymization
//! (`hash(email)` — joinable but not readable).

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use tracing::info;

/// A redaction policy: specifies which fields to redact for which roles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionPolicy {
    /// Policy name.
    pub name: String,
    /// Collection this policy applies to.
    pub collection: String,
    /// Role this policy applies to (e.g., "support").
    pub for_role: String,
    /// Field → redaction rule.
    pub rules: Vec<RedactionRule>,
}

/// A single field redaction rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionRule {
    /// Field name to redact.
    pub field: String,
    /// Redaction mode.
    pub mode: RedactionMode,
}

/// How a field value is redacted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedactionMode {
    /// Replace with a static mask string (e.g., "***@***.com").
    Mask(String),
    /// Replace with SHA-256 hash of the original value (pseudonymization).
    /// Joinable across queries but not human-readable.
    Hash,
    /// Replace with null.
    Null,
}

/// Redaction policy store.
pub struct RedactionStore {
    /// "{collection}:{role}" → redaction policy.
    policies: RwLock<HashMap<String, RedactionPolicy>>,
}

impl RedactionStore {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
        }
    }

    /// Create or replace a redaction policy.
    pub fn create_policy(&self, policy: RedactionPolicy) {
        let key = format!("{}:{}", policy.collection, policy.for_role);
        let mut policies = self.policies.write().unwrap_or_else(|p| p.into_inner());
        info!(
            name = %policy.name,
            collection = %policy.collection,
            role = %policy.for_role,
            rules = policy.rules.len(),
            "redaction policy created"
        );
        policies.insert(key, policy);
    }

    /// Drop a redaction policy.
    pub fn drop_policy(&self, collection: &str, for_role: &str) -> bool {
        let key = format!("{collection}:{for_role}");
        let mut policies = self.policies.write().unwrap_or_else(|p| p.into_inner());
        policies.remove(&key).is_some()
    }

    /// Get redaction rules for a collection + role combination.
    pub fn rules_for(&self, collection: &str, role: &str) -> Vec<RedactionRule> {
        let key = format!("{collection}:{role}");
        let policies = self.policies.read().unwrap_or_else(|p| p.into_inner());
        policies
            .get(&key)
            .map(|p| p.rules.clone())
            .unwrap_or_default()
    }

    /// Apply redaction rules to a JSON document.
    ///
    /// Modifies the document in-place, replacing redacted field values.
    pub fn apply(&self, collection: &str, roles: &[String], doc: &mut serde_json::Value) {
        let policies = self.policies.read().unwrap_or_else(|p| p.into_inner());
        for role in roles {
            let key = format!("{collection}:{role}");
            if let Some(policy) = policies.get(&key) {
                for rule in &policy.rules {
                    if let Some(obj) = doc.as_object_mut()
                        && obj.contains_key(&rule.field)
                    {
                        let redacted = match &rule.mode {
                            RedactionMode::Mask(mask) => serde_json::Value::String(mask.clone()),
                            RedactionMode::Hash => {
                                let original = obj
                                    .get(&rule.field)
                                    .map(|v| v.to_string())
                                    .unwrap_or_default();
                                serde_json::Value::String(hash_value(&original))
                            }
                            RedactionMode::Null => serde_json::Value::Null,
                        };
                        obj.insert(rule.field.clone(), redacted);
                    }
                }
            }
        }
    }

    /// List all redaction policies.
    pub fn list(&self) -> Vec<RedactionPolicy> {
        let policies = self.policies.read().unwrap_or_else(|p| p.into_inner());
        policies.values().cloned().collect()
    }
}

impl Default for RedactionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// SHA-256 hash for pseudonymization.
fn hash_value(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(input.as_bytes());
    format!("hash:{:x}", hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mask_redaction() {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: "mask_pii".into(),
            collection: "users".into(),
            for_role: "support".into(),
            rules: vec![
                RedactionRule {
                    field: "email".into(),
                    mode: RedactionMode::Mask("***@***.com".into()),
                },
                RedactionRule {
                    field: "ssn".into(),
                    mode: RedactionMode::Mask("***-**-****".into()),
                },
            ],
        });

        let mut doc = json!({"email": "alice@example.com", "ssn": "123-45-6789", "name": "Alice"});
        store.apply("users", &["support".into()], &mut doc);

        assert_eq!(doc["email"], "***@***.com");
        assert_eq!(doc["ssn"], "***-**-****");
        assert_eq!(doc["name"], "Alice"); // Not redacted.
    }

    #[test]
    fn hash_pseudonymization() {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: "pseudo".into(),
            collection: "users".into(),
            for_role: "analyst".into(),
            rules: vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Hash,
            }],
        });

        let mut doc1 = json!({"email": "alice@example.com"});
        let mut doc2 = json!({"email": "alice@example.com"});
        store.apply("users", &["analyst".into()], &mut doc1);
        store.apply("users", &["analyst".into()], &mut doc2);

        // Same input → same hash (joinable).
        assert_eq!(doc1["email"], doc2["email"]);
        // But not the original value.
        assert_ne!(doc1["email"], "alice@example.com");
        assert!(doc1["email"].as_str().unwrap().starts_with("hash:"));
    }

    #[test]
    fn no_policy_no_redaction() {
        let store = RedactionStore::new();
        let mut doc = json!({"email": "alice@example.com"});
        store.apply("users", &["admin".into()], &mut doc);
        assert_eq!(doc["email"], "alice@example.com");
    }
}
