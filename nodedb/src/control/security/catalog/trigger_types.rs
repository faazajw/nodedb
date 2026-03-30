//! Type definitions for trigger catalog storage.

/// When the trigger fires relative to the DML operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

impl TriggerTiming {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Before => "BEFORE",
            Self::After => "AFTER",
            Self::InsteadOf => "INSTEAD OF",
        }
    }
}

/// Which DML event(s) the trigger responds to.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TriggerEvents {
    pub on_insert: bool,
    pub on_update: bool,
    pub on_delete: bool,
}

impl TriggerEvents {
    pub fn display(&self) -> String {
        let mut parts = Vec::new();
        if self.on_insert {
            parts.push("INSERT");
        }
        if self.on_update {
            parts.push("UPDATE");
        }
        if self.on_delete {
            parts.push("DELETE");
        }
        parts.join(" OR ")
    }
}

/// Row-level or statement-level granularity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerGranularity {
    Row,
    Statement,
}

impl TriggerGranularity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Row => "FOR EACH ROW",
            Self::Statement => "FOR EACH STATEMENT",
        }
    }
}

/// Serializable trigger definition for redb storage.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredTrigger {
    pub tenant_id: u32,
    pub name: String,
    /// Collection this trigger is attached to.
    pub collection: String,
    pub timing: TriggerTiming,
    pub events: TriggerEvents,
    pub granularity: TriggerGranularity,
    /// Optional WHEN condition (SQL expression). Trigger body only fires
    /// if this predicate evaluates to true for the row.
    #[serde(default)]
    pub when_condition: Option<String>,
    /// Procedural SQL body (BEGIN ... END).
    pub body_sql: String,
    /// Firing priority. Lower numbers fire first.
    /// Tiebreaker: alphabetical by trigger name.
    #[serde(default = "default_priority")]
    pub priority: i32,
    /// Whether the trigger is currently enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub owner: String,
    pub created_at: u64,
}

fn default_priority() -> i32 {
    0
}

fn default_enabled() -> bool {
    true
}

impl StoredTrigger {
    /// Sort key for deterministic execution order: (priority, name).
    pub fn sort_key(&self) -> (i32, &str) {
        (self.priority, &self.name)
    }
}
