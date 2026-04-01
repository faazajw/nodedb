//! Per-session plan cache for prepared statements.
//!
//! Caches DataFusion `LogicalPlan` per SQL string, keyed by `(sql_hash, schema_version)`.
//! When the schema version changes (CREATE/DROP/ALTER), cached plans are invalidated.
//! This avoids re-parsing identical SQL on every Execute in the extended query protocol.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::logical_expr::LogicalPlan;

/// Global schema version counter, bumped on any DDL that changes the schema.
///
/// Stored on `SharedState`. Plan caches compare their snapshot against this
/// to detect invalidation.
pub struct SchemaVersion {
    version: AtomicU64,
}

impl SchemaVersion {
    pub fn new() -> Self {
        Self {
            version: AtomicU64::new(1),
        }
    }

    /// Get current schema version.
    pub fn current(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Bump the schema version. Called on CREATE/DROP/ALTER DDL.
    pub fn bump(&self) -> u64 {
        self.version.fetch_add(1, Ordering::AcqRel) + 1
    }
}

impl Default for SchemaVersion {
    fn default() -> Self {
        Self::new()
    }
}

/// Cached plan entry: the plan and the schema version it was compiled against.
struct CachedPlan {
    plan: LogicalPlan,
    schema_version: u64,
}

/// Per-session LRU plan cache.
///
/// Keyed by SQL hash. Each entry records the schema version at plan time.
/// On lookup, if the schema version has changed, the entry is evicted.
pub struct PlanCache {
    entries: HashMap<u64, CachedPlan>,
    max_entries: usize,
    /// Insertion order for LRU eviction (oldest first).
    order: Vec<u64>,
}

impl PlanCache {
    /// Create a new plan cache.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
            order: Vec::new(),
        }
    }

    /// Look up a cached plan for the given SQL.
    ///
    /// Returns `None` if the plan is not cached or if the schema version
    /// has changed (the stale entry is evicted).
    pub fn get(&mut self, sql: &str, current_schema_version: u64) -> Option<&LogicalPlan> {
        let key = hash_sql(sql);

        // Check if entry exists and whether version matches.
        let version_matches = self
            .entries
            .get(&key)
            .map(|entry| entry.schema_version == current_schema_version);

        match version_matches {
            Some(true) => {
                // Safe: we just checked the entry exists.
                Some(&self.entries.get(&key).expect("just checked").plan)
            }
            Some(false) => {
                // Schema changed — evict the stale entry.
                self.entries.remove(&key);
                self.order.retain(|k| *k != key);
                None
            }
            None => None,
        }
    }

    /// Store a plan in the cache.
    pub fn put(&mut self, sql: &str, plan: LogicalPlan, schema_version: u64) {
        let key = hash_sql(sql);

        // If replacing an existing entry, don't change LRU order.
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.entries.entry(key) {
            e.insert(CachedPlan {
                plan,
                schema_version,
            });
            return;
        }

        // Evict oldest if at capacity.
        while self.entries.len() >= self.max_entries {
            if let Some(oldest_key) = self.order.first().copied() {
                self.entries.remove(&oldest_key);
                self.order.remove(0);
            } else {
                break;
            }
        }

        self.entries.insert(
            key,
            CachedPlan {
                plan,
                schema_version,
            },
        );
        self.order.push(key);
    }

    /// Invalidate all entries (called on DISCARD ALL, session reset, etc.).
    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

fn hash_sql(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn dummy_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        })
    }

    #[test]
    fn cache_hit_same_version() {
        let mut cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_plan(), 1);
        assert!(cache.get("SELECT 1", 1).is_some());
    }

    #[test]
    fn cache_miss_version_change() {
        let mut cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_plan(), 1);
        // Schema version bumped — cache miss.
        assert!(cache.get("SELECT 1", 2).is_none());
        // Entry should be evicted.
        assert!(cache.get("SELECT 1", 1).is_none());
    }

    #[test]
    fn lru_eviction() {
        let mut cache = PlanCache::new(2);
        cache.put("SELECT 1", dummy_plan(), 1);
        cache.put("SELECT 2", dummy_plan(), 1);
        // Cache is full — inserting a third evicts the oldest.
        cache.put("SELECT 3", dummy_plan(), 1);
        assert!(cache.get("SELECT 1", 1).is_none());
        assert!(cache.get("SELECT 2", 1).is_some());
        assert!(cache.get("SELECT 3", 1).is_some());
    }

    #[test]
    fn clear_empties_cache() {
        let mut cache = PlanCache::new(10);
        cache.put("SELECT 1", dummy_plan(), 1);
        cache.clear();
        assert!(cache.get("SELECT 1", 1).is_none());
    }

    #[test]
    fn schema_version_bump() {
        let sv = SchemaVersion::new();
        assert_eq!(sv.current(), 1);
        assert_eq!(sv.bump(), 2);
        assert_eq!(sv.current(), 2);
    }
}
