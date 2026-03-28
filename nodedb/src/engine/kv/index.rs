//! KV secondary indexes: in-memory B-Tree indexes on value fields.
//!
//! Each `KvFieldIndex` maps a single field's value → set of primary keys.
//! `KvIndexSet` manages all indexes for a collection with write-amp tracking.
//!
//! Design:
//! - In-memory BTreeMap (matches the ephemeral hash table — both rebuilt from WAL).
//! - O(log n) insert/delete/range scan per indexed field.
//! - Synchronous maintenance on every PUT/DELETE — no eventual consistency.
//! - Zero-index fast path: `KvIndexSet::is_empty()` lets callers skip entirely.

use std::collections::{BTreeMap, BTreeSet};

/// A single secondary index on one value field.
///
/// Maps field value bytes → set of primary keys that have that value.
/// Sorted by field value (BTreeMap) for efficient range scans.
#[derive(Debug)]
pub struct KvFieldIndex {
    /// Field name this index covers.
    field: String,
    /// Field position in the schema column list (for Binary Tuple extraction).
    field_position: usize,
    /// value_bytes → set of primary_key_bytes.
    tree: BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>,
}

impl KvFieldIndex {
    pub fn new(field: impl Into<String>, field_position: usize) -> Self {
        Self {
            field: field.into(),
            field_position,
            tree: BTreeMap::new(),
        }
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn field_position(&self) -> usize {
        self.field_position
    }

    /// Insert a (value, primary_key) pair into the index.
    pub fn insert(&mut self, field_value: Vec<u8>, primary_key: Vec<u8>) {
        self.tree
            .entry(field_value)
            .or_default()
            .insert(primary_key);
    }

    /// Remove a (value, primary_key) pair from the index.
    ///
    /// Returns true if the pair was found and removed.
    pub fn remove(&mut self, field_value: &[u8], primary_key: &[u8]) -> bool {
        if let Some(keys) = self.tree.get_mut(field_value) {
            let removed = keys.remove(primary_key);
            if keys.is_empty() {
                self.tree.remove(field_value);
            }
            removed
        } else {
            false
        }
    }

    /// Exact-match lookup: find all primary keys where field == value.
    pub fn lookup_eq(&self, field_value: &[u8]) -> Vec<&[u8]> {
        self.tree
            .get(field_value)
            .map(|keys| keys.iter().map(|k| k.as_slice()).collect())
            .unwrap_or_default()
    }

    /// Range lookup: find all primary keys where field value is in [lower, upper).
    ///
    /// `lower` = None means unbounded start. `upper` = None means unbounded end.
    pub fn lookup_range(&self, lower: Option<&[u8]>, upper: Option<&[u8]>) -> Vec<(&[u8], &[u8])> {
        use std::ops::Bound;

        let lo = match lower {
            Some(l) => Bound::Included(l.to_vec()),
            None => Bound::Unbounded,
        };
        let hi = match upper {
            Some(u) => Bound::Excluded(u.to_vec()),
            None => Bound::Unbounded,
        };

        let mut results = Vec::new();
        for (value, keys) in self.tree.range((lo, hi)) {
            for key in keys {
                results.push((value.as_slice(), key.as_slice()));
            }
        }
        results
    }

    /// Total number of index entries (sum of all primary key sets).
    pub fn entry_count(&self) -> usize {
        self.tree.values().map(|s| s.len()).sum()
    }

    /// Number of distinct field values indexed.
    pub fn distinct_values(&self) -> usize {
        self.tree.len()
    }

    /// Clear all entries (used during DROP INDEX).
    pub fn clear(&mut self) {
        self.tree.clear();
    }
}

/// A composite secondary index on multiple value fields.
///
/// Maps concatenated field values → set of primary keys. The composite key
/// is built by joining individual field value bytes with a null separator.
/// Supports prefix-based lookups (e.g., `WHERE a = X` on a `(a, b)` index).
#[derive(Debug)]
pub struct KvCompositeIndex {
    /// Field names in index order.
    fields: Vec<String>,
    /// Field positions in the schema column list.
    field_positions: Vec<usize>,
    /// composite_key_bytes → set of primary_key_bytes.
    tree: BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>,
}

impl KvCompositeIndex {
    pub fn new(fields: Vec<String>, field_positions: Vec<usize>) -> Self {
        Self {
            fields,
            field_positions,
            tree: BTreeMap::new(),
        }
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    pub fn field_positions(&self) -> &[usize] {
        &self.field_positions
    }

    /// Build a composite key from individual field values.
    ///
    /// Values are concatenated with `\0` separator. This preserves
    /// lexicographic ordering for prefix scans on leading fields.
    ///
    /// **Limitation:** field values must not contain null bytes (`\0`), as they
    /// are used as separators. This holds for typical KV keys (strings, UUIDs,
    /// integers encoded as big-endian bytes).
    fn build_key(values: &[&[u8]]) -> Vec<u8> {
        let mut key = Vec::new();
        for (i, v) in values.iter().enumerate() {
            if i > 0 {
                key.push(0); // Null separator.
            }
            key.extend_from_slice(v);
        }
        key
    }

    /// Insert a composite entry.
    pub fn insert(&mut self, field_values: &[&[u8]], primary_key: Vec<u8>) {
        let key = Self::build_key(field_values);
        self.tree.entry(key).or_default().insert(primary_key);
    }

    /// Remove a composite entry.
    pub fn remove(&mut self, field_values: &[&[u8]], primary_key: &[u8]) -> bool {
        let key = Self::build_key(field_values);
        if let Some(keys) = self.tree.get_mut(&key) {
            let removed = keys.remove(primary_key);
            if keys.is_empty() {
                self.tree.remove(&key);
            }
            removed
        } else {
            false
        }
    }

    /// Exact-match lookup on all fields.
    pub fn lookup_eq(&self, field_values: &[&[u8]]) -> Vec<&[u8]> {
        let key = Self::build_key(field_values);
        self.tree
            .get(&key)
            .map(|keys| keys.iter().map(|k| k.as_slice()).collect())
            .unwrap_or_default()
    }

    /// Prefix lookup: match on leading fields only.
    ///
    /// E.g., on a `(region, status)` index, `lookup_prefix(&[b"us-east"])`
    /// returns all keys where `region = "us-east"` regardless of status.
    ///
    /// Uses `starts_with()` on the B-Tree range to avoid false matches from
    /// the `0xFF` upper-bound trick, which breaks if field values contain
    /// bytes >= `0xFF`.
    pub fn lookup_prefix(&self, prefix_values: &[&[u8]]) -> Vec<&[u8]> {
        let prefix = Self::build_key(prefix_values);
        let mut results = Vec::new();
        for (composite_key, primary_keys) in self.tree.range(prefix.clone()..) {
            if !composite_key.starts_with(&prefix) {
                break;
            }
            for pk in primary_keys {
                results.push(pk.as_slice());
            }
        }
        results
    }

    /// Total number of index entries.
    pub fn entry_count(&self) -> usize {
        self.tree.values().map(|s| s.len()).sum()
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.tree.clear();
    }
}

/// Manages all secondary indexes for a single KV collection.
///
/// Tracks write amplification and provides the zero-index fast path check.
#[derive(Debug)]
pub struct KvIndexSet {
    /// Single-field indexes.
    indexes: Vec<KvFieldIndex>,
    /// Composite (multi-field) indexes.
    composite_indexes: Vec<KvCompositeIndex>,
    /// Total PUT operations on this collection (denominator for write-amp ratio).
    total_puts: u64,
    /// Total index write operations (numerator for write-amp ratio).
    total_index_writes: u64,
}

impl KvIndexSet {
    pub fn new() -> Self {
        Self {
            indexes: Vec::new(),
            composite_indexes: Vec::new(),
            total_puts: 0,
            total_index_writes: 0,
        }
    }

    /// Whether this collection has zero secondary indexes (fast path eligible).
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty() && self.composite_indexes.is_empty()
    }

    /// Number of active indexes (single + composite).
    pub fn index_count(&self) -> usize {
        self.indexes.len() + self.composite_indexes.len()
    }

    /// Add a new index on a field. Returns false if already indexed.
    pub fn add_index(&mut self, field: &str, field_position: usize) -> bool {
        if self.indexes.iter().any(|i| i.field == field) {
            return false;
        }
        self.indexes.push(KvFieldIndex::new(field, field_position));
        true
    }

    /// Remove an index on a field. Returns the removed index, or None if not found.
    pub fn remove_index(&mut self, field: &str) -> Option<KvFieldIndex> {
        if let Some(pos) = self.indexes.iter().position(|i| i.field == field) {
            Some(self.indexes.remove(pos))
        } else {
            None
        }
    }

    /// Get an index by field name.
    pub fn get_index(&self, field: &str) -> Option<&KvFieldIndex> {
        self.indexes.iter().find(|i| i.field == field)
    }

    /// Add a composite index on multiple fields. Returns false if already exists.
    pub fn add_composite_index(
        &mut self,
        fields: Vec<String>,
        field_positions: Vec<usize>,
    ) -> bool {
        if self.composite_indexes.iter().any(|ci| ci.fields == fields) {
            return false;
        }
        self.composite_indexes
            .push(KvCompositeIndex::new(fields, field_positions));
        true
    }

    /// Remove a composite index. Returns the removed index, or None.
    pub fn remove_composite_index(&mut self, fields: &[String]) -> Option<KvCompositeIndex> {
        if let Some(pos) = self
            .composite_indexes
            .iter()
            .position(|ci| ci.fields == fields)
        {
            Some(self.composite_indexes.remove(pos))
        } else {
            None
        }
    }

    /// Get a composite index by its field list.
    pub fn get_composite_index(&self, fields: &[String]) -> Option<&KvCompositeIndex> {
        self.composite_indexes.iter().find(|ci| ci.fields == fields)
    }

    /// Find a composite index that has the given field as a leading prefix.
    pub fn find_composite_with_prefix(&self, field: &str) -> Option<&KvCompositeIndex> {
        self.composite_indexes
            .iter()
            .find(|ci| ci.fields.first().is_some_and(|f| f == field))
    }

    /// Record a PUT and update all indexes with the new field values.
    ///
    /// `field_values` is an iterator of `(field_name, field_value_bytes)` extracted
    /// from the value being inserted. Only indexed fields are processed.
    ///
    /// Returns the number of index writes performed.
    pub fn on_put(
        &mut self,
        primary_key: &[u8],
        field_values: &[(&str, &[u8])],
        old_field_values: Option<&[(&str, &[u8])]>,
    ) -> usize {
        self.total_puts += 1;

        if self.is_empty() {
            return 0;
        }

        let mut writes = 0;

        // Remove old single-field index entries (if this is an update).
        if let Some(old_values) = old_field_values {
            for idx in &mut self.indexes {
                for &(field, value) in old_values {
                    if field == idx.field {
                        idx.remove(value, primary_key);
                        writes += 1;
                    }
                }
            }
        }

        // Insert new single-field index entries.
        for idx in &mut self.indexes {
            for &(field, value) in field_values {
                if field == idx.field {
                    idx.insert(value.to_vec(), primary_key.to_vec());
                    writes += 1;
                }
            }
        }

        // Maintain composite indexes.
        for ci in &mut self.composite_indexes {
            // Remove old composite entry.
            if let Some(old_values) = old_field_values {
                let old_vals: Vec<&[u8]> = ci
                    .fields
                    .iter()
                    .filter_map(|f| {
                        old_values
                            .iter()
                            .find(|(name, _)| *name == f.as_str())
                            .map(|(_, v)| *v)
                    })
                    .collect();
                if old_vals.len() == ci.fields.len() {
                    ci.remove(&old_vals, primary_key);
                    writes += 1;
                }
            }

            // Insert new composite entry.
            let new_vals: Vec<&[u8]> = ci
                .fields
                .iter()
                .filter_map(|f| {
                    field_values
                        .iter()
                        .find(|(name, _)| *name == f.as_str())
                        .map(|(_, v)| *v)
                })
                .collect();
            if new_vals.len() == ci.fields.len() {
                ci.insert(&new_vals, primary_key.to_vec());
                writes += 1;
            }
        }

        self.total_index_writes += writes as u64;
        writes
    }

    /// Remove all index entries for a deleted primary key.
    ///
    /// `field_values` are the field values from the deleted entry.
    pub fn on_delete(&mut self, primary_key: &[u8], field_values: &[(&str, &[u8])]) {
        for idx in &mut self.indexes {
            for &(field, value) in field_values {
                if field == idx.field {
                    idx.remove(value, primary_key);
                    self.total_index_writes += 1;
                }
            }
        }

        // Maintain composite indexes on delete.
        for ci in &mut self.composite_indexes {
            let vals: Vec<&[u8]> = ci
                .fields
                .iter()
                .filter_map(|f| {
                    field_values
                        .iter()
                        .find(|(name, _)| *name == f.as_str())
                        .map(|(_, v)| *v)
                })
                .collect();
            if vals.len() == ci.fields.len() {
                ci.remove(&vals, primary_key);
                self.total_index_writes += 1;
            }
        }
    }

    /// Write amplification ratio: total_index_writes / total_puts.
    ///
    /// Returns 0.0 if no PUTs have been performed.
    pub fn write_amp_ratio(&self) -> f64 {
        if self.total_puts == 0 {
            return 0.0;
        }
        self.total_index_writes as f64 / self.total_puts as f64
    }

    /// Lookup primary keys by exact field value match.
    pub fn lookup_eq(&self, field: &str, value: &[u8]) -> Vec<&[u8]> {
        self.indexes
            .iter()
            .find(|i| i.field == field)
            .map(|i| i.lookup_eq(value))
            .unwrap_or_default()
    }

    /// Lookup primary keys by field value range.
    pub fn lookup_range(
        &self,
        field: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Vec<(&[u8], &[u8])> {
        self.indexes
            .iter()
            .find(|i| i.field == field)
            .map(|i| i.lookup_range(lower, upper))
            .unwrap_or_default()
    }

    /// Iterator over all index field names.
    pub fn indexed_fields(&self) -> impl Iterator<Item = &str> {
        self.indexes.iter().map(|i| i.field.as_str())
    }
}

impl Default for KvIndexSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_index_insert_and_lookup() {
        let mut idx = KvFieldIndex::new("region", 2);
        idx.insert(b"us-east".to_vec(), b"key1".to_vec());
        idx.insert(b"us-east".to_vec(), b"key2".to_vec());
        idx.insert(b"eu-west".to_vec(), b"key3".to_vec());

        let results = idx.lookup_eq(b"us-east");
        assert_eq!(results.len(), 2);
        assert!(results.contains(&b"key1".as_slice()));
        assert!(results.contains(&b"key2".as_slice()));

        let results = idx.lookup_eq(b"eu-west");
        assert_eq!(results.len(), 1);

        let results = idx.lookup_eq(b"ap-south");
        assert!(results.is_empty());
    }

    #[test]
    fn field_index_remove() {
        let mut idx = KvFieldIndex::new("status", 1);
        idx.insert(b"active".to_vec(), b"k1".to_vec());
        idx.insert(b"active".to_vec(), b"k2".to_vec());

        assert!(idx.remove(b"active", b"k1"));
        assert_eq!(idx.lookup_eq(b"active").len(), 1);

        assert!(idx.remove(b"active", b"k2"));
        assert!(idx.lookup_eq(b"active").is_empty());
        assert_eq!(idx.distinct_values(), 0);

        // Remove nonexistent.
        assert!(!idx.remove(b"active", b"k3"));
    }

    #[test]
    fn field_index_range_lookup() {
        let mut idx = KvFieldIndex::new("score", 0);
        for i in 0u32..10 {
            idx.insert(i.to_be_bytes().to_vec(), format!("k{i}").into_bytes());
        }

        // Range [3, 7)
        let results = idx.lookup_range(Some(&3u32.to_be_bytes()), Some(&7u32.to_be_bytes()));
        assert_eq!(results.len(), 4); // 3, 4, 5, 6
    }

    #[test]
    fn index_set_zero_index_fast_path() {
        let set = KvIndexSet::new();
        assert!(set.is_empty());
        assert_eq!(set.index_count(), 0);
    }

    #[test]
    fn index_set_add_and_remove() {
        let mut set = KvIndexSet::new();
        assert!(set.add_index("region", 2));
        assert!(!set.add_index("region", 2)); // Duplicate.
        assert_eq!(set.index_count(), 1);
        assert!(!set.is_empty());

        assert!(set.remove_index("region").is_some());
        assert!(set.is_empty());
        assert!(set.remove_index("region").is_none());
    }

    #[test]
    fn index_set_on_put_maintains_indexes() {
        let mut set = KvIndexSet::new();
        set.add_index("region", 2);
        set.add_index("status", 3);

        let field_values: Vec<(&str, &[u8])> = vec![("region", b"us-east"), ("status", b"active")];

        let writes = set.on_put(b"key1", &field_values, None);
        assert_eq!(writes, 2); // One per index.

        assert_eq!(set.lookup_eq("region", b"us-east").len(), 1);
        assert_eq!(set.lookup_eq("status", b"active").len(), 1);
    }

    #[test]
    fn index_set_on_put_update_replaces_old() {
        let mut set = KvIndexSet::new();
        set.add_index("status", 0);

        // Insert.
        set.on_put(b"k1", &[("status", b"active")], None);
        assert_eq!(set.lookup_eq("status", b"active").len(), 1);

        // Update: old was "active", new is "inactive".
        set.on_put(
            b"k1",
            &[("status", b"inactive")],
            Some(&[("status", b"active")]),
        );
        assert!(set.lookup_eq("status", b"active").is_empty());
        assert_eq!(set.lookup_eq("status", b"inactive").len(), 1);
    }

    #[test]
    fn index_set_on_delete_cleans_up() {
        let mut set = KvIndexSet::new();
        set.add_index("region", 0);

        set.on_put(b"k1", &[("region", b"us")], None);
        set.on_put(b"k2", &[("region", b"us")], None);
        assert_eq!(set.lookup_eq("region", b"us").len(), 2);

        set.on_delete(b"k1", &[("region", b"us")]);
        assert_eq!(set.lookup_eq("region", b"us").len(), 1);
    }

    #[test]
    fn write_amp_ratio() {
        let mut set = KvIndexSet::new();
        set.add_index("a", 0);
        set.add_index("b", 1);

        for i in 0..10 {
            let k = format!("k{i}");
            set.on_put(k.as_bytes(), &[("a", b"x"), ("b", b"y")], None);
        }
        // 10 PUTs, 2 index writes each = 20 index writes.
        assert!((set.write_amp_ratio() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn unindexed_field_ignored() {
        let mut set = KvIndexSet::new();
        set.add_index("region", 0);

        // PUT with a field that isn't indexed — should be ignored.
        let writes = set.on_put(b"k1", &[("name", b"alice")], None);
        assert_eq!(writes, 0);
    }

    // -----------------------------------------------------------------------
    // Composite index tests
    // -----------------------------------------------------------------------

    #[test]
    fn composite_index_insert_and_exact_lookup() {
        let mut ci = KvCompositeIndex::new(vec!["region".into(), "status".into()], vec![0, 1]);
        ci.insert(&[b"us-east", b"active"], b"k1".to_vec());
        ci.insert(&[b"us-east", b"inactive"], b"k2".to_vec());
        ci.insert(&[b"eu-west", b"active"], b"k3".to_vec());

        let results = ci.lookup_eq(&[b"us-east", b"active"]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], b"k1");

        let results = ci.lookup_eq(&[b"eu-west", b"active"]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], b"k3");

        // Non-matching returns empty.
        assert!(ci.lookup_eq(&[b"ap-south", b"active"]).is_empty());
    }

    #[test]
    fn composite_index_prefix_lookup() {
        let mut ci = KvCompositeIndex::new(vec!["region".into(), "status".into()], vec![0, 1]);
        ci.insert(&[b"us-east", b"active"], b"k1".to_vec());
        ci.insert(&[b"us-east", b"inactive"], b"k2".to_vec());
        ci.insert(&[b"eu-west", b"active"], b"k3".to_vec());

        // Prefix lookup on leading field only.
        let results = ci.lookup_prefix(&[b"us-east"]);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn composite_index_remove() {
        let mut ci = KvCompositeIndex::new(vec!["a".into(), "b".into()], vec![0, 1]);
        ci.insert(&[b"x", b"y"], b"k1".to_vec());
        assert_eq!(ci.entry_count(), 1);

        assert!(ci.remove(&[b"x", b"y"], b"k1"));
        assert_eq!(ci.entry_count(), 0);
    }

    #[test]
    fn index_set_composite_on_put() {
        let mut set = KvIndexSet::new();
        set.add_composite_index(vec!["region".into(), "status".into()], vec![0, 1]);

        let writes = set.on_put(b"k1", &[("region", b"us"), ("status", b"active")], None);
        assert!(writes > 0);

        // Lookup via composite index.
        let ci = set
            .get_composite_index(&["region".into(), "status".into()])
            .unwrap();
        let results = ci.lookup_eq(&[b"us", b"active"]);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn index_set_composite_on_delete() {
        let mut set = KvIndexSet::new();
        set.add_composite_index(vec!["a".into(), "b".into()], vec![0, 1]);

        set.on_put(b"k1", &[("a", b"x"), ("b", b"y")], None);
        set.on_delete(b"k1", &[("a", b"x"), ("b", b"y")]);

        let ci = set.get_composite_index(&["a".into(), "b".into()]).unwrap();
        assert!(ci.lookup_eq(&[b"x", b"y"]).is_empty());
    }
}
