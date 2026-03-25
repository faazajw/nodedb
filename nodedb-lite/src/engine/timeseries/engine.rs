//! Lite timeseries engine: columnar memtable + Gorilla compression + redb persistence.
//!
//! Reuses shared types from `nodedb-types`: GorillaEncoder/Decoder, SeriesKey,
//! SeriesCatalog, SymbolDictionary, TieredPartitionConfig, PartitionMeta.
//!
//! Architecture:
//! - In-memory columnar memtable (same layout as Origin)
//! - Flush to redb with partition key prefix (`ts:{collection}:{partition_start}:{column}`)
//! - Auto-partitioning biased toward coarser intervals (DAY/WEEK for Lite)
//! - Auto retention (7d default)
//! - Gorilla compression for timestamp and f64 columns

use std::collections::HashMap;

use nodedb_types::gorilla::{GorillaDecoder, GorillaEncoder};
use nodedb_types::timeseries::{
    MetricSample, PartitionMeta, PartitionState, SeriesCatalog, SeriesId, SeriesKey,
    TieredPartitionConfig, TimeRange,
};

/// Lite timeseries engine.
///
/// Not `Send` — owned by a single task. The `NodeDbLite` wrapper handles
/// async bridging.
pub struct TimeseriesEngine {
    /// Per-collection columnar data: `collection → CollectionTs`.
    collections: HashMap<String, CollectionTs>,
    /// Series catalog (shared across all collections for collision safety).
    catalog: SeriesCatalog,
    /// Global config (Lite defaults: 7d retention, 4MB memtable, etc.)
    config: TieredPartitionConfig,
}

/// Per-collection timeseries state.
struct CollectionTs {
    /// In-memory columnar buffers (hot data).
    timestamps: Vec<i64>,
    values: Vec<f64>,
    series_ids: Vec<SeriesId>,
    /// Total memory estimate in bytes.
    memory_bytes: usize,
    /// Partition boundaries for flushed data.
    partitions: Vec<FlushedPartition>,
}

/// A flushed partition stored in redb.
#[derive(Debug, Clone)]
struct FlushedPartition {
    meta: PartitionMeta,
    /// redb key prefix: `ts:{collection}:{start_ms}`.
    key_prefix: String,
}

impl CollectionTs {
    fn new() -> Self {
        Self {
            timestamps: Vec::with_capacity(4096),
            values: Vec::with_capacity(4096),
            series_ids: Vec::with_capacity(4096),
            memory_bytes: 0,
            partitions: Vec::new(),
        }
    }

    fn row_count(&self) -> usize {
        self.timestamps.len()
    }
}

impl Default for TimeseriesEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeseriesEngine {
    /// Create with Lite defaults.
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
            catalog: SeriesCatalog::new(),
            config: TieredPartitionConfig::lite_defaults(),
        }
    }

    /// Create with custom config.
    pub fn with_config(config: TieredPartitionConfig) -> Self {
        Self {
            collections: HashMap::new(),
            catalog: SeriesCatalog::new(),
            config,
        }
    }

    // ─── Ingest ──────────────────────────────────────────────────────

    /// Ingest a metric sample.
    ///
    /// Returns true if the memtable needs flushing (memory pressure).
    pub fn ingest_metric(
        &mut self,
        collection: &str,
        metric_name: &str,
        tags: Vec<(String, String)>,
        sample: MetricSample,
    ) -> bool {
        let key = SeriesKey::new(metric_name, tags);
        let series_id = self.catalog.resolve(&key);

        let coll = self
            .collections
            .entry(collection.to_string())
            .or_insert_with(CollectionTs::new);

        coll.timestamps.push(sample.timestamp_ms);
        coll.values.push(sample.value);
        coll.series_ids.push(series_id);
        coll.memory_bytes += 24; // 8 + 8 + 8 bytes per row

        coll.memory_bytes >= self.config.memtable_max_memory_bytes as usize
    }

    /// Ingest a batch of samples for one series.
    pub fn ingest_batch(
        &mut self,
        collection: &str,
        metric_name: &str,
        tags: Vec<(String, String)>,
        samples: &[MetricSample],
    ) -> bool {
        let key = SeriesKey::new(metric_name, tags);
        let series_id = self.catalog.resolve(&key);

        let coll = self
            .collections
            .entry(collection.to_string())
            .or_insert_with(CollectionTs::new);

        for sample in samples {
            coll.timestamps.push(sample.timestamp_ms);
            coll.values.push(sample.value);
            coll.series_ids.push(series_id);
        }
        coll.memory_bytes += samples.len() * 24;

        coll.memory_bytes >= self.config.memtable_max_memory_bytes as usize
    }

    // ─── Flush to redb ───────────────────────────────────────────────

    /// Flush a collection's memtable to Gorilla-compressed redb entries.
    ///
    /// Returns the serialized partition entries for redb storage.
    /// The caller is responsible for writing to redb.
    pub fn flush(&mut self, collection: &str) -> Option<FlushResult> {
        let coll = self.collections.get_mut(collection)?;
        if coll.timestamps.is_empty() {
            return None;
        }

        let row_count = coll.row_count();
        let min_ts = *coll.timestamps.iter().min().unwrap_or(&0);
        let max_ts = *coll.timestamps.iter().max().unwrap_or(&0);

        // Gorilla-encode timestamps.
        let mut ts_encoder = GorillaEncoder::new();
        for &ts in &coll.timestamps {
            ts_encoder.encode(ts, 0.0);
        }
        let ts_block = ts_encoder.finish();

        // Gorilla-encode values.
        let mut val_encoder = GorillaEncoder::new();
        for (i, &val) in coll.values.iter().enumerate() {
            val_encoder.encode(i as i64, val);
        }
        let val_block = val_encoder.finish();

        // Series IDs as raw LE bytes.
        let series_block: Vec<u8> = coll
            .series_ids
            .iter()
            .flat_map(|&id| id.to_le_bytes())
            .collect();

        let key_prefix = format!("ts:{collection}:{min_ts}");

        let meta = PartitionMeta {
            min_ts,
            max_ts,
            row_count: row_count as u64,
            size_bytes: (ts_block.len() + val_block.len() + series_block.len()) as u64,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: (max_ts - min_ts) as u64,
            last_flushed_wal_lsn: 0,
        };

        let partition = FlushedPartition {
            meta: meta.clone(),
            key_prefix: key_prefix.clone(),
        };
        coll.partitions.push(partition);

        // Clear memtable.
        coll.timestamps.clear();
        coll.values.clear();
        coll.series_ids.clear();
        coll.memory_bytes = 0;

        Some(FlushResult {
            key_prefix,
            ts_block,
            val_block,
            series_block,
            meta,
        })
    }

    // ─── Query ───────────────────────────────────────────────────────

    /// Scan metric samples in a time range.
    ///
    /// Returns (timestamp, value, series_id) triples from both memtable
    /// (hot data) and flushed partitions (cold data).
    pub fn scan(&self, collection: &str, range: &TimeRange) -> Vec<(i64, f64, SeriesId)> {
        let Some(coll) = self.collections.get(collection) else {
            return Vec::new();
        };

        let mut results = Vec::new();

        // Scan memtable (hot data).
        for i in 0..coll.timestamps.len() {
            let ts = coll.timestamps[i];
            if range.contains(ts) {
                results.push((ts, coll.values[i], coll.series_ids[i]));
            }
        }

        // Results from flushed partitions would be read from redb by the caller
        // (the engine doesn't hold redb references). The caller passes decoded
        // partition data via `scan_with_partitions`.

        // Sort by timestamp.
        results.sort_by_key(|(ts, _, _)| *ts);
        results
    }

    /// Aggregate over a time range with time_bucket grouping.
    ///
    /// Returns `(bucket_start, count, sum, min, max)` per bucket.
    pub fn aggregate_by_bucket(
        &self,
        collection: &str,
        range: &TimeRange,
        bucket_ms: i64,
    ) -> Vec<(i64, u64, f64, f64, f64)> {
        let rows = self.scan(collection, range);
        if rows.is_empty() || bucket_ms <= 0 {
            return Vec::new();
        }

        let mut buckets: std::collections::BTreeMap<i64, (u64, f64, f64, f64)> =
            std::collections::BTreeMap::new();

        for (ts, val, _) in &rows {
            let bucket = (*ts / bucket_ms) * bucket_ms;
            let entry = buckets
                .entry(bucket)
                .or_insert((0, 0.0, f64::INFINITY, f64::NEG_INFINITY));
            entry.0 += 1;
            entry.1 += val;
            if *val < entry.2 {
                entry.2 = *val;
            }
            if *val > entry.3 {
                entry.3 = *val;
            }
        }

        buckets
            .into_iter()
            .map(|(bucket, (count, sum, min, max))| (bucket, count, sum, min, max))
            .collect()
    }

    // ─── Retention ───────────────────────────────────────────────────

    /// Drop partitions older than the retention period.
    ///
    /// Returns the key prefixes of dropped partitions (for redb cleanup).
    pub fn apply_retention(&mut self, now_ms: i64) -> Vec<String> {
        if self.config.retention_period_ms == 0 {
            return Vec::new();
        }
        let cutoff = now_ms - self.config.retention_period_ms as i64;
        let mut dropped = Vec::new();

        for coll in self.collections.values_mut() {
            coll.partitions.retain(|p| {
                if p.meta.max_ts < cutoff {
                    dropped.push(p.key_prefix.clone());
                    false
                } else {
                    true
                }
            });
        }
        dropped
    }

    // ─── Accessors ───────────────────────────────────────────────────

    pub fn collection_names(&self) -> Vec<&str> {
        self.collections.keys().map(|s| s.as_str()).collect()
    }

    pub fn row_count(&self, collection: &str) -> usize {
        self.collections
            .get(collection)
            .map(|c| c.row_count())
            .unwrap_or(0)
    }

    pub fn memory_bytes(&self, collection: &str) -> usize {
        self.collections
            .get(collection)
            .map(|c| c.memory_bytes)
            .unwrap_or(0)
    }

    pub fn partition_count(&self, collection: &str) -> usize {
        self.collections
            .get(collection)
            .map(|c| c.partitions.len())
            .unwrap_or(0)
    }

    pub fn catalog(&self) -> &SeriesCatalog {
        &self.catalog
    }

    pub fn config(&self) -> &TieredPartitionConfig {
        &self.config
    }

    /// Decode a flushed timestamp block (Gorilla-encoded).
    pub fn decode_timestamps(block: &[u8]) -> Vec<i64> {
        let mut dec = GorillaDecoder::new(block);
        dec.decode_all().into_iter().map(|(ts, _)| ts).collect()
    }

    /// Decode a flushed value block (Gorilla-encoded).
    pub fn decode_values(block: &[u8]) -> Vec<f64> {
        let mut dec = GorillaDecoder::new(block);
        dec.decode_all().into_iter().map(|(_, v)| v).collect()
    }

    /// Decode a flushed series_id block (raw LE u64).
    ///
    /// Each series ID is 8 bytes (u64 LE). Trailing bytes that don't form
    /// a complete u64 are silently ignored (via `chunks_exact`).
    pub fn decode_series_ids(block: &[u8]) -> Vec<SeriesId> {
        block
            .chunks_exact(8)
            .map(|chunk| {
                // Safety: chunks_exact(8) guarantees exactly 8 bytes.
                let arr: [u8; 8] = chunk
                    .try_into()
                    .expect("chunks_exact(8) guarantees 8 bytes");
                u64::from_le_bytes(arr)
            })
            .collect()
    }
}

/// A key-value entry for redb persistence.
pub type RedbEntry = (Vec<u8>, Vec<u8>);

/// Result of flushing a collection's memtable.
pub struct FlushResult {
    /// redb key prefix for this partition.
    pub key_prefix: String,
    /// Gorilla-encoded timestamps.
    pub ts_block: Vec<u8>,
    /// Gorilla-encoded values.
    pub val_block: Vec<u8>,
    /// Raw LE u64 series IDs.
    pub series_block: Vec<u8>,
    /// Partition metadata.
    pub meta: PartitionMeta,
}

impl FlushResult {
    /// redb key-value pairs to persist this partition.
    ///
    /// Returns `Err` if metadata serialization fails.
    pub fn to_redb_entries(&self) -> Result<Vec<RedbEntry>, serde_json::Error> {
        let meta_bytes = serde_json::to_vec(&self.meta)?;
        Ok(vec![
            (
                format!("{}:ts", self.key_prefix).into_bytes(),
                self.ts_block.clone(),
            ),
            (
                format!("{}:val", self.key_prefix).into_bytes(),
                self.val_block.clone(),
            ),
            (
                format!("{}:series", self.key_prefix).into_bytes(),
                self.series_block.clone(),
            ),
            (format!("{}:meta", self.key_prefix).into_bytes(), meta_bytes),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_and_scan() {
        let mut engine = TimeseriesEngine::new();

        for i in 0..100 {
            engine.ingest_metric(
                "metrics",
                "cpu_usage",
                vec![("host".into(), "prod-1".into())],
                MetricSample {
                    timestamp_ms: 1000 + i,
                    value: 50.0 + (i as f64) * 0.1,
                },
            );
        }

        assert_eq!(engine.row_count("metrics"), 100);

        let results = engine.scan("metrics", &TimeRange::new(1000, 1099));
        assert_eq!(results.len(), 100);
        assert_eq!(results[0].0, 1000); // sorted by timestamp
        assert_eq!(results[99].0, 1099);
    }

    #[test]
    fn aggregate_by_bucket() {
        let mut engine = TimeseriesEngine::new();

        for i in 0..100 {
            engine.ingest_metric(
                "metrics",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 10,
                    value: i as f64,
                },
            );
        }

        // 10ms bucket → 100 rows / 10ms per bucket = ~1 row per bucket at 10ms interval
        let buckets = engine.aggregate_by_bucket("metrics", &TimeRange::new(0, 999), 100);
        assert_eq!(buckets.len(), 10); // 0, 100, 200, ..., 900
        assert_eq!(buckets[0].1, 10); // 10 rows per 100ms bucket
    }

    #[test]
    fn flush_and_decode() {
        let mut engine = TimeseriesEngine::new();

        for i in 0..50 {
            engine.ingest_metric(
                "metrics",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: 5000 + i * 100,
                    value: (i as f64) * 0.5,
                },
            );
        }

        let flush = engine.flush("metrics").unwrap();
        assert_eq!(flush.meta.row_count, 50);
        assert_eq!(flush.meta.min_ts, 5000);

        // Decode and verify roundtrip.
        let decoded_ts = TimeseriesEngine::decode_timestamps(&flush.ts_block);
        assert_eq!(decoded_ts.len(), 50);
        assert_eq!(decoded_ts[0], 5000);
        assert_eq!(decoded_ts[49], 5000 + 49 * 100);

        let decoded_vals = TimeseriesEngine::decode_values(&flush.val_block);
        assert_eq!(decoded_vals.len(), 50);
        assert!((decoded_vals[0] - 0.0).abs() < f64::EPSILON);

        // Memtable should be empty after flush.
        assert_eq!(engine.row_count("metrics"), 0);
        assert_eq!(engine.partition_count("metrics"), 1);
    }

    #[test]
    fn retention() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            retention_period_ms: 1000, // 1 second
            ..TieredPartitionConfig::lite_defaults()
        });

        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.flush("metrics");

        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 2000,
                value: 2.0,
            },
        );
        engine.flush("metrics");

        assert_eq!(engine.partition_count("metrics"), 2);

        // Apply retention at t=2500 → partition at 100 (max_ts=100) is older than 1000ms ago.
        let dropped = engine.apply_retention(2500);
        assert_eq!(dropped.len(), 1);
        assert_eq!(engine.partition_count("metrics"), 1);
    }

    #[test]
    fn series_catalog_integration() {
        let mut engine = TimeseriesEngine::new();

        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![("host".into(), "a".into())],
            MetricSample {
                timestamp_ms: 100,
                value: 1.0,
            },
        );
        engine.ingest_metric(
            "metrics",
            "cpu",
            vec![("host".into(), "b".into())],
            MetricSample {
                timestamp_ms: 200,
                value: 2.0,
            },
        );
        engine.ingest_metric(
            "metrics",
            "mem",
            vec![("host".into(), "a".into())],
            MetricSample {
                timestamp_ms: 300,
                value: 3.0,
            },
        );

        // 3 distinct series in the catalog.
        assert_eq!(engine.catalog().len(), 3);
    }

    #[test]
    fn flush_result_redb_entries() {
        let mut engine = TimeseriesEngine::new();
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 1000,
                value: 42.0,
            },
        );
        let flush = engine.flush("m").unwrap();
        let entries = flush.to_redb_entries().unwrap();
        assert_eq!(entries.len(), 4); // ts, val, series, meta
        assert!(entries[0].0.starts_with(b"ts:m:1000:ts"));
    }

    #[test]
    fn empty_scan() {
        let engine = TimeseriesEngine::new();
        assert!(
            engine
                .scan("nonexistent", &TimeRange::new(0, 1000))
                .is_empty()
        );
    }

    #[test]
    fn batch_ingest() {
        let mut engine = TimeseriesEngine::new();
        let samples: Vec<MetricSample> = (0..1000)
            .map(|i| MetricSample {
                timestamp_ms: i * 10,
                value: i as f64,
            })
            .collect();
        engine.ingest_batch("metrics", "cpu", vec![], &samples);
        assert_eq!(engine.row_count("metrics"), 1000);
    }
}
