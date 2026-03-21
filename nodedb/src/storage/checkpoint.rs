//! Checkpoint spread / dirty page throttling.
//!
//! Prevents checkpoint storms by tracking dirty page counts per engine
//! and flushing incrementally (configurable % per tick). Rate-limited
//! by the memory governor's I/O budget.
//!
//! Also wires `RecordType::Checkpoint` into the WAL to mark consistent
//! snapshot points for crash recovery.

use std::time::{Duration, Instant};

use tracing::{debug, info};

/// Checkpoint configuration.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Fraction of dirty pages to flush per tick (0.0-1.0).
    /// Default: 0.10 = flush 10% per tick.
    pub flush_fraction: f64,
    /// Minimum interval between checkpoint ticks.
    pub tick_interval: Duration,
    /// Maximum dirty pages before forcing a full flush.
    pub force_flush_threshold: usize,
    /// Maximum I/O bytes per tick (rate limiting).
    pub io_budget_bytes_per_tick: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            flush_fraction: 0.10,
            tick_interval: Duration::from_secs(30),
            io_budget_bytes_per_tick: 64 * 1024 * 1024, // 64 MiB
            force_flush_threshold: 100_000,
        }
    }
}

/// Per-engine dirty page tracking.
#[derive(Debug, Clone)]
pub struct EngineCheckpointState {
    pub engine_name: String,
    pub dirty_pages: usize,
    pub total_flushed: u64,
    pub last_flush: Option<Instant>,
}

impl EngineCheckpointState {
    pub fn new(engine_name: &str) -> Self {
        Self {
            engine_name: engine_name.to_string(),
            dirty_pages: 0,
            total_flushed: 0,
            last_flush: None,
        }
    }

    /// Mark pages as dirty (called on writes).
    pub fn mark_dirty(&mut self, count: usize) {
        self.dirty_pages += count;
    }

    /// Compute how many pages to flush this tick.
    pub fn pages_to_flush(&self, config: &CheckpointConfig) -> usize {
        if self.dirty_pages >= config.force_flush_threshold {
            // Over threshold: flush everything to prevent stalling.
            self.dirty_pages
        } else {
            // Normal: flush a fraction.
            let target = (self.dirty_pages as f64 * config.flush_fraction).ceil() as usize;
            target.max(1).min(self.dirty_pages)
        }
    }

    /// Record that pages were flushed.
    pub fn record_flush(&mut self, count: usize) {
        self.dirty_pages = self.dirty_pages.saturating_sub(count);
        self.total_flushed += count as u64;
        self.last_flush = Some(Instant::now());
    }
}

/// Checkpoint coordinator: manages incremental flushing across engines.
pub struct CheckpointCoordinator {
    config: CheckpointConfig,
    engines: Vec<EngineCheckpointState>,
    last_tick: Option<Instant>,
    /// LSN at the last completed checkpoint.
    checkpoint_lsn: u64,
    /// Total checkpoint cycles completed.
    checkpoint_count: u64,
}

impl CheckpointCoordinator {
    pub fn new(config: CheckpointConfig) -> Self {
        Self {
            config,
            engines: Vec::new(),
            last_tick: None,
            checkpoint_lsn: 0,
            checkpoint_count: 0,
        }
    }

    /// Register an engine for checkpoint tracking.
    pub fn register_engine(&mut self, name: &str) {
        if !self.engines.iter().any(|e| e.engine_name == name) {
            self.engines.push(EngineCheckpointState::new(name));
        }
    }

    /// Mark dirty pages for an engine (called on writes).
    pub fn mark_dirty(&mut self, engine: &str, count: usize) {
        if let Some(state) = self.engines.iter_mut().find(|e| e.engine_name == engine) {
            state.mark_dirty(count);
        }
    }

    /// Execute one checkpoint tick: compute pages to flush per engine.
    ///
    /// Returns `(engine_name, pages_to_flush)` pairs. The caller is
    /// responsible for actually performing the I/O and calling
    /// `record_flush()` after completion.
    ///
    /// Returns empty vec if the tick interval hasn't elapsed or
    /// there are no dirty pages.
    pub fn tick(&mut self) -> Vec<(String, usize)> {
        let now = Instant::now();

        // Respect tick interval.
        if let Some(last) = self.last_tick
            && now.duration_since(last) < self.config.tick_interval
        {
            return Vec::new();
        }
        self.last_tick = Some(now);

        let mut flush_plan = Vec::new();
        let mut budget_remaining = self.config.io_budget_bytes_per_tick;
        // Assume 4 KiB per page for budget calculation.
        let page_size = 4096;

        for engine in &self.engines {
            if engine.dirty_pages == 0 {
                continue;
            }
            let target = engine.pages_to_flush(&self.config);
            let budget_pages = budget_remaining / page_size;
            let actual = target.min(budget_pages);
            if actual > 0 {
                flush_plan.push((engine.engine_name.clone(), actual));
                budget_remaining = budget_remaining.saturating_sub(actual * page_size);
            }
        }

        if !flush_plan.is_empty() {
            debug!(
                engines = flush_plan.len(),
                total_pages = flush_plan.iter().map(|(_, p)| p).sum::<usize>(),
                "checkpoint tick: flushing"
            );
        }

        flush_plan
    }

    /// Record completed flush for an engine.
    pub fn record_flush(&mut self, engine: &str, count: usize) {
        if let Some(state) = self.engines.iter_mut().find(|e| e.engine_name == engine) {
            state.record_flush(count);
        }
    }

    /// Mark a checkpoint as complete at the given LSN.
    ///
    /// The WAL can be safely truncated up to this LSN after
    /// all engines have flushed their dirty pages.
    pub fn complete_checkpoint(&mut self, lsn: u64) {
        self.checkpoint_lsn = lsn;
        self.checkpoint_count += 1;
        info!(lsn, count = self.checkpoint_count, "checkpoint completed");
    }

    /// LSN of the last completed checkpoint.
    /// WAL entries before this LSN are safe to truncate.
    pub fn checkpoint_lsn(&self) -> u64 {
        self.checkpoint_lsn
    }

    /// Whether all engines have zero dirty pages (clean checkpoint).
    pub fn is_clean(&self) -> bool {
        self.engines.iter().all(|e| e.dirty_pages == 0)
    }

    /// Total dirty pages across all engines.
    pub fn total_dirty_pages(&self) -> usize {
        self.engines.iter().map(|e| e.dirty_pages).sum()
    }

    /// Total checkpoint cycles completed.
    pub fn checkpoint_count(&self) -> u64 {
        self.checkpoint_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_flush() {
        let config = CheckpointConfig {
            flush_fraction: 0.10,
            tick_interval: Duration::from_millis(0), // No delay for test.
            ..Default::default()
        };
        let mut coord = CheckpointCoordinator::new(config);
        coord.register_engine("sparse");
        coord.register_engine("vector");

        coord.mark_dirty("sparse", 100);
        coord.mark_dirty("vector", 50);

        let plan = coord.tick();
        assert!(!plan.is_empty());
        // Sparse: 10% of 100 = 10 pages.
        let sparse_flush = plan.iter().find(|(e, _)| e == "sparse").unwrap().1;
        assert_eq!(sparse_flush, 10);

        // Record flush.
        coord.record_flush("sparse", sparse_flush);
        assert_eq!(
            coord
                .engines
                .iter()
                .find(|e| e.engine_name == "sparse")
                .unwrap()
                .dirty_pages,
            90
        );
    }

    #[test]
    fn force_flush_over_threshold() {
        let config = CheckpointConfig {
            force_flush_threshold: 50,
            tick_interval: Duration::from_millis(0),
            ..Default::default()
        };
        let mut coord = CheckpointCoordinator::new(config);
        coord.register_engine("sparse");
        coord.mark_dirty("sparse", 100); // Over threshold.

        let plan = coord.tick();
        let sparse_flush = plan.iter().find(|(e, _)| e == "sparse").unwrap().1;
        assert_eq!(sparse_flush, 100); // Force full flush.
    }

    #[test]
    fn clean_after_all_flushed() {
        let config = CheckpointConfig {
            flush_fraction: 1.0, // Flush everything.
            tick_interval: Duration::from_millis(0),
            ..Default::default()
        };
        let mut coord = CheckpointCoordinator::new(config);
        coord.register_engine("sparse");
        coord.mark_dirty("sparse", 50);

        let plan = coord.tick();
        for (engine, count) in &plan {
            coord.record_flush(engine, *count);
        }
        assert!(coord.is_clean());
    }

    #[test]
    fn checkpoint_lsn_tracking() {
        let mut coord = CheckpointCoordinator::new(CheckpointConfig::default());
        assert_eq!(coord.checkpoint_lsn(), 0);

        coord.complete_checkpoint(42);
        assert_eq!(coord.checkpoint_lsn(), 42);
        assert_eq!(coord.checkpoint_count(), 1);
    }
}
