//! Async mmap pre-fetcher for Data Plane TPC cores.
//!
//! Ensures mmap'd pages are resident before compute touches them.
//! Uses `madvise(MADV_WILLNEED)` for asynchronous page loading — the
//! kernel initiates I/O but the call returns immediately.
//!
//! For io_uring-capable systems, can alternatively use `IORING_OP_READ`
//! to pre-fault pages with explicit completion notification.
//!
//! ## Integration
//!
//! Call `prefetch_pages()` during the planning phase of a query (before
//! the execution touches mmap'd data). The kernel will initiate readahead
//! I/O in the background. By the time execution reaches those pages,
//! they're likely already resident.
//!
//! ## Fault Tracking
//!
//! `FaultCounter` tracks major page faults per core. When the fault rate
//! exceeds a threshold, the core is marked degraded and queries are
//! re-routed to a replica.

use std::sync::atomic::{AtomicU64, Ordering};

/// Pre-fetch a range of pages into the page cache.
///
/// Uses `madvise(MADV_WILLNEED)` which is non-blocking — the kernel
/// initiates readahead I/O and returns immediately. The TPC core
/// continues processing other work while pages load asynchronously.
///
/// # Safety
///
/// `ptr` must point to a valid mmap'd region of at least `len` bytes.
pub fn prefetch_pages(ptr: *const u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    // Align to page boundary.
    let page_size = 4096;
    let aligned_ptr = (ptr as usize & !(page_size - 1)) as *mut libc::c_void;
    let aligned_len = (len + page_size - 1) & !(page_size - 1);

    unsafe {
        libc::madvise(aligned_ptr, aligned_len, libc::MADV_WILLNEED);
    }
}

/// Pre-fetch a batch of disjoint memory ranges.
///
/// Used during BFS planning: prefetch adjacency data for all nodes
/// in the next frontier before the traversal loop touches them.
pub fn prefetch_batch(ranges: &[(*const u8, usize)]) {
    for &(ptr, len) in ranges {
        prefetch_pages(ptr, len);
    }
}

/// Hint the kernel that pages won't be needed soon (reverse of prefetch).
///
/// Used after processing a segment to allow the kernel to reclaim pages.
pub fn advise_dontneed(ptr: *const u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    let page_size = 4096;
    let aligned_ptr = (ptr as usize & !(page_size - 1)) as *mut libc::c_void;
    let aligned_len = (len + page_size - 1) & !(page_size - 1);
    unsafe {
        libc::madvise(aligned_ptr, aligned_len, libc::MADV_DONTNEED);
    }
}

/// Per-core major page fault counter.
///
/// Reads `/proc/self/stat` field 12 (majflt) to track major faults.
/// The difference between consecutive reads gives the fault count
/// for that interval.
///
/// When the fault rate exceeds a configurable threshold, the core
/// should be marked degraded and queries re-routed to replicas.
pub struct FaultCounter {
    /// Accumulated major faults for this core.
    total_faults: AtomicU64,
    /// Faults at last reset (for interval measurement).
    last_snapshot: AtomicU64,
    /// Total prefetch requests issued.
    prefetch_issued: AtomicU64,
    /// Prefetch misses (pages still not resident at access time).
    prefetch_misses: AtomicU64,
    /// Threshold: faults per interval that triggers degraded mode.
    threshold: u64,
    /// Core ID for logging.
    core_id: usize,
}

impl FaultCounter {
    /// Create a new fault counter for a core.
    pub fn new(core_id: usize, threshold: u64) -> Self {
        Self {
            total_faults: AtomicU64::new(0),
            last_snapshot: AtomicU64::new(0),
            prefetch_issued: AtomicU64::new(0),
            prefetch_misses: AtomicU64::new(0),
            threshold,
            core_id,
        }
    }

    /// Record a major page fault.
    pub fn record_fault(&self) {
        self.total_faults.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a prefetch request.
    pub fn record_prefetch(&self) {
        self.prefetch_issued.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a prefetch miss (page wasn't resident when accessed).
    pub fn record_miss(&self) {
        self.prefetch_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if the fault rate exceeds the threshold since last snapshot.
    ///
    /// Returns `true` if the core should enter degraded mode.
    pub fn is_degraded(&self) -> bool {
        let total = self.total_faults.load(Ordering::Relaxed);
        let last = self.last_snapshot.load(Ordering::Relaxed);
        total.saturating_sub(last) >= self.threshold
    }

    /// Take a snapshot (reset interval for threshold check).
    pub fn snapshot(&self) {
        let total = self.total_faults.load(Ordering::Relaxed);
        self.last_snapshot.store(total, Ordering::Relaxed);
    }

    /// Total major faults since creation.
    pub fn total_faults(&self) -> u64 {
        self.total_faults.load(Ordering::Relaxed)
    }

    /// Faults in the current interval (since last snapshot).
    pub fn interval_faults(&self) -> u64 {
        let total = self.total_faults.load(Ordering::Relaxed);
        let last = self.last_snapshot.load(Ordering::Relaxed);
        total.saturating_sub(last)
    }

    /// Prefetch hit rate: `1.0 - (misses / issued)`.
    ///
    /// High miss rate signals that the prefetch predictions are inaccurate
    /// and need recalibration (e.g., increase prefetch depth or change strategy).
    pub fn hit_rate(&self) -> f64 {
        let issued = self.prefetch_issued.load(Ordering::Relaxed);
        let misses = self.prefetch_misses.load(Ordering::Relaxed);
        if issued == 0 {
            1.0
        } else {
            1.0 - (misses as f64 / issued as f64)
        }
    }

    /// Miss rate: `misses / issued`.
    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }

    /// Total prefetch requests issued.
    pub fn prefetch_issued(&self) -> u64 {
        self.prefetch_issued.load(Ordering::Relaxed)
    }

    /// Total prefetch misses.
    pub fn prefetch_misses(&self) -> u64 {
        self.prefetch_misses.load(Ordering::Relaxed)
    }

    /// Core ID.
    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Read the process's major page faults from `/proc/self/stat`.
    ///
    /// Returns `None` on non-Linux or if the file can't be read.
    pub fn read_process_majflt() -> Option<u64> {
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        // Field 12 (0-indexed) is majflt.
        let fields: Vec<&str> = stat.split_whitespace().collect();
        fields.get(11)?.parse::<u64>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_counter_basics() {
        let counter = FaultCounter::new(0, 10);
        assert!(!counter.is_degraded());
        assert_eq!(counter.total_faults(), 0);

        for _ in 0..10 {
            counter.record_fault();
        }
        assert!(counter.is_degraded());
        assert_eq!(counter.total_faults(), 10);

        counter.snapshot();
        assert!(!counter.is_degraded());
        assert_eq!(counter.interval_faults(), 0);
    }

    #[test]
    fn prefetch_hit_rate() {
        let counter = FaultCounter::new(0, 100);
        for _ in 0..100 {
            counter.record_prefetch();
        }
        for _ in 0..10 {
            counter.record_miss();
        }
        let rate = counter.hit_rate();
        assert!((rate - 0.9).abs() < 0.01, "hit_rate: {rate}");
        assert!((counter.miss_rate() - 0.1).abs() < 0.01);
    }

    #[test]
    fn prefetch_pages_null_safe() {
        // Should not crash on null pointer.
        prefetch_pages(std::ptr::null(), 0);
        prefetch_pages(std::ptr::null(), 4096);
    }

    #[test]
    fn read_majflt() {
        // On Linux, this should return Some.
        #[cfg(target_os = "linux")]
        {
            let faults = FaultCounter::read_process_majflt();
            assert!(faults.is_some());
        }
    }
}
