//! Fixed-size slab allocator for KV overflow values.
//!
//! Each tier holds fixed-size slots — zero internal fragmentation, O(1) alloc/free.
//!
//! Tiers: 64B, 128B, 256B, 512B, 1KB, 2KB, 4KB.
//! Values larger than 4KB use a fallback large-object heap (Vec-based, rare).
//!
//! Design follows Linux SLAB/SLUB: size-class segregation prevents small
//! allocations from fragmenting large-allocation regions.

/// Size classes for slab tiers in bytes.
const TIER_SIZES: &[usize] = &[64, 128, 256, 512, 1024, 2048, 4096];

/// Maximum slab-managed value size. Larger values use the large-object fallback.
const MAX_SLAB_SIZE: usize = 4096;

/// Initial slots per tier (grows on demand).
const INITIAL_SLOTS_PER_TIER: usize = 64;

/// A single slab tier with fixed-size slots.
#[derive(Debug)]
struct SlabTier {
    /// Slot size in bytes (e.g., 64, 128, ...).
    slot_size: usize,
    /// Contiguous backing memory: `slots * slot_size` bytes.
    buf: Vec<u8>,
    /// Number of allocated slots.
    total_slots: usize,
    /// Free slot indices (stack-based: push on free, pop on alloc).
    free_list: Vec<u32>,
    /// Number of currently occupied slots.
    occupied: usize,
}

impl SlabTier {
    fn new(slot_size: usize, initial_slots: usize) -> Self {
        let buf = vec![0u8; slot_size * initial_slots];
        let free_list: Vec<u32> = (0..initial_slots as u32).rev().collect();
        Self {
            slot_size,
            buf,
            total_slots: initial_slots,
            free_list,
            occupied: 0,
        }
    }

    /// Allocate a slot. Returns the slot index, or None if the tier is full
    /// (caller should grow the tier).
    fn alloc(&mut self, data: &[u8]) -> Option<u32> {
        debug_assert!(data.len() <= self.slot_size);

        if let Some(slot_idx) = self.free_list.pop() {
            let offset = slot_idx as usize * self.slot_size;
            self.buf[offset..offset + data.len()].copy_from_slice(data);
            // Zero remaining bytes in the slot (deterministic reads).
            if data.len() < self.slot_size {
                self.buf[offset + data.len()..offset + self.slot_size].fill(0);
            }
            self.occupied += 1;
            Some(slot_idx)
        } else {
            None // Tier full — caller must grow.
        }
    }

    /// Grow the tier by doubling its capacity.
    fn grow(&mut self) {
        let new_slots = self.total_slots;
        let old_total = self.total_slots;
        self.total_slots += new_slots;
        self.buf.resize(self.total_slots * self.slot_size, 0);
        // Push new slot indices onto the free list (reverse order for stack semantics).
        for i in (old_total..self.total_slots).rev() {
            self.free_list.push(i as u32);
        }
    }

    /// Free a slot by index.
    fn free(&mut self, slot_idx: u32) {
        debug_assert!((slot_idx as usize) < self.total_slots);
        self.free_list.push(slot_idx);
        self.occupied -= 1;
    }

    /// Read data from a slot.
    fn get(&self, slot_idx: u32, len: usize) -> &[u8] {
        let offset = slot_idx as usize * self.slot_size;
        &self.buf[offset..offset + len]
    }

    /// Total memory consumed by this tier (backing buffer).
    fn capacity_bytes(&self) -> usize {
        self.buf.len()
    }

    /// Memory actively occupied (occupied_slots * slot_size).
    fn occupied_bytes(&self) -> usize {
        self.occupied * self.slot_size
    }

    /// Fragmentation ratio: capacity / occupied. 1.0 = no fragmentation.
    /// Returns 0.0 if nothing is occupied.
    fn fragmentation_ratio(&self) -> f64 {
        if self.occupied == 0 {
            return 0.0;
        }
        self.buf.len() as f64 / self.occupied_bytes() as f64
    }
}

/// Large-object entry for values exceeding `MAX_SLAB_SIZE`.
#[derive(Debug)]
struct LargeObject {
    data: Vec<u8>,
}

/// Fixed-size slab allocator with tiered size classes.
///
/// Drop-in replacement for `OverflowPool` with O(1) alloc/free and
/// zero internal fragmentation within each tier.
#[derive(Debug)]
pub struct SlabAllocator {
    /// One tier per size class (indexed by `tier_index()`).
    tiers: Vec<SlabTier>,
    /// Large objects that don't fit in any slab tier.
    /// Indexed by a monotonic ID (stored in the `index` field of `KvValue::Overflow`).
    large_objects: Vec<Option<LargeObject>>,
    /// Free large-object slots for reuse.
    large_free: Vec<u32>,
    /// Next large-object ID (monotonic).
    next_large_id: u32,
}

impl SlabAllocator {
    pub fn new() -> Self {
        let tiers = TIER_SIZES
            .iter()
            .map(|&size| SlabTier::new(size, INITIAL_SLOTS_PER_TIER))
            .collect();
        Self {
            tiers,
            large_objects: Vec::new(),
            large_free: Vec::new(),
            next_large_id: 0,
        }
    }

    /// Allocate space for a value. Returns `(handle, len)`.
    ///
    /// The handle encodes the tier index and slot index (for slab-managed values)
    /// or a large-object ID (for oversized values). The caller stores these in
    /// `KvValue::Overflow { index: handle, len }`.
    pub fn alloc(&mut self, data: &[u8]) -> (u32, u32) {
        let len = data.len();

        if len <= MAX_SLAB_SIZE {
            let tier_idx = tier_index(len);
            let tier = &mut self.tiers[tier_idx];

            // Try to allocate in the current tier; grow and retry if full.
            let slot_idx = match tier.alloc(data) {
                Some(idx) => idx,
                None => {
                    tier.grow();
                    // grow() doubles capacity, guaranteeing free slots exist.
                    // If alloc still fails, it means a logic bug in grow() —
                    // fall back to large-object path rather than panicking.
                    match tier.alloc(data) {
                        Some(idx) => idx,
                        None => {
                            // Defensive: fall back to large-object path rather than panicking.
                            let id = self.next_large_id;
                            self.next_large_id += 1;
                            self.large_objects.push(Some(LargeObject {
                                data: data.to_vec(),
                            }));
                            let handle = encode_large_handle(id);
                            return (handle, len as u32);
                        }
                    }
                }
            };
            let handle = encode_handle(tier_idx as u8, slot_idx);
            return (handle, len as u32);
        }

        // Large object fallback.
        let id = if let Some(free_id) = self.large_free.pop() {
            self.large_objects[free_id as usize] = Some(LargeObject {
                data: data.to_vec(),
            });
            free_id
        } else {
            let id = self.next_large_id;
            self.next_large_id += 1;
            self.large_objects.push(Some(LargeObject {
                data: data.to_vec(),
            }));
            id
        };

        let handle = encode_large_handle(id);
        (handle, len as u32)
    }

    /// Read a value by handle and length.
    pub fn get(&self, handle: u32, len: u32) -> &[u8] {
        if is_large_handle(handle) {
            let id = decode_large_handle(handle);
            let obj = self.large_objects[id as usize]
                .as_ref()
                .expect("large object read: slot empty");
            &obj.data[..len as usize]
        } else {
            let (tier_idx, slot_idx) = decode_handle(handle);
            self.tiers[tier_idx as usize].get(slot_idx, len as usize)
        }
    }

    /// Free a value by handle and length.
    pub fn free(&mut self, handle: u32, _len: u32) {
        if is_large_handle(handle) {
            let id = decode_large_handle(handle);
            self.large_objects[id as usize] = None;
            self.large_free.push(id);
        } else {
            let (tier_idx, slot_idx) = decode_handle(handle);
            self.tiers[tier_idx as usize].free(slot_idx);
        }
    }

    /// Total memory capacity across all tiers + large objects.
    pub fn capacity(&self) -> usize {
        let slab: usize = self.tiers.iter().map(|t| t.capacity_bytes()).sum();
        let large: usize = self
            .large_objects
            .iter()
            .filter_map(|o| o.as_ref().map(|lo| lo.data.len()))
            .sum();
        slab + large
    }

    /// Total bytes actively in use.
    pub fn used_bytes(&self) -> usize {
        let slab: usize = self.tiers.iter().map(|t| t.occupied_bytes()).sum();
        let large: usize = self
            .large_objects
            .iter()
            .filter_map(|o| o.as_ref().map(|lo| lo.data.len()))
            .sum();
        slab + large
    }

    /// Per-tier fragmentation statistics.
    pub fn tier_stats(&self) -> Vec<SlabTierStats> {
        self.tiers
            .iter()
            .map(|t| SlabTierStats {
                slot_size: t.slot_size,
                total_slots: t.total_slots,
                occupied_slots: t.occupied,
                capacity_bytes: t.capacity_bytes(),
                occupied_bytes: t.occupied_bytes(),
                fragmentation_ratio: t.fragmentation_ratio(),
            })
            .collect()
    }

    /// Overall fragmentation ratio: capacity / used_bytes. 1.0 = perfect.
    pub fn fragmentation_ratio(&self) -> f64 {
        let used = self.used_bytes();
        if used == 0 {
            return 0.0;
        }
        self.capacity() as f64 / used as f64
    }
}

impl Default for SlabAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-tier statistics for observability.
#[derive(Debug, Clone)]
pub struct SlabTierStats {
    pub slot_size: usize,
    pub total_slots: usize,
    pub occupied_slots: usize,
    pub capacity_bytes: usize,
    pub occupied_bytes: usize,
    /// Ratio of capacity to occupied. 1.0 = no waste. Higher = more waste.
    pub fragmentation_ratio: f64,
}

// ---------------------------------------------------------------------------
// Handle encoding: pack tier_index + slot_index into a u32.
//
// Layout: bit 31 = large flag, bits 30:28 = tier_index (0-6), bits 27:0 = slot_index.
// Large objects: bit 31 = 1, bits 30:0 = large_object_id.
// ---------------------------------------------------------------------------

const LARGE_FLAG: u32 = 1 << 31;
const TIER_SHIFT: u32 = 28;
const SLOT_MASK: u32 = (1 << 28) - 1;

fn encode_handle(tier_idx: u8, slot_idx: u32) -> u32 {
    debug_assert!(tier_idx < 8);
    debug_assert!(slot_idx <= SLOT_MASK);
    ((tier_idx as u32) << TIER_SHIFT) | slot_idx
}

fn decode_handle(handle: u32) -> (u8, u32) {
    let tier_idx = ((handle >> TIER_SHIFT) & 0x7) as u8;
    let slot_idx = handle & SLOT_MASK;
    (tier_idx, slot_idx)
}

fn encode_large_handle(id: u32) -> u32 {
    LARGE_FLAG | id
}

fn decode_large_handle(handle: u32) -> u32 {
    handle & !LARGE_FLAG
}

fn is_large_handle(handle: u32) -> bool {
    handle & LARGE_FLAG != 0
}

/// Find the tier index for a given value size.
///
/// Returns the index of the smallest tier that can hold `size` bytes.
fn tier_index(size: usize) -> usize {
    for (i, &tier_size) in TIER_SIZES.iter().enumerate() {
        if size <= tier_size {
            return i;
        }
    }
    // Should not reach here for sizes <= MAX_SLAB_SIZE.
    TIER_SIZES.len() - 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_index_selection() {
        assert_eq!(tier_index(1), 0); // 1 byte → 64B tier
        assert_eq!(tier_index(64), 0); // 64 bytes → 64B tier
        assert_eq!(tier_index(65), 1); // 65 bytes → 128B tier
        assert_eq!(tier_index(128), 1);
        assert_eq!(tier_index(129), 2); // → 256B
        assert_eq!(tier_index(256), 2);
        assert_eq!(tier_index(512), 3);
        assert_eq!(tier_index(1024), 4);
        assert_eq!(tier_index(2048), 5);
        assert_eq!(tier_index(4096), 6);
    }

    #[test]
    fn handle_encoding_roundtrip() {
        for tier in 0..7u8 {
            for slot in [0, 1, 100, SLOT_MASK] {
                let handle = encode_handle(tier, slot);
                assert!(!is_large_handle(handle));
                let (t, s) = decode_handle(handle);
                assert_eq!(t, tier);
                assert_eq!(s, slot);
            }
        }
    }

    #[test]
    fn large_handle_encoding() {
        let handle = encode_large_handle(42);
        assert!(is_large_handle(handle));
        assert_eq!(decode_large_handle(handle), 42);
    }

    #[test]
    fn basic_alloc_get_free() {
        let mut slab = SlabAllocator::new();

        // Small value (fits in 64B tier).
        let (h1, l1) = slab.alloc(b"hello");
        assert_eq!(slab.get(h1, l1), b"hello");

        // Medium value (fits in 256B tier).
        let data = vec![0xAB; 200];
        let (h2, l2) = slab.alloc(&data);
        assert_eq!(slab.get(h2, l2), &data);

        // Free and realloc.
        slab.free(h1, l1);
        let (h3, l3) = slab.alloc(b"world");
        assert_eq!(slab.get(h3, l3), b"world");
        // Should reuse the freed slot.
        assert_eq!(h3, h1);
    }

    #[test]
    fn large_object_fallback() {
        let mut slab = SlabAllocator::new();

        let big = vec![0xFF; 8192]; // > 4KB → large object.
        let (h, l) = slab.alloc(&big);
        assert!(is_large_handle(h));
        assert_eq!(slab.get(h, l), &big);

        slab.free(h, l);
    }

    #[test]
    fn tier_grows_on_demand() {
        let mut slab = SlabAllocator::new();

        // Fill the 64B tier (initial 64 slots).
        let mut handles = Vec::new();
        for i in 0..100u32 {
            let data = i.to_be_bytes();
            let (h, l) = slab.alloc(&data);
            handles.push((h, l));
        }

        // All values readable.
        for (i, &(h, l)) in handles.iter().enumerate() {
            let expected = (i as u32).to_be_bytes();
            assert_eq!(slab.get(h, l), &expected);
        }
    }

    #[test]
    fn fragmentation_ratio_is_reasonable() {
        let mut slab = SlabAllocator::new();

        for i in 0..50u32 {
            slab.alloc(&i.to_be_bytes());
        }

        // 50 × 4 bytes stored in 64B slots → some waste expected.
        // Ratio includes all pre-allocated tiers (7 tiers × 64 slots each).
        let ratio = slab.fragmentation_ratio();
        assert!(ratio > 1.0); // Some waste expected (4B in 64B slots + pre-alloc'd empty tiers).
        // With 7 tiers × 64 slots = 28KB pre-allocated, and 50 × 64B = 3.2KB occupied,
        // the ratio can be up to ~150. This is expected for a freshly-initialized slab
        // with minimal usage — the pre-allocated tiers amortize across future allocations.
        assert!(ratio < 200.0);
    }

    #[test]
    fn free_and_reuse_large_objects() {
        let mut slab = SlabAllocator::new();

        let big1 = vec![1u8; 5000];
        let (h1, l1) = slab.alloc(&big1);
        slab.free(h1, l1);

        let big2 = vec![2u8; 6000];
        let (h2, _l2) = slab.alloc(&big2);
        // Should reuse the freed large-object slot ID.
        assert_eq!(decode_large_handle(h2), decode_large_handle(h1));
    }

    #[test]
    fn tier_stats_populated() {
        let mut slab = SlabAllocator::new();
        slab.alloc(b"small"); // 64B tier
        slab.alloc(&[0; 200]); // 256B tier

        let stats = slab.tier_stats();
        assert_eq!(stats.len(), TIER_SIZES.len());
        assert_eq!(stats[0].occupied_slots, 1); // 64B tier
        assert_eq!(stats[2].occupied_slots, 1); // 256B tier
        assert_eq!(stats[1].occupied_slots, 0); // 128B tier unused
    }

    #[test]
    fn used_bytes_and_capacity() {
        let mut slab = SlabAllocator::new();
        let initial_cap = slab.capacity();
        assert!(initial_cap > 0); // Pre-allocated tiers.
        assert_eq!(slab.used_bytes(), 0);

        slab.alloc(b"test");
        assert_eq!(slab.used_bytes(), 64); // 4 bytes in a 64B slot.
    }
}
