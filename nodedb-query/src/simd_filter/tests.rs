use super::{
    bitmask_all, bitmask_and, bitmask_not, bitmask_or, bitmask_to_indices, filter_runtime, popcount,
};

#[test]
fn runtime_detects() {
    let rt = filter_runtime();
    assert!(!rt.name.is_empty());
}

#[test]
fn eq_u32_basic() {
    let rt = filter_runtime();
    let values: Vec<u32> = (0..100).collect();
    let mask = (rt.eq_u32)(&values, 42);
    assert_eq!(popcount(&mask), 1);
    let indices = bitmask_to_indices(&mask);
    assert_eq!(indices, vec![42]);
}

#[test]
fn ne_u32_basic() {
    let rt = filter_runtime();
    let values: Vec<u32> = (0..100).collect();
    let mask = (rt.ne_u32)(&values, 42);
    assert_eq!(popcount(&mask), 99);
}

#[test]
fn eq_u32_repeated() {
    let rt = filter_runtime();
    // 1000 values cycling through 0..8.
    let values: Vec<u32> = (0..1000).map(|i| (i % 8) as u32).collect();
    let mask = (rt.eq_u32)(&values, 3);
    assert_eq!(popcount(&mask), 125); // 1000/8
    let indices = bitmask_to_indices(&mask);
    assert!(indices.iter().all(|&i| values[i as usize] == 3));
}

#[test]
fn gt_f64_basic() {
    let rt = filter_runtime();
    let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
    let mask = (rt.gt_f64)(&values, 500.0);
    assert_eq!(popcount(&mask), 499); // 501..999
}

#[test]
fn gte_f64_basic() {
    let rt = filter_runtime();
    let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
    let mask = (rt.gte_f64)(&values, 500.0);
    assert_eq!(popcount(&mask), 500); // 500..999
}

#[test]
fn range_i64_basic() {
    let rt = filter_runtime();
    let values: Vec<i64> = (0..1000).collect();
    let mask = (rt.range_i64)(&values, 100, 200);
    assert_eq!(popcount(&mask), 101); // 100..=200
}

#[test]
fn bitmask_and_works() {
    let a = vec![0b1111_0000u64];
    let b = vec![0b1010_1010u64];
    let c = bitmask_and(&a, &b);
    assert_eq!(c, vec![0b1010_0000u64]);
}

#[test]
fn bitmask_or_works() {
    let a = vec![0b1111_0000u64];
    let b = vec![0b0000_1111u64];
    let c = bitmask_or(&a, &b);
    assert_eq!(c, vec![0b1111_1111u64]);
}

#[test]
fn bitmask_not_works() {
    let mask = vec![0u64];
    let inv = bitmask_not(&mask, 10);
    assert_eq!(popcount(&inv), 10);
}

#[test]
fn bitmask_all_works() {
    let mask = bitmask_all(100);
    assert_eq!(popcount(&mask), 100);
    // Should have exactly 2 words (64 + 36 bits).
    assert_eq!(mask.len(), 2);
}

#[test]
fn bitmask_to_indices_works() {
    let mask = vec![0b1010_0101u64];
    let indices = bitmask_to_indices(&mask);
    assert_eq!(indices, vec![0, 2, 5, 7]);
}

#[test]
fn popcount_works() {
    assert_eq!(popcount(&[u64::MAX, u64::MAX]), 128);
    assert_eq!(popcount(&[0, 0]), 0);
    assert_eq!(popcount(&[1]), 1);
}

#[test]
fn empty_input() {
    let rt = filter_runtime();
    assert!(popcount(&(rt.eq_u32)(&[], 0)) == 0);
    assert!(popcount(&(rt.gt_f64)(&[], 0.0)) == 0);
    assert!(popcount(&(rt.range_i64)(&[], 0, 100)) == 0);
}

#[test]
fn large_input_eq_u32() {
    let rt = filter_runtime();
    let n: u32 = 10_000;
    let values: Vec<u32> = (0..n).map(|i| i % 256).collect();
    let mask = (rt.eq_u32)(&values, 0);
    // 0, 256, 512, ... → ceil(n/256) occurrences.
    let expected = values.iter().filter(|&&v| v == 0).count() as u64;
    assert_eq!(popcount(&mask), expected);
    let indices = bitmask_to_indices(&mask);
    assert!(indices.iter().all(|&i| values[i as usize] == 0));
}

#[test]
fn i64_comparisons() {
    let rt = filter_runtime();
    let values: Vec<i64> = (0..100).collect();

    assert_eq!(popcount(&(rt.gt_i64)(&values, 50)), 49);
    assert_eq!(popcount(&(rt.gte_i64)(&values, 50)), 50);
    assert_eq!(popcount(&(rt.lt_i64)(&values, 50)), 50);
    assert_eq!(popcount(&(rt.lte_i64)(&values, 50)), 51);
}
