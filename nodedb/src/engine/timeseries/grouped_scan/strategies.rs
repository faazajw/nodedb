//! Tiered grouping strategies: dispatch, direct-index, FxHash, generic,
//! time-bucket, and integer-to-string key resolution.

use rustc_hash::FxHashMap;

use super::super::columnar_agg::AggAccum;
use super::super::columnar_memtable::{ColumnData, ColumnType};
use super::types::{GroupedAggResult, ResolvedSchema, accumulate_row, for_each_set_bit};

const DIRECT_INDEX_MAX_CARDINALITY: u32 = 65536;

/// Integer group key for local (per-source) grouping.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) enum IntGroupKey {
    None,
    SingleU32(u32),
    Multi(Vec<u64>),
}

#[allow(clippy::too_many_arguments)]
pub(super) fn dispatch_grouping<'a>(
    resolved: &ResolvedSchema,
    columns: &[Option<&'a ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
    group_by: &[String],
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
    timestamps: Option<&[i64]>,
    bucket_interval_ms: i64,
) -> GroupedAggResult {
    let has_bucket = bucket_interval_ms > 0 && timestamps.is_some();

    if has_bucket {
        return aggregate_with_bucket(
            resolved,
            columns,
            mask,
            row_count,
            num_aggs,
            timestamps.unwrap(),
            bucket_interval_ms,
            sym_lookup,
        );
    }

    let local_groups = if group_by.is_empty() {
        aggregate_no_group(resolved, columns, mask, row_count, num_aggs)
    } else if group_by.len() == 1 && resolved.group_cols[0].1 == ColumnType::Symbol {
        let (col_idx, _) = resolved.group_cols[0];
        if let Some(data) = columns[col_idx] {
            let sym_ids = data.as_symbols();
            let cardinality = sym_lookup(col_idx).map(|d| d.len() as u32).unwrap_or(0);
            if cardinality <= DIRECT_INDEX_MAX_CARDINALITY && cardinality > 0 {
                aggregate_direct_index(
                    resolved,
                    columns,
                    mask,
                    row_count,
                    num_aggs,
                    sym_ids,
                    cardinality,
                )
            } else {
                aggregate_hash_u32(resolved, columns, mask, row_count, num_aggs, sym_ids)
            }
        } else {
            aggregate_hash_generic(resolved, columns, mask, row_count, num_aggs)
        }
    } else {
        aggregate_hash_generic(resolved, columns, mask, row_count, num_aggs)
    };

    // Resolve integer keys to strings.
    let mut result = GroupedAggResult::new(num_aggs);
    for (int_key, accums) in local_groups {
        let str_key = resolve_group_key(&int_key, resolved, sym_lookup);
        let entry = result
            .groups
            .entry(str_key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        for (i, a) in accums.iter().enumerate() {
            entry[i].merge(a);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Grouping strategies
// ---------------------------------------------------------------------------

fn aggregate_no_group(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let mut accums: Vec<AggAccum> = (0..num_aggs).map(|_| AggAccum::default()).collect();
    for_each_set_bit(mask, row_count, |row_idx| {
        accumulate_row(&mut accums, resolved, columns, row_idx);
    });
    vec![(IntGroupKey::None, accums)]
}

fn aggregate_direct_index(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
    sym_ids: &[u32],
    cardinality: u32,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let card = cardinality as usize;
    let mut table: Vec<Vec<AggAccum>> = (0..card)
        .map(|_| (0..num_aggs).map(|_| AggAccum::default()).collect())
        .collect();

    for_each_set_bit(mask, row_count, |row_idx| {
        let id = sym_ids[row_idx] as usize;
        if id < card {
            accumulate_row(&mut table[id], resolved, columns, row_idx);
        }
    });

    table
        .into_iter()
        .enumerate()
        .filter(|(_, accums)| accums.iter().any(|a| a.count > 0))
        .map(|(id, accums)| (IntGroupKey::SingleU32(id as u32), accums))
        .collect()
}

fn aggregate_hash_u32(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
    sym_ids: &[u32],
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let mut groups: FxHashMap<u32, Vec<AggAccum>> = FxHashMap::default();

    for_each_set_bit(mask, row_count, |row_idx| {
        let id = sym_ids[row_idx];
        let accums = groups
            .entry(id)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        accumulate_row(accums, resolved, columns, row_idx);
    });

    groups
        .into_iter()
        .map(|(id, accums)| (IntGroupKey::SingleU32(id), accums))
        .collect()
}

fn aggregate_hash_generic(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let mut groups: FxHashMap<Vec<u64>, Vec<AggAccum>> = FxHashMap::default();

    for_each_set_bit(mask, row_count, |row_idx| {
        let key = build_generic_key(resolved, columns, row_idx);
        let accums = groups
            .entry(key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        accumulate_row(accums, resolved, columns, row_idx);
    });

    groups
        .into_iter()
        .map(|(key, accums)| (IntGroupKey::Multi(key), accums))
        .collect()
}

/// Time-bucket aggregation with integer keys.
///
/// Packs (bucket_ts, group_key_parts...) into a `Vec<u64>`:
/// - `parts[0]` = bucket_ts as u64
/// - `parts[1..]` = group column values (symbol IDs, i64, f64 bits)
///
/// Resolves to string keys with "bucket_ts\0group1\0group2" format
/// so `emit_grouped_results` can parse them.
#[allow(clippy::too_many_arguments)]
fn aggregate_with_bucket<'a>(
    resolved: &ResolvedSchema,
    columns: &[Option<&'a ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
    timestamps: &[i64],
    bucket_interval_ms: i64,
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
) -> GroupedAggResult {
    let key_len = 1 + resolved.group_cols.len(); // bucket + group columns

    let mut groups: FxHashMap<Vec<u64>, Vec<AggAccum>> = FxHashMap::default();

    for_each_set_bit(mask, row_count, |row_idx| {
        let bucket =
            super::super::time_bucket::time_bucket(bucket_interval_ms, timestamps[row_idx]);

        let mut key = Vec::with_capacity(key_len);
        key.push(bucket as u64);

        // Pack group-by columns as integers.
        for &(col_idx, ty) in &resolved.group_cols {
            let part = columns[col_idx]
                .map(|data| match ty {
                    ColumnType::Symbol => {
                        if let ColumnData::Symbol(ids) = data {
                            ids[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Int64 => {
                        if let ColumnData::Int64(v) = data {
                            v[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Float64 => {
                        if let ColumnData::Float64(v) = data {
                            v[row_idx].to_bits()
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Timestamp => {
                        if let ColumnData::Timestamp(v) = data {
                            v[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                })
                .unwrap_or(u64::MAX);
            key.push(part);
        }

        let accums = groups
            .entry(key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        accumulate_row(accums, resolved, columns, row_idx);
    });

    // Resolve integer keys to string keys: "bucket_ts\0group1\0group2"
    let mut result = GroupedAggResult::new(num_aggs);
    for (key_parts, accums) in groups {
        let bucket_ts = key_parts[0] as i64;
        let mut str_key = bucket_ts.to_string();

        for (i, &part) in key_parts[1..].iter().enumerate() {
            str_key.push('\0');
            if i < resolved.group_cols.len() {
                let (col_idx, ty) = resolved.group_cols[i];
                match ty {
                    ColumnType::Symbol => {
                        if let Some(dict) = sym_lookup(col_idx)
                            && let Some(name) = dict.get(part as u32)
                        {
                            str_key.push_str(name);
                        }
                    }
                    ColumnType::Int64 | ColumnType::Timestamp => {
                        use std::fmt::Write;
                        let _ = write!(str_key, "{}", part as i64);
                    }
                    ColumnType::Float64 => {
                        use std::fmt::Write;
                        let _ = write!(str_key, "{}", f64::from_bits(part));
                    }
                }
            }
        }

        let entry = result
            .groups
            .entry(str_key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        for (i, a) in accums.iter().enumerate() {
            entry[i].merge(a);
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

fn build_generic_key(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    row_idx: usize,
) -> Vec<u64> {
    resolved
        .group_cols
        .iter()
        .map(|&(idx, ty)| {
            columns[idx]
                .map(|data| match ty {
                    ColumnType::Symbol => {
                        if let ColumnData::Symbol(ids) = data {
                            ids[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Int64 => {
                        if let ColumnData::Int64(vals) = data {
                            vals[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Float64 => {
                        if let ColumnData::Float64(vals) = data {
                            vals[row_idx].to_bits()
                        } else {
                            u64::MAX
                        }
                    }
                    ColumnType::Timestamp => {
                        if let ColumnData::Timestamp(vals) = data {
                            vals[row_idx] as u64
                        } else {
                            u64::MAX
                        }
                    }
                })
                .unwrap_or(u64::MAX)
        })
        .collect()
}

fn resolve_group_key<'a>(
    key: &IntGroupKey,
    resolved: &ResolvedSchema,
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
) -> String {
    match key {
        IntGroupKey::None => String::new(),
        IntGroupKey::SingleU32(id) => {
            let col_idx = resolved.group_cols[0].0;
            if resolved.group_cols[0].1 == ColumnType::Symbol {
                sym_lookup(col_idx)
                    .and_then(|d: &nodedb_types::timeseries::SymbolDictionary| d.get(*id))
                    .unwrap_or("")
                    .to_string()
            } else {
                id.to_string()
            }
        }
        IntGroupKey::Multi(parts) => {
            let mut s = String::with_capacity(parts.len() * 16);
            for (i, &part) in parts.iter().enumerate() {
                if i > 0 {
                    s.push('\0');
                }
                let (col_idx, ty) = resolved.group_cols[i];
                match ty {
                    ColumnType::Symbol => {
                        if let Some(dict) = sym_lookup(col_idx)
                            && let Some(name) = dict.get(part as u32)
                        {
                            s.push_str(name);
                        }
                    }
                    ColumnType::Int64 | ColumnType::Timestamp => {
                        use std::fmt::Write;
                        let _ = write!(s, "{}", part as i64);
                    }
                    ColumnType::Float64 => {
                        use std::fmt::Write;
                        let _ = write!(s, "{}", f64::from_bits(part));
                    }
                }
            }
            s
        }
    }
}
