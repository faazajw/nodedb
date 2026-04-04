//! Gap-fill for bucketed timeseries aggregation results.
//!
//! After time-bucket GROUP BY, some buckets may be missing (no data for that
//! interval). Gap-fill generates synthetic rows for those empty buckets using
//! one of several strategies: NULL, PREV (LOCF), LINEAR, or LITERAL.
//!
//! Operates at query time only — no storage writes.

use super::columnar_agg::AggResult;

/// Fill strategy for missing time buckets.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GapFillStrategy {
    /// Fill with a NULL/zero aggregate (count=0).
    Null,
    /// Carry forward the last known value (Last Observation Carried Forward).
    Prev,
    /// Linear interpolation between surrounding real values.
    Linear,
    /// Fill with a constant value.
    Literal(f64),
}

impl GapFillStrategy {
    /// Parse a strategy from a SQL string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "null" => Some(Self::Null),
            "prev" | "locf" | "previous" => Some(Self::Prev),
            "linear" | "interpolate" => Some(Self::Linear),
            _ => {
                // Try to parse as a literal f64.
                s.parse::<f64>().ok().map(Self::Literal)
            }
        }
    }
}

/// Apply gap-fill to bucketed aggregation results.
///
/// Given `buckets` sorted by timestamp, `start_ms` and `end_ms` defining the
/// full query time range, and `interval_ms` defining the bucket width:
///
/// 1. Enumerate all expected bucket timestamps in `[start_ms, end_ms)`.
/// 2. For each missing bucket, generate a synthetic `AggResult` using `strategy`.
/// 3. Return the complete sequence (real + synthetic) sorted by timestamp.
pub fn gap_fill_buckets(
    buckets: &[(i64, AggResult)],
    start_ms: i64,
    end_ms: i64,
    interval_ms: i64,
    strategy: GapFillStrategy,
) -> Vec<(i64, AggResult)> {
    if interval_ms <= 0 || start_ms >= end_ms {
        return buckets.to_vec();
    }

    // Build a lookup of existing buckets.
    let existing: std::collections::HashMap<i64, &AggResult> =
        buckets.iter().map(|(ts, agg)| (*ts, agg)).collect();

    // Enumerate all expected bucket timestamps.
    let mut result = Vec::new();
    let mut ts = align_bucket(start_ms, interval_ms);
    while ts < end_ms {
        if let Some(agg) = existing.get(&ts) {
            result.push((ts, (*agg).clone()));
        } else {
            // Missing bucket — apply fill strategy.
            let filled = match strategy {
                GapFillStrategy::Null => null_agg(),
                GapFillStrategy::Literal(val) => literal_agg(val),
                GapFillStrategy::Prev => prev_fill(&result),
                GapFillStrategy::Linear => linear_fill(ts, &result, &existing, interval_ms, end_ms),
            };
            result.push((ts, filled));
        }
        ts += interval_ms;
    }

    result
}

/// Align a timestamp to the nearest bucket boundary at or before it.
fn align_bucket(ts: i64, interval_ms: i64) -> i64 {
    ts - (ts.rem_euclid(interval_ms))
}

/// Create a null/zero aggregate for missing buckets.
fn null_agg() -> AggResult {
    AggResult {
        count: 0,
        sum: 0.0,
        min: f64::NAN,
        max: f64::NAN,
        first: f64::NAN,
        last: f64::NAN,
    }
}

/// Create a literal-filled aggregate.
fn literal_agg(val: f64) -> AggResult {
    AggResult {
        count: 1,
        sum: val,
        min: val,
        max: val,
        first: val,
        last: val,
    }
}

/// LOCF: carry forward the last real value.
fn prev_fill(preceding: &[(i64, AggResult)]) -> AggResult {
    // Find the last real (non-zero-count) entry.
    for (_, agg) in preceding.iter().rev() {
        if agg.count > 0 {
            return agg.clone();
        }
    }
    // No prior real value: return null.
    null_agg()
}

/// Linear interpolation between surrounding real values.
fn linear_fill(
    target_ts: i64,
    preceding: &[(i64, AggResult)],
    existing: &std::collections::HashMap<i64, &AggResult>,
    interval_ms: i64,
    end_ms: i64,
) -> AggResult {
    // Find last real value before target.
    let prev = preceding.iter().rev().find(|(_, a)| a.count > 0);

    // Find next real value after target.
    let mut next_ts = target_ts + interval_ms;
    let mut next: Option<(i64, &AggResult)> = None;
    while next_ts < end_ms {
        if let Some(agg) = existing.get(&next_ts) {
            next = Some((next_ts, agg));
            break;
        }
        next_ts += interval_ms;
    }

    match (prev, next) {
        (Some((prev_ts, prev_agg)), Some((next_ts, next_agg))) => {
            let t = (target_ts - prev_ts) as f64 / (next_ts - prev_ts) as f64;
            let avg_prev = if prev_agg.count > 0 {
                prev_agg.sum / prev_agg.count as f64
            } else {
                0.0
            };
            let avg_next = if next_agg.count > 0 {
                next_agg.sum / next_agg.count as f64
            } else {
                0.0
            };
            let interpolated = avg_prev + t * (avg_next - avg_prev);
            literal_agg(interpolated)
        }
        (Some((_, prev_agg)), None) => {
            // No next value: flat extrapolation from prev.
            prev_agg.clone()
        }
        (None, Some((_, next_agg))) => {
            // No prev value: flat extrapolation from next.
            (*next_agg).clone()
        }
        (None, None) => null_agg(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agg(val: f64) -> AggResult {
        AggResult {
            count: 1,
            sum: val,
            min: val,
            max: val,
            first: val,
            last: val,
        }
    }

    #[test]
    fn no_gaps() {
        let buckets = vec![(0, agg(1.0)), (100, agg(2.0)), (200, agg(3.0))];
        let filled = gap_fill_buckets(&buckets, 0, 300, 100, GapFillStrategy::Null);
        assert_eq!(filled.len(), 3);
        assert_eq!(filled[0].1.sum, 1.0);
        assert_eq!(filled[2].1.sum, 3.0);
    }

    #[test]
    fn null_fill() {
        let buckets = vec![(0, agg(1.0)), (200, agg(3.0))];
        let filled = gap_fill_buckets(&buckets, 0, 300, 100, GapFillStrategy::Null);
        assert_eq!(filled.len(), 3);
        assert_eq!(filled[1].0, 100); // Gap bucket
        assert_eq!(filled[1].1.count, 0); // Null-filled
        assert!(filled[1].1.min.is_nan());
    }

    #[test]
    fn prev_fill_strategy() {
        let buckets = vec![(0, agg(10.0)), (200, agg(30.0))];
        let filled = gap_fill_buckets(&buckets, 0, 300, 100, GapFillStrategy::Prev);
        assert_eq!(filled.len(), 3);
        assert_eq!(filled[1].1.sum, 10.0); // Carried forward from bucket 0
    }

    #[test]
    fn linear_fill_strategy() {
        let buckets = vec![(0, agg(10.0)), (200, agg(30.0))];
        let filled = gap_fill_buckets(&buckets, 0, 300, 100, GapFillStrategy::Linear);
        assert_eq!(filled.len(), 3);
        // Linear interpolation: 10 + 0.5 * (30 - 10) = 20
        assert!((filled[1].1.sum - 20.0).abs() < 1e-10);
    }

    #[test]
    fn literal_fill() {
        let buckets = vec![(0, agg(1.0)), (200, agg(3.0))];
        let filled = gap_fill_buckets(&buckets, 0, 300, 100, GapFillStrategy::Literal(99.0));
        assert_eq!(filled[1].1.sum, 99.0);
    }

    #[test]
    fn leading_gap_prev() {
        // No data at start.
        let buckets = vec![(200, agg(5.0))];
        let filled = gap_fill_buckets(&buckets, 0, 300, 100, GapFillStrategy::Prev);
        assert_eq!(filled.len(), 3);
        // Leading gaps with Prev: no prior value, should be null.
        assert_eq!(filled[0].1.count, 0);
        assert_eq!(filled[1].1.count, 0);
        assert_eq!(filled[2].1.sum, 5.0);
    }

    #[test]
    fn trailing_gap_linear() {
        // No data at end.
        let buckets = vec![(0, agg(10.0))];
        let filled = gap_fill_buckets(&buckets, 0, 300, 100, GapFillStrategy::Linear);
        assert_eq!(filled.len(), 3);
        // Trailing gaps with Linear: flat extrapolation from last.
        assert_eq!(filled[1].1.sum, 10.0);
        assert_eq!(filled[2].1.sum, 10.0);
    }

    #[test]
    fn parse_strategies() {
        assert_eq!(GapFillStrategy::parse("null"), Some(GapFillStrategy::Null));
        assert_eq!(GapFillStrategy::parse("prev"), Some(GapFillStrategy::Prev));
        assert_eq!(GapFillStrategy::parse("LOCF"), Some(GapFillStrategy::Prev));
        assert_eq!(
            GapFillStrategy::parse("linear"),
            Some(GapFillStrategy::Linear)
        );
        assert_eq!(
            GapFillStrategy::parse("42.5"),
            Some(GapFillStrategy::Literal(42.5))
        );
        assert_eq!(GapFillStrategy::parse("not_a_strategy"), None);
    }

    #[test]
    fn align_bucket_boundary() {
        assert_eq!(align_bucket(150, 100), 100);
        assert_eq!(align_bucket(200, 100), 200);
        assert_eq!(align_bucket(0, 100), 0);
    }
}
