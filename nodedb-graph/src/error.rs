//! Typed error enum for the shared graph engine.
//!
//! Every fallible operation on `CsrIndex` (label interning, edge insert,
//! edge delete) returns `Result<T, GraphError>`. The skill / CLAUDE.md
//! discipline is explicit: silent casts or `debug_assert!` at capacity
//! boundaries reproduce the same class of bug as the one being fixed —
//! loud, typed errors only.

use thiserror::Error;

/// Hard upper bound on the number of distinct edge labels an individual
/// `CsrIndex` can intern. `u32::MAX` is the type-theoretic ceiling;
/// leaving one slot unused lets callers use `u32::MAX` as an "invalid"
/// sentinel should they need it.
pub const MAX_EDGE_LABELS: usize = (u32::MAX - 1) as usize;

/// Errors returned by graph-engine operations.
#[derive(Debug, Error)]
pub enum GraphError {
    /// The CSR's edge-label id space is exhausted. Happens only when
    /// more than `MAX_EDGE_LABELS` distinct labels have been interned
    /// — in practice unreachable because the DSL ingress caps label
    /// length and realistic workloads use orders of magnitude fewer
    /// labels. Surfaced here so the failure mode is a typed error, not
    /// a silent wrap (the bug this crate was shipping before).
    #[error("CSR edge-label id space exhausted ({used}/{MAX_EDGE_LABELS} labels interned)")]
    LabelOverflow { used: usize },
}
