//! Vector engine error types.

/// Errors from vector index operations.
#[derive(Debug, thiserror::Error)]
pub enum VectorError {
    #[error("vector dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
}
