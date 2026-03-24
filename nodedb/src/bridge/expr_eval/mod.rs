//! Serializable expression tree for Data Plane evaluation.
//!
//! `SqlExpr` is the bridge between DataFusion's `Expr` (Control Plane) and
//! the Data Plane's JSON document evaluation.

mod cast;
mod eval;
mod functions;

pub use eval::{BinaryOp, CastType, ComputedColumn, SqlExpr};
