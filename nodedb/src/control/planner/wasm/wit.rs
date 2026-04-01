//! WASM Interface Type (WIT) definitions for NodeDB UDFs.
//!
//! Defines the contract between NodeDB and user-supplied WASM modules.
//! Scalar UDFs export a single function. Aggregate UDFs export four functions:
//! `init`, `accumulate`, `merge`, `finalize`.

/// Expected export name for scalar UDFs.
///
/// The WASM module must export a function with this name (or the user-specified
/// function name from CREATE FUNCTION). Parameters and return type must match
/// the SQL declaration.
pub const SCALAR_EXPORT: &str = "invoke";

/// Expected export names for aggregate UDFs.
///
/// ```text
/// init()            → state: i64     Create initial accumulator state
/// accumulate(state: i64, row_val: T) → state: i64   Add a row to the accumulator
/// merge(a: i64, b: i64)              → state: i64   Merge two partial states (for distributed agg)
/// finalize(state: i64)               → result: T    Produce the final aggregate result
/// ```
///
/// State is passed as i64 (opaque handle into WASM linear memory).
/// The actual state layout is internal to the WASM module.
pub const AGG_INIT: &str = "agg_init";
pub const AGG_ACCUMULATE: &str = "agg_accumulate";
pub const AGG_MERGE: &str = "agg_merge";
pub const AGG_FINALIZE: &str = "agg_finalize";

/// Validate that a WASM module exports the required scalar UDF function.
pub fn validate_scalar_exports(module: &wasmtime::Module, func_name: &str) -> crate::Result<()> {
    // Try user-specified name first, then fallback to "invoke".
    let has_named = module
        .exports()
        .any(|e| e.name() == func_name && e.ty().func().is_some());
    let has_invoke = module
        .exports()
        .any(|e| e.name() == SCALAR_EXPORT && e.ty().func().is_some());

    if !has_named && !has_invoke {
        return Err(crate::Error::BadRequest {
            detail: format!("WASM module must export function '{func_name}' or '{SCALAR_EXPORT}'"),
        });
    }
    Ok(())
}

/// Validate that a WASM module exports all four aggregate UDF functions.
pub fn validate_aggregate_exports(module: &wasmtime::Module) -> crate::Result<()> {
    for name in [AGG_INIT, AGG_ACCUMULATE, AGG_MERGE, AGG_FINALIZE] {
        let has_export = module
            .exports()
            .any(|e| e.name() == name && e.ty().func().is_some());
        if !has_export {
            return Err(crate::Error::BadRequest {
                detail: format!("WASM aggregate module must export function '{name}'"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_names_are_consistent() {
        assert_eq!(SCALAR_EXPORT, "invoke");
        assert_eq!(AGG_INIT, "agg_init");
        assert_eq!(AGG_ACCUMULATE, "agg_accumulate");
        assert_eq!(AGG_MERGE, "agg_merge");
        assert_eq!(AGG_FINALIZE, "agg_finalize");
    }
}
