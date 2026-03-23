//! Top-level format dispatch.

use nodedb_types::result::QueryResult;

use crate::args::OutputFormat;

/// Format a query result according to the output format.
pub fn format_result(qr: &QueryResult, fmt: OutputFormat) -> String {
    match fmt {
        OutputFormat::Table => super::table::format(qr),
        OutputFormat::Json => super::json::format(qr),
        OutputFormat::Csv => super::csv::format(qr),
    }
}
