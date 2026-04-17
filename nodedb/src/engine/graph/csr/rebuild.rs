//! Origin-specific CSR rebuild from EdgeStore.
//!
//! This cannot live in the shared crate because it depends on `EdgeStore`
//! (redb-backed persistent edge storage), which is Origin-only.

use nodedb_graph::CsrIndex;
use nodedb_graph::csr::weights::extract_weight_from_properties;

use crate::engine::graph::edge_store::EdgeStore;

/// Rebuild the entire CSR index from an EdgeStore.
///
/// Extracts the `"weight"` property from edge properties (if present)
/// and populates the parallel weight arrays. Edges without a weight
/// property default to 1.0.
pub fn rebuild_from_store(store: &EdgeStore) -> crate::Result<CsrIndex> {
    let mut csr = CsrIndex::new();
    let all_edges = store.scan_all_edges()?;
    for edge in &all_edges {
        csr.add_node(&edge.src_id);
        csr.add_node(&edge.dst_id);
    }
    for edge in &all_edges {
        let weight = extract_weight_from_properties(&edge.properties);
        let res = if weight != 1.0 {
            csr.add_edge_weighted(&edge.src_id, &edge.label, &edge.dst_id, weight)
        } else {
            csr.add_edge(&edge.src_id, &edge.label, &edge.dst_id)
        };
        // A LabelOverflow here means the edge_store has more distinct
        // labels than the CSR can hold — surface it rather than silently
        // drop edges from the rebuilt index.
        res.map_err(|e| crate::Error::Internal {
            detail: format!("CSR rebuild: {e}"),
        })?;
    }
    csr.compact();
    Ok(csr)
}
