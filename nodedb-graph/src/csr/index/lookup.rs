//! Read-side queries: neighbor lookup, counters, degree, iterators,
//! dense-array helpers, and the `add_node` / `build_dense` utilities.

use super::types::{CsrIndex, Direction};

impl CsrIndex {
    /// Get immediate neighbors.
    pub fn neighbors(
        &self,
        node: &str,
        label_filter: Option<&str>,
        direction: Direction,
    ) -> Vec<(String, String)> {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return Vec::new();
        };
        self.record_access(node_id);
        let label_id = label_filter.and_then(|l| self.label_to_id.get(l).copied());

        let mut result = Vec::new();

        if matches!(direction, Direction::Out | Direction::Both) {
            for (lid, dst) in self.iter_out_edges(node_id) {
                if label_id.is_none_or(|f| f == lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[dst as usize].clone(),
                    ));
                }
            }
        }
        if matches!(direction, Direction::In | Direction::Both) {
            for (lid, src) in self.iter_in_edges(node_id) {
                if label_id.is_none_or(|f| f == lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[src as usize].clone(),
                    ));
                }
            }
        }

        result
    }

    /// Get neighbors with multi-label filter. Empty labels = all edges.
    pub fn neighbors_multi(
        &self,
        node: &str,
        label_filters: &[&str],
        direction: Direction,
    ) -> Vec<(String, String)> {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return Vec::new();
        };
        self.record_access(node_id);
        let label_ids: Vec<u32> = label_filters
            .iter()
            .filter_map(|l| self.label_to_id.get(*l).copied())
            .collect();
        let match_label = |lid: u32| label_ids.is_empty() || label_ids.contains(&lid);

        let mut result = Vec::new();

        if matches!(direction, Direction::Out | Direction::Both) {
            for (lid, dst) in self.iter_out_edges(node_id) {
                if match_label(lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[dst as usize].clone(),
                    ));
                }
            }
        }
        if matches!(direction, Direction::In | Direction::Both) {
            for (lid, src) in self.iter_in_edges(node_id) {
                if match_label(lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[src as usize].clone(),
                    ));
                }
            }
        }

        result
    }

    /// Add a node without any edges (used for isolated/dangling nodes).
    /// Returns the dense node ID. Idempotent — returns existing ID if present.
    pub fn add_node(&mut self, name: &str) -> u32 {
        self.ensure_node(name)
    }

    pub fn node_count(&self) -> usize {
        self.id_to_node.len()
    }

    pub fn contains_node(&self, node: &str) -> bool {
        self.node_to_id.contains_key(node)
    }

    /// Get the string node ID for a dense node index.
    pub fn node_name(&self, dense_id: u32) -> &str {
        &self.id_to_node[dense_id as usize]
    }

    /// Look up the dense node ID for a string node ID.
    pub fn node_id(&self, name: &str) -> Option<u32> {
        self.node_to_id.get(name).copied()
    }

    /// Get the string label for a dense label index.
    pub fn label_name(&self, label_id: u32) -> &str {
        &self.id_to_label[label_id as usize]
    }

    /// Look up the dense label ID for a string label.
    pub fn label_id(&self, name: &str) -> Option<u32> {
        self.label_to_id.get(name).copied()
    }

    /// Out-degree of a node (including buffer, excluding deleted).
    pub fn out_degree(&self, node_id: u32) -> usize {
        self.iter_out_edges(node_id).count()
    }

    /// In-degree of a node.
    pub fn in_degree(&self, node_id: u32) -> usize {
        self.iter_in_edges(node_id).count()
    }

    /// Total edge count (dense + buffer - deleted). O(V).
    pub fn edge_count(&self) -> usize {
        let n = self.id_to_node.len();
        (0..n).map(|i| self.out_degree(i as u32)).sum()
    }

    // ── Internal helpers ──

    /// Build contiguous offset/target/label arrays from per-node edge lists.
    pub(crate) fn build_dense(edges: &[Vec<(u32, u32)>]) -> (Vec<u32>, Vec<u32>, Vec<u32>) {
        let n = edges.len();
        let total: usize = edges.iter().map(|e| e.len()).sum();
        let mut offsets = Vec::with_capacity(n + 1);
        let mut targets = Vec::with_capacity(total);
        let mut labels = Vec::with_capacity(total);

        let mut offset = 0u32;
        for node_edges in edges {
            offsets.push(offset);
            for &(lid, target) in node_edges {
                targets.push(target);
                labels.push(lid);
            }
            offset += node_edges.len() as u32;
        }
        offsets.push(offset);

        (offsets, targets, labels)
    }

    /// Check if a specific edge exists in the dense CSR.
    pub(crate) fn dense_has_edge(&self, src: u32, label: u32, dst: u32) -> bool {
        for (lid, target) in self.dense_out_edges(src) {
            if lid == label && target == dst {
                return true;
            }
        }
        false
    }

    /// Iterate dense outbound edges for a node.
    pub(crate) fn dense_out_edges(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let idx = node as usize;
        if idx + 1 >= self.out_offsets.len() {
            return Vec::new().into_iter();
        }
        let start = self.out_offsets[idx] as usize;
        let end = self.out_offsets[idx + 1] as usize;
        (start..end)
            .map(move |i| (self.out_labels[i], self.out_targets[i]))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Iterate dense inbound edges for a node.
    pub(crate) fn dense_in_edges(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let idx = node as usize;
        if idx + 1 >= self.in_offsets.len() {
            return Vec::new().into_iter();
        }
        let start = self.in_offsets[idx] as usize;
        let end = self.in_offsets[idx + 1] as usize;
        (start..end)
            .map(move |i| (self.in_labels[i], self.in_targets[i]))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Iterate all outbound edges for a node (dense + buffer, minus deleted).
    pub fn iter_out_edges(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let idx = node as usize;
        let dense = self
            .dense_out_edges(node)
            .filter(move |&(lid, dst)| !self.deleted_edges.contains(&(node, lid, dst)));
        let buffer = if idx < self.buffer_out.len() {
            self.buffer_out[idx].to_vec()
        } else {
            Vec::new()
        };
        dense.chain(buffer)
    }

    /// Iterate all inbound edges for a node (dense + buffer, minus deleted).
    pub fn iter_in_edges(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let idx = node as usize;
        let dense = self
            .dense_in_edges(node)
            .filter(move |&(lid, src)| !self.deleted_edges.contains(&(src, lid, node)));
        let buffer = if idx < self.buffer_in.len() {
            self.buffer_in[idx].to_vec()
        } else {
            Vec::new()
        };
        dense.chain(buffer)
    }
}
