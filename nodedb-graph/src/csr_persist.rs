//! CSR checkpoint serialization/deserialization and compaction.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::csr::CsrIndex;

#[derive(Serialize, Deserialize)]
struct CsrSnapshot {
    nodes: Vec<String>,
    labels: Vec<String>,
    out_offsets: Vec<u32>,
    out_targets: Vec<u32>,
    out_labels: Vec<u16>,
    in_offsets: Vec<u32>,
    in_targets: Vec<u32>,
    in_labels: Vec<u16>,
    buffer_out: Vec<Vec<(u16, u32)>>,
    buffer_in: Vec<Vec<(u16, u32)>>,
    deleted: Vec<(u32, u16, u32)>,
}

impl CsrIndex {
    /// Merge the mutable buffer into dense CSR arrays.
    pub fn compact(&mut self) {
        let n = self.id_to_node.len();
        let mut new_out_edges: Vec<Vec<(u16, u32)>> = vec![Vec::new(); n];
        let mut new_in_edges: Vec<Vec<(u16, u32)>> = vec![Vec::new(); n];

        // Collect surviving dense edges.
        for node in 0..n {
            let node_id = node as u32;
            let idx = node_id as usize;

            if idx + 1 < self.out_offsets.len() {
                let start = self.out_offsets[idx] as usize;
                let end = self.out_offsets[idx + 1] as usize;
                for i in start..end {
                    let lid = self.out_labels[i];
                    let dst = self.out_targets[i];
                    if !self.deleted_edges.contains(&(node_id, lid, dst)) {
                        new_out_edges[node].push((lid, dst));
                    }
                }
            }

            if idx + 1 < self.in_offsets.len() {
                let start = self.in_offsets[idx] as usize;
                let end = self.in_offsets[idx + 1] as usize;
                for i in start..end {
                    let lid = self.in_labels[i];
                    let src = self.in_targets[i];
                    if !self.deleted_edges.contains(&(src, lid, node_id)) {
                        new_in_edges[node].push((lid, src));
                    }
                }
            }
        }

        // Merge buffer edges.
        for node in 0..n {
            for &(lid, dst) in &self.buffer_out[node] {
                if !new_out_edges[node]
                    .iter()
                    .any(|&(l, d)| l == lid && d == dst)
                {
                    new_out_edges[node].push((lid, dst));
                }
            }
            for &(lid, src) in &self.buffer_in[node] {
                if !new_in_edges[node]
                    .iter()
                    .any(|&(l, s)| l == lid && s == src)
                {
                    new_in_edges[node].push((lid, src));
                }
            }
        }

        // Build new dense arrays.
        let (out_offsets, out_targets, out_labels) = build_dense(&new_out_edges);
        let (in_offsets, in_targets, in_labels) = build_dense(&new_in_edges);

        self.out_offsets = out_offsets;
        self.out_targets = out_targets;
        self.out_labels = out_labels;
        self.in_offsets = in_offsets;
        self.in_targets = in_targets;
        self.in_labels = in_labels;

        for buf in &mut self.buffer_out {
            buf.clear();
        }
        for buf in &mut self.buffer_in {
            buf.clear();
        }
        self.deleted_edges.clear();
    }

    /// Serialize the index to MessagePack bytes for storage.
    pub fn checkpoint_to_bytes(&self) -> Vec<u8> {
        let snapshot = CsrSnapshot {
            nodes: self.id_to_node.clone(),
            labels: self.id_to_label.clone(),
            out_offsets: self.out_offsets.clone(),
            out_targets: self.out_targets.clone(),
            out_labels: self.out_labels.clone(),
            in_offsets: self.in_offsets.clone(),
            in_targets: self.in_targets.clone(),
            in_labels: self.in_labels.clone(),
            buffer_out: self.buffer_out.clone(),
            buffer_in: self.buffer_in.clone(),
            deleted: self.deleted_edges.iter().copied().collect(),
        };
        match rmp_serde::to_vec_named(&snapshot) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!(error = %e, "CSR checkpoint serialization failed");
                Vec::new()
            }
        }
    }

    /// Restore an index from a checkpoint snapshot.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        let snap: CsrSnapshot = rmp_serde::from_slice(bytes).ok()?;

        let node_to_id: HashMap<String, u32> = snap
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as u32))
            .collect();
        let label_to_id: HashMap<String, u16> = snap
            .labels
            .iter()
            .enumerate()
            .map(|(i, l)| (l.clone(), i as u16))
            .collect();

        Some(Self {
            node_to_id,
            id_to_node: snap.nodes,
            label_to_id,
            id_to_label: snap.labels,
            out_offsets: snap.out_offsets,
            out_targets: snap.out_targets,
            out_labels: snap.out_labels,
            in_offsets: snap.in_offsets,
            in_targets: snap.in_targets,
            in_labels: snap.in_labels,
            buffer_out: snap.buffer_out,
            buffer_in: snap.buffer_in,
            deleted_edges: snap.deleted.into_iter().collect(),
        })
    }
}

/// Build contiguous offset/target/label arrays from per-node edge lists.
pub(crate) fn build_dense(edges: &[Vec<(u16, u32)>]) -> (Vec<u32>, Vec<u32>, Vec<u16>) {
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
