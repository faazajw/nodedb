//! Cluster configuration and post-start state.

use std::net::SocketAddr;

use crate::multi_raft::MultiRaft;
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;

/// Configuration for cluster formation.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's unique ID.
    pub node_id: u64,
    /// Address to listen on for Raft RPCs.
    pub listen_addr: SocketAddr,
    /// Seed node addresses for bootstrap/join.
    pub seed_nodes: Vec<SocketAddr>,
    /// Number of Raft groups to create on bootstrap.
    pub num_groups: u64,
    /// Replication factor (number of replicas per group).
    pub replication_factor: usize,
    /// Data directory for persistent Raft log storage.
    pub data_dir: std::path::PathBuf,
}

/// Result of cluster startup — everything needed to run the Raft loop.
pub struct ClusterState {
    pub topology: ClusterTopology,
    pub routing: RoutingTable,
    pub multi_raft: MultiRaft,
}
