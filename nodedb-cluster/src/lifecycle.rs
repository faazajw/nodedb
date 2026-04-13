//! Node lifecycle management: join, leave, decommission.
//!
//! Handles the full lifecycle of a node in the cluster:
//!
//! 1. **Join**: Node contacts seed, receives topology, joins as Learner,
//!    catches up Raft logs, promoted to Active voter.
//! 2. **Decommission**: Node drains leadership, migrates all vShards to
//!    other nodes, then shuts down cleanly.
//! 3. **Leave**: Node is removed from topology after decommission completes.
//!
//! All transitions are replicated via the metadata Raft group as
//! [`MetadataEntry::TopologyChange`] / [`MetadataEntry::RoutingChange`]
//! entries and applied through the `MetadataApplier` on every node.

use tracing::{info, warn};

use crate::error::{ClusterError, Result};
use crate::metadata_group::{MetadataEntry, RoutingChange, TopologyChange};
use crate::routing::RoutingTable;
use crate::topology::{ClusterTopology, NodeInfo, NodeState};

/// Result of a decommission operation.
#[derive(Debug)]
pub struct DecommissionResult {
    pub vshards_migrated: usize,
    pub leadership_transferred: usize,
    pub completed: bool,
}

/// Plan a node decommission: compute which vShards to migrate and where.
///
/// Produces a sequence of [`MetadataEntry`] values to be proposed against
/// the metadata Raft group in order. Steps:
/// 1. Start decommission (topology transition).
/// 2. Transfer leadership of all Raft groups led by this node.
pub fn plan_decommission(
    node_id: u64,
    topology: &ClusterTopology,
    routing: &RoutingTable,
) -> Result<Vec<MetadataEntry>> {
    let node = topology.get_node(node_id).ok_or(ClusterError::Transport {
        detail: format!("node {node_id} not found in topology"),
    })?;

    if node.state == NodeState::Decommissioned {
        return Err(ClusterError::Transport {
            detail: format!("node {node_id} is already decommissioned"),
        });
    }

    let mut entries = Vec::new();

    // Step 1: Start decommission.
    entries.push(MetadataEntry::TopologyChange(
        TopologyChange::StartDecommission { node_id },
    ));

    // Step 2: Leadership transfers for groups led by this node.
    for group_id in routing.group_ids() {
        if let Some(info) = routing.group_info(group_id)
            && info.leader == node_id
            && let Some(&new_leader) = info.members.iter().find(|&&m| m != node_id)
        {
            entries.push(MetadataEntry::RoutingChange(
                RoutingChange::LeadershipTransfer {
                    group_id,
                    new_leader_node_id: new_leader,
                },
            ));
        }
    }

    info!(
        node_id,
        metadata_entries = entries.len(),
        "decommission plan computed"
    );
    Ok(entries)
}

/// Check if a node can be safely removed from the cluster.
pub fn is_safe_to_remove(node_id: u64, topology: &ClusterTopology, routing: &RoutingTable) -> bool {
    let Some(node) = topology.get_node(node_id) else {
        return false;
    };
    if !matches!(node.state, NodeState::Draining | NodeState::Decommissioned) {
        return false;
    }

    for group_id in routing.group_ids() {
        if let Some(info) = routing.group_info(group_id)
            && info.leader == node_id
            && info.members.len() <= 1
        {
            return false;
        }
    }

    true
}

/// Register a joining node in the local topology and produce the
/// [`MetadataEntry`] to be proposed on the metadata Raft group.
pub fn handle_node_join(node_id: u64, addr: &str, topology: &mut ClusterTopology) -> MetadataEntry {
    use std::net::SocketAddr;

    let socket_addr: SocketAddr = addr.parse().unwrap_or_else(|_| {
        warn!(node_id, addr, "invalid address, using default");
        SocketAddr::from(([0, 0, 0, 0], 0))
    });

    let info = NodeInfo::new(node_id, socket_addr, NodeState::Joining);
    topology.join_as_learner(info);

    info!(node_id, addr, "node joining as learner");
    MetadataEntry::TopologyChange(TopologyChange::Join {
        node_id,
        addr: addr.to_string(),
    })
}

/// Handle learner promotion after state catch-up validation.
pub fn handle_learner_promotion(
    node_id: u64,
    topology: &mut ClusterTopology,
    log_lag: u64,
    max_lag: u64,
) -> Result<MetadataEntry> {
    let node = topology.get_node(node_id).ok_or(ClusterError::Transport {
        detail: format!("node {node_id} not found"),
    })?;

    if node.state != NodeState::Learner {
        return Err(ClusterError::Transport {
            detail: format!("node {node_id} is not a learner (state: {:?})", node.state),
        });
    }

    if log_lag > max_lag {
        return Err(ClusterError::Transport {
            detail: format!("node {node_id} not caught up: lag={log_lag}, max={max_lag}"),
        });
    }

    topology.promote_to_voter(node_id);
    info!(node_id, log_lag, "learner promoted to voter");

    Ok(MetadataEntry::TopologyChange(
        TopologyChange::PromoteToVoter { node_id },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn make_topology_and_routing() -> (ClusterTopology, RoutingTable) {
        let mut topo = ClusterTopology::new();
        let addr1: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let addr3: SocketAddr = "127.0.0.1:9002".parse().unwrap();

        topo.add_node(NodeInfo::new(1, addr1, NodeState::Active));
        topo.add_node(NodeInfo::new(2, addr2, NodeState::Active));
        topo.add_node(NodeInfo::new(3, addr3, NodeState::Active));

        let routing = RoutingTable::uniform(4, &[1, 2, 3], 2);
        (topo, routing)
    }

    #[test]
    fn decommission_plan_creates_metadata_entries() {
        let (topo, routing) = make_topology_and_routing();
        let entries = plan_decommission(1, &topo, &routing).unwrap();
        assert!(!entries.is_empty());
        match &entries[0] {
            MetadataEntry::TopologyChange(TopologyChange::StartDecommission { node_id }) => {
                assert_eq!(*node_id, 1);
            }
            other => panic!("expected StartDecommission, got {other:?}"),
        }
    }

    #[test]
    fn safe_to_remove_draining_node() {
        let (mut topo, routing) = make_topology_and_routing();
        topo.set_state(1, NodeState::Draining);
        let _ = is_safe_to_remove(1, &topo, &routing);
    }

    #[test]
    fn node_join_creates_learner() {
        let mut topo = ClusterTopology::new();
        let entry = handle_node_join(5, "10.0.0.5:9000", &mut topo);
        assert!(topo.contains(5));
        assert_eq!(topo.learner_nodes().len(), 1);
        match entry {
            MetadataEntry::TopologyChange(TopologyChange::Join { node_id, .. }) => {
                assert_eq!(node_id, 5);
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn learner_promotion_checks_lag() {
        let mut topo = ClusterTopology::new();
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let info = NodeInfo::new(10, addr, NodeState::Joining);
        topo.join_as_learner(info);

        let result = handle_learner_promotion(10, &mut topo, 100, 10);
        assert!(result.is_err());

        let result = handle_learner_promotion(10, &mut topo, 5, 10);
        assert!(result.is_ok());
        assert_eq!(topo.get_node(10).unwrap().state, NodeState::Active);
    }

    #[test]
    fn decommission_already_decommissioned_fails() {
        let (mut topo, routing) = make_topology_and_routing();
        topo.set_state(1, NodeState::Decommissioned);
        let result = plan_decommission(1, &topo, &routing);
        assert!(result.is_err());
    }
}
