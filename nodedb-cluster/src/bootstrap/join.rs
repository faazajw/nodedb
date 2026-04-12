//! Join path: contact seeds, receive full cluster state, apply locally.

use std::net::SocketAddr;
use tracing::{info, warn};

use crate::catalog::ClusterCatalog;
use crate::error::{ClusterError, Result};
use crate::multi_raft::MultiRaft;
use crate::routing::{GroupInfo, RoutingTable};
use crate::rpc_codec::{JoinRequest, JoinResponse, RaftRpc};
use crate::topology::{ClusterTopology, NodeInfo, NodeState};
use crate::transport::NexarTransport;

use super::config::{ClusterConfig, ClusterState};

/// Join an existing cluster by contacting seed nodes.
pub(super) async fn join(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
) -> Result<ClusterState> {
    info!(
        node_id = config.node_id,
        seeds = ?config.seed_nodes,
        "joining existing cluster"
    );

    let req = RaftRpc::JoinRequest(JoinRequest {
        node_id: config.node_id,
        listen_addr: config.listen_addr.to_string(),
    });

    // Try each seed until one accepts.
    let mut last_err = None;
    for addr in &config.seed_nodes {
        match transport.send_rpc_to_addr(*addr, req.clone()).await {
            Ok(RaftRpc::JoinResponse(resp)) => {
                if !resp.success {
                    last_err = Some(ClusterError::Transport {
                        detail: format!("join rejected by {addr}: {}", resp.error),
                    });
                    continue;
                }
                return apply_join_response(config, catalog, transport, &resp);
            }
            Ok(other) => {
                last_err = Some(ClusterError::Transport {
                    detail: format!("unexpected response from {addr}: {other:?}"),
                });
            }
            Err(e) => {
                warn!(%addr, error = %e, "seed unreachable");
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| ClusterError::Transport {
        detail: "no seed nodes configured".into(),
    }))
}

/// Apply a JoinResponse: reconstruct topology, routing, and MultiRaft from wire data.
fn apply_join_response(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
    resp: &JoinResponse,
) -> Result<ClusterState> {
    // Reconstruct topology.
    let mut topology = ClusterTopology::new();
    for node in &resp.nodes {
        let state = NodeState::from_u8(node.state).unwrap_or(NodeState::Active);
        let mut info = NodeInfo {
            node_id: node.node_id,
            addr: node.addr.clone(),
            state,
            raft_groups: node.raft_groups.clone(),
        };
        // If this is us, mark as Active.
        if node.node_id == config.node_id {
            info.state = NodeState::Active;
        }
        topology.add_node(info);
    }

    // Reconstruct routing table.
    let mut group_members = std::collections::HashMap::new();
    for g in &resp.groups {
        group_members.insert(
            g.group_id,
            GroupInfo {
                leader: g.leader,
                members: g.members.clone(),
                learners: g.learners.clone(),
            },
        );
    }
    let routing = RoutingTable::from_parts(resp.vshard_to_group.clone(), group_members);

    // Create MultiRaft — join any group that includes this node, either
    // as a voter (group members) or as a learner (group learners). A
    // learner-started group boots in the `Learner` role and will not run
    // an election until a subsequent `PromoteLearner` conf change is
    // applied.
    let mut multi_raft = MultiRaft::new(config.node_id, routing.clone(), config.data_dir.clone());
    for g in &resp.groups {
        let is_voter = g.members.contains(&config.node_id);
        let is_learner = g.learners.contains(&config.node_id);

        if is_voter {
            let peers: Vec<u64> = g
                .members
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group(g.group_id, peers)?;
        } else if is_learner {
            // Voters = full member set (none of them is self).
            let voters = g.members.clone();
            // Other learners catching up alongside us (exclude self).
            let other_learners: Vec<u64> = g
                .learners
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group_as_learner(g.group_id, voters, other_learners)?;
        }
    }

    // Register all peers in the transport.
    for node in &resp.nodes {
        if node.node_id != config.node_id
            && let Ok(addr) = node.addr.parse::<SocketAddr>()
        {
            transport.register_peer(node.node_id, addr);
        }
    }

    // Persist.
    catalog.save_topology(&topology)?;
    catalog.save_routing(&routing)?;

    info!(
        node_id = config.node_id,
        nodes = topology.node_count(),
        groups = routing.num_groups(),
        "joined cluster"
    );

    Ok(ClusterState {
        topology,
        routing,
        multi_raft,
    })
}

#[cfg(test)]
mod tests {
    use super::super::bootstrap_fn::bootstrap;
    use super::super::handle_join::handle_join_request;
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn temp_catalog() -> (tempfile::TempDir, ClusterCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let catalog = ClusterCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    #[tokio::test]
    async fn full_bootstrap_join_flow() {
        // Node 1 bootstraps, Node 2 joins via QUIC.
        let t1 = Arc::new(NexarTransport::new(1, "127.0.0.1:0".parse().unwrap()).unwrap());
        let t2 = Arc::new(NexarTransport::new(2, "127.0.0.1:0".parse().unwrap()).unwrap());

        let (_dir1, catalog1) = temp_catalog();
        let (_dir2, catalog2) = temp_catalog();

        let addr1 = t1.local_addr();
        let addr2 = t2.local_addr();

        // Bootstrap node 1.
        let config1 = ClusterConfig {
            node_id: 1,
            listen_addr: addr1,
            seed_nodes: vec![addr1],
            num_groups: 2,
            replication_factor: 1,
            data_dir: _dir1.path().to_path_buf(),
        };
        let state1 = bootstrap(&config1, &catalog1).unwrap();

        // Set up a handler for node 1 that handles JoinRequests.
        let topology1 = Arc::new(Mutex::new(state1.topology));
        let routing1 = Arc::new(state1.routing);

        struct JoinHandler {
            topology: Arc<Mutex<ClusterTopology>>,
            routing: Arc<RoutingTable>,
        }

        impl crate::transport::RaftRpcHandler for JoinHandler {
            async fn handle_rpc(&self, rpc: RaftRpc) -> Result<RaftRpc> {
                match rpc {
                    RaftRpc::JoinRequest(req) => {
                        let mut topo = self.topology.lock().unwrap();
                        let resp = handle_join_request(&req, &mut topo, &self.routing);
                        Ok(RaftRpc::JoinResponse(resp))
                    }
                    other => Err(ClusterError::Transport {
                        detail: format!("unexpected: {other:?}"),
                    }),
                }
            }
        }

        let handler = Arc::new(JoinHandler {
            topology: topology1.clone(),
            routing: routing1.clone(),
        });

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let t1c = t1.clone();
        tokio::spawn(async move {
            t1c.serve(handler, shutdown_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(30)).await;

        // Node 2 joins.
        let config2 = ClusterConfig {
            node_id: 2,
            listen_addr: addr2,
            seed_nodes: vec![addr1],
            num_groups: 2,
            replication_factor: 1,
            data_dir: _dir2.path().to_path_buf(),
        };

        let state2 = join(&config2, &catalog2, &t2).await.unwrap();

        assert_eq!(state2.topology.node_count(), 2);
        assert_eq!(state2.routing.num_groups(), 2);

        // Verify node 2's state was persisted.
        assert!(catalog2.load_topology().unwrap().is_some());
        assert!(catalog2.load_routing().unwrap().is_some());

        // Verify node 1's topology was updated.
        let topo1 = topology1.lock().unwrap();
        assert_eq!(topo1.node_count(), 2);
        assert!(topo1.contains(2));

        shutdown_tx.send(true).unwrap();
    }
}
