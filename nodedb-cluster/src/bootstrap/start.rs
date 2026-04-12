//! Cluster startup entry point: dispatches to bootstrap, join, or restart.

use crate::catalog::ClusterCatalog;
use crate::error::Result;
use crate::rpc_codec::{JoinRequest, RaftRpc};
use crate::transport::NexarTransport;

use super::bootstrap_fn::bootstrap;
use super::config::{ClusterConfig, ClusterState};
use super::join::join;
use super::restart::restart;

/// Start the cluster — bootstrap, join, or restart depending on state.
///
/// Returns the initialized cluster state ready for the Raft loop.
pub async fn start_cluster(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
) -> Result<ClusterState> {
    // Check if we have existing state.
    if catalog.is_bootstrapped()? {
        return restart(config, catalog, transport);
    }

    // No existing state — try bootstrap or join.
    let is_seed = config.seed_nodes.contains(&config.listen_addr);

    if is_seed && should_bootstrap(config, transport).await {
        bootstrap(config, catalog)
    } else {
        join(config, catalog, transport).await
    }
}

/// Check if this seed should bootstrap a new cluster.
///
/// A seed bootstraps if no other seed is already running.
pub(super) async fn should_bootstrap(config: &ClusterConfig, transport: &NexarTransport) -> bool {
    for addr in &config.seed_nodes {
        if *addr == config.listen_addr {
            continue;
        }
        // Try to contact another seed.
        let probe = RaftRpc::JoinRequest(JoinRequest {
            node_id: config.node_id,
            listen_addr: config.listen_addr.to_string(),
        });
        match transport.send_rpc_to_addr(*addr, probe).await {
            Ok(_) => return false, // Another seed is alive — join instead.
            Err(_) => continue,    // Seed not reachable — keep checking.
        }
    }
    // No other seed responded — we bootstrap.
    true
}
