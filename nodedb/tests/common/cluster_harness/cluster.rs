//! Multi-node cluster orchestration.

use std::time::Duration;

use super::node::TestClusterNode;
use super::wait::wait_for;

/// An in-process cluster of `TestClusterNode`s.
pub struct TestCluster {
    pub nodes: Vec<TestClusterNode>,
}

impl TestCluster {
    /// Spawn a 3-node cluster: node 1 bootstraps, nodes 2 and 3 join
    /// via node 1's pre-bound address. Waits until every node sees
    /// topology_size == 3 (10s deadline).
    pub async fn spawn_three() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let node1 = TestClusterNode::spawn(1, vec![]).await?;

        // Give node 1's transport + raft loop a moment to start
        // accepting before peers dial in.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let seeds = vec![node1.listen_addr];
        let node2 = TestClusterNode::spawn(2, seeds.clone()).await?;
        let node3 = TestClusterNode::spawn(3, seeds).await?;

        let cluster = Self {
            nodes: vec![node1, node2, node3],
        };

        wait_for(
            "all 3 nodes report topology_size == 3",
            Duration::from_secs(10),
            Duration::from_millis(100),
            || cluster.nodes.iter().all(|n| n.topology_size() == 3),
        )
        .await;

        Ok(cluster)
    }

    /// Find a node that will accept the given DDL — retries up to
    /// 10 seconds across all nodes. Non-leader nodes surface
    /// `not metadata-group leader` errors via the pgwire error path;
    /// the retry loop tries the next node on failure so the test
    /// doesn't have to discover the leader explicitly.
    pub async fn exec_ddl_on_any_leader(&self, sql: &str) -> Result<usize, String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut last_err = String::new();
        while std::time::Instant::now() < deadline {
            for (idx, node) in self.nodes.iter().enumerate() {
                match node.exec(sql).await {
                    Ok(()) => return Ok(idx),
                    Err(e) => last_err = e,
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!(
            "no node accepted DDL within 10s; last error: {last_err}"
        ))
    }

    /// Cooperatively shut down every node. Reverse order so peers
    /// observe their neighbours' drop without rejecting inbound
    /// traffic on an already-closed transport.
    pub async fn shutdown(self) {
        let mut nodes = self.nodes;
        while let Some(node) = nodes.pop() {
            node.shutdown().await;
        }
    }
}
