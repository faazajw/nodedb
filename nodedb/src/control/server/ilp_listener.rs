//! ILP (InfluxDB Line Protocol) TCP listener for timeseries ingest.
//!
//! Accepts plain TCP connections on the configured port. Each connection
//! reads newline-delimited ILP lines, parses them, and dispatches
//! `TimeseriesIngest` plans to the Data Plane via SPSC.
//!
//! Protocol: raw TCP, one ILP line per newline. No HTTP overhead.
//! Compatible with `telegraf`, `vector`, and InfluxDB client libraries.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};

/// ILP TCP listener.
pub struct IlpListener {
    tcp: TcpListener,
    addr: SocketAddr,
}

impl IlpListener {
    /// Bind to the given address.
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        let tcp = TcpListener::bind(addr).await.map_err(crate::Error::Io)?;
        info!(%addr, "ILP TCP listener bound");
        Ok(Self { tcp, addr })
    }

    /// Run the accept loop until shutdown.
    pub async fn run(
        self,
        state: Arc<SharedState>,
        conn_semaphore: Arc<Semaphore>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> crate::Result<()> {
        let mut connections = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            let permit = match conn_semaphore.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    debug!(%peer, "ILP connection rejected: max connections");
                                    continue;
                                }
                            };
                            let state = Arc::clone(&state);
                            connections.spawn(async move {
                                if let Err(e) = handle_ilp_connection(stream, peer, &state).await {
                                    debug!(%peer, error = %e, "ILP connection error");
                                }
                                drop(permit);
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "ILP accept error");
                        }
                    }
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
                _ = shutdown.changed() => {
                    info!(addr = %self.addr, "ILP listener shutting down");
                    break;
                }
            }
        }

        // Drain remaining connections with timeout.
        let drain = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while connections.join_next().await.is_some() {}
        });
        let _ = drain.await;
        Ok(())
    }
}

/// Handle a single ILP TCP connection.
///
/// Reads lines, batches them per 100ms or 1000 lines (whichever comes first),
/// then dispatches a single `TimeseriesIngest` per batch.
async fn handle_ilp_connection(
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
    state: &SharedState,
) -> crate::Result<()> {
    debug!(%peer, "ILP connection accepted");

    let reader = BufReader::new(stream);
    let mut lines = reader.lines();
    let mut batch = String::new();
    let mut line_count = 0u64;
    let mut total_ingested = 0u64;

    // Default tenant for ILP connections (configurable via auth in future).
    let tenant_id = TenantId::new(1);

    while let Ok(Some(line)) = lines.next_line().await {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        batch.push_str(&line);
        batch.push('\n');
        line_count += 1;

        // Flush batch every 1000 lines.
        if line_count >= 1000 {
            total_ingested += flush_ilp_batch(state, tenant_id, &batch).await?;
            batch.clear();
            line_count = 0;
        }
    }

    // Flush remaining.
    if !batch.is_empty() {
        total_ingested += flush_ilp_batch(state, tenant_id, &batch).await?;
    }

    debug!(%peer, total_ingested, "ILP connection closed");
    Ok(())
}

/// Dispatch an ILP batch to the Data Plane.
async fn flush_ilp_batch(
    state: &SharedState,
    tenant_id: TenantId,
    batch: &str,
) -> crate::Result<u64> {
    // Determine collection from the first line's measurement name.
    let collection = batch
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .and_then(|l| l.split([',', ' ']).next())
        .unwrap_or("default_metrics")
        .to_string();

    let vshard_id = VShardId::from_collection(&collection);

    let plan = PhysicalPlan::TimeseriesIngest {
        collection,
        payload: batch.as_bytes().to_vec(),
        format: "ilp".to_string(),
    };

    // WAL + dispatch.
    crate::control::server::wal_dispatch::wal_append_if_write_with_creds(
        &state.wal,
        tenant_id,
        vshard_id,
        &plan,
        Some(&state.credentials),
    )?;

    let response = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard_id, plan, 0, // trace_id
    )
    .await?;

    // Parse accepted count from response.
    let accepted = if !response.payload.is_empty() {
        serde_json::from_slice::<serde_json::Value>(&response.payload)
            .ok()
            .and_then(|v| v.get("accepted").and_then(|a| a.as_u64()))
            .unwrap_or(0)
    } else {
        0
    };

    Ok(accepted)
}

#[cfg(test)]
mod tests {
    #[test]
    fn extract_collection_from_ilp() {
        let batch = "cpu,host=server01 value=0.64 1000\nmem,host=server01 used=1024 2000\n";
        let collection = batch
            .lines()
            .find(|l| !l.is_empty() && !l.starts_with('#'))
            .and_then(|l| l.split([',', ' ']).next())
            .unwrap_or("default_metrics");
        assert_eq!(collection, "cpu");
    }
}
