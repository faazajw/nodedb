//! Inbound Raft RPC handling.
//!
//! Accepts connections from the QUIC endpoint, dispatches incoming bidi
//! streams to a [`RaftRpcHandler`], and writes back the response frame.
//!
//! # Cooperative shutdown
//!
//! Every long-lived `.await` in this module is wrapped in a
//! `tokio::select!` over a `watch::Receiver<bool>` shutdown signal
//! that is cloned into every spawned child task. When the
//! top-level `NexarTransport::serve` loop observes its `shutdown`
//! receiver flip to `true`, the same signal reaches every
//! grandchild stream-handler task at its next await point, their
//! futures drop, and the per-connection captured
//! `Arc<RaftRpcHandler>` clones are released promptly. Without
//! this propagation, a graceful shutdown of the serve loop
//! leaves grandchild tasks pinned inside
//! `quinn::Connection::accept_bi` or `quinn::RecvStream::read_exact`
//! forever, holding the handler Arc — and therefore any redb file
//! handles the handler owns — for the lifetime of the runtime.

use std::sync::Arc;

use tokio::sync::watch;
use tracing::debug;

use crate::error::{ClusterError, Result};
use crate::rpc_codec::{self, RaftRpc};

/// Trait for handling incoming Raft RPCs.
///
/// Implementors receive a request [`RaftRpc`] and return the corresponding
/// response variant. The transport calls this for each incoming bidi stream.
pub trait RaftRpcHandler: Send + Sync + 'static {
    fn handle_rpc(&self, rpc: RaftRpc)
    -> impl std::future::Future<Output = Result<RaftRpc>> + Send;
}

/// Handle all bidi streams on a single connection.
///
/// Exits cleanly (Ok) on shutdown, on normal connection close,
/// or on unrecoverable transport error.
pub(crate) async fn handle_connection<H: RaftRpcHandler>(
    conn: quinn::Connection,
    handler: Arc<H>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        // Respect shutdown even if the peer is idle. `accept_bi`
        // otherwise blocks indefinitely, pinning the handler Arc.
        let accepted = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            result = conn.accept_bi() => result,
        };

        let (send, recv) = match accepted {
            Ok(streams) => streams,
            Err(quinn::ConnectionError::ApplicationClosed(_)) => return Ok(()),
            Err(quinn::ConnectionError::LocallyClosed) => return Ok(()),
            Err(e) => {
                return Err(ClusterError::Transport {
                    detail: format!("accept_bi: {e}"),
                });
            }
        };

        let h = handler.clone();
        let stream_shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_stream(h, send, recv, stream_shutdown).await {
                debug!(error = %e, "raft RPC stream error");
            }
        });
    }
}

/// Handle a single bidi stream: read request → dispatch → write response.
///
/// Every long-lived await is racing a shutdown signal — see the
/// module docstring for the rationale.
async fn handle_stream<H: RaftRpcHandler>(
    handler: Arc<H>,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let work = async {
        let request_frame = read_frame(&mut recv).await?;
        let request = rpc_codec::decode(&request_frame)?;
        let response = handler.handle_rpc(request).await?;
        let response_frame = rpc_codec::encode(&response)?;
        send.write_all(&response_frame)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write response: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish response: {e}"),
        })?;
        Ok::<(), ClusterError>(())
    };

    tokio::select! {
        biased;
        _ = shutdown.changed() => Ok(()),
        result = work => result,
    }
}

/// Read a complete RPC frame from a QUIC receive stream.
///
/// Reads the header first to determine frame size, then reads the payload.
pub(crate) async fn read_frame(recv: &mut quinn::RecvStream) -> Result<Vec<u8>> {
    let mut header = [0u8; rpc_codec::HEADER_SIZE];
    recv.read_exact(&mut header)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("read header: {e}"),
        })?;

    let total = rpc_codec::frame_size(&header)?;
    let mut frame = vec![0u8; total];
    frame[..rpc_codec::HEADER_SIZE].copy_from_slice(&header);

    if total > rpc_codec::HEADER_SIZE {
        recv.read_exact(&mut frame[rpc_codec::HEADER_SIZE..])
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("read payload: {e}"),
            })?;
    }

    Ok(frame)
}
