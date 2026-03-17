use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::{info, warn};

use pgwire::tokio::process_socket;

use crate::config::auth::AuthMode;
use crate::control::state::SharedState;

use super::factory::NodeDbPgHandlerFactory;

/// PostgreSQL wire protocol listener.
///
/// Accepts TCP connections and handles them using the pgwire crate.
/// Optionally supports TLS (SSLRequest negotiation + upgrade).
/// Runs on the Control Plane (Tokio).
pub struct PgListener {
    tcp: TcpListener,
    addr: SocketAddr,
}

impl PgListener {
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        let tcp = TcpListener::bind(addr).await?;
        let local_addr = tcp.local_addr()?;
        info!(%local_addr, "pgwire listener bound");
        Ok(Self {
            tcp,
            addr: local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Run the accept loop for pgwire connections.
    ///
    /// `tls_acceptor`: if Some, pgwire will negotiate SSL on SSLRequest.
    /// If None, all connections are plaintext.
    pub async fn run(
        self,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> crate::Result<()> {
        let factory = Arc::new(NodeDbPgHandlerFactory::new(state, auth_mode));

        let tls_label = if tls_acceptor.is_some() { "tls" } else { "plain" };
        info!(addr = %self.addr, tls = tls_label, "accepting pgwire connections");

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            info!(%peer_addr, "new pgwire connection");
                            let factory = Arc::clone(&factory);
                            let tls = tls_acceptor.clone();
                            tokio::spawn(async move {
                                if let Err(e) = process_socket(stream, tls, factory).await {
                                    warn!(%peer_addr, error = %e, "pgwire session error");
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "pgwire accept failed, retrying");
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(addr = %self.addr, "shutdown signal, stopping pgwire listener");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
