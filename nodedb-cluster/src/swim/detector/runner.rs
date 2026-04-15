//! `FailureDetector` — the SWIM runtime task.
//!
//! One instance per node. Owns the membership list (shared via `Arc`),
//! the probe scheduler, the suspicion timer, the inflight-probe registry,
//! and the async transport. Drives a `tokio::select!` loop over four
//! arms: probe tick, inbound datagram, suspicion expiry, shutdown.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, watch};
use tokio::time::{Instant, interval};

use crate::swim::config::SwimConfig;
use crate::swim::error::SwimError;
use crate::swim::incarnation::Incarnation;
use crate::swim::member::MemberState;
use crate::swim::member::record::MemberUpdate;
use crate::swim::membership::MembershipList;
use crate::swim::wire::{Ack, Ping, PingReq, ProbeId, SwimMessage};

use super::probe_round::{InflightProbes, ProbeOutcome, ProbeRound};
use super::scheduler::ProbeScheduler;
use super::suspicion::SuspicionTimer;
use super::transport::Transport;

/// Top-level failure detector handle.
///
/// Construct with [`FailureDetector::new`], then call
/// [`FailureDetector::run`] on a dedicated tokio task. The run loop
/// returns when `shutdown` flips to `true`.
pub struct FailureDetector {
    cfg: SwimConfig,
    membership: Arc<MembershipList>,
    transport: Arc<dyn Transport>,
    scheduler: Mutex<ProbeScheduler>,
    suspicion: Mutex<SuspicionTimer>,
    inflight: Arc<InflightProbes>,
    probe_counter: AtomicU64,
    local_incarnation: Mutex<Incarnation>,
}

impl FailureDetector {
    /// Construct. Does not spawn anything — the caller is responsible
    /// for driving [`Self::run`] on a tokio task.
    pub fn new(
        cfg: SwimConfig,
        membership: Arc<MembershipList>,
        transport: Arc<dyn Transport>,
        scheduler: ProbeScheduler,
    ) -> Self {
        let initial_inc = cfg.initial_incarnation;
        Self {
            cfg,
            membership,
            transport,
            scheduler: Mutex::new(scheduler),
            suspicion: Mutex::new(SuspicionTimer::new()),
            inflight: Arc::new(InflightProbes::new()),
            probe_counter: AtomicU64::new(0),
            local_incarnation: Mutex::new(initial_inc),
        }
    }

    /// Exposed for tests that need to route a synthetic message into the
    /// inflight table without going through the transport.
    #[cfg(test)]
    pub fn inflight(&self) -> &Arc<InflightProbes> {
        &self.inflight
    }

    fn next_probe_id(&self) -> ProbeId {
        ProbeId::new(self.probe_counter.fetch_add(1, Ordering::Relaxed))
    }

    /// Main loop. Returns when `shutdown` receives `true`.
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = interval(self.cfg.probe_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Consume the first immediate tick so the first probe aligns
        // with a full interval from start.
        tick.tick().await;
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
                _ = tick.tick() => {
                    self.on_tick().await;
                }
                recv = self.transport.recv() => {
                    match recv {
                        Ok((from_addr, msg)) => self.on_incoming(from_addr, msg).await,
                        Err(SwimError::TransportClosed) => break,
                        Err(_) => {}
                    }
                }
            }
        }
    }

    async fn on_tick(&self) {
        // Expire suspect members that have waited out their timeout.
        let now = Instant::now();
        let expired = self.suspicion.lock().await.drain_expired(now);
        for node_id in expired {
            if let Some(member) = self.membership.get(&node_id) {
                self.membership.apply(&MemberUpdate {
                    node_id: node_id.clone(),
                    addr: member.addr.to_string(),
                    state: MemberState::Dead,
                    incarnation: member.incarnation,
                });
            }
        }

        // Execute one probe round against the next target.
        let local_inc = *self.local_incarnation.lock().await;
        let mut sched = self.scheduler.lock().await;
        let outcome = ProbeRound {
            scheduler: &mut sched,
            membership: &self.membership,
            transport: &self.transport,
            inflight: &self.inflight,
            probe_timeout: self.cfg.probe_timeout,
            k_indirect: self.cfg.indirect_probes as usize,
            next_probe_id: || self.next_probe_id(),
            local_incarnation: local_inc,
        }
        .execute()
        .await;
        drop(sched);

        match outcome {
            Ok(ProbeOutcome::Idle) | Ok(ProbeOutcome::Acked { .. }) => {}
            Ok(ProbeOutcome::Suspect { target }) => {
                if let Some(member) = self.membership.get(&target) {
                    self.membership.apply(&MemberUpdate {
                        node_id: target.clone(),
                        addr: member.addr.to_string(),
                        state: MemberState::Suspect,
                        incarnation: member.incarnation,
                    });
                    let cluster_size = self.membership.len();
                    self.suspicion.lock().await.arm(
                        target,
                        Instant::now(),
                        &self.cfg,
                        cluster_size,
                    );
                }
            }
            Err(_) => {}
        }
    }

    async fn on_incoming(&self, from_addr: SocketAddr, msg: SwimMessage) {
        match msg {
            SwimMessage::Ping(ping) => self.handle_ping(from_addr, ping).await,
            SwimMessage::PingReq(req) => self.handle_ping_req(from_addr, req).await,
            SwimMessage::Ack(ack) => {
                self.inflight
                    .resolve(ack.probe_id, SwimMessage::Ack(ack))
                    .await
            }
            SwimMessage::Nack(nack) => {
                self.inflight
                    .resolve(nack.probe_id, SwimMessage::Nack(nack))
                    .await
            }
        }
    }

    async fn handle_ping(&self, from_addr: SocketAddr, ping: Ping) {
        let local_inc = *self.local_incarnation.lock().await;
        // Any self-refutation bump from piggyback is handled by
        // `MembershipList::apply`; E-γ does not yet ingest piggyback
        // (that's E-δ) but the reply incarnation still reflects any
        // local bump the detector already performed.
        let ack = SwimMessage::Ack(Ack {
            probe_id: ping.probe_id,
            from: self.membership.local_node_id().clone(),
            incarnation: local_inc,
            piggyback: vec![],
        });
        let _ = self.transport.send(from_addr, ack).await;
    }

    async fn handle_ping_req(&self, requester_addr: SocketAddr, req: PingReq) {
        let Ok(target_sock) = req.target_addr.parse::<SocketAddr>() else {
            return;
        };

        // Register a nested probe id; when the forwarded ack arrives
        // we rewrap it with the original probe id and relay to the
        // requester. The relay runs on a dedicated task so the detector
        // run-loop stays responsive.
        let forward_id = self.next_probe_id();
        let Ok(forward_rx) = self.inflight.register(forward_id).await else {
            return;
        };

        let local_node = self.membership.local_node_id().clone();
        let local_inc = *self.local_incarnation.lock().await;
        let transport = Arc::clone(&self.transport);
        let inflight = Arc::clone(&self.inflight);
        let timeout_dur = self.cfg.probe_timeout;
        let original_probe_id = req.probe_id;

        tokio::spawn(async move {
            let send_res = transport
                .send(
                    target_sock,
                    SwimMessage::Ping(Ping {
                        probe_id: forward_id,
                        from: local_node.clone(),
                        incarnation: local_inc,
                        piggyback: vec![],
                    }),
                )
                .await;
            if send_res.is_err() {
                inflight.forget(forward_id).await;
                return;
            }
            match tokio::time::timeout(timeout_dur, forward_rx).await {
                Ok(Ok(SwimMessage::Ack(ack))) => {
                    let relay = SwimMessage::Ack(Ack {
                        probe_id: original_probe_id,
                        from: ack.from,
                        incarnation: ack.incarnation,
                        piggyback: vec![],
                    });
                    let _ = transport.send(requester_addr, relay).await;
                }
                _ => {
                    inflight.forget(forward_id).await;
                }
            }
        });
    }

    /// Refute a self-suspect rumour by bumping local incarnation and
    /// rebroadcasting `Alive`. E-γ exposes the handle so tests can
    /// assert the behaviour; the dissemination queue in E-δ will call
    /// this automatically from the piggyback ingestor.
    #[cfg(test)]
    pub async fn bump_local_incarnation(&self, past: Incarnation) -> Incarnation {
        let mut guard = self.local_incarnation.lock().await;
        *guard = guard.refute(past);
        *guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::detector::transport::TransportFabric;
    use crate::swim::member::MemberState;
    use crate::swim::wire::ProbeId;
    use nodedb_types::NodeId;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p)
    }

    fn cfg() -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            probe_timeout: Duration::from_millis(40),
            indirect_probes: 2,
            suspicion_mult: 4,
            min_suspicion: Duration::from_millis(500),
            initial_incarnation: Incarnation::ZERO,
        }
    }

    async fn spawn_node(
        fab: &Arc<TransportFabric>,
        id: &str,
        port: u16,
        peers: &[(String, u16)],
    ) -> (
        Arc<FailureDetector>,
        watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    ) {
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(port)).await);
        let list = Arc::new(MembershipList::new_local(
            NodeId::new(id),
            addr(port),
            Incarnation::ZERO,
        ));
        for (peer_id, peer_port) in peers {
            list.apply(&MemberUpdate {
                node_id: NodeId::new(peer_id.as_str()),
                addr: addr(*peer_port).to_string(),
                state: MemberState::Alive,
                incarnation: Incarnation::new(1),
            });
        }
        let detector = Arc::new(FailureDetector::new(
            cfg(),
            list,
            transport,
            ProbeScheduler::with_seed(port as u64),
        ));
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn({
            let det = Arc::clone(&detector);
            async move { det.run(rx).await }
        });
        (detector, tx, handle)
    }

    #[tokio::test(start_paused = true)]
    async fn three_node_mesh_converges_when_target_partitioned() {
        let fab = TransportFabric::new();
        let peers_of = |me: &str| {
            ["a", "b", "c"]
                .iter()
                .filter(|p| **p != me)
                .map(|p| {
                    let port = match *p {
                        "a" => 7010,
                        "b" => 7011,
                        "c" => 7012,
                        _ => unreachable!(),
                    };
                    (p.to_string(), port)
                })
                .collect::<Vec<_>>()
        };
        let (det_a, sd_a, h_a) = spawn_node(&fab, "a", 7010, &peers_of("a")).await;
        let (_det_b, sd_b, h_b) = spawn_node(&fab, "b", 7011, &peers_of("b")).await;
        let (_det_c, sd_c, h_c) = spawn_node(&fab, "c", 7012, &peers_of("c")).await;

        // Partition b from everything (both directions).
        fab.drop_edge(addr(7010), addr(7011)).await;
        fab.drop_edge(addr(7011), addr(7010)).await;
        fab.drop_edge(addr(7012), addr(7011)).await;
        fab.drop_edge(addr(7011), addr(7012)).await;

        // Give the detector a few probe intervals to converge. Use
        // advance() in a loop so timers, inflight probes, and suspicion
        // expiry all get a chance to fire.
        for _ in 0..30 {
            tokio::time::advance(cfg().probe_interval).await;
            tokio::task::yield_now().await;
        }

        // A's membership view must have marked b as Dead (Suspect →
        // Dead after suspicion timeout).
        let m = det_a.membership.get(&NodeId::new("b")).expect("b in list");
        assert!(
            matches!(m.state, MemberState::Suspect | MemberState::Dead),
            "expected Suspect or Dead, got {:?}",
            m.state
        );

        // Shutdown.
        let _ = sd_a.send(true);
        let _ = sd_b.send(true);
        let _ = sd_c.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(200), h_a).await;
        let _ = tokio::time::timeout(Duration::from_millis(200), h_b).await;
        let _ = tokio::time::timeout(Duration::from_millis(200), h_c).await;
    }

    #[tokio::test(start_paused = true)]
    async fn ping_triggers_ack_reply() {
        let fab = TransportFabric::new();
        let (_det_a, sd_a, h_a) = spawn_node(&fab, "a", 7020, &[]).await;
        let probe_addr = addr(7021);
        let probe_transport = Arc::new(fab.bind(probe_addr).await);

        // Send a raw Ping from probe → a and wait for the Ack.
        probe_transport
            .send(
                addr(7020),
                SwimMessage::Ping(Ping {
                    probe_id: ProbeId::new(42),
                    from: NodeId::new("probe"),
                    incarnation: Incarnation::ZERO,
                    piggyback: vec![],
                }),
            )
            .await
            .unwrap();

        // Let the detector's recv arm fire.
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }

        let (from, msg) = tokio::time::timeout(Duration::from_millis(50), probe_transport.recv())
            .await
            .expect("recv did not time out")
            .expect("recv");
        assert_eq!(from, addr(7020));
        match msg {
            SwimMessage::Ack(ack) => assert_eq!(ack.probe_id, ProbeId::new(42)),
            other => panic!("expected Ack, got {other:?}"),
        }

        let _ = sd_a.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(100), h_a).await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_terminates_loop_promptly() {
        let fab = TransportFabric::new();
        let (_det_a, sd_a, h_a) = spawn_node(&fab, "a", 7030, &[]).await;
        let _ = sd_a.send(true);
        let joined = tokio::time::timeout(Duration::from_millis(100), h_a).await;
        assert!(joined.is_ok(), "detector did not shut down in time");
    }

    #[tokio::test(start_paused = true)]
    async fn bump_local_incarnation_is_monotonic() {
        let fab = TransportFabric::new();
        let (det_a, sd_a, h_a) = spawn_node(&fab, "a", 7040, &[]).await;
        let bumped = det_a.bump_local_incarnation(Incarnation::new(5)).await;
        assert!(bumped > Incarnation::new(5));
        let _ = sd_a.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(100), h_a).await;
    }
}
