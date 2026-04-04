//! PresenceManager: channel subscription, broadcast fan-out, TTL sweeping.
//!
//! Lives entirely in the Control Plane (Tokio, `Send + Sync`). No persistence,
//! no WAL, no SPSC bridge — presence is ephemeral, in-memory only.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use nodedb_types::sync::wire::{
    PresenceBroadcastMsg, PresenceLeaveMsg, PresenceUpdateMsg, SyncFrame, SyncMessageType,
};

use super::channel::ChannelState;
use super::types::{PeerState, PresenceConfig};

/// Handle for sending outbound frames to a specific WebSocket session.
///
/// The manager holds one sender per connected session. When presence state
/// changes, the manager fans out broadcast frames through these senders.
#[derive(Debug, Clone)]
pub struct SessionSender {
    /// Bounded channel sender — if the buffer is full, the oldest presence
    /// update is dropped (acceptable for ephemeral data).
    tx: mpsc::Sender<SyncFrame>,
}

impl SessionSender {
    pub fn new(tx: mpsc::Sender<SyncFrame>) -> Self {
        Self { tx }
    }

    /// Try to send a frame. Returns `false` if the channel is full or closed.
    pub fn try_send(&self, frame: SyncFrame) -> bool {
        self.tx.try_send(frame).is_ok()
    }
}

/// Central presence manager for all channels on this node.
///
/// Thread-safe: wrapped in `Arc<tokio::sync::RwLock<_>>` by the caller
/// (the sync listener). The RwLock allows concurrent reads (broadcast fan-out)
/// with exclusive writes (upsert/remove/sweep).
pub struct PresenceManager {
    /// All active channels: `channel_name → ChannelState`.
    channels: HashMap<String, ChannelState>,
    /// Reverse index: `session_id → set of channel names` the session is in.
    /// Used for efficient cleanup on disconnect.
    session_channels: HashMap<String, HashSet<String>>,
    /// Outbound frame senders: `session_id → SessionSender`.
    senders: HashMap<String, SessionSender>,
    /// Configuration.
    config: PresenceConfig,
}

impl PresenceManager {
    pub fn new(config: PresenceConfig) -> Self {
        Self {
            channels: HashMap::new(),
            session_channels: HashMap::new(),
            senders: HashMap::new(),
            config,
        }
    }

    /// Register a session's outbound sender. Called when a WebSocket connects.
    pub fn register_session(&mut self, session_id: String, sender: SessionSender) {
        self.senders.insert(session_id, sender);
    }

    /// Unregister a session and remove it from all channels.
    ///
    /// Returns `PresenceLeaveMsg`s to broadcast for each channel the session
    /// was in. The caller is responsible for broadcasting these (or they can
    /// be ignored if the caller uses `broadcast_leave` internally).
    pub fn unregister_session(&mut self, session_id: &str) -> Vec<PresenceLeaveMsg> {
        self.senders.remove(session_id);
        let channel_names = match self.session_channels.remove(session_id) {
            Some(names) => names,
            None => return Vec::new(),
        };

        // Collect leaves and empty channels first (avoids borrow conflict).
        let mut leaves = Vec::new();
        let mut empty_channels = Vec::new();
        for channel_name in &channel_names {
            if let Some(channel) = self.channels.get_mut(channel_name) {
                if let Some(leave) = channel.remove_peer(session_id) {
                    leaves.push((channel_name.clone(), leave));
                }
                if channel.is_empty() {
                    empty_channels.push(channel_name.clone());
                }
            }
        }

        // Broadcast leaves after releasing mutable borrow on individual channels.
        for (channel_name, leave) in &leaves {
            self.broadcast_leave_to_channel(channel_name, leave);
        }

        // Garbage collect empty channels.
        for name in &empty_channels {
            self.channels.remove(name);
        }

        leaves.into_iter().map(|(_, leave)| leave).collect()
    }

    /// Handle a `PresenceUpdate` from a client.
    ///
    /// Upserts the peer in the channel and broadcasts the updated peer list
    /// to all other subscribers.
    pub fn handle_update(&mut self, session_id: &str, user_id: &str, msg: &PresenceUpdateMsg) {
        // Enforce max channels per session.
        let session_channel_count = self.session_channels.get(session_id).map_or(0, |s| s.len());
        let is_new_channel = !self
            .session_channels
            .get(session_id)
            .is_some_and(|s| s.contains(&msg.channel));

        if is_new_channel && session_channel_count >= self.config.max_channels_per_session {
            warn!(
                session = session_id,
                channel = %msg.channel,
                limit = self.config.max_channels_per_session,
                "presence: max channels per session exceeded, ignoring"
            );
            return;
        }

        // Enforce max subscribers per channel.
        if is_new_channel
            && let Some(ch) = self.channels.get(&msg.channel)
            && ch.peer_count() >= self.config.max_subscribers_per_channel
        {
            warn!(
                session = session_id,
                channel = %msg.channel,
                limit = self.config.max_subscribers_per_channel,
                "presence: max subscribers per channel exceeded, ignoring"
            );
            return;
        }

        let peer = PeerState {
            user_id: user_id.to_owned(),
            session_id: session_id.to_owned(),
            state: msg.state.clone(),
            last_seen: Instant::now(),
        };

        // Upsert into channel.
        let channel = self
            .channels
            .entry(msg.channel.clone())
            .or_insert_with(|| ChannelState::new(msg.channel.clone()));
        let broadcast = channel.upsert_peer(session_id, peer);

        // Track reverse mapping.
        self.session_channels
            .entry(session_id.to_owned())
            .or_default()
            .insert(msg.channel.clone());

        // Fan out broadcast to all subscribers EXCEPT the sender.
        self.broadcast_to_channel(&msg.channel, session_id, &broadcast);

        debug!(
            session = session_id,
            channel = %msg.channel,
            peers = broadcast.peers.len(),
            "presence: update broadcast"
        );
    }

    /// Sweep all channels for expired peers (TTL exceeded).
    ///
    /// Called periodically by a Tokio timer task. Returns the total number
    /// of peers evicted.
    pub fn sweep_expired(&mut self) -> usize {
        let ttl = self.config.ttl_ms;
        let mut total_evicted = 0;
        let mut empty_channels = Vec::new();
        let mut all_leaves: Vec<(String, PresenceLeaveMsg)> = Vec::new();

        for (channel_name, channel) in &mut self.channels {
            let leaves = channel.sweep_expired(ttl);
            total_evicted += leaves.len();
            for leave in leaves {
                all_leaves.push((channel_name.clone(), leave));
            }
            if channel.is_empty() {
                empty_channels.push(channel_name.clone());
            }
        }

        // Broadcast leaves after releasing mutable borrow on channels.
        for (channel_name, leave) in &all_leaves {
            self.broadcast_leave_to_channel(channel_name, leave);
        }

        // Garbage collect empty channels.
        for name in &empty_channels {
            self.channels.remove(name);
        }

        // Clean up session_channels for sessions that no longer appear in any channel.
        if total_evicted > 0 {
            let channels_ref = &self.channels;
            self.session_channels.retain(|session_id, session_chs| {
                session_chs.retain(|ch_name| {
                    channels_ref
                        .get(ch_name)
                        .is_some_and(|ch| ch.has_session(session_id))
                });
                !session_chs.is_empty()
            });
        }

        if total_evicted > 0 {
            info!(evicted = total_evicted, "presence: TTL sweep complete");
        }

        total_evicted
    }

    /// Number of active channels.
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Total peers across all channels.
    pub fn total_peers(&self) -> usize {
        self.channels.values().map(|ch| ch.peer_count()).sum()
    }

    /// Broadcast a `PresenceBroadcastMsg` to all subscribers of a channel
    /// except the sender.
    fn broadcast_to_channel(
        &self,
        channel_name: &str,
        exclude_session: &str,
        msg: &PresenceBroadcastMsg,
    ) {
        let Some(channel) = self.channels.get(channel_name) else {
            return;
        };
        let frame = SyncFrame::encode_or_empty(SyncMessageType::PresenceBroadcast, msg);
        for session_id in channel.session_ids() {
            if session_id == exclude_session {
                continue;
            }
            if let Some(sender) = self.senders.get(session_id)
                && !sender.try_send(frame.clone())
            {
                debug!(
                    session = session_id,
                    channel = channel_name,
                    "presence: send buffer full, dropping broadcast"
                );
            }
        }
    }

    /// Broadcast a `PresenceLeaveMsg` to all remaining subscribers of a channel.
    fn broadcast_leave_to_channel(&self, channel_name: &str, leave: &PresenceLeaveMsg) {
        let Some(channel) = self.channels.get(channel_name) else {
            return;
        };
        let frame = SyncFrame::encode_or_empty(SyncMessageType::PresenceLeave, leave);
        for session_id in channel.session_ids() {
            if let Some(sender) = self.senders.get(session_id)
                && !sender.try_send(frame.clone())
            {
                debug!(
                    session = session_id,
                    channel = channel_name,
                    "presence: send buffer full, dropping leave"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (
        PresenceManager,
        mpsc::Receiver<SyncFrame>,
        mpsc::Receiver<SyncFrame>,
    ) {
        let config = PresenceConfig::default();
        let mut mgr = PresenceManager::new(config);

        let (tx1, rx1) = mpsc::channel(64);
        let (tx2, rx2) = mpsc::channel(64);
        mgr.register_session("s1".into(), SessionSender::new(tx1));
        mgr.register_session("s2".into(), SessionSender::new(tx2));

        (mgr, rx1, rx2)
    }

    #[test]
    fn update_broadcasts_to_others() {
        let (mut mgr, _rx1, mut rx2) = setup();

        let msg = PresenceUpdateMsg {
            channel: "doc:d1".into(),
            state: vec![0x01],
        };
        mgr.handle_update("s1", "alice", &msg);

        // s2 should NOT receive a broadcast yet (s2 is not in the channel).
        assert!(rx2.try_recv().is_err());

        // Now s2 joins.
        let msg2 = PresenceUpdateMsg {
            channel: "doc:d1".into(),
            state: vec![0x02],
        };
        mgr.handle_update("s2", "bob", &msg2);

        // s1 should NOT receive because the broadcast from s2's update goes
        // to all EXCEPT s2. But s1 IS in the channel now, so s1 gets it.
        // (rx1 was captured as _rx1, we can't read it — that's fine for this test.)

        // Update s1 again — s2 should now receive.
        mgr.handle_update("s1", "alice", &msg);
        let frame = rx2.try_recv().unwrap();
        assert_eq!(frame.msg_type, SyncMessageType::PresenceBroadcast);
        let broadcast: PresenceBroadcastMsg = frame.decode_body().unwrap();
        assert_eq!(broadcast.channel, "doc:d1");
        assert_eq!(broadcast.peers.len(), 2);
    }

    #[test]
    fn unregister_broadcasts_leave() {
        let (mut mgr, _rx1, mut rx2) = setup();

        // Both join the same channel.
        mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![],
            },
        );
        mgr.handle_update(
            "s2",
            "bob",
            &PresenceUpdateMsg {
                channel: "doc:d1".into(),
                state: vec![],
            },
        );
        // Drain any broadcast frames from joining.
        while rx2.try_recv().is_ok() {}

        // Disconnect s1.
        let leaves = mgr.unregister_session("s1");
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].user_id, "alice");

        // s2 should receive a PresenceLeave.
        let frame = rx2.try_recv().unwrap();
        assert_eq!(frame.msg_type, SyncMessageType::PresenceLeave);
        let leave: PresenceLeaveMsg = frame.decode_body().unwrap();
        assert_eq!(leave.user_id, "alice");
    }

    #[test]
    fn max_channels_per_session_enforced() {
        let config = PresenceConfig {
            max_channels_per_session: 2,
            ..Default::default()
        };
        let mut mgr = PresenceManager::new(config);
        let (tx, _rx) = mpsc::channel(64);
        mgr.register_session("s1".into(), SessionSender::new(tx));

        mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "ch1".into(),
                state: vec![],
            },
        );
        mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "ch2".into(),
                state: vec![],
            },
        );
        mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "ch3".into(),
                state: vec![],
            },
        );

        // Only 2 channels should exist for this session.
        assert_eq!(mgr.session_channels.get("s1").unwrap().len(), 2);
    }

    #[test]
    fn sweep_expired_peers() {
        let config = PresenceConfig {
            ttl_ms: 100, // Very short TTL for testing.
            ..Default::default()
        };
        let mut mgr = PresenceManager::new(config);
        let (tx, _rx) = mpsc::channel(64);
        mgr.register_session("s1".into(), SessionSender::new(tx));

        // Insert a peer with old timestamp.
        let channel = mgr
            .channels
            .entry("doc:d1".into())
            .or_insert_with(|| ChannelState::new("doc:d1".into()));
        channel.upsert_peer(
            "s1",
            PeerState {
                user_id: "alice".into(),
                session_id: "s1".into(),
                state: vec![],
                last_seen: Instant::now() - std::time::Duration::from_secs(1),
            },
        );
        mgr.session_channels
            .entry("s1".into())
            .or_default()
            .insert("doc:d1".into());

        assert_eq!(mgr.total_peers(), 1);
        let evicted = mgr.sweep_expired();
        assert_eq!(evicted, 1);
        assert_eq!(mgr.total_peers(), 0);
        assert_eq!(mgr.channel_count(), 0); // Empty channel garbage collected.
    }

    #[test]
    fn empty_after_all_disconnect() {
        let (mut mgr, _, _) = setup();
        mgr.handle_update(
            "s1",
            "alice",
            &PresenceUpdateMsg {
                channel: "ch1".into(),
                state: vec![],
            },
        );
        mgr.handle_update(
            "s2",
            "bob",
            &PresenceUpdateMsg {
                channel: "ch1".into(),
                state: vec![],
            },
        );

        mgr.unregister_session("s1");
        mgr.unregister_session("s2");

        assert_eq!(mgr.channel_count(), 0);
        assert_eq!(mgr.total_peers(), 0);
    }
}
