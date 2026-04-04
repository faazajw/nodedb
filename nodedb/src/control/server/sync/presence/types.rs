//! Presence subsystem configuration and internal types.

use std::time::Instant;

/// Configuration for the presence subsystem.
#[derive(Debug, Clone)]
pub struct PresenceConfig {
    /// TTL in milliseconds: if no `PresenceUpdate` received within this window,
    /// the peer is automatically removed and a `PresenceLeave` is broadcast.
    pub ttl_ms: u64,
    /// Interval in milliseconds between sweeps that check for expired peers.
    pub sweep_interval_ms: u64,
    /// Maximum number of channels per connection (prevents resource exhaustion).
    pub max_channels_per_session: usize,
    /// Maximum number of subscribers per channel.
    pub max_subscribers_per_channel: usize,
}

impl Default for PresenceConfig {
    fn default() -> Self {
        Self {
            ttl_ms: 30_000,
            sweep_interval_ms: 5_000,
            max_channels_per_session: 64,
            max_subscribers_per_channel: 1024,
        }
    }
}

/// Internal representation of a single peer's presence within a channel.
#[derive(Debug, Clone)]
pub struct PeerState {
    /// User identifier (from authenticated identity).
    pub user_id: String,
    /// Session ID of the WebSocket connection (for disconnect cleanup).
    pub session_id: String,
    /// Opaque application-defined state (MessagePack bytes).
    pub state: Vec<u8>,
    /// When this peer last sent a `PresenceUpdate`.
    pub last_seen: Instant,
}

impl PeerState {
    /// Milliseconds since the last update.
    pub fn elapsed_ms(&self) -> u64 {
        self.last_seen.elapsed().as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = PresenceConfig::default();
        assert_eq!(cfg.ttl_ms, 30_000);
        assert_eq!(cfg.sweep_interval_ms, 5_000);
        assert_eq!(cfg.max_channels_per_session, 64);
        assert_eq!(cfg.max_subscribers_per_channel, 1024);
    }

    #[test]
    fn peer_state_elapsed() {
        let peer = PeerState {
            user_id: "u1".into(),
            session_id: "s1".into(),
            state: vec![],
            last_seen: Instant::now(),
        };
        // Should be very close to 0.
        assert!(peer.elapsed_ms() < 100);
    }
}
