//! Sync wire protocol — re-exports from `nodedb-types`.
//!
//! All wire types are defined in `nodedb-types::sync::wire` so that both
//! Origin and NodeDB-Lite share identical serialization. This module
//! re-exports them for backwards-compatible use within the Origin codebase.

// ── Re-export all wire types from nodedb-types ──
pub use nodedb_types::sync::wire::{
    DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg, HandshakeAckMsg, HandshakeMsg, PeerPresence,
    PingPongMsg, PresenceBroadcastMsg, PresenceLeaveMsg, PresenceUpdateMsg, ResyncReason,
    ResyncRequestMsg, ShapeDeltaMsg, ShapeSnapshotMsg, ShapeSubscribeMsg, ShapeUnsubscribeMsg,
    SyncFrame, SyncMessageType, ThrottleMsg, TimeseriesAckMsg, TimeseriesPushMsg,
    TokenRefreshAckMsg, TokenRefreshMsg, VectorClockSyncMsg,
};

// ── Re-export CompensationHint (used by dlq.rs and session.rs) ──
pub use nodedb_types::sync::compensation::CompensationHint;
