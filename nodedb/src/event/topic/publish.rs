//! Publish a message to a durable topic.
//!
//! Creates a CdcEvent from the user payload and pushes it into the
//! topic's StreamBuffer (same buffer type used by change streams).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::control::state::SharedState;
use crate::event::cdc::buffer::StreamBuffer;
use crate::event::cdc::event::CdcEvent;
use crate::event::cdc::stream_def::RetentionConfig;

/// Publish a message to a durable topic.
///
/// Returns the sequence number assigned to the message.
pub fn publish_to_topic(
    state: &SharedState,
    tenant_id: u32,
    topic_name: &str,
    payload: &str,
) -> Result<u64, PublishError> {
    // Verify topic exists.
    let topic = state
        .ep_topic_registry
        .get(tenant_id, topic_name)
        .ok_or_else(|| PublishError::TopicNotFound(topic_name.to_string()))?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Parse payload as JSON (or wrap raw string in a JSON object).
    let value: serde_json::Value =
        serde_json::from_str(payload).unwrap_or_else(|_| serde_json::json!({"message": payload}));

    // Get or create the topic's buffer via the CdcRouter buffer pool.
    let buffer = get_or_create_topic_buffer(state, tenant_id, topic_name, &topic.retention);

    // Use buffer's total_pushed as monotonic sequence.
    let sequence = buffer.total_pushed() + 1;

    let event = CdcEvent {
        sequence,
        partition: 0, // Topics use a single partition (no vShard routing).
        collection: format!("topic:{topic_name}"),
        op: "PUBLISH".into(),
        row_id: format!("msg-{sequence}"),
        event_time: now_ms,
        lsn: now_ms, // Topics don't have WAL LSNs; use timestamp as monotonic ordering.
        tenant_id,
        new_value: Some(value),
        old_value: None,
        schema_version: 0,
    };

    buffer.push(event);
    Ok(sequence)
}

/// Get or create a StreamBuffer for a topic.
fn get_or_create_topic_buffer(
    state: &SharedState,
    tenant_id: u32,
    topic_name: &str,
    retention: &RetentionConfig,
) -> Arc<StreamBuffer> {
    // Topics use the CdcRouter's buffer pool with a "topic:" prefix
    // to avoid name collisions with change streams.
    let buffer_key = format!("topic:{topic_name}");

    if let Some(buf) = state.cdc_router.get_buffer(tenant_id, &buffer_key) {
        return buf;
    }

    // Create a new buffer. Use the router's internal mechanism.
    // Since CdcRouter.get_or_create_buffer is private, we route through
    // a dummy event to force buffer creation, then return it.
    // Instead, let's add a public create method to CdcRouter.
    // For now, use the public get_buffer after forcing creation.
    //
    // Actually, we can just create the buffer directly and register it.
    state
        .cdc_router
        .ensure_buffer(tenant_id, &buffer_key, retention)
}

#[derive(Debug)]
pub enum PublishError {
    TopicNotFound(String),
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TopicNotFound(t) => write!(f, "topic '{t}' does not exist"),
        }
    }
}
