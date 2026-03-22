//! Live sync tests against a running Origin server.
//!
//! Run: `cargo run -p nodedb-lite --example live_sync`
//! Requires: Origin with sync endpoint on ws://127.0.0.1:9090
//!   Start Origin: `RUST_LOG=info cargo run --release -p nodedb`

use std::time::Instant;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::*;

const ORIGIN_WS: &str = "ws://127.0.0.1:9090";

async fn connect_and_handshake()
-> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let (mut ws, _) = tokio_tungstenite::connect_async(ORIGIN_WS)
        .await
        .expect("connect to Origin");

    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "live-test".into(),
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::Handshake, &hs)
            .to_bytes()
            .into(),
    ))
    .await
    .unwrap();

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let ack: HandshakeAckMsg = SyncFrame::from_bytes(resp.into_data().as_ref())
        .unwrap()
        .decode_body()
        .unwrap();
    assert!(ack.success, "handshake failed: {:?}", ack.error);
    ws
}

#[tokio::main]
async fn main() {
    println!("=== Live Sync Tests (Origin at {ORIGIN_WS}) ===\n");

    if tokio_tungstenite::connect_async(ORIGIN_WS).await.is_err() {
        println!("SKIP: Origin not available at {ORIGIN_WS}");
        println!("Start: RUST_LOG=info cargo run --release -p nodedb");
        return;
    }

    let mut passed = 0u32;
    let mut failed = 0u32;

    macro_rules! run_test {
        ($name:expr, $body:expr) => {
            print!("test {} ... ", $name);
            match $body.await {
                Ok(()) => {
                    println!("ok");
                    passed += 1;
                }
                Err(e) => {
                    println!("FAILED: {e}");
                    failed += 1;
                }
            }
        };
    }

    run_test!("handshake", test_handshake());
    run_test!("delta_push", test_delta_push());
    run_test!("ping_pong", test_ping_pong());
    run_test!("reconnect_under_200ms", test_reconnect_latency());
    run_test!("vector_clock_sync", test_clock_sync());
    run_test!("shape_subscribe", test_shape_subscribe());

    println!("\nresult: {passed} passed; {failed} failed");
    if failed > 0 {
        std::process::exit(1);
    }
}

async fn test_handshake() -> Result<(), String> {
    let (mut ws, _) = tokio_tungstenite::connect_async(ORIGIN_WS)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "test-handshake".into(),
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::Handshake, &hs)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::HandshakeAck {
        return Err(format!("expected HandshakeAck, got {:?}", frame.msg_type));
    }
    let ack: HandshakeAckMsg = frame.decode_body().ok_or("decode")?;
    if !ack.success {
        return Err(format!("rejected: {:?}", ack.error));
    }
    Ok(())
}

async fn test_delta_push() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    let delta = DeltaPushMsg {
        collection: "live_test".into(),
        document_id: "d1".into(),
        delta: rmp_serde::to_vec_named(&serde_json::json!({"key": "value"}))
            .map_err(|e| format!("serialize: {e}"))?,
        peer_id: 42,
        mutation_id: 1,
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::DeltaPush, &delta)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::DeltaAck && frame.msg_type != SyncMessageType::DeltaReject
    {
        return Err(format!("unexpected: {:?}", frame.msg_type));
    }
    Ok(())
}

async fn test_ping_pong() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    let ping = PingPongMsg {
        timestamp_ms: 123456789,
        is_pong: false,
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::PingPong, &ping)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::PingPong {
        return Err(format!("expected PingPong, got {:?}", frame.msg_type));
    }
    let pong: PingPongMsg = frame.decode_body().ok_or("decode")?;
    if !pong.is_pong {
        return Err("expected is_pong=true".into());
    }
    if pong.timestamp_ms != 123456789 {
        return Err(format!("wrong timestamp: {}", pong.timestamp_ms));
    }
    Ok(())
}

async fn test_reconnect_latency() -> Result<(), String> {
    let start = Instant::now();
    let _ws = connect_and_handshake().await;
    let elapsed = start.elapsed();
    if elapsed.as_millis() >= 200 {
        return Err(format!("took {}ms, target < 200ms", elapsed.as_millis()));
    }
    Ok(())
}

async fn test_clock_sync() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    let clock = VectorClockSyncMsg {
        clocks: {
            let mut m = std::collections::HashMap::new();
            m.insert("0000000000000001".to_string(), 42u64);
            m
        },
        sender_id: 1,
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::VectorClockSync, &clock)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::VectorClockSync {
        return Err(format!(
            "expected VectorClockSync, got {:?}",
            frame.msg_type
        ));
    }
    Ok(())
}

async fn test_shape_subscribe() -> Result<(), String> {
    let mut ws = connect_and_handshake().await;

    // Subscribe to a document shape.
    let subscribe = ShapeSubscribeMsg {
        shape: nodedb_types::sync::shape::ShapeDefinition {
            shape_id: "test-shape".into(),
            tenant_id: 0,
            shape_type: nodedb_types::sync::shape::ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "test".into(),
        },
    };
    ws.send(Message::Binary(
        SyncFrame::encode_or_empty(SyncMessageType::ShapeSubscribe, &subscribe)
            .to_bytes()
            .into(),
    ))
    .await
    .map_err(|e| format!("send: {e}"))?;

    // Should get ShapeSnapshot back.
    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout")?
        .ok_or("closed")?
        .map_err(|e| format!("read: {e}"))?;

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).ok_or("bad frame")?;
    if frame.msg_type != SyncMessageType::ShapeSnapshot {
        return Err(format!("expected ShapeSnapshot, got {:?}", frame.msg_type));
    }

    let snapshot: ShapeSnapshotMsg = frame.decode_body().ok_or("decode")?;
    if snapshot.shape_id != "test-shape" {
        return Err(format!("wrong shape_id: {}", snapshot.shape_id));
    }

    Ok(())
}
