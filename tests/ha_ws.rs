use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

/// Start a minimal stub HA WebSocket server that runs the auth handshake,
/// then calls `on_connected` with the authenticated stream.
/// Returns the bound ws:// URL.
pub async fn start_stub_ha<F, Fut>(on_connected: F) -> (String, tokio::task::JoinHandle<()>)
where
    F: FnOnce(tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let url = format!("ws://{addr}/api/websocket");

    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();

        // Send auth_required.
        ws.send(Message::Text(
            serde_json::json!({"type": "auth_required", "ha_version": "test"}).to_string(),
        ))
        .await
        .unwrap();

        // Read auth.
        let auth = ws.next().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(auth.to_text().unwrap()).unwrap();
        assert_eq!(v["type"], "auth");

        if v["access_token"] == "valid-token" {
            ws.send(Message::Text(
                serde_json::json!({"type": "auth_ok", "ha_version": "test"}).to_string(),
            ))
            .await
            .unwrap();
            on_connected(ws).await;
        } else {
            ws.send(Message::Text(
                serde_json::json!({"type": "auth_invalid", "message": "bad token"}).to_string(),
            ))
            .await
            .unwrap();
        }
    });

    (url, handle)
}

#[tokio::test]
async fn auth_succeeds_with_valid_token() {
    let (url, server) = start_stub_ha(|_ws| async move {
        // Keep the connection alive briefly.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    })
    .await;

    let ws = artnet_to_hass::ha_client::connect_and_authenticate(&url, "valid-token")
        .await
        .expect("auth should succeed");
    drop(ws);
    let _ = server.await;
}

#[tokio::test]
async fn auth_fails_with_bad_token() {
    let (url, server) = start_stub_ha(|_ws| async move {}).await;

    let err = artnet_to_hass::ha_client::connect_and_authenticate(&url, "bad-token")
        .await
        .expect_err("auth should fail");
    assert!(err.to_string().contains("auth_invalid"), "got: {err}");
    let _ = server.await;
}

#[tokio::test]
async fn sends_turn_on_and_receives_result() {
    use artnet_to_hass::ha_client::{connect_and_authenticate, HaConnection};

    let (url, server) = start_stub_ha(|mut ws| async move {
        // Expect a call_service message and respond with success.
        let frame = ws.next().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(v["type"], "call_service");
        assert_eq!(v["domain"], "light");
        assert_eq!(v["service"], "turn_on");
        assert_eq!(v["target"]["entity_id"], serde_json::json!(["light.a"]));
        assert_eq!(
            v["service_data"]["rgb_color"],
            serde_json::json!([10, 20, 30])
        );
        let id = v["id"].as_u64().unwrap();
        ws.send(Message::Text(
            serde_json::json!({"id": id, "type": "result", "success": true, "result": null})
                .to_string(),
        ))
        .await
        .unwrap();
    })
    .await;

    let ws = connect_and_authenticate(&url, "valid-token").await.unwrap();
    let conn = HaConnection::new(ws);
    let ok = conn
        .turn_on(&["light.a".into()], (10, 20, 30))
        .await
        .unwrap();
    assert!(ok);

    let _ = server.await;
}

#[tokio::test]
async fn send_reports_false_on_ha_failure() {
    use artnet_to_hass::ha_client::{connect_and_authenticate, HaConnection};

    let (url, server) = start_stub_ha(|mut ws| async move {
        let frame = ws.next().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        let id = v["id"].as_u64().unwrap();
        ws.send(Message::Text(
            serde_json::json!({
                "id": id, "type": "result", "success": false,
                "error": {"code": "not_found", "message": "entity not found"}
            })
            .to_string(),
        ))
        .await
        .unwrap();
    })
    .await;

    let ws = connect_and_authenticate(&url, "valid-token").await.unwrap();
    let conn = HaConnection::new(ws);
    let ok = conn.turn_off(&["light.nope".into()]).await.unwrap();
    assert!(!ok);

    let _ = server.await;
}

/// Regression test for the 2026-05-30 production wedge: a server that accepts
/// TCP but never completes the WebSocket/auth handshake hung
/// `connect_and_authenticate` forever, freezing the reconnect loop for 12 days.
/// The client must return Err on its own within its internal timeout.
#[tokio::test]
async fn connect_errors_against_unresponsive_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("ws://{addr}/api/websocket");

    // Accept connections, hold them open, never respond.
    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let _hold = stream;
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            });
        }
    });

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        artnet_to_hass::ha_client::connect_and_authenticate(&url, "any-token"),
    )
    .await;

    server.abort();
    let inner = result.expect(
        "connect_and_authenticate hung past 15s — missing internal timeout \
         (regression of the 2026-05-30 production wedge)",
    );
    assert!(inner.is_err(), "expected Err from unresponsive server");
}

/// Same hang class on the send path: server auths fine but never answers a
/// call_service. `turn_on` must return Err on its own, so the reconnect logic
/// can invalidate the connection instead of blocking forever.
#[tokio::test]
async fn send_errors_when_result_never_arrives() {
    use artnet_to_hass::ha_client::{connect_and_authenticate, HaConnection};

    let (url, server) = start_stub_ha(|mut ws| async move {
        // Read the call_service but never reply; keep the socket open.
        let _ = ws.next().await;
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    })
    .await;

    let ws = connect_and_authenticate(&url, "valid-token").await.unwrap();
    let conn = HaConnection::new(ws);

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        conn.turn_on(&["light.a".into()], (1, 2, 3)),
    )
    .await;

    server.abort();
    let inner = result.expect(
        "turn_on hung past 15s waiting for a result frame — missing internal timeout",
    );
    assert!(inner.is_err(), "expected Err when result never arrives");
}
