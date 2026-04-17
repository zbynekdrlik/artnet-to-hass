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
