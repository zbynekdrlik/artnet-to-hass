use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{error, info, warn};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Build the HA WebSocket `call_service` JSON for `light.turn_on` with rgb_color.
pub fn build_turn_on(id: u64, lights: &[String], rgb: (u8, u8, u8)) -> Value {
    json!({
        "id": id,
        "type": "call_service",
        "domain": "light",
        "service": "turn_on",
        "target": {"entity_id": lights},
        "service_data": {"rgb_color": [rgb.0, rgb.1, rgb.2]}
    })
}

/// Build the HA WebSocket `call_service` JSON for `light.turn_off`.
pub fn build_turn_off(id: u64, lights: &[String]) -> Value {
    json!({
        "id": id,
        "type": "call_service",
        "domain": "light",
        "service": "turn_off",
        "target": {"entity_id": lights}
    })
}

/// Build the HA WebSocket auth message.
pub fn build_auth(token: &str) -> Value {
    json!({"type": "auth", "access_token": token})
}

/// Connect to HA and perform the auth handshake. Returns the authenticated WS stream.
///
/// HA sends `auth_required` first, then we send `auth`, then HA responds `auth_ok`
/// or `auth_invalid`. `auth_invalid` is fatal.
pub async fn connect_and_authenticate(url: &str, token: &str) -> Result<WsStream> {
    let (mut ws, _resp) = tokio_tungstenite::connect_async(url)
        .await
        .with_context(|| format!("connecting to {url}"))?;

    // 1. Expect auth_required.
    let msg = ws
        .next()
        .await
        .ok_or_else(|| anyhow!("connection closed before auth_required"))?
        .context("reading auth_required")?;
    let text = msg.to_text().context("auth_required not text")?;
    let v: serde_json::Value = serde_json::from_str(text).context("auth_required not JSON")?;
    if v.get("type").and_then(|t| t.as_str()) != Some("auth_required") {
        return Err(anyhow!("expected auth_required, got {v}"));
    }

    // 2. Send auth.
    let auth = build_auth(token);
    ws.send(Message::Text(auth.to_string()))
        .await
        .context("sending auth")?;

    // 3. Expect auth_ok or auth_invalid.
    let msg = ws
        .next()
        .await
        .ok_or_else(|| anyhow!("connection closed before auth result"))?
        .context("reading auth result")?;
    let text = msg.to_text().context("auth result not text")?;
    let v: serde_json::Value = serde_json::from_str(text).context("auth result not JSON")?;
    match v.get("type").and_then(|t| t.as_str()) {
        Some("auth_ok") => Ok(ws),
        Some("auth_invalid") => Err(anyhow!(
            "auth_invalid: {}",
            v.get("message").and_then(|m| m.as_str()).unwrap_or("")
        )),
        other => Err(anyhow!("unexpected auth response: {:?} — {v}", other)),
    }
}

/// Send a pre-built message with the given id and await the matching `result`.
/// Returns `Ok(true)` if HA reports success, `Ok(false)` if success=false, `Err` on I/O failure.
pub async fn send_and_await_result(ws: &mut WsStream, msg: &serde_json::Value) -> Result<bool> {
    let id = msg
        .get("id")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("message missing numeric id"))?;

    ws.send(Message::Text(msg.to_string()))
        .await
        .context("send call_service")?;

    // Read messages until we get a `result` with matching id.
    // HA may send `event` messages between result frames; we ignore non-result.
    loop {
        let frame = ws
            .next()
            .await
            .ok_or_else(|| anyhow!("WS closed while waiting for result id={id}"))?
            .context("reading WS frame")?;
        let text = match &frame {
            Message::Text(t) => t.clone(),
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => return Err(anyhow!("WS closed (close frame)")),
            Message::Binary(_) => continue,
            Message::Frame(_) => continue,
        };
        let v: serde_json::Value = serde_json::from_str(&text).context("result not JSON")?;
        if v.get("type").and_then(|t| t.as_str()) != Some("result") {
            continue;
        }
        if v.get("id").and_then(|i| i.as_u64()) != Some(id) {
            continue;
        }
        return Ok(v.get("success").and_then(|s| s.as_bool()).unwrap_or(false));
    }
}

/// Owned, thread-safe HA connection plus id counter.
pub struct HaConnection {
    ws: Arc<Mutex<WsStream>>,
    next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl HaConnection {
    pub fn new(ws: WsStream) -> Self {
        Self {
            ws: Arc::new(Mutex::new(ws)),
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }
    fn next_id(&self) -> u64 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
    pub async fn turn_on(&self, lights: &[String], rgb: (u8, u8, u8)) -> Result<bool> {
        let msg = build_turn_on(self.next_id(), lights, rgb);
        let mut ws = self.ws.lock().await;
        send_and_await_result(&mut ws, &msg).await
    }
    pub async fn turn_off(&self, lights: &[String]) -> Result<bool> {
        let msg = build_turn_off(self.next_id(), lights);
        let mut ws = self.ws.lock().await;
        send_and_await_result(&mut ws, &msg).await
    }
}

#[async_trait]
pub trait HaClient: Send + Sync {
    async fn turn_on(&self, rgb: (u8, u8, u8)) -> Result<()>;
    async fn turn_off(&self) -> Result<()>;
}

/// A long-lived HA client that maintains a WS connection and reconnects on failure.
///
/// Send operations return `Err` immediately when disconnected — no queueing.
/// A background task owns the reconnect loop.
pub struct ReconnectingHaClient {
    lights: Vec<String>,
    conn: Arc<RwLock<Option<HaConnection>>>,
}

impl ReconnectingHaClient {
    /// Spawn a background task that connects, authenticates, and reconnects forever.
    /// Returns the client; the connection becomes available asynchronously.
    pub fn spawn(url: String, token: String, lights: Vec<String>) -> Arc<Self> {
        let client = Arc::new(Self {
            lights,
            conn: Arc::new(RwLock::new(None)),
        });
        let bg = client.clone();
        tokio::spawn(async move {
            bg.run_reconnect_loop(url, token).await;
        });
        client
    }

    async fn run_reconnect_loop(self: Arc<Self>, url: String, token: String) {
        let mut backoff = Duration::from_secs(1);
        const MAX_BACKOFF: Duration = Duration::from_secs(30);
        loop {
            info!("HA: connecting to {}", url);
            match connect_and_authenticate(&url, &token).await {
                Ok(ws) => {
                    info!("HA: authenticated");
                    *self.conn.write().await = Some(HaConnection::new(ws));
                    backoff = Duration::from_secs(1);
                    // Hold here until the next send fails. We detect that by
                    // clearing `conn` from within the send path and observing the
                    // state change. Simple approach: poll.
                    loop {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        if self.conn.read().await.is_none() {
                            break;
                        }
                    }
                    warn!("HA: connection lost, will reconnect");
                }
                Err(e) if is_fatal_auth(&e) => {
                    error!("HA: fatal auth failure, exiting: {e}");
                    std::process::exit(2);
                }
                Err(e) => {
                    warn!("HA: connect failed ({e}), retrying in {:?}", backoff);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    async fn invalidate(&self) {
        *self.conn.write().await = None;
    }
}

fn is_fatal_auth(e: &anyhow::Error) -> bool {
    e.to_string().contains("auth_invalid")
}

#[async_trait]
impl HaClient for ReconnectingHaClient {
    async fn turn_on(&self, rgb: (u8, u8, u8)) -> Result<()> {
        let guard = self.conn.read().await;
        let Some(conn) = guard.as_ref() else {
            return Err(anyhow!("HA disconnected"));
        };
        match conn.turn_on(&self.lights, rgb).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(anyhow!("HA returned success=false for turn_on")),
            Err(e) => {
                drop(guard);
                self.invalidate().await;
                Err(e)
            }
        }
    }

    async fn turn_off(&self) -> Result<()> {
        let guard = self.conn.read().await;
        let Some(conn) = guard.as_ref() else {
            return Err(anyhow!("HA disconnected"));
        };
        match conn.turn_off(&self.lights).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(anyhow!("HA returned success=false for turn_off")),
            Err(e) => {
                drop(guard);
                self.invalidate().await;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_on_matches_ha_protocol() {
        let lights = vec!["light.a".into(), "light.b".into()];
        let msg = build_turn_on(7, &lights, (255, 80, 0));
        assert_eq!(
            msg,
            json!({
                "id": 7,
                "type": "call_service",
                "domain": "light",
                "service": "turn_on",
                "target": {"entity_id": ["light.a", "light.b"]},
                "service_data": {"rgb_color": [255, 80, 0]}
            })
        );
    }

    #[test]
    fn turn_off_matches_ha_protocol() {
        let lights = vec!["light.a".into()];
        let msg = build_turn_off(9, &lights);
        assert_eq!(
            msg,
            json!({
                "id": 9,
                "type": "call_service",
                "domain": "light",
                "service": "turn_off",
                "target": {"entity_id": ["light.a"]}
            })
        );
    }

    #[test]
    fn auth_message_format() {
        let msg = build_auth("abc.def.ghi");
        assert_eq!(msg, json!({"type": "auth", "access_token": "abc.def.ghi"}));
    }
}
