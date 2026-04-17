# artnet-to-hass — Design

**Date:** 2026-04-17
**Status:** Approved for implementation planning

## 1. Purpose

Receive Art-Net DMX from a Blinder Kitten lighting console and drive a set of Home Assistant lights in real time, low-latency, for live performance use.

## 2. Constraints & decisions

| Area | Decision |
|---|---|
| Language | Rust (async, `tokio`) |
| Art-Net source | Universe 0, channels 1/2/3 = R/G/B |
| Home Assistant transport | WebSocket API (`/api/websocket`) |
| HA host | `ha-snv.local:8123` |
| Target lights | `light.h70a1_1e41`, `light.h70a1_1e86` (2× Govee WiFi) |
| Update strategy | Change-detect + rate cap, default 10 Hz (`MIN_SEND_INTERVAL_MS=100`) |
| Black handling | RGB `(0,0,0)` → `light.turn_off`; any non-zero → `light.turn_on` with `rgb_color` |
| Dev machine | Linux x86_64 |
| Prod machine | Linux x86_64 (same arch — single binary, no cross-compile) |
| Secrets | Long-lived HA token in `.env` (gitignored), never committed |
| Deployment | `systemd` service on prod |

Rationale for WebSocket: REST adds per-call TCP handshake (~20–50 ms) vs. WebSocket's 5–15 ms on an open socket. For live performance, WebSocket is the only real choice.

Rationale for 10 Hz cap: Govee WiFi via HA handles ~5 Hz reliably; 10 Hz is the upper end, tunable. Govee's light response (50–200 ms) dominates end-to-end latency — app overhead is negligible.

## 3. Architecture

```
┌─────────────────┐    UDP:6454         ┌──────────────────────────────────┐
│ Kitten console  │ ─── ArtDmx ────────▶│  artnet-to-hass (Rust, Tokio)    │
└─────────────────┘    (~40 Hz)         │                                  │
                                        │  ┌──────────┐   watch    ┌─────┐ │
                                        │  │ listener │──(R,G,B)──▶│ send│ │
                                        │  └──────────┘  channel   └──┬──┘ │
                                        └──────────────────────────────┼───┘
                                                                       │ WS
                                                                       ▼
                                                      ┌──────────────────────┐
                                                      │ ha-snv.local:8123    │
                                                      │  light.turn_on/off   │
                                                      │  → 2 Govee WiFi      │
                                                      └──────────────────────┘
```

- Single binary, two async tasks.
- Inter-task communication: `tokio::sync::watch<Option<(u8,u8,u8)>>`.
- Watch coalesces bursts — under a rapid fade, only the latest RGB survives. No queue, no backlog.

## 4. Components (modules)

```
src/
├── main.rs          # wire everything up, load config, spawn tasks
├── config.rs        # Config struct, load from .env + env vars
├── artnet.rs        # UDP listener + ArtDmx parser
├── ha_client.rs     # WebSocket client, auth, service calls, reconnect
└── bridge.rs        # rate-cap + change-detect loop
```

### 4.1 `config.rs`

Loaded via `dotenvy` + `std::env` at startup. Fails fast on missing required vars.

| Variable | Default | Purpose |
|---|---|---|
| `HA_URL` | _required_ | e.g. `ws://ha-snv.local:8123/api/websocket` |
| `HA_TOKEN` | _required_ | HA long-lived access token |
| `HA_LIGHTS` | _required_ | Comma-separated entity IDs |
| `ARTNET_UNIVERSE` | `0` | Universe to listen on |
| `ARTNET_BIND` | `0.0.0.0:6454` | UDP bind address |
| `RGB_START_CHANNEL` | `1` | 1-indexed start channel for RGB triplet |
| `MIN_SEND_INTERVAL_MS` | `100` | Rate cap between HA sends |
| `RUST_LOG` | `info` | Log level (`tracing-subscriber` env filter) |

### 4.2 `artnet.rs`

Hand-rolled `ArtDmx` parser. Validates:

- Magic: `"Art-Net\0"` (8 bytes)
- OpCode: `0x5000` (ArtDmx, little-endian)
- Protocol version: ≥ 14
- Universe matches configured value
- Payload length sufficient to cover `RGB_START_CHANNEL + 2`

Returns `(R, G, B)` from the triplet at `RGB_START_CHANNEL` (1-indexed). Drops all other Art-Net opcodes (ArtPoll, ArtSync, ArtPollReply, etc.) silently.

Parsing is a pure function `parse(&[u8], universe: u16, start_ch: u16) -> Option<(u8,u8,u8)>` — trivially unit-testable.

### 4.3 `ha_client.rs`

Wraps `tokio-tungstenite`. Exposes:

```rust
trait HaClient {
    async fn turn_on(&self, rgb: (u8,u8,u8)) -> Result<()>;
    async fn turn_off(&self) -> Result<()>;
}
```

Internal state: `Arc<Mutex<Option<WsSink>>>` + atomic message-ID counter.

**Auth handshake (per HA docs):**

1. Server sends `{"type": "auth_required", "ha_version": "..."}`
2. Client sends `{"type": "auth", "access_token": "<token>"}`
3. Server responds `{"type": "auth_ok"}` or `{"type": "auth_invalid", ...}`. `auth_invalid` is fatal — log and exit.

**Service call format:**

```json
{
  "id": 42,
  "type": "call_service",
  "domain": "light",
  "service": "turn_on",
  "target": {"entity_id": ["light.h70a1_1e41", "light.h70a1_1e86"]},
  "service_data": {"rgb_color": [255, 80, 0]}
}
```

For `turn_off`, omit `service_data`, use `"service": "turn_off"`.

**Reconnect:** on any send/recv error, clear the sink, enter backoff loop (1s, 2s, 4s, 8s, 16s, cap 30s). During disconnect, `turn_on`/`turn_off` calls return `Err` immediately and bridge logs a warn. No queueing — stale RGB is worse than missed RGB for live use.

### 4.4 `bridge.rs`

The coordination loop:

```rust
let mut last_sent: Option<(u8,u8,u8)> = None;
loop {
    rx.changed().await?;                          // wake on new RGB
    let rgb_opt = *rx.borrow();
    let Some(rgb) = rgb_opt else { continue };
    if Some(rgb) == last_sent { continue; }       // change-detect
    let result = if rgb == (0,0,0) {
        ha.turn_off().await
    } else {
        ha.turn_on(rgb).await
    };
    match result {
        Ok(()) => { last_sent = Some(rgb); }
        Err(e) => { warn!("HA send failed: {e}"); }
    }
    tokio::time::sleep(MIN_SEND_INTERVAL).await;  // rate cap
}
```

Coalescing works because `rx.changed()` only reports the *latest* value — if the listener wrote 5 times during the sleep, we see the 5th on the next `changed().await`.

## 5. Data flow — one color change, end-to-end

1. `t=0 ms` — Kitten sends ArtDmx UDP to `<dev-ip>:6454`, Universe 0, channels 1–3 = `(255, 80, 0)`.
2. `t≈0.1 ms` — listener receives UDP, validates, extracts RGB.
3. `t≈0.1 ms` — listener writes to `watch`.
4. `t≈0.2 ms` — bridge wakes on `watch.changed()`, compares to `last_sent` — differs.
5. `t≈0.3 ms` — `ha_client.turn_on((255,80,0))` serializes JSON.
6. `t≈0.5 ms` — sent over open WebSocket.
7. `t≈5–15 ms` — HA receives, dispatches to Govee integration.
8. `t≈50–200 ms` — Govee lights physically change (out of our control).
9. bridge sleeps 100 ms, then loops. Any RGB values that arrived during the sleep are coalesced into the latest.

**End-to-end budget:** ~55–215 ms, dominated by the Govee side.

## 6. Error handling

| Scenario | Behavior |
|---|---|
| Invalid Art-Net packet (bad magic/opcode/version) | Drop, increment `artnet.invalid` counter (DEBUG log) |
| Art-Net packet for other universe | Drop silently (expected) |
| UDP bind fails at startup | Log ERROR, exit — systemd restarts |
| HA unreachable at startup | Retry with exponential backoff forever, WARN each attempt. Listener keeps running. |
| HA auth rejected | Log ERROR and exit (fatal misconfig — don't hammer) |
| WebSocket drops mid-session | Clear sink, reconnect with backoff, WARN. RGB calls during gap are dropped. |
| `call_service` returns `success: false` | Log WARN (rate-limited to 1/min), keep going |
| RGB unchanged for hours | No sends, no load |
| Console stops sending | App idles |
| Rapid fade | Handled by watch coalescing + change-detect |

## 7. Logging & observability

- Crate: `tracing` + `tracing-subscriber` with env filter.
- Default: `info` (startup, reconnects, send failures).
- `debug`: every RGB received / every HA send.
- `trace`: raw Art-Net bytes.
- Controlled via `RUST_LOG` env var.
- No metrics endpoint in v1. Adding Prometheus later is ~30 lines if needed.

## 8. Testing

| Layer | What | How |
|---|---|---|
| Unit — artnet parser | Valid ArtDmx → correct RGB; rejects bad magic, opcode, version, universe, short packets | Pure-function tests, hand-crafted byte arrays |
| Unit — bridge logic | Change-detect skips duplicates; rate cap enforces min interval; `(0,0,0)` → `turn_off`; non-zero → `turn_on` | Mock `HaClient` trait, drive with `watch::Sender`, assert call sequence + timing |
| Unit — HA JSON | `call_service` matches HA protocol; IDs increment | `assert_eq!` on serialized JSON |
| Integration — artnet e2e | Send real ArtDmx UDP to bound listener, verify watch receives expected RGB | Tokio test, bind `127.0.0.1:0` |
| Integration — HA WebSocket | Stub WS server mimics `auth_required`/`auth_ok`/`result` flow; verify handshake + `call_service` bytes | `tokio-tungstenite` server in test |
| End-to-end — live HA | Real HA + Govee; synthetic Art-Net UDP from dev machine; observe Govee reacting | Manual, during dev. Not in CI. |

**CI:** GitHub Actions. `cargo fmt --check`, `cargo clippy -D warnings`, `cargo nextest`, `cargo llvm-cov --fail-under-lines 60`. No Playwright (no UI — this project has no browser component).

The "end-to-end with live HA" manual test substitutes for browser E2E per the e2e-real-user-testing policy — the project has no UI, the console + lights are the "user".

## 9. Deployment

- **Dev:** `cargo run` from `/home/newlevel/devel/artnet-to-hass`, with `.env` alongside.
- **Prod:** single release binary copied to prod PC, `.env` placed in service working dir, systemd unit file included in repo (`deploy/artnet-to-hass.service`). Unit sets `Restart=always`, `EnvironmentFile=<path>/.env`.
- **Version bumping:** per airuleset, `Cargo.toml` version is bumped on `dev` before any feature work and before every PR to `main`.
- **Branching:** two-branch workflow (`main`, `dev`). All work on `dev`, PR to `main` for releases.

## 10. Out of scope (v1)

- Multiple Art-Net universes simultaneously
- Multiple light groups driven by different channel ranges
- Brightness channel separate from RGB
- Transitions (HA's `transition` parameter) — adds latency; Govee's own response smoothing is enough
- Prometheus / metrics endpoint
- Web UI
- Art-Net broadcast discovery / ArtPollReply

Each is a clean v2 addition without rewriting v1.
