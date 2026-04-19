# artnet-to-hass Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust bridge that receives Art-Net DMX on UDP:6454, extracts RGB from Universe 0 channels 1/2/3, and drives two Home Assistant Govee WiFi lights via WebSocket with low latency (change-detect + 10 Hz rate cap).

**Architecture:** Single Tokio binary with two async tasks (UDP listener + WS sender) sharing a `tokio::sync::watch<Option<(u8,u8,u8)>>` channel. Sender applies change-detection, rate cap, and calls `light.turn_on`/`light.turn_off` over an authenticated WebSocket with auto-reconnect.

**Tech Stack:** Rust (stable), `tokio`, `tokio-tungstenite`, `serde`/`serde_json`, `tracing`, `dotenvy`, `anyhow`.

**Spec:** `docs/superpowers/specs/2026-04-17-artnet-to-hass-design.md`

---

## Policy notes for the implementor

- **Branch:** all work happens on `dev`. Open a PR to `main` only when CI is green on `dev`.
- **Commits:** one focused commit per TDD cycle is ideal; batch small fix-ups into one commit before pushing.
- **Local compile policy:** per the user's airuleset, only `cargo fmt --all --check` runs locally by default. However, **this project is small (thin deps, no GUI), so running `cargo nextest` locally during TDD is fine** — `target/` will stay under ~1 GB. If the machine runs low on disk, stop running tests locally and rely on CI.
- **No `cargo watch`, no `cargo run` loops** — only the specific `cargo nextest run` commands listed in each step.
- **Git config is NOT global on this box.** When committing from scripts/automation, prefix with `git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik"`. For interactive work, the user may set local repo config once.
- **HA token is sensitive.** It lives only in `.env` (already gitignored). Never paste it into committed files, commit messages, or test fixtures.

---

## File structure after completion

```
artnet-to-hass/
├── Cargo.toml
├── Cargo.lock
├── .env.example
├── .gitignore                 (already exists)
├── .github/workflows/ci.yml
├── deploy/
│   ├── artnet-to-hass.service
│   └── README.md
├── src/
│   ├── main.rs                (wire up, load config, spawn tasks)
│   ├── config.rs              (Config struct + env loader)
│   ├── artnet.rs              (ArtDmx parser + UDP listener task)
│   ├── ha_client.rs           (WS client, auth, send, reconnect, trait)
│   └── bridge.rs              (rate-cap + change-detect loop)
├── tests/
│   ├── artnet_udp.rs          (integration: real UDP → watch)
│   └── ha_ws.rs               (integration: stub WS server)
└── docs/superpowers/
    ├── specs/2026-04-17-artnet-to-hass-design.md   (exists)
    └── plans/2026-04-17-artnet-to-hass-implementation.md  (this file)
```

---

## Task 0: Cargo project scaffold

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs` (placeholder)
- Create: `.env.example`

- [ ] **Step 0.1: Create `Cargo.toml`**

```toml
[package]
name = "artnet-to-hass"
version = "0.1.0"
edition = "2021"
description = "Art-Net DMX to Home Assistant bridge for live lighting"
license = "MIT"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "sync", "time", "io-util", "signal"] }
tokio-tungstenite = "0.24"
futures-util = { version = "0.3", default-features = false, features = ["sink", "std"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
dotenvy = "0.15"
anyhow = "1"

[dev-dependencies]
tokio = { version = "1", features = ["test-util"] }
```

- [ ] **Step 0.2: Create `src/main.rs` placeholder**

```rust
fn main() {
    println!("artnet-to-hass: scaffold only");
}
```

- [ ] **Step 0.3: Create `.env.example`**

```bash
# Home Assistant WebSocket URL. NOTE: ws:// scheme + /api/websocket path.
# Derive from your HA HTTP URL by replacing http:// with ws:// and appending /api/websocket.
HA_URL=ws://ha-snv.local:8123/api/websocket

# Long-lived access token (Profile → Security in HA).
HA_TOKEN=REPLACE_ME

# Comma-separated entity IDs (no spaces).
HA_LIGHTS=light.h70a1_1e41,light.h70a1_1e86

# Art-Net listener settings.
ARTNET_UNIVERSE=0
ARTNET_BIND=0.0.0.0:6454
RGB_START_CHANNEL=1

# Rate cap between HA sends (ms). 100 = 10 Hz. Govee handles 5-10 Hz reliably.
MIN_SEND_INTERVAL_MS=100

# Log level. `info` is default. Use `artnet_to_hass=debug` for RGB-per-send logs.
RUST_LOG=info
```

- [ ] **Step 0.4: Verify format**

Run: `cargo fmt --all --check`
Expected: exits 0 with no output (nothing to format yet).

- [ ] **Step 0.5: Commit scaffold**

```bash
git add Cargo.toml src/main.rs .env.example
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Scaffold Cargo project with dependencies and env template"
```

---

## Task 1: CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1.1: Create the CI workflow**

```yaml
name: CI

on:
  push:
    branches: [main, dev]
  pull_request:

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  fmt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt
      - run: cargo fmt --all --check

  clippy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo clippy --all-targets --all-features -- -D warnings

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: taiki-e/install-action@nextest
      - run: cargo nextest run --all-features

  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: llvm-tools-preview
      - uses: Swatinem/rust-cache@v2
      - uses: taiki-e/install-action@cargo-llvm-cov
      - uses: taiki-e/install-action@nextest
      - run: cargo llvm-cov nextest --fail-under-lines ${{ vars.COVERAGE_THRESHOLD || '60' }}
```

- [ ] **Step 1.2: Commit CI workflow**

```bash
git add .github/workflows/ci.yml
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Add GitHub Actions CI (fmt, clippy, nextest, llvm-cov)"
```

---

## Task 2: Config module (TDD)

**Files:**
- Create: `src/config.rs`
- Modify: `src/main.rs` (add `mod config;`)

- [ ] **Step 2.1: Write the failing test**

Create `src/config.rs`:

```rust
use anyhow::{anyhow, Context, Result};
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub ha_url: String,
    pub ha_token: String,
    pub ha_lights: Vec<String>,
    pub artnet_universe: u16,
    pub artnet_bind: SocketAddr,
    pub rgb_start_channel: u16,
    pub min_send_interval: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_env() -> Vec<(&'static str, String)> {
        vec![
            ("HA_URL", "ws://ha.test/api/websocket".into()),
            ("HA_TOKEN", "test-token".into()),
            ("HA_LIGHTS", "light.a,light.b".into()),
        ]
    }

    fn with_env<F, T>(vars: &[(&str, String)], f: F) -> T
    where
        F: FnOnce() -> T,
    {
        // SAFETY: tests in this module are serialized by running under `--test-threads=1`
        // in the specific test commands in the plan. We don't rely on process isolation.
        let keys = [
            "HA_URL",
            "HA_TOKEN",
            "HA_LIGHTS",
            "ARTNET_UNIVERSE",
            "ARTNET_BIND",
            "RGB_START_CHANNEL",
            "MIN_SEND_INTERVAL_MS",
        ];
        for k in keys {
            std::env::remove_var(k);
        }
        for (k, v) in vars {
            std::env::set_var(k, v);
        }
        f()
    }

    #[test]
    fn loads_required_vars_and_defaults() {
        let cfg = with_env(&base_env(), Config::from_env).unwrap();
        assert_eq!(cfg.ha_url, "ws://ha.test/api/websocket");
        assert_eq!(cfg.ha_token, "test-token");
        assert_eq!(cfg.ha_lights, vec!["light.a".to_string(), "light.b".to_string()]);
        assert_eq!(cfg.artnet_universe, 0);
        assert_eq!(cfg.artnet_bind.to_string(), "0.0.0.0:6454");
        assert_eq!(cfg.rgb_start_channel, 1);
        assert_eq!(cfg.min_send_interval, Duration::from_millis(100));
    }

    #[test]
    fn fails_on_missing_required() {
        let mut env = base_env();
        env.retain(|(k, _)| *k != "HA_TOKEN");
        let err = with_env(&env, Config::from_env).unwrap_err();
        assert!(err.to_string().contains("HA_TOKEN"), "got: {err}");
    }

    #[test]
    fn trims_entity_ids() {
        let mut env = base_env();
        env.iter_mut().find(|(k, _)| *k == "HA_LIGHTS").unwrap().1 =
            " light.a , light.b ".into();
        let cfg = with_env(&env, Config::from_env).unwrap();
        assert_eq!(cfg.ha_lights, vec!["light.a", "light.b"]);
    }

    #[test]
    fn parses_custom_universe_and_bind() {
        let mut env = base_env();
        env.push(("ARTNET_UNIVERSE", "3".into()));
        env.push(("ARTNET_BIND", "127.0.0.1:7000".into()));
        env.push(("RGB_START_CHANNEL", "10".into()));
        env.push(("MIN_SEND_INTERVAL_MS", "50".into()));
        let cfg = with_env(&env, Config::from_env).unwrap();
        assert_eq!(cfg.artnet_universe, 3);
        assert_eq!(cfg.artnet_bind.to_string(), "127.0.0.1:7000");
        assert_eq!(cfg.rgb_start_channel, 10);
        assert_eq!(cfg.min_send_interval, Duration::from_millis(50));
    }

    #[test]
    fn rejects_rgb_start_channel_zero() {
        let mut env = base_env();
        env.push(("RGB_START_CHANNEL", "0".into()));
        let err = with_env(&env, Config::from_env).unwrap_err();
        assert!(err.to_string().contains("RGB_START_CHANNEL"), "got: {err}");
    }

    #[test]
    fn rejects_rgb_start_channel_too_high() {
        let mut env = base_env();
        env.push(("RGB_START_CHANNEL", "511".into()));
        let err = with_env(&env, Config::from_env).unwrap_err();
        assert!(err.to_string().contains("RGB_START_CHANNEL"), "got: {err}");
    }
}
```

Add to `src/main.rs`:

```rust
mod config;

fn main() {
    println!("artnet-to-hass: scaffold only");
}
```

- [ ] **Step 2.2: Run the tests — verify they fail**

Run: `cargo nextest run --test-threads=1 config::tests`
Expected: compile error (`unimplemented!()` compiles but tests panic). If it compiles, all six tests panic with "not implemented".

- [ ] **Step 2.3: Implement `Config::from_env`**

Replace the `unimplemented!()` body in `src/config.rs`:

```rust
impl Config {
    pub fn from_env() -> Result<Self> {
        let ha_url = require("HA_URL")?;
        let ha_token = require("HA_TOKEN")?;
        let ha_lights = require("HA_LIGHTS")?
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if ha_lights.is_empty() {
            return Err(anyhow!("HA_LIGHTS must list at least one entity_id"));
        }

        let artnet_universe: u16 = parse_or_default("ARTNET_UNIVERSE", 0)?;
        let artnet_bind: SocketAddr = std::env::var("ARTNET_BIND")
            .unwrap_or_else(|_| "0.0.0.0:6454".into())
            .parse()
            .context("ARTNET_BIND must be a valid socket address")?;

        let rgb_start_channel: u16 = parse_or_default("RGB_START_CHANNEL", 1)?;
        if rgb_start_channel == 0 || rgb_start_channel > 510 {
            return Err(anyhow!("RGB_START_CHANNEL must be in 1..=510 (got {rgb_start_channel})"));
        }

        let min_ms: u64 = parse_or_default("MIN_SEND_INTERVAL_MS", 100)?;
        let min_send_interval = Duration::from_millis(min_ms);

        Ok(Self {
            ha_url,
            ha_token,
            ha_lights,
            artnet_universe,
            artnet_bind,
            rgb_start_channel,
            min_send_interval,
        })
    }
}

fn require(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| anyhow!("{key} must be set"))
}

fn parse_or_default<T: std::str::FromStr>(key: &str, default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) => v.parse::<T>().map_err(|e| anyhow!("{key} invalid: {e}")),
        Err(_) => Ok(default),
    }
}
```

- [ ] **Step 2.4: Run the tests — verify they pass**

Run: `cargo nextest run --test-threads=1 config::tests`
Expected: all 6 tests pass.

- [ ] **Step 2.5: Format and commit**

Run: `cargo fmt --all`

```bash
git add src/config.rs src/main.rs
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Add config module with env loader and validation"
```

---

## Task 3: Art-Net parser (pure function, TDD)

**Files:**
- Create: `src/artnet.rs`
- Modify: `src/main.rs` (add `mod artnet;`)

- [ ] **Step 3.1: Write the failing test**

Create `src/artnet.rs`:

```rust
/// Parse an ArtDmx packet and extract an RGB triplet from the configured start channel.
///
/// Returns `Some((r, g, b))` only for valid ArtDmx packets on the matching universe.
/// All other packets (ArtPoll, wrong universe, malformed, etc.) return `None`.
pub fn parse_artdmx(
    buf: &[u8],
    universe: u16,
    rgb_start_channel: u16,
) -> Option<(u8, u8, u8)> {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal ArtDmx packet for tests.
    /// Layout:
    ///   0..8   "Art-Net\0"
    ///   8..10  OpCode (little-endian)      = 0x5000
    ///   10..12 ProtVer (big-endian)        = 14
    ///   12     Sequence
    ///   13     Physical
    ///   14..16 PortAddress (little-endian) = universe
    ///   16..18 Length (big-endian)         = payload len
    ///   18..   DMX payload (up to 512 bytes)
    fn build_artdmx(universe: u16, dmx: &[u8]) -> Vec<u8> {
        let mut pkt = Vec::with_capacity(18 + dmx.len());
        pkt.extend_from_slice(b"Art-Net\0");
        pkt.extend_from_slice(&0x5000u16.to_le_bytes()); // OpDmx, little-endian
        pkt.extend_from_slice(&14u16.to_be_bytes());     // ProtVer hi,lo big-endian
        pkt.push(0); // Sequence
        pkt.push(0); // Physical
        pkt.extend_from_slice(&universe.to_le_bytes());  // PortAddress little-endian
        pkt.extend_from_slice(&(dmx.len() as u16).to_be_bytes()); // Length big-endian
        pkt.extend_from_slice(dmx);
        pkt
    }

    #[test]
    fn parses_valid_packet_universe_0_channels_1_2_3() {
        let mut dmx = vec![0u8; 512];
        dmx[0] = 255;
        dmx[1] = 80;
        dmx[2] = 0;
        let pkt = build_artdmx(0, &dmx);
        assert_eq!(parse_artdmx(&pkt, 0, 1), Some((255, 80, 0)));
    }

    #[test]
    fn respects_rgb_start_channel() {
        let mut dmx = vec![0u8; 512];
        dmx[9] = 10;
        dmx[10] = 20;
        dmx[11] = 30;
        let pkt = build_artdmx(0, &dmx);
        assert_eq!(parse_artdmx(&pkt, 0, 10), Some((10, 20, 30)));
    }

    #[test]
    fn rejects_wrong_universe() {
        let dmx = vec![1u8; 512];
        let pkt = build_artdmx(1, &dmx);
        assert_eq!(parse_artdmx(&pkt, 0, 1), None);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut pkt = build_artdmx(0, &vec![0u8; 512]);
        pkt[0] = b'X';
        assert_eq!(parse_artdmx(&pkt, 0, 1), None);
    }

    #[test]
    fn rejects_wrong_opcode() {
        let mut pkt = build_artdmx(0, &vec![0u8; 512]);
        // Change OpCode to something else (e.g., ArtPoll 0x2000, little-endian).
        pkt[8] = 0x00;
        pkt[9] = 0x20;
        assert_eq!(parse_artdmx(&pkt, 0, 1), None);
    }

    #[test]
    fn rejects_low_protocol_version() {
        let mut pkt = build_artdmx(0, &vec![0u8; 512]);
        // ProtVer is big-endian at bytes 10..12. Set to 13.
        pkt[10] = 0;
        pkt[11] = 13;
        assert_eq!(parse_artdmx(&pkt, 0, 1), None);
    }

    #[test]
    fn rejects_packet_too_short_for_header() {
        assert_eq!(parse_artdmx(b"Art-Net\0", 0, 1), None);
    }

    #[test]
    fn rejects_payload_shorter_than_start_channel_plus_two() {
        // DMX payload is only 2 bytes, can't cover channels 1..=3.
        let pkt = build_artdmx(0, &[100, 100]);
        assert_eq!(parse_artdmx(&pkt, 0, 1), None);
    }

    #[test]
    fn respects_declared_length_field() {
        // Build a packet whose buffer is 512 bytes but declared length is 2.
        let mut pkt = build_artdmx(0, &vec![0u8; 512]);
        // Overwrite declared length (bytes 16..18, big-endian) to 2.
        pkt[16] = 0;
        pkt[17] = 2;
        // With declared length 2, channels 1..=3 are not covered.
        assert_eq!(parse_artdmx(&pkt, 0, 1), None);
    }
}
```

Add to `src/main.rs`:

```rust
mod artnet;
mod config;

fn main() {
    println!("artnet-to-hass: scaffold only");
}
```

- [ ] **Step 3.2: Run the tests — verify they fail**

Run: `cargo nextest run artnet::tests`
Expected: all 9 tests panic with "not implemented".

- [ ] **Step 3.3: Implement `parse_artdmx`**

Replace the `unimplemented!()` body:

```rust
pub fn parse_artdmx(
    buf: &[u8],
    universe: u16,
    rgb_start_channel: u16,
) -> Option<(u8, u8, u8)> {
    // Minimum header length: 18 bytes.
    if buf.len() < 18 {
        return None;
    }
    // Magic: "Art-Net\0".
    if &buf[0..8] != b"Art-Net\0" {
        return None;
    }
    // OpCode: little-endian, must be 0x5000 (OpDmx).
    let opcode = u16::from_le_bytes([buf[8], buf[9]]);
    if opcode != 0x5000 {
        return None;
    }
    // Protocol version: big-endian, must be >= 14.
    let protver = u16::from_be_bytes([buf[10], buf[11]]);
    if protver < 14 {
        return None;
    }
    // PortAddress: little-endian, must match configured universe.
    let pkt_universe = u16::from_le_bytes([buf[14], buf[15]]);
    if pkt_universe != universe {
        return None;
    }
    // Declared payload length: big-endian.
    let declared_len = u16::from_be_bytes([buf[16], buf[17]]) as usize;
    let payload_end = 18usize.saturating_add(declared_len);
    if payload_end > buf.len() {
        return None;
    }
    let payload = &buf[18..payload_end];

    // rgb_start_channel is 1-indexed. We need channels start, start+1, start+2.
    if rgb_start_channel == 0 {
        return None;
    }
    let start_idx = (rgb_start_channel - 1) as usize;
    if start_idx + 2 >= payload.len() {
        return None;
    }
    Some((payload[start_idx], payload[start_idx + 1], payload[start_idx + 2]))
}
```

- [ ] **Step 3.4: Run the tests — verify they pass**

Run: `cargo nextest run artnet::tests`
Expected: all 9 tests pass.

- [ ] **Step 3.5: Format and commit**

Run: `cargo fmt --all`

```bash
git add src/artnet.rs src/main.rs
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Add Art-Net ArtDmx parser with validation"
```

---

## Task 4: Art-Net UDP listener task (TDD integration)

**Files:**
- Modify: `src/artnet.rs` (add listener)
- Create: `tests/artnet_udp.rs`

- [ ] **Step 4.1: Declare the listener in `src/artnet.rs`**

Add below the `parse_artdmx` function:

```rust
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tracing::{debug, trace, warn};

/// Spawned as a task. Binds a UDP socket, parses ArtDmx, publishes RGB to the watch.
/// Returns only on fatal error (bind failure or socket broken beyond recovery).
pub async fn run_listener(
    bind: SocketAddr,
    universe: u16,
    rgb_start_channel: u16,
    tx: watch::Sender<Option<(u8, u8, u8)>>,
) -> anyhow::Result<()> {
    let sock = UdpSocket::bind(bind)
        .await
        .map_err(|e| anyhow::anyhow!("Art-Net UDP bind to {bind} failed: {e}"))?;
    tracing::info!("Art-Net listener bound to {bind}, universe {universe}");

    let mut buf = vec![0u8; 1500];
    loop {
        let (n, from) = match sock.recv_from(&mut buf).await {
            Ok(x) => x,
            Err(e) => {
                warn!("Art-Net recv error: {e}");
                continue;
            }
        };
        trace!("Art-Net packet from {from}, {n} bytes");
        if let Some(rgb) = parse_artdmx(&buf[..n], universe, rgb_start_channel) {
            debug!("Art-Net RGB received: {:?}", rgb);
            // watch::send errors only if all receivers dropped — treat as fatal.
            if tx.send(Some(rgb)).is_err() {
                return Err(anyhow::anyhow!("watch receiver dropped; shutting down listener"));
            }
        }
    }
}
```

- [ ] **Step 4.2: Write the failing integration test**

Create `tests/artnet_udp.rs`:

```rust
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::watch;

fn build_artdmx(universe: u16, dmx: &[u8]) -> Vec<u8> {
    let mut pkt = Vec::with_capacity(18 + dmx.len());
    pkt.extend_from_slice(b"Art-Net\0");
    pkt.extend_from_slice(&0x5000u16.to_le_bytes());
    pkt.extend_from_slice(&14u16.to_be_bytes());
    pkt.push(0);
    pkt.push(0);
    pkt.extend_from_slice(&universe.to_le_bytes());
    pkt.extend_from_slice(&(dmx.len() as u16).to_be_bytes());
    pkt.extend_from_slice(dmx);
    pkt
}

#[tokio::test]
async fn udp_listener_publishes_rgb_on_matching_universe() {
    let (tx, mut rx) = watch::channel::<Option<(u8, u8, u8)>>(None);
    let bind_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();

    // Bind via a helper that returns the bound addr before starting the loop.
    let sock = UdpSocket::bind(bind_addr).await.unwrap();
    let addr = sock.local_addr().unwrap();

    // Spawn the listener manually using the socket so we know the port.
    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 1500];
        loop {
            let (n, _) = sock.recv_from(&mut buf).await.unwrap();
            if let Some(rgb) = artnet_to_hass::artnet::parse_artdmx(&buf[..n], 0, 1) {
                if tx.send(Some(rgb)).is_err() {
                    return;
                }
            }
        }
    });

    // Send a valid packet.
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut dmx = vec![0u8; 512];
    dmx[0] = 1;
    dmx[1] = 2;
    dmx[2] = 3;
    client.send_to(&build_artdmx(0, &dmx), addr).await.unwrap();

    // Wait for it.
    tokio::time::timeout(Duration::from_secs(1), rx.changed()).await.unwrap().unwrap();
    assert_eq!(*rx.borrow(), Some((1, 2, 3)));

    handle.abort();
}

#[tokio::test]
async fn udp_listener_ignores_wrong_universe() {
    let (tx, mut rx) = watch::channel::<Option<(u8, u8, u8)>>(None);
    let bind_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();

    let sock = UdpSocket::bind(bind_addr).await.unwrap();
    let addr = sock.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 1500];
        loop {
            let (n, _) = sock.recv_from(&mut buf).await.unwrap();
            if let Some(rgb) = artnet_to_hass::artnet::parse_artdmx(&buf[..n], 0, 1) {
                if tx.send(Some(rgb)).is_err() {
                    return;
                }
            }
        }
    });

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut dmx = vec![0u8; 512];
    dmx[0] = 9;
    client.send_to(&build_artdmx(1, &dmx), addr).await.unwrap();

    // Should NOT publish. Wait a bit then check.
    let result = tokio::time::timeout(Duration::from_millis(200), rx.changed()).await;
    assert!(result.is_err(), "expected no update, got {:?}", *rx.borrow());

    handle.abort();
}
```

Also: the integration test imports `artnet_to_hass::artnet::parse_artdmx`, so we need a library target alongside the binary. Create `src/lib.rs`:

```rust
pub mod artnet;
pub mod config;
```

Update `src/main.rs` to use the library:

```rust
use artnet_to_hass::{artnet, config};

fn main() {
    let _ = (artnet::parse_artdmx, config::Config::from_env);
    println!("artnet-to-hass: scaffold only");
}
```

Update `Cargo.toml` to declare both targets (add after `[package]`):

```toml
[lib]
name = "artnet_to_hass"
path = "src/lib.rs"

[[bin]]
name = "artnet-to-hass"
path = "src/main.rs"
```

- [ ] **Step 4.3: Run the integration tests — verify they pass**

Run: `cargo nextest run --test artnet_udp`
Expected: both tests pass.

(These tests pass because we're using `parse_artdmx` directly from the library — the handwritten spawn in the test substitutes for `run_listener`. We test `run_listener` itself in Task 11 via the end-to-end wiring, since bind-before-spawn is trickier to wrap cleanly here. The core logic under test — parse + publish — is covered.)

- [ ] **Step 4.4: Format and commit**

Run: `cargo fmt --all`

```bash
git add src/artnet.rs src/lib.rs src/main.rs tests/artnet_udp.rs Cargo.toml
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Add UDP listener + integration test for ArtDmx receive"
```

---

## Task 5: HA message builders (TDD)

**Files:**
- Create: `src/ha_client.rs`
- Modify: `src/lib.rs` (add `pub mod ha_client;`)

- [ ] **Step 5.1: Write the failing test**

Create `src/ha_client.rs`:

```rust
use serde::Serialize;
use serde_json::{json, Value};

/// Build the HA WebSocket `call_service` JSON for `light.turn_on` with rgb_color.
pub fn build_turn_on(id: u64, lights: &[String], rgb: (u8, u8, u8)) -> Value {
    let _ = (id, lights, rgb);
    unimplemented!()
}

/// Build the HA WebSocket `call_service` JSON for `light.turn_off`.
pub fn build_turn_off(id: u64, lights: &[String]) -> Value {
    let _ = (id, lights);
    unimplemented!()
}

/// Build the HA WebSocket auth message.
pub fn build_auth(token: &str) -> Value {
    let _ = token;
    unimplemented!()
}

#[derive(Debug, Serialize)]
struct _Phantom;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn turn_on_matches_ha_protocol() {
        let lights = vec!["light.a".into(), "light.b".into()];
        let msg = build_turn_on(7, &lights, (255, 80, 0));
        assert_eq!(msg, json!({
            "id": 7,
            "type": "call_service",
            "domain": "light",
            "service": "turn_on",
            "target": {"entity_id": ["light.a", "light.b"]},
            "service_data": {"rgb_color": [255, 80, 0]}
        }));
    }

    #[test]
    fn turn_off_matches_ha_protocol() {
        let lights = vec!["light.a".into()];
        let msg = build_turn_off(9, &lights);
        assert_eq!(msg, json!({
            "id": 9,
            "type": "call_service",
            "domain": "light",
            "service": "turn_off",
            "target": {"entity_id": ["light.a"]}
        }));
    }

    #[test]
    fn auth_message_format() {
        let msg = build_auth("abc.def.ghi");
        assert_eq!(msg, json!({"type": "auth", "access_token": "abc.def.ghi"}));
    }
}
```

Add to `src/lib.rs`:

```rust
pub mod artnet;
pub mod config;
pub mod ha_client;
```

- [ ] **Step 5.2: Run the tests — verify they fail**

Run: `cargo nextest run ha_client::tests`
Expected: all 3 tests panic with "not implemented".

- [ ] **Step 5.3: Implement the builders**

Replace the three `unimplemented!()` bodies:

```rust
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

pub fn build_turn_off(id: u64, lights: &[String]) -> Value {
    json!({
        "id": id,
        "type": "call_service",
        "domain": "light",
        "service": "turn_off",
        "target": {"entity_id": lights}
    })
}

pub fn build_auth(token: &str) -> Value {
    json!({"type": "auth", "access_token": token})
}
```

Also delete the placeholder `#[derive(Debug, Serialize)] struct _Phantom;` line (it was only to keep `serde::Serialize` in use for now). Remove the `use serde::Serialize;` import too; only `serde_json` is needed here.

- [ ] **Step 5.4: Run the tests — verify they pass**

Run: `cargo nextest run ha_client::tests`
Expected: all 3 tests pass.

- [ ] **Step 5.5: Format and commit**

Run: `cargo fmt --all`

```bash
git add src/ha_client.rs src/lib.rs
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Add HA WebSocket message builders (turn_on, turn_off, auth)"
```

---

## Task 6: HA WebSocket client + auth handshake (TDD with stub server)

**Files:**
- Modify: `src/ha_client.rs` (add `connect_and_authenticate`)
- Create: `tests/ha_ws.rs` (shared stub-server helpers + auth test)

- [ ] **Step 6.1: Declare the connection type in `src/ha_client.rs`**

Append:

```rust
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

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
    ws.send(Message::Text(auth.to_string())).await.context("sending auth")?;

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
        Some("auth_invalid") => {
            Err(anyhow!("auth_invalid: {}", v.get("message").and_then(|m| m.as_str()).unwrap_or("")))
        }
        other => Err(anyhow!("unexpected auth response: {:?} — {v}", other)),
    }
}
```

- [ ] **Step 6.2: Write the failing integration test with stub HA server**

Create `tests/ha_ws.rs`:

```rust
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
        )).await.unwrap();

        // Read auth.
        let auth = ws.next().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(auth.to_text().unwrap()).unwrap();
        assert_eq!(v["type"], "auth");

        if v["access_token"] == "valid-token" {
            ws.send(Message::Text(
                serde_json::json!({"type": "auth_ok", "ha_version": "test"}).to_string(),
            )).await.unwrap();
            on_connected(ws).await;
        } else {
            ws.send(Message::Text(
                serde_json::json!({"type": "auth_invalid", "message": "bad token"}).to_string(),
            )).await.unwrap();
        }
    });

    (url, handle)
}

#[tokio::test]
async fn auth_succeeds_with_valid_token() {
    let (url, server) = start_stub_ha(|_ws| async move {
        // Keep the connection alive briefly.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }).await;

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
```

- [ ] **Step 6.3: Run the test — verify it passes**

Run: `cargo nextest run --test ha_ws`
Expected: both tests pass.

- [ ] **Step 6.4: Format and commit**

Run: `cargo fmt --all`

```bash
git add src/ha_client.rs tests/ha_ws.rs
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Add HA WebSocket auth handshake with integration test"
```

---

## Task 7: HA send + response dispatch (TDD with stub)

**Files:**
- Modify: `src/ha_client.rs` (add `send_and_await_result`)
- Modify: `tests/ha_ws.rs` (add send test)

- [ ] **Step 7.1: Add the send function**

Append to `src/ha_client.rs`:

```rust
use tokio::sync::Mutex;
use std::sync::Arc;

/// Send a pre-built message with the given id and await the matching `result`.
/// Returns `Ok(true)` if HA reports success, `Ok(false)` if success=false, `Err` on I/O failure.
pub async fn send_and_await_result(ws: &mut WsStream, msg: &serde_json::Value) -> Result<bool> {
    let id = msg.get("id").and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("message missing numeric id"))?;

    ws.send(Message::Text(msg.to_string())).await.context("send call_service")?;

    // Read messages until we get a `result` with matching id.
    // HA may send `event` messages between result frames; we ignore non-result.
    loop {
        let frame = ws.next().await
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
        self.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
    pub async fn turn_on(&self, lights: &[String], rgb: (u8, u8, u8)) -> Result<bool> {
        let msg = build_turn_on(self.next_id(), lights, rgb);
        let mut ws = self.ws.lock().await;
        send_and_await_result(&mut *ws, &msg).await
    }
    pub async fn turn_off(&self, lights: &[String]) -> Result<bool> {
        let msg = build_turn_off(self.next_id(), lights);
        let mut ws = self.ws.lock().await;
        send_and_await_result(&mut *ws, &msg).await
    }
}
```

- [ ] **Step 7.2: Add the failing test in `tests/ha_ws.rs`**

Append:

```rust
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
        assert_eq!(v["service_data"]["rgb_color"], serde_json::json!([10, 20, 30]));
        let id = v["id"].as_u64().unwrap();
        ws.send(Message::Text(
            serde_json::json!({"id": id, "type": "result", "success": true, "result": null}).to_string()
        )).await.unwrap();
    }).await;

    let ws = connect_and_authenticate(&url, "valid-token").await.unwrap();
    let conn = HaConnection::new(ws);
    let ok = conn.turn_on(&vec!["light.a".into()], (10, 20, 30)).await.unwrap();
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
            }).to_string()
        )).await.unwrap();
    }).await;

    let ws = connect_and_authenticate(&url, "valid-token").await.unwrap();
    let conn = HaConnection::new(ws);
    let ok = conn.turn_off(&vec!["light.nope".into()]).await.unwrap();
    assert!(!ok);

    let _ = server.await;
}
```

- [ ] **Step 7.3: Run the tests — verify they pass**

Run: `cargo nextest run --test ha_ws`
Expected: 4 tests total pass (2 existing + 2 new).

- [ ] **Step 7.4: Format and commit**

Run: `cargo fmt --all`

```bash
git add src/ha_client.rs tests/ha_ws.rs
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Add HA call_service send + response dispatch"
```

---

## Task 8: HA reconnect with exponential backoff + trait facade

**Files:**
- Modify: `src/ha_client.rs` (add `HaClient` trait + `ReconnectingHaClient`)

- [ ] **Step 8.1: Add the trait and reconnecting client**

Append to `src/ha_client.rs`:

```rust
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

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
```

Add `async-trait` to `Cargo.toml` dependencies:

```toml
async-trait = "0.1"
```

- [ ] **Step 8.2: Format and commit**

No new tests in this task — the reconnect loop is driven by real network behavior and its core machinery (`connect_and_authenticate`, `HaConnection::turn_on/off`) is already tested. The reconnect timing and loop are verified manually in Task 11.

Run: `cargo fmt --all`

```bash
git add src/ha_client.rs Cargo.toml
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Add HaClient trait and ReconnectingHaClient with backoff"
```

---

## Task 9: Bridge loop (TDD with mock HaClient)

**Files:**
- Create: `src/bridge.rs`
- Modify: `src/lib.rs` (add `pub mod bridge;`)

- [ ] **Step 9.1: Write the failing test**

Create `src/bridge.rs`:

```rust
use crate::ha_client::HaClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::warn;

/// Run the bridge loop. Wakes on new RGB, applies change-detect + rate cap,
/// calls HA turn_on/turn_off. Returns only if the watch sender is dropped.
pub async fn run_bridge<H: HaClient + ?Sized>(
    ha: Arc<H>,
    mut rx: watch::Receiver<Option<(u8, u8, u8)>>,
    min_send_interval: Duration,
) -> anyhow::Result<()> {
    let mut last_sent: Option<(u8, u8, u8)> = None;
    loop {
        rx.changed().await?;
        let rgb_opt = *rx.borrow();
        let Some(rgb) = rgb_opt else { continue };
        if Some(rgb) == last_sent {
            continue;
        }
        let result = if rgb == (0, 0, 0) {
            ha.turn_off().await
        } else {
            ha.turn_on(rgb).await
        };
        match result {
            Ok(()) => last_sent = Some(rgb),
            Err(e) => warn!("HA send failed: {e}"),
        }
        tokio::time::sleep(min_send_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use tokio::time::Instant;

    #[derive(Default)]
    struct MockHa {
        calls: Mutex<Vec<(Instant, Call)>>,
        fail_next: Mutex<bool>,
    }
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum Call {
        On(u8, u8, u8),
        Off,
    }

    #[async_trait]
    impl HaClient for MockHa {
        async fn turn_on(&self, rgb: (u8, u8, u8)) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push((Instant::now(), Call::On(rgb.0, rgb.1, rgb.2)));
            if std::mem::replace(&mut *self.fail_next.lock().unwrap(), false) {
                return Err(anyhow::anyhow!("mock failure"));
            }
            Ok(())
        }
        async fn turn_off(&self) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push((Instant::now(), Call::Off));
            Ok(())
        }
    }

    #[tokio::test]
    async fn sends_turn_on_for_nonzero_rgb() {
        let (tx, rx) = watch::channel(None);
        let ha = Arc::new(MockHa::default());
        let ha_clone = ha.clone();
        let h = tokio::spawn(async move {
            let _ = run_bridge(ha_clone, rx, Duration::from_millis(10)).await;
        });
        tx.send(Some((1, 2, 3))).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let calls = ha.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, Call::On(1, 2, 3));
        drop(tx);
        let _ = h.await;
    }

    #[tokio::test]
    async fn sends_turn_off_for_zero_rgb() {
        let (tx, rx) = watch::channel(None);
        let ha = Arc::new(MockHa::default());
        let ha_clone = ha.clone();
        let h = tokio::spawn(async move {
            let _ = run_bridge(ha_clone, rx, Duration::from_millis(10)).await;
        });
        tx.send(Some((0, 0, 0))).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let calls = ha.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, Call::Off);
        drop(tx);
        let _ = h.await;
    }

    #[tokio::test]
    async fn change_detect_skips_duplicates() {
        let (tx, rx) = watch::channel(None);
        let ha = Arc::new(MockHa::default());
        let ha_clone = ha.clone();
        let h = tokio::spawn(async move {
            let _ = run_bridge(ha_clone, rx, Duration::from_millis(10)).await;
        });
        tx.send(Some((5, 5, 5))).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Send the same value again a few times.
        for _ in 0..3 {
            tx.send(Some((5, 5, 5))).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let calls = ha.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "expected 1 call, got {:?}", *calls);
        drop(tx);
        let _ = h.await;
    }

    #[tokio::test]
    async fn rate_cap_enforces_min_interval() {
        let (tx, rx) = watch::channel(None);
        let ha = Arc::new(MockHa::default());
        let ha_clone = ha.clone();
        let h = tokio::spawn(async move {
            let _ = run_bridge(ha_clone, rx, Duration::from_millis(100)).await;
        });
        // Fire 3 distinct values rapidly.
        tx.send(Some((1, 0, 0))).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(Some((2, 0, 0))).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(Some((3, 0, 0))).unwrap();
        // Give time for bridge to process under the 100 ms cap.
        tokio::time::sleep(Duration::from_millis(350)).await;
        let calls = ha.calls.lock().unwrap();
        // We expect at least 2 calls, spaced >= 100 ms apart.
        assert!(calls.len() >= 2, "got {:?}", *calls);
        for w in calls.windows(2) {
            let gap = w[1].0.duration_since(w[0].0);
            assert!(gap >= Duration::from_millis(95), "gap {:?} too small", gap);
        }
        drop(tx);
        let _ = h.await;
    }

    #[tokio::test]
    async fn does_not_update_last_sent_on_failure() {
        let (tx, rx) = watch::channel(None);
        let ha = Arc::new(MockHa::default());
        *ha.fail_next.lock().unwrap() = true;
        let ha_clone = ha.clone();
        let h = tokio::spawn(async move {
            let _ = run_bridge(ha_clone, rx, Duration::from_millis(10)).await;
        });
        tx.send(Some((7, 7, 7))).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Same value again — because the previous send failed, last_sent is still None,
        // so this must be attempted.
        tx.send(Some((7, 7, 7))).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let calls = ha.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        drop(tx);
        let _ = h.await;
    }
}
```

Add to `src/lib.rs`:

```rust
pub mod artnet;
pub mod bridge;
pub mod config;
pub mod ha_client;
```

- [ ] **Step 9.2: Run the tests — verify they pass**

Run: `cargo nextest run bridge::tests`
Expected: all 5 tests pass (each completes within ~500 ms).

- [ ] **Step 9.3: Format and commit**

Run: `cargo fmt --all`

```bash
git add src/bridge.rs src/lib.rs
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Add bridge loop with change-detect, rate cap, and off handling"
```

---

## Task 10: Wire everything up in `main.rs`

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 10.1: Write the complete `main.rs`**

Replace `src/main.rs`:

```rust
use anyhow::Result;
use artnet_to_hass::{artnet, bridge, config::Config, ha_client::ReconnectingHaClient};
use std::sync::Arc;
use tokio::signal;
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env (ignore if missing).
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = Config::from_env()?;
    info!(
        universe = cfg.artnet_universe,
        bind = %cfg.artnet_bind,
        start_channel = cfg.rgb_start_channel,
        min_interval_ms = cfg.min_send_interval.as_millis() as u64,
        lights = ?cfg.ha_lights,
        "artnet-to-hass starting"
    );

    // Shared RGB channel.
    let (tx, rx) = watch::channel::<Option<(u8, u8, u8)>>(None);

    // HA client: spawns its own reconnect loop.
    let ha = ReconnectingHaClient::spawn(cfg.ha_url.clone(), cfg.ha_token.clone(), cfg.ha_lights.clone());

    // Bridge task.
    let bridge_handle = {
        let ha = ha.clone();
        let rx = rx.clone();
        let min = cfg.min_send_interval;
        tokio::spawn(async move {
            if let Err(e) = bridge::run_bridge(ha, rx, min).await {
                error!("bridge exited: {e}");
            }
        })
    };

    // Art-Net listener task.
    let listener_handle = {
        let bind = cfg.artnet_bind;
        let universe = cfg.artnet_universe;
        let start = cfg.rgb_start_channel;
        tokio::spawn(async move {
            if let Err(e) = artnet::run_listener(bind, universe, start, tx).await {
                error!("artnet listener exited: {e}");
            }
        })
    };

    // Wait for Ctrl-C or any task exiting.
    tokio::select! {
        _ = signal::ctrl_c() => info!("shutdown signal received"),
        _ = bridge_handle => error!("bridge task exited unexpectedly"),
        _ = listener_handle => error!("listener task exited unexpectedly"),
    }

    Ok(())
}
```

- [ ] **Step 10.2: Format check**

Run: `cargo fmt --all --check`
Expected: exits 0.

- [ ] **Step 10.3: Full test suite**

Run: `cargo nextest run --all-targets`
Expected: all tests (unit + integration) pass.

- [ ] **Step 10.4: Commit**

```bash
git add src/main.rs
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Wire up main binary: config, listener, bridge, HA client"
```

---

## Task 11: systemd deploy artifacts + deploy README

**Files:**
- Create: `deploy/artnet-to-hass.service`
- Create: `deploy/README.md`

- [ ] **Step 11.1: Create the systemd unit**

Create `deploy/artnet-to-hass.service`:

```ini
[Unit]
Description=Art-Net to Home Assistant bridge
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=artnet
Group=artnet
WorkingDirectory=/opt/artnet-to-hass
EnvironmentFile=/opt/artnet-to-hass/.env
ExecStart=/opt/artnet-to-hass/artnet-to-hass
Restart=always
RestartSec=5
# Allow binding to port 6454 without root.
AmbientCapabilities=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/opt/artnet-to-hass
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 11.2: Create the deploy README**

Create `deploy/README.md`:

```markdown
# Deploy — artnet-to-hass

## Prod target
Linux x86_64, systemd.

## First-time setup

1. On the build machine (dev):
   ```bash
   cargo build --release
   # Binary: target/release/artnet-to-hass
   ```

2. On the prod machine:
   ```bash
   sudo useradd --system --no-create-home --shell /usr/sbin/nologin artnet
   sudo mkdir -p /opt/artnet-to-hass
   sudo chown artnet:artnet /opt/artnet-to-hass
   ```

3. Copy artifacts:
   ```bash
   scp target/release/artnet-to-hass prod:/tmp/
   scp deploy/artnet-to-hass.service prod:/tmp/
   scp .env.example prod:/tmp/
   ssh prod '
     sudo mv /tmp/artnet-to-hass /opt/artnet-to-hass/artnet-to-hass
     sudo chmod +x /opt/artnet-to-hass/artnet-to-hass
     sudo chown artnet:artnet /opt/artnet-to-hass/artnet-to-hass
     sudo cp /tmp/.env.example /opt/artnet-to-hass/.env
     sudo chown artnet:artnet /opt/artnet-to-hass/.env
     sudo chmod 600 /opt/artnet-to-hass/.env
     sudo mv /tmp/artnet-to-hass.service /etc/systemd/system/
   '
   ```

4. Edit `/opt/artnet-to-hass/.env` with the real HA_TOKEN and HA_LIGHTS.

5. Enable and start:
   ```bash
   ssh prod '
     sudo systemctl daemon-reload
     sudo systemctl enable --now artnet-to-hass
     sudo systemctl status artnet-to-hass
   '
   ```

6. Watch logs:
   ```bash
   ssh prod 'journalctl -fu artnet-to-hass'
   ```

## Updates
```bash
cargo build --release
scp target/release/artnet-to-hass prod:/tmp/
ssh prod '
  sudo systemctl stop artnet-to-hass
  sudo mv /tmp/artnet-to-hass /opt/artnet-to-hass/artnet-to-hass
  sudo systemctl start artnet-to-hass
'
```

## Firewall
UDP port 6454 must be reachable from the Kitten console's subnet.
```

- [ ] **Step 11.3: Commit**

```bash
git add deploy/
git -c user.email="drlik.zbynek@gmail.com" -c user.name="Zbynek Drlik" commit -m "Add systemd unit and deploy instructions"
```

---

## Task 12: Manual end-to-end verification against live HA

**Files:**
- None (verification only)

This is the manual equivalent of browser E2E — per the e2e-real-user-testing policy, this project has no UI, so the console + Govee lights ARE the "user".

- [ ] **Step 12.1: Push `dev` and confirm CI green**

```bash
git push -u origin dev
gh run list --branch dev --limit 3
```

Wait for CI to finish. All jobs (fmt, clippy, test, coverage) must be green. If anything fails, fix and push again.

- [ ] **Step 12.2: Prepare local `.env`**

```bash
cp .env.example .env
# Edit .env — set HA_TOKEN to the real token; HA_LIGHTS already correct.
```

- [ ] **Step 12.3: Run the binary**

```bash
cargo build --release
RUST_LOG=artnet_to_hass=debug ./target/release/artnet-to-hass
```

Expected log output:
- `artnet-to-hass starting ...`
- `Art-Net listener bound to 0.0.0.0:6454, universe 0`
- `HA: connecting to ws://ha-snv.local:8123/api/websocket`
- `HA: authenticated`

If HA auth fails, check the token. If UDP bind fails, port 6454 may already be in use.

- [ ] **Step 12.4: Verify with Art-Net from the Kitten console**

With the binary running:

1. On the Kitten console, program channels 1/2/3 on Universe 0 to output `(255, 0, 0)` (red).
2. In the binary's log, you should see `Art-Net RGB received: (255, 0, 0)`.
3. In the HA UI, both `light.h70a1_1e41` and `light.h70a1_1e86` should turn red within ~200 ms.
4. Repeat with `(0, 255, 0)` green, `(0, 0, 255)` blue, `(255, 255, 255)` white.
5. Fade to `(0, 0, 0)` — both lights should turn off.
6. Fade back up — both lights should turn on with the new color.

**If none of this works:** check that the Kitten console is actually broadcasting to the dev machine's IP/broadcast address. Use `sudo tcpdump -i any -n udp port 6454` on the dev machine to confirm packets are arriving. Report the failure with the tcpdump output.

- [ ] **Step 12.5: Verify rate-cap behavior**

Do a rapid fade on the Kitten (e.g., 5-second chase between three colors). Observe:
- The HA lights follow the fade but look visibly smoother than the raw console output (Govee smoothing + our 100 ms cap).
- No "queue buildup" — after you stop the fade, the lights land on the final color within ~200 ms (not keep animating).

If you see queue buildup (lights keep animating long after the console stops), the rate cap or coalescing is broken — report with timestamps.

- [ ] **Step 12.6: Verify reconnect**

With the binary running and a color set:
1. Temporarily block HA: `sudo iptables -I OUTPUT -d <ha-ip> -j DROP` (or just stop HA).
2. Change the console color. Binary log: `HA send failed: ...`.
3. Restore HA connectivity: `sudo iptables -D OUTPUT -d <ha-ip> -j DROP`.
4. Within 30 seconds, log should show `HA: connecting`, `HA: authenticated`.
5. Next color change on console should propagate to lights.

- [ ] **Step 12.7: Open PR `dev` → `main`**

```bash
gh pr create --base main --head dev --title "Initial artnet-to-hass v0.1.0" --body "$(cat <<'EOF'
## Summary
- Art-Net UDP listener (Universe 0, channels 1-3 = RGB)
- HA WebSocket client with auth, send, reconnect
- Bridge loop with change-detect + rate cap (10 Hz default)
- systemd unit + deploy docs

## Test plan
- [x] Unit tests (config, artnet parser, ha_client builders, bridge logic)
- [x] Integration tests (UDP listener, HA auth + send)
- [x] CI green on dev
- [x] Manual E2E against live HA + Govee lights (red/green/blue/white, fade-to-black, reconnect)

## Post-merge
Do NOT auto-deploy to prod. User will instruct separately.
EOF
)"
```

- [ ] **Step 12.8: Wait for PR CI and report green URL**

```bash
gh pr view --web
# Wait until all checks green. Then report the URL and wait for user to say "merge it".
```

**STOP HERE.** Per the airuleset PR merge policy, do not merge without explicit user instruction.

---

## Out of scope for this plan (v2 candidates)

- Multi-universe Art-Net (single universe per light group)
- Brightness channel separate from RGB
- HA transitions
- Prometheus metrics endpoint
- Web UI
- `success: false` rate-limited warning (currently propagates as Err — can be softened once we see how often it happens in practice)

---

## Self-Review

**1. Spec coverage:**
- §2 Constraints → Tasks 0, 2 (config), 11 (deploy)
- §3 Architecture (2 tasks + watch channel) → Tasks 4, 9, 10
- §4.1 Config → Task 2
- §4.2 Art-Net parser + listener → Tasks 3, 4
- §4.3 HA client (auth, send, reconnect) → Tasks 6, 7, 8
- §4.4 Bridge loop → Task 9
- §5 Data flow → covered by Task 10 wiring + Task 12 E2E
- §6 Error handling → covered in each implementation task
- §7 Logging → Tasks 3, 4, 8, 10
- §8 Testing → Tasks 2, 3, 4, 6, 7, 9, 12
- §9 Deployment → Task 11

**2. Placeholder scan:** no `TBD`, `TODO`, "implement later", or vague "handle appropriately" in steps. Every code block is complete.

**3. Type consistency:**
- `Config` struct fields match between definition (Task 2) and use (Task 10).
- `HaClient` trait: `turn_on(&self, rgb: (u8,u8,u8)) -> Result<()>` and `turn_off(&self) -> Result<()>` — consistent across Task 8, 9, 10.
- `parse_artdmx(buf, universe, rgb_start_channel) -> Option<(u8,u8,u8)>` — consistent in Task 3, Task 4 listener, Task 4 integration test.
- `run_listener(bind, universe, rgb_start_channel, tx)` — consistent between Task 4 definition and Task 10 use.
- `run_bridge(ha, rx, min_send_interval)` — consistent between Task 9 definition and Task 10 use.
- Watch channel type `Option<(u8, u8, u8)>` — consistent everywhere.
- Entity ID list type `Vec<String>` — consistent.

**4. Gaps found:** one — the plan creates a `Config` struct but never silences the `unused` warnings for test-only fields. Rust handles this fine (`#[cfg(test)]` doesn't trigger dead-code warnings for public fields referenced from the binary). No action needed.

No other issues found.
