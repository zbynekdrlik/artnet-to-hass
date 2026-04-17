/// Parse an ArtDmx packet and extract an RGB triplet from the configured start channel.
///
/// Returns `Some((r, g, b))` only for valid ArtDmx packets on the matching universe.
/// All other packets (ArtPoll, wrong universe, malformed, etc.) return `None`.
pub fn parse_artdmx(buf: &[u8], universe: u16, rgb_start_channel: u16) -> Option<(u8, u8, u8)> {
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
    Some((
        payload[start_idx],
        payload[start_idx + 1],
        payload[start_idx + 2],
    ))
}

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
                return Err(anyhow::anyhow!(
                    "watch receiver dropped; shutting down listener"
                ));
            }
        }
    }
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
        pkt.extend_from_slice(&14u16.to_be_bytes()); // ProtVer hi,lo big-endian
        pkt.push(0); // Sequence
        pkt.push(0); // Physical
        pkt.extend_from_slice(&universe.to_le_bytes()); // PortAddress little-endian
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

    #[test]
    fn rejects_declared_length_beyond_buffer() {
        // Sender claims 100 payload bytes but only 2 are actually present.
        // Simulates a truncated UDP datagram.
        let mut pkt = build_artdmx(0, &[10u8, 20]);
        pkt[16] = 0;
        pkt[17] = 100;
        assert_eq!(parse_artdmx(&pkt, 0, 1), None);
    }

    #[test]
    fn rejects_zero_start_channel() {
        // rgb_start_channel is 1-indexed; 0 is always invalid.
        let pkt = build_artdmx(0, &vec![0u8; 512]);
        assert_eq!(parse_artdmx(&pkt, 0, 0), None);
    }
}
