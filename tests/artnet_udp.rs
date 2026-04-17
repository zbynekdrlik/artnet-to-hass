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
    dmx[0] = 1;
    dmx[1] = 2;
    dmx[2] = 3;
    client.send_to(&build_artdmx(0, &dmx), addr).await.unwrap();

    tokio::time::timeout(Duration::from_secs(1), rx.changed())
        .await
        .unwrap()
        .unwrap();
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
    assert!(
        result.is_err(),
        "expected no update, got {:?}",
        *rx.borrow()
    );

    handle.abort();
}
