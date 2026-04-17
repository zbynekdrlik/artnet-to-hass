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
            self.calls
                .lock()
                .unwrap()
                .push((Instant::now(), Call::On(rgb.0, rgb.1, rgb.2)));
            if std::mem::replace(&mut *self.fail_next.lock().unwrap(), false) {
                return Err(anyhow::anyhow!("mock failure"));
            }
            Ok(())
        }
        async fn turn_off(&self) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push((Instant::now(), Call::Off));
            if std::mem::replace(&mut *self.fail_next.lock().unwrap(), false) {
                return Err(anyhow::anyhow!("mock failure"));
            }
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
