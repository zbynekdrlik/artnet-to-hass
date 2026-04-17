use anyhow::Result;
use artnet_to_hass::{artnet, bridge, config::Config, ha_client::ReconnectingHaClient};
use tokio::signal;
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env (ignore if missing).
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
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
    let ha = ReconnectingHaClient::spawn(
        cfg.ha_url.clone(),
        cfg.ha_token.clone(),
        cfg.ha_lights.clone(),
    );

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
