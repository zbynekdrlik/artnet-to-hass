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
            return Err(anyhow!(
                "RGB_START_CHANNEL must be in 1..=510 (got {rgb_start_channel})"
            ));
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

    // Serializes env mutations across tests so `cargo test` (which runs tests in
    // parallel by default) doesn't race on process-global state.
    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_env<F, T>(vars: &[(&str, String)], f: F) -> T
    where
        F: FnOnce() -> T,
    {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
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
        assert_eq!(
            cfg.ha_lights,
            vec!["light.a".to_string(), "light.b".to_string()]
        );
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
        env.iter_mut().find(|(k, _)| *k == "HA_LIGHTS").unwrap().1 = " light.a , light.b ".into();
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
