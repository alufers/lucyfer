//! YAML configuration schema and loading.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub dante: DanteConfig,
    #[serde(default)]
    pub spotify: SpotifyConfig,
    #[serde(default)]
    pub airplay: AirPlayConfig,
    pub speakers: Vec<SpeakerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DanteConfig {
    /// IPv4 address or interface name (inferno resolves both). Maps to `BIND_IP`.
    pub interface: String,
    #[serde(default = "default_device_name")]
    pub device_name: String,
    #[serde(default = "default_sample_rate")]
    pub sample_rate: u32,
    #[serde(default = "default_tx_latency_ns")]
    pub tx_latency_ns: u32,
    /// null -> inferno default usrvclock socket; else a socket path or "/dev/ptp0".
    #[serde(default)]
    pub clock_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpotifyConfig {
    /// Advertise every speaker over Spotify Connect.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// mDNS-advertised IP for the Spotify Connect side. null -> all interfaces.
    #[serde(default)]
    pub interface_ip: Option<String>,
    #[serde(default = "default_bitrate")]
    pub bitrate: u32,
    #[serde(default)]
    pub cache_dir: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AirPlayConfig {
    /// Advertise every speaker over AirPlay.
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub interface_ip: Option<String>,
    /// RTSP port for the first speaker; speaker N listens on `base_port + N`.
    #[serde(default = "default_airplay_base_port")]
    pub base_port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpeakerConfig {
    pub name: String,
    #[serde(default = "default_true")]
    pub apply_volume: bool,
    /// Initial volume 0.0 - 1.0 applied when a session connects.
    #[serde(default)]
    pub initial_volume: Option<f32>,
}

impl Default for SpotifyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interface_ip: None,
            bitrate: default_bitrate(),
            cache_dir: None,
        }
    }
}

impl Default for AirPlayConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interface_ip: None,
            base_port: default_airplay_base_port(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        let cfg: Config =
            serde_yaml::from_str(&text).with_context(|| "parsing config YAML".to_string())?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(!self.speakers.is_empty(), "at least one speaker required");
        anyhow::ensure!(
            self.spotify.enabled || self.airplay.enabled,
            "at least one audio source must be enabled (spotify.enabled / airplay.enabled)"
        );
        if self.airplay.enabled {
            // Speaker N listens on base_port + N, so the whole block must fit.
            anyhow::ensure!(
                u16::try_from(self.airplay.base_port as usize + self.speakers.len() - 1).is_ok(),
                "airplay.base_port ({}) leaves no room for {} speaker(s) below port 65535",
                self.airplay.base_port,
                self.speakers.len()
            );
        }
        let mut names = std::collections::HashSet::new();
        for sp in &self.speakers {
            anyhow::ensure!(
                names.insert(sp.name.clone()),
                "duplicate speaker name: {}",
                sp.name
            );
            if let Some(v) = sp.initial_volume {
                anyhow::ensure!(
                    (0.0..=1.0).contains(&v),
                    "speaker {} initial_volume {} out of range 0.0-1.0",
                    sp.name,
                    v
                );
            }
        }
        Ok(())
    }
}

fn default_device_name() -> String {
    "lucyfer".to_string()
}
fn default_sample_rate() -> u32 {
    48000
}
fn default_tx_latency_ns() -> u32 {
    10_000_000
}
fn default_bitrate() -> u32 {
    320
}
fn default_airplay_base_port() -> u16 {
    5000
}
fn default_true() -> bool {
    true
}