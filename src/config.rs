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
    /// Omit the block entirely to run without an MQTT control surface.
    #[serde(default)]
    pub mqtt: Option<MqttConfig>,
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
pub struct MqttConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// "host" or "host:port". Port defaults to 1883.
    pub broker: String,
    /// null -> "lucyfer".
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// File whose trimmed contents are the password. Mutually exclusive with `password`.
    #[serde(default)]
    pub password_file: Option<String>,
    /// Topic prefix: state is published at `<prefix>/<mqtt_name>/status`.
    #[serde(default = "default_mqtt_prefix")]
    pub prefix: String,
}

impl MqttConfig {
    /// Host and port split out of `broker`.
    pub fn host_port(&self) -> Result<(String, u16)> {
        let broker = self.broker.trim();
        // Strip an optional scheme so both "mqtt://host:1883" and "host:1883" work.
        let broker = broker
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or(broker);
        let broker = broker.trim_end_matches('/');
        match broker.rsplit_once(':') {
            Some((host, port)) => {
                let port = port
                    .parse()
                    .with_context(|| format!("parsing mqtt.broker port '{port}'"))?;
                anyhow::ensure!(!host.is_empty(), "mqtt.broker has no host");
                Ok((host.to_string(), port))
            }
            None => {
                anyhow::ensure!(!broker.is_empty(), "mqtt.broker is empty");
                Ok((broker.to_string(), 1883))
            }
        }
    }

    /// The password, read from `password_file` when that is the form given.
    pub fn resolve_password(&self) -> Result<Option<String>> {
        if let Some(path) = &self.password_file {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading mqtt.password_file {path}"))?;
            return Ok(Some(text.trim().to_string()));
        }
        Ok(self.password.clone())
    }

    pub fn client_id(&self) -> String {
        self.client_id.clone().unwrap_or_else(|| "lucyfer".into())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpeakerConfig {
    pub name: String,
    /// Topic segment for this speaker. null -> the slug of `name` ("living-room").
    #[serde(default)]
    pub mqtt_name: Option<String>,
    /// Topic carrying the amplifier's *current* volume (0-100). Subscribed: a value here
    /// is pushed into Spotify/AirPlay so their sliders show the truth.
    #[serde(default)]
    pub volume_topic: Option<String>,
    /// Topic the amplifier takes commands on (0-100). Published to when the user changes
    /// the volume from the Spotify or AirPlay app.
    #[serde(default)]
    pub volume_topic_set: Option<String>,
}

impl SpeakerConfig {
    /// The `<speaker_name>` topic segment.
    pub fn mqtt_name(&self) -> String {
        self.mqtt_name
            .clone()
            .unwrap_or_else(|| crate::source::speaker_id(&self.name))
    }
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
        if let Some(mqtt) = &self.mqtt {
            anyhow::ensure!(
                !(mqtt.password.is_some() && mqtt.password_file.is_some()),
                "mqtt.password and mqtt.password_file are mutually exclusive"
            );
            mqtt.host_port()?;
        }
        let mqtt_active = self.mqtt.as_ref().is_some_and(|m| m.enabled);

        let mut names = std::collections::HashSet::new();
        let mut mqtt_names = std::collections::HashSet::new();
        for sp in &self.speakers {
            anyhow::ensure!(
                names.insert(sp.name.clone()),
                "duplicate speaker name: {}",
                sp.name
            );

            let mqtt_name = sp.mqtt_name();
            anyhow::ensure!(
                !mqtt_name.is_empty(),
                "speaker {} has an empty mqtt_name (the slug of its name is empty; set mqtt_name explicitly)",
                sp.name
            );
            anyhow::ensure!(
                !mqtt_name.contains(['+', '#', '/', '\0']),
                "speaker {} mqtt_name '{}' must not contain '+', '#', '/' or NUL",
                sp.name,
                mqtt_name
            );
            anyhow::ensure!(
                mqtt_names.insert(mqtt_name.clone()),
                "duplicate mqtt_name: {}",
                mqtt_name
            );

            // One without the other is a half-loop that silently does nothing useful.
            anyhow::ensure!(
                sp.volume_topic.is_some() == sp.volume_topic_set.is_some(),
                "speaker {}: volume_topic and volume_topic_set must be set together",
                sp.name
            );
            if !mqtt_active && sp.volume_topic.is_some() {
                tracing::warn!(
                    "speaker '{}' sets volume topics but MQTT is not enabled; they are ignored",
                    sp.name
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
fn default_mqtt_prefix() -> String {
    "lucyfer".to_string()
}
