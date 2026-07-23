//! Per-speaker Spotify Connect device: a zeroconf discovery loop that, on each new
//! set of credentials, tears down any prior session and builds a fresh
//! Session/Player/Spirc wired to this speaker's Dante pacing queue.

pub mod events;
pub mod sink;

use crate::audio::queue::QueueProducer;
use crate::config::{SpeakerConfig, SpotifyConfig};
use crate::state::StateHub;
use anyhow::{Context, Result};
use librespot_connect::{ConnectConfig, Spirc};
use librespot_core::config::{DeviceType, SessionConfig};
use librespot_core::{authentication::Credentials, cache::Cache, session::Session};
use librespot_discovery::Discovery;
use librespot_playback::config::{Bitrate, PlayerConfig};
use librespot_playback::mixer::{Mixer, MixerConfig, NoOpVolume, VolumeGetter};
use librespot_playback::mixer::softmixer::SoftMixer;
use librespot_playback::player::Player;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

use futures_util::StreamExt;

/// Handle used by the API to send commands to a speaker's current Spirc (if any).
#[derive(Clone)]
pub struct SpeakerHandle {
    pub id: String,
    /// Human-readable speaker name; exposed for callers/logging.
    #[allow(dead_code)]
    pub name: String,
    spirc: Arc<Mutex<Option<Spirc>>>,
}

impl SpeakerHandle {
    fn new(id: String, name: String) -> Self {
        Self {
            id,
            name,
            spirc: Arc::new(Mutex::new(None)),
        }
    }

    fn set(&self, spirc: Option<Spirc>) {
        *self.spirc.lock().unwrap() = spirc;
    }

    /// Run a command against the active Spirc. Returns `false` if no session is active.
    pub fn with_spirc<F: FnOnce(&Spirc) -> Result<(), librespot_core::Error>>(
        &self,
        f: F,
    ) -> CommandResult {
        let guard = self.spirc.lock().unwrap();
        match guard.as_ref() {
            Some(spirc) => match f(spirc) {
                Ok(()) => CommandResult::Ok,
                Err(e) => CommandResult::Failed(e.to_string()),
            },
            None => CommandResult::Inactive,
        }
    }
}

pub enum CommandResult {
    Ok,
    Inactive,
    Failed(String),
}

/// Registry of all speaker handles, keyed by id, for the API layer.
#[derive(Clone, Default)]
pub struct SpeakerRegistry {
    map: Arc<Mutex<HashMap<String, SpeakerHandle>>>,
}

impl SpeakerRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    fn insert(&self, handle: SpeakerHandle) {
        self.map.lock().unwrap().insert(handle.id.clone(), handle);
    }
    pub fn get(&self, id: &str) -> Option<SpeakerHandle> {
        self.map.lock().unwrap().get(id).cloned()
    }
}

/// Derive a stable id/slug and the librespot device_id from a speaker name.
pub fn speaker_id(name: &str) -> String {
    let mut slug = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c.to_ascii_lowercase());
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.trim_matches('-').to_string()
}

fn device_id(name: &str) -> String {
    hex::encode(Sha1::digest(name.as_bytes()))
}

/// Spawn the discovery/session loop for one speaker. Never returns unless discovery
/// terminates (fatal mDNS error).
pub async fn run_speaker(
    speaker: SpeakerConfig,
    spotify: SpotifyConfig,
    producer: Arc<Mutex<QueueProducer>>,
    dante_rate: u32,
    hub: StateHub,
    handle: SpeakerHandle,
) -> Result<()> {
    let device_id = device_id(&speaker.name);
    let session_config = SessionConfig {
        device_id: device_id.clone(),
        ..Default::default()
    };

    let zeroconf_ip: Vec<IpAddr> = match &spotify.interface_ip {
        Some(ip) => vec![ip.parse().with_context(|| format!("parsing spotify.interface_ip '{ip}'"))?],
        None => vec![],
    };

    let mut discovery = Discovery::builder(device_id.clone(), session_config.client_id.clone())
        .name(speaker.name.clone())
        .device_type(DeviceType::Speaker)
        .port(0)
        .zeroconf_ip(zeroconf_ip)
        .launch()
        .with_context(|| format!("launching discovery for '{}'", speaker.name))?;

    tracing::info!("speaker '{}' discoverable (device_id {})", speaker.name, device_id);

    // The active Spirc lives inside `handle` (Spirc is not Clone); we only need to
    // track its background task here so we can abort it on the next credential.
    let mut active_task: Option<JoinHandle<()>> = None;

    while let Some(credentials) = discovery.next().await {
        tracing::info!("speaker '{}' received credentials, (re)starting session", speaker.name);
        if let Some(task) = active_task.take() {
            handle.with_spirc(|s| s.shutdown());
            handle.set(None);
            task.abort();
        }

        match start_session(
            &speaker,
            &spotify,
            &session_config,
            dante_rate,
            credentials,
            producer.clone(),
            hub.clone(),
        )
        .await
        {
            Ok((spirc, spirc_task)) => {
                handle.set(Some(spirc));
                active_task = Some(spirc_task);
            }
            Err(e) => {
                tracing::error!("speaker '{}' failed to start session: {e:#}", speaker.name);
            }
        }
    }

    tracing::warn!("speaker '{}' discovery stream ended", speaker.name);
    Ok(())
}

async fn start_session(
    speaker: &SpeakerConfig,
    spotify: &SpotifyConfig,
    session_config: &SessionConfig,
    dante_rate: u32,
    credentials: Credentials,
    producer: Arc<Mutex<QueueProducer>>,
    hub: StateHub,
) -> Result<(Spirc, JoinHandle<()>)> {
    let cache = match &spotify.cache_dir {
        Some(dir) => {
            let base = PathBuf::from(dir).join(speaker_id(&speaker.name));
            Some(
                Cache::new(Some(&base), Some(&base), Some(&base.join("audio")), None)
                    .context("opening cache")?,
            )
        }
        None => None,
    };

    let session = Session::new(session_config.clone(), cache);

    let mixer: Arc<dyn Mixer> =
        Arc::new(SoftMixer::open(MixerConfig::default()).context("opening mixer")?);
    let volume_getter: Box<dyn VolumeGetter + Send> = if speaker.apply_volume {
        mixer.get_soft_volume()
    } else {
        Box::new(NoOpVolume)
    };

    let player_config = PlayerConfig {
        bitrate: bitrate_from(spotify.bitrate),
        position_update_interval: Some(Duration::from_secs(1)),
        ..Default::default()
    };

    let player = Player::new(player_config, session.clone(), volume_getter, move || {
        Box::new(sink::DanteSink::new(dante_rate, producer))
    });

    let id = speaker_id(&speaker.name);
    tokio::spawn(events::pump(player.get_player_event_channel(), hub, id));

    let connect_config = ConnectConfig {
        name: speaker.name.clone(),
        device_type: DeviceType::Speaker,
        initial_volume: speaker
            .initial_volume
            .map(|v| (v.clamp(0.0, 1.0) * u16::MAX as f32) as u16)
            .unwrap_or(u16::MAX / 2),
        ..Default::default()
    };

    let (spirc, spirc_task) = Spirc::new(connect_config, session, credentials, player, mixer)
        .await
        .context("creating Spirc")?;

    let task = tokio::spawn(spirc_task);
    Ok((spirc, task))
}

/// Build the registry + per-speaker handles up front, in config order.
pub fn build_registry(speakers: &[SpeakerConfig]) -> (SpeakerRegistry, Vec<SpeakerHandle>) {
    let registry = SpeakerRegistry::new();
    let mut handles = Vec::new();
    for sp in speakers {
        let handle = SpeakerHandle::new(speaker_id(&sp.name), sp.name.clone());
        registry.insert(handle.clone());
        handles.push(handle);
    }
    (registry, handles)
}

fn bitrate_from(kbps: u32) -> Bitrate {
    match kbps {
        96 => Bitrate::Bitrate96,
        320 => Bitrate::Bitrate320,
        _ => Bitrate::Bitrate160,
    }
}
