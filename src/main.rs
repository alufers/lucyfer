//! lucyfer — expose Spotify Connect and AirPlay speakers, transmitting their audio
//! over Dante.

mod config;
mod dante;
mod mqtt;
mod resampler;
mod source;
mod state;

use anyhow::{Context, Result};
use clap::Parser;
use config::Config;
use source::{SourceKind, SpeakerAudio, SpeakerRegistry, speaker_id};
use state::{SpeakerState, StateHub};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(
    name = "lucyfer",
    version,
    about = "Spotify Connect / AirPlay -> Dante bridge"
)]
struct Args {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "/etc/lucyfer/config.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,lucyfer=debug")),
        )
        .init();

    let args = Args::parse();
    let cfg = Config::load(&args.config)?;

    let mut sources = Vec::new();
    if cfg.spotify.enabled {
        sources.push(SourceKind::Spotify);
    }
    if cfg.airplay.enabled {
        sources.push(SourceKind::Airplay);
    }
    tracing::info!(
        "loaded config with {} speaker(s), sources: {}",
        cfg.speakers.len(),
        sources
            .iter()
            .map(|s| s.label())
            .collect::<Vec<_>>()
            .join(" + ")
    );

    let hub = StateHub::new();

    // Start the Dante device. Startup does NOT block on the media clock: discovery comes
    // up immediately; only audio TX is gated until a media clock (PTP/usrvclock) becomes
    // available.
    let speaker_names: Vec<String> = cfg.speakers.iter().map(|sp| sp.name.clone()).collect();
    let (dante, sinks) = dante::DanteOutput::start(&cfg.dante, &speaker_names)
        .await
        .context("starting Dante output")?;

    // One sink per speaker, shared by every source through `SpeakerAudio`.
    let registry = SpeakerRegistry::new();
    let mut speaker_audio = Vec::new();
    for (sp, sink) in cfg.speakers.iter().zip(sinks) {
        let id = speaker_id(&sp.name);
        hub.register(SpeakerState::new(
            id.clone(),
            sp.name.clone(),
            sources.clone(),
        ));
        let audio = Arc::new(SpeakerAudio::new(id, sp.name.clone(), hub.clone(), sink));
        registry.insert(audio.clone());
        speaker_audio.push(audio);
    }

    // Spawn one task per (speaker, enabled source).
    let mut source_tasks = Vec::new();
    for (index, (sp, audio)) in cfg.speakers.iter().zip(speaker_audio.iter()).enumerate() {
        if cfg.spotify.enabled {
            let (sp, spotify, audio, hub) =
                (sp.clone(), cfg.spotify.clone(), audio.clone(), hub.clone());
            let rate = cfg.dante.sample_rate;
            source_tasks.push(tokio::spawn(async move {
                let name = sp.name.clone();
                if let Err(e) = source::spotify::run_speaker(sp, spotify, audio, rate, hub).await {
                    tracing::error!("speaker '{name}' Spotify source ended with error: {e:#}");
                }
            }));
        }
        if cfg.airplay.enabled {
            let (sp, airplay, audio, hub) =
                (sp.clone(), cfg.airplay.clone(), audio.clone(), hub.clone());
            let rate = cfg.dante.sample_rate;
            let port = cfg.airplay.base_port + index as u16;
            source_tasks.push(tokio::spawn(async move {
                let name = sp.name.clone();
                if let Err(e) =
                    source::airplay::run_speaker(sp, airplay, port, audio, rate, hub).await
                {
                    tracing::error!("speaker '{name}' AirPlay source ended with error: {e:#}");
                }
            }));
        }
    }

    // The MQTT control surface, when configured. It owns the read side of the hub and
    // the command side of the registry.
    let mqtt_task = match &cfg.mqtt {
        Some(m) if m.enabled => {
            let (cfg, hub, registry) = (cfg.clone(), hub.clone(), registry.clone());
            Some(tokio::spawn(async move {
                if let Err(e) = mqtt::run(&cfg, hub, registry).await {
                    tracing::error!("MQTT client ended with error: {e:#}");
                }
            }))
        }
        _ => {
            tracing::info!("MQTT not configured; running without a control surface");
            None
        }
    };

    shutdown_signal().await;

    tracing::info!("shutting down");
    if let Some(t) = mqtt_task {
        t.abort();
    }
    for t in source_tasks {
        t.abort();
    }
    dante.shutdown().await;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
