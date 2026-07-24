//! lucyfer — expose Spotify Connect and AirPlay speakers, transmitting their audio
//! over Dante.

mod api;
mod audio;
mod config;
mod dante;
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

    // One pacing queue per speaker, shared by every source through `SpeakerAudio`.
    let pacing_frames =
        (cfg.dante.sample_rate as u64 * cfg.audio.pacing_buffer_ms as u64 / 1000) as usize;
    let lead_samples = (cfg.dante.sample_rate as u64 * cfg.audio.lead_ms as u64 / 1000) as usize;

    let registry = SpeakerRegistry::new();
    let mut speaker_audio = Vec::new();
    let mut consumers = Vec::new();
    let mut speaker_names = Vec::new();
    for sp in &cfg.speakers {
        let id = speaker_id(&sp.name);
        hub.register(SpeakerState::new(
            id.clone(),
            sp.name.clone(),
            sp.apply_volume,
            sources.clone(),
        ));
        let (producer, consumer) = audio::queue::channel(pacing_frames);
        let audio = Arc::new(SpeakerAudio::new(
            id,
            sp.name.clone(),
            hub.clone(),
            producer,
            // Real-time sources (AirPlay) keep the queue around half full so a
            // free-running sender clock has drift headroom in both directions.
            pacing_frames / 2,
        ));
        registry.insert(audio.clone());
        speaker_audio.push(audio);
        consumers.push(consumer);
        speaker_names.push(sp.name.clone());
    }

    // Start the Dante device + ring writer. Startup does NOT block on the media clock:
    // discovery and the API come up immediately; only audio TX is gated until a media
    // clock (PTP/usrvclock) becomes available.
    let dante = dante::DanteOutput::start(&cfg.dante, &speaker_names, consumers, lead_samples)
        .await
        .context("starting Dante output")?;

    // Spawn one task per (speaker, enabled source).
    let mut source_tasks = Vec::new();
    for (index, (sp, audio)) in cfg.speakers.iter().zip(speaker_audio.iter()).enumerate() {
        if cfg.spotify.enabled {
            let (sp, spotify, audio, hub) = (
                sp.clone(),
                cfg.spotify.clone(),
                audio.clone(),
                hub.clone(),
            );
            let rate = cfg.dante.sample_rate;
            source_tasks.push(tokio::spawn(async move {
                let name = sp.name.clone();
                if let Err(e) = source::spotify::run_speaker(sp, spotify, audio, rate, hub).await {
                    tracing::error!("speaker '{name}' Spotify source ended with error: {e:#}");
                }
            }));
        }
        if cfg.airplay.enabled {
            let (sp, airplay, audio, hub) = (
                sp.clone(),
                cfg.airplay.clone(),
                audio.clone(),
                hub.clone(),
            );
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

    // Serve the API.
    let app = api::router(hub, registry);
    let listener = tokio::net::TcpListener::bind(&cfg.api.bind)
        .await
        .with_context(|| format!("binding API to {}", cfg.api.bind))?;
    tracing::info!("API listening on http://{}", cfg.api.bind);

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

    if let Err(e) = server.await {
        tracing::error!("API server error: {e:#}");
    }

    tracing::info!("shutting down");
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
