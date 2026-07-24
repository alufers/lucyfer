//! lucyfer — expose Spotify Connect speakers and transmit their audio over Dante.

mod api;
mod audio;
mod config;
mod dante;
mod speaker;
mod state;

use anyhow::{Context, Result};
use clap::Parser;
use config::Config;
use state::{SpeakerState, StateHub};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Parser, Debug)]
#[command(name = "lucyfer", version, about = "Spotify Connect -> Dante bridge")]
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
    tracing::info!("loaded config with {} speaker(s)", cfg.speakers.len());

    let hub = StateHub::new();
    let (registry, handles) = speaker::build_registry(&cfg.speakers);

    // Register initial state and build one pacing queue per speaker.
    let pacing_frames =
        (cfg.dante.sample_rate as u64 * cfg.audio.pacing_buffer_ms as u64 / 1000) as usize;
    let lead_samples =
        (cfg.dante.sample_rate as u64 * cfg.audio.lead_ms as u64 / 1000) as usize;

    let mut producers = Vec::new();
    let mut consumers = Vec::new();
    let mut speaker_names = Vec::new();
    for sp in &cfg.speakers {
        hub.register(SpeakerState::new(
            speaker::speaker_id(&sp.name),
            sp.name.clone(),
            sp.apply_volume,
        ));
        let (prod, cons) = audio::queue::channel(pacing_frames);
        producers.push(Arc::new(Mutex::new(prod)));
        consumers.push(cons);
        speaker_names.push(sp.name.clone());
    }

    // Start the Dante device + ring writer. Startup does NOT block on the media clock:
    // discovery and the API come up immediately; only audio TX is gated until a media
    // clock (PTP/usrvclock) becomes available.
    let dante = dante::DanteOutput::start(&cfg.dante, &speaker_names, consumers, lead_samples)
        .await
        .context("starting Dante output")?;

    // Spawn one discovery/session loop per speaker.
    let mut speaker_tasks = Vec::new();
    for ((sp, producer), handle) in cfg
        .speakers
        .iter()
        .cloned()
        .zip(producers.into_iter())
        .zip(handles.into_iter())
    {
        let spotify = cfg.spotify.clone();
        let hub = hub.clone();
        let rate = cfg.dante.sample_rate;
        speaker_tasks.push(tokio::spawn(async move {
            if let Err(e) = speaker::run_speaker(sp, spotify, producer, rate, hub, handle).await {
                tracing::error!("speaker task ended with error: {e:#}");
            }
        }));
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
    for t in speaker_tasks {
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
