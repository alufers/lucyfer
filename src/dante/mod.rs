//! Dante (inferno_aoip) TX side: one `DeviceServer` exposing two TX channels per
//! speaker, fed by timeline-indexed ring buffers written by the `RingWriter` thread.

pub mod writer;

use crate::audio::QueueConsumer;
use crate::config::DanteConfig;
use anyhow::{Context, Result};
use inferno_aoip::device_server::{
    AtomicSample, DeviceServer, ExternalBufferParameters, MediaClock, Sample, Settings,
    TransferNotifier,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;
use writer::RingWriter;

/// A single timeline-indexed TX ring. The transmitter reads `buf[ts & mask]`.
pub struct TimelineRing {
    buf: Arc<Vec<AtomicSample>>,
    valid: Arc<RwLock<bool>>,
    mask: usize,
}

impl TimelineRing {
    fn new(len: usize) -> Self {
        assert!(len.is_power_of_two());
        let buf = Arc::new((0..len).map(|_| AtomicSample::new(0)).collect::<Vec<_>>());
        Self {
            buf,
            valid: Arc::new(RwLock::new(true)),
            mask: len - 1,
        }
    }

    #[inline]
    pub fn write(&self, ts: usize, s: Sample) {
        self.buf[ts & self.mask].store(s, Ordering::Relaxed);
    }

    #[cfg(test)]
    #[inline]
    pub fn read(&self, ts: usize) -> Sample {
        self.buf[ts & self.mask].load(Ordering::Relaxed)
    }

    /// Build the `ExternalBufferParameters` inferno needs to read this ring.
    ///
    /// # Safety
    /// The returned params hold a raw pointer into `self.buf`. inferno only reads
    /// through it while the `valid` flag is true; we keep `buf` alive for the whole
    /// process (it is `Arc`-cloned into the struct) and flip `valid` to false before
    /// teardown, so the pointer never dangles during a read.
    fn params(&self) -> ExternalBufferParameters<Sample> {
        unsafe {
            ExternalBufferParameters::new(
                self.buf.as_ptr(),
                self.buf.len(),
                1,
                self.valid.clone(),
                None, // unconditional read (timeline-indexed)
            )
        }
    }

    fn clone_handle(&self) -> Self {
        Self {
            buf: self.buf.clone(),
            valid: self.valid.clone(),
            mask: self.mask,
        }
    }

    fn invalidate(&self) {
        *self.valid.write().unwrap() = false;
    }
}

pub struct DanteOutput {
    pub server: DeviceServer,
    rings: Vec<TimelineRing>,
    writer_shutdown: Arc<AtomicBool>,
    writer_handle: Option<std::thread::JoinHandle<()>>,
    wake: Arc<(Mutex<bool>, Condvar)>,
}

impl DanteOutput {
    /// Start the Dante device and the ring writer. Consumes one `QueueConsumer` per
    /// speaker (in the same order as `speaker_names`).
    ///
    /// NOTE: `DeviceServer::start` blocks until a media clock is available.
    pub async fn start(
        cfg: &DanteConfig,
        speaker_names: &[String],
        consumers: Vec<QueueConsumer>,
        lead_samples: usize,
    ) -> Result<Self> {
        assert_eq!(speaker_names.len(), consumers.len());
        let tx_channels = speaker_names.len() * 2;

        let mut config = BTreeMap::new();
        config.insert("BIND_IP".to_string(), cfg.interface.clone());
        config.insert("NAME".to_string(), cfg.device_name.clone());
        config.insert("SAMPLE_RATE".to_string(), cfg.sample_rate.to_string());
        config.insert("TX_CHANNELS".to_string(), tx_channels.to_string());
        config.insert("RX_CHANNELS".to_string(), "0".to_string());
        config.insert("TX_LATENCY_NS".to_string(), cfg.tx_latency_ns.to_string());
        if let Some(clock_path) = &cfg.clock_path {
            config.insert("CLOCK_PATH".to_string(), clock_path.clone());
        }

        let mut settings = Settings::new(&cfg.device_name, "lucyfer", None, &config);
        settings.make_tx_channels(tx_channels);
        // Name channels "<speaker> L" / "<speaker> R".
        for (i, name) in speaker_names.iter().enumerate() {
            *settings.self_info.tx_channels[i * 2]
                .friendly_name
                .write()
                .unwrap() = format!("{name} L");
            *settings.self_info.tx_channels[i * 2 + 1]
                .friendly_name
                .write()
                .unwrap() = format!("{name} R");
        }

        tracing::info!(
            "starting Dante device '{}' on {} ({} TX channels @ {} Hz)",
            cfg.device_name,
            cfg.interface,
            tx_channels,
            cfg.sample_rate
        );
        describe_clock_source(&cfg.clock_path);
        let mut server = DeviceServer::start(settings).await;
        let sample_rate = cfg.sample_rate as u64;

        // Build rings (2 per speaker) and their external params.
        let rings: Vec<TimelineRing> =
            (0..tx_channels).map(|_| TimelineRing::new(cfg.ring_len)).collect();
        let params: Vec<ExternalBufferParameters<Sample>> = rings.iter().map(|r| r.params()).collect();

        let current_timestamp = Arc::new(AtomicUsize::new(usize::MAX));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let notifier = {
            let wake = wake.clone();
            TransferNotifier {
                callback: Box::new(move || {
                    let (lock, cvar) = &*wake;
                    if let Ok(mut g) = lock.lock() {
                        *g = true;
                        cvar.notify_one();
                    }
                }),
                max_interval_samples: (sample_rate / 100) as usize, // ~10 ms
            }
        };

        let (start_tx, start_rx) = tokio::sync::oneshot::channel::<usize>();
        server
            .transmit_from_external_buffer(params, start_rx, current_timestamp, Some(notifier))
            .await;

        // Anchor the TX timeline once a media clock is actually available. This runs
        // in the background so the Spotify Connect side and the API come up
        // immediately even when no clock is present yet (audio simply starts flowing
        // once the clock arrives). Blocking here would gate discovery on the clock.
        {
            let clock_rx = server.get_realtime_clock_receiver();
            let clock_path = cfg.clock_path.clone();
            tokio::spawn(async move {
                tracing::warn!(
                    "Dante TX is waiting for a media clock (PTP/usrvclock); \
                     no audio will be transmitted until one is available"
                );
                let start_ts = wait_for_clock(clock_rx, sample_rate, clock_path).await;
                let _ = start_tx.send(start_ts);
                tracing::info!("Dante media clock acquired; TX timeline anchored");
            });
        }

        // Spawn the ring writer thread.
        let writer_shutdown = Arc::new(AtomicBool::new(false));
        let writer = RingWriter {
            rings: rings.iter().map(|r| r.clone_handle()).collect(),
            consumers,
            clock_rx: server.get_realtime_clock_receiver(),
            sample_rate,
            lead_samples,
        };
        let writer_handle = {
            let wake = wake.clone();
            let shutdown = writer_shutdown.clone();
            std::thread::Builder::new()
                .name("dante-ring-writer".to_string())
                .spawn(move || {
                    if let Err(e) = raise_thread_priority() {
                        tracing::warn!("could not raise ring-writer thread priority: {e:#}");
                    }
                    writer.run(wake, shutdown);
                })
                .context("spawning ring writer thread")?
        };

        Ok(Self {
            server,
            rings,
            writer_shutdown,
            writer_handle: Some(writer_handle),
            wake,
        })
    }

    pub async fn shutdown(mut self) {
        // Stop the writer first so it no longer touches the rings.
        self.writer_shutdown.store(true, Ordering::Relaxed);
        let (lock, cvar) = &*self.wake;
        if let Ok(mut g) = lock.lock() {
            *g = true;
            cvar.notify_all();
        }
        if let Some(h) = self.writer_handle.take() {
            let _ = h.join();
        }
        // Invalidate rings before the transmitter is torn down / buffers drop.
        for r in &self.rings {
            r.invalidate();
        }
        self.server.shutdown().await;
    }
}

/// The effective media-clock source path: the configured `clock_path`, or inferno's
/// default usrvclock socket when unset.
fn resolve_clock_path(clock_path: &Option<String>) -> String {
    clock_path
        .clone()
        .unwrap_or_else(|| usrvclock::DEFAULT_SERVER_SOCKET_PATH.to_string())
}

/// Log the effective media-clock source at startup so a missing or invalid one is
/// obvious. This never fails or aborts: by design the service still comes up without a
/// clock (discovery + API), and only audio TX is gated until a clock arrives.
fn describe_clock_source(clock_path: &Option<String>) {
    use std::os::unix::fs::FileTypeExt;

    let path = resolve_clock_path(clock_path);
    match std::fs::metadata(&path) {
        Ok(md) => {
            let ft = md.file_type();
            if ft.is_char_device() {
                tracing::info!("media clock: using PTP char device '{path}'");
            } else if ft.is_socket() {
                tracing::info!("media clock: using usrvclock server socket '{path}'");
            } else {
                tracing::warn!(
                    "media clock: '{path}' is neither a char device nor a socket, so it \
                     is almost certainly not a valid clock source. Set dante.clock_path \
                     to a PTP char device (e.g. /dev/ptp0) or a usrvclock socket."
                );
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::error!(
                "media clock: '{path}' does not exist — no PTP char device and no \
                 usrvclock server socket. NO AUDIO will be transmitted until a media \
                 clock is available. Run a PTP daemon that publishes a usrvclock socket \
                 (Statime/ptp4l bridge) and point dante.clock_path at it, or set it to a \
                 PTP char device such as /dev/ptp0."
            );
        }
        Err(e) => {
            tracing::warn!("media clock: cannot stat '{path}': {e}");
        }
    }
}

async fn wait_for_clock(
    mut clock_rx: inferno_aoip::device_server::RealTimeClockReceiver,
    sample_rate: u64,
    clock_path: Option<String>,
) -> usize {
    let resolved = resolve_clock_path(&clock_path);
    let mut media_clock = MediaClock::new(false);
    let mut iters: u64 = 0;
    loop {
        clock_rx.update();
        if let Some(overlay) = clock_rx.get() {
            media_clock.update_overlay(*overlay);
            if let Some(now) = media_clock.wrapping_now_in_timebase(sample_rate) {
                return now as usize;
            }
        }
        iters += 1;
        // ~100 ms per iteration; re-warn every ~10 s so a stuck clock is visible in a
        // log tail, not just a single line at boot.
        if iters % 100 == 0 {
            tracing::warn!(
                "still waiting for a media clock from '{resolved}' after {} s; \
                 no audio is being transmitted",
                iters / 10
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(target_os = "linux")]
fn raise_thread_priority() -> Result<()> {
    use thread_priority::{
        RealtimeThreadSchedulePolicy, ThreadPriority, ThreadSchedulePolicy, set_thread_priority_and_policy,
        thread_native_id,
    };
    let policy = ThreadSchedulePolicy::Realtime(RealtimeThreadSchedulePolicy::Fifo);
    set_thread_priority_and_policy(thread_native_id(), ThreadPriority::Crossplatform(50u8.try_into().unwrap()), policy)
        .map_err(|e| anyhow::anyhow!("{e:?}"))
}

#[cfg(not(target_os = "linux"))]
fn raise_thread_priority() -> Result<()> {
    Ok(())
}
