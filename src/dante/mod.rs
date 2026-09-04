//! Dante (inferno_aoip) TX side: one `DeviceServer` exposing two TX channels per
//! speaker, written directly by the audio sources through a per-speaker [`SpeakerSink`].
//!
//! inferno owns the rings and tracks how far we have written (`readable_pos`), so
//! everything we do *not* write is transmitted as silence. There is no writer thread and
//! no queue: a source writes when it has audio, and stops writing when it doesn't.
//!
//! # Timeline
//!
//! We hand inferno a start time of 0, so it reads each cycle at
//! `media_clock_now - tx_latency` on the raw media-clock timeline. A sink therefore
//! writes at `media_clock_now + LEAD`, keeping `LEAD + tx_latency` of audio ahead of the
//! transmitter. The clock is read per sink from the device's own clock receiver, never
//! from inferno's published read position — that one stops advancing whenever no Dante
//! receiver is subscribed, which would stall playback.

use crate::config::DanteConfig;
use anyhow::Result;
use inferno_aoip::device_server::{
    AtomicSample, DeviceServer, MediaClock, OwnedBuffer, RBInput, RealTimeClockReceiver, Sample,
    Settings,
};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

/// One stereo frame of Dante samples (i32, MSB-aligned).
pub type Frame = [Sample; 2];

/// Per-channel ring length in samples. MUST be a power of two; 65536 @ 48 kHz ~= 1.37 s,
/// which is far more than the write-ahead below ever needs.
const RING_LEN: usize = 65536;

/// How far ahead of the media clock a sink keeps its write cursor. This *is* the
/// end-to-end buffer: larger absorbs more jitter at the cost of latency.
const LEAD_MS: u64 = 150;

/// How long inferno waits before silence-filling a gap left by a cursor re-anchor.
const HOLE_FIX_MS: u64 = 10;

/// Signed wrapping difference `a - b` over the timeline (see inferno's `wrapsub`).
#[inline]
fn wrapsub(a: usize, b: usize) -> isize {
    (a as isize).wrapping_sub(b as isize)
}

pub struct DanteOutput {
    server: DeviceServer,
}

impl DanteOutput {
    /// Start the Dante device, returning one sink per speaker in `speaker_names` order.
    ///
    /// Startup does not block on the media clock: discovery comes up immediately and the
    /// sinks simply drop audio (inferno transmits silence) until a clock arrives.
    pub async fn start(
        cfg: &DanteConfig,
        speaker_names: &[String],
    ) -> Result<(Self, Vec<Arc<SpeakerSink>>)> {
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
        let mut server = DeviceServer::start(settings).await;

        let sample_rate = cfg.sample_rate as u64;
        let lead = (sample_rate * LEAD_MS / 1000) as usize;
        let hole_fix_wait = (sample_rate * HOLE_FIX_MS / 1000) as usize;

        // Start time 0 keeps the ring timeline identical to the media clock timeline.
        let (start_tx, start_rx) = tokio::sync::oneshot::channel::<usize>();
        let _ = start_tx.send(0);
        let rb_inputs = server
            .transmit_from_owned_buffer(
                tx_channels,
                RING_LEN,
                hole_fix_wait,
                start_rx,
                // Neither of these is used: we pace off the media clock instead.
                Arc::new(AtomicUsize::new(usize::MAX)),
                Arc::new(AtomicUsize::new(usize::MAX)),
                None,
                None,
            )
            .await;

        // Two channels per speaker, in the order the TX channels were named above.
        let mut rb_inputs = rb_inputs.into_iter();
        let mut sinks = Vec::with_capacity(speaker_names.len());
        for _ in speaker_names {
            let pair = [rb_inputs.next().unwrap(), rb_inputs.next().unwrap()];
            sinks.push(Arc::new(SpeakerSink::new(
                pair,
                sample_rate,
                lead,
                Some(server.get_realtime_clock_receiver()),
            )));
        }

        Ok((Self { server }, sinks))
    }

    pub async fn shutdown(self) {
        self.server.shutdown().await;
    }
}

type ChannelRing = RBInput<Sample, OwnedBuffer<AtomicSample>>;

/// One speaker's pair of TX rings plus the write cursor into them.
pub struct SpeakerSink {
    /// Target write-ahead over the media clock, in samples.
    lead: usize,
    inner: Mutex<Inner>,
}

struct Inner {
    /// The speaker's two rings: [L, R].
    rb: [ChannelRing; 2],
    sample_rate: u64,
    clock: MediaClock,
    /// `None` only in tests, where there is no device and hence no clock.
    clock_rx: Option<RealTimeClockReceiver>,
    /// Next timeline position to write, or `None` until the first write anchors it.
    cursor: Option<usize>,
}

impl SpeakerSink {
    fn new(
        rb: [ChannelRing; 2],
        sample_rate: u64,
        lead: usize,
        clock_rx: Option<RealTimeClockReceiver>,
    ) -> Self {
        Self {
            lead,
            inner: Mutex::new(Inner {
                rb,
                sample_rate,
                clock: MediaClock::new(false),
                clock_rx,
                cursor: None,
            }),
        }
    }

    /// Write as many frames as the buffer window currently allows, returning how many
    /// were taken. `0` means the cursor is already a full `lead` ahead (or there is no
    /// media clock yet) and the caller should park and retry — this is what paces a
    /// decode-ahead source.
    pub fn try_write(&self, frames: &[Frame]) -> usize {
        self.write(frames, self.lead)
    }

    /// Write what fits without ever asking the caller to wait, returning how many frames
    /// were taken; the rest are dropped.
    ///
    /// A real-time source (AirPlay) runs on its own free-running clock, so it is allowed
    /// a further `lead` of slack above the paced ceiling to absorb drift in both
    /// directions. Overshooting that drops audio; falling behind re-anchors, which
    /// rebuilds the cushion with silence.
    pub fn write_realtime(&self, frames: &[Frame]) -> usize {
        self.write(frames, self.lead * 2)
    }

    /// Silence everything written but not yet transmitted, and drop the cursor so the
    /// next write starts a fresh buffer.
    ///
    /// Used when a source takes over the speaker (so the displaced source's audio never
    /// reaches Dante) and on an AirPlay flush.
    pub fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        if let (Some(now), Some(cursor)) = (inner.media_now(), inner.cursor) {
            let pending = wrapsub(cursor, now);
            if pending > 0 {
                let n = (pending as usize).min(RING_LEN / 4);
                for rb in &mut inner.rb {
                    rb.write_from_at(now, std::iter::repeat_n(0 as Sample, n));
                }
            }
        }
        inner.cursor = None;
    }

    fn write(&self, frames: &[Frame], max_ahead: usize) -> usize {
        if frames.is_empty() {
            return 0;
        }
        let mut inner = self.inner.lock().unwrap();
        let Some(now) = inner.media_now() else {
            return 0;
        };

        let cursor = resolve_cursor(inner.cursor, now, self.lead);
        let room = wrapsub(now.wrapping_add(max_ahead), cursor);
        if room <= 0 {
            // Keep the resolved cursor: a re-anchor must not be recomputed next call.
            inner.cursor = Some(cursor);
            return 0;
        }

        let n = frames.len().min(room as usize).min(RING_LEN / 4);
        inner.rb[0].write_from_at(cursor, frames[..n].iter().map(|f| f[0]));
        inner.rb[1].write_from_at(cursor, frames[..n].iter().map(|f| f[1]));
        inner.cursor = Some(cursor.wrapping_add(n));
        n
    }

    /// A sink with no device behind it, for tests: it has rings but no clock, so every
    /// write is dropped.
    #[cfg(test)]
    pub fn detached() -> Self {
        use inferno_aoip::device_server::new_owned_ring_buffer;
        let l = new_owned_ring_buffer(RING_LEN, 0, 480).0;
        let r = new_owned_ring_buffer(RING_LEN, 0, 480).0;
        Self::new([l, r], 48000, 7200, None)
    }
}

impl Inner {
    /// The media clock's current position on the TX timeline, or `None` while no clock
    /// (PTP / usrvclock) is available.
    fn media_now(&mut self) -> Option<usize> {
        let clock_rx = self.clock_rx.as_mut()?;
        clock_rx.update();
        if let Some(overlay) = clock_rx.get() {
            self.clock.update_overlay(*overlay);
        }
        // `Clock` is a `usize` on the same timeline inferno's transmitter reads from.
        self.clock.wrapping_now_in_timebase(self.sample_rate)
    }
}

/// Decide where to write next: keep the cursor where it is while it sits in the window
/// the media clock has moved it into, otherwise (re)anchor a full `lead` ahead.
///
/// Anchoring happens on the first write, after a [`SpeakerSink::reset`], when the source
/// has fallen behind the clock (underrun, or a long pause), and when the cursor is
/// absurdly far ahead (a clock jump). The resulting gap is silence-filled by inferno's
/// own hole handling, so the audio after it lands at the right time rather than late.
#[inline]
fn resolve_cursor(cursor: Option<usize>, now: usize, lead: usize) -> usize {
    match cursor {
        Some(c) if wrapsub(c, now) > 0 && wrapsub(c, now.wrapping_add(lead * 4)) <= 0 => c,
        _ => now.wrapping_add(lead),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_is_kept_while_inside_the_window() {
        // Comfortably ahead of the clock but not absurdly so: keep it.
        assert_eq!(resolve_cursor(Some(1100), 1000, 100), 1100);
        assert_eq!(resolve_cursor(Some(1001), 1000, 100), 1001);
    }

    #[test]
    fn cursor_is_anchored_when_unset_or_out_of_the_window() {
        // First write.
        assert_eq!(resolve_cursor(None, 1000, 100), 1100);
        // Fallen behind the clock (underrun / long pause).
        assert_eq!(resolve_cursor(Some(900), 1000, 100), 1100);
        assert_eq!(resolve_cursor(Some(1000), 1000, 100), 1100);
        // Absurdly far ahead (clock jumped backwards).
        assert_eq!(resolve_cursor(Some(1000 + 401), 1000, 100), 1100);
    }

    #[test]
    fn cursor_survives_timeline_wraparound() {
        let now = usize::MAX - 10;
        // The cursor has wrapped past zero while the clock has not yet.
        assert_eq!(resolve_cursor(Some(89), now, 100), 89);
        assert_eq!(resolve_cursor(None, now, 100), 89);
    }

    #[test]
    fn detached_sink_drops_everything() {
        let sink = SpeakerSink::detached();
        assert_eq!(sink.try_write(&[[1, 1], [2, 2]]), 0);
        assert_eq!(sink.write_realtime(&[[1, 1]]), 0);
        // Nothing to silence, and no cursor to keep.
        sink.reset();
        assert!(sink.inner.lock().unwrap().cursor.is_none());
    }
}
