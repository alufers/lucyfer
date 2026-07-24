//! The single timeline ring-writer thread.
//!
//! inferno's TX rings are *timeline-indexed*: the transmitter reads whatever sample
//! sits at `media_clock_timestamp & mask`, unconditionally. So there is no queue to
//! drain — we must keep writing the correct sample at each timeline position, ahead
//! of the transmitter, forever.
//!
//! Crucially the writer paces itself from its OWN media clock, never from the
//! transmitter's `current_timestamp` (which is `usize::MAX` whenever no Dante
//! receiver is subscribed). If it paced off the transmitter it would stall Spotify
//! playback whenever nothing is subscribed.
//!
//! On underrun or pause the queue yields nothing, and we write silence — this is
//! what stops the transmitter from re-reading (looping) stale ring content.

use crate::audio::QueueConsumer;
use crate::dante::TimelineRing;
use inferno_aoip::device_server::{MediaClock, RealTimeClockReceiver};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// Signed wrapping difference `a - b` over the timeline (see inferno's `wrapsub`).
#[inline]
fn wrapsub(a: usize, b: usize) -> isize {
    (a as isize).wrapping_sub(b as isize)
}

pub struct RingWriter {
    /// Two rings per speaker: [s0_L, s0_R, s1_L, s1_R, ...].
    pub rings: Vec<TimelineRing>,
    /// One consumer per speaker.
    pub consumers: Vec<QueueConsumer>,
    pub clock_rx: RealTimeClockReceiver,
    pub sample_rate: u64,
    pub lead_samples: usize,
}

impl RingWriter {
    /// Run until `shutdown` is set. `wake` is notified by the TX `TransferNotifier`.
    pub fn run(mut self, wake: Arc<(Mutex<bool>, Condvar)>, shutdown: Arc<AtomicBool>) {
        let n_speakers = self.consumers.len();
        // Per-speaker write cursor (next timeline position to fill). None until first sync.
        let mut cursors: Vec<Option<usize>> = vec![None; n_speakers];
        let mut clock = MediaClock::new(false);

        while !shutdown.load(Ordering::Relaxed) {
            self.clock_rx.update();
            let now = match self.clock_rx.get() {
                Some(overlay) => {
                    clock.update_overlay(*overlay);
                    match clock.wrapping_now_in_timebase(self.sample_rate) {
                        Some(t) => t as usize,
                        None => {
                            std::thread::sleep(Duration::from_millis(5));
                            continue;
                        }
                    }
                }
                None => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
            };

            let target = now.wrapping_add(self.lead_samples);

            for s in 0..n_speakers {
                let cursor = resync_cursor(cursors[s], now, self.lead_samples);
                cursors[s] = Some(fill_speaker(
                    &self.rings[s * 2],
                    &self.rings[s * 2 + 1],
                    &mut self.consumers[s],
                    cursor,
                    target,
                ));
            }

            // Wait for the next transmit cycle (or a short timeout) before refilling.
            let (lock, cvar) = &*wake;
            let guard = lock.lock().unwrap();
            let _ = cvar
                .wait_timeout(guard, Duration::from_millis(5))
                .unwrap();
        }
    }
}

/// Decide the write cursor for this cycle: (re)sync to `now` when unset, lagging
/// behind the clock, or absurdly far ahead (clock jump / wrap discontinuity).
#[inline]
fn resync_cursor(cursor: Option<usize>, now: usize, lead_samples: usize) -> usize {
    match cursor {
        None => now,
        Some(c) => {
            let ahead = wrapsub(c, now);
            if ahead < 0 || ahead > (lead_samples as isize) * 4 {
                now
            } else {
                c
            }
        }
    }
}

/// Fill one speaker's L/R rings from `cursor` up to (but not including) `target`,
/// popping frames from `consumer` and writing silence on underrun. Returns the new
/// cursor.
///
/// A pending discard request (raised when a source takes over the speaker, or on an
/// AirPlay flush) is honoured first: everything queued is thrown away, so the rest of
/// this cycle silence-fills instead of transmitting the displaced source's audio.
#[inline]
fn fill_speaker(
    ring_l: &TimelineRing,
    ring_r: &TimelineRing,
    consumer: &mut QueueConsumer,
    mut cursor: usize,
    target: usize,
) -> usize {
    if consumer.take_discard_request() {
        tracing::debug!("ring writer discarded queued audio (source switch or flush)");
    }
    while wrapsub(target, cursor) > 0 {
        let (l, r) = match consumer.pop() {
            Some(frame) => (frame[0], frame[1]),
            None => (0, 0), // silence-fill on underrun / pause
        };
        ring_l.write(cursor, l);
        ring_r.write(cursor, r);
        cursor = cursor.wrapping_add(1);
    }
    cursor
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::queue;
    use crate::dante::TimelineRing;

    #[test]
    fn resync_when_unset_or_stale() {
        // Unset -> now.
        assert_eq!(resync_cursor(None, 1000, 100), 1000);
        // Lagging behind now -> resync to now.
        assert_eq!(resync_cursor(Some(900), 1000, 100), 1000);
        // Reasonable position ahead -> keep.
        assert_eq!(resync_cursor(Some(1050), 1000, 100), 1050);
        // Absurdly far ahead (> 4*lead) -> resync.
        assert_eq!(resync_cursor(Some(1000 + 401), 1000, 100), 1000);
    }

    #[test]
    fn fills_frames_then_silence() {
        let ring_l = TimelineRing::new(1024);
        let ring_r = TimelineRing::new(1024);
        let (mut prod, mut cons) = queue::channel(16);
        // Two frames available; request four positions.
        assert_eq!(prod.push_some(&[[111, 222], [333, 444]]), 2);

        let end = fill_speaker(&ring_l, &ring_r, &mut cons, 10, 14);
        assert_eq!(end, 14);
        // First two positions carry the pushed frames.
        assert_eq!(ring_l.read(10), 111);
        assert_eq!(ring_r.read(10), 222);
        assert_eq!(ring_l.read(11), 333);
        assert_eq!(ring_r.read(11), 444);
        // Underrun positions are silence.
        assert_eq!(ring_l.read(12), 0);
        assert_eq!(ring_r.read(13), 0);
    }

    #[test]
    fn discard_request_drops_queued_audio() {
        let ring_l = TimelineRing::new(1024);
        let ring_r = TimelineRing::new(1024);
        let (mut prod, mut cons) = queue::channel(16);
        assert_eq!(prod.push_some(&[[111, 222], [333, 444]]), 2);
        prod.discard_request().request();

        // The queued frames are thrown away, so every position silence-fills.
        let end = fill_speaker(&ring_l, &ring_r, &mut cons, 10, 12);
        assert_eq!(end, 12);
        assert_eq!(ring_l.read(10), 0);
        assert_eq!(ring_r.read(11), 0);
    }

    #[test]
    fn no_fill_when_target_not_ahead() {
        let ring_l = TimelineRing::new(1024);
        let ring_r = TimelineRing::new(1024);
        let (_prod, mut cons) = queue::channel(16);
        // target == cursor -> nothing written, cursor unchanged.
        let end = fill_speaker(&ring_l, &ring_r, &mut cons, 500, 500);
        assert_eq!(end, 500);
    }
}
