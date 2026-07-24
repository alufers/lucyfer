//! Bounded SPSC frame queue between an audio source (producer) and the single Dante
//! ring-writer thread (consumer).
//!
//! Two very different producers share this queue, one at a time (see
//! `crate::source::SpeakerAudio`, which arbitrates between them):
//!
//! - **Spotify** is *paced* by the queue: `SpeakerAudio::push_blocking` parks while the
//!   queue is full instead of letting the decoder run ahead unbounded. This mirrors how
//!   a hardware audio backend applies back-pressure.
//! - **AirPlay** must never be paced — its callbacks run on tokio tasks and the sender
//!   already delivers in real time — so `SpeakerAudio::push_realtime` drops frames
//!   rather than blocking.
//!
//! Both go through the non-blocking [`QueueProducer::push_some`]; the pacing policy
//! lives in `SpeakerAudio`, not here.

use inferno_aoip::device_server::Sample;
use ringbuf::traits::{Consumer, Observer, Producer, Split};
use ringbuf::{HeapCons, HeapProd, HeapRb};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// One stereo frame of Dante samples (i32, MSB-aligned).
pub type Frame = [Sample; 2];

pub struct QueueProducer {
    prod: HeapProd<Frame>,
    discard: Arc<AtomicBool>,
}

pub struct QueueConsumer {
    cons: HeapCons<Frame>,
    discard: Arc<AtomicBool>,
}

/// Cheap handle for requesting a discard without holding the producer's lock.
///
/// The producer lives behind a `Mutex` (two sources take turns pushing to it), and a
/// source switch must never wait on whichever source currently holds it — hence a
/// separate handle rather than a `&mut QueueProducer` method.
#[derive(Clone)]
pub struct DiscardRequest(Arc<AtomicBool>);

impl DiscardRequest {
    pub fn request(&self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Create a bounded frame queue with room for `capacity` frames.
pub fn channel(capacity: usize) -> (QueueProducer, QueueConsumer) {
    let rb = HeapRb::<Frame>::new(capacity.max(2));
    let (prod, cons) = rb.split();
    let discard = Arc::new(AtomicBool::new(false));
    (
        QueueProducer {
            prod,
            discard: discard.clone(),
        },
        QueueConsumer { cons, discard },
    )
}

impl QueueProducer {
    /// Push as many frames as currently fit, without blocking. Returns how many were
    /// accepted (0 when the queue is full).
    pub fn push_some(&mut self, frames: &[Frame]) -> usize {
        self.prod.push_slice(frames)
    }

    /// Whether the consumer (ring writer) is still alive. Once it is gone nothing will
    /// ever drain the queue again, so callers must stop rather than block forever.
    pub fn is_consumer_alive(&self) -> bool {
        self.prod.read_is_held()
    }

    /// Frames currently queued and not yet transmitted.
    pub fn occupied_len(&self) -> usize {
        self.prod.occupied_len()
    }

    /// A handle for asking the ring writer to throw away everything still queued on its
    /// next cycle.
    ///
    /// Used when a source takes over a speaker (so the displaced source's buffered
    /// audio never reaches Dante) and on an AirPlay stream flush. `HeapProd` has no way
    /// to drop already-written frames, so the consumer performs the drain.
    pub fn discard_request(&self) -> DiscardRequest {
        DiscardRequest(self.discard.clone())
    }

    /// Enqueue `frames` frames of silence, up to whatever currently fits.
    ///
    /// Used to top a real-time (AirPlay) source's queue back up to its target depth:
    /// sitting half full gives the sender's free-running clock drift headroom in both
    /// directions instead of underrunning continuously.
    pub fn push_silence(&mut self, frames: usize) {
        const CHUNK: usize = 512;
        let silence = [[0 as Sample; 2]; CHUNK];
        let mut left = frames;
        while left > 0 {
            let n = left.min(CHUNK);
            let pushed = self.push_some(&silence[..n]);
            if pushed == 0 {
                break;
            }
            left -= pushed;
        }
    }
}

impl QueueConsumer {
    /// Pop a single frame if available.
    #[inline]
    pub fn pop(&mut self) -> Option<Frame> {
        self.cons.try_pop()
    }

    /// Consume a pending discard request, dropping everything queued. Returns `true`
    /// when a discard actually happened. Checked once per ring-writer cycle.
    pub fn take_discard_request(&mut self) -> bool {
        if !self.discard.swap(false, Ordering::AcqRel) {
            return false;
        }
        while self.cons.try_pop().is_some() {}
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_some_reports_partial_writes() {
        let (mut prod, mut cons) = channel(4);
        assert_eq!(prod.push_some(&[[1, 1], [2, 2]]), 2);
        // Only two slots left, so the third frame does not fit.
        assert_eq!(prod.push_some(&[[3, 3], [4, 4], [5, 5]]), 2);
        assert_eq!(prod.push_some(&[[6, 6]]), 0);
        assert_eq!(cons.pop(), Some([1, 1]));
    }

    #[test]
    fn discard_request_drains_the_queue() {
        let (mut prod, mut cons) = channel(8);
        prod.push_some(&[[1, 1], [2, 2], [3, 3]]);
        assert!(!cons.take_discard_request());
        prod.discard_request().request();
        assert!(cons.take_discard_request());
        assert_eq!(cons.pop(), None);
        // The flag is one-shot.
        assert!(!cons.take_discard_request());
    }

    #[test]
    fn silence_prefill_stops_at_capacity() {
        let (mut prod, mut cons) = channel(600);
        prod.push_silence(1_000);
        let mut queued = 0;
        while let Some(frame) = cons.pop() {
            assert_eq!(frame, [0, 0]);
            queued += 1;
        }
        assert_eq!(queued, 600);
    }
}
