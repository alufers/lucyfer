//! Bounded SPSC frame queue between a librespot player thread (producer) and the
//! single Dante ring-writer thread (consumer).
//!
//! The producer's blocking push is what paces librespot: when the queue is full,
//! `write` parks briefly instead of letting the decoder run ahead unbounded. This
//! mirrors how a hardware audio backend applies back-pressure.

use inferno_aoip::device_server::Sample;
use ringbuf::traits::{Consumer, Observer, Producer, Split};
use ringbuf::{HeapCons, HeapProd, HeapRb};
use std::time::Duration;

/// One stereo frame of Dante samples (i32, MSB-aligned).
pub type Frame = [Sample; 2];

pub struct QueueProducer {
    prod: HeapProd<Frame>,
}

pub struct QueueConsumer {
    cons: HeapCons<Frame>,
}

/// Create a bounded frame queue with room for `capacity` frames.
pub fn channel(capacity: usize) -> (QueueProducer, QueueConsumer) {
    let rb = HeapRb::<Frame>::new(capacity.max(2));
    let (prod, cons) = rb.split();
    (QueueProducer { prod }, QueueConsumer { cons })
}

impl QueueProducer {
    /// Push all frames, blocking (parking) while the queue is full. Returns `false`
    /// if the consumer has been dropped (writer shut down), so the caller can stop.
    pub fn push_blocking(&mut self, mut frames: &[Frame]) -> bool {
        while !frames.is_empty() {
            let pushed = self.prod.push_slice(frames);
            if pushed == 0 {
                // Consumer (ring writer) dropped: nothing will ever drain again.
                if !self.prod.read_is_held() {
                    return false;
                }
                // Queue full: wait for the writer to drain some. One frame at 48 kHz
                // is ~20 us; parking ~1 ms is a good balance against busy-spinning.
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            frames = &frames[pushed..];
        }
        true
    }
}

impl QueueConsumer {
    /// Pop a single frame if available.
    #[inline]
    pub fn pop(&mut self) -> Option<Frame> {
        self.cons.try_pop()
    }
}
