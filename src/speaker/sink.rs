//! A librespot audio `Sink` that resamples the decoded stream and pushes it into a
//! speaker's pacing queue. Blocking on a full queue is what paces librespot.

use crate::audio::queue::{Frame, QueueProducer};
use crate::audio::resampler::SpeakerResampler;
use librespot_playback::audio_backend::{Sink, SinkError, SinkResult};
use librespot_playback::convert::Converter;
use librespot_playback::decoder::AudioPacket;
use std::sync::{Arc, Mutex};

pub struct DanteSink {
    resampler: SpeakerResampler,
    producer: Arc<Mutex<QueueProducer>>,
    scratch: Vec<Frame>,
}

impl DanteSink {
    pub fn new(out_rate: u32, producer: Arc<Mutex<QueueProducer>>) -> Self {
        let resampler = SpeakerResampler::new(out_rate)
            .unwrap_or(SpeakerResampler::Bypass);
        Self {
            resampler,
            producer,
            scratch: Vec::with_capacity(4096),
        }
    }
}

impl Sink for DanteSink {
    fn start(&mut self) -> SinkResult<()> {
        self.resampler.reset();
        Ok(())
    }

    fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }

    fn write(&mut self, packet: AudioPacket, _converter: &mut Converter) -> SinkResult<()> {
        let samples = match packet.samples() {
            Ok(s) => s,
            // Passthrough / raw packets are not expected (decoded PCM only).
            Err(e) => return Err(SinkError::OnWrite(e.to_string())),
        };

        self.scratch.clear();
        self.resampler.process(samples, &mut self.scratch);

        let mut producer = self.producer.lock().unwrap();
        if !producer.push_blocking(&self.scratch) {
            // Consumer (ring writer) gone: report disconnect so librespot stops.
            return Err(SinkError::NotConnected("dante ring writer stopped".into()));
        }
        Ok(())
    }
}
