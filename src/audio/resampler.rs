//! Sample-rate conversion from librespot's fixed 44.1 kHz stereo f64 output to the
//! configured Dante rate, producing interleaved `Frame`s of MSB-aligned i32.
//!
//! `Bypass` is used when the Dante rate is already 44100 (bit-exact passthrough).
//! Otherwise an FFT resampler with a fixed input chunk is fed from an accumulation
//! buffer, since librespot delivers variable-sized packets.

use super::queue::Frame;
use inferno_aoip::device_server::Sample;
use rubato::{FftFixedIn, Resampler};

const SPOTIFY_RATE: usize = 44_100;
const CHUNK_IN: usize = 1024;
const SUB_CHUNKS: usize = 2;
const CHANNELS: usize = 2;

pub enum SpeakerResampler {
    Bypass,
    Fft(FftState),
}

pub struct FftState {
    resampler: FftFixedIn<f32>,
    // Accumulated deinterleaved input, one Vec per channel.
    in_buf: [Vec<f32>; CHANNELS],
    // Reused output scratch, one Vec per channel.
    out_buf: [Vec<f32>; CHANNELS],
}

impl SpeakerResampler {
    /// Build a resampler for the given output rate. `out_rate == 44100` -> `Bypass`.
    pub fn new(out_rate: u32) -> anyhow::Result<Self> {
        if out_rate as usize == SPOTIFY_RATE {
            return Ok(SpeakerResampler::Bypass);
        }
        let resampler = FftFixedIn::<f32>::new(
            SPOTIFY_RATE,
            out_rate as usize,
            CHUNK_IN,
            SUB_CHUNKS,
            CHANNELS,
        )?;
        let out_max = resampler.output_frames_max();
        Ok(SpeakerResampler::Fft(FftState {
            resampler,
            in_buf: [Vec::with_capacity(CHUNK_IN * 2), Vec::with_capacity(CHUNK_IN * 2)],
            out_buf: [vec![0.0; out_max], vec![0.0; out_max]],
        }))
    }

    /// Reset internal buffers (called on sink start).
    pub fn reset(&mut self) {
        if let SpeakerResampler::Fft(s) = self {
            s.in_buf[0].clear();
            s.in_buf[1].clear();
        }
    }

    /// Consume interleaved stereo f64 (44.1 kHz) and append output frames to `out`.
    pub fn process(&mut self, interleaved: &[f64], out: &mut Vec<Frame>) {
        match self {
            SpeakerResampler::Bypass => {
                for pair in interleaved.chunks_exact(2) {
                    out.push([f64_to_sample(pair[0]), f64_to_sample(pair[1])]);
                }
            }
            SpeakerResampler::Fft(s) => s.process(interleaved, out),
        }
    }
}

impl FftState {
    fn process(&mut self, interleaved: &[f64], out: &mut Vec<Frame>) {
        for pair in interleaved.chunks_exact(2) {
            self.in_buf[0].push(pair[0] as f32);
            self.in_buf[1].push(pair[1] as f32);
        }

        while self.in_buf[0].len() >= CHUNK_IN {
            let wave_in = [&self.in_buf[0][..CHUNK_IN], &self.in_buf[1][..CHUNK_IN]];
            let (out0, out1) = self.out_buf.split_at_mut(1);
            let mut wave_out = [out0[0].as_mut_slice(), out1[0].as_mut_slice()];
            let (used, produced) = self
                .resampler
                .process_into_buffer(&wave_in, &mut wave_out, None)
                .expect("resampler process");
            debug_assert_eq!(used, CHUNK_IN);

            for i in 0..produced {
                out.push([
                    f32_to_sample(self.out_buf[0][i]),
                    f32_to_sample(self.out_buf[1][i]),
                ]);
            }

            self.in_buf[0].drain(..CHUNK_IN);
            self.in_buf[1].drain(..CHUNK_IN);
        }
    }
}

#[inline]
fn f64_to_sample(x: f64) -> Sample {
    (x.clamp(-1.0, 1.0) * (Sample::MAX as f64)) as Sample
}

#[inline]
fn f32_to_sample(x: f32) -> Sample {
    (x.clamp(-1.0, 1.0) * (Sample::MAX as f32)) as Sample
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sine(rate: usize, freq: f64, n: usize) -> Vec<f64> {
        let mut v = Vec::with_capacity(n * 2);
        for i in 0..n {
            let s = (2.0 * std::f64::consts::PI * freq * i as f64 / rate as f64).sin() * 0.5;
            v.push(s);
            v.push(s);
        }
        v
    }

    #[test]
    fn bypass_is_bit_exact() {
        let mut r = SpeakerResampler::new(44100).unwrap();
        assert!(matches!(r, SpeakerResampler::Bypass));
        let inp = make_sine(44100, 1000.0, 512);
        let mut out = Vec::new();
        r.process(&inp, &mut out);
        assert_eq!(out.len(), 512);
        // Left and right identical, matching the source scaling.
        assert_eq!(out[100][0], f64_to_sample(inp[200]));
        assert_eq!(out[100][0], out[100][1]);
    }

    #[test]
    fn upsample_ratio_and_peak() {
        let mut r = SpeakerResampler::new(48000).unwrap();
        let n_in = 44100; // 1 second
        let inp = make_sine(44100, 1000.0, n_in);
        let mut out = Vec::new();
        r.process(&inp, &mut out);
        // Roughly n_in * 48000 / 44100, minus at most one input chunk of latency.
        let expected = n_in * 48000 / 44100;
        let diff = expected as isize - out.len() as isize;
        assert!(
            diff.unsigned_abs() <= CHUNK_IN * 2,
            "out {} expected ~{}",
            out.len(),
            expected
        );
        // Peak of a 0.5-amplitude sine should be near 0.5 * i32::MAX.
        let peak = out.iter().map(|f| f[0].unsigned_abs() as u64).max().unwrap();
        let target = (0.5 * Sample::MAX as f64) as u64;
        assert!(
            peak > target * 8 / 10 && peak < target * 12 / 10,
            "peak {} target {}",
            peak,
            target
        );
    }
}
