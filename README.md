# lucyfer

A Rust microservice that exposes one or more **speakers** on your LAN — over **Spotify
Connect** and **AirPlay** simultaneously — and transmits their audio over **Dante**
(Audio over IP). Each speaker shows up in the Spotify app *and* in the AirPlay picker;
its decoded audio is resampled and published as a stereo pair of TX channels on a
single Dante device that other Dante gear can subscribe to. A REST + WebSocket API
reports now-playing metadata / album art / volume and drives transport controls (play,
pause, next, previous, seek, volume).

Only one source drives a speaker at a time: whichever starts playing most recently
takes it over and the other is paused (see [Source arbitration](#source-arbitration)).

Libraries it builds on:

- [`librespot`](https://github.com/librespot-org/librespot) — Spotify Connect (MIT).
- [`shairplay`](https://crates.io/crates/shairplay) — a pure-Rust AirPlay receiver
  (LGPL-3.0-or-later).
- [`inferno`](https://gitlab.com/lumifaza/inferno) (`inferno_aoip`, a git submodule) —
  an unofficial Dante implementation (GPL-3.0-or-later OR AGPL-3.0-or-later).

Because it links `inferno_aoip`, **lucyfer is licensed GPL-3.0-or-later.**

## How it works

```
                per speaker                                    one Dante device
  Spotify app ─► librespot ──┐                       ┌─► ring ─► inferno TX ─► Dante
   (Connect)    (f64 44.1k)  ├─► resample ─► pacing ─┤   writer  (2 ch/speaker)
                             │   to Dante    queue   │  (clock-driven)
  iPhone / Mac ─► shairplay ─┘   rate, i32           │
    (AirPlay)     (f32 44.1k)       ▲                │
                                    │                │
                             SpeakerAudio arbiter ───┘
                             (one owner at a time)
                                    │
                       events ──────┴──► StateHub ─► REST + WebSocket API
```

- librespot delivers interleaved **f64 stereo @ 44.1 kHz**; shairplay delivers **f32**
  at the stream's native rate (44.1 kHz for AirPlay 1). Both go through the same
  resampler to the configured Dante rate (default 48 kHz, via `rubato`; bypassed at
  44100) and are converted to inferno's MSB-aligned `i32` samples.
- A bounded **pacing queue** decouples a source from the Dante clock. For Spotify,
  blocking on a full queue is what paces playback (as a hardware sink would). AirPlay
  is never blocked — its callbacks run on tokio tasks and the sender already streams in
  real time — so it drops on overflow and tops the queue back up to about half full on
  underrun, absorbing sender-clock drift in both directions.
- inferno's TX rings are **timeline-indexed** — the transmitter reads whatever sits at
  `media_clock_timestamp & mask`. A single **ring-writer thread**, paced by its own
  media clock, keeps each speaker's rings filled ahead of the transmitter and writes
  **silence on underrun/pause** (so the transmitter never loops stale audio). The
  writer never paces off the transmitter's read cursor, which is idle (`usize::MAX`)
  whenever no Dante receiver is subscribed.

## Source arbitration

Every enabled source advertises **the same speakers** and feeds **the same** Dante
channel pair, so they have to take turns. `SpeakerAudio` (`src/source/mod.rs`) owns
that decision, on a last-writer-wins basis:

- A source **claims** the speaker when it starts playing (librespot's `Playing` event /
  the first AirPlay audio packet).
- The displaced source is **gracefully stopped**: Spotify gets `spirc.pause()`, AirPlay
  gets a DACP `Pause` sent back to the iPhone. Neither network session is torn down, so
  handing playback back is instant.
- Anything the displaced source had already queued is discarded, so its buffered audio
  never reaches Dante, and its state updates stop applying until it owns the speaker
  again.

`SpeakerState.source` reports the current owner (`"spotify"`, `"airplay"` or `null`);
`SpeakerState.sources` lists the ones this build is advertising on. API commands are
routed to the owning source, so `POST /speakers/{id}/pause` pauses whatever is actually
playing.

## Configuration

Copy `config.example.yaml` and edit it. Key fields:

| Section | Field | Meaning |
| --- | --- | --- |
| `dante` | `interface` | Dante NIC: IPv4 address **or** interface name (`BIND_IP`). |
| `dante` | `sample_rate` | Dante network rate. Must match your network (44100 skips resampling). |
| `dante` | `clock_path` | `null` = inferno's default usrvclock socket; else a socket path or `/dev/ptp0`. |
| `dante` | `ring_len` | Per-channel ring length in samples; **power of two**. |
| `spotify` | `enabled` | Advertise every speaker over Spotify Connect (default `true`). |
| `spotify` | `interface_ip` | mDNS-advertised IP for the Connect side (`null` = all). |
| `spotify` | `bitrate` | 96 / 160 / 320. |
| `spotify` | `cache_dir` | Per-speaker credential + audio cache root (`null` disables). |
| `airplay` | `enabled` | Advertise every speaker over AirPlay (default `true`). |
| `airplay` | `interface_ip` | Address the RTSP listener binds to (`null` = all). Does **not** scope mDNS. |
| `airplay` | `base_port` | RTSP port of the first speaker; speaker N uses `base_port + N` (default 5000). |
| `speakers[]` | `name` | Name shown in Spotify and AirPlay; Dante channels become `"<name> L"` / `"<name> R"`. |
| `speakers[]` | `apply_volume` | `true` scales the Dante stream by the source's volume; `false` sends full-scale and only reports volume. |
| `speakers[]` | `initial_volume` | 0.0–1.0 applied on Spotify session connect. |
| `api` | `bind` | REST/WS listen address. |

At least one source must be enabled; startup fails otherwise.

## API

REST, under `/api/v1`:

| Method / path | Action |
| --- | --- |
| `GET /speakers` | List all speakers with state (positions extrapolated). |
| `GET /speakers/{id}` | One speaker's state (404 if unknown). |
| `POST /speakers/{id}/play` | Activate + play. |
| `POST /speakers/{id}/pause` | Pause. |
| `POST /speakers/{id}/playpause` | Toggle. |
| `POST /speakers/{id}/next` \| `/previous` | Skip. |
| `POST /speakers/{id}/seek` | Body `{"position_ms": 61000}`. |
| `POST /speakers/{id}/volume` | Body `{"level": 0.55}` (0.0–1.0). |
| `GET /speakers/{id}/artwork` | Cover art: the image bytes (AirPlay) or a 307 redirect to the CDN URL (Spotify). |
| `GET /healthz` | Liveness. |

`{id}` is the slug of the speaker name (e.g. `"Living Room"` → `living-room`).
Commands go to whichever source currently owns the speaker. Responses: `204` ok,
`409 {"error":"speaker_inactive"}` when no session has connected yet,
`501 {"error":"unsupported_for_source"}` when the owning source cannot do it (AirPlay 1
has no seek), `404` unknown speaker, `400` bad body.

WebSocket, `GET /api/v1/ws`:

```jsonc
// server -> client
{"type":"snapshot","speakers":[ /* SpeakerState */ ]}   // on connect
{"type":"speaker_update","speaker": { /* SpeakerState */ }}  // on every change
{"type":"ack","speaker_id":"living-room","action":"play"}
{"type":"error","message":"..."}
// client -> server
{"type":"command","speaker_id":"living-room","action":"play|pause|playpause|next|previous"}
{"type":"command","speaker_id":"living-room","action":"seek","position_ms":61000}
{"type":"command","speaker_id":"living-room","action":"volume","level":0.55}
```

`SpeakerState` carries: `id`, `name`, `apply_volume`, `sources`
(`["spotify","airplay"]` — which sources this speaker is advertised on), `source`
(which one currently drives the Dante channels, or `null`), `playback`
(`inactive|stopped|playing|paused|loading`), `active_user`, `volume` (0–1), `track`
(`uri`, `name`, `artists`, `album`, `duration_ms`, `art_url`), `position_ms` +
`position_captured_at_ms` (extrapolate while playing), `shuffle`, `repeat`.

## Running

### Prerequisites

The inferno path dependency uses git submodules. After cloning:

```sh
cd inferno && git submodule update --init --recursive && cd ..
```

### Local (needs a media clock)

`DeviceServer::start` **blocks until a media clock is available.** For a bench test
without real PTP, build the fake usrvclock server from the inferno test suite and
point `dante.clock_path` at its socket:

```sh
# terminal 1: fake clock
gcc inferno/test/dockerized_trx/fake_usrvclock_server/fake_usrvclock_server.c -o /tmp/fakeclock
USRVCLOCK_SOCKET=/tmp/usrvclock /tmp/fakeclock &

# terminal 2: lucyfer (clock_path: "/tmp/usrvclock" in the config)
cargo run --release -- --config config.example.yaml
```

### Docker

```sh
git -C inferno submodule update --init --recursive
# edit config.yaml: set dante.interface (and optionally the per-source interface_ip)
docker compose up --build
```

The dev `docker-compose.yml` includes a **fake clock** sidecar, host networking (for
mDNS + Dante multicast), a state volume, `memlock` ulimit and `SYS_NICE`.

## Production clocking

The fake clock free-runs on `CLOCK_MONOTONIC` and **drifts against a real Dante
network**. For production, provide a real PTP-derived clock:

- Run [Statime](https://github.com/pendulum-project/statime) (or `ptp4l`) synced to
  the Dante grandmaster and bridge it to a usrvclock socket that lucyfer reads
  (`clock_path`), **or**
- Set `dante.clock_path: /dev/ptp0` and give the container the NIC's PHC
  (`devices: [/dev/ptp0]`) when it is already PTP-synced.

See inferno's README "Clocking options" for details.

## Caveats

- **AirPlay 1 only.** shairplay's AirPlay 2 handlers deliver no metadata, artwork,
  volume or remote-control callbacks — those exist solely on the AP1 path — so lucyfer
  builds the crate with default features and advertises as a classic RAOP receiver.
  That is what makes the now-playing state and transport controls work.
- **AirPlay has no clock recovery.** The sender's 44.1 kHz clock free-runs against the
  Dante media clock. lucyfer keeps its queue about half full and corrects by inserting
  silence on underrun / dropping frames on overflow, so expect an occasional glitch on
  long streams. Raise `audio.pacing_buffer_ms` if you see repeated overflow warnings.
- **`seek` is unsupported while a speaker is AirPlay-owned** (DACP has no seek verb) —
  the API returns `501`.
- **Discovery binds all interfaces.** librespot's zeroconf credential HTTP server
  always listens on `0.0.0.0`/`[::]`; only the mDNS *advertisement* is
  interface-scoped (`spotify.interface_ip`, libmdns backend). On the AirPlay side it is
  the other way round: `airplay.interface_ip` pins the RTSP *listener*, but shairplay
  registers mDNS with address auto-detection across every interface. Restrict with a
  firewall if you need true per-interface isolation.
- **PTP clock is a hard startup dependency** — the service waits for a clock before
  serving.
- **Sample rate is fixed at start.** Changing `dante.sample_rate` requires a restart,
  and it must match the rest of your Dante network (inferno rejects mismatched
  subscribers).
- **Spotify Premium** is required for Spotify Connect. Credentials arrive via the
  zeroconf handoff; no client ID/secret configuration is needed (librespot ships a
  built-in one). AirPlay needs no account at all.

## License

GPL-3.0-or-later. See `LICENSE`.
