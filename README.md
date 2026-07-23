# lucyfer

A Rust microservice that exposes one or more **Spotify Connect speakers** on your
LAN and transmits their audio over **Dante** (Audio over IP). Each speaker shows up
in the Spotify app; its decoded audio is resampled and published as a stereo pair of
TX channels on a single Dante device that other Dante gear can subscribe to. A REST +
WebSocket API reports now-playing metadata / album art / volume and drives transport
controls (play, pause, next, previous, seek, volume).

It embeds two libraries as path dependencies:

- [`librespot`](https://github.com/librespot-org/librespot) — Spotify Connect (MIT).
- [`inferno`](https://gitlab.com/lumifaza/inferno) (`inferno_aoip`) — an unofficial
  Dante implementation (GPL-3.0-or-later OR AGPL-3.0-or-later).

Because it links `inferno_aoip`, **lucyfer is licensed GPL-3.0-or-later.**

## How it works

```
                per speaker                         one Dante device
  Spotify app ─► librespot ─► DanteSink ─► pacing ─► ring ─► inferno TX ─► Dante network
   (Connect)    (decode f64   (resample   queue     writer   (2 ch/speaker)
                 44.1 kHz)     to Dante    (paces    (clock-
                               rate, i32)  librespot) driven)
                    │
                    └─► PlayerEvent ─► StateHub ─► REST + WebSocket API
```

- librespot delivers interleaved **f64 stereo @ 44.1 kHz**. The `DanteSink` resamples
  it to the configured Dante rate (default 48 kHz, via `rubato`; bypassed at 44100)
  and converts to inferno's MSB-aligned `i32` samples.
- A bounded **pacing queue** decouples librespot's bursty decode from the Dante clock.
  Blocking on a full queue is what paces playback (as a hardware sink would).
- inferno's TX rings are **timeline-indexed** — the transmitter reads whatever sits at
  `media_clock_timestamp & mask`. A single **ring-writer thread**, paced by its own
  media clock, keeps each speaker's rings filled ahead of the transmitter and writes
  **silence on underrun/pause** (so the transmitter never loops stale audio). The
  writer never paces off the transmitter's read cursor, which is idle (`usize::MAX`)
  whenever no Dante receiver is subscribed.

## Configuration

Copy `config.example.yaml` and edit it. Key fields:

| Section | Field | Meaning |
| --- | --- | --- |
| `dante` | `interface` | Dante NIC: IPv4 address **or** interface name (`BIND_IP`). |
| `dante` | `sample_rate` | Dante network rate. Must match your network (44100 skips resampling). |
| `dante` | `clock_path` | `null` = inferno's default usrvclock socket; else a socket path or `/dev/ptp0`. |
| `dante` | `ring_len` | Per-channel ring length in samples; **power of two**. |
| `spotify` | `interface_ip` | mDNS-advertised IP for the Connect side (`null` = all). |
| `spotify` | `bitrate` | 96 / 160 / 320. |
| `spotify` | `cache_dir` | Per-speaker credential + audio cache root (`null` disables). |
| `speakers[]` | `name` | Connect name; Dante channels become `"<name> L"` / `"<name> R"`. |
| `speakers[]` | `apply_volume` | `true` scales the Dante stream by Spotify volume; `false` sends full-scale and only reports volume. |
| `speakers[]` | `initial_volume` | 0.0–1.0 applied on session connect. |
| `api` | `bind` | REST/WS listen address. |

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
| `GET /speakers/{id}/artwork` | 307 redirect to the current cover URL. |
| `GET /healthz` | Liveness. |

`{id}` is the slug of the speaker name (e.g. `"Living Room"` → `living-room`).
Command responses: `204` ok, `409 {"error":"speaker_inactive"}` when no Spotify
session has connected yet, `404` unknown speaker, `400` bad body.

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

`SpeakerState` carries: `id`, `name`, `apply_volume`, `playback`
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
# edit config.yaml: set dante.interface (and optionally spotify.interface_ip)
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

- **Discovery binds all interfaces.** librespot's zeroconf credential HTTP server
  always listens on `0.0.0.0`/`[::]`; only the mDNS *advertisement* is
  interface-scoped (`spotify.interface_ip`, libmdns backend). Restrict with a
  firewall if you need true per-interface isolation.
- **PTP clock is a hard startup dependency** — the service waits for a clock before
  serving.
- **Sample rate is fixed at start.** Changing `dante.sample_rate` requires a restart,
  and it must match the rest of your Dante network (inferno rejects mismatched
  subscribers).
- **Spotify Premium** is required for Spotify Connect. Credentials arrive via the
  zeroconf handoff; no client ID/secret configuration is needed (librespot ships a
  built-in one).

## License

GPL-3.0-or-later. See `LICENSE`.
