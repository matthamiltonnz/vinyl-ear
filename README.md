# Vinyl Ear

Home Assistant add-on that works out **what record is playing** on the turntable and
publishes it to Home Assistant over MQTT. Vinyl is analogue, so nothing reports track
metadata — this listens to a room microphone, fingerprints the audio with Shazam, and
fills in the gap.

```
room mic ──▶ noise gate ──▶ 15 s clip ──▶ Shazam (shazamio) ──▶ MQTT discovery
                                                                 ├─ sensor.turntable_now_playing
                                                                 └─ binary_sensor.turntable_audio
```

## Why not an existing project?

Two exist, both tiny and neither works unmodified on Home Assistant OS:

- **[shazam2mqtt](https://github.com/TigreGotico/shazam2mqtt)** — closest match
  (noise gate, capture, Shazam, MQTT discovery with ten entities). Its Docker image
  drives the mic through PortAudio's **ALSA** backend, and inside a container on HAOS
  ALSA cannot see the USB capture devices: they are held by the host PulseAudio, and
  `/proc/asound` is not visible. Enumerated devices were `[]` (or HDMI outputs only).
- **[ha_shazamio](https://github.com/iamjoshk/ha_shazamio)** — a HACS integration that
  exposes ShazamIO as services/events. It does not capture audio at all; you have to
  feed it a file. It describes itself as "VERY ALPHA".

Vinyl Ear keeps the behaviour everyone wants from shazam2mqtt (noise-gated capture,
cooldowns, MQTT discovery) but records through the **PulseAudio socket** that Home
Assistant hands to add-ons with `audio: true` — the same path that `parec` and
`ffmpeg -f pulse` use, which is what actually works on this box.

## What it publishes

| Entity | State | Attributes |
| --- | --- | --- |
| `sensor.vinyl_ear_<device_name>_now_playing` | `Artist - Title`, or `Nothing playing` after `silence_clear_seconds` seconds of silence | `artist`, `title`, `album`, `year`, `genre`, `artwork`, `url`, `source`, `identified_at` |
| `binary_sensor.vinyl_ear_<device_name>_audio_detected` | `ON` while the room is above the noise gate | — |

`<device_name>` is the add-on option of the same name (`turntable` by default), and it
also names the MQTT topics under `vinyl_ear/<device_name>/`. Entity ids follow the HA
device name, so they carry the `vinyl_ear_` prefix; rename them in the UI if you want
`sensor.turntable_now_playing`. Everything appears under one HA device named
`Vinyl Ear (<device_name>)`, created by MQTT discovery — no YAML required.

Album artwork is downloaded to `/config/www/vinyl-art/<device_name>.jpg` and the
`artwork` attribute points at `/local/vinyl-art/<device_name>.jpg?v=<epoch>` so every
client (including Cast displays and other networks) loads the cover from Home Assistant
instead of Apple's CDN, whose URLs expire. The untouched CDN URL is kept in
`artwork_remote`. This needs the add-on's read-write access to `/config` (`map: config:rw`),
which is already declared; if the folder is not writable the remote URL is published as
before.

## Options

The microphone is opened once and read continuously in 250 ms chunks, so the level is
sampled four times a second rather than in bursts:

- **Silence → sound triggers an immediate lookup.** After at least 2 s of quiet, the
  next audio above the noise gate starts a fresh clip (with 1.5 s of pre-roll so the
  first beat isn't cut), and the track is identified as soon as `listen_seconds` has
  been gathered — no waiting for a poll interval. That covers needle drop, the gap
  between tracks and a side change.
- **While music continues**, it re-identifies every `poll_seconds` so a track change
  without a silent gap is still caught, and a repeat match of the same track is
  suppressed.
- **Silence clears the state** after `silence_clear_seconds` — see
  [What it publishes](#what-it-publishes).
- **`active_entity`**, when set, pauses all of the above unless that entity is `on`.
- **The activity threshold adapts to the room.** `noise_gate_db` is only a floor: the
  listener measures the quiet tenth percentile of the last 30 seconds and raises the
  threshold to 6 dB above it, so a fridge, fan or amplifier hiss that sits above the
  fixed gate stops looking like a record that never ends. It only recalibrates while
  the room is quiet, or when the audio is steady and Shazam has already failed to name
  it, so music isn't mistaken for noise. The log says `Room noise floor ~-35 dBFS,
  activity threshold now -29 dBFS` when it moves.
- **Repeated misses back off.** After failed lookups the interval doubles, capping at
  five minutes, so an unrecognisable record doesn't burn a lookup every 30 seconds. A
  fresh silence → sound transition resets it.
- **Every five minutes** the log reports the level, the current threshold, whether it
  thinks audio is playing and how many attempts have failed — the numbers you need to
  tune `noise_gate_db` if the room is unusually noisy.

| Option | Default | Meaning |
| --- | --- | --- |
| `source` | *(auto)* | PulseAudio source name. Empty picks the first non-monitor input. |
| `active_entity` | *(none)* | Listen only while this entity is `on`, e.g. `input_boolean.activity_turntable`. Empty means always listen. Read through the supervisor's Home Assistant API; if it cannot be read the add-on keeps listening. |
| `sample_rate` | `44100` | Capture rate; Shazam downsamples internally. |
| `listen_seconds` | `15` | How much audio above the gate makes a clip. Short clips miss more often. |
| `silence_clear_seconds` | `45` | How long the room must be judged quiet before the now-playing state is cleared (lifted needle, side change, record put away). |
| `noise_gate_db` | `-45` | dBFS considered "not silence". Room noise sits near -60; music is well above. |
| `poll_seconds` | `30` | Re-check interval while the music keeps playing. A new sound after a silent gap is recognised immediately, without waiting for this. |
| `device_name` | `turntable` | Node name for topics and entity IDs. |
| `mqtt_host` | `core-mosquitto` | Broker. Use `<ha-host>` if you prefer the LAN address. |
| `mqtt_port` | `1883` | Broker port. |
| `mqtt_user` / `mqtt_password` | *(empty)* | Leave empty for anonymous brokers. |
| `discovery_prefix` | `homeassistant` | HA's MQTT discovery prefix. |
| `log_level` | `info` | `debug` logs every capture attempt and the audio level. |

## Before you trust the audio

The webcam microphone must actually be listening. On this installation the USB webcam mic source was sitting at **12% (-55 dB)** in PulseAudio, i.e. effectively muted,
and no amount of fingerprinting will fix that:

```bash
pactl set-source-volume <usb-mic-source> 45%
```

45% measured -24.7 dB mean / -8.1 dB peak on a record and matched within seconds; 70%
clipped. Nothing else uses that mic (the go2rtc "Turntable" stream is video-only), so
the gain can stay where you put it.

## Install

### From this repository (recommended)

In Home Assistant: **Settings → Add-ons → Add-on store → ⋮ → Repositories**, add

```
https://github.com/matthamiltonnz/vinyl-ear
```

then close the dialog, refresh the store and install **Vinyl Ear**. Before pressing
Start, open the **Configuration** tab and set `mqtt_user` / `mqtt_password`.

### As a local add-on

Copy the `vinyl_ear` folder to the Home Assistant host and reload the store:

```bash
scp -r vinyl_ear <ssh-user>@<home-assistant-host>:/addons/vinyl_ear
```

Then **Settings → Add-ons → Add-on store → ⋮ → Reload**, pick *Vinyl Ear*, install,
paste your MQTT username/password in the configuration tab, and start it.

## Lovelace card

Ready-made views live in [`lovelace/`](lovelace/):

| File | Layout |
| --- | --- |
| `turntable-overlay-card.yaml` | **Recommended** — single `custom:webrtc-camera` card, metadata pill painted over the video with `card_mod` (full-screen panel view) |
| `turntable-stack-view.yaml` | Safe fallback — metadata card above the stream, no `card_mod` |
| `turntable-picture-elements-view.yaml` | Overlay via core `picture-elements` (needs a transparent backdrop image) |
| `turntable-sections-view.yaml` | Sections view with the metadata in the view header |
| `turntable-remote-stack.yaml` | One conditional card for a remote-control stack: track details full width on top, a square (centre-cropped) view of the turntable camera underneath |

If you edit the overlay card's CSS, note the line break escape is `\00000A`, not `\A`. A
short hex escape swallows the next character when it is a hex digit — `\AB` becomes
U+0ABE and eats the `B`, which displays as `Laufey*lst Friend` instead of
`Laufey` / `Best Friend`, and `\Ad` becomes a soft hyphen, so `sombr` and `dime` run
together as `sombrime`.

The full YAML is in
[`lovelace/turntable-overlay-card.yaml`](lovelace/turntable-overlay-card.yaml):
a `panel` view holding a single `custom:webrtc-camera` card, with a `card_mod`
style that paints the cover, artist, title and album over the stream.

## Notes and limits

- Shazam identification is via the reverse-engineered API (`shazamio`), the same way
  shazam2mqtt and ha_shazamio do it. It is free and needs no key, but it is unofficial:
  if it ever breaks, swap the client for ACRCloud or AudD behind the same interface.
- Expect ~15-25 s from needle drop to a published track: 15 s of audio plus the lookup.
- Quiet passages, spoken intros and private pressings may not match. The last known
  track stays published until something new is recognised, so the dashboard never
  blanks mid-side.
- Silence clears it: once the room is judged quiet for `silence_clear_seconds`
  seconds — needle lifted, side changed, record put away — the state is replaced with
  `Nothing playing` and the artwork and attributes are emptied, so the dashboard stops
  claiming a track that finished minutes ago.
- Recognition requests only happen while the room is above the noise gate, at most
  once every `poll_seconds`.
