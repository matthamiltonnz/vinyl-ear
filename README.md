# Vinyl Ear

Home Assistant add-on that works out **what record is playing** on the turntable and
publishes it to Home Assistant over MQTT. Vinyl is analogue, so nothing reports track
metadata — this listens to a room microphone or a line input, fingerprints the audio with
Shazam, and fills in the gap. The same input can be recorded losslessly, a side at a time,
for ripping.

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
| `sensor.vinyl_ear_<device_name>_now_playing` | `Artist - Title`; `Artist - Album` while a record plays but the track is not recognised (`unknown_track: true`, no title); empty between records, because a name for a record that is not there is worse than no name | `artist`, `title`, `album`, `year`, `genre`, `artwork`, `unknown_track`, `url`, `source`, `identified_at` |
| `binary_sensor.vinyl_ear_<device_name>_audio_detected` | `ON` while the room is above the noise gate | — |
| `switch.vinyl_ear_<device_name>_rip` | `ON` while a rip is recording | `file`, `seconds`, `peak_dbfs`, `clipped`, `channels`, `sample_rate`, `format` |

`<device_name>` is the add-on option of the same name (`turntable` by default), and it
also names the MQTT topics under `vinyl_ear/<device_name>/`. Entity ids follow the HA
device name, so they carry the `vinyl_ear_` prefix; rename them in the UI if you want
`sensor.turntable_now_playing`. Everything appears under one HA device named
`Vinyl Ear (<device_name>)`, created by MQTT discovery — no YAML required. The rip is
driven over `vinyl_ear/<device_name>/rip/set` (`ON`/`OFF`), with its state on
`.../rip/state` and its progress on `.../rip`.

Album artwork is downloaded to `/config/www/vinyl-art/<device_name>-<epoch>.jpg` (the
fixed `<device_name>.jpg` is refreshed alongside it) and the `artwork` attribute points at
that epoch-named file, so every client (including Cast displays and other networks) loads
the cover from Home Assistant instead of Apple's CDN, whose URLs expire. A new file name
each time matters: `/local` is served with a 31 day cache, 404s included, and a Cast
display will not re-fetch a URL it has already seen — query strings included. The
untouched CDN URL is kept in `artwork_remote`. This needs the add-on's read-write access to
`/config` (`map: config:rw`),
which is already declared; if the folder is not writable the remote URL is published as
before.

## Install

[![Open your Home Assistant instance and add this add-on repository.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Fmatthamiltonnz%2Fvinyl-ear)

Click, pick your instance, confirm — then install. No terminal, no YAML. You need
Home Assistant OS or Supervised (add-ons), and the **Mosquitto broker** add-on.

1. **Add the repository** — the button above opens Home Assistant and asks to add this
   repository; say yes. Typing it yourself works too: **Settings → Add-ons → Add-on
   store → ⋮ → Repositories** and paste `https://github.com/matthamiltonnz/vinyl-ear`.
2. **Install** — the store now lists **Vinyl Ear** under *Add-on repositories* (refresh
   the store page if it looks empty). Open it and press **Install**.
3. **Configure and start** — on the add-on's **Configuration** tab set `mqtt_user` and
   `mqtt_password`, change `device_name` if you want something other than `turntable`,
   then press **Start**.

Watch the **Log** tab: it prints the PulseAudio source it chose and every match. The
first lookup takes a few seconds while ffmpeg and Shazam warm up.

<details>
<summary>Running it as a local add-on instead (development)</summary>

Copy the `vinyl_ear` folder to the Home Assistant host and reload the store — handy while
editing the code, because the add-on then runs straight from disk:

```bash
scp -r vinyl_ear <ssh-user>@<home-assistant-host>:/addons/vinyl_ear
```

**Settings → Add-ons → Add-on store → ⋮ → Reload**, then install *Vinyl Ear* as above.
A local add-on must be **Rebuilt** (not just restarted) after every code change.

</details>

## Before you trust the audio

The webcam microphone must actually be listening. A USB webcam mic is usually turned
right down in PulseAudio — one seen here was sitting at **12% (-55 dB)**, i.e. muted —
and no amount of fingerprinting will fix that. List the sources and raise the one that
is not a `.monitor`:

```bash
pactl list short sources
pactl set-source-volume <source> 45%
```

45% measured about -25 dB mean / -8 dB peak on a record and matched within seconds, while
70% clipped. Nothing else uses that mic (the go2rtc "Turntable" stream is video-only), so
the gain can stay where you put it.

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
- **A track change has to be confirmed.** Shazam names the same audio differently from
  one lookup to the next — the single, a deluxe re-release, a compilation — so a single
  answer is not treated as a new track: the name only replaces what is on the display
  after it has been heard twice within three minutes (at the default `poll_seconds` that
  is one extra poll, about 30 s). The first answer after silence clears the state is
  published straight away, because the alternative is a blank card, and any later
  disagreement corrects it.
- **Albums stick.** The same recording turns up on the original album, an EP and a
  dozen compilations, and Shazam picks whichever its catalogue prefers, so a track
  can change album between lookups. The first album seen for an artist + title is
  remembered in `/data/albums.json` and reused (set `keep_first_album: false` to
  take Shazam at its word).
- **Unidentified tracks never name the wrong song.** When a clip comes back empty while
  the record keeps playing — an obscure pressing, a quiet passage, a spoken intro —
  nothing has been silent for `silence_clear_seconds`, so the record cannot have been
  changed: the artist, album and cover are kept and only the track name is dropped
  (`title` empty, `unknown_track: true`, state `Artist - Album`). Set
  `blank_unknown_track: false` to hold the last recognised title instead.
- **Silence clears the state** after `silence_clear_seconds` — see
  [What it publishes](#what-it-publishes).
- **`active_entity`**, when set, pauses all of the above unless that entity is `on`.
- **The activity threshold adapts to the room, without eating the music.**
  `noise_gate_db` is only a floor: the listener learns the room's own noise level and sits
  6 dB above it, so a fridge, fan or amplifier hiss that is above the fixed gate stops
  looking like a record that never ends. The floor is taken from a *two minute* window that
  is flat — tenth and ninetieth percentile within 6 dB — because noise is steady while
  music, even a compressed master, changes song and section. The gate falls as soon as a
  lower floor is measured, but only rises once that window has been flat, the listener
  has been hearing the same uninterrupted sound for five minutes **and** Shazam has
  refused it four times: a record that keeps being named is never a noise floor, and a
  record that has just started has not had its chance yet. A floor measured within 6 dB
  of the level at which a track was last named is refused outright — that is the record,
  not the room. The log says `Room noise floor ~-30 dBFS (steady), activity threshold now
  -24 dBFS` when it moves.
- **A dip in the music is not the end of the track.** A clip survives up to a second below
  the gate before it is discarded, so the quiet bar of a track — or a chunk landing just
  under a slightly-too-high gate — cannot stop a lookup from ever happening.
- **Repeated misses back off.** After failed lookups the interval doubles, capping at
  five minutes, so an unrecognisable record doesn't burn a lookup every 30 seconds. A
  fresh silence → sound transition resets it.
- **Every five minutes** the log reports the level, the current threshold, what share of
  the last half minute was above it, whether it thinks audio is playing, how many attempts
  have failed, which track it is holding and how long ago it last asked Shazam — the
  numbers you need to tune `noise_gate_db` if the room is unusually noisy, and the quickest
  way to tell a silent room from a stuck listener.

| Option | Default | Meaning |
| --- | --- | --- |
| `source` | *(auto)* | PulseAudio source name, e.g. the line input of a USB interface. Empty picks the first non-monitor input; set it explicitly once a second capture device is plugged in, because the order is not guaranteed. |
| `active_entity` | *(none)* | Listen only while this entity is `on`, e.g. `input_boolean.activity_turntable`. Empty means always listen. Read through the supervisor's Home Assistant API; if it cannot be read the add-on keeps listening. |
| `sample_rate` | `44100` | Capture rate; Shazam downsamples internally. |
| `listen_seconds` | `15` | How much audio above the gate makes a clip. Short clips miss more often. |
| `keep_first_album` | `true` | A song can appear on several releases, and Shazam answers with whichever one its catalogue prefers — so the album can jump between lookups. Remember the album a song was first matched with (in `/data/albums.json`) and keep using it. Set `false` to always take Shazam's latest answer. |
| `blank_unknown_track` | `true` | A clip Shazam cannot name keeps the album and blanks the track name instead of repeating the previous song. Set `false` to hold the last recognised title. |
| `silence_clear_seconds` | `45` | How long the room must be judged quiet before the now-playing state is cleared (lifted needle, side change, record put away). |
| `noise_gate_db` | `-45` | dBFS considered "not silence". Room noise sits near -60; music is well above. |
| `poll_seconds` | `30` | Re-check interval while the music keeps playing. A new sound after a silent gap is recognised immediately, without waiting for this. |
| `device_name` | `turntable` | Node name for topics and entity IDs. |
| `mqtt_host` | `core-mosquitto` | Broker. Use the host's LAN address instead if you prefer. |
| `mqtt_port` | `1883` | Broker port. |
| `mqtt_user` / `mqtt_password` | *(empty)* | Leave empty for anonymous brokers. |
| `discovery_prefix` | `homeassistant` | HA's MQTT discovery prefix. |
| `log_level` | `info` | `debug` logs every capture attempt and the audio level. |
| `rip_dir` | `/share/vinyl-rips` | Where a rip is written. `/share`, `/media` and `/config` are all writable from the add-on; `/media/vinyl-rips` also shows up in Home Assistant's Media browser. |
| `rip_format` | `flac` | `flac` (lossless, about a third of the size) or `wav` (uncompressed). Both keep exactly what the input delivered. |
| `rip_max_minutes` | `30` | A rip stops itself after this long, so a forgotten switch cannot fill the disk. A 12" side is 20-25 minutes. |

## Ripping a record

Switch on the **Rip record** switch (MQTT discovery creates `switch.<device>_rip`) and drop
the needle; switch it off when the side ends. The listener keeps running while it records —
it is the same audio — so names keep appearing in the dashboard as the side plays. The
finished file is `/share/vinyl-rips/<device_name>-<YYYYmmdd-HHMMSS>.flac`, and the switch's
attributes carry the file, the elapsed seconds and the peak level.

Some things decide whether the result is worth keeping:

- **Line input, not a microphone.** A USB interface such as a Behringer UCA202/UCA222 is a
  line-level, 16-bit, 44.1/48 kHz stereo input: set the `source` option to its PulseAudio
  source (the add-on logs the name at startup, or run `pactl list short sources` in the
  add-on's terminal). A microphone gives a mono room recording, and the add-on warns when
  it is about to rip a mono source.
- **The turntable needs a phono preamp** between it and the interface — RCA line level
  with RIAA equalisation. A moving-magnet deck plugged straight in sounds thin, quiet and
  noisy; a deck with a built-in preamp, or a separate phono stage, is what the interface
  expects.
- **Set the level once.** Play a loud passage and watch the `peak_dbfs` attribute: aim for
  about -6 dBFS, never 0. The add-on reports `clipped: true` and warns when a sample hits
  full scale — vinyl clicks make that easy, so keep a few dB of headroom. Trim with
  `pactl set-source-volume <source> 100%` (or lower).
- **Lossless here means "not compressed", not "more than 16 bits".** The capture is 16-bit
  PCM at 44.1 kHz because that is what these interfaces deliver; FLAC stores it exactly,
  WAV stores it unpacked. Keep `sample_rate` at `44100` for rips — 48 kHz would resample
  CD-era material for nothing.
- **One file per side, not per track.** The add-on records faithfully and does not guess
  where the band stopped playing. Split a side afterwards with the ffmpeg already in the
  image:

  ```bash
  ffmpeg -i side-a.flac -af silencedetect=noise=-40dB:d=1 -f null -  # where the gaps are
  ffmpeg -i side-a.flac -ss 0 -to 212 -c copy track-01.flac          # then cut them out
  ```

## Lovelace card

Ready-made views live in [`lovelace/`](lovelace/):

| File | Layout |
| --- | --- |
| `turntable-overlay-card.yaml` | **Recommended** — single `custom:webrtc-camera` card, metadata pill painted over the video with `card_mod` (full-screen panel view) |
| `turntable-stack-view.yaml` | Safe fallback — metadata card above the stream, no `card_mod` |
| `turntable-picture-elements-view.yaml` | Overlay via core `picture-elements` (needs a transparent backdrop image) |
| `turntable-sections-view.yaml` | Sections view with the metadata in the view header |
| `turntable-remote-stack.yaml` | One conditional card for a remote-control stack: track details full width on top, a square (centre-cropped) view of the turntable camera underneath |

The overlay card references the cached cover by a fixed path,
`/local/vinyl-art/turntable.jpg` — change that path if `device_name` is not
`turntable`. It is not a template on purpose: a Cast display evaluates the
style once, often before the sensor has attributes, so a templated
`background-image` is baked in empty and never appears.

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
- Quiet passages, spoken intros and private pressings may not match. Nothing has been
  silent since the last match, so the album is kept and the track name is blanked
  (`Artist - Album`, `unknown_track: true`) rather than repeating the previous song;
  set `blank_unknown_track: false` to keep the last title instead.
- Silence clears it: once the room is judged quiet for `silence_clear_seconds`
  seconds — needle lifted, side changed, record put away — the state is emptied (no
  display text, no artwork, no attributes), so the dashboard stops claiming a track that
  finished minutes ago. The bundled cards hide themselves when there is nothing to show:
  no "Nothing playing" line, no "track not identified" placeholder, just the camera.
- Recognition requests only happen while the room is above the noise gate, at most
  once every `poll_seconds`.
