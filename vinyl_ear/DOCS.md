# Vinyl Ear — add-on guide

Vinyl Ear works out what record is playing: it listens to an audio input, fingerprints 15-second
clips with Shazam, and publishes the artist, title, album, year and sleeve to Home Assistant over
MQTT (with discovery, so the entities appear by themselves). The same capture can be recorded
losslessly — a side at a time, split into tracks — and served as a live radio stream.

```
mic/line ──▶ noise gate ──▶ 15 s clip ──▶ Shazam (shazamio) ──▶ MQTT discovery
                                                                 ├─ sensor.<device>_now_playing
                                                                 └─ binary_sensor.<device>_audio_detected
```

## What it needs

- **An audio input on the Home Assistant host** — anything PulseAudio can read: a webcam
  microphone in the room, or (much better) a line input such as a USB interface fed by the
  turntable's own output. The add-on opens one source and reads it continuously; the same chunks
  go to Shazam, to the ripper and to the stream.
- **An MQTT broker** — the Mosquitto add-on is the usual one.
- **A phono stage** if you use a line input: a USB interface is line level, so the turntable
  needs its RIAA preamp (built in, or a separate one) before it.

## Install

1. Install the add-on, then open **Configuration**.
2. Fill in `source` if you have more than one capture device (the log prints the name it picks
   at startup; `pactl list short sources` inside the add-on terminal lists them all — skip the
   `.monitor` ones). Leave it empty otherwise.
3. Fill in `mqtt_user` / `mqtt_password` if your broker wants them, and check `device_name` —
   it drives the entity ids, the topics, the artwork filenames and the add-on's own device.
4. Set `active_entity` if you want an `input_boolean` to switch listening on and off (see
   below), or leave it empty and use the button in the web interface.
5. Start the add-on and check the log for
   `Capturing <source> at 44100 Hz, 2 channel(s)`.

## The input you point it at

- **A line input beats a microphone.** A USB interface (Behringer UCA202/UCA222 and friends) is
  line level, 16-bit, 44.1/48 kHz stereo; a microphone gives a mono room recording, which the
  add-on warns about when you start a rip.
- **Check the gain.** USB capture devices are often turned right down in PulseAudio — one webcam
  mic seen here sat at 12% (-55 dB), i.e. muted. In the add-on terminal:

  ```bash
  pactl list short sources
  pactl set-source-volume <source> 45%
  ```

  45% measured about -25 dB mean / -8 dB peak on a record. The **Set the gain** button in the
  web interface aims for about -6 dBFS peak by itself, but it cannot fix a muted source.
- The volume belongs to the source, not to the add-on, so it also changes what the stream
  sends, and it does not survive an audio-container restart.

## Options

Everything on the **Configuration** page has a default, so only `source`, `active_entity`,
`mqtt_user` and `mqtt_password` ever need a decision. The ones worth knowing:

| Option | Default | Meaning |
| --- | --- | --- |
| `source` | *(auto)* | PulseAudio source of the capture device. Set it explicitly once a second device is plugged in. |
| `active_entity` | *(none)* | Optional `input_boolean` that switches listening on and off. Empty means the web interface's **Listening** button decides. |
| `device_name` | `turntable` | Lower-case name used for the device, entity ids, topics and artwork. |
| `sample_rate` | `44100` | Capture rate; 48000 is fine too, 16000 is smaller and noisier. |
| `listen_seconds` | `15` | How much audio goes into each Shazam lookup. |
| `silence_clear_seconds` | `45` | How long the input must be quiet before the published track is cleared. |
| `noise_gate_db` | `-45` | Starting threshold for "audio is playing"; the listener learns the room's floor from the quiet between records. |
| `stream_enabled` | `false` | Serve the capture as a live audio stream on port 8098. |
| `stream_format` | `mp3` | `flac` is lossless; `mp3` is the most widely understood. |

The **rip** settings — folder, format, time limit, auto-rip, gain, trimming and splitting — are
not add-on options: they are on the add-on's own page and take effect without a restart.

## Listening on and off

The add-on only works while it is "listening": with the gate closed it still captures and still
streams, but it does not look anything up.

- If `active_entity` is set, that entity decides. The web interface shows the state and disables
  its button.
- If `active_entity` is empty, the web interface's **Start listening / Stop listening** button
  decides. It starts off and is remembered across restarts.
- If the entity cannot be read at all (no `homeassistant_api`), the manual switch takes over.

## The web interface

**Open Web UI** on the add-on page (or add it to the sidebar) is the add-on's own page, behind
Home Assistant's ingress:

- **Shazam detection** — the track and sleeve, a level meter with the gate marked, the activity
  state, the measured room floor, how many lookups were refused, and what Shazam last answered.
  If nothing seems to be happening, check the `activity` row first.
- **Rip** — state, elapsed time, peak level, the file being written, a Start/Stop button and
  **Set the gain**, plus the finished recordings with play, download and delete.
- **Rip settings** — folder, format, time limit, auto start, auto stop, auto gain, trim silence
  and split tracks. Saved with **Save settings**, applied immediately.
- **Radio stream** — whether the encoder is running, how many listeners, the codec, and the URL
  with a **Copy URL** button.
- **Event log** — the last 40 things that happened: recognitions, refusals, gate changes, rip
  starts and stops, a stalled capture.

## Ripping a side

1. Press **Start a rip** *before* dropping the needle (or tick **auto start** to let sound start
   it for you).
2. Play the side. The file is written as it arrives, in the folder shown in the rip settings.
3. With **auto stop** on, the rip ends by itself once the input has been quiet for the configured
   window — but only after it has heard music, so brushing the record never ends a side.
4. With **trim silence** on, the run-in and run-out are cut from the finished file, keeping half
   a second of music either side.
5. With **split tracks** on, the side is cut at its gaps and the pieces are numbered
   `<name>-01.flac`, `<name>-02.flac`, …. The whole-side file is kept.

Recordings land in `/share/vinyl-rips` by default, which is the `share` folder on the Home
Assistant host. **Set the gain** aims the capture so the loudest sample sits near -6 dBFS; it
needs the resolved source name, which the add-on already knows.

## Streaming

With `stream_enabled` on, the capture is also served as a radio stream on port 8098:

```
http://<home-assistant>:8098/stream.flac
http://<home-assistant>:8098/stream.mp3
http://<home-assistant>:8098/now-playing
```

It is a plain HTTP audio stream, so anything that takes a radio URL can play it — Music
Assistant, VLC, a phone, another Home Assistant. MP3 carries the current track as ICY metadata;
FLAC has none, so use `/now-playing` there. The port is published because Music Assistant is not
a browser and cannot carry an ingress session; the web interface stays inside ingress.

## If it does not seem to work

- **`activity` is off** — the gate is closed by design; switch it on in the web interface or turn
  the `input_boolean` on.
- **The level is `—` or the page looks frozen** — no audio is arriving at all. The add-on says so
  in the event log (`No audio from the source for 30 s — capture stalled?`) and reopens the
  capture by itself; check that the source exists and is not muted.
- **`audio above gate` is always `no`** — the gain is too low, or the gate is above the music.
  Set the source volume and press **Set the gain**.
- **"track not identified"** — Shazam does not know the pressing. The album and sleeve are kept;
  the title goes blank. Obscure and private pressings are simply unknown to it.

## More

The [repository README](https://github.com/matthamiltonnz/vinyl-ear#readme) covers the design,
the noise-gate reasoning, the artwork cache and the Lovelace cards.

