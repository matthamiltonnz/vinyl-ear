# Working notes for this repo (agent memory)

Home Assistant add-on: identifies the record playing on a turntable by listening to a
room microphone, asking Shazam, and publishing the result over MQTT with Home Assistant
discovery. Python 3, `shazamio` for Shazam, `paho-mqtt` for publishing, ffmpeg for
decoding; no framework, no tests directory.

Version lives in two places and both must be bumped together:
`vinyl_ear/config.yaml` (`version:`) and `VERSION` in `vinyl_ear/app/main.py` (that is
what the startup banner prints). Current: **0.6.0**.

```
vinyl_ear/config.yaml      add-on manifest: audio: true, homeassistant_api: true,
                           map: config:rw, options + schema
vinyl_ear/Dockerfile       python:3.11-slim + ffmpeg + pulseaudio-utils
vinyl_ear/app/main.py      everything: capture, noise gate, Shazam, artwork cache,
                           MQTT discovery, run loop
vinyl_ear/run.sh           sets PULSE_SERVER to /run/audio/pulse.sock when present
lovelace/*.yaml            card/view YAML that is pasted or patched into Lovelace
repository.yaml            makes this repo an add-on repository
```

## The live installation this was built against

This repository is public, so host addresses, the SSH user, the microphone source name
and the broker address are **not** in it. They live in `AGENTS.local.md` beside this file,
which is gitignored — read that before touching the live installation. Everything below
is architecture, not addresses.

- Home Assistant OS **2026.11.0** on a home LAN, reached over SSH as the Advanced SSH
  add-on's user with passwordless `sudo` (the `ha` CLI is **not** authorised from there,
  but `docker` is).
- Installed as a **local add-on** from `/addons/vinyl_ear` → container
  `app_local_vinyl_ear`. Editing this repo is not enough: the add-on must be
  **Rebuilt** from the Add-on page. Hacks inside the running container do not survive it.
- Microphone: a USB webcam microphone; the model and its exact PulseAudio source are in
  `AGENTS.local.md`. The go2rtc "Turntable" stream is video-only (`/dev/video0`, mjpeg),
  so the mic is free.
- Capture gain was 12 % (-55 dBFS) when this started, i.e. deaf; 45 % works. Set with
  `pactl set-source-volume <source> 45%` over `PULSE_SERVER=unix:/run/audio/pulse.sock`.
- Room noise floor measured **-35 dBFS**; music measured about **-23 dBFS**. That is why
  a fixed -45 dBFS gate failed (see gotcha 6).
- MQTT: Mosquitto add-on, broker on the HA host (`AGENTS.local.md` has the address and
  port), user and password in the add-on options (never in this repo). Topics live under
  `vinyl_ear/<device_name>/`.
- `device_name` (default `turntable`) drives the HA device, the entity ids
  (`sensor.vinyl_ear_<device_name>_now_playing`,
  `binary_sensor.vinyl_ear_<device_name>_audio_detected`), the topic prefix and the
  artwork filename. Documentation must never present `turntable` as fixed.
- Artwork is cached to `/config/www/vinyl-art/<device_name>.jpg` and published as

## Lovelace

Storage-mode dashboard, so views are edited by patching
`/config/.storage/lovelace.lovelace` (JSON) **with Home Assistant stopped**, then
`docker start homeassistant`. HA caches the dashboard config at startup, so a file edit
alone shows nothing; and an open editor tab writes its stale copy back over any patch
when the user presses Done. Take a `lovelace.lovelace.bak-<timestamp>` copy every time.

Relevant cards on that installation:

- View `views[0]` (title in `AGENTS.local.md`) → `vertical-stack` → **card 6** is the turntable
  conditional: `vertical-stack [ markdown(details), horizontal-stack[ markdown(artwork),
  webrtc-camera(square) ] ]`, gated on `input_boolean.activity_turntable == on`.
- View `Turntable` (path `turntable`): `type: panel`, one `custom:webrtc-camera` with a
  `card_mod` pill (`ha-card::after`) — this is the one that gets cast to the TV.
- `custom:webrtc-camera` comes from the WebRTC integration; `lovelace-card-mod` is
  installed, so `card_mod` is available everywhere.

## Gotchas, with the symptom that exposed each

1. **ALSA cannot reach USB mics inside a container on HAOS.** `ffmpeg -f alsa -i hw:1,0`
   fails with EIO/EBUSY (PulseAudio holds the device and `/proc/asound` is absent), and
   PortAudio enumerates nothing — which is why
   [shazam2mqtt](https://github.com/TigreGotico/shazam2mqtt) cannot work here, and
   [ha_shazamio](https://github.com/iamjoshk/ha_shazamio) does not capture at all. Use
   the PulseAudio socket `/run/audio/pulse.sock` with `parec` or `ffmpeg -f pulse`;
   add-ons with `audio: true` get it mounted automatically.
2. **CSS hex escapes swallow the next character.** `"Laufey\ABest Friend"` parsed as
   `\ABe` (U+0ABE) followed by `st Friend`, shown as "Laufey*lst Friend";
   `"sombr\Adime"` parsed as `\Ad` (soft hyphen) followed by `ime`, shown as "sombrime".
   Always use the six digit `\00000A` for a newline inside a CSS `content` string; in a
   YAML block that is written `'\\00000A'`.
3. **The markdown renderer drops `display: flex`** on a wrapping `<div>`, so an image and
   its text stack instead of sitting side by side. `float: left` on the `<img>` works, as
   do plain `<div>`/`<img style>` blocks.
4. **`picture-elements` is not safe on every client.** On an Android 14 device running
   Chrome 92 it produced a "Configuration error" card while the frontend logged
   `SyntaxError: Unexpected token '{'` — a lazily loaded chunk that browser cannot parse.
   Prefer cards already proven on the target screen.
5. **A Cast display renders a `card_mod` style once**, usually before the entity carries
   attributes, so a templated `background-image` can be baked in empty and never appear.
   The overlay pill therefore references the cover by a fixed path, not `{{ art }}`; the
   text template still updates normally.
6. **A fixed noise gate is useless in a quiet-ish but not silent room.** At -45 dBFS in a
   -35 dBFS room every chunk counted as audio: the state never cleared and Shazam was
   called every 30 s on noise. `calibrate_threshold()` takes the tenth percentile of the
   last 30 s and sets the threshold 6 dB above it, with `noise_gate_db` as a floor, and
   only recalibrates while the room looks quiet or when the level is steady and lookups
   keep failing.
7. **The supervisor's Home Assistant API needs `homeassistant_api: true`.** Without it the
   `active_entity` poll returns 401; the code raises `GateUnavailable`, logs **once** and
   disables the gate instead of warning every 5 s.
8. **Apple's artwork URLs expire**, and other clients (a TV) may not fetch that CDN at
   all, which is why the cover is cached beside the HA config and served from `/local`.

  `/local/vinyl-art/<device_name>.jpg?v=<epoch>`; the untouched CDN URL is kept in
  `artwork_remote`. Needs `map: config:rw`.

## Testing without a turntable

The add-on image already has every dependency, so use it as the test bed:

```bash
sudo docker exec -i app_local_vinyl_ear sh -c 'cat > /tmp/main_under_test.py' < vinyl_ear/app/main.py
sudo docker exec -i app_local_vinyl_ear sh -c 'cat > /tmp/test.py' < /tmp/local_test.py
sudo docker exec app_local_vinyl_ear python /tmp/test.py
```

When importing a module by path, register it in `sys.modules` **before**
`spec.loader.exec_module()`, otherwise `@dataclasses.dataclass` fails resolving
annotations.

For the state machine, monkeypatch `audio_reader` (push synthetic chunks paced at
`CHUNK_SECONDS`), `recognise`, `cache_artwork` and `publish_discovery`, then run
`module.run(cfg, FakeClient())` under `asyncio.wait_for` and print what was published.
Generate tones with `struct.pack("<h", …)`: amplitude 820 is about -35 dBFS, 3600 about
-22 dBFS. That is how the threshold calibration, the silence clearing and the back-off
were verified.

For MQTT, read retained payloads from inside the `homeassistant` container (it ships
`paho`) using the credentials in `/config/.storage/core.config_entries`. There is no
long-lived token, so Home Assistant's own state and API cannot be queried directly; use
the storage files under `/config/.storage` instead.

## Release checklist

1. Bump `VERSION` in `vinyl_ear/app/main.py` and `version:` in `vinyl_ear/config.yaml`.
2. `python3 -m py_compile vinyl_ear/app/main.py` — there are no automated tests.
3. `tar -czf - . | ssh <ssh-user>@<ha-host> 'sudo -n tar -xzf - -C /addons/vinyl_ear'` so
   the local install matches the repo (both values are in `AGENTS.local.md`).
4. Commit and push to https://github.com/matthamiltonnz/vinyl-ear (public — check the
   diff for anything installation specific first, see below).
5. The user presses **Rebuild** on the add-on page; Lovelace changes land through an HA
   restart (see above).
6. Remove scratch files from `/config` and `/tmp` — `/config` is their live Home
   Assistant configuration directory.
7. Before pushing: `grep -rInE '([0-9]{1,3}\.){3}[0-9]{1,3}|@|alsa_input|ssh ' --exclude-dir=.git .`
   should return only `<...>` placeholders. The repo is public, so host addresses, emails,
   SSH users, mic source names and broker details must all stay in `AGENTS.local.md`.
8. Commit identity must stay `matthamiltonnz <matthamiltonnz@users.noreply.github.com>`
   (set repo-locally). A personal address crept into the first twelve commits and had to be
   rewritten out; do not let one back in.

## Known limitations

- Shazam here is reverse engineered (`shazamio`): no key, no quota, but it can break.
  ACRCloud or AudD would slot in behind the same interface.
- Obscure and private pressings are simply unknown to Shazam. Rather than holding the last
  title, the listener keeps the album and blanks the track name (`unidentified_track()`,
  `unknown_track: true`) because no `silence_clear_seconds` gap has passed — so a side
  change with less than that much quiet keeps the previous album until the state clears.
- Album and year come from Shazam's catalogue, not the physical pressing;
  `keep_first_album` keeps the first answer but cannot know the sleeve in your hand.
- The square camera tile relies on `aspect-ratio` plus `object-fit: cover`; if a client
  letterboxes instead, that is where to look.
- Artwork beside a second live camera would mean two WebRTC sessions of the same stream,
  so the remote card keeps one camera and shows the cached cover next to it.

