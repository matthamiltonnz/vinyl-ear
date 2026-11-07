# Working notes for this repo (agent memory)

Home Assistant add-on: identifies the record playing on a turntable by listening to a
room microphone, asking Shazam, and publishing the result over MQTT with Home Assistant
discovery. Python 3, `shazamio` for Shazam, `paho-mqtt` for publishing, ffmpeg for
decoding; no framework, no tests directory.

Version lives in two places and both must be bumped together:
`vinyl_ear/config.yaml` (`version:`) and `VERSION` in `vinyl_ear/app/main.py` (that is
what the startup banner prints). Current: **0.6.5**.

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
5. **`/local` is cached for 31 days, 404s included.** `curl -I /local/…/x.jpg` returns
   `Cache-Control: public, max-age=2678400`, and a *missing* file returns that same header
   with a 404. A fixed artwork path (`/local/vinyl-art/<device_name>.jpg`) therefore pins
   whatever the Cast display fetched first — usually a 404 from before the first match — and
   it stays blank for a month however often the add-on rewrites the file behind it. The panel
   pill now uses the epoch-tagged `artwork` attribute for `background-image` (a new URL per
   cover, so the browser must refetch) and keeps the fixed path only as the fallback for when
   the attribute is absent. The pill's text template updates normally on each state change,
   so the style *is* re-rendered; it was the cache, not the renderer, that hid the cover.
   That still left the cover unpainted on the cast while the *same* URL loaded fine in the
   markdown `<img>` beside the camera, so the pill also paints it with
   `ha-card::before { content: url('{{ art }}') }` — real replaced content rather than a
   background. If it ever goes missing again, compare the two: markdown `<img>` working and
   `::before` not means the trouble is painting, both failing means the URL or the file.
6. **A fixed noise gate is useless in a quiet-ish but not silent room, and a clever one
   can silence the listener.** At -45 dBFS in a -35 dBFS room every chunk counted as
   audio: the state never cleared and Shazam was called every 30 s on noise. But taking
   the tenth percentile of the last 30 s as a "noise floor" is wrong while a record plays
   — that percentile is the quiet part of the *music*, so the gate climbed to -28 dBFS,
   above the record's own dips, and because one chunk below the gate emptied the clip no
   lookup ever completed again (symptom: heartbeats and nothing else for ten minutes).
   Now `room_floor()` accepts a window only when its p10 and p90 are within
   `NOISE_SPREAD_DB` (6 dB) — noise is steady, music is not — over a
   `FLOOR_WINDOW_SECONDS` (2 min) window, and `calibrate_threshold()` is asymmetric: a
   *lower* gate is taken immediately (it only costs a lookup the back-off handles) while a
   *higher* one needs five minutes without a single recognition
   (`FLOOR_LEARN_AFTER_SECONDS`), starting at boot. Steadiness alone is not enough:
   compressed pop is flat to within 1 dB for a whole side, and 0.6.3 learned a -28 dBFS
   "floor" from a record that was playing (gate -22, above the music) on the first
   calibration after a restart, because `last_identified_at` was `None` then. A clip also
   survives `GATE_HOLD_SECONDS` (1 s) below the gate, so a dip in the music cannot throw it
   away. The heartbeat prints floor, gate, the share of the last 30 s above the gate and
   how long ago Shazam was last asked, which is what made all of this diagnosable.
   It was *not* enough for 0.6.4 in the field: a record started 8.5 h after boot, Shazam
   refused one clip, and `calibrate_threshold()` raised the gate from -45 to -23 dBFS on a
   2 min window of the record itself — above the music, which measures about -26. The
   panel showed "Nothing playing" for seven minutes because only the loudest passages
   crossed the gate, so clips never filled and, after `silence_clear_seconds`, the state
   was cleared. `last_identified_at` is the boot time, not `None`, so "5 min without a
   recognition" is true immediately whenever a record starts on an add-on that has been
   up a while. Three guards now (0.6.5): the clock runs from `segment_started_at` (when
   the current uninterrupted stretch of sound began) rather than from boot, a raise needs
   `FLOOR_LEARN_FAILURES` (4) refusals from Shazam, and a raise is refused when the floor
   it was measured from sits within `SILENCE_MARGIN_DB` of `music_level` (the median level
   at which a track was last named) — a floor that close to the music is the record, not
   the room. Keep that test on the *floor*, not on the gate: this room's hum is about
   8-10 dB below the music, so the gate has to be allowed up to `floor + 6` (about -28)
   once the record stops, or the hum keeps the state alive forever. Verified with a
   scratch harness (fake clock, synthetic chunks - see *Testing without a turntable*): on the
   deployed 0.6.4 the "music, nothing named" case raised the gate 40 s in and left the
   state OFF; 0.6.5 does not raise, still raises for a genuine steady hum (after 5 min and
   4 refusals instead of 40 s), and keeps the album of a side Shazam stops answering
   mid-way.
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

To exercise the time-based rules in seconds rather than hours, replace `module.time` with
a shim whose `monotonic()` returns a mutable value and whose other attributes fall through
to the real `time` module (`__getattr__`), have the fake `audio_reader` add
`CHUNK_SECONDS` to it per chunk, and jump it forward before the first chunk to fake an
add-on that has been up for hours. Without that jump `last_identified_at`, which is set
at boot, makes "five minutes since the last recognition" false, and the interesting case
(a record starting on an idle add-on) never happens. `mod.chunk_level()` already returns
dBFS, do not feed it to `rms_to_dbfs()`.

For MQTT, read retained payloads from inside the `homeassistant` container (it ships
`paho`) using the credentials in `/config/.storage/core.config_entries`. There is no
long-lived token, so Home Assistant's own state and API cannot be queried directly; use
the storage files under `/config/.storage` instead.

## Release checklist

1. Bump `VERSION` in `vinyl_ear/app/main.py` and `version:` in `vinyl_ear/config.yaml`.
2. `python3 -m py_compile vinyl_ear/app/main.py` — there are no automated tests.
3. `tar -czf - -C vinyl_ear . | ssh <ssh-user>@<ha-host> 'sudo -n tar -xzf - -C /addons/vinyl_ear'`
   so the local install matches the repo (both values are in `AGENTS.local.md`). The
   `-C vinyl_ear` matters: the add-on manifest has to land at `/addons/vinyl_ear/config.yaml`,
   not at `/addons/vinyl_ear/vinyl_ear/config.yaml`. Omitting it is how the add-on directory
   drifted to **0.5.0** while the running container was 0.6.4 — an unnoticed Rebuild would
   have shipped a three-release-old listener. Check `sudo grep '^version' /addons/vinyl_ear/config.yaml`
   and `sudo md5sum /addons/vinyl_ear/app/main.py` against the repo before claiming a release,
   and `sudo ls /addons/vinyl_ear` for a stale nested `vinyl_ear/` left by the old command.
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
  `keep_first_album` keeps the first answer but cannot know the sleeve in your hand. The
  same recording also comes back under different names and releases (`Tubular Bells, Pt. 1`
  vs `Tubular Bells (Two Sides Excerpt)`, seen alternating on one side), so a *change* of
  track is only published after `TRACK_CONFIRM_VOTES` sightings inside
  `TRACK_CONFIRM_SECONDS` (`agreed_track()`), and pinning the first title would pin
  whichever variant Shazam happened to answer first — do not add a `keep_first_title`.
- The square camera tile relies on `aspect-ratio` plus `object-fit: cover`; if a client
  letterboxes instead, that is where to look.
- Artwork beside a second live camera would mean two WebRTC sessions of the same stream,
  so the remote card keeps one camera and shows the cached cover next to it.

