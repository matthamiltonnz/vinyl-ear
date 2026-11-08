# Working notes for this repo (agent memory)

Home Assistant add-on: identifies the record playing on a turntable by listening to a
microphone or a line input, asking Shazam, and publishing the result over MQTT with Home
Assistant discovery. The same capture can be recorded - stereo, 16-bit, 44.1/48 kHz, FLAC
or WAV - a side at a time, for ripping. Python 3, `shazamio` for Shazam, `paho-mqtt` for
publishing, ffmpeg for decoding and for the FLAC encode; no framework, no tests directory.

Version lives in two places and both must be bumped together:
`vinyl_ear/config.yaml` (`version:`) and `VERSION` in `vinyl_ear/app/main.py` (that is
what the startup banner prints). Current: **0.7.1**.

```
vinyl_ear/config.yaml      add-on manifest: audio: true, homeassistant_api: true,
                           map: config:rw + share:rw (rips), options + schema
vinyl_ear/Dockerfile       python:3.11-slim + ffmpeg + pulseaudio-utils
vinyl_ear/app/main.py      everything: capture, noise gate, Shazam, artwork cache,
                           MQTT discovery, rip recorder, run loop, the ingress web UI
                           (aiohttp server + the page it serves, as one string)
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
- A line input is being added (Behringer UCA202/UCA222: line level, 16-bit, 44.1/48 kHz,
  stereo) so the listener hears the record rather than the room and so sides can be
  ripped. Its source name goes in `AGENTS.local.md` and in the add-on's `source` option -
  with two capture devices the auto-pick is not deterministic.
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

With Home Assistant stopped its container cannot `docker exec`, so patch from a one-off
container built on the same image (it has PyYAML):
`sudo docker stop --timeout 60 homeassistant` then
`sudo docker run --rm -i --volumes-from homeassistant --entrypoint python ghcr.io/home-assistant/generic-x86-64-homeassistant:<version> -`
with the script on stdin. `-v /config:/config` does **not** work on HAOS (there is no such
path on the host), and `/tmp` bind mounts come up empty, so keep the patch self-contained.
Also note that `card_mod` and markdown templates are rendered *server side* through the
`render_template` websocket call: a broken one shows up in `docker logs homeassistant` as
`Error while processing template: Template<template=(...)` — check that log after every
dashboard change, and remember a client that was already open keeps re-rendering its old
copy, so errors can appear for a minute after the restart. Parse every template with
`jinja2.Environment().parse()` before writing the file; a stray `{%- endif %}` closing the
wrong `if` is exactly how a working pill turns into "Encountered unknown tag 'else'".

Relevant cards on that installation:

- View `views[0]` (title in `AGENTS.local.md`) → `vertical-stack` → **card 6** is the turntable
  conditional: `vertical-stack [ markdown(details), horizontal-stack[ markdown(artwork),
  webrtc-camera(square) ] ]`, gated on `input_boolean.activity_turntable == on`.
- View `Turntable` (path `turntable`): `type: panel`, one `custom:webrtc-camera` with a
  `card_mod` pill (`ha-card::after`) — this is the one that gets cast to the TV.
- `custom:webrtc-camera` comes from the WebRTC integration; `lovelace-card-mod` is
  installed, so `card_mod` is available everywhere.

Both turntable cards hide themselves when nothing is named — that is the rule the user
asked for in 0.6.6: no "Nothing playing" line, no "track not identified" placeholder, no
pill over the camera until there is an artist, album or title to put in it. An unrecognised
track keeps the artist and sleeve from the last answer and drops its own line; the add-on
publishes an empty `display` for idle and for "audio but nothing known", so the templates
test the attributes rather than the state string. `lovelace/turntable-remote-stack.yaml`
and `lovelace/turntable-overlay-card.yaml` are the two files that mirror this dashboard;
patch the storage file from those, do not hand-edit the JSON.

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
   It was still missing on the cast with all of that in place (0.6.6): the pill's *text*
   rendered, so painting was fine and it was the image request that failed — and the only
   artwork URL that cast had ever been given was the fixed-path fallback, because the pill
   painted `url('/local/vinyl-art/<device>.jpg')` whenever the state carried no `artwork`
   attribute (idle, or a record nobody had named). That path is the cached 404. The add-on
   now writes each cover to `<device_name>-<epoch>.jpg` and publishes that path, so no
   client ever sees the same artwork URL twice; the fixed `<device_name>.jpg` is only kept
   in step for anything else that points at it. The cards no longer fall back to it at all.
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
9. **A second `parec` on the same USB source can come back as digital silence.** Measured
   here: with the add-on already reading the USB webcam's source, a second `parec` on it
   returned 12 s of exact zeros (peak -270 dBFS) while the first stream was hearing music.
   That is why the rip is not a second reader: `audio_reader()` takes a `tap` callback and
   the same chunks go to `rip_worker()`, so recording and Shazam share one capture. It also
   means the rip cannot drift out of step with what the listener hears, and there is no
   "which stream is the real one" question when a line input is added.
10. **A line input needs a phono preamp, and the channel count must come from `pactl`.**
   The UCA202/UCA222 (and any USB class-compliant interface) is line level, 16-bit,
   44.1/48 kHz stereo — it is not a phono stage, so the turntable must arrive with RIAA
   equalisation already applied. `source_channels()` reads the `s16le 2ch` column of
   `pactl list short sources`; asking `parec` for `--channels=2` on a mono source gets a
   duplicated mono channel, and asking for mono on a stereo one throws half the signal
   away, so the count is discovered rather than assumed. With two capture devices plugged
   in, `source` must be set explicitly: "the first non-monitor source" is not stable.
11. **A rip command was only served when audio arrived.** `rip_worker()` looked at
    `commands` and then sat in `await tape.get()`, so pressing Stop — MQTT switch or web UI —
    while the capture stream was silent *or gone* did nothing until the next chunk arrived.
    With `parec` alive that is 0.1 s and invisible; with it dead the rip could not be stopped
    at all (not even by `rip_max_minutes`, whose check was also behind the read). The worker
    now uses `tape.get_nowait()` and sleeps `RIP_POLL_SECONDS` (0.5 s) when the tape is empty,
    which serves commands and the time limit whatever the audio is doing. Found by feeding
    `rip_worker()` a hand-made tape: the fake reader stopped, and `OFF` sat in the queue
    while the state stayed `recording`.

## Ripping

`RipRecorder` + `rip_worker()` write WAV as the chunks arrive (a `wave` handle, patched on
close) and encode to FLAC with ffmpeg at stop, deleting the WAV on success and keeping it
with a warning when the encode fails — a failed encode must never lose the side. Rip state
lives on `vinyl_ear/<device>/rip` (progress JSON) and `.../rip/state` (`ON`/`OFF`, retained),
and the switch entity drives `.../rip/set`. `rip_max_minutes` bounds a forgotten switch.
Files land in `rip_dir`, `/share/vinyl-rips` by default, which is why the manifest now maps
`share:rw`.

## Web interface

`web_server()` is the third task beside `audio_reader()` and `rip_worker()` inside `run()`,
bound to `0.0.0.0:8099` — that number must equal `ingress_port` in `config.yaml`, because
ingress proxies to it inside the container and the port is never published. `ingress: true`
is what puts **Open Web UI** on the add-on page and in the sidebar; `webui:` was deliberately
*not* used, since without a `ports:` mapping that button points at a port nothing listens on.

There is no second state store. `run()` keeps one `status` dict and mutates it while it
listens — level, the last `WEB_LEVELS` (120) chunk levels, `playing`, refusals, threshold and
measured floor, last lookup time and answer, the published track, the rip snapshot and a
40-line event log through `note()` — and `rip_worker()` writes the very object `report()`
publishes to `.../rip` into `status["rip"]`, so the page agrees with Home Assistant by
construction. The page polls `/api/status` once a second. `POST /api/rip` with
`{"action":"start"|"stop"}` puts `ON`/`OFF` on **the same `commands` queue the MQTT switch
uses**, so there is one worker and one code path. Recordings are served by *name* only
(`GET /files/<name>`, `?download=1`, `DELETE`), resolved inside `rip_dir` with a parent
check: there is no path parameter to traverse, and this is the add-on's only write/delete
surface.

Everything the page asks for is **relative** (`fetch("api/status")`, `href="files/…"`), which
is what makes it work under the ingress prefix `/api/hassio_ingress/<token>/` without knowing
the prefix. Absolute `/api/...` URLs would hit Home Assistant itself and 404. Artwork is the
exception and stays as published: `/local/vinyl-art/...` is root-relative on the same origin,
which is correct because the ingress page is served from Home Assistant's origin.

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

The rip is easy to test the same way and worth testing, because it touches the filesystem:
point `rip_dir` at `/tmp/…`, call `RipRecorder.start()`/`write()`/`finish()` with synthetic
stereo chunks, and check the result with `ffprobe -show_entries
stream=codec_name,channels,sample_rate:format=duration` — a 4 s stereo sine comes out as
`pcm_s16le 44100 2ch 4.000000` for WAV and `flac 44100 2ch 4.000000` for FLAC. To exercise
the MQTT end of it, drive `rip_worker()` with a tape queue and a command queue directly, or
go through `run()` with a fake client that records `on_message` and call
`client.on_message(None, None, Message("ON"))` — that covers thread-safe hand-off, the
worker, the encode and the published state.

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

The web UI is worth testing too, because it is the add-on's only write surface, and
`aiohttp` is already in the image. Load `main.py` by path, start
`asyncio.create_task(web_server(cfg, status, commands))` with a hand-made `status` dict and a
`rip_dir` full of fake files, and drive it with a `ClientSession`: `GET /` (200, and the page
has `id="ripButton"`), `GET /api/status` (the rip list skips non-`.flac`/`.wav` names and
directories), `POST /api/rip` with `{"action":"start"}` (leaves `"ON"` on the queue, so the
MQTT switch and the button provably share one path) and with `{"action":"rewind"}` (400,
queue untouched), `GET /files/<name>` (inline) and `?download=1` (attachment),
`GET /files/notes.txt` and `GET /files/%2e%2e%2fmain.py` (both 404), then `DELETE` and check
the file is gone and the listing shrank. Move `WEB_PORT` off 8099 in the harness so it cannot
collide with a real Home Assistant on the developer's machine. Driving `rip_worker()` with a
fake tape and a fake client checks the other half: `status["rip"]` goes `recording` → `idle`,
names the `.wav` while recording and the `.flac` after, and — gotcha 11 — the tape must be
allowed to go *dry* before `OFF` is queued. The whole `run()` can be driven the same way with
`audio_reader`, `recognise`, `cache_artwork`, `publish_discovery`, `find_source` and
`source_channels` monkeypatched and `Shazam` stubbed with a bare class; that is what proves the
page reads the live `status` rather than a snapshot.

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
   That nested copy is not merely clutter: it is a **second add-on with the same slug**, the
   supervisor picks it up, and every Rebuild then builds *it* — the symptom is
   `Starting build for local/amd64-addon-vinyl_ear:0.6.4` in `docker logs hassio_supervisor`
   while `/addons/vinyl_ear/config.yaml` says 0.7.0, no new image tag, and the Add-on page
   showing "no update". Nothing in the UI fixes that; delete the nested folder (keep
   `config.yaml Dockerfile requirements.txt run.sh app/` and nothing else), then nudge the
   supervisor, because it caches add-on metadata across restarts: `POST /addons/reload` and
   then `POST /addons/<slug>/update` through `http://supervisor` with the **Home Assistant
   container's** `SUPERVISOR_TOKEN` (`docker exec homeassistant python -` with a script on
   stdin). `curl`ing it from the SSH add-on will not work — its token only reaches core.
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

- The web UI only exists through ingress. It has no authentication of its own, so there is
  no `ports:` mapping and no `webui:` — a published 8099 would let anyone on the LAN start,
  stop and delete rips. If it ever has to be exposed, put it behind HA's auth proxy rather
  than adding a password to the page.
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

