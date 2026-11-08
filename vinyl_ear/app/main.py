"""Vinyl Ear — identify the record playing on the turntable, publish it to Home Assistant.

Audio comes from the host microphone through PulseAudio (Home Assistant exposes the
socket to add-ons that set `audio: true`). A clip is recorded only while the signal is
above a noise gate, fingerprinted with Shazam (shazamio), and the result is published to
MQTT with Home Assistant discovery.
"""

from __future__ import annotations

import array
import asyncio
import collections
import dataclasses
import json
import logging
import math
import os
import re
import shutil
import signal
import statistics
import subprocess
import tempfile
import threading
import time
import wave
from pathlib import Path
from typing import Any, Awaitable, Callable

import aiohttp
import paho.mqtt.client as mqtt
from aiohttp import web
from shazamio import Shazam

OPTIONS_FILE = Path(os.environ.get("VINYL_EAR_OPTIONS", "/data/options.json"))
ARTWORK_DIR = Path("/config/www/vinyl-art")
# How many epoch-named covers to keep beside the fixed `device_name.jpg`.
ARTWORK_KEEP = 5
ALBUM_STORE = Path("/data/albums.json")
VERSION = "0.8.0"
FULL_SCALE = 32768.0
CHUNK_SECONDS = 0.25
GAP_SECONDS = 2.0
PREBUFFER_SECONDS = 1.5
SILENCE_DEBOUNCE_SECONDS = 2.0
ACTIVE_CHECK_SECONDS = 5.0
SILENCE_MARGIN_DB = 6.0
# A chunk below the gate in the middle of the music is a dip, not the end of the
# track: the clip survives until the room has been quiet for this long.
GATE_HOLD_SECONDS = 1.0
# Noise barely moves over half a minute; music swings with every bar. Only a window
# this flat is accepted as the noise floor.
NOISE_SPREAD_DB = 6.0
# The floor is measured over a longer window than the gate: noise is steady for minutes,
# music - even heavily compressed music - changes song and section over two.
FLOOR_WINDOW_SECONDS = 120.0
# Stop learning the floor while a record is still being recognised.
FLOOR_LEARN_AFTER_SECONDS = 300.0
# ... and until Shazam has refused the same audio this many times. A record that has
# only just started has not had its chance to be named yet, and 0.6.4 raised the gate
# into a playing record because one failed lookup plus the time since boot was enough.
FLOOR_LEARN_FAILURES = 4
CALIBRATION_SECONDS = 1.0
LEVELS_WINDOW = int(30 / CHUNK_SECONDS)
FLOOR_WINDOW = int(FLOOR_WINDOW_SECONDS / CHUNK_SECONDS)
# Shazam answers the same audio with different catalogue entries, so a track change
# is only believed once it has been seen this many times within this many seconds.
TRACK_CONFIRM_VOTES = 2
TRACK_CONFIRM_SECONDS = 180.0
TRACK_CONFIRM_LOOKBACK = 4
MAX_COOLDOWN_SECONDS = 300.0
HEARTBEAT_SECONDS = 300.0
# How often a running rip publishes its elapsed time and peak level.
RIP_REPORT_SECONDS = 10.0
# How long the rip worker waits for a chunk before looking at its command queue again.
RIP_POLL_SECONDS = 0.5
# Auto-rip: music above the gate for this long means the needle has dropped, and this long
# below it means the side has ended. The start is deliberately shorter than the stop so the
# run-out groove (which is noisy) does not stop a rip the moment the last track ends.
AUTO_START_SECONDS = 3.0
AUTO_COMMAND_GUARD_SECONDS = 5.0
# Audio kept in front of the auto-start decision and written into the rip, so the first
# seconds of the side are not lost to the time it takes to notice the music.
RIP_PREROLL_SECONDS = 4.0
# Gain calibration: aim the loudest sample at this level, measure over this long, and refuse
# to touch the volume when the input is this quiet (nothing is playing) or already close.
GAIN_TARGET_DBFS = -6.0
GAIN_SAMPLE_SECONDS = 5.0
GAIN_SILENT_DBFS = -50.0
GAIN_DEADBAND_DB = 3.0
GAIN_MIN_VOLUME = 5.0
GAIN_MAX_VOLUME = 100.0
# Track splitting: a gap is a stretch this much below the side's own loud level, this long,
# and a piece has to be at least this long to count as a track.
TRACK_GAP_DEPTH_DB = 18.0
TRACK_GAP_SECONDS = 1.5
TRACK_MIN_SECONDS = 10.0
TRACK_MAX_COUNT = 24
# The web UI is served on this port and reached through Home Assistant ingress (see
# `ingress_port` in config.yaml); it is never published to the LAN.
WEB_PORT = 8099
# How many chunk levels the UI keeps for its level history.
WEB_LEVELS = 120
# Chunks buffered between the capture and the rip writer: 2 s of audio, enough to cover a
# slow disk or a FLAC encode without stalling the listener for long.
RIP_QUEUE_CHUNKS = 8

log = logging.getLogger("vinyl_ear")
stop = threading.Event()


class CaptureError(RuntimeError):
    """Raised when the PulseAudio capture stream cannot be opened or dies."""


class GateUnavailable(RuntimeError):
    """The supervisor will not let this add-on read Home Assistant states."""


@dataclasses.dataclass(frozen=True)
class Config:
    source: str
    active_entity: str
    sample_rate: int
    listen_seconds: int
    silence_clear_seconds: int
    keep_first_album: bool
    blank_unknown_track: bool
    noise_gate_db: float
    poll_seconds: int
    device_name: str
    mqtt_host: str
    mqtt_port: int
    mqtt_user: str
    mqtt_password: str
    discovery_prefix: str
    log_level: str
    rip_dir: str
    rip_format: str
    rip_max_minutes: int
    rip_auto: bool = False
    rip_auto_gain: bool = False
    rip_auto_stop_seconds: int = 20
    rip_split: bool = False

    def bytes_per_second(self, channels: int = 1) -> int:
        return self.sample_rate * 2 * channels  # signed 16-bit little endian

    @property
    def base_topic(self) -> str:
        return f"vinyl_ear/{self.device_name}"


def load_config() -> Config:
    raw: dict[str, Any] = {}
    if OPTIONS_FILE.is_file():
        raw = json.loads(OPTIONS_FILE.read_text())
    return Config(
        source=str(raw.get("source") or "").strip(),
        active_entity=str(raw.get("active_entity") or "").strip(),
        sample_rate=int(raw.get("sample_rate", 44100)),
        listen_seconds=int(raw.get("listen_seconds", 15)),
        silence_clear_seconds=int(raw.get("silence_clear_seconds") or 45),
        keep_first_album=bool(raw.get("keep_first_album", True)),
        blank_unknown_track=bool(raw.get("blank_unknown_track", True)),
        noise_gate_db=float(raw.get("noise_gate_db", -45)),
        poll_seconds=int(raw.get("poll_seconds", 30)),
        device_name=str(raw.get("device_name", "turntable")).strip() or "turntable",
        mqtt_host=str(raw.get("mqtt_host", "core-mosquitto")),
        mqtt_port=int(raw.get("mqtt_port", 1883)),
        mqtt_user=str(raw.get("mqtt_user") or ""),
        mqtt_password=str(raw.get("mqtt_password") or ""),
        discovery_prefix=str(raw.get("discovery_prefix", "homeassistant")).strip("/"),
        log_level=str(raw.get("log_level", "info")),
        rip_dir=str(raw.get("rip_dir") or "/share/vinyl-rips").strip(),
        rip_format=str(raw.get("rip_format") or "flac").strip().lower(),
        rip_max_minutes=int(raw.get("rip_max_minutes", 30)),
        rip_auto=bool(raw.get("rip_auto", False)),
        rip_auto_gain=bool(raw.get("rip_auto_gain", False)),
        rip_auto_stop_seconds=int(raw.get("rip_auto_stop_seconds", 20)),
        rip_split=bool(raw.get("rip_split", False)),
    )


def rms_to_dbfs(rms: float) -> float:
    return 20.0 * math.log10(max(rms, 1e-9) / FULL_SCALE)


def room_floor(levels: "collections.deque[float]") -> float | None:
    """The quiet level of the room, or None when the window is not quiet.

    Noise is steady: a fan, a fridge or amplifier hiss barely moves across minutes, so
    its tenth and ninetieth percentiles sit close together. Music - even a heavily
    compressed master - changes song and section, so its tenth percentile is simply the
    quiet part of the music; takingthat for a noise floor is how the gate climbed into
    the music and stopped the listener calling Shazam at all.
    """
    window = sorted(levels)
    if len(window) < FLOOR_WINDOW // 3:
        return None
    low = window[len(window) // 10]
    high = window[len(window) * 9 // 10]
    if high - low > NOISE_SPREAD_DB:
        return None
    return low


def calibrate_threshold(
    levels: "collections.deque[float]",
    cfg: Config,
    since_match: float,
    current: float,
    failures: int,
    music_level: float | None,
) -> tuple[float, float] | None:
    """Return (floor, threshold), or None to keep the current threshold.

    A fixed gate is useless in a room whose fan or amplifier sits above it: the listener
    then sees 'audio' forever, never identifies anything and never settles. Learn the
    room instead, and be asymmetric about it: a lower gate only costs a Shazam call the
    back-off already handles, while a higher one can stop the record being heard at all.
    So the gate drops as soon as a steady floor is measured, and only rises after two
    minutes of steady room *and* five minutes of the same unrecognised sound *and* at
    least FLOOR_LEARN_FAILURES refusals from Shazam - a record being named is not a noise
    floor, however compressed its master is. A floor measured within SILENCE_MARGIN_DB of
    the level at which this listener last named a track is refused outright: that is the
    record, not the room, and a gate learned from it stops the record being heard.
    """
    floor = room_floor(levels)
    if floor is None:
        return None
    wanted = min(
        max(floor + SILENCE_MARGIN_DB, cfg.noise_gate_db),
        cfg.noise_gate_db + 25.0,
    )
    if wanted >= current - 1.0:
        if since_match < FLOOR_LEARN_AFTER_SECONDS or failures < FLOOR_LEARN_FAILURES:
            return None
        if music_level is not None and floor > music_level - SILENCE_MARGIN_DB:
            return None
    return floor, wanted


def list_sources() -> list[tuple[str, int]]:
    """PulseAudio capture sources as (name, channels); output monitors are skipped."""
    if shutil.which("pactl") is None:
        raise CaptureError("pactl not found; set the 'source' option explicitly")
    result = subprocess.run(
        ["pactl", "list", "short", "sources"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if result.returncode != 0:
        raise CaptureError(f"pactl failed: {result.stderr.strip() or 'unknown error'}")
    found: list[tuple[str, int]] = []
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) < 2:
            continue
        name = fields[1]
        if ".monitor" in name:
            continue
        channels = 1
        # The spec column reads like "s16le 2ch 44100Hz" - sometimes without the rate.
        for token in " ".join(fields[2:]).split():
            if token.endswith("ch") and token[:-2].isdigit():
                channels = int(token[:-2])
        found.append((name, channels))
    return found


def find_source(cfg: Config) -> str:
    """Return the PulseAudio source to record from."""
    sources = list_sources()
    names = [name for name, _ in sources]
    if cfg.source:
        if cfg.source in names:
            return cfg.source
        raise CaptureError(
            f"configured source {cfg.source!r} is not available; capture sources are: "
            + (", ".join(names) if names else "none")
        )
    if not names:
        raise CaptureError("no capture source found; is the line input plugged in?")
    log.info("Using PulseAudio source %s (capture sources: %s)", names[0], ", ".join(names))
    return names[0]


def source_channels(source: str) -> int:
    """Channel count the source reports, so a stereo line input is not mixed down."""
    try:
        sources = list_sources()
    except CaptureError as err:
        log.warning("Could not read the channel count (%s); assuming mono", err)
        return 1
    for name, channels in sources:
        if name == source:
            return channels
    log.warning("Source %s is not in the capture list; assuming mono", source)
    return 1


def parec_command(cfg: Config, source: str, channels: int = 1) -> list[str]:
    return [
        "parec",
        f"--device={source}",
        "--format=s16le",
        f"--rate={cfg.sample_rate}",
        f"--channels={channels}",
        "--latency-msec=200",
        "--raw",
    ]


def chunk_level(data: bytes) -> float:
    """dBFS of one s16le chunk."""
    samples = array.array("h")
    samples.frombytes(data)
    if not samples:
        return -120.0
    rms = math.sqrt(sum(s * s for s in samples) / len(samples))
    return rms_to_dbfs(rms)


def chunk_peak(data: bytes) -> int:
    """The largest absolute 16-bit sample in a chunk, for level and peak tracking."""
    samples = array.array("h")
    samples.frombytes(data[: len(data) - (len(data) % 2)])
    if not samples:
        return 0
    return max(max(samples), -min(samples))


async def audio_reader(
    cfg: Config,
    source: str,
    queue: "asyncio.Queue[bytes]",
    channels: int = 1,
    tap: Callable[[bytes], Awaitable[None]] | None = None,
) -> None:
    """Stream capture chunks into the queue for as long as the add-on runs.

    `tap` sees every chunk as well - that is how a rip records the same audio the listener
    hears, from a single `parec`, because a second reader on the same USB device cannot be
    relied on to return sound.
    """
    command = parec_command(cfg, source, channels)
    log.debug("Starting capture: %s", " ".join(command))
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    assert proc.stdout is not None
    chunk_bytes = int(cfg.sample_rate * 2 * channels * CHUNK_SECONDS)
    try:
        while not stop.is_set():
            data = await asyncio.to_thread(proc.stdout.read, chunk_bytes)
            if not data:
                stderr = b""
                if proc.stderr is not None:
                    stderr = proc.stderr.read() or b""
                raise CaptureError(
                    "capture stream ended: " + stderr.decode("utf-8", "replace").strip()
                )
            chunk = data[: len(data) - (len(data) % 2)]
            await queue.put(chunk)
            if tap is not None:
                await tap(chunk)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


async def supervised_entity_is_on(session: aiohttp.ClientSession, entity_id: str) -> bool:
    """Read an entity state through the supervisor's Home Assistant API proxy."""
    token = os.environ.get("SUPERVISOR_TOKEN", "")
    if not token:
        raise GateUnavailable("no SUPERVISOR_TOKEN in this container")
    url = f"http://supervisor/core/api/states/{entity_id}"
    async with session.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=aiohttp.ClientTimeout(total=10),
    ) as response:
        if response.status in (401, 403):
            raise GateUnavailable(f"HTTP {response.status}")
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        payload = await response.json()
    return payload.get("state") == "on"


async def watch_active_entity(
    cfg: Config, session: aiohttp.ClientSession, flag: dict[str, bool]
) -> None:
    """Keep flag['active'] in step with the configured entity."""
    failures = 0
    while not stop.is_set():
        try:
            flag["active"] = await supervised_entity_is_on(session, cfg.active_entity)
            failures = 0
        except GateUnavailable as err:
            log.error(
                "Cannot read %s (%s); listening continuously instead. The entity gate "
                "needs homeassistant_api: true in the add-on configuration.",
                cfg.active_entity,
                err,
            )
            flag["active"] = True
            return
        except Exception as err:  # noqa: BLE001 - fail open rather than stop listening
            failures += 1
            if failures == 1 or failures % 60 == 0:
                log.warning("Active entity check failed (%s); assuming active", err)
            flag["active"] = True
        await asyncio.sleep(ACTIVE_CHECK_SECONDS)


def write_wav(pcm: bytes, sample_rate: int, channels: int = 1) -> str:
    """Write raw s16le PCM to a temporary WAV file for shazamio."""
    handle = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    path = handle.name
    handle.close()
    with wave.open(path, "wb") as wav:
        wav.setnchannels(channels)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        wav.writeframes(pcm)
    return path


async def cache_artwork(
    session: aiohttp.ClientSession, url: str, device_name: str
) -> str:
    """Copy the cover into Home Assistant's www folder and return a local URL.

    Dashboards served to Cast displays or restricted networks cannot always reach
    Apple's image host, and its URLs expire, so the artwork is stored next to the
    configuration and served from the same origin as the dashboard.
    """
    if not url:
        return ""
    try:
        ARTWORK_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as err:
        log.warning("Artwork folder unavailable (%s); publishing the remote URL", err)
        return url

    # Every cover gets its own file name. A query string is not enough to shake a Cast
    # display loose of a /local URL it has already fetched (the folder is served with a
    # 31 day cache, 404s included); a path it has never seen cannot be in any cache. The
    # fixed name is still written for anything that points at it.
    stamp = int(time.time())
    versioned = ARTWORK_DIR / f"{device_name}-{stamp}.jpg"
    target = ARTWORK_DIR / f"{device_name}.jpg"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as response:
            response.raise_for_status()
            data = await response.read()
        if not data:
            raise ValueError("empty response")
        temporary = versioned.with_suffix(".tmp")
        temporary.write_bytes(data)
        os.replace(temporary, versioned)
        try:
            shutil.copyfile(versioned, target)
        except OSError as err:
            log.debug("Could not refresh %s (%s)", target.name, err)
    except Exception as err:  # noqa: BLE001 - the remote URL is an acceptable fallback
        log.warning("Could not cache artwork (%s); publishing the remote URL", err)
        return url

    log.info("Cached artwork for %s (%d bytes) as %s", device_name, len(data), versioned.name)
    for stale in sorted(ARTWORK_DIR.glob(f"{device_name}-*.jpg"))[:-ARTWORK_KEEP]:
        try:
            stale.unlink()
        except OSError:
            pass
    return f"/local/vinyl-art/{versioned.name}"


def parse_track(response: dict[str, Any], device_name: str) -> dict[str, Any] | None:
    """Turn a Shazam response into the attributes we publish."""
    track = response.get("track")
    if not track:
        return None
    metadata: dict[str, str] = {}
    for section in track.get("sections") or []:
        if section.get("type") != "SONG":
            continue
        for item in section.get("metadata") or []:
            key = str(item.get("title") or "").lower()
            if key in {"album", "released", "genre"}:
                metadata[key] = str(item.get("text") or "")
    images = track.get("images") or {}
    artist = track.get("subtitle") or ""
    title = track.get("title") or ""
    return {
        "display": f"{artist} - {title}" if artist and title else (title or artist or "Unknown track"),
        "title": title,
        "unknown_track": False,
        "artist": artist,
        "album": metadata.get("album") or "",
        "year": metadata.get("released") or "",
        "genre": metadata.get("genre") or "",
        "artwork": images.get("coverart") or images.get("background") or "",
        "artwork_remote": "",
        "url": track.get("url") or "",
        "source": device_name,
        "identified_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }


def load_album_store() -> dict[str, dict[str, str]]:
    """Songs seen before, with the album first matched for each."""
    try:
        data = json.loads(ALBUM_STORE.read_text())
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def save_album_store(store: dict[str, dict[str, str]]) -> None:
    try:
        ALBUM_STORE.write_text(json.dumps(store, indent=1, sort_keys=True))
    except OSError as err:
        log.warning("Could not save the album memory (%s); it will be rebuilt", err)


def apply_album_preference(
    track: dict[str, Any], store: dict[str, dict[str, str]], keep_first: bool
) -> bool:
    """Pin a song to the album it was first matched with.

    Shazam answers with whichever release its catalogue prefers, so the same track can
    jump between the album and a compilation between lookups. Remember the first album
    per artist+title (persisted in the add-on data folder) and reuse it.
    """
    key = f"{track['artist'].strip().casefold()}|{track['title'].strip().casefold()}"
    remembered = store.get(key)
    if remembered is None:
        store[key] = {"album": track["album"], "year": track["year"]}
        return False
    if not keep_first:
        store[key] = {"album": track["album"], "year": track["year"]}
        return False
    if remembered.get("album") and remembered["album"] != track["album"]:
        previous = track["album"]
        track["album"] = remembered["album"]
        track["year"] = remembered.get("year") or track["year"]
        log.debug("Kept album %r instead of %r", track["album"], previous)
        return True
    return False


def idle_track(device_name: str) -> dict[str, Any]:
    # No display text at all: "Nothing playing" is a name for a record that is not there,
    # and the panel hides its pill rather than print it. The `playing` binary sensor is
    # what says whether the turntable is running.
    return {
        "display": "",
        "title": "",
        "artist": "",
        "album": "",
        "year": "",
        "genre": "",
        "artwork": "",
        "artwork_remote": "",
        "url": "",
        "source": device_name,
        "identified_at": "",
        "unknown_track": False,
    }


def unidentified_track(previous: dict[str, Any]) -> dict[str, Any]:
    """What to publish when a clip above the gate was not recognised.

    Nothing has been quiet for `silence_clear_seconds`, so the record on the platter has
    not been changed and the artist, album and cover from the last identified track still
    describe it. Only the track name is dropped: the sleeve is known, the song on it is
    not, and naming the previous song would be a guess that the card states as fact.
    """
    artist = previous.get("artist") or ""
    album = previous.get("album") or ""
    if artist and album:
        display = f"{artist} - {album}"
    else:
        display = artist or album or ""
    return {
        **previous,
        "display": display,
        "title": "",
        "url": "",
        "unknown_track": True,
    }


def track_identity(track: dict[str, Any]) -> tuple[str, str]:
    """The pair that decides whether two answers describe the same song."""
    return (
        str(track.get("artist") or "").strip().casefold(),
        str(track.get("title") or "").strip().casefold(),
    )


def agreed_track(
    recent: "collections.deque[tuple[float, tuple[str, str], dict[str, Any]]]",
    displayed: tuple[str, str] | None,
    now: float,
) -> tuple[tuple[str, str], dict[str, Any]] | None:
    """An answer heard twice recently that is not what the display already shows.

    Shazam names the same audio differently from one lookup to the next - the single,
    a deluxe re-release, a compilation - so one answer is no evidence that the track
    changed, and the first answer of a side is not automatically the truth either.
    Two sightings inside `TRACK_CONFIRM_SECONDS` are evidence; the newest of them is
    the one worth publishing.
    """
    fresh = [key for stamp, key, _ in recent if now - stamp <= TRACK_CONFIRM_SECONDS]
    sightings = collections.Counter(fresh)
    for stamp, key, track in reversed(recent):
        if now - stamp > TRACK_CONFIRM_SECONDS:
            continue
        if sightings[key] >= TRACK_CONFIRM_VOTES and key != displayed:
            return key, track
    return None


def device_block(cfg: Config) -> dict[str, Any]:
    return {
        "identifiers": [f"vinyl_ear_{cfg.device_name}"],
        "name": f"Vinyl Ear ({cfg.device_name})",
        "manufacturer": "Vinyl Ear",
        "model": "Shazam room listener",
        "sw_version": "0.1.0",
    }


def publish(client: mqtt.Client, topic: str, payload: str, retain: bool = True) -> None:
    info = client.publish(topic, payload, retain=retain)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        log.warning("MQTT publish to %s failed (rc=%s)", topic, info.rc)


def connect_mqtt(cfg: Config) -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"vinyl-ear-{cfg.device_name}",
    )
    if cfg.mqtt_user:
        client.username_pw_set(cfg.mqtt_user, cfg.mqtt_password or None)
    client.will_set(f"{cfg.base_topic}/availability", payload="offline", retain=True)
    client.connect(cfg.mqtt_host, cfg.mqtt_port, keepalive=60)
    client.loop_start()
    log.info("Connected to MQTT broker %s:%s", cfg.mqtt_host, cfg.mqtt_port)
    return client


def publish_discovery(client: mqtt.Client, cfg: Config) -> None:
    prefix = cfg.discovery_prefix
    state_topic = f"{cfg.base_topic}/state"
    sensor_topic = f"{prefix}/sensor/vinyl_ear_{cfg.device_name}/now_playing/config"
    binary_topic = f"{prefix}/binary_sensor/vinyl_ear_{cfg.device_name}/audio/config"
    switch_topic = f"{prefix}/switch/vinyl_ear_{cfg.device_name}/rip/config"
    # `has_entity_name` is on by default for MQTT discovery when a device is set, so the
    # entity ids come out as sensor.<device>_now_playing / binary_sensor.<device>_audio_detected.
    sensor = {
        "name": "Now playing",
        "has_entity_name": True,
        "unique_id": f"vinyl_ear_{cfg.device_name}_now_playing",
        "state_topic": state_topic,
        "value_template": "{{ value_json.display }}",
        "json_attributes_topic": state_topic,
        "availability_topic": f"{cfg.base_topic}/availability",
        "icon": "mdi:record-player",
        "device": device_block(cfg),
    }
    binary = {
        "name": "Audio detected",
        "has_entity_name": True,
        "unique_id": f"vinyl_ear_{cfg.device_name}_audio",
        "state_topic": f"{cfg.base_topic}/playing",
        "availability_topic": f"{cfg.base_topic}/availability",
        "device_class": "sound",
        "icon": "mdi:music-note",
        "device": device_block(cfg),
    }
    switch = {
        "name": "Rip record",
        "has_entity_name": True,
        "unique_id": f"vinyl_ear_{cfg.device_name}_rip",
        "state_topic": f"{cfg.base_topic}/rip/state",
        "command_topic": f"{cfg.base_topic}/rip/set",
        "payload_on": "ON",
        "payload_off": "OFF",
        "json_attributes_topic": f"{cfg.base_topic}/rip",
        "availability_topic": f"{cfg.base_topic}/availability",
        "icon": "mdi:album",
        "device": device_block(cfg),
    }
    publish(client, sensor_topic, json.dumps(sensor))
    publish(client, binary_topic, json.dumps(binary))
    publish(client, switch_topic, json.dumps(switch))
    log.info(
        "Published MQTT discovery for device 'Vinyl Ear (%s)' — now_playing + audio_detected + rip",
        cfg.device_name,
    )


async def recognise(
    shazam: Shazam, cfg: Config, clip: bytes, channels: int = 1
) -> dict[str, Any] | None:
    """Ask Shazam what the clip contains."""
    path = write_wav(clip, cfg.sample_rate, channels)
    recognise_fn = getattr(shazam, "recognize", None) or getattr(shazam, "recognize_song")
    try:
        response = await recognise_fn(path)
    except Exception as err:  # noqa: BLE001 - a failed lookup must not kill the loop
        log.warning("Shazam lookup failed: %s", err)
        return None
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
    return parse_track(response or {}, cfg.device_name)


def note(status: dict[str, Any], text: str) -> None:
    """Add one line to the event log the web UI shows."""
    events = status.setdefault("events", [])
    events.append((time.strftime("%H:%M:%S"), text[:160]))
    del events[:-40]


def pactl(*args: str, timeout: float = 10.0) -> str | None:
    """Run `pactl` (PULSE_SERVER comes from run.sh) and return its stdout, or None."""
    try:
        result = subprocess.run(
            ["pactl", *args], capture_output=True, text=True, timeout=timeout
        )
    except (OSError, subprocess.SubprocessError) as err:
        log.warning("pactl %s could not run (%s)", " ".join(args), err)
        return None
    if result.returncode != 0:
        log.warning(
            "pactl %s failed: %s", " ".join(args), (result.stderr or "").strip()[:160]
        )
        return None
    return result.stdout


def source_volume(source: str) -> float | None:
    """The capture source's current volume in percent, or None when it cannot be read."""
    output = pactl("get-source-volume", source)
    if not output:
        return None
    match = re.search(r"(\d+)%", output)
    return float(match.group(1)) if match else None


def set_source_volume(source: str, percent: float) -> bool:
    """Set the capture source's volume; returns whether pactl accepted it."""
    return pactl("set-source-volume", source, f"{percent:.0f}%") is not None


def percent_to_db(percent: float) -> float:
    """PulseAudio's software volume is cubic: dB = 60 * log10(percent / 100)."""
    return 60.0 * math.log10(max(percent, 0.1) / 100.0)


def volume_for_delta(percent: float, delta_db: float) -> float:
    """The volume percent that moves the source by `delta_db`, clamped to a sane range."""
    linear = (max(percent, 0.1) / 100.0) ** 3 * 10.0 ** (delta_db / 20.0)
    return max(GAIN_MIN_VOLUME, min(GAIN_MAX_VOLUME, 100.0 * linear ** (1.0 / 3.0)))


def calibrate_gain(
    source: str, measured_dbfs: float, status: dict[str, Any]
) -> dict[str, Any] | None:
    """Aim the loudest sample of the record at GAIN_TARGET_DBFS by moving the capture gain.

    Blocking (two pactl calls), so callers run it in a thread. The returned dict says what
    happened; `change_db` is the dB the source actually moved, which is what a pre-roll has
    to be scaled by so a rip does not change level part way through.
    """
    if measured_dbfs <= GAIN_SILENT_DBFS:
        note(
            status,
            "Gain: nothing loud enough to measure (%.0f dBFS) — is the record playing?"
            % measured_dbfs,
        )
        return None
    volume = source_volume(source)
    if volume is None:
        note(status, "Gain: cannot read the source volume — is pactl available?")
        return None
    delta = GAIN_TARGET_DBFS - measured_dbfs
    if abs(delta) < GAIN_DEADBAND_DB:
        status["gain"] = {
            "measured_dbfs": round(measured_dbfs, 1),
            "volume": round(volume),
            "change_db": 0.0,
            "target_dbfs": GAIN_TARGET_DBFS,
            "at": int(time.time()),
        }
        note(
            status,
            "Gain: %.0f dBFS peak at %.0f%% is already on target" % (measured_dbfs, volume),
        )
        return status["gain"]
    wanted = volume_for_delta(volume, delta)
    if not set_source_volume(source, wanted):
        note(status, "Gain: could not set the source volume to %.0f%%" % wanted)
        return None
    applied = percent_to_db(wanted) - percent_to_db(volume)
    status["gain"] = {
        "measured_dbfs": round(measured_dbfs, 1),
        "volume": round(wanted),
        "volume_before": round(volume),
        "change_db": round(applied, 1),
        "target_dbfs": GAIN_TARGET_DBFS,
        "at": int(time.time()),
    }
    note(
        status,
        "Gain: %.0f dBFS peak moved to volume %.0f%% (was %.0f%%, %+.1f dB)"
        % (measured_dbfs, wanted, volume, applied),
    )
    log.info(
        "Gain: %.0f dBFS peak at %.0f%% → %.0f%% (%+.1f dB), aiming for %.0f dBFS",
        measured_dbfs,
        volume,
        wanted,
        applied,
        GAIN_TARGET_DBFS,
    )
    return status["gain"]


def scale_pcm(data: bytes, factor: float) -> bytes:
    """Multiply 16-bit samples by `factor` with clipping — pre-roll captured before a gain move."""
    if abs(factor - 1.0) < 0.02:
        return data
    samples = array.array("h")
    samples.frombytes(data[: len(data) - (len(data) % 2)])
    for index, value in enumerate(samples):
        scaled = int(value * factor)
        if scaled > 32767:
            samples[index] = 32767
        elif scaled < -32768:
            samples[index] = -32768
        else:
            samples[index] = scaled
    return samples.tobytes()


def find_track_gaps(
    levels: list[float], chunk_seconds: float, duration: float
) -> list[float]:
    """Where one track ends and the next begins, from the chunk levels of a side.

    A gap is a stretch at least TRACK_GAP_DEPTH_DB below the side's own loud level (its
    90th percentile, so a quiet passage cannot set the bar) lasting TRACK_GAP_SECONDS. The
    cut goes in the middle of the gap, and cuts that would leave a piece shorter than
    TRACK_MIN_SECONDS are dropped, which also throws away lead-in and run-out gaps.
    """
    if len(levels) < 5 or chunk_seconds <= 0:
        return []
    ordered = sorted(levels)
    loud = ordered[min(len(ordered) - 1, int(len(ordered) * 0.9))]
    floor = loud - TRACK_GAP_DEPTH_DB
    runs: list[tuple[int, int]] = []
    start: int | None = None
    for index, level in enumerate(levels):
        if level <= floor:
            if start is None:
                start = index
        elif start is not None:
            runs.append((start, index))
            start = None
    if start is not None:
        runs.append((start, len(levels)))
    cuts: list[float] = []
    for begin, end in runs:
        if (end - begin) * chunk_seconds < TRACK_GAP_SECONDS:
            continue
        cut = (begin + end) / 2.0 * chunk_seconds
        if cut < TRACK_MIN_SECONDS or cut > duration - TRACK_MIN_SECONDS:
            continue
        cuts.append(round(cut, 3))
    return cuts


def split_rip(path: Path, cuts: list[float], duration: float) -> list[Path]:
    """Cut a finished side at `cuts` with a stream copy, keeping the whole-side file."""
    if not cuts or len(cuts) > TRACK_MAX_COUNT:
        return []
    bounds = [0.0, *cuts, duration]
    created: list[Path] = []
    for index in range(len(bounds) - 1):
        start, end = bounds[index], bounds[index + 1]
        if end - start < TRACK_MIN_SECONDS:
            continue
        target = path.with_name(f"{path.stem}-{index + 1:02d}{path.suffix}")
        try:
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-nostdin",
                    "-y",
                    "-loglevel",
                    "error",
                    "-ss",
                    f"{start:.3f}",
                    "-to",
                    f"{end:.3f}",
                    "-i",
                    str(path),
                    "-c",
                    "copy",
                    str(target),
                ],
                capture_output=True,
                text=True,
                timeout=600,
            )
        except (OSError, subprocess.SubprocessError) as err:
            log.warning("Could not cut %s (%s)", target.name, err)
            break
        if result.returncode != 0 or not target.exists():
            log.warning(
                "Could not cut %s (%s)", target.name, (result.stderr or "").strip()[:200]
            )
            break
        created.append(target)
    return created


class RipRecorder:
    """Capture every chunk to disk, in the source's own channel count and rate.

    The tape is the same audio the listener hears - one `parec`, no second reader - so a
    rip and Shazam can run at once. WAV is written as it goes; `finish()` closes it and,
    when `rip_format` is flac, encodes it losslessly with the ffmpeg already in the image.
    """

    def __init__(self, cfg: Config, channels: int) -> None:
        self.cfg = cfg
        self.channels = channels
        self.recording = False
        self.frames = 0
        self.peak = 0
        self.file: Path | None = None
        # One peak dBFS per chunk, in order: the timeline `find_track_gaps()` splits on.
        self.levels: list[float] = []
        self.chunk_seconds = 0.0
        self.wav_path: Path | None = None
        self._wav: "wave.Wave_write | None" = None

    @property
    def seconds(self) -> float:
        return self.frames / float(self.cfg.sample_rate)

    @property
    def peak_dbfs(self) -> float:
        return rms_to_dbfs(self.peak)

    def start(self) -> None:
        directory = Path(self.cfg.rip_dir).expanduser()
        directory.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        self.wav_path = directory / f"{self.cfg.device_name}-{stamp}.wav"
        handle = wave.open(str(self.wav_path), "wb")
        handle.setnchannels(self.channels)
        handle.setsampwidth(2)
        handle.setframerate(self.cfg.sample_rate)
        self._wav = handle
        self.recording = True
        self.frames = 0
        self.peak = 0
        self.levels = []
        self.chunk_seconds = 0.0

    def write(self, data: bytes) -> None:
        if self._wav is None:
            return
        try:
            self._wav.writeframes(data)
        except OSError as err:
            log.error("Rip write failed (%s); closing the file", err)
            self.finish()
            return
        self.frames += len(data) // (2 * self.channels)
        if len(data) >= 2:
            level = chunk_peak(data)
            if level > self.peak:
                self.peak = level
            if not self.chunk_seconds:
                self.chunk_seconds = len(data) / float(
                    2 * self.channels * self.cfg.sample_rate
                )
            self.levels.append(round(rms_to_dbfs(level), 1))

    def finish(self) -> Path | None:
        """Close the file, encode to FLAC when configured, and return what was kept."""
        handle, source = self._wav, self.wav_path
        self._wav = None
        self.recording = False
        if handle is None or source is None:
            return None
        handle.close()
        self.file = source
        if self.cfg.rip_format == "flac":
            target = source.with_suffix(".flac")
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-nostdin",
                    "-y",
                    "-loglevel",
                    "error",
                    "-i",
                    str(source),
                    "-c:a",
                    "flac",
                    "-sample_fmt",
                    "s16",
                    str(target),
                ],
                capture_output=True,
                text=True,
                timeout=900,
            )
            if result.returncode == 0 and target.exists():
                try:
                    source.unlink()
                except OSError:
                    pass
                self.file = target
            else:
                log.warning(
                    "Could not encode %s to FLAC (%s); keeping the WAV",
                    source.name,
                    (result.stderr or "").strip()[:200],
                )
        return self.file


async def rip_worker(
    cfg: Config,
    source: str,
    channels: int,
    tape: "asyncio.Queue[bytes]",
    commands: "asyncio.Queue[str]",
    client: mqtt.Client,
    status: dict[str, Any],
) -> None:
    """Record while a rip is running, on MQTT, web or auto-rip commands.

    The tape runs whether or not a rip does, so the last RIP_PREROLL_SECONDS of audio is
    kept here: an auto start writes it into the file, which is what makes "the needle
    dropped" lose nothing. While idle the tape is drained and thrown away.
    """
    state_topic = f"{cfg.base_topic}/rip/state"
    info_topic = f"{cfg.base_topic}/rip"
    recorder = RipRecorder(cfg, channels)
    last_report = 0.0
    preroll_budget = int(RIP_PREROLL_SECONDS * cfg.bytes_per_second(channels))
    preroll: collections.deque[bytes] = collections.deque()
    preroll_peaks: collections.deque[int] = collections.deque()
    preroll_held = 0

    def report(retain: bool = False) -> None:
        current = recorder.wav_path if recorder.recording else recorder.file
        payload = {
            "state": "recording" if recorder.recording else "idle",
            "file": str(current) if current else "",
            "seconds": round(recorder.seconds),
            "peak_dbfs": round(recorder.peak_dbfs) if recorder.peak else -120,
            "clipped": recorder.peak >= 32767,
            "channels": channels,
            "sample_rate": cfg.sample_rate,
            "format": cfg.rip_format,
        }
        status["rip"] = payload
        publish(client, state_topic, "ON" if recorder.recording else "OFF", retain=retain)
        publish(client, info_topic, json.dumps(payload), retain=retain)

    def remember(data: bytes) -> None:
        """Keep the last RIP_PREROLL_SECONDS of audio, and each chunk's peak with it."""
        nonlocal preroll_held
        preroll.append(data)
        preroll_peaks.append(chunk_peak(data))
        preroll_held += len(data)
        while preroll_held > preroll_budget and len(preroll) > 1:
            preroll_held -= len(preroll.popleft())
            preroll_peaks.popleft()

    async def measure_tape(seconds: float) -> float | None:
        """Peak dBFS of the next `seconds` of tape, or None when nothing arrived."""
        deadline = time.monotonic() + seconds
        peak = 0
        heard = 0
        while time.monotonic() < deadline:
            try:
                data = tape.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.05)
                continue
            heard += 1
            peak = max(peak, chunk_peak(data))
        if not heard:
            return None
        return rms_to_dbfs(peak)

    async def gain_before_rip() -> float:
        """Set the gain from the audio already in the pre-roll; return the pre-roll scale."""
        if not preroll_peaks:
            return 1.0
        measured = rms_to_dbfs(max(preroll_peaks))
        plan = await asyncio.to_thread(calibrate_gain, source, measured, status)
        if not plan:
            return 1.0
        return 10.0 ** (float(plan.get("change_db") or 0.0) / 20.0)

    def start_rip(with_preroll: bool = False, factor: float = 1.0) -> None:
        """Open a new file; an auto start writes what was heard before the decision."""
        nonlocal last_report, preroll_held
        try:
            recorder.start()
        except OSError as err:
            log.error("Cannot write a rip to %s (%s)", cfg.rip_dir, err)
            note(status, "Cannot write a rip to %s (%s)" % (cfg.rip_dir, err))
            report(retain=True)
            return
        last_report = 0.0
        carried = 0
        if with_preroll:
            for data in list(preroll):
                recorder.write(scale_pcm(data, factor))
                carried += len(data)
            preroll.clear()
            preroll_peaks.clear()
            preroll_held = 0
        report(retain=True)
        note(
            status,
            "Rip started: %s (%d ch, %d Hz, %s%s)"
            % (
                recorder.wav_path.name,
                channels,
                cfg.sample_rate,
                cfg.rip_format,
                ", %.0fs of pre-roll" % (carried / cfg.bytes_per_second(channels))
                if carried
                else "",
            ),
        )
        log.info(
            "Ripping to %s (%d ch, %d Hz, %s)",
            recorder.wav_path,
            channels,
            cfg.sample_rate,
            cfg.rip_format,
        )
        if channels == 1:
            log.warning(
                "The source is mono, so this rip will be mono too; pick a "
                "source that reports '2ch' in `pactl list short sources` "
                "and set the 'source' option to it"
            )

    async def stop_rip() -> None:
        seconds = recorder.seconds
        peak = recorder.peak_dbfs
        clipped = recorder.peak >= 32767
        levels, chunk_seconds = list(recorder.levels), recorder.chunk_seconds
        path = recorder.finish()
        report(retain=True)
        if path is None:
            log.info("Rip stopped before anything was written")
            note(status, "Rip stopped before anything was written")
            return
        note(status, "Rip finished: %s (%.0fs, peak %.0f dBFS%s)" % (
            path.name, seconds, peak, ", CLIPPED" if clipped else ""))
        log.info(
            "Rip finished: %s (%.0fs, peak %.0f dBFS%s)",
            path,
            seconds,
            peak,
            ", CLIPPED - lower the capture gain" if clipped else "",
        )
        if not cfg.rip_split:
            return
        cuts = find_track_gaps(levels, chunk_seconds, seconds)
        if not cuts:
            note(status, "No track gaps found in %s — left it whole" % path.name)
            return
        created = await asyncio.to_thread(split_rip, path, cuts, seconds)
        if created:
            note(
                status,
                "Split %s into %d tracks: %s …"
                % (path.name, len(created), created[0].name),
            )
            log.info("Split %s into %d tracks", path.name, len(created))
        else:
            note(status, "Could not split %s; the whole side is kept" % path.name)

    report(retain=True)
    try:
        while not stop.is_set():
            try:
                while not commands.empty():
                    command = commands.get_nowait().strip().upper()
                    if command in {"START", "ON"} and not recorder.recording:
                        start_rip()
                    elif command == "AUTO" and not recorder.recording:
                        factor = await gain_before_rip() if cfg.rip_auto_gain else 1.0
                        start_rip(with_preroll=True, factor=factor)
                    elif command == "GAIN":
                        if recorder.recording:
                            note(status, "Gain: stop the rip first — measuring would cut it")
                        else:
                            measured = await measure_tape(GAIN_SAMPLE_SECONDS)
                            if measured is None:
                                note(
                                    status,
                                    "Gain: no audio arriving — is the source connected?",
                                )
                            else:
                                await asyncio.to_thread(
                                    calibrate_gain, source, measured, status
                                )
                    elif command in {"STOP", "OFF"} and recorder.recording:
                        await stop_rip()
                if recorder.recording and recorder.seconds >= cfg.rip_max_minutes * 60:
                    log.info("Rip reached the %d minute limit; stopping", cfg.rip_max_minutes)
                    note(status, "Rip reached the %d minute limit" % cfg.rip_max_minutes)
                    await stop_rip()
                try:
                    data = tape.get_nowait()
                except asyncio.QueueEmpty:
                    # Nothing to record: wait a moment so a STOP still gets served when
                    # the capture stream has gone quiet or away.
                    await asyncio.sleep(RIP_POLL_SECONDS)
                    continue
                remember(data)
                if not recorder.recording:
                    continue
                recorder.write(data)
                now = time.monotonic()
                if now - last_report >= RIP_REPORT_SECONDS:
                    last_report = now
                    report()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - a rip must not kill the listener
                log.exception("Rip worker error: %s", err)
                await asyncio.sleep(1)
    finally:
        if recorder.recording:
            log.info("Add-on stopping; closing the rip")
            path = recorder.finish()
            report(retain=True)
            if path is not None:
                note(status, "Add-on stopped; closed the rip as %s" % path.name)


async def run(cfg: Config, client: mqtt.Client) -> None:
    publish_discovery(client, cfg)
    publish(client, f"{cfg.base_topic}/availability", "online")

    state_topic = f"{cfg.base_topic}/state"
    playing_topic = f"{cfg.base_topic}/playing"

    shazam = Shazam()
    session = aiohttp.ClientSession()
    queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=240)
    tape: asyncio.Queue[bytes] = asyncio.Queue(maxsize=RIP_QUEUE_CHUNKS)
    commands: asyncio.Queue[str] = asyncio.Queue()
    active = {"active": True}
    watcher = (
        asyncio.create_task(watch_active_entity(cfg, session, active))
        if cfg.active_entity
        else None
    )

    source = await asyncio.to_thread(find_source, cfg)
    channels = await asyncio.to_thread(source_channels, source)
    bytes_per_second = cfg.bytes_per_second(channels)
    listen_bytes = int(bytes_per_second * cfg.listen_seconds)
    prebuffer_bytes = int(bytes_per_second * PREBUFFER_SECONDS)
    log.info(
        "Capturing %s at %d Hz, %d channel(s) (clip %ss, %s dBFS gate)",
        source,
        cfg.sample_rate,
        channels,
        cfg.listen_seconds,
        f"{cfg.noise_gate_db:.0f}",
    )

    loop = asyncio.get_running_loop()

    def on_message(_client: mqtt.Client, _data: Any, message: mqtt.MQTTMessage) -> None:
        payload = message.payload.decode("utf-8", "replace")
        loop.call_soon_threadsafe(commands.put_nowait, payload)

    client.on_message = on_message
    client.subscribe(f"{cfg.base_topic}/rip/set")

    async def tap(data: bytes) -> None:
        await tape.put(data)

    status: dict[str, Any] = {
        "version": VERSION,
        "device": cfg.device_name,
        "source": source,
        "channels": channels,
        "sample_rate": cfg.sample_rate,
        "gate": cfg.noise_gate_db,
        "threshold": cfg.noise_gate_db,
        "floor": None,
        "level": None,
        "levels": [],
        "playing": False,
        "failures": 0,
        "last_lookup": None,
        "last_answer": None,
        "track": None,
        "events": [],
        "gain": None,
        "auto": {"enabled": cfg.rip_auto, "holding": False},
        "rip": {"state": "idle", "file": "", "seconds": 0, "peak_dbfs": -120,
                "clipped": False, "channels": channels,
                "sample_rate": cfg.sample_rate, "format": cfg.rip_format},
    }

    reader = asyncio.create_task(audio_reader(cfg, source, queue, channels, tap))
    ripper = asyncio.create_task(
        rip_worker(cfg, source, channels, tape, commands, client, status)
    )
    server = asyncio.create_task(web_server(cfg, status, commands))

    prebuffer = bytearray()
    clip = bytearray()
    last_loud_at: float | None = None
    last_attempt_at = 0.0
    current_key: tuple[str, str] | None = None
    last_track: dict[str, Any] | None = None
    last_identified_at: float = time.monotonic()
    # Auto-rip bookkeeping: `auto_hold` keeps a hand-stopped rip from springing back up
    # while the same side is still playing, and the guard keeps the command queue quiet.
    was_recording = False
    auto_hold = False
    auto_owned = False
    last_auto_command = 0.0
    # When the current uninterrupted stretch of sound began. Learning a floor while a
    # record is playing must not count the time since the add-on started as "nothing has
    # been recognised for five minutes".
    segment_started_at: float = last_identified_at
    # Level (dBFS) at which Shazam last named a track: the gate must stay well below it.
    music_level: float | None = None
    state_is_unidentified = False
    playing = False
    new_segment = True
    albums = load_album_store()
    levels: collections.deque[float] = collections.deque(maxlen=LEVELS_WINDOW)
    floor_levels: collections.deque[float] = collections.deque(maxlen=FLOOR_WINDOW)
    threshold = cfg.noise_gate_db
    failures = 0
    last_calibration = 0.0
    last_heartbeat = time.monotonic()
    recent: collections.deque[tuple[float, tuple[str, str], dict[str, Any]]] = (
        collections.deque(maxlen=TRACK_CONFIRM_LOOKBACK)
    )

    async def commit(key: tuple[str, str], track: dict[str, Any]) -> None:
        """Publish a track the listener is confident about."""
        nonlocal current_key, last_track, state_is_unidentified, last_identified_at
        nonlocal music_level
        current_key = key
        last_identified_at = now
        # Remember how loud the record that Shazam just named actually was, so the noise
        # gate cannot be learned up into the music it is supposed to be listening for.
        music_level = statistics.median(levels) if levels else None
        state_is_unidentified = False
        if apply_album_preference(track, albums, cfg.keep_first_album):
            log.info("Song already known, keeping the first album: %s", track["album"])
        save_album_store(albums)
        remote_artwork = track["artwork"]
        track["artwork"] = await cache_artwork(session, remote_artwork, cfg.device_name)
        track["artwork_remote"] = remote_artwork
        last_track = track
        status["track"] = track
        status["last_answer"] = track.get("display") or track.get("title") or ""
        note(status, "Now playing: %s - %s (%s)" % (
            track["artist"] or "?", track["title"] or "?", track["album"] or "unknown album"))
        publish(client, state_topic, json.dumps(track))
        log.info(
            "Now playing: %s - %s (%s, %s)",
            track["artist"] or "?",
            track["title"] or "?",
            track["album"] or "unknown album",
            track["year"] or "unknown year",
        )

    try:
        while not stop.is_set():
            data = await queue.get()
            now = time.monotonic()

            if not active["active"]:
                if playing:
                    playing = False
                    status["playing"] = False
                    publish(client, playing_topic, "OFF")
                if current_key is not None:
                    current_key = None
                    last_track = None
                    status["track"] = None
                    recent.clear()
                    state_is_unidentified = False
                    publish(client, state_topic, json.dumps(idle_track(cfg.device_name)))
                    log.info("%s is off — cleared the now playing state", cfg.active_entity)
                    note(status, "%s is off — cleared the now playing state"
                         % cfg.active_entity)
                clip = bytearray()
                last_loud_at = now
                continue

            level = chunk_level(data)
            levels.append(level)
            floor_levels.append(level)
            status["level"] = round(level, 1)
            status["levels"] = (status["levels"] + [round(level, 1)])[-WEB_LEVELS:]
            status["failures"] = failures
            status["threshold"] = threshold

            if now - last_calibration >= CALIBRATION_SECONDS:
                last_calibration = now
                calibrated = calibrate_threshold(
                    floor_levels,
                    cfg,
                    now - max(last_identified_at, segment_started_at),
                    threshold,
                    failures,
                    music_level,
                )
                if calibrated is not None:
                    floor, wanted = calibrated
                    status["floor"] = round(floor, 1)
                    if abs(wanted - threshold) >= 1.0:
                        threshold = wanted
                        log.info(
                            "Room noise floor ~%.0f dBFS (steady), "
                            "activity threshold now %.0f dBFS",
                            floor,
                            threshold,
                        )
                        note(status, "Gate moved to %.0f dBFS (floor %.0f dBFS)" % (
                            threshold, floor))

            gap = last_loud_at is None or (now - last_loud_at) >= GAP_SECONDS

            if level >= threshold:
                if gap:
                    clip = bytearray(prebuffer) + data
                    new_segment = True
                    failures = 0
                    segment_started_at = now
                    log.debug(
                        "Audio at %.0f dBFS (threshold %.0f) — starting a clip",
                        level,
                        threshold,
                    )
                elif len(clip) < listen_bytes * 3:
                    clip.extend(data)
                last_loud_at = now
                if not playing:
                    playing = True
                    status["playing"] = True
                    publish(client, playing_topic, "ON")
            else:
                if last_loud_at is None:
                    last_loud_at = now - SILENCE_DEBOUNCE_SECONDS
                quiet_for = now - last_loud_at
                if playing and quiet_for >= SILENCE_DEBOUNCE_SECONDS:
                    playing = False
                    status["playing"] = False
                    publish(client, playing_topic, "OFF")
                if quiet_for >= GATE_HOLD_SECONDS:
                    # A real gap, not the quiet bar of the track that is playing.
                    clip = bytearray()
                elif len(clip) < listen_bytes * 3:
                    clip.extend(data)

            # Auto-rip: start on the needle drop, stop when the side has run out. The
            # worker does the recording; this only decides when to ask it to.
            recording = status["rip"].get("state") == "recording"
            quiet_for = (now - last_loud_at) if last_loud_at is not None else math.inf
            if cfg.rip_auto:
                if recording and auto_owned:
                    if quiet_for >= cfg.rip_auto_stop_seconds:
                        commands.put_nowait("OFF")
                        last_auto_command = now
                        auto_owned = False
                        note(
                            status,
                            "Auto-rip: quiet for %ds — stopping"
                            % cfg.rip_auto_stop_seconds,
                        )
                elif recording:
                    # Started from the switch or the page: not ours to stop.
                    auto_hold = False
                elif was_recording and auto_owned:
                    # Our rip ended while the input was still loud — a hand stop, or the
                    # time limit. Do not spring back up until the side has actually ended.
                    auto_owned = False
                    if quiet_for < cfg.rip_auto_stop_seconds:
                        auto_hold = True
                        note(
                            status,
                            "Auto-rip: stopped by hand — waiting for the side to end",
                        )
                elif auto_hold:
                    if quiet_for >= cfg.rip_auto_stop_seconds:
                        auto_hold = False
                        note(status, "Auto-rip: quiet — re-armed")
                elif (
                    quiet_for <= GATE_HOLD_SECONDS
                    and now - segment_started_at >= AUTO_START_SECONDS
                    and now - last_auto_command >= AUTO_COMMAND_GUARD_SECONDS
                ):
                    commands.put_nowait("AUTO")
                    last_auto_command = now
                    auto_owned = True
                    note(status, "Auto-rip: sound for %ds — starting" % AUTO_START_SECONDS)
            was_recording = recording
            status["auto"] = {"enabled": cfg.rip_auto, "holding": auto_hold}

            prebuffer.extend(data)
            if len(prebuffer) > prebuffer_bytes:
                del prebuffer[: len(prebuffer) - prebuffer_bytes]

            if now - last_heartbeat >= HEARTBEAT_SECONDS:
                last_heartbeat = now
                log.info(
                    "Room level %.0f dBFS (threshold %.0f, %d%% of the last 30s above), "
                    "playing=%s, failures=%d, holding=%s, last lookup %ds ago",
                    level,
                    threshold,
                    100 * sum(1 for value in levels if value >= threshold)
                    // max(len(levels), 1),
                    playing,
                    failures,
                    (last_track or {}).get("display") or "nothing",
                    int(now - last_attempt_at) if last_attempt_at else -1,
                )

            if current_key is not None and (now - last_loud_at) >= cfg.silence_clear_seconds:
                current_key = None
                last_track = None
                recent.clear()
                state_is_unidentified = False
                publish(client, state_topic, json.dumps(idle_track(cfg.device_name)))
                log.info(
                    "Silent for %ss — cleared the now playing state", cfg.silence_clear_seconds
                )
                status["track"] = None
                status["last_answer"] = None
                note(status, "Silent for %ss — cleared the now playing state"
                     % cfg.silence_clear_seconds)

            cooldown = (
                0.0
                if new_segment
                else min(cfg.poll_seconds * (2 ** min(failures, 3)), MAX_COOLDOWN_SECONDS)
            )
            if len(clip) >= listen_bytes and (now - last_attempt_at) >= cooldown:
                last_attempt_at = now
                new_segment = False
                track = await recognise(shazam, cfg, bytes(clip[:listen_bytes]), channels)
                status["last_lookup"] = time.time()
                clip = bytearray()
                last_loud_at = time.monotonic()
                while not queue.empty():
                    queue.get_nowait()
                prebuffer = bytearray()

                if track is None:
                    failures += 1
                    status["last_answer"] = None
                    note(status, "Heard audio (%.0f dBFS) but Shazam did not recognise it" % level)
                    log.info(
                        "Heard audio (%.0f dBFS) but Shazam did not recognise it", level
                    )
                    if cfg.blank_unknown_track and not state_is_unidentified:
                        # Nothing is named until Shazam answers again: the previous track
                        # name is a guess about what is on the platter now, so drop it and
                        # keep only the artist and sleeve that were already known.
                        previous = (
                            last_track
                            if last_track is not None
                            else idle_track(cfg.device_name)
                        )
                        publish(client, state_topic, json.dumps(unidentified_track(previous)))
                        state_is_unidentified = True
                        log.info(
                            "Unidentified track: keeping the album %r, blanking the track name",
                            previous.get("album") or "unknown",
                        )
                else:
                    key = track_identity(track)
                    failures = 0
                    if key == current_key:
                        if state_is_unidentified:
                            # Same sleeve as the display: put the name back.
                            await commit(key, track)
                        else:
                            log.debug("Same track still playing: %s - %s", *key)
                        recent.clear()
                    elif current_key is None:
                        # Nothing on the display, so a first answer beats a blank card;
                        # a later, different answer corrects it.
                        await commit(key, track)
                        recent.clear()
                    else:
                        recent.append((now, key, track))
                        agreed = agreed_track(recent, current_key, now)
                        if agreed is None:
                            log.debug(
                                "Heard %s - %s; waiting for a second look",
                                track["artist"] or "?",
                                track["title"] or "?",
                            )
                        else:
                            await commit(*agreed)
                            recent.clear()
    finally:
        reader.cancel()
        ripper.cancel()
        server.cancel()
        if watcher is not None:
            watcher.cancel()
        await session.close()


INDEX_HTML = (
    """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vinyl Ear</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; background: #0f1217; color: #e7eaf0;
         font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
  header { display: flex; gap: 16px; align-items: baseline; flex-wrap: wrap;
           padding: 14px 20px; background: #161b23; border-bottom: 1px solid #222a36; }
  header h1 { margin: 0; font-size: 1.15rem; letter-spacing: .02em; }
  .meta { font-size: .8rem; opacity: .6; }
  main { display: grid; gap: 16px; padding: 16px 20px 32px;
         grid-template-columns: repeat(auto-fit, minmax(330px, 1fr)); }
  section { background: #161b23; border: 1px solid #222a36; border-radius: 14px;
            padding: 16px; }
  h2 { margin: 0 0 12px; font-size: .78rem; text-transform: uppercase;
       letter-spacing: .1em; opacity: .55; }
  .track { display: flex; gap: 14px; align-items: center; min-height: 84px; }
  .cover { width: 84px; height: 84px; border-radius: 10px; object-fit: cover;
           background: #212936; flex: 0 0 auto; }
  .artist { font-size: 1.1rem; font-weight: 600; }
  .line { font-size: .9rem; opacity: .7; margin-top: 3px; }
  .meter { position: relative; height: 22px; margin: 14px 0 8px; border-radius: 7px;
           background: #0b0e13; overflow: hidden; }
  .fill { position: absolute; inset: 0 auto 0 0; width: 0;
          background: linear-gradient(90deg, #2e7d32, #9ccc65 55%, #fdd835 80%, #e53935);
          transition: width .12s linear; }
  .gate { position: absolute; top: 0; bottom: 0; width: 2px; background: #e53935; }
  .spark { display: flex; align-items: flex-end; gap: 2px; height: 46px; margin-top: 6px; }
  .spark span { flex: 1 1 auto; background: #3b5b8c; border-radius: 1px 1px 0 0;
                min-height: 1px; }
  .row { display: flex; justify-content: space-between; gap: 12px; font-size: .88rem;
         padding: 4px 0; border-bottom: 1px solid #1d2430; }
  .row:last-child { border-bottom: 0; }
  .row span:first-child { opacity: .6; }
  .pill { display: inline-block; padding: 2px 10px; border-radius: 999px; font-size: .78rem;
          background: #2a3446; }
  .pill.on { background: #1b5e20; }
  .pill.warn { background: #7f4f00; }
  button { font: inherit; border: 0; border-radius: 10px; padding: 9px 18px; cursor: pointer;
           background: #2b6cb0; color: #fff; }
  button.stop { background: #b02b2b; }
  button.quiet { background: #37474f; }
  .buttons { display: flex; gap: 10px; flex-wrap: wrap; }
  button:disabled { opacity: .45; cursor: default; }
  table { width: 100%; border-collapse: collapse; font-size: .85rem; margin-top: 12px; }
  th, td { text-align: left; padding: 6px 4px; border-bottom: 1px solid #1d2430; }
  th { font-weight: 500; opacity: .55; font-size: .78rem; }
  td.actions { text-align: right; white-space: nowrap; }
  a { color: #8ab4f8; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .del { background: none; color: #ef9a9a; padding: 2px 6px; }
  ul.events { list-style: none; margin: 0; padding: 0; font-size: .84rem;
              max-height: 240px; overflow-y: auto; }
  ul.events li { padding: 4px 0; border-bottom: 1px solid #1d2430; opacity: .85; }
  ul.events time { opacity: .5; margin-right: 8px; font-variant-numeric: tabular-nums; }
  .muted { opacity: .45; font-size: .85rem; }
</style>
</head>
"""

    """<body>
<header>
  <h1>Vinyl Ear</h1>
  <span class="meta" id="meta">connecting…</span>
</header>
<main>
  <section>
    <h2>Shazam detection</h2>
    <div class="track">
      <img class="cover" id="cover" alt="" hidden>
      <div>
        <div class="artist" id="artist">—</div>
        <div class="line" id="title"></div>
        <div class="line" id="album"></div>
      </div>
    </div>
    <div class="meter">
      <div class="fill" id="fill"></div>
      <div class="gate" id="gateMark"></div>
    </div>
    <div class="spark" id="spark"></div>
    <div class="row"><span>level</span><span id="level">—</span></div>
    <div class="row"><span>gate / measured floor</span><span id="gates">—</span></div>
    <div class="row"><span>audio above gate</span><span id="playing">—</span></div>
    <div class="row"><span>refused lookups</span><span id="failures">—</span></div>
    <div class="row"><span>last lookup</span><span id="lookup">—</span></div>
    <div class="row"><span>last answer</span><span id="answer">—</span></div>
  </section>
  <section>
    <h2>Rip</h2>
    <div class="row"><span>state</span><span><span class="pill" id="ripState">idle</span></span></div>
    <div class="row"><span>elapsed</span><span id="ripElapsed">—</span></div>
    <div class="row"><span>peak</span><span id="ripPeak">—</span></div>
    <div class="row"><span>file</span><span id="ripFile">—</span></div>
    <div class="row"><span>gain</span><span id="gain">—</span></div>
    <div class="row"><span>auto-rip</span><span id="auto">off</span></div>
    <p class="buttons">
      <button id="ripButton">Start a rip</button>
      <button id="gainButton" class="quiet">Set the gain</button>
    </p>
    <table>
      <thead><tr><th>recording</th><th>size</th><th></th></tr></thead>
      <tbody id="files"><tr><td colspan="3" class="muted">no rips yet</td></tr></tbody>
    </table>
  </section>
  <section>
    <h2>Event log</h2>
    <ul class="events" id="events"><li class="muted">nothing yet</li></ul>
  </section>
</main>
"""

    """<script>
const $ = (id) => document.getElementById(id);
const FLOOR_DB = -60;
let ripBusy = false;

function pct(value) {
  if (value === null || value === undefined) return 0;
  return Math.max(0, Math.min(100, ((value - FLOOR_DB) / -FLOOR_DB) * 100));
}
function db(value) {
  return value === null || value === undefined ? "—" : Math.round(value) + " dBFS";
}
function ago(seconds) {
  if (seconds === null || seconds === undefined) return "—";
  if (seconds < 90) return seconds + " s ago";
  if (seconds < 5400) return Math.round(seconds / 60) + " min ago";
  return (seconds / 3600).toFixed(1) + " h ago";
}
function clock(seconds) {
  const total = Math.max(0, Math.round(seconds || 0));
  return Math.floor(total / 60) + ":" + String(total % 60).padStart(2, "0");
}
function size(bytes) {
  return bytes >= 1048576
    ? (bytes / 1048576).toFixed(1) + " MB"
    : Math.round(bytes / 1024) + " kB";
}

function renderTrack(track) {
  const info = track || {};
  $("artist").textContent = info.artist || "—";
  $("title").textContent = info.title || "";
  $("album").textContent = [info.album, info.year].filter(Boolean).join(" · ");
  const cover = $("cover");
  if (info.artwork) {
    cover.src = info.artwork;
    cover.hidden = false;
  } else {
    cover.hidden = true;
    cover.removeAttribute("src");
  }
}

function renderMeter(status) {
  $("fill").style.width = pct(status.level) + "%";
  $("gateMark").style.left = pct(status.threshold) + "%";
  $("level").textContent = db(status.level);
  $("gates").textContent = db(status.threshold) + " / " + db(status.floor);
  $("playing").textContent = status.playing ? "yes — filling a clip" : "no";
  $("playing").style.color = status.playing ? "#9ccc65" : "";
  $("failures").textContent = String(status.failures || 0);
  $("lookup").textContent = ago(status.last_lookup_seconds);
  $("answer").textContent = status.last_answer || "—";
  const bars = (status.levels || []).map((value) => {
    const bar = document.createElement("span");
    bar.style.height = Math.max(2, pct(value)) + "%";
    bar.style.background = value >= status.threshold ? "#7cb342" : "#37507a";
    bar.title = db(value);
    return bar;
  });
  $("spark").replaceChildren(...bars);
}
function renderRip(status) {
  const rip = status.rip || {};
  const recording = rip.state === "recording";
  const pill = $("ripState");
  pill.textContent = rip.state || "idle";
  pill.className = "pill" + (recording ? " on" : "");
  $("ripElapsed").textContent = rip.seconds ? clock(rip.seconds) : "—";
  $("ripPeak").textContent = (rip.peak_dbfs || -120) > -120
    ? db(rip.peak_dbfs) + (rip.clipped ? " — clipping" : "")
    : "—";
  $("ripFile").textContent = rip.file ? rip.file.split("/").pop() : "—";
  const gain = status.gain || {};
  $("gain").textContent = gain.measured_dbfs === undefined || gain.measured_dbfs === null
    ? "—"
    : db(gain.measured_dbfs) + " peak → " + (gain.volume ?? "?") + "%"
      + (gain.change_db ? " (" + (gain.change_db > 0 ? "+" : "") + gain.change_db + " dB)" : "");
  const auto = status.auto || {};
  $("auto").textContent = !auto.enabled
    ? "off"
    : (auto.holding ? "armed, holding (stopped by hand)" : "armed");
  const button = $("ripButton");
  button.textContent = recording ? "Stop the rip" : "Start a rip";
  button.className = recording ? "stop" : "";
  button.disabled = ripBusy;
  $("gainButton").disabled = ripBusy || recording;

  const rows = (status.rips || []).map((file) => {
    const row = document.createElement("tr");
    const name = document.createElement("td");
    const link = document.createElement("a");
    link.href = "files/" + encodeURIComponent(file.name);
    link.textContent = file.name;
    name.append(link);
    const bytes = document.createElement("td");
    bytes.textContent = size(file.size);
    const actions = document.createElement("td");
    actions.className = "actions";
    const download = document.createElement("a");
    download.href = "files/" + encodeURIComponent(file.name) + "?download=1";
    download.textContent = "download";
    const remove = document.createElement("button");
    remove.className = "del";
    remove.textContent = "delete";
    remove.onclick = () => deleteRip(file.name);
    actions.append(download, " ", remove);
    row.append(name, bytes, actions);
    return row;
  });
  if (!rows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 3;
    cell.className = "muted";
    cell.textContent = "no rips in " + (status.rip_dir || "the rip folder");
    row.append(cell);
    rows.push(row);
  }
  $("files").replaceChildren(...rows);
}

function renderEvents(events) {
  const rows = (events || []).slice().reverse().map((event) => {
    const item = document.createElement("li");
    const stamp = document.createElement("time");
    stamp.textContent = event[0];
    item.append(stamp, document.createTextNode(event[1]));
    return item;
  });
  if (!rows.length) {
    const item = document.createElement("li");
    item.className = "muted";
    item.textContent = "nothing yet";
    rows.push(item);
  }
  $("events").replaceChildren(...rows);
}

function render(status) {
  $("meta").textContent = [
    "v" + status.version,
    status.device,
    (status.source || "").replace(/^alsa_(input|output)\\./, ""),
    status.sample_rate + " Hz",
    status.channels + (status.channels === 1 ? " channel" : " channels"),
  ].join(" · ");
  renderTrack(status.track);
  renderMeter(status);
  renderRip(status);
  renderEvents(status.events);
}

async function refresh() {
  try {
    const response = await fetch("api/status", { cache: "no-store" });
    if (!response.ok) throw new Error("HTTP " + response.status);
    render(await response.json());
  } catch (err) {
    $("meta").textContent = "the add-on is not answering (" + err.message + ")";
  }
}

async function ripAction(action) {
  ripBusy = true;
  $("ripButton").disabled = true;
  $("gainButton").disabled = true;
  try {
    const response = await fetch("api/rip", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: action }),
    });
    if (!response.ok) throw new Error("HTTP " + response.status);
  } catch (err) {
    $("meta").textContent = "the rip command failed (" + err.message + ")";
  }
  ripBusy = false;
  refresh();
}

async function deleteRip(name) {
  if (!confirm("Delete " + name + "?")) return;
  try {
    const response = await fetch("files/" + encodeURIComponent(name), { method: "DELETE" });
    if (!response.ok) throw new Error("HTTP " + response.status);
  } catch (err) {
    $("meta").textContent = "the delete failed (" + err.message + ")";
  }
  refresh();
}

$("ripButton").onclick = () => {
  ripAction($("ripState").textContent === "recording" ? "stop" : "start");
};

$("gainButton").onclick = () => ripAction("gain");

refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
"""
)


def rip_files(cfg: Config) -> list[dict[str, Any]]:
    """Finished rips in `rip_dir`, newest first."""
    directory = Path(cfg.rip_dir).expanduser()
    try:
        entries = list(directory.iterdir())
    except OSError:
        return []
    files: list[dict[str, Any]] = []
    for entry in entries:
        if entry.suffix.lower() not in {".flac", ".wav"}:
            continue
        try:
            info = entry.stat()
        except OSError:
            continue
        files.append(
            {"name": entry.name, "size": info.st_size, "modified": int(info.st_mtime)}
        )
    files.sort(key=lambda item: item["modified"], reverse=True)
    return files[:50]


def rip_path(cfg: Config, name: str) -> Path | None:
    """`name` resolved inside `rip_dir`, or None when it is not a finished rip there."""
    directory = Path(cfg.rip_dir).expanduser()
    try:
        resolved = (directory / name).resolve()
        if resolved.parent != directory.resolve():
            return None
    except OSError:
        return None
    if resolved.suffix.lower() not in {".flac", ".wav"} or not resolved.is_file():
        return None
    return resolved


async def web_server(
    cfg: Config, status: dict[str, Any], commands: "asyncio.Queue[str]"
) -> None:
    """The UI behind Home Assistant ingress: what the listener hears, and the ripper."""

    async def index(_request: web.Request) -> web.StreamResponse:
        return web.Response(
            text=INDEX_HTML,
            content_type="text/html",
            charset="utf-8",
            headers={"Cache-Control": "no-store"},
        )

    def recording_name() -> str:
        """The file the ripper is writing right now, or "" when nothing is being written."""
        rip = status.get("rip") or {}
        if rip.get("state") != "recording":
            return ""
        current = rip.get("file") or ""
        return Path(current).name if current else ""

    async def api_status(_request: web.Request) -> web.StreamResponse:
        payload: dict[str, Any] = dict(status)
        rips = await asyncio.to_thread(rip_files, cfg)
        mid_rip = recording_name()
        payload["rips"] = [item for item in rips if item["name"] != mid_rip]
        payload["rip_dir"] = cfg.rip_dir
        payload["now"] = time.time()
        looked_up = status.get("last_lookup")
        payload["last_lookup_seconds"] = (
            round(time.time() - looked_up) if looked_up else None
        )
        return web.json_response(payload)

    async def api_rip(request: web.Request) -> web.StreamResponse:
        try:
            body = await request.json()
        except (ValueError, aiohttp.ContentTypeError):
            body = {}
        action = str(body.get("action") or "").lower()
        if action == "start":
            await commands.put("ON")
        elif action == "stop":
            await commands.put("OFF")
        elif action == "gain":
            await commands.put("GAIN")
        else:
            return web.json_response(
                {"error": "action must be start, stop or gain"}, status=400
            )
        return web.json_response({"ok": True, "action": action})

    async def api_file(request: web.Request) -> web.StreamResponse:
        if request.match_info["name"] == recording_name():
            raise web.HTTPConflict(text="that recording is still being written")
        path = rip_path(cfg, request.match_info["name"])
        if path is None:
            raise web.HTTPNotFound()
        disposition = "attachment" if request.query.get("download") == "1" else "inline"
        return web.FileResponse(
            path,
            headers={"Content-Disposition": f'{disposition}; filename="{path.name}"'},
        )

    async def api_delete(request: web.Request) -> web.StreamResponse:
        if request.match_info["name"] == recording_name():
            raise web.HTTPConflict(text="that recording is still being written")
        path = rip_path(cfg, request.match_info["name"])
        if path is None:
            raise web.HTTPNotFound()
        try:
            path.unlink()
        except OSError as err:
            return web.json_response({"error": str(err)}, status=500)
        note(status, "Deleted %s" % path.name)
        log.info("Deleted rip %s", path)
        return web.json_response({"ok": True})

    app = web.Application()
    app.add_routes(
        [
            web.get("/", index),
            web.get("/api/status", api_status),
            web.post("/api/rip", api_rip),
            web.get("/files/{name}", api_file),
            web.delete("/files/{name}", api_delete),
        ]
    )
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WEB_PORT)
    try:
        try:
            await site.start()
        except OSError as err:
            log.error("Cannot serve the web UI on port %d (%s)", WEB_PORT, err)
            return
        log.info("Web UI listening on port %d — open it from the add-on page", WEB_PORT)
        while not stop.is_set():
            await asyncio.sleep(30)
    finally:
        await runner.cleanup()


def handle_signal(signum: int, _frame: Any) -> None:
    log.info("Received signal %s, shutting down", signum)
    stop.set()


def main() -> None:
    cfg = load_config()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
    )
    log.info(
        "Vinyl Ear %s starting (device=%s, gate=%.0f dBFS, clip=%ss, poll=%ss)",
        VERSION,
        cfg.device_name,
        cfg.noise_gate_db,
        cfg.listen_seconds,
        cfg.poll_seconds,
    )
    log.info("PulseAudio server: %s", os.environ.get("PULSE_SERVER", "(default)"))

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, handle_signal)

    client = connect_mqtt(cfg)
    try:
        asyncio.run(run(cfg, client))
    finally:
        publish(client, f"{cfg.base_topic}/availability", "offline")
        client.loop_stop()
        client.disconnect()
        log.info("Vinyl Ear stopped")


if __name__ == "__main__":
    main()
