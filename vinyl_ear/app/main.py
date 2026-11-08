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
from shazamio import Shazam

OPTIONS_FILE = Path(os.environ.get("VINYL_EAR_OPTIONS", "/data/options.json"))
ARTWORK_DIR = Path("/config/www/vinyl-art")
# How many epoch-named covers to keep beside the fixed `device_name.jpg`.
ARTWORK_KEEP = 5
ALBUM_STORE = Path("/data/albums.json")
VERSION = "0.7.0"
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
        samples = array.array("h")
        samples.frombytes(data[: len(data) - (len(data) % 2)])
        if samples:
            level = max(max(samples), -min(samples))
            if level > self.peak:
                self.peak = level

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
    channels: int,
    tape: "asyncio.Queue[bytes]",
    commands: "asyncio.Queue[str]",
    client: mqtt.Client,
) -> None:
    """Record while a rip is running, on MQTT commands; ignore the tape in between."""
    state_topic = f"{cfg.base_topic}/rip/state"
    info_topic = f"{cfg.base_topic}/rip"
    recorder = RipRecorder(cfg, channels)
    last_report = 0.0

    def report(retain: bool = False) -> None:
        publish(client, state_topic, "ON" if recorder.recording else "OFF", retain=retain)
        publish(
            client,
            info_topic,
            json.dumps(
                {
                    "state": "recording" if recorder.recording else "idle",
                    "file": str(recorder.file) if recorder.file else "",
                    "seconds": round(recorder.seconds),
                    "peak_dbfs": round(recorder.peak_dbfs) if recorder.peak else -120,
                    "clipped": recorder.peak >= 32767,
                    "channels": channels,
                    "sample_rate": cfg.sample_rate,
                    "format": cfg.rip_format,
                }
            ),
            retain=retain,
        )

    def stop_rip() -> None:
        seconds = recorder.seconds
        peak = recorder.peak_dbfs
        clipped = recorder.peak >= 32767
        path = recorder.finish()
        report(retain=True)
        if path is None:
            log.info("Rip stopped before anything was written")
            return
        log.info(
            "Rip finished: %s (%.0fs, peak %.0f dBFS%s)",
            path,
            seconds,
            peak,
            ", CLIPPED - lower the capture gain" if clipped else "",
        )

    report(retain=True)
    try:
        while not stop.is_set():
            try:
                while not commands.empty():
                    command = commands.get_nowait().strip().upper()
                    if command in {"START", "ON"} and not recorder.recording:
                        try:
                            recorder.start()
                        except OSError as err:
                            log.error("Cannot write a rip to %s (%s)", cfg.rip_dir, err)
                            report(retain=True)
                        else:
                            last_report = 0.0
                            report(retain=True)
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
                    elif command in {"STOP", "OFF"} and recorder.recording:
                        stop_rip()
                data = await tape.get()
                if not recorder.recording:
                    continue
                recorder.write(data)
                now = time.monotonic()
                if now - last_report >= RIP_REPORT_SECONDS:
                    last_report = now
                    report()
                if recorder.seconds >= cfg.rip_max_minutes * 60:
                    log.info("Rip reached the %d minute limit; stopping", cfg.rip_max_minutes)
                    stop_rip()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - a rip must not kill the listener
                log.exception("Rip worker error: %s", err)
                await asyncio.sleep(1)
    finally:
        if recorder.recording:
            log.info("Add-on stopping; closing the rip")
            stop_rip()


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

    reader = asyncio.create_task(audio_reader(cfg, source, queue, channels, tap))
    ripper = asyncio.create_task(rip_worker(cfg, channels, tape, commands, client))

    prebuffer = bytearray()
    clip = bytearray()
    last_loud_at: float | None = None
    last_attempt_at = 0.0
    current_key: tuple[str, str] | None = None
    last_track: dict[str, Any] | None = None
    last_identified_at: float = time.monotonic()
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
                    publish(client, playing_topic, "OFF")
                if current_key is not None:
                    current_key = None
                    last_track = None
                    recent.clear()
                    state_is_unidentified = False
                    publish(client, state_topic, json.dumps(idle_track(cfg.device_name)))
                    log.info("%s is off — cleared the now playing state", cfg.active_entity)
                clip = bytearray()
                last_loud_at = now
                continue

            level = chunk_level(data)
            levels.append(level)
            floor_levels.append(level)

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
                    if abs(wanted - threshold) >= 1.0:
                        threshold = wanted
                        log.info(
                            "Room noise floor ~%.0f dBFS (steady), "
                            "activity threshold now %.0f dBFS",
                            floor,
                            threshold,
                        )

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
                    publish(client, playing_topic, "ON")
            else:
                if last_loud_at is None:
                    last_loud_at = now - SILENCE_DEBOUNCE_SECONDS
                quiet_for = now - last_loud_at
                if playing and quiet_for >= SILENCE_DEBOUNCE_SECONDS:
                    playing = False
                    publish(client, playing_topic, "OFF")
                if quiet_for >= GATE_HOLD_SECONDS:
                    # A real gap, not the quiet bar of the track that is playing.
                    clip = bytearray()
                elif len(clip) < listen_bytes * 3:
                    clip.extend(data)

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

            cooldown = (
                0.0
                if new_segment
                else min(cfg.poll_seconds * (2 ** min(failures, 3)), MAX_COOLDOWN_SECONDS)
            )
            if len(clip) >= listen_bytes and (now - last_attempt_at) >= cooldown:
                last_attempt_at = now
                new_segment = False
                track = await recognise(shazam, cfg, bytes(clip[:listen_bytes]), channels)
                clip = bytearray()
                last_loud_at = time.monotonic()
                while not queue.empty():
                    queue.get_nowait()
                prebuffer = bytearray()

                if track is None:
                    failures += 1
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
        if watcher is not None:
            watcher.cancel()
        await session.close()


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
