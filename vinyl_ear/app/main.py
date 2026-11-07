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
import subprocess
import tempfile
import threading
import time
import wave
from pathlib import Path
from typing import Any

import aiohttp
import paho.mqtt.client as mqtt
from shazamio import Shazam

OPTIONS_FILE = Path(os.environ.get("VINYL_EAR_OPTIONS", "/data/options.json"))
ARTWORK_DIR = Path("/config/www/vinyl-art")
ALBUM_STORE = Path("/data/albums.json")
VERSION = "0.6.3"
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
# Stop learning the floor while a record is still being recognised.
FLOOR_LEARN_AFTER_SECONDS = 300.0
CALIBRATION_SECONDS = 1.0
LEVELS_WINDOW = int(30 / CHUNK_SECONDS)
# Shazam answers the same audio with different catalogue entries, so a track change
# is only believed once it has been seen this many times within this many seconds.
TRACK_CONFIRM_VOTES = 2
TRACK_CONFIRM_SECONDS = 180.0
TRACK_CONFIRM_LOOKBACK = 4
MAX_COOLDOWN_SECONDS = 300.0
HEARTBEAT_SECONDS = 300.0

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

    @property
    def bytes_per_second(self) -> int:
        return self.sample_rate * 2  # mono, signed 16-bit little endian

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
    )


def rms_to_dbfs(rms: float) -> float:
    return 20.0 * math.log10(max(rms, 1e-9) / FULL_SCALE)


def room_floor(levels: "collections.deque[float]") -> float | None:
    """The quiet level of the room, or None when the window is not quiet.

    Noise is steady: a fan, a fridge or amplifier hiss barely moves across half a
    minute, so its tenth and ninetieth percentiles sit close together. Music swings
    with every bar, so the tenth percentile of a music-heavy window is simply the
    quiet part of the music - taking that for a noise floor is how the gate climbed
    into the music and stopped the listener calling Shazam at all.
    """
    window = sorted(levels)
    if len(window) < LEVELS_WINDOW // 3:
        return None
    low = window[len(window) // 10]
    high = window[len(window) * 9 // 10]
    if high - low > NOISE_SPREAD_DB:
        return None
    return low


def calibrate_threshold(
    levels: "collections.deque[float]", cfg: Config, since_match: float | None
) -> tuple[float, float] | None:
    """Return (floor, threshold), or None to keep the current threshold.

    A fixed gate is useless in a room whose fan or amplifier sits above it: the
    listener then sees 'audio' forever, never identifies anything and never settles.
    Learn the room instead - but only from steady audio, and only while nothing has
    been recognised for a while: a record that is being named is not a noise floor.
    """
    if since_match is not None and since_match < FLOOR_LEARN_AFTER_SECONDS:
        return None
    floor = room_floor(levels)
    if floor is None:
        return None
    wanted = min(
        max(floor + SILENCE_MARGIN_DB, cfg.noise_gate_db),
        cfg.noise_gate_db + 25.0,
    )
    return floor, wanted


def find_source(cfg: Config) -> str:
    """Return the PulseAudio source to record from."""
    if cfg.source:
        return cfg.source
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
    sources = [
        line.split("\t")[1] for line in result.stdout.splitlines() if len(line.split("\t")) >= 2
    ]
    for name in sources:
        if ".monitor" not in name:
            log.info("Using PulseAudio source %s", name)
            return name
    raise CaptureError(
        "no capture source found; available sources: "
        + (", ".join(sources) if sources else "none")
    )


def parec_command(cfg: Config, source: str) -> list[str]:
    return [
        "parec",
        f"--device={source}",
        "--format=s16le",
        f"--rate={cfg.sample_rate}",
        "--channels=1",
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


async def audio_reader(cfg: Config, source: str, queue: "asyncio.Queue[bytes]") -> None:
    """Stream capture chunks into the queue for as long as the add-on runs."""
    command = parec_command(cfg, source)
    log.debug("Starting capture: %s", " ".join(command))
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    assert proc.stdout is not None
    chunk_bytes = int(cfg.sample_rate * 2 * CHUNK_SECONDS)
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
            await queue.put(data[: len(data) - (len(data) % 2)])
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


def write_wav(pcm: bytes, sample_rate: int) -> str:
    """Write raw s16le mono PCM to a temporary WAV file for shazamio."""
    handle = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    path = handle.name
    handle.close()
    with wave.open(path, "wb") as wav:
        wav.setnchannels(1)
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

    target = ARTWORK_DIR / f"{device_name}.jpg"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as response:
            response.raise_for_status()
            data = await response.read()
        if not data:
            raise ValueError("empty response")
        temporary = target.with_suffix(".tmp")
        temporary.write_bytes(data)
        os.replace(temporary, target)
    except Exception as err:  # noqa: BLE001 - the remote URL is an acceptable fallback
        log.warning("Could not cache artwork (%s); publishing the remote URL", err)
        return url

    log.info("Cached artwork for %s (%d bytes)", device_name, len(data))
    return f"/local/vinyl-art/{device_name}.jpg?v={int(time.time())}"


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
    return {
        "display": "Nothing playing",
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
        display = artist or album or "Track not identified"
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
    publish(client, sensor_topic, json.dumps(sensor))
    publish(client, binary_topic, json.dumps(binary))
    log.info(
        "Published MQTT discovery for device 'Vinyl Ear (%s)' — now_playing + audio_detected",
        cfg.device_name,
    )


async def recognise(shazam: Shazam, cfg: Config, clip: bytes) -> dict[str, Any] | None:
    """Ask Shazam what the clip contains."""
    path = write_wav(clip, cfg.sample_rate)
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


async def run(cfg: Config, client: mqtt.Client) -> None:
    publish_discovery(client, cfg)
    publish(client, f"{cfg.base_topic}/availability", "online")

    state_topic = f"{cfg.base_topic}/state"
    playing_topic = f"{cfg.base_topic}/playing"
    listen_bytes = int(cfg.bytes_per_second * cfg.listen_seconds)
    prebuffer_bytes = int(cfg.bytes_per_second * PREBUFFER_SECONDS)

    shazam = Shazam()
    session = aiohttp.ClientSession()
    queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=240)
    active = {"active": True}
    watcher = (
        asyncio.create_task(watch_active_entity(cfg, session, active))
        if cfg.active_entity
        else None
    )

    source = await asyncio.to_thread(find_source, cfg)
    reader = asyncio.create_task(audio_reader(cfg, source, queue))

    prebuffer = bytearray()
    clip = bytearray()
    last_loud_at: float | None = None
    last_attempt_at = 0.0
    current_key: tuple[str, str] | None = None
    last_track: dict[str, Any] | None = None
    last_identified_at: float | None = None
    state_is_unidentified = False
    playing = False
    new_segment = True
    albums = load_album_store()
    levels: collections.deque[float] = collections.deque(maxlen=LEVELS_WINDOW)
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
        current_key = key
        last_identified_at = now
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

            if now - last_calibration >= CALIBRATION_SECONDS:
                last_calibration = now
                since_match = (
                    None if last_identified_at is None else now - last_identified_at
                )
                calibrated = calibrate_threshold(levels, cfg, since_match)
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
                track = await recognise(shazam, cfg, bytes(clip[:listen_bytes]))
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
                    if (
                        cfg.blank_unknown_track
                        and current_key is not None
                        and last_track is not None
                        and not state_is_unidentified
                    ):
                        publish(client, state_topic, json.dumps(unidentified_track(last_track)))
                        state_is_unidentified = True
                        log.info(
                            "Unidentified track: keeping the album %r, blanking the track name",
                            last_track.get("album") or "unknown",
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
