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
# The rip options live in the web interface, so they need a home the add-on owns: the
# add-on's own options are only written by the supervisor, and only at a restart.
RIP_SETTINGS_FILE = Path("/data/rip-settings.json")
# The manual listening switch, for installations with no `active_entity`. Off unless
# it has been switched on, which is what the add-on means by "not listening".
LISTENING_FILE = Path("/data/listening.json")
RIP_SETTING_FIELDS = (
    "rip_dir",
    "rip_format",
    "rip_max_minutes",
    "rip_auto_start",
    "rip_auto_stop",
    "rip_trim",
    "rip_auto_gain",
    "rip_auto_stop_seconds",
    "rip_split",
)
VERSION = "0.11.7"
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
# A source can be `RUNNING` and still deliver nothing, which blocks `parec`'s pipe
# forever and freezes the gate, the lookups and the rip with it. A silent read for this
# long is treated as a stall: the process is killed and reopened.
CAPTURE_STALL_SECONDS = 20.0
CAPTURE_RESTART_SECONDS = 5.0
CAPTURE_RESTART_MAX_SECONDS = 60.0
# How long the listener waits for a chunk before saying so in the log and the event list.
CAPTURE_WARN_SECONDS = 30.0
CAPTURE_WARN_AGAIN_SECONDS = 300.0
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
# How much of the lead-in to keep when trimming a hand-started side: enough that the needle
# drop and the first groove are still there, without minutes of run-in before the music.
TRIM_LEAD_SECONDS = 0.5
# Streaming: one ffmpeg turns the live PCM into MP3, and every listener gets the same bytes
# with ICY metadata blocks mixed in every STREAM_METAINT bytes of audio.
STREAM_PORT = 8098
STREAM_METAINT = 16000
# Chunks of PCM held for the encoder before we start dropping: a slow encoder must never
# stall the capture that the listener and the rip share.
STREAM_QUEUE_CHUNKS = 40
# How much encoded audio is kept for listeners, and how long an unwatched stream keeps
# running before its ffmpeg is stopped.
STREAM_BUFFER_BYTES = 512 * 1024
STREAM_IDLE_STOP_SECONDS = 15.0
# FLAC compression level for the lossless stream: 5 is a good CPU/size balance at 44.1 kHz.
STREAM_FLAC_LEVEL = 5
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


@dataclasses.dataclass
class Config:
    """Everything the add-on was configured with.

    Not frozen on purpose: the rip options are changed from the web UI while it runs, and
    every task holds this same object, so a changed field is seen by the next decision
    without restarting anything.
    """
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
    rip_auto_start: bool = False
    rip_auto_stop: bool = True
    rip_trim: bool = True
    rip_auto_gain: bool = False
    rip_auto_stop_seconds: int = 20
    rip_split: bool = False
    stream_enabled: bool = False
    stream_bitrate: int = 128
    stream_format: str = "mp3"

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
        rip_auto_start=bool(raw.get("rip_auto_start", raw.get("rip_auto", False))),
        rip_auto_stop=bool(raw.get("rip_auto_stop", raw.get("rip_auto", True))),
        rip_trim=bool(raw.get("rip_trim", True)),
        rip_auto_gain=bool(raw.get("rip_auto_gain", False)),
        rip_auto_stop_seconds=int(raw.get("rip_auto_stop_seconds", 20)),
        rip_split=bool(raw.get("rip_split", False)),
        stream_enabled=bool(raw.get("stream_enabled", False)),
        stream_bitrate=int(raw.get("stream_bitrate", 128)),
        stream_format=str(raw.get("stream_format") or "mp3"),
    )


def rip_settings(cfg: Config) -> dict[str, Any]:
    """The rip options as plain data: what the web UI shows and the settings file holds."""
    return {name: getattr(cfg, name) for name in RIP_SETTING_FIELDS}


def check_rip_settings(changes: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Validate a partial update; returns (clean values, error message or "")."""
    clean: dict[str, Any] = {}
    for name, value in changes.items():
        if name not in RIP_SETTING_FIELDS:
            continue  # not ours to change
        if name in {"rip_auto_start", "rip_auto_stop", "rip_auto_gain", "rip_trim", "rip_split"}:
            if not isinstance(value, bool):
                return {}, "%s must be true or false" % name
            clean[name] = value
        elif name == "rip_format":
            text = str(value).lower()
            if text not in {"flac", "wav"}:
                return {}, "format must be flac or wav"
            clean[name] = text
        elif name in {"rip_max_minutes", "rip_auto_stop_seconds"}:
            try:
                number = int(value)
            except (TypeError, ValueError):
                return {}, "%s must be a whole number" % name.replace("rip_", "")
            low, high = (1, 180) if name == "rip_max_minutes" else (5, 120)
            if not low <= number <= high:
                return (
                    {},
                    "%s must be between %d and %d"
                    % (name.replace("rip_", "").replace("_", " "), low, high),
                )
            clean[name] = number
        elif name == "rip_dir":
            text = str(value or "").strip()
            if not text:
                return {}, "the rip folder cannot be empty"
            path = Path(text).expanduser()
            if not path.is_absolute():
                return {}, "the rip folder must be an absolute path, like /share/vinyl-rips"
            try:
                path.mkdir(parents=True, exist_ok=True)
                probe = path / ".vinyl-ear-write-test"
                probe.write_text("ok")
                probe.unlink()
            except OSError as err:
                return {}, "cannot write to %s (%s)" % (path, err)
            clean[name] = str(path)
    return clean, ""


def apply_rip_settings(
    cfg: Config, values: dict[str, Any], status: dict[str, Any] | None = None
) -> None:
    """Use new rip settings straight away and remember them; no restart is involved."""
    for name, value in values.items():
        setattr(cfg, name, value)
    try:
        RIP_SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        RIP_SETTINGS_FILE.write_text(json.dumps(rip_settings(cfg), indent=1, sort_keys=True))
    except OSError as err:
        log.warning("Could not save the rip settings (%s)", err)
        if status is not None:
            note(status, "Rip settings applied but not saved (%s)" % err)
        return
    log.info("Rip settings updated: %s", values)
    if status is not None:
        note(
            status,
            "Rip settings updated: %s" % ", ".join("%s=%s" % item for item in values.items()),
        )


def load_rip_settings(cfg: Config) -> None:
    """Overlay the saved rip settings on the add-on options, seeding the file on first run."""
    if RIP_SETTINGS_FILE.is_file():
        try:
            saved = json.loads(RIP_SETTINGS_FILE.read_text())
        except (OSError, ValueError) as err:
            log.warning("Ignoring unreadable %s (%s)", RIP_SETTINGS_FILE, err)
            return
        clean, error = check_rip_settings(saved)
        if error:
            log.warning("Ignoring saved rip settings: %s", error)
            return
        if "rip_auto" in saved and "rip_auto_start" not in saved:
            # 0.10.x had one switch for both halves; keep what it meant.
            clean["rip_auto_start"] = bool(saved["rip_auto"])
            clean.setdefault("rip_auto_stop", bool(saved["rip_auto"]))
        for name, value in clean.items():
            setattr(cfg, name, value)
        log.info("Rip settings: %s", rip_settings(cfg))
        return
    # First start on this version: until now the add-on options were the settings, so keep
    # whatever they say by writing them into the file that is now the source of truth.
    apply_rip_settings(cfg, rip_settings(cfg))


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

    `parec` is supervised: it is reopened when it ends or when its pipe goes silent for
    `CAPTURE_STALL_SECONDS`, because a source that is `RUNNING` but delivering nothing
    leaves the read blocked and the whole add-on waiting behind it.
    """
    restarts = 0
    while not stop.is_set():
        try:
            await capture_stream(cfg, source, queue, channels, tap)
        except CaptureError as err:
            if stop.is_set():
                return
            restarts += 1
            await asyncio.to_thread(resume_source, source)
            delay = min(
                CAPTURE_RESTART_MAX_SECONDS, CAPTURE_RESTART_SECONDS * restarts
            )
            log.warning("%s — reopening the capture in %ds", err, delay)
            await asyncio.sleep(delay)
        else:
            return


async def capture_stream(
    cfg: Config,
    source: str,
    queue: "asyncio.Queue[bytes]",
    channels: int = 1,
    tap: Callable[[bytes], Awaitable[None]] | None = None,
) -> None:
    """One `parec`, read until it hands over audio, stalls or ends."""
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
            try:
                # `wait_for` around `to_thread` cannot work here: cancelling the task does
                # not interrupt the thread, and asyncio waits for that cancellation to
                # finish - so a blocked pipe leaves the timeout hanging for ever. `wait`
                # just stops waiting, and the kill below unblocks the read.
                read = asyncio.ensure_future(
                    asyncio.to_thread(proc.stdout.read, chunk_bytes)
                )
                done, _pending = await asyncio.wait(
                    {read}, timeout=CAPTURE_STALL_SECONDS
                )
                if not done:
                    proc.kill()
                    read.add_done_callback(forget_task)
                    raise CaptureError(
                        "no audio for %ds" % CAPTURE_STALL_SECONDS
                    )
                data = read.result()
            except CaptureError:
                raise
            except (OSError, ValueError) as err:
                raise CaptureError("capture read failed: %s" % err) from err
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
            flag["entity"] = await supervised_entity_is_on(session, cfg.active_entity)
            apply_gate(cfg, flag)
            failures = 0
        except GateUnavailable as err:
            log.error(
                "Cannot read %s (%s); listening continuously instead. The entity gate "
                "needs homeassistant_api: true in the add-on configuration.",
                cfg.active_entity,
                err,
            )
            flag["entity"] = None
            apply_gate(cfg, flag)
            return
        except Exception as err:  # noqa: BLE001 - fail open rather than stop listening
            failures += 1
            if failures == 1 or failures % 60 == 0:
                log.warning("Active entity check failed (%s); assuming active", err)
            # Keep the last known answer: a failed poll must not flip the switch.
            apply_gate(cfg, flag)
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


def load_listening_state() -> bool:
    """The manual listening switch; off when nothing has been saved yet."""
    try:
        saved = json.loads(LISTENING_FILE.read_text())
    except (OSError, ValueError):
        return False
    return bool(saved.get("listening", False))


def save_listening_state(listening: bool) -> None:
    try:
        LISTENING_FILE.write_text(json.dumps({"listening": listening}))
    except OSError as err:
        log.warning("Could not save %s (%s)", LISTENING_FILE, err)


def apply_gate(cfg: Config, gate: dict[str, Any]) -> None:
    """Decide whether to listen: the entity when there is one, else the web switch.

    `gate["entity"]` stays None while nothing has been read from Home Assistant (no
    `active_entity`, no `homeassistant_api`, or a failed poll), and None is what makes the
    manual switch the authority - which is the fallback this add-on offers.
    """
    entity = gate.get("entity")
    gate["active"] = bool(entity) if entity is not None else bool(gate.get("manual"))
    gate["source"] = "entity" if entity is not None else "manual"


def forget_task(task: asyncio.Task) -> None:
    """Retrieve an abandoned task's outcome so asyncio does not report it as unhandled."""
    if not task.cancelled():
        task.exception()


def watch_task(task: asyncio.Task, name: str) -> None:
    """Say so when a background task stops, instead of letting it die unobserved.

    `run()` holds every task in a local, so an exception inside one is never retrieved and
    never printed: a dead capture reader looks exactly like a quiet room.
    """

    def done(finished: asyncio.Task) -> None:
        if finished.cancelled():
            return
        error = finished.exception()
        if error is not None:
            log.error("%s stopped: %r", name, error)

    task.add_done_callback(done)


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


def resume_source(source: str) -> None:
    """Undo a suspended source, which is what a capture that stalls usually is.

    The audio server suspends an idle source, and a `parec` that connects while it is
    suspended blocks on its pipe for ever. Reopening `parec` alone does not always help,
    so the source is asked to resume first.
    """
    if not source:
        return
    if pactl("suspend-source", source, "0") is not None:
        log.info("Asked the audio server to resume %s", source)


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


def loud_window(levels: list[float], chunk_seconds: float) -> tuple[int, int] | None:
    """The first and last chunk of a side that carry music, or None when there is none."""
    if len(levels) < 3 or chunk_seconds <= 0:
        return None
    ordered = sorted(levels)
    loud = ordered[min(len(ordered) - 1, int(len(ordered) * 0.9))]
    floor = loud - TRACK_GAP_DEPTH_DB
    first = next((index for index, level in enumerate(levels) if level > floor), None)
    last = next(
        (index for index in range(len(levels) - 1, -1, -1) if levels[index] > floor), None
    )
    if first is None or last is None:
        return None
    return first, last


def trim_rip(
    path: Path, levels: list[float], chunk_seconds: float, duration: float
) -> tuple[float, float] | None:
    """Cut the run-in and run-out silence off a finished side, in place.

    Returns the (start, end) window that was kept, or None when there was nothing worth
    cutting. The file is re-cut with a stream copy, so this is quick and lossless.
    """
    window = loud_window(levels, chunk_seconds)
    if window is None:
        return None
    first, last = window
    start = max(0.0, first * chunk_seconds - TRIM_LEAD_SECONDS)
    end = min(duration, (last + 1) * chunk_seconds + TRIM_LEAD_SECONDS)
    if start < 0.5 and end > duration - 0.5:
        return None
    temporary = path.with_name(path.stem + "-trimmed" + path.suffix)
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
                # A stream copy of a WAV is sample accurate and keeps the header honest. A
                # FLAC copy does not: STREAMINFO carries the *original* total sample count, so
                # the cut file still claims the whole side (44 s of audio in an 11 s file).
                # Re-encoding FLAC is lossless, so that is what gets used.
                *(["-c", "copy"] if path.suffix.lower() == ".wav" else ["-c:a", "flac"]),
                str(temporary),
            ],
            capture_output=True,
            text=True,
            timeout=600,
        )
    except (OSError, subprocess.SubprocessError) as err:
        log.warning("Could not trim %s (%s)", path.name, err)
        return None
    if result.returncode != 0 or not temporary.exists():
        log.warning("Could not trim %s (%s)", path.name, (result.stderr or "").strip()[:200])
        return None
    try:
        os.replace(temporary, path)
    except OSError as err:
        log.warning("Could not replace %s after trimming (%s)", path.name, err)
        return None
    return start, end


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
        # Snapshot of the format at start, so changing the setting mid-side cannot change
        # what this side is encoded as.
        self.format = cfg.rip_format
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
        self.format = self.cfg.rip_format  # the setting as it is *now*, not last time
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
        if self.format == "flac":
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
            "format": recorder.format,
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
                recorder.format,
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
            recorder.format,
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
        if cfg.rip_trim:
            trimmed = await asyncio.to_thread(
                trim_rip, path, levels, chunk_seconds, seconds
            )
            if trimmed is not None:
                start, end = trimmed
                note(
                    status,
                    "Trimmed %.1fs of silence from %s (kept %.0fs)"
                    % (seconds - (end - start), path.name, end - start),
                )
                keep_from = int(start / chunk_seconds) if chunk_seconds else 0
                keep_to = int(end / chunk_seconds) + 1 if chunk_seconds else len(levels)
                levels = levels[keep_from:keep_to]
                original, seconds = start, end - start
                # The file is shorter now, so the reported length must follow it or the page
                # and the retained payload keep advertising the untrimmed side.
                recorder.frames = int(round(seconds * cfg.sample_rate))
                report(retain=True)
            else:
                original = 0.0
        else:
            original = 0.0
        if not cfg.rip_split:
            return
        cuts = find_track_gaps(levels, chunk_seconds, seconds)
        if not cuts:
            note(status, "No track gaps found in %s — left it whole" % path.name)
            return
        created = await asyncio.to_thread(
            split_rip, path, [cut + original for cut in cuts], seconds
        )
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
    active: dict[str, Any] = {
        "entity": None,
        "manual": load_listening_state(),
        "active": False,
        "source": "manual",
    }
    apply_gate(cfg, active)
    watcher = (
        asyncio.create_task(watch_active_entity(cfg, session, active))
        if cfg.active_entity
        else None
    )

    source = await asyncio.to_thread(find_source, cfg)
    channels = await asyncio.to_thread(source_channels, source)
    encoder = StreamEncoder(cfg, channels) if cfg.stream_enabled else None
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
        if encoder is not None:
            encoder.push(data)

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
        "active": None,
        "activity_source": "manual",
        "failures": 0,
        "last_lookup": None,
        "last_answer": None,
        "track": None,
        "events": [],
        "gain": None,
        "auto": {
            "start": cfg.rip_auto_start,
            "stop": cfg.rip_auto_stop,
            "holding": False,
        },
        "stream": {
            "enabled": cfg.stream_enabled,
            "clients": 0,
            "bitrate": cfg.stream_bitrate,
            "format": "flac" if cfg.stream_format == "flac" else "mp3",
            "running": False,
        },
        "rip": {"state": "idle", "file": "", "seconds": 0, "peak_dbfs": -120,
                "clipped": False, "channels": channels,
                "sample_rate": cfg.sample_rate, "format": cfg.rip_format},
    }

    reader = asyncio.create_task(audio_reader(cfg, source, queue, channels, tap))
    ripper = asyncio.create_task(
        rip_worker(cfg, source, channels, tape, commands, client, status)
    )
    server = asyncio.create_task(web_server(cfg, status, commands, active))
    radio = (
        asyncio.create_task(stream_server(cfg, encoder, status))
        if encoder is not None
        else None
    )
    # A task that stops on its own leaves the add-on half dead and says nothing: keep an eye
    # on all four, because the reader dying is otherwise indistinguishable from a quiet room.
    watch_task(reader, "capture reader")
    watch_task(ripper, "rip worker")
    watch_task(server, "web server")
    if radio is not None:
        watch_task(radio, "stream")

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
    rip_heard_music = False
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

    last_chunk_at = time.monotonic()
    gap_noted_at: float | None = None
    try:
        while not stop.is_set():
            try:
                data = await asyncio.wait_for(
                    queue.get(), timeout=CAPTURE_WARN_SECONDS
                )
            except asyncio.TimeoutError:
                # No chunk at all, which is never silence - a digital-silent source still
                # sends bytes. Say so once, then every few minutes, and keep waiting.
                silent_for = time.monotonic() - last_chunk_at
                if gap_noted_at is None or (
                    time.monotonic() - gap_noted_at >= CAPTURE_WARN_AGAIN_SECONDS
                ):
                    gap_noted_at = time.monotonic()
                    log.warning(
                        "No audio from the source for %ds - has the capture stalled?",
                        silent_for,
                    )
                    note(
                        status,
                        "No audio from the source for %ds - capture stalled?"
                        % silent_for,
                    )
                continue
            now = time.monotonic()
            if gap_noted_at is not None:
                gap_noted_at = None
                if now - last_chunk_at >= CAPTURE_WARN_SECONDS:
                    log.info("Audio is back after %ds", now - last_chunk_at)
                    note(status, "Audio is back after %ds" % (now - last_chunk_at))
            last_chunk_at = now

            if status.get("active") is not active["active"]:
                # The gate only spoke up once a track had been playing, which made a cold
                # start with the gate closed look like a dead add-on.
                status["active"] = active["active"]
                if active.get("source") == "entity":
                    where = "%s is" % cfg.active_entity
                else:
                    where = "Listening (web switch) is"
                state = "on — listening" if active["active"] else "off — not listening"
                log.info("%s %s", where, state)
                note(status, "%s %s" % (where, state))

            status["activity_source"] = active.get("source", "manual")
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

            recording = status["rip"].get("state") == "recording"
            quiet_for = (now - last_loud_at) if last_loud_at is not None else math.inf
            # A rip that has just ended forgets that it heard music; until then the flag also
            # covers an `AUTO` start, whose music arrived before the worker was recording.
            stopped = was_recording and not recording
            # Auto-rip has two halves that are separate settings: starting on its own when
            # a record appears, and stopping when the side runs out. A hand-started rip is
            # always stopped by the second one, but only after it has actually heard music,
            # so a long wait before dropping the needle is not mistaken for an ending.
            if cfg.rip_auto_stop and recording and rip_heard_music:
                if quiet_for >= cfg.rip_auto_stop_seconds:
                    commands.put_nowait("OFF")
                    last_auto_command = now
                    note(
                        status,
                        "Auto-rip: quiet for %ds — stopping" % cfg.rip_auto_stop_seconds,
                    )
            if cfg.rip_auto_start:
                if recording:
                    auto_hold = False
                elif was_recording:
                    # It ended while the record was still playing (a hand stop, or the time
                    # limit): do not spring back up until the side has really finished.
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
                    # The pre-roll is what gets recorded first, so the music that caused this
                    # start is behind us by the time the worker is recording: count it now.
                    rip_heard_music = True
                    note(status, "Auto-rip: sound for %ds — starting" % AUTO_START_SECONDS)
            if recording and level >= threshold:
                rip_heard_music = True
            if stopped:
                rip_heard_music = False
            was_recording = recording
            status["auto"]["holding"] = auto_hold

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
        if radio is not None:
            radio.cancel()
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
  input, select { background: #0b0e13; color: #e7eaf0; border: 1px solid #2a3446;
                  border-radius: 7px; padding: 3px 6px; font: inherit; font-size: .85rem; }
  input.small { width: 64px; }
  input[type="checkbox"] { width: auto; }
  .buttons { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
  code {
    background: #1b212b; border: 1px solid #262d38; border-radius: 7px;
    padding: 4px 8px; font-size: .85rem; user-select: all;
  }
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
    <div class="row"><span>activity</span><span id="activity">—</span></div>
    <div class="row"><span>level</span><span id="level">—</span></div>
    <div class="row"><span>gate / measured floor</span><span id="gates">—</span></div>
    <div class="row"><span>audio above gate</span><span id="playing">—</span></div>
    <div class="row"><span>refused lookups</span><span id="failures">—</span></div>
    <div class="row"><span>last lookup</span><span id="lookup">—</span></div>
    <div class="row"><span>last answer</span><span id="answer">—</span></div>
    <p class="buttons">
      <button id="listenButton" class="quiet">Start listening</button>
    </p>
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
    <h2>Rip settings</h2>
    <div class="row"><span>format</span><span>
      <select id="setFormat"><option value="flac">flac</option><option value="wav">wav</option></select>
    </span></div>
    <div class="row"><span>folder</span><span><input id="setDir" size="16" spellcheck="false"></span></div>
    <div class="row"><span>stop after (min)</span><span>
      <input id="setMax" type="number" min="1" max="180" class="small">
    </span></div>
    <div class="row"><span>auto start</span><span>
      <input id="setAutoStart" type="checkbox"> when a record starts playing
    </span></div>
    <div class="row"><span>auto stop</span><span>
      <input id="setAutoStop" type="checkbox"> after
      <input id="setStop" type="number" min="5" max="120" class="small"> s quiet
    </span></div>
    <div class="row"><span>auto gain</span><span><input id="setGain" type="checkbox"></span></div>
    <div class="row"><span>trim silence</span><span><input id="setTrim" type="checkbox"> cut the run-in and run-out</span></div>
    <div class="row"><span>split tracks</span><span><input id="setSplit" type="checkbox"></span></div>
    <p class="buttons">
      <button id="saveSettings">Save settings</button>
      <span class="muted" id="settingsHint">applies immediately, no restart</span>
    </p>
  </section>
  <section>
    <h2>Radio stream</h2>
    <div class="row"><span>state</span><span id="streamState">off</span></div>
    <div class="row"><span>listeners</span><span id="streamClients">0</span></div>
    <div class="row"><span>quality</span><span id="streamBitrate">—</span></div>
    <p class="muted">A plain HTTP audio stream, so anything that takes a radio URL can
      play it — Music Assistant, VLC, a phone, another Home Assistant:</p>
    <p class="buttons"><code id="streamUrl">—</code>
      <button id="streamCopy" class="quiet">Copy URL</button></p>
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
  const gateBy = status.activity_source === "entity"
    ? (status.active_entity || "the activity entity")
    : "web switch";
  listeningOn = status.active === true;
  $("activity").textContent = status.active === null || status.active === undefined
    ? "—"
    : (listeningOn ? "on — " + gateBy : "off — not listening (" + gateBy + ")");
  const listenButton = $("listenButton");
  if (status.active_entity) {
    listenButton.disabled = true;
    listenButton.textContent = "Controlled by " + status.active_entity;
  } else {
    listenButton.disabled = false;
    listenButton.textContent = listeningOn ? "Stop listening" : "Start listening";
  }
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
  const autoParts = [];
  if (auto.start) autoParts.push("starts on music");
  if (auto.stop) autoParts.push("stops after quiet");
  if (!autoParts.length) autoParts.push("off");
  if (auto.holding) autoParts.push("holding (stopped by hand)");
  $("auto").textContent = autoParts.join(", ");
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

let settingsDirty = false;
let streamUrlText = "";
let listeningOn = false;

function renderSettings(settings) {
  if (!settings || settingsDirty) return;   // never stomp on what is being edited
  $("setFormat").value = settings.rip_format || "flac";
  $("setDir").value = settings.rip_dir || "";
  $("setMax").value = settings.rip_max_minutes ?? 30;
  $("setAutoStart").checked = !!settings.rip_auto_start;
  $("setAutoStop").checked = !!settings.rip_auto_stop;
  $("setStop").value = settings.rip_auto_stop_seconds ?? 20;
  $("setGain").checked = !!settings.rip_auto_gain;
  $("setTrim").checked = !!settings.rip_trim;
  $("setSplit").checked = !!settings.rip_split;
}

function settingsPayload() {
  return {
    rip_format: $("setFormat").value,
    rip_dir: $("setDir").value.trim(),
    rip_max_minutes: Number($("setMax").value),
    rip_auto_start: $("setAutoStart").checked,
    rip_auto_stop: $("setAutoStop").checked,
    rip_auto_stop_seconds: Number($("setStop").value),
    rip_auto_gain: $("setGain").checked,
    rip_trim: $("setTrim").checked,
    rip_split: $("setSplit").checked,
  };
}

async function saveSettings() {
  $("saveSettings").disabled = true;
  $("settingsHint").textContent = "saving…";
  try {
    const response = await fetch("api/settings", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(settingsPayload()),
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body.error || "HTTP " + response.status);
    settingsDirty = false;
    $("settingsHint").textContent = "saved";
    refresh();
  } catch (err) {
    $("settingsHint").textContent = "not saved: " + err.message;
  }
  $("saveSettings").disabled = false;
}

for (const id of ["setFormat", "setDir", "setMax", "setAutoStart", "setAutoStop", "setStop", "setGain", "setTrim", "setSplit"]) {
  $(id).addEventListener("input", () => {
    settingsDirty = true;
    $("settingsHint").textContent = "unsaved changes";
  });
  $(id).addEventListener("change", () => {
    settingsDirty = true;
    $("settingsHint").textContent = "unsaved changes";
  });
}

$("saveSettings").onclick = () => saveSettings();

function renderStream(status) {
  const stream = status.stream || {};
  const format = stream.format === "flac" ? "flac" : "mp3";
  $("streamState").textContent = !stream.enabled
    ? "off — set stream_enabled in the add-on options"
    : (stream.running ? "live" : "ready — starts when a listener arrives");
  $("streamClients").textContent = String(stream.clients || 0);
  $("streamBitrate").textContent = !stream.enabled
    ? "—"
    : (format === "flac" ? "lossless FLAC" : (stream.bitrate || 128) + " kbit/s MP3");
  streamUrlText = stream.enabled
    ? "http://" + location.hostname + ":8098/stream." + format
    : "";
  $("streamUrl").textContent = streamUrlText || "—";
  $("streamCopy").disabled = !streamUrlText;
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
  renderStream(status);
  renderSettings(status.settings);
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

// navigator.clipboard only exists in a secure context, and ingress can be plain http,
// so fall back to the old select-and-copy trick before giving up.
async function copyText(value) {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(value);
      return true;
    }
  } catch (err) {
    // fall through to the textarea
  }
  const area = document.createElement("textarea");
  area.value = value;
  area.setAttribute("readonly", "");
  area.style.position = "fixed";
  area.style.top = "-1000px";
  document.body.appendChild(area);
  area.select();
  let copied = false;
  try {
    copied = document.execCommand("copy");
  } catch (err) {
    copied = false;
  }
  document.body.removeChild(area);
  return copied;
}

$("streamCopy").onclick = async () => {
  if (!streamUrlText) return;
  const button = $("streamCopy");
  const copied = await copyText(streamUrlText);
  button.textContent = copied ? "Copied" : "Copy failed";
  setTimeout(() => { button.textContent = "Copy URL"; }, 1500);
};

// Only shown as a real switch when no activity entity is configured: with one, the entity
// decides and the button is disabled.
$("listenButton").onclick = async () => {
  const button = $("listenButton");
  const wanted = !listeningOn;
  button.disabled = true;
  try {
    const response = await fetch("api/listening", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ listening: wanted }),
    });
    if (!response.ok) {
      const answer = await response.json().catch(() => ({}));
      button.textContent = answer.error || "could not change it";
      return;
    }
  } catch (err) {
    button.textContent = "could not change it";
    return;
  }
  refresh();
};

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
    cfg: Config,
    status: dict[str, Any],
    commands: "asyncio.Queue[str]",
    gate: dict[str, Any] | None = None,
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
        payload["settings"] = rip_settings(cfg)
        payload["active_entity"] = cfg.active_entity
        # The chunk loop is the only writer of `holding`, but it only runs while audio
        # arrives: taking the on/off flags from it made the page say "off" after the
        # settings had been changed on a silent input.
        payload["auto"] = {
            "start": cfg.rip_auto_start,
            "stop": cfg.rip_auto_stop,
            "holding": payload.get("auto", {}).get("holding", False),
        }
        payload["now"] = time.time()
        looked_up = status.get("last_lookup")
        payload["last_lookup_seconds"] = (
            round(time.time() - looked_up) if looked_up else None
        )
        return web.json_response(payload)

    async def api_listening(request: web.Request) -> web.StreamResponse:
        """The manual listening switch, for installations with no activity entity."""
        if cfg.active_entity:
            return web.json_response(
                {"error": "%s decides this" % cfg.active_entity}, status=409
            )
        try:
            body = await request.json()
        except (ValueError, aiohttp.ContentTypeError):
            body = None
        if not isinstance(body, dict):
            return web.json_response({"error": "send a JSON object"}, status=400)
        wanted = body.get("listening")
        if not isinstance(wanted, bool):
            return web.json_response(
                {"error": "listening must be true or false"}, status=400
            )
        if gate is None:
            return web.json_response({"error": "no gate to change"}, status=500)
        gate["manual"] = wanted
        apply_gate(cfg, gate)
        await asyncio.to_thread(save_listening_state, wanted)
        log.info(
            "Listening switched %s from the web UI", "on" if wanted else "off"
        )
        note(status, "Listening switched %s from the web UI" % ("on" if wanted else "off"))
        return web.json_response({"ok": True, "listening": gate["active"]})

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

    async def api_settings(request: web.Request) -> web.StreamResponse:
        """Change the rip options from the page; they apply at once and are saved."""
        try:
            body = await request.json()
        except (ValueError, aiohttp.ContentTypeError):
            body = None
        if not isinstance(body, dict):
            return web.json_response({"error": "send a JSON object"}, status=400)
        clean, error = check_rip_settings(body)
        if error:
            return web.json_response({"error": error}, status=400)
        if not clean:
            return web.json_response(
                {"error": "nothing to change", "settings": rip_settings(cfg)}, status=400
            )
        await asyncio.to_thread(apply_rip_settings, cfg, clean, status)
        return web.json_response({"ok": True, "settings": rip_settings(cfg)})

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
            web.post("/api/listening", api_listening),
            web.patch("/api/settings", api_settings),
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


def icy_title(track: dict[str, Any] | None) -> str:
    """`Artist - Title` for an ICY StreamTitle, or "" when nothing is named."""
    if not track:
        return ""
    artist = (track.get("artist") or "").strip()
    title = (track.get("title") or "").strip()
    if artist and title:
        return f"{artist} - {title}"[:120]
    return (artist or title)[:120]


def icy_block(title: str) -> bytes:
    """One ICY metadata block: StreamTitle='…'; padded with zeros to a multiple of 16."""
    text = "StreamTitle='%s';" % title.replace("'", "").replace(";", ",") if title else ""
    payload = text.encode("utf-8")
    blocks = min(255, (len(payload) + 15) // 16)
    return bytes([blocks]) + payload[: blocks * 16].ljust(blocks * 16, b"\0")


class StreamBuffer:
    """The encoder's MP3 bytes, kept long enough for the clients reading them.

    Listeners are independent: each one has an absolute offset into this buffer, so a slow
    client misses audio (and is dropped once it falls off the front) rather than holding up
    the record. Polling beats an Event here — an append between a readiness check and a
    `clear()` would lose the wakeup, and 20 ms of latency is nothing on a radio stream.
    """

    def __init__(self, limit: int = STREAM_BUFFER_BYTES) -> None:
        self.limit = limit
        self.data = bytearray()
        self.base = 0  # absolute offset of data[0]
        self.end = 0  # absolute offset just past the last byte

    def append(self, data: bytes) -> None:
        self.data.extend(data)
        self.end += len(data)
        if len(self.data) > self.limit:
            drop = len(self.data) - self.limit
            del self.data[:drop]
            self.base += drop

    def behind(self, offset: int) -> bool:
        """True when `offset` has fallen off the front of the buffer."""
        return offset < self.base

    async def read(self, offset: int) -> tuple[int, bytes]:
        """Wait for audio past `offset` and return it with the new offset."""
        while offset >= self.end:
            await asyncio.sleep(0.02)
        offset = max(offset, self.base)
        return self.end, bytes(self.data[offset - self.base :])


class StreamEncoder:
    """One ffmpeg process turning the live PCM into MP3 for radio listeners.

    It runs only while somebody is listening (plus a grace period), and the PCM is *pushed*
    at it rather than pulled, so a stalled or busy encoder drops audio instead of stalling
    the capture that the listener and the rip share.
    """

    def __init__(self, cfg: Config, channels: int) -> None:
        self.cfg = cfg
        self.channels = channels
        self.audio = StreamBuffer()
        self.clients = 0
        self.idle_since: float | None = time.monotonic()
        self.process: asyncio.subprocess.Process | None = None
        self.feed: "asyncio.Queue[bytes] | None" = None
        self.tasks: list["asyncio.Task[Any]"] = []
        self._lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.returncode is None

    @property
    def format(self) -> str:
        """The stream's format: `mp3` (with ICY metadata) or `flac` (lossless, no ICY)."""
        return "flac" if str(self.cfg.stream_format).lower() == "flac" else "mp3"

    @property
    def content_type(self) -> str:
        return "audio/flac" if self.format == "flac" else "audio/mpeg"

    def codec_args(self) -> list[str]:
        """The output half of the ffmpeg command."""
        if self.format == "flac":
            return [
                "-c:a",
                "flac",
                "-compression_level",
                str(STREAM_FLAC_LEVEL),
                "-f",
                "flac",
            ]
        return [
            "-c:a",
            "libmp3lame",
            "-b:a",
            f"{self.cfg.stream_bitrate}k",
            "-f",
            "mp3",
        ]

    async def start(self) -> None:
        async with self._lock:
            if self.running:
                return
            command = [
                "ffmpeg",
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "s16le",
                "-ar",
                str(self.cfg.sample_rate),
                "-ac",
                str(self.channels),
                "-i",
                "pipe:0",
                *self.codec_args(),
                "pipe:1",
            ]
            try:
                self.process = await asyncio.create_subprocess_exec(
                    *command,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                )
            except OSError as err:
                log.error("Could not start ffmpeg for the stream (%s)", err)
                self.process = None
                return
            self.feed = asyncio.Queue(maxsize=STREAM_QUEUE_CHUNKS)
            self.tasks = [
                asyncio.create_task(self._feed_encoder()),
                asyncio.create_task(self._read_encoder()),
            ]
            log.info(
                "Streaming at %d kbit/s MP3 on port %d", self.cfg.stream_bitrate, STREAM_PORT
            )

    async def stop(self) -> None:
        async with self._lock:
            process, self.process = self.process, None
            self.feed = None
            tasks, self.tasks = self.tasks, []
            for task in tasks:
                task.cancel()
            if process is None:
                return
            try:
                if process.stdin is not None:
                    process.stdin.close()
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except (asyncio.TimeoutError, ProcessLookupError, OSError):
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            log.info("Stopped streaming (nobody listening)")

    def push(self, data: bytes) -> None:
        """Hand a chunk of PCM to the encoder; never blocks and never raises."""
        feed = self.feed
        if feed is None or not self.running:
            return
        try:
            feed.put_nowait(data)
        except asyncio.QueueFull:
            pass

    async def _feed_encoder(self) -> None:
        process = self.process
        if process is None or process.stdin is None:
            return
        feed = self.feed
        assert feed is not None
        try:
            while True:
                data = await feed.get()
                process.stdin.write(data)
                await process.stdin.drain()
        except (asyncio.CancelledError, OSError):
            pass

    async def _read_encoder(self) -> None:
        process = self.process
        if process is None or process.stdout is None:
            return
        try:
            while True:
                data = await process.stdout.read(4096)
                if not data:
                    break
                self.audio.append(data)
        except (asyncio.CancelledError, OSError):
            pass


async def stream_server(
    cfg: Config, encoder: StreamEncoder, status: dict[str, Any]
) -> None:
    """A radio stream for Music Assistant: live MP3 with ICY metadata.

    This one is on a published port rather than behind ingress, because the listener is
    another add-on (Music Assistant), not a browser that can carry Home Assistant's auth.
    It therefore serves audio and nothing else — no control, no files.
    """

    async def index(_request: web.Request) -> web.StreamResponse:
        quality = (
            "lossless FLAC"
            if encoder.format == "flac"
            else "%d kbit/s MP3" % cfg.stream_bitrate
        )
        lines = [
            "Vinyl Ear — %s" % quality,
            "stream:    /stream.%s" % encoder.format,
            "playlist:  /stream.m3u",
            "now:       /now-playing",
            "playing:   %s" % (icy_title(status.get("track")) or "nothing named yet"),
        ]
        return web.Response(text="\n".join(lines) + "\n", content_type="text/plain")

    async def playlist(_request: web.Request) -> web.StreamResponse:
        return web.Response(
            text="#EXTM3U\nstream.%s\n" % encoder.format, content_type="audio/x-mpegurl"
        )

    async def now_playing(_request: web.Request) -> web.StreamResponse:
        return web.json_response(
            {
                "title": icy_title(status.get("track")),
                "track": status.get("track"),
                "listeners": encoder.clients,
                "format": encoder.format,
                "bitrate": None if encoder.format == "flac" else cfg.stream_bitrate,
            }
        )

    async def stream(request: web.Request) -> web.StreamResponse:
        wanted = request.path.rsplit(".", 1)[-1].lower()
        if wanted in {"mp3", "flac"} and wanted != encoder.format:
            # One extension per format: send the player where the audio actually is.
            raise web.HTTPFound("/stream.%s" % encoder.format)
        await encoder.start()
        headers = {
            "Content-Type": encoder.content_type,
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
        }
        if encoder.format == "mp3":
            # ICY is an MP3/AAC convention: a raw FLAC stream has nowhere to put it.
            headers.update(
                {
                    "icy-name": "Vinyl Ear",
                    "icy-genre": "Vinyl",
                    "icy-br": str(cfg.stream_bitrate),
                    "icy-metaint": str(STREAM_METAINT),
                    "icy-pub": "0",
                }
            )
        response = web.StreamResponse(headers=headers)
        await response.prepare(request)
        encoder.clients += 1
        encoder.idle_since = None
        offset = encoder.audio.end  # join at the live edge
        sent = 0
        log.info("Stream listener joined (%d now)", encoder.clients)
        try:
            while True:
                if request.transport is None or request.transport.is_closing():
                    log.info("Stream listener went away")
                    break
                if encoder.audio.behind(offset):
                    # So far behind that the audio is gone; a reconnect starts afresh.
                    log.info("Dropping a stream listener that fell behind")
                    break
                try:
                    offset, data = await asyncio.wait_for(
                        encoder.audio.read(offset), timeout=20.0
                    )
                except asyncio.TimeoutError:
                    if not encoder.running:
                        log.info("Stream encoder stopped; closing the listener")
                        break
                    continue
                if encoder.format != "mp3":
                    # A raw FLAC stream has nowhere to put ICY metadata: send the frames on.
                    await response.write(data)
                    sent += len(data)
                    continue
                view = memoryview(data)
                while view:
                    room = STREAM_METAINT - sent % STREAM_METAINT
                    piece = view[:room]
                    await response.write(piece)
                    sent += len(piece)
                    view = view[room:]
                    if sent % STREAM_METAINT == 0:
                        await response.write(icy_block(icy_title(status.get("track"))))
        except (asyncio.CancelledError, ConnectionResetError, RuntimeError):
            pass
        finally:
            encoder.clients -= 1
            if encoder.clients <= 0:
                encoder.clients = 0
                encoder.idle_since = time.monotonic()
            log.info("Stream listener left (%d now)", encoder.clients)
        return response

    async def reaper() -> None:
        """Stop ffmpeg when nobody has been listening for a while."""
        while not stop.is_set():
            status["stream"] = {
                "enabled": True,
                "clients": encoder.clients,
                "bitrate": cfg.stream_bitrate,
                "format": encoder.format,
                "running": encoder.running,
            }
            if encoder.clients > 0:
                encoder.idle_since = None
            elif encoder.idle_since is None:
                encoder.idle_since = time.monotonic()
            elif time.monotonic() - encoder.idle_since >= STREAM_IDLE_STOP_SECONDS:
                await encoder.stop()
                encoder.idle_since = None
            await asyncio.sleep(2.0)

    app = web.Application()
    app.add_routes(
        [
            web.get("/", index),
            web.get("/stream.mp3", stream),
            web.get("/stream.flac", stream),
            web.get("/stream.m3u", playlist),
            web.get("/now-playing", now_playing),
        ]
    )
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", STREAM_PORT)
    try:
        try:
            await site.start()
        except OSError as err:
            log.error("Cannot serve the stream on port %d (%s)", STREAM_PORT, err)
            return
        log.info(
            "Stream listening on port %d (http://<home-assistant>:%d/stream.mp3)",
            STREAM_PORT,
            STREAM_PORT,
        )
        await reaper()
    finally:
        status["stream"] = {
            "enabled": cfg.stream_enabled,
            "clients": 0,
            "bitrate": cfg.stream_bitrate,
            "format": encoder.format,
            "running": False,
        }
        await runner.cleanup()
        await encoder.stop()


def handle_signal(signum: int, _frame: Any) -> None:
    log.info("Received signal %s, shutting down", signum)
    stop.set()


def main() -> None:
    cfg = load_config()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
    )
    load_rip_settings(cfg)
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
