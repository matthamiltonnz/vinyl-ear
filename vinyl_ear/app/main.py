"""Vinyl Ear — identify the record playing on the turntable, publish it to Home Assistant.

Audio comes from the host microphone through PulseAudio (Home Assistant exposes the
socket to add-ons that set `audio: true`). A clip is recorded only while the signal is
above a noise gate, fingerprinted with Shazam (shazamio), and the result is published to
MQTT with Home Assistant discovery.
"""

from __future__ import annotations

import array
import asyncio
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
FULL_SCALE = 32768.0
CHUNK_SECONDS = 0.25
SILENCE_RESET_SECONDS = 1.5

log = logging.getLogger("vinyl_ear")
stop = threading.Event()


class CaptureError(RuntimeError):
    """Raised when the PulseAudio capture stream cannot be opened or dies."""


@dataclasses.dataclass(frozen=True)
class Config:
    source: str
    sample_rate: int
    listen_seconds: int
    capture_timeout: int
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
        sample_rate=int(raw.get("sample_rate", 44100)),
        listen_seconds=int(raw.get("listen_seconds", 15)),
        capture_timeout=int(raw.get("capture_timeout", 60)),
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


def capture_clip(cfg: Config, source: str) -> bytes | None:
    """Record until `listen_seconds` of audio above the noise gate is collected.

    Returns None when nothing but silence was heard within `capture_timeout`.
    """
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
    target_bytes = int(cfg.bytes_per_second * cfg.listen_seconds)
    clip = bytearray()
    silence = 0.0
    deadline = time.monotonic() + cfg.capture_timeout

    try:
        while not stop.is_set():
            if time.monotonic() > deadline:
                log.debug(
                    "No audio above %.0f dBFS within %ss", cfg.noise_gate_db, cfg.capture_timeout
                )
                return None
            data = proc.stdout.read(chunk_bytes)
            if not data:
                stderr = b""
                if proc.stderr is not None:
                    stderr = proc.stderr.read() or b""
                raise CaptureError(
                    "capture stream ended: " + stderr.decode("utf-8", "replace").strip()
                )
            usable = len(data) - (len(data) % 2)
            samples = array.array("h")
            samples.frombytes(data[:usable])
            rms = math.sqrt(sum(s * s for s in samples) / len(samples)) if samples else 0.0
            if rms_to_dbfs(rms) >= cfg.noise_gate_db:
                silence = 0.0
                clip.extend(data[:usable])
                if len(clip) >= target_bytes:
                    return bytes(clip[:target_bytes])
            else:
                silence += CHUNK_SECONDS
                if silence >= SILENCE_RESET_SECONDS:
                    clip.clear()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    return None


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


def idle_track(device_name: str) -> dict[str, Any]:
    """State published once the room has been silent long enough to assume a stop."""
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
    }


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
    shazam = Shazam()
    session = aiohttp.ClientSession()
    source: str | None = None
    current_key: tuple[str, str] | None = None
    playing = False

    while not stop.is_set():
        if source is None:
            try:
                source = await asyncio.to_thread(find_source, cfg)
            except CaptureError as err:
                log.error("%s", err)
                await asyncio.sleep(15)
                continue

        try:
            clip = await asyncio.to_thread(capture_clip, cfg, source)
        except CaptureError as err:
            log.error("%s", err)
            source = None
            await asyncio.sleep(10)
            continue

        if clip is None:
            if playing:
                playing = False
                publish(client, playing_topic, "OFF")
            if current_key is not None:
                current_key = None
                publish(client, state_topic, json.dumps(idle_track(cfg.device_name)))
                log.info(
                    "Silence for %ss — cleared the now playing state", cfg.capture_timeout
                )
            await asyncio.sleep(1)
            continue

        if not playing:
            playing = True
            publish(client, playing_topic, "ON")

        track = await recognise(shazam, cfg, clip)
        if track is None:
            log.info("Heard audio but Shazam did not recognise it")
        else:
            key = (track["artist"], track["title"])
            if key != current_key:
                current_key = key
                remote_artwork = track["artwork"]
                track["artwork"] = await cache_artwork(session, remote_artwork, cfg.device_name)
                track["artwork_remote"] = remote_artwork
                publish(client, state_topic, json.dumps(track))
                log.info(
                    "Now playing: %s - %s (%s, %s)",
                    track["artist"] or "?",
                    track["title"] or "?",
                    track["album"] or "unknown album",
                    track["year"] or "unknown year",
                )
            else:
                log.debug("Same track still playing: %s - %s", *key)

        await asyncio.sleep(cfg.poll_seconds)

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
        "Vinyl Ear 0.1.0 starting (device=%s, gate=%.0f dBFS, clip=%ss, poll=%ss)",
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
