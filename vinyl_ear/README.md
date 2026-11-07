# Vinyl Ear

Identifies the record spinning on the turntable by listening to a room microphone and
asking Shazam, then publishes the result to Home Assistant over MQTT (discovery).

See the [repository README](../README.md) for the full story, options and a Lovelace
card. The short version:

1. Set the microphone gain so music is audible — e.g.
   `pactl set-source-volume <usb-mic-source> 45%`.
2. Install the add-on, fill in `mqtt_user` / `mqtt_password`, start it.
3. Watch the log: it prints the PulseAudio source it picked and every match.
4. A new device **Vinyl Ear (turntable)** appears under *Settings → Devices &
   Services → MQTT* with `sensor.vinyl_ear_turntable_now_playing` and
   `binary_sensor.vinyl_ear_turntable_audio_detected`.

If the log says no capture source was found, fill in the `source` option with a name
from `pactl list short sources` (skip anything ending in `.monitor`).
