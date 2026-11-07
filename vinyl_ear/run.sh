#!/usr/bin/env bash
# Home Assistant add-on entry point.
set -euo pipefail

# Add-ons with `audio: true` get the host PulseAudio socket here. libpulse does
# not look in /run/audio on its own, so point it there unless the supervisor
# already did.
for candidate in /run/audio/pulse.sock /var/run/audio/pulse.sock; do
    if [ -S "${candidate}" ]; then
        export PULSE_SERVER="${PULSE_SERVER:-unix:${candidate}}"
        break
    fi
done

exec python3 -u /app/main.py
