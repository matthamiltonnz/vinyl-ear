ARG BUILD_FROM=python:3.11-slim-bookworm
FROM ${BUILD_FROM}

# ffmpeg      - shazamio decodes the clip with it
# pulseaudio-utils - parec (record) and pactl (list sources) talk to the host
#                    PulseAudio server that Home Assistant exposes via audio: true
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg pulseaudio-utils \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY app/ /app/
COPY run.sh /run.sh
RUN chmod 0755 /run.sh

CMD ["/run.sh"]
