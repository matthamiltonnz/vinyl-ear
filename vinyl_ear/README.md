# Vinyl Ear

Identifies the record spinning on the turntable by listening to a microphone or any capture
device connected to Home Assistant, asking Shazam, and publishing the answer over MQTT with
discovery — so `sensor.vinyl_ear_<device_name>_now_playing` carries the artist, title, album,
year and sleeve, and `binary_sensor.…_audio_detected` says whether the input is live. The same
capture can be recorded losslessly, a side at a time, and served as a live radio stream.

It has to hear the record anyway, so ripping and streaming came free: the same chunks that are
fingerprinted are written to a FLAC or WAV file (optionally split at the side's track gaps) and,
if you switch it on, offered as a radio URL that Music Assistant or any other player can use.

```
mic/line ──▶ noise gate ──▶ 15 s clip ──▶ Shazam (shazamio) ──▶ MQTT discovery
```

The **Documentation** tab beside this page is the add-on's own guide (install, options, the web
interface, ripping and the stream). The [repository
README](https://github.com/matthamiltonnz/vinyl-ear#readme) has the long version, including why
the noise gate and the track splitting behave the way they do and the Lovelace cards that show
the sleeve next to the camera.
