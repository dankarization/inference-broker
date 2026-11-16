# Optional broker-side diarization contract

This tree includes `voice-diarization-v1` admission, validation and GPU custody
integration. The matching native worker is a separate component: its presence,
model quality and real shared-queue readiness are not established by importing
the broker profile. Nothing is enabled or deployed by this document.

Use existing source `local-voice`, kind `transcribe`, and bounded durable
external/source-item IDs. Input is whole normalized mono 16kHz FLAC (at most
32 MiB), filename `audio.flac`, plus `diarization` exactly matching
`broker.diarization_contract.OPTIONS`. The options pin the model revision,
weights SHA, worker portable SHA, offline variant, CUDA device and eight-speaker
limit. Do not substitute another format or options silently.

Private operator configuration: `BROKER_DIARIZATION_URL` and
`BROKER_DIARIZATION_TOKEN`. There is no default URL, reused token, caller-selected
endpoint, cloud/CPU fallback or second dispatcher. Worker traffic disables
proxies and redirects. Existing `/v1/status`, `/v1/transcribe`, `/v1/release`
request-owned semantics and the durable GPU guard are mandatory. A result alone
does not prove process-tree release; uncertain release retains the fence.

Output `stt-diarization-v1` must attest exact CUDA/model/weights/options, input
FLAC SHA-256, decoded duration/timebase and bounded speaker activity intervals.
Speaker IDs are anonymous, overlap is retained; this does not invent names or
word timestamps. See the pinned constants and validator in
`broker/diarization_contract.py` for the authoritative wire fields.

Later OGG/MP3/FLAC acoustic protocol extensions and atomic local-voice hold work
from separate branches are not included here. Broker integration does not
publish the separate worker or provide E2E/model-fitness evidence.

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=.:tests python3 -m unittest \
  test_diarization test_voice test_voice_gpu
```
