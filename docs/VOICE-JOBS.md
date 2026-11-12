# Sequential voice jobs (phase 1)

The broker admits `local-voice` at reciprocal weight 2, with the same one-slot, non-preemptive scheduler, lease, attempts, forecast and status as other jobs. Merge the `local-voice` entry from `config/sources.production.json` into the actual policy; do not replace other source settings. No caller has been switched.

Profiles are server-owned: `voice-wyoming-v1`, `voice-gigaam-v1`,
`voice-whisper-gpu-v1`, `voice-whisper-gpu-large-v3-v1`. Submit JSON to
`POST /v1/jobs`:

```json
{"source":"local-voice","profile":"voice-gigaam-v1","kind":"transcribe","external_id":"unique-request-id","source_item_id":"unique-file-id","payload":{"filename":"audio.ogg","audio_base64":"<base64 file bytes>"}}
```

Audio must be nonempty and at most 8 MiB decoded; filename is 1–128 ASCII letters, digits, `.`, `_`, or `-`. Wyoming accepts only mono 16 kHz PCM16 WAV; convert before submitting. HTTP workers accept formats supported by their own service. Do not send URLs, paths, raw model names or endpoint credentials. The POST response includes the broker job `id`. Poll `GET /v1/jobs/{id}` until `completed`, `failed`, or `cancelled`; completed `result` contains the worker response including `text` (Wyoming returns `{"text": ...}`). Inspect actual response/status before integration; existing external/source item IDs support broker idempotency rules. `POST /v1/jobs/{id}/cancel` marks a running job cancel-requested; it is not preempted.

Server configuration: `BROKER_WYOMING_HOST`/`BROKER_WYOMING_PORT` (defaults 127.0.0.1/10300); `BROKER_GIGAAM_URL` and `BROKER_GIGAAM_TOKEN`; `BROKER_WHISPER_GPU_URL` and `BROKER_WHISPER_GPU_TOKEN`; `BROKER_WHISPER_LARGE_URL` and `BROKER_WHISPER_LARGE_TOKEN`. URLs are worker base URLs; HTTP clients POST raw file bytes to `/v1/transcribe`. GigaAM retains its original no-query contract. Both Whisper lanes omit forced language/profile query parameters so the worker auto-detects language. Each HTTP lane requires its reported model identity: `gigaam-v3-e2e-rnnt`, `whisper-medium-int8-mainpc-cuda`, or `whisper-large-v3-mainpc-cuda` respectively. A Large endpoint returning medium is rejected. Each lane fails closed if its own URL/token is missing; no lane falls back to a sibling worker. Worker credentials remain server-side and are never stored in the repository. Ensure network reachability and start/configure workers separately. The broker does not manage Wyoming service startup. Calls timeout at the profile's 1800-second socket timeout; the lease covers that deadline plus the normal broker margin. Neither HTTP interface exposes a cancellation endpoint in the inspected clients; Wyoming cancellation is likewise not assumed. The broker retains the slot until the call returns or the lease expires, matching existing Ollama behavior. Timeout, disconnection, process death or expired lease does not prove that the remote GPU has been released; residual GPU use after a killed worker is a known phase-1 limitation. Voice dispatch does not unload Ollama or manage resident models, so GPU memory availability and worker lifecycle must be arranged operationally. No durable Whisper fence exists in this broker tree.

Inline audio and transcript are stored in the broker database, subject to the configured source retention policy; protect access and choose retention accordingly. This contract does not deploy, start workers, or change Archivist/OpenClaw callers.

The standalone `clients/broker_voice_client.py` submitter is disabled unless `BROKER_VOICE_ENABLED=1`. Call `submit(path, backend, broker_url, external_id=stable_id, source_item_id=file_id)` with backend `wyoming`, `gigaam`, `whisper-gpu`, or `whisper-gpu-large` (the latter maps to `voice-whisper-gpu-large-v3-v1`). Persist the external ID before submitting and reuse it with identical inputs when recovering an ambiguous result. Transient connection failures retry with the same ID; HTTP errors are returned to the caller. The matching broker revision must be deployed first: earlier revisions do not deduplicate `local-voice` submissions. This client does not activate or modify the Archivist queue, poll for a result, or change the server policy. Weight 2 is server-owned and is never sent in the submission.


Raw native segments remain in the queued result, available to clients requesting
JSON/SRT; the broker does not synthesize timestamps or fall back to CPU/cloud.
Responses exceeding 2 MiB are rejected rather than accepted after truncation.
