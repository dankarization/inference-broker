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

## Shared staged voice contour (contract56302)

Media Loader defaults to queued profile `voice-contour-v1`, source `local-voice`.
The existing voice execution lease encloses CPU → selected Windows → cloud;
there are no nested broker submissions, direct client-to-Windows calls or
separate queues. Payload example:

```json
{"filename":"audio.ogg","audio_base64":"<base64>","source_duration_seconds":2911.424,"windows_model":"large","language":"auto","response_format":"segments-v1","local_only":true}
```

`windows_model` defaults medium and selects ONLY second tier. `language` defaults
auto; `local_only` defaults false independently of model/timestamps. Existing
low-level Windows lanes remain compatible and run under the same queue.

CPU uses the existing private native-CPU adapter bridge, gated when original
source duration exceeds600s, FLAC admission is too large, configuration is
unavailable or its transport cannot honor explicit language. The original
source duration survives chunking: splitting a48min video does not make it
eligible for CPU. Known adapter terminal failure can proceed to Windows;
ambiguous durable-adapter outcome remains an error requiring reconciliation.

Windows Whisper auto language sends no query; explicit language requires
`/v1/status.language_override=true` BEFORE audio upload, then query language and
matching result.settings.language. Missing capability is a known gate, not
permission to ignore the user's language. GigaAM supports auto/Russian only.
Known rejection/unconfigured worker may proceed to cloud, but ambiguous
transport timeout does not start another inference/spend. Native segment
capability is required for segments-v1 at whichever tier succeeds.

Cloud runs ONLY last and ONLY when local_only=false. Broker-owned settings:
`BROKER_CLOUD_STT_TOKEN` (or existing OPENAI_API_KEY), root URL
`BROKER_CLOUD_STT_URL` (default https://api.openai.com), text model
`BROKER_CLOUD_STT_MODEL` (preserves gpt-4o-mini-transcribe), timestamp model
`BROKER_CLOUD_STT_SEGMENTS_MODEL` (whisper-1 verbose_json only until its documented
2027-02-26 retirement). No caller URLs/tokens/models are accepted. Missing cloud
credential is an explicit final-tier failure, never fabricated success. No
cloud call occurs on local-only; stage policy is checked again at execution.
Legacy text-only cloud models cannot generate native timestamps. Segments are
validated for finite bounds, order and transcript reconciliation, not invented.

Result `contour` records schema/tier/local_only/requested_language/windows_model
and ordered attempts with CPU gate reasons. Requested policy and actual worker
reported/detected language are separate facts. All input/result artifacts retain
the same broker IDs and source policy. Do not change source weights/admission,
restart unrelated active jobs or replace a live Archivist release to deploy this
additive profile: use a reviewed delta against the actual live revision.
