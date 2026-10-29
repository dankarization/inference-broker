# CPU segment bridge: source candidate only

This adds the async `voice-native-cpu-v1` profile for the immutable consumer
`79f58d22ef83d41e4a70fa423b455bb8af241826`. It is **disabled by default**;
no policy or deployment changes are included. This is not a GPU timing route,
new synchronous broker endpoint, or concurrency/VRAM qualification. Existing
voice profiles and CPU canonical consumer selection remain unchanged.

## Contract

A configured process serves local metadata at `GET /v1/voice/capabilities` with
one `segments-v1`, FLAC, 8 MiB profile. The endpoint makes no network calls or
model wake. If not configured it returns 503 and the consumer falls back before
binding. The reported revision is the server's `BROKER_RELEASE_REVISION` value:
**the release operator must independently verify that it equals the exact source
and deployed bytes**. Capability is self-report, not attestation of loaded code,
route enforcement, model, or the real availability of native timing.

Submit through existing `POST /v1/jobs`, source `local-voice`, kind `transcribe`,
with stable external_id and source_item_id, and payload `filename: audio.flac`,
`audio_base64`, `response_format: segments-v1`. The adapter checks the FLAC magic
and bounds bytes, while the existing CPU adapter performs actual media validation.
The existing payload-sensitive broker idempotency and UUID are preserved. Poll
`GET /v1/jobs/{id}`; after terminal payload cleanup `input_hash` is retained for
correlation. Only genuinely returned, validated adapter timings are forwarded;
missing, empty, wrong-hash, GPU, nonlocal, invalid or mismatched segments fail.
No silence, word alignment, or segment timestamps are synthesized.

The bridge uses a server-owned private literal HTTP address, no environment proxy,
redirect or caller-supplied endpoint, and selects `local-only-v1` and
`segments-v1` on the existing authenticated durable adapter. It checks its
loaded `/health`, source job ID, audio hash, policy, format, result status, route,
transcript, timing/backend/device/model and weights evidence before returning the
consumer provenance envelope. The adapter's distinct durable idempotency key is
`broker-voice-<broker UUID>` and survives an explicit retry of the same broker job.
The bridge does not change or claim delivery of the adapter's retained result.

Source-only configuration for a *future separately reviewed activation*:
`BROKER_CPU_VOICE_ENABLED=1`, `BROKER_RELEASE_REVISION=<exact 40-hex release>`,
`BROKER_CPU_STT_URL=http://<private-literal-ip>:<port>`, and
`BROKER_CPU_STT_TOKEN_FILE=<absolute private owned file>`. These settings are not
provided or enabled here. Existing `local-voice` admission remains closed until
main accepts the consumer, policy, runtime and lifecycle gates. A private network
address and adapter's self-report do not independently prove all transitive
routing is local or authenticate an unencrypted LAN peer; review topology and
adapter identity separately before use. Do not place the bearer token in URLs,
arguments, source, or proof artifacts.

## Activation blockers and recovery

The historical CPU native route source exists, but the supplied prior report says
real native segment attempts had failed; this change has only synthetic loopback
tests. Independently verify loaded CPU adapter source/environment, endpoint and
local-only policy, real timing/model provenance, compatibility with the accepted
consumer's private capability pin, durable result retention, CPU memory/headroom,
and transport trust before new admission. Keep CPU canonical route and producer
HOLD untouched until separately approved. `local-voice` uses server-owned weight 2.
The profile retains unknown/exclusive occupancy, and this change does not permit
parallel broker workers or claim that idle Ollama has freed GPU VRAM.

A lost POST, polling/network failure, cancellation, broker crash or lease expiry
can leave an adapter-side queued/running job after the broker records a failure.
The existing broker does not fence, cancel, or reconcile that remote CPU worker,
and the CPU adapter has its own queue. Do not treat a failed broker attempt or
idle GPU as proof of remote termination. Do not auto-repost a new ID or switch
backends after uncertain admission. Reconcile the same `broker-voice-<UUID>`
adapter identity/retention and establish terminality before any retry or rollout;
remote lifecycle proof remains a separate **blocking** production gate.

Offline contract check against the exact consumer checkout, reusing its existing
interpreter (the test uses synthetic data and loopback only):

```
PYTHONDONTWRITEBYTECODE=1 /path/to/existing/python -B scripts/verify_voice_consumer_contract.py /path/to/nextcloud-agent
python3 -B -m unittest tests.test_voice_cpu -v
```
