# Optional queued diarization candidate — 2026-11-14

Source only, not installed or deployed; no live queue/DB changes or inference.
Companion Whisper branch: `feat/optional-nvidia-diarization-20261113`.
That repository's `docs/DIARIZATION.md` defines CLI state, output/partial alignment,
model pins, runtime proposal and isolated-test gates.

Adds `voice-diarization-v1` under existing `local-voice` / `transcribe`. Payload:
`audio_base64` (whole normalized mono 16 kHz FLAC, <=32 MiB), `filename:audio.flac`,
`diarization` must exactly match `broker.diarization_contract.OPTIONS` (pinned
model revision, official Q8 GGUF SHA, Windows CUDA v0.3.0 portable SHA,
`v3-offline`, `cuda:0`, eight speakers).
Require external/source-item IDs. Existing payload hashes, idempotency,
retention tombstones, source weights, admission, fair scheduling and durable
job/attempt/event journals are reused without schema changes.

Server-owned future operator configuration:
`BROKER_DIARIZATION_URL`, `BROKER_DIARIZATION_TOKEN`. No default worker URL,
token reuse, caller-selected endpoint, cloud/CPU fallback or second dispatcher.
Worker must provide existing request-owned `/v1/status`, `/v1/transcribe` and
`/v1/release` semantics. This profile rejects execution without the existing
durable queue GPU guard. The guard waits for natural Ollama release, fences
before upload, and clears only with matching request-owned release proof.
Completed result does not itself prove release; uncertain release retains fence.

Output `stt-diarization-v1`: exact CUDA/model revision/weights hash/options,
input FLAC SHA-256, decoded-audio duration/timebase and bounded speaker activity
intervals. Anonymous speaker_0..speaker_7 (or unknown), overlap retained.
No invented names, word timestamps, per-ASR-chunk speaker resets or 4-speaker cap.
The worker candidate now uses native Windows NeMo-Speech.cpp CUDA portable
with request-owned kill-on-close Job Objects; no NeMo/WSL Python stack. Positive
CUDA backend and complete process-tree release proof are mandatory at the wire
boundary. Official compatibility is established, but actual native and real
shared-queue trial evidence must be obtained before calling it E2E PASS. No additional alignment model is included.

The actual deployed dashboard release differs from the feature branch base.
Use the [offline additive candidate and gated rollout plan](DIARIZATION-ADDITIVE-ROLLOUT.md),
not a whole-release replacement. It preserves all current dashboard/priority/
native-summary bytes except the exact documented admission/profile anchors.
No deployment/restart is authorized by that preparation.

Tests: `python3 -B -m unittest tests.test_diarization tests.test_voice tests.test_voice_gpu`.
Synthetic temporary databases/mock workers only; not evidence of model fitness.
