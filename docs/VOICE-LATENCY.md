# GPU voice latency and request-local Ollama residency

## Conditional idle-residency amendment

Immediately before sending a **broker-owned Ollama inference** request, the
broker checks for queued GPU voice work in enabled sources. This includes
Windows voice workers, diarization, the voice contour and native queued ASR.
CPU-only and Wyoming jobs do not request this hint. This read neither claims
work nor selects, filters, prioritizes or gates the scheduler.

When such demand exists, the request uses
`keep_alive = min(profile.keep_alive_seconds, 30)` seconds. Otherwise its
profile default is unchanged (often 300s, but some profiles use 120/180/1800s).
Explicit model preloading/readiness retains the normal profile TTL: loading
with zero TTL could make readiness impossible. Caller overrides stay forbidden.
No additional API call unloads or refreshes an existing resident model.

This explicitly amends the **idle TTL** portion of fixed-default execution
contracts, including Nextcloud Qwen64, nightly and Syncopia summary/daily.
Pinned model identity, context, thinking/output limits and result validation
are unchanged. The JSON contract fixtures continue to describe normal defaults.
A completed result with an override stores broker-owned `_broker_residency`
metadata. Completed Nextcloud `request_contract.keep_alive` reports the actual
stored execution TTL, including after a future policy change. Before execution
that projection describes the normal default, not a guarantee of future TTL.
Consumers must not assert the default TTL for a completed conditional request.

Why 30s, not zero: Qwen64, nightly and summary require a native post-inference
`/api/ps` identity/context sample, with a bounded five-second read. Immediate
unload would invalidate that evidence. The positive grace permits those reads;
if expiry, transport failure or foreign activity invalidates evidence, the
existing checks still fail closed. Thirty seconds is **not** proof of measured
unload latency or a guarantee against a delayed response.

The voice guard still polls until `/api/ps` proves no Ollama residents, checks
worker capabilities/idleness, fences before upload and clears only after
request-owned release proof. An out-of-band caller may extend residency. A
voice job arriving after an LLM request was sent cannot change its existing TTL.
An old resident therefore can still cost the old timeout on the first handoff.

## Queue delay is separate

Running LLM work is non-preemptive. Source weights, source-time budgets,
source-local model turns, FIFO, forecast and GPU custody are unchanged.
In particular, a newly ready lower-weight source on a different model may
follow the established one-job model-switch boundary. A long active job plus
that boundary can outweigh any residency improvement. This patch does not
promise voice-start deadlines or remove the observed queue wait.

Making several chunks ready together can avoid producer gaps and repeated
source re-entry, but does **not** create an atomic meeting batch. Existing
fairness and fences still apply, and each worker request retains its release
proof. A producer/proxy admission change requires its own scoped handoff.

## Timing and impact

`voice.execution_timing` logs job ID/profile, outcome, durable queue-wait seconds,
monotonic execution seconds, worker-call count, fence-pending status and
accumulated stages: `ollama_residency_wait`, `worker_preflight`, `worker_http`,
`worker_release`. Missing stages were not reached; contour attempts accumulate.
Failure paths emit the same metadata, without media, transcripts, tokens or raw
worker errors. Telemetry is best-effort and cannot change execution outcome.

Synthetic virtual-clock tests force twelve LLM/voice switches at a 300s normal
TTL: residency wait changes **3600 -> 360 seconds**, native voice work remains
**20 seconds**, and model loads remain twelve. This proves the mechanism, not a
live performance result. No voice demand preserves defaults and hot reuse.
With pending voice, an LLM reuse gap longer than 30s can cause an extra cold load
versus the old TTL (synthetic 31s-gap test: one load becomes two). Native GPU
load/runtime impact requires an authorized live
trial; no production inference or user recording replay belongs to unit tests.

Ollama's documented `keep_alive` is request-scoped idle residency:
https://docs.ollama.com/api/generate and https://docs.ollama.com/api/chat.

## Additive deploy and rollback

Do not replace a deployed release with an older public checkout. Capture the
active release identity/hashes, drain using the existing supported procedure,
back up source/control state, and apply only the reviewed source hunks plus
`broker/residency.py` to a separate release. Preserve all other release47 code,
credentials, policy and dependency paths. Test that exact combined tree offline
before the authorized owner switches/restarts it. No schema migration is needed.

After deployment, verify health, the exact source hashes and metadata-only
stages/actual TTL on **new authorized** requests. Observe LLM post-inference
identity evidence, loads and fence clearance; stop if evidence or release fails.
Do not replay a private recording as a benchmark. Rollback by returning to the
saved untouched release, using the same supported drain/custody procedure.
This document and patch do not themselves authorize production mutation.
