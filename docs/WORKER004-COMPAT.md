# Worker004 compatibility on a07b359 (main-owned rollout only)

This additive adapter recovers the Qwen/projection portions of prior 7fe0d37 onto
current a07b3594d4303c1b83b48fef3272a50c776d59c5. It does not revert isolated
pilot evaluation, small VLM or Bolt. No policy/config/schema/service activation.

Supported source/profile/kind: nextcloud / nextcloud-eval-qwen / generate only.
Model qwen3-vl:30b, think=true REQUIRED, num_ctx16384, num_predict8192,
temperature0, stream=false, keep_alive300s, execution timeout900s.
1..4 base64 images, <=2MiB decoded each, prompt1..16384 bytes. Fixed options
may be omitted and normalized; widening, extra fields and bool-as-number are
rejected. No Gemma/Nemotron eval profiles; worker matrix must select Qwen only.
Installed model digest/quantization and approved prompt must be independently
verified/pinned by main in the worker matrix; broker does not attest model digest.
No direct Ollama/cloud fallback or new model downloads.

Legacy nextcloud-qwen3-vl normalization/replay remains unchanged, including its
old two-outstanding-source-jobs guard. Only nextcloud-eval-qwen admits a bounded
bulk backlog: at most256 nonterminal Nextcloud jobs and256MiB inline input bytes
across that source. Existing exact replay precedes capacity checks. Other source
weights/limits/scheduling/storage/retention/order/pending jobs are unchanged.
Legacy admission may return429 while evaluation occupies >=2 source jobs.
Weight7/enabled/admission still requires existing main-managed source policy.
The maximum is a finite broker spool guard, not an instruction to fill it; worker
retains its own lower queue/cache budgets.

POST /v1/jobs?projection=nextcloud-final-v1 and
GET /v1/jobs/ID?projection=nextcloud-final-v1 opt into final-only wire output.
Default endpoints remain unchanged. Projected identity: id,source,profile,kind,
external_id,source_item_id,state,timestamps,input_hash/input_bytes. Eval projection
also names fixed request_contract schema/model/options/think/keep_alive. Result
contains typed final response/model/done/done_reason, bounded integer metrics and
boolean error ONLY. No payload, thinking/reasoning, messages or raw errors; nested
values are not copied. This is a wire projection, NOT a change to existing broker
internal result retention. Invalid/length/empty/wrong-model finals remain failures
in worker004; projection never manufactures a successful final.
Unknown/duplicate/blank projection is rejected before admission. Non-Nextcloud
projection rejected; missing jobs remain404. Correlations and idempotence unchanged.

## Main preflight / rollout

1. Verify sealed hashes/manifest and candidate ancestry, reconcile current live
   source against basea07. If live has advanced, preserve it; do not overwrite
   unreviewed changes. Back up authorized broker release/config/SQLite using the
   existing safe procedure, quiesce dispatch/admission before snapshot as needed.
2. Record ordered pending job IDs, source/profile/input hashes, current release
   and external policy hash. Keep Nextcloud weight7, other lanes and job identities.
3. Adopt clean immutable candidate source via existing broker unit/deploy mechanism;
   this package does not install units or supply a new config. Main alone owns
   drain/restart, readiness, deployed-byte parity and pending continuity checks.
   Do not copy scratch/test SQLite into production. No new DB migration needed.
4. With worker timers still disabled, verify source enabled/admission/weight7 and
   approved Qwen model digest, principal/epoch, exact canary ACL/source metadata.
   Use source004 RUNBOOK-WORKER.md validate/stage/feed/collect/status/readback on
   the already reviewed photo+video canary. A queued ACK is not a valid final.
5. Require both durable PG finals with exact job IDs/final hashes, strict model/
   stop/nonempty gates, honest frame coverage; then demonstrate continued supply
   over multiple sweeps before enabling reviewed timers. No real canary was run
   during source verification; synthetic HTTP client proof uses no live PG.

## Rollback

Stop feeder timer/service first, preserve all intents/correlations/job IDs/PG rows
and active caches. Drain evaluation jobs using this compatible broker+collector
before reverting to a07; basea07 cannot dispatch the new profile or serve its
projection. Never revert with queued/running new-profile jobs, delete/replay them,
or repurpose the old profile. Restore prior authorized broker release via main's
backup path after drain; verify pending continuity, policy and old routes. If a
compatible drain cannot be completed, stop and retain state for reviewed recovery.

## Verification

Use existing Python>=3.11 and stdlib; no install/build/environment needed. Run tests
in isolated Plex TMPDIR with fake GPU/WOL and external sockets forbidden. Receipt
commands/counts/exits/source manifests accompany the immutable candidate. Relevant
boundaries: legacy Nextcloud, new adapter, isolated pilot, native Bolt/small VLM,
generic compatibility, producer-owned storage and retention. Full historical
suite/production throughput/model quality are not claimed.

scripts/test_worker004_contract.py --worker-tree PATH --scratch PLEX_PATH executes
the actual unchanged worker004 producer/collector client against a loopback broker,
Qwen-only ten-cell synthetic matrix, final-only collection and correlation recovery.
This does not claim a real10-object pilot, persistent PG runtime or live canary.
