# Four-model pilot adapter (source-only)

Machine contract: `pilot-doc100-contract.json`. This candidate combines the
Nextcloud Gemma26 verifier adapter with two additional installed vision chat
profiles. It does not install a runner, submit inference, change source policy,
or qualify a model. Existing Nextcloud producer routes, Bolt/native and Gemma12 /
Nemo profiles remain distinct. Helper verifier HEAD cb7a94a is unchanged.

## Capacity and provenance

Pilot requires source `pilot-mainpc`, kind `chat`, and a dedicated one of the four
evaluation profiles. Generic/native/Nextcloud profiles cannot borrow this lane.
Server-owned ctx8192/output2048 applies to the Qwen2B and Gemma pilot profiles;
the deployed Qwen30 pilot profile retains ctx16384/output8192. All four use
temp0/thinkfalse/streamfalse. Nextcloud Gemma26 generate uses ctx16384/output4096. Four user images,
24MiB decoded total, 32MiB normalized image request JSON, 2MiB text-only JSON,
16 system/user messages, no tools/history/producer refs. These bounds apply AFTER
HTTP JSON parsing: `/v1/jobs` has no global preparse Content-Length hard cap.
Do not expose the existing LAN service to untrusted callers based on these bounds.

The pilot admits any number of active jobs within its 512MiB normalized payload
byte budget (including base64), counting queued/running/cancel_requested. There
is no job-count cap; the byte budget full returns429; exact keyed replay still
returns the retained job. Closed or unknown source returns403. All400 doc100 jobs
fit if existing bytes + actual planned bytes <=512MiB, source is open, and shared
storage guards pass. Large images can require bounded admission/collection
sweeps. Never borrow the Nextcloud byte budget, change source names, or bypass
queues. Cloud admission/completion is independent of all local outcomes,
including backpressure and failures.

There is NO global hard active count/byte/spool quota. The common disk admission
guard reserves max(configured minimum (default2GiB),5% total filesystem); storage
rejection currently returns400. Configured terminal inline budgets (default
128MiB/source and768MiB global) apply ONLY when retention is enabled, not as an
active queue cap. WAL/DB size targets are maintenance targets, not hard quotas.
Actual live policy/free space/occupancy must be read by main before admission.

The digest table is an intended installed snapshot, not per-dispatch digest
attestation. Local `/api/show` metadata confirms installed Gemma12/Gemma26 vision
capability, not semantic quality. Main must recheck exact tags/digests before
qualification, and record actual job, prompt/input/frame hashes and model metadata.
Qualification remains empty; completion or model size never establishes approval.
Audio transcripts are not supported by this pilot contract.

## Operator route (main only, not executed)

1. Review combined diff against deployed e9bfc; preserve separate dashboard work.
   The incremental e489 patch assumes the Nextcloud Gemma26 adapter already exists.
2. Use established immutable broker release/backup procedure; freeze admissions,
   inspect active jobs/correlations, preserve DB/policy, prove source parity before
   authorized restart. No SQL migration, new dependency, weights, units or default
   priority change is required by this adapter.
3. Recheck intended installed model digests/capabilities from the machine contract
   with metadata APIs. Keep source priority/weight unchanged; main explicitly opens
   pilot admission only when authorized and budgets/free space permit.
4. Persist a DOC100 plan outside the broker BEFORE POST: exact document/source/
   version/frame hashes, prompt+hash, normalized payload hash/bytes, profile/model/
   intended digest, immutable external_id/source_item_id, pending/unknown/jobID.
   Use `PilotClient.submit` once per saved intent; no automatic POST retry.
   On timeout/crash/unknown outcome call `recover`; exact match may attach, ambiguity
   or absent history remains unresolved, never a license to blindly repost.
5. Canary one approved frame through EACH four profiles via the real client. Main
   records jobIDs, attempt/source/model/options, actual timings and durable terminal
   status. Check strict input correlation, result done/model/content and truncation;
   do not approve or publish merely because transport completed. No canary executed
   by this source task. Nextcloud verifier qualification uses its independent route
   and migration005 publication contract; this adapter does not change migration005.
6. Admit the400 saved intents with count AND byte backpressure; collect independently,
   reconcile on restart. An exhausted count is not silently skipped. Never block the
   independent cloud lane on local terminal state. A no-submit qualification runner
   is separate follow-up, not provided or claimed here.

## Rollback

Close new pilot/Nextcloud verifier admissions under main authority. Drain or use
the authorized broker cancellation workflow for new Gemma pilot/verifier jobs
before restoring e9bfc, which does not know their profiles/projection. Do not edit
history, delete the DB, reset quarantine or silently resubmit intents. Preserve all
jobIDs and source evidence; reconcile old existing external_id duplicates explicitly.
Restoring older code removes new pilot idempotency/caps, so stop the DOC100 producer
first. Recheck pending-job continuity and health after the authorized rollback.
