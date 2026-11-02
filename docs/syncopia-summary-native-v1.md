# Versioned opt-in native-Boolean memory summaries

Source-only change. No deployment, producer mutation, admission, model
installation or queue-policy change is authorized by this document.

The existing syncopia-memory-qwen38 normalizer and daily/nightly contracts
remain byte-unchanged. These two new profiles use the EXISTING source
syncopia-telegram-memory and kind=chat, inline text only:

| Profile | Exact model | Context ceiling | Output ceiling |
|---|---|---:|---:|
| syncopia-summary-gemma12-v1 | gemma4:12b | 131072 | 65536 |
| syncopia-summary-bolt-e4b-v1 | nemotron-3.5-bolt:e4b | 65536 | 32768 |

These are profile ceilings, NOT default trial settings, effective runtime
observations, proof of occupied-context VRAM fit or recommendations to use
maximum output. Context/output/temperature must be explicit on every request.
No streaming, tools, media, schema format, keep_alive override or extra options.
Exactly two messages, System then User, each with only role/content. Exact
UTF-8 content is copied, never sliced, normalized or summarized by this adapter.
External/source-item IDs are required bounded durable identities.
Exact queued/completed/failed retries reconstruct deterministic normalized input
and pinned admission proof, then compare the complete durable identity under the
DB lock, without fresh metadata. They return the original receipt even if native
metadata is now offline/drifted; this never authorizes execution. New keys still
require live preflight before INSERT; dispatch preflight is unchanged.
Changing profile, kind, source item or
normalized input under the same source/external ID rejects. Collisions with old
memory jobs reject in either direction; legacy-to-legacy behavior is unchanged.

## Wire contract

Use POST /v1/jobs only after main authorization (not executed in this work).

    {
      "source": "syncopia-telegram-memory",
      "profile": "syncopia-summary-gemma12-v1",
      "kind": "chat",
      "model": "gemma4:12b",
      "options": {"num_ctx": 65536, "num_predict": 8192, "temperature": 0},
      "external_id": "synthetic-unique-example",
      "source_item_id": "synthetic-source-example",
      "payload": {
        "messages": [
          {"role": "system", "content": "Synthetic system."},
          {"role": "user", "content": "Synthetic source."}
        ],
        "tools": [], "stream": false, "think": false
      }
    }

Top-level model and optional payload model must both match the selected
profile. Options may be top-level or payload, but conflicting duplicates reject.
Model/options cannot escape the versioned contract through the generic override
stage. False is JSON Boolean false, NOT "false", "low", 0, null, omission or true.
The old Qwen path still requires "low"; it is not translated to off.

Server-owned truncate=false/shift=false are stored and forwarded. They are
requested native controls, not proof that a backend retains every prompt token.
A final's semantic completeness still requires evaluation.

## Resource rule and honest limits

Let M be canonical compact-JSON UTF-8 bytes of the complete messages array.
Admission requires:

- M <= 196608;
- positive integer context/output within the selected profile ceilings;
- temperature numeric zero (not Boolean);
- output + 4096 wrapper reserve < context;
- M + output + 4096 <= context.

M is a deliberately conservative scheduling proxy, NOT tokenizer output.
Rejection does NOT prove that the exact native tokenizer cannot fit the input.
A successful proxy check does NOT establish runtime token counts or VRAM fit.
No request is silently clamped, truncated or converted to smaller output.

Bolt's 65536 output / 65536 context leaves no input room and rejects before
execution. Other large-output/verbose-source cells can also reject under this
proxy. Keep every planned case in the comparison ledger with the exact reason.
Do not skip those cells or call them submitted. A less conservative admission
mechanism needs a separate reviewed exact native-token measurement contract;
do not weaken this version after receipts exist. Changing context/output for a
fit trial creates a new declared comparison, not mutation of frozen cases.

Model-native thinking is an unavoidable comparison factor: Qwen uses low,
Gemma/Bolt use Boolean false. Bolt context cannot match a 131072 Qwen/Gemma
case; any 65536 context proposal is explicitly different, not a runtime clamp
or an identical-options match. Preserve these differences and every blocked
case in the ledger. Do not create duplicate Qwen jobs to rebuild a comparison.

## Native artifact and capability evidence

Pinned installed manifest digests:

- Gemma12: 4eb23ef187e2c5462566d6a1d3bbbc2f1346d0b4327cbb66d58fffbcc9b2b05c
- Bolt E4B: 69f2be39b309ade35ef04ee02507915f4cd839f6631e14425146cb6c9df3bd39

Prior installed metadata for exact Gemma digest advertises thinking.values
[false,true]. Bolt's exact artifact observation and native renderer contract
are documented in bolt-e4b.md, with context 65536 and Boolean controls.
Current tag identity alone is not current thinking-capability proof.

Official API semantics: https://docs.ollama.com/capabilities/thinking states
false requests no thinking only when supported, and unsupported named levels
fall back to defaults. Hence "low" cannot mean false for these models.

Admission AND dispatch independently read native tags/show metadata with bounded
deadlines, no wake/load/run/download. They require exact digest, native
gemma4.context_length (Gemma262144/Bolt65536), completion+thinking capability,
and exactly Boolean thinking.values=[false,true] in either order. No inference
from capabilities alone; missing/string/numeric metadata fails closed. Defaults
are irrelevant to explicit false and are not silently substituted.

Stored summary_contract records native identity/capability and admission budget.
Dispatch first verifies persisted payload hash/bytes against admission identity,
then recomputes and compares this immutable contract before wake/model run.
Even a same-byte-length message change rejects before metadata/wake.
Only that private field is removed from executor payload. Exact requested
options and false reach the executor. After execution, observed resident
context/model digest are checked and recorded separately from requested options;
mismatch fails. Unexpected thinking/tool output fails. Quality_GO is always
false: stop/length and semantic evaluation remain caller responsibilities.
Historical GET reads persisted state without contacting native metadata.

## Review/deployment plan — main owner only

1. Review exact source commit and synthetic tests, including the unchanged
   historical profile snapshot hashes. No dependencies/schema migration needed.
2. Finish all planned Qwen cases under their existing source pins, including
   accepted compact12 after Astra input-invariant review. Do not abandon Qwen,
   cancel/replay old18/36 or edit their UUIDs/pins. Their historical GET remains.
3. Before a separately authorized activation, record live service WorkingDirectory,
   exact deployed file hashes/HEAD, queue/active-job identities and source-policy
   hash. Back up SQLite consistently with the SQLite backup API plus deployment
   artifact/config, without overwriting production state or exposing secrets.
4. Run read-only native metadata preflight for both exact installed digests,
   capabilities and context. If unavailable/different, do not install models
   or reinterpret native modes; return the blocker.
5. Prepare an immutable release from the reviewed HEAD using the existing host
   deployment procedure; compare file hashes. No new policy/default model/source
   weights or global config edits. Drain through the existing controlled release
   process without cancellation. Deploy/reload/restart requires separate exact
   main authorization; none was exercised by the programmer.
6. Verify health and source policy unchanged, old historical GET identity,
   deployed SHA and profile contracts before any new admission. No live POST
   is part of the deployment smoke checks.
7. Create a NEW producer campaign/adapter with explicit new profile and source
   pins; do not patch historical broker pins. Require Astra exact-input
   acceptance. Reuse all existing Qwen references, mirror SAME complete
   messages/arms on Gemma/Bolt, retain explicit resource blockers. Admission,
   semantic follow-up and any changes to requested comparison settings belong
   to main. Current compact_matched drafts are proposals, not an admission client.

## Rollback and state continuity

Before new-profile jobs exist, main can roll back to its exact prior artifact
after authorized drain; do not restore a database snapshot just to roll back
code. No schema changes exist.

After new jobs exist, first stop only NEW producer intake, not the shared source
or old Qwen pipeline. Allow accepted new jobs to reach terminal state and
collect durable results by GET. Preserve all IDs/intents/payload hashes and the
current SQLite database. Do not cancel/replay jobs or revert state to erase them.
Do not activate an old binary while these profiles are queued/running: it does
not know how to execute them. Keep the reviewed release for historical reads
until rollback compatibility is reviewed; never assume old-client display logic
understands new execution evidence. If safe rollback cannot preserve custody,
hold the new release with intake stopped and hand off to main.

## Verification

Tests use temporary synthetic SQLite and injected native adapters only. They
cover both native models, exact messages/options, wrong source/model/mode/schema,
input room/ceilings, metadata identity/drift, dispatch and actual context,
old low contract unchanged and historical GET without metadata. Original 31/35
profile snapshot hashes are retained; only these two additive profiles are
excluded from those historical inventories.

    PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest discover -s tests

No inference performance or model-quality acceptance is claimed.
