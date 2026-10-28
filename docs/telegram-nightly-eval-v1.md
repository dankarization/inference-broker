# Telegram nightly text evaluation — source v1 / partial metadata v2 (prepare-only)

## Ownership and immutable scope

Main owns accepted real case bytes, matrix, source policy, activation, admissions,
collection and blinded Astra review. This patch does not run a comparison,
qualify a nightly pipeline, or choose/change a production model. Tests use
synthetic contracts, not replacements for the four reviewer-owned real chats.
No watcher, scheduler, background collector, download or cloud fallback exists.
TEXT ONLY: document/photo/video/audio acquisition or analysis is outside this
contract. URLs inside chat content stay literal text; no URL target is fetched.

Dedicated source: `telegram-nightly-eval-20261028-v1`.
Do not borrow, close or reopen `pilot-mainpc`, `nextcloud`,
`syncopia-telegram-memory`, or `openclaw` to run this comparison.

| New profile | Exact installed model |
| --- | --- |
| `telegram-nightly-qwen38-v1` | `qwen3.8:ad-iq2-xs` |
| `telegram-nightly-gemma12-v1` | `gemma4:12b` |
| `telegram-nightly-gemma26-v1` | `gemma4:26b` |
| `telegram-nightly-nemotron-nano-v1` | `nemotron-3-nano:30b-a3b-q4_K_M` |

All new profiles: **text-only, kind chat, freeform, num_ctx 32768,
num_predict 8192, temperature 0**, concurrency 1, keep_alive 300 seconds,
executor timeout 1800 seconds. Explicit native thinking mode is required.
No tools, assistant history, images, JSON schema/format or producer-storage refs.
No truncation/context shift. Input byte cap 128 KiB, final cap 128 KiB.
Byte caps do not prove token occupancy. Existing 31 profiles, their limits,
model routes and all existing source-specific guards are unchanged.

Admission is fail-closed without an explicit dedicated enabled/admitting policy.
One outstanding job per dedicated source; **at most 32 distinct jobs over this
source's lifetime**, counting retained tombstones. Idempotent replay requires
identical profile/kind/item/hash and is checked before policy/budget gates.
This lane refuses individual and bulk retries: failed/unknown/length outcomes
consume the original cell; there is no make-up cell or automatic resubmission.
The helper also commits one durable intent per cell and never POSTs it again.
The 32-call budget includes native thinking within each 8192-token generation
ceiling, not 8192 final tokens plus a separate hidden reasoning allowance.
Do not delete/reset its journal or create parallel journals to evade this rule.

## Native metadata is a prerequisite, not a claimed result

Observed GET-only tags are pinned in `broker.nightly_eval.MODEL_DIGESTS`.
Do not infer thinking controls from model names, OpenClaw's permissive enum,
a library's latest artifact or documentation alone. Native `/api/show` metadata
must be collected by main for **these exact local artifacts** and accepted.
A v2 freeze retains unknown models as blocked planning rows; only known controls
can produce requests. `/api/show` is a read-only metadata operation but uses POST;
this source-only task made no such POST. GET `/api/show?...` returned 405 on
Ollama 0.40.11. Main subsequently observed boolean false/true controls for
Gemma12, Gemma26 and Nano, but no thinking object for QwenAD despite a thinking
capability. Capability is not a values/default contract. Missing metadata blocks
only that model, not the other 24 known cells. No Qwen controls are invented.

Official contracts: <https://docs.ollama.com/capabilities/thinking>,
<https://docs.ollama.com/api/show>. `thinking.values` and `thinking.default` are
authoritative. `false` means off only when native values contains boolean false;
`low` is always a named thinking level, never off. Always-thinking artifacts must
show off unsupported, with their real native levels explicitly retained.
Choose two supported named levels for two actual comparison cells, or keep one
unsupported-off slot (which is never submitted) and one supported level. A matrix
with fewer than 32 supported cells is reported honestly, not padded/retried.

Main may use `runner/capture-native.py` **once under its own metadata authority**
to capture only model/digest and projected known/unknown thinking metadata,
tags/version GETs plus four
`/api/show` metadata POSTs. It never calls chat/generate/load/unload. Read the
output, then select modes; do not assume the example below fits actual artifacts.
Main's command (not run here):

```sh
python3 '/media/dankar/Plex/for claw/telegram-nightly-model-evaluation-20261028/runner/capture-native.py' --out "$NATIVE"
```

Set PYTHONPATH to the accepted broker source first, as in the CLI section.
Neither capture nor dispatch downloads or inspects documents/media/URL targets;
only the four installed model control metadata records are requested.
The broker repeats exact digest/native metadata checks at dispatch **before any
load/run** and refuses metadata drift. After inference it verifies exclusive
resident digest and effective context via GET `/api/ps` equals 32768.

## Main-owned input schemas

The runner accepts only Astra's exact immutable fixture manifest:

- Path: `/media/dankar/Plex/for claw/telegram-nightly-model-evaluation-20261028/fixtures/manifest.json`
- SHA-256: `5f8046556912ad1e7c09239717c315eb4cde39f889e1640dfb72fca6f7cad0e8`
- Four cases / four chats / 87 selected primary messages (not complete histories).
- Model-facing paths, exclusively: `case-01-porta/messages.json`,
  `case-02-kp/messages.json`, `case-03-pm/messages.json`,
  `case-04-s92/messages.json`.

Pass this manifest directly as `--cases`; do not translate it to a new cases.json.
The manifest hash plus each message file hash are pinned in the helper. Every
freeze/load revalidates them and rejects alternate/changed/symlinked inputs.
Only the manifest and these four JSON objects `{"messages":[...]}` are opened;
their `messages` arrays alone become model input. Reference/rubric,
structured case.json, source files and proofs are not opened, copied or sent.
There is no glob, attachment resolver, document analysis, URL dereference, or
prompt reconstruction. Literal URLs remain in the original role/content text.
Exactly the same parsed role/content bytes are used for all models and modes.
The frozen plan records hashes + absolute paths, not chat text. Inputs/ground
truth stay outside Git, under main/reviewer ownership. Do not edit the fixtures.

Offline case-only validation (no native metadata, matrix or model calls needed):

```sh
export CASES='/media/dankar/Plex/for claw/telegram-nightly-model-evaluation-20261028/fixtures/manifest.json'
python3 "$RUNNER" verify-cases --cases "$CASES"
```

`native-v2.json` contains all four exact model/digest records. Capture projects
known records as `metadata_state:known` with exact values/default, and unknown
records as `metadata_state:unknown, thinking:null` plus a fixed reason
`thinking_object_missing` or `thinking_metadata_invalid`. Observed
`thinking_capability` is a boolean or null; it never grants native mode support.
Raw templates/modelfile/invalid values/errors are never saved. Unknown is not
unsupported and does not borrow controls from another model.

```json
{"schema":"telegram-nightly-native-v2","models":[
  {"model":"EXACT_TAG","digest":"UNPREFIXED_TAG_DIGEST","metadata_state":"known","thinking":{"values":[false,true],"default":true},"thinking_capability":true,"unknown_reason":null},
  {"model":"QWEN_EXACT_TAG","digest":"QWEN_UNPREFIXED_DIGEST","metadata_state":"unknown","thinking":null,"thinking_capability":true,"unknown_reason":"thinking_object_missing"}
]}
```

These are record-shape examples, not a two-model input or guessed defaults.
The native-v1 format remains accepted for fully known records only. Capture
uses exclusive output in an existing directory, so an already-existing main-run
folder is safe and an existing file/symlink is never replaced or read as auth.
Transport/tag/digest failures still abort capture without an output; a valid
show object lacking controls does not discard other models' metadata.

`matrix-v2.json`: exactly four entries with the four new profile names. Each has
exactly two modes. Boolean example **only if native metadata supports both**:

```json
{"schema":"telegram-nightly-matrix-v2","source":"telegram-nightly-eval-20261028-v1","models":[
 {"profile":"telegram-nightly-qwen38-v1","modes":[
  {"label":"off","think":false,"supported":true},
  {"label":"on","think":true,"supported":true}
 ]}
]}
```

A named mode uses e.g. `{"label":"level-low","think":"low","supported":true}`.
An unsupported off slot is `{"label":"off","think":false,"supported":false}`.
Known support flags must exactly match accepted native metadata or freeze fails.
For a model with unknown metadata, both proposed mode slots use `supported:null`,
not false, with ordinary proposal labels (off/on are proposals, not support claims):

```json
{"profile":"telegram-nightly-qwen38-v1","modes":[
 {"label":"off","think":false,"supported":null},
 {"label":"on","think":true,"supported":null}
]}
```

Unknown models require matrix-v2; legacy matrix-v1 remains valid for fully known
inputs only. A new freeze emits `telegram-nightly-frozen-v2`, with known/unknown
observations, `supported:true/false/null` and statuses `supported`,
`unsupported_native_mode` or `blocked_native_metadata`. Unknown rows have null
payload hash/bytes/external key/item ID, never an executable request. With main's
observations the result is **32 planned / 24 supported / 8 blocked**, not 8
unsupported Qwen rows. Neither off_unsupported_models nor unsupported_modes
includes the unknown model. Request/admit/reconcile/collect reject those rows
before any HTTP call or journal intent. Broker dispatch still independently
requires actual native controls and fails closed before load/run.
There are exactly 4 models x 2 modes x 4 cases = 32 planned cells, never more.
Model outputs never change the accepted plan or reviewer ground truth.

## CLI: bounded, manual, one exact cell per invocation

Set PYTHONPATH to the accepted immutable source export/worktree, not a stale
system checkout. No dependency installation is required (Python >=3.11).
Use private main-owned paths; no real auth, inputs, outputs or state in Git.

```sh
export PYTHONPATH='/media/dankar/Plex/for claw/telegram-nightly-model-evaluation-20261028/broker-worktree'
export RUNNER='/media/dankar/Plex/for claw/telegram-nightly-model-evaluation-20261028/runner/evaluate.py'
umask 077
export CASES='/media/dankar/Plex/for claw/telegram-nightly-model-evaluation-20261028/fixtures/manifest.json'
python3 "$RUNNER" verify-cases --cases "$CASES"
python3 "$RUNNER" dry-run --cases "$CASES" --matrix "$MATRIX" --native "$NATIVE"
python3 "$RUNNER" freeze --cases "$CASES" --matrix "$MATRIX" --native "$NATIVE" --out "$FROZEN"
python3 "$RUNNER" request --frozen "$FROZEN" --cell "$CELL" --out "$PRIVATE_REQUEST"
```

These commands perform **zero network operations**. They validate the finite plan,
print counts/hashes, and create exclusive (never overwrite) files. They do not
send inputs or print content. `CELL` is e.g.
`case-01-porta.telegram-nightly-gemma12-v1.off`; it is listed in `FROZEN.cells`.
Request output is optional for human inspection, not a curl replay mechanism.

After separately authorized activation, exact runtime contract readback, explicit
source registration/enablement by main, and main's acceptance of case/native files:

```sh
python3 "$RUNNER" admit --frozen "$FROZEN" --cell "$CELL" --journal "$PRIVATE_STATE/journal.sqlite3"
python3 "$RUNNER" collect --frozen "$FROZEN" --cell "$CELL" --journal "$PRIVATE_STATE/journal.sqlite3" --out "$PRIVATE_RESULT"
```

`PRIVATE_STATE` must have mode 0700. Runtime bearer auth, if required, uses
`--token-env NAME` reading an existing environment variable: no token argument,
commit, frozen artifact or log. Endpoint defaults to `http://192.168.2.19:8088`;
only that LAN host or loopback is accepted; proxies/redirects are disabled.
Before POST, `admit` GET-verifies `/v1/nightly-eval/contract` against the frozen
contract and GET `/v1/sources` for only its dedicated source's enabled/admitting
policy. It never mutates policy and performs exactly one job POST after a
SQLite FULL-synchronous intent commit. The minimal returned UUID + expected/observed
input hash receipt is committed **before** optional identity/result validation.

If interrupted, timeout, invalid JSON, lost ACK, HTTP error, empty correlation,
or a crash between intent/POST/receipt: **never admit that cell again**. Run:

```sh
python3 "$RUNNER" reconcile --frozen "$FROZEN" --cell "$CELL" --journal "$PRIVATE_STATE/journal.sqlite3"
```

`reconcile` uses only GET exact dedicated-source/external-key correlation (limit
2) and then GET the resulting UUID. It refuses absent/ambiguous matches and
never treats an empty search as proof that admission failed. A known recorded ID
is collected by GET directly; no broad jobs/history scan, no cross-source IDs,
no retries or automatic polling. An unresolved intent blocks subsequent cells
until main resolves the uncertain admission without inventing a new cell/key.
A new invocation is required for every status check and admission.

`collect --out` creates a private allowlisted JSON artifact after durable journal
storage; stdout prints only ID/state, never final text. The journal is bound to
one frozen hash + broker endpoint. Legacy frozen-v1 known plans retain their
original keys and remain readable; do not rewrite them as v2 or reset/rebind a
journal. A new v2 frozen hash cannot reuse an old journal. This correction does
not create a new source, expand the source lifetime cap or authorize make-up
calls. Main's current source-only preparation had no frozen admission journal;
any future unknown-to-known change needs main review, not automatic replacement. No content from HTTP errors/native thinking/
reasoning fields is persisted or printed, including nested malformed metadata.
Broker storage itself is final-only for the new lane, not merely CLI filtering.
Inline reasoning channel markers in purported final content fail closed.

Exit 0: requested bounded operation succeeded (a status may still be pending).
Exit 2: blocked/unknown/invalid contract; inspect journal and GET-reconcile only.
Exit 3: terminal failure/cancel/expiry, empty final, unknown done_reason or
`done_reason:length` — **not an acceptable quality result** and no retry.
Retain failure/truncation evidence separately from Astra quality scoring.
A terminal rejected result still releases the next independent planned cell.

## Metadata semantics

Allowlist includes queue_seconds and execution_seconds (job timestamps), native
load_duration/total_duration/prompt_eval_duration/eval_duration (nanoseconds),
prompt_eval_count, eval_count, final_tokens, done_reason, truncated and acceptable.
`eval_count` can include thinking tokens; it is **not** relabeled final tokens.
`final_tokens` is null unless the backend explicitly supplies that scalar.
`request_options` proves what the broker sent; `runtime_context_length` is
independently checked from `/api/ps`. `effective_output_limit` is null: native
metadata does not independently expose the num_predict ceiling. A `stop` reason
and count are not proof of semantic completeness. Final quality remains Astra's.

## Activation proposal and preservation gates (main only; not executed)

1. Re-read current live unit, HEAD/tree receipt and release bytes. The prepared
   base was 24a5b69f3f0166dbceef90db84537fdc1eec48e6, not either stale home checkout.
   Compare against the supplied live-before manifest. If live source changed,
   preserve/review every addition; never blanket-copy an older tree over it.
2. Keep the new source absent or disabled/admission false. Preserve all existing
   source weights, dispatch sources, queue, DB/token/policy file locations and
   unrelated unit/drop-ins. No OpenClaw configuration changes are needed.
3. For an authorized broker rollout, use its normal `systemctl --user reload
   ollama-inference-broker.service` drain (ExecReload targets MainPID only).
   Wait for the current lease to settle without consuming the backlog. Coordinate
   existing producers; do not close/reopen another source to obtain a quiet lane.
4. Capture SQLite transaction-consistent backup and before queue evidence (IDs,
   source/profile/kind, input hash/bytes, FIFO order), policy SHA and unit/env paths
   in main's protected runtime backup area. No DB or credentials in this bundle.
5. Export the **accepted commit** with `git archive`, stage an immutable new release,
   verify manifest hashes, retain the old release. Run local compile/contract
   validation against the clean staged bytes. Under separate restart authority,
   switch only the broker release path using the established broker drop-in
   procedure and activate once. No schema migration/new dependencies required.
6. Verify readiness and `/v1/nightly-eval/contract`, deployed-byte parity, preserved
   queue inputs/order and unchanged existing policy/weights. Only after accepting
   preservation evidence, main registers the proposal's one new disabled source
   by merge (not by replacing the policy). Main may then deliberately enable this
   source and add its exact name to dispatcher allowlist while retaining all
   existing names; keep weight 1. Do not infer dispatcher access from admission.
7. Main owns all admissions/collection and closes only this source after the finite
   matrix. No independent activation or source registration occurred in this task.

Rollback: stop new dedicated admissions, let its single active job settle,
collect all recorded IDs. Preserve journal/results/queue DB. Before restoring the
old source release, settle/cancel only this source's remaining queued jobs under
main authority (old code does not know the new profiles). Restore the previously
verified broker release/drop-in and previous dispatch allowlist, restart once
under authority, verify health + unchanged queue/policy. Remove/disable only the
new policy entry under main authority; never restore a stale whole-policy file
over concurrent edits. Do not overwrite or roll back the live SQLite DB simply
to remove this additive source-only code.
