# Accepted Astra stage1: existing native queue, code-only delivery

This supersedes the six-preset inventory plan for the selected comparison; it
does not rewrite prior archives, source profiles, aliases or frozen fixtures.
Selection SHA e82cb85ae3109ff1c1c1cb797c8881a5b208b7e2b22bde4ffd9488adf7d9716c.
Only that exact selection and the exact four pinned TEXT messages inputs are
accepted. Selection/reference/rubric are never part of the model-facing payload.

16 strict sequential cells: QwenAD low, Gemma12 true, Gemma26 true, Nano true;
within each block KP -> PM -> Porta -> S92. Explicit native wire request uses
`think` (not provider `thinking`), num_ctx65536, num_predict8192, temperature0,
keep_alive5m, freeform/no format/tools/media, no truncation/shift. Unknown Qwen off
is not scheduled. Stage2 is not constructed or admitted: it needs later main
acceptance after blinded review, at most eight off calls within the same lifetime
ceiling32, never a reset/second journal or automatic budget-filling matrix.

## Server contract: distinguish existing behavior from a new blocker

No NEW option-override/correlation blocker was found for this stage1. Existing
native POST `/openclaw/api/chat` accepts explicit model/options/think and keyed
streaming; it returns X-Broker-Job-Id before frames. Provider aliases are not read
by this native broker handler. Existing profile caps all permit65536/8192; the
server uses min(requested,profile cap) and keep_alive300s (same as5m). Focused
exact-base tests prove Gemma26 reaches the fake executor as65536/8192/true/300s,
not its agent alias32768/512/false. No global config change or broker rollout is
needed for those options or the native receipt route.

Previous report mixed two different requirements:

- Existing raw thinking persistence is REAL, not new: OpenClaw validates native
  strings then stores full result_json and returns raw GET job results. New helper
  does not read native streamed frames; GET raw JSON is held only in memory and
  projected to final/scalars before journal/output. No reasoning/raw body/error
  is printed or written by this helper. Server retention is unchanged and belongs
  to main. If end-to-end server no-reasoning persistence is mandatory, this remains
  a separate HIGH privacy acceptance blocker, not an option/correlation bug.
- Job-bound effective native digest/context/output/think evidence is absent on the
  existing route. Selection expressly permits UNVERIFIED; receipts therefore
  keep effective_settings=null/effective_status=UNVERIFIED. Source-clamped expected
  controls and pending normalized payload verification are not presented as native
  runtime proof. This is a MEDIUM evidence limitation, not a mandatory rollout
  gate under the accepted selection. Native tokenizer/rendered-template fit and
  actual generation/final-token split remain unmeasured when not exposed.

For the strict server privacy extension, minimal separate code acceptance would
be opt-in final-only projection BEFORE jobs.result_json/storage.record_result,
consistent final-only status output and regression tests for the native route.
Likely changed files: broker/service.py (pre-persistence result projection),
broker/http.py (explicit safe projection/native route contract), broker/openclaw.py
(pure validation/projection helper), tests/test_openclaw_route.py and new privacy
regressions. Preserve other native agent behavior by explicit opt-in, no new source
or impersonation. Job-scoped evidence, if separately required, belongs in
service._execute and an allowlisted HTTP projection, never fabricated by a client.
No such server code or deployment is part of this candidate.

## Durable finite helper

The helper sends ONLY native chat POST, never /v1/jobs or direct Ollama. It commits
FULL-sync intent before POST and UUID/hash receipt immediately on response header,
BEFORE closing the keyed stream without reading raw frames. The existing keyed
disconnect contract retains the job. One outstanding job and strict preceding-cell
terminal checks apply; there is no watcher, loop, retry or automatic next cell.

Unknown POST/invalid/lost UUID header remains an intent: GET-only exact
source=openclaw/external-key correlation (limit2) and then GET recorded UUID.
Absent/ambiguous correlation never permits replay. Public job hash is unavailable
after payload cleanup; receipt distinguishes pending-payload hash verification
from keyed-local-hash-only terminal evidence. Profile/source/key/UUID/attempt
identity is checked; response bodies are projected before durable collection.
Length/empty/error terminal execution is separate from semantic grading and
blocks further admissions pending main review. Each journal is bound to one frozen
hash and endpoint. Existing Journal code enforces lifetime32 and one outstanding;
this overlay exposes only16 slots and no automatic stage2 transition/rebind.

## Exact CLI (main only; not executed against production by code owner)

```sh
ROOT='/media/dankar/Plex/for claw/telegram-nightly-model-evaluation-20261028'
export PYTHONPATH='/path/to/accepted-source-export'
export PYTHONDONTWRITEBYTECODE=1
RUNNER="$PYTHONPATH/scripts/selected_native_queue.py"
SELECTION='/media/dankar/Plex/for claw/syncopia-chat-prompts-astra-review/local-model-selection-20261028/selection.json'
CASES="$ROOT/fixtures/manifest.json"
FROZEN='/existing-main-owned-private-directory/selected-stage1.json'
JOURNAL='/existing-main-owned-private-directory/campaign.sqlite3'
umask 077
python3 -B "$RUNNER" dry-run --selection "$SELECTION" --cases "$CASES"
python3 -B "$RUNNER" freeze --selection "$SELECTION" --cases "$CASES" --out "$FROZEN"
CELL='s1-01'
python3 -B "$RUNNER" request --frozen "$FROZEN" --cell "$CELL" --out '/fresh-private-path/one-native-request.json'
# Only after main accepts the handoff/retention boundary and authorizes admission:
python3 -B "$RUNNER" admit --frozen "$FROZEN" --cell "$CELL" --journal "$JOURNAL"
python3 -B "$RUNNER" collect --frozen "$FROZEN" --cell "$CELL" --journal "$JOURNAL"
# Lost ACK / interrupted header receipt: NEVER repeat POST:
python3 -B "$RUNNER" reconcile --frozen "$FROZEN" --cell "$CELL" --journal "$JOURNAL"
# Save projected final to a fresh exclusive file with collect --out PATH.
```

Default broker-root base URL is http://192.168.2.19:8088; the helper appends
/openclaw/api/chat. Do not pass provider base containing /openclaw as --base-url.
Optional --token-env EXISTING_ENV_NAME avoids literal auth arguments/logs. Outputs
are exclusive/private; no auth copies. Fixture/reference/rubric/selection bytes
are unchanged. Every actual admission/failed attempt counts; no silent resubmit or
replacement campaign. Frozen overlay is not proof of inference or ops approval.
