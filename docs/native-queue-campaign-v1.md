# Six existing OpenClaw queued presets — finite TEXT plan / confirmed blocker

This artifact is separate from the completed nightly-eval source/profile lane.
It changes no server, existing profile, source, model, policy or OpenClaw config.
No inference/admission/deploy is authorized or performed by preparation.

## Scope and exact existing configuration

Existing broker queue admission is `POST /v1/jobs`, source `openclaw`, kind `chat`.
The native provider endpoint `/openclaw/api/chat` reaches the same Broker.submit /
execute / storage path. It is not a separate privacy-preserving queue. Async jobs
return UUIDs immediately and support exact-key GET `/v1/correlations` recovery.
No new source or eval profile is needed merely to enqueue these models.

| Existing profile | Model | Profile ctx/output caps | Configured provider think | Explicit initial baseline ctx/output/think |
| --- | --- | --- | --- | --- |
| openclaw | qwen3.8:ad-iq2-xs | 131072/16384 | low | 131072/16384/low |
| openclaw-qwen3-vl | qwen3-vl:30b | 212992/16384 | max | 212992/16384/max |
| openclaw-nemotron3 | nemotron3:33b | 131072/8192 | true | 131072/8192/true |
| openclaw-nemotron-nano | nemotron-3-nano:30b-a3b-q4_K_M | 262144/16384 | true | 262144/16384/true |
| openclaw-gemma4 | gemma4:12b | 262144/16384 | true | 262144/16384/true |
| openclaw-gemma4-26b | gemma4:26b | 262144/8192 | true | **32768/512/false** |

Caps are live deployed source metadata. Configured thinking and the Gemma26
agents.defaults.models override are main's explicit config observation in 7549,
not a reconstructed or copied global config. Gemma26 provider caps/think are not
its effective per-model override. Each request explicitly includes context,
num_predict, temperature 0 and think; no global alias or defaults are modified.
The campaign is not artificially limited to common 32K context.

Six pinned installed digests were checked by GET `/api/tags`; see the proof receipt.
A configured baseline may exist despite missing `/show` values. That does not
prove native off support or execution success. Low/max remain thinking levels.
Preparation does not block other baselines because one model lacks toggle metadata.
Extras require accepted exact model/digest `/api/show` boolean false+true controls,
change only think with the same options, and total at most two extra modes x
four cases (8 calls). Baseline is 6 x 4 = 24, lifetime planned ceiling 32.
Draft metadata/settings may be edited before freeze; frozen plans are revalidated
against their complete draft and exact case manifest on every load. Do not reset
or fork a campaign journal to make additional calls. This blocked adapter does
not create an execution journal or make any POST.

## Strict acceptance blocker, not a ready-to-run comparison

The existing deployed server cannot meet the end-to-end final-only persistence
and job-scoped effective-parameter evidence contract without server changes:

1. `openclaw.validate_result` accepts native message.thinking/reasoning strings.
   `Broker._execute` serializes the full result and UPDATEs jobs.result_json;
   `storage.record_result` hashes the same full native object. Both synchronous
   native chat and generic async jobs share this path. Client-side filtering
   happens after server persistence and cannot prevent it.
2. GET OpenClaw job status returns the full result; no OpenClaw final-only
   projection exists. The Nextcloud projection explicitly rejects this source.
   OpenClaw's default status also hides input_hash/input_bytes; the protected
   compact-status endpoint is distinct and requires its existing runtime auth.
3. Existing OpenClaw dispatch records no job-bound native digest/effective context,
   output ceiling or executed think evidence. It clamps requested options by the
   profile but does not prove native runtime effective settings. GET native ps is
   a current global snapshot, not durable evidence tied to a completed job, and
   another queued producer may already have switched the resident model.

These are confirmed source/contract facts, not hypothetical risks. The focused
probe uses immutable deployed base modules and synthetic fake executor results:
raw reasoning is retained and returned; an intentionally mismatching native ps
context/digest does not prevent OpenClaw completion. No real inference is used.

Therefore `gate.admission_enabled=false`, `deploy_needed_for_full_acceptance=true`.
`offline_prepare_deploy_needed=false` applies only to inspecting/freezing requests,
not permission or capability to run the full accepted comparison. `admit` always
returns exit 2 before networking/state creation, with no override/fallback.
At-most-one, durable journal/reconcile/counter execution acceptance is deliberately
not claimed for a blocked adapter. Existing queue capability is proven separately;
there is no replacement queue, retry, watcher or hidden admission.

If the intended privacy scope is *only new helper files* and unchanged existing
server retention is explicitly outside it, a client-only final filter is feasible,
but that is a narrower acceptance contract. Likewise requested/clamped settings
can be reported without claiming observed per-job native effectiveness. This
artifact does not silently substitute either interpretation or relax the gate.

## Finite offline commands for main (not executed by code owner)

Use this accepted source export as PYTHONPATH. No new Python environment/install.
Use main-owned private paths and keep the original frozen fixtures unchanged.

```sh
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH='/path/to/accepted-native-queue-source-export'
RUNNER="$PYTHONPATH/scripts/native_queue_campaign.py"
CASES='/media/dankar/Plex/for claw/telegram-nightly-model-evaluation-20261028/fixtures/manifest.json'
# Existing private directory, fresh exclusive paths; never recreate main state.
DRAFT='/main-owned/private/path/native-campaign-draft.json'
FROZEN='/main-owned/private/path/native-campaign-frozen.json'
python3 -B "$RUNNER" prepare --campaign telegram-nightly-20261028 --out "$DRAFT"
# Main may review/edit requested budgets within caps; no automatic model selection.
python3 -B "$RUNNER" dry-run --draft "$DRAFT" --cases "$CASES"
python3 -B "$RUNNER" freeze --draft "$DRAFT" --cases "$CASES" --out "$FROZEN"
python3 -B "$RUNNER" request --frozen "$FROZEN" --cell case-01-porta.openclaw-qwen3-vl.configured-baseline --out '/main-owned/private/path/one-request.json'
python3 -B "$RUNNER" admit  # exit 2, always, no network or execution state
```

Dry-run reports only pinned metadata/settings/request hashes and blocked status,
not case text. Request artifacts are private 0600/exclusive and for review only,
not a curl replay path. Frozen manifest SHA:
`5f8046556912ad1e7c09239717c315eb4cde39f889e1640dfb72fca6f7cad0e8`.
Only manifest plus four case-*/messages.json objects are opened; role/content
remain unchanged, references/rubrics/source documents/media and URL targets are
never opened. No raw response, reasoning, credential or cloud fallback exists in
planner output. Actual observed effective settings remain null, not guessed from
caps, preset configuration, token estimates or later residency snapshots.

## Minimal missing server contract (proposal only)

A separately authorized broker change would need source-scoped opt-in final-only
projection *before persistence*, stable job UUID + durable input hash correlation,
and per-job allowlisted digest/requested/effective native evidence (unavailable
values explicitly null). Existing non-comparison OpenClaw behavior should remain
unchanged. A helper cannot install this as a local fix, source-policy mutation,
metadata flag or prompt instruction. No such broker change/rollout was made.

Rollback/retirement: no runtime state changed. Select the original completed
37eecb1 runner/source artifact for that previous lane. Retain this source/proof
package for reviewing the exact confirmed blocker; no production rollback needed.
