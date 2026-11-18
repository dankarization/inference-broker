# Read-only forecast and the Nextcloud owner gate

The forecast is a projection, never a lease, readiness proof or wall-clock SLA.
No forecast request changes queue state, source weights, pause, fences or owner
state. The observer uses a separate query-only WAL snapshot, its existing 750ms
deadline and no dispatcher mutex, network/owner query or inference.

## Ordinary projection

Without an eligible Nextcloud lane controlled by the external owner gate,
`next_selections` retains its existing bounded (default 10, maximum 20) contract.
The canonical `conditional_scenario.next_selections` contains one projection
for all eligible sources. In the ordinary case the legacy `next_selections`
is an identical conditional compatibility alias; it is not a guarantee. The
same selector works on private model-turn/time-budget copies with estimated
non-preemptive execution times. Cycle reconstruction now uses only that snapshot,
not live dispatcher accounting. Scheduler state is published after claim/completion
commit and anchored to the existing audit sequence. An older WAL reader paired
with newer memory (or the reverse) rejects only the forecast with an explicit
retry reason; otherwise valid dashboard counters remain live. Admissions alone
do not invalidate scheduler accounting. Rollback restores all private cycle,
model-turn, active-attempt and duration state before any later publication.
Later admissions, cancellations, retries, source
schedules/policy and executor readiness can change the actual selections.
If a source is delayed or a selected profile imposes a source interval, the
projection stops before the first interval-dependent selection and reports
`source_schedule_recheck_required`; it does not guess newly eligible work.
Public-only projections exclude private jobs and are explicitly hypothetical,
not a global order. At most 512 distinct raw lanes are inspected; exceeding
that cap returns unavailable rather than silently omitting competitors.

`dispatch_holds` reports observed global pause and executor fences without
claiming their future release. A paused forecast applies **only after resume**.
A fence may allow compatible warm reuse; its presence alone does not prove
global executor unavailability. There is no owner/release RPC.

## Conditional Nextcloud scenario

The old implementation returned global `unavailable` whenever any eligible
Nextcloud row existed: local FIFO/model heads cannot reproduce the external
owner-wide eligibility, minimum tier or effective weight. That gate remains
authoritative and unchanged.

The dispatcher now publishes a **payload-free forecast observation only after
the claim transaction commits**. It contains the already owner-filtered minimum
tier, effective weight (7 for primary, 10 for deferred), base-policy signature,
read timestamp and a bounded candidate preview. It excludes the claimed job.
No new table, persisted state, migration or owner-protocol change is needed.
Optional evidence capture/publication errors cannot prevent a committed job
from executing; the forecast is invalidated rather than repeating the claim.

The preview keeps independently bounded all-job and public-job views: at most
20 rows per raw profile/model/local-priority class and 512 rows in each view.
Private rows cannot consume a public lane's quota. If either aggregate cap is
exceeded, no truncated lane set is ranked. The observer revalidates each complete expression-index key, queued
state, attempt, external/item identity and priority. Public reads also use the
public partial index; private job identifiers, payloads, owner credentials and
aggregate details never enter the response. New admissions are never silently
adopted into the observed tier, and successors come only from this preview.

When a Nextcloud candidate exists:

- `next_selections: []` deliberately does **not** pretend a scenario is the
  actual global dispatch order.
- `conditional_scenario.next_selections` contains the bounded simulation.
  `basis: last_owner_gate` means **if the observed owner eligibility/tier/weight
  stays valid**, using that observed weight, not the configured base weight.
  On exhausting this observed tier, the sequence stops with
  `stop_reason: owner_recheck_required`; the next tier is unknown.
- If no valid observation exists, `basis: nextcloud_excluded` projects other
  sources **only if Nextcloud is excluded**, not their unconditional positions.
- `owner_gate` includes observation state/time/age. After 60 seconds it is
  explicitly `stale`, still only a hypothetical last-read scenario, never live
  authority. Timestamps are not refreshed by completions/unrelated publications.
  No observation exists after process restart until a real gate read occurs.
- A legacy observation without an independently captured public view is
  `public_preview_unavailable` rather than proof of an empty eligible tier.
- If previously captured public rows all become private, the observer reports
  `public_preview_changed` rather than claiming that the tier is exhausted.
- Gate failure, failed claim transaction, changed policy/clock, changed captured
  job identity or aggregate overflow prevents an owner-ranked scenario.
- `unranked_candidates` shows up to the requested limit of current local
  Nextcloud raw-lane heads. This is a **non-exhaustive unranked preview**, not
  fallback FIFO or proof those jobs can dispatch. Already scenario-ranked IDs
  are excluded; the preview is still non-exhaustive.

The API and dashboard always expose these conditions together with the rows.
The contingency paragraph is visible (it was previously hidden). UI renders
scenario basis, observation age/state, stop boundary, holds and a separate
unranked lane-head table with textContent, not HTML. There is no fabricated
exact rank or ETA. Database observer errors still report real unavailable/stale
state; they are not converted to a fabricated empty queue.

## Validation

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=.:tests python3 -m unittest \
  test_forecast_owner_gate test_source_model_batches test_batch_scheduler
node tests/dashboard_dom.cjs
```

Tests use synthetic SQLite data, fake owner gates and fake executors.
A source update does not deploy code or change any running queue.
