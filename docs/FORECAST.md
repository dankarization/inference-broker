# Read-only queue forecast

The broker queue is the sole scheduling authority. Forecast is a bounded,
read-only simulation of that scheduler, not a lease, readiness proof or
wall-clock SLA. It never changes queue state, source weights, pause or fences,
and does not contact an external owner, executor or inference backend.

## Contract

`next_selections` and `conditional_scenario.next_selections` are identical
compatibility views of one scenario across all currently eligible sources;
`basis` is `all_eligible_sources`. The default is 10 selections, maximum 20.
Source weights, FIFO lanes, source-local Nextcloud new-upload promotion and
bounded actual-model turns use the same selection logic as real dispatch.
Durations are estimates: new admissions, cancellations, retries, policy changes,
executor availability and actual non-preemptive runtimes can change the order.
The projection stops at a source interval boundary and reports
`source_schedule_recheck_required` instead of guessing future eligibility.

`dispatch_holds` reports observed pause and executor fences without claiming
their future release. A paused forecast applies only after resume. A fence may
allow compatible reuse; its presence alone does not prove global unavailability.

Unauthenticated HTTP and dashboard projections exclude producer-private jobs
and explicitly say so. They are hypothetical without those competitors, not a
prediction of global dispatch. To avoid leaking private activity through shared
model turns, source budgets or execution estimates, they simulate fresh scheduler
accounting, not the live global accounting. Separate public partial indexes
prevent a deep private backlog from consuming the public preview. Authenticated storage users
may request the all-job view. Payloads and credentials are never forecast data.

## Snapshot consistency and limits

The observer uses a query-only SQLite WAL snapshot, a 750 ms deadline and no
dispatcher mutex. It discovers at most 512 raw model lanes, seeks one head per
lane and at most one indexed successor per projected selection. An incomplete
competitor set or a database error yields unavailable, not a partial ranking.

The all-job scheduler accounting is copied only after claim/completion commit
and anchored to the audit sequence. An older WAL snapshot paired with newer accounting, or
vice versa, invalidates only forecast, not otherwise valid dashboard counters.
Admissions alone do not invalidate accounting. Rollback restores cycle, turn,
active-attempt and duration state; optional publication failure never repeats
or prevents an already committed job. The public and private views use the same
consistent WAL snapshot and apply visibility predicates at query time.

## Validation

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=.:tests python3 -m unittest \
  test_forecast_owner_gate test_source_model_batches test_batch_scheduler
node tests/dashboard_dom.cjs
```

Tests use synthetic SQLite data and fake executors. Updating source does not
deploy code or change any running queue.
