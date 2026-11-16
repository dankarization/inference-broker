# Source budgets and bounded actual-model turns

With source policy, each ready source has one reciprocal-weight execution-time
budget in a recurring 3600-second horizon:

`B_s = 3600 * (1 / weight_s) / sum(1 / weight)`.

Multiple actual models do not create additional source shares. Without policy,
the scheduler uses global FIFO. Hard gates (pause, source eligibility, schedule,
GPU custody and availability) apply before model affinity. An optional Nextcloud
owner gate determines its authoritative eligible tier and effective weight
before source/model selection.

Within each source, compatible profiles targeting the same actual model share
a FIFO lane (`queued_at,id`). A finite FIFO of model turns prevents a third
model being continually overtaken by newly arriving models. The current turn
rotates at the first applicable claim boundary:

- after 10 claimed attempts, including failures/cancel-requested attempts;
- after 300 charged seconds, including setup/model switching;
- after a competing lane has waited 600 seconds since last service or its
  first queued head (including time spent executing another source);
- when the current lane is empty or ineligible.

A single model remains work-conserving; a later competitor can trigger immediate
rotation at the next boundary. Actual running work is never interrupted, and
its full elapsed time is charged even when it exceeds a turn or source budget.
A source still owed time runs before a new horizon is started; a long first
source must not reset the horizon and starve its peer. Ready-source changes and
policy reloads rebase remaining time without erasing charged execution.
Nextcloud new-upload priority is source-local and precedes historical lanes.

These bounds do not establish a wall-clock per-job SLA: attempts are
non-preemptive, other sources have their own shares, and gates can withhold a
lane indefinitely. For a fixed finite set of M continuously eligible models,
a lane's next turn follows at most `10 * (M - 1)` claims of that source.

## Observability and costs

Audit metadata records source/model decisions without request bodies. Forecast
uses private copies of scheduler state and the read-only WAL snapshot, never
advancing the real cycle. See [conditional forecast](FORECAST.md).

Covering and partial indexes avoid visiting payload-heavy history pages for
ordinary dashboard counters. Queued raw model heads are discovered lazily;
private rows have a separate public partial index. Observer reads keep their
750ms deadline and do not take the dispatch mutex. History uses bounded pages
and scalar requested/observed model extraction, not public request/result bodies.

This does not make all costs constant: source discovery remains O(history),
the dispatcher retains a full eligible-candidate scan, lane cardinality matters,
and initial index creation can require disk space and startup time. Rehearse
schema/index work on a consistent offline backup before a separate rollout.
Do not restore an old database over newly admitted jobs as a code rollback.

## Synthetic regression checks

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=.:tests python3 -m unittest \
  test_source_model_batches test_batch_scheduler test_dashboard_observer test_occupancy
```

The tests use isolated temporary databases and fake execution clocks. They are
not production performance or GPU throughput evidence.
