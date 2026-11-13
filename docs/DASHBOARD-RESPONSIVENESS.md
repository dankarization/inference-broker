# Dashboard observer candidate (2026-11-13)

## Source identity / scope

Branch: `fix/dashboard-responsive-20261113`. Base `origin/main` 368b2d4;
the original local main 5957550 was behind by 12 commits and was not changed.
Production is an exported tree, not an identifiable Git checkout. Commit cf8d9ed
records its six differing/new modules over origin/main. All 42 non-bytecode
broker files match the deployed export byte-for-byte (revalidated after recovery) (manifest in
`dashboard-proofs/production-baseline-sha256.json`). This baseline commit is
NOT a feature change to deploy on top of production: deploy the subsequent
dashboard diff against this baseline. Preserve production summary/Nextcloud code.

Read-only source/FD observations confirmed:
- host 192.168.2.19, user unit `ollama-inference-broker.service`, PID 1075155;
- cwd `/home/dankar/.local/state/ollama-inference-broker/broker56552/candidate`;
- database `/home/dankar/.local/state/ollama-inference-broker/broker.sqlite3`.

No live data was copied or queried for benchmarks. No production file, DB,
service, configuration or automation was changed. No backend inference was run.

## Changes and remaining limits

- Historical-only sources remain visible. Public discovery, state/retry/lease
  counts read narrow partial covering metadata, not payload pages. Historical
  aggregation still costs O(N); this is not a constant-cost materialized summary.
- Cover completion audit windows by source, event type, time and visibility.
- Forecast uses bounded per-source indexed seeks, extracting JSON only after the
  ordered LIMIT; separate FIFO and Nextcloud-priority indexes preserve semantics.
  Private jobs cannot force public seeks through a private backlog.
- Observer reads use one read-only, bounded WAL transaction; close on all exits.
  Dashboard stage timing and failure operation/elapsed time contain no payload,
  expanded SQL, IDs, source names or parameters. Deadline remains 0.75 seconds.
- ETag generation does not wait for the dispatch lock. If the lock is busy it
  returns a unique revision, avoiding reuse of an uncommitted writer revision.
- Dashboard and history share one read slot. Hidden documents issue neither
  initial/periodic dashboard reads nor scroll reads; visible return refreshes.
  Failed history reads do not immediately rearm an intersecting sentinel (no tight
  retry loop); a later scroll/queue refresh retries. Without IntersectionObserver,
  the first screen and periodic queue updates still work; infinite scroll does not.
  Existing in-flight requests may finish after hiding. Network reads time out
  after 10 seconds on browsers supporting AbortSignal.timeout.
- Scroll uses keysets and de-duplicates rows; missed busy intersections are
  rechecked after reads. Once paging starts the historical slice is kept stable
  across queue refreshes until reload (no dropped appended rows/cursor reset).
  New terminal jobs still change counters; reload to start a fresh history slice.
- Dispatcher full-candidate selection is intentionally unchanged. SQLite still
  has a single writer; long external writers, disk stalls, unbounded source
  cardinality, huge individual queued payloads and WAL/checkpoint pressure remain
  possible. No claim of absolute absence of blocking or future timeouts.

## Reproduction and verification

No new packages or build environments. Python stdlib tests; optional installed
Chromium + Python websockets for browser test.

```sh
python3 -m unittest discover -s tests -p 'test_dashboard*.py'
node tests/dashboard_dom.cjs
PYTHONPATH=. python3 tests/browser_dashboard.py
# Use a disposable OFFLINE database, never the live broker DB:
PYTHONPATH=<exported-baseline-source> python3 tests/profile_dashboard.py scratch.db --seed
PYTHONPATH=. python3 tests/profile_dashboard.py scratch.db
```

Benchmark: same offline DB, 100,000 jobs (80k terminal / 20k queued), 20 sources,
8KiB prompt per job, 25% private jobs, 80k completion audit events with 256-byte
fixture metadata; 15k public queued jobs. Three bounded sequential reads, three
separate 30s-budget profiling reads, 12 reads across up to four readers alongside
60 committed indexed job updates, and six public history pages (180 unique IDs).
The 30s setting exists only inside this local profiler; shipped deadline is unchanged.
A local TRUNCATE checkpoint before trials separates migration cost from steady state.

Final proofs: `dashboard-proofs/{before,after}.json`.

| Metric (ms) | Deployed baseline | Candidate |
|---|---:|---:|
| Public dashboard, 750ms budget | 751 / 751 / 751, unavailable | 67 / 66 / 74, live |
| Unbounded local profile | 1987–2071 | 61–65 |
| Concurrent dashboard maximum | 756, all stale | 183, all live |
| Indexed writer median / p95 / max | 15.7 / 40.1 / 188.0 | 14.4 / 22.7 / 28.3 |

The sampled baseline snapshot stage reaches 1817ms; forecast 252–261ms.
Candidate snapshot 53–142ms, forecast 6–42ms (includes concurrent samples).
These are warm local synthetic observations, not production latency guarantees.
No OS cache eviction, load test or production benchmark was performed.

23 focused Python tests pass, including actual public SQL, historical-only
sources, exact public pagination across tied timestamps (900 unique IDs),
malformed JSON outside selected heads, FIFO/priority preservation, concurrent
readers/writer, bounded consistent WAL snapshot permitting another writer commit,
and nonblocking ETag generation. Node DOM regression passes, including failed-page retry-loop prevention and
IntersectionObserver absence. After recovering the interrupted run, the 23 focused
Python tests passed again in 16.4s; Node and Chromium reruns exited 0.
Real Chromium passes initial display, scroll to 40 rows, retained rows after
refresh, zero reads while hidden, one on resume, one with overlapping refreshes.
Visibility is deterministically emulated via document.hidden in this browser
test; native OS tab switching/focus is not separately verified.

Full suite recovery rerun: 402 tests, 3 skipped, 3 failures (exit 1); all three failures reproduce
unchanged on the exact production baseline using the same upstream tests:
- nightly profile count 33 vs frozen 31;
- daily profile count 37 vs frozen 35;
- native waiter budget 2100 vs old 1800 expectation.
These predate this dashboard patch and were not papered over by changing tests.
Full suite recovery rerun took 55.6s with task-local RAM tempfiles and 4GiB
address-space cap. Its only failures are the same three independently reproduced
on the production baseline; the two additional tests cover WAL consistency and
nonblocking validator behavior. Task-owned residual RAM test files were removed.
An earlier disk-temp run timed out after 300s; not counted as successful.

## Migration footprint and rollout (NOT executed; separate approval required)

Seven additive indexes, no columns/rows/triggers changed by this patch:
`dashboard-proofs/indexes.sql` contains exact, idempotent, transactional DDL.
On the fixture: total index pages 10,702,848 bytes (~10.21MiB):
audit windows 2,957,312; all/public aggregates 2,932,736 / 2,203,648;
all FIFO/priority heads 733,184 / 753,664;
public FIFO/priority heads 552,960 / 569,344 bytes.
Initial observed DB growth was exactly this total. Later repetitions reuse free
pages after offline DROP, so unchanged file length is not zero index cost.

Final candidate whole Broker initialization (including builds/recovery) was
4085ms; subsequent checkpoint 415ms. An earlier contended initialization took
21.6s. These are upper bounds on index build time in those local runs, not
per-index timings and not production estimates. A subsequent exact transactional
DDL rehearsal on the retained fixture measured 5268ms total: audit 218ms;
all aggregates 1750ms; all FIFO/priority 537/540ms; public aggregates 520ms;
public FIFO/priority 578/575ms. WAL after commit: 10,786,192 bytes; checkpoint
3.1ms with busy=0. This rehearsal used existing free pages and memory sorting,
not a production estimate or a disk/RAM peak. See `dashboard-proofs/migration.json`.
Before explicitly checkpointing
migration WAL, the first indexed writer hit 965ms; this is why a pre-admission
checkpoint is required in the plan. Ongoing index write amplification remains.

Operator/Main rollout:
1. Revalidate deployed source manifest and candidate tests; refuse source drift.
   Rehearse on an authorized consistent production-sized backup on comparable
   storage; record actual per-index build durations, WAL/temp peak, free space
   and writer tails. Production row counts and index size are not inferred from
   the 1.8GB payload-heavy DB size. This rehearsal is a required deployment gate.
2. Obtain explicit maintenance/deploy authorization. Fence admissions/arrange
   idempotent producer retries, drain/wait for active work; stop this broker unit
   only in the approved window. Record queue identities/state and runtime policy.
3. Take a consistent SQLite backup with the writer stopped; retain old release,
   unit settings and DB backup; validate backup quick_check/integrity as appropriate.
   Never copy just the DB file from a running WAL database.
4. While service is stopped, apply indexes.sql to the exact DB via an approved
   SQLite maintenance runner. It uses BEGIN IMMEDIATE and COMMIT; if it fails,
   roll back and do not start the new code with missing indexes. Check all seven
   sqlite_master definitions, integrity/queue identities and free space.
5. After commit perform PRAGMA wal_checkpoint(TRUNCATE) with no readers/writers;
   require busy=0 and a successful checkpoint before reopening admissions.
   No VACUUM or destructive retention maintenance is part of this rollout.
6. Install the verified source/static release, preserving existing environment,
   source policies and data paths; point the user unit at that release using the
   established operator mechanism. Start only with separate restart approval.
   Check health, public first screen, first and next history page, stage timing,
   live observation, queue identities, and one authorized normal queue lifecycle.
   Do not replay synthetic load against production.

Rollback: stop/fence under approval, restore old source/static/unit cwd and restart.
Additive indexes are compatible with old code: leave them initially, avoiding data
loss and long DROP operations. Do NOT restore an old DB over post-rollout jobs.
Optional later removal of exactly seven observer indexes requires a separate
maintenance window; dropping does not shrink the file. An interrupted transaction
rolls back. Restoring the pre-rollout DB is a last-resort recovery decision with
explicit reconciliation of all jobs admitted since backup.

## Storage / ownership

Owner: this task / programmer, one worktree writer; no subagents, no dependencies
installed/copied, no extra dependency/build slots, no push/PR/automation.
Worktree: /home/dankar/repos/inference-broker-dashboard-20261113
Existing main and other worktrees unchanged. Common Git objects reused normally.
Proof export, logs and one ~933MiB logical synthetic DB remain under
`.diagnostics/` (gitignored), approximately 935MiB physically with logs/export;
not production data. Final measured diagnostics footprint: 957,880KiB; remaining
source tree including bytecode: 4,136KiB (common Git storage excluded).
No diagnostic file has link count >1. No hardlinked materialized dependencies or DB copies.
Peak disk usage was not continuously sampled; observed final footprint is not
an asserted peak. RAM scratch used <300MiB in sampled observations and is removed
after tests. Synthetic DB/export/cache are task-owned retirement candidates after
acceptance; source, committed proofs and unique work must be retained.
