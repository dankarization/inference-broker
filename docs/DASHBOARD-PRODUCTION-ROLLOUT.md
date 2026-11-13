# Dashboard rollout receipt — 2026-11-13

## Identity / authorization

Explicit authorization covered consistent backups, offline rehearsal, normal drain,
seven additive indexes, the broker-only stop/start, source cutover and smoke.
No OpenClaw configuration/service or other service was changed. No push, PR,
permanent automation, package install, job cancellation or synthetic submit.
One feature/write owner: programmer; no subagents.

Source: `7e893d938ccaccbc48bf79535b83f4a75cca1d15`, baseline `cf8d9ed`.
The 42-file deployed source manifest matched baseline before staging and again
immediately before stopping. Staged release matches the committed candidate.
Runtime dependencies and every existing Environment/EnvironmentFile were retained,
including BROKER_RELEASE_REVISION (a voice compatibility identity, not a dashboard
build label). Only WorkingDirectory in the existing final broker drop-in changed.
The old exported release was left untouched.

Host: `192.168.2.19` (verified distinct from the execution Gateway).
User unit: `ollama-inference-broker.service`.
New PID at verification: `1564325`.
New cwd: `/home/dankar/.local/state/ollama-inference-broker/dashboard-20261113-7e893d9/release`.

## Rehearsal / stop-window decision

Initial measured available space: 13,485,649,920 bytes. Task disk budget 6 GiB,
reserved free space 4 GiB. Private artifacts stay on that host; no production DB
or payload was downloaded. SQLite backup API used a bounded read transaction,
allowing live writers in WAL mode; never copied the live DB file alone.

- Consistent online backup: 105.829 s, 1,822,621,696 bytes, mode 0400.
- Separate rehearsal copy: 36.601 s. Integrity check before/after: **ok**.
- Seven-index transaction: **0.587 s**, 6,742,016 logical index bytes (6.43 MiB).
- WAL after commit: 6,884,552 bytes; checkpoint busy=0.
- Queue identity/state/attempt/retry digest and all state counts exactly unchanged.
- Public production-copy dashboard: **27.115–27.987 ms**, all live, budget 750 ms.

The initial conservative rehearsal gate required copy<20s and therefore reported
`short_stop_gate=false`. This was NOT ignored: the cutover plan was changed so
**the final drained consistent backup and its quick_check happened online before
stopping**, not inside downtime. No need to copy 1.8 GB during the stop window.
Index build and verification fit the bounded stop window; actual outage below.
No storage/safety gate was relaxed and no job was interrupted.

## Production cutover

The initially observed voice job held a request-owned GPU fence; it was not
cancelled or released by this task. By cutover preflight it had completed and a
normal queued job was running. Existing `/v1/queue/pause` stopped new claims while
that attempt finished naturally: **129.193 s**. Waited for both active=0 and
GPU fences=0. Admission was then fenced through the authenticated existing
`/v1/maintenance/admission` route using its current policy SHA256 CAS.
The existing private storage credential was read locally solely for that route;
its value was neither stored in proofs nor changed.

Final drained online backup + quick_check: **41.470 s**, quick_check itself 0.304s.
Only then stop the idle broker, migrate, verify, checkpoint, change the existing
release drop-in WorkingDirectory, daemon-reload and start this unit.

- Migration: **0.647 s**, all seven indexes present.
- Live DB quick_check while stopped: **ok**, 0.300 s.
- Checkpoint: busy=0; no pending WAL frames.
- **Stop-to-health-ready: 2.225 s.** First dashboard HTTP followed immediately.
- Three HTTP `/v1/dashboard`: **13.064 / 12.767 / 22.631 ms**, state=live.
- `/dashboard`: HTTP 200. Two history pages: **60 distinct public rows**.
- Candidate source hashes match, service active in new cwd, ordinary dispatch resumed.
- Original `paused=false` and policy maintenance=false restored through their APIs.
  No source weights/admission flags/runtime environment were replaced.

Queue proof (24,922 total jobs throughout):

| Moment | completed | queued | running | failed | cancelled |
|---|---:|---:|---:|---:|---:|
| Cutover preflight | 22,769 | 797 | 1 | 1,066 | 289 |
| Drained / stopped / migrated / restarted paused | 22,770 | 797 | 0 | 1,066 | 289 |
| Dispatch resumed, first 180s observation | 22,770 | 796 | 1 | 1,066 | 289 |
| Final post-rollout check | 22,771 | 795 | 1 | 1,066 | 289 |

The same identity/state/attempt/retry digest holds at pre-stop, stopped,
post-migration and restarted-paused checkpoints. The two later changes are the
old attempt completing naturally and one existing queued job starting normally.
No new job was submitted and failures/cancellations did not increase.
The initial 180-second smoke correctly recorded running, not a fabricated completion.
A subsequent bounded read-only observation **confirmed running -> completed** with
attempt_count=1/retry_count=0; another ordinary queued job then started. See
`dashboard-proofs/production-normal-execution.json`. This accounts for the final
completed+1/queued-1 change. Total remains 24,922; no failure/cancellation increase.

## Real UI / observer verification

Actual deployed HTTP UI in installed Chromium, not a fixture:
- initial history 10 rows; scroll 40 rows; retained after refresh;
- native second-tab activation changed document.hidden (no property emulation);
- zero network reads during 31 seconds hidden (>30-second refresh interval);
- one dashboard read on native visible return; overlapping refreshes issue one read.
No UI control was pressed. RAM browser scratch cleaned; no page content or
screenshot retained. Proof: `dashboard-proofs/production-browser.json`.

Sanitized journal evidence records source identity, observer error counts and
stage timings without expanded SQL, payload, transcript or job IDs. Initial
post-start check had **zero observer errors** and confirmed scheduler.selected /
job.running events, not merely systemd active. Final journal check also confirms
job.completed and zero observer errors; live snapshot age was **0.012 s**.
The later post-check is authoritative
for its recorded observation window; this is not a permanent monitoring service.

## Rollback, if subsequently required

Do not stop an active job or clear a GPU fence to roll back. Use the same supported
pause/drain + admission-CAS barrier, wait for active=0/fences=0, and stop the broker.
Retain the current override separately, restore the exact saved original override:

- Original source/cwd: `/home/dankar/.local/state/ollama-inference-broker/broker56552/candidate`.
- Saved original override: `dashboard-20261113-7e893d9/original-release-override.conf`.
- Target override: `~/.config/systemd/user/ollama-inference-broker.service.d/zzzzzzzzzzzzzzzzzzzzzzzzz-broker56552.conf`.

After restoration, `systemctl --user daemon-reload`, then start ONLY
`ollama-inference-broker.service`; verify HTTP and restore previous control flags
through the existing APIs, preserving unrelated policy edits. Leave the seven
additive indexes in place: old source is compatible. No DB restoration is needed.
**Never overwrite the live DB with either backup after newer jobs/receipts exist.**
Full backup restoration is disaster recovery only, requiring explicit later-data
reconciliation. No index drop, VACUUM, retention change or destructive cleanup was
performed or is part of ordinary rollback.

## Storage / retained paths

Owner: this task/programmer. Remote private root:
`/home/dankar/.local/state/ollama-inference-broker/dashboard-20261113-7e893d9/`.
- `release/` — active committed source; retain.
- `online-backup.sqlite3` — original consistent backup, 0400; retain for recovery.
- `rehearsal.sqlite3` — separate migrated production-data rehearsal, 0600.
- `drained-backup.sqlite3` — pre-cutover drained consistent backup, 0400; retain.
- `original-release-override.conf`, `original-sources.json` — private rollback evidence.
- `indexes.sql`, manifests, one-off scripts and JSON receipts — task audit artifacts.

Measured remote physical artifact size at initial completion: **5,468,700,672
bytes**, under the 6 GiB budget. Final measured physical artifacts:
**5,468,712,960 bytes**; available **7,846,440,960 bytes**, above the 4 GiB reserve. Index pages reused existing DB free pages; unchanged DB
file length does not mean zero index cost. Peak was not continuously sampled.
No hardlinked DBs/dependencies or new dependency slots. Existing notebook synthetic
fixture/source export/logs remain in worktree `.diagnostics/`; see candidate report
for their earlier measured ~935 MiB footprint. Final notebook measurement:
`.diagnostics/` 957,952 KiB; source tree excluding diagnostics/common Git 4,172 KiB.
Relative to the candidate-phase measurements, these are +72 KiB/+36 KiB; no local
production DB copy and zero diagnostic hardlinked files. Local paths added here
are `.diagnostics/deploy_rehearse.py`, `deploy_cutover.py`, `production_browser.py`,
`summarize_production.py`, `candidate-sha256.json`, and `production-*` logs/JSON;
committed sanitized evidence is under `docs/dashboard-proofs/production-*`.
All backups are private production data, not publishable source artifacts.

Nothing was manually deleted. Rehearsal and old synthetic fixtures are retirement
candidates after acceptance; backups/old source must follow the operator's retention
decision. No automatic cleanup was installed. Historical O(N) aggregates, single
SQLite writer, external disk stalls/WAL pressure and very large payloads remain
scalability limits; no absolute nonblocking claim is made.
