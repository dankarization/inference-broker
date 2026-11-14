# Model identity + current native diarization integration

## Source custody and provenance

Prepared in the existing model-identity worktree; no other worktree changed.
Fresh remote systemd inspection found PID 2120592, unit
`ollama-inference-broker.service`, WorkingDirectory
`/home/dankar/.local/state/ollama-inference-broker/native-diarization-20261114/release-4e20196619a2`.
Effective final drop-in is `zzzzzzzzzzzzzzzzzzzzzzzzzz-native-diarization-20261114.conf`;
its existing private `diarization-v5.env` remains untouched. Full 43-file live/candidate hashes are in `model-identity-integration-manifest.json`.
Detailed unit/drop-in paths and registered worktree HEAD/status inventory remain
local-only in `.integration/live-inventory.json` and `local-worktrees.json`.
No env contents were collected.
This is source evidence, NOT a restart/custody window.

Live manifest aggregate SHA256:
`4e20196619a254d1ba136982ab94321e8b0fc9b331a64bc8aed8dbf89d94d7f2`.
Fresh live bytes exactly equal `production-trial-v4/additive-release/merge-receipt.json`
`after`, the artifact installed by production-trial-v5. Compared with e431424,
ALL live changes are the five approved diarization files: profiles, service,
voice, voice_gpu, new diarization_contract. They originate from 0628e6f's pinned
additive preparation, not a wholesale merge of its older branch. The three transport/
contract files are exact approved bytes; profiles preserves existing catalogue;
service changes only durable diarization correlation admission. No unknown code
was included. Recovery-v7 concerns runtime/worker attempts, not a broker source delta;
fresh live bytes, not historical claims, are authoritative.

Candidate retains all 43 live source/assets. Only FOUR differ from live:
`broker/service.py`, `broker/dashboard.py`, `broker/static/dashboard.html`,
`broker/static/dashboard.js`: the already-reviewed fe40415 identity changes.
New diarization source/tests coexist with generic model-identity regressions.
`test_live_integration.py` enforces complete release-byte parity and this footprint.

All seven registered worktrees were inspected before mutation. Other six were
clean: main5957550, voice-large569a5ab, dashboard e431424, media-stt89afee0,
summary a704508, diarization0628e6f. Their unmerged/unpublished historical work is
NOT implicitly authorized for inclusion. In particular, never merge the old
0628e6f branch wholesale (it would remove newer dashboard/priority/summary code).
Native-worker owner218158f6 remains active but broker-read-only by delegation;
no source-owner conflict found. No descendants spawned.

## Main-owned operations contract (not executed)

1. Accept candidate/test evidence and obtain a NEW coordinated peer HOLD/custody
   handshake. A released worker snapshot is not a maintenance window. Preserve
   Nextcloud execution=false and every other current control. No worker/env change.
2. Repeat effective unit/cwd/source manifest inspection. Refuse ANY drift from
   the recorded 43-file live manifest and return to this source owner. Record
   private unit/drop-in/env hashes; preserve all private values and paths.
3. Use the existing production-trial-v5 `control-remote.py` workflow semantics:
   checksum-guarded admission maintenance and per-source dispatch gating, fieldwise
   restore, no global queue pause. Do NOT replay historical scripts/receipts:
   their paths, policy hashes and phase preconditions are stale. Wait naturally
   for running/cancel_requested=0, no fences, and fresh all-executor custody proofs.
   Capture row-level invariants using the existing read-only snapshot workflow.
   No cancel/retry/fence manipulation. Backup only through approved SQLite online
   backup workflow, never copy a live DB without WAL consistency.
4. Stage `.integration/release.tar.gz` as a NEW private release on the notebook,
   with `umask 077`, never overlay active source. Archive contains only broker/
   runtime files listed by candidate manifest. Rehash ALL extracted files against
   candidate manifest; reject links/unexpected entries. Preserve existing env
   references including diarization-v5.env, DB, spool, policies and ASR endpoints.
5. Under the existing policy lock and final custody/idle/source checks, use the
   existing v5 `switch-remote.py` transaction pattern: preserve fragment/drop-in
   hashes and old cwd, stop ONLY broker, create ONE new later owned drop-in with
   `[Service]\nWorkingDirectory=<new immutable release>\n` (NO EnvironmentFile or
   other setting changes), daemon-reload, start broker. Concrete service commands,
   ONLY after those gates, on notebook:
   `systemctl --user stop ollama-inference-broker.service`, then write the exact
   new owned WorkingDirectory drop-in, `systemctl --user daemon-reload`,
   `systemctl --user start ollama-inference-broker.service`.
   Old switch script is a reference, NOT directly executable for this candidate:
   old cwd/new paths/env assumptions must not be replayed.
6. While gated, verify /healthz, full deployed hashes, one broker PID, unchanged
   effective env values and original unit/drop-in hashes; compare job/attempt/
   receipt/fence/control invariants. Verify dashboard and /v1/history report
   retained requested/observed models with provenance, generic overrides and
   historical fallback. No real inference needed. Preserve existing queued IDs.
   Restore ONLY saved admission/dispatch fields using current checksums, retaining
   concurrent edits and Nextcloud execution=false. Release peer HOLD explicitly.

No migration or backfill. Rollback to the CURRENT native-diarization release,
NOT the older non-diarization dashboard release, using the same gated-idle/custody
procedure and removing only the exact new owned drop-in. Never restore an old DB.
No deploy, restart, config/control/env/DB/worker mutation was performed here.
Git authorization covers feature push only, not PR or default-branch merge.
