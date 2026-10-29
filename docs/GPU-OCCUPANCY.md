# GPU occupancy and coexistence qualification

All shipped profiles have unknown GPU occupancy and are treated as exclusive.
No VRAM figure or shareable profile has been inferred. The existing dispatcher
still runs exactly one job at a time; this change does **not** enable parallel
voice or LLM execution. The `can_admit` hook supplies an additional fail-closed
check, not a replacement for the current global running-job guard.

A future parallel dispatcher must atomically read all running and
cancel-requested leases and check `can_admit` while claiming the next job under
the same lock/transaction. It must retain a lease until the remote process has
actually stopped, including cancellation, timeout, recovery and drain. Merely
expiring a lease is not proof of GPU release. Multiple dispatcher processes need
cross-process transaction serialization. Scheduler usage accounting is keyed by
attempt, but its behavior under real overlapping execution remains unproven;
verify reciprocal source weighting and non-preemption before enabling concurrency.
It must continue enforcing profile/source concurrency and FIFO semantics.

## Probe hook

Use a dedicated, explicitly authorized low-weight source and bounded test inputs.
After the sequential voice executor is available, measure the **joint** vector:
exact GPU/driver, model artifact and settings, voice backend/profile and audio
shape, N streams, peak VRAM including retained Ollama models, latency, failures,
and cleanup; repeat across realistic worst-case context/audio and restarts.
Record reproducible evidence and measured peaks, including idle retained model
occupancy. Never infer compatibility solely from adding per-job VRAM figures.
Only after review, add a server-owned `Envelope` with exact profile limits and
matching shareable metadata, evidence and measured peak for every member. A
vector is a jointly validated upper bound, not a budget calculation. Unknown,
missing or invalid evidence stays exclusive. Configuration alone does not
activate parallel dispatch. Avoid interrupting Archivist visual jobs; if STT must
be paused for a real test, restore its previous state even on failure.
