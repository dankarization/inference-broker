# Prospective full-day KP text capacity v1 (SOURCE ONLY)

This is separate from the frozen Astra16 comparison at65536. No inference,
admission, rollout, source policy, scheduler, defaults or pilot runtime is changed
by preparing this source. The existing64K profile/endpoint and queued payloads
remain byte-identical. No additional model, dependency or environment is needed.

## Exact contract and reason an additive profile is needed

| Field | Contract |
| --- | --- |
| Contract/schema | `syncopia-memory-daily-text-128k-v1` |
| Opt-in profile | `syncopia-memory-qwen38-128k-v1` |
| Existing protected source | `syncopia-telegram-memory` (no new source) |
| Admission route/kind | `POST /v1/jobs`, `chat`, existing storage bearer required |
| Model tag | `qwen3.8:ad-iq2-xs` (not a global alias) |
| Explicit controls | `think="low"`, `temperature=0`, `num_ctx=131072`, `num_predict=8192` |
| Server controls | concurrency1, keep_alive1800s, request timeout900s |
| Payload | exact system then user string role/content; freeform only |
| Durable identity | nonempty `source_item_id`, `external_id`, existing `producer_storage` capability |
| Byte-policy input ceiling |118784 compact serialized-message UTF-8 bytes |
| Budget reserve |8192 output +4096 wrapper; NOT native tokenization |

`broker/syncopia_daily.py` is the pure authoritative normalizer/proof. Its static
machine-readable description is `docs/contracts/syncopia-memory-daily-text-128k-v1.json`.
The model-facing normalized payload contains only messages, think and options.
Optional model/tools=[]/stream=false are checked then omitted. Explicit options
are mandatory and rejected on drift, never inherited from OpenClaw aliases.
Unknown keys, schemas/response formats, off/high thinking, tools and media are
rejected. Literal URLs remain literal strings: no files or targets are fetched.
Messages are not stripped, sliced, split, summarized or re-ordered.

The existing profile `syncopia-memory-qwen38` and synchronous
`/v1/syncopia-memory/extract` stay65536/8192, with their existing extraction and
freeform normalization unchanged. Existing native `openclaw` has a131072 cap,
but requires source=openclaw and inline payload and rejects producer_storage.
It therefore cannot satisfy THIS pilot's protected source/storage identity.
Borrowing that source or globally widening the old64K profile is not a valid
capacity change. Current deployed24a5b69 rejects the new profile until main
accepts and rolls out the additive broker source: **deploy_needed=true** for this
prospective capacity contract. Astra16's native route still needs no rollout.

The existing dispatcher's running-job lock is global, and candidate concurrency
is also counted per source. Old64K and new128K jobs share the same source/slot;
there is no second queue or parallel executor. No weights, enable/admission flags,
retention defaults, DB schema, indexes, retry or ACK protocol are changed. Both
submit and pre-executor dispatch fail closed on the new exact contract. Old
profiles/producers are neither adopted nor re-normalized.

## Capacity evidence, not a claim of native fit

The accepted daily-input-v6 observation (main-owned bundle; not read or rewritten
here) is69 KP primary messages plus9 compact memory items,84045 message UTF-8
bytes. This contract's policy arithmetic is:

```text
84045 +8192 +4096 =96333 <=131072; policy margin34739
input byte ceiling =131072 -8192 -4096 =118784
```

These are NOT tokenizer counts. Rendered template overhead, native occupied
context, truncation behavior, final/reasoning token split, latency and quality
remain unmeasured. The synthetic regression reproduces byte sizes, not private
messages or model outcomes. No text is removed to meet the bound; an oversized
request is held for main, not chunked or silently truncated. Output8192 also does
not promise8192 final-only tokens when the native model spends tokens thinking.
Low is the configured baseline, not off, and no unsupported toggle is invented.

DAILY-INPUTS.md in the pilot remains runtime-owner9c's acceptance source. Its
currently observed22:19:31 cutoff is partial, not full day: main must reconcile
coverage through day end and remeasure the actual complete request before a
canary. The new byte policy must pass that updated payload; this source change
does not waive coverage, compact-memory, held-slot or one-task/chat/night gates.

## Producer handoff: matching normalization and storage evidence

Same runtime owner9c changes its producer_bridge/local admission contract only
after main accepts this version. It adds an explicit opt-in128K mode, not a new
default/global alias and not a rewrite of held/queued64K tasks. It must preserve
the daily durable slot and unknown-POST recovery: no duplicate local task or
blind broker POST retry. No pilot source/state is edited by this broker owner.

For the chosen opt-in request, build the normalized payload with
`syncopia_daily.normalize_request(payload)` and derive **both** input hash and
input byte size with `broker.storage.content_evidence(normalized_payload)`.
That is canonical UTF-8 JSON with sorted keys/compact separators, not the
message-only bytes or the full HTTP envelope. Numeric0.0 and protocol-only fields
normalize before hashing; hash the normalized object exactly. The normalizer is
idempotent. `input_proof` emits only scalar metadata/hashes, never message text.

Existing producer-storage capability remains unchanged: same producer attempt,
input/result mode/ref/schema, storage auth, durable input/result readback, ACK and
receipt checks. A hybrid/producer-owned input must declare the normalized hash
and byte size; otherwise admission fails before enqueue. The existing policy
must already allow the mode. There is no implicit source-policy mutation or
fallback to a public/unprotected admission. A conflicting correlation/profile
cannot adopt an old64K job; recover the recorded job with GET-only owner auth.
Native result persistence/privacy is unchanged from the old memory route; this
capacity patch is not a server reasoning-retention fix. Producer final-only
collection and the accepted retention boundary remain runtime/main concerns.

Envelope template (main/runtime owner only, after acceptance and rollout; NOT
executed by this code owner):

```python
from broker import syncopia_daily as daily
from broker.storage import content_evidence
payload = daily.normalize_request({
    "messages": exact_system_user_messages,  # original text, not references/media
    "think": "low",
    "options": {"temperature": 0, "num_ctx": 131072, "num_predict": 8192},
})
input_hash, input_bytes = content_evidence(payload)
body = {
    "profile": daily.PROFILE, "kind": "chat", "source": daily.SOURCE,
    "source_item_id": existing_durable_slot_item_id,
    "external_id": accepted_durable_request_identity,
    "payload": payload,
    "producer_storage": existing_capability_with_matching_normalized_input_evidence,
}
# Existing authenticated POST /v1/jobs; commit intent/hash before it and job UUID
# immediately after ACK. Lost ACK: GET /v1/correlations then recorded UUID/status.
# No direct Ollama, /openclaw route, inference retry loop or new journal/slot.
```

Do not use message byte bounds as `producer_storage.input.byte_size`; the
canonical executable payload includes options and think. All old artifact schema
and receipt semantics are retained unless runtime owner separately versions them.

## Finite offline CLI and focused verification

Run on an accepted immutable source export; no CLI below submits a request:

```sh
export PYTHONPATH='/path/to/accepted-source-export'
export PYTHONDONTWRITEBYTECODE=1
python3 -B -m broker.syncopia_daily describe
# Local, accepted original text payload JSON; proof never prints input text.
python3 -B -m broker.syncopia_daily prove --payload-json '/private/accepted-payload.json'
# Required fields in that file: messages, think="low", exact options above.
# Missing/malformed/drifting/oversized input exits2 with a fixed non-echoing error.
```

The code owner's tests use synthetic full-day-sized text and loopback fake
executor/storage only: old35-profile fingerprints unchanged; exact options/text;
boundary118784/118785; no off/media/schema/alias drift; storage canonical hash,
401 guards, ACK and idempotency; protected source/kind/capability; queued old job
hash/bytes/order across restart; one running slot across old/new; corrupted
persisted options fail before WOL/executor. The existing memory regressions and
original31-profile hash are also checked. A historical interrupted full suite
remains NOT PASS; no new full-suite claim is made.

## Main-owned forward plan and rollback (not performed)

1. Accept the exact versioned source/normalizer/proof and hand it to SAME runtime
   owner9c. Review the capacity-only live-base overlay separately from the retained
   cumulative evaluation source; no eval profile/source installation is required.
2. Before authorized broker rollout, re-read live unit/source parity and classify
   any new live-only source. Quiesce only replenishing affected producer under
   main authority; keep its collector active. Use supported drain and wait for
   any running lease to finish; do not execute the backlog just to empty it.
3. Capture a transaction-consistent DB backup and pending job IDs/order, profile,
   normalized input hashes/bytes, plus policy/env checksums at protected locations.
   This patch needs no schema/index/policy migration. Install only the reviewed
   source overlay and authorized restart; verify readiness and exact source parity.
   Pending64K payloads/identity/FIFO must survive; no reprofile/cancel/resubmit.
4. Runtime owner implements matching opt-in local mode, pure normalization and
   canonical artifact hashes before any admission. Main reconciles full workday
   inputs, remaining local-slot state and readiness. Actual bounded canary is a
   separate main-owned step, not authorized or performed by this code handoff.
5. Logical rollback: disable the producer's128K opt-in while retaining the additive
   server reader/profile/guard for already queued/retryable/visible jobs. Resume
   old64K only for inputs that pass their old gates; full-day KP stays held if too
   large. Never relabel/truncate a131072 payload to fit64K or reset a journal/slot.
   Do NOT downgrade to a binary without the profile while any durable job still
   references it. A physical old-binary rollback needs main's job/reference
   inventory and a compatibility reader or proof no such references remain; keep
   policy, DB/receipts and queued work intact. No DB restore over advancing work.
