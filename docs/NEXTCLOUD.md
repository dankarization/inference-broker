# Dedicated read-only Nextcloud media producer

`POST /v1/jobs` is the existing asynchronous admission API. New clients use:

```json
{
  "source": "nextcloud",
  "profile": "nextcloud-qwen3-vl",
  "kind": "generate",
  "source_item_id": "<Nextcloud fileid>",
  "external_id": "<stable hash of version + processing recipe>",
  "payload": {"prompt": "Describe these sampled frames and uncertain OCR", "images": ["<base64 JPEG>"]}
}
```

Only `qwen3-vl:30b`; 1–4 images, ≤2 MiB decoded per image, prompt ≤16 KiB,
context 16384, output 2048, temperature0, executor timeout900s. No caller tools,
model override, context widening, streaming or fallback. Both the source and
profile are mutually pinned; generic `/api/generate` cannot bypass these checks.
The separate native OpenClaw route remains unchanged and uses source `openclaw`.

A configured source policy is mandatory. Operators merge a dedicated nextcloud
entry with weight7; existing policy files/weights are never modified by this code.
There is no job-count admission cap: any number of queued jobs accumulates.
Exact replay is available even when current admission closes. Byte budgets
(evaluation 256 MiB active normalized payload, request byte limits) still apply.

`external_id` and `source_item_id` are required. Admission persists identity before
returning; exact retries reuse the same job. Reusing a key with another payload,
profile, kind or file identity is an error. Completed payload cleanup retains
hash evidence, so payload-sensitive replay still works. Retention tombstones
preserve identity until their configured finite TTL and refuse expired replay.
Startup adds the partial unique index `jobs_nextcloud_external`; no existing
storage/retention or routing behavior is changed.

After a timeout read `GET /v1/correlations?source=nextcloud&external_id=...`, then
`GET /v1/jobs/<id>`. Do not mint a new key or automatically retry failed/expired jobs.
The companion helper verifies result source/profile/model before atomic persistence.
Its MVP uses legacy broker-temporary storage with existing retention. It does not
send producer-storage capability/ACKs; never enable ACK-required for that client.
Producer-owned clients elsewhere retain their existing protocols unchanged.

Tests: `python3 -m unittest discover -s tests -p test_nextcloud.py -v`.
Synthetic fixtures only; this does not activate a production source or run media.

## Source-local new-upload priority

`POST /v1/jobs` accepts `nextcloud_priority: "new_upload"` or `"historical"`
only for source `nextcloud`; omission defaults to historical. With source
policy, confirmed new uploads precede historical model lanes within that
source's weighted turn, without preempting active jobs or other sources.
Without policy, scheduling remains global FIFO.

`POST /v1/jobs/{id}/nextcloud-priority` requires exactly `source`, `external_id`,
`source_item_id` and `nextcloud_priority: "new_upload"`. All correlation fields
must match the existing job. Promotion is queued-only, monotonic and idempotent:
it does not create work, reset queue age or demote on historical replay.
Producer-storage jobs retain their owner-token requirement. Legacy endpoints
still require a trusted local network, not public exposure.
`GET /v1/nextcloud/capabilities` describes this contract.

## Optional external owner gate

`BROKER_NEXTCLOUD_PRIORITY_FILE` enables the restricted PostgreSQL owner gate.
Without it there is no owner connection or `psycopg` dependency. Enabling it
requires separately installing `psycopg`, provisioning the external
`arch.priority_read(jsonb)` contract and supplying an owner-only regular JSON
connection file (current UID, no group/other access, at most 4096 bytes).
Remote connections require `sslmode=verify-full`; privileged/migration roles
are rejected. Do not commit the connection file or credentials.

The external schema identifier is `nextcloud-priority-55852-v2` (an exact
protocol name, not a configurable job priority). The aggregate supplies the
owner-wide tier/floor and completeness evidence; local FIFO alone cannot
reconstruct it. The gate holds its read transaction through SQLite claim
commit, never through inference. It fails closed for Nextcloud on owner error
or queue-bound overflow, while leaving other eligible sources dispatchable.
Primary/deferred eligible tiers use effective source weights 7/10, without
rewriting the policy file. `GET /v1/nextcloud/priority` exposes enabled/contract
state, not connection credentials or hidden owner rows.
See [conditional forecast](FORECAST.md) for read-only projection limitations.

Malformed/partial owner responses are rejected before publishing eligibility:
all required keys, Boolean completeness, integer-or-null floor and positive
integer job tiers must validate. The unavailable path clears authority so a
broken owner response cannot stop otherwise eligible peer sources. This is a
defensive code hardening, not a change to the external schema or live deployment.

Correlation matching is not authentication. Inline jobs can be admitted or
promoted by callers on the trusted broker interface, just like the legacy job
controls. If other clients on that interface are untrusted, isolate the server
behind an appropriate authentication boundary; do not treat a file ID or
correlation key as a source credential. This sync does not introduce a new
source-auth protocol or change existing producer credentials.
