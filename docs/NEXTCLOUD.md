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

## Retired external priority contract

The broker queue is the sole scheduler. The former external owner-priority
integration no longer reads a PostgreSQL aggregate, filters candidates or
changes source weights. `BROKER_NEXTCLOUD_PRIORITY_FILE`, if left in an older
unit environment, is ignored; remove it only through an authorized operator
configuration change. `GET /v1/nextcloud/priority` remains available and
returns `{"schema":"nextcloud-priority-55852-v2","enabled":false}`.
Existing source-local new-upload promotion remains part of queue scheduling.
See [forecast](FORECAST.md) for projection limitations.

Correlation matching is not authentication. Inline jobs can be admitted or
promoted by callers on the trusted broker interface, just like the legacy job
controls. If other clients on that interface are untrusted, isolate the server
behind an appropriate authentication boundary; do not treat a file ID or
correlation key as a source credential. This sync does not introduce a new
source-auth protocol or change existing producer credentials.
