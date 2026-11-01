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
Maximum two queued/running/cancel-requested jobs for the new source. Exact replay
is available even when current admission closes or this queue limit is reached.

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
