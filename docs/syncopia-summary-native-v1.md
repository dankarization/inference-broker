# Opt-in native-Boolean memory summaries

The existing syncopia-memory-qwen38 normalizer and daily/nightly contracts
remain byte-unchanged. These two new profiles use the EXISTING source
syncopia-telegram-memory and kind=chat, inline text only:

| Profile | Exact model | Context ceiling | Output ceiling |
|---|---|---:|---:|
| syncopia-summary-gemma12-v1 | gemma4:12b | 131072 | 65536 |
| syncopia-summary-bolt-e4b-v1 | nemotron-3.5-bolt:e4b | 65536 | 32768 |

These are profile ceilings, NOT default trial settings, effective runtime
observations, proof of occupied-context VRAM fit or recommendations to use
maximum output. Context/output/temperature must be explicit on every request.
No streaming, tools, media, schema format, keep_alive override or extra options.
Exactly two messages, System then User, each with only role/content. Exact
UTF-8 content is copied, never sliced, normalized or summarized by this adapter.
External/source-item IDs are required bounded durable identities.
Exact queued/completed/failed retries reconstruct deterministic normalized input
and pinned admission proof, then compare the complete durable identity under the
DB lock, without fresh metadata. They return the original receipt even if native
metadata is now offline/drifted; this never authorizes execution. New keys still
require live preflight before INSERT; dispatch preflight is unchanged.
Changing profile, kind, source item or
normalized input under the same source/external ID rejects. Collisions with old
memory jobs reject in either direction; legacy-to-legacy behavior is unchanged.

## Wire contract

Use `POST /v1/jobs` with explicit operator-enabled source admission.

    {
      "source": "syncopia-telegram-memory",
      "profile": "syncopia-summary-gemma12-v1",
      "kind": "chat",
      "model": "gemma4:12b",
      "options": {"num_ctx": 65536, "num_predict": 8192, "temperature": 0},
      "external_id": "synthetic-unique-example",
      "source_item_id": "synthetic-source-example",
      "payload": {
        "messages": [
          {"role": "system", "content": "Synthetic system."},
          {"role": "user", "content": "Synthetic source."}
        ],
        "tools": [], "stream": false, "think": false
      }
    }

Top-level model and optional payload model must both match the selected
profile. Options may be top-level or payload, but conflicting duplicates reject.
Model/options cannot escape the versioned contract through the generic override
stage. False is JSON Boolean false, NOT "false", "low", 0, null, omission or true.
The old Qwen path still requires "low"; it is not translated to off.

Server-owned truncate=false/shift=false are stored and forwarded. They are
requested native controls, not proof that a backend retains every prompt token.
A final's semantic completeness still requires evaluation.

## Resource rule and honest limits

Let M be canonical compact-JSON UTF-8 bytes of the complete messages array.
Admission requires:

- M <= 196608;
- positive integer context/output within the selected profile ceilings;
- temperature numeric zero (not Boolean);
- output + 4096 wrapper reserve < context;
- M + output + 4096 <= context.

M is a deliberately conservative scheduling proxy, NOT tokenizer output.
Rejection does NOT prove that the exact native tokenizer cannot fit the input.
A successful proxy check does NOT establish runtime token counts or VRAM fit.
No request is silently clamped, truncated or converted to smaller output.

Bolt's 65536 output / 65536 context leaves no input room and rejects before
execution. Other large-output/verbose-source cells can also reject under this
proxy. A less conservative admission
mechanism needs a separate reviewed exact native-token measurement contract;
do not weaken this version after receipts exist. Changing context/output changes the admitted identity; do not silently mutate an existing receipt.


## Native checks and execution

New admissions and dispatch independently read bounded native tags/show metadata.
They require the exact model digests pinned in `broker/syncopia_summary.py`,
completion/thinking capabilities, Boolean thinking values and expected native
context limits. These reads do not wake, load, run or download models. Exact
receipt recovery does not require a fresh metadata read, but never authorizes
execution without dispatch revalidation.

After execution, the result and `/api/ps` must match the model, digest and exact
requested runtime context. A final must be an assistant text response without
errors, tools or thinking output. Runtime evidence is not proof of semantic
quality; `quality_GO` remains false. Legacy Qwen thinking semantics are not
translated to Boolean false.

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=.:tests python3 -m unittest test_syncopia_summary
```

Tests use synthetic messages and fake model metadata, not private chat payloads.
