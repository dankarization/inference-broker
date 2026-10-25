# Opt-in technical-document evaluation

Four dedicated `/v1/jobs` profiles accept only inline `kind: chat` requests with
`source: pilot-mainpc`. They do not alter `/openclaw/api/chat`, its model mapping,
its history/tool contract, or any existing profile/source guard.

| Profile | Exact model |
| --- | --- |
| `eval-qwen3-vl-2b` | `qwen3-vl:2b-instruct-q4_K_M` |
| `eval-qwen3-vl-30b` | `qwen3-vl:30b` |
| `eval-gemma4-12b` | `gemma4:12b` |
| `eval-gemma4-26b` | `gemma4:26b` |

All four profiles use context 8192, output ceiling 2048, temperature 0, `think: false`,
`stream: false`, keep-alive 300 seconds and a 900-second executor timeout. Missing
options receive those server-owned values; conflicting values are rejected,
not clamped. Tools, assistant/tool history and producer-storage references are
not accepted. Up to 16 system/user messages and four nonempty base64 user images
are accepted. Existing native-chat bounds apply: decoded images total at most
24 MiB, image requests at most 32 MiB JSON, text-only requests at most 2 MiB,
JSON format/schema at most 64 KiB. Image encoding is checked, not decoded image
semantics or pixel dimensions. Optional `format` accepts `json` or a schema
object; this is a transport contract, not validation of generated JSON against
that schema. Terminal responses must be native assistant chat with `done: true`,
string content, the exact model tag and no tool calls. This does not establish
semantic correctness, non-truncation, or model capability.

## Exact matrix request example (prepare only)

The following stdlib command reads the frozen matrix and image without changing
them, then prints a runnable request to stdout. Set `task_index` to 0 or 1 and
select one of the four profiles. It does not perform inference.

```sh
python3 - <<'PY' > request.json
import base64, hashlib, json
from pathlib import Path
root = Path('/media/dankar/Plex/for claw/s114-techdocs-benchmark-20261024-50420')
task_index = 0
profile = 'eval-qwen3-vl-2b'  # or eval-qwen3-vl-30b
matrix = json.loads((root / 'models/comparison-matrix.json').read_text())
task = matrix['candidate_pairs'][task_index]
payload = {
    'messages': [{'role': 'user', 'content': task['prompt'],
                  'images': [base64.b64encode((root / task['input']).read_bytes()).decode()]}],
    'think': False, 'stream': False,
    'options': {'num_ctx': 8192, 'num_predict': 2048, 'temperature': 0},
}
request = {'profile': profile, 'kind': 'chat', 'source': 'pilot-mainpc', 'payload': payload}
encoded = json.dumps(request, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
request['external_id'] = 's114-eval-v2-' + hashlib.sha256(encoded).hexdigest()
request['source_item_id'] = 's114-50420-task-' + str(task_index)
print(json.dumps(request, ensure_ascii=False))
PY
```

Only after separately authorized deployment, policy reopening and evaluation:

```sh
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary @request.json http://192.168.2.19:8088/v1/jobs
```

Use the returned job ID to poll `/v1/jobs/ID` and `/v1/jobs/ID/attempts`.
Do not blindly retry uncertain admissions: reconcile by source/external ID at
`/v1/correlations` first. Keyed pilot replay now requires identical profile/kind, normalized input
hash and source item ID; retained identical jobs return before admission gates.
Changed requests conflict. Expired tombstones cannot recreate a job. Anonymous
legacy pilot submissions remain supported but do not provide durable DOC100 keys.
The stdlib `broker.pilot_client.PilotClient` never automatically replays an unknown
POST; an empty correlation search is not proof that admission never happened.
Pilot job responses additionally expose input hash/bytes for exact recovery after
terminal payload cleanup. Other sources' legacy serialization is unchanged. The frozen `run-comparison.py` still names OpenClaw profiles; it must
not be run unchanged. A separately owned evaluator must use these new profile
names, keep source `pilot-mainpc`, and reconcile prior admissions separately.

## Rollout and rollback (not executed here)

Keep `pilot-mainpc.admission_allowed` false during source review/installation.
Review all concurrent source changes against the current live bytes; do not
replace newer c4/Archivist/Bolt work with this base. Run the unit suite using
Plex temporary storage and stdlib Python >=3.11. Deploy through the established
broker release process only under a separate production authorization. Preserve
the prior immutable release, prove byte parity, restart only when authorized,
and check health before separately reopening admission for the bounded matrix.
No schema migration, new dependency, live policy edit or OpenClaw config change
is required by this source patch.

For rollback: stop admitting pilot jobs, let active jobs settle using the normal
broker procedure, restore the prior release and restart under authorization.
Already queued evaluation jobs require the new profiles: settle/cancel those
jobs before restoring old code, rather than leaving unknown profile names in
the queue. Existing DB/history/retention mechanisms are unchanged.
