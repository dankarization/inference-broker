# Bolt E4B: opt-in queued native chat

Exact model: `nemotron-3.5-bolt:e4b`; server profile: `openclaw-bolt-e4b`.
Use POST /openclaw/api/chat (or broker submission with source=openclaw,
kind=chat and that exact profile). It uses the existing durable queue/resource,
source controls, idempotency and storage contracts; no new source is needed.
This does not enable an OpenClaw default or change the model catalog.

## Evidence, 2026-10-24

Read-only MAIN-PC /api/version: 0.40.10. After model download completed,
/api/show returned renderer=gemma4-nemotron, parser=gemma4,
template={{ .Prompt }}, template_source=renderer_parser, requires=0.40.10;
thinking.values=[false,true], thinking.default=true;
model_info.gemma4.context_length=65536. Capabilities include completion,
vision, audio, tools, thinking. Projector clip.has_vision_encoder=true,
clip.vision.projector_type=gemma4v, clip.vision.image_size=224.
This establishes executor-advertised image support, NOT successful inference.
Audio is NOT admitted by this native broker contract.

Registry manifest https://registry.ollama.ai/v2/library/nemotron-3.5-bolt/manifests/e4b
pins model sha256:c8af2b8aea10a690a1d9491db744a9446660173cb38fc489612b556615a69612,
projector sha256:3507502e413925a5c827061c08ee234687a605f0daca438ff0d835e87dc49ea8.
Native GGUF header independently reports gemma4.context_length=65536.

Primary NVIDIA card:
https://huggingface.co/nvidia/NVIDIA-Nemotron-3.5-Bolt-E4B-4GB
publishes input/output up to 64K and enable_thinking true/false. Its runtime
reasoning budget is a llama-server --reasoning-budget control, not an Ollama
chat option. The Ollama family page https://ollama.com/library/nemotron-3.5-bolt
also mentions 256K; that is NOT this exact E4B artifact's native ceiling.
Pinned implementation evidence:
https://github.com/ollama/ollama/blob/v0.40.10/api/types.go
https://github.com/ollama/ollama/blob/v0.40.10/model/renderers/gemma4.go
https://github.com/ollama/ollama/blob/v0.40.10/model/renderers/renderer.go
The API Options has no reasoning-budget field; Gemma4 renderer uses boolean
thinking, not low/medium/high/max. Live /api/show is authoritative for default.

## Bounded contract

- think=false: direct-answer mode; think=true: reasoning enabled.
- Omitted think: preserve executor default (currently true). Explicit null,
  numeric 0/1, low/medium/high/max are rejected. No invented discrete levels.
- No thinking_budget/reasoning_budget, chat_template_kwargs/enable_thinking,
  or conversational-mode kwargs through this API. Unsupported fields reject.
- options.num_predict is a TOTAL generation cap, not a reasoning-only budget.
  Positive integers 1..8192; default 8192 (broker safety policy, not model max).
- options.num_ctx: positive integers 1..65536, default 65536. Above ceiling
  rejects, rather than silently clamps; output over broker cap also rejects.
  Prompt/template/tools/images/history and output share available context.
- truncate=false and shift=false are forced; attempts to enable either reject.
- Execution timeout 1800s; existing HTTP waiter automatically allows 2100s.
- Up to four base64 images across user messages, existing 24MiB decoded /
  32MiB request bound. Ordered image lists can carry caller-sampled video
  frames with timestamps in text; this is NOT native video/audio ingestion,
  decoding, sampling, temporal acceptance or a media pipeline change.

Example (send only through broker, not directly to executor):

    {"model":"nemotron-3.5-bolt:e4b","messages":[{"role":"user","content":"probe"}],
     "think":false,"options":{"num_ctx":8192,"num_predict":512},
     "truncate":false,"shift":false}

## Main-owned controlled acceptance (not run by this change)

First test both boolean modes on identical text/tool/image fixtures and fixed
sampling/output limits. Record omission/default separately, not as a third mode.
Then ladder num_ctx 8192,16384,32768,49152,65536; reserve output and template
headroom, independently measure occupied prompt tokens. Test near failure
boundary without shortening, compacting, fallback models, truncation or shift.
Record exact model, requested/dispatch num_ctx, input hash, prompt_eval_count,
eval_count, done_reason, reasoning/text, recalls at beginning/middle/end,
latencies and executor logs. A configured window or completed job alone does
not prove occupied-context success. Length termination is not quality success.
Image/video-frame fixture comparison must use identical ordered frames in both
modes; image support is not native-video support. No inference was run here.

## Rollout / rollback (main only)

Recheck live DEPLOYED_HEAD and source parity; this commit is based on deployed
69625e878ee017ca9bba49958160b4a0e8719420. Coordinate any concurrent Nextcloud
release before composing a new target; never overwrite it with this old base.
Review/test immutable target export, back up runtime, drain queue, then use the
existing authorized rollout procedure. No schema/config/scheduler change is
required. Restore prior source release on rollback only after reconciling any
pending jobs using the new profile: old code cannot dispatch that profile.
