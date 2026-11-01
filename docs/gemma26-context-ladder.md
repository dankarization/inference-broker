# Gemma 26B: MAIN-owned Gateway context ladder

This is a task runner, not a monitoring service. The feature owner prepares and tests it without inference. MAIN alone runs each attempt, admits the next rung, edits OpenClaw config, and reports results. No direct Ollama generation/chat calls occur in the runner.

## Admission/config (MAIN)

Use a dedicated agent with the same bootstrap/tools configuration throughout calibration and ladder. Set its explicit model object to:
{"primary":"local/gemma4:26b","fallbacks":[]}.
This suppresses inherited cloud/model fallbacks without changing the default primary/fallbacks for other agents. Keep this agent tool-free using supported agent tools policy; prompts also request no tools. A fresh UUID session/key is generated every attempt.

For each C in 32768, 65536, 131072, 196608, 262144, change only the Gemma model/test overrides through supported OpenClaw config tooling:

- models.providers.local.api = "ollama"; baseUrl = "http://192.168.2.19:8088/openclaw".
- Gemma entry in models.providers.local.models: contextWindow=C, maxTokens=512; params.num_ctx=C, params.num_predict=512, params.thinking=false.
- agents.defaults.models["local/gemma4:26b"].params: num_ctx=C, num_predict=512, thinking=false.
- Remove/conflict-check any per-agent model parameter override that could supersede those settings.
- Gateway turn timeout must accommodate 4200 seconds (runner explicitly uses --timeout 4200).
- Broker dispatch budget is 3600 seconds; native waiter 3900 seconds. No other profile changes.

Do not switch api/baseUrl away from this existing broker route. Do not change global primary/fallback defaults. Validate config and confirm Gateway has adopted it before starting. Pass the actual live strict-JSON config file, not a proposed/stale candidate, to --config-file; it is only read and is never copied to evidence. No credentials are emitted.

Leave bootstrap and tool policy constant. The runner rejects missing provider proof, nonlocal provider/model, compacted history, altered/missing prompt, stale assistant/tool messages, multiple matching jobs, wrong source/profile/options, missing native tokens, and service restarts. It requires an idle broker before submitting a turn. CLI runs via Gateway, without --local or --deliver. Gateway transport failure does not request embedded rerun.

## Prepare without inference

From repository root:

    python3 scripts/gemma26_context.py prepare --out /tmp/gemma26-context/prepared
    python3 scripts/gemma26_context.py prepare --out /tmp/gemma26-context/pilots --context 32768 --target-tokens 12288
    python3 scripts/gemma26_context.py prepare --out /tmp/gemma26-context/pilots --context 32768 --target-tokens 20480

Files are deterministic UTF-8 with diverse shuffled natural-language/structured records. Vault codes occur once each near 2%, 50%, 98%. Answers are in separate *.expected.json files; never send those manifests to the model. The final prompt instruction does not disclose answers.

Initial files are byte-size estimates, NOT tokenizer measurements or validated context occupancy. Keep each new run output directory unique. Main runs the two pilots with C=32768 (one at a time):

    python3 scripts/gemma26_context.py run \
      --manifest /tmp/gemma26-context/pilots/ctx-32768-target-12288.expected.json \
      --agent TEST_AGENT --config-file /home/dankar/.openclaw/openclaw.json \
      --out /tmp/gemma26-context/pilot-a

Repeat with target-20480 and output pilot-b. The same config/bootstrap must be used. Uncalibrated pilots intentionally exit 2 even if recall succeeds; inspect report.json/path_verified and actual tokens. If these byte estimates overfill given the bootstrap, stop, reduce the pilot target, and regenerate. Never retry silently or reuse the session.

## Calibrate and run one rung at a time

Fit user-byte/token slope and bootstrap intercept from native prompt_eval_count in the two verified pilot reports. No approximate third-party tokenizer is called:

    python3 scripts/gemma26_context.py prepare \
      --out /tmp/gemma26-context/calibrated \
      --calibration /tmp/gemma26-context/pilot-a/report.json /tmp/gemma26-context/pilot-b/report.json

This targets TOTAL native prompt tokens C - 512 output - 8192 safety reserve (e.g. 253440 at 262144), accounting for calibrated bootstrap intercept. Default reserve can be increased with --reserve; keep headroom for injected OpenClaw system/tools. Calibration is workload-specific, not an exact native tokenize endpoint. Records may tokenize differently with size: use actual measurements to iterate, never presume the estimate prevented native truncation.

After MAIN sets/verifies C, run exactly that rung:

    python3 scripts/gemma26_context.py run \
      --manifest /tmp/gemma26-context/calibrated/ctx-32768-target-24064.expected.json \
      --agent TEST_AGENT --config-file /home/dankar/.openclaw/openclaw.json \
      --out /tmp/gemma26-context/rung-32768-a

Review report and raw evidence BEFORE manually changing config/admitting the next C. A rung passes only with calibration, actual native prompt tokens within ±5% of target and below C-512, all three needle hits, path verification and unchanged broker PID/restart count. For tighter occupancy increase target using --target-tokens after measured calibration; never call configured num_ctx "actual tokens". Near-target miss or recall miss exits 2. Underfilled attempts are useful calibration evidence, not a pass. Preserve failed attempts.

Each attempt records CLI stdout/stderr, command/session, broker raw payload/result and job ID/source, actual prompt_eval_count/eval_count, native load/prefill/decode/total durations (ns), health, read-only /api/ps before/after, and systemd PID/NRestarts/WorkingDirectory. Broker telemetry is read-only over existing SSH; no new endpoint/system is installed. If CLI/verification fails, raw evidence remains; do not automatically rerun or advance. On an ambiguous timeout inspect whether the original job is still active before any action.

After each valid rung MAIN may leave its validated C. Do not automatically leave 262144 after a failure, and do not claim 256k passed merely because deployment permits it. Restore model maxTokens/output test overrides separately if desired, without raising final C above the last actually validated rung.
