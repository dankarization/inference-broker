from dataclasses import dataclass


@dataclass(frozen=True)
class Profile:
    name: str
    model: str
    max_context: int
    max_output: int
    keep_alive_seconds: int
    # These are admission/dispatch limits, not caller-controlled Ollama options.
    max_concurrency: int = 1
    min_interval_seconds: int = 0
    request_timeout_seconds: int = 300
    max_images: int = 0
    max_schema_bytes: int = 0


# This is deliberately server-owned. Callers can select a profile, never a model
# or unbounded generation parameters.
PROFILES = {
    "eval-gemma4-12b": Profile(
        "eval-gemma4-12b", "gemma4:12b", 8_192, 2_048, 300,
        request_timeout_seconds=900, max_images=4,
    ),
    "eval-gemma4-26b": Profile(
        "eval-gemma4-26b", "gemma4:26b", 8_192, 2_048, 300,
        request_timeout_seconds=900, max_images=4,
    ),
    # Opt-in independent visual verification. Qualification belongs to caller.
    "nextcloud-verify-gemma26": Profile(
        "nextcloud-verify-gemma26", "gemma4:26b", 16_384, 4_096, 300,
        max_images=4, request_timeout_seconds=900,
    ),
    # Worker004 generate lane, distinct from isolated pilot image-chat evaluation.
    "nextcloud-eval-qwen": Profile(
        "nextcloud-eval-qwen", "qwen3-vl:30b", 16_384, 8_192, 300,
        max_images=4, request_timeout_seconds=900,
    ),
    # Explicit opt-in technical-document evaluation; never an OpenClaw route.
    "eval-qwen3-vl-2b": Profile(
        "eval-qwen3-vl-2b", "qwen3-vl:2b-instruct-q4_K_M", 8_192, 2_048, 300,
        request_timeout_seconds=900, max_images=4,
    ),
    "eval-qwen3-vl-30b": Profile(
        "eval-qwen3-vl-30b", "qwen3-vl:30b", 16_384, 8_192, 300,
        request_timeout_seconds=900, max_images=4,
    ),
    "nextcloud-qwen3-vl": Profile(
        "nextcloud-qwen3-vl", "qwen3-vl:30b", 16_384, 8_192, 300,
        max_images=4, request_timeout_seconds=900,
    ),
    "interactive": Profile("interactive", "nemotron3:33b", 16_384, 2_048, 180),
    "cron": Profile("cron", "nemotron3:33b", 8_192, 1_024, 120),
    # Preserve the original profile name for already-queued agent turns.
    "openclaw": Profile("openclaw", "qwen3.8:ad-iq2-xs", 131_072, 16_384, 300,
                        request_timeout_seconds=900),
    "openclaw-gemma4": Profile("openclaw-gemma4", "gemma4:12b", 262_144, 16_384, 300,
                               request_timeout_seconds=900, max_images=16),
    # Native metadata ceiling, not a claim of validated occupied context.
    # Bound long-prefill attempts to one hour; other model lanes are unchanged.
    "openclaw-gemma4-26b": Profile("openclaw-gemma4-26b", "gemma4:26b", 262_144, 8_192, 300,
                                   request_timeout_seconds=3600, max_images=16),
    "openclaw-qwen3-vl": Profile("openclaw-qwen3-vl", "qwen3-vl:30b", 212_992, 16_384, 300,
                                request_timeout_seconds=900, max_images=16),
    "openclaw-nemotron3": Profile("openclaw-nemotron3", "nemotron3:33b", 131_072, 8_192, 300,
                                 request_timeout_seconds=900, max_images=16),
    "openclaw-unc-rvn-iq2xxs": Profile("openclaw-unc-rvn-iq2xxs", "qwen3.8:unc-rvn-iq2xxs",
                                      131_072, 16_384, 300, request_timeout_seconds=900),
    "openclaw-ministral3": Profile("openclaw-ministral3", "frob/ministral-3:14b-thinking-q4_K_M",
                                   262_144, 16_384, 300, request_timeout_seconds=900, max_images=16),
    "openclaw-nemotron-nano": Profile("openclaw-nemotron-nano", "nemotron-3-nano:30b-a3b-q4_K_M",
                                      262_144, 16_384, 300, request_timeout_seconds=900),
    "openclaw-gpt-oss": Profile("openclaw-gpt-oss", "gpt-oss:20b", 131_072, 16_384, 300,
                                request_timeout_seconds=900),
    # Exact artifact metadata ceiling; not measured context/vision acceptance.
    "openclaw-bolt-e4b": Profile(
        "openclaw-bolt-e4b", "nemotron-3.5-bolt:e4b", 65_536, 8_192, 300,
        request_timeout_seconds=1800, max_images=4,
    ),
    # Opt-in bounded smoke lanes only. Admission does not establish executor
    # vision support or agent/tool compatibility for these model artifacts.
    "openclaw-qwen3-vl-2b": Profile(
        "openclaw-qwen3-vl-2b", "qwen3-vl:2b-instruct-q4_K_M", 8_192, 2_048, 300,
        request_timeout_seconds=600, max_images=4,
    ),
    "openclaw-smolvlm2-2b": Profile(
        "openclaw-smolvlm2-2b", "hf.co/ggml-org/SmolVLM2-2.2B-Instruct-GGUF:Q4_K_M",
        8_192, 2_048, 300, request_timeout_seconds=600, max_images=4,
    ),
    "openclaw-smolvlm2-500m": Profile(
        "openclaw-smolvlm2-500m", "hf.co/ggml-org/SmolVLM2-500M-Video-Instruct-GGUF:Q8_0",
        8_192, 2_048, 300, request_timeout_seconds=600, max_images=4,
    ),
    # Shutterstock photos stay on cloud/OmniRoute and deliberately have no
    # broker profile. Only the local video workload may acquire MAIN-PC.
    # The video lane is multimodal: one chunk arrives as base64 frames plus a
    # JSON Schema contract, so the profile admits bounded media input.
    "shutterstock-video": Profile(
        "shutterstock-video", "nemotron3:33b", 16_384, 1_024, 120,
        max_images=12, max_schema_bytes=32_768, request_timeout_seconds=600,
    ),
    # `olya` is intentionally only a source/profile key. Do not infer an
    # integration from this name; its model can be changed in broker config.
    "olya": Profile("olya", "nemotron3:33b", 8_192, 1_024, 120),
    # Photos-only Olya keeps its verified two-model routing. Both profiles use
    # one dedicated weighted source; callers may select only these exact
    # server-owned profiles via the bounded synchronous VLM endpoint.
    "olya-vision-gemma": Profile(
        "olya-vision-gemma", "gemma4:12b", 245_760, 4_096, 1_800,
        max_concurrency=1, request_timeout_seconds=600,
        max_images=16, max_schema_bytes=65_536,
    ),
    "olya-vision-qwen": Profile(
        "olya-vision-qwen", "qwen3-vl:30b", 212_992, 4_096, 1_800,
        max_concurrency=1, request_timeout_seconds=600,
        max_images=16, max_schema_bytes=65_536,
    ),
    # Text-only apartment decision lane. The exact local model is pinned
    # server-side; callers cannot replace it or expand its runtime limits.
    "olya-decision-qwen38": Profile(
        "olya-decision-qwen38", "qwen3.8:ad-iq2-xs", 32_768, 4_096, 1_800,
        max_concurrency=1, request_timeout_seconds=300,
        max_schema_bytes=16_384,
    ),
    # Phase-2 Telegram-memory extraction is a separate text-only source.  The
    # 64k context and schema/freeform thinking modes are enforced again by its
    # compatibility contract; callers cannot borrow the Olya lane or widen limits.
    "syncopia-memory-qwen38": Profile(
        "syncopia-memory-qwen38", "qwen3.8:ad-iq2-xs", 65_536, 8_192, 1_800,
        max_concurrency=1, request_timeout_seconds=900,
        max_schema_bytes=65_536,
    ),
    "batch-video": Profile("batch-video", "nemotron3:33b", 8_192, 512, 120),
    # This is an intentionally separate source from the active Shutterstock
    # worker.  It is the only VLM/media contract eligible for a future canary.
    "shutterstock-canary": Profile(
        "shutterstock-canary", "qwen3-vl:30b", 16_384, 512, 60,
        max_concurrency=1, min_interval_seconds=60, request_timeout_seconds=300,
        max_images=4, max_schema_bytes=16_384,
    ),
}

_OPENCLAW_PROFILE_NAMES = (
    "openclaw", "openclaw-gemma4", "openclaw-qwen3-vl", "openclaw-nemotron3",
    "openclaw-unc-rvn-iq2xxs", "openclaw-ministral3", "openclaw-nemotron-nano",
    "openclaw-gpt-oss", "openclaw-gemma4-26b", "openclaw-bolt-e4b",
    "openclaw-qwen3-vl-2b", "openclaw-smolvlm2-2b", "openclaw-smolvlm2-500m",
)
OPENCLAW_PROFILES_BY_MODEL = {
    PROFILES[name].model: name for name in _OPENCLAW_PROFILE_NAMES
}
OPENCLAW_PROFILE_NAMES = frozenset(OPENCLAW_PROFILES_BY_MODEL.values())

EVALUATION_PROFILE_NAMES = frozenset({
    "eval-qwen3-vl-2b", "eval-qwen3-vl-30b", "eval-gemma4-12b", "eval-gemma4-26b",
})
