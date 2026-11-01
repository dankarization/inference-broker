"""Opt-in, comparable image-chat evaluations; no agent or tool routing."""
from __future__ import annotations

import base64

from .openclaw import normalize_request as normalize_chat, validate_result as validate_chat
from .profiles import EVALUATION_PROFILE_NAMES, PROFILES

# Independent pilot admission budget, NOT the Nextcloud256 budget.
PILOT_MAX_ACTIVE_JOBS = 512
PILOT_MAX_ACTIVE_INPUT_BYTES = 512 * 1024**2
PILOT_MODEL_DIGESTS = {
    "eval-qwen3-vl-2b": "sha256:afc393e3572c07db0bc84aa460d928a16c75f3fbb9ca9e0dccc4fbd2dafb573e",
    "eval-qwen3-vl-30b": "sha256:eda0be1008774ea4e575187712b9a3b1201f3e66e7ecff899cc295db1cf00248",
    "eval-gemma4-12b": "sha256:4eb23ef187e2c5462566d6a1d3bbbc2f1346d0b4327cbb66d58fffbcc9b2b05c",
    "eval-gemma4-26b": "sha256:001e5dafc3c77684c2307ebc6ab8e336e10c9b18eca52acf547d72fc83c3ca8c",
}


def normalize_request(request: dict, profile_name: str) -> dict:
    if profile_name not in EVALUATION_PROFILE_NAMES:
        raise ValueError("unknown evaluation profile")
    if not isinstance(request, dict) or set(request) - {
        "model", "messages", "stream", "options", "format", "think", "truncate", "shift"
    }:
        raise ValueError("unsupported evaluation request fields")
    profile = PROFILES[profile_name]
    if request.get("think", False) is not False or request.get("stream", False) is not False:
        raise ValueError("evaluation requires think false and stream false")
    options = request.get("options", {})
    expected = {"num_ctx": profile.max_context, "num_predict": profile.max_output,
                "temperature": 0}
    if not isinstance(options, dict) or set(options) - expected.keys():
        raise ValueError("unsupported evaluation options")
    for key, value in options.items():
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or (key != "temperature" and not isinstance(value, int))):
            raise ValueError(f"evaluation requires {key}={expected[key]}")
    # Reuse bounded native-chat validation without changing OpenClaw's map,
    # source guard, or history contract. The lane's stricter checks stay here.
    payload = normalize_chat({**request, "model": request.get("model", profile.model), "think": False,
                              "stream": False, "options": {**expected, **options}}, profile_override=profile)
    messages = payload["messages"]
    if len(messages) > 16 or not any(m["role"] == "user" for m in messages):
        raise ValueError("evaluation requires user input and at most 16 messages")
    for message in messages:
        if (message["role"] not in {"system", "user"}
                or set(message) - {"role", "content", "images"}):
            raise ValueError("evaluation accepts only system/user text and user images")
        for image in message.get("images", []):
            if not base64.b64decode(image, validate=True):
                raise ValueError("evaluation images must not be empty")
    # Keep this bound independent of the existing OpenClaw profiles.
    if sum(len(m.get("images", [])) for m in messages) > profile.max_images:
        raise ValueError("too many evaluation images")
    if "model" in request:
        payload["model"] = request["model"]
    return payload


def validate_result(result: dict, model: str) -> None:
    validate_chat(result, model)
    if result.get("model") != model or "tool_calls" in result["message"]:
        raise RuntimeError("Ollama returned an invalid evaluation chat response")
