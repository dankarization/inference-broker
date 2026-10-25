"""Opt-in, comparable image-chat evaluations; no agent or tool routing."""
from __future__ import annotations

import base64

from .openclaw import normalize_request as normalize_chat, validate_result as validate_chat
from .profiles import EVALUATION_PROFILE_NAMES, PROFILES


def normalize_request(request: dict, profile_name: str) -> dict:
    if profile_name not in EVALUATION_PROFILE_NAMES:
        raise ValueError("unknown evaluation profile")
    if not isinstance(request, dict) or set(request) - {
        "model", "messages", "stream", "options", "format", "think", "truncate", "shift"
    }:
        raise ValueError("unsupported evaluation request fields")
    profile = PROFILES[profile_name]
    if "model" in request and request["model"] != profile.model:
        raise ValueError("evaluation model must match its profile")
    if request.get("think", False) is not False or request.get("stream", False) is not False:
        raise ValueError("evaluation requires think false and stream false")
    options = request.get("options", {})
    expected = {"num_ctx": profile.max_context, "num_predict": profile.max_output,
                "temperature": 0}
    if not isinstance(options, dict) or set(options) - expected.keys():
        raise ValueError("unsupported evaluation options")
    for key, value in options.items():
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or value != expected[key]
                or (key != "temperature" and not isinstance(value, int))):
            raise ValueError(f"evaluation requires {key}={expected[key]}")
    # Reuse bounded native-chat validation without changing OpenClaw's map,
    # source guard, or history contract. The lane's stricter checks stay here.
    payload = normalize_chat({**request, "model": profile.model, "think": False,
                              "stream": False, "options": expected})
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
    return payload


def validate_result(result: dict, model: str) -> None:
    validate_chat(result, model)
    if result.get("model") != model or "tool_calls" in result["message"]:
        raise RuntimeError("Ollama returned an invalid evaluation chat response")
