"""Prospective opt-in full-day text capacity; no network or producer mutations."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .profiles import PROFILES
from .storage import content_evidence

PROFILE = "syncopia-memory-qwen38-128k-v1"
CONTRACT = "syncopia-memory-daily-text-128k-v1"
SOURCE = "syncopia-telegram-memory"
MODEL = "qwen3.8:ad-iq2-xs"
CONTEXT = 131_072
OUTPUT = 8_192
WRAPPER_RESERVE = 4_096
MAX_MESSAGE_BYTES = CONTEXT - OUTPUT - WRAPPER_RESERVE
OPTIONS = dict(temperature=0, num_ctx=CONTEXT, num_predict=OUTPUT)


def _profile_guard() -> None:
    profile = PROFILES[PROFILE]
    if (profile.name, profile.model, profile.max_context, profile.max_output,
            profile.keep_alive_seconds, profile.max_concurrency,
            profile.request_timeout_seconds, profile.max_images, profile.max_schema_bytes) != (
            PROFILE, MODEL, CONTEXT, OUTPUT, 1_800, 1, 900, 0, 0):
        raise ValueError("daily capacity profile drift")


def describe() -> dict[str, Any]:
    _profile_guard()
    return dict(schema=CONTRACT, profile=PROFILE, source=SOURCE, kind="chat",
                model=MODEL, think="low", options=dict(OPTIONS),
                max_concurrency=1, keep_alive_seconds=1_800,
                request_timeout_seconds=900, producer_storage_required=True,
                max_serialized_message_utf8_bytes=MAX_MESSAGE_BYTES,
                wrapper_reserve=WRAPPER_RESERVE,
                budget_method="compact messages UTF-8 bytes + output + wrapper reserve",
                tokenizer_count=None, native_effective_settings=None,
                input_transform="none; preserve exact system/user role and content",
                deployment_required=True, legacy_profile_unchanged="syncopia-memory-qwen38")


def _messages(request: dict[str, Any]) -> list[dict[str, str]]:
    messages = request.get("messages")
    if (not isinstance(messages, list) or len(messages) != 2
            or any(not isinstance(item, dict) or set(item) != {"role", "content"}
                   or not isinstance(item.get("content"), str) for item in messages)
            or [item.get("role") for item in messages] != ["system", "user"]):
        raise ValueError("daily text requires exactly system/user string role/content only")
    return [dict(role=item["role"], content=item["content"]) for item in messages]


def _message_bytes(messages: list[dict[str, str]]) -> int:
    try:
        return len(json.dumps(messages, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    except UnicodeEncodeError:
        raise ValueError("daily messages must be valid UTF-8 text") from None


def normalize_request(request: Any) -> dict[str, Any]:
    """Idempotent, pure executable payload; its canonical hash is storage identity.

    Optional protocol-only model/tools/stream fields are checked then omitted.
    Explicit options are mandatory; drift is rejected, not clamped or inherited.
    No file/media/URL dereference, message slicing, schema or thinking inference.
    """
    _profile_guard()
    if not isinstance(request, dict) or set(request) - {
            "model", "messages", "tools", "stream", "think", "options"}:
        raise ValueError("daily payload has unsupported fields")
    if "model" in request and request["model"] != MODEL:
        raise ValueError("daily model must match the pinned profile")
    if "tools" in request and request["tools"] != []:
        raise ValueError("daily text forbids tools")
    if "stream" in request and request["stream"] is not False:
        raise ValueError("daily text requires stream=false")
    if request.get("think") != "low":
        raise ValueError("daily text requires configured think=low; low is not off")
    options = request.get("options")
    if (not isinstance(options, dict) or set(options) != set(OPTIONS)
            or type(options.get("temperature")) not in (int, float)
            or options["temperature"] != 0
            or type(options.get("num_ctx")) is not int or options["num_ctx"] != CONTEXT
            or type(options.get("num_predict")) is not int or options["num_predict"] != OUTPUT):
        raise ValueError("daily text requires exact temperature=0/num_ctx=131072/num_predict=8192")
    messages = _messages(request)
    if _message_bytes(messages) > MAX_MESSAGE_BYTES:
        raise ValueError("daily input byte bound exceeds capacity; no truncation or splitting")
    return dict(messages=messages, think="low", options=dict(OPTIONS))


def input_proof(request: Any) -> dict[str, Any]:
    """Scalar-only offline evidence, deliberately not tokenizer/runtime evidence."""
    payload = normalize_request(request)
    message_bytes = _message_bytes(payload["messages"])
    input_hash, input_bytes = content_evidence(payload)
    bound = message_bytes + OUTPUT + WRAPPER_RESERVE
    return dict(schema=CONTRACT, profile=PROFILE, source=SOURCE, model=MODEL,
                think="low", options=dict(OPTIONS), input_hash=input_hash,
                input_bytes=input_bytes, serialized_message_utf8_bytes=message_bytes,
                conservative_input_output_wrapper_bound=bound,
                margin_under_context=CONTEXT - bound, admitted_by_byte_policy=True,
                tokenizer_count=None, native_effective_settings=None)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("describe")
    proof = sub.add_parser("prove")
    proof.add_argument("--payload-json", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "describe":
            result = describe()
        else:
            # Bounded local JSON input only; no URLs, secrets or raw payload output.
            if args.payload_json.stat().st_size > 1_048_576:
                raise ValueError("local payload file exceeds the proof reader limit")
            result = input_proof(json.loads(args.payload_json.read_text(encoding="utf-8")))
    except (OSError, ValueError, TypeError, KeyError):
        print(json.dumps(dict(error="daily capacity proof rejected; no raw input/error retained")))
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
