"""Versioned text-only, final-only comparison; no network at import/admission.

Native /api/show thinking metadata is authoritative, not OpenClaw's enum.
No static claim that false/true (or named levels) work on a local artifact.
"""
from __future__ import annotations
import json
import math
import re
from .profiles import NIGHTLY_PROFILE_NAMES, PROFILES

SOURCE = "telegram-nightly-eval-20261028-v1"
SCHEMA = "telegram-nightly-eval-v1"
OPTIONS = dict(num_ctx=32768, num_predict=8192, temperature=0)
MAX_CALLS = 32
MAX_INPUT_BYTES = 128 * 1024
MAX_FINAL_BYTES = 128 * 1024
MODEL_DIGESTS = {
    "qwen3.8:ad-iq2-xs": "b6f02c5a32addd8a6b03ae205617bec9472903d9755e116084376fd429d100b9",
    "gemma4:12b": "4eb23ef187e2c5462566d6a1d3bbbc2f1346d0b4327cbb66d58fffbcc9b2b05c",
    "gemma4:26b": "001e5dafc3c77684c2307ebc6ab8e336e10c9b18eca52acf547d72fc83c3ca8c",
    "nemotron-3-nano:30b-a3b-q4_K_M": "b725f11174073334edd0c2ff396b8d4e66d7dab22a5a63717ccad5a08a270cf1",
}
THINK_LEVELS = {"low", "medium", "high", "max"}
COUNTERS = ("total_duration", "load_duration", "prompt_eval_count", "prompt_eval_duration",
            "eval_count", "eval_duration", "final_tokens")


def think_value(value):
    return type(value) is bool or (type(value) is str and value in THINK_LEVELS)


def same_value(left, right):
    return type(left) is type(right) and left == right


def thinking_metadata(raw):
    """No omission/default guessing: require native /api/show thinking.values."""
    value = raw.get("thinking") if isinstance(raw, dict) else None
    values = value.get("values") if isinstance(value, dict) else None
    if (not isinstance(values, list) or not 1 <= len(values) <= 6
            or not all(think_value(v) for v in values)
            or len({json.dumps(v) for v in values}) != len(values)
            or not any(same_value(value.get("default"), v) for v in values)):
        raise ValueError("native thinking metadata unavailable/invalid; do not guess modes")
    return dict(values=list(values), default=value["default"])


def supports(metadata, think):
    return think_value(think) and any(same_value(think, v) for v in metadata["values"])


def validate_messages(messages):
    if not isinstance(messages, list) or not 1 <= len(messages) <= 16:
        raise ValueError("nightly requires 1..16 system/user messages")
    for message in messages:
        if (not isinstance(message, dict) or set(message) != {"role", "content"}
                or message["role"] not in {"system", "user"}
                or not isinstance(message["content"], str)):
            raise ValueError("nightly text-only system/user messages required")
    if not any(m["role"] == "user" and m["content"].strip() for m in messages):
        raise ValueError("nightly requires nonempty user input")


def normalize_request(request, profile_name):
    if profile_name not in NIGHTLY_PROFILE_NAMES or not isinstance(request, dict):
        raise ValueError("dedicated nightly profile and object required")
    profile = PROFILES[profile_name]
    if set(request) - {"messages", "model", "think", "options", "stream", "truncate", "shift", "native_thinking"}:
        raise ValueError("nightly accepts text/freeform chat only")
    if not think_value(request.get("think")):
        raise ValueError("explicit native boolean/level required; no default coercion")
    for field in ("stream", "truncate", "shift"):
        if request.get(field, False) is not False:
            raise ValueError("nightly disables streaming, truncation and context shift")
    options = {**OPTIONS, **request.get("options", {})} if isinstance(request.get("options", {}), dict) else None
    if (not isinstance(options, dict) or set(options) != set(OPTIONS)
            or any(type(options[k]) not in (int, float) or type(options[k]) is bool
                   or (options[k] <= 0 if k != "temperature" else not math.isfinite(options[k])) or
                   (k != "temperature" and type(options[k]) is not int) for k in OPTIONS)):
        raise ValueError("nightly requires positive integer context/output and finite temperature")
    messages = request.get("messages")
    validate_messages(messages)
    metadata = thinking_metadata({"thinking": request.get("native_thinking")})
    if not supports(metadata, request["think"]):
        raise ValueError("requested native mode unsupported in accepted metadata")
    result = dict(messages=messages, think=request["think"], native_thinking=metadata, options=dict(options),
                  truncate=False, shift=False)
    if "model" in request:
        result["model"] = request["model"]
    if len(json.dumps(result, ensure_ascii=False).encode()) > MAX_INPUT_BYTES:
        raise ValueError("nightly input byte bound exceeded (not a token estimate)")
    return result


def native_contract(executor, profile_name, think, model=None, options=None):
    """Only during explicitly dispatched job; metadata calls never infer."""
    model = model or PROFILES[profile_name].model
    tags = executor.tags(timeout_seconds=5)
    rows = tags.get("models") if isinstance(tags, dict) else None
    matches = [r for r in rows if isinstance(r, dict) and r.get("name") == model] if isinstance(rows, list) else []
    digest = matches[0].get("digest") if len(matches) == 1 else None
    if not isinstance(digest, str) or re.fullmatch(r"(?:sha256:)?[a-f0-9]{64}", digest) is None or (model in MODEL_DIGESTS and digest.removeprefix("sha256:") != MODEL_DIGESTS[model]):
        raise ValueError("nightly installed model identity mismatch")
    metadata = thinking_metadata(executor.show(model, timeout_seconds=5))
    if not supports(metadata, think):
        raise ValueError("unsupported native thinking mode; off is not a named level")
    return dict(schema=SCHEMA, model=model, model_digest=digest.removeprefix("sha256:"),
                native_thinking=metadata, think=think, request_options=dict(options or OPTIONS))


def resident_context(executor, model, expected_context=32768, expected_digest=None):
    raw = executor.ps(timeout_seconds=5)
    rows = raw.get("models") if isinstance(raw, dict) else None
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise ValueError("nightly exclusive runtime context unavailable")
    row = rows[0]
    digest = row.get("digest")
    if (row.get("name") != model or not isinstance(digest, str)
            or digest.removeprefix("sha256:") != (expected_digest or MODEL_DIGESTS.get(model))
            or type(row.get("context_length")) is not int or row["context_length"] != expected_context):
        raise ValueError("nightly effective model/context mismatch")
    return row["context_length"]


def project_result(raw, model):
    """Final text/scalars only; NEVER retain native thinking/reasoning/errors."""
    message = raw.get("message") if isinstance(raw, dict) else None
    if (not isinstance(message, dict) or raw.get("model") != model
            or raw.get("done") is not True or message.get("role") != "assistant"
            or not isinstance(message.get("content"), str) or "tool_calls" in message
            or raw.get("error")):
        raise ValueError("invalid nightly final response")
    content = message["content"]
    if any(marker in content.lower() for marker in ("<think", "</think", "<|channel>", "[thought]", "[analysis]")):
        raise ValueError("nightly final contains unseparated reasoning markers")
    if len(content.encode()) > MAX_FINAL_BYTES:
        raise ValueError("nightly final byte bound exceeded")
    reason = raw.get("done_reason")
    result = dict(model=model, done=True, done_reason=reason if reason in ("stop", "length") else "unknown",
                  message=dict(role="assistant", content=content))
    for key in COUNTERS:
        value = raw.get(key)
        result[key] = value if type(value) is int and 0 <= value <= 10**15 else None
    # eval_count can include thinking. Never relabel it as final_tokens.
    result["truncated"] = reason == "length"
    result["acceptable"] = reason == "stop" and bool(content.strip())
    return result


def project_evidence(value):
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        raise ValueError("nightly execution evidence missing")
    model = value.get("model")
    if (not isinstance(model, str) or not isinstance(value.get("model_digest"), str)
            or re.fullmatch(r"[a-f0-9]{64}", value["model_digest"]) is None
            or (model in MODEL_DIGESTS and value["model_digest"] != MODEL_DIGESTS[model])
            or not isinstance(value.get("request_options"), dict)
            or value.get("runtime_context_length") != value["request_options"].get("num_ctx")):
        raise ValueError("nightly execution limits/identity mismatch")
    metadata = thinking_metadata({"thinking": value.get("native_thinking")})
    if not supports(metadata, value.get("think")):
        raise ValueError("nightly native mode mismatch")
    return dict(schema=SCHEMA, model=model, model_digest=value["model_digest"],
                think=value["think"], native_thinking=metadata, request_options=dict(value["request_options"]),
                runtime_context_length=value["runtime_context_length"], effective_output_limit=None,
                output_limit_evidence="request num_predict only; native effective ceiling unavailable")


def projection(job):
    if (not isinstance(job, dict) or job.get("source") != SOURCE
            or job.get("profile") not in NIGHTLY_PROFILE_NAMES):
        raise ValueError("dedicated nightly job required")
    result = {k: job.get(k) for k in (
        "id", "source", "profile", "kind", "external_id", "source_item_id", "input_hash",
        "input_bytes", "state", "created", "queued_at", "started", "finished", "attempt_count",
    )}
    for key in ("created", "queued_at", "started", "finished"):
        value = result[key]
        result[key] = value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None
    for name, start, end in (("queue_seconds", "queued_at", "started"),
                             ("execution_seconds", "started", "finished")):
        result[name] = max(0, result[end] - result[start]) if result[start] is not None and result[end] is not None else None
    result["projection"] = SCHEMA
    if isinstance(job.get("result"), dict):
        result["result"] = project_result(job["result"], job["result"].get("model", PROFILES[job["profile"]].model))
        result["execution_evidence"] = project_evidence(job["result"].get("execution_evidence", job.get("execution_evidence")))
    return result


def contract():
    return dict(schema=SCHEMA, source=SOURCE, max_calls=MAX_CALLS, max_outstanding=1,
                profiles=[dict(profile=name, model=PROFILES[name].model,
                    model_digest=MODEL_DIGESTS[PROFILES[name].model], options=dict(OPTIONS),
                    kind="chat", max_images=0, format="freeform", timeout_seconds=1800)
                    for name in sorted(NIGHTLY_PROFILE_NAMES)])
