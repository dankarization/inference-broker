"""Versioned opt-in native-boolean summaries; never weakens legacy memory."""
from copy import deepcopy
import json
import re
from .profiles import PROFILES

SOURCE = "syncopia-telegram-memory"
VERSION = "syncopia-summary-native-v1"
MODELS = {
    "syncopia-summary-gemma12-v1": ("gemma4:12b", "4eb23ef187e2c5462566d6a1d3bbbc2f1346d0b4327cbb66d58fffbcc9b2b05c", 262144),
    "syncopia-summary-bolt-e4b-v1": ("nemotron-3.5-bolt:e4b", "69f2be39b309ade35ef04ee02507915f4cd839f6631e14425146cb6c9df3bd39", 65536),
}
MAX_MESSAGE_BYTES = 196608
WRAPPER_RESERVE = 4096


def normalize_request(request, profile_name, model=None, options=None, *, persisted=False):
    if profile_name not in MODELS or not isinstance(request, dict):
        raise ValueError("summary v1 requires dedicated profile and object")
    allowed = {"model", "messages", "tools", "stream", "think", "options"}
    if persisted:
        allowed |= {"summary_contract", "truncate", "shift"}
    if set(request) - allowed:
        raise ValueError("summary v1 accepts text/freeform only")
    expected = MODELS[profile_name][0]
    if any(value is not None and value != expected for value in (model, request.get("model"))):
        raise ValueError("summary v1 model must match dedicated profile")
    if request.get("think") is not False:
        raise ValueError("summary v1 requires native boolean think=false")
    if request.get("tools") != [] or request.get("stream") is not False:
        raise ValueError("summary v1 requires tools=[] and stream=false")
    if persisted and (request.get("truncate") is not False or request.get("shift") is not False):
        raise ValueError("summary v1 persisted truncation controls drift")
    messages = request.get("messages")
    if (not isinstance(messages, list) or len(messages) != 2 or
        any(not isinstance(m, dict) or set(m) != {"role", "content"} or
            not isinstance(m["content"], str) for m in messages) or
        [m["role"] for m in messages] != ["system", "user"]):
        raise ValueError("summary v1 requires exact System/User text-only messages")
    try:
        size = len(json.dumps(messages, ensure_ascii=False, separators=(",", ":")).encode())
    except (ValueError, TypeError) as exc:
        raise ValueError("summary v1 messages must be valid UTF-8") from exc
    if size > MAX_MESSAGE_BYTES:
        raise ValueError("summary v1 message byte bound exceeded")
    supplied = request.get("options", {})
    if not isinstance(supplied, dict) or (options is not None and not isinstance(options, dict)):
        raise ValueError("summary v1 options must be object")
    options = options or {}
    if any(supplied[k] != options[k] or type(supplied[k]) is not type(options[k])
           for k in supplied.keys() & options.keys()):
        raise ValueError("summary v1 conflicting duplicate options")
    selected = {**supplied, **options}
    if (set(selected) != {"temperature", "num_ctx", "num_predict"} or
        any(type(selected[k]) is not int or selected[k] <= 0 for k in ("num_ctx", "num_predict")) or
        type(selected["temperature"]) not in (int, float) or selected["temperature"] != 0):
        raise ValueError("summary v1 requires explicit positive integer budgets and temperature=0")
    profile = PROFILES[profile_name]
    ctx, out = selected["num_ctx"], selected["num_predict"]
    if ctx > profile.max_context:
        raise ValueError("summary v1 requested context exceeds profile ceiling")
    if out + WRAPPER_RESERVE >= ctx:
        raise ValueError("summary v1 output plus wrapper leaves no input room")
    if out > profile.max_output:
        raise ValueError("summary v1 requested output exceeds profile ceiling")
    # A conservative scheduling proxy, NOT an observed tokenizer/runtime count.
    if size + out + WRAPPER_RESERVE > ctx:
        raise ValueError("summary v1 conservative UTF-8 input proxy exceeds input room; token fit unmeasured")
    return dict(model=expected, messages=deepcopy(messages), tools=[], stream=False,
                think=False, options=dict(selected), truncate=False, shift=False)


def budget(payload):
    size = len(json.dumps(payload["messages"], ensure_ascii=False, separators=(",", ":")).encode())
    return dict(input_utf8_proxy=size, wrapper_reserve=WRAPPER_RESERVE,
                requested=payload["options"], input_room=payload["options"]["num_ctx"] -
                payload["options"]["num_predict"] - WRAPPER_RESERVE,
                tokenizer="NOT_USED", runtime_fit="UNMEASURED")


def native_contract(executor, profile_name):
    """Read-only metadata only; never load/infer/download or change options."""
    model, pinned, native_limit = MODELS[profile_name]
    try:
        tags = executor.tags(timeout_seconds=5)
        show = executor.show(model, timeout_seconds=5)
    except (AttributeError, OSError, RuntimeError, ValueError) as exc:
        raise ValueError("summary v1 native metadata unavailable") from exc
    rows = tags.get("models") if isinstance(tags, dict) else None
    matches = [m for m in rows if isinstance(m, dict) and m.get("name") == model] if isinstance(rows, list) else []
    identity = matches[0].get("digest") if len(matches) == 1 else None
    if not isinstance(identity, str) or identity.removeprefix("sha256:") != pinned:
        raise ValueError("summary v1 installed model digest mismatch")
    thinking = show.get("thinking") if isinstance(show, dict) else None
    values = thinking.get("values") if isinstance(thinking, dict) else None
    caps = show.get("capabilities") if isinstance(show, dict) else None
    info = show.get("model_info") if isinstance(show, dict) else None
    if (not isinstance(values, list) or len(values) != 2 or
        any(type(v) is not bool for v in values) or set(values) != {False, True} or
        not isinstance(caps, list) or any(not isinstance(c,str) for c in caps) or not {"completion", "thinking"} <= set(caps) or
        not isinstance(info, dict) or type(info.get("gemma4.context_length")) is not int or
        info["gemma4.context_length"] != native_limit):
        raise ValueError("summary v1 native boolean capability/context unavailable")
    return dict(schema=VERSION, model=model, model_digest=pinned, native_context=native_limit,
                native_thinking_values=[False, True], requested_think=False)


def admit(executor, profile, source, kind, payload, model, options,
          external_id, source_item_id, producer_storage):
    if source != SOURCE or kind != "chat" or producer_storage is not None:
        raise ValueError("summary v1 requires existing memory source and inline chat")
    if any(not isinstance(v, str) or re.fullmatch(r"[A-Za-z0-9_.:/-]{1,160}", v) is None
           for v in (external_id, source_item_id)):
        raise ValueError("summary v1 requires bounded durable correlation IDs")
    result = normalize_request(payload, profile, model, options)
    result["summary_contract"] = dict(native_contract(executor, profile), admission_budget=budget(result))
    return result


def persisted_contract(executor, profile, payload):
    expected = normalize_request(payload, profile, persisted=True)
    evidence = dict(native_contract(executor, profile), admission_budget=budget(expected))
    if payload != dict(expected, summary_contract=evidence):
        raise ValueError("summary v1 persisted contract or native metadata drift")
    return evidence


def execution_evidence(executor, profile, payload, result):
    model, pinned, _ = MODELS[profile]
    raw = executor.ps(timeout_seconds=5)
    rows = raw.get("models") if isinstance(raw, dict) else None
    if (not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0],dict) or rows[0].get("name") != model or
        not isinstance(rows[0].get("digest"), str) or rows[0]["digest"].removeprefix("sha256:") != pinned or
        type(rows[0].get("context_length")) is not int or
        rows[0]["context_length"] != payload["options"]["num_ctx"]):
        raise ValueError("summary v1 runtime context/model identity mismatch")
    message = result.get("message")
    if (result.get("model") != model or result.get("done") is not True or
        not isinstance(message, dict) or message.get("role") != "assistant" or
        not isinstance(message.get("content"), str) or message.get("thinking") or
        message.get("tool_calls") or result.get("error")):
        raise ValueError("summary v1 native final contract mismatch")
    return dict(schema=VERSION, model=model, model_digest=pinned, think=False,
                request_options=payload["options"], runtime_context_length=rows[0]["context_length"],
                admission_budget=budget(payload), quality_GO=False)
