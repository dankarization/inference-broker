"""Opt-in Qwen64 contract and scalar-only native residency evidence."""
import math
import time

PROFILE = "nextcloud-eval-qwen-64k-v1"
CONTRACT = dict(
    schema=PROFILE, model="qwen3-vl:30b",
    model_digest="sha256:eda0be1008774ea4e575187712b9a3b1201f3e66e7ecff899cc295db1cf00248",
    quantization="Q4_K_M", think=True,
    options=dict(num_ctx=65536, num_predict=16384, temperature=0),
    keep_alive=300, stream=False, request_timeout_seconds=1800,
)


def _resident(value, *, effective=False):
    if not isinstance(value, dict):
        raise RuntimeError("Qwen64 runtime residency unavailable")
    digest = value.get("digest")
    if isinstance(digest, str) and not digest.startswith("sha256:"):
        digest = "sha256:" + digest
    details = value.get("details")
    quantization = details.get("quantization_level") if isinstance(details, dict) else None
    if (value.get("name") != CONTRACT["model"] or digest != CONTRACT["model_digest"]
            or quantization != CONTRACT["quantization"]):
        raise RuntimeError("Qwen64 resident model identity mismatch")
    result = dict(name=CONTRACT["model"], digest=digest,
                  details=dict(quantization_level=quantization))
    for name in ("size", "size_vram", "context_length"):
        item = value.get(name)
        if type(item) is not int or not 0 < item < 2**50:
            raise RuntimeError("Qwen64 runtime resource metadata unavailable")
        result[name] = item
    if effective and result["context_length"] != CONTRACT["options"]["num_ctx"]:
        raise RuntimeError("Qwen64 effective context mismatch")
    return result


def snapshot(executor, *, effective=False):
    # Read-only native metadata through the broker adapter; never load/run here.
    data = executor.ps(timeout_seconds=5)
    models = data.get("models") if isinstance(data, dict) else None
    if not isinstance(models, list) or len(models) != 1:
        raise RuntimeError("Qwen64 requires exclusive known model residency")
    return _resident(models[0], effective=effective)


def evidence(before, after, *, sampled_at=None):
    return dict(schema="nextcloud-qwen64-execution-v1", model=CONTRACT["model"],
                model_digest=CONTRACT["model_digest"], think=True,
                options=dict(CONTRACT["options"]), before=_resident(before),
                after=_resident(after, effective=True),
                sampled_at=time.time() if sampled_at is None else sampled_at)


def project(value):
    """Reject malformed metadata rather than tunnelling nested executor fields."""
    if (not isinstance(value, dict) or value.get("schema") != "nextcloud-qwen64-execution-v1"
            or value.get("model") != CONTRACT["model"]
            or value.get("model_digest") != CONTRACT["model_digest"]
            or value.get("think") is not True or value.get("options") != CONTRACT["options"]
            or type(value.get("sampled_at")) not in (int, float)
            or not math.isfinite(value["sampled_at"]) or value["sampled_at"] <= 0):
        return None
    try:
        return evidence(value.get("before"), value.get("after"), sampled_at=value["sampled_at"])
    except RuntimeError:
        return None
