"""A request-local idle TTL hint, never a scheduler or an unload operation."""

# Retain a positive grace for bounded post-inference /api/ps evidence (5s).
# Zero would invalidate Qwen64, nightly and summary residency contracts.
VOICE_DEMAND_KEEP_ALIVE_SECONDS = 30


def inference_policy(broker, profile):
    from .voice import _HTTP_WORKERS, voice_contour
    from .asr import PROFILE as ASR_PROFILE

    configured = profile.keep_alive_seconds
    gpu_profiles = tuple(sorted(set(_HTTP_WORKERS) | {voice_contour.PROFILE, ASR_PROFILE}))
    with broker.lock:
        # This is only a residency hint. The existing scheduler still decides
        # eligibility, order, weights and leases; no work is claimed here.
        sources = (broker.source_policy.enabled_sources()
                   if broker.source_policy is not None else None)
        values = list(gpu_profiles)
        query = ("SELECT 1 FROM jobs WHERE state='queued' AND profile IN (" +
                 ",".join("?" for _ in values) + ")")
        if sources is not None:
            if not sources:
                return configured, None
            query += " AND source IN (" + ",".join("?" for _ in sources) + ")"
            values.extend(sorted(sources))
        pending = broker.db.execute(query + " LIMIT 1", values).fetchone() is not None
    effective = min(configured, VOICE_DEMAND_KEEP_ALIVE_SECONDS) if pending else configured
    if effective == configured:
        return effective, None
    return effective, {
        "reason": "queued_gpu_voice", "configured_keep_alive_seconds": configured,
        "effective_keep_alive_seconds": effective,
    }
