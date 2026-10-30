"""Payload-free operational snapshot and history display values for the broker."""
from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .storage import PUBLIC_JOB_PREDICATE
from .profiles import PROFILES


LOCAL_TIMEZONE = ZoneInfo("Asia/Tbilisi")


def format_duration(seconds: float | None) -> str:
    """Human duration for wait/turn cells in one format: mm:ss, then h:mm:ss.

    Every duration uses the same clock shape (``00:42``, ``02:30``, ``1:05:00``)
    so a column of them lines up and nothing is misread as minutes. This is the
    server half of a contract mirrored by ``duration()`` in
    ``broker/static/dashboard.js``; both must agree.
    """
    if seconds is None:
        return "—"
    total = max(0, math.trunc(float(seconds)))
    minutes, sec = divmod(total, 60)
    if minutes < 60:
        return f"{minutes:02d}:{sec:02d}"
    hours, minutes = divmod(minutes, 60)
    return f"{hours:d}:{minutes:02d}:{sec:02d}"


def history_display(row: dict[str, Any]) -> dict[str, Any]:
    """Shared display values for dashboard JSON and paged history (no payload reads)."""
    finished = row.get("finished")
    started = row.get("started")
    duration = (
        float(finished) - float(started)
        if row.get("state") == "completed" and started is not None
        and finished is not None and finished >= started else None
    )
    profile = PROFILES.get(row.get("profile"))
    return dict(
        row,
        model=profile.model if profile else None,
        model_turn_seconds=duration,
        model_turn_display=format_duration(duration),
        finished_display=(
            datetime.fromtimestamp(float(finished), tz=timezone.utc)
            .astimezone(LOCAL_TIMEZONE).strftime("%d/%m/%Y, %H:%M:%S")
            if finished is not None else "—"
        ),
    )


def snapshot(
    db: sqlite3.Connection, *, now: float, policy_snapshot: dict[str, Any] | None,
    include_producer_storage: bool = True,
) -> dict[str, Any]:
    """Return operational data only; request payloads, results and errors are excluded."""
    policy_sources = (policy_snapshot or {}).get("sources", {})
    policy_active = policy_snapshot is not None
    public_clause = "" if include_producer_storage else " AND " + PUBLIC_JOB_PREDICATE
    schedules = dict(db.execute("SELECT source,next_allowed FROM source_schedules"))
    # Do not discover sources by scanning all historical jobs.  `jobs` holds
    # payloads and can be multiple GiB, so even an index-only global scan can
    # exceed the observer deadline on a cold cache.  Policy/schedule sources
    # are known directly; active unconfigured sources remain visible through
    # a covering state/source index.
    source_names = set(policy_sources)
    source_names.update(schedules)
    source_names.update(
        row["source"]
        for row in db.execute(
            "SELECT DISTINCT source FROM jobs INDEXED BY jobs_state_source "
            "WHERE state IN ('queued','running','cancel_requested','completed','failed','cancelled')" + public_clause
        )
    )
    # Historical terminal jobs keep their source visible so the History table
    # can show a weight even after a source stopped admitting new work.
    source_weights = {
        name: (policy_sources.get(name) or {}).get("weight") for name in source_names
    }
    sources: list[dict[str, Any]] = []
    overall = {name: 0 for name in ("queued", "running", "lease", "retry", "delayed", "completed", "failed", "dead", "cancelled")}

    for source in sorted(source_names):
        configured = policy_sources.get(source)
        state_counts = {
            row["state"]: row["count"]
            for row in db.execute(
                "SELECT state,count(*) AS count FROM jobs "
                "INDEXED BY jobs_source_state WHERE source=?" + public_clause +
                " GROUP BY state",
                (source,),
            )
        }
        queued = state_counts.get("queued", 0)
        next_allowed = schedules.get(source)
        delayed = queued if next_allowed is not None and next_allowed > now else 0
        # Completion audit records are committed with the terminal job update.
        # Their source/time index avoids reading every completed job payload to
        # derive the two recent windows.
        completed_windows = db.execute(
            "SELECT "
            "coalesce(sum(occurred>=?),0) AS completed_1h,"
            "coalesce(sum(occurred>=?),0) AS completed_24h "
            "FROM audit_events INDEXED BY audit_events_source_time "
            "WHERE source=? AND occurred>=? AND event_type='job.completed'" +
            ("" if include_producer_storage else " AND producer_storage=0"),
            (now - 3_600, now - 86_400, source, now - 86_400),
        ).fetchone()
        retry = db.execute(
            "SELECT count(*) FROM jobs INDEXED BY jobs_source_state_retry "
            "WHERE source=? AND state='queued' AND retry_count>0" + public_clause,
            (source,),
        ).fetchone()[0]
        lease = db.execute(
            "SELECT count(*) FROM jobs INDEXED BY jobs_source_state "
            "WHERE source=? AND state IN ('running','cancel_requested') "
            "AND lease_until IS NOT NULL" + public_clause,
            (source,),
        ).fetchone()[0]
        states = {
            "queued": queued,
            "running": state_counts.get("running", 0) + state_counts.get("cancel_requested", 0),
            "lease": lease,
            "retry": retry,
            "delayed": delayed,
            "completed": state_counts.get("completed", 0),
            "failed": state_counts.get("failed", 0),
            # The broker has no separate `dead` state: terminal jobs are failed.
            "dead": 0,
            "cancelled": state_counts.get("cancelled", 0),
        }
        for name, value in states.items():
            overall[name] += value
        sources.append({
            "source": source,
            "scheduler": {
                "enabled": configured.get("enabled") if isinstance(configured, dict) else (False if policy_active else None),
                "dispatch_paused": (not configured.get("enabled", True)) if isinstance(configured, dict) else (True if policy_active else None),
                "admission_allowed": (not (policy_snapshot or {}).get("maintenance", False) and
                                      configured.get("admission_allowed", True)) if isinstance(configured, dict) else not policy_active,
                "weight": configured.get("weight") if isinstance(configured, dict) else None,
                "next_allowed": next_allowed,
            },
            "states": states,
            "completed_last_hour": completed_windows["completed_1h"],
            "completed_last_24_hours": completed_windows["completed_24h"],
        })

    active_jobs = []
    for row in db.execute(
        "SELECT id,source,state,created,started,lease_until,attempt_count,retry_count "
        "FROM jobs WHERE state IN ('running','cancel_requested')" + public_clause +
        " ORDER BY started,created,id"
    ):
        job = dict(row)
        job["weight"] = source_weights.get(job["source"])
        active_jobs.append(job)
    return {
        "timestamp": now,
        "maintenance": bool((policy_snapshot or {}).get("maintenance", False)),
        "sources": sources,
        "active_jobs": active_jobs,
        "overall": {
            "states": overall,
            "completed_last_hour": sum(item["completed_last_hour"] for item in sources),
            "completed_last_24_hours": sum(item["completed_last_24_hours"] for item in sources),
        },
    }
