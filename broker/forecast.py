"""Payload-free, bounded evidence for a conditional Nextcloud forecast.

This is not an owner gate, cache of authority, or alternate dispatch path.
Only a successful claim transaction publishes its observation. Readers never
contact the owner or take the dispatch mutex; every ranked row is revalidated.
"""
import json
import sqlite3


class ForecastSnapshotChanged(sqlite3.OperationalError):
    """SQLite data and committed scheduler evidence belong to different cuts."""


class ForecastPolicy:
    """One consistent policy read; a reload cannot mix two forecast scenarios."""
    def __init__(self, snapshot):
        self.value = snapshot

    def snapshot(self):
        return self.value

    def enabled_sources(self):
        return frozenset(s for s, row in self.value["sources"].items() if row["enabled"])

    def weight(self, source):
        return self.value["sources"].get(source, {}).get("weight")


MAX_OWNER_PREVIEW_ROWS = 512
OWNER_PREVIEW_PER_LANE = 20
OWNER_OBSERVATION_STALE_SECONDS = 60.0
MAX_PUBLIC_VISIBILITY_PROBES = 2048
VISIBILITY_EVENT = "forecast.visibility_revision"


def visibility_revision(db):
    """Transactional generation for queued Nextcloud membership/visibility."""
    row = db.execute("SELECT metadata_json FROM audit_events "
                     "WHERE sequence=0 AND event_type=? AND producer_storage=1", (VISIBILITY_EVENT,)).fetchone()
    if row is None:
        return None
    value = json.loads(row[0])
    revision = value.get("revision") if isinstance(value, dict) else None
    return revision if type(revision) is int and revision >= 0 else None


def capture_owner_preview(candidates, gate, signature, now, selected_id, *,
                          db=None, public_predicate=None, visibility_version=None):
    """Capture the *filtered minimum owner tier*, not local FIFO heads."""
    preview = {"observed_at": now, "signature": signature, "rows": (),
               "state": "unavailable" if gate.value is None else "observed"}
    counts, public_counts, rows, public_rows = {}, {}, [], []
    visibility_probes, public_complete = 0, True
    for candidate in candidates:
        if candidate["source"] != "nextcloud" or candidate["id"] == selected_id:
            continue
        row = dict(candidate)
        # Keep both local priority classes: a deep new upload must not be
        # hidden behind twenty older historical rows of the same raw lane.
        key = (row["profile"], row["target_model"], row["nextcloud_priority"])
        counts[key] = counts.get(key, 0) + 1
        if counts[key] <= OWNER_PREVIEW_PER_LANE:
            if len(rows) >= MAX_OWNER_PREVIEW_ROWS:
                return dict(preview, state="preview_limit")  # Never rank a truncated lane set.
            rows.append(row)
        # Exact primary-key probes avoid widening the dispatcher's covering
        # scan to producer-storage fields for the entire eligible queue.
        if db is not None and public_predicate is not None and public_complete:
            if public_counts.get(key, 0) < OWNER_PREVIEW_PER_LANE:
                if visibility_probes >= MAX_PUBLIC_VISIBILITY_PROBES:
                    public_complete = False
                    public_rows.clear()  # A partial competitor set is never ranked.
                    continue
                visibility_probes += 1
                if not is_public_preview_job(db, row["id"], public_predicate):
                    continue
                if len(public_rows) >= MAX_OWNER_PREVIEW_ROWS:
                    return dict(preview, state="preview_limit")
                public_rows.append(row)
                public_counts[key] = public_counts.get(key, 0) + 1
    if rows:
        tier = gate.value["jobs"][rows[0]["external_id"]]
        preview.update(weight=7 if tier == 1 else 10)
    preview["rows"] = tuple(rows)
    preview["visibility_version"] = visibility_version
    if db is not None and public_predicate is not None and public_complete:
        preview["public_rows"] = tuple(public_rows)
    elif db is not None and public_predicate is not None:
        preview["public_state"] = "public_preview_limit"
    return preview


def is_public_preview_job(db, job_id, public_predicate):
    return db.execute("SELECT 1 FROM jobs WHERE id=? AND " + public_predicate,
                      (job_id,)).fetchone() is not None


def revalidate_owner_preview(db, preview, signature, now, include_private,
                             model_sql, priority_sql, public_predicate, schema_version=None):
    """Exact indexed seeks; do not load payloads or silently adopt new jobs."""
    if preview is None:
        return [], {"state": "not_observed", "observed_at": None}
    info = {"state": preview["state"], "observed_at": preview["observed_at"],
            "age_seconds": round(max(0.0, now - preview["observed_at"]), 3)}
    if info["state"] != "observed":
        return [], info
    if signature != preview["signature"] or now < preview["observed_at"]:
        return [], dict(info, state="policy_or_clock_changed")
    if not include_private and preview.get("public_state"):
        return [], dict(info, state=preview["public_state"])
    if include_private and "visibility_version" in preview:
        if preview["visibility_version"] is None:
            return [], dict(info, state="queue_changed")
        try:
            if (schema_version is None or
                    db.execute("PRAGMA schema_version").fetchone()[0] != schema_version or
                    visibility_revision(db) != preview["visibility_version"]):
                return [], dict(info, state="queue_changed")
        except (sqlite3.Error, ValueError, TypeError):
            return [], dict(info, state="queue_changed")
    if not include_private and "public_rows" in preview:
        if preview.get("visibility_version") is None:
            return [], dict(info, state="public_preview_unavailable")
        try:
            if (schema_version is None or
                    db.execute("PRAGMA schema_version").fetchone()[0] != schema_version):
                return [], dict(info, state="public_preview_unavailable")
            if visibility_revision(db) != preview["visibility_version"]:
                return [], dict(info, state="public_preview_changed")
        except (sqlite3.Error, ValueError, TypeError):
            return [], dict(info, state="public_preview_unavailable")
    rows = []
    # Older observations have no independently captured public preview.
    # Their visible prefix is only partial; never assert tier exhaustion.
    public_complete = "public_rows" in preview
    captured = preview["rows"] if include_private else preview.get("public_rows", preview["rows"])
    for row in captured:
        priority = 0 if row["nextcloud_priority"] == "new_upload" else 1
        values = (row["profile"], row["target_model"], priority, row["queued_at"], row["id"],
                  row["attempt_count"], row["external_id"], row["source_item_id"],
                  row["nextcloud_priority"], row["created"])
        where = (" WHERE state='queued' AND source='nextcloud' AND profile=?"
                 f" AND {model_sql} IS ? AND {priority_sql}=? AND queued_at IS ? AND id=?"
                 " AND attempt_count=? AND external_id IS ? AND source_item_id IS ?"
                 " AND nextcloud_priority=? AND created=?")
        # The complete index key bounds each probe, even in a deep model lane.
        if db.execute("SELECT id FROM jobs INDEXED BY jobs_model_heads_all" + where,
                      values).fetchone() is None:
            return [], dict(info, state="queue_changed")
        if not include_private and db.execute(
                "SELECT id FROM jobs INDEXED BY jobs_model_heads_public" + where +
                " AND " + public_predicate, values).fetchone() is None:
            continue
        rows.append(dict(row))
    if not include_private and not public_complete:
        return [], dict(info, state="public_preview_unavailable")
    if not rows:
        if not include_private and captured:
            return [], dict(info, state="public_preview_changed")
        return [], dict(info, state="observed_tier_exhausted")
    if info["age_seconds"] > OWNER_OBSERVATION_STALE_SECONDS:
        info["state"] = "stale"
    info["effective_weight"] = preview["weight"]
    return rows, info
