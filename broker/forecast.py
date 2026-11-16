"""Payload-free, bounded evidence for a conditional Nextcloud forecast.

This is not an owner gate, cache of authority, or alternate dispatch path.
Only a successful claim transaction publishes its observation. Readers never
contact the owner or take the dispatch mutex; every ranked row is revalidated.
"""
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


def capture_owner_preview(candidates, gate, signature, now, selected_id):
    """Capture the *filtered minimum owner tier*, not local FIFO heads."""
    preview = {"observed_at": now, "signature": signature, "rows": (),
               "state": "unavailable" if gate.value is None else "observed"}
    counts, rows = {}, []
    for candidate in candidates:
        if candidate["source"] != "nextcloud" or candidate["id"] == selected_id:
            continue
        row = dict(candidate)
        # Keep both local priority classes: a deep new upload must not be
        # hidden behind twenty older historical rows of the same raw lane.
        key = (row["profile"], row["target_model"], row["nextcloud_priority"])
        counts[key] = counts.get(key, 0) + 1
        if counts[key] > OWNER_PREVIEW_PER_LANE:
            continue
        if len(rows) >= MAX_OWNER_PREVIEW_ROWS:
            return dict(preview, state="preview_limit")  # Never rank a truncated lane set.
        rows.append(row)
    if rows:
        tier = gate.value["jobs"][rows[0]["external_id"]]
        preview.update(weight=7 if tier == 1 else 10)
    preview["rows"] = tuple(rows)
    return preview


def revalidate_owner_preview(db, preview, signature, now, include_private,
                             model_sql, priority_sql, public_predicate):
    """Exact indexed seeks; do not load payloads or silently adopt new jobs."""
    if preview is None:
        return [], {"state": "not_observed", "observed_at": None}
    info = {"state": preview["state"], "observed_at": preview["observed_at"],
            "age_seconds": round(max(0.0, now - preview["observed_at"]), 3)}
    if info["state"] != "observed":
        return [], info
    if signature != preview["signature"] or now < preview["observed_at"]:
        return [], dict(info, state="policy_or_clock_changed")
    rows = []
    for row in preview["rows"]:
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
    if not rows:
        return [], dict(info, state="observed_tier_exhausted")
    if info["age_seconds"] > OWNER_OBSERVATION_STALE_SECONDS:
        info["state"] = "stale"
    info["effective_weight"] = preview["weight"]
    return rows, info
