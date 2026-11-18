"""Policy snapshot and consistency primitives for the read-only forecast."""
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
