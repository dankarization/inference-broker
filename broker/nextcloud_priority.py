"""Compatibility shim for retired external Nextcloud priority service.

The broker is the sole scheduler. No owner connection or filtering is performed.
"""
from contextlib import contextmanager

SCHEMA = "nextcloud-priority-55852-v2"


class NextcloudPriority:
    """Deprecated inert API: never read files, connect, or alter candidates."""
    def __init__(self, connect=None):
        self.value = None

    @classmethod
    def from_file(cls, path):
        return cls()

    def contract(self):
        return {"schema": SCHEMA, "enabled": False}

    @contextmanager
    def hold(self, db):
        yield

    def candidates(self, rows):
        return rows, None
