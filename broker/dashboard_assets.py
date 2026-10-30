"""Hot-readable dashboard files; no queue, policy or snapshot rendering logic."""
from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Asset:
    content: bytes
    content_type: str

    @property
    def etag(self) -> str:
        return '"' + hashlib.sha256(self.content).hexdigest() + '"'


class DashboardAssets:
    TYPES = {
        "dashboard.html": "text/html; charset=utf-8",
        "dashboard-unavailable.html": "text/html; charset=utf-8",
        "dashboard.css": "text/css; charset=utf-8",
        "dashboard.js": "text/javascript; charset=utf-8",
        "table_sort.js": "text/javascript; charset=utf-8",
    }

    def __init__(self, directory: Path | None = None):
        self.directory = directory or Path(__file__).resolve().parent / "static"
        self._cache: dict[str, tuple[tuple[int, ...], Asset]] = {}
        self._lock = threading.Lock()

    def read(self, name: str) -> Asset:
        # Explicit allowlist, not a general file server (including traversal).
        if name not in self.TYPES:
            raise KeyError(name)
        path = (self.directory.parent if name == "table_sort.js" else self.directory) / name
        with self._lock:
            stat = path.stat()
            # mtime/size cover edits; inode/ctime also detect atomic replacement
            # and same-size edits whose mtime was preserved by a copy tool.
            signature = (stat.st_mtime_ns, stat.st_size, stat.st_ino, stat.st_ctime_ns)
            cached = self._cache.get(name)
            if cached is not None and cached[0] == signature:
                return cached[1]
            asset = Asset(path.read_bytes(), self.TYPES[name])
            # Store the pre-read signature: a concurrent change invalidates this
            # entry on the next request, never caching new metadata with old bytes.
            self._cache[name] = (signature, asset)
            return asset

    def pages(self) -> tuple[Asset, Asset, str]:
        assets = {name: self.read(name) for name in self.TYPES}
        pages = []
        for name in ("dashboard.html", "dashboard-unavailable.html"):
            content = assets[name].content
            for dependency in ("dashboard.css", "dashboard.js", "table_sort.js"):
                content = content.replace(
                    ("__VERSION_" + dependency + "__").encode(),
                    assets[dependency].etag.strip('"').encode(),
                )
            pages.append(Asset(content, self.TYPES[name]))
        # Frontend changes invalidate /dashboard independently of the JSON data.
        version = hashlib.sha256("".join(a.etag for a in assets.values()).encode()).hexdigest()
        return pages[0], pages[1], version
