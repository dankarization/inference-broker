#!/usr/bin/env python3
"""Synthetic broker + dashboard for browser acceptance (no production contact).

Serves the real HTTP routes on a loopback port over a temporary SQLite queue
seeded with queued/running/completed jobs, so a real browser can check the
shipped HTML/CSS/JS: background refresh without a page reload, sort and scroll
preservation, and responsive geometry on narrow screens.

Prints one JSON line with the base URL, the fixture database path and the
seeded source names; use --port 0 to let the kernel pick a free port.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from broker.http import serve  # noqa: E402
from broker.service import Broker, SourcePolicy  # noqa: E402

WEIGHTS = {"alpha-src": 1.0, "beta-src": 4.0, "gamma-src": 10.0}
PAYLOAD = json.dumps({"prompt": "synthetic-fixture-not-a-real-request"})


class FakeOllama:
    def ps(self): return {"models": []}
    def is_ready(self, _): return True
    def unload(self, _): pass
    def run(self, *_): return {"done": True}


class FakeWol:
    def wake(self): pass


def seed(db: sqlite3.Connection, now: float) -> None:
    """Seed terminal and live jobs, including all three duration shapes.

    Completed turn lengths are 15 s (mm:ss), 105 s (mm:ss) and 3610/4010 s
    (h:mm:ss), so a browser check can assert the rendered format rather than
    only the source text.
    """
    rows = []
    index = 0
    for source, weight in WEIGHTS.items():
        queue = max(1, int(weight))
        for _ in range(queue):
            index += 1
            rows.append((f"{source}-queued-{index}", source, "queued", now - 30, None, None, None, 0, 0))
        index += 1
        rows.append((f"{source}-running-{index}", source, "running", now - 20, now - 15, now + 600, None, 1, 0))
        for turn in (15, 105, 3610, 4010):
            index += 1
            rows.append((f"{source}-done-{index}", source, "completed", now - turn - 30,
                         now - turn - 30, None, now - 30, 1, 0))
    with db:
        db.executemany(
            "INSERT INTO jobs(id,profile,kind,source,payload,state,created,started,lease_until,"
            "finished,attempt_count,retry_count) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            [(job_id, "interactive", "generate", source, PAYLOAD, state, created, started,
              lease, finished, attempts, retries)
             for job_id, source, state, created, started, lease, finished, attempts, retries in rows],
        )
        db.executemany(
            "INSERT INTO audit_events(occurred,event_type,job_id,source) VALUES(?,?,?,?)",
            [(finished, "job.completed", job_id, source)
             for job_id, source, state, _created, _started, _lease, finished, _a, _r in rows
             if state == "completed" and finished is not None],
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--ready-file", type=Path, default=None,
                        help="write the bound base URL here once the server is listening")
    parser.add_argument("--assets-dir", type=Path, default=None,
                        help="serve dashboard assets from <dir>/static + <dir>/table_sort.js "
                             "instead of the checkout (used to compare a previous revision)")
    arguments = parser.parse_args()

    workdir = Path(tempfile.mkdtemp(prefix="dashboard-browser-fixture-"))
    policy_path = workdir / "policy.json"
    policy_path.write_text(json.dumps({
        "version": 1,
        "sources": {name: {"enabled": True, "weight": weight} for name, weight in WEIGHTS.items()},
    }), encoding="utf-8")

    now = time.time()
    broker = Broker(str(workdir / "broker.sqlite3"), FakeOllama(), FakeWol(), clock=lambda: now)
    seed(broker.db, now)
    policy = SourcePolicy(policy_path)
    if arguments.assets_dir:
        import broker.http as http_module
        from broker.dashboard_assets import DashboardAssets
        assets_dir = arguments.assets_dir
        http_module.DashboardAssets = lambda *args, **kwargs: DashboardAssets(assets_dir / "static")
    server = serve(broker, host="127.0.0.1", port=arguments.port, policy=policy)
    base = f"http://127.0.0.1:{server.server_port}"
    print(json.dumps({
        "base": base,
        "db": str(workdir / "broker.sqlite3"),
        "policy": str(policy_path),
        "sources": sorted(WEIGHTS),
    }), flush=True)
    if arguments.ready_file:
        arguments.ready_file.write_text(base, encoding="utf-8")
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
