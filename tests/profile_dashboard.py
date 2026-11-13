"""Local synthetic benchmark only. No network, backend, live DB or dependencies.
Usage: PYTHONPATH=<source> python tests/profile_dashboard.py DB [--seed]
"""
import json
import logging
import sqlite3
import sys
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import broker.service as service
from broker.service import Broker

logging.basicConfig(level=logging.CRITICAL)
path = Path(sys.argv[1])
seed = "--seed" in sys.argv
class Ollama:
    def ps(self): return {"models": []}
class Wol:
    pass
start = time.perf_counter()
b = Broker(path, Ollama(), Wol(), clock=lambda: 1_000_000)
init_ms = (time.perf_counter()-start)*1000
if seed:
    payload = json.dumps({"model": "test-model", "prompt": "x"*8192})
    for offset in range(0, 100_000, 1000):
        with b.db:
            b.db.executemany(
                "INSERT INTO jobs(id,profile,kind,source,payload,state,created,queued_at,finished,"
                "input_storage_mode,result_storage_mode,producer_attempt_id,nextcloud_priority) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ((f"job-{i:09}", "interactive", "generate", f"source-{i%20:02}", payload,
                  "completed" if i < 80000 else "queued", float(i), float(i),
                  float(i) if i < 80000 else None, "broker_temporary", "broker_temporary",
                  "private" if i%4==0 else None, "historical") for i in range(offset, offset+1000)))
    with b.db:
        b.db.executemany(
            "INSERT INTO audit_events(occurred,event_type,job_id,source,producer_storage,metadata_json) "
            "VALUES(?,?,?,?,?,?)",
            ((1_000_000-i, "job.completed", f"job-{i:09}", f"source-{i%20:02}",
              int(i%4==0), json.dumps({"fixture":"x"*256})) for i in range(80000)))
    b.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
checkpoint_start = time.perf_counter()
b.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
checkpoint_ms = (time.perf_counter()-checkpoint_start)*1000
timings = {}
def wrap(name, fn):
    def call(*args, **kwargs):
        start = time.perf_counter()
        try: return fn(*args, **kwargs)
        finally: timings.setdefault(name, []).append(round((time.perf_counter()-start)*1000,3))
    return call
service.dashboard_snapshot = wrap("snapshot", service.dashboard_snapshot)
b._forecast = wrap("forecast", b._forecast)
b._terminal_history = wrap("history", b._terminal_history)
def read():
    start = time.perf_counter()
    data = b.dashboard(include_producer_storage=False)
    return {"ms": round((time.perf_counter()-start)*1000,3), "state": data["observation"]["state"]}
bounded = [read() for _ in range(3)]
# Unbounded local profiling, separately from the unchanged 750ms acceptance.
service.OBSERVER_READ_DEADLINE_SECONDS = 30
profile = [read() for _ in range(3)]
service.OBSERVER_READ_DEADLINE_SECONDS = .75
writes = []
def writer():
    db = sqlite3.connect(path, timeout=1)
    try:
        for i in range(60):
            start = time.perf_counter()
            with db:
                db.execute("UPDATE jobs SET retry_count=? WHERE id=?", (i%3, f"job-{80000+i:09}"))
            writes.append((time.perf_counter()-start)*1000)
            time.sleep(.003)
    finally: db.close()
with ThreadPoolExecutor(max_workers=5) as pool:
    work = pool.submit(writer)
    concurrent = list(pool.map(lambda _: read(), range(12)))
    work.result()
cursor = None
ids = []
for _ in range(6):
    page = b.terminal_history(limit=30, cursor=cursor, include_producer_storage=False)
    assert not page.get("unavailable"), page
    ids.extend(row["id"] for row in page["items"])
    cursor = tuple(page["next_cursor"])
assert len(set(ids)) == len(ids) == 180
assert all(int(i.split("-")[1])%4 != 0 for i in ids)
indexes = [dict(row) for row in b.db.execute(
    "SELECT name,sum(pgsize) AS bytes FROM dbstat WHERE name LIKE '%observer%' GROUP BY name")]
result = dict(source=service.__file__, init_ms=round(init_ms,3), rows=100000,
    payload_bytes=8192, bounded=bounded, profile=profile, stages_ms=timings,
    concurrent=concurrent, writer_max_ms=round(max(writes),3),
    writer_median_ms=round(sorted(writes)[len(writes)//2],3),
    writer_p95_ms=round(sorted(writes)[int(len(writes)*.95)-1],3),
    checkpoint_ms=round(checkpoint_ms,3), writes=len(writes),
    pagination_unique=len(ids), indexes=indexes, database_bytes=path.stat().st_size)
print(json.dumps(result, indent=2))
b.db.close()
