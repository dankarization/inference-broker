"""Actual public SQL regression coverage (not simplified EXPLAIN-only SQL)."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from broker.service import Broker

class Ollama:
    def ps(self): return {"models": []}

class ObserverRegression(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "test.db"
        self.b = Broker(self.path, Ollama(), object(), clock=lambda: 1000)
        self.addCleanup(self.b.db.close)
        with self.b.db:
            self.b.db.executemany(
                "INSERT INTO jobs(id,profile,kind,source,payload,state,created,queued_at,finished,producer_attempt_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                ((f"j{i:05}", "interactive", "generate", "history-only",
                  json.dumps({"model":"local", "prompt":"private"*100}), "completed", i, i, i//3,
                  "hidden" if i%4==0 else None) for i in range(1200)))
            self.b.db.executemany(
                "INSERT INTO jobs(id,profile,kind,source,payload,state,created,queued_at) VALUES(?,?,?,?,?,?,?,?)",
                ((f"q{i:05}", "interactive", "generate", "backlog", '{"model":"local"}', "queued", i, i)
                 for i in range(3000)))
            # JSON after the first ten heads must never be evaluated by forecast.
            self.b.db.execute("UPDATE jobs SET payload='invalid JSON' WHERE id='q02999'")

    def test_public_path_history_source_counts_and_bounded_json(self):
        data = self.b.dashboard(include_producer_storage=False)
        self.assertEqual(data["observation"]["state"], "live")
        self.assertEqual(data["overall"]["states"]["completed"], 900)
        self.assertEqual(len(data["history"]), 10)
        self.assertEqual(len(data["forecast"]["next_selections"]), 10)
        self.assertIn("history-only", [s["source"] for s in data["sources"]])
        cursor, ids = None, []
        while True:
            page = self.b.terminal_history(limit=30, cursor=cursor, include_producer_storage=False)
            self.assertFalse(page.get("unavailable"))
            if not page["items"]: break
            ids += [r["id"] for r in page["items"]]
            cursor = tuple(page["next_cursor"])
        self.assertEqual(len(ids), 900)
        self.assertEqual(len(set(ids)), 900)

    def test_concurrent_public_readers_and_writer(self):
        def write():
            connection = sqlite3.connect(self.path, timeout=1)
            try:
                for i in range(30):
                    with connection:
                        connection.execute("UPDATE queue_control SET paused=? WHERE id=1", (i%2,))
            finally: connection.close()
        with ThreadPoolExecutor(max_workers=4) as pool:
            writer = pool.submit(write)
            results = list(pool.map(lambda _: self.b.dashboard(include_producer_storage=False), range(12)))
            writer.result()
        self.assertTrue(all(r["observation"]["state"] == "live" for r in results))
        self.assertEqual(self.b.db.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_priority_public_heads_match_fifo_contract(self):
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET source='nextcloud' WHERE state='queued'")
            self.b.db.execute("UPDATE jobs SET nextcloud_priority='new_upload',payload='{}' WHERE id='q02999'")
        priority = self.b._candidates(frozenset({"nextcloud"}), limit_per_source=1,
                                     include_producer_storage=False, source_priority=True)
        fifo = self.b._candidates(frozenset({"nextcloud"}), limit_per_source=1,
                                 include_producer_storage=False, source_priority=False)
        self.assertEqual(priority[0]["id"], "q02999")
        self.assertEqual(fifo[0]["id"], "q00000")

    def test_validator_does_not_wait_for_dispatch_lock(self):
        import threading
        ready, release = threading.Event(), threading.Event()
        def hold():
            with self.b.lock:
                ready.set()
                release.wait(2)
        thread = threading.Thread(target=hold)
        thread.start()
        try:
            self.assertTrue(ready.wait(1))
            self.assertNotEqual(self.b.dashboard_etag(), self.b.dashboard_etag())
        finally:
            release.set()
            thread.join()

    def test_wal_snapshot_does_not_prevent_writer_commit(self):
        def read(db):
            before = db.execute("SELECT paused FROM queue_control WHERE id=1").fetchone()[0]
            other = sqlite3.connect(self.path, timeout=1)
            try:
                with other: other.execute("UPDATE queue_control SET paused=1 WHERE id=1")
            finally: other.close()
            self.assertEqual(db.execute("SELECT paused FROM queue_control WHERE id=1").fetchone()[0], before)
            return before
        self.assertEqual(self.b._observer_read(read), (0, None))
        self.assertEqual(self.b.db.execute("SELECT paused FROM queue_control WHERE id=1").fetchone()[0], 1)
