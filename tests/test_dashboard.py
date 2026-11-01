import json
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest.mock import patch

from broker.dashboard import format_duration, history_display, snapshot
from broker.dashboard_assets import DashboardAssets
from broker.profiles import PROFILES
from broker.http import serve
from broker.service import Broker, SourcePolicy


class FakeOllama:
    def ps(self): return {"models": []}
    def is_ready(self, _): return True
    def unload(self, _): pass
    def run(self, *_): return {"done": True}


class FakeWol:
    def wake(self): pass


class DashboardTests(unittest.TestCase):
    def test_history_fields_are_payload_free_and_exclude_queue_wait(self):
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol())
        self.addCleanup(broker.db.close)
        finished = datetime(2026, 10, 28, 13, 52, 17, tzinfo=timezone.utc).timestamp()
        with broker.db:
            for job_id, state, started in (
                ('done', 'completed', finished - 12.25),
                ('cancelled', 'cancelled', finished - 20),
                ('unstarted', 'completed', None),
                ('failed', 'failed', finished - 30),
            ):
                broker.db.execute(
                    'INSERT INTO jobs(id,profile,kind,source,payload,state,created,started,finished) '
                    'VALUES(?,?,?,?,?,?,?,?,?)',
                    (job_id, 'interactive', 'generate', 'source', '{"secret":"private"}',
                     state, finished - 3600, started, finished),
                )
        first = broker.terminal_history(limit=2)
        second = broker.terminal_history(limit=2, cursor=tuple(first['next_cursor']))
        rows = first['items'] + second['items']
        self.assertEqual(len(rows), 4)
        for row in rows:
            self.assertEqual(row['model'], PROFILES['interactive'].model)
            self.assertEqual(row['finished_display'], '28/10/2026, 17:52:17')
            self.assertEqual(row['model_turn_seconds'], 12.25 if row['id'] == 'done' else None)
        self.assertNotIn('private', json.dumps(rows))
        self.assertEqual(history_display({'finished': 0})['finished_display'], '01/01/1970, 04:00:00')
        self.assertEqual(history_display({'finished': None})['finished_display'], '—')

    def test_frontend_rendering_and_controls_with_real_scripts(self):
        if shutil.which('node') is None:
            self.skipTest('node required for real frontend execution')
        root = Path(__file__).resolve().parents[1]
        checked = subprocess.run(['node', '--check', str(root / 'broker/static/dashboard.js')],
                                 capture_output=True, text=True)
        self.assertEqual(checked.returncode, 0, checked.stderr)
        result = subprocess.run(['node', str(root / 'tests/dashboard_dom.cjs')],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('dashboard frontend contract PASS', result.stdout)

    def test_policy_select_state_machine_recovers_and_serializes(self):
        # Executable model of the rendered desired/saved/saving state machine.
        saved, desired, saving, sent = "A", "A", False, []
        def change(value):
            nonlocal desired, saving
            desired = value
            if not saving:
                saving = True
                sent.append(desired)
        def settle(ok, persisted=None):
            nonlocal saved, saving
            value = sent[-1]
            if ok:
                saved = persisted or value
                if desired != saved: sent.append(desired); return
                saving = False; return
            elif desired != value:
                sent.append(desired); return
            saving = False
        change("B"); settle(False)  # latest B fails and releases the flag
        self.assertFalse(saving); change("C"); self.assertEqual(sent[-1], "C"); settle(True)
        self.assertEqual((saved, saving), ("C", False))
        change("A"); change("B")  # B is queued; only A is in flight
        self.assertEqual(sent[-1], "A"); settle(False); self.assertEqual(sent[-1], "B")
        settle(True); self.assertEqual((saved, saving), ("B", False))
    def test_history_keyset_is_payload_free_ordered_and_exhaustible(self):
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol(), clock=lambda: 100)
        payload = json.dumps({"secret": "x" * 65_536})
        with broker.db:
            for index in range(40):
                broker.db.execute(
                    "INSERT INTO jobs(id,profile,kind,source,payload,state,created,finished,attempt_count) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (f"terminal-{index:02d}", "interactive", "generate", "history", payload,
                     "completed", float(index), float(index), index % 3 + 1),
                )
        first_plan = " ".join(row[3] for row in broker.db.execute(
            "EXPLAIN QUERY PLAN SELECT id,source,profile,state,created,started,finished,attempt_count,retry_count "
            "FROM jobs INDEXED BY jobs_terminal_history_v3 WHERE state IN ('completed','failed','cancelled') "
            "AND finished IS NOT NULL ORDER BY finished DESC,id DESC LIMIT 30"
        ))
        cursor_plan = " ".join(row[3] for row in broker.db.execute(
            "EXPLAIN QUERY PLAN SELECT id,source,profile,state,created,started,finished,attempt_count,retry_count "
            "FROM jobs INDEXED BY jobs_terminal_history_v3 WHERE state IN ('completed','failed','cancelled') "
            "AND finished IS NOT NULL AND (finished,id) < (?,?) "
            "ORDER BY finished DESC,id DESC LIMIT 30",
            (30.0, "terminal-30"),
        ))
        for plan in (first_plan, cursor_plan):
            self.assertIn("COVERING INDEX jobs_terminal_history_v3", plan)
            self.assertNotIn("USE TEMP B-TREE FOR ORDER BY", plan)
        first = broker.terminal_history(limit=10)
        second = broker.terminal_history(limit=30, cursor=tuple(first["next_cursor"]))
        self.assertEqual(len(first["items"]), 10)
        self.assertEqual(len(second["items"]), 30)
        ids = [item["id"] for item in first["items"] + second["items"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(ids, [f"terminal-{index:02d}" for index in range(39, -1, -1)])
        self.assertNotIn("payload", json.dumps(first))

    def test_dashboard_html_contains_static_loading_contract(self):
        live, unavailable, _ = DashboardAssets().pages()
        html = live.content.decode()
        self.assertIn('id="history-body"', html)
        self.assertLess(html.index('<h2>Forecast</h2>'), html.index('<h2>History</h2>'))
        self.assertIn('/dashboard/assets/dashboard.js?v=', html)
        self.assertIn('/dashboard/assets/dashboard.css?v=', html)
        self.assertNotIn('__VERSION_', html)
        # A meta refresh reloads the whole page: it would drop the chosen sort,
        # the scroll position and the form state on every tick. Data is patched
        # in the background instead.
        for page in (html, unavailable.content.decode()):
            self.assertNotIn('http-equiv="refresh"', page)
            self.assertNotIn('http-equiv=refresh', page)
            self.assertNotIn('http-equiv', page.lower())
            self.assertIn('every 30 seconds in the background', page)
        self.assertIn('refreshes every 30 seconds', html)
        self.assertIn('<footer>', html)
        self.assertLess(html.index('</main>'), html.index('Data refreshes every 30 seconds'))
        self.assertNotIn('Finished: Asia/Tbilisi', html)
        self.assertEqual(html.count('id="queue-pause"'), 1)
        self.assertLess(html.index('<h1>'), html.index('id="queue-pause"'))
        self.assertLess(html.index('id="queue-pause"'), html.index('<main'))
        # Wide tables scroll in their own box instead of stretching the page.
        self.assertEqual(html.count('<div class="table-scroll">'), 4)
        for name in ('sources-table', 'active-table', 'forecast-table', 'history-table'):
            self.assertIn(f'<div class="table-scroll"><table id="{name}">', html)
        self.assertIn('mm:ss', (Path(__file__).resolve().parents[1] / 'broker/static/dashboard.js').read_text(encoding='utf-8'))
        self.assertIn('Dashboard data unavailable', unavailable.content.decode())
        self.assertNotIn('Completed:', unavailable.content.decode())

    def test_no_meta_refresh_in_shipped_dashboard_html(self):
        """The auto-reload contract: no meta refresh in either shipped shell."""
        dashboard = Path(__file__).resolve().parents[1] / 'broker/static/dashboard.html'
        raw = dashboard.read_text(encoding='utf-8')
        self.assertNotIn('http-equiv="refresh"', raw)
        self.assertNotIn('http-equiv=refresh', raw)
        self.assertNotIn('http-equiv', raw.lower())
        live, unavailable, _ = DashboardAssets().pages()
        for page in (live.content.decode(), unavailable.content.decode()):
            self.assertNotIn('http-equiv', page.lower())

    def test_dashboard_css_keeps_tables_compact_and_responsive(self):
        """Narrow screens must not be stretched by numeric columns."""
        css = (Path(__file__).resolve().parents[1] / 'broker/static/dashboard.css').read_text(encoding='utf-8')
        self.assertIn('.table-scroll', css)
        self.assertIn('overflow-x: auto', css)
        # Cells stay on one line (short rows) and the table sizes to its
        # content instead of stretching columns into empty space.
        self.assertIn('white-space: nowrap', css)
        self.assertIn('width: auto', css)
        self.assertNotIn('th, td { width: 1%', css)
        # Narrow-screen rules exist and never touch the wide default layout.
        self.assertIn('@media (max-width: 900px)', css)
        self.assertIn('@media (max-width: 640px)', css)
        self.assertIn('.error:empty', css)
        narrow = css[css.index('@media (max-width: 640px)'):]
        self.assertIn('.topbar-in { flex-wrap: wrap;', narrow)
        self.assertIn('.stamp { flex: 1 0 100%;', narrow)
        self.assertIn('#active-table thead th', css)
        self.assertIn('#forecast-table thead th', css)
        self.assertIn('#history-table thead th', css)
        self.assertIn('@media (max-width: 480px)', css)
        self.assertLess(css.index('.table-scroll'), css.index('@media'))
        # Column hiding is scoped to the narrow queries only (the [hidden]
        # attribute utility is the one pre-existing display rule).
        default_rules = css[:css.index('@media')].replace('[hidden] { display: none !important; }', '')
        self.assertNotIn('display: none', default_rules)
        for query in css[css.index('@media'):].split('@media')[1:]:
            self.assertIn('display: none', query)
        # The 30-second background refresh lives in the script, not the document.
        script = (Path(__file__).resolve().parents[1] / 'broker/static/dashboard.js').read_text(encoding='utf-8')
        self.assertIn('REFRESH_INTERVAL_MS = 30000', script)
        self.assertIn("'visibilitychange'", script)
        self.assertIn('setInterval', script)
        self.assertIn('clearInterval', script)
        # A reload survives only as the unavailable-shell recovery branch.
        # Comments are stripped so the count reflects executable code only.
        code = '\n'.join(line for line in script.splitlines() if not line.strip().startswith('//'))
        self.assertEqual(code.count('location.reload'), 1)
        self.assertIn('pauseRefreshTimer(); window.location.reload();', code)
        self.assertIn('captureView', script)
        self.assertIn('restoreView', script)
        self.assertIn('refreshRows', script)

    def test_duration_format_is_mm_ss_and_matches_the_javascript_helper(self):
        """One duration format for both halves of the dashboard contract.

        The same fixture file drives ``duration()`` in tests/dashboard_dom.cjs,
        so the Python and JavaScript helpers cannot drift apart.
        """
        cases = json.loads((Path(__file__).resolve().parent / 'duration_format_cases.json')
                           .read_text(encoding='utf-8'))['cases']
        self.assertGreaterEqual(len(cases), 10)
        for seconds, expected in cases:
            with self.subTest(seconds=seconds):
                self.assertEqual(format_duration(seconds), expected)
        # The former "42.000s" shape is gone: every duration is a clock value.
        self.assertEqual(format_duration(42), '00:42')
        self.assertEqual(format_duration(150.2), '02:30')
        self.assertEqual(format_duration(3725.9), '1:02:05')
        self.assertNotIn('s', format_duration(0.4))
        row = history_display({'state': 'completed', 'started': 100.0, 'finished': 142.0})
        self.assertEqual(row['model_turn_seconds'], 42.0)
        self.assertEqual(row['model_turn_display'], '00:42')
        self.assertEqual(history_display({'finished': 0})['finished_display'], '01/01/1970, 04:00:00')

    def test_payload_history_uses_the_bounded_observer_index_path(self):
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol(), clock=lambda: 100_000)
        payload = json.dumps({"prompt": "x" * 16_384})
        with broker.db:
            broker.db.executemany(
                "INSERT INTO jobs("
                "id,profile,kind,source,payload,state,created,finished,queued_at"
                ") VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (
                        f"history-{index}", "interactive", "generate", "archive",
                        payload, "completed", float(index), float(index), float(index),
                    )
                    for index in range(2_000)
                ],
            )
            broker.db.execute(
                "INSERT INTO jobs("
                "id,profile,kind,source,payload,state,created,finished,queued_at"
                ") VALUES (?,?,?,?,?,?,?,?,?)",
                ("history", "interactive", "generate", "history", payload,
                 "completed", 1.0, 99_999.0, 1.0),
            )
            broker.db.execute(
                "INSERT INTO audit_events(occurred,event_type,job_id,source) VALUES(?,?,?,?)",
                (99_999.0, "job.completed", "history", "history"),
            )

        progress_calls = [0]
        queries: list[str] = []

        def stop_full_table_scan():
            progress_calls[0] += 1
            return progress_calls[0] >= 70_000

        broker.db.set_progress_handler(stop_full_table_scan, 1)
        broker.db.set_trace_callback(queries.append)
        try:
            data = snapshot(
                broker.db, now=100_000,
                policy_snapshot={"sources": {"history": {"enabled": True, "weight": 1.0}}},
                # Keep this probe focused on the covering completion windows:
                # exclude the 2_000 archive rows so the state counts describe the
                # same single source the policy knows about.
                include_producer_storage=False,
            )
        finally:
            broker.db.set_progress_handler(None, 0)
            broker.db.set_trace_callback(None)

        history_source = next(
            item for item in data["sources"] if item["source"] == "history"
        )
        self.assertEqual(history_source["states"]["completed"], 1)
        self.assertEqual(history_source["completed_last_hour"], 1)
        self.assertLess(progress_calls[0], 70_000)
        self.assertTrue(any("FROM audit_events" in query for query in queries))
        self.assertFalse(any("finished>=" in query for query in queries))

    def test_dashboard_queued_payload_reads_use_covering_indexes(self):
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol(), clock=lambda: 100_000)
        payload = json.dumps({"prompt": "x" * 65_536})
        with broker.db:
            broker.db.executemany(
                "INSERT INTO jobs(id,profile,kind,source,payload,state,created,queued_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                [
                    (f"queued-{index}", "interactive", "generate", "payload-source",
                     payload, "queued", float(index), float(index))
                    for index in range(500)
                ],
            )
        state_plan = " ".join(
            row[3] for row in broker.db.execute(
                "EXPLAIN QUERY PLAN SELECT DISTINCT source FROM jobs "
                "INDEXED BY jobs_state_source WHERE state IN ('queued','running','cancel_requested')"
            )
        )
        retry_plan = " ".join(
            row[3] for row in broker.db.execute(
                "EXPLAIN QUERY PLAN SELECT count(*) FROM jobs "
                "INDEXED BY jobs_source_state_retry WHERE source=? AND state='queued' AND retry_count>0",
                ("payload-source",),
            )
        )
        candidate_plan = " ".join(
            row[3] for row in broker.db.execute(
                "EXPLAIN QUERY PLAN SELECT id,profile,source,created,queued_at,attempt_count "
                "FROM jobs INDEXED BY jobs_queued_candidates WHERE state='queued' "
                "ORDER BY queued_at,id"
            )
        )
        self.assertIn("COVERING INDEX jobs_state_source", state_plan)
        self.assertIn("COVERING INDEX jobs_source_state_retry", retry_plan)
        self.assertIn("COVERING INDEX jobs_queued_candidates", candidate_plan)
        data = snapshot(broker.db, now=100_000, policy_snapshot={"sources": {}})
        self.assertEqual(data["overall"]["states"]["queued"], 500)
        for _ in range(3):
            observed = broker.dashboard()
            self.assertEqual(observed["observation"]["state"], "live")
            self.assertEqual(observed["overall"]["states"]["queued"], 500)
            forecast = broker.forecast()
            self.assertNotIn("unavailable", forecast)

    def test_dashboard_endpoints_report_unavailable_observer_instead_of_empty_queue(self):
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol())
        broker.submit("interactive", "generate", {"prompt": "must not become zero"})
        server = serve(broker, port=0)
        self.addCleanup(server.server_close)

        def get(path):
            worker = threading.Thread(target=server.handle_request)
            worker.start()
            try:
                with urlopen(f"http://127.0.0.1:{server.server_port}{path}") as response:
                    status, body = response.status, response.read()
            except HTTPError as error:
                status, body = error.code, error.read()
            worker.join(timeout=1)
            return status, body

        with patch(
            "broker.service.sqlite3.connect",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            api_status, api_body = get("/v1/dashboard")
            html_status, html_body = get("/dashboard")

        data = json.loads(api_body)
        self.assertEqual(api_status, 503)
        self.assertEqual(data["observation"]["state"], "unavailable")
        self.assertEqual(data["observation"]["sqlite_error"], "SQLITE_BUSY")
        self.assertNotIn("overall", data)
        self.assertNotIn("sources", data)
        self.assertEqual(html_status, 503)
        self.assertIn(b"Dashboard data unavailable", html_body)
        self.assertNotIn(b"Completed:", html_body)

        empty_db = tempfile.NamedTemporaryFile()
        self.addCleanup(empty_db.close)
        empty = Broker(empty_db.name, FakeOllama(), FakeWol()).dashboard()
        self.assertEqual(empty["observation"]["state"], "live")
        self.assertEqual(empty["overall"]["states"]["queued"], 0)

    def test_unchanged_dashboard_etag_skips_regeneration_and_transfer(self):
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        clock = [120.0]
        broker = Broker(db.name, FakeOllama(), FakeWol(), clock=lambda: clock[0])
        self.addCleanup(broker.db.close)
        server = serve(broker, port=0)
        self.addCleanup(server.server_close)

        def get(headers=None):
            worker = threading.Thread(target=server.handle_request)
            worker.start()
            request = Request(
                f"http://127.0.0.1:{server.server_port}/dashboard",
                headers=headers or {},
            )
            try:
                with urlopen(request) as response:
                    result = response.status, response.read(), response.headers
            except HTTPError as error:
                result = error.code, error.read(), error.headers
            worker.join(timeout=1)
            return result

        with patch.object(broker, "dashboard", wraps=broker.dashboard) as regenerate:
            status, body, headers = get()
            self.assertEqual(status, 200)
            self.assertTrue(body)
            etag = headers["ETag"]
            self.assertEqual(headers["Cache-Control"], "private, no-cache")
            self.assertEqual(regenerate.call_count, 1)

            status, body, unchanged_headers = get({"If-None-Match": etag})
            self.assertEqual((status, body), (304, b""))
            self.assertEqual(unchanged_headers["ETag"], etag)
            self.assertEqual(regenerate.call_count, 1)

            clock[0] = 149.0
            status, body, same_epoch_headers = get({"If-None-Match": etag})
            self.assertEqual((status, body), (304, b""))
            self.assertEqual(same_epoch_headers["ETag"], etag)
            self.assertEqual(regenerate.call_count, 1)

            clock[0] = 150.0
            status, body, aged_headers = get({"If-None-Match": etag})
            self.assertEqual(status, 200)
            self.assertTrue(body)
            self.assertNotEqual(aged_headers["ETag"], etag)
            self.assertEqual(regenerate.call_count, 2)

            etag = aged_headers["ETag"]
            broker.submit("interactive", "generate", {"prompt": "changed"})
            status, body, changed_headers = get({"If-None-Match": etag})
            self.assertEqual(status, 200)
            self.assertTrue(body)
            self.assertNotEqual(changed_headers["ETag"], etag)
            self.assertEqual(regenerate.call_count, 3)

    def test_dashboard_is_payload_free_and_uses_finished_completion_windows(self):
        clock = lambda: 100_000
        db = tempfile.NamedTemporaryFile()
        self.addCleanup(db.close)
        broker = Broker(db.name, FakeOllama(), FakeWol(), clock=clock)
        policy_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump({"version": 1, "sources": {
            "dashboard-source": {"enabled": True, "weight": 3.0},
            "disabled-source": {"enabled": False, "weight": 8.0},
        }}, policy_file); policy_file.close()
        self.addCleanup(lambda: __import__("os").unlink(policy_file.name))
        policy = SourcePolicy(policy_file.name)
        completed_recent = broker.submit("batch-video", "generate", {"prompt": "do-not-expose"}, source="dashboard-source")["id"]
        completed_old = broker.submit("batch-video", "generate", {"prompt": "also-private"}, source="dashboard-source")["id"]
        running = broker.submit("batch-video", "generate", {}, source="dashboard-source")["id"]
        # An old persisted job remains visible even if its source is not registered.
        broker.submit("batch-video", "generate", {}, source="omitted-source")
        with broker.db:
            broker.db.execute("UPDATE jobs SET state='completed',finished=? WHERE id=?", (99_000, completed_recent))
            broker.db.execute("UPDATE jobs SET state='completed',finished=? WHERE id=?", (10_000, completed_old))
            broker.db.execute("UPDATE jobs SET state='running',started=?,lease_until=? WHERE id=?", (99_900, 100_300, running))
            broker.db.execute(
                "INSERT INTO audit_events(occurred,event_type,job_id,source) VALUES(?,?,?,?)",
                (99_000, "job.completed", completed_recent, "dashboard-source"),
            )
            broker.db.execute(
                "INSERT INTO audit_events(occurred,event_type,job_id,source) VALUES(?,?,?,?)",
                (10_000, "job.completed", completed_old, "dashboard-source"),
            )
        server = serve(broker, port=0, policy=policy)
        self.addCleanup(server.server_close)

        def get(path):
            worker = threading.Thread(target=server.handle_request); worker.start()
            with urlopen(f"http://127.0.0.1:{server.server_port}{path}") as response:
                body, content_type = response.read(), response.headers["Content-Type"]
            worker.join(timeout=1)
            return body, content_type

        def post(path, payload):
            request = Request(
                f"http://127.0.0.1:{server.server_port}{path}",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"}, method="POST",
            )
            worker = threading.Thread(target=server.handle_request); worker.start()
            try:
                with urlopen(request) as response:
                    status, body = response.status, response.read()
            except HTTPError as error:
                status, body = error.code, error.read()
            worker.join(timeout=1)
            return status, json.loads(body)

        body, _ = get("/v1/dashboard")
        data = json.loads(body)
        self.assertEqual(set(data), {"timestamp", "maintenance", "queue_paused", "sources", "active_jobs",
                                     "overall", "forecast", "history", "observation"})
        self.assertEqual(data, broker.dashboard(policy, include_producer_storage=False))
        source = next(item for item in data["sources"] if item["source"] == "dashboard-source")
        self.assertEqual(source["scheduler"], {
            "enabled": True, "dispatch_paused": False,
            "admission_allowed": True, "weight": 3.0, "next_allowed": None,
        })
        self.assertEqual(source["states"]["lease"], 1)
        self.assertEqual(source["states"]["completed"], 2)
        self.assertEqual(source["completed_last_hour"], 1)
        self.assertEqual(source["completed_last_24_hours"], 1)
        self.assertNotIn("payload", json.dumps(data))
        data = broker.dashboard(policy)
        omitted = next(item for item in data["sources"] if item["source"] == "omitted-source")
        self.assertEqual(omitted["scheduler"], {
            "enabled": False, "dispatch_paused": True,
            "admission_allowed": False, "weight": None, "next_allowed": None,
        })
        html, content_type = get("/dashboard")
        self.assertIn("text/html", content_type)
        self.assertIn(b"Completed 1h", html)
        self.assertIn(b"Weight", html)
        # HTML is a data-free shell; the DOM harness exercises real controls.
        self.assertNotIn(b'dashboard-source', html)
        self.assertNotIn(b'active-job', html)
        self.assertNotIn(b"Dead", html)
        self.assertNotIn(b"do-not-expose", html)
        status, result = post("/v1/sources/dashboard-source/weight", {"weight": 7})
        self.assertEqual((status, result), (200, {"source": "dashboard-source", "weight": 7}))
        self.assertEqual(SourcePolicy(policy_file.name).weight("dashboard-source"), 7.0)
        with open(policy_file.name) as handle:
            self.assertEqual(json.load(handle)["sources"]["dashboard-source"]["weight"], 7)
        for invalid_weight in (0, 11, 3.5, "3", True):
            status, result = post("/v1/sources/dashboard-source/weight", {"weight": invalid_weight})
            self.assertEqual(status, 400)
            self.assertIn("integer from 1 through 10", result["error"])
        self.assertEqual(SourcePolicy(policy_file.name).weight("dashboard-source"), 7.0)


if __name__ == "__main__":
    unittest.main()
