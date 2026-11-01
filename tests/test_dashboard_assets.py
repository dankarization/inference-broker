"""HTTP/file-cache regressions: one server instance, edits visible on reload."""
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from broker.dashboard_assets import DashboardAssets
from broker.http import serve
from broker.service import Broker


class FakeOllama:
    def ps(self): return {"models": []}
    def is_ready(self, _): return True
    def unload(self, _): pass
    def run(self, *_): return {"done": True}


class FakeWol:
    def wake(self): pass


class DashboardAssetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        source = Path(__file__).resolve().parents[1] / "broker"
        shutil.copytree(source / "static", self.root / "static")
        shutil.copyfile(source / "table_sort.js", self.root / "table_sort.js")
        self.assets = DashboardAssets(self.root / "static")
        self.broker = Broker(str(self.root / "test.db"), FakeOllama(), FakeWol(), clock=lambda: 120)
        self.addCleanup(self.broker.db.close)
        with patch("broker.http.DashboardAssets", return_value=self.assets):
            self.server = serve(self.broker, port=0)
        self.addCleanup(self.server.server_close)

    def get(self, path, etag=None):
        worker = threading.Thread(target=self.server.handle_request)
        worker.start()
        try:
            request = Request(f"http://127.0.0.1:{self.server.server_port}{path}",
                              headers={"If-None-Match": etag} if etag else {})
            try:
                with urlopen(request, timeout=5) as response:
                    return response.status, response.read(), response.headers
            except HTTPError as error:
                return error.code, error.read(), error.headers
        finally:
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())

    def test_cache_reads_unchanged_file_only_once_and_invalidates_mtime_or_size(self):
        target = self.root / "static/dashboard.css"
        original_read = Path.read_bytes
        reads = []
        def read(path):
            reads.append(path)
            return original_read(path)
        with patch.object(Path, "read_bytes", read):
            first = self.assets.read("dashboard.css")
            self.assertIs(first, self.assets.read("dashboard.css"))
            self.assertEqual(reads, [target])
            stat = target.stat()
            # Same-size edit, explicitly advance only mtime.
            target.write_bytes(first.content.replace(b"#14181a", b"#ffffff"))
            os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
            second = self.assets.read("dashboard.css")
            self.assertNotEqual(first.etag, second.etag)
            self.assertIs(second, self.assets.read("dashboard.css"))
            # Size change must invalidate even if the previous mtime is restored.
            stat = target.stat()
            target.write_bytes(second.content + b"/* changed size */")
            os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            third = self.assets.read("dashboard.css")
            self.assertNotEqual(second.etag, third.etag)
            self.assertEqual(len(reads), 3)
            self.assertIs(third, self.assets.read("dashboard.css"))

    def test_asset_http_types_validators_and_allowlist(self):
        for name, content_type in self.assets.TYPES.items():
            with self.subTest(name=name):
                status, body, headers = self.get("/dashboard/assets/" + name)
                self.assertEqual(status, 200)
                self.assertEqual(body, self.assets.read(name).content)
                self.assertEqual(headers["Content-Type"], content_type)
                self.assertEqual(headers["Cache-Control"], "no-cache")
                status, body, _ = self.get("/dashboard/assets/" + name + "?v=any", headers["ETag"])
                self.assertEqual((status, body), (304, b""))
        for name in ("../http.py", "%2e%2e/http.py", "http.py", "nested/dashboard.css", ""):
            self.assertEqual(self.get("/dashboard/assets/" + name)[0], 404)
        (self.root / "static/dashboard.css").unlink()
        status, _, headers = self.get("/dashboard/assets/dashboard.css")
        self.assertEqual(status, 503)
        self.assertEqual(headers["Cache-Control"], "no-store")

    def test_html_css_js_edits_invalidate_same_server_and_do_not_change_json_or_jobs(self):
        queued = self.broker.submit("interactive", "generate", {"prompt": "private"})
        running = self.broker.submit("interactive", "generate", {"prompt": "private"})
        with self.broker.db:
            self.broker.db.execute(
                "UPDATE jobs SET state='running',started=110,lease_until=999 WHERE id=?", (running["id"],))
        api_status, api_body, api_headers = self.get("/v1/dashboard")
        before = json.loads(api_body)
        self.assertEqual(api_status, 200)
        self.assertEqual(set(before), {"timestamp", "maintenance", "queue_paused", "sources", "active_jobs",
                                      "overall", "forecast", "history", "observation"})
        self.assertEqual(before, self.broker.dashboard(include_producer_storage=False))
        instance = self.server
        status, body, headers = self.get("/dashboard")
        self.assertEqual(status, 200)
        etag = headers["ETag"]
        self.assertNotIn(queued["id"].encode(), body)
        self.assertNotIn(running["id"].encode(), body)
        self.assertEqual(self.get("/dashboard", etag)[:2], (304, b""))
        for name, marker in (
            ("dashboard.html", b"<!-- hot html -->"),
            ("dashboard.css", b"/* hot css */"),
            ("dashboard.js", b"/* hot js */"),
            ("table_sort.js", b"/* hot sort */"),
        ):
            with self.subTest(name=name):
                _, _, asset_headers = self.get("/dashboard/assets/" + name)
                asset_path = (self.root if name == "table_sort.js" else self.root / "static") / name
                asset_path.write_bytes(asset_path.read_bytes() + marker)
                status, page, next_headers = self.get("/dashboard", etag)
                self.assertEqual(status, 200)
                self.assertNotEqual(next_headers["ETag"], etag)
                self.assertNotEqual(page, body)
                if name == "dashboard.html":
                    self.assertIn(marker, page)
                else:
                    self.assertIn(self.assets.read(name).etag.strip('"').encode(), page)
                status, updated_asset, _ = self.get("/dashboard/assets/" + name, asset_headers["ETag"])
                self.assertEqual(status, 200)
                self.assertIn(marker, updated_asset)
                etag, body = next_headers["ETag"], page
                self.assertIs(self.server, instance)
                self.assertEqual(self.broker.status(queued["id"])["state"], "queued")
                self.assertEqual(self.broker.status(running["id"])["state"], "running")
                self.assertEqual(self.broker.status(running["id"])["lease_until"], 999)
                self.assertEqual(self.get("/v1/dashboard", api_headers["ETag"])[:2], (304, b""))
        self.assertEqual(json.loads(self.get("/v1/dashboard")[1]), before)

    def test_hot_ui_update_while_real_dispatch_attempt_keeps_running_then_completes(self):
        entered, release = threading.Event(), threading.Event()
        runs = []
        class BlockingOllama(FakeOllama):
            def run(self, *args):
                runs.append(args)
                entered.set()
                if not release.wait(timeout=20):
                    raise RuntimeError("test did not release the execution")
                return {"done": True}
        self.broker.ollama = BlockingOllama()
        job = self.broker.submit("interactive", "generate", {"prompt": "private"})
        worker = threading.Thread(target=self.broker.dispatch_once)
        worker.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            before = self.broker.status(job["id"])
            self.assertEqual(before["state"], "running")
            status, _, headers = self.get("/dashboard")
            self.assertEqual(status, 200)
            target = self.root / "static/dashboard.js"
            target.write_bytes(target.read_bytes() + b"/* update during execution */")
            self.assertEqual(self.get("/dashboard", headers["ETag"])[0], 200)
            self.assertIn(b"/* update during execution */", self.get("/dashboard/assets/dashboard.js")[1])
            after = self.broker.status(job["id"])
            self.assertEqual(after["state"], "running")
            self.assertEqual(after["lease_until"], before["lease_until"])
            self.assertEqual(after["attempt_count"], before["attempt_count"])
            self.assertTrue(worker.is_alive())
        finally:
            release.set()
            worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        final = self.broker.status(job["id"])
        self.assertEqual(final["state"], "completed")
        self.assertEqual(final["attempt_count"], 1)
        self.assertEqual(len(runs), 1)

    def test_unavailable_shell_is_hot_readable_and_never_caches_a_zero_queue(self):
        unavailable = {"observation": {"state": "unavailable", "observed_at": 120, "reason": "locked"}}
        with patch.object(self.broker, "dashboard", return_value=unavailable):
            status, body, headers = self.get("/dashboard")
            self.assertEqual(status, 503)
            self.assertIn(b"Dashboard data unavailable", body)
            self.assertNotIn(b"Completed:", body)
            self.assertEqual(headers["Cache-Control"], "no-store")
            target = self.root / "static/dashboard-unavailable.html"
            target.write_bytes(target.read_bytes() + b"<!-- unavailable hot edit -->")
            status, body, changed_headers = self.get("/dashboard", headers["ETag"])
            self.assertEqual(status, 503)
            self.assertIn(b"<!-- unavailable hot edit -->", body)
            self.assertNotEqual(headers["ETag"], changed_headers["ETag"])
            status, body, _ = self.get("/v1/dashboard")
            self.assertEqual(status, 503)
            self.assertEqual(json.loads(body), unavailable)
