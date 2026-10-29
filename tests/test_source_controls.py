import json
import hashlib
import os
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from broker.http import serve
from broker.service import Broker, SourcePolicy


class FakeWol:
    def wake(self):
        pass


class FakeOllama:
    def ps(self):
        return {"models": [{"name": "qwen3.5:9b"}]}

    def is_ready(self, _model):
        return True

    def unload(self, _model):
        pass

    def run(self, _kind, _request):
        return {"done": True}


class BlockingOllama(FakeOllama):
    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()

    def run(self, _kind, _request):
        self.started.set()
        self.release.wait(2)
        return {"done": True}


class SourceControlsTests(unittest.TestCase):
    def setUp(self):
        self.database = tempfile.NamedTemporaryFile()
        self.addCleanup(self.database.close)
        descriptor, self.policy_path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(descriptor, "w") as handle:
            json.dump({
                "version": 1,
                "sources": {
                    "alpha": {"enabled": True, "weight": 1},
                    "beta": {
                        "enabled": True,
                        "weight": 2,
                        "admission_allowed": True,
                    },
                },
            }, handle)
        self.addCleanup(lambda: os.unlink(self.policy_path))
        self.policy = SourcePolicy(self.policy_path)
        self.broker = Broker(self.database.name, FakeOllama(), FakeWol(), clock=lambda: 100)
        self.addCleanup(self.broker.db.close)
        self.server = serve(self.broker, port=0, policy=self.policy, storage_token='test-token')
        self.addCleanup(self.server.server_close)

    def request(self, path, body=None, *, authenticated=True):
        data = None if body is None else json.dumps(body).encode()
        request = Request(
            f"http://127.0.0.1:{self.server.server_port}{path}",
            data=data,
            headers={**({"Authorization": "Bearer test-token"} if authenticated else {}), **({"Content-Type": "application/json"} if data is not None else {})},
            method="POST" if data is not None else "GET",
        )
        worker = threading.Thread(target=self.server.handle_request)
        worker.start()
        try:
            with urlopen(request) as response:
                status, payload = response.status, json.loads(response.read())
        except HTTPError as error:
            status, payload = error.code, json.loads(error.read())
        worker.join(timeout=1)
        self.assertFalse(worker.is_alive())
        return status, payload

    def submit(self, source, prompt="private"):
        return self.broker.submit(
            "interactive", "generate", {"prompt": prompt}, source=source
        )

    def test_unregistered_source_and_retries_are_blocked_while_reads_remain(self):
        existing = self.submit('alpha')
        self.broker.cancel(existing['id'])
        self.assertEqual(self.request('/v1/jobs', {'profile': 'interactive', 'kind': 'generate',
            'source': 'unregistered', 'payload': {'prompt': 'synthetic'}})[0], 403)
        self.policy.set_admission_allowed('alpha', False)
        self.assertEqual(self.request('/v1/jobs/'+existing['id']+'/retry', {})[0], 403)
        self.assertEqual(self.request('/v1/sources/alpha/failed/retry', {'confirm': True})[0], 403)
        self.assertEqual(self.request('/v1/jobs/'+existing['id'])[0], 200)
        self.assertEqual(self.broker.status(existing['id'])['state'], 'cancelled')

    def test_declared_maintenance_blocks_all_submission_shapes_and_retains_replay(self):
        with open(self.policy_path) as f: raw = json.load(f)
        for name in ('local-voice', 'openclaw', 'shutterstock-canary', 'shutterstock-video',
                     'olya-vision', 'olya-decision', 'syncopia-telegram-memory'):
            raw['sources'][name] = {'enabled': True, 'weight': 2}
        with open(self.policy_path, 'w') as f: json.dump(raw, f)
        voice = {'filename': 'a.ogg', 'audio_base64': 'YQ=='}
        first = self.broker.submit('voice-gigaam-v1', 'transcribe', voice,
                                   source='local-voice', external_id='k', source_item_id='item')
        with open(self.policy_path, 'rb') as f: checksum = hashlib.sha256(f.read()).hexdigest()
        self.assertEqual(self.request('/v1/maintenance/admission', {'enabled': True, 'expected_sha256': checksum}), (200, {'maintenance': True}))
        self.assertTrue(self.policy.snapshot()['maintenance'])
        projected = self.broker.dashboard(self.policy)
        alpha = next(row for row in projected['sources'] if row['source'] == 'alpha')
        self.assertFalse(alpha['scheduler']['admission_allowed'])
        from broker.dashboard import render
        html = render(projected)
        self.assertIn(b'Admission maintenance is active', html)
        self.assertIn(b'<select name="admission" aria-label="Admission for alpha" disabled>', html)
        self.assertEqual(self.broker.submit('voice-gigaam-v1', 'transcribe', voice,
                         source='local-voice', external_id='k', source_item_id='item')['id'], first['id'])
        from broker.openclaw import MODEL
        media = {'prompt': 'test', 'images': ['YQ=='], 'format': {'type': 'object'}}
        routes = [
            ('/v1/jobs', {'profile': 'interactive', 'kind': 'generate', 'source': 'alpha', 'payload': {'prompt': 'x'}}),
            ('/api/generate', {'profile': 'interactive', 'source': 'alpha', 'prompt': 'x', 'stream': False}),
            ('/api/chat', {'profile': 'interactive', 'source': 'alpha', 'messages': [{'role': 'user', 'content': 'x'}], 'stream': False}),
            ('/openclaw/api/chat', {'model': MODEL, 'messages': [{'role': 'user', 'content': 'x'}], 'stream': False}),
            ('/v1/shutterstock-canary/generate', media),
            ('/v1/shutterstock-video/generate', media),
            ('/v1/olya-vision/generate', {**media, 'model': 'gemma4:12b'}),
            ('/v1/olya-decision/generate', {'model': 'qwen3.8:ad-iq2-xs', 'prompt': 'x', 'format': {'type': 'object'}}),
            ('/v1/syncopia-memory/extract', {'model': 'qwen3.8:ad-iq2-xs', 'messages': [{'role': 'system', 'content': 'x'}, {'role': 'user', 'content': 'x'}], 'tools': [], 'stream': False, 'response_format': {'type': 'json_object'}, 'format': {'type': 'object'}}),
        ]
        for path, body in routes:
            with self.subTest(path=path):
                status, response = self.request(path, body)
                self.assertEqual((status, response.get('error', {}).get('code')), (403, 'source_admission_blocked'))
        self.assertEqual(self.request('/v1/jobs/'+first['id'])[0], 200)
        self.assertEqual(self.broker.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 1)

    def test_maintenance_requires_auth_and_compare_hash_and_waits_for_claim_lock(self):
        with open(self.policy_path, 'rb') as f: checksum = hashlib.sha256(f.read()).hexdigest()
        body = {'enabled': True, 'expected_sha256': checksum}
        self.assertEqual(self.request('/v1/maintenance/admission', body, authenticated=False)[0], 401)
        self.assertEqual(self.request('/v1/maintenance/admission', {**body, 'expected_sha256': '0'*64})[0], 400)
        self.assertFalse(self.policy.snapshot()['maintenance'])
        result = []
        with self.broker.lock:
            thread = threading.Thread(target=lambda: result.append(self.request('/v1/maintenance/admission', body)))
            thread.start()
            thread.join(0.1)
            self.assertTrue(thread.is_alive())
        thread.join(2)
        self.assertEqual(result, [(200, {'maintenance': True})])
        self.assertFalse(self.policy.admission_allowed('alpha'))
        self.assertEqual(self.request('/v1/maintenance/admission', {'enabled': False, 'expected_sha256': checksum})[0], 400)
        self.assertTrue(self.policy.snapshot()['maintenance'])

    def test_old_policy_defaults_admission_allowed_and_dashboard_reports_controls(self):
        self.assertTrue(self.policy.admission_allowed("alpha"))
        status, body = self.request("/v1/sources")
        self.assertEqual(status, 200)
        self.assertTrue(body["policy"]["sources"]["alpha"]["admission_allowed"])
        dashboard = self.broker.dashboard(self.policy)
        alpha = next(row for row in dashboard["sources"] if row["source"] == "alpha")
        self.assertEqual(alpha["scheduler"]["dispatch_paused"], False)
        self.assertEqual(alpha["scheduler"]["admission_allowed"], True)

    def test_admission_block_is_hot_machine_readable_and_does_not_create_jobs(self):
        existing = self.submit("alpha")
        before = self.broker.db.execute("SELECT count(*) FROM jobs").fetchone()[0]

        status, body = self.request(
            "/v1/sources/alpha/admission", {"allowed": False}
        )
        self.assertEqual(
            (status, body),
            (200, {"source": "alpha", "admission_allowed": False}),
        )
        for path, request_body in (
            ("/v1/jobs", {
                "profile": "interactive",
                "kind": "generate",
                "source": "alpha",
                "payload": {"prompt": "must-not-persist"},
            }),
            ("/api/generate", {
                "profile": "interactive",
                "source": "alpha",
                "prompt": "must-not-persist",
                "stream": False,
            }),
        ):
            status, rejection = self.request(path, request_body)
            self.assertEqual(status, 403)
            self.assertEqual(rejection["error"]["code"], "source_admission_blocked")
            self.assertEqual(rejection["source"], "alpha")
            self.assertFalse(rejection["admission_allowed"])

        self.assertEqual(
            self.broker.db.execute("SELECT count(*) FROM jobs").fetchone()[0],
            before,
        )
        self.assertEqual(self.broker.status(existing["id"])["state"], "queued")
        rejected = self.broker.audit_events(source="alpha")
        self.assertEqual(
            sum(event["event_type"] == "admission.rejected" for event in rejected), 2
        )
        self.assertTrue(any(
            event["event_type"] == "source.admission_changed" for event in rejected
        ))
        self.assertNotIn("must-not-persist", json.dumps(rejected))

        status, _ = self.request("/v1/sources/alpha/admission", {"allowed": True})
        self.assertEqual(status, 200)
        status, admitted = self.request("/v1/jobs", {
            "profile": "interactive",
            "kind": "generate",
            "source": "alpha",
            "payload": {"prompt": "hot-unblock"},
        })
        self.assertEqual(status, 202)
        self.assertEqual(admitted["state"], "queued")

    def test_dispatch_pause_preserves_queue_and_resume_is_hot(self):
        first = self.submit("alpha")
        second = self.submit("alpha")
        beta = self.submit("beta")
        with self.broker.db:
            self.broker.db.execute(
                "UPDATE jobs SET state='completed',finished=? WHERE id=?",
                (100, first["id"]),
            )

        status, paused = self.request("/v1/sources/alpha/dispatch", {"paused": True})
        self.assertEqual(
            paused, {"source": "alpha", "paused": True, "enabled": False}
        )
        self.assertEqual(status, 200)
        self.assertTrue(any(
            event["event_type"] == "source.dispatch_changed"
            for event in self.broker.audit_events(source="alpha")
        ))
        self.assertTrue(
            self.broker.dispatch_once(self.policy.enabled_sources(), self.policy)
        )
        self.assertEqual(self.broker.status(beta["id"])["state"], "completed")
        self.assertEqual(self.broker.status(second["id"])["state"], "queued")

        status, resumed = self.request("/v1/sources/alpha/dispatch", {"paused": False})
        self.assertEqual(status, 200)
        self.assertFalse(resumed["paused"])
        self.assertTrue(
            self.broker.dispatch_once(self.policy.enabled_sources(), self.policy)
        )
        self.assertEqual(self.broker.status(second["id"])["state"], "completed")

    def test_pause_while_running_allows_completion_but_prevents_next_lease(self):
        database = tempfile.NamedTemporaryFile()
        self.addCleanup(database.close)
        ollama = BlockingOllama()
        ticks = iter(range(100, 10_000))
        broker = Broker(database.name, ollama, FakeWol(), clock=lambda: next(ticks))
        self.addCleanup(broker.db.close)
        broker.use_source_policy(self.policy)
        running = broker.submit("interactive", "generate", {"prompt": "first"}, source="alpha")
        queued = broker.submit("interactive", "generate", {"prompt": "second"}, source="alpha")

        worker = threading.Thread(
            target=broker.dispatch_once,
            args=(self.policy.enabled_sources(), self.policy),
        )
        worker.start()
        self.assertTrue(ollama.started.wait(1))
        self.policy.set_enabled("alpha", False)
        self.assertEqual(broker.status(running["id"])["state"], "running")
        self.assertEqual(broker.status(queued["id"])["state"], "queued")
        ollama.release.set()
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(broker.status(running["id"])["state"], "completed")
        self.assertFalse(
            broker.dispatch_once(self.policy.enabled_sources(), self.policy)
        )
        self.assertEqual(broker.status(queued["id"])["state"], "queued")
        self.policy.set_enabled("alpha", True)
        self.assertTrue(
            broker.dispatch_once(self.policy.enabled_sources(), self.policy)
        )
        self.assertEqual(broker.status(queued["id"])["state"], "completed")

    def test_bulk_state_boundaries_idempotency_history_and_exact_source(self):
        queued = self.submit("alpha")
        other_queued = self.submit("beta")
        failed = self.submit("alpha")
        cancelled = self.submit("alpha")
        completed = self.submit("alpha")
        running = self.submit("alpha")
        with self.broker.db:
            self.broker.db.execute(
                "UPDATE jobs SET state='failed',finished=90,error='private failure',"
                "attempt_count=1 WHERE id=?", (failed["id"],)
            )
            self.broker.db.execute(
                "INSERT INTO job_attempts(job_id,attempt_no,source,selected_at,"
                "scheduler_mode,selection_reason,outcome,error) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (failed["id"], 1, "alpha", 80, "fifo", "test", "failed", "private failure"),
            )
            self.broker.db.execute(
                "UPDATE jobs SET state='cancelled',finished=90 WHERE id=?",
                (cancelled["id"],),
            )
            self.broker.db.execute(
                "UPDATE jobs SET state='completed',finished=90 WHERE id=?",
                (completed["id"],),
            )
            self.broker.db.execute(
                "UPDATE jobs SET state='running',started=90,lease_until=200 WHERE id=?",
                (running["id"],),
            )

        status, body = self.request(
            "/v1/sources/alpha/queued/cancel", {"confirm": True}
        )
        self.assertEqual((status, body["cancelled"]), (200, 1))
        self.assertEqual(self.broker.status(queued["id"])["state"], "cancelled")
        self.assertEqual(self.broker.status(other_queued["id"])["state"], "queued")
        self.assertEqual(self.broker.status(running["id"])["state"], "running")
        self.assertEqual(
            self.request("/v1/sources/alpha/queued/cancel", {"confirm": True})[1]["cancelled"],
            0,
        )

        status, body = self.request(
            "/v1/sources/alpha/failed/retry", {"confirm": True}
        )
        self.assertEqual((status, body["retried"]), (200, 1))
        retried = self.broker.status(failed["id"])
        self.assertEqual(retried["state"], "queued")
        self.assertEqual((retried["attempt_count"], retried["retry_count"]), (1, 1))
        attempts = self.broker.attempts(failed["id"])
        self.assertEqual((len(attempts), attempts[0]["outcome"]), (1, "failed"))
        self.assertEqual(self.broker.status(cancelled["id"])["state"], "cancelled")
        self.assertEqual(self.broker.status(completed["id"])["state"], "completed")
        self.assertEqual(self.broker.status(running["id"])["state"], "running")
        self.assertEqual(
            self.request("/v1/sources/alpha/failed/retry", {"confirm": True})[1]["retried"],
            0,
        )
        events = self.broker.audit_events(source="alpha", limit=100)
        self.assertTrue(any(event["event_type"] == "source.bulk_cancel" for event in events))
        self.assertTrue(any(event["event_type"] == "source.bulk_retry" for event in events))
        self.assertNotIn("private failure", json.dumps(events))

    def test_bulk_confirmation_invalid_source_and_invalid_inputs_fail_closed(self):
        self.submit("alpha")
        for path in (
            "/v1/sources/alpha/queued/cancel",
            "/v1/sources/alpha/failed/retry",
        ):
            status, _ = self.request(path, {})
            self.assertEqual(status, 400)
        status, _ = self.request(
            "/v1/sources/missing/queued/cancel", {"confirm": True}
        )
        self.assertEqual(status, 400)
        self.assertEqual(
            self.broker.db.execute(
                "SELECT count(*) FROM jobs WHERE state='queued'"
            ).fetchone()[0],
            1,
        )
        for body in ({"allowed": "false"}, {}, {"allowed": False, "extra": 1}):
            status, _ = self.request("/v1/sources/alpha/admission", body)
            self.assertEqual(status, 400)

    def test_concurrent_policy_mutations_are_serialized_and_valid(self):
        barrier = threading.Barrier(3)
        errors = []

        def set_weight():
            try:
                barrier.wait()
                for value in range(1, 11):
                    self.policy.set_weight("alpha", value)
            except Exception as error:
                errors.append(error)

        def set_admission():
            try:
                barrier.wait()
                for value in (False, True) * 5:
                    self.policy.set_admission_allowed("beta", value)
            except Exception as error:
                errors.append(error)

        threads = [threading.Thread(target=set_weight), threading.Thread(target=set_admission)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        with open(self.policy_path) as handle:
            raw = json.load(handle)
        self.assertEqual(raw["sources"]["alpha"]["weight"], 10)
        self.assertTrue(raw["sources"]["beta"]["admission_allowed"])
        reloaded = SourcePolicy(self.policy_path)
        self.assertEqual(reloaded.weight("alpha"), 10.0)
        self.assertTrue(reloaded.admission_allowed("beta"))


if __name__ == "__main__":
    unittest.main()
