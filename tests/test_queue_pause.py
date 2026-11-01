import tempfile
import threading
import unittest
from unittest.mock import Mock
from broker.service import Broker


class QueuePauseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = self.temp.name + '/queue.db'
        self.backend = Mock()
        self.backend.ps.return_value = {'models': [{'name': 'qwen3.5:9b'}]}
        self.backend.is_ready.return_value = True
        self.backend.run.return_value = {'done': True}
        self.broker = Broker(self.path, self.backend, Mock(), clock=lambda: 100)
        self.addCleanup(self.broker.db.close)

    def submit(self):
        return self.broker.submit('interactive', 'generate', {'prompt': 'test'}, source='alpha')

    def test_pause_persists_allows_enqueue_and_resume(self):
        self.broker.set_queue_paused(True)
        job = self.submit()
        self.assertFalse(self.broker.dispatch_once())
        self.assertEqual(self.broker.status(job['id'])['state'], 'queued')
        self.backend.unload.assert_called_with('qwen3.5:9b')
        other = Broker(self.path, self.backend, Mock(), clock=lambda: 100)
        try:
            self.assertTrue(other.queue_paused())
        finally:
            other.db.close()
        self.broker.set_queue_paused(False)
        self.assertTrue(self.broker.dispatch_once())
        self.assertEqual(self.broker.status(job['id'])['state'], 'completed')

    def test_active_job_finishes_before_unload(self):
        started, release = threading.Event(), threading.Event()
        def run(*args):
            started.set()
            if not release.wait(5):
                raise RuntimeError('test timed out')
            return {'done': True}
        self.backend.run.side_effect = run
        job = self.submit()
        worker = threading.Thread(target=self.broker.dispatch_once)
        worker.start()
        try:
            self.assertTrue(started.wait(2))
            self.backend.unload.reset_mock()
            self.broker.set_queue_paused(True)
            self.assertEqual(self.broker.status(job['id'])['state'], 'running')
            self.assertFalse(self.broker.dispatch_once())
            self.backend.unload.assert_not_called()
            self.submit()
        finally:
            release.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.broker.status(job['id'])['state'], 'completed')
        self.backend.unload.assert_called_once_with('qwen3.5:9b')
        self.assertFalse(self.broker.dispatch_once())

    def test_backend_failures_do_not_escape_and_are_retried(self):
        self.submit()
        self.broker.set_queue_paused(True)
        self.backend.ps.side_effect = RuntimeError('offline')
        self.assertFalse(self.broker.dispatch_once())
        self.backend.ps.side_effect = None
        self.backend.unload.side_effect = RuntimeError('unload failed')
        self.assertFalse(self.broker.dispatch_once())
        self.backend.unload.side_effect = None
        self.assertFalse(self.broker.dispatch_once())
        self.backend.run.assert_not_called()
        self.assertEqual(self.backend.unload.call_count, 2)

    def test_http_validation_and_dashboard_state(self):
        import json
        from broker.http import serve
        from urllib.request import Request, urlopen
        from urllib.error import HTTPError
        server = serve(self.broker, port=0)
        self.addCleanup(server.server_close)
        for body, expected in [({'paused': True}, 200), ({'paused': 1}, 400), ({}, 400), ({'paused': False, 'extra': 1}, 400), ({'paused': False}, 200)]:
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            try:
                req = Request(f'http://127.0.0.1:{server.server_port}/v1/queue/pause', data=json.dumps(body).encode(), headers={'Content-Type': 'application/json'})
                try:
                    with urlopen(req, timeout=5) as response:
                        self.assertEqual(response.status, expected)
                except HTTPError as error:
                    self.assertEqual(error.code, expected)
            finally:
                thread.join(5)
            self.assertFalse(thread.is_alive())
        self.assertFalse(self.broker.dashboard()['queue_paused'])

    def test_pause_releases_only_idle_whisper_owner_before_ollama_unload(self):
        executor = Mock()
        executor.can_release.return_value = False
        executor.release_idle.return_value = False
        self.broker.whisper_executor = executor
        self.broker.set_executor_fence('warm-owner', 'finished-job')
        self.broker.set_queue_paused(True)
        self.assertFalse(self.broker.dispatch_once())
        executor.release_idle.assert_called_once_with(self.broker)
        self.backend.unload.assert_not_called()
        self.assertIsNotNone(self.broker.executor_fence())
        executor.can_release.return_value = True
        self.assertFalse(self.broker.dispatch_once())
        self.assertIsNone(self.broker.executor_fence())
        self.backend.unload.assert_called_once_with('qwen3.5:9b')
