"""Synthetic dedicated-source tests: no network GPU or production policy."""
import base64
import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from broker.http import serve
from broker.profiles import PROFILES
from broker.service import Broker, SourcePolicy, SourceAdmissionBlocked, SourceQueueFull


class FakeGPU:
    def __init__(self):
        self.requests = []
    def ps(self):
        return {'models': [{'name': 'qwen3-vl:30b', 'size_vram': 1}]}
    def is_ready(self, model):
        return model == 'qwen3-vl:30b'
    def run(self, kind, request):
        self.requests.append((kind, request))
        return {'model': request['model'], 'response': 'synthetic description', 'done': True}


class FakeWOL:
    def wake(self):
        pass


class NextcloudTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.policy_path = self.root / 'policy.json'
        self.policy_path.write_text(json.dumps({'sources': {
            'nextcloud': {'weight': 7, 'enabled': True, 'admission_allowed': True},
            'openclaw': {'weight': 1}, 'olya-vision': {'weight': 8}}}))
        self.policy = SourcePolicy(self.policy_path)
        self.gpu = FakeGPU()
        self.broker = Broker(self.root / 'test.sqlite3', self.gpu, FakeWOL(), source_policy=self.policy)
        self.addCleanup(self.broker.db.close)
        self.payload = {'prompt': 'describe', 'images': [base64.b64encode(b'fixture').decode()]}

    def submit(self, key='version1', payload=None, **kwargs):
        args = {'profile': 'nextcloud-qwen3-vl', 'kind': 'generate', 'source': 'nextcloud',
                'external_id': key, 'source_item_id': '3', 'payload': payload or self.payload}
        args.update(kwargs)
        return self.broker.submit(**args)

    def test_exact_source_model_options_and_retained_replay(self):
        original_policy = self.policy_path.read_bytes()
        first = self.submit()
        self.assertEqual(first['source'], 'nextcloud')
        self.assertEqual(self.submit()['id'], first['id'])
        self.assertTrue(self.broker.dispatch_once(policy=self.policy))
        result = self.broker.status(first['id'])
        self.assertEqual(result['state'], 'completed')
        self.assertEqual(result['payload'], {})  # existing retention cleanup preserved
        self.assertEqual(self.submit()['id'], first['id'])
        self.assertEqual(len(self.gpu.requests), 1)
        kind, request = self.gpu.requests[0]
        self.assertEqual((kind, request['model']), ('generate', 'qwen3-vl:30b'))
        self.assertEqual(request['options'], {'num_ctx': 16384, 'num_predict': 2048, 'temperature': 0})
        self.assertEqual(request['_broker_timeout_seconds'], 900)
        self.assertEqual(self.policy_path.read_bytes(), original_policy)
        with self.assertRaisesRegex(ValueError, 'another request'):
            self.submit(payload=self.payload | {'prompt': 'changed'})
        self.policy.set_admission_allowed('nextcloud', False)
        self.assertEqual(self.submit()['id'], first['id'])
        with self.assertRaises(SourceAdmissionBlocked):
            self.submit('version2')

    def test_identity_bidirectional_and_caps(self):
        for changes in ({'source': 'openclaw'}, {'source': 'olya-vision'}, {'kind': 'chat'},
                        {'profile': 'interactive'}, {'external_id': None}, {'source_item_id': None}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.submit(**changes)
        for changes in ({'model': 'gemma4:12b'}, {'images': ['!!']}, {'images': ['']},
                        {'images': self.payload['images'] * 5}, {'prompt': 'x' * 16385},
                        {'options': {'num_ctx': 999999}}, {'tools': []}, {'stream': True}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.submit(payload=self.payload | changes)
        self.submit(); self.submit('version2')
        with self.assertRaises(SourceQueueFull):
            self.submit('version3')
        with self.assertRaisesRegex(ValueError, 'item conflict'):
            self.submit(source_item_id='other')

    def test_unknown_source_policy_fails_closed(self):
        self.broker.use_source_policy(None)
        with self.assertRaises(SourceAdmissionBlocked):
            self.submit()
        self.policy_path.write_text('{"sources": {"openclaw": {"weight": 1}}}')
        self.broker.use_source_policy(SourcePolicy(self.policy_path))
        with self.assertRaises(SourceAdmissionBlocked):
            self.submit()

    def test_concurrent_same_key_and_restart_keep_one_job(self):
        jobs = []
        def worker():
            jobs.append(self.submit()['id'])
        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(len(jobs), 8)
        self.assertEqual(len(set(jobs)), 1)
        other = Broker(self.root / 'test.sqlite3', FakeGPU(), FakeWOL(), source_policy=self.policy)
        try:
            replay = other.submit('nextcloud-qwen3-vl', 'generate', self.payload, 'nextcloud', '3', 'version1')
            self.assertEqual(replay['id'], jobs[0])
        finally:
            other.db.close()

    def test_http_admission_correlation_result_evidence(self):
        server = serve(self.broker, port=0, policy=self.policy)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        def request(path, data=None):
            req = Request(f'http://127.0.0.1:{server.server_port}' + path,
                          data=json.dumps(data).encode() if data is not None else None,
                          headers={'Content-Type': 'application/json'})
            with urlopen(req) as response:
                return json.load(response)
        try:
            body = {'source': 'nextcloud', 'profile': 'nextcloud-qwen3-vl', 'kind': 'generate',
                    'source_item_id': '3', 'external_id': 'http-version', 'payload': self.payload}
            job = request('/v1/jobs', body)
            correlation = request('/v1/correlations?source=nextcloud&external_id=http-version')['jobs']
            self.assertEqual(correlation[0]['job_id'], job['id'])
            self.broker.dispatch_once(policy=self.policy)
            result = request('/v1/jobs/' + job['id'])
            self.assertEqual(result['source'], 'nextcloud')
            self.assertEqual(result['profile'], 'nextcloud-qwen3-vl')
            self.assertEqual(result['result']['model'], 'qwen3-vl:30b')
            self.assertEqual(request('/v1/jobs', body)['id'], job['id'])
        finally:
            server.shutdown(); thread.join(); server.server_close()


if __name__ == '__main__':
    unittest.main()
