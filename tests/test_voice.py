import base64
import json
import os
import tempfile
import threading
import unittest
import http.server
from broker import voice
from unittest.mock import patch
from broker.service import Broker, SourcePolicy
from tests.test_batch_scheduler import MutableClock, FakeOllama, FakeWol


class VoiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = MutableClock()
        self.calls = []
        self.broker = Broker(os.path.join(self.tmp.name, 'db'), FakeOllama(self.calls, self.clock, 400), FakeWol(self.calls), clock=self.clock)
        self.addCleanup(self.broker.db.close)
        path = os.path.join(self.tmp.name, 'policy')
        with open(path, 'w') as f:
            json.dump({'sources': {'local-voice': {'weight': 2}, 'shutterstock-video': {'weight': 7}}}, f)
        self.policy = SourcePolicy(path)
        self.payload = {'filename': 'a.ogg', 'audio_base64': base64.b64encode(b'audio').decode()}

    def voice(self):
        return self.broker.submit('voice-gigaam-v1', 'transcribe', self.payload, source='local-voice')['id']

    def test_voice_dispatch_and_ollama_unchanged(self):
        job = self.voice()
        with patch('broker.voice.execute', return_value={'text': 'hello'}) as execute:
            self.broker.dispatch_once(policy=self.policy)
        self.assertEqual(execute.call_args.args[0], 'voice-gigaam-v1')
        self.assertEqual(self.broker.status(job)['state'], 'completed')
        self.assertEqual(self.broker.status(job)['switch_reason'], 'voice executor call completed')
        self.assertFalse(any(c == 'ps' or isinstance(c, tuple) and c[0] in ('run', 'unload') for c in self.calls))
        normal = self.broker.submit('interactive', 'generate', {'prompt': 'hello'})['id']
        self.broker.dispatch_once()
        self.assertEqual(self.broker.status(normal)['state'], 'completed')
        self.assertTrue(any(isinstance(c, tuple) and c[0] == 'run' for c in self.calls))

    def test_weight_two_versus_seven(self):
        voice = [self.voice() for _ in range(12)]
        video = [self.broker.submit('shutterstock-video', 'generate', {'prompt': str(i)})['id'] for i in range(12)]
        def run(*args):
            self.clock.advance(400)
            return {'text': 'ok'}
        with patch('broker.voice.execute', side_effect=run):
            for _ in range(9): self.broker.dispatch_once(policy=self.policy)
        self.assertEqual(sum(self.broker.status(i)['state'] == 'completed' for i in voice), 7)
        self.assertEqual(sum(self.broker.status(i)['state'] == 'completed' for i in video), 2)

    def test_no_preemption_or_cancel_release_until_return(self):
        job = self.voice()
        entered, release = threading.Event(), threading.Event()
        def run(*args):
            entered.set()
            if not release.wait(5): raise RuntimeError('test timeout')
            return {'text': 'ok'}
        with patch('broker.voice.execute', side_effect=run):
            thread = threading.Thread(target=lambda: self.broker.dispatch_once(policy=self.policy))
            thread.start()
            try:
                self.assertTrue(entered.wait(2))
                self.broker.cancel(job)
                self.broker.submit('shutterstock-video', 'generate', {'prompt': 'waiting'})
                self.assertFalse(self.broker.dispatch_once(policy=self.policy))
                self.assertEqual(self.broker.status(job)['state'], 'cancel_requested')
            finally:
                release.set()
                thread.join(5)
            self.assertEqual(self.broker.status(job)['state'], 'cancelled')

    def test_http_worker_wire_contract(self):
        received = {}
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                received.update(path=self.path, auth=self.headers.get('Authorization'),
                                audio=self.rfile.read(int(self.headers['Content-Length'])))
                body = json.dumps({'text': 'test', 'model': 'gigaam-v3-e2e-rnnt'}).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args): pass
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            with patch.dict(os.environ, {'BROKER_GIGAAM_URL': 'http://127.0.0.1:%d' % server.server_port,
                                        'BROKER_GIGAAM_TOKEN': 'test-token'}):
                result = voice.execute('voice-gigaam-v1', self.payload, 5)
            self.assertEqual(result['text'], 'test')
            self.assertEqual(received, {'path': '/v1/transcribe', 'auth': 'Bearer test-token', 'audio': b'audio'})
        finally:
            server.shutdown()
            worker.join(5)
            server.server_close()

    def test_profile_contract(self):
        with self.assertRaises(ValueError): self.broker.submit('voice-gigaam-v1', 'transcribe', self.payload, source='other')
        with self.assertRaises(ValueError): self.broker.submit('interactive', 'transcribe', self.payload, source='local-voice')
        with self.assertRaises(ValueError): self.broker.submit('voice-wyoming-v1', 'transcribe', self.payload, source='local-voice')
