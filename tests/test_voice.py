import base64
import json
import os
import tempfile
import threading
import unittest
import http.server
import io
import wave
import urllib.error
from clients import broker_voice_client
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

    def test_multiple_streams_hold_one_lease_until_each_call_returns(self):
        wav = io.BytesIO()
        with wave.open(wav, 'wb') as audio:
            audio.setparams((1, 2, 16000, 0, 'NONE', 'not compressed'))
            audio.writeframes(b'\0\0' * 32)
        payloads = {
            'voice-wyoming-v1': {'filename': 'a.wav', 'audio_base64': base64.b64encode(wav.getvalue()).decode()},
            'voice-whisper-gpu-v1': self.payload,
        }
        entered = [threading.Event() for _ in range(4)]
        release = [threading.Event() for _ in range(4)]
        calls, active = [], []
        def run(profile, payload, timeout):
            index = len(calls)
            calls.append(profile)
            active.append(profile)
            entered[index].set()
            try:
                if not release[index].wait(5):
                    raise RuntimeError('test timeout')
                return {'text': profile}
            finally:
                active.remove(profile)

        profiles = ['voice-wyoming-v1', 'voice-whisper-gpu-v1'] * 2
        jobs = []
        with patch('broker.voice.execute', side_effect=run):
            for index, profile in enumerate(profiles):
                jobs.append(self.broker.submit(profile, 'transcribe', payloads[profile], source='local-voice')['id'])
                self.clock.advance(1)
                if index == 0:
                    worker = threading.Thread(target=lambda: self.broker.dispatch_once(policy=self.policy))
                    worker.start()
                    self.assertTrue(entered[0].wait(2))
            try:
                for index in range(4):
                    if index:
                        worker = threading.Thread(target=lambda: self.broker.dispatch_once(policy=self.policy))
                        worker.start()
                        self.assertTrue(entered[index].wait(2))
                    running = [job for job in jobs if self.broker.status(job)['state'] == 'running']
                    self.assertEqual(len(running), 1)
                    self.assertEqual(len(active), 1)
                    attempt = self.broker.attempts(running[0])[0]
                    self.assertGreaterEqual(attempt['lease_until'], self.clock() + 1800)
                    self.assertFalse(self.broker.dispatch_once(policy=self.policy))
                    self.assertEqual(sum(len(self.broker.attempts(job)) for job in jobs), index + 1)
                    release[index].set()
                    worker.join(5)
                    self.assertFalse(worker.is_alive())
                    self.assertEqual(self.broker.status(running[0])['state'], 'completed')
                self.assertCountEqual(calls, profiles)
                self.assertTrue(all(self.broker.status(job)['state'] == 'completed' for job in jobs))
            finally:
                for event in release:
                    event.set()
                worker.join(5)

    def test_low_priority_voice_coexists_with_light_model(self):
        path = os.path.join(self.tmp.name, 'coexistence-policy')
        with open(path, 'w') as f:
            json.dump({'sources': {'local-voice': {'weight': 7}, 'openclaw': {'weight': 1}}}, f)
        policy = SourcePolicy(path)  # Test-only policy; production voice remains weight 2.
        voice_jobs = [self.voice() for _ in range(8)]
        light_jobs = [self.broker.submit('openclaw-smolvlm2-500m', 'chat',
                      {'messages': [{'role': 'user', 'content': str(i)}]}, source='openclaw')['id']
                      for i in range(8)]
        def transcribe(*args):
            self.clock.advance(400)
            return {'text': 'ok'}
        with patch('broker.voice.execute', side_effect=transcribe):
            # 400-second non-preemptive jobs overrun the 3,150-second light
            # allocation by 50 seconds; the ninth selection is voice.
            for index in range(9):
                self.assertTrue(self.broker.dispatch_once(policy=policy))
                if index == 0:
                    light_jobs.append(self.broker.submit('openclaw-smolvlm2-500m', 'chat',
                        {'messages': [{'role': 'user', 'content': 'extra'}]}, source='openclaw')['id'])
        self.assertEqual(sum(self.broker.status(job)['state'] == 'completed' for job in light_jobs), 8)
        self.assertEqual(sum(self.broker.status(job)['state'] == 'completed' for job in voice_jobs), 1)
        self.assertTrue(any(isinstance(call, tuple) and call[0] == 'run' and
                            call[2].get('model', '').startswith('hf.co/ggml-org/SmolVLM2-500M')
                            for call in self.calls))

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

    def test_client_request_and_retry_after_lost_response(self):
        path = os.path.join(self.tmp.name, 'audio.ogg')
        with open(path, 'wb') as f:
            f.write(b'audio')
        requests = []
        def submit(request, timeout):
            body = json.loads(request.data)
            requests.append(body)
            self.assertEqual(request.full_url, 'http://broker.test/v1/jobs')
            job = self.broker.submit(body['profile'], body['kind'], body['payload'],
                                     source=body['source'], source_item_id=body['source_item_id'],
                                     external_id=body['external_id'])
            if len(requests) == 1:
                raise urllib.error.URLError('response lost')
            return io.BytesIO(json.dumps({'id': job['id']}).encode())
        with patch.dict(os.environ, {'BROKER_VOICE_ENABLED': '1'}), \
             patch('urllib.request.urlopen', side_effect=submit), patch('time.sleep'):
            result = broker_voice_client.submit(path, 'gigaam', 'http://broker.test',
                                                 external_id='stable', source_item_id='audio')
        self.assertEqual(requests[0], requests[1])
        self.assertEqual(requests[0]['source'], 'local-voice')
        self.assertEqual(requests[0]['profile'], 'voice-gigaam-v1')
        self.assertEqual(requests[0]['kind'], 'transcribe')
        self.assertNotIn('weight', requests[0])  # Weight belongs to server policy.
        self.assertEqual(self.policy.weight('local-voice'), 2)
        production_policy = SourcePolicy(os.path.join(os.path.dirname(__file__), '..', 'config', 'sources.production.json'))
        self.assertEqual(production_policy.weight('local-voice'), 2)
        self.assertEqual(self.broker.status(result['id'])['state'], 'queued')
        self.assertEqual(self.broker.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 1)
        with self.assertRaises(ValueError):
            self.broker.submit('voice-gigaam-v1', 'transcribe',
                               {'filename': 'other.ogg', 'audio_base64': self.payload['audio_base64']},
                               source='local-voice', source_item_id='audio', external_id='stable')

    def test_client_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=True), patch('urllib.request.urlopen') as urlopen:
            with self.assertRaisesRegex(RuntimeError, 'disabled'):
                broker_voice_client.submit('not-opened', 'gigaam', 'http://broker.test')
            urlopen.assert_not_called()

    def test_profile_contract(self):
        with self.assertRaises(ValueError): self.broker.submit('voice-gigaam-v1', 'transcribe', self.payload, source='other')
        with self.assertRaises(ValueError): self.broker.submit('interactive', 'transcribe', self.payload, source='local-voice')
        with self.assertRaises(ValueError): self.broker.submit('voice-wyoming-v1', 'transcribe', self.payload, source='local-voice')
