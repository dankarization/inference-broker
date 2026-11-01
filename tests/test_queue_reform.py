"""Stage 1: caller model/options, atomic registration, durable compatibility."""
import json
import tempfile
import unittest
from pathlib import Path

from broker.service import Broker, SourcePolicy
from broker.profiles import PROFILES
from test_broker import FakeOllama, FakeWol
from broker import nightly_eval
from broker.evaluation import normalize_request


class QueueReformTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / 'policy.json'
        self.original = {'sources': {'interactive': {'enabled': True, 'admission_allowed': True, 'weight': 3}}}
        self.path.write_text(json.dumps(self.original))
        self.policy = SourcePolicy(self.path)
        self.calls = []
        self.gpu = FakeOllama(self.calls)
        self.broker = Broker(self.root / 'queue.sqlite', self.gpu, FakeWol(self.calls), source_policy=self.policy)
        self.addCleanup(self.broker.db.close)

    def test_override_and_model_lane_and_execution(self):
        job = self.broker.submit('interactive', 'generate', {'prompt': 'hello'},
                                 model='gemma4:12b', options={'num_ctx': 65536, 'num_predict': 123})
        self.assertEqual(self.broker.forecast(self.policy)['next_selections'][0]['model'], 'gemma4:12b')
        self.assertTrue(self.broker.dispatch_once(None, self.policy))
        self.assertEqual(self.broker.status(job['id'])['state'], 'completed')
        request = [c[2] for c in self.calls if isinstance(c, tuple) and c[0] == 'run'][-1]
        self.assertEqual(request['model'], 'gemma4:12b')
        self.assertEqual(request['options'], {'num_ctx': 65536, 'num_predict': 123})

    def test_oversize_descriptive_error_before_admission(self):
        for field in ('num_ctx', 'num_predict'):
            with self.assertRaisesRegex(ValueError, f'{field}=999999.*262144.*gemma4:12b'):
                self.broker.submit('interactive', 'generate', {}, model='gemma4:12b', options={field: 999999})
        self.assertEqual(self.broker.db.execute('select count(*) from jobs').fetchone()[0], 0)

    def test_registration_and_existing_policy_preserved(self):
        before = self.path.read_bytes()
        self.broker.submit('interactive', 'generate', {}, weight=9)
        self.assertEqual(self.path.read_bytes(), before)
        for source, weight in [('new-default', None), ('new-weight', 2.5)]:
            self.broker.submit('cron', 'generate', {}, source=source, weight=weight)
            self.assertEqual(self.policy.weight(source), weight or 5)
            self.assertTrue(self.policy.admission_allowed(source))
        self.assertEqual(json.loads(self.path.read_text())['sources']['interactive'], self.original['sources']['interactive'])

    def test_legacy_durable_job_defaults_after_reopen(self):
        job = self.broker.submit('cron', 'generate', {'prompt': 'legacy'})
        reopened = Broker(self.root / 'queue.sqlite', self.gpu, FakeWol(self.calls), source_policy=self.policy)
        try:
            self.assertTrue(reopened.dispatch_once(None, self.policy))
            self.assertEqual(reopened.status(job['id'])['state'], 'completed')
            request = [c[2] for c in self.calls if isinstance(c, tuple) and c[0] == 'run'][-1]
            self.assertEqual(request['model'], PROFILES['cron'].model)
            self.assertEqual(request['options'], {'num_ctx': 8192, 'num_predict': 1024})
            self.assertEqual(reopened.status(job['id'])['source'], 'cron')
        finally:
            reopened.db.close()

    def test_eval_parameters_not_pinned(self):
        payload = normalize_request({'messages': [{'role': 'user', 'content': 'hello'}],
                    'model': 'gemma4:26b', 'options': {'num_ctx': 65536, 'num_predict': 100}}, 'eval-gemma4-12b')
        self.assertEqual(payload['model'], 'gemma4:26b')
        self.assertEqual(payload['options']['num_ctx'], 65536)
        self.assertEqual(payload['options']['num_predict'], 100)

    def test_pilot_identity_and_custom_runtime(self):
        job = self.broker.submit('eval-gemma4-12b', 'chat',
            {'messages': [{'role': 'user', 'content': 'test'}]}, source='pilot-mainpc',
            model='gemma4:26b', options={'num_ctx': 65536, 'num_predict': 100})
        self.assertEqual(job['source'], 'pilot-mainpc')
        row = self.broker.db.execute('select * from jobs where id=?', (job['id'],)).fetchone()
        self.assertEqual(self.broker._lane_key(row), ('pilot-mainpc', 'gemma4:26b'))
        self.assertEqual(json.loads(row['payload'])['options']['num_ctx'], 65536)

    def test_nightly_custom_model_and_context_keep_source_and_evidence(self):
        from test_nightly_eval import NativeFake, BOOL, P
        gpu = NativeFake()
        self.broker.ollama = gpu
        model = PROFILES[P].model
        other = next(m for m in nightly_eval.MODEL_DIGESTS if m != model)
        gpu.context = 65536
        job = self.broker.submit(P, 'chat', {'messages': [{'role': 'user', 'content': 'test'}],
            'think': False, 'native_thinking': BOOL}, source=nightly_eval.SOURCE,
            external_id='custom', source_item_id='cell', model=other,
            options={'num_ctx': 65536, 'num_predict': 100})
        self.assertEqual(job['source'], nightly_eval.SOURCE)
        self.assertTrue(self.broker.dispatch_once(None, self.policy))
        final = self.broker.status(job['id'])
        self.assertEqual(final['state'], 'completed', final.get('error'))
        self.assertEqual(final['result']['model'], other)
        self.assertEqual(final['execution_evidence']['runtime_context_length'], 65536)

    def test_top_level_precedence(self):
        job = self.broker.submit('interactive', 'generate', {'model': 'nemotron3:33b',
            'options': {'num_ctx': 8192, 'num_predict': 40}}, model='gemma4:12b', options={'num_ctx': 65536})
        row = self.broker.db.execute('select * from jobs where id=?', (job['id'],)).fetchone()
        payload = json.loads(row['payload'])
        self.assertEqual(payload['model'], 'gemma4:12b')
        self.assertEqual(payload['options'], {'num_ctx': 65536, 'num_predict': 40})

    def test_unknown_model_metadata_and_no_guessing(self):
        self.gpu.show = lambda model, **kwargs: {'model_info': {'test.context_length': 65536}}
        job = self.broker.submit('interactive', 'generate', {}, model='new:model', options={'num_ctx': 60000})
        self.assertEqual(job['payload']['model'], 'new:model')
        with self.assertRaisesRegex(ValueError, 'num_ctx=65537.*65536.*new:model'):
            self.broker.submit('interactive', 'generate', {}, model='new:model', options={'num_ctx': 65537})

    def test_source_registration_serializes_independent_writers(self):
        import threading
        errors = []
        def register(name):
            try:
                SourcePolicy(self.path).register(name)
            except Exception as exc:
                errors.append(exc)
        threads = [threading.Thread(target=register, args=(f'source-{i}',)) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(json.loads(self.path.read_text())['sources']), 5)

    def test_new_source_can_use_existing_chat_profile(self):
        job = self.broker.submit('openclaw-gemma4', 'chat',
            {'messages': [{'role': 'user', 'content': 'test'}]}, source='new-chat', model='gemma4:26b')
        self.assertEqual(job['source'], 'new-chat')
        self.assertEqual(job['payload']['model'], 'gemma4:26b')
