"""Synthetic contracts only: no model calls, WOL, remote policy or real chats."""
import copy
import contextlib
import io
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

from broker import nightly_eval as lane
from broker import nightly_client as client
from broker.nightly_client import (Closed, Uncertain, Transport, Journal, freeze_plan,
    load_frozen, new_json, request_for, admit, collect, main, accepted_cases)
from broker.profiles import PROFILES, NIGHTLY_PROFILE_NAMES
from broker.service import Broker, SourcePolicy, SourceAdmissionBlocked, SourceQueueFull
from broker.http import serve

BOOL = dict(values=[False, True], default=True)
P = 'telegram-nightly-qwen38-v1'
SECRET = 'SYNTHETIC_RAW_THINKING_DO_NOT_PERSIST'


class FakeWol:
    def wake(self):
        pass


class NativeFake:
    def __init__(self):
        self.model = PROFILES[P].model
        self.metadata = copy.deepcopy(BOOL)
        self.requests = []
        self.context = 32768
        self.reason = 'stop'
        self.final = 'Final synthetic answer'
        self.bad_digest = False
        self.error = None

    def tags(self, **kwargs):
        return dict(models=[dict(name=k, digest=v if not self.bad_digest else '0'*64)
                            for k, v in lane.MODEL_DIGESTS.items()])

    def show(self, model, **kwargs):
        self.model = model
        return dict(thinking=copy.deepcopy(self.metadata))

    def ps(self, **kwargs):
        return dict(models=[dict(name=self.model, digest=lane.MODEL_DIGESTS[self.model],
                                context_length=self.context, size_vram=123)])

    def ensure_model_ready(self, model, **kwargs):
        self.model = model
        return True

    def run(self, kind, request):
        self.requests.append((kind, copy.deepcopy(request)))
        if self.error:
            raise RuntimeError(SECRET)
        return dict(model=request['model'], done=True, done_reason=self.reason,
                    message=dict(role='assistant', content=self.final, thinking=SECRET, reasoning=SECRET),
                    thinking=SECRET, ignored=dict(reasoning=SECRET),
                    total_duration=1_000_000_000, load_duration=200_000_000,
                    prompt_eval_count=1234, prompt_eval_duration=300_000_000,
                    eval_count=100, eval_duration=500_000_000,
                    final_tokens={'malformed': SECRET})


class NightlyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.policy_path = self.root/'policy.json'
        self.policy_path.write_text(json.dumps(dict(sources={
            lane.SOURCE: dict(enabled=True, admission_allowed=True, weight=1),
            'pilot-mainpc': dict(enabled=True, admission_allowed=True, weight=1),
            'nextcloud': dict(enabled=True, admission_allowed=True, weight=7),
            'syncopia-telegram-memory': dict(enabled=True, admission_allowed=True, weight=4)})))
        self.policy = SourcePolicy(self.policy_path)
        self.gpu = NativeFake()
        self.broker = Broker(self.root/'broker.sqlite3', self.gpu, FakeWol(), source_policy=self.policy)
        self.addCleanup(self.broker.db.close)

    def payload(self, **changes):
        return dict(messages=[dict(role='user', content='Synthetic contract test, not an Astra case')],
                    think=False, native_thinking=copy.deepcopy(BOOL), options=dict(lane.OPTIONS), **changes)

    def submit(self, key='one', profile=P, payload=None, **changes):
        return self.broker.submit(**(dict(profile=profile, kind='chat', source=lane.SOURCE,
            source_item_id='synthetic', external_id=key, payload=self.payload() if payload is None else payload) | changes))

    def run_job(self):
        self.assertTrue(self.broker.dispatch_once(frozenset({lane.SOURCE}), self.policy))

    def client(self):
        server = serve(self.broker, port=0, policy=self.policy)
        server.daemon_threads = False
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return Transport('http://127.0.0.1:' + str(server.server_port))

    def frozen(self, metadata=None, modes=None):
        metadata = metadata or BOOL
        modes = modes or [dict(label='off', think=False, supported=lane.supports(metadata, False)),
                          dict(label='on', think=True, supported=lane.supports(metadata, True))]
        cases = []
        for i in range(4):
            case = 'case-synthetic-'+str(i)
            path = self.root/case/'messages.json'
            path.parent.mkdir()
            path.write_text(json.dumps(dict(messages=[dict(role='user', content='synthetic distinct case '+str(i))])))
            cases.append(dict(case_id=case, runner_input=case+'/messages.json',
                              runner_input_sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
        (self.root/'cases.json').write_text(json.dumps(dict(schema=client.ASTRA_SCHEMA, case_count=4, cases=cases)))
        # Synthetic trust pins exist only inside this test process, never in CLI/env.
        for name, value in (
            ('ASTRA_MANIFEST_SHA256', hashlib.sha256((self.root/'cases.json').read_bytes()).hexdigest()),
            ('CASE_INPUTS', {c['case_id']: c['runner_input_sha256'] for c in cases}),
        ):
            patcher = patch.object(client, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        (self.root/'matrix.json').write_text(json.dumps(dict(schema='telegram-nightly-matrix-v1', source=lane.SOURCE,
            models=[dict(profile=p, modes=copy.deepcopy(modes)) for p in sorted(NIGHTLY_PROFILE_NAMES)])))
        (self.root/'native.json').write_text(json.dumps(dict(schema='telegram-nightly-native-v1', models=[
            dict(model=m, digest=d, thinking=metadata) for m, d in lane.MODEL_DIGESTS.items()])))
        plan = freeze_plan(self.root/'cases.json', self.root/'matrix.json', self.root/'native.json')
        new_json(self.root/'frozen.json', plan)
        return load_frozen(self.root/'frozen.json')

    def journal(self, digest, transport):
        journal = Journal(self.root/'private'/'journal.sqlite3', digest, transport.base)
        self.addCleanup(journal.close)
        return journal

    def test_all_four_profiles_dispatch_exact_limits_without_persisting_reasoning(self):
        before = self.policy_path.read_bytes()
        for profile in sorted(NIGHTLY_PROFILE_NAMES):
            job = self.submit(profile, profile=profile)
            self.assertNotIn('payload', job)
            self.run_job()
            result = self.broker.status(job['id'])
            req = self.gpu.requests[-1][1]
            self.assertEqual((req['model'], req['options'], req['think']),
                             (PROFILES[profile].model, lane.OPTIONS, False))
            self.assertNotIn('native_thinking', req)
            self.assertEqual((req['stream'], req['truncate'], req['shift']), (False, False, False))
            self.assertEqual((req['_broker_timeout_seconds'], req['keep_alive']), (1800, '300s'))
            self.assertEqual(result['execution_evidence']['runtime_context_length'], 32768)
            self.assertIsNone(result['execution_evidence']['effective_output_limit'])
            self.assertEqual(result['result']['eval_count'], 100)
            self.assertIsNone(result['result']['final_tokens'])
            self.assertTrue(result['result']['acceptable'])
            self.assertNotIn(SECRET, json.dumps(result))
        self.assertNotIn(SECRET, '\n'.join(self.broker.db.iterdump()))
        self.assertEqual(before, self.policy_path.read_bytes())

    def test_all_31_preexisting_profiles_are_unchanged(self):
        old = {k: asdict(v) for k, v in PROFILES.items() if k not in NIGHTLY_PROFILE_NAMES}
        self.assertEqual(len(old), 31)
        self.assertEqual(hashlib.sha256(json.dumps(old, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                         'ddc9184d744685ed87969710c82a87cb53e7c7889780556b89b5e92824b611e2')
        self.assertEqual({p.model for k, p in PROFILES.items() if k in NIGHTLY_PROFILE_NAMES}, set(lane.MODEL_DIGESTS))
        for p in (PROFILES[k] for k in NIGHTLY_PROFILE_NAMES):
            self.assertEqual((p.max_context, p.max_output, p.max_images, p.max_schema_bytes, p.request_timeout_seconds),
                             (32768, 8192, 0, 0, 1800))

    def test_source_isolation_and_rejections_before_persistence(self):
        for profile in NIGHTLY_PROFILE_NAMES:
            for changes in (dict(source='pilot-mainpc'), dict(source='openclaw'), dict(source='nextcloud'),
                            dict(source='syncopia-telegram-memory'), dict(source=None),
                            dict(kind='generate'), dict(producer_storage={}), dict(external_id=None),
                            dict(source_item_id=None)):
                with self.subTest(profile=profile, changes=changes), self.assertRaises(ValueError):
                    self.submit(profile=profile, **changes)
        for profile in set(PROFILES) - NIGHTLY_PROFILE_NAMES:
            with self.assertRaises(ValueError):
                self.submit(profile=profile)
        for changes in (dict(images=[]), dict(format='json'), dict(tools=[]), dict(keep_alive=0),
                        dict(model='uninstalled'), dict(think=0), dict(think=None), dict(stream=True),
                        dict(truncate=True), dict(shift=True), dict(options=dict(lane.OPTIONS, num_ctx=65536)),
                        dict(options=dict(lane.OPTIONS, num_predict=8192.0)),
                        dict(messages=[dict(role='assistant', content='history')]),
                        dict(messages=[dict(role='user', content='x', thinking=SECRET)])):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.submit(payload=self.payload() | changes)
        self.assertEqual(self.broker.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 0)

    def test_source_requires_explicit_registered_policy_and_replay_before_gates(self):
        job = self.submit()
        self.policy.set_admission_allowed(lane.SOURCE, False)
        self.assertEqual(self.submit()['id'], job['id'])
        with self.assertRaises(SourceAdmissionBlocked):
            self.submit('new')
        self.broker.source_policy = None
        with self.assertRaises(SourceAdmissionBlocked):
            self.submit('absent-policy')

    def test_idempotent_hash_item_profile_checks_restart_and_no_retries(self):
        job = self.submit()
        for changes in (dict(source_item_id='changed'), dict(profile='telegram-nightly-gemma12-v1'),
                        dict(payload=self.payload() | dict(think=True)),
                        dict(payload=self.payload() | dict(messages=[dict(role='user', content='changed')]))):
            with self.assertRaises(ValueError):
                self.submit(**changes)
        other = Broker(self.root/'broker.sqlite3', self.gpu, FakeWol(), source_policy=self.policy)
        try:
            self.assertEqual(other.submit(P, 'chat', self.payload(), lane.SOURCE, 'synthetic', 'one')['id'], job['id'])
        finally:
            other.db.close()
        self.broker.cancel(job['id'])
        with self.assertRaisesRegex(ValueError, 'never retries'):
            self.broker.retry(job['id'])
        with self.assertRaisesRegex(ValueError, 'never retries'):
            self.broker.bulk_retry_failed(lane.SOURCE)

    def test_lifetime_32_and_single_outstanding_real_admissions(self):
        for i in range(32):
            job = self.submit(str(i))
            with self.assertRaises(SourceQueueFull):
                self.submit('parallel')
            self.run_job()
        with self.assertRaises(SourceQueueFull):
            self.submit('33')
        self.assertEqual(self.submit('31')['id'], job['id'])
        self.assertEqual(len(self.gpu.requests), 32)
        # Tombstones remain in lifetime accounting, and never recreate a key.
        with self.broker.db:
            self.broker.db.execute("INSERT INTO job_retention_tombstones(job_id,source,external_id,profile,kind,final_state,input_hash,created,purged_at) SELECT id,source,external_id,profile,kind,state,input_hash,created,0 FROM jobs WHERE external_id='0'")
            self.broker.db.execute("DELETE FROM jobs WHERE external_id='0'")
        with self.assertRaises(SourceQueueFull):
            self.submit('34')
        with self.assertRaisesRegex(ValueError, 'retention expired'):
            self.submit('0')

    def test_native_always_thinking_levels_are_not_off_and_drift_fails_before_inference(self):
        self.gpu.metadata = dict(values=['low', 'high'], default='high')
        with self.assertRaisesRegex(ValueError, 'unsupported'):
            self.submit(payload=self.payload() | dict(native_thinking=self.gpu.metadata))
        payload = self.payload() | dict(native_thinking=self.gpu.metadata, think='low')
        job = self.submit(payload=payload)
        self.run_job()
        self.assertEqual(self.gpu.requests[-1][1]['think'], 'low')
        self.assertEqual(self.broker.status(job['id'])['execution_evidence']['native_thinking']['values'], ['low', 'high'])
        drift = self.submit('drift', payload=payload)
        self.gpu.metadata = dict(values=['low', 'medium', 'high'], default='high')
        self.run_job()
        self.assertEqual(len(self.gpu.requests), 1)
        self.assertEqual(self.broker.status(drift['id'])['state'], 'failed')
        self.assertEqual(self.broker.status(job['id'])['state'], 'completed')

    def test_native_missing_metadata_wrong_artifact_and_clamped_context_fail_closed(self):
        for mode in ('metadata', 'digest', 'context', 'error'):
            self.gpu = NativeFake()
            self.broker.ollama = self.gpu
            if mode == 'metadata':
                self.gpu.metadata = None
            if mode == 'digest':
                self.gpu.bad_digest = True
            if mode == 'context':
                self.gpu.context = 16384
            if mode == 'error':
                self.gpu.error = True
            job = self.submit(mode)
            self.run_job()
            self.assertEqual(self.broker.status(job['id'])['state'], 'failed')
            self.assertNotIn(SECRET, '\n'.join(self.broker.db.iterdump()))
            self.assertEqual(len(self.gpu.requests), 1 if mode in ('context', 'error') else 0)

    def test_length_empty_unknown_and_inline_thinking_rejected_for_quality(self):
        for reason, final, state in (('length', 'partial', 'completed'), ('stop', '', 'completed'),
                                     ('unknown', 'text', 'completed'), ('stop', '<think>RAW</think>answer', 'failed')):
            self.gpu.reason, self.gpu.final = reason, final
            job = self.submit(reason+str(len(self.gpu.requests)))
            self.run_job()
            result = self.broker.status(job['id'])
            self.assertEqual(result['state'], state)
            if state == 'completed':
                self.assertFalse(result['result']['acceptable'])
                self.assertEqual(result['result']['truncated'], reason == 'length')
            self.assertNotIn('<think>', '\n'.join(self.broker.db.iterdump()))

    def test_loopback_cli_full_dry_freeze_request_admit_collect(self):
        plan, digest = self.frozen()
        transport = self.client()
        cell = plan['cells'][0]
        journal = self.journal(digest, transport)
        receipt = admit(plan, cell, journal, transport)
        self.assertIsNotNone(journal.row(cell)['job_id'])
        self.assertEqual(receipt['expected_input_hash'], cell['input_hash'])
        with self.assertRaises(Closed):
            admit(plan, cell, journal, transport)
        with self.assertRaises(Closed):
            admit(plan, plan['cells'][1], journal, transport)
        self.run_job()
        result = collect(plan, cell, journal, transport)
        self.assertEqual(result['id'], receipt['job_id'])
        self.assertEqual(result['state'], 'completed')
        self.assertNotIn(SECRET, '\n'.join(journal.db.iterdump()))
        self.assertEqual(journal.row(cell)['phase'], 'terminal')
        # Exercise argparse/main with test-only trust pins; the unmodified child
        # process must reject these synthetic fixtures under the real Astra pin.
        for command in ('dry-run', 'freeze'):
            args = [command, '--cases', str(self.root/'cases.json'), '--matrix', str(self.root/'matrix.json'),
                    '--native', str(self.root/'native.json')]
            if command == 'freeze':
                args += ['--out', str(self.root/'cli-frozen.json')]
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.assertEqual(main(args), 0)
            self.assertIn('"planned_cells": 32', stdout.getvalue())
            self.assertNotIn('synthetic distinct', stdout.getvalue())
        proc = subprocess.run([sys.executable, '-m', 'broker.nightly_client', 'dry-run',
            '--cases', str(self.root/'cases.json'), '--matrix', str(self.root/'matrix.json'),
            '--native', str(self.root/'native.json')], capture_output=True, text=True, timeout=10)
        self.assertEqual(proc.returncode, 2)
        self.assertNotIn('synthetic distinct', proc.stdout+proc.stderr)
        self.assertEqual(main(['request', '--frozen', str(self.root/'frozen.json'), '--cell', cell['cell_id'],
                               '--out', str(self.root/'request.json')]), 0)
        self.assertEqual(json.loads((self.root/'request.json').read_text()), request_for(plan, cell))
        self.gpu.reason = 'length'
        receipt2 = admit(plan, plan['cells'][1], journal, transport)
        self.run_job()
        self.assertEqual(main(['collect', '--frozen', str(self.root/'frozen.json'), '--cell', plan['cells'][1]['cell_id'],
            '--journal', str(self.root/'private'/'journal.sqlite3'), '--base-url', transport.base,
            '--out', str(self.root/'length-final.json')]), 3)
        exported = json.loads((self.root/'length-final.json').read_text())
        self.assertEqual(exported['result']['done_reason'], 'length')
        self.assertNotIn(SECRET, json.dumps(exported))

    def test_lost_ack_then_restart_get_only_reconcile_and_empty_search_never_reposts(self):
        plan, digest = self.frozen()
        cell = plan['cells'][0]
        transport = self.client()
        journal = self.journal(digest, transport)
        original = transport.request
        calls = []
        def lose_ack(path, body=None):
            calls.append(('POST' if body else 'GET', path))
            result = original(path, body)
            if body is not None:
                raise Uncertain('synthetic lost acknowledgement')
            return result
        with patch.object(transport, 'request', side_effect=lose_ack):
            with self.assertRaises(Uncertain):
                admit(plan, cell, journal, transport)
            self.assertEqual(journal.row(cell)['phase'], 'intent')
            again = Journal(self.root/'private'/'journal.sqlite3', digest, transport.base)
            try:
                with self.assertRaises(Closed):
                    admit(plan, cell, again, transport)
                got = collect(plan, cell, again, transport, reconcile=True)
                self.assertEqual(got['state'], 'queued')
            finally:
                again.close()
        self.assertEqual(sum(method == 'POST' for method, _ in calls), 1)
        self.assertEqual(self.broker.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 1)
        self.broker.cancel(got['id'])
        collect(plan, cell, journal, transport)
        other = plan['cells'][1]
        journal.intent(other)  # Crash BEFORE network: still cannot prove nonadmission.
        with patch.object(transport, 'request', return_value={'jobs': []}) as request:
            with self.assertRaises(Uncertain):
                collect(plan, other, journal, transport, reconcile=True)
            self.assertEqual(request.call_args.args[0].split('?')[0], '/v1/correlations')
        with self.assertRaises(Closed):
            admit(plan, other, journal, transport)

    def test_uuid_receipt_is_durable_even_when_identity_or_optional_projection_invalid(self):
        plan, digest = self.frozen()
        transport = self.client()
        journal = self.journal(digest, transport)
        cell = plan['cells'][0]
        original = transport.request
        def corrupt(path, body=None):
            raw = original(path, body)
            if body is not None:
                raw['projection'] = 'wrong-schema'
                raw['message'] = {'thinking': SECRET}
            return raw
        with patch.object(transport, 'request', side_effect=corrupt):
            with self.assertRaises(Uncertain):
                admit(plan, cell, journal, transport)
        self.assertIsNotNone(journal.row(cell)['job_id'])
        self.assertEqual(json.loads(journal.row(cell)['receipt'])['observed_input_hash'], cell['input_hash'])
        self.assertNotIn(SECRET, '\n'.join(journal.db.iterdump()))
        self.assertEqual(collect(plan, cell, journal, transport)['state'], 'queued')

    def test_frozen_changes_cross_broker_and_private_journal_rejected(self):
        plan, digest = self.frozen()
        transport = self.client()
        self.journal(digest, transport)
        with self.assertRaises(Closed):
            Journal(self.root/'private'/'journal.sqlite3', 'changed', transport.base)
        with self.assertRaises(Closed):
            Journal(self.root/'private'/'journal.sqlite3', digest, 'http://localhost:3333')
        (self.root/'case-synthetic-0'/'messages.json').write_text('[]')
        with self.assertRaises(Closed):
            load_frozen(self.root/'frozen.json')
        self.root.chmod(0o755)
        with self.assertRaises(Closed):
            Journal(self.root/'public.sqlite3', digest, transport.base)

    def test_named_level_plan_exposes_unsupported_off_without_coercion(self):
        metadata = dict(values=['low', 'medium', 'high', 'max'], default='high')
        plan, _ = self.frozen(metadata, [dict(label='off', think=False, supported=False),
                                       dict(label='level-low', think='low', supported=True)])
        self.assertEqual(sum(c['supported'] for c in plan['cells']), 16)
        off = next(c for c in plan['cells'] if c['mode'] == 'off')
        transport = self.client()
        journal = self.journal('test-bound', transport)
        with patch.object(transport, 'request') as request:
            with self.assertRaisesRegex(Closed, 'unsupported'):
                admit(plan, off, journal, transport)
            request.assert_not_called()
        self.assertIsNone(journal.row(off))
        raw = json.loads((self.root/'matrix.json').read_text())
        raw['models'][0]['modes'][1]['label'] = 'off'
        (self.root/'matrix.json').write_text(json.dumps(raw))
        with self.assertRaises(Closed):
            freeze_plan(self.root/'cases.json', self.root/'matrix.json', self.root/'native.json')

    def test_exact_fixture_contract_never_opens_references_or_follows_literal_urls(self):
        plan, _ = self.frozen()
        original = client.read_json
        reads = []
        def bounded_read(path):
            path = Path(path).resolve()
            reads.append(path)
            return original(path)
        with patch.object(client, 'read_json', side_effect=bounded_read), patch('socket.socket') as network:
            cases, _ = accepted_cases(self.root/'cases.json')
            network.assert_not_called()
        self.assertEqual(set(reads), {self.root/'cases.json'} | {
            self.root/case['case_id']/'messages.json' for case in cases})
        self.assertEqual(len(cases), 4)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout), patch('socket.socket') as network:
            self.assertEqual(main(['verify-cases', '--cases', str(self.root/'cases.json')]), 0)
            network.assert_not_called()
        self.assertEqual(json.loads(stdout.getvalue())['case_count'], 4)
        self.assertNotIn('synthetic distinct', stdout.getvalue())
        # Path redirection is rejected independently of the accepted byte hash.
        original_bytes = (self.root/'cases.json').read_bytes()
        manifest = original(self.root/'cases.json')[0]
        manifest['cases'][0]['runner_input'] = 'references/private.json'
        (self.root/'cases.json').write_text(json.dumps(manifest))
        with patch.object(client, 'ASTRA_MANIFEST_SHA256', hashlib.sha256((self.root/'cases.json').read_bytes()).hexdigest()):
            with self.assertRaisesRegex(Closed, 'model-facing'):
                accepted_cases(self.root/'cases.json')
        (self.root/'cases.json').write_bytes(original_bytes)
        # A forged frozen path cannot substitute another same-byte JSON file.
        path = self.root/'forged-messages.json'
        path.write_bytes((self.root/'case-synthetic-0'/'messages.json').read_bytes())
        plan['cases'][0]['messages_path'] = str(path)
        new_json(self.root/'forged.json', plan)
        with self.assertRaises(Closed):
            load_frozen(self.root/'forged.json')
        source = self.root/'case-synthetic-0'/'messages.json'
        source.unlink()
        source.symlink_to(path)
        with self.assertRaisesRegex(Closed, 'fixture root'):
            accepted_cases(self.root/'cases.json')

    def test_frozen_missing_model_cells_and_matrix_expansion_rejected(self):
        plan, _ = self.frozen()
        plan['cells'][0]['profile'] = plan['cells'][-1]['profile']
        new_json(self.root/'missing-model.json', plan)
        with self.assertRaisesRegex(Closed, '4x2x4'):
            load_frozen(self.root/'missing-model.json')
        plan['cells'].append(copy.deepcopy(plan['cells'][0]))
        new_json(self.root/'expanded.json', plan)
        with self.assertRaises(Closed):
            load_frozen(self.root/'expanded.json')

    def test_text_only_literal_urls_stay_text_without_network(self):
        url_text = 'Literal only https://example.invalid/document.pdf image.jpg audio.ogg'
        with patch('socket.socket') as network:
            normalized = lane.normalize_request(self.payload() | dict(messages=[dict(role='user', content=url_text)]), P)
            network.assert_not_called()
        self.assertEqual(normalized['messages'][0]['content'], url_text)
        for message in (dict(role='user', content=[dict(type='image_url', image_url='x')]),
                        dict(role='user', content='caption', images=['x']),
                        dict(role='user', content='caption', audio='x')):
            with self.assertRaises(ValueError):
                lane.normalize_request(self.payload() | dict(messages=[message]), P)

    def test_capture_native_filters_metadata_and_never_uses_inference_endpoints(self):
        import importlib.util
        path = Path(__file__).resolve().parents[1]/'scripts/telegram_nightly_capture_native.py'
        spec = importlib.util.spec_from_file_location('capture_native_test', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        calls = []
        def metadata(path, body=None):
            calls.append((path, body))
            if path == '/api/tags':
                return dict(models=[dict(name=m, digest=d) for m, d in lane.MODEL_DIGESTS.items()])
            if path == '/api/version':
                return dict(version='0.40.11')
            self.assertEqual(path, '/api/show')
            self.assertEqual(set(body), {'model'})
            return dict(thinking=BOOL, template=SECRET, modelfile=SECRET)
        result = module.capture(metadata)
        self.assertEqual(len(result['models']), 4)
        self.assertEqual(sum(body is not None for _, body in calls), 4)
        self.assertNotIn(SECRET, json.dumps(result))
        self.assertEqual({p for p, _ in calls}, {'/api/tags', '/api/version', '/api/show'})

    def test_transport_has_no_redirect_retry_or_secret_error_body(self):
        from urllib.error import HTTPError
        transport = Transport('http://127.0.0.1:1', token='SYNTHETIC_AUTH')
        with patch.object(transport.opener, 'open', side_effect=HTTPError('url', 503, SECRET, {}, None)) as send:
            with self.assertRaises(Uncertain) as caught:
                transport.request('/v1/jobs', {})
            self.assertNotIn(SECRET, str(caught.exception))
            self.assertEqual(send.call_count, 1)
        for base in ('https://example.org', 'http://name:password@localhost', 'http://localhost/redirect'):
            with self.assertRaises(Closed):
                Transport(base)


if __name__ == '__main__':
    unittest.main()
