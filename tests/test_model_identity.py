"""Generic, payload-free identity across target and observed execution views."""
import json
import tempfile
import unittest
from broker.profiles import PROFILES
from broker.service import Broker


class Ollama:
    def ps(self): return {'models': []}


class ModelIdentityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.b = Broker(self.tmp.name + '/jobs.db', Ollama(), object(), clock=lambda: 1000)
        self.addCleanup(self.b.db.close)

    def insert(self, ident, profile, source, model, state, observed=None):
        with self.b.db:
            self.b.db.execute(
                'INSERT INTO jobs(id,profile,kind,source,payload,result_json,state,created,started,finished) '
                'VALUES(?,?,?,?,?,?,?,?,?,?)',
                (ident, profile, 'generate', source,
                 json.dumps({'model': model, 'prompt': 'PRIVATE'}) if model else '{}',
                 json.dumps({'model': observed, 'response': 'PRIVATE'}) if observed else None,
                 state, 1, 2, 3 if state in ('completed', 'failed', 'cancelled') else None))

    def test_multiple_profiles_sources_and_terminal_states(self):
        for i, (profile, source, model, state, observed) in enumerate([
            ('interactive', 'video-summary-frames', 'deepseek-ocr:latest', 'completed', 'deepseek-ocr:latest'),
            ('interactive', 'other', 'glm-ocr:latest', 'failed', None),
            ('eval-gemma4-12b', 'third', 'hf.co/example/arbitrary:tag', 'cancelled', None),
            ('interactive', 'other', 'requested:tag', 'completed', 'canonical:tag'),
            ('eval-gemma4-12b', 'legacy', None, 'completed', None),
        ]):
            self.insert(str(i), profile, source, model, state, observed)
            row = next(r for r in self.b.terminal_history()['items'] if r['id'] == str(i))
            self.assertEqual(row['requested_model'], model or PROFILES[profile].model)
            self.assertEqual(row['model'], observed or model or PROFILES[profile].model)
            self.assertEqual(row['observed_model'], observed)
            self.assertEqual(row['profile'], profile)
            self.assertEqual(row['source'], source)
        history = self.b.dashboard()['history']
        self.assertNotIn('PRIVATE', json.dumps(history))
        self.assertEqual({r['id']: r['model'] for r in history},
                         {r['id']: r['model'] for r in self.b.terminal_history()['items']})

    def test_forecast_active_and_history_lifecycle(self):
        self.insert('one', 'interactive', 'generic', 'arbitrary:new', 'queued')
        self.assertEqual(self.b.forecast()['next_selections'][0]['model'], 'arbitrary:new')
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET state='running' WHERE id='one'")
        data = self.b.dashboard()
        self.assertEqual(data['active_jobs'][0]['model'], 'arbitrary:new')
        self.assertEqual(data['forecast']['current_model'], 'arbitrary:new')
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET state='completed',finished=4 WHERE id='one'")
        self.assertEqual(self.b.terminal_history()['items'][0]['model'], 'arbitrary:new')

    def test_missing_invalid_and_non_string_historical_identity(self):
        self.insert('old', 'interactive', 'legacy', None, 'failed')
        for payload, result in [('{}', None), ('broken', 'broken'),
                                ('{"model":42}', '{"model":{"private":"PRIVATE"}}')]:
            with self.b.db:
                self.b.db.execute('UPDATE jobs SET payload=?,result_json=? WHERE id=?', (payload, result, 'old'))
            row = self.b.terminal_history()['items'][0]
            self.assertEqual(row['model'], PROFILES['interactive'].model)
            self.assertEqual(row['model_identity_source'], 'profile_default')
            self.assertIsNone(row['observed_model'])
            self.assertNotIn('PRIVATE', json.dumps(row))

    def test_history_identity_extraction_is_page_bounded_and_private(self):
        with self.b.db:
            self.b.db.executemany(
                'INSERT INTO jobs(id,profile,kind,source,payload,state,created,finished,producer_attempt_id) '
                'VALUES(?,?,?,?,?,?,?,?,?)',
                ((str(i), 'interactive', 'generate', 'bulk', '{"model":"generic"}',
                  'completed', i, i, 'hidden' if i % 2 else None) for i in range(400)))
        calls = []
        # Instrument the actual SQL projection, not a simplified EXPLAIN query.
        self.b.db.create_function('json_extract', 2,
            lambda value, path: calls.append(path) or json.loads(value).get('model'))
        rows = self.b._terminal_history(self.b.db, limit=30, include_producer_storage=False)
        self.assertEqual(len(rows), 30)
        self.assertEqual(len(calls), 30)
        self.assertTrue(all(int(row['id']) % 2 == 0 for row in rows))
        self.assertTrue(all(row['model'] == 'generic' for row in rows))
