"""Synthetic priority admission/promotion API; no owner database or inference."""
import json
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from broker.http import serve
from broker.nextcloud_priority import NextcloudPriority
import test_nextcloud as fixtures


class NextcloudPriorityTests(unittest.TestCase):
    setUp = fixtures.NextcloudTests.setUp
    submit = fixtures.NextcloudTests.submit

    def promotion(self, job):
        return {k: job[k] for k in ('source', 'external_id', 'source_item_id')} | {
            'nextcloud_priority': 'new_upload'}

    def test_priority_defaults_and_invalid_admission_never_inserts(self):
        job = self.submit()
        self.assertEqual(job['nextcloud_priority'], 'historical')
        self.assertEqual(self.submit('upload', nextcloud_priority='new_upload')['nextcloud_priority'],
                         'new_upload')
        for value in ('urgent', 1, True, {}, []):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.submit('invalid', nextcloud_priority=value)
        with self.assertRaises(ValueError):
            self.broker.submit('interactive', 'generate', {'prompt': 'synthetic'},
                               source='interactive', nextcloud_priority='new_upload')
        self.assertEqual(self.broker.health()['queue_depth'], 2)

    def test_exact_replay_promotes_without_resetting_identity_or_queue_age(self):
        job = self.submit()
        promoted = self.submit(nextcloud_priority='new_upload')
        self.assertEqual(promoted['nextcloud_priority'], 'new_upload')
        for field in ('id', 'created', 'queued_at', 'attempt_count'):
            self.assertEqual(job[field], promoted[field])
        self.assertEqual(self.submit()['nextcloud_priority'], 'new_upload')
        self.assertEqual(self.broker.promote_nextcloud(job['id'], self.promotion(job))['id'], job['id'])
        count = self.broker.db.execute(
            "SELECT count(*) FROM audit_events WHERE event_type='nextcloud.priority_promoted'").fetchone()[0]
        self.assertEqual(count, 1)
        self.assertEqual(self.broker.health()['queue_depth'], 1)

    def test_promotion_requires_exact_correlation_and_only_changes_queued_rows(self):
        job = self.submit()
        body = self.promotion(job)
        for change in ({'source': 'other'}, {'source_item_id': 'other'},
                       {'external_id': 'other'}, {'source_item_id': None},
                       {'nextcloud_priority': 'historical'}, {'extra': True}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.broker.promote_nextcloud(job['id'], body | change)
        self.assertIsNone(self.broker.promote_nextcloud('missing', body))
        for state in ('running', 'cancel_requested', 'completed', 'failed', 'cancelled'):
            with self.subTest(state=state), self.broker.db:
                self.broker.db.execute('UPDATE jobs SET state=? WHERE id=?', (state, job['id']))
                result = self.broker.promote_nextcloud(job['id'], body)
                self.assertEqual(result['nextcloud_priority'], 'historical')
        self.assertEqual(self.broker.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 1)

    def test_http_capabilities_and_private_promotion_require_owner_token(self):
        job = self.submit()
        with self.broker.db:
            self.broker.db.execute('UPDATE jobs SET producer_attempt_id=? WHERE id=?',
                                   ('synthetic-owner', job['id']))
        token = 'synthetic-owner-token-not-a-secret'
        server = serve(self.broker, port=0, policy=self.policy, storage_token=token)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        def request(path, body=None, bearer=None):
            headers = {'Content-Type': 'application/json'}
            if bearer:
                headers['Authorization'] = 'Bearer ' + bearer
            req = Request(f'http://127.0.0.1:{server.server_port}' + path,
                          data=json.dumps(body).encode() if body is not None else None,
                          headers=headers)
            with urlopen(req, timeout=5) as response:
                return json.load(response)

        self.assertEqual(request('/v1/nextcloud/capabilities'), self.broker.nextcloud_capabilities())
        self.assertFalse(request('/v1/nextcloud/priority')['enabled'])
        endpoint = '/v1/jobs/' + job['id'] + '/nextcloud-priority'
        for bearer in (None, 'wrong-synthetic-token'):
            with self.subTest(bearer=bearer), self.assertRaises(HTTPError) as caught:
                request(endpoint, self.promotion(job), bearer)
            self.assertEqual(caught.exception.code, 401)
            caught.exception.close()
        self.assertEqual(request(endpoint, self.promotion(job), token)['nextcloud_priority'],
                         'new_upload')

    def test_owner_candidates_fail_closed_and_use_effective_minimum_tier(self):
        gate = NextcloudPriority(lambda: self.fail('no owner connection expected'))
        rows = [dict(id='peer', source='peer', queued_at=0, external_id='peer'),
                dict(id='deferred', source='nextcloud', queued_at=1, external_id='deferred'),
                dict(id='primary', source='nextcloud', queued_at=2, external_id='primary')]
        self.assertEqual(gate.candidates(rows), ([rows[0]], None))
        gate.value = dict(jobs={'primary': 1, 'deferred': 3}, floor=3, scope_complete=True)
        self.assertEqual(gate.candidates(rows), ([rows[0], rows[2]], 7))
        gate.value = dict(jobs={'deferred': 3}, floor=3, scope_complete=False)
        self.assertEqual(gate.candidates(rows), ([rows[0]], None))
        gate.value['scope_complete'] = True
        self.assertEqual(gate.candidates(rows), ([rows[0], rows[1]], 10))


class FakeOwner:
    """Context-compatible PostgreSQL stand-in; never opens a connection."""
    def __init__(self, value):
        self.value = value
        self.in_transaction = False
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def transaction(self):
        from contextlib import contextmanager
        @contextmanager
        def hold():
            self.in_transaction = True
            try:
                yield
            finally:
                self.in_transaction = False
        return hold()

    def execute(self, query, *args):
        from types import SimpleNamespace
        if 'pg_roles' in query:
            value = {'unsafe': False}
        else:
            value = {'value': self.value}
        return SimpleNamespace(fetchone=lambda: value)


class OwnerGateContractTests(unittest.TestCase):
    setUp = fixtures.NextcloudTests.setUp
    submit = fixtures.NextcloudTests.submit

    def aggregate(self):
        from broker.nextcloud_priority import SCHEMA
        return dict(schema=SCHEMA, jobs={'version1': 3}, floor=3,
                    scope_complete=True, blockers=0, reasons=[])

    def test_valid_aggregate_keeps_owner_transaction_until_context_exit(self):
        job = self.submit()
        owner = FakeOwner(self.aggregate())
        gate = NextcloudPriority(lambda: owner)
        with gate.hold(self.broker.db):
            self.assertTrue(owner.in_transaction)
            self.assertFalse(owner.closed)
            rows, weight = gate.candidates(self.broker._candidates())
            self.assertEqual(([row['id'] for row in rows], weight), ([job['id']], 10))
        self.assertFalse(owner.in_transaction)
        self.assertTrue(owner.closed)
        self.assertIsNone(gate.value)

    def test_malformed_aggregate_never_publishes_partial_authority(self):
        self.submit()
        invalid = [self.aggregate() | change for change in (
            {'scope_complete': 'true'}, {'scope_complete': 1},
            {'floor': '3'}, {'floor': True},
            {'jobs': {'version1': '3'}}, {'jobs': {'version1': True}},
            {'jobs': {'version1': None}}, {'jobs': {'version1': 0}},
        )]
        for field in ('floor', 'blockers', 'scope_complete', 'reasons'):
            value = self.aggregate()
            value.pop(field)
            invalid.append(value)
        invalid += [None, [], {}, self.aggregate() | {'schema': 'wrong'}]
        for value in invalid:
            with self.subTest(value=value):
                owner = FakeOwner(value)
                gate = NextcloudPriority(lambda: owner)
                with gate.hold(self.broker.db):
                    self.assertIsNone(gate.value)
                    self.assertEqual(gate.candidates(self.broker._candidates()), ([], None))
                    self.assertEqual(gate.last['reason'], 'priority_unavailable')
                self.assertTrue(owner.closed)
                self.assertFalse(owner.in_transaction)

    def test_invalid_owner_does_not_prevent_eligible_peer_dispatch(self):
        job = self.submit()
        value = self.aggregate()
        value.pop('floor')
        self.broker.nextcloud_priority = NextcloudPriority(lambda: FakeOwner(value))
        self.policy.register('peer', weight=7)
        peer = self.broker.submit('interactive', 'generate',
                                  {'model': 'qwen3-vl:30b', 'prompt': 'synthetic peer'},
                                  source='peer')
        self.assertTrue(self.broker.dispatch_once(policy=self.policy))
        self.assertEqual(self.broker.status(job['id'])['state'], 'queued')
        self.assertEqual(self.broker.status(peer['id'])['state'], 'completed')
        self.assertIsNone(self.broker.nextcloud_priority.value)
