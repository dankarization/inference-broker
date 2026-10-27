"""Opt-in Qwen64: synthetic executor only; no live model, no old-route mutation."""
import copy
import json
import unittest
import test_nextcloud as fixtures
from test_worker004_compat import Worker004Tests
from broker.service import Broker, SourceQueueFull
from broker.profiles import PROFILES
from broker.qwen_budget import PROFILE, CONTRACT as C, project


class Qwen64Tests(Worker004Tests):
    def payload64(self):
        return self.payload | dict(think=True, options=dict(C['options']), stream=False)

    def admit64(self, key='qwen64', payload=None, **changes):
        return self.submit(key, self.payload64() if payload is None else payload, profile=PROFILE, **changes)

    def runtime(self):
        self.resident = dict(name=C['model'], digest=C['model_digest'].removeprefix('sha256:'),
                            size=19756975387, size_vram=9987361340, context_length=16384,
                            details=dict(quantization_level='Q4_K_M'))
        self.gpu.ps = lambda **kw: dict(models=[copy.deepcopy(self.resident)])
        def run(kind, request):
            self.gpu.requests.append((kind,request))
            self.resident['context_length']=65536
            self.resident['size_vram']=11000000000
            return dict(model=request['model'],response='strict valid fixture',done=True,
                        done_reason='stop',eval_count=12,thinking='PRIVATE_REASONING')
        self.gpu.run=run

    def test_qwen64_exact_contract_rejects_wrong_settings_and_old_routes_unchanged(self):
        job=self.admit64()
        self.assertEqual(self.broker.nextcloud_projection(job)['request_contract'],C)
        self.assertRegex(self.broker.nextcloud_projection(job)['input_hash'],r'^sha256:[a-f0-9]{64}$')
        self.assertEqual(PROFILES['nextcloud-eval-qwen'].max_context,16384)
        self.assertEqual(PROFILES['nextcloud-eval-qwen'].max_output,8192)
        self.assertEqual(PROFILES['eval-qwen3-vl-30b'].max_output,8192)
        for fields in ({'think':False},{'think':1},{'model':'qwen3-vl:2b'},{'stream':True},
                       {'options':dict(C['options'],num_predict=32768)},
                       {'options':dict(C['options'],num_ctx=65536.0)},
                       {'options':dict(C['options'],temperature=False)}):
            with self.subTest(fields=fields),self.assertRaises(ValueError):
                self.admit64('bad',self.payload64()|fields)
        for changes in ({'source':'pilot-mainpc'},{'kind':'chat'},{'external_id':None},
                        {'source_item_id':None}):
            with self.assertRaises(ValueError):self.admit64('bad',**changes)

    def test_qwen64_dispatch_runtime_identity_lease_and_restart_replay(self):
        self.runtime();job=self.admit64()
        # Execution path is synchronous, so inspect the durable claim during run.
        run=self.gpu.run
        def tracked(kind,request):
            row=self.broker.db.execute('SELECT started,lease_until FROM jobs WHERE id=?',(job['id'],)).fetchone()
            self.assertGreaterEqual(row['lease_until']-row['started'],2115)
            return run(kind,request)
        self.gpu.run=tracked
        self.assertTrue(self.broker.dispatch_once(policy=self.policy))
        req=self.gpu.requests[-1][1]
        self.assertEqual(req['options'],C['options']);self.assertIs(req['think'],True)
        self.assertEqual(req['_broker_timeout_seconds'],1800)
        projected=self.broker.nextcloud_projection(self.broker.status(job['id']))
        self.assertEqual(projected['state'],'completed')
        self.assertEqual(projected['execution_evidence']['after']['context_length'],65536)
        self.assertNotIn('PRIVATE',json.dumps(projected))
        other=Broker(self.root/'test.sqlite3',self.gpu,fixtures.FakeWOL(),source_policy=self.policy)
        try:
            replay=other.submit(PROFILE,'generate',self.payload64(),'nextcloud','3','qwen64')
            self.assertEqual(replay['id'],job['id'])
            self.assertEqual(other.db.execute('SELECT count(*) FROM jobs').fetchone()[0],1)
        finally:other.db.close()

    def test_qwen64_identity_or_clamped_runtime_fails_closed(self):
        for mode in ('digest','quantization','context'):
            with self.subTest(mode=mode):
                self.runtime();job=self.admit64(mode)
                if mode=='digest':self.resident['digest']='0'*64
                elif mode=='quantization':self.resident['details']['quantization_level']='Q8'
                else:
                    run=self.gpu.run
                    def clamped(kind,request):
                        result=run(kind,request);self.resident['context_length']=16384;return result
                    self.gpu.run=clamped
                self.assertTrue(self.broker.dispatch_once(policy=self.policy))
                final=self.broker.status(job['id'])
                self.assertEqual(final['state'],'failed')
                if mode!='context':self.assertEqual(len(self.gpu.requests),0)

    def test_qwen64_shared_byte_cap_and_replay_at_cap(self):
        job=self.admit64()
        self.broker.db.execute('UPDATE jobs SET input_bytes=? WHERE id=?',(256*1024**2,job['id']))
        self.broker.db.commit()
        with self.assertRaises(SourceQueueFull):self.admit('overflow-old')
        with self.assertRaises(SourceQueueFull):self.admit64('overflow-new')
        self.assertEqual(self.admit64()['id'],job['id'])

    def test_qwen64_projection_rebuilds_metadata_and_rejects_malformed(self):
        self.runtime();job=self.admit64();self.broker.dispatch_once(policy=self.policy)
        final=self.broker.status(job['id'])
        raw=final['result']['_broker_execution_evidence']
        raw['after']['error']={'thinking':'PRIVATE'}
        raw['after']['details']['thinking']='PRIVATE'
        safe=project(raw)
        self.assertNotIn('PRIVATE',json.dumps(safe))
        for change in ({'sampled_at':float('nan')},{'model_digest':'sha256:'+'0'*64},
                       {'options':dict(C['options'],num_ctx=16384)}):
            self.assertIsNone(project(raw|change))

def load_tests(loader, tests, pattern):
    return unittest.TestSuite(Qwen64Tests(n) for n in loader.getTestCaseNames(Qwen64Tests)
                              if n.startswith('test_qwen64_'))

if __name__=='__main__':unittest.main()
