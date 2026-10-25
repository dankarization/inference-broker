"""Opt-in Gemma26 route only; synthetic GPU/SQLite, no live network."""
import copy,json
import test_nextcloud as f
from broker.compat import GEMMA26_CONTRACT as C,validate_nextcloud_payload
from broker.profiles import PROFILES,OPENCLAW_PROFILE_NAMES,EVALUATION_PROFILE_NAMES
from broker.service import Broker,SourceQueueFull
from test_worker004_compat import Worker004Tests


class Gemma26Tests(Worker004Tests):
    def payload_gemma(self):return self.payload|dict(think=False,options=dict(C['options']))
    def gemma(self,key='verify-1',payload=None,**kw):
        return self.submit(key,payload or self.payload_gemma(),profile='nextcloud-verify-gemma26',**kw)
    def test_gemma_contract_bounds_and_strict_rejections(self):
        job=self.gemma()
        self.assertEqual(self.broker.nextcloud_projection(job)['request_contract'],C)
        self.assertEqual(validate_nextcloud_payload(job['payload'],'nextcloud-verify-gemma26'),job['payload'])
        self.assertNotIn('nextcloud-verify-gemma26',OPENCLAW_PROFILE_NAMES|EVALUATION_PROFILE_NAMES)
        for fields in ({'think':True},{'think':0},{'think':None},{'model':'gemma4:12b'},
                       {'model_digest':C['model_digest']},{'prompt':'x'*16385},{'images':[]},
                       {'images':self.payload['images']*5},{'stream':True},{'tools':[]},
                       {'options':C['options']|dict(num_predict=8192)},
                       {'options':C['options']|dict(num_ctx=16384.0)},
                       {'options':C['options']|dict(temperature=False)}):
            with self.subTest(fields=fields),self.assertRaises(ValueError):
                self.gemma('bad',self.payload_gemma()|fields)
        for changes in ({'source':'openclaw'},{'source':'pilot-mainpc'},{'kind':'chat'},
                        {'external_id':None},{'source_item_id':None}):
            with self.assertRaises(ValueError):self.gemma('bad',**changes)
        payload=self.payload_gemma();payload.pop('think')
        with self.assertRaises(ValueError):self.gemma('missing-think',payload)
    def test_gemma_dispatch_restart_replay_and_shared_capacity(self):
        job=self.gemma();self.assertEqual(self.gemma()['id'],job['id'])
        other=Broker(self.root/'test.sqlite3',self.gpu,f.FakeWOL(),source_policy=self.policy)
        try:
            self.assertEqual(other.submit('nextcloud-verify-gemma26','generate',self.payload_gemma(),
                'nextcloud','3','verify-1')['id'],job['id'])
        finally:other.db.close()
        self.gpu.ps=lambda:dict(models=[])
        self.gpu.is_ready=lambda model:model=='gemma4:26b'
        self.assertTrue(self.broker.dispatch_once(policy=self.policy))
        request=self.gpu.requests[-1][1]
        self.assertEqual(request['model'],'gemma4:26b');self.assertIs(request['think'],False)
        self.assertEqual(request['options'],C['options']);self.assertEqual(request['_broker_timeout_seconds'],900)
        self.assertEqual(self.gemma()['id'],job['id'])
        with self.assertRaises(ValueError):self.admit('verify-1') # same correlation, different route
        active=self.gemma('budget')
        self.broker.db.execute('UPDATE jobs SET input_bytes=? WHERE id=?',(256*1024**2,active['id']));self.broker.db.commit()
        with self.assertRaises(SourceQueueFull):self.admit('over-qwen')
        with self.assertRaises(SourceQueueFull):self.gemma('over-gemma')
    def test_gemma_projection_final_only(self):
        job=self.gemma();job['result']=dict(model='gemma4:26b',response='{}',done=True,done_reason='stop',
                                           eval_count=10,thinking='PRIVATE_THOUGHTS',error={'message':'PRIVATE_ERROR'})
        projected=self.broker.nextcloud_projection(job)
        self.assertEqual(projected['request_contract'],C)
        self.assertNotIn('PRIVATE',json.dumps(projected));self.assertNotIn('payload',projected)


def load_tests(loader,tests,pattern):
    suite=__import__('unittest').TestSuite()
    for name in loader.getTestCaseNames(Gemma26Tests):
        if name.startswith('test_gemma_'):suite.addTest(Gemma26Tests(name))
    return suite
