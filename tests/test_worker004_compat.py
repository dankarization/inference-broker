"""Isolated worker004 broker compatibility; fake GPU, loopback and owned SQLite."""
import json
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, build_opener, ProxyHandler
import test_nextcloud as fixtures
from broker.compat import validate_nextcloud_payload
from broker.profiles import PROFILES, OPENCLAW_PROFILE_NAMES, EVALUATION_PROFILE_NAMES
from broker.service import Broker, SourceQueueFull, SourceAdmissionBlocked
from broker.http import serve


class Worker004Tests(unittest.TestCase):
    setUp = fixtures.NextcloudTests.setUp
    submit = fixtures.NextcloudTests.submit

    def payload_eval(self):
        return self.payload | dict(think=True, options=dict(num_ctx=16384,num_predict=8192,temperature=0))

    def admit(self,key='eval-1',**changes):
        return self.submit(key,self.payload_eval(),profile='nextcloud-eval-qwen',**changes)

    def test_exact_qwen_contract_and_no_other_profiles_or_routes(self):
        job=self.admit()
        self.assertEqual(job['payload'],validate_nextcloud_payload(self.payload_eval(),'nextcloud-eval-qwen'))
        self.assertEqual(PROFILES['nextcloud-eval-qwen'].model,'qwen3-vl:30b')
        self.assertNotIn('nextcloud-eval-qwen',OPENCLAW_PROFILE_NAMES | EVALUATION_PROFILE_NAMES)
        for profile in ('nextcloud-eval-gemma','nextcloud-eval-nemotron'):
            self.assertNotIn(profile,PROFILES)
            with self.assertRaises(ValueError):self.submit(profile, self.payload_eval(),profile=profile)
        for changes in ({'source':'pilot-mainpc'},{'source':'openclaw'},{'kind':'chat'},
                        {'external_id':None},{'source_item_id':None}):
            with self.subTest(changes=changes),self.assertRaises(ValueError):self.admit('bad',**changes)
        for change in ({'think':False},{'think':1},{'think':None},{'model':'wrong'},
                       {'options':dict(num_ctx=16384.0,num_predict=8192,temperature=0)},
                       {'options':dict(num_ctx=16384,num_predict=8192,temperature=False)},
                       {'options':dict(num_ctx=16384,num_predict=9999,temperature=0)},
                       {'stream':True},{'tools':[]},{'messages':[]},{'keep_alive':-1}):
            with self.subTest(change=change),self.assertRaises(ValueError):
                self.submit('bad',self.payload_eval()|change,profile='nextcloud-eval-qwen')
        with self.assertRaises(ValueError):self.submit('missing-think',profile='nextcloud-eval-qwen')

    def test_finite_bulk_budget_legacy_cap_and_replay_at_capacity(self):
        before=self.policy_path.read_bytes();first=self.admit()
        for i in range(1,256):self.admit('bulk-'+str(i))
        with self.assertRaises(SourceQueueFull):self.admit('overflow')
        with self.assertRaises(SourceQueueFull):self.submit('legacy-at-cap')
        self.assertEqual(self.admit()['id'],first['id'])
        self.assertEqual(self.policy_path.read_bytes(),before)
        self.assertEqual(len(self.gpu.requests),0)
        self.policy.set_admission_allowed('nextcloud',False)
        self.assertEqual(self.admit()['id'],first['id'])

    def test_byte_budget_and_closed_unknown_policy(self):
        first=self.admit()
        self.broker.db.execute('UPDATE jobs SET input_bytes=? WHERE id=?',(256*1024**2,first['id']));self.broker.db.commit()
        with self.assertRaises(SourceQueueFull):self.admit('over-bytes')
        self.assertEqual(self.admit()['id'],first['id'])
        self.broker.use_source_policy(None)
        with self.assertRaises(SourceAdmissionBlocked):self.admit('unknown-policy')

    def test_restart_preserves_identity_payload_and_no_duplicate(self):
        first=self.admit()
        other=Broker(self.root/'test.sqlite3',fixtures.FakeGPU(),fixtures.FakeWOL(),source_policy=self.policy)
        try:
            replay=other.submit('nextcloud-eval-qwen','generate',self.payload_eval(),'nextcloud','3','eval-1')
            self.assertEqual(replay['id'],first['id'])
            self.assertEqual(other.db.execute('SELECT count(*) FROM jobs').fetchone()[0],1)
            with self.assertRaises(ValueError):other.submit('nextcloud-eval-qwen','generate',self.payload_eval()|{'prompt':'changed'},'nextcloud','3','eval-1')
        finally:other.db.close()

    def test_projection_never_reads_reasoning_and_filters_nested_fields(self):
        class Guard(dict):
            def get(self,key,*args):
                if key in ('thinking','analysis','reasoning'):raise AssertionError('private field accessed')
                return super().get(key,*args)
            def items(self):raise AssertionError('raw result enumerated')
            def __iter__(self):raise AssertionError('raw result iterated')
        job=self.admit()
        job['result']=Guard(model='qwen3-vl:30b',response='final',thinking='PRIVATE',done=True,
                            done_reason='stop',eval_count=1,error='PRIVATE_ERROR')
        projected=self.broker.nextcloud_projection(job)
        self.assertNotIn('PRIVATE',json.dumps(projected));self.assertNotIn('payload',projected)
        self.assertTrue(projected['result']['error'])
        self.assertEqual(projected['request_contract']['options'],self.payload_eval()['options'])
        job['result']=Guard(model={'thinking':'PRIVATE'},response={'thinking':'PRIVATE'},done=True,eval_count=True)
        projected=self.broker.nextcloud_projection(job)
        self.assertNotIn('PRIVATE',json.dumps(projected));self.assertIsNone(projected['result']['response'])
        self.assertIsNone(projected['result']['eval_count'])
        with self.assertRaises(ValueError):self.broker.nextcloud_projection(job|dict(source='pilot-mainpc'))
        self.assertIsNone(self.broker.nextcloud_projection(None))

    def test_http_projection_pending_final_invalid_query_and_unchanged_default(self):
        server=serve(self.broker,port=0,policy=self.policy)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        opener=build_opener(ProxyHandler({}))
        def request(path,body=None):
            req=Request(f'http://127.0.0.1:{server.server_port}'+path,
                        data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
            try:
                with opener.open(req,timeout=5) as r:return r.status,json.load(r)
            except HTTPError as r:return r.code,json.load(r)
        body=dict(source='nextcloud',profile='nextcloud-eval-qwen',kind='generate',external_id='wire',source_item_id='3',payload=self.payload_eval())
        projection='?projection=nextcloud-final-v1'
        try:
            for suffix in ('?projection=wrong','?projection=','?projection=nextcloud-final-v1&projection=nextcloud-final-v1'):
                self.assertEqual(request('/v1/jobs'+suffix,body)[0],400)
            self.assertEqual(self.broker.db.execute('SELECT count(*) FROM jobs').fetchone()[0],0)
            self.assertEqual(request('/v1/jobs'+projection,[])[0],400)
            status,job=request('/v1/jobs'+projection,body);self.assertEqual(status,202)
            self.assertEqual(job['projection'],'nextcloud-final-v1');self.assertNotIn('payload',job)
            self.assertEqual(request('/v1/jobs/'+job['id']+projection)[1]['state'],'queued')
            self.assertEqual(request('/v1/jobs?projection=nextcloud-final-v1',body)[1]['id'],job['id'])
            self.assertIn('payload',request('/v1/jobs/'+job['id'])[1])
            self.gpu.run=lambda kind,req:dict(model=req['model'],response='valid final',thinking='PRIVATE',error=None,done=True,done_reason='stop',eval_count=10)
            self.assertTrue(self.broker.dispatch_once(policy=self.policy))
            final=request('/v1/jobs/'+job['id']+projection)[1]
            self.assertEqual(final['state'],'completed');self.assertNotIn('PRIVATE',json.dumps(final))
            self.assertEqual(final['result']['response'],'valid final')
            self.assertEqual(request('/v1/jobs'+projection,body)[1]['id'],job['id'])
            self.assertEqual(request('/v1/jobs/'+job['id']+'?projection=wrong')[0],400)
            self.assertEqual(request('/v1/jobs/00000000-0000-0000-0000-000000000000'+projection)[0],404)
        finally:server.shutdown();thread.join();server.server_close()

if __name__=='__main__':unittest.main()
