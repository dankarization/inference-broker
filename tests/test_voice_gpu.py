import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
from broker import voice_gpu as g
from broker.voice import _HTTP_WORKERS

PROFILE='voice-whisper-gpu-large-v3-v1'
ID='b0f3e631-e89a-4e09-869d-919c12f9b8ef'

def ready(**changes):
    return dict({'model':_HTTP_WORKERS[PROFILE][2],'active_jobs':0,'model_loaded':False,
        'request_tracking':True,'request_owned_release':True,'gpu_release_confirmed':True,
        'last_request_id':ID,'last_request_gpu_released':True},**changes)

class Store:
    def __init__(self):self.fence=None;self.ollama=Mock()
    def executor_fence(self):return self.fence
    def set_executor_fence(self,request_id,job_id):
        if self.fence:raise RuntimeError('already fenced')
        self.fence={'request_id':request_id,'job_id':job_id}
    def clear_executor_fence(self,request_id):
        if self.fence and self.fence['request_id']==request_id:self.fence=None

class VoiceGPUTests(unittest.TestCase):
    def test_waits_for_natural_ollama_expiry_then_releases_only_own_idle_request(self):
        broker=Store()
        broker.ollama.ps.side_effect=[{'models':[{'name':'qwen'}]},{'models':[]}]
        observed=ready(last_request_id=None,last_request_gpu_released=False,request_tracking=False)
        def worker(profile,path,**kwargs):
            self.assertEqual(profile,PROFILE)
            if path=='/v1/release':
                self.assertEqual(kwargs['request_id'],ID)
                self.assertTrue(kwargs['release'])
                observed.update(model_loaded=False,gpu_release_confirmed=True,last_request_gpu_released=True)
                return {'gpu_released':True}
            return dict(observed)
        with patch.object(g,'worker',side_effect=worker) as api,patch.object(g.uuid,'uuid4',return_value=ID),patch.object(g.time,'sleep') as sleep:
            with g.Guard(broker,'job').claim(PROFILE,10) as claim:
                self.assertEqual(claim.request_id,ID)
                self.assertEqual(broker.fence['request_id'],g.PREFIX+PROFILE+'|'+ID)
                observed.update(model_loaded=True,gpu_release_confirmed=False,last_request_id=ID)
            self.assertIsNone(broker.fence)
            sleep.assert_called_once()
        broker.ollama.unload.assert_not_called()
        self.assertEqual(sum(c.args[1]=='/v1/release' for c in api.call_args_list),1)

    def test_legacy_tracking_unknown_or_foreign_worker_never_claimed_or_released(self):
        for changes in ({'request_owned_release':False},{'request_tracking':None},{'model_loaded':True},
                {'active_jobs':1},{'gpu_release_confirmed':False},
                {'model':'different'},{'active_jobs':True}):
            with self.subTest(changes=changes):
                broker=Store();broker.ollama.ps.return_value={'models':[]}
                with patch.object(g,'worker',return_value=ready(**changes)) as api:
                    with self.assertRaises(RuntimeError):
                        with g.Guard(broker,'job').claim(PROFILE,1):self.fail('unsafe admission')
                self.assertIsNone(broker.fence)
                self.assertFalse(any(c.kwargs.get('release') for c in api.call_args_list))

    def test_unknown_residency_and_expired_wait_never_unload_or_upload(self):
        for response in ({}, {'models':[{'name':'qwen'}]}):
            broker=Store();broker.ollama.ps.return_value=response
            with patch.object(g.time,'monotonic',side_effect=[0,2]),patch.object(g,'worker') as worker:
                with self.assertRaises((RuntimeError,TimeoutError)):
                    with g.Guard(broker,'job').claim(PROFILE,1):self.fail('unsafe admission')
            worker.assert_not_called();broker.ollama.unload.assert_not_called()

    def test_foreign_owner_active_job_and_unknown_transport_keep_durable_fence(self):
        fence={'request_id':g.PREFIX+PROFILE+'|'+ID,'job_id':'job'}
        for value in (ready(last_request_id='different'),ready(active_jobs=1),RuntimeError('unavailable')):
            broker=Store();broker.fence=dict(fence)
            with patch.object(g,'worker',side_effect=value if isinstance(value,Exception) else None,
                    return_value=value if not isinstance(value,Exception) else None) as worker:
                self.assertFalse(g.reconcile(broker,fence))
            self.assertEqual(broker.fence,fence)
            self.assertFalse(any(c.kwargs.get('release') for c in worker.call_args_list))

    def test_release_uncertainty_never_clears_fence(self):
        broker=Store();broker.fence={'request_id':g.PREFIX+PROFILE+'|'+ID,'job_id':'job'}
        with patch.object(g,'worker',side_effect=[ready(model_loaded=True),TimeoutError('unknown')]):
            self.assertFalse(g.reconcile(broker,broker.fence))
        self.assertIsNotNone(broker.fence)

    def test_timeout_never_replays_inference_and_release_waits_for_real_completion(self):
        broker=Store();broker.ollama.ps.return_value={'models':[]}
        observed=ready(last_request_id=None)
        with patch.object(g,'worker',side_effect=lambda *a,**k:dict(observed)), \
                patch.object(g.uuid,'uuid4',return_value=ID),patch.object(g.time,'sleep'):
            with self.assertRaisesRegex(TimeoutError,'inference unknown'):
                with g.Guard(broker,'job').claim(PROFILE,10):
                    observed.update(active_jobs=1)
                    raise TimeoutError('inference unknown')
            self.assertIsNotNone(broker.fence)
            observed.update(active_jobs=0,last_request_id=ID,model_loaded=False,
                gpu_release_confirmed=True,last_request_gpu_released=True)
            self.assertTrue(g.reconcile(broker,broker.fence))
        self.assertIsNone(broker.fence)

    def test_existing_fence_table_survives_restart_and_blocks_llm_until_release(self):
        from broker.service import Broker
        from tests.test_batch_scheduler import FakeOllama,FakeWol,MutableClock
        with tempfile.TemporaryDirectory() as tmp:
            clock=MutableClock();calls=[]
            def make():return Broker(os.path.join(tmp,'db'),FakeOllama(calls,clock,400),FakeWol(calls),clock=clock)
            first=make()
            first.set_executor_fence(g.PREFIX+PROFILE+'|'+ID,'original-job')
            first.db.close()
            second=make()
            try:
                with patch.object(g,'worker',return_value=ready(active_jobs=1)):
                    self.assertFalse(second.reconcile_executor_fence())
                with patch.object(g,'worker',return_value=ready()):
                    self.assertTrue(second.reconcile_executor_fence())
                self.assertIsNone(second.executor_fence())
            finally:second.db.close()

    def test_actual_queue_executor_fences_upload_and_releases_matching_worker(self):
        import base64
        import io
        from broker.service import Broker
        from tests.test_batch_scheduler import FakeOllama,FakeWol,MutableClock
        with tempfile.TemporaryDirectory() as tmp:
            calls=[];clock=MutableClock()
            ollama=FakeOllama(calls,clock,400)
            ollama.ps=Mock(return_value={'models':[]})
            broker=Broker(os.path.join(tmp,'db'),ollama,FakeWol(calls),clock=clock)
            observed=ready(last_request_id=None,last_request_gpu_released=False)
            paths=[]
            def api(request,timeout):
                path=request.full_url.rsplit('/',1)[1];paths.append(path)
                if path=='status':return io.BytesIO(json.dumps(observed).encode())
                if path=='transcribe':
                    identity=request.get_header('X-request-id')
                    self.assertTrue(identity)
                    self.assertEqual(broker.executor_fence()['request_id'],g.PREFIX+PROFILE+'|'+identity)
                    observed.update(last_request_id=identity,model_loaded=True,gpu_release_confirmed=False)
                    return io.BytesIO(json.dumps({'model':_HTTP_WORKERS[PROFILE][2],'device':'cuda',
                        'text':'native','segments':[{'start':0.,'end':1.,'text':'native'}]}).encode())
                if path=='release':
                    self.assertEqual(request.get_header('X-request-id'),observed['last_request_id'])
                    observed.update(model_loaded=False,gpu_release_confirmed=True,last_request_gpu_released=True)
                    return io.BytesIO(b'{"gpu_released":true}')
                self.fail(path)
            try:
                job=broker.submit(PROFILE,'transcribe',{'filename':'audio.ogg',
                    'audio_base64':base64.b64encode(b'audio').decode()},source='local-voice')
                with patch.dict(os.environ,{'BROKER_WHISPER_LARGE_URL':'http://127.0.0.1:18083',
                        'BROKER_WHISPER_LARGE_TOKEN':'synthetic'},clear=True), \
                        patch('urllib.request.urlopen',side_effect=api):
                    self.assertTrue(broker.dispatch_once())
                self.assertEqual(broker.status(job['id'])['state'],'completed')
                self.assertIsNone(broker.executor_fence())
                self.assertEqual(paths.count('transcribe'),1)
                self.assertEqual(paths.count('release'),1)
            finally:broker.db.close()

    def test_native_asr_fence_is_not_interpreted_as_voice(self):
        broker=Store();broker.fence={'request_id':ID,'job_id':'job'}
        with patch.object(g,'worker') as worker:
            self.assertFalse(g.reconcile(broker,broker.fence))
        worker.assert_not_called()
