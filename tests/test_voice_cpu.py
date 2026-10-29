"""Synthetic adapter only: no audio decoding, model, GPU, or live service."""
import base64
import hashlib
import http.client
import http.server
import json
import os
import tempfile
import threading
import unittest
from unittest.mock import patch

from broker import voice_cpu
from broker.http import serve
from broker.service import Broker, SourcePolicy
from tests.test_source_controls import FakeOllama, FakeWol


class CPUVoiceContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.audio = b'fLaC synthetic fixture, not real audio'
        self.sha = hashlib.sha256(self.audio).hexdigest()
        self.token = os.path.join(self.tmp.name, 'token')
        with open(self.token, 'w') as f: f.write('fixture-token')
        os.chmod(self.token, 0o600)
        self.posts = []
        self.adapter_gets = []
        self.bad = None
        self.lost_ack = False
        outer = self
        class Adapter(http.server.BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def respond(self, status, data):
                raw=json.dumps(data).encode(); self.send_response(status)
                self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
            def do_GET(self):
                outer.adapter_gets.append(self.path)
                if self.path == '/health':
                    return self.respond(200,dict(service='openclaw-stt-http-adapter',routing_policies=['local-only-v1'],response_formats=['segments-v1']))
                timing=dict(schema='stt-segments-v1',backend=voice_cpu.BACKEND,device='cpu',model=voice_cpu.MODEL,
                    model_snapshot='a'*40,engine_version='1.2.1',weights_sha256='b'*64,timebase='decoded_audio_seconds',
                    method='faster_whisper_native_segments',precision='estimate',duration_seconds=1.0,audio_sha256=outer.sha,
                    segments=[dict(start=.2,end=.6,text='synthetic')])
                if outer.bad == 'missing': timing['segments']=[]
                if outer.bad == 'gpu': timing['device']='cuda'
                if outer.bad == 'hash': timing['audio_sha256']='c'*64
                backend = 'cloud' if outer.bad == 'cloud' else voice_cpu.BACKEND
                return self.respond(200,dict(schema_version=1,job_id='a'*32,status='done',media_sha256=outer.sha,routing_policy='local-only-v1',
                    response_format='segments-v1',result_http_status=200,result=dict(status='success',transcript='synthetic',timing=timing,
                    routing=dict(policy='local-only-v1',selected_backend=backend,attempts=[dict(backend=backend,status='success')]))))
            def do_POST(self):
                body=self.rfile.read(int(self.headers['Content-Length']))
                outer.posts.append((self.headers['Idempotency-Key'],body,self.headers['X-STT-Routing-Policy'],self.headers['X-STT-Response-Format']))
                if outer.lost_ack:
                    outer.lost_ack = False
                    self.close_connection = True
                    return
                return self.respond(202,dict(schema_version=1,job_id='a'*32,status_path='/v1/transcription-jobs/'+'a'*32,media_sha256=outer.sha,
                    routing_policy='local-only-v1',response_format='segments-v1'))
        self.adapter=http.server.ThreadingHTTPServer(('127.0.0.1',0),Adapter)
        self.adapter_thread=threading.Thread(target=self.adapter.serve_forever,daemon=True);self.adapter_thread.start()
        self.addCleanup(self.adapter.server_close);self.addCleanup(self.adapter.shutdown)
        self.env=patch.dict(os.environ,dict(BROKER_CPU_VOICE_ENABLED='1',BROKER_RELEASE_REVISION='a'*40,
            BROKER_CPU_STT_URL=f'http://127.0.0.1:{self.adapter.server_port}',BROKER_CPU_STT_TOKEN_FILE=self.token))
        self.env.start();self.addCleanup(self.env.stop)
        policyfile=os.path.join(self.tmp.name,'policy')
        with open(policyfile,'w') as f:json.dump({'sources':{'local-voice':{'weight':2}}},f)
        self.policy=SourcePolicy(policyfile)
        self.wake=[]
        class Wol:
            def wake(inner): outer.wake.append(True)
        self.broker=Broker(os.path.join(self.tmp.name,'db'),FakeOllama(),Wol(),source_policy=self.policy)
        self.addCleanup(self.broker.db.close)
        self.server=serve(self.broker,port=0,policy=self.policy)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.addCleanup(self.server.server_close);self.addCleanup(self.server.shutdown)
        self.payload=dict(filename='audio.flac',audio_base64=base64.b64encode(self.audio).decode(),response_format='segments-v1')
        self.body=dict(source='local-voice',profile=voice_cpu.PROFILE,kind='transcribe',external_id='arch-voice-'+'1'*64,
                       source_item_id='1'*64,payload=self.payload)

    def request(self,path,body=None):
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=5)
        data=json.dumps(body).encode() if body is not None else None
        try:
            conn.request('POST' if body is not None else 'GET',path,data,{'Content-Type':'application/json'})
            response=conn.getresponse(); return response.status,json.loads(response.read())
        finally:conn.close()

    def test_async_consumer_contract_replay_result_correlation_no_wake(self):
        status,caps=self.request('/v1/voice/capabilities')
        self.assertEqual(status,200);self.assertEqual(caps['profiles'][voice_cpu.PROFILE]['timing_device'],'cpu')
        self.assertEqual(self.posts,[])
        self.assertEqual(self.adapter_gets,[])
        status,ack=self.request('/v1/jobs',self.body);self.assertEqual(status,202)
        status,again=self.request('/v1/jobs',self.body);self.assertEqual(again['id'],ack['id'])
        self.assertEqual(self.broker.db.execute('select count(*) from jobs').fetchone()[0],1)
        changed=self.body|{'payload':self.payload|{'audio_base64':base64.b64encode(b'fLaC changed').decode()}}
        self.assertEqual(self.request('/v1/jobs',changed)[0],400)
        self.assertTrue(self.broker.dispatch_once(policy=self.policy))
        _,job=self.request('/v1/jobs/'+ack['id'])
        self.assertEqual(job['state'],'completed'); self.assertNotIn('payload',job)
        expected='sha256:'+hashlib.sha256(json.dumps(self.payload,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        self.assertEqual(job['input_hash'],expected)
        self.assertEqual(job['result']['voice_provenance']['audio_sha256'],self.sha)
        self.assertEqual(job['result']['voice_provenance']['revision'],caps['revision'])
        self.assertEqual(job['result']['timing']['segments'][0]['start'],.2)
        self.assertEqual(self.posts,[('broker-voice-'+ack['id'],self.audio,'local-only-v1','segments-v1')])
        self.assertEqual(self.wake,[])

    def test_bad_or_missing_timing_never_becomes_success(self):
        for bad in ('missing','gpu','hash','cloud'):
            self.bad=bad
            with self.subTest(bad=bad),self.assertRaises(RuntimeError):voice_cpu.execute(self.audio,'fixture-'+bad,5)

    def test_disabled_and_untrusted_network_fail_closed(self):
        with patch.dict(os.environ,{'BROKER_CPU_VOICE_ENABLED':'0'}):
            self.assertEqual(self.request('/v1/voice/capabilities')[0],503)
            self.assertEqual(self.request('/v1/jobs',self.body)[0],400)
        for url in ('http://example.org:18081','http://8.8.8.8:18081','http://user:pw@127.0.0.1:80','http://127.0.0.1/redirect'):
            with self.subTest(url=url),patch.dict(os.environ,{'BROKER_CPU_STT_URL':url}),self.assertRaises(ValueError):voice_cpu.configuration()
        self.assertEqual(self.posts,[])

    def test_source_admission_and_maintenance_still_fence_jobs(self):
        self.policy.set_admission_allowed('local-voice',False)
        self.assertEqual(self.request('/v1/jobs',self.body)[0],403)
        self.assertEqual(self.request('/v1/jobs',self.body|{'source':'arbitrary'})[0],400)
        self.assertEqual(self.request('/v1/voice/capabilities')[0],200)
        self.assertEqual(self.posts,[])

    def test_size_and_selector_are_bounded(self):
        oversized=base64.b64encode(b'fLaC'+b'x'*voice_cpu.MAX_AUDIO).decode()
        for payload in (self.payload|{'audio_base64':oversized},
                        self.payload|{'response_format':'text'},
                        self.payload|{'filename':'audio.wav'}):
            with self.subTest(selector=payload.get('response_format')):
                self.assertEqual(self.request('/v1/jobs',self.body|{'payload':payload})[0],400)
        self.assertEqual(self.posts,[])

    def test_lost_adapter_ack_is_not_automatically_reposted(self):
        self.lost_ack=True
        _,ack=self.request('/v1/jobs',self.body)
        self.assertTrue(self.broker.dispatch_once(policy=self.policy))
        self.assertEqual(self.broker.status(ack['id'])['state'],'failed')
        self.assertFalse(self.broker.dispatch_once(policy=self.policy))
        self.assertEqual(len(self.posts),1)
        # An explicitly coordinated reconciliation uses the same durable key.
        voice_cpu.execute(self.audio,ack['id'],5)
        self.assertEqual([entry[0] for entry in self.posts],['broker-voice-'+ack['id']]*2)


if __name__ == '__main__':unittest.main()
