"""Synthetic queued diarization only; no MAIN-PC, models or production state."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from broker import diarization_contract as d, voice, voice_gpu
from broker.service import Broker, SourcePolicy
from broker.profiles import PROFILES, VOICE_PROFILE_NAMES
from tests.test_batch_scheduler import FakeOllama, FakeWol, MutableClock

AUDIO = b'fLaC synthetic fixture'
SHA = hashlib.sha256(AUDIO).hexdigest()
PROFILE = d.PROFILE


def result():
    return dict(schema='stt-diarization-v1', model=d.MODEL, model_revision=d.REVISION,
                weights_sha256=d.WEIGHTS_SHA256, device='cuda', options=dict(d.OPTIONS),
                gpu_proof=dict(selected_backend='CUDA0',worker_platform='win32',
                    runtime_archive_sha256=d.PORTABLE_SHA256,runtime_binary_sha256='a'*64,process_tree_released=True,gpu_memory_delta_mib_peak=150),
                audio_sha256=SHA, duration_seconds=10, timebase=d.TIMEBASE,
                segments=[dict(start=0,end=4,speaker='speaker_0'),
                          dict(start=3,end=9,speaker='speaker_7')])


def ready(**changes):
    return dict(model=d.MODEL, active_jobs=0, model_loaded=False,
                request_tracking=True, request_owned_release=True,
                gpu_release_confirmed=True, last_request_id=None,
                last_request_gpu_released=False) | changes


class DiarizationQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock=MutableClock()
        self.calls=[]
        self.ollama=FakeOllama(self.calls,self.clock,400)
        self.ollama.ps=Mock(return_value=dict(models=[]))
        self.broker=Broker(os.path.join(self.tmp.name,'db'),self.ollama,
                           FakeWol(self.calls),clock=self.clock)
        self.addCleanup(self.broker.db.close)
        # Tests still intercept all HTTP at urlopen; native lane builds a
        # no-proxy/no-redirect opener, so redirect its open to that same fixture.
        import urllib.request
        from types import SimpleNamespace
        opener=patch('urllib.request.build_opener', side_effect=lambda *a: SimpleNamespace(
            open=lambda request,timeout: urllib.request.urlopen(request,timeout=timeout)))
        opener.start();self.addCleanup(opener.stop)
        self.payload=dict(filename='audio.flac', audio_base64=base64.b64encode(AUDIO).decode(),
                          diarization=dict(d.OPTIONS))

    def submit(self, key='fixture', payload=None):
        return self.broker.submit(PROFILE,'transcribe',payload or self.payload,
                                  source='local-voice',external_id=key,source_item_id=key)

    def test_catalogue_and_durable_admission_idempotency_audio_options_linkage(self):
        self.assertIn(PROFILE, VOICE_PROFILE_NAMES)
        self.assertEqual(PROFILES[PROFILE].executor,'voice')
        first=self.submit()
        self.assertEqual(self.submit()['id'],first['id'])
        self.assertEqual(self.broker.db.execute('select count(*) from jobs').fetchone()[0],1)
        with self.assertRaises(ValueError):
            self.submit(payload=self.payload | dict(audio_base64=base64.b64encode(AUDIO+b'changed').decode()))
        with self.assertRaises(ValueError):
            self.broker.submit(PROFILE,'transcribe',self.payload,source='local-voice')
        for changed in (dict(diarization=d.OPTIONS | dict(max_speakers=4)),
                        dict(language='ru'),dict(filename='chunk.wav'),
                        dict(worker_url='http://untrusted/'),dict(audio_base64='AAAA')):
            with self.subTest(changed=changed),self.assertRaises(ValueError):
                self.submit('bad',self.payload | changed)

    def test_direct_voice_execution_without_durable_guard_is_forbidden(self):
        with patch.dict(os.environ,dict(BROKER_DIARIZATION_URL='http://127.0.0.1:18084',
                                      BROKER_DIARIZATION_TOKEN='fixture')), \
             patch('urllib.request.urlopen') as transport:
            with self.assertRaisesRegex(RuntimeError,'durable queue GPU guard'):
                voice.execute(PROFILE,self.payload,10)
        transport.assert_not_called()

    def dispatch_with_worker(self, *, bad_result=False, lost=False, retained=False):
        state=ready()
        paths=[]
        def api(request,timeout):
            path=request.full_url.rsplit('/',1)[1];paths.append(path)
            if path=='status':
                return io.BytesIO(json.dumps(state).encode())
            if path=='transcribe':
                identity=request.get_header('X-request-id')
                self.assertEqual(self.broker.executor_fence()['request_id'],
                                 voice_gpu.PREFIX+PROFILE+'|'+identity)
                self.assertEqual(request.data,AUDIO)
                state.update(last_request_id=identity,model_loaded=True,gpu_release_confirmed=False)
                if lost:
                    state['active_jobs']=1
                    raise TimeoutError('lost inference reply')
                value=result()
                if bad_result: value['device']='cpu'
                return io.BytesIO(json.dumps(value).encode())
            if path=='release':
                self.assertEqual(request.get_header('X-request-id'),state['last_request_id'])
                if retained:
                    raise TimeoutError('release unknown')
                state.update(model_loaded=False,gpu_release_confirmed=True,last_request_gpu_released=True)
                return io.BytesIO(b'{"gpu_released":true}')
            self.fail(path)
        with patch.dict(os.environ,dict(BROKER_DIARIZATION_URL='http://127.0.0.1:18084',
                                      BROKER_DIARIZATION_TOKEN='fixture')), \
             patch('urllib.request.urlopen',side_effect=api):
            self.assertTrue(self.broker.dispatch_once())
        return state,paths

    def test_real_dispatch_claim_release_and_no_duplicate_execution(self):
        job=self.submit()
        state,paths=self.dispatch_with_worker()
        observed=self.broker.status(job['id'])
        self.assertEqual(observed['state'],'completed')
        self.assertEqual(observed['result'],result())
        self.assertIsNone(self.broker.executor_fence())
        self.assertEqual(paths.count('transcribe'),1)
        self.assertEqual(paths.count('release'),1)
        self.assertFalse(self.broker.dispatch_once())
        self.assertEqual(self.submit()['id'],job['id'])
        self.assertEqual(self.broker.db.execute('select count(*) from job_attempts').fetchone()[0],1)
        self.assertFalse(any(isinstance(c,tuple) and c[0]=='unload' for c in self.calls))

    def test_bad_result_or_unknown_inference_does_not_become_clean_or_replay(self):
        for mode in ('invalid','lost'):
            job=self.submit(mode)
            _state,paths=self.dispatch_with_worker(bad_result=mode=='invalid',lost=mode=='lost')
            self.assertEqual(self.broker.status(job['id'])['state'],'failed')
            self.assertFalse(self.broker.dispatch_once())
            self.assertEqual(paths.count('transcribe'),1)
            if mode=='lost':
                self.assertIsNotNone(self.broker.executor_fence())

    def test_release_uncertainty_retains_fence_even_with_completed_result(self):
        job=self.submit()
        state,paths=self.dispatch_with_worker(retained=True)
        self.assertEqual(self.broker.status(job['id'])['state'],'completed')
        fence=self.broker.executor_fence()
        self.assertIsNotNone(fence)
        self.broker.submit('interactive','generate',dict(prompt='waiting'))
        with patch.object(voice_gpu,'worker',return_value=state):
            self.assertFalse(self.broker.dispatch_once())
        self.assertEqual(self.broker.executor_fence(),fence)

    def test_source_weight_admission_pause_and_existing_journals_are_used(self):
        policyfile=os.path.join(self.tmp.name,'policy')
        Path(policyfile).write_text(json.dumps(dict(sources={'local-voice':dict(weight=2)})))
        policy=SourcePolicy(policyfile)
        self.broker.use_source_policy(policy)
        policy.set_admission_allowed('local-voice',False)
        from broker.service import SourceAdmissionBlocked
        with self.assertRaises(SourceAdmissionBlocked):
            self.submit()
        self.assertEqual(policy.snapshot()['sources']['local-voice']['weight'],2)
        policy.set_admission_allowed('local-voice',True)
        job=self.submit()
        # Same durable job table/event journal; no private dispatcher or queue.
        self.assertEqual(self.broker.status(job['id'])['source'],'local-voice')
        self.assertEqual(self.broker.db.execute('select count(*) from audit_events where job_id=?',
                                             (job['id'],)).fetchone()[0],1)


if __name__=='__main__':unittest.main()
