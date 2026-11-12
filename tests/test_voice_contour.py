import base64
import io
import json
import os
import unittest
from unittest.mock import Mock,patch
from broker import voice,voice_contour as c


def payload(**changes):
    return dict({'audio_base64':base64.b64encode(b'audio').decode(),'filename':'audio.ogg',
        'source_duration_seconds':79.,'windows_model':'medium','language':'auto',
        'local_only':False,'response_format':'text'},**changes)


def result(model='whisper-medium-int8-mainpc-cuda',device='cuda'):
    return {'text':'hello','segments':[{'start':0.,'end':1.,'text':'hello'}],'model':model,'device':device}


class ContourTests(unittest.TestCase):
    def test_cpu_first_even_when_large_requested(self):
        windows=Mock();r=result('rhasspy/faster-whisper-medium-int8','cpu')
        with patch.object(c,'cpu',return_value=r) as cpu,patch.object(c,'cloud') as cloud:
            out=c.execute(payload(windows_model='large'),10,windows)
        self.assertEqual(out['contour']['tier'],'cpu');cpu.assert_called_once();windows.assert_not_called();cloud.assert_not_called()

    def test_long_source_gate_selects_each_windows_model_in_same_queued_executor(self):
        for model,profile in c.WINDOWS.items():
            windows=Mock(return_value=result())
            with patch.object(c,'cpu') as cpu,patch.object(c,'cloud') as cloud:
                out=c.execute(payload(source_duration_seconds=2911.424,windows_model=model,local_only=True),10,windows)
            self.assertEqual(windows.call_args.args[0],profile)
            self.assertEqual(out['contour']['attempts'][0]['reason'],'source_duration_gate')
            cpu.assert_not_called();cloud.assert_not_called()

    def test_cloud_only_after_known_local_failure_and_never_local_only(self):
        windows=Mock(side_effect=c.BackendUnconfigured('not configured'))
        with patch.object(c,'cpu',side_effect=ValueError('CPU disabled')),patch.object(c,'cloud',return_value=result('cloud','cloud')) as cloud:
            out=c.execute(payload(),10,windows)
            self.assertEqual(out['contour']['tier'],'cloud');cloud.assert_called_once()
            with self.assertRaisesRegex(RuntimeError,'local-only'):c.execute(payload(local_only=True),10,windows)
            self.assertEqual(cloud.call_count,1)

    def test_uncertain_windows_transport_never_starts_cloud(self):
        windows=Mock(side_effect=TimeoutError('remote outcome unknown'))
        with patch.object(c,'cpu',side_effect=ValueError),patch.object(c,'cloud') as cloud:
            with self.assertRaises(TimeoutError):c.execute(payload(),10,windows)
            cloud.assert_not_called()

    def test_timestamps_skip_plain_cpu_then_use_real_windows_segments(self):
        with patch.object(c,'cpu',return_value={'text':'plain'}) as cpu,patch.object(c,'cloud') as cloud:
            out=c.execute(payload(response_format='segments-v1'),10,Mock(return_value=result()))
            self.assertEqual(out['contour']['tier'],'windows');cpu.assert_called_once();cloud.assert_not_called()

    def test_language_override_is_forwarded_not_silently_ignored(self):
        windows=Mock(return_value=dict(result(),settings={'language':None}))
        with patch.object(c,'cpu') as cpu,patch.object(c,'cloud',return_value=result('cloud','cloud')) as cloud:
            out=c.execute(payload(language='en'),10,windows)
            self.assertEqual(windows.call_args.args[1]['language'],'en')
            self.assertEqual(out['contour']['tier'],'cloud');cpu.assert_not_called();cloud.assert_called_once()
        windows=Mock(return_value=dict(result(),settings={'language':'en'}))
        with patch.object(c,'cloud') as cloud:
            self.assertEqual(c.execute(payload(language='en',local_only=True),10,windows)['contour']['tier'],'windows')
            cloud.assert_not_called()

    def test_cloud_wire_explicit_language_and_native_segments(self):
        requests=[]
        def call(request,timeout):
            requests.append(request)
            return io.BytesIO(json.dumps({'text':'hello','language':'en','segments':[{'id':0,'start':0.,'end':1.,'text':'hello'}]}).encode())
        with patch.dict(os.environ,{'BROKER_CLOUD_STT_TOKEN':'synthetic-test-secret'},clear=True),patch('urllib.request.urlopen',side_effect=call):
            r=c.cloud(b'audio',payload(language='en',response_format='segments-v1'),10)
            c.native(r,2)
            self.assertEqual(requests[0].full_url,'https://api.openai.com/v1/audio/transcriptions')
            self.assertIn(b'whisper-1',requests[0].data);self.assertIn(b'verbose_json',requests[0].data)
            self.assertIn(b'name="language"\r\n\r\nen',requests[0].data)
            with self.assertRaises(ValueError):c.cloud(b'audio',payload(local_only=True),10)
            self.assertEqual(len(requests),1)

    def test_queued_profile_validate_keeps_legacy_voice_contracts(self):
        self.assertEqual(voice.validate(payload(),'voice-contour-v1')['windows_model'],'medium')
        for key,value in [('local_only','yes'),('language','en;cmd'),('source_duration_seconds',float('nan'))]:
            with self.assertRaises(ValueError):voice.validate(payload(**{key:value}),'voice-contour-v1')

    def test_existing_broker_deduplicates_and_dispatches_contour_under_voice_lease(self):
        import tempfile
        from broker.service import Broker
        from tests.test_batch_scheduler import FakeOllama,FakeWol,MutableClock
        with tempfile.TemporaryDirectory() as tmp:
            clock=MutableClock();calls=[]
            broker=Broker(os.path.join(tmp,'db'),FakeOllama(calls,clock,400),FakeWol(calls),clock=clock)
            try:
                args=dict(source='local-voice',external_id='same-input',source_item_id='same-input')
                first=broker.submit(c.PROFILE,'transcribe',payload(windows_model='large'),**args)
                second=broker.submit(c.PROFILE,'transcribe',payload(windows_model='large'),**args)
                self.assertEqual(first['id'],second['id'])
                with patch.object(c,'cpu',return_value=result('rhasspy/faster-whisper-medium-int8','cpu')) as cpu,patch.object(c,'cloud') as cloud:
                    self.assertTrue(broker.dispatch_once())
                observed=broker.status(first['id'])
                self.assertEqual(observed['state'],'completed')
                self.assertEqual(observed['result']['contour']['tier'],'cpu')
                self.assertEqual(len(broker.attempts(first['id'])),1)
                cpu.assert_called_once();cloud.assert_not_called()
            finally:broker.db.close()

    def test_explicit_language_capability_is_checked_before_windows_upload(self):
        requests=[]
        def call(request,timeout):
            requests.append(request)
            if request.full_url.endswith('/v1/status'):
                return io.BytesIO(json.dumps({'language_override':False}).encode())
            self.fail('unsupported explicit language uploaded audio')
        with patch.dict(os.environ,{'BROKER_WHISPER_GPU_URL':'http://127.0.0.1:18082','BROKER_WHISPER_GPU_TOKEN':'synthetic'},clear=True),patch('urllib.request.urlopen',side_effect=call):
            with self.assertRaisesRegex(c.BackendRejected,'explicit language'):
                voice.execute('voice-whisper-gpu-v1',{'audio_base64':base64.b64encode(b'audio').decode(),'filename':'audio.ogg','language':'en'},10)
        self.assertEqual(len(requests),1)
