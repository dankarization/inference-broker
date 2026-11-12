"""One queued voice-contour execution: CPU gate -> Windows -> optional cloud.

Called only by the broker's voice executor, under its existing scheduler lease.
No nested broker submission, alternate queue or caller-selected worker URL.
"""
import base64
import io
import json
import math
import os
from pathlib import Path
import re
import subprocess
import tempfile
import urllib.error
import urllib.request
import uuid

PROFILE = 'voice-contour-v1'
WINDOWS = {'medium':'voice-whisper-gpu-v1', 'large':'voice-whisper-gpu-large-v3-v1', 'gigaam':'voice-gigaam-v1'}
CPU_MAX_SECONDS = 600

class BackendRejected(RuntimeError):
    """Known terminal response/capability rejection, not transport uncertainty."""

class BackendUnconfigured(RuntimeError):
    """No upload attempted; fallback may select the next configured tier."""


def validate(payload):
    allowed={'audio_base64','filename','windows_model','response_format','language','local_only','source_duration_seconds'}
    if not isinstance(payload,dict) or set(payload)-allowed:
        raise ValueError('invalid voice contour fields')
    if payload.get('filename') != 'audio.ogg':raise ValueError('contour requires canonical audio.ogg')
    model=payload.get('windows_model','medium')
    language=payload.get('language','auto')
    fmt=payload.get('response_format','text')
    duration=payload.get('source_duration_seconds')
    if (not isinstance(model,str) or model not in WINDOWS or not isinstance(language,str)
            or not re.fullmatch(r'auto|[a-z]{2,3}',language) or not isinstance(fmt,str) or fmt not in {'text','segments-v1'}
            or type(payload.get('local_only',False)) is not bool
            or type(duration) not in (int,float) or not math.isfinite(duration) or not 0<duration<=7200):
        raise ValueError('invalid voice contour options')
    return dict(payload,windows_model=model,language=language,response_format=fmt,
                local_only=payload.get('local_only',False))


def native(result,duration):
    rows=result.get('segments');text=result.get('text')
    if not isinstance(rows,list) or not rows or not isinstance(text,str) or not text.strip():
        raise ValueError('native segments unavailable')
    previous=-1
    for row in rows:
        if not isinstance(row,dict):raise ValueError('invalid native segment')
        a,b,t=row.get('start'),row.get('end'),row.get('text')
        finite=lambda x:type(x) in (int,float) and math.isfinite(x)
        if not finite(a) or not finite(b) or not 0<=a<b<=duration+.1 or a<previous or not isinstance(t,str) or not t.strip():
            raise ValueError('invalid native segment bounds')
        previous=a
    if ' '.join(r['text'].strip() for r in rows)!=text.strip():
        raise ValueError('segments and transcript disagree')
    return result


def cpu(audio,request_id,timeout):
    from . import voice_cpu
    # Configuration failure is a pre-admission capability gate, not a failed
    # remote job or permission to run an alternate CPU service.
    voice_cpu.configuration()
    import time
    started=time.monotonic()
    with tempfile.TemporaryDirectory(prefix='broker-voice-cpu-') as tmp:
        source=Path(tmp)/'audio.ogg';target=Path(tmp)/'audio.flac';source.write_bytes(audio)
        subprocess.run(['ffmpeg','-nostdin','-v','error','-protocol_whitelist','file','-f','ogg','-i',str(source),
            '-t','300.1','-fs',str(voice_cpu.MAX_AUDIO+1),'-map','0:a:0','-vn','-map_metadata','-1','-ac','1','-ar','16000','-c:a','flac',str(target)],
            stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
            check=True,timeout=min(120,timeout))
        if not 0<target.stat().st_size<=voice_cpu.MAX_AUDIO:
            raise ValueError('CPU FLAC admission limit')
        result=voice_cpu.execute(target.read_bytes(),request_id,max(.1,timeout-(time.monotonic()-started)))
    timing=result['timing'];proof=result['voice_provenance']
    return {'text':result['text'],'segments':timing['segments'],'model':proof['model'],
            'device':'cpu','backend':voice_cpu.BACKEND,'timing':timing,'voice_provenance':proof}


def cloud(audio,payload,timeout):
    """Last tier only. Preserve old cloud text model; native Whisper timing opt-in."""
    if payload['local_only']:raise ValueError('cloud forbidden by local-only')
    token=os.environ.get('BROKER_CLOUD_STT_TOKEN') or os.environ.get('OPENAI_API_KEY')
    if not token:raise ValueError('cloud credential not configured')
    timestamps=payload['response_format']=='segments-v1'
    model=os.environ.get('BROKER_CLOUD_STT_SEGMENTS_MODEL','whisper-1') if timestamps else os.environ.get('BROKER_CLOUD_STT_MODEL','gpt-4o-mini-transcribe')
    if timestamps and model!='whisper-1':
        raise ValueError('configured cloud model native timing capability unknown')
    fields={'model':model,'response_format':'verbose_json' if timestamps else 'json'}
    if payload['language']!='auto':fields['language']=payload['language']
    boundary='voice-'+uuid.uuid4().hex
    parts=[]
    for key,value in fields.items():
        parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n').encode())
    parts.extend([(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="audio.ogg"\r\nContent-Type: audio/ogg\r\n\r\n').encode(),audio,f'\r\n--{boundary}--\r\n'.encode()])
    url=os.environ.get('BROKER_CLOUD_STT_URL','https://api.openai.com').rstrip('/')+'/v1/audio/transcriptions'
    request=urllib.request.Request(url,data=b''.join(parts),method='POST',headers={
        'Authorization':'Bearer '+token,'Content-Type':'multipart/form-data; boundary='+boundary})
    try:
        with urllib.request.urlopen(request,timeout=timeout) as response:
            raw=response.read(2*1024*1024+1)
            if len(raw)>2*1024*1024:raise ValueError('cloud response exceeds limit')
            data=json.loads(raw)
    except Exception:
        raise RuntimeError('cloud transcription unavailable; remote outcome may be unknown') from None
    if not isinstance(data,dict) or not isinstance(data.get('text'),str) or not data['text'].strip():
        raise ValueError('cloud transcript missing')
    result={'text':data['text'],'model':model,'device':'cloud','backend':'openai',
            'reported_language':data.get('language')}
    if timestamps:
        rows=data.get('segments')
        if not isinstance(rows,list):raise ValueError('cloud native timing unavailable')
        result['segments']=[{'start':r.get('start'),'end':r.get('end'),'text':r.get('text')} for r in rows if isinstance(r,dict)]
        if len(result['segments'])!=len(rows):raise ValueError('invalid cloud segments')
    return result


def execute(payload,timeout,windows):
    import time
    from .voice_cpu import CPUAdmissionRejected, CPUJobFailed
    payload=validate(payload);audio=base64.b64decode(payload['audio_base64'],validate=True)
    deadline=time.monotonic()+timeout;trace=[]
    def remaining():
        n=deadline-time.monotonic()
        if n<=0:raise RuntimeError('voice contour budget exhausted')
        return n
    def finish(result,tier):
        if not isinstance(result,dict) or not isinstance(result.get('text'),str) or not result['text'].strip():
            raise ValueError('backend returned no transcript')
        if payload['response_format']=='segments-v1':native(result,min(payload['source_duration_seconds'],300))
        result=dict(result,contour={'schema':'voice-contour-v1','tier':tier,'local_only':payload['local_only'],
            'requested_language':payload['language'],'windows_model':payload['windows_model'], 'attempts':trace+[{'tier':tier,'status':'success'}]})
        return result
    if payload['source_duration_seconds']>CPU_MAX_SECONDS:
        trace.append({'tier':'cpu','status':'skipped','reason':'source_duration_gate'})
    elif payload['language']!='auto':
        trace.append({'tier':'cpu','status':'skipped','reason':'explicit_language_not_supported_by_cpu_bridge'})
    else:
        try:
            identity=__import__('hashlib').sha256(audio).hexdigest()
            return finish(cpu(audio,'voice-contour-'+identity,remaining()),'cpu')
        except (ValueError,subprocess.CalledProcessError,subprocess.TimeoutExpired,FileNotFoundError,CPUAdmissionRejected,CPUJobFailed):
            trace.append({'tier':'cpu','status':'skipped','reason':'capability_or_admission_gate'})
        # Runtime/transport error from the durable CPU adapter is ambiguous;
        # never turn it into a successful stage transition or new inference.
    model=payload['windows_model']
    if model=='gigaam' and payload['language'] not in {'auto','ru'}:
        trace.append({'tier':'windows','status':'skipped','reason':'language_capability'})
    else:
        sub={'audio_base64':payload['audio_base64'],'filename':payload['filename']}
        if payload['language']!='auto':sub['language']=payload['language']
        try:
            result=windows(WINDOWS[model],sub,remaining())
            if model!='gigaam' and result.get('device')!='cuda':raise ValueError('CUDA proof missing')
            if payload['language']!='auto' and model!='gigaam' and result.get('settings',{}).get('language')!=payload['language']:
                raise ValueError('explicit language not honored')
            return finish(result,'windows')
        except (ValueError,urllib.error.HTTPError,BackendRejected,BackendUnconfigured):
            trace.append({'tier':'windows','status':'failed','reason':'backend_rejected_or_capability_unavailable'})
        except urllib.error.URLError as exc:
            if not isinstance(exc.reason,ConnectionRefusedError):raise
            trace.append({'tier':'windows','status':'skipped','reason':'connection_refused_before_upload'})
        # Timeout/transport errors stay uncertain. Same queued job owns that
        # uncertainty: no second Windows call and no automatic cloud spend.
    if payload['local_only']:
        raise RuntimeError('local-only contour exhausted; cloud was not called')
    return finish(cloud(audio,payload,remaining()),'cloud')
