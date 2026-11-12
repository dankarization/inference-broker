"""Opt-in bridge to the existing local-only durable CPU segment adapter.

Metadata is configuration, not runtime attestation. No model/network access occurs
when reading capabilities. The operator must independently review the adapter.
"""
import hashlib
import http.client
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import stat
import time
from urllib.parse import urlsplit

PROFILE = 'voice-native-cpu-v1'
BACKEND = 'faster_whisper_segments_cpu'
MODEL = 'rhasspy/faster-whisper-medium-int8'
MAX_AUDIO = 8 * 1024 * 1024
MAX_RESPONSE = 2 * 1024 * 1024
PATH = '/v1/transcription-jobs'

class CPUAdmissionRejected(RuntimeError):
    """Adapter explicitly rejected admission; no remote inference was accepted."""

class CPUJobFailed(RuntimeError):
    """Durable adapter reports a known terminal failure, not transport loss."""



def configuration():
    if os.environ.get('BROKER_CPU_VOICE_ENABLED') != '1':
        raise ValueError('CPU voice bridge disabled')
    revision = os.environ.get('BROKER_RELEASE_REVISION', '')
    if not re.fullmatch('[a-f0-9]{40}', revision):
        raise ValueError('CPU voice release revision missing')
    p = urlsplit(os.environ.get('BROKER_CPU_STT_URL', ''))
    if p.scheme != 'http' or not p.hostname or p.username or p.password or p.path not in ('', '/') or p.query or p.fragment:
        raise ValueError('CPU voice adapter URL invalid')
    try:
        host = ipaddress.ip_address(p.hostname)
        port = 80 if p.port is None else p.port
    except ValueError:
        raise ValueError('CPU voice adapter address invalid') from None
    if not 1 <= port <= 65535 or not (host.is_loopback or (host.version == 4 and any(host in ipaddress.ip_network(net) for net in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')))):
        raise ValueError('CPU voice adapter must use a private literal address')
    token_path = Path(os.environ.get('BROKER_CPU_STT_TOKEN_FILE', ''))
    if not token_path.is_absolute():
        raise ValueError('CPU voice adapter token path missing')
    return revision, str(host), port, token_path


def capabilities():
    revision, _, _, _ = configuration()
    return {'schema': 'arch-broker-voice-capabilities-v1', 'revision': revision,
            'local_only': True, 'idempotency': 'source-external-id-payload-v1',
            'profiles': {PROFILE: {'response_formats': ['segments-v1'], 'input_formats': ['flac'],
                'max_audio_bytes': MAX_AUDIO, 'provenance': 'arch-broker-voice-result-v1',
                'backend': BACKEND, 'device': 'cpu', 'model': MODEL,
                'timing_backend': BACKEND, 'timing_device': 'cpu'}}}


def _token(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 8192:
        raise RuntimeError('CPU adapter token file must be private and owned')
    token = path.read_text().strip()
    if not token or '\n' in token or '\r' in token:
        raise RuntimeError('CPU adapter token invalid')
    return token


def _request(host, port, token, method, path, body=None, key=None, timeout=10):
    headers = {'Authorization': 'Bearer ' + token, 'X-STT-Routing-Policy': 'local-only-v1',
               'X-STT-Response-Format': 'segments-v1'}
    if body is not None:
        headers.update({'Content-Type': 'application/octet-stream', 'X-Filename': 'audio.flac', 'Idempotency-Key': key})
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request(method, path, body, headers)
        response = conn.getresponse()
        raw = response.read(MAX_RESPONSE + 1)
        if len(raw) > MAX_RESPONSE:
            raise RuntimeError('CPU adapter response exceeds bound')
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError('CPU adapter response invalid')
        return response.status, data
    except (OSError, http.client.HTTPException, ValueError):
        raise RuntimeError('CPU adapter response unavailable; remote outcome unknown') from None
    finally:
        conn.close()


def _timing(result, audio_hash):
    t = result.get('timing')
    allowed = {'schema','backend','device','compute_type','model','model_snapshot','engine_version',
               'weights_sha256','timebase','method','precision','duration_seconds','audio_sha256','segments'}
    if not isinstance(t, dict) or set(t) - allowed:
        raise RuntimeError('CPU timing missing or invalid')
    expected = {'schema':'stt-segments-v1', 'backend':BACKEND, 'device':'cpu', 'model':MODEL,
                'timebase':'decoded_audio_seconds', 'method':'faster_whisper_native_segments', 'audio_sha256':audio_hash}
    if any(t.get(k) != v for k,v in expected.items()):
        raise RuntimeError('CPU timing identity mismatch')
    if not all(isinstance(t.get(k), str) and 0 < len(t[k]) <= 256 for k in ('model_snapshot','engine_version')) or not re.fullmatch('[a-f0-9]{64}', str(t.get('weights_sha256', ''))):
        raise RuntimeError('CPU timing model provenance missing')
    duration = t.get('duration_seconds'); rows = t.get('segments')
    finite = lambda n: type(n) in (float, int) and math.isfinite(n)
    if not finite(duration) or not 0 < duration <= 600.1 or not isinstance(rows, list) or not 1 <= len(rows) <= 8192:
        raise RuntimeError('CPU timing bounds invalid')
    previous = -1
    for item in rows:
        if not isinstance(item, dict) or set(item) != {'start','end','text'}:
            raise RuntimeError('CPU segment fields invalid')
        a, b, text = item['start'], item['end'], item['text']
        if not finite(a) or not finite(b) or not 0 <= a < b <= duration + .05 or a < previous or not isinstance(text, str) or not text.strip() or '\0' in text or len(text.encode()) > 8192:
            raise RuntimeError('CPU segment invalid')
        previous = a
    transcript = result.get('transcript')
    if not isinstance(transcript, str) or not transcript.strip() or len(transcript.encode()) > MAX_RESPONSE or transcript.strip() != ' '.join(r['text'] for r in rows).strip():
        raise RuntimeError('CPU transcript mismatch')
    return transcript, t


def execute(audio, job_id, timeout):
    revision, host, port, token_path = configuration()
    token = _token(token_path)
    deadline = time.monotonic() + timeout
    def request(method, path, body=None, key=None):
        remaining = deadline - time.monotonic()
        if remaining <= 0: raise RuntimeError('CPU adapter timeout; remote outcome unknown')
        return _request(host, port, token, method, path, body, key, min(10, remaining))
    status, health = request('GET', '/health')
    if status != 200 or health.get('service') != 'openclaw-stt-http-adapter' or 'local-only-v1' not in health.get('routing_policies', []) or 'segments-v1' not in health.get('response_formats', []):
        raise RuntimeError('CPU adapter capability unavailable')
    audio_hash = hashlib.sha256(audio).hexdigest()
    status, ack = request('POST', PATH, audio, 'broker-voice-' + job_id)
    if status in (400,404,413,429):
        raise CPUAdmissionRejected('CPU adapter rejected admission')
    adapter_id = ack.get('job_id')
    if status not in (200,202) or ack.get('schema_version') != 1 or not isinstance(adapter_id, str) or not re.fullmatch('[a-f0-9]{32}', adapter_id) or ack.get('status_path') != PATH+'/'+adapter_id or ack.get('media_sha256') != audio_hash or ack.get('routing_policy') != 'local-only-v1' or ack.get('response_format') != 'segments-v1':
        raise RuntimeError('CPU adapter admission unconfirmed; remote outcome unknown')
    while True:
        status, data = request('GET', PATH+'/'+adapter_id)
        if status != 200 or data.get('schema_version') != 1 or data.get('job_id') != adapter_id or data.get('media_sha256') != audio_hash or data.get('routing_policy') != 'local-only-v1' or data.get('response_format') != 'segments-v1':
            raise RuntimeError('CPU adapter poll identity mismatch; remote outcome unknown')
        state = data.get('status')
        if state in ('queued','running'):
            remaining = deadline - time.monotonic()
            if remaining <= 0: raise RuntimeError('CPU adapter timeout; remote outcome unknown')
            time.sleep(min(1, remaining)); continue
        if state in ('failed','expired') or (state=='done' and data.get('result_http_status') != 200):
            raise CPUJobFailed('CPU adapter terminal failure')
        if state != 'done' or data.get('result_http_status') != 200 or not isinstance(data.get('result'), dict):
            raise RuntimeError('CPU adapter did not complete successfully')
        result = data['result']; routing = result.get('routing', {}); attempts = routing.get('attempts')
        if result.get('status') != 'success' or routing.get('policy') != 'local-only-v1' or routing.get('selected_backend') != BACKEND or not isinstance(attempts,list) or not any(isinstance(a,dict) and a.get('backend') == BACKEND and a.get('status') == 'success' for a in attempts) or any(not isinstance(a,dict) or a.get('backend') not in {BACKEND,'audio_activity','short_speech_check','media_recovery'} for a in attempts):
            raise RuntimeError('CPU adapter route proof invalid')
        text, timing = _timing(result, audio_hash)
        return {'text':text, 'timing':timing, 'voice_provenance':{
            'schema':'arch-broker-voice-result-v1', 'local_only':True, 'revision':revision,
            'profile':PROFILE, 'audio_sha256':audio_hash, 'response_format':'segments-v1',
            'backend':BACKEND, 'device':'cpu', 'model':MODEL,
            **{k:timing[k] for k in ('model_snapshot','engine_version','weights_sha256')}}}
