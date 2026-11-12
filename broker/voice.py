"""Bounded, server-configured voice transports; no caller-selected endpoints."""
import base64
import binascii
import io
import json
import os
import socket
import re
import urllib.parse
import urllib.request
import urllib.error
import wave
from contextlib import nullcontext
from . import voice_gpu
from . import voice_cpu, voice_contour

MAX_AUDIO_BYTES = 8 * 1024 * 1024
# Bounded whole-source contour only (2h 24kbit/s Opus < 22 MiB).
MAX_CONTOUR_AUDIO_BYTES = 32 * 1024 * 1024

# Fixed server-owned lanes: no caller endpoints or sibling fallback.
# Require reported model identity, not merely a configured port label.
_HTTP_WORKERS = {
    'voice-gigaam-v1': ('GIGAAM', '', 'gigaam-v3-e2e-rnnt'),
    'voice-whisper-gpu-v1': ('WHISPER_GPU', '', 'whisper-medium-int8-mainpc-cuda'),
    'voice-whisper-gpu-large-v3-v1': ('WHISPER_LARGE', '', 'whisper-large-v3-mainpc-cuda'),
}


def validate(payload, profile, *, _contour_transport=False):
    if profile == voice_contour.PROFILE:
        payload = voice_contour.validate(payload)
        validate({k:payload[k] for k in ("audio_base64","filename")}, 'voice-gigaam-v1', _contour_transport=True)
        return payload
    allowed = {"audio_base64", "filename", "response_format"} if profile == voice_cpu.PROFILE else {"audio_base64", "filename", "language"}
    if not isinstance(payload, dict) or set(payload) - allowed:
        raise ValueError("voice payload requires only audio_base64 and filename")
    encoded, filename = payload.get("audio_base64"), payload.get("filename")
    limit = MAX_CONTOUR_AUDIO_BYTES if _contour_transport else MAX_AUDIO_BYTES
    if not isinstance(encoded, str) or len(encoded) > ((limit + 2) // 3) * 4:
        raise ValueError("voice audio exceeds limit or is not base64")
    try:
        audio = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        raise ValueError("invalid audio base64") from None
    if not audio or len(audio) > limit:
        raise ValueError(f"voice audio must be nonempty and at most {limit} bytes")
    if not isinstance(filename, str) or not filename or len(filename) > 128 or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-' for c in filename):
        raise ValueError("invalid filename")
    if profile == voice_cpu.PROFILE:
        voice_cpu.configuration()
        if payload.get("response_format") != "segments-v1" or filename != "audio.flac" or not audio.startswith(b'fLaC'):
            raise ValueError("CPU voice requires FLAC and segments-v1")
        return {"audio_base64": encoded, "filename": filename, "response_format": "segments-v1"}
    if profile == "voice-wyoming-v1":
        try:
            with wave.open(io.BytesIO(audio), 'rb') as wav:
                if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate(), wav.getcomptype()) != (1, 2, 16000, 'NONE'):
                    raise ValueError("Wyoming requires 16 kHz mono PCM16 WAV")
        except (wave.Error, EOFError):
            raise ValueError("Wyoming requires 16 kHz mono PCM16 WAV") from None
    result = {"audio_base64": encoded, "filename": filename}
    if "language" in payload:
        language=payload["language"]
        if not isinstance(language,str) or not re.fullmatch(r'[a-z]{2,3}',language):
            raise ValueError('invalid explicit language')
        if profile=='voice-gigaam-v1' and language!='ru':raise ValueError('GigaAM language unsupported')
        result['language']=language
    return result


def safe_error_summary(error):
    # Never store raw backend response/exception text in a transcript receipt.
    if isinstance(error, (voice_contour.BackendRejected, voice_contour.LocalContourExhausted)):
        return 'ASR execution failed (' + type(error).__name__ + ': ' + error.safe_code + ')'
    return 'ASR execution failed (' + type(error).__name__ + ')'


def execute(profile, payload, timeout, *, _contour_transport=False):
    validate(payload, profile, _contour_transport=_contour_transport)
    if profile==voice_contour.PROFILE:
        def windows(model_profile, audio_payload, remaining):
            return execute(model_profile, audio_payload, remaining, _contour_transport=True)
        return voice_contour.execute(payload,timeout,windows)
    audio = base64.b64decode(payload['audio_base64'])
    if profile == 'voice-wyoming-v1':
        return _wyoming(audio, timeout)
    if profile not in _HTTP_WORKERS:
        raise RuntimeError('unknown voice profile')
    prefix, query, required_model = _HTTP_WORKERS[profile]
    url = os.environ.get('BROKER_' + prefix + '_URL', '').rstrip('/')
    token = os.environ.get('BROKER_' + prefix + '_TOKEN', '')
    if not url or not token:
        raise voice_contour.BackendUnconfigured(prefix + ' worker URL/token not configured')
    if payload.get('language') and prefix!='GIGAAM':
        status_request=urllib.request.Request(url+'/v1/status',headers={'Authorization':'Bearer '+token})
        with urllib.request.urlopen(status_request,timeout=min(5,timeout)) as response:
            status_raw=response.read(65537)
            if len(status_raw)>65536:raise voice_contour.BackendRejected('worker status too large')
            status=json.loads(status_raw)
        if not isinstance(status,dict) or status.get('language_override') is not True:
            raise voice_contour.BackendRejected('worker does not support explicit language')
        query='?'+urllib.parse.urlencode({'language':payload['language']})
    guard = voice_gpu.CURRENT.get()
    with guard.claim(profile,timeout) if guard else nullcontext(None) as claim:
        request = urllib.request.Request(url + '/v1/transcribe' + query, data=audio, method='POST', headers={
            'Authorization': 'Bearer ' + token, 'Content-Type': 'application/octet-stream',
            'Accept': 'application/json', 'X-Filename': payload['filename'],
            **({'X-Request-Id':claim.request_id} if claim else {}),
        })
        try:
            with urllib.request.urlopen(request, timeout=claim.remaining() if claim else timeout) as response:
                raw = response.read(2 * 1024 * 1024 + 1)
                if len(raw) > 2 * 1024 * 1024:
                    raise voice_contour.BackendRejected('voice worker response exceeds bound')
                result = json.loads(raw)
        except urllib.error.HTTPError as exc:
            # Preserve only worker-owned enumerated diagnostics, not raw response
            # text, credentials, transcript content or arbitrary exception details.
            code = 'backend_rejected'
            try:
                raw = exc.read(4097)
                data = json.loads(raw) if len(raw) <= 4096 else None
                if isinstance(data,dict):
                    for field in ('detail','error'):
                        candidate = data.get(field)
                        if isinstance(candidate,str) and candidate in voice_contour.SAFE_WORKER_CODES:
                            code = candidate
                            break
            except (OSError,ValueError):
                pass
            finally:
                exc.close()
            raise voice_contour.BackendRejected(code) from None
    if not isinstance(result, dict) or not isinstance(result.get('text'), str):
        raise voice_contour.BackendRejected('voice worker returned invalid result')
    if required_model is not None and result.get('model') != required_model:
        raise voice_contour.BackendRejected('voice worker model identity is invalid')
    return result


def _wyoming(audio, timeout):
    host = os.environ.get('BROKER_WYOMING_HOST', '127.0.0.1')
    port = int(os.environ.get('BROKER_WYOMING_PORT', '10300'))
    def send(sock, typ, data=None, payload=b''):
        body = json.dumps(data).encode() if data else b''
        header = {'type': typ}
        if body: header['data_length'] = len(body)
        if payload: header['payload_length'] = len(payload)
        sock.sendall(json.dumps(header).encode() + b'\n' + body + payload)
    with socket.create_connection((host, port), timeout=timeout) as sock, wave.open(io.BytesIO(audio), 'rb') as wav:
        sock.settimeout(timeout)
        send(sock, 'transcribe')
        meta = {'rate': 16000, 'width': 2, 'channels': 1}
        send(sock, 'audio-start', meta)
        while chunk := wav.readframes(16000):
            send(sock, 'audio-chunk', meta, chunk)
        send(sock, 'audio-stop')
        stream = sock.makefile('rb')
        while True:
            line = stream.readline(65537)
            if not line or len(line) > 65536: raise RuntimeError('invalid Wyoming response')
            header = json.loads(line)
            data_len, payload_len = header.get('data_length', 0), header.get('payload_length', 0)
            if not isinstance(data_len, int) or not isinstance(payload_len, int) or min(data_len, payload_len) < 0 or data_len + payload_len > 1048576:
                raise RuntimeError('invalid Wyoming event size')
            body = stream.read(data_len + payload_len)
            if len(body) != data_len + payload_len: raise RuntimeError('truncated Wyoming response')
            data = json.loads(body[:data_len]) if data_len else {}
            if header.get('type') == 'transcript':
                if not isinstance(data.get('text'), str): raise RuntimeError('invalid Wyoming transcript')
                return {'text': data['text']}
            if header.get('type') == 'error': raise RuntimeError('Wyoming worker error')
