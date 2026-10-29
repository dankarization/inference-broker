"""Bounded, server-configured voice transports; no caller-selected endpoints."""
import base64
import binascii
import io
import json
import os
import socket
import urllib.request
import wave
from . import voice_cpu

MAX_AUDIO_BYTES = 8 * 1024 * 1024


def validate(payload, profile):
    allowed = {"audio_base64", "filename", "response_format"} if profile == voice_cpu.PROFILE else {"audio_base64", "filename"}
    if not isinstance(payload, dict) or set(payload) - allowed:
        raise ValueError("voice payload requires only audio_base64 and filename")
    encoded, filename = payload.get("audio_base64"), payload.get("filename")
    if not isinstance(encoded, str) or len(encoded) > ((MAX_AUDIO_BYTES + 2) // 3) * 4:
        raise ValueError("voice audio exceeds limit or is not base64")
    try:
        audio = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        raise ValueError("invalid audio base64") from None
    if not audio or len(audio) > MAX_AUDIO_BYTES:
        raise ValueError("voice audio must be nonempty and at most 8 MiB")
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
    return {"audio_base64": encoded, "filename": filename}


def execute(profile, payload, timeout):
    validate(payload, profile)
    audio = base64.b64decode(payload['audio_base64'])
    if profile == 'voice-wyoming-v1':
        return _wyoming(audio, timeout)
    prefix = 'GIGAAM' if profile == 'voice-gigaam-v1' else 'WHISPER_GPU'
    url = os.environ.get('BROKER_' + prefix + '_URL', '').rstrip('/')
    token = os.environ.get('BROKER_' + prefix + '_TOKEN', '')
    if not url or not token:
        raise RuntimeError(prefix + ' worker URL/token not configured')
    endpoint = '/v1/transcribe' + ('?profile=background&language=ru' if prefix == 'WHISPER_GPU' else '')
    request = urllib.request.Request(url + endpoint, data=audio, method='POST', headers={
        'Authorization': 'Bearer ' + token, 'Content-Type': 'application/octet-stream',
        'Accept': 'application/json', 'X-Filename': payload['filename'],
    })
    with urllib.request.urlopen(request, timeout=timeout) as response:
        result = json.loads(response.read(1024 * 1024 + 1))
    if not isinstance(result, dict) or not isinstance(result.get('text'), str):
        raise RuntimeError('voice worker returned invalid result')
    if prefix == 'GIGAAM' and result.get('model') != 'gigaam-v3-e2e-rnnt':
        raise RuntimeError('GigaAM worker identity is invalid')
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
