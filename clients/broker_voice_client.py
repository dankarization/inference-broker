"""Opt-in standalone submitter for the broker's server-owned voice profiles."""
import base64
import json
import os
import socket
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

PROFILES = {'wyoming': 'voice-wyoming-v1', 'gigaam': 'voice-gigaam-v1',
            'whisper-gpu': 'voice-whisper-gpu-v1'}
MAX_AUDIO = 8 * 1024 * 1024


def submit(audio_path, backend, broker_url, *, external_id=None, source_item_id=None,
           attempts=3, timeout=10, retry_delay=0.25):
    """Submit once logically; retain external_id across retries and caller restarts.

    Callers should persist an explicit external_id before calling. The same ID
    and identical payload return the same broker job, including after a lost reply.
    """
    if os.environ.get('BROKER_VOICE_ENABLED', '0') != '1':
        raise RuntimeError('broker voice submission is disabled')
    if backend not in PROFILES:
        raise ValueError('unknown voice backend')
    if not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 1:
        raise ValueError('attempts must be positive')
    path = Path(audio_path)
    with path.open('rb') as audio_file:
        audio = audio_file.read(MAX_AUDIO + 1)
    if not audio or len(audio) > MAX_AUDIO:
        raise ValueError('audio must be nonempty and at most 8 MiB')
    external_id = external_id or str(uuid.uuid4())
    body = json.dumps({'source': 'local-voice', 'profile': PROFILES[backend],
                       'kind': 'transcribe', 'external_id': external_id,
                       'source_item_id': source_item_id,
                       'payload': {'filename': path.name,
                                   'audio_base64': base64.b64encode(audio).decode('ascii')}}).encode()
    request = urllib.request.Request(broker_url.rstrip('/') + '/v1/jobs', data=body,
                                     headers={'Content-Type': 'application/json'}, method='POST')
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                result = json.load(response)
            if not isinstance(result, dict) or not isinstance(result.get('id'), str):
                raise ValueError('invalid broker submission response')
            return result
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, TimeoutError, socket.timeout, ConnectionError):
            if attempt + 1 == attempts:
                raise
            time.sleep(retry_delay * (2 ** attempt))
