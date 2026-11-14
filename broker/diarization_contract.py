"""Acoustic speaker activity, not identity or word alignment. No ML imports."""
import math
import re

MODEL = 'nvidia/Nemotron-3-Diarization'
REVISION = '52a432672732425e4c19765b9fec9b5810e6f4ba'
WEIGHTS_SHA256 = '08456d9e22cd9a323c0364d98375f3746d6e68507ebb705cd46438c534c7a3a1'
PORTABLE_SHA256 = 'd0d32ae1bf6ee0534767a47b2aa0e9e2ff6fe39643465c0baae9301d04f6ed42'
PROFILE = 'voice-diarization-v1'
RUNTIME_REVISION = 'NeMo-Speech.cpp/v0.3.0/windows-x86_64-cuda'
OPTIONS = {'model_revision': REVISION, 'runtime_revision': RUNTIME_REVISION,
           'runtime_archive_sha256': PORTABLE_SHA256, 'weights_sha256': WEIGHTS_SHA256,
           'preset': 'v3-offline', 'device': 'cuda:0', 'max_speakers': 8}
MAX_AUDIO_BYTES = 32 * 1024 * 1024
MAX_SECONDS = 7200
TIMEBASE = 'decoded_audio_seconds'


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def validate_result(result, audio_sha256=None, duration=None):
    if (not isinstance(result, dict) or result.get('schema') != 'stt-diarization-v1'
            or result.get('model') != MODEL or result.get('model_revision') != REVISION
            or result.get('weights_sha256') != WEIGHTS_SHA256
            or result.get('device') != 'cuda' or result.get('timebase') != TIMEBASE
            or result.get('options') != OPTIONS):
        raise ValueError('diarization_provenance')
    proof = result.get('gpu_proof')
    if (not isinstance(proof, dict) or proof.get('selected_backend') != 'CUDA0'
            or proof.get('worker_platform') != 'win32'
            or proof.get('runtime_archive_sha256') != PORTABLE_SHA256
            or proof.get('process_tree_released') is not True
            or not number(proof.get('gpu_memory_delta_mib_peak'))
            or proof['gpu_memory_delta_mib_peak'] < 8
            or not isinstance(proof.get('runtime_binary_sha256'), str)
            or not re.fullmatch('[a-f0-9]{64}', proof['runtime_binary_sha256'])):
        raise ValueError('diarization_cuda_proof')
    digest = result.get('audio_sha256')
    if not isinstance(digest, str) or not re.fullmatch('[a-f0-9]{64}', digest):
        raise ValueError('diarization_audio')
    if audio_sha256 is not None and digest != audio_sha256:
        raise ValueError('diarization_audio')
    length = result.get('duration_seconds')
    if not number(length) or not 0 < length <= MAX_SECONDS:
        raise ValueError('diarization_duration')
    if duration is not None and (not number(duration) or abs(length - duration) > .05):
        raise ValueError('diarization_timebase')
    rows = result.get('segments')
    if not isinstance(rows, list) or len(rows) > 20000:
        raise ValueError('diarization_segments')
    previous = -1
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'start', 'end', 'speaker'}:
            raise ValueError('diarization_segment_fields')
        a, b, speaker = row['start'], row['end'], row['speaker']
        if not number(a) or not number(b) or not 0 <= a < b <= length or a < previous:
            raise ValueError('diarization_interval')
        if speaker is not None and (not isinstance(speaker, str) or not re.fullmatch('speaker_[0-7]', speaker)):
            raise ValueError('diarization_speaker_limit')
        previous = a
    return result


def attribute(timing, diarization, transcript):
    """Attribute only native ASR intervals completely covered by one speaker.

    Cross-speaker/uncovered intervals stay unknown. Never apportion text by time.
    """
    if not isinstance(timing, dict):
        return [], 'alignment_unavailable'
    if (timing.get('timebase') != TIMEBASE
            or timing.get('audio_sha256') != diarization['audio_sha256']
            or not number(timing.get('duration_seconds'))
            or abs(timing['duration_seconds'] - diarization['duration_seconds']) > .05):
        return [], 'alignment_timebase_mismatch'
    rows = timing.get('segments')
    if not isinstance(rows, list) or len(rows) > 8192 or any(not isinstance(r, dict) or not isinstance(r.get('text'), str) for r in rows):
        return [], 'alignment_invalid'
    if ' '.join(r['text'].strip() for r in rows) != transcript:
        return [], 'alignment_text_mismatch'
    attributed, previous = [], -1
    for row in rows:
        a, b = row.get('start'), row.get('end')
        if (not number(a) or not number(b) or not 0 <= a < b <= diarization['duration_seconds']
                or a < previous):
            return [], 'alignment_invalid'
        previous = a
        active = [r for r in diarization['segments'] if r['start'] < b and r['end'] > a]
        labels = sorted({r['speaker'] for r in active if r['speaker'] is not None})
        end = a
        for interval in active:
            if interval['start'] > end or interval['speaker'] is None:
                break
            end = max(end, interval['end'])
        known = len(labels) == 1 and end >= b and all(r['speaker'] is not None for r in active)
        # Track maximum end by label rather than quadratic interval pairs.
        ends, overlap = {}, False
        for interval in active:
            label = interval['speaker']
            if label is not None:
                if any(other != label and end > max(a, interval['start'])
                       for other, end in ends.items()):
                    overlap = True
                ends[label] = max(ends.get(label, a), min(b, interval['end']))
        attributed.append(dict(row, speaker=labels[0] if known else None,
                               candidates=labels, overlap=overlap))
    return attributed, 'segment_estimates' if attributed and all(r['speaker'] is not None for r in attributed) else 'partial_alignment'
