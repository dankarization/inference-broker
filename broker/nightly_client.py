"""Finite main-owned request/collect helper; stdlib only, no scheduler or inference.

Journal the intent before the only POST; journal job UUID/hash BEFORE validating
any optional result. Unknown POSTs are permanently non-replayable in this state.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from . import nightly_eval as lane
from .profiles import NIGHTLY_PROFILE_NAMES, PROFILES
from .storage import canonical_json_bytes, content_evidence

FROZEN_SCHEMA = "telegram-nightly-frozen-v2"
LEGACY_FROZEN_SCHEMA = "telegram-nightly-frozen-v1"
NATIVE_SCHEMA = "telegram-nightly-native-v2"
MAX_FILE = 4 * 1024**2
ASTRA_SCHEMA = "telegram-nightly-model-screen-fixtures-v1"
ASTRA_MANIFEST_SHA256 = "5f8046556912ad1e7c09239717c315eb4cde39f889e1640dfb72fca6f7cad0e8"
CASE_INPUTS = {
    "case-01-porta": "782f3b71d0b8acb34f692efa61e6cd11e2fa3754594fea848a140f27c66d1574",
    "case-02-kp": "61d0fd1bece24bbc88d3a0cbe9f59d28fb9a40b0f336b7015c7913496b3c144e",
    "case-03-pm": "63a4c5e22f3971f93b3ed80c6d209fac9e4d699bc086cd8dd2ec537c4df81d87",
    "case-04-s92": "f8fca9dcc93da68802bc9528aa7b310600b5a8641475cc270387704b3c6b040c",
}


class Closed(RuntimeError):
    pass


class Uncertain(Closed):
    pass


def encode(value):
    return canonical_json_bytes(value)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    path = Path(path)
    with path.open('rb') as stream:
        raw = stream.read(MAX_FILE + 1)
    if len(raw) > MAX_FILE:
        raise Closed('input file exceeds bound')
    return json.loads(raw), sha(raw)


def new_json(path, value):
    """Exclusive + fsync file AND directory; never replace an accepted artifact."""
    path = Path(path)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(encode(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def mode_label(think):
    return 'on' if think is True else 'off' if think is False else 'level-' + str(think)


def read_messages(path):
    raw, digest = read_json(path)
    if not isinstance(raw, dict) or set(raw) != {'messages'}:
        raise Closed('exact messages.json object required; no extra model-facing fields')
    lane.validate_messages(raw['messages'])
    return raw['messages'], digest


def accepted_cases(cases_path):
    """Read only manifest + four text messages files; never follow embedded URLs.

    Do not open source documents, references, rubric, attachments or proof paths
    from the manifest. Main/reviewer owns all those bytes, not this runner.
    """
    manifest_path = Path(cases_path).resolve()
    manifest, digest = read_json(manifest_path)
    if (digest != ASTRA_MANIFEST_SHA256 or not isinstance(manifest, dict)
            or manifest.get('schema') != ASTRA_SCHEMA or manifest.get('case_count') != 4):
        raise Closed('exact accepted Astra manifest required')
    rows = manifest.get('cases')
    if not isinstance(rows, list) or len(rows) != 4:
        raise Closed('exact four accepted Astra cases required')
    cases = []
    seen = set()
    for row in rows:
        case = row.get('case_id') if isinstance(row, dict) else None
        if case not in CASE_INPUTS or case in seen:
            raise Closed('unexpected/duplicate Astra case')
        seen.add(case)
        relative = case + '/messages.json'
        if row.get('runner_input') != relative or row.get('runner_input_sha256') != CASE_INPUTS[case]:
            raise Closed('only pinned case-*/messages.json is model-facing')
        path = manifest_path.parent / relative
        if path.is_symlink() or path.parent.is_symlink() or path.resolve() != path:
            raise Closed('model-facing input must remain inside accepted fixture root')
        messages, message_hash = read_messages(path)
        if message_hash != CASE_INPUTS[case]:
            raise Closed('accepted case bytes changed')
        cases.append(dict(case_id=case, messages_path=str(path), sha256=message_hash))
    return cases, digest


def observed_thinking(raw):
    """Project native show metadata, never infer controls from capability/name."""
    if not isinstance(raw, dict):
        raise Closed('native show response must be an object')
    capabilities = raw.get('capabilities')
    capability = ('thinking' in capabilities if isinstance(capabilities, list)
                  and all(isinstance(v, str) for v in capabilities) else None)
    try:
        metadata = lane.thinking_metadata(raw)
    except ValueError:
        return dict(metadata_state='unknown', thinking=None, thinking_capability=capability,
                    unknown_reason='thinking_object_missing' if 'thinking' not in raw
                    else 'thinking_metadata_invalid')
    return dict(metadata_state='known', thinking=metadata, thinking_capability=capability,
                unknown_reason=None)


def validate_observation(item):
    if not isinstance(item, dict) or set(item) != {
            'model', 'digest', 'metadata_state', 'thinking', 'thinking_capability', 'unknown_reason'}:
        raise Closed('exact versioned native observation required')
    model = item['model']
    if model not in lane.MODEL_DIGESTS or item['digest'] != lane.MODEL_DIGESTS[model]:
        raise Closed('native metadata exact model/digest mismatch')
    if item['thinking_capability'] is not None and type(item['thinking_capability']) is not bool:
        raise Closed('invalid observed capability scalar')
    if item['metadata_state'] == 'known':
        metadata = lane.thinking_metadata(item)
        if item['thinking'] != metadata or item['unknown_reason'] is not None:
            raise Closed('known metadata must be projected exactly')
        return metadata
    if (item['metadata_state'] != 'unknown' or item['thinking'] is not None
            or item['unknown_reason'] not in {'thinking_object_missing', 'thinking_metadata_invalid'}):
        raise Closed('unknown metadata must not invent values/defaults')
    return None


def cell_contract(case, profile, think, metadata, schema):
    if not lane.think_value(think):
        raise Closed('native boolean/level proposal required')
    label = mode_label(think)
    cell_id = case['case_id'] + '.' + profile + '.' + label
    supported = None if metadata is None else lane.supports(metadata, think)
    expected = dict(cell_id=cell_id, case_id=case['case_id'], profile=profile, mode=label,
                    think=think, supported=supported)
    if schema == FROZEN_SCHEMA:
        expected['status'] = ('blocked_native_metadata' if metadata is None else
                              'supported' if supported else 'unsupported_native_mode')
    if metadata is None:
        # A blocked planning row is NOT a payload, idempotency key or admission.
        expected.update(external_id=None, source_item_id=None, input_hash=None, input_bytes=None)
        return expected, None
    messages, digest = read_messages(case['messages_path'])
    if digest != case['sha256']:
        raise Closed('frozen case bytes changed')
    metadata = lane.thinking_metadata({'thinking': metadata})
    payload = dict(messages=messages, think=think, native_thinking=metadata,
                   options=dict(lane.OPTIONS), truncate=False, shift=False)
    if supported:
        payload = lane.normalize_request(payload, profile)
    input_hash, input_bytes = content_evidence(payload)
    identity = dict(schema=schema, source=lane.SOURCE, cell_id=cell_id,
                    input_hash=input_hash, profile=profile, native=metadata)
    expected.update(external_id='tn-v1-' + sha(encode(identity)), input_hash=input_hash,
                    input_bytes=input_bytes, source_item_id='tn-v1-' + case['case_id'])
    return expected, dict(source=lane.SOURCE, profile=profile, kind='chat', payload=payload,
                         external_id=expected['external_id'], source_item_id=expected['source_item_id'])


def freeze_plan(cases_path, matrix_path, native_path):
    frozen_cases, cases_hash = accepted_cases(cases_path)
    matrix, matrix_hash = read_json(matrix_path)
    native, native_hash = read_json(native_path)
    if (not isinstance(matrix, dict) or matrix.get('schema') not in {
            'telegram-nightly-matrix-v1', 'telegram-nightly-matrix-v2'}
            or matrix.get('source') != lane.SOURCE):
        raise Closed('accepted dedicated source matrix schema required')
    models = matrix.get('models')
    if (not isinstance(models, list) or len(models) != 4
            or {m.get('profile') for m in models if isinstance(m, dict)} != NIGHTLY_PROFILE_NAMES):
        raise Closed('exactly four independent cases and four dedicated profiles required')
    if (not isinstance(native, dict) or native.get('schema') not in {
            'telegram-nightly-native-v1', NATIVE_SCHEMA}
            or not isinstance(native.get('models'), list) or len(native['models']) != 4):
        raise Closed('main-approved native metadata for four models required')
    metadata, observations = {}, {}
    for item in native['models']:
        model = item.get('model') if isinstance(item, dict) else None
        if model not in lane.MODEL_DIGESTS or item.get('digest') != lane.MODEL_DIGESTS[model] or model in metadata:
            raise Closed('native metadata exact model/digest mismatch')
        if native['schema'] == NATIVE_SCHEMA:
            metadata[model] = validate_observation(item)
            observations[model] = item
        else:
            # Legacy v1 meant fully known controls; missing/invalid stays fatal.
            metadata[model] = lane.thinking_metadata(item)
            observations[model] = dict(model=model, digest=item['digest'],
                metadata_state='known', thinking=metadata[model], thinking_capability=None, unknown_reason=None)
    cells = []
    for item in models:
        profile = item['profile']
        model = PROFILES[profile].model
        modes = item.get('modes')
        if not isinstance(modes, list) or len(modes) != 2:
            raise Closed('exactly two explicit modes per model required')
        if metadata[model] is None:
            if matrix['schema'] != 'telegram-nightly-matrix-v2':
                raise Closed('unknown controls require explicit v2 planning rows')
        elif not any(isinstance(mode, dict) and lane.supports(metadata[model], mode.get('think')) for mode in modes):
            raise Closed('at least one genuinely supported mode per known model required')
        labels = set()
        for mode in modes:
            think = mode.get('think') if isinstance(mode, dict) else None
            if not lane.think_value(think) or mode.get('label') != mode_label(think):
                raise Closed('native labels must be off/on/level-N; low is never off')
            if mode['label'] in labels:
                raise Closed('duplicate mode')
            labels.add(mode['label'])
            supported = None if metadata[model] is None else lane.supports(metadata[model], think)
            if 'supported' not in mode or not lane.same_value(mode['supported'], supported):
                raise Closed('support must match native metadata: null means unknown, not unsupported')
            for case in frozen_cases:
                cell, _ = cell_contract(case, profile, think, metadata[model], FROZEN_SCHEMA)
                cells.append(cell)
    if len(cells) != lane.MAX_CALLS:
        raise Closed('bounded 4x2x4 plan required')
    return dict(schema=FROZEN_SCHEMA, contract=lane.contract(), cases_sha256=cases_hash,
                cases_manifest_path=str(Path(cases_path).resolve()),
                matrix_sha256=matrix_hash, native_sha256=native_hash, native=metadata,
                native_observations=observations, cases=frozen_cases, cells=cells)


def validate_cell(frozen, cell):
    case = next((c for c in frozen['cases'] if c['case_id'] == cell['case_id']), None)
    if case is None or cell['profile'] not in NIGHTLY_PROFILE_NAMES:
        raise Closed('frozen case/profile missing')
    model = PROFILES[cell['profile']].model
    expected, request = cell_contract(case, cell['profile'], cell['think'],
                                      frozen['native'][model], frozen['schema'])
    if any(k not in cell or not lane.same_value(cell[k], v) for k, v in expected.items()):
        raise Closed('frozen payload/identity/support changed')
    return request


def load_frozen(path):
    frozen, digest = read_json(path)
    if (not isinstance(frozen, dict) or frozen.get('schema') not in {FROZEN_SCHEMA, LEGACY_FROZEN_SCHEMA}
            or frozen.get('contract') != lane.contract() or len(frozen.get('cells', [])) != 32
            or len(frozen.get('cases', [])) != 4):
        raise Closed('frozen schema/local contract mismatch')
    cases, cases_hash = accepted_cases(frozen['cases_manifest_path'])
    if cases != frozen['cases'] or cases_hash != frozen['cases_sha256']:
        raise Closed('frozen cases differ from exact accepted Astra fixture root')
    if not isinstance(frozen.get('native'), dict) or set(frozen['native']) != set(lane.MODEL_DIGESTS):
        raise Closed('frozen native models changed')
    if frozen['schema'] == FROZEN_SCHEMA:
        observations = frozen.get('native_observations')
        if not isinstance(observations, dict) or set(observations) != set(lane.MODEL_DIGESTS):
            raise Closed('frozen native observations changed')
        for model, item in observations.items():
            if (not isinstance(item, dict) or item.get('model') != model
                    or validate_observation(item) != frozen['native'][model]):
                raise Closed('frozen known/unknown observation changed')
    else:
        for item in frozen['native'].values():
            lane.thinking_metadata({'thinking': item})
    case_ids = {c['case_id'] for c in cases}
    for profile in NIGHTLY_PROFILE_NAMES:
        cells = [c for c in frozen['cells'] if c['profile'] == profile]
        modes = {c['mode'] for c in cells}
        if len(cells) != 8 or len(modes) != 2 or any(
                {c['case_id'] for c in cells if c['mode'] == mode} != case_ids for mode in modes):
            raise Closed('frozen 4x2x4 matrix changed')
    seen = set()
    for cell in frozen['cells']:
        if cell['cell_id'] in seen:
            raise Closed('frozen cell identity mismatch')
        seen.add(cell['cell_id'])
        validate_cell(frozen, cell)
    return frozen, digest


def request_for(frozen, cell):
    request = validate_cell(frozen, cell)
    if request is None:
        raise Closed('blocked_native_metadata: no payload or POST')
    if cell['supported'] is not True:
        raise Closed('native mode unsupported: no request or POST')
    return request


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise Uncertain('redirect refused; reconcile only')


class Transport:
    def __init__(self, base='http://192.168.2.19:8088', token=None):
        url = urlsplit(base)
        if (url.scheme != 'http' or url.hostname not in {'192.168.2.19', '127.0.0.1', 'localhost', '::1'}
                or url.username or url.password or url.path not in {'', '/'} or url.query or url.fragment):
            raise Closed('fixed LAN broker or loopback URL required')
        if token is not None and (not isinstance(token, str) or not token or '\n' in token or '\r' in token):
            raise Closed('invalid runtime auth')
        self.base, self.token = base.rstrip('/'), token
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def request(self, path, body=None):
        headers = {'Content-Type': 'application/json'}
        if self.token:
            headers['Authorization'] = 'Bearer ' + self.token
        req = Request(self.base + path, data=None if body is None else encode(body), headers=headers)
        try:
            with self.opener.open(req, timeout=30) as response:
                raw = response.read(MAX_FILE + 1)
            if len(raw) > MAX_FILE:
                raise Uncertain('response exceeds byte bound')
            return json.loads(raw)
        except HTTPError as error:
            status = error.code
            error.close()  # Never read/store/log raw error bodies.
            raise Uncertain('HTTP status ' + str(status) + '; GET reconcile only') from None
        except (URLError, OSError, ValueError):
            raise Uncertain('transport/JSON outcome unknown; GET reconcile only') from None


class Journal:
    def __init__(self, path, frozen_hash, endpoint):
        path = Path(path)
        # sqlite sidecars inherit a private directory and restrictive umask.
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if path.parent.stat().st_mode & 0o077:
            raise Closed('journal directory must be private (chmod 700)')
        self.db = sqlite3.connect(path)
        os.chmod(path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS owner (id INTEGER PRIMARY KEY CHECK(id=1), frozen TEXT, endpoint TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS cells (cell_id TEXT PRIMARY KEY, external_id TEXT UNIQUE, input_hash TEXT, '
                        'job_id TEXT UNIQUE, phase TEXT NOT NULL, receipt TEXT, collected TEXT)')
        with self.db:
            row = self.db.execute('SELECT * FROM owner').fetchone()
            if row is not None and (row['frozen'] != frozen_hash or row['endpoint'] != endpoint):
                raise Closed('journal bound to different frozen plan/broker')
            self.db.execute('INSERT OR IGNORE INTO owner VALUES (1,?,?)', (frozen_hash, endpoint))
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def close(self):
        self.db.close()

    def row(self, cell):
        return self.db.execute('SELECT * FROM cells WHERE cell_id=?', (cell['cell_id'],)).fetchone()

    def intent(self, cell):
        # BEGIN IMMEDIATE serializes separate helper processes. A committed intent
        # blocks every later POST, even after a crash before/after network send.
        self.db.execute('BEGIN IMMEDIATE')
        try:
            if self.row(cell) is not None:
                raise Closed('cell already has durable intent; GET reconcile/collect only')
            if self.db.execute('SELECT count(*) FROM cells').fetchone()[0] >= 32:
                raise Closed('32-call journal cap')
            if self.db.execute("SELECT 1 FROM cells WHERE phase NOT IN ('terminal') LIMIT 1").fetchone():
                raise Closed('another cell is outstanding/uncertain; collect it first')
            self.db.execute('INSERT INTO cells VALUES (?,?,?,NULL,?,NULL,NULL)',
                (cell['cell_id'], cell['external_id'], cell['input_hash'], 'intent'))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def record_id(self, cell, raw):
        # Minimal UUID/hash receipt is committed before projection/result handling.
        if not isinstance(raw, dict):
            raise Uncertain('missing admission receipt')
        job_id = raw.get('id')
        try:
            if not isinstance(job_id, str) or str(uuid.UUID(job_id)) != job_id:
                raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise Uncertain('invalid job UUID; GET reconcile only') from None
        receipt = dict(job_id=job_id, external_id=cell['external_id'], expected_input_hash=cell['input_hash'])
        raw_hash = raw.get('input_hash')
        if isinstance(raw_hash, str) and re.fullmatch(r'sha256:[a-f0-9]{64}', raw_hash):
            receipt['observed_input_hash'] = raw_hash
        with self.db:
            row = self.row(cell)
            if row is None or row['job_id'] not in (None, job_id):
                raise Uncertain('receipt job identity conflict')
            self.db.execute('UPDATE cells SET job_id=?,phase=?,receipt=? WHERE cell_id=?',
                            (job_id, 'recorded', json.dumps(receipt), cell['cell_id']))
        return receipt


def identity(raw, cell):
    expected = dict(source=lane.SOURCE, kind='chat', profile=cell['profile'], external_id=cell['external_id'],
                    source_item_id=cell['source_item_id'], input_hash=cell['input_hash'], input_bytes=cell['input_bytes'])
    if (not isinstance(raw, dict) or raw.get('projection') != lane.SCHEMA
            or any(not lane.same_value(raw.get(k), v) for k, v in expected.items())):
        raise Uncertain('job identity/hash/schema mismatch; no replay')
    result = lane.projection(raw)
    if raw.get('state') == 'completed':
        if 'result' not in result or 'execution_evidence' not in result:
            raise Uncertain('completed final/evidence missing')
        evidence = result['execution_evidence']
        if not lane.same_value(evidence['think'], cell['think']):
            raise Uncertain('executed native think mismatch')
    return result


def admit(frozen, cell, journal, transport):
    request = request_for(frozen, cell)
    if journal.row(cell) is not None:
        raise Closed('cell already has durable intent; GET reconcile/collect only')
    if transport.request('/v1/nightly-eval/contract') != lane.contract():
        raise Closed('runtime contract differs from frozen/local contract')
    policy = transport.request('/v1/sources')
    entry = policy.get('policy', {}).get('sources', {}).get(lane.SOURCE) if isinstance(policy, dict) else None
    if not isinstance(entry, dict) or entry.get('enabled') is not True or entry.get('admission_allowed') is not True:
        raise Closed('dedicated source is not explicitly enabled/admitting')
    journal.intent(cell)
    raw = transport.request('/v1/jobs', request)  # Exactly one POST; no loop/retry.
    receipt = journal.record_id(cell, raw)
    identity(raw, cell)
    return receipt


def collect(frozen, cell, journal, transport, reconcile=False):
    request_for(frozen, cell)
    row = journal.row(cell)
    if row is None:
        raise Closed('GET collection requires a recorded local intent')
    if row['job_id'] is None:
        if not reconcile:
            raise Closed('unknown POST outcome; explicit GET reconcile required')
        raw = transport.request('/v1/correlations?' + urlencode(dict(source=lane.SOURCE, external_id=cell['external_id'], limit=2)))
        rows = raw.get('jobs') if isinstance(raw, dict) else None
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
            raise Uncertain('absent/ambiguous correlation does NOT permit resubmission')
        job_id = rows[0].get('job_id')
        # Validate UUID before putting anything into an endpoint path.
        try:
            if not isinstance(job_id, str) or str(uuid.UUID(job_id)) != job_id:
                raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise Uncertain('invalid correlation UUID') from None
        journal.record_id(cell, dict(id=job_id))
        row = journal.row(cell)
    raw = transport.request('/v1/jobs/' + row['job_id'])
    if not isinstance(raw, dict) or raw.get('id') != row['job_id']:
        raise Uncertain('recorded job ID mismatch')
    result = identity(raw, cell)
    if result.get('execution_evidence', {}).get('native_thinking', frozen['native'][PROFILES[cell['profile']].model]) != frozen['native'][PROFILES[cell['profile']].model]:
        raise Uncertain('native metadata drift from frozen plan')
    # These derived values are safe metadata, not raw attempts/errors/policies.
    if result['state'] not in {'queued', 'running', 'cancel_requested', 'completed', 'failed', 'cancelled', 'expired'}:
        raise Uncertain('unknown job state')
    attempts = result.get('attempt_count')
    if type(attempts) is not int or not 0 <= attempts <= 1:
        raise Uncertain('unexpected re-execution count')
    terminal = result['state'] in {'completed', 'failed', 'cancelled', 'expired'}
    with journal.db:
        journal.db.execute('UPDATE cells SET phase=?,collected=? WHERE cell_id=?',
            ('terminal' if terminal else 'recorded', encode(result).decode(), cell['cell_id']))
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('verify-cases')
    p.add_argument('--cases', required=True)
    for command in ('dry-run', 'freeze'):
        p = sub.add_parser(command)
        p.add_argument('--cases', required=True)
        p.add_argument('--matrix', required=True)
        p.add_argument('--native', required=True)
        if command == 'freeze':
            p.add_argument('--out', required=True)
    for command in ('request', 'admit', 'reconcile', 'collect'):
        p = sub.add_parser(command)
        p.add_argument('--frozen', required=True)
        p.add_argument('--cell', required=True)
        if command == 'request':
            p.add_argument('--out', required=True)
        else:
            p.add_argument('--journal', required=True)
            p.add_argument('--base-url', default='http://192.168.2.19:8088')
            p.add_argument('--token-env', help='optional runtime bearer env name; value never persisted')
            if command in ('collect', 'reconcile'):
                p.add_argument('--out', help='exclusive private allowlisted result JSON; never raw HTTP')
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        if args.command == 'verify-cases':
            cases, digest = accepted_cases(args.cases)
            print(json.dumps(dict(case_count=len(cases), manifest_sha256=digest,
                inputs=[dict(case_id=c['case_id'], sha256=c['sha256']) for c in cases], network_calls=0)))
            return 0
        if args.command in ('freeze', 'dry-run'):
            frozen = freeze_plan(args.cases, args.matrix, args.native)
            if args.command == 'freeze':
                new_json(args.out, frozen)
            print(json.dumps(dict(planned_cells=len(frozen['cells']),
                supported_cells=sum(c['supported'] is True for c in frozen['cells']), max_posts=32,
                blocked_native_metadata_cells=sum(c.get('status') == 'blocked_native_metadata' for c in frozen['cells']),
                unsupported_modes=sorted({c['profile'] + ':' + c['mode'] for c in frozen['cells'] if c['supported'] is False}),
                off_unsupported_models=sorted(m for m, v in frozen['native'].items() if v is not None and not lane.supports(v, False)),
                unknown_native_models=sorted(m for m, v in frozen['native'].items() if v is None))))
            return 0
        frozen, digest = load_frozen(args.frozen)
        cells = [c for c in frozen['cells'] if c['cell_id'] == args.cell]
        if len(cells) != 1:
            raise Closed('one exact recorded cell required')
        cell = cells[0]
        if args.command == 'request':
            new_json(args.out, request_for(frozen, cell))
            print(json.dumps(dict(cell_id=cell['cell_id'], input_hash=cell['input_hash'])))
            return 0
        token = os.environ.get(args.token_env) if args.token_env else None
        if args.token_env and not token:
            raise Closed('runtime auth env missing')
        transport = Transport(args.base_url, token)
        journal = Journal(args.journal, digest, transport.base)
        try:
            result = admit(frozen, cell, journal, transport) if args.command == 'admit' else collect(
                frozen, cell, journal, transport, reconcile=args.command == 'reconcile')
            if getattr(args, 'out', None):
                new_json(args.out, result)
            # Never print final text or private inputs; result remains in private journal.
            print(json.dumps({k: result[k] for k in ('job_id', 'id', 'state', 'expected_input_hash') if k in result}))
            if result.get('state') in {'failed', 'cancelled', 'expired'} or (result.get('state') == 'completed' and not result.get('result', {}).get('acceptable')):
                print('quality-admission rejected: empty/length/unknown final', file=sys.stderr)
                return 3
            return 0
        finally:
            journal.close()
    except (Closed, ValueError, TypeError, KeyError, OSError, sqlite3.Error, StopIteration):
        # No exception repr/raw body/traceback can leak prompts, auth or reasoning.
        print('operation blocked/uncertain; inspect private journal, GET reconcile only; no automatic retry', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
