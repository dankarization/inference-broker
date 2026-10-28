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

FROZEN_SCHEMA = "telegram-nightly-frozen-v1"
MAX_FILE = 4 * 1024**2


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


def identifier(value):
    if not isinstance(value, str) or re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', value) is None:
        raise Closed('case/mode identity must be a bounded ASCII identifier')
    return value


def mode_label(think):
    return 'on' if think is True else 'off' if think is False else 'level-' + str(think)


def freeze_plan(cases_path, matrix_path, native_path):
    cases, cases_hash = read_json(cases_path)
    matrix, matrix_hash = read_json(matrix_path)
    native, native_hash = read_json(native_path)
    if not isinstance(cases, dict) or cases.get('schema') != 'telegram-nightly-cases-v1':
        raise Closed('accepted cases manifest schema required')
    if (not isinstance(matrix, dict) or matrix.get('schema') != 'telegram-nightly-matrix-v1'
            or matrix.get('source') != lane.SOURCE):
        raise Closed('accepted dedicated source matrix schema required')
    rows = cases.get('cases')
    models = matrix.get('models')
    if (not isinstance(rows, list) or len(rows) != 4 or not isinstance(models, list) or len(models) != 4
            or {m.get('profile') for m in models if isinstance(m, dict)} != NIGHTLY_PROFILE_NAMES):
        raise Closed('exactly four independent cases and four dedicated profiles required')
    if (not isinstance(native, dict) or native.get('schema') != 'telegram-nightly-native-v1'
            or not isinstance(native.get('models'), list) or len(native['models']) != 4):
        raise Closed('main-approved native metadata for four models required')
    metadata = {}
    for item in native['models']:
        model = item.get('model') if isinstance(item, dict) else None
        if model not in lane.MODEL_DIGESTS or item.get('digest') != lane.MODEL_DIGESTS[model] or model in metadata:
            raise Closed('native metadata exact model/digest mismatch')
        metadata[model] = lane.thinking_metadata(item)
    frozen_cases = []
    seen = set()
    seen_hash = set()
    for row in rows:
        case = identifier(row['case_id'])
        path = (Path(cases_path).resolve().parent / row['messages_path']).resolve()
        messages, digest = read_json(path)
        if case in seen or digest in seen_hash or digest != row['sha256']:
            raise Closed('case duplicate/changed input')
        seen.add(case)
        seen_hash.add(digest)
        frozen_cases.append(dict(case_id=case, messages_path=str(path), sha256=digest))
    cells = []
    for item in models:
        profile = item['profile']
        model = PROFILES[profile].model
        modes = item.get('modes')
        if not isinstance(modes, list) or len(modes) != 2:
            raise Closed('exactly two explicit modes per model required')
        if not any(isinstance(mode, dict) and lane.supports(metadata[model], mode.get('think')) for mode in modes):
            raise Closed('at least one genuinely supported mode per model required')
        labels = set()
        for mode in modes:
            think = mode.get('think') if isinstance(mode, dict) else None
            if not lane.think_value(think) or mode.get('label') != mode_label(think):
                raise Closed('native labels must be off/on/level-N; low is never off')
            label = mode['label']
            if label in labels:
                raise Closed('duplicate mode')
            labels.add(label)
            supported = lane.supports(metadata[model], think)
            if type(mode.get('supported')) is not bool or mode['supported'] is not supported:
                raise Closed('mode support must match accepted native metadata exactly')
            for case in frozen_cases:
                messages, _ = read_json(case['messages_path'])
                # Unsupported off cells remain explicit but never get a payload/POST.
                if supported:
                    payload = lane.normalize_request(dict(messages=messages, think=think,
                        native_thinking=metadata[model]), profile)
                else:
                    payload = dict(messages=messages, think=think, native_thinking=metadata[model],
                                   options=dict(lane.OPTIONS), truncate=False, shift=False)
                input_hash, input_bytes = content_evidence(payload)
                cell_id = case['case_id'] + '.' + profile + '.' + label
                identity = dict(schema=FROZEN_SCHEMA, source=lane.SOURCE, cell_id=cell_id,
                                input_hash=input_hash, profile=profile, native=metadata[model])
                key = 'tn-v1-' + sha(encode(identity))
                cells.append(dict(cell_id=cell_id, case_id=case['case_id'], profile=profile,
                    mode=label, think=think, supported=supported, external_id=key,
                    source_item_id='tn-v1-' + case['case_id'], input_hash=input_hash, input_bytes=input_bytes))
    if len(cells) != lane.MAX_CALLS:
        raise Closed('bounded 4x2x4 plan required')
    return dict(schema=FROZEN_SCHEMA, contract=lane.contract(), cases_sha256=cases_hash,
                matrix_sha256=matrix_hash, native_sha256=native_hash, native=metadata,
                cases=frozen_cases, cells=cells)


def load_frozen(path):
    frozen, digest = read_json(path)
    if (not isinstance(frozen, dict) or frozen.get('schema') != FROZEN_SCHEMA
            or frozen.get('contract') != lane.contract() or len(frozen.get('cells', [])) != 32
            or len(frozen.get('cases', [])) != 4):
        raise Closed('frozen schema/local contract mismatch')
    # Recheck payloads, keys and modes instead of trusting editable cell metadata.
    seen = set()
    for cell in frozen['cells']:
        if (cell['cell_id'] in seen or cell['profile'] not in NIGHTLY_PROFILE_NAMES
                or cell['cell_id'] != cell['case_id'] + '.' + cell['profile'] + '.' + cell['mode']):
            raise Closed('frozen cell identity mismatch')
        seen.add(cell['cell_id'])
        request_for(frozen, cell)
    return frozen, digest


def request_for(frozen, cell):
    case = next((c for c in frozen['cases'] if c['case_id'] == cell['case_id']), None)
    if case is None:
        raise Closed('frozen case missing')
    messages, digest = read_json(case['messages_path'])
    if digest != case['sha256']:
        raise Closed('frozen case bytes changed')
    model = PROFILES[cell['profile']].model
    payload = dict(messages=messages, think=cell['think'], native_thinking=frozen['native'][model],
                   options=dict(lane.OPTIONS), truncate=False, shift=False)
    if cell['supported']:
        payload = lane.normalize_request(payload, cell['profile'])
    metadata = lane.thinking_metadata({'thinking': frozen['native'][model]})
    identity = dict(schema=FROZEN_SCHEMA, source=lane.SOURCE, cell_id=cell['cell_id'],
                    input_hash=content_evidence(payload)[0], profile=cell['profile'], native=metadata)
    expected = dict(external_id='tn-v1-' + sha(encode(identity)), input_hash=content_evidence(payload)[0],
                    input_bytes=content_evidence(payload)[1], mode=mode_label(cell['think']),
                    supported=lane.supports(metadata, cell['think']), source_item_id='tn-v1-' + case['case_id'])
    if digest != case['sha256'] or any(not lane.same_value(cell[k], v) for k, v in expected.items()):
        raise Closed('frozen payload/identity/support changed')
    return dict(source=lane.SOURCE, profile=cell['profile'], kind='chat', payload=payload,
                external_id=cell['external_id'], source_item_id=cell['source_item_id'])


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
    if not cell['supported']:
        raise Closed('native mode unsupported: no POST')
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
        if args.command in ('freeze', 'dry-run'):
            frozen = freeze_plan(args.cases, args.matrix, args.native)
            if args.command == 'freeze':
                new_json(args.out, frozen)
            print(json.dumps(dict(planned_cells=len(frozen['cells']),
                supported_cells=sum(c['supported'] for c in frozen['cells']), max_posts=32,
                unsupported_modes=sorted({c['profile'] + ':' + c['mode'] for c in frozen['cells'] if not c['supported']}),
                off_unsupported_models=sorted(m for m, v in frozen['native'].items() if not lane.supports(v, False)))))
            return 0
        frozen, digest = load_frozen(args.frozen)
        cells = [c for c in frozen['cells'] if c['cell_id'] == args.cell]
        if len(cells) != 1:
            raise Closed('one exact recorded cell required')
        cell = cells[0]
        if args.command == 'request':
            if not cell['supported']:
                raise Closed('unsupported mode: no request')
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
