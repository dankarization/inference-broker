"""Opt-in Nextcloud-only dispatch gate; no policy writes or inference changes.

Owner transaction fence spans aggregate read -> SQLite lease COMMIT only.
All calls use the restricted owner principal/aggregate, never migrator credentials.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import stat

SCHEMA = 'nextcloud-priority-55852-v2'

class EffectivePolicy:
    def __init__(self, base, weight):
        self.base, self.effective_weight = base, weight
    def __getattr__(self, name):
        return getattr(self.base, name)
    def weight(self, source):
        return self.effective_weight if source == 'nextcloud' else self.base.weight(source)
    def snapshot(self):
        value = self.base.snapshot()
        # Do not mutate shared SourcePolicy's nested snapshot, even if reused.
        value = dict(value, sources={k:dict(v) for k,v in value['sources'].items()})
        if 'nextcloud' in value['sources']:
            value['sources']['nextcloud']['weight'] = self.effective_weight
        return value

class NextcloudPriority:
    def __init__(self, connect):
        self.connect = connect
        self.value = None
        self.last = dict(reason='not_read')
    @classmethod
    def from_file(cls, path):
        p = Path(path)
        s = p.lstat()
        if not stat.S_ISREG(s.st_mode) or s.st_uid != os.getuid() or s.st_mode & 0o077 or s.st_size > 4096:
            raise ValueError('private restricted priority connection file required')
        config = json.loads(p.read_text())
        if (set(config)-{'host','port','dbname','user','password','sslmode','sslrootcert'}
            or not {'host','dbname','user'} <= config.keys()
            or config['user'] in {'postgres','migration','archivist_migrate'}
            or (config['host'] not in {'127.0.0.1','::1'} and config.get('sslmode') != 'verify-full')):
            raise ValueError('restricted priority connection configuration required')
        def connect():
            import psycopg
            from psycopg.rows import dict_row
            return psycopg.connect(**config, autocommit=True, row_factory=dict_row,
                connect_timeout=3, options='-c statement_timeout=5000 -c lock_timeout=500')
        return cls(connect)
    def contract(self):
        return dict(schema=SCHEMA, enabled=True, primary_weight=7, deferred_weight=10)

    @contextmanager
    def hold(self, db):
        self.value = None
        if not db.execute("SELECT 1 FROM jobs WHERE source='nextcloud' AND state='queued' LIMIT 1").fetchone():
            yield
            return
        # Bound input; refuse Nextcloud only on overflow, without hiding other sources.
        rows = db.execute("SELECT external_id,source_item_id FROM jobs WHERE source='nextcloud' AND state='queued' LIMIT 100001").fetchall()
        requests = [dict(key=r['external_id'],fileid=r['source_item_id']) for r in rows]
        from contextlib import ExitStack
        stack = ExitStack()
        try:
            if len(rows)>100000:
                raise ValueError('nextcloud queued bound')
            conn = stack.enter_context(self.connect())
            stack.enter_context(conn.transaction())
            conn.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY')
            role = conn.execute("""SELECT r.rolsuper OR r.rolbypassrls OR
                pg_has_role(session_user,'archivist_migrate','MEMBER') unsafe
                FROM pg_roles r WHERE r.rolname=session_user""").fetchone()
            if not role or role['unsafe']:
                raise ValueError('restricted owner principal required')
            value = conn.execute('SELECT arch.priority_read(%s::jsonb) value',
                                 (json.dumps(requests),)).fetchone()['value']
            if value.get('schema') != SCHEMA or not isinstance(value.get('jobs'),dict):
                raise ValueError('priority contract mismatch')
            self.value = value
            self.last = dict(reason='live_ownerwide_aggregate', floor=value['floor'],
                             blockers=value['blockers'], scope_complete=value['scope_complete'], reasons=value['reasons'])
        except Exception as exc:
            # Fail closed for Nextcloud; no DB/credentials/hidden rows in diagnostic.
            stack.close()
            self.last = dict(reason='priority_unavailable', error_type=type(exc).__name__)
        try:
            yield
        finally:
            self.value = None
            stack.close()  # after SQLite lease COMMIT, before _execute/network

    def candidates(self, rows):
        other = [r for r in rows if r['source'] != 'nextcloud']
        if self.value is None:
            return other, None
        jobs, floor = self.value['jobs'], self.value['floor']
        nc = [r for r in rows if r['source']=='nextcloud' and r['external_id'] in jobs
              and (jobs[r['external_id']]==1 or
                   (self.value['scope_complete'] is True and
                    (floor is None or jobs[r['external_id']]<=floor)))]
        if not nc:
            return other, None
        tier = min(jobs[r['external_id']] for r in nc)
        nc = [r for r in nc if jobs[r['external_id']]==tier]
        # Source head changes only for Nextcloud; other source FIFO/model order stays.
        return sorted(other+nc, key=lambda r:(r['queued_at'],r['id'])), 7 if tier==1 else 10
