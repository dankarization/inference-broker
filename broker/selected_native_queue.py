"""Pinned Astra stage1 on existing native chat; no automatic next cell/stage2."""
import argparse
import json
import math
import os
import sys
import uuid
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request
from . import nightly_client as files
from .native_queue_campaign import PRESETS
from .nightly_eval import project_result
from .openclaw import normalize_request
from .storage import content_evidence

SCHEMA='selected-native-stage1-v1'
SELECTION_SHA='e82cb85ae3109ff1c1c1cb797c8881a5b208b7e2b22bde4ffd9488adf7d9716c'
ORDER=['case-02-kp','case-03-pm','case-01-porta','case-04-s92']
CANDIDATES=[('qwen-low','openclaw','low'),('gemma12-on','openclaw-gemma4',True),
            ('gemma26-on','openclaw-gemma4-26b',True),('nano-on','openclaw-nemotron-nano',True)]
OPTIONS=dict(num_ctx=65536,num_predict=8192,temperature=0)


def build(selection_path,cases_path):
    selection,sha=files.read_json(selection_path)
    if sha!=SELECTION_SHA or selection.get('schema')!='nightly-local-model-selection-v1':
        raise files.Closed('exact accepted Astra selection required')
    cases,manifest_sha=files.accepted_cases(cases_path)
    by_case={c['case_id']:c for c in cases}
    expected=[(cid,case) for cid,_,_ in CANDIDATES for case in ORDER]
    if [(c['candidate_id'],c['case_id']) for c in selection['stage1']['cells']]!=expected:
        raise files.Closed('stage1 order changed')
    cells=[]
    for cid,profile,think in CANDIDATES:
        model,digest,ctx,out,_=PRESETS[profile]
        row=next(c for c in selection['candidates'] if c['id']==cid)
        requested=dict(num_ctx=65536,num_predict=8192,temperature=0,thinking=think,
                       keep_alive='5m',output_format='freeform',format_constraint=None)
        if row['model']!=model or row['queue_profile']!=profile or row['planned']!=requested or ctx<65536 or out<8192:
            raise files.Closed('selected model/settings differ from deployed caps')
        for case_id in ORDER:
            case=by_case[case_id];messages,_=files.read_messages(case['messages_path'])
            wire=dict(model=model,messages=messages,think=think,options=dict(OPTIONS),
                      keep_alive='5m',stream=True,truncate=False,shift=False)
            normalized=normalize_request(wire)
            input_hash,input_bytes=content_evidence(normalized)
            cell_id='s1-'+str(len(cells)+1).zfill(2)
            key='s1-v1-'+files.sha(files.encode(dict(selection=sha,cell=cell_id,input_hash=input_hash,model_digest=digest)))
            cells.append(dict(cell_id=cell_id,order=len(cells)+1,case_id=case_id,candidate_id=cid,profile=profile,
                model=model,model_digest=digest,think=think,options=dict(OPTIONS),keep_alive='5m',
                input_hash=input_hash,input_bytes=input_bytes,wire_hash='sha256:'+files.sha(files.encode(wire)),
                external_id=key,source_item_id=None))
    return dict(schema=SCHEMA,selection_path=str(Path(selection_path).resolve()),selection_sha256=sha,
        cases_manifest_path=str(Path(cases_path).resolve()),manifest_sha256=manifest_sha,cases=cases,cells=cells,
        route='/openclaw/api/chat',lifetime_ceiling=32,stage1_cells=16,stage2='not_admitted_not_automatically_created',
        server_retention='existing native raw result retention; helper never persists reasoning',
        effective_evidence='UNVERIFIED unless a job-bound readback exists; not inferred from requested caps')


def load(path):
    plan,digest=files.read_json(path)
    if not isinstance(plan,dict) or build(plan['selection_path'],plan['cases_manifest_path'])!=plan:
        raise files.Closed('frozen overlay/selection/fixtures changed')
    return plan,digest


def wire_request(plan,cell):
    case=next(c for c in plan['cases'] if c['case_id']==cell['case_id'])
    messages,digest=files.read_messages(case['messages_path'])
    if digest!=case['sha256']:raise files.Closed('case drift')
    wire=dict(model=cell['model'],messages=messages,think=cell['think'],options=dict(OPTIONS),
              keep_alive='5m',stream=True,truncate=False,shift=False)
    if (content_evidence(normalize_request(wire))!=(cell['input_hash'],cell['input_bytes'])
            or 'sha256:'+files.sha(files.encode(wire))!=cell['wire_hash']):
        raise files.Closed('serialized request drift')
    return wire


class NativeTransport(files.Transport):
    def submit_native(self,wire,key):
        headers={'Content-Type':'application/json','Idempotency-Key':key}
        if self.token:headers['Authorization']='Bearer '+self.token
        req=Request(self.base+'/openclaw/api/chat',data=files.encode(wire),headers=headers)
        try:
            response=self.opener.open(req,timeout=30)
        except Exception as error:
            close=getattr(error,'close',None)
            if close is not None:close()
            raise files.Uncertain('native POST outcome unknown; GET reconcile only') from None
        # Caller commits UUID before closing the connection. No native response
        # frame/body is read, persisted or printed. Keyed disconnect is noncancel.
        return response


def job_identity(raw,cell):
    expected=dict(source='openclaw',profile=cell['profile'],kind='chat',external_id=cell['external_id'],source_item_id=None)
    if not isinstance(raw,dict) or any(raw.get(k)!=v for k,v in expected.items()):
        raise files.Uncertain('job correlation mismatch')
    if raw.get('payload'):
        if content_evidence(raw['payload'])!=(cell['input_hash'],cell['input_bytes']):
            raise files.Uncertain('server normalized request mismatch')
    state=raw.get('state')
    if state not in {'queued','running','cancel_requested','completed','failed','cancelled','expired'}:
        raise files.Uncertain('unknown job state')
    attempts=raw.get('attempt_count')
    if type(attempts) is not int or not 0<=attempts<=1:raise files.Uncertain('unexpected execution count')
    result=dict(id=raw['id'],source='openclaw',profile=cell['profile'],state=state,attempt_count=attempts,
        external_id=cell['external_id'],input_hash=cell['input_hash'],serialized_wire_hash=cell['wire_hash'],
        requested=dict(model=cell['model'],model_digest=cell['model_digest'],think=cell['think'],options=cell['options'],keep_alive='5m'),
        effective_settings=None,effective_status='UNVERIFIED',server_input_hash_status='verified_pending_payload' if raw.get('payload') else 'keyed_local_hash_only')
    for key in ('created','queued_at','started','finished'):
        value=raw.get(key);result[key]=value if type(value) in (int,float) and math.isfinite(value) and value>=0 else None
    for key,start,end in [('queue_seconds','queued_at','started'),('execution_seconds','started','finished'),('end_to_end_seconds','created','finished')]:
        result[key]=max(0,result[end]-result[start]) if result[start] is not None and result[end] is not None else None
    if state=='completed':result['result']=project_result(raw.get('result'),cell['model'])
    return result


def admit(plan,cell,journal,transport):
    if cell not in plan['cells']:raise files.Closed('cell outside frozen stage1')
    wire=wire_request(plan,cell)
    if journal.row(cell) is not None:raise files.Closed('recorded intent: GET reconcile only')
    for prior in plan['cells'][:cell['order']-1]:
        row=journal.row(prior)
        if row is None or row['phase']!='terminal':raise files.Closed('strict sequential stage1 order')
        collected=json.loads(row['collected'])
        if collected['state']!='completed' or not collected.get('result',{}).get('acceptable'):
            raise files.Closed('execution failure requires main review, no automatic next cell')
    policy=transport.request('/v1/sources')
    entry=policy.get('policy',{}).get('sources',{}).get('openclaw')
    if not isinstance(entry,dict) or entry.get('enabled') is not True or entry.get('admission_allowed') is not True:
        raise files.Closed('existing source not admitting; never mutate it')
    journal.intent(cell)
    response=transport.submit_native(wire,cell['external_id'])
    try:
        return journal.record_id(cell,dict(id=response.headers.get('X-Broker-Job-Id')))
    finally:response.close()


def collect(plan,cell,journal,transport,reconcile=False):
    wire_request(plan,cell)
    row=journal.row(cell)
    if row is None:raise files.Closed('recorded intent required')
    if row['job_id'] is None:
        if not reconcile:raise files.Closed('uncertain POST: explicit GET reconcile required')
        raw=transport.request('/v1/correlations?'+urlencode(dict(source='openclaw',external_id=cell['external_id'],limit=2)))
        rows=raw.get('jobs') if isinstance(raw,dict) else None
        if not isinstance(rows,list) or len(rows)!=1:raise files.Uncertain('missing/ambiguous correlation never permits replay')
        record=rows[0]
        if any(record.get(k)!=v for k,v in dict(source='openclaw',profile=cell['profile'],external_id=cell['external_id'],source_item_id=None).items()):
            raise files.Uncertain('correlation identity mismatch')
        journal.record_id(cell,dict(id=record.get('job_id')));row=journal.row(cell)
    raw=transport.request('/v1/jobs/'+row['job_id'])
    if not isinstance(raw,dict) or raw.get('id')!=row['job_id']:raise files.Uncertain('job UUID mismatch')
    projected=job_identity(raw,cell)
    terminal=projected['state'] in {'completed','failed','cancelled','expired'}
    with journal.db:
        journal.db.execute('UPDATE cells SET phase=?,collected=? WHERE cell_id=?',
            ('terminal' if terminal else 'recorded',files.encode(projected).decode(),cell['cell_id']))
    return projected


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
    for command in ('dry-run','freeze'):
        p=sub.add_parser(command);p.add_argument('--selection',required=True);p.add_argument('--cases',required=True)
        if command=='freeze':p.add_argument('--out',required=True)
    for command in ('request','admit','collect','reconcile'):
        p=sub.add_parser(command);p.add_argument('--frozen',required=True);p.add_argument('--cell',required=True)
        if command=='request':p.add_argument('--out',required=True)
        else:
            p.add_argument('--journal',required=True);p.add_argument('--base-url',default='http://192.168.2.19:8088');p.add_argument('--token-env')
            if command in ('collect','reconcile'):p.add_argument('--out')
    args=parser.parse_args(argv);os.umask(0o077)
    try:
        if args.command in ('dry-run','freeze'):
            plan=build(args.selection,args.cases)
            if args.command=='freeze':files.new_json(args.out,plan)
            print(json.dumps(dict(stage1_cells=16,selection_sha256=plan['selection_sha256'],cells=plan['cells'])));return 0
        plan,digest=load(args.frozen);cell=next(c for c in plan['cells'] if c['cell_id']==args.cell)
        if args.command=='request':files.new_json(args.out,wire_request(plan,cell));print(json.dumps(dict(cell_id=cell['cell_id'],wire_hash=cell['wire_hash'])));return 0
        token=os.environ.get(args.token_env) if args.token_env else None
        if args.token_env and not token:raise files.Closed('runtime auth missing')
        transport=NativeTransport(args.base_url,token);journal=files.Journal(args.journal,digest,transport.base)
        try:
            value=admit(plan,cell,journal,transport) if args.command=='admit' else collect(plan,cell,journal,transport,args.command=='reconcile')
            if getattr(args,'out',None):files.new_json(args.out,value)
            print(json.dumps({k:value[k] for k in ('job_id','id','state') if k in value}))
            return 3 if value.get('state') in {'failed','cancelled','expired'} or (value.get('state')=='completed' and not value['result']['acceptable']) else 0
        finally:journal.close()
    except (files.Closed,ValueError,TypeError,KeyError,OSError,StopIteration):
        print('blocked/uncertain; no repeat POST; use GET reconcile on existing journal',file=sys.stderr);return 2


if __name__=='__main__':raise SystemExit(main())
