"""Finite existing-preset TEXT campaign preparation; admission contract is blocked.

No server/profile/policy writes. Configured baseline is not verified toggle
support. Existing OpenClaw server stores raw thinking and lacks job-scoped
native digest/effective-context evidence; this helper cannot fix that locally.
"""
from __future__ import annotations
import argparse
import copy
import hashlib
import json
import re
import sys
from pathlib import Path
from . import nightly_client as files
from .nightly_eval import think_value, same_value, supports, thinking_metadata
from .openclaw import normalize_request
from .storage import content_evidence

SCHEMA = 'native-queue-campaign-v1'
FROZEN = 'native-queue-campaign-frozen-v1'
SOURCE = 'openclaw'
MAX_CALLS = 32
# Read-only deployed profile metadata + main's active config observation 7549.
PRESETS = {
    'openclaw': ('qwen3.8:ad-iq2-xs', 'b6f02c5a32addd8a6b03ae205617bec9472903d9755e116084376fd429d100b9', 131072, 16384, 'low'),
    'openclaw-qwen3-vl': ('qwen3-vl:30b', 'eda0be1008774ea4e575187712b9a3b1201f3e66e7ecff899cc295db1cf00248', 212992, 16384, 'max'),
    'openclaw-nemotron3': ('nemotron3:33b', 'f6d8b7ff496ccc53429cc480ad53971d522b443ee4a5aa58a6da49e57acf42cf', 131072, 8192, True),
    'openclaw-nemotron-nano': ('nemotron-3-nano:30b-a3b-q4_K_M', 'b725f11174073334edd0c2ff396b8d4e66d7dab22a5a63717ccad5a08a270cf1', 262144, 16384, True),
    'openclaw-gemma4': ('gemma4:12b', '4eb23ef187e2c5462566d6a1d3bbbc2f1346d0b4327cbb66d58fffbcc9b2b05c', 262144, 16384, True),
    'openclaw-gemma4-26b': ('gemma4:26b', '001e5dafc3c77684c2307ebc6ab8e336e10c9b18eca52acf547d72fc83c3ca8c', 262144, 8192, True),
}
BLOCKERS = ('existing_openclaw_server_persists_raw_thinking',
            'no_openclaw_final_only_wire_projection',
            'no_job_scoped_native_digest_or_effective_context_evidence')


def gate():
    return dict(state='blocked_existing_server_contract', reasons=list(BLOCKERS),
                admission_enabled=False, deploy_needed_for_full_acceptance=True,
                offline_prepare_deploy_needed=False,
                scope='strict end-to-end no-reasoning persistence and per-job effective evidence',
                helper_cannot_repair_server_retention=True)


def prepare(campaign):
    if not isinstance(campaign, str) or re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}', campaign) is None:
        raise files.Closed('bounded campaign ID required')
    models = []
    for profile, (model, digest, context, output, think) in PRESETS.items():
        settings = dict(think=think, options=dict(num_ctx=context, num_predict=output, temperature=0))
        basis = 'main_observed_provider_config_7549'
        if profile == 'openclaw-gemma4-26b':
            settings = dict(think=False, options=dict(num_ctx=32768, num_predict=512, temperature=0))
            basis = 'main_observed_agents_defaults_models_override_7549'
        models.append(dict(profile=profile, model=model, digest=digest,
            provider_configured_think=think, caps=dict(num_ctx=context, num_predict=output),
            baseline_basis=basis, baseline=settings, verified_native_thinking=None))
    return dict(schema=SCHEMA, campaign=campaign, source=SOURCE, max_calls=MAX_CALLS,
                models=models, extra_modes=[], gate=gate())


def settings(row, value, toggle=False):
    if not isinstance(value, dict) or set(value) != {'think', 'options'} or not think_value(value['think']):
        raise files.Closed('explicit native settings required')
    options = value['options']
    if (not isinstance(options, dict) or set(options) != {'num_ctx','num_predict','temperature'}
            or any(type(options[k]) is not int or not 0 < options[k] <= row['caps'][k]
                   for k in ('num_ctx','num_predict'))
            or type(options['temperature']) not in (int,float) or options['temperature'] != 0):
        raise files.Closed('settings exceed existing caps or change controlled temperature')
    if toggle:
        native = row['verified_native_thinking']
        if type(value['think']) is not bool or not isinstance(native, dict) or set(native) != {'model','digest','thinking','origin'}:
            raise files.Closed('extra on/off requires explicit exact-artifact native evidence')
        if native['model'] != row['model'] or native['digest'] != row['digest'] or native['origin'] != '/api/show':
            raise files.Closed('native toggle evidence identity mismatch')
        metadata = thinking_metadata(native)
        if not supports(metadata, False) or not supports(metadata, True):
            raise files.Closed('both boolean modes must be observed; levels are never off')
        if same_value(value['think'], row['baseline']['think']) or value['options'] != row['baseline']['options']:
            raise files.Closed('toggle must change only native think, not context/output')
    return value


def validate_draft(draft):
    if not isinstance(draft, dict) or draft.get('schema') != SCHEMA:
        raise files.Closed('campaign schema required')
    expected = prepare(draft.get('campaign'))
    if set(draft) != set(expected) or any(draft.get(k) != expected[k] for k in ('source','max_calls','gate')):
        raise files.Closed('campaign lane/budget/gate cannot be changed')
    rows = draft.get('models')
    if not isinstance(rows, list) or len(rows) != 6 or {r.get('profile') for r in rows if isinstance(r,dict)} != set(PRESETS):
        raise files.Closed('exact six existing presets required')
    by_profile = {r['profile']:r for r in rows}
    for ref in expected['models']:
        row = by_profile[ref['profile']]
        if set(row) != set(ref):
            raise files.Closed('exact preset metadata fields required; no raw response/auth')
        native = row['verified_native_thinking']
        if native is not None:
            if (not isinstance(native,dict) or set(native)!={'model','digest','thinking','origin'}
                    or native['model']!=row['model'] or native['digest']!=row['digest'] or native['origin']!='/api/show'
                    or native['thinking']!=thinking_metadata(native)):
                raise files.Closed('only projected exact-artifact native evidence allowed')
        for k in ('profile','model','digest','caps','provider_configured_think','baseline_basis'):
            if not same_value(row.get(k),ref[k]):
                raise files.Closed('configured preset identity/evidence changed')
        settings(row,row['baseline'])
        # Baseline may lower requested budgets explicitly before freeze, but not
        # invent a different configured native mode or mislabel low/max as off.
        if not same_value(row['baseline']['think'],ref['baseline']['think']):
            raise files.Closed('baseline must use observed configured think; use verified extra modes for comparisons')
    extra = draft.get('extra_modes')
    if not isinstance(extra,list) or len(extra)>2:
        raise files.Closed('only eight extra calls beyond 24 baseline calls')
    seen=set()
    for mode in extra:
        if not isinstance(mode,dict) or set(mode)!={'profile','settings'} or mode['profile'] not in by_profile or mode['profile'] in seen:
            raise files.Closed('one extra boolean mode per selected model')
        seen.add(mode['profile']);settings(by_profile[mode['profile']],mode['settings'],toggle=True)
    return by_profile


def build(draft, cases_path):
    by_profile=validate_draft(draft)
    cases, manifest_hash=files.accepted_cases(cases_path)
    cells=[]
    modes=[(r['profile'],'configured-baseline',r['baseline'],'configured_baseline_not_toggle_proof') for r in draft['models']]
    modes += [(m['profile'],'verified-on' if m['settings']['think'] else 'verified-off',m['settings'],'verified_native_boolean_toggle') for m in draft['extra_modes']]
    for profile,label,effective,basis in modes:
        row=by_profile[profile]
        for case in cases:
            messages,_=files.read_messages(case['messages_path'])
            payload=normalize_request(dict(model=row['model'],messages=messages,**effective,truncate=False,shift=False,stream=False))
            payload_hash,byte_count=content_evidence(payload)
            cell_id=case['case_id']+'.'+profile+'.'+label
            identity=dict(schema=FROZEN,campaign=draft['campaign'],cell_id=cell_id,profile=profile,
                          model=row['model'],digest=row['digest'],input_hash=payload_hash)
            key='ncq-v1-'+files.sha(files.encode(identity))
            cells.append(dict(cell_id=cell_id,case_id=case['case_id'],profile=profile,model=row['model'],
                model_digest=row['digest'],mode=label,mode_basis=basis,requested_think=effective['think'],
                requested_options=effective['options'],expected_broker_clamped_options=effective['options'],
                observed_effective_options=None,observed_per_job_digest=None,
                input_hash=payload_hash,input_bytes=byte_count,external_id=key,
                source_item_id='ncq-v1-'+draft['campaign']+'-'+case['case_id']))
    if not 24<=len(cells)<=MAX_CALLS:raise files.Closed('finite 24..32-call campaign required')
    return dict(schema=FROZEN,campaign=draft['campaign'],draft=copy.deepcopy(draft),
                manifest_path=str(Path(cases_path).resolve()),manifest_sha256=manifest_hash,
                cases=cases,cells=cells,gate=gate())


def load_frozen(path):
    plan,digest=files.read_json(path)
    if not isinstance(plan,dict) or plan.get('schema')!=FROZEN or build(plan['draft'],plan['manifest_path'])!=plan:
        raise files.Closed('frozen campaign/fixtures changed')
    return plan,digest


def request_for(plan,cell):
    case=next(c for c in plan['cases'] if c['case_id']==cell['case_id'])
    messages,digest=files.read_messages(case['messages_path'])
    if digest!=case['sha256']:raise files.Closed('case changed')
    payload=normalize_request(dict(model=cell['model'],messages=messages,think=cell['requested_think'],
        options=cell['requested_options'],truncate=False,shift=False,stream=False))
    if content_evidence(payload)!=(cell['input_hash'],cell['input_bytes']):raise files.Closed('request hash changed')
    return dict(source=SOURCE,profile=cell['profile'],kind='chat',payload=payload,
                external_id=cell['external_id'],source_item_id=cell['source_item_id'])


def admit(*args,**kwargs):
    # No opt-out/env override. A client cannot stop the existing server from
    # persisting raw native thinking or manufacture missing job-scoped evidence.
    raise files.Closed('blocked_existing_server_contract: no POST, journal reset, or blind retry')


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('prepare');p.add_argument('--campaign',required=True);p.add_argument('--out',required=True)
    for command in ('dry-run','freeze'):
        p=sub.add_parser(command);p.add_argument('--draft',required=True);p.add_argument('--cases',required=True)
        if command=='freeze':p.add_argument('--out',required=True)
    p=sub.add_parser('request');p.add_argument('--frozen',required=True);p.add_argument('--cell',required=True);p.add_argument('--out',required=True)
    sub.add_parser('admit')
    args=parser.parse_args(argv)
    try:
        if args.command=='admit':admit()
        if args.command=='prepare':
            draft=prepare(args.campaign);files.new_json(args.out,draft)
            print(json.dumps(dict(models=6,baseline_calls=24,max_calls=32,gate=gate())));return 0
        if args.command in ('dry-run','freeze'):
            draft,_=files.read_json(args.draft);plan=build(draft,args.cases)
            if args.command=='freeze':files.new_json(args.out,plan)
            print(json.dumps(dict(planned_calls=len(plan['cells']),gate=gate(),cells=plan['cells'])));return 0
        plan,_=load_frozen(args.frozen);cells=[c for c in plan['cells'] if c['cell_id']==args.cell]
        if len(cells)!=1:raise files.Closed('one exact frozen cell required')
        files.new_json(args.out,request_for(plan,cells[0]))
        print(json.dumps(dict(cell_id=cells[0]['cell_id'],input_hash=cells[0]['input_hash'],gate=gate())));return 0
    except (files.Closed,ValueError,TypeError,KeyError,OSError,StopIteration):
        print('blocked/invalid existing queue contract; no inference or POST',file=sys.stderr);return 2


if __name__=='__main__':raise SystemExit(main())
