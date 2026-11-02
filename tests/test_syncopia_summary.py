"""Synthetic native summary contract; no real HTTP, wake, inference or install."""
import copy
import hashlib
import inspect
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from broker import syncopia_summary as s
from broker.compat import validate_syncopia_memory_payload
from broker.profiles import PROFILES
from broker.service import Broker


class Wake:
    def __init__(self): self.calls=0
    def wake(self): self.calls+=1


class Native:
    def __init__(self):
        self.calls=[];self.requests=[];self.active=None;self.context=None
        self.digest_override=None;self.values=[False,True];self.return_context=None
    def tags(self,timeout_seconds=5):
        self.calls.append('tags')
        return {'models':[dict(name=v[0],digest=self.digest_override or v[1]) for v in s.MODELS.values()]}
    def show(self,model,timeout_seconds=5):
        self.calls.append('show')
        limit=next(v[2] for v in s.MODELS.values() if v[0]==model)
        return dict(capabilities=['completion','thinking'],thinking=dict(values=self.values,default=True),
                    model_info={'gemma4.context_length':limit})
    def ps(self,timeout_seconds=None):
        return {'models':[] if self.active is None else [dict(name=self.active,
          digest=next(v[1] for v in s.MODELS.values() if v[0]==self.active),
          context_length=self.return_context or self.context)]}
    def unload(self,model):
        if self.active==model:self.active=None
    def is_ready(self,model): return True
    def run(self,kind,request):
        self.requests.append((kind,copy.deepcopy(request)))
        self.active=request['model'];self.context=request['options']['num_ctx']
        return dict(model=self.active,done=True,done_reason='stop',
                    message=dict(role='assistant',content='Synthetic exact final Ω'),prompt_eval_count=7,eval_count=4)


class SummaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.native=Native();self.wake=Wake()
        self.broker=Broker(self.root/'synthetic.sqlite3',self.native,self.wake,min_free_space_bytes=1)
        self.addCleanup(self.broker.db.close)

    def body(self,profile=None):
        profile=profile or next(iter(s.MODELS))
        return dict(profile=profile,source=s.SOURCE,kind='chat',external_id='synthetic-summary',
            source_item_id='synthetic-source',model=s.MODELS[profile][0],
            options=dict(num_ctx=32768,num_predict=4096,temperature=0),
            payload=dict(messages=[dict(role='system',content='Synthetic summary task.'),
              dict(role='user',content='Exact Ω https://example.invalid/?a=1#f')],
              think=False,tools=[],stream=False))

    def count(self): return self.broker.db.execute('select count(*) from jobs').fetchone()[0]

    def test_both_profiles_admit_exact_boolean_wire_and_distinct_budget_evidence(self):
        for i,profile in enumerate(s.MODELS):
            body=self.body(profile);body['external_id']+=str(i)
            original=copy.deepcopy(body)
            job=self.broker.submit(**body)
            self.assertEqual(body,original)
            p=job['payload'];self.assertIs(p['think'],False)
            self.assertEqual(p['messages'],body['payload']['messages'])
            self.assertEqual(p['options'],body['options'])
            self.assertEqual(p['model'],body['model'])
            self.assertEqual(p['summary_contract']['schema'],s.VERSION)
            self.assertEqual(p['summary_contract']['admission_budget']['runtime_fit'],'UNMEASURED')
        self.assertFalse(self.native.requests);self.assertEqual(self.wake.calls,0)

    def test_reject_boolean_coercion_models_sources_formats_options_and_conflicts(self):
        changes=[
          ('payload',{'think':'low'}),('payload',{'think':'false'}),('payload',{'think':0}),
          ('payload',{'think':None}),('payload',{'think':True}),
          ('payload',{'format':{}}),('payload',{'images':[]}),('payload',{'stream':True}),
          ('payload',{'tools':[{}]}),('payload',{'keep_alive':'1h'}),
          ('payload',{'messages':[dict(role='user',content='missing system')]}),
          ('payload',{'messages':[dict(role='system',content='s'),dict(role='user',content='u',images=[])]}),
          ('payload',{'summary_contract':{}}),
          ('top',{'model':'qwen3.8:ad-iq2-xs'}),('top',{'source':'wrong'}),
          ('top',{'kind':'generate'}),('top',{'external_id':None}),
          ('top',{'source_item_id':None}),('top',{'producer_storage':{}}),
          ('options',{'num_ctx':True}),('options',{'num_predict':4096.0}),
          ('options',{'temperature':False}),('options',{'temperature':0.1}),
          ('options',{'seed':42}),
        ]
        for target,change in changes:
            body=self.body()
            if target=='top':body.update(change)
            else:body['payload' if target=='payload' else 'options'].update(change)
            with self.subTest(change=change),self.assertRaises(ValueError):self.broker.submit(**body)
        body=self.body();body['payload']['model']='gpt-oss:20b'
        with self.assertRaises(ValueError):self.broker.submit(**body)
        body=self.body();body['payload']['options']=dict(num_ctx=65536)
        with self.assertRaisesRegex(ValueError,'conflicting duplicate'):self.broker.submit(**body)
        self.assertEqual(self.count(),0);self.assertFalse(self.native.requests);self.assertEqual(self.wake.calls,0)

    def test_malformed_nested_options_reject_before_generic_override_merge(self):
        for value in (None, [], 'not-an-object', 0):
            body=self.body();body['payload']['options']=value
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'options must be object'):
                self.broker.submit(**body)
        self.assertEqual(self.count(),0)
        self.assertEqual(self.native.calls,[])

    def test_budget_input_room_and_profile_ceiling_fail_before_native_calls(self):
        bolt='syncopia-summary-bolt-e4b-v1'
        for ctx,out,fragment in ((65536,65536,'no input room'),(65537,4096,'exceeds'),
                                 (65536,32768,'UTF-8 input proxy')):
            body=self.body(bolt);body['options'].update(num_ctx=ctx,num_predict=out)
            if fragment=='UTF-8 input proxy':body['payload']['messages'][1]['content']='Ω'*20000
            with self.subTest(ctx=ctx,out=out),self.assertRaisesRegex(ValueError,fragment):self.broker.submit(**body)
        self.assertEqual(self.count(),0);self.assertFalse(self.native.calls)

    def test_metadata_identity_and_native_boolean_support_fail_closed(self):
        self.native.digest_override='0'*64
        with self.assertRaisesRegex(ValueError,'digest mismatch'):self.broker.submit(**self.body())
        self.native.digest_override=None
        for value in (None,['low'],[0,1],[False],{'false':True}):
            self.native.values=value
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'boolean capability'):
                self.broker.submit(**self.body())
        self.assertEqual(self.count(),0);self.assertFalse(self.native.requests)

    def test_dispatch_preserves_false_options_and_strips_only_private_contract(self):
        for i,profile in enumerate(s.MODELS):
            body=self.body(profile);body['external_id']+=str(i)
            job=self.broker.submit(**body)
            row=self.broker.db.execute('select * from jobs where id=?',(job['id'],)).fetchone()
            self.broker._execute(row)
            kind,request=self.native.requests[-1]
            self.assertEqual(kind,'chat');self.assertIs(request['think'],False)
            self.assertEqual(request['messages'],body['payload']['messages'])
            self.assertEqual(request['options'],body['options'])
            self.assertNotIn('summary_contract',request)
            self.assertIs(request['truncate'],False);self.assertIs(request['shift'],False)
            got=self.broker.status(job['id'])
            self.assertEqual(got['state'],'completed')
            self.assertEqual((got['input_hash'],got['input_bytes']),
                             (job['input_hash'],job['input_bytes']))
            self.assertEqual(got['payload'],{})
            ev=got['result']['execution_evidence']
            self.assertEqual(ev['runtime_context_length'],body['options']['num_ctx'])
            self.assertEqual(ev['request_options'],body['options'])
            self.assertFalse(ev['quality_GO'])

    def test_dispatch_drift_prevents_execution_and_historical_get_does_not_query_native(self):
        job=self.broker.submit(**self.body())
        row=self.broker.db.execute('select * from jobs where id=?',(job['id'],)).fetchone()
        self.native.digest_override='0'*64
        with self.assertRaisesRegex(ValueError,'digest mismatch'):self.broker._execute(row)
        self.assertEqual(self.wake.calls,0);self.assertFalse(self.native.requests)
        before=list(self.native.calls)
        self.assertEqual(self.broker.status(job['id'])['id'],job['id'])
        self.assertEqual(self.native.calls,before)

    def test_persisted_payload_and_effective_context_drift_fail_closed(self):
        job=self.broker.submit(**self.body())
        row=dict(self.broker.db.execute('select * from jobs where id=?',(job['id'],)).fetchone())
        payload=json.loads(row['payload']);payload['options']['num_predict']=1024
        row['payload']=json.dumps(payload)
        with self.assertRaisesRegex(ValueError,'persisted input identity'):self.broker._execute(row)
        self.assertFalse(self.native.requests)
        row=self.broker.db.execute('select * from jobs where id=?',(job['id'],)).fetchone()
        self.native.return_context=8192
        with self.assertRaisesRegex(ValueError,'runtime context'):self.broker._execute(row)

    def test_same_size_message_tampering_fails_identity_before_native_or_wake(self):
        job=self.broker.submit(**self.body())
        row=dict(self.broker.db.execute('select * from jobs where id=?',(job['id'],)).fetchone())
        payload=json.loads(row['payload']);payload['messages'][1]['content']=payload['messages'][1]['content'].replace('Exact','Other')
        row['payload']=json.dumps(payload)
        before=list(self.native.calls)
        with self.assertRaisesRegex(ValueError,'persisted input identity'):self.broker._execute(row)
        self.assertEqual(before,self.native.calls)
        self.assertEqual(self.wake.calls,0);self.assertFalse(self.native.requests)

    def test_unknown_model_rejected_before_any_native_metadata_fallback(self):
        body=self.body();body['model']='unreviewed-model:latest'
        with self.assertRaisesRegex(ValueError,'model must match'):
            self.broker.submit(**body)
        self.assertEqual(self.native.calls,[]);self.assertEqual(self.count(),0)

    def test_exact_queued_completed_failed_receipts_ignore_offline_and_drift_metadata(self):
        for profile in s.MODELS:
            for state in ('queued','completed','failed'):
                for fault in ('offline','digest','capability'):
                    with self.subTest(profile=profile,state=state,fault=fault):
                        self.native.digest_override=None;self.native.values=[False,True]
                        body=self.body(profile)
                        body['external_id']='receipt-'+str(self.count())
                        job=self.broker.submit(**body)
                        if state=='completed':
                            row=self.broker.db.execute('select * from jobs where id=?',(job['id'],)).fetchone()
                            self.broker._execute(row)
                        elif state=='failed':
                            with self.broker.db:
                                self.broker.db.execute("update jobs set state='failed',payload='{}',error='synthetic failed' where id=?",(job['id'],))
                        if state!='queued':self.assertEqual(self.broker.status(job['id'])['payload'],{})
                        self.native.digest_override='0'*64 if fault=='digest' else None
                        self.native.values=['low'] if fault=='capability' else [False,True]
                        count=self.count();calls=list(self.native.calls);runs=copy.deepcopy(self.native.requests)
                        wakes=self.wake.calls
                        tag_method=self.native.tags
                        with patch.object(self.native,'tags',side_effect=OSError('synthetic offline') if fault=='offline' else tag_method) as tags, patch.object(self.native,'show',side_effect=AssertionError('receipt must not query show')) as show:
                            retried=self.broker.submit(**copy.deepcopy(body))
                            tags.assert_not_called();show.assert_not_called()
                        self.assertEqual((retried['id'],retried['state']),(job['id'],state))
                        self.assertEqual((retried['input_hash'],retried['input_bytes']),(job['input_hash'],job['input_bytes']))
                        self.assertEqual(self.count(),count)
                        self.assertEqual(self.native.calls,calls);self.assertEqual(self.native.requests,runs)
                        self.assertEqual(self.wake.calls,wakes)
        self.native.digest_override=None;self.native.values=[False,True]

    def test_offline_collision_and_pinned_proof_mismatch_reject_without_preflight(self):
        body=self.body();job=self.broker.submit(**body)
        for change in ('text','item','profile','options','legacy'):
            changed=copy.deepcopy(body)
            if change=='text':changed['payload']['messages'][1]['content']='different'
            elif change=='item':changed['source_item_id']='different-item'
            elif change=='options':changed['options']['num_predict']+=1
            elif change=='profile':
                changed['profile']='syncopia-summary-bolt-e4b-v1';changed['model']=s.MODELS[changed['profile']][0]
            else:
                changed.update(profile='syncopia-memory-qwen38',model='qwen3.8:ad-iq2-xs');changed['payload']['think']='low'
            with self.subTest(change=change),patch.object(self.native,'tags',side_effect=AssertionError('must not preflight')) as tags:
                with self.assertRaisesRegex(ValueError,'identity conflict'):self.broker.submit(**changed)
                tags.assert_not_called()
        proof=s.pinned_contract(body['profile']);proof['schema']='different-proof'
        with patch.object(s,'pinned_contract',return_value=proof),patch.object(self.native,'tags',side_effect=AssertionError('must not preflight')):
            with self.assertRaisesRegex(ValueError,'identity conflict'):self.broker.submit(**body)
        self.assertEqual(self.count(),1);self.assertEqual(self.broker.status(job['id'])['id'],job['id'])

    def test_expired_receipt_remains_closed_without_metadata_or_reinsertion(self):
        from broker.storage import content_evidence
        body=self.body()
        normalized=s.prepare(body['profile'],body['source'],body['kind'],body['payload'],
                             body['model'],body['options'],body['external_id'],body['source_item_id'],None)
        with self.broker.db:
            self.broker.db.execute("insert into job_retention_tombstones (job_id,source,external_id,profile,kind,final_state,input_hash,created,purged_at) values (?,?,?,?,?,?,?,?,?)",
                ('expired-synthetic',s.SOURCE,body['external_id'],body['profile'],'chat','completed',content_evidence(normalized)[0],1,2))
        with patch.object(self.native,'tags',side_effect=AssertionError('must not preflight')) as tags:
            with self.assertRaisesRegex(ValueError,'recover the durable producer artifact'):self.broker.submit(**body)
            body['payload']['messages'][1]['content']='changed expired input'
            with self.assertRaisesRegex(ValueError,'expired correlation'):self.broker.submit(**body)
            tags.assert_not_called()
        self.assertEqual(self.count(),0);self.assertFalse(self.native.requests)

    def test_stored_receipt_byte_identity_corruption_cannot_return_uuid(self):
        body=self.body();job=self.broker.submit(**body)
        with self.broker.db:self.broker.db.execute('update jobs set input_bytes=input_bytes+1 where id=?',(job['id'],))
        with patch.object(self.native,'tags',side_effect=AssertionError('must not preflight')):
            with self.assertRaisesRegex(ValueError,'identity conflict'):self.broker.submit(**body)
        self.assertEqual(self.count(),1)

    def test_new_keys_still_require_live_metadata_and_do_not_insert_when_offline_or_drifted(self):
        for profile in s.MODELS:
            body=self.body(profile)
            with patch.object(self.native,'tags',side_effect=OSError('synthetic offline')) as tags:
                with self.assertRaisesRegex(ValueError,'metadata unavailable'):self.broker.submit(**body)
                tags.assert_called_once()
            self.native.digest_override='0'*64
            with self.assertRaisesRegex(ValueError,'digest mismatch'):self.broker.submit(**body)
            self.native.digest_override=None;self.native.values=['low']
            with self.assertRaisesRegex(ValueError,'boolean capability'):self.broker.submit(**body)
            self.native.values=[False,True]
        self.assertEqual(self.count(),0);self.assertFalse(self.native.requests);self.assertEqual(self.wake.calls,0)

    def test_idempotency_exact_retry_and_payload_model_item_collisions(self):
        body=self.body();job=self.broker.submit(**body)
        self.assertEqual(self.broker.submit(**body)['id'],job['id'])
        for target,value in (('text','different'),('item','other-source-item'),
                             ('profile','syncopia-summary-bolt-e4b-v1'),('old',None)):
            changed=copy.deepcopy(body)
            if target=='text':changed['payload']['messages'][1]['content']=value
            elif target=='item':changed['source_item_id']=value
            elif target=='profile':
                changed['profile']=value;changed['model']=s.MODELS[value][0]
            else:
                changed['profile']='syncopia-memory-qwen38';changed['model']='qwen3.8:ad-iq2-xs'
                changed['payload']['think']='low'
            with self.subTest(target=target),self.assertRaisesRegex(ValueError,'identity conflict'):
                self.broker.submit(**changed)
        self.assertEqual(self.count(),1)
        self.assertFalse(self.native.requests)

    def test_legacy_identity_cannot_be_rebound_to_summary_profile(self):
        body=self.body();old=copy.deepcopy(body)
        old.update(profile='syncopia-memory-qwen38',model='qwen3.8:ad-iq2-xs')
        old['payload']['think']='low'
        job=self.broker.submit(**old)
        with self.assertRaisesRegex(ValueError,'identity conflict'):self.broker.submit(**body)
        self.assertEqual(self.broker.status(job['id'])['payload']['think'],'low')
        self.assertEqual(self.count(),1)

    def test_existing_source_policy_still_blocks_new_admission_not_historical_get(self):
        from broker.service import SourcePolicy, SourceAdmissionBlocked
        job=self.broker.submit(**self.body())
        policy=self.root/'policy.json'
        policy.write_text(json.dumps(dict(version=1,sources={s.SOURCE:dict(enabled=True,
            weight=1,admission_allowed=False)})))
        self.broker.source_policy=SourcePolicy(policy)
        body=self.body();body['external_id']='new-blocked-correlation'
        with self.assertRaises(SourceAdmissionBlocked):self.broker.submit(**body)
        self.assertEqual(self.count(),1)
        self.native.digest_override='0'*64
        before=list(self.native.calls)
        self.assertEqual(self.broker.status(job['id'])['id'],job['id'])
        self.assertEqual(before,self.native.calls)
        self.assertFalse(self.native.requests)

    def test_native_mode_and_model_metadata_rechecked_before_wake(self):
        job=self.broker.submit(**self.body())
        row=self.broker.db.execute('select * from jobs where id=?',(job['id'],)).fetchone()
        self.native.values=['low','high']
        with self.assertRaisesRegex(ValueError,'boolean capability'):self.broker._execute(row)
        self.assertEqual(self.wake.calls,0)
        self.assertFalse(self.native.requests)

    def test_contract_document_matches_code_and_dedicated_profile_limits(self):
        from dataclasses import asdict
        contract=json.loads((Path(__file__).resolve().parents[1]/'docs/contracts/syncopia-summary-native-v1.json').read_text())
        self.assertEqual(contract['schema'],s.VERSION)
        self.assertIs(contract['think'],False)
        for name,(model,digest,limit) in s.MODELS.items():
            entry=contract['profiles'][name]
            self.assertEqual(entry['profile'],asdict(PROFILES[name]))
            self.assertEqual((entry['model'],entry['model_digest'],entry['native_context']),(model,digest,limit))

    def test_original_memory_function_and_all_existing_profiles_unchanged(self):
        original=subprocess.check_output(['git','show','595755049f29bbc4868cb98ce2235b882e5371d3:broker/compat.py'],text=True)
        start=original.index('def validate_syncopia_memory_payload(')
        end=original.index('\ndef submit_syncopia_memory(',start)
        self.assertEqual(inspect.getsource(validate_syncopia_memory_payload).strip(),original[start:end].strip())
        body=self.body();old=dict(body['payload'],model='qwen3.8:ad-iq2-xs',think='low')
        normalized=validate_syncopia_memory_payload(old);self.assertEqual(normalized['think'],'low')
        with self.assertRaisesRegex(ValueError,'requires think=low'):
            validate_syncopia_memory_payload(dict(old,think=False))
        # Old path still accepts explicit top-level runtime overrides without native metadata.
        before=list(self.native.calls)
        body['profile']='syncopia-memory-qwen38';body['model']='qwen3.8:ad-iq2-xs'
        body['payload']['think']='low'
        got=self.broker.submit(**body)
        self.assertEqual(got['payload']['think'],'low');self.assertEqual(self.native.calls,before)
