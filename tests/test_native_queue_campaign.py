"""Synthetic text/offline campaign contracts plus exact existing-server blockers.

Raw-thinking markers are artificial regression sentinels, not real model data.
NATIVE_QUEUE_TEST_BASE_EXPORT may select immutable deployed-base source bytes.
"""
import copy
import hashlib
import importlib
import importlib.util
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request,ProxyHandler,build_opener
from broker import native_queue_campaign as campaign
from broker import nightly_client as files

SENTINEL='SYNTHETIC_REASONING_CONTRACT_PROBE'


class CampaignPlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);cases=[];pins={}
        for i in range(4):
            name='case-synthetic-'+str(i);path=self.root/name/'messages.json';path.parent.mkdir()
            path.write_text(json.dumps(dict(messages=[dict(role='user',content='Synthetic text literal https://example.invalid/media.pdf '+str(i))])))
            digest=hashlib.sha256(path.read_bytes()).hexdigest();pins[name]=digest
            cases.append(dict(case_id=name,runner_input=name+'/messages.json',runner_input_sha256=digest))
        self.manifest=self.root/'manifest.json'
        self.manifest.write_text(json.dumps(dict(schema=files.ASTRA_SCHEMA,case_count=4,cases=cases)))
        for name,value in [('CASE_INPUTS',pins),('ASTRA_MANIFEST_SHA256',hashlib.sha256(self.manifest.read_bytes()).hexdigest())]:
            p=patch.object(files,name,value);p.start();self.addCleanup(p.stop)
        self.draft=campaign.prepare('synthetic-campaign')

    def test_six_existing_models_24_baselines_missing_controls_do_not_block(self):
        with patch('socket.socket') as network:
            plan=campaign.build(self.draft,self.manifest);network.assert_not_called()
        self.assertEqual(len(plan['cells']),24)
        self.assertEqual({c['model'] for c in plan['cells']},{r[0] for r in campaign.PRESETS.values()})
        for cell in plan['cells']:
            req=campaign.request_for(plan,cell)
            self.assertEqual(req['source'],'openclaw')
            self.assertNotIn('images',req['payload']['messages'][0])
            self.assertIn('https://example.invalid/media.pdf',req['payload']['messages'][0]['content'])
            self.assertIsNone(cell['observed_effective_options'])
            self.assertEqual(cell['mode_basis'],'configured_baseline_not_toggle_proof')
        qwen=next(c for c in plan['cells'] if c['profile']=='openclaw')
        vl=next(c for c in plan['cells'] if c['profile']=='openclaw-qwen3-vl')
        nano33=next(c for c in plan['cells'] if c['profile']=='openclaw-nemotron3')
        gemma=next(c for c in plan['cells'] if c['profile']=='openclaw-gemma4-26b')
        self.assertEqual((qwen['requested_think'],vl['requested_think']),('low','max'))
        self.assertEqual(vl['requested_options'],dict(num_ctx=212992,num_predict=16384,temperature=0))
        self.assertEqual(nano33['requested_options'],dict(num_ctx=131072,num_predict=8192,temperature=0))
        self.assertEqual((gemma['requested_think'],gemma['requested_options']),(False,dict(num_ctx=32768,num_predict=512,temperature=0)))
        self.assertTrue(plan['gate']['deploy_needed_for_full_acceptance'])
        self.assertFalse(plan['gate']['admission_enabled'])

    def add_toggle(self,profile):
        row=next(r for r in self.draft['models'] if r['profile']==profile)
        row['verified_native_thinking']=dict(model=row['model'],digest=row['digest'],origin='/api/show',thinking=dict(values=[False,True],default=True))
        value=copy.deepcopy(row['baseline']);value['think']=not value['think']
        self.draft['extra_modes'].append(dict(profile=profile,settings=value))

    def test_extras_need_exact_boolean_evidence_and_lifetime_32_plan(self):
        self.add_toggle('openclaw-gemma4');self.add_toggle('openclaw-gemma4-26b')
        self.assertEqual(len(campaign.build(self.draft,self.manifest)['cells']),32)
        self.add_toggle('openclaw-nemotron-nano')
        with self.assertRaises(files.Closed):campaign.build(self.draft,self.manifest)
        self.draft=campaign.prepare('synthetic-campaign')
        q=next(r for r in self.draft['models'] if r['profile']=='openclaw')
        mode=copy.deepcopy(q['baseline']);mode['think']=False
        self.draft['extra_modes']=[dict(profile='openclaw',settings=mode)]
        with self.assertRaises(files.Closed):campaign.build(self.draft,self.manifest)
        q['verified_native_thinking']=dict(model=q['model'],digest=q['digest'],origin='/api/show',thinking=dict(values=['low','max'],default='low'))
        with self.assertRaises(files.Closed):campaign.build(self.draft,self.manifest)

    def test_caps_identity_and_raw_metadata_rejected_before_freeze(self):
        for change in ('caps','digest','baseline-think','raw'):
            draft=copy.deepcopy(self.draft);row=draft['models'][0]
            if change=='caps':row['baseline']['options']['num_ctx']=row['caps']['num_ctx']+1
            if change=='digest':row['digest']='0'*64
            if change=='baseline-think':row['baseline']['think']=False
            if change=='raw':row['raw_response']={'thinking':SENTINEL}
            with self.assertRaises(files.Closed):campaign.build(draft,self.manifest)

    def test_frozen_and_fixture_hashes_immutable_no_reference_reads(self):
        plan=campaign.build(self.draft,self.manifest);p=self.root/'frozen.json';files.new_json(p,plan)
        self.assertEqual(campaign.load_frozen(p)[0],plan)
        plan['cells'][0]['requested_options']['num_ctx']=1024
        files.new_json(self.root/'edited.json',plan)
        with self.assertRaises(files.Closed):campaign.load_frozen(self.root/'edited.json')
        (self.root/'case-synthetic-0/messages.json').write_text('{}')
        with self.assertRaises(files.Closed):campaign.load_frozen(p)

    def test_confirmed_gate_never_posts_or_creates_execution_state(self):
        with patch('socket.socket') as network:
            with self.assertRaisesRegex(files.Closed,'blocked_existing_server_contract'):campaign.admit()
            self.assertEqual(campaign.main(['admit']),2)
            network.assert_not_called()


def existing_backend():
    export=os.environ.get('NATIVE_QUEUE_TEST_BASE_EXPORT')
    if export:
        root=Path(export)/'broker'
        spec=importlib.util.spec_from_file_location('_existing_native_broker',root/'__init__.py',submodule_search_locations=[str(root)])
        module=importlib.util.module_from_spec(spec);import sys;sys.modules[spec.name]=module;spec.loader.exec_module(module)
        return importlib.import_module(spec.name+'.service'),importlib.import_module(spec.name+'.http')
    return importlib.import_module('broker.service'),importlib.import_module('broker.http')


class ExistingServerContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        service,http=existing_backend()
        class Fake:
            model='nemotron3:33b'
            def ps(self,**kw):return {'models':[dict(name=self.model,context_length=1024,digest='0'*64)]}
            def ensure_model_ready(self,model,**kw):self.model=model;return True
            def run(self,kind,request):
                return dict(model=request['model'],done=True,done_reason='stop',message=dict(role='assistant',content='Synthetic final',thinking=SENTINEL,reasoning=SENTINEL),eval_count=2)
        class Wol:
            def wake(self):pass
        self.broker=service.Broker(Path(self.tmp.name)/'queue.sqlite',Fake(),Wol())
        self.addCleanup(self.broker.db.close)
        self.server=http.serve(self.broker,port=0);self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.addCleanup(self.server.server_close);self.addCleanup(self.server.shutdown)
        self.opener=build_opener(ProxyHandler({}));self.base='http://127.0.0.1:'+str(self.server.server_port)

    def submit(self,profile='openclaw-nemotron3'):
        model,digest,ctx,out,think=campaign.PRESETS[profile]
        req=dict(profile=profile,source='openclaw',kind='chat',external_id='synthetic-key-'+profile,source_item_id='synthetic-case',
                 payload=dict(messages=[dict(role='user',content='Synthetic text')],think=think,options=dict(num_ctx=ctx,num_predict=out,temperature=0),truncate=False,shift=False))
        with self.opener.open(Request(self.base+'/v1/jobs',data=json.dumps(req).encode(),headers={'Content-Type':'application/json'}),timeout=5) as response:
            self.assertEqual(response.status,202);return json.load(response)

    def test_existing_six_profiles_admit_with_uuid_and_get_key_recovery_no_new_lane(self):
        for profile in campaign.PRESETS:
            job=self.submit(profile)
            with self.opener.open(self.base+'/v1/correlations?source=openclaw&external_id=synthetic-key-'+profile+'&limit=2',timeout=5) as response:
                rows=json.load(response)['jobs']
            self.assertEqual(len(rows),1);self.assertEqual(rows[0]['job_id'],job['id'])
            self.assertEqual((rows[0]['source'],rows[0]['profile']),('openclaw',profile))
            self.assertNotIn('input_hash',job)  # absent from default OpenClaw public status

    def test_confirmed_raw_reasoning_storage_and_unverified_effective_identity(self):
        job=self.submit();self.assertTrue(self.broker.dispatch_once(frozenset({'openclaw'})))
        got=self.broker.status(job['id']);self.assertEqual(got['state'],'completed')
        stored=self.broker.db.execute('SELECT result_json FROM jobs WHERE id=?',(job['id'],)).fetchone()[0]
        self.assertIn(SENTINEL,stored)  # proves blocker, not a privacy PASS
        self.assertIn(SENTINEL,json.dumps(got['result']))
        self.assertNotIn('execution_evidence',got['result'])
        self.assertNotIn('context_length',got['result'])
        self.assertNotIn('model_digest',got['result'])
        # Server accepted a final despite the fake executor reporting ctx1024/wrong digest.
        self.assertEqual(self.broker.ollama.ps()['models'][0]['context_length'],1024)
        for projection in ('nextcloud-final-v1','openclaw-final-v1'):
            with self.assertRaises(HTTPError) as error:
                self.opener.open(self.base+'/v1/jobs/'+job['id']+'?projection='+projection,timeout=5)
            self.assertEqual(error.exception.code,400)


if __name__=='__main__':unittest.main()
