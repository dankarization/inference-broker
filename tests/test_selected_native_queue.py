"""Synthetic pinned overlay + exact existing native chat transport; no inference."""
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from broker import selected_native_queue as selected
from broker import nightly_client as files
import test_native_queue_campaign as baseprobe
SENTINEL=baseprobe.SENTINEL


class SelectedNativeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        cases=[];pins={}
        for name in selected.ORDER:
            p=self.root/name/'messages.json';p.parent.mkdir();p.write_text(json.dumps(dict(messages=[dict(role='user',content='Synthetic only '+name)])))
            digest=hashlib.sha256(p.read_bytes()).hexdigest();pins[name]=digest
            cases.append(dict(case_id=name,runner_input=name+'/messages.json',runner_input_sha256=digest))
        self.cases=self.root/'manifest.json';self.cases.write_text(json.dumps(dict(schema=files.ASTRA_SCHEMA,case_count=4,cases=cases)))
        selection=dict(schema='nightly-local-model-selection-v1',candidates=[],stage1=dict(cells=[]))
        for cid,profile,think in selected.CANDIDATES:
            selection['candidates'].append(dict(id=cid,model=selected.PRESETS[profile][0],queue_profile=profile,
                planned=dict(num_ctx=65536,num_predict=8192,temperature=0,thinking=think,keep_alive='5m',output_format='freeform',format_constraint=None)))
            selection['stage1']['cells'] += [dict(candidate_id=cid,case_id=c) for c in selected.ORDER]
        self.selection=self.root/'selection.json';self.selection.write_text(json.dumps(selection))
        for module,key,value in [(files,'CASE_INPUTS',pins),(files,'ASTRA_MANIFEST_SHA256',hashlib.sha256(self.cases.read_bytes()).hexdigest()),
            (selected,'SELECTION_SHA',hashlib.sha256(self.selection.read_bytes()).hexdigest())]:
            p=patch.object(module,key,value);p.start();self.addCleanup(p.stop)
        self.plan=selected.build(self.selection,self.cases)
        self.frozen=self.root/'frozen.json';files.new_json(self.frozen,self.plan)
        self.plan,self.digest=selected.load(self.frozen)

    def backend(self):
        fixture=baseprobe.ExistingServerContractTests('test_confirmed_raw_reasoning_storage_and_unverified_effective_identity')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        transport=selected.NativeTransport(fixture.base)
        original=transport.request
        def reads(path,body=None):
            self.assertIsNone(body)
            if path=='/v1/sources':return dict(policy=dict(sources=dict(openclaw=dict(enabled=True,admission_allowed=True))))
            return original(path)
        p=patch.object(transport,'request',side_effect=reads);p.start();self.addCleanup(p.stop)
        journal=files.Journal(self.root/'private/journal.sqlite',self.digest,transport.base);self.addCleanup(journal.close)
        return fixture,transport,journal

    def test_exact_16_overlay_order_typed_modes_and_immutable_drift(self):
        self.assertEqual(len(self.plan['cells']),16)
        self.assertEqual([c['case_id'] for c in self.plan['cells']],selected.ORDER*4)
        self.assertEqual([c['think'] for c in self.plan['cells']],['low']*4+[True]*12)
        for cell in self.plan['cells']:
            wire=selected.wire_request(self.plan,cell)
            self.assertEqual(wire['options'],dict(num_ctx=65536,num_predict=8192,temperature=0))
            self.assertEqual(wire['keep_alive'],'5m');self.assertTrue(wire['stream'])
            self.assertNotIn('source',wire);self.assertNotIn('tools',wire);self.assertNotIn('format',wire)
        altered=copy.deepcopy(self.plan);altered['cells'][8]['options']['num_predict']=512
        files.new_json(self.root/'wrong.json',altered)
        with self.assertRaises(files.Closed):selected.load(self.root/'wrong.json')
        self.selection.write_text('{}')
        with self.assertRaises(files.Closed):selected.load(self.frozen)

    def test_native_lost_header_ack_get_only_reconcile_no_repeat_and_privacy(self):
        fixture,transport,journal=self.backend();cell=self.plan['cells'][0]
        original=transport.submit_native;calls=[]
        def lose(wire,key):
            calls.append(key);response=original(wire,key);response.close();raise files.Uncertain('synthetic lost ACK')
        with patch.object(transport,'submit_native',side_effect=lose):
            with self.assertRaises(files.Uncertain):selected.admit(self.plan,cell,journal,transport)
        self.assertIsNone(journal.row(cell)['job_id'])
        with patch.object(transport,'submit_native') as post:
            with self.assertRaises(files.Closed):selected.admit(self.plan,cell,journal,transport)
            queued=selected.collect(self.plan,cell,journal,transport,reconcile=True);post.assert_not_called()
        self.assertEqual(queued['state'],'queued');self.assertEqual(len(calls),1)
        with self.assertRaises(files.Closed):selected.admit(self.plan,self.plan['cells'][1],journal,transport)
        fixture.broker.dispatch_once(frozenset({'openclaw'}))
        final=selected.collect(self.plan,cell,journal,transport)
        self.assertEqual(final['state'],'completed');self.assertEqual(final['effective_status'],'UNVERIFIED')
        self.assertNotIn(SENTINEL,'\n'.join(journal.db.iterdump()))
        self.assertIn(SENTINEL,fixture.broker.db.execute('SELECT result_json FROM jobs WHERE id=?',(queued['id'],)).fetchone()[0])

    def test_all_16_native_header_receipts_dispatch_explicit_overrides_and_no_stage2(self):
        fixture,transport,journal=self.backend();requests=[];original=fixture.broker.ollama.run
        def run(kind,request):requests.append(copy.deepcopy(request));return original(kind,request)
        fixture.broker.ollama.run=run
        for cell in self.plan['cells']:
            receipt=selected.admit(self.plan,cell,journal,transport)
            self.assertEqual(journal.row(cell)['job_id'],receipt['job_id'])
            fixture.broker.dispatch_once(frozenset({'openclaw'}))
            result=selected.collect(self.plan,cell,journal,transport);self.assertTrue(result['result']['acceptable'])
        self.assertEqual(len(requests),16)
        for request in requests:
            self.assertEqual(request['options'],dict(num_ctx=65536,num_predict=8192,temperature=0))
            self.assertEqual(request['keep_alive'],'300s')
        self.assertIs(requests[8]['think'],True)  # Gemma26 alias false/512 never applied
        self.assertEqual(journal.db.execute('SELECT count(*) FROM cells').fetchone()[0],16)
        with patch.object(transport,'submit_native') as post:
            with self.assertRaises(files.Closed):selected.admit(self.plan,self.plan['cells'][0],journal,transport)
            post.assert_not_called()
        self.assertNotIn(SENTINEL,'\n'.join(journal.db.iterdump()))


if __name__=='__main__':unittest.main()
