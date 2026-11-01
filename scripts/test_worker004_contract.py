#!/usr/bin/env python3
"""Real worker004 client over loopback; no PG/live credentials/model calls."""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from unittest.mock import patch

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--worker-tree',type=Path,required=True)
p.add_argument('--scratch',type=Path,required=True)
a=p.parse_args();root=Path(__file__).resolve().parents[1]
for path in (root,a.worker_tree.resolve(),a.scratch.resolve()):
    if not path.is_relative_to('/media/dankar/Plex'):p.error('Plex only')
a.scratch.mkdir(parents=True,exist_ok=True);os.environ['TMPDIR']=str(a.scratch.resolve());sys.dont_write_bytecode=True
sys.path[:0]=[str(root),str(a.worker_tree.resolve()),str(a.worker_tree.resolve()/'tests')]
def guard(event,args):
    if event in {'socket.connect','socket.sendto'}:
        address=args[-1]
        if isinstance(address,tuple) and address[0] not in {'127.0.0.1','localhost','::1'}:raise AssertionError('external sockets forbidden')
sys.addaudithook(guard)
import evaluation as e
import nextcloud_helper as h
from test_evaluation import inputs,MemoryStore
from broker.http import serve
from broker.service import Broker,SourcePolicy
assert Path(e.__file__).resolve().parent==a.worker_tree.resolve()
class GPU:
    def __init__(self):self.calls=[]
    def ps(self):return {'models':[]}
    def ensure_model_ready(self,model,**kwargs):return True
    def run(self,kind,request):
        assert kind=='generate' and request['model']=='qwen3-vl:30b'
        assert request['think'] is True and request['options']==dict(num_ctx=16384,num_predict=8192,temperature=0)
        assert request['_broker_timeout_seconds']==900
        self.calls.append(request['model'])
        return dict(model=request['model'],response='synthetic final',thinking='NEVER_TRANSMIT',done=True,done_reason='stop',eval_count=10)
class WOL:
    def wake(self):pass
with tempfile.TemporaryDirectory(dir=a.scratch) as temp:
    work=Path(temp);inventory,matrix,prep=inputs(work)
    matrix['variants']=[v for v in matrix['variants'] if v['profile']=='nextcloud-eval-qwen']
    plan=e.build_plan(inventory,matrix,work,work,prep);store=MemoryStore(plan)
    policyfile=work/'policy.json';policyfile.write_text(json.dumps(dict(sources=dict(nextcloud=dict(weight=7,enabled=True,admission_allowed=True)))))
    before=policyfile.read_bytes();policy=SourcePolicy(policyfile);gpu=GPU()
    broker=Broker(work/'jobs.db',gpu,WOL(),source_policy=policy)
    server=serve(broker,port=0,policy=policy);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        with patch.object(h,'BROKER',f'http://127.0.0.1:{server.server_port}'):
            client=e.EvaluationBroker()
            assert e.producer(store,plan['run_id'],client)['admitted']==10
            assert gpu.calls==[]
            assert broker.db.execute("SELECT count(*) FROM jobs WHERE state='queued'").fetchone()[0]==10
            assert e.collector(store,plan['run_id'],client)['pending']==10
            for _ in range(10):assert broker.dispatch_once(policy=policy)
            assert e.collector(store,plan['run_id'],e.EvaluationBroker())['completed']==10
            first=store.get(plan['run_id'],store.keys(plan['run_id'])[0])
            wire=client.status(first['job_id'])
            assert wire['request_contract']['options']==matrix['variants'][0]['options']
            assert 'NEVER_TRANSMIT' not in json.dumps(wire) and 'payload' not in wire
            assert e.producer(store,plan['run_id'],client)['admitted']==0
            assert len(gpu.calls)==10 and policyfile.read_bytes()==before
            assert 'NEVER_TRANSMIT' not in json.dumps(store.rows)
            # Actual correlation + projected status recovery after lost ACK.
            for task in store.rows.values():task['status']='uncertain'
            assert e.producer(store,plan['run_id'],e.EvaluationBroker())['admitted']==10
            assert broker.db.execute('SELECT count(*) FROM jobs').fetchone()[0]==10
            assert e.collector(store,plan['run_id'],client)['completed']==10
            print(json.dumps(dict(admitted_before_poll=10,completed=10,correlation_recovered_without_new_jobs=10,
                 gpu='fake',worker_evaluation_module=e.__file__,broker_module=str(root/'broker/service.py'),
                 policy_unchanged=True,thinking_transmitted=False,pg_used=False,live_inference=False)),flush=True)
    finally:server.shutdown();thread.join();server.server_close();broker.db.close()
