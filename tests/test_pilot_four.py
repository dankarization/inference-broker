"""Real four-profile pilot HTTP client; SQLite durability, no real inference."""
import copy,json,threading,unittest,uuid
from pathlib import Path
from unittest.mock import patch
from broker.service import Broker,SourceQueueFull,SourceAdmissionBlocked
from broker.profiles import PROFILES,EVALUATION_PROFILE_NAMES
from broker.pilot_client import PilotClient,PilotRejected,PilotUncertain
from broker.http import serve
from broker.evaluation import PILOT_MAX_ACTIVE_JOBS,PILOT_MAX_ACTIVE_INPUT_BYTES
from test_evaluation import EvaluationTests,FakeOllama,FakeWol


class FourPilotTests(unittest.TestCase):
    setUp=EvaluationTests.setUp;payload=EvaluationTests.payload
    def client(self):
        server=serve(self.broker,port=0,policy=self.policy)
        server.daemon_threads=False
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        return PilotClient(f'http://127.0.0.1:{server.server_port}')
    def test_real_client_four_profiles_replay_restart_recover_and_distinct_models(self):
        client=self.client();ids=[]
        for profile in sorted(EVALUATION_PROFILE_NAMES):
            key='doc100-fixture-'+profile;item='synthetic-doc-1';payload=self.payload()
            job=client.submit(key,item,profile,payload);ids.append(job['id'])
            self.assertEqual(client.submit(key,item,profile,payload)['id'],job['id'])
            self.assertEqual(client.recover(key,item,profile,payload)['id'],job['id'])
            for args in [(key,'other-item',profile,payload),(key,item,profile,payload|dict(messages=[{'role':'user','content':'changed'}]))]:
                with self.assertRaises(PilotRejected):client.submit(*args)
            self.assertTrue(self.broker.dispatch_once(frozenset({'pilot-mainpc'}),self.policy))
            result=client.status(job['id'],key,item,profile,payload)
            self.assertEqual(result['state'],'completed')
            self.assertEqual(result['result']['model'],PROFILES[profile].model)
            self.assertIs(self.ollama.requests[-1][1]['think'],False)
        self.assertEqual(len(set(ids)),4)
        other=Broker(self.broker.database,FakeOllama(),FakeWol(),source_policy=self.policy)
        try:self.assertEqual(other.db.execute("SELECT count(*) FROM jobs WHERE source='pilot-mainpc'").fetchone()[0],4)
        finally:other.db.close()
        self.policy.set_admission_allowed('pilot-mainpc',False)
        for profile in EVALUATION_PROFILE_NAMES:
            self.assertIsNotNone(client.recover('doc100-fixture-'+profile,'synthetic-doc-1',profile,self.payload()))
        with self.assertRaises(PilotRejected) as caught:
            client.submit('new','doc','eval-gemma4-12b',self.payload())
        self.assertEqual(caught.exception.status,403)
    def test_pilot_count_bytes_disk_backpressure_and_replay_at_cap(self):
        self.assertEqual((PILOT_MAX_ACTIVE_JOBS,PILOT_MAX_ACTIVE_INPUT_BYTES),(512,512*1024**2))
        client=self.client();profile='eval-gemma4-26b';p=self.payload()
        first=client.submit('cap','doc',profile,p)
        # Synthetic occupancy fixture, not512 claimed admissions; preserve real cap path.
        row=dict(self.broker.db.execute('SELECT * FROM jobs WHERE id=?',(first['id'],)).fetchone())
        columns=list(row);sql='INSERT INTO jobs('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')'
        with self.broker.db:
            self.broker.db.executemany(sql,[[str(uuid.uuid4()) if c=='id' else
                'seed-'+str(i) if c=='external_id' else row[c] for c in columns] for i in range(511)])
        with self.assertRaises(PilotRejected) as caught:client.submit('over','doc',profile,p)
        self.assertEqual(caught.exception.status,429)
        self.assertEqual(client.submit('cap','doc',profile,p)['id'],first['id'])
        with self.broker.lock,self.broker.db:
            self.broker.db.execute("UPDATE jobs SET state='failed' WHERE id<>?",(first['id'],))
            self.broker.db.execute('UPDATE jobs SET input_bytes=? WHERE id=?',(PILOT_MAX_ACTIVE_INPUT_BYTES,first['id']))
        with self.assertRaises(PilotRejected) as caught:client.submit('over-bytes','doc',profile,p)
        self.assertEqual(caught.exception.status,429)
        self.broker.db.execute('UPDATE jobs SET input_bytes=1 WHERE id=?',(first['id'],));self.broker.db.commit()
        with patch.object(self.broker.storage,'_disk_capacity',return_value=dict(admission_available_bytes=0)):
            with self.assertRaises(PilotRejected) as caught:client.submit('disk','doc',profile,p)
        self.assertEqual(caught.exception.status,400) # existing storage contract, not a fake global429
    def test_unknown_post_is_not_replayed_and_recovery_checks_identity(self):
        client=self.client();p=self.payload();profile='eval-gemma4-12b'
        for bad in (' padded','x'*257,'line\nfeed'):
            with self.assertRaises(ValueError):client.submit(bad,'doc',profile,p)
        original=client._request;calls=[]
        def lose_ack(path,body=None):
            raw=original(path,body)
            if body is not None:
                calls.append(body['external_id']);raise PilotUncertain('synthetic lost ACK')
            return raw
        with patch.object(client,'_request',side_effect=lose_ack):
            with self.assertRaises(PilotUncertain):client.submit('lost','doc',profile,p)
            recovered=client.recover('lost','doc',profile,p)
            self.assertIsNotNone(recovered)
            self.assertEqual(calls,['lost'])
            self.assertIsNone(client.recover('absent','doc',profile,p))
        self.assertEqual(self.broker.db.execute('SELECT count(*) FROM jobs').fetchone()[0],1)
        with patch.object(client,'_request',return_value={'jobs':[{'job_id':recovered['id']}]*2}):
            with self.assertRaises(PilotUncertain):client.recover('lost','doc',profile,p)
        with self.assertRaises(PilotUncertain):client.status(recovered['id'],'lost','changed',profile,p)
        for borrowed in ('openclaw','nextcloud-eval-qwen'):
            with self.assertRaisesRegex(ValueError,'dedicated evaluation profile'):
                self.broker.submit(borrowed,'chat',p,source='pilot-mainpc')
        with patch.object(client,'_request',return_value={'jobs':None}):
            with self.assertRaises(PilotUncertain):client.recover('lost','doc',profile,p)

    def test_machine_contract_matches_all_four_profiles_and_limits(self):
        from broker.evaluation import PILOT_MODEL_DIGESTS
        doc=json.loads((Path(__file__).resolve().parents[1]/'docs/pilot-doc100-contract.json').read_text())
        self.assertEqual(len(doc['pilot_profiles']),4)
        self.assertEqual({r['profile'] for r in doc['pilot_profiles']},set(EVALUATION_PROFILE_NAMES))
        for r in doc['pilot_profiles']:
            p=PROFILES[r['profile']]
            self.assertEqual((r['source'],r['kind'],r['model']),('pilot-mainpc','chat',p.model))
            self.assertEqual(r['intended_model_digest'],PILOT_MODEL_DIGESTS[r['profile']])
            self.assertEqual(r['options'],dict(num_ctx=p.max_context,num_predict=p.max_output,temperature=0))
            self.assertEqual((r['max_images'],r['timeout_seconds']),(p.max_images,p.request_timeout_seconds))
        self.assertEqual(doc['capacity']['pilot']['active_jobs'],PILOT_MAX_ACTIVE_JOBS)
        self.assertEqual(doc['capacity']['pilot']['active_normalized_payload_bytes'],PILOT_MAX_ACTIVE_INPUT_BYTES)
        self.assertIsNone(doc['capacity']['global_limits']['active_job_count_hard_cap'])
        self.assertEqual(doc['nextcloud_verifier']['profile'],'nextcloud-verify-gemma26')

    def test_actual_400_keyed_durable_admissions_without_execution(self):
        client=self.client();ids=[]
        for doc in range(100):
            for profile in sorted(EVALUATION_PROFILE_NAMES):
                ids.append(client.submit(f'doc100-{doc}-{profile}',f'fixture-doc-{doc}',profile,self.payload())['id'])
        self.assertEqual(len(set(ids)),400)
        self.assertEqual(self.broker.db.execute("SELECT count(*) FROM jobs WHERE source='pilot-mainpc' AND state='queued'").fetchone()[0],400)
        self.assertEqual(self.ollama.requests,[])
        other=Broker(self.broker.database,FakeOllama(),FakeWol(),source_policy=self.policy)
        try:self.assertEqual(other.db.execute('SELECT count(*) FROM jobs').fetchone()[0],400)
        finally:other.db.close()
        print('PILOT400_RECEIPT '+json.dumps(dict(real_http=True,durable_jobs=400,unique_ids=400,inference=0)),flush=True)
        (Path(self.broker.database).parent/'pilot400-receipt.json').write_text(json.dumps(
            dict(synthetic_images=True,real_http=True,durable_jobs=400,unique_ids=400,inference=0,
                 pilot_max_active_jobs=PILOT_MAX_ACTIVE_JOBS,pilot_max_active_input_bytes=PILOT_MAX_ACTIVE_INPUT_BYTES))+'\n')


def load_tests(loader,tests,pattern):return loader.loadTestsFromTestCase(FourPilotTests)


if __name__=='__main__':unittest.main()
