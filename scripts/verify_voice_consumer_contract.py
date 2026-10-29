#!/usr/bin/env python3
"""Run synthetic loopback integration against the supplied immutable consumer checkout.

No production services or ASR are contacted. Pass its path as argv[1] and use
an existing Python environment with its dependencies installed.
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tests.test_voice_cpu import CPUVoiceContractTests

consumer=Path(sys.argv[1]).resolve()
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=consumer,text=True).strip()
if revision != '79f58d22ef83d41e4a70fa423b455bb8af241826':
    raise SystemExit('wrong consumer revision')
sys.path.insert(0,str(consumer))
import broker_voice_adapter as bridge
import nextcloud_helper as helper

fixture=CPUVoiceContractTests('test_async_consumer_contract_replay_result_correlation_no_wake')
fixture.setUp()
try:
    url='http://127.0.0.1:'+str(fixture.server.server_port)
    status,caps=fixture.request('/v1/voice/capabilities')
    assert status==200
    config=dict(accepted=True,url=url,profile='voice-native-cpu-v1',capabilities_sha256=helper.digest(caps))
    assert bridge.capability(config,'segments-v1')[0] == caps
    intent=dict(chunk_key='1'*64,audio_sha256=fixture.sha,response_format='segments-v1',
        profile=config['profile'],capability=caps,capabilities_sha256=helper.digest(caps),
        payload_sha256=helper.digest(bridge.payload(fixture.audio,'segments-v1')),
        external_id='arch-voice-'+'1'*64)
    body=bridge.request(intent,fixture.audio)
    status,ack=bridge.http(url,'POST','/v1/jobs',helper.canonical(body))
    assert status==202
    row=dict(intent=intent,broker_id=None,alias='2'*32,policy_sha256='3'*64)
    bridge.verify_job(ack,row)
    row['broker_id']=ack['id']
    assert fixture.broker.dispatch_once(policy=fixture.policy)
    status,job=bridge.http(url,'GET','/v1/jobs/'+ack['id'])
    assert status==200
    result=bridge.normalize(job,row)
    assert result['status']=='done' and result['result']['transcript']=='synthetic'
    assert result['result']['timing']['segments'][0]['start']==.2
    assert result['broker_voice']['broker_id']==ack['id']
    print(json.dumps({'status':'PASS','consumer_revision':revision,'fixture':'synthetic-loopback-no-ASR',
                      'capability':'accepted','post_ack':'correlated','terminal_input_hash':'verified',
                      'native_result':'consumer-normalized'}))
finally:
    fixture.doCleanups()
