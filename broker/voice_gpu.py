"""Request-owned Windows voice custody under the existing broker queue lease.

Never unload Ollama: a resident model may still belong to an out-of-band caller.
A tracked worker request owns a durable existing GPU fence until release proof.
"""
from contextlib import contextmanager
from contextvars import ContextVar
import json
import time
import urllib.request
import uuid

PREFIX = 'voice|'
CURRENT = ContextVar('queued_voice_gpu', default=None)


def parse_fence(fence):
    value = fence.get('request_id', '') if fence else ''
    if not isinstance(value,str) or not value.startswith(PREFIX):return None
    try:
        _, profile, request_id = value.split('|')
        if str(uuid.UUID(request_id)) != request_id:return None
        from .voice import _HTTP_WORKERS
        if profile not in _HTTP_WORKERS:return None
        return profile, request_id
    except (ValueError,TypeError):return None


def worker(profile, path, *, request_id=None, release=False):
    from .voice import _HTTP_WORKERS
    import os
    prefix, _, model = _HTTP_WORKERS[profile]
    url=os.environ.get('BROKER_'+prefix+'_URL','').rstrip('/')
    token=os.environ.get('BROKER_'+prefix+'_TOKEN','')
    if not url or not token:raise RuntimeError('voice worker custody unconfigured')
    headers={'Authorization':'Bearer '+token}
    if request_id:headers['X-Request-Id']=request_id
    req=urllib.request.Request(url+path,headers=headers,method='POST' if release else 'GET',
        data=b'' if release else None)
    with urllib.request.urlopen(req,timeout=5) as response:
        raw=response.read(65537)
        if len(raw)>65536:raise RuntimeError('voice custody response exceeds bound')
        result=json.loads(raw)
    if not isinstance(result,dict):raise RuntimeError('voice custody response invalid')
    return result


def status(profile):
    from .voice import _HTTP_WORKERS
    result=worker(profile,'/v1/status')
    if (result.get('model')!=_HTTP_WORKERS[profile][2]
            or result.get('request_owned_release') is not True
            or type(result.get('request_tracking')) is not bool
            or type(result.get('active_jobs')) is not int
            or result.get('active_jobs')<0
            or type(result.get('model_loaded')) is not bool):
        raise RuntimeError('request-owned voice custody capability unavailable')
    return result


def reconcile(broker, fence):
    parsed=parse_fence(fence)
    if not parsed:return False
    profile, request_id=parsed
    try:
        observed=status(profile)
        if observed['active_jobs']!=0 or observed.get('last_request_id')!=request_id:return False
        if (observed['model_loaded'] or observed.get('gpu_release_confirmed') is not True
                or observed.get('last_request_gpu_released') is not True):
            # Worker must check X-Request-Id under its own admission lock.
            released=worker(profile,'/v1/release',request_id=request_id,release=True)
            if released.get('gpu_released') is not True:return False
            observed=status(profile)
        if (observed['active_jobs']==0 and observed['model_loaded'] is False
                and observed.get('last_request_id')==request_id
                and observed.get('gpu_release_confirmed') is True
                and observed.get('last_request_gpu_released') is True):
            broker.clear_executor_fence(fence['request_id'])
            return broker.executor_fence() is None
    except Exception:
        # Unknown transport/release is still owned; never turn it into a new
        # inference, completion claim or an unfenced LLM allocation.
        return False
    return False


@contextmanager
def queued(broker, row):
    token=CURRENT.set(Guard(broker,row['id']))
    try:yield
    finally:CURRENT.reset(token)


class Claim:
    def __init__(self,request_id,deadline):
        self.request_id,self.deadline=request_id,deadline
        self.upload_started=False
    def remaining(self):
        value=self.deadline-time.monotonic()
        if value<=0:raise TimeoutError('queued voice GPU execution deadline')
        return value


class Guard:
    def __init__(self,broker,job_id):self.broker,self.job_id=broker,job_id

    @contextmanager
    def claim(self,profile,timeout):
        deadline=time.monotonic()+timeout
        # Hold the current durable queue job while waiting for natural expiry.
        # No unload/keep_alive request, cancellation or parallel Windows call.
        while True:
            residents=self.broker.ollama.ps(timeout_seconds=5)
            if not isinstance(residents,dict) or not isinstance(residents.get('models'),list):
                raise RuntimeError('Ollama residency unknown')
            if not residents['models']:break
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('Ollama residency did not naturally release before voice deadline')
            time.sleep(min(1,remaining))
        observed=status(profile)
        if (observed['active_jobs']!=0 or observed['model_loaded']
                or observed.get('gpu_release_confirmed') is not True
                or self.broker.executor_fence() is not None):
            raise RuntimeError('voice GPU not unambiguously idle')
        request_id=str(uuid.uuid4())
        encoded=PREFIX+profile+'|'+request_id
        self.broker.set_executor_fence(encoded,self.job_id)
        claim=Claim(request_id,deadline)
        # The fence precedes upload and survives timeout/process restart.
        try:
            claim.remaining()
            yield claim
        finally:
            # Before entering HTTP transport there can be no remote request.
            # After transport starts, uncertainty still requires worker proof.
            if not claim.upload_started:
                self.broker.clear_executor_fence(encoded)
            # HTTP response may arrive just before worker's finally clears
            # active_jobs/records last_request_id; allow that bounded race.
            for _ in range(10 if claim.upload_started else 0):
                if reconcile(self.broker,{'request_id':encoded,'job_id':self.job_id}):break
                try:
                    observed=status(profile)
                    if observed['active_jobs']==0:break
                except Exception:break
                time.sleep(.1)
