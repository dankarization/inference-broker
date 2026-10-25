"""Bounded pilot HTTP client. Persist intent before POST; NEVER auto-replay unknown POST.

No CLI auto-submit, inference, policy mutation, cloud fallback or output approval.
"""
from __future__ import annotations
import json,uuid
from urllib.parse import urlsplit,urlencode
from urllib.request import Request,build_opener,ProxyHandler,HTTPRedirectHandler
from urllib.error import HTTPError,URLError
from .evaluation import normalize_request
from .profiles import EVALUATION_PROFILE_NAMES,PROFILES
from .storage import content_evidence

MAX_RESPONSE_BYTES=34*1024**2


class PilotUncertain(RuntimeError):pass


class PilotRejected(RuntimeError):
    def __init__(self,status):
        self.status=status;super().__init__('pilot admission definitively rejected: '+str(status))


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise PilotUncertain('redirect refused')


class PilotClient:
    def __init__(self,base_url='http://192.168.2.19:8088'):
        u=urlsplit(base_url)
        if (u.scheme!='http' or u.hostname not in {'192.168.2.19','127.0.0.1','localhost','::1'}
            or u.username is not None or u.password is not None or u.path not in {'','/'} or u.query or u.fragment):
            raise ValueError('fixed LAN broker or loopback test URL required')
        self.base=base_url.rstrip('/');self.opener=build_opener(ProxyHandler({}),NoRedirect())
    def _request(self,path,body=None):
        req=Request(self.base+path,data=None if body is None else json.dumps(body,ensure_ascii=False).encode(),
                    headers={'Content-Type':'application/json'})
        try:
            with self.opener.open(req,timeout=30) as r:raw=r.read(MAX_RESPONSE_BYTES+1)
            if len(raw)>MAX_RESPONSE_BYTES:raise PilotUncertain('pilot response byte bound')
            return json.loads(raw)
        except HTTPError as error:
            if body is not None and error.code in {400,403,409,413,429}:raise PilotRejected(error.code) from None
            raise PilotUncertain('pilot HTTP outcome unknown; reconcile exact key') from None
        except (URLError,OSError,ValueError):
            raise PilotUncertain('pilot transport/response unknown; reconcile exact key') from None
    def _identity(self,raw,key,item_id,profile,payload):
        expected=dict(source='pilot-mainpc',kind='chat',profile=profile,external_id=key,source_item_id=item_id,
                      input_hash=content_evidence(normalize_request(payload,profile))[0])
        if not isinstance(raw,dict) or any(raw.get(k)!=v for k,v in expected.items()):
            raise PilotUncertain('pilot retained identity/payload conflict')
        job_id=raw.get('id')
        try:
            if not isinstance(job_id,str) or str(uuid.UUID(job_id))!=job_id:raise ValueError()
        except (ValueError,TypeError,AttributeError):
            raise PilotUncertain('pilot invalid job id') from None
        result={k:raw.get(k) for k in ('id','source','kind','profile','external_id','source_item_id','input_hash',
                                     'input_bytes','state','created','queued_at','started','finished')}
        # Native chat output only; never copy/hit raw thinking/reasoning/error.
        native=raw.get('result')
        if isinstance(native,dict):
            message=native.get('message')
            content=message.get('content') if isinstance(message,dict) else None
            result['result']=dict(model=PROFILES[profile].model if native.get('model')==PROFILES[profile].model else None,
                done=native.get('done') is True,
                done_reason=native.get('done_reason') if native.get('done_reason') in {'stop','length'} else 'unknown',
                content=content if isinstance(content,str) and len(content.encode())<=65536 else None)
        return result
    def submit(self,key,item_id,profile,payload):
        if profile not in EVALUATION_PROFILE_NAMES:raise ValueError('dedicated pilot profile required')
        for value in (key,item_id):
            if (not isinstance(value,str) or not 1<=len(value)<=256 or value!=value.strip()
                or any(c in value for c in '\x00\n\r')):
                raise ValueError('bounded exact durable identities required')
        normalized=normalize_request(payload,profile)
        raw=self._request('/v1/jobs',dict(source='pilot-mainpc',kind='chat',profile=profile,
                                        external_id=key,source_item_id=item_id,payload=normalized))
        return self._identity(raw,key,item_id,profile,normalized)
    def status(self,job_id,key,item_id,profile,payload):
        if not isinstance(job_id,str) or str(uuid.UUID(job_id))!=job_id:raise ValueError('canonical UUID required')
        return self._identity(self._request('/v1/jobs/'+job_id),key,item_id,profile,payload)
    def recover(self,key,item_id,profile,payload):
        raw=self._request('/v1/correlations?'+urlencode(dict(source='pilot-mainpc',external_id=key,limit=2)))
        rows=raw.get('jobs') if isinstance(raw,dict) else None
        if not isinstance(rows,list) or any(not isinstance(r,dict) or not isinstance(r.get('job_id'),str) for r in rows):
            raise PilotUncertain('invalid pilot correlation response')
        if len(rows)>1:raise PilotUncertain('ambiguous pilot history; never choose/repost')
        return self.status(rows[0]['job_id'],key,item_id,profile,payload) if rows else None
