#!/usr/bin/env python3
"""Main-only metadata capture: GET tags/version + 4 POST show; NEVER load/infer.

Not executed by source preparation. No templates, raw responses or credentials
are retained. Unknown native thinking metadata blocks freeze rather than guessing.
"""
import argparse
import json
import os
import re
import sys
from urllib.request import Request, build_opener, ProxyHandler
from broker.nightly_client import NoRedirect, new_json, Closed
from broker.nightly_eval import MODEL_DIGESTS, thinking_metadata

BASE = 'http://192.168.2.5:11434'


def capture(request):
    tags = request('/api/tags')
    rows = tags.get('models') if isinstance(tags, dict) else None
    if not isinstance(rows, list):
        raise Closed('native tag metadata unavailable')
    seen = {}
    for model, digest in MODEL_DIGESTS.items():
        matches = [m for m in rows if isinstance(m, dict) and m.get('name') == model]
        if len(matches) != 1 or matches[0].get('digest') != digest:
            raise Closed('installed model digest changed')
        seen[model] = digest
    version = request('/api/version')
    version = version.get('version') if isinstance(version, dict) else None
    if not isinstance(version, str) or re.fullmatch(r'[0-9A-Za-z.+-]{1,64}', version) is None:
        raise Closed('invalid native version metadata')
    result = []
    for model, digest in seen.items():
        raw = request('/api/show', {'model': model})
        result.append(dict(model=model, digest=digest, thinking=thinking_metadata(raw)))
    return dict(schema='telegram-nightly-native-v1', ollama_version=version, models=result)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True)
    args = parser.parse_args(argv)
    opener = build_opener(ProxyHandler({}), NoRedirect())
    def request(path, body=None):
        req = Request(BASE + path, data=None if body is None else json.dumps(body).encode(),
                      headers={'Content-Type': 'application/json'})
        with opener.open(req, timeout=10) as stream:
            raw = stream.read(4*1024**2 + 1)
        if len(raw) > 4*1024**2:
            raise Closed('metadata response byte bound')
        return json.loads(raw)
    os.umask(0o077)
    try:
        result = capture(request)
        new_json(args.out, result)
        print('captured native controls for four exact installed models; no inference')
        return 0
    except Exception:
        print('native metadata capture blocked; no raw response/error retained', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
