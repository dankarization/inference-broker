"""Optional real Chromium smoke/regression test; uses installed websockets, no npm.
PYTHONPATH=. python3 tests/browser_dashboard.py
"""
import asyncio
import base64
from contextlib import contextmanager
import os
import json
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path
from urllib.request import Request, urlopen

import websockets
from broker.service import Broker, SourcePolicy
from broker.nextcloud_priority import NextcloudPriority
from broker.http import serve

class Ollama:
    def ps(self): return {"models": []}
    def is_ready(self, model): return True
    def run(self, kind, request): return {"done": True, "response": "synthetic"}

class Gate(NextcloudPriority):
    @contextmanager
    def hold(self, db):
        self.value = {"jobs": {f"q{i:04}": 3 for i in range(25)},
                      "floor": 3, "scope_complete": True}
        try:
            yield
        finally:
            self.value = None

async def run():
    with tempfile.TemporaryDirectory(prefix="broker-browser-") as temp:
        root = Path(temp)
        policy_file = root / "policy.json"
        policy_file.write_text(json.dumps({"sources": {"nextcloud": {"weight": 7}}}))
        policy = SourcePolicy(policy_file)
        b = Broker(root/"test.db", Ollama(), object(), source_policy=policy,
                   nextcloud_priority=Gate(lambda: None))
        with b.db:
            b.db.executemany(
                "INSERT INTO jobs(id,profile,kind,source,payload,state,created,queued_at,finished) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                ((f"j{i:04}", "interactive", "generate", "history", "{}", "completed", i, i, i) for i in range(100)))
            b.db.executemany(
                "INSERT INTO jobs(id,profile,kind,source,payload,state,created,queued_at,external_id) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                ((f"q{i:04}", "interactive", "generate", "nextcloud",
                  '{"model":"synthetic","prompt":"fixture"}', "queued", i, i, f"q{i:04}")
                 for i in range(25)))
        assert b.dispatch_once(policy=policy)  # Real claim, fake executor only.
        server = serve(b, port=0, policy=policy)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        browser = subprocess.Popen([shutil.which("chromium"), "--headless", "--no-sandbox",
            "--disable-gpu", "--disable-dev-shm-usage", "--no-proxy-server", "--remote-debugging-port=0",
            "--user-data-dir="+str(root/"chrome"), "about:blank"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(100):
                if (root/"chrome/DevToolsActivePort").exists(): break
                await asyncio.sleep(.1)
            port = (root/"chrome/DevToolsActivePort").read_text().splitlines()[0]
            target = json.load(urlopen(Request(
                f"http://127.0.0.1:{port}/json/new?http://127.0.0.1:{server.server_port}/dashboard", method="PUT")))
            async with websockets.connect(target["webSocketDebuggerUrl"], max_size=8*1024*1024) as ws:
                sequence = 0
                async def call(method, params=None):
                    nonlocal sequence
                    sequence += 1
                    await ws.send(json.dumps(dict(id=sequence, method=method, params=params or {})))
                    while True:
                        response = json.loads(await ws.recv())
                        if response.get("id") == sequence:
                            if "error" in response: raise RuntimeError(response)
                            return response.get("result", {})
                async def evaluate(expression):
                    result = await call("Runtime.evaluate", dict(expression=expression, awaitPromise=True, returnByValue=True))
                    if result.get("exceptionDetails"): raise RuntimeError(result)
                    return result["result"].get("value")
                await call("Page.bringToFront")
                for _ in range(300):
                    if await evaluate("typeof renderedOnce !== 'undefined' && renderedOnce"): break
                    await asyncio.sleep(.1)
                assert await evaluate("document.querySelectorAll('#history-body tr').length") >= 10, await evaluate("({url:location.href,ready:document.readyState,hidden:document.hidden,body:document.body.innerText})")
                assert await evaluate("!document.querySelector('#forecast-live').hidden")
                assert await evaluate("document.querySelector('#forecast-basis').textContent.includes('not a live Nextcloud ranking')")
                assert await evaluate("document.querySelector('#forecast-body').rows.length") == 10
                assert await evaluate("!document.querySelector('#forecast-unranked').hidden")
                assert await evaluate("document.querySelector('#contingency').offsetParent !== null")
                proof = os.environ.get("BROKER_BROWSER_PROOF_DIR")
                for width in (390, 1440):
                    await call("Emulation.setDeviceMetricsOverride", {
                        "width": width, "height": 900, "deviceScaleFactor": 1, "mobile": False})
                    layout = await evaluate("({width: innerWidth, scroll: document.documentElement.scrollWidth, note: document.querySelector('#contingency').offsetParent !== null})")
                    assert layout["scroll"] <= layout["width"], layout
                    assert layout["note"], layout
                    if proof:
                        target_dir = Path(proof)
                        target_dir.mkdir(parents=True, exist_ok=True)
                        shot = await call("Page.captureScreenshot", {"format": "png", "captureBeyondViewport": True})
                        (target_dir / f"forecast-{width}.png").write_bytes(base64.b64decode(shot["data"]))
                await evaluate("window.scrollTo(0,document.body.scrollHeight)")
                for _ in range(300):
                    if await evaluate("document.querySelectorAll('#history-body tr').length") > 10: break
                    await asyncio.sleep(.1)
                rows = await evaluate("document.querySelectorAll('#history-body tr').length")
                assert rows > 10, rows
                await evaluate("window.scrollTo(0,0); loadDashboard()")
                assert await evaluate("document.querySelectorAll('#history-body tr').length") == rows
                result = await evaluate("""(async () => {
                    const original = window.fetch; let reads=0;
                    window.fetch = (...args) => { reads++; return original(...args); };
                    Object.defineProperty(document,'hidden',{configurable:true,value:true});
                    document.dispatchEvent(new Event('visibilitychange'));
                    await loadDashboard();
                    const hiddenReads=reads;
                    Object.defineProperty(document,'hidden',{configurable:true,value:false});
                    document.dispatchEvent(new Event('visibilitychange'));
                    await new Promise(r=>setTimeout(r,300));
                    const resumeReads=reads;
                    let release;
                    window.fetch = (...args) => { reads++; return new Promise(r=>{release=()=>original(...args).then(r);}); };
                    const first=loadDashboard(); await loadDashboard();
                    const overlapReads=reads-resumeReads;
                    release(); await first;
                    window.fetch=original;
                    return {hiddenReads,resumeReads,overlapReads};
                })()""")
                assert result == dict(hiddenReads=0,resumeReads=1,overlapReads=1), result
                print(json.dumps(dict(browser="Chromium", paged_rows=rows, **result)))
        finally:
            browser.terminate()
            try: browser.wait(timeout=10)
            except subprocess.TimeoutExpired: browser.kill(); browser.wait()
            server.shutdown(); server.server_close(); thread.join(); b.db.close()
if __name__ == "__main__": asyncio.run(run())
