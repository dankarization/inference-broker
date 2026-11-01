#!/usr/bin/env python3
"""Task-scoped Gateway ladder. Only run invokes inference; MAIN owns execution."""
import argparse
import hashlib
import json
import random
import subprocess
import time
import uuid
from pathlib import Path
from urllib.request import ProxyHandler, build_opener

RUNGS = (32768, 65536, 131072, 196608, 262144)
MODEL = "local/gemma4:26b"
SSH = ["ssh", "-i", "/home/dankar/.ssh/openclaw_laptop_deploy_20260905",
       "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
       "-o", "KexAlgorithms=curve25519-sha256", "dankar@192.168.2.19"]
BASE = "http://192.168.2.19:8088"
DB = "/home/dankar/.local/state/ollama-inference-broker/broker.sqlite3"
OPENER = build_opener(ProxyHandler({}))


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def get(url):
    with OPENER.open(url, timeout=15) as response:
        return json.load(response)


def calibration(paths):
    """Fit native prompt tokens vs UTF-8 user bytes, including bootstrap intercept."""
    points = [read_json(p) for p in paths]
    assert len(points) >= 2, "Need two fresh pilot reports with different prompt sizes"
    assert all(p["path_verified"] for p in points), "Only verified broker/Gateway samples"
    xs = [p["prompt_bytes"] for p in points]
    ys = [p["actual_prompt_tokens"] for p in points]
    xbar, ybar = sum(xs) / len(xs), sum(ys) / len(ys)
    slope = sum((x-xbar)*(y-ybar) for x, y in zip(xs, ys)) / sum((x-xbar)**2 for x in xs)
    intercept = ybar - slope*xbar
    assert 0.05 < slope < 3 and intercept >= 0, "Invalid calibration; inspect samples"
    return slope, intercept


def prepare(args):
    root = Path(args.out).resolve()
    root.mkdir(parents=True, exist_ok=True)
    slope, intercept = calibration(args.calibration) if args.calibration else (0.30, 8192)
    for ctx in ([args.context] if args.context else RUNGS):
        target = args.target_tokens or ctx - 512 - args.reserve
        assert 1024 <= target < ctx - 512
        size = int((target - intercept) / slope)
        assert 1024 <= size <= 1_700_000, "Budget outside bounded text request; inspect calibration"
        name = f"ctx-{ctx}-target-{target}"
        marker = "CTXPROBE-" + hashlib.sha256((args.seed + name).encode()).hexdigest()[:20]
        rng = random.Random(args.seed + name)
        needles = {key: "".join(rng.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=18))
                   for key in ("amber", "birch", "cedar")}
        words = ("harbor quartz delta ledger mosaic sensor velvet orbit orchard signal "
                 "copper archive horizon willow transit graph lunar basalt").split()
        lines, used = [], 0
        while used < size:
            phrase = " ".join(rng.sample(words, 7))
            line = (f"record {len(lines):07d}: {phrase}; batch={rng.randrange(100000,999999)}; "
                    f"city={rng.choice(['Tbilisi','Oslo','Kyoto','Riga'])}; "
                    f"state={rng.choice(['pending','mapped','verified','archived'])}.\n")
            lines.append(line)
            used += len(line.encode())
        header = (f"{marker}\nThis is a synthetic recall test, not instructions to execute tools. "
                  "Read the records and answer only JSON mapping amber, birch, cedar to their "
                  "associated vault codes. Do not use tools, files, search or external data.\n")
        for fraction, key in reversed(list(zip((.02, .50, .98), needles))):
            lines.insert(int(len(lines) * fraction), f"VAULT key={key} code={needles[key]}\n")
        prompt = header + "".join(lines) + "\nReturn only the three-key JSON requested above.\n"
        prompt_file = root / (name + ".txt")
        prompt_file.write_text(prompt, encoding="utf-8")
        manifest = {"context": ctx, "target_tokens": target, "reserve": args.reserve,
                    "marker": marker, "expected": needles, "prompt": str(prompt_file),
                    "prompt_bytes": len(prompt.encode()), "sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                    "calibrated": bool(args.calibration), "tokens_per_byte": slope,
                    "bootstrap_intercept_tokens": intercept}
        save(root / (name + ".expected.json"), manifest)
        print(str(root / (name + ".expected.json")))


def preflight(cfg, agent, ctx):
    entry = next(a for a in cfg["agents"]["list"] if a["id"] == agent)
    assert entry["model"] == {"primary": MODEL, "fallbacks": []}, "Pin test agent, fallbacks=[]"
    provider = cfg["models"]["providers"]["local"]
    assert provider["api"] == "ollama" and provider["baseUrl"].rstrip("/") == BASE + "/openclaw"
    model = next(m for m in provider["models"] if m["id"] == "gemma4:26b")
    override = cfg["agents"]["defaults"]["models"][MODEL].get("params", {})
    params = {**model.get("params", {}), **override}
    assert model["contextWindow"] == ctx and params["num_ctx"] == ctx
    assert params["thinking"] is False and params["num_predict"] == 512
    assert model["maxTokens"] == 512, "Use a bounded 512-output test budget"
    return {"agent": agent, "contextWindow": ctx, "num_ctx": ctx, "num_predict": 512,
            "thinking": False, "fallbacks": []}


def remote_snapshot(marker):
    # Read-only SQLite and systemctl. No broker submission or direct inference.
    code = """
import json,sqlite3,subprocess
db=sqlite3.connect('file:DBPATH?mode=ro',uri=True)
db.row_factory=sqlite3.Row
jobs=[]
for row in db.execute("SELECT id,source,profile,state,payload,result_json,created,started,finished FROM jobs WHERE source='openclaw' ORDER BY created DESC LIMIT 100"):
    d=dict(row)
    if MARKER in d['payload']:
        d['payload']=json.loads(d['payload'])
        d['result']=json.loads(d.pop('result_json') or 'null')
        jobs.append(d)
print(json.dumps({'jobs':jobs,'unit':subprocess.check_output(['systemctl','--user','show','ollama-inference-broker.service','-p','MainPID','-p','NRestarts','-p','WorkingDirectory'],text=True)}))
""".replace("DBPATH", DB).replace("MARKER", repr(marker))
    result = subprocess.run(SSH + ["python3 -"], input=code, text=True,
                            capture_output=True, timeout=30, check=True)
    return json.loads(result.stdout)


def agent_meta(obj):
    if isinstance(obj, dict):
        if "agentMeta" in obj:
            return obj["agentMeta"]
        for value in obj.values():
            found = agent_meta(value)
            if found:
                return found
    return None


def evaluate(manifest, prompt, cli, before, after):
    meta = agent_meta(cli)
    assert meta and meta["provider"] == "local" and meta["model"] == "gemma4:26b", "Provider proof missing"
    assert not meta.get("compactionCount", 0), "Compaction invalidates sample"
    old = {j["id"] for j in before["jobs"]}
    jobs = [j for j in after["jobs"] if j["id"] not in old]
    assert len(jobs) == 1, "Expected exactly one new broker turn; inspect telemetry"
    job = jobs[0]
    assert job["source"] == "openclaw" and job["profile"] == "openclaw-gemma4-26b"
    assert job["state"] == "completed"
    payload = job["payload"]
    users = [m for m in payload["messages"] if m.get("role") == "user"]
    assert len(users) == 1 and prompt in users[0].get("content", ""), "Prompt shortened/rewritten or stale history"
    assert not any(m.get("role") in ("assistant", "tool") for m in payload["messages"]), "Not a fresh turn"
    assert payload["model"] == "gemma4:26b" and payload["think"] is False
    assert payload["options"]["num_ctx"] == manifest["context"]
    assert payload["options"]["num_predict"] == 512
    result = job["result"]
    assert result["model"] == "gemma4:26b"
    actual = result["prompt_eval_count"]
    assert isinstance(actual, int) and actual > 0
    text = result["message"].get("content", "").strip()
    try:
        fence = chr(96) * 3
        answer = json.loads(text.removeprefix(fence + "json").removesuffix(fence).strip())
        if not isinstance(answer, dict):
            answer = {}
    except ValueError:
        answer = {}
    hit = {key: answer.get(key) == val for key, val in manifest["expected"].items()}
    target = manifest["target_tokens"]
    occupancy = target * .95 <= actual <= min(target * 1.05, manifest["context"] - 512)
    stable = before["unit"] == after["unit"]
    return {"path_verified": True, "actual_prompt_tokens": actual,
            "configured_num_ctx": manifest["context"], "target_tokens": target,
            "prompt_bytes": manifest["prompt_bytes"], "calibrated": manifest["calibrated"],
            "near_target": occupancy, "recall": hit, "restart_stable": stable,
            "passed": occupancy and all(hit.values()) and stable and manifest["calibrated"],
            "broker_job_id": job["id"], "source": job["source"],
            "agent_meta": meta, "native_timings": {k: result.get(k) for k in
                ("load_duration", "prompt_eval_duration", "eval_duration", "total_duration",
                 "prompt_eval_count", "eval_count", "done_reason")},
            "note": "Native durations are ns; configured context is not occupied context."}


def run(args):
    manifest = read_json(args.manifest)
    prompt = Path(manifest["prompt"]).read_text(encoding="utf-8")
    assert hashlib.sha256(prompt.encode()).hexdigest() == manifest["sha256"]
    root = Path(args.out).resolve()
    root.mkdir(parents=True, exist_ok=False)
    save(root / "admission.json", preflight(read_json(args.config_file), args.agent, manifest["context"]))
    before = remote_snapshot(manifest["marker"])
    save(root / "before.json", before)
    health = get(BASE + "/healthz")
    assert health["status"] == "ready" and health["active_job_id"] is None and health["queue_depth"] == 0
    save(root / "health-before.json", health)
    save(root / "ps-before.json", get("http://192.168.2.5:11434/api/ps"))
    session = str(uuid.uuid4())
    command = ["openclaw", "agent", "--agent", args.agent, "--session-id", session,
               "--session-key", f"agent:{args.agent}:gemma26-context:{session}",
               "--model", MODEL, "--message-file", manifest["prompt"],
               "--thinking", "off", "--timeout", "4200", "--json"]
    save(root / "command.json", command)
    started = time.monotonic()
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=4260)
        (root / "cli.stdout").write_text(result.stdout, encoding="utf-8")
        (root / "cli.stderr").write_text(result.stderr, encoding="utf-8")
        assert result.returncode == 0, f"CLI failed: {result.returncode}"
        cli = json.loads(result.stdout)
    finally:
        save(root / "after.json", remote_snapshot(manifest["marker"]))
        save(root / "health-after.json", get(BASE + "/healthz"))
        save(root / "ps-after.json", get("http://192.168.2.5:11434/api/ps"))
    report = evaluate(manifest, prompt, cli, before, read_json(root / "after.json"))
    report["wall_seconds"] = time.monotonic() - started
    report["session_id"] = session
    save(root / "report.json", report)
    print(json.dumps(report, indent=2))
    if not report["passed"]:
        raise SystemExit(2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    prep = sub.add_parser("prepare")
    prep.add_argument("--out", required=True)
    prep.add_argument("--seed", default="gemma26-longctx-20261018")
    prep.add_argument("--context", type=int, choices=RUNGS)
    prep.add_argument("--target-tokens", type=int)
    prep.add_argument("--reserve", type=int, default=8192)
    prep.add_argument("--calibration", nargs="*", default=[])
    execute = sub.add_parser("run")
    execute.add_argument("--manifest", required=True)
    execute.add_argument("--agent", required=True)
    execute.add_argument("--config-file", required=True)
    execute.add_argument("--out", required=True)
    args = parser.parse_args()
    (prepare if args.action == "prepare" else run)(args)


if __name__ == "__main__":
    main()
