import base64
import copy
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, ProxyHandler, build_opener

from broker.http import serve
from broker.profiles import EVALUATION_PROFILE_NAMES, OPENCLAW_PROFILES_BY_MODEL, PROFILES
from broker.service import Broker, SourcePolicy


class FakeWol:
    def wake(self):
        pass


class FakeOllama:
    def __init__(self):
        self.requests = []
        self.result_override = None

    def ps(self):
        return {"models": []}

    def ensure_model_ready(self, model, **kwargs):
        return True

    def run(self, kind, request):
        self.requests.append((kind, copy.deepcopy(request)))
        if self.result_override is not None:
            return self.result_override
        return {"model": request["model"], "done": True,
                "message": {"role": "assistant", "content": '{"labels":[]}'}}


class EvaluationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        policy_path = root / "sources.json"
        policy_path.write_text(json.dumps({"version": 1, "sources": {
            "pilot-mainpc": {"enabled": True, "weight": 1, "admission_allowed": True}}}))
        self.policy = SourcePolicy(policy_path)
        self.ollama = FakeOllama()
        self.broker = Broker(root / "jobs.sqlite3", self.ollama, FakeWol(),
                             source_policy=self.policy)
        self.addCleanup(self.broker.db.close)

    def payload(self):
        return {"messages": [{"role": "user", "content": "Read visible labels only",
                              "images": [base64.b64encode(b'image fixture').decode()]}],
                "options": {"num_ctx": 8192, "num_predict": 2048, "temperature": 0},
                "think": False, "stream": False,
                "format": {"type": "object", "properties": {"labels": {"type": "array"}}}}

    def submit(self, profile, payload=None, **kwargs):
        return self.broker.submit(profile, "chat", self.payload() if payload is None else payload,
                                  source="pilot-mainpc", **kwargs)

    def count(self):
        return self.broker.db.execute("SELECT count(*) FROM jobs").fetchone()[0]

    def test_both_profiles_real_admission_dispatch_and_terminal_cleanup(self):
        for profile in sorted(EVALUATION_PROFILE_NAMES):
            with self.subTest(profile=profile):
                original = self.payload()
                original["model"] = PROFILES[profile].model
                job = self.submit(profile, original)
                self.assertEqual(job["source"], "pilot-mainpc")
                self.assertTrue(self.broker.dispatch_once(frozenset({"pilot-mainpc"}), self.policy))
                kind, request = self.ollama.requests[-1]
                self.assertEqual(kind, "chat")
                self.assertEqual(request["model"], PROFILES[profile].model)
                for field in ("messages", "options", "think", "format"):
                    self.assertEqual(request[field], original[field])
                self.assertFalse(request["stream"])
                self.assertFalse(request["truncate"])
                self.assertFalse(request["shift"])
                self.assertEqual(request["_broker_timeout_seconds"], 900)
                status = self.broker.status(job["id"])
                self.assertEqual(status["state"], "completed")
                self.assertEqual(status["payload"], {})
                self.assertEqual(status["result"]["model"], PROFILES[profile].model)

    def test_missing_options_are_normalized_to_same_server_owned_values(self):
        for profile in EVALUATION_PROFILE_NAMES:
            body = self.payload()
            for field in ("options", "think", "stream"):
                body.pop(field)
            job = self.submit(profile, body)
            self.assertEqual(job["payload"]["options"], self.payload()["options"])
            self.assertIs(job["payload"]["think"], False)

    def test_source_kind_storage_and_old_openclaw_guards(self):
        for profile in EVALUATION_PROFILE_NAMES:
            for source, kind in ((None, "chat"), ("openclaw", "chat"),
                                 ("other", "chat"), ("pilot-mainpc", "generate")):
                with self.subTest(profile=profile, source=source, kind=kind):
                    with self.assertRaises(ValueError):
                        self.broker.submit(profile, kind, self.payload(), source=source)
            with self.assertRaises(ValueError):
                self.submit(profile, producer_storage={})
        for model, profile in OPENCLAW_PROFILES_BY_MODEL.items():
            self.assertNotIn(profile, EVALUATION_PROFILE_NAMES)
            with self.assertRaisesRegex(ValueError, "source openclaw and kind chat"):
                self.submit(profile)
        self.assertEqual(OPENCLAW_PROFILES_BY_MODEL["qwen3-vl:30b"], "openclaw-qwen3-vl")
        self.assertEqual(OPENCLAW_PROFILES_BY_MODEL["qwen3-vl:2b-instruct-q4_K_M"],
                         "openclaw-qwen3-vl-2b")
        self.assertEqual(self.count(), 0)

    def test_negative_payloads_rejected_before_job_creation_for_both_profiles(self):
        changes = [
            {"model": "qwen3-vl:untrusted"}, {"tools": []}, {"keep_alive": -1},
            {"think": True}, {"think": 0}, {"stream": True}, {"stream": 0},
            {"truncate": True}, {"shift": True}, {"messages": []},
            {"messages": [{"role": "assistant", "content": "x"}]},
            {"messages": [{"role": "user", "content": "x", "tool_calls": []}]},
            {"messages": [{"role": "user", "content": "x", "images": ["!!!"]}]},
            {"messages": [{"role": "user", "content": "x", "images": [""]}]},
            {"messages": [{"role": "system", "content": "x", "images": ["eA=="]}]},
            {"messages": [{"role": "user", "content": "x", "images": ["eA=="] * 5}]},
            {"messages": [{"role": "user", "content": "x"}] * 17},
            {"format": []}, {"format": "xml"}, {"format": {"description": "x" * 65536}},
        ]
        for key, values in {"num_ctx": [8193, 8191, 0, -1, True, None, 8192.0],
                            "num_predict": [2049, 2047, 0, True, None],
                            "temperature": [1, True, None, float("nan")],
                            "num_gpu": [1]}.items():
            changes.extend({"options": {key: value}} for value in values)
        for profile in EVALUATION_PROFILE_NAMES:
            for change in changes:
                with self.subTest(profile=profile, change=str(change)[:100]):
                    with self.assertRaises(ValueError):
                        self.submit(profile, {**self.payload(), **change})
            with self.assertRaises(ValueError):
                self.submit(profile, [])
            with patch("broker.openclaw.MAX_IMAGE_BYTES", 1):
                with self.assertRaisesRegex(ValueError, "decoded images"):
                    self.submit(profile)
            with patch("broker.openclaw.MAX_REQUEST_BYTES", 1):
                with self.assertRaisesRegex(ValueError, "byte limit"):
                    self.submit(profile)
        self.assertEqual(self.count(), 0)

    def test_result_schema_and_model_fail_closed_for_both_profiles(self):
        for profile in EVALUATION_PROFILE_NAMES:
            good = {"model": PROFILES[profile].model, "done": True,
                    "message": {"role": "assistant", "content": "answer"}}
            bad_results = [[], {**good, "done": False}, {**good, "model": "wrong"},
                           {k: v for k, v in good.items() if k != "model"},
                           {**good, "message": {"role": "user", "content": "x"}},
                           {**good, "message": {"role": "assistant", "content": []}},
                           {**good, "message": {"role": "assistant", "content": "x", "tool_calls": []}}]
            for result in bad_results:
                self.ollama.result_override = result
                job = self.submit(profile)
                self.assertTrue(self.broker.dispatch_once(frozenset({"pilot-mainpc"})))
                self.assertEqual(self.broker.status(job["id"])["state"], "failed")

    def test_http_route_admits_only_when_pilot_policy_allows_without_inference(self):
        server = serve(self.broker, port=0, policy=self.policy)
        self.addCleanup(server.server_close)
        opener = build_opener(ProxyHandler({}))
        def post(profile):
            request = Request(f"http://127.0.0.1:{server.server_port}/v1/jobs",
                              data=json.dumps({"profile": profile, "kind": "chat",
                                               "source": "pilot-mainpc", "payload": self.payload()}).encode(),
                              headers={"Content-Type": "application/json"})
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            try:
                with opener.open(request, timeout=3) as response:
                    return response.status, json.loads(response.read())
            except HTTPError as exc:
                return exc.code, json.loads(exc.read())
            finally:
                thread.join(timeout=3)
                self.assertFalse(thread.is_alive())
        self.policy.set_admission_allowed("pilot-mainpc", False)
        for profile in EVALUATION_PROFILE_NAMES:
            status, body = post(profile)
            self.assertEqual(status, 403)
            self.assertEqual(body["error"]["code"], "source_admission_blocked")
        self.assertEqual(self.count(), 0)
        self.policy.set_admission_allowed("pilot-mainpc", True)
        for profile in EVALUATION_PROFILE_NAMES:
            status, body = post(profile)
            self.assertEqual(status, 202)
            self.assertEqual(body["source"], "pilot-mainpc")
        self.assertEqual(len(self.ollama.requests), 0)
