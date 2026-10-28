"""Synthetic, finite capacity/identity regressions; never reach a real model."""
import contextlib
import copy
import hashlib
import io
import json
import tempfile
import threading
import unittest
from dataclasses import asdict
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from broker import syncopia_daily as daily
from broker.compat import validate_syncopia_memory_payload
from broker.http import serve
from broker.profiles import PROFILES
from broker.service import Broker, SourcePolicy
from broker.storage import content_evidence


class FakeWol:
    def __init__(self):
        self.calls = 0

    def wake(self):
        self.calls += 1


class FakeOllama:
    def __init__(self):
        self.requests = []
        self.started = threading.Event()
        self.release = threading.Event()
        self.block = False

    def ps(self):
        return {"models": [{"name": daily.MODEL, "size_vram": 1}]}

    def is_ready(self, _model):
        return True

    def run(self, kind, request):
        self.requests.append((kind, copy.deepcopy(request)))
        self.started.set()
        if self.block and not self.release.wait(15):
            raise RuntimeError("synthetic executor gate timed out")
        return {"model": daily.MODEL, "done": True, "done_reason": "stop",
                "message": {"role": "assistant", "content": "Synthetic summary."}}


class DailyCapacityTests(unittest.TestCase):
    @staticmethod
    def request():
        return {"messages": [{"role": "system", "content": "Summarize literal text."},
                             {"role": "user", "content": "Синтетический день. https://never-fetch.invalid/a"}],
                "think": "low", "options": dict(daily.OPTIONS)}

    def sized_request(self, size):
        request = self.request()
        messages = request["messages"]
        current = len(json.dumps(messages, ensure_ascii=False, separators=(",", ":")).encode())
        self.assertGreaterEqual(size, current)
        messages[1]["content"] += "x" * (size - current)
        return request

    def make(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.policy_path = self.root / "sources.json"
        self.policy_path.write_text(json.dumps({"sources": {daily.SOURCE: {
            "enabled": True, "admission_allowed": True, "weight": 4,
            "producer_storage_enabled": True, "producer_storage_mode": "hybrid",
            "ack_required": False, "compaction_enabled": False, "legacy_result_fallback": True,
        }}}))
        self.policy_bytes = self.policy_path.read_bytes()
        self.policy = SourcePolicy(self.policy_path)
        self.gpu, self.wol = FakeOllama(), FakeWol()
        self.broker = Broker(self.root / "broker.sqlite3", self.gpu, self.wol, source_policy=self.policy)
        self.addCleanup(self.broker.db.close)

    def body(self, request=None, key="daily-synthetic"):
        payload = self.request() if request is None else request
        digest, size = content_evidence(daily.normalize_request(payload))
        return dict(profile=daily.PROFILE, kind="chat", source=daily.SOURCE,
                    source_item_id="synthetic-full-day", external_id=key, payload=payload,
                    producer_storage={"producer_attempt_id": "attempt-128k-v1",
                        "input": {"mode": "hybrid", "storage_ref": "producer://daily/synthetic/input",
                                  "content_hash": digest, "byte_size": size},
                        "result": {"mode": "hybrid", "schema_version": "freeform-v1"}})

    def test_additive_profile_preserves_every_prior_profile(self):
        old = {k: asdict(v) for k, v in PROFILES.items() if k != daily.PROFILE}
        self.assertEqual(len(old), 35)
        self.assertEqual(hashlib.sha256(json.dumps(old, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "71073b5b7f598194832a1d9b57316b78ee64f6607ce79f98105e56ad8ce10147")
        profile = PROFILES[daily.PROFILE]
        self.assertEqual((profile.model, profile.max_context, profile.max_output, profile.max_concurrency),
                         (daily.MODEL, 131072, 8192, 1))
        self.assertEqual(daily.describe()["legacy_profile_unchanged"], "syncopia-memory-qwen38")
        contract_path = Path(__file__).resolve().parents[1] / "docs/contracts/syncopia-memory-daily-text-128k-v1.json"
        self.assertEqual(json.loads(contract_path.read_text()), daily.describe())
        request = dict(self.request(), model=daily.MODEL, tools=[], stream=False)
        old_payload = validate_syncopia_memory_payload(request)
        self.assertEqual(old_payload["options"]["num_ctx"], 65536)
        self.assertEqual(old_payload["think"], "low")

    def test_full_day_sized_literal_text_has_scalar_not_tokenizer_proof(self):
        request = self.sized_request(84045)
        request.update(model=daily.MODEL, tools=[], stream=False)
        before = copy.deepcopy(request)
        payload = daily.normalize_request(request)
        proof = daily.input_proof(request)
        self.assertEqual(request, before)
        self.assertEqual(payload["messages"], request["messages"])
        self.assertEqual(daily.normalize_request(payload), payload)
        self.assertEqual(proof["serialized_message_utf8_bytes"], 84045)
        self.assertEqual(proof["conservative_input_output_wrapper_bound"], 96333)
        self.assertEqual(proof["margin_under_context"], 34739)
        self.assertEqual((proof["input_hash"], proof["input_bytes"]), content_evidence(payload))
        self.assertIsNone(proof["tokenizer_count"])
        self.assertIsNone(proof["native_effective_settings"])
        self.assertNotIn(request["messages"][1]["content"], json.dumps(proof))

    def test_byte_boundary_rejects_oversize_without_slicing(self):
        exact = self.sized_request(118784)
        self.assertEqual(daily.input_proof(exact)["margin_under_context"], 0)
        over = copy.deepcopy(exact)
        over["messages"][1]["content"] += "x"
        before = copy.deepcopy(over)
        with self.assertRaisesRegex(ValueError, "no truncation or splitting"):
            daily.normalize_request(over)
        self.assertEqual(over, before)

    def test_drift_media_formats_off_and_cli_input_leak_fail_closed(self):
        for change in (
            {"think": False}, {"think": True}, {"think": "high"}, {"think": None},
            {"options": {**daily.OPTIONS, "num_ctx": 65536}},
            {"options": {**daily.OPTIONS, "num_predict": 512}},
            {"options": {**daily.OPTIONS, "temperature": False}},
            {"options": {**daily.OPTIONS, "num_ctx": 131072.0}},
            {"options": {**daily.OPTIONS, "seed": 0}}, {"options": None},
            {"model": "openclaw"}, {"model": "gemma4:26b"},
            {"tools": [{"type": "function"}]}, {"stream": True},
            {"format": None}, {"response_format": None}, {"images": []}, {"keep_alive": "5m"},
            {"messages": [{"role": "user", "content": "missing system"}]},
            {"messages": [{"role": "system", "content": "s"},
                          {"role": "user", "content": "u", "images": []}]},
        ):
            with self.subTest(change=list(change)), self.assertRaises(ValueError):
                daily.normalize_request({**self.request(), **change})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            request = {**self.request(), "format": "PRIVATE-SYNTHETIC-ERROR-SENTINEL"}
            path.write_text(json.dumps(request))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = daily.main(["prove", "--payload-json", str(path)])
            self.assertEqual(code, 2)
            self.assertNotIn("PRIVATE-SYNTHETIC", out.getvalue())
            self.assertNotIn(request["messages"][1]["content"], out.getvalue())
            path.write_text(json.dumps(self.request()))
            with contextlib.redirect_stdout(io.StringIO()) as ok:
                self.assertEqual(daily.main(["prove", "--payload-json", str(path)]), 0)
            self.assertEqual(json.loads(ok.getvalue())["input_hash"], daily.input_proof(self.request())["input_hash"])

    def test_producer_http_auth_hash_idempotency_and_ack_are_unchanged(self):
        self.make()
        token = "synthetic-test-storage-token-at-least-32"
        server = serve(self.broker, port=0, policy=self.policy, storage_token=token)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        base = f"http://127.0.0.1:{server.server_port}"
        def call(path, body=None, auth=True):
            headers = {"Content-Type": "application/json"}
            if auth:
                headers["Authorization"] = "Bearer " + token
            request = Request(base + path, data=None if body is None else json.dumps(body).encode(), headers=headers)
            with urlopen(request, timeout=10) as response:
                return response.status, json.load(response)
        body = self.body(self.sized_request(84045))
        with self.assertRaises(HTTPError) as denied:
            call("/v1/jobs", body, False)
        self.assertEqual(denied.exception.code, 401)
        wrong = copy.deepcopy(body)
        wrong["producer_storage"]["input"]["content_hash"] = "sha256:" + "0" * 64
        with self.assertRaises(HTTPError) as mismatch:
            call("/v1/jobs", wrong)
        self.assertEqual(mismatch.exception.code, 400)
        self.assertEqual(self.broker.health()["queue_depth"], 0)
        code, job = call("/v1/jobs", body)
        self.assertEqual(code, 202)
        self.assertEqual(call("/v1/jobs", body)[1]["id"], job["id"])
        self.assertEqual(self.broker.compact_status(job["id"])["input_hash"], daily.input_proof(body["payload"])["input_hash"])
        # A v1 request must never adopt/rewrite an old-profile correlation.
        old = copy.deepcopy(body)
        old["profile"] = "syncopia-memory-qwen38"
        with self.assertRaises(HTTPError):
            call("/v1/jobs", old)
        self.assertTrue(self.broker.dispatch_once(frozenset({daily.SOURCE})))
        request = self.gpu.requests[0][1]
        self.assertEqual((request["model"], request["options"], request["think"], request["keep_alive"]),
                         (daily.MODEL, daily.OPTIONS, "low", "1800s"))
        self.assertEqual(request["messages"], body["payload"]["messages"])
        path = "/v1/jobs/" + job["id"]
        with self.assertRaises(HTTPError) as denied:
            call(path, auth=False)
        self.assertEqual(denied.exception.code, 401)
        status = call(path + "/status")[1]
        ack = dict(job_id=job["id"], producer=daily.SOURCE, producer_attempt_id="attempt-128k-v1",
                   storage_ref="producer://daily/synthetic/result", result_hash=status["result_hash"],
                   result_bytes=status["result_bytes"], schema_version="freeform-v1",
                   persisted_at="2026-10-28T23:59:00Z")
        receipt = call(path + "/ack", ack)[1]
        self.assertEqual(call(path + "/ack", ack)[1], receipt)
        self.assertEqual(call("/v1/jobs", body)[1]["id"], job["id"])
        self.assertFalse(self.broker.dispatch_once(frozenset({daily.SOURCE})))
        self.assertEqual(len(self.gpu.requests), 1)
        self.assertEqual(self.policy_path.read_bytes(), self.policy_bytes)

    def test_source_kind_capability_guards_and_persisted_drift_before_wake(self):
        self.make()
        body = self.body()
        for change in ({"source": "openclaw"}, {"source": "pilot-mainpc"}, {"source": None},
                       {"kind": "generate"}, {"external_id": None}, {"source_item_id": None},
                       {"producer_storage": None}):
            with self.subTest(change=list(change)), self.assertRaises(ValueError):
                self.broker.submit(**{**body, **change})
        self.assertEqual(self.broker.health()["queue_depth"], 0)
        job = self.broker.submit(**body)
        payload = daily.normalize_request(body["payload"])
        payload["options"]["num_ctx"] = 65536
        with self.broker.db:
            self.broker.db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(payload), job["id"]))
        self.assertTrue(self.broker.dispatch_once(frozenset({daily.SOURCE})))
        self.assertEqual(self.broker.status(job["id"])["state"], "failed")
        self.assertEqual((self.wol.calls, self.gpu.requests), (0, []))

    def test_legacy_queue_survives_restart_and_shares_one_running_slot(self):
        self.make()
        legacy = self.broker.submit("syncopia-memory-qwen38", "chat", self.request(),
                                   source=daily.SOURCE, external_id="old-64k", source_item_id="old-synthetic")
        before = dict(self.broker.db.execute("SELECT id,payload,input_hash,input_bytes,created,queued_at FROM jobs WHERE id=?",
                                           (legacy["id"],)).fetchone())
        new = self.broker.submit(**self.body())
        self.assertEqual(dict(self.broker.db.execute("SELECT id,payload,input_hash,input_bytes,created,queued_at FROM jobs WHERE id=?",
                                                   (legacy["id"],)).fetchone()), before)
        self.broker.db.close()
        self.broker = Broker(self.root / "broker.sqlite3", self.gpu, self.wol, source_policy=self.policy)
        self.addCleanup(self.broker.db.close)
        self.assertEqual(dict(self.broker.db.execute("SELECT id,payload,input_hash,input_bytes,created,queued_at FROM jobs WHERE id=?",
                                                   (legacy["id"],)).fetchone()), before)
        self.gpu.block = True
        dispatcher = threading.Thread(target=self.broker.dispatch_once, args=(frozenset({daily.SOURCE}),))
        dispatcher.start()
        try:
            self.assertTrue(self.gpu.started.wait(10))
            self.assertFalse(self.broker.dispatch_once(frozenset({daily.SOURCE})))
            self.assertEqual(self.broker._candidates(frozenset({daily.SOURCE})), [])
            self.assertEqual(self.broker.status(new["id"])["state"], "queued")
        finally:
            self.gpu.release.set()
            dispatcher.join(15)
        self.assertFalse(dispatcher.is_alive())
        self.assertEqual(self.broker.status(legacy["id"])["state"], "completed")
        self.assertTrue(self.broker.dispatch_once(frozenset({daily.SOURCE})))
        self.assertEqual([item[1]["options"]["num_ctx"] for item in self.gpu.requests], [65536, 131072])
        self.assertEqual(self.broker.status(new["id"])["state"], "completed")
        self.assertEqual(self.policy_path.read_bytes(), self.policy_bytes)


if __name__ == "__main__":
    unittest.main()
