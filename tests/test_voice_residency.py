"""Deterministic request-policy/custody proofs. Never call a live executor."""
import base64
import io
import json
import os
import tempfile
import unittest
from dataclasses import replace
from unittest.mock import patch

from broker import residency, voice, voice_gpu
from broker.service import Broker, SourcePolicy
from broker.profiles import PROFILES
from test_batch_scheduler import FakeWol, MutableClock
from test_broker import FakeOllama
from test_voice_gpu import PROFILE, ready

AUDIO = {"filename": "audio.ogg", "audio_base64": base64.b64encode(b"synthetic audio").decode()}


class ResidencyTests(unittest.TestCase):
    def make(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = tmp.name
        self.calls = []
        self.clock = MutableClock()
        self.ollama = FakeOllama(self.calls)
        self.broker = Broker(os.path.join(self.root, "db"), self.ollama,
                             FakeWol(self.calls), clock=self.clock)
        self.addCleanup(self.broker.db.close)
        return self.broker

    def test_only_pending_gpu_voice_shortens_request_not_preload_or_profile(self):
        broker = self.make()
        llm = broker.submit("interactive", "generate", {"prompt": "synthetic"})
        self.clock.advance(1)
        broker.submit(PROFILE, "transcribe", AUDIO, source="local-voice")
        self.assertTrue(broker.dispatch_once())
        requests = [c[2] for c in self.calls if isinstance(c, tuple) and c[0] == "run"]
        self.assertEqual([r["keep_alive"] for r in requests], ["180s", "30s"])
        self.assertEqual(PROFILES["interactive"].keep_alive_seconds, 180)
        result = broker.status(llm["id"])["result"]
        self.assertEqual(result["_broker_residency"]["effective_keep_alive_seconds"], 30)
        self.assertFalse(any(isinstance(c, tuple) and c[0] == "unload" for c in self.calls))

    def test_no_demand_preserves_hot_reuse_and_cannot_forge_receipt(self):
        broker = self.make()
        real_run = self.ollama.run
        def forged(kind, request):
            return dict(real_run(kind, request), _broker_residency={"reason": "forged"})
        self.ollama.run = forged
        jobs = [broker.submit("interactive", "generate", {"prompt": str(i)}) for i in range(2)]
        for job in jobs:
            broker.dispatch_once()
        for job in jobs:
            self.assertNotIn("_broker_residency", broker.status(job["id"])["result"])
        requests = [c[2] for c in self.calls if isinstance(c, tuple) and c[0] == "run"]
        self.assertTrue(all(r["keep_alive"] == "180s" for r in requests))
        self.assertEqual(sum(r.get("prompt") == "" for r in requests), 1)

    def test_pending_voice_can_cost_a_reload_after_a_thirty_one_second_llm_gap(self):
        def loads(adaptive):
            broker=self.make()
            expires=[0.]
            original_ps=self.ollama.ps
            original_run=self.ollama.run
            def ps():
                if self.clock() >= expires[0]:
                    self.ollama.loaded.clear()
                return original_ps()
            def run(kind,request):
                result=original_run(kind,request)
                expires[0]=self.clock()+float(request['keep_alive'].removesuffix('s'))
                return result
            self.ollama.ps=ps
            self.ollama.run=run
            for i in range(2):
                broker.submit('interactive','generate',{'prompt':str(i)})
                self.clock.advance(1)
            broker.submit(PROFILE,'transcribe',AUDIO,source='local-voice')
            policy=residency.inference_policy if adaptive else lambda b,p:(p.keep_alive_seconds,None)
            with patch.object(residency,'inference_policy',side_effect=policy):
                broker.dispatch_once()
                self.clock.advance(31)
                broker.dispatch_once()
            return sum(isinstance(c,tuple) and c[0]=='run' and c[2].get('prompt')=='' for c in self.calls)
        self.assertEqual(loads(False),1)
        self.assertEqual(loads(True),2)

    def test_cpu_wyoming_cancelled_and_disabled_voice_do_not_change_ttl(self):
        for profile, state, enabled in (("voice-wyoming-v1", "queued", True),
                ("voice-native-cpu-v1", "queued", True), (PROFILE, "cancelled", True),
                (PROFILE, "queued", False)):
            with self.subTest(profile=profile, state=state, enabled=enabled):
                broker = self.make()
                # Policy helper uses metadata only; insert no audio at all.
                job = broker.submit(PROFILE, "transcribe", AUDIO, source="local-voice")
                broker.db.execute("UPDATE jobs SET profile=?,state=? WHERE id=?", (profile, state, job["id"]))
                path = os.path.join(self.root, "policy.json")
                with open(path, "w") as f:
                    json.dump({"sources": {"local-voice": {"enabled": enabled}}}, f)
                broker.use_source_policy(SourcePolicy(path))
                self.assertEqual(residency.inference_policy(broker, PROFILES["interactive"]), (180, None))

    def test_all_gpu_routes_are_demand_including_contour_and_native_asr(self):
        broker = self.make()
        job = broker.submit(PROFILE, "transcribe", AUDIO, source="local-voice")
        for profile in (*voice._HTTP_WORKERS, voice.voice_contour.PROFILE, "stt-whisper"):
            with self.subTest(profile=profile):
                broker.db.execute("UPDATE jobs SET profile=? WHERE id=?", (profile, job["id"]))
                self.assertEqual(residency.inference_policy(broker, PROFILES["interactive"])[0], 30)

    def test_late_voice_arrival_does_not_rewrite_inflight_or_unload(self):
        broker = self.make()
        llm = broker.submit("interactive", "generate", {"prompt": "synthetic"})
        run = self.ollama.run
        def late(kind, request):
            if request.get("prompt") == "synthetic":
                broker.submit(PROFILE, "transcribe", AUDIO, source="local-voice")
            return run(kind, request)
        self.ollama.run = late
        broker.dispatch_once()
        self.assertNotIn("_broker_residency", broker.status(llm["id"])["result"])
        self.assertEqual(self.calls[-1][2]["keep_alive"], "180s")

    def test_timing_emits_on_failure_without_payload_or_raw_error(self):
        broker = self.make()
        row = {"id": "synthetic", "profile": PROFILE, "queued_at": 10., "started": 14.}
        with self.assertLogs(voice_gpu.LOGGER, level="INFO") as logs:
            with self.assertRaisesRegex(RuntimeError, "private"):
                with voice_gpu.queued(broker, row):
                    guard = voice_gpu.CURRENT.get()
                    with guard.span("worker_http"):
                        raise RuntimeError("private audio transcript token")
        event = json.loads(logs.records[-1].getMessage())
        self.assertEqual(event["queue_wait_seconds"], 4)
        self.assertEqual(event["outcome"], "failed")
        self.assertIn("worker_http", event["stages_seconds"])
        self.assertNotIn("private", json.dumps(event))
        self.assertIsNone(voice_gpu.CURRENT.get())

    def test_telemetry_failure_never_changes_success_or_original_error(self):
        broker = self.make()
        row = {"id": "synthetic", "profile": PROFILE, "queued_at": 10., "started": 14.}
        for target in (patch.object(broker, "executor_fence", side_effect=RuntimeError("db")),
                       patch.object(voice_gpu.LOGGER, "info", side_effect=RuntimeError("log"))):
            with target:
                with voice_gpu.queued(broker, row):
                    pass
                with self.assertRaisesRegex(ValueError, "original"):
                    with voice_gpu.queued(broker, row):
                        raise ValueError("original")
            self.assertIsNone(voice_gpu.CURRENT.get())

    def simulate_chunks(self, adaptive):
        broker = self.make()
        clock = self.clock
        native = 20. / 12
        expires = 0.
        observed = ready(last_request_id=None)
        loads = 0
        requests = []
        def ps(**kwargs):
            return {"models": [{"name": "nemotron3:33b"}] if clock() < expires else []}
        def run(kind, request):
            nonlocal expires, loads
            requests.append(dict(request))
            if request.get("prompt") == "":
                loads += 1
            expires = clock() + float(request["keep_alive"].removesuffix("s"))
            return {"done": True}
        self.ollama.ps = ps
        self.ollama.is_ready = lambda model: bool(ps()["models"])
        self.ollama.run = run
        def transport(profile, request, timeout):
            request_id = request.get_header("X-request-id")
            self.assertEqual(broker.executor_fence()["request_id"], voice_gpu.PREFIX + PROFILE + "|" + request_id)
            self.assertEqual(ps(), {"models": []})
            observed.update(last_request_id=request_id, last_request_gpu_released=True)
            clock.advance(native)
            return io.BytesIO(json.dumps({"model": voice._HTTP_WORKERS[PROFILE][2], "text": "synthetic"}).encode())
        default_policy = lambda broker, profile: (profile.keep_alive_seconds, None)
        policy = residency.inference_policy if adaptive else default_policy
        start = clock()
        events = []
        with patch.dict(PROFILES, {"interactive": replace(PROFILES["interactive"], keep_alive_seconds=300)}), \
                patch.object(residency, "inference_policy", side_effect=policy), \
                patch.object(voice_gpu.time, "monotonic", side_effect=clock), \
                patch.object(voice_gpu.time, "perf_counter", side_effect=clock), \
                patch.object(voice_gpu.time, "sleep", side_effect=clock.advance), \
                patch.object(voice_gpu, "worker", side_effect=lambda *a, **k: dict(observed)), \
                patch.object(voice, "open_worker", side_effect=transport), \
                patch.dict(os.environ, {"BROKER_WHISPER_LARGE_URL": "http://synthetic.invalid",
                                        "BROKER_WHISPER_LARGE_TOKEN": "synthetic"}, clear=True), \
                self.assertLogs(voice_gpu.LOGGER, level="INFO") as logs:
            for i in range(12):
                # A 300s lane models the incident TTL, with no native inference.
                llm = broker.submit("interactive", "generate", {"prompt": "synthetic"})
                broker.db.execute("UPDATE jobs SET queued_at=queued_at-1 WHERE id=?", (llm["id"],))
                job = broker.submit(PROFILE, "transcribe", AUDIO, source="local-voice")
                broker.dispatch_once()
                broker.dispatch_once()
                self.assertEqual(broker.status(job["id"])["state"], "completed")
                self.assertIsNone(broker.executor_fence())
            events = [json.loads(r.getMessage()) for r in logs.records
                      if json.loads(r.getMessage()).get("event") == "voice.execution_timing"]
        return {"execution_seconds": round(clock()-start, 6),
                "residency_wait_seconds": sum(e["stages_seconds"]["ollama_residency_wait"] for e in events),
                "native_seconds": sum(e["stages_seconds"]["worker_http"] for e in events),
                "loads": loads, "worker_calls": sum(e["worker_calls"] for e in events)}

    def test_twelve_chunks_virtual_clock_measures_ttl_not_live_speedup(self):
        baseline = self.simulate_chunks(False)
        candidate = self.simulate_chunks(True)
        self.assertEqual(baseline["residency_wait_seconds"], 3600)
        self.assertEqual(candidate["residency_wait_seconds"], 360)
        self.assertAlmostEqual(baseline["native_seconds"], 20, places=4)
        self.assertAlmostEqual(candidate["native_seconds"], 20, places=4)
        self.assertEqual(baseline["execution_seconds"], 3620)
        self.assertEqual(candidate["execution_seconds"], 380)
        self.assertEqual(baseline["loads"], candidate["loads"])
        self.assertEqual(candidate["worker_calls"], 12)


if __name__ == "__main__":
    unittest.main()
