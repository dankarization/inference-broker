"""Deterministic coverage for 60-minute model-affine time batching."""
import json
import os
import tempfile
import threading
import unittest
from urllib.request import urlopen

from broker.compat import submit as compatibility_submit
from broker.http import serve
from broker.service import Broker, SourcePolicy, SCHEDULING_HORIZON_SECONDS


class MutableClock:
    def __init__(self, value=1_000.0):
        self.value = float(value)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)


class FakeWol:
    def __init__(self, calls):
        self.calls = calls

    def wake(self):
        self.calls.append("wake")


class FakeOllama:
    def __init__(self, calls, clock, execution_seconds=300.0, loaded=None):
        self.calls = calls
        self.clock = clock
        self.execution_seconds = execution_seconds
        self.loaded = list(loaded or [])

    def ps(self):
        self.calls.append("ps")
        return {"models": [{"name": name, "size_vram": 1} for name in self.loaded]}

    def is_ready(self, model):
        self.calls.append(("ready", model))
        return model in self.loaded

    def unload(self, model):
        self.calls.append(("unload", model))
        self.loaded.remove(model)

    def run(self, kind, request):
        self.calls.append(("run", kind, request.copy()))
        if request.get("prompt") == "" and request["model"] not in self.loaded:
            self.loaded.append(request["model"])
        elif request.get("prompt") != "":
            seconds = self.execution_seconds
            if callable(seconds):
                seconds = seconds(kind, request)
            self.clock.advance(seconds)
        return {"done": True, "message": {"role": "assistant", "content": "answer"}}


class TimeBatchSchedulerTests(unittest.TestCase):
    def make(self, *, execution_seconds=300.0, loaded=None):
        self.calls = []
        self.clock = MutableClock()
        self.tmp = tempfile.NamedTemporaryFile()
        self.addCleanup(self.tmp.close)
        broker = Broker(
            self.tmp.name,
            FakeOllama(self.calls, self.clock, execution_seconds, loaded),
            FakeWol(self.calls),
            clock=self.clock,
        )

        self.addCleanup(broker.db.close)
        return broker

    def policy(self, sources):
        descriptor, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(descriptor, "w") as handle:
            json.dump({"version": 1, "sources": sources}, handle)
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        return SourcePolicy(path)

    @staticmethod
    def olya_payload(prompt):
        return {
            "prompt": prompt,
            "images": ["aGVsbG8="],
            "format": {"type": "object"},
        }

    def submit_olya(self, broker, prompt):
        job = broker.submit(
            "olya-vision-gemma", "generate", self.olya_payload(prompt),
            source="olya-vision",
        )
        self.clock.advance(1)
        return job["id"]

    def submit_video(self, broker, prompt):
        job = broker.submit("shutterstock-video", "generate", {"prompt": prompt})
        self.clock.advance(1)
        return job["id"]

    def completed_seconds(self, broker, job_ids):
        return sum(
            broker.attempts(job_id)[0]["finished"] - broker.attempts(job_id)[0]["started"]
            for job_id in job_ids
            if broker.status(job_id)["state"] == "completed"
        )

    def load_calls(self, model):
        return [
            call for call in self.calls
            if isinstance(call, tuple) and call[0] == "run"
            and call[2].get("model") == model and call[2].get("prompt") == ""
        ]

    def test_reciprocal_1_to_7_allocates_3150_and_450_execution_seconds(self):
        broker = self.make(execution_seconds=450)
        policy = self.policy({
            "olya-vision": {"enabled": True, "weight": 1},
            "shutterstock-video": {"enabled": True, "weight": 7},
        })
        olya = [self.submit_olya(broker, f"o{index}") for index in range(12)]
        video = [self.submit_video(broker, f"v{index}") for index in range(12)]

        for _ in range(8):
            self.assertTrue(broker.dispatch_once(policy=policy))

        # Reciprocal shares: 7/8 and 1/8 of a 3,600-second horizon.
        self.assertEqual(self.completed_seconds(broker, olya), 3_150)
        self.assertEqual(self.completed_seconds(broker, video), 450)
        self.assertEqual(
            sum(
                broker.attempts(job_id)[0]["finished"] - broker.attempts(job_id)[0]["started"]
                for job_id in olya + video if broker.status(job_id)["state"] == "completed"
            ),
            SCHEDULING_HORIZON_SECONDS,
        )

    def test_sustained_backlogs_form_two_contiguous_model_batches(self):
        broker = self.make(execution_seconds=225)
        policy = self.policy({
            "olya-vision": {"enabled": True, "weight": 1},
            "shutterstock-video": {"enabled": True, "weight": 7},
        })
        olya = [self.submit_olya(broker, f"o{index}") for index in range(18)]
        video = [self.submit_video(broker, f"v{index}") for index in range(18)]

        for _ in range(16):
            broker.dispatch_once(policy=policy)
        selected = [
            event["job_id"] for event in reversed(broker.audit_events(limit=1_000))
            if event["event_type"] == "scheduler.selected"
        ]
        models = [
            "gemma4:12b" if job_id in olya else "nemotron3:33b" for job_id in selected
        ]
        self.assertEqual(models, ["gemma4:12b"] * 14 + ["nemotron3:33b"] * 2)
        self.assertEqual(len(self.load_calls("gemma4:12b")), 1)
        self.assertEqual(len(self.load_calls("nemotron3:33b")), 1)
        self.assertEqual(
            [call[1] for call in self.calls if isinstance(call, tuple) and call[0] == "unload"],
            ["gemma4:12b"],
        )

    def test_non_preemptive_job_charges_the_full_boundary_overrun(self):
        durations = iter((3_200, 450))
        broker = self.make(execution_seconds=lambda _kind, _request: next(durations))
        policy = self.policy({
            "olya-vision": {"enabled": True, "weight": 1},
            "shutterstock-video": {"enabled": True, "weight": 7},
        })
        olya = self.submit_olya(broker, "long")
        video = self.submit_video(broker, "video")

        self.assertTrue(broker.dispatch_once(policy=policy))
        self.assertTrue(broker.dispatch_once(policy=policy))
        self.assertEqual(self.completed_seconds(broker, [olya]), 3_200)
        self.assertEqual(self.completed_seconds(broker, [video]), 450)
        first_context = broker.attempts(olya)[0]["scheduler_context"]
        self.assertEqual(first_context["time_budget_seconds"], 3_150.0)
        self.assertEqual(first_context["horizon_seconds"], 3_600)

    def test_admission_boundary_same_model_first_different_model_second(self):
        positions = {}
        for same_model in (True, False):
            for cancel_requested in (False, True):
                with self.subTest(same_model=same_model, cancel_requested=cancel_requested):
                    broker = self.make(execution_seconds=300)
                    policy = self.policy({"nextcloud": {"weight": 7}, "openclaw": {"weight": 1},
                                          "olya-vision": {"weight": 1}})
                    broker.source_policy = policy
                    def qwen(source, token):
                        job_id = broker.submit("nextcloud-eval-qwen", "generate",
                            {"prompt": token, "images": ["aGVsbG8="], "think": True},
                            source=source, external_id=token, source_item_id=token)["id"]
                        self.clock.advance(1)
                        return job_id
                    active = qwen("nextcloud", "active")
                    queued = [qwen("nextcloud", str(i)) for i in range(12)]
                    entered, release = threading.Event(), threading.Event()
                    original = broker._execute
                    def blocked(row):
                        entered.set()
                        if not release.wait(30):
                            raise RuntimeError("test release timeout")
                        original(row)
                    broker._execute = blocked
                    worker = threading.Thread(target=lambda: broker.dispatch_once(policy=policy))
                    worker.start()
                    try:
                        self.assertTrue(entered.wait(2))
                        if cancel_requested:
                            broker.cancel(active)
                        self.clock.advance(1)
                        if same_model:
                            lower = broker.submit("olya-vision-qwen", "generate",
                                self.olya_payload("new"), source="olya-vision")["id"]
                            # This profile targets the current qwen model.
                            self.assertEqual(broker.status(lower)["profile"], "olya-vision-qwen")
                        else:
                            lower = broker.submit("openclaw-gemma4", "chat",
                                {"messages": [{"role": "user", "content": "new"}]},
                                source="openclaw")["id"]
                        before = broker.status(active)
                        forecast = broker.forecast(policy)["next_selections"]
                        ids = [row["job_id"] for row in forecast]
                        position = ids.index(lower) + 1
                        positions[same_model] = position
                        self.assertEqual(position, 1 if same_model else 2)
                        self.assertFalse(broker.dispatch_once(policy=policy))
                        self.assertEqual(before, broker.status(active))
                        self.assertEqual(len(broker.attempts(active)), 1)
                    finally:
                        release.set()
                        worker.join(5)
                    self.assertFalse(worker.is_alive())
                    self.assertEqual(broker.status(active)["state"],
                                     "cancelled" if cancel_requested else "completed")
                    # Actual selections reproduce the forecast, including boundary.
                    for expected in ids:
                        self.assertTrue(broker.dispatch_once(policy=policy))
                        self.assertEqual(broker.db.execute(
                            "SELECT job_id FROM audit_events WHERE event_type='scheduler.selected' "
                            "ORDER BY sequence DESC LIMIT 1").fetchone()[0], expected)
        self.assertLess(positions[True], positions[False])

    def test_larger_arrival_does_not_jump_smaller_queued_work(self):
        broker = self.make()
        policy = self.policy({"olya-vision": {"weight": 1}, "shutterstock-video": {"weight": 7}})
        lower = [self.submit_olya(broker, str(i)) for i in range(4)]
        broker.dispatch_once(policy=policy)
        self.submit_video(broker, "new larger weight")
        self.assertEqual(broker.forecast(policy)["next_selections"][0]["job_id"], lower[1])

    def test_overlong_job_does_not_block_compatible_admission(self):
        broker = self.make(execution_seconds=4000)
        policy = self.policy({"nextcloud": {"weight": 7}, "olya-vision": {"weight": 1}})
        broker.source_policy = policy
        for i in range(8):
            broker.submit("nextcloud-eval-qwen", "generate",
                {"prompt": str(i), "images": ["aGVsbG8="], "think": True},
                source="nextcloud", external_id=str(i), source_item_id=str(i))
        broker.dispatch_once(policy=policy)
        lower = broker.submit("olya-vision-qwen", "generate", self.olya_payload("new"),
                              source="olya-vision")["id"]
        forecast = broker.forecast(policy)["next_selections"]
        self.assertEqual(forecast[0]["job_id"], lower)
        self.assertEqual(forecast[0]["model"], "qwen3-vl:30b")
        broker.dispatch_once(policy=policy)
        self.assertEqual(broker.status(lower)["state"], "completed")
        self.assertEqual(len(self.load_calls("qwen3-vl:30b")), 1)

    def test_lower_weight_cannot_bypass_disabled_or_min_interval_gates(self):
        broker = self.make()
        policy = self.policy({
            "olya-vision": {"enabled": False, "weight": 1},
            "interactive": {"weight": 2}, "shutterstock-video": {"weight": 7},
        })
        disabled = self.submit_olya(broker, "disabled")
        delayed = broker.submit("interactive", "generate", {"prompt": "delayed"})["id"]
        broker.db.execute("INSERT INTO source_schedules(source,next_allowed) VALUES(?,?)",
                          ("interactive", self.clock() + 100))
        broker.db.commit()
        eligible = self.submit_video(broker, "eligible")
        self.assertEqual([r["job_id"] for r in broker.forecast(policy)["next_selections"]], [eligible])
        self.assertTrue(broker.dispatch_once(policy=policy))
        self.assertEqual(broker.status(eligible)["state"], "completed")
        self.assertEqual(broker.status(disabled)["state"], "queued")
        self.assertEqual(broker.status(delayed)["state"], "queued")

    def test_empty_disabled_and_reactivated_lanes_remain_work_conserving(self):
        broker = self.make(execution_seconds=600)
        policy = self.policy({
            "olya-vision": {"enabled": True, "weight": 1},
            "shutterstock-video": {"enabled": True, "weight": 7},
        })
        videos = [self.submit_video(broker, f"v{index}") for index in range(7)]

        # Olya is empty, so Shutterstock consumes capacity immediately.
        self.assertTrue(broker.dispatch_once(policy=policy))
        self.assertEqual(broker.status(videos[0])["state"], "completed")
        returning = self.submit_olya(broker, "returned")
        for _ in range(6):
            self.assertTrue(broker.dispatch_once(policy=policy))
            if broker.status(returning)["state"] == "completed":
                break
        self.assertEqual(broker.status(returning)["state"], "completed")

        disabled_broker = self.make(execution_seconds=600)
        disabled = self.policy({
            "olya-vision": {"enabled": False, "weight": 1},
            "shutterstock-video": {"enabled": True, "weight": 7},
        })
        blocked = self.submit_olya(disabled_broker, "disabled")
        available = self.submit_video(disabled_broker, "available")
        self.assertTrue(disabled_broker.dispatch_once(policy=disabled))
        self.assertEqual(disabled_broker.status(available)["state"], "completed")
        self.assertEqual(disabled_broker.status(blocked)["state"], "queued")
        disabled.set_enabled("olya-vision", True)
        self.assertTrue(disabled_broker.dispatch_once(policy=disabled))
        self.assertEqual(disabled_broker.status(blocked)["state"], "completed")

        def fail_gemma(_kind, request):
            if request["model"] == "gemma4:12b":
                raise RuntimeError("synthetic lane failure")
            return 600

        failed_broker = self.make(execution_seconds=fail_gemma)
        failed_policy = self.policy({
            "olya-vision": {"enabled": True, "weight": 1},
            "shutterstock-video": {"enabled": True, "weight": 7},
        })
        failed = self.submit_olya(failed_broker, "fails")
        after_failure = self.submit_video(failed_broker, "still-runs")
        self.assertTrue(failed_broker.dispatch_once(policy=failed_policy))
        self.assertEqual(failed_broker.status(failed)["state"], "failed")
        self.assertTrue(failed_broker.dispatch_once(policy=failed_policy))
        self.assertEqual(failed_broker.status(after_failure)["state"], "completed")

    def test_forecast_of_ten_uses_the_same_time_batch_order_as_dispatch(self):
        broker = self.make(execution_seconds=300)
        policy = self.policy({
            "olya-vision": {"enabled": True, "weight": 1},
            "shutterstock-video": {"enabled": True, "weight": 7},
        })
        job_ids = [self.submit_olya(broker, f"o{index}") for index in range(12)]
        job_ids.extend(self.submit_video(broker, f"v{index}") for index in range(12))

        forecast = broker.forecast(policy)
        self.assertEqual(len(forecast["next_selections"]), 10)
        forecast_ids = [item["job_id"] for item in forecast["next_selections"]]
        self.assertEqual(
            [item["model"] for item in forecast["next_selections"]],
            ["gemma4:12b"] * 10,
        )
        actual = []
        for _ in range(10):
            self.assertTrue(broker.dispatch_once(policy=policy))
            selected = broker.db.execute(
                "SELECT job_id FROM audit_events WHERE event_type='scheduler.selected' "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()[0]
            actual.append(selected)
        self.assertEqual(actual, forecast_ids)

    def test_forecast_api_and_dashboard_default_to_ten_or_fewer(self):
        broker = self.make()
        policy = self.policy({"interactive": {"enabled": True, "weight": 1}})
        for index in range(12):
            broker.submit("interactive", "generate", {"prompt": str(index)})
            self.clock.advance(1)
        self.assertEqual(len(broker.forecast(policy)["next_selections"]), 10)
        dashboard = broker.dashboard(policy)
        dashboard_ids = [item["job_id"] for item in dashboard["forecast"]["next_selections"]]
        self.assertEqual(len(dashboard_ids), 10)
        from broker.dashboard import render
        html = render(dashboard).decode()
        self.assertTrue(all(job_id in html for job_id in dashboard_ids))

        server = serve(broker, port=0, policy=policy)
        self.addCleanup(server.server_close)
        worker = threading.Thread(target=server.handle_request)
        worker.start()
        try:
            with urlopen(f"http://127.0.0.1:{server.server_port}/v1/forecast") as response:
                body = json.loads(response.read())
        finally:
            worker.join(timeout=1)
        self.assertEqual(len(body["next_selections"]), 10)

        fewer = self.make()
        for index in range(3):
            fewer.submit("interactive", "generate", {"prompt": str(index)})
        self.assertEqual(len(fewer.forecast(policy)["next_selections"]), 3)
        self.assertEqual(len(fewer.dashboard(policy)["forecast"]["next_selections"]), 3)

    def test_interactive_compatibility_and_no_policy_fifo_are_unchanged(self):
        broker = self.make(execution_seconds=1)
        interactive = compatibility_submit(broker, "generate", {
            "profile": "interactive", "prompt": "interactive", "priority": 1,
        })
        self.clock.advance(1)
        later = broker.submit("cron", "generate", {"prompt": "later"})

        self.assertNotIn("priority", interactive["payload"])
        self.assertTrue(broker.dispatch_once())
        self.assertEqual(broker.status(interactive["id"])["state"], "completed")
        self.assertEqual(broker.status(later["id"])["state"], "queued")
        self.assertEqual(broker.attempts(interactive["id"])[0]["scheduler_mode"], "fifo")

    def test_forecast_is_read_only_and_payload_free(self):
        broker = self.make()
        policy = self.policy({"interactive": {"enabled": True, "weight": 1}})
        broker.submit("interactive", "generate", {"prompt": "secret"})
        before_events = broker.db.execute("SELECT count(*) FROM audit_events").fetchone()[0]
        first = broker.forecast(policy)
        second = broker.forecast(policy)
        self.assertEqual(first, second)
        self.assertNotIn("secret", json.dumps(first))
        self.assertEqual(
            broker.db.execute("SELECT count(*) FROM audit_events").fetchone()[0], before_events,
        )


if __name__ == "__main__":
    unittest.main()
