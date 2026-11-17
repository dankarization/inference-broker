"""Offline source-local model turns: real claims/attempts, fake executor only."""
from contextlib import contextmanager
from copy import deepcopy
import json
import logging
from pathlib import Path
import tempfile
import threading
import unittest

from broker.profiles import PROFILES
from broker.service import (Broker, MODEL_BATCH_MAX_JOBS, MODEL_BATCH_MAX_SECONDS,
                            MODEL_BATCH_MAX_WAIT_SECONDS, SourcePolicy)
from test_batch_scheduler import FakeOllama, FakeWol, MutableClock

MODELS = ("glm-ocr:latest", "gemma4:12b", "qwen3-vl:30b")
SOURCE = "video-summary-frames"


class SourceModelBatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = MutableClock(1000)
        self.calls = []
        self.serial = 0
        self.path = Path(self.tmp.name) / "jobs.db"
        self.ollama = FakeOllama(self.calls, self.clock, 20)
        self.b = self.open()
        self.p = self.policy({SOURCE: {"weight": 3}})

    def open(self):
        b = Broker(self.path, self.ollama, FakeWol(self.calls), clock=self.clock)
        self.addCleanup(b.db.close)
        return b

    def restart(self):
        self.b.db.close()
        self.b = self.open()

    def policy(self, sources):
        p = Path(self.tmp.name) / ("policy-" + str(self.serial) + ".json")
        self.serial += 1
        p.write_text(json.dumps({"sources": sources}))
        return SourcePolicy(p)

    def submit(self, model, source=SOURCE, profile="interactive"):
        # General profiles permit validated actual-model overrides. Default
        # requested num_ctx/output remain within the supplied profile budget.
        job = self.b.submit(profile, "generate",
                            {"model": model, "prompt": "synthetic fixture"},
                            source=source)
        self.clock.advance(.01)
        return job["id"]

    def enqueue(self, repetitions=20, models=MODELS[:2], source=SOURCE):
        return [self.submit(m, source) for _ in range(repetitions) for m in models]

    def step(self, count=1):
        out = []
        for _ in range(count):
            self.assertTrue(self.b.dispatch_once(policy=self.p))
            row = self.b.db.execute(
                "SELECT job_id,metadata_json FROM audit_events WHERE event_type='scheduler.selected' "
                "ORDER BY sequence DESC LIMIT 1").fetchone()
            out.append((row["job_id"], json.loads(row["metadata_json"])))
        return out

    @staticmethod
    def runs(models):
        groups = []
        for model in models:
            if not groups or groups[-1][0] != model:
                groups.append([model, 1])
            else:
                groups[-1][1] += 1
        return groups

    def assert_forecast_matches(self, count=20):
        # Known estimates are a precondition for exact projected time boundaries.
        for row in self.b._candidates():
            self.b._duration_estimates[self.b._lane_key(row)] = self.ollama.execution_seconds
        self.b._publish_scheduler_snapshot()
        before = deepcopy(self.b._scheduler_snapshot)
        changes = self.b.db.total_changes
        forecast = self.b.forecast(self.p, count)
        self.assertFalse(forecast.get("unavailable"), forecast)
        self.assertEqual(self.b.forecast(self.p, count), forecast)
        self.assertEqual(self.b._scheduler_snapshot, before)
        self.assertEqual(self.b.db.total_changes, changes)
        actual = self.step(len(forecast["next_selections"]))
        self.assertEqual([j for j, _ in actual],
                         [r["job_id"] for r in forecast["next_selections"]])
        return actual

    def test_interleaved_two_models_form_ten_job_blocks_fifo_and_reduce_switches(self):
        ids = self.enqueue(20)
        before = [(j, self.b.status(j)["payload"]["model"]) for j in ids]
        actual = self.step(40)
        models = [d["selected_model"] for _, d in actual]
        self.assertEqual(self.runs(models), [[MODELS[0], 10], [MODELS[1], 10]] * 2)
        for model in MODELS[:2]:
            self.assertEqual([j for j, d in actual if d["selected_model"] == model],
                             [j for j, m in before if m == model])
        self.assertEqual(sum(a != b for a, b in zip(models, models[1:])), 3)
        self.assertEqual(sum(a[1] != b[1] for a, b in zip(before, before[1:])), 39)

    def test_three_models_rotate_in_finite_fifo_turns_with_forecast_parity(self):
        self.enqueue(21, MODELS)
        actual = self.assert_forecast_matches()
        actual += self.step(43)
        models = [d["selected_model"] for _, d in actual]
        self.assertEqual(self.runs(models)[:6], [[m, 10] for m in MODELS] * 2)
        self.assertEqual(models[:30:10], list(MODELS))
        self.assertTrue(all(n <= 10 for _, n in self.runs(models)))
        # Every lane's head is served within at most two preceding turns.
        for model in MODELS:
            positions = [i for i, m in enumerate(models[:60]) if m == model]
            self.assertLessEqual(positions[0], 20)
            self.assertLessEqual(max(b-a for a, b in zip(positions, positions[1:])), 21)

    def test_profiles_with_same_actual_model_share_one_turn_and_fifo(self):
        expected = []
        for i in range(12):
            profile = "interactive" if i % 2 else "cron"
            expected.append(self.submit(MODELS[0], profile=profile))
            self.submit(MODELS[1], profile=profile)
        actual = self.assert_forecast_matches()
        self.assertEqual([j for j, _ in actual[:10]], expected[:10])
        self.assertEqual([d["selected_model"] for _, d in actual[:10]], [MODELS[0]] * 10)
        self.assertEqual(self.b._time_cycle["sources"].keys(), {SOURCE})

    def test_equal_weight_cross_source_model_blocks_and_forecast_parity(self):
        self.p = self.policy({SOURCE: {"weight": 3}, "peer": {"weight": 3}})
        ids = {}
        for source in (SOURCE, "peer"):
            for model in MODELS:
                ids[source, model] = [self.submit(model, source,
                    profile="cron" if i % 2 else "interactive") for i in range(20)]
        actual = self.assert_forecast_matches(20) + self.step(100)
        models = [d["selected_model"] for _, d in actual]
        self.assertEqual(self.runs(models)[0], [MODELS[0], 20])
        # Waiting/time boundaries may force later turns sooner than 10 claims.
        self.assertLessEqual(len(self.runs(models)), 12)
        self.assertEqual(len({job for job, _ in actual}), 120)
        for (source, model), expected in ids.items():
            selected = [j for j, d in actual
                        if d["selected_source"] == source and d["selected_model"] == model]
            self.assertEqual(selected, expected)
        self.assertEqual(sum(1 for _, d in actual if d["selected_source"] == SOURCE), 60)

    def test_cross_source_affinity_cannot_override_unequal_weight_or_exhausted_budget(self):
        self.p = self.policy({SOURCE: {"weight": 3}, "peer": {"weight": 4}})
        self.enqueue(20, MODELS[:2], SOURCE)
        self.enqueue(20, MODELS[:2], "peer")
        first = self.step(11)
        self.assertEqual([d["selected_source"] for _, d in first], [SOURCE] * 11)
        self.assertEqual(first[-1][1]["selected_model"], MODELS[1])

    def test_equal_weight_model_affinity_never_extends_source_local_turn(self):
        self.p = self.policy({SOURCE: {"weight": 3}, "peer": {"weight": 3}})
        self.enqueue(25, MODELS[:2], SOURCE)
        self.enqueue(25, MODELS[:2], "peer")
        actual = self.step(42)
        for source in (SOURCE, "peer"):
            local = [d["selected_model"] for _, d in actual if d["selected_source"] == source]
            self.assertTrue(all(count <= 10 for _, count in self.runs(local)))
        self.assertEqual(self.runs([d["selected_model"] for _, d in actual])[:2],
                         [[MODELS[0], 20], [MODELS[1], 20]])

    def test_all_sources_share_one_conditional_scenario_legacy_alias(self):
        jobs = self.enqueue(2, MODELS[:2])
        data = self.b.forecast(self.p)
        self.assertEqual(data["conditional_scenario"]["basis"], "all_eligible_sources")
        self.assertEqual(data["conditional_scenario"]["next_selections"], data["next_selections"])
        self.assertEqual({r["job_id"] for r in data["next_selections"]}, set(jobs))

    def test_forecast_stops_before_delayed_source_can_join(self):
        self.p = self.policy({SOURCE: {"weight": 3}, "peer": {"weight": 3}})
        self.enqueue(5, (MODELS[0],))
        self.submit(MODELS[1], "peer")
        with self.b.db:
            self.b.db.execute("INSERT INTO source_schedules(source,next_allowed) VALUES(?,?)",
                              ("peer", self.clock() + 5))
        self.b._duration_estimates[(SOURCE, MODELS[0])] = 20
        self.b._publish_scheduler_snapshot()
        forecast = self.b.forecast(self.p)
        self.assertEqual(len(forecast["next_selections"]), 1)
        self.assertEqual(forecast["conditional_scenario"]["stop_reason"],
                         "source_schedule_recheck_required")
        self.assertTrue(self.b.dispatch_once(policy=self.p))
        self.assertEqual(self.b._last_scheduler_decision["selected_source"], SOURCE)

    def test_delayed_source_ready_before_running_job_finishes_stops_at_start(self):
        self.p = self.policy({SOURCE: {"weight": 3}, "peer": {"weight": 3}})
        self.enqueue(2, (MODELS[0],))
        self.submit(MODELS[1], "peer")
        with self.b.db:
            self.b.db.execute("INSERT INTO source_schedules(source,next_allowed) VALUES(?,?)",
                              ("peer", self.clock() + 5))
            self.b.db.execute("UPDATE jobs SET state='running' WHERE id=(SELECT id FROM jobs WHERE source=? LIMIT 1)",
                              (SOURCE,))
        self.b._active_time_batches["running"] = {"lane": (SOURCE, MODELS[0]), "started": self.clock()}
        self.b._duration_estimates[(SOURCE, MODELS[0])] = 20
        self.b._publish_scheduler_snapshot()
        data = self.b.forecast(self.p)
        self.assertEqual(data["next_selections"], [])
        self.assertEqual(data["conditional_scenario"]["stop_reason"],
                         "source_schedule_recheck_required")

    def test_forecast_refuses_truncated_raw_lanes(self):
        from unittest.mock import patch
        self.enqueue(2, MODELS)
        with patch("broker.service.FORECAST_MAX_RAW_LANES", 2):
            forecast = self.b.forecast(self.p)
        self.assertTrue(forecast["unavailable"])
        self.assertNotIn("next_selections", forecast)

    def test_time_limit_switches_earlier_and_never_preempts_long_job(self):
        self.ollama.execution_seconds = 80
        self.enqueue()
        actual = self.assert_forecast_matches()
        self.assertEqual(self.runs([d["selected_model"] for _, d in actual])[:2],
                         [[MODELS[0], 4], [MODELS[1], 4]])
        self.assertEqual(actual[4][1]["reason"].split("; ")[-1], "model execution-time limit")

    def test_wait_limit_uses_wait_since_service_not_permanently_old_queue_age(self):
        self.enqueue()
        self.step(2)
        self.clock.advance(MODEL_BATCH_MAX_WAIT_SECONDS + 1)
        first = self.step()[0]
        self.assertEqual(first[1]["selected_model"], MODELS[1])
        self.assertIn("competing model wait limit", first[1]["reason"])
        # It may take one turn of A after B because both had waited during the
        # idle gap. Afterwards old queued_at cannot force permanent one-job turns.
        actual = self.step(12)
        self.assertIn(10, [n for _, n in self.runs([d["selected_model"] for _, d in actual])])

    def test_arrival_during_another_sources_long_job_keeps_its_real_wait_age(self):
        self.p = self.policy({SOURCE: {"weight": 3}, "peer": {"weight": 1}})
        self.enqueue(20, (MODELS[0],))
        self.step()
        self.submit(MODELS[2], "peer")
        new = []
        def execution(kind, request):
            if request["model"] == MODELS[2]:
                # Executor calls occur outside the claim lock/transaction.
                # B arrives AFTER the scheduler's last candidate snapshot.
                new.append(self.submit(MODELS[1]))
                self.clock.advance(670)
                return 0
            return 20
        self.ollama.execution_seconds = execution
        self.step(2)  # Established different-model admission boundary, then peer.
        chosen, decision = self.step()[0]
        self.assertEqual(chosen, new[0])
        self.assertIn("competing model wait limit", decision["reason"])

    def test_new_lane_joins_tail_without_overtaking_third_waiter(self):
        self.enqueue(24, MODELS)
        self.step(3)
        fourth = self.submit("nemotron3:33b")
        actual = self.step(28)
        models = [d["selected_model"] for _, d in actual]
        self.assertEqual(models[7:27:10], list(MODELS[1:]))
        self.assertEqual(actual[27][0], fourth)

    def test_empty_reappearing_lanes_do_not_idle_or_create_source_budgets(self):
        a = self.submit(MODELS[0])
        b = self.submit(MODELS[1])
        self.assertEqual([j for j, _ in self.step(2)], [a, b])
        self.assertFalse(self.b.dispatch_once(policy=self.p))
        later = self.submit(MODELS[0])
        self.assertEqual(self.step()[0][0], later)
        self.assertEqual(list(self.b._time_cycle["sources"]), [SOURCE])
        # A previously solo lane with an exhausted turn yields immediately.
        self.enqueue(12, (MODELS[0],))
        self.step(11)
        new = self.submit(MODELS[2])
        self.assertEqual(self.step()[0][0], new)

    def test_source_weights_not_multiplied_by_two_or_three_models(self):
        self.ollama.execution_seconds = 150
        self.p = self.policy({SOURCE: {"weight": 3}, "peer": {"weight": 1}})
        self.enqueue(12, MODELS)
        self.enqueue(20, ("nemotron3:33b",), "peer")
        actual = self.step(24)
        seconds = {s: sum(150 for _, d in actual if d["selected_source"] == s)
                   for s in (SOURCE, "peer")}
        self.assertEqual(seconds, {SOURCE: 900, "peer": 2700})
        self.assertEqual(self.b._time_cycle["sources"][SOURCE]["budget"], 900)
        self.assertEqual(self.b._time_cycle["sources"]["peer"]["budget"], 2700)

    def test_new_model_does_not_rebase_source_time_share(self):
        self.p = self.policy({SOURCE: {"weight": 3}, "peer": {"weight": 1}})
        self.enqueue(60, (MODELS[0],))
        self.enqueue(150, ("nemotron3:33b",), "peer")
        # Peer holds its 2700s budget; then this source starts its 900s.
        self.step(136)
        before = deepcopy(self.b._time_cycle)
        self.submit(MODELS[1])
        self.step()
        self.assertEqual(self.b._time_cycle["started_at"], before["started_at"])
        self.assertEqual(self.b._time_cycle["sources"][SOURCE]["budget"], 900)
        self.assertEqual(len(self.b._time_cycle["sources"]), 2)

    def test_over_horizon_jobs_do_not_reset_away_unserved_sources(self):
        self.ollama.execution_seconds = 4000
        self.p = self.policy({SOURCE: {"weight": 1}, "peer": {"weight": 7}})
        self.enqueue(6, MODELS)
        self.enqueue(6, ("nemotron3:33b",), "peer")
        actual = self.assert_forecast_matches(10)
        self.assertEqual([d["selected_source"] for _, d in actual], [SOURCE, "peer"] * 5)

    def test_restart_preserves_job_count_turn_order_and_time_usage(self):
        self.enqueue(30, MODELS)
        first = self.step(7)
        self.restart()
        self.assertEqual(self.b._model_batches[SOURCE]["jobs"], 7)
        self.assertEqual(self.b._model_batches[SOURCE]["seconds"], 140)
        actual = self.assert_forecast_matches()
        self.assertEqual([d["selected_model"] for _, d in actual[:3]], [MODELS[0]] * 3)
        self.assertEqual([d["selected_model"] for _, d in actual[3:13]], [MODELS[1]] * 10)
        # Repeated restarts cannot keep buying first turns for A.
        self.restart()
        self.assertEqual(self.step()[0][1]["selected_model"], MODELS[2])
        self.assertEqual(len(first), 7)

    def test_restart_after_time_boundary_and_missing_legacy_evidence(self):
        self.ollama.execution_seconds = 180
        self.enqueue()
        self.step(2)
        self.restart()
        self.assertEqual(self.step()[0][1]["selected_model"], MODELS[1])
        # A legacy scheduler audit without the new metadata starts a fresh turn.
        with self.b.db:
            self.b.db.execute("UPDATE audit_events SET metadata_json='{}' "
                              "WHERE event_type='scheduler.selected'")
        self.restart()
        self.assertEqual(self.b._model_batches, {})
        oldest = self.b._lane_key(self.b._candidates()[0])[1]
        self.assertEqual(self.step()[0][1]["selected_model"], oldest)

    def test_cancel_and_retry_preserve_fifo_in_lane_and_spend_attempt_turns(self):
        self.enqueue()
        cancelled = self.b._candidates()[2]["id"]
        self.b.cancel(cancelled)
        self.clock.advance(1)
        self.b.retry(cancelled)
        actual = self.step(10)
        self.assertNotIn(cancelled, [j for j, _ in actual])
        self.assertEqual(self.b._model_batches[SOURCE]["jobs"], 10)
        actual += self.step(30)
        lane = [j for j, d in actual if d["selected_model"] == MODELS[0]]
        self.assertEqual(lane[-1], cancelled)
        self.assertEqual(self.b.status(cancelled)["retry_count"], 1)

    def test_running_nonpreemptive_cancel_requested_with_competing_model(self):
        entered, release = threading.Event(), threading.Event()
        def duration(kind, payload):
            entered.set()
            self.assertTrue(release.wait(3))
            return 4000
        self.ollama.execution_seconds = duration
        self.enqueue()
        worker = threading.Thread(target=lambda: self.b.dispatch_once(policy=self.p))
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            row = self.b.db.execute("SELECT id FROM jobs WHERE state='running'").fetchone()
            self.b.cancel(row["id"])
            self.assertFalse(self.b.dispatch_once(policy=self.p))
            self.assertEqual(len(self.b.attempts(row["id"])), 1)
            forecast = self.b.forecast(self.p, 1)
            self.assertEqual(forecast["next_selections"][0]["model"], MODELS[1])
        finally:
            release.set()
            worker.join(4)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.b.status(row["id"])["state"], "cancelled")
        self.ollama.execution_seconds = 20
        self.assertEqual(self.step()[0][1]["selected_model"], MODELS[1])

    def test_nextcloud_new_uploads_remain_strict_above_model_affinity(self):
        self.p = self.policy({"nextcloud": {"weight": 7}})
        # Synthetic metadata deliberately isolates scheduler priority semantics.
        self.enqueue(12, MODELS[:2])
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET source='nextcloud'")
        self.step()
        chosen = self.b.db.execute("SELECT id FROM jobs WHERE state='queued' "
                                  "AND json_extract(payload,'$.model')=? ORDER BY queued_at,id LIMIT 1",
                                  (MODELS[1],)).fetchone()[0]
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET nextcloud_priority='new_upload' WHERE id=?", (chosen,))
        self.assertEqual(self.step()[0][0], chosen)

    def test_fence_and_pause_never_spend_model_turns(self):
        self.enqueue()
        self.b.set_queue_paused(True)
        self.assertFalse(self.b.dispatch_once(policy=self.p))
        self.assertEqual(self.b._model_batches, {})
        self.b.set_queue_paused(False)
        self.b.reconcile_executor_fence = lambda: False
        self.assertFalse(self.b.dispatch_once(policy=self.p))
        self.assertEqual(self.b._model_batches, {})

    def test_failed_attempts_consume_model_job_limit_and_explicit_retry_is_fifo(self):
        self.enqueue(12)
        def fail(kind, request):
            raise RuntimeError("synthetic failure")
        self.ollama.execution_seconds = fail
        first = self.step(11)
        self.assertEqual([d["selected_model"] for _, d in first],
                         [MODELS[0]] * 10 + [MODELS[1]])
        self.assertTrue(all(self.b.status(j)["state"] == "failed" for j, _ in first))
        self.b.retry(first[0][0])
        self.ollama.execution_seconds = 20
        self.assertNotEqual(self.step()[0][0], first[0][0])

    def test_nextcloud_owner_tier_gate_precedes_model_lanes_and_forecast_is_conditional(self):
        from broker.nextcloud_priority import NextcloudPriority
        class Gate(NextcloudPriority):
            @contextmanager
            def hold(gate, db):
                gate.value = gate.fixture
                try:
                    yield
                finally:
                    gate.value = None
        gate = Gate(lambda: None)  # No owner DB/network access in this test.
        self.enqueue(12)
        rows = self.b._candidates()
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET source='nextcloud',external_id=id")
        gate.fixture = {"jobs": {r["id"]: (1 if r["target_model"] == MODELS[1] else 3)
                                for r in rows}, "floor": 3, "scope_complete": True}
        self.b.nextcloud_priority = gate
        self.p = self.policy({"nextcloud": {"weight": 3}})
        for _ in range(12):
            self.assertEqual(self.step()[0][1]["selected_model"], MODELS[1])
        self.assertIsNone(gate.value)
        forecast = self.b.forecast(self.p)
        self.assertFalse(forecast.get("unavailable"))
        self.assertEqual(forecast["next_selections"], [])
        self.assertEqual(forecast["owner_gate"]["state"], "observed_tier_exhausted")
        self.assertTrue(forecast["unranked_candidates"])
        self.assertEqual(forecast["conditional_scenario"]["basis"], "nextcloud_excluded")
        gate.fixture = None
        self.assertFalse(self.b.dispatch_once(policy=self.p))

    def test_unconfirmed_attempt_after_restart_cannot_buy_another_time_slice(self):
        self.enqueue()
        self.step()
        latest = self.b.db.execute(
            "SELECT job_id,attempt_no FROM job_attempts ORDER BY selected_at DESC LIMIT 1").fetchone()
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET state='running',finished=NULL,lease_until=? WHERE id=?",
                              (self.clock() + 100, latest["job_id"]))
            self.b.db.execute("UPDATE job_attempts SET finished=NULL,outcome=NULL WHERE job_id=?",
                              (latest["job_id"],))
        self.restart()
        self.assertEqual(self.b._model_batches[SOURCE]["seconds"], MODEL_BATCH_MAX_SECONDS)
        self.assertFalse(self.b.dispatch_once(policy=self.p))
        self.clock.advance(101)
        self.b.recover()
        self.assertEqual(self.step()[0][1]["selected_model"], MODELS[1])

    def test_missing_attempt_evidence_does_not_buy_a_fresh_model_time_slice(self):
        self.enqueue()
        selected = self.step()[0][0]
        with self.b.db:
            self.b.db.execute("DELETE FROM job_attempts WHERE job_id=?", (selected,))
        self.restart()
        self.assertEqual(self.b._model_batches[SOURCE]["seconds"], MODEL_BATCH_MAX_SECONDS)
        self.assertEqual(self.step()[0][1]["selected_model"], MODELS[1])

    def test_bad_restored_metadata_fails_closed_to_a_fresh_turn(self):
        self.enqueue()
        self.step()
        with self.b.db:
            self.b.db.execute(
                "UPDATE audit_events SET metadata_json=? WHERE event_type='scheduler.selected'",
                (json.dumps({"model_batch": {"version": 1, "model": MODELS[0], "order": [False]}}),))
        self.restart()
        self.assertEqual(self.b._model_batches, {})
        self.assertTrue(self.b.dispatch_once(policy=self.p))

    def test_clock_rollback_does_not_undo_job_limit_or_restore_event_sequence(self):
        self.enqueue()
        self.step(6)
        self.clock.advance(-100)
        self.step(4)
        self.restart()
        self.assertEqual(self.b._model_batches[SOURCE]["jobs"], 10)
        self.assertEqual(self.step()[0][1]["selected_model"], MODELS[1])

    def test_policy_reload_rebases_sources_but_not_model_job_limit(self):
        self.enqueue()
        self.step(9)
        self.p.set_weight(SOURCE, 1)
        actual = self.step(2)
        self.assertEqual([d["selected_model"] for _, d in actual], list(MODELS[:2]))
        self.assertEqual(actual[0][1]["active_weights"][SOURCE], 1)

    def test_zero_cost_attempts_still_rotate_within_one_source(self):
        self.ollama.execution_seconds = 0
        self.enqueue(21, MODELS)
        models = [d["selected_model"] for _, d in self.step(60)]
        self.assertEqual(self.runs(models), [[m, 10] for m in MODELS] * 2)

    def test_legacy_null_model_profile_defaults_merge_with_explicit_target(self):
        default = PROFILES["interactive"].model
        ids = [self.submit(default) for _ in range(5)]
        self.enqueue(5, (MODELS[0],))
        with self.b.db:
            for j in ids[::2]:
                self.b.db.execute("UPDATE jobs SET payload=? WHERE id=?",
                                  ('{"prompt":"fixture"}', j))
        rows = self.b._candidates(frozenset({SOURCE}), limit_per_source=20, source_priority=True)
        lanes = self.b._source_heads(rows)
        self.assertEqual(set(lanes), {(SOURCE, MODELS[0]), (SOURCE, default)})
        self.assert_forecast_matches(10)

    def test_forecast_reads_one_head_per_raw_lane_then_only_selected_successors(self):
        for _ in range(30):
            for profile in ("interactive", "cron"):
                for model in MODELS[:2]:
                    self.submit(model, profile=profile)
        initial, successors = [], []
        candidates = self.b._candidates
        successor = self.b._forecast_lane_successor
        def get_candidates(*args, **kwargs):
            result = candidates(*args, **kwargs)
            initial.append((kwargs["limit_per_source"], len(result)))
            return result
        def get_successor(*args, **kwargs):
            result = successor(*args, **kwargs)
            successors.append(result)
            return result
        self.b._candidates = get_candidates
        self.b._forecast_lane_successor = get_successor
        forecast = self.b.forecast(self.p, 20)
        self.assertEqual(len(forecast["next_selections"]), 20)
        self.assertEqual(initial, [(1, 4)])
        self.assertEqual(len(successors), 19)
        self.assertTrue(all(row is not None for row in successors))

    def test_nextcloud_forecast_replenishes_priority_heads_without_historical_overtake(self):
        ids = self.enqueue(6)
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET source='nextcloud'")
            for job in (ids[4], ids[9]):
                self.b.db.execute("UPDATE jobs SET nextcloud_priority='new_upload' WHERE id=?", (job,))
        self.p = self.policy({"nextcloud": {"weight": 7}})
        actual = self.assert_forecast_matches(12)
        self.assertEqual([j for j, _ in actual[:2]], [ids[4], ids[9]])

    def test_deep_lane_forecast_and_private_exclusion_are_index_bounded(self):
        # The competing model lies beyond the old ten-per-source prefix.
        self.enqueue(500, (MODELS[0],))
        self.enqueue(10, (MODELS[1],))
        hidden = self.submit(MODELS[2])
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET producer_attempt_id='hidden' WHERE id=?", (hidden,))
        self.b._duration_estimates[(SOURCE, MODELS[0])] = 20
        self.b._duration_estimates[(SOURCE, MODELS[1])] = 20
        self.b._publish_scheduler_snapshot()
        statements = []
        self.b.db.set_trace_callback(statements.append)
        vm_steps = []
        self.b.db.set_progress_handler(lambda: vm_steps.append(1) or 0, 100)
        candidates = self.b._candidates(frozenset({SOURCE}), limit_per_source=20,
                                       source_priority=True, include_producer_storage=False)
        self.b.db.set_progress_handler(None, 0)
        self.b.db.set_trace_callback(None)
        self.assertNotIn(hidden, [r["id"] for r in candidates])
        self.assertEqual(len(candidates), 30)
        self.assertLess(len(vm_steps), 100)
        self.assertTrue(any("jobs_model_heads_public" in s for s in statements))
        forecast = self.b.forecast(self.p, 20, include_producer_storage=False)
        self.assertEqual([r["model"] for r in forecast["next_selections"]],
                         [MODELS[0]] * 10 + [MODELS[1]] * 10)
        self.assertNotIn(hidden, json.dumps(forecast))


if __name__ == "__main__":
    logging.getLogger("ollama_inference_broker.audit").setLevel(logging.CRITICAL)
    unittest.main()
