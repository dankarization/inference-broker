"""Offline owner-gate forecasts: real SQLite/claim path, fake owner and executor."""
from contextlib import contextmanager
from copy import deepcopy
import json
import threading
import unittest
from unittest.mock import patch
from urllib.request import urlopen

from broker.forecast import capture_owner_preview, MAX_OWNER_PREVIEW_ROWS
from broker.http import serve
from broker.nextcloud_priority import NextcloudPriority
import test_source_model_batches as fixtures

MODELS = fixtures.MODELS


class Gate(NextcloudPriority):
    def __init__(self):
        super().__init__(lambda: (_ for _ in ()).throw(AssertionError("No owner network")))
        self.fixture = {"jobs": {}, "floor": 3, "scope_complete": True}
        self.reads = 0

    @contextmanager
    def hold(self, db):
        self.reads += 1
        self.value = self.fixture
        try:
            yield
        finally:
            self.value = None


class OwnerGateForecastTests(unittest.TestCase):
    setUp = fixtures.SourceModelBatchTests.setUp
    open = fixtures.SourceModelBatchTests.open
    policy = fixtures.SourceModelBatchTests.policy
    submit = fixtures.SourceModelBatchTests.submit
    step = fixtures.SourceModelBatchTests.step

    def prepare(self):
        self.gate = Gate()
        self.b.nextcloud_priority = self.gate
        self.p = self.policy({"nextcloud": {"weight": 7}, "peer": {"weight": 20}})

    def nc(self, model=MODELS[0], tier=3, priority="historical", private=False):
        job = self.submit(model)
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET source='nextcloud',external_id=id,"
                             "nextcloud_priority=?,producer_attempt_id=? WHERE id=?",
                             (priority, "secret-owner" if private else None, job))
        self.gate.fixture["jobs"][job] = tier
        self.b._duration_estimates[("nextcloud", model)] = 20
        self.b._publish_scheduler_snapshot()
        return job

    def projected(self, count=20, public=False):
        data = self.b.forecast(self.p, count, include_producer_storage=not public)
        self.assertFalse(data.get("unavailable"), data)
        self.assertEqual(data["next_selections"], [])
        return data, data["conditional_scenario"]["next_selections"]

    def test_no_observation_keeps_useful_other_source_scenario_and_unranked_nc(self):
        self.prepare()
        nc = self.nc()
        peer = self.submit(MODELS[2], "peer")
        data, rows = self.projected()
        self.assertEqual(data["owner_gate"]["state"], "not_observed")
        self.assertEqual([r["job_id"] for r in rows], [peer])
        self.assertEqual(data["unranked_candidates"][0]["job_id"], nc)
        self.assertEqual(self.gate.reads, 0)

    def test_deep_owner_eligible_model_and_successors_match_actual_dispatch(self):
        self.prepare()
        excluded = [self.nc(MODELS[0]) for _ in range(35)]
        eligible = [self.nc(MODELS[1], tier=1) for _ in range(25)]
        self.assertEqual(self.step()[0][0], eligible[0])
        data, rows = self.projected()
        self.assertEqual([r["job_id"] for r in rows], eligible[1:21])
        self.assertTrue(all(r["weight"] == 7 for r in rows))
        self.assertTrue(set(excluded).isdisjoint(r["job_id"] for r in rows))
        self.assertEqual([j for j, _ in self.step(20)], [r["job_id"] for r in rows])
        self.assertEqual(data["conditional_scenario"]["basis"], "last_owner_gate")

    def test_deep_eligible_job_in_same_raw_lane_not_hidden_by_ineligible_fifo(self):
        self.prepare()
        self.nc()
        first, second = self.nc(tier=1), self.nc(tier=1)
        self.assertEqual(self.step()[0][0], first)
        _, rows = self.projected()
        self.assertEqual([r["job_id"] for r in rows], [second])

    def test_observed_tier_exhaustion_stops_before_guessing_next_owner_tier(self):
        self.prepare()
        historical = self.nc()
        first, last = self.nc(MODELS[1], 1), self.nc(MODELS[1], 1)
        self.assertEqual(self.step()[0][0], first)
        data, rows = self.projected()
        self.assertEqual([r["job_id"] for r in rows], [last])
        self.assertEqual(data["conditional_scenario"]["stop_reason"], "owner_recheck_required")
        self.assertEqual(self.step()[0][0], last)
        self.assertEqual(self.step()[0][0], historical)

    def test_deferred_weight_is_actual_owner_weight_not_configured_seven(self):
        self.prepare()
        self.nc()
        next_job = self.nc()
        self.step()
        _, rows = self.projected()
        self.assertEqual(rows[0]["job_id"], next_job)
        self.assertEqual(rows[0]["weight"], 10)

    def test_new_upload_class_and_bounded_model_turns_preserved(self):
        self.prepare()
        for _ in range(14):
            self.nc(MODELS[0])
            self.nc(MODELS[1])
        newer = self.nc(MODELS[2], priority="new_upload")
        self.assertEqual(self.step()[0][0], newer)
        data, rows = self.projected()
        self.assertEqual([r["model"] for r in rows], [MODELS[0]] * 10 + [MODELS[1]] * 10)
        self.assertEqual([j for j, _ in self.step(20)], [r["job_id"] for r in rows])

    def test_failed_owner_read_invalidates_previous_success(self):
        self.prepare()
        self.nc()
        self.nc()
        self.step()
        self.gate.fixture = None
        self.assertFalse(self.b.dispatch_once(policy=self.p))
        data, rows = self.projected()
        self.assertEqual(data["owner_gate"]["state"], "unavailable")
        self.assertEqual(rows, [])

    def test_scope_incomplete_or_owner_blocked_never_guesses_nc_eligibility(self):
        self.prepare()
        self.nc()
        self.gate.fixture["scope_complete"] = False
        self.assertFalse(self.b.dispatch_once(policy=self.p))
        data, rows = self.projected()
        self.assertEqual(rows, [])
        self.assertTrue(data["unranked_candidates"])

    def test_stale_snapshot_has_age_without_timestamp_refresh_or_network(self):
        self.prepare()
        self.nc()
        self.nc()
        self.step()
        stamp = self.b._nextcloud_forecast_preview["observed_at"]
        self.clock.advance(100)
        self.b._publish_scheduler_snapshot()
        data, rows = self.projected()
        self.assertTrue(rows)
        self.assertEqual(data["owner_gate"]["state"], "stale")
        self.assertEqual(data["owner_gate"]["observed_at"], stamp)
        self.assertGreaterEqual(data["owner_gate"]["age_seconds"], 100)
        self.assertEqual(self.gate.reads, 1)

    def test_policy_change_invalidates_owner_projection(self):
        self.prepare()
        self.nc()
        self.nc()
        self.step()
        self.p = self.policy({"nextcloud": {"weight": 9}, "peer": {"weight": 20}})
        data, rows = self.projected()
        self.assertEqual(data["owner_gate"]["state"], "policy_or_clock_changed")
        self.assertEqual(rows, [])

    def test_cancellation_retry_promotion_and_identity_drift_not_adopted(self):
        for change in ("state='cancelled'", "attempt_count=attempt_count+1",
                       "external_id='different'", "source_item_id='different'",
                       "nextcloud_priority='new_upload'", "queued_at=queued_at+1", "created=created+1"):
            with self.subTest(change=change):
                self.prepare()
                self.nc()
                pending = self.nc()
                self.nc()  # A current lane remains after cancelling the observed head.
                self.step()
                with self.b.db:
                    self.b.db.execute("UPDATE jobs SET " + change + " WHERE id=?", (pending,))
                data, rows = self.projected()
                self.assertEqual(data["owner_gate"]["state"], "queue_changed")
                self.assertEqual(rows, [])
                with self.b.db:
                    self.b.db.execute("DELETE FROM jobs")
                self.b._time_cycle = None
                self.b._model_batches = {}

    def test_private_jobs_never_leak_in_scenario_or_unranked_public_heads(self):
        self.prepare()
        self.nc()
        secret = self.nc(MODELS[1], private=True)
        visible = self.nc()
        self.step()
        data, rows = self.projected(public=True)
        self.assertEqual([r["job_id"] for r in rows], [visible])
        self.assertNotIn(secret, json.dumps(data))
        self.assertNotIn("secret-owner", json.dumps(data))

    def test_observer_is_readonly_payload_free_and_does_not_lock_or_call_owner(self):
        self.prepare()
        self.nc()
        self.nc()
        self.step()
        before = deepcopy(self.b._scheduler_snapshot)
        changes = self.b.db.total_changes
        calls = len(self.calls)
        ready, release = threading.Event(), threading.Event()
        def hold():
            with self.b.lock:
                ready.set()
                release.wait(3)
        thread = threading.Thread(target=hold)
        thread.start()
        try:
            self.assertTrue(ready.wait(1))
            with patch.object(self.gate, "hold", side_effect=AssertionError("owner must not be read")):
                data, rows = self.projected(public=True)
            self.assertTrue(rows)
            self.assertNotIn("synthetic fixture", json.dumps(data))
            self.assertEqual(self.b._scheduler_snapshot, before)
            self.assertEqual(self.b.db.total_changes, changes)
            self.assertEqual(len(self.calls), calls)
        finally:
            release.set()
            thread.join()

    def test_snapshot_only_published_after_commit_and_not_after_rollback(self):
        self.prepare()
        self.nc()
        self.nc()
        old = self.b._nextcloud_forecast_preview
        with self.b.lock, self.gate.hold(self.b.db), self.b._claim_transaction():
            chosen = self.b._next(policy=self.p)
            self.assertIsNotNone(chosen)
            self.assertIs(self.b._nextcloud_forecast_preview, old)
            self.assertIs(self.b._scheduler_snapshot["nextcloud_forecast"], old)
            self.assertIsNotNone(self.b._pending_forecast_preview)
        self.assertIsNot(self.b._nextcloud_forecast_preview, old)
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            with self.b.lock, self.gate.hold(self.b.db), self.b._claim_transaction():
                self.b._next(policy=self.p)
                raise RuntimeError("rollback")
        data = self.b.forecast(self.p)
        self.assertTrue(data["unavailable"])
        self.assertFalse(self.b._scheduler_snapshot["committed"])
        self.assertEqual(self.b._scheduler_snapshot["nextcloud_forecast"]["state"], "transaction_failed")

    def test_preview_capture_failure_cannot_abort_real_dispatch(self):
        self.prepare()
        self.nc()
        remaining = self.nc()
        with patch("broker.service.capture_owner_preview", side_effect=ValueError("bad preview")):
            claimed = self.step()[0][0]
        self.assertEqual(self.b.status(claimed)["state"], "completed")
        data, rows = self.projected()
        self.assertEqual(data["owner_gate"]["state"], "capture_failed")
        self.assertEqual(rows, [])
        self.assertEqual(self.b.status(remaining)["state"], "queued")

    def test_failed_claim_does_not_publish_owner_rank_or_change_queue(self):
        self.prepare()
        first = self.nc()
        self.nc()
        audit = self.b._audit
        def fail(event, **kwargs):
            if event == "scheduler.selected":
                raise RuntimeError("synthetic claim failure")
            return audit(event, **kwargs)
        with patch.object(self.b, "_audit", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "synthetic claim"):
                self.b.dispatch_once(policy=self.p)
        self.assertEqual(self.b.status(first)["state"], "queued")
        self.assertEqual(self.b.db.execute("SELECT count(*) FROM job_attempts").fetchone()[0], 0)
        data = self.b.forecast(self.p)
        self.assertTrue(data["unavailable"])
        self.assertFalse(self.b._scheduler_snapshot["committed"])
        self.assertEqual(self.b._scheduler_snapshot["nextcloud_forecast"]["state"], "transaction_failed")

    def test_mid_projection_policy_reload_cannot_mix_weights(self):
        self.prepare()
        self.submit(MODELS[0], "peer")
        self.submit(MODELS[0], "peer")
        original = self.b._time_batch_pick
        def pick(*args, **kwargs):
            self.p.set_weight("peer", 2)
            return original(*args, **kwargs)
        with patch.object(self.b, "_time_batch_pick", side_effect=pick):
            data = self.b.forecast(self.p)
        self.assertEqual([row["weight"] for row in data["next_selections"]], [20, 20])
        self.assertEqual(self.p.weight("peer"), 2)

    def test_commit_publication_failure_cannot_prevent_claim_or_execution(self):
        self.prepare()
        self.nc()
        remaining = self.nc()
        publish = self.b._publish_scheduler_snapshot
        def fail_after_commit():
            if not self.b._forecast_publication_pending:
                raise ValueError("synthetic publication failure")
            return publish()
        with patch.object(self.b, "_publish_scheduler_snapshot", side_effect=fail_after_commit):
            claimed = self.step()[0][0]
        self.assertEqual(self.b.status(claimed)["state"], "completed")
        self.assertEqual(self.b.status(remaining)["state"], "queued")
        self.assertFalse(self.b._scheduler_snapshot["committed"])
        self.assertTrue(self.b.forecast(self.p)["unavailable"])

    def test_new_memory_snapshot_cannot_be_used_with_older_wal_snapshot(self):
        self.prepare()
        for _ in range(4):
            self.nc()
        self.step()
        def old_reader(db):
            db.execute("SELECT MAX(sequence) FROM audit_events").fetchone()
            self.step()  # Writer commits while the old WAL read remains open.
            return self.b._forecast(db, self.p, self.clock(), 10)
        data, error = self.b._observer_read(old_reader)
        self.assertIsNone(data)
        self.assertIsNotNone(error)
        self.assertFalse(self.b.forecast(self.p).get("unavailable"))

    def test_old_memory_snapshot_cannot_be_used_after_new_claim_or_completion(self):
        self.prepare()
        for _ in range(4):
            self.nc()
        self.step()
        old = self.b._scheduler_snapshot
        self.step()
        current = self.b._scheduler_snapshot
        try:
            self.b._scheduler_snapshot = old
            self.assertTrue(self.b.forecast(self.p)["unavailable"])
        finally:
            self.b._scheduler_snapshot = current
        self.assertFalse(self.b.forecast(self.p).get("unavailable"))

    def test_admission_does_not_invalidate_committed_accounting_or_adopt_new_owner_job(self):
        self.prepare()
        self.nc()
        pending = self.nc()
        self.step()
        new_job = self.nc(MODELS[1], tier=1)
        data, rows = self.projected()
        self.assertEqual([r["job_id"] for r in rows], [pending])
        self.assertNotIn(new_job, [r["job_id"] for r in rows])
        self.assertTrue(data["unranked_candidates"])

    def test_completion_state_not_published_before_wal_commit(self):
        self.prepare()
        self.nc()
        self.nc()
        original = self.b._refresh_health_cache_locked
        during_completion = []
        def refresh():
            if self.b.db.execute("SELECT 1 FROM jobs WHERE state='completed' LIMIT 1").fetchone():
                self.assertTrue(self.b._scheduler_snapshot["active_time_batches"])
                data = self.b.forecast(self.p)
                self.assertFalse(data.get("unavailable"), data)
                during_completion.append(data)
            return original()
        with patch.object(self.b, "_refresh_health_cache_locked", side_effect=refresh):
            self.step()
        self.assertEqual(len(during_completion), 1)
        self.assertFalse(self.b._scheduler_snapshot["active_time_batches"])

    def test_default_policy_matches_dispatch_and_keeps_same_lane_successors(self):
        self.prepare()
        self.b.source_policy = self.p
        jobs = [self.nc() for _ in range(4)]
        self.step()
        data = self.b.forecast()
        rows = data["conditional_scenario"]["next_selections"]
        self.assertEqual([r["job_id"] for r in rows], jobs[1:])

    def test_fifo_conditional_preview_replenishes_same_lane_when_no_policy_exists(self):
        self.prepare()
        jobs = [self.nc() for _ in range(2)]
        self.submit(MODELS[1], "peer")
        self.gate.value = self.gate.fixture
        self.b._nextcloud_forecast_preview = capture_owner_preview(
            self.b._candidates(), self.gate, None, self.clock(), None)
        self.gate.value = None
        self.b._publish_scheduler_snapshot()
        data = self.b.forecast()
        self.assertEqual([r["job_id"] for r in data["conditional_scenario"]["next_selections"]],
                         jobs)

    def test_claim_rollback_then_noop_cannot_spend_a_model_turn_or_source_budget(self):
        self.prepare()
        for _ in range(12):
            self.nc(MODELS[0])
            self.nc(MODELS[1])
        self.step(9)
        before = deepcopy((self.b._time_cycle, self.b._model_batches,
            self.b._active_time_batches, self.b._duration_estimates,
            self.b._last_scheduler_decision))
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            with self.b.lock, self.gate.hold(self.b.db), self.b._claim_transaction():
                self.b._next(policy=self.p)  # Would be claim ten, but never committed.
                raise RuntimeError("rollback")
        after = (self.b._time_cycle, self.b._model_batches,
            self.b._active_time_batches, self.b._duration_estimates,
            self.b._last_scheduler_decision)
        self.assertEqual(before, after)
        with self.b.lock, self.b._claim_transaction():
            pass
        self.assertTrue(self.b._scheduler_snapshot["committed"])
        self.b.nextcloud_priority = None
        forecast = self.b.forecast(self.p, 2)["next_selections"]
        self.b.nextcloud_priority = self.gate
        actual = self.step(2)
        self.assertEqual([j for j, _ in actual], [r["job_id"] for r in forecast])
        self.assertEqual([d["selected_model"] for _, d in actual], [MODELS[0], MODELS[1]])

    def test_completion_rollback_restores_active_charge_and_duration_estimate(self):
        self.prepare()
        self.nc()
        self.nc()
        self.step()
        self.b._active_time_batches["owned"] = {
            "lane": ("nextcloud", MODELS[0]), "started": self.clock()}
        before = deepcopy((self.b._time_cycle, self.b._model_batches,
            self.b._active_time_batches, self.b._duration_estimates))
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            with self.b.lock, self.b._claim_transaction():
                self.b._complete_time_batch("owned", self.clock()+30)
                raise RuntimeError("rollback")
        self.assertEqual(before, (self.b._time_cycle, self.b._model_batches,
            self.b._active_time_batches, self.b._duration_estimates))

    def test_aggregate_preview_cap_refuses_incomplete_lane_set(self):
        self.prepare()
        row = {"id": "", "source": "nextcloud", "profile": "interactive",
               "target_model": "", "nextcloud_priority": "historical", "external_id": ""}
        rows = []
        for i in range(MAX_OWNER_PREVIEW_ROWS + 1):
            rows.append(dict(row, id=str(i), target_model=str(i), external_id=str(i)))
            self.gate.fixture["jobs"][str(i)] = 3
        self.gate.value = self.gate.fixture
        preview = capture_owner_preview(rows, self.gate, (), 1000, None)
        self.assertEqual(preview["state"], "preview_limit")
        self.assertEqual(preview["rows"], ())

    def test_paused_and_fenced_queue_reports_holds_without_mutating_them(self):
        self.prepare()
        self.nc()
        self.nc()
        self.step()
        self.b.set_queue_paused(True)
        with self.b.db:
            self.b.db.execute("INSERT INTO gpu_executor_fences VALUES('mainpc-gpu','secret-request','secret-job')")
        before = self.b.db.total_changes
        data, rows = self.projected()
        self.assertTrue(rows)
        self.assertEqual(len(data["dispatch_holds"]), 2)
        self.assertNotIn("secret-request", json.dumps(data))
        self.assertEqual(self.b.db.total_changes, before)
        self.assertTrue(self.b.queue_paused())

    def test_public_http_and_dashboard_share_conditional_contract(self):
        self.prepare()
        self.nc()
        self.nc()
        self.step()
        server = serve(self.b, port=0, policy=self.p)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urlopen(f"http://127.0.0.1:{server.server_port}/v1/forecast") as response:
                api = json.load(response)
            self.assertTrue(api["conditional_scenario"]["next_selections"])
            self.assertEqual(api, self.b.dashboard(self.p, include_producer_storage=False)["forecast"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_forecast_cycle_rebuild_never_reads_mutable_dispatcher_state(self):
        self.prepare()
        self.submit(MODELS[0], "peer")
        self.b._publish_scheduler_snapshot()
        class Forbidden(dict):
            def values(self):
                raise AssertionError("forecast read mutable active batches")
            def get(self, *args):
                raise AssertionError("forecast read mutable cycle")
        self.b._active_time_batches = Forbidden()
        self.b._time_cycle = Forbidden()
        result = self.b.forecast(self.p)
        self.assertFalse(result.get("unavailable"))
        self.assertTrue(result["next_selections"])


if __name__ == "__main__":
    unittest.main()
