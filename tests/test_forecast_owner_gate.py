"""Queue-only forecast: SQLite snapshots, transactional accounting and privacy."""
from copy import deepcopy
import json
import sqlite3
import threading
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen

from broker.http import serve
import test_source_model_batches as fixtures

MODELS = fixtures.MODELS

class QueueForecastTests(unittest.TestCase):
    setUp = fixtures.SourceModelBatchTests.setUp
    open = fixtures.SourceModelBatchTests.open
    policy = fixtures.SourceModelBatchTests.policy
    submit = fixtures.SourceModelBatchTests.submit
    step = fixtures.SourceModelBatchTests.step

    def prepare(self):
        self.p = self.policy({"nextcloud": {"weight": 7}, "peer": {"weight": 20}})

    def nc(self, model=MODELS[0], priority="historical", private=False):
        job = self.submit(model)
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET source='nextcloud',external_id=id,"
                              "nextcloud_priority=?,producer_attempt_id=? WHERE id=?",
                              (priority, "secret-owner" if private else None, job))
        self.b._duration_estimates[("nextcloud", model)] = 20
        self.b._publish_scheduler_snapshot()
        return job

    def projected(self, count=20, public=False):
        data = self.b.forecast(self.p, count, include_producer_storage=not public)
        self.assertFalse(data.get("unavailable"), data)
        return data, data["conditional_scenario"]["next_selections"]

    def test_deep_private_prefix_does_not_hide_public_nextcloud_lane(self):
        self.prepare()
        private = [self.nc(private=True) for _ in range(70)]
        visible = [self.nc() for _ in range(6)]
        public, rows = self.projected(public=True)
        self.assertEqual([row["job_id"] for row in rows], visible)
        self.assertEqual(public["conditional_scenario"]["basis"], "all_eligible_sources")
        self.assertEqual(public["next_selections"], rows)
        self.assertNotIn("secret-owner", json.dumps(public))
        self.assertTrue(all(job not in json.dumps(public) for job in private))
        self.assertIn("excludes private jobs", public["contingency"])
        self.assertEqual(self.step()[0][0], private[0])

    def test_visibility_changes_follow_one_consistent_wal_snapshot(self):
        self.prepare()
        visible = self.nc()
        private = self.nc(private=True)

        def old_reader(db):
            before = self.b._forecast(db, self.p, self.clock(), 10,
                                      include_producer_storage=False)
            with sqlite3.connect(self.path) as other:
                other.execute("UPDATE jobs SET producer_attempt_id='private' WHERE id=?", (visible,))
                other.execute("UPDATE jobs SET producer_attempt_id=NULL WHERE id=?", (private,))
            after = self.b._forecast(db, self.p, self.clock(), 10,
                                     include_producer_storage=False)
            self.assertEqual(before, after)
            self.assertEqual([r["job_id"] for r in after["next_selections"]], [visible])
            return after

        data, error = self.b._observer_read(old_reader)
        self.assertIsNone(error)
        self.assertIsNotNone(data)
        _, fresh = self.projected(public=True)
        self.assertEqual([r["job_id"] for r in fresh], [private])

    def test_dispatch_and_forecast_ignore_injected_legacy_gate(self):
        self.prepare()
        class ForbiddenGate:
            def hold(self, *_):
                raise AssertionError("external gate must never be consulted")
            def candidates(self, *_):
                raise AssertionError("external filtering must never be consulted")
        self.b.nextcloud_priority = ForbiddenGate()
        jobs = [self.nc(), self.nc(MODELS[1])]
        self.submit(MODELS[2], "peer")
        data, rows = self.projected()
        self.assertEqual(data["next_selections"], rows)
        self.assertEqual([r["weight"] for r in rows if r["source"] == "nextcloud"], [7, 7])
        self.assertEqual([job for job, _ in self.step(len(rows))], [r["job_id"] for r in rows])
        self.assertTrue(all(self.b.status(job)["state"] == "completed" for job in jobs))

    def test_fifo_forecast_does_not_promote_new_upload(self):
        self.prepare()
        historical = self.nc()
        newer = self.nc(priority="new_upload")
        forecast = self.b.forecast()
        self.assertEqual([r["job_id"] for r in forecast["next_selections"]], [historical, newer])
        self.assertTrue(self.b.dispatch_once())
        self.assertEqual(self.b.status(historical)["state"], "completed")

    def test_legacy_constructor_uses_local_policy_without_calling_external_adapter(self):
        from broker.service import Broker
        policy = self.policy({"nextcloud": {"weight": 7},
                              "peer": {"weight": 1, "enabled": False}})
        class ForbiddenGate:
            def hold(self, *_):
                raise AssertionError("external gate must not be consulted")
        other = Broker(self.path, self.ollama, fixtures.FakeWol(self.calls),
                       clock=self.clock, source_policy=policy, nextcloud_priority=ForbiddenGate())
        self.addCleanup(other.db.close)
        historical = self.nc()
        upload = self.nc(priority="new_upload")
        peer = self.submit(MODELS[0], "peer")
        data = other.forecast()
        self.assertEqual([r["job_id"] for r in data["next_selections"]], [upload, historical])
        self.assertTrue(other.dispatch_once())
        self.assertEqual(other.status(upload)["state"], "completed")
        self.assertEqual(other.status(peer)["state"], "queued")

    def test_normal_constructor_uses_source_policy_by_default(self):
        from broker.service import Broker
        policy = self.policy({"nextcloud": {"weight": 7},
                              "peer": {"weight": 1, "enabled": False}})
        other = Broker(self.path, self.ollama, fixtures.FakeWol(self.calls),
                       clock=self.clock, source_policy=policy)
        self.addCleanup(other.db.close)
        historical = self.nc()
        upload = self.nc(priority="new_upload")
        peer = self.submit(MODELS[0], "peer")
        forecast = other.forecast()
        self.assertEqual([r["job_id"] for r in forecast["next_selections"]], [upload, historical])
        self.assertEqual(other.dashboard()["forecast"], forecast)
        self.assertTrue(other.dispatch_once())
        self.assertEqual(other.status(upload)["state"], "completed")
        self.assertEqual(other.status(peer)["state"], "queued")

    def test_explicit_allowlist_cannot_bypass_disabled_source(self):
        self.prepare()
        self.p = self.policy({"nextcloud": {"weight": 7},
                              "peer": {"weight": 1, "enabled": False}})
        peer = self.submit(MODELS[0], "peer")
        allowed = frozenset({"peer"})
        self.assertIsNone(self.b._next(allowed, self.p))
        self.assertFalse(self.b.dispatch_once(allowed, self.p))
        self.assertEqual(self.b.status(peer)["state"], "queued")
        self.assertEqual(self.b.db.execute("SELECT count(*) FROM job_attempts").fetchone()[0], 0)
        self.assertTrue(self.b.dispatch_once(allowed))  # No configured policy: legacy FIFO.

    def test_allowlist_rechecks_policy_immediately_before_claim(self):
        self.prepare()
        peer = self.submit(MODELS[0], "peer")
        original = self.b.reconcile_executor_fence
        def disable_at_probe():
            self.p.set_enabled("peer", False)
            return original()
        with patch.object(self.b, "reconcile_executor_fence", side_effect=disable_at_probe):
            self.assertFalse(self.b.dispatch_once(frozenset({"peer"}), self.p))
        self.assertEqual(self.b.status(peer)["state"], "queued")

    def test_private_canary_claim_cannot_change_public_cooldown_forecast(self):
        self.p = self.policy({"shutterstock-canary": {"weight": 7}})
        payload = {"prompt": "synthetic", "images": ["aGVsbG8="],
                   "format": {"type": "object"}}
        private = self.b.submit("shutterstock-canary", "generate", payload)["id"]
        self.clock.advance(.01)
        public = self.b.submit("shutterstock-canary", "generate", payload)["id"]
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET producer_attempt_id='private' WHERE id=?", (private,))
        before = self.b.forecast(self.p, include_producer_storage=False)
        self.assertTrue(self.b.dispatch_once(policy=self.p))
        self.assertEqual(self.b.status(private)["state"], "completed")
        after = self.b.forecast(self.p, include_producer_storage=False)
        self.assertEqual([row["job_id"] for row in before["next_selections"]], [public])
        self.assertEqual([row["job_id"] for row in after["next_selections"]], [public])
        self.assertEqual(self.b.forecast(self.p)["next_selections"], [])
        self.assertFalse(self.b.dispatch_once(policy=self.p))

    def test_public_hypothetical_ignores_private_source_cooldown(self):
        self.prepare()
        private = self.nc(private=True)
        public = self.nc()
        for policy in (None, self.p):
            before = self.b.forecast(policy, include_producer_storage=False)
            with self.b.db:
                self.b.db.execute("INSERT OR REPLACE INTO source_schedules(source,next_allowed) VALUES(?,?)",
                                  ("nextcloud", self.clock() + 60))
            after = self.b.forecast(policy, include_producer_storage=False)
            self.assertEqual([r["job_id"] for r in before["next_selections"]], [public])
            self.assertEqual([r["job_id"] for r in after["next_selections"]], [public])
            self.assertEqual(before["conditional_scenario"], after["conditional_scenario"])
            self.assertIn("cooldowns", after["contingency"])
            full = self.b.forecast(policy)
            self.assertEqual(full["next_selections"], [])
            with self.b.db:
                self.b.db.execute("DELETE FROM source_schedules WHERE source='nextcloud'")
        self.assertEqual(self.b.status(private)["state"], "queued")

    def test_public_forecast_does_not_adopt_private_model_turns_or_budget(self):
        self.prepare()
        private = self.nc(MODELS[1], private=True)
        visible = [self.nc(MODELS[0]), self.nc(MODELS[1])]
        before, rows = self.projected(public=True)
        self.assertEqual([r["job_id"] for r in rows], visible)
        self.assertEqual(self.step()[0][0], private)
        after, rows = self.projected(public=True)
        self.assertEqual([r["job_id"] for r in rows], visible)
        self.assertEqual([r["model"] for r in before["next_selections"]],
                         [r["model"] for r in after["next_selections"]])
        self.assertIn("fresh scheduler accounting", after["contingency"])
        full, _ = self.projected()
        self.assertEqual(full["next_selections"][0]["job_id"], visible[1])

    def test_public_projection_does_not_depend_on_global_snapshot_validity(self):
        self.prepare()
        visible = self.nc()
        self.b._scheduler_snapshot = {"committed": False}
        public, rows = self.projected(public=True)
        self.assertEqual([row["job_id"] for row in rows], [visible])
        self.assertTrue(self.b.forecast(self.p)["unavailable"])

    def test_public_forecast_ignores_private_running_for_concurrency(self):
        self.prepare()
        private = self.nc(private=True)
        visible = [self.nc(), self.nc()]
        for policy in (None, self.p):
            before = self.b.forecast(policy, include_producer_storage=False)
            with self.b.db:
                self.b.db.execute("UPDATE jobs SET state='running' WHERE id=?", (private,))
            during = self.b.forecast(policy, include_producer_storage=False)
            self.assertEqual([r["job_id"] for r in before["next_selections"]], visible)
            self.assertEqual([r["job_id"] for r in during["next_selections"]], visible)
            with self.b.db:
                self.b.db.execute("UPDATE jobs SET state='queued' WHERE id=?", (private,))

    def test_public_forecast_ignores_private_delayed_source(self):
        self.prepare()
        visible = [self.nc(), self.nc()]
        private = self.submit(MODELS[1], "peer")
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET producer_attempt_id='secret' WHERE id=?", (private,))
            self.b.db.execute("INSERT INTO source_schedules(source,next_allowed) VALUES(?,?)",
                              ("peer", self.clock() + 5))
        for policy in (None, self.p):
            public = self.b.forecast(policy, include_producer_storage=False)
            self.assertEqual([r["job_id"] for r in public["next_selections"]], visible)
            self.assertIsNone(public["conditional_scenario"]["stop_reason"])
            private_view = self.b.forecast(policy)
            self.assertEqual(len(private_view["next_selections"]), 1)
            self.assertEqual(private_view["conditional_scenario"]["stop_reason"],
                             "source_schedule_recheck_required")

    def test_failed_claim_rolls_back_queue_and_forecast_is_unavailable(self):
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
        self.assertTrue(self.b.forecast(self.p)["unavailable"])

    def test_public_http_excludes_private_prefix_and_authorized_view_includes_it(self):
        self.prepare()
        private = [self.nc(private=True) for _ in range(25)]
        visible = self.nc()
        token = "synthetic-token-not-a-secret"
        server = serve(self.b, port=0, policy=self.p, storage_token=token)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_port}/v1/forecast"
        with urlopen(url, timeout=5) as response:
            public = json.load(response)
        self.assertEqual([r["job_id"] for r in public["next_selections"]], [visible])
        self.assertNotIn(private[0], json.dumps(public))
        with urlopen(Request(url, headers={"Authorization": "Bearer " + token}), timeout=5) as response:
            authorized = json.load(response)
        self.assertEqual(authorized["next_selections"][0]["job_id"], private[0])

    def test_noop_recover_does_not_take_writer_lock_or_change_snapshot(self):
        self.prepare()
        blocker = sqlite3.connect(self.path, timeout=0)
        self.b.db.execute("PRAGMA busy_timeout=30")
        try:
            blocker.execute("BEGIN IMMEDIATE")
            self.b.recover()
            self.assertTrue(self.b._scheduler_snapshot["committed"])
        finally:
            blocker.rollback()
            blocker.close()

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
            data, rows = self.projected(public=True)
            self.assertTrue(rows)
            self.assertNotIn("synthetic fixture", json.dumps(data))
            self.assertEqual(self.b._scheduler_snapshot, before)
            self.assertEqual(self.b.db.total_changes, changes)
            self.assertEqual(len(self.calls), calls)
        finally:
            release.set()
            thread.join()

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

    def test_public_contingency_mentions_private_competitors(self):
        self.prepare()
        self.nc()
        data, _ = self.projected(public=True)
        self.assertIn("excludes private jobs", data["contingency"])

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
            with self.b.lock, self.b._claim_transaction():
                self.b._next(policy=self.p)  # Would be claim ten, but never committed.
                raise RuntimeError("rollback")
        after = (self.b._time_cycle, self.b._model_batches,
            self.b._active_time_batches, self.b._duration_estimates,
            self.b._last_scheduler_decision)
        self.assertEqual(before, after)
        with self.b.lock, self.b._claim_transaction():
            pass
        self.assertTrue(self.b._scheduler_snapshot["committed"])
        forecast = self.b.forecast(self.p, 2)["next_selections"]
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
