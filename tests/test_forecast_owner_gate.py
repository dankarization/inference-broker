"""Offline owner-gate forecasts: real SQLite/claim path, fake owner and executor."""
from contextlib import contextmanager
from copy import deepcopy
import json
import sqlite3
import threading
import time
import unittest
from unittest.mock import patch
from urllib.request import urlopen

from broker.forecast import capture_owner_preview, MAX_OWNER_PREVIEW_ROWS, visibility_revision
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

    def test_no_owner_gate_does_not_install_revision_or_change_schema(self):
        self.assertFalse(self.b._forecast_tracker_ready)
        self.assertIsNone(self.b.db.execute("SELECT 1 FROM audit_events WHERE sequence=0").fetchone())
        self.assertEqual(self.b.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'forecast_visibility_%'").fetchone()[0], 0)

    def test_configured_gate_installs_tracker_idempotently(self):
        from broker.service import Broker
        policy = self.policy({"nextcloud": {"weight": 7}})
        gated = Broker(self.path, self.ollama, fixtures.FakeWol(self.calls),
                       clock=self.clock, source_policy=policy, nextcloud_priority=Gate())
        self.addCleanup(gated.db.close)
        self.assertTrue(gated._forecast_tracker_ready)
        self.assertIsNotNone(gated.db.execute("SELECT 1 FROM audit_events WHERE sequence=0").fetchone())
        with gated.db:
            gated._setup_forecast_visibility()
        self.assertTrue(gated._forecast_tracker_ready)

    def prepare(self):
        self.gate = Gate()
        self.b.nextcloud_priority = self.gate
        if not self.b._forecast_tracker_ready:
            with self.b.db:
                self.b._setup_forecast_visibility()
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

    def test_actual_owner_shape_incomplete_scope_floor_one_blocks_tier_two(self):
        self.prepare()
        queued = [self.nc(tier=2) for _ in range(8)]
        self.gate.fixture.update(floor=1, scope_complete=False)
        peer = self.submit(MODELS[1], "peer")
        self.assertEqual(self.step()[0][0], peer)
        data, rows = self.projected()
        self.assertFalse(any(r["job_id"] in queued for r in rows))
        self.assertEqual(data["owner_gate"]["state"], "observed_tier_exhausted")
        self.assertTrue(all(self.b.status(job)["state"] == "queued" for job in queued))

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

    def test_private_prefix_cannot_hide_owner_eligible_public_lane(self):
        self.prepare()
        private = [self.nc(tier=1, private=True) for _ in range(25)]
        visible = [self.nc(tier=1) for _ in range(6)]
        excluded = self.nc(tier=3)
        self.assertEqual(self.step()[0][0], private[0])
        preview = self.b._nextcloud_forecast_preview
        self.assertEqual(len(preview["rows"]), 20)
        self.assertEqual([r["id"] for r in preview["public_rows"]], visible)
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "observed")
        self.assertEqual([r["job_id"] for r in rows], visible)
        self.assertEqual(data["conditional_scenario"]["basis"], "last_owner_gate")
        self.assertNotIn(excluded, json.dumps(rows))
        self.assertTrue(all(job not in json.dumps(data) for job in private))
        self.assertNotIn("secret-owner", json.dumps(data))

    def test_public_preview_respects_visibility_mutation_and_owner_invalidation(self):
        self.prepare()
        for _ in range(22):
            self.nc(tier=1, private=True)
        visible = self.nc(tier=1)
        self.nc(tier=1)
        self.step()
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET producer_attempt_id='new-private' WHERE id=?", (visible,))
        data, rows = self.projected(public=True)
        self.assertNotIn(visible, json.dumps(data))
        self.assertEqual(data["owner_gate"]["state"], "public_preview_changed")
        self.assertEqual(rows, [])
        self.gate.fixture = None
        self.assertFalse(self.b.dispatch_once(policy=self.p))
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "unavailable")
        self.assertEqual(rows, [])

    def test_public_preview_visibility_loss_does_not_claim_tier_exhausted(self):
        self.prepare()
        for _ in range(22):
            self.nc(tier=1, private=True)
        visible = self.nc(tier=1)
        self.nc(tier=3)
        self.step()
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET producer_attempt_id='new-private' WHERE id=?", (visible,))
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_changed")
        self.assertEqual(rows, [])
        self.assertNotIn(visible, json.dumps(data))

    def test_legacy_private_only_preview_does_not_claim_tier_exhausted(self):
        self.prepare()
        self.nc(tier=1, private=True)
        self.nc(tier=1, private=True)
        visible = self.nc(tier=1)
        self.gate.value = self.gate.fixture
        self.b._nextcloud_forecast_preview = capture_owner_preview(
            self.b._candidates(self.p.enabled_sources())[:2], self.gate,
            self.b._policy_signature(self.p), self.clock(), None)
        self.gate.value = None
        self.b._publish_scheduler_snapshot()
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_unavailable")
        self.assertEqual(rows, [])
        self.assertEqual(data["unranked_candidates"][0]["job_id"], visible)

    def test_public_preview_all_private_reports_no_visible_observed_rows(self):
        self.prepare()
        for _ in range(22):
            self.nc(tier=1, private=True)
        self.nc(tier=3)  # Visible, but not in the selected tier.
        self.step()
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "observed_tier_exhausted")
        self.assertEqual(rows, [])

    def test_public_preview_bounds_each_lane_even_with_private_prefix(self):
        self.prepare()
        for _ in range(22):
            self.nc(tier=1, private=True)
        visible = [self.nc(tier=1) for _ in range(25)]
        self.step()
        preview = self.b._nextcloud_forecast_preview
        self.assertEqual(len(preview["public_rows"]), 20)
        data, rows = self.projected(public=True)
        self.assertEqual([r["job_id"] for r in rows], visible[:20])
        self.assertEqual(data["owner_gate"]["state"], "observed")

    def test_legacy_partly_visible_preview_does_not_rank_incomplete_competitors(self):
        self.prepare()
        private = self.nc(MODELS[0], tier=1, private=True)
        visible = self.nc(MODELS[1], tier=1)
        self.gate.value = self.gate.fixture
        self.b._nextcloud_forecast_preview = capture_owner_preview(
            self.b._candidates(self.p.enabled_sources()), self.gate,
            self.b._policy_signature(self.p), self.clock(), None)
        self.gate.value = None
        self.b._publish_scheduler_snapshot()
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_unavailable")
        self.assertEqual(rows, [])
        self.assertNotIn(private, json.dumps(data))
        self.assertIn(visible, json.dumps(data))

    def test_public_preview_total_limit_fails_closed(self):
        self.prepare()
        rows = []
        for i in range(4):
            job = self.nc(model=MODELS[i % 3], tier=1)
            row = dict(next(r for r in self.b._candidates() if r["id"] == job))
            row["target_model"] = str(i)  # Synthetic distinct lanes for the bound.
            rows.append(row)
        self.gate.value = self.gate.fixture
        with patch("broker.forecast.MAX_OWNER_PREVIEW_ROWS", 2):
            preview = capture_owner_preview(rows, self.gate, (), 1000, None,
                db=self.b.db, public_predicate="producer_attempt_id IS NULL")
        self.assertEqual(preview["state"], "preview_limit")
        self.assertEqual(preview["rows"], ())

    def test_private_to_public_visibility_change_invalidates_old_observation(self):
        self.prepare()
        private = [self.nc(tier=1, private=True) for _ in range(22)]
        self.nc(tier=1)
        self.step()
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET producer_attempt_id=NULL WHERE id=?", (private[1],))
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_changed")
        self.assertEqual(rows, [])

    def test_deep_private_prefix_is_bounded_and_never_delays_claim(self):
        self.prepare()
        private = [self.nc(tier=1, private=True) for _ in range(60)]
        self.nc(tier=1)
        with patch("broker.forecast.MAX_PUBLIC_VISIBILITY_PROBES", 10):
            self.assertEqual(self.step()[0][0], private[0])
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_limit")
        self.assertEqual(rows, [])

    def test_visibility_probe_error_never_prevents_real_claim(self):
        self.prepare()
        first = self.nc(tier=1)
        self.nc(tier=1)
        with patch("broker.forecast.is_public_preview_job", side_effect=sqlite3.OperationalError("fixture")):
            self.assertEqual(self.step()[0][0], first)
        self.assertEqual(self.b.status(first)["state"], "completed")
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "capture_failed")
        self.assertEqual(rows, [])

    def test_probe_budget_bounds_private_backlog_without_public_jobs(self):
        self.prepare()
        private = [self.nc(tier=1, private=True) for _ in range(80)]
        self.nc(tier=3)
        from broker.forecast import is_public_preview_job
        started = time.monotonic()
        with patch("broker.forecast.MAX_PUBLIC_VISIBILITY_PROBES", 10), \
             patch("broker.forecast.is_public_preview_job", wraps=is_public_preview_job) as probe:
            self.assertEqual(self.step()[0][0], private[0])
        self.assertEqual(probe.call_count, 10)
        self.assertLess(time.monotonic() - started, 2.0)
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_limit")
        self.assertEqual(rows, [])
        self.assertTrue(all(job not in json.dumps(data) for job in private))

    def test_revision_detects_multiple_commits_without_audit_between_claim_and_finish(self):
        self.prepare()
        self.nc(tier=1, private=True)
        earlier = self.nc(tier=1, private=True)
        visible = self.nc(tier=1)
        execute = self.b._execute
        def mutate_then_execute(row):
            other = sqlite3.connect(self.path)
            try:
                with other:
                    other.execute("UPDATE jobs SET producer_attempt_id=NULL WHERE id=?", (earlier,))
                with other:
                    other.execute("UPDATE jobs SET producer_attempt_id='private' WHERE id=?", (visible,))
            finally:
                other.close()
            execute(row)
        with patch.object(self.b, "_execute", side_effect=mutate_then_execute):
            self.step()
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_changed")
        self.assertEqual(rows, [])
        self.assertNotIn(visible, json.dumps(data))
        self.assertEqual(self.step()[0][0], earlier)

    def test_change_during_candidate_capture_cannot_be_adopted_as_snapshot(self):
        self.prepare()
        first = self.nc(tier=1)
        private = self.nc(tier=1, private=True)
        self.nc(tier=1)
        candidates = self.b._candidates
        mutated = False
        def mutate_after_read(*args, **kwargs):
            nonlocal mutated
            result = candidates(*args, **kwargs)
            if not mutated and kwargs.get("db") is None:
                mutated = True
                other = sqlite3.connect(self.path)
                try:
                    with other:
                        other.execute("UPDATE jobs SET producer_attempt_id=NULL WHERE id=?", (private,))
                finally:
                    other.close()
            return result
        with patch.object(self.b, "_candidates", side_effect=mutate_after_read):
            self.assertEqual(self.step()[0][0], first)
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_unavailable")
        self.assertEqual(rows, [])

    def test_old_wal_snapshot_does_not_adopt_later_preview_revision(self):
        self.prepare()
        self.nc(tier=1, private=True)
        private = self.nc(tier=1, private=True)
        visible = self.nc(tier=1)
        self.step()
        def old_reader(db):
            db.execute("SELECT MAX(sequence) FROM audit_events").fetchone()
            other = sqlite3.connect(self.path)
            try:
                with other:
                    other.execute("UPDATE jobs SET producer_attempt_id=NULL WHERE id=?", (private,))
            finally:
                other.close()
            return self.b._forecast(db, self.p, self.clock(), 20, include_producer_storage=False)
        data, error = self.b._observer_read(old_reader)
        self.assertIsNone(error)
        self.assertEqual([r["job_id"] for r in data["conditional_scenario"]["next_selections"]], [visible])
        current, rows = self.projected(public=True)
        self.assertEqual(current["owner_gate"]["state"], "public_preview_changed")
        self.assertEqual(rows, [])

    def test_dropped_visibility_trigger_invalidates_forecast_without_breaking_claim(self):
        self.prepare()
        self.nc(tier=1)
        self.nc(tier=1)
        self.nc(tier=1)
        self.step()
        with self.b.db:
            self.b.db.execute("DROP TRIGGER forecast_visibility_update")
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_unavailable")
        self.assertEqual(rows, [])
        self.assertTrue(self.b.dispatch_once(policy=self.p))

    def test_promoting_uncaptured_deep_job_invalidates_both_views(self):
        self.prepare()
        jobs = [self.nc(tier=1) for _ in range(24)]
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET source_item_id='item' WHERE id=?", (jobs[-1],))
        self.step()
        self.b.promote_nextcloud(jobs[-1], {"source": "nextcloud", "external_id": jobs[-1],
            "source_item_id": "item", "nextcloud_priority": "new_upload"})
        for public in (False, True):
            data, rows = self.projected(public=public)
            self.assertEqual(data["owner_gate"]["state"],
                             "public_preview_changed" if public else "queue_changed")
            self.assertEqual(rows, [])
        self.assertEqual(self.step()[0][0], jobs[-1])

    def test_malformed_tracker_never_aborts_claim_or_producer_admission(self):
        self.prepare()
        first = self.nc(tier=1)
        self.nc(tier=1)
        with self.b.db:
            self.b.db.execute("UPDATE audit_events SET metadata_json='not-json' WHERE sequence=0")
        self.assertEqual(self.step()[0][0], first)
        self.assertEqual(self.b.status(first)["state"], "completed")
        self.nc(tier=1)
        private_data, private_rows = self.projected()
        self.assertEqual(private_rows, [])
        self.assertIn(private_data["owner_gate"]["state"], ("queue_changed", "capture_failed"))
        data, rows = self.projected(public=True)
        self.assertEqual(rows, [])
        self.assertIn(data["owner_gate"]["state"], ("public_preview_unavailable", "capture_failed"))
        marker = self.b.db.execute("SELECT metadata_json FROM audit_events WHERE sequence=0").fetchone()[0]
        self.assertEqual(json.loads(marker), {"invalid": True})

    def test_deeply_nested_tracker_never_aborts_forecast_claim_or_restart(self):
        from broker.service import Broker
        self.prepare()
        first = self.nc(tier=1)
        self.nc(tier=1)
        self.nc(tier=1)
        self.step()
        with self.b.db:
            self.b.db.execute("UPDATE audit_events SET metadata_json=? WHERE sequence=0",
                              ("[" * 5000 + "]" * 5000,))
        gated = Broker(self.path, self.ollama, fixtures.FakeWol(self.calls),
                       clock=self.clock, source_policy=self.p, nextcloud_priority=self.gate)
        self.addCleanup(gated.db.close)
        self.assertFalse(gated._forecast_tracker_ready)
        for public in (False, True):
            data, rows = self.projected(public=public)
            self.assertEqual(rows, [])
            self.assertIn(data["owner_gate"]["state"],
                          ("queue_changed", "public_preview_changed", "public_preview_unavailable"))
        self.assertTrue(self.b.dispatch_once(policy=self.p))
        self.assertEqual(self.b.status(first)["state"], "completed")

    def test_duplicate_revision_keys_cannot_create_aba_observation(self):
        self.prepare()
        self.nc(tier=1, private=True)
        earlier = self.nc(tier=1, private=True)
        visible = self.nc(tier=1)
        self.step()
        revision = visibility_revision(self.b.db)
        with self.b.db:
            self.b.db.execute("UPDATE audit_events SET metadata_json=? WHERE sequence=0",
                              ('{"revision":%d,"revision":%d}' % (revision-1, revision),))
            self.b.db.execute("UPDATE jobs SET producer_attempt_id=NULL WHERE id=?", (earlier,))
        for public in (False, True):
            data, rows = self.projected(public=public)
            self.assertEqual(rows, [])
            self.assertIn(data["owner_gate"]["state"], ("queue_changed", "public_preview_changed"))
        self.assertEqual(self.step()[0][0], earlier)
        self.assertEqual(self.b.status(visible)["state"], "queued")

    def test_failed_tracker_initialization_is_nonfatal_and_fail_closed(self):
        self.prepare()
        self.nc(tier=1)
        self.nc(tier=1)
        with self.b.db:
            self.b.db.execute("UPDATE audit_events SET event_type='occupied' WHERE sequence=0")
        other = self.open()
        with other.db:
            other._setup_forecast_visibility()
        self.assertFalse(other._forecast_tracker_ready)
        self.b = other
        self.b.nextcloud_priority = self.gate
        self.assertTrue(self.b.dispatch_once(policy=self.p))
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_unavailable")
        self.assertEqual(rows, [])

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

    def test_revision_is_atomic_with_queue_changes_and_survives_broker_restart(self):
        from broker.forecast import visibility_revision
        self.prepare()
        job = self.nc(tier=1, private=True)
        initial = visibility_revision(self.b.db)
        with self.assertRaisesRegex(RuntimeError, "abort"):
            with self.b.db:
                self.b.db.execute("UPDATE jobs SET producer_attempt_id=NULL WHERE id=?", (job,))
                raise RuntimeError("abort")
        self.assertEqual(visibility_revision(self.b.db), initial)
        with self.b.db:
            self.b.db.execute("UPDATE jobs SET producer_attempt_id=NULL WHERE id=?", (job,))
        self.assertEqual(visibility_revision(self.b.db), initial + 1)
        other = self.open()
        with other.db:
            other._setup_forecast_visibility()
        self.assertTrue(other._forecast_tracker_ready)
        self.assertEqual(visibility_revision(other.db), initial + 1)
        self.assertEqual(other.db.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_tracker_schema_and_marker_absence_fail_closed(self):
        self.prepare()
        self.nc(tier=1)
        self.nc(tier=1)
        self.step()
        with self.b.db:
            self.b.db.execute("DELETE FROM audit_events WHERE sequence=0")
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_changed")
        self.assertEqual(rows, [])
        self.assertTrue(self.b.dispatch_once(policy=self.p))

    def test_marker_loss_mutation_and_stale_restore_cannot_forge_revision(self):
        self.prepare()
        self.nc(tier=1, private=True)
        earlier = self.nc(tier=1, private=True)
        visible = self.nc(tier=1)
        self.step()
        saved = self.b.db.execute("SELECT * FROM audit_events WHERE sequence=0").fetchone()
        columns = [r[1] for r in self.b.db.execute("PRAGMA table_info(audit_events)")]
        with self.b.db:
            self.b.db.execute("DELETE FROM audit_events WHERE sequence=0")
            self.b.db.execute("UPDATE jobs SET producer_attempt_id=NULL WHERE id=?", (earlier,))
            # A normal restore attempt must not replace the invalid sentinel.
            self.b.db.execute("INSERT OR IGNORE INTO audit_events(" + ",".join(columns) +
                              ") VALUES(" + ",".join("?" for _ in columns) + ")", tuple(saved))
        self.assertIsNone(visibility_revision(self.b.db))
        for public in (False, True):
            data, rows = self.projected(public=public)
            self.assertEqual(rows, [])
            self.assertIn(data["owner_gate"]["state"], ("queue_changed", "public_preview_changed"))
        self.assertEqual(self.step()[0][0], earlier)
        self.assertEqual(self.b.status(visible)["state"], "queued")

    def test_counter_before_and_after_claim_never_adopts_multiple_commits(self):
        self.prepare()
        first = self.nc(tier=1)
        private = self.nc(tier=1, private=True)
        self.nc(tier=1)
        capture = self.b._prepare_owner_forecast
        def mutate_after_capture(*args, **kwargs):
            capture(*args, **kwargs)
            other = sqlite3.connect(self.path)
            try:
                with other:
                    other.execute("UPDATE jobs SET producer_attempt_id=NULL WHERE id=?", (private,))
                with other:
                    other.execute("UPDATE jobs SET producer_attempt_id='secret-owner' WHERE id=?", (private,))
            finally:
                other.close()
        with patch.object(self.b, "_prepare_owner_forecast", side_effect=mutate_after_capture):
            self.assertEqual(self.step()[0][0], first)
        data, rows = self.projected(public=True)
        self.assertEqual(data["owner_gate"]["state"], "public_preview_changed")
        self.assertEqual(rows, [])

    def test_internal_revision_never_exposes_history_or_counts(self):
        self.prepare()
        self.nc(tier=1, private=True)
        self.nc(tier=1)
        self.step()
        self.assertTrue(self.b.db.execute("SELECT 1 FROM audit_events WHERE sequence=0").fetchone())
        self.assertTrue(all(event["sequence"] > 0 for event in self.b.audit_events(limit=1000)))
        self.assertTrue(all(event["sequence"] > 0 for event in self.b.audit_events(
            limit=1000, include_producer_storage=False)))
        self.assertNotIn("forecast.visibility_revision", json.dumps(self.b.dashboard(include_producer_storage=False)))

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

    def test_admission_invalidates_owner_prefix_but_preserves_committed_accounting(self):
        self.prepare()
        self.nc()
        pending = self.nc()
        self.step()
        snapshot = deepcopy(self.b._scheduler_snapshot)
        new_job = self.nc(MODELS[1], tier=1)
        data, rows = self.projected()
        self.assertEqual(data["owner_gate"]["state"], "queue_changed")
        self.assertEqual(rows, [])
        self.assertEqual(snapshot["time_cycle"], self.b._scheduler_snapshot["time_cycle"])
        self.assertIn(pending, [r["job_id"] for r in data["unranked_candidates"]])
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
            self.b._candidates(), self.gate, None, self.clock(), None,
            visibility_version=visibility_revision(self.b.db))
        self.gate.value = None
        self.b._publish_scheduler_snapshot()
        data = self.b.forecast()
        self.assertEqual([r["job_id"] for r in data["conditional_scenario"]["next_selections"]][:2],
                         jobs)

    def test_no_policy_fifo_does_not_promote_new_upload(self):
        self.prepare()
        historical = self.nc(priority="historical")
        newer = self.nc(priority="new_upload")
        self.gate.value = self.gate.fixture
        self.b._nextcloud_forecast_preview = capture_owner_preview(
            self.b._candidates(), self.gate, None, self.clock(), None,
            visibility_version=visibility_revision(self.b.db))
        self.gate.value = None
        self.b._publish_scheduler_snapshot()
        rows = self.b.forecast()["conditional_scenario"]["next_selections"]
        self.assertEqual([row["job_id"] for row in rows], [historical, newer])

    def test_scenario_and_unranked_are_disjoint_and_all_sources_share_one_list(self):
        self.prepare()
        self.p = self.policy({"nextcloud": {"weight": 7}, "peer": {"weight": 1}})
        self.nc()
        for _ in range(5):
            self.nc()
        self.step()
        self.submit(MODELS[1], "peer")
        data, rows = self.projected()
        self.assertFalse({r["job_id"] for r in rows} &
                         {r["job_id"] for r in data["unranked_candidates"]})
        self.assertEqual({r["source"] for r in rows}, {"nextcloud", "peer"})

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
