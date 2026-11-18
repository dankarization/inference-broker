import unittest
from dataclasses import replace

from broker.occupancy import Envelope, can_admit
from broker.profiles import Profile, PROFILES
from broker.service import Broker, SCHEDULING_HORIZON_SECONDS


class OccupancyTest(unittest.TestCase):
    def setUp(self):
        base = Profile('base', 'model', 1, 1, 1)
        self.profiles = {
            'unknown': base,
            'exclusive': replace(base, occupancy_class='exclusive'),
            'llm': replace(base, occupancy_class='shareable', occupancy_cohort='probe',
                           occupancy_max_jobs=1, occupancy_peak_vram_bytes=10,
                           occupancy_evidence='synthetic test'),
            'voice': replace(base, occupancy_class='shareable', occupancy_cohort='probe',
                             occupancy_max_jobs=3, occupancy_peak_vram_bytes=5,
                             occupancy_evidence='synthetic test'),
        }
        self.envelopes = {'probe': Envelope({'llm': 1, 'voice': 3}, 'synthetic only')}

    def admit(self, name, active):
        return can_admit(name, active, self.profiles, self.envelopes)

    def test_unknown_and_exclusive_cannot_share_in_either_direction(self):
        for name in ('unknown', 'exclusive'):
            self.assertTrue(self.admit(name, []))
            self.assertFalse(self.admit(name, ['voice']))
            self.assertFalse(self.admit('voice', [name]))
        self.assertFalse(self.admit('voice', ['missing']))

    def test_joint_vector_and_multiple_voice_slots(self):
        self.assertTrue(self.admit('voice', ['llm', 'voice', 'voice']))
        self.assertFalse(self.admit('voice', ['llm', 'voice', 'voice', 'voice']))
        self.assertFalse(self.admit('llm', ['llm']))

    def test_incomplete_or_unmeasured_evidence_fails_closed(self):
        self.profiles['voice'] = replace(self.profiles['voice'], occupancy_peak_vram_bytes=None)
        self.assertFalse(self.admit('voice', ['llm']))
        self.profiles['voice'] = replace(self.profiles['voice'], occupancy_peak_vram_bytes=5)
        self.envelopes.clear()
        self.assertFalse(self.admit('voice', ['llm']))

    def test_exclusive_or_unknown_blocks_all_other_occupants(self):
        for active in ('unknown', 'exclusive'):
            for candidate in self.profiles:
                with self.subTest(active=active, candidate=candidate):
                    self.assertFalse(self.admit(candidate, [active]))

    def test_shipped_profiles_have_no_unmeasured_sharing(self):
        for name, profile in PROFILES.items():
            with self.subTest(profile=name):
                self.assertEqual(profile.occupancy_class, 'unknown')
                self.assertIsNone(profile.occupancy_cohort)
                self.assertIsNone(profile.occupancy_peak_vram_bytes)
                self.assertIsNone(profile.occupancy_evidence)

    def test_voice_weight_two_receives_half_weight_one_budget(self):
        class Policy:
            def weight(self, source):
                return {'local-voice': 2, 'llm-source': 1}[source]

            def snapshot(self):
                return {'sources': {'local-voice': {'weight': 2}, 'llm-source': {'weight': 1}}}

        broker = Broker.__new__(Broker)
        broker._active_time_batches = {}
        available = {
            'local-voice': {'source': 'local-voice', 'target_model': 'voice-model', 'queued_at': 1, 'created': 1},
            'llm-source': {'source': 'llm-source', 'target_model': 'llm-model', 'queued_at': 1, 'created': 1},
        }
        cycle = broker._new_time_cycle(available, Policy(), 1)
        voice = cycle['sources']['local-voice']['budget']
        llm = cycle['sources']['llm-source']['budget']
        self.assertAlmostEqual(voice, SCHEDULING_HORIZON_SECONDS / 3)
        self.assertAlmostEqual(llm, 2 * voice)

    def test_parallel_attempts_are_charged_to_their_own_lanes(self):
        broker = Broker.__new__(Broker)
        a, b = ('local-voice', 'voice'), ('llm-source', 'llm')
        broker._active_time_batches = {
            'a': {'lane': a, 'started': 10},
            'b': {'lane': b, 'started': 12},
        }
        broker._duration_estimates = {}
        broker._time_cycle = {'sources': {a[0]: {'used': 0}, b[0]: {'used': 0}}, 'total_used': 0}
        broker._model_batches = {}
        broker._last_scheduler_decision = None
        broker._complete_time_batch('b', 17)
        self.assertEqual(broker._time_cycle['sources'][b[0]]['used'], 5)
        self.assertIn('a', broker._active_time_batches)
        broker._complete_time_batch('a', 20)
        self.assertEqual(broker._time_cycle['sources'][a[0]]['used'], 10)
        self.assertEqual(broker._time_cycle['total_used'], 15)
        self.assertFalse(broker._active_time_batches)

    def test_inflight_lane_survives_ready_queue_rebase(self):
        class Policy:
            def weight(self, source):
                return {'local-voice': 2, 'llm-source': 1}[source]

            def snapshot(self):
                return {'sources': {'local-voice': {'weight': 2}, 'llm-source': {'weight': 1}}}

        broker = Broker.__new__(Broker)
        broker._active_time_batches = {}
        a, b = ('local-voice', 'voice'), ('llm-source', 'llm')
        rows = {a[0]: {'source': a[0], 'target_model': a[1], 'queued_at': 1, 'created': 1},
                b[0]: {'source': b[0], 'target_model': b[1], 'queued_at': 1, 'created': 1}}
        cycle = broker._new_time_cycle(rows, Policy(), 1)
        broker._active_time_batches['a'] = {'lane': a, 'started': 1}
        cycle = broker._new_time_cycle({b[0]: rows[b[0]]}, Policy(), 2, cycle)
        self.assertIn(a[0], cycle['sources'])
        broker._time_cycle = cycle
        broker._duration_estimates = {}
        broker._model_batches = {}
        broker._last_scheduler_decision = None
        broker._complete_time_batch('a', 11)
        self.assertEqual(cycle['sources'][a[0]]['used'], 10)

    def test_inflight_lane_survives_fresh_horizon(self):
        class Policy:
            def weight(self, source):
                return 1

            def snapshot(self):
                return {'sources': {'llm-source': {'weight': 1}}}

        broker = Broker.__new__(Broker)
        a, b = ('local-voice', 'voice'), ('llm-source', 'llm')
        broker._active_time_batches = {'a': {'lane': a, 'started': 1}}
        broker._time_cycle = {'sources': {a[0]: {'used': 100, 'budget': 100,
                                                  'weight': 2, 'queued_at': 1}}}
        rows = {b[0]: {'source': b[0], 'target_model': b[1], 'queued_at': 1, 'created': 1}}
        cycle = broker._new_time_cycle(rows, Policy(), 2)
        self.assertEqual(cycle['sources'][a[0]]['used'], 0)
        self.assertTrue(cycle['sources'][a[0]]['accounting_only'])
        broker._time_cycle = cycle
        broker._duration_estimates = {}
        broker._model_batches = {}
        broker._last_scheduler_decision = None
        broker._complete_time_batch('a', 11)
        self.assertEqual(cycle['sources'][a[0]]['used'], 10)


if __name__ == '__main__':
    unittest.main()
