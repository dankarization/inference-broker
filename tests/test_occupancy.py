import unittest
from dataclasses import replace

from broker.occupancy import Envelope, can_admit
from broker.profiles import Profile
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

    def test_voice_weight_two_receives_half_weight_one_budget(self):
        class Policy:
            def weight(self, source):
                return {'local-voice': 2, 'llm-source': 1}[source]

            def snapshot(self):
                return {'sources': {'local-voice': {'weight': 2}, 'llm-source': {'weight': 1}}}

        broker = Broker.__new__(Broker)
        available = {
            ('local-voice', 'voice-model'): {'source': 'local-voice', 'queued_at': 1, 'created': 1},
            ('llm-source', 'llm-model'): {'source': 'llm-source', 'queued_at': 1, 'created': 1},
        }
        cycle = broker._new_time_cycle(available, Policy(), 1)
        voice = cycle['lanes'][('local-voice', 'voice-model')]['budget']
        llm = cycle['lanes'][('llm-source', 'llm-model')]['budget']
        self.assertAlmostEqual(voice, SCHEDULING_HORIZON_SECONDS / 3)
        self.assertAlmostEqual(llm, 2 * voice)


if __name__ == '__main__':
    unittest.main()
