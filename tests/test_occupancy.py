import unittest
from dataclasses import replace

from broker.occupancy import Envelope, can_admit
from broker.profiles import Profile


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


if __name__ == '__main__':
    unittest.main()
