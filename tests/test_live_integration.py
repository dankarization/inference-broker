"""Portable integration contract; private deployment receipts are not fixtures."""
import unittest


class LiveIntegrationTests(unittest.TestCase):
    def test_optional_components_import_without_deployment_files(self):
        from broker import forecast, nextcloud_priority, syncopia_summary
        from broker.service import Broker
        self.assertEqual(Broker.nextcloud_capabilities()['default'], 'historical')
        self.assertTrue(callable(nextcloud_priority.NextcloudPriority.from_file))
        self.assertEqual(len(syncopia_summary.MODELS), 2)
        self.assertIsNotNone(forecast)

    def test_approved_diarization_and_existing_profiles_coexist(self):
        from broker.profiles import VOICE_PROFILE_NAMES, PROFILES
        from broker import diarization_contract as d, voice
        self.assertIn(d.PROFILE, VOICE_PROFILE_NAMES)
        self.assertIs(voice.diarization, d)
        self.assertIn('interactive', PROFILES)
        self.assertIn('eval-gemma4-12b', PROFILES)
