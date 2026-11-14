"""Release-byte guard: approved live overlay plus only model-identity changes."""
import hashlib
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class LiveIntegrationTests(unittest.TestCase):
    def test_exact_release_manifest_and_preserved_live_files(self):
        receipt = json.loads((ROOT / 'docs/model-identity-integration-manifest.json').read_text())
        expected = receipt['candidate_manifest']
        actual = {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in (ROOT / 'broker').rglob('*')
                  if p.is_file() and '__pycache__' not in p.parts
                  and p.suffix in {'.py', '.js', '.css', '.html'}}
        self.assertEqual(actual, expected)
        before = receipt['live']['manifest']
        self.assertEqual(set(actual), set(before))
        self.assertEqual({n for n in actual if actual[n] != before[n]},
                         set(receipt['changed_over_live']))
        self.assertEqual(set(receipt['changed_over_live']), {
            'broker/service.py', 'broker/dashboard.py',
            'broker/static/dashboard.js', 'broker/static/dashboard.html'})

    def test_approved_diarization_and_existing_profiles_coexist(self):
        from broker.profiles import VOICE_PROFILE_NAMES, PROFILES
        from broker import diarization_contract as d, voice
        self.assertIn(d.PROFILE, VOICE_PROFILE_NAMES)
        self.assertIs(voice.diarization, d)
        self.assertIn('interactive', PROFILES)
        self.assertIn('eval-gemma4-12b', PROFILES)
