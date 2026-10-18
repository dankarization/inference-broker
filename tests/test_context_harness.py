import argparse
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from scripts.gemma26_context import calibration, evaluate, prepare


class ContextHarnessTests(unittest.TestCase):
    def test_reproducible_bounded_prompts_and_separate_answers(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(out=directory, calibration=[], context=None,
                                      target_tokens=None, reserve=8192, seed="regression")
            with contextlib.redirect_stdout(io.StringIO()):
                prepare(args)
            first = {p.name: p.read_bytes() for p in Path(directory).iterdir()}
            with contextlib.redirect_stdout(io.StringIO()):
                prepare(args)
            self.assertEqual(first, {p.name: p.read_bytes() for p in Path(directory).iterdir()})
            manifests = list(Path(directory).glob("*.expected.json"))
            self.assertEqual(len(manifests), 5)
            for path in manifests:
                manifest = json.loads(path.read_text())
                prompt = Path(manifest["prompt"]).read_text()
                self.assertLess(len(prompt.encode()), 1_700_000)
                for position, (key, value) in zip((.02, .50, .98), manifest["expected"].items()):
                    self.assertEqual(prompt.count(value), 1)
                    self.assertAlmostEqual(prompt.index(value) / len(prompt), position, delta=.02)
                self.assertNotIn(value, prompt[-100:])

    def test_calibration_uses_measured_tokens_not_context_window(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for size in (10000, 50000):
                path = Path(directory) / str(size)
                path.write_text(json.dumps({"path_verified": True, "prompt_bytes": size,
                                            "actual_prompt_tokens": size*.25+7000}))
                paths.append(path)
            slope, intercept = calibration(paths)
            self.assertAlmostEqual(slope, .25)
            self.assertAlmostEqual(intercept, 7000)

    def test_evaluation_rejects_fallback_compaction_and_shortened_prompt(self):
        manifest = {"context": 32768, "target_tokens": 24064, "prompt_bytes": 100,
                    "calibrated": True, "expected": {"amber": "alpha"}}
        cli = {"result": {"meta": {"agentMeta": {"provider": "local", "model": "gemma4:26b"}}}}
        job = {"id": "j1", "source": "openclaw", "profile": "openclaw-gemma4-26b",
               "state": "completed", "payload": {"model": "gemma4:26b", "think": False,
               "messages": [{"role": "user", "content": "prompt"}],
               "options": {"num_ctx": 32768, "num_predict": 512}},
               "result": {"model": "gemma4:26b", "prompt_eval_count": 24064,
                          "message": {"content": '{"amber":"alpha"}'}}}
        before = {"jobs": [], "unit": "PID=1"}
        after = {"jobs": [job], "unit": "PID=1"}
        self.assertTrue(evaluate(manifest, "prompt", cli, before, after)["passed"])
        meta = cli["result"]["meta"]["agentMeta"]
        meta["provider"] = "cloud"
        with self.assertRaises(AssertionError):
            evaluate(manifest, "prompt", cli, before, after)
        meta["provider"] = "local"
        meta["compactionCount"] = 1
        with self.assertRaises(AssertionError):
            evaluate(manifest, "prompt", cli, before, after)
        meta.pop("compactionCount")
        with self.assertRaises(AssertionError):
            evaluate(manifest, "longer original prompt", cli, before, after)
        job["result"]["prompt_eval_count"] = 5000
        self.assertFalse(evaluate(manifest, "prompt", cli, before, after)["passed"])
        job["result"]["message"]["content"] = '{"amber":"wrong"}'
        self.assertFalse(evaluate(manifest, "prompt", cli, before, after)["recall"]["amber"])
