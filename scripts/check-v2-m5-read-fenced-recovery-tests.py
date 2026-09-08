#!/usr/bin/env python3
"""Negative contract tests for observation recovery and its evidence boundary."""
import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "recovery", Path(__file__).with_name("check-v2-m5-read-fenced-recovery.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class RecoveryContractTest(unittest.TestCase):
    def test_accepts_exact_focused_projection(self):
        module.validate_projection(copy.deepcopy(module.EXPECTED))

    def test_rejects_every_recovery_predicate_flip_or_missing_binding(self):
        for key in module.EXPECTED["recovery"]:
            with self.subTest(key=key):
                value = copy.deepcopy(module.EXPECTED)
                value["recovery"][key] = not value["recovery"][key]
                with self.assertRaises(ValueError):
                    module.validate_projection(value)
        for binding in module.EXPECTED["observationBinding"]:
            with self.subTest(binding=binding):
                value = copy.deepcopy(module.EXPECTED)
                value["observationBinding"].remove(binding)
                with self.assertRaises(ValueError):
                    module.validate_projection(value)

    def test_rejects_native_metadata_claim_changes(self):
        for key, expected in module.EXPECTED["nativeMetadataExecution"].items():
            with self.subTest(key=key):
                value = copy.deepcopy(module.EXPECTED)
                value["nativeMetadataExecution"][key] = not expected if isinstance(expected, bool) else None
                with self.assertRaises(ValueError):
                    module.validate_projection(value)

    def test_rejects_intent_recovery_predicate_changes(self):
        for key, expected in module.EXPECTED["intentRecovery"].items():
            with self.subTest(key=key):
                value = copy.deepcopy(module.EXPECTED)
                value["intentRecovery"][key] = not expected if isinstance(expected, bool) else None
                with self.assertRaises(ValueError):
                    module.validate_projection(value)

    def test_rejects_native_integration_and_evidence_overclaims(self):
        for key, expected in module.EXPECTED.items():
            if expected is False:
                with self.subTest(key=key):
                    value = copy.deepcopy(module.EXPECTED)
                    value[key] = True
                    with self.assertRaises(ValueError):
                        module.validate_projection(value)

if __name__ == "__main__":
    unittest.main()
