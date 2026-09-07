#!/usr/bin/env python3
"""Reject semantic-core predicate bypass and premature BK lifecycle promotion."""
import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "core", Path(__file__).with_name("check-v2-m5-kafka-semantic-core.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class SemanticCoreContractTest(unittest.TestCase):
    def test_current_sources(self):
        module.validate(module.ROOT)

    def test_rejects_missing_index_or_semantic_output(self):
        for field in ("indexKinds", "compiledOutput"):
            value = copy.deepcopy(module.EXPECTED)
            value[field].pop()
            with self.assertRaises(ValueError):
                module.validate_projection(value)

    def test_rejects_predicate_bypass_and_evidence_overclaims(self):
        for field, expected in module.EXPECTED.items():
            if isinstance(expected, bool):
                with self.subTest(field=field):
                    value = copy.deepcopy(module.EXPECTED)
                    value[field] = not expected
                    with self.assertRaises(ValueError):
                        module.validate_projection(value)

if __name__ == "__main__":
    unittest.main()
