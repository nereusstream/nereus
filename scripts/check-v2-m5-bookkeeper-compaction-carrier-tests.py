#!/usr/bin/env python3
"""Reject bounded BK carrier scope/cap changes and premature lifecycle promotion."""
import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("carrier", Path(__file__).with_name("check-v2-m5-bookkeeper-compaction-carrier.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class CarrierContractTest(unittest.TestCase):
    def test_current_sources(self):
        module.validate(module.ROOT)

    def test_rejects_changed_native_locks_bounds_and_suite_claims(self):
        for field in ("bookKeeperSource", "bookKeeperClientJarSha256", "bookKeeperImage", "bounds", "focusedSuites"):
            value = copy.deepcopy(module.EXPECTED)
            value.pop(field)
            with self.assertRaises(ValueError):
                module.validate_projection(value)

    def test_rejects_authority_and_predicate_overclaims(self):
        for field, expected in module.EXPECTED.items():
            if isinstance(expected, bool):
                with self.subTest(field=field):
                    value = copy.deepcopy(module.EXPECTED)
                    value[field] = not expected
                    with self.assertRaises(ValueError):
                        module.validate_projection(value)

if __name__ == "__main__":
    unittest.main()
