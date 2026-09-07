#!/usr/bin/env python3
"""Reject evidence scope and authority expansion in the BK M4 bridge projection."""
import copy
import importlib.util
from pathlib import Path
import unittest
spec = importlib.util.spec_from_file_location("checker", Path(__file__).with_name("check-v2-m5-bookkeeper-m4-recovery.py"))
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)

class ProjectionTest(unittest.TestCase):
    def test_accepts_exact_scope(self):
        checker.validate(checker.ROOT)

    def test_rejects_native_authority_claim(self):
        value = copy.deepcopy(checker.EXPECTED)
        value["nativeProtocolOwnerAdmissionIntegrated"] = True
        with self.assertRaises(ValueError):
            checker.validate_projection(value)

    def test_rejects_missing_negative_boundary_or_skipped_case(self):
        value = copy.deepcopy(checker.EXPECTED)
        del value["physicalDeleteAuthority"]
        with self.assertRaises(ValueError):
            checker.validate_projection(value)
        value = copy.deepcopy(checker.EXPECTED)
        value["focusedSuites"]["skipped"] = 1
        with self.assertRaises(ValueError):
            checker.validate_projection(value)

if __name__ == "__main__":
    unittest.main()
