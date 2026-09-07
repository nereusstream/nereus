#!/usr/bin/env python3
"""Negative governance checks for typed M5 eligibility requirements."""

import copy
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("m5_eligibility", ROOT / "scripts/check-v2-m5-delete-eligibility.py")
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class EligibilityProjectionTest(unittest.TestCase):
    def test_current_implementation(self):
        CHECK.validate(ROOT)

    def test_no_missing_reason_or_semantic_aspect(self):
        for key in ("reasons", "semanticAspects", "referenceScopes"):
            value = copy.deepcopy(CHECK.EXPECTED)
            value[key].pop()
            with self.assertRaises(ValueError):
                CHECK.validate_projection(value)

    def test_no_bypass_or_native_evidence_overclaim(self):
        for key in CHECK.EXPECTED["gates"]:
            value = copy.deepcopy(CHECK.EXPECTED)
            value["gates"][key] = not value["gates"][key]
            with self.assertRaises(ValueError):
                CHECK.validate_projection(value)
        for key in ("nativeProtocolProofProducersIntegrated", "realDependencyReclamationEvidencePresent",
                    "sourceBoundReceiptPresent", "physicalDeleteAuthority", "productionAuthority"):
            value = copy.deepcopy(CHECK.EXPECTED)
            value[key] = True
            with self.assertRaises(ValueError):
                CHECK.validate_projection(value)


if __name__ == "__main__":
    unittest.main()
