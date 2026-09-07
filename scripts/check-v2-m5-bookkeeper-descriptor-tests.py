#!/usr/bin/env python3
"""Reject descriptor scope/cap regressions and premature native lifecycle promotion."""
import copy
import importlib.util
from pathlib import Path
import unittest
spec = importlib.util.spec_from_file_location("descriptor", Path(__file__).with_name("check-v2-m5-bookkeeper-descriptor.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class DescriptorContractTest(unittest.TestCase):
    def test_current_sources(self):
        module.validate(module.ROOT)

    def test_rejects_changed_descriptor_domain_caps_and_suite_claims(self):
        for field in ("descriptorKind", "descriptorWire", "maximumDescriptorBytes", "physicalIndexLocators", "focusedSuites"):
            value = copy.deepcopy(module.EXPECTED)
            value.pop(field)
            with self.assertRaises(ValueError):
                module.validate_projection(value)

    def test_rejects_removed_predicates_or_premature_authority(self):
        for field, expected in module.EXPECTED.items():
            if isinstance(expected, bool):
                with self.subTest(field=field):
                    value = copy.deepcopy(module.EXPECTED)
                    value[field] = not expected
                    with self.assertRaises(ValueError):
                        module.validate_projection(value)

if __name__ == "__main__":
    unittest.main()
