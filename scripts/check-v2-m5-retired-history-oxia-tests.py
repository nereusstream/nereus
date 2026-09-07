#!/usr/bin/env python3
"""Reject projection promotion, fake dependency provenance and silently weakened history bounds."""
import copy
import importlib.util
import unittest
from pathlib import Path

path = Path(__file__).with_name("check-v2-m5-retired-history-oxia.py")
spec = importlib.util.spec_from_file_location("history_oxia_scope", path)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class ScopeTest(unittest.TestCase):
    def test_exact_scope_is_accepted(self):
        checker.validate_projection(copy.deepcopy(checker.EXPECTED))

    def test_native_authority_cannot_be_inferred_from_real_metadata(self):
        for key in ("nativeNamespaceQuotaAndRestartHeadroomIntegrated", "nativePerCellIoSchedulingAndMetricsIntegrated",
                    "nativeProtocolWriterMatrixComplete", "physicalDoneCacheLifecycleComplete",
                    "crossModulePhysicalDeleteCompositionComplete", "sourceBoundM5ReceiptPresent",
                    "m5FinalAuthority", "physicalDeleteAuthority", "productionAuthority", "historyPhysicalGcAuthority"):
            with self.subTest(key=key):
                value = copy.deepcopy(checker.EXPECTED)
                value[key] = True
                with self.assertRaises(ValueError):
                    checker.validate_projection(value)

    def test_source_lock_restart_and_continuous_bounds_cannot_be_weakened(self):
        for key, replacement in (("serverSource", "unlocked"), ("clientJarSha256", "unlocked"),
                                 ("loadedClientJarSha256Verified", False), ("continuousRetirements", 1024),
                                 ("sameNativeServerRestartPreservesSelectorAndHistory", False),
                                 ("sourceAndReferenceAuthority", "NATIVE_PROTOCOL_WRITERS")):
            with self.subTest(key=key):
                value = copy.deepcopy(checker.EXPECTED)
                value[key] = replacement
                with self.assertRaises(ValueError):
                    checker.validate_projection(value)


if __name__ == "__main__":
    unittest.main()
