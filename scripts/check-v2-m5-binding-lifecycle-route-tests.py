#!/usr/bin/env python3
"""Reject route projection promotion and conflation with native ownership/quota admission."""
import copy
import importlib.util
import unittest
from pathlib import Path

path = Path(__file__).with_name("check-v2-m5-binding-lifecycle-route.py")
spec = importlib.util.spec_from_file_location("binding_route_scope", path)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class ScopeTest(unittest.TestCase):
    def test_exact_scope_is_accepted(self):
        checker.validate_projection(copy.deepcopy(checker.EXPECTED))

    def test_configured_route_cannot_claim_native_ownership_quota_or_final(self):
        for key in ("nativeUniqueNamespaceAndBindingAssignmentAdmissionComplete",
                    "nativeNamespaceQuotaAndRestartHeadroomIntegrated", "nativePerCellIoSchedulingAndMetricsIntegrated",
                    "nativeProtocolWriterMatrixComplete", "physicalDoneCacheLifecycleComplete",
                    "bookKeeperCompactionMetadataRouteComplete", "sourceBoundM5ReceiptPresent", "m5FinalAuthority",
                    "physicalDeleteAuthority", "productionAuthority", "historyPhysicalGcAuthority"):
            with self.subTest(key=key):
                value = copy.deepcopy(checker.EXPECTED)
                value[key] = True
                with self.assertRaises(ValueError):
                    checker.validate_projection(value)

    def test_key_version_and_native_verification_bounds_are_required(self):
        for key, replacement in (("nativeKeyBytesMaximum", 1024), ("versionTokenBytes", 8),
                                 ("differentCellVersionReuseRejected", False),
                                 ("continuousRetirements", 1024),
                                 ("realM4HistoryControlBridge", "TEST_ONLY"),
                                 ("externalSourceAndReferenceAuthority", "NATIVE_WRITERS")):
            with self.subTest(key=key):
                value = copy.deepcopy(checker.EXPECTED)
                value[key] = replacement
                with self.assertRaises(ValueError):
                    checker.validate_projection(value)


if __name__ == "__main__":
    unittest.main()
