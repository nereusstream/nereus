#!/usr/bin/env python3
"""Negative tests for M5 lifecycle contract chain and evidence ownership."""

import copy
import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("m5_lifecycle", ROOT / "scripts/check-v2-m5-lifecycle-design.py")
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class LifecycleDesignTest(unittest.TestCase):
    def setUp(self):
        self.manifest = CHECK.read_json(ROOT, CHECK.MANIFEST)
        self.acceptance = CHECK.read_json(ROOT, CHECK.ACCEPTANCE)

    def test_current_chain(self):
        CHECK.validate(ROOT)

    def test_no_design_authority_escalation(self):
        for name in ("physicalDeleteAuthority", "scenarioPromotionAuthority", "productionAuthority"):
            for invalid in (True, 0, "false", None):
                value = copy.deepcopy(self.manifest)
                value[name] = invalid
                with self.subTest(name=name, invalid=invalid), self.assertRaises(ValueError):
                    CHECK.validate_manifest(ROOT, value)

    def test_bound_bytes_paths_and_chain(self):
        for field in ("path", "sha256"):
            value = copy.deepcopy(self.manifest)
            value["predecessorAmendment"][field] = "../substitute"
            with self.assertRaises(ValueError):
                CHECK.validate_manifest(ROOT, value)
        value = copy.deepcopy(self.manifest)
        value["documents"][0]["sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            CHECK.validate_manifest(ROOT, value)

    def test_missing_duplicate_or_misowned_obligation(self):
        for mutation in (lambda rows: rows.pop(), lambda rows: rows.__setitem__(1, rows[0]),
                         lambda rows: rows[0].__setitem__("owner", "M6")):
            value = copy.deepcopy(self.acceptance)
            mutation(value["obligations"])
            with self.assertRaises(ValueError):
                CHECK.validate_acceptance(value)

    def test_no_mock_only_or_object_dependent_bk_only(self):
        for row_id, dependencies in (("BK_ONLY_COMPACTION", ["BOOKKEEPER", "OXIA", "OBJECT"]),
                                     ("WRITER_RACES", []), ("READ_FENCED_TAKEOVER", ["OXIA"])):
            value = copy.deepcopy(self.acceptance)
            next(row for row in value["obligations"] if row["id"] == row_id)["requiredDependencies"] = dependencies
            with self.assertRaises(ValueError):
                CHECK.validate_acceptance(value)

    def test_design_never_promotes_m5_or_m6(self):
        for key in ("obligations", "deferred"):
            value = copy.deepcopy(self.acceptance)
            value[key][0]["status"] = "PASSED_CURRENT_SOURCE"
            with self.assertRaises(ValueError):
                CHECK.validate_acceptance(value)


if __name__ == "__main__":
    unittest.main()
