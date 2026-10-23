#!/usr/bin/env python3
"""M5-specific fail-closed tests for the immutable historical M4 dependency."""

from __future__ import annotations

import importlib.util
from pathlib import Path, PurePosixPath
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-historical-m4.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


M5 = load_module(SCRIPT, "nereus_v2_m5_historical_m4_contract_tests")


class HistoricalM4DependencyTest(unittest.TestCase):
    def test_accepts_current_m5_design_descendant_without_recertifying_head(self) -> None:
        head = M5.validate(ROOT)
        self.assertEqual(40, len(head))

    def test_rejects_legacy_or_alternate_final_path(self) -> None:
        with self.assertRaisesRegex(M5.DependencyError, "exact M4 Final path"):
            M5.require_fixed_path(M5.LEGACY_FINAL_PATH)
        with self.assertRaisesRegex(M5.DependencyError, "exact M4 Final path"):
            M5.require_fixed_path(PurePosixPath("docs/v2/evidence/v2-m4/final/alternate.json"))

    def test_rejects_modified_final_bytes(self) -> None:
        raw = (ROOT / M5.FINAL_PATH).read_bytes()
        with self.assertRaisesRegex(M5.DependencyError, "Final SHA-256 differs"):
            M5.require_fixed_final(raw + b"\n")

    def test_rejects_broken_ancestry(self) -> None:
        with self.assertRaisesRegex(M5.DependencyError, "required ancestry is broken"):
            M5.require_ancestor(ROOT, M5.TESTED, M5.CLOSURE, lambda *_: False)

    def test_rejects_dirty_immutable_m4_evidence(self) -> None:
        with mock.patch.object(
            M5,
            "git",
            return_value=b" M docs/v2/evidence/v2-m4/final/example.json\0",
        ):
            with self.assertRaisesRegex(M5.DependencyError, "M4 evidence path is dirty"):
                M5.require_clean_m4_evidence(ROOT)

    def test_historical_rule_reuses_validator_without_current_source_recategorization(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("contract.validate_final_value(root, value, TESTED)", source)
        self.assertIn("contract.validate_scenarios(root, receipt_path)", source)
        self.assertNotIn("contract.validate_descendants", source)
        self.assertNotIn("contract.validate_final(root", source)


if __name__ == "__main__":
    unittest.main()
