#!/usr/bin/env python3
"""M4-specific fail-closed tests for the immutable historical M3 dependency."""

from __future__ import annotations

import base64
import importlib.util
import json
from pathlib import Path, PurePosixPath
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m4-historical-m3.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


M4 = load_module(SCRIPT, "nereus_v2_m4_historical_m3_contract_tests")


class HistoricalM3DependencyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.m3 = M4.load_m3_contract(ROOT)

    def test_accepts_current_normal_m4_descendant_without_recertifying_head(self) -> None:
        head, selected = M4.validate(ROOT)
        self.assertEqual(40, len(head))
        self.assertEqual("RANGE_64", selected)

    def test_unchanged_m3_descendant_policy_still_rejects_current_head(self) -> None:
        with self.assertRaisesRegex(self.m3.FinalError, "non-evidence path changed"):
            self.m3.validate_descendants(ROOT, M4.TESTED)

    def test_rejects_legacy_or_alternate_final_path(self) -> None:
        with self.assertRaisesRegex(M4.DependencyError, "exact M3 Final path"):
            M4.require_fixed_path(M4.LEGACY_FINAL_PATH)
        with self.assertRaisesRegex(M4.DependencyError, "exact M3 Final path"):
            M4.require_fixed_path(PurePosixPath("docs/v2/evidence/v2-m3/final/alternate.json"))

    def test_rejects_modified_final_bytes(self) -> None:
        raw = (ROOT / M4.FINAL_PATH).read_bytes()
        with self.assertRaisesRegex(M4.DependencyError, "Final SHA-256 differs"):
            M4.require_fixed_final(raw + b"\n")

    def test_rejects_broken_ancestry(self) -> None:
        with self.assertRaisesRegex(M4.DependencyError, "required ancestry is broken"):
            M4.require_ancestor(ROOT, M4.TESTED, M4.CLOSURE, lambda *_: False)

    def test_rejects_dirty_immutable_m3_evidence(self) -> None:
        with mock.patch.object(
            M4,
            "git",
            return_value=b" M docs/v2/evidence/v2-m3/final/example.json\0",
        ):
            with self.assertRaisesRegex(M4.DependencyError, "M3 evidence path is dirty"):
                M4.require_clean_m3_evidence(ROOT)

    def test_allocator_candidate_is_exact_range_64(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = PurePosixPath("evidence/allocator.json")
            (root / path).parent.mkdir(parents=True)
            decision = {"status": "PROMOTABLE", "selectedCandidate": "RANGE_16"}
            verification = {
                "promotionDecisionBase64": base64.b64encode(
                    json.dumps(decision, separators=(",", ":")).encode()
                ).decode()
            }
            (root / path).write_text(json.dumps(verification), encoding="utf-8")
            receipt = {
                "childReceipts": [
                    {
                        "kind": "ALLOCATOR_SELECTION",
                        "attachments": [
                            {"kind": "ALLOCATOR_V5_CAMPAIGN_VERIFICATION", "path": str(path)}
                        ],
                    }
                ]
            }
            with self.assertRaisesRegex(M4.DependencyError, "PROMOTABLE/RANGE_64"):
                M4.selected_allocator_candidate(root, receipt)

    def test_historical_rule_uses_e5_blob_not_current_source_lock_digest(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn('return contract.validate_receipt_value(root, value, TESTED)', source)
        self.assertIn('child_contract._validate_source_freshness = historical_source_freshness', source)
        self.assertNotIn('sha256((root / "docs/v2/source-locks.json")', source)


if __name__ == "__main__":
    unittest.main()
