#!/usr/bin/env python3
"""Fail-closed tests for the complete M5-C retirement implementation checker."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-retention-retirement.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RETIREMENT = load_module(SCRIPT, "nereus_v2_m5_retention_retirement_tests")


class M5RetentionRetirementContractTest(unittest.TestCase):
    def test_accepts_current_implementation(self) -> None:
        RETIREMENT.validate(ROOT)

    def test_projection_rejects_wrong_real_oxia_image(self) -> None:
        value = json.loads((ROOT / RETIREMENT.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["realOxiaBoundary"]["serverImageId"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(RETIREMENT.RetentionRetirementError, "real Oxia boundary"):
            RETIREMENT.validate_projection_value(changed)

    def test_projection_rejects_missing_full_gate(self) -> None:
        value = json.loads((ROOT / RETIREMENT.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["fullRetirementGatePresent"] = False
        with self.assertRaisesRegex(RETIREMENT.RetentionRetirementError, "not present"):
            RETIREMENT.validate_projection_value(changed)

    def test_projection_rejects_receipt_or_physical_delete_authority(self) -> None:
        value = json.loads((ROOT / RETIREMENT.PROJECTION_PATH).read_text(encoding="utf-8"))
        for field in ("sourceBoundReceiptPresent", "physicalDeleteAuthority"):
            changed = copy.deepcopy(value)
            changed[field] = True
            with self.assertRaisesRegex(RETIREMENT.RetentionRetirementError, "overstates"):
                RETIREMENT.validate_projection_value(changed)

    def test_source_lock_rejects_changed_client_artifact(self) -> None:
        value = json.loads((ROOT / "docs/v2/source-locks.json").read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["dependencyEvidenceBindings"]["oxiaClientArtifacts"]["artifacts"]["clientJar"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(RETIREMENT.RetentionRetirementError, "client artifact"):
            RETIREMENT.validate_source_lock_value(changed)

    def test_required_sources_reject_missing_real_integration(self) -> None:
        with self.assertRaisesRegex(RETIREMENT.RetentionRetirementError, "missing/empty"):
            RETIREMENT.validate_required_sources(ROOT, ("missing/M5RetentionOxiaIntegrationTest.java",))


if __name__ == "__main__":
    unittest.main()
