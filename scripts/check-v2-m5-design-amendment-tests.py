#!/usr/bin/env python3
"""Fail-closed contract tests for the M5-C single-Binding design amendment."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-design-amendment.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


AMENDMENT = load_module(SCRIPT, "nereus_v2_m5_design_amendment_tests")


class M5DesignAmendmentContractTest(unittest.TestCase):
    def test_accepts_current_amendment(self) -> None:
        AMENDMENT.validate(ROOT)

    def test_manifest_rejects_changed_bound_document(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for source in AMENDMENT.EXPECTED_DOCUMENTS:
                candidate = root / source
                candidate.parent.mkdir(parents=True, exist_ok=True)
                candidate.write_bytes((ROOT / source).read_bytes())
            value = json.loads((ROOT / AMENDMENT.MANIFEST_PATH).read_text(encoding="utf-8"))
            (root / AMENDMENT.DESIGN_PATH).write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(AMENDMENT.AmendmentError, "SHA differs"):
                AMENDMENT.validate_manifest(root, value)

    def test_manifest_rejects_base_identity_change(self) -> None:
        value = json.loads((ROOT / AMENDMENT.MANIFEST_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["baseFreezeManifestSha256"] = "0" * 64
        with self.assertRaisesRegex(AMENDMENT.AmendmentError, "baseFreezeManifestSha256 differs"):
            AMENDMENT.validate_manifest(ROOT, changed)

    def test_contract_rejects_missing_ticket_fence_race(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for source in AMENDMENT.EXPECTED_DOCUMENTS:
                candidate = root / source
                candidate.parent.mkdir(parents=True, exist_ok=True)
                text = (ROOT / source).read_text(encoding="utf-8")
                candidate.write_text(text.replace("PENDING_REFERENCE_MUTATION_V1", "REMOVED"), encoding="utf-8")
            with self.assertRaisesRegex(AMENDMENT.AmendmentError, "lacks amendment contract"):
                AMENDMENT.validate_contract(root)


if __name__ == "__main__":
    unittest.main()
