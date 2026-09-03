#!/usr/bin/env python3
"""Fail-closed contract tests for the M5-D target-scoped authority amendment."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-design-amendment-2.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


AMENDMENT = load_module(SCRIPT, "nereus_v2_m5_design_amendment_2_tests")


class M5DesignAmendment2ContractTest(unittest.TestCase):
    def test_accepts_current_amendment(self) -> None:
        AMENDMENT.validate(ROOT)

    def test_manifest_rejects_changed_predecessor_identity(self) -> None:
        value = json.loads((ROOT / AMENDMENT.MANIFEST_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["predecessorAmendment"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(AMENDMENT.Amendment2Error, "predecessorAmendment identity differs"):
            AMENDMENT.validate_manifest(ROOT, changed)

    def test_manifest_rejects_changed_bound_document(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for source in (
                AMENDMENT.BASE_MANIFEST_PATH,
                AMENDMENT.PREDECESSOR_MANIFEST_PATH,
                *AMENDMENT.EXPECTED_DOCUMENTS,
            ):
                candidate = root / source
                candidate.parent.mkdir(parents=True, exist_ok=True)
                candidate.write_bytes((ROOT / source).read_bytes())
            value = json.loads((ROOT / AMENDMENT.MANIFEST_PATH).read_text(encoding="utf-8"))
            (root / AMENDMENT.DESIGN_PATH).write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(AMENDMENT.Amendment2Error, "SHA differs"):
                AMENDMENT.validate_manifest(root, value)

    def test_manifest_rejects_authority_overclaim(self) -> None:
        value = json.loads((ROOT / AMENDMENT.MANIFEST_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["physicalDeleteAuthority"] = True
        with self.assertRaisesRegex(AMENDMENT.Amendment2Error, "physicalDeleteAuthority must remain false"):
            AMENDMENT.validate_manifest(ROOT, changed)

    def test_contract_rejects_missing_monotonic_revision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for source in AMENDMENT.EXPECTED_DOCUMENTS:
                candidate = root / source
                candidate.parent.mkdir(parents=True, exist_ok=True)
                text = (ROOT / source).read_text(encoding="utf-8")
                candidate.write_text(text.replace("authorityRevision", "REMOVED_REVISION"), encoding="utf-8")
            with self.assertRaisesRegex(AMENDMENT.Amendment2Error, "lacks amendment contract"):
                AMENDMENT.validate_contract(root)

    def test_rejects_mutated_original_m5_d(self) -> None:
        frozen = (ROOT / AMENDMENT.ORIGINAL_M5_D_PATH).read_bytes()
        with self.assertRaisesRegex(AMENDMENT.Amendment2Error, "immutable original M5-D was changed"):
            AMENDMENT.validate_immutable_original_bytes(frozen + b"changed\n", frozen)


if __name__ == "__main__":
    unittest.main()
