#!/usr/bin/env python3
"""Fail-closed contract tests for the non-promotable M5-C core checker."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-retention-core.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


M5C = load_module(SCRIPT, "nereus_m5_c_core_contract_tests")


class M5RetentionCoreContractTest(unittest.TestCase):
    def test_accepts_current_non_promotable_blocked_core(self) -> None:
        M5C.validate(ROOT)

    def test_rejects_missing_core_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(M5C.RetentionCoreError, "missing/empty"):
                M5C.validate_required_sources(Path(directory), (M5C.REQUIRED_SOURCES[0],))

    def test_rejects_adapter_that_does_not_fail_closed(self) -> None:
        with self.assertRaisesRegex(M5C.RetentionCoreError, "lacks required"):
            M5C.require_literals("return false", ("return false", "UNSUPPORTED"), "fixture")

    def test_rejects_projection_that_claims_retirement_authority(self) -> None:
        value = json.loads((ROOT / M5C.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["metadataRetirementAuthority"] = True
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / M5C.PROJECTION_PATH
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(changed), encoding="utf-8")
            with self.assertRaisesRegex(M5C.RetentionCoreError, "overstates"):
                M5C.validate_projection(root)

    def test_rejects_projection_that_enables_sequential_cas(self) -> None:
        value = json.loads((ROOT / M5C.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["selectedMetadataBackend"]["sequentialCasFallback"] = "ENABLED"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / M5C.PROJECTION_PATH
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(changed), encoding="utf-8")
            with self.assertRaisesRegex(M5C.RetentionCoreError, "capability projection differs"):
                M5C.validate_projection(root)


if __name__ == "__main__":
    unittest.main()
