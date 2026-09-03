#!/usr/bin/env python3
"""Fail-closed tests for the focused M5-D orphan/admission checker."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-orphan-admission.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECK = load_module(SCRIPT, "nereus_v2_m5_orphan_admission_tests")


class M5OrphanAdmissionContractTest(unittest.TestCase):
    def test_accepts_current_orphan_admission_slice(self) -> None:
        CHECK.validate(ROOT)

    def test_projection_rejects_open_orphan_taxonomy(self) -> None:
        value = json.loads((ROOT / CHECK.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["closedOrphanClasses"].append("UNREVIEWED")
        with self.assertRaisesRegex(CHECK.OrphanAdmissionError, "taxonomy differs"):
            CHECK.validate_projection_value(changed)

    def test_projection_rejects_permanent_evidence_gc(self) -> None:
        value = json.loads((ROOT / CHECK.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["classesAllowedToMark"].append("ALLOCATOR_NO_REUSE_EVIDENCE")
        with self.assertRaisesRegex(CHECK.OrphanAdmissionError, "mark-eligible"):
            CHECK.validate_projection_value(changed)

    def test_projection_rejects_authority_overclaim(self) -> None:
        value = json.loads((ROOT / CHECK.PROJECTION_PATH).read_text(encoding="utf-8"))
        for field in (
            "externalDeleteApiPresent",
            "intentMutationApiPresent",
            "fullM5DGatePresent",
            "physicalDeleteAuthority",
        ):
            changed = copy.deepcopy(value)
            changed[field] = True
            with self.assertRaisesRegex(CHECK.OrphanAdmissionError, "overstates"):
                CHECK.validate_projection_value(changed)

    def test_sources_reject_missing_protocol(self) -> None:
        original = CHECK.REQUIRED_SOURCES
        try:
            CHECK.REQUIRED_SOURCES = ("missing/M5PhysicalOrphanProtocolV1.java",)
            with self.assertRaisesRegex(CHECK.OrphanAdmissionError, "missing/empty"):
                CHECK.validate_sources(ROOT)
        finally:
            CHECK.REQUIRED_SOURCES = original


if __name__ == "__main__":
    unittest.main()
