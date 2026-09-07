#!/usr/bin/env python3
"""Fail-closed tests for the M5-D target authority coordinator checker."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-target-delete-authority-coordinator.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECK = load_module(SCRIPT, "nereus_v2_m5_target_delete_authority_coordinator_tests")


class M5TargetDeleteAuthorityCoordinatorContractTest(unittest.TestCase):
    def test_accepts_current_coordinator(self) -> None:
        CHECK.validate(ROOT)

    def test_projection_rejects_multi_key_operation(self) -> None:
        value = json.loads((ROOT / CHECK.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["metadataOperation"] = "conditionalTransaction"
        with self.assertRaisesRegex(CHECK.TargetAuthorityCoordinatorError, "metadata operation differs"):
            CHECK.validate_projection_value(changed)

    def test_projection_rejects_timeout_ticket_clear(self) -> None:
        value = json.loads((ROOT / CHECK.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["writerGuard"]["timeoutClearsTicket"] = True
        with self.assertRaisesRegex(CHECK.TargetAuthorityCoordinatorError, "writer guard projection differs"):
            CHECK.validate_projection_value(changed)

    def test_projection_rejects_unproven_runtime_or_authority(self) -> None:
        value = json.loads((ROOT / CHECK.PROJECTION_PATH).read_text(encoding="utf-8"))
        for field in (
            "allProofChangingWritersIntegrated",
            "externalDeleteCompositionPresent",
            "realOxiaExecutionPresent",
            "physicalDeleteAuthority",
            "productionAuthority",
        ):
            changed = copy.deepcopy(value)
            changed[field] = True
            with self.assertRaisesRegex(CHECK.TargetAuthorityCoordinatorError, "overstates"):
                CHECK.validate_projection_value(changed)

    def test_sources_reject_missing_writer_guard(self) -> None:
        original = CHECK.REQUIRED_SOURCES
        try:
            CHECK.REQUIRED_SOURCES = ("missing/M5TargetDeleteWriterGuardV1.java",)
            with self.assertRaisesRegex(CHECK.TargetAuthorityCoordinatorError, "missing/empty"):
                CHECK.validate_sources(ROOT)
        finally:
            CHECK.REQUIRED_SOURCES = original


if __name__ == "__main__":
    unittest.main()
