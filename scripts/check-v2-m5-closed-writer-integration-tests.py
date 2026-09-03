#!/usr/bin/env python3
"""Fail-closed tests for the focused M5-C closed writer integration checker."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-closed-writer-integration.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


WRITERS = load_module(SCRIPT, "nereus_v2_m5_closed_writer_integration_tests")
BINDING = load_module(
    ROOT / "scripts/check-v2-m5-binding-authority.py", "nereus_v2_m5_binding_for_writer_tests"
)


class M5ClosedWriterIntegrationContractTest(unittest.TestCase):
    def test_accepts_current_implementation(self) -> None:
        WRITERS.validate(ROOT)

    def test_projection_rejects_missing_reference_kind(self) -> None:
        value = json.loads((ROOT / WRITERS.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["registry"]["referenceKinds"].pop()
        with self.assertRaisesRegex(WRITERS.ClosedWriterIntegrationError, "inventory"):
            WRITERS.validate_projection_value(changed, BINDING.FLOOR_CLASSES, BINDING.REFERENCE_KINDS)

    def test_projection_rejects_external_dispatch_before_ticket_visibility(self) -> None:
        value = json.loads((ROOT / WRITERS.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        order = changed["guardProtocol"]["order"]
        order[1], order[3] = order[3], order[1]
        with self.assertRaisesRegex(WRITERS.ClosedWriterIntegrationError, "mutation order"):
            WRITERS.validate_projection_value(changed, BINDING.FLOOR_CLASSES, BINDING.REFERENCE_KINDS)

    def test_projection_rejects_missing_full_gate_or_production_authority(self) -> None:
        value = json.loads((ROOT / WRITERS.PROJECTION_PATH).read_text(encoding="utf-8"))
        missing = copy.deepcopy(value)
        missing["fullRetirementGatePresent"] = False
        with self.assertRaisesRegex(WRITERS.ClosedWriterIntegrationError, "lacks"):
            WRITERS.validate_projection_value(missing, BINDING.FLOOR_CLASSES, BINDING.REFERENCE_KINDS)
        overstated = copy.deepcopy(value)
        overstated["productionAuthority"] = True
        with self.assertRaisesRegex(WRITERS.ClosedWriterIntegrationError, "overstates"):
            WRITERS.validate_projection_value(overstated, BINDING.FLOOR_CLASSES, BINDING.REFERENCE_KINDS)

    def test_required_sources_reject_missing_guard(self) -> None:
        with self.assertRaisesRegex(WRITERS.ClosedWriterIntegrationError, "missing/empty"):
            WRITERS.validate_required_sources(ROOT, ("missing/M5ReferenceMutationGuardV1.java",))


if __name__ == "__main__":
    unittest.main()
