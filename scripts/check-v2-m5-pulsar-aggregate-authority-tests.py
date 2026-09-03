#!/usr/bin/env python3
"""Fail-closed tests for the focused M5-C Pulsar aggregate authority checker."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-pulsar-aggregate-authority.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PULSAR = load_module(SCRIPT, "nereus_v2_m5_pulsar_aggregate_authority_tests")
BINDING = load_module(
    ROOT / "scripts/check-v2-m5-binding-authority.py", "nereus_v2_m5_binding_for_pulsar_tests"
)


class M5PulsarAggregateAuthorityContractTest(unittest.TestCase):
    def test_accepts_current_implementation(self) -> None:
        PULSAR.validate(ROOT)

    def test_projection_rejects_missing_deleted_selector_binding(self) -> None:
        value = json.loads((ROOT / PULSAR.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["selectorRequirement"] = "DELETED_ONLY"
        with self.assertRaisesRegex(PULSAR.PulsarAggregateAuthorityError, "selectorRequirement"):
            PULSAR.validate_projection_value(changed, BINDING.FLOOR_CLASSES, BINDING.REFERENCE_KINDS)

    def test_projection_rejects_m5d_or_production_authority(self) -> None:
        value = json.loads((ROOT / PULSAR.PROJECTION_PATH).read_text(encoding="utf-8"))
        for field in ("m5DImplementationPresent", "productionAuthority"):
            changed = copy.deepcopy(value)
            changed[field] = True
            with self.assertRaisesRegex(PULSAR.PulsarAggregateAuthorityError, "overstates"):
                PULSAR.validate_projection_value(changed, BINDING.FLOOR_CLASSES, BINDING.REFERENCE_KINDS)

    def test_required_sources_reject_missing_coordinator(self) -> None:
        with self.assertRaisesRegex(PULSAR.PulsarAggregateAuthorityError, "missing/empty"):
            PULSAR.validate_required_sources(ROOT, ("missing/M5PulsarAggregateRetirementCoordinatorV1.java",))


if __name__ == "__main__":
    unittest.main()
