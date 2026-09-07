#!/usr/bin/env python3
"""Contract tests for the non-promotable M5-A implementation checker."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-materialization.py"


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


M5A = load(SCRIPT, "nereus_m5_materialization_contract_tests")


class M5MaterializationContractTest(unittest.TestCase):
    def test_accepts_current_non_promotable_implementation(self) -> None:
        M5A.validate(ROOT)

    def test_rejects_missing_runtime_source(self) -> None:
        with self.assertRaisesRegex(M5A.MaterializationError, "required sources are missing"):
            M5A.validate_required_sources(ROOT, ("missing.java",))

    def test_rejects_missing_contract_literal(self) -> None:
        with self.assertRaisesRegex(M5A.MaterializationError, "lacks required"):
            M5A.require_literals("REFERENCE_REUSE", ("REFERENCE_REUSE", "REWRITE_GENERATION"), "fixture")

    def test_rejects_broken_design_ancestry(self) -> None:
        with mock.patch.object(M5A.subprocess, "run") as run:
            run.return_value.returncode = 1
            with self.assertRaisesRegex(M5A.MaterializationError, "not an ancestor"):
                M5A.require_design_ancestor(ROOT)

    def test_accepts_exact_machine_readable_wire_projection(self) -> None:
        M5A.validate_projection(ROOT)

    def test_current_index_requires_amended_in_progress_without_evidence_promotion(self) -> None:
        text = (ROOT / "docs/v2/detailed_design/m5/README.md").read_text()
        M5A.validate_current_index(ROOT, text)
        for before, after in (
            ("implementationStatus: InProgress", "implementationStatus: NotStarted"),
            ("evidenceStatus: NotRun", "evidenceStatus: Final"),
            ("m5-current-contracts.md", "missing-current-contracts.md"),
        ):
            with self.subTest(after=after), self.assertRaises(M5A.MaterializationError):
                M5A.validate_current_index(ROOT, text.replace(before, after))


if __name__ == "__main__":
    unittest.main()
