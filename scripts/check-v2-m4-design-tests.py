#!/usr/bin/env python3
"""Fail-closed contract tests for the governance-only M4 design hard-freeze."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m4-design.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


M4 = load_module(SCRIPT, "nereus_v2_m4_design_contract_tests")


class M4DesignContractTest(unittest.TestCase):
    def test_accepts_current_design_freeze(self) -> None:
        M4.validate(ROOT)

    def manifest_fixture(self, root: Path) -> tuple[list[PurePosixPath], dict[str, object]]:
        paths = [PurePosixPath("design/a.md"), PurePosixPath("review/b.md")]
        for path, body in zip(paths, (b"a\n", b"b\n"), strict=True):
            (root / path).parent.mkdir(parents=True, exist_ok=True)
            (root / path).write_bytes(body)
        rows = [
            {"path": str(path), "sha256": hashlib.sha256((root / path).read_bytes()).hexdigest()}
            for path in paths
        ]
        return paths, {"schema": M4.SCHEMA, "result": M4.RESULT, "documents": rows}

    def test_manifest_rejects_reordered_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths, value = self.manifest_fixture(root)
            value["documents"] = list(reversed(value["documents"]))
            with self.assertRaisesRegex(M4.DesignError, "not sorted"):
                M4.validate_manifest_value(root, value, paths)

    def test_manifest_rejects_changed_bound_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths, value = self.manifest_fixture(root)
            (root / paths[0]).write_bytes(b"changed\n")
            with self.assertRaisesRegex(M4.DesignError, "SHA differs"):
                M4.validate_manifest_value(root, value, paths)

    def test_manifest_rejects_path_escape(self) -> None:
        with self.assertRaisesRegex(M4.DesignError, "safe-relative"):
            M4.safe_relative("../outside.md", "fixture")

    def test_review_record_requires_one_complete_response(self) -> None:
        valid = f"before\n{M4.BEGIN_REVIEW}\nanswer\n{M4.END_REVIEW}\nafter\n"
        M4.validate_review_record(valid, "fixture")
        with self.assertRaisesRegex(M4.DesignError, "exactly one"):
            M4.validate_review_record(valid + M4.BEGIN_REVIEW, "fixture")
        with self.assertRaisesRegex(M4.DesignError, "exactly one"):
            M4.validate_review_record(valid.replace(M4.END_REVIEW, ""), "fixture")

    def test_design_front_matter_rejects_proposed(self) -> None:
        text = (
            "---\nproductLine: V2\ndesignStatus: Proposed\nimplementationStatus: NotStarted\n"
            "evidenceStatus: NotRun\nsourceTuple: v2-m1\n---\n"
        )
        with self.assertRaisesRegex(M4.DesignError, "designStatus differs"):
            M4.validate_design_document(text, "fixture")

    def test_scenario_rejects_premature_m4_pass(self) -> None:
        value = json.loads((ROOT / M4.SCENARIO_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        row = next(row for row in changed["scenarios"] if row["id"] == "V2-READ-001")
        row["status"] = "PASSED_CURRENT_SOURCE"
        row["evidenceReceipt"] = "docs/v2/evidence/v2-m4/final.json"
        with self.assertRaisesRegex(M4.DesignError, "PLANNED/null"):
            M4.validate_scenarios_value(changed)

    def test_scenario_rejects_changed_existing_m2_receipt(self) -> None:
        value = json.loads((ROOT / M4.SCENARIO_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        row = next(row for row in changed["scenarios"] if row["id"] == "V2-BK-010")
        row["evidenceReceipt"] = None
        with self.assertRaisesRegex(M4.DesignError, "existing M2 receipt authority"):
            M4.validate_scenarios_value(changed)

    def test_build_rejects_placeholder_runtime_gate(self) -> None:
        with self.assertRaisesRegex(M4.DesignError, "placeholder v2M4Check"):
            M4.validate_build(
                'tasks.register("v2M4DesignCheck") {}\n'
                'tasks.register("v2M4Check") {}\n'
            )

    def test_no_evidence_check_rejects_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / M4.EVIDENCE_PREFIX / "receipt.json"
            path.parent.mkdir(parents=True)
            path.write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(M4.DesignError, "evidence/receipt path is non-empty"):
                M4.validate_no_m4_evidence(root)


if __name__ == "__main__":
    unittest.main()
