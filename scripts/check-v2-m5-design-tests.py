#!/usr/bin/env python3
"""Fail-closed contract tests for the governance-only M5 design hard-freeze."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-design.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


M5 = load_module(SCRIPT, "nereus_v2_m5_design_contract_tests")


class M5DesignContractTest(unittest.TestCase):
    def test_accepts_current_design_freeze(self) -> None:
        manifest = M5.load_json(M5.read_bytes(ROOT, M5.MANIFEST_PATH), str(M5.MANIFEST_PATH))
        M5.validate_manifest_value(ROOT, manifest)
        M5.validate_m4_final(ROOT)
        M5.validate_scenarios_value(
            M5.load_json(M5.read_bytes(ROOT, M5.SCENARIO_PATH), str(M5.SCENARIO_PATH))
        )
        M5.validate_build(M5.read_text(ROOT, M5.BUILD_PATH))

    @staticmethod
    def manifest_fixture(root: Path) -> tuple[list[PurePosixPath], dict[str, object]]:
        paths = [PurePosixPath("design/a.md"), PurePosixPath("design/b.md")]
        for path, body in zip(paths, (b"a\n", b"b\n"), strict=True):
            (root / path).parent.mkdir(parents=True, exist_ok=True)
            (root / path).write_bytes(body)
        rows = [
            {"path": str(path), "sha256": hashlib.sha256((root / path).read_bytes()).hexdigest()}
            for path in paths
        ]
        return paths, {"schema": M5.SCHEMA, "result": M5.RESULT, "documents": rows}

    def test_manifest_rejects_reordered_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths, value = self.manifest_fixture(root)
            value["documents"] = list(reversed(value["documents"]))
            with self.assertRaisesRegex(M5.DesignError, "not sorted"):
                M5.validate_manifest_value(root, value, paths)

    def test_manifest_rejects_changed_bound_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths, value = self.manifest_fixture(root)
            (root / paths[0]).write_bytes(b"changed\n")
            with self.assertRaisesRegex(M5.DesignError, "SHA differs"):
                M5.validate_manifest_value(root, value, paths)

    def test_manifest_rejects_path_escape(self) -> None:
        with self.assertRaisesRegex(M5.DesignError, "safe-relative"):
            M5.safe_relative("../outside.md", "fixture")

    def test_design_front_matter_rejects_proposed_or_started(self) -> None:
        proposed = (
            "---\nproductLine: V2\ndesignStatus: Proposed\nimplementationStatus: NotStarted\n"
            "evidenceStatus: NotRun\nsourceTuple: v2-m1\n---\n"
        )
        with self.assertRaisesRegex(M5.DesignError, "designStatus differs"):
            M5.validate_design_document(proposed, "fixture")
        started = proposed.replace("designStatus: Proposed", "designStatus: Accepted").replace(
            "implementationStatus: NotStarted", "implementationStatus: InProgress"
        )
        with self.assertRaisesRegex(M5.DesignError, "implementationStatus differs"):
            M5.validate_design_document(started, "fixture")

    def test_scenario_rejects_premature_m5_pass(self) -> None:
        value = json.loads((ROOT / M5.SCENARIO_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        row = next(row for row in changed["scenarios"] if row["id"] == "V2-READ-002")
        row["status"] = "PASSED_CURRENT_SOURCE"
        row["evidenceReceipt"] = "docs/v2/evidence/v2-m5/final.json"
        with self.assertRaisesRegex(M5.DesignError, "PLANNED/null"):
            M5.validate_scenarios_value(changed)

    def test_scenario_rejects_changed_milestone_or_added_m5_row(self) -> None:
        value = json.loads((ROOT / M5.SCENARIO_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        row = next(row for row in changed["scenarios"] if row["id"] == "V2-KAF-DATA-022")
        row["milestone"] = "M5"
        with self.assertRaisesRegex(M5.DesignError, "PLANNED/null exact milestone"):
            M5.validate_scenarios_value(changed)
        changed = copy.deepcopy(value)
        row = next(row for row in changed["scenarios"] if row["id"] == "V2-OBJ-001")
        row["milestone"] = "M3/M5"
        with self.assertRaisesRegex(M5.DesignError, "M5 scenario set differs"):
            M5.validate_scenarios_value(changed)

    def test_scenario_rejects_changed_existing_m4_receipt(self) -> None:
        value = json.loads((ROOT / M5.SCENARIO_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        row = next(row for row in changed["scenarios"] if row["id"] == "V2-READ-001")
        row["evidenceReceipt"] = None
        with self.assertRaisesRegex(M5.DesignError, "existing M4 receipt authority"):
            M5.validate_scenarios_value(changed)

    def test_build_rejects_future_runtime_and_final_tasks(self) -> None:
        base = (
            'tasks.register<Exec>("v2M5HistoricalM4DependencyContractTest") {}\n'
            'tasks.register<Exec>("v2M5HistoricalM4DependencyCheck") {}\n'
            'tasks.register<Exec>("v2M5DesignContractTest") {}\n'
            'tasks.register<Exec>("v2M5DesignSourceCheck") {}\n'
            'tasks.register("v2M5DesignCheck") {\n'
            '  dependsOn("v2DocumentationCheck", "v2M5HistoricalM4DependencyContractTest", '
            '"v2M5HistoricalM4DependencyCheck", "v2M5DesignContractTest", '
            '"v2M5DesignSourceCheck")\n}\n'
        )
        M5.validate_build(base)
        for task in ("v2M5MaterializationCheck", "v2M5EvidenceExecutionCheck", "v2M5Check"):
            with self.subTest(task=task), self.assertRaisesRegex(M5.DesignError, "forbidden pre-implementation"):
                M5.validate_build(base + f'tasks.register("{task}") {{}}\n')

    def test_no_evidence_check_rejects_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / M5.EVIDENCE_PREFIX / "receipt.json"
            path.parent.mkdir(parents=True)
            path.write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(M5.DesignError, "evidence/receipt path is non-empty"):
                M5.validate_no_m5_evidence(root)

    def test_settings_rejects_new_materialization_module(self) -> None:
        M5.validate_settings('include("nereus-storage-object")\n')
        with self.assertRaisesRegex(M5.DesignError, "must not add"):
            M5.validate_settings('include("nereus-materialization")\n')

    def test_runtime_marker_rejects_m5_main_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "nereus-storage-object/src/main/java/example/M5GcCoordinator.java"
            path.parent.mkdir(parents=True)
            path.write_text("final class M5GcCoordinator {}\n", encoding="utf-8")
            with self.assertRaisesRegex(M5.DesignError, "runtime marker exists"):
                M5.validate_no_m5_runtime(root)

    def test_open_questions_requires_permanent_baseline(self) -> None:
        current = M5.read_text(ROOT, M5.OPEN_PATH)
        M5.validate_open_questions(current)
        with self.assertRaisesRegex(M5.DesignError, "lacks frozen literals"):
            M5.validate_open_questions(current.replace("Tombstone deletion remains evidence-blocked.", ""))


if __name__ == "__main__":
    unittest.main()
