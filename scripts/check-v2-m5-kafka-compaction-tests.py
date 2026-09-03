#!/usr/bin/env python3
"""Fail-closed contract tests for the non-promotable M5-B implementation checker."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-kafka-compaction.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


M5B = load_module(SCRIPT, "nereus_m5_b_contract_tests")


class M5BContractTest(unittest.TestCase):
    def test_accepts_current_non_promotable_implementation(self) -> None:
        M5B.validate(ROOT)

    def test_rejects_missing_production_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(M5B.KafkaCompactionError, "missing/empty"):
                M5B.validate_required_sources(Path(directory), (M5B.REQUIRED_SOURCES[0],))

    def test_rejects_incomplete_index_projection(self) -> None:
        value = json.loads((ROOT / M5B.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["rebuiltIndexes"].remove("ABORTED_TRANSACTION")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / M5B.PROJECTION_PATH
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(changed), encoding="utf-8")
            with self.assertRaisesRegex(M5B.KafkaCompactionError, "index set differs"):
                M5B.validate_projection(root)

    def test_rejects_weakened_fallback_suppression(self) -> None:
        with self.assertRaisesRegex(M5B.KafkaCompactionError, "lacks required"):
            M5B.require_literals("allowFromFallback", ("allowFromFallback", "suppressionRoot"), "fixture")

    def test_rejects_unlocked_kafka_dependency(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "nereus-kafka-bookkeeper").mkdir()
            (root / "nereus-kafka-bookkeeper/build.gradle.kts").write_text("dependencies {}", encoding="utf-8")
            (root / "build.gradle.kts").write_text("", encoding="utf-8")
            with self.assertRaisesRegex(M5B.KafkaCompactionError, "dependency"):
                M5B.validate_tasks_and_dependency(root)


if __name__ == "__main__":
    unittest.main()
