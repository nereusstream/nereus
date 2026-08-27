#!/usr/bin/env python3
"""Positive and negative tests for the current-source M2 regression publisher."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tempfile
import unittest


SOURCE_ROOT = Path(__file__).resolve().parent.parent
CHECKER_PATH = Path(__file__).with_name("check-v2-m3-m2-regression.py")
PUBLISHER_PATH = Path(__file__).with_name("publish-v2-m3-m2-regression.py")


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


CONTRACT = load_module("m3_m2_regression_contract_publish_test", CHECKER_PATH)
PUBLISHER = load_module("m3_m2_regression_publisher_test", PUBLISHER_PATH)
OUTPUT = PurePosixPath("docs/v2/evidence/v2-m3/w1/m2-regression/receipt.json")


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True, stderr=subprocess.STDOUT
    ).strip()


class PublisherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="nereus-m3-m2-publisher-")
        self.base = Path(self.temporary.name)
        self.root = self.base / "repo"
        self.root.mkdir()
        git(self.root, "init", "-b", "main")
        git(self.root, "config", "user.name", "M3 M2 Publisher Test")
        git(self.root, "config", "user.email", "m3-m2-publisher@example.invalid")
        git(self.root, "config", "commit.gpgsign", "false")
        historical = self.root.joinpath(*CONTRACT.HISTORICAL_FINAL_PATH.parts)
        historical.parent.mkdir(parents=True)
        historical.write_bytes(SOURCE_ROOT.joinpath(*CONTRACT.HISTORICAL_FINAL_PATH.parts).read_bytes())
        (self.root / "src.txt").write_text("current M2 source\n")
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "tested current source")
        self.tested = git(self.root, "rev-parse", "HEAD")
        self.input_root = self.base / "trusted-results"
        self.input_root.mkdir()
        self.children = self.write_children()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def child_value(self, gate: str, tests: int = 1) -> dict:
        return {
            "errors": 0,
            "executionProfile": CONTRACT.EXECUTION_PROFILE,
            "failures": 0,
            "gateId": gate,
            "result": "PASS",
            "schema": CONTRACT.CHILD_SCHEMA,
            "skipped": 0,
            "testedNereusCommit": self.tested,
            "tests": tests,
        }

    def write_children(self) -> list[Path]:
        paths: list[Path] = []
        for index, gate in enumerate(CONTRACT.REQUIRED_GATES, start=1):
            path = self.input_root / f"{gate}.json"
            path.write_bytes(CONTRACT.canonical_bytes(self.child_value(gate, index)))
            paths.append(path)
        return paths

    def test_publishes_complete_non_promotable_receipt(self) -> None:
        raw = PUBLISHER.publish(self.root, self.tested, OUTPUT, self.children)
        receipt_path = self.root.joinpath(*OUTPUT.parts)
        self.assertEqual(raw, receipt_path.read_bytes())
        self.assertEqual(raw, CONTRACT.canonical_bytes(CONTRACT.load_canonical_json(raw, "receipt")))
        receipt = CONTRACT.load_canonical_json(raw, "receipt")
        self.assertEqual(CONTRACT.RECEIPT_RESULT, receipt["result"])
        self.assertFalse(receipt["promotionEligible"])
        self.assertFalse(receipt["scenarioPromotion"])
        self.assertEqual([], receipt["m2AmendmentLineage"])
        self.assertEqual(len(CONTRACT.REQUIRED_GATES), len(receipt["childGates"]))
        CONTRACT.validate_receipt(self.root, OUTPUT, self.tested)

    def test_cli_publishes_only_from_explicit_trusted_results(self) -> None:
        command = [
            sys.executable,
            "-B",
            str(PUBLISHER_PATH),
            "--repo-root",
            str(self.root),
            "--tested-commit",
            self.tested,
            "--output",
            str(OUTPUT),
        ]
        for child in self.children:
            command.extend(["--child-result", str(child)])
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        result = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("childGates=25 promotionEligible=false scenarioPromotion=false", result.stdout)

    def test_rejects_dirty_repository_before_writing(self) -> None:
        (self.root / "dirty.txt").write_text("dirty\n")
        with self.assertRaisesRegex(PUBLISHER.CONTRACT.RegressionError, "clean exact HEAD"):
            PUBLISHER.publish(self.root, self.tested, OUTPUT, self.children)

    def test_rejects_fast_profile_result(self) -> None:
        child = CONTRACT.load_canonical_json(self.children[0].read_bytes(), "child")
        child["executionProfile"] = "FAST_CURRENT_SOURCE_M2"
        self.children[0].write_bytes(CONTRACT.canonical_bytes(child))
        with self.assertRaisesRegex(
            PUBLISHER.CONTRACT.RegressionError, "identity/profile/source differs"
        ):
            PUBLISHER.publish(self.root, self.tested, OUTPUT, self.children)

    def test_rejects_missing_full_profile_gate(self) -> None:
        with self.assertRaisesRegex(
            PUBLISHER.CONTRACT.RegressionError, "exactly 25 explicit trusted child"
        ):
            PUBLISHER.publish(self.root, self.tested, OUTPUT, self.children[:-1])

    def test_rejects_zero_tests_or_skip(self) -> None:
        for counter, value, message in (("tests", 0, "zero tests"), ("skipped", 1, "non-zero skipped")):
            with self.subTest(counter=counter):
                child = self.child_value(CONTRACT.REQUIRED_GATES[0])
                child[counter] = value
                self.children[0].write_bytes(CONTRACT.canonical_bytes(child))
                with self.assertRaisesRegex(PUBLISHER.CONTRACT.RegressionError, message):
                    PUBLISHER.publish(self.root, self.tested, OUTPUT, self.children)
                self.children[0].write_bytes(
                    CONTRACT.canonical_bytes(self.child_value(CONTRACT.REQUIRED_GATES[0]))
                )

    def test_refuses_to_overwrite_existing_evidence(self) -> None:
        PUBLISHER.publish(self.root, self.tested, OUTPUT, self.children)
        git(self.root, "add", str(CONTRACT.EVIDENCE_PREFIX))
        git(self.root, "commit", "-m", "commit regression evidence")
        self.tested = git(self.root, "rev-parse", "HEAD")
        self.children = self.write_children()
        with self.assertRaisesRegex(PUBLISHER.CONTRACT.RegressionError, "refuses to overwrite evidence"):
            PUBLISHER.publish(self.root, self.tested, OUTPUT, self.children)


if __name__ == "__main__":
    unittest.main(verbosity=2)
