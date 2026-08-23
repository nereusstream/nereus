#!/usr/bin/env python3
"""Positive and fail-closed tests for the current-source M2 regression checker."""

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


CONTRACT = load_module("m3_m2_regression_contract_check_test", CHECKER_PATH)
PUBLISHER = load_module("m3_m2_regression_publisher_check_test", PUBLISHER_PATH)
OUTPUT = PurePosixPath("docs/v2/evidence/v2-m3/w1/m2-regression/receipt.json")


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True, stderr=subprocess.STDOUT
    ).strip()


class CheckerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="nereus-m3-m2-checker-")
        self.base = Path(self.temporary.name)
        self.root = self.base / "repo"
        self.root.mkdir()
        git(self.root, "init", "-b", "main")
        git(self.root, "config", "user.name", "M3 M2 Checker Test")
        git(self.root, "config", "user.email", "m3-m2-checker@example.invalid")
        historical = self.root.joinpath(*CONTRACT.HISTORICAL_FINAL_PATH.parts)
        historical.parent.mkdir(parents=True)
        historical.write_bytes(SOURCE_ROOT.joinpath(*CONTRACT.HISTORICAL_FINAL_PATH.parts).read_bytes())
        (self.root / "src.txt").write_text("current M2 source\n")
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "tested current source")
        self.tested = git(self.root, "rev-parse", "HEAD")
        inputs = self.base / "trusted-results"
        inputs.mkdir()
        children: list[Path] = []
        for index, gate in enumerate(CONTRACT.REQUIRED_GATES, start=1):
            child = {
                "errors": 0,
                "executionProfile": CONTRACT.EXECUTION_PROFILE,
                "failures": 0,
                "gateId": gate,
                "result": "PASS",
                "schema": CONTRACT.CHILD_SCHEMA,
                "skipped": 0,
                "testedNereusCommit": self.tested,
                "tests": index,
            }
            path = inputs / f"{gate}.json"
            path.write_bytes(CONTRACT.canonical_bytes(child))
            children.append(path)
        PUBLISHER.publish(self.root, self.tested, OUTPUT, children)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def receipt_path(self) -> Path:
        return self.root.joinpath(*OUTPUT.parts)

    def commit_evidence(self) -> str:
        git(self.root, "add", str(CONTRACT.EVIDENCE_PREFIX))
        git(self.root, "commit", "-m", "publish current-source M2 regression")
        return git(self.root, "rev-parse", "HEAD")

    def test_accepts_uncommitted_receipt_at_exact_current_source(self) -> None:
        head, descendants = CONTRACT.validate_receipt(self.root, OUTPUT, self.tested)
        self.assertEqual(self.tested, head)
        self.assertEqual(0, descendants)

    def test_cli_accepts_exact_current_source(self) -> None:
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                str(CHECKER_PATH),
                "--repo-root",
                str(self.root),
                "--receipt",
                str(OUTPUT),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("childGates=25 promotionEligible=false scenarioPromotion=false", result.stdout)

    def test_accepts_only_evidence_descendant(self) -> None:
        evidence_head = self.commit_evidence()
        head, descendants = CONTRACT.validate_receipt(self.root, OUTPUT, self.tested)
        self.assertEqual(evidence_head, head)
        self.assertEqual(1, descendants)

    def test_rejects_non_evidence_descendant(self) -> None:
        self.commit_evidence()
        (self.root / "src.txt").write_text("changed after regression\n")
        git(self.root, "add", "src.txt")
        git(self.root, "commit", "-m", "change production source")
        with self.assertRaisesRegex(CONTRACT.RegressionError, "non-evidence path changed"):
            CONTRACT.validate_receipt(self.root, OUTPUT, self.tested)

    def test_rejects_attachment_tamper(self) -> None:
        target = self.receipt_path().parent / "attachments" / "KAFKA_K0.json"
        target.write_bytes(target.read_bytes() + b"\n")
        with self.assertRaisesRegex(CONTRACT.RegressionError, "attachment bytes/SHA differ"):
            CONTRACT.validate_receipt(self.root, OUTPUT, self.tested)

    def test_rejects_noncanonical_receipt(self) -> None:
        self.receipt_path().write_bytes(self.receipt_path().read_bytes() + b"\n")
        with self.assertRaisesRegex(CONTRACT.RegressionError, "not exact closed-domain JCS"):
            CONTRACT.validate_receipt(self.root, OUTPUT, self.tested)

    def test_rejects_wrong_expected_tested_commit(self) -> None:
        wrong = "f" * 40
        with self.assertRaisesRegex(CONTRACT.RegressionError, "not an ancestor of HEAD"):
            CONTRACT.validate_receipt(self.root, OUTPUT, wrong)

    def test_rejects_promotion_or_nonempty_amendment_lineage(self) -> None:
        for field, value, message in (
            ("scenarioPromotion", True, "promotion boundary differs"),
            ("m2AmendmentLineage", [{"amendmentId": "implicit"}], "no implicit M2 Amendment"),
        ):
            with self.subTest(field=field):
                receipt = CONTRACT.load_canonical_json(self.receipt_path().read_bytes(), "receipt")
                receipt[field] = value
                self.receipt_path().write_bytes(CONTRACT.canonical_bytes(receipt))
                with self.assertRaisesRegex(CONTRACT.RegressionError, message):
                    CONTRACT.validate_receipt(self.root, OUTPUT, self.tested)
                receipt[field] = False if field == "scenarioPromotion" else []
                self.receipt_path().write_bytes(CONTRACT.canonical_bytes(receipt))


if __name__ == "__main__":
    unittest.main(verbosity=2)
