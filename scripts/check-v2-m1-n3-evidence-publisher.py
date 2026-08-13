#!/usr/bin/env python3
"""Deterministic tests for the N3 JUnit-to-canonical-evidence publisher."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import tempfile
import unittest


sys.dont_write_bytecode = True
MODULE_PATH = pathlib.Path(__file__).with_name("publish-v2-m1-n3-evidence.py")
SPEC = importlib.util.spec_from_file_location("n3_publisher", MODULE_PATH)
assert SPEC and SPEC.loader
PUBLISHER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = PUBLISHER
SPEC.loader.exec_module(PUBLISHER)


class N3EvidencePublisherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="nereus-n3-publisher-")
        self.root = pathlib.Path(self.temporary.name)
        self.reports = self.root / "reports"
        self.reports.mkdir()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_report(
        self,
        name: str = "example.Suite",
        tests: int = 3,
        failures: int = 0,
        errors: int = 0,
        skipped: int = 0,
        aborted: int = 0,
    ) -> None:
        (self.reports / f"TEST-{name}.xml").write_text(
            f'<testsuite name="{name}" tests="{tests}" failures="{failures}" errors="{errors}" '
            f'skipped="{skipped}" aborted="{aborted}"/>\n'
        )

    def test_normalizes_one_all_pass_suite_deterministically(self) -> None:
        self.write_report()
        first = PUBLISHER.read_suites(self.root, ("reports",), {"example.Suite"})
        second = PUBLISHER.read_suites(self.root, ("reports",), {"example.Suite"})
        self.assertEqual(first, second)
        row = first["example.Suite"]
        self.assertEqual(3, row.discovered)
        self.assertEqual(3, row.executed)
        self.assertEqual(3, row.passed)
        self.assertEqual(0, row.failures + row.errors + row.skipped + row.aborted)
        self.assertEqual(
            PUBLISHER.normalized_report("V2_M1_FAST", "a" * 64, first),
            PUBLISHER.normalized_report("V2_M1_FAST", "a" * 64, second),
        )

    def test_rejects_zero_tests(self) -> None:
        self.write_report(tests=0)
        with self.assertRaisesRegex(PUBLISHER.EvidenceError, "not all-pass"):
            PUBLISHER.read_suites(self.root, ("reports",), {"example.Suite"})

    def test_rejects_failure_error_skip_and_abort(self) -> None:
        for field in ("failures", "errors", "skipped", "aborted"):
            with self.subTest(field=field):
                for path in self.reports.iterdir():
                    path.unlink()
                self.write_report(tests=4, **{field: 1})
                with self.assertRaisesRegex(PUBLISHER.EvidenceError, "not all-pass"):
                    PUBLISHER.read_suites(self.root, ("reports",), {"example.Suite"})

    def test_rejects_missing_extra_and_duplicate_suites(self) -> None:
        self.write_report()
        with self.assertRaisesRegex(PUBLISHER.EvidenceError, "inventory differs"):
            PUBLISHER.read_suites(self.root, ("reports",), {"other.Suite"})
        (self.reports / "TEST-duplicate.xml").write_text(
            '<testsuite name="example.Suite" tests="1" failures="0" errors="0" skipped="0"/>\n'
        )
        with self.assertRaisesRegex(PUBLISHER.EvidenceError, "duplicate JUnit suite"):
            PUBLISHER.read_suites(self.root, ("reports",), {"example.Suite"})

    def test_gate_result_must_be_canonical_pass_for_exact_tuple(self) -> None:
        gate = self.root / "gate.json"
        expected = {
            "gateId": "V2_M1_FAST",
            "outcome": "PASS",
            "schema": "NEREUS_V2_M1_GATE_RESULT_V1",
            "sourceTupleSha": "a" * 64,
        }
        gate.write_bytes(PUBLISHER.canonical(expected))
        self.assertEqual(gate.read_bytes(), PUBLISHER.verify_gate(gate, "V2_M1_FAST", "a" * 64))
        gate.write_text('{ "gateId": "V2_M1_FAST" }\n')
        with self.assertRaisesRegex(PUBLISHER.EvidenceError, "non-canonical or source-mismatched"):
            PUBLISHER.verify_gate(gate, "V2_M1_FAST", "a" * 64)


if __name__ == "__main__":
    unittest.main(verbosity=2)
