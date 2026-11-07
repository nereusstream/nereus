#!/usr/bin/env python3
"""Exercise M6 completeness and native result rejection against the live scenario registry."""

import importlib.util
from pathlib import Path
import tempfile
import time
import unittest


spec = importlib.util.spec_from_file_location(
    "m6_acceptance", Path(__file__).with_name("run-v2-m6-check.py")
)
M6 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M6)


class M6CoverageTest(unittest.TestCase):
    def test_whole_scenario_union_cannot_hide_an_unexercised_profile(self):
        claims = {name: sorted(rows) for name, rows in M6.COVERAGE.REQUIRED.items()}
        self.assertEqual(25, len(M6.COVERAGE.validate_profiles(M6.ROOT, claims)))
        claims["kafka-bookkeeper-wal-only"].remove("V2-KAF-DATA-017")
        self.assertEqual(25, len(set().union(*claims.values())))
        with self.assertRaisesRegex(M6.COVERAGE.CoverageError, "kafka-bookkeeper-wal-only"):
            M6.COVERAGE.validate_profiles(M6.ROOT, claims)

    def test_wrong_source_and_skipped_native_case_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            started = time.time_ns()
            profile = "kafka-bookkeeper-wal-only"
            test = b'<testsuite name="Suite" tests="1" failures="0" errors="0" skipped="1">' \
                   b'<testcase name="required()" classname="Suite" time="0"><skipped/></testcase></testsuite>'
            (root / "suite.xml").write_bytes(test)
            facts = b'{"example":"real-process-contract-fixture"}\n'
            (root / "backend.json").write_bytes(facts)
            M6.write(root / "profile-result.json", {
                "schema": "NEREUS_V2_M6_NATIVE_PROFILE_RESULT_V1", "profile": profile,
                "inputRootSha256": "a" * 64, "junit": [{
                    "path": "suite.xml", "task": ":core:nativeTest",
                    "bytes": len(test), "sha256": M6.SOURCE.INPUTS.digest(test),
                }],
                "backendFacts": {
                    "path": "backend.json", "bytes": len(facts), "sha256": M6.SOURCE.INPUTS.digest(facts),
                },
            })
            cases = {":core:nativeTest|Suite|required()": ["V2-KAF-PROC-001"]}
            with self.assertRaisesRegex(M6.SOURCE.InputError, "wrong execution inputs"):
                M6.verify_native_result(root, started, profile, cases, "b" * 64)
            with self.assertRaisesRegex(M6.JUNIT.JUnitEvidenceError, "skipped"):
                M6.verify_native_result(root, started, profile, cases, "a" * 64)


if __name__ == "__main__":
    unittest.main()
