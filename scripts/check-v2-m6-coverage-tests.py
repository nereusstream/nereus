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
            (root / "kafka-process-logs").mkdir()
            for name in ("backend.json", "runtime.json", "development-repository.json", "kafka-process-logs/server.log"):
                (root / name).write_text('{"example":"native-process-contract-fixture"}\n')
            sources = {
                "nereus": {"commit": "a" * 40, "tree": "b" * 40},
                "kafka": {"commit": "c" * 40, "tree": "d" * 40},
            }
            M6.write(root / "process-result.json", {
                "kind": "FOCUSED_KAFKA_BK_PROCESS_CHECK", "promotionEligible": False,
                "sourceCheckout": {repo: row["commit"] for repo, row in sources.items()},
                "sourceTrees": {repo: row["tree"] for repo, row in sources.items()},
                "junit": {"suite.xml": {
                    "bytes": len(test), "sha256": M6.SOURCE.INPUTS.digest(test),
                }},
                "backendFiles": {"backend.json": M6.SOURCE.file_identity(root / "backend.json")},
                "processLogs": {"server.log": M6.SOURCE.file_identity(root / "kafka-process-logs/server.log")},
                "kafkaRuntime": {"path": "runtime.json", **M6.SOURCE.file_identity(root / "runtime.json")},
                "developmentRepositorySha256": M6.SOURCE.file_identity(root / "development-repository.json")["sha256"],
            })
            cases = {":core:nereusBookKeeperOwnerRelocationRealTest|Suite|required()": ["V2-KAF-PROC-001"]}
            with self.assertRaisesRegex(M6.SOURCE.InputError, "wrong execution inputs"):
                M6.verify_kafka_result(root, started, profile, cases, {})
            with self.assertRaisesRegex(M6.JUNIT.JUnitEvidenceError, "skipped"):
                M6.verify_kafka_result(root, started, profile, cases, sources)

    def test_pulsar_process_requires_fresh_raw_attachments_and_exact_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            started = time.time_ns()
            profile = "BOOKKEEPER_WAL_ONLY"
            kind = "BK"
            sources = {
                "nereus": {"commit": "a" * 40, "tree": "b" * 40},
                "pulsar": {"commit": "c" * 40, "tree": "d" * 40},
            }
            backend = root / "backend.json"
            backend.write_text('{"bookies":3}\n')
            launch = root / "broker.conf"
            launch.write_text("managedLedgerDefaultEnsembleSize=3\n")
            M6.write(root / "process-result.json", {
                "schema": "NEREUS_V2_M6_PULSAR_BK_FOCUSED_PROCESS_V1",
                "profile": profile, "status": "PASS", "promotionEligible": False,
                "sources": sources, "checks": ["official-client"],
                "outputs": {backend.name: M6.SOURCE.file_identity(backend)},
                "generatedLaunchInputs": {launch.name: M6.SOURCE.file_identity(launch)},
            })
            expected = {"official-client": ["V2-PUL-PROC-001"]}
            self.assertEqual(1, M6.verify_pulsar_process(root, started, kind, profile, expected, sources)["checks"])
            with self.assertRaisesRegex(M6.SOURCE.InputError, "frozen source"):
                M6.verify_pulsar_process(root, started, kind, "OBJECT_WAL", expected, sources)
            with self.assertRaisesRegex(M6.SOURCE.InputError, "check inventory differs"):
                M6.verify_pulsar_process(root, started, kind, profile, {"different-cut": []}, sources)
            launch.write_text("managedLedgerDefaultEnsembleSize=1\n")
            with self.assertRaisesRegex(M6.SOURCE.InputError, "attachment differs"):
                M6.verify_pulsar_process(root, started, kind, profile, expected, sources)

    def test_actual_runtime_file_must_match_the_frozen_declaration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            jar = root / "provider.jar"
            jar.write_bytes(b"original provider")
            observed = M6.SOURCE.file_identity(jar)
            frozen = {"runtimeInputs": {}}
            with self.assertRaisesRegex(M6.SOURCE.InputError, "was not frozen"):
                M6.verify_runtime_file(jar, observed, {}, root / "output", frozen, {})
            frozen["runtimeInputs"]["provider"] = {"path": str(jar), **observed}
            M6.verify_runtime_file(jar, observed, {}, root / "output", frozen, {})
            jar.write_bytes(b"changed provider")
            with self.assertRaisesRegex(M6.SOURCE.InputError, "bytes changed"):
                M6.verify_runtime_file(jar, observed, {}, root / "output", frozen, {})


if __name__ == "__main__":
    unittest.main()
