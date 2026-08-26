#!/usr/bin/env python3
"""Contract tests for the serial exact-source M3 native evidence runner."""

from __future__ import annotations

from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent.parent
RUNNER = ROOT / "scripts/run-v2-m3-native-evidence.sh"
BUILD = ROOT / "build.gradle.kts"


class NativeEvidenceRunnerContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runner = RUNNER.read_text()
        cls.build = BUILD.read_text()

    def test_shell_is_syntactically_closed(self) -> None:
        subprocess.run(["bash", "-n", str(RUNNER)], check=True)
        self.assertTrue(RUNNER.stat().st_mode & 0o100)

    def test_runner_reads_only_m3_native_coordinates(self) -> None:
        for token in (
            ".m3NativeEvidenceBindings.${name}.repository",
            ".m3NativeEvidenceBindings.${name}.branch",
            ".m3NativeEvidenceBindings.${name}.sourceCommit",
            ".m3NativeEvidenceBindings.kafka.logicalRepository",
            ".m3NativeEvidenceBindings.kafka.sourceCommit",
            ".m3NativeEvidenceBindings.pulsar.logicalRepository",
            ".m3NativeEvidenceBindings.pulsar.sourceCommit",
        ):
            self.assertIn(token, self.runner)
        self.assertNotIn("k1KafkaAuthorityBinding", self.runner)
        self.assertNotIn("m2PulsarNativeBinding", self.runner)

    def test_runner_fails_closed_on_source_and_remote_drift(self) -> None:
        for token in (
            "git rev-parse origin/main",
            "git ls-remote --heads origin refs/heads/main",
            "status --porcelain=v1 --untracked-files=all",
            "refs/remotes/origin/$expected_branch",
            "remote get-url origin",
            "fresh output directory already exists",
            "output must be outside the repository",
        ):
            self.assertIn(token, self.runner)

    def test_runner_executes_and_seals_both_closed_native_profiles(self) -> None:
        for token in (
            ":$module:cleanTest",
            "--rerun-tasks",
            ":$module:test",
            ":$module:spotlessCheck",
            ":$module:checkstyleMain",
            ":$module:checkstyleTest",
            "v2M3KafkaNativeReceiptEmit",
            "v2M3KafkaNativeReceiptCheck",
            "v2M3PulsarNativeReceiptEmit",
            "v2M3PulsarNativeReceiptCheck",
            "--seal-native-kind",
            "--seal-junit-kind",
            "U_KAFKA_OBJECT_WAL",
            "P_PULSAR_OBJECT_WAL",
        ):
            self.assertIn(token, self.runner)

    def test_root_gate_requires_explicit_exact_source_inputs(self) -> None:
        task_start = self.build.index('tasks.register<Exec>("v2M3NativeEvidenceCheck")')
        task = self.build[task_start : task_start + 900]
        for token in (
            "usesService(m3NestedGradleGate)",
            "v2M3TestedCommit.get()",
            "v2M3KafkaEvidenceWorktree.get()",
            "v2M3PulsarEvidenceWorktree.get()",
            "v2M3NativeEvidenceOutputDirectory.get()",
            "scripts/run-v2-m3-native-evidence.sh",
        ):
            self.assertIn(token, task)


if __name__ == "__main__":
    unittest.main(verbosity=2)
