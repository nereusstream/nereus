#!/usr/bin/env python3
"""Positive and fail-closed negative tests for check-v2-m3-inputs.py."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check-v2-m3-inputs.py")
SOURCE_ROOT = SCRIPT.parent.parent
SPEC = importlib.util.spec_from_file_location("check_v2_m3_inputs", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def git(root: Path, *args: str, input_text: str | None = None) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *args],
        input=input_text,
        text=True,
        stderr=subprocess.STDOUT,
    ).strip()


class M3InputsGateTest(unittest.TestCase):
    def clone_repository(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        temporary = tempfile.TemporaryDirectory(prefix="nereus-m3-inputs-")
        root = Path(temporary.name) / "repo"
        subprocess.check_call(
            ["git", "clone", "--quiet", "--shared", str(SOURCE_ROOT), str(root)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return temporary, root

    def run_check(self, root: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), str(root)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def assert_rejected(self, root: Path, message: str) -> None:
        result = self.run_check(root)
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(message, result.stderr)

    def test_accepts_exact_historical_m2_input(self) -> None:
        result = self.run_check(SOURCE_ROOT)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn(f"tested={MODULE.TESTED_SOURCE_COMMIT}", result.stdout)
        self.assertIn(f"published={MODULE.PUBLISHED_SOURCE_COMMIT}", result.stdout)
        self.assertIn("children=2 scenarios=21 exclusions=M3/M6/M8", result.stdout)

    def test_accepts_current_source_lock_advancement(self) -> None:
        temporary, root = self.clone_repository()
        self.addCleanup(temporary.cleanup)
        target = root.joinpath(*MODULE.SOURCE_LOCKS_PATH.parts)
        target.write_text('{"sourceTupleId":"v2-m3"}\n')
        result = self.run_check(root)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn(f"sourceLocksSha256={MODULE.SOURCE_LOCKS_SHA256}", result.stdout)

    def test_rejects_tampered_root_bytes(self) -> None:
        temporary, root = self.clone_repository()
        self.addCleanup(temporary.cleanup)
        target = root.joinpath(*MODULE.ROOT_PATH.parts)
        target.write_bytes(target.read_bytes() + b" ")
        self.assert_rejected(root, "worktree historical input bytes differ")

    def test_rejects_tampered_child_bytes(self) -> None:
        temporary, root = self.clone_repository()
        self.addCleanup(temporary.cleanup)
        target = root.joinpath(*MODULE.KAFKA_PATH.parts)
        target.write_bytes(target.read_bytes() + b"\n")
        self.assert_rejected(root, "worktree historical input bytes differ")

    def test_rejects_head_before_published_final(self) -> None:
        temporary, root = self.clone_repository()
        self.addCleanup(temporary.cleanup)
        git(root, "checkout", "--quiet", f"{MODULE.PUBLISHED_SOURCE_COMMIT}^")
        self.assert_rejected(root, "published M2 Final commit is not an ancestor of")

    def test_rejects_replaced_historical_source_lock_blob(self) -> None:
        temporary, root = self.clone_repository()
        self.addCleanup(temporary.cleanup)
        git(root, "config", "user.name", "M3 Inputs Test")
        git(root, "config", "user.email", "m3-inputs@example.invalid")
        git(root, "checkout", "--quiet", MODULE.TESTED_SOURCE_COMMIT)
        target = root.joinpath(*MODULE.SOURCE_LOCKS_PATH.parts)
        target.write_bytes(target.read_bytes() + b" ")
        git(root, "add", str(MODULE.SOURCE_LOCKS_PATH))
        tree = git(root, "write-tree")
        parents = git(root, "show", "-s", "--format=%P", MODULE.TESTED_SOURCE_COMMIT).split()
        command = ["commit-tree", tree]
        for parent in parents:
            command.extend(["-p", parent])
        replacement = git(root, *command, input_text="replace tested source for negative test\n")
        git(root, "replace", MODULE.TESTED_SOURCE_COMMIT, replacement)
        git(root, "checkout", "--quiet", "-f", git(SOURCE_ROOT, "rev-parse", "HEAD"))
        self.assert_rejected(root, "tested-source historical source-lock bytes differ")

    def test_semantics_reject_wrong_exclusions(self) -> None:
        receipt = json.loads(SOURCE_ROOT.joinpath(*MODULE.ROOT_PATH.parts).read_bytes())
        kafka = json.loads(SOURCE_ROOT.joinpath(*MODULE.KAFKA_PATH.parts).read_bytes())
        pulsar = json.loads(SOURCE_ROOT.joinpath(*MODULE.PULSAR_PATH.parts).read_bytes())
        receipt = copy.deepcopy(receipt)
        receipt["boundaries"]["excludedMilestones"].remove("M8_NATIVE_PARITY")
        with self.assertRaisesRegex(MODULE.M3InputsError, "M3/M6/M8 exclusions differ"):
            MODULE.validate_semantics(receipt, kafka, pulsar)

    def test_strict_json_rejects_duplicate_members(self) -> None:
        with self.assertRaisesRegex(MODULE.M3InputsError, "duplicate JSON member 'schema'"):
            MODULE._strict_json(b'{"schema":1,"schema":2}', "duplicate.json")


if __name__ == "__main__":
    unittest.main(verbosity=2)
