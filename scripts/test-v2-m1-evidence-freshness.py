#!/usr/bin/env python3
"""Deterministic boundary tests for the M1 checkout-to-Final freshness gate."""

from __future__ import annotations

import hashlib
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("v2_m1_evidence_freshness.py")


def git(root: pathlib.Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


class EvidenceFreshnessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="nereus-m1-freshness-")
        self.root = pathlib.Path(self.temporary.name)
        git(self.root, "init", "-b", "main")
        git(self.root, "config", "user.name", "M1 Freshness Test")
        git(self.root, "config", "user.email", "m1-freshness@example.invalid")
        (self.root / "src").mkdir()
        (self.root / "src/main.txt").write_text("tested source\n")
        (self.root / "docs/v2").mkdir(parents=True)
        (self.root / "docs/v2/source-locks.json").write_text('{"sourceTupleId":"v2-m1"}\n')
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "tested source")
        self.tested = git(self.root, "rev-parse", "HEAD")
        self.source_locks_sha = hashlib.sha256((self.root / "docs/v2/source-locks.json").read_bytes()).hexdigest()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_final(self, tested: str | None = None, locks_sha: str | None = None) -> None:
        directory = self.root / "docs/v2/evidence/v2-m1/n3"
        directory.mkdir(parents=True, exist_ok=True)
        receipt = {
            "sourceTuple": {
                "nereusCommit": tested or self.tested,
                "sourceLocksSha256": locks_sha or self.source_locks_sha,
            }
        }
        (directory / "receipt.json").write_text(json.dumps(receipt, separators=(",", ":")) + "\n")
        index = {"receiptRefs": [{"path": "receipt.json"}]}
        (directory / "final-index.json").write_text(json.dumps(index, separators=(",", ":")) + "\n")

    def commit_final(self, tested: str | None = None, locks_sha: str | None = None) -> None:
        self.write_final(tested, locks_sha)
        git(self.root, "add", "docs/v2/evidence/v2-m1/n3")
        git(self.root, "commit", "-m", "final evidence")

    def run_check(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                str(self.root),
                str(self.root / "docs/v2/evidence/v2-m1/n3/final-index.json"),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def assert_rejected(self, message: str) -> None:
        result = self.run_check()
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(message, result.stderr)

    def test_accepts_clean_evidence_only_descendant(self) -> None:
        self.commit_final()
        result = self.run_check()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn(f"tested={self.tested}", result.stdout)
        self.assertIn("evidenceCommits=1", result.stdout)

    def test_rejects_code_change_after_evidence(self) -> None:
        self.commit_final()
        (self.root / "src/main.txt").write_text("changed after test\n")
        git(self.root, "add", "src/main.txt")
        git(self.root, "commit", "-m", "change code")
        self.assert_rejected("non-evidence path changed after tested source")

    def test_rejects_dirty_checkout(self) -> None:
        self.commit_final()
        (self.root / "untracked.txt").write_text("dirty\n")
        self.assert_rejected("worktree is dirty")

    def test_rejects_wrong_source_lock_digest(self) -> None:
        self.commit_final(locks_sha="f" * 64)
        self.assert_rejected("source-lock bytes do not match")

    def test_rejects_existing_non_ancestor_commit(self) -> None:
        git(self.root, "switch", "-c", "side")
        (self.root / "side.txt").write_text("side\n")
        git(self.root, "add", "side.txt")
        git(self.root, "commit", "-m", "side commit")
        side = git(self.root, "rev-parse", "HEAD")
        git(self.root, "switch", "main")
        self.commit_final(tested=side)
        self.assert_rejected("tested Nereus commit is not an ancestor of HEAD")

    def test_rejects_intermediate_code_change_even_if_reverted(self) -> None:
        (self.root / "src/main.txt").write_text("temporary code change\n")
        git(self.root, "add", "src/main.txt")
        git(self.root, "commit", "-m", "temporary code change")
        (self.root / "src/main.txt").write_text("tested source\n")
        git(self.root, "add", "src/main.txt")
        git(self.root, "commit", "-m", "revert temporary code change")
        self.commit_final()
        self.assert_rejected("non-evidence path changed after tested source")

    def test_rejects_symlinked_final_index(self) -> None:
        self.commit_final()
        index = self.root / "docs/v2/evidence/v2-m1/n3/final-index.json"
        target = index.with_name("real-final-index.json")
        shutil.move(index, target)
        index.symlink_to(target.name)
        git(self.root, "add", "docs/v2/evidence/v2-m1/n3")
        git(self.root, "commit", "-m", "replace Final index with symlink")
        self.assert_rejected("Final index must be the committed authority")

    def test_rejects_symlinked_receipt_directory(self) -> None:
        self.commit_final()
        n3 = self.root / "docs/v2/evidence/v2-m1/n3"
        real = n3 / "real"
        real.mkdir()
        shutil.move(n3 / "receipt.json", real / "receipt.json")
        (n3 / "linked").symlink_to("real", target_is_directory=True)
        (n3 / "final-index.json").write_text('{"receiptRefs":[{"path":"linked/receipt.json"}]}\n')
        git(self.root, "add", "docs/v2/evidence/v2-m1/n3")
        git(self.root, "commit", "-m", "replace receipt directory with symlink")
        self.assert_rejected("receipt reference has a symlink component")


if __name__ == "__main__":
    unittest.main(verbosity=2)
