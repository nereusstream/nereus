#!/usr/bin/env python3
"""Exercise M6 identity and closure rejection with real Git worktrees."""

from contextlib import redirect_stderr, redirect_stdout
import hashlib
import importlib.util
from io import StringIO
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "m6_inputs", Path(__file__).with_name("m6-execution-inputs.py")
)
M6 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M6)


class ExecutionInputsTest(unittest.TestCase):
    def setUp(self):
        workspace = tempfile.TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        base = Path(workspace.name).resolve()
        self.roots = {name: base / name for name in M6.REPOSITORIES}
        self.archive = "docs/v2/evidence/v2-m5/receipt.json"
        for name, root in self.roots.items():
            root.mkdir()
            self.git(root, "init", "-q")
            (root / "src").write_text(name)
            if name == "nereus":
                (root / self.archive).parent.mkdir(parents=True)
                (root / self.archive).write_text("original receipt")
            self.commit(root, "initial")
        self.root = self.roots["nereus"]
        archive = patch.object(M6, "ARCHIVE_COMMIT", M6.revision(self.root, "HEAD"))
        archive.start()
        self.addCleanup(archive.stop)
        self.output = base / "output"
        self.runtime = base / "tool"
        self.runtime.write_bytes(b"tool")
        self.manifest = base / "runtime.json"
        self.manifest.write_text(json.dumps({"tool": {
            "path": str(self.runtime), "sha256": hashlib.sha256(b"tool").hexdigest(),
        }}))

    @staticmethod
    def git(root, *args):
        return subprocess.check_output(
            ["git", "-C", str(root), "-c", "user.name=Test", "-c", "user.email=test@example.org",
             "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", *args]
        )

    def commit(self, root, message):
        self.git(root, "add", "--all")
        self.git(root, "commit", "-qm", message)

    def capture(self, history=(), output=None, runtime=None):
        return M6.capture(
            self.roots, output or self.output, history,
            {"tool": self.runtime} if runtime is None else runtime, 4096
        )

    def command(self, operation):
        args = [operation, "--output", str(self.output), "--runtime-manifest", str(self.manifest),
                "--runtime-byte-budget", "4096", "--historical-path", self.archive]
        for key, root in self.roots.items():
            args.extend((f"--{key}", str(root)))
        with redirect_stderr(StringIO()), redirect_stdout(StringIO()):
            return M6.main(args)

    def test_retains_every_tracked_node_and_selects_history_separately(self):
        omitted = self.capture()
        selected = self.capture([self.archive])
        for key, root in self.roots.items():
            version = selected["sourceIdentity"][key]
            self.assertEqual(M6.revision(root, "HEAD"), version["commit"])
            self.assertEqual(M6.revision(root, "HEAD^{tree}"), version["tree"])
            self.assertEqual(set(version["files"]), set(M6.tree(root, version["commit"])))
        self.assertEqual("RETAINED_ARCHIVE",
                         omitted["sourceIdentity"]["nereus"]["files"][self.archive]["classification"])
        self.assertNotIn(self.archive, omitted["sourceInputs"]["nereus"])
        self.assertEqual({}, omitted["historicalInputs"]["files"])
        self.assertEqual(M6.ARCHIVE_COMMIT, selected["historicalInputs"]["archiveCommit"])
        self.assertEqual(hashlib.sha256(b"original receipt").hexdigest(),
                         selected["historicalInputs"]["files"][self.archive]["sha256"])

    def test_capture_and_verify_refuse_runtime_drift_or_wrong_commit(self):
        self.assertEqual(0, self.command("capture"))
        self.assertEqual(0, self.command("verify"))
        self.runtime.write_bytes(b"Tool")
        self.assertEqual(1, self.command("verify"))
        self.runtime.write_bytes(b"tool")
        self.git(self.roots["kafka"], "commit", "-qm", "different identity", "--allow-empty")
        self.assertEqual(1, self.command("verify"))

    def test_dirty_source_rejected_even_if_hidden_from_git_status(self):
        root = self.roots["kafka"]
        (root / "src").write_text("KAFKA")
        with self.assertRaisesRegex(M6.InputError, "dirty"):
            self.capture()
        self.git(root, "update-index", "--assume-unchanged", "src")
        self.assertEqual(b"", self.git(root, "status", "--porcelain"))
        with self.assertRaisesRegex(M6.InputError, "checkout bytes differ"):
            self.capture()

    def test_missing_and_committed_rewritten_history_are_rejected(self):
        with self.assertRaisesRegex(M6.InputError, "absent"):
            self.capture([self.archive + ".unknown"])
        (self.root / self.archive).write_text("different receipt")
        self.commit(self.root, "rewrite historical archive")
        with self.assertRaisesRegex(M6.InputError, "historical archive"):
            self.capture()

    def test_historical_bytes_have_a_separate_bound_and_detect_disk_drift(self):
        with patch.object(M6, "ARCHIVE_BUDGET", 1):
            self.capture()
            with self.assertRaisesRegex(M6.InputError, "historical execution inputs exceed"):
                self.capture([self.archive])
        self.git(self.root, "update-index", "--assume-unchanged", self.archive)
        (self.root / self.archive).write_text("altered! receipt")
        with self.assertRaisesRegex(M6.InputError, "checkout bytes differ"):
            self.capture([self.archive])

    def test_output_cannot_be_source_or_feed_its_own_stage(self):
        with self.assertRaisesRegex(M6.InputError, "outside every source"):
            self.capture(output=self.root / "build")
        self.output.mkdir()
        produced = self.output / "candidate"
        produced.write_text("result")
        with self.assertRaisesRegex(M6.InputError, "overlaps source or evidence output"):
            self.capture(runtime={"artifact": produced})

    def test_tracked_symlink_requires_a_declared_in_tree_target(self):
        link = self.root / "receipt-link"
        link.symlink_to(self.archive)
        self.commit(self.root, "declare source link")
        with self.assertRaisesRegex(M6.InputError, "outside the declared inputs"):
            self.capture()
        self.assertEqual("120000", self.capture([self.archive])["sourceInputs"]["nereus"]["receipt-link"]["mode"])


if __name__ == "__main__":
    unittest.main()
