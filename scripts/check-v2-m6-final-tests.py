#!/usr/bin/env python3
"""Verify failure modes of the M6 publication boundary without native backend runs."""

import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("m6_final", Path(__file__).with_name("check-v2-m6-final.py"))
FINAL = importlib.util.module_from_spec(spec)
spec.loader.exec_module(FINAL)


class FinalTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.git("init", "--quiet")
        (self.root / "implementation").write_text("executed source")
        self.git("add", "implementation")
        self.git("commit", "--quiet", "-m", "executed source")
        self.commit = FINAL.SOURCE.revision(self.root, "HEAD")

    def git(self, *args):
        subprocess.run(["git", "-C", str(self.root), "-c", "user.name=Test",
                        "-c", "user.email=test@example.org", "-c", "commit.gpgsign=false",
                        "-c", "core.hooksPath=/dev/null", *args], check=True, capture_output=True)

    def test_delivery_rejects_uncommitted_and_committed_source_drift(self):
        with patch.dict(FINAL.COMMITS, nereus=self.commit):
            FINAL.verify_delivery_source(self.root)
            (self.root / "implementation").write_text("changed source")
            with self.assertRaisesRegex(FINAL.SOURCE.InputError, "outside the M6"):
                FINAL.verify_delivery_source(self.root)
            self.git("add", "implementation")
            self.git("commit", "--quiet", "-m", "different implementation")
            with self.assertRaisesRegex(FINAL.SOURCE.InputError, "outside the M6"):
                FINAL.verify_delivery_source(self.root)

    def test_final_must_be_committed(self):
        with patch.dict(FINAL.COMMITS, nereus=self.commit):
            with self.assertRaisesRegex(FINAL.SOURCE.InputError, "Final must be committed"):
                FINAL.check(self.root, {})

    def test_rejects_uncommitted_missing_and_tampered_attachment(self):
        raw = self.root / "raw"
        raw.mkdir()
        attachment = raw / "fixture.json"
        attachment.write_bytes(b"fixture\n")
        with self.assertRaisesRegex(FINAL.SOURCE.InputError, "uncommitted M6 attachment"):
            FINAL.inventory(raw, {})
        self.git("add", "raw")
        self.git("commit", "--quiet", "-m", "publish original attachment")
        committed = FINAL.SOURCE.tree(self.root, "HEAD")["raw/fixture.json"]
        nodes = {"fixture.json": committed}
        self.assertEqual(FINAL.inventory(raw), FINAL.inventory(raw, nodes))
        attachment.write_bytes(b"changed\n")
        with self.assertRaisesRegex(FINAL.SOURCE.InputError, "checkout bytes differ from Git"):
            FINAL.inventory(raw, nodes)
        attachment.unlink()
        with self.assertRaisesRegex(FINAL.SOURCE.InputError, "missing committed M6 attachments"):
            FINAL.inventory(raw, nodes)

    def test_rejects_altered_original_verdict(self):
        (self.root / FINAL.RESULT).write_bytes(b'{"promotionEligible":true}\n')
        with self.assertRaisesRegex(FINAL.SOURCE.InputError, "original M6 execution result differs"):
            FINAL.execution(self.root)


if __name__ == "__main__":
    unittest.main()
