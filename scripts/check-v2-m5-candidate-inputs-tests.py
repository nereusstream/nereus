#!/usr/bin/env python3
"""Contract tests for independently readable M5 candidate inputs and real Git commit equivalence."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("m5-candidate-inputs.py")
SPEC = importlib.util.spec_from_file_location("m5_candidate_inputs_test_contract", SCRIPT)
CONTRACT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTRACT)


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.PIPE, text=True).strip()


class Fixture:
    def __init__(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="nereus-m5-candidate-contract-")
        self.base = Path(self.temporary.name).resolve()
        self.roots = {name: self.base / name for name in CONTRACT.REPOSITORIES}
        for name, root in self.roots.items():
            root.mkdir()
            git(root, "init", "-b", "main")
            git(root, "config", "user.name", "M5 Candidate Contract")
            git(root, "config", "user.email", "m5-candidate@example.invalid")
            git(root, "config", "commit.gpgsign", "false")
            (root / "source.txt").write_text(name + " base\n")
            (root / "to-delete").write_text("old\n")
            (root / ".gitignore").write_text("build/\n")
            git(root, "add", "--", "source.txt", "to-delete", ".gitignore")
            if name == "pulsar":
                (root / "version-link").symlink_to("source.txt")
                git(root, "add", "--", "version-link")
            git(root, "commit", "-qm", "fixture baseline")
        self.output = self.base / "candidate"
        root = self.roots["nereus"]
        (root / "source.txt").write_text("actual changed input\n")
        (root / "new-source.txt").write_text("actual untracked input\n")
        (self.roots["pulsar"] / "to-delete").unlink()
        self.jar = root / "build/dependency.jar"
        self.jar.parent.mkdir()
        self.jar.write_bytes(b"registered dependency bytes")
        self.inventory = root / "build/inputs.json"
        self.inventory.write_text(json.dumps({"build/dependency.jar": CONTRACT.digest(self.jar.read_bytes()),
                                               "source.txt": CONTRACT.digest((root / "source.txt").read_bytes())}))
        self.specs = [("nereus", self.inventory)]

    def prepare(self):
        return CONTRACT.prepare(self.roots, self.specs, self.output)

    def rewrite(self, value):
        value["inputRootSha256"] = CONTRACT.digest(CONTRACT.canonical({key: value[key] for key in
                                                                       ("repositories", "extraInputs", "inputManifests")}))
        (self.output / "input-closure.json").write_bytes(CONTRACT.canonical(value))

    def commits(self):
        commits = {}
        for name, root in self.roots.items():
            paths = ["source.txt", "to-delete", ".gitignore"]
            if name == "nereus":
                paths.append("new-source.txt")
            git(root, "add", "--", *paths)
            if git(root, "diff", "--cached", "--name-only"):
                git(root, "commit", "-qm", "fixture source binding")
            commits[name] = git(root, "rev-parse", "HEAD")
        return commits


class CandidateInputTests(unittest.TestCase):
    def setUp(self):
        self.fixture = Fixture()
        self.addCleanup(self.fixture.temporary.cleanup)

    def test_complete_dirty_sources_dependencies_and_internal_links_verify_without_original_repositories(self):
        f = self.fixture
        value = f.prepare()
        self.assertIn("new-source.txt", value["repositories"]["nereus"]["files"])
        self.assertEqual({"deleted": True}, value["repositories"]["pulsar"]["files"]["to-delete"])
        self.assertFalse(value["executionEvidence"])
        self.assertFalse(value["promotionEligible"])
        for root in f.roots.values():
            shutil.rmtree(root)
        self.assertEqual(value, CONTRACT.verify(f.output))

    def test_real_commit_binding_requires_exact_complete_source_tree(self):
        f = self.fixture
        value = f.prepare()
        commits = f.commits()
        result = CONTRACT.bind(f.output, f.roots, commits)
        self.assertEqual(value["inputRootSha256"], result["inputRootSha256"])
        self.assertFalse(result["executionEvidence"])
        root = f.roots["kafka"]
        (root / "unregistered.txt").write_text("extra committed input\n")
        git(root, "add", "--", "unregistered.txt")
        git(root, "commit", "-qm", "fixture incompatible input")
        commits["kafka"] = git(root, "rev-parse", "HEAD")
        with self.assertRaisesRegex(CONTRACT.ClosureError, "committed source differs"):
            CONTRACT.bind(f.output, f.roots, commits)

    def test_declared_dependency_drift_stops_preparation_before_destination_exists(self):
        f = self.fixture
        f.jar.write_bytes(b"changed")
        with self.assertRaisesRegex(CONTRACT.ClosureError, "changed or conflicts"):
            f.prepare()
        self.assertFalse(f.output.exists())

    def test_source_changes_during_copy_leave_no_valid_manifest(self):
        f = self.fixture
        original = CONTRACT.store
        changed = False

        def copy_then_change(output, raw):
            nonlocal changed
            original(output, raw)
            if not changed:
                changed = True
                (f.roots["kafka"] / "late-source.txt").write_text("unregistered late writer\n")

        CONTRACT.store = copy_then_change
        self.addCleanup(setattr, CONTRACT, "store", original)
        with self.assertRaisesRegex(CONTRACT.ClosureError, "cohort changed"):
            f.prepare()
        self.assertFalse((f.output / "input-closure.json").exists())

    def test_missing_object_and_unregistered_workspace_source_cannot_verify(self):
        f = self.fixture
        f.prepare()
        path = next((f.output / "objects").iterdir())
        raw = path.read_bytes()
        path.unlink()
        with self.assertRaisesRegex(CONTRACT.ClosureError, "object inventory"):
            CONTRACT.verify(f.output)
        path.write_bytes(raw)
        workspace = f.output / "workspace" / CONTRACT.LAYOUT["kafka"]
        (workspace / "late-source.txt").write_text("not captured")
        with self.assertRaisesRegex(CONTRACT.ClosureError, "workspace input inventory"):
            CONTRACT.verify(f.output)

    def test_dependency_edges_are_reparsed_independently(self):
        f = self.fixture
        value = f.prepare()
        value["extraInputs"].pop(str(f.jar))
        f.rewrite(value)
        expected = {record["sha256"] for repo in value["repositories"].values() for record in repo["files"].values() if "sha256" in record}
        expected.update(record["sha256"] for record in value["extraInputs"].values())
        for path in (f.output / "objects").iterdir():
            if path.name not in expected:
                path.unlink()
        with self.assertRaisesRegex(CONTRACT.ClosureError, "manifest edge"):
            CONTRACT.verify(f.output)

    def test_object_symlinks_and_external_source_symlinks_are_rejected(self):
        f = self.fixture
        f.prepare()
        path = next((f.output / "objects").iterdir())
        outside = f.base / "outside-object"
        outside.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(outside)
        with self.assertRaisesRegex(CONTRACT.ClosureError, "bounded regular"):
            CONTRACT.verify(f.output)
        (f.roots["nereus"] / "outside-link").symlink_to(outside)
        with self.assertRaisesRegex(CONTRACT.ClosureError, "escapes"):
            CONTRACT.source_index(f.roots["nereus"])

    def test_preparation_cannot_claim_execution_or_overwrite_a_candidate(self):
        f = self.fixture
        value = f.prepare()
        with self.assertRaisesRegex(CONTRACT.ClosureError, "already exists"):
            f.prepare()
        value["executionEvidence"] = True
        f.rewrite(value)
        with self.assertRaisesRegex(CONTRACT.ClosureError, "cannot claim execution"):
            CONTRACT.verify(f.output)

    def test_git_subdirectory_cannot_stand_in_for_a_complete_repository(self):
        path = self.fixture.roots["kafka"] / "module"
        path.mkdir()
        with self.assertRaisesRegex(CONTRACT.ClosureError, "complete Git worktree root"):
            CONTRACT.repository(path)

    def test_ignored_materialized_dependencies_are_independently_verified(self):
        f = self.fixture
        f.prepare()
        path = f.output / "workspace" / CONTRACT.LAYOUT["nereus"] / "build/dependency.jar"
        path.write_bytes(b"changed ignored input")
        with self.assertRaisesRegex(CONTRACT.ClosureError, "materialized dependency input") as failure:
            CONTRACT.verify(f.output)
        message = str(failure.exception)
        for detail in (str(f.jar), str(path), "expected=", "observed=",
                       CONTRACT.digest(b"registered dependency bytes"), CONTRACT.digest(path.read_bytes()),
                       "'bytes': 27", "'bytes': 21"):
            self.assertIn(detail, message)

    def test_mode_drift_and_external_git_alternates_are_rejected(self):
        f = self.fixture
        f.prepare()
        workspace = f.output / "workspace" / CONTRACT.LAYOUT["pulsar"]
        path = workspace / "source.txt"
        path.chmod(0o755)
        with self.assertRaisesRegex(CONTRACT.ClosureError, "workspace input inventory"):
            CONTRACT.verify(f.output)
        path.chmod(0o644)
        (workspace / ".git/objects/info/alternates").write_text(str(f.roots["pulsar"] / ".git/objects") + "\n")
        with self.assertRaisesRegex(CONTRACT.ClosureError, "external Git object dependencies"):
            CONTRACT.verify(f.output)


if __name__ == "__main__":
    unittest.main()
