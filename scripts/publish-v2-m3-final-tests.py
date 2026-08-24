#!/usr/bin/env python3
"""Positive and negative tests for the M3 Final exclusive publisher."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


TEST_SUPPORT_PATH = Path(__file__).with_name("check-v2-m3-final-tests.py")
PUBLISHER_PATH = Path(__file__).with_name("publish-v2-m3-final.py")


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


SUPPORT = load_module("m3_final_publish_test_support", TEST_SUPPORT_PATH)
PUBLISHER = load_module("m3_final_publisher_test", PUBLISHER_PATH)


class PublisherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = SUPPORT.FixtureBuilder()

    def tearDown(self) -> None:
        self.fixture.cleanup()

    def test_publishes_exact_canonical_receipt_after_evidence_only_children(self) -> None:
        raw = PUBLISHER.publish(self.fixture.root, self.fixture.candidate, SUPPORT.OUTPUT)
        destination = self.fixture.root.joinpath(*SUPPORT.OUTPUT.parts)
        self.assertEqual(raw, destination.read_bytes())
        receipt = PUBLISHER.CONTRACT.load_canonical_json(raw, "published receipt")
        self.assertEqual(PUBLISHER.CONTRACT.RESULT, receipt["result"])
        PUBLISHER.CONTRACT.validate_receipt(
            self.fixture.root,
            SUPPORT.OUTPUT,
            self.fixture.tested,
            require_scenario_sync=False,
        )

    def test_rejects_dirty_head_before_writing(self) -> None:
        (self.fixture.root / "dirty.txt").write_text("dirty\n")
        with self.assertRaisesRegex(PUBLISHER.CONTRACT.FinalError, "clean HEAD"):
            PUBLISHER.publish(self.fixture.root, self.fixture.candidate, SUPPORT.OUTPUT)

    def test_refuses_to_overwrite_final_evidence(self) -> None:
        PUBLISHER.publish(self.fixture.root, self.fixture.candidate, SUPPORT.OUTPUT)
        SUPPORT.git(self.fixture.root, "add", str(SUPPORT.OUTPUT))
        SUPPORT.git(self.fixture.root, "commit", "-m", "publish first Final")
        with self.assertRaisesRegex(PUBLISHER.CONTRACT.FinalError, "refuses to overwrite"):
            PUBLISHER.publish(self.fixture.root, self.fixture.candidate, SUPPORT.OUTPUT)

    def test_rejects_non_evidence_descendant_of_tested_source(self) -> None:
        path = self.fixture.root / "production.txt"
        path.write_text("invalid post-test production change\n")
        SUPPORT.git(self.fixture.root, "add", "production.txt")
        SUPPORT.git(self.fixture.root, "commit", "-m", "invalid production descendant")
        with self.assertRaisesRegex(PUBLISHER.CONTRACT.FinalError, "non-evidence path changed"):
            PUBLISHER.publish(self.fixture.root, self.fixture.candidate, SUPPORT.OUTPUT)


if __name__ == "__main__":
    unittest.main(verbosity=2)
