#!/usr/bin/env python3
"""Fail-closed tests for the focused M5-D BookKeeper deletion checker."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-bookkeeper-delete.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECK = load_module(SCRIPT, "nereus_v2_m5_bookkeeper_delete_tests")


class M5BookKeeperDeleteContractTest(unittest.TestCase):
    def test_accepts_current_bookkeeper_slice(self) -> None:
        CHECK.validate(ROOT)

    def test_projection_rejects_wrong_image(self) -> None:
        value = json.loads((ROOT / CHECK.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["bookKeeperBoundary"]["serverImageId"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(CHECK.BookKeeperDeleteError, "BookKeeper boundary"):
            CHECK.validate_projection_value(changed)

    def test_projection_rejects_authority_overclaim(self) -> None:
        value = json.loads((ROOT / CHECK.PROJECTION_PATH).read_text(encoding="utf-8"))
        for field in (
            "dispatchAuthorityApiPresent",
            "fullM5DGatePresent",
            "physicalDeleteAuthority",
            "sourceBoundReceiptPresent",
        ):
            changed = copy.deepcopy(value)
            changed[field] = True
            with self.assertRaisesRegex(CHECK.BookKeeperDeleteError, "overstates"):
                CHECK.validate_projection_value(changed)

    def test_source_lock_rejects_changed_server_image(self) -> None:
        value = json.loads((ROOT / "docs/v2/source-locks.json").read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["m2KafkaK0InputSourceBinding"]["bookKeeperInput"]["serverImageConfigDigest"] = (
            "sha256:" + "0" * 64
        )
        with self.assertRaisesRegex(CHECK.BookKeeperDeleteError, "client/server tuple"):
            CHECK.validate_source_locks(changed)

    def test_sources_reject_missing_adapter(self) -> None:
        original = CHECK.REQUIRED_SOURCES
        try:
            CHECK.REQUIRED_SOURCES = ("missing/M5BookKeeperDeleteAdapterV1.java",)
            with self.assertRaisesRegex(CHECK.BookKeeperDeleteError, "missing/empty"):
                CHECK.validate_sources(ROOT)
        finally:
            CHECK.REQUIRED_SOURCES = original


if __name__ == "__main__":
    unittest.main()
