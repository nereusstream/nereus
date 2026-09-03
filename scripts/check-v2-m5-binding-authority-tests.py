#!/usr/bin/env python3
"""Fail-closed contract tests for the focused M5-C Binding-authority checker."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/check-v2-m5-binding-authority.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BINDING = load_module(SCRIPT, "nereus_v2_m5_binding_authority_tests")


class M5BindingAuthorityContractTest(unittest.TestCase):
    def test_accepts_current_implementation(self) -> None:
        BINDING.validate(ROOT)

    def test_projection_rejects_multi_key_requirement(self) -> None:
        value = json.loads((ROOT / BINDING.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["selectedMetadataBackend"]["atomicMultiKeyTransactionRequired"] = True
        with self.assertRaisesRegex(BINDING.BindingAuthorityError, "backend capability"):
            BINDING.validate_projection_value(changed)

    def test_projection_rejects_missing_writer_kind(self) -> None:
        value = json.loads((ROOT / BINDING.PROJECTION_PATH).read_text(encoding="utf-8"))
        changed = copy.deepcopy(value)
        changed["writerEnrollment"]["referenceKinds"].pop()
        with self.assertRaisesRegex(BINDING.BindingAuthorityError, "writer enrollment"):
            BINDING.validate_projection_value(changed)

    def test_required_sources_reject_missing_codec(self) -> None:
        with self.assertRaisesRegex(BINDING.BindingAuthorityError, "missing/empty"):
            BINDING.validate_required_sources(ROOT, ("missing/M5BindingAuthorityCodecV1.java",))


if __name__ == "__main__":
    unittest.main()
