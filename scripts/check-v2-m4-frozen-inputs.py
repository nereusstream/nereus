#!/usr/bin/env python3
"""Verify the immutable M4 hard-freeze inputs after implementation has started."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


def load_design_checker(root: Path):
    path = root / "scripts/check-v2-m4-design.py"
    spec = importlib.util.spec_from_file_location("m4_design_checker", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load M4 design checker")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    checker = load_design_checker(root)
    try:
        manifest = checker.load_json(
            checker.read_bytes(root, checker.MANIFEST_PATH), str(checker.MANIFEST_PATH)
        )
        checker.validate_manifest_value(root, manifest)
        for path in checker.REVIEW_RECORDS:
            checker.validate_review_record(checker.read_text(root, path), str(path))
        checker.validate_m4d(checker.read_text(root, checker.M4D_PATH))
    except (RuntimeError, checker.DesignError) as error:
        print(f"M4 frozen design inputs: {error}", file=sys.stderr)
        return 1
    print("M4_FROZEN_DESIGN_INPUTS_VERIFIED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
