#!/usr/bin/env python3
"""Replay the unchanged pre-implementation M5 validator on its immutable source."""

import importlib.util
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent.parent
FROZEN = "c86fde3ed6f4319642987fd599022bd32e2cca5e"
SCRIPT = "scripts/check-v2-m5-design.py"


def historical_bytes(root: Path, path: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), "show", f"{FROZEN}:{path}"])


def validate(root: Path) -> None:
    subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", FROZEN, "HEAD"], check=True)
    with tempfile.TemporaryDirectory(prefix="nereus-m5-frozen-design-") as directory:
        snapshot = Path(directory)
        script = snapshot / SCRIPT
        script.parent.mkdir(parents=True)
        script.write_bytes(historical_bytes(root, SCRIPT))
        spec = importlib.util.spec_from_file_location("immutable_m5_design", script)
        checker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(checker)
        paths = set(checker.DESIGN_DOCUMENTS) | {
            checker.MANIFEST_PATH, checker.OPEN_PATH, checker.SCENARIO_PATH, checker.PLAN_PATH,
            checker.V2_INDEX_PATH, checker.MATRIX_PATH, checker.BUILD_PATH, checker.SETTINGS_PATH,
            checker.M4_FINAL_PATH,
        }
        for path in paths:
            target = snapshot / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(historical_bytes(root, str(path)))
        for path in (checker.MANIFEST_PATH, *checker.BOUND_DOCUMENTS):
            if (root / path).read_bytes() != (snapshot / path).read_bytes():
                raise ValueError(f"current immutable M5 bytes changed: {path}")
        # Prove the runtime/evidence absence against the full frozen tree, not just the extracted inputs.
        paths = subprocess.check_output(
            ["git", "-C", str(root), "ls-tree", "-r", "--name-only", FROZEN], text=True
        ).splitlines()
        for path in paths:
            parts = Path(path).parts
            if path.startswith(str(checker.EVIDENCE_PREFIX) + "/"):
                raise ValueError("frozen tree contains M5 evidence")
            if parts[0].startswith("nereus-") and "/src/main/" in path:
                if "m5" in parts[-1].lower() or "materialization" in parts:
                    raise ValueError("frozen tree contains M5 runtime")
        checker.validate(snapshot)


if __name__ == "__main__":
    validate(ROOT)
    print(f"PASS_V2_M5_HISTORICAL_DESIGN source={FROZEN} currentImmutableBytes=EXACT runtimeAuthority=NONE")
