#!/usr/bin/env python3
"""Validate M5's immutable historical M4 dependency without recertifying current HEAD as M4."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
from types import ModuleType
from typing import Callable


TESTED = "595c8b34779d1e88187eb0084bf18e65ab2dd742"
CLOSURE = "3c01457295258cf586d56630a63067469be34743"
FINAL_PATH = PurePosixPath(
    "docs/v2/evidence/v2-m4/final/"
    "final-source-595c8b34779d1e88187eb0084bf18e65ab2dd742/m4-final.json"
)
FINAL_SHA256 = "31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07"
SOURCE_LOCK_SHA256 = "02601b3de76857d5f0c8657b285bc91584486d08077e201a4d9fd34f377b07a2"
LEGACY_FINAL_PATH = PurePosixPath("docs/v2/evidence/v2-m4/final/m4-final.json")


class DependencyError(RuntimeError):
    """Fail-closed historical M4 dependency rejection."""


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def git(root: Path, *args: str) -> bytes:
    try:
        return subprocess.check_output(
            ["git", "-C", os.fspath(root), *args], stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as error:
        output = error.output.decode("utf-8", "replace").strip()
        raise DependencyError(f"git {' '.join(args)} failed: {output}") from error


def require_ancestor(
    root: Path,
    ancestor: str,
    descendant: str,
    runner: Callable[[Path, str, str], bool] | None = None,
) -> None:
    if runner is None:
        result = subprocess.run(
            ["git", "-C", os.fspath(root), "merge-base", "--is-ancestor", ancestor, descendant],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        accepted = result.returncode == 0
    else:
        accepted = runner(root, ancestor, descendant)
    if not accepted:
        raise DependencyError(f"required ancestry is broken: {ancestor} -> {descendant}")


def require_fixed_path(path: PurePosixPath) -> None:
    if path != FINAL_PATH or path == LEGACY_FINAL_PATH:
        raise DependencyError(f"M5 historical dependency requires exact M4 Final path: {FINAL_PATH}")


def require_fixed_final(raw: bytes) -> None:
    actual = sha256(raw)
    if actual != FINAL_SHA256:
        raise DependencyError(f"M4 Final SHA-256 differs: {actual} != {FINAL_SHA256}")


def load_m4_contract(root: Path) -> ModuleType:
    path = root / "scripts/check-v2-m4-evidence.py"
    spec = importlib.util.spec_from_file_location("nereus_v2_m4_contract_for_m5", path)
    if spec is None or spec.loader is None:
        raise DependencyError(f"cannot load canonical M4 Final validator: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_clean_m4_evidence(root: Path) -> None:
    raw = git(root, "status", "--porcelain=v1", "-z", "--", "docs/v2/evidence/v2-m4")
    if raw:
        paths = [item.decode("utf-8", "replace") for item in raw.split(b"\0") if item]
        raise DependencyError(f"immutable M4 evidence path is dirty: {paths}")


def validate(root: Path, receipt_path: PurePosixPath = FINAL_PATH) -> str:
    root = root.resolve(strict=True)
    top = Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve(strict=True)
    if top != root:
        raise DependencyError(f"repository root differs from Git top-level: {root} != {top}")
    require_fixed_path(receipt_path)
    require_clean_m4_evidence(root)

    try:
        raw = (root / receipt_path).read_bytes()
    except OSError as error:
        raise DependencyError(f"cannot read exact M4 Final: {receipt_path}") from error
    require_fixed_final(raw)
    if git(root, "show", f"{CLOSURE}:{receipt_path}") != raw:
        raise DependencyError("current exact M4 Final differs from the closure ancestor")

    contract = load_m4_contract(root)
    try:
        value = contract.load_canonical(raw, str(receipt_path))
        tested = contract.validate_final_value(root, value, TESTED)
        contract.validate_scenarios(root, receipt_path)
    except contract.EvidenceError as error:
        raise DependencyError(f"canonical M4 validation failed: {error}") from error
    if tested != TESTED:
        raise DependencyError(f"M4 tested source differs: {tested} != {TESTED}")
    if value.get("sourceTuple", {}).get("sourceLocksSha256") != SOURCE_LOCK_SHA256:
        raise DependencyError("M4 Final-bound source-lock SHA differs")

    head = git(root, "rev-parse", "HEAD").decode().strip()
    require_ancestor(root, TESTED, CLOSURE)
    require_ancestor(root, CLOSURE, head)
    return head


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--receipt", default=str(FINAL_PATH))
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        head = validate(args.repo_root, PurePosixPath(args.receipt))
    except DependencyError as error:
        print(f"M5 historical M4 dependency: {error}", file=sys.stderr)
        return 1
    print(
        "M5 historical M4 dependency verified: "
        f"tested={TESTED} closure={CLOSURE} head={head} FinalSha256={FINAL_SHA256} "
        "children=4 scenarios=5 sharedPredicates=9"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
