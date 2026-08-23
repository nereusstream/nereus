#!/usr/bin/env python3
"""Publish a non-promotable full trusted current-source M2 regression receipt."""

from __future__ import annotations

import argparse
import importlib.util
import os
from pathlib import Path
import stat
import sys
from typing import Any


CHECKER_PATH = Path(__file__).with_name("check-v2-m3-m2-regression.py")
SPEC = importlib.util.spec_from_file_location("check_v2_m3_m2_regression", CHECKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load checker contract: {CHECKER_PATH}")
CONTRACT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CONTRACT
SPEC.loader.exec_module(CONTRACT)


def read_explicit_result(path: Path) -> bytes:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise CONTRACT.RegressionError(f"trusted child result is missing: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise CONTRACT.RegressionError(f"trusted child result is not a regular non-symlink file: {path}")
    return path.read_bytes()


def _create_parents(path: Path) -> list[Path]:
    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for directory in reversed(missing):
        directory.mkdir()
    return list(reversed(missing))


def _exclusive_write(path: Path, raw: bytes) -> None:
    with path.open("xb") as output:
        output.write(raw)


def build_receipt(
    tested_commit: str,
    output_path: CONTRACT.PurePosixPath,
    children: dict[str, tuple[dict[str, Any], bytes]],
) -> tuple[dict[str, Any], dict[CONTRACT.PurePosixPath, bytes]]:
    copied: dict[CONTRACT.PurePosixPath, bytes] = {}
    rows: list[dict[str, Any]] = []
    for gate in CONTRACT.REQUIRED_GATES:
        child, raw = children[gate]
        destination = output_path.parent / "attachments" / f"{gate}.json"
        copied[destination] = raw
        rows.append(
            {
                "attachment": CONTRACT.attachment_identity(destination, raw),
                "errors": child["errors"],
                "failures": child["failures"],
                "gateId": gate,
                "result": "PASS",
                "skipped": child["skipped"],
                "tests": child["tests"],
            }
        )
    receipt = {
        "childGates": rows,
        "evidenceClass": CONTRACT.EXECUTION_PROFILE,
        "exclusions": list(CONTRACT.EXCLUSIONS),
        "historicalFinal": CONTRACT.historical_final_identity(),
        "kind": CONTRACT.RECEIPT_KIND,
        "m2AmendmentLineage": [],
        "promotionEligible": False,
        "result": CONTRACT.RECEIPT_RESULT,
        "scenarioPromotion": False,
        "schema": CONTRACT.RECEIPT_SCHEMA,
        "sources": CONTRACT.expected_sources(tested_commit),
        "testedNereusCommit": tested_commit,
    }
    return receipt, copied


def publish(
    root: Path,
    tested_commit: str,
    output_path: CONTRACT.PurePosixPath,
    child_paths: list[Path],
) -> bytes:
    root = CONTRACT.ensure_root(root)
    head = CONTRACT.current_head(root)
    if tested_commit != head:
        raise CONTRACT.RegressionError(
            f"publisher requires tested commit to equal exact clean HEAD: {tested_commit} != {head}"
        )
    status = str(CONTRACT.git(root, "status", "--porcelain=v1", "--untracked-files=all", text=True))
    if status:
        raise CONTRACT.RegressionError("publisher requires a clean exact HEAD before writing evidence")
    if not CONTRACT.is_under(output_path, CONTRACT.EVIDENCE_PREFIX):
        raise CONTRACT.RegressionError(f"output is outside {CONTRACT.EVIDENCE_PREFIX}: {output_path}")
    if len(child_paths) != len(CONTRACT.REQUIRED_GATES):
        raise CONTRACT.RegressionError(
            f"publisher requires exactly {len(CONTRACT.REQUIRED_GATES)} explicit trusted child results"
        )

    children: dict[str, tuple[dict[str, Any], bytes]] = {}
    for path in child_paths:
        raw = read_explicit_result(path)
        child = CONTRACT.load_canonical_json(raw, os.fspath(path))
        CONTRACT.validate_child_result(child, tested_commit)
        gate = child["gateId"]
        if gate in children:
            raise CONTRACT.RegressionError(f"duplicate trusted child gate: {gate}")
        children[gate] = (child, raw)
    missing = sorted(set(CONTRACT.REQUIRED_GATES) - set(children))
    extra = sorted(set(children) - set(CONTRACT.REQUIRED_GATES))
    if missing or extra:
        raise CONTRACT.RegressionError(
            f"trusted child full-profile allowlist differs: missing={missing} extra={extra}"
        )

    receipt, copied = build_receipt(tested_commit, output_path, children)
    receipt_raw = CONTRACT.canonical_bytes(receipt)
    destinations = [root.joinpath(*path.parts) for path in (*copied, output_path)]
    existing = [path for path in destinations if path.exists() or path.is_symlink()]
    if existing:
        raise CONTRACT.RegressionError(
            "publisher refuses to overwrite evidence: " + ", ".join(os.fspath(path) for path in existing)
        )

    created_directories: list[Path] = []
    created_files: list[Path] = []
    try:
        for relative, raw in copied.items():
            destination = root.joinpath(*relative.parts)
            created_directories.extend(_create_parents(destination.parent))
            _exclusive_write(destination, raw)
            created_files.append(destination)
        output = root.joinpath(*output_path.parts)
        created_directories.extend(_create_parents(output.parent))
        _exclusive_write(output, receipt_raw)
        created_files.append(output)
    except OSError as error:
        for path in reversed(created_files):
            path.unlink(missing_ok=True)
        for directory in reversed(created_directories):
            try:
                directory.rmdir()
            except OSError:
                pass
        raise CONTRACT.RegressionError(f"cannot publish regression evidence atomically: {error}") from error
    return receipt_raw


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--tested-commit", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--child-result", action="append", type=Path, required=True)
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        output = CONTRACT.safe_relative(args.output, "--output")
        raw = publish(args.repo_root, args.tested_commit, output, args.child_result)
    except (CONTRACT.RegressionError, OSError) as error:
        print(f"V2 M3 current-source M2 regression publisher: {error}", file=sys.stderr)
        return 1
    print(
        "V2 M3 current-source M2 regression published: "
        f"tested={args.tested_commit} output={output} bytes={len(raw)} "
        f"sha256={CONTRACT.sha256(raw)} childGates={len(CONTRACT.REQUIRED_GATES)} "
        "promotionEligible=false scenarioPromotion=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
