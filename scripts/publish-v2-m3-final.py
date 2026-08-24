#!/usr/bin/env python3
"""Publish one prebuilt, fully validated V2 M3 Final JCS receipt without overwrite."""

from __future__ import annotations

import argparse
import importlib.util
import os
from pathlib import Path
import stat
import sys


CHECKER_PATH = Path(__file__).with_name("check-v2-m3-final.py")
SPEC = importlib.util.spec_from_file_location("check_v2_m3_final", CHECKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load M3 Final checker contract: {CHECKER_PATH}")
CONTRACT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CONTRACT
SPEC.loader.exec_module(CONTRACT)


def read_candidate(path: Path) -> bytes:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise CONTRACT.FinalError(f"M3 Final candidate is missing: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise CONTRACT.FinalError(f"M3 Final candidate is not a regular non-symlink file: {path}")
    raw = path.read_bytes()
    if not raw or len(raw) > CONTRACT.MAX_CANONICAL_BYTES:
        raise CONTRACT.FinalError("M3 Final candidate bytes outside cap")
    return raw


def publish(root: Path, candidate_path: Path, output_path: CONTRACT.PurePosixPath) -> bytes:
    root = CONTRACT.ensure_root(root)
    status = str(CONTRACT.git(root, "status", "--porcelain=v1", "--untracked-files=all", text=True))
    if status:
        raise CONTRACT.FinalError("M3 Final publisher requires a clean HEAD before writing evidence")
    raw = read_candidate(candidate_path)
    candidate = CONTRACT.load_canonical_json(raw, os.fspath(candidate_path))
    tested = CONTRACT.validate_receipt_value(root, candidate)
    CONTRACT.validate_descendants(root, tested)
    output_path = CONTRACT.safe_relative(str(output_path), "--output", CONTRACT.FINAL_PREFIX)
    destination = root.joinpath(*output_path.parts)
    if destination.exists() or destination.is_symlink():
        raise CONTRACT.FinalError(f"M3 Final publisher refuses to overwrite evidence: {output_path}")
    created: list[Path] = []
    current = destination.parent
    while not current.exists():
        created.append(current)
        current = current.parent
    try:
        for directory in reversed(created):
            directory.mkdir()
        with destination.open("xb") as output:
            output.write(raw)
    except OSError as error:
        destination.unlink(missing_ok=True)
        for directory in created:
            try:
                directory.rmdir()
            except OSError:
                pass
        raise CONTRACT.FinalError(f"cannot publish M3 Final atomically: {error}") from error
    return raw


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument(
        "--output",
        default="docs/v2/evidence/v2-m3/final/m3-final.json",
    )
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        output = CONTRACT.safe_relative(args.output, "--output", CONTRACT.FINAL_PREFIX)
        raw = publish(args.repo_root, args.candidate, output)
        receipt = CONTRACT.load_canonical_json(raw, "published M3 Final")
    except (CONTRACT.FinalError, OSError) as error:
        print(f"V2 M3 Final publisher: {error}", file=sys.stderr)
        return 1
    print(
        "V2 M3 Final published: "
        f"tested={receipt['sourceTuple']['nereusCommit']} output={output} "
        f"bytes={len(raw)} sha256={CONTRACT.sha256(raw)} children={len(CONTRACT.CHILD_KINDS)} "
        f"scenarios={len(CONTRACT.REQUIRED_SCENARIOS)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
