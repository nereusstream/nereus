#!/usr/bin/env python3
"""Validate the immutable M2 prerequisite inside the current M3 source-lock document."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Any


K0_RECEIPT = Path("docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json")
SOURCE_LOCKS = Path("docs/v2/source-locks.json")
CURRENT_ONLY_KEYS = frozenset(
    {
        "m3AllocatorEvidenceBinding",
        "m3EvidenceBindings",
        "m3KafkaNativeBinding",
        "m3PulsarNativeBinding",
    }
)
COMMIT = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")


class ProjectionError(RuntimeError):
    """A stable fail-closed prerequisite-projection rejection."""


def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProjectionError(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def regular(path: Path) -> bytes:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise ProjectionError(f"input is missing: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise ProjectionError(f"input is not a regular non-symlink file: {path}")
    return path.read_bytes()


def parse(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProjectionError(f"cannot parse {label}: {error}") from error
    if not isinstance(value, dict):
        raise ProjectionError(f"JSON root is not an object: {label}")
    return value


def git_blob(root: Path, commit: str, path: Path) -> bytes:
    try:
        return subprocess.check_output(
            ["git", "-C", os.fspath(root), "show", f"{commit}:{path}"],
            stderr=subprocess.STDOUT,
        )
    except subprocess.CalledProcessError as error:
        raise ProjectionError(
            f"cannot read exact Git blob {commit}:{path}: "
            f"{error.output.decode('utf-8', 'replace').strip()}"
        ) from error


def validate(root: Path, tested: str, output: Path | None = None) -> tuple[str, str, bytes]:
    root = root.resolve(strict=True)
    if not COMMIT.fullmatch(tested):
        raise ProjectionError("tested commit is not canonical")
    receipt = parse(regular(root / K0_RECEIPT), str(K0_RECEIPT))
    if (
        receipt.get("schema") != "NEREUS_V2_M2_KAFKA_INPUTS_RECEIPT_V1"
        or receipt.get("result") != "PASS_KAFKA_M2_INPUTS_ONLY"
        or receipt.get("promotionEligible") is not False
    ):
        raise ProjectionError("historical K0 prerequisite receipt identity/result differs")
    source = receipt.get("sourceTuple")
    if not isinstance(source, dict):
        raise ProjectionError("historical K0 prerequisite source tuple is absent")
    historical_commit = source.get("nereusCommit")
    historical_sha = source.get("sourceLocksSha256")
    if not isinstance(historical_commit, str) or not COMMIT.fullmatch(historical_commit):
        raise ProjectionError("historical K0 prerequisite commit is not canonical")
    if not isinstance(historical_sha, str) or not SHA256.fullmatch(historical_sha):
        raise ProjectionError("historical K0 source-lock SHA is not canonical")

    historical_raw = git_blob(root, historical_commit, SOURCE_LOCKS)
    if hashlib.sha256(historical_raw).hexdigest() != historical_sha:
        raise ProjectionError("historical source-lock blob differs from the immutable K0 receipt")
    current_raw = regular(root / SOURCE_LOCKS)
    if current_raw != git_blob(root, tested, SOURCE_LOCKS):
        raise ProjectionError("working source-lock input differs from the exact tested commit")
    historical = parse(historical_raw, f"{historical_commit}:{SOURCE_LOCKS}")
    current = parse(current_raw, str(SOURCE_LOCKS))
    added = set(current) - set(historical)
    removed = set(historical) - set(current)
    changed = {key for key in historical.keys() & current.keys() if historical[key] != current[key]}
    if added != CURRENT_ONLY_KEYS or removed or changed:
        raise ProjectionError(
            "current source locks are not immutable historical M2 plus the exact M3-only allowlist: "
            f"added={sorted(added)} removed={sorted(removed)} changed={sorted(changed)}"
        )

    if output is not None:
        if not output.is_absolute():
            raise ProjectionError("projection output path must be absolute")
        try:
            mode = output.parent.lstat().st_mode
        except OSError as error:
            raise ProjectionError(f"projection output parent is absent: {output.parent}") from error
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise ProjectionError("projection output parent is not a non-symlink directory")
        try:
            with output.open("xb") as destination:
                destination.write(historical_raw)
        except OSError as error:
            raise ProjectionError(f"cannot exclusively write projection output: {output}") from error
    return historical_commit, historical_sha, historical_raw


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--tested-commit", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv[1:])
    try:
        historical_commit, historical_sha, _ = validate(
            args.repo_root, args.tested_commit, args.output
        )
    except (ProjectionError, OSError) as error:
        print(f"V2 M3 current-source M2 prerequisite projection: {error}", file=sys.stderr)
        return 1
    print(
        "V2 M3 current-source M2 prerequisite projection verified: "
        f"historical={historical_commit} sourceLocks={historical_sha} "
        f"current={args.tested_commit} m3OnlyKeys={len(CURRENT_ONLY_KEYS)} "
        f"written={args.output is not None}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
