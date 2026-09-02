#!/usr/bin/env python3
"""Build and publish one canonical M4 Final from four validated child receipts."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path, PurePosixPath
import sys


CHECKER_PATH = Path(__file__).with_name("check-v2-m4-evidence.py")
SPEC = importlib.util.spec_from_file_location("check_v2_m4_evidence_for_final_publisher", CHECKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load M4 evidence checker: {CHECKER_PATH}")
CONTRACT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CONTRACT
SPEC.loader.exec_module(CONTRACT)


def final_value(root: Path, tested: str) -> dict:
    source_raw = CONTRACT.git_blob(root, tested, CONTRACT.SOURCE_LOCKS_PATH)
    source_sha = CONTRACT.sha256(source_raw)
    bindings = CONTRACT.source_bindings(root, tested, source_sha)
    child_root = CONTRACT.CHILD_PREFIX / f"final-source-{tested}"
    children = []
    for ordinal, kind in enumerate(CONTRACT.CHILD_KINDS, start=1):
        path = child_root / f"{ordinal:02d}-{kind}" / "receipt.json"
        children.append(CONTRACT.child_identity(root, path, kind, tested))
    return {
        "backendAdmissions": bindings["backendAdmissions"],
        "childReceipts": children,
        "exclusions": CONTRACT.FINAL_EXCLUSIONS,
        "frozenM3": {
            "closureCommit": CONTRACT.M3_CLOSURE_COMMIT,
            "finalPath": str(CONTRACT.M3_FINAL_PATH),
            "finalSha256": CONTRACT.M3_FINAL_SHA256,
            "sourceLocksSha256": CONTRACT.M3_SOURCE_LOCKS_SHA256,
            "testedCommit": CONTRACT.M3_TESTED_COMMIT,
        },
        "kind": CONTRACT.FINAL_KIND,
        "physicalSelection": bindings["physicalSelection"],
        "promotionEligible": True,
        "result": CONTRACT.FINAL_RESULT,
        "scenarios": list(CONTRACT.PROMOTED_SCENARIOS),
        "schema": CONTRACT.FINAL_SCHEMA,
        "sharedPredicates": list(CONTRACT.SHARED_PREDICATES),
        "sourceTuple": {"nereusCommit": tested, "sourceLocksSha256": source_sha},
    }


def publish(root: Path, tested: str, output: PurePosixPath) -> bytes:
    root = CONTRACT.ensure_root(root)
    status = str(CONTRACT.git(root, "status", "--porcelain=v1", "--untracked-files=all", text=True))
    if status:
        raise CONTRACT.EvidenceError("M4 Final publisher requires a clean worktree")
    CONTRACT.validate_descendants(root, tested)
    output = CONTRACT.safe_relative(str(output), "--output", CONTRACT.FINAL_PREFIX)
    destination = root.joinpath(*output.parts)
    if destination.exists() or destination.is_symlink():
        raise CONTRACT.EvidenceError(f"M4 Final publisher refuses to overwrite: {output}")
    value = final_value(root, tested)
    CONTRACT.validate_final_value(root, value, tested)
    raw = CONTRACT.canonical_bytes(value)
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("xb") as stream:
            stream.write(raw)
    except Exception:
        destination.unlink(missing_ok=True)
        raise
    return raw


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--tested-commit", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        tested = CONTRACT.commit(args.tested_commit, "--tested-commit")
        output = CONTRACT.safe_relative(args.output, "--output", CONTRACT.FINAL_PREFIX)
        raw = publish(args.repo_root, tested, output)
    except (CONTRACT.EvidenceError, OSError) as error:
        print(f"V2 M4 Final publisher: {error}", file=sys.stderr)
        return 1
    print(f"V2 M4 Final published: tested={tested} output={output} bytes={len(raw)} sha256={CONTRACT.sha256(raw)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
