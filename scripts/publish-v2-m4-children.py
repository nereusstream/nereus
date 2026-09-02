#!/usr/bin/env python3
"""Publish a prebuilt M4 child package without overwrite and validate every byte."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path, PurePosixPath
import shutil
import sys


CHECKER_PATH = Path(__file__).with_name("check-v2-m4-evidence.py")
SPEC = importlib.util.spec_from_file_location("check_v2_m4_evidence_for_child_publisher", CHECKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load M4 evidence checker: {CHECKER_PATH}")
CONTRACT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CONTRACT
SPEC.loader.exec_module(CONTRACT)


def package_files(package: Path) -> list[tuple[PurePosixPath, Path]]:
    if not package.is_dir() or package.is_symlink():
        raise CONTRACT.EvidenceError("M4 package is not a regular directory")
    files: list[tuple[PurePosixPath, Path]] = []
    for path in sorted(package.rglob("*")):
        if path.is_symlink():
            raise CONTRACT.EvidenceError(f"M4 package contains a symlink: {path}")
        if path.is_file():
            relative = PurePosixPath(path.relative_to(package).as_posix())
            CONTRACT.safe_relative(str(relative), "package path", CONTRACT.CHILD_PREFIX)
            files.append((relative, path))
    if not files:
        raise CONTRACT.EvidenceError("M4 package contains no files")
    return files


def publish(root: Path, package: Path, tested: str) -> int:
    root = CONTRACT.ensure_root(root)
    status = str(CONTRACT.git(root, "status", "--porcelain=v1", "--untracked-files=all", text=True))
    if status:
        raise CONTRACT.EvidenceError("M4 child publisher requires a clean worktree")
    if str(CONTRACT.git(root, "rev-parse", "HEAD", text=True)).strip() != tested:
        raise CONTRACT.EvidenceError("M4 child publisher requires HEAD at the exact tested commit")
    files = package_files(package)
    expected: set[PurePosixPath] = set()
    child_root = CONTRACT.CHILD_PREFIX / f"final-source-{tested}"
    for ordinal, kind in enumerate(CONTRACT.CHILD_KINDS, start=1):
        parent = child_root / f"{ordinal:02d}-{kind}"
        expected.add(parent / "receipt.json")
        for index, attachment_kind in enumerate(CONTRACT.ATTACHMENTS[kind]):
            expected.add(parent / "attachments" / f"{index:02d}-{attachment_kind}.json")
    actual = {relative for relative, _ in files}
    if actual != expected:
        raise CONTRACT.EvidenceError(
            f"M4 package file inventory differs: missing={sorted(map(str, expected - actual))} "
            f"extra={sorted(map(str, actual - expected))}"
        )
    created: list[Path] = []
    try:
        for relative, source in files:
            destination = root.joinpath(*relative.parts)
            if destination.exists() or destination.is_symlink():
                raise CONTRACT.EvidenceError(f"M4 child publisher refuses to overwrite: {relative}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb") as output:
                output.write(source.read_bytes())
            created.append(destination)
        for ordinal, kind in enumerate(CONTRACT.CHILD_KINDS, start=1):
            receipt = child_root / f"{ordinal:02d}-{kind}" / "receipt.json"
            CONTRACT.validate_child(root, receipt, kind, tested)
    except Exception:
        for path in reversed(created):
            path.unlink(missing_ok=True)
        for path in sorted({item.parent for item in created}, key=lambda value: len(value.parts), reverse=True):
            try:
                path.rmdir()
            except OSError:
                pass
        raise
    return len(created)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--tested-commit", required=True)
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        tested = CONTRACT.commit(args.tested_commit, "--tested-commit")
        count = publish(args.repo_root, args.package.resolve(), tested)
    except (CONTRACT.EvidenceError, OSError) as error:
        print(f"V2 M4 child publisher: {error}", file=sys.stderr)
        return 1
    print(f"V2 M4 children published: tested={tested} files={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
