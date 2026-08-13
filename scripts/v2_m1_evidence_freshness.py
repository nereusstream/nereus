#!/usr/bin/env python3
"""Fail-closed binding between committed M1 Final evidence and this checkout."""

from __future__ import annotations

import hashlib
import json
import pathlib
import re
import subprocess
import sys
from typing import Any


FINAL_INDEX_RELATIVE = pathlib.PurePosixPath("docs/v2/evidence/v2-m1/n3/final-index.json")
EVIDENCE_PREFIX = "docs/v2/evidence/v2-m1/n3/"
SOURCE_LOCKS_RELATIVE = pathlib.PurePosixPath("docs/v2/source-locks.json")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class FreshnessError(RuntimeError):
    """A stable, user-facing freshness rejection."""


def _git(root: pathlib.Path, *args: str, text: bool = True) -> str | bytes:
    try:
        return subprocess.check_output(
            ["git", "-C", str(root), *args],
            stderr=subprocess.STDOUT,
            text=text,
        )
    except subprocess.CalledProcessError as error:
        detail = error.output.strip() if isinstance(error.output, str) else error.output.decode().strip()
        raise FreshnessError(f"git {' '.join(args)} failed: {detail}") from error


def _load_json(path: pathlib.Path) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise FreshnessError(f"duplicate JSON member {key!r} in {path}")
            result[key] = value
        return result

    try:
        value = json.loads(path.read_bytes(), object_pairs_hook=reject_duplicates)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FreshnessError(f"cannot read strict JSON from {path}: {error}") from error
    if not isinstance(value, dict):
        raise FreshnessError(f"JSON root is not an object: {path}")
    return value


def _safe_relative_path(value: object, label: str) -> pathlib.PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise FreshnessError(f"{label} is not a safe POSIX-relative path")
    raw_parts = value.split("/")
    path = pathlib.PurePosixPath(value)
    if path.is_absolute() or any(part in ("", ".", "..") for part in raw_parts):
        raise FreshnessError(f"{label} is not a safe POSIX-relative path")
    return path


def _receipt_source_tuple(index_path: pathlib.Path) -> tuple[str, str]:
    index = _load_json(index_path)
    references = index.get("receiptRefs")
    if not isinstance(references, list) or not references:
        raise FreshnessError("Final index has no receipt references")

    nereus_commits: set[str] = set()
    source_lock_hashes: set[str] = set()
    for position, reference in enumerate(references):
        if not isinstance(reference, dict):
            raise FreshnessError(f"receiptRefs[{position}] is not an object")
        relative = _safe_relative_path(reference.get("path"), f"receiptRefs[{position}].path")
        receipt_path = index_path.parent
        for component in relative.parts:
            receipt_path = receipt_path / component
            if receipt_path.is_symlink():
                raise FreshnessError(f"receipt reference has a symlink component: {relative}")
        if not receipt_path.is_file():
            raise FreshnessError(f"receipt reference is missing or unsafe: {relative}")
        receipt = _load_json(receipt_path)
        source_tuple = receipt.get("sourceTuple")
        if not isinstance(source_tuple, dict):
            raise FreshnessError(f"receipt has no sourceTuple object: {relative}")
        nereus_commit = source_tuple.get("nereusCommit")
        source_locks_sha = source_tuple.get("sourceLocksSha256")
        if not isinstance(nereus_commit, str) or not COMMIT_PATTERN.fullmatch(nereus_commit):
            raise FreshnessError(f"receipt has an invalid Nereus commit: {relative}")
        if not isinstance(source_locks_sha, str) or not SHA256_PATTERN.fullmatch(source_locks_sha):
            raise FreshnessError(f"receipt has an invalid source-lock digest: {relative}")
        nereus_commits.add(nereus_commit)
        source_lock_hashes.add(source_locks_sha)

    if len(nereus_commits) != 1 or len(source_lock_hashes) != 1:
        raise FreshnessError("Final receipts do not share one Nereus commit and source-lock digest")
    return next(iter(nereus_commits)), next(iter(source_lock_hashes))


def _changed_paths(root: pathlib.Path, parent: str, commit: str) -> list[str]:
    raw = _git(root, "diff", "--no-renames", "--name-only", "-z", parent, commit, text=False)
    assert isinstance(raw, bytes)
    try:
        return [entry.decode("utf-8") for entry in raw.split(b"\0") if entry]
    except UnicodeDecodeError as error:
        raise FreshnessError(f"commit contains a non-UTF-8 path: {commit}") from error


def check_freshness(root: pathlib.Path, final_index: pathlib.Path) -> tuple[str, str, int, str]:
    root = root.resolve(strict=True)
    expected_index = root.joinpath(*FINAL_INDEX_RELATIVE.parts)
    try:
        resolved_index = final_index.resolve(strict=True)
    except OSError as error:
        raise FreshnessError(f"Final index is missing: {final_index}") from error
    if resolved_index != expected_index or final_index.is_symlink() or not final_index.is_file():
        raise FreshnessError(f"Final index must be the committed authority {FINAL_INDEX_RELATIVE}")

    try:
        top_level = pathlib.Path(str(_git(root, "rev-parse", "--show-toplevel")).strip()).resolve(strict=True)
    except OSError as error:
        raise FreshnessError("cannot resolve Git top-level directory") from error
    if top_level != root:
        raise FreshnessError(f"repository root differs from Git top-level: {root}")

    dirty = str(_git(root, "status", "--porcelain=v1", "--untracked-files=all"))
    if dirty:
        raise FreshnessError("Nereus worktree is dirty")

    tested_commit, expected_source_locks_sha = _receipt_source_tuple(resolved_index)
    source_locks_path = root.joinpath(*SOURCE_LOCKS_RELATIVE.parts)
    if source_locks_path.is_symlink() or not source_locks_path.is_file():
        raise FreshnessError(f"source-lock authority is missing or unsafe: {SOURCE_LOCKS_RELATIVE}")
    actual_source_locks_sha = hashlib.sha256(source_locks_path.read_bytes()).hexdigest()
    if actual_source_locks_sha != expected_source_locks_sha:
        raise FreshnessError(
            "current source-lock bytes do not match the Final receipt "
            f"({actual_source_locks_sha} != {expected_source_locks_sha})"
        )

    _git(root, "cat-file", "-e", f"{tested_commit}^{{commit}}")
    try:
        subprocess.run(
            ["git", "-C", str(root), "merge-base", "--is-ancestor", tested_commit, "HEAD"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as error:
        raise FreshnessError(f"tested Nereus commit is not an ancestor of HEAD: {tested_commit}") from error

    head = str(_git(root, "rev-parse", "HEAD")).strip()
    if head == tested_commit:
        raise FreshnessError("Final evidence must be committed after the tested Nereus source commit")

    commits = str(_git(root, "rev-list", "--reverse", f"{tested_commit}..{head}")).splitlines()
    if not commits:
        raise FreshnessError("no evidence-only descendant commit follows the tested source")
    for commit in commits:
        parents = str(_git(root, "show", "-s", "--format=%P", commit)).strip().split()
        if len(parents) != 1:
            raise FreshnessError(f"evidence-only descendant history contains a merge commit: {commit}")
        paths = _changed_paths(root, parents[0], commit)
        if not paths:
            raise FreshnessError(f"evidence-only descendant history contains an empty commit: {commit}")
        forbidden = sorted(path for path in paths if not path.startswith(EVIDENCE_PREFIX))
        if forbidden:
            raise FreshnessError(
                f"non-evidence path changed after tested source at {commit}: {', '.join(forbidden)}"
            )

    return tested_commit, head, len(commits), actual_source_locks_sha


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(f"usage: {argv[0]} REPO_ROOT FINAL_INDEX", file=sys.stderr)
        return 2
    try:
        tested, head, count, locks_sha = check_freshness(pathlib.Path(argv[1]), pathlib.Path(argv[2]))
    except FreshnessError as error:
        print(f"V2 M1 evidence freshness check: {error}", file=sys.stderr)
        return 1
    print(
        "V2 M1 evidence freshness passed: "
        f"tested={tested} head={head} evidenceCommits={count} sourceLocksSha256={locks_sha}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
