#!/usr/bin/env python3
"""Validate the immutable historical V2 M2 Final input consumed by M3.

This checker deliberately does not call or weaken the current-source M2 Final
checker.  M3 may advance ``docs/v2/source-locks.json``; the historical M2
receipt must therefore be checked against the exact source-lock blob stored at
its tested/published commits, never against mutable current-source bytes.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
from typing import Any


ROOT_PATH = PurePosixPath("docs/v2/evidence/v2-m2/final/m2-final.json")
KAFKA_PATH = PurePosixPath("docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json")
PULSAR_PATH = PurePosixPath("docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json")
SOURCE_LOCKS_PATH = PurePosixPath("docs/v2/source-locks.json")

TESTED_SOURCE_COMMIT = "4af3278234d84df7a2fdce4fc6b3e4e227916d56"
PUBLISHED_SOURCE_COMMIT = "0349fd68e04d94085d9c722c7ebc448cbb810d72"
SOURCE_LOCKS_BYTES = 28_306
SOURCE_LOCKS_SHA256 = "35003df5974902d2d92c2f0b18b830b89809f81517e87462c9b548aab012573e"


@dataclass(frozen=True)
class BlobIdentity:
    path: PurePosixPath
    bytes: int
    sha256: str


ROOT_IDENTITY = BlobIdentity(
    ROOT_PATH,
    1_927,
    "2ba2d1cab0547c456ec7e492edaf9b953e9e0d71707770d3c4b4fe8a4d6217dd",
)
KAFKA_IDENTITY = BlobIdentity(
    KAFKA_PATH,
    11_215,
    "1f8af32b548c71f2f389d18160fba534fea1e3e333459fbeb5c9b2667df66ac5",
)
PULSAR_IDENTITY = BlobIdentity(
    PULSAR_PATH,
    9_130,
    "e89dfbb247dc97f0655043c95d38eebf1daf9756ca2394628090a16c7fd4fcd5",
)
HISTORICAL_IDENTITIES = (ROOT_IDENTITY, KAFKA_IDENTITY, PULSAR_IDENTITY)

KAFKA_SCENARIOS = (
    "V2-BK-003",
    "V2-BK-014",
    "V2-BK-015",
    "V2-BK-016",
    "V2-BK-017",
    "V2-KAF-DATA-001",
    "V2-KAF-DATA-002",
    "V2-KAF-DATA-004",
    "V2-KAF-DATA-005",
    "V2-KAF-DATA-014",
)
PULSAR_SCENARIOS = (
    "V2-BK-001",
    "V2-BK-002",
    "V2-BK-004",
    "V2-BK-005",
    "V2-BK-006",
    "V2-BK-007",
    "V2-BK-008",
    "V2-BK-009",
    "V2-BK-010",
    "V2-BK-012",
    "V2-BK-013",
)
PROMOTED_SCENARIOS = tuple(sorted((*KAFKA_SCENARIOS, *PULSAR_SCENARIOS)))
EXCLUSIONS = {
    "excludedMilestones": ["M3_OBJECT_WAL", "M6_PROCESS_ACTIVATION", "M8_NATIVE_PARITY"],
    "plannedScenarioIds": ["V2-BK-011", "V2-PUL-001"],
}

COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")


class M3InputsError(RuntimeError):
    """A stable, fail-closed historical-input rejection."""


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _verify_identity(raw: bytes, identity: BlobIdentity, label: str) -> None:
    actual_sha = _sha256(raw)
    if len(raw) != identity.bytes or actual_sha != identity.sha256:
        raise M3InputsError(
            f"{label} bytes differ for {identity.path}: "
            f"bytes={len(raw)}/{identity.bytes} sha256={actual_sha}/{identity.sha256}"
        )


def _strict_json(raw: bytes, label: str) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise M3InputsError(f"duplicate JSON member {key!r} in {label}")
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise M3InputsError(f"cannot parse strict JSON {label}: {error}") from error
    if not isinstance(value, dict):
        raise M3InputsError(f"JSON root is not an object: {label}")
    return value


def _git(root: Path, *args: str, text: bool = False) -> bytes | str:
    try:
        return subprocess.check_output(
            ["git", "-C", os.fspath(root), *args],
            stderr=subprocess.STDOUT,
            text=text,
        )
    except subprocess.CalledProcessError as error:
        output = error.output if isinstance(error.output, str) else error.output.decode("utf-8", "replace")
        raise M3InputsError(f"git {' '.join(args)} failed: {output.strip()}") from error


def _git_blob(root: Path, commit: str, path: PurePosixPath) -> bytes:
    raw = _git(root, "show", f"{commit}:{path}")
    assert isinstance(raw, bytes)
    return raw


def _safe_worktree_file(root: Path, relative: PurePosixPath) -> bytes:
    current = root
    for component in relative.parts:
        current = current / component
        try:
            mode = current.lstat().st_mode
        except OSError as error:
            raise M3InputsError(f"historical input is missing: {relative}") from error
        if stat.S_ISLNK(mode):
            raise M3InputsError(f"historical input has a symlink component: {relative}")
    if not stat.S_ISREG(mode):
        raise M3InputsError(f"historical input is not a regular file: {relative}")
    try:
        return current.read_bytes()
    except OSError as error:
        raise M3InputsError(f"cannot read historical input: {relative}: {error}") from error


def _verify_index_entry(root: Path, identity: BlobIdentity) -> None:
    stage = str(_git(root, "ls-files", "--stage", "--", str(identity.path), text=True)).strip()
    lines = stage.splitlines()
    fields = lines[0].split(maxsplit=3) if len(lines) == 1 else []
    if len(fields) != 4 or fields[0] != "100644" or fields[3] != str(identity.path):
        raise M3InputsError(f"historical input is not one regular tracked index entry: {identity.path}")
    raw = _git(root, "show", f":{identity.path}")
    assert isinstance(raw, bytes)
    _verify_identity(raw, identity, "index historical input")


def _ensure_commit(root: Path, commit: str, label: str) -> None:
    if not COMMIT_PATTERN.fullmatch(commit):
        raise M3InputsError(f"{label} is not a canonical commit id: {commit}")
    _git(root, "cat-file", "-e", f"{commit}^{{commit}}")


def _ensure_ancestor(root: Path, ancestor: str, descendant: str, label: str) -> None:
    try:
        result = subprocess.run(
            ["git", "-C", os.fspath(root), "merge-base", "--is-ancestor", ancestor, descendant],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError as error:
        raise M3InputsError(f"cannot execute git ancestry check for {label}: {error}") from error
    if result.returncode != 0:
        raise M3InputsError(f"{label} is not an ancestor of {descendant}: {ancestor}")


def _scenario_ids(child: dict[str, Any], expected: tuple[str, ...], label: str) -> tuple[str, ...]:
    scenarios = child.get("scenarios")
    if not isinstance(scenarios, list):
        raise M3InputsError(f"{label} scenarios are not an array")
    values: list[str] = []
    for index, row in enumerate(scenarios):
        if not isinstance(row, dict) or not isinstance(row.get("scenarioId"), str):
            raise M3InputsError(f"{label} scenarios[{index}] lacks a canonical scenarioId")
        values.append(row["scenarioId"])
    ids = tuple(values)
    if len(ids) != len(set(ids)) or ids != expected:
        raise M3InputsError(f"{label} scenario count/order/set differs from immutable M2 Final")
    return ids


def validate_semantics(
    receipt: dict[str, Any], kafka: dict[str, Any], pulsar: dict[str, Any]
) -> None:
    """Validate the explicit M2 Final contract in addition to fixed blob hashes."""

    if set(receipt) != {
        "schema",
        "kind",
        "result",
        "promotionEligible",
        "sourceTuple",
        "childReceipts",
        "promotedScenarios",
        "boundaries",
    }:
        raise M3InputsError("historical M2 Final top-level member set differs")
    if (
        receipt.get("schema") != "NEREUS_V2_M2_FINAL_V1"
        or receipt.get("kind") != "V2_M2_FINAL"
        or receipt.get("result") != "PASS_V2_M2_FINAL"
        or receipt.get("promotionEligible") is not True
    ):
        raise M3InputsError("historical M2 Final schema/kind/result/promotion differs")

    expected_source = {
        "bookKeeperSourceCommit": "cd06340851d6d657b7c7546df01df365c18980de",
        "kafkaFinalReceiptSha256": KAFKA_IDENTITY.sha256,
        "kafkaForkCommit": "8afbc425660f3466bdc3255e3dd4eb43f8685af1",
        "nereusCommit": TESTED_SOURCE_COMMIT,
        "pulsarFinalReceiptSha256": PULSAR_IDENTITY.sha256,
        "pulsarForkCommit": "a14e0e6f4e49be0677318b4ceefc7b85b445823b",
        "sourceLocksSha256": SOURCE_LOCKS_SHA256,
    }
    if receipt.get("sourceTuple") != expected_source:
        raise M3InputsError("historical M2 Final source tuple differs")

    expected_children = [
        {
            "bytes": KAFKA_IDENTITY.bytes,
            "kind": "KAFKA_M2_FINAL",
            "path": str(KAFKA_IDENTITY.path),
            "result": "PASS_KAFKA_M2_FINAL",
            "scenarios": len(KAFKA_SCENARIOS),
            "sha256": KAFKA_IDENTITY.sha256,
        },
        {
            "bytes": PULSAR_IDENTITY.bytes,
            "kind": "PULSAR_M2_FINAL",
            "path": str(PULSAR_IDENTITY.path),
            "result": "PASS_PULSAR_M2_FINAL",
            "scenarios": len(PULSAR_SCENARIOS),
            "sha256": PULSAR_IDENTITY.sha256,
        },
    ]
    if receipt.get("childReceipts") != expected_children:
        raise M3InputsError("historical M2 Final child path/bytes/SHA/result/scenario counts differ")

    kafka_ids = _scenario_ids(kafka, KAFKA_SCENARIOS, "Kafka M2 Final child")
    pulsar_ids = _scenario_ids(pulsar, PULSAR_SCENARIOS, "Pulsar M2 Final child")
    if set(kafka_ids) & set(pulsar_ids) or tuple(sorted((*kafka_ids, *pulsar_ids))) != PROMOTED_SCENARIOS:
        raise M3InputsError("historical child scenarios do not form the exact disjoint 21-scenario union")
    if receipt.get("promotedScenarios") != list(PROMOTED_SCENARIOS):
        raise M3InputsError("historical M2 Final promoted 21-scenario union differs")
    if receipt.get("boundaries") != EXCLUSIONS:
        raise M3InputsError("historical M2 Final M3/M6/M8 exclusions differ")

    expected_child_headers = (
        (
            kafka,
            "NEREUS_V2_M2_KAFKA_FINAL_RECEIPT_V1",
            "KAFKA_M2_FINAL",
            "PASS_KAFKA_M2_FINAL",
            "Kafka",
        ),
        (
            pulsar,
            "NEREUS_V2_M2_PULSAR_FINAL_RECEIPT_V1",
            "PULSAR_M2_FINAL",
            "PASS_PULSAR_M2_FINAL",
            "Pulsar",
        ),
    )
    for child, schema, kind, result, label in expected_child_headers:
        if (
            child.get("schema") != schema
            or child.get("kind") != kind
            or child.get("result") != result
            or child.get("promotionEligible") is not True
        ):
            raise M3InputsError(f"historical {label} child schema/kind/result/promotion differs")
        child_source = child.get("sourceTuple")
        if (
            not isinstance(child_source, dict)
            or child_source.get("nereusCommit") != TESTED_SOURCE_COMMIT
            or child_source.get("sourceLocksSha256") != SOURCE_LOCKS_SHA256
        ):
            raise M3InputsError(f"historical {label} child tested source/source-lock differs")
    if pulsar.get("sourceTuple", {}).get("kafkaFinalReceiptSha256") != KAFKA_IDENTITY.sha256:
        raise M3InputsError("historical Pulsar child does not bind the exact Kafka child")


def check_inputs(root: Path) -> str:
    try:
        root = root.resolve(strict=True)
    except OSError as error:
        raise M3InputsError(f"repository root is missing: {root}") from error
    top = Path(str(_git(root, "rev-parse", "--show-toplevel", text=True)).strip()).resolve(strict=True)
    if top != root:
        raise M3InputsError(f"repository root differs from Git top-level: {root}")

    head = str(_git(root, "rev-parse", "HEAD", text=True)).strip()
    _ensure_commit(root, TESTED_SOURCE_COMMIT, "historical tested source")
    _ensure_commit(root, PUBLISHED_SOURCE_COMMIT, "historical published source")
    _ensure_commit(root, head, "current M3 source")
    _ensure_ancestor(root, TESTED_SOURCE_COMMIT, PUBLISHED_SOURCE_COMMIT, "historical tested source")
    _ensure_ancestor(root, TESTED_SOURCE_COMMIT, head, "historical tested source")
    _ensure_ancestor(root, PUBLISHED_SOURCE_COMMIT, head, "published M2 Final commit")

    parsed: dict[PurePosixPath, dict[str, Any]] = {}
    for identity in HISTORICAL_IDENTITIES:
        worktree_raw = _safe_worktree_file(root, identity.path)
        _verify_identity(worktree_raw, identity, "worktree historical input")
        _verify_index_entry(root, identity)
        head_raw = _git_blob(root, head, identity.path)
        _verify_identity(head_raw, identity, "HEAD historical input")
        published_raw = _git_blob(root, PUBLISHED_SOURCE_COMMIT, identity.path)
        _verify_identity(published_raw, identity, "published historical input")
        if not (worktree_raw == head_raw == published_raw):
            raise M3InputsError(
                "historical input byte equality differs across worktree/index/HEAD/publish: "
                f"{identity.path}"
            )
        parsed[identity.path] = _strict_json(worktree_raw, str(identity.path))

    for commit, label in (
        (TESTED_SOURCE_COMMIT, "tested-source historical source-lock"),
        (PUBLISHED_SOURCE_COMMIT, "published-source historical source-lock"),
    ):
        source_locks = _git_blob(root, commit, SOURCE_LOCKS_PATH)
        source_identity = BlobIdentity(SOURCE_LOCKS_PATH, SOURCE_LOCKS_BYTES, SOURCE_LOCKS_SHA256)
        _verify_identity(source_locks, source_identity, label)
        _strict_json(source_locks, f"{commit}:{SOURCE_LOCKS_PATH}")

    validate_semantics(parsed[ROOT_PATH], parsed[KAFKA_PATH], parsed[PULSAR_PATH])
    return head


def main(argv: list[str]) -> int:
    if len(argv) > 2:
        print(f"usage: {argv[0]} [REPO_ROOT]", file=sys.stderr)
        return 2
    root = Path(argv[1]) if len(argv) == 2 else Path(__file__).resolve().parent.parent
    try:
        head = check_inputs(root)
    except (M3InputsError, OSError) as error:
        print(f"V2 M3 Inputs gate: {error}", file=sys.stderr)
        return 1
    print(
        "V2 M3 historical M2 inputs verified: "
        f"tested={TESTED_SOURCE_COMMIT} published={PUBLISHED_SOURCE_COMMIT} "
        f"head={head} children=2 scenarios=21 exclusions=M3/M6/M8 "
        f"sourceLocksBytes={SOURCE_LOCKS_BYTES} sourceLocksSha256={SOURCE_LOCKS_SHA256}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
