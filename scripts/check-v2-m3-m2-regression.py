#!/usr/bin/env python3
"""Validate a full trusted current-source M2 regression receipt for M3.

The receipt is intentionally non-promotable.  It proves that the complete M2
profile ran against one exact Nereus commit; it is not an M3 scenario or Final
receipt.  This module also owns the closed schemas used by the publisher.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
from typing import Any


RECEIPT_SCHEMA = "NEREUS_V2_M3_CURRENT_SOURCE_M2_REGRESSION_V1"
RECEIPT_KIND = "CURRENT_SOURCE_M2_REGRESSION"
RECEIPT_RESULT = "PASS_CURRENT_SOURCE_M2_REGRESSION_ONLY"
CHILD_SCHEMA = "NEREUS_V2_M3_M2_TRUSTED_CHILD_RESULT_V1"
EXECUTION_PROFILE = "TRUSTED_FULL_CURRENT_SOURCE_M2"
EVIDENCE_PREFIX = PurePosixPath("docs/v2/evidence/v2-m3")
HISTORICAL_FINAL_PATH = PurePosixPath("docs/v2/evidence/v2-m2/final/m2-final.json")
HISTORICAL_FINAL_BYTES = 1_927
HISTORICAL_FINAL_SHA256 = "2ba2d1cab0547c456ec7e492edaf9b953e9e0d71707770d3c4b4fe8a4d6217dd"
HISTORICAL_TESTED_COMMIT = "4af3278234d84df7a2fdce4fc6b3e4e227916d56"
HISTORICAL_PUBLISHED_COMMIT = "0349fd68e04d94085d9c722c7ebc448cbb810d72"

REQUIRED_GATES = (
    "KAFKA_K0",
    "KAFKA_K1",
    "KAFKA_K2",
    "KAFKA_K3",
    "KAFKA_K4",
    "KAFKA_K5",
    "KAFKA_K6",
    "KAFKA_K7",
    "KAFKA_K8",
    "KAFKA_K9",
    "KAFKA_K10",
    "KAFKA_EXACT",
    "KAFKA_REAL_BOOKKEEPER",
    "KAFKA_SCALE_10000",
    "KAFKA_SCALE_100000",
    "PULSAR_P0",
    "PULSAR_P1",
    "PULSAR_P2",
    "PULSAR_P3",
    "PULSAR_P4",
    "PULSAR_P5",
    "PULSAR_P6",
    "PULSAR_NATIVE",
    "PULSAR_P6_PROVIDER",
    "PULSAR_FINAL_PARSER_POLICY",
)

EXCLUSIONS = (
    "M3_IMPLEMENTATION_AND_FINAL",
    "M6_PROCESS_ACTIVATION",
    "M8_NATIVE_PARITY",
    "SCENARIO_PROMOTION",
)

COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class RegressionError(RuntimeError):
    """A stable, fail-closed regression receipt rejection."""


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _reject_duplicate_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RegressionError(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def _validate_jcs_domain(value: Any, label: str) -> None:
    if value is None or type(value) is bool:
        return
    if type(value) is int:
        if abs(value) > 9_007_199_254_740_991:
            raise RegressionError(f"integer exceeds the closed JCS safe domain in {label}")
        return
    if isinstance(value, str):
        if not value.isascii():
            raise RegressionError(f"non-ASCII string is outside the closed JCS schema in {label}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_jcs_domain(item, f"{label}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key.isascii():
                raise RegressionError(f"non-ASCII object key is outside the closed JCS schema in {label}")
            _validate_jcs_domain(item, f"{label}.{key}")
        return
    raise RegressionError(f"unsupported JCS value type in {label}: {type(value).__name__}")


def canonical_bytes(value: dict[str, Any]) -> bytes:
    _validate_jcs_domain(value, "root")
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def load_canonical_json(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_members)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RegressionError(f"cannot parse JSON {label}: {error}") from error
    if not isinstance(value, dict):
        raise RegressionError(f"JSON root is not an object: {label}")
    if canonical_bytes(value) != raw:
        raise RegressionError(f"JSON is not exact closed-domain JCS: {label}")
    return value


def safe_relative(value: object, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise RegressionError(f"{label} is not a safe POSIX-relative path")
    parts = value.split("/")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in ("", ".", "..") for part in parts):
        raise RegressionError(f"{label} is not a safe POSIX-relative path")
    return path


def is_under(path: PurePosixPath, parent: PurePosixPath) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def read_safe_file(root: Path, relative: PurePosixPath) -> bytes:
    current = root
    mode = 0
    for part in relative.parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except OSError as error:
            raise RegressionError(f"attachment is missing: {relative}") from error
        if stat.S_ISLNK(mode):
            raise RegressionError(f"attachment has a symlink component: {relative}")
    if not stat.S_ISREG(mode):
        raise RegressionError(f"attachment is not a regular file: {relative}")
    return current.read_bytes()


def git(root: Path, *args: str, text: bool = False) -> bytes | str:
    try:
        return subprocess.check_output(
            ["git", "-C", os.fspath(root), *args],
            stderr=subprocess.STDOUT,
            text=text,
        )
    except subprocess.CalledProcessError as error:
        output = error.output if isinstance(error.output, str) else error.output.decode("utf-8", "replace")
        raise RegressionError(f"git {' '.join(args)} failed: {output.strip()}") from error


def git_blob(root: Path, commit: str, path: PurePosixPath) -> bytes:
    raw = git(root, "show", f"{commit}:{path}")
    assert isinstance(raw, bytes)
    return raw


def current_head(root: Path) -> str:
    value = str(git(root, "rev-parse", "HEAD", text=True)).strip()
    if not COMMIT_PATTERN.fullmatch(value):
        raise RegressionError("current HEAD is not a canonical commit")
    return value


def ensure_root(root: Path) -> Path:
    try:
        root = root.resolve(strict=True)
        top = Path(str(git(root, "rev-parse", "--show-toplevel", text=True)).strip()).resolve(strict=True)
    except OSError as error:
        raise RegressionError(f"cannot resolve repository root: {root}") from error
    if top != root:
        raise RegressionError(f"repository root differs from Git top-level: {root}")
    return root


def ensure_ancestor(root: Path, ancestor: str, descendant: str) -> None:
    if not COMMIT_PATTERN.fullmatch(ancestor):
        raise RegressionError(f"tested Nereus commit is not canonical: {ancestor}")
    try:
        result = subprocess.run(
            ["git", "-C", os.fspath(root), "merge-base", "--is-ancestor", ancestor, descendant],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError as error:
        raise RegressionError(f"cannot execute ancestry check: {error}") from error
    if result.returncode != 0:
        raise RegressionError(f"tested Nereus commit is not an ancestor of HEAD: {ancestor}")


def expected_sources(tested_commit: str) -> dict[str, Any]:
    return {
        "bookKeeper": {
            "commit": "cd06340851d6d657b7c7546df01df365c18980de",
            "repository": "https://github.com/apache/bookkeeper.git",
            "serverImage": (
                "apache/bookkeeper@sha256:"
                "c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d"
            ),
            "tag": "release-4.18.0",
            "tagObject": "bb51381cfb1126a79000cd3211f6293dbd982554",
        },
        "kafka": {
            "branch": "nereus/future9-native-kafka-storage",
            "commit": "8afbc425660f3466bdc3255e3dd4eb43f8685af1",
            "repository": "https://github.com/nereusstream/kafka.git",
        },
        "objectProviders": [
            {
                "imageId": "sha256:ad76d8f93de9cb765653983d32f4b2994ca981b8f6ccfcf7b52b2d1800b18581",
                "kind": "LOCALSTACK_S3_PROTOCOL",
                "reference": "localstack/localstack:4.14.0",
                "repoDigest": (
                    "localstack/localstack@sha256:"
                    "3ebc37595918b8accb852f8048fef2aff047d465167edd655528065b07bc364a"
                ),
            },
            {
                "imageId": "sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253",
                "kind": "MINIO_S3_COMPATIBLE",
                "reference": "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z",
                "repoDigest": (
                    "quay.io/minio/minio@sha256:"
                    "14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
                ),
            },
        ],
        "providerAdapter": {
            "awsSdkV2": "2.47.5",
            "module": "nereus-pulsar-offload",
            "nereusCommit": tested_commit,
            "repository": "https://github.com/nereusstream/nereus.git",
        },
        "pulsar": {
            "branch": "nereus/v2-m2-pulsar-native-offload",
            "commit": "a14e0e6f4e49be0677318b4ceefc7b85b445823b",
            "repository": "https://github.com/nereusstream/pulsar.git",
        },
    }


def historical_final_identity() -> dict[str, Any]:
    return {
        "bytes": HISTORICAL_FINAL_BYTES,
        "path": str(HISTORICAL_FINAL_PATH),
        "publishedNereusCommit": HISTORICAL_PUBLISHED_COMMIT,
        "sha256": HISTORICAL_FINAL_SHA256,
        "testedNereusCommit": HISTORICAL_TESTED_COMMIT,
    }


def attachment_identity(path: PurePosixPath, raw: bytes) -> dict[str, Any]:
    return {"bytes": len(raw), "path": str(path), "sha256": sha256(raw)}


def validate_attachment_identity(value: object, label: str) -> tuple[PurePosixPath, int, str]:
    if not isinstance(value, dict) or set(value) != {"bytes", "path", "sha256"}:
        raise RegressionError(f"{label} attachment identity member set differs")
    path = safe_relative(value.get("path"), f"{label}.path")
    byte_count = value.get("bytes")
    digest = value.get("sha256")
    if type(byte_count) is not int or byte_count <= 0:
        raise RegressionError(f"{label}.bytes is not positive")
    if not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest):
        raise RegressionError(f"{label}.sha256 is not canonical")
    return path, byte_count, digest


def validate_child_result(value: dict[str, Any], tested_commit: str) -> None:
    if set(value) != {
        "errors",
        "executionProfile",
        "failures",
        "gateId",
        "result",
        "schema",
        "skipped",
        "testedNereusCommit",
        "tests",
    }:
        raise RegressionError("trusted child result member set differs")
    gate = value.get("gateId")
    if gate not in REQUIRED_GATES:
        raise RegressionError(f"trusted child gate is not in the full-profile allowlist: {gate}")
    if (
        value.get("schema") != CHILD_SCHEMA
        or value.get("executionProfile") != EXECUTION_PROFILE
        or value.get("result") != "PASS"
        or value.get("testedNereusCommit") != tested_commit
    ):
        raise RegressionError(f"trusted child result identity/profile/source differs: {gate}")
    if type(value.get("tests")) is not int or value["tests"] <= 0:
        raise RegressionError(f"trusted child gate has zero tests: {gate}")
    for counter in ("failures", "errors", "skipped"):
        if type(value.get(counter)) is not int or value[counter] != 0:
            raise RegressionError(f"trusted child gate has non-zero {counter}: {gate}")


def _changed_paths(root: Path, parent: str, commit: str) -> list[str]:
    raw = git(root, "diff", "--no-renames", "--name-only", "-z", parent, commit)
    assert isinstance(raw, bytes)
    try:
        return [entry.decode("utf-8") for entry in raw.split(b"\0") if entry]
    except UnicodeDecodeError as error:
        raise RegressionError(f"evidence descendant has a non-UTF-8 path: {commit}") from error


def validate_evidence_descendants(root: Path, tested: str, head: str) -> int:
    ensure_ancestor(root, tested, head)
    if tested == head:
        return 0
    commits = str(git(root, "rev-list", "--reverse", f"{tested}..{head}", text=True)).splitlines()
    if not commits:
        raise RegressionError("tested source differs from HEAD without an evidence descendant")
    for commit in commits:
        parents = str(git(root, "show", "-s", "--format=%P", commit, text=True)).strip().split()
        if len(parents) != 1:
            raise RegressionError(f"evidence-only descendant contains a merge commit: {commit}")
        paths = _changed_paths(root, parents[0], commit)
        if not paths:
            raise RegressionError(f"evidence-only descendant contains an empty commit: {commit}")
        forbidden = sorted(
            path for path in paths if not is_under(PurePosixPath(path), EVIDENCE_PREFIX)
        )
        if forbidden:
            raise RegressionError(
                f"non-evidence path changed after tested source at {commit}: {', '.join(forbidden)}"
            )
    return len(commits)


def _verify_bound_file(
    root: Path,
    identity: object,
    label: str,
    head: str,
    require_head_blob: bool,
) -> tuple[PurePosixPath, bytes]:
    path, byte_count, digest = validate_attachment_identity(identity, label)
    raw = read_safe_file(root, path)
    if len(raw) != byte_count or sha256(raw) != digest:
        raise RegressionError(f"{label} attachment bytes/SHA differ: {path}")
    if require_head_blob:
        committed = git_blob(root, head, path)
        if committed != raw:
            raise RegressionError(f"{label} attachment differs from HEAD: {path}")
        index = git(root, "show", f":{path}")
        assert isinstance(index, bytes)
        if index != raw:
            raise RegressionError(f"{label} attachment differs from index: {path}")
    return path, raw


def validate_receipt(root: Path, receipt_path: PurePosixPath, expected_tested: str) -> tuple[str, int]:
    root = ensure_root(root)
    if not is_under(receipt_path, EVIDENCE_PREFIX):
        raise RegressionError(f"regression receipt is outside {EVIDENCE_PREFIX}: {receipt_path}")
    head = current_head(root)
    evidence_commits = validate_evidence_descendants(root, expected_tested, head)
    receipt_raw = read_safe_file(root, receipt_path)
    receipt = load_canonical_json(receipt_raw, str(receipt_path))
    if evidence_commits:
        committed = git_blob(root, head, receipt_path)
        if committed != receipt_raw:
            raise RegressionError("regression receipt differs from HEAD")
        index = git(root, "show", f":{receipt_path}")
        assert isinstance(index, bytes)
        if index != receipt_raw:
            raise RegressionError("regression receipt differs from index")

    if set(receipt) != {
        "childGates",
        "evidenceClass",
        "exclusions",
        "historicalFinal",
        "kind",
        "m2AmendmentLineage",
        "promotionEligible",
        "result",
        "scenarioPromotion",
        "schema",
        "sources",
        "testedNereusCommit",
    }:
        raise RegressionError("regression receipt top-level member set differs")
    if (
        receipt.get("schema") != RECEIPT_SCHEMA
        or receipt.get("kind") != RECEIPT_KIND
        or receipt.get("result") != RECEIPT_RESULT
        or receipt.get("evidenceClass") != EXECUTION_PROFILE
        or receipt.get("promotionEligible") is not False
        or receipt.get("scenarioPromotion") is not False
    ):
        raise RegressionError("regression schema/kind/result/profile/promotion boundary differs")
    if receipt.get("testedNereusCommit") != expected_tested:
        raise RegressionError("regression tested commit differs from the specified/current tested source")
    if receipt.get("m2AmendmentLineage") != []:
        raise RegressionError("v1 regression accepts no implicit M2 Amendment lineage")
    if receipt.get("historicalFinal") != historical_final_identity():
        raise RegressionError("historical M2 Final attachment identity differs")
    if receipt.get("sources") != expected_sources(expected_tested):
        raise RegressionError("Kafka/Pulsar/BookKeeper/Provider source identities differ")
    if receipt.get("exclusions") != list(EXCLUSIONS):
        raise RegressionError("M3/M6/M8/scenario exclusions differ")

    historical_raw = read_safe_file(root, HISTORICAL_FINAL_PATH)
    if len(historical_raw) != HISTORICAL_FINAL_BYTES or sha256(historical_raw) != HISTORICAL_FINAL_SHA256:
        raise RegressionError("historical M2 Final attachment bytes/SHA differ")
    if git_blob(root, head, HISTORICAL_FINAL_PATH) != historical_raw:
        raise RegressionError("historical M2 Final attachment differs from HEAD")
    historical_index = git(root, "show", f":{HISTORICAL_FINAL_PATH}")
    assert isinstance(historical_index, bytes)
    if historical_index != historical_raw:
        raise RegressionError("historical M2 Final attachment differs from index")

    rows = receipt.get("childGates")
    if not isinstance(rows, list) or len(rows) != len(REQUIRED_GATES):
        raise RegressionError("regression child gate count differs from the full-profile allowlist")
    observed: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or set(row) != {
            "attachment",
            "errors",
            "failures",
            "gateId",
            "result",
            "skipped",
            "tests",
        }:
            raise RegressionError(f"regression childGates[{index}] member set differs")
        gate = row.get("gateId")
        if gate != REQUIRED_GATES[index]:
            raise RegressionError("regression child gate order/set differs from the full-profile allowlist")
        observed.append(gate)
        expected_attachment = receipt_path.parent / "attachments" / f"{gate}.json"
        path, child_raw = _verify_bound_file(
            root,
            row.get("attachment"),
            f"child gate {gate}",
            head,
            evidence_commits > 0,
        )
        if path != expected_attachment:
            raise RegressionError(f"child gate attachment path differs: {gate}")
        child = load_canonical_json(child_raw, str(path))
        validate_child_result(child, expected_tested)
        if child.get("gateId") != gate:
            raise RegressionError(f"child attachment gate differs from receipt row: {gate}")
        for counter in ("tests", "failures", "errors", "skipped"):
            if row.get(counter) != child.get(counter):
                raise RegressionError(f"child gate counter differs from attachment: {gate}:{counter}")
        if row.get("result") != "PASS":
            raise RegressionError(f"child gate result is not PASS: {gate}")
    if tuple(observed) != REQUIRED_GATES:
        raise RegressionError("regression full-profile child allowlist differs")
    return head, evidence_commits


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--tested-commit")
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        root = ensure_root(args.repo_root)
        head = current_head(root)
        expected = args.tested_commit or head
        receipt = safe_relative(args.receipt, "--receipt")
        validated_head, descendants = validate_receipt(root, receipt, expected)
    except (RegressionError, OSError) as error:
        print(f"V2 M3 current-source M2 regression check: {error}", file=sys.stderr)
        return 1
    print(
        "V2 M3 current-source M2 regression verified: "
        f"tested={expected} head={validated_head} evidenceCommits={descendants} "
        f"profile={EXECUTION_PROFILE} childGates={len(REQUIRED_GATES)} "
        "promotionEligible=false scenarioPromotion=false exclusions=M3/M6/M8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
