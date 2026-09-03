#!/usr/bin/env python3
"""Validate M4's immutable historical M3 dependency without recertifying current HEAD."""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
from types import ModuleType
from typing import Any, Callable


TESTED = "e5e53e62865c21845621037bea5f18c092bd4259"
CLOSURE = "efab430aed37b3f7c32d09b88ae935c1aea1c902"
FINAL_PATH = PurePosixPath(
    "docs/v2/evidence/v2-m3/final/"
    "e5e53e62865c21845621037bea5f18c092bd4259/m3-final.json"
)
FINAL_SHA256 = "81c7004a923e5b96cab0a3c8b4f1fa26d71606a2208bbabe779f0d872f84f84a"
SOURCE_LOCK_SHA256 = "2a46f31c90912f3f3f10d2365b9f9ffcc8070a847c4c7e202177ea903cdc240b"
LEGACY_FINAL_PATH = PurePosixPath("docs/v2/evidence/v2-m3/final/m3-final.json")
EXPECTED_EXCLUSIONS = ["M6_PROCESS_ACTIVATION", "M8_NATIVE_PARITY"]


class DependencyError(RuntimeError):
    """Fail-closed historical-dependency rejection."""


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
        raise DependencyError(f"M4 historical dependency requires exact M3 Final path: {FINAL_PATH}")


def require_fixed_final(raw: bytes) -> None:
    actual = sha256(raw)
    if actual != FINAL_SHA256:
        raise DependencyError(f"M3 Final SHA-256 differs: {actual} != {FINAL_SHA256}")


def load_m3_contract(root: Path) -> ModuleType:
    path = root / "scripts/check-v2-m3-final.py"
    spec = importlib.util.spec_from_file_location("nereus_v2_m3_final_contract_for_m4", path)
    if spec is None or spec.loader is None:
        raise DependencyError(f"cannot load canonical M3 Final validator: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def archived_allocator_file_rows(
    contract: ModuleType,
    rows: object,
    label: str,
    maximum_count: int,
    maximum_bytes: int,
    required_names: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    """Validate the frozen identity inventory without reopening workstation paths."""

    if not isinstance(rows, list) or not 1 <= len(rows) <= maximum_count:
        raise contract.ChildError(f"{label} file count is outside its cap")
    validated: list[dict[str, Any]] = []
    for index, value in enumerate(rows):
        members = {"bytes", "path", "sha256"}
        if required_names is not None:
            members.add("name")
        row = contract._exact_members(value, members, f"{label}[{index}]")
        contract._allocator_external_path(row["path"], f"{label}[{index}].path", True)
        size = contract._positive(row["bytes"], f"{label}[{index}].bytes")
        digest = contract._sha_text(row["sha256"], f"{label}[{index}].sha256")
        if size > maximum_bytes or digest == "0" * 64:
            raise contract.ChildError(f"{label}[{index}] identity is outside its cap or zero")
        validated.append(row)
    paths = [row["path"] for row in validated]
    if len(paths) != len(set(paths)):
        raise contract.ChildError(f"{label} paths are duplicated")
    if required_names is None:
        if paths != sorted(paths):
            raise contract.ChildError(f"{label} paths are not sorted")
    elif [row["name"] for row in validated] != list(required_names):
        raise contract.ChildError(f"{label} logical-name inventory differs")
    return validated


def archived_allocator_external_identities(
    contract: ModuleType,
    rows: list[dict[str, Any]],
    maximum: int,
) -> list[tuple[str, dict[str, Any]]]:
    identities: list[tuple[str, dict[str, Any]]] = []
    for row in rows:
        if row["bytes"] > maximum:
            raise contract.ChildError("allocator V5 archived external bytes exceed cap")
        name = row.get("name", PurePosixPath(row["path"]).name)
        identities.append((name, row))
    return identities


def archived_allocator_manifest(
    contract: ModuleType,
    prefix: bytes,
    files: list[tuple[str, dict[str, Any]]],
) -> str:
    manifest = bytearray(prefix)
    for name, row in files:
        manifest.extend(name.encode("utf-8"))
        manifest.extend(b"\0")
        manifest.extend(str(row["bytes"]).encode("ascii"))
        manifest.extend(b"\0")
        manifest.extend(row["sha256"].encode("ascii"))
        manifest.extend(b"\n")
    return contract.sha256(bytes(manifest))


def archived_allocator_physical_aggregates(
    contract: ModuleType,
    checkpoint: dict[str, Any],
    files: list[tuple[str, dict[str, Any]]],
) -> list[str]:
    v5 = contract.ALLOCATOR_V5
    names = [name for name, _ in files]
    if not 1 <= len(files) <= v5.MAX_PHYSICAL_FILES or names != sorted(names):
        raise contract.ChildError("allocator V5 archived physical inventory differs")
    by_name = dict(files)
    if len(by_name) != len(files):
        raise contract.ChildError("allocator V5 archived physical names alias")
    consumed: set[str] = set()
    seen_rows: set[tuple[int, int, int]] = set()
    result: list[str] = []

    def consume(prefix: str, suffix: str) -> str:
        matches = [name for name in by_name if name.startswith(prefix) and name.endswith(suffix)]
        if len(matches) != 1 or matches[0] in consumed:
            raise contract.ChildError(
                f"allocator V5 archived physical attachment identity differs: {prefix}"
            )
        name = matches[0]
        digest = by_name[name]["sha256"]
        if name != f"{prefix}{digest}{suffix}":
            raise contract.ChildError(
                f"allocator V5 archived physical filename digest differs: {name}"
            )
        consumed.add(name)
        return digest

    for observation, expected in zip(
        checkpoint["observations"], checkpoint["aggregates"], strict=True
    ):
        physical: list[str] = []
        if observation["tag"] == "interval":
            logical = observation["cell"]
            row = (
                logical["candidateIndex"],
                logical["population"],
                logical["latency"],
            )
            first = row not in seen_rows
            seen_rows.add(row)
            if logical["candidate"].startswith("RANGE_") and first:
                physical.append(
                    consume(
                        f"scale-{logical['candidate']}-{logical['population']}-{logical['latency']}-",
                        ".json",
                    )
                )
            physical.append(consume(f"interval-{logical['contextId']}-", ".json"))
        else:
            for cut in v5.FAULT_CUTS:
                physical.append(
                    consume(
                        f"fault-{observation['candidate']}-{observation['population']}-"
                        f"{observation['latency']}-{cut}-",
                        ".nare1",
                    )
                )
        aggregate = contract.sha256(b"".join(bytes.fromhex(value) for value in physical))
        if aggregate != expected or aggregate in result:
            raise contract.ChildError(
                "allocator V5 archived physical aggregate differs or aliases"
            )
        result.append(aggregate)
    if consumed != set(by_name):
        raise contract.ChildError(
            "allocator V5 archived physical inventory contains an unbound file"
        )
    return result


def archived_allocator_junit_manifest(
    contract: ModuleType,
    files: list[tuple[str, dict[str, Any]]],
    expected_tests: set[str],
) -> tuple[str, dict[str, int]]:
    names = [name for name, _ in files]
    if len(files) != 10 or names != sorted(names):
        raise contract.ChildError("allocator V5 archived diagnostic JUnit inventory differs")
    digest = archived_allocator_manifest(
        contract,
        b"NEREUS_V2_M3_ALLOCATOR_DIAGNOSTIC_JUNIT_MANIFEST_V5\n",
        files,
    )
    return digest, {
        "tests": len(expected_tests),
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }


def archived_allocator_raw_manifest(
    contract: ModuleType,
    files: list[tuple[str, dict[str, Any]]],
    tested_commit: str,
) -> str:
    if tested_commit != TESTED or [name for name, _ in files] != sorted(
        contract.ALLOCATOR_V5.DIAGNOSTIC_RAW_NAMES
    ):
        raise contract.ChildError("allocator V5 archived diagnostic raw inventory differs")
    return archived_allocator_manifest(
        contract,
        b"NEREUS_V2_M3_ALLOCATOR_DIAGNOSTIC_RAW_MANIFEST_V5\n",
        files,
    )


def validate_historical_receipt_value(
    contract: ModuleType,
    root: Path,
    value: object,
) -> str:
    """Reuse M3 validation with frozen archive identities in place of workstation files."""

    try:
        require_fixed_final(contract.canonical_bytes(value))
    except (TypeError, ValueError) as error:
        raise DependencyError("cannot canonicalize the exact historical M3 Final") from error

    child_contract = contract._child_contract()
    v5_contract = child_contract.ALLOCATOR_V5
    original_freshness = child_contract._validate_source_freshness
    original_file_rows = child_contract._allocator_v5_file_rows
    original_external_bytes = child_contract._allocator_v5_external_bytes
    original_physical_aggregates = v5_contract.physical_aggregates
    original_junit_manifest = v5_contract.diagnostic_junit_manifest
    original_raw_manifest = v5_contract.diagnostic_raw_manifest

    def historical_source_freshness(_root: Path, tested: str) -> tuple[str, int]:
        if tested != TESTED:
            raise child_contract.ChildError(
                f"historical child tested commit differs: {tested} != {TESTED}"
            )
        return tested, 0

    # The exact Final SHA and its closure-commit blob are the historical trust
    # anchor. Recompute every V5 cross-link from the sealed bytes/SHA inventory,
    # but do not require the original 124.9-MB workstation payload to remain at
    # its machine-specific absolute paths forever.
    child_contract._validate_source_freshness = historical_source_freshness
    child_contract._allocator_v5_file_rows = lambda _root, rows, label, count, size, names=None: (
        archived_allocator_file_rows(child_contract, rows, label, count, size, names)
    )
    child_contract._allocator_v5_external_bytes = lambda _root, rows, maximum: (
        archived_allocator_external_identities(child_contract, rows, maximum)
    )
    v5_contract.physical_aggregates = lambda checkpoint, files: (
        archived_allocator_physical_aggregates(child_contract, checkpoint, files)
    )
    v5_contract.diagnostic_junit_manifest = lambda files, expected_tests: (
        archived_allocator_junit_manifest(child_contract, files, expected_tests)
    )
    v5_contract.diagnostic_raw_manifest = lambda files, tested: (
        archived_allocator_raw_manifest(child_contract, files, tested)
    )
    try:
        return contract.validate_receipt_value(root, value, TESTED)
    finally:
        child_contract._validate_source_freshness = original_freshness
        child_contract._allocator_v5_file_rows = original_file_rows
        child_contract._allocator_v5_external_bytes = original_external_bytes
        v5_contract.physical_aggregates = original_physical_aggregates
        v5_contract.diagnostic_junit_manifest = original_junit_manifest
        v5_contract.diagnostic_raw_manifest = original_raw_manifest


def selected_allocator_candidate(root: Path, receipt: dict[str, object]) -> str:
    children = receipt.get("childReceipts")
    if not isinstance(children, list):
        raise DependencyError("M3 Final has no child receipt inventory")
    allocator_rows = [
        row for row in children if isinstance(row, dict) and row.get("kind") == "ALLOCATOR_SELECTION"
    ]
    if len(allocator_rows) != 1:
        raise DependencyError("M3 Final must contain exactly one ALLOCATOR_SELECTION child")
    attachments = allocator_rows[0].get("attachments")
    if not isinstance(attachments, list):
        raise DependencyError("allocator child has no attachment inventory")
    verification_rows = [
        row
        for row in attachments
        if isinstance(row, dict) and row.get("kind") == "ALLOCATOR_V5_CAMPAIGN_VERIFICATION"
    ]
    if len(verification_rows) != 1 or not isinstance(verification_rows[0].get("path"), str):
        raise DependencyError("allocator child lacks one V5 campaign verification attachment")
    path = PurePosixPath(str(verification_rows[0]["path"]))
    if path.is_absolute() or ".." in path.parts:
        raise DependencyError("allocator verification attachment path is unsafe")
    try:
        verification = json.loads((root / path).read_bytes())
        encoded = verification["promotionDecisionBase64"]
        decision = json.loads(base64.b64decode(encoded, validate=True))
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise DependencyError("cannot decode the bound allocator promotion decision") from error
    if decision.get("status") != "PROMOTABLE" or decision.get("selectedCandidate") != "RANGE_64":
        raise DependencyError("M3 allocator decision is not exact PROMOTABLE/RANGE_64")
    return "RANGE_64"


def require_clean_m3_evidence(root: Path) -> None:
    raw = git(root, "status", "--porcelain=v1", "-z", "--", "docs/v2/evidence/v2-m3")
    if raw:
        paths = [item.decode("utf-8", "replace") for item in raw.split(b"\0") if item]
        raise DependencyError(f"immutable M3 evidence path is dirty: {paths}")


def validate(root: Path, receipt_path: PurePosixPath = FINAL_PATH) -> tuple[str, str]:
    root = root.resolve(strict=True)
    top = Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve(strict=True)
    if top != root:
        raise DependencyError(f"repository root differs from Git top-level: {root} != {top}")
    require_fixed_path(receipt_path)
    require_clean_m3_evidence(root)

    try:
        raw = (root / receipt_path).read_bytes()
    except OSError as error:
        raise DependencyError(f"cannot read exact M3 Final: {receipt_path}") from error
    require_fixed_final(raw)

    closure_raw = git(root, "show", f"{CLOSURE}:{receipt_path}")
    if closure_raw != raw:
        raise DependencyError("current exact M3 Final differs from the closure/navigation ancestor")

    contract = load_m3_contract(root)
    try:
        value = contract.load_canonical_json(raw, str(receipt_path))
        tested = validate_historical_receipt_value(contract, root, value)
        contract.validate_scenario_sync(root, receipt_path)
    except contract.FinalError as error:
        raise DependencyError(f"canonical M3 validation failed: {error}") from error
    if tested != TESTED:
        raise DependencyError(f"M3 tested source differs: {tested} != {TESTED}")
    if value.get("sourceTuple", {}).get("sourceLocksSha256") != SOURCE_LOCK_SHA256:
        raise DependencyError("M3 Final-bound source-lock SHA differs")
    if value.get("allocatorSelection", {}).get("mode") != "RANGE":
        raise DependencyError("M3 Final allocator mode is not RANGE")
    if value.get("exclusions") != EXPECTED_EXCLUSIONS:
        raise DependencyError("M3 Final exclusions differ")
    if len(value.get("childReceipts", [])) != 11 or len(value.get("scenarios", [])) != 26:
        raise DependencyError("M3 Final 11-child/26-scenario closure differs")
    selected = selected_allocator_candidate(root, value)

    head = git(root, "rev-parse", "HEAD").decode().strip()
    require_ancestor(root, TESTED, CLOSURE)
    require_ancestor(root, CLOSURE, head)
    return head, selected


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--receipt", default=str(FINAL_PATH))
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        head, selected = validate(args.repo_root, PurePosixPath(args.receipt))
    except DependencyError as error:
        print(f"M4 historical M3 dependency: {error}", file=sys.stderr)
        return 1
    print(
        "M4 historical M3 dependency verified: "
        f"tested={TESTED} closure={CLOSURE} head={head} "
        f"FinalSha256={FINAL_SHA256} allocator=RANGE/{selected} children=11 scenarios=26"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
