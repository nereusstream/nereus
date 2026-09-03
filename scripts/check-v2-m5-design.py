#!/usr/bin/env python3
"""Validate the governance-only M5 design hard-freeze without claiming runtime evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
from typing import Iterable


SCHEMA = "NEREUS_V2_M5_DESIGN_FREEZE_V1"
RESULT = "DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED"
MANIFEST_PATH = PurePosixPath("docs/v2/detailed_design/m5/m5-design-freeze.json")
INDEX_PATH = PurePosixPath("docs/v2/detailed_design/m5/README.md")
I0_PATH = PurePosixPath("docs/v2/detailed_design/m5/m5-i0-implementation-input-closure.md")
A_PATH = PurePosixPath("docs/v2/detailed_design/m5/m5-a-materialization-and-manifest-publication.md")
B_PATH = PurePosixPath("docs/v2/detailed_design/m5/m5-b-kafka-compaction-and-index-rebuild.md")
C_PATH = PurePosixPath("docs/v2/detailed_design/m5/m5-c-retention-reference-free-and-metadata-retirement.md")
D_PATH = PurePosixPath("docs/v2/detailed_design/m5/m5-d-physical-delete-orphan-and-gc.md")
E_PATH = PurePosixPath("docs/v2/detailed_design/m5/m5-e-evidence-ownership-and-freeze.md")
OPEN_PATH = PurePosixPath("docs/v2/open-questions.md")
SCENARIO_PATH = PurePosixPath("docs/v2/v2-scenarios.json")
PLAN_PATH = PurePosixPath("docs/v2/08-implementation-plan-and-gates.md")
V2_INDEX_PATH = PurePosixPath("docs/v2/README.md")
MATRIX_PATH = PurePosixPath("docs/v2/09-scenario-evidence-matrix.md")
BUILD_PATH = PurePosixPath("build.gradle.kts")
SETTINGS_PATH = PurePosixPath("settings.gradle.kts")
M4_FINAL_PATH = PurePosixPath(
    "docs/v2/evidence/v2-m4/final/"
    "final-source-595c8b34779d1e88187eb0084bf18e65ab2dd742/m4-final.json"
)
M4_FINAL_SHA256 = "31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07"
M4_TESTED_COMMIT = "595c8b34779d1e88187eb0084bf18e65ab2dd742"
M4_SOURCE_LOCKS_SHA256 = "02601b3de76857d5f0c8657b285bc91584486d08077e201a4d9fd34f377b07a2"
EVIDENCE_PREFIX = PurePosixPath("docs/v2/evidence/v2-m5")

DESIGN_DOCUMENTS = (INDEX_PATH, I0_PATH, A_PATH, B_PATH, C_PATH, D_PATH, E_PATH)
BOUND_DOCUMENTS = tuple(sorted(DESIGN_DOCUMENTS[1:], key=str))

M5_MILESTONES = {
    "V2-META-007": "M5",
    "V2-FABRIC-003": "M5",
    "V2-BK-011": "M2/M5",
    "V2-KAF-DATA-011": "M2/M5",
    "V2-KAF-DATA-012": "M2/M4/M5/M6",
    "V2-KAF-DATA-013": "M4/M5/M6",
    "V2-KAF-DATA-022": "M2/M5/M6",
    "V2-READ-002": "M5",
    "V2-READ-006": "M4/M5",
    "V2-READ-008": "M4/M5",
    "V2-READ-009": "M4/M5",
    "V2-READ-010": "M4/M5",
    "V2-READ-011": "M4/M5",
    "V2-READ-012": "M4/M5",
    "V2-READ-013": "M4/M5",
    "V2-READ-014": "M4/M5",
    "V2-READ-015": "M4/M5",
}
M4_PROMOTED = {
    "V2-READ-001": "M4",
    "V2-READ-003": "M3/M4",
    "V2-READ-004": "M4",
    "V2-READ-005": "M4",
    "V2-READ-007": "M4",
}
M4_RECEIPT = str(M4_FINAL_PATH)
ACTIVE_GATES = (
    "V2-OPEN-OBJ-19",
    "V2-OPEN-PUL-OBJ-09",
    "V2-OPEN-OBJ-22",
    "V2-OPEN-OBJ-24",
    "V2-OPEN-READ-15",
)
FUTURE_RUNTIME_TASKS = (
    "v2M5MaterializationCheck",
    "v2M5KafkaCompactionCheck",
    "v2M5RetentionRetirementCheck",
    "v2M5PhysicalGcCheck",
    "v2M5CurrentSourceIsolationCheck",
    "v2M5EvidenceContractTest",
    "v2M5EvidenceExecutionCheck",
    "v2M5FinalSourceCheck",
    "v2M5Check",
)


class DesignError(RuntimeError):
    """Fail-closed M5 design-freeze rejection."""


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def read_bytes(root: Path, path: PurePosixPath) -> bytes:
    try:
        return (root / path).read_bytes()
    except OSError as error:
        raise DesignError(f"cannot read {path}: {error}") from error


def read_text(root: Path, path: PurePosixPath) -> str:
    try:
        return read_bytes(root, path).decode("utf-8")
    except UnicodeDecodeError as error:
        raise DesignError(f"{path} is not UTF-8") from error


def load_json(raw: bytes, label: str) -> object:
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DesignError(f"cannot parse {label}: {error}") from error


def exact_object(value: object, members: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != members:
        actual = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise DesignError(f"{label} members differ: {actual}")
    return value


def safe_relative(value: object, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value:
        raise DesignError(f"{label} is not a non-empty path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or str(path) != value:
        raise DesignError(f"{label} is not safe-relative: {value}")
    return path


def validate_manifest_value(
    root: Path,
    value: object,
    expected_paths: Iterable[PurePosixPath] = BOUND_DOCUMENTS,
) -> None:
    manifest = exact_object(value, {"documents", "result", "schema"}, "M5 freeze manifest")
    if manifest["schema"] != SCHEMA or manifest["result"] != RESULT:
        raise DesignError("M5 freeze manifest schema/result differs")
    documents = manifest["documents"]
    if not isinstance(documents, list):
        raise DesignError("M5 freeze manifest documents is not a list")

    paths: list[PurePosixPath] = []
    for index, raw_row in enumerate(documents):
        row = exact_object(raw_row, {"path", "sha256"}, f"M5 freeze manifest documents[{index}]")
        path = safe_relative(row["path"], f"M5 freeze manifest documents[{index}].path")
        digest = row["sha256"]
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise DesignError(f"M5 freeze manifest SHA is invalid: {path}")
        if sha256(read_bytes(root, path)) != digest:
            raise DesignError(f"M5 freeze manifest SHA differs: {path}")
        paths.append(path)

    expected = tuple(sorted(expected_paths, key=str))
    if tuple(paths) != tuple(sorted(paths, key=str)):
        raise DesignError("M5 freeze manifest paths are not sorted")
    if len(paths) != len(set(paths)):
        raise DesignError("M5 freeze manifest contains duplicate paths")
    if tuple(paths) != expected:
        raise DesignError(
            "M5 freeze manifest path set differs: "
            f"{[str(path) for path in paths]} != {[str(path) for path in expected]}"
        )


def parse_front_matter(text: str, label: str) -> dict[str, str]:
    lines = text.splitlines()
    if len(lines) < 3 or lines[0] != "---":
        raise DesignError(f"{label} has no front matter")
    try:
        end = lines.index("---", 1)
    except ValueError as error:
        raise DesignError(f"{label} front matter is unterminated") from error
    result: dict[str, str] = {}
    for line in lines[1:end]:
        if ": " not in line:
            raise DesignError(f"{label} front matter line is invalid: {line}")
        key, item = line.split(": ", 1)
        if key in result:
            raise DesignError(f"{label} front matter duplicates {key}")
        result[key] = item
    return result


def validate_design_document(text: str, label: str) -> None:
    front = parse_front_matter(text, label)
    expected = {
        "productLine": "V2",
        "designStatus": "Accepted",
        "implementationStatus": "NotStarted",
        "evidenceStatus": "NotRun",
        "sourceTuple": "v2-m1",
    }
    for key, expected_value in expected.items():
        if front.get(key) != expected_value:
            raise DesignError(f"{label} {key} differs: {front.get(key)} != {expected_value}")


def require_literals(text: str, literals: Iterable[str], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise DesignError(f"{label} lacks frozen literals: {missing}")


def validate_index(text: str) -> None:
    require_literals(
        text,
        (
            "m5-i0-implementation-input-closure.md",
            "m5-a-materialization-and-manifest-publication.md",
            "m5-b-kafka-compaction-and-index-rebuild.md",
            "m5-c-retention-reference-free-and-metadata-retirement.md",
            "m5-d-physical-delete-orphan-and-gc.md",
            "m5-e-evidence-ownership-and-freeze.md",
            "m5-design-freeze.json",
            RESULT,
            "All 17 scenario rows whose milestone names",
            "M5 remain `PLANNED` with null receipts.",
            "No blocking design question remains",
            "Hard freeze authorizes only later implementation work.",
        ),
        "M5 index",
    )


def validate_frozen_content(values: dict[PurePosixPath, str]) -> None:
    require_literals(
        values[I0_PATH],
        (
            M4_TESTED_COMMIT,
            M4_FINAL_SHA256,
            "No new Gradle module is added for M5.",
            "SOURCE_RETIREMENT_BATCH_FULL_METADATA_RETIRED",
            "Pulsar incarnation metadata uses a stricter",
            "No blocking design question remains.",
        ),
        "M5-I0",
    )
    require_literals(
        values[A_PATH],
        (
            "The mutable read authority remains M4's `BindingReadSelector`.",
            "REFERENCE_REUSE",
            "INDEX_ONLY_GENERATION",
            "REWRITE_GENERATION",
            "KAFKA_BATCH_PRESERVING_V1",
            "KAFKA_SEMANTIC_COMPACTED_V1",
            "PULSAR_ENTRY_PRESERVING_V1",
            "No blocking design question remains for M5-A.",
        ),
        "M5-A",
    )
    require_literals(
        values[B_PATH],
        (
            "A window-local key map is insufficient.",
            "DROP_EXPIRED_TOMBSTONE",
            "KafkaCompactionProtocolStateRootV1",
            "floor(index, requested)",
            "compaction suppression/gap root",
            "`V2-KAF-DATA-022` remains `PLANNED`",
            "No blocking design question remains for M5-B.",
        ),
        "M5-B",
    )
    require_literals(
        values[C_PATH],
        (
            "ReferenceKindV1",
            "FullSourceRetirementBatchV1",
            "FULL_V1 -> RETIRED_V1",
            "One Oxia conditional transaction",
            "RetiredSourceRetirementBatchTombstoneV1",
            "RetiredTopicIncarnationTombstone",
            "`V2-OPEN-READ-15` remains active",
            "No blocking design question remains for M5-C.",
        ),
        "M5-C",
    )
    require_literals(
        values[D_PATH],
        (
            "VERSION_MATCH_DELETE_V1",
            "DELETE_NONE",
            "DELETE_INTENT",
            "DELETE_DONE",
            "root, data, and multipart residue",
            "bookkeeperDeleted=true",
            "ALLOCATOR_NO_REUSE_EVIDENCE",
            "No blocking design question remains for M5-D.",
        ),
        "M5-D",
    )
    require_literals(
        values[E_PATH],
        (
            "MATERIALIZATION_MANIFEST_PUBLICATION",
            "KAFKA_COMPACTION_INDEX_REBUILD",
            "RETENTION_METADATA_RETIREMENT",
            "PHYSICAL_DELETE_ORPHAN_RECONCILIATION",
            "CURRENT_SOURCE_CELL_ISOLATION",
            "The 14-row promotion set is exact:",
            "`V2-KAF-DATA-012`",
            "`V2-KAF-DATA-013`",
            "`V2-KAF-DATA-022`",
            "v2M5DesignCheck",
            "v2M5HistoricalM4DependencyCheck",
            "It never recertifies current HEAD as M4-tested.",
            "No blocking design question remains.",
        ),
        "M5-E",
    )


def scenario_rows(value: object) -> dict[str, dict[str, object]]:
    if not isinstance(value, dict) or not isinstance(value.get("scenarios"), list):
        raise DesignError("scenario manifest has no scenario rows")
    result: dict[str, dict[str, object]] = {}
    for row in value["scenarios"]:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str):
            raise DesignError("scenario manifest contains an invalid row")
        identifier = row["id"]
        if identifier in result:
            raise DesignError(f"scenario manifest duplicates {identifier}")
        result[identifier] = row
    return result


def validate_scenarios_value(value: object) -> None:
    rows = scenario_rows(value)
    actual_m5 = {identifier for identifier, row in rows.items() if "M5" in str(row.get("milestone", ""))}
    if actual_m5 != set(M5_MILESTONES):
        raise DesignError(f"M5 scenario set differs: {sorted(actual_m5)}")
    for identifier, milestone in M5_MILESTONES.items():
        row = rows[identifier]
        if (
            row.get("milestone") != milestone
            or row.get("status") != "PLANNED"
            or row.get("evidenceReceipt") is not None
        ):
            raise DesignError(f"M5 design freeze requires PLANNED/null exact milestone for {identifier}")
    for identifier, milestone in M4_PROMOTED.items():
        row = rows.get(identifier)
        if (
            row is None
            or row.get("milestone") != milestone
            or row.get("status") != "PASSED_CURRENT_SOURCE"
            or row.get("evidenceReceipt") != M4_RECEIPT
        ):
            raise DesignError(f"M5 design freeze changed existing M4 receipt authority: {identifier}")


def validate_m4_final(root: Path) -> None:
    raw = read_bytes(root, M4_FINAL_PATH)
    if sha256(raw) != M4_FINAL_SHA256:
        raise DesignError("frozen M4 Final SHA differs")
    value = load_json(raw, str(M4_FINAL_PATH))
    if not isinstance(value, dict):
        raise DesignError("frozen M4 Final is not an object")
    source = value.get("sourceTuple")
    if (
        value.get("schema") != "NEREUS_V2_M4_FINAL_V1"
        or value.get("result") != "PASS_V2_M4_FINAL"
        or value.get("promotionEligible") is not True
        or not isinstance(source, dict)
        or source.get("nereusCommit") != M4_TESTED_COMMIT
        or source.get("sourceLocksSha256") != M4_SOURCE_LOCKS_SHA256
    ):
        raise DesignError("frozen M4 Final identity/result differs")


def validate_open_questions(text: str) -> None:
    for gate in ACTIVE_GATES:
        if gate not in text:
            raise DesignError(f"open-question log lacks active gate {gate}")
    require_literals(
        text,
        (
            "### `V2-OPEN-READ-15`: compact batch metadata retirement",
            "Tombstone deletion remains evidence-blocked.",
            "No retired-through/frontier",
            "symbol is an accepted 0.2 contract.",
            "closed design inputs only. No runtime",
            "or evidence exists yet.",
        ),
        "V2-OPEN-READ-15",
    )


def validate_plan_and_index(plan: str, index: str) -> None:
    for text, label in ((plan, "implementation plan"), (index, "V2 index")):
        require_literals(
            text,
            (
                "detailed_design/m5/README.md",
                RESULT,
                "all 17" if label == "implementation plan" else "all 17",
                "physical deletion",
            ),
            label,
        )
    require_literals(
        plan,
        (
            "DESIGN HARD-FROZEN / implementation NotStarted / evidence NotRun",
            "V2-KAF-DATA-012/013/022",
            "future `v2M5Check`",
        ),
        "implementation plan",
    )


def validate_scenario_matrix(text: str) -> None:
    require_literals(
        text,
        (
            "detailed_design/m5/README.md",
            "detailed_design/m5/m5-e-evidence-ownership-and-freeze.md",
            "current 17-row M5 inventory",
            "exact 14-row eventual M5 promotion set",
            "V2-KAF-DATA-012/013/022",
            "All 17 remain `PLANNED` with null receipts",
        ),
        "scenario evidence matrix",
    )


def validate_build(text: str) -> None:
    for required in (
        "v2M5HistoricalM4DependencyContractTest",
        "v2M5HistoricalM4DependencyCheck",
        "v2M5DesignContractTest",
        "v2M5DesignSourceCheck",
        "v2M5DesignCheck",
    ):
        if re.search(rf'tasks\.register(?:<[^>]+>)?\(\s*"{re.escape(required)}"', text) is None:
            raise DesignError(f"build does not register {required}")
    for forbidden in FUTURE_RUNTIME_TASKS:
        if re.search(rf'tasks\.register(?:<[^>]+>)?\(\s*"{re.escape(forbidden)}"', text):
            raise DesignError(f"build registers forbidden pre-implementation task {forbidden}")
    design_start = text.find('tasks.register("v2M5DesignCheck")')
    if design_start < 0:
        raise DesignError("build lacks v2M5DesignCheck body")
    design_body = text[design_start : design_start + 1200]
    require_literals(
        design_body,
        (
            "v2DocumentationCheck",
            "v2M5HistoricalM4DependencyContractTest",
            "v2M5HistoricalM4DependencyCheck",
            "v2M5DesignContractTest",
            "v2M5DesignSourceCheck",
        ),
        "v2M5DesignCheck",
    )


def validate_settings(text: str) -> None:
    if "nereus-materialization" in text:
        raise DesignError("M5 design freeze must not add a materialization module")


def validate_no_m5_evidence(root: Path) -> None:
    path = root / EVIDENCE_PREFIX
    if path.exists() and any(candidate.is_file() for candidate in path.rglob("*")):
        raise DesignError("M5 evidence/receipt path is non-empty before implementation")


def validate_no_m5_runtime(root: Path) -> None:
    for module in root.glob("nereus-*"):
        source = module / "src/main"
        if not source.exists():
            continue
        for candidate in source.rglob("*"):
            lowered = candidate.name.lower()
            if candidate.is_file() and ("m5" in lowered or "materialization" in candidate.parts):
                raise DesignError(f"M5 runtime marker exists before implementation: {candidate.relative_to(root)}")


def validate(root: Path, manifest_path: PurePosixPath = MANIFEST_PATH) -> None:
    root = root.resolve(strict=True)
    if manifest_path != MANIFEST_PATH:
        raise DesignError(f"M5 design gate requires exact manifest path: {MANIFEST_PATH}")
    manifest = load_json(read_bytes(root, manifest_path), str(manifest_path))
    validate_manifest_value(root, manifest)

    values = {path: read_text(root, path) for path in DESIGN_DOCUMENTS}
    for path, value in values.items():
        validate_design_document(value, str(path))
    validate_index(values[INDEX_PATH])
    validate_frozen_content(values)
    validate_m4_final(root)
    validate_scenarios_value(load_json(read_bytes(root, SCENARIO_PATH), str(SCENARIO_PATH)))
    validate_open_questions(read_text(root, OPEN_PATH))
    validate_plan_and_index(read_text(root, PLAN_PATH), read_text(root, V2_INDEX_PATH))
    validate_scenario_matrix(read_text(root, MATRIX_PATH))
    validate_build(read_text(root, BUILD_PATH))
    validate_settings(read_text(root, SETTINGS_PATH))
    validate_no_m5_evidence(root)
    validate_no_m5_runtime(root)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--manifest", default=str(MANIFEST_PATH))
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        validate(args.repo_root, PurePosixPath(args.manifest))
    except DesignError as error:
        print(f"M5 design freeze: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
