#!/usr/bin/env python3
"""Validate the governance-only M4 design hard-freeze without claiming runtime evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
from typing import Iterable


SCHEMA = "NEREUS_V2_M4_DESIGN_FREEZE_V1"
RESULT = "DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED"
MANIFEST_PATH = PurePosixPath("docs/v2/detailed_design/m4/m4-design-freeze.json")
INDEX_PATH = PurePosixPath("docs/v2/detailed_design/m4/README.md")
M4D_PATH = PurePosixPath("docs/v2/detailed_design/m4/m4-d-evidence-ownership-and-freeze.md")
OPEN_PATH = PurePosixPath("docs/v2/open-questions.md")
SCENARIO_PATH = PurePosixPath("docs/v2/v2-scenarios.json")
BUILD_PATH = PurePosixPath("build.gradle.kts")
EVIDENCE_PREFIX = PurePosixPath("docs/v2/evidence/v2-m4")

DESIGN_DOCUMENTS = (
    PurePosixPath("docs/v2/detailed_design/m4/README.md"),
    PurePosixPath("docs/v2/detailed_design/m4/m4-a-read-view-authority.md"),
    PurePosixPath("docs/v2/detailed_design/m4/m4-b-source-plan-and-fallback.md"),
    PurePosixPath("docs/v2/detailed_design/m4/m4-c-hazard-slot-reclamation.md"),
    M4D_PATH,
)
REVIEW_RECORDS = (
    PurePosixPath("docs/v2/grill-notes/32-m4-read-snapshot-authority.md"),
    PurePosixPath("docs/v2/grill-notes/33-m4-read-path-fallback-matrix.md"),
    PurePosixPath("docs/v2/grill-notes/34-m4-hazard-slot-reclamation-races.md"),
    PurePosixPath("docs/v2/grill-notes/35-m4-final-design-freeze.md"),
)
BOUND_DOCUMENTS = tuple(sorted((*DESIGN_DOCUMENTS[1:], *REVIEW_RECORDS), key=str))

READ_MILESTONES = {
    "V2-READ-001": "M4",
    "V2-READ-002": "M5",
    "V2-READ-003": "M3/M4",
    "V2-READ-004": "M4",
    "V2-READ-005": "M4",
    "V2-READ-006": "M4/M5",
    "V2-READ-007": "M4",
    "V2-READ-008": "M4/M5",
    "V2-READ-009": "M4/M5",
    "V2-READ-010": "M4/M5",
    "V2-READ-011": "M4/M5",
    "V2-READ-012": "M4/M5",
    "V2-READ-013": "M4/M5",
    "V2-READ-014": "M4/M5",
    "V2-READ-015": "M4/M5",
}
REFERENCE_BK_RECEIPTS = {
    "V2-BK-008": "docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json",
    "V2-BK-010": "docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json",
    "V2-BK-014": "docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json",
    "V2-BK-016": "docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json",
}
BEGIN_REVIEW = "<!-- BEGIN FIXED REVIEWER RESPONSE -->"
END_REVIEW = "<!-- END FIXED REVIEWER RESPONSE -->"


class DesignError(RuntimeError):
    """Fail-closed M4 design-freeze rejection."""


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
    manifest = exact_object(value, {"documents", "result", "schema"}, "M4 freeze manifest")
    if manifest["schema"] != SCHEMA or manifest["result"] != RESULT:
        raise DesignError("M4 freeze manifest schema/result differs")
    documents = manifest["documents"]
    if not isinstance(documents, list):
        raise DesignError("M4 freeze manifest documents is not a list")

    paths: list[PurePosixPath] = []
    for index, raw_row in enumerate(documents):
        row = exact_object(raw_row, {"path", "sha256"}, f"M4 freeze manifest documents[{index}]")
        path = safe_relative(row["path"], f"M4 freeze manifest documents[{index}].path")
        digest = row["sha256"]
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise DesignError(f"M4 freeze manifest SHA is invalid: {path}")
        if sha256(read_bytes(root, path)) != digest:
            raise DesignError(f"M4 freeze manifest SHA differs: {path}")
        paths.append(path)

    expected = tuple(sorted(expected_paths, key=str))
    if tuple(paths) != tuple(sorted(paths, key=str)):
        raise DesignError("M4 freeze manifest paths are not sorted")
    if len(paths) != len(set(paths)):
        raise DesignError("M4 freeze manifest contains duplicate paths")
    if tuple(paths) != expected:
        raise DesignError(
            "M4 freeze manifest path set differs: "
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
        key, value = line.split(": ", 1)
        if key in result:
            raise DesignError(f"{label} front matter duplicates {key}")
        result[key] = value
    return result


def validate_design_document(text: str, label: str) -> None:
    front = parse_front_matter(text, label)
    expected = {
        "designStatus": "Accepted",
        "implementationStatus": "NotStarted",
        "evidenceStatus": "NotRun",
        "productLine": "V2",
        "sourceTuple": "v2-m1",
    }
    for key, expected_value in expected.items():
        if front.get(key) != expected_value:
            raise DesignError(f"{label} {key} differs: {front.get(key)} != {expected_value}")


def validate_review_record(text: str, label: str) -> None:
    if text.count(BEGIN_REVIEW) != 1 or text.count(END_REVIEW) != 1:
        raise DesignError(f"{label} does not preserve exactly one fixed reviewer response")
    if text.index(BEGIN_REVIEW) >= text.index(END_REVIEW):
        raise DesignError(f"{label} fixed reviewer response markers are reversed")
    if not text[text.index(BEGIN_REVIEW) + len(BEGIN_REVIEW) : text.index(END_REVIEW)].strip():
        raise DesignError(f"{label} fixed reviewer response is empty")


def require_literals(text: str, literals: Iterable[str], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise DesignError(f"{label} lacks frozen literals: {missing}")


def validate_index(text: str) -> None:
    if "PENDING" in text or "NOT REACHED" in text:
        raise DesignError("M4 index retains a pending design node")
    require_literals(
        text,
        (
            "m4-a-read-view-authority.md",
            "m4-b-source-plan-and-fallback.md",
            "m4-c-hazard-slot-reclamation.md",
            "m4-d-evidence-ownership-and-freeze.md",
            "m4-design-freeze.json",
            RESULT,
            "Implementation, runtime evidence, scenario promotion, and M4 Final have not started.",
        ),
        "M4 index",
    )


def validate_m4d(text: str) -> None:
    require_literals(
        text,
        (
            "M4 owns the complete read-quiescence authority through the irreversible exact protection-generation release CAS.",
            "starts only from exact `RELEASED` state",
            "READ_VIEW_HAZARD",
            "SOURCE_PLAN_EXECUTION",
            "QUIESCENCE_PROTECTION_RELEASE",
            "CURRENT_SOURCE_INTEGRATION_PERFORMANCE",
            "V2-OPEN-READ-08",
            "V2-OPEN-READ-09",
            "V2-OPEN-READ-15",
            "v2M4HistoricalM3DependencyCheck",
            RESULT,
            "No blocking design question remains.",
        ),
        "M4-D",
    )


def scenario_rows(value: object) -> dict[str, dict[str, object]]:
    if not isinstance(value, dict) or not isinstance(value.get("scenarios"), list):
        raise DesignError("scenario manifest has no scenario rows")
    rows = value["scenarios"]
    by_id: dict[str, dict[str, object]] = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str):
            raise DesignError("scenario manifest contains an invalid row")
        identifier = row["id"]
        if identifier in by_id:
            raise DesignError(f"scenario manifest duplicates {identifier}")
        by_id[identifier] = row
    return by_id


def validate_scenarios_value(value: object) -> None:
    rows = scenario_rows(value)
    for identifier, milestone in READ_MILESTONES.items():
        row = rows.get(identifier)
        if row is None:
            raise DesignError(f"scenario manifest lacks {identifier}")
        if (
            row.get("milestone") != milestone
            or row.get("status") != "PLANNED"
            or row.get("evidenceReceipt") is not None
        ):
            raise DesignError(f"M4 design freeze requires PLANNED/null exact milestone for {identifier}")
    for identifier, receipt in REFERENCE_BK_RECEIPTS.items():
        row = rows.get(identifier)
        if row is None or row.get("status") != "PASSED_CURRENT_SOURCE" or row.get("evidenceReceipt") != receipt:
            raise DesignError(f"M4 design freeze changed existing M2 receipt authority: {identifier}")


def markdown_section(text: str, heading: str) -> str:
    start = text.find(heading)
    if start < 0:
        raise DesignError(f"open-question log lacks {heading}")
    end = text.find("\n### `", start + len(heading))
    return text[start:] if end < 0 else text[start:end]


def validate_open_questions(text: str) -> None:
    section08 = markdown_section(text, "### `V2-OPEN-READ-08`")
    section09 = markdown_section(text, "### `V2-OPEN-READ-09`")
    section15 = markdown_section(text, "### `V2-OPEN-READ-15`")
    require_literals(
        section08,
        ("evidence-blocked", "does not block M4 implementation candidates or design freeze", "blocks physical"),
        "V2-OPEN-READ-08",
    )
    require_literals(
        section09,
        (
            "evidence-blocked",
            "does not block fail-closed",
            "M4 Grill 35 moved exact protection-generation release execution into M4",
            "M5 begins from exact `RELEASED`",
        ),
        "V2-OPEN-READ-09",
    )
    require_literals(
        section15,
        ("Tombstone deletion remains evidence-blocked", "tombstone remains permanent"),
        "V2-OPEN-READ-15",
    )


def validate_build(text: str) -> None:
    if re.search(r'tasks\.register(?:<[^>]+>)?\(\s*"v2M4Check"', text) and not all(
        literal in text for literal in ("v2M4FinalSourceCheck", "v2M4EvidenceContractTest")
    ):
        raise DesignError("build registers forbidden placeholder v2M4Check")
    if re.search(r'tasks\.register(?:<[^>]+>)?\(\s*"v2M4DesignCheck"', text) is None:
        raise DesignError("build does not register v2M4DesignCheck")


def validate_no_m4_evidence(root: Path) -> None:
    path = root / EVIDENCE_PREFIX
    if path.exists() and any(candidate.is_file() for candidate in path.rglob("*")):
        raise DesignError("M4 evidence/receipt path is non-empty before implementation")


def validate(root: Path, manifest_path: PurePosixPath = MANIFEST_PATH) -> None:
    root = root.resolve(strict=True)
    if manifest_path != MANIFEST_PATH:
        raise DesignError(f"M4 design gate requires exact manifest path: {MANIFEST_PATH}")
    manifest = load_json(read_bytes(root, manifest_path), str(manifest_path))
    validate_manifest_value(root, manifest)

    for path in DESIGN_DOCUMENTS:
        validate_design_document(read_text(root, path), str(path))
    for path in REVIEW_RECORDS:
        validate_review_record(read_text(root, path), str(path))

    validate_index(read_text(root, INDEX_PATH))
    validate_m4d(read_text(root, M4D_PATH))
    validate_scenarios_value(load_json(read_bytes(root, SCENARIO_PATH), str(SCENARIO_PATH)))
    validate_open_questions(read_text(root, OPEN_PATH))
    validate_build(read_text(root, BUILD_PATH))
    validate_no_m4_evidence(root)


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
        print(f"M4 design freeze: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
