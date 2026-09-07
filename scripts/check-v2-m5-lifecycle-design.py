#!/usr/bin/env python3
"""Validate M5 amendment 3 governance; never substitute for runtime evidence."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parent.parent
BASELINE = "f05037bb16017f24e8147e99a61f26e539ed85fa"
DIRECTORY = "docs/v2/detailed_design/m5/"
MANIFEST = DIRECTORY + "m5-design-amendment-3.json"
PREDECESSOR = DIRECTORY + "m5-design-amendment-2.json"
DOCUMENTS = (
    "docs/decisions/0148-v2-m5-lifecycle-contract-amendment.md",
    DIRECTORY + "m5-lifecycle-contract-amendment.md",
    DIRECTORY + "m5-lifecycle-writer-matrix.md",
)
ACCEPTANCE = DIRECTORY + "m5-lifecycle-acceptance.json"
CHILDREN = (
    "MATERIALIZATION_MANIFEST_PUBLICATION",
    "KAFKA_COMPACTION_INDEX_REBUILD",
    "RETENTION_METADATA_RETIREMENT",
    "PHYSICAL_DELETE_ORPHAN_RECONCILIATION",
    "CURRENT_SOURCE_CELL_ISOLATION",
)
OWNERS = {
    "RESOURCE_UNIQUENESS": CHILDREN[3],
    "REPLACEMENT_RETAINED_MESSAGES": CHILDREN[3],
    "THREE_RECLAMATION_REASONS": CHILDREN[2],
    "BK_ONLY_COMPACTION": CHILDREN[1],
    "BK_OUTPUT_FAILURE": CHILDREN[0],
    "RETIREMENT_HISTORY_1024": CHILDREN[2],
    "RETIREMENT_HISTORY_RECOVERY": CHILDREN[2],
    "DONE_CAPACITY": CHILDREN[3],
    "READ_FENCED_TAKEOVER": CHILDREN[3],
    "WRITER_RACES": CHILDREN[3],
    "PULSAR_NATIVE_LIFECYCLE": CHILDREN[3],
    "SLOW_UNKNOWN_PUT": CHILDREN[4],
    "RUN_RECOVERY_VS_DELETE": CHILDREN[2],
    "QUARANTINE_EXIT": CHILDREN[4],
    "KAFKA_STORAGE_REPLAY_LOAD": CHILDREN[4],
    "CURRENT_SOURCE_REGRESSIONS": CHILDREN[4],
    "SOURCE_CHILD_ARCHIVE_AGGREGATE": "AGGREGATE",
}
DEFERRED = {"V2-KAF-DATA-012", "V2-KAF-DATA-013", "V2-KAF-DATA-022"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def read_json(root: Path, path: str):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads((root / path).read_text(), object_pairs_hook=unique)


def validate_manifest(root: Path, value: dict) -> None:
    expected = {
        "schema": "NEREUS_V2_M5_DESIGN_AMENDMENT_3_V1",
        "amendmentId": "M5-AMENDMENT-3-LIFECYCLE-V2",
        "result": "ACCEPTED_DESIGN_IMPLEMENTATION_REQUIRED",
        "authorizedDate": "2026-09-07",
        "reviewBaselineCommit": BASELINE,
        "acceptanceMatrix": ACCEPTANCE,
        "physicalDeleteAuthority": False,
        "scenarioPromotionAuthority": False,
        "productionAuthority": False,
    }
    require(set(value) == set(expected) | {"reviewSha256", "predecessorAmendment", "documents"},
            "manifest members differ")
    for key, expected_value in expected.items():
        require(type(value[key]) is type(expected_value) and value[key] == expected_value,
                f"manifest field differs: {key}")
    review_digest = value["reviewSha256"]
    require(isinstance(review_digest, str) and len(review_digest) == 64
            and all(c in "0123456789abcdef" for c in review_digest), "review SHA invalid")
    bound = [value["predecessorAmendment"], *value["documents"]]
    require(len(bound) == 1 + len(DOCUMENTS), "document inventory differs")
    for row, expected_path in zip(bound, (PREDECESSOR, *DOCUMENTS)):
        require(set(row) == {"path", "sha256"} and row["path"] == expected_path,
                "bound path/order differs")
        candidate = root / expected_path
        require(candidate.is_file() and not candidate.is_symlink(), "bound document is not a regular file")
        require(digest(candidate.read_bytes()) == row["sha256"], f"bound document SHA differs: {expected_path}")


def validate_acceptance(value: dict) -> None:
    require(set(value) == {"schema", "authority", "obligations", "deferred"}, "acceptance members differ")
    require(value["schema"] == "NEREUS_V2_M5_LIFECYCLE_ACCEPTANCE_V2"
            and value["authority"] == "REQUIRED_COVERAGE_NOT_EXECUTION_EVIDENCE", "acceptance authority differs")
    rows = value["obligations"]
    require(len(rows) == len(OWNERS) and {row["id"] for row in rows} == set(OWNERS),
            "obligation inventory missing or duplicate")
    for row in rows:
        require(set(row) == {"id", "reviewSection", "owner", "requiredDependencies", "acceptance", "status", "receipt"},
                "obligation members differ")
        require(row["owner"] == OWNERS[row["id"]], "exclusive owner differs")
        require(row["status"] == "OPEN" and row["receipt"] is None, "design cannot promote execution")
        require(isinstance(row["acceptance"], str) and len(row["acceptance"]) >= 40, "acceptance predicate missing")
        dependencies = row["requiredDependencies"]
        require(isinstance(dependencies, list) and len(dependencies) == len(set(dependencies))
                and set(dependencies) <= {"OXIA", "BOOKKEEPER", "OBJECT", "PULSAR"}, "dependency inventory invalid")
        if row["owner"] != "AGGREGATE":
            require(bool(dependencies), "real boundary missing")
        if row["id"] == "BK_ONLY_COMPACTION":
            require(set(dependencies) == {"BOOKKEEPER", "OXIA"}, "BK_ONLY depends on Object or lacks real BK/Oxia")
        if row["id"] in {"RESOURCE_UNIQUENESS", "WRITER_RACES", "READ_FENCED_TAKEOVER"}:
            require(set(dependencies) == {"OXIA", "BOOKKEEPER", "OBJECT"}, "real composition boundary missing")
    deferred = value["deferred"]
    require(len(deferred) == 3 and {row["scenario"] for row in deferred} == DEFERRED, "M6 inventory differs")
    for row in deferred:
        require(set(row) == {"scenario", "milestone", "status", "interface", "acceptance", "receipt"},
                "deferred members differ")
        require(row["milestone"] == "M6" and row["status"] == "PLANNED" and row["receipt"] is None,
                "M6 promotion forbidden")
        require(len(row["interface"]) >= 40 and len(row["acceptance"]) >= 40, "M6 handoff missing")


def validate(root: Path) -> None:
    require(subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", BASELINE, "HEAD"],
                           capture_output=True).returncode == 0, "review baseline is not an ancestor")
    original = subprocess.check_output(["git", "-C", str(root), "show", f"{BASELINE}:{PREDECESSOR}"])
    require((root / PREDECESSOR).read_bytes() == original, "historical predecessor mutated")
    spec = importlib.util.spec_from_file_location("m5_amendment_2", root / "scripts/check-v2-m5-design-amendment-2.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.validate(root)
    validate_manifest(root, read_json(root, MANIFEST))
    validate_acceptance(read_json(root, ACCEPTANCE))
    current = (root / (DIRECTORY + "m5-current-contracts.md")).read_text()
    for name in ("ADR 0148", "m5-lifecycle-contract-amendment.md", "m5-design-amendment-3.json",
                 "m5-lifecycle-acceptance.json", "M6 PLANNED"):
        require(name in current, f"current index missing: {name}")
    for path in ("docs/v2/README.md", "docs/v2/08-implementation-plan-and-gates.md",
                 "docs/v2/open-questions.md", DIRECTORY + "README.md", DIRECTORY + "m5-implementation-log.md"):
        require("m5-current-contracts.md" in (root / path).read_text(), f"entry lacks current view: {path}")


if __name__ == "__main__":
    try:
        validate(ROOT)
    except Exception as error:
        print(f"M5 lifecycle design: {error}", file=sys.stderr)
        raise SystemExit(1)
    print("PASS_V2_M5_LIFECYCLE_DESIGN_NO_RUNTIME_AUTHORITY")
