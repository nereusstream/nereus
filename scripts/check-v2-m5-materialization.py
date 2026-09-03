#!/usr/bin/env python3
"""Validate the non-promotable M5-A implementation boundary and immutable design dependency."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_MATERIALIZATION_IMPLEMENTATION_NON_PROMOTABLE"
DESIGN_COMMIT = "c86fde3ed6f4319642987fd599022bd32e2cca5e"
REQUIRED_SOURCES = (
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5BytePreservingMaterializerV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5LookupIndexV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationAdmissionV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationCodecV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationCoordinatorV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationKeysV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationObjectSessionV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationPlannerV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationRecordsV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationValidatorV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/Nms1CodecV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/Nms1ObjectV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/materialization/M5MaterializationV1Test.java",
)
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-a-wire-projection.json"


class MaterializationError(RuntimeError):
    """Fail-closed M5-A implementation rejection."""


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise MaterializationError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_design_ancestor(root: Path) -> None:
    result = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", DESIGN_COMMIT, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        raise MaterializationError("accepted M5 design commit is not an ancestor of HEAD")


def validate_required_sources(root: Path, paths: tuple[str, ...] = REQUIRED_SOURCES) -> None:
    missing = [path for path in paths if not (root / path).is_file()]
    if missing:
        raise MaterializationError(f"M5-A required sources are missing: {missing}")
    for path in paths:
        raw = (root / path).read_bytes()
        if not raw or hashlib.sha256(raw).digest() == bytes(32):
            raise MaterializationError(f"M5-A required source is empty: {path}")


def require_literals(text: str, values: tuple[str, ...], label: str) -> None:
    missing = [value for value in values if value not in text]
    if missing:
        raise MaterializationError(f"{label} lacks required implementation contract: {missing}")


def validate_runtime_contract(root: Path) -> None:
    def source(name: str) -> str:
        path = next(path for path in REQUIRED_SOURCES if path.endswith("/" + name))
        return (root / path).read_text(encoding="utf-8")

    materializer = source("M5BytePreservingMaterializerV1.java")
    records = source("M5MaterializationRecordsV1.java")
    planner = source("M5MaterializationPlannerV1.java")
    validator = source("M5MaterializationValidatorV1.java")
    coordinator = source("M5MaterializationCoordinatorV1.java")
    nms1 = source("Nms1CodecV1.java")
    admission = source("M5MaterializationAdmissionV1.java")
    tests = (root / REQUIRED_SOURCES[-1]).read_text(encoding="utf-8")
    require_literals(
        records,
        (
            "REFERENCE_REUSE",
            "INDEX_ONLY_GENERATION",
            "REWRITE_GENERATION",
            "PREFERRED_WITH_FALLBACK",
            "CANCELLED_STALE",
            "QUARANTINED",
        ),
        "M5-A records",
    )
    require_literals(planner, ("payloadReusable", "requireSharedObjectIsolation", "M5-B"), "M5-A planner")
    require_literals(
        materializer,
        ("M5LookupIndexV1", "Nms1ObjectV1", "REFERENCE_REUSE", "source length or digest differs"),
        "M5-A materializer",
    )
    require_literals(
        validator,
        (
            "readAndValidateSources",
            "readAndValidatePayloads",
            "readAndValidateIndexes",
            "requireBytePreservingSemantics",
            "requireLookupBoundaries",
        ),
        "M5-A validator",
    )
    require_literals(
        coordinator,
        (
            "introduceFallback",
            "updateMembershipNeutralView",
            "reconcileTaskTransition",
            "OUTCOME_UNKNOWN",
        ),
        "M5-A coordinator",
    )
    require_literals(nms1, ("FOOTER_MAGIC", "MAX_CANONICAL_BODY_BYTES", "verifyFooter"), "NMS1 codec")
    require_literals(admission, ("REJECTED_CAP", "maximumResponseUnknowns", "reservations"), "M5-A admission")
    require_literals(
        tests,
        (
            "deterministicPlannerSelectsReuseIndexOnlyAndRewriteWithoutCopyingHealthyObjectWal",
            "fullValidationAndSelectorPublicationAreExactIdempotentAndResponseLossSafe",
            "validationRejectsStaleAuthorityCorruptPayloadIndexAndWrongFallbackMembership",
            "persistedCellAdmissionReservesBeforeDispatchAndFailsClosedAtEveryCap",
            "bytePreservingMaterializerBuildsExactRewriteIndexAndReuseOutputs",
        ),
        "M5-A tests",
    )


def validate_projection(root: Path) -> None:
    value = json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8"))
    if value.get("schema") != "NEREUS_V2_M5_A_WIRE_PROJECTION_V1":
        raise MaterializationError("M5-A wire projection schema differs")
    exact = {
        ("control", "maximumBytes"): 2_097_152,
        ("nms1", "footerBytes"): 176,
        ("nms1", "maximumCanonicalBodyBytes"): 402_653_184,
        ("lookupIndex", "maximumRows"): 1_048_576,
        ("lookupIndex", "maximumBytes"): 67_108_864,
        ("hardCounts", "maximumPersistedReservations"): 1_024,
    }
    for path, expected in exact.items():
        actual = value
        for member in path:
            if not isinstance(actual, dict) or member not in actual:
                raise MaterializationError(f"M5-A wire projection lacks {'.'.join(path)}")
            actual = actual[member]
        if actual != expected:
            raise MaterializationError(
                f"M5-A wire projection {'.'.join(path)} differs: {actual} != {expected}"
            )
    if value.get("representationModes") != {
        "REFERENCE_REUSE": 0,
        "INDEX_ONLY_GENERATION": 1,
        "REWRITE_GENERATION": 2,
    }:
        raise MaterializationError("M5-A representation mode codes differ")
    if value.get("lookupIndex", {}).get("lookupRule") != "FLOOR_COVERAGE_THEN_SUCCESSOR":
        raise MaterializationError("M5-A lookup rule differs")


def validate_tasks(root: Path) -> None:
    root_build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    module_build = (root / "nereus-storage-object/build.gradle.kts").read_text(encoding="utf-8")
    if re.search(r'tasks\.register\("v2M5MaterializationCheck"\)', root_build) is None:
        raise MaterializationError("root build lacks v2M5MaterializationCheck")
    if re.search(r'tasks\.register<Test>\("v2M5MaterializationTest"\)', module_build) is None:
        raise MaterializationError("storage-object build lacks v2M5MaterializationTest")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    design = load(root / "scripts/check-v2-m5-design.py", "nereus_m5_design_for_materialization")
    manifest = design.load_json(design.read_bytes(root, design.MANIFEST_PATH), str(design.MANIFEST_PATH))
    design.validate_manifest_value(root, manifest)
    values = {path: design.read_text(root, path) for path in design.DESIGN_DOCUMENTS}
    for path, value in values.items():
        design.validate_design_document(value, str(path))
    design.validate_index(values[design.INDEX_PATH])
    design.validate_frozen_content(values)
    design.validate_m4_final(root)
    design.validate_scenarios_value(json.loads((root / design.SCENARIO_PATH).read_text(encoding="utf-8")))
    design.validate_no_m5_evidence(root)
    require_design_ancestor(root)
    validate_required_sources(root)
    validate_runtime_contract(root)
    validate_projection(root)
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except (MaterializationError, Exception) as error:
        print(f"M5-A implementation: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
