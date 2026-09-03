#!/usr/bin/env python3
"""Validate the focused non-promotable M5-D orphan and per-Cell admission core."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_ORPHAN_ADMISSION_CORE_NON_PROMOTABLE"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-d-orphan-admission-projection.json"
IMPLEMENTATION_ANCESTOR = "21110c8d340f4020039fbbd57b154bd359ee441e"
ORPHAN_CLASSES = [
    "PHYSICAL_OUTPUT_ORPHAN_CANDIDATE",
    "MULTIPART_RESIDUE_CANDIDATE",
    "RELEASED_SOURCE_CANDIDATE",
    "PERMANENT_METADATA_FENCE",
    "ALLOCATOR_NO_REUSE_EVIDENCE",
    "UNKNOWN_OR_FOREIGN",
]
MARK_CLASSES = ORPHAN_CLASSES[:3]
PERMANENT_CLASSES = ORPHAN_CLASSES[3:5]
REQUIRED_SOURCES = (
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc/M5PhysicalOrphanProtocolV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc/M5PhysicalGcCellAdmissionV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/gc/M5PhysicalOrphanProtocolV1Test.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/gc/M5PhysicalGcCellAdmissionV1Test.java",
)


class OrphanAdmissionError(RuntimeError):
    """Stable fail-closed M5-D orphan/admission rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise OrphanAdmissionError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise OrphanAdmissionError(f"{label} lacks closed M5 contract: {missing}")


def validate_projection_value(value: object) -> None:
    if not isinstance(value, dict) or set(value) != {
        "schema",
        "status",
        "implementationAncestor",
        "closedOrphanClasses",
        "classesAllowedToMark",
        "permanentRetainClasses",
        "quarantineOnlyClasses",
        "markProtocol",
        "perCellAdmission",
        "focusedTests",
        "externalDeleteApiPresent",
        "intentMutationApiPresent",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    }:
        raise OrphanAdmissionError("M5-D orphan/admission projection members differ")
    if value.get("schema") != "NEREUS_V2_M5_D_ORPHAN_ADMISSION_PROJECTION_V1" or value.get(
        "status"
    ) != "ORPHAN_AND_CELL_ADMISSION_CORE_IMPLEMENTED_NON_PROMOTABLE":
        raise OrphanAdmissionError("M5-D orphan/admission projection schema/status differs")
    if value.get("implementationAncestor") != IMPLEMENTATION_ANCESTOR:
        raise OrphanAdmissionError("M5-D orphan/admission projection ancestor differs")
    if value.get("closedOrphanClasses") != ORPHAN_CLASSES:
        raise OrphanAdmissionError("closed orphan taxonomy differs")
    if value.get("classesAllowedToMark") != MARK_CLASSES:
        raise OrphanAdmissionError("mark-eligible orphan taxonomy differs")
    if value.get("permanentRetainClasses") != PERMANENT_CLASSES:
        raise OrphanAdmissionError("permanent orphan evidence taxonomy differs")
    if value.get("quarantineOnlyClasses") != ["UNKNOWN_OR_FOREIGN"]:
        raise OrphanAdmissionError("foreign/unknown quarantine taxonomy differs")
    if value.get("markProtocol") != {
        "sequence": [
            "DISCOVERED",
            "ORPHAN_MARK",
            "AUTHORITY_TIME_GRACE",
            "FULL_RESCAN",
            "FUTURE_INTENT_CANDIDATE",
        ],
        "listingIsAuthority": False,
        "responseUnknownCreateCanOrphan": False,
        "liveDeterministicOwnerIsAdopted": True,
        "markRootRevalidated": True,
        "futureIntentCandidateIsDispatchAuthority": False,
    }:
        raise OrphanAdmissionError("mark/grace/rescan protocol differs")
    admission = value.get("perCellAdmission")
    if not isinstance(admission, dict) or admission != {
        "hardIdentityIsolation": True,
        "reservedMinimumBoundedByHardLimit": True,
        "overflowFailsClosed": True,
        "accountedGroups": [
            "candidate_intent_done_counts_and_bytes",
            "delete_reconciliation_unknown_queues_and_age",
            "object_multipart_bookkeeper_oxia_kms_network_concurrency",
            "request_byte_io_retry_rates",
            "cache_entries_bytes_and_target_buffers",
            "scanner_pages_keys_bytes",
            "quarantine_count_bytes_age",
        ],
    }:
        raise OrphanAdmissionError("per-Cell admission inventory differs")
    if value.get("focusedTests") != {
        "orphanProtocolTests": 7,
        "cellAdmissionTests": 5,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }:
        raise OrphanAdmissionError("focused orphan/admission test binding differs")
    for field in (
        "externalDeleteApiPresent",
        "intentMutationApiPresent",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        if value.get(field) is not False:
            raise OrphanAdmissionError(f"M5-D orphan/admission projection overstates {field}")


def validate_sources(root: Path) -> None:
    missing = [path for path in REQUIRED_SOURCES if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise OrphanAdmissionError(f"M5-D orphan/admission sources are missing/empty: {missing}")
    orphan = (root / REQUIRED_SOURCES[0]).read_text(encoding="utf-8")
    admission = (root / REQUIRED_SOURCES[1]).read_text(encoding="utf-8")
    orphan_test = (root / REQUIRED_SOURCES[2]).read_text(encoding="utf-8")
    admission_test = (root / REQUIRED_SOURCES[3]).read_text(encoding="utf-8")
    require_literals(
        orphan,
        tuple(ORPHAN_CLASSES)
        + (
            "MAY_ENTER_MARK_PROTOCOL",
            "PERMANENT_RETAIN",
            "QUARANTINE_ONLY",
            "FUTURE_INTENT_CANDIDATE",
            "currentAuthorityTimeMillis < mark.graceDeadlineAuthorityTimeMillis()",
            "validMark(mark)",
            "responseLossPathsReconciled()",
            "authoritativeOwnerPresent()",
        ),
        "orphan protocol",
    )
    require_literals(
        admission,
        (
            "CellBudgetEnvelopeV1",
            "reservedMinimum.within(hardLimit)",
            "CELL_HARD_LIMIT_REACHED",
            "CELL_IDENTITY_MISMATCH",
            "InventoryUsageV1",
            "QueueUsageV1",
            "ConcurrencyUsageV1",
            "RateUsageV1",
            "MemoryUsageV1",
            "ScannerUsageV1",
            "QuarantineUsageV1",
            "Math.addExact",
        ),
        "per-Cell admission",
    )
    require_literals(
        orphan_test,
        (
            "closedTaxonomyAllowsOnlyThreeClassesToEnterMarkProtocol",
            "permanentEvidenceNeverMarksAndForeignIdentityOnlyQuarantines",
            "pureProtocolExposesNoDeleteOrIntentMutationApi",
        ),
        "orphan protocol tests",
    )
    require_literals(
        admission_test,
        (
            "anyCountByteQueueConcurrencyRateMemoryScannerOrQuarantineCapFailsClosed",
            "arithmeticOverflowFailsClosedInsteadOfWrappingBelowLimit",
            "aCellCannotBorrowAnotherCellsIdentityOrCapacity",
        ),
        "per-Cell admission tests",
    )


def validate_tasks(root: Path) -> None:
    build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    for task in (
        "v2M5OrphanAdmissionContractTest",
        "v2M5OrphanAdmissionSourceCheck",
        "v2M5OrphanAdmissionCheck",
    ):
        if re.search(rf'tasks\.register(?:<[^>]+>)?\("{task}"\)', build) is None:
            raise OrphanAdmissionError(f"root build lacks {task}")
    task = re.search(
        r'tasks\.register\("v2M5OrphanAdmissionCheck"\) \{(?P<body>.*?)\n\}', build, re.DOTALL
    )
    if task is None or not all(
        required in task.group("body")
        for required in (
            '"v2M5BookKeeperDeleteSourceCheck"',
            '"v2M5OrphanAdmissionSourceCheck"',
            '":nereus-storage-object:test"',
        )
    ):
        raise OrphanAdmissionError("orphan/admission gate omits predecessor, tests, or source check")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    prior = load_module(root / "scripts/check-v2-m5-bookkeeper-delete.py", "nereus_m5_d_bk_for_orphan")
    prior.validate(root)
    if subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", IMPLEMENTATION_ANCESTOR, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode != 0:
        raise OrphanAdmissionError("required BookKeeper-delete implementation ancestor is missing")
    validate_projection_value(json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8")))
    validate_sources(root)
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-D orphan/admission: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
