#!/usr/bin/env python3
"""Validate the non-promotable M5-D same-key CAS coordinator and writer guard."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_TARGET_DELETE_AUTHORITY_COORDINATOR_NON_PROMOTABLE"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-d-target-delete-authority-coordinator-projection.json"
FOUNDATION_COMMIT = "f05037bb16017f24e8147e99a61f26e539ed85fa"
TRANSITIONS = [
    "CREATE_OPEN",
    "QUALIFY_TYPED_ELIGIBILITY_AFTER_EXACT_FACT_REREAD",
    "ACQUIRE_WRITER_TICKET",
    "COMPLETE_WRITER_TICKET_AFTER_EXACT_RECONCILIATION",
    "CAS_1_PREPARE_IDENTITY_READ",
    "REFRESH_READ_FENCED_AFTER_NATIVE_OWNER_VERIFICATION",
    "CAS_2_BIND_DELETE_INTENT",
    "TAKE_OVER_DISPATCH_AFTER_OLD_OWNER_FENCED",
    "COMPLETE_DELETE_DONE",
]
OUTCOMES = [
    "APPLIED_EXACT",
    "EXISTING_EXACT",
    "EXISTING_TERMINAL",
    "PREDECESSOR_UNCHANGED",
    "DEFINITIVE_CONFLICT",
    "RESPONSE_UNKNOWN",
    "QUARANTINED",
]
REQUIRED_SOURCES = (
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc/M5TargetDeleteAuthorityCoordinatorV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc/M5TargetDeleteWriterGuardV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/gc/M5TargetDeleteAuthorityCoordinatorV1Test.java",
)


class TargetAuthorityCoordinatorError(RuntimeError):
    """Stable fail-closed M5-D same-key coordinator rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise TargetAuthorityCoordinatorError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise TargetAuthorityCoordinatorError(f"{label} lacks closed M5-D contract: {missing}")


def validate_projection_value(value: object) -> None:
    members = {
        "schema",
        "status",
        "foundationCommit",
        "metadataOperation",
        "transitionOperations",
        "reconciliationOutcomes",
        "writerGuard",
        "sameKeyOnly",
        "conditionalMultiKeyCalls",
        "sequentialMultiKeyEmulation",
        "focusedTests",
        "persistedMutationCoordinatorPresent",
        "genericClosedWriterGuardPresent",
        "allProofChangingWritersIntegrated",
        "externalIdentityReaderPresent",
        "externalDeleteCompositionPresent",
        "realOxiaExecutionPresent",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    }
    if not isinstance(value, dict) or set(value) != members:
        raise TargetAuthorityCoordinatorError("target-authority coordinator projection members differ")
    if value.get("schema") != "NEREUS_V2_M5_D_TARGET_DELETE_AUTHORITY_COORDINATOR_PROJECTION_V1" or value.get(
        "status"
    ) != "SAME_KEY_CAS_COORDINATOR_AND_WRITER_GUARD_IMPLEMENTED_NON_PROMOTABLE":
        raise TargetAuthorityCoordinatorError("target-authority coordinator projection schema/status differs")
    if value.get("foundationCommit") != FOUNDATION_COMMIT:
        raise TargetAuthorityCoordinatorError("target-authority coordinator foundation commit differs")
    if value.get("metadataOperation") != "ExactMetadataTransactionStoreV1.compareAndSet":
        raise TargetAuthorityCoordinatorError("target-authority metadata operation differs")
    if value.get("transitionOperations") != TRANSITIONS:
        raise TargetAuthorityCoordinatorError("target-authority transition inventory differs")
    if value.get("reconciliationOutcomes") != OUTCOMES:
        raise TargetAuthorityCoordinatorError("target-authority reconciliation outcomes differ")
    if value.get("writerGuard") != {
        "ticketMustBeAuthoritativelyVisibleBeforeDispatch": True,
        "responseUnknownRetainsTicket": True,
        "exceptionRetainsTicket": True,
        "terminalReconciliationRequiredBeforeTicketRemoval": True,
        "fenceWinningFirstPreventsDispatch": True,
        "timeoutClearsTicket": False,
    }:
        raise TargetAuthorityCoordinatorError("proof-bound writer guard projection differs")
    if value.get("sameKeyOnly") is not True or value.get("conditionalMultiKeyCalls") != 0:
        raise TargetAuthorityCoordinatorError("coordinator is not projected as exact same-key only")
    if value.get("sequentialMultiKeyEmulation") is not False:
        raise TargetAuthorityCoordinatorError("projection permits sequential multi-key emulation")
    if value.get("focusedTests") != {
        "coordinatorAndGuardTests": 19,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }:
        raise TargetAuthorityCoordinatorError("coordinator focused test binding differs")
    for field in ("persistedMutationCoordinatorPresent", "genericClosedWriterGuardPresent"):
        if value.get(field) is not True:
            raise TargetAuthorityCoordinatorError(f"coordinator projection understates {field}")
    for field in (
        "allProofChangingWritersIntegrated",
        "externalIdentityReaderPresent",
        "externalDeleteCompositionPresent",
        "realOxiaExecutionPresent",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        if value.get(field) is not False:
            raise TargetAuthorityCoordinatorError(f"coordinator projection overstates {field}")


def validate_sources(root: Path) -> None:
    missing = [path for path in REQUIRED_SOURCES if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise TargetAuthorityCoordinatorError(f"target-authority coordinator sources are missing/empty: {missing}")
    coordinator, writer_guard, tests = [(root / path).read_text(encoding="utf-8") for path in REQUIRED_SOURCES]
    require_literals(
        coordinator,
        tuple(OUTCOMES)
        + (
            "ExactMetadataTransactionStoreV1",
            "metadata.compareAndSet(predecessor, authorityKey, candidate)",
            "exactCandidateIsAuthoritative",
            "observed.equals(predecessor)",
            "create && observed.isPresent()",
            "!create && observed.isEmpty()",
            "prepareIdentityRead",
            "bindDeleteIntent",
            "takeOverDispatch",
            "completeDelete",
            "unsupported multi-key transactions remain",
        ),
        "target-authority coordinator",
    )
    require_literals(
        writer_guard,
        (
            "ProofBoundMutationV1",
            "DurableWriterDispatchV1",
            "reconcileOrDispatch",
            "TICKET_RETAINED_RESPONSE_UNKNOWN_V1",
            "TICKET_RETAINED_CONCURRENT_AUTHORITY_CHANGE_V1",
            "NOT_DISPATCHED_V1",
            "activeWriterTickets().stream().noneMatch(ticket::equals)",
            "completeWriterTicket",
        ),
        "proof-bound writer guard",
    )
    require_literals(
        tests,
        (
            "persistsEveryLifecycleTransitionAtOneKeyWithSameKeyCasOnly",
            "responseUnknownAfterApplyReconcilesTheExactCandidate",
            "responseUnknownWithoutApplyLeavesTheExactPredecessorAndDoesNotAdvance",
            "ticketWinningTheExactCasMakesTheCompetingFenceConflict",
            "missingPermanentAuthorityAfterTransitionAttemptQuarantines",
            "writerGuardDispatchesOnlyAfterTicketIsAuthoritativelyVisibleThenClearsIt",
            "writerResponseLossRetainsTheDurableTicket",
            "fenceWinningFirstPreventsGuardedExternalWriterDispatch",
            "transactionCalls).isZero()",
        ),
        "target-authority coordinator tests",
    )
    if coordinator.count("metadata.compareAndSet(") != 1:
        raise TargetAuthorityCoordinatorError("coordinator does not have exactly one same-key CAS call site")
    production = coordinator + "\n" + writer_guard
    if re.search(r"\.conditionalTransaction\s*\(", production):
        raise TargetAuthorityCoordinatorError("coordinator calls forbidden conditional multi-key transaction")
    for forbidden in ("ObjectProviderTransport", "M5BookKeeperDeleteAdapterV1", "M5ObjectDeleteSessionV1"):
        if forbidden in production:
            raise TargetAuthorityCoordinatorError(
                f"in-memory coordinator unexpectedly composes an external delete adapter: {forbidden}"
            )


def validate_governance(root: Path) -> None:
    for path, literals, label in (
        (
            "docs/v2/detailed_design/m5/README.md",
            (PROJECTION_PATH.split("/")[-1], "v2M5TargetDeleteAuthorityCoordinatorCheck"),
            "M5 index",
        ),
        (
            "docs/v2/detailed_design/m5/m5-implementation-log.md",
            ("M5-D same-key coordinator and writer guard", RESULT, "Real Oxia execution remains absent"),
            "M5 implementation log",
        ),
        (
            "docs/v2/08-implementation-plan-and-gates.md",
            ("v2M5TargetDeleteAuthorityCoordinatorCheck", "durable writer-ticket guard"),
            "M5 implementation plan",
        ),
        (
            "docs/v2/README.md",
            ("v2M5TargetDeleteAuthorityCoordinatorCheck", "same-key CAS coordinator"),
            "V2 index",
        ),
    ):
        require_literals((root / path).read_text(encoding="utf-8"), literals, label)


def validate_tasks(root: Path) -> None:
    root_build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    module_build = (root / "nereus-storage-object/build.gradle.kts").read_text(encoding="utf-8")
    for task in (
        "v2M5TargetDeleteAuthorityCoordinatorContractTest",
        "v2M5TargetDeleteAuthorityCoordinatorSourceCheck",
        "v2M5TargetDeleteAuthorityCoordinatorCheck",
    ):
        if re.search(rf'tasks\.register(?:<[^>]+>)?\("{task}"\)', root_build) is None:
            raise TargetAuthorityCoordinatorError(f"root build lacks {task}")
    aggregate = re.search(
        r'tasks\.register\("v2M5TargetDeleteAuthorityCoordinatorCheck"\) \{(?P<body>.*?)\n\}',
        root_build,
        re.DOTALL,
    )
    if aggregate is None or not all(
        value in aggregate.group("body")
        for value in (
            '"v2M5TargetDeleteAuthorityFoundationCheck"',
            '"v2M5TargetDeleteAuthorityCoordinatorContractTest"',
            '"v2M5TargetDeleteAuthorityCoordinatorSourceCheck"',
            '":nereus-storage-object:v2M5TargetDeleteAuthorityCoordinatorTest"',
            '":nereus-storage-object:checkstyleMain"',
            '":nereus-storage-object:checkstyleTest"',
            '":nereus-storage-object:spotlessCheck"',
        )
    ):
        raise TargetAuthorityCoordinatorError("target-authority coordinator gate omits a required predecessor/check")
    if re.search(r'tasks\.register<Test>\("v2M5TargetDeleteAuthorityCoordinatorTest"\)', module_build) is None or (
        "com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1Test" not in module_build
    ):
        raise TargetAuthorityCoordinatorError("storage-object build lacks the focused coordinator test task")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    foundation = load_module(
        root / "scripts/check-v2-m5-target-delete-authority-foundation.py",
        "nereus_m5_d_foundation_for_target_authority_coordinator",
    )
    foundation.validate(root)
    if subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", FOUNDATION_COMMIT, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode != 0:
        raise TargetAuthorityCoordinatorError("target-authority foundation commit is not an ancestor of HEAD")
    validate_projection_value(json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8")))
    validate_sources(root)
    validate_governance(root)
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-D target authority coordinator: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
