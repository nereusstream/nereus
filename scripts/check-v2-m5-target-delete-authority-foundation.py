#!/usr/bin/env python3
"""Validate the non-promotable M5-D target-scoped delete-authority foundation."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_TARGET_DELETE_AUTHORITY_FOUNDATION_NON_PROMOTABLE"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-d-target-delete-authority-foundation-projection.json"
AMENDMENT_COMMIT = "a3b3884b3da8664465bc114e10b33e35ad32f311"
STATES = ["OPEN_V1", "READ_FENCED_V1", "DELETE_INTENT_V1", "DELETE_DONE_V1"]
WRITER_CLASSES = [
    "M4_SOURCE_PROTECTION_RELEASE_V1",
    "MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1",
    "LOGICAL_TRIM_RETENTION_FLOOR_V1",
    "REFERENCE_SHARED_PHYSICAL_MEMBER_V1",
    "REPLICA_TOPOLOGY_V1",
    "MULTIPART_INVENTORY_PUBLICATION_RESPONSE_LOSS_V1",
    "TASK_PROJECTION_MIGRATION_RECOVERY_V1",
    "OWNER_WORKER_LEASE_HANDLE_PIN_V1",
    "PROVIDER_KMS_STORAGE_PROFILE_CAPABILITY_V1",
    "DISPATCH_OWNER_CELL_RESERVATION_V1",
]
TARGET_KINDS = [
    "OBJECT_VERSION_V1",
    "BOOKKEEPER_LEDGER_V1",
    "PULSAR_ROOT_OBJECT_V1",
    "PULSAR_DATA_OBJECT_V1",
    "MULTIPART_UPLOAD_V1",
]
REQUIRED_SOURCES = (
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc/M5TargetDeleteAuthorityKeysV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc/M5TargetDeleteAuthorityRecordsV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc/M5TargetDeleteAuthorityCodecV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc/M5TargetDeleteAuthorityStateMachineV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/gc/M5TargetDeleteAuthorityV1Test.java",
)


class TargetAuthorityFoundationError(RuntimeError):
    """Stable fail-closed M5-D target-authority foundation rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise TargetAuthorityFoundationError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise TargetAuthorityFoundationError(f"{label} lacks closed M5-D contract: {missing}")


def validate_projection_value(value: object) -> None:
    members = {
        "schema",
        "status",
        "amendmentCommit",
        "wire",
        "authorityStates",
        "closedWriterClasses",
        "targetKinds",
        "invariants",
        "focusedTests",
        "pureCandidateStateMachinePresent",
        "persistedMutationCoordinatorPresent",
        "closedWriterRuntimeIntegrationPresent",
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
        raise TargetAuthorityFoundationError("target-authority projection members differ")
    if value.get("schema") != "NEREUS_V2_M5_D_TARGET_DELETE_AUTHORITY_FOUNDATION_PROJECTION_V1" or value.get(
        "status"
    ) != "TARGET_SCOPED_AUTHORITY_FOUNDATION_IMPLEMENTED_NON_PROMOTABLE":
        raise TargetAuthorityFoundationError("target-authority projection schema/status differs")
    if value.get("amendmentCommit") != AMENDMENT_COMMIT:
        raise TargetAuthorityFoundationError("target-authority projection amendment commit differs")
    if value.get("wire") != {
        "magic": "M5DA",
        "version": 1,
        "maximumAuthorityBytes": 1_048_576,
        "maximumTargetIdentityBytes": 65_536,
        "maximumExternalIdentityBytes": 262_144,
        "maximumWriterTickets": 256,
    }:
        raise TargetAuthorityFoundationError("target-authority wire projection differs")
    if value.get("authorityStates") != STATES:
        raise TargetAuthorityFoundationError("target-authority states differ")
    if value.get("closedWriterClasses") != WRITER_CLASSES:
        raise TargetAuthorityFoundationError("closed proof-bound writer inventory differs")
    if value.get("targetKinds") != TARGET_KINDS:
        raise TargetAuthorityFoundationError("closed physical target inventory differs")
    if value.get("invariants") != {
        "onePermanentKeyPerTarget": True,
        "cellAndTargetDomainSeparated": True,
        "everySuccessorIncrementsRevision": True,
        "everySuccessorBindsExactPredecessorDigest": True,
        "canonicalNoOpAndAbaForbidden": True,
        "ticketVisibleBeforeProofMutation": True,
        "activeTicketVetoesReadFence": True,
        "readFenceRejectsNewTickets": True,
        "cas1GrantsDispatch": False,
        "cas2BindsExactIdentityAttemptOwnerAndCapability": True,
        "takeoverKeepsFixedAttemptAndIntentRevision": True,
        "doneIsPermanent": True,
        "crossTargetAtomicity": False,
    }:
        raise TargetAuthorityFoundationError("target-authority invariant projection differs")
    if value.get("focusedTests") != {
        "authorityTests": 11,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }:
        raise TargetAuthorityFoundationError("target-authority focused test binding differs")
    if value.get("pureCandidateStateMachinePresent") is not True:
        raise TargetAuthorityFoundationError("pure target-authority state machine is not recorded")
    for field in (
        "persistedMutationCoordinatorPresent",
        "closedWriterRuntimeIntegrationPresent",
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
            raise TargetAuthorityFoundationError(f"target-authority projection overstates {field}")


def validate_sources(root: Path) -> None:
    missing = [path for path in REQUIRED_SOURCES if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise TargetAuthorityFoundationError(f"target-authority sources are missing/empty: {missing}")
    keys, records, codec, state_machine, tests = [
        (root / path).read_text(encoding="utf-8") for path in REQUIRED_SOURCES
    ]
    require_literals(
        keys,
        (
            "NEREUS_V2_M5_TARGET_DELETE_IDENTITY_V1",
            "NEREUS_V2_M5_EXTERNAL_DELETE_IDENTITY_V1",
            "NEREUS_V2_M5_DELETE_DISPATCH_TOKEN_V1",
            '"v2/physical-delete-m5/"',
            '"/authority-v1"',
            "targetIdentitySha256",
            "dispatchTokenSha256",
        ),
        "target-authority keys",
    )
    require_literals(
        records,
        tuple(STATES)
        + tuple(WRITER_CLASSES)
        + tuple(TARGET_KINDS)
        + (
            "authorityRevision",
            "predecessorAuthoritySha256",
            "closedWriterFenceEpoch",
            "ProofBoundWriterTicketV1",
            "TargetReadFenceV1",
            "ExactExternalIdentityV1",
            "TargetDeleteIntentV1",
            "TargetDeleteDoneV1",
            "writer enrollment does not cover the exact closed inventory",
            "writer tickets may exist only while target authority is OPEN_V1",
        ),
        "target-authority records",
    )
    require_literals(
        codec,
        (
            "0x4d354441",
            "Math.addExact(current.authorityRevision(), 1)",
            "Sha256Digest.hash(predecessor)",
            "DELETE_DONE_V1 is permanent and has no successor",
            "target delete authority canonical SHA-256 differs",
            "target delete authority has trailing bytes",
        ),
        "target-authority codec",
    )
    require_literals(
        state_machine,
        (
            "acquireWriterTicket",
            "completeWriterTicket",
            "prepareIdentityRead",
            "active or unresolved writer ticket vetoes CAS-1",
            "bindDeleteIntent",
            "takeOverDispatch",
            "oldOwnerFencedProofSha256",
            "completeDelete",
            "requireExactDispatchAuthority",
            "ExternalIdentityObservationV1.ABSENT_EXACT_V1",
        ),
        "target-authority state machine",
    )
    require_literals(
        tests,
        (
            "canonicalOpenAuthorityRoundTripsAtOnePermanentTargetKey",
            "everyTicketMutationConsumesARevisionAndPreventsCanonicalAba",
            "ticketBeforeFenceVetoesCas1AndFenceBeforeTicketRejectsTheWriter",
            "competingTicketAndFenceCandidatesBindTheSameExactPredecessor",
            "cas2BindsExactIdentityAttemptOwnerAndDispatchToken",
            "dispatchTakeoverKeepsTheFixedAttemptAndRequiresFencedOldOwner",
            "doneIsPermanentAndBindsTheExactLastIntent",
            "codecRejectsTamperTruncationAndTrailingBytes",
            "closedEnrollmentAndCellBoundTargetIdentityFailClosed",
        ),
        "target-authority tests",
    )
    production = "\n".join((keys, records, codec, state_machine))
    for forbidden in (
        "conditionalTransaction(",
        "commitExactMultiKey",
        "ObjectProviderTransport",
        "BookKeeper",
        "compareAndSet(",
    ):
        if forbidden in production:
            raise TargetAuthorityFoundationError(
                f"pure target-authority foundation unexpectedly contains runtime/external surface: {forbidden}"
            )


def validate_governance(root: Path) -> None:
    for path, literals, label in (
        (
            "docs/v2/detailed_design/m5/README.md",
            (PROJECTION_PATH.split("/")[-1], "v2M5TargetDeleteAuthorityFoundationCheck"),
            "M5 index",
        ),
        (
            "docs/v2/detailed_design/m5/m5-implementation-log.md",
            ("M5-D target-scoped authority foundation", RESULT, "no metadata mutation"),
            "M5 implementation log",
        ),
        (
            "docs/v2/08-implementation-plan-and-gates.md",
            ("v2M5TargetDeleteAuthorityFoundationCheck", "pure target-scoped authority"),
            "M5 implementation plan",
        ),
        (
            "docs/v2/README.md",
            ("v2M5TargetDeleteAuthorityFoundationCheck", "M5DA"),
            "V2 index",
        ),
    ):
        require_literals((root / path).read_text(encoding="utf-8"), literals, label)


def validate_tasks(root: Path) -> None:
    root_build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    module_build = (root / "nereus-storage-object/build.gradle.kts").read_text(encoding="utf-8")
    for task in (
        "v2M5TargetDeleteAuthorityFoundationContractTest",
        "v2M5TargetDeleteAuthorityFoundationSourceCheck",
        "v2M5TargetDeleteAuthorityFoundationCheck",
    ):
        if re.search(rf'tasks\.register(?:<[^>]+>)?\("{task}"\)', root_build) is None:
            raise TargetAuthorityFoundationError(f"root build lacks {task}")
    aggregate = re.search(
        r'tasks\.register\("v2M5TargetDeleteAuthorityFoundationCheck"\) \{(?P<body>.*?)\n\}',
        root_build,
        re.DOTALL,
    )
    if aggregate is None or not all(
        value in aggregate.group("body")
        for value in (
            '"v2M5DesignAmendment2Check"',
            '"v2M5MultipartCleanupSourceCheck"',
            '"v2M5TargetDeleteAuthorityFoundationContractTest"',
            '"v2M5TargetDeleteAuthorityFoundationSourceCheck"',
            '":nereus-storage-object:v2M5TargetDeleteAuthorityFoundationTest"',
            '":nereus-storage-object:checkstyleMain"',
            '":nereus-storage-object:checkstyleTest"',
            '":nereus-storage-object:spotlessCheck"',
        )
    ):
        raise TargetAuthorityFoundationError("target-authority foundation gate omits a required predecessor/check")
    if re.search(r'tasks\.register<Test>\("v2M5TargetDeleteAuthorityFoundationTest"\)', module_build) is None or (
        "com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityV1Test" not in module_build
    ):
        raise TargetAuthorityFoundationError("storage-object build lacks the focused target-authority test task")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    amendment = load_module(
        root / "scripts/check-v2-m5-design-amendment-2.py",
        "nereus_m5_d_amendment_for_target_authority_foundation",
    )
    amendment.validate(root)
    prior = load_module(
        root / "scripts/check-v2-m5-multipart-cleanup.py",
        "nereus_m5_d_multipart_for_target_authority_foundation",
    )
    prior.validate(root)
    if subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", AMENDMENT_COMMIT, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode != 0:
        raise TargetAuthorityFoundationError("accepted M5-D amendment commit is not an ancestor of HEAD")
    validate_projection_value(json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8")))
    validate_sources(root)
    validate_governance(root)
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-D target authority foundation: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
