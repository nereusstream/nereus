#!/usr/bin/env python3
"""Validate the focused M5-C Binding authority/ticket/fence implementation slice."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_BINDING_AUTHORITY_IMPLEMENTATION_NON_PROMOTABLE"
AMENDMENT_COMMIT = "3d641ab6c1a709155c40774e3304f7ffd86115bd"
AMENDMENT_MANIFEST_SHA256 = "1f769b44740d9bfb2fa5d5d29fe2207d1cc72cb9e1ea5a8384223a6da143b452"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-c-binding-authority-projection.json"
FLOOR_CLASSES = (
    "BINDING_EPOCH_POLICY",
    "KAFKA_CONSUMER_GROUP",
    "KAFKA_PRODUCER_TRANSACTION",
    "KAFKA_REPLICATION_RECOVERY",
    "PULSAR_CURSOR_SUBSCRIPTION",
    "GENERATION_READ",
    "LIFECYCLE_TASK",
    "SHARED_PHYSICAL_SOURCE",
    "PROJECTION_MIGRATION",
    "AUDIT_GRACE",
)
REFERENCE_KINDS = (
    "MANIFEST_SELECTED",
    "MANIFEST_FALLBACK",
    "READ_GENERATION_PIN_OR_OPEN_HANDLE",
    "SOURCE_PROTECTION",
    "KAFKA_GROUP_RETENTION",
    "KAFKA_PRODUCER_RECOVERY",
    "KAFKA_TRANSACTION_OR_ABORTED_RECOVERY",
    "KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY",
    "PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR",
    "RECOVERY_CHECKPOINT_OR_SNAPSHOT",
    "MATERIALIZATION_OR_COMPACTION_TASK",
    "RETIREMENT_OR_DELETE_RECONCILIATION",
    "SHARED_PHYSICAL_MEMBER",
    "PROJECTION_MIGRATION_OR_EXPORT",
    "AUDIT_GRACE",
)
REQUIRED_SOURCES = (
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/read/control/M4ReadControlCoordinatorV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5BindingAuthorityCodecV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5BindingAuthorityControlMetadataStoreV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5BindingAuthorityRecordsV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5BindingRetirementCoordinatorV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/retention/M5RetentionRetirementV1Test.java",
)


class BindingAuthorityError(RuntimeError):
    """Fail-closed M5-C Binding-authority rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise BindingAuthorityError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise BindingAuthorityError(f"{label} lacks Binding-authority contract: {missing}")


def require_ancestor(root: Path) -> None:
    result = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", AMENDMENT_COMMIT, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        raise BindingAuthorityError("accepted M5-C amendment is not an ancestor of HEAD")


def validate_required_sources(root: Path, paths: tuple[str, ...] = REQUIRED_SOURCES) -> None:
    missing = [path for path in paths if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise BindingAuthorityError(f"M5-C Binding-authority sources are missing/empty: {missing}")


def source(root: Path, name: str) -> str:
    path = next(path for path in REQUIRED_SOURCES if path.endswith("/" + name))
    return (root / path).read_text(encoding="utf-8")


def validate_runtime_contract(root: Path) -> None:
    records = source(root, "M5BindingAuthorityRecordsV1.java")
    codec = source(root, "M5BindingAuthorityCodecV1.java")
    facade = source(root, "M5BindingAuthorityControlMetadataStoreV1.java")
    coordinator = source(root, "M5BindingRetirementCoordinatorV1.java")
    m4 = source(root, "M4ReadControlCoordinatorV1.java")
    tests = source(root, "M5RetentionRetirementV1Test.java")
    require_literals(
        records,
        (
            "BindingRetirementAuthorityV1",
            "FULL_V1",
            "RETIRED_V1",
            "REFERENCE_SCAN_FENCED_V1",
            "ReferenceMutationTicketV1",
            "ReferenceWriterEnrollmentV1",
            "MAX_AUTHORITY_BYTES = 1_048_576",
            "MAX_BATCH_SLOTS = 1_024",
            "MAX_REFERENCE_MUTATION_TICKETS = 256",
            "writer enrollment does not cover the closed proof inventory",
        ),
        "authority records",
    )
    require_literals(
        codec,
        (
            "0x4d355231",
            "migrateLegacy",
            "migrateLegacyWithSuccessor",
            "selectorSuccessor",
            "projectSelector",
            "Binding authority canonical SHA-256 differs",
        ),
        "authority codec",
    )
    require_literals(
        facade,
        (
            "implements CanonicalControlMetadataStore",
            "Only the exact selector key is projected",
            "REFERENCE_SCAN_FENCED_V1",
            "requireSelectorCapacity",
        ),
        "M4 projection facade",
    )
    require_literals(m4, ("M5BindingAuthorityControlMetadataStoreV1", "keys.selector()"), "M4 coordinator")
    require_literals(
        coordinator,
        (
            "enrollWriters",
            "acquireTicket",
            "clearTicket",
            "REFERENCE_SCAN_FENCED_V1",
            "requireFresh(proof)",
            "metadata.compareAndSet",
        ),
        "single-key coordinator",
    )
    if "conditionalTransaction" in coordinator or "supportsAtomicMultiKeyTransactions" in coordinator:
        raise BindingAuthorityError("single-key coordinator references the rejected multi-key path")
    require_literals(
        tests,
        (
            "singleBindingAuthorityMigrationPreservesExactM4Projection",
            "ticketAndFenceRaceHasOnlyOneWinningAuthorityCas",
            "fencedBatchRetiresInOneSingleKeyCasAndLostResponseConverges",
            "writerEnrollment",
            "transactionCalls).isZero",
        ),
        "Binding-authority tests",
    )


def validate_projection_value(value: object) -> None:
    if not isinstance(value, dict):
        raise BindingAuthorityError("Binding-authority projection is not an object")
    if value.get("schema") != "NEREUS_V2_M5_C_BINDING_AUTHORITY_PROJECTION_V1" or value.get(
        "status"
    ) != "BINDING_AUTHORITY_IMPLEMENTED_NON_PROMOTABLE":
        raise BindingAuthorityError("Binding-authority projection schema/status differs")
    if value.get("amendmentManifestSha256") != AMENDMENT_MANIFEST_SHA256:
        raise BindingAuthorityError("Binding-authority amendment manifest SHA differs")
    expected = {
        "authorityKey": "EXISTING_M4_BINDING_SELECTOR_KEY",
        "authorityWireMagic": "M5R1",
        "legacySelectorWireMagic": "M4S1",
        "legacyMigration": "ONE_EXACT_SELECTOR_KEY_CAS",
        "m4SelectorProjection": "FULL_V1_SLOTS_IN_ACTIVATION_ORDER_ONLY",
        "referenceMutationProtocol": "DURABLE_TICKET_THEN_EXTERNAL_MUTATION_THEN_EXACT_REREAD_THEN_TICKET_CLEAR",
        "scanFence": "ZERO_TICKETS_THEN_REFERENCE_SCAN_FENCED_V1",
        "retirementTransition": "FENCED_FULL_V1_TO_PERMANENT_RETIRED_V1_SLOT_ONE_EXACT_CAS",
    }
    for field, expected_value in expected.items():
        if value.get(field) != expected_value:
            raise BindingAuthorityError(f"Binding-authority projection {field} differs")
    if value.get("hardCaps") != {
        "authorityBytes": 1_048_576,
        "batchSlots": 1_024,
        "referenceMutationTickets": 256,
        "concurrentScanFences": 1,
    }:
        raise BindingAuthorityError("Binding-authority hard caps differ")
    enrollment = value.get("writerEnrollment", {})
    if enrollment.get("required") is not True or enrollment.get("floorClasses") != list(
        FLOOR_CLASSES
    ) or enrollment.get("referenceKinds") != list(REFERENCE_KINDS):
        raise BindingAuthorityError("Binding-authority writer enrollment differs")
    backend = value.get("selectedMetadataBackend", {})
    if backend != {
        "adapter": "Oxia09ExactMetadataTransactionStoreV1",
        "clientArtifact": "io.github.oxia-db:oxia-client:0.9.4",
        "operation": "EXACT_SINGLE_KEY_COMPARE_AND_SET",
        "supportsSelectedOperation": True,
        "atomicMultiKeyTransactionRequired": False,
        "internalWriteBatchAuthority": False,
        "sequentialCasFallback": "FORBIDDEN",
    }:
        raise BindingAuthorityError("Binding-authority backend capability projection differs")
    if value.get("bindingAuthorityGatePresent") is not True:
        raise BindingAuthorityError("Binding-authority focused gate is not present")
    for field in (
        "fullRetirementGatePresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        if value.get(field) is not False:
            raise BindingAuthorityError(f"Binding-authority projection overstates {field}")


def validate_tasks(root: Path) -> None:
    root_build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    module_build = (root / "nereus-storage-object/build.gradle.kts").read_text(encoding="utf-8")
    require_literals(
        root_build,
        (
            "v2M5DesignAmendmentCheck",
            "v2M5RetentionCoreCheck",
            "v2M5BindingAuthorityContractTest",
            "v2M5BindingAuthoritySourceCheck",
            "v2M5BindingAuthorityCheck",
        ),
        "root build",
    )
    if 'tasks.register<Test>("v2M5BindingAuthorityTest")' not in module_build:
        raise BindingAuthorityError("storage-object build lacks v2M5BindingAuthorityTest")
    if re.search(r'tasks\.register(?:<[^>]+>)?\("v2M5RetentionRetirementCheck"\)', root_build):
        raise BindingAuthorityError("full M5-C retirement gate exists before the complete retirement slice")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    amendment = load_module(
        root / "scripts/check-v2-m5-design-amendment.py", "nereus_m5_amendment_for_binding_authority"
    )
    amendment.validate(root)
    require_ancestor(root)
    validate_required_sources(root)
    validate_runtime_contract(root)
    validate_projection_value(json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8")))
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-C Binding authority: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
