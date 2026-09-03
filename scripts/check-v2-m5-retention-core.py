#!/usr/bin/env python3
"""Validate the non-promotable M5-C core and its explicit metadata capability blocker."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_RETENTION_CORE_NON_PROMOTABLE_OXIA_ATOMIC_TRANSACTION_UNSUPPORTED"
M5_B_COMMIT = "027e3bbdb6e60e2b3ec72108ed4b546ba6a15cc8"
OXIA_CLIENT_COMMIT = "091a42c2780d92da56e9ec1f02ce1c3d988adc16"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-c-capability-projection.json"
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
LIMIT_KINDS = (
    "RESERVATIONS",
    "RETAINED_SOURCE_COUNT",
    "RETAINED_SOURCE_BYTES",
    "RETAINED_SOURCE_MAX_AGE_MILLIS",
    "ACTIVE_FULL_BATCHES",
    "FULL_BATCH_BYTES",
    "MEMBER_SCAN_COUNT",
    "EXTERNALIZATION_UNKNOWNS",
    "PERMANENT_BATCH_TOMBSTONE_COUNT",
    "PERMANENT_BATCH_TOMBSTONE_BYTES",
    "PULSAR_SELECTOR_COUNT",
    "PULSAR_SELECTOR_BYTES",
    "PULSAR_TOMBSTONE_COUNT",
    "PULSAR_TOMBSTONE_BYTES",
    "REFERENCE_ROWS",
    "REFERENCE_PAGES",
    "REFERENCE_BYTES",
    "AUDIT_GRACE_BACKLOG",
    "QUARANTINES",
)
REQUIRED_SOURCES = (
    "nereus-metadata-spi/src/main/java/com/nereusstream/metadata/spi/retention/ExactMetadataTransactionStoreV1.java",
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2/retention/Oxia09ExactMetadataTransactionStoreV1.java",
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2/retention/Oxia09ExactMetadataTransactionStoreV1Test.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5LogicalTrimCoordinatorV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5ReferenceFreshnessVerifierV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5RetentionAdmissionV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5RetentionCodecV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5RetentionEvidenceAssemblerV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5RetentionKeysV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5RetentionPlannerV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5RetentionRecordsV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5RetirementCoordinatorV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/retention/M5RetentionEvidenceAssemblerV1Test.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/retention/M5RetentionRetirementV1Test.java",
)


class RetentionCoreError(RuntimeError):
    """Fail-closed M5-C core rejection."""


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RetentionCoreError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_ancestor(root: Path, commit: str = M5_B_COMMIT) -> None:
    result = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", commit, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        raise RetentionCoreError("accepted M5-B implementation is not an ancestor of HEAD")


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise RetentionCoreError(f"{label} lacks required M5-C core contract: {missing}")


def validate_required_sources(root: Path, paths: tuple[str, ...] = REQUIRED_SOURCES) -> None:
    missing = [path for path in paths if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise RetentionCoreError(f"M5-C core sources are missing/empty: {missing}")


def source(root: Path, name: str) -> str:
    path = next(path for path in REQUIRED_SOURCES if path.endswith("/" + name))
    return (root / path).read_text(encoding="utf-8")


def validate_runtime_contract(root: Path) -> None:
    port = source(root, "ExactMetadataTransactionStoreV1.java")
    adapter = source(root, "Oxia09ExactMetadataTransactionStoreV1.java")
    records = source(root, "M5RetentionRecordsV1.java")
    admission = source(root, "M5RetentionAdmissionV1.java")
    assembler = source(root, "M5RetentionEvidenceAssemblerV1.java")
    coordinator = source(root, "M5RetirementCoordinatorV1.java")
    freshness = source(root, "M5ReferenceFreshnessVerifierV1.java")
    trim = source(root, "M5LogicalTrimCoordinatorV1.java")
    tests = source(root, "M5RetentionRetirementV1Test.java")
    assembler_tests = source(root, "M5RetentionEvidenceAssemblerV1Test.java")
    adapter_tests = source(root, "Oxia09ExactMetadataTransactionStoreV1Test.java")
    require_literals(port, ("ExactTransaction", "conditionalTransaction", "UNSUPPORTED", "sequential"), "SPI")
    require_literals(
        adapter,
        (
            "supportsAtomicMultiKeyTransactions()",
            "return false",
            "TransactionOutcome.UNSUPPORTED",
            "prevents a sequential-CAS approximation",
        ),
        "Oxia adapter",
    )
    require_literals(
        records,
        REFERENCE_KINDS
        + ("FULL_V1", "RETIRED_V1", "PhysicalCleanupSummaryV1", "canonicalProtectionBytes"),
        "records",
    )
    require_literals(admission, LIMIT_KINDS + ("REJECTED_CAP", "AlertV1", "compareAndSet"), "admission")
    require_literals(
        assembler,
        ("exactFloorRegistry", "exactReferenceRegistry", "freshness.requireFresh", "closed inventory"),
        "evidence assembler",
    )
    require_literals(
        coordinator,
        (
            "supportsAtomicMultiKeyTransactions",
            "conditionalTransaction",
            "Outcome.UNSUPPORTED",
            "Outcome.QUARANTINED",
            "retirePulsarAggregate",
            "validateReleases",
        ),
        "retirement coordinator",
    )
    require_literals(freshness, ("requireFresh", "StaleAuthorityException"), "freshness verifier")
    require_literals(trim, ("minimumSafeFloor", "compareAndSet", "no physical-delete API"), "logical trim")
    require_literals(
        tests,
        (
            "referenceFreeProofRequiresEveryKindCompleteAndAbsent",
            "externalizationIsOneAtomicTransitionAndResponseLossConverges",
            "unsupportedBackendPerformsNoTransactionAndNeverCreatesSplitState",
            "admissionPersistsExactCapsAndRejectsBeforeExceedingThem",
            "pulsarAggregateRetirementInstallsPermanentSameKeyTombstone",
        ),
        "retention tests",
    )
    require_literals(
        assembler_tests,
        (
            "closedRegistriesBuildFreshCanonicalSnapshotAndProof",
            "everyMissingFloorAdapterIsRejectedBeforeAnyScan",
            "everyMissingReferenceAdapterIsRejectedBeforeAnyScan",
            "everyFloorAuthorityVersionChangeDuringScanIsRejected",
            "everyPresentReferenceKindVetoesProofAssembly",
            "everyReferenceAuthorityVersionChangeDuringScanIsRejected",
            "multiplePresentReferenceKindsRemainACombinedVeto",
        ),
        "evidence assembler tests",
    )
    require_literals(
        adapter_tests,
        (
            "singleKeyCasUsesExactVersionAndConvergesAfterResponseLoss",
            "multiKeyTransactionIsExplicitlyUnsupportedWithoutAnyBackendMutation",
        ),
        "Oxia capability tests",
    )


def validate_source_lock(root: Path) -> None:
    locks = json.loads((root / "docs/v2/source-locks.json").read_text(encoding="utf-8"))
    binding = locks.get("dependencyEvidenceBindings", {}).get("oxiaClientArtifacts", {})
    if binding.get("requiredModules") != [
        "io.github.oxia-db:oxia-client-api:0.9.4",
        "io.github.oxia-db:oxia-client:0.9.4",
    ]:
        raise RetentionCoreError("source-locked Oxia 0.9.4 modules differ")
    outputs = locks.get("dependencyForkOutputs", [])
    output = next((value for value in outputs if value.get("id") == "oxia-client-notification-continuity"), None)
    if output is None or output.get("finalForkCommit") != OXIA_CLIENT_COMMIT:
        raise RetentionCoreError("source-locked Oxia client commit differs")


def validate_projection(root: Path) -> None:
    value = json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8"))
    if value.get("schema") != "NEREUS_V2_M5_C_CAPABILITY_PROJECTION_V1":
        raise RetentionCoreError("M5-C capability projection schema differs")
    if value.get("status") != "RETENTION_CORE_NON_PROMOTABLE":
        raise RetentionCoreError("M5-C capability projection overstates its status")
    backend = value.get("selectedMetadataBackend", {})
    expected = {
        "clientArtifact": "io.github.oxia-db:oxia-client:0.9.4",
        "clientSourceCommit": OXIA_CLIENT_COMMIT,
        "adapter": "Oxia09ExactMetadataTransactionStoreV1",
        "supportsAtomicMultiKeyTransaction": False,
        "unsupportedBehavior": "RETURN_UNSUPPORTED_BEFORE_MUTATION",
        "sequentialCasFallback": "FORBIDDEN",
    }
    if value.get("designRequiresAtomicMultiKeyTransaction") is not True or backend != expected:
        raise RetentionCoreError("M5-C atomic transaction capability projection differs")
    if value.get("closedReferenceKinds") != list(REFERENCE_KINDS):
        raise RetentionCoreError("M5-C closed reference inventory differs")
    if value.get("closedAdmissionLimits") != list(LIMIT_KINDS):
        raise RetentionCoreError("M5-C closed admission inventory differs")
    for field in (
        "fullRetirementGatePresent",
        "metadataRetirementAuthority",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
    ):
        if value.get(field) is not False:
            raise RetentionCoreError(f"M5-C capability projection overstates {field}")


def validate_tasks(root: Path) -> None:
    root_build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    storage_build = (root / "nereus-storage-object/build.gradle.kts").read_text(encoding="utf-8")
    oxia_build = (root / "nereus-metadata-oxia/build.gradle.kts").read_text(encoding="utf-8")
    if re.search(r'tasks\.register\("v2M5RetentionCoreCheck"\)', root_build) is None:
        raise RetentionCoreError("root build lacks v2M5RetentionCoreCheck")
    if 'tasks.register<Test>("v2M5RetentionCoreTest")' not in storage_build:
        raise RetentionCoreError("storage-object build lacks v2M5RetentionCoreTest")
    if 'tasks.register<Test>("v2M5RetentionOxiaCapabilityTest")' not in oxia_build:
        raise RetentionCoreError("metadata-oxia build lacks v2M5RetentionOxiaCapabilityTest")
    require_literals(root_build, ("v2M5KafkaCompactionCheck", "v2M5RetentionCoreSourceCheck"), "M5-C aggregate")
    if re.search(r'tasks\.register(?:<[^>]+>)?\("v2M5RetentionRetirementCheck"\)', root_build):
        raise RetentionCoreError("full M5-C retirement gate exists while the Oxia capability is blocked")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    m5_b = load(root / "scripts/check-v2-m5-kafka-compaction.py", "nereus_m5_b_for_m5_c_core")
    m5_b.validate(root)
    require_ancestor(root)
    validate_required_sources(root)
    validate_runtime_contract(root)
    validate_source_lock(root)
    validate_projection(root)
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-C retention core: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
