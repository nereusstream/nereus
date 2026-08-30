#!/usr/bin/env python3
"""Fail-closed validator for one non-promotable Nereus V2 M3 child receipt.

The generic child format is deliberately smaller than M3 Final.  It can only
record focused evidence, derives its counters from normalized evidence files,
and cannot carry production or scenario-promotion authority.  The W1 child is
the already-closed current-source M2 regression format and is delegated to its
own validator instead of being weakened into the generic format.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import struct
import subprocess
import sys
from typing import Any
import xml.etree.ElementTree as ET


SCRIPT_DIR = Path(__file__).resolve().parent


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load receipt contract: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


FINAL = _load("check_v2_m3_final_for_child", SCRIPT_DIR / "check-v2-m3-final.py")
M2 = _load("check_v2_m3_m2_for_child", SCRIPT_DIR / "check-v2-m3-m2-regression.py")
ALLOCATOR_V5 = _load(
    "check_v2_m3_allocator_v5_for_child", SCRIPT_DIR / "check-v2-m3-allocator-v5.py"
)

SCHEMA = "NEREUS_V2_M3_CHILD_EVIDENCE_V1"
JUNIT_SCHEMA = "NEREUS_V2_M3_GOVERNED_JUNIT_EXECUTION_RECEIPT_V1"
TYPED_SCHEMA = "NEREUS_V2_M3_NORMALIZED_TYPED_EVIDENCE_V1"
CHILD_PREFIX = PurePosixPath("docs/v2/evidence/v2-m3/children")
MAX_CANONICAL_BYTES = 131_072
MUTATION_MANIFEST_MAX_BYTES = 1_048_576
RECOVERY_MANIFEST_MAX_BYTES = 65_536
PROTOCOL_FIXTURE_MAX_BYTES = 65_536
MAX_SINGLE_ATTACHMENT_BYTES = FINAL.MAX_SINGLE_EVIDENCE_BYTES
MAX_TOTAL_ATTACHMENT_BYTES = FINAL.MAX_TOTAL_EVIDENCE_BYTES
MAX_SAFE_INTEGER = FINAL.MAX_SAFE_INTEGER
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
SOURCE_IDENTITY_PATTERN = re.compile(r"[ -~]{8,512}")

CHILD_RESULTS = dict(FINAL.CHILD_RESULTS)
CHILD_KINDS = tuple(FINAL.CHILD_KINDS)
GENERIC_CHILD_KINDS = tuple(kind for kind in CHILD_KINDS if kind != "W1_CURRENT_SOURCE_M2_REGRESSION")
ATTACHMENT_KINDS = set(FINAL.ATTACHMENT_KINDS)

BASE_EXCLUSIONS = ("M3_FINAL_AGGREGATE", "SCENARIO_PROMOTION")
EXCLUSIONS_BY_KIND = {
    kind: list(BASE_EXCLUSIONS) for kind in GENERIC_CHILD_KINDS
}
EXCLUSIONS_BY_KIND["D_LOCAL_CAP"] = [
    "M3_FINAL_AGGREGATE",
    "REAL_KMS",
    "REAL_PROVIDER",
    "SCENARIO_PROMOTION",
]
EXCLUSIONS_BY_KIND["C2_SEGMENTED_PREFIX"] = [
    "C1_EVIDENCE_SUBSTITUTE",
    "M3_FINAL_AGGREGATE",
    "PRODUCTION_ALLOWLIST",
    "SCENARIO_PROMOTION",
]

REQUIRED_ATTACHMENTS = {
    kind: {"JUNIT_SUMMARY"} | set(FINAL.REQUIRED_TYPED_ATTACHMENTS.get(kind, set()))
    for kind in GENERIC_CHILD_KINDS
}

NORMALIZED_TYPED_KINDS = {
    "PROVIDER_REAL_RECEIPT",
    "KMS_REAL_RECEIPT",
    "NATIVE_RESULT",
    "ALLOCATOR_FAULT_SUMMARY",
    "ALLOCATOR_NATIVE_RELATIVE_SUMMARY",
    "ALLOCATOR_SCALE_10000_SUMMARY",
    "ALLOCATOR_SCALE_100000_SUMMARY",
}
ALLOCATOR_DERIVED_KINDS = {
    "ALLOCATOR_FAULT_SUMMARY",
    "ALLOCATOR_NATIVE_RELATIVE_SUMMARY",
    "ALLOCATOR_SCALE_10000_SUMMARY",
    "ALLOCATOR_SCALE_100000_SUMMARY",
}
ALLOCATOR_V1_AUTHORITY_ATTACHMENTS = set(FINAL.ALLOCATOR_V1_AUTHORITY_ATTACHMENTS)
ALLOCATOR_V2_AUTHORITY_ATTACHMENTS = set(FINAL.ALLOCATOR_V2_AUTHORITY_ATTACHMENTS)
ALLOCATOR_V5_AUTHORITY_ATTACHMENTS = set(FINAL.ALLOCATOR_V5_AUTHORITY_ATTACHMENTS)
REAL_RECEIPT_KINDS = {"PROVIDER_REAL_RECEIPT", "KMS_REAL_RECEIPT"}

REAL_PROVIDER_BACKENDS = {
    "AWS_S3",
    "AZURE_BLOB_STORAGE",
    "GCS_OBJECT_STORAGE",
    "MINIO_S3_COMPATIBLE",
}
REAL_KMS_BACKENDS = {"AWS_KMS", "AZURE_KEY_VAULT", "GCP_CLOUD_KMS", "VAULT_TRANSIT"}
SOURCE_BINDING_SCHEMA = "NEREUS_V2_M3_EVIDENCE_SOURCE_LOCKS_V2"
SOURCE_LOCK_ALLOCATOR_MODES = {*FINAL.ALLOCATOR_MODES, "UNSELECTED"}
M3_EXTERNAL_EVIDENCE_BRANCH = "nereus/v2-m3-object-wal-evidence"
EXTERNAL_ARTIFACT_IDENTITY = re.compile(
    r"(?:AWS_S3|AZURE_BLOB_STORAGE|GCS_OBJECT_STORAGE|MINIO_S3_COMPATIBLE|"
    r"AWS_KMS|AZURE_KEY_VAULT|GCP_CLOUD_KMS|VAULT_TRANSIT)"
    r"\|artifactReference=[^|]{8,320}@sha256:[0-9a-f]{64}"
    r"\|artifactConfigDigest=sha256:[0-9a-f]{64}"
)
MUTATION_PATHS = {
    "ROUTINE_RANGE_READ",
    "FULL_BODY_RECONCILIATION",
    "OPEN_RUN_RECOVERY",
}
EXTERNAL_CALL_KINDS = {
    "ROOT_AUTHORITY_READ",
    "METADATA_READ",
    "METADATA_CONDITIONAL_MUTATION",
    "KMS_WRAP",
    "KMS_UNWRAP",
    "OBJECT_CONDITIONAL_PUT",
    "OBJECT_HEAD",
    "OBJECT_FULL_GET",
    "OBJECT_PREFIX_RANGE_GET",
    "OBJECT_FRAME_RANGE_GET",
    "OBJECT_LIST_PAGE",
}
MUTATION_EXPECTED_COUNTS = {
    "componentKinds": 16,
    "deepMutationRoots": 50,
    "externalFixtures": 2,
    "goldenRows": 114,
    "mutationOperations": 10,
    "mutationPathExecutions": 240,
    "mutationRecords": 84,
    "positiveVectors": 6,
    "rejectionCodes": 25,
    "resignOperations": 8,
    "validationStages": 16,
}
RECOVERY_MANIFEST_HEADER = ("recordId", "component", "testClass", "testMethod", "claim")
RECOVERY_MANIFEST_ROWS = (
    (
        "R01_CONTROL_WIRE",
        "CONTROL_WIRE",
        "com.nereusstream.storage.object.control.WalRunControlCodecTest",
        "rootPointerSealAndCheckpointRecordsRoundTripCanonically",
        "strict canonical Root Pointer Seal checkpoint round trip",
    ),
    (
        "R02_LAZY_LANES",
        "LAZY_LANES",
        "com.nereusstream.storage.object.control.WalRunRuntimeTest",
        "lanesInstantiateLazilyAndResolveIndependently",
        "three lazy lanes remain independently resolved",
    ),
    (
        "R03_CHECKPOINT_PUBLISH",
        "CHECKPOINT_PUBLISH",
        "com.nereusstream.storage.object.control.WalCheckpointPublisherTest",
        "takeoverPreservesCommittedHeadAndStaleEpochCannotRegress",
        "checkpoint takeover preserves the committed Head",
    ),
    (
        "R04_CHECKPOINT_RECOVERY",
        "CHECKPOINT_RECOVERY",
        "com.nereusstream.storage.object.control.WalCheckpointChainVerifierTest",
        "streamingRecoveryWalksBackFromHeadAndChargesTheRootOwnedBudget",
        "streaming chain recovery charges the Root budget",
    ),
    (
        "R05_SEAL_SUCCESSOR",
        "SEAL_SUCCESSOR",
        "com.nereusstream.storage.object.control.WalRunLifecycleManagerTest",
        "sealedPointerCrashNeedsCandidateThenAdvancesOrAdoptsOnlyExactLineage",
        "sealed pointer recovery accepts only exact successor lineage",
    ),
    (
        "R06_LINEAGE_RECOVERY",
        "LINEAGE_RECOVERY",
        "com.nereusstream.storage.object.recovery.WalRunLineageRecoveryTest",
        "exactLineageWalkValidatesRootAndPredecessorSeal",
        "bounded lineage walk verifies every Root and predecessor Seal",
    ),
    (
        "R07_BOUNDED_TAIL",
        "BOUNDED_TAIL",
        "com.nereusstream.storage.object.recovery.BoundedObjectTailRecoveryTest",
        "productionRootBoundInventoryParsesExactLeafAndRejectsRuntimeExpansion",
        "Root bound inventory rejects runtime expansion",
    ),
    (
        "R08_SESSION_LIFECYCLE",
        "SESSION_LIFECYCLE",
        "com.nereusstream.storage.object.control.WalRunObjectSessionTest",
        "ownsProviderAndKmsLifecycleAndErasesRunKeysOnClose",
        "Provider and KMS sessions close with run key erasure",
    ),
)
PROTOCOL_FIXTURE_HEADER = ("artifactId", "state", "key", "length", "sha256", "hex")
PROTOCOL_FIXTURE_IDS = (
    ("NWKCP1_OBJECT", "IMMUTABLE"),
    ("KAFKA_PROTOCOL_CHECKPOINT_HEAD_OPEN", "OPEN"),
    ("KAFKA_PROTOCOL_CHECKPOINT_HEAD_TERMINAL", "TERMINAL"),
)
PROTOCOL_FIXTURE_ROOT = "cells/01/shards/0007/runs/0000000000000000001"
NWKCP1_MAGIC = bytes.fromhex("4e574b4350310001")
NWKCP1_HEAD_MAGIC = bytes.fromhex("4e574b4831000001")

NATIVE_RESULT_SCHEMA = "nereus-v2-m3-native-result-v1"
NATIVE_RESULT_MAX_BYTES = 256 * 1024
NATIVE_RESULT_EXCLUSIONS = ["M6_NATIVE_BROKER_CONTROLLER_ACTIVATION"]
SEALED_NATIVE_EXECUTION_SCHEMA = "NEREUS_V2_M3_SEALED_NATIVE_EXECUTION_RECEIPT_V1"
MAX_NATIVE_JUNIT_XML_BYTES = 4 * 1024 * 1024
SEALED_NATIVE_MAX_BYTES = 16 * 1024 * 1024
SEALED_JUNIT_MAX_BYTES = MAX_SINGLE_ATTACHMENT_BYTES
MAX_GOVERNED_JUNIT_FILES = 512
LOCAL_CAP_SCHEMA = "NEREUS_V2_M3_D1_LOCAL_CAP_RESULT_V1"
LOCAL_CAP_RESULT = "PASS_LOCAL_FORMAT_CAP_CONFORMANCE_ONLY"
LOCAL_CAP_HARNESS_SOURCE = (
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/"
    "ObjectWalLocalCapacityHarnessV1.java"
)
LOCAL_CAP_HARNESS_TEST_SOURCE = (
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/extent/"
    "CheckedExtentAccountingTest.java"
)
LOCAL_CAP_RECORDS = (
    (
        "NWG1_CAP_LOCAL_FORMULA_V1",
        "NWG1 v1 format-hard cap formula and Cartesian non-closure",
        "exactBoundaryChecks=15;cartesianNonClosureChecks=1",
        "nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/ObjectWalFormatCaps.java",
    ),
    (
        "NWG1_CAP_LOCAL_PARSER_V1",
        "lengths-first bounded NWG1 local parser inputs",
        "truncatedInputRejects=2;validationStage=HEADER_GRAMMAR",
        "nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ObjectReaderV1.java",
    ),
    (
        "NWG1_CAP_LOCAL_CHECKED_ARITHMETIC_V1",
        "checked count offset range and narrowing arithmetic",
        "rejectionChecks=17;checkedToIntChecks=1",
        "nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/CheckedExtentAccounting.java",
    ),
    (
        "NWG1_CAP_LOCAL_KMS_ENVELOPE_V1",
        "bounded canonical KMS envelope round trip and oversize rejection",
        "canonicalRoundTrips=1;oversizeRejections=1",
        "nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1EnvelopeV1.java",
    ),
    (
        "NWG1_CAP_LOCAL_ZSTD_V1",
        "bounded ZSTD semantic round trip without exact compressor-output authority",
        "semanticRoundTrips=1;productionExactOutputClaims=0",
        "nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ZstdV1.java",
    ),
    (
        "NWG1_CAP_LOCAL_STREAMING_COUNTER_V1",
        "allocation-free analytical counters only; no real Provider transfer",
        "analytical4GiBCounters=2;realProviderTransfers=0;streamingCounterChecks=4",
        "nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/CheckedStreamingCounter.java",
    ),
)
GOVERNED_JUNIT_REQUIRED_TESTS = {
    "AB_NWG1_WIRE": {
        (
            "com.nereusstream.storage.object.nwg1.Nwg1WireGoldenV1Test",
            "exactCorpusHasClosedCountsAndRoundTrips",
        ),
        (
            "com.nereusstream.storage.object.nwg1.Nwg1ManifestAndMutationV1Test",
            "allEightyFourRecordsAreExplicitAndExecuteTwoHundredFortyPaths",
        ),
    },
    "C_OBJECT_WAL_STATE_TRACE": {
        (
            "com.nereusstream.storage.object.wal.ObjectWalStateKernelV1Test",
            "replaysAllFiftyAuthoredExpectationsExactly",
        ),
        (
            "com.nereusstream.storage.object.wal.ObjectWalStateTraceManifestV1Test",
            "coversExactTwentyOneOutcomesAndCallProfiles",
        ),
    },
    "D_LOCAL_CAP": {
        ("com.nereusstream.storage.object.extent.CheckedExtentAccountingTest", method)
        for method in (
            "localFormulaRecordExercisesExactAndCartesianBoundaries",
            "localParserRecordExercisesLengthsFirstEnvelopeParser",
            "localCheckedArithmeticRecordExercisesOverflowAndNarrowing",
            "localKmsEnvelopeRecordExercisesRoundTripAndOversizeRejection",
            "localZstdRecordExercisesSemanticRoundTripWithoutExactOutputClaim",
            "localStreamingCounterRecordIsAnalyticalAndClaimsNoProviderTransfer",
        )
    },
    "C1_REAL_PROVIDER_KMS": {
        (
            "com.nereusstream.storage.object.provider.C1ObjectProviderSessionTest",
            "responseUnknownRequiresExactFullReadOrStrongCompleteAbsence",
        ),
        (
            "com.nereusstream.storage.object.kms.KmsCellSessionTest",
            "recoveryUnwrapsOnceCachesByRunAndErasesOnClose",
        ),
    },
    "C2_SEGMENTED_PREFIX": {
        (
            "com.nereusstream.storage.object.provider.SegmentedObjectLayoutC2Test",
            "candidateIsImplementedButAlwaysNonPromotable",
        )
    },
    "R_CONTROL_RECOVERY": {
        (
            "com.nereusstream.storage.object.recovery.WalRunRecoveryManifestV1Test",
            "closedInventoryBindsExactControlAndRecoveryCases",
        ),
        (
            "com.nereusstream.storage.object.control.WalRunControlCodecTest",
            "rootPointerSealAndCheckpointRecordsRoundTripCanonically",
        ),
        (
            "com.nereusstream.storage.object.control.WalRunRuntimeTest",
            "lanesInstantiateLazilyAndResolveIndependently",
        ),
        (
            "com.nereusstream.storage.object.control.WalCheckpointPublisherTest",
            "takeoverPreservesCommittedHeadAndStaleEpochCannotRegress",
        ),
        (
            "com.nereusstream.storage.object.control.WalCheckpointChainVerifierTest",
            "streamingRecoveryWalksBackFromHeadAndChargesTheRootOwnedBudget",
        ),
        (
            "com.nereusstream.storage.object.control.WalRunLifecycleManagerTest",
            "sealedPointerCrashNeedsCandidateThenAdvancesOrAdoptsOnlyExactLineage",
        ),
        (
            "com.nereusstream.storage.object.recovery.BoundedObjectTailRecoveryTest",
            "productionRootBoundInventoryParsesExactLeafAndRejectsRuntimeExpansion",
        ),
        (
            "com.nereusstream.storage.object.recovery.WalRunLineageRecoveryTest",
            "exactLineageWalkValidatesRootAndPredecessorSeal",
        ),
        (
            "com.nereusstream.storage.object.control.WalRunObjectSessionTest",
            "ownsProviderAndKmsLifecycleAndErasesRunKeysOnClose",
        ),
    },
    "K_NWKCP1": {
        (
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1ProtocolFixtureV1Test",
            "exactProtocolFixtureMatchesProductionCodecAndStrictRoundTrips",
        ),
        (
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1CodecV1Test",
            "roundTripsStrictWireKeyAndHead",
        ),
        (
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.ObjectKafkaProtocolCheckpointStoreV1Test",
            "convergesResponseLossAdvancesOrdinalAndTakesOver",
        ),
        (
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.StorageObjectNwkcp1BackendV1Test",
            "terminalHeadRequiresExactPhysicalClosureAndFencesFurtherPublicationAndTakeover",
        ),
        (
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.StorageObjectNwkcp1BackendV1Test",
            "underboundStreamingCheckpointBudgetFailsBeforeAnyPageMetadataRead",
        ),
    },
    "ALLOCATOR_SELECTION": {
        (
            "com.nereusstream.metadata.oxia.v2.allocator.OxiaVirtualLedgerAllocatorStoreTest",
            "reservationCasResponseLossConvergesAppliedExactWithoutSecondGrant",
        )
    },
}
def _governed_junit_source_path(test_class: str) -> str:
    if test_class.startswith("com.nereusstream.metadata.oxia."):
        root = "nereus-metadata-oxia/src/test/java/"
    elif test_class.startswith("com.nereusstream.kafka.bookkeeper."):
        root = "nereus-kafka-bookkeeper/src/test/java/"
    else:
        root = "nereus-storage-object/src/test/java/"
    return root + test_class.replace(".", "/") + ".java"


GOVERNED_JUNIT_SOURCE_PATHS = {
    test_class: _governed_junit_source_path(test_class)
    for required in GOVERNED_JUNIT_REQUIRED_TESTS.values()
    for test_class, _ in required
}
SEALED_ALLOCATOR_VERIFICATION_SCHEMA = "NEREUS_V2_M3_GOVERNED_ALLOCATOR_VERIFICATION_V1"
JAVA_ALLOCATOR_VERIFICATION_SCHEMA = "NEREUS_V2_M3_ALLOCATOR_SEALED_VERIFICATION_V1"
JAVA_ALLOCATOR_RAW_SCHEMA = "NEREUS_V2_M3_ALLOCATOR_RAW_RECOMPUTATION_V1"
ALLOCATOR_VERIFICATION_SELF_HASH_RULE = "SHA256_OF_EXACT_UTF8_WITH_SELF_SHA256_64_ZERO_HEX"
ALLOCATOR_VERIFIER_TEST_CLASS = (
    "com.nereusstream.metadata.oxia.v2.allocator.evidence."
    "M3AllocatorRawEvidenceVerificationTest"
)
ALLOCATOR_VERIFIER_TEST_CASE = "recomputesNarsNaeaJunitAndExactSourceArtifacts()"
ALLOCATOR_VERIFICATION_MAX_BYTES = 2 * 1024 * 1024
ALLOCATOR_V2_VERIFICATION_SCHEMA = (
    "NEREUS_V2_M3_GOVERNED_ALLOCATOR_CAMPAIGN_VERIFICATION_V2"
)
ALLOCATOR_V2_PROMOTION_SCHEMA = "NEREUS_V2_M3_ALLOCATOR_PROMOTION_DECISION_V2"
ALLOCATOR_V2_VERIFICATION_MAX_BYTES = 48 * 1024 * 1024
ALLOCATOR_V2_CHECKPOINT_MAX_BYTES = 2 * 1024 * 1024
ALLOCATOR_V2_JUNIT_MAX_BYTES = 16 * 1024 * 1024
ALLOCATOR_V2_EXTERNAL_MAX_BYTES = 16 * 1024 * 1024
ALLOCATOR_V2_MAX_EXTERNAL_ATTACHMENTS = 328
ALLOCATOR_V2_NAEV_BYTES = 284
ALLOCATOR_V2_NADV_BYTES = 212
ALLOCATOR_V2_CANDIDATES = tuple(range(6))
ALLOCATOR_V2_POPULATIONS = (10_000, 100_000)
ALLOCATOR_V2_LATENCIES_MILLIS = (1, 5, 10, 25)
ALLOCATOR_V2_DESCENDING_RATES = (1000, 750, 500, 333, 250, 200)
ALLOCATOR_V2_LONG_MAX = (1 << 63) - 1
ALLOCATOR_V2_DIAGNOSTIC_TESTS = {
    "strictWorkflowUsesRealOxia()",
    "installedRangeReusesGrant()",
    "rangeRenewalUsesCellCas()",
    "conflictStormUsesFourIndependentCoordinators()",
}
ALLOCATOR_V5_VERIFICATION_SCHEMA = (
    "NEREUS_V2_M3_GOVERNED_ALLOCATOR_CAMPAIGN_VERIFICATION_V5"
)
ALLOCATOR_V5_PROMOTION_SCHEMA = "NEREUS_V2_M3_ALLOCATOR_PROMOTION_DECISION_V5"
ALLOCATOR_V5_VERIFICATION_MAX_BYTES = 64 * 1024 * 1024
ALLOCATOR_V5_EXTERNAL_MAX_BYTES = 32 * 1024 * 1024
ALLOCATOR_NAEA_HEADER_BYTES = 360
ALLOCATOR_NAEA_MAX_BYTES = (8 << 30) + ALLOCATOR_NAEA_HEADER_BYTES
ALLOCATOR_SOURCE_ARTIFACT_MAX_BYTES = 2 << 30
ALLOCATOR_FORMAL_PREFIX = PurePosixPath(
    "nereus-metadata-oxia/build/m3-allocator-evidence/formal"
)
ALLOCATOR_VERIFIER_JUNIT_PATH = PurePosixPath(
    "nereus-metadata-oxia/build/test-results/realAllocatorRawVerificationTest/"
    f"TEST-{ALLOCATOR_VERIFIER_TEST_CLASS}.xml"
)
ALLOCATOR_EXTERNAL_NAMES = (
    "selection.nars",
    "test.naea",
    "native.naea",
    "fault.naea",
    "scale-10000.naea",
    "scale-100000.naea",
    "executorManifest",
    "oxiaClientJar",
    "runtimeDomainArtifact",
    "runtimeMetadataOxiaArtifact",
    "runtimeMetadataSpiArtifact",
    "sourceLocks",
    "testedEvidenceArtifact",
)


def allocator_authority_profile(kinds: set[str]) -> str:
    v1 = kinds.intersection(ALLOCATOR_V1_AUTHORITY_ATTACHMENTS)
    v2 = kinds.intersection(ALLOCATOR_V2_AUTHORITY_ATTACHMENTS)
    v5 = kinds.intersection(ALLOCATOR_V5_AUTHORITY_ATTACHMENTS)
    supplied = sum(bool(profile) for profile in (v1, v2, v5))
    if supplied != 1:
        raise ChildError("allocator child lacks exactly one complete V1, V2, or V5 authority profile")
    for name, actual, expected in (
        ("V1", v1, ALLOCATOR_V1_AUTHORITY_ATTACHMENTS),
        ("V2", v2, ALLOCATOR_V2_AUTHORITY_ATTACHMENTS),
        ("V5", v5, ALLOCATOR_V5_AUTHORITY_ATTACHMENTS),
    ):
        if actual:
            if actual != expected:
                raise ChildError(f"allocator child incompletely supplies the {name} authority profile")
            return name
    raise AssertionError("allocator authority profile did not fail closed")
PULSAR_NATIVE_COMMAND = (
    "./gradlew :nereus-pulsar-offload:test :nereus-pulsar-offload:spotlessCheck "
    ":nereus-pulsar-offload:checkstyleMain :nereus-pulsar-offload:checkstyleTest"
)
PULSAR_NATIVE_SUITES = (
    "com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1Test",
    "com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1Test",
)
PULSAR_NATIVE_REQUIRED_TESTS = tuple(sorted({
    f"{PULSAR_NATIVE_SUITES[1]}#opensFirstLedgerAndPublishesExplicitSuccessorLink",
    f"{PULSAR_NATIVE_SUITES[1]}#rejectsAllocatorValueOutsideTheImmutableSlice",
    f"{PULSAR_NATIVE_SUITES[1]}#failsClosedWhenAllocatorExhaustsInsteadOfSearchingAnotherSlice",
    f"{PULSAR_NATIVE_SUITES[0]}#successorAllocationExhaustionLeavesTheCurrentHeadUnchanged",
    f"{PULSAR_NATIVE_SUITES[0]}#definitiveAbsenceStopsAdmissionThenResumeUsesTheSameEntry",
    f"{PULSAR_NATIVE_SUITES[0]}#exceptionalPublishRetainsTheExactPositionAndTicketForSameEntryRecovery",
    f"{PULSAR_NATIVE_SUITES[0]}#exceptionalReconcileRetainsUnknownPositionAndTicketForSameEntryRecovery",
    f"{PULSAR_NATIVE_SUITES[0]}#definitivelyAbsentSingleEntryCanSealAndMoveToSuccessorEntryZero",
    f"{PULSAR_NATIVE_SUITES[0]}#successorLocalSealFailureRetainsExactEntryZeroForOldPlanRetry",
    f"{PULSAR_NATIVE_SUITES[0]}#sharedDefinitiveAbsenceCannotPartiallyRolloverHeads",
    f"{PULSAR_NATIVE_SUITES[0]}#bindingLocalAppendFailureRetainsOnlyItsTypedGapWhileSiblingPublishesAndContinues",
    f"{PULSAR_NATIVE_SUITES[0]}#combinedTrackerAndLocatorReservationFailsBeforeAnySharedPositionOrTicket",
    f"{PULSAR_NATIVE_SUITES[0]}#checkedTicketCounterCrossesSignedBoundaryAndUsesUnsignedMaxOnce"
    "ThenFailsBeforeAnotherPosition",
    f"{PULSAR_NATIVE_SUITES[0]}#durableTakeoverFenceDiscardsOldOwnerReservationsAndTickets",
    f"{PULSAR_NATIVE_SUITES[0]}#localSealFailureReleasesTheExactPostPositionTicketBeforeProviderDispatch",
    f"{PULSAR_NATIVE_SUITES[0]}#everyCancellationUnknownRetryAndCompletionReleaseRequiresExactTicketOwnership",
    f"{PULSAR_NATIVE_SUITES[0]}#staleTicketCannotReleaseAReusedRingSlot",
    f"{PULSAR_NATIVE_SUITES[0]}#boundedRecoveryValidatesAdjacencyThenReconstructsFreshTickets",
    f"{PULSAR_NATIVE_SUITES[0]}#failedRecoveryAuthenticationNeverActivatesBindingOrPublishesFrontier",
    f"{PULSAR_NATIVE_SUITES[0]}#sharedFailedPlanTakeoverMustFenceAndDiscardEverySiblingAtomically",
    f"{PULSAR_NATIVE_SUITES[0]}#publishesSharedExtentWithExactPositionsAndIndependentBindingFrontiers",
    f"{PULSAR_NATIVE_SUITES[0]}#installsActiveTailBeforeAckAndReadsEachSharedMemberWithoutCrossBindingPoisoning",
    f"{PULSAR_NATIVE_SUITES[0]}#perMemberFailureCannotMasqueradeAsSharedOrPreBindingValidation",
    f"{PULSAR_NATIVE_SUITES[0]}#manifestHandoffWaitsForActiveReadPinsBeforeReleasingCoveredLocator",
    f"{PULSAR_NATIVE_SUITES[0]}#manifestAuthorityMismatchRetainsEveryActiveLocator",
    f"{PULSAR_NATIVE_SUITES[0]}#recoveredManifestCoverageIsAuthorityVerifiedBeforeActivationAndRead",
    f"{PULSAR_NATIVE_SUITES[0]}#exceptionalManifestVerificationCannotPublishCoverageOrReleaseLocator",
    f"{PULSAR_NATIVE_SUITES[1]}#reconcilesUnknownMutationOnlyByExactReread",
    f"{PULSAR_NATIVE_SUITES[1]}#rejectsUnknownMutationWhoseRereadIsAnotherWinner",
    f"{PULSAR_NATIVE_SUITES[1]}#redrivesTheSameCandidateWhenUnknownRereadStillShowsExactPredecessor",
    f"{PULSAR_NATIVE_SUITES[1]}#productionNvAdapterRejectsEvidenceOnlyCoordinatorAsRuntimeAuthority",
    f"{PULSAR_NATIVE_SUITES[0]}#productionObjectWalAdapterOwnsCommonSessionAndExposesNoPlaintextKeyOrListBudget",
    f"{PULSAR_NATIVE_SUITES[0]}#pulsarOwnerFenceCoversRecoveryAndRejectsOldOwnerLatePut",
    f"{PULSAR_NATIVE_SUITES[0]}#pulsarOwnerFenceRejectsMonotonicRollbackBeforeRecoveryCallback",
    f"{PULSAR_NATIVE_SUITES[0]}#pulsarOwnerFenceReleasesAuthorityAfterRecoveryCallbackFailure",
    f"{PULSAR_NATIVE_SUITES[0]}#unknownDrainRetainsCommonRecoveryAndKmsUntilReconcileThenTerminalClose",
    f"{PULSAR_NATIVE_SUITES[0]}#productionAtomicProjectionIgnoresExistingVersionTokenUnderRootNoneAndPublishesClosure",
    f"{PULSAR_NATIVE_SUITES[0]}#rawNativeResultRoundTripsWithExactSourceJunitCountersAndM6Exclusion",
    f"{PULSAR_NATIVE_SUITES[0]}#rawNativeResultRejectsCallerStatusTrailingBytesAndReceiptHashTampering",
    f"{PULSAR_NATIVE_SUITES[0]}#rawNativeResultRejectsAnyFailureErrorOrSkipCounter",
}))
PULSAR_NATIVE_COUNTERS = {
    "fixedSliceLedgerIds": 1 << 40,
    "completionTicketBits": 64,
    "normalAppendRemoteMetadataOperations": 0,
    "sharedExtentTicketsPerMember": 1,
    "fixedSliceTests": 4,
    "noGapTests": 7,
    "completionTicketTests": 8,
    "perMemberIsolationTests": 4,
    "manifestAuthorityTests": 4,
    "responseLossTests": 3,
    "productionAdapterTests": 7,
    "recoveryAuthorityTests": 2,
    "rawReceiptTests": 3,
}
PULSAR_NATIVE_SOURCE_ROOTS = (
    "nereus-pulsar-offload/build.gradle.kts",
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/objectwal/"
    "PulsarObjectWalBridgeV1.java",
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/objectwal/"
    "PulsarObjectWalNativeResultV1.java",
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/objectwal/"
    "PulsarVirtualLedgerChainControllerV1.java",
    "nereus-pulsar-offload/src/test/java/com/nereusstream/pulsar/offload/objectwal/"
    "PulsarObjectWalBridgeV1Test.java",
    "nereus-pulsar-offload/src/test/java/com/nereusstream/pulsar/offload/objectwal/"
    "PulsarVirtualLedgerChainControllerV1Test.java",
)

KAFKA_NATIVE_COMMAND = (
    "./gradlew :nereus-kafka-bookkeeper:test :nereus-kafka-bookkeeper:spotlessCheck "
    ":nereus-kafka-bookkeeper:checkstyleMain :nereus-kafka-bookkeeper:checkstyleTest"
)
KAFKA_NATIVE_SUITES = (
    "com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1CodecV1Test",
    "com.nereusstream.kafka.bookkeeper.object.nwkcp1.ObjectKafkaProtocolCheckpointStoreV1Test",
    "com.nereusstream.kafka.bookkeeper.object.nwkcp1.StorageObjectNwkcp1BackendV1Test",
    "com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectPublicationBridgeV1Test",
    "com.nereusstream.kafka.bookkeeper.object.evidence.KafkaObjectWalNativeResultV1Test",
)
KAFKA_NATIVE_REQUIRED_TESTS = tuple(sorted({
    f"{KAFKA_NATIVE_SUITES[0]}#roundTripsStrictWireKeyAndHead",
    f"{KAFKA_NATIVE_SUITES[0]}#rejectsHeadOutsideExpectedRootBoundKeyContext",
    f"{KAFKA_NATIVE_SUITES[0]}#rejectsHeaderCorruptionTrailingBytesAndNonCanonicalKeys",
    f"{KAFKA_NATIVE_SUITES[0]}#rejectsNullHeadVectorElementAtConstruction",
    f"{KAFKA_NATIVE_SUITES[2]}#rejectsSyntacticallyValidUnissuedCreatedAndHeadSelectedTokensBeforeProviderIo",
    f"{KAFKA_NATIVE_SUITES[1]}#convergesResponseLossAdvancesOrdinalAndTakesOver",
    f"{KAFKA_NATIVE_SUITES[1]}#missingHeadWithoutAuthenticatedReplayFailsClosed",
    f"{KAFKA_NATIVE_SUITES[1]}#definitiveProviderConflictFailsWithoutUnknownOutcomeReread",
    f"{KAFKA_NATIVE_SUITES[2]}#mapsC1ProviderAndExactControlCasIncludingResponseLoss",
    f"{KAFKA_NATIVE_SUITES[2]}#authenticatedEmptyPhysicalChainProducesBoundedRecoveryStateAndTail",
    f"{KAFKA_NATIVE_SUITES[2]}#authenticatedPhysicalSuffixMergesInterleavedLanesAndCleansExactTempSpools",
    f"{KAFKA_NATIVE_SUITES[2]}#sameLaneNonMonotonicKafkaOffsetsFailAndCleanTempSpools",
    f"{KAFKA_NATIVE_SUITES[2]}#exactRecoveryDiskCapRejectsOverflowAndOneByteOverbound",
    f"{KAFKA_NATIVE_SUITES[2]}#nativeDurableOwnerAuthorityRejectsRegressionEscapeSubstitutionAndReleasesFailure",
    f"{KAFKA_NATIVE_SUITES[2]}#truncatedOrTamperedRecoveryLaneSpoolFailsAndAlwaysCleansFiles",
    f"{KAFKA_NATIVE_SUITES[2]}#terminalHeadRequiresExactPhysicalClosureAndFencesFurtherPublicationAndTakeover",
    f"{KAFKA_NATIVE_SUITES[2]}#unresolvedLaneZeroCandidateDoesNotStopLaneOneAndExactRetryDoesNotRepeatPut",
    f"{KAFKA_NATIVE_SUITES[2]}#unresolvedProviderCandidateForbidsSealAndCreatesNoSealMetadata",
    f"{KAFKA_NATIVE_SUITES[2]}#underboundStreamingCheckpointBudgetFailsBeforeAnyPageMetadataRead",
    f"{KAFKA_NATIVE_SUITES[2]}#publicationReadFailureRetriesFromProviderExactWithoutSecondPut",
    f"{KAFKA_NATIVE_SUITES[2]}#freshSessionCannotReplayAndPerformsNoMetadataOrProviderIo",
    f"{KAFKA_NATIVE_SUITES[2]}#emptyTerminalClosureUsesRootBoundPublisherAndObjectSession",
    f"{KAFKA_NATIVE_SUITES[3]}#repositoryRootSelectsLocatorNativeStateAndQueueBeforeAckThenRetainsLocatorBudget",
    f"{KAFKA_NATIVE_SUITES[3]}#ackFailureRetainsRootPublishedTicketAndRetryDoesNotRepublish",
    f"{KAFKA_NATIVE_SUITES[2]}#checkpointIoFailureRetainsDebtButCurrentVerifiedMemberReachesM2Tracker",
    f"{KAFKA_NATIVE_SUITES[3]}#wholeSuffixRollbackUsesRepositoryRootCasThenReleasesExactSuffixTicket",
    f"{KAFKA_NATIVE_SUITES[3]}#issuedWholeSuffixRollbackFencesSequenceUntilExactRootCasCompletes",
    f"{KAFKA_NATIVE_SUITES[3]}#noEffectSequenceClaimFreezesRollbackAndAbortRestoresExactEligibility",
    f"{KAFKA_NATIVE_SUITES[3]}#rollbackAfterSequenceEffectLeavesRootAndExactTicketIntact",
    f"{KAFKA_NATIVE_SUITES[3]}#rollbackValidatesExactTicketCommitPairBeforeRootCas",
    f"{KAFKA_NATIVE_SUITES[3]}#invalidNativeCutFailsBeforeRootCasAndLeavesTicketAndLocatorUnpublished",
    f"{KAFKA_NATIVE_SUITES[3]}#forgedLastStableOffsetCannotExposeAnOpenTransaction",
    f"{KAFKA_NATIVE_SUITES[3]}#locatorReservationUsesTheActualCanonicalLocatorWireCharge",
    f"{KAFKA_NATIVE_SUITES[3]}#retainedLocatorBudgetRequiresManifestBoundRootRetirementAndExactPinDrain",
    f"{KAFKA_NATIVE_SUITES[3]}#takeoverKeepsRootSelectedLocatorBudgetUntilTypedRetirement",
    f"{KAFKA_NATIVE_SUITES[3]}#sharedPhysicalAndBindingFailureDomainsRemainIndependent",
    f"{KAFKA_NATIVE_SUITES[2]}#sharedPhysicalObjectUsesOneFullGetAndIsolatesRealMemberFailureFromSibling",
    f"{KAFKA_NATIVE_SUITES[3]}#takeoverRejectsAssignedPositionsAndInvalidatesOnlyUnassignedReservationValues",
    f"{KAFKA_NATIVE_SUITES[4]}#roundTripsClosedRawResultWithExactCountersAndM6Exclusion",
    f"{KAFKA_NATIVE_SUITES[4]}#rejectsCallerStatusTrailingBytesAndSelfHashTampering",
    f"{KAFKA_NATIVE_SUITES[4]}#rejectsFailureErrorSkipAndNonCanonicalInventory",
}))
KAFKA_NATIVE_COUNTERS = {
    "checkpointWireTests": 5,
    "rootBoundRecoveryTests": 17,
    "ackOrderingTests": 3,
    "wholeSuffixRollbackTests": 5,
    "nativeStateIntegrityTests": 2,
    "locatorRetirementTests": 3,
    "sharedIsolationTests": 3,
    "rawReceiptTests": 3,
    "completionTicketBits": 64,
    "m6ActivationClaims": 0,
}
KAFKA_NATIVE_SOURCE_ROOTS = (
    "nereus-kafka-bookkeeper/build.gradle.kts",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object",
    "nereus-kafka-bookkeeper/src/test/java/com/nereusstream/kafka/bookkeeper/object",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/"
    "KafkaCoherentCommitCoordinatorV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
    "KafkaPartitionPublicationCellV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
    "KafkaPartitionPublicationKindV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
    "KafkaPartitionPublicationOutcomeV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
    "KafkaPartitionObjectTailRetirementSlotV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
    "KafkaPartitionSpeculativeRollbackSlotV1.java",
)

NATIVE_PROFILES = {
    "P_PULSAR_OBJECT_WAL": {
        "backend": "PULSAR_NATIVE",
        "command": PULSAR_NATIVE_COMMAND,
        "counters": PULSAR_NATIVE_COUNTERS,
        "junitRoot": "nereus-pulsar-offload/build/test-results/test",
        "requiredTests": PULSAR_NATIVE_REQUIRED_TESTS,
        "sourceArtifactCount": 6,
        "exactNonJavaSources": ("nereus-pulsar-offload/build.gradle.kts",),
        "sourceRoots": PULSAR_NATIVE_SOURCE_ROOTS,
        "suiteTestCounts": (39, 7),
        "suites": PULSAR_NATIVE_SUITES,
    },
    "U_KAFKA_OBJECT_WAL": {
        "backend": "KAFKA_NATIVE",
        "command": KAFKA_NATIVE_COMMAND,
        "counters": KAFKA_NATIVE_COUNTERS,
        "junitRoot": "nereus-kafka-bookkeeper/build/test-results/test",
        "requiredTests": KAFKA_NATIVE_REQUIRED_TESTS,
        "sourceArtifactCount": 52,
        "exactNonJavaSources": ("nereus-kafka-bookkeeper/build.gradle.kts",),
        "sourceRoots": KAFKA_NATIVE_SOURCE_ROOTS,
        "suiteTestCounts": (4, 3, 17, 14, 3),
        "suites": KAFKA_NATIVE_SUITES,
    },
}
SEALED_REAL_EXECUTION_SCHEMA = "NEREUS_V2_M3_SEALED_REAL_EXECUTION_RECEIPT_V1"
REAL_EXECUTION_PROFILES = {
    "PROVIDER_REAL_RECEIPT": (
        "realProviderTest",
        "com.nereusstream.storage.object.s3.MinioC1RealProviderEvidenceTest",
        "provesC1AtTheAdmitted64MiBRootCap()",
    ),
    "KMS_REAL_RECEIPT": (
        "realKmsTest",
        "com.nereusstream.storage.object.vault.VaultTransitRealKmsEvidenceTest",
        "provesRealTransitWrapUnwrapVersionRotationAndLifecycle()",
    ),
}


class ChildError(RuntimeError):
    """A stable fail-closed child receipt rejection."""


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _reject_duplicate_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ChildError(f"duplicate JSON member: {key}")
        value[key] = item
    return value


def _validate_jcs_domain(value: Any, label: str) -> None:
    if value is None or type(value) is bool:
        return
    if type(value) is int:
        if abs(value) > MAX_SAFE_INTEGER:
            raise ChildError(f"integer exceeds the closed JCS safe domain in {label}")
        return
    if isinstance(value, str):
        if not value.isascii():
            raise ChildError(f"non-ASCII string is outside the closed JCS schema in {label}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_jcs_domain(item, f"{label}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key.isascii():
                raise ChildError(f"non-ASCII object key is outside the closed JCS schema in {label}")
            _validate_jcs_domain(item, f"{label}.{key}")
        return
    raise ChildError(f"unsupported JCS value type in {label}: {type(value).__name__}")


def canonical_bytes(value: Any) -> bytes:
    _validate_jcs_domain(value, "root")
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def load_canonical_json(raw: bytes, label: str, maximum: int = MAX_CANONICAL_BYTES) -> dict[str, Any]:
    if not raw or len(raw) > maximum:
        raise ChildError(f"canonical JSON bytes outside cap: {label}")
    try:
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_members)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ChildError(f"cannot parse JSON {label}: {error}") from error
    if not isinstance(value, dict):
        raise ChildError(f"JSON root is not an object: {label}")
    if canonical_bytes(value) != raw:
        raise ChildError(f"JSON is not exact closed-domain JCS: {label}")
    return value


def load_external_json(raw: bytes, label: str, maximum: int = MAX_CANONICAL_BYTES) -> dict[str, Any]:
    if not raw or len(raw) > maximum:
        raise ChildError(f"JSON evidence bytes outside cap: {label}")
    try:
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_members)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ChildError(f"cannot parse JSON {label}: {error}") from error
    if not isinstance(value, dict):
        raise ChildError(f"JSON evidence root is not an object: {label}")
    _validate_jcs_domain(value, label)
    return value


def _exact_members(value: object, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        actual = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise ChildError(f"{label} member set differs: {actual}")
    return value


def _positive(value: object, label: str) -> int:
    if type(value) is not int or value <= 0 or value > MAX_SAFE_INTEGER:
        raise ChildError(f"{label} must be a positive JCS-safe integer")
    return value


def _nonnegative(value: object, label: str) -> int:
    if type(value) is not int or value < 0 or value > MAX_SAFE_INTEGER:
        raise ChildError(f"{label} must be a non-negative JCS-safe integer")
    return value


def _zero(value: object, label: str) -> int:
    if type(value) is not int or value != 0:
        raise ChildError(f"{label} must be zero")
    return value


def safe_relative(value: object, label: str, prefix: PurePosixPath = FINAL.EVIDENCE_PREFIX) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ChildError(f"{label} is not a safe POSIX-relative path")
    parts = value.split("/")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in ("", ".", "..") for part in parts):
        raise ChildError(f"{label} is not a safe POSIX-relative path")
    if not FINAL.is_under(path, prefix):
        raise ChildError(f"{label} is outside {prefix}: {path}")
    return path


def read_safe_file(root: Path, relative: PurePosixPath, maximum: int = MAX_SINGLE_ATTACHMENT_BYTES) -> bytes:
    current = root
    mode = 0
    for part in relative.parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except OSError as error:
            raise ChildError(f"evidence file is missing: {relative}") from error
        if stat.S_ISLNK(mode):
            raise ChildError(f"evidence path has a symlink component: {relative}")
    if not stat.S_ISREG(mode):
        raise ChildError(f"evidence path is not a regular file: {relative}")
    size = current.stat().st_size
    if size <= 0 or size > maximum:
        raise ChildError(f"evidence file bytes outside cap: {relative}")
    return current.read_bytes()


def git(root: Path, *args: str, text: bool = False) -> bytes | str:
    try:
        return subprocess.check_output(
            ["git", "-C", os.fspath(root), *args], stderr=subprocess.STDOUT, text=text
        )
    except subprocess.CalledProcessError as error:
        output = error.output if isinstance(error.output, str) else error.output.decode("utf-8", "replace")
        raise ChildError(f"git {' '.join(args)} failed: {output.strip()}") from error


def ensure_root(root: Path) -> Path:
    try:
        resolved = root.resolve(strict=True)
        top = Path(str(git(resolved, "rev-parse", "--show-toplevel", text=True)).strip()).resolve(strict=True)
    except OSError as error:
        raise ChildError(f"cannot resolve repository root: {root}") from error
    if top != resolved:
        raise ChildError(f"repository root differs from Git top-level: {resolved}")
    return resolved


def current_head(root: Path) -> str:
    head = str(git(root, "rev-parse", "HEAD", text=True)).strip()
    if not COMMIT_PATTERN.fullmatch(head):
        raise ChildError("current HEAD is not a canonical lowercase commit")
    return head


def _source_locks_sha(root: Path, commit: str) -> str:
    if not COMMIT_PATTERN.fullmatch(commit):
        raise ChildError("tested Nereus commit is not canonical lowercase hexadecimal")
    try:
        raw = git(root, "show", f"{commit}:{FINAL.SOURCE_LOCKS_PATH}")
    except ChildError as error:
        raise ChildError("source locks are absent from the exact tested production tree") from error
    assert isinstance(raw, bytes)
    return sha256(raw)


def expected_source_tuple(root: Path, commit: str) -> dict[str, str]:
    return {"nereusCommit": commit, "sourceLocksSha256": _source_locks_sha(root, commit)}


def _source_locks(root: Path, commit: str) -> dict[str, Any]:
    raw = git(root, "show", f"{commit}:{FINAL.SOURCE_LOCKS_PATH}")
    assert isinstance(raw, bytes)
    try:
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_members)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ChildError(f"cannot parse exact tested source locks: {error}") from error
    if not isinstance(value, dict):
        raise ChildError("exact tested source locks root is not an object")
    return value


def _required_object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ChildError(f"source-lock object is absent: {label}")
    return value


def _required_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ChildError(f"source-lock string is absent: {label}")
    return value


def _m3_evidence_branch(value: object, label: str) -> str:
    branch = _required_string(value, label)
    if branch != M3_EXTERNAL_EVIDENCE_BRANCH:
        raise ChildError(f"M3 external evidence branch differs: {label}")
    return branch


def _find_by_id(value: object, expected_id: str, label: str) -> dict[str, Any]:
    if not isinstance(value, list):
        raise ChildError(f"source-lock list is absent: {label}")
    rows = [row for row in value if isinstance(row, dict) and row.get("id") == expected_id]
    if len(rows) != 1:
        raise ChildError(f"source-lock ID is absent or duplicated: {label}/{expected_id}")
    return rows[0]


def _expected_locked_identity(
    locks: dict[str, Any], evidence_kind: str, backend: str, locked_identity: str
) -> str:
    if evidence_kind == "NATIVE_RESULT" and backend == "KAFKA_NATIVE":
        kafka = _required_object(locks.get("m3KafkaNativeBinding"), "m3KafkaNativeBinding")
        return (
            "KAFKA_NATIVE|repository=apache/kafka"
            + "|commit="
            + _required_string(kafka.get("sourceCommit"), "m3KafkaNativeBinding.sourceCommit")
        )
    if evidence_kind == "NATIVE_RESULT":
        pulsar = _required_object(locks.get("m3PulsarNativeBinding"), "m3PulsarNativeBinding")
        return (
            "PULSAR_NATIVE|repository=apache/pulsar"
            + "|commit="
            + _required_string(pulsar.get("sourceCommit"), "m3PulsarNativeBinding.sourceCommit")
        )
    allocator = _required_object(
        locks.get("m3AllocatorEvidenceBinding"), "m3AllocatorEvidenceBinding"
    )
    if evidence_kind == "ALLOCATOR_NATIVE_RELATIVE_SUMMARY":
        return (
            "PULSAR_NATIVE|repository=apache/pulsar"
            + "|commit="
            + _required_string(
                allocator.get("pulsarSourceCommit"),
                "m3AllocatorEvidenceBinding.pulsarSourceCommit",
            )
        )
    if evidence_kind in {
        "ALLOCATOR_FAULT_SUMMARY",
        "ALLOCATOR_SCALE_10000_SUMMARY",
        "ALLOCATOR_SCALE_100000_SUMMARY",
    }:
        return (
            "OXIA_AND_PULSAR|pulsarCommit="
            + _required_string(
                allocator.get("pulsarSourceCommit"),
                "m3AllocatorEvidenceBinding.pulsarSourceCommit",
            )
            + "|oxiaClientCommit="
            + _required_string(
                allocator.get("oxiaClientSourceCommit"),
                "m3AllocatorEvidenceBinding.oxiaClientSourceCommit",
            )
            + "|oxiaClientJarSha256="
            + _required_string(
                allocator.get("oxiaClientJarSha256"),
                "m3AllocatorEvidenceBinding.oxiaClientJarSha256",
            )
            + "|oxiaServerCommit="
            + _required_string(
                allocator.get("oxiaServerSourceCommit"),
                "m3AllocatorEvidenceBinding.oxiaServerSourceCommit",
            )
            + "|oxiaServerImageDigest="
            + _required_string(
                allocator.get("oxiaServerImageDigest"),
                "m3AllocatorEvidenceBinding.oxiaServerImageDigest",
            )
        )
    if evidence_kind in {"PROVIDER_REAL_RECEIPT", "KMS_REAL_RECEIPT"}:
        if (
            not EXTERNAL_ARTIFACT_IDENTITY.fullmatch(locked_identity)
            or not locked_identity.startswith(backend + "|")
        ):
            raise ChildError(f"external source-lock identity is not canonical: {evidence_kind}")
        return locked_identity
    raise ChildError(f"typed evidence kind has no source-lock derivation: {evidence_kind}")


def _expected_locked_provenance(
    locks: dict[str, Any], evidence_kind: str, backend: str, source_identity: str
) -> str:
    if evidence_kind == "NATIVE_RESULT" and backend == "KAFKA_NATIVE":
        kafka = _required_object(locks.get("m3KafkaNativeBinding"), "m3KafkaNativeBinding")
        return (
            "KAFKA_FORK|repository="
            + _required_string(kafka.get("repository"), "m3KafkaNativeBinding.repository")
            + "|branch="
            + _m3_evidence_branch(kafka.get("branch"), "m3KafkaNativeBinding.branch")
            + "|commit="
            + _required_string(kafka.get("sourceCommit"), "m3KafkaNativeBinding.sourceCommit")
            + "|logicalRepository=apache/kafka"
        )
    if evidence_kind == "NATIVE_RESULT":
        pulsar = _required_object(locks.get("m3PulsarNativeBinding"), "m3PulsarNativeBinding")
        return (
            "PULSAR_FORK|repository="
            + _required_string(pulsar.get("repository"), "m3PulsarNativeBinding.repository")
            + "|branch="
            + _m3_evidence_branch(pulsar.get("branch"), "m3PulsarNativeBinding.branch")
            + "|commit="
            + _required_string(pulsar.get("sourceCommit"), "m3PulsarNativeBinding.sourceCommit")
            + "|logicalRepository=apache/pulsar"
        )
    allocator = _required_object(
        locks.get("m3AllocatorEvidenceBinding"), "m3AllocatorEvidenceBinding"
    )
    if evidence_kind == "ALLOCATOR_NATIVE_RELATIVE_SUMMARY":
        return (
            "PULSAR_FORK|repository="
            + _required_string(
                allocator.get("pulsarRepository"),
                "m3AllocatorEvidenceBinding.pulsarRepository",
            )
            + "|branch="
            + _m3_evidence_branch(
                allocator.get("pulsarBranch"), "m3AllocatorEvidenceBinding.pulsarBranch"
            )
            + "|commit="
            + _required_string(
                allocator.get("pulsarSourceCommit"),
                "m3AllocatorEvidenceBinding.pulsarSourceCommit",
            )
            + "|logicalRepository=apache/pulsar"
        )
    if evidence_kind in {
        "ALLOCATOR_FAULT_SUMMARY",
        "ALLOCATOR_SCALE_10000_SUMMARY",
        "ALLOCATOR_SCALE_100000_SUMMARY",
    }:
        return (
            "ALLOCATOR_FORKS|pulsarRepository="
            + _required_string(
                allocator.get("pulsarRepository"),
                "m3AllocatorEvidenceBinding.pulsarRepository",
            )
            + "|pulsarBranch="
            + _m3_evidence_branch(
                allocator.get("pulsarBranch"), "m3AllocatorEvidenceBinding.pulsarBranch"
            )
            + "|pulsarCommit="
            + _required_string(
                allocator.get("pulsarSourceCommit"),
                "m3AllocatorEvidenceBinding.pulsarSourceCommit",
            )
            + "|pulsarLogicalRepository=apache/pulsar|oxiaClientRepository="
            + _required_string(
                allocator.get("oxiaClientRepository"),
                "m3AllocatorEvidenceBinding.oxiaClientRepository",
            )
            + "|oxiaClientBranch="
            + _m3_evidence_branch(
                allocator.get("oxiaClientEvidenceCheckoutBranch"),
                "m3AllocatorEvidenceBinding.oxiaClientEvidenceCheckoutBranch",
            )
            + "|oxiaClientCommit="
            + _required_string(
                allocator.get("oxiaClientSourceCommit"),
                "m3AllocatorEvidenceBinding.oxiaClientSourceCommit",
            )
        )
    return source_identity


def source_bindings(
    root: Path, commit: str, allocator_mode: str | None = None
) -> tuple[str, dict[tuple[str, str], dict[str, str]]]:
    locks = _source_locks(root, commit)
    policy = _exact_members(
        locks.get("m3EvidenceBindings"),
        {"allocatorMode", "bindings", "schema"},
        "sourceLocks.m3EvidenceBindings",
    )
    mode = policy["allocatorMode"]
    if policy["schema"] != SOURCE_BINDING_SCHEMA or mode not in SOURCE_LOCK_ALLOCATOR_MODES:
        raise ChildError("M3 evidence source-lock schema or allocator mode differs")
    if allocator_mode is not None and mode != allocator_mode:
        raise ChildError("Final allocator mode is not derived from exact tested source locks")
    expected_keys = sorted(
        (child_kind, evidence_kind)
        for child_kind, kinds in FINAL.REQUIRED_TYPED_ATTACHMENTS.items()
        for evidence_kind in (
            kinds
            | (
                ALLOCATOR_DERIVED_KINDS
                if child_kind == "ALLOCATOR_SELECTION"
                else set()
            )
        )
        if evidence_kind in NORMALIZED_TYPED_KINDS
    )
    rows = policy["bindings"]
    if not isinstance(rows, list) or len(rows) != len(expected_keys):
        raise ChildError("M3 typed evidence source-lock binding count differs")
    result: dict[tuple[str, str], dict[str, str]] = {}
    for index, (expected_child, expected_kind) in enumerate(expected_keys):
        row = _exact_members(
            rows[index],
            {
                "backend",
                "childKind",
                "evidenceKind",
                "executionClass",
                "sourceIdentity",
                "sourceProvenance",
            },
            f"sourceLocks.m3EvidenceBindings.bindings[{index}]",
        )
        if row["childKind"] != expected_child or row["evidenceKind"] != expected_kind:
            raise ChildError("M3 typed evidence source-lock bindings are not exact sorted inventory")
        backends, execution_class, _ = _expected_typed_profile(expected_kind, expected_child)
        if row["backend"] not in backends or row["executionClass"] != execution_class:
            raise ChildError(f"M3 typed source-lock backend/execution class differs: {expected_kind}")
        if row["sourceIdentity"] != _expected_locked_identity(
            locks, expected_kind, row["backend"], row["sourceIdentity"]
        ):
            raise ChildError(
                f"M3 typed source identity differs from external locked coordinates: {expected_kind}"
            )
        if row["sourceProvenance"] != _expected_locked_provenance(
            locks, expected_kind, row["backend"], row["sourceIdentity"]
        ):
            raise ChildError(
                f"M3 typed source provenance differs from exact fork/artifact coordinates: {expected_kind}"
            )
        result[(expected_child, expected_kind)] = row
    return mode, result


def expected_exclusions(kind: str) -> list[str]:
    if kind not in EXCLUSIONS_BY_KIND:
        raise ChildError(f"generic child kind is outside the closed inventory: {kind}")
    return list(EXCLUSIONS_BY_KIND[kind])


def _validate_summary(value: object, label: str) -> dict[str, int]:
    summary = _exact_members(value, {"errors", "failures", "skipped", "tests"}, label)
    _positive(summary["tests"], f"{label}.tests")
    _zero(summary["failures"], f"{label}.failures")
    _zero(summary["errors"], f"{label}.errors")
    _zero(summary["skipped"], f"{label}.skipped")
    return summary


def _governed_junit_path(value: object, label: str) -> str:
    path = _native_relative(value, label)
    parts = path.parts
    if (
        path.suffix != ".xml"
        or not path.name.startswith("TEST-")
        or not any(
            parts[index : index + 2] == ("build", "test-results")
            for index in range(len(parts) - 1)
        )
    ):
        raise ChildError(f"{label} is not an exact Gradle JUnit XML identity")
    return str(path)


def _validate_governed_junit_xml(
    raw: bytes, receipt_path: str
) -> tuple[dict[str, int], set[tuple[str, str]]]:
    if (
        not raw
        or len(raw) > MAX_NATIVE_JUNIT_XML_BYTES
        or b"<!DOCTYPE" in raw.upper()
        or b"<!ENTITY" in raw.upper()
    ):
        raise ChildError("governed JUnit XML is empty, unsafe, or over cap")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as error:
        raise ChildError(f"cannot parse governed JUnit XML {receipt_path}: {error}") from error
    if root.tag != "testsuite" or not root.attrib.get("name"):
        raise ChildError("governed JUnit XML root/suite identity differs")
    counts: dict[str, int] = {}
    for field in ("tests", "failures", "errors", "skipped"):
        text = root.attrib.get(field, "")
        if not re.fullmatch(r"0|[1-9][0-9]*", text):
            raise ChildError(f"governed JUnit XML {field} is not canonical nonnegative decimal")
        counts[field] = int(text)
    summary = _validate_summary(counts, f"governed JUnit XML {receipt_path}")
    cases = root.findall("testcase")
    if len(cases) != summary["tests"]:
        raise ChildError("governed JUnit XML declared testcase count differs")
    identities: set[tuple[str, str]] = set()
    for case in cases:
        test_name = case.attrib.get("name", "")
        if test_name.endswith("()"):
            test_name = test_name[:-2]
        identity = (case.attrib.get("classname", ""), test_name)
        if (
            not identity[0]
            or not identity[1]
            or identity in identities
            or case.find(".//failure") is not None
            or case.find(".//error") is not None
            or case.find(".//skipped") is not None
        ):
            raise ChildError("governed JUnit XML testcase identity/result is invalid or duplicated")
        identities.add(identity)
    return summary, identities


def _junit_wrapper_unsigned(value: dict[str, Any]) -> dict[str, Any]:
    unsigned = dict(value)
    unsigned["receiptSha256"] = "0" * 64
    return unsigned


def _validate_required_junit_sources(root: Path, tested_commit: str, child_kind: str) -> None:
    for test_class, method in sorted(GOVERNED_JUNIT_REQUIRED_TESTS.get(child_kind, set())):
        source_path = GOVERNED_JUNIT_SOURCE_PATHS[test_class]
        source = git(root, "show", f"{tested_commit}:{source_path}")
        assert isinstance(source, bytes)
        try:
            text = source.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ChildError(f"governed JUnit exact test source is not UTF-8: {source_path}") from error
        method_pattern = re.compile(
            r"@Test\s+(?:(?:public|protected|private|static|final)\s+)*void\s+"
            + re.escape(method)
            + r"\s*\("
        )
        if not method_pattern.search(text):
            raise ChildError(
                "governed JUnit required testcase is absent from exact tested source: "
                f"{test_class}#{method}"
            )


def seal_junit_execution_receipt(
    root: Path,
    junit_xml_inputs: list[tuple[str, bytes]],
    child_kind: str,
    tested_commit: str,
) -> dict[str, Any]:
    if child_kind not in GENERIC_CHILD_KINDS:
        raise ChildError(f"governed JUnit child kind is outside the closed inventory: {child_kind}")
    if not COMMIT_PATTERN.fullmatch(tested_commit):
        raise ChildError("governed JUnit tested commit is not canonical lowercase hexadecimal")
    if not 1 <= len(junit_xml_inputs) <= MAX_GOVERNED_JUNIT_FILES:
        raise ChildError("governed JUnit XML file count is empty or over cap")
    _validate_required_junit_sources(root, tested_commit, child_kind)
    rows: list[dict[str, Any]] = []
    paths: list[str] = []
    digests: list[str] = []
    observed: set[tuple[str, str]] = set()
    for index, (raw_path, raw_xml) in enumerate(junit_xml_inputs):
        path = _governed_junit_path(raw_path, f"governed JUnit XML[{index}].path")
        _, identities = _validate_governed_junit_xml(raw_xml, path)
        if observed.intersection(identities):
            raise ChildError("governed JUnit testcase identity is duplicated across XML files")
        observed.update(identities)
        digest = sha256(raw_xml)
        paths.append(path)
        digests.append(digest)
        rows.append(
            {
                "bytes": len(raw_xml),
                "path": path,
                "sha256": digest,
                "xmlBase64": base64.b64encode(raw_xml).decode("ascii"),
            }
        )
    if paths != sorted(set(paths)) or len(digests) != len(set(digests)):
        raise ChildError("governed JUnit XML paths or byte digests are duplicated/unsorted")
    missing = GOVERNED_JUNIT_REQUIRED_TESTS.get(child_kind, set()) - observed
    if missing:
        raise ChildError(f"governed JUnit XML lacks required child tests: {sorted(missing)}")
    receipt: dict[str, Any] = {
        "childKind": child_kind,
        "junitXml": rows,
        "receiptSha256": "0" * 64,
        "schema": JUNIT_SCHEMA,
        "testedCommit": tested_commit,
    }
    receipt["receiptSha256"] = sha256(canonical_bytes(receipt))
    return receipt


def validate_junit(
    value: object, root: Path, tested_commit: str, child_kind: str
) -> dict[str, int]:
    row = _exact_members(
        value,
        {"childKind", "junitXml", "receiptSha256", "schema", "testedCommit"},
        "governed JUnit execution receipt",
    )
    if (
        row["schema"] != JUNIT_SCHEMA
        or row["testedCommit"] != tested_commit
        or row["childKind"] != child_kind
        or child_kind not in GENERIC_CHILD_KINDS
    ):
        raise ChildError("governed JUnit schema/source/child identity differs")
    _validate_required_junit_sources(root, tested_commit, child_kind)
    _sha_text(row["receiptSha256"], "governed JUnit receipt SHA")
    if row["receiptSha256"] != sha256(canonical_bytes(_junit_wrapper_unsigned(row))):
        raise ChildError("governed JUnit receipt self-hash differs")
    xml_rows = row["junitXml"]
    if not isinstance(xml_rows, list) or not 1 <= len(xml_rows) <= MAX_GOVERNED_JUNIT_FILES:
        raise ChildError("governed JUnit XML inventory is empty or over cap")
    paths: list[str] = []
    digests: list[str] = []
    observed: set[tuple[str, str]] = set()
    summaries: list[dict[str, int]] = []
    for index, raw_xml_row in enumerate(xml_rows):
        xml_row = _exact_members(
            raw_xml_row,
            {"bytes", "path", "sha256", "xmlBase64"},
            f"governed JUnit XML[{index}]",
        )
        path = _governed_junit_path(xml_row["path"], f"governed JUnit XML[{index}].path")
        raw_xml = _decode_sealed_bytes(
            xml_row["xmlBase64"],
            f"governed JUnit XML[{index}]",
            MAX_NATIVE_JUNIT_XML_BYTES,
        )
        digest = sha256(raw_xml)
        if xml_row["bytes"] != len(raw_xml) or xml_row["sha256"] != digest:
            raise ChildError("governed JUnit embedded XML bytes/SHA differ")
        summary, identities = _validate_governed_junit_xml(raw_xml, path)
        if observed.intersection(identities):
            raise ChildError("governed JUnit testcase identity is duplicated across XML files")
        observed.update(identities)
        paths.append(path)
        digests.append(digest)
        summaries.append(summary)
    if paths != sorted(set(paths)) or len(digests) != len(set(digests)):
        raise ChildError("governed JUnit XML paths or byte digests are duplicated/unsorted")
    missing = GOVERNED_JUNIT_REQUIRED_TESTS.get(child_kind, set()) - observed
    if missing:
        raise ChildError(f"governed JUnit XML lacks required child tests: {sorted(missing)}")
    return _sum_summaries(summaries)


def _expected_local_cap_records(root: Path, tested_commit: str) -> list[dict[str, Any]]:
    if not COMMIT_PATTERN.fullmatch(tested_commit):
        raise ChildError("D1 local-cap tested commit is not canonical lowercase hexadecimal")
    records: list[dict[str, Any]] = []
    for name, subject, counter, source_path in LOCAL_CAP_RECORDS:
        source = git(root, "show", f"{tested_commit}:{source_path}")
        assert isinstance(source, bytes)
        if not source:
            raise ChildError(f"D1 local-cap exact source is empty: {source_path}")
        records.append(
            {
                "counter": counter,
                "name": name,
                "sourcePath": source_path,
                "sourceSha256": sha256(source),
                "subject": subject,
            }
        )
    return records


def validate_local_cap_result(
    value: object, root: Path, tested_commit: str
) -> dict[str, int]:
    row = _exact_members(
        value,
        {
            "allocationFreeAnalyticalOnly",
            "childKind",
            "errors",
            "failures",
            "harnessSourceSha256",
            "harnessTestSourceSha256",
            "nereusCommit",
            "promotionEligible",
            "providerTransferClaimed",
            "receiptSha256",
            "records",
            "result",
            "schema",
            "skipped",
            "tests",
        },
        "D1 governed local-cap result",
    )
    summary = _validate_summary(
        {key: row[key] for key in ("errors", "failures", "skipped", "tests")},
        "D1 governed local-cap result",
    )
    receipt_sha = _sha_text(row["receiptSha256"], "D1 local-cap receipt SHA")
    unsigned = dict(row)
    unsigned["receiptSha256"] = "0" * 64
    if receipt_sha != sha256(canonical_bytes(unsigned)):
        raise ChildError("D1 governed local-cap result self-hash differs")
    harness_source = git(root, "show", f"{tested_commit}:{LOCAL_CAP_HARNESS_SOURCE}")
    harness_test_source = git(
        root, "show", f"{tested_commit}:{LOCAL_CAP_HARNESS_TEST_SOURCE}"
    )
    assert isinstance(harness_source, bytes) and isinstance(harness_test_source, bytes)
    if (
        row["schema"] != LOCAL_CAP_SCHEMA
        or row["childKind"] != "D_LOCAL_CAP"
        or row["result"] != LOCAL_CAP_RESULT
        or row["nereusCommit"] != tested_commit
        or row["promotionEligible"] is not False
        or row["allocationFreeAnalyticalOnly"] is not True
        or row["providerTransferClaimed"] is not False
        or row["harnessSourceSha256"] != sha256(harness_source)
        or row["harnessTestSourceSha256"] != sha256(harness_test_source)
        or summary != {"errors": 0, "failures": 0, "skipped": 0, "tests": 6}
        or row["records"] != _expected_local_cap_records(root, tested_commit)
    ):
        raise ChildError(
            "D1 governed local-cap result differs from exact six-record source-bound contract"
        )
    if (
        row["allocationFreeAnalyticalOnly"] is not True
        or row["providerTransferClaimed"] is not False
        or any(
            not record["subject"]
            or not record["counter"]
            or not SHA256_PATTERN.fullmatch(record["sourceSha256"])
            for record in row["records"]
        )
    ):
        raise ChildError("D1 local cap is empty or claims a real Provider transfer")
    return summary


def _expected_typed_profile(kind: str, child_kind: str) -> tuple[set[str], str, int]:
    if kind == "PROVIDER_REAL_RECEIPT":
        return REAL_PROVIDER_BACKENDS, "REAL_EXTERNAL_PROCESS", 0
    if kind == "KMS_REAL_RECEIPT":
        return REAL_KMS_BACKENDS, "REAL_EXTERNAL_PROCESS", 0
    if kind == "NATIVE_RESULT":
        backend = "KAFKA_NATIVE" if child_kind == "U_KAFKA_OBJECT_WAL" else "PULSAR_NATIVE"
        if child_kind not in {"U_KAFKA_OBJECT_WAL", "P_PULSAR_OBJECT_WAL"}:
            raise ChildError(f"NATIVE_RESULT cannot be attached to child {child_kind}")
        return {backend}, "NATIVE_REFERENCE_EXECUTION", 0
    if kind == "ALLOCATOR_NATIVE_RELATIVE_SUMMARY":
        return {"PULSAR_NATIVE"}, "NATIVE_REFERENCE_EXECUTION", 0
    if kind == "ALLOCATOR_FAULT_SUMMARY":
        return {"OXIA_AND_PULSAR"}, "FAULT_INJECTION", 0
    if kind == "ALLOCATOR_SCALE_10000_SUMMARY":
        return {"OXIA_AND_PULSAR"}, "SCALE_EXECUTION", 10_000
    if kind == "ALLOCATOR_SCALE_100000_SUMMARY":
        return {"OXIA_AND_PULSAR"}, "SCALE_EXECUTION", 100_000
    raise ChildError(f"attachment kind has no normalized typed profile: {kind}")


def _summary_from_receipt(value: dict[str, Any], label: str) -> dict[str, int]:
    return _validate_summary(
        {key: value[key] for key in ("errors", "failures", "skipped", "tests")},
        label,
    )


def _validate_provider_raw_receipt(
    value: object, tested_commit: str, binding: dict[str, str]
) -> dict[str, int]:
    row = _exact_members(
        value,
        {
            "absenceListPlusExactGetNotFound",
            "actualFrameRangeBytes",
            "actualPrefixRangeBytes",
            "actualStreamingFullGetSha256Bytes",
            "actualStreamingPutBytes",
            "adapterVersion",
            "c2PromotionEligible",
            "c2Tested",
            "candidateRootAdmissionContractSha256",
            "containerAutoRemove",
            "containerId",
            "coreC1ObjectProviderSession",
            "errors",
            "etagUsedAsContentProof",
            "failures",
            "forcedPaginationObjects",
            "forcedPaginationPageKeys",
            "headCalls",
            "imageConfigDigest",
            "imageReference",
            "inFlightAtTerminal",
            "networkBinding",
            "nereusCommit",
            "promotionEligible",
            "providerProduct",
            "providerProofMode",
            "providerVersion",
            "realProvider",
            "responseUnknownCuts",
            "responseUnknownFaultInjection",
            "result",
            "rootBodyCapBytes",
            "samePrefixImmediateList",
            "schema",
            "skipped",
            "spoolResourcesAtTerminal",
            "strategy",
            "terminalOutcomes",
            "testClass",
            "testMethod",
            "testTask",
            "tests",
            "unexpectedErrors",
            "userMetadataUsedAsContentProof",
        },
        "real Provider receipt",
    )
    identity = (
        f"{binding['backend']}|artifactReference={row['imageReference']}"
        f"|artifactConfigDigest={row['imageConfigDigest']}"
    )
    if (
        binding["childKind"] != "C1_REAL_PROVIDER_KMS"
        or binding["evidenceKind"] != "PROVIDER_REAL_RECEIPT"
        or binding["backend"] != "MINIO_S3_COMPATIBLE"
        or binding["executionClass"] != "REAL_EXTERNAL_PROCESS"
        or binding["sourceIdentity"] != identity
        or row["schema"] != "NEREUS_V2_M3_EXACT_PROVIDER_CAPACITY_EVIDENCE_V1"
        or row["result"] != "PASS_REAL_PROVIDER_C1_ONLY"
        or row["promotionEligible"] is not False
        or row["realProvider"] is not True
        or row["strategy"] != "C1_SINGLE_OBJECT_V1"
        or row["c2Tested"] is not False
        or row["c2PromotionEligible"] is not False
        or row["providerProduct"] != "MinIO"
        or row["networkBinding"] != "127.0.0.1:RANDOM"
        or row["nereusCommit"] != tested_commit
        or not isinstance(row["containerId"], str)
        or not re.fullmatch(r"[0-9a-f]{12,64}", row["containerId"])
        or row["adapterVersion"] != "nereus-s3-c1-v1/aws-sdk-s3-2.47.5"
        or row["coreC1ObjectProviderSession"] is not True
        or row["providerProofMode"] != "NONE"
        or row["containerAutoRemove"] is not True
        or row["samePrefixImmediateList"] is not True
        or row["absenceListPlusExactGetNotFound"] is not True
        or row["etagUsedAsContentProof"] is not False
        or row["userMetadataUsedAsContentProof"] is not False
        or row["responseUnknownFaultInjection"] != "DETERMINISTIC_CLIENT_BOUNDARY"
        or row["rootBodyCapBytes"] != 67_108_864
        or row["actualPrefixRangeBytes"] != 4_194_304
        or row["actualFrameRangeBytes"] != 1_048_576
        or row["forcedPaginationPageKeys"] != 2
        or row["forcedPaginationObjects"] != 5
        or row["terminalOutcomes"]
        != ["APPLIED_EXACT", "EXISTING_EXACT", "DEFINITIVELY_NOT_APPLIED", "DEFINITIVE_CONFLICT"]
        or row["responseUnknownCuts"] != ["PRESENT", "ABSENT", "UNKNOWN"]
        or row["testTask"] != "realProviderTest"
        or row["testClass"]
        != "com.nereusstream.storage.object.s3.MinioC1RealProviderEvidenceTest"
        or row["testMethod"] != "provesC1AtTheAdmitted64MiBRootCap()"
        or row["tests"] != 1
    ):
        raise ChildError("real Provider receipt schema/result/backend/proof boundary differs")
    for counter in (
        "rootBodyCapBytes",
        "actualStreamingPutBytes",
        "actualStreamingFullGetSha256Bytes",
        "actualPrefixRangeBytes",
        "actualFrameRangeBytes",
        "forcedPaginationPageKeys",
        "forcedPaginationObjects",
    ):
        _positive(row[counter], f"real Provider receipt.{counter}")
    for counter in (
        "headCalls",
        "unexpectedErrors",
        "inFlightAtTerminal",
        "spoolResourcesAtTerminal",
    ):
        _zero(row[counter], f"real Provider receipt.{counter}")
    if (
        row["actualStreamingPutBytes"] != row["rootBodyCapBytes"]
        or row["actualStreamingFullGetSha256Bytes"] != row["rootBodyCapBytes"]
    ):
        raise ChildError("real Provider receipt does not exercise the exact root body cap")
    contract = (
        "strategy=C1_SINGLE_OBJECT_V1\n"
        f"provider=minio/{row['providerVersion']}\n"
        f"adapter={row['adapterVersion']}\n"
        f"maxObjectBodyBytes={row['rootBodyCapBytes']}\n"
        f"maxDirectoryPrefixBytes={row['actualPrefixRangeBytes']}\n"
        "maxSingleRangeBytes=67108864\n"
        f"listPageKeys={row['forcedPaginationPageKeys']}\n"
        "providerProofMode=NONE\n"
        "conditionalCreate=true\n"
        "streamingFullGetSha256=true\n"
        "strongListAbsence=true\n"
    )
    if row["candidateRootAdmissionContractSha256"] != sha256(contract.encode("ascii")):
        raise ChildError("real Provider admission contract digest differs from exact receipt fields")
    return _summary_from_receipt(row, "real Provider receipt")


def _sha_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_PATTERN.fullmatch(value):
        raise ChildError(f"{label} is not canonical lowercase SHA-256")
    return value


def _nonnegative_map(value: object, expected_keys: set[str], label: str) -> dict[str, int]:
    row = _exact_members(value, expected_keys, label)
    for key in expected_keys:
        _nonnegative(row[key], f"{label}.{key}")
    return row


def validate_mutation_manifest(value: object) -> None:
    manifest = _exact_members(
        value,
        {
            "artifact",
            "componentInventory",
            "expectedCounts",
            "externalCallProfiles",
            "externalFixtures",
            "mutationOperations",
            "mutations",
            "resignOperations",
            "vectors",
            "zstdFixtures",
        },
        "NWG1 mutation manifest",
    )
    if manifest["artifact"] != "NWG1_GOLDEN_MANIFEST_V1":
        raise ChildError("NWG1 mutation manifest artifact token differs")
    if manifest["expectedCounts"] != MUTATION_EXPECTED_COUNTS:
        raise ChildError("NWG1 mutation manifest expectedCounts differ")

    def exact_string_inventory(name: str, expected_count: int) -> list[str]:
        rows = manifest[name]
        if (
            not isinstance(rows, list)
            or len(rows) != expected_count
            or any(not isinstance(row, str) or not row for row in rows)
            or len(set(rows)) != len(rows)
        ):
            raise ChildError(f"NWG1 mutation manifest {name} inventory differs")
        return rows

    components = set(exact_string_inventory("componentInventory", 16))
    operations = set(exact_string_inventory("mutationOperations", 10))
    resign_operations = set(exact_string_inventory("resignOperations", 8))

    profiles = manifest["externalCallProfiles"]
    if not isinstance(profiles, list) or len(profiles) != 2:
        raise ChildError("NWG1 external-call profile count differs")
    profile_tokens: set[str] = set()
    for index, raw_profile in enumerate(profiles):
        profile = _exact_members(
            raw_profile, {"count", "maximumCalls", "token"},
            f"NWG1.externalCallProfiles[{index}]",
        )
        _positive(profile["count"], f"NWG1.externalCallProfiles[{index}].count")
        _nonnegative_map(
            profile["maximumCalls"], EXTERNAL_CALL_KINDS,
            f"NWG1.externalCallProfiles[{index}].maximumCalls",
        )
        if not isinstance(profile["token"], str) or not profile["token"]:
            raise ChildError("NWG1 external-call profile token is invalid")
        profile_tokens.add(profile["token"])
    if len(profile_tokens) != 2 or sum(row["count"] for row in profiles) != 84:
        raise ChildError("NWG1 external-call profile coverage differs")

    mutations = manifest["mutations"]
    if not isinstance(mutations, list) or len(mutations) != 84:
        raise ChildError("NWG1 mutation record count differs")
    base_keys = {
        "actualExternalCallsByPath",
        "applicablePaths",
        "baseVectorId",
        "expectedIsolationScope",
        "expectedMaximumExternalCallsByKind",
        "expectedPublication",
        "expectedRejectionCode",
        "expectedStage",
        "mutationClass",
        "mutationId",
        "mutationOperations",
        "mutationRecipeSha256",
        "neutralizedEarlierChecks",
        "resignOperations",
        "verificationEntryCut",
    }
    mutation_ids: set[str] = set()
    codes: set[str] = set()
    stages: set[str] = set()
    deep_roots = 0
    path_executions = 0
    for index, raw_mutation in enumerate(mutations):
        if not isinstance(raw_mutation, dict):
            raise ChildError(f"NWG1.mutations[{index}] is not an object")
        extra = set(raw_mutation) - base_keys
        if extra not in (set(), {"mutationRootSha256"}):
            raise ChildError(f"NWG1.mutations[{index}] member set differs")
        _exact_members(raw_mutation, base_keys | extra, f"NWG1.mutations[{index}]")
        mutation = raw_mutation
        if extra:
            _sha_text(mutation["mutationRootSha256"], f"NWG1.mutations[{index}].mutationRootSha256")
            deep_roots += 1
        for key in (
            "baseVectorId", "expectedIsolationScope", "expectedRejectionCode", "expectedStage",
            "mutationClass", "mutationId",
        ):
            if not isinstance(mutation[key], str) or not mutation[key]:
                raise ChildError(f"NWG1.mutations[{index}].{key} is invalid")
        mutation_ids.add(mutation["mutationId"])
        codes.add(mutation["expectedRejectionCode"])
        stages.add(mutation["expectedStage"])
        _sha_text(mutation["mutationRecipeSha256"], f"NWG1.mutations[{index}].mutationRecipeSha256")
        if (
            mutation["expectedPublication"] != "NONE"
            or mutation["verificationEntryCut"]
            != "PRELOADED_VERIFIED_ROOT_AND_ACQUIRED_BYTES_V1"
        ):
            raise ChildError("NWG1 mutation publication/verification cut differs")
        paths = mutation["applicablePaths"]
        if (
            not isinstance(paths, list)
            or not paths
            or len(set(paths)) != len(paths)
            or not set(paths).issubset(MUTATION_PATHS)
        ):
            raise ChildError(f"NWG1.mutations[{index}] applicable path set differs")
        path_executions += len(paths)
        calls = _exact_members(
            mutation["actualExternalCallsByPath"], set(paths),
            f"NWG1.mutations[{index}].actualExternalCallsByPath",
        )
        for path in paths:
            _nonnegative_map(
                calls[path], EXTERNAL_CALL_KINDS,
                f"NWG1.mutations[{index}].actualExternalCallsByPath.{path}",
            )
        _nonnegative_map(
            mutation["expectedMaximumExternalCallsByKind"], EXTERNAL_CALL_KINDS,
            f"NWG1.mutations[{index}].expectedMaximumExternalCallsByKind",
        )
        ops = mutation["mutationOperations"]
        if not isinstance(ops, list) or not ops:
            raise ChildError(f"NWG1.mutations[{index}] mutationOperations are empty")
        for op_index, raw_op in enumerate(ops):
            op_base = {"componentKind", "offset", "operandHex", "operation", "rowOrdinal"}
            if not isinstance(raw_op, dict):
                raise ChildError("NWG1 mutation operation is not an object")
            op_extra = set(raw_op) - op_base
            if op_extra not in (set(), {"componentProfile"}):
                raise ChildError("NWG1 mutation operation member set differs")
            op = _exact_members(
                raw_op, op_base | op_extra,
                f"NWG1.mutations[{index}].mutationOperations[{op_index}]",
            )
            if op["componentKind"] not in components or op["operation"] not in operations:
                raise ChildError("NWG1 mutation operation inventory reference differs")
            _nonnegative(op["offset"], "NWG1 mutation operation offset")
            _nonnegative(op["rowOrdinal"], "NWG1 mutation operation rowOrdinal")
            if not isinstance(op["operandHex"], str) or not re.fullmatch(r"(?:[0-9a-f]{2})*", op["operandHex"]):
                raise ChildError("NWG1 mutation operandHex is not canonical")
            rke_preimage = op["componentKind"] == "WRAPPED_ENVELOPE_COMMITMENT"
            if rke_preimage != bool(op_extra) or (
                op_extra and op["componentProfile"] != "RKE_ENVELOPE_PREIMAGE_V1"
            ):
                raise ChildError("NWG1 mutation componentProfile differs")
        resign = mutation["resignOperations"]
        neutralized = mutation["neutralizedEarlierChecks"]
        if (
            not isinstance(resign, list)
            or len(set(resign)) != len(resign)
            or not set(resign).issubset(resign_operations)
            or not isinstance(neutralized, list)
            or any(not isinstance(item, str) or not item for item in neutralized)
        ):
            raise ChildError("NWG1 mutation resign/neutralized inventory differs")
    if (
        len(mutation_ids) != 84
        or deep_roots != 50
        or path_executions != 240
        or len(codes) != 25
        or len(stages) != 16
    ):
        raise ChildError("NWG1 mutation manifest derived counts differ")
    for name, count in (("externalFixtures", 2), ("vectors", 6), ("zstdFixtures", 2)):
        rows = manifest[name]
        if not isinstance(rows, list) or len(rows) != count or any(not isinstance(row, dict) for row in rows):
            raise ChildError(f"NWG1 mutation manifest {name} count/schema differs")


def _closed_tsv_rows(
    raw: bytes, label: str, maximum_bytes: int, header: tuple[str, ...]
) -> list[tuple[str, ...]]:
    if not raw or len(raw) > maximum_bytes or b"\r" in raw or b"\0" in raw or not raw.endswith(b"\n"):
        raise ChildError(f"{label} byte envelope or line endings differ")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ChildError(f"{label} is not strict UTF-8") from error
    lines = text[:-1].split("\n")
    if not lines or tuple(lines[0].split("\t")) != header:
        raise ChildError(f"{label} header differs")
    rows = [tuple(line.split("\t")) for line in lines[1:]]
    if any(len(row) != len(header) or any(not field for field in row) for row in rows):
        raise ChildError(f"{label} row grammar differs")
    return rows


def validate_recovery_manifest(raw: bytes) -> None:
    rows = _closed_tsv_rows(
        raw,
        "WalRun recovery manifest",
        RECOVERY_MANIFEST_MAX_BYTES,
        RECOVERY_MANIFEST_HEADER,
    )
    if tuple(rows) != RECOVERY_MANIFEST_ROWS:
        raise ChildError("WalRun recovery manifest closed row inventory differs")
    manifested_tests = {(row[2], row[3]) for row in rows}
    governed_tests = set(GOVERNED_JUNIT_REQUIRED_TESTS["R_CONTROL_RECOVERY"])
    governed_tests.remove(
        (
            "com.nereusstream.storage.object.recovery.WalRunRecoveryManifestV1Test",
            "closedInventoryBindsExactControlAndRecoveryCases",
        )
    )
    if manifested_tests != governed_tests:
        raise ChildError("WalRun recovery manifest differs from the governed JUnit inventory")


def validate_protocol_fixture(raw: bytes) -> None:
    rows = _closed_tsv_rows(
        raw,
        "NWKCP1 protocol fixture",
        PROTOCOL_FIXTURE_MAX_BYTES,
        PROTOCOL_FIXTURE_HEADER,
    )
    if len(rows) != 3 or tuple((row[0], row[1]) for row in rows) != PROTOCOL_FIXTURE_IDS:
        raise ChildError("NWKCP1 protocol fixture closed artifact/state inventory differs")

    decoded: list[bytes] = []
    for index, row in enumerate(rows):
        try:
            length = int(row[3])
            body = bytes.fromhex(row[5])
        except (ValueError, TypeError) as error:
            raise ChildError("NWKCP1 protocol fixture length/hex is not canonical") from error
        if row[3] != str(length) or length <= 0 or length != len(body):
            raise ChildError("NWKCP1 protocol fixture declared length differs")
        if row[5] != body.hex() or row[4] != sha256(body):
            raise ChildError("NWKCP1 protocol fixture body SHA/hex differs")
        decoded.append(body)

    object_key = (
        f"{PROTOCOL_FIXTURE_ROOT}/protocol/kafka/nwkcp1-v1/objects/"
        f"sha256-v1-{rows[0][4]}.nwkcp1"
    )
    head_key = f"{PROTOCOL_FIXTURE_ROOT}/protocol/kafka/nwkcp1-v1/head"
    if rows[0][2] != object_key or rows[1][2] != head_key or rows[2][2] != head_key:
        raise ChildError("NWKCP1 protocol fixture key grammar or content identity differs")

    object_body, open_head, terminal_head = decoded
    if (
        len(object_body) != 324
        or object_body[:8] != NWKCP1_MAGIC
        or int.from_bytes(object_body[8:12], "big") != 64
        or object_body[12:16] != bytes(4)
        or int.from_bytes(object_body[16:24], "big") != len(object_body)
        or int.from_bytes(object_body[56:60], "big") != 1
        or int.from_bytes(object_body[64:68], "big") != 224
    ):
        raise ChildError("NWKCP1 protocol fixture Object header/row envelope differs")
    for head, expected_state in ((open_head, 0), (terminal_head, 1)):
        if (
            len(head) != 434
            or head[:8] != NWKCP1_HEAD_MAGIC
            or int.from_bytes(head[8:12], "big") != len(head)
            or head[12:44] != object_body[24:56]
            or int.from_bytes(head[44:52], "big") != 9
            or head[52] != expected_state
            or head[53:56] != bytes(3)
        ):
            raise ChildError("NWKCP1 protocol fixture Head wire/context/state differs")
    if open_head[:52] != terminal_head[:52] or open_head[53:-4] != terminal_head[53:-4]:
        raise ChildError("NWKCP1 OPEN to TERMINAL fixture changes fields outside state/CRC")


def _validate_kms_raw_receipt(
    value: object, tested_commit: str, binding: dict[str, str]
) -> dict[str, int]:
    row = _exact_members(
        value,
        {
            "adapterVersion",
            "applicationOwnedPlaintextKeyArraysZeroized",
            "containerAutoRemove",
            "containerId",
            "contractSha256",
            "coreKmsTransportSpi",
            "derivedKeyContext",
            "devMode",
            "errors",
            "failures",
            "imageConfigDigest",
            "imageReference",
            "jdkHttpTlsInternalBufferZeroizationProven",
            "kmsContextAuthority",
            "networkBinding",
            "nereusCommit",
            "envelopeAlgorithmId",
            "envelopeProviderId",
            "oldVersionDecryptAfterRotation",
            "product",
            "productVersion",
            "productionDeploymentProven",
            "promotionEligible",
            "realKms",
            "result",
            "runKeyBytes",
            "schema",
            "skipped",
            "testClass",
            "testMethod",
            "testTask",
            "tests",
            "tokenStringZeroizationProven",
            "versionsProven",
            "walRunTerminalClosureProof",
            "wrappingKeyIdentity",
        },
        "real KMS receipt",
    )
    identity = (
        f"{binding['backend']}|artifactReference={row['imageReference']}"
        f"|artifactConfigDigest={row['imageConfigDigest']}"
    )
    if (
        binding["childKind"] != "C1_REAL_PROVIDER_KMS"
        or binding["evidenceKind"] != "KMS_REAL_RECEIPT"
        or binding["backend"] != "VAULT_TRANSIT"
        or binding["executionClass"] != "REAL_EXTERNAL_PROCESS"
        or binding["sourceIdentity"] != identity
        or row["schema"] != "NEREUS_V2_M3_REAL_KMS_EVIDENCE_V1"
        or row["result"] != "PASS_REAL_VAULT_TRANSIT_KMS_ONLY"
        or row["promotionEligible"] is not False
        or row["realKms"] is not True
        or row["product"] != "HashiCorp Vault Transit"
        or row["networkBinding"] != "127.0.0.1:RANDOM"
        or row["nereusCommit"] != tested_commit
        or not isinstance(row["containerId"], str)
        or not re.fullmatch(r"[0-9a-f]{12,64}", row["containerId"])
        or row["adapterVersion"] != "nereus-vault-transit-v1/jdk-http"
        or row["containerAutoRemove"] is not True
        or row["devMode"] is not True
        or row["coreKmsTransportSpi"] is not True
        or row["derivedKeyContext"] is not False
        or row["kmsContextAuthority"] != "NONE"
        or row["envelopeProviderId"] != "HASHICORP_VAULT_TRANSIT"
        or row["envelopeAlgorithmId"] != "AES256_GCM96"
        or row["wrappingKeyIdentity"]
        != "vault-transit://transit/keys/nereus-m3-run-key"
        or row["oldVersionDecryptAfterRotation"] is not True
        or row["walRunTerminalClosureProof"] is not True
        or row["applicationOwnedPlaintextKeyArraysZeroized"] is not True
        or row["tokenStringZeroizationProven"] is not False
        or row["jdkHttpTlsInternalBufferZeroizationProven"] is not False
        or row["productionDeploymentProven"] is not False
        or row["versionsProven"] != [1, 2]
        or row["runKeyBytes"] != 32
        or row["testTask"] != "realKmsTest"
        or row["testClass"]
        != "com.nereusstream.storage.object.vault.VaultTransitRealKmsEvidenceTest"
        or row["testMethod"]
        != "provesRealTransitWrapUnwrapVersionRotationAndLifecycle()"
        or row["tests"] != 1
    ):
        raise ChildError("real KMS receipt schema/result/backend/lifecycle boundary differs")
    contract = (
        "provider=hashicorp-vault-transit\n"
        f"productVersion={row['productVersion']}\n"
        "keyType=aes256-gcm96\n"
        "derivedKeyContext=false\n"
        "kmsContextAuthority=NONE\n"
        f"runKeyBytes={row['runKeyBytes']}\n"
        f"envelopeProviderId={row['envelopeProviderId']}\n"
        f"envelopeAlgorithmId={row['envelopeAlgorithmId']}\n"
        f"envelopeIdentity={row['wrappingKeyIdentity']}\n"
        "rotationBoundary=WALRUN_TERMINAL_CLOSURE_PROOF_V1\n"
    )
    if row["contractSha256"] != sha256(contract.encode("ascii")):
        raise ChildError("real KMS contract digest differs from exact receipt fields")
    return _summary_from_receipt(row, "real KMS receipt")


def _decode_sealed_bytes(value: object, label: str, maximum: int) -> bytes:
    if not isinstance(value, str) or not value or not value.isascii():
        raise ChildError(f"{label} is not canonical base64 text")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, base64.binascii.Error) as error:
        raise ChildError(f"{label} is not canonical base64 text") from error
    if not raw or len(raw) > maximum or base64.b64encode(raw).decode("ascii") != value:
        raise ChildError(f"{label} bytes are empty, over cap, or non-canonical")
    return raw


def _validate_real_junit_xml(raw: bytes, attachment_kind: str) -> dict[str, int]:
    if len(raw) > 1_048_576 or b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
        raise ChildError("sealed real execution JUnit XML is unsafe or over cap")
    task, expected_class, expected_method = REAL_EXECUTION_PROFILES[attachment_kind]
    del task
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as error:
        raise ChildError(f"cannot parse sealed real execution JUnit XML: {error}") from error
    if root.tag != "testsuite" or root.attrib.get("name") != expected_class:
        raise ChildError("sealed real execution JUnit suite identity differs")
    summary: dict[str, int] = {}
    for field in ("tests", "failures", "errors", "skipped"):
        try:
            summary[field] = int(root.attrib[field])
        except (KeyError, ValueError) as error:
            raise ChildError(f"sealed real execution JUnit {field} is invalid") from error
    _validate_summary(summary, "sealed real execution JUnit summary")
    cases = root.findall("testcase")
    if len(cases) != 1:
        raise ChildError("sealed real execution JUnit must contain exactly one testcase")
    case = cases[0]
    if (
        case.attrib.get("classname") != expected_class
        or case.attrib.get("name") not in {expected_method, expected_method.removesuffix("()")}
        or case.find("failure") is not None
        or case.find("error") is not None
        or case.find("skipped") is not None
    ):
        raise ChildError("sealed real execution JUnit testcase identity/result differs")
    return summary


def seal_real_execution_receipt(
    raw_evidence: bytes,
    junit_xml: bytes,
    attachment_kind: str,
    tested_commit: str,
) -> dict[str, Any]:
    if attachment_kind not in REAL_EXECUTION_PROFILES:
        raise ChildError(f"real execution receipt kind is not closed: {attachment_kind}")
    if not COMMIT_PATTERN.fullmatch(tested_commit):
        raise ChildError("real execution receipt tested commit is not canonical")
    raw_value = load_external_json(raw_evidence, f"raw {attachment_kind}")
    task, test_class, test_method = REAL_EXECUTION_PROFILES[attachment_kind]
    if (
        raw_value.get("nereusCommit") != tested_commit
        or raw_value.get("testTask") != task
        or raw_value.get("testClass") != test_class
        or raw_value.get("testMethod") != test_method
    ):
        raise ChildError("raw real evidence source/test identity differs before sealing")
    _validate_real_junit_xml(junit_xml, attachment_kind)
    receipt: dict[str, Any] = {
        "evidenceKind": attachment_kind,
        "execution": {
            "task": task,
            "testClass": test_class,
            "testMethod": test_method,
        },
        "junitXmlBase64": base64.b64encode(junit_xml).decode("ascii"),
        "junitXmlSha256": sha256(junit_xml),
        "rawEvidenceBase64": base64.b64encode(raw_evidence).decode("ascii"),
        "rawEvidenceSha256": sha256(raw_evidence),
        "receiptSha256": "0" * 64,
        "schema": SEALED_REAL_EXECUTION_SCHEMA,
        "testedCommit": tested_commit,
    }
    receipt["receiptSha256"] = sha256(canonical_bytes(receipt))
    return receipt


def _validate_sealed_real_execution_receipt(
    value: object,
    tested_commit: str,
    binding: dict[str, str],
    attachment_kind: str,
) -> dict[str, int]:
    row = _exact_members(
        value,
        {
            "evidenceKind",
            "execution",
            "junitXmlBase64",
            "junitXmlSha256",
            "rawEvidenceBase64",
            "rawEvidenceSha256",
            "receiptSha256",
            "schema",
            "testedCommit",
        },
        "sealed real execution receipt",
    )
    task, test_class, test_method = REAL_EXECUTION_PROFILES[attachment_kind]
    execution = _exact_members(
        row["execution"], {"task", "testClass", "testMethod"},
        "sealed real execution receipt.execution",
    )
    if (
        row["schema"] != SEALED_REAL_EXECUTION_SCHEMA
        or row["evidenceKind"] != attachment_kind
        or row["testedCommit"] != tested_commit
        or execution
        != {"task": task, "testClass": test_class, "testMethod": test_method}
    ):
        raise ChildError("sealed real execution receipt schema/source/test identity differs")
    raw_evidence = _decode_sealed_bytes(
        row["rawEvidenceBase64"], "sealed raw real evidence", MAX_CANONICAL_BYTES
    )
    junit_xml = _decode_sealed_bytes(
        row["junitXmlBase64"], "sealed real execution JUnit XML", 1_048_576
    )
    if row["rawEvidenceSha256"] != sha256(raw_evidence):
        raise ChildError("sealed raw real evidence SHA-256 differs")
    if row["junitXmlSha256"] != sha256(junit_xml):
        raise ChildError("sealed real execution JUnit XML SHA-256 differs")
    _sha_text(row["receiptSha256"], "sealed real execution receipt SHA")
    unsigned = dict(row)
    unsigned["receiptSha256"] = "0" * 64
    if row["receiptSha256"] != sha256(canonical_bytes(unsigned)):
        raise ChildError("sealed real execution receipt self-hash differs")
    junit_summary = _validate_real_junit_xml(junit_xml, attachment_kind)
    raw_value = load_external_json(raw_evidence, f"sealed raw {attachment_kind}")
    if attachment_kind == "PROVIDER_REAL_RECEIPT":
        raw_summary = _validate_provider_raw_receipt(raw_value, tested_commit, binding)
    else:
        raw_summary = _validate_kms_raw_receipt(raw_value, tested_commit, binding)
    if raw_summary != junit_summary:
        raise ChildError("raw real evidence counters differ from exact sealed JUnit XML")
    return junit_summary


def _allocator_basename(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or not value.isascii()
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
        or "\0" in value
    ):
        raise ChildError(f"{label} is not an exact safe basename")
    return value


def _validate_allocator_verifier_junit(
    raw: bytes, expected: dict[str, Any]
) -> dict[str, int]:
    if (
        not raw
        or len(raw) > MAX_NATIVE_JUNIT_XML_BYTES
        or b"<!DOCTYPE" in raw.upper()
        or b"<!ENTITY" in raw.upper()
    ):
        raise ChildError("allocator verifier JUnit XML is empty, unsafe, or over cap")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as error:
        raise ChildError(f"cannot parse allocator verifier JUnit XML: {error}") from error
    if (
        root.tag != "testsuite"
        or root.attrib.get("name") != ALLOCATOR_VERIFIER_TEST_CLASS
        or root.attrib.get("tests") != "1"
        or root.attrib.get("failures") != "0"
        or root.attrib.get("errors") != "0"
        or root.attrib.get("skipped") != "0"
    ):
        raise ChildError("allocator verifier JUnit suite counters differ from exact 1/0/0/0")
    cases = root.findall("testcase")
    if len(cases) != 1:
        raise ChildError("allocator verifier JUnit must contain exactly one testcase")
    case = cases[0]
    if (
        case.attrib.get("classname") != ALLOCATOR_VERIFIER_TEST_CLASS
        or case.attrib.get("name") != ALLOCATOR_VERIFIER_TEST_CASE
        or any(isinstance(child.tag, str) for child in case)
    ):
        raise ChildError("allocator verifier JUnit testcase identity/result differs")
    if expected != {
        "basename": expected.get("basename"),
        "bytes": len(raw),
        "sha256": sha256(raw),
        "tests": 1,
        "failures": 0,
        "errors": 0,
        "skips": 0,
        "testClass": ALLOCATOR_VERIFIER_TEST_CLASS,
        "testCase": ALLOCATOR_VERIFIER_TEST_CASE,
    }:
        raise ChildError("allocator sealed verification differs from exact verifier JUnit XML")
    _allocator_basename(expected["basename"], "allocator verifier JUnit basename")
    return {"errors": 0, "failures": 0, "skipped": 0, "tests": 1}


def _allocator_external_path(
    value: object, label: str, allow_absolute: bool = False
) -> PurePosixPath:
    if not isinstance(value, str) or not value or not value.isascii() or "\\" in value or "\0" in value:
        raise ChildError(f"{label} is not a canonical allocator external path")
    path = PurePosixPath(value)
    if path.is_absolute():
        if not allow_absolute or any(part in {"", ".", ".."} for part in path.parts[1:]):
            raise ChildError(f"{label} cannot use this absolute external path")
        return path
    path = _native_relative(value, label)
    if path.parts[0] == ".git":
        raise ChildError(f"{label} cannot address repository metadata")
    return path


def _allocator_stream_identity(
    root: Path,
    relative: PurePosixPath,
    maximum: int,
    prefix_bytes: int = 0,
    payload_offset: int | None = None,
    allow_absolute: bool = False,
) -> tuple[int, str, bytes, str | None]:
    relative = _allocator_external_path(str(relative), f"allocator external path {relative}", allow_absolute)
    current = Path("/") if relative.is_absolute() else root
    parts = relative.parts[1:] if relative.is_absolute() else relative.parts
    for index, part in enumerate(parts):
        current /= part
        try:
            mode = current.lstat().st_mode
        except OSError as error:
            raise ChildError(f"allocator external file is missing: {relative}") from error
        if stat.S_ISLNK(mode) and (not relative.is_absolute() or index == len(parts) - 1):
            raise ChildError(f"allocator external path has a symlink component: {relative}")
    if not stat.S_ISREG(mode):
        raise ChildError(f"allocator external file is not regular: {relative}")
    before = current.stat()
    if before.st_size <= 0 or before.st_size > maximum:
        raise ChildError(f"allocator external file bytes outside cap: {relative}")
    try:
        descriptor = os.open(current, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as error:
        raise ChildError(f"allocator external file cannot be opened safely: {relative}") from error
    whole = hashlib.sha256()
    payload = hashlib.sha256() if payload_offset is not None else None
    prefix = bytearray()
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino, opened.st_size) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
        ):
            raise ChildError(f"allocator external file identity changed before read: {relative}")
        position = 0
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            while True:
                block = source.read(1024 * 1024)
                if not block:
                    break
                whole.update(block)
                if len(prefix) < prefix_bytes:
                    prefix.extend(block[: prefix_bytes - len(prefix)])
                if payload is not None:
                    block_end = position + len(block)
                    if block_end > payload_offset:
                        payload.update(block[max(0, payload_offset - position) :])
                position += len(block)
        after = os.fstat(descriptor)
        if position != before.st_size or (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
            raise ChildError(f"allocator external file changed while hashing: {relative}")
    finally:
        os.close(descriptor)
    if len(prefix) != prefix_bytes:
        raise ChildError(f"allocator external file prefix is truncated: {relative}")
    return before.st_size, whole.hexdigest(), bytes(prefix), payload.hexdigest() if payload else None


def allocator_external_file_rows(
    root: Path,
    tested_commit: str,
    external_paths: list[tuple[str, object]],
) -> list[dict[str, Any]]:
    if [name for name, _ in external_paths] != list(ALLOCATOR_EXTERNAL_NAMES):
        raise ChildError("allocator external path inventory/order differs from exact thirteen-file contract")
    formal_directory = ALLOCATOR_FORMAL_PREFIX / tested_commit
    rows: list[dict[str, Any]] = []
    observed_paths: set[PurePosixPath] = set()
    for name, raw_path in external_paths:
        allow_absolute = name in ALLOCATOR_EXTERNAL_NAMES[6:] and name != "sourceLocks"
        path = _allocator_external_path(
            raw_path, f"allocator external {name}.path", allow_absolute
        )
        if path in observed_paths:
            raise ChildError("allocator external file path is duplicated")
        observed_paths.add(path)
        if name in ALLOCATOR_EXTERNAL_NAMES[:6] and path != formal_directory / name:
            raise ChildError(f"allocator formal raw path differs: {name}")
        if name == "sourceLocks" and path != FINAL.SOURCE_LOCKS_PATH:
            raise ChildError("allocator external sourceLocks path differs")
        if name == "executorManifest" and path != (
            PurePosixPath("nereus-metadata-oxia/build/m3-allocator-evidence/executor")
            / f"{tested_commit}.json"
        ):
            raise ChildError("allocator external executor manifest path differs")
        maximum = (
            2328
            if name == "selection.nars"
            else (
                ALLOCATOR_NAEA_MAX_BYTES
                if name.endswith(".naea")
                else ALLOCATOR_SOURCE_ARTIFACT_MAX_BYTES
            )
        )
        size, digest, _, _ = _allocator_stream_identity(
            root, path, maximum, allow_absolute=allow_absolute
        )
        if name == "selection.nars" and size != 2328:
            raise ChildError("allocator external selection.nars fixed bytes differ")
        rows.append({"bytes": size, "name": name, "path": str(path), "sha256": digest})
    return rows


def _allocator_source_tuple_from_bytes(raw: bytes, label: str) -> dict[str, str]:
    if len(raw) != 304:
        raise ChildError(f"{label} source tuple length differs")
    offset = 0
    commits: list[str] = []
    for _ in range(4):
        commits.append(raw[offset : offset + 20].hex())
        offset += 20
    digests: list[str] = []
    for _ in range(7):
        digest = raw[offset : offset + 32].hex()
        if digest == "0" * 64:
            raise ChildError(f"{label} source tuple contains zero digest")
        digests.append(digest)
        offset += 32
    if any(not COMMIT_PATTERN.fullmatch(commit) or commit == "0" * 40 for commit in commits):
        raise ChildError(f"{label} source tuple contains invalid commit")
    keys = (
        "nereusCommit",
        "pulsarCommit",
        "oxiaClientCommit",
        "oxiaServerCommit",
        "oxiaClientJarSha256",
        "testedEvidenceArtifactSha256",
        "runtimeDomainArtifactSha256",
        "runtimeMetadataSpiArtifactSha256",
        "runtimeMetadataOxiaArtifactSha256",
        "sourceLocksSha256",
        "executorManifestSha256",
    )
    return dict(zip(keys, (*commits, *digests), strict=True))


def _validate_allocator_external_files(
    root: Path,
    tested_commit: str,
    external_rows: object,
    recomputation: dict[str, Any],
) -> None:
    if not isinstance(external_rows, list) or len(external_rows) != len(ALLOCATOR_EXTERNAL_NAMES):
        raise ChildError("allocator governed external file inventory count differs")
    rows: dict[str, dict[str, Any]] = {}
    paths: list[tuple[str, object]] = []
    for index, raw_row in enumerate(external_rows):
        row = _exact_members(
            raw_row,
            {"bytes", "name", "path", "sha256"},
            f"allocator governed externalFiles[{index}]",
        )
        expected_name = ALLOCATOR_EXTERNAL_NAMES[index]
        if row["name"] != expected_name:
            raise ChildError("allocator governed external file inventory/order differs")
        _positive(row["bytes"], f"allocator external {expected_name}.bytes")
        _sha_text(row["sha256"], f"allocator external {expected_name}.sha256")
        paths.append((expected_name, row["path"]))
        rows[expected_name] = row
    actual_rows = allocator_external_file_rows(root, tested_commit, paths)
    if actual_rows != external_rows:
        raise ChildError("allocator external file bytes/SHA differ from governed wrapper")

    selection_path = _allocator_external_path(rows["selection.nars"]["path"], "selection.nars")
    _, _, selection, _ = _allocator_stream_identity(root, selection_path, 2328, 2328)
    if selection[:4] != b"NARS" or struct.unpack_from(">H", selection, 4)[0] != 1:
        raise ChildError("allocator NARS1 magic/schema differs")
    mode_code = struct.unpack_from(">H", selection, 6)[0]
    mode = {1: "STRICT_SERIALIZED", 2: "RANGE_LEASED"}.get(mode_code)
    protocol, flags, selected_range, brokers, required_cuts, completed_cuts = struct.unpack_from(
        ">IIqIHH", selection, 8
    )
    if (
        mode != recomputation["selectedMode"]
        or selected_range != recomputation["selectedRangeSize"]
        or protocol != 1
        or flags != 7
        or brokers != 4
        or required_cuts != 9
        or completed_cuts != 9
        or struct.unpack_from(">I", selection, 496)[0] != 8
        or selection[-36:] != bytes(36)
    ):
        raise ChildError("allocator NARS1 fixed header/eligibility fields differ")
    nars_tuple = _allocator_source_tuple_from_bytes(selection[32:336], "allocator NARS1")
    if nars_tuple != recomputation["source"]:
        raise ChildError("allocator NARS1 source tuple differs from raw recomputation")
    nars_attachment_digests = [
        selection[offset : offset + 32].hex() for offset in range(336, 496, 32)
    ]
    if (
        rows["selection.nars"]["sha256"] != recomputation["selection"]["sha256"]
        or rows["selection.nars"]["bytes"] != recomputation["selection"]["bytes"]
    ):
        raise ChildError("allocator NARS1 identity differs from raw recomputation")

    for index, name in enumerate(ALLOCATOR_EXTERNAL_NAMES[1:6], start=1):
        path = _allocator_external_path(rows[name]["path"], f"allocator {name}")
        size, _, header, payload_digest = _allocator_stream_identity(
            root,
            path,
            ALLOCATOR_NAEA_MAX_BYTES,
            ALLOCATOR_NAEA_HEADER_BYTES,
            ALLOCATOR_NAEA_HEADER_BYTES,
        )
        if (
            header[:4] != b"NAEA"
            or struct.unpack_from(">H", header, 4)[0] != 1
            or struct.unpack_from(">H", header, 6)[0] != index
            or header[352:360] != bytes(8)
        ):
            raise ChildError(f"allocator {name} fixed header/kind differs")
        source_tuple = _allocator_source_tuple_from_bytes(header[8:312], f"allocator {name}")
        declared_payload = struct.unpack_from(">q", header, 344)[0]
        if (
            source_tuple != nars_tuple
            or declared_payload <= 0
            or declared_payload != size - ALLOCATOR_NAEA_HEADER_BYTES
            or header[312:344].hex() != payload_digest
            or nars_attachment_digests[index - 1] != rows[name]["sha256"]
            or recomputation["attachments"][name]
            != {"bytes": size, "envelopeSha256": rows[name]["sha256"]}
        ):
            raise ChildError(f"allocator {name} payload/source/envelope identity differs")

    for name in ALLOCATOR_EXTERNAL_NAMES[6:]:
        expected = recomputation["sourceArtifacts"][name]
        row = rows[name]
        if expected != {
            "basename": PurePosixPath(row["path"]).name,
            "bytes": row["bytes"],
            "sha256": row["sha256"],
        }:
            raise ChildError(f"allocator external source artifact differs: {name}")


def _validate_java_allocator_verification(
    raw: bytes,
    junit_xml: bytes,
    root: Path,
    tested_commit: str,
    locked_mode: str,
    external_rows: object,
) -> dict[str, Any]:
    if not raw or len(raw) > ALLOCATOR_VERIFICATION_MAX_BYTES:
        raise ChildError("allocator Java sealed verification bytes are empty or over cap")
    value = load_external_json(raw, "allocator Java sealed verification", ALLOCATOR_VERIFICATION_MAX_BYTES)
    if (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=False,
        ).encode("utf-8")
        + b"\n"
        != raw
    ):
        raise ChildError("allocator Java sealed verification is not exact fixed-order JSON")
    row = _exact_members(
        value,
        {
            "rawVerification",
            "rawVerificationBytes",
            "rawVerificationSha256",
            "schema",
            "selfHashRule",
            "selfSha256",
            "verifierJUnit",
        },
        "allocator Java sealed verification",
    )
    if tuple(row) != (
        "schema",
        "selfSha256",
        "selfHashRule",
        "rawVerification",
        "rawVerificationBytes",
        "rawVerificationSha256",
        "verifierJUnit",
    ):
        raise ChildError("allocator Java sealed verification member order differs")
    self_sha = _sha_text(row["selfSha256"], "allocator Java sealed verification self SHA")
    prefix = (
        '{"schema":"' + JAVA_ALLOCATOR_VERIFICATION_SCHEMA + '","selfSha256":"'
    ).encode("ascii")
    if not raw.startswith(prefix + self_sha.encode("ascii")):
        raise ChildError("allocator Java sealed verification prefix/order differs")
    self_offset = len(prefix)
    zeroed = raw[:self_offset] + b"0" * 64 + raw[self_offset + 64 :]
    if sha256(zeroed) != self_sha:
        raise ChildError("allocator Java sealed verification self-hash differs")
    if (
        row["schema"] != JAVA_ALLOCATOR_VERIFICATION_SCHEMA
        or row["selfHashRule"] != ALLOCATOR_VERIFICATION_SELF_HASH_RULE
    ):
        raise ChildError("allocator Java sealed verification schema/self-hash rule differs")

    recomputation = _exact_members(
        row["rawVerification"],
        {
            "attachments",
            "authority",
            "derived",
            "junit",
            "schema",
            "selectedMode",
            "selectedRangeSize",
            "selection",
            "selectionEligible",
            "selfHashRule",
            "selfSha256",
            "source",
            "sourceLocksSha256",
            "sourceArtifacts",
            "status",
            "testedCommit",
        },
        "allocator raw recomputation",
    )
    if tuple(recomputation) != (
        "schema",
        "selfSha256",
        "selfHashRule",
        "authority",
        "status",
        "selectionEligible",
        "testedCommit",
        "sourceLocksSha256",
        "selectedMode",
        "selectedRangeSize",
        "selection",
        "derived",
        "junit",
        "source",
        "attachments",
        "sourceArtifacts",
    ):
        raise ChildError("allocator raw recomputation member order differs")
    raw_recomputation = json.dumps(
        recomputation,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=False,
    ).encode("utf-8") + b"\n"
    if (
        row["rawVerificationBytes"] != len(raw_recomputation)
        or row["rawVerificationSha256"] != sha256(raw_recomputation)
    ):
        raise ChildError("allocator raw recomputation bytes/SHA differ")
    raw_self_sha = _sha_text(
        recomputation["selfSha256"], "allocator raw recomputation self SHA"
    )
    raw_prefix = (
        '{"schema":"' + JAVA_ALLOCATOR_RAW_SCHEMA + '","selfSha256":"'
    ).encode("ascii")
    if not raw_recomputation.startswith(raw_prefix + raw_self_sha.encode("ascii")):
        raise ChildError("allocator raw recomputation self-hash prefix/order differs")
    raw_self_offset = len(raw_prefix)
    raw_zeroed = (
        raw_recomputation[:raw_self_offset]
        + b"0" * 64
        + raw_recomputation[raw_self_offset + 64 :]
    )
    if (
        recomputation["selfHashRule"] != ALLOCATOR_VERIFICATION_SELF_HASH_RULE
        or sha256(raw_zeroed) != raw_self_sha
    ):
        raise ChildError("allocator raw recomputation self-hash differs")
    selected_mode = recomputation["selectedMode"]
    expected_wire_mode = "STRICT_SERIALIZED" if locked_mode == "STRICT" else "RANGE_LEASED"
    selected_range = recomputation["selectedRangeSize"]
    if (
        recomputation["schema"] != JAVA_ALLOCATOR_RAW_SCHEMA
        or recomputation["authority"] is not False
        or recomputation["status"] != "PASS_RAW_RECOMPUTED"
        or recomputation["selectionEligible"] is not True
        or selected_mode != expected_wire_mode
        or (
            selected_mode == "STRICT_SERIALIZED"
            and selected_range != 1
        )
        or (
            selected_mode == "RANGE_LEASED"
            and selected_range not in {16, 64, 256, 1024}
        )
    ):
        raise ChildError("allocator raw recomputation selection/mode/range differs from source locks")
    selection = _exact_members(
        recomputation["selection"], {"basename", "bytes", "sha256"},
        "allocator raw recomputation selection",
    )
    if (
        selection["basename"] != "selection.nars"
        or selection["bytes"] != 2328
    ):
        raise ChildError("allocator NARS1 basename/fixed bytes differ")
    _sha_text(selection["sha256"], "allocator NARS1 SHA")
    derived = _exact_members(
        recomputation["derived"], {"faultCutKinds", "intervals", "selectedRows"},
        "allocator raw recomputation derived counts",
    )
    if derived != {"intervals": 288, "faultCutKinds": 9, "selectedRows": 8}:
        raise ChildError("allocator raw recomputation 288/9/8 derived counts differ")
    junit = _exact_members(
        recomputation["junit"], {"errors", "failures", "skips", "tests"},
        "allocator raw evidence JUnit",
    )
    summary = _validate_summary(
        {
            "errors": junit["errors"],
            "failures": junit["failures"],
            "skipped": junit["skips"],
            "tests": junit["tests"],
        },
        "allocator raw evidence JUnit",
    )

    locks = _source_locks(root, tested_commit)
    pulsar = _required_object(locks.get("m2PulsarNativeBinding"), "m2PulsarNativeBinding")
    oxia = _find_by_id(
        locks.get("dependencyForkOutputs"),
        "oxia-client-notification-continuity",
        "dependencyForkOutputs",
    )
    dependencies = _required_object(
        locks.get("dependencyEvidenceBindings"), "dependencyEvidenceBindings"
    )
    client = _required_object(dependencies.get("oxiaClientArtifacts"), "oxiaClientArtifacts")
    client_jar = _required_object(
        _required_object(client.get("artifacts"), "oxiaClientArtifacts.artifacts").get("clientJar"),
        "oxiaClientArtifacts.clientJar",
    )
    server = _required_object(dependencies.get("oxiaServerRuntime"), "oxiaServerRuntime")
    source_locks_raw = git(root, "show", f"{tested_commit}:{FINAL.SOURCE_LOCKS_PATH}")
    assert isinstance(source_locks_raw, bytes)
    source = _exact_members(
        recomputation["source"],
        {
            "executorManifestSha256",
            "nereusCommit",
            "oxiaClientCommit",
            "oxiaClientJarSha256",
            "oxiaServerCommit",
            "pulsarCommit",
            "runtimeDomainArtifactSha256",
            "runtimeMetadataOxiaArtifactSha256",
            "runtimeMetadataSpiArtifactSha256",
            "sourceLocksSha256",
            "testedEvidenceArtifactSha256",
        },
        "allocator raw recomputation source tuple",
    )
    if (
        recomputation["testedCommit"] != tested_commit
        or recomputation["sourceLocksSha256"] != sha256(source_locks_raw)
        or source["nereusCommit"] != tested_commit
        or source["pulsarCommit"]
        != _required_string(pulsar.get("finalForkCommit"), "m2PulsarNativeBinding.finalForkCommit")
        or source["oxiaClientCommit"]
        != _required_string(oxia.get("finalForkCommit"), "oxia client finalForkCommit")
        or source["oxiaServerCommit"]
        != _required_string(server.get("sourceCommit"), "oxia server sourceCommit")
        or source["oxiaClientJarSha256"]
        != _required_string(client_jar.get("sha256"), "oxia client jar SHA")
        or source["sourceLocksSha256"] != sha256(source_locks_raw)
    ):
        raise ChildError("allocator raw recomputation exact source tuple differs from source locks")
    for name in (
        "executorManifestSha256",
        "runtimeDomainArtifactSha256",
        "runtimeMetadataOxiaArtifactSha256",
        "runtimeMetadataSpiArtifactSha256",
        "testedEvidenceArtifactSha256",
    ):
        if _sha_text(source[name], f"allocator source.{name}") == "0" * 64:
            raise ChildError(f"allocator source.{name} cannot be zero")

    attachment_names = (
        "test.naea",
        "native.naea",
        "fault.naea",
        "scale-10000.naea",
        "scale-100000.naea",
    )
    attachments = _exact_members(
        recomputation["attachments"], set(attachment_names),
        "allocator raw NAEA1 attachment inventory",
    )
    for name in attachment_names:
        attachment = _exact_members(
            attachments[name], {"bytes", "envelopeSha256"}, f"allocator attachment {name}"
        )
        if _positive(attachment["bytes"], f"allocator attachment {name}.bytes") <= 360:
            raise ChildError(f"allocator attachment {name} is not a complete NAEA1 envelope")
        _sha_text(attachment["envelopeSha256"], f"allocator attachment {name}.envelopeSha256")

    artifact_names = {
        "executorManifest",
        "oxiaClientJar",
        "runtimeDomainArtifact",
        "runtimeMetadataOxiaArtifact",
        "runtimeMetadataSpiArtifact",
        "sourceLocks",
        "testedEvidenceArtifact",
    }
    source_artifacts = _exact_members(
        recomputation["sourceArtifacts"], artifact_names,
        "allocator source artifact inventory",
    )
    digest_fields = {
        "executorManifest": "executorManifestSha256",
        "oxiaClientJar": "oxiaClientJarSha256",
        "runtimeDomainArtifact": "runtimeDomainArtifactSha256",
        "runtimeMetadataOxiaArtifact": "runtimeMetadataOxiaArtifactSha256",
        "runtimeMetadataSpiArtifact": "runtimeMetadataSpiArtifactSha256",
        "sourceLocks": "sourceLocksSha256",
        "testedEvidenceArtifact": "testedEvidenceArtifactSha256",
    }
    for name in sorted(artifact_names):
        artifact = _exact_members(
            source_artifacts[name], {"basename", "bytes", "sha256"},
            f"allocator source artifact {name}",
        )
        _allocator_basename(artifact["basename"], f"allocator source artifact {name}.basename")
        _positive(artifact["bytes"], f"allocator source artifact {name}.bytes")
        if artifact["sha256"] != source[digest_fields[name]]:
            raise ChildError(f"allocator source artifact {name} SHA differs from NAEA1 source tuple")
    expected_client_basename = PurePosixPath(
        _required_string(client_jar.get("relativePath"), "oxia client jar relativePath")
    ).name
    if (
        source_artifacts["oxiaClientJar"]["basename"] != expected_client_basename
        or source_artifacts["oxiaClientJar"]["bytes"]
        != client_jar.get("bytes")
        or source_artifacts["sourceLocks"]
        != {
            "basename": FINAL.SOURCE_LOCKS_PATH.name,
            "bytes": len(source_locks_raw),
            "sha256": sha256(source_locks_raw),
        }
    ):
        raise ChildError("allocator locked Oxia/source-lock artifact identity differs")

    _validate_allocator_external_files(root, tested_commit, external_rows, recomputation)

    verifier = _exact_members(
        row["verifierJUnit"],
        {
            "basename",
            "bytes",
            "errors",
            "failures",
            "sha256",
            "skips",
            "testCase",
            "testClass",
            "tests",
        },
        "allocator verifier JUnit identity",
    )
    _validate_allocator_verifier_junit(junit_xml, verifier)
    return {
        "mode": locked_mode,
        "rawSummary": summary,
        "selectedRangeSize": selected_range,
        "selectionSha256": selection["sha256"],
        "verifierSummary": {"errors": 0, "failures": 0, "skipped": 0, "tests": 1},
    }


def _allocator_wrapper_unsigned(value: dict[str, Any]) -> dict[str, Any]:
    unsigned = dict(value)
    unsigned["receiptSha256"] = "0" * 64
    return unsigned


def seal_allocator_verification_receipt(
    root: Path,
    java_verification: bytes,
    java_verification_path: object,
    junit_xml: bytes,
    junit_xml_path: object,
    tested_commit: str,
    external_paths: list[tuple[str, object]],
) -> dict[str, Any]:
    expected_java_path = ALLOCATOR_FORMAL_PREFIX / tested_commit / "raw-verification.json"
    if _allocator_external_path(java_verification_path, "allocator Java verification path") != expected_java_path:
        raise ChildError("allocator Java verification formal path differs")
    if _allocator_external_path(junit_xml_path, "allocator verifier JUnit path") != ALLOCATOR_VERIFIER_JUNIT_PATH:
        raise ChildError("allocator verifier JUnit formal path differs")
    locked_mode, _ = source_bindings(root, tested_commit)
    if locked_mode == "UNSELECTED":
        raise ChildError("allocator evidence requires a selected source-lock mode")
    external_rows = allocator_external_file_rows(root, tested_commit, external_paths)
    _validate_java_allocator_verification(
        java_verification, junit_xml, root, tested_commit, locked_mode, external_rows
    )
    receipt: dict[str, Any] = {
        "allocatorVerificationBase64": base64.b64encode(java_verification).decode("ascii"),
        "allocatorVerificationPath": str(expected_java_path),
        "allocatorVerificationSha256": sha256(java_verification),
        "externalFiles": external_rows,
        "receiptSha256": "0" * 64,
        "schema": SEALED_ALLOCATOR_VERIFICATION_SCHEMA,
        "testedCommit": tested_commit,
        "verifierJunitXmlBase64": base64.b64encode(junit_xml).decode("ascii"),
        "verifierJunitXmlPath": str(ALLOCATOR_VERIFIER_JUNIT_PATH),
        "verifierJunitXmlSha256": sha256(junit_xml),
    }
    receipt["receiptSha256"] = sha256(canonical_bytes(receipt))
    return receipt


def validate_allocator_verification(
    value: object,
    root: Path,
    tested_commit: str,
) -> dict[str, Any]:
    wrapper = _exact_members(
        value,
        {
            "allocatorVerificationBase64",
            "allocatorVerificationPath",
            "allocatorVerificationSha256",
            "externalFiles",
            "receiptSha256",
            "schema",
            "testedCommit",
            "verifierJunitXmlBase64",
            "verifierJunitXmlPath",
            "verifierJunitXmlSha256",
        },
        "governed allocator verification",
    )
    if (
        wrapper["schema"] != SEALED_ALLOCATOR_VERIFICATION_SCHEMA
        or wrapper["testedCommit"] != tested_commit
        or wrapper["allocatorVerificationPath"]
        != str(ALLOCATOR_FORMAL_PREFIX / tested_commit / "raw-verification.json")
        or wrapper["verifierJunitXmlPath"] != str(ALLOCATOR_VERIFIER_JUNIT_PATH)
    ):
        raise ChildError("governed allocator verification schema/source differs")
    java_verification = _decode_sealed_bytes(
        wrapper["allocatorVerificationBase64"],
        "governed allocator Java verification",
        ALLOCATOR_VERIFICATION_MAX_BYTES,
    )
    junit_xml = _decode_sealed_bytes(
        wrapper["verifierJunitXmlBase64"],
        "governed allocator verifier JUnit XML",
        MAX_NATIVE_JUNIT_XML_BYTES,
    )
    if (
        wrapper["allocatorVerificationSha256"] != sha256(java_verification)
        or wrapper["verifierJunitXmlSha256"] != sha256(junit_xml)
    ):
        raise ChildError("governed allocator verification embedded bytes/SHA differ")
    _sha_text(wrapper["receiptSha256"], "governed allocator verification receipt SHA")
    if wrapper["receiptSha256"] != sha256(canonical_bytes(_allocator_wrapper_unsigned(wrapper))):
        raise ChildError("governed allocator verification self-hash differs")
    locked_mode, _ = source_bindings(root, tested_commit)
    if locked_mode == "UNSELECTED":
        raise ChildError("allocator evidence requires a selected source-lock mode")
    return _validate_java_allocator_verification(
        java_verification,
        junit_xml,
        root,
        tested_commit,
        locked_mode,
        wrapper["externalFiles"],
    )


class _AllocatorV2Cursor:
    def __init__(self, raw: bytes, label: str) -> None:
        self.raw = raw
        self.label = label
        self.offset = 0

    def take(self, count: int) -> bytes:
        if count < 0 or self.offset + count > len(self.raw):
            raise ChildError(f"{self.label} is truncated")
        value = self.raw[self.offset : self.offset + count]
        self.offset += count
        return value

    def unsigned(self, width: int) -> int:
        return int.from_bytes(self.take(width), "big", signed=False)

    def nonnegative_long(self) -> int:
        value = self.unsigned(8)
        if value > ALLOCATOR_V2_LONG_MAX:
            raise ChildError(f"{self.label} contains a negative Java long")
        return value

    def finish(self) -> None:
        if self.offset != len(self.raw):
            raise ChildError(f"{self.label} has trailing bytes")


def _allocator_v2_logical_cells() -> list[dict[str, int]]:
    cells: list[dict[str, int]] = []
    for candidate in ALLOCATOR_V2_CANDIDATES:
        base = 0 if candidate == 0 else 48 + (candidate - 1) * 48
        for population_index, population in enumerate(ALLOCATOR_V2_POPULATIONS):
            for latency_index, latency in enumerate(ALLOCATOR_V2_LATENCIES_MILLIS):
                for rate, rate_index in zip(
                    ALLOCATOR_V2_DESCENDING_RATES, (5, 4, 3, 2, 1, 0), strict=True
                ):
                    cells.append(
                        {
                            "candidate": candidate,
                            "contextId": base
                            + population_index * 24
                            + latency_index * 6
                            + rate_index,
                            "latency": latency,
                            "population": population,
                            "rate": rate,
                        }
                    )
    context_ids = [cell["contextId"] for cell in cells]
    if len(context_ids) != 288 or len(set(context_ids)) != 288:
        raise AssertionError("allocator V2 logical context inventory differs")
    return cells


def _allocator_v2_context_ids() -> list[int]:
    return [cell["contextId"] for cell in _allocator_v2_logical_cells()]


def _allocator_v2_rows(candidate: int) -> list[tuple[int, int, int]]:
    return [
        (candidate, population, latency)
        for population in ALLOCATOR_V2_POPULATIONS
        for latency in ALLOCATOR_V2_LATENCIES_MILLIS
    ]


def _allocator_v2_complete_zero_failure(observation: dict[str, Any]) -> bool:
    cell = observation["cell"]
    values = observation["values"]
    expected = cell["rate"] * 30
    return (
        values[0] == expected
        and values[1] == expected
        and values[2] == 0
        and values[3] == expected
        and values[4] == 0
        and values[5] == 0
        and all(values[index] == 0 for index in (7, 8, 9, 10, 11, 18, 19, 20))
    )


def _allocator_v2_absolute_candidate_bounds_pass(observation: dict[str, Any]) -> bool:
    cell = observation["cell"]
    values = observation["values"]
    return (
        _allocator_v2_complete_zero_failure(observation)
        and values[12] <= 250_000
        and values[13] <= 250_000
        and values[14] <= 1_000_000
        and values[15] <= 2 * cell["rate"]
        and values[16] <= 2_000_000
        and values[17] <= 2_000_000
    )


def _allocator_v2_fault_bounds_pass(observation: dict[str, Any]) -> bool:
    population = observation["row"][1]
    values = observation["values"]
    recovery_bound = 30_000_000 if population == 10_000 else 60_000_000
    return (
        observation["cuts"] == 0x1FF
        and all(values[index] == 0 for index in range(8))
        and values[8] <= 1
        and values[9] <= recovery_bound
    )


class _AllocatorV2Planner:
    """Independent Python transcription of AllocatorCampaignPlannerV2."""

    def __init__(self) -> None:
        self.cells = _allocator_v2_logical_cells()
        self.cell_by_context = {cell["contextId"]: cell for cell in self.cells}
        self.native_rows = _allocator_v2_rows(0)
        self.native_results: dict[tuple[int, int, int], dict[str, Any]] = {}
        self.dispositions: list[tuple[int, int, tuple[int, ...]]] = []
        self.disposition_contexts: set[int] = set()
        self.qualified_candidates: list[int] = []
        self.current_row_contexts: list[int] = []
        self.current_candidate_qualification_contexts: list[int] = []
        self.phase = "NATIVE"
        self.native_row_index = 0
        self.native_rate_index = 0
        self.candidate = 1
        self.candidate_row_index = 0
        self.candidate_eligible_rate_index = 0
        self.candidate_eliminated = False
        self.elimination_dependency: int | None = None
        self.pending_sustainable: dict[str, Any] | None = None
        self.executed_performance_cells = 0

    def _cell(self, candidate: int, population: int, latency: int, rate: int) -> dict[str, int]:
        base = 0 if candidate == 0 else 48 + (candidate - 1) * 48
        population_index = ALLOCATOR_V2_POPULATIONS.index(population)
        latency_index = ALLOCATOR_V2_LATENCIES_MILLIS.index(latency)
        rate_index = (5, 4, 3, 2, 1, 0)[ALLOCATOR_V2_DESCENDING_RATES.index(rate)]
        return self.cell_by_context[
            base + population_index * 24 + latency_index * 6 + rate_index
        ]

    def _disposition(
        self, cell: dict[str, int], kind: int, dependencies: list[int] | tuple[int, ...]
    ) -> None:
        context = cell["contextId"]
        if context in self.disposition_contexts:
            raise ChildError("allocator V2 deterministic planner produced a duplicate disposition")
        self.disposition_contexts.add(context)
        self.dispositions.append((context, kind, tuple(dependencies)))

    @staticmethod
    def _eligible_rates(native_sustainable_rate: int) -> list[int]:
        eligible = [
            rate
            for rate in ALLOCATOR_V2_DESCENDING_RATES
            if rate * 100 >= native_sustainable_rate * 80
        ]
        if not eligible:
            raise ChildError("allocator V2 native sustainable rate has no relative candidate rate")
        return eligible

    def next_action(self) -> tuple[str, Any] | None:
        while True:
            if self.phase == "COMPLETE":
                return None
            if self.phase == "NATIVE":
                if self.native_row_index == len(self.native_rows):
                    self.phase = "CANDIDATE"
                    continue
                _, population, latency = self.native_rows[self.native_row_index]
                return (
                    "interval",
                    self._cell(
                        0,
                        population,
                        latency,
                        ALLOCATOR_V2_DESCENDING_RATES[self.native_rate_index],
                    ),
                )
            if self.candidate >= len(ALLOCATOR_V2_CANDIDATES):
                self.phase = "COMPLETE"
                continue
            rows = _allocator_v2_rows(self.candidate)
            if self.candidate_eliminated:
                self._disposition_remaining_candidate(rows)
                self._advance_candidate()
                continue
            if self.candidate_row_index == len(rows):
                self.qualified_candidates.append(self.candidate)
                if self.candidate >= 2:
                    self._disposition_larger_ranges()
                    self.phase = "COMPLETE"
                else:
                    self._advance_candidate()
                continue
            _, population, latency = rows[self.candidate_row_index]
            native_result = self.native_results.get((0, population, latency))
            if native_result is None:
                raise ChildError("allocator V2 candidate row precedes its native baseline")
            if native_result["sustainableRate"] == 0:
                for rate in ALLOCATOR_V2_DESCENDING_RATES:
                    self._disposition(
                        self._cell(self.candidate, population, latency, rate),
                        3,
                        native_result["executedContexts"],
                    )
                self.candidate_eliminated = True
                self.elimination_dependency = native_result["executedContexts"][-1]
                self.candidate_row_index += 1
                continue
            if self.pending_sustainable is not None:
                return ("fault", (self.candidate, population, latency))
            eligible = self._eligible_rates(native_result["sustainableRate"])
            self._disposition_rates_below_floor(population, latency, native_result, eligible)
            return (
                "interval",
                self._cell(
                    self.candidate,
                    population,
                    latency,
                    eligible[self.candidate_eligible_rate_index],
                ),
            )

    def accept(self, observation: dict[str, Any]) -> None:
        if observation["tag"] == "interval":
            self.executed_performance_cells += 1
            self.current_row_contexts.append(observation["cell"]["contextId"])
            if self.phase == "NATIVE":
                self._accept_native(observation)
            elif self.phase == "CANDIDATE":
                self._accept_candidate(observation)
            else:
                raise ChildError("allocator V2 interval occurs after campaign completion")
            return
        if self.pending_sustainable is None:
            raise ChildError("allocator V2 fault row has no sustainable interval dependency")
        dependency = self.pending_sustainable["cell"]["contextId"]
        if not _allocator_v2_fault_bounds_pass(observation):
            self._eliminate_candidate(dependency)
            return
        self.current_candidate_qualification_contexts.append(dependency)
        self.candidate_row_index += 1
        self.candidate_eligible_rate_index = 0
        self.pending_sustainable = None
        self.current_row_contexts.clear()

    def _accept_native(self, observation: dict[str, Any]) -> None:
        cell = observation["cell"]
        if _allocator_v2_complete_zero_failure(observation):
            for lower_index in range(
                self.native_rate_index + 1, len(ALLOCATOR_V2_DESCENDING_RATES)
            ):
                self._disposition(
                    self._cell(0, cell["population"], cell["latency"], ALLOCATOR_V2_DESCENDING_RATES[lower_index]),
                    0,
                    [cell["contextId"]],
                )
            self.native_results[(0, cell["population"], cell["latency"])] = {
                "appendStallP99Micros": observation["values"][17],
                "executedContexts": list(self.current_row_contexts),
                "sustainableRate": cell["rate"],
            }
            self._advance_native_row()
        elif self.native_rate_index + 1 == len(ALLOCATOR_V2_DESCENDING_RATES):
            self.native_results[(0, cell["population"], cell["latency"])] = {
                "appendStallP99Micros": 0,
                "executedContexts": list(self.current_row_contexts),
                "sustainableRate": 0,
            }
            self._advance_native_row()
        else:
            self.native_rate_index += 1

    def _accept_candidate(self, observation: dict[str, Any]) -> None:
        cell = observation["cell"]
        native_result = self.native_results[(0, cell["population"], cell["latency"])]
        eligible = self._eligible_rates(native_result["sustainableRate"])
        if not _allocator_v2_absolute_candidate_bounds_pass(observation):
            if self.candidate_eligible_rate_index + 1 < len(eligible):
                self.candidate_eligible_rate_index += 1
                return
            self._eliminate_candidate(cell["contextId"])
            return
        for lower_index in range(self.candidate_eligible_rate_index + 1, len(eligible)):
            self._disposition(
                self._cell(
                    cell["candidate"],
                    cell["population"],
                    cell["latency"],
                    eligible[lower_index],
                ),
                2,
                [cell["contextId"]],
            )
        if observation["values"][17] > native_result["appendStallP99Micros"] + 250_000:
            self._eliminate_candidate(cell["contextId"])
            return
        self.pending_sustainable = observation

    def _advance_native_row(self) -> None:
        self.native_row_index += 1
        self.native_rate_index = 0
        self.current_row_contexts.clear()

    def _advance_candidate(self) -> None:
        self.candidate += 1
        self.candidate_row_index = 0
        self.candidate_eligible_rate_index = 0
        self.candidate_eliminated = False
        self.elimination_dependency = None
        self.pending_sustainable = None
        self.current_row_contexts.clear()
        self.current_candidate_qualification_contexts.clear()

    def _eliminate_candidate(self, dependency: int) -> None:
        self.candidate_eliminated = True
        self.elimination_dependency = dependency
        self.pending_sustainable = None
        self.candidate_row_index += 1
        self.candidate_eligible_rate_index = 0
        self.current_row_contexts.clear()

    def _disposition_rates_below_floor(
        self,
        population: int,
        latency: int,
        native_result: dict[str, Any],
        eligible: list[int],
    ) -> None:
        for rate in ALLOCATOR_V2_DESCENDING_RATES:
            cell = self._cell(self.candidate, population, latency, rate)
            if rate not in eligible and cell["contextId"] not in self.disposition_contexts:
                self._disposition(cell, 1, [native_result["executedContexts"][-1]])

    def _disposition_remaining_candidate(self, rows: list[tuple[int, int, int]]) -> None:
        if self.elimination_dependency is None:
            raise ChildError("allocator V2 eliminated candidate has no executed dependency")
        for _, population, latency in rows[self.candidate_row_index :]:
            for rate in ALLOCATOR_V2_DESCENDING_RATES:
                cell = self._cell(self.candidate, population, latency, rate)
                if cell["contextId"] not in self.disposition_contexts:
                    self._disposition(cell, 4, [self.elimination_dependency])

    def _disposition_larger_ranges(self) -> None:
        if len(self.current_candidate_qualification_contexts) != 8:
            raise ChildError("allocator V2 qualified RANGE does not bind all eight row intervals")
        for candidate in range(self.candidate + 1, len(ALLOCATOR_V2_CANDIDATES)):
            for _, population, latency in _allocator_v2_rows(candidate):
                for rate in ALLOCATOR_V2_DESCENDING_RATES:
                    self._disposition(
                        self._cell(candidate, population, latency, rate),
                        5,
                        self.current_candidate_qualification_contexts,
                    )


def _allocator_v2_recompute(observations: list[dict[str, Any]]) -> dict[str, Any]:
    planner = _AllocatorV2Planner()
    for observation in observations:
        expected = planner.next_action()
        if expected is None or expected[0] != observation["tag"]:
            raise ChildError("allocator V2 observation differs from deterministic planner action")
        if observation["tag"] == "interval":
            if expected[1]["contextId"] != observation["cell"]["contextId"]:
                raise ChildError("allocator V2 interval observation is reordered or for another cell")
        elif expected[1] != observation["row"]:
            raise ChildError("allocator V2 fault observation is reordered or for another row")
        planner.accept(observation)
    completed = planner.next_action() is None
    if completed and planner.executed_performance_cells + len(planner.dispositions) != 288:
        raise ChildError("allocator V2 terminal deterministic plan does not account for 288 cells")
    return {
        "completed": completed,
        "dispositions": planner.dispositions,
        "executedPerformanceCells": planner.executed_performance_cells,
        "qualifiedCandidates": planner.qualified_candidates,
    }


def _allocator_v2_source(cursor: _AllocatorV2Cursor) -> dict[str, str]:
    try:
        commit = cursor.take(40).decode("ascii")
    except UnicodeDecodeError as error:
        raise ChildError(f"{cursor.label} Nereus commit is not ASCII") from error
    if not COMMIT_PATTERN.fullmatch(commit):
        raise ChildError(f"{cursor.label} Nereus commit is not canonical")
    return {
        "nereusCommit": commit,
        "oxiaImageDigest": cursor.take(32).hex(),
        "dependencyLockDigest": cursor.take(32).hex(),
        "executorDigest": cursor.take(32).hex(),
        "workloadDigest": cursor.take(32).hex(),
    }


def _allocator_v2_campaign_id(source: dict[str, str], context_ids: list[int]) -> str:
    digest = hashlib.sha256()
    digest.update(b"NEREUS-V2-M3-ALLOCATOR-CAMPAIGN-ID-V2")
    digest.update(source["nereusCommit"].encode("ascii"))
    for name in (
        "oxiaImageDigest",
        "dependencyLockDigest",
        "executorDigest",
        "workloadDigest",
    ):
        digest.update(bytes.fromhex(source[name]))
    digest.update((2).to_bytes(4, "big"))
    for context_id in context_ids:
        digest.update(context_id.to_bytes(4, "big"))
    return digest.hexdigest()


def _allocator_v2_attachment_root(digests: list[str]) -> str:
    digest = hashlib.sha256()
    digest.update(b"NEREUS-V2-M3-ALLOCATOR-ATTACHMENT-ROOT-V2")
    for value in digests:
        digest.update(bytes.fromhex(value))
    return digest.hexdigest()


def _parse_allocator_v2_checkpoint(raw: bytes) -> dict[str, Any]:
    if not raw or len(raw) > ALLOCATOR_V2_CHECKPOINT_MAX_BYTES:
        raise ChildError("allocator V2 NACP2 bytes are empty or over cap")
    cursor = _AllocatorV2Cursor(raw, "allocator V2 NACP2")
    if cursor.take(8) != b"NACP2\0\0\0" or cursor.unsigned(2) != 2:
        raise ChildError("allocator V2 NACP2 magic/version differs")
    status = cursor.unsigned(1)
    if status != 1 or cursor.unsigned(1) != 0:
        raise ChildError("allocator V2 NACP2 is not a completed canonical checkpoint")
    sequence = cursor.nonnegative_long()
    source = _allocator_v2_source(cursor)
    campaign_id = cursor.take(32).hex()
    predecessor = cursor.take(32).hex()
    if (sequence == 0) != (predecessor == "0" * 64):
        raise ChildError("allocator V2 NACP2 predecessor lineage differs")
    budget_bounds = (900, 5_400, 7_200, 5_400, 11_520, 1_440, 600)
    budgets = tuple(cursor.nonnegative_long() for _ in budget_bounds)
    if any(value > maximum for value, maximum in zip(budgets, budget_bounds, strict=True)):
        raise ChildError("allocator V2 NACP2 remaining phase budget exceeds its frozen cap")
    expected_contexts = _allocator_v2_context_ids()
    if cursor.unsigned(4) != len(expected_contexts):
        raise ChildError("allocator V2 NACP2 logical inventory count differs")
    actual_contexts = [cursor.unsigned(4) for _ in expected_contexts]
    if actual_contexts != expected_contexts:
        raise ChildError("allocator V2 NACP2 logical inventory order differs")
    known_contexts = set(expected_contexts)
    record_count = cursor.unsigned(4)
    if not 1 <= record_count <= ALLOCATOR_V2_MAX_EXTERNAL_ATTACHMENTS:
        raise ChildError("allocator V2 NACP2 execution record count differs")
    attachment_digests: list[str] = []
    executed_contexts: set[int] = set()
    fault_rows: set[tuple[int, int, int]] = set()
    observations: list[dict[str, Any]] = []
    interval_count = 0
    cells_by_context = {
        cell["contextId"]: cell for cell in _allocator_v2_logical_cells()
    }
    for _ in range(record_count):
        tag = cursor.unsigned(1)
        if tag == 1:
            context_id = cursor.unsigned(4)
            if context_id not in known_contexts or context_id in executed_contexts:
                raise ChildError("allocator V2 NACP2 interval context is unknown or duplicated")
            executed_contexts.add(context_id)
            values = [cursor.nonnegative_long() for _ in range(21)]
            offered, admitted, dropped, completed, failed, timed_out, terminal = values[:7]
            if (
                offered != dropped + completed + failed + timed_out
                or admitted != completed + failed + timed_out
                or terminal != admitted
            ):
                raise ChildError("allocator V2 NACP2 interval conservation differs")
            observations.append(
                {
                    "cell": cells_by_context[context_id],
                    "tag": "interval",
                    "values": values,
                }
            )
            interval_count += 1
        elif tag == 2:
            candidate = cursor.unsigned(1)
            population = cursor.unsigned(4)
            latency = cursor.unsigned(4)
            cuts = cursor.unsigned(4)
            row = (candidate, population, latency)
            if (
                candidate not in {1, 2, 3, 4, 5}
                or population not in {10_000, 100_000}
                or latency not in {1, 5, 10, 25}
                or cuts & ~0x1FF != 0
                or row in fault_rows
            ):
                raise ChildError("allocator V2 NACP2 fault row/cut inventory differs")
            fault_rows.add(row)
            values = [cursor.nonnegative_long() for _ in range(10)]
            observations.append(
                {"cuts": cuts, "row": row, "tag": "fault", "values": values}
            )
        else:
            raise ChildError("allocator V2 NACP2 observation tag differs")
        attachment = cursor.take(32).hex()
        if attachment == "0" * 64 or attachment in attachment_digests:
            raise ChildError("allocator V2 NACP2 execution attachment digest is zero or aliased")
        attachment_digests.append(attachment)
    disposition_count = cursor.unsigned(4)
    if disposition_count > 288:
        raise ChildError("allocator V2 NACP2 disposition count exceeds the logical inventory")
    disposition_contexts: set[int] = set()
    dispositions: list[tuple[int, int, tuple[int, ...]]] = []
    for _ in range(disposition_count):
        context_id = cursor.unsigned(4)
        kind = cursor.unsigned(1)
        dependency_count = cursor.unsigned(2)
        dependencies = [cursor.unsigned(4) for _ in range(dependency_count)]
        if (
            context_id not in known_contexts
            or context_id in executed_contexts
            or context_id in disposition_contexts
            or kind > 5
            or not 1 <= dependency_count <= record_count
            or len(set(dependencies)) != len(dependencies)
            or any(dependency not in executed_contexts for dependency in dependencies)
        ):
            raise ChildError("allocator V2 NACP2 disposition envelope differs")
        disposition_contexts.add(context_id)
        dispositions.append((context_id, kind, tuple(dependencies)))
    cursor.finish()
    recomputed = _allocator_v2_recompute(observations)
    if not recomputed["completed"]:
        raise ChildError("allocator V2 NACP2 completed checkpoint has a required action")
    if dispositions != recomputed["dispositions"]:
        raise ChildError(
            "allocator V2 NACP2 caller dispositions differ from deterministic recomputation"
        )
    if (
        interval_count != recomputed["executedPerformanceCells"]
        or interval_count + disposition_count != 288
    ):
        raise ChildError("allocator V2 NACP2 executed/disposition cell conservation differs")
    if campaign_id != _allocator_v2_campaign_id(source, expected_contexts):
        raise ChildError("allocator V2 NACP2 campaign identity differs from its source tuple")
    return {
        "attachmentDigests": attachment_digests,
        "attachmentRootDigest": _allocator_v2_attachment_root(attachment_digests),
        "campaignId": campaign_id,
        "checkpointDigest": sha256(raw),
        "dispositionCells": disposition_count,
        "executedPerformanceCells": interval_count,
        "qualifiedCandidates": recomputed["qualifiedCandidates"],
        "source": source,
    }


def _parse_allocator_v2_evaluation(raw: bytes, checkpoint: dict[str, Any]) -> dict[str, Any]:
    if len(raw) != ALLOCATOR_V2_NAEV_BYTES:
        raise ChildError("allocator V2 NAEV2 fixed bytes differ")
    cursor = _AllocatorV2Cursor(raw, "allocator V2 NAEV2")
    if cursor.take(8) != b"NAEV2\0\0\0" or cursor.unsigned(2) != 2:
        raise ChildError("allocator V2 NAEV2 magic/version differs")
    status = cursor.unsigned(1)
    selected = cursor.unsigned(1)
    executed = cursor.unsigned(4)
    dispositions = cursor.unsigned(4)
    source = _allocator_v2_source(cursor)
    campaign_id = cursor.take(32).hex()
    checkpoint_digest = cursor.take(32).hex()
    attachment_root = cursor.take(32).hex()
    cursor.finish()
    qualified = checkpoint["qualifiedCandidates"]
    strict = 1 in qualified
    ranges = [candidate for candidate in qualified if candidate >= 2]
    selected_range = min(ranges) if ranges else None
    if strict and selected_range is None:
        recomputed_status, recomputed_selected = 0, 1
    elif not strict and selected_range is not None:
        recomputed_status, recomputed_selected = 1, selected_range
    elif not strict:
        recomputed_status, recomputed_selected = 2, 255
    else:
        recomputed_status, recomputed_selected = 3, 255
    eligible = status in {0, 1}
    if (
        status not in {0, 1, 2, 3}
        or eligible != (selected != 255)
        or (status == 0 and selected != 1)
        or (status == 1 and selected not in {2, 3, 4, 5})
        or (status in {2, 3} and selected != 255)
        or status != recomputed_status
        or selected != recomputed_selected
        or executed + dispositions != 288
        or executed != checkpoint["executedPerformanceCells"]
        or dispositions != checkpoint["dispositionCells"]
        or source != checkpoint["source"]
        or campaign_id != checkpoint["campaignId"]
        or checkpoint_digest != checkpoint["checkpointDigest"]
        or attachment_root != checkpoint["attachmentRootDigest"]
    ):
        raise ChildError("allocator V2 NAEV2 accounting/selection/link differs")
    return {
        "evaluationDigest": sha256(raw),
        "selectedCandidate": selected,
        "status": status,
    }


def _allocator_v2_junit_summary(raw: bytes, diagnostic: bool) -> dict[str, int]:
    if (
        not raw
        or len(raw) > ALLOCATOR_V2_JUNIT_MAX_BYTES
        or b"<!DOCTYPE" in raw.upper()
        or b"<!ENTITY" in raw.upper()
    ):
        raise ChildError("allocator V2 JUnit XML is empty, unsafe, or over cap")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as error:
        raise ChildError(f"cannot parse allocator V2 JUnit XML: {error}") from error
    if root.tag != "testsuite":
        raise ChildError("allocator V2 JUnit root must be one exact testsuite")
    cases = root.findall("testcase")
    names: set[str] = set()
    observed = {"tests": len(cases), "failures": 0, "errors": 0, "skipped": 0}
    for case in cases:
        name = case.attrib.get("name")
        if not name or name in names:
            raise ChildError("allocator V2 JUnit testcase name is absent or duplicated")
        names.add(name)
        for field in ("failure", "error", "skipped"):
            if case.find(field) is not None:
                observed["skipped" if field == "skipped" else field + "s"] += 1
    declared: dict[str, int] = {}
    for field in ("tests", "failures", "errors", "skipped"):
        try:
            declared[field] = int(root.attrib[field])
        except (KeyError, ValueError) as error:
            raise ChildError(f"allocator V2 JUnit declared {field} is invalid") from error
    if declared != observed or declared["tests"] <= 0 or any(
        declared[field] != 0 for field in ("failures", "errors", "skipped")
    ):
        raise ChildError("allocator V2 JUnit counters are empty, nonzero, or differ from testcase nodes")
    if diagnostic and names != ALLOCATOR_V2_DIAGNOSTIC_TESTS:
        raise ChildError("allocator V2 diagnostic JUnit testcase inventory differs")
    return declared


def _parse_allocator_v2_diagnostic(
    raw: bytes, checkpoint: dict[str, Any], diagnostic_junit: bytes
) -> None:
    if len(raw) != ALLOCATOR_V2_NADV_BYTES:
        raise ChildError("allocator V2 NADV2 fixed bytes differ")
    cursor = _AllocatorV2Cursor(raw, "allocator V2 NADV2")
    if cursor.take(8) != b"NADV2\0\0\0" or cursor.unsigned(2) != 2:
        raise ChildError("allocator V2 NADV2 magic/version differs")
    if cursor.unsigned(1) != 0x0F or cursor.unsigned(1) != 0:
        raise ChildError("allocator V2 NADV2 diagnostic mask/reserved byte differs")
    source = _allocator_v2_source(cursor)
    receipt_digest = cursor.take(32).hex()
    cursor.finish()
    if source != checkpoint["source"] or receipt_digest != sha256(diagnostic_junit):
        raise ChildError("allocator V2 NADV2 source or diagnostic JUnit link differs")


def _allocator_v2_embedded(
    wrapper: dict[str, Any], prefix: str, maximum: int
) -> bytes:
    raw = _decode_sealed_bytes(
        wrapper[f"{prefix}Base64"], f"allocator V2 {prefix}", maximum
    )
    if wrapper[f"{prefix}Sha256"] != sha256(raw):
        raise ChildError(f"allocator V2 {prefix} embedded bytes/SHA differ")
    return raw


def _allocator_v2_file_rows(
    root: Path,
    rows: object,
    label: str,
    maximum_count: int,
    required_names: tuple[str, ...] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    if not isinstance(rows, list) or not 1 <= len(rows) <= maximum_count:
        raise ChildError(f"{label} file count is outside its cap")
    validated: list[dict[str, Any]] = []
    digests: list[str] = []
    for index, value in enumerate(rows):
        members = {"bytes", "path", "sha256"}
        if required_names is not None:
            members.add("name")
        row = _exact_members(value, members, f"{label}[{index}]")
        path = _allocator_external_path(row["path"], f"{label}[{index}].path", True)
        size, digest, _, _ = _allocator_stream_identity(
            root, path, ALLOCATOR_V2_EXTERNAL_MAX_BYTES, allow_absolute=True
        )
        if row["bytes"] != size or row["sha256"] != digest:
            raise ChildError(f"{label}[{index}] bytes/SHA differ from the exact file")
        validated.append(row)
        digests.append(digest)
    paths = [row["path"] for row in validated]
    if len(paths) != len(set(paths)) or (required_names is None and paths != sorted(paths)):
        raise ChildError(f"{label} paths are duplicated or unsorted")
    if required_names is not None:
        names = [row["name"] for row in validated]
        if names != list(required_names):
            raise ChildError(f"{label} logical-name inventory differs")
    return validated, digests


def _allocator_v2_expected_source(
    root: Path,
    tested_commit: str,
    source_files: list[dict[str, Any]],
) -> dict[str, str]:
    locks = _source_locks(root, tested_commit)
    allocator = _required_object(locks.get("m3AllocatorEvidenceBinding"), "m3AllocatorEvidenceBinding")
    image = _required_string(
        allocator.get("oxiaServerImageDigest"), "m3AllocatorEvidenceBinding.oxiaServerImageDigest"
    )
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
        raise ChildError("allocator V2 locked Oxia image digest differs")
    source_locks_raw = git(root, "show", f"{tested_commit}:{FINAL.SOURCE_LOCKS_PATH}")
    assert isinstance(source_locks_raw, bytes)
    by_name = {row["name"]: row for row in source_files}
    return {
        "nereusCommit": tested_commit,
        "oxiaImageDigest": image.removeprefix("sha256:"),
        "dependencyLockDigest": sha256(source_locks_raw),
        "executorDigest": by_name["executorArtifact"]["sha256"],
        "workloadDigest": by_name["workloadPlan"]["sha256"],
    }


def _allocator_v2_unsigned(value: dict[str, Any]) -> dict[str, Any]:
    unsigned = dict(value)
    unsigned["receiptSha256"] = "0" * 64
    return unsigned


def validate_allocator_v2_campaign_verification(
    value: object, root: Path, tested_commit: str
) -> dict[str, Any]:
    members = {
        "checkpointBase64",
        "checkpointSha256",
        "diagnosticBase64",
        "diagnosticJunitBase64",
        "diagnosticJunitSha256",
        "diagnosticSha256",
        "evaluationBase64",
        "evaluationSha256",
        "executionAttachments",
        "formalJunitBase64",
        "formalJunitSha256",
        "promotionDecisionBase64",
        "promotionDecisionSha256",
        "receiptSha256",
        "schema",
        "sourceFiles",
        "testedCommit",
    }
    wrapper = _exact_members(value, members, "governed allocator V2 campaign verification")
    if wrapper["schema"] != ALLOCATOR_V2_VERIFICATION_SCHEMA or wrapper["testedCommit"] != tested_commit:
        raise ChildError("governed allocator V2 verification schema/source differs")
    _sha_text(wrapper["receiptSha256"], "governed allocator V2 verification receipt SHA")
    if wrapper["receiptSha256"] != sha256(canonical_bytes(_allocator_v2_unsigned(wrapper))):
        raise ChildError("governed allocator V2 verification self-hash differs")
    checkpoint_raw = _allocator_v2_embedded(wrapper, "checkpoint", ALLOCATOR_V2_CHECKPOINT_MAX_BYTES)
    evaluation_raw = _allocator_v2_embedded(wrapper, "evaluation", 4_096)
    diagnostic_raw = _allocator_v2_embedded(wrapper, "diagnostic", 4_096)
    diagnostic_junit = _allocator_v2_embedded(wrapper, "diagnosticJunit", ALLOCATOR_V2_JUNIT_MAX_BYTES)
    formal_junit = _allocator_v2_embedded(wrapper, "formalJunit", ALLOCATOR_V2_JUNIT_MAX_BYTES)
    promotion_raw = _allocator_v2_embedded(wrapper, "promotionDecision", MAX_CANONICAL_BYTES)
    source_files, _ = _allocator_v2_file_rows(
        root,
        wrapper["sourceFiles"],
        "allocator V2 source files",
        2,
        ("executorArtifact", "workloadPlan"),
    )
    execution_files, execution_digests = _allocator_v2_file_rows(
        root,
        wrapper["executionAttachments"],
        "allocator V2 execution attachments",
        ALLOCATOR_V2_MAX_EXTERNAL_ATTACHMENTS,
    )
    del execution_files
    checkpoint = _parse_allocator_v2_checkpoint(checkpoint_raw)
    if checkpoint["source"] != _allocator_v2_expected_source(root, tested_commit, source_files):
        raise ChildError("allocator V2 NACP2 source tuple differs from exact tested locks/files")
    if set(execution_digests) != set(checkpoint["attachmentDigests"]):
        raise ChildError("allocator V2 external attachment set differs from NACP2")
    evaluation = _parse_allocator_v2_evaluation(evaluation_raw, checkpoint)
    diagnostic_summary = _allocator_v2_junit_summary(diagnostic_junit, True)
    formal_summary = _allocator_v2_junit_summary(formal_junit, False)
    _parse_allocator_v2_diagnostic(diagnostic_raw, checkpoint, diagnostic_junit)
    decision = load_canonical_json(
        promotion_raw, "allocator V2 promotion decision", MAX_CANONICAL_BYTES
    )
    decision = _exact_members(
        decision,
        {
            "checkpointSha256",
            "diagnosticJUnitSha256",
            "diagnosticSha256",
            "evaluationSha256",
            "formalJUnitSha256",
            "schema",
            "selectedCandidate",
            "status",
        },
        "allocator V2 promotion decision",
    )
    candidate_names = {1: "STRICT", 2: "RANGE_16", 3: "RANGE_64", 4: "RANGE_256", 5: "RANGE_1024"}
    selected_name = candidate_names.get(evaluation["selectedCandidate"])
    locked_mode, _ = source_bindings(root, tested_commit)
    expected_mode = "STRICT" if selected_name == "STRICT" else "RANGE"
    if (
        evaluation["status"] not in {0, 1}
        or selected_name is None
        or decision
        != {
            "schema": ALLOCATOR_V2_PROMOTION_SCHEMA,
            "status": "PROMOTABLE",
            "selectedCandidate": selected_name,
            "checkpointSha256": sha256(checkpoint_raw),
            "evaluationSha256": sha256(evaluation_raw),
            "diagnosticSha256": sha256(diagnostic_raw),
            "diagnosticJUnitSha256": sha256(diagnostic_junit),
            "formalJUnitSha256": sha256(formal_junit),
        }
        or locked_mode != expected_mode
    ):
        raise ChildError("allocator V2 promotion decision/evaluation/source-lock mode differs")
    return {
        "mode": locked_mode,
        "selectedRangeSize": {
            "STRICT": 1,
            "RANGE_16": 16,
            "RANGE_64": 64,
            "RANGE_256": 256,
            "RANGE_1024": 1024,
        }[selected_name],
        "summary": _sum_summaries([formal_summary, diagnostic_summary]),
    }


def seal_allocator_v2_campaign_verification_receipt(
    root: Path,
    tested_commit: str,
    checkpoint_raw: bytes,
    evaluation_raw: bytes,
    diagnostic_raw: bytes,
    diagnostic_junit: bytes,
    formal_junit: bytes,
    promotion_raw: bytes,
    source_paths: list[tuple[str, object]],
    execution_paths: list[object],
) -> dict[str, Any]:
    if [name for name, _ in source_paths] != ["executorArtifact", "workloadPlan"]:
        raise ChildError("allocator V2 source-file logical-name inventory differs")

    def file_row(name: str | None, raw_path: object) -> dict[str, Any]:
        path = _allocator_external_path(raw_path, "allocator V2 governed external path", True)
        size, digest, _, _ = _allocator_stream_identity(
            root, path, ALLOCATOR_V2_EXTERNAL_MAX_BYTES, allow_absolute=True
        )
        row: dict[str, Any] = {"bytes": size, "path": str(path), "sha256": digest}
        if name is not None:
            row["name"] = name
        return row

    source_files = [file_row(name, path) for name, path in source_paths]
    execution_files = sorted(
        (file_row(None, path) for path in execution_paths), key=lambda row: row["path"]
    )
    promotion_value = load_external_json(
        promotion_raw, "allocator V2 promotion decision", MAX_CANONICAL_BYTES
    )
    promotion_raw = canonical_bytes(promotion_value)
    receipt: dict[str, Any] = {
        "checkpointBase64": base64.b64encode(checkpoint_raw).decode("ascii"),
        "checkpointSha256": sha256(checkpoint_raw),
        "diagnosticBase64": base64.b64encode(diagnostic_raw).decode("ascii"),
        "diagnosticJunitBase64": base64.b64encode(diagnostic_junit).decode("ascii"),
        "diagnosticJunitSha256": sha256(diagnostic_junit),
        "diagnosticSha256": sha256(diagnostic_raw),
        "evaluationBase64": base64.b64encode(evaluation_raw).decode("ascii"),
        "evaluationSha256": sha256(evaluation_raw),
        "executionAttachments": execution_files,
        "formalJunitBase64": base64.b64encode(formal_junit).decode("ascii"),
        "formalJunitSha256": sha256(formal_junit),
        "promotionDecisionBase64": base64.b64encode(promotion_raw).decode("ascii"),
        "promotionDecisionSha256": sha256(promotion_raw),
        "receiptSha256": "0" * 64,
        "schema": ALLOCATOR_V2_VERIFICATION_SCHEMA,
        "sourceFiles": source_files,
        "testedCommit": tested_commit,
    }
    receipt["receiptSha256"] = sha256(canonical_bytes(receipt))
    validate_allocator_v2_campaign_verification(receipt, root, tested_commit)
    return receipt


def _allocator_v5_file_rows(
    root: Path,
    rows: object,
    label: str,
    maximum_count: int,
    maximum_bytes: int,
    required_names: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(rows, list) or not 1 <= len(rows) <= maximum_count:
        raise ChildError(f"{label} file count is outside its cap")
    validated: list[dict[str, Any]] = []
    for index, value in enumerate(rows):
        members = {"bytes", "path", "sha256"}
        if required_names is not None:
            members.add("name")
        row = _exact_members(value, members, f"{label}[{index}]")
        path = _allocator_external_path(row["path"], f"{label}[{index}].path", True)
        size, digest, _, _ = _allocator_stream_identity(
            root, path, maximum_bytes, allow_absolute=True
        )
        if row["bytes"] != size or row["sha256"] != digest:
            raise ChildError(f"{label}[{index}] bytes/SHA differ from the exact file")
        validated.append(row)
    paths = [row["path"] for row in validated]
    if len(paths) != len(set(paths)):
        raise ChildError(f"{label} paths are duplicated")
    if required_names is None:
        if paths != sorted(paths):
            raise ChildError(f"{label} paths are not sorted")
    elif [row["name"] for row in validated] != list(required_names):
        raise ChildError(f"{label} logical-name inventory differs")
    return validated


def _allocator_v5_embedded(
    wrapper: dict[str, Any], prefix: str, maximum: int
) -> bytes:
    raw = _decode_sealed_bytes(
        wrapper[f"{prefix}Base64"], f"allocator V5 {prefix}", maximum
    )
    if wrapper[f"{prefix}Sha256"] != sha256(raw):
        raise ChildError(f"allocator V5 {prefix} embedded bytes/SHA differ")
    return raw


def _allocator_v5_dependency_lock(root: Path, tested_commit: str) -> str:
    paths = (
        "gradle/libs.versions.toml",
        "gradle/wrapper/gradle-wrapper.properties",
        "gradle/locked-artifacts/oxia-client-java/"
        "091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2/io/github/oxia-db/"
        "oxia-client/0.9.4/oxia-client-0.9.4.jar",
    )
    manifest = bytearray()
    for path in paths:
        raw = git(root, "show", f"{tested_commit}:{path}")
        assert isinstance(raw, bytes)
        manifest.extend(path.encode("ascii"))
        manifest.extend(b"\0")
        manifest.extend(sha256(raw).encode("ascii"))
        manifest.extend(b"\n")
    return sha256(bytes(manifest))


def _allocator_v5_expected_source(
    root: Path, tested_commit: str, executor: dict[str, Any]
) -> dict[str, str]:
    locks = _source_locks(root, tested_commit)
    allocator = _required_object(
        locks.get("m3AllocatorEvidenceBinding"), "m3AllocatorEvidenceBinding"
    )
    image = _required_string(
        allocator.get("oxiaServerImageDigest"),
        "m3AllocatorEvidenceBinding.oxiaServerImageDigest",
    )
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
        raise ChildError("allocator V5 locked Oxia image digest differs")
    return {
        "nereusCommit": tested_commit,
        "oxiaImageDigest": image.removeprefix("sha256:"),
        "dependencyLockDigest": _allocator_v5_dependency_lock(root, tested_commit),
        "executorDigest": executor["sha256"],
        "workloadDigest": ALLOCATOR_V5.PLAN_DIGEST,
    }


def _allocator_v5_external_bytes(
    root: Path, rows: list[dict[str, Any]], maximum: int
) -> list[tuple[str, bytes]]:
    result: list[tuple[str, bytes]] = []
    for row in rows:
        path = _allocator_external_path(row["path"], "allocator V5 external path", True)
        absolute = Path(path) if path.is_absolute() else root.joinpath(*path.parts)
        raw = absolute.read_bytes()
        if not raw or len(raw) > maximum:
            raise ChildError(f"allocator V5 external bytes outside cap: {path}")
        result.append((row.get("name", absolute.name), raw))
    return result


def _allocator_v5_unsigned(value: dict[str, Any]) -> dict[str, Any]:
    unsigned = dict(value)
    unsigned["receiptSha256"] = "0" * 64
    return unsigned


def validate_allocator_v5_campaign_verification(
    value: object, root: Path, tested_commit: str
) -> dict[str, Any]:
    members = {
        "checkpointBase64",
        "checkpointSha256",
        "diagnosticBase64",
        "diagnosticJunitFiles",
        "diagnosticRawFiles",
        "diagnosticSha256",
        "evaluationBase64",
        "evaluationSha256",
        "executionAttachments",
        "executorArtifact",
        "formalJunitBase64",
        "formalJunitSha256",
        "promotionDecisionBase64",
        "promotionDecisionSha256",
        "receiptSha256",
        "schema",
        "selectionBase64",
        "selectionSha256",
        "testedCommit",
    }
    wrapper = _exact_members(value, members, "governed allocator V5 campaign verification")
    if (
        wrapper["schema"] != ALLOCATOR_V5_VERIFICATION_SCHEMA
        or wrapper["testedCommit"] != tested_commit
    ):
        raise ChildError("governed allocator V5 verification schema/source differs")
    _sha_text(wrapper["receiptSha256"], "governed allocator V5 verification receipt SHA")
    if wrapper["receiptSha256"] != sha256(canonical_bytes(_allocator_v5_unsigned(wrapper))):
        raise ChildError("governed allocator V5 verification self-hash differs")

    checkpoint_raw = _allocator_v5_embedded(
        wrapper, "checkpoint", ALLOCATOR_V5.MAX_CHECKPOINT_BYTES
    )
    evaluation_raw = _allocator_v5_embedded(wrapper, "evaluation", 4_096)
    diagnostic_raw = _allocator_v5_embedded(wrapper, "diagnostic", 4_096)
    selection_raw = _allocator_v5_embedded(wrapper, "selection", 4_096)
    formal_junit_raw = _allocator_v5_embedded(
        wrapper, "formalJunit", ALLOCATOR_V2_JUNIT_MAX_BYTES
    )
    promotion_raw = _allocator_v5_embedded(
        wrapper, "promotionDecision", MAX_CANONICAL_BYTES
    )

    executor_rows = _allocator_v5_file_rows(
        root,
        [wrapper["executorArtifact"]],
        "allocator V5 executor artifact",
        1,
        ALLOCATOR_V5_EXTERNAL_MAX_BYTES,
        ("executorArtifact",),
    )
    diagnostic_junit_rows = _allocator_v5_file_rows(
        root,
        wrapper["diagnosticJunitFiles"],
        "allocator V5 diagnostic JUnit files",
        10,
        ALLOCATOR_V2_JUNIT_MAX_BYTES,
        tuple(sorted(f"TEST-{suite}.xml" for suite in {
            identity.split("#", 1)[0] for identity in ALLOCATOR_V5.DIAGNOSTIC_TESTS
        })),
    )
    diagnostic_raw_rows = _allocator_v5_file_rows(
        root,
        wrapper["diagnosticRawFiles"],
        "allocator V5 diagnostic raw files",
        19,
        ALLOCATOR_V2_EXTERNAL_MAX_BYTES,
        tuple(sorted(ALLOCATOR_V5.DIAGNOSTIC_RAW_NAMES)),
    )
    execution_rows = _allocator_v5_file_rows(
        root,
        wrapper["executionAttachments"],
        "allocator V5 physical execution attachments",
        ALLOCATOR_V5.MAX_PHYSICAL_FILES,
        ALLOCATOR_V5_EXTERNAL_MAX_BYTES,
    )
    all_paths = [
        executor_rows[0]["path"],
        *(row["path"] for row in diagnostic_junit_rows),
        *(row["path"] for row in diagnostic_raw_rows),
        *(row["path"] for row in execution_rows),
    ]
    if len(all_paths) != len(set(all_paths)):
        raise ChildError("allocator V5 governed external inventories alias paths")

    try:
        checkpoint = ALLOCATOR_V5.parse_checkpoint(checkpoint_raw)
        if checkpoint["source"] != _allocator_v5_expected_source(
            root, tested_commit, executor_rows[0]
        ):
            raise ChildError("allocator V5 NACP5 source tuple differs from exact locks/executor")
        physical = _allocator_v5_external_bytes(
            root, execution_rows, ALLOCATOR_V5_EXTERNAL_MAX_BYTES
        )
        if ALLOCATOR_V5.physical_aggregates(checkpoint, physical) != checkpoint["aggregates"]:
            raise ChildError("allocator V5 physical aggregate inventory differs")
        evaluation = ALLOCATOR_V5.parse_evaluation(evaluation_raw, checkpoint)
        diagnostic = ALLOCATOR_V5.parse_diagnostic(diagnostic_raw)
        if diagnostic["source"] != checkpoint["source"]:
            raise ChildError("allocator V5 diagnostic source differs from checkpoint")
        diagnostic_junit_manifest, diagnostic_summary = (
            ALLOCATOR_V5.diagnostic_junit_manifest(
                _allocator_v5_external_bytes(
                    root, diagnostic_junit_rows, ALLOCATOR_V2_JUNIT_MAX_BYTES
                )
            )
        )
        diagnostic_raw_manifest = ALLOCATOR_V5.diagnostic_raw_manifest(
            _allocator_v5_external_bytes(
                root, diagnostic_raw_rows, ALLOCATOR_V2_EXTERNAL_MAX_BYTES
            ),
            tested_commit,
        )
        if (
            diagnostic["junitManifest"] != diagnostic_junit_manifest
            or diagnostic["rawManifest"] != diagnostic_raw_manifest
        ):
            raise ChildError("allocator V5 NADV5 manifest links differ")
        formal_summary, formal_identities = ALLOCATOR_V5.parse_junit(formal_junit_raw)
        if (
            formal_summary != {"tests": 1, "failures": 0, "errors": 0, "skipped": 0}
            or len(formal_identities) != 1
        ):
            raise ChildError("allocator V5 formal JUnit inventory/result differs")
        selection = ALLOCATOR_V5.parse_selection(
            selection_raw, checkpoint, evaluation, diagnostic, formal_summary
        )
    except ALLOCATOR_V5.V5Error as error:
        raise ChildError(str(error)) from error

    decision = _exact_members(
        load_external_json(
            promotion_raw, "allocator V5 promotion decision", MAX_CANONICAL_BYTES
        ),
        {
            "checkpointSha256",
            "diagnosticJUnitSha256",
            "diagnosticRawManifestSha256",
            "diagnosticSha256",
            "evaluationSha256",
            "formalJUnitSha256",
            "schema",
            "selectedCandidate",
            "status",
        },
        "allocator V5 promotion decision",
    )
    expected_decision = ALLOCATOR_V5.promotion_decision_expected(
        checkpoint_raw,
        evaluation_raw,
        diagnostic_raw,
        diagnostic_junit_manifest,
        diagnostic_raw_manifest,
        formal_junit_raw,
        selection["candidate"],
    )
    locked_mode, _ = source_bindings(root, tested_commit)
    if decision != expected_decision or selection["mode"] != locked_mode:
        raise ChildError("allocator V5 promotion decision/selection/source-lock mode differs")
    return {
        "mode": locked_mode,
        "selectedCandidate": selection["candidate"],
        "selectedRangeSize": ALLOCATOR_V5.RANGE_SIZES[selection["candidate"]],
        "selectionSha256": selection["digest"],
        "summary": _sum_summaries([formal_summary, diagnostic_summary]),
    }


def seal_allocator_v5_campaign_verification_receipt(
    root: Path,
    tested_commit: str,
    checkpoint_raw: bytes,
    evaluation_raw: bytes,
    diagnostic_raw: bytes,
    selection_raw: bytes,
    formal_junit_raw: bytes,
    promotion_raw: bytes,
    executor_path: object,
    diagnostic_junit_paths: list[object],
    diagnostic_raw_paths: list[object],
    execution_paths: list[object],
) -> dict[str, Any]:
    def file_row(raw_path: object, name: str | None, maximum: int) -> dict[str, Any]:
        path = _allocator_external_path(raw_path, "allocator V5 governed external path", True)
        size, digest, _, _ = _allocator_stream_identity(
            root, path, maximum, allow_absolute=True
        )
        row: dict[str, Any] = {"bytes": size, "path": str(path), "sha256": digest}
        if name is not None:
            row["name"] = name
        return row

    executor = file_row(executor_path, "executorArtifact", ALLOCATOR_V5_EXTERNAL_MAX_BYTES)
    diagnostic_junit_files = sorted(
        (
            file_row(path, Path(path).name, ALLOCATOR_V2_JUNIT_MAX_BYTES)
            for path in diagnostic_junit_paths
        ),
        key=lambda row: row["name"],
    )
    diagnostic_raw_files = sorted(
        (
            file_row(path, Path(path).name, ALLOCATOR_V2_EXTERNAL_MAX_BYTES)
            for path in diagnostic_raw_paths
        ),
        key=lambda row: row["name"],
    )
    execution_files = sorted(
        (file_row(path, None, ALLOCATOR_V5_EXTERNAL_MAX_BYTES) for path in execution_paths),
        key=lambda row: row["path"],
    )
    load_external_json(promotion_raw, "allocator V5 promotion decision", MAX_CANONICAL_BYTES)
    receipt: dict[str, Any] = {
        "checkpointBase64": base64.b64encode(checkpoint_raw).decode("ascii"),
        "checkpointSha256": sha256(checkpoint_raw),
        "diagnosticBase64": base64.b64encode(diagnostic_raw).decode("ascii"),
        "diagnosticJunitFiles": diagnostic_junit_files,
        "diagnosticRawFiles": diagnostic_raw_files,
        "diagnosticSha256": sha256(diagnostic_raw),
        "evaluationBase64": base64.b64encode(evaluation_raw).decode("ascii"),
        "evaluationSha256": sha256(evaluation_raw),
        "executionAttachments": execution_files,
        "executorArtifact": executor,
        "formalJunitBase64": base64.b64encode(formal_junit_raw).decode("ascii"),
        "formalJunitSha256": sha256(formal_junit_raw),
        "promotionDecisionBase64": base64.b64encode(promotion_raw).decode("ascii"),
        "promotionDecisionSha256": sha256(promotion_raw),
        "receiptSha256": "0" * 64,
        "schema": ALLOCATOR_V5_VERIFICATION_SCHEMA,
        "selectionBase64": base64.b64encode(selection_raw).decode("ascii"),
        "selectionSha256": sha256(selection_raw),
        "testedCommit": tested_commit,
    }
    receipt["receiptSha256"] = sha256(canonical_bytes(receipt))
    validate_allocator_v5_campaign_verification(receipt, root, tested_commit)
    return receipt


def _native_ordered_projection(
    row: dict[str, Any], counter_keys: tuple[str, ...], receipt_sha256: str | None = None
) -> dict[str, Any]:
    tested = row["testedSource"]
    execution = row["execution"]
    junit = row["junit"]
    totals = junit["totals"]
    return {
        "schema": row["schema"],
        "componentKind": row["componentKind"],
        "status": row["status"],
        "testedSource": {
            "repository": tested["repository"],
            "commit": tested["commit"],
            "treeSha256": tested["treeSha256"],
        },
        "externalSources": [
            {"repository": source["repository"], "commit": source["commit"]}
            for source in row["externalSources"]
        ],
        "execution": {
            "command": execution["command"],
            "startedAtUtc": execution["startedAtUtc"],
            "finishedAtUtc": execution["finishedAtUtc"],
            "jvm": execution["jvm"],
            "os": execution["os"],
        },
        "junit": {
            "xmlRoot": junit["xmlRoot"],
            "xmlFiles": [
                {
                    "path": value["path"],
                    "sha256": value["sha256"],
                    "tests": value["tests"],
                    "failures": value["failures"],
                    "errors": value["errors"],
                    "skipped": value["skipped"],
                }
                for value in junit["xmlFiles"]
            ],
            "totals": {
                "suites": totals["suites"],
                "tests": totals["tests"],
                "failures": totals["failures"],
                "errors": totals["errors"],
                "skipped": totals["skipped"],
            },
        },
        "requiredTests": list(row["requiredTests"]),
        "counters": {key: row["counters"][key] for key in counter_keys},
        "artifacts": [
            {"path": value["path"], "sha256": value["sha256"], "bytes": value["bytes"]}
            for value in row["artifacts"]
        ],
        "exclusions": list(row["exclusions"]),
        "receiptSha256": row["receiptSha256"] if receipt_sha256 is None else receipt_sha256,
    }


def _native_canonical_bytes(
    row: dict[str, Any], counter_keys: tuple[str, ...], receipt_sha256: str | None = None
) -> bytes:
    return json.dumps(
        _native_ordered_projection(row, counter_keys, receipt_sha256),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=False,
    ).encode("utf-8")


def _native_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or not value.isascii() or "\0" in value:
        raise ChildError(f"native result {label} is not canonical non-empty ASCII")
    return value


def _native_relative(value: object, label: str) -> PurePosixPath:
    text = _native_text(value, label)
    path = PurePosixPath(text)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts) or "\\" in text:
        raise ChildError(f"native result {label} is not a safe relative path")
    return path


def _native_instant(value: object, label: str) -> dt.datetime:
    text = _native_text(value, label)
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?Z", text):
        raise ChildError(f"native result {label} is not a canonical UTC instant")
    try:
        return dt.datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as error:
        raise ChildError(f"native result {label} is not a valid UTC instant") from error


def _validate_native_source_artifacts(
    root: Path,
    tested_commit: str,
    artifacts: object,
    source_roots: tuple[str, ...],
    expected_tree_sha256: str,
    exact_non_java_sources: tuple[str, ...],
    expected_artifact_count: int,
) -> None:
    if not isinstance(artifacts, list) or not artifacts or len(artifacts) > 512:
        raise ChildError("native result source artifact inventory is empty or over cap")
    listed = str(
        git(root, "ls-tree", "-r", "--name-only", tested_commit, "--", *source_roots, text=True)
    ).splitlines()
    expected_paths = sorted(
        path for path in listed
        if path.endswith(".java") or path in exact_non_java_sources
    )
    if len(expected_paths) != expected_artifact_count:
        raise ChildError("native result exact source artifact count differs")
    actual_paths: list[str] = []
    tree = hashlib.sha256()
    for index, raw_artifact in enumerate(artifacts):
        artifact = _exact_members(
            raw_artifact, {"path", "sha256", "bytes"},
            f"native result artifacts[{index}]",
        )
        path = str(_native_relative(artifact["path"], f"artifacts[{index}].path"))
        if not any(path == prefix or path.startswith(prefix + "/") for prefix in source_roots):
            raise ChildError("native result source artifact lies outside the closed component roots")
        _sha_text(artifact["sha256"], f"native result artifacts[{index}].sha256")
        _positive(artifact["bytes"], f"native result artifacts[{index}].bytes")
        blob = git(root, "show", f"{tested_commit}:{path}")
        assert isinstance(blob, bytes)
        if len(blob) != artifact["bytes"] or sha256(blob) != artifact["sha256"]:
            raise ChildError("native result artifact differs from exact tested Nereus source")
        path_bytes = path.encode("utf-8")
        tree.update(struct.pack(">I", len(path_bytes)))
        tree.update(path_bytes)
        tree.update(struct.pack(">Q", len(blob)))
        tree.update(blob)
        actual_paths.append(path)
    if actual_paths != expected_paths:
        raise ChildError("native result artifact inventory differs from all exact component Java sources")
    if tree.hexdigest() != expected_tree_sha256:
        raise ChildError("native result tested source tree SHA-256 differs")


def _validate_native_raw_result(
    raw: bytes,
    root: Path,
    child_kind: str,
    tested_commit: str,
    binding: dict[str, str],
) -> tuple[dict[str, Any], dict[str, int], dict[str, Any]]:
    if not raw or len(raw) > NATIVE_RESULT_MAX_BYTES:
        raise ChildError("raw native result bytes are empty or over cap")
    try:
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_members)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ChildError(f"cannot parse raw native result: {error}") from error
    row = _exact_members(
        value,
        {
            "schema", "componentKind", "status", "testedSource", "externalSources",
            "execution", "junit", "requiredTests", "counters", "artifacts",
            "exclusions", "receiptSha256",
        },
        "raw native result",
    )
    profile = NATIVE_PROFILES.get(child_kind)
    if profile is None:
        raise ChildError(f"raw native result component profile is not implemented: {child_kind}")
    counter_keys = tuple(profile["counters"])
    if (
        row["schema"] != NATIVE_RESULT_SCHEMA
        or row["componentKind"] != child_kind
        or row["status"] != "PASS"
        or row["exclusions"] != NATIVE_RESULT_EXCLUSIONS
        or row["requiredTests"] != list(profile["requiredTests"])
        or row["counters"] != profile["counters"]
    ):
        raise ChildError("raw native result closed identity/inventory differs")
    tested = _exact_members(
        row["testedSource"], {"repository", "commit", "treeSha256"},
        "raw native result.testedSource",
    )
    if tested["repository"] != "nereus" or tested["commit"] != tested_commit:
        raise ChildError("raw native result tested Nereus source differs")
    _sha_text(tested["treeSha256"], "raw native result tested tree SHA")
    external = row["externalSources"]
    if not isinstance(external, list) or len(external) != 1:
        raise ChildError("raw native result external source inventory differs")
    external_source = _exact_members(
        external[0], {"repository", "commit"}, "raw native result.externalSources[0]"
    )
    expected_identity = (
        f"{binding['backend']}|repository={external_source['repository']}"
        f"|commit={external_source['commit']}"
    )
    if (
        binding["childKind"] != child_kind
        or binding["evidenceKind"] != "NATIVE_RESULT"
        or binding["backend"] != profile["backend"]
        or binding["executionClass"] != "NATIVE_REFERENCE_EXECUTION"
        or binding["sourceIdentity"] != expected_identity
    ):
        raise ChildError("raw native result external locked source differs")
    execution = _exact_members(
        row["execution"], {"command", "startedAtUtc", "finishedAtUtc", "jvm", "os"},
        "raw native result.execution",
    )
    if execution["command"] != profile["command"]:
        raise ChildError("raw native result execution command differs")
    start = _native_instant(execution["startedAtUtc"], "execution.startedAtUtc")
    finish = _native_instant(execution["finishedAtUtc"], "execution.finishedAtUtc")
    if finish < start:
        raise ChildError("raw native result execution finish precedes start")
    _native_text(execution["jvm"], "execution.jvm")
    _native_text(execution["os"], "execution.os")
    junit = _exact_members(
        row["junit"], {"xmlRoot", "xmlFiles", "totals"}, "raw native result.junit"
    )
    expected_root = profile["junitRoot"]
    if junit["xmlRoot"] != expected_root:
        raise ChildError("raw native result JUnit root differs")
    files = junit["xmlFiles"]
    suites = profile["suites"]
    if not isinstance(files, list) or len(files) != len(suites):
        raise ChildError("raw native result JUnit file inventory differs")
    file_summaries: list[dict[str, int]] = []
    for index, (raw_file, suite) in enumerate(zip(files, suites, strict=True)):
        file = _exact_members(
            raw_file, {"path", "sha256", "tests", "failures", "errors", "skipped"},
            f"raw native result.junit.xmlFiles[{index}]",
        )
        if file["path"] != f"{expected_root}/TEST-{suite}.xml":
            raise ChildError("raw native result JUnit file path/order differs")
        suite_counts = profile.get("suiteTestCounts")
        if suite_counts is not None and file["tests"] != suite_counts[index]:
            raise ChildError("raw native result JUnit exact suite test count differs")
        _sha_text(file["sha256"], f"raw native result JUnit XML SHA[{index}]")
        file_summaries.append(_summary_from_receipt(file, f"raw native result JUnit file[{index}]"))
    totals = _exact_members(
        junit["totals"], {"suites", "tests", "failures", "errors", "skipped"},
        "raw native result.junit.totals",
    )
    summary = _validate_summary(
        {key: totals[key] for key in ("errors", "failures", "skipped", "tests")},
        "raw native result JUnit totals",
    )
    if totals["suites"] != len(suites) or summary != _sum_summaries(file_summaries):
        raise ChildError("raw native result JUnit totals do not exactly add up")
    _validate_native_source_artifacts(
        root,
        tested_commit,
        row["artifacts"],
        profile["sourceRoots"],
        tested["treeSha256"],
        profile["exactNonJavaSources"],
        profile["sourceArtifactCount"],
    )
    _sha_text(row["receiptSha256"], "raw native result self-hash")
    if row["receiptSha256"] != sha256(_native_canonical_bytes(row, counter_keys, "0" * 64)):
        raise ChildError("raw native result self-hash differs")
    if raw != _native_canonical_bytes(row, counter_keys):
        raise ChildError("raw native result is not exact canonical JSON")
    return row, summary, profile


def _validate_native_junit_xml(
    raw: bytes, suite: str, expected: dict[str, Any]
) -> tuple[dict[str, int], set[str]]:
    if (
        not raw
        or len(raw) > MAX_NATIVE_JUNIT_XML_BYTES
        or b"<!DOCTYPE" in raw.upper()
        or b"<!ENTITY" in raw.upper()
    ):
        raise ChildError("sealed native JUnit XML is empty, unsafe, or over cap")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as error:
        raise ChildError(f"cannot parse sealed native JUnit XML: {error}") from error
    if root.tag != "testsuite" or root.attrib.get("name") != suite:
        raise ChildError("sealed native JUnit suite identity differs")
    summary: dict[str, int] = {}
    for field in ("tests", "failures", "errors", "skipped"):
        try:
            summary[field] = int(root.attrib[field])
        except (KeyError, ValueError) as error:
            raise ChildError(f"sealed native JUnit {field} is invalid") from error
    _validate_summary(summary, "sealed native JUnit summary")
    cases = root.findall("testcase")
    if len(cases) != summary["tests"]:
        raise ChildError("sealed native JUnit declared testcase count differs")
    observed: set[str] = set()
    for case in cases:
        class_name = case.attrib.get("classname") or suite
        test_name = case.attrib.get("name", "")
        if test_name.endswith("()"):
            test_name = test_name[:-2]
        if (
            not class_name
            or not test_name
            or case.find(".//failure") is not None
            or case.find(".//error") is not None
            or case.find(".//skipped") is not None
        ):
            raise ChildError("sealed native JUnit testcase identity/result differs")
        observed.add(f"{class_name}#{test_name}")
    if (
        expected.get("sha256") != sha256(raw)
        or expected.get("tests") != summary["tests"]
        or expected.get("failures") != summary["failures"]
        or expected.get("errors") != summary["errors"]
        or expected.get("skipped") != summary["skipped"]
    ):
        raise ChildError("raw native result differs from exact sealed JUnit XML")
    return summary, observed


def _native_wrapper_unsigned(value: dict[str, Any]) -> dict[str, Any]:
    unsigned = dict(value)
    unsigned["receiptSha256"] = "0" * 64
    return unsigned


def seal_native_execution_receipt(
    root: Path,
    raw_evidence: bytes,
    junit_xml_inputs: list[tuple[str, bytes]],
    child_kind: str,
    tested_commit: str,
) -> dict[str, Any]:
    if child_kind not in NATIVE_PROFILES:
        raise ChildError(f"native execution receipt kind is not closed: {child_kind}")
    _, bindings = source_bindings(root, tested_commit)
    binding = bindings[(child_kind, "NATIVE_RESULT")]
    row, _, profile = _validate_native_raw_result(
        raw_evidence, root, child_kind, tested_commit, binding
    )
    expected_files = row["junit"]["xmlFiles"]
    expected_paths = [value["path"] for value in expected_files]
    if [path for path, _ in junit_xml_inputs] != expected_paths:
        raise ChildError("sealed native JUnit XML input path/order differs from raw result")
    sealed_rows: list[dict[str, Any]] = []
    observed: set[str] = set()
    summaries: list[dict[str, int]] = []
    for (path, raw_xml), expected, suite in zip(
        junit_xml_inputs, expected_files, profile["suites"], strict=True
    ):
        summary, test_ids = _validate_native_junit_xml(raw_xml, suite, expected)
        summaries.append(summary)
        observed.update(test_ids)
        sealed_rows.append(
            {
                "bytes": len(raw_xml),
                "path": path,
                "sha256": sha256(raw_xml),
                "xmlBase64": base64.b64encode(raw_xml).decode("ascii"),
            }
        )
    missing = set(profile["requiredTests"]) - observed
    if missing:
        raise ChildError(f"sealed native JUnit XML lacks required tests: {sorted(missing)}")
    if _sum_summaries(summaries) != {
        key: row["junit"]["totals"][key]
        for key in ("errors", "failures", "skipped", "tests")
    }:
        raise ChildError("sealed native JUnit XML totals differ from raw result")
    receipt: dict[str, Any] = {
        "componentKind": child_kind,
        "junitXml": sealed_rows,
        "rawEvidenceBase64": base64.b64encode(raw_evidence).decode("ascii"),
        "rawEvidenceSha256": sha256(raw_evidence),
        "receiptSha256": "0" * 64,
        "schema": SEALED_NATIVE_EXECUTION_SCHEMA,
        "testedCommit": tested_commit,
    }
    receipt["receiptSha256"] = sha256(canonical_bytes(receipt))
    return receipt


def validate_native_result(
    value: object,
    root: Path,
    child_kind: str,
    tested_commit: str,
    binding: dict[str, str],
) -> dict[str, int]:
    wrapper = _exact_members(
        value,
        {
            "componentKind",
            "junitXml",
            "rawEvidenceBase64",
            "rawEvidenceSha256",
            "receiptSha256",
            "schema",
            "testedCommit",
        },
        "sealed native execution receipt",
    )
    if (
        wrapper["schema"] != SEALED_NATIVE_EXECUTION_SCHEMA
        or wrapper["componentKind"] != child_kind
        or wrapper["testedCommit"] != tested_commit
    ):
        raise ChildError("sealed native execution receipt schema/source/component differs")
    raw_evidence = _decode_sealed_bytes(
        wrapper["rawEvidenceBase64"], "sealed raw native evidence", NATIVE_RESULT_MAX_BYTES
    )
    if wrapper["rawEvidenceSha256"] != sha256(raw_evidence):
        raise ChildError("sealed raw native evidence SHA-256 differs")
    _sha_text(wrapper["receiptSha256"], "sealed native execution receipt SHA")
    if wrapper["receiptSha256"] != sha256(canonical_bytes(_native_wrapper_unsigned(wrapper))):
        raise ChildError("sealed native execution receipt self-hash differs")
    row, raw_summary, profile = _validate_native_raw_result(
        raw_evidence, root, child_kind, tested_commit, binding
    )
    sealed_rows = wrapper["junitXml"]
    expected_files = row["junit"]["xmlFiles"]
    if not isinstance(sealed_rows, list) or len(sealed_rows) != len(expected_files):
        raise ChildError("sealed native JUnit XML inventory differs")
    observed: set[str] = set()
    summaries: list[dict[str, int]] = []
    for index, (sealed_value, expected, suite) in enumerate(
        zip(sealed_rows, expected_files, profile["suites"], strict=True)
    ):
        sealed = _exact_members(
            sealed_value,
            {"bytes", "path", "sha256", "xmlBase64"},
            f"sealed native JUnit XML[{index}]",
        )
        raw_xml = _decode_sealed_bytes(
            sealed["xmlBase64"],
            f"sealed native JUnit XML[{index}]",
            MAX_NATIVE_JUNIT_XML_BYTES,
        )
        if (
            sealed["path"] != expected["path"]
            or sealed["bytes"] != len(raw_xml)
            or sealed["sha256"] != sha256(raw_xml)
        ):
            raise ChildError("sealed native JUnit XML bytes/path/SHA differ")
        summary, test_ids = _validate_native_junit_xml(raw_xml, suite, expected)
        summaries.append(summary)
        observed.update(test_ids)
    missing = set(profile["requiredTests"]) - observed
    if missing:
        raise ChildError(f"sealed native JUnit XML lacks required tests: {sorted(missing)}")
    derived = _sum_summaries(summaries)
    if derived != raw_summary:
        raise ChildError("raw native result counters differ from exact sealed JUnit XML")
    return derived


def validate_typed_evidence(
    value: object,
    attachment_kind: str,
    child_kind: str,
    tested_commit: str,
    binding: dict[str, str],
) -> dict[str, int]:
    if attachment_kind == "PROVIDER_REAL_RECEIPT":
        return _validate_sealed_real_execution_receipt(
            value, tested_commit, binding, attachment_kind
        )
    if attachment_kind == "KMS_REAL_RECEIPT":
        return _validate_sealed_real_execution_receipt(
            value, tested_commit, binding, attachment_kind
        )
    if attachment_kind == "NATIVE_RESULT":
        raise ChildError("NATIVE_RESULT requires the sealed raw/JUnit validator")
    row = _exact_members(
        value,
        {
            "backend",
            "errors",
            "evidenceKind",
            "executionClass",
            "failures",
            "nereusCommit",
            "result",
            "schema",
            "skipped",
            "sourceIdentity",
            "subjectCount",
            "tests",
        },
        f"normalized typed evidence {attachment_kind}",
    )
    backends, execution_class, subject_count = _expected_typed_profile(attachment_kind, child_kind)
    if (
        row["schema"] != TYPED_SCHEMA
        or row["evidenceKind"] != attachment_kind
        or row["result"] != "PASS_EVIDENCE_ONLY"
        or row["nereusCommit"] != tested_commit
        or row["backend"] not in backends
        or row["executionClass"] != execution_class
        or row["subjectCount"] != subject_count
    ):
        raise ChildError(f"normalized typed evidence identity/profile/source differs: {attachment_kind}")
    if set(binding) != {
        "backend",
        "childKind",
        "evidenceKind",
        "executionClass",
        "sourceIdentity",
        "sourceProvenance",
    } or (
        binding["childKind"] != child_kind
        or binding["evidenceKind"] != attachment_kind
        or row["backend"] != binding["backend"]
        or row["executionClass"] != binding["executionClass"]
        or row["sourceIdentity"] != binding["sourceIdentity"]
    ):
        raise ChildError(
            f"normalized typed evidence does not match exact tested source locks: {attachment_kind}"
        )
    identity = row["sourceIdentity"]
    if not isinstance(identity, str) or not SOURCE_IDENTITY_PATTERN.fullmatch(identity):
        raise ChildError(f"normalized typed evidence source identity is invalid: {attachment_kind}")
    lowered = identity.lower().replace("-", "_")
    if any(token in lowered for token in ("fake", "mock", "in_memory", "local_only")):
        raise ChildError(f"normalized typed evidence claims a fake/local source: {attachment_kind}")
    if not (re.search(r"[0-9a-f]{40}", identity) or re.search(r"sha256:[0-9a-f]{64}", identity)):
        raise ChildError(f"normalized typed evidence source identity lacks an exact commit/digest: {attachment_kind}")
    _nonnegative(row["subjectCount"], f"{attachment_kind}.subjectCount")
    return _validate_summary(
        {key: row[key] for key in ("errors", "failures", "skipped", "tests")},
        f"normalized typed evidence {attachment_kind}",
    )


def validate_allocator_derived_evidence(
    value: object,
    attachment_kind: str,
    tested_commit: str,
    binding: dict[str, str],
    authority: dict[str, Any],
) -> None:
    if attachment_kind not in ALLOCATOR_DERIVED_KINDS:
        raise ChildError(f"allocator derived evidence kind is not closed: {attachment_kind}")
    summary = validate_typed_evidence(
        value,
        attachment_kind,
        "ALLOCATOR_SELECTION",
        tested_commit,
        binding,
    )
    if summary != authority["rawSummary"]:
        raise ChildError(
            f"allocator derived evidence counters differ from governed raw recomputation: {attachment_kind}"
        )


def validate_attachment_row(value: object, label: str) -> dict[str, Any]:
    row = _exact_members(value, {"bytes", "kind", "path", "sha256"}, label)
    _positive(row["bytes"], f"{label}.bytes")
    if row["kind"] not in ATTACHMENT_KINDS:
        raise ChildError(f"{label}.kind is outside the closed inventory")
    safe_relative(row["path"], f"{label}.path")
    if not isinstance(row["sha256"], str) or not SHA256_PATTERN.fullmatch(row["sha256"]):
        raise ChildError(f"{label}.sha256 is not canonical lowercase SHA-256")
    return row


def _sum_summaries(rows: list[dict[str, int]]) -> dict[str, int]:
    return {
        key: sum(row[key] for row in rows)
        for key in ("errors", "failures", "skipped", "tests")
    }


def validate_generic_value(
    root: Path,
    receipt: object,
    expected_kind: str | None = None,
    expected_tested_commit: str | None = None,
) -> tuple[str, list[dict[str, Any]], dict[str, int]]:
    value = _exact_members(
        receipt,
        {
            "attachments",
            "exclusions",
            "kind",
            "promotionEligible",
            "result",
            "schema",
            "sourceTuple",
            "testSummary",
        },
        "M3 child receipt",
    )
    kind = value["kind"]
    if kind not in GENERIC_CHILD_KINDS or (expected_kind is not None and kind != expected_kind):
        raise ChildError(f"child kind differs from the closed/expected inventory: {kind}")
    if (
        value["schema"] != SCHEMA
        or value["result"] != CHILD_RESULTS[kind]
        or value["promotionEligible"] is not False
    ):
        raise ChildError(f"child schema/result/promotion boundary differs: {kind}")
    if value["exclusions"] != expected_exclusions(kind):
        raise ChildError(f"child exclusions differ from the exact kind profile: {kind}")
    source = _exact_members(
        value["sourceTuple"], {"nereusCommit", "sourceLocksSha256"}, "child sourceTuple"
    )
    tested = source["nereusCommit"]
    if not isinstance(tested, str) or not COMMIT_PATTERN.fullmatch(tested):
        raise ChildError("child Nereus commit is not canonical lowercase hexadecimal")
    if expected_tested_commit is not None and tested != expected_tested_commit:
        raise ChildError("child tested source differs from the explicitly expected source")
    if source != expected_source_tuple(root, tested):
        raise ChildError("child source tuple differs from exact tested source locks")

    attachments = value["attachments"]
    if not isinstance(attachments, list) or not 1 <= len(attachments) <= FINAL.MAX_ATTACHMENTS_PER_CHILD:
        raise ChildError(f"child attachment count outside cap: {kind}")
    rows = [
        validate_attachment_row(row, f"attachments[{index}]")
        for index, row in enumerate(attachments)
    ]
    attachment_kinds = [row["kind"] for row in rows]
    attachment_paths = [row["path"] for row in rows]
    if attachment_kinds != sorted(set(attachment_kinds)):
        raise ChildError("child attachment kinds are duplicated or unsorted")
    if attachment_paths != sorted(set(attachment_paths)):
        raise ChildError("child attachment paths are duplicated or unsorted")
    missing = REQUIRED_ATTACHMENTS[kind] - set(attachment_kinds)
    if missing:
        raise ChildError(f"child mandatory typed attachments are absent: {kind} {sorted(missing)}")
    allocator_profile: str | None = None
    if kind == "ALLOCATOR_SELECTION":
        allocator_profile = allocator_authority_profile(set(attachment_kinds))
    allowed_typed = set(REQUIRED_ATTACHMENTS[kind])
    if allocator_profile == "V1":
        allowed_typed.update(ALLOCATOR_DERIVED_KINDS)
    misplaced_typed = set(attachment_kinds).intersection(NORMALIZED_TYPED_KINDS) - allowed_typed
    if misplaced_typed:
        raise ChildError(
            "child contains typed attachments outside its closed profile: "
            f"{kind} {sorted(misplaced_typed)}"
        )
    if "LOCAL_CAP_RESULT" in attachment_kinds and kind != "D_LOCAL_CAP":
        raise ChildError("child contains D1 local-cap evidence outside D_LOCAL_CAP")

    normalized: list[dict[str, int]] = []
    _, bindings = source_bindings(root, tested)
    allocator_authority: dict[str, Any] | None = None
    allocator_v2_authority: dict[str, Any] | None = None
    allocator_v5_authority: dict[str, Any] | None = None
    if kind == "ALLOCATOR_SELECTION" and allocator_profile == "V1":
        raw_row = next(
            (row for row in rows if row["kind"] == "ALLOCATOR_RAW_VERIFICATION"),
            None,
        )
        if raw_row is None:
            raise ChildError("allocator child lacks governed raw verification")
        raw_path = safe_relative(raw_row["path"], "allocator raw verification path")
        raw_bytes = read_safe_file(root, raw_path, ALLOCATOR_VERIFICATION_MAX_BYTES)
        allocator_authority = validate_allocator_verification(
            load_canonical_json(
                raw_bytes, str(raw_path), ALLOCATOR_VERIFICATION_MAX_BYTES
            ),
            root,
            tested,
        )
    elif kind == "ALLOCATOR_SELECTION" and allocator_profile == "V2":
        v2_row = next(
            (row for row in rows if row["kind"] == "ALLOCATOR_V2_CAMPAIGN_VERIFICATION"),
            None,
        )
        if v2_row is None:
            raise ChildError("allocator V2 child lacks governed campaign verification")
        v2_path = safe_relative(v2_row["path"], "allocator V2 verification path")
        v2_bytes = read_safe_file(root, v2_path, ALLOCATOR_V2_VERIFICATION_MAX_BYTES)
        allocator_v2_authority = validate_allocator_v2_campaign_verification(
            load_canonical_json(
                v2_bytes, str(v2_path), ALLOCATOR_V2_VERIFICATION_MAX_BYTES
            ),
            root,
            tested,
        )
    elif kind == "ALLOCATOR_SELECTION":
        v5_row = next(
            (row for row in rows if row["kind"] == "ALLOCATOR_V5_CAMPAIGN_VERIFICATION"),
            None,
        )
        if v5_row is None:
            raise ChildError("allocator V5 child lacks governed campaign verification")
        v5_path = safe_relative(v5_row["path"], "allocator V5 verification path")
        v5_bytes = read_safe_file(root, v5_path, ALLOCATOR_V5_VERIFICATION_MAX_BYTES)
        allocator_v5_authority = validate_allocator_v5_campaign_verification(
            load_canonical_json(
                v5_bytes, str(v5_path), ALLOCATOR_V5_VERIFICATION_MAX_BYTES
            ),
            root,
            tested,
        )
    total_bytes = 0
    for row in rows:
        path = safe_relative(row["path"], "attachment.path")
        raw = read_safe_file(root, path)
        if len(raw) != row["bytes"] or sha256(raw) != row["sha256"]:
            raise ChildError(f"child attachment bytes/SHA differ: {path}")
        total_bytes += len(raw)
        if total_bytes > MAX_TOTAL_ATTACHMENT_BYTES:
            raise ChildError("child total attachment bytes exceed cap")
        if row["kind"] == "JUNIT_SUMMARY":
            normalized.append(
                validate_junit(
                    load_canonical_json(raw, str(path), SEALED_JUNIT_MAX_BYTES),
                    root,
                    tested,
                    kind,
                )
            )
        elif row["kind"] == "LOCAL_CAP_RESULT":
            normalized.append(
                validate_local_cap_result(
                    load_canonical_json(raw, str(path)), root, tested
                )
            )
        elif row["kind"] == "MUTATION_MANIFEST":
            validate_mutation_manifest(
                load_canonical_json(raw, str(path), MUTATION_MANIFEST_MAX_BYTES)
            )
        elif row["kind"] == "RECOVERY_MANIFEST":
            validate_recovery_manifest(raw)
        elif row["kind"] == "PROTOCOL_FIXTURE":
            validate_protocol_fixture(raw)
        elif row["kind"] == "NATIVE_RESULT":
            normalized.append(
                validate_native_result(
                    load_canonical_json(raw, str(path), SEALED_NATIVE_MAX_BYTES),
                    root,
                    kind,
                    tested,
                    bindings[(kind, row["kind"])],
                )
            )
        elif row["kind"] == "ALLOCATOR_RAW_VERIFICATION":
            assert allocator_authority is not None
            normalized.extend(
                [
                    allocator_authority["rawSummary"],
                    allocator_authority["verifierSummary"],
                ]
            )
        elif row["kind"] == "ALLOCATOR_V2_CAMPAIGN_VERIFICATION":
            assert allocator_v2_authority is not None
            normalized.append(allocator_v2_authority["summary"])
        elif row["kind"] == "ALLOCATOR_V5_CAMPAIGN_VERIFICATION":
            assert allocator_v5_authority is not None
            normalized.append(allocator_v5_authority["summary"])
        elif row["kind"] in ALLOCATOR_DERIVED_KINDS:
            assert allocator_authority is not None
            validate_allocator_derived_evidence(
                load_canonical_json(raw, str(path)),
                row["kind"],
                tested,
                bindings[(kind, row["kind"])],
                allocator_authority,
            )
        elif row["kind"] in NORMALIZED_TYPED_KINDS:
            normalized.append(
                validate_typed_evidence(
                    load_canonical_json(raw, str(path)),
                    row["kind"],
                    kind,
                    tested,
                    bindings[(kind, row["kind"])],
                )
            )
    derived = _sum_summaries(normalized)
    summary = _validate_summary(value["testSummary"], "child testSummary")
    if summary != derived:
        raise ChildError("child testSummary is not exactly derived from normalized evidence attachments")
    return tested, rows, summary


def _dirty_paths(root: Path) -> set[PurePosixPath]:
    paths: set[PurePosixPath] = set()
    for args in (
        ("diff", "--name-only", "-z"),
        ("diff", "--cached", "--name-only", "-z"),
        ("ls-files", "--others", "--exclude-standard", "-z"),
    ):
        raw = git(root, *args)
        assert isinstance(raw, bytes)
        paths.update(PurePosixPath(item.decode("utf-8")) for item in raw.split(b"\0") if item)
    return paths


def _validate_source_freshness(root: Path, tested: str) -> tuple[str, int]:
    try:
        head, descendants = FINAL.validate_descendants(root, tested)
    except FINAL.FinalError as error:
        raise ChildError(str(error)) from error
    invalid_dirty = sorted(
        str(path)
        for path in _dirty_paths(root)
        if not FINAL.is_under(path, FINAL.EVIDENCE_PREFIX)
        and path not in FINAL.EVIDENCE_ONLY_EXACT
    )
    if invalid_dirty:
        raise ChildError(f"non-evidence dirty path exists during child validation: {invalid_dirty}")
    return head, descendants


def _w1_identity(root: Path, receipt_path: PurePosixPath, tested: str) -> dict[str, Any]:
    raw = read_safe_file(root, receipt_path, MAX_CANONICAL_BYTES)
    receipt = load_canonical_json(raw, str(receipt_path))
    expected_members = {
        "childGates",
        "evidenceClass",
        "exclusions",
        "historicalFinal",
        "kind",
        "m2AmendmentLineage",
        "promotionEligible",
        "result",
        "scenarioPromotion",
        "schema",
        "sources",
        "testedNereusCommit",
    }
    _exact_members(receipt, expected_members, "W1 current-source M2 regression")
    if (
        receipt["schema"] != M2.RECEIPT_SCHEMA
        or receipt["kind"] != M2.RECEIPT_KIND
        or receipt["result"] != M2.RECEIPT_RESULT
        or receipt["evidenceClass"] != M2.EXECUTION_PROFILE
        or receipt["testedNereusCommit"] != tested
        or receipt["promotionEligible"] is not False
        or receipt["scenarioPromotion"] is not False
        or receipt["m2AmendmentLineage"] != []
        or receipt["exclusions"] != list(M2.EXCLUSIONS)
        or receipt["historicalFinal"] != M2.historical_final_identity()
        or receipt["sources"] != M2.expected_sources(tested)
    ):
        raise ChildError("W1 current-source M2 regression closed identity/source differs")

    historical_raw = read_safe_file(root, M2.HISTORICAL_FINAL_PATH)
    if (
        len(historical_raw) != M2.HISTORICAL_FINAL_BYTES
        or sha256(historical_raw) != M2.HISTORICAL_FINAL_SHA256
    ):
        raise ChildError("W1 historical M2 Final bytes/SHA differ")
    try:
        if git(root, "show", f"{tested}:{M2.HISTORICAL_FINAL_PATH}") != historical_raw:
            raise ChildError("W1 historical M2 Final differs from the exact tested source")
    except ChildError:
        raise

    rows = receipt["childGates"]
    if not isinstance(rows, list) or len(rows) != len(M2.REQUIRED_GATES):
        raise ChildError("W1 current-source M2 gate count differs")
    attachments: list[dict[str, Any]] = []
    tests = 0
    for index, (gate, row) in enumerate(zip(M2.REQUIRED_GATES, rows, strict=True)):
        _exact_members(
            row,
            {"attachment", "errors", "failures", "gateId", "result", "skipped", "tests"},
            f"W1.childGates[{index}]",
        )
        if row["gateId"] != gate or row["result"] != "PASS":
            raise ChildError(f"W1 child gate order/result differs: {gate}")
        _positive(row["tests"], f"W1.{gate}.tests")
        for counter in ("failures", "errors", "skipped"):
            _zero(row[counter], f"W1.{gate}.{counter}")
        identity = row["attachment"]
        _exact_members(identity, {"bytes", "path", "sha256"}, f"W1.{gate}.attachment")
        path = safe_relative(identity["path"], f"W1.{gate}.attachment.path")
        expected_path = receipt_path.parent / "attachments" / f"{gate}.json"
        if path != expected_path:
            raise ChildError(f"W1 child gate attachment path differs: {gate}")
        child_raw = read_safe_file(root, path, MAX_CANONICAL_BYTES)
        if len(child_raw) != _positive(identity["bytes"], f"W1.{gate}.attachment.bytes"):
            raise ChildError(f"W1 child gate attachment bytes differ: {gate}")
        if sha256(child_raw) != identity["sha256"]:
            raise ChildError(f"W1 child gate attachment SHA differs: {gate}")
        try:
            child_value = M2.load_canonical_json(child_raw, str(path))
            M2.validate_child_result(child_value, tested)
        except M2.RegressionError as error:
            raise ChildError(str(error)) from error
        if child_value["gateId"] != gate or any(
            child_value[counter] != row[counter]
            for counter in ("tests", "failures", "errors", "skipped")
        ):
            raise ChildError(f"W1 child gate attachment counters/identity differ: {gate}")
        attachments.append(
            {
                "bytes": identity["bytes"],
                "kind": "CURRENT_SOURCE_M2_GATE_RESULT",
                "path": identity["path"],
                "sha256": identity["sha256"],
            }
        )
        tests += row["tests"]
    attachments.sort(key=lambda item: item["path"])
    source_value = receipt["sources"]
    return {
        "attachments": attachments,
        "bytes": len(raw),
        "errors": 0,
        "exclusions": list(BASE_EXCLUSIONS),
        "failures": 0,
        "kind": "W1_CURRENT_SOURCE_M2_REGRESSION",
        "path": str(receipt_path),
        "promotionEligible": False,
        "result": CHILD_RESULTS["W1_CURRENT_SOURCE_M2_REGRESSION"],
        "sha256": sha256(raw),
        "skipped": 0,
        "sourceTuple": {
            "nereusCommit": tested,
            "sourceTupleId": "SOURCES",
            "sourceTupleSha256": sha256(canonical_bytes(source_value)),
        },
        "tests": tests,
    }


def build_final_child_identity(
    root: Path,
    receipt_path: PurePosixPath,
    expected_kind: str,
    expected_tested_commit: str | None = None,
) -> dict[str, Any]:
    root = ensure_root(root)
    receipt_path = safe_relative(str(receipt_path), "receipt")
    if expected_kind not in CHILD_KINDS:
        raise ChildError(f"expected child kind is outside the closed inventory: {expected_kind}")
    if expected_kind == "W1_CURRENT_SOURCE_M2_REGRESSION":
        tested = expected_tested_commit or current_head(root)
        identity = _w1_identity(root, receipt_path, tested)
        _validate_source_freshness(root, tested)
        return identity

    if not FINAL.is_under(receipt_path, CHILD_PREFIX):
        raise ChildError(f"generic child receipt is outside {CHILD_PREFIX}: {receipt_path}")
    raw = read_safe_file(root, receipt_path, MAX_CANONICAL_BYTES)
    value = load_canonical_json(raw, str(receipt_path))
    tested, attachments, summary = validate_generic_value(
        root, value, expected_kind, expected_tested_commit
    )
    _validate_source_freshness(root, tested)
    source_value = value["sourceTuple"]
    return {
        "attachments": attachments,
        "bytes": len(raw),
        "errors": summary["errors"],
        "exclusions": value["exclusions"],
        "failures": summary["failures"],
        "kind": expected_kind,
        "path": str(receipt_path),
        "promotionEligible": False,
        "result": CHILD_RESULTS[expected_kind],
        "sha256": sha256(raw),
        "skipped": summary["skipped"],
        "sourceTuple": {
            "nereusCommit": tested,
            "sourceTupleId": "SOURCE_TUPLE",
            "sourceTupleSha256": sha256(canonical_bytes(source_value)),
        },
        "tests": summary["tests"],
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=SCRIPT_DIR.parent)
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--expected-kind", required=True, choices=CHILD_KINDS)
    parser.add_argument("--expected-tested-commit")
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        receipt = safe_relative(args.receipt, "--receipt")
        identity = build_final_child_identity(
            args.repo_root, receipt, args.expected_kind, args.expected_tested_commit
        )
    except (ChildError, OSError) as error:
        print(f"V2 M3 child check: {error}", file=sys.stderr)
        return 1
    print(
        "V2 M3 child verified: "
        f"kind={identity['kind']} tested={identity['sourceTuple']['nereusCommit']} "
        f"tests={identity['tests']} attachments={len(identity['attachments'])} "
        "failures=0 errors=0 skipped=0 promotionEligible=false scenarioPromotion=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
