#!/usr/bin/env python3
"""Check M5 execution provenance, exclusive child ownership and source-bound Final evidence."""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parent.parent


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    sys.modules[name] = value
    spec.loader.exec_module(value)
    return value


INPUTS = module("m5_evidence_inputs", "m5-candidate-inputs.py")
JUNIT = module("m5_evidence_raw_junit", "m5-evidence-junit.py")
M4 = module("m5_evidence_m4_contract", "check-v2-m4-evidence.py")
HISTORICAL_M4 = module("m5_evidence_historical_m4", "check-v2-m5-historical-m4.py")
EQUIVALENCE = module("m5_stage_input_equivalence", "m5-stage-input-equivalence.py")
NATIVE_INPUTS = module("m5_native_isolated_inputs", "m5-native-isolated-inputs.py")
NATIVE_BUILD = module("m5_native_build_proof", "m5-native-build-proof.py")
KINDS = (
    "MATERIALIZATION_MANIFEST_PUBLICATION", "KAFKA_COMPACTION_INDEX_REBUILD",
    "RETENTION_METADATA_RETIREMENT", "PHYSICAL_DELETE_ORPHAN_RECONCILIATION",
    "CURRENT_SOURCE_CELL_ISOLATION",
)
OWNERS = dict(zip((
    "RESOURCE_UNIQUENESS", "REPLACEMENT_RETAINED_MESSAGES", "THREE_RECLAMATION_REASONS",
    "BK_ONLY_COMPACTION", "BK_OUTPUT_FAILURE", "RETIREMENT_HISTORY_1024", "RETIREMENT_HISTORY_RECOVERY",
    "DONE_CAPACITY", "READ_FENCED_TAKEOVER", "WRITER_RACES", "PULSAR_NATIVE_LIFECYCLE", "SLOW_UNKNOWN_PUT",
    "RUN_RECOVERY_VS_DELETE", "QUARANTINE_EXIT", "KAFKA_STORAGE_REPLAY_LOAD", "CURRENT_SOURCE_REGRESSIONS",
), (KINDS[3], KINDS[3], KINDS[2], KINDS[1], KINDS[0], KINDS[2], KINDS[2],
    KINDS[3], KINDS[3], KINDS[3], KINDS[3], KINDS[4], KINDS[2], KINDS[4], KINDS[4], KINDS[4])))
STAGES = {
    "COMPONENTS": ("scripts/run-v2-m5-evidence.py", ()),
    "BOOKKEEPER_OXIA": ("scripts/run-v2-m5-bookkeeper-oxia-evidence.py", ("BOOKKEEPER", "OXIA")),
    "OBJECT": ("scripts/run-v2-m5-object-evidence.py", ("OBJECT", "OXIA")),
    "NATIVE_KAFKA": ("scripts/run-v2-m5-native-kafka-evidence.py", ("BOOKKEEPER", "OXIA", "NATIVE_KAFKA")),
    "NATIVE_PULSAR": ("scripts/run-v2-m5-native-pulsar-evidence.py",
                      ("BOOKKEEPER", "OBJECT", "OXIA", "NATIVE_PULSAR")),
}
PROMOTED = (
    "V2-META-007", "V2-FABRIC-003", "V2-BK-011", "V2-KAF-DATA-011", "V2-READ-002", "V2-READ-006",
    "V2-READ-008", "V2-READ-009", "V2-READ-010", "V2-READ-011", "V2-READ-012", "V2-READ-013",
    "V2-READ-014", "V2-READ-015",
)
DEFERRED = ("V2-KAF-DATA-012", "V2-KAF-DATA-013", "V2-KAF-DATA-022")
SHARED_M4 = ("V2-READ-006", "V2-READ-008", "V2-READ-009", "V2-READ-010", "V2-READ-011",
             "V2-READ-012", "V2-READ-013", "V2-READ-014", "V2-READ-015")
EXCLUSIONS = ("M6_PROCESS_ACTIVATION", "M7_OPERATIONAL_HANDOFF", "M8_NATIVE_PARITY",
              "PRODUCTION_DEPLOYMENT_AUTHORITY", "TOMBSTONE_DELETION", "ALLOCATOR_ORPHAN_GC")
MAX_BYTES = INPUTS.MAX_FILE_BYTES
MAX_JSON = INPUTS.MAX_MANIFEST_BYTES
MAX_ATTACHMENTS = 20_000
SHA = INPUTS.SHA
COMMIT = INPUTS.COMMIT
# These are required execution anchors, not a count-based replacement for the normative assertions.
PULSAR = "com.nereusstream.pulsar.offload.M5NativePulsarOffloadRealTest"
KAFKA = "kafka.server.nsip.KafkaBookKeeperM4AdmissionRealTest"
HISTORY = "com.nereusstream.metadata.oxia.v2.retention.M5RetiredHistoryOxiaIntegrationTest"
RESTART = "com.nereusstream.metadata.oxia.v2.retention.M5RetiredHistoryOxiaRestartTest"
GC_QUOTA = "com.nereusstream.metadata.oxia.v2.retention.M5GcQuotaOxiaIntegrationTest"
RETENTION = "com.nereusstream.metadata.oxia.v2.retention.M5RetentionOxiaIntegrationTest"
RETIREMENT = "com.nereusstream.kafka.bookkeeper.object.nwkcp1.M5ObjectRunRecoveryRetirementRealTest"
CELL = "com.nereusstream.kafka.bookkeeper.object.nwkcp1.M5ObjectCellIsolationRealTest"
ANCHORS = {
    "RESOURCE_UNIQUENESS": ((GC_QUOTA, "memberHistoryStaysChargedAfterPermanentDoneAndNativeReopen"),),
    "REPLACEMENT_RETAINED_MESSAGES": ((KAFKA, "nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache"),
                                       (KAFKA, "nativeM5ReplacedSourceReclaimsAndSettles")),
    "THREE_RECLAMATION_REASONS": ((HISTORY, "actualFloorWriterCannotCrossTheFinalInlineTrimScan"),
                                  (RETENTION, "pulsarLogicalTrimFenceSurvivesActualAppliedReplyLossAndReconnect")),
    "BK_ONLY_COMPACTION": ((KAFKA, "nativeM5BookKeeperOnlyInternalTopicsCompactReclaimAndRecover"),),
    "BK_OUTPUT_FAILURE": ((KAFKA, "nativeM5PublishesItsActualTaskAndFetchesSelectedBytes"),),
    "RETIREMENT_HISTORY_1024": ((HISTORY, "continuousNativeRetirementExceedsLifetimeCapAndReconnectRejectsOldIds"),),
    "RETIREMENT_HISTORY_RECOVERY": ((RESTART, "readAfterServerRestart"),),
    "DONE_CAPACITY": ((GC_QUOTA, "continuousNativeDoneHistoryHasMeasuredPermanentCostAndBoundedHeadAcrossClients"),),
    "READ_FENCED_TAKEOVER": ((PULSAR, "nativeM4ColdOwnerRequiresActualDurableDrain"),),
    "WRITER_RACES": ((PULSAR, "nativeMetadataWriteUnknownUsesActualVersionFence"),
                     (PULSAR, "nativeCursorFloorScansActualCheckpointInsideSingleAuthorityFence")),
    "PULSAR_NATIVE_LIFECYCLE": (
        (PULSAR, "nativeBookKeeperReplacementRequiresExactReleaseAndReconcilesUnknown"),
        (PULSAR, "nativeBookKeeperExpiryRequiresTypedTrimAndRetainsActiveBacklog")),
    "SLOW_UNKNOWN_PUT": ((PULSAR, "nativeMaterializationCancelledOutputObserverRetainsActualPutUntilDrain"),
                          (CELL, "nativePutReplyHeldThenUnknownPreservesBindingsAcrossLanesCellsAndShards")),
    "RUN_RECOVERY_VS_DELETE": ((RETIREMENT,
        "checkpointOnlyEmptyRunTransfersFullStateAndColdRecoversFromSelectedCommonCheckpoint"),),
    "QUARANTINE_EXIT": ((PULSAR, "nativeInitialGrantUnknownWithoutActorRetainsQuarantine"),
                         (PULSAR, "nativeActorlessGrantReconcilesActualDrainAndColdOwner")),
    "KAFKA_STORAGE_REPLAY_LOAD": ((KAFKA, "nativeM5MaterializesAndDrainsItsActualTaskScope"),),
    "CURRENT_SOURCE_REGRESSIONS": ((CELL, "nativePutReplyHeldThenUnknownPreservesBindingsAcrossLanesCellsAndShards"),),
}
CORE_ANCHORS = {
    KINDS[0]: ((PULSAR, "nativeMaterializationWritesRealVersionsAndIndependentlyVerifiesEveryEntry"),
               (PULSAR, "nativeMaterializationAppliedPublicationUnknownRetainsOriginalCandidateUntilHandoff")),
    KINDS[1]: ((KAFKA, "nativeM5BookKeeperOnlyInternalTopicsCompactReclaimAndRecover"),
               (KAFKA, "nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache")),
    KINDS[2]: ((HISTORY, "actualFloorWriterCannotCrossTheFinalInlineTrimScan"),
               (RETIREMENT, "completeSharedRunRetiresAfterEveryMemberTransfersWhileSelectedObjectsRemainReadable")),
    KINDS[3]: ((PULSAR, "nativeOnlyTaskTerminalReferencesRequireOriginalCellDrain"),
               (RETENTION, "nativeM4ReleaseIsConsumedByM5BindingFence")),
    KINDS[4]: ((CELL, "nativePutReplyHeldThenUnknownPreservesBindingsAcrossLanesCellsAndShards"),),
}


# Closed original 79-method owner corpus plus the two actual Native BK lifecycle cases.
EXPECTED_PULSAR81 = (
    'nativeActorlessGrantReconcilesActualDrainAndColdOwner',
    'nativeBookKeeperExpiryRequiresTypedTrimAndRetainsActiveBacklog',
    'nativeBookKeeperReplacementRequiresExactReleaseAndReconcilesUnknown',
    'nativeCancelledInitialOwnerRetainsOriginalGrantCleanup',
    'nativeCellAllocatorRejectsIndependentOwnerUntilActualDrain',
    'nativeCellAllocatorUnknownDrainRetainsOriginalOwner',
    'nativeCellReservationReleaseUnknownRetainsLocalCharge',
    'nativeCellReservationUnknownDoesNotDispatchSource',
    'nativeCompletedInitialOwnerRetainsOriginalGrantCleanup',
    'nativeCursorFloorScansActualCheckpointInsideSingleAuthorityFence',
    'nativeCursorResetRejectsCommittedTrimBeforeOriginalSdkDispatch',
    'nativeCursorResetReplyLossRetainsOriginalCAndDReferences',
    'nativeCursorTrimColdCommitRecoversDurableSnapshotAfterAppliedReplyLoss',
    'nativeEnrolledColdOwnerKeepsActualP1ReadTicketUntilEntryDrain',
    'nativeEnrolledColdOwnerUnknownKeepsOriginalGrantAndCell',
    'nativeEnrolledReadOwnerRejectsMissingControlWriterBeforeGrant',
    'nativeExplicitLacReplyLossRetainsOriginalSdkReferencesAfterFence',
    'nativeGuardedPulsarBirthUnknownCannotCreatePhysicalLedger',
    'nativeGuardedPulsarCreatesDurableBirthsAndMembers',
    'nativeInitialClaimMissingHeadCannotReinitialize',
    'nativeInitialClaimRejectsConcurrentInvocation',
    'nativeInitialClaimUnknownAndAbortUnknownKeepOriginalCharge',
    'nativeInitialGrantUnknownWithoutActorRetainsQuarantine',
    'nativeInitialOwnerUnknownRetainsExactCleanup',
    'nativeM4CancelledVerificationCannotClearSdkLease',
    'nativeM4CellSharesRetainUnknownReaderAndHealthyBinding',
    'nativeM4CleanupFailureRetainsHazard',
    'nativeM4ColdOwnerRequiresActualDurableDrain',
    'nativeM4ColdOwnerUnknownRetainsUniqueAttempt',
    'nativeM4ColdSourceUsesPublishedObjectVersions',
    'nativeM4CompletedVerificationObserverCannotClearSdkLease',
    'nativeM4FallbackRetiresAfterActualGetAndAliasDrain',
    'nativeM4FallbackUnknownRetainsProtectionAndRetriesExact',
    'nativeM4OrdinaryReadCannotFallbackAfterCutChanges',
    'nativeM4OwnerHoldsActualCleanupAndBorrowedAliases',
    'nativeM4PreferredOnlyOwnerRejectsBookKeeperFallback',
    'nativeM4SourceMapUnknownCannotPublishSelector',
    'nativeM4VerificationUsesCachedVersionsAndPrivateDrain',
    'nativeMaterializationActualPutReplyLossUsesOriginalImmutableVersion',
    'nativeMaterializationAppliedPublicationUnknownRetainsOriginalCandidateUntilHandoff',
    'nativeMaterializationBirthCapacityRejectsBeforeAnyProviderOutput',
    'nativeMaterializationBirthCreditUnknownRetainsOriginalCapacityUntilNoDispatchDrain',
    'nativeMaterializationCancelledOutputObserverRetainsActualPutUntilDrain',
    'nativeMaterializationConnectionRetainsOriginalSdkUntilActualGetDrain',
    'nativeMaterializationInputPersistsExactP1ViewAndRecoveryReferences',
    'nativeMaterializationInputUnknownRetainsOriginalCellChargeUntilExactRecovery',
    'nativeMaterializationMappedGetObserverCancelRetainsSdkHazardAndCellUntilDrain',
    'nativeMaterializationPartialCutRetainsOutsideEntryAndActiveTailRoutes',
    'nativeMaterializationPendingInventoryUnknownRetainsOriginalKeysAndCharge',
    'nativeMaterializationProviderScopeUnknownRetainsOriginalSourceAndCellCharge',
    'nativeMaterializationProviderScopeUsesOriginalSdkAndFrozenCellPrefix',
    'nativeMaterializationPublishesActualManifestAfterOriginalOwnerDrain',
    'nativeMaterializationQualificationGetRetainsWorkspaceThroughClose',
    'nativeMaterializationSourceClosesFromOriginalDeliveryCallback',
    'nativeMaterializationSourceUsesActualChainAndEveryEntry',
    'nativeMaterializationStopBeforeSelectorDrainsOriginalPhysicalTickets',
    'nativeMaterializationTaskBindsOriginalPlanScopeOwnerAndSharedCellCharge',
    'nativeMaterializationTaskCapacityRejectsBeforePlanAndOutput',
    'nativeMaterializationTaskCurrentReadRetainsOriginalSourceUntilDrain',
    'nativeMaterializationTaskPlanUnknownRetainsOriginalSourceAndCellCharge',
    'nativeMaterializationTaskUnknownRetainsExclusiveSourceThenAdvancesOriginalWorker',
    'nativeMaterializationWriteIntentUnknownCannotDispatchProviderOutput',
    'nativeMaterializationWritesRealVersionsAndIndependentlyVerifiesEveryEntry',
    'nativeMetadataCleanupUnknownRetainsOriginalSdkCapacity',
    'nativeMetadataCreateFencedAbsenceExcludesLateCreation',
    'nativeMetadataCreatePreDispatchReadFailureReconcilesOriginalTicket',
    'nativeMetadataCreateUnknownReconcilesOriginalCreatedIdentity',
    'nativeMetadataCreateUnknownRejectsForeignCreator',
    'nativeMetadataTicketUnknownRetainsOriginalNoDispatchCleanup',
    'nativeMetadataWriteUnknownKeepsOriginalProofAcrossCleanupLoss',
    'nativeMetadataWriteUnknownRejectsChangedCreationIdentity',
    'nativeMetadataWriteUnknownUsesActualVersionFence',
    'nativeObjectOnlyCursorAndGenerationFloorsSurviveMissingOriginalBookKeeper',
    'nativeOnlyFirstAdmissionPersistsPolicyAndFencesKafkaFloors',
    'nativeOnlyTaskTerminalReferencesRequireOriginalCellDrain',
    'nativePartialMaterializationColdFactoryRetainsOriginalSourceDependencies',
    'nativePreparedSourceRequiresGrantAndDrainsWithOwner',
    'nativeReadOwnerPinsCommittedTrimThroughOriginalEntryDrain',
    'nativeRecoveryAddReplyLossRetainsOriginalSdkReferencesAfterFence',
    'nativeSourcePreparationRetainsHeldRootOpen',
    'nativeSuccessorOwnerUnknownRetainsExactCleanup',
)


EXPECTED_KAFKA57 = (
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_UNKNOWN'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_CLOSE_HELD'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_ROLLOVER'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_ROLLOVER_UNKNOWN'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_CACHE_BUDGET_UNKNOWN'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_COLD'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_RELEASE'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_RELEASE_UNKNOWN'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_RELEASE_DRAIN_HELD'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_RELEASE_COLD'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_RELEASE_ROLLOVER'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_RELEASE_OWNER_UNKNOWN'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_RELEASE_OWNER_CLOSED'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_RELEASE_OWNER_REFRESHED'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_RELEASE_SCOPE_UNKNOWN'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_COLD_SCOPE_HELD'),
    ('nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'TASK_OUTPUT_PUBLISH_RELEASE_COLD_SCOPE_HELD'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_SCOPE_HELD'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_PUBLISH_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_PUBLISH_UNKNOWN_COLD'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_SCOPE_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_SCOPE_UNKNOWN_COLD_SCOPE_HELD'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_FACTS_REFRESH'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_PROOF_REFRESH'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_PROOF_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_PHYSICAL_REFRESH'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_PHYSICAL_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_QUOTA_REFRESH'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_DIRECTORY_REFRESH'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_DIRECTORY_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_INVENTORY_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_DECISION_REFRESH'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_DECISION_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_TASK_TERMINAL_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_READER_SCOPE_REFRESH'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_READER_SCOPE_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_REPLACEMENT_READER_SCOPE_MISSING'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_READER_SCOPE_REFRESH'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_READER_SCOPE_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_READER_OWNER_REFRESH'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_READER_OWNER_UNKNOWN'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_RECOVERY_REFRESH'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_TRANSACTION_COLD'),
    ('nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache', 'TASK_OUTPUT_PUBLISH_RELEASE_RECOMPACT_COLD_REPLACEMENT_SPECULATIVE_PENDING'),
    ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'TASK_OUTPUT'),
    ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'TASK_OUTPUT_READ'),
    ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'TASK_OUTPUT_ROLLOVER'),
    ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'TASK_OUTPUT_APPEND_UNKNOWN'),
    ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'TASK_OUTPUT_CLOSE_UNKNOWN'),
    ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'TASK_OUTPUT_CLOSE_HELD'),
    ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'TASK_OUTPUT_TICKET_UNKNOWN'),
    ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'TASK_OUTPUT_WORKER_UNKNOWN'),
    ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'TASK_OUTPUT_WORKER_STALE'),
    ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'TASK_OUTPUT_EMPTY_KEY'),
)


class EvidenceError(RuntimeError):
    pass


def require(ok, message):
    if not ok:
        raise EvidenceError(message)


def exact(value, keys, label):
    require(isinstance(value, dict) and set(value) == set(keys), f"{label} members differ")
    return value


def integer(value, label, minimum=0):
    require(type(value) is int and minimum <= value <= 9_007_199_254_740_991, f"invalid {label}")
    return value


def digest(value, label):
    require(isinstance(value, str) and SHA.fullmatch(value) is not None, f"invalid {label} digest")
    return value


def domain(value):
    if value is None or type(value) in (bool, str):
        return
    if type(value) is int:
        require(abs(value) <= 9_007_199_254_740_991, "JSON integer exceeds exact domain")
        return
    if isinstance(value, list):
        for item in value:
            domain(item)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for item in value.values():
            domain(item)
        return
    raise EvidenceError("unsupported JSON value")


def canonical(value):
    domain(value)
    return INPUTS.canonical(value)


def load(raw):
    require(0 < len(raw) <= MAX_JSON, "JSON outside bound")
    value = INPUTS.load(raw)
    require(canonical(value) == raw, "noncanonical evidence JSON")
    return value


def relative(value):
    require(isinstance(value, str), "attachment path is not text")
    path = INPUTS.relative(value)
    require(str(path) != ".", "empty attachment path")
    return path


def read(root, value, maximum=MAX_BYTES):
    path = relative(value)
    current = root
    for part in path.parts:
        current = current / part
        require(not current.is_symlink(), "symlink evidence input")
    require(current.resolve(strict=True).is_relative_to(root.resolve(strict=True)), "evidence path escapes root")
    return INPUTS.regular(current, maximum)


def resource(root, value, prefix=None):
    exact(value, ("path", "bytes", "sha256"), "attachment")
    path = relative(value["path"])
    if prefix is not None:
        require(path.is_relative_to(PurePosixPath(prefix)), "attachment escapes its exclusive owner")
    raw = read(root, str(path))
    require(len(raw) == integer(value["bytes"], "attachment bytes")
            and hashlib.sha256(raw).hexdigest() == digest(value["sha256"], "attachment"),
            "attachment bytes or digest differ")
    return raw


def source_node(inputs, repository, path):
    require(repository in INPUTS.REPOSITORIES, "unknown source repository")
    record = inputs["repositories"][repository]["files"].get(str(relative(path)))
    require(record is not None and record != {"deleted": True}, "executed source is absent from complete closure")
    return record


def case_key(stage, case):
    return json.dumps([stage, case["task"], case["class"], case["name"]], separators=(",", ":"))


def matches(case, class_name, method):
    return case["class"] == class_name and re.search(r"(?:^|[\"'])" + re.escape(method) + r"(?:\(|\s|\[)",
                                                    case["name"]) is not None


CLASS_STAGES = {
    PULSAR: "NATIVE_PULSAR", KAFKA: "NATIVE_KAFKA", HISTORY: "BOOKKEEPER_OXIA",
    RESTART: "BOOKKEEPER_OXIA", GC_QUOTA: "BOOKKEEPER_OXIA", RETENTION: "BOOKKEEPER_OXIA",
    RETIREMENT: "OBJECT", CELL: "OBJECT",
}


def require_anchors(cases, anchors, label):
    for class_name, method in anchors:
        require(any(matches(case, class_name, method)
                    and case.get("executionStage") == CLASS_STAGES[class_name] for case in cases),
                f"{label} lacks executed assertion anchor {class_name}.{method}")


def validate_execution(root, record, inputs):
    exact(record, ("schema", "stage", "inputRootSha256", "sourceLocksSha256", "startedAtMicros", "finishedAtMicros",
                   "exitCode", "command", "driver", "workingDirectory", "inputBeforeSha256", "inputAfterSha256",
                   "log", "junit", "runtimeInputs", "boundaries", "attachments", "backendSelections", "cleanup"), "execution")
    require(record["schema"] == "NEREUS_V2_M5_EXECUTION_V1" and record["stage"] in STAGES,
            "unknown execution schema/stage")
    stage = record["stage"]
    require(record["inputRootSha256"] == inputs["inputRootSha256"], "execution input cohort differs")
    require(record["sourceLocksSha256"] == source_node(inputs, "nereus", "docs/v2/source-locks.json")["sha256"],
            "execution source locks differ")
    require(record["inputBeforeSha256"] == inputs["inputRootSha256"]
            and record["inputAfterSha256"] == inputs["inputRootSha256"], "executed input closure changed")
    require(isinstance(record["workingDirectory"], str)
            and record["workingDirectory"].endswith("/workspace/GITHUB/nereus"),
            "execution did not use the isolated complete workspace")
    integer(record["startedAtMicros"], "start timestamp", 1)
    integer(record["finishedAtMicros"], "finish timestamp", 1)
    require(record["finishedAtMicros"] > record["startedAtMicros"] and record["exitCode"] == 0
            and type(record["exitCode"]) is int, "execution did not terminate successfully")
    exact(record["driver"], ("repository", "path", "bytes", "sha256"), "execution driver")
    driver = record["driver"]
    require(driver["repository"] == "nereus" and driver["path"] == STAGES[stage][0], "wrong actual stage driver")
    node = source_node(inputs, "nereus", driver["path"])
    require(driver["bytes"] == node["bytes"] and driver["sha256"] == node["sha256"],
            "driver is not the frozen executed source")
    command = record["command"]
    require(isinstance(command, list) and 2 <= len(command) <= 100
            and all(isinstance(arg, str) and 0 < len(arg) <= 4096 for arg in command), "invalid execution command")
    require(command[0] in ("python3", "bash") and command[1] == driver["path"], "execution command differs from driver")
    attachments = record["attachments"]
    require(isinstance(attachments, list) and 0 < len(attachments) <= MAX_ATTACHMENTS,
            "execution attachment inventory outside bound")
    registered = {}
    for attachment in attachments:
        resource(root, attachment, f"executions/{stage}")
        require(attachment["path"] not in registered, "duplicate execution attachment path")
        registered[attachment["path"]] = attachment
    log = resource(root, record["log"], f"executions/{stage}").decode("utf-8")
    require(registered.get(record["log"]["path"]) == record["log"], "execution log is unregistered")
    require(f"M5_STAGE_EXIT stage={stage} code=0" in log, "actual stage completion is absent")
    require(record["boundaries"] == list(STAGES[stage][1]), "actual boundary set differs")
    exact(record["cleanup"], ("ownedAbsent", "originalUnchanged"), "owned cleanup")
    require(all(value is True for value in record["cleanup"].values()), "native ownership cleanup differs")
    selections = record["backendSelections"]
    require(isinstance(selections, list), "missing actual backend selections")
    selected = set()
    for row in selections:
        exact(row, ("boundary", "facts"), "backend selection")
        require(row["boundary"] in record["boundaries"] and row["boundary"] not in selected,
                "duplicate/foreign backend selection")
        facts = load(resource(root, row["facts"], f"executions/{stage}"))
        validate_backend(root, stage, row["boundary"], facts, record, inputs)
        require(registered.get(row["facts"]["path"]) == row["facts"], "backend facts are unregistered")
        selected.add(row["boundary"])
    require(selected == set(record["boundaries"]), "missing actual backend selection provenance")
    require(isinstance(record["runtimeInputs"], list), "missing actual runtime dependency inventory")
    if record["boundaries"]:
        require(record["runtimeInputs"], "real stage has no actual runtime dependency inputs")
    seen = set()
    for row in record["runtimeInputs"]:
        exact(row, ("sourcePath", "bytes", "sha256"), "runtime input")
        path = row["sourcePath"]
        require(path not in seen and path in inputs["extraInputs"], "runtime input is unregistered or duplicated")
        seen.add(path)
        require(inputs["extraInputs"][path] == {key: row[key] for key in ("bytes", "sha256")},
                "loaded runtime input differs from frozen dependency")
    rows = record["junit"]
    require(isinstance(rows, list) and rows, "execution has no raw test results")
    raw_rows = []
    for row in rows:
        exact(row, ("path", "bytes", "sha256", "task", "producedAtMicros"), "raw JUnit attachment")
        integer(row["producedAtMicros"], "raw XML production timestamp", 1)
        require(record["startedAtMicros"] <= row["producedAtMicros"] <= record["finishedAtMicros"],
                "raw XML was not freshly produced by this execution")
        require(registered.get(row["path"]) == {key: row[key] for key in ("path", "bytes", "sha256")},
                "raw XML is unregistered")
        raw_rows.append({key: row[key] for key in ("path", "bytes", "sha256", "task")})
        require(PurePosixPath(row["path"]).is_relative_to(PurePosixPath(f"executions/{stage}")),
                "JUnit attachment is outside its actual execution")
    verified = JUNIT.validate_suites(raw_rows, lambda path: read(root, path))
    xml_by_case = {}
    for row in raw_rows:
        suite = JUNIT.parse_suite(read(root, row["path"]), row["task"])
        for case in suite["cases"]:
            xml_by_case[case_key(stage, case)] = row["sha256"]
    for case in verified["cases"]:
        require(case["class"] not in CLASS_STAGES or CLASS_STAGES[case["class"]] == stage,
                "test assertion was attributed to the wrong real execution stage")
        case["executionStage"] = stage
        case["xmlSha256"] = xml_by_case[case_key(stage, case)]
    if stage == "NATIVE_KAFKA":
        actual57 = []
        methods = {row[0] for row in EXPECTED_KAFKA57}
        for case in verified["cases"]:
            if case["class"] == KAFKA:
                for method in methods:
                    if matches(case, KAFKA, method):
                        mode = re.fullmatch(re.escape(method) + r"\(String\)\[([1-9][0-9]*)\]([A-Z0-9_]+)",
                                            case["name"])
                        require(mode is not None, "shared Kafka scenario lost its exact mode identity")
                        expected = [value for name, value in EXPECTED_KAFKA57 if name == method]
                        index = int(mode[1])
                        require(index <= len(expected) and expected[index - 1] == mode[2],
                                "shared Kafka scenario index/mode differs from the fixed original array")
                        actual57.append((method, mode[2]))
        require(len(actual57) == 57 and set(actual57) == set(EXPECTED_KAFKA57),
                "the fixed original shared Kafka57 corpus is incomplete or changed")
    actual_paths = inventory(root, f"executions/{stage}") - {f"executions/{stage}/record.json"}
    require(actual_paths == set(registered), "execution archive has missing or unreferenced files")
    verified['inputRootSha256'] = record['inputRootSha256']
    return verified


def validate_child(root, value, executions, inputs, bound_commits=None):
    exact(value, ("schema", "kind", "result", "promotionEligible", "sourceTuple", "inputRootSha256",
                  "executionInputRoots", "attachments", "obligations", "coreCases", "summary", "exclusions"), "child")
    kind = value["kind"]
    require(kind in KINDS and value["schema"] == "NEREUS_V2_M5_CHILD_EVIDENCE_V1", "unknown child schema/kind")
    require(value["result"] == f"PASS_V2_M5_{kind}" and value["promotionEligible"] is False,
            "child cannot independently grant promotion")
    require(value["inputRootSha256"] == inputs["inputRootSha256"], "child input cohort differs")
    expected_tuple = None if bound_commits is None else {
        "commits": bound_commits,
        "sourceLocksSha256": source_node(inputs, "nereus", "docs/v2/source-locks.json")["sha256"],
    }
    require(value["sourceTuple"] == expected_tuple, "child source binding differs")
    require(value["exclusions"] == list(EXCLUSIONS), "child exclusion set differs")
    attachments = value["attachments"]
    require(isinstance(attachments, list) and 0 < len(attachments) <= MAX_ATTACHMENTS, "child attachment bound differs")
    case_ids, attached, paths = {}, {}, set()
    for stage, execution in executions.items():
        for case in execution["cases"]:
            case_ids[case_key(stage, case)] = case
    for row in attachments:
        raw = resource(root, row, f"children/{kind}")
        require(row["path"] not in paths, "duplicate child attachment path")
        paths.add(row["path"])
        attached[row["sha256"]] = raw
    declared = value["obligations"]
    require(isinstance(declared, dict) and set(declared) == {key for key, owner in OWNERS.items() if owner == kind},
            "child semantic ownership differs")
    promotion = set()
    for obligation, identities in declared.items():
        require(isinstance(identities, list) and identities and len(set(identities)) == len(identities),
                "obligation has missing/duplicate assertion references")
        cases = []
        for identity in identities:
            require(identity in case_ids, "obligation references an unexecuted assertion")
            cases.append(case_ids[identity])
            promotion.add(identity)
        require_anchors(cases, ANCHORS[obligation], obligation)
    core = value["coreCases"]
    require(isinstance(core, list) and core and len(set(core)) == len(core), "missing/duplicate core assertions")
    require(all(identity in case_ids for identity in core), "core assertion was not executed")
    require_anchors([case_ids[identity] for identity in core], CORE_ANCHORS[kind], kind)
    promotion.update(core)
    require(value['executionInputRoots'] == {stage: execution['inputRootSha256']
            for stage, execution in executions.items()
            if any(case_ids[identity]['executionStage'] == stage for identity in promotion)},
            'child actual execution input origins differ')
    summary = dict.fromkeys(JUNIT.COUNTERS, 0)
    for identity in promotion:
        case = case_ids[identity]
        # A child's assertion must have its exact underlying raw execution XML in the exclusive archive.
        require(case["xmlSha256"] in attached, "owned assertion lacks its exact raw attachment")
    summary["tests"] = sum(case["xmlSha256"] in attached for case in case_ids.values())
    require(value["summary"] == summary and summary["tests"] > 0, "child test summary differs")
    require(inventory(root, f"children/{kind}") == paths | {f"children/{kind}/receipt.json"},
            "exclusive child archive has missing or unreferenced files")
    return {"kind": kind, "promotionCases": promotion, "paths": paths, "summary": summary}


def inventory(root, prefix):
    directory = root if prefix == "" else root / relative(prefix)
    require(directory.is_dir() and not directory.is_symlink(), "missing regular archive directory")
    paths = set()
    for path in directory.rglob("*"):
        require(not path.is_symlink(), "symlink in evidence archive")
        if path.is_file():
            require(stat.S_ISREG(path.lstat().st_mode), "nonregular evidence node")
            paths.add(path.relative_to(root).as_posix())
        else:
            require(path.is_dir(), "nonregular evidence archive node")
    return paths


IMAGES = {
    "BOOKKEEPER": ("apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d",
                   "sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605"),
    "OXIA": ("nereus/oxia-m3-allocator:37a17bef1720",
             "sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da"),
    "OBJECT": ("quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e",
               "sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253"),
}


def validate_backend(root, stage, boundary, facts, execution, inputs):
    exact(facts, ("schema", "boundary", "implementation", "identity", "rawEvidence"), "backend facts")
    require(facts["schema"] == "NEREUS_V2_M5_BACKEND_SELECTION_V1" and facts["boundary"] == boundary,
            "backend fact identity differs")
    raw = resource(root, facts["rawEvidence"], f"executions/{stage}")
    if boundary in IMAGES:
        require(facts["implementation"] == "PINNED_CONTAINER", "physical backend is not an actual pinned service")
        expected_image, expected_id = IMAGES[boundary]
        exact(facts["identity"], ("image", "imageId", "containerId"), "actual container identity")
        identity = facts["identity"]
        require(identity["image"] == expected_image and identity["imageId"] == expected_id
                and isinstance(identity["containerId"], str)
                and re.fullmatch(r"[0-9a-f]{64}", identity["containerId"]) is not None,
                "backend image identity differs from the admitted profile")
        observed = json.loads(raw, object_pairs_hook=INPUTS.pairs)
        require(isinstance(observed, list) and any(
            row.get("Id") == identity["containerId"] and row.get("Image") == expected_id
            and row.get("Config", {}).get("Image") == expected_image for row in observed),
            "actual native container observation differs")
    else:
        require(boundary in ("NATIVE_KAFKA", "NATIVE_PULSAR")
                and facts["implementation"] == "ACTUAL_NATIVE_SDK", "unknown native boundary")
        exact(facts["identity"], ("repository", "baseCommit", "loadedInputs"), "actual SDK identity")
        repository = "kafka" if boundary == "NATIVE_KAFKA" else "pulsar"
        identity = facts["identity"]
        require(identity["repository"] == repository
                and identity["baseCommit"] == inputs["repositories"][repository]["baseCommit"],
                "native SDK source cohort differs")
        loaded = json.loads(raw, object_pairs_hook=INPUTS.pairs)
        require(isinstance(loaded, dict) and loaded == identity["loadedInputs"] and loaded,
                "actual loaded SDK inventory differs")
        runtime = {row["sourcePath"]: row["sha256"] for row in execution["runtimeInputs"]}
        require(all(path in runtime and runtime[path] == digest(sha, "actual loaded SDK")
                    for path, sha in loaded.items()), "loaded SDK is not an executed frozen runtime input")
        source_root = inputs["repositories"][repository]["sourceRoot"].rstrip("/") + "/"
        require(any(path.startswith(source_root) for path in loaded),
                "native boundary lacks an actually loaded artifact from its captured SDK source")


def fixed_m4():
    return {"testedCommit": HISTORICAL_M4.TESTED, "sourceLocksSha256": HISTORICAL_M4.SOURCE_LOCK_SHA256,
            "finalPath": str(HISTORICAL_M4.FINAL_PATH), "finalSha256": HISTORICAL_M4.FINAL_SHA256}


def validate_m4(root):
    raw = read(root, str(HISTORICAL_M4.FINAL_PATH))
    HISTORICAL_M4.require_fixed_final(raw)
    # Current scenarios belong to M5 at Final. The exact historical Final/children/locks and ancestry remain fixed.
    M4.validate_final_value(root, M4.load_canonical(raw, "immutable M4 Final"), HISTORICAL_M4.TESTED)
    HISTORICAL_M4.require_ancestor(root, HISTORICAL_M4.TESTED, HISTORICAL_M4.CLOSURE)
    HISTORICAL_M4.require_ancestor(root, HISTORICAL_M4.CLOSURE,
                                   INPUTS.git(root, "rev-parse", "HEAD").decode().strip())


def validate_package(root, value, inputs, bound_commits=None):
    keys = ("schema", "kind", "result", "promotionEligible", "executionEvidence", "sourceTuple",
                  "inputRootSha256", "executionInputRoots", "sourceLocksSha256", "m4Dependency", "executionReceipts", "childReceipts",
                  "physicalSelection", "obligations", "scenarios", "sharedPredicates", "deferredScenarios",
                  "openGates", "exclusions", "summary")
    if "stageInputEquivalence" in value:
        keys += ("stageInputEquivalence",)
    exact(value, keys, "M5 aggregate")
    final = bound_commits is not None
    require(value["schema"] == ("NEREUS_V2_M5_FINAL_V1" if final else "NEREUS_V2_M5_COMPLETE_CANDIDATE_V1"),
            "aggregate schema differs")
    require(value["kind"] == ("V2_M5_FINAL" if final else "V2_M5_COMPLETE_CANDIDATE")
            and value["result"] == ("PASS_V2_M5_FINAL" if final else
                                    "PASS_V2_M5_COMPLETE_CANDIDATE_AWAITING_BINDING"), "aggregate phase differs")
    require(value["promotionEligible"] is final and value["executionEvidence"] is True,
            "candidate/Final authority differs")
    require(value["inputRootSha256"] == inputs["inputRootSha256"], "aggregate input cohort differs")
    locks = source_node(inputs, "nereus", "docs/v2/source-locks.json")["sha256"]
    require(value["sourceLocksSha256"] == locks, "aggregate source locks differ")
    expected_tuple = None if not final else {"commits": bound_commits, "sourceLocksSha256": locks}
    require(value["sourceTuple"] == expected_tuple, "aggregate commit binding differs")
    require(value["m4Dependency"] == fixed_m4(), "immutable M4 prerequisite differs")
    require(value["exclusions"] == list(EXCLUSIONS) and value["scenarios"] == list(PROMOTED)
            and value["sharedPredicates"] == list(SHARED_M4)
            and value["deferredScenarios"] == list(DEFERRED)
            and value["openGates"] == ["V2-OPEN-READ-15"], "aggregate scope differs")
    executions = {}
    registered = set()
    equivalence = None
    if "stageInputEquivalence" in value:
        equivalence = EQUIVALENCE.validate(root, value["stageInputEquivalence"], inputs, SimpleNamespace(**globals()))
        registered.update(equivalence["paths"])
    totals = dict.fromkeys(JUNIT.COUNTERS, 0)
    rows = value["executionReceipts"]
    require(isinstance(rows, list) and len(rows) == len(STAGES), "complete five-stage execution inventory is missing")
    for row in rows:
        exact(row, ("stage", "path", "bytes", "sha256"), "execution receipt identity")
        stage = row["stage"]
        require(stage in STAGES and stage not in executions
                and row["path"] == f"executions/{stage}/record.json", "duplicate/foreign execution receipt")
        reference = {key: row[key] for key in ("path", "bytes", "sha256")}
        execution_record = load(resource(root, reference))
        execution_inputs = equivalence['origins'].get(stage, inputs) if equivalence is not None else inputs
        if equivalence is not None and stage == "NATIVE_KAFKA":
            command = execution_record["command"]
            target = equivalence['nativeSourceDirectory']
            require("--source-run" in command and command[command.index("--source-run") + 1] == target,
                    "actual native execution did not consume the verified relocated source input")
        execution = validate_execution(root, execution_record, execution_inputs)
        registered.update(item["path"] for item in execution_record["attachments"])
        require(all(case["executionStage"] == stage for case in execution["cases"]), "execution receipt stage differs")
        executions[stage] = execution
        registered.add(row["path"])
        for key in totals:
            totals[key] += execution["testSummary"][key]
    require(value["summary"] == totals and totals["tests"] > 0, "aggregate raw execution counts differ")
    require(value['executionInputRoots'] == {stage: execution['inputRootSha256']
            for stage, execution in executions.items()}, 'aggregate actual execution input origins differ')
    validate_current_m4(root, executions, inputs)
    children, claimed, paths = {}, {}, set()
    require(isinstance(value["childReceipts"], list) and len(value["childReceipts"]) == len(KINDS),
            "five exclusive evidence children are missing")
    for row in value["childReceipts"]:
        exact(row, ("kind", "result", "path", "bytes", "sha256", "summary"), "child identity")
        kind = row["kind"]
        require(kind in KINDS and kind not in children and row["path"] == f"children/{kind}/receipt.json",
                "duplicate/foreign child")
        reference = {key: row[key] for key in ("path", "bytes", "sha256")}
        child_value = load(resource(root, reference))
        require(row["result"] == child_value["result"] and row["summary"] == child_value["summary"],
                "child descriptor differs from its actual receipt")
        child = validate_child(root, child_value, executions, inputs, bound_commits)
        require(not paths.intersection(child["paths"]), "attachment has multiple exclusive archive owners")
        paths.update(child["paths"])
        for case in child["promotionCases"]:
            require(case not in claimed or claimed[case] == kind, "assertion has multiple semantic promotion owners")
            claimed[case] = kind
        children[kind] = child
        registered.add(row["path"])
        registered.update(child["paths"])
    require(value["obligations"] == {key: {"owner": owner, "result": "VERIFIED_IN_COMPLETE_CANDIDATE"}
                                      for key, owner in OWNERS.items()} | {
        "SOURCE_CHILD_ARCHIVE_AGGREGATE": {"owner": "AGGREGATE", "result": "VERIFIED_IN_COMPLETE_CANDIDATE"}},
        "complete17-obligation ownership/result map differs")
    require(value["physicalSelection"]["path"] == "physical-selection.json", "physical selection path differs")
    physical = load(resource(root, value["physicalSelection"]))
    exact(physical, ("schema", "inputRootSha256", "backendSelectionDigests", "capacitySelections"), "physical selection")
    require(physical["schema"] == "NEREUS_V2_M5_PHYSICAL_SELECTION_V1"
            and physical["inputRootSha256"] == inputs["inputRootSha256"], "physical selection cohort differs")
    selected = {}
    for row in rows:
        execution = load(read(root, row["path"]))
        for backend in execution["backendSelections"]:
            selected[f"{row['stage']}/{backend['boundary']}"] = backend["facts"]["sha256"]
    require(physical["backendSelectionDigests"] == selected, "aggregate physical selection does not bind actual executions")
    require(isinstance(physical["capacitySelections"], list) and physical["capacitySelections"],
            "actual capacity/profile selection roots are missing")
    for row in physical["capacitySelections"]:
        exact(row, ("executionStage", "facts"), "capacity selection")
        require(row["executionStage"] in STAGES, "capacity selection belongs to another execution")
        expected_path = {'NATIVE_KAFKA': 'physical-capacity.json',
                         'NATIVE_PULSAR': 'physical-capacity-pulsar.json'}.get(row['executionStage'])
        require(row['facts']['path'] == expected_path, 'noncanonical aggregate capacity selection')
        facts = load(resource(root, row["facts"]))
        registered.add(row['facts']['path'])
        validate_capacity(root, row["executionStage"], facts, executions[row["executionStage"]])
    require({row["executionStage"] for row in physical["capacitySelections"]} == {"NATIVE_KAFKA", "NATIVE_PULSAR"},
            "fixed native capacity selection is missing or duplicated")
    require(len(physical["capacitySelections"]) == 2, "duplicate capacity selection")
    registered.update({"input-closure.json", "physical-selection.json", "m5-final.json" if final else "candidate.json"})
    if final:
        registered.add("commit-binding.json")
        if (root / "input-objects").exists():
            registered.update(inventory(root, "input-objects"))
    require(inventory(root, "") == registered, "aggregate archive has missing or unreferenced files")
    return {"executions": executions, "children": children, "summary": totals}


def validate_capacity(root, stage, facts, execution):
    exact(facts, ("schema", "executionStage", "canonicalCapacityBytes", "rawEvidence"), "capacity facts")
    require(facts["schema"] == "NEREUS_V2_M5_CAPACITY_SELECTION_V1"
            and facts["executionStage"] == stage and stage in ("NATIVE_KAFKA", "NATIVE_PULSAR")
            and type(facts["canonicalCapacityBytes"]) is int and facts["canonicalCapacityBytes"] == 500_000_000,
            "fixed actual canonical capacity differs")
    evidence = facts["rawEvidence"]
    require(isinstance(evidence, list) and evidence, "actual capacity observations are missing")
    suites = {(row["path"], row["bytes"], row["sha256"]) for row in execution["suites"]}
    seen, observations, observed_tasks = set(), [], {}
    for row in evidence:
        exact(row, ("path", "bytes", "sha256"), "capacity raw XML")
        key = (row["path"], row["bytes"], row["sha256"])
        require(key in suites and key not in seen, "capacity observation is not an exact actual raw suite")
        seen.add(key)
        raw = resource(root, row, "executions/" + stage).decode("utf-8")
        if stage == "NATIVE_KAFKA":
            samples = re.findall(r"M5_CANONICAL_CAPACITY_BYTES ([0-9]+)", raw)
            require(samples and all(value == "500000000" for value in samples),
                    "actual native capacity observation differs from fixed 500M profile")
            continue
        import struct
        import xml.etree.ElementTree as ET
        tree = ET.fromstring(raw)
        task = next(suite['task'] for suite in execution['suites'] if suite['path'] == row['path'])
        require(task not in observed_tasks, 'duplicate Pulsar quota observation task')
        observed_tasks[task] = tree
        samples = [line for node in tree.iter('system-out') for line in (node.text or '').splitlines()
                   if line.startswith('NATIVE_PULSAR_QUOTA ')]
        require(samples, 'actual Pulsar quota raw observations are missing')
        for line in samples:
            fields = line.split()[1:]
            sample = dict(field.split('=', 1) for field in fields)
            require(len(fields) == len(sample), 'duplicate quota observation field')
            exact(sample, ('phase', 'nativeKey', 'version', 'provider', 'service', 'container', 'head',
                           'charged', 'available', 'resource', 'entry', 'done'), 'actual Pulsar quota observation')
            def wire(value, magic=None, size=None):
                try:
                    result = base64.b64decode(value, validate=True)
                except ValueError as error:
                    raise EvidenceError('invalid quota observation base64') from error
                if magic is not None:
                    require(result[:4] == magic and (size is None or len(result) == size)
                            and hashlib.sha256(result[:-32]).digest() == result[-32:],
                            'quota observation wire/checksum differs')
                return result
            def number(value):
                require(re.fullmatch(r'0|[1-9][0-9]*', value) is not None, 'invalid quota observation integer')
                return int(value)
            provider = sample['provider']
            require(provider in ('OBJECT_PROVIDER', 'BOOKKEEPER'), 'unknown quota provider')
            ordinal = int(provider == 'BOOKKEEPER')
            service, container = wire(sample['service']), wire(sample['container'])
            def text(value):
                return struct.pack('>i', len(value)) + value
            native_root = '/nereus/m5-native-authorities-v2/' + hashlib.sha256(
                struct.pack('>i', ordinal + 1) + text(service) + text(container)).hexdigest()
            head_key = native_root + '/v2/physical-delete-quota-v2/head'
            require(sample['nativeKey'] == head_key, 'quota physical namespace identity differs')
            member_key = b'v2/physical-members-m5-v2/' + b'0' * 64 + b'/inventory-v2'
            scope = hashlib.sha256(struct.pack('>ii', 0x4d354d51, 8554) + text(member_key)
                + struct.pack('>i', ordinal) + text(native_root.encode()) + text(service) + text(container)).digest()
            head = wire(sample['head'], b'M5GH')
            version = int.from_bytes(head[4:8], 'big')
            require((version, len(head)) in ((2, 272), (3, 8492)) and head[8:40] == scope,
                    'quota head scope/version differs')
            revision, capacity, reserved, settled, compact_bytes = struct.unpack_from('>5q', head, 40)
            require(revision > 0 and capacity > 0 and min(reserved, settled, compact_bytes) >= 0
                    and settled <= compact_bytes <= settled * 66048 and head[80:240] == bytes(160),
                    'quota head counters or pending reservation differ')
            reservation = len(native_root + '/v2/physical-delete-quota-v2/resources/' + '0' * 64) + 156 \
                + len(native_root + '/v2/physical-delete-m5-v2/' + '0' * 64 + '/authority-v2') + 1048576 \
                + len(native_root + '/' + member_key.decode()) + 8554
            birth = ()
            charged = len(head_key) + 272 + reserved * reservation + settled * (reservation - 1048576) + compact_bytes
            if version == 3:
                issued, assigned, cancelled = struct.unpack_from('>3q', head, 240)
                require(ordinal == 0 and issued > 0 and min(assigned, cancelled) >= 0
                        and assigned + cancelled <= issued and head[264:8460] == bytes(8196),
                        'quota Object birth counters or pending credit differ')
                birth = (issued, assigned, cancelled)
                charged += 8220 + issued * (len(native_root + '/v3/object-birth-credits/' + '0' * 64) + 8192) \
                    + (issued - assigned - cancelled) * reservation
            require(charged <= capacity and number(sample['charged']) == charged
                    and number(sample['available']) == capacity - charged, 'quota canonical charge differs')
            sample.update(rawHead=head, nativeRoot=native_root, versionId=number(sample['version']),
                          serviceBytes=service, containerBytes=container,
                          revision=revision, capacity=capacity, reserved=reserved, settled=settled,
                          compactBytes=compact_bytes, birth=birth, chargedBytes=charged, task=task)
            if provider == 'BOOKKEEPER':
                require(capacity == 500_000_000 and not birth and reserved + settled <= 444,
                        'actual Native BK fixed budget or lifetime resource bound differs')
            if sample['resource'] != 'none':
                require(re.fullmatch('[0-9a-f]{64}', sample['resource']) is not None, 'invalid quota resource identity')
                entry = wire(sample['entry'], b'M5GQ', 156)
                require(entry[4:8] == struct.pack('>i', 2) and entry[8:40] == scope
                        and entry[40:72].hex() == sample['resource'] and struct.unpack_from('>q', entry, 72)[0] >= 2,
                        'permanent quota entry identity differs')
                sample['rawEntry'] = entry
                sample['rawDone'] = b'' if sample['done'] == 'none' else wire(sample['done'], b'M5DC')
            else:
                require(sample['entry'] == sample['done'] == 'none', 'unbound quota entry/DONE observation')
            observations.append(sample)
    if stage == 'NATIVE_KAFKA':
        return
    owner = ':nereus-pulsar-offload:v2M5NativeM4OwnerRealTest'
    negative = ':nereus-pulsar-offload:v2M5NativeOffloadRealTest'
    require(set(observed_tasks) == {owner, negative}, 'complete Pulsar Owner and final namespace observations missing')
    cases = observed_tasks[owner].findall('testcase')
    require(len(cases) == len(EXPECTED_PULSAR81)
            and {case.get('name') for case in cases} == {name + '()' for name in EXPECTED_PULSAR81}
            and all(case.get('classname') == PULSAR for case in cases), 'complete fixed Pulsar81 corpus differs')
    phases = {}
    capacities = {}
    for sample in observations:
        phases.setdefault(sample['phase'], []).append(sample)
        capacities.setdefault(sample['nativeRoot'], set()).add(sample['capacity'])
    require(all(len(values) == 1 for values in capacities.values()), 'Pulsar quota capacity changed during execution')
    require(set(phases) == {'bk-initial', 'bk-final', 'bk-sdk-completion', 'gc-before', 'gc-after',
                            'readmission-before', 'readmission-after', 'bk-gc-before', 'bk-gc-after'},
            'Pulsar quota lifecycle observations incomplete')
    require(all(len(phases[phase]) == 1 for phase in ('bk-initial', 'bk-final', 'bk-sdk-completion')),
            'duplicate Native BK lifecycle observation')
    initial, final, completion = (phases[phase][0] for phase in ('bk-initial', 'bk-final', 'bk-sdk-completion'))
    require(initial['provider'] == final['provider'] == completion['provider'] == 'BOOKKEEPER'
            and initial['nativeRoot'] == final['nativeRoot'] == completion['nativeRoot']
            and initial['reserved'] == initial['settled'] == 0
            and initial['versionId'] <= final['versionId'] <= completion['versionId']
            and final['rawHead'] == completion['rawHead'] and initial['task'] == final['task'] == owner
            and completion['task'] == negative, 'Native BK initial/final same-namespace budget differs')
    def settlements(before_phase, after_phase, provider):
        before = {(s['nativeRoot'], s['resource']): s for s in phases[before_phase]}
        after = {(s['nativeRoot'], s['resource']): s for s in phases[after_phase]}
        require(len(before) == len(phases[before_phase]) == len(after) == len(phases[after_phase])
                and before.keys() == after.keys() and before, 'exact GC resource observation pairs differ')
        for key, old in before.items():
            new = after[key]
            done, entry = new['rawDone'], new['rawEntry']
            require(old['provider'] == new['provider'] == provider and old['task'] == new['task'] == owner
                and old['rawEntry'][80:124] == bytes(44) and not old['rawDone']
                and entry[:80] == old['rawEntry'][:80] and entry[80:84] == struct.pack('>i', 1)
                and 400 < len(done) <= 66048 and done[4:8] == struct.pack('>i', 2)
                and len(done) == 400 + int.from_bytes(done[8:12], 'big')
                and hashlib.sha256(text(b'NEREUS_V2_M5_PHYSICAL_RESOURCE_V2')
                    + text(done[12:12 + int.from_bytes(done[8:12], 'big')])).hexdigest() == key[1]
                and entry[84:116] == hashlib.sha256(done).digest()
                and struct.unpack_from('>q', entry, 116)[0] == len(done)
                and new['reserved'] == old['reserved'] - 1 and new['settled'] == old['settled'] + 1
                and new['compactBytes'] == old['compactBytes'] + len(done) and new['birth'] == old['birth']
                and new['versionId'] > old['versionId'] and new['revision'] > old['revision']
                and new['chargedBytes'] == old['chargedBytes'] - 1048576 + len(done),
                    'actual GC exact entry/DONE settlement or permanent charge differs')
            if provider == 'BOOKKEEPER':
                resource_bytes = done[12:12 + int.from_bytes(done[8:12], 'big')]
                prefix = b'M5RI\x00\x02\x02\x02' + text(old['serviceBytes']) + text(old['containerBytes'])
                require(old['nativeRoot'] == initial['nativeRoot']
                        and len(resource_bytes) == len(prefix) + 8 and resource_bytes.startswith(prefix)
                        and int.from_bytes(resource_bytes[-8:], 'big', signed=True) >= 0,
                        'Native BK settlement resource namespace/kind differs')
        return before, after
    before, after = settlements('gc-before', 'gc-after', 'OBJECT_PROVIDER')
    bk_before, bk_after = settlements('bk-gc-before', 'bk-gc-after', 'BOOKKEEPER')
    require(len(bk_before) == len(bk_after) == 2, 'both Native BK reclamation reasons require exact settlement')
    read_before, read_after = phases['readmission-before'], phases['readmission-after']
    require(len(read_before) == len(read_after) == len(before), 'same-backend readmission observations incomplete')
    for old, new in zip(read_before, read_after):
        require(old['provider'] == new['provider'] == 'OBJECT_PROVIDER' and old['task'] == new['task'] == owner
                and old['nativeRoot'] == new['nativeRoot'] and old['capacity'] == new['capacity']
                and any(old['rawHead'] == settled['rawHead'] and old['nativeRoot'] == settled['nativeRoot']
                        for settled in after.values())
                and new['reserved'] > old['reserved'] and new['settled'] == old['settled']
                and new['compactBytes'] == old['compactBytes'] and new['versionId'] > old['versionId']
                and new['chargedBytes'] > old['chargedBytes'], 'same-backend post-GC readmission differs')


M4_TASKS = {
    "READ_VIEW_HAZARD": (":nereus-storage-object:v2M4ReadViewHazardEvidenceTest",),
    "SOURCE_PLAN_EXECUTION": (":nereus-storage-object:v2M4SourcePlanExecutionEvidenceTest",),
    "QUIESCENCE_PROTECTION_RELEASE": (":nereus-storage-object:v2M4QuiescenceProtectionReleaseEvidenceTest",
                                       ":nereus-metadata-oxia:v2M4ReadControlOxiaAdapterTest"),
    "CURRENT_SOURCE_INTEGRATION_PERFORMANCE": (":nereus-kafka-bookkeeper:v2M4CurrentSourceKafkaTest",
                                              ":nereus-pulsar-offload:v2M4CurrentSourcePulsarTest"),
}

# M5 extends these shared test classes. Keep the historical M4 reader's closed inventory intact.
CURRENT_M4_ADDITIONAL_TESTS = {
    ":nereus-storage-object:v2M4QuiescenceProtectionReleaseEvidenceTest": {
        "capabilityStoredUnderAnotherGenerationCannotAdmitASelector",
        "nativeTakeoverAndSourceMapUpdateFenceTheLocalPredecessorBeforeDurableCas",
    },
    ":nereus-metadata-oxia:v2M4ReadControlOxiaAdapterTest": {
        "compactionRouteAcceptsOnlyItsExactCellKeyAndReconcilesLostCasReply",
        "emptyProtocolDomainTagCannotBeReopenedEvenByARawCasCaller",
        "runRetirementCertificateUsesOneExactCreateOnlyRootRoute",
    },
}


def validate_current_m4(package, executions, inputs):
    suites = [row for execution in executions.values() for row in execution["suites"]]
    for kind, tasks in M4_TASKS.items():
        rows = [row for row in suites if row["task"] in tasks]
        require(len(rows) == len(tasks) and {row["task"] for row in rows} == set(tasks),
                "fresh shared M4 regression tasks are missing or duplicated")
        payload = {"schema": M4.JUNIT_SCHEMA, "childKind": kind,
                   "testedCommit": inputs["repositories"]["nereus"]["baseCommit"], "suites": [
            {"task": row["task"], "bytes": row["bytes"], "xmlSha256": row["sha256"],
             "xmlBase64": base64.b64encode(read(package, row["path"])).decode("ascii")} for row in rows]}
        for row in rows:
            suite = JUNIT.parse_suite(read(package, row["path"]), row["task"])
            expected_class = M4.EXPECTED_SUITES[kind][row["task"]]
            expected_names = (M4.EXPECTED_SUITE_TESTS[kind][row["task"]]
                              | CURRENT_M4_ADDITIONAL_TESTS.get(row["task"], set()))
            names = {case["name"].removesuffix("()") for case in suite["cases"]}
            require(names == expected_names and len(suite["cases"]) == len(expected_names)
                    and all(case["class"] == case["suite"] == expected_class for case in suite["cases"]),
                    "current shared M4 exact test inventory differs: " + row["task"])
            require(all(suite["summary"][key] == 0 for key in ("failures", "errors", "skipped")),
                    "current shared M4 regression has failures, errors or skipped cases")
        # Parse measurements from the complete original XML, without relabeling it as a new M4 Final.
        M4.junit_metrics(payload, kind)


def validate_committed_inputs(package, inputs, roots, commits):
    require(set(roots) == set(INPUTS.REPOSITORIES) and set(commits) == set(INPUTS.REPOSITORIES),
            "Final source binding requires all three repositories")
    INPUTS.validate_value(inputs)
    versioned = {}
    for name in INPUTS.REPOSITORIES:
        tested = commits[name]
        require(isinstance(tested, str) and COMMIT.fullmatch(tested) is not None, "Final uses an incomplete commit ID")
        subprocess.run(["git", "-C", str(roots[name]), "merge-base", "--is-ancestor",
                        inputs["repositories"][name]["baseCommit"], tested], check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        tree = INPUTS.committed_tree(roots[name], tested)
        expected = {path: record for path, record in inputs["repositories"][name]["files"].items()
                    if record != {"deleted": True}}
        require(tree == expected, f"Final {name} commit differs from the actually executed complete tree")
        original = Path(inputs["repositories"][name]["sourceRoot"])
        versioned.update({str(original / path): record for path, record in tree.items() if record["mode"] != "120000"})
    objects = set()
    for source, record in inputs["extraInputs"].items():
        tracked = versioned.get(source)
        if tracked is not None and {key: tracked[key] for key in ("bytes", "sha256")} == record:
            continue
        path = "input-objects/" + record["sha256"]
        raw = read(package, path)
        require(len(raw) == record["bytes"] and hashlib.sha256(raw).hexdigest() == record["sha256"],
                "Final execution dependency bytes are missing or different")
        objects.add(path)
    actual_objects = inventory(package, "input-objects") if (package / "input-objects").exists() else set()
    require(actual_objects == objects, "Final dependency closure contains missing or unregistered objects")


EVIDENCE_PREFIX = PurePosixPath("docs/v2/evidence/v2-m5")
SCENARIOS = PurePosixPath("docs/v2/v2-scenarios.json")
STATUS_DOCS = {
    "docs/v2/08-implementation-plan-and-gates.md", "docs/v2/09-scenario-evidence-matrix.md", "docs/v2/README.md",
    "docs/v2/NSIP/nsip-1.md", "docs/v2/detailed_design/m5/README.md",
    "docs/v2/detailed_design/m5/m5-current-contracts.md", "docs/v2/detailed_design/m5/m5-implementation-log.md",
    "docs/v2/open-questions.md", str(SCENARIOS),
}


def evidence_only(path):
    path = relative(path)
    return path.is_relative_to(EVIDENCE_PREFIX) or str(path) in STATUS_DOCS


def validate_descendants(root, tested):
    head = INPUTS.git(root, "rev-parse", "HEAD").decode().strip()
    subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", tested, head], check=True,
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for descendant in INPUTS.git(root, "rev-list", "--reverse", f"{tested}..{head}").decode().splitlines():
        parents = INPUTS.git(root, "rev-list", "--parents", "-n", "1", descendant).decode().split()
        require(len(parents) == 2, "Final evidence successor is a merge")
        paths = [path.decode() for path in INPUTS.git(root, "diff-tree", "--no-commit-id", "--name-only", "-r", "-z",
                                                    descendant).split(b"\0") if path]
        require(paths and all(evidence_only(path) for path in paths), "runtime/source changed after tested M5 source")
    for args in (("diff", "--name-only", "-z"), ("diff", "--cached", "--name-only", "-z"),
                 ("ls-files", "--others", "--exclude-standard", "-z")):
        changed = [path.decode() for path in INPUTS.git(root, *args).split(b"\0") if path]
        require(all(evidence_only(path) for path in changed), "current source contains untested non-evidence changes")
    return head


def scenario_rows(root):
    value = json.loads(read(root, str(SCENARIOS), MAX_JSON), object_pairs_hook=INPUTS.pairs)
    require(isinstance(value, dict) and isinstance(value.get("scenarios"), list), "scenario registry differs")
    rows = {}
    for row in value["scenarios"]:
        require(isinstance(row, dict) and isinstance(row.get("id"), str) and row["id"] not in rows,
                "duplicate/malformed scenario")
        rows[row["id"]] = row
    return rows


def validate_scenarios(root, final_path):
    rows = scenario_rows(root)
    for identity in PROMOTED:
        require(identity in rows and rows[identity].get("status") == "PASSED_CURRENT_SOURCE"
                and rows[identity].get("evidenceReceipt") == str(final_path),
                f"scenario {identity} does not bind the complete M5 Final")
    for identity in DEFERRED:
        require(identity in rows and rows[identity].get("status") == "PLANNED"
                and rows[identity].get("evidenceReceipt") is None, "M6-deferred scenario was promoted by M5")
    require({identity for identity, row in rows.items() if row.get("evidenceReceipt") == str(final_path)} == set(PROMOTED),
            "Final scenario receipt is borrowed by another predicate")
    require(all("docs/v2/evidence/v2-m5/children/" not in str(row.get("evidenceReceipt")) for row in rows.values()),
            "a scenario cannot promote a focused child")


def current_final_path(root):
    rows = scenario_rows(root)
    receipts = {rows[identity].get("evidenceReceipt") for identity in PROMOTED if identity in rows}
    require(len(receipts) == 1 and None not in receipts, "scenarios do not select one complete M5 Final")
    path = relative(next(iter(receipts)))
    require(path.is_relative_to(EVIDENCE_PREFIX / "final"), "scenario selects a candidate/noncanonical Final")
    return path


def historical_source_commits(root):
    """Verify the published M5 dependency's source tuple without certifying the current checkout."""
    path = current_final_path(root)
    value = load(read(root, str(path), MAX_JSON))
    require(value.get("schema") == "NEREUS_V2_M5_FINAL_V1" and value.get("kind") == "V2_M5_FINAL"
            and value.get("result") == "PASS_V2_M5_FINAL" and value.get("promotionEligible") is True
            and value.get("executionEvidence") is True, "CI requires a published complete M5 Final")
    source = exact(value.get("sourceTuple"), ("commits", "sourceLocksSha256"), "CI Final source tuple")
    commits = exact(source["commits"], INPUTS.REPOSITORIES, "CI Final commits")
    require(all(isinstance(commit, str) and COMMIT.fullmatch(commit) for commit in commits.values()),
            "CI Final requires full commit IDs")
    require(path == EVIDENCE_PREFIX / "final" / f"final-source-{commits['nereus']}" / "m5-final.json",
            "CI Final path differs from its bound source")
    package = root / path.parent
    inputs = load(read(package, "input-closure.json", MAX_JSON))
    INPUTS.validate_value(inputs)
    locks = hashlib.sha256(read(root, "docs/v2/source-locks.json")).hexdigest()
    require(value["sourceLocksSha256"] == source["sourceLocksSha256"] == locks
            and source_node(inputs, "nereus", "docs/v2/source-locks.json")["sha256"] == locks,
            "CI Final source locks differ from the checkout")
    binding = load(read(package, "commit-binding.json", MAX_JSON))
    require(binding == {"schema": "NEREUS_V2_M5_CANDIDATE_COMMIT_EQUIVALENCE_V1",
        "kind": "INPUT_EQUIVALENCE_ONLY", "promotionEligible": False, "executionEvidence": False,
        "inputRootSha256": inputs["inputRootSha256"], "commits": commits}, "CI Final commit binding differs")
    expected = {name: node for name, node in inputs["repositories"]["nereus"]["files"].items()
                if node != {"deleted": True}}
    require(INPUTS.committed_tree(root, commits["nereus"]) == expected,
            "CI Final Root commit differs from its complete input tree")
    subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", commits["nereus"], "HEAD"],
                   check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    validate_scenarios(root, path)
    validate_package(package, value, inputs, commits)
    return commits


def historical_pulsar_commit(root):
    """Select only the SDK from the verified historical source tuple."""
    return historical_source_commits(root)["pulsar"]


def ci_pulsar_commit(root):
    """Retain the original selector's strict current-source successor requirement."""
    commits = historical_source_commits(root)
    validate_descendants(root, commits["nereus"])
    return commits["pulsar"]


def validate_candidate(package, input_closure, repository):
    inputs = INPUTS.verify(input_closure)
    require(repository.resolve(strict=True) ==
            (input_closure / "workspace" / INPUTS.LAYOUT["nereus"]).resolve(strict=True),
            "candidate verification requires its actual isolated source workspace")
    require(source_node(inputs, "nereus", "scripts/check-v2-m5-evidence.py")["sha256"] ==
            hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "candidate validator source differs")
    require(read(package, "input-closure.json", MAX_JSON) == INPUTS.canonical(inputs),
            "candidate does not carry the independently verified complete input manifest")
    validate_m4(repository)
    value = load(read(package, "candidate.json", MAX_JSON))
    return validate_package(package, value, inputs)


def validate_final(root, final_path, roots, check_current=True):
    final_path = relative(str(final_path))
    require(final_path.is_relative_to(EVIDENCE_PREFIX / "final") and final_path.name == "m5-final.json",
            "Final must be a canonical source-bound M5 artifact")
    package = root / final_path.parent
    value = load(read(root, str(final_path), MAX_JSON))
    require(isinstance(value.get("sourceTuple"), dict), "Final is not commit-bound")
    commits = value["sourceTuple"].get("commits")
    require(isinstance(commits, dict) and set(commits) == set(INPUTS.REPOSITORIES), "incomplete Final source tuple")
    require(final_path.parent.name == f"final-source-{commits['nereus']}", "Final path and tested commit differ")
    inputs = load(read(package, "input-closure.json", MAX_JSON))
    binding = load(read(package, "commit-binding.json", MAX_JSON))
    require(binding == {
        "schema": "NEREUS_V2_M5_CANDIDATE_COMMIT_EQUIVALENCE_V1", "kind": "INPUT_EQUIVALENCE_ONLY",
        "promotionEligible": False, "executionEvidence": False,
        "inputRootSha256": inputs["inputRootSha256"], "commits": commits,
    }, "Final has no exact independent executed-input/commit equivalence")
    validate_committed_inputs(package, inputs, roots, commits)
    validate_m4(root)
    verified = validate_package(package, value, inputs, commits)
    if check_current:
        validate_descendants(root, commits["nereus"])
        for name in ("kafka", "pulsar"):
            require(INPUTS.git(roots[name], "rev-parse", "HEAD").decode().strip() == commits[name],
                    "native dependency HEAD changed after the Final source binding")
            require(INPUTS.repository(roots[name])["files"] == {
                path: record for path, record in inputs["repositories"][name]["files"].items()
                if record != {"deleted": True}},
                    "native dependency has untested working-tree inputs")
        validate_scenarios(root, final_path)
    return verified


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("candidate", "final", "ci-pulsar-commit", "historical-pulsar-commit"))
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--package", type=Path)
    parser.add_argument("--input-closure", type=Path)
    parser.add_argument("--final")
    parser.add_argument("--kafka-root", type=Path)
    parser.add_argument("--pulsar-root", type=Path)
    args = parser.parse_args(argv)
    try:
        root = args.repo_root.resolve(strict=True)
        if args.operation == "candidate":
            require(args.package is not None and args.input_closure is not None,
                    "candidate check requires explicit execution package and complete input closure")
            result = validate_candidate(args.package.resolve(strict=True), args.input_closure.resolve(strict=True), root)
            label = "COMPLETE_PRECOMMIT_CANDIDATE_AWAITING_BINDING"
        elif args.operation == "ci-pulsar-commit":
            print(ci_pulsar_commit(root))
            return 0
        elif args.operation == "historical-pulsar-commit":
            print(historical_pulsar_commit(root))
            return 0
        else:
            require(args.kafka_root is not None and args.pulsar_root is not None, "Final source check requires native roots")
            final = relative(args.final) if args.final else current_final_path(root)
            result = validate_final(root, final, {"nereus": root, "kafka": args.kafka_root.resolve(strict=True),
                                                 "pulsar": args.pulsar_root.resolve(strict=True)})
            label = "PASS_V2_M5_FINAL_CURRENT_SOURCE"
    except (EvidenceError, INPUTS.ClosureError, JUNIT.JUnitEvidenceError, M4.EvidenceError,
            HISTORICAL_M4.DependencyError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"M5 evidence: {error}", file=sys.stderr)
        return 1
    print(f"{label} children=5 obligations=17 tests={result['summary']['tests']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
