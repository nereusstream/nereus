#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
design_dir="$repo_root/docs/phase-bk-bookkeeper-primary-wal"
nereus_audit_lock="35c58c575c3da220633c53e48a581f16756ea047"
pulsar_source_lock="52825536806a02eeb2418c9f4a39b0802d33d849"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing BookKeeper primary-WAL documentation contract '$literal' in $path" >&2
        exit 1
    fi
}

required_docs=(
    README.md
    01-current-contract-and-source-audit.md
    02-domain-api-module-and-target-contract.md
    03-oxia-metadata-ledger-lifecycle-and-codecs.md
    04-append-read-recovery-and-fencing.md
    05-retention-materialization-and-completion.md
    06-pulsar-runtime-rollout-and-compatibility.md
    07-implementation-plan-and-gates.md
    08-scenario-evidence-matrix.md
)

for name in "${required_docs[@]}"; do
    if [[ ! -f "$design_dir/$name" ]]; then
        echo "missing BookKeeper primary-WAL design document: $name" >&2
        exit 1
    fi
done

require_literal "$nereus_audit_lock" \
    "docs/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md"
require_literal "$nereus_audit_lock" \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
for path in \
    docs/phase-bk-bookkeeper-primary-wal/README.md \
    docs/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md \
    docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md \
    docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md; do
    require_literal "$pulsar_source_lock" "$path"
done

require_literal "BookKeeper 4.18.0" \
    "docs/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md"
require_literal 'DeleteBuilder.withLedgerId(long)' \
    "docs/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md"
require_literal "F1-BK / BookKeeper Primary WAL Delivery" \
    "docs/phase-bk-bookkeeper-primary-wal/README.md"
require_literal "explicit implementation evidence" \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY' \
    "docs/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'CreateAdvBuilder.withLedgerId(long)' \
    "docs/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'reserved-namespace exact ledger id' \
    "docs/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'maxAppendRangesPerLedger' \
    "docs/phase-bk-bookkeeper-primary-wal/02-domain-api-module-and-target-contract.md"
require_literal 'CREATE_UNCERTAIN' \
    "docs/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md"
require_literal 'BookKeeperAllocationSlotRecord' \
    "docs/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md"
require_literal 'lateCreateHazard' \
    "docs/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md"
require_literal '${rangeSlot:05d}/${protectionSlot:02d}' \
    "docs/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md"
require_literal '${readerSlot:05d}' \
    "docs/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md"
require_literal 'BookKeeperLedgerIdNamespaceReservationRecord' \
    "docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md"
require_literal 'nereus.bookkeeper-primary-wal-activation' \
    "docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md"
require_literal 'BookKeeperLedgerIdNamespaceProvisioningCoordinator' \
    "docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperLedgerIdNamespaceProvisioningCoordinator.java"
require_literal 'BookKeeperProtocolActivationCoordinator' \
    "docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperProtocolActivationCoordinator.java"
require_literal 'NBKA1' \
    "docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperProtocolActivationCodecV1.java"
require_literal 'NBKAP1' \
    "docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperProtocolActivation.java"
require_literal 'BookKeeperBrokerReadinessProvider' \
    "docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperBrokerReadinessProvider.java"
require_literal 'activationKeepsPublicationIdentityStableButRebindsEveryDeletionRecord' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperProtocolAdministrationTest.java"
require_literal 'deletionReadinessUsesTheStablePublicationBindingAndInvalidatesOnDrift' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'namespaceProvisionIsIdempotentAndRevokeIsTerminalVersionedCas' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperProtocolAdministrationTest.java"
require_literal 'withRecovery(false)' \
    "docs/phase-bk-bookkeeper-primary-wal/04-append-read-recovery-and-fencing.md"
require_literal 'REQUIRED_OBJECT_GENERATION' \
    "docs/phase-bk-bookkeeper-primary-wal/05-retention-materialization-and-completion.md"
require_literal 'CommittedObjectGenerationAuthority' \
    "docs/phase-bk-bookkeeper-primary-wal/05-retention-materialization-and-completion.md"
require_literal 'BookKeeperSealedLedgerMaterializationTrigger' \
    "docs/phase-bk-bookkeeper-primary-wal/05-retention-materialization-and-completion.md"
require_literal 'BookKeeperLedgerRetentionService' \
    "docs/phase-bk-bookkeeper-primary-wal/05-retention-materialization-and-completion.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperLedgerRetentionService.java"
require_literal 'scansEveryShardAndRoutesOnlyTheExactBinding' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperLedgerRetentionScannerTest.java"
require_literal 'oneRootFailureIsAccountedAndDoesNotStopLaterRootsOrShards' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperLedgerRetentionScannerTest.java"
require_literal 'bookKeeperPrimaryWalM3SourceRetirementCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3LiveReadCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3SealedLedgerCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3RealServiceCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3PhysicalRetirementCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3ResponseLossCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3LagFailureCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3FinalCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2StableRecoveryCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2IsolationRetentionCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2AllocationAuthorityCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2Check' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2FinalCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'No online migration in BK-M0–M6' \
    "docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md"
require_literal 'BK-96' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'bookKeeperPrimaryWalDocumentationCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M0 design/source audit       documentation-gated on 2026-07-19' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M1 provider-neutral foundation complete/final-gated on 2026-07-19' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M2 BOOKKEEPER_WAL_ONLY       complete/final-gated on 2026-07-20' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M3 BOOKKEEPER_WAL_ASYNC_OBJECT complete/final-gated on 2026-07-20' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M4 BOOKKEEPER_WAL_SYNC_OBJECT complete/final-gated on 2026-07-20' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2FinalCheck` passed its 212-task aggregate' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3FinalCheck` passed its 223-task aggregate' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'multiEntryAppendUsesOneExactConsecutiveBookKeeperRange' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'byteRangeAndAgeRolloverPreserveWholeBatchesAndDenseOffsets' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'BookKeeperAppenderResourceTest.releasesEveryOwnedResource' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'void releasesEveryOwnedResource()' \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppenderResourceTest.java"
require_literal 'BookKeeperAppenderDeadlineTest.propagatesRemainingBudget' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'void propagatesRemainingBudget()' \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppenderDeadlineTest.java"
require_literal 'BookKeeperLedgerRecoveryTest.recoversEverySealCut' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'void recoversEverySealCut()' \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperLedgerRecoveryTest.java"
require_literal 'newOwnerRecoveryOpenFencesLiveOldHandleAndPreventsOldHeadCommit' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'realReaderNeverRecoveryOpensVerifiesWholeRangeBeforeClippingAndFailsClosedOnChecksumDrift' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'realReaderFailsClosedOnCountIdAndConfigurationDrift' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'referenceAppearingAfterMarkUnmarksToSealedBeforeDelete' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperWalRetentionGateTest.java"
require_literal 'disabledAndDryRunGcNeverMutateRootOrProvider' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperWalRetentionGateTest.java"
require_literal 'referenceAfterMarkUnmarksAndSafeGcModesNeverDelete' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'oneProcessSharesOneRenewableSlotUntilItsFinalLocalRelease' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperReaderLeaseManagerTest.java"
require_literal 'fixedSlotsBoundIndependentProcessesWithoutDeletingForeignOccupants' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperReaderLeaseManagerTest.java"
require_literal 'finalRevalidationFailsWhenTheExactDurableLeaseDisappears' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperReaderLeaseManagerTest.java"
require_literal 'renewalFailureDoesNotLeakTheRememberedDurableSlot' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperReaderLeaseManagerTest.java"
require_literal 'realReaderSlotsArePerProcessBoundedAndFinalPinRevalidationFailsClosed' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'foreignLedgerRecreationAndNamespaceDriftStopBeforePhysicalDelete' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'partialRangeAndMixedLedgerTrimNeverDeleteLiveBookKeeperBytes' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'firstMiddleAndLastWriteFailureSealTheLedgerBeforeReuse' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'reachableHeadRecoveryRepairsGenerationZeroWithoutRewritingBookKeeper' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperStreamStorageIntegrationTest.java"
require_literal 'defaultAdapterMakesDeferredSyncUnrepresentableAndAlwaysUsesEmptyWriteFlags' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperClientApiContractTest.java"
require_literal 'BookKeeperAppendReservationIds' \
    "docs/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperAppendReservationIds.java"
require_literal 'BookKeeperAppendRecoveryCoordinator' \
    "docs/phase-bk-bookkeeper-primary-wal/04-append-read-recovery-and-fencing.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperAppendRecoveryCoordinator.java"
require_literal 'currentSessionCommitsTheSameDurableRangeWithoutAnotherBookKeeperWrite' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppendRecoveryCoordinatorTest.java"
require_literal 'newSessionAbandonsUnreachableDurableRangeAndAllocatesAnotherLedger' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppendRecoveryCoordinatorTest.java"
require_literal 'restartRecoveryReusesCurrentSessionRangeAndFencesExpiredSessionRange' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'bookKeeperPrimaryWalDocumentationCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM1Check' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2MetadataCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2AllocatorCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2AppendReadCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2RecoveryFencingCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2RuntimeCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2RetentionCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2PulsarCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2RealServiceCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3ExactSourceCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3ProtectionCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3AsyncProfileCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3LagCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3SourceRetirementCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3LiveReadCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3SealedLedgerCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3Check' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3RealServiceCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3PhysicalRetirementCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3ResponseLossCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3LagFailureCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM3FinalCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM4CompletionPolicyCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM4TaskReuseCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM4KnownCommittedCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM4ReadAdmissionCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM4Check' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM4FinalCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM5ConfigurationCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM5CapabilityCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM5FirstCreateCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM5BorrowedClientCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM5RetentionCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM5DeletionActivationCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM4Check --rerun-tasks` passes 62/62 executable tasks' \
    "docs/phase-bk-bookkeeper-primary-wal/README.md" \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM4FinalCheck --rerun-tasks` passes its 215-task aggregate' \
    "docs/phase-bk-bookkeeper-primary-wal/README.md" \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2StableRecoveryCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2IsolationRetentionCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2AllocationAuthorityCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2Check' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM2FinalCheck' "build.gradle.kts"
require_literal 'realOxiaStableAppendResponseLossReusesOneBookKeeperRangeAndRepairsGenerationZero' \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'foreignLedgerCreatedAtProviderBoundaryIsQuarantinedAndNeverDeletedBeforeFreshCandidateWins' \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'realOxiaProtectionCartesianBoundPreservesTaskRepairAndMandatoryVetoes' \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'productionAdapterReloadsEveryAppliedBookKeeperMutationResponseLoss' \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/metadata/oxia/BookKeeperMetadataOxiaResponseLossIntegrationTest.java"
require_literal 'twoStreamsChoosingOneCandidateConvergeToTwoOwnedLedgersWithoutDelete' \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/bookkeeper/BookKeeperAllocatorOxiaBkContentionIntegrationTest.java"
require_literal 'stableHeadFallsBackToBookKeeperThenFreshRuntimePublishesAndReadsExactObject' \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'freshRuntimesConvergeAppliedTaskSourceOutputAndPublicationResponseLoss' \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'sharedRealLagAdmissionRejectsBeforeBookKeeperIoAndRecoversAfterObjectCoverage' \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'missingCommittedObjectVetoesBookKeeperRetirementAndFallsBackToExactRange' \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'syncObjectAcknowledgesOnlyAfterExactObjectGenerationIsNormallyReadable' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'syncObjectUnreadablePublicationIsKnownCommittedAndRecoveryReusesBkRange' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'syncObjectKeepsBkVisibleWhileProducerWaitsForObjectPublication' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'syncObjectSequentialAppendsKeepOneDeterministicTaskPerBkRange' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'syncRestartWaitsForExactObjectProofWithoutAnotherBookKeeperWrite' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppendRecoveryCoordinatorTest.java"
require_literal 'void throttlesAndRejectsBeforeWal' \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAsyncAdmissionTest.java"
require_literal 'readsBookKeeperSourceThroughRegisteredProviderWithoutObjectIdentityOrPin' \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/ExactSourceRangeReaderTest.java"
require_literal 'reconstructsBookKeeperProtectionThroughTheProviderRegistry' \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/MaterializationTaskProtectionReconcilerTest.java"
require_literal 'measuresBookKeeperAsyncObjectWithTheSharedLagAuthority' \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/DefaultMaterializationLagSnapshotReaderTest.java"
require_literal 'requiresExactCommittedIndexActiveRootVisibleProtectionAndCoveringCheckpoint' \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/CommittedObjectGenerationAuthorityTest.java"
require_literal 'retirementProofAcquiresNormalPinsAndReadsTheExactGenerationEndToEnd' \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/NormalPathCommittedObjectGenerationReadVerifierTest.java"
require_literal 'bookKeeperAsyncFallsBackToProviderProtectedGenerationZeroWithoutObjectPin' \
    "nereus-core/src/test/java/com/nereusstream/core/read/GenerationReadResolverTest.java"
require_literal 'retiresTerminalTaskWithProviderOwnedBookKeeperSourceProtection' \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/TerminalWorkflowMetadataRetirementTest.java"
require_literal 'plansFinalSingleBookKeeperGenerationZeroThroughTheOrdinaryPlanner' \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/MaterializationPlannerTest.java"
require_literal 'healthyCommittedObjectGenerationRetiresOnlyMandatoryAsyncSourceReferences' \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperWalRetentionGateTest.java"
require_literal 'sealedLedgerTriggerRevalidatesExactRootAndUsesTheSharedMaterializationScanner' \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperWalRetentionGateTest.java"
require_literal 'BookKeeperUncertainAllocationReconciler' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'class BookKeeperUncertainAllocationReconciler' \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperUncertainAllocationReconciler.java"
require_literal 'BookKeeperWriterStatePropertyTest' "nereus-bookkeeper/build.gradle.kts"
require_literal 'realOxiaColdScanCoversEveryRootAndAllocationSlotShard' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'void realOxiaColdScanCoversEveryRootAndAllocationSlotShard' \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'class BookKeeperLedgerTransitionsTest' \
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/BookKeeperLedgerTransitionsTest.java"

scenario_matrix="$design_dir/08-scenario-evidence-matrix.md"
scenario_count="$(rg -c '^\| BK-[0-9]+ \|' "$scenario_matrix")"
if [[ "$scenario_count" != "96" ]]; then
    echo "BookKeeper primary-WAL scenario matrix must contain exactly 96 rows, found $scenario_count" >&2
    exit 1
fi
for ((number = 1; number <= 96; number++)); do
    printf -v scenario_id 'BK-%02d' "$number"
    occurrence_count="$(rg -F -c -- "| $scenario_id |" "$scenario_matrix" || true)"
    if [[ "$occurrence_count" != "1" ]]; then
        echo "BookKeeper primary-WAL scenario matrix must contain $scenario_id exactly once" >&2
        exit 1
    fi
done

if [[ ! -d "$repo_root/nereus-bookkeeper" ]]; then
    echo "nereus-bookkeeper is missing while F1-BK documents BK-M1 as complete" >&2
    exit 1
fi
if [[ ! -x "$repo_root/scripts/check-bookkeeper-module-boundaries.sh" ]]; then
    echo "BookKeeper module-boundary gate is missing or not executable" >&2
    exit 1
fi
for unfinished_task in \
    bookKeeperPrimaryWalM5AdminRoutingCheck \
    bookKeeperPrimaryWalM5TwoBrokerCheck \
    bookKeeperPrimaryWalM5Check \
    bookKeeperPrimaryWalM5FinalCheck \
    bookKeeperPrimaryWalM6ScenarioEvidenceCheck \
    bookKeeperPrimaryWalM6ScaleCheck \
    bookKeeperPrimaryWalM6ChaosCheck \
    bookKeeperPrimaryWalM6CompatibilityCheck \
    bookKeeperPrimaryWalM6Check \
    bookKeeperPrimaryWalM6FinalCheck; do
    if rg --pcre2 -n "tasks\\.register[^\\n]*\"${unfinished_task}\"" \
        "$repo_root/build.gradle.kts"; then
        echo "unfinished task $unfinished_task must not be registered before executable implementation exists" >&2
        exit 1
    fi
done

global_links=(
    README.md
    docs/design/README.md
    docs/design/nereus-design-index.md
    docs/design/nereus-futures.md
    docs/design/nereus-future1-core-stream-storage.md
    docs/design/nereus-commit-protocol.md
    docs/design/nereus-overall-architecture.md
    docs/design/nereus-terminology.md
    docs/phase-1.5-core-storage-foundation/README.md
    docs/phase-4-compaction-generation/README.md
)
for path in "${global_links[@]}"; do
    require_literal "phase-bk-bookkeeper-primary-wal" "$path"
done

if rg -n --glob '*.md' \
    'BK-M[5-6][[:space:]:=-]+(complete|final-gated)|BookKeeper primary WAL[[:space:]:=-]+Implemented|Implemented[[:space:]:=-]+BookKeeper primary WAL' \
    "$design_dir"; then
    echo "BookKeeper primary-WAL design incorrectly claims BK-M5-M6 or whole-delivery completion" >&2
    exit 1
fi

while IFS=: read -r source match; do
    target="${match#*](}"
    target="${target%)}"
    target="${target#<}"
    target="${target%>}"
    case "$target" in
        ""|\#*|*://*|mailto:*|app://*) continue ;;
    esac
    target="${target%%#*}"
    target="${target//%20/ }"
    if [[ "$target" = /* ]]; then
        resolved="$target"
    else
        resolved="$(dirname "$source")/$target"
    fi
    if [[ ! -e "$resolved" ]]; then
        echo "broken local Markdown link in ${source#"$repo_root/"}: $target" >&2
        exit 1
    fi
done < <(rg --with-filename --no-heading -o --glob '*.md' '\]\(([^)]+)\)' "$design_dir")

echo "BookKeeper primary-WAL design/implementation status, source locks, contracts, links, and scenario range verified."
