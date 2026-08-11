#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
design_dir="$repo_root/docs/v1/phase-bk-bookkeeper-primary-wal"
nereus_audit_lock="35c58c575c3da220633c53e48a581f16756ea047"
pulsar_source_lock="2f9c1eb93be96e2036fbdc8c5e39545f21fa6200"

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
    09-m6-executable-evidence-matrix.md
)

for name in "${required_docs[@]}"; do
    if [[ ! -f "$design_dir/$name" ]]; then
        echo "missing BookKeeper primary-WAL design document: $name" >&2
        exit 1
    fi
done

require_literal "$nereus_audit_lock" \
    "docs/v1/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md"
require_literal "$nereus_audit_lock" \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
for path in \
    docs/v1/phase-bk-bookkeeper-primary-wal/README.md \
    docs/v1/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md \
    docs/v1/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md \
    docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md; do
    require_literal "$pulsar_source_lock" "$path"
done

require_literal "BookKeeper 4.18.0" \
    "docs/v1/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md"
require_literal 'DeleteBuilder.withLedgerId(long)' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md"
require_literal "F1-BK / BookKeeper Primary WAL Delivery" \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md"
require_literal "explicit implementation evidence" \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'CreateAdvBuilder.withLedgerId(long)' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'reserved-namespace exact ledger id' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'maxAppendRangesPerLedger' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/02-domain-api-module-and-target-contract.md"
require_literal 'CREATE_UNCERTAIN' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md"
require_literal 'BookKeeperAllocationSlotRecord' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md"
require_literal 'lateCreateHazard' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md"
require_literal '${rangeSlot:05d}/${protectionSlot:02d}' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md"
require_literal '${readerSlot:05d}' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md"
require_literal 'BookKeeperLedgerIdNamespaceReservationRecord' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md"
require_literal 'nereus.bookkeeper-primary-wal-activation' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md"
require_literal 'BookKeeperLedgerIdNamespaceProvisioningCoordinator' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperLedgerIdNamespaceProvisioningCoordinator.java"
require_literal 'BookKeeperProtocolActivationCoordinator' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperProtocolActivationCoordinator.java"
require_literal 'NBKA1' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperProtocolActivationCodecV1.java"
require_literal 'NBKAP1' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperProtocolActivation.java"
require_literal 'BookKeeperBrokerReadinessProvider' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperBrokerReadinessProvider.java"
require_literal 'activationKeepsPublicationIdentityStableButRebindsEveryDeletionRecord' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperProtocolAdministrationTest.java"
require_literal 'deletionReadinessUsesTheStablePublicationBindingAndInvalidatesOnDrift' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'namespaceProvisionIsIdempotentAndRevokeIsTerminalVersionedCas' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperProtocolAdministrationTest.java"
require_literal 'withRecovery(false)' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/04-append-read-recovery-and-fencing.md"
require_literal 'REQUIRED_OBJECT_GENERATION' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/05-retention-materialization-and-completion.md"
require_literal 'CommittedObjectGenerationAuthority' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/05-retention-materialization-and-completion.md"
require_literal 'BookKeeperSealedLedgerMaterializationTrigger' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/05-retention-materialization-and-completion.md"
require_literal 'BookKeeperLedgerRetentionService' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/05-retention-materialization-and-completion.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperLedgerRetentionService.java"
require_literal 'scansEveryShardAndRoutesOnlyTheExactBinding' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperLedgerRetentionScannerTest.java"
require_literal 'oneRootFailureIsAccountedAndDoesNotStopLaterRootsOrShards' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperLedgerRetentionScannerTest.java"
require_literal 'bookKeeperPrimaryWalM3SourceRetirementCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3LiveReadCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3SealedLedgerCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3RealServiceCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3PhysicalRetirementCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3ResponseLossCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3LagFailureCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3FinalCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2StableRecoveryCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2IsolationRetentionCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2AllocationAuthorityCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2Check' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM2FinalCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'No online migration in BK-M0–M6' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md"
require_literal 'BK-96' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'bookKeeperPrimaryWalDocumentationCheck' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M0 design/source audit       documentation-gated on 2026-07-19' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M1 provider-neutral foundation complete/final-gated on 2026-07-19' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M2 BOOKKEEPER_WAL_ONLY       complete/final-gated on 2026-07-20' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M3 BOOKKEEPER_WAL_ASYNC_OBJECT complete/final-gated on 2026-07-20' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M4 BOOKKEEPER_WAL_SYNC_OBJECT complete/final-gated on 2026-07-20' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M5 Pulsar rollout            complete/final-gated on 2026-07-22' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BK-M6 aggregate final gate      complete/final-gated on 2026-07-22' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM6Check --rerun-tasks`：123/123 outer tasks in 10m22s' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalFinalCheck --rerun-tasks`：236/236 outer tasks in 30m57s' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'BookKeeper Primary WAL Delivery is complete/final-gated' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'BK-M6 and the complete F1-BK / BookKeeper Primary WAL Delivery are complete/final-gated' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/09-m6-executable-evidence-matrix.md"
require_literal "$pulsar_source_lock" \
    "docs/v1/phase-bk-bookkeeper-primary-wal/09-m6-executable-evidence-matrix.md"
require_literal 'bookKeeperPrimaryWalM5Check --rerun-tasks` passes 105/105 outer tasks in 6m47s' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'bookKeeperPrimaryWalM5FinalCheck --rerun-tasks` then passes' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md"
require_literal '231/231 fresh tasks in 27m42s' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'bookKeeperPrimaryWalM2FinalCheck` passed its 212-task aggregate' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM3FinalCheck` passed its 223-task aggregate' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'multiEntryAppendUsesOneExactConsecutiveBookKeeperRange' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'byteRangeAndAgeRolloverPreserveWholeBatchesAndDenseOffsets' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'BookKeeperAppenderResourceTest.releasesEveryOwnedResource' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'void releasesEveryOwnedResource()' \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppenderResourceTest.java"
require_literal 'BookKeeperAppenderDeadlineTest.propagatesRemainingBudget' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'void propagatesRemainingBudget()' \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppenderDeadlineTest.java"
require_literal 'BookKeeperLedgerRecoveryTest.recoversEverySealCut' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'void recoversEverySealCut()' \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperLedgerRecoveryTest.java"
require_literal 'newOwnerRecoveryOpenFencesLiveOldHandleAndPreventsOldHeadCommit' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'realReaderNeverRecoveryOpensVerifiesWholeRangeBeforeClippingAndFailsClosedOnChecksumDrift' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'realReaderFailsClosedOnCountIdAndConfigurationDrift' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'referenceAppearingAfterMarkUnmarksToSealedBeforeDelete' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperWalRetentionGateTest.java"
require_literal 'disabledAndDryRunGcNeverMutateRootOrProvider' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperWalRetentionGateTest.java"
require_literal 'referenceAfterMarkUnmarksAndSafeGcModesNeverDelete' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'oneProcessSharesOneRenewableSlotUntilItsFinalLocalRelease' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperReaderLeaseManagerTest.java"
require_literal 'fixedSlotsBoundIndependentProcessesWithoutDeletingForeignOccupants' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperReaderLeaseManagerTest.java"
require_literal 'finalRevalidationFailsWhenTheExactDurableLeaseDisappears' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperReaderLeaseManagerTest.java"
require_literal 'renewalFailureDoesNotLeakTheRememberedDurableSlot' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperReaderLeaseManagerTest.java"
require_literal 'realReaderSlotsArePerProcessBoundedAndFinalPinRevalidationFailsClosed' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'foreignLedgerRecreationAndNamespaceDriftStopBeforePhysicalDelete' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'partialRangeAndMixedLedgerTrimNeverDeleteLiveBookKeeperBytes' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'firstMiddleAndLastWriteFailureSealTheLedgerBeforeReuse' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java"
require_literal 'reachableHeadRecoveryRepairsGenerationZeroWithoutRewritingBookKeeper' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperStreamStorageIntegrationTest.java"
require_literal 'defaultAdapterMakesDeferredSyncUnrepresentableAndAlwaysUsesEmptyWriteFlags' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperClientApiContractTest.java"
require_literal 'BookKeeperAppendReservationIds' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperAppendReservationIds.java"
require_literal 'BookKeeperAppendRecoveryCoordinator' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/04-append-read-recovery-and-fencing.md" \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperAppendRecoveryCoordinator.java"
require_literal 'currentSessionCommitsTheSameDurableRangeWithoutAnotherBookKeeperWrite' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppendRecoveryCoordinatorTest.java"
require_literal 'newSessionAbandonsUnreachableDurableRangeAndAllocatesAnotherLedger' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppendRecoveryCoordinatorTest.java"
require_literal 'restartRecoveryReusesCurrentSessionRangeAndFencesExpiredSessionRange' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
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
require_literal 'checkBookKeeperPrimaryWalM5AdminRoutingContractSurface' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM5AdminRoutingCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM5TwoBrokerCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM5Check' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalM5FinalCheck' "build.gradle.kts"
require_literal 'NereusBookKeeperMultiBrokerIntegrationTest.preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'NereusBookKeeperMultiBrokerIntegrationTest.preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'NereusMixedPrimaryProfilesMultiBrokerTest.coexistsAcrossProfiles' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'NereusBookKeeperCapabilityRolloverTest.excludesOldBroker' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'NereusBookKeeperCapabilityRolloverTest.reestablishesExactAuthority' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'bookKeeperWalOnlyResolvesGenerationZeroWithoutAdmittingHigherGenerations' \
    "nereus-core/src/test/java/com/nereusstream/core/read/GenerationReadResolverTest.java"
require_literal 'assertThat(reusedAfterReset).isSameAs(navigation)' \
    "nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/NereusManagedLedgerFacadeTest.java"
require_literal 'bookKeeperPrimaryWalM5AdminRoutingCheck --rerun-tasks` passes 103/103 outer tasks in 3m32s' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md" \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM5TwoBrokerCheck --rerun-tasks` passes 104/104 outer tasks in 4m59s' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'bookKeeperPrimaryWalM5TwoBrokerCheck --rerun-tasks` passes 104/104 outer tasks in 4m59s' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM4Check --rerun-tasks` passes 62/62 executable tasks' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md" \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalM4FinalCheck --rerun-tasks` passes its 215-task aggregate' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/README.md" \
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
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
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'syncObjectUnreadablePublicationIsKnownCommittedAndRecoveryReusesBkRange' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'syncObjectKeepsBkVisibleWhileProducerWaitsForObjectPublication' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'syncObjectSequentialAppendsKeepOneDeterministicTaskPerBkRange' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
    "nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java"
require_literal 'syncRestartWaitsForExactObjectProofWithoutAnotherBookKeeperWrite' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md" \
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
    "docs/v1/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'class BookKeeperUncertainAllocationReconciler' \
    "nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperUncertainAllocationReconciler.java"
require_literal 'BookKeeperWriterStatePropertyTest' "nereus-bookkeeper/build.gradle.kts"
require_literal 'realOxiaColdScanCoversEveryRootAndAllocationSlotShard' \
    "docs/v1/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
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
for implemented_task in \
    bookKeeperPrimaryWalM6ScenarioEvidenceCheck \
    bookKeeperPrimaryWalM6ScaleCheck \
    bookKeeperPrimaryWalM6ChaosCheck \
    bookKeeperPrimaryWalM6CompatibilityCheck \
    bookKeeperPrimaryWalM6Check \
    bookKeeperPrimaryWalM6FinalCheck; do
    if ! rg --pcre2 -q "tasks\\.register[^\\n]*\"${implemented_task}\"" \
        "$repo_root/build.gradle.kts"; then
        echo "implemented task $implemented_task is not registered" >&2
        exit 1
    fi
done
require_literal 'bookKeeperPrimaryWalCheck' "build.gradle.kts"
require_literal 'bookKeeperPrimaryWalFinalCheck' "build.gradle.kts"

if [[ ! -x "$repo_root/scripts/check-bookkeeper-primary-wal-m6-scenario-evidence.sh" ]]; then
    echo "BookKeeper M6 executable scenario-evidence gate is missing or not executable" >&2
    exit 1
fi

if [[ ! -x "$repo_root/scripts/check-bookkeeper-primary-wal-m5-admin-routing-contract-surface.sh" ]]; then
    echo "BookKeeper M5 admin-routing contract gate is missing or not executable" >&2
    exit 1
fi

global_links=(
    docs/v1/design/nereus-futures.md
    docs/v1/design/nereus-future1-core-stream-storage.md
    docs/v1/design/nereus-future4-compaction-generation.md
    docs/v1/design/nereus-commit-protocol.md
    docs/v1/design/nereus-terminology.md
    docs/v1/phase-1.5-core-storage-foundation/README.md
    docs/v1/phase-4-compaction-generation/README.md
)
for path in "${global_links[@]}"; do
    require_literal "phase-bk-bookkeeper-primary-wal" "$path"
done
require_literal 'F1-BK BK-M0–M6 complete/final-gated' \
    "docs/v1/design/nereus-future4-compaction-generation.md"

if rg -Fq -- 'production broker 中仍为 reserved，等待 BK-M5 rollout' \
    "$repo_root/docs/v1/design/nereus-future4-compaction-generation.md"; then
    echo "Future 4 design regressed to the pre-BK-M5 BookKeeper rollout status" >&2
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
