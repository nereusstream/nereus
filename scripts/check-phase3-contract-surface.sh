#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:?usage: check-phase3-contract-surface.sh PULSAR_CHECKOUT}"

require_basename() {
    local root="$1"
    local basename="$2"
    local label="$3"
    if [[ -z "$(rg --files -g "$basename" "$root")" ]]; then
        echo "missing Phase 3 $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 3 contract '$literal' in $path" >&2
        exit 1
    fi
}

nereus_production=(
    CursorMetadataStore.java
    OxiaJavaCursorMetadataStore.java
    CursorMetadataStoreCore.java
    CursorKeyspace.java
    CursorNames.java
    CursorIds.java
    CursorMetadataStoreConfig.java
    ManagedLedgerCursorProtocol.java
    CursorScanPage.java
    CursorScanToken.java
    VersionedCursorState.java
    VersionedCursorRetention.java
    CursorMetadataConditionFailedException.java
    CursorRecordLifecycle.java
    CursorRetentionLifecycle.java
    CursorProtectionKind.java
    CursorStateRecord.java
    CursorSnapshotReferenceRecord.java
    CursorAckRangeRecord.java
    CursorPartialBatchAckRecord.java
    CursorProtectionIntentRecord.java
    CursorRetentionRecord.java
    F3MetadataCodecs.java
    CursorStateRecordCodecV1.java
    CursorRetentionRecordCodecV1.java
    ProjectionCreateRequest.java
    ManagedLedgerProjectionMetadataStore.java
    ProjectionMetadataStoreCore.java
    OxiaJavaManagedLedgerProjectionMetadataStore.java
    TopicProjectionRecord.java
    MetadataRecordCodecFactory.java
    OffsetRange.java
    BatchAckState.java
    CursorAckState.java
    AckNormalizer.java
    AckWords.java
    CursorLifecycle.java
    InitialCursorPosition.java
    CursorPropertyMutation.java
    CursorLedgerIdentity.java
    CursorOwnerSession.java
    CursorIdentity.java
    CursorState.java
    CursorSnapshotCodecV1.java
    CursorSnapshotStore.java
    DefaultCursorSnapshotStore.java
    CursorSnapshotReference.java
    CursorSnapshotWriteRequest.java
    CursorRetentionView.java
    CursorStorageConfig.java
    CursorStorage.java
    DefaultCursorStorage.java
    CursorProtocolActivationGuard.java
    CursorHandle.java
    CursorOpenRequest.java
    CursorAckRequest.java
    CursorResetRequest.java
    CursorMutationResult.java
    CursorMutationOutcome.java
    CursorStateMachine.java
    CursorStatePersistencePlanner.java
    CursorStateHydrator.java
    CursorMutationLane.java
    CursorRetentionCoordinator.java
    DefaultCursorRetentionCoordinator.java
    NereusManagedCursor.java
    NereusManagedCursorStats.java
    NereusManagedLedger.java
    NereusLedgerOpenResult.java
    NereusWritableLedgerOpenResult.java
    NereusManagedLedgerOwnershipGuard.java
    NereusManagedLedgerOpenCoordinator.java
    NereusManagedLedgerFactory.java
    NereusManagedLedgerRuntime.java
    NereusManagedLedgerFactoryConfig.java
    ManagedLedgerModule.java
    ManagedLedgerErrorMapper.java
    NereusRuntimeConfiguration.java
    NereusRuntimeContext.java
    DefaultNereusRuntimeProvider.java
    CursorSnapshotKeys.java
    CursorSnapshotInventory.java
)

for artifact in "${nereus_production[@]}"; do
    require_basename "$repo_root" "$artifact" "production"
done

nereus_tests=(
    CursorKeyspaceTest.java
    CursorStateRecordCodecTest.java
    CursorRetentionRecordCodecTest.java
    F3MetadataCodecsCompatibilityTest.java
    CursorMetadataStoreContractTest.java
    OxiaJavaCursorMetadataStoreIntegrationTest.java
    CursorSnapshotCodecV1Test.java
    CursorSnapshotStoreTest.java
    CursorStateMachineTest.java
    CursorStateMachinePropertyTest.java
    CursorStorageOpenTest.java
    CursorStorageAckTest.java
    CursorStorageResetDeleteTest.java
    CursorStorageSnapshotSpillTest.java
    CursorStorageConcurrencyTest.java
    CursorStoragePropertyTest.java
    CursorMutationLaneTest.java
    CursorRetentionCoordinatorTest.java
    CursorRetentionFailureInjectionTest.java
    CursorStorageOxiaS3IntegrationTest.java
    NereusManagedCursorDurableTest.java
    NereusManagedCursorBatchAckTest.java
    NereusManagedCursorReadWaitReplayTest.java
    NereusManagedCursorResetSeekTest.java
    NereusManagedCursorPropertiesTest.java
    NereusManagedCursorCallbackSafetyTest.java
    NereusManagedLedgerCursorHydrationTest.java
    NereusManagedLedgerCursorLifecycleTest.java
    NereusManagedLedgerFactoryCursorOpenTest.java
    ManagedCursorPublicSurfaceClassificationTest.java
    NereusRuntimeConfigurationCursorTest.java
    DefaultNereusRuntimeProviderCursorTest.java
    CursorStorageScaleTest.java
    CursorStorageLimitExhaustionTest.java
    CursorSnapshotInventoryTest.java
    ManagedLedgerCursorProtocolTest.java
    CallbackPrimitivesTest.java
)

for artifact in "${nereus_tests[@]}"; do
    require_basename "$repo_root" "$artifact" "test"
done

pulsar_production=(
    pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusTopicFeatureValidator.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusAcknowledgeValidator.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusAdminOperation.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusBrokerCapabilityCoordinator.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusCursorProtocolCapability.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusStorageBindingCapability.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusBrokerStorageConfiguration.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusManagedLedgerStorage.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/extensions/BrokerRegistryImpl.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/service/persistent/PersistentTopic.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/service/persistent/PersistentSubscription.java
    pulsar-broker/src/main/java/org/apache/pulsar/broker/service/Consumer.java
)

for path in "${pulsar_production[@]}"; do
    if [[ ! -f "$pulsar_checkout/$path" ]]; then
        echo "missing Phase 3 Pulsar production artifact: $path" >&2
        exit 1
    fi
done

pulsar_tests=(
    NereusTopicFeatureValidatorTest.java
    NereusAcknowledgeValidatorTest.java
    NereusCursorProtocolCapabilityTest.java
    NereusBrokerStorageConfigurationCursorTest.java
    NereusPersistentTopicCursorRecoveryTest.java
    NereusPersistentSubscriptionAckTest.java
    NereusConsumerAckOrderingTest.java
    NereusAdminOperationTest.java
    NereusCursorMultiBrokerIntegrationTest.java
)

for artifact in "${pulsar_tests[@]}"; do
    require_basename "$pulsar_checkout/pulsar-broker/src/test" "$artifact" "Pulsar test"
done

cursor_test="$pulsar_checkout/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusCursorMultiBrokerIntegrationTest.java"
require_literal "preservesDurableCursorTruthAcrossUnloadFailoverRestartExpiryAndBookKeeper" "$cursor_test"
require_literal "preservesMessageIdsPropertiesAndIncarnationAcrossCompatibilityCuts" "$cursor_test"
require_literal "claimAndLoadActiveCursors(owner)" \
    "$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/NereusManagedLedgerOpenCoordinator.java"
require_literal "writable open final publication" \
    "$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/NereusManagedLedgerOpenCoordinator.java"
require_literal "Math.addExact(current.ackStateEpoch(), 1)" \
    "$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/CursorStateMachine.java"
require_literal "deletionVetoed()" \
    "$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/CursorSnapshotInventory.java"
require_literal "stillMatches(" \
    "$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/CursorSnapshotInventory.java"

# Normal dispatch position is deliberately local-only. No durable cursor root,
# retention root, or snapshot wire codec may acquire a read/dispatch field.
durable_cursor_files=(
    "$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/records/CursorStateRecord.java"
    "$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/records/CursorRetentionRecord.java"
    "$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/codec/CursorStateRecordCodecV1.java"
    "$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/codec/CursorRetentionRecordCodecV1.java"
    "$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/CursorSnapshotCodecV1.java"
)
if rg -n '\b(readPosition|dispatchPosition|lastReadPosition|nextReadPosition)\b' "${durable_cursor_files[@]}"; then
    echo "durable Phase 3 cursor state must not persist normal read/dispatch position" >&2
    exit 1
fi

echo "Phase 3 production/test inventory and code-level completion surface verified."
