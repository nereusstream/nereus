#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_root="${1:?usage: $0 <pulsar-checkout>}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M5 final artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M5 final contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$path"; then
        echo "forbidden Phase 4 M5 final contract '$literal' in $path" >&2
        exit 1
    fi
}

metadata_snapshot="$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/StreamMetadataSnapshot.java"
planner="$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/DefaultRetentionCandidatePlanner.java"
ledger="$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/NereusManagedLedger.java"
activation_guard="$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/ManagedLedgerGenerationProtocolActivationGuard.java"
planner_test="$repo_root/nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/retention/RetentionCandidatePlannerTest.java"
ledger_test="$repo_root/nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/NereusManagedLedgerFacadeTest.java"
activation_test="$repo_root/nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/generation/ManagedLedgerGenerationProtocolActivationGuardTest.java"
pulsar_admin="$pulsar_root/pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/impl/PersistentTopicsBase.java"
pulsar_fixture="$pulsar_root/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusMultiBrokerIntegrationTest.java"
pulsar_test="$pulsar_root/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusAsyncRetentionMultiBrokerIntegrationTest.java"

for path in \
    "$metadata_snapshot" \
    "$planner" \
    "$ledger" \
    "$activation_guard" \
    "$planner_test" \
    "$ledger_test" \
    "$activation_test" \
    "$pulsar_admin" \
    "$pulsar_fixture" \
    "$pulsar_test"; do
    require_file "$path"
done

require_literal "public boolean sameVersionedAuthority(" "$metadata_snapshot"
require_literal "public boolean sameSemanticAuthority(" "$metadata_snapshot"
require_literal ".sameVersionedAuthority(head)" "$planner"
require_literal "stableCaptureIgnoresHydratedTrimObservationDrift" "$planner_test"
require_literal "slowest.getMarkDeletedPosition()" "$ledger"
require_literal ".filter(NereusManagedCursor::isDurable)" "$ledger"
reject_literal ".filter(NereusManagedCursor::isActive)" "$ledger"
require_literal "getEstimatedBacklogSize())" "$ledger_test"
require_literal "operation == GenerationOperation.PHYSICAL_DELETE" "$activation_guard"
require_literal "logicalTrimUsesPublicationAuthorityWithoutPhysicalDeletion" "$activation_test"

require_literal "getTopicReferenceOrLoadActiveNereusAsync" "$pulsar_admin"
require_literal "nereusStorage.hasActiveNereusBinding(topicName)" "$pulsar_admin"
require_literal "persistentTopic.isNereusManagedLedger()" "$pulsar_admin"

require_literal "new NereusMultiBrokerIntegrationTest(" "$pulsar_test"
require_literal "StorageProfile.OBJECT_WAL_ASYNC_OBJECT" "$pulsar_test"
require_literal "repairsAsyncHistoryAndLogicallyTrimsEvictedBacklogAcrossOwnershipCuts" "$pulsar_test"
require_literal "activateOrRolloverGeneration(" "$pulsar_test"
require_literal "appendCompressedBatch(" "$pulsar_test"
require_literal "getEstimatedBacklogSize()).isPositive()" "$pulsar_test"
require_literal "handleExceededBacklogQuota(" "$pulsar_test"
require_literal ".topics().unload(topic)" "$pulsar_test"
require_literal ".topics().trimTopic(topic)" "$pulsar_test"
require_literal "assertThat(cluster.logicalObjectKeys()).containsAll(retainedWal)" "$pulsar_test"
require_literal "cluster.stopBroker(stoppedOwner)" "$pulsar_test"
require_literal "cluster.startBroker(stoppedOwner)" "$pulsar_test"
require_literal "bookKeeperTopic" "$pulsar_test"
require_literal "exact.getBatchIndex()" "$pulsar_test"
require_literal "Duration.ofSeconds(120)" "$pulsar_fixture"
reject_literal "Thread.sleep(" "$pulsar_test"

require_literal "phase4M5AsyncRetentionMultiBrokerPulsarCheck" "$repo_root/build.gradle.kts"
require_literal '"-PtestRetryCount=0"' "$repo_root/build.gradle.kts"
require_literal "phase4M5Check" "$repo_root/build.gradle.kts"
require_literal "phase4M5FinalCheck" "$repo_root/build.gradle.kts"
require_literal "F4-M5 is complete/final-gated" \
    "$repo_root/docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "F4-M5 已完成并通过 final gate" \
    "$repo_root/docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M5 async Object-WAL, logical retention, ownership-cut, and coexistence contract surface: PASS"
