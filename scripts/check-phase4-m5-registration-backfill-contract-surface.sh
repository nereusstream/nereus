#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:?usage: check-phase4-m5-registration-backfill-contract-surface.sh PULSAR_CHECKOUT}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M5 registration-backfill artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M5 registration-backfill contract '$literal' in $path" >&2
        exit 1
    fi
}

managed_ledger="$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger"
managed_ledger_test="$repo_root/nereus-managed-ledger/src/test/java/com/nereusstream/managedledger"
pulsar_main="$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker"
pulsar_test="$pulsar_checkout/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus"

candidate="$managed_ledger/generation/ManagedLedgerMaterializationRegistrationCandidate.java"
open_coordinator="$managed_ledger/NereusManagedLedgerOpenCoordinator.java"
factory="$managed_ledger/NereusManagedLedgerFactory.java"
open_test="$managed_ledger_test/NereusManagedLedgerOpenCoordinatorTest.java"
projection_test="$managed_ledger_test/generation/ManagedLedgerGenerationProjectionRefV1Test.java"
backfill="$pulsar_main/storage/nereus/DefaultNereusGenerationRegistrationBackfill.java"
request="$pulsar_main/storage/nereus/GenerationRegistrationBackfillRequest.java"
report="$pulsar_main/storage/nereus/GenerationRegistrationBackfillReport.java"
failure="$pulsar_main/storage/nereus/BackfillFailure.java"
stage="$pulsar_main/storage/nereus/GenerationRegistrationBackfillStage.java"
storage="$pulsar_main/storage/nereus/NereusManagedLedgerStorage.java"
registry="$pulsar_main/loadbalance/extensions/BrokerRegistryImpl.java"
configuration="$pulsar_checkout/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/ServiceConfiguration.java"
backfill_test="$pulsar_test/NereusGenerationRegistrationBackfillTest.java"
configuration_test="$pulsar_test/NereusBrokerStorageConfigurationTest.java"

for path in \
    "$candidate" \
    "$open_coordinator" \
    "$factory" \
    "$open_test" \
    "$projection_test" \
    "$backfill" \
    "$request" \
    "$report" \
    "$failure" \
    "$stage" \
    "$storage" \
    "$registry" \
    "$configuration" \
    "$backfill_test" \
    "$configuration_test"; do
    require_file "$path"
done

require_literal "record ManagedLedgerMaterializationRegistrationCandidate" "$candidate"
require_literal "projection identity binding generation mismatch" "$candidate"
require_literal "projectionIdentitySha256 does not match the exact NPR1 identity" "$candidate"
require_literal "inspectMaterializationRegistrationCandidate(" "$open_coordinator"
require_literal "expectedStorageClassBindingGeneration" "$open_coordinator"
require_literal "ManagedLedgerFacadeState.OPEN" "$open_coordinator"
require_literal "ManagedLedgerFacadeState.SEALED" "$open_coordinator"
require_literal "ensureMaterializationRegistration(" "$factory"
require_literal "unloadedBackfillCapturesExactLiveProjectionBeforeRegistration" "$open_test"
require_literal "backfillCandidateFreezesTheSameExactNpr1Identity" "$projection_test"

require_literal "TenantResources.listTenantsAsync" \
    "$repo_root/docs/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal "namespaceResources.listNamespacesAsync" "$backfill"
require_literal "topicResources.listPersistentTopicsAsync" "$backfill"
require_literal "SystemTopicNames.isSystemTopic" "$backfill"
require_literal "processTopicBatches(" "$backfill"
require_literal "request.maxConcurrency()" "$backfill"
require_literal "first.equals(last)" "$backfill"
require_literal "nereus-generation-registration-backfill-v1" "$backfill"
require_literal "nereus-generation-backfill-resource-v1" "$backfill"
require_literal "MAX_FAILURES = 100" "$report"
require_literal "persistentTopicsScanned must equal registered + skipped + topic failures" "$report"
require_literal "topic failures cannot exceed total failures" "$report"
require_literal "runId must be lowercase base32 without padding" "$request"
require_literal "BINDING_READ" "$stage"
require_literal "PROJECTION_READ" "$stage"
require_literal "REGISTRATION_WRITE" "$stage"
require_literal "resourceIdentitySha256 must be lowercase SHA-256" "$failure"
require_literal "attachGenerationRegistrationBackfill(" "$storage"
require_literal "runGenerationRegistrationBackfill(" "$storage"
require_literal "pulsar.getPulsarResources().getTenantResources()" "$registry"
require_literal "nereusGenerationRegistrationBackfillConcurrency = 16" "$configuration"
require_literal "nereusGenerationRegistrationBackfillTimeoutSeconds = 3600" "$configuration"

require_literal "canonicalTraversalIsOrderIndependentAndRegistersOnlyLiveNereusTopics" "$backfill_test"
require_literal "topicRegistrationConcurrencyNeverExceedsTheRequestBound" "$backfill_test"
require_literal "retainsOnlyFirstHundredCanonicalFailuresWhileHashingAllTopics" "$backfill_test"
require_literal "finalBindingDriftIsAReportedFailureRatherThanFalseCoverage" "$backfill_test"
require_literal "readinessMustRemainExactlyStableAcrossTheWholeTraversal" "$backfill_test"
require_literal "2f234d6b9baa3a760460090850d22734f94cd72d51fd0f27706fda272fc01d7c" \
    "$backfill_test"
require_literal "generationRegistrationBackfillConcurrency" "$configuration_test"
require_literal "phase4M5RegistrationBackfillCheck" "$repo_root/build.gradle.kts"
require_literal "phase4M5RegistrationBackfillCheck" \
    "$repo_root/docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "Checkpoint Z" "$repo_root/docs/phase-4-compaction-generation/README.md"

if rg -Fq -- "compareAndSetGenerationProtocolActivation" "$backfill"; then
    echo "registration-backfill traversal must not write the durable activation proof" >&2
    exit 1
fi

echo "Phase 4 M5 exact unloaded-topic registration candidate, canonical traversal, and bounded report verified."
