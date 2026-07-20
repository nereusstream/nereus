#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_root="${1:?usage: check-phase4-m5-retention-policy-admin-contract-surface.sh PULSAR_CHECKOUT}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M5 retention policy/admin artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M5 retention policy/admin contract '$literal' in $path" >&2
        exit 1
    fi
}

managed_main="$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger"
managed_test="$repo_root/nereus-managed-ledger/src/test/java/com/nereusstream/managedledger"
pulsar_main="$pulsar_root/pulsar-broker/src/main/java/org/apache/pulsar/broker"
pulsar_test="$pulsar_root/pulsar-broker/src/test/java/org/apache/pulsar/broker"

retention_service="$managed_main/retention/NereusManagedLedgerRetentionService.java"
ledger="$managed_main/NereusManagedLedger.java"
retention_test="$managed_test/retention/NereusManagedLedgerRetentionTest.java"
features="$pulsar_main/storage/nereus/NereusResolvedTopicFeatures.java"
resolver="$pulsar_main/storage/nereus/NereusTopicFeatureResolver.java"
validator="$pulsar_main/storage/nereus/NereusTopicFeatureValidator.java"
open_context="$pulsar_main/storage/nereus/NereusTopicOpenContext.java"
policy_snapshot="$pulsar_main/storage/nereus/NereusTopicPolicySnapshot.java"
storage="$pulsar_main/storage/nereus/NereusManagedLedgerStorage.java"
broker_service="$pulsar_main/service/BrokerService.java"
persistent_topic="$pulsar_main/service/persistent/PersistentTopic.java"
admin_route="$pulsar_main/admin/impl/PersistentTopicsBase.java"
resolver_test="$pulsar_test/storage/nereus/NereusTopicFeatureResolverTest.java"
validator_test="$pulsar_test/storage/nereus/NereusTopicFeatureValidatorTest.java"
admin_test="$pulsar_test/storage/nereus/NereusAdminOperationTest.java"
topic_test="$pulsar_test/service/persistent/PersistentTopicNereusAdmissionTest.java"

for path in \
    "$retention_service" \
    "$ledger" \
    "$retention_test" \
    "$features" \
    "$resolver" \
    "$validator" \
    "$open_context" \
    "$policy_snapshot" \
    "$storage" \
    "$broker_service" \
    "$persistent_topic" \
    "$admin_route" \
    "$resolver_test" \
    "$validator_test" \
    "$admin_test" \
    "$topic_test"; do
    require_file "$path"
done

require_literal "public CompletableFuture<Void> ensurePolicyAdmissionReady()" "$retention_service"
require_literal "activationGuard.requireReady(" "$retention_service"
require_literal "activationGuard::revalidate" "$retention_service"
require_literal "public CompletableFuture<Void> ensureGenerationProtocolReadyForPolicy()" "$ledger"
require_literal "retentionService.ensurePolicyAdmissionReady()" "$ledger"
require_literal "policyAdmissionActivatesAndRevalidatesWithoutPlanningOrCursorMutation" "$retention_test"

require_literal "Optional<RetentionPolicies> retention" "$features"
require_literal "Map<BacklogQuotaType, BacklogQuota> backlogQuotas" "$features"
require_literal "boolean preciseTimeBasedBacklogQuotaCheck" "$features"
require_literal "boolean generationProtocolRuntimeReady" "$features"
require_literal "RetentionPolicySnapshot.fromCanonicalMinutesAndMebibytes(" "$features"
require_literal "ImmutableBacklogQuota.copyOf(quota)" "$features"
require_literal "public boolean requiresGenerationProtocolRuntime()" "$features"

require_literal "resolveBacklogQuotas(" "$resolver"
require_literal "broker.isPreciseTimeBasedBacklogQuotaCheck()" "$resolver"
require_literal "Optional.ofNullable(retention)" "$resolver"
require_literal "boolean generationProtocolRuntimeReady" "$resolver"

require_literal "RetentionPolicySnapshot retentionPolicy" "$open_context"
require_literal "retentionPolicy must be derived from the exact resolved Pulsar retention values" "$open_context"
require_literal "public boolean hasSamePolicyInputs(" "$policy_snapshot"

require_literal "BACKLOG_TIME_EVICTION_REQUIRES_PRECISE_CHECK" "$validator"
require_literal "GENERATION_PROTOCOL_NOT_READY" "$validator"
require_literal "case TRIM_TOPIC ->" "$validator"
require_literal "validateAdminOperation(operation, features.generationProtocolRuntimeReady())" "$validator"

require_literal "RetentionPolicySnapshot.fromCanonicalMinutesAndMebibytes(" "$broker_service"
require_literal "thenCompose(this::resolveNereusGenerationReadiness)" "$broker_service"
require_literal "nereusStorage.capabilityCoordinator().requireGenerationReadiness()" "$broker_service"

require_literal "installRetentionPolicy(openContext.retentionPolicy())" "$persistent_topic"
require_literal "loadPreparedNereusPolicySnapshot(topicName, 0)" "$persistent_topic"
require_literal "ensureGenerationProtocolReadyForPolicy()" "$persistent_topic"
require_literal "snapshot.hasSamePolicyInputs(reloaded)" "$persistent_topic"
require_literal "validateAdminOperation(operation, nereusFeatures)" "$persistent_topic"

require_literal "capabilityCoordinator::requireGenerationReadiness" "$storage"
require_literal "validateBoundAdminOperation(" "$storage"
require_literal "nereusFactory.inspectStorageState(persistenceName)" "$storage"
require_literal "operation == NereusAdminOperation.TRIM_TOPIC" "$storage"
require_literal "validateNereusAdminOperationForLoadedOrBoundTopic(" "$admin_route"
require_literal "NereusAdminOperation.TRIM_TOPIC" "$admin_route"

require_literal "preservesExactRetentionAndBacklogPrecedenceInImmutableCopies" "$resolver_test"
require_literal "openContextRejectsRetentionProjectionThatDoesNotMatchExactPulsarFacts" "$resolver_test"
require_literal "admitsOnlyGenerationReadySizeAndPreciseTimeEviction" "$validator_test"
require_literal "unloadedTrimWaitsForReadinessOnlyWhenGenerationProtocolIsEnabled" "$admin_test"
require_literal "generationPolicyPreparationWaitsForMarkerAdmissionAndStablePolicyReload" "$topic_test"

require_literal "phase4M5RetentionPolicyAdminCheck" "$repo_root/build.gradle.kts"
require_literal "Checkpoint AI" "$repo_root/docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M5 exact retention policy, generation admission, and loaded/unloaded admin routing verified."
