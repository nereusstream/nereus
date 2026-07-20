#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_root="${1:?usage: check-bookkeeper-primary-wal-m5-admin-routing-contract-surface.sh PULSAR_CHECKOUT}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing BookKeeper M5 admin-routing artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing BookKeeper M5 admin-routing contract '$literal' in $path" >&2
        exit 1
    fi
}

broker_main="$pulsar_root/pulsar-broker/src/main/java/org/apache/pulsar/broker"
broker_test="$pulsar_root/pulsar-broker/src/test/java/org/apache/pulsar/broker"
models="$broker_main/admin/impl/NereusBookKeeperPrimaryWalAdminModels.java"
brokers="$broker_main/admin/impl/BrokersBase.java"
topics="$broker_main/admin/impl/PersistentTopicsBase.java"
storage="$broker_main/storage/nereus/NereusManagedLedgerStorage.java"
operation="$broker_main/storage/nereus/NereusAdminOperation.java"
admin_test="$broker_test/admin/impl/NereusBookKeeperPrimaryWalAdminTest.java"
routing_test="$broker_test/admin/impl/PersistentTopicsNereusDurableProfileRoutingTest.java"
operation_test="$broker_test/storage/nereus/NereusAdminOperationTest.java"

for path in \
    "$models" \
    "$brokers" \
    "$topics" \
    "$storage" \
    "$operation" \
    "$admin_test" \
    "$routing_test" \
    "$operation_test"; do
    require_file "$path"
done

require_literal 'record PublicationActivationRequest(' "$models"
require_literal 'record DeletionActivationRequest(' "$models"
require_literal 'MAX_TIMEOUT_SECONDS = 86_400' "$models"
require_literal 'exposesNoCallerControlledDeletionProofFields' "$admin_test"
require_literal 'rejectsBeforeLookingUpStorageWhenSuperUserValidationFails' "$admin_test"
require_literal 'mapsPreparePublicationDeletionAndReadExactly' "$admin_test"

for route in \
    '/bookkeeper-primary-wal/namespace' \
    '/bookkeeper-primary-wal/namespace/revoke' \
    '/bookkeeper-primary-wal/activation/prepare' \
    '/bookkeeper-primary-wal/activation/publications' \
    '/bookkeeper-primary-wal/activation/deletion' \
    '/bookkeeper-primary-wal/activation'; do
    require_literal "$route" "$brokers"
done
require_literal 'validateSuperUserAccessAsync()' "$brokers"
require_literal 'BookKeeperProtocolActivationUpdate.publications(' "$brokers"
require_literal 'new BookKeeperDeletionActivationRequest(' "$brokers"

require_literal 'public CompletableFuture<Void> validateBoundAdminOperation(' "$storage"
require_literal 'nereusFactory.inspectStorageState(persistenceName)' "$storage"
require_literal 'projection.storageClassBindingGeneration()' "$storage"
require_literal 'exactBinding.bindingGeneration()' "$storage"
require_literal '.profile()' "$storage"
require_literal 'capabilityCoordinator::requireStorageProfileReady' "$storage"
require_literal 'public boolean requiresStorageProfileReadiness()' "$operation"

require_literal 'nereusStorage.validateBoundAdminOperation(' "$topics"
require_literal 'loadedNereusTopicUsesTheSameDurableStorageRoute' "$routing_test"
require_literal 'unloadedNereusBindingUsesTheDurableStorageRoute' "$routing_test"
require_literal 'partitionedParentValidatesEveryConcretePartition' "$routing_test"
require_literal 'durableProfileReadinessIsAppliedOnlyToStorageDependentOperations' "$operation_test"
require_literal 'durableProfileComesFromExactBindingGenerationAndL0Metadata' "$operation_test"

echo "BookKeeper M5 authenticated admin and loaded/unloaded/partitioned durable-profile routing verified."
