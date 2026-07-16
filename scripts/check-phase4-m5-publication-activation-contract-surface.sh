#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:?usage: check-phase4-m5-publication-activation-contract-surface.sh PULSAR_CHECKOUT}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M5 publication-activation artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M5 publication-activation contract '$literal' in $path" >&2
        exit 1
    fi
}

managed="$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger"
managed_test="$repo_root/nereus-managed-ledger/src/test/java/com/nereusstream/managedledger"
adapter="$repo_root/nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar"
pulsar_main="$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus"
pulsar_test="$pulsar_checkout/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus"

coordinator_contract="$managed/generation/ManagedLedgerGenerationProtocolActivationCoordinator.java"
coordinator="$managed/generation/DefaultManagedLedgerGenerationProtocolActivationCoordinator.java"
coordinator_test="$managed_test/generation/ManagedLedgerGenerationProtocolActivationCoordinatorTest.java"
factory="$managed/NereusManagedLedgerFactory.java"
runtime="$managed/NereusManagedLedgerRuntime.java"
runtime_test="$managed_test/NereusManagedLedgerRuntimeTest.java"
provider="$adapter/DefaultNereusRuntimeProvider.java"
broker_storage="$pulsar_main/NereusManagedLedgerStorage.java"
broker_test="$pulsar_test/NereusManagedLedgerStorageGenerationActivationTest.java"

for path in \
    "$coordinator_contract" \
    "$coordinator" \
    "$coordinator_test" \
    "$factory" \
    "$runtime" \
    "$runtime_test" \
    "$provider" \
    "$broker_storage" \
    "$broker_test"; do
    require_file "$path"
done

require_literal "interface ManagedLedgerGenerationProtocolActivationCoordinator" "$coordinator_contract"
require_literal "activatePublication()" "$coordinator_contract"
require_literal "MAX_CAS_ATTEMPTS = 32" "$coordinator"
require_literal "first generation publication activation is disabled" "$coordinator"
require_literal "generation registration backfill is incomplete for the current readiness epoch" "$coordinator"
require_literal "GenerationProtocolActivationLifecycle.ACTIVE" "$coordinator"
require_literal "generation publication activation CAS retry budget exhausted" "$coordinator"
require_literal "generation readiness was invalidated after publication activation" "$coordinator"
require_literal "durable generation reference-domain set differs from the local runtime" "$coordinator"

require_literal "disabledFirstActivationDoesNotCreateRolloutAuthority" "$coordinator_test"
require_literal "requiresCurrentCompletedRegistrationProofBeforeActiveCas" "$coordinator_test"
require_literal "activatesPublicationOnlyAndAnExistingActiveRecordIgnoresTheSwitch" "$coordinator_test"
require_literal "lostCasResponseConvergesFromTheDurableActiveRecord" "$coordinator_test"
require_literal "conditionConflictRetriesAgainstTheReloadedPreparedRecord" "$coordinator_test"
require_literal "readinessInvalidationAfterCasFailsCallerButLeavesSafeActiveRecord" "$coordinator_test"
require_literal "readinessOrDomainDriftCannotActivateThePreparedRecord" "$coordinator_test"

require_literal "activateGenerationPublication()" "$factory"
require_literal "generationProtocolActivationCoordinator()" "$runtime"
require_literal "generationProtocolActivationCoordinator" "$runtime_test"
require_literal "DefaultManagedLedgerGenerationProtocolActivationCoordinator" "$provider"

require_literal "activateAfterSuccessfulBackfill" "$broker_storage"
require_literal "generationProtocolEnabled" "$broker_storage"
require_literal "this::activateGenerationPublication" "$broker_storage"
require_literal "disabledOrFailedBackfillNeverInvokesClusterActivation" "$broker_test"
require_literal "successfulEnabledBackfillWaitsForClusterActivation" "$broker_test"
require_literal "activationFailureFailsTheBackfillCompletionPromise" "$broker_test"

require_literal "phase4M5PublicationActivationCheck" "$repo_root/build.gradle.kts"
require_literal "Checkpoint AC" "$repo_root/docs/phase-4-compaction-generation/README.md"

if rg -Fq -- "GenerationProtocolActivationStore" "$broker_storage"; then
    echo "broker storage must invoke the product coordinator and never own activation metadata" >&2
    exit 1
fi

echo "Phase 4 M5 proof-gated publication activation, response-loss convergence, and broker sequencing verified."
