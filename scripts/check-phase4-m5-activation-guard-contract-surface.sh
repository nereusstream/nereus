#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:?usage: check-phase4-m5-activation-guard-contract-surface.sh PULSAR_CHECKOUT}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M5 activation-guard artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M5 activation-guard contract '$literal' in $path" >&2
        exit 1
    fi
}

managed="$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger"
managed_test="$repo_root/nereus-managed-ledger/src/test/java/com/nereusstream/managedledger"
adapter="$repo_root/nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar"
adapter_test="$repo_root/nereus-pulsar-adapter/src/test/java/com/nereusstream/pulsar"
pulsar_common="$pulsar_checkout/pulsar-broker-common/src/main/java/org/apache/pulsar/broker"
pulsar_main="$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus"
pulsar_test="$pulsar_checkout/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus"

guard="$managed/generation/ManagedLedgerGenerationProtocolActivationGuard.java"
guard_test="$managed_test/generation/ManagedLedgerGenerationProtocolActivationGuardTest.java"
runtime="$managed/NereusManagedLedgerRuntime.java"
runtime_test="$managed_test/NereusManagedLedgerRuntimeTest.java"
context="$adapter/NereusRuntimeContext.java"
provider="$adapter/DefaultNereusRuntimeProvider.java"
context_test="$adapter_test/DefaultNereusRuntimeProviderCursorTest.java"
service_configuration="$pulsar_common/ServiceConfiguration.java"
broker_configuration="$pulsar_main/NereusBrokerStorageConfiguration.java"
broker_storage="$pulsar_main/NereusManagedLedgerStorage.java"
broker_configuration_test="$pulsar_test/NereusBrokerStorageConfigurationTest.java"

for path in \
    "$guard" \
    "$guard_test" \
    "$runtime" \
    "$runtime_test" \
    "$context" \
    "$provider" \
    "$context_test" \
    "$service_configuration" \
    "$broker_configuration" \
    "$broker_storage" \
    "$broker_configuration_test"; do
    require_file "$path"
done

require_literal "implements GenerationProtocolActivationGuard" "$guard"
require_literal "first generation activation is disabled" "$guard"
require_literal "generation cluster activation is absent" "$guard"
require_literal "streamRegistrationBackfill().complete()" "$guard"
require_literal "durable generation reference-domain set differs from the local runtime" "$guard"
require_literal "activateGenerationProtocol(" "$guard"
require_literal "generation marker is absent after activation" "$guard"
require_literal "generation physical deletion is not active for the current readiness epoch" "$guard"
require_literal "projection-generation-v1" "$guard"
require_literal "currentGenerationCapabilityReadiness()" "$guard"
require_literal "referenceDomainSetSha256" "$guard"

require_literal "firstActivationRequiresExactClusterRegistrationAndMarksTheTopic" "$guard_test"
require_literal "markerWriteResponseLossConvergesByExactProjectionReload" "$guard_test"
require_literal "disabledFirstActivationLeavesMarkerAbsentButCannotDisableAnExistingMarker" "$guard_test"
require_literal "incompleteCoverageOrMissingRegistrationCannotCreateAMarker" "$guard_test"
require_literal "projectionOrBrokerReadinessDriftInvalidatesTheEphemeralProof" "$guard_test"
require_literal "physicalDeleteRequiresDeleteBitsAndTheExactProjectionDomainSnapshot" "$guard_test"
require_literal "5b29cf6df71cce198d01299f5bd740f0f123c601e12f04d8251d336a6a2a8c4d" "$guard_test"

require_literal "generationProtocolActivationGuard()" "$runtime"
require_literal "generationProtocolActivationGuard" "$runtime_test"
require_literal "generationProtocolActivationEnabled" "$context"
require_literal "ManagedLedgerGenerationProtocolActivationGuard" "$provider"
require_literal "NereusGenerationProtocolReferenceDomains" "$provider"
require_literal ".currentV1()" "$provider"
require_literal "canonicalContextCarriesTheExplicitGenerationActivationSwitch" "$context_test"

require_literal "nereusGenerationProtocolEnabled = false" "$service_configuration"
require_literal "generationProtocolEnabled()" "$broker_configuration"
require_literal "checked.generationProtocolEnabled()" "$broker_storage"
require_literal "setNereusGenerationProtocolEnabled(true)" "$broker_configuration_test"
require_literal "phase4M5ActivationGuardCheck" "$repo_root/build.gradle.kts"
require_literal "Checkpoint AB" "$repo_root/docs/phase-4-compaction-generation/README.md"

if rg -Fq -- "GenerationProtocolActivationStore" "$broker_storage"; then
    echo "broker storage must receive the product guard through runtime composition, not own activation metadata" >&2
    exit 1
fi

echo "Phase 4 M5 product-owned generation activation guard, exact proof revalidation, and disabled-by-default broker switch verified."
