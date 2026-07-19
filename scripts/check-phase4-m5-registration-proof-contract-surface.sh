#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:?usage: check-phase4-m5-registration-proof-contract-surface.sh PULSAR_CHECKOUT}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M5 registration-proof artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M5 registration-proof contract '$literal' in $path" >&2
        exit 1
    fi
}

core="$repo_root/nereus-core/src/main/java/com/nereusstream/core/capability"
managed="$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger"
managed_test="$repo_root/nereus-managed-ledger/src/test/java/com/nereusstream/managedledger"
adapter="$repo_root/nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar"
pulsar_main="$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus"
pulsar_test="$pulsar_checkout/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus"

readiness="$core/GenerationCapabilityReadiness.java"
readiness_provider="$core/GenerationCapabilityReadinessProvider.java"
completion="$core/GenerationRegistrationBackfillCompletion.java"
proof_contract="$managed/generation/ManagedLedgerGenerationRegistrationBackfillProofCoordinator.java"
proof_coordinator="$managed/generation/DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator.java"
factory="$managed/NereusManagedLedgerFactory.java"
runtime="$managed/NereusManagedLedgerRuntime.java"
proof_test="$managed_test/generation/ManagedLedgerGenerationRegistrationBackfillProofCoordinatorTest.java"
runtime_test="$managed_test/NereusManagedLedgerRuntimeTest.java"
runtime_context="$adapter/NereusRuntimeContext.java"
runtime_provider="$adapter/DefaultNereusRuntimeProvider.java"
domains="$adapter/NereusGenerationProtocolReferenceDomains.java"
broker_readiness="$pulsar_main/NereusGenerationCapabilityReadiness.java"
broker_capabilities="$pulsar_main/NereusBrokerCapabilityCoordinator.java"
broker_storage="$pulsar_main/NereusManagedLedgerStorage.java"
broker_backfill="$pulsar_main/DefaultNereusGenerationRegistrationBackfill.java"
broker_backfill_test="$pulsar_test/NereusGenerationRegistrationBackfillTest.java"
broker_capability_test="$pulsar_test/NereusGenerationProtocolCapabilityTest.java"

for path in \
    "$readiness" \
    "$readiness_provider" \
    "$completion" \
    "$proof_contract" \
    "$proof_coordinator" \
    "$factory" \
    "$runtime" \
    "$proof_test" \
    "$runtime_test" \
    "$runtime_context" \
    "$runtime_provider" \
    "$domains" \
    "$broker_readiness" \
    "$broker_capabilities" \
    "$broker_storage" \
    "$broker_backfill" \
    "$broker_backfill_test" \
    "$broker_capability_test"; do
    require_file "$path"
done

require_literal "record GenerationCapabilityReadiness" "$readiness"
require_literal "brokerSetSha256" "$readiness"
require_literal "interface GenerationCapabilityReadinessProvider" "$readiness_provider"
require_literal "currentGenerationCapabilityReadiness()" "$readiness_provider"
require_literal "record GenerationRegistrationBackfillCompletion" "$completion"
require_literal "failureCount must be non-negative" "$completion"
require_literal "interface ManagedLedgerGenerationRegistrationBackfillProofCoordinator" "$proof_contract"
require_literal "MAX_CAS_ATTEMPTS = 32" "$proof_coordinator"
require_literal "registration backfill contains failures" "$proof_coordinator"
require_literal "completed registration backfill has another coverage digest" "$proof_coordinator"
require_literal "registration proof cannot advance readiness while deletion is enabled" "$proof_coordinator"
require_literal "generation readiness was invalidated after proof CAS" "$proof_coordinator"
require_literal "completeGenerationRegistrationBackfill(" "$factory"
require_literal "generationRegistrationBackfillProofCoordinator()" "$runtime"
require_literal "generationCapabilityReadinessProvider" "$runtime_context"
require_literal "GenerationProtocolActivationStore.usingSharedRuntime" "$runtime_provider"
require_literal "DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator" "$runtime_provider"
require_literal "append-recovery-v1" "$domains"
require_literal "cursor-snapshot-v1" "$domains"
require_literal "future-catalog-sentinel-v1" "$domains"
require_literal "generation-v1" "$domains"
require_literal "materialization-v1" "$domains"
require_literal "projection-generation-v1" "$domains"

require_literal "installsExactProofAndConvergesAnIdempotentRerun" "$proof_test"
require_literal "completedCoverageIsImmutableWithinOneReadinessEpoch" "$proof_test"
require_literal "changedOpaqueReadinessRefreshesProofAndInvalidatesOtherEpochFacts" "$proof_test"
require_literal "lostCasResponseConvergesFromTheExactDurableCoverage" "$proof_test"
require_literal "readinessInvalidationAfterCasFailsTheCallerButLeavesSafeOldEpochProof" "$proof_test"
require_literal "\"generation-activation\"" "$runtime_test"

require_literal "toCore()" "$broker_readiness"
require_literal "GenerationCapabilityReadinessProvider" "$broker_capabilities"
require_literal "currentGenerationCapabilityReadiness()" "$broker_capabilities"
require_literal "completeGenerationRegistrationBackfill(" "$broker_storage"
require_literal "storage.completeGenerationRegistrationBackfill(" "$broker_backfill"
require_literal "report.failureCount() != 0" "$broker_backfill"
require_literal "first.toCore()" "$broker_backfill"
require_literal "proofs.complete(" "$broker_backfill"
require_literal "assertThat(proofs).hasSize(2)" "$broker_backfill_test"
require_literal "assertThat(proofs).hasValue(0)" "$broker_backfill_test"
require_literal "currentGenerationCapabilityReadiness()" "$broker_capability_test"
require_literal "phase4M5RegistrationProofCheck" "$repo_root/build.gradle.kts"
require_literal "Checkpoint AA" "$repo_root/docs/phase-4-compaction-generation/README.md"

if rg -Fq -- "GenerationProtocolActivationStore" "$broker_backfill"; then
    echo "broker traversal must not own or mutate durable generation activation metadata" >&2
    exit 1
fi

echo "Phase 4 M5 exact readiness handoff and product-owned durable registration proof CAS verified."
