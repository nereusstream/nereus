#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_root="${1:?usage: $0 <pulsar-checkout>}"

if [[ ! -d "$pulsar_root" ]]; then
    echo "missing locked Pulsar checkout: $pulsar_root" >&2
    exit 1
fi

require_file() {
    local path="$1"
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 readiness-rollover artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 readiness-rollover contract '$literal' in $path" >&2
        exit 1
    fi
}

require_pulsar_file() {
    local path="$1"
    if [[ ! -f "$pulsar_root/$path" ]]; then
        echo "missing locked Pulsar readiness-rollover artifact: $path" >&2
        exit 1
    fi
}

require_pulsar_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$pulsar_root/$path"; then
        echo "missing locked Pulsar readiness-rollover contract '$literal' in $path" >&2
        exit 1
    fi
}

rollover_api="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/ManagedLedgerGenerationReadinessRolloverCoordinator.java"
proof_api="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/ManagedLedgerGenerationRegistrationBackfillProofCoordinator.java"
proof_coordinator="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator.java"
factory="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/NereusManagedLedgerFactory.java"
backfill_api="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalRootBackfillCoordinator.java"
backfill="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/DefaultPhysicalRootBackfillCoordinator.java"
activation="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultPhase4PhysicalDeletionActivationCoordinator.java"
runtime="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4PhysicalGcRuntime.java"
provider="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultNereusRuntimeProvider.java"
proof_test="nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/generation/ManagedLedgerGenerationRegistrationBackfillProofCoordinatorTest.java"
backfill_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalRootBackfillCoordinatorTest.java"
activation_test="nereus-pulsar-adapter/src/test/java/com/nereusstream/pulsar/Phase4PhysicalDeletionActivationCoordinatorTest.java"
pulsar_backfill="pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/DefaultNereusGenerationRegistrationBackfill.java"
pulsar_request="pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/GenerationRegistrationBackfillRequest.java"
pulsar_configuration="pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusBrokerStorageConfiguration.java"
pulsar_storage="pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusManagedLedgerStorage.java"
pulsar_backfill_test="pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusGenerationRegistrationBackfillTest.java"
pulsar_configuration_test="pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusBrokerStorageConfigurationTest.java"

for path in \
    "$rollover_api" \
    "$proof_api" \
    "$proof_coordinator" \
    "$factory" \
    "$backfill_api" \
    "$backfill" \
    "$activation" \
    "$runtime" \
    "$provider" \
    "$proof_test" \
    "$backfill_test" \
    "$activation_test"; do
    require_file "$path"
done

for path in \
    "$pulsar_backfill" \
    "$pulsar_request" \
    "$pulsar_configuration" \
    "$pulsar_storage" \
    "$pulsar_backfill_test" \
    "$pulsar_configuration_test"; do
    require_pulsar_file "$path"
done

require_literal "CompletableFuture<VersionedGenerationProtocolActivation> rollover(" "$rollover_api"
require_literal "int maxConcurrentStreams" "$rollover_api"
require_literal "Duration timeout" "$rollover_api"
require_literal "default CompletableFuture<Void> complete(" "$proof_api"
require_literal "readinessRollover.rollover(" "$proof_coordinator"
require_literal "ManagedLedgerPhysicalDeletionActivationRequest" "$proof_coordinator"
require_literal ".MAX_CONCURRENT_STREAMS" "$proof_coordinator"
require_literal "Deadline.start(requireTimeout(timeout), nanoTime)" "$proof_coordinator"
require_literal "deadline.callWithRemaining(remaining ->" "$proof_coordinator"
require_literal "future.orTimeout(" "$proof_coordinator"
require_literal "completeGenerationRegistrationBackfill(" "$factory"

require_pulsar_literal "storage.completeGenerationRegistrationBackfill(" "$pulsar_backfill"
require_pulsar_literal "exact.maxConcurrency()," "$pulsar_backfill"
require_pulsar_literal "remaining = remaining(deadlineNanos);" "$pulsar_backfill"
require_pulsar_literal "interface ProofAccess" "$pulsar_backfill"
require_pulsar_literal "int maxConcurrentStreams," "$pulsar_backfill"
require_pulsar_literal "Duration timeout);" "$pulsar_backfill"
require_pulsar_literal "MAX_CONCURRENCY = 1_024" "$pulsar_request"
require_pulsar_literal "maxConcurrency > MAX_CONCURRENCY" "$pulsar_request"
require_pulsar_literal "GenerationRegistrationBackfillRequest.MAX_CONCURRENCY" "$pulsar_configuration"
require_pulsar_literal "completion, maxConcurrentStreams, timeout" "$pulsar_storage"
require_pulsar_literal ".isLessThan(request.timeout())" "$pulsar_backfill_test"
require_pulsar_literal ".hasValue(request.maxConcurrency())" "$pulsar_backfill_test"
require_pulsar_literal ".hasMessageContaining(\"at most 1024\")" "$pulsar_configuration_test"

require_literal "default CompletableFuture<PhysicalRootBackfillReport> runRollover(" "$backfill_api"
require_literal "public CompletableFuture<PhysicalRootBackfillReport> runRollover(" "$backfill"
require_literal "revalidateRolloverBasis(" "$backfill"
require_literal "ACTIVATION_AUTHORITY_CHANGED" "$backfill"
require_literal "READINESS_EPOCH_UNCHANGED" "$backfill"

require_literal "ManagedLedgerGenerationReadinessRolloverCoordinator" "$activation"
require_literal "backfill.runRollover(" "$activation"
require_literal "capabilityProbe.probe(probeRequest)" "$activation"
require_literal "rolloverReplacement(" "$activation"
require_literal "activations.compareAndSet(" "$activation"
require_literal "proofs.registration()" "$activation"
require_literal "proofs.physicalRoot()" "$activation"
require_literal "proofs.cursorSnapshot()" "$activation"
require_literal "requireCurrentReadiness(registration.readiness())" "$activation"
require_literal "sameRolloverAuthority(" "$activation"

require_literal "ManagedLedgerGenerationReadinessRolloverCoordinator," "$runtime"
require_literal "return deletionActivationCoordinator" "$runtime"
require_literal ".rollover(" "$runtime"
require_literal "startMutatingLifecycleIfAuthorized(true)" "$runtime"
require_literal "physicalGcRuntime," "$provider"

require_literal "deletionActiveChangedOpaqueEpochDelegatesOneBoundedAtomicRollover" "$proof_test"
require_literal ".isLessThan(deadline)" "$proof_test"
require_literal "deletionActiveRolloverScansWithoutPublishingPartialProofs" "$backfill_test"
require_literal "deletionActiveReadinessRolloverReplacesEveryProofInOneCas" "$activation_test"

require_literal "phase4M4ReadinessRolloverCheck" "build.gradle.kts"
require_literal "phase4M4ReadinessRolloverPulsarCheck" "build.gradle.kts"
require_literal "Checkpoint BC" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4ReadinessRolloverCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 deletion-active readiness rollover contract surface: PASS"
