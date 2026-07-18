#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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

require_literal "CompletableFuture<VersionedGenerationProtocolActivation> rollover(" "$rollover_api"
require_literal "int maxConcurrentStreams" "$rollover_api"
require_literal "Duration timeout" "$rollover_api"
require_literal "default CompletableFuture<Void> complete(" "$proof_api"
require_literal "readinessRollover.rollover(" "$proof_coordinator"
require_literal "ManagedLedgerPhysicalDeletionActivationRequest" "$proof_coordinator"
require_literal ".MAX_CONCURRENT_STREAMS" "$proof_coordinator"
require_literal "completeGenerationRegistrationBackfill(" "$factory"

require_literal "default CompletableFuture<PhysicalRootBackfillReport> runRollover(" "$backfill_api"
require_literal "public CompletableFuture<PhysicalRootBackfillReport> runRollover(" "$backfill"
require_literal "revalidateRolloverBasis(" "$backfill"
require_literal "ACTIVATION_AUTHORITY_CHANGED" "$backfill"
require_literal "READINESS_EPOCH_NOT_NEWER" "$backfill"

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

require_literal "deletionActiveEpochDelegatesOneBoundedAtomicRollover" "$proof_test"
require_literal "deletionActiveRolloverScansWithoutPublishingPartialProofs" "$backfill_test"
require_literal "deletionActiveReadinessRolloverReplacesEveryProofInOneCas" "$activation_test"

require_literal "phase4M4ReadinessRolloverCheck" "build.gradle.kts"
require_literal "Checkpoint BC" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4ReadinessRolloverCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 deletion-active readiness rollover contract surface: PASS"
