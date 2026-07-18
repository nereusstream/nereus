#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_root="${1:?usage: $0 /path/to/locked/pulsar}"

require_file() {
    local root="$1"
    local path="$2"
    if [[ ! -f "$root/$path" ]]; then
        echo "missing Phase 4 M4 physical-deletion activation artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local root="$1"
    local literal="$2"
    local path="$3"
    if ! rg -Fq -- "$literal" "$root/$path"; then
        echo "missing Phase 4 M4 physical-deletion activation contract '$literal' in $path" >&2
        exit 1
    fi
}

api="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/ManagedLedgerPhysicalDeletionActivationCoordinator.java"
request="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/ManagedLedgerPhysicalDeletionActivationRequest.java"
result="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/ManagedLedgerPhysicalDeletionActivationResult.java"
guard="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/ManagedLedgerGenerationProtocolActivationGuard.java"
runtime_owner="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/NereusManagedLedgerRuntime.java"
factory="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/NereusManagedLedgerFactory.java"
coordinator="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultPhase4PhysicalDeletionActivationCoordinator.java"
startup_gate="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4PhysicalGcStartupGate.java"
physical_runtime="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4PhysicalGcRuntime.java"
provider="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultNereusRuntimeProvider.java"
coordinator_test="nereus-pulsar-adapter/src/test/java/com/nereusstream/pulsar/Phase4PhysicalDeletionActivationCoordinatorTest.java"
startup_test="nereus-pulsar-adapter/src/test/java/com/nereusstream/pulsar/Phase4PhysicalGcStartupGateTest.java"
pulsar_storage="pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusManagedLedgerStorage.java"
pulsar_test="pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusManagedLedgerStorageGenerationActivationTest.java"

for path in \
    "$api" \
    "$request" \
    "$result" \
    "$guard" \
    "$runtime_owner" \
    "$factory" \
    "$coordinator" \
    "$startup_gate" \
    "$physical_runtime" \
    "$provider" \
    "$coordinator_test" \
    "$startup_test"; do
    require_file "$repo_root" "$path"
done
require_file "$pulsar_root" "$pulsar_storage"
require_file "$pulsar_root" "$pulsar_test"

require_literal "$repo_root" "CompletableFuture<ManagedLedgerPhysicalDeletionActivationResult> activate(" "$api"
require_literal "$repo_root" "int maxConcurrentStreams" "$request"
require_literal "$repo_root" "public static final int MAX_CONCURRENT_STREAMS = 1_024;" "$request"
require_literal "$repo_root" "timeout must be positive and millisecond-representable" "$request"
require_literal "$repo_root" "ACTIVATED," "$result"
require_literal "$repo_root" "ALREADY_ACTIVE" "$result"

require_literal "$repo_root" "MAX_CAS_ATTEMPTS = 32" "$coordinator"
require_literal "$repo_root" "backfill.run(backfillRequest)" "$coordinator"
require_literal "$repo_root" "capabilityProbe.probe(probeRequest)" "$coordinator"
require_literal "$repo_root" "activations.compareAndSet(" "$coordinator"
require_literal "$repo_root" "requireCurrentReadiness(basis.readiness())" "$coordinator"
require_literal "$repo_root" "physical-root/cursor-root backfill contains failures" "$coordinator"
require_literal "$repo_root" "object-store capability differs within the same broker readiness epoch" "$coordinator"

require_literal "$repo_root" "CompletableFuture<Boolean> destructiveLifecycleAuthorized()" "$startup_gate"
require_literal "$repo_root" "activation.requiredReferenceDomains().equals(requiredDomains)" "$startup_gate"
require_literal "$repo_root" "activation.objectStoreCapabilitySha256()" "$startup_gate"
require_literal "$repo_root" ".equals(expectedCapabilitySha256)" "$startup_gate"
require_literal "$repo_root" "another configured object-store scope" "$startup_gate"

require_literal "$repo_root" "implements ManagedLedgerPhysicalDeletionActivationCoordinator, AutoCloseable" "$physical_runtime"
require_literal "$repo_root" "new DefaultPhysicalRootBackfillCoordinator(" "$physical_runtime"
require_literal "$repo_root" "new DefaultPhase4PhysicalDeletionActivationCoordinator(" "$physical_runtime"
require_literal "$repo_root" "startMutatingLifecycleIfAuthorized(false).join()" "$physical_runtime"
require_literal "$repo_root" "startMutatingLifecycleIfAuthorized(true)" "$physical_runtime"
require_literal "$repo_root" "exactConfig.mutationsAllowed()" "$physical_runtime"

require_literal "$repo_root" "new DefaultObjectStoreDeleteCapabilityProbe(" "$provider"
require_literal "$repo_root" ".expectedCapabilitySha256()" "$provider"
require_literal "$repo_root" "objectStoreDeleteCapabilityProbe," "$provider"
require_literal "$repo_root" "physicalDeletionActivationCoordinator()" "$runtime_owner"
require_literal "$repo_root" "activatePhysicalDeletion(" "$factory"
require_literal "$repo_root" ".equals(expectedObjectStoreCapabilitySha256)" "$guard"

require_literal "$pulsar_root" "physicalGcMutationsAllowed" "$pulsar_storage"
require_literal "$pulsar_root" "runtimeConfiguration.physicalGc().mutationsAllowed()" "$pulsar_storage"
require_literal "$pulsar_root" "this::activateGenerationPublication" "$pulsar_storage"
require_literal "$pulsar_root" "new ManagedLedgerPhysicalDeletionActivationRequest(" "$pulsar_storage"
require_literal "$pulsar_root" "publicationActivation.get()" "$pulsar_storage"
require_literal "$pulsar_root" "physicalDeletionActivation.get()" "$pulsar_storage"

require_literal "$repo_root" "backfillsThenProbesThenAtomicallyEnablesBothDeletionBits" "$coordinator_test"
require_literal "$repo_root" "readinessDriftAfterCanaryPreventsActivationCas" "$coordinator_test"
require_literal "$repo_root" "configuredScopeDriftFailsStartupNonRetryably" "$startup_test"
require_literal "$repo_root" "exactDurableScopeAuthorizesRestartRecovery" "$startup_test"
require_literal "$pulsar_root" "physicalDeletionActivationRunsOnlyAfterPublicationAndIsAwaited" "$pulsar_test"
require_literal "$pulsar_root" "physicalDeletionFailureFailsTheBackfillCompletionPromise" "$pulsar_test"

require_literal "$repo_root" "phase4M4PhysicalDeletionActivationCheck" "build.gradle.kts"
require_literal "$repo_root" "Checkpoint AR" "docs/phase-4-compaction-generation/README.md"
require_literal "$repo_root" "phase4M4PhysicalDeletionActivationCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 physical-deletion activation and restart-scope contract surface: PASS"
