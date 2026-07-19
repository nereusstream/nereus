#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:?usage: check-phase4-m5-generation-capability-contract-surface.sh PULSAR_CHECKOUT}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M5 generation-capability artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M5 generation-capability contract '$literal' in $path" >&2
        exit 1
    fi
}

main_dir="$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus"
test_dir="$pulsar_checkout/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus"

capability="$main_dir/NereusGenerationProtocolCapability.java"
readiness="$main_dir/NereusGenerationCapabilityReadiness.java"
coordinator="$main_dir/NereusBrokerCapabilityCoordinator.java"
cursor_capability="$main_dir/NereusCursorProtocolCapability.java"
generation_test="$test_dir/NereusGenerationProtocolCapabilityTest.java"
cursor_test="$test_dir/NereusCursorProtocolCapabilityTest.java"
binding_test="$test_dir/NereusStorageBindingCapabilityTest.java"

for path in \
    "$capability" \
    "$readiness" \
    "$coordinator" \
    "$cursor_capability" \
    "$generation_test" \
    "$cursor_test" \
    "$binding_test"; do
    require_file "$path"
done

require_literal 'PROPERTY = "nereus.generation-protocol"' "$capability"
require_literal 'VERSION = "2"' "$capability"
require_literal 'NereusGenerationProtocolCapability.requireUnreserved' "$cursor_capability"

require_literal "long brokerReadinessEpoch" "$readiness"
require_literal "String brokerSetSha256" "$readiness"
require_literal "int persistentBrokerCount" "$readiness"
require_literal "brokerSetSha256 must be lowercase SHA-256" "$readiness"

require_literal "requireGenerationClusterReady()" "$coordinator"
require_literal "requireGenerationReadiness()" "$coordinator"
require_literal "currentGenerationReadiness()" "$coordinator"
require_literal "NereusGenerationProtocolCapability.PROPERTY" "$coordinator"
require_literal "registry.addListener" "$coordinator"
require_literal "brokerRegistryRevision.incrementAndGet()" "$coordinator"
require_literal "nereus-generation-broker-readiness-v1" "$coordinator"
require_literal "data.startTimestamp()" "$coordinator"
require_literal "NEREUS_CLUSTER_CAPABILITY_SNAPSHOT_CHANGED" "$coordinator"

require_literal "readinessIdentityRejectsNonCanonicalFields" "$generation_test"
require_literal "generationReadinessRequiresAllThreeProtocolVersions" "$generation_test"
require_literal "stableSnapshotProducesOrderIndependentFrozenReadiness" "$generation_test"
require_literal "sameBrokerIdRestartBetweenSnapshotsInvalidatesReadiness" "$generation_test"
require_literal "registryNotificationInvalidatesCachedGenerationEpoch" "$generation_test"
require_literal "registryNotificationBetweenEqualSnapshotsRejectsGenerationEpoch" "$generation_test"
require_literal "36151462167742895L" "$generation_test"
require_literal "80806f90349e89afb16f65d2e90f06339f48babe836f9954ad41fefc2869ab75" \
    "$generation_test"
require_literal "publishesThreeIndependentReservedCapabilities" "$cursor_test"
require_literal "nereus.generation-protocol is reserved by the broker" "$binding_test"

require_literal "phase4M5GenerationCapabilityCheck" "$repo_root/build.gradle.kts"
require_literal "phase4M5GenerationCapabilityCheck" \
    "$repo_root/docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "Checkpoint Y" "$repo_root/docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M5 generation capability, deterministic broker readiness, and invalidation surface verified."
