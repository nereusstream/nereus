#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 lifecycle contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 lifecycle contract '$literal' in $path" >&2
        exit 1
    fi
}

require_ordered() {
    local path="$1"
    shift
    local previous=0
    local literal
    for literal in "$@"; do
        local line
        line="$(rg -n -F -- "$literal" "$repo_root/$path" | head -n 1 | cut -d: -f1)"
        if [[ -z "$line" || "$line" -le "$previous" ]]; then
            echo "Phase 4 M4 lifecycle ordering is missing or invalid at '$literal' in $path" >&2
            exit 1
        fi
        previous="$line"
    done
}

scanner="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/StreamRegistrationRetirementScanner.java"
scan_result="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/StreamRegistrationRetirementScanResult.java"
pass="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalGcLifecyclePass.java"
service="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/DefaultPhysicalGcLifecycleService.java"
ownerless="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/OwnerlessObjectGcExecutor.java"
router="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4PhysicalRootLifecycleRouter.java"
runtime="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4PhysicalGcRuntime.java"
provider="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultNereusRuntimeProvider.java"
scanner_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/StreamRegistrationRetirementScannerTest.java"
pass_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalGcLifecyclePassTest.java"
service_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/DefaultPhysicalGcLifecycleServiceTest.java"
ownerless_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/OwnerlessObjectGcExecutorTest.java"
router_test="nereus-pulsar-adapter/src/test/java/com/nereusstream/pulsar/Phase4PhysicalRootLifecycleRouterTest.java"

for path in \
    "$scanner" \
    "$scan_result" \
    "$pass" \
    "$service" \
    "$ownerless" \
    "$router" \
    "$runtime" \
    "$provider" \
    "$scanner_test" \
    "$pass_test" \
    "$service_test" \
    "$ownerless_test" \
    "$router_test"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 lifecycle artifact: $path" >&2
        exit 1
    fi
done

require_literal "F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS" "$scanner"
require_literal "config.metadataScanPageSize()" "$scanner"
require_literal "a registration-retirement scan is already running" "$scanner"
require_literal "registration scan returned a key/value outside its exact shard" "$scanner"
require_literal "statusCounts must contain every status" "$scan_result"

require_ordered \
    "$pass" \
    "return rootPass.thenCompose" \
    'require(registrations, "registration-retirement pass")' \
    'require(inventory, "object-inventory pass")'

require_literal "rescanRequested = true" "$service"
require_literal "config.scanInterval()" "$service"
require_literal "config.closeTimeout()" "$service"
require_literal "source.cancel(true)" "$service"
require_literal "target.cancel(true)" "$service"
reject_literal "scheduleAtFixedRate" "$service"
reject_literal "scheduleWithFixedDelay" "$service"
reject_literal ".shutdown(" "$service"
reject_literal ".shutdownNow(" "$service"

require_literal 'abandonedAppendIntents.inspectActive(active)' "$ownerless"
require_literal 'inspection.protectionRemovals()' "$ownerless"
require_literal 'inspection.metadataRemovals()' "$ownerless"
require_literal 'abandonedAppendIntents.inspectMarked(marked)' "$ownerless"
require_literal 'referenceDomains.snapshotForDeletion(' "$ownerless"
require_literal 'abandonedAppendIntents.reload(' "$ownerless"
reject_literal 'scanProtections(' "$ownerless"
require_literal "OWNERLESS_ORPHAN_CANDIDATE" "$ownerless"
require_literal "PLAN_DRIFT_UNMARKED" "$ownerless_test"
require_literal "anyDurableProtectionSkipsTheExpensiveOwnerlessGlobalProof" "$ownerless_test"

require_literal "case ACTIVE -> visitActive(exact)" "$router"
require_literal "case MARKED -> visitMarked(exact)" "$router"
require_literal "case DELETING ->" "$router"
require_literal "case DELETED ->" "$router"
require_literal "case QUARANTINED ->" "$router"
require_literal "activeCursorStreams.add(stream)" "$router"
reject_literal "listObjects(" "$router"

require_literal "new GenerationZeroIndexRetirementHandler(" "$runtime"
require_literal "new GenerationZeroCommitRetirementHandler(" "$runtime"
require_literal "new GenerationZeroMarkerRetirementHandler(" "$runtime"
require_literal "new HigherGenerationIndexRetirementHandler(" "$runtime"
require_literal "new DefaultPhysicalRootTombstoneRetirementCoordinator(" "$runtime"
require_literal "new StreamRegistrationRetirementCoordinator(" "$runtime"
require_literal "new PhysicalObjectRootScanner(" "$runtime"
require_literal "new StreamRegistrationRetirementScanner(" "$runtime"
require_literal "new DefaultPhysicalGcLifecycleService(" "$runtime"
require_literal "if (!config.enabled())" "$runtime"
require_literal "if (config.dryRun())" "$runtime"
require_literal "startMutatingLifecycleIfAuthorized(false).join()" "$runtime"

require_literal "OxiaJavaSourceRetirementMetadataStore.usingSharedRuntime(" "$provider"
require_literal "OxiaJavaObjectAuditRetirementStore.usingSharedRuntime(" "$provider"
require_literal "physicalGcRuntime.start();" "$provider"

require_literal "scansEveryShardAndPageAndCountsEveryCoordinatorOutcome" "$scanner_test"
require_literal "runsMetadataRootsThenRegistrationRetirementThenListing" "$pass_test"
require_literal "coalescesOneImmediatePassAndUsesFixedDelayAfterCompletion" "$service_test"
require_literal "closeDeadlineCancelsHungPassAndRejectsNewWork" "$service_test"
require_literal "restartReconstructsMarkedOwnerlessPlanAndDeletesExactBytes" "$ownerless_test"
require_literal "routesEveryLifecycleAndDeduplicatesActiveCursorInventoryByStream" "$router_test"

require_literal "phase4M4LifecycleSchedulingCheck" "build.gradle.kts"
require_literal "Checkpoint AN" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4LifecycleSchedulingCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 metadata-first lifecycle scheduling, routing, and recovery surfaces verified."
