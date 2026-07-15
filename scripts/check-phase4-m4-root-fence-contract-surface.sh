#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 root-fence $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 root-fence contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    GcReferenceDomainVersion.java
    GcReferenceCollectionStatus.java
    GcReferenceCollection.java
    GcReferenceDomainRegistry.java
    GcPlanMetadataRevalidator.java
    PhysicalGcMarkStatus.java
    PhysicalGcMarkResult.java
    PhysicalGcAdvanceStatus.java
    PhysicalGcAdvanceResult.java
    PhysicalObjectGarbageCollector.java
    PhysicalObjectRootVisitor.java
    PhysicalObjectRootScanResult.java
    PhysicalObjectRootScanner.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

require_basename "GcReferenceDomainRegistryTest.java" "test"
require_basename "PhysicalObjectGarbageCollectorTest.java" "test"
require_basename "PhysicalObjectRootScannerTest.java" "test"

gc="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollector.java"
registry="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/GcReferenceDomainRegistry.java"
scanner="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalObjectRootScanner.java"

require_literal "snapshotForDeletion" "$registry"
require_literal "stillMatches" "$registry"
require_literal "LIMIT_EXCEEDED" "$registry"
require_literal "reference-domain ids must be unique" "$registry"
require_literal "PROJECTION_REFERENCE_DOMAIN" "$gc"
require_literal "config.deadline(now, config.drainGrace())" "$gc"
require_literal "metadataStillMatches" "$gc"
require_literal "activeLeaseRetryAt" "$gc"
require_literal "scanProtections" "$gc"
require_literal "activationGuard.revalidate" "$gc"
require_literal "PhysicalObjectLifecycle.MARKED" "$gc"
require_literal "PhysicalObjectLifecycle.DELETING" "$gc"
require_literal "PLAN_DRIFT_UNMARKED" "$gc"
require_literal "public static final int ROOT_SHARDS = 256" "$scanner"
require_literal "scanRoots" "$scanner"
require_literal "markBindsExactFactsAndRecoversALostCasResponse" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollectorTest.java"
require_literal "drainWaitsForGraceAndEveryReaderLeaseBeforeEnteringDeleteIntent" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollectorTest.java"
require_literal "exactMetadataVersionOrEnvelopeDriftBlocksMarkAndUnmarksDrain" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollectorTest.java"
require_literal "deleteIntentLostResponseAndMarkedRestartBothConvergeWithoutDeletingAnything" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollectorTest.java"
require_literal "scansAllShardsWithBoundedPagesAndExactLifecycleCounts" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalObjectRootScannerTest.java"
require_literal "phase4M4RootFenceCheck" "build.gradle.kts"
require_literal "phase4M4RootFenceCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

if rg -q "deleteObject|deleteProtection|SourceRetirementMetadataStore|ObjectAuditRetirementStore" \
        "$repo_root/$gc"; then
    echo "root-fence checkpoint must stop before metadata/protection/object deletion" >&2
    exit 1
fi

echo "Phase 4 M4 reference-domain and physical-root fence/recovery surfaces verified."
