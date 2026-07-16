#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 tombstone-retirement $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 tombstone-retirement contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    PhysicalRootTombstoneRetirementCoordinator.java
    DefaultPhysicalRootTombstoneRetirementCoordinator.java
    TombstoneRetirementStatus.java
    TombstoneRetirementResult.java
    TombstoneRetirementDigests.java
)

test_artifacts=(
    PhysicalRootTombstoneRetirementTest.java
    LatePutAfterTombstoneTest.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done
for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

coordinator="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/DefaultPhysicalRootTombstoneRetirementCoordinator.java"
result="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/TombstoneRetirementResult.java"
retirement_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalRootTombstoneRetirementTest.java"
late_put_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/LatePutAfterTombstoneTest.java"

require_literal "config.tombstoneAuditGrace()" "$coordinator"
require_literal "config.orphanGrace()" "$coordinator"
require_literal "scanReaderLeases" "$coordinator"
require_literal "scanProtections" "$coordinator"
require_literal "referenceDomains.snapshotForDeletion" "$coordinator"
require_literal "referenceDomains.stillMatches" "$coordinator"
require_literal "HEAD exact DELETED physical object key" "$coordinator"
require_literal "delete exact reappearing object under DELETED root" "$coordinator"
require_literal "auditStore.deleteReferences" "$coordinator"
require_literal "auditStore.deleteManifest" "$coordinator"
require_literal "requireAuditsAbsent" "$coordinator"
require_literal "metadataStore.deleteRoot" "$coordinator"
require_literal "reload physical root after uncertain final retirement" "$coordinator"
require_literal "clearObservation" "$coordinator"
require_literal "rootRetired != (status == TombstoneRetirementStatus.RETIRED)" "$result"

require_literal "durableFirstAbsenceAndSecondWindowRetireRootLast" "$retirement_test"
require_literal "authorityDigestDriftRestartsTheSeparatedAbsenceWindow" "$retirement_test"
require_literal "auditAndRootDeleteResponseLossConvergeReferencesThenManifestThenRoot" "$retirement_test"
require_literal "mismatchedReappearingBytesAreQuarantinedWithoutDelete" "$retirement_test"
require_literal "lostLateDeleteResponseConvergesOnExactHeadAbsenceButKeepsTheRoot" "$late_put_test"
require_literal "ownerAppearingBeforeProviderDeleteInvalidatesTheLatePutAttempt" "$late_put_test"
require_literal "phase4M4TombstoneRetirementCheck" "build.gradle.kts"
require_literal "phase4M4TombstoneRetirementCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

references_line="$(rg -n -F 'deleteReferences(root, audits.references(), deadline)' "$repo_root/$coordinator" | head -1 | cut -d: -f1)"
manifest_line="$(rg -n -F 'deleteManifest(root, audits.manifest(), deadline)' "$repo_root/$coordinator" | head -1 | cut -d: -f1)"
root_line="$(rg -n -F 'return deleteRoot(' "$repo_root/$coordinator" | tail -1 | cut -d: -f1)"
if [[ -z "$references_line" || -z "$manifest_line" || -z "$root_line" \
        || "$references_line" -ge "$manifest_line" || "$manifest_line" -ge "$root_line" ]]; then
    echo "tombstone retirement must visibly delete references, then manifest, then root" >&2
    exit 1
fi

if rg -q "Thread\\.sleep|scheduler\\.schedule|ScheduledFuture" "$repo_root/$coordinator"; then
    echo "tombstone retirement must persist its absence window instead of retaining an in-memory timer" >&2
    exit 1
fi

echo "Phase 4 M4 dual-absence, ownerless-domain, audit-order, root-last tombstone retirement verified."
