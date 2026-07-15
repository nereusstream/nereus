#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 GC-plan $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 GC-plan contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    PhysicalGcConfig.java
    GcCandidate.java
    GcCandidateRootState.java
    GcPlan.java
    GcPlannedProtectionRemoval.java
    GcPlannedMetadataRemoval.java
    GcIdGenerator.java
    SecureGcIdGenerator.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

require_basename "PhysicalGcConfigTest.java" "test"
require_basename "GcPlanTest.java" "test"

gc_dir="nereus-materialization/src/main/java/com/nereusstream/materialization/gc"
config="$gc_dir/PhysicalGcConfig.java"
candidate="$gc_dir/GcCandidate.java"
plan="$gc_dir/GcPlan.java"

require_literal "public boolean mutationsAllowed()" "$config"
require_literal "return enabled && !dryRun" "$config"
require_literal "validateAgainst(MaterializationConfig materialization)" "$config"
require_literal "OptionalLong deadline" "$config"
require_literal "Math.addExact" "$config"
require_literal "fromActiveRoot" "$candidate"
require_literal "fromMarkedRoot" "$candidate"
require_literal "GcCandidateRootState.MARKED_RECOVERY" "$candidate"
require_literal "PhysicalObjectLifecycle.ACTIVE" "$candidate"
require_literal "maxStreamsPerCandidate" "$candidate"
require_literal "computeReferenceSetSha256" "$plan"
require_literal "fromMarkedRoot" "$plan"
require_literal "!snapshot.complete()" "$plan"
require_literal "snapshot.veto()" "$plan"
require_literal "markedRoot.value().gcAttemptId().equals(gcAttemptId)" "$plan"
require_literal "referenceSetDigestUsesExactFactsNotEphemeralCandidateIdentity" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GcPlanTest.java"
require_literal "protectionOwnerVersionAndEnvelopeArePartOfTheMarkDigest" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GcPlanTest.java"
require_literal "protectionForAnotherObjectCannotEnterMarkDigest" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GcPlanTest.java"
require_literal "responseLossReconstructionRequiresExactRootAttemptAndDigest" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GcPlanTest.java"
require_literal "reconstructsPlanFromMarkedRootWithoutInventingPreviousOxiaVersion" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GcPlanTest.java"
require_literal "phase4M4GcPlanCheck" "build.gradle.kts"
require_literal "phase4M4GcPlanCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

if rg -q "ObjectStore|deleteObject|deleteIfVersion|compareAndSetRoot" \
        "$repo_root/$candidate" "$repo_root/$plan" "$repo_root/$config"; then
    echo "GC plan/config values must not perform metadata or object deletion" >&2
    exit 1
fi

if rg -q "Serializable|ObjectOutputStream|ObjectInputStream" "$repo_root/$gc_dir"; then
    echo "process-local GC plans must not become durable serialized correctness state" >&2
    exit 1
fi

echo "Phase 4 M4 bounded reconstructable GC plan/config surfaces verified."
