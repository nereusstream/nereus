#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 generation-retirement $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 generation-retirement contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    GenerationCandidateKeyIdentity.java
    GenerationZeroIndexRetirementHandler.java
    HigherGenerationIndexRetirementHandler.java
    GenerationRetirementOperations.java
    GcMetadataRetirementRegistry.java
    SourceRetirementCoordinator.java
)

test_artifacts=(
    KeyComponentCodecTest.java
    F4KeyspaceTest.java
    GenerationIndexRetirementHandlerTest.java
    SourceRetirementCoordinatorTest.java
    GcPlanTest.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done
for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

codec="nereus-api/src/main/java/com/nereusstream/api/keys/KeyComponentCodec.java"
keyspace="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/F4Keyspace.java"
zero_handler="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/GenerationZeroIndexRetirementHandler.java"
higher_handler="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/HigherGenerationIndexRetirementHandler.java"
coordinator="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/SourceRetirementCoordinator.java"
handler_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GenerationIndexRetirementHandlerTest.java"
coordinator_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/SourceRetirementCoordinatorTest.java"

require_literal "decodeComponent" "$codec"
require_literal "decodeNonNegativeLong" "$codec"
require_literal "parseGenerationIndexKey" "$keyspace"
require_literal "generationIndexKey(stream, view, offsetEnd, generation).equals(key)" "$keyspace"

require_literal 'REMOVAL_TYPE = "generation-zero-index"' "$zero_handler"
require_literal "deleteGenerationZeroIndex" "$zero_handler"
require_literal "generation-zero index changed after uncertain delete" "$zero_handler"
require_literal 'REMOVAL_TYPE = "generation-index"' "$higher_handler"
require_literal "GenerationLifecycle.DRAINING" "$higher_handler"
require_literal "GenerationLifecycle.RETIRED" "$higher_handler"
require_literal "physical-gc:" "$higher_handler"
require_literal "higher-generation index changed after uncertain retirement CAS" "$higher_handler"

if rg -Fq -- "deleteIndex(" "$repo_root/$higher_handler"; then
    echo "higher-generation retirement must preserve a RETIRED audit record instead of deleting the index" >&2
    exit 1
fi

require_literal "reauthenticateContext" "$coordinator"
require_literal "runAuthenticatedBatches" "$coordinator"
require_literal "requireSameJournal" "$coordinator"
require_literal "loadExactJournal(exact, deadline)" "$coordinator"
require_literal "removal.removalType().equals(reference.referenceType())" \
    "nereus-materialization/src/main/java/com/nereusstream/materialization/gc/GcPlanValidation.java"

require_literal "generationZeroLostDeleteResponseConvergesOnlyAfterExactReloadIsAbsent" "$handler_test"
require_literal "higherGenerationLostCasResponseConvergesOnExactAttemptBoundReplacement" "$handler_test"
require_literal "journalIsReauthenticatedBeforeEveryMetadataBatch" "$coordinator_test"
require_literal "finalJournalReloadFencesPhysicalHeadAndDelete" "$coordinator_test"
require_literal "phase4M4GenerationRetirementCheck" "build.gradle.kts"
require_literal "phase4M4GenerationRetirementCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 canonical generation-index retirement and authenticated destructive batches verified."
