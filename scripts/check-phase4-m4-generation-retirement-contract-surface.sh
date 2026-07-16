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
    GenerationZeroMarkerRetirementHandler.java
    GenerationZeroCommitRetirementHandler.java
    HigherGenerationIndexRetirementHandler.java
    GenerationRetirementOperations.java
    SourceRetirementPlanBuilder.java
    VersionedGenerationZeroCommit.java
    GcMetadataRetirementRegistry.java
    SourceRetirementCoordinator.java
)

test_artifacts=(
    KeyComponentCodecTest.java
    F4KeyspaceTest.java
    GenerationIndexRetirementHandlerTest.java
    GenerationZeroSourceRetirementHandlerTest.java
    SourceRetirementPlanBuilderTest.java
    SourceRetirementMetadataStoreContractTest.java
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
marker_handler="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/GenerationZeroMarkerRetirementHandler.java"
commit_handler="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/GenerationZeroCommitRetirementHandler.java"
source_planner="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/SourceRetirementPlanBuilder.java"
source_store="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/retirement/OxiaJavaSourceRetirementMetadataStore.java"
coordinator="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/SourceRetirementCoordinator.java"
handler_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GenerationIndexRetirementHandlerTest.java"
source_handler_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GenerationZeroSourceRetirementHandlerTest.java"
source_planner_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/SourceRetirementPlanBuilderTest.java"
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

require_literal 'REMOVAL_TYPE = "generation-zero-marker"' "$marker_handler"
require_literal "getCommittedMarkerByKey" "$marker_handler"
require_literal "generation-zero committed marker changed after uncertain delete" "$marker_handler"
require_literal 'REMOVAL_TYPE = "generation-zero-commit"' "$commit_handler"
require_literal "getCommitNodeByKey" "$commit_handler"
require_literal "generation-zero commit node changed after uncertain delete" "$commit_handler"

require_literal "getCommittedMarkerByKey" "$source_store"
require_literal "getCommitNodeByKey" "$source_store"
require_literal "markerKey(keys, route.streamId(), identity).equals(route.key())" "$source_store"
require_literal "streamCommitKey(route.streamId(), canonical.commitId()).equals(route.key())" "$source_store"

require_literal "implements GcPlanMetadataRevalidator" "$source_planner"
require_literal "findCommitCoveringOffset" "$source_planner"
require_literal "canonicalCommitRecordSha256" "$source_planner"
require_literal "getRecoveryRoot(cluster, stream)" "$source_planner"
require_literal "recovery root changed while source facts were frozen" "$source_planner"

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
require_literal "markerLostDeleteResponseConvergesOnlyAfterExactKeyIsAbsent" "$source_handler_test"
require_literal "commitDeleteAndRestartUseOnlyTheJournaledExactKey" "$source_handler_test"
require_literal "freezesIndexMarkerAndCheckpointReplacedCommitThenReloadsExactFacts" "$source_planner_test"
require_literal "checkpointEntryWithAnotherCanonicalCommitCannotAuthorizeSourceRetirement" "$source_planner_test"
require_literal "revalidatorRejectsAnUnboundExtraSourceRemoval" "$source_planner_test"
require_literal "journalIsReauthenticatedBeforeEveryMetadataBatch" "$coordinator_test"
require_literal "finalJournalReloadFencesPhysicalHeadAndDelete" "$coordinator_test"
require_literal "phase4M4GenerationRetirementCheck" "build.gradle.kts"
require_literal "phase4M4GenerationRetirementCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 canonical generation/source retirement planning and authenticated destructive batches verified."
