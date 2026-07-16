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
    RecoveryReplacementVerifier.java
    CompletedTrimRetirementVerifier.java
    HigherGenerationRecoveryCoverageVerifier.java
    TopicCompactedReplacementVerifier.java
    HigherGenerationRetirementEligibilityVerifier.java
    HigherGenerationPreDrainCoordinator.java
    HigherGenerationPreDrainResult.java
    HigherGenerationPreDrainStatus.java
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
    HigherGenerationPreDrainCoordinatorTest.java
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
replacement_verifier="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/RecoveryReplacementVerifier.java"
higher_coverage="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/HigherGenerationRecoveryCoverageVerifier.java"
completed_trim="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/CompletedTrimRetirementVerifier.java"
topic_replacement="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/TopicCompactedReplacementVerifier.java"
higher_eligibility="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/HigherGenerationRetirementEligibilityVerifier.java"
pre_drain="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/HigherGenerationPreDrainCoordinator.java"
source_store="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/retirement/OxiaJavaSourceRetirementMetadataStore.java"
coordinator="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/SourceRetirementCoordinator.java"
handler_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GenerationIndexRetirementHandlerTest.java"
source_handler_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GenerationZeroSourceRetirementHandlerTest.java"
source_planner_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/SourceRetirementPlanBuilderTest.java"
pre_drain_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/HigherGenerationPreDrainCoordinatorTest.java"
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
require_literal "higherEligibility.prove" "$source_planner"
require_literal "completedTrim.proveIfCompleted" "$source_planner"
require_literal "scanPublications" "$replacement_verifier"
require_literal "GenerationIndexDigests.canonicalRecordSha256" "$replacement_verifier"
require_literal "PhysicalObjectLifecycle.ACTIVE" "$replacement_verifier"
require_literal 'source + " source has no current healthy NRC1 replacement"' "$replacement_verifier"
require_literal 'revalidate(expected, "healthy NRC1 replacement")' "$replacement_verifier"
require_literal 'revalidate(expected, "healthy same-view replacement")' "$replacement_verifier"
require_literal 'label + " index changed while source facts were frozen"' "$replacement_verifier"
require_literal 'label + " root changed while source facts were frozen"' "$replacement_verifier"
require_literal "record.generation() <= requirement.minimumGenerationExclusive()" "$replacement_verifier"

require_literal "TOPIC_COMPACTED higher-generation retirement requires a view-specific replacement proof" "$higher_coverage"
require_literal "higher-generation source is not an exact tiling of NRC1 commit entries" "$higher_coverage"
require_literal "higher-generation NRC1 tiling does not reproduce source counts or schemas" "$higher_coverage"
require_literal "higher-generation source changed while coverage was frozen" "$higher_coverage"

require_literal "proveIfCompleted" "$completed_trim"
require_literal "completed trim changed while retirement facts were frozen" "$completed_trim"
require_literal "recovery root changed while below-trim facts were frozen" "$completed_trim"
require_literal "loadCurrentHealthy" "$replacement_verifier"
require_literal "TOPIC_COMPACTED source has no current healthy same-view replacement" "$topic_replacement"
require_literal "revalidateSameView" "$topic_replacement"
require_literal "trim.proveIfCompleted" "$higher_eligibility"
require_literal "topicCompacted.prove" "$higher_eligibility"

require_literal "physical-gc-pre-drain:" "$pre_drain"
require_literal "GenerationLifecycle.COMMITTED" "$pre_drain"
require_literal "GenerationLifecycle.QUARANTINED" "$pre_drain"
require_literal "GenerationLifecycle.DRAINING" "$pre_drain"
require_literal "eligibility.prove" "$pre_drain"
require_literal "HigherGenerationPreDrainResult.notEligibleYet()" "$pre_drain"
require_literal "candidate physical root changed before higher-generation pre-drain" "$pre_drain"
require_literal "higher-generation source changed after uncertain pre-drain CAS" "$pre_drain"

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
require_literal "quarantinedReplacementIndexCannotAuthorizeSourceRetirement" "$source_planner_test"
require_literal "markedReplacementRootCannotAuthorizeSourceRetirement" "$source_planner_test"
require_literal "replacementIndexChangeDuringFreezeRejectsThePlan" "$source_planner_test"
require_literal "replacementRootChangeDuringFreezeRejectsThePlan" "$source_planner_test"
require_literal "committedSourceDrainsOnlyAfterExactRecoveryReplacementCoverage" "$pre_drain_test"
require_literal "quarantinedSourceUsesTheSameCoverageProofBeforeDraining" "$pre_drain_test"
require_literal "lostCasResponseConvergesOnlyOnTheExactDrainingReplacement" "$pre_drain_test"
require_literal "candidateRootChangeAtTheFinalFencePreventsTheSourceCas" "$pre_drain_test"
require_literal "alreadyDrainingSourceStillRequiresCurrentHealthyCoverage" "$pre_drain_test"
require_literal "incompleteNrc1TilingCountsVetoBeforeTheSourceCas" "$pre_drain_test"
require_literal "dryRunReturnsBeforeAnyMetadataOrRootRead" "$pre_drain_test"
require_literal "drainingHigherRemovalPlannerReprovesCoverageBeforeFreezing" "$pre_drain_test"
require_literal "topicCompactedSourceUsesAHealthySameViewReplacement" "$pre_drain_test"
require_literal "drainingTopicRemovalPlannerReprovesTheSameViewReplacement" "$pre_drain_test"
require_literal "completedTrimDrainsTopicSourceWithoutReplacementOrCheckpointReads" "$pre_drain_test"
require_literal "completedTrimDriftVetoesBeforeTheSourceCas" "$pre_drain_test"
require_literal "sourceRetirementGraceReturnsBeforeAnyMetadataOrRootRead" "$pre_drain_test"
require_literal "completedTrimAuthorizesGenerationZeroWithoutAHealthyReplacement" "$source_planner_test"
require_literal "completedTrimDriftRejectsGenerationZeroBeforeReplacementReads" "$source_planner_test"
require_literal "journalIsReauthenticatedBeforeEveryMetadataBatch" "$coordinator_test"
require_literal "finalJournalReloadFencesPhysicalHeadAndDelete" "$coordinator_test"
require_literal "phase4M4GenerationRetirementCheck" "build.gradle.kts"
require_literal "phase4M4GenerationRetirementCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 view-specific/below-trim source eligibility, higher pre-drain, and authenticated destructive batches verified."
