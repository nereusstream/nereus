#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M2 $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M2 contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    StreamViewReader.java
    ViewReadResult.java
    ReadTargetReader.java
    ReadTargetReaderKey.java
    ReadTargetReaderRegistry.java
    GenerationReadResolver.java
    GenerationReadCandidate.java
    PinnedResolvedRange.java
    ReadTargetDispatcher.java
    GenerationReadRetryPolicy.java
    MetadataGenerationReadFailureHandler.java
    GenerationAllocator.java
    GenerationIndexValidator.java
    GenerationCommitter.java
    DefaultGenerationCommitter.java
    GenerationCommitResult.java
    GenerationPublicationReconciler.java
    MaterializationTask.java
    MaterializationOutput.java
    MaterializationPolicy.java
    SourceGeneration.java
    DefaultMaterializationOutputVerifier.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

test_artifacts=(
    GenerationAllocatorConcurrencyTest.java
    GenerationIndexPublicationTest.java
    GenerationPublicationFailureInjectionTest.java
    GenerationPublicationPropertyTest.java
    GenerationPublicationReconcilerTest.java
    MaterializationDomainTest.java
    GenerationIndexCompatibilityTest.java
    GenerationReadResolverTest.java
    MetadataGenerationReadFailureHandlerTest.java
    ReadTargetReaderRegistryTest.java
    ReadTargetDispatcherMixedFormatTest.java
    PinnedReadCoordinatorTest.java
    GenerationPublicationOxiaS3IntegrationTest.java
)

for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

require_literal "concurrentPublishersConvergeToOneTaskAttachedGenerationAndOnePublicationPoint" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/GenerationPublicationPropertyTest.java"
require_literal "resolvesLostCommittedCasResponseFromTheExactIndex" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/GenerationPublicationFailureInjectionTest.java"
require_literal "provesAbortedThenClearsTheOldAllocationBeforeStartingANewPublication" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/GenerationPublicationReconcilerTest.java"
require_literal "staleHigherCandidateAtPinTimeFallsBackOnlyWithinCommittedView" \
    "nereus-core/src/test/java/com/nereusstream/core/read/GenerationReadResolverTest.java"
require_literal "candidateOverflowFailsInsteadOfSilentlyIgnoringAHigherGeneration" \
    "nereus-core/src/test/java/com/nereusstream/core/read/GenerationReadResolverTest.java"
require_literal "retriesRetriableTransientFailureOnTheSameCandidateBeforeFallback" \
    "nereus-core/src/test/java/com/nereusstream/core/read/PinnedReadCoordinatorTest.java"
require_literal "quarantinesEveryDiscoveredCommittedIndexThatReferencesTheCorruptObject" \
    "nereus-core/src/test/java/com/nereusstream/core/read/MetadataGenerationReadFailureHandlerTest.java"

for gate in phase4M2Check phase4M2FinalCheck; do
    require_literal "$gate" "build.gradle.kts"
    require_literal "$gate" "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
done

require_literal "F4-M2 is complete/final-gated" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M2FinalCheck --rerun-tasks" \
    "docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M2 publication, read, fallback, quarantine, test, integration, and documentation surfaces verified."
