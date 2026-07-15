#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M3 $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M3 contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    CompactedObjectFormatV1.java
    ParquetCompactedObjectWriter.java
    ParquetCompactedObjectReader.java
    TopicCompactedObjectReader.java
    TopicCompactionKeyEncodingV1.java
    ManagedStagingFile.java
    PrivateStagingSpillFile.java
    MaterializationPolicyFactory.java
    DefaultMaterializationPlanner.java
    MaterializationTaskStore.java
    MaterializationTaskRecovery.java
    RegisteredMaterializationStreamScanner.java
    DefaultExactSourceRangeReader.java
    LosslessMaterializationRowPublisher.java
    DefaultMaterializationWorker.java
    DefaultMaterializationTaskProtectionReconciler.java
    DefaultMaterializationCheckpointReconciler.java
    DefaultMaterializationTaskDispatcher.java
    DefaultMaterializationService.java
    TopicCompactionRegistry.java
    DefaultTopicCompactionEngine.java
    DefaultTerminalWorkflowMetadataRetirer.java
    CompactedMaterializationFormatVerifier.java
    ParquetCompactedTargetReader.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

test_artifacts=(
    CompactedParquetGoldenTest.java
    CompactedParquetStrictReaderTest.java
    CompactedParquetDensePropertyTest.java
    CompactedParquetCorruptionTest.java
    CompactedObjectStreamingUploadTest.java
    CompactedObjectStagingLimitTest.java
    TopicCompactedSparseFormatTest.java
    TopicCompactionKeyEncodingV1Test.java
    PulsarEntryOpaqueRoundTripTest.java
    ParquetCompactedTargetReaderTest.java
    CompactedMaterializationFormatVerifierTest.java
    MaterializationPlannerFixedPointTest.java
    MaterializationPlannerOverlapTilingTest.java
    MaterializationTaskStoreTest.java
    MaterializationWorkerTest.java
    MaterializationWorkerFailureInjectionTest.java
    MaterializationWorkerClaimModelTest.java
    ExactSourceRangeReaderTest.java
    MaterializationTaskRecoveryTest.java
    MaterializationTaskProtectionReconcilerTest.java
    RegisteredMaterializationStreamScannerTest.java
    MaterializationCheckpointReconcilerTest.java
    DefaultMaterializationTaskDispatcherTest.java
    TerminalWorkflowMetadataRetirementTest.java
    MaterializationServiceCloseTest.java
    MaterializationConfigTest.java
    TopicCompactionEngineTest.java
    MaterializationWorkerOxiaS3IntegrationTest.java
)

for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

require_literal "sourceView()" \
    "nereus-materialization/src/main/java/com/nereusstream/materialization/TaskKind.java"
require_literal "TopicCompactionKeyEncodingV1.ID" \
    "nereus-object-store/src/main/java/com/nereusstream/objectstore/compacted/CompactedObjectFormatV1.java"
require_literal "createSpill" \
    "nereus-materialization/src/main/java/com/nereusstream/materialization/DefaultTopicCompactionEngine.java"
require_literal "claimed.value().createdAtMillis()" \
    "nereus-materialization/src/main/java/com/nereusstream/materialization/DefaultMaterializationWorker.java"
require_literal "sourceChangeAfterProtectionCancelsAndReleasesTheExactClaim" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/MaterializationWorkerFailureInjectionTest.java"
require_literal "lostOutputReadyCasResponseReloadsTheExactFrozenOutput" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/MaterializationWorkerFailureInjectionTest.java"
require_literal "twoIndependentWorkersCannotExecuteTheSameDurableClaimConcurrently" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/MaterializationWorkerClaimModelTest.java"
require_literal "fullScanPaginatesEveryShardAndFreshProcessRecoversWithoutWatchHints" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/RegisteredMaterializationStreamScannerTest.java"
require_literal "failsClosedWhenDecoderFactsChangeBetweenPasses" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/TopicCompactionEngineTest.java"
require_literal "publishesTopicCompactionOnlyIntoTheIsolatedTargetView" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/GenerationIndexPublicationTest.java"

for gate in phase4M3Check phase4M3FinalCheck; do
    require_literal "$gate" "build.gradle.kts"
    require_literal "$gate" "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
done

echo "Phase 4 M3 format, planner, worker, recovery, model, integration, and documentation surfaces verified."
