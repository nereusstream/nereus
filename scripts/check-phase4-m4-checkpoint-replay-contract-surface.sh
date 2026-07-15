#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 checkpoint-replay $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 checkpoint-replay contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    AppendReplayRecords.java
    AppendRecoverySearcher.java
    AppendReplayEvidenceSource.java
    AppendReplayResolution.java
    MetadataAppendRecoverySearcher.java
    CheckpointAppendReplayReader.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

require_basename "CheckpointAppendReplayTest.java" "test"
require_basename "RecoveryCheckpointSparseDirectoryTest.java" "test"

require_literal "findCommitCoveringOffset" \
    "nereus-object-store/src/main/java/com/nereusstream/objectstore/checkpoint/RecoveryCheckpointCodecV1.java"
require_literal "floorIndex(state.commits().offsetStarts(), offset)" \
    "nereus-object-store/src/main/java/com/nereusstream/objectstore/checkpoint/DefaultRecoveryCheckpointCodecV1.java"
require_literal "AppendReplayRecords.requireMatches" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaJavaClientMetadataStore.java"
require_literal "AppendReplayRecords.requireMatches" \
    "nereus-metadata-oxia/src/testFixtures/java/com/nereusstream/metadata/oxia/testing/FakeOxiaMetadataStore.java"
require_literal "pinManager.acquire" \
    "nereus-core/src/main/java/com/nereusstream/core/recovery/CheckpointAppendReplayReader.java"
require_literal "codec.openAndVerify" \
    "nereus-core/src/main/java/com/nereusstream/core/recovery/CheckpointAppendReplayReader.java"
require_literal "codec.findCommitCoveringOffset" \
    "nereus-core/src/main/java/com/nereusstream/core/recovery/CheckpointAppendReplayReader.java"
require_literal "thenCompose(result -> requireExactRoot(root)" \
    "nereus-core/src/main/java/com/nereusstream/core/recovery/CheckpointAppendReplayReader.java"
require_literal "new MetadataAppendRecoverySearcher" \
    "nereus-core/src/main/java/com/nereusstream/core/DefaultStreamStorage.java"
require_literal "AppendReplayEvidenceSource.RECOVERY_CHECKPOINT" \
    "nereus-core/src/main/java/com/nereusstream/core/append/AppendCoordinator.java"
require_literal "findsExactAppendAfterLiveCommitKeyRetirementAndReleasesReadPin" \
    "nereus-core/src/test/java/com/nereusstream/core/recovery/CheckpointAppendReplayTest.java"
require_literal "restartsWhenRootChangesDuringPinAndNeverAliasesAnotherCommit" \
    "nereus-core/src/test/java/com/nereusstream/core/recovery/CheckpointAppendReplayTest.java"
require_literal "phase4M4CheckpointReplayCheck" "build.gradle.kts"
require_literal "phase4M4CheckpointReplayCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 checkpoint-aware append replay surfaces verified."
