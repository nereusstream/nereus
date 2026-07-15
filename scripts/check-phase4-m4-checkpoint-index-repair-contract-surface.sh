#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 checkpoint-index-repair $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 checkpoint-index-repair contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    GenerationIndexDigests.java
    GenerationIndexRepairer.java
    GenerationIndexRepairResult.java
    GenerationIndexRepairSource.java
    MetadataGenerationIndexRepairer.java
    RecoveryCheckpointProtectionIdentities.java
    CheckpointDerivedIndexRepairer.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

require_basename "CheckpointDerivedIndexRepairTest.java" "test"
require_basename "MetadataGenerationIndexRepairerTest.java" "test"

require_literal "restoreCommittedFromCheckpoint" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/GenerationMetadataStore.java"
require_literal "GenerationIndexDigests.canonicalRecordSha256" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaJavaGenerationMetadataStore.java"
require_literal "GenerationIndexDigests.durableValueSha256" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaJavaGenerationMetadataStore.java"
require_literal "requireExactRestoredIndex" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaJavaGenerationMetadataStore.java"
require_literal "new MetadataGenerationIndexRepairer" \
    "nereus-core/src/main/java/com/nereusstream/core/read/GenerationReadResolver.java"
require_literal "repairer.repair" \
    "nereus-core/src/main/java/com/nereusstream/core/read/GenerationReadResolver.java"
require_literal "codec.findCommitCoveringOffset" \
    "nereus-core/src/main/java/com/nereusstream/core/recovery/CheckpointDerivedIndexRepairer.java"
require_literal "codec.scanPublications" \
    "nereus-core/src/main/java/com/nereusstream/core/recovery/CheckpointDerivedIndexRepairer.java"
require_literal "pinManager.acquire" \
    "nereus-core/src/main/java/com/nereusstream/core/recovery/CheckpointDerivedIndexRepairer.java"
require_literal "ObjectProtectionType.RECOVERY_CHECKPOINT_TARGET" \
    "nereus-core/src/main/java/com/nereusstream/core/recovery/CheckpointDerivedIndexRepairer.java"
require_literal ".restoreCommittedFromCheckpoint" \
    "nereus-core/src/main/java/com/nereusstream/core/recovery/CheckpointDerivedIndexRepairer.java"
require_literal "RecoveryCheckpointProtectionIdentities.checkpointTargetReferenceId" \
    "nereus-materialization/src/main/java/com/nereusstream/materialization/recovery/RecoveryCheckpointProtectionManager.java"
require_literal "restoresHighestHealthyPublicationAfterIndexRetirementAndProtectsTarget" \
    "nereus-core/src/test/java/com/nereusstream/core/recovery/CheckpointDerivedIndexRepairTest.java"
require_literal "restartsWholeProofWhenRootChangesDuringCheckpointPin" \
    "nereus-core/src/test/java/com/nereusstream/core/recovery/CheckpointDerivedIndexRepairTest.java"
require_literal "trimmedTargetCreatesNeitherIndexNorReadPin" \
    "nereus-core/src/test/java/com/nereusstream/core/recovery/CheckpointDerivedIndexRepairTest.java"
require_literal "checkpointRepairerRestoresAuthorityBeforeResolverRescans" \
    "nereus-core/src/test/java/com/nereusstream/core/read/GenerationReadResolverTest.java"
require_literal "accumulatesBoundedLiveRepairPagesUntilTargetIsCovered" \
    "nereus-core/src/test/java/com/nereusstream/core/read/MetadataGenerationIndexRepairerTest.java"
require_literal "phase4M4CheckpointIndexRepairCheck" "build.gradle.kts"
require_literal "phase4M4CheckpointIndexRepairCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 checkpoint-derived generation-index repair surfaces verified."
