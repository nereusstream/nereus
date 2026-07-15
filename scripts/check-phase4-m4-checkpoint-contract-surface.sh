#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 checkpoint $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 checkpoint contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    RecoveryCheckpointCodecV1.java
    DefaultRecoveryCheckpointCodecV1.java
    RecoveryCheckpointFormatV1.java
    RecoveryCheckpointFormatException.java
    RecoveryCheckpointObject.java
    RecoveryCheckpointEntry.java
    RecoveryCheckpointPublication.java
    RecoveryCheckpointWriteRequest.java
    RecoveryCheckpointWriteResult.java
    RecoveryCheckpointDirectory.java
    RecoveryCheckpointVerifier.java
    MetadataRecoveryCheckpointVerifier.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

test_artifacts=(
    RecoveryCheckpointGoldenTest.java
    RecoveryCheckpointAttemptIdentityTest.java
    RecoveryCheckpointStrictDecodeTest.java
    RecoveryCheckpointStreamingTest.java
    RecoveryCheckpointSparseDirectoryTest.java
    RecoveryCheckpointDomainValidationTest.java
    MetadataRecoveryCheckpointVerifierTest.java
)

for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

require_literal "writer.string(value.cluster(), \"cluster\")" \
    "nereus-object-store/src/main/java/com/nereusstream/objectstore/checkpoint/RecoveryCheckpointBinary.java"
require_literal "openRandomAccessReader" \
    "nereus-object-store/src/main/java/com/nereusstream/objectstore/checkpoint/DefaultRecoveryCheckpointCodecV1.java"
require_literal "bodySha256 of every preceding byte" \
    "docs/phase-4-compaction-generation/02-domain-api-and-object-format.md"
require_literal "lookupRejectsBadEntryCrcEvenWhenObjectAndFooterDigestsWereRecomputed" \
    "nereus-object-store/src/test/java/com/nereusstream/objectstore/checkpoint/RecoveryCheckpointStrictDecodeTest.java"
require_literal "findsFirstMiddleAndLastCommitAcrossThreeSparseBlocks" \
    "nereus-object-store/src/test/java/com/nereusstream/objectstore/checkpoint/RecoveryCheckpointSparseDirectoryTest.java"
require_literal "phase4M4CheckpointCheck" "build.gradle.kts"
require_literal "phase4M4CheckpointCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 NRC1 object-protocol implementation checkpoint surfaces verified."
