#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 recovery-root $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 recovery-root contract '$literal' in $path" >&2
        exit 1
    fi
}

require_ordered() {
    local path="$1"
    shift
    local previous=0
    local literal
    for literal in "$@"; do
        local line
        line="$(rg -n -F -- "$literal" "$repo_root/$path" | head -n 1 | cut -d: -f1)"
        if [[ -z "$line" || "$line" -le "$previous" ]]; then
            echo "Phase 4 M4 recovery-root ordering is missing or invalid at '$literal' in $path" >&2
            exit 1
        fi
        previous="$line"
    done
}

production_artifacts=(
    AppendRecoveryAnchor.java
    AppendRecoveryCommit.java
    AppendRecoveryTailCursor.java
    AppendRecoveryTailPage.java
    RecoveryCheckpointRootDigests.java
    AnchorAwareCommitWalker.java
    RecoveryCheckpointBuilder.java
    RecoveryCheckpointCoordinator.java
    RecoveryCheckpointProtectionManager.java
    RecoveryCheckpointRootReconciler.java
    RecoveryCheckpointPublicationPage.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

test_artifacts=(
    AnchorAwareCommitWalkerTest.java
    RecoveryCheckpointBuilderTest.java
    RecoveryCheckpointCoordinatorTest.java
    RecoveryCheckpointSparseDirectoryTest.java
)

for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

require_ordered \
    "nereus-materialization/src/main/java/com/nereusstream/materialization/recovery/RecoveryCheckpointCoordinator.java" \
    '"write staged NRC1 recovery checkpoint"' \
    '"upload NRC1 recovery checkpoint"' \
    '"acquire bounded recovery checkpoint pending protection"' \
    '"publish recovery checkpoint root"'

require_literal "scanPublications" \
    "nereus-object-store/src/main/java/com/nereusstream/objectstore/checkpoint/RecoveryCheckpointCodecV1.java"
require_literal "replace recovery checkpoint pending protection" \
    "nereus-materialization/src/main/java/com/nereusstream/materialization/recovery/RecoveryCheckpointRootReconciler.java"
require_literal "restartRepairsPermanentProtectionsAfterRootCas" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/recovery/RecoveryCheckpointCoordinatorTest.java"
require_literal "publishesAndConvergesLostRecoveryRootCasResponse" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/recovery/RecoveryCheckpointCoordinatorTest.java"
require_literal "phase4M4RecoveryRootCheck" "build.gradle.kts"
require_literal "phase4M4RecoveryRootCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 recovery-root publication and reconciliation surfaces verified."
