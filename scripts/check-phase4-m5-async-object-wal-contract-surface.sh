#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M5 async Object-WAL artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M5 async Object-WAL contract '$literal' in $path" >&2
        exit 1
    fi
}

core="$repo_root/nereus-core/src/main/java/com/nereusstream/core"
core_test="$repo_root/nereus-core/src/test/java/com/nereusstream/core"

profile="$core/profile/Phase4StorageProfileResolver.java"
async_append="$core/append/AsyncObjectWalAppendCoordinator.java"
append_coordinator="$core/append/AppendCoordinator.java"
storage="$core/DefaultStreamStorage.java"
scanner="$core/recovery/GenerationZeroRepairScanner.java"
read_repair="$core/read/ReadAfterStableCommitRepair.java"
checkpoint_repair="$core/recovery/CheckpointDerivedIndexRepairer.java"
generation_resolver="$core/read/GenerationReadResolver.java"
profile_test="$core_test/profile/Phase4StorageProfileResolverTest.java"
async_test="$core_test/append/AsyncObjectWalAppendCoordinatorTest.java"
physical_test="$core_test/append/AsyncAppendPhysicalProtectionTest.java"
scanner_test="$core_test/recovery/GenerationZeroRepairScannerTest.java"
read_test="$core_test/recovery/AsyncReadAfterCommitRepairTest.java"
resolver_test="$core_test/read/GenerationReadResolverTest.java"

for path in \
    "$profile" \
    "$async_append" \
    "$append_coordinator" \
    "$storage" \
    "$scanner" \
    "$read_repair" \
    "$checkpoint_repair" \
    "$generation_resolver" \
    "$profile_test" \
    "$async_test" \
    "$physical_test" \
    "$scanner_test" \
    "$read_test" \
    "$resolver_test"; do
    require_file "$path"
done

require_literal "profile == StorageProfile.OBJECT_WAL_ASYNC_OBJECT" "$profile"
require_literal "DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED" "$profile"
require_literal "!profile.usesObjectWal()" "$profile"

require_literal "completeAfterStableCommit" "$async_append"
require_literal "startBackgroundRepair(exact.reachableAppend())" "$async_append"
require_literal "repairAndProtect(" "$async_append"
require_literal "physicalReferences.protectVisibleIndex" "$async_append"
require_literal "backgroundRepairTimeout" "$async_append"
require_literal "backgroundRepairFailures.incrementAndGet()" "$async_append"

require_literal "appendCompletionCoordinator.completeAfterStableCommit" "$append_coordinator"
require_literal "attempt.options().durabilityLevel()" "$append_coordinator"
require_literal "new Phase15StorageProfileResolver()" "$append_coordinator"
require_literal "StorageProfileResolver profileResolver" "$storage"
require_literal "new Phase15StorageProfileResolver()" "$storage"

require_literal "walker.walk(streamId, maxCommits, pageSize)" "$scanner"
require_literal "metadataStore.materializeGenerationZero" "$scanner"
require_literal "physicalReferences.protectVisibleIndex" "$scanner"
require_literal "repair target belongs to recovery-checkpoint evidence" "$scanner"
require_literal "implements GenerationIndexRepairer" "$read_repair"
require_literal "GenerationIndexRepairer liveRepairer" "$checkpoint_repair"
require_literal "if (!profile.objectMaterializationEnabled())" "$generation_resolver"

require_literal "walDurableReturnsBeforeDetachedGenerationZeroWorkStarts" "$async_test"
require_literal "strictDurabilityWaitsForExactVisibleGenerationProtection" "$async_test"
require_literal "fullAppendReturnsAtProtectedHeadAndDetachedRepairPublishesProtection" "$physical_test"
require_literal "repairsAndProtectsEveryUntrimmedLiveCommitFromStableHeadEvidence" "$scanner_test"
require_literal "committedIndexGapIsRepairedWithVisibleGenerationProtection" "$read_test"
require_literal "asyncObjectWalProfileUsesTheSameCommittedGenerationResolver" "$resolver_test"

require_literal "phase4M5AsyncObjectWalCheck" "$repo_root/build.gradle.kts"
require_literal "Checkpoint AD" "$repo_root/docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M5 async Object-WAL acknowledgement, protected generation-zero repair, and object-materializing profile seam verified."
