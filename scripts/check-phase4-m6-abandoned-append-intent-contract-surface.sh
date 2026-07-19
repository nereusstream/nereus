#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M6 abandoned-append artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M6 abandoned-append contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$path"; then
        echo "forbidden Phase 4 M6 abandoned-append contract '$literal' in $path" >&2
        exit 1
    fi
}

planner="$repo_root/nereus-materialization/src/main/java/com/nereusstream/materialization/gc/AbandonedAppendIntentPlanBuilder.java"
ownerless="$repo_root/nereus-materialization/src/main/java/com/nereusstream/materialization/gc/OwnerlessObjectGcExecutor.java"
runtime="$repo_root/nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4PhysicalGcRuntime.java"
keyspace="$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaKeyspace.java"
gc_test="$repo_root/nereus-materialization/src/test/java/com/nereusstream/materialization/gc/AbandonedAppendIntentGcTest.java"
append_test="$repo_root/nereus-core/src/test/java/com/nereusstream/core/append/AsyncAppendPhysicalProtectionTest.java"

for path in "$planner" "$ownerless" "$runtime" "$keyspace" "$gc_test" "$append_test"; do
    require_file "$path"
done

require_literal "class AbandonedAppendIntentPlanBuilder implements GcPlanMetadataRevalidator" "$planner"
require_literal "inspectActive(" "$planner"
require_literal "inspectMarked(" "$planner"
require_literal "ObjectProtectionType.REACHABLE_APPEND" "$planner"
require_literal "parseStreamCommitKey(" "$planner"
require_literal "GenerationZeroProtectionIdentities.reachableAppendReferenceId(" "$planner"
require_literal "AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1" "$planner"
require_literal "GenerationZeroCommitRetirementHandler.REMOVAL_TYPE" "$planner"
require_literal "config.orphanGrace().toMillis()" "$planner"
require_literal "config.maximumClockSkew().toMillis()" "$planner"
require_literal "protectionManager.transfer(" "$planner"
require_literal "previously absent append owner appeared during epoch rebind" "$planner"
require_literal "getCommitNodeByKey(" "$planner"
reject_literal "deleteProtection(" "$planner"
reject_literal "deleteCommitNode" "$planner"

require_literal "inspection.protectionRemovals()" "$ownerless"
require_literal "inspection.metadataRemovals()" "$ownerless"
require_literal "abandonedAppendIntents.reload(" "$ownerless"
require_literal "abandonedAppendIntents.reload(candidate, expected)" "$runtime"
require_literal "new GenerationZeroCommitRetirementHandler(" "$runtime"
require_literal "public StreamCommitKeyIdentity parseStreamCommitKey(" "$keyspace"

require_literal "restartRetiresAbandonedIntentOnlyAfterGraceAndAllGlobalDomainsClear" "$gc_test"
require_literal "completeDriftUnmarksThenStaleProtectionIsReboundBeforeAnotherMark" "$gc_test"
require_literal "ownerAppearanceAfterAnAbsentOwnerPlanChangesTheExactMetadataReload" "$gc_test"
require_literal "lost commit delete response" "$gc_test"
require_literal "headRemainsUnchangedWhileReachableAppendProtectionIsPending" "$append_test"
require_literal ".committedEndOffset())" "$append_test"
require_literal ".isZero();" "$append_test"
require_literal "ObjectProtectionType.REACHABLE_APPEND" "$append_test"

require_literal "phase4M6AbandonedAppendIntentCheck" "$repo_root/build.gradle.kts"
require_literal "Checkpoint BJ" "$repo_root/docs/phase-4-compaction-generation/README.md"
require_literal "phase4M6AbandonedAppendIntentCheck" \
    "$repo_root/docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M6 protected-head ordering and abandoned append-intent GC surface: PASS"
